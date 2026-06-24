"""
aggregate.py
────────────
สร้างตาราง B2C (user_tracking_b2c) ด้วย BigQuery SQL — แปลงตรงจาก notebook PySpark เดิม

กลยุทธ์: CREATE OR REPLACE TABLE (full rebuild ทุกรอบ) เพราะ logic ใช้ window function
ครอบทั้งประวัติของ user (package_row / event_row / current_package_flag / user package list)
จึง incremental ราย วันไม่ได้ — แต่ข้อมูลระดับนี้ rebuild ใน BigQuery ถูกและเร็ว

หมายเหตุการแปลงที่ตั้งใจให้ "ตรงกับ notebook":
  • date_id ยึด Asia/Bangkok (ตามทั้งระบบ); ขอบบน < CURRENT_DATE (วันนี้) = cutoff เที่ยงคืน
  • week_id = ปีปฏิทิน + ISO week (เหมือน Spark weekofyear) — quirk เดิมที่ขอบปีอาจคาบเกี่ยว
  • B2C = packageId IN (1,2,3,12)
  • eggToken ของ event 'Token Used' ถูกกลับเครื่องหมาย (*-1) ก่อน sum
  • แถว 'Trial Conversion' ถูก union เพิ่มเป็น 'Subscribe' (เลียนแบบ unionByName เดิม)
"""

from __future__ import annotations

import logging
from datetime import date

from google.cloud import bigquery

log = logging.getLogger("pipeline.aggregate")

# B2C package ids (ตาม notebook: .where(col('packageId').isin(1,2,3,12)))
_B2C_PACKAGE_IDS = "1, 2, 3, 12"


def build_b2c_sql(
    *,
    event_table_fqn: str,
    package_table_fqn: str,
    users_table_fqn: str,
    b2c_table_fqn: str,
    start_date: date,
    tz_name: str,
) -> str:
    """คืน SQL `CREATE OR REPLACE TABLE ... AS SELECT ...` สำหรับตาราง B2C."""
    return f"""
CREATE OR REPLACE TABLE `{b2c_table_fqn}`
PARTITION BY date_id
CLUSTER BY userId AS
WITH
-- B2C packages (id 1,2,3,12) + ชื่อ
pkg AS (
  SELECT DISTINCT
    SAFE_CAST(packageId AS INT64) AS packageId,
    packageName
  FROM `{package_table_fqn}`
  WHERE SAFE_CAST(packageId AS INT64) IN ({_B2C_PACKAGE_IDS})
),
-- user ที่โดน ban (isBanned = TRUE) — ใช้ตัดออกจาก event
banned AS (
  SELECT DISTINCT userId
  FROM `{users_table_fqn}`
  WHERE isBanned = TRUE
),
-- event ในช่วง [start_date, วันนี้) ตาม date_id (Asia/Bangkok)
-- LEFT ANTI JOIN กับ banned: ตัด event ของ user ที่โดน ban ออก "ก่อน" aggregate ทั้งหมด
evt AS (
  SELECT DISTINCT
    e.event_id AS _id,
    e.date_id,
    e.eventTimeStamp,
    e.userId,
    e.eventType,
    SAFE_CAST(e.packageId AS INT64) AS packageId,
    e.eggToken,
    e.chatToken,
    e.totalCostThb
  FROM `{event_table_fqn}` e
  LEFT JOIN banned b USING (userId)
  WHERE b.userId IS NULL                       -- left-anti: เอาเฉพาะ user ที่ไม่อยู่ใน banned
    AND e.date_id >= DATE '{start_date.isoformat()}'
    AND e.date_id < CURRENT_DATE('{tz_name}')
),
-- inner join เฉพาะ event ของ B2C package
evt_pkg AS (
  SELECT evt.*, pkg.packageName
  FROM evt JOIN pkg USING (packageId)
),
-- รายชื่อ package ของแต่ละ user (เรียง asc แล้วหยิบ 2 ตัวแรก)
user_pkg AS (
  SELECT
    userId,
    ARRAY_AGG(DISTINCT packageName ORDER BY packageName)[SAFE_OFFSET(0)] AS package_1,
    ARRAY_AGG(DISTINCT packageName ORDER BY packageName)[SAFE_OFFSET(1)] AS package_2
  FROM evt_pkg
  GROUP BY userId
),
-- จัดอันดับ package ของ user (สูง->ต่ำ) เพื่อแยก Subscribe / Trial Conversion
evt_pkg_ranked AS (
  SELECT
    evt_pkg.*,
    DENSE_RANK() OVER (PARTITION BY userId ORDER BY packageId DESC) AS package_row
  FROM evt_pkg
),
pre_conv AS (
  SELECT
    *,
    CASE
      WHEN eventType = 'Subscribe' AND package_row = 1 THEN 'Subscribe'
      WHEN eventType = 'Subscribe' AND package_row = 2 THEN 'Trial Conversion'
      WHEN eventType = 'Token Used' THEN 'Active'
      ELSE eventType
    END AS event_flag,
    CONCAT(
      FORMAT_DATE('%Y', date_id),
      LPAD(CAST(EXTRACT(ISOWEEK FROM date_id) AS STRING), 2, '0')
    ) AS week_id,
    CONCAT(FORMAT_DATE('%Y', date_id), FORMAT_DATE('%m', date_id)) AS month_id
  FROM evt_pkg_ranked
),
-- union: แถว Trial Conversion ถูกนับซ้ำเป็น Subscribe ด้วย (เลียนแบบ notebook)
unioned AS (
  SELECT * FROM pre_conv
  UNION ALL
  SELECT * REPLACE ('Subscribe' AS event_flag)
  FROM pre_conv
  WHERE event_flag = 'Trial Conversion'
),
final_conv AS (
  SELECT
    u.* EXCEPT (eggToken),
    CASE WHEN u.eventType = 'Token Used' THEN u.eggToken * -1 ELSE u.eggToken END AS eggToken,
    up.package_1,
    up.package_2
  FROM unioned u
  JOIN user_pkg up USING (userId)
),
agg AS (
  SELECT
    month_id,
    week_id,
    date_id,
    userId,
    MAX(packageName) AS packageName,
    COUNT(DISTINCT CASE WHEN event_flag = 'Trial Conversion' THEN event_flag END) AS trial_conversion_cnt,
    SUM(CASE WHEN eventType = 'Token Used' THEN eggToken END) AS token_used,
    SUM(CASE WHEN eventType = 'Token Used' THEN totalCostThb END) AS totalCostThb,
    SUM(CASE
          WHEN eventType = 'Token Used' AND package_1 = 'Free Trial' AND package_row = 1
          THEN eggToken
        END) AS free_trial_token_used
  FROM final_conv
  GROUP BY month_id, week_id, date_id, userId
)
SELECT
  a.month_id,
  a.week_id,
  a.date_id,
  a.userId,
  a.packageName,
  a.trial_conversion_cnt,
  a.token_used,
  a.totalCostThb,
  a.free_trial_token_used,
  up.package_1,
  up.package_2,
  DENSE_RANK() OVER (PARTITION BY a.userId ORDER BY a.date_id) AS event_row,
  CASE
    WHEN DENSE_RANK() OVER (PARTITION BY a.userId ORDER BY a.date_id DESC) = 1 THEN 1 ELSE 0
  END AS current_package_flag,
  pk.packageId,
  CURRENT_DATE('{tz_name}') AS run_date
FROM agg a
JOIN user_pkg up USING (userId)
JOIN pkg pk USING (packageName)
""".strip()


def run_b2c_aggregate(client: bigquery.Client, cfg) -> int:
    """รัน SQL สร้างตาราง B2C แล้วคืนจำนวนแถวผลลัพธ์."""
    sql = build_b2c_sql(
        event_table_fqn=cfg.bq_table_fqn,
        package_table_fqn=cfg.bq_package_table_fqn,
        users_table_fqn=cfg.bq_users_table_fqn,
        b2c_table_fqn=cfg.bq_b2c_table_fqn,
        start_date=cfg.start_date,
        tz_name=cfg.timezone_name,
    )
    client.query(sql).result()  # รอจบ + raise ถ้า error
    rows = client.get_table(cfg.bq_b2c_table_fqn).num_rows
    log.info("rebuilt %s -> %d rows", cfg.bq_b2c_table_fqn, rows)
    return rows


# package ที่ "ไม่นับ" ฝั่ง B2B (ตาม notebook: ~isin(5,7,10,97,98))
_B2B_EXCLUDE_PACKAGE_IDS = "5, 7, 10, 97, 98"


def build_b2b_sql(
    *,
    event_table_fqn: str,
    package_table_fqn: str,
    users_table_fqn: str,
    company_table_fqn: str,
    team_table_fqn: str,
    b2b_table_fqn: str,
    start_date: date,
    tz_name: str,
) -> str:
    """
    คืน SQL สร้างตาราง B2B — แปลงจาก notebook section B2B
    เพิ่มมิติ company/team + company_size_range (bin ทีละ 10 คน) + window
    company_first_event_row / team_first_event_row
    (B2B ไม่มี Trial Conversion/Free Trial และไม่ตัด banned ตาม notebook เดิม)
    """
    return f"""
CREATE OR REPLACE TABLE `{b2b_table_fqn}`
PARTITION BY date_id
CLUSTER BY companyId, userId AS
WITH
-- map user -> team -> company
b2b_user_base AS (
  SELECT DISTINCT u.userId, u.teamId, u.teamName, t.companyId, c.companyName
  FROM `{users_table_fqn}` u
  JOIN `{team_table_fqn}` t USING (teamId)
  JOIN `{company_table_fqn}` c USING (companyId)
),
-- ขนาดบริษัท -> bin ทีละ 10 คน (เช่น 1-10, 11-20)
company_range AS (
  SELECT
    companyId,
    num_bin AS number_of_user_bin,
    CONCAT('(', CAST((num_bin - 1) * 10 + 1 AS STRING), '-', CAST(num_bin * 10 AS STRING), ')')
      AS company_size_range
  FROM (
    SELECT companyId, DIV(COUNT(DISTINCT userId) - 1, 10) + 1 AS num_bin
    FROM b2b_user_base
    GROUP BY companyId
  )
),
b2b_user AS (
  SELECT b.*, r.number_of_user_bin, r.company_size_range
  FROM b2b_user_base b JOIN company_range r USING (companyId)
),
-- B2B packages (ตัด id 5,7,10,97,98)
pkg AS (
  SELECT DISTINCT SAFE_CAST(packageId AS INT64) AS packageId, packageName
  FROM `{package_table_fqn}`
  WHERE SAFE_CAST(packageId AS INT64) NOT IN ({_B2B_EXCLUDE_PACKAGE_IDS})
),
evt AS (
  SELECT DISTINCT
    event_id AS _id, date_id, eventTimeStamp, userId, eventType,
    SAFE_CAST(packageId AS INT64) AS packageId, eggToken, chatToken, totalCostThb
  FROM `{event_table_fqn}`
  WHERE date_id >= DATE '{start_date.isoformat()}'
    AND date_id < CURRENT_DATE('{tz_name}')
),
evt_pkg AS (
  SELECT evt.*, pkg.packageName FROM evt JOIN pkg USING (packageId)
),
user_pkg AS (
  SELECT
    userId,
    ARRAY_AGG(DISTINCT packageName ORDER BY packageName)[SAFE_OFFSET(0)] AS package_1,
    ARRAY_AGG(DISTINCT packageName ORDER BY packageName)[SAFE_OFFSET(1)] AS package_2
  FROM evt_pkg GROUP BY userId
),
prep AS (
  SELECT
    *,
    CONCAT(FORMAT_DATE('%Y', date_id), LPAD(CAST(EXTRACT(ISOWEEK FROM date_id) AS STRING), 2, '0')) AS week_id,
    CONCAT(FORMAT_DATE('%Y', date_id), FORMAT_DATE('%m', date_id)) AS month_id,
    CASE WHEN eventType = 'Token Used' THEN eggToken * -1 ELSE eggToken END AS eggToken_adj
  FROM evt_pkg
),
agg AS (
  SELECT
    month_id, week_id, date_id, userId,
    MAX(packageName) AS packageName,
    SUM(CASE WHEN eventType = 'Token Used' THEN eggToken_adj END) AS token_used,
    SUM(CASE WHEN eventType = 'Token Used' THEN totalCostThb END) AS totalCostThb
  FROM prep
  GROUP BY month_id, week_id, date_id, userId
)
SELECT
  a.month_id, a.week_id, a.date_id, a.userId,
  a.packageName, a.token_used, a.totalCostThb,
  up.package_1, up.package_2,
  bu.teamId, bu.teamName, bu.companyId, bu.companyName,
  bu.number_of_user_bin, bu.company_size_range,
  DENSE_RANK() OVER (PARTITION BY a.userId ORDER BY a.date_id) AS event_row,
  DENSE_RANK() OVER (PARTITION BY bu.companyId ORDER BY a.date_id) AS company_first_event_row,
  DENSE_RANK() OVER (PARTITION BY bu.teamId ORDER BY a.date_id) AS team_first_event_row,
  CASE
    WHEN DENSE_RANK() OVER (PARTITION BY a.userId ORDER BY a.date_id DESC) = 1 THEN 1 ELSE 0
  END AS current_package_flag,
  pk.packageId,
  CURRENT_DATE('{tz_name}') AS run_date
FROM agg a
JOIN user_pkg up USING (userId)
JOIN b2b_user bu USING (userId)
JOIN pkg pk USING (packageName)
""".strip()


def run_b2b_aggregate(client: bigquery.Client, cfg) -> int:
    """รัน SQL สร้างตาราง B2B แล้วคืนจำนวนแถวผลลัพธ์."""
    sql = build_b2b_sql(
        event_table_fqn=cfg.bq_b2b_event_fqn,
        package_table_fqn=cfg.bq_b2b_package_fqn,
        users_table_fqn=cfg.bq_b2b_users_fqn,
        company_table_fqn=cfg.bq_b2b_company_fqn,
        team_table_fqn=cfg.bq_b2b_team_fqn,
        b2b_table_fqn=cfg.bq_b2b_agg_fqn,
        start_date=cfg.start_date,
        tz_name=cfg.timezone_name,
    )
    client.query(sql).result()
    rows = client.get_table(cfg.bq_b2b_agg_fqn).num_rows
    log.info("rebuilt %s -> %d rows", cfg.bq_b2b_agg_fqn, rows)
    return rows
