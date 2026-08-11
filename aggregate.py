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
-- จัดอันดับ package ของ user (สูง->ต่ำ) ใช้กับ free_trial_token_used
-- + ลำดับสะสมของ Subscribe (paid/trial) ใช้ระบุ "การซื้อ paid ครั้งแรกหลัง trial"
evt_pkg_ranked AS (
  SELECT
    evt_pkg.*,
    DENSE_RANK() OVER (PARTITION BY userId ORDER BY packageId DESC) AS package_row,
    SUM(IF(eventType = 'Subscribe' AND packageId != 12, 1, 0)) OVER (
      PARTITION BY userId ORDER BY eventTimeStamp, _id ROWS UNBOUNDED PRECEDING
    ) AS paid_sub_cum,
    SUM(IF(eventType = 'Subscribe' AND packageId = 12, 1, 0)) OVER (
      PARTITION BY userId ORDER BY eventTimeStamp, _id ROWS UNBOUNDED PRECEDING
    ) AS trial_sub_cum
  FROM evt_pkg
),
pre_conv AS (
  SELECT
    *,
    -- Trial Conversion = การซื้อแพ็คเสียเงิน "ครั้งแรก" ของ user ที่เคยได้ trial มาก่อน
    -- (ครั้งเดียวต่อ user — การซื้อซ้ำครั้งถัดไปเป็น 'Subscribe' ธรรมดา)
    CASE
      WHEN eventType = 'Subscribe' AND packageId != 12
           AND paid_sub_cum = 1 AND trial_sub_cum >= 1 THEN 'Trial Conversion'
      WHEN eventType = 'Subscribe' THEN 'Subscribe'
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
  DATE_SUB(CURRENT_DATE('{tz_name}'), INTERVAL 1 DAY) AS run_date  -- data as of (T-1)
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
  DATE_SUB(CURRENT_DATE('{tz_name}'), INTERVAL 1 DAY) AS run_date  -- data as of (T-1)
FROM agg a
JOIN user_pkg up USING (userId)
JOIN b2b_user bu USING (userId)
JOIN pkg pk USING (packageName)
""".strip()


def build_total_view_sql(*, b2c_table_fqn: str, b2b_table_fqn: str, view_fqn: str) -> str:
    """
    VIEW รวม B2C + B2B (เลียนแบบ df_total_final ใน notebook):
      • เพิ่มคอลัมน์ version ('B2C'/'B2B')
      • unionByName allowMissingColumns: คอลัมน์ที่มีฝั่งเดียว อีกฝั่งเป็น null
      • fillna('null'): คอลัมน์ STRING ที่เป็น null -> ข้อความ 'null' (คอลัมน์ตัวเลขคง null)
    เป็น VIEW -> อัปเดตตามตารางต้นทางอัตโนมัติ ไม่ต้อง rebuild
    """
    return f"""
CREATE OR REPLACE VIEW `{view_fqn}` AS
SELECT
  'B2C' AS version,
  COALESCE(month_id, 'null') AS month_id,
  COALESCE(week_id, 'null') AS week_id,
  date_id,
  COALESCE(userId, 'null') AS userId,
  COALESCE(packageName, 'null') AS packageName,
  trial_conversion_cnt,
  token_used,
  totalCostThb,
  free_trial_token_used,
  COALESCE(package_1, 'null') AS package_1,
  COALESCE(package_2, 'null') AS package_2,
  'null' AS teamId,
  'null' AS teamName,
  'null' AS companyId,
  'null' AS companyName,
  CAST(NULL AS INT64) AS number_of_user_bin,
  'null' AS company_size_range,
  event_row,
  CAST(NULL AS INT64) AS company_first_event_row,
  CAST(NULL AS INT64) AS team_first_event_row,
  current_package_flag,
  packageId,
  run_date
FROM `{b2c_table_fqn}`
UNION ALL
SELECT
  'B2B' AS version,
  COALESCE(month_id, 'null'),
  COALESCE(week_id, 'null'),
  date_id,
  COALESCE(userId, 'null'),
  COALESCE(packageName, 'null'),
  CAST(NULL AS INT64) AS trial_conversion_cnt,
  token_used,
  totalCostThb,
  CAST(NULL AS INT64) AS free_trial_token_used,
  COALESCE(package_1, 'null'),
  COALESCE(package_2, 'null'),
  COALESCE(teamId, 'null'),
  COALESCE(teamName, 'null'),
  COALESCE(companyId, 'null'),
  COALESCE(companyName, 'null'),
  number_of_user_bin,
  COALESCE(company_size_range, 'null'),
  event_row,
  company_first_event_row,
  team_first_event_row,
  current_package_flag,
  packageId,
  run_date
FROM `{b2b_table_fqn}`
""".strip()


# suffix ของ compat view (ชื่อ field ตรงกับ Looker dashboard เดิมที่มาจาก PySpark)
_COMPAT_SUFFIX = "_compat"


def build_compat_view_sql(*, src_fqn: str, view_fqn: str, has_b2c: bool, has_b2b: bool,
                          has_version: bool) -> str:
    """
    VIEW สำหรับ Looker dashboard เดิม — cast month_id/week_id เป็น Number (INT64)
    ให้ตรง type เดิม. ใช้ชื่อคอลัมน์ valid (snake_case) เพราะ BigQuery/Looker
    ไม่ยอมรับเว้นวรรคในชื่อ field (จะขึ้น "Invalid field name error")
    has_b2c = มีคอลัมน์เฉพาะ B2C (trial_conversion_cnt / free_trial_token_used)
    has_b2b = มีคอลัมน์เฉพาะ B2B (team/company)
    """
    cols: list[str] = ["version"] if has_version else []
    cols += ["SAFE_CAST(month_id AS INT64) AS month_id",
             "SAFE_CAST(week_id AS INT64) AS week_id",
             "date_id", "userId", "packageName"]
    if has_b2c:
        cols.append("trial_conversion_cnt")
    cols.append("token_used")
    cols.append("totalCostThb")
    if has_b2c:
        cols.append("free_trial_token_used")
    cols += ["package_1", "package_2"]
    if has_b2b:
        cols += ["teamId", "teamName", "companyId", "companyName",
                 "number_of_user_bin", "company_size_range"]
    cols.append("event_row")
    if has_b2b:
        cols += ["company_first_event_row", "team_first_event_row"]
    cols += ["current_package_flag", "packageId", "run_date"]
    select_list = ",\n  ".join(cols)
    return f"CREATE OR REPLACE VIEW `{view_fqn}` AS\nSELECT\n  {select_list}\nFROM `{src_fqn}`"


def ensure_compat_views(client: bigquery.Client, cfg) -> None:
    """สร้าง compat view ของ B2C / B2B / Total (ให้ dashboard เดิม swap source ได้ลื่น)."""
    specs = [
        (cfg.bq_b2c_table_fqn, True, False, False),
        (cfg.bq_b2b_agg_fqn, False, True, False),
        (cfg.bq_total_view_fqn, True, True, True),
    ]
    for src, has_b2c, has_b2b, has_version in specs:
        sql = build_compat_view_sql(
            src_fqn=src, view_fqn=src + _COMPAT_SUFFIX,
            has_b2c=has_b2c, has_b2b=has_b2b, has_version=has_version,
        )
        client.query(sql).result()
    log.info("ensured compat views (*%s)", _COMPAT_SUFFIX)


# ═════════════════════════════════════════════════════════════════════
# Summary views สำหรับหน้า "Summary Report" ใน Looker Studio
#
# ทำไมต้องมี: ตาราง user_tracking_* มีแต่ฝั่ง "การใช้งาน" (token/serving cost)
# ยังไม่มีฝั่ง "ธุรกิจ" (ยอดสมัครใหม่ / trial / หมดอายุ / รายได้) และไม่มี
# มิติ aiModel — 2 view นี้เติมช่องว่างนั้น
#
# กฎการกรองต้องตรงกับ user_tracking_* เป๊ะ ไม่งั้นตัวเลขจะไม่ตรงกัน:
#   B2C: ตัด user ที่ isBanned + เฉพาะ package 1,2,3,12
#   B2B: ตัด package 5,7,10,97,98
# Subscribe/MainExpired นับ "ครั้งแรกของแต่ละ subscriptionId" เท่านั้น
# (source มี event ซ้ำต่อ subscription เดียว)
# ═════════════════════════════════════════════════════════════════════
SUMMARY_DAILY_VIEW = "summary_daily"
SUMMARY_MODEL_VIEW = "summary_model_daily"


def _summary_base_cte(b2c_ds: str, b2b_ds: str, project: str) -> str:
    """CTE ร่วม: รวม event B2C+B2B ที่กรองแล้ว + ผูกชื่อ/ราคา package."""
    return f"""
b2c_banned AS (
  SELECT DISTINCT userId FROM `{project}.{b2c_ds}.librechat_users` WHERE isBanned = TRUE
),
-- user B2B ที่ผูก team+company ได้ (user_tracking_b2b ใช้ inner join จึงเห็นเฉพาะกลุ่มนี้)
-- เก็บเป็น flag เพื่อให้ Looker เลือกได้: ดูภาพรวมครบ หรือกรองให้ตรงกับหน้า B2B เดิม
b2b_mapped AS (
  SELECT DISTINCT u.userId
  FROM `{project}.{b2b_ds}.librechat_users` u
  JOIN `{project}.{b2b_ds}.b2b_team` t USING (teamId)
  JOIN `{project}.{b2b_ds}.b2b_company` c USING (companyId)
),
pkg AS (
  SELECT 'B2C' AS version, SAFE_CAST(packageId AS INT64) AS packageId, packageName, priceThb
  FROM `{project}.{b2c_ds}.package_master_v3`
  WHERE SAFE_CAST(packageId AS INT64) IN ({_B2C_PACKAGE_IDS})
  UNION ALL
  SELECT 'B2B', SAFE_CAST(packageId AS INT64), packageName, priceThb
  FROM `{project}.{b2b_ds}.package_master_v3`
  WHERE SAFE_CAST(packageId AS INT64) NOT IN ({_B2B_EXCLUDE_PACKAGE_IDS})
),
evt_raw AS (
  SELECT 'B2C' AS version, e.event_id, e.date_id, e.userId, e.eventType, e.subscriptionId,
         e.eventTimeStamp, e.eggToken, e.totalCostThb, e.aiModel,
         SAFE_CAST(e.packageId AS INT64) AS packageId
  FROM `{project}.{b2c_ds}.user_usage_event` e
  LEFT JOIN b2c_banned b USING (userId)
  WHERE b.userId IS NULL AND SAFE_CAST(e.packageId AS INT64) IN ({_B2C_PACKAGE_IDS})
  UNION ALL
  SELECT 'B2B', e.event_id, e.date_id, e.userId, e.eventType, e.subscriptionId,
         e.eventTimeStamp, e.eggToken, e.totalCostThb, e.aiModel,
         SAFE_CAST(e.packageId AS INT64)
  FROM `{project}.{b2b_ds}.user_usage_event` e
  WHERE SAFE_CAST(e.packageId AS INT64) NOT IN ({_B2B_EXCLUDE_PACKAGE_IDS})
),
-- Subscribe / MainExpired: เก็บเฉพาะครั้งแรกของแต่ละ subscriptionId
lifecycle_first AS (
  SELECT * FROM evt_raw
  WHERE eventType IN ('Subscribe', 'MainExpired')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY version, eventType, subscriptionId ORDER BY eventTimeStamp, event_id
  ) = 1
),
evt AS (
  SELECT * FROM evt_raw WHERE eventType NOT IN ('Subscribe', 'MainExpired')
  UNION ALL SELECT * FROM lifecycle_first
),
fact AS (
  SELECT e.*, p.packageName, p.priceThb,
         IFNULL(p.packageName LIKE 'Free Trial%', FALSE) AS is_trial,
         (e.version = 'B2C' OR m.userId IS NOT NULL) AS has_company_mapping
  FROM evt e
  LEFT JOIN pkg p USING (version, packageId)
  LEFT JOIN b2b_mapped m ON e.version = 'B2B' AND e.userId = m.userId
)"""


def build_summary_daily_sql(*, project: str, b2c_ds: str, b2b_ds: str, view_fqn: str) -> str:
    """
    VIEW สรุปรายวัน — grain: version × date_id × package
    ทุก metric บวกกันข้ามวันได้ ยกเว้น daily_active_users (ตั้งชื่อให้ชัดว่าเป็นราย "วัน")
    ถ้าต้องนับ user แบบไม่ซ้ำข้ามช่วงเวลา ให้ใช้ user_tracking_total (grain ราย user)
    """
    return f"""
CREATE OR REPLACE VIEW `{view_fqn}` AS
WITH{_summary_base_cte(b2c_ds, b2b_ds, project)}
SELECT
  version,
  date_id,
  packageId,
  packageName,
  is_trial,
  has_company_mapping,
  COUNT(DISTINCT IF(eventType LIKE 'Token Used%', userId, NULL)) AS daily_active_users,
  COUNTIF(eventType LIKE 'Token Used%') AS usage_events,
  SUM(IF(eventType LIKE 'Token Used%', ABS(eggToken), 0)) AS tokens_used,
  SUM(IF(eventType LIKE 'Token Used%', totalCostThb, 0)) AS serving_cost_thb,
  COUNTIF(eventType = 'Subscribe') AS new_subscriptions,
  COUNTIF(eventType = 'Subscribe' AND NOT is_trial) AS new_paid_subscriptions,
  COUNTIF(eventType = 'Subscribe' AND is_trial) AS new_trial_subscriptions,
  COUNTIF(eventType = 'MainExpired') AS expirations,
  COUNTIF(eventType = 'MonthlyReset') AS monthly_resets,
  SUM(IF(eventType = 'Subscribe' AND NOT is_trial, priceThb, 0)) AS revenue_thb_est
FROM fact
GROUP BY 1, 2, 3, 4, 5, 6
""".strip()


def build_summary_model_sql(*, project: str, b2c_ds: str, b2b_ds: str, view_fqn: str) -> str:
    """VIEW สรุปการใช้งานตามโมเดล AI — grain: version × date_id × aiModel."""
    return f"""
CREATE OR REPLACE VIEW `{view_fqn}` AS
WITH{_summary_base_cte(b2c_ds, b2b_ds, project)}
SELECT
  version,
  date_id,
  IFNULL(aiModel, '(ไม่ระบุ)') AS aiModel,
  has_company_mapping,
  COUNT(DISTINCT userId) AS daily_active_users,
  COUNT(*) AS usage_events,
  SUM(ABS(eggToken)) AS tokens_used,
  SUM(totalCostThb) AS serving_cost_thb
FROM fact
WHERE eventType LIKE 'Token Used%'
GROUP BY 1, 2, 3, 4
""".strip()


def ensure_summary_views(client: bigquery.Client, cfg) -> None:
    """สร้าง/อัปเดต view สรุปสำหรับหน้า Summary Report ใน Looker."""
    common = dict(project=cfg.gcp_project_id, b2c_ds=cfg.bq_dataset, b2b_ds=cfg.bq_dataset_b2b)
    for builder, name in ((build_summary_daily_sql, SUMMARY_DAILY_VIEW),
                          (build_summary_model_sql, SUMMARY_MODEL_VIEW)):
        client.query(builder(view_fqn=f"{cfg.bq_total_dataset_fqn}.{name}", **common)).result()
    log.info("ensured summary views (%s, %s)", SUMMARY_DAILY_VIEW, SUMMARY_MODEL_VIEW)


# ═════════════════════════════════════════════════════════════════════
# Slide views — ป้อนตัวเลข/กราฟให้ Google Slides deck (โครงต้นไม้ผู้ใช้)
#
# นิยาม: 1 user นับครั้งเดียว จัดกลุ่มตาม "แพ็คเกจล่าสุดที่สมัคร"
#        (Subscribe event ล่าสุด) จึงบวกกันได้ลงตัวแบบเดียวกับสไลด์
#        B2C = Subscriber(Starter+Standard+Pro) + Free Trial
#        B2B = Subscriber(Biz Starter+Standard+Pro) + Others
# ═════════════════════════════════════════════════════════════════════
SLIDE_METRICS_VIEW = "slide_metrics"
SLIDE_COMPANY_SIZE_VIEW = "slide_company_size"
SLIDE_NEW_USERS_VIEW = "slide_new_users_weekly"

_B2B_SUBSCRIBER_PACKAGES = "'Biz Starter', 'Biz Standard', 'Biz Pro'"
_B2C_SUBSCRIBER_PACKAGES = "'Starter', 'Standard', 'Pro'"


def _slide_base_cte(project: str, b2c_ds: str, b2b_ds: str) -> str:
    """CTE ร่วม: subscribe event (ตัดซ้ำต่อ subscription) + แพ็คเกจล่าสุดต่อ user."""
    return f"""
b2c_banned AS (
  SELECT DISTINCT userId FROM `{project}.{b2c_ds}.librechat_users` WHERE isBanned = TRUE
),
pkg AS (
  SELECT 'B2C' AS v, SAFE_CAST(packageId AS INT64) AS pid, packageName
  FROM `{project}.{b2c_ds}.package_master_v3`
  UNION ALL
  SELECT 'B2B', SAFE_CAST(packageId AS INT64), packageName
  FROM `{project}.{b2b_ds}.package_master_v3`
),
sub_raw AS (
  SELECT 'B2C' AS v, e.userId, SAFE_CAST(e.packageId AS INT64) AS pid,
         e.eventTimeStamp, e.date_id, e.subscriptionId, e.event_id
  FROM `{project}.{b2c_ds}.user_usage_event` e
  LEFT JOIN b2c_banned b USING (userId)
  WHERE e.eventType = 'Subscribe' AND b.userId IS NULL
  UNION ALL
  SELECT 'B2B', userId, SAFE_CAST(packageId AS INT64),
         eventTimeStamp, date_id, subscriptionId, event_id
  FROM `{project}.{b2b_ds}.user_usage_event`
  WHERE eventType = 'Subscribe'
    AND SAFE_CAST(packageId AS INT64) NOT IN ({_B2B_EXCLUDE_PACKAGE_IDS})
),
-- 1 แถวต่อ 1 subscription (source มี Subscribe ซ้ำต่อ subscription เดียว)
sub AS (
  SELECT * FROM sub_raw
  QUALIFY ROW_NUMBER() OVER (PARTITION BY v, subscriptionId ORDER BY eventTimeStamp, event_id) = 1
),
-- แพ็คเกจ "ปัจจุบัน" ของแต่ละ user = แพ็คของ subscription ล่าสุด
latest AS (
  SELECT v, userId, pid FROM sub
  QUALIFY ROW_NUMBER() OVER (PARTITION BY v, userId ORDER BY eventTimeStamp DESC, event_id DESC) = 1
),
u AS (
  SELECT l.v, l.userId, p.packageName AS pkg
  FROM latest l JOIN pkg p ON p.v = l.v AND p.pid = l.pid
),
-- ผู้ใช้ B2C ที่เคยได้ trial แล้วต่อมาสมัครแพ็คเสียเงิน = Free Trial Conversion
conv AS (
  SELECT userId, MIN(IF(pid != 12, eventTimeStamp, NULL)) AS first_paid_ts
  FROM sub WHERE v = 'B2C'
  GROUP BY userId
  HAVING COUNTIF(pid = 12) > 0 AND COUNTIF(pid != 12) > 0
),
conv_pkg AS (
  SELECT c.userId, p.packageName AS to_pkg
  FROM conv c
  JOIN sub s ON s.v = 'B2C' AND s.userId = c.userId AND s.eventTimeStamp = c.first_paid_ts
  JOIN pkg p ON p.v = 'B2C' AND p.pid = s.pid
)"""


def build_slide_metrics_sql(*, project: str, b2c_ds: str, b2b_ds: str,
                            view_fqn: str, tz_name: str) -> str:
    """
    VIEW แบบ long format: 1 แถว = 1 ตัวเลขบนสไลด์
    คอลัมน์ metric_key ใช้เป็นชื่อ placeholder ในสไลด์ (เช่น {{total_users}})
    """
    return f"""
CREATE OR REPLACE VIEW `{view_fqn}` AS
WITH{_slide_base_cte(project, b2c_ds, b2b_ds)},
m AS (
  SELECT 'total_users' AS metric_key, 'ผู้ใช้ทั้งหมด' AS metric_label,
         (SELECT COUNT(*) FROM u) AS value_num, 1 AS sort_order
  UNION ALL SELECT 'b2c_users', 'ผู้ใช้ B2C', (SELECT COUNTIF(v='B2C') FROM u), 2
  UNION ALL SELECT 'b2b_users', 'ผู้ใช้ B2B', (SELECT COUNTIF(v='B2B') FROM u), 3
  -- B2C
  UNION ALL SELECT 'b2c_subscriber', 'B2C ผู้สมัครแบบเสียเงิน',
    (SELECT COUNTIF(v='B2C' AND pkg IN ({_B2C_SUBSCRIBER_PACKAGES})) FROM u), 10
  UNION ALL SELECT 'b2c_starter', 'B2C Starter',
    (SELECT COUNTIF(v='B2C' AND pkg='Starter') FROM u), 11
  UNION ALL SELECT 'b2c_standard', 'B2C Standard',
    (SELECT COUNTIF(v='B2C' AND pkg='Standard') FROM u), 12
  UNION ALL SELECT 'b2c_pro', 'B2C Pro',
    (SELECT COUNTIF(v='B2C' AND pkg='Pro') FROM u), 13
  UNION ALL SELECT 'b2c_free_trial', 'B2C Free Trial',
    (SELECT COUNTIF(v='B2C' AND pkg NOT IN ({_B2C_SUBSCRIBER_PACKAGES})) FROM u), 14
  -- Free Trial Conversion
  UNION ALL SELECT 'free_trial_conversion', 'Free Trial Conversion',
    (SELECT COUNT(*) FROM conv_pkg), 20
  UNION ALL SELECT 'to_starter', 'แปลงเป็น Starter',
    (SELECT COUNTIF(to_pkg='Starter') FROM conv_pkg), 21
  UNION ALL SELECT 'to_standard', 'แปลงเป็น Standard',
    (SELECT COUNTIF(to_pkg='Standard') FROM conv_pkg), 22
  UNION ALL SELECT 'to_pro', 'แปลงเป็น Pro',
    (SELECT COUNTIF(to_pkg='Pro') FROM conv_pkg), 23
  -- B2B
  UNION ALL SELECT 'b2b_subscriber', 'B2B ผู้สมัครแบบเสียเงิน',
    (SELECT COUNTIF(v='B2B' AND pkg IN ({_B2B_SUBSCRIBER_PACKAGES})) FROM u), 30
  UNION ALL SELECT 'b2b_biz_starter', 'B2B Biz Starter',
    (SELECT COUNTIF(v='B2B' AND pkg='Biz Starter') FROM u), 31
  UNION ALL SELECT 'b2b_biz_standard', 'B2B Biz Standard',
    (SELECT COUNTIF(v='B2B' AND pkg='Biz Standard') FROM u), 32
  UNION ALL SELECT 'b2b_biz_pro', 'B2B Biz Pro',
    (SELECT COUNTIF(v='B2B' AND pkg='Biz Pro') FROM u), 33
  UNION ALL SELECT 'b2b_others', 'B2B อื่น ๆ (trial/ภายใน)',
    (SELECT COUNTIF(v='B2B' AND pkg NOT IN ({_B2B_SUBSCRIBER_PACKAGES})) FROM u), 34
  -- บริษัท
  UNION ALL SELECT 'b2b_companies', 'จำนวนบริษัท B2B',
    (SELECT COUNT(DISTINCT companyId) FROM `{project}.{b2b_ds}.user_tracking_b2b`), 40
)
SELECT
  metric_key, metric_label, value_num,
  FORMAT("%'d", value_num) AS value_display,
  sort_order,
  (SELECT MAX(date_id) FROM sub) AS data_end,
  FORMAT_DATE('%e %B %Y', (SELECT MAX(date_id) FROM sub)) AS period_display,
  CURRENT_TIMESTAMP() AS generated_at
FROM m
ORDER BY sort_order
""".strip()


def build_slide_company_size_sql(*, project: str, b2b_ds: str, view_fqn: str) -> str:
    """VIEW กราฟ '# Company by company size' — 1 แถว = 1 ช่วงขนาดบริษัท."""
    return f"""
CREATE OR REPLACE VIEW `{view_fqn}` AS
SELECT
  company_size_range,
  MIN(number_of_user_bin) AS bin_order,
  COUNT(DISTINCT companyId) AS companies,
  COUNT(DISTINCT userId) AS users
FROM `{project}.{b2b_ds}.user_tracking_b2b`
GROUP BY company_size_range
ORDER BY bin_order
""".strip()


def build_slide_new_users_sql(*, project: str, b2c_ds: str, b2b_ds: str,
                              view_fqn: str) -> str:
    """
    VIEW กราฟ 'New User' รายสัปดาห์ + ยอดสะสม (แยก B2C/B2B)
    นับผู้ใช้ใหม่จากสัปดาห์ที่ subscribe ครั้งแรก
    """
    return f"""
CREATE OR REPLACE VIEW `{view_fqn}` AS
WITH{_slide_base_cte(project, b2c_ds, b2b_ds)},
first_sub AS (
  SELECT v, userId, MIN(date_id) AS first_date FROM sub GROUP BY 1, 2
),
wk AS (
  SELECT v AS segment,
         DATE_TRUNC(first_date, WEEK(MONDAY)) AS week_start,
         COUNT(*) AS new_users
  FROM first_sub GROUP BY 1, 2
)
SELECT
  segment,
  week_start,
  FORMAT_DATE('%G%V', week_start) AS week_id,
  new_users,
  SUM(new_users) OVER (PARTITION BY segment ORDER BY week_start) AS new_users_cumulative
FROM wk
ORDER BY segment, week_start
""".strip()


def ensure_slide_views(client: bigquery.Client, cfg) -> None:
    """สร้าง/อัปเดต view ที่ป้อนตัวเลขและกราฟให้ Google Slides deck."""
    ds = cfg.bq_total_dataset_fqn
    client.query(build_slide_metrics_sql(
        project=cfg.gcp_project_id, b2c_ds=cfg.bq_dataset, b2b_ds=cfg.bq_dataset_b2b,
        view_fqn=f"{ds}.{SLIDE_METRICS_VIEW}", tz_name=cfg.timezone_name)).result()
    client.query(build_slide_company_size_sql(
        project=cfg.gcp_project_id, b2b_ds=cfg.bq_dataset_b2b,
        view_fqn=f"{ds}.{SLIDE_COMPANY_SIZE_VIEW}")).result()
    client.query(build_slide_new_users_sql(
        project=cfg.gcp_project_id, b2c_ds=cfg.bq_dataset, b2b_ds=cfg.bq_dataset_b2b,
        view_fqn=f"{ds}.{SLIDE_NEW_USERS_VIEW}")).result()
    log.info("ensured slide views (%s, %s, %s)",
             SLIDE_METRICS_VIEW, SLIDE_COMPANY_SIZE_VIEW, SLIDE_NEW_USERS_VIEW)


def ensure_total_view(client: bigquery.Client, cfg) -> None:
    """สร้าง/อัปเดต VIEW รวม B2C+B2B."""
    sql = build_total_view_sql(
        b2c_table_fqn=cfg.bq_b2c_table_fqn,
        b2b_table_fqn=cfg.bq_b2b_agg_fqn,
        view_fqn=cfg.bq_total_view_fqn,
    )
    client.query(sql).result()
    log.info("ensured total view %s", cfg.bq_total_view_fqn)


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


# ═════════════════════════════════════════════════════════════════════
# Repeat subscribe analysis (B2C เท่านั้น)
#
# Event semantics (ยืนยันจากข้อมูลจริง + เจ้าของระบบ):
#   Subscribe     : ซื้อ (subscriptionId ใหม่), eggToken บวก = quota; ซื้อซ้ำ = reset balance
#   Token Used(*) : ใช้ token, eggToken ลบ (รวม 'Token Used (Unsettled)')
#   MonthlyReset  : recurring เติมรอบเดือน (id เดิม), eggToken = 0 -> quota อิง grant ล่าสุด
#   MainExpired   : หมดอายุไม่ต่อ, eggToken ลบ = claw back token ที่เหลือ (= leftover จริง)
#
# Cycle = ช่วงชีวิต token 1 ก้อน: เริ่มที่ Subscribe/MonthlyReset
# จบที่ boundary ถัดไป (Subscribe=repurchase / MonthlyReset=monthly_reset /
# MainExpired=expired) หรือยังไม่จบ (active = censored)
# ═════════════════════════════════════════════════════════════════════

# threshold "ใช้หมด" (กันเศษ ไม่ใช้ 100% เป๊ะ)
EXHAUST_THRESHOLD = 0.95
# paid packages ของ B2C (trial = 12 แยกวิเคราะห์ ไม่นับใน repeat)
_B2C_PAID_PACKAGE_IDS = "1, 2, 3"


def build_token_cycle_sql(
    *,
    event_table_fqn: str,
    package_table_fqn: str,
    users_table_fqn: str,
    cycle_table_fqn: str,
    start_date: date,
    tz_name: str,
) -> str:
    """ตาราง user_token_cycle: grain = user × cycle (ช่วงชีวิต token 1 ก้อน)."""
    return f"""
CREATE OR REPLACE TABLE `{cycle_table_fqn}`
PARTITION BY DATE(cycle_start_ts)
CLUSTER BY userId AS
WITH
banned AS (
  SELECT DISTINCT userId FROM `{users_table_fqn}` WHERE isBanned = TRUE
),
pkg AS (
  SELECT DISTINCT SAFE_CAST(packageId AS INT64) AS packageId, packageName
  FROM `{package_table_fqn}`
  WHERE SAFE_CAST(packageId AS INT64) IN ({_B2C_PACKAGE_IDS})
),
evt AS (
  SELECT e.event_id, e.userId, e.eventType, e.subscriptionId,
         SAFE_CAST(e.packageId AS INT64) AS packageId,
         e.eggToken, e.eventTimeStamp, e.totalCostThb
  FROM `{event_table_fqn}` e
  LEFT JOIN banned b USING (userId)
  WHERE b.userId IS NULL
    AND SAFE_CAST(e.packageId AS INT64) IN ({_B2C_PACKAGE_IDS})
    AND e.date_id >= DATE '{start_date.isoformat()}'
    AND e.date_id < CURRENT_DATE('{tz_name}')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.event_id ORDER BY e.eventTimeStamp) = 1
),
-- grant ล่าสุดต่อ user (เป็น quota ของ cycle ที่เริ่มด้วย MonthlyReset ซึ่ง eggToken=0)
evt_ctx AS (
  SELECT *,
    LAST_VALUE(IF(eventType = 'Subscribe', eggToken, NULL) IGNORE NULLS)
      OVER (PARTITION BY userId ORDER BY eventTimeStamp, event_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_grant
  FROM evt
),
boundary AS (
  SELECT userId, event_id, eventTimeStamp, eventType, subscriptionId, packageId, eggToken, last_grant
  FROM evt_ctx
  WHERE eventType IN ('Subscribe', 'MonthlyReset', 'MainExpired')
  -- กัน duplicate boundary เวลาเดียวกัน (สร้าง cycle ความยาว 0)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY userId, eventTimeStamp, eventType ORDER BY event_id) = 1
),
seq AS (
  SELECT *,
    LEAD(eventTimeStamp) OVER w AS next_ts,
    LEAD(eventType) OVER w AS next_type,
    LEAD(eggToken) OVER w AS next_egg
  FROM boundary
  WINDOW w AS (PARTITION BY userId ORDER BY eventTimeStamp, event_id)
),
cyc AS (
  SELECT
    userId, subscriptionId, packageId,
    eventTimeStamp AS cycle_start_ts,
    eventType AS start_type,
    IF(eventType = 'Subscribe', eggToken, last_grant) AS quota,
    next_ts AS cycle_end_ts,
    CASE next_type
      WHEN 'Subscribe' THEN 'repurchase'
      WHEN 'MonthlyReset' THEN 'monthly_reset'
      WHEN 'MainExpired' THEN 'expired'
      ELSE 'active'
    END AS end_type,
    -- MainExpired claw back token ที่เหลือ -> |eggToken| = leftover จริง ณ วันหมดอายุ
    IF(next_type = 'MainExpired', ABS(next_egg), NULL) AS clawback_leftover
  FROM seq
  WHERE eventType IN ('Subscribe', 'MonthlyReset')
),
-- การใช้ token ภายใน cycle + cumulative เพื่อหาจุด "ใช้หมด"
use_evt AS (
  SELECT c.userId, c.cycle_start_ts, c.quota,
         u.eventTimeStamp, ABS(u.eggToken) AS used, u.totalCostThb,
         SUM(ABS(u.eggToken)) OVER (
           PARTITION BY c.userId, c.cycle_start_ts
           ORDER BY u.eventTimeStamp, u.event_id
         ) AS cum_used
  FROM cyc c
  JOIN evt u
    ON u.userId = c.userId
   AND u.eventType LIKE 'Token Used%'
   AND u.eventTimeStamp >= c.cycle_start_ts
   AND (c.cycle_end_ts IS NULL OR u.eventTimeStamp < c.cycle_end_ts)
),
use_agg AS (
  SELECT userId, cycle_start_ts,
         SUM(used) AS consumed,
         SUM(totalCostThb) AS serving_cost_thb,
         COUNT(*) AS usage_event_cnt,
         MIN(IF(cum_used >= {EXHAUST_THRESHOLD} * quota, eventTimeStamp, NULL)) AS exhaust_ts
  FROM use_evt
  GROUP BY 1, 2
)
SELECT
  c.userId,
  c.subscriptionId,
  c.packageId,
  p.packageName,
  (c.packageId = 12) AS is_trial,
  c.cycle_start_ts,
  c.start_type,
  c.cycle_end_ts,
  c.end_type,
  ROW_NUMBER() OVER (PARTITION BY c.userId ORDER BY c.cycle_start_ts) AS cycle_index,
  c.quota,
  IFNULL(u.consumed, 0) AS consumed,
  IFNULL(u.usage_event_cnt, 0) AS usage_event_cnt,
  SAFE_DIVIDE(IFNULL(u.consumed, 0), c.quota) AS utilization,
  IFNULL(IFNULL(u.consumed, 0) >= {EXHAUST_THRESHOLD} * c.quota, FALSE) AS exhausted,
  u.exhaust_ts,
  ROUND(TIMESTAMP_DIFF(u.exhaust_ts, c.cycle_start_ts, HOUR) / 24.0, 2) AS days_to_exhaust,
  COALESCE(c.clawback_leftover, GREATEST(c.quota - IFNULL(u.consumed, 0), 0)) AS leftover_at_end,
  ROUND(TIMESTAMP_DIFF(c.cycle_end_ts, c.cycle_start_ts, HOUR) / 24.0, 2) AS cycle_days,
  IFNULL(u.serving_cost_thb, 0) AS serving_cost_thb,
  CASE
    WHEN c.end_type = 'active' THEN 'C0_active'
    WHEN c.quota IS NULL THEN 'C9_unknown_quota'
    WHEN IFNULL(u.consumed, 0) >= {EXHAUST_THRESHOLD} * c.quota AND c.end_type = 'repurchase'
      THEN 'A1_exhausted_repurchase'
    WHEN IFNULL(u.consumed, 0) >= {EXHAUST_THRESHOLD} * c.quota AND c.end_type = 'expired'
      THEN 'A2_exhausted_churn'
    WHEN IFNULL(u.consumed, 0) >= {EXHAUST_THRESHOLD} * c.quota AND c.end_type = 'monthly_reset'
      THEN 'A3_exhausted_wait_reset'
    WHEN c.end_type = 'repurchase' THEN 'B1_underuse_repurchase'
    WHEN c.end_type = 'expired' THEN 'B2_underuse_churn'
    ELSE 'B3_underuse_continue'
  END AS segment
FROM cyc c
LEFT JOIN use_agg u ON u.userId = c.userId AND u.cycle_start_ts = c.cycle_start_ts
LEFT JOIN pkg p ON p.packageId = c.packageId
""".strip()


def build_repeat_behavior_sql(
    *,
    cycle_table_fqn: str,
    event_table_fqn: str,
    package_table_fqn: str,
    users_table_fqn: str,
    repeat_table_fqn: str,
    start_date: date,
    tz_name: str,
    mature_days: int = 35,
) -> str:
    """
    ตาราง user_repeat_behavior: grain = user (roll-up จาก cycle + subscribe events)
    repeat = มี paid Subscribe (packageId 1,2,3) ตั้งแต่ 2 subscription ขึ้นไป
    (trial -> paid ครั้งแรก = conversion ไม่ใช่ repeat)
    """
    return f"""
CREATE OR REPLACE TABLE `{repeat_table_fqn}` AS
WITH
banned AS (
  SELECT DISTINCT userId FROM `{users_table_fqn}` WHERE isBanned = TRUE
),
pkg AS (
  SELECT DISTINCT SAFE_CAST(packageId AS INT64) AS packageId, packageName, priceThb
  FROM `{package_table_fqn}`
  WHERE SAFE_CAST(packageId AS INT64) IN ({_B2C_PACKAGE_IDS})
),
evt AS (
  SELECT e.event_id, e.userId, e.eventType, e.subscriptionId,
         SAFE_CAST(e.packageId AS INT64) AS packageId,
         e.eggToken, e.eventTimeStamp, e.totalCostThb, e.date_id
  FROM `{event_table_fqn}` e
  LEFT JOIN banned b USING (userId)
  WHERE b.userId IS NULL
    AND SAFE_CAST(e.packageId AS INT64) IN ({_B2C_PACKAGE_IDS})
    AND e.date_id >= DATE '{start_date.isoformat()}'
    AND e.date_id < CURRENT_DATE('{tz_name}')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.event_id ORDER BY e.eventTimeStamp) = 1
),
asof AS (SELECT MAX(date_id) AS data_end FROM evt),
-- paid subscriptions: 1 แถวต่อ subscriptionId (กัน Subscribe ซ้ำใน sub เดียว)
paid_sub_raw AS (
  SELECT userId, subscriptionId, packageId, eventTimeStamp
  FROM evt
  WHERE eventType = 'Subscribe' AND packageId IN ({_B2C_PAID_PACKAGE_IDS})
  QUALIFY ROW_NUMBER() OVER (PARTITION BY userId, subscriptionId ORDER BY eventTimeStamp) = 1
),
paid_sub AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY userId ORDER BY eventTimeStamp) AS sub_seq,
    LAG(eventTimeStamp) OVER (PARTITION BY userId ORDER BY eventTimeStamp) AS prev_ts
  FROM paid_sub_raw
),
paid_agg AS (
  SELECT userId,
    COUNT(*) AS paid_subscribe_cnt,
    MIN(eventTimeStamp) AS first_paid_ts,
    MAX(eventTimeStamp) AS last_paid_ts,
    ROUND(AVG(IF(sub_seq > 1, TIMESTAMP_DIFF(eventTimeStamp, prev_ts, HOUR) / 24.0, NULL)), 1)
      AS avg_days_between_paid
  FROM paid_sub GROUP BY 1
),
first_pkg AS (
  SELECT s.userId, s.packageId AS first_paid_packageId, p.packageName AS first_paid_packageName
  FROM paid_sub s JOIN pkg p USING (packageId)
  WHERE s.sub_seq = 1
),
revenue AS (
  -- ประมาณการจาก list price (priceThb) ของแพ็คที่ซื้อ
  SELECT s.userId, SUM(p.priceThb) AS revenue_thb
  FROM paid_sub s JOIN pkg p USING (packageId)
  GROUP BY 1
),
trial AS (
  SELECT userId, COUNT(DISTINCT subscriptionId) AS trial_cnt
  FROM evt WHERE eventType = 'Subscribe' AND packageId = 12
  GROUP BY 1
),
cost AS (
  SELECT userId,
         SUM(totalCostThb) AS serving_cost_thb,
         SUM(ABS(eggToken)) AS tokens_used_total
  FROM evt WHERE eventType LIKE 'Token Used%'
  GROUP BY 1
),
cyc AS (SELECT * FROM `{cycle_table_fqn}`),
cyc_flags AS (
  SELECT *,
    LAG(end_type) OVER (PARTITION BY userId ORDER BY cycle_start_ts) AS prev_end_type,
    ROW_NUMBER() OVER (PARTITION BY userId ORDER BY cycle_start_ts DESC) AS rn_last
  FROM cyc
),
cyc_agg AS (
  SELECT userId,
    COUNT(*) AS cycles_total,
    COUNTIF(end_type != 'active') AS cycles_completed,
    COUNTIF(NOT is_trial AND end_type != 'active') AS paid_cycles_completed,
    COUNTIF(NOT is_trial AND end_type != 'active' AND exhausted) AS paid_cycles_exhausted,
    LOGICAL_OR(exhausted) AS ever_exhausted,
    ROUND(AVG(IF(NOT is_trial AND end_type != 'active', utilization, NULL)), 4) AS avg_utilization_paid,
    ROUND(AVG(IF(NOT is_trial AND end_type = 'repurchase', leftover_at_end, NULL)), 0) AS avg_leftover_at_repurchase,
    COUNTIF(NOT is_trial AND segment = 'A1_exhausted_repurchase') AS exhausted_repurchase_cnt,
    COUNTIF(NOT is_trial AND segment = 'A2_exhausted_churn') AS exhausted_churn_cnt,
    COUNTIF(NOT is_trial AND segment = 'A3_exhausted_wait_reset') AS exhausted_wait_reset_cnt,
    COUNTIF(start_type = 'Subscribe' AND prev_end_type = 'expired') AS winback_cnt,
    MAX(IF(rn_last = 1, end_type, NULL)) AS last_cycle_end_type,
    ARRAY_AGG(IF(end_type = 'expired', leftover_at_end, NULL) IGNORE NULLS
              ORDER BY cycle_start_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_leftover_at_churn
  FROM cyc_flags GROUP BY 1
),
seg_mode AS (
  SELECT userId, segment AS dominant_segment FROM (
    SELECT userId, segment,
      ROW_NUMBER() OVER (PARTITION BY userId ORDER BY COUNT(*) DESC, segment) AS rn
    FROM cyc
    WHERE end_type != 'active' AND NOT is_trial AND segment != 'C9_unknown_quota'
    GROUP BY userId, segment
  ) WHERE rn = 1
)
SELECT
  c.userId,
  IFNULL(pa.paid_subscribe_cnt, 0) AS paid_subscribe_cnt,
  IFNULL(t.trial_cnt, 0) AS trial_cnt,
  CASE
    WHEN IFNULL(pa.paid_subscribe_cnt, 0) >= 2 THEN 'repeat'
    WHEN IFNULL(pa.paid_subscribe_cnt, 0) = 1 THEN 'paid_once'
    WHEN IFNULL(t.trial_cnt, 0) >= 1 THEN 'trial_only'
    ELSE 'other'
  END AS user_type,
  (IFNULL(pa.paid_subscribe_cnt, 0) >= 2) AS is_repeat,
  DATE(pa.first_paid_ts, '{tz_name}') AS first_paid_date,
  DATE(pa.last_paid_ts, '{tz_name}') AS last_paid_date,
  -- cohort ที่ "แก่พอ" จะตัดสิน repeat ได้ (ซื้อครั้งแรกมาแล้ว >= {mature_days} วัน)
  IFNULL(DATE(pa.first_paid_ts, '{tz_name}') <= DATE_SUB(a.data_end, INTERVAL {mature_days} DAY), FALSE)
    AS is_mature_cohort,
  pa.avg_days_between_paid,
  fp.first_paid_packageId,
  fp.first_paid_packageName,
  c.cycles_total,
  c.cycles_completed,
  c.paid_cycles_completed,
  c.paid_cycles_exhausted,
  IFNULL(c.ever_exhausted, FALSE) AS ever_exhausted,
  c.avg_utilization_paid,
  c.avg_leftover_at_repurchase,
  c.exhausted_repurchase_cnt,
  c.exhausted_churn_cnt,
  c.exhausted_wait_reset_cnt,
  IFNULL(c.winback_cnt, 0) AS winback_cnt,
  (c.last_cycle_end_type = 'expired') AS churned,
  c.last_leftover_at_churn,
  sm.dominant_segment,
  IFNULL(r.revenue_thb, 0) AS revenue_thb,
  IFNULL(co.serving_cost_thb, 0) AS serving_cost_thb,
  IFNULL(r.revenue_thb, 0) - IFNULL(co.serving_cost_thb, 0) AS margin_thb,
  IFNULL(co.tokens_used_total, 0) AS tokens_used_total,
  a.data_end
FROM cyc_agg c
CROSS JOIN asof a
LEFT JOIN paid_agg pa USING (userId)
LEFT JOIN trial t USING (userId)
LEFT JOIN first_pkg fp USING (userId)
LEFT JOIN revenue r USING (userId)
LEFT JOIN cost co USING (userId)
LEFT JOIN seg_mode sm USING (userId)
""".strip()


def run_repeat_aggregates(client: bigquery.Client, cfg) -> tuple[int, int]:
    """rebuild ตาราง repeat-analysis 2 ตัว (cycle ก่อน แล้ว behavior อ่านจาก cycle)."""
    cycle_sql = build_token_cycle_sql(
        event_table_fqn=cfg.bq_table_fqn,
        package_table_fqn=cfg.bq_package_table_fqn,
        users_table_fqn=cfg.bq_users_table_fqn,
        cycle_table_fqn=cfg.bq_cycle_table_fqn,
        start_date=cfg.start_date,
        tz_name=cfg.timezone_name,
    )
    client.query(cycle_sql).result()
    cycle_rows = client.get_table(cfg.bq_cycle_table_fqn).num_rows
    log.info("rebuilt %s -> %d rows", cfg.bq_cycle_table_fqn, cycle_rows)

    behavior_sql = build_repeat_behavior_sql(
        cycle_table_fqn=cfg.bq_cycle_table_fqn,
        event_table_fqn=cfg.bq_table_fqn,
        package_table_fqn=cfg.bq_package_table_fqn,
        users_table_fqn=cfg.bq_users_table_fqn,
        repeat_table_fqn=cfg.bq_repeat_table_fqn,
        start_date=cfg.start_date,
        tz_name=cfg.timezone_name,
    )
    client.query(behavior_sql).result()
    repeat_rows = client.get_table(cfg.bq_repeat_table_fqn).num_rows
    log.info("rebuilt %s -> %d rows", cfg.bq_repeat_table_fqn, repeat_rows)
    return cycle_rows, repeat_rows
