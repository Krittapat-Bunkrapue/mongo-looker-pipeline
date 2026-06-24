"""
load.py
───────
เขียน DataFrame ลง BigQuery แบบ idempotent

กลยุทธ์ idempotency: โหลดเข้า partition decorator `table$YYYYMMDD` ด้วย
WRITE_TRUNCATE -> BigQuery จะ "แทนที่ทั้ง partition ของวันนั้นแบบ atomic"
(เทียบเท่า delete+insert ของทั้งวัน แต่ atomic กว่า ไม่มีช่วง partition ว่าง)
=> รันซ้ำวันเดิมกี่ครั้งข้อมูลก็ไม่ซ้ำ/ไม่หาย

หมายเหตุ: ทุกแถวที่โหลดเข้า partition decorator ต้องมี date_id ตรงกับ
วันของ decorator ไม่งั้น BigQuery จะ reject (เป็น guard ในตัว)
"""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

log = logging.getLogger("pipeline.load")

# ─────────────────────────────────────────────────────────────────────
# SCHEMA — แหล่งความจริงเดียว (single source of truth) ของตารางปลายทาง
# transform.py import ค่านี้ไปใช้ validate ว่า DataFrame ตรง schema
# ลำดับ/ชื่อคอลัมน์ต้องตรงกับที่ transform สร้าง
# ─────────────────────────────────────────────────────────────────────
TABLE_SCHEMA: list[bigquery.SchemaField] = [
    # _id ของ Mongo -> event_id (กัน prefix สงวนของ BQ + ใช้ใน Looker ง่าย)
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("eventTimeStamp", "TIMESTAMP", mode="REQUIRED"),  # เก็บเป็น UTC
    bigquery.SchemaField("date_id", "DATE", mode="REQUIRED"),              # Asia/Bangkok
    bigquery.SchemaField("userId", "STRING"),
    bigquery.SchemaField("eventType", "STRING"),
    bigquery.SchemaField("subscriptionId", "STRING"),
    bigquery.SchemaField("packageId", "STRING"),
    bigquery.SchemaField("eggToken", "INTEGER"),
    bigquery.SchemaField("chatToken", "INTEGER"),
    bigquery.SchemaField("websearchToken", "INTEGER"),
    bigquery.SchemaField("totalCostUsd", "NUMERIC"),
    bigquery.SchemaField("chatCostUsd", "NUMERIC"),
    bigquery.SchemaField("websearchCostUsd", "NUMERIC"),
    bigquery.SchemaField("externalToken", "INTEGER"),
    bigquery.SchemaField("externalCostUsd", "NUMERIC"),
    bigquery.SchemaField("externalCostName", "STRING"),
    bigquery.SchemaField("externalTransactionReference", "STRING"),
    bigquery.SchemaField("traceId", "STRING"),
    bigquery.SchemaField("aiModel", "STRING"),
    bigquery.SchemaField("agentId", "STRING"),
    bigquery.SchemaField("teamId", "STRING"),
    bigquery.SchemaField("deductType", "STRING"),
    bigquery.SchemaField("teamSubscriptionId", "STRING"),
    bigquery.SchemaField("deductionBreakdown", "STRING"),  # JSON-serialized (อาจ null)
    # ── derived ──
    bigquery.SchemaField("totalCostThb", "NUMERIC", mode="REQUIRED"),
    # ── load metadata ──
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

# ชื่อคอลัมน์ตามลำดับ schema (ให้ transform ใช้ validate + จัดเรียง)
COLUMN_NAMES: list[str] = [f.name for f in TABLE_SCHEMA]

# คอลัมน์ที่ใช้ partition / cluster
PARTITION_FIELD = "date_id"
CLUSTERING_FIELDS = ["userId", "eventType"]

# watermark table schema
STATE_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("pipeline_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("last_processed_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]

# ─────────────────────────────────────────────────────────────────────
# package_master_v3 — master/reference table (lean) เก็บแบบ full reload
# ─────────────────────────────────────────────────────────────────────
PACKAGE_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("packageId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("packageName", "STRING"),
    bigquery.SchemaField("packageType", "STRING"),
    bigquery.SchemaField("tierName", "STRING"),
    bigquery.SchemaField("priceThb", "NUMERIC"),
    bigquery.SchemaField("eggToken", "INTEGER"),
    bigquery.SchemaField("durationDay", "INTEGER"),
    bigquery.SchemaField("durationMonth", "INTEGER"),
    bigquery.SchemaField("createdAt", "TIMESTAMP"),
    bigquery.SchemaField("updatedAt", "TIMESTAMP"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
]
PACKAGE_COLUMN_NAMES: list[str] = [f.name for f in PACKAGE_SCHEMA]

# ─────────────────────────────────────────────────────────────────────
# Librechat.users — เก็บแค่ field ที่ใช้ตัด user ที่โดน ban (full reload)
# ─────────────────────────────────────────────────────────────────────
USERS_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("userId", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("isBanned", "BOOLEAN"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
]
USERS_COLUMN_NAMES: list[str] = [f.name for f in USERS_SCHEMA]

# ─────────────────────────────────────────────────────────────────────
# B2B master tables (full reload): users (team/company), company, team
# ─────────────────────────────────────────────────────────────────────
B2B_USERS_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("userId", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("teamId", "STRING"),
    bigquery.SchemaField("teamName", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
]
B2B_USERS_COLUMN_NAMES: list[str] = [f.name for f in B2B_USERS_SCHEMA]

B2B_COMPANY_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("companyId", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("companyName", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
]
B2B_COMPANY_COLUMN_NAMES: list[str] = [f.name for f in B2B_COMPANY_SCHEMA]

B2B_TEAM_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("teamId", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("companyId", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
]
B2B_TEAM_COLUMN_NAMES: list[str] = [f.name for f in B2B_TEAM_SCHEMA]


def partition_decorator(table_fqn: str, day: date) -> str:
    """คืน 'project.dataset.table$YYYYMMDD' สำหรับ load เข้า partition เดียว."""
    return f"{table_fqn}${day:%Y%m%d}"


def ensure_dataset(client: bigquery.Client, dataset_fqn: str, location: str) -> None:
    try:
        client.get_dataset(dataset_fqn)
    except NotFound:
        ds = bigquery.Dataset(dataset_fqn)
        ds.location = location
        client.create_dataset(ds, exists_ok=True)
        log.info("created dataset %s (%s)", dataset_fqn, location)


def ensure_main_table(client: bigquery.Client, table_fqn: str) -> None:
    """สร้างตารางหลัก (partition by date_id, cluster) ถ้ายังไม่มี."""
    try:
        client.get_table(table_fqn)
        return
    except NotFound:
        pass
    table = bigquery.Table(table_fqn, schema=TABLE_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=PARTITION_FIELD,
    )
    table.clustering_fields = CLUSTERING_FIELDS
    client.create_table(table, exists_ok=True)
    log.info("created table %s (partition=%s cluster=%s)",
             table_fqn, PARTITION_FIELD, CLUSTERING_FIELDS)


def ensure_state_table(client: bigquery.Client, state_table_fqn: str) -> None:
    try:
        client.get_table(state_table_fqn)
    except NotFound:
        client.create_table(bigquery.Table(state_table_fqn, schema=STATE_SCHEMA), exists_ok=True)
        log.info("created state table %s", state_table_fqn)


def validate_dataframe(df: pd.DataFrame, day: date) -> None:
    """
    Guard ก่อนเขียน BQ (กัน schema drift + กันข้อมูลผิด partition):
      • คอลัมน์ต้องตรง schema เป๊ะ
      • ทุกแถวต้องมี date_id == day (ไม่งั้น partition decorator จะ reject)
    """
    actual = list(df.columns)
    if actual != COLUMN_NAMES:
        missing = set(COLUMN_NAMES) - set(actual)
        extra = set(actual) - set(COLUMN_NAMES)
        raise ValueError(
            f"schema drift: columns ไม่ตรง schema. ขาด={sorted(missing)} เกิน={sorted(extra)}"
        )
    if df.empty:
        return
    bad = df.loc[df["date_id"] != day]
    if not bad.empty:
        raise ValueError(
            f"พบ {len(bad)} แถวที่ date_id != {day} — โหลดเข้า partition {day} ไม่ได้"
        )


def write_day(
    client: bigquery.Client,
    table_fqn: str,
    df: pd.DataFrame,
    day: date,
) -> int:
    """
    เขียนข้อมูล "ทั้งวัน" ของ day ลง partition แบบ atomic replace
    คืนจำนวนแถวที่เขียน

    df ว่าง = วันนั้นไม่มี event -> ยังต้อง truncate partition ให้ว่าง
    (กันกรณีก่อนหน้านี้เคยมีข้อมูลผิดค้างอยู่)
    """
    validate_dataframe(df, day)

    # โหลดเข้า partition decorator (table$YYYYMMDD) -> ไม่ต้องระบุ time_partitioning ซ้ำ
    # (ตารางกำหนด partitioning ไว้แล้ว การระบุซ้ำอาจชน "incompatible partitioning spec")
    job_config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    target = partition_decorator(table_fqn, day)
    load_job = client.load_table_from_dataframe(df, target, job_config=job_config)
    load_job.result()  # รอจบ + raise ถ้า error
    log.info("wrote %d rows -> %s", len(df), target)
    return len(df)


def write_full_table(
    client: bigquery.Client,
    table_fqn: str,
    df: pd.DataFrame,
    schema: list[bigquery.SchemaField],
) -> int:
    """
    เขียนทับทั้งตารางแบบ atomic (WRITE_TRUNCATE) — ใช้กับ master table ที่ full reload
    สร้างตารางให้อัตโนมัติถ้ายังไม่มี (ตาม schema ที่ส่งมา)
    """
    expected = [f.name for f in schema]
    if list(df.columns) != expected:
        raise ValueError(
            f"schema drift (master): columns ไม่ตรง schema. "
            f"ได้={list(df.columns)} คาด={expected}"
        )
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(df, table_fqn, job_config=job_config).result()
    log.info("wrote %d rows -> %s (full reload)", len(df), table_fqn)
    return len(df)
