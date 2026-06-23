"""
transform.py
────────────
แปลง raw Mongo documents -> pandas DataFrame ที่พร้อมโหลดเข้า BigQuery

ทำไมคำนวณ date_id / totalCostThb ที่นี่ (ไม่ใช่ใน Mongo):
  • คุม timezone (Asia/Bangkok) และ precision ของเงิน (Decimal) ได้เป๊ะ
  • เป็น pure function -> unit-test ได้โดยไม่ต่อ network
  (ส่วน "งานหนัก" คือ $match ช่วงวัน + $project ตัด field ทำฝั่ง Mongo แล้วใน extract.py)

ความถูกต้องของเงิน: *CostUsd และ totalCostThb เก็บเป็น Decimal -> BigQuery NUMERIC
(ห้ามแปลงเป็น float ระหว่างทาง เพราะจะเพี้ยน)
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

import pandas as pd

from load import COLUMN_NAMES

# precision ของเงินที่เก็บใน BQ NUMERIC
_MONEY_QUANT = Decimal("0.000001")

# field ที่เป็นเงิน (Decimal128 ใน Mongo) -> NUMERIC
_DECIMAL_FIELDS = ("totalCostUsd", "chatCostUsd", "websearchCostUsd", "externalCostUsd")
# field ที่เป็นจำนวนเต็ม (อาจติดลบได้)
_INT_FIELDS = ("eggToken", "chatToken", "websearchToken", "externalToken")
# field ข้อความที่ map ตรงชื่อ Mongo
_STR_FIELDS = (
    "userId", "eventType", "subscriptionId", "packageId",
    "externalCostName", "externalTransactionReference", "traceId",
    "aiModel", "agentId", "teamId", "deductType", "teamSubscriptionId",
)


class TransformError(ValueError):
    """raise เมื่อ document จาก Mongo ผิดรูปจน transform ไม่ได้ (schema drift)."""


def _to_str_id(value) -> str:
    """ObjectId / {'$oid': ...} / str -> hex string."""
    if value is None:
        raise TransformError("document ขาด _id")
    if isinstance(value, dict) and "$oid" in value:
        return str(value["$oid"])
    return str(value)


def _to_utc(value) -> datetime:
    """แปลง eventTimeStamp -> tz-aware UTC datetime."""
    if value is None:
        raise TransformError("document ขาด eventTimeStamp")
    if isinstance(value, dict) and "$date" in value:  # extended JSON (เช่นใน test)
        value = value["$date"]
    if isinstance(value, str):
        # รองรับ ...Z
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise TransformError(f"eventTimeStamp ชนิดไม่รองรับ: {type(value)!r}")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_decimal(value) -> Decimal | None:
    """Decimal128 / {'$numberDecimal': ...} / number / str -> Decimal (หรือ None)."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if hasattr(value, "to_decimal"):       # bson.Decimal128
        return value.to_decimal()
    if isinstance(value, dict) and "$numberDecimal" in value:
        return Decimal(str(value["$numberDecimal"]))
    return Decimal(str(value))


def _to_int(value) -> int | None:
    if value is None:
        return None
    return int(value)


def _to_str(value) -> str | None:
    return None if value is None else str(value)


def _bangkok_date(ts_utc: datetime, tz: ZoneInfo) -> date:
    return ts_utc.astimezone(tz).date()


def _money(value: Decimal | None) -> Decimal:
    return (value or Decimal(0)).quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)


def _row_from_doc(doc: dict, exchange_rate: Decimal, tz: ZoneInfo, ingested_at: datetime) -> dict:
    ts = _to_utc(doc.get("eventTimeStamp"))
    total_usd = _to_decimal(doc.get("totalCostUsd"))
    total_thb = _money(total_usd) * exchange_rate

    row: dict = {
        "event_id": _to_str_id(doc.get("_id")),
        "eventTimeStamp": ts,
        "date_id": _bangkok_date(ts, tz),
        "eggToken": None, "chatToken": None, "websearchToken": None, "externalToken": None,
        "totalCostUsd": _money(total_usd) if total_usd is not None else None,
        "totalCostThb": total_thb.quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP),
        "_ingested_at": ingested_at,
    }
    for f in _STR_FIELDS:
        row[f] = _to_str(doc.get(f))
    for f in _INT_FIELDS:
        row[f] = _to_int(doc.get(f))
    for f in ("chatCostUsd", "websearchCostUsd", "externalCostUsd"):
        d = _to_decimal(doc.get(f))
        row[f] = _money(d) if d is not None else None

    breakdown = doc.get("deductionBreakdown")
    row["deductionBreakdown"] = None if breakdown is None else json.dumps(breakdown, default=str, ensure_ascii=False)
    return row


def normalize_records(
    records: list[dict],
    *,
    exchange_rate: Decimal,
    tz: ZoneInfo,
    ingested_at: datetime | None = None,
) -> pd.DataFrame:
    """
    แปลง list ของ Mongo docs -> DataFrame ตาม schema (คอลัมน์เรียงตาม COLUMN_NAMES)
    คืน DataFrame ว่าง (แต่มีคอลัมน์ครบ) ถ้า records ว่าง
    """
    ingested_at = ingested_at or datetime.now(timezone.utc)
    rows = [_row_from_doc(d, exchange_rate, tz, ingested_at) for d in records]

    df = pd.DataFrame(rows, columns=COLUMN_NAMES)

    # cast dtype ให้ตรงกับที่ BigQuery/pyarrow คาดหวัง
    df["eventTimeStamp"] = pd.to_datetime(df["eventTimeStamp"], utc=True)
    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], utc=True)
    for f in _INT_FIELDS:
        df[f] = df[f].astype("Int64")  # nullable int (รองรับค่าติดลบ/None)
    # date_id, *CostUsd, totalCostThb คงเป็น object (date / Decimal) ให้ pyarrow map ตาม schema

    return df
