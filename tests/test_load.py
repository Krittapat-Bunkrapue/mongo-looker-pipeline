"""
unit test ของ load.py — schema mapping + idempotency logic (mock BigQuery)
ไม่ต่อ network
"""

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest
from google.cloud import bigquery

import load
from transform import normalize_records

TZ = ZoneInfo("Asia/Bangkok")
DAY = date(2026, 6, 23)


def _df_for_day():
    doc = {
        "_id": "abc123",
        "eventTimeStamp": datetime(2026, 6, 23, 7, 23, 9, tzinfo=timezone.utc),
        "userId": "u-1",
        "eventType": "Token Used",
        "totalCostUsd": {"$numberDecimal": "0.5"},
    }
    return normalize_records([doc], exchange_rate=Decimal("32.67"), tz=TZ)


def test_column_names_match_schema():
    assert load.COLUMN_NAMES == [f.name for f in load.TABLE_SCHEMA]


def test_partition_decorator_format():
    assert load.partition_decorator("p.d.t", DAY) == "p.d.t$20260623"


def test_validate_dataframe_ok():
    df = _df_for_day()
    load.validate_dataframe(df, DAY)  # ไม่ควร raise


def test_validate_dataframe_rejects_wrong_columns():
    df = _df_for_day().drop(columns=["traceId"])
    with pytest.raises(ValueError, match="schema drift"):
        load.validate_dataframe(df, DAY)


def test_validate_dataframe_rejects_row_in_wrong_partition():
    df = _df_for_day()
    df.loc[0, "date_id"] = date(2026, 6, 22)  # ไม่ตรงกับ DAY
    with pytest.raises(ValueError, match="date_id"):
        load.validate_dataframe(df, DAY)


def test_write_day_uses_truncate_and_partition_decorator():
    df = _df_for_day()
    fake_job = MagicMock()
    fake_job.result.return_value = None
    client = MagicMock()
    client.load_table_from_dataframe.return_value = fake_job

    rows = load.write_day(client, "proj.credit_service.user_usage_event", df, DAY)

    assert rows == 1
    client.load_table_from_dataframe.assert_called_once()
    args, kwargs = client.load_table_from_dataframe.call_args
    # target ต้องเป็น partition decorator ของวันนั้น
    assert args[1] == "proj.credit_service.user_usage_event$20260623"
    # ต้องเป็น WRITE_TRUNCATE (atomic partition replace) ไม่ใช่ append
    job_config = kwargs["job_config"]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    fake_job.result.assert_called_once()


def test_write_day_validates_before_load():
    df = _df_for_day()
    df.loc[0, "date_id"] = date(2026, 1, 1)  # ผิด partition
    client = MagicMock()
    with pytest.raises(ValueError):
        load.write_day(client, "p.d.t", df, DAY)
    client.load_table_from_dataframe.assert_not_called()  # ต้องไม่แตะ BQ ถ้า validate ไม่ผ่าน
