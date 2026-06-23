"""
unit test ของ transform.py — รันได้โดยไม่ต่อ network
ทดสอบ: date_id (timezone), totalCostThb (precision/Decimal), type mapping,
schema columns, edge cases
"""

from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from load import COLUMN_NAMES
from transform import TransformError, normalize_records

TZ = ZoneInfo("Asia/Bangkok")
RATE = Decimal("32.67")


def _doc(**overrides):
    base = {
        "_id": {"$oid": "6a3a345dafd107304899e3ff"},
        "eventTimeStamp": datetime(2026, 6, 23, 7, 23, 9, tzinfo=timezone.utc),
        "userId": "u-1",
        "eventType": "Token Used",
        "subscriptionId": "sub-1",
        "packageId": "2",
        "eggToken": -1900120,
        "chatToken": -1896678,
        "websearchToken": 0,
        "totalCostUsd": {"$numberDecimal": "0.553325"},
        "chatCostUsd": {"$numberDecimal": "0.547625"},
        "websearchCostUsd": {"$numberDecimal": "0"},
        "externalToken": -3452,
        "externalCostUsd": {"$numberDecimal": "0.001"},
        "externalCostName": "not_diamond",
        "externalTransactionReference": "ref-1",
        "traceId": "trace-1",
        "aiModel": "gpt-5.5",
        "agentId": None,
        "teamId": None,
        "deductType": "USER_TOKEN",
        "teamSubscriptionId": None,
        "deductionBreakdown": None,
    }
    base.update(overrides)
    return base


def test_columns_match_schema_order():
    df = normalize_records([_doc()], exchange_rate=RATE, tz=TZ)
    assert list(df.columns) == COLUMN_NAMES


def test_event_id_from_oid():
    df = normalize_records([_doc()], exchange_rate=RATE, tz=TZ)
    assert df.loc[0, "event_id"] == "6a3a345dafd107304899e3ff"


def test_date_id_uses_bangkok_timezone():
    # 07:23 UTC -> 14:23 Bangkok -> 2026-06-23
    df = normalize_records([_doc()], exchange_rate=RATE, tz=TZ)
    assert df.loc[0, "date_id"] == date(2026, 6, 23)


def test_date_id_crosses_midnight_boundary():
    # 2026-06-22 17:30Z -> 2026-06-23 00:30 Bangkok -> วันที่ขยับเป็น 23
    late = _doc(eventTimeStamp=datetime(2026, 6, 22, 17, 30, tzinfo=timezone.utc))
    early = _doc(eventTimeStamp=datetime(2026, 6, 22, 16, 30, tzinfo=timezone.utc))
    df = normalize_records([late, early], exchange_rate=RATE, tz=TZ)
    assert df.loc[0, "date_id"] == date(2026, 6, 23)
    assert df.loc[1, "date_id"] == date(2026, 6, 22)


def test_total_cost_thb_is_decimal_and_precise():
    df = normalize_records([_doc()], exchange_rate=RATE, tz=TZ)
    thb = df.loc[0, "totalCostThb"]
    assert isinstance(thb, Decimal)  # ห้ามเป็น float
    expected = (Decimal("0.553325") * RATE).quantize(Decimal("0.000001"))
    assert thb == expected


def test_cost_usd_kept_as_decimal():
    df = normalize_records([_doc()], exchange_rate=RATE, tz=TZ)
    assert isinstance(df.loc[0, "totalCostUsd"], Decimal)
    assert df.loc[0, "totalCostUsd"] == Decimal("0.553325")


def test_negative_int_tokens_preserved():
    df = normalize_records([_doc()], exchange_rate=RATE, tz=TZ)
    assert df.loc[0, "eggToken"] == -1900120
    assert df.loc[0, "externalToken"] == -3452


def test_iso_string_and_extended_json_timestamp():
    d = _doc(eventTimeStamp={"$date": "2026-06-23T07:23:09.562Z"})
    df = normalize_records([d], exchange_rate=RATE, tz=TZ)
    assert df.loc[0, "date_id"] == date(2026, 6, 23)


def test_deduction_breakdown_serialized_to_json_string():
    d = _doc(deductionBreakdown={"a": 1, "b": [1, 2]})
    df = normalize_records([d], exchange_rate=RATE, tz=TZ)
    assert df.loc[0, "deductionBreakdown"] == '{"a": 1, "b": [1, 2]}'


def test_empty_input_returns_empty_df_with_columns():
    df = normalize_records([], exchange_rate=RATE, tz=TZ)
    assert df.empty
    assert list(df.columns) == COLUMN_NAMES


def test_missing_id_raises():
    bad = _doc()
    del bad["_id"]
    with pytest.raises(TransformError):
        normalize_records([bad], exchange_rate=RATE, tz=TZ)


def test_missing_total_cost_usd_defaults_thb_zero():
    d = _doc(totalCostUsd=None)
    df = normalize_records([d], exchange_rate=RATE, tz=TZ)
    assert df.loc[0, "totalCostUsd"] is None
    assert df.loc[0, "totalCostThb"] == Decimal("0.000000")
