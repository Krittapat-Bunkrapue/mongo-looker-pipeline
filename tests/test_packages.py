"""
unit test ของ normalize_packages (transform.py) — master table package_master_v3
ไม่ต่อ network
"""

from datetime import datetime, timezone
from decimal import Decimal

from load import PACKAGE_COLUMN_NAMES
from transform import normalize_packages


def _pkg(**overrides):
    base = {
        "packageId": 1,
        "packageName": "Starter",
        "packageType": "Individual",
        "tierName": None,
        "priceThb": {"$numberDecimal": "259"},
        "eggToken": 6500,
        "durationDay": 30,
        "durationMonth": 1,
        "createdAt": datetime(2025, 9, 11, 8, 30, 29, tzinfo=timezone.utc),
        "updatedAt": {"$date": "2025-11-24T13:02:20.481Z"},
    }
    base.update(overrides)
    return base


def test_columns_match_schema():
    df = normalize_packages([_pkg()])
    assert list(df.columns) == PACKAGE_COLUMN_NAMES


def test_scalar_mapping():
    df = normalize_packages([_pkg()])
    row = df.iloc[0]
    assert row["packageId"] == 1
    assert row["packageName"] == "Starter"
    assert row["packageType"] == "Individual"
    assert row["eggToken"] == 6500
    assert row["durationDay"] == 30
    assert row["durationMonth"] == 1


def test_price_is_decimal():
    df = normalize_packages([_pkg()])
    assert isinstance(df.iloc[0]["priceThb"], Decimal)
    assert df.iloc[0]["priceThb"] == Decimal("259")


def test_packageid_from_string_is_int():
    df = normalize_packages([_pkg(packageId="12")])
    assert df.iloc[0]["packageId"] == 12


def test_nullable_fields_ok():
    import pandas as pd
    df = normalize_packages([_pkg(tierName=None, priceThb=None, updatedAt=None)])
    row = df.iloc[0]
    assert row["tierName"] is None
    assert row["priceThb"] is None
    # missing datetime -> pandas เก็บเป็น NaT (BigQuery จะ map เป็น NULL)
    assert pd.isna(row["updatedAt"])


def test_empty_input():
    df = normalize_packages([])
    assert df.empty
    assert list(df.columns) == PACKAGE_COLUMN_NAMES
