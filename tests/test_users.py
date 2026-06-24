"""
unit test ของ normalize_users (transform.py) — Librechat.users (userId, isBanned)
ไม่ต่อ network
"""

import pandas as pd

from load import USERS_COLUMN_NAMES
from transform import normalize_users


def test_columns_match_schema():
    df = normalize_users([{"userId": "u-1", "isBanned": True}])
    assert list(df.columns) == USERS_COLUMN_NAMES


def test_bool_mapping_true_false():
    df = normalize_users([
        {"userId": "u-1", "isBanned": True},
        {"userId": "u-2", "isBanned": False},
    ])
    assert bool(df.iloc[0]["isBanned"]) is True
    assert bool(df.iloc[1]["isBanned"]) is False


def test_bool_from_string():
    df = normalize_users([
        {"userId": "u-1", "isBanned": "TRUE"},
        {"userId": "u-2", "isBanned": "false"},
    ])
    assert bool(df.iloc[0]["isBanned"]) is True
    assert bool(df.iloc[1]["isBanned"]) is False


def test_missing_isbanned_is_na():
    df = normalize_users([{"userId": "u-1"}])
    assert pd.isna(df.iloc[0]["isBanned"])  # ไม่มีค่า -> NULL (ไม่ถูกนับว่า banned)


def test_empty_input():
    df = normalize_users([])
    assert df.empty
    assert list(df.columns) == USERS_COLUMN_NAMES
