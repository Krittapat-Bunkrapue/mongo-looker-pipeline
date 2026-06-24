"""
unit test ฝั่ง B2B: master transforms (company/team/users) + build_b2b_sql
ไม่ต่อ network
"""

from datetime import date

from aggregate import build_b2b_sql
from load import (
    B2B_COMPANY_COLUMN_NAMES,
    B2B_TEAM_COLUMN_NAMES,
    B2B_USERS_COLUMN_NAMES,
)
from transform import normalize_b2b_company, normalize_b2b_team, normalize_b2b_users


# ── master transforms ────────────────────────────────────────────────
def test_b2b_users_columns_and_values():
    df = normalize_b2b_users([{"userId": "u-1", "teamId": "t-1", "teamName": "Team A"}])
    assert list(df.columns) == B2B_USERS_COLUMN_NAMES
    assert df.iloc[0]["teamName"] == "Team A"


def test_b2b_company_columns():
    df = normalize_b2b_company([{"companyId": "c-1", "companyName": "ACME"}])
    assert list(df.columns) == B2B_COMPANY_COLUMN_NAMES
    assert df.iloc[0]["companyName"] == "ACME"


def test_b2b_team_columns():
    df = normalize_b2b_team([{"teamId": "t-1", "companyId": "c-1"}])
    assert list(df.columns) == B2B_TEAM_COLUMN_NAMES
    assert df.iloc[0]["companyId"] == "c-1"


def test_b2b_masters_empty():
    for fn, cols in (
        (normalize_b2b_users, B2B_USERS_COLUMN_NAMES),
        (normalize_b2b_company, B2B_COMPANY_COLUMN_NAMES),
        (normalize_b2b_team, B2B_TEAM_COLUMN_NAMES),
    ):
        df = fn([])
        assert df.empty
        assert list(df.columns) == cols


# ── B2B SQL ──────────────────────────────────────────────────────────
def _sql():
    return build_b2b_sql(
        event_table_fqn="proj.B2B.user_usage_event",
        package_table_fqn="proj.B2B.package_master_v3",
        users_table_fqn="proj.B2B.librechat_users",
        company_table_fqn="proj.B2B.b2b_company",
        team_table_fqn="proj.B2B.b2b_team",
        b2b_table_fqn="proj.B2B.user_tracking_b2b",
        start_date=date(2026, 1, 1),
        tz_name="Asia/Bangkok",
    )


def test_b2b_sql_targets_and_dataset():
    sql = _sql()
    assert "CREATE OR REPLACE TABLE `proj.B2B.user_tracking_b2b`" in sql
    assert "`proj.B2B.b2b_company`" in sql
    assert "`proj.B2B.b2b_team`" in sql
    assert "PARTITION BY date_id" in sql


def test_b2b_package_exclusion():
    # ตัด package 5,7,10,97,98 (ตาม notebook)
    assert "NOT IN (5, 7, 10, 97, 98)" in _sql()


def test_b2b_company_size_bin_and_windows():
    sql = _sql()
    assert "company_size_range" in sql
    assert "DIV(COUNT(DISTINCT userId) - 1, 10)" in sql   # bin ทีละ 10 คน (BigQuery DIV() function)
    assert "company_first_event_row" in sql
    assert "team_first_event_row" in sql


def test_b2b_has_no_trial_conversion():
    # B2B ไม่มี Trial Conversion / Free Trial / banned-exclusion
    sql = _sql()
    assert "Trial Conversion" not in sql
    assert "free_trial_token_used" not in sql
    assert "isBanned" not in sql
