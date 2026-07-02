"""
unit test ของ repeat-subscribe analysis (aggregate.py):
build_token_cycle_sql + build_repeat_behavior_sql — เช็ค SQL logic markers
ไม่ต่อ network
"""

from datetime import date

from aggregate import EXHAUST_THRESHOLD, build_repeat_behavior_sql, build_token_cycle_sql


def _cycle_sql():
    return build_token_cycle_sql(
        event_table_fqn="proj.B2C.user_usage_event",
        package_table_fqn="proj.B2C.package_master_v3",
        users_table_fqn="proj.B2C.librechat_users",
        cycle_table_fqn="proj.B2C.user_token_cycle",
        start_date=date(2026, 1, 1),
        tz_name="Asia/Bangkok",
    )


def _behavior_sql():
    return build_repeat_behavior_sql(
        cycle_table_fqn="proj.B2C.user_token_cycle",
        event_table_fqn="proj.B2C.user_usage_event",
        package_table_fqn="proj.B2C.package_master_v3",
        users_table_fqn="proj.B2C.librechat_users",
        repeat_table_fqn="proj.B2C.user_repeat_behavior",
        start_date=date(2026, 1, 1),
        tz_name="Asia/Bangkok",
    )


# ── cycle table ──────────────────────────────────────────────────────
def test_cycle_targets_and_partition():
    sql = _cycle_sql()
    assert "CREATE OR REPLACE TABLE `proj.B2C.user_token_cycle`" in sql
    assert "PARTITION BY DATE(cycle_start_ts)" in sql
    assert "`proj.B2C.user_usage_event`" in sql


def test_cycle_excludes_banned_users():
    sql = _cycle_sql()
    assert "isBanned = TRUE" in sql
    assert "b.userId IS NULL" in sql


def test_cycle_boundary_events():
    sql = _cycle_sql()
    # cycle เริ่มด้วย Subscribe/MonthlyReset, จบด้วย boundary 3 แบบ
    assert "eventType IN ('Subscribe', 'MonthlyReset', 'MainExpired')" in sql
    assert "WHERE eventType IN ('Subscribe', 'MonthlyReset')" in sql
    assert "'repurchase'" in sql and "'monthly_reset'" in sql and "'expired'" in sql
    assert "'active'" in sql  # censored


def test_cycle_quota_from_grant_with_monthlyreset_fallback():
    sql = _cycle_sql()
    # MonthlyReset มี eggToken=0 -> quota อิง grant ล่าสุด (last_grant)
    assert "LAST_VALUE(IF(eventType = 'Subscribe', eggToken, NULL) IGNORE NULLS)" in sql
    assert "IF(eventType = 'Subscribe', eggToken, last_grant) AS quota" in sql


def test_cycle_clawback_leftover_from_mainexpired():
    sql = _cycle_sql()
    # MainExpired claw back token ที่เหลือ -> ใช้ |eggToken| เป็น leftover จริง
    assert "IF(next_type = 'MainExpired', ABS(next_egg), NULL)" in sql
    assert "COALESCE(c.clawback_leftover, GREATEST(c.quota - IFNULL(u.consumed, 0), 0))" in sql


def test_cycle_exhaust_threshold_and_segments():
    sql = _cycle_sql()
    assert f"{EXHAUST_THRESHOLD} * quota" in sql or f"{EXHAUST_THRESHOLD} * c.quota" in sql
    for seg in ("A1_exhausted_repurchase", "A2_exhausted_churn", "A3_exhausted_wait_reset",
                "B1_underuse_repurchase", "B2_underuse_churn", "B3_underuse_continue",
                "C0_active", "C9_unknown_quota"):
        assert seg in sql, f"missing segment {seg}"


def test_cycle_counts_unsettled_usage():
    # ต้องนับ 'Token Used (Unsettled)' เป็น consumption ด้วย
    assert "LIKE 'Token Used%'" in _cycle_sql()


# ── behavior table ───────────────────────────────────────────────────
def test_behavior_targets():
    sql = _behavior_sql()
    assert "CREATE OR REPLACE TABLE `proj.B2C.user_repeat_behavior`" in sql
    assert "`proj.B2C.user_token_cycle`" in sql


def test_behavior_repeat_is_paid_only():
    sql = _behavior_sql()
    # repeat นับเฉพาะ paid packages (1,2,3) — trial (12) แยก
    assert "packageId IN (1, 2, 3)" in sql
    assert "packageId = 12" in sql
    assert ">= 2) AS is_repeat" in sql
    assert "'trial_only'" in sql and "'paid_once'" in sql and "'repeat'" in sql


def test_behavior_dedupes_subscription():
    # 1 แถวต่อ subscriptionId (กัน Subscribe event ซ้ำใน sub เดียว)
    assert "PARTITION BY userId, subscriptionId ORDER BY eventTimeStamp) = 1" in _behavior_sql()


def test_behavior_mature_cohort_and_churn():
    sql = _behavior_sql()
    assert "INTERVAL 35 DAY" in sql          # censoring window
    assert "is_mature_cohort" in sql
    assert "(c.last_cycle_end_type = 'expired') AS churned" in sql
    assert "prev_end_type = 'expired'" in sql  # winback


def test_behavior_revenue_and_cost():
    sql = _behavior_sql()
    assert "SUM(p.priceThb) AS revenue_thb" in sql
    assert "serving_cost_thb" in sql
    assert "margin_thb" in sql
