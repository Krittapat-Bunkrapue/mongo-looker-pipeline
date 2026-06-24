"""
unit test ของ build_b2c_sql (aggregate.py) — ตรวจว่า SQL ที่ render ออกมาถูกต้อง
ไม่ต่อ BigQuery (เช็ค string)
"""

from datetime import date

from aggregate import build_b2c_sql


def _sql():
    return build_b2c_sql(
        event_table_fqn="proj.credit_service.user_usage_event",
        package_table_fqn="proj.credit_service.package_master_v3",
        users_table_fqn="proj.credit_service.librechat_users",
        b2c_table_fqn="proj.credit_service.user_tracking_b2c",
        start_date=date(2026, 1, 1),
        tz_name="Asia/Bangkok",
    )


def test_targets_correct_tables():
    sql = _sql()
    assert "CREATE OR REPLACE TABLE `proj.credit_service.user_tracking_b2c`" in sql
    assert "`proj.credit_service.user_usage_event`" in sql
    assert "`proj.credit_service.package_master_v3`" in sql
    assert "`proj.credit_service.librechat_users`" in sql


def test_excludes_banned_users_before_aggregate():
    sql = _sql()
    # banned CTE จาก users ที่ isBanned = TRUE
    assert "isBanned = TRUE" in sql
    # left-anti join ใน evt (ก่อน aggregate)
    assert "LEFT JOIN banned" in sql
    assert "b.userId IS NULL" in sql
    # banned ต้องอยู่ก่อน evt_pkg/agg (ตัดออกตั้งแต่ระดับ event)
    assert sql.index("LEFT JOIN banned") < sql.index("evt_pkg AS")
    assert sql.index("banned AS") < sql.index("agg AS")


def test_partition_and_cluster():
    sql = _sql()
    assert "PARTITION BY date_id" in sql
    assert "CLUSTER BY userId" in sql


def test_b2c_package_filter():
    sql = _sql()
    assert "IN (1, 2, 3, 12)" in sql


def test_date_window_uses_bangkok_and_start_date():
    sql = _sql()
    assert "date_id >= DATE '2026-01-01'" in sql
    assert "CURRENT_DATE('Asia/Bangkok')" in sql


def test_replicates_notebook_logic_markers():
    sql = _sql()
    # eggToken กลับเครื่องหมายตอน Token Used
    assert "eggToken * -1" in sql
    # union Trial Conversion -> Subscribe
    assert "REPLACE ('Subscribe' AS event_flag)" in sql
    # window สำคัญ
    assert "DENSE_RANK() OVER (PARTITION BY userId ORDER BY packageId DESC)" in sql
    assert "current_package_flag" in sql
    # metric ผลลัพธ์
    assert "free_trial_token_used" in sql
    assert "trial_conversion_cnt" in sql
