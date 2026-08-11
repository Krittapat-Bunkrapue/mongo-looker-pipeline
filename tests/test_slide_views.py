"""
unit test ของ slide views (aggregate.py) — ป้อนตัวเลข/กราฟให้ Google Slides
ไม่ต่อ network
"""

from aggregate import (
    build_slide_company_size_sql,
    build_slide_metrics_sql,
    build_slide_new_users_sql,
)

ARGS = dict(project="proj", b2c_ds="B2C", b2b_ds="B2B")


def _metrics():
    return build_slide_metrics_sql(view_fqn="proj.Total.slide_metrics",
                                   tz_name="Asia/Bangkok", **ARGS)


def _company():
    return build_slide_company_size_sql(project="proj", b2b_ds="B2B",
                                        view_fqn="proj.Total.slide_company_size")


def _newusers():
    return build_slide_new_users_sql(view_fqn="proj.Total.slide_new_users_weekly", **ARGS)


def test_views_target_total_dataset():
    assert "CREATE OR REPLACE VIEW `proj.Total.slide_metrics`" in _metrics()
    assert "CREATE OR REPLACE VIEW `proj.Total.slide_company_size`" in _company()
    assert "CREATE OR REPLACE VIEW `proj.Total.slide_new_users_weekly`" in _newusers()


def test_metrics_cover_every_number_on_the_deck():
    sql = _metrics()
    for key in ("total_users", "b2c_users", "b2b_users",
                "b2c_subscriber", "b2c_starter", "b2c_standard", "b2c_pro", "b2c_free_trial",
                "free_trial_conversion", "to_starter", "to_standard", "to_pro",
                "b2b_subscriber", "b2b_biz_starter", "b2b_biz_standard", "b2b_biz_pro",
                "b2b_others", "b2b_companies"):
        assert f"'{key}'" in sql, f"missing metric {key}"


def test_metrics_long_format_columns():
    sql = _metrics()
    # long format: 1 แถว = 1 ตัวเลข -> ใช้ metric_key เป็นชื่อ placeholder
    for col in ("metric_key", "metric_label", "value_num", "value_display",
                "period_display", "generated_at"):
        assert col in sql, f"missing column {col}"
    assert "FORMAT(\"%'d\", value_num)" in sql   # มี comma คั่นหลักพัน


def test_user_counted_once_by_latest_package():
    sql = _metrics()
    # 1 user = 1 แถว จัดกลุ่มตามแพ็คล่าสุด -> ตัวเลขลูกบวกได้เท่ากับตัวแม่
    assert "PARTITION BY v, userId ORDER BY eventTimeStamp DESC" in sql
    # และ Subscribe ต้อง dedup ต่อ subscription ก่อน
    assert "PARTITION BY v, subscriptionId ORDER BY eventTimeStamp" in sql


def test_tree_buckets_are_complementary():
    sql = _metrics()
    # free_trial = "ไม่ได้อยู่ในกลุ่มเสียเงิน" -> subscriber + free_trial = ยอดรวม เสมอ
    assert "pkg NOT IN ('Starter', 'Standard', 'Pro')" in sql
    assert "pkg NOT IN ('Biz Starter', 'Biz Standard', 'Biz Pro')" in sql


def test_conversion_requires_trial_then_paid():
    sql = _metrics()
    assert "COUNTIF(pid = 12) > 0 AND COUNTIF(pid != 12) > 0" in sql
    assert "first_paid_ts" in sql


def test_excludes_banned_and_test_packages():
    for sql in (_metrics(), _newusers()):
        assert "isBanned = TRUE" in sql
        assert "b.userId IS NULL" in sql
        assert "NOT IN (5, 7, 10, 97, 98)" in sql


def test_new_users_has_cumulative_line():
    sql = _newusers()
    assert "new_users_cumulative" in sql
    assert "SUM(new_users) OVER (PARTITION BY segment ORDER BY week_start)" in sql
    assert "DATE_TRUNC(first_date, WEEK(MONDAY))" in sql


def test_company_size_uses_existing_bins():
    sql = _company()
    assert "company_size_range" in sql
    assert "COUNT(DISTINCT companyId) AS companies" in sql
    assert "bin_order" in sql   # เรียงตามขนาด ไม่ใช่เรียงตามตัวอักษร
