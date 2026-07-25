"""
unit test ของ summary views (aggregate.py) — สำหรับหน้า Summary Report ใน Looker
ไม่ต่อ network
"""

from aggregate import build_summary_daily_sql, build_summary_model_sql

ARGS = dict(project="proj", b2c_ds="B2C", b2b_ds="B2B")


def _daily():
    return build_summary_daily_sql(view_fqn="proj.Total.summary_daily", **ARGS)


def _model():
    return build_summary_model_sql(view_fqn="proj.Total.summary_model_daily", **ARGS)


def test_views_target_total_dataset():
    assert "CREATE OR REPLACE VIEW `proj.Total.summary_daily`" in _daily()
    assert "CREATE OR REPLACE VIEW `proj.Total.summary_model_daily`" in _model()


def test_filters_match_tracking_tables():
    # ต้องกรองเหมือน user_tracking_* ไม่งั้นตัวเลขไม่ตรงกับ dashboard เดิม
    for sql in (_daily(), _model()):
        assert "isBanned = TRUE" in sql            # B2C ตัด banned
        assert "b.userId IS NULL" in sql
        assert "IN (1, 2, 3, 12)" in sql           # B2C packages
        assert "NOT IN (5, 7, 10, 97, 98)" in sql  # B2B excluded packages


def test_lifecycle_events_deduped_per_subscription():
    sql = _daily()
    # source มี Subscribe/MainExpired ซ้ำต่อ subscriptionId เดียว -> เก็บครั้งแรก
    assert "eventType IN ('Subscribe', 'MainExpired')" in sql
    assert "PARTITION BY version, eventType, subscriptionId" in sql
    assert "eventType NOT IN ('Subscribe', 'MainExpired')" in sql


def test_daily_metrics_present():
    sql = _daily()
    for m in ("daily_active_users", "usage_events", "tokens_used", "serving_cost_thb",
              "new_subscriptions", "new_paid_subscriptions", "new_trial_subscriptions",
              "expirations", "monthly_resets", "revenue_thb_est"):
        assert m in sql, f"missing metric {m}"


def test_revenue_excludes_trial():
    # trial ราคา 0 อยู่แล้ว แต่ต้องกันชัดเจนว่าไม่นับเป็นรายได้
    assert "eventType = 'Subscribe' AND NOT is_trial, priceThb" in _daily()
    assert "packageName LIKE 'Free Trial%'" in _daily()


def test_model_view_usage_only():
    sql = _model()
    assert "WHERE eventType LIKE 'Token Used%'" in sql
    assert "aiModel" in sql
    # ไม่ควรมี metric ฝั่ง subscription ใน view ระดับโมเดล
    assert "new_subscriptions" not in sql
    assert "revenue_thb_est" not in sql


def test_has_company_mapping_flag_reconciles_with_b2b_page():
    # B2B มี user ที่ไม่มี team/company (user_tracking_b2b inner join ตัดทิ้ง)
    # -> ต้องมี flag ให้ Looker กรองให้ตรงกันได้
    for sql in (_daily(), _model()):
        assert "b2b_mapped AS" in sql
        assert "(e.version = 'B2C' OR m.userId IS NOT NULL) AS has_company_mapping" in sql
        assert "has_company_mapping,\n" in sql   # เป็น dimension ใน GROUP BY


def test_grain_documented_in_active_user_name():
    # ชื่อ field ต้องบอกว่าเป็นราย "วัน" (บวกข้ามวันไม่ได้)
    for sql in (_daily(), _model()):
        assert "AS daily_active_users" in sql
        assert "AS active_users" not in sql
