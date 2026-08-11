"""
unit test ของ slides.py — สร้าง request แทน placeholder (ไม่ต่อ network)
"""
from slides import build_replace_requests


ROWS = [
    {"metric_key": "total_users", "value_display": "7,453", "period_display": "10 August 2026"},
    {"metric_key": "b2c_users", "value_display": "6,930", "period_display": "10 August 2026"},
    {"metric_key": "b2b_users", "value_display": None, "period_display": "10 August 2026"},
]


def test_builds_one_request_per_metric_plus_period():
    reqs = build_replace_requests(ROWS)
    # b2b_users มี value เป็น None -> ข้าม ; +1 สำหรับ {{period}}
    assert len(reqs) == 3
    texts = [r["replaceAllText"]["containsText"]["text"] for r in reqs]
    assert "{{total_users}}" in texts
    assert "{{b2c_users}}" in texts
    assert "{{period}}" in texts
    assert "{{b2b_users}}" not in texts


def test_replaces_with_formatted_value():
    reqs = build_replace_requests(ROWS)
    by_text = {r["replaceAllText"]["containsText"]["text"]: r["replaceAllText"]["replaceText"]
               for r in reqs}
    assert by_text["{{total_users}}"] == "7,453"
    assert by_text["{{period}}"] == "10 August 2026"


def test_match_case_true_so_only_exact_placeholders_change():
    for r in build_replace_requests(ROWS):
        assert r["replaceAllText"]["containsText"]["matchCase"] is True


def test_empty_input_is_safe():
    assert build_replace_requests([]) == []
