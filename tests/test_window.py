"""
unit test ของ compute_date_window (main.py) — logic incremental ที่สำคัญที่สุด
ไม่ต่อ network
"""

from datetime import date

from main import compute_date_window

START = date(2026, 1, 1)


def test_first_run_backfills_from_start_date():
    dates = compute_date_window(
        watermark=None, start_date=START, lookback_days=1, today_local=date(2026, 6, 24)
    )
    assert dates[0] == START
    assert dates[-1] == date(2026, 6, 23)  # ถึงเมื่อวาน
    assert len(dates) == (date(2026, 6, 23) - START).days + 1


def test_steady_state_processes_yesterday_only():
    dates = compute_date_window(
        watermark=date(2026, 6, 22), start_date=START, lookback_days=1,
        today_local=date(2026, 6, 24),
    )
    assert dates == [date(2026, 6, 23)]


def test_missed_runs_fill_the_gap():
    dates = compute_date_window(
        watermark=date(2026, 6, 19), start_date=START, lookback_days=1,
        today_local=date(2026, 6, 24),
    )
    assert dates == [date(2026, 6, 20), date(2026, 6, 21), date(2026, 6, 22), date(2026, 6, 23)]


def test_lookback_reprocesses_recent_days():
    dates = compute_date_window(
        watermark=date(2026, 6, 22), start_date=START, lookback_days=3,
        today_local=date(2026, 6, 24),
    )
    assert dates == [date(2026, 6, 21), date(2026, 6, 22), date(2026, 6, 23)]


def test_nothing_to_do_before_start_date():
    dates = compute_date_window(
        watermark=None, start_date=START, lookback_days=1, today_local=date(2026, 1, 1)
    )
    assert dates == []  # upper = 2025-12-31 < start


def test_never_goes_before_start_date():
    dates = compute_date_window(
        watermark=None, start_date=START, lookback_days=30, today_local=date(2026, 1, 10)
    )
    assert dates[0] == START  # lookback ไม่ลากต่ำกว่า start_date
