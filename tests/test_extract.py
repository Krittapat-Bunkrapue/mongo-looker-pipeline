"""
unit test ของ extract.py — day_bounds_utc + build_pipeline (_id-range filter)
ไม่ต่อ network
"""

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from bson import ObjectId

from extract import build_pipeline, day_bounds_utc

TZ = ZoneInfo("Asia/Bangkok")
DAY = date(2026, 6, 23)


def test_day_bounds_utc_for_bangkok():
    # วันไทย 2026-06-23 = [2026-06-22T17:00Z, 2026-06-23T17:00Z)
    start, end = day_bounds_utc(DAY, TZ)
    assert start == datetime(2026, 6, 22, 17, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 6, 23, 17, 0, tzinfo=timezone.utc)


def test_pipeline_always_filters_event_timestamp_exactly():
    pipeline = build_pipeline(DAY, TZ, id_buffer_hours=24)
    match = pipeline[0]["$match"]
    start, end = day_bounds_utc(DAY, TZ)
    assert match["eventTimeStamp"] == {"$gte": start, "$lt": end}
    # ต้องมี $project ตัด field
    assert "$project" in pipeline[1]


def test_pipeline_adds_id_range_when_buffer_positive():
    pipeline = build_pipeline(DAY, TZ, id_buffer_hours=24)
    match = pipeline[0]["$match"]
    assert "_id" in match
    start, end = day_bounds_utc(DAY, TZ)
    # ขอบ _id ต้องสอดคล้องกับ start-24h และ end+24h (ระดับวินาที)
    lo_dt = match["_id"]["$gte"].generation_time
    hi_dt = match["_id"]["$lt"].generation_time
    assert lo_dt == datetime(2026, 6, 21, 17, 0, tzinfo=timezone.utc)
    assert hi_dt == datetime(2026, 6, 24, 17, 0, tzinfo=timezone.utc)
    # ขอบล่างต้อง <= ขอบล่างของ eventTimeStamp (กันพลาด event ที่ insert ก่อนเวลา event)
    assert ObjectId.from_datetime(start) >= match["_id"]["$gte"]


def test_pipeline_omits_id_range_when_buffer_zero():
    pipeline = build_pipeline(DAY, TZ, id_buffer_hours=0)
    match = pipeline[0]["$match"]
    assert "_id" not in match  # กรองด้วย eventTimeStamp ล้วน
