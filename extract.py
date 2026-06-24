"""
extract.py
──────────
ดึงข้อมูลจาก MongoDB Atlas แบบ incremental ราย "วัน" (Asia/Bangkok)

งานหนักทำฝั่ง Mongo:
  • $match  -> filter เฉพาะ event ในช่วงเวลาของวันนั้น
  • $project -> ดึงเฉพาะ field ที่ใช้ (ตัด field ที่ไม่เกี่ยว ลด network/หน่วยความจำ)

การใช้ index (กรณีไม่มี index ที่ eventTimeStamp):
  ObjectId (_id) ฝัง timestamp ตอน insert ไว้ในตัว และ _id มี index ติดมา default
  เราจึงเสริมเงื่อนไข `_id` range เพื่อให้ planner seek ผ่าน _id index (ข้ามข้อมูลเก่า)
  แล้วยังกรอง `eventTimeStamp` แบบเป๊ะอีกชั้น -> date_id / partition ถูกต้องเสมอ

  ⚠️ ข้อแม้: assume "เวลา insert ≈ eventTimeStamp" (ห่างไม่เกิน id_buffer_hours)
     ซึ่งจริงสำหรับ usage event ที่เขียน real-time. ถ้าตั้ง id_buffer_hours=0
     จะกรองด้วย eventTimeStamp ล้วน (ถูก 100% แต่ scan ทั้ง collection)

ความปลอดภัย:
  • บังคับ TLS, ตั้ง serverSelectionTimeoutMS / connectTimeoutMS กัน hang
  • ใช้ MongoDB user แบบ read-only (บังคับฝั่ง Atlas — ดู README)
  • ไม่ log connection string (ถูก mask ที่ config)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger("pipeline.extract")

# timeout (ms)
_SERVER_SELECTION_TIMEOUT_MS = 10_000
_CONNECT_TIMEOUT_MS = 10_000
_SOCKET_TIMEOUT_MS = 120_000

# field ที่ดึงออกมา (projection) — ต้องครอบทุก field ที่ transform ใช้
_PROJECTION = {
    "_id": 1,
    "eventTimeStamp": 1,
    "userId": 1,
    "eventType": 1,
    "subscriptionId": 1,
    "packageId": 1,
    "eggToken": 1,
    "chatToken": 1,
    "websearchToken": 1,
    "totalCostUsd": 1,
    "chatCostUsd": 1,
    "websearchCostUsd": 1,
    "externalToken": 1,
    "externalCostUsd": 1,
    "externalCostName": 1,
    "externalTransactionReference": 1,
    "traceId": 1,
    "aiModel": 1,
    "agentId": 1,
    "teamId": 1,
    "deductType": 1,
    "teamSubscriptionId": 1,
    "deductionBreakdown": 1,
}

# projection ของ master table package_master_v3 (เก็บแบบ lean — ตัด array modelList/capabilities)
# _id:0 = ไม่ดึง _id (ตารางใช้ packageId เป็น key)
PACKAGE_PROJECTION = {
    "_id": 0,
    "packageId": 1,
    "packageName": 1,
    "packageType": 1,
    "tierName": 1,
    "priceThb": 1,
    "eggToken": 1,
    "durationDay": 1,
    "durationMonth": 1,
    "createdAt": 1,
    "updatedAt": 1,
}


def day_bounds_utc(day: date, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """
    คืนช่วง [start, end) เป็น UTC ที่ครอบ "วัน day" ตาม timezone ที่กำหนด
    เช่น day=2026-06-23, tz=Asia/Bangkok -> [2026-06-22T17:00Z, 2026-06-23T17:00Z)
    """
    start_local = datetime.combine(day, time.min, tzinfo=tz)
    end_local = datetime.combine(day + timedelta(days=1), time.min, tzinfo=tz)
    return start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC"))


def build_pipeline(day: date, tz: ZoneInfo, id_buffer_hours: int = 0) -> list[dict]:
    """
    สร้าง aggregation pipeline สำหรับดึง event ทั้งหมดของวัน day

    เงื่อนไข $match:
      • eventTimeStamp อยู่ในช่วงของวัน (เป๊ะเสมอ -> ใช้กำหนด date_id/partition)
      • ถ้า id_buffer_hours > 0: เสริม _id range (±buffer) เพื่อยืม default _id index
        ให้ planner seek ข้ามข้อมูลเก่า แทนการ scan ทั้ง collection
    """
    start_utc, end_utc = day_bounds_utc(day, tz)
    match: dict = {"eventTimeStamp": {"$gte": start_utc, "$lt": end_utc}}
    if id_buffer_hours > 0:
        buf = timedelta(hours=id_buffer_hours)
        match["_id"] = {
            "$gte": ObjectId.from_datetime(start_utc - buf),
            "$lt": ObjectId.from_datetime(end_utc + buf),
        }
    return [
        {"$match": match},
        {"$project": _PROJECTION},
    ]


class MongoExtractor:
    """จัดการ connection + ดึงข้อมูลราย วัน (ใช้เป็น context manager)."""

    def __init__(
        self,
        uri: str,
        db_name: str,
        collection_name: str,
        tz: ZoneInfo,
        id_buffer_hours: int = 0,
    ):
        self._uri = uri
        self._db_name = db_name
        self._collection_name = collection_name
        self._tz = tz
        self._id_buffer_hours = id_buffer_hours
        self._client: MongoClient | None = None

    def __enter__(self) -> "MongoExtractor":
        self._client = MongoClient(
            self._uri,
            tls=True,
            tz_aware=True,                # คืน datetime แบบ tz-aware (UTC)
            appname="mongo-looker-pipeline",
            serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=_CONNECT_TIMEOUT_MS,
            socketTimeoutMS=_SOCKET_TIMEOUT_MS,
            retryReads=True,
        )
        # ping เพื่อ fail fast ถ้าต่อไม่ได้ (เช่น IP ไม่ได้ whitelist)
        self._client.admin.command("ping")
        log.info("connected to MongoDB (%s.%s)", self._db_name, self._collection_name)
        return self

    def __exit__(self, *exc) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    @retry(
        retry=retry_if_exception_type(PyMongoError),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        reraise=True,
    )
    def extract_day(self, day: date) -> list[dict]:
        """ดึง event ทั้งหมดของวัน day -> list ของ raw docs (มี retry+backoff)."""
        assert self._client is not None, "ต้องใช้ภายใน context manager"
        collection = self._client[self._db_name][self._collection_name]
        pipeline = build_pipeline(day, self._tz, self._id_buffer_hours)
        docs = list(collection.aggregate(pipeline, allowDiskUse=True))
        log.info("extracted %d docs for %s", len(docs), day.isoformat())
        return docs

    @retry(
        retry=retry_if_exception_type(PyMongoError),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        reraise=True,
    )
    def extract_full(self, collection_name: str, projection: dict | None = None) -> list[dict]:
        """
        ดึงทั้ง collection (ใช้กับ master/reference table ขนาดเล็ก เช่น package_master_v3)
        full reload — ไม่ใช้ incremental เพราะเป็นตารางอ้างอิงที่เปลี่ยนไม่บ่อย
        """
        assert self._client is not None, "ต้องใช้ภายใน context manager"
        coll = self._client[self._db_name][collection_name]
        docs = list(coll.find({}, projection))
        log.info("extracted %d docs from %s (full)", len(docs), collection_name)
        return docs
