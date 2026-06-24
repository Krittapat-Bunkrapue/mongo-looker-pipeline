"""
config.py
─────────
โหลด environment variables, validate ว่าครบ (fail fast) และ expose ค่า config
แบบ immutable ให้ module อื่นใช้

หลักการ:
  • ความลับอ่านจาก env var เท่านั้น (บน prod มาจาก Secret Manager)
  • ถ้าตัวแปร "required" ขาด -> raise ConfigError พร้อมบอกชื่อที่ขาด
  • มี helper mask_secret() กัน log ความลับหลุด stdout
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class ConfigError(RuntimeError):
    """ถูก raise เมื่อ config ไม่ครบหรือไม่ถูกต้อง (fail fast)."""


# ── ชื่อ env var ที่ "ต้องมีเสมอ" (ขาดแม้แต่ตัวเดียว = fail) ────────────
_REQUIRED_VARS: tuple[str, ...] = (
    "MONGODB_URI",        # B2C Mongo server
    "MONGODB_URI_B2B",    # B2B Mongo server (คนละ server)
    "GCHAT_WEBHOOK_URL",
    "GCP_PROJECT_ID",
    "EXPECTED_EGRESS_IP",
)


def mask_secret(value: str | None, *, show: int = 4) -> str:
    """
    คืนค่าที่ mask แล้วสำหรับ logging — แสดงเฉพาะ show ตัวท้าย
    ใช้ทุกครั้งที่จำเป็นต้อง log ค่าที่อาจเป็นความลับ
    """
    if not value:
        return "<empty>"
    if len(value) <= show:
        return "*" * len(value)
    return f"{'*' * (len(value) - show)}{value[-show:]}"


def _get(name: str, default: str | None = None) -> str | None:
    val = os.environ.get(name, default)
    if val is not None:
        val = val.strip()
    return val or default


def _parse_decimal(name: str, raw: str) -> Decimal:
    try:
        return Decimal(raw)
    except (InvalidOperation, TypeError) as exc:
        raise ConfigError(f"ENV '{name}' ต้องเป็นตัวเลข (decimal) แต่ได้ '{raw}'") from exc


def _parse_int(name: str, raw: str) -> int:
    try:
        return int(raw)
    except (ValueError, TypeError) as exc:
        raise ConfigError(f"ENV '{name}' ต้องเป็นจำนวนเต็ม แต่ได้ '{raw}'") from exc


def _parse_date(name: str, raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except (ValueError, TypeError) as exc:
        raise ConfigError(f"ENV '{name}' ต้องเป็นรูปแบบ YYYY-MM-DD แต่ได้ '{raw}'") from exc


@dataclass(frozen=True)
class Config:
    # ── ความลับ ──
    mongodb_uri: str          # B2C Mongo server
    mongodb_uri_b2b: str      # B2B Mongo server (คนละ server)
    gchat_webhook_url: str

    # ── GCP / BigQuery ──
    gcp_project_id: str
    bq_location: str
    bq_dataset: str           # dataset ของ B2C (เช่น "B2C")
    bq_dataset_b2b: str       # dataset ของ B2B (เช่น "B2B")
    bq_dataset_total: str     # dataset ของ view รวม (เช่น "Total")
    bq_table: str             # ชื่อตาราง event (ใช้ทั้ง B2C/B2B คนละ dataset)
    bq_state_table: str
    bq_total_view: str        # ชื่อ view รวม B2C+B2B

    # ── MongoDB (ชื่อ db/collection เหมือนกันทั้ง 2 server) ──
    mongo_db: str
    mongo_collection: str
    mongo_package_collection: str
    mongo_users_db: str           # users/company/team อยู่ใน database Librechat
    mongo_users_collection: str
    mongo_company_collection: str  # b2b_company (B2B เท่านั้น)
    mongo_team_collection: str     # b2b_team (B2B เท่านั้น)

    # ── BigQuery: ตารางเพิ่มเติม (master + aggregate) ──
    bq_package_table: str
    bq_users_table: str
    bq_company_table: str
    bq_team_table: str
    bq_b2c_table: str
    bq_b2b_table: str

    # ── Pipeline logic ──
    timezone: ZoneInfo
    timezone_name: str
    start_date: date
    lookback_days: int
    exchange_rate: Decimal
    # buffer (ชม.) สำหรับ _id-range coarse filter เพื่อยืม default _id index
    # 0 = ปิด (กรองด้วย eventTimeStamp ล้วน: ถูกต้อง 100% แต่ scan ทั้ง collection)
    id_index_buffer_hours: int

    # ── IP drift ──
    expected_egress_ip: str

    def _fqn(self, dataset: str, table: str) -> str:
        return f"{self.gcp_project_id}.{dataset}.{table}"

    # ── B2C tables (dataset = bq_dataset) ──
    @property
    def bq_table_fqn(self) -> str:
        return self._fqn(self.bq_dataset, self.bq_table)

    @property
    def bq_state_table_fqn(self) -> str:
        return self._fqn(self.bq_dataset, self.bq_state_table)

    @property
    def bq_package_table_fqn(self) -> str:
        return self._fqn(self.bq_dataset, self.bq_package_table)

    @property
    def bq_users_table_fqn(self) -> str:
        return self._fqn(self.bq_dataset, self.bq_users_table)

    @property
    def bq_b2c_table_fqn(self) -> str:
        return self._fqn(self.bq_dataset, self.bq_b2c_table)

    @property
    def bq_dataset_fqn(self) -> str:
        return f"{self.gcp_project_id}.{self.bq_dataset}"

    # ── B2B tables (dataset = bq_dataset_b2b) ──
    @property
    def bq_b2b_dataset_fqn(self) -> str:
        return f"{self.gcp_project_id}.{self.bq_dataset_b2b}"

    @property
    def bq_b2b_event_fqn(self) -> str:
        return self._fqn(self.bq_dataset_b2b, self.bq_table)

    @property
    def bq_b2b_state_fqn(self) -> str:
        return self._fqn(self.bq_dataset_b2b, self.bq_state_table)

    @property
    def bq_b2b_package_fqn(self) -> str:
        return self._fqn(self.bq_dataset_b2b, self.bq_package_table)

    @property
    def bq_b2b_users_fqn(self) -> str:
        return self._fqn(self.bq_dataset_b2b, self.bq_users_table)

    @property
    def bq_b2b_company_fqn(self) -> str:
        return self._fqn(self.bq_dataset_b2b, self.bq_company_table)

    @property
    def bq_b2b_team_fqn(self) -> str:
        return self._fqn(self.bq_dataset_b2b, self.bq_team_table)

    @property
    def bq_b2b_agg_fqn(self) -> str:
        return self._fqn(self.bq_dataset_b2b, self.bq_b2b_table)

    # ── Total view (รวม B2C + B2B) ──
    @property
    def bq_total_dataset_fqn(self) -> str:
        return f"{self.gcp_project_id}.{self.bq_dataset_total}"

    @property
    def bq_total_view_fqn(self) -> str:
        return self._fqn(self.bq_dataset_total, self.bq_total_view)

    def safe_summary(self) -> dict[str, str]:
        """dict สำหรับ log ได้ปลอดภัย (ความลับถูก mask)."""
        return {
            "gcp_project_id": self.gcp_project_id,
            "bq_b2c_dataset": self.bq_dataset,
            "bq_b2b_dataset": self.bq_dataset_b2b,
            "mongo": f"{self.mongo_db}.{self.mongo_collection}",
            "timezone": self.timezone_name,
            "start_date": self.start_date.isoformat(),
            "lookback_days": str(self.lookback_days),
            "exchange_rate": str(self.exchange_rate),
            "id_index_buffer_hours": str(self.id_index_buffer_hours),
            "expected_egress_ip": self.expected_egress_ip,
            "mongodb_uri": mask_secret(self.mongodb_uri),
            "mongodb_uri_b2b": mask_secret(self.mongodb_uri_b2b),
            "gchat_webhook_url": mask_secret(self.gchat_webhook_url),
        }


def load_config() -> Config:
    """
    โหลด + validate config จาก env ทั้งหมด
    raise ConfigError ทันทีถ้าขาด required var หรือค่าผิดรูปแบบ
    """
    missing = [name for name in _REQUIRED_VARS if not _get(name)]
    if missing:
        raise ConfigError(
            "ENV ที่จำเป็นขาดหายไป: " + ", ".join(missing) +
            " — ตรวจสอบ Secret Manager binding / .env"
        )

    tz_name = _get("PIPELINE_TIMEZONE", "Asia/Bangkok")
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError as exc:
        raise ConfigError(
            f"PIPELINE_TIMEZONE '{tz_name}' ไม่รู้จัก — ติดตั้ง tzdata หรือใช้ชื่อ IANA ที่ถูกต้อง"
        ) from exc

    lookback = _parse_int("LOOKBACK_DAYS", _get("LOOKBACK_DAYS", "1"))
    if lookback < 1:
        raise ConfigError("LOOKBACK_DAYS ต้อง >= 1")

    rate = _parse_decimal("EXCHANGE_RATE", _get("EXCHANGE_RATE", "32.67"))
    if rate <= 0:
        raise ConfigError("EXCHANGE_RATE ต้อง > 0")

    id_buffer = _parse_int("ID_INDEX_BUFFER_HOURS", _get("ID_INDEX_BUFFER_HOURS", "24"))
    if id_buffer < 0:
        raise ConfigError("ID_INDEX_BUFFER_HOURS ต้อง >= 0 (0 = ปิด _id-range filter)")

    return Config(
        mongodb_uri=_get("MONGODB_URI"),
        mongodb_uri_b2b=_get("MONGODB_URI_B2B"),
        gchat_webhook_url=_get("GCHAT_WEBHOOK_URL"),
        gcp_project_id=_get("GCP_PROJECT_ID"),
        bq_location=_get("BQ_LOCATION", "asia-southeast1"),
        bq_dataset=_get("BQ_DATASET", "B2C"),
        bq_dataset_b2b=_get("BQ_DATASET_B2B", "B2B"),
        bq_dataset_total=_get("BQ_DATASET_TOTAL", "Total"),
        bq_table=_get("BQ_TABLE", "user_usage_event"),
        bq_state_table=_get("BQ_STATE_TABLE", "pipeline_state"),
        bq_total_view=_get("BQ_TOTAL_VIEW", "user_tracking_total"),
        mongo_db=_get("MONGO_DB", "credit_service"),
        mongo_collection=_get("MONGO_COLLECTION", "user_usage_event"),
        mongo_package_collection=_get("MONGO_PACKAGE_COLLECTION", "package_master_v3"),
        mongo_users_db=_get("MONGO_USERS_DB", "Librechat"),
        mongo_users_collection=_get("MONGO_USERS_COLLECTION", "users"),
        mongo_company_collection=_get("MONGO_COMPANY_COLLECTION", "b2b_company"),
        mongo_team_collection=_get("MONGO_TEAM_COLLECTION", "b2b_team"),
        bq_package_table=_get("BQ_PACKAGE_TABLE", "package_master_v3"),
        bq_users_table=_get("BQ_USERS_TABLE", "librechat_users"),
        bq_company_table=_get("BQ_COMPANY_TABLE", "b2b_company"),
        bq_team_table=_get("BQ_TEAM_TABLE", "b2b_team"),
        bq_b2c_table=_get("BQ_B2C_TABLE", "user_tracking_b2c"),
        bq_b2b_table=_get("BQ_B2B_TABLE", "user_tracking_b2b"),
        timezone=tz,
        timezone_name=tz_name,
        start_date=_parse_date("START_DATE", _get("START_DATE", "2026-01-01")),
        lookback_days=lookback,
        exchange_rate=rate,
        id_index_buffer_hours=id_buffer,
        expected_egress_ip=_get("EXPECTED_EGRESS_IP"),
    )
