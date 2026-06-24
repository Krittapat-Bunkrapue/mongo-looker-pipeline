"""
main.py
───────
Orchestrate ทั้ง pipeline + error handling (entrypoint ของ Cloud Run Job)

ลำดับ:
  1) load + validate config (fail fast)
  2) ตรวจ egress IP (IP drift guard) — แจ้งเตือนถ้าไม่ตรง แต่ยังรันต่อ
  3) คำนวณช่วงวันที่ต้องประมวลผล (จาก watermark + lookback)
  4) ต่อ Mongo -> ดึงราย วัน -> transform -> เขียน BQ (atomic partition replace)
  5) อัปเดต watermark หลังเขียนแต่ละวันสำเร็จ
  6) แจ้งผลสำเร็จ / ถ้า error ระหว่างทาง -> แจ้ง fail + exit ไม่เป็น 0 (Cloud Run mark failed)
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta

import requests
from google.cloud import bigquery
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

import aggregate
import load
import state
from config import Config, ConfigError, load_config
from extract import (
    B2B_COMPANY_PROJECTION,
    B2B_TEAM_PROJECTION,
    B2B_USERS_PROJECTION,
    PACKAGE_PROJECTION,
    USERS_PROJECTION,
    MongoExtractor,
)
from notify import Notifier
from transform import (
    normalize_b2b_company,
    normalize_b2b_team,
    normalize_b2b_users,
    normalize_packages,
    normalize_records,
    normalize_users,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
log = logging.getLogger("pipeline.main")

_IPIFY_URL = "https://api.ipify.org"
_IPIFY_TIMEOUT_S = 10


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def get_egress_ip() -> str:
    """คืน public egress IP จริงของรอบนี้ (ผ่าน Cloud NAT)."""
    resp = requests.get(_IPIFY_URL, timeout=_IPIFY_TIMEOUT_S)
    resp.raise_for_status()
    return resp.text.strip()


def compute_date_window(
    *,
    watermark: date | None,
    start_date: date,
    lookback_days: int,
    today_local: date,
) -> list[date]:
    """
    คืน list วันที่ต้องประมวลผล (เรียงจากเก่า->ใหม่) ครอบคลุม:
      • backfill ครั้งแรก (watermark = None) ตั้งแต่ start_date
      • เติมช่วงที่พลาด (ถ้า job ไม่ได้รันหลายวัน)
      • reprocess ย้อนหลัง lookback_days วันล่าสุด (กัน event มาช้า)
    ขอบบน = เมื่อวาน (today_local - 1) เพราะ cutoff เที่ยงคืน
    """
    upper = today_local - timedelta(days=1)
    if upper < start_date:
        return []

    effective_wm = watermark if watermark is not None else (start_date - timedelta(days=1))
    gap_lower = effective_wm + timedelta(days=1)
    lookback_lower = upper - timedelta(days=lookback_days - 1)
    lower = min(gap_lower, lookback_lower)
    if lower < start_date:
        lower = start_date
    if lower > upper:
        return []

    n = (upper - lower).days + 1
    return [lower + timedelta(days=i) for i in range(n)]


def _check_egress_ip(cfg: Config, notifier: Notifier) -> str | None:
    """ดึง egress IP จริง เทียบกับที่คาด -> แจ้งเตือนถ้าไม่ตรง (ไม่ fail)."""
    try:
        ip = get_egress_ip()
    except requests.RequestException as exc:
        log.warning("ดึง egress IP ไม่ได้ (%s) — ข้ามการเช็ค drift, จะลองต่อ Mongo ต่อไป", exc)
        return None
    if ip != cfg.expected_egress_ip:
        log.warning("IP DRIFT: expected=%s actual=%s", cfg.expected_egress_ip, ip)
        notifier.ip_drift(expected=cfg.expected_egress_ip, actual=ip)
    else:
        log.info("egress IP ตรงตามที่คาด: %s", ip)
    return ip


def run() -> int:
    stage = "config"
    egress_ip: str | None = None
    notifier: Notifier | None = None

    try:
        cfg = load_config()
        notifier = Notifier(cfg.gchat_webhook_url)
        log.info("config: %s", cfg.safe_summary())

        stage = "egress-ip-check"
        egress_ip = _check_egress_ip(cfg, notifier)

        stage = "bigquery-setup"
        client = bigquery.Client(project=cfg.gcp_project_id)
        load.ensure_main_table(client, cfg.bq_table_fqn)
        load.ensure_state_table(client, cfg.bq_state_table_fqn)

        stage = "watermark-read"
        watermark = state.get_watermark(client, cfg.bq_state_table_fqn, cfg.bq_table)
        today_local = datetime.now(cfg.timezone).date()
        dates = compute_date_window(
            watermark=watermark,
            start_date=cfg.start_date,
            lookback_days=cfg.lookback_days,
            today_local=today_local,
        )

        if dates:
            log.info("จะประมวลผล event %d วัน: %s → %s",
                     len(dates), dates[0].isoformat(), dates[-1].isoformat())
        else:
            log.info("ไม่มีวันใหม่ของ event (watermark=%s) — จะ refresh package + rebuild B2C", watermark)

        total_rows = 0
        ingested_at = datetime.now(cfg.timezone).astimezone()
        with MongoExtractor(
            cfg.mongodb_uri,
            cfg.mongo_db,
            cfg.mongo_collection,
            cfg.timezone,
            id_buffer_hours=cfg.id_index_buffer_hours,
        ) as ex:
            # 1a) master: package_master_v3 (full reload ทุกรอบ)
            stage = "extract-package"
            pkg_docs = ex.extract_full(cfg.mongo_package_collection, PACKAGE_PROJECTION)
            stage = "load-package"
            pkg_df = normalize_packages(pkg_docs, ingested_at=ingested_at)
            pkg_rows = load.write_full_table(client, cfg.bq_package_table_fqn, pkg_df, load.PACKAGE_SCHEMA)

            # 1b) master: Librechat.users (full reload) — ใช้ตัด user ที่โดน ban ตอน aggregate
            stage = "extract-users"
            users_docs = ex.extract_full(
                cfg.mongo_users_collection, USERS_PROJECTION, db_name=cfg.mongo_users_db
            )
            stage = "load-users"
            users_df = normalize_users(users_docs, ingested_at=ingested_at)
            users_rows = load.write_full_table(client, cfg.bq_users_table_fqn, users_df, load.USERS_SCHEMA)

            # 2) event: incremental ราย วัน
            for day in dates:
                stage = f"extract:{day.isoformat()}"
                docs = ex.extract_day(day)

                stage = f"transform:{day.isoformat()}"
                df = normalize_records(
                    docs,
                    exchange_rate=cfg.exchange_rate,
                    tz=cfg.timezone,
                    ingested_at=ingested_at,
                )

                stage = f"load:{day.isoformat()}"
                total_rows += load.write_day(client, cfg.bq_table_fqn, df, day)

                # อัปเดต watermark หลังเขียนวันนี้สำเร็จเท่านั้น (กันข้อมูลหายถ้า fail วันถัดไป)
                stage = f"watermark:{day.isoformat()}"
                state.set_watermark(client, cfg.bq_state_table_fqn, cfg.bq_table, day)

        # 3) rebuild ตาราง B2C ด้วย BigQuery SQL (อ่านจาก event + package ที่เพิ่งอัปเดต)
        stage = "aggregate-b2c"
        b2c_rows = aggregate.run_b2c_aggregate(client, cfg)
        log.info("B2C เสร็จ: event=%d, package=%d, users=%d, B2C=%d แถว",
                 total_rows, pkg_rows, users_rows, b2c_rows)

        # ═══════════════ B2B phase (คนละ Mongo server, dataset = B2B) ═══════════════
        stage = "b2b-bigquery-setup"
        load.ensure_main_table(client, cfg.bq_b2b_event_fqn)
        load.ensure_state_table(client, cfg.bq_b2b_state_fqn)

        stage = "b2b-watermark-read"
        wm_b2b = state.get_watermark(client, cfg.bq_b2b_state_fqn, cfg.bq_table)
        dates_b2b = compute_date_window(
            watermark=wm_b2b,
            start_date=cfg.start_date,
            lookback_days=cfg.lookback_days,
            today_local=today_local,
        )

        total_rows_b2b = 0
        with MongoExtractor(
            cfg.mongodb_uri_b2b,
            cfg.mongo_db,
            cfg.mongo_collection,
            cfg.timezone,
            id_buffer_hours=cfg.id_index_buffer_hours,
        ) as ex_b2b:
            # masters: package / users(team) / company / team (full reload)
            stage = "b2b-extract-package"
            pkg_rows_b = load.write_full_table(
                client, cfg.bq_b2b_package_fqn,
                normalize_packages(ex_b2b.extract_full(cfg.mongo_package_collection, PACKAGE_PROJECTION),
                                   ingested_at=ingested_at),
                load.PACKAGE_SCHEMA,
            )
            stage = "b2b-extract-users"
            load.write_full_table(
                client, cfg.bq_b2b_users_fqn,
                normalize_b2b_users(
                    ex_b2b.extract_full(cfg.mongo_users_collection, B2B_USERS_PROJECTION, db_name=cfg.mongo_users_db),
                    ingested_at=ingested_at),
                load.B2B_USERS_SCHEMA,
            )
            stage = "b2b-extract-company"
            load.write_full_table(
                client, cfg.bq_b2b_company_fqn,
                normalize_b2b_company(
                    ex_b2b.extract_full(cfg.mongo_company_collection, B2B_COMPANY_PROJECTION, db_name=cfg.mongo_users_db),
                    ingested_at=ingested_at),
                load.B2B_COMPANY_SCHEMA,
            )
            stage = "b2b-extract-team"
            load.write_full_table(
                client, cfg.bq_b2b_team_fqn,
                normalize_b2b_team(
                    ex_b2b.extract_full(cfg.mongo_team_collection, B2B_TEAM_PROJECTION, db_name=cfg.mongo_users_db),
                    ingested_at=ingested_at),
                load.B2B_TEAM_SCHEMA,
            )

            # event B2B: incremental ราย วัน (watermark แยกใน dataset B2B)
            for day in dates_b2b:
                stage = f"b2b-extract:{day.isoformat()}"
                df = normalize_records(
                    ex_b2b.extract_day(day),
                    exchange_rate=cfg.exchange_rate,
                    tz=cfg.timezone,
                    ingested_at=ingested_at,
                )
                stage = f"b2b-load:{day.isoformat()}"
                total_rows_b2b += load.write_day(client, cfg.bq_b2b_event_fqn, df, day)
                stage = f"b2b-watermark:{day.isoformat()}"
                state.set_watermark(client, cfg.bq_b2b_state_fqn, cfg.bq_table, day)

        stage = "aggregate-b2b"
        b2b_rows = aggregate.run_b2b_aggregate(client, cfg)
        log.info("B2B เสร็จ: event=%d, package=%d, B2B=%d แถว", total_rows_b2b, pkg_rows_b, b2b_rows)

        # Total view (รวม B2C + B2B) — เป็น VIEW, อัปเดตตามตารางต้นทางอัตโนมัติ
        stage = "ensure-total-view"
        aggregate.ensure_total_view(client, cfg)

        notifier.success(
            processed_dates=dates,
            total_rows=total_rows,
            egress_ip=egress_ip or "n/a",
            extra=(f"B2C: pkg={pkg_rows:,} users={users_rows:,} → {b2c_rows:,} แถว | "
                   f"B2B: event={total_rows_b2b:,} → {b2b_rows:,} แถว"),
        )
        return 0

    except ConfigError as exc:
        # config พังตั้งแต่ต้น — อาจยังไม่มี notifier
        log.exception("config error")
        if notifier is not None:
            notifier.failure(error=str(exc), stage=stage, egress_ip=egress_ip)
        return 2

    except Exception as exc:  # noqa: BLE001 — top-level guard
        log.exception("pipeline ล้มเหลวที่ stage=%s", stage)
        if notifier is not None:
            notifier.failure(error=f"{type(exc).__name__}: {exc}", stage=stage, egress_ip=egress_ip)
        return 1


if __name__ == "__main__":
    sys.exit(run())
