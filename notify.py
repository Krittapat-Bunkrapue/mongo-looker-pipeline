"""
notify.py
─────────
แจ้งเตือนผ่าน Google Chat incoming webhook: success / fail / IP drift

ความปลอดภัย: webhook URL เป็นความลับ -> ไม่ log URL (มี key+token อยู่ใน URL)
ทุกการ POST มี timeout + retry (exponential backoff)
"""

from __future__ import annotations

import logging
from datetime import date

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger("pipeline.notify")

_TIMEOUT_S = 15


class Notifier:
    def __init__(self, webhook_url: str, *, dry_run: bool = False):
        self._url = webhook_url
        self._dry_run = dry_run

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _post(self, text: str) -> None:
        if self._dry_run:
            log.info("[dry-run notify] %s", text)
            return
        resp = requests.post(self._url, json={"text": text}, timeout=_TIMEOUT_S)
        # Google Chat คืน 200 เมื่อสำเร็จ
        if resp.status_code >= 400:
            # ไม่ log body ของ resp ตรง ๆ เผื่อมี token สะท้อนกลับ
            raise requests.HTTPError(f"Google Chat webhook ตอบ {resp.status_code}")
        log.info("notify sent (%d chars)", len(text))

    # ── public ───────────────────────────────────────────────────────
    def success(self, *, processed_dates: list[date], total_rows: int, egress_ip: str) -> None:
        if processed_dates:
            span = f"{processed_dates[0].isoformat()} → {processed_dates[-1].isoformat()}"
        else:
            span = "ไม่มีวันใหม่ให้ประมวลผล"
        text = (
            "✅ *Pipeline สำเร็จ* — user_usage_event\n"
            f"• ช่วงวันที่: {span} ({len(processed_dates)} วัน)\n"
            f"• แถวที่เขียน: {total_rows:,}\n"
            f"• Egress IP: {egress_ip}"
        )
        self._safe_send(text)

    def failure(self, *, error: str, stage: str, egress_ip: str | None = None) -> None:
        text = (
            "🔴 *Pipeline ล้มเหลว* — user_usage_event\n"
            f"• ขั้นตอน: {stage}\n"
            f"• Error: {error}\n"
            f"• Egress IP: {egress_ip or 'n/a'}"
        )
        self._safe_send(text)

    def ip_drift(self, *, expected: str, actual: str) -> None:
        text = (
            "⚠️ *IP DRIFT* — egress IP ไม่ตรงที่คาด!\n"
            f"• คาดไว้: {expected}\n"
            f"• ได้จริง: {actual}\n"
            "→ MongoDB whitelist อาจหลุด ตรวจสอบ Cloud NAT / reserved IP ด่วน"
        )
        self._safe_send(text)

    def _safe_send(self, text: str) -> None:
        """ส่งแจ้งเตือนแบบไม่ให้ error การแจ้งเตือนไปล้ม pipeline หลัก."""
        try:
            self._post(text)
        except Exception as exc:  # noqa: BLE001 — ตั้งใจกลืน error ของ notify
            log.error("ส่งแจ้งเตือน Google Chat ไม่สำเร็จ: %s", exc)
