"""
slides.py
─────────
อัปเดตตัวเลขใน Google Slides deck อัตโนมัติ (วิธี B)

หลักการ: อ่านตาราง Total.slide_metrics (long format) แล้วสั่ง Slides API
แทนข้อความ placeholder ทุกที่ในเด็ค เช่น {{total_users}} -> 7,453

ความปลอดภัย:
  • แตะเฉพาะข้อความที่อยู่ในรูปแบบ {{metric_key}} เท่านั้น ส่วนอื่นของสไลด์ไม่ถูกแตะ
  • ถ้าไม่ได้ตั้ง SLIDES_PRESENTATION_ID -> ข้ามทั้งขั้นตอน (ไม่ทำให้ pipeline ล้ม)
  • ถ้า Slides API ล้มเหลว -> log + คืน 0 ไม่ล้ม pipeline หลัก (ตัวเลขใน BigQuery ถูกแล้ว)

ต้องเตรียมก่อนใช้:
  1) gcloud services enable slides.googleapis.com
  2) แชร์ deck ให้ service account ของ job เป็น Editor
  3) ตั้ง env SLIDES_PRESENTATION_ID (เอาจาก URL ของ deck)
"""

from __future__ import annotations

import logging

log = logging.getLogger("pipeline.slides")

_SCOPES = ["https://www.googleapis.com/auth/presentations"]
# placeholder เพิ่มเติมที่ไม่ได้มาจาก metric_key
_PERIOD_KEY = "period"


def _placeholder(key: str) -> str:
    return "{{" + key + "}}"


def build_replace_requests(metrics: list[dict]) -> list[dict]:
    """
    แปลง rows ของ slide_metrics -> requests ของ Slides API
    metrics: list ของ dict ที่มี metric_key, value_display (+ period_display)
    """
    requests = []
    for m in metrics:
        key, val = m.get("metric_key"), m.get("value_display")
        if not key or val is None:
            continue
        requests.append({
            "replaceAllText": {
                "containsText": {"text": _placeholder(key), "matchCase": True},
                "replaceText": str(val),
            }
        })
    period = next((m.get("period_display") for m in metrics if m.get("period_display")), None)
    if period:
        requests.append({
            "replaceAllText": {
                "containsText": {"text": _placeholder(_PERIOD_KEY), "matchCase": True},
                "replaceText": str(period),
            }
        })
    return requests


def fetch_metrics(client, metrics_view_fqn: str) -> list[dict]:
    """อ่านตัวเลขทั้งหมดจาก view (คืน list[dict])."""
    rows = client.query(
        f"SELECT metric_key, value_display, period_display "
        f"FROM `{metrics_view_fqn}` ORDER BY sort_order"
    ).result()
    return [dict(r) for r in rows]


def update_presentation(cfg, client) -> int:
    """
    อัปเดต deck ตามค่าล่าสุด คืนจำนวน placeholder ที่ถูกแทนจริง
    คืน 0 ถ้าไม่ได้ตั้งค่า/ทำไม่สำเร็จ (ไม่ raise เพื่อไม่ให้ล้ม pipeline)
    """
    pres_id = getattr(cfg, "slides_presentation_id", None)
    if not pres_id:
        log.info("ข้าม Slides — ไม่ได้ตั้ง SLIDES_PRESENTATION_ID")
        return 0

    try:
        import google.auth
        from googleapiclient.discovery import build

        metrics = fetch_metrics(client, cfg.bq_slide_metrics_fqn)
        requests = build_replace_requests(metrics)
        if not requests:
            log.warning("ไม่มี metric ให้อัปเดต")
            return 0

        creds, _ = google.auth.default(scopes=_SCOPES)
        service = build("slides", "v1", credentials=creds, cache_discovery=False)
        resp = service.presentations().batchUpdate(
            presentationId=pres_id, body={"requests": requests}
        ).execute()

        replaced = sum(r.get("replaceAllText", {}).get("occurrencesChanged", 0)
                       for r in resp.get("replies", []))
        log.info("อัปเดต Google Slides: แทนค่า %d จุด จาก %d metric",
                 replaced, len(requests))
        if replaced == 0:
            log.warning("ไม่พบ placeholder ในเด็ค — ตรวจว่าวาง {{metric_key}} ไว้แล้วหรือยัง")
        return replaced

    except Exception as exc:  # noqa: BLE001 — ไม่ให้ Slides ทำ pipeline ล้ม
        log.error("อัปเดต Google Slides ไม่สำเร็จ: %s", exc)
        return 0
