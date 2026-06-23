"""
state.py
────────
อ่าน/เขียน watermark (วันล่าสุดที่ประมวลผลสำเร็จ) ในตาราง BigQuery แยกต่างหาก

กฎสำคัญ: watermark จะถูกอัปเดต "หลังเขียนข้อมูลสำเร็จเท่านั้น"
ถ้า pipeline fail กลางทาง watermark ต้องไม่ขยับ -> รอบหน้าจะ reprocess ต่อจากเดิม
(กันข้อมูลหาย)
"""

from __future__ import annotations

import logging
from datetime import date

from google.cloud import bigquery

log = logging.getLogger("pipeline.state")


def get_watermark(
    client: bigquery.Client,
    state_table_fqn: str,
    pipeline_name: str,
) -> date | None:
    """คืน last_processed_date ของ pipeline_name หรือ None ถ้ายังไม่เคยรัน."""
    sql = f"""
        SELECT last_processed_date
        FROM `{state_table_fqn}`
        WHERE pipeline_name = @name
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("name", "STRING", pipeline_name)]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    if not rows:
        return None
    return rows[0]["last_processed_date"]


def set_watermark(
    client: bigquery.Client,
    state_table_fqn: str,
    pipeline_name: str,
    new_date: date,
) -> None:
    """อัปเดต (upsert) watermark เป็น new_date แบบ idempotent."""
    sql = f"""
        MERGE `{state_table_fqn}` T
        USING (
            SELECT @name AS pipeline_name,
                   @d AS last_processed_date,
                   CURRENT_TIMESTAMP() AS updated_at
        ) S
        ON T.pipeline_name = S.pipeline_name
        WHEN MATCHED THEN
            UPDATE SET last_processed_date = S.last_processed_date,
                       updated_at = S.updated_at
        WHEN NOT MATCHED THEN
            INSERT (pipeline_name, last_processed_date, updated_at)
            VALUES (S.pipeline_name, S.last_processed_date, S.updated_at)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("name", "STRING", pipeline_name),
            bigquery.ScalarQueryParameter("d", "DATE", new_date),
        ]
    )
    client.query(sql, job_config=job_config).result()
    log.info("watermark[%s] -> %s", pipeline_name, new_date.isoformat())
