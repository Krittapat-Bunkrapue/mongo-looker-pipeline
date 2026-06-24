# ─────────────────────────────────────────────────────────────────────
# Cloud Run Job container — Python ETL pipeline
# ─────────────────────────────────────────────────────────────────────
FROM python:3.12-slim

# กัน Python buffer + ไม่เขียน .pyc
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# ติดตั้ง dependencies ก่อน (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy source (ทุก .py ที่ root — กันลืมเพิ่มไฟล์ใหม่; tests/ ไม่ถูก copy)
COPY *.py ./

# รันแบบ non-root (least privilege)
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

# Cloud Run Job เรียก entrypoint นี้ครั้งเดียวต่อรอบ แล้วจบ
ENTRYPOINT ["python", "main.py"]
