#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# teardown.sh — ลบ resource ทั้งหมดที่ deploy.sh สร้าง (กันลืมปิดแล้วโดนชาร์จ)
#
# ⚠️ ค่าใช้จ่ายหลักคือ Cloud NAT + reserved IP — ตัวที่ "เผาเงินตลอดเวลา"
#    teardown จะลบให้หมด ถ้าอยากเก็บข้อมูล BQ ใช้ --keep-data
#
# วิธีใช้:  ./teardown.sh            # ลบทุกอย่าง (รวม dataset)
#          ./teardown.sh --keep-data # ลบ infra แต่เก็บ BigQuery dataset ไว้
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail   # ไม่ใช้ -e: ถ้าบางตัวถูกลบไปแล้วก็ข้ามไป

# ── ตัวแปร (ต้องตรงกับ deploy.sh) ──
PROJECT_ID="trueaihub-mongo-pipeline-2026"
REGION="asia-southeast1"
NETWORK="pipeline-vpc"; SUBNET="pipeline-subnet"
ROUTER="pipeline-router"; NAT="pipeline-nat"; ADDRESS="pipeline-egress-ip"
REPO="pipeline-repo"; JOB_NAME="mongo-looker-job"; SCHEDULER_JOB="mongo-looker-daily"
SA_JOB="pipeline-job-sa"; SA_SCHED="pipeline-scheduler-sa"
SECRET_MONGO="mongodb-uri"; SECRET_MONGO_B2B="mongodb-uri-b2b"; SECRET_GCHAT="gchat-webhook-url"
DATASETS=("B2C" "B2B")

KEEP_DATA=0
[[ "${1:-}" == "--keep-data" ]] && KEEP_DATA=1

[[ "$PROJECT_ID" == "CHANGE_ME" ]] && { echo "!! แก้ PROJECT_ID ใน teardown.sh ก่อน"; exit 1; }
gcloud config set project "$PROJECT_ID" >/dev/null

SA_JOB_EMAIL="${SA_JOB}@${PROJECT_ID}.iam.gserviceaccount.com"
SA_SCHED_EMAIL="${SA_SCHED}@${PROJECT_ID}.iam.gserviceaccount.com"
say() { echo -e "\033[1;34m▶ $*\033[0m"; }

say "ลบ Cloud Scheduler"
gcloud scheduler jobs delete "$SCHEDULER_JOB" --location="$REGION" --quiet 2>/dev/null

say "ลบ Cloud Run Job"
gcloud run jobs delete "$JOB_NAME" --region="$REGION" --quiet 2>/dev/null

say "ลบ Cloud NAT + Router + static IP (หยุดค่าใช้จ่ายหลัก)"
gcloud compute routers nats delete "$NAT" --router="$ROUTER" --region="$REGION" --quiet 2>/dev/null
gcloud compute routers delete "$ROUTER" --region="$REGION" --quiet 2>/dev/null
gcloud compute addresses delete "$ADDRESS" --region="$REGION" --quiet 2>/dev/null

say "ลบ subnet + VPC"
gcloud compute networks subnets delete "$SUBNET" --region="$REGION" --quiet 2>/dev/null
gcloud compute networks delete "$NETWORK" --quiet 2>/dev/null

say "ลบ Artifact Registry repo"
gcloud artifacts repositories delete "$REPO" --location="$REGION" --quiet 2>/dev/null

say "ลบ secrets"
gcloud secrets delete "$SECRET_MONGO" --quiet 2>/dev/null
gcloud secrets delete "$SECRET_MONGO_B2B" --quiet 2>/dev/null
gcloud secrets delete "$SECRET_GCHAT" --quiet 2>/dev/null

say "ลบ service accounts"
gcloud iam service-accounts delete "$SA_JOB_EMAIL" --quiet 2>/dev/null
gcloud iam service-accounts delete "$SA_SCHED_EMAIL" --quiet 2>/dev/null

if [[ "$KEEP_DATA" == "1" ]]; then
  echo -e "\033[1;33m! เก็บ BigQuery datasets (${DATASETS[*]}) ไว้ (ตามที่ระบุ --keep-data)\033[0m"
else
  say "ลบ BigQuery datasets (รวมตารางทั้งหมด)"
  for ds in "${DATASETS[@]}"; do
    bq rm -r -f -d "${PROJECT_ID}:${ds}" 2>/dev/null
  done
fi

echo -e "\n\033[1;32m✓ teardown เสร็จ — ตรวจ Billing เพื่อยืนยันว่าไม่มี resource ค้าง\033[0m"
