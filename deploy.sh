#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy.sh — Deploy ทั้ง pipeline ขึ้น Google Cloud (idempotent รันซ้ำได้)
#
# แนะนำให้รันใน Google Cloud Shell (มี gcloud พร้อม + auth แล้ว) หรือ Git Bash
#
# ก่อนรัน:
#   1) สร้าง GCP project + ผูก billing แล้ว (ดู README ขั้น "สร้าง project")
#   2) gcloud auth login ; gcloud config set project <PROJECT_ID>
#   3) แก้ตัวแปร PROJECT_ID ด้านล่าง
#   4) (ครั้งแรก) export ค่า secret ก่อนรัน เพื่อสร้าง secret version อัตโนมัติ:
#        export MONGODB_URI_VALUE='mongodb+srv://readonly:...@cluster/...'
#        export GCHAT_WEBHOOK_URL_VALUE='https://chat.googleapis.com/v1/spaces/.../messages?key=...&token=...'
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ══════════════════════ ตัวแปร (แก้ตรงนี้) ═══════════════════════════════════
PROJECT_ID="trueaihub-mongo-pipeline-2026"                 # <<< ใส่ GCP project id ของคุณ
REGION="asia-southeast1"               # Singapore (ใกล้ไทยสุด)

# ── ชื่อ resource ──
NETWORK="pipeline-vpc"
SUBNET="pipeline-subnet"
SUBNET_RANGE="10.100.0.0/26"
ROUTER="pipeline-router"
NAT="pipeline-nat"
ADDRESS="pipeline-egress-ip"           # reserved static IP สำหรับ egress
REPO="pipeline-repo"                   # Artifact Registry
IMAGE="mongo-looker-pipeline"
JOB_NAME="mongo-looker-job"            # Cloud Run Job
SCHEDULER_JOB="mongo-looker-daily"
SA_JOB="pipeline-job-sa"               # runtime SA ของ job
SA_SCHED="pipeline-scheduler-sa"       # SA ที่ scheduler ใช้ trigger job

# ── secret names ──
SECRET_MONGO="mongodb-uri"
SECRET_GCHAT="gchat-webhook-url"

# ── BigQuery ──
DATASET="credit_service"
TABLE="user_usage_event"
STATE_TABLE="pipeline_state"
PACKAGE_COLLECTION="package_master_v3"   # master table ใน Mongo (credit_service)
PACKAGE_TABLE="package_master_v3"        # ตาราง master ใน BigQuery
B2C_TABLE="user_tracking_b2c"            # ตาราง aggregate B2C (rebuild ทุกรอบ)

# ── pipeline config ──
START_DATE="2026-01-01"
LOOKBACK_DAYS="1"
EXCHANGE_RATE="32.67"
ID_INDEX_BUFFER_HOURS="24"             # ยืม default _id index (ไม่ต้องสร้าง index ใหม่); 0=ปิด
PIPELINE_TZ="Asia/Bangkok"
SCHEDULE_CRON="0 6 * * *"              # 06:00 ทุกวัน (Asia/Bangkok)

# ══════════════════════ helper ══════════════════════════════════════════════
log()  { echo -e "\n\033[1;34m▶ $*\033[0m"; }
ok()   { echo -e "  \033[1;32m✓ $*\033[0m"; }
warn() { echo -e "  \033[1;33m! $*\033[0m"; }

SA_JOB_EMAIL="${SA_JOB}@${PROJECT_ID}.iam.gserviceaccount.com"
SA_SCHED_EMAIL="${SA_SCHED}@${PROJECT_ID}.iam.gserviceaccount.com"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE}:latest"

[[ "$PROJECT_ID" == "CHANGE_ME" ]] && { echo "!! แก้ PROJECT_ID ใน deploy.sh ก่อน"; exit 1; }
gcloud config set project "$PROJECT_ID" >/dev/null

# ── ensure secret: สร้าง secret + เพิ่ม version จาก env var (ถ้ามี) ──────────
ensure_secret() {
  local name="$1" value_var="$2"
  if ! gcloud secrets describe "$name" >/dev/null 2>&1; then
    gcloud secrets create "$name" --replication-policy="automatic" >/dev/null
    ok "สร้าง secret $name"
  fi
  if ! gcloud secrets versions list "$name" --format='value(name)' 2>/dev/null | grep -q .; then
    local value="${!value_var:-}"
    if [[ -n "$value" ]]; then
      printf '%s' "$value" | gcloud secrets versions add "$name" --data-file=- >/dev/null
      ok "เพิ่มค่า secret $name (จาก \$$value_var)"
    else
      warn "secret $name ยังไม่มีค่า — ใส่เองด้วย:"
      echo "      printf '%s' 'YOUR_VALUE' | gcloud secrets versions add $name --data-file=-"
      MISSING_SECRET=1
    fi
  else
    ok "secret $name มีค่าอยู่แล้ว"
  fi
}

# ══════════════════════ 1) เปิด API ═════════════════════════════════════════
log "1) เปิด API ที่จำเป็น"
gcloud services enable \
  run.googleapis.com cloudscheduler.googleapis.com bigquery.googleapis.com \
  compute.googleapis.com artifactregistry.googleapis.com \
  secretmanager.googleapis.com cloudbuild.googleapis.com >/dev/null
ok "APIs enabled"

# ══════════════════════ 2) จอง static egress IP ═════════════════════════════
log "2) จอง static IP สำหรับ egress"
if ! gcloud compute addresses describe "$ADDRESS" --region="$REGION" >/dev/null 2>&1; then
  gcloud compute addresses create "$ADDRESS" --region="$REGION" >/dev/null
fi
EGRESS_IP="$(gcloud compute addresses describe "$ADDRESS" --region="$REGION" --format='value(address)')"
ok "Static egress IP = $EGRESS_IP"

# ══════════════════════ 3) VPC + subnet + Router + NAT ══════════════════════
log "3) VPC + Cloud NAT (ผูก static IP)"
gcloud compute networks describe "$NETWORK" >/dev/null 2>&1 || \
  gcloud compute networks create "$NETWORK" --subnet-mode=custom >/dev/null
gcloud compute networks subnets describe "$SUBNET" --region="$REGION" >/dev/null 2>&1 || \
  gcloud compute networks subnets create "$SUBNET" --network="$NETWORK" \
    --region="$REGION" --range="$SUBNET_RANGE" >/dev/null
gcloud compute routers describe "$ROUTER" --region="$REGION" >/dev/null 2>&1 || \
  gcloud compute routers create "$ROUTER" --network="$NETWORK" --region="$REGION" >/dev/null
if ! gcloud compute routers nats describe "$NAT" --router="$ROUTER" --region="$REGION" >/dev/null 2>&1; then
  gcloud compute routers nats create "$NAT" --router="$ROUTER" --region="$REGION" \
    --nat-custom-subnet-ip-ranges="$SUBNET" --nat-external-ip-pool="$ADDRESS" >/dev/null
fi
ok "VPC/NAT พร้อม — egress ทั้งหมดของ subnet จะออกผ่าน $EGRESS_IP"

# ══════════════════════ 4) BigQuery dataset + tables ════════════════════════
log "4) BigQuery dataset + tables"
bq --location="$REGION" mk --dataset --force "${PROJECT_ID}:${DATASET}" >/dev/null 2>&1 || true
# ตารางหลัก (partition by date_id, cluster) — โค้ดก็ ensure ให้ แต่สร้างไว้ก่อนได้
bq mk --table --force=false \
  --time_partitioning_field=date_id --time_partitioning_type=DAY \
  --clustering_fields=userId,eventType \
  "${PROJECT_ID}:${DATASET}.${TABLE}" \
  event_id:STRING,eventTimeStamp:TIMESTAMP,date_id:DATE,userId:STRING,eventType:STRING,subscriptionId:STRING,packageId:STRING,eggToken:INTEGER,chatToken:INTEGER,websearchToken:INTEGER,totalCostUsd:NUMERIC,chatCostUsd:NUMERIC,websearchCostUsd:NUMERIC,externalToken:INTEGER,externalCostUsd:NUMERIC,externalCostName:STRING,externalTransactionReference:STRING,traceId:STRING,aiModel:STRING,agentId:STRING,teamId:STRING,deductType:STRING,teamSubscriptionId:STRING,deductionBreakdown:STRING,totalCostThb:NUMERIC,_ingested_at:TIMESTAMP \
  >/dev/null 2>&1 || true
bq mk --table --force=false "${PROJECT_ID}:${DATASET}.${STATE_TABLE}" \
  pipeline_name:STRING,last_processed_date:DATE,updated_at:TIMESTAMP >/dev/null 2>&1 || true
ok "dataset/tables พร้อม (${DATASET}.${TABLE}, ${DATASET}.${STATE_TABLE})"

# ══════════════════════ 5) Service accounts ═════════════════════════════════
log "5) Service accounts (least privilege)"
gcloud iam service-accounts describe "$SA_JOB_EMAIL" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$SA_JOB" --display-name="Mongo->BQ pipeline runtime" >/dev/null
gcloud iam service-accounts describe "$SA_SCHED_EMAIL" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$SA_SCHED" --display-name="Scheduler trigger SA" >/dev/null
ok "service accounts พร้อม"

# ══════════════════════ 6) Secrets ══════════════════════════════════════════
log "6) Secret Manager"
MISSING_SECRET=0
ensure_secret "$SECRET_MONGO" "MONGODB_URI_VALUE"
ensure_secret "$SECRET_GCHAT" "GCHAT_WEBHOOK_URL_VALUE"
# ให้ SA ของ job อ่าน secret ได้ (เฉพาะ 2 ตัวนี้)
for s in "$SECRET_MONGO" "$SECRET_GCHAT"; do
  gcloud secrets add-iam-policy-binding "$s" \
    --member="serviceAccount:${SA_JOB_EMAIL}" \
    --role="roles/secretmanager.secretAccessor" >/dev/null
done
ok "secret IAM ผูกแล้ว"
if [[ "$MISSING_SECRET" == "1" ]]; then
  warn "มี secret ที่ยังไม่มีค่า — ใส่ค่าตามคำสั่งด้านบน แล้วรัน deploy.sh ซ้ำอีกครั้ง"
  exit 1
fi

# ══════════════════════ 7) IAM (least privilege) ═══════════════════════════
log "7) IAM bindings"
# runtime SA: เขียน BQ + รัน job/query (jobUser จำเป็นสำหรับ load + MERGE)
for role in roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_JOB_EMAIL}" --role="$role" \
    --condition=None >/dev/null
done
ok "runtime SA: bigquery.dataEditor + bigquery.jobUser + secretAccessor (เฉพาะ 2 secret)"

# ══════════════════════ 8) Build image -> Artifact Registry ════════════════
log "8) Build container -> Artifact Registry"
gcloud artifacts repositories describe "$REPO" --location="$REGION" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "$REPO" --repository-format=docker --location="$REGION" >/dev/null
gcloud builds submit --tag "$IMAGE_URI" . >/dev/null
ok "image: $IMAGE_URI"

# ══════════════════════ 9) Deploy Cloud Run Job ════════════════════════════
log "9) Deploy Cloud Run Job (Direct VPC egress + static IP)"
gcloud run jobs deploy "$JOB_NAME" \
  --image="$IMAGE_URI" \
  --region="$REGION" \
  --service-account="$SA_JOB_EMAIL" \
  --network="$NETWORK" --subnet="$SUBNET" --vpc-egress=all-traffic \
  --set-secrets="MONGODB_URI=${SECRET_MONGO}:latest,GCHAT_WEBHOOK_URL=${SECRET_GCHAT}:latest" \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},BQ_LOCATION=${REGION},BQ_DATASET=${DATASET},BQ_TABLE=${TABLE},BQ_STATE_TABLE=${STATE_TABLE},MONGO_DB=${DATASET},MONGO_COLLECTION=${TABLE},PIPELINE_TIMEZONE=${PIPELINE_TZ},START_DATE=${START_DATE},LOOKBACK_DAYS=${LOOKBACK_DAYS},EXCHANGE_RATE=${EXCHANGE_RATE},ID_INDEX_BUFFER_HOURS=${ID_INDEX_BUFFER_HOURS},MONGO_PACKAGE_COLLECTION=${PACKAGE_COLLECTION},BQ_PACKAGE_TABLE=${PACKAGE_TABLE},BQ_B2C_TABLE=${B2C_TABLE},EXPECTED_EGRESS_IP=${EGRESS_IP}" \
  --max-retries=1 --task-timeout=3600 --memory=1Gi --cpu=1 >/dev/null
ok "Cloud Run Job '$JOB_NAME' deployed (egress -> $EGRESS_IP)"

# ══════════════════════ 10) Cloud Scheduler ════════════════════════════════
log "10) Cloud Scheduler (06:00 Asia/Bangkok)"
# ให้ scheduler SA invoke job ได้
gcloud run jobs add-iam-policy-binding "$JOB_NAME" --region="$REGION" \
  --member="serviceAccount:${SA_SCHED_EMAIL}" --role="roles/run.invoker" >/dev/null
RUN_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"
if gcloud scheduler jobs describe "$SCHEDULER_JOB" --location="$REGION" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "$SCHEDULER_JOB" --location="$REGION" \
    --schedule="$SCHEDULE_CRON" --time-zone="$PIPELINE_TZ" \
    --uri="$RUN_URI" --http-method=POST \
    --oauth-service-account-email="$SA_SCHED_EMAIL" >/dev/null
else
  gcloud scheduler jobs create http "$SCHEDULER_JOB" --location="$REGION" \
    --schedule="$SCHEDULE_CRON" --time-zone="$PIPELINE_TZ" \
    --uri="$RUN_URI" --http-method=POST \
    --oauth-service-account-email="$SA_SCHED_EMAIL" >/dev/null
fi
ok "Scheduler '$SCHEDULER_JOB' ตั้งเวลา '$SCHEDULE_CRON' ($PIPELINE_TZ)"

# ══════════════════════ สรุป ═══════════════════════════════════════════════
cat <<EOF

╔══════════════════════════════════════════════════════════════════════╗
║  ✅ DEPLOY สำเร็จ                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║  ส่ง IP นี้ให้ admin whitelist ใน MongoDB Atlas:                       ║
║                                                                        ║
║        >>>  ${EGRESS_IP}
║                                                                        ║
║  ทดสอบรันทันที (ไม่ต้องรอ 06:00):                                       ║
║     gcloud run jobs execute ${JOB_NAME} --region=${REGION}            ║
║                                                                        ║
║  ดู log:                                                                ║
║     gcloud run jobs executions list --job=${JOB_NAME} --region=${REGION}
╚══════════════════════════════════════════════════════════════════════╝
EOF
