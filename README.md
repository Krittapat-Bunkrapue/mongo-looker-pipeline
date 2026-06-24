# Mongo → BigQuery → Looker Studio Pipeline

ETL อัตโนมัติ: ดึง `user_usage_event` จาก **MongoDB Atlas** → แปลงด้วย **Python** →
เขียนเข้า **BigQuery** → แสดงผลใน **Looker Studio**
รันทุกวัน **06:00 น. (เวลาไทย)** ผ่าน Cloud Scheduler → Cloud Run Job
โดยออกอินเทอร์เน็ตด้วย **static IP เดียว** (Cloud NAT) เพื่อให้ admin whitelist ครั้งเดียวจบ

```
Cloud Scheduler (06:00 Asia/Bangkok)
  └─► Cloud Run Job (Python)
        └─► egress ผ่าน Cloud NAT + Static IP  ◄── admin whitelist IP นี้
              └─► MongoDB Atlas ($match incremental + $project, read-only)
                    └─► transform (pandas: date_id ตาม Asia/Bangkok, totalCostThb)
                          └─► BigQuery (partition by date_id, atomic replace)
                                └─► Looker Studio (BigQuery connector)
        └─► แจ้งผล success / fail / IP-drift ผ่าน Google Chat webhook
```

---

## 📁 โครงสร้างไฟล์

| ไฟล์ | หน้าที่ |
|---|---|
| `config.py` | โหลด+validate env/secret (fail fast ถ้าขาด) |
| `state.py` | อ่าน/เขียน watermark (วันล่าสุดที่สำเร็จ) ใน BigQuery |
| `extract.py` | ต่อ MongoDB (TLS+timeout) + aggregation pipeline ราย วัน |
| `transform.py` | pandas: สกัด `date_id` (Asia/Bangkok), คำนวณ `totalCostThb`, แปลง type |
| `load.py` | เขียน BigQuery แบบ idempotent (atomic partition replace) + schema |
| `aggregate.py` | BigQuery SQL สร้างตาราง B2C (`user_tracking_b2c`) — แปลงจาก notebook PySpark |
| `notify.py` | Google Chat webhook: success / fail / IP-drift |
| `main.py` | orchestrate ทั้ง pipeline + error handling |
| `deploy.sh` / `teardown.sh` | สร้าง / ลบ resource บน GCP (idempotent) |
| `tests/` | unit test (รันได้โดยไม่ต่อ network) |

---

## 🧠 การออกแบบที่สำคัญ (ทำไมถึงทำแบบนี้)

- **Grain = event-level** — 1 แถว = 1 event เก็บเกือบทุก field จาก Mongo เพิ่ม `date_id` + `totalCostThb`
  (สรุประดับ user/วันไปทำใน Looker/BigQuery ได้)
- **`date_id` ยึด Asia/Bangkok** — สกัดจาก `eventTimeStamp` (เก็บเป็น **UTC**) แปลงเป็นวันไทย
  ตรงกับ cutoff เที่ยงคืนไทย ส่วนการ display timezone อื่น ๆ แปลงตอนแสดงผลเอา
- **Idempotency = atomic partition replace** — โหลดเข้า `table$YYYYMMDD` ด้วย `WRITE_TRUNCATE`
  → รันซ้ำวันเดิมข้อมูลไม่ซ้ำ/ไม่หาย (ดีกว่า DELETE+INSERT เพราะ atomic)
  ⚠️ จึงต้องดึง **ทุก event ของวันนั้นใหม่ทั้งวัน** ไม่ใช่แค่ event ใหม่บางส่วน
- **Incremental ราย "วัน"** — รอบปกติ reprocess เฉพาะ "เมื่อวาน" (`LOOKBACK_DAYS=1`)
  รันแรก backfill ตั้งแต่ `START_DATE=2026-01-01`; ถ้า job หายไปหลายวันจะ **เติมช่วงที่ขาดให้เอง**
- **Watermark ขยับหลังเขียนสำเร็จเท่านั้น** — fail กลางทาง = watermark ไม่ขยับ → รอบหน้า reprocess ต่อ (กันข้อมูลหาย)
- **เงินเป็น NUMERIC + Decimal ตลอดทาง** — ไม่แปลงเป็น float (กันเพี้ยน)
- **ความลับอยู่ใน Secret Manager เท่านั้น** — `config.py` mask ค่าก่อน log เสมอ

---

## 📦 ตารางที่ pipeline สร้างใน BigQuery

| ตาราง | grain | วิธีเขียน | ใช้ทำอะไร |
|---|---|---|---|
| `credit_service.user_usage_event` | 1 แถว/event | incremental ราย วัน (atomic partition replace) | ข้อมูล event ดิบ + `date_id`/`totalCostThb` |
| `credit_service.package_master_v3` | 1 แถว/package | full reload ทุกรอบ | master ของ package (lean: id, name, price, ...) |
| `credit_service.librechat_users` | 1 แถว/user | full reload ทุกรอบ | `userId` + `isBanned` (จาก db `Librechat`) — ใช้ตัด user ที่โดน ban |
| `credit_service.pipeline_state` | 1 แถว/pipeline | upsert | watermark |
| `credit_service.user_tracking_b2c` | 1 แถว/(month,week,date,user) | **rebuild ทุกรอบ** (BigQuery SQL) | metric B2C: Token Used, totalCostThb, Trial Conversion, Free Trial Token, package list, flags |

### ตาราง B2C (`user_tracking_b2c`) — แปลงจาก notebook PySpark เดิม

`aggregate.py` แปลง logic section **B2C** จาก notebook เป็น BigQuery SQL ตรง ๆ (กรอง `packageId IN (1,2,3,12)`,
window `package_row`/`event_row`/`current_package_flag`, union Trial Conversion→Subscribe, negate eggToken)
อ่านจาก `user_usage_event` + `package_master_v3` ที่ pipeline อัปเดตแล้ว → `CREATE OR REPLACE TABLE` (rebuild เต็ม)

**ตัด user ที่โดน ban:** ก่อน aggregate ทำ **left-anti join** event กับ `librechat_users` ที่ `isBanned=TRUE`
(ตัด event ของ user ที่โดน ban ออกตั้งแต่ระดับ event ก่อนทุก transformation)

> ⚠️ **ต่างจาก notebook เดิมเล็กน้อยโดยตั้งใจ:** `date_id`/`week_id`/`month_id` ยึด **Asia/Bangkok**
> (notebook เดิม `to_date()` ตาม tz ของ cluster ซึ่งอาจเป็น UTC) → ตัวเลขรายวันอาจต่างกันที่ event ใกล้เที่ยงคืน
> ส่วน B2B จะเพิ่มภายหลัง (ตอนนี้ทำเฉพาะ B2C)

## 🚀 วิธี Deploy

### ขั้นที่ 0 — สร้าง GCP project (ทำเองครั้งเดียว)

> ยังไม่เคยมี project มาก่อน เริ่มจากตรงนี้ ใช้ [Google Cloud Shell](https://shell.cloud.google.com) จะง่ายสุด (มี `gcloud`/`bq` พร้อม)

```bash
# 1) login
gcloud auth login

# 2) สร้าง project (เปลี่ยน id ให้ไม่ซ้ำใครทั้งโลก)
gcloud projects create my-mongo-pipeline-123 --name="Mongo Pipeline"

# 3) ผูก billing account (จำเป็น! Cloud Run/NAT/BQ ต้องมี billing)
gcloud billing accounts list                 # ดู ACCOUNT_ID
gcloud billing projects link my-mongo-pipeline-123 --billing-account=XXXXXX-XXXXXX-XXXXXX

# 4) set เป็น project ปัจจุบัน
gcloud config set project my-mongo-pipeline-123
```

> 💸 **เรื่องฟรี / ค่าใช้จ่าย:**
> - GCP มี **เครดิตทดลองฟรี $300 / 90 วัน** สำหรับ account ใหม่ — ครอบ pipeline นี้สบาย ๆ ช่วงทดสอบ
> - BigQuery / Cloud Run Job / Scheduler / Secret Manager → แทบไม่มีค่าใช้จ่ายที่ปริมาณนี้
> - ❗ **Cloud NAT + reserved IP ไม่ฟรี** (~**\$32/เดือน**) เพราะต้องเปิดทิ้งไว้ให้ IP นิ่ง
>   ถ้าเลิกใช้ → รัน `./teardown.sh` เพื่อหยุดค่าใช้จ่ายส่วนนี้

### ขั้นที่ 1 — สร้าง MongoDB read-only user (ทำเองที่ Atlas)

1. **สร้าง user แบบ read-only:** Atlas → Database Access → Add New Database User
   → Built-in Role = **`Only read any database`** (หรือ `read` เฉพาะ `credit_service`)
2. เก็บ connection string (SRV) ของ user นี้ไว้ใส่ Secret Manager ในขั้นถัดไป

> **ไม่ต้องสร้าง index ใหม่** — pipeline ใช้ **default `_id` index** ที่ Atlas มีให้อยู่แล้ว
> (`_id` ฝัง timestamp ตอน insert ในตัว) โดยกรองด้วย `_id` range เพื่อ seek ข้ามข้อมูลเก่า
> แล้วกรอง `eventTimeStamp` แบบเป๊ะอีกชั้น — คุมด้วย `ID_INDEX_BUFFER_HOURS` (default 24 ชม.)
>
> ⚠️ assume ว่า "เวลา insert ≈ eventTimeStamp" (จริงสำหรับ usage event ที่เขียน real-time)
> ถ้า event อาจถูก backdate เกิน buffer ให้เพิ่ม `ID_INDEX_BUFFER_HOURS` หรือตั้ง `0` เพื่อกรอง
> ด้วย `eventTimeStamp` ล้วน (ถูก 100% แต่ scan ทั้ง collection)
> ถ้าภายหลังมีสิทธิ์สร้าง index ที่ `eventTimeStamp` ได้ ก็ยิ่งดี แต่ไม่บังคับ

### ขั้นที่ 2 — รัน deploy.sh

```bash
# แก้ PROJECT_ID ใน deploy.sh ก่อน (บรรทัดบนสุด)

# ครั้งแรก: ส่งค่าความลับผ่าน env (script จะเอาเข้า Secret Manager ให้ ไม่แตะ git)
export MONGODB_URI_VALUE='mongodb+srv://readonly:PASSWORD@cluster0.xxxx.mongodb.net/?retryWrites=true&w=majority&tls=true'
export GCHAT_WEBHOOK_URL_VALUE='https://chat.googleapis.com/v1/spaces/XXXX/messages?key=XXXX&token=XXXX'

chmod +x deploy.sh teardown.sh
./deploy.sh
```

`deploy.sh` จะ: เปิด API → จอง static IP → สร้าง VPC+NAT → สร้าง BQ dataset/tables →
สร้าง secrets → build image → deploy Cloud Run Job (Direct VPC egress) → ตั้ง Scheduler →
ผูก IAM แบบ least-privilege แล้ว **echo static IP ออกมาให้ส่ง admin**

> รัน `./deploy.sh` ซ้ำได้ทุกเมื่อ — เป็น idempotent (เช็คก่อนสร้าง)

### ขั้นที่ 3 — ส่ง IP ให้ admin + ทดสอบ

```bash
# ส่งเลข IP ที่ deploy.sh พิมพ์ออกมา ให้ admin ไป whitelist ใน Atlas (Network Access)

# ทดสอบรันทันที ไม่ต้องรอ 06:00
gcloud run jobs execute mongo-looker-job --region=asia-southeast1

# ดูสถานะ / log
gcloud run jobs executions list --job=mongo-looker-job --region=asia-southeast1
gcloud logging read 'resource.type=cloud_run_job' --limit=50 --freshness=1h
```

---

## 📊 ตั้ง Looker Studio Dashboard

1. ไปที่ [lookerstudio.google.com](https://lookerstudio.google.com) → **Create → Data source**
2. เลือก connector **BigQuery** → project ของคุณ → dataset `credit_service` → table `user_usage_event`
3. **ตั้ง type ให้ถูก:** `date_id` = Date, `totalCostThb` / `*CostUsd` = Number (Currency), `eventTimeStamp` = Date & Time
4. **แนะนำให้ใช้ partition:** เวลาเพิ่ม chart ให้ใส่ filter ที่ `date_id` เสมอ (เช่น last 30 days)
   → Looker จะ query แค่ partition ที่ต้องการ = เร็ว + ถูก
5. **เปิด cache:** File → Report settings → Data freshness ตั้งเป็น **12 ชั่วโมง** (ข้อมูลอัปเดตวันละครั้งอยู่แล้ว)
   ลดการ query BigQuery ซ้ำ ๆ = ประหยัด
6. ตัวอย่าง chart: รายจ่าย THB ต่อวัน (`date_id` × `SUM(totalCostThb)`),
   Top users (`userId` × `SUM(totalCostThb)`), แยกตาม `aiModel` / `eventType`

> 💡 ถ้า dashboard ซับซ้อน แนะนำสร้าง **view** สรุปใน BigQuery (เช่น `daily_user_cost`)
> แล้วให้ Looker ต่อ view แทน table ดิบ — query เบากว่า

---

## 🔧 Troubleshooting

| อาการ | สาเหตุ / วิธีแก้ |
|---|---|
| แจ้งเตือน **IP DRIFT** | egress IP จริง ≠ `EXPECTED_EGRESS_IP` → Cloud NAT/IP อาจถูกแก้ ตรวจ `gcloud compute addresses describe pipeline-egress-ip --region=asia-southeast1` แล้วอัปเดต env ของ job |
| ต่อ Mongo ไม่ได้ (timeout) | IP ยังไม่ถูก whitelist ที่ Atlas / user ผิด / cluster pause — เช็ค Atlas Network Access |
| `ConfigError: ENV ที่จำเป็นขาด...` | secret/env ไม่ถูก inject — ตรวจ `--set-secrets` ของ job + ว่ามี secret version |
| BQ `Access Denied` ตอน query | runtime SA ขาด `bigquery.jobUser` — deploy.sh ผูกให้แล้ว ลองรันซ้ำ |
| ข้อมูลซ้ำใน BQ | ไม่ควรเกิด (atomic partition replace) ถ้าเกิดให้เช็คว่าไม่มีใคร append เข้า table ตรง ๆ |
| อยากย้อน backfill ใหม่ | ลบ row ใน `pipeline_state` (`DELETE ... WHERE pipeline_name='user_usage_event'`) แล้วรัน job — จะ backfill ตั้งแต่ `START_DATE` |
| Google Chat ไม่เด้ง | webhook หมดอายุ/ถูกลบ → สร้างใหม่ที่ Space → Manage webhooks แล้ว `gcloud secrets versions add gchat-webhook-url --data-file=-` |

---

## ✅ สรุปสิ่งที่ต้องทำเอง (manual checklist)

- [ ] สร้าง GCP project + ผูก billing (ขั้นที่ 0)
- [ ] สร้าง MongoDB **read-only user** (ขั้นที่ 1 — ไม่ต้องสร้าง index, ใช้ default `_id` index)
- [ ] แก้ `PROJECT_ID` ใน `deploy.sh` และ `teardown.sh`
- [ ] `export` ค่า `MONGODB_URI_VALUE` + `GCHAT_WEBHOOK_URL_VALUE` แล้วรัน `./deploy.sh`
- [ ] **ส่ง static IP ให้ admin whitelist** ใน Atlas
- [ ] ทดสอบ `gcloud run jobs execute ...` แล้วเช็ค Google Chat ว่าได้ข้อความ
- [ ] สร้าง Looker Studio dashboard (ต่อ BigQuery connector + เปิด cache)
- [ ] (ถ้ากังวล webhook ที่เคยวางในแชต) **regenerate Google Chat webhook** แล้วอัปเดต secret

---

## 🧪 รัน test (local)

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest -q
```

> `requirements.txt` pin เวอร์ชันสำหรับ **container (Python 3.12)**
> ถ้าเครื่อง local เป็น Python ใหม่กว่าและ install ไม่ผ่าน ให้ใช้เวอร์ชันล่าสุดแทนตอนรัน test
