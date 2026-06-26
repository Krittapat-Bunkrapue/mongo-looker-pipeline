# CLAUDE.md

คู่มือสำหรับ Claude Code (และคนใหม่) ที่เข้ามาทำงานใน repo นี้ — อ่านก่อนเริ่มเสมอ

---

## 1. ภาพรวมโปรเจค

ETL pipeline แบบ production: ดึง usage/billing data จาก **MongoDB 2 server (B2C + B2B)** →
แปลงด้วย **Python (pandas)** + **BigQuery SQL** → เก็บใน **BigQuery (3 dataset)** → แสดงผลใน **Looker Studio**
รัน **อัตโนมัติทุกวัน 06:00 น. (Asia/Bangkok)** ผ่าน Cloud Scheduler → Cloud Run Job
โดยออกเน็ตด้วย **static IP เดียว** (Cloud NAT) เพื่อให้ admin whitelist ที่ MongoDB ครั้งเดียวจบ

```
Cloud Scheduler (06:00 Asia/Bangkok)
  └─► Cloud Run Job (job เดียว, container Python)
        ├─ egress ผ่าน Cloud NAT + Static IP 34.21.253.252  ◄── whitelist IP นี้ที่ Mongo ทั้ง 2 server
        ├─ B2C:  Mongo server #1 ──► transform ──► dataset B2C
        ├─ B2B:  Mongo server #2 ──► transform ──► dataset B2B
        ├─ rebuild aggregate (BigQuery SQL): user_tracking_b2c / user_tracking_b2b
        ├─ view: Total.user_tracking_total (union B2C+B2B) + *_compat views
        └─ แจ้งผล success / fail / IP-drift ผ่าน Google Chat webhook
```

---

## 2. Identifiers / Infra (ค่าจริงของระบบนี้ — ไม่ใช่ความลับ)

| รายการ | ค่า |
|---|---|
| GCP project | `trueaihub-mongo-pipeline-2026` |
| Region | `asia-southeast1` |
| Static egress IP | `34.21.253.252` (reserved: `pipeline-egress-ip`) |
| Cloud Run Job | `mongo-looker-job` |
| Cloud Scheduler | `mongo-looker-daily` (`0 6 * * *`, Asia/Bangkok) |
| Service accounts | `pipeline-job-sa` (runtime), `pipeline-scheduler-sa` (trigger) |
| Secrets (Secret Manager) | `mongodb-uri`, `mongodb-uri-b2b`, `gchat-webhook-url` |
| VPC / NAT | `pipeline-vpc` / `pipeline-subnet` (10.100.0.0/26) / `pipeline-router` / `pipeline-nat` |
| Artifact Registry | repo `pipeline-repo`, image `mongo-looker-pipeline` |
| GitHub | `Krittapat-Bunkrapue/mongo-looker-pipeline` (private, branch `main`) |

**ความลับ** (Mongo URI ทั้ง 2, webhook) อยู่ใน **Secret Manager เท่านั้น** — ห้าม hardcode/commit เด็ดขาด

---

## 3. โครงสร้างไฟล์ (modules)

| ไฟล์ | หน้าที่ |
|---|---|
| `config.py` | โหลด+validate env/secret (fail fast). มี `Config` dataclass + properties สร้าง FQN ของตาราง BQ ทุกตัว (B2C/B2B/Total) |
| `state.py` | อ่าน/เขียน watermark (วันล่าสุดที่สำเร็จ) ใน BQ table `pipeline_state` (แยกต่อ dataset) |
| `extract.py` | ต่อ Mongo (TLS+timeout) + aggregation pipeline ราย วัน (`extract_day`) + ดึงทั้ง collection (`extract_full`, รับ `db_name` override). มี projection ของแต่ละ collection |
| `transform.py` | pandas: `normalize_records` (event), `normalize_packages`, `normalize_users` (B2C), `normalize_b2b_users/company/team`. คุม type → BQ (Decimal→NUMERIC, ฯลฯ) |
| `load.py` | เขียน BQ idempotent: `write_day` (atomic partition replace), `write_full_table` (full reload). เป็น **single source of truth ของ schema** ทุกตาราง |
| `aggregate.py` | BigQuery SQL: `build_b2c_sql`, `build_b2b_sql`, `build_total_view_sql`, `build_compat_view_sql` + ตัว `run_*`/`ensure_*` |
| `notify.py` | Google Chat webhook: success / fail / ip_drift (มี retry, mask secret) |
| `main.py` | orchestrate ทั้ง pipeline (B2C → B2B → Total view → compat views) + error handling + IP drift check |
| `deploy.sh` / `teardown.sh` | สร้าง/ลบ resource บน GCP (idempotent, ตัวแปรบนสุด) |
| `tests/` | unit test (รันได้โดยไม่ต่อ network — mock/pure functions) |

---

## 4. ตาราง/วิว ใน BigQuery

มี 3 dataset: **`B2C`**, **`B2B`**, **`Total`**
> ⚠️ ชื่อ Mongo database ยังเป็น `credit_service`/`Librechat` — เปลี่ยนแค่ชื่อ **BigQuery dataset** เป็น B2C/B2B
> (config แยก `MONGO_DB` กับ `BQ_DATASET` คนละตัวแปร — อย่าสับสน)

| dataset.ตาราง | ชนิด | grain | หมายเหตุ |
|---|---|---|---|
| `B2C/B2B . user_usage_event` | TABLE | 1/event | partition `date_id`, cluster `userId[,eventType]`, incremental ราย วัน |
| `B2C/B2B . package_master_v3` | TABLE | 1/package | full reload |
| `B2C . librechat_users` | TABLE | 1/user | `userId`+`isBanned` (ตัด banned ใน B2C) |
| `B2B . librechat_users` | TABLE | 1/user | `userId`+`teamId`+`teamName` (map team/company) |
| `B2B . b2b_company`, `b2b_team` | TABLE | master | map team→company + ขนาดบริษัท |
| `B2C/B2B . pipeline_state` | TABLE | 1/pipeline | watermark |
| `B2C . user_tracking_b2c` | TABLE | 1/(month,week,date,user) | **rebuild ทุกรอบ** (CREATE OR REPLACE) |
| `B2B . user_tracking_b2b` | TABLE | 1/(month,week,date,user) | **rebuild ทุกรอบ** |
| `Total . user_tracking_total` | **VIEW** | union | B2C+B2B + คอลัมน์ `version` + fillna('null') |
| `*_compat` (ทุก dataset) | **VIEW** | — | สำหรับ Looker dashboard เดิม (cast month_id/week_id เป็น INT64) |

---

## 5. Logic ธุรกิจ (แปลงจาก notebook PySpark `user_tracking.ipynb`)

- **B2C** (`build_b2c_sql`): กรอง `packageId IN (1,2,3,12)`; **ตัด user ที่ `isBanned=TRUE`** ด้วย left-anti join *ก่อน* aggregate;
  มี Trial Conversion (union แถว Trial Conversion → Subscribe), Free Trial Token; window `package_row`/`event_row`/`current_package_flag`
- **B2B** (`build_b2b_sql`): กรอง `packageId NOT IN (5,7,10,97,98)`; **ไม่ตัด banned** (ตาม notebook);
  เพิ่มมิติ **team/company** + `company_size_range` (bin ทีละ 10 คน) + window `company_first_event_row`/`team_first_event_row`
- ทั้งคู่: `eggToken` ของ event `Token Used` ถูก **กลับเครื่องหมาย (×-1)** ก่อน sum; `totalCostThb = totalCostUsd × 32.67`
- `date_id`/`week_id`/`month_id` ยึด **Asia/Bangkok**; ขอบบน `< CURRENT_DATE` = cutoff เที่ยงคืน → ข้อมูล "ถึงเมื่อวาน"
- `run_date` = **data as of (T-1)** = `DATE_SUB(CURRENT_DATE, 1)` (ไม่ใช่วันที่รัน)

---

## 6. Invariants / กฎที่ห้ามพัง

1. **Idempotency** — เขียน BQ ด้วย atomic partition replace (`table$YYYYMMDD` + WRITE_TRUNCATE) หรือ WRITE_TRUNCATE ทั้งตาราง; **ห้าม append เปล่า**
2. **Watermark ขยับหลังเขียนสำเร็จเท่านั้น** — fail กลางทาง watermark ไม่ขยับ → รอบหน้า reprocess ต่อ (กันข้อมูลหาย)
3. **เงินเป็น Decimal/NUMERIC ตลอดทาง** — ห้ามแปลงเป็น float
4. **ความลับอ่านจาก env (Secret Manager) เท่านั้น** + mask ก่อน log; `config.py` fail fast ถ้าขาด
5. **incremental ราย "วัน"** (ไม่ใช่ราย event) เพราะ partition replace ต้องโหลดทั้งวัน; aggregate ใช้ window ครอบทั้งประวัติ user → **rebuild เต็ม** ทุกรอบ
6. แก้ schema ตาราง = แก้ที่ `load.py` (source of truth) เท่านั้น แล้ว transform/validate จะตามให้

---

## 7. Dev บนเครื่อง (Windows) — สำคัญมาก

เครื่อง dev เป็น **Windows + PowerShell**, Python 3.14 (container ใช้ 3.12). gcloud ติดตั้งผ่าน winget แล้ว

**gcloud/bq อยู่ที่:** `C:\Users\kritt\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin`

### รัน gcloud/bq จาก Git Bash (สำหรับ deploy.sh)
```bash
export PATH="/c/Users/kritt/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin:$PATH"
export CLOUDSDK_CORE_DISABLE_PROMPTS=1
```

### รัน bq query (⚠️ gotcha) — bq หา python ไม่เจอถ้าไม่ตั้ง CLOUDSDK_PYTHON
```bash
export PATH="/c/.../google-cloud-sdk/bin:$PATH"
export CLOUDSDK_PYTHON="/c/Users/kritt/AppData/Local/Google/Cloud SDK/google-cloud-sdk/platform/bundledpython/python.exe"
bq query --use_legacy_sql=false < query.sql      # multi-line ใช้ < file (อย่าส่งเป็น arg ผ่าน cmd)
```
- บน PowerShell: ตั้ง `$env:CLOUDSDK_PYTHON` + ใส่ bin ใน `$env:PATH` (เพราะ bq เรียก gcloud หา auth)
- **อย่าใช้ string literal เครื่องหมาย `"`** ใน query ผ่าน PowerShell/cmd (โดน strip) — ใช้ single-quote หรืออ่านจากไฟล์
- `bq head` ใช้กับ VIEW ไม่ได้ → ใช้ `bq query ... LIMIT`

### Render SQL จาก build_*_sql แล้ว dry-run (validate ก่อน deploy)
```bash
export PYTHONIOENCODING=utf-8 PYTHONUTF8=1      # SQL มีคอมเมนต์ไทย
./.venv/Scripts/python.exe -c "from datetime import date; from aggregate import build_b2b_sql; print(build_b2b_sql(...))" > /tmp/q.sql
bq query --use_legacy_sql=false --dry_run < /tmp/q.sql
```
> หมายเหตุ: Windows python เขียน `/tmp/` ไม่ได้ (มองเป็น `C:\tmp`) → ใช้ bash redirect `> /tmp/x` แทน `open()`

### Test (ไม่ต่อ network)
```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements-dev.txt
.venv/Scripts/python -m pytest -q
```
`.venv/` ถูก gitignore — สร้างใหม่ได้ตลอด

---

## 8. Commands cheat-sheet

| งาน | คำสั่ง |
|---|---|
| Test | `.venv/Scripts/python -m pytest -q` |
| Deploy (rebuild image + redeploy) | `bash deploy.sh` (idempotent) |
| รัน job ทันที | `gcloud run jobs execute mongo-looker-job --region=asia-southeast1 --wait` |
| ดู executions | `gcloud run jobs executions list --job=mongo-looker-job --region=asia-southeast1` |
| ดู log error | `gcloud logging read 'resource.type="cloud_run_job" AND severity>=ERROR' --freshness=1h --format='value(textPayload)'` |
| นับแถวตาราง | `bq show --format=prettyjson <proj>:<ds>.<table>` (อ่าน `numRows`) |
| Teardown ทั้งหมด | `bash teardown.sh` (หรือ `--keep-data` เก็บ dataset) |

**Workflow แก้โค้ด:** แก้ local → `pytest` → commit → `git push` → `bash deploy.sh` → `gcloud run jobs execute`
> เปลี่ยนเฉพาะ aggregate SQL: render + `bq query < file` rebuild ได้เลย (เร็วกว่า ไม่แตะ Mongo) แต่ยังต้อง commit+redeploy ให้ job รอบ 06:00 ใช้โค้ดใหม่

---

## 9. Gotchas / บทเรียน (เคยพลาดมาแล้ว — อย่าซ้ำ)

- **BigQuery/Looker ห้ามเว้นวรรคในชื่อคอลัมน์** → "Invalid field name error". ใช้ snake_case เสมอ
- **`DIV` เป็นฟังก์ชัน** ใน BigQuery: `DIV(x, y)` ไม่ใช่ `x DIV y` (อันนั้น MySQL)
- **Dockerfile copy `*.py`** (ไม่ระบุชื่อทีละไฟล์) — เคยลืมเพิ่ม `aggregate.py` แล้ว ModuleNotFound
- **Cloud Run Job ต้อง `--vpc-egress=all-traffic`** + `--network/--subnet` (Direct VPC egress) ไม่งั้น IP ไม่นิ่ง
- **master key เป็น NULLABLE** — `Librechat.users` ฝั่ง B2B มี `userId=null` (REQUIRED จะ reject ตอนเขียน BQ)
- **Secret Mongo URI**: ถ้า auth fail (code 18) แต่ต่อ server ได้ → password ไม่ถูก/มีอักขระพิเศษต้อง URL-encode/authSource ผิด (ไม่ใช่เรื่อง IP)
- **`.gitattributes` บังคับ LF** — `.sh` ที่เป็น CRLF จะพังบน Linux/Cloud Shell
- secret อ่าน `:latest` ตอนรัน → แก้ secret แล้ว execute ใหม่ได้เลย ไม่ต้อง redeploy

---

## 10. Looker Studio

- dashboard ต่อที่ `*_compat` view (ชื่อ field/type ใกล้ของเดิมสุด): `B2C/B2B/Total.user_tracking_*_compat`
- dashboard เดิม (มาจาก CSV) มี 3 field ชื่อมีเว้นวรรค (`Trial Conversion cnt`/`Token Used`/`Free Trial Token Used`) →
  ตอน swap source ต้อง **map มือครั้งเดียว** ไปที่ snake_case (`trial_conversion_cnt`/`token_used`/`free_trial_token_used`)
- วิธี repoint: Resource → Manage added data sources → Edit → Edit connection → เลือก compat view → Reconnect → Apply
- เปิด cache (Data freshness 12 ชม.) เพราะข้อมูลอัปเดตวันละครั้ง

---

## 11. การทำงานที่อยากให้ทำ (convention กับ user)

- ภาษา: คุยกับ user เป็น **ภาษาไทย**
- ทำเป็นขั้นตอน, จุดที่กระทบความถูกต้อง/ความปลอดภัย → **หยุดถามก่อน อย่าเดา** ค่าความลับ/ค่าเฉพาะระบบ
- มี gcloud บนเครื่องแล้ว → Claude deploy/execute/verify ให้เองได้ (รัน background + monitor)
- หลังแก้โค้ดทุกครั้ง: `pytest` ให้ผ่าน + commit/push ก่อน
- งานค้าง: ดู `[[mongo-looker-pipeline]]` ใน auto-memory (สถานะล่าสุด + TODO)
