# คู่มือ: ทำให้ Google Slides deck อัปเดตตัวเลขเอง

สำหรับ deck โครงต้นไม้ผู้ใช้ (Total Users → B2C/B2B → แพ็คเกจ → Free Trial Conversion)

**หลักการ:** ไม่ดึงจาก Looker Studio (ไม่มี API) แต่ดึงจาก **BigQuery ต้นทางเดียวกับ Looker**
→ ตัวเลขตรงกันเสมอ และอัปเดตอัตโนมัติได้

---

## แบ่งงาน 2 ส่วน

| ส่วน | วิธี | ผลลัพธ์ |
|---|---|---|
| **ตัวเลขในกล่อง** (7,653 / 6,196 / 78 / 86 …) | B — pipeline แทน placeholder | อัปเดตเองทุก 06:00 ไม่ต้องกดอะไร |
| **กราฟ 2 ตัว** (company size, new user รายสัปดาห์) | A — Connected Sheets → linked chart | กดปุ่ม Update ในสไลด์ทีเดียว |

---

# ส่วน A — กราฟ (Connected Sheets → linked chart)

### A1. สร้าง Google Sheets ต่อ BigQuery
1. สร้าง Google Sheets ใหม่ → **Data → Data connectors → Connect to BigQuery**
2. เลือก project `trueaihub-mongo-pipeline-2026` → dataset **Total** → เลือก view:

| กราฟบนสไลด์ | ใช้ view |
|---|---|
| `# Company by company size` | `Total.slide_company_size` |
| `New User` (B2B / B2C รายสัปดาห์) | `Total.slide_new_users_weekly` |

3. กด **Extract**/**Connect** แล้วตั้ง **Refresh schedule → รายวัน** (แนะนำ 07:00 น. หลัง pipeline รันเสร็จ)

### A2. สร้างกราฟใน Sheets
- **Company size:** Chart type = Column · X = `company_size_range` · Y = `companies` · เรียงตาม `bin_order`
- **New User:** กรอง `segment` = B2C หรือ B2B (ทำ 2 ชีต) · X = `week_id` · Bar = `new_users` · Line = `new_users_cumulative`

### A3. ฝังเข้า Slides แบบ linked
1. ในสไลด์: **Insert → Chart → From Sheets** → เลือกไฟล์และกราฟ
2. ✅ ติ๊ก **Link to spreadsheet** (สำคัญ — ถ้าไม่ติ๊กจะกลายเป็นรูปนิ่ง)
3. เวลาข้อมูลเปลี่ยน จะมีปุ่ม **Update** ขึ้นมุมกราฟ กดทีเดียวอัปเดตทั้งเด็ค

> 💡 อยากให้ auto ไม่ต้องกด: ใส่ Apps Script ใน deck แล้วตั้ง trigger รายวัน
> ```javascript
> function refreshCharts() {
>   SlidesApp.openById('PRESENTATION_ID').getSlides()
>     .forEach(s => s.getSheetsCharts().forEach(c => c.refresh()));
> }
> ```

---

# ส่วน B — ตัวเลข (pipeline แทน placeholder อัตโนมัติ)

### B1. เตรียมสิทธิ์ (ทำครั้งเดียว)

```bash
# เปิด API
gcloud services enable slides.googleapis.com drive.googleapis.com \
  --project=trueaihub-mongo-pipeline-2026
```

แชร์ deck ให้ service account เป็น **Editor**:
```
pipeline-job-sa@trueaihub-mongo-pipeline-2026.iam.gserviceaccount.com
```

### B2. วาง placeholder ในสไลด์
แทนตัวเลขในกล่องด้วยข้อความ `{{...}}` (พิมพ์ตรง ๆ ในกล่องเดิม รูปแบบ/ฟอนต์คงเดิม)

| กล่องบนสไลด์ | ใส่ placeholder | ตัวอย่างค่าจริง |
|---|---|---|
| # Total Users | `{{total_users}}` | 7,453 |
| # B2C | `{{b2c_users}}` | 6,930 |
| # B2B | `{{b2b_users}}` | 523 |
| # Subscriber (B2C) | `{{b2c_subscriber}}` | 513 |
| # Starter | `{{b2c_starter}}` | 148 |
| # Standard | `{{b2c_standard}}` | 302 |
| # Pro | `{{b2c_pro}}` | 63 |
| # Free Trial | `{{b2c_free_trial}}` | 6,417 |
| # Free Trial Conversion | `{{free_trial_conversion}}` | 97 |
| # To Starter | `{{to_starter}}` | 42 |
| # To Standard | `{{to_standard}}` | 45 |
| # To Pro | `{{to_pro}}` | 10 |
| # Subscriber (B2B) | `{{b2b_subscriber}}` | 239 |
| # Biz Starter | `{{b2b_biz_starter}}` | 80 |
| # Biz Standard | `{{b2b_biz_standard}}` | 53 |
| # Biz Pro | `{{b2b_biz_pro}}` | 106 |
| # Others (B2B) | `{{b2b_others}}` | 284 |
| จำนวนบริษัท | `{{b2b_companies}}` | 205 |
| **YTD Period (21 July 2026)** | `{{period}}` | 10 August 2026 |

### B3. เปิดใช้งาน
ใส่ presentation ID (ส่วนกลาง URL: `docs.google.com/presentation/d/`**`<ID>`**`/edit`)
ลงในตัวแปร `SLIDES_PRESENTATION_ID` ที่หัวไฟล์ `deploy.sh` แล้ว:
```bash
bash deploy.sh
gcloud run jobs execute mongo-looker-job --region=asia-southeast1 --wait
```

ถ้าปล่อยว่าง = ระบบข้ามขั้นตอนนี้ไปเฉย ๆ (ไม่ทำให้ pipeline ล้ม)

---

## นิยามตัวเลข (สำคัญ — ต้องตรงกับที่ทีมเข้าใจ)

- **1 user นับครั้งเดียว** จัดกลุ่มตาม **แพ็คเกจของ subscription ล่าสุด**
  → ยอดลูกบวกกันได้เท่ากับยอดแม่เสมอ (B2C = Subscriber + Free Trial, B2B = Subscriber + Others)
- **Subscriber** = แพ็คเสียเงิน (B2C: Starter/Standard/Pro · B2B: Biz Starter/Standard/Pro)
- **Others (B2B)** = แพ็คที่ไม่ใช่ 3 ตัวนั้น เช่น Free Trial VIP, Free Trial Standard, Instructor
- **Free Trial Conversion** = ผู้ใช้ B2C ที่เคยได้ trial **แล้วต่อมาสมัครแพ็คเสียเงิน**
  (To Starter/Standard/Pro = แพ็คแรกที่จ่ายเงิน — บวกกันได้เท่ากับยอด conversion)
- **ตัดออก:** ผู้ใช้ B2C ที่ถูกระงับ (isBanned) และแพ็คทดสอบภายในของ B2B (id 5, 7, 10, 97, 98)
- **`{{period}}`** = วันที่ล่าสุดที่มีข้อมูล (T-1 จากวันที่รัน)

> ตรวจสอบตัวเลขได้ตลอดด้วย:
> ```sql
> SELECT metric_key, value_display FROM `trueaihub-mongo-pipeline-2026.Total.slide_metrics` ORDER BY sort_order
> ```

---

## ข้อควรพิจารณาด้านความปลอดภัย

วิธี B ต้องให้ service account มีสิทธิ์ **Editor** บน deck — ระบบจะแตะเฉพาะข้อความรูปแบบ `{{...}}`
เท่านั้น (ใช้ `replaceAllText` ของ Slides API) ไม่ยุ่งกับ layout/รูป/ข้อความอื่น
ถ้าไม่สบายใจ ใช้เฉพาะวิธี A ก็ได้ — ไม่ต้องให้สิทธิ์เขียนใด ๆ แต่ต้องพิมพ์ตัวเลขเอง
