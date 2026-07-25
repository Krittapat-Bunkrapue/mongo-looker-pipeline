# คู่มือสร้างหน้า "Summary Report" ใน Looker Studio

หน้ารวมสำหรับผู้บริหาร/ทีม — ตอบ 4 คำถามในหน้าเดียว: **โตไหม · เงินเป็นยังไง · ลูกค้าอยู่ต่อไหม · เงินไหลไปไหน**

> ชั้นข้อมูลเตรียมไว้ให้หมดแล้ว หน้านี้จึงเป็นแค่งาน "ลากฟิลด์" ไม่ต้องเขียนสูตรซับซ้อน

---

## 1. Data source ที่ต้องเพิ่ม (4 ตัว)

Looker Studio → **Resource → Manage added data sources → Add a data source → BigQuery**
project `trueaihub-mongo-pipeline-2026`

| # | ตาราง/วิว | ใช้ทำอะไรในหน้านี้ | grain |
|---|---|---|---|
| **A** | `Total.summary_daily` | ยอดสมัคร / trial / หมดอายุ / รายได้ / ต้นทุน AI | วัน × version × package |
| **B** | `Total.user_tracking_total_compat` *(มีอยู่แล้ว)* | จำนวนผู้ใช้ (นับไม่ซ้ำได้ทุกช่วงเวลา) | วัน × user |
| **C** | `B2C.user_repeat_behavior` | อัตราซื้อซ้ำ / churn / กลุ่มพฤติกรรม | user |
| **D** | `Total.summary_model_daily` | ต้นทุนแยกตามโมเดล AI | วัน × version × model |

⚠️ **กฎเหล็ก 2 ข้อ**
1. **นับจำนวนคน ให้ใช้ source B เท่านั้น** (`COUNT DISTINCT userId`) — ฟิลด์ `daily_active_users` ใน A/D เป็นค่าราย *วัน* บวกข้ามวันไม่ได้ (จะนับคนเดิมซ้ำ)
2. **ถ้าต้องการให้ตัวเลข B2B ตรงกับหน้า B2B เดิมเป๊ะ** ให้ใส่ filter `has_company_mapping = TRUE` (มี B2B 46 คนที่ไม่มี team/company ซึ่งหน้าเดิมตัดทิ้ง แต่ summary เก็บไว้)

---

## 2. โครงหน้า (แนะนำ 5 แถว)

### แถว 0 — ตัวควบคุม (บนสุด)
| Control | ตั้งค่า |
|---|---|
| Date range control | default: **Last 30 days** |
| Drop-down list | `version` (B2C / B2B) |
| Drop-down list | `packageName` |

> ตั้ง date range เป็น **report-level** (Make report-level) เพื่อให้คุมทุก chart พร้อมกัน

### แถว 1 — Scorecard 6 ใบ (ตัวเลขสำคัญ)
| การ์ด | Source | Metric | หมายเหตุ |
|---|---|---|---|
| ผู้ใช้ที่ใช้งานจริง | B | `COUNT DISTINCT userId` | เลือก comparison = previous period |
| สมัครใหม่ (จ่ายเงิน) | A | `SUM(new_paid_subscriptions)` | |
| เริ่มทดลองฟรี | A | `SUM(new_trial_subscriptions)` | |
| รายได้ (ประมาณการ) | A | `SUM(revenue_thb_est)` | ราคาตั้ง ไม่รวมส่วนลด |
| ต้นทุน AI | A | `SUM(serving_cost_thb)` | |
| หมดอายุ / ไม่ต่อ | A | `SUM(expirations)` | |

💡 เปิด **Comparison date range = Previous period** ทุกใบ → ได้ลูกศร ↑↓ % อัตโนมัติ

### แถว 2 — เทรนด์ (2 กราฟ)
- **Time series** — X: `date_id`, Y: `COUNT DISTINCT userId` *(source B)* → ผู้ใช้รายวัน
- **Combo chart** — X: `date_id`, Bar: `revenue_thb_est`, Line: `serving_cost_thb` *(source A)* → รายได้ vs ต้นทุน

### แถว 3 — สัดส่วน (3 กราฟ)
- **Donut** — `version` × `SUM(serving_cost_thb)` → B2C vs B2B
- **Bar** — `packageName` × `SUM(revenue_thb_est)` → แพ็คไหนทำเงิน
- **Bar (แนวนอน)** — `aiModel` × `SUM(serving_cost_thb)` *(source D)*, Sort DESC, Limit 10 → เงินไหลไปโมเดลไหน

### แถว 4 — สุขภาพลูกค้า (source C — B2C เท่านั้น)
- **Scorecard**: อัตราซื้อซ้ำ → calculated field:
  ```
  SUM(CASE WHEN is_repeat THEN 1 ELSE 0 END) / SUM(CASE WHEN paid_subscribe_cnt >= 1 THEN 1 ELSE 0 END)
  ```
  (ตั้ง Type = Percent) — เติม filter `is_mature_cohort = true` จะได้ตัวเลขที่แฟร์กว่า
- **Pie/Bar**: `user_type` (repeat / paid_once / trial_only) × `COUNT userId`
- **Bar**: `dominant_segment` × `COUNT userId` → กลุ่มพฤติกรรม (ใช้หมด-ซื้อซ้ำ / ใช้ไม่หมด-หาย ฯลฯ)
- **Table**: `first_paid_packageName` + repeat rate + `AVG(revenue_thb)`

### แถว 5 — ท้ายหน้า
Text box ระบุที่มา + ข้อจำกัด:
> ข้อมูลอัปเดตอัตโนมัติทุกวัน 06:00 น. (ครอบถึงเมื่อวาน) · รายได้เป็นประมาณการจากราคาตั้ง ·
> ผู้ใช้ที่ถูกระงับ (banned) ถูกตัดออกจากทุกตัวเลข

---

## 3. ตั้งค่าให้เร็ว + ประหยัด

1. **File → Report settings → Data freshness = 12 ชั่วโมง** (ข้อมูลเข้าวันละครั้ง ไม่ต้อง query ถี่)
2. ทุก chart ควรอยู่ใต้ date range control (จะ prune partition อัตโนมัติ)
3. อย่าใช้ `user_usage_event` ดิบเป็น source ของหน้านี้ — ใหญ่และช้า ให้ใช้ view ที่เตรียมไว้

---

## 4. เช็คก่อนส่งมอบ

- [ ] ตัวเลข B2C ในหน้า summary ตรงกับหน้า B2C เดิม
- [ ] B2B: กด filter `has_company_mapping = TRUE` แล้วตรงกับหน้า B2B เดิม
- [ ] Scorecard "ผู้ใช้" มาจาก source B (ไม่ใช่ `daily_active_users`)
- [ ] เปลี่ยน date range แล้วทุก chart ขยับตาม

---

## ภาคผนวก — ฟิลด์ที่มีใน `Total.summary_daily`

`version` · `date_id` · `packageId` · `packageName` · `is_trial` · `has_company_mapping` ·
`daily_active_users` · `usage_events` · `tokens_used` · `serving_cost_thb` ·
`new_subscriptions` · `new_paid_subscriptions` · `new_trial_subscriptions` ·
`expirations` · `monthly_resets` · `revenue_thb_est`

**`Total.summary_model_daily`**: `version` · `date_id` · `aiModel` · `has_company_mapping` ·
`daily_active_users` · `usage_events` · `tokens_used` · `serving_cost_thb`
