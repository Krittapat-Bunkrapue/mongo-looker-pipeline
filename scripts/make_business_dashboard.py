# -*- coding: utf-8 -*-
"""
สร้าง reports/business_dashboard.html — dashboard ภาพรวมธุรกิจ (self-contained HTML)

วิธีใช้:  python scripts/make_business_dashboard.py
ต้อง `gcloud auth login` ไว้ก่อน (สคริปต์เรียกผ่าน bq CLI จึงใช้ credential เดียวกับ gcloud)
ข้อมูลดึงสดจาก BigQuery ทุกครั้งที่รัน
"""
import json, os, shutil, subprocess, sys

PROJECT = os.environ.get("GCP_PROJECT_ID", "trueaihub-mongo-pipeline-2026")
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "reports", "business_dashboard.html")

QUERIES = {
    "kpi": "WITH d AS (SELECT MAX(date_id) AS mx FROM `trueaihub-mongo-pipeline-2026.Total.summary_daily`),\ncur AS (SELECT\n  SUM(revenue_thb_est) rev, SUM(serving_cost_thb) cost, SUM(new_paid_subscriptions) subs,\n  SUM(new_trial_subscriptions) trials, SUM(expirations) exp, SUM(tokens_used) tok\n  FROM `trueaihub-mongo-pipeline-2026.Total.summary_daily`, d WHERE date_id > DATE_SUB(d.mx, INTERVAL 30 DAY)),\nprev AS (SELECT\n  SUM(revenue_thb_est) rev, SUM(serving_cost_thb) cost, SUM(new_paid_subscriptions) subs,\n  SUM(new_trial_subscriptions) trials, SUM(expirations) exp, SUM(tokens_used) tok\n  FROM `trueaihub-mongo-pipeline-2026.Total.summary_daily`, d\n  WHERE date_id > DATE_SUB(d.mx, INTERVAL 60 DAY) AND date_id <= DATE_SUB(d.mx, INTERVAL 30 DAY)),\nau_cur AS (SELECT COUNT(DISTINCT userId) u FROM `trueaihub-mongo-pipeline-2026.Total.user_tracking_total`, d\n  WHERE date_id > DATE_SUB(d.mx, INTERVAL 30 DAY)),\nau_prev AS (SELECT COUNT(DISTINCT userId) u FROM `trueaihub-mongo-pipeline-2026.Total.user_tracking_total`, d\n  WHERE date_id > DATE_SUB(d.mx, INTERVAL 60 DAY) AND date_id <= DATE_SUB(d.mx, INTERVAL 30 DAY)),\nallt AS (SELECT SUM(revenue_thb_est) rev, SUM(serving_cost_thb) cost,\n  SUM(new_paid_subscriptions) subs, MIN(date_id) d0, MAX(date_id) d1\n  FROM `trueaihub-mongo-pipeline-2026.Total.summary_daily`)\nSELECT d.mx AS data_end, cur.rev cur_rev, prev.rev prev_rev, cur.cost cur_cost, prev.cost prev_cost,\n  cur.subs cur_subs, prev.subs prev_subs, cur.trials cur_trials, prev.trials prev_trials,\n  cur.exp cur_exp, prev.exp prev_exp, cur.tok cur_tok, prev.tok prev_tok,\n  au_cur.u cur_users, au_prev.u prev_users,\n  allt.rev all_rev, allt.cost all_cost, allt.subs all_subs, allt.d0 first_date\nFROM d, cur, prev, au_cur, au_prev, allt",
    "weekly": "SELECT FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(s.date_id, WEEK(MONDAY))) AS wk,\n  CAST(SUM(s.revenue_thb_est) AS INT64) AS revenue,\n  CAST(SUM(s.serving_cost_thb) AS INT64) AS cost,\n  CAST(SUM(s.new_paid_subscriptions) AS INT64) AS paid_subs,\n  CAST(SUM(s.new_trial_subscriptions) AS INT64) AS trials\nFROM `trueaihub-mongo-pipeline-2026.Total.summary_daily` s\nWHERE s.date_id >= '2026-03-01'\nGROUP BY 1 ORDER BY 1",
    "weekly_users": "SELECT FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(date_id, WEEK(MONDAY))) AS wk,\n  COUNT(DISTINCT IF(version='B2C', userId, NULL)) AS b2c_users,\n  COUNT(DISTINCT IF(version='B2B', userId, NULL)) AS b2b_users\nFROM `trueaihub-mongo-pipeline-2026.Total.user_tracking_total`\nWHERE date_id >= '2026-03-01'\nGROUP BY 1 ORDER BY 1",
    "version": "SELECT s.version,\n  CAST(SUM(s.revenue_thb_est) AS INT64) AS revenue,\n  CAST(SUM(s.serving_cost_thb) AS INT64) AS cost,\n  CAST(SUM(s.new_paid_subscriptions) AS INT64) AS paid_subs,\n  CAST(SUM(s.tokens_used)/1e6 AS INT64) AS tokens_m\nFROM `trueaihub-mongo-pipeline-2026.Total.summary_daily` s GROUP BY 1 ORDER BY 1",
    "package": "SELECT version, packageName,\n  CAST(SUM(revenue_thb_est) AS INT64) AS revenue,\n  CAST(SUM(new_paid_subscriptions) AS INT64) AS subs,\n  CAST(SUM(serving_cost_thb) AS INT64) AS cost\nFROM `trueaihub-mongo-pipeline-2026.Total.summary_daily`\nWHERE NOT is_trial\nGROUP BY 1,2 HAVING subs > 0 ORDER BY revenue DESC",
    "model": "SELECT aiModel, CAST(SUM(serving_cost_thb) AS INT64) AS cost,\n  CAST(SUM(tokens_used)/1e6 AS INT64) AS tokens_m\nFROM `trueaihub-mongo-pipeline-2026.Total.summary_model_daily`\nGROUP BY 1 ORDER BY cost DESC LIMIT 8",
    "health": "SELECT user_type, COUNT(*) AS users,\n  CAST(SUM(revenue_thb) AS INT64) AS revenue,\n  CAST(AVG(revenue_thb) AS INT64) AS avg_rev\nFROM `trueaihub-mongo-pipeline-2026.B2C.user_repeat_behavior` GROUP BY 1 ORDER BY users DESC",
    "repeat": "SELECT\n  COUNTIF(paid_subscribe_cnt>=1) AS paid_users,\n  COUNTIF(is_repeat) AS repeat_users,\n  ROUND(100*SAFE_DIVIDE(COUNTIF(is_repeat),COUNTIF(paid_subscribe_cnt>=1)),2) AS repeat_pct,\n  COUNTIF(is_mature_cohort AND paid_subscribe_cnt>=1) AS mature_paid,\n  COUNTIF(is_mature_cohort AND is_repeat) AS mature_repeat,\n  ROUND(100*SAFE_DIVIDE(COUNTIF(is_mature_cohort AND is_repeat),COUNTIF(is_mature_cohort AND paid_subscribe_cnt>=1)),2) AS mature_repeat_pct,\n  COUNTIF(churned AND paid_subscribe_cnt>=1) AS churned,\n  COUNTIF(trial_cnt>0) AS trial_users,\n  ROUND(100*SAFE_DIVIDE(COUNTIF(trial_cnt>0 AND paid_subscribe_cnt>=1),COUNTIF(trial_cnt>0)),2) AS trial_conv_pct\nFROM `trueaihub-mongo-pipeline-2026.B2C.user_repeat_behavior`"
}


_SDK = os.path.expandvars(r"%LOCALAPPDATA%\Google\Cloud SDK\google-cloud-sdk")


def _bq_env():
    """หา bq CLI + ตั้ง env ให้ครบ (bq ต้องมี python ของตัวเอง + เรียก gcloud หา auth ได้)"""
    bq = shutil.which("bq") or shutil.which("bq.cmd")
    env = dict(os.environ)
    if not bq and os.path.isdir(_SDK):
        bq = os.path.join(_SDK, "bin", "bq.cmd")
        env["PATH"] = os.path.join(_SDK, "bin") + os.pathsep + env.get("PATH", "")
        env.setdefault("CLOUDSDK_PYTHON",
                       os.path.join(_SDK, "platform", "bundledpython", "python.exe"))
    if not bq or not os.path.exists(bq):
        sys.exit("หา bq CLI ไม่เจอ — ติดตั้ง Google Cloud SDK แล้ว gcloud auth login ก่อน")
    return bq, env


def load(name):
    """รัน query ผ่าน bq CLI แล้วคืน list[dict] (ค่าทุกตัวเป็น str)"""
    bq, env = _bq_env()
    p = subprocess.run(
        [bq, "query", "--use_legacy_sql=false", "--format=json", "--max_rows=1000"],
        input=QUERIES[name], capture_output=True, text=True, encoding="utf-8", env=env)
    if p.returncode != 0 or not p.stdout.strip():
        sys.exit(f"query '{name}' ล้มเหลว:\n{(p.stderr or p.stdout)[:800]}")
    return json.loads(p.stdout)


def num(x):
    """แปลงค่าจาก bq (มาเป็น string เสมอ) เป็น float; None/ว่าง -> 0"""
    return float(x) if x not in (None, "") else 0.0


kpi = load("kpi")[0]
weekly = load("weekly")
wusers = load("weekly_users")
version = load("version")
package = load("package")
model = load("model")
health = load("health")
rep = load("repeat")[0]

# ── KPI + deltas ────────────────────────────────────────────────────
cur_rev, prev_rev = num(kpi["cur_rev"]), num(kpi["prev_rev"])
cur_cost, prev_cost = num(kpi["cur_cost"]), num(kpi["prev_cost"])
cur_profit, prev_profit = cur_rev - cur_cost, prev_rev - prev_cost
cur_margin = cur_profit / cur_rev * 100 if cur_rev else 0
prev_margin = prev_profit / prev_rev * 100 if prev_rev else 0
cur_users, prev_users = num(kpi["cur_users"]), num(kpi["prev_users"])
cur_subs, prev_subs = num(kpi["cur_subs"]), num(kpi["prev_subs"])
cur_trials, prev_trials = num(kpi["cur_trials"]), num(kpi["prev_trials"])
data_end, first_date = kpi["data_end"], kpi["first_date"]
all_rev, all_cost = num(kpi["all_rev"]), num(kpi["all_cost"])


def pct(cur, prev):
    return (cur - prev) / prev * 100 if prev else 0.0


def fmt(v, unit=""):
    return f"{v:,.0f}{unit}"


# tone: good / warn / bad / flat  (ไม่ใช้สีอย่างเดียว — มีไอคอน+ข้อความกำกับ)
def tile(label, value, sub, dp, tone, note=""):
    return dict(label=label, value=value, sub=sub, dp=dp, tone=tone, note=note)


TILES = [
    tile("รายได้ (ประมาณการ)", "฿" + fmt(cur_rev), "30 วันล่าสุด", pct(cur_rev, prev_rev), "good"),
    tile("ต้นทุน AI", "฿" + fmt(cur_cost), "30 วันล่าสุด", pct(cur_cost, prev_cost), "warn",
         "โตเร็วกว่ารายได้"),
    tile("กำไรขั้นต้น", "฿" + fmt(cur_profit), f"อัตรากำไร {cur_margin:.0f}%", pct(cur_profit, prev_profit),
         "bad", f"เดือนก่อน {prev_margin:.0f}%"),
    tile("ผู้ใช้ที่ใช้งานจริง", fmt(cur_users) + " คน", "30 วันล่าสุด", pct(cur_users, prev_users), "flat"),
    tile("สมัครใหม่ (จ่ายเงิน)", fmt(cur_subs) + " ราย", "30 วันล่าสุด", pct(cur_subs, prev_subs), "good"),
    tile("เริ่มทดลองใช้ฟรี", fmt(cur_trials) + " ราย", "30 วันล่าสุด", pct(cur_trials, prev_trials), "warn"),
]

# ── data สำหรับกราฟ ─────────────────────────────────────────────────
wk_labels = [w["wk"] for w in weekly]
rev_series = [num(w["revenue"]) for w in weekly]
cost_series = [num(w["cost"]) for w in weekly]

uw_labels = [w["wk"] for w in wusers]
b2c_users = [num(w["b2c_users"]) for w in wusers]
b2b_users = [num(w["b2b_users"]) for w in wusers]

pkg_rows = [dict(name=p["packageName"], version=p["version"], revenue=num(p["revenue"]),
                 subs=num(p["subs"]), cost=num(p["cost"])) for p in package]
mdl_rows = [dict(name=m["aiModel"], cost=num(m["cost"]), tokens=num(m["tokens_m"])) for m in model]
ver_rows = [dict(v=v["version"], revenue=num(v["revenue"]), cost=num(v["cost"]),
                 subs=num(v["paid_subs"]), tokens=num(v["tokens_m"])) for v in version]
health_rows = [dict(t=h["user_type"], users=num(h["users"]), rev=num(h["revenue"]),
                    avg=num(h["avg_rev"])) for h in health]

PAYLOAD = dict(
    weeks=wk_labels, revenue=rev_series, cost=cost_series,
    uweeks=uw_labels, b2c=b2c_users, b2b=b2b_users,
    packages=pkg_rows, models=mdl_rows, versions=ver_rows, health=health_rows,
)

TILE_HTML = ""
for t in TILES:
    d = t["dp"]
    arrow = "▲" if d > 0.05 else ("▼" if d < -0.05 else "—")
    word = "เพิ่มขึ้น" if d > 0.05 else ("ลดลง" if d < -0.05 else "ทรงตัว")
    TILE_HTML += f"""
      <div class="tile">
        <div class="tile-label">{t['label']}</div>
        <div class="tile-value">{t['value']}</div>
        <div class="tile-foot">
          <span class="delta delta-{t['tone']}"><span aria-hidden="true">{arrow}</span> {abs(d):.1f}% <span class="sr-word">{word}</span></span>
          <span class="tile-sub">{t['sub']}</span>
        </div>
        {f'<div class="tile-note">{t["note"]}</div>' if t["note"] else ''}
      </div>"""

pkg_table = "".join(
    f"<tr><td>{p['name']}</td><td>{p['version']}</td><td class='n'>{p['subs']:,.0f}</td>"
    f"<td class='n'>฿{p['revenue']:,.0f}</td><td class='n'>฿{p['cost']:,.0f}</td></tr>"
    for p in pkg_rows)
wk_table = "".join(
    f"<tr><td>{w}</td><td class='n'>฿{r:,.0f}</td><td class='n'>฿{c:,.0f}</td>"
    f"<td class='n'>฿{r - c:,.0f}</td></tr>"
    for w, r, c in zip(wk_labels, rev_series, cost_series))

HTML = f"""<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ภาพรวมธุรกิจ TrueAIHub</title>
<style>
  .viz-root {{
    color-scheme: light;
    --surface-1:#fcfcfb; --page:#f9f9f7;
    --text-primary:#0b0b0b; --text-secondary:#52514e; --muted:#898781;
    --grid:#e1e0d9; --baseline:#c3c2b7; --border:rgba(11,11,11,0.10);
    --s1:#2a78d6; --s2:#eb6834; --s3:#1baf7a;
    --good-text:#006300; --status-good:#0ca30c; --status-warn:#fab219;
    --status-serious:#ec835a; --status-critical:#d03b3b;
    --tint:#eef3fc;
  }}
  @media (prefers-color-scheme: dark) {{
    :root:where(:not([data-theme="light"])) .viz-root {{
      color-scheme: dark;
      --surface-1:#1a1a19; --page:#0d0d0d;
      --text-primary:#ffffff; --text-secondary:#c3c2b7; --muted:#898781;
      --grid:#2c2c2a; --baseline:#383835; --border:rgba(255,255,255,0.10);
      --s1:#3987e5; --s2:#d95926; --s3:#199e70;
      --good-text:#0ca30c; --tint:#20242c;
    }}
  }}
  :root[data-theme="dark"] .viz-root {{
    color-scheme: dark;
    --surface-1:#1a1a19; --page:#0d0d0d;
    --text-primary:#ffffff; --text-secondary:#c3c2b7; --muted:#898781;
    --grid:#2c2c2a; --baseline:#383835; --border:rgba(255,255,255,0.10);
    --s1:#3987e5; --s2:#d95926; --s3:#199e70;
    --good-text:#0ca30c; --tint:#20242c;
  }}
  *{{box-sizing:border-box}}
  body{{margin:0;background:var(--page);color:var(--text-primary);
       font-family:system-ui,-apple-system,"Segoe UI","Leelawadee UI",sans-serif;line-height:1.5}}
  .viz-root{{background:var(--page);padding:28px 20px 48px}}
  .wrap{{max-width:1120px;margin:0 auto}}
  h1{{font-size:24px;margin:0 0 4px;letter-spacing:-0.01em}}
  .meta{{color:var(--text-secondary);font-size:13px;margin-bottom:22px}}
  h2{{font-size:16px;margin:0 0 2px}}
  .sub{{font-size:12.5px;color:var(--muted);margin-bottom:14px}}
  .card{{background:var(--surface-1);border:1px solid var(--border);border-radius:12px;padding:18px 20px;margin-bottom:16px}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:12px;margin-bottom:16px}}
  .tile{{background:var(--surface-1);border:1px solid var(--border);border-radius:12px;padding:14px 16px}}
  .tile-label{{font-size:12.5px;color:var(--text-secondary);margin-bottom:6px}}
  .tile-value{{font-size:27px;font-weight:650;letter-spacing:-0.02em;line-height:1.15}}
  .tile-foot{{display:flex;align-items:center;gap:8px;margin-top:6px;flex-wrap:wrap}}
  .tile-sub,.tile-note{{font-size:11.5px;color:var(--muted)}}
  .tile-note{{margin-top:4px}}
  .delta{{font-size:12px;font-weight:600;display:inline-flex;align-items:center;gap:3px}}
  .delta-good{{color:var(--good-text)}} .delta-bad{{color:var(--status-critical)}}
  .delta-warn{{color:var(--status-serious)}} .delta-flat{{color:var(--text-secondary)}}
  .sr-word{{font-weight:400;font-size:11px;color:var(--text-secondary)}}
  .legend{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:8px;font-size:12.5px;color:var(--text-secondary)}}
  .key{{display:inline-flex;align-items:center;gap:6px}}
  .swatch{{width:11px;height:11px;border-radius:3px;display:inline-block}}
  .two{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
  @media (max-width:820px){{.two{{grid-template-columns:1fr}}}}
  svg{{width:100%;height:auto;display:block;overflow:visible}}
  .tt{{position:fixed;pointer-events:none;background:var(--surface-1);border:1px solid var(--border);
      border-radius:8px;padding:8px 11px;font-size:12.5px;box-shadow:0 4px 14px rgba(0,0,0,.14);
      opacity:0;transition:opacity .1s;z-index:50;white-space:nowrap}}
  .tt b{{display:block;margin-bottom:4px;font-size:12px;color:var(--text-secondary);font-weight:500}}
  .tt-row{{display:flex;align-items:center;gap:7px}}
  .tt-val{{font-variant-numeric:tabular-nums;font-weight:600}}
  .insight{{background:var(--tint);border:1px solid var(--border);border-radius:12px;padding:14px 18px;margin-bottom:16px;font-size:13.5px}}
  .insight b{{display:block;margin-bottom:3px;font-size:14px}}
  table{{width:100%;border-collapse:collapse;font-size:13px}}
  th{{text-align:left;font-weight:600;color:var(--text-secondary);padding:7px 10px;border-bottom:1px solid var(--baseline);font-size:12px}}
  td{{padding:7px 10px;border-bottom:1px solid var(--grid)}}
  td.n,th.n{{text-align:right;font-variant-numeric:tabular-nums}}
  details{{margin-top:6px}}
  summary{{cursor:pointer;font-size:13px;color:var(--text-secondary);padding:6px 0}}
  .foot{{font-size:12px;color:var(--muted);margin-top:22px;line-height:1.7}}
</style>
</head>
<body>
<div class="viz-root">
<div class="wrap">

  <h1>ภาพรวมธุรกิจ TrueAIHub</h1>
  <div class="meta">ข้อมูล {first_date} – {data_end} · เปรียบเทียบ 30 วันล่าสุดกับ 30 วันก่อนหน้า · อัปเดตอัตโนมัติทุกวัน 06:00 น.</div>

  <div class="grid">{TILE_HTML}
  </div>

  <div class="insight">
    <b>สิ่งที่ต้องจับตา — กำไรกำลังถูกบีบ</b>
    ต้นทุน AI โตเร็วกว่ารายได้เกือบ 3 เท่า (+{pct(cur_cost, prev_cost):.0f}% เทียบกับ +{pct(cur_rev, prev_rev):.0f}%)
    ทำให้อัตรากำไรลดจาก {prev_margin:.0f}% เหลือ {cur_margin:.0f}% ภายในเดือนเดียว
    ขณะที่จำนวนผู้ใช้แทบไม่เปลี่ยน — แปลว่าผู้ใช้เดิมใช้งานหนักขึ้น (หรือย้ายไปโมเดลที่แพงขึ้น) โดยรายได้ไม่ได้ตามไปด้วย
  </div>

  <div class="card">
    <h2>รายได้เทียบต้นทุน AI รายสัปดาห์</h2>
    <div class="sub">หน่วยบาท · เส้นทั้งสองใช้แกนเดียวกัน จึงเทียบระยะห่างได้ตรง ๆ (ระยะห่าง = กำไรขั้นต้น)</div>
    <div class="legend">
      <span class="key"><span class="swatch" style="background:var(--s1)"></span> รายได้</span>
      <span class="key"><span class="swatch" style="background:var(--s2)"></span> ต้นทุน AI</span>
    </div>
    <svg id="c1" viewBox="0 0 760 280" role="img" aria-label="กราฟเส้นรายได้และต้นทุน AI รายสัปดาห์"></svg>
  </div>

  <div class="card">
    <h2>ผู้ใช้ที่ใช้งานจริงรายสัปดาห์</h2>
    <div class="sub">นับผู้ใช้ไม่ซ้ำที่มีการใช้ token ในสัปดาห์นั้น</div>
    <div class="legend">
      <span class="key"><span class="swatch" style="background:var(--s1)"></span> B2C</span>
      <span class="key"><span class="swatch" style="background:var(--s2)"></span> B2B</span>
    </div>
    <svg id="c2" viewBox="0 0 760 240" role="img" aria-label="กราฟเส้นผู้ใช้ที่ใช้งานจริงรายสัปดาห์ แยก B2C และ B2B"></svg>
  </div>

  <div class="two">
    <div class="card">
      <h2>รายได้แยกตามแพ็คเกจ</h2>
      <div class="sub">ตลอดช่วงข้อมูล · ประมาณการจากราคาตั้ง</div>
      <div class="legend">
        <span class="key"><span class="swatch" style="background:var(--s1)"></span> B2C</span>
        <span class="key"><span class="swatch" style="background:var(--s3)"></span> B2B</span>
      </div>
      <svg id="c3" viewBox="0 0 520 244" role="img" aria-label="กราฟแท่งรายได้แยกตามแพ็คเกจ"></svg>
    </div>
    <div class="card">
      <h2>ต้นทุนแยกตามโมเดล AI</h2>
      <div class="sub">8 อันดับแรก · ตลอดช่วงข้อมูล</div>
      <svg id="c4" viewBox="0 0 520 260" role="img" aria-label="กราฟแท่งต้นทุนแยกตามโมเดล AI"></svg>
    </div>
  </div>

  <div class="card">
    <h2>สุขภาพฐานลูกค้า (B2C)</h2>
    <div class="sub">ลูกค้าจ่ายเงินสะสม {int(num(rep['paid_users'])):,} ราย · ซื้อซ้ำ {int(num(rep['repeat_users']))} ราย</div>
    <div class="two" style="gap:22px;align-items:start">
      <svg id="c5" viewBox="0 0 520 190" role="img" aria-label="กราฟแท่งสัดส่วนประเภทผู้ใช้"></svg>
      <div>
        <table>
          <tr><th>ตัวชี้วัด</th><th class="n">ค่า</th></tr>
          <tr><td>อัตราซื้อซ้ำ (ทั้งหมด)</td><td class="n">{num(rep['repeat_pct']):.2f}%</td></tr>
          <tr><td>อัตราซื้อซ้ำ (เฉพาะลูกค้าที่ครบ 35 วัน)</td><td class="n">{num(rep['mature_repeat_pct']):.2f}%</td></tr>
          <tr><td>ทดลองฟรี → จ่ายเงิน</td><td class="n">{num(rep['trial_conv_pct']):.2f}%</td></tr>
          <tr><td>ลูกค้าที่หมดอายุแล้วไม่ต่อ</td><td class="n">{int(num(rep['churned'])):,} ราย</td></tr>
          <tr><td>รายได้เฉลี่ย — ลูกค้าซื้อซ้ำ</td><td class="n">฿{int(num([h for h in health if h['user_type'] == 'repeat'][0]['avg_rev'])):,}</td></tr>
          <tr><td>รายได้เฉลี่ย — ลูกค้าซื้อครั้งเดียว</td><td class="n">฿{int(num([h for h in health if h['user_type'] == 'paid_once'][0]['avg_rev'])):,}</td></tr>
        </table>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>ตารางข้อมูล</h2>
    <div class="sub">ตัวเลขเดียวกับกราฟด้านบน สำหรับอ่าน/คัดลอก</div>
    <details><summary>▸ รายได้และต้นทุนรายสัปดาห์ ({len(wk_labels)} สัปดาห์)</summary>
      <table><tr><th>สัปดาห์เริ่ม</th><th class="n">รายได้</th><th class="n">ต้นทุน AI</th><th class="n">กำไรขั้นต้น</th></tr>{wk_table}</table>
    </details>
    <details><summary>▸ แพ็คเกจ</summary>
      <table><tr><th>แพ็คเกจ</th><th>กลุ่ม</th><th class="n">สมัคร</th><th class="n">รายได้</th><th class="n">ต้นทุน AI</th></tr>{pkg_table}</table>
    </details>
  </div>

  <div class="foot">
    <b>ที่มาและข้อจำกัด</b><br>
    • ข้อมูลจาก <code>Total.summary_daily</code>, <code>Total.summary_model_daily</code>,
      <code>Total.user_tracking_total</code> และ <code>B2C.user_repeat_behavior</code> (BigQuery) — คำนวณใหม่อัตโนมัติทุกวัน 06:00 น.<br>
    • รายได้เป็น<b>ประมาณการจากราคาตั้ง</b>ของแพ็คเกจ ณ วันที่สมัคร ไม่รวมส่วนลด/โปรโมชัน — ฝั่ง B2B ราคาจริงอาจต่างจากราคาตั้ง<br>
    • ต้นทุน AI คือค่าใช้จ่ายจริงจากการเรียกโมเดล (แปลงเป็นบาทที่อัตรา 32.67)<br>
    • ผู้ใช้ที่ถูกระงับ (banned) ถูกตัดออกทั้งหมด · ตัวเลขชุดนี้รวมผู้ใช้ B2B ที่ยังไม่ผูกบริษัท จึงสูงกว่าหน้า B2B เดิมเล็กน้อย (~3%)<br>
    • ยอด "ซื้อซ้ำ" นับเฉพาะแพ็คเกจเสียเงินตั้งแต่ 2 subscription ขึ้นไป (การเปลี่ยนจากทดลองฟรีเป็นจ่ายเงินครั้งแรกนับเป็น conversion)
  </div>

</div>
</div>
<div class="tt" id="tt"></div>

<script>
const DATA = {json.dumps(PAYLOAD, ensure_ascii=False)};
const tt = document.getElementById('tt');
const NS = 'http://www.w3.org/2000/svg';
const cs = getComputedStyle(document.querySelector('.viz-root'));
const C = k => cs.getPropertyValue(k).trim();
const el = (n, a) => {{ const e = document.createElementNS(NS, n);
  for (const k in a) e.setAttribute(k, a[k]); return e; }};
const thb = v => '฿' + Math.round(v).toLocaleString('en-US');
const int = v => Math.round(v).toLocaleString('en-US');
const shortDate = s => {{ const d = new Date(s + 'T00:00:00');
  return d.getDate() + '/' + (d.getMonth() + 1); }};

function showTT(evt, html) {{
  tt.innerHTML = html; tt.style.opacity = 1;
  const r = tt.getBoundingClientRect();
  let x = evt.clientX + 14, y = evt.clientY - r.height - 10;
  if (x + r.width > innerWidth - 8) x = evt.clientX - r.width - 14;
  if (y < 8) y = evt.clientY + 16;
  tt.style.left = x + 'px'; tt.style.top = y + 'px';
}}
const hideTT = () => tt.style.opacity = 0;

/* ── line chart (แกนเดียว, หลายเส้น) ───────────────────────────── */
function lineChart(id, labels, series, fmt) {{
  const svg = document.getElementById(id); svg.innerHTML = '';
  const vb = svg.viewBox.baseVal, W = vb.width, H = vb.height;
  const m = {{ t: 14, r: 54, b: 30, l: 58 }};
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  // ปัดแกน y ขึ้นเป็นเลขกลม เพื่อให้ tick อ่านง่าย (0 / 8k / 16k / 24k / 32k)
  const raw = Math.max(...series.flatMap(s => s.values)) * 1.08 || 1;
  const mag = Math.pow(10, Math.floor(Math.log10(raw / 4)));
  const stepNice = [1, 2, 2.5, 5, 10].find(m => m * mag >= raw / 4) * mag;
  const max = stepNice * 4;
  const X = i => m.l + (labels.length < 2 ? iw / 2 : i * iw / (labels.length - 1));
  const Y = v => m.t + ih - (v / max) * ih;

  for (let k = 0; k <= 4; k++) {{
    const v = max * k / 4, y = Y(v);
    svg.appendChild(el('line', {{ x1: m.l, x2: m.l + iw, y1: y, y2: y,
      stroke: C('--grid'), 'stroke-width': 1 }}));
    const t = el('text', {{ x: m.l - 9, y: y + 4, 'text-anchor': 'end',
      fill: C('--muted'), 'font-size': 11 }});
    t.textContent = fmt.axis(v); svg.appendChild(t);
  }}
  svg.appendChild(el('line', {{ x1: m.l, x2: m.l + iw, y1: m.t + ih, y2: m.t + ih,
    stroke: C('--baseline'), 'stroke-width': 1 }}));

  const step = Math.ceil(labels.length / 8);
  labels.forEach((lb, i) => {{ if (i % step && i !== labels.length - 1) return;
    const t = el('text', {{ x: X(i), y: m.t + ih + 18, 'text-anchor': 'middle',
      fill: C('--muted'), 'font-size': 11 }});
    t.textContent = shortDate(lb); svg.appendChild(t); }});

  series.forEach(s => {{
    const d = s.values.map((v, i) => (i ? 'L' : 'M') + X(i) + ' ' + Y(v)).join(' ');
    svg.appendChild(el('path', {{ d, fill: 'none', stroke: C(s.color),
      'stroke-width': 2, 'stroke-linejoin': 'round', 'stroke-linecap': 'round' }}));
  }});

  // direct label ปลายเส้น + กันชนกันเมื่อค่าสุดท้ายใกล้กัน
  const li = labels.length - 1;
  const ends = series.map((s, k) => ({{ k, name: s.name, color: s.color,
    y: Y(s.values[li]) }})).sort((a, b) => a.y - b.y);
  const MIN_GAP = 15;
  for (let i = 1; i < ends.length; i++)
    if (ends[i].y - ends[i - 1].y < MIN_GAP) ends[i].y = ends[i - 1].y + MIN_GAP;
  ends.forEach(e => {{
    const dot = el('circle', {{ cx: X(li), cy: Y(series[e.k].values[li]), r: 3.5,
      fill: C(e.color) }});
    svg.appendChild(dot);
    const lab = el('text', {{ x: X(li) + 9, y: e.y + 4,
      fill: C('--text-secondary'), 'font-size': 11.5, 'font-weight': 600 }});
    lab.textContent = e.name; svg.appendChild(lab);
  }});

  const cross = el('line', {{ y1: m.t, y2: m.t + ih, stroke: C('--baseline'),
    'stroke-width': 1, opacity: 0 }});
  svg.appendChild(cross);
  const dots = series.map(s => {{
    const c = el('circle', {{ r: 4.5, fill: C(s.color), stroke: C('--surface-1'),
      'stroke-width': 2, opacity: 0 }});
    svg.appendChild(c); return c; }});

  const hit = el('rect', {{ x: m.l, y: m.t, width: iw, height: ih,
    fill: 'transparent', style: 'cursor:crosshair' }});
  svg.appendChild(hit);
  hit.addEventListener('mousemove', e => {{
    const r = svg.getBoundingClientRect();
    const px = (e.clientX - r.left) * W / r.width;
    let i = Math.round((px - m.l) / (iw / Math.max(labels.length - 1, 1)));
    i = Math.max(0, Math.min(labels.length - 1, i));
    cross.setAttribute('x1', X(i)); cross.setAttribute('x2', X(i));
    cross.setAttribute('opacity', 1);
    let html = '<b>สัปดาห์ ' + shortDate(labels[i]) + '</b>';
    series.forEach((s, k) => {{
      dots[k].setAttribute('cx', X(i)); dots[k].setAttribute('cy', Y(s.values[i]));
      dots[k].setAttribute('opacity', 1);
      html += '<div class="tt-row"><span class="swatch" style="background:' + C(s.color) +
        '"></span>' + s.name + ' <span class="tt-val">' + fmt.tip(s.values[i]) + '</span></div>';
    }});
    if (series.length === 2) {{
      const diff = series[0].values[i] - series[1].values[i];
      html += '<div class="tt-row" style="margin-top:4px;color:' + C('--text-secondary') +
        '">ส่วนต่าง <span class="tt-val">' + fmt.tip(diff) + '</span></div>';
    }}
    showTT(e, html);
  }});
  hit.addEventListener('mouseleave', () => {{
    hideTT(); cross.setAttribute('opacity', 0);
    dots.forEach(d => d.setAttribute('opacity', 0));
  }});
}}

/* ── horizontal bars (ปลายข้อมูลมน 4px, ฐานชิดแกน) ─────────────── */
function barChart(id, rows, fmt, opt) {{
  opt = opt || {{}};
  const svg = document.getElementById(id); svg.innerHTML = '';
  const vb = svg.viewBox.baseVal, W = vb.width, H = vb.height;
  const m = {{ t: 6, r: 74, b: 6, l: opt.labelW || 118 }};
  const iw = W - m.l - m.r;
  const n = rows.length, gap = 2;
  const bh = Math.min(opt.maxBar || 26, (H - m.t - m.b - gap * (n - 1)) / n);
  const max = Math.max(...rows.map(r => r.value)) || 1;
  const R = 4;

  rows.forEach((row, i) => {{
    const y = m.t + i * ((H - m.t - m.b - gap * (n - 1)) / n + gap);
    const w = Math.max((row.value / max) * iw, 3);
    const rr = Math.min(R, w);
    const d = `M${{m.l}} ${{y}} H${{m.l + w - rr}} A${{rr}} ${{rr}} 0 0 1 ${{m.l + w}} ${{y + rr}}` +
              ` V${{y + bh - rr}} A${{rr}} ${{rr}} 0 0 1 ${{m.l + w - rr}} ${{y + bh}} H${{m.l}} Z`;
    const p = el('path', {{ d, fill: C(row.color || '--s1'), style: 'cursor:pointer' }});
    svg.appendChild(p);

    const lb = el('text', {{ x: m.l - 10, y: y + bh / 2 + 4, 'text-anchor': 'end',
      fill: C('--text-primary'), 'font-size': 12 }});
    lb.textContent = row.label.length > 22 ? row.label.slice(0, 21) + '…' : row.label;
    svg.appendChild(lb);

    const vl = el('text', {{ x: m.l + w + 8, y: y + bh / 2 + 4,
      fill: C('--text-secondary'), 'font-size': 11.5, 'font-weight': 600 }});
    vl.textContent = fmt.label(row.value); svg.appendChild(vl);

    p.addEventListener('mousemove', e => showTT(e,
      '<b>' + row.label + '</b>' + row.tip));
    p.addEventListener('mouseleave', hideTT);
  }});
}}

/* ── render ─────────────────────────────────────────────────────── */
lineChart('c1', DATA.weeks, [
  {{ name: 'รายได้', values: DATA.revenue, color: '--s1' }},
  {{ name: 'ต้นทุน', values: DATA.cost, color: '--s2' }}
], {{ axis: v => '฿' + (v >= 1000 ? Math.round(v / 1000) + 'k' : Math.round(v)),
     tip: v => thb(v) }});

lineChart('c2', DATA.uweeks, [
  {{ name: 'B2C', values: DATA.b2c, color: '--s1' }},
  {{ name: 'B2B', values: DATA.b2b, color: '--s2' }}
], {{ axis: v => int(v), tip: v => int(v) + ' คน' }});

barChart('c3', DATA.packages.map(p => ({{
  label: p.name, value: p.revenue, color: p.version === 'B2C' ? '--s1' : '--s3',
  tip: '<div class="tt-row">รายได้ <span class="tt-val">' + thb(p.revenue) + '</span></div>' +
       '<div class="tt-row">สมัคร <span class="tt-val">' + int(p.subs) + ' ราย</span></div>' +
       '<div class="tt-row">ต้นทุน AI <span class="tt-val">' + thb(p.cost) + '</span></div>'
}})), {{ label: v => thb(v) }}, {{ labelW: 106 }});

barChart('c4', DATA.models.map(m => ({{
  label: m.name, value: m.cost,
  tip: '<div class="tt-row">ต้นทุน <span class="tt-val">' + thb(m.cost) + '</span></div>' +
       '<div class="tt-row">โทเคน <span class="tt-val">' + int(m.tokens) + ' ล้าน</span></div>'
}})), {{ label: v => thb(v) }}, {{ labelW: 156 }});

const HL = {{ repeat: 'ซื้อซ้ำ', paid_once: 'ซื้อครั้งเดียว', trial_only: 'ทดลองฟรีอย่างเดียว' }};
barChart('c5', DATA.health.map(h => ({{
  label: HL[h.t] || h.t, value: h.users,
  color: h.t === 'repeat' ? '--s1' : (h.t === 'paid_once' ? '--s3' : '--s2'),
  tip: '<div class="tt-row">จำนวน <span class="tt-val">' + int(h.users) + ' คน</span></div>' +
       '<div class="tt-row">รายได้รวม <span class="tt-val">' + thb(h.rev) + '</span></div>' +
       (h.avg > 0 ? '<div class="tt-row">เฉลี่ย/คน <span class="tt-val">' + thb(h.avg) + '</span></div>' : '')
}})), {{ label: v => int(v) + ' คน' }}, {{ labelW: 132, maxBar: 34 }});

addEventListener('resize', () => hideTT());
</script>
</body>
</html>
"""

os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    f.write(HTML)
print("saved:", OUT)
print(f"weeks={len(wk_labels)} pkgs={len(pkg_rows)} models={len(mdl_rows)}")
print(f"margin {prev_margin:.1f}% -> {cur_margin:.1f}%")
