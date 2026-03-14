"""
配布用・1ファイル統合HTML生成スクリプト
使い方:
  python run_export_daily.py 2026-02-25
  python run_export_daily.py          ← 今日の日付で自動

output/<DATE>_share.html を生成。
Flask サーバー不要・CSS/JS完全内包のスタンドアロンHTML。
LINEやメールでそのまま配布可能。
"""
import sys, io, os, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

from datetime import date as _date

# ── 引数処理 ─────────────────────────────────────────────────────────
if len(sys.argv) >= 2:
    DATE = sys.argv[1].strip()
else:
    DATE = _date.today().strftime("%Y-%m-%d")

DATE_KEY = DATE.replace("-", "")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
OUT_PATH = os.path.join(OUTPUT_DIR, f"{DATE_KEY}_share.html")

print(f"配布用HTML生成: {DATE}")

# ── 対象ファイル収集 ──────────────────────────────────────────────────
files = sorted([
    f for f in os.listdir(OUTPUT_DIR)
    if f.startswith(DATE_KEY + "_")
    and f.endswith(".html")
    and "全レース" not in f
    and "share" not in f
])

if not files:
    print(f"[ERROR] {DATE} の個別レースHTMLが見つかりません（先に run_analysis_date.py を実行してください）")
    sys.exit(1)

print(f"  対象ファイル: {len(files)}件")

# ── HTMLパース関数 ────────────────────────────────────────────────────
def extract_css(html: str) -> str:
    m = re.search(r"<style>(.*?)</style>", html, re.DOTALL)
    return m.group(1) if m else ""

def extract_body(html: str) -> str:
    m = re.search(r"<body>(.*?)</body>", html, re.DOTALL)
    return m.group(1).strip() if m else html

def extract_title(html: str) -> str:
    m = re.search(r"<title>(.*?)</title>", html)
    return m.group(1) if m else ""

def parse_filename(fname: str):
    """例: 20260225_浦和1R.html → (venue="浦和", race_no=1)"""
    name = fname.replace(DATE_KEY + "_", "").replace(".html", "")
    m = re.match(r"^(.+?)(\d+)R$", name)
    if m:
        return m.group(1), int(m.group(2))
    return name, 0

# ── 各ファイル読み込み ────────────────────────────────────────────────
css_base = ""
races = []  # {venue, race_no, body, title}

for fname in files:
    path = os.path.join(OUTPUT_DIR, fname)
    try:
        with open(path, encoding="utf-8") as f:
            html = f.read()
    except Exception as e:
        print(f"  [SKIP] {fname}: {e}")
        continue

    if not css_base:
        css_base = extract_css(html)

    venue, race_no = parse_filename(fname)
    title = extract_title(html)
    body  = extract_body(html)
    races.append({"venue": venue, "race_no": race_no, "title": title, "body": body})
    print(f"  読み込み: {venue} {race_no}R")

if not races:
    print("[ERROR] 有効なレースデータがありません")
    sys.exit(1)

# ── 競馬場でグループ化 ────────────────────────────────────────────────
venue_order = []
venue_races = {}
for r in sorted(races, key=lambda x: (x["venue"], x["race_no"])):
    v = r["venue"]
    if v not in venue_races:
        venue_order.append(v)
        venue_races[v] = []
    venue_races[v].append(r)

total = sum(len(v) for v in venue_races.values())

# ── タブHTML生成 ──────────────────────────────────────────────────────
venue_tabs_html = ""
venue_panels_html = ""

for vi, venue in enumerate(venue_order):
    active_v = "v-active" if vi == 0 else ""
    venue_tabs_html += f'<button class="vtab {active_v}" onclick="showVenue({vi})" id="vtab-{vi}">{venue}</button>\n'

    race_tabs_html   = ""
    race_panels_html = ""
    rlist = venue_races[venue]

    for ri, r in enumerate(rlist):
        active_r = "r-active" if ri == 0 else ""
        race_tabs_html += (
            f'<button class="rtab {active_r}" '
            f'onclick="showRace({vi},{ri})" id="rtab-{vi}-{ri}">'
            f'{r["race_no"]}R</button>\n'
        )
        race_panels_html += (
            f'<div class="rpanel {active_r}" id="rpanel-{vi}-{ri}">'
            f'{r["body"]}'
            f'</div>\n'
        )

    venue_panels_html += f"""
<div class="vpanel {active_v}" id="vpanel-{vi}">
  <div class="race-tabs" id="rtabs-{vi}">{race_tabs_html}</div>
  <div class="race-panels">{race_panels_html}</div>
</div>
"""

# ── 最終HTML組み立て ──────────────────────────────────────────────────
combined = f"""<!DOCTYPE html>
<html lang="ja"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>D-AI競馬予想　{DATE}</title>
<style>
{css_base}

/* ── 配布用ラッパースタイル ── */
:root{{--share-navy:#0d2b5e;--share-gold:#c9952a;--share-bg:#f2f4f8}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--share-bg);font-family:"Hiragino Sans","Yu Gothic UI",sans-serif}}

.share-header{{
  background:linear-gradient(135deg,#0d2b5e 60%,#1a4a8a);
  color:#fff;padding:12px 20px;
  display:flex;align-items:center;gap:12px;flex-wrap:wrap
}}
.share-header h1{{font-size:18px;font-weight:700}}
.share-date{{
  background:var(--share-gold);color:#fff;
  font-size:12px;font-weight:700;
  padding:3px 10px;border-radius:4px
}}
.share-count{{font-size:12px;opacity:.8}}
.share-credit{{
  margin-left:auto;font-size:11px;opacity:.6
}}

/* 場タブ */
.venue-tabs{{
  display:flex;gap:0;background:#fff;
  box-shadow:0 1px 4px rgba(0,0,0,.12);
  flex-wrap:wrap;position:sticky;top:0;z-index:100
}}
.vtab{{
  flex:1;min-width:70px;padding:10px 6px;
  font-size:13px;font-weight:700;
  border:none;background:#f8f9fb;color:#555;
  cursor:pointer;border-bottom:3px solid transparent;
  transition:.15s
}}
.vtab:hover{{background:#eef1f9;color:#0d2b5e}}
.vtab.v-active{{background:#fff;color:#0d2b5e;border-bottom:3px solid var(--share-gold)}}

/* 場パネル */
.vpanel{{display:none}}
.vpanel.v-active{{display:block}}

/* レースタブ */
.race-tabs{{
  display:flex;gap:4px;padding:10px 12px 0;
  background:#fff;flex-wrap:wrap;
  border-bottom:1px solid #dde3ee
}}
.rtab{{
  padding:5px 12px;font-size:12px;font-weight:600;
  border:1px solid #dde3ee;border-radius:6px 6px 0 0;
  background:#f8f9fb;color:#555;cursor:pointer;
  border-bottom:none;transition:.15s
}}
.rtab:hover{{background:#eef1f9;color:#0d2b5e}}
.rtab.r-active{{background:#0d2b5e;color:#fff;border-color:#0d2b5e}}

/* レースパネル */
.race-panels{{background:#fff}}
.rpanel{{display:none;padding:0}}
.rpanel.r-active{{display:block}}

/* 元のHTMLのwrapスタイル上書き */
.rpanel .wrap{{max-width:100%;padding:12px}}
</style>
</head><body>

<div class="share-header">
  <h1>🏇 D-AI競馬予想</h1>
  <span class="share-date">{DATE}</span>
  <span class="share-count">{len(venue_order)}場・{total}レース</span>
  <span class="share-credit">D-AIkeiba</span>
</div>

<div class="venue-tabs">
{venue_tabs_html}</div>

{venue_panels_html}

<div style="text-align:center;color:#9ca3af;font-size:11px;padding:16px">
  D-AI競馬予想システム　|　{DATE}　|　{total}レース分析済み
</div>

<script>
function showVenue(vi) {{
  document.querySelectorAll('.vtab').forEach((t,i) => t.classList.toggle('v-active', i===vi));
  document.querySelectorAll('.vpanel').forEach((p,i) => p.classList.toggle('v-active', i===vi));
}}
function showRace(vi, ri) {{
  const rtabs   = document.querySelectorAll('#rtabs-'   + vi + ' .rtab');
  const rpanels = document.querySelectorAll('#vpanel-'  + vi + ' .rpanel');
  rtabs.forEach((t,i)   => t.classList.toggle('r-active', i===ri));
  rpanels.forEach((p,i) => p.classList.toggle('r-active', i===ri));
}}
</script>
</body></html>"""

# ── ファイル書き出し ──────────────────────────────────────────────────
with open(OUT_PATH, "w", encoding="utf-8") as f:
    f.write(combined)

size_kb = os.path.getsize(OUT_PATH) // 1024
print(f"\n完成: {OUT_PATH}")
print(f"  サイズ: {size_kb:,} KB")
print(f"  場数:   {len(venue_order)}")
print(f"  レース: {total}")
print(f"\nLINEでそのまま送付できます: {os.path.basename(OUT_PATH)}")
