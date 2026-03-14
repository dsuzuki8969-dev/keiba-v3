"""
output/ をスキャンしてダッシュボード index.html を生成する。
run_tokyo11r.py の末尾から自動呼び出しされる。
"""
import os
import json
import re
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, "output")
COURSE_DB_PATH      = os.path.join(PROJECT_ROOT, "data", "course_db_preload.json")
COLLECTOR_STATE_PATH= os.path.join(PROJECT_ROOT, "data", "course_db_collector_state.json")
BLOODLINE_DB_PATH   = os.path.join(PROJECT_ROOT, "data", "bloodline_db.json")
TRAINER_BL_PATH     = os.path.join(PROJECT_ROOT, "data", "trainer_baseline_db.json")


# ── ファイルスキャン ──────────────────────────────────────────────

def _parse_filename(name: str) -> dict:
    """ファイル名から日付・場所・レース番号を推定"""
    # 20260222_東京11R.html
    m = re.match(r"(\d{4})(\d{2})(\d{2})_(.+)\.html$", name)
    if m:
        y, mo, d, label = m.groups()
        date_s = f"{y}/{mo}/{d}"
        return {"date": date_s, "label": label, "sort_key": f"{y}{mo}{d}"}
    # 202506010101_中山1R.html  (race_id形式)
    m2 = re.match(r"(\d{4})(\d{2})(\d{2})\d{4}_(.+)\.html$", name)
    if m2:
        y, mo, d, label = m2.groups()
        date_s = f"{y}/{mo}/{d}"
        return {"date": date_s, "label": label, "sort_key": f"{y}{mo}{d}"}
    return {"date": "—", "label": name.replace(".html",""), "sort_key": "0"}


def scan_output():
    if not os.path.isdir(OUTPUT_DIR):
        return []
    results = []
    for name in os.listdir(OUTPUT_DIR):
        if not name.endswith(".html") or name == "index.html":
            continue
        path  = os.path.join(OUTPUT_DIR, name)
        mtime = datetime.fromtimestamp(os.path.getmtime(path))
        size  = os.path.getsize(path) // 1024
        info  = _parse_filename(name)
        info.update({"file": name, "mtime": mtime, "mtime_s": mtime.strftime("%m/%d %H:%M"), "size_kb": size})
        results.append(info)
    results.sort(key=lambda x: (x["sort_key"], x["mtime"]), reverse=True)
    return results


# ── DB 状態 ───────────────────────────────────────────────────────

def get_db_state():
    s = {"course_runs": 0, "last_date": "—", "bloodline": 0, "trainer_bl": 0}
    if os.path.exists(COURSE_DB_PATH):
        try:
            with open(COURSE_DB_PATH, encoding="utf-8") as f:
                d = json.load(f)
            db = d.get("course_db", d)
            s["course_runs"] = sum(len(v) for v in db.values())
        except Exception:
            pass
    if os.path.exists(COLLECTOR_STATE_PATH):
        try:
            with open(COLLECTOR_STATE_PATH, encoding="utf-8") as f:
                cs = json.load(f)
            s["last_date"] = cs.get("last_date", "—")
        except Exception:
            pass
    if os.path.exists(BLOODLINE_DB_PATH):
        try:
            with open(BLOODLINE_DB_PATH, encoding="utf-8") as f:
                bl = json.load(f)
            s["bloodline"] = len(bl)
        except Exception:
            pass
    if os.path.exists(TRAINER_BL_PATH):
        try:
            with open(TRAINER_BL_PATH, encoding="utf-8") as f:
                tb = json.load(f)
            s["trainer_bl"] = len(tb)
        except Exception:
            pass
    return s


# ── HTML 生成 ─────────────────────────────────────────────────────

RUN_FORM_HTML = """
<div class="run-box">
  <div class="run-title">🏇 予想を実行する</div>
  <div class="run-form">
    <input id="run-date" type="date" value="">
    <button id="run-date-btn" onclick="runDate()">全レース実行</button>
  </div>
  <div style="font-size:11px;color:#6b7280;margin-top:6px">中央・地方24場の全レースを順次分析します</div>

  <div id="progress-area" style="display:none;margin-top:14px">
    <div class="prog-bar-wrap"><div id="prog-bar" class="prog-bar"></div></div>
    <div id="prog-msg" class="prog-msg">—</div>
    <div id="prog-result"></div>
    <details style="margin-top:8px">
      <summary style="font-size:11px;color:#6b7280;cursor:pointer">ログを表示</summary>
      <pre id="prog-log" class="prog-log"></pre>
    </details>
  </div>
</div>
<script>
(function(){
  var d = new Date(); var s = d.toISOString().slice(0,10);
  document.getElementById('run-date').value = s;
})();

let _polling = null;

function runDate() {
  var date = document.getElementById('run-date').value;
  if (!date) { alert('日付を選択してください'); return; }
  document.getElementById('run-date-btn').disabled = true;
  document.getElementById('progress-area').style.display = 'block';
  document.getElementById('prog-result').innerHTML = '';
  setProgress(3, '送信中...');
  fetch('/api/run_date', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({date: date})})
    .then(r => r.json()).then(d => {
      if (!d.ok) { alert(d.error); document.getElementById('run-date-btn').disabled = false; return; }
      _polling = setInterval(pollStatus, 1500);
    }).catch(function(){ alert('サーバーに接続できません（app.py が起動しているか確認）'); document.getElementById('run-date-btn').disabled = false; });
}

function pollStatus() {
  fetch('/api/status').then(r => r.json()).then(d => {
    setProgress(d.progress, d.message);
    if (d.log) document.getElementById('prog-log').textContent = d.log.slice(-40).join('\\n');
    if (!d.running) {
      clearInterval(_polling);
      document.getElementById('run-date-btn').disabled = false;
      if (d.result_url) {
        document.getElementById('prog-result').innerHTML =
          '<a class="result-link" href="' + d.result_url + '" target="_blank">📄 分析結果を開く</a>' +
          ' &nbsp;<a class="result-link" href="/" style="background:#0d2b5e">↺ ダッシュボードを更新</a>';
      } else if (d.error) {
        document.getElementById('prog-result').innerHTML = '<span style="color:#c0392b">⚠ ' + d.error.replace(/\\n/g,'<br>') + '</span>';
      }
    }
  }).catch(function(){});
}
function setProgress(pct, msg) {
  document.getElementById('prog-bar').style.width = pct + '%';
  document.getElementById('prog-msg').textContent = msg;
}
</script>
"""

def _card_html(item: dict) -> str:
    is_all = "全レース" in item["label"]
    badge = '<span class="badge-all">全R</span>' if is_all else '<span class="badge-single">単R</span>'
    return f"""<a class="race-card" href="{item['file']}" target="_blank">
  <div class="rc-date">{item['date']}</div>
  <div class="rc-label">{badge} {item['label']}</div>
  <div class="rc-meta">{item['mtime_s']} &nbsp;·&nbsp; {item['size_kb']}KB</div>
</a>"""


def generate_html():
    db    = get_db_state()
    now_s = datetime.now().strftime("%Y/%m/%d %H:%M")

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>競馬解析 v3 — ダッシュボード</title>
<style>
:root{{--navy:#0d2b5e;--gold:#c9952a;--bg:#f2f4f8;--card:#fff;--border:#dde2ea;--muted:#6b7280;--red:#c0392b;--green:#1e8c4a}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:"Hiragino Sans","Yu Gothic UI",sans-serif;background:var(--bg);color:#1a1a2e;min-height:100vh}}
.hero{{background:linear-gradient(135deg,var(--navy) 60%,#1a4a8a);color:#fff;padding:28px 32px 24px}}
.hero h1{{font-size:22px;font-weight:800;letter-spacing:.03em;margin-bottom:4px}}
.hero .sub{{font-size:13px;opacity:.75}}
.hero .updated{{font-size:11px;opacity:.55;margin-top:6px}}
.wrap{{max-width:800px;margin:0 auto;padding:24px 20px}}
/* DBステータス */
.db-strip{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px}}
.db-chip{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:10px 16px;font-size:12px;color:var(--muted)}}
.db-chip strong{{display:block;font-size:16px;font-weight:800;color:var(--navy)}}
/* 実行フォーム */
.run-box{{background:var(--card);border:2px solid var(--gold);border-radius:12px;padding:22px 26px;box-shadow:0 2px 8px rgba(0,0,0,.08)}}
.run-title{{font-size:15px;font-weight:700;color:var(--navy);margin-bottom:12px}}
.run-form{{display:flex;gap:8px;flex-wrap:wrap}}
.run-form input[type=date]{{flex:1;min-width:180px;padding:9px 14px;border:1px solid var(--border);border-radius:8px;font-size:14px}}
.run-form button{{padding:9px 28px;background:var(--gold);color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:700;cursor:pointer}}
.run-form button:hover{{background:#b8841f}}.run-form button:disabled{{background:#ccc;cursor:not-allowed}}
.prog-bar-wrap{{background:#eee;border-radius:6px;height:8px;margin:14px 0 6px;overflow:hidden}}
.prog-bar{{height:100%;background:linear-gradient(90deg,var(--gold),var(--navy));border-radius:6px;width:0%;transition:width .4s}}
.prog-msg{{font-size:13px;color:var(--muted);margin-bottom:6px}}
.prog-log{{font-size:10px;background:#f8f9fb;border-radius:6px;padding:8px;max-height:180px;overflow-y:auto;white-space:pre-wrap;margin-top:6px}}
.result-link{{display:inline-block;padding:8px 20px;background:var(--green);color:#fff;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none;margin-top:10px}}
footer{{text-align:center;color:var(--muted);font-size:11px;padding:28px 0;border-top:1px solid var(--border);margin-top:36px}}
</style>
</head>
<body>
<div class="hero">
  <h1>🏇 競馬解析マスターシステム v3</h1>
  <div class="sub">D-AI 予想エンジン — ダッシュボード</div>
  <div class="updated">最終更新: {now_s}</div>
</div>

<div class="wrap">

  <div class="db-strip" style="margin-top:20px">
    <div class="db-chip"><strong>{db['course_runs']:,}</strong>コースDB走数</div>
    <div class="db-chip"><strong>{db['last_date']}</strong>DB最終収集日</div>
    <div class="db-chip"><strong>{db['bloodline']:,}</strong>血統DB（父馬数）</div>
    <div class="db-chip"><strong>{db['trainer_bl']:,}</strong>調教DB蓄積調教師数</div>
  </div>

  {RUN_FORM_HTML}

</div>

<footer>D-AI競馬解析 v3</footer>
</body>
</html>"""


def main():
    html = generate_html()
    out  = os.path.join(OUTPUT_DIR, "index.html")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[dashboard] 生成完了: {out}")


if __name__ == "__main__":
    main()
