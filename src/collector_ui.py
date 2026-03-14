"""
基準タイムDB収集 - Web管理画面
ブラウザから日付入力・ボタン操作で収集を実行できる。
"""

import json
import os
import sys
import threading
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, jsonify, render_template_string, request

from src.log import get_logger

logger = get_logger(__name__)

try:
    from config.settings import (
        COURSE_DB_COLLECTOR_STATE_PATH,
        COURSE_DB_PRELOAD_PATH,
    )
except Exception:
    COURSE_DB_PRELOAD_PATH = os.path.join(
        os.path.dirname(__file__), "..", "data", "course_db_preload.json"
    )
    COURSE_DB_COLLECTOR_STATE_PATH = os.path.join(
        os.path.dirname(__file__), "..", "data", "course_db_collector_state.json"
    )

# 実行中状態（スレッド間共有）
_collector_state = {
    "running": False,
    "day_index": 0,
    "total_days": 0,
    "total_runs": 0,
    "current_date": "",
    "status": "idle",
    "error": None,
}

HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>基準タイムDB収集 - D-AI競馬予想</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: 'Segoe UI', 'Yu Gothic', sans-serif; margin: 24px; max-width: 640px; }
    h1 { font-size: 1.4rem; margin-bottom: 16px; }
    .card { background: #f5f5f5; padding: 20px; border-radius: 8px; margin-bottom: 16px; }
    label { display: block; margin-bottom: 4px; font-weight: 500; }
    input[type="date"] { padding: 8px; font-size: 1rem; margin-bottom: 12px; width: 100%; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; }
    .row > * { flex: 1; min-width: 140px; }
    button { padding: 10px 20px; font-size: 1rem; cursor: pointer; border: none; border-radius: 6px; }
    button.primary { background: #2563eb; color: white; }
    button.secondary { background: #64748b; color: white; }
    button.success { background: #16a34a; color: white; }
    button:disabled { opacity: 0.6; cursor: not-allowed; }
    button + button { margin-left: 8px; margin-top: 8px; }
    #progress { margin-top: 16px; }
    .progress-bar { height: 24px; background: #e2e8f0; border-radius: 4px; overflow: hidden; }
    .progress-fill { height: 100%; background: #2563eb; transition: width 0.3s; }
    .status { margin-top: 8px; color: #475569; font-size: 0.9rem; }
    .error { color: #dc2626; margin-top: 8px; }
    .info { font-size: 0.85rem; color: #64748b; margin-top: 12px; }
  </style>
</head>
<body>
  <h1>基準タイムDB収集</h1>
  <p class="info">JRA・地方競馬のレース結果（1〜3着）をスクレイピングし、基準タイムを蓄積します。</p>

  <div class="card">
    <label>開始日</label>
    <input type="date" id="start_date" value="{{ default_start }}">
    <label>終了日</label>
    <input type="date" id="end_date" value="{{ default_end }}">
  </div>

  <div class="card">
    <p style="margin: 0 0 12px 0;"><strong>操作</strong></p>
    <button id="btn_full" class="primary">全件収集</button>
    <button id="btn_resume" class="secondary">途中再開</button>
    <button id="btn_append" class="success">新規分のみ追加</button>
  </div>

  <div id="progress" style="display:none;">
    <div class="progress-bar"><div class="progress-fill" id="fill" style="width:0%"></div></div>
    <p class="status" id="status">準備中...</p>
    <p class="error" id="err"></p>
  </div>

  <div class="card">
    <p style="margin: 0 0 8px 0;"><strong>収集済み状態</strong></p>
    <p class="status" id="saved_state">読込中...</p>
    <p class="info">保存先: {{ output_path }}</p>
  </div>

  <script>
    const start = document.getElementById('start_date');
    const end = document.getElementById('end_date');
    const btnFull = document.getElementById('btn_full');
    const btnResume = document.getElementById('btn_resume');
    const btnAppend = document.getElementById('btn_append');
    const progress = document.getElementById('progress');
    const fill = document.getElementById('fill');
    const status = document.getElementById('status');
    const err = document.getElementById('err');
    const savedState = document.getElementById('saved_state');

    function setRunning(r) {
      btnFull.disabled = btnResume.disabled = btnAppend.disabled = r;
      progress.style.display = r ? 'block' : 'none';
      if (!r) err.textContent = '';
    }

    async function startCollect(mode) {
      setRunning(true);
      err.textContent = '';
      const res = await fetch('/api/start', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          mode,
          start_date: start.value,
          end_date: end.value
        })
      });
      const j = await res.json();
      if (!j.ok) { err.textContent = j.error || 'エラー'; setRunning(false); return; }
      pollStatus();
    }

    async function pollStatus() {
      const res = await fetch('/api/status');
      const j = await res.json();
      if (j.running) {
        fill.style.width = (j.total_days ? (j.day_index / j.total_days * 100) : 0) + '%';
        status.textContent = j.day_index + '/' + j.total_days + '日 (' + (j.total_runs||0) + '走) ' + (j.current_date || '');
        setTimeout(pollStatus, 1500);
      } else {
        if (j.error) err.textContent = j.error;
        status.textContent = j.status === 'completed' ? '完了' : j.status || '停止';
        setRunning(false);
        loadState();
      }
    }

    async function loadState() {
      const res = await fetch('/api/state');
      const j = await res.json();
      if (j.last_date) {
        savedState.textContent = '最終収集日: ' + j.last_date + ' | 総走数: ' + (j.total_runs||0) + ' 走';
      } else {
        savedState.textContent = '未収集';
      }
    }

    btnFull.onclick = () => startCollect('full');
    btnResume.onclick = () => startCollect('resume');
    btnAppend.onclick = () => startCollect('append');

    loadState();
  </script>
</body>
</html>
"""


def create_app():
    app = Flask(__name__)
    today = datetime.now().strftime("%Y-%m-%d")
    default_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    @app.route("/")
    def index():
        return render_template_string(
            HTML,
            default_start=default_start,
            default_end=today,
            output_path=COURSE_DB_PRELOAD_PATH,
        )

    @app.route("/api/start", methods=["POST"])
    def api_start():
        global _collector_state
        if _collector_state["running"]:
            return jsonify(ok=False, error="既に実行中です")
        data = request.get_json() or {}
        mode = data.get("mode", "full")
        start_date = data.get("start_date", default_start)
        end_date = data.get("end_date", today)
        _collector_state["running"] = True
        _collector_state["error"] = None
        _collector_state["day_index"] = 0
        _collector_state["total_days"] = 0
        _collector_state["total_runs"] = 0
        _collector_state["current_date"] = ""
        _collector_state["status"] = "starting"
        thread = threading.Thread(
            target=_run_collector,
            args=(mode, start_date, end_date),
            daemon=True,
        )
        thread.start()
        return jsonify(ok=True)

    @app.route("/api/status")
    def api_status():
        return jsonify(_collector_state)

    @app.route("/api/state")
    def api_state():
        st = {}
        if os.path.exists(COURSE_DB_COLLECTOR_STATE_PATH):
            try:
                with open(COURSE_DB_COLLECTOR_STATE_PATH, "r", encoding="utf-8") as f:
                    st = json.load(f)
            except Exception:
                logger.debug("api/state JSON read failed", exc_info=True)
        return jsonify(st)

    return app


def _run_collector(mode: str, start_date: str, end_date: str):
    global _collector_state
    try:
        from src.scraper.course_db_collector import collect_course_db_from_results
        from src.scraper.netkeiba import NetkeibaClient, RaceListScraper

        def progress(day_i, total, runs, cur_date, st):
            _collector_state["day_index"] = day_i
            _collector_state["total_days"] = total
            _collector_state["total_runs"] = runs
            _collector_state["current_date"] = cur_date
            _collector_state["status"] = st

        client = NetkeibaClient(no_cache=True)
        race_list = RaceListScraper(client)
        collect_course_db_from_results(
            client,
            race_list,
            start_date,
            end_date,
            COURSE_DB_PRELOAD_PATH,
            state_path=COURSE_DB_COLLECTOR_STATE_PATH,
            mode=mode,
            progress_callback=progress,
        )
    except Exception as e:
        logger.warning("collector run failed: %s", e, exc_info=True)
        _collector_state["error"] = str(e)
        _collector_state["status"] = "error"
    finally:
        _collector_state["running"] = False


def run_server(port: int = 5050, open_browser: bool = True):
    app = create_app()
    url = f"http://127.0.0.1:{port}"
    logger.info("\n[基準タイムDB収集] Web管理画面")
    logger.info(f"   {url}")
    logger.info("   ブラウザで開いて操作してください。\n")
    if open_browser:

        def _open():
            import time

            time.sleep(1.2)
            try:
                import webbrowser

                webbrowser.open(url)
            except Exception:
                logger.debug("browser open failed", exc_info=True)

        threading.Thread(target=_open, daemon=True).start()
    app.run(host="127.0.0.1", port=port, threaded=True, use_reloader=False)


if __name__ == "__main__":
    run_server(port=int(os.environ.get("PORT", 5050)))
