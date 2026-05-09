"""Watchdog: ダッシュボード + cloudflared の生存確認・自動再起動"""
import subprocess, os, datetime, json, sys, time
import urllib.request

# プロジェクトルートを sys.path に追加 (src.slack_notify を import するため)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

LOG = os.path.join(_PROJECT_ROOT, "data", "watchdog.log")
LOG_MAX_BYTES = 1 * 1024 * 1024  # 1 MB でローテーション
STATE_FILE = os.path.join(_PROJECT_ROOT, "data", "watchdog_state.json")
DASHBOARD_PY = os.path.join(_PROJECT_ROOT, "src", "dashboard.py")
HEALTH_URL = "http://127.0.0.1:5051/api/health"

# ウィンドウを一切表示しないフラグ
_NO_WINDOW = 0x08000000  # CREATE_NO_WINDOW
_DETACHED = 0x00000008   # DETACHED_PROCESS


def _rotate_log_if_needed():
    """watchdog.log が LOG_MAX_BYTES を超えたら .1 にローテーション (1世代)。"""
    try:
        if os.path.isfile(LOG) and os.path.getsize(LOG) > LOG_MAX_BYTES:
            backup = LOG + ".1"
            if os.path.isfile(backup):
                os.remove(backup)
            os.rename(LOG, backup)
    except Exception:
        pass


def log(msg):
    _rotate_log_if_needed()
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)


def _load_state() -> dict:
    """watchdog_state.json を読み込む。存在しない場合は初期値を返す。"""
    default = {
        "consecutive_failures": 0,
        "last_failure": None,
        "last_restart": None,
        "last_code_restart": None,
        "dashboard_mtime": None,
    }
    if not os.path.isfile(STATE_FILE):
        return default
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 欠損キーを補完
        for k, v in default.items():
            data.setdefault(k, v)
        return data
    except Exception:
        return default


def _save_state(state: dict):
    """watchdog_state.json に状態を書き込む。"""
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"[WARN] state.json 書き込み失敗: {e}")


def _now_iso() -> str:
    """現在時刻を ISO 形式文字列で返す。"""
    return datetime.datetime.now().isoformat(timespec="seconds")


def _try_send_slack(message: str, level: str, title: str):
    """Slack 通知を送る。SLACK_WEBHOOK_URL 未設定やエラーでも無視して続行。"""
    try:
        from src.slack_notify import send_slack
        send_slack(message, level=level, title=title)
    except Exception as e:
        log(f"[WARN] Slack 通知失敗 (無視して続行): {e}")


def _start_dashboard():
    """dashboard.py をバックグラウンドで起動する。"""
    subprocess.Popen(
        [r"C:\Program Files\Python311\pythonw.exe", "src/dashboard.py"],
        cwd=_PROJECT_ROOT,
        creationflags=_DETACHED | _NO_WINDOW,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
    )


def _kill_dashboard_by_pid(pid: int):
    """指定 PID の dashboard プロセスを強制終了する。"""
    try:
        subprocess.run(
            ["taskkill", "/F", "/PID", str(pid)],
            capture_output=True,
            creationflags=_NO_WINDOW,
        )
        log(f"ダッシュボード PID {pid} を kill しました")
    except Exception as e:
        log(f"[WARN] PID {pid} kill 失敗: {e}")


def check_dashboard(state: dict) -> dict:
    """ダッシュボードの HTTP ヘルスチェックと自動再起動。

    Returns:
        更新後の state dict
    """
    # --- コード変更検知: dashboard.py の mtime を記録 ---
    current_mtime = None
    code_changed = False
    try:
        current_mtime = str(os.path.getmtime(DASHBOARD_PY))
    except Exception:
        pass

    prev_mtime = state.get("dashboard_mtime")
    if current_mtime and prev_mtime and current_mtime != prev_mtime:
        code_changed = True
        log(f"dashboard.py のコード変更を検知しました (mtime: {prev_mtime} → {current_mtime})")

    # mtime を更新
    if current_mtime:
        state["dashboard_mtime"] = current_mtime

    # --- コード変更による再起動 ---
    if code_changed:
        # /api/health から PID を取得して kill → 再起動
        pid = None
        try:

            with urllib.request.urlopen(HEALTH_URL, timeout=5) as resp:
                health = json.loads(resp.read().decode("utf-8"))
                pid = health.get("pid")
        except Exception:
            pass

        if pid:
            _kill_dashboard_by_pid(pid)
            time.sleep(3)

        log("dashboard.py コード更新のため再起動します")
        _start_dashboard()
        state["last_code_restart"] = _now_iso()

        _try_send_slack(
            f"dashboard.py のコード変更を検知し、自動再起動しました。\n再起動時刻: {_now_iso()}",
            level="info",
            title="Dashboard コード更新再起動",
        )
        # コード更新再起動後は通常の health チェックをスキップ
        return state

    # --- HTTP ヘルスチェック ---
    healthy = False
    health_data = {}
    try:
        import urllib.request
        with urllib.request.urlopen(HEALTH_URL, timeout=5) as resp:
            health_data = json.loads(resp.read().decode("utf-8"))
        # status == "ok" かつ db_connected == true を healthy とみなす
        if health_data.get("status") == "ok" and health_data.get("db_connected") is not False:
            healthy = True
        else:
            reason = []
            if health_data.get("status") != "ok":
                reason.append(f"status={health_data.get('status')}")
            if health_data.get("db_connected") is False:
                reason.append("db_connected=false")
            log(f"Dashboard ヘルスチェック異常: {', '.join(reason)}")
    except Exception as e:
        log(f"Dashboard HTTP 接続失敗: {e}")

    if healthy:
        # 復旧: 連続失敗カウントをリセット
        if state["consecutive_failures"] > 0:
            log(f"Dashboard 復旧を確認しました (連続失敗 {state['consecutive_failures']} 回 → 0 にリセット)")
        state["consecutive_failures"] = 0
        return state

    # --- 異常: 失敗カウントを増やして再起動 ---
    state["consecutive_failures"] += 1
    state["last_failure"] = _now_iso()
    failures = state["consecutive_failures"]
    log(f"Dashboard ダウン検知 (連続失敗: {failures} 回)")

    # 3 回連続失敗 (= 約 15 分ダウン) → Slack critical 通知
    if failures >= 3:
        log(f"Dashboard 連続障害 {failures} 回: Slack critical 通知を送ります")
        _try_send_slack(
            f"Dashboard が {failures} 回連続 (約 {failures * 5} 分) ダウンしています。\n"
            f"最終失敗: {state['last_failure']}\n"
            f"最終再起動: {state.get('last_restart') or '未実施'}",
            level="critical",
            title="Dashboard 連続障害",
        )

    # 再起動
    log("Dashboard を再起動します...")
    _start_dashboard()
    state["last_restart"] = _now_iso()

    _try_send_slack(
        f"Dashboard が停止していたため自動再起動しました。\n"
        f"再起動時刻: {_now_iso()}\n"
        f"連続失敗回数: {failures}",
        level="warning",
        title="Dashboard 自動再起動",
    )

    return state


# ── メイン処理 ────────────────────────────────────────────────────────────

# 状態を読み込む
state = _load_state()

# ダッシュボードチェック (HTTP ヘルスチェック + コード変更検知 + 自動再起動)
state = check_dashboard(state)

# 状態を永続化
_save_state(state)

# cloudflaredチェック（プロセス存在確認のみ・既存ロジック維持）
result = subprocess.run(
    ["tasklist", "/FI", "IMAGENAME eq cloudflared.exe"],
    capture_output=True, text=True, creationflags=_NO_WINDOW
)
if "cloudflared.exe" not in result.stdout:
    log("cloudflared down, restarting...")
    subprocess.Popen(
        [r"c:\Users\dsuzu\keiba\keiba-v3\cloudflared.exe",
         "tunnel", "--config", r"C:\Users\dsuzu\.cloudflared\config.yml",
         "run", "keiba-dash"],
        cwd=_PROJECT_ROOT,
        creationflags=_DETACHED | _NO_WINDOW,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
    )
