"""
収集プロセス自動ウォッチャー
collect_course_db が止まったら自動で再起動し続ける
使い方:
  python watch_collect.py                 # 今日まで収集
  python watch_collect.py 2026-12-31      # 指定日まで収集
"""
import subprocess, time, sys, os, json, io, datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

WORK_DIR = os.path.dirname(os.path.abspath(__file__))

# コマンドライン引数で目標日を指定可能（デフォルト: 今日）
if len(sys.argv) > 1:
    TARGET_END = sys.argv[1]
else:
    TARGET_END = datetime.date.today().strftime("%Y-%m-%d")

STATE_PATH     = os.path.join(WORK_DIR, "data", "course_db_collector_state.json")
DB_PATH        = os.path.join(WORK_DIR, "data", "course_db_preload.json")
CHECK_INTERVAL = 60   # 60秒ごとに生存確認
MAX_IDLE_SEC   = 300  # 300秒間DBが更新されなければ強制再起動

proc = None
last_mtime = 0

def get_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def is_done():
    st = get_state()
    last = st.get("last_date", "")
    status = st.get("status", "")
    if status == "completed":
        return True
    if last >= TARGET_END:
        return True
    return False

def db_mtime():
    try:
        return os.path.getmtime(DB_PATH)
    except Exception:
        return 0

def start_proc():
    global proc
    print("[ウォッチャー] 収集プロセスを起動します...")
    proc = subprocess.Popen(
        [sys.executable, "-u", "main.py",
         "--collect_course_db", "--append",
         "--end", TARGET_END],
        cwd=WORK_DIR,
    )
    print(f"  PID: {proc.pid}")
    return proc

def run():
    global proc, last_mtime
    print(f"[ウォッチャー] 開始 (目標: {TARGET_END})")
    print(f"  WORK_DIR: {WORK_DIR}")
    last_mtime = db_mtime()
    start_proc()

    idle_since = time.time()
    while True:
        time.sleep(CHECK_INTERVAL)

        if is_done():
            print("[ウォッチャー] 収集完了! 終了します")
            if proc and proc.poll() is None:
                proc.terminate()
            break

        alive = proc is not None and proc.poll() is None
        cur_mtime = db_mtime()

        if cur_mtime != last_mtime:
            last_mtime = cur_mtime
            idle_since = time.time()
            st = get_state()
            print(f"[進捗] last_date={st.get('last_date')}  DB更新確認")

        idle_sec = time.time() - idle_since

        if not alive or idle_sec > MAX_IDLE_SEC:
            reason = "プロセス終了" if not alive else f"DB未更新({idle_sec:.0f}秒)"
            st = get_state()
            print(f"[ウォッチャー] 再起動 ({reason}) last_date={st.get('last_date')}")
            if proc and proc.poll() is None:
                proc.terminate()
                time.sleep(2)
            proc = start_proc()
            idle_since = time.time()

if __name__ == "__main__":
    run()
