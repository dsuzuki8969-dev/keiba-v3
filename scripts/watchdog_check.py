"""Watchdog: ダッシュボード + cloudflared の生存確認・自動再起動"""
import subprocess, socket, os, datetime

LOG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "watchdog.log")
# ウィンドウを一切表示しないフラグ
_NO_WINDOW = 0x08000000  # CREATE_NO_WINDOW
_DETACHED = 0x00000008   # DETACHED_PROCESS

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

# ダッシュボードチェック (port 5051)
try:
    s = socket.create_connection(("127.0.0.1", 5051), timeout=3)
    s.close()
except Exception:
    log("Dashboard down, restarting...")
    subprocess.Popen(
        [r"C:\Program Files\Python311\pythonw.exe", "src/dashboard.py"],
        cwd=r"c:\Users\dsuzu\keiba\keiba-v3",
        creationflags=_DETACHED
    )

# cloudflaredチェック（プロセス存在確認のみ）
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
        cwd=r"c:\Users\dsuzu\keiba\keiba-v3",
        creationflags=_DETACHED
    )
