"""翌日予想生成ラッパー（pythonw経由で非表示実行）"""
import sys, os, subprocess
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
subprocess.run(
    [sys.executable.replace("pythonw", "python"), "run_daily_auto.py",
     "--predict", "--date", tomorrow, "--official"],
    cwd=r"c:\Users\dsuzu\keiba\keiba-v3",
    creationflags=0x08000000  # CREATE_NO_WINDOW
)
