# -*- coding: utf-8 -*-
"""失敗10日分リトライ (offline+カレンダーバイパス)"""
import os
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

failed = ["20260508", "20260509", "20260511", "20260512", "20260513",
          "20260514", "20260515", "20260516", "20260517", "20260518"]
total = len(failed)
python_exe = sys.executable.replace("/", "\\")
log_path = Path("data/logs/batch_rerun_retry.log")


def out(msg):
    print(msg, flush=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def main():
    out(f"=== 失敗10日リトライ (offline+カレンダーバイパス) ===")
    ok = 0
    ng = 0
    for i, d in enumerate(failed):
        fmt = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        out(f"[{i+1}/{total}] {fmt}")
        # 中断マーカー削除
        marker = Path(f"output/.done_{d}.txt")
        if marker.exists():
            marker.unlink()
        t0 = time.time()
        cmd = (
            f'"{python_exe}" run_analysis_date.py {fmt}'
            f" --offline --no-html --race-ids-from-pred"
            f" > data/logs/_batch_day_{d}.log 2>&1"
        )
        rc = os.system(cmd)
        elapsed = time.time() - t0
        if rc == 0:
            ok += 1
            out(f"  ✅ {fmt} ({elapsed:.0f}秒)")
        else:
            ng += 1
            out(f"  ❌ {fmt} ({elapsed:.0f}秒) rc={rc}")
        time.sleep(5)
    out(f"\n完了: 成功={ok} 失敗={ng}")


if __name__ == "__main__":
    main()
