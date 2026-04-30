"""バックフィル完了を待って hybrid_summary キャッシュを再計算する long-running waiter。

マスター不在中の自動処理用 (2026-05-01)。
- backfill_all_payouts.progress.log を 60 秒毎に監視
- 「完了:」or「FINAL」を検出したら hybrid/summary/trend/detailed の API を直列で叩いて再キャッシュ
- 全完了をフラグファイル data/logs/_recompute_done.flag に記録
- バックフィル完了後 5 分以内に全キャッシュ更新完了
"""
from __future__ import annotations

import io
import os
import sys
import time
from pathlib import Path
import urllib.request

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

LOG_DIR = Path("data/logs")
PROG_LOG = LOG_DIR / "backfill_all_payouts.progress.log"
WAITER_LOG = LOG_DIR / "wait_and_recompute.log"
DONE_FLAG = LOG_DIR / "_recompute_done.flag"

API_BASE = "http://localhost:5051"


def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}\n"
    with WAITER_LOG.open("a", encoding="utf-8") as f:
        f.write(line)
    try:
        print(line, end="", flush=True)
    except Exception:
        pass


def call_api(path: str, timeout: int = 300) -> tuple[int, float]:
    """API を呼び戻り値 (status, elapsed) を返す。"""
    url = f"{API_BASE}{path}"
    t0 = time.time()
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read()
            return r.status, time.time() - t0
    except Exception as e:
        log(f"  ERR {path}: {e}")
        return 0, time.time() - t0


def wait_backfill():
    log("=== Waiter started: バックフィル完了待機 ===")
    while True:
        if PROG_LOG.exists():
            try:
                text = PROG_LOG.read_text(encoding="utf-8", errors="replace")
            except Exception:
                text = ""
            if "完了:" in text or "FINAL " in text:
                log("バックフィル完了検出 ✓")
                return
        time.sleep(60)


def recompute_caches():
    log("=== キャッシュ再計算開始 ===")
    # キャッシュクリア
    log("invalidate_cache POST...")
    call_api("/api/results/invalidate_cache?_=clear", timeout=30)
    # 全年事前計算 (直列)
    for endpoint, name in [
        ("hybrid_summary", "hybrid"),
        ("summary", "summary"),
        ("trend", "trend"),
        ("detailed", "detailed"),
    ]:
        for y in ["all", "2026", "2025", "2024"]:
            force = "&force=1" if endpoint == "hybrid_summary" else ""
            status, el = call_api(f"/api/results/{endpoint}?year={y}{force}")
            log(f"  {name} {y}: status={status} {el:.1f}s")
    log("=== 全キャッシュ更新完了 ===")


def main():
    LOG_DIR.mkdir(exist_ok=True, parents=True)
    log("waiter PID: " + str(os.getpid()))
    wait_backfill()
    recompute_caches()
    DONE_FLAG.write_text(time.strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")
    log("DONE FLAG written")


if __name__ == "__main__":
    main()
