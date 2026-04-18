"""既存 _results.json のうち official 経由で time/popularity/odds が欠けている
レースに対して netkeiba 補完を走らせるバックフィルスクリプト。

使い方:
    python scripts/backfill_result_details.py --date 2026-04-18
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.results_tracker import (  # noqa: E402
    fetch_single_race_result,
    _is_details_incomplete,
    _is_corners_empty,
)
from src.scraper.netkeiba import NetkeibaClient  # noqa: E402


def _format_duration(sec: float) -> str:
    sec = max(0, int(sec))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _progress_bar(done: int, total: int, start_ts: float) -> str:
    pct = (done / total * 100) if total else 0.0
    bar_len = 40
    filled = int(bar_len * done / total) if total else 0
    bar = "#" * filled + "-" * (bar_len - filled)
    elapsed = time.time() - start_ts
    if done > 0:
        eta = elapsed / done * (total - done)
    else:
        eta = 0.0
    return (
        f"進捗: [{bar}] {done}/{total} {pct:5.1f}% "
        f"経過 {_format_duration(elapsed)} / 残り {_format_duration(eta)}"
    )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = p.parse_args()

    date = args.date
    fpath = os.path.join("data", "results", f"{date.replace('-', '')}_results.json")
    if not os.path.exists(fpath):
        print(f"[ERROR] results.json が見つかりません: {fpath}")
        return 1

    with open(fpath, "r", encoding="utf-8") as f:
        results = json.load(f)

    # 欠落レースを抽出
    targets = []
    for rid, entry in results.items():
        if not isinstance(entry, dict):
            continue
        order = entry.get("order", [])
        if not order:
            continue
        if _is_details_incomplete(order) or _is_corners_empty(order):
            targets.append(rid)

    print("=" * 72)
    print("結果詳細バックフィル (time / popularity / odds / corners)")
    print(f"対象ファイル: {fpath}")
    print(f"欠落レース: {len(targets)}/{len(results)} 件")
    print("=" * 72)

    if not targets:
        print("補完対象なし")
        return 0

    client = NetkeibaClient()
    start_ts = time.time()
    success, failed = 0, 0

    for i, rid in enumerate(targets, start=1):
        venue = rid[4:6]
        rno = int(rid[10:12]) if len(rid) >= 12 else 0
        print(f"[{i:3d}/{len(targets)}] race_id={rid} (場={venue} R={rno})")
        try:
            ret = fetch_single_race_result(date=date, race_id=rid, client=client)
            if ret and not _is_details_incomplete(ret.get("order", [])):
                print("    OK  補完完了")
                success += 1
            else:
                print("    NG  補完失敗 or 依然欠落")
                failed += 1
        except Exception as e:
            print(f"    ERR {e}")
            failed += 1
        print(_progress_bar(i, len(targets), start_ts))

    print("=" * 72)
    print(f"完了: 成功 {success} / 失敗 {failed} / 計 {len(targets)}")
    print(f"総時間: {_format_duration(time.time() - start_ts)}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
