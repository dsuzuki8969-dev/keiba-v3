# -*- coding: utf-8 -*-
"""payouts が完全空の NAR レースを netkeiba から再取得

帯広(ばんえい, venue=65)は三連複制度なしのため除外。
results.json の該当エントリを netkeiba 結果で上書きする。

使用方法:
  python scripts/backfill_empty_results.py --dry-run
  python scripts/backfill_empty_results.py
  python scripts/backfill_empty_results.py --max-fetch 5
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import RESULTS_DIR

BANEI_VENUE = "65"


def find_empty_payouts():
    """payouts が空の非ばんえいレースを抽出"""
    missing = []
    res_dir = Path(RESULTS_DIR)
    for fp in sorted(res_dir.glob("*_results.json")):
        date_str = fp.stem.split("_")[0]
        with open(fp, encoding="utf-8") as f:
            data = json.load(f)
        for race_id, race in data.items():
            if race_id[4:6] == BANEI_VENUE:
                continue
            payouts = race.get("payouts", {})
            has_trio = bool(payouts.get("三連複") or payouts.get("sanrenpuku"))
            if not has_trio:
                missing.append({
                    "file": str(fp),
                    "date": date_str,
                    "race_id": race_id,
                    "payouts_empty": len(payouts) == 0,
                })
    return missing


def main():
    parser = argparse.ArgumentParser(description="空 payouts NAR レース再取得")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-fetch", type=int, default=0, help="最大取得数 (0=無制限)")
    parser.add_argument("--rate", type=float, default=2.5, help="リクエスト間隔 (秒)")
    args = parser.parse_args()

    missing = find_empty_payouts()
    print(f"三連複欠損 (非ばんえい): {len(missing)} レース")
    empty_cnt = sum(1 for m in missing if m["payouts_empty"])
    partial_cnt = len(missing) - empty_cnt
    print(f"  payouts 完全空: {empty_cnt}, 三連複のみ欠損: {partial_cnt}")
    est_min = len(missing) * args.rate / 60
    print(f"  推定所要時間: {est_min:.1f} 分 ({args.rate}秒/件)")

    if args.dry_run or not missing:
        return

    from src.results_tracker import fetch_single_race_result
    from src.scraper.netkeiba import NetkeibaClient

    client = NetkeibaClient()

    by_file = {}
    for m in missing:
        by_file.setdefault(m["file"], []).append(m)

    total = len(missing)
    if args.max_fetch > 0:
        total = min(total, args.max_fetch)

    done = 0
    fixed = 0
    failed = 0

    for fpath, entries in sorted(by_file.items()):
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        changed = False
        for entry in entries:
            if args.max_fetch > 0 and done >= args.max_fetch:
                break

            race_id = entry["race_id"]
            date_str = entry["date"]
            date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            try:
                result = fetch_single_race_result(
                    date=date_fmt,
                    race_id=race_id,
                    client=client,
                )
                if result and result.get("payouts"):
                    data[race_id] = result
                    changed = True
                    fixed += 1
                elif result and result.get("order"):
                    data[race_id] = result
                    changed = True
                    fixed += 1
                    print(f"  ⚠ {date_str} {race_id}: order あり payouts なし")
                else:
                    failed += 1
                    print(f"  ✗ {date_str} {race_id}: 結果取得失敗")
            except Exception as e:
                failed += 1
                print(f"  ✗ {date_str} {race_id}: {e}")

            done += 1
            if done % 10 == 0 or done == total:
                pct = done / total * 100
                filled = int(30 * done / total)
                bar = "█" * filled + "░" * (30 - filled)
                print(f"  [{bar}] {pct:5.1f}% ({done}/{total}) fixed={fixed} failed={failed}")

            time.sleep(args.rate)

        if changed:
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n完了: fixed={fixed}, failed={failed}, total={done}")


if __name__ == "__main__":
    main()
