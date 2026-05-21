# -*- coding: utf-8 -*-
"""壊れた results.json エントリを netkeiba から直接再取得して修復

backfill_results_full.py は HTML キャッシュからの再パースだが、
キャッシュ自体が 3 頭バグの影響を受けているため、
netkeiba から直接再取得する方式に切り替え。
"""
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import RESULTS_DIR


def find_broken_results():
    """壊れた results.json エントリを検出"""
    broken = []
    for fname in sorted(os.listdir(RESULTS_DIR)):
        if not fname.endswith("_results.json"):
            continue
        fpath = os.path.join(RESULTS_DIR, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
        dt = fname.replace("_results.json", "")
        for race_id, result in data.items():
            order = result.get("order", [])
            if len(order) <= 3 and len(order) > 0:
                max_hn = max(
                    (int(o.get("horse_no", 0)) for o in order), default=0
                )
                if max_hn > 3:
                    broken.append({
                        "file": fpath,
                        "date": dt,
                        "race_id": race_id,
                        "order_count": len(order),
                    })
    return broken


def fix_broken():
    broken = find_broken_results()
    if not broken:
        print("✅ 壊れた results なし")
        return

    print(f"壊れた results: {len(broken)}件 — netkeiba から再取得開始")

    from src.results_tracker import fetch_single_race_result
    from src.scraper.netkeiba import NetkeibaClient

    client = NetkeibaClient()

    fixed = 0
    failed = 0
    # ファイル別にグループ化して一括更新
    by_file = {}
    for b in broken:
        by_file.setdefault(b["file"], []).append(b)

    total = len(broken)
    done = 0

    for fpath, entries in sorted(by_file.items()):
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        changed = False
        for entry in entries:
            race_id = entry["race_id"]
            date_str = entry["date"]
            date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            try:
                # 壊れたエントリの payouts を退避してからエントリ削除
                # (fetch が "既存完全" と判断して再取得スキップするのを防ぐ)
                old_payouts = data.get(race_id, {}).get("payouts", {})
                if race_id in data:
                    del data[race_id]
                    changed = True

                # HTMLキャッシュも壊れている可能性 → 削除して強制再取得
                import lz4.frame as _lz4
                for _prefix in ["race.netkeiba.com", "nar.netkeiba.com"]:
                    _cp = f"data/cache/{_prefix}_race_result.html_race_id={race_id}.html.lz4"
                    if os.path.exists(_cp):
                        os.remove(_cp)

                result = fetch_single_race_result(
                    date=date_fmt,
                    race_id=race_id,
                    client=client,
                )
                if result and len(result.get("order", [])) > 3:
                    data[race_id] = result
                    if not result.get("payouts") and old_payouts:
                        data[race_id]["payouts"] = old_payouts
                    changed = True
                    fixed += 1
                else:
                    new_cnt = len(result.get("order", [])) if result else 0
                    failed += 1
                    print(f"  ⚠ {date_str} {race_id}: 再取得 {new_cnt}頭 (改善なし)")
            except Exception as e:
                failed += 1
                print(f"  ❌ {date_str} {race_id}: {e}")

            done += 1
            if done % 10 == 0 or done == total:
                pct = done / total * 100
                bar_len = 30
                filled = int(bar_len * done / total)
                bar = "█" * filled + "░" * (bar_len - filled)
                print(f"  [{bar}] {pct:5.1f}% ({done}/{total}) fixed={fixed} failed={failed}")

            time.sleep(2.5)  # netkeiba rate limit

        if changed:
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    print()
    print(f"=== 完了: fixed={fixed}, failed={failed}, total={total} ===")


if __name__ == "__main__":
    fix_broken()
