# -*- coding: utf-8 -*-
"""time/last_3f が欠落した NAR レースを公式(keiba.go.jp)から再取得して補完する。

マスター指摘(2026-06-21): レースによって着差・後半3Fタイム・走破タイムが無い。
真因: NAR レース直後の速報取得時は keiba.go.jp にタイム/上がり3F が未掲載で
      time_sec=0.0 / last_3f=0.0 で保存される。確定後は掲載されるが、payouts が
      完整なため fetch_actual_results のキャッシュ再fetch がトリガーされず欠落が残る。
対処: order の半数以上が time_sec 欠落の NAR レースを OfficialNARScraper.get_result で
      再取得し、time/last_3f/margin/corners を含む order に上書きする。JRA は結果ページに
      タイム列があり欠落しないため対象外。

使い方:
  python scripts/refetch_incomplete_nar_results.py --date 20260621
  python scripts/refetch_incomplete_nar_results.py --date 20260621 --dry-run
"""
import argparse
import json
import os
import shutil
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.masters.venue_master import JRA_CODES
from src.scraper.official_nar import OfficialNARScraper


def _is_incomplete(order: list) -> bool:
    """order の半数以上で time_sec が欠落していれば不完全とみなす。"""
    if not order:
        return False
    n_zero = sum(1 for o in order if not o.get("time_sec"))
    return n_zero > len(order) / 2


def refetch(date: str, dry_run: bool = False) -> dict:
    res_fp = os.path.join("data", "results", f"{date}_results.json")
    if not os.path.isfile(res_fp):
        print(f"results.json なし: {res_fp}")
        return {"fixed": 0}
    with open(res_fp, "r", encoding="utf-8") as f:
        res = json.load(f)

    date_hyphen = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
    targets = [
        rid for rid, r in res.items()
        if isinstance(r, dict) and rid[4:6] not in JRA_CODES
        and _is_incomplete(r.get("order", []))
    ]
    print(f"再取得対象(NAR・time欠落): {len(targets)}レース {targets}")
    if dry_run or not targets:
        return {"fixed": 0, "targets": len(targets)}

    nar = OfficialNARScraper()
    fixed = 0
    for i, rid in enumerate(targets, 1):
        print(f"  [{i}/{len(targets)}] {rid} 再取得中...", end=" ")
        try:
            result = nar.get_result(rid, date_hyphen)
        except Exception as e:
            print(f"ERROR {e}")
            continue
        new_order = (result or {}).get("order", [])
        if new_order and any(o.get("time_sec") for o in new_order):
            res[rid]["order"] = new_order
            if result.get("payouts"):
                res[rid]["payouts"] = result["payouts"]
            fixed += 1
            print(f"OK (time有 {sum(1 for o in new_order if o.get('time_sec'))}頭)")
        else:
            print("まだタイム未掲載")
        time.sleep(2.0)  # NAR レート制限

    if fixed:
        bak = res_fp + ".bak_refetch"
        shutil.copy(res_fp, bak)
        print(f"backup -> {bak}")
        with open(res_fp, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)
        print(f"保存完了: {res_fp}")
    return {"fixed": fixed, "targets": len(targets)}


def main():
    ap = argparse.ArgumentParser(description="time欠落NARレースを公式再取得")
    ap.add_argument("--date", required=True, help="対象日 (例: 20260621)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    print(f"=== NAR time欠落レース 再取得 date={args.date} ===")
    stats = refetch(args.date, dry_run=args.dry_run)
    print(f"完了: 補完={stats.get('fixed', 0)} / 対象={stats.get('targets', 0)}")


if __name__ == "__main__":
    main()
