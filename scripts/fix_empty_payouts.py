# -*- coding: utf-8 -*-
"""payouts空のレースをnetkeibaから補完するスクリプト

results.json で着順(order)はあるが payouts が空のレースを検出し、
netkeiba の結果ページから payouts を再取得して補完する。

使い方:
  python scripts/fix_empty_payouts.py                    # 全日程チェック
  python scripts/fix_empty_payouts.py --date 20260516    # 特定日のみ
  python scripts/fix_empty_payouts.py --dry-run           # 対象一覧のみ
"""
import json
import os
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

RESULTS_DIR = Path("data/results")


def find_empty_payouts(date_filter: str = "") -> list:
    """payoutsが空のレースを検出"""
    targets = []
    for fp in sorted(RESULTS_DIR.glob("*_results.json")):
        if date_filter and date_filter not in fp.name:
            continue
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        for rid, race in data.items():
            order = race.get("order", [])
            if not order:
                continue  # 着順なし = レース未確定
            payouts = race.get("payouts", {})
            if not payouts or not any(v for v in payouts.values() if v):
                targets.append((fp.name, rid))
    return targets


def fix_payouts(targets: list, dry_run: bool = False) -> dict:
    """JRA公式 / netkeibaからpayoutsを補完

    JRA公式(jra.go.jp)を1stソースとし、失敗時はnetkeiba fallback。
    netkeibaがcooldown中でもJRA公式は独立してアクセス可能。
    """
    if dry_run:
        print(f"対象: {len(targets)}レース (dry-run)")
        by_date = {}
        for fname, rid in targets:
            by_date.setdefault(fname, []).append(rid)
        for fname, rids in by_date.items():
            print(f"  {fname}: {len(rids)}R")
        return {"fixed": 0, "failed": 0}

    from data.masters.venue_master import JRA_CODES
    from src.scraper.netkeiba import NetkeibaClient
    from src.scraper.official_odds import OfficialOddsScraper

    client = NetkeibaClient(no_cache=True)
    official = OfficialOddsScraper()

    # ファイル単位でグルーピング
    by_file = {}
    for fname, rid in targets:
        by_file.setdefault(fname, []).append(rid)

    stats = {"fixed": 0, "failed": 0, "skipped": 0}
    total = len(targets)
    done = 0

    for fname, rids in sorted(by_file.items()):
        fp = RESULTS_DIR / fname
        data = json.loads(fp.read_text(encoding="utf-8"))
        modified = False

        for rid in rids:
            done += 1
            pct = done / total * 100
            print(f"  [{done}/{total}] {pct:.0f}% {fname} {rid}", end=" ")

            race = data.get(rid, {})
            if not race.get("order"):
                print("SKIP(order無)")
                stats["skipped"] += 1
                continue

            vc = rid[4:6]
            is_jra = vc in JRA_CODES
            payouts = None

            # 1st: JRA公式（netkeibaのcooldownとは独立）
            if is_jra:
                try:
                    result = official.get_jra_result(rid)
                    if result and result.get("payouts"):
                        p = result["payouts"]
                        if any(v for v in p.values() if v):
                            payouts = p
                except Exception as e:
                    pass

            # 2nd: netkeiba fallback
            if not payouts:
                base_url = "https://race.netkeiba.com" if is_jra else "https://nar.netkeiba.com"
                url = f"{base_url}/race/result.html"
                try:
                    soup = client.get(url, params={"race_id": rid})
                    if soup:
                        from src.results_tracker import _parse_payouts
                        payouts = _parse_payouts(soup)
                        if not (payouts and any(v for v in payouts.values() if v)):
                            payouts = None
                    time.sleep(2.0)
                except Exception:
                    pass

            if payouts:
                race["payouts"] = payouts
                modified = True
                stats["fixed"] += 1
                print(f"OK ({len(payouts)}券種)")
            else:
                print("FAIL")
                stats["failed"] += 1

        if modified:
            fp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  -> {fname} 保存完了")

    return stats


def main():
    date_filter = ""
    dry_run = False

    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--date" and i < len(sys.argv) - 1:
            date_filter = sys.argv[i + 1]
        if arg == "--dry-run":
            dry_run = True

    print("=== payouts空レース補完 ===")
    targets = find_empty_payouts(date_filter)
    print(f"対象: {len(targets)}レース")

    if not targets:
        print("補完対象なし")
        return

    stats = fix_payouts(targets, dry_run=dry_run)
    print(f"\n完了: 修正={stats['fixed']} 失敗={stats['failed']}")


if __name__ == "__main__":
    main()
