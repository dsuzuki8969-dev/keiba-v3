"""三連複ペイアウト欠損レースを netkeiba から再取得して results.json に補完

NAR 公式は三連複を返さないため、netkeiba 経由で取得する。
既存 results.json の payouts にマージ（既存データは上書きしない）。

使用方法:
  python scripts/backfill_sanrenpuku_netkeiba.py --start 2024-11-01 --end 2024-11-30 --dry-run
  python scripts/backfill_sanrenpuku_netkeiba.py --start 2024-11-01 --end 2024-11-30
  python scripts/backfill_sanrenpuku_netkeiba.py --start 2024-11-01 --end 2024-11-30 --max-fetch 10
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, PROJECT_ROOT)
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import RESULTS_DIR

# 三連複欠損レースを検出
def find_missing_sanrenpuku(start: str, end: str):
    """results.json から三連複ペイアウトがないレースを抽出"""
    missing = []
    start_d = start.replace("-", "")
    end_d = end.replace("-", "")

    res_dir = Path(RESULTS_DIR)
    for fp in sorted(res_dir.glob("*_results.json")):
        date_str = fp.stem.split("_")[0]
        if len(date_str) != 8:
            continue
        if date_str < start_d or date_str > end_d:
            continue

        with open(fp, encoding="utf-8") as f:
            results = json.load(f)

        for race_id, rdata in results.items():
            payouts = rdata.get("payouts", {})
            if not payouts:
                continue
            # 三連複があるかチェック
            has_sanren = any(k in ("三連複", "sanrenpuku", "Fuku3") for k in payouts)
            if not has_sanren:
                missing.append({
                    "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
                    "date_raw": date_str,
                    "race_id": race_id,
                    "existing_keys": list(payouts.keys()),
                })
    return missing


def main():
    parser = argparse.ArgumentParser(description="三連複ペイアウト netkeiba 再取得")
    parser.add_argument("--start", required=True, help="開始日 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="終了日 YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-fetch", type=int, default=0, help="最大取得数 (0=無制限)")
    parser.add_argument("--rate", type=float, default=2.5, help="リクエスト間隔 (秒)")
    args = parser.parse_args()

    missing = find_missing_sanrenpuku(args.start, args.end)
    is_nar = lambda rid: int(rid[4:6]) > 10
    nar_cnt = sum(1 for m in missing if is_nar(m["race_id"]))
    jra_cnt = len(missing) - nar_cnt

    print(f"三連複欠損: {len(missing)} レース ({args.start} ~ {args.end})")
    print(f"  JRA: {jra_cnt}, NAR: {nar_cnt}")
    est_min = len(missing) * args.rate / 60
    print(f"  推定所要時間: {est_min:.0f} 分 ({args.rate}秒/件)")

    if args.dry_run:
        return

    # netkeiba クライアント + パーサー初期化
    from src.scraper.auth import NetkeibaClient
    from src.results_tracker import _parse_payouts
    from data.masters.venue_master import JRA_CODES

    client = NetkeibaClient()

    limit = args.max_fetch if args.max_fetch > 0 else len(missing)
    targets = missing[:limit]

    updated = 0
    no_sanren = 0
    failed = 0
    dates_updated = set()

    for i, m in enumerate(targets):
        race_id = m["race_id"]
        date_raw = m["date_raw"]

        try:
            vc = race_id[4:6]
            base_url = "https://race.netkeiba.com" if vc in JRA_CODES else "https://nar.netkeiba.com"
            url = f"{base_url}/race/result.html"
            soup = client.get(url, params={"race_id": race_id})

            if soup:
                new_payouts = _parse_payouts(soup)
                has_sanren = any(k in ("三連複", "sanrenpuku") for k in new_payouts)

                if has_sanren:
                    # results.json にマージ
                    fpath = os.path.join(RESULTS_DIR, f"{date_raw}_results.json")
                    with open(fpath, encoding="utf-8") as f:
                        results = json.load(f)

                    if race_id in results:
                        old_payouts = results[race_id].get("payouts", {})
                        # 新しい三連複データを追加（既存キーは上書きしない）
                        for k, v in new_payouts.items():
                            if k not in old_payouts:
                                old_payouts[k] = v
                        results[race_id]["payouts"] = old_payouts

                        with open(fpath, "w", encoding="utf-8") as f:
                            json.dump(results, f, ensure_ascii=False, separators=(",", ":"))

                        updated += 1
                        dates_updated.add(date_raw)
                else:
                    no_sanren += 1
            else:
                failed += 1

        except Exception as e:
            if i < 5:
                print(f"  ERROR {race_id}: {type(e).__name__}: {e}")
            failed += 1

        if (i + 1) % 50 == 0 or i == len(targets) - 1:
            pct = (i + 1) / len(targets) * 100
            print(f"  [{i+1}/{len(targets)}] {pct:.0f}% — "
                  f"更新={updated:,}, 三連複なし={no_sanren:,}, 失敗={failed:,}",
                  flush=True)

        time.sleep(args.rate)

    print(f"\n完了: 更新={updated:,}, 三連複なし={no_sanren:,}, 失敗={failed:,}")
    print(f"日付更新: {len(dates_updated)}")


if __name__ == "__main__":
    main()
