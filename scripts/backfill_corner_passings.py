"""過去results.json全日付のコーナー通過順を一括補完するスクリプト

処理内容:
  1. data/results/YYYYMMDD_results.json を全走査
  2. 各レースで order[*].corners が空の場合、
     netkeiba レース結果ページ下部の通過順テーブルをスクレイピング
  3. 共通パーサ parse_corner_passing_from_text で馬番→通過順位マップを構築
  4. 既存 results.json に上書き保存（既存非corners値は保持）

使い方:
  python scripts/backfill_corner_passings.py            # 全日付処理
  python scripts/backfill_corner_passings.py 2026-04-17 # 特定日付のみ
  python scripts/backfill_corner_passings.py --dry-run  # 確認のみ
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scraper.netkeiba import NetkeibaClient
from src.scraper.official_nar import parse_corner_passing_from_text
from data.masters.venue_master import JRA_CODES

RESULTS_DIR = Path(__file__).resolve().parent.parent / "data" / "results"


def backfill_date(date_key: str, client: NetkeibaClient, dry_run: bool = False) -> dict:
    """特定日付のresults.jsonに対してcornersを補完

    Args:
        date_key: YYYYMMDD形式
        client: NetkeibaClient
        dry_run: Trueなら書き込みせず統計だけ返す
    Returns:
        {"total": レース数, "need_backfill": 要補完数, "merged": マージ頭数, "failed": 失敗数}
    """
    fpath = RESULTS_DIR / f"{date_key}_results.json"
    if not fpath.exists():
        return {"total": 0, "need_backfill": 0, "merged": 0, "failed": 0}

    with open(fpath, "r", encoding="utf-8") as f:
        results = json.load(f)

    if not isinstance(results, dict):
        return {"total": 0, "need_backfill": 0, "merged": 0, "failed": 0}

    total = 0
    need_backfill = 0
    total_merged = 0
    failed = 0
    modified = False

    for race_id, entry in results.items():
        if not isinstance(entry, dict):
            continue
        order = entry.get("order") or []
        if not order:
            continue
        total += 1

        # 全馬cornersが埋まっているか確認
        filled = sum(1 for o in order if o.get("corners"))
        if filled == len(order):
            continue  # 全員埋まっている → スキップ

        need_backfill += 1

        # netkeibaレース結果ページから通過順を取得
        vc = race_id[4:6]
        base_url = (
            "https://race.netkeiba.com" if vc in JRA_CODES
            else "https://nar.netkeiba.com"
        )
        url = f"{base_url}/race/result.html"
        try:
            soup = client.get(url, params={"race_id": race_id})
            if not soup:
                failed += 1
                continue
            full_text = soup.get_text()
            corners_map = parse_corner_passing_from_text(full_text)
            if not corners_map:
                failed += 1
                continue

            race_merged = 0
            for o in order:
                if o.get("corners"):
                    continue
                hno = o.get("horse_no")
                if hno is None:
                    continue
                new_c = corners_map.get(hno)
                if new_c:
                    o["corners"] = new_c
                    race_merged += 1

            if race_merged > 0:
                total_merged += race_merged
                modified = True
                print(f"    {race_id}: {race_merged}頭分 corners補完")
            else:
                failed += 1

            # レート制限（netkeiba）
            time.sleep(1.5)

        except Exception as e:
            failed += 1
            print(f"    {race_id}: エラー {e}")
            continue

    # 保存
    if modified and not dry_run:
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

    return {
        "total": total,
        "need_backfill": need_backfill,
        "merged": total_merged,
        "failed": failed,
    }


def find_all_date_keys() -> list:
    """data/results/ 下の全日付キーを取得（新しい順）"""
    keys = []
    for f in RESULTS_DIR.glob("*_results.json"):
        name = f.stem  # YYYYMMDD_results
        date_key = name.replace("_results", "")
        if len(date_key) == 8 and date_key.isdigit():
            keys.append(date_key)
    # 新しい順に処理（最新から補完）
    keys.sort(reverse=True)
    return keys


def main():
    parser = argparse.ArgumentParser(description="過去results.jsonコーナー通過順一括補完")
    parser.add_argument("date", nargs="?", default=None,
                        help="対象日付 YYYY-MM-DD or YYYYMMDD（省略時は全日付）")
    parser.add_argument("--dry-run", action="store_true",
                        help="書き込みせず統計のみ表示")
    parser.add_argument("--limit", type=int, default=None,
                        help="先頭N日付のみ処理（テスト用）")
    parser.add_argument("--from", dest="from_date", default=None,
                        help="この日付以降のみ処理 YYYY-MM-DD or YYYYMMDD")
    args = parser.parse_args()

    client = NetkeibaClient(no_cache=True)

    if args.date:
        date_key = args.date.replace("-", "")
        date_keys = [date_key]
    else:
        date_keys = find_all_date_keys()
        if args.from_date:
            threshold = args.from_date.replace("-", "")
            date_keys = [d for d in date_keys if d >= threshold]
        if args.limit:
            date_keys = date_keys[:args.limit]

    print(f"=== backfill_corner_passings 開始 {len(date_keys)}日付 ===")
    if args.dry_run:
        print("[DRY-RUN モード: 書き込みなし]")

    start_time = datetime.now()
    grand_total = {"total": 0, "need_backfill": 0, "merged": 0, "failed": 0}

    for i, dk in enumerate(date_keys, 1):
        elapsed = (datetime.now() - start_time).total_seconds()
        pct = i / len(date_keys) * 100
        remaining = (elapsed / i * (len(date_keys) - i)) if i > 0 else 0
        print(f"\n[{i}/{len(date_keys)}] ({pct:.1f}%, 経過{elapsed:.0f}秒, 残り{remaining:.0f}秒) {dk}")

        stats = backfill_date(dk, client, dry_run=args.dry_run)
        print(f"  → total={stats['total']} need_backfill={stats['need_backfill']} "
              f"merged={stats['merged']}頭 failed={stats['failed']}")
        for k in grand_total:
            grand_total[k] += stats[k]

    total_elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n=== 完了（所要時間 {total_elapsed:.0f}秒 = {total_elapsed/60:.1f}分）===")
    print(f"  総レース数   : {grand_total['total']}")
    print(f"  要補完       : {grand_total['need_backfill']}")
    print(f"  補完頭数合計 : {grand_total['merged']}")
    print(f"  失敗         : {grand_total['failed']}")


if __name__ == "__main__":
    main()
