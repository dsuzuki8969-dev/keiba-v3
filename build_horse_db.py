"""
馬別過去走DB構築スクリプト
キャッシュ済みの result.html (75,000件以上) から horse_db.json を構築する。
新規 HTTP リクエスト不要 — 全てキャッシュから再パース。

使い方:
  python build_horse_db.py           # 増分モード（既存データ保持）
  python build_horse_db.py --full    # 全件再構築
  python build_horse_db.py --check   # 統計のみ表示
"""

import sys
import io
import os
import json
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

_HERE = os.path.dirname(os.path.abspath(__file__))

CACHE_DIR   = os.path.join(_HERE, 'data', 'cache')
OUTPUT_PATH = os.path.join(_HERE, 'data', 'horse_db.json')


def check_stats():
    """既存 horse_db.json の統計を表示"""
    if not os.path.exists(OUTPUT_PATH):
        print("horse_db.json が見つかりません。build_horse_db.py を実行してください。")
        return
    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    with open(OUTPUT_PATH, 'r', encoding='utf-8') as f:
        db = json.load(f)
    total_runs = sum(len(v.get('runs', [])) for v in db.values())
    print(f"=== horse_db.json 統計 ===")
    print(f"  馬数    : {len(db):,}頭")
    print(f"  総走数  : {total_runs:,}走")
    print(f"  ファイル: {size_mb:.1f} MB")
    # サンプル
    sample_ids = list(db.keys())[:3]
    for hid in sample_ids:
        entry = db[hid]
        runs = entry.get('runs', [])
        latest = runs[0].get('race_date', '') if runs else ''
        oldest = runs[-1].get('race_date', '') if runs else ''
        print(f"  {hid}: {entry['horse_name']} {len(runs)}走 ({oldest}〜{latest})")


def main():
    args = sys.argv[1:]
    full_mode = '--full' in args
    check_mode = '--check' in args

    if check_mode:
        check_stats()
        return

    if full_mode:
        print("=== 全件再構築モード ===")
        incremental = False
    else:
        print("=== 増分モード（既存データ保持）===")
        incremental = True

    if not os.path.isdir(CACHE_DIR):
        print(f"キャッシュディレクトリが見つかりません: {CACHE_DIR}")
        sys.exit(1)

    start = time.time()
    from src.scraper.horse_db_builder import build_horse_db_from_cache

    processed, total_runs = build_horse_db_from_cache(
        cache_dir=CACHE_DIR,
        output_path=OUTPUT_PATH,
        progress_every=2000,
        incremental=incremental,
    )

    elapsed = time.time() - start
    print(f"\n完了: {elapsed:.0f}秒 / {processed}レース / {total_runs:,}走")
    print(f"保存先: {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
