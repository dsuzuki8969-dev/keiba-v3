# -*- coding: utf-8 -*-
"""
2026年の旧予想データをDB・JSONから完全削除するスクリプト

batch_history.py 完了後に実行することで、旧システムで生成した
2026年分の予想を全て消去し、新システムのデータのみにする。

使い方:
  python tools/reset_2026_predictions.py          # 2026年全て削除
  python tools/reset_2026_predictions.py --year 2025  # 2025年を削除（年指定可）
  python tools/reset_2026_predictions.py --dry-run    # 削除せず確認のみ
"""

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DATABASE_PATH, PREDICTIONS_DIR, RESULTS_DIR


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", default="2026", help="削除対象年 (default: 2026)")
    parser.add_argument("--dry-run", action="store_true", help="確認のみ（削除しない）")
    args = parser.parse_args()

    year = args.year
    dry = args.dry_run
    prefix = f"{year}-"      # DB の date 列フィルタ
    file_prefix = year       # JSON ファイル名フィルタ

    print(f"\n{'='*55}")
    print(f"  {year}年 旧予想データ {'[DRY-RUN] ' if dry else ''}削除")
    print(f"{'='*55}")

    # ── DB の現状確認 ────────────────────────────────────────────────
    con = sqlite3.connect(DATABASE_PATH)
    tables = {
        "predictions":  f"SELECT COUNT(*) FROM predictions  WHERE date LIKE '{prefix}%'",
        "race_results": f"SELECT COUNT(*) FROM race_results WHERE date LIKE '{prefix}%'",
        "match_results":f"SELECT COUNT(*) FROM match_results WHERE date LIKE '{prefix}%'",
    }
    counts = {}
    for tbl, sql in tables.items():
        try:
            counts[tbl] = con.execute(sql).fetchone()[0]
        except Exception:
            counts[tbl] = 0

    print(f"\n■ DB レコード ({year}年)")
    total_db = 0
    for tbl, cnt in counts.items():
        print(f"  {tbl:<20}: {cnt:>5} 件")
        total_db += cnt

    # ── JSON ファイルの確認 ─────────────────────────────────────────
    pred_files = sorted(
        Path(PREDICTIONS_DIR).glob(f"{file_prefix}*_pred.json")
    ) if Path(PREDICTIONS_DIR).exists() else []
    result_files = sorted(
        Path(RESULTS_DIR).glob(f"{file_prefix}*_results.json")
    ) if Path(RESULTS_DIR).exists() else []
    summary_files = sorted(
        Path(PREDICTIONS_DIR).glob(f"{file_prefix}*_summary.txt")
    ) if Path(PREDICTIONS_DIR).exists() else []

    all_files = pred_files + result_files + summary_files
    files_size = sum(f.stat().st_size for f in all_files) / 1024

    print(f"\n■ JSON/テキストファイル ({year}年)")
    print(f"  予想 JSON    : {len(pred_files):>3} ファイル")
    if pred_files:
        for f in pred_files[:5]:
            print(f"    {f.name}")
        if len(pred_files) > 5:
            print(f"    ... 他 {len(pred_files)-5} ファイル")
    print(f"  結果 JSON    : {len(result_files):>3} ファイル")
    print(f"  サマリー TXT : {len(summary_files):>3} ファイル")
    print(f"  合計サイズ   : {files_size:.0f} KB")

    print(f"\n■ 削除対象合計")
    print(f"  DB レコード  : {total_db} 件")
    print(f"  ファイル     : {len(all_files)} 個  ({files_size:.0f} KB)")

    if dry:
        print("\n[DRY-RUN] 実際には削除しません。--dry-run なしで再実行してください。")
        con.close()
        return

    # ── 確認プロンプト ──────────────────────────────────────────────
    print(f"\n{year}年の全予想データを削除します。")
    print("batch_history.py 完了後に実行してください。")
    ans = input("続行しますか？ [y/N]: ").strip().lower()
    if ans != "y":
        print("キャンセルしました。")
        con.close()
        return

    t0 = time.time()

    # ── DB 削除 ────────────────────────────────────────────────────
    print("\n■ DB削除中...")
    deleted_db = {}
    for tbl in tables:
        sql = f"DELETE FROM {tbl} WHERE date LIKE '{prefix}%'"
        cur = con.execute(sql)
        deleted_db[tbl] = cur.rowcount
        print(f"  {tbl:<20}: {cur.rowcount:>5} 件削除")
    con.commit()
    con.close()

    # ── ファイル削除 ────────────────────────────────────────────────
    print("\n■ ファイル削除中...")
    deleted_files = 0
    for f in all_files:
        try:
            f.unlink()
            deleted_files += 1
        except Exception as e:
            print(f"  スキップ: {f.name} ({e})")
    print(f"  {deleted_files} ファイル削除完了")

    elapsed = time.time() - t0
    print(f"\n{'='*55}")
    print(f"  完了 ({elapsed:.1f}秒)")
    print(f"  DB {sum(deleted_db.values())} 件 + {deleted_files} ファイル 削除")
    print(f"\n次のステップ:")
    print(f"  python tools/batch_history.py --from {year}-01-01 --to {year}-12-31")
    print(f"  （または全期間: python tools/batch_history.py）")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
