#!/usr/bin/env python
"""
バックフィル: race_log の win_odds バグ修復

バグの概要:
  旧スクレイパーが race_results.order_json の 'odds' フィールドに
  単勝オッズではなく人気順位（整数）を誤って保存していた。
  その結果、race_log.win_odds に人気順位が入っている。

修復方法:
  1. win_odds != tansho_odds かつ win_odds == popularity (バグパターン)
     → win_odds = tansho_odds で上書き
  2. win_odds が NULL かつ tansho_odds が存在する
     → win_odds = tansho_odds で補完

バグ検出条件:
  ABS(win_odds - popularity) < 0.01  AND  ABS(win_odds - tansho_odds) >= 0.2

実行例:
  python scripts/backfill_race_log_odds_from_results.py --dry-run
  python scripts/backfill_race_log_odds_from_results.py
  python scripts/backfill_race_log_odds_from_results.py --since 2025-01-01
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

# プロジェクトルートをパスに追加
_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root))

DB_PATH = _root / "data" / "keiba.db"


def run_backfill(dry_run: bool = False, since: str = None) -> dict:
    """バックフィル実行

    Args:
        dry_run: True の場合、件数集計のみ（DB更新なし）
        since: 開始日 YYYY-MM-DD (指定時はその日以降のレースのみ対象)

    Returns:
        {"fixed_bug": int, "fixed_null": int, "skipped": int, "total_checked": int}
    """
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    date_filter = ""
    date_params = []
    if since:
        date_filter = " AND race_date >= ?"
        date_params = [since]

    # ── Case 1: win_odds にバグ値（人気順位）が入っているケース ──
    # パターン A: win_odds == popularity (完全一致)
    # パターン B: win_odds が整数値で tansho_odds と大きく乖離 (別馬の人気順位が混入)
    # 条件: (ABS(win_odds - popularity) < 0.01 OR win_odds=整数) AND ABS(win_odds - tansho_odds) >= 2.0

    bug_query = f"""
        SELECT id, race_id, race_date, horse_no, win_odds, tansho_odds, popularity
        FROM race_log
        WHERE win_odds IS NOT NULL
          AND tansho_odds IS NOT NULL
          AND tansho_odds > 0
          AND ABS(win_odds - tansho_odds) >= 2.0
          AND (
            -- パターンA: win_odds が popularity と完全一致（人気順位がそのまま入っている）
            (popularity IS NOT NULL AND ABS(win_odds - popularity) < 0.01)
            OR
            -- パターンB: win_odds が整数値（X.0形式）で tansho_odds と大きく乖離
            (win_odds = CAST(win_odds AS INTEGER) AND win_odds >= 1.0 AND win_odds <= 18.0)
          )
          {date_filter}
    """
    bug_rows = cur.execute(bug_query, date_params).fetchall()
    fixed_bug = len(bug_rows)

    # ── Case 2: win_odds が NULL で tansho_odds がある ──
    null_query = f"""
        SELECT id, race_id, race_date, horse_no, tansho_odds
        FROM race_log
        WHERE win_odds IS NULL
          AND tansho_odds IS NOT NULL
          AND tansho_odds > 0
          {date_filter}
    """
    null_rows = cur.execute(null_query, date_params).fetchall()
    fixed_null = len(null_rows)

    print(f"[バックフィル集計]")
    print(f"  Case 1 (バグ修復: win_odds=popularity → tansho_odds): {fixed_bug:,}件")
    print(f"  Case 2 (NULL補完: win_odds=NULL → tansho_odds):       {fixed_null:,}件")
    print(f"  合計:                                                   {fixed_bug + fixed_null:,}件")

    if dry_run:
        print("\n[dry-run モード] DB は更新しません")
        conn.close()
        return {
            "fixed_bug": fixed_bug,
            "fixed_null": fixed_null,
            "skipped": 0,
            "total_checked": fixed_bug + fixed_null,
        }

    # ── DB 更新 ──
    print("\n[DB更新開始]")
    start = time.time()

    batch_size = 10000
    updated_bug = 0
    updated_null = 0

    # Case 1: バグ修復バッチ
    batch_ids = []
    for i, row in enumerate(bug_rows):
        batch_ids.append((row["tansho_odds"], row["id"]))
        if len(batch_ids) >= batch_size:
            conn.executemany(
                "UPDATE race_log SET win_odds = ? WHERE id = ?",
                batch_ids,
            )
            conn.commit()
            updated_bug += len(batch_ids)
            elapsed = time.time() - start
            pct = updated_bug / fixed_bug * 100 if fixed_bug else 100
            print(
                f"  [{'#' * int(pct/5)}{'-' * (20 - int(pct/5))}] {pct:.1f}%"
                f" Case1: {updated_bug:,}/{fixed_bug:,}件 ({elapsed:.1f}s)",
                flush=True,
            )
            batch_ids = []

    if batch_ids:
        conn.executemany(
            "UPDATE race_log SET win_odds = ? WHERE id = ?",
            batch_ids,
        )
        conn.commit()
        updated_bug += len(batch_ids)

    print(f"  Case 1 完了: {updated_bug:,}件修復")

    # Case 2: NULL補完バッチ
    batch_ids = []
    for i, row in enumerate(null_rows):
        batch_ids.append((row["tansho_odds"], row["id"]))
        if len(batch_ids) >= batch_size:
            conn.executemany(
                "UPDATE race_log SET win_odds = ? WHERE id = ?",
                batch_ids,
            )
            conn.commit()
            updated_null += len(batch_ids)
            pct = updated_null / fixed_null * 100 if fixed_null else 100
            print(
                f"  [{'#' * int(pct/5)}{'-' * (20 - int(pct/5))}] {pct:.1f}%"
                f" Case2: {updated_null:,}/{fixed_null:,}件",
                flush=True,
            )
            batch_ids = []

    if batch_ids:
        conn.executemany(
            "UPDATE race_log SET win_odds = ? WHERE id = ?",
            batch_ids,
        )
        conn.commit()
        updated_null += len(batch_ids)

    print(f"  Case 2 完了: {updated_null:,}件補完")

    elapsed = time.time() - start
    print(f"\n[バックフィル完了] 合計 {updated_bug + updated_null:,}件更新 ({elapsed:.1f}s)")

    conn.close()
    return {
        "fixed_bug": updated_bug,
        "fixed_null": updated_null,
        "skipped": 0,
        "total_checked": fixed_bug + fixed_null,
    }


def verify_result(since: str = None) -> None:
    """バックフィル後の検証"""
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    date_filter = ""
    date_params = []
    if since:
        date_filter = " AND race_date >= ?"
        date_params = [since]

    # バグが残っている件数
    cur.execute(f"""
        SELECT COUNT(*) FROM race_log
        WHERE win_odds IS NOT NULL AND popularity IS NOT NULL AND tansho_odds IS NOT NULL
        AND ABS(win_odds - popularity) < 0.01
        AND ABS(win_odds - tansho_odds) >= 0.2
        {date_filter}
    """, date_params)
    remaining_bug = cur.fetchone()[0]

    # win_odds NULL 件数
    cur.execute(f"""
        SELECT COUNT(*) FROM race_log
        WHERE win_odds IS NULL {date_filter}
    """, date_params)
    null_count = cur.fetchone()[0]

    # 正常件数
    cur.execute(f"""
        SELECT COUNT(*) FROM race_log
        WHERE win_odds IS NOT NULL AND win_odds > 0 {date_filter}
    """, date_params)
    normal_count = cur.fetchone()[0]

    print("\n[バックフィル後の検証]")
    print(f"  残バグ件数 (win_odds==popularity bug):  {remaining_bug:,}件")
    print(f"  win_odds NULL:                         {null_count:,}件")
    print(f"  win_odds 正常 (>0):                    {normal_count:,}件")

    if remaining_bug > 0:
        print(f"\n  [WARNING] 残バグあり: {remaining_bug:,}件 -> 追加確認が必要")
    else:
        print("\n  [OK] バグ解消確認")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="race_log の win_odds バグ修復バックフィル"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="件数集計のみ、DB は更新しない",
    )
    parser.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        help="開始日 (指定時はその日以降のみ対象)",
    )
    args = parser.parse_args()

    print(f"=== race_log win_odds バックフィル ===")
    print(f"DB: {DB_PATH}")
    if args.since:
        print(f"対象: {args.since} 以降")
    else:
        print(f"対象: 全期間")
    print(f"dry-run: {args.dry_run}")
    print()

    result = run_backfill(dry_run=args.dry_run, since=args.since)

    if not args.dry_run:
        verify_result(since=args.since)

    return 0 if result["fixed_bug"] >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
