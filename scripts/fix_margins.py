#!/usr/bin/env python3
"""
race_log テーブルの margin_ahead / margin_behind を finish_time_sec から再計算するスクリプト。

margin_ahead = 当該馬の finish_time_sec - 同レース1着馬の finish_time_sec
margin_behind = 次着馬の finish_time_sec - 当該馬の finish_time_sec (最下位は0)

修復対象: margin_ahead=0 AND finish_pos > 1 AND finish_time_sec > 0 のレコード
"""

import sys
import os
import argparse
import sqlite3
import time

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# プロジェクトルートをパスに追加
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, "data", "keiba.db")


def progress_bar(current, total, width=40, prefix=""):
    """プログレスバーを表示"""
    pct = current / total if total > 0 else 0
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    sys.stdout.write(f"\r{prefix}[{bar}] {pct*100:.1f}% ({current:,}/{total:,})")
    sys.stdout.flush()


def fix_margins(db_path: str, dry_run: bool = False):
    """margin_ahead / margin_behind を finish_time_sec から再計算"""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    cur = conn.cursor()

    # ── 修復前カウント ──
    cur.execute("""
        SELECT COUNT(*) FROM race_log
        WHERE margin_ahead = 0 AND finish_pos > 1 AND finish_time_sec > 0
    """)
    ahead_count = cur.fetchone()[0]
    print(f"[修復前] margin_ahead=0 かつ finish_pos>1 かつ finish_time_sec>0: {ahead_count:,} 件")

    # ── 対象レース取得 ──
    # margin_ahead=0 の2着以降を含むレースのみ処理する
    # ただし margin_behind も同レース内で一括修復する
    cur.execute("""
        SELECT DISTINCT race_id FROM race_log
        WHERE margin_ahead = 0 AND finish_pos > 1 AND finish_time_sec > 0
    """)
    target_races = [r[0] for r in cur.fetchall()]
    total_races = len(target_races)
    print(f"[対象] {total_races:,} レースを処理します")

    if total_races == 0:
        print("修復対象がありません。終了します。")
        conn.close()
        return

    # ── レースごとに処理 ──
    updated_ahead = 0
    updated_behind = 0
    skipped_races = 0
    start_time = time.time()

    for i, race_id in enumerate(target_races):
        if i % 200 == 0 or i == total_races - 1:
            elapsed = time.time() - start_time
            eta = (elapsed / (i + 1)) * (total_races - i - 1) if i > 0 else 0
            progress_bar(i + 1, total_races, prefix=f"処理中 (ETA {eta:.0f}s) ")

        # レース内の全馬を finish_pos 順に取得
        cur.execute("""
            SELECT id, finish_pos, finish_time_sec, margin_ahead, margin_behind
            FROM race_log
            WHERE race_id = ? AND finish_pos > 0
            ORDER BY finish_pos ASC
        """, (race_id,))
        rows = cur.fetchall()

        if not rows:
            skipped_races += 1
            continue

        # 1着馬の finish_time_sec を取得
        winner_time = None
        for row in rows:
            if row[1] == 1 and row[2] and row[2] > 0:
                winner_time = row[2]
                break

        if winner_time is None:
            # 1着馬のタイムが不明 → スキップ
            skipped_races += 1
            continue

        # 有効なタイムを持つ馬のリスト (finish_pos順)
        valid_rows = [(r[0], r[1], r[2], r[3], r[4]) for r in rows if r[2] and r[2] > 0]

        for j, (row_id, pos, time_sec, old_ahead, old_behind) in enumerate(valid_rows):
            # ── margin_ahead 計算 ──
            if pos == 1:
                new_ahead = None  # 1着は NULL
            else:
                new_ahead = round(time_sec - winner_time, 1)

            # ── margin_behind 計算 ──
            if j == len(valid_rows) - 1:
                # 最下位 (有効タイム持ちの中で) → 0
                new_behind = 0.0
            else:
                next_time = valid_rows[j + 1][2]
                new_behind = round(next_time - time_sec, 1)

            # 更新が必要か判定
            need_ahead = (pos > 1 and (old_ahead == 0 or old_ahead is None) and new_ahead is not None and new_ahead >= 0)
            need_behind_update = False

            # margin_behind は margin_ahead が壊れてるレースでは同様に壊れている可能性が高いので
            # 同レース内の margin_behind も再計算する
            if old_behind == 0 and new_behind > 0:
                need_behind_update = True

            if not dry_run:
                if need_ahead and need_behind_update:
                    cur.execute("""
                        UPDATE race_log SET margin_ahead = ?, margin_behind = ?
                        WHERE id = ?
                    """, (new_ahead, new_behind, row_id))
                    updated_ahead += 1
                    updated_behind += 1
                elif need_ahead:
                    cur.execute("""
                        UPDATE race_log SET margin_ahead = ?
                        WHERE id = ?
                    """, (new_ahead, row_id))
                    updated_ahead += 1
                elif need_behind_update:
                    cur.execute("""
                        UPDATE race_log SET margin_behind = ?
                        WHERE id = ?
                    """, (new_behind, row_id))
                    updated_behind += 1
            else:
                if need_ahead:
                    updated_ahead += 1
                if need_behind_update:
                    updated_behind += 1

    # プログレスバー完了
    progress_bar(total_races, total_races, prefix="完了 ")
    print()

    elapsed = time.time() - start_time
    mode = "[DRY-RUN]" if dry_run else "[実行完了]"

    print(f"\n{mode} 処理結果:")
    print(f"  処理レース数: {total_races:,}")
    print(f"  スキップレース数: {skipped_races:,} (1着タイム不明)")
    print(f"  margin_ahead 更新: {updated_ahead:,} 件")
    print(f"  margin_behind 更新: {updated_behind:,} 件")
    print(f"  処理時間: {elapsed:.1f}秒")

    if not dry_run:
        conn.commit()
        print("\n[コミット完了] 変更を保存しました。")

        # 修復後カウント
        cur.execute("""
            SELECT COUNT(*) FROM race_log
            WHERE margin_ahead = 0 AND finish_pos > 1 AND finish_time_sec > 0
        """)
        remaining = cur.fetchone()[0]
        print(f"[修復後] margin_ahead=0 残り: {remaining:,} 件")
    else:
        print("\n[DRY-RUN] 実際の変更は行っていません。--dry-run を外して実行してください。")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="race_log の margin_ahead/margin_behind を finish_time_sec から再計算"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="実際の更新を行わず、影響件数のみ表示"
    )
    parser.add_argument(
        "--db",
        default=DB_PATH,
        help=f"DBファイルパス (デフォルト: {DB_PATH})"
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"[エラー] DBファイルが見つかりません: {args.db}")
        sys.exit(1)

    print(f"DB: {args.db}")
    print(f"モード: {'DRY-RUN (事前確認)' if args.dry_run else '本実行'}")
    print("-" * 60)

    fix_margins(args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
