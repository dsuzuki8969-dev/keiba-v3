"""
cleanup_predictions_race_log_dup.py
====================================
predictions テーブルの重複行と race_log のズレ行を削除するクリーンアップスクリプト。

使用方法:
  python scripts/cleanup_predictions_race_log_dup.py           # 本実行
  python scripts/cleanup_predictions_race_log_dup.py --dry-run # ドライラン（SELECT のみ）
"""

import sqlite3
import shutil
import sys
from datetime import datetime
from pathlib import Path

# ドライランモード判定
DRY_RUN = '--dry-run' in sys.argv

# DB パス
DB = Path('data/keiba.db')


def main():
    print('=' * 60)
    print('predictions / race_log 重複クリーンアップ')
    print(f'モード: {"DRY-RUN（削除なし）" if DRY_RUN else "本実行"}')
    print('=' * 60)

    if not DB.exists():
        print(f'エラー: DB が見つかりません → {DB}')
        sys.exit(1)

    # ────────────────────────────────
    # バックアップ（本実行時のみ）
    # ────────────────────────────────
    if not DRY_RUN:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        bak_db = DB.parent / f'keiba.db.bak_cleanup_{ts}'
        shutil.copy(str(DB), str(bak_db))
        print(f'バックアップ作成: {bak_db.name}')
        for ext in ('-wal', '-shm'):
            src = Path(str(DB) + ext)
            if src.exists():
                dst = Path(str(bak_db) + ext)
                shutil.copy(str(src), str(dst))
                print(f'バックアップ作成: {dst.name}')

    conn = sqlite3.connect(str(DB))
    try:
        # ────────────────────────────────
        # Step A: race_log ズレ行の確認
        # ────────────────────────────────
        print('\n--- Step A: race_log ズレ行の確認 ---')

        # 事前カウント
        before_rl = conn.execute('SELECT COUNT(*) FROM race_log').fetchone()[0]
        print(f'race_log 総件数（削除前）: {before_rl}')

        # 削除対象を特定: 同一 race_id に複数の race_date が存在する場合、件数が少ない方を削除
        # ※ SQLite では WITH (CTE) は FROM/JOIN の中に置けないため、サブクエリで書き直す
        select_a = """
        SELECT rl.id, rl.race_id, rl.race_date, rl.finish_pos
        FROM race_log rl
        WHERE EXISTS (
            SELECT 1 FROM (
                SELECT c.race_id, c.race_date
                FROM (
                    SELECT race_id, race_date, COUNT(*) AS cnt
                    FROM race_log GROUP BY race_id, race_date
                ) c
                WHERE c.cnt < (
                    SELECT MAX(c2.cnt) FROM (
                        SELECT race_id, COUNT(*) AS cnt
                        FROM race_log GROUP BY race_id, race_date
                    ) c2 WHERE c2.race_id = c.race_id
                )
            ) bad
            WHERE bad.race_id = rl.race_id AND bad.race_date = rl.race_date
        )
        """
        rows_a = conn.execute(select_a).fetchall()
        print(f'race_log 削除対象件数: {len(rows_a)} 件')
        if rows_a:
            # finish_pos 分布を確認
            fp_dist = {}
            for row in rows_a:
                fp = row[3]
                fp_dist[fp] = fp_dist.get(fp, 0) + 1
            print(f'  finish_pos 分布: {fp_dist}')
            # 先頭 5 行を表示
            print('  先頭 5 行 (id, race_id, race_date, finish_pos):')
            for row in rows_a[:5]:
                print(f'    {row}')

        # ────────────────────────────────
        # Step B: predictions 重複行の確認
        # ────────────────────────────────
        print('\n--- Step B: predictions 重複行の確認 ---')

        before_pred = conn.execute('SELECT COUNT(*) FROM predictions').fetchone()[0]
        print(f'predictions 総件数（削除前）: {before_pred}')

        dup_race_ids = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT race_id FROM predictions GROUP BY race_id HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        print(f'重複 race_id 数（削除前）: {dup_race_ids} 件')

        # 削除対象: 同一 race_id で最古の date 以外の行
        select_b = """
        SELECT id, race_id, date
        FROM predictions p1
        WHERE EXISTS (
            SELECT 1 FROM predictions p2
            WHERE p2.race_id = p1.race_id
              AND p2.date < p1.date
        )
        """
        rows_b = conn.execute(select_b).fetchall()
        print(f'predictions 削除対象件数: {len(rows_b)} 件')
        if rows_b:
            # 先頭 5 行を表示
            print('  先頭 5 行 (id, race_id, date):')
            for row in rows_b[:5]:
                print(f'    {row}')

        # ────────────────────────────────
        # ドライランなら終了
        # ────────────────────────────────
        if DRY_RUN:
            print('\n[DRY-RUN] DELETE は実行せず終了します。')
            print(f'  race_log:    {before_rl} → {before_rl - len(rows_a)} (-{len(rows_a)})')
            print(f'  predictions: {before_pred} → {before_pred - len(rows_b)} (-{len(rows_b)})')
            return

        # ────────────────────────────────
        # 本実行: トランザクション内で A + B を削除
        # ────────────────────────────────
        print('\n--- 本実行: トランザクション内で削除を実行 ---')

        delete_a = """
        DELETE FROM race_log
        WHERE id IN (
            SELECT rl.id FROM race_log rl
            WHERE EXISTS (
                SELECT 1 FROM (
                    SELECT c.race_id, c.race_date
                    FROM (
                        SELECT race_id, race_date, COUNT(*) AS cnt
                        FROM race_log GROUP BY race_id, race_date
                    ) c
                    WHERE c.cnt < (
                        SELECT MAX(c2.cnt) FROM (
                            SELECT race_id, COUNT(*) AS cnt
                            FROM race_log GROUP BY race_id, race_date
                        ) c2 WHERE c2.race_id = c.race_id
                    )
                ) bad
                WHERE bad.race_id = rl.race_id AND bad.race_date = rl.race_date
            )
        )
        """

        delete_b = """
        DELETE FROM predictions
        WHERE id IN (
            SELECT id FROM predictions p1
            WHERE EXISTS (
                SELECT 1 FROM predictions p2
                WHERE p2.race_id = p1.race_id
                  AND p2.date < p1.date
            )
        )
        """

        conn.execute('BEGIN')
        try:
            cur_a = conn.execute(delete_a)
            deleted_a = cur_a.rowcount
            print(f'race_log 削除件数: {deleted_a} 件')

            cur_b = conn.execute(delete_b)
            deleted_b = cur_b.rowcount
            print(f'predictions 削除件数: {deleted_b} 件')

            conn.execute('COMMIT')
            print('COMMIT 完了')
        except Exception as e:
            conn.execute('ROLLBACK')
            print(f'エラー発生。ROLLBACK しました: {e}')
            raise

        # ────────────────────────────────
        # 事後カウント・検証
        # ────────────────────────────────
        print('\n--- 事後検証 ---')

        after_rl = conn.execute('SELECT COUNT(*) FROM race_log').fetchone()[0]
        after_pred = conn.execute('SELECT COUNT(*) FROM predictions').fetchone()[0]

        dup_rl_after = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT race_id FROM race_log
                GROUP BY race_id HAVING COUNT(DISTINCT race_date) > 1
            )
        """).fetchone()[0]

        dup_pred_after = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT race_id FROM predictions
                GROUP BY race_id HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        print(f'race_log 総件数: {before_rl} → {after_rl} (差: -{before_rl - after_rl})')
        print(f'predictions 総件数: {before_pred} → {after_pred} (差: -{before_pred - after_pred})')
        print(f'race_log 重複 race_id 数（事後）: {dup_rl_after} ← 0 であること')
        print(f'predictions 重複 race_id 数（事後）: {dup_pred_after} ← 0 であること')

        if dup_rl_after == 0 and dup_pred_after == 0:
            print('\n[OK] 検証 OK: 重複は完全に解消されました。')
        else:
            print('\n[警告] まだ重複が残っています。手動確認してください。')

    finally:
        conn.close()

    print('\n完了。')


if __name__ == '__main__':
    main()
