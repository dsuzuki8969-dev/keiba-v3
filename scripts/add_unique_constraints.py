"""
predictions と race_log に UNIQUE 制約を追加する migration script。

予想重複・race_log ズレ行の再発を防止する。
事前に Step 2-AB cleanup で重複を 0 件にしておく必要がある。

使い方:
  python scripts/add_unique_constraints.py --dry-run  # 検証のみ
  python scripts/add_unique_constraints.py             # 本実行
"""
import sqlite3
import shutil
import sys
from pathlib import Path
from datetime import datetime

DRY_RUN = "--dry-run" in sys.argv
DB = Path("data/keiba.db")


def main():
    # 事前バックアップ（dry-run 時はスキップ）
    if not DRY_RUN:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = DB.parent / f"keiba.db.bak_unique_{ts}"
        shutil.copy(DB, backup)
        print(f"バックアップ作成: {backup}")
        for ext in ("-wal", "-shm"):
            try:
                shutil.copy(f"{DB}{ext}", f"{backup}{ext}")
            except FileNotFoundError:
                pass

    conn = sqlite3.connect(DB)
    try:
        # 既存 UNIQUE INDEX の有無確認
        for idx_name in ("idx_predictions_race_id_unique", "idx_racelog_race_horse_unique"):
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
                (idx_name,),
            ).fetchone()
            print(f"既存 INDEX: {idx_name} → {'あり' if row else 'なし'}")

        # 重複チェック（UNIQUE 追加前の安全確認）
        dup_pred = conn.execute(
            "SELECT COUNT(*) FROM (SELECT race_id FROM predictions GROUP BY race_id HAVING COUNT(*)>1)"
        ).fetchone()[0]
        dup_log = conn.execute(
            "SELECT COUNT(*) FROM (SELECT race_id, horse_no FROM race_log GROUP BY race_id, horse_no HAVING COUNT(*)>1)"
        ).fetchone()[0]
        print(f"事前重複チェック: predictions={dup_pred} / race_log={dup_log}")

        if dup_pred > 0 or dup_log > 0:
            print("[ERROR] 重複が残存しているため UNIQUE INDEX 作成不可。")
            print("先に scripts/cleanup_predictions_race_log_dup.py を実行すること。")
            sys.exit(1)

        if DRY_RUN:
            print("[DRY-RUN] CREATE UNIQUE INDEX をスキップ（実行せず）")
            return

        # 本実行
        with conn:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_race_id_unique "
                "ON predictions(race_id)"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_racelog_race_horse_unique "
                "ON race_log(race_id, horse_no)"
            )
        print("[OK] UNIQUE INDEX 追加完了")

        # 事後確認
        for idx_name in ("idx_predictions_race_id_unique", "idx_racelog_race_horse_unique"):
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
                (idx_name,),
            ).fetchone()
            assert row, f"INDEX {idx_name} が見つかりません"
        print("[OK] 事後検証完了")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
