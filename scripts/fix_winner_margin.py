"""
fix_winner_margin.py
race_log の finish_pos=1 かつ margin_ahead=0 の行を margin_ahead=NULL に修正。

使い方:
  python scripts/fix_winner_margin.py --dry-run   # 件数確認のみ
  python scripts/fix_winner_margin.py             # 本実行
"""
import argparse
import sqlite3
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import DATABASE_PATH as DB_PATH


def main() -> None:
    parser = argparse.ArgumentParser(description="1着馬の margin_ahead=0 を NULL に修正")
    parser.add_argument("--dry-run", action="store_true", help="変更せず件数のみ表示")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE finish_pos = 1 AND margin_ahead = 0"
        )
        count = cur.fetchone()[0]
        print(f"対象件数: {count} 件 (finish_pos=1 AND margin_ahead=0)")

        if args.dry_run:
            print("--dry-run モード: 変更は行いません。")
            return

        conn.execute(
            "UPDATE race_log SET margin_ahead = NULL WHERE finish_pos = 1 AND margin_ahead = 0"
        )
        conn.commit()
        print(f"修正完了: {count} 件を margin_ahead=NULL に更新しました。")

        # 検証: 残件数が 0 であることを確認
        remain = conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE finish_pos = 1 AND margin_ahead = 0"
        ).fetchone()[0]
        print(f"残件数: {remain} 件 (0 であれば修正完了)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
