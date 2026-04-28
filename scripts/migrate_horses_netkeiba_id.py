#!/usr/bin/env python3
"""
D Phase 2: horses.netkeiba_id 補完スクリプト

- old_10digit (10桁数値 horse_id) → netkeiba_id = horse_id（直値）
- nar_prefix / B_prefix → netkeiba_id = NULL（将来スクレイパー連携で補完）

使い方:
    python scripts/migrate_horses_netkeiba_id.py          # dry-run（変更なし）
    python scripts/migrate_horses_netkeiba_id.py --apply  # 実際に UPDATE
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

# プロジェクトルートを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import DATABASE_PATH
from src.database import backup_db, init_schema
from src.log import get_logger

logger = get_logger(__name__)

# 10桁数値パターン（netkeiba horse_id 形式）
_NETKEIBA_10DIGIT_GLOB = "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"


def count_targets(conn: sqlite3.Connection) -> dict:
    """補完対象件数の内訳を返す"""
    cur = conn.cursor()

    # 全 horses 件数
    cur.execute("SELECT COUNT(*) FROM horses")
    total = cur.fetchone()[0]

    # old_10digit かつ netkeiba_id が NULL（= 補完対象）
    cur.execute(
        f"""
        SELECT COUNT(*) FROM horses
        WHERE horse_id GLOB '{_NETKEIBA_10DIGIT_GLOB}'
          AND netkeiba_id IS NULL
        """
    )
    target_null = cur.fetchone()[0]

    # old_10digit かつ netkeiba_id が既に埋まっている（= 済み）
    cur.execute(
        f"""
        SELECT COUNT(*) FROM horses
        WHERE horse_id GLOB '{_NETKEIBA_10DIGIT_GLOB}'
          AND netkeiba_id IS NOT NULL
        """
    )
    already_done = cur.fetchone()[0]

    # B_prefix 件数
    cur.execute("SELECT COUNT(*) FROM horses WHERE horse_id LIKE 'B%'")
    b_prefix = cur.fetchone()[0]

    # nar_prefix 件数（10桁数字でも B_ でもない）
    cur.execute(
        f"""
        SELECT COUNT(*) FROM horses
        WHERE horse_id NOT GLOB '{_NETKEIBA_10DIGIT_GLOB}'
          AND horse_id NOT LIKE 'B%'
        """
    )
    nar_prefix = cur.fetchone()[0]

    return {
        "total": total,
        "target_null": target_null,
        "already_done": already_done,
        "b_prefix": b_prefix,
        "nar_prefix": nar_prefix,
    }


def dry_run(conn: sqlite3.Connection) -> dict:
    """dry-run: 変更件数のみを返す（UPDATE 実行なし）"""
    info = count_targets(conn)
    print("[dry-run] 実行予定サマリー:")
    print(f"  horses 総件数          : {info['total']:>8,}")
    print(f"  UPDATE 対象 (old_10digit, netkeiba_id=NULL): {info['target_null']:>8,}")
    print(f"  既補完済み             : {info['already_done']:>8,}")
    print(f"  B_prefix (スキップ)    : {info['b_prefix']:>8,}")
    print(f"  nar_prefix (スキップ)  : {info['nar_prefix']:>8,}")
    print()
    print("[dry-run] 実際の更新は --apply フラグを付けて実行してください。")
    return info


def apply_migration(conn: sqlite3.Connection) -> dict:
    """実際に UPDATE を実行する"""
    info_before = count_targets(conn)
    print(f"[apply] 補完対象件数: {info_before['target_null']:,} 件")

    t0 = time.time()

    # old_10digit の horse_id を netkeiba_id にセット
    cur = conn.cursor()
    cur.execute(
        f"""
        UPDATE horses
           SET netkeiba_id = horse_id,
               updated_at  = datetime('now', 'localtime')
         WHERE horse_id GLOB '{_NETKEIBA_10DIGIT_GLOB}'
           AND netkeiba_id IS NULL
        """
    )
    updated = cur.rowcount
    conn.commit()

    elapsed = time.time() - t0
    print(f"[apply] UPDATE 完了: {updated:,} 件 / {elapsed:.2f} 秒")

    info_after = count_targets(conn)
    return {
        "updated": updated,
        "elapsed": elapsed,
        "info_after": info_after,
    }


def verify_after(conn: sqlite3.Connection) -> None:
    """補完後の内訳を表示"""
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM horses WHERE netkeiba_id IS NOT NULL")
    with_id = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM horses WHERE netkeiba_id IS NULL")
    without_id = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM horses")
    total = cur.fetchone()[0]

    print()
    print("[検証] horses テーブル内訳（補完後）:")
    print(f"  全件数                        : {total:>8,}")
    print(f"  netkeiba_id NOT NULL (補完済) : {with_id:>8,}")
    print(f"  netkeiba_id IS NULL  (未補完) : {without_id:>8,}")

    # サンプル確認（old_10digit）
    cur.execute(
        """
        SELECT horse_id, horse_name, netkeiba_id
        FROM horses
        WHERE netkeiba_id IS NOT NULL
        LIMIT 3
        """
    )
    print()
    print("  netkeiba_id 補完済サンプル:")
    for row in cur.fetchall():
        print(f"    horse_id={row[0]}  horse_name={row[1]}  netkeiba_id={row[2]}")

    # サンプル確認（NULL のまま）
    cur.execute(
        """
        SELECT horse_id, horse_name
        FROM horses
        WHERE netkeiba_id IS NULL
        LIMIT 3
        """
    )
    print()
    print("  netkeiba_id NULL サンプル（B/nar_prefix）:")
    for row in cur.fetchall():
        print(f"    horse_id={row[0]}  horse_name={row[1]}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="D Phase 2: horses.netkeiba_id を old_10digit から補完する"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="実際に UPDATE を実行する（省略時は dry-run）",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="バックアップをスキップする（テスト用）",
    )
    args = parser.parse_args()

    print(f"DB: {DATABASE_PATH}")

    # スキーマ初期化（netkeiba_id カラムが存在しない場合 ALTER TABLE を実行）
    print("[1/4] スキーマ初期化 (netkeiba_id カラム確認)...")
    init_schema()
    print("[1/4] 完了")

    # DB 接続
    conn = sqlite3.connect(DATABASE_PATH)

    # カラム存在確認
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(horses)")
    cols = [c[1] for c in cur.fetchall()]
    if "netkeiba_id" not in cols:
        print("[エラー] horses テーブルに netkeiba_id カラムが存在しません。")
        print("         init_schema() が正しく動作していない可能性があります。")
        conn.close()
        sys.exit(1)
    print(f"[確認] netkeiba_id カラム: 存在")

    if args.apply:
        # バックアップ取得
        if not args.no_backup:
            print("[2/4] DB バックアップ取得...")
            bk = backup_db()
            if bk:
                print(f"[2/4] バックアップ: {bk}")
            else:
                print("[2/4] バックアップ失敗（続行）")
        else:
            print("[2/4] バックアップスキップ（--no-backup）")

        print("[3/4] UPDATE 実行...")
        result = apply_migration(conn)
        print(f"[3/4] 更新件数: {result['updated']:,} / 経過: {result['elapsed']:.2f}秒")

        print("[4/4] 補完後検証...")
        verify_after(conn)
    else:
        print("[2/4] dry-run モード（--apply なし）")
        dry_run(conn)
        print()
        print("[3/4] dry-run 検証（実データ変更なし）")
        verify_after(conn)
        print()
        print("[4/4] 実行するには --apply を付けてください:")
        print(f"       python {Path(__file__).name} --apply")

    conn.close()


if __name__ == "__main__":
    main()
