#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
merge_duplicate_horses.py
=========================
horses テーブル内で B_prefix と nar_prefix が同一馬名を持つ重複レコードを統合する。

処理フロー:
1. 同名異形式ペアを検出 (B_prefix x nar_prefix の horse_name 一致)
2. 正規 horse_id の選定 (nar_prefix 優先、B_prefix が 6 ヶ月超新しければ SKIP)
3. DBバックアップ
4. トランザクション内でマージ実行
   - race_log の horse_id を更新 (B→nar)
   - horses から B_prefix 行を削除
5. 検証クエリ実行

使用例:
    python scripts/merge_duplicate_horses.py --dry-run
    python scripts/merge_duplicate_horses.py --apply
"""

import argparse
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime

# 文字化け対策
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# ---- 設定 ----------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "keiba.db")
BACKUP_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "backups")

# B_prefix が nar_prefix より N 日以上新しければ SKIP（安全側）
SKIP_DAYS_THRESHOLD = 180

# バッチサイズ（race_log UPDATE）
BATCH_SIZE = 1000


def detect_pairs(conn: sqlite3.Connection) -> list[dict]:
    """
    同名異形式の horse_id ペアを検出する。

    Returns:
        list[dict]: 検出ペアのリスト
            - b_horse_id   : B_prefix の horse_id
            - n_horse_id   : nar_prefix の horse_id
            - horse_name   : 馬名
            - b_count      : B_prefix の race_count
            - n_count      : nar_prefix の race_count
            - b_last       : B_prefix の last_seen_date
            - n_last       : nar_prefix の last_seen_date
            - canonical_id : 採用する horse_id (nar_prefix 優先)
            - drop_id      : 削除する horse_id (B_prefix)
            - action       : "merge" or "skip"
            - skip_reason  : skip の理由
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            b.horse_id   AS b_horse_id,
            b.horse_name AS horse_name,
            n.horse_id   AS n_horse_id,
            b.race_count AS b_count,
            n.race_count AS n_count,
            b.last_seen_date AS b_last,
            n.last_seen_date AS n_last
        FROM horses b
        INNER JOIN horses n ON b.horse_name = n.horse_name
        WHERE b.horse_id LIKE 'B%'
          AND n.horse_id LIKE 'nar_%'
        ORDER BY b.horse_id
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    pairs = []
    for row in rows:
        d = dict(zip(cols, row))
        action = "merge"
        skip_reason = ""
        canonical_id = d["n_horse_id"]  # デフォルト: nar_prefix を採用
        drop_id = d["b_horse_id"]

        # B_prefix が 6 ヶ月以上新しい場合は安全側で SKIP
        if d["b_last"] and d["n_last"]:
            try:
                b_date = datetime.strptime(d["b_last"], "%Y-%m-%d")
                n_date = datetime.strptime(d["n_last"], "%Y-%m-%d")
                diff_days = (b_date - n_date).days
                if diff_days > SKIP_DAYS_THRESHOLD:
                    action = "skip"
                    skip_reason = f"B_prefix が {diff_days} 日新しい (閾値 {SKIP_DAYS_THRESHOLD} 日超)"
            except ValueError:
                pass

        d["canonical_id"] = canonical_id
        d["drop_id"] = drop_id
        d["action"] = action
        d["skip_reason"] = skip_reason
        pairs.append(d)

    return pairs


def count_race_log_references(conn: sqlite3.Connection, horse_id: str) -> int:
    """race_log 内の指定 horse_id の参照件数を返す"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM race_log WHERE horse_id = ?", (horse_id,))
    return cur.fetchone()[0]


def backup_db(db_path: str, backup_dir: str) -> str:
    """DB をタイムスタンプ付きでバックアップ。バックアップパスを返す。"""
    os.makedirs(backup_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"keiba_{ts}_pre_merge_horses.db")
    print(f"  DBバックアップ中: {backup_path}")
    shutil.copy2(db_path, backup_path)
    size_mb = os.path.getsize(backup_path) / (1024 * 1024)
    print(f"  バックアップ完了: {size_mb:.1f} MB")
    return backup_path


def dry_run(pairs: list[dict]) -> None:
    """dry-run: 検出結果のサマリとサンプルを表示する"""
    merge_pairs = [p for p in pairs if p["action"] == "merge"]
    skip_pairs  = [p for p in pairs if p["action"] == "skip"]

    print("\n" + "=" * 70)
    print("【DRY-RUN 結果】")
    print("=" * 70)
    print(f"  検出ペア総数  : {len(pairs):,} 件")
    print(f"  マージ対象    : {len(merge_pairs):,} 件 (nar_prefix を採用)")
    print(f"  スキップ      : {len(skip_pairs):,} 件 (B_prefix が大幅に新しい)")
    print()

    # 採用方針内訳
    print("【採用方針内訳】")
    print(f"  nar_prefix 採用 (B_prefix 削除) : {len(merge_pairs):,} 件")
    print(f"  スキップ                         : {len(skip_pairs):,} 件")
    if skip_pairs:
        print("  ── スキップ詳細 ──")
        for p in skip_pairs:
            print(f"    {p['b_horse_id']} / {p['n_horse_id']} [{p['horse_name']}] 理由: {p['skip_reason']}")
    print()

    # サンプル20件
    print("【マージ対象サンプル (最大20件)】")
    header = f"  {'B_horse_id':15} {'horse_name':15} {'nar_horse_id':22} {'B_cnt':6} {'N_cnt':6} {'B_last':12} {'N_last':12}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for p in merge_pairs[:20]:
        print(
            f"  {p['b_horse_id']:15} {p['horse_name']:15} {p['n_horse_id']:22}"
            f" {str(p['b_count']):6} {str(p['n_count']):6}"
            f" {str(p['b_last']):12} {str(p['n_last']):12}"
        )
    if len(merge_pairs) > 20:
        print(f"  ... 他 {len(merge_pairs) - 20:,} 件")
    print()
    print("  ※ --apply で実際にマージを実行します")
    print("=" * 70)


def apply_merge(conn: sqlite3.Connection, pairs: list[dict]) -> dict:
    """
    実際のマージを実行する（トランザクション内）。

    Returns:
        dict: 実行結果サマリ
    """
    merge_pairs = [p for p in pairs if p["action"] == "merge"]
    skip_count  = len([p for p in pairs if p["action"] == "skip"])

    cur = conn.cursor()
    total_race_log_updated = 0
    total_horses_deleted   = 0
    start_time = time.time()

    print(f"\n  マージ対象: {len(merge_pairs):,} 件 / スキップ: {skip_count:,} 件")
    print("  トランザクション開始...")
    conn.execute("BEGIN TRANSACTION")

    try:
        # race_log の horse_id 更新（バッチ処理）
        print(f"\n  [1/2] race_log 更新中 (バッチサイズ: {BATCH_SIZE})...")
        for i in range(0, len(merge_pairs), BATCH_SIZE):
            batch = merge_pairs[i : i + BATCH_SIZE]
            for p in batch:
                cur.execute(
                    "UPDATE race_log SET horse_id = ? WHERE horse_id = ?",
                    (p["canonical_id"], p["drop_id"]),
                )
                total_race_log_updated += cur.rowcount

            done = min(i + BATCH_SIZE, len(merge_pairs))
            elapsed = time.time() - start_time
            pct = done / len(merge_pairs) * 100
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            print(f"    [{bar}] {pct:.1f}% ({done:,}/{len(merge_pairs):,}) {elapsed:.1f}s")

        # horses テーブルから B_prefix 行を削除
        print(f"\n  [2/2] horses テーブルから B_prefix 削除中...")
        drop_ids = [(p["drop_id"],) for p in merge_pairs]
        cur.executemany("DELETE FROM horses WHERE horse_id = ?", drop_ids)
        total_horses_deleted = len(drop_ids)

        conn.execute("COMMIT")
        elapsed = time.time() - start_time
        print(f"\n  コミット完了 ({elapsed:.1f}秒)")

    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"\n  !! エラー発生 → ROLLBACK: {e}")
        raise

    return {
        "merge_count"          : len(merge_pairs),
        "skip_count"           : skip_count,
        "race_log_updated"     : total_race_log_updated,
        "horses_deleted"       : total_horses_deleted,
        "elapsed_sec"          : time.time() - start_time,
    }


def verify(conn: sqlite3.Connection, pairs: list[dict], merge_result: dict) -> None:
    """マージ後の整合性を検証する"""
    cur = conn.cursor()
    merge_pairs = [p for p in pairs if p["action"] == "merge"]

    print("\n" + "=" * 70)
    print("【マージ後 検証】")
    print("=" * 70)

    # horses 件数確認
    cur.execute("SELECT COUNT(*) FROM horses")
    total_horses = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM horses WHERE horse_id LIKE 'B%'")
    remaining_b = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM horses WHERE horse_id LIKE 'nar_%'")
    remaining_nar = cur.fetchone()[0]

    print(f"  horses 総件数     : {total_horses:,}")
    print(f"  B_prefix 残存     : {remaining_b:,} (期待: 0)")
    print(f"  nar_prefix 件数   : {remaining_nar:,}")
    print()

    # race_log に削除 horse_id が残存していないか
    drop_ids = [p["drop_id"] for p in merge_pairs]
    if drop_ids:
        # SQLite の IN 句は変数バインドで対応
        placeholders = ",".join("?" * len(drop_ids))
        cur.execute(
            f"SELECT COUNT(*) FROM race_log WHERE horse_id IN ({placeholders})",
            drop_ids,
        )
        leftover = cur.fetchone()[0]
    else:
        leftover = 0

    print(f"  race_log 削除ID残存: {leftover:,} (期待: 0)")
    if leftover > 0:
        print("  !! 警告: race_log に削除 horse_id が残存しています")
    print()

    # サンプル5馬の race_log 統合確認
    print("  【サンプル5馬 マージ前後比較】")
    print(f"  {'horse_name':15} {'旧B_id':15} {'採用nar_id':22} {'race_log件数':12}")
    print("  " + "-" * 68)
    for p in merge_pairs[:5]:
        cur.execute(
            "SELECT COUNT(*) FROM race_log WHERE horse_id = ?",
            (p["canonical_id"],),
        )
        rl_count = cur.fetchone()[0]
        print(
            f"  {p['horse_name']:15} {p['b_horse_id']:15} {p['n_horse_id']:22} {rl_count:,}"
        )
    print()

    # 残存 B_prefix の確認（本来 0 件のはず）
    if remaining_b > 0:
        # nar_prefix との同名ペアを持たない B_prefix だけ表示（マージ未対象）
        cur.execute("""
            SELECT horse_id, horse_name FROM horses
            WHERE horse_id LIKE 'B%'
              AND NOT EXISTS (
                  SELECT 1 FROM horses n
                  WHERE n.horse_id LIKE 'nar_%' AND n.horse_name = horses.horse_name
              )
            LIMIT 5
        """)
        leftovers = cur.fetchall()
        if leftovers:
            print("  !! マージ対象外の残存 B_prefix (nar_pairなし) サンプル:")
            for row in leftovers:
                print(f"    horse_id={row[0]}, horse_name={row[1]}")

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="horses テーブルの B_prefix / nar_prefix 同名重複をマージする"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="マージ対象を表示するだけ（実行しない）",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="実際にマージを実行する",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("使用方法: --dry-run または --apply を指定してください")
        sys.exit(1)

    # DB 接続
    db_path = os.path.abspath(DB_PATH)
    if not os.path.exists(db_path):
        print(f"エラー: DB が見つかりません: {db_path}")
        sys.exit(1)

    print(f"DB: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        # ペア検出
        print("\n[1/4] 同名異形式ペアを検出中...")
        pairs = detect_pairs(conn)
        merge_count = sum(1 for p in pairs if p["action"] == "merge")
        skip_count  = sum(1 for p in pairs if p["action"] == "skip")
        print(f"  検出: 総 {len(pairs):,} ペア / マージ {merge_count:,} 件 / スキップ {skip_count:,} 件")

        if args.dry_run:
            dry_run(pairs)
            return

        # apply モード
        if merge_count == 0:
            print("マージ対象が 0 件のため終了します。")
            return

        # バックアップ
        print("\n[2/4] DBバックアップ中...")
        backup_path = backup_db(db_path, os.path.abspath(BACKUP_DIR))

        # マージ実行
        print("\n[3/4] マージ実行中...")
        result = apply_merge(conn, pairs)

        # 検証
        print("\n[4/4] 検証中...")
        verify(conn, pairs, result)

        # 最終サマリ
        print("\n" + "=" * 70)
        print("【最終サマリ】")
        print("=" * 70)
        print(f"  マージ件数        : {result['merge_count']:,} ペア")
        print(f"  スキップ件数      : {result['skip_count']:,} ペア")
        print(f"  race_log 更新行数 : {result['race_log_updated']:,} 行")
        print(f"  horses 削除行数   : {result['horses_deleted']:,} 行")
        print(f"  実行時間          : {result['elapsed_sec']:.1f} 秒")
        print(f"  バックアップ      : {backup_path}")
        print("=" * 70)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
