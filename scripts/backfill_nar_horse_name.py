"""
backfill_nar_horse_name.py
===========================
race_log の nar_prefix レコードに horse_name を補完するスクリプト。

horse_name が空の nar_prefix レコードに対して、
同一 race_id × horse_no で predictions.horses_json から horse_name を取得し UPDATE する。

再取得 (スクレイピング) は行わない。predictions キャッシュのみを利用する。

使い方:
  python scripts/backfill_nar_horse_name.py --dry-run
  python scripts/backfill_nar_horse_name.py --apply
"""

import argparse
import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime

# ── 共通設定 ──────────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "keiba.db")
DB_PATH = os.path.normpath(DB_PATH)

BACKUP_SUFFIX = f"bak_pre_horse_name_backfill_{datetime.now().strftime('%Y%m%d%H%M%S')}"


def _connect(db_path: str) -> sqlite3.Connection:
    """SQLite 接続（WAL モード）"""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def build_name_map_from_predictions(con: sqlite3.Connection) -> dict:
    """
    predictions.horses_json から (race_id, horse_no) → horse_name のマップを構築する。

    Returns:
        {(race_id, horse_no): horse_name, ...}
    """
    cur = con.cursor()

    # nar_prefix race_id で horse_name が空のものに絞って predictions を検索
    cur.execute("""
        SELECT DISTINCT race_id FROM race_log
        WHERE horse_id LIKE 'nar_%'
          AND (horse_name IS NULL OR horse_name = '')
    """)
    target_race_ids = {r["race_id"] for r in cur.fetchall()}

    if not target_race_ids:
        return {}

    print(f"  対象 race_id: {len(target_race_ids):,} 件")

    # predictions から一括取得（全件取得後 Python 側でフィルタ）
    placeholders = ",".join("?" * len(target_race_ids))
    cur.execute(
        f"SELECT race_id, horses_json FROM predictions WHERE race_id IN ({placeholders})",
        tuple(target_race_ids)
    )
    pred_rows = cur.fetchall()
    print(f"  predictions ヒット: {len(pred_rows):,} 件")

    name_map = {}
    parse_errors = 0
    for row in pred_rows:
        race_id = row["race_id"]
        try:
            horses = json.loads(row["horses_json"])
            for h in horses:
                hno = h.get("horse_no")
                hname = h.get("horse_name", "") or ""
                if hno is not None and hname:
                    name_map[(race_id, int(hno))] = hname
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            parse_errors += 1
            if parse_errors <= 5:
                print(f"  [警告] horses_json パースエラー race_id={race_id}: {e}")

    if parse_errors:
        print(f"  [警告] パースエラー合計: {parse_errors} 件")

    return name_map


def analyze_backfill(con: sqlite3.Connection, name_map: dict) -> dict:
    """
    バックフィル可能件数を分析する（DB 変更なし）。

    Returns:
        {"total_empty": int, "mappable": int, "not_mappable": int, "samples": [...]}
    """
    cur = con.cursor()

    cur.execute("""
        SELECT race_id, horse_no, race_date
        FROM race_log
        WHERE horse_id LIKE 'nar_%'
          AND (horse_name IS NULL OR horse_name = '')
        ORDER BY race_date DESC
    """)
    rows = cur.fetchall()

    mappable = 0
    not_mappable = 0
    samples_map = []
    samples_nomap = []

    for row in rows:
        key = (row["race_id"], row["horse_no"])
        if key in name_map:
            mappable += 1
            if len(samples_map) < 5:
                samples_map.append({
                    "race_id": row["race_id"],
                    "horse_no": row["horse_no"],
                    "race_date": row["race_date"],
                    "horse_name": name_map[key],
                })
        else:
            not_mappable += 1
            if len(samples_nomap) < 5:
                samples_nomap.append({
                    "race_id": row["race_id"],
                    "horse_no": row["horse_no"],
                    "race_date": row["race_date"],
                })

    return {
        "total_empty": len(rows),
        "mappable": mappable,
        "not_mappable": not_mappable,
        "samples_map": samples_map,
        "samples_nomap": samples_nomap,
    }


def apply_backfill(con: sqlite3.Connection, name_map: dict) -> int:
    """
    horse_name を race_log に UPDATE する。

    Returns:
        更新件数
    """
    cur = con.cursor()

    # バックフィル対象レコードを取得
    cur.execute("""
        SELECT race_id, horse_no
        FROM race_log
        WHERE horse_id LIKE 'nar_%'
          AND (horse_name IS NULL OR horse_name = '')
    """)
    rows = cur.fetchall()

    updated = 0
    skipped = 0

    # バッチ更新: race_id × horse_no をキーに UPDATE
    updates = []
    for row in rows:
        key = (row["race_id"], row["horse_no"])
        if key in name_map:
            updates.append((name_map[key], row["race_id"], row["horse_no"]))
        else:
            skipped += 1

    if updates:
        con.executemany(
            "UPDATE race_log SET horse_name = ? WHERE race_id = ? AND horse_no = ?",
            updates
        )
        con.commit()
        updated = len(updates)

    return updated, skipped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="nar_prefix レコードの horse_name を predictions から補完する"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="補完可能件数の分析のみ（DB 変更なし）"
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="実際に UPDATE を実行する"
    )
    parser.add_argument(
        "--db", default=DB_PATH,
        help=f"DB パス (デフォルト: {DB_PATH})"
    )
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("エラー: --dry-run または --apply を指定してください。")
        parser.print_help()
        sys.exit(1)

    db_path = os.path.normpath(args.db)
    if not os.path.exists(db_path):
        print(f"エラー: DB ファイルが見つかりません: {db_path}")
        sys.exit(1)

    print("=" * 60)
    print("backfill_nar_horse_name.py")
    print("=" * 60)
    print(f"対象 DB : {db_path}")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"モード  : {'apply' if args.apply else 'dry-run'}")
    print()

    con = _connect(db_path)

    try:
        # ── Step 1: predictions から name_map を構築 ──
        print("[Step 1] predictions.horses_json から horse_name マップを構築中...")
        name_map = build_name_map_from_predictions(con)
        print(f"  マップ構築完了: {len(name_map):,} エントリ")
        print()

        if not name_map:
            print("  [情報] predictions に対応するレコードが存在しません。")
            print("         nar_prefix race_id が predictions に未登録の可能性があります。")
            con.close()
            sys.exit(0)

        # ── Step 2: バックフィル可能件数を分析 ──
        print("[Step 2] バックフィル可能件数を分析中...")
        result = analyze_backfill(con, name_map)

        print(f"  horse_name 空の nar_prefix レコード: {result['total_empty']:,} 件")
        print(f"  predictions でマッピング可能:        {result['mappable']:,} 件")
        print(f"  マッピング不可 (predictions なし):   {result['not_mappable']:,} 件 → スキップ（フォールバック禁止）")
        print()

        if result["samples_map"]:
            print("  [補完可能サンプル (最大5件)]")
            for s in result["samples_map"]:
                print(f"    race_id={s['race_id']} horse_no={s['horse_no']} "
                      f"date={s['race_date']} → {s['horse_name']}")

        if result["samples_nomap"]:
            print()
            print("  [マッピング不可サンプル (最大5件)]")
            for s in result["samples_nomap"]:
                print(f"    race_id={s['race_id']} horse_no={s['horse_no']} "
                      f"date={s['race_date']} → predictions なし (skip)")

        print()

        # ── Step 3: dry-run の場合はここで終了 ──
        if args.dry_run:
            print("[dry-run] DB 変更なし。--apply で実行してください。")
            con.close()
            return

        # ── Step 4: apply ──
        if result["mappable"] == 0:
            print("[apply] マッピング可能件数が 0 件のため、DB への変更は行いませんでした。")
            con.close()
            return

        # バックアップ作成
        backup_path = db_path + "." + BACKUP_SUFFIX
        print(f"[apply] バックアップ作成: {backup_path}")
        con.close()  # いったんクローズしてバックアップ
        shutil.copy2(db_path, backup_path)
        print(f"        完了: {os.path.getsize(backup_path) / 1024 / 1024:.1f} MB")
        print()

        # 再接続して UPDATE
        con = _connect(db_path)
        print(f"[apply] UPDATE 実行中... ({result['mappable']:,} 件)")
        updated, skipped = apply_backfill(con, name_map)
        print(f"  UPDATE 完了: {updated:,} 件")
        print(f"  スキップ:    {skipped:,} 件 (predictions なし)")
        print()

        # ── Step 5: 検証 ──
        cur = con.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM race_log
            WHERE horse_id LIKE 'nar_%'
              AND (horse_name IS NULL OR horse_name = '')
        """)
        remaining_empty = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM race_log WHERE horse_id LIKE 'nar_%'")
        nar_total = cur.fetchone()[0]

        print("[検証結果]")
        print(f"  nar_prefix 総件数: {nar_total:,}")
        print(f"  horse_name 空 (残): {remaining_empty:,} 件")
        filled = nar_total - remaining_empty
        pct = filled / nar_total * 100 if nar_total else 0
        print(f"  horse_name 充足:   {filled:,} 件 ({pct:.1f}%)")

        if remaining_empty > 0:
            print()
            print(f"  [情報] {remaining_empty:,} 件は predictions が存在しないためスキップ（仕様）")

    finally:
        con.close()

    print()
    print("完了。")


if __name__ == "__main__":
    main()
