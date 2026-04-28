"""
backfill_empty_horse_id.py
===========================
race_log.horse_id が空（NULL / ''）かつ horse_name がある行について、
predictions.horses_json を race_id × horse_no で突合し、
正しい horse_name と horse_id を両方補完するスクリプト。

背景:
  horse_id 空レコードの horse_name カラムには馬番（数字文字列）が
  誤入力されており、同名逆引きではなく predictions からの直接補完が必要。

マッピング戦略:
  1. race_id × horse_no で predictions.horses_json を引く
  2. horse_name / horse_id の両方を取得し、horse_name が正規馬名（カナ/漢字あり）
     であることを確認してから UPDATE
  3. predictions にない race_id、horse_no 不一致は除外（フォールバック禁止）

更新カラム:
  - race_log.horse_id   (空 → predictions の horse_id)
  - race_log.horse_name (馬番誤入力 → predictions の horse_name)

使い方:
  python scripts/backfill_empty_horse_id.py --dry-run
  python scripts/backfill_empty_horse_id.py --apply
"""

import argparse
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from datetime import datetime

# ── 共通設定 ──────────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "keiba.db")
DB_PATH = os.path.normpath(DB_PATH)

BACKUP_SUFFIX = "bak_pre_e_20260428"

SAMPLE_SIZE = 20  # dry-run で表示するサンプル件数

# 正規馬名判定: カタカナ・ひらがな・漢字のいずれかを含む
REAL_NAME_RE = re.compile(r'[゠-ヿぁ-ゟ一-鿿]')


def _connect(db_path: str) -> sqlite3.Connection:
    """SQLite 接続（WAL モード・Row ファクトリ付き）"""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def build_predictions_map(con: sqlite3.Connection, target_race_ids: set) -> dict:
    """
    predictions.horses_json から (race_id, horse_no) → {horse_name, horse_id} のマップを構築する。

    Args:
        target_race_ids: 対象の race_id 集合

    Returns:
        {(race_id, horse_no): {"horse_name": str, "horse_id": str}, ...}
    """
    cur = con.cursor()

    if not target_race_ids:
        return {}

    placeholders = ",".join("?" * len(target_race_ids))
    cur.execute(
        f"SELECT race_id, horses_json FROM predictions WHERE race_id IN ({placeholders})",
        tuple(sorted(target_race_ids)),
    )
    pred_rows = cur.fetchall()
    print(f"  predictions ヒット: {len(pred_rows):,} / {len(target_race_ids):,} race_id")

    pred_map: dict = {}
    parse_errors = 0

    for row in pred_rows:
        race_id = row["race_id"]
        try:
            horses = json.loads(row["horses_json"])
            for h in horses:
                hno = h.get("horse_no")
                hname = (h.get("horse_name") or "").strip()
                hid = str(h.get("horse_id") or "").strip()
                if hno is not None and hname:
                    pred_map[(race_id, int(hno))] = {
                        "horse_name": hname,
                        "horse_id": hid,
                    }
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            parse_errors += 1
            if parse_errors <= 5:
                print(f"  [警告] horses_json パースエラー race_id={race_id}: {e}")

    if parse_errors:
        print(f"  [警告] パースエラー合計: {parse_errors} 件")

    return pred_map


def analyze_targets(con: sqlite3.Connection) -> list:
    """
    horse_id が空の対象レコードを全件取得する（horse_name の値は問わない）。

    Returns:
        対象行のリスト (id, race_id, race_date, horse_no, horse_name)
    """
    cur = con.cursor()
    cur.execute("""
        SELECT id, race_id, race_date, horse_no, horse_name
        FROM race_log
        WHERE horse_id IS NULL OR horse_id = ''
        ORDER BY race_date DESC, horse_no ASC
    """)
    return cur.fetchall()


def run_dry_run(
    targets: list,
    pred_map: dict,
) -> dict:
    """
    dry-run: マッピング可能件数・除外件数・不可件数を集計しサンプルを表示する。

    除外条件:
      - predictions に race_id がない
      - predictions に horse_no が一致しない
      - predictions の horse_name が正規馬名でない（数字 or 英字のみ）

    Returns:
        {
            "mappable_both": int,      horse_id + horse_name 両方補完可
            "mappable_id_only": int,   horse_id のみ補完可（horse_name は正規馬名）
            "excluded_no_pred": int,   predictions にない
            "excluded_bad_name": int,  predictions の horse_name が不正
            "samples": [...]
        }
    """
    mappable_both = 0
    mappable_id_only = 0
    excluded_no_pred = 0
    excluded_bad_name = 0
    samples: list[dict] = []

    for row in targets:
        key = (row["race_id"], int(row["horse_no"]))
        pred = pred_map.get(key)

        if pred is None:
            excluded_no_pred += 1
            continue

        pred_name = pred["horse_name"]
        pred_hid = pred["horse_id"]

        if not REAL_NAME_RE.search(pred_name):
            # predictions の horse_name も数字 or 英字のみ → 補完不可
            excluded_bad_name += 1
            continue

        # horse_name が数字誤入力かどうかで分岐
        current_name = row["horse_name"] or ""
        name_needs_fix = not REAL_NAME_RE.search(current_name) if current_name else True

        if name_needs_fix:
            mappable_both += 1
        else:
            # horse_name は正規馬名だが horse_id が空
            mappable_id_only += 1

        if len(samples) < SAMPLE_SIZE:
            samples.append({
                "id": row["id"],
                "race_id": row["race_id"],
                "race_date": row["race_date"],
                "horse_no": row["horse_no"],
                "horse_name_before": current_name or "(空)",
                "horse_name_after": pred_name,
                "horse_id_before": "(空)",
                "horse_id_after": pred_hid,
                "fix_type": "horse_id+horse_name" if name_needs_fix else "horse_id_only",
            })

    return {
        "mappable_both": mappable_both,
        "mappable_id_only": mappable_id_only,
        "excluded_no_pred": excluded_no_pred,
        "excluded_bad_name": excluded_bad_name,
        "samples": samples,
    }


def apply_updates(
    con: sqlite3.Connection,
    targets: list,
    pred_map: dict,
) -> tuple[int, int, int, int]:
    """
    トランザクション内で horse_id (+ horse_name) を UPDATE する。

    Returns:
        (updated_both, updated_id_only, skipped_no_pred, skipped_bad_name)
    """
    cur = con.cursor()

    updates_both: list[tuple] = []    # horse_id + horse_name 両方
    updates_id_only: list[tuple] = [] # horse_id のみ
    skipped_no_pred = 0
    skipped_bad_name = 0

    for row in targets:
        key = (row["race_id"], int(row["horse_no"]))
        pred = pred_map.get(key)

        if pred is None:
            skipped_no_pred += 1
            continue

        pred_name = pred["horse_name"]
        pred_hid = pred["horse_id"]

        if not REAL_NAME_RE.search(pred_name):
            skipped_bad_name += 1
            continue

        current_name = row["horse_name"] or ""
        if not REAL_NAME_RE.search(current_name):
            # horse_name も誤入力 → 両方 UPDATE
            updates_both.append((pred_hid, pred_name, row["id"]))
        else:
            # horse_id のみ UPDATE
            updates_id_only.append((pred_hid, row["id"]))

    # アトミック UPDATE
    con.execute("BEGIN")
    if updates_both:
        cur.executemany(
            "UPDATE race_log SET horse_id = ?, horse_name = ? WHERE id = ?",
            updates_both,
        )
    if updates_id_only:
        cur.executemany(
            "UPDATE race_log SET horse_id = ? WHERE id = ?",
            updates_id_only,
        )
    con.commit()

    return len(updates_both), len(updates_id_only), skipped_no_pred, skipped_bad_name


def count_empty_horse_id(con: sqlite3.Connection) -> int:
    """horse_id が空の race_log 件数を返す"""
    cur = con.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM race_log
        WHERE horse_id IS NULL OR horse_id = ''
    """)
    return cur.fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="race_log.horse_id 空行を predictions から補完するスクリプト"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="マッピング可能件数の分析のみ（DB 変更なし）"
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
    print("backfill_empty_horse_id.py")
    print("=" * 60)
    print(f"対象 DB : {db_path}")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"モード  : {'apply' if args.apply else 'dry-run'}")
    print()

    con = _connect(db_path)

    try:
        # ── Step 1: 対象件数確認 ──────────────────────────────────────────────
        print("[Step 1] 対象レコードを抽出中...")
        initial_empty = count_empty_horse_id(con)
        print(f"  race_log.horse_id 空 (全体): {initial_empty:,} 件")

        targets = analyze_targets(con)
        # うち horse_name あり件数（馬番誤入力含む）
        name_nonempty = sum(1 for r in targets if r["horse_name"])
        print(f"  → 対象 (horse_id 空):        {len(targets):,} 件")
        print(f"    うち horse_name あり:        {name_nonempty:,} 件 (馬番誤入力含む)")
        print()

        if not targets:
            print("  [情報] 対象レコードが存在しないため処理終了。")
            con.close()
            sys.exit(0)

        # ── Step 2: predictions から (race_id, horse_no) → {name, id} マップ構築 ──
        print("[Step 2] predictions から補完マップを構築中...")
        t0 = time.time()
        target_race_ids = {r["race_id"] for r in targets}
        pred_map = build_predictions_map(con, target_race_ids)
        elapsed = time.time() - t0
        print(f"  マップエントリ: {len(pred_map):,} 件")
        print(f"  構築時間:       {elapsed:.2f}s")
        print()

        # ── Step 3: dry-run 分析 ─────────────────────────────────────────────
        print("[Step 3] マッピング可否を集計中...")
        result = run_dry_run(targets, pred_map)

        total_mappable = result["mappable_both"] + result["mappable_id_only"]
        print(f"  マッピング可能 (合計):          {total_mappable:,} 件")
        print(f"    horse_id + horse_name 両方:   {result['mappable_both']:,} 件")
        print(f"    horse_id のみ:                {result['mappable_id_only']:,} 件")
        print(f"  除外 (predictions なし):        {result['excluded_no_pred']:,} 件")
        print(f"  除外 (pred horse_name が不正):  {result['excluded_bad_name']:,} 件")
        print()

        if result["samples"]:
            print(f"  [補完サンプル (最大 {SAMPLE_SIZE} 件)]")
            hdr = f"  {'id':>8}  {'race_id':<14}  {'date':>10}  {'no':>3}  {'name_before':<10}  {'name_after':<16}  {'horse_id_after':<14}  type"
            print(hdr)
            print(f"  {'-'*8}  {'-'*14}  {'-'*10}  {'-'*3}  {'-'*10}  {'-'*16}  {'-'*14}  {'-'*18}")
            for s in result["samples"]:
                print(
                    f"  {s['id']:>8}  {s['race_id']:<14}  {s['race_date']:>10}  "
                    f"{s['horse_no']:>3}  {s['horse_name_before']:<10}  "
                    f"{s['horse_name_after']:<16}  {s['horse_id_after']:<14}  {s['fix_type']}"
                )
            print()

        # dry-run モードはここで終了
        if args.dry_run:
            print("[dry-run] DB 変更なし。--apply で本実行してください。")
            con.close()
            return

        # ── Step 4: apply ────────────────────────────────────────────────────
        if total_mappable == 0:
            print("[apply] マッピング可能件数が 0 件のため DB 変更なし。")
            con.close()
            return

        # バックアップ作成（失敗時は本実行禁止）
        backup_path = db_path + "." + BACKUP_SUFFIX
        print(f"[Step 4] バックアップ作成: {backup_path}")
        con.close()  # いったんクローズしてからバイナリコピー
        try:
            shutil.copy2(db_path, backup_path)
        except Exception as exc:
            print(f"  [エラー] バックアップ失敗: {exc}")
            print("  本実行を中止します（バックアップ取得失敗時は本実行禁止）。")
            sys.exit(1)

        bak_size_mb = os.path.getsize(backup_path) / 1024 / 1024
        print(f"  完了: {bak_size_mb:.1f} MB")
        assert os.path.exists(backup_path), "バックアップファイルが見つかりません（assert 失敗）"
        print()

        # 再接続
        con = _connect(db_path)

        print(f"[Step 5] UPDATE 実行中... ({total_mappable:,} 件)")
        t1 = time.time()
        updated_both, updated_id_only, skipped_no_pred, skipped_bad_name = apply_updates(
            con, targets, pred_map
        )
        elapsed_apply = time.time() - t1
        updated_total = updated_both + updated_id_only
        print(f"  UPDATE 完了 (合計):           {updated_total:,} 件")
        print(f"    horse_id + horse_name 両方: {updated_both:,} 件")
        print(f"    horse_id のみ:              {updated_id_only:,} 件")
        print(f"  スキップ (predictions なし):  {skipped_no_pred:,} 件")
        print(f"  スキップ (horse_name 不正):   {skipped_bad_name:,} 件")
        print(f"  経過時間:                     {elapsed_apply:.2f}s")
        print()

        # ── Step 6: 検証 ─────────────────────────────────────────────────────
        print("[Step 6] 修正後 empty 件数を確認中...")
        remaining_empty = count_empty_horse_id(con)
        fixed = initial_empty - remaining_empty
        print(f"  修正前 horse_id 空: {initial_empty:,} 件")
        print(f"  修正後 horse_id 空: {remaining_empty:,} 件")
        print(f"  削減件数:           {fixed:,} 件")
        print()

        if fixed != updated_total:
            print(f"  [警告] UPDATE 件数 ({updated_total}) と削減件数 ({fixed}) が一致しません。")
            print("         predictions の horse_id が空だった可能性があります。")

        print("=" * 60)
        print("完了。")
        print("=" * 60)

    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
