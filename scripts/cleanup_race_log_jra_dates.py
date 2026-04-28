#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cleanup_race_log_jra_dates.py
===============================
race_log テーブルの JRA race_date 汚染を修正するスクリプト。

D-1a で構築した真値マスタ data/masters/race_id_date_master.json を使い、
race_log の race_date を正しい値に UPDATE する。

使用方法:
  # dry-run (デフォルト): 影響行数確認のみ
  python scripts/cleanup_race_log_jra_dates.py
  python scripts/cleanup_race_log_jra_dates.py --dry-run

  # 本実行
  python scripts/cleanup_race_log_jra_dates.py --apply

  # 詳細ログ付き本実行
  python scripts/cleanup_race_log_jra_dates.py --apply --verbose
"""

import argparse
import json
import shutil
import sqlite3
import sys
import time
from pathlib import Path

# プロジェクトルートを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent.parent))

# ─────────────────────────────────────────
# パス定義
# ─────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH      = PROJECT_ROOT / "data" / "keiba.db"
MASTER_PATH  = PROJECT_ROOT / "data" / "masters" / "race_id_date_master.json"
BAK_BASE     = DB_PATH.parent / "keiba.db.bak_t033_20260428"


# ─────────────────────────────────────────
# バックアップ取得
# ─────────────────────────────────────────
def _backup_db() -> Path:
    """DB をバックアップし、バックアップパスを返す。
    同名が存在する場合は _2, _3 ... サフィックスを付与。
    """
    bak_path = BAK_BASE
    if bak_path.exists():
        n = 2
        while True:
            candidate = Path(str(BAK_BASE) + f"_{n}")
            if not candidate.exists():
                bak_path = candidate
                break
            n += 1

    print(f"\n[BACKUP] DB コピー中: {DB_PATH.name} → {bak_path.name}")
    shutil.copy2(str(DB_PATH), str(bak_path))

    # WAL / SHM もコピー
    for ext in ("-wal", "-shm"):
        src = Path(str(DB_PATH) + ext)
        if src.exists():
            dst = Path(str(bak_path) + ext)
            shutil.copy2(str(src), str(dst))
            size_kb = dst.stat().st_size / 1024
            print(f"[BACKUP]   {dst.name}: {size_kb:.1f} KB")

    size_mb = bak_path.stat().st_size / (1024 * 1024)
    print(f"[BACKUP] 完了: {bak_path} ({size_mb:.1f} MB)")
    return bak_path


# ─────────────────────────────────────────
# 真値マスタ読み込み
# ─────────────────────────────────────────
def _load_master() -> dict:
    """race_id_date_master.json から mapping dict を返す。"""
    with open(str(MASTER_PATH), "r", encoding="utf-8") as f:
        data = json.load(f)

    # フォーマット: {"version": ..., "mapping": {"race_id": "YYYY-MM-DD", ...}}
    if isinstance(data, dict) and "mapping" in data:
        mapping = data["mapping"]
    else:
        # フラット形式 fallback (将来仕様変更対応)
        mapping = data

    print(f"\n[MASTER] 読み込み完了: {len(mapping)} エントリ ({MASTER_PATH.name})")
    return mapping


# ─────────────────────────────────────────
# dry-run: 影響行数確認
# ─────────────────────────────────────────
def _dry_run(conn: sqlite3.Connection, mapping: dict, verbose: bool) -> tuple[int, int]:
    """修正が必要な行数と race_id 数を返す。サンプル 20 件を表示。"""
    print("\n" + "=" * 60)
    print("[DRY-RUN] 影響行数を調査中...")
    print("=" * 60)

    # 影響行数・影響 race_id 数を集計
    affected_rows  = 0
    affected_ids   = 0
    sample_rows    = []   # (race_id, current_date, true_date)

    # mapping の全 race_id に対して SELECT で確認
    # バッチ処理で効率化 (SQLite の IN 句は 999 上限)
    race_ids = list(mapping.keys())
    batch_size = 900

    for i in range(0, len(race_ids), batch_size):
        batch = race_ids[i : i + batch_size]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(
            f"""
            SELECT race_id, race_date, COUNT(*) AS cnt
            FROM race_log
            WHERE race_id IN ({placeholders})
            GROUP BY race_id, race_date
            """,
            batch,
        ).fetchall()

        for race_id, current_date, cnt in rows:
            true_date = mapping[race_id]
            if current_date != true_date:
                affected_rows += cnt
                affected_ids  += 1
                if len(sample_rows) < 20:
                    sample_rows.append((race_id, current_date, true_date, cnt))

    print(f"\n[結果] 修正対象行数   : {affected_rows:,} 行")
    print(f"[結果] 影響 race_id 数: {affected_ids:,} ID")

    if sample_rows:
        print(f"\n[サンプル] 先頭最大 20 件 (race_id, 現在 race_date, 正値 race_date, 行数):")
        for rid, cur, tru, cnt in sample_rows:
            print(f"  {rid}  {cur} → {tru}  ({cnt} 行)")
    else:
        print("[サンプル] 修正対象なし")

    if verbose:
        # 詳細: 全修正対象を表示
        print(f"\n[VERBOSE] 全修正対象 ({affected_ids} ID):")
        # 再取得は省略: sample で確認済みとする
        pass

    return affected_rows, affected_ids


# ─────────────────────────────────────────
# 本実行: UPDATE
# ─────────────────────────────────────────
def _apply(conn: sqlite3.Connection, mapping: dict, verbose: bool) -> tuple[int, int]:
    """race_log を UPDATE し、更新行数と影響 race_id 数を返す。"""
    print("\n" + "=" * 60)
    print("[APPLY] トランザクション開始...")
    print("=" * 60)

    t_start = time.perf_counter()
    total_updated  = 0
    total_id_count = 0

    race_ids   = list(mapping.keys())
    batch_size = 900

    conn.execute("BEGIN")
    try:
        for i in range(0, len(race_ids), batch_size):
            batch = race_ids[i : i + batch_size]

            for race_id in batch:
                true_date = mapping[race_id]
                cur = conn.execute(
                    """
                    UPDATE race_log
                    SET    race_date = ?
                    WHERE  race_id   = ?
                      AND  race_date != ?
                    """,
                    (true_date, race_id, true_date),
                )
                if cur.rowcount > 0:
                    total_updated  += cur.rowcount
                    total_id_count += 1
                    if verbose:
                        print(f"  UPDATE {race_id}: {cur.rowcount} 行 → {true_date}")

            # 進捗表示 (バッチごと)
            processed = min(i + batch_size, len(race_ids))
            pct = processed / len(race_ids) * 100
            print(f"  進捗: {processed:,}/{len(race_ids):,} race_id 処理済 ({pct:.1f}%) | 更新行数: {total_updated:,}")

        conn.execute("COMMIT")

    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"\n[ERROR] エラー発生 → ROLLBACK: {e}")
        raise

    elapsed = time.perf_counter() - t_start
    print(f"\n[APPLY] COMMIT 完了")
    print(f"[APPLY] 実 UPDATE 行数   : {total_updated:,} 行")
    print(f"[APPLY] 影響 race_id 数  : {total_id_count:,} ID")
    print(f"[APPLY] 経過時間         : {elapsed:.2f} 秒")

    return total_updated, total_id_count


# ─────────────────────────────────────────
# 事後検証
# ─────────────────────────────────────────
def _verify(conn: sqlite3.Connection, mapping: dict) -> None:
    """修正後の整合性を検証して表示する。"""
    print("\n" + "=" * 60)
    print("[VERIFY] 事後検証")
    print("=" * 60)

    # 1) 元旦 JRA レコード件数 (期待: 0)
    jan1_count = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE race_date = '2026-01-01' AND is_jra = 1"
    ).fetchone()[0]
    status_jan1 = "OK" if jan1_count == 0 else "NG (汚染残存あり)"
    print(f"\n[VERIFY] 元旦 (2026-01-01) JRA レコード件数: {jan1_count} ← 期待: 0  [{status_jan1}]")

    # 2) 2026-01-04 JRA レコード件数 (中山金杯・京都金杯等)
    jan4_count = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE race_date = '2026-01-04' AND is_jra = 1"
    ).fetchone()[0]
    jan4_ids = conn.execute(
        "SELECT DISTINCT race_id FROM race_log WHERE race_date = '2026-01-04' AND is_jra = 1"
    ).fetchall()
    print(f"[VERIFY] 2026-01-04 JRA レコード件数: {jan4_count} (race_id {len(jan4_ids)} 種)")
    for row in jan4_ids[:10]:
        print(f"  {row[0]}")

    # 3) 真値マスタにあるのに race_log に存在しない race_id を警告
    master_ids = set(mapping.keys())
    race_ids_in_db = {
        row[0]
        for row in conn.execute("SELECT DISTINCT race_id FROM race_log").fetchall()
    }
    missing_in_log = master_ids - race_ids_in_db
    if missing_in_log:
        print(f"\n[WARN] 真値マスタに存在するが race_log にない race_id: {len(missing_in_log)} 件")
        for rid in sorted(missing_in_log)[:20]:
            print(f"  {rid}")
        if len(missing_in_log) > 20:
            print(f"  ... (残り {len(missing_in_log) - 20} 件省略)")
    else:
        print(f"\n[VERIFY] 真値マスタ全 race_id が race_log に存在: OK")

    # 4) 修正後の不整合チェック (race_log に残っている JRA race_id で race_date が mapping と違う行)
    mismatches = 0
    race_ids_jra = list(mapping.keys())
    batch_size = 900
    for i in range(0, len(race_ids_jra), batch_size):
        batch = race_ids_jra[i : i + batch_size]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(
            f"""
            SELECT race_id, race_date
            FROM race_log
            WHERE race_id IN ({placeholders})
            GROUP BY race_id, race_date
            """,
            batch,
        ).fetchall()
        for race_id, current_date in rows:
            true_date = mapping[race_id]
            if current_date != true_date:
                mismatches += 1
                if mismatches <= 5:
                    print(f"[WARN] 未修正残存: {race_id}  DB={current_date}  正値={true_date}")

    status_mismatch = "OK" if mismatches == 0 else f"NG ({mismatches} 件残存)"
    print(f"\n[VERIFY] 不整合残存チェック: {status_mismatch}")

    print("\n" + "=" * 60)
    print("[VERIFY] 検証完了")
    print("=" * 60)


# ─────────────────────────────────────────
# メイン
# ─────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="race_log の JRA race_date 汚染を真値マスタで修正する"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="影響行数確認のみ（DB を変更しない）",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="本実行（UPDATE + COMMIT）",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="詳細ログを表示",
    )
    args = parser.parse_args()

    # --apply も --dry-run も未指定の場合は dry-run として扱う
    is_apply = args.apply
    is_dry   = not is_apply  # --apply がなければ dry-run

    print("=" * 60)
    print("race_log JRA race_date クリーンアップ (T-033 D-1c)")
    print(f"モード: {'本実行 (--apply)' if is_apply else 'DRY-RUN (変更なし)'}")
    print("=" * 60)

    # ─── 前提チェック ───────────────────────
    if not DB_PATH.exists():
        print(f"[ERROR] DB が見つかりません: {DB_PATH}")
        sys.exit(1)

    if not MASTER_PATH.exists():
        print(f"[ERROR] 真値マスタが見つかりません: {MASTER_PATH}")
        sys.exit(1)

    # ─── バックアップ (本実行時のみ) ─────────
    if is_apply:
        bak_path = _backup_db()
        assert bak_path.exists(), "バックアップ取得に失敗しました。本実行を中止します。"

    # ─── 真値マスタ読み込み ──────────────────
    mapping = _load_master()

    # ─── DB 接続 (WAL モード) ─────────────────
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    try:
        if is_dry:
            # dry-run: 影響行数確認のみ
            affected_rows, affected_ids = _dry_run(conn, mapping, args.verbose)
            print("\n[DRY-RUN] DB への変更は行いませんでした。")
            print(f"  本実行は --apply オプションで実行してください。")
        else:
            # 本実行: dry-run → apply → verify
            dry_rows, dry_ids = _dry_run(conn, mapping, args.verbose)

            if dry_rows == 0:
                print("\n[INFO] 修正対象なし。本実行をスキップします。")
            else:
                updated_rows, updated_ids = _apply(conn, mapping, args.verbose)
                _verify(conn, mapping)

    finally:
        conn.close()

    print("\n完了。")


if __name__ == "__main__":
    main()
