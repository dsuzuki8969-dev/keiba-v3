#!/usr/bin/env python
"""
D-AI Keiba DBメンテナンス・バックアップスクリプト

機能:
  - SQLite VACUUM（断片化解消・サイズ削減）
  - ANALYZE（クエリ最適化用統計更新）
  - WALチェックポイント
  - バックアップ (data/backup/keiba_YYYYMMDD.db)
  - バックアップの世代管理（デフォルト7世代保持）

Usage:
  python scripts/db_maintenance.py                  # VACUUM + ANALYZE + バックアップ
  python scripts/db_maintenance.py --no-backup      # バックアップなし
  python scripts/db_maintenance.py --backup-only    # バックアップのみ
  python scripts/db_maintenance.py --no-vacuum      # VACUUMスキップ
  python scripts/db_maintenance.py --keep 14        # バックアップ14世代保持
"""

import argparse
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH    = "data/keiba.db"
BACKUP_DIR = "data/backup"


def _log(msg: str, t_start: float = None) -> None:
    """タイムスタンプ付きでログを出力する"""
    ts = datetime.now().strftime("%H:%M:%S")
    if t_start is not None:
        elapsed = time.time() - t_start
        print(f"  [{ts}] {msg}  ({elapsed:.1f}秒)", flush=True)
    else:
        print(f"  [{ts}] {msg}", flush=True)


def human_size(path: str) -> str:
    sz = os.path.getsize(path)
    if sz >= 1024 ** 3:
        return f"{sz / 1024**3:.2f} GB"
    elif sz >= 1024 ** 2:
        return f"{sz / 1024**2:.1f} MB"
    return f"{sz / 1024:.1f} KB"


def human_size_bytes(sz: int) -> str:
    if sz >= 1024 ** 3:
        return f"{sz / 1024**3:.2f} GB"
    elif sz >= 1024 ** 2:
        return f"{sz / 1024**2:.1f} MB"
    return f"{sz / 1024:.1f} KB"


def run_vacuum(conn: sqlite3.Connection):
    t = time.time()
    _log("VACUUM 開始 (断片化解消・サイズ削減)...")
    _log("  ※ DBサイズにより数十秒〜数分かかります。しばらくお待ちください。")
    conn.execute("VACUUM")
    _log("VACUUM 完了", t)


def run_analyze(conn: sqlite3.Connection):
    t = time.time()
    _log("ANALYZE 開始 (クエリプランナー用統計更新)...")
    conn.execute("ANALYZE")
    _log("ANALYZE 完了", t)


def run_wal_checkpoint(conn: sqlite3.Connection):
    t = time.time()
    _log("WAL チェックポイント実行中...")
    result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    # result = (busy, log, checkpointed)
    busy, log_pages, ckpt_pages = result
    _log(f"WAL チェックポイント完了  ログページ={log_pages}, 書込済={ckpt_pages}", t)


def show_stats(conn: sqlite3.Connection):
    print(f"\n  ─── テーブル統計 ───────────────────────────────")
    print(f"  {'テーブル名':<22} {'レコード数':>10}")
    print(f"  {'-'*34}")
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    for (t,) in tables:
        try:
            cnt = conn.execute(f"SELECT COUNT(*) FROM \"{t}\"").fetchone()[0]
            print(f"  {t:<22} {cnt:>10,}")
        except Exception:
            print(f"  {t:<22} {'(エラー)':>10}")

    # インデックス一覧
    print(f"\n  ─── インデックス ───────────────────────────────")
    idxs = conn.execute(
        "SELECT name, tbl_name FROM sqlite_master WHERE type='index' ORDER BY tbl_name, name"
    ).fetchall()
    for name, tbl in idxs:
        print(f"  {tbl:<22} → {name}")
    print()


def do_backup(keep: int) -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BACKUP_DIR, f"keiba_{today}.db")

    t = time.time()
    src_size = os.path.getsize(DB_PATH)
    _log(f"バックアップ開始: {dst}  ({human_size_bytes(src_size)})")
    shutil.copy2(DB_PATH, dst)
    _log(f"バックアップ完了: {human_size(dst)}", t)

    # 世代管理
    backups = sorted(
        [f for f in os.listdir(BACKUP_DIR) if f.startswith("keiba_") and f.endswith(".db")]
    )
    while len(backups) > keep:
        old = os.path.join(BACKUP_DIR, backups.pop(0))
        _log(f"古いバックアップを削除: {os.path.basename(old)}")
        os.remove(old)

    _log(f"バックアップ保持数: {len(backups)}/{keep}世代")
    return dst


def main():
    parser = argparse.ArgumentParser(description="DBメンテナンス・バックアップ")
    parser.add_argument("--no-backup",   action="store_true", help="バックアップをスキップ")
    parser.add_argument("--backup-only", action="store_true", help="バックアップのみ実行")
    parser.add_argument("--no-vacuum",   action="store_true", help="VACUUMをスキップ")
    parser.add_argument("--keep",        type=int, default=7, help="バックアップ保持世代数 (デフォルト:7)")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"[ERROR] DBファイルが見つかりません: {DB_PATH}")
        sys.exit(1)

    t_total = time.time()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    size_before_bytes = os.path.getsize(DB_PATH)

    print(f"\n{'='*62}")
    print(f"  D-AI Keiba DB メンテナンス")
    print(f"  実行日時: {now_str}")
    print(f"  DB: {DB_PATH}")
    print(f"  DBサイズ (実行前): {human_size_bytes(size_before_bytes)}")
    print(f"{'='*62}\n")

    # バックアップのみモード
    if args.backup_only:
        do_backup(args.keep)
        elapsed = time.time() - t_total
        print(f"\n  完了! (合計 {elapsed:.1f}秒)\n")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    # テーブル統計表示
    show_stats(conn)

    # [1/4] WALチェックポイント
    print(f"  ─── [1/4] WAL チェックポイント ─────────────────")
    run_wal_checkpoint(conn)

    # [2/4] VACUUM
    print(f"\n  ─── [2/4] VACUUM ───────────────────────────────")
    if not args.no_vacuum:
        conn.close()  # VACUUMはclose後に再接続して実行（WALを閉じるため）
        conn = sqlite3.connect(DB_PATH)
        run_vacuum(conn)
        conn.close()
        conn = sqlite3.connect(DB_PATH)
    else:
        _log("VACUUM: スキップ (--no-vacuum 指定)")

    # [3/4] ANALYZE
    print(f"\n  ─── [3/4] ANALYZE ──────────────────────────────")
    run_analyze(conn)
    conn.close()

    # サイズ比較
    size_after_bytes  = os.path.getsize(DB_PATH)
    size_diff         = size_before_bytes - size_after_bytes
    print(f"\n  ─── サイズ比較 ─────────────────────────────────")
    print(f"  実行前: {human_size_bytes(size_before_bytes)}")
    print(f"  実行後: {human_size_bytes(size_after_bytes)}")
    if size_diff > 0:
        print(f"  削減量: -{human_size_bytes(size_diff)}  ({size_diff / size_before_bytes * 100:.1f}%削減)")
    elif size_diff < 0:
        print(f"  増加量: +{human_size_bytes(-size_diff)}")
    else:
        print(f"  変化なし")

    # [4/4] バックアップ
    print(f"\n  ─── [4/4] バックアップ ─────────────────────────")
    if not args.no_backup:
        do_backup(args.keep)
    else:
        _log("バックアップ: スキップ (--no-backup 指定)")

    elapsed = time.time() - t_total
    print(f"\n{'='*62}")
    print(f"  メンテナンス完了! (合計 {elapsed:.1f}秒)")
    print(f"{'='*62}\n")


if __name__ == "__main__":
    main()
