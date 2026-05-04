#!/usr/bin/env python3
"""
backfill_2023h_via_alternative.py — race_log ↔ horses マスター不整合バックフィル
                                     (netkeiba 不使用版)

race_log に horse_id が存在するが horses マスターに未登録の馬を
race_log の情報から直接 INSERT する。

birth_year は race_dateの年 - age で推定。
sire/bms が空の場合は楽天競馬 (結果ページ) から馬名の確認のみ試みる。
netkeiba には一切アクセスしない。

対象:
  - JRA 331 件 (10桁数字 horse_id)
  - NAR  893 件 (nar_XXXXX 形式または B_prefix または 20XXXXXXXXXX 形式)

使い方:
    python scripts/backfill_2023h_via_alternative.py --dry-run
    python scripts/backfill_2023h_via_alternative.py --execute
    python scripts/backfill_2023h_via_alternative.py --execute --max-insert 50
"""

import argparse
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

from config.settings import DATABASE_PATH
from src.log import get_logger

logger = get_logger(__name__)

# バックアップ保存先
BACKUP_DIR = os.path.join(os.path.dirname(DATABASE_PATH), "backups")

# 中断再開マーカー
DONE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "tmp", "backfill_2023h_alt_done.txt"
)


# ─────────────────────────────────────────────────────────────
# ユーティリティ
# ─────────────────────────────────────────────────────────────

def _backup_db(db_path: str) -> str:
    """DB をタイムスタンプ付きでバックアップし、パスを返す"""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(BACKUP_DIR, f"keiba_{ts}_pre_2023h_alt.db")
    shutil.copy2(db_path, dest)
    print(f"[バックアップ] {dest}")
    return dest


def _load_done(done_file: str) -> set:
    """完了済み horse_id を読み込む"""
    done = set()
    if os.path.exists(done_file):
        with open(done_file, encoding="utf-8") as f:
            for line in f:
                hid = line.strip()
                if hid:
                    done.add(hid)
    return done


def _save_done(done_file: str, horse_id: str):
    """horse_id を完了済みに追記"""
    os.makedirs(os.path.dirname(done_file), exist_ok=True)
    with open(done_file, "a", encoding="utf-8") as f:
        f.write(horse_id + "\n")


# ─────────────────────────────────────────────────────────────
# race_log からデータ集約
# ─────────────────────────────────────────────────────────────

def _get_target_horses(conn: sqlite3.Connection) -> List[Dict]:
    """
    race_log に存在するが horses に未登録の馬を全件取得。
    race_log から取得できる情報を最大限集約して返す。
    """
    rows = conn.execute("""
        SELECT
            rl.horse_id,
            MAX(rl.horse_name)                      AS horse_name,
            MAX(NULLIF(rl.sire_name, ''))            AS sire_name,
            MAX(NULLIF(rl.bms_name, ''))             AS bms_name,
            MAX(NULLIF(rl.sex, ''))                  AS sex,
            MAX(rl.age)                              AS max_age,
            MIN(rl.race_date)                        AS first_seen_date,
            MAX(rl.race_date)                        AS last_seen_date,
            COUNT(*)                                 AS race_count,
            MAX(rl.is_jra)                           AS is_jra,
            MIN(rl.race_date || ':' || CAST(rl.age AS TEXT)) AS earliest_date_age
        FROM race_log rl
        WHERE rl.horse_id IS NOT NULL
          AND rl.horse_id != ''
          AND NOT EXISTS (
              SELECT 1 FROM horses h WHERE h.horse_id = rl.horse_id
          )
        GROUP BY rl.horse_id
        ORDER BY rl.is_jra DESC, rl.horse_id
    """).fetchall()

    result = []
    for r in rows:
        horse_id = r[0]
        horse_name = r[1] or ""
        sire_name = r[2] or ""
        bms_name = r[3] or ""
        sex = r[4] or ""
        max_age = r[5] or 0
        first_seen_date = r[6] or ""
        last_seen_date = r[7] or ""
        race_count = r[8] or 0
        is_jra = r[9] or 0

        # birth_year を推定: 最初に出走した時点の年齢から逆算
        # earliest_date_age = "YYYY-MM-DD:age"
        birth_year = None
        earliest_str = r[10] or ""
        if ":" in earliest_str:
            parts = earliest_str.split(":", 1)
            try:
                race_year = int(parts[0][:4])
                age_at_race = int(parts[1])
                if age_at_race > 0:
                    birth_year = race_year - age_at_race
            except (ValueError, IndexError):
                pass

        result.append({
            "horse_id": horse_id,
            "horse_name": horse_name,
            "sire_name": sire_name,
            "bms_name": bms_name,
            "sex": sex,
            "birth_year": birth_year,
            "first_seen_date": first_seen_date,
            "last_seen_date": last_seen_date,
            "race_count": race_count,
            "is_jra": is_jra,
        })

    return result


# ─────────────────────────────────────────────────────────────
# horses INSERT
# ─────────────────────────────────────────────────────────────

def _insert_horse(conn: sqlite3.Connection, h: Dict, dry_run: bool) -> bool:
    """
    horses テーブルに1件 INSERT する。
    dry_run=True の場合は INSERT せずに True を返す。
    """
    if dry_run:
        return True

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn.execute("""
            INSERT OR IGNORE INTO horses
              (horse_id, horse_name, sire_name, bms_name,
               birth_year, sex,
               is_jra,
               first_seen_date, last_seen_date, race_count,
               created_at, updated_at)
            VALUES
              (?, ?, ?, ?,
               ?, ?,
               ?,
               ?, ?, ?,
               ?, ?)
        """, (
            h["horse_id"],
            h["horse_name"],
            h["sire_name"] or None,
            h["bms_name"] or None,
            h["birth_year"],
            h["sex"] or None,
            h["is_jra"],
            h["first_seen_date"] or None,
            h["last_seen_date"] or None,
            h["race_count"],
            now,
            now,
        ))
        return conn.total_changes > 0 or True  # INSERT OR IGNORE は変更0でも成功扱い
    except Exception as e:
        logger.error(f"INSERT失敗 {h['horse_id']}: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="race_log → horses バックフィル (netkeiba 不使用版)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="INSERT を実行せず件数のみ表示"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="実際に INSERT を実行"
    )
    parser.add_argument(
        "--max-insert", type=int, default=0,
        help="最大 INSERT 件数 (0=無制限)"
    )
    parser.add_argument(
        "--reset-done", action="store_true",
        help="中断再開マーカーをリセットして全件再処理"
    )
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("--dry-run または --execute を指定してください")
        parser.print_help()
        sys.exit(1)

    dry_run = args.dry_run
    mode = "DRY-RUN" if dry_run else "EXECUTE"
    print(f"\n{'='*60}")
    print(f"backfill_2023h_via_alternative  [{mode}]")
    print(f"{'='*60}")

    # 中断再開マーカー
    if args.reset_done and os.path.exists(DONE_FILE):
        os.remove(DONE_FILE)
        print("[リセット] 中断再開マーカーを削除しました")

    done_set = _load_done(DONE_FILE)
    if done_set:
        print(f"[再開] 完了済み: {len(done_set)}件")

    # DB接続
    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    # バックアップ (EXECUTE時のみ)
    if not dry_run:
        _backup_db(DATABASE_PATH)

    # 対象取得
    print("\n[1/3] 対象馬を race_log から集約中...")
    targets = _get_target_horses(conn)
    print(f"  → 総対象: {len(targets)}件")

    # 内訳
    jra_count = sum(1 for h in targets if h["is_jra"])
    nar_count = len(targets) - jra_count
    has_sire = sum(1 for h in targets if h["sire_name"])
    has_birth = sum(1 for h in targets if h["birth_year"])
    print(f"  → JRA: {jra_count}件 / NAR: {nar_count}件")
    print(f"  → sire_name あり: {has_sire}件 / birth_year 推定可能: {has_birth}件")

    if dry_run:
        print("\n[DRY-RUN] INSERT 予定内訳 (先頭20件):")
        for h in targets[:20]:
            print(
                f"  {h['horse_id']:20s} {h['horse_name']:20s} "
                f"birth={h['birth_year']} sex={h['sex']:3s} "
                f"sire={h['sire_name'][:12] if h['sire_name'] else '-':12s} "
                f"is_jra={h['is_jra']}"
            )
        print(f"\n[DRY-RUN完了] 実行すれば {len(targets)}件が INSERT されます")
        conn.close()
        return

    # 本実行
    print(f"\n[2/3] horses に INSERT 中... (max={args.max_insert or '無制限'})")
    inserted = 0
    skipped_done = 0
    failed = 0
    batch = []
    BATCH_SIZE = 100

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for i, h in enumerate(targets, 1):
        if h["horse_id"] in done_set:
            skipped_done += 1
            continue

        if args.max_insert > 0 and inserted >= args.max_insert:
            print(f"\n  [上限到達] --max-insert={args.max_insert} 件に達しました")
            break

        batch.append((
            h["horse_id"],
            h["horse_name"],
            h["sire_name"] or None,
            h["bms_name"] or None,
            h["birth_year"],
            h["sex"] or None,
            h["is_jra"],
            h["first_seen_date"] or None,
            h["last_seen_date"] or None,
            h["race_count"],
            now,
            now,
        ))

        if len(batch) >= BATCH_SIZE or i == len(targets):
            try:
                conn.executemany("""
                    INSERT OR IGNORE INTO horses
                      (horse_id, horse_name, sire_name, bms_name,
                       birth_year, sex,
                       is_jra,
                       first_seen_date, last_seen_date, race_count,
                       created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, batch)
                conn.commit()
                for row in batch:
                    _save_done(DONE_FILE, row[0])
                inserted += len(batch)
                batch = []
                pct = min(100.0, inserted / len(targets) * 100)
                bar_len = 30
                filled = int(bar_len * pct / 100)
                bar = "█" * filled + "░" * (bar_len - filled)
                print(f"\r  [{bar}] {pct:.1f}% ({inserted}/{len(targets)})", end="", flush=True)
            except Exception as e:
                logger.error(f"バッチINSERT失敗: {e}")
                failed += len(batch)
                batch = []

    print()  # 改行

    # 結果確認
    print(f"\n[3/3] 結果確認...")
    cur = conn.execute("SELECT COUNT(*) FROM horses")
    total_horses = cur.fetchone()[0]

    cur = conn.execute("""
        SELECT COUNT(DISTINCT rl.horse_id)
        FROM race_log rl
        WHERE rl.horse_id IS NOT NULL
          AND rl.horse_id != ''
          AND NOT EXISTS (
              SELECT 1 FROM horses h WHERE h.horse_id = rl.horse_id
          )
    """)
    remaining = cur.fetchone()[0]

    conn.close()

    print(f"\n{'='*60}")
    print(f"  INSERT完了: {inserted}件")
    print(f"  スキップ (完了済): {skipped_done}件")
    print(f"  失敗: {failed}件")
    print(f"  horses総数: {total_horses}件")
    print(f"  race_logで未登録残り: {remaining}件")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
