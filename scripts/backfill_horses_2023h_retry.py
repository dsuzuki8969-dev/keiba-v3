#!/usr/bin/env python3
"""
backfill_horses_2023h_retry.py — 2023 年生まれ若駒 race_log ↔ horses 不整合バックフィル

backfill_horses_2023h.py の安全装置強化版。
netkeiba 403 エラーで失敗した 2023 年生まれの若駒 (現在 race_log ↔ horses 不整合 568 件)
を再試行する。

既存スクリプトとの違い:
  - request_interval を 2.0 秒以上に強制 (元スクリプトは 1.0 秒 → 違反歴に抵触)
  - 危険時間帯チェック (06:00-06:30 / 22:00-23:30) を追加
  - 競合プロセスチェックを追加
  - --execute 必須 (省略時は --dry-run として動作)
  - 中断再開対応 (tmp/backfill_2023h_retry_done.txt)

対象:
  race_log に horse_id があるが horses テーブルに未登録の馬
  (JRA 10 桁形式: 現在約 422 件 / NAR nar_prefix: 現在約 223 件)

使い方:
    # 件数と推定所要時間のみ確認 (DB 変更なし)
    python scripts/backfill_horses_2023h_retry.py --dry-run

    # 本実行 (マスター起床後・T-063b 完了後・B_prefix 完了後に実行)
    python scripts/backfill_horses_2023h_retry.py --execute

    # smoke test (先頭 10 件のみ)
    python scripts/backfill_horses_2023h_retry.py --execute --max-fetch 10

推定所要時間: 568 件 × 2.0 秒 = 約 19 分
  (JRA 422 件: netkeiba 取得, NAR 223 件: race_log horse_name のみ)
"""

from __future__ import annotations

import argparse
import io
import shutil
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── UTF-8 出力 (Windows 対応) ─────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DATABASE_PATH, CACHE_DIR
from src.log import get_logger
from src.scraper.netkeiba_checks import assert_safe_to_proceed

logger = get_logger(__name__)

# ── 定数 ──────────────────────────────────────────────────────────────────────
# レート制限: 2.0 秒 / 件 (絶対厳守)
RATE_LIMIT_SEC = 2.0

# 中断再開マーカー
DONE_MARKER_FILE = Path(__file__).resolve().parent.parent / "tmp" / "backfill_2023h_retry_done.txt"

# バックアップ保存先
BACKUP_DIR = Path(DATABASE_PATH).parent / "backups"

# JRA 会場コード
JRA_VENUE_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}

# ── ユーティリティ ─────────────────────────────────────────────────────────────

def _is_jra_horse_id(horse_id: str) -> bool:
    return horse_id.isdigit() and len(horse_id) == 10


def _load_done_ids() -> set[str]:
    if not DONE_MARKER_FILE.exists():
        return set()
    done = set()
    with open(DONE_MARKER_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                done.add(line)
    return done


def _mark_done(horse_id: str) -> None:
    DONE_MARKER_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DONE_MARKER_FILE, "a", encoding="utf-8") as f:
        f.write(horse_id + "\n")


def _backup_db() -> str:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = BACKUP_DIR / f"keiba_{ts}_pre_2023h_retry.db"
    shutil.copy2(DATABASE_PATH, str(dest))
    print(f"[バックアップ] {dest}")
    return str(dest)


def _progress_bar(done: int, total: int, width: int = 25) -> str:
    if total <= 0:
        return f"[{'?' * width}] ?%"
    pct = done / total * 100
    filled = int(width * done / total)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {pct:.1f}% ({done:,}/{total:,})"


# ── DB 操作 ───────────────────────────────────────────────────────────────────

def _get_target_horse_ids(conn: sqlite3.Connection) -> list[str]:
    """race_log に存在するが horses に未登録の horse_id を取得"""
    rows = conn.execute(
        """
        SELECT DISTINCT r.horse_id
        FROM race_log r
        LEFT JOIN horses h ON r.horse_id = h.horse_id
        WHERE r.horse_id IS NOT NULL
          AND r.horse_id != ''
          AND h.horse_id IS NULL
        ORDER BY r.horse_id
        """
    ).fetchall()
    return [row[0] for row in rows]


def _get_race_log_stats(conn: sqlite3.Connection, horse_ids: list[str]) -> dict:
    if not horse_ids:
        return {}
    placeholders = ",".join("?" * len(horse_ids))
    rows = conn.execute(
        f"""
        SELECT
            horse_id,
            MAX(horse_name) AS horse_name,
            MIN(race_date)  AS first_seen_date,
            MAX(race_date)  AS last_seen_date,
            COUNT(*)        AS race_count,
            GROUP_CONCAT(DISTINCT venue_code) AS venue_codes_csv
        FROM race_log
        WHERE horse_id IN ({placeholders})
        GROUP BY horse_id
        """,
        horse_ids,
    ).fetchall()
    result = {}
    for r in rows:
        result[r[0]] = {
            "horse_name_from_log": r[1] or "",
            "first_seen_date": r[2],
            "last_seen_date": r[3],
            "race_count": r[4],
            "venue_codes": set((r[5] or "").split(",")) if r[5] else set(),
        }
    return result


# ── netkeiba スクレイピング ────────────────────────────────────────────────────

def _fetch_horse_info(client, horse_id: str) -> Optional[tuple]:
    """
    netkeiba から馬情報を取得する。
    元スクリプト (backfill_horses_2023h.py) の _fetch_horse_info と同じロジック。
    Returns: (horse_name, sire_name, dam_name, bms_name, birth_year_str, sex, color, owner) or None
    """
    from src.scraper.netkeiba import HorseHistoryParser, PedigreeParser
    from src.models import Horse

    horse = Horse(
        horse_id=horse_id,
        horse_name="",
        sex="",
        age=0,
        color="",
        trainer="",
        trainer_id="",
        owner="",
        breeder="",
        sire="",
        dam="",
    )

    history_parser = HorseHistoryParser(client)
    try:
        history_parser.parse(horse_id, horse)
    except Exception as e:
        logger.warning("HorseHistoryParser 失敗 horse_id=%s: %s", horse_id, e)
        return None

    if not horse.horse_name:
        try:
            top_soup = client.get(f"https://db.netkeiba.com/horse/{horse_id}/")
            if top_soup:
                h1 = top_soup.select_one("div.horse_title h1") or top_soup.select_one("h1")
                if h1:
                    horse.horse_name = h1.get_text(strip=True)
        except Exception as e:
            logger.warning("馬名 h1 取得失敗 horse_id=%s: %s", horse_id, e)

    ped_parser = PedigreeParser(client)
    sire_name = dam_name = bms_name = ""
    try:
        sire_id, sire_name, dam_id, dam_name, mgs_id, mgs_name = ped_parser.parse(horse_id, horse)
        sire_name = sire_name or horse.sire or ""
        dam_name = dam_name or horse.dam or ""
        bms_name = mgs_name or horse.maternal_grandsire or ""
    except Exception as e:
        logger.warning("PedigreeParser 失敗 horse_id=%s: %s", horse_id, e)

    return (
        horse.horse_name or "",
        sire_name or "",
        dam_name or "",
        bms_name or "",
        "",  # birth_year: このスクリプトでは取得しない
        horse.sex or "",
        horse.color or "",
        horse.owner or "",
    )


# ── dry-run ───────────────────────────────────────────────────────────────────

def run_dry_run(conn: sqlite3.Connection) -> None:
    """対象件数と推定所要時間のみ表示"""
    print("=" * 65)
    print("【dry-run】2023 年生まれ若駒 race_log ↔ horses 不整合バックフィル")
    print("=" * 65)

    horse_ids = _get_target_horse_ids(conn)
    print(f"\n対象 horse_id: {len(horse_ids):,} 件 (race_log に存在, horses に未登録)")

    jra_ids = [h for h in horse_ids if _is_jra_horse_id(h)]
    nar_ids = [h for h in horse_ids if not _is_jra_horse_id(h)]
    print(f"  JRA 形式 (10 桁数字): {len(jra_ids):,} 件 → netkeiba 取得")
    print(f"  NAR 形式 (その他):    {len(nar_ids):,} 件 → race_log horse_name から登録")

    # 処理済みマーカー
    done_ids = _load_done_ids()
    remaining = [h for h in horse_ids if h not in done_ids]
    print(f"\n  処理済マーカー: {len(done_ids):,} 件 (スキップ)")
    print(f"  実処理対象: {len(remaining):,} 件")

    if nar_ids:
        print(f"\n  NAR ID サンプル: {nar_ids[:5]}")

    # race_log の horse_name
    stats = _get_race_log_stats(conn, remaining)
    has_name = sum(1 for v in stats.values() if v["horse_name_from_log"])
    no_name = len(remaining) - has_name
    print(f"\nrace_log に horse_name あり: {has_name:,} 件")
    print(f"race_log に horse_name なし: {no_name:,} 件 (スキップ予定)")

    # 推定所要時間
    jra_remaining = [h for h in remaining if _is_jra_horse_id(h)]
    nar_remaining = [h for h in remaining if not _is_jra_horse_id(h)]
    est_sec = len(jra_remaining) * RATE_LIMIT_SEC + len(nar_remaining) * 0.1
    est_min = est_sec / 60
    print(f"\n推定所要時間: 約 {est_min:.0f} 分")
    print(f"  JRA {len(jra_remaining):,} 件 × {RATE_LIMIT_SEC}秒 = {len(jra_remaining) * RATE_LIMIT_SEC / 60:.0f} 分")
    print(f"  NAR {len(nar_remaining):,} 件 × 0.1秒  = {len(nar_remaining) * 0.1 / 60:.1f} 分")

    try:
        assert_safe_to_proceed(force=False)
        print(f"\n[安全] 現在は実行可能時間帯です。")
    except RuntimeError as e:
        print(f"\n[警告] {e}")

    print(f"\n実行コマンド:")
    print(f"  python scripts/backfill_horses_2023h_retry.py --execute")
    print(f"\n[dry-run 完了] DB への書き込みは行っていません。")


# ── 本実行 ────────────────────────────────────────────────────────────────────

def run_execute(conn: sqlite3.Connection, max_fetch: int | None = None) -> None:
    """race_log ↔ horses 不整合を解消する本実行"""
    print("=" * 65)
    print("【execute】2023 年生まれ若駒 horses バックフィル 開始")
    print("=" * 65)

    # ──── 安全チェック ────
    try:
        assert_safe_to_proceed(force=False)
    except RuntimeError as e:
        print(str(e))
        sys.exit(1)

    # ──── Step 1: 対象抽出 ────
    print("\n[1/5] 対象 horse_id 抽出中...")
    all_horse_ids = _get_target_horse_ids(conn)
    done_ids = _load_done_ids()
    horse_ids = [h for h in all_horse_ids if h not in done_ids]
    print(f"  全対象: {len(all_horse_ids):,} 件")
    print(f"  処理済スキップ: {len(done_ids):,} 件")
    print(f"  実処理: {len(horse_ids):,} 件")

    if max_fetch:
        horse_ids = horse_ids[:max_fetch]
        print(f"  --max-fetch {max_fetch} 件に制限")

    if not horse_ids:
        print("\n処理対象なし。終了します。")
        return

    # ──── Step 2: race_log から統計情報 ────
    print("\n[2/5] race_log から統計情報を集計中...")
    stats = _get_race_log_stats(conn, horse_ids)

    # ──── Step 3: バックアップ ────
    print("\n[3/5] DB バックアップ取得...")
    try:
        _backup_db()
    except Exception as e:
        print(f"[ERROR] バックアップ失敗: {e}")
        sys.exit(1)

    # ──── Step 4: スクレイピング ────
    print(f"\n[4/5] netkeiba スクレイピング開始 ({len(horse_ids):,} 件, {RATE_LIMIT_SEC} 秒間隔)")

    from src.scraper.netkeiba import NetkeibaClient

    client = NetkeibaClient(
        cache_dir=CACHE_DIR,
        ignore_ttl=True,
        request_interval=RATE_LIMIT_SEC,  # 2.0 秒強制
    )

    results: dict[str, Optional[tuple]] = {}
    success_count = 0
    fail_count = 0
    skip_count = 0
    t_start = time.time()

    for i, horse_id in enumerate(horse_ids, 1):
        elapsed = time.time() - t_start
        pct = i / len(horse_ids) * 100

        if i == 1 or i == len(horse_ids) or i % 20 == 0:
            bar = _progress_bar(i, len(horse_ids))
            remaining_sec = (elapsed / i) * (len(horse_ids) - i) if i > 0 else 0
            print(
                f"{bar} "
                f"経過{elapsed:.0f}s 残り約{remaining_sec:.0f}s "
                f"成功{success_count} 失敗{fail_count} スキップ{skip_count}"
            )

        # NAR 形式: race_log の horse_name から登録
        if not _is_jra_horse_id(horse_id):
            name_from_log = stats.get(horse_id, {}).get("horse_name_from_log", "")
            if not name_from_log:
                logger.warning("NAR horse_id=%s race_log に horse_name なし → スキップ", horse_id)
                skip_count += 1
                results[horse_id] = None
            else:
                results[horse_id] = (name_from_log, "", "", "", "", "", "", "")
                success_count += 1
            _mark_done(horse_id)
            continue

        # JRA 形式: netkeiba から取得
        try:
            info = _fetch_horse_info(client, horse_id)
            if info is None:
                logger.warning("取得失敗 horse_id=%s → スキップ", horse_id)
                fail_count += 1
                results[horse_id] = None
            else:
                results[horse_id] = info
                success_count += 1
        except Exception as e:
            logger.warning("例外発生 horse_id=%s: %s", horse_id, e, exc_info=True)
            fail_count += 1
            results[horse_id] = None

        _mark_done(horse_id)

    total_elapsed = time.time() - t_start
    print(f"\nスクレイピング完了: 成功{success_count} / 失敗{fail_count} / スキップ{skip_count} / {total_elapsed:.1f}秒")

    # ──── Step 5: horses テーブルへ INSERT ────
    print("\n[5/5] horses テーブルへ INSERT OR IGNORE...")
    inserted = 0
    skipped_no_name = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for horse_id, info in results.items():
        if info is None:
            skipped_no_name += 1
            continue

        (horse_name, sire_name, dam_name, bms_name,
         birth_year_str, sex, color, owner) = info

        if not horse_name:
            horse_name = stats.get(horse_id, {}).get("horse_name_from_log", "") or ""

        if not horse_name:
            logger.warning("horse_name なし horse_id=%s → 登録スキップ", horse_id)
            skipped_no_name += 1
            continue

        stat = stats.get(horse_id, {})
        venue_codes = stat.get("venue_codes", set())
        first_seen = stat.get("first_seen_date")
        last_seen = stat.get("last_seen_date")
        race_count = stat.get("race_count", 0)

        is_jra = 1 if _is_jra_horse_id(horse_id) else 0
        birth_year = None
        if birth_year_str and birth_year_str.isdigit():
            birth_year = int(birth_year_str)
        netkeiba_id = horse_id if _is_jra_horse_id(horse_id) else None

        conn.execute(
            """
            INSERT OR IGNORE INTO horses (
                horse_id, horse_name, sire_name, dam_name, bms_name,
                birth_year, sex, color, breeder, owner,
                is_jra, first_seen_date, last_seen_date, race_count,
                created_at, updated_at, netkeiba_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                horse_id,
                horse_name.strip(),
                sire_name.strip() if sire_name else None,
                dam_name.strip() if dam_name else None,
                bms_name.strip() if bms_name else None,
                birth_year,
                sex.strip() if sex else None,
                color.strip() if color else None,
                None,
                owner.strip() if owner else None,
                is_jra,
                first_seen,
                last_seen,
                race_count,
                now_str,
                now_str,
                netkeiba_id,
            ),
        )
        inserted += 1

    conn.commit()
    print(f"  INSERT 完了: {inserted:,} 件 / スキップ (horse_name 空): {skipped_no_name:,} 件")

    # 検証
    remaining = conn.execute(
        """
        SELECT COUNT(DISTINCT r.horse_id)
        FROM race_log r
        LEFT JOIN horses h ON r.horse_id = h.horse_id
        WHERE r.horse_id IS NOT NULL AND r.horse_id != ''
        AND h.horse_id IS NULL
        """
    ).fetchone()[0]
    print(f"\n  修正後 race_log ↔ horses 不整合件数: {remaining:,} 件")
    print(f"\n[完了] backfill_horses_2023h_retry.py 終了")
    print(f"  中断再開マーカー: {DONE_MARKER_FILE}")


# ── エントリーポイント ─────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="race_log ↔ horses 不整合バックフィル (2.0 秒レート制限・安全装置付き)"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="件数のみ表示 (DB 変更なし)")
    mode.add_argument("--execute", action="store_true", help="本実行 (DB への INSERT あり)")
    parser.add_argument(
        "--max-fetch", type=int, default=None, metavar="N",
        help="処理上限 (smoke test 用。例: --max-fetch 10)",
    )
    parser.add_argument(
        "--reset-marker", action="store_true",
        help="中断再開マーカーをリセットして全件再処理",
    )
    args = parser.parse_args()

    if not args.execute:
        args.dry_run = True

    if args.reset_marker:
        if args.dry_run:
            parser.error("--reset-marker は --execute と組み合わせてください")
        if DONE_MARKER_FILE.exists():
            DONE_MARKER_FILE.unlink()
            print(f"[マーカーリセット] {DONE_MARKER_FILE}")

    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row

    try:
        if args.dry_run:
            run_dry_run(conn)
        else:
            run_execute(conn, max_fetch=args.max_fetch)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
