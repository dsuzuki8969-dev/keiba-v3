#!/usr/bin/env python3
"""
backfill_horses_2023h.py — race_log ↔ horses マスター不整合バックフィル

race_log に horse_id が存在するが horses マスターに未登録の馬を
netkeiba 馬詳細ページからスクレイピングして登録する。

主な対象:
  - 2023 年生まれ若駒 (Phase 1 集約時点で horse_name が空だった)
  - すべて JRA 形式 (10 桁数字) を想定

使い方:
    python scripts/backfill_horses_2023h.py --dry-run         # 件数のみ確認
    python scripts/backfill_horses_2023h.py --apply           # 本実行
    python scripts/backfill_horses_2023h.py --apply --max-fetch 50  # smoke test
"""

import argparse
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime
from typing import Dict, Optional, Tuple

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

from config.settings import CACHE_DIR, DATABASE_PATH
from src.log import get_logger
from src.scraper.netkeiba import (
    NetkeibaClient,
    HorseHistoryParser,
    PedigreeParser,
)
from src.models import Horse

logger = get_logger(__name__)

# JRA 会場コード (detect_is_jra 判定用)
JRA_VENUE_CODES = {
    "01", "02", "03", "04", "05",
    "06", "07", "08", "09", "10",
}

# バックアップ保存先
BACKUP_DIR = os.path.join(os.path.dirname(DATABASE_PATH), "backups")


# ─────────────────────────────────────────────────────────────
# ユーティリティ
# ─────────────────────────────────────────────────────────────

def _is_jra_horse_id(horse_id: str) -> bool:
    """10 桁数字なら JRA 形式と判断"""
    return horse_id.isdigit() and len(horse_id) == 10


def _backup_db(db_path: str) -> str:
    """DB をタイムスタンプ付きでバックアップし、パスを返す"""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(BACKUP_DIR, f"keiba_{ts}_pre_horses_backfill.db")
    shutil.copy2(db_path, dest)
    print(f"[バックアップ] {dest}")
    return dest


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


def _get_race_log_stats(conn: sqlite3.Connection, horse_ids: list[str]) -> Dict[str, dict]:
    """対象 horse_id ごとに race_log から統計情報を集計する"""
    if not horse_ids:
        return {}

    # SQLite の IN 句は 1000 件以下なら問題なし。分割は不要
    placeholders = ",".join("?" * len(horse_ids))
    rows = conn.execute(
        f"""
        SELECT
            horse_id,
            MAX(horse_name)  AS horse_name,
            MIN(race_date)   AS first_seen_date,
            MAX(race_date)   AS last_seen_date,
            COUNT(*)         AS race_count,
            GROUP_CONCAT(DISTINCT venue_code) AS venue_codes_csv
        FROM race_log
        WHERE horse_id IN ({placeholders})
        GROUP BY horse_id
        """,
        horse_ids,
    ).fetchall()

    result: Dict[str, dict] = {}
    for r in rows:
        result[r[0]] = {
            "horse_name_from_log": r[1] or "",
            "first_seen_date": r[2],
            "last_seen_date": r[3],
            "race_count": r[4],
            "venue_codes": set((r[5] or "").split(",")) if r[5] else set(),
        }
    return result


# ─────────────────────────────────────────────────────────────
# スクレイピング & 馬情報取得
# ─────────────────────────────────────────────────────────────

def _fetch_horse_info(
    client: NetkeibaClient,
    horse_id: str,
) -> Optional[Tuple[str, str, str, str, str, str, str, str]]:
    """
    netkeiba から馬名・血統・性別・生年・馬主を取得する。

    Returns:
        (horse_name, sire_name, dam_name, bms_name,
         birth_year_str, sex, color, owner)
        取得失敗時は None
    """
    # 馬オブジェクトを生成してスクレイパーに渡す
    # Horse は @dataclass で color/trainer/trainer_id/owner/breeder/sire/dam が必須引数
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

    # ──── 過去走ページ (horse/result/) から性別・馬名取得 ────
    history_parser = HorseHistoryParser(client)
    try:
        history_parser.parse(horse_id, horse)
    except Exception as e:
        logger.warning("HorseHistoryParser.parse 失敗 horse_id=%s: %s", horse_id, e)
        return None

    # 馬名は db.netkeiba.com/horse/<id>/ の h1 から取得を試みる
    # HorseHistoryParser.parse 内で _enrich_horse_profile_from_top が呼ばれているため
    # horse.horse_name は TOP ページ解析後に埋まる場合がある
    # TOP ページの h1 から直接取得する補完処理
    if not horse.horse_name:
        try:
            top_soup = client.get(f"https://db.netkeiba.com/horse/{horse_id}/")
            if top_soup:
                h1 = top_soup.select_one("div.horse_title h1")
                if not h1:
                    h1 = top_soup.select_one("h1")
                if h1:
                    horse.horse_name = h1.get_text(strip=True)
        except Exception as e:
            logger.warning("馬名 h1 取得失敗 horse_id=%s: %s", horse_id, e)

    # ──── 血統ページ (horse/ped/) から父・母・母父取得 ────
    ped_parser = PedigreeParser(client)
    sire_id, sire_name, dam_id, dam_name, mgs_id, mgs_name = ("", "", "", "", "", "")
    try:
        sire_id, sire_name, dam_id, dam_name, mgs_id, mgs_name = ped_parser.parse(
            horse_id, horse
        )
    except Exception as e:
        logger.warning("PedigreeParser.parse 失敗 horse_id=%s: %s", horse_id, e)

    # horse.sire / horse.dam / horse.maternal_grandsire に既に入っている場合もある
    # (PedigreeParser.parse で horse に直接セットされる)
    sire_name = sire_name or horse.sire or ""
    dam_name = dam_name or horse.dam or ""
    bms_name = mgs_name or horse.maternal_grandsire or ""

    # birth_year は Horse dataclass に存在しないため取得しない
    birth_year_str = ""

    # sex / color / owner は Horse dataclass の属性
    sex = horse.sex or ""
    color = horse.color or ""
    owner = horse.owner or ""

    return (
        horse.horse_name or "",
        sire_name or "",
        dam_name or "",
        bms_name or "",
        birth_year_str,
        sex,
        color,
        owner,
    )


# ─────────────────────────────────────────────────────────────
# dry-run
# ─────────────────────────────────────────────────────────────

def run_dry_run(conn: sqlite3.Connection) -> None:
    """対象件数と内訳のみ表示"""
    print("=" * 60)
    print("【dry-run】race_log ↔ horses 不整合バックフィル 件数確認")
    print("=" * 60)

    horse_ids = _get_target_horse_ids(conn)
    print(f"\n対象 horse_id: {len(horse_ids):,} 件")

    jra_ids = [h for h in horse_ids if _is_jra_horse_id(h)]
    nar_ids = [h for h in horse_ids if not _is_jra_horse_id(h)]
    print(f"  JRA 形式 (10 桁数字): {len(jra_ids):,} 件")
    print(f"  NAR 形式 (その他):    {len(nar_ids):,} 件")

    if nar_ids:
        print(f"\n  NAR ID サンプル: {nar_ids[:5]}")

    # race_log から horse_name があるもの vs 空のもの
    stats = _get_race_log_stats(conn, horse_ids)
    has_name = sum(1 for v in stats.values() if v["horse_name_from_log"])
    no_name = len(horse_ids) - has_name
    print(f"\nrace_log に horse_name あり: {has_name:,} 件")
    print(f"race_log に horse_name なし: {no_name:,} 件 (netkeiba から取得が必要)")

    print("\n[dry-run 完了] --apply で本実行してください")


# ─────────────────────────────────────────────────────────────
# 本実行
# ─────────────────────────────────────────────────────────────

def run_apply(
    conn: sqlite3.Connection,
    max_fetch: Optional[int] = None,
) -> None:
    """horses テーブルへのバックフィルを本実行する"""
    print("=" * 60)
    print("【apply】race_log ↔ horses バックフィル 開始")
    print("=" * 60)

    # ──── Step 1: 対象 horse_id 抽出 ────
    print("\n[1/5] 対象 horse_id 抽出中...")
    horse_ids = _get_target_horse_ids(conn)
    print(f"  対象件数: {len(horse_ids):,} 件")

    if max_fetch:
        horse_ids = horse_ids[:max_fetch]
        print(f"  --max-fetch {max_fetch} が指定されたため先頭 {max_fetch} 件に制限")

    if not horse_ids:
        print("\n対象馬なし。終了します。")
        return

    # ──── Step 2: race_log から統計情報を集計 ────
    print("\n[2/5] race_log から統計情報を集計中...")
    stats = _get_race_log_stats(conn, horse_ids)

    # ──── Step 3: netkeiba スクレイピング ────
    print(f"\n[3/5] netkeiba スクレイピング開始 ({len(horse_ids):,} 件, 1.0 秒間隔)")
    client = NetkeibaClient(
        cache_dir=CACHE_DIR,
        ignore_ttl=True,        # キャッシュがあれば TTL 無視で再利用
        request_interval=1.0,   # レート制限遵守
    )

    results: Dict[str, Optional[tuple]] = {}
    success_count = 0
    fail_count = 0
    skip_count = 0
    t_start = time.time()

    for i, horse_id in enumerate(horse_ids, 1):
        elapsed = time.time() - t_start
        pct = i / len(horse_ids) * 100
        # プログレスバー (10 件ごと or 最初・最後)
        if i == 1 or i == len(horse_ids) or i % 10 == 0:
            bar_filled = int(pct / 5)
            bar = "█" * bar_filled + "░" * (20 - bar_filled)
            remaining_sec = (elapsed / i) * (len(horse_ids) - i) if i > 0 else 0
            print(
                f"[{bar}] {pct:5.1f}% "
                f"({i}/{len(horse_ids)}) "
                f"経過{elapsed:.0f}s 残り約{remaining_sec:.0f}s "
                f"成功{success_count} 失敗{fail_count} スキップ{skip_count}"
            )

        # NAR 形式 (nar_xxx) は netkeiba スクレイピングではなく race_log から取得試行
        if not _is_jra_horse_id(horse_id):
            name_from_log = stats.get(horse_id, {}).get("horse_name_from_log", "")
            if not name_from_log:
                logger.warning(
                    "NAR horse_id=%s は race_log にも horse_name なし → スキップ",
                    horse_id,
                )
                skip_count += 1
                results[horse_id] = None
            else:
                # race_log の horse_name のみで登録 (血統は取得せず)
                results[horse_id] = (name_from_log, "", "", "", "", "", "", "")
                success_count += 1
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
            logger.warning("例外発生 horse_id=%s: %s → スキップ", horse_id, e, exc_info=True)
            fail_count += 1
            results[horse_id] = None

    total_elapsed = time.time() - t_start
    print(f"\nスクレイピング完了: 成功 {success_count} / 失敗 {fail_count} / スキップ {skip_count} / 経過 {total_elapsed:.1f}秒")

    # ──── Step 4: horses テーブルへ INSERT ────
    print("\n[4/5] horses テーブルへ INSERT OR IGNORE...")
    inserted = 0
    skipped_no_name = 0

    for horse_id, info in results.items():
        if info is None:
            # 取得失敗: 推定で埋めない。完全スキップ
            skipped_no_name += 1
            continue

        (horse_name, sire_name, dam_name, bms_name,
         birth_year_str, sex, color, owner) = info

        # horse_name が取れなかった場合も race_log の値でフォールバックを試みる
        if not horse_name:
            horse_name = stats.get(horse_id, {}).get("horse_name_from_log", "") or ""

        if not horse_name:
            # 推定・フィクション禁止: horse_name なしは登録しない
            logger.warning(
                "horse_name が取得できなかった horse_id=%s → 登録スキップ（推定で書き換えない）",
                horse_id,
            )
            skipped_no_name += 1
            continue

        stat = stats.get(horse_id, {})
        venue_codes = stat.get("venue_codes", set())
        first_seen = stat.get("first_seen_date")
        last_seen = stat.get("last_seen_date")
        race_count = stat.get("race_count", 0)

        # is_jra 判定
        if _is_jra_horse_id(horse_id):
            is_jra = 1 if (venue_codes & JRA_VENUE_CODES) else 1  # 10 桁数字は JRA 優先
        else:
            is_jra = 0

        # birth_year を int に変換
        birth_year: Optional[int] = None
        if birth_year_str and birth_year_str.isdigit():
            birth_year = int(birth_year_str)

        # netkeiba_id: JRA 10 桁数字なら horse_id 自身
        netkeiba_id = horse_id if _is_jra_horse_id(horse_id) else None

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
                None,   # breeder: このスクリプトでは取得しない
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

    # ──── Step 5: 検証 ────
    print("\n[5/5] 検証中...")

    # 修正後不整合件数
    remaining = conn.execute(
        """
        SELECT COUNT(DISTINCT r.horse_id)
        FROM race_log r
        LEFT JOIN horses h ON r.horse_id = h.horse_id
        WHERE r.horse_id IS NOT NULL
          AND r.horse_id != ''
          AND h.horse_id IS NULL
        """
    ).fetchone()[0]
    print(f"  修正後 race_log ↔ horses 不整合件数: {remaining:,} 件")

    # サンプル 5 件
    print("\n  サンプル 5 件 (新規登録馬):")
    if not horse_ids:
        print("    (対象なし)")
    else:
        sample_ids = [
            hid for hid in horse_ids[:20]
            if results.get(hid) is not None
        ][:5]
        for sid in sample_ids:
            row = conn.execute(
                """
                SELECT horse_id, horse_name, sire_name, dam_name, bms_name,
                       birth_year, sex, is_jra
                FROM horses WHERE horse_id = ?
                """,
                (sid,),
            ).fetchone()
            if row:
                print(
                    f"    horse_id={row[0]} name={row[1]} "
                    f"父={row[2]} 母={row[3]} 母父={row[4]} "
                    f"生年={row[5]} 性={row[6]} JRA={row[7]}"
                )
            else:
                print(f"    horse_id={sid} → horses に未登録（スキップ済み）")

    # 残存スキップ馬の方針
    skipped_ids = [hid for hid, r in results.items() if r is None]
    if skipped_ids:
        print(f"\n  スキップされた horse_id: {len(skipped_ids):,} 件")
        print("  ─ 今後の方針 ─")
        print("    1. netkeiba に 404 が返った馬は当該ページが削除済みの可能性がある")
        print("    2. race_log に horse_name が入ったタイミングで再実行すれば登録可能")
        print("    3. 必要な場合は JRA 公式 (official_odds.py) から補完を検討")
        if len(skipped_ids) <= 20:
            print(f"    スキップ IDs: {skipped_ids}")
        else:
            print(f"    スキップ IDs (先頭 20 件): {skipped_ids[:20]}")

    print("\n【完了】backfill_horses_2023h.py 終了")


# ─────────────────────────────────────────────────────────────
# エントリーポイント
# ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="race_log ↔ horses 不整合をバックフィル (netkeiba 馬詳細から取得)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="件数のみ表示し DB への書き込みは行わない",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="実際に horses テーブルへ INSERT する",
    )
    parser.add_argument(
        "--max-fetch",
        type=int,
        default=None,
        metavar="N",
        help="取得上限 (smoke test 用。例: --max-fetch 50)",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.error("--dry-run または --apply を指定してください")

    # DB 接続
    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row

    try:
        if args.dry_run:
            run_dry_run(conn)
        elif args.apply:
            # バックアップ取得 (失敗したら本実行禁止)
            try:
                _backup_db(DATABASE_PATH)
            except Exception as e:
                print(f"[ERROR] バックアップ取得失敗: {e}")
                print("バックアップなしでの本実行は禁止されています。終了します。")
                sys.exit(1)

            run_apply(conn, max_fetch=args.max_fetch)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
