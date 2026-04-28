#!/usr/bin/env python3
"""
2023年下半期 (2023-07-01〜2023-12-31) の race_log バックフィル。

JRA 約 1,944 R + NAR 約 12,505 R の合計 14,449 R を対象に:
  1. kaisai_calendar.json から対象日を列挙
  2. RaceListFetcher で日別 race_id を取得 (未キャッシュ時は netkeiba へ)
  3. result.html を取得・キャッシュ
  4. parse_result_page() で解析して race_log へ INSERT
  5. 100 件ごとバッチコミット

CLI:
  python scripts/backfill_race_log_2023h2.py --dry-run
  python scripts/backfill_race_log_2023h2.py --apply
  python scripts/backfill_race_log_2023h2.py --apply --max-fetch 100

PYTHONIOENCODING=utf-8 必須。
"""
from __future__ import annotations

import argparse
import io
import json
import re
import shutil
import sys
import time
from datetime import date as _date_cls
from pathlib import Path

# ──────────────────────────────────────────
# stdout/stderr を UTF-8 に統一（Windows 対応）
# ──────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lz4.frame  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402

from config.settings import CACHE_DIR, DATABASE_PATH  # noqa: E402
from data.masters.venue_master import JRA_VENUE_CODES  # noqa: E402
from src.database import get_db, init_schema  # noqa: E402
from src.scraper.kaisai_calendar_util import get_open_dates  # noqa: E402
from src.scraper.ml_data_collector import parse_result_page  # noqa: E402
from src.scraper.netkeiba import NetkeibaClient, RaceListScraper  # noqa: E402

# ──────────────────────────────────────────
# 定数
# ──────────────────────────────────────────
PERIOD_START = "2023-07-01"
PERIOD_END   = "2023-12-31"
BATCH_SIZE   = 100          # N 件ごとに DB コミット
LOG_INTERVAL = 100          # N 件ごとにログ出力
LOG_DIR      = Path(__file__).resolve().parent.parent / "log"
LOG_PATH     = LOG_DIR / "backfill_2023h2.log"
BACKUP_SUFFIX = "bak_pre_b_20260428"


# ──────────────────────────────────────────
# ログユーティリティ
# ──────────────────────────────────────────
def _ensure_log_dir() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def log(msg: str, also_file: bool = True) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if also_file:
        try:
            _ensure_log_dir()
            with open(LOG_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass


def progress_bar(done: int, total: int, width: int = 30) -> str:
    """テキストプログレスバーを返す。"""
    if total <= 0:
        return f"[{'?' * width}] ?%"
    pct = done / total * 100
    filled = int(width * done / total)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {pct:.1f}% ({done:,}/{total:,})"


# ──────────────────────────────────────────
# キャッシュ読み書き
# ──────────────────────────────────────────
def _cache_path(race_id: str) -> Path:
    """race_result HTML のキャッシュパスを返す (lz4 圧縮)。"""
    # JRA: race.netkeiba.com / NAR: nar.netkeiba.com
    vc = race_id[4:6] if len(race_id) >= 6 else ""
    domain = "race.netkeiba.com" if vc in JRA_VENUE_CODES else "nar.netkeiba.com"
    fname = f"{domain}_race_result.html_race_id={race_id}.html.lz4"
    return Path(CACHE_DIR) / fname


def _read_cache(race_id: str) -> str | None:
    """キャッシュがあれば HTML 文字列を返す。なければ None。"""
    p = _cache_path(race_id)
    if p.exists():
        try:
            with lz4.frame.open(str(p), "rb") as f:
                return f.read().decode("utf-8", errors="replace")
        except Exception:
            return None
    # .lz4 なし → 非圧縮版を試す
    p2 = Path(str(p).removesuffix(".lz4"))
    if p2.exists():
        try:
            return p2.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return None
    return None


def _write_cache(race_id: str, html: str) -> None:
    """HTML をキャッシュに保存する (lz4 圧縮)。"""
    p = _cache_path(race_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        data = html.encode("utf-8", errors="replace")
        with lz4.frame.open(str(p), "wb") as f:
            f.write(data)
    except Exception as e:
        log(f"  [WARN] キャッシュ書き込み失敗 race_id={race_id}: {e}")


# ──────────────────────────────────────────
# race_id → race_date 変換
# ──────────────────────────────────────────
def _date_from_race_id(race_id: str) -> str:
    """NAR race_id (YYYY[VV][MM][DD][RR]) から日付を推定。JRA は '' を返す。"""
    if len(race_id) != 12 or not race_id.isdigit():
        return ""
    if race_id[4:6] in JRA_VENUE_CODES:
        return ""  # JRA は日付情報なし
    try:
        year = int(race_id[:4])
        mm   = int(race_id[6:8])
        dd   = int(race_id[8:10])
        _date_cls(year, mm, dd)  # バリデーション
        return f"{year:04d}-{mm:02d}-{dd:02d}"
    except ValueError:
        return ""


# ──────────────────────────────────────────
# DB: 既存 race_id をセットで取得
# ──────────────────────────────────────────
def _load_existing_race_ids(conn) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT race_id FROM race_log "
        "WHERE race_date BETWEEN ? AND ?",
        (PERIOD_START, PERIOD_END),
    ).fetchall()
    return {r[0] for r in rows}


# ──────────────────────────────────────────
# race_log バッチ INSERT
# ──────────────────────────────────────────
_INSERT_SQL = """
INSERT OR IGNORE INTO race_log
    (race_date, race_id, venue_code, surface, distance,
     horse_no, finish_pos,
     jockey_id, jockey_name, trainer_id, trainer_name,
     field_count, is_jra, win_odds,
     horse_id, horse_name, gate_no, sex, age, weight_kg,
     odds, tansho_odds, popularity, horse_weight, weight_change,
     position_4c, positions_corners,
     finish_time_sec, last_3f_sec, first_3f_sec,
     margin_ahead, margin_behind, status,
     course_id, grade, race_name, weather, direction,
     race_first_3f, race_pace,
     pace, is_generation, race_level_dev, source, condition)
VALUES
    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def _build_insert_rows(parsed: dict) -> list[tuple]:
    """parse_result_page() の返り値から INSERT 用タプルのリストを構築する。"""
    race_id     = parsed.get("race_id", "")
    race_date   = parsed.get("date", "")
    venue_code  = parsed.get("venue_code", "")
    surface     = parsed.get("surface", "")
    distance    = int(parsed.get("distance", 0) or 0)
    is_jra      = 1 if parsed.get("is_jra") else 0
    field_count = int(parsed.get("field_count", 0) or 0)
    grade       = parsed.get("grade", "")
    race_name   = parsed.get("race_name", "")
    weather     = parsed.get("weather", "")
    direction   = parsed.get("direction", "")
    race_first_3f = parsed.get("first_3f")
    race_pace   = parsed.get("pace", "")
    condition   = parsed.get("condition", "")
    course_id   = f"{venue_code}_{surface}_{distance}" if venue_code else ""

    # 着差マップ構築（winner_time 基準）
    horses_list = parsed.get("horses", [])
    time_entries = []
    for h in horses_list:
        fp = h.get("finish_pos")
        ft = h.get("finish_time_sec", 0)
        hno = h.get("horse_no")
        if hno is not None and fp is not None and fp < 90 and ft and ft > 0:
            time_entries.append((hno, fp, ft))
    time_entries.sort(key=lambda x: (x[1], x[2]))
    winner_time = min((e[2] for e in time_entries if e[1] == 1), default=0)
    margin_map: dict[int, tuple[float, float]] = {}
    for idx, (hno, _fp, ft) in enumerate(time_entries):
        ma = round(ft - winner_time, 1)
        mb = 0.0
        if idx + 1 < len(time_entries):
            next_t = time_entries[idx + 1][2]
            if next_t > ft:
                mb = round(next_t - ft, 1)
        margin_map[hno] = (ma, mb)

    rows = []
    for h in horses_list:
        horse_no = h.get("horse_no")
        if horse_no is None:
            continue

        finish_pos = h.get("finish_pos")
        if finish_pos is None:
            finish_pos = 99

        corners = h.get("positions_corners", [])
        corners_json = json.dumps(corners) if corners else ""
        position_4c = corners[-1] if corners else 0
        margins = margin_map.get(horse_no, (0.0, 0.0))
        status_val = None if finish_pos < 90 else "取消"

        rows.append((
            race_date, race_id, venue_code, surface, distance,
            horse_no, finish_pos,
            h.get("jockey_id", ""), h.get("jockey", ""),
            h.get("trainer_id", ""), h.get("trainer", ""),
            field_count, is_jra, h.get("odds"),
            h.get("horse_id", ""), h.get("horse_name", ""),
            h.get("gate_no", 0), h.get("sex", ""), h.get("age", 0),
            h.get("weight_kg", 0),
            h.get("odds"), h.get("odds"),  # odds / tansho_odds
            h.get("popularity"), h.get("horse_weight"), h.get("weight_change"),
            position_4c, corners_json,
            h.get("finish_time_sec", 0), h.get("last_3f_sec", 0), None,
            margins[0], margins[1], status_val,
            course_id, grade, race_name, weather, direction,
            race_first_3f, race_pace,
            None, 0, None, "backfill_2023h2", condition,
        ))
    return rows


# ──────────────────────────────────────────
# フェーズ 1: 対象 race_id を列挙
# ──────────────────────────────────────────
def enumerate_target_race_ids(client: NetkeibaClient) -> list[tuple[str, str]]:
    """
    kaisai_calendar.json の 2023-07-01〜2023-12-31 全開催日について
    RaceListFetcher で race_id を取得する。
    Returns: list of (race_id, race_date)
    """
    fetcher = RaceListScraper(client)

    # カレンダーから対象日を取得
    all_dates = get_open_dates(kind="all")
    target_dates = [d for d in all_dates if PERIOD_START <= d <= PERIOD_END]
    log(f"対象開催日: {len(target_dates)} 日 ({PERIOD_START}〜{PERIOD_END})")

    all_rids: list[tuple[str, str]] = []  # (race_id, race_date)
    t0 = time.time()

    for i, date_str in enumerate(target_dates):
        try:
            rids = fetcher.get_race_ids(date_str)
        except Exception as e:
            log(f"  [WARN] {date_str} race_id 取得失敗: {e}")
            rids = []

        for rid in rids:
            # 日付が確実な場合はそのまま、JRA は race_id から日付が分からないため date_str 使用
            all_rids.append((rid, date_str))

        if (i + 1) % 10 == 0 or i == len(target_dates) - 1:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (len(target_dates) - i - 1) / rate if rate > 0 else 0
            log(
                f"  {progress_bar(i + 1, len(target_dates))} "
                f"累計 race_id={len(all_rids):,} 件  "
                f"残り {remaining / 60:.1f} 分"
            )

    # 重複除去（race_id 優先）
    seen: set[str] = set()
    deduped: list[tuple[str, str]] = []
    for rid, rd in all_rids:
        if rid not in seen:
            seen.add(rid)
            deduped.append((rid, rd))

    log(f"列挙完了: {len(deduped):,} race_id (重複除去後)")
    return deduped


# ──────────────────────────────────────────
# フェーズ 2: HTML 取得 + parse + INSERT
# ──────────────────────────────────────────
def fetch_and_insert(
    target_rids: list[tuple[str, str]],
    existing_rids: set[str],
    client: NetkeibaClient,
    conn,
    dry_run: bool,
    max_fetch: int | None,
) -> dict:
    """
    HTML 取得 → parse_result_page → race_log INSERT を実行する。
    Returns: 統計 dict
    """
    # 既存 race_id を除外
    to_process = [(rid, rd) for rid, rd in target_rids if rid not in existing_rids]
    log(f"未投入 race_id: {len(to_process):,} 件（既存 {len(existing_rids):,} 件をスキップ）")

    if max_fetch is not None and max_fetch > 0:
        to_process = to_process[:max_fetch]
        log(f"--max-fetch {max_fetch} 件に制限")

    if not to_process:
        log("処理対象なし。終了します。")
        return {"inserted": 0, "skipped": 0, "errors": 0, "fetched": 0}

    total = len(to_process)
    inserted = 0
    skipped  = 0
    errors   = 0
    fetched  = 0

    batch_rows: list[tuple] = []

    def flush_batch() -> None:
        nonlocal inserted
        if dry_run or not batch_rows:
            batch_rows.clear()
            return
        try:
            conn.executemany(_INSERT_SQL, batch_rows)
            conn.commit()
            inserted += len(batch_rows)
        except Exception as e:
            log(f"  [ERROR] バッチ INSERT 失敗: {e}")
        batch_rows.clear()

    t0 = time.time()

    for i, (race_id, race_date) in enumerate(to_process):
        # キャッシュ確認
        html = _read_cache(race_id)

        if html is None:
            # netkeiba から取得
            vc = race_id[4:6] if len(race_id) >= 6 else ""
            is_jra_race = vc in JRA_VENUE_CODES
            if is_jra_race:
                base_url = "https://race.netkeiba.com/race/result.html"
            else:
                base_url = "https://nar.netkeiba.com/race/result.html"

            try:
                soup = client.get(base_url, params={"race_id": race_id})
                if soup is None:
                    log(f"  [SKIP] 取得失敗 race_id={race_id}")
                    skipped += 1
                    continue
                html = str(soup)
                _write_cache(race_id, html)
                fetched += 1
            except Exception as e:
                log(f"  [ERROR] {race_id}: {e}")
                errors += 1
                continue
        else:
            # キャッシュ利用（再取得なし）
            pass

        # parse
        try:
            soup = BeautifulSoup(html, "html.parser")
            parsed = parse_result_page(soup, race_id)
        except Exception as e:
            log(f"  [ERROR] parse 失敗 race_id={race_id}: {e}")
            errors += 1
            continue

        if not parsed or not parsed.get("horses"):
            skipped += 1
            continue

        # race_date が parse 結果にない場合は列挙時の date_str を使用
        if not parsed.get("date"):
            parsed["date"] = race_date

        # 日付バリデーション（汚染防止）
        rd = parsed.get("date", "")
        if not re.match(r"20\d{2}-\d{2}-\d{2}", rd):
            log(f"  [SKIP] 日付不正 race_id={race_id} date={rd!r}")
            skipped += 1
            continue

        try:
            rows = _build_insert_rows(parsed)
            batch_rows.extend(rows)
        except Exception as e:
            log(f"  [ERROR] INSERT 行構築失敗 race_id={race_id}: {e}")
            errors += 1
            continue

        # バッチコミット
        if len(batch_rows) >= BATCH_SIZE:
            flush_batch()

        # ログ
        if (i + 1) % LOG_INTERVAL == 0 or i == total - 1:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (total - i - 1) / rate if rate > 0 else 0
            log(
                f"  {progress_bar(i + 1, total)}  "
                f"inserted={inserted:,} fetched={fetched:,} "
                f"skip={skipped:,} err={errors:,}  "
                f"経過 {elapsed / 60:.1f}分 残り {remaining / 60:.1f}分"
            )

    flush_batch()

    return {
        "inserted": inserted,
        "skipped": skipped,
        "errors": errors,
        "fetched": fetched,
    }


# ──────────────────────────────────────────
# DB バックアップ
# ──────────────────────────────────────────
def backup_db() -> Path:
    src = Path(DATABASE_PATH)
    dst = src.with_suffix(f".{BACKUP_SUFFIX}")
    if not dst.exists():
        shutil.copy2(str(src), str(dst))
        log(f"DBバックアップ完了: {dst}")
    else:
        log(f"バックアップ既存のためスキップ: {dst}")
    return dst


# ──────────────────────────────────────────
# エントリーポイント
# ──────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="2023年下半期 race_log バックフィル (JRA+NAR)"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="取得対象 race_id 数のみ表示")
    mode.add_argument("--apply",   action="store_true", help="本実行（DB 書き込みあり）")
    parser.add_argument(
        "--max-fetch",
        type=int,
        default=None,
        metavar="N",
        help="取得上限（smoke test 用）。指定しない場合は全件処理",
    )
    args = parser.parse_args()

    _ensure_log_dir()
    log("=" * 60)
    log("  2023年下半期 race_log バックフィル 開始")
    log(f"  期間: {PERIOD_START} ～ {PERIOD_END}")
    log(f"  モード: {'dry-run' if args.dry_run else 'apply'}"
        + (f"  max-fetch={args.max_fetch}" if args.max_fetch else ""))
    log("=" * 60)

    # ── DB 初期化 ──
    init_schema()
    conn = get_db()

    # ── バックアップ（apply 時のみ） ──
    if args.apply:
        backup_db()

    # ── NetkeibaClient（キャッシュ優先、TTL 無視で古いキャッシュも再利用） ──
    client = NetkeibaClient(ignore_ttl=True, request_interval=1.0)

    # ── フェーズ 1: race_id 列挙 ──
    log("\n[Phase 1] race_id 列挙中...")
    target_rids = enumerate_target_race_ids(client)

    if args.dry_run:
        log(f"\n[dry-run] 取得対象 race_id 数: {len(target_rids):,} 件")
        # 既存との重複もカウント
        existing = _load_existing_race_ids(conn)
        new_cnt = sum(1 for rid, _ in target_rids if rid not in existing)
        log(f"[dry-run] 未投入 (未 DB): {new_cnt:,} 件")
        log(f"[dry-run] 既存 DB にある: {len(existing):,} 件")
        log("[dry-run] 完了。--apply で本実行してください。")
        return

    # ── フェーズ 2: 既存 race_id 除外 ──
    log("\n[Phase 2] 既存 race_id を DB から取得中...")
    existing_rids = _load_existing_race_ids(conn)
    log(f"  既存 race_id (2023H2): {len(existing_rids):,} 件")

    # ── フェーズ 3: HTML 取得 + parse + INSERT ──
    log("\n[Phase 3] HTML 取得 → parse → INSERT 開始...")
    t_start = time.time()
    stats = fetch_and_insert(
        target_rids=target_rids,
        existing_rids=existing_rids,
        client=client,
        conn=conn,
        dry_run=False,
        max_fetch=args.max_fetch,
    )

    elapsed_total = time.time() - t_start
    log("\n" + "=" * 60)
    log("  バックフィル完了サマリ")
    log(f"  inserted : {stats['inserted']:,} 行")
    log(f"  fetched  : {stats['fetched']:,} 件（新規 netkeiba 取得）")
    log(f"  skipped  : {stats['skipped']:,} 件")
    log(f"  errors   : {stats['errors']:,} 件")
    log(f"  総所要時間: {elapsed_total / 60:.1f} 分")
    log("=" * 60)

    # ── 整合性確認 ──
    after_cnt = conn.execute(
        "SELECT COUNT(DISTINCT race_id) FROM race_log "
        "WHERE race_date BETWEEN ? AND ?",
        (PERIOD_START, PERIOD_END),
    ).fetchone()[0]
    log(f"\n[検証] race_log の 2023H2 race_id 数: {after_cnt:,} 件")
    log(f"[ログ] {LOG_PATH}")
    log(f"[監視] tail -f {LOG_PATH}")


if __name__ == "__main__":
    main()
