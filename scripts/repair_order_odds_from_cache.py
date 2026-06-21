#!/usr/bin/env python
"""
HTMLキャッシュから単勝オッズを抽出してrace_results.order_jsonに補完する。

対象: order_jsonの各エントリに「odds」フィールドが欠落しているrace_id

使い方:
    python scripts/repair_order_odds_from_cache.py --dry-run
    python scripts/repair_order_odds_from_cache.py --date 2026-06-20
    python scripts/repair_order_odds_from_cache.py --start 2026-06-01 --end 2026-06-21
    python scripts/repair_order_odds_from_cache.py --start 2026-06-01 --end 2026-06-21 --force

注意:
    - ネットワークアクセスなし（キャッシュHTMLのみ使用）
    - ばんえい(venue_code='65')除外
    - order_jsonにoddsがない馬のみ補完（既存oddsは上書きしない）
"""

import argparse
import glob
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

import lz4.frame
from bs4 import BeautifulSoup

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "keiba.db"
CACHE_DIR = ROOT / "data" / "cache"

# ばんえい(帯広) venue_code
BANEI_VENUE = "65"


def _get_cache_path(race_id: str) -> str | None:
    """race_idに対応するキャッシュHTMLファイルパスを返す（なければNone）"""
    jra = CACHE_DIR / f"race.netkeiba.com_race_result.html_race_id={race_id}.html.lz4"
    nar = CACHE_DIR / f"nar.netkeiba.com_race_result.html_race_id={race_id}.html.lz4"
    jra_plain = CACHE_DIR / f"race.netkeiba.com_race_result.html_race_id={race_id}.html"
    nar_plain = CACHE_DIR / f"nar.netkeiba.com_race_result.html_race_id={race_id}.html"
    for p in [jra, nar, jra_plain, nar_plain]:
        if p.exists():
            return str(p)
    return None


def _read_html(fpath: str) -> str:
    """キャッシュHTMLを読み込む（lz4圧縮・非圧縮両対応）"""
    if fpath.endswith(".lz4"):
        with lz4.frame.open(fpath, "rb") as f:
            return f.read().decode("utf-8", errors="replace")
    else:
        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
            return f.read()


def _extract_win_odds_from_html(html: str) -> dict[int, float]:
    """
    RaceTable01テーブルから horse_no → 単勝オッズ のマップを抽出する。

    HTMLのカラム構造（index）:
      [0] 着順  [1] 枠番  [2] 馬番  [3] 馬名  [4] 性齢  [5] 斤量
      [6] 騎手  [7] タイム [8] 着差  [9] 人気  [10] 単勝オッズ [11] 後3F ...
    """
    soup = BeautifulSoup(html, "html.parser")
    tbl = soup.find("table", class_="RaceTable01")
    if not tbl:
        return {}

    odds_map: dict[int, float] = {}
    rows = tbl.find_all("tr")

    for tr in rows[1:]:  # ヘッダ行をスキップ
        cells = tr.find_all("td")
        if len(cells) < 11:
            continue
        try:
            horse_no = int(cells[2].get_text(strip=True))
            odds_str = cells[10].get_text(strip=True)
            odds_val = float(odds_str)
            if odds_val > 0:
                odds_map[horse_no] = odds_val
        except (ValueError, TypeError):
            continue

    return odds_map


def _is_banei(race_id: str) -> bool:
    """ばんえい(venue_code=65)かどうか判定"""
    # JRAフォーマット: YYYYVVDDddRR (VV=venue_code 2桁)
    # race_idは12桁: YYYY+VV+DDDD+RR
    if len(race_id) >= 6:
        venue_code = race_id[4:6]
        if venue_code == BANEI_VENUE:
            return True
    return False


def main():
    parser = argparse.ArgumentParser(description="HTMLキャッシュからorder_jsonの単勝オッズを補完")
    parser.add_argument("--date", type=str, default=None, help="対象日 YYYY-MM-DD（--start/--end の代替）")
    parser.add_argument("--start", type=str, default=None, help="開始日 YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None, help="終了日 YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="DB更新せず結果のみ表示")
    parser.add_argument("--force", action="store_true", help="既にoddsがある馬も上書きする")
    args = parser.parse_args()

    # 日付範囲設定
    if args.date:
        date_start = args.date
        date_end = args.date
    elif args.start and args.end:
        date_start = args.start
        date_end = args.end
    elif args.start:
        date_start = args.start
        date_end = args.start
    else:
        # デフォルト: 本日のみ
        from datetime import date
        today = date.today().strftime("%Y-%m-%d")
        date_start = today
        date_end = today

    print(f"=== order_json 単勝オッズ補完 ===")
    print(f"対象期間: {date_start} ～ {date_end}")
    print(f"dry-run: {args.dry_run}")
    print(f"force: {args.force}")
    print(f"DB: {DB_PATH}")
    print()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    # 対象race_idを取得（ばんえい除外）
    cur.execute("""
        SELECT race_id, order_json
        FROM race_results
        WHERE date BETWEEN ? AND ?
        ORDER BY race_id
    """, (date_start, date_end))
    rows = cur.fetchall()

    print(f"DB内レース数: {len(rows)}件")

    # oddsが欠落しているrace_idを絞り込む
    target_races = []
    banei_skipped = 0
    already_ok = 0

    for race_id, order_json_raw in rows:
        # ばんえい除外
        if _is_banei(race_id):
            banei_skipped += 1
            continue

        try:
            order = json.loads(order_json_raw) if order_json_raw else []
        except (json.JSONDecodeError, TypeError):
            order = []

        if not order:
            continue

        # oddsが欠落している馬が存在するか確認
        if not args.force:
            missing_odds = [h for h in order if h.get("odds") is None]
            if not missing_odds:
                already_ok += 1
                continue

        target_races.append((race_id, order))

    print(f"ばんえい除外: {banei_skipped}件")
    print(f"odds既設定済み: {already_ok}件")
    print(f"odds補完対象: {len(target_races)}件")
    print()

    # キャッシュ確認
    no_cache = []
    cache_found = []
    for race_id, order in target_races:
        cp = _get_cache_path(race_id)
        if cp:
            cache_found.append((race_id, order, cp))
        else:
            no_cache.append(race_id)

    print(f"キャッシュあり: {len(cache_found)}件（修復可能）")
    print(f"キャッシュなし: {len(no_cache)}件（修復不可）")
    if no_cache:
        print("  修復不可race_id:")
        for rid in no_cache:
            print(f"    {rid}")
    print()

    if not cache_found:
        print("修復可能なレースがありません。")
        conn.close()
        return

    # 修復処理
    updated_races = 0
    updated_horses = 0
    no_html_odds = 0
    errors = 0

    for i, (race_id, order, cache_path) in enumerate(cache_found):
        try:
            html = _read_html(cache_path)
        except Exception as e:
            print(f"[ERROR] {race_id}: HTML読み込み失敗 ({e})")
            errors += 1
            continue

        # HTMLから単勝オッズ抽出
        try:
            odds_map = _extract_win_odds_from_html(html)
        except Exception as e:
            print(f"[ERROR] {race_id}: odds抽出失敗 ({e})")
            errors += 1
            continue

        if not odds_map:
            print(f"[WARN] {race_id}: HTMLからoddが取得できず（レース情報なし?）")
            no_html_odds += 1
            continue

        # order_jsonを更新
        modified = False
        for h in order:
            horse_no = h.get("horse_no")
            if horse_no is None:
                continue
            if not args.force and h.get("odds") is not None:
                continue  # 既にoddsあり（--forceなし）
            odds_val = odds_map.get(int(horse_no))
            if odds_val is not None:
                h["odds"] = odds_val
                updated_horses += 1
                modified = True
            # else: HTMLにも存在しない（取消馬等）→ そのままNone

        if modified:
            new_order_json = json.dumps(order, ensure_ascii=False, separators=(",", ":"))
            if args.dry_run:
                if i < 5:
                    print(f"[DRY-RUN] {race_id}: order_json更新予定（例: {order[0]}）")
            else:
                cur.execute(
                    "UPDATE race_results SET order_json = ? WHERE race_id = ?",
                    (new_order_json, race_id),
                )
            updated_races += 1

    if not args.dry_run and updated_races > 0:
        conn.commit()

    conn.close()

    print()
    print("=" * 60)
    print(f"完了")
    print(f"  更新レース数: {updated_races}件")
    print(f"  更新馬数:     {updated_horses}頭")
    print(f"  HTML oddsなし: {no_html_odds}件")
    print(f"  エラー:       {errors}件")
    print(f"  キャッシュなし（修復不可）: {len(no_cache)}件")
    if args.dry_run:
        print("  ※ dry-runモード: DBは更新されていません")
    print()


if __name__ == "__main__":
    main()
