#!/usr/bin/env python
"""
HTMLキャッシュから ML データ（data/ml/YYYYMMDD.json）を生成する。

既存のキャッシュファイル（.html / .html.lz4）を読み込み、
parse_result_page() で解析して日別 JSON に保存する。

Usage:
  python scripts/backfill_ml_from_cache.py
  python scripts/backfill_ml_from_cache.py --year 2026
  python scripts/backfill_ml_from_cache.py --start 2026-01-01 --end 2026-03-01 --force
"""

import argparse
import glob
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lz4.frame
from bs4 import BeautifulSoup

from src.scraper.ml_data_collector import parse_result_page, ML_DATA_DIR
from data.masters.venue_master import get_venue_code_from_race_id, is_banei

CACHE_DIR = "data/cache"


def _read_cache_file(fpath: str) -> str:
    """LZ4圧縮またはそのままのHTMLファイルを読む"""
    if fpath.endswith(".lz4"):
        with lz4.frame.open(fpath, "rb") as f:
            return f.read().decode("utf-8")
    else:
        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
            return f.read()


def extract_race_id(fpath: str) -> str:
    m = re.search(r"race_id=(\d+)", fpath)
    return m.group(1) if m else ""


def date_from_race_id(race_id: str) -> str:
    """race_id (12桁) → 'YYYY-MM-DD'"""
    if len(race_id) < 10:
        return ""
    year = race_id[:4]
    mm   = race_id[6:8]
    dd   = race_id[8:10]
    return f"{year}-{mm}-{dd}"


def _build_race_id_date_map(start_date: str, end_date: str) -> dict:
    """race_logテーブルから race_id → date マッピングを構築（JRA race_idは日付を含まないため必要）"""
    import sqlite3
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "keiba.db")
    if not os.path.exists(db_path):
        return {}
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT DISTINCT race_id, race_date FROM race_log WHERE race_date >= ? AND race_date <= ?",
        (start_date, end_date),
    ).fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def collect_cache_files(start_date: str, end_date: str) -> list:
    """対象期間の race_result キャッシュファイル一覧を返す"""
    patterns = [
        os.path.join(CACHE_DIR, "race.netkeiba.com_race_result.html_race_id=*.html"),
        os.path.join(CACHE_DIR, "race.netkeiba.com_race_result.html_race_id=*.html.lz4"),
        os.path.join(CACHE_DIR, "nar.netkeiba.com_race_result.html_race_id=*.html"),
        os.path.join(CACHE_DIR, "nar.netkeiba.com_race_result.html_race_id=*.html.lz4"),
    ]
    all_files = []
    for pat in patterns:
        all_files.extend(glob.glob(pat))

    # race_logから正確なrace_id→dateマッピングを構築
    # JRA race_idは YYYYVVRRDDNN 形式で日付を含まないため必須
    rid_date_map = _build_race_id_date_map(start_date, end_date)
    target_rids = set(rid_date_map.keys())

    start = start_date.replace("-", "")
    end   = end_date.replace("-", "")

    filtered = []
    for f in all_files:
        rid = extract_race_id(f)
        if not rid or len(rid) < 10:
            continue

        # race_logマッピングに存在すればそのまま採用
        if rid in target_rids:
            filtered.append(f)
            continue

        # NARフォールバック: race_id[6:10]がMMDD
        year = rid[:4]
        mmdd = rid[6:10]
        rid_date = year + mmdd
        if start <= rid_date <= end:
            filtered.append(f)

    filtered.sort(key=lambda x: extract_race_id(x))
    return filtered


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-01-01")
    parser.add_argument("--end",   default="2026-03-02")
    parser.add_argument("--year",  default=None, help="年指定 (例: 2026) → --start/--end を上書き")
    parser.add_argument("--force", action="store_true", help="既存ファイルを上書き")
    args = parser.parse_args()

    if args.year:
        args.start = f"{args.year}-01-01"
        args.end   = f"{args.year}-12-31"

    print(f"\n{'='*60}")
    print(f"  ML データ バックフィル（キャッシュ→JSON）")
    print(f"  期間: {args.start} ～ {args.end}  force={args.force}")
    print(f"{'='*60}\n")

    # race_logからrace_id→dateマッピングを構築（JRA対応）
    rid_date_map = _build_race_id_date_map(args.start, args.end)
    print(f"race_log マッピング: {len(rid_date_map):,} race_ids")

    files = collect_cache_files(args.start, args.end)
    print(f"対象キャッシュファイル: {len(files):,}件")
    if not files:
        print("対象ファイルがありません。終了します。")
        return

    # 日付ごとにグループ化（race_logマッピング優先、NARフォールバック）
    by_date: dict = defaultdict(list)
    for f in files:
        rid = extract_race_id(f)
        d = rid_date_map.get(rid) or date_from_race_id(rid)
        if d:
            by_date[d].append((rid, f))

    print(f"日付数: {len(by_date)}日\n")

    os.makedirs(ML_DATA_DIR, exist_ok=True)

    total_saved_days = 0
    total_skipped    = 0
    total_races      = 0
    total_errors     = 0

    for date_str in sorted(by_date.keys()):
        out_path = os.path.join(ML_DATA_DIR, date_str.replace("-", "") + ".json")
        if os.path.exists(out_path) and not args.force:
            total_skipped += 1
            continue

        items = by_date[date_str]
        day_races = []
        day_errors = 0

        for rid, fpath in sorted(items):
            try:
                html = _read_cache_file(fpath)
            except Exception as e:
                day_errors += 1
                continue

            try:
                soup = BeautifulSoup(html, "html.parser")
                parsed = parse_result_page(soup, rid)
                if parsed:
                    day_races.append(parsed)
            except Exception as e:
                day_errors += 1
                continue

        if day_races:
            data = {"date": date_str, "race_count": len(day_races), "races": day_races}
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            total_saved_days += 1
            total_races += len(day_races)
            venues = sorted(set(r.get("venue") or "不明" for r in day_races))
            print(f"  {date_str}  {len(day_races):3d}R  {', '.join(venues[:5])}  (err:{day_errors})")
        else:
            print(f"  {date_str}  0R (スキップ, err:{day_errors})")

        total_errors += day_errors

    print(f"\n{'='*60}")
    print(f"  完了!  保存:{total_saved_days}日 / {total_races}レース")
    print(f"  スキップ(既存):{total_skipped}日  エラー:{total_errors}件")
    print(f"{'='*60}\n")
    print("次のステップ: python scripts/bulk_backfill_predictions.py --start 2026-01-01 --end 2026-03-02 --force")


if __name__ == "__main__":
    main()
