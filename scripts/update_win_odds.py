"""
race_log の win_odds を既存キャッシュHTMLから一括更新する。

実行: python scripts/update_win_odds.py [--year 2024]
"""
import os
import re
import sys
import time
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import lz4.frame
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

from src.database import get_db, init_schema

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "cache")

_RE_RACE_ID = re.compile(r'result\.html_race_id=(\d{10,12})\.html(?:\.lz4)?$')


def _read_html(path: str) -> str:
    if path.endswith('.lz4'):
        if not HAS_LZ4:
            return ""
        with open(path, 'rb') as f:
            return lz4.frame.decompress(f.read()).decode('utf-8', errors='replace')
    with open(path, encoding='utf-8', errors='replace') as f:
        return f.read()


def _extract_odds(html: str) -> dict:
    """race_id + horse_no → win_odds のマッピングを返す"""
    tbl_m = re.search(r'ResultTableWrap.*?<tbody>(.*?)</tbody>', html, re.DOTALL)
    if not tbl_m:
        return {}

    result = {}
    td_text = lambda raw: re.sub(r'<[^>]+>', '', raw).strip()

    for tr_m in re.finditer(r'<tr[^>]*>(.*?)</tr>', tbl_m.group(1), re.DOTALL):
        tds = re.findall(r'<td[^>]*>(.*?)</td>', tr_m.group(1), re.DOTALL)
        if len(tds) < 11:
            continue
        finish_str = td_text(tds[0])
        if not finish_str.isdigit():
            continue
        hno_str = td_text(tds[2])
        if not hno_str.isdigit():
            continue
        horse_no = int(hno_str)
        try:
            win_odds = float(td_text(tds[10]))  # tds[10] = 単勝オッズ
        except (ValueError, IndexError):
            continue
        if win_odds > 0:
            result[horse_no] = win_odds
    return result


def update_odds(start_year: int = 2024, verbose: bool = True):
    init_schema()
    conn = get_db()

    # win_odds が NULL の race_id を取得
    null_races = {r[0] for r in conn.execute(
        "SELECT DISTINCT race_id FROM race_log WHERE win_odds IS NULL"
    ).fetchall()}
    if verbose:
        print(f"win_odds未設定レース: {len(null_races):,} 件")

    pattern = re.compile(r'result\.html_race_id=(' + str(start_year) + r'\d{8,10})\.html(?:\.lz4)?$')
    files = []
    for fname in os.listdir(CACHE_DIR):
        m = pattern.search(fname)
        if m and m.group(1) in null_races:
            files.append((fname, m.group(1)))

    if verbose:
        print(f"処理対象: {len(files):,} ファイル")
    if not files:
        print("処理対象なし。")
        return 0

    BATCH = 1000
    total_updated = 0
    t0 = time.time()
    batch_params = []

    def flush():
        nonlocal total_updated
        if not batch_params:
            return
        conn.executemany(
            "UPDATE race_log SET win_odds=? WHERE race_id=? AND horse_no=?",
            batch_params,
        )
        conn.commit()
        total_updated += len(batch_params)
        batch_params.clear()

    for i, (fname, race_id) in enumerate(files):
        fpath = os.path.join(CACHE_DIR, fname)
        try:
            html = _read_html(fpath)
        except Exception:
            continue
        odds_map = _extract_odds(html)
        for horse_no, odds in odds_map.items():
            batch_params.append((odds, race_id, horse_no))
        if len(batch_params) >= BATCH:
            flush()
        if verbose and (i + 1) % 2000 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            rem = (len(files) - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1:,}/{len(files):,} 処理, 更新: {total_updated:,}, 残り: {rem/60:.1f}分")

    flush()
    elapsed = time.time() - t0
    if verbose:
        print(f"\n完了: {total_updated:,} 行更新, 経過: {elapsed:.1f}秒")
    return total_updated


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=0, help="0=全年, 2024=2024年以降")
    args = ap.parse_args()
    if args.year:
        update_odds(start_year=args.year)
    else:
        for yr in [2024, 2025, 2026]:
            print(f"\n=== {yr}年 ===")
            update_odds(start_year=yr)
