"""
キャッシュ済みレース結果HTMLから race_log テーブルを一括構築する。
111,000件のキャッシュを解析して全馬・着外含む成績を投入する。

実行: python scripts/backfill_race_log.py
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
JRA_VENUE_CODES = frozenset(["01","02","03","04","05","06","07","08","09","10"])

# ── HTML パーサー（軽量regex版）──────────────────────────────
_RE_JOCKEY_ID  = re.compile(r'/jockey/(?:result/recent/)?(\w+)/?')
_RE_TRAINER_ID = re.compile(r'/trainer/(?:result/recent/)?(\w+)/?')
_RE_DATE_META  = re.compile(r'(\d{4})年(\d{1,2})月(\d{1,2})日')
_RE_SURFACE    = re.compile(r'(?:芝|ダ(?:ート)?|障)')
_RE_DISTANCE   = re.compile(r'(\d{3,4})m')
_RE_RACE_ID    = re.compile(r'race_id=(\d{10,12})')

def _read_html(path: str) -> str:
    if path.endswith('.lz4'):
        if not HAS_LZ4:
            return ""
        with open(path, 'rb') as f:
            return lz4.frame.decompress(f.read()).decode('utf-8', errors='replace')
    with open(path, encoding='utf-8', errors='replace') as f:
        return f.read()

def _parse_race_result(html: str, race_id: str):
    """
    HTMLから全馬着順と騎手・調教師IDを抽出する。
    Returns: (race_date, venue_code, surface, distance, rows)
      rows: list of (horse_no, finish_pos, jockey_id, jockey_name, trainer_id, trainer_name)
    """
    venue_code = race_id[4:6] if len(race_id) >= 6 else ""
    is_jra = venue_code in JRA_VENUE_CODES

    # 日付
    if is_jra:
        m = _RE_DATE_META.search(html)
        if m:
            race_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        else:
            race_date = f"{race_id[:4]}-01-01"
    elif len(race_id) >= 10:
        # NAR: YYYYVVMMDD NN
        race_date = f"{race_id[:4]}-{race_id[6:8]}-{race_id[8:10]}"
    else:
        race_date = f"{race_id[:4]}-01-01"

    # 馬場・距離（RaceData01）
    surface = ""
    distance = 0
    rd_m = re.search(r'RaceData01[^>]*>(.*?)</(?:span|div|p)', html, re.DOTALL)
    if rd_m:
        rd_text = re.sub(r'<[^>]+>', '', rd_m.group(1))
        sm = _RE_SURFACE.search(rd_text)
        dm = _RE_DISTANCE.search(rd_text)
        if sm:
            raw = sm.group(0)
            surface = "芝" if raw == "芝" else ("障害" if "障" in raw else "ダート")
        if dm:
            distance = int(dm.group(1))

    # 着順テーブル（ResultTableWrap）
    tbl_m = re.search(r'ResultTableWrap.*?<tbody>(.*?)</tbody>', html, re.DOTALL)
    if not tbl_m:
        return race_date, venue_code, surface, distance, []

    tbody = tbl_m.group(1)
    rows_out = []

    for tr_m in re.finditer(r'<tr[^>]*>(.*?)</tr>', tbody, re.DOTALL):
        tr_html = tr_m.group(1)
        tds = re.findall(r'<td[^>]*>(.*?)</td>', tr_html, re.DOTALL)
        if len(tds) < 8:
            continue

        def td_text(raw):
            return re.sub(r'<[^>]+>', '', raw).strip()

        finish_str = td_text(tds[0])
        if not finish_str.isdigit():
            continue
        finish = int(finish_str)
        if finish <= 0 or finish > 99:
            continue

        hno_str = td_text(tds[2])
        horse_no = int(hno_str) if hno_str.isdigit() else 0

        jockey_col = tds[6] if len(tds) > 6 else ""
        # JRA(15列): td[12]=コーナー通過順, td[13]=調教師, td[14]=馬体重
        # NAR(14列): td[12]=調教師, td[13]=馬体重
        if is_jra and len(tds) >= 15:
            trainer_col = tds[13]
        elif len(tds) > 12:
            trainer_col = tds[12]
        else:
            trainer_col = ""

        jockey_name = td_text(jockey_col)
        trainer_name = td_text(trainer_col)

        # 単勝オッズ (tds[10])
        try:
            win_odds = float(td_text(tds[10])) if len(tds) > 10 and td_text(tds[10]) else None
        except ValueError:
            win_odds = None

        jm = _RE_JOCKEY_ID.search(tr_html)
        tm = _RE_TRAINER_ID.search(tr_html)
        jockey_id  = jm.group(1) if jm else ""
        trainer_id = tm.group(1) if tm else ""

        rows_out.append((horse_no, finish, jockey_id, jockey_name, trainer_id, trainer_name, win_odds))

    return race_date, venue_code, surface, distance, rows_out


def backfill(start_year: int = 2024, dry_run: bool = False, verbose: bool = True):
    """
    キャッシュファイルをスキャンして race_log を投入する。
    """
    init_schema()
    conn = get_db()

    # 既存の race_id を一括取得（重複スキップ用）
    existing = {r[0] for r in conn.execute("SELECT DISTINCT race_id FROM race_log").fetchall()}
    if verbose:
        print(f"既存 race_id: {len(existing):,} 件")

    # 対象キャッシュファイルを列挙
    pattern = re.compile(r'result\.html_race_id=(' + str(start_year) + r'\d{8,10})\.html(?:\.lz4)?$')
    files = []
    for fname in os.listdir(CACHE_DIR):
        m = pattern.search(fname)
        if m:
            rid = m.group(1)
            if rid not in existing:
                files.append((fname, rid))

    if verbose:
        print(f"処理対象: {len(files):,} ファイル（{start_year}年〜・未投入分）")

    if not files:
        print("処理対象なし。")
        return 0

    # バッチ処理
    BATCH = 500
    total_inserted = 0
    total_races = 0
    skipped = 0
    t0 = time.time()

    batch_rows = []

    def flush_batch():
        nonlocal total_inserted
        if dry_run or not batch_rows:
            batch_rows.clear()
            return
        conn.executemany(
            """INSERT OR IGNORE INTO race_log
               (race_date, race_id, venue_code, surface, distance,
                horse_no, finish_pos, jockey_id, jockey_name,
                trainer_id, trainer_name, field_count, is_jra, win_odds)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            batch_rows,
        )
        conn.commit()
        total_inserted += len(batch_rows)
        batch_rows.clear()

    for i, (fname, race_id) in enumerate(files):
        fpath = os.path.join(CACHE_DIR, fname)
        try:
            html = _read_html(fpath)
        except Exception:
            skipped += 1
            continue

        race_date, venue_code, surface, distance, rows = _parse_race_result(html, race_id)
        if not rows:
            skipped += 1
            continue

        # race_date の簡易バリデーション
        if not re.match(r'20\d{2}-\d{2}-\d{2}', race_date):
            skipped += 1
            continue
        y, mo, dd = race_date.split('-')
        if not (2020 <= int(y) <= 2030 and 1 <= int(mo) <= 12 and 1 <= int(dd) <= 31):
            skipped += 1
            continue

        is_jra = 1 if venue_code in JRA_VENUE_CODES else 0
        field_count = len(rows)

        for (horse_no, finish, jid, jname, tid, tname, win_odds) in rows:
            batch_rows.append((
                race_date, race_id, venue_code, surface, distance,
                horse_no, finish, jid, jname, tid, tname,
                field_count, is_jra, win_odds,
            ))

        total_races += 1

        if len(batch_rows) >= BATCH:
            flush_batch()

        if verbose and (i + 1) % 1000 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            remaining = (len(files) - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1:,}/{len(files):,} レース, 投入済: {total_inserted:,}, "
                  f"速度: {rate:.0f}件/秒, 残り: {remaining/60:.1f}分")

    flush_batch()

    elapsed = time.time() - t0
    if verbose:
        print(f"\n完了: {total_races:,} レース処理, {total_inserted:,} 行投入, "
              f"スキップ: {skipped:,}, 経過: {elapsed:.1f}秒")
    return total_inserted


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    backfill(start_year=args.year, dry_run=args.dry_run)
