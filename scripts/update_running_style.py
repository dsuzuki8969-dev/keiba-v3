"""
race_log の running_style をキャッシュHTMLのコーナー通過順から更新する。

脚質分類（最終コーナー通過順位から推定）:
  逃げ: 1位
  先行: 2 〜 field*0.30
  差し: field*0.30 〜 field*0.65
  追込: それ以外（後方）

実行: python scripts/update_running_style.py [--year 2024]
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


def _read_html(path: str) -> str:
    if path.endswith('.lz4'):
        if not HAS_LZ4:
            return ""
        with open(path, 'rb') as f:
            return lz4.frame.decompress(f.read()).decode('utf-8', errors='replace')
    with open(path, encoding='utf-8', errors='replace') as f:
        return f.read()


def _parse_corner_positions(html: str) -> dict:
    """
    コーナー通過順テーブルから {horse_no: last_corner_pos} を返す。
    last_corner_pos は 1始まりの通過順位（同着は同一の値）。
    """
    # 最終コーナーを取得（最後のtr）
    m = re.search(r'class="RaceCommon_Table Corner_Num">(.*?)</table>', html, re.DOTALL)
    if not m:
        return {}

    # 各コーナー行の中の最後のものを使用
    tr_matches = list(re.finditer(r'<tr>(.*?)</tr>', m.group(1), re.DOTALL))
    if not tr_matches:
        return {}

    # 最終コーナーのtd内容を取得
    last_tr = tr_matches[-1].group(1)
    td_m = re.search(r'<td>(.*?)</td>', last_tr, re.DOTALL)
    if not td_m:
        return {}

    # タグを除去してテキスト化
    td_text = re.sub(r'<[^>]+>', '', td_m.group(1)).strip()

    # "( 8,10),( 4,11),1,( 3,7,12),9,5,6,2" のようなフォーマットをパース
    result = {}
    cumulative_pos = 1
    i = 0
    text = td_text.replace(' ', '')
    while i < len(text):
        if text[i] == '(':
            # グループ: 括弧内の馬番を全て同じ位置に
            end = text.find(')', i)
            if end == -1:
                break
            group_str = text[i+1:end]
            nums = re.findall(r'\d+', group_str)
            for n in nums:
                result[int(n)] = cumulative_pos
            cumulative_pos += len(nums)
            i = end + 1
            if i < len(text) and text[i] == ',':
                i += 1
        elif text[i].isdigit():
            # 単独馬番
            j = i
            while j < len(text) and text[j].isdigit():
                j += 1
            horse_no = int(text[i:j])
            result[horse_no] = cumulative_pos
            cumulative_pos += 1
            i = j
            if i < len(text) and text[i] == ',':
                i += 1
        else:
            i += 1
    return result


def _classify_running_style(pos: int, field_count: int) -> str:
    """コーナー位置から脚質を分類"""
    if field_count <= 0:
        return ""
    if pos == 1:
        return "逃げ"
    ratio = pos / field_count
    if ratio <= 0.30:
        return "先行"
    if ratio <= 0.65:
        return "差し"
    return "追込"


def update_running_style(start_year: int = 2024, verbose: bool = True):
    init_schema()
    conn = get_db()

    # running_style が NULL の race_id を取得
    null_races = {r[0] for r in conn.execute(
        "SELECT DISTINCT race_id FROM race_log WHERE running_style IS NULL"
    ).fetchall()}
    if verbose:
        print(f"running_style未設定レース: {len(null_races):,} 件")

    pattern = re.compile(
        r'result\.html_race_id=(' + str(start_year) + r'\d{8,10})\.html(?:\.lz4)?$'
    )
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
            "UPDATE race_log SET running_style=? WHERE race_id=? AND horse_no=?",
            batch_params,
        )
        conn.commit()
        total_updated += len(batch_params)
        batch_params.clear()

    skipped = 0
    for i, (fname, race_id) in enumerate(files):
        fpath = os.path.join(CACHE_DIR, fname)
        try:
            html = _read_html(fpath)
        except Exception:
            skipped += 1
            continue

        corner_pos = _parse_corner_positions(html)
        if not corner_pos:
            # コーナー通過順なし（直線競走等）→ スキップ（NULLのまま）
            # レースの全馬を "直線" として設定するか、スキップ
            # 馬体重から head countを取得して全馬を一旦設定
            skipped += 1
            continue

        # field_count を race_log から取得（最初の1行）
        row = conn.execute(
            "SELECT field_count FROM race_log WHERE race_id=? LIMIT 1", (race_id,)
        ).fetchone()
        field_count = row[0] if row else len(corner_pos)

        for horse_no, pos in corner_pos.items():
            style = _classify_running_style(pos, field_count)
            if style:
                batch_params.append((style, race_id, horse_no))

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
        print(f"\n完了: {total_updated:,} 行更新, スキップ: {skipped:,}, 経過: {elapsed:.1f}秒")
    return total_updated


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=0, help="0=全年")
    args = ap.parse_args()
    if args.year:
        update_running_style(start_year=args.year)
    else:
        for yr in [2024, 2025, 2026]:
            print(f"\n=== {yr}年 ===")
            update_running_style(start_year=yr)
