"""
キャッシュHTMLから venue 01(札幌)・02(函館) の course_db を補完するスクリプト。

TTL の制約を受けずにキャッシュ済みの過去結果HTMLを直接読み込む。
既存の course_db_preload.json にマージ追記する（既存データは保持）。

実行: python scripts/supplement_course_db_from_cache.py [--venues 01 02]
"""

import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import lz4.frame as _lz4
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

from bs4 import BeautifulSoup

from config.settings import CACHE_DIR, COURSE_DB_PRELOAD_PATH
from src.scraper.course_db_collector import (
    _parse_full_lap_data,
    _parse_result_to_past_runs,
    _past_run_to_dict,
)
from src.models import PaceType

CACHE_DIR_PATH = CACHE_DIR


def _read_html(path: str) -> str:
    """lz4 or plain HTML を読み込む"""
    if path.endswith(".lz4"):
        if not HAS_LZ4:
            return ""
        with open(path, "rb") as f:
            return _lz4.decompress(f.read()).decode("utf-8", errors="replace")
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def supplement_venues(venues=("01", "02"), verbose=True):
    """
    指定 venue の結果 HTML をキャッシュから全スキャンして course_db に追記する。
    """
    # 既存データの読み込み
    all_runs = {}
    if os.path.exists(COURSE_DB_PRELOAD_PATH):
        try:
            with open(COURSE_DB_PRELOAD_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            all_runs = data.get("course_db", {})
            if verbose:
                print(f"既存 course_db: {len(all_runs)} コース, {sum(len(v) for v in all_runs.values())} 走")
        except Exception as e:
            print(f"既存データ読み込みエラー: {e}")

    # 既存データの重複チェック用インデックス
    existing_keys = {}
    for cid, runs in all_runs.items():
        existing_keys[cid] = set()
        for r in runs:
            key = (
                r.get("race_date", ""),
                r.get("finish_pos", 0),
                r.get("horse_no", 0),
                round(r.get("finish_time_sec", 0), 1),
            )
            existing_keys[cid].add(key)

    # 対象ファイルを収集（.lz4 優先、なければ .html）
    venue_pat = "|".join(venues)
    pat = re.compile(
        r"race\.netkeiba\.com_race_result\.html_race_id=(\d{4})(0[12])\d+\.html(?:\.lz4)?$"
    )
    found = {}  # race_id -> filepath
    for fname in os.listdir(CACHE_DIR_PATH):
        m = pat.match(fname)
        if not m:
            continue
        if m.group(2) not in venues:
            continue
        race_id = fname.split("race_id=")[1].split(".html")[0]
        fpath = os.path.join(CACHE_DIR_PATH, fname)
        # lz4 優先
        if race_id not in found:
            found[race_id] = fpath
        elif fname.endswith(".lz4") and not found[race_id].endswith(".lz4"):
            found[race_id] = fpath

    if verbose:
        venue_names = {"01": "札幌", "02": "函館"}
        for v in venues:
            cnt = sum(1 for rid in found if rid[4:6] == v)
            print(f"venue {v} ({venue_names.get(v, v)}): {cnt} ファイル")
        print(f"合計: {len(found)} ファイル")

    if not found:
        print("対象ファイルなし。")
        return 0

    added = 0
    skipped = 0
    t0 = time.time()

    for i, (race_id, fpath) in enumerate(sorted(found.items())):
        try:
            html = _read_html(fpath)
        except Exception as e:
            skipped += 1
            continue

        soup = BeautifulSoup(html, "lxml")

        # 距離・馬場種別
        data_el = soup.select_one(".RaceData01")
        surface, distance = "芝", 1600
        if data_el:
            txt = data_el.get_text()
            sm = re.search(r"(芝|ダ|障)", txt)
            dm = re.search(r"(\d{3,4})m", txt)
            if sm:
                surface = "芝" if sm.group(1) == "芝" else "ダート"
            if dm:
                distance = int(dm.group(1))

        cond_el = re.search(r"馬場[：:]([良稍重不])", soup.get_text())
        condition = cond_el.group(1) if cond_el else "良"

        name_el = soup.select_one(".RaceName")
        race_name = name_el.get_text(strip=True) if name_el else ""

        venue_code = race_id[4:6]
        field_count = 16
        fc_el = soup.select_one(".RaceData02")
        if fc_el:
            fm = re.search(r"(\d+)頭", fc_el.get_text())
            if fm:
                field_count = int(fm.group(1))

        first_3f, race_last3f, pace_letter = _parse_full_lap_data(soup, distance)
        pace_from_lap = {"H": PaceType.H, "M": PaceType.M, "S": PaceType.S}.get(pace_letter)

        past_runs = _parse_result_to_past_runs(
            soup,
            race_id,
            venue_code,
            surface,
            distance,
            condition,
            race_name,
            field_count,
            first_3f_sec=first_3f,
            race_pace=pace_from_lap,
        )

        for pr in past_runs:
            cid = pr.course_id
            dup_key = (
                pr.race_date,
                pr.finish_pos,
                pr.horse_no,
                round(pr.finish_time_sec, 1),
            )
            if cid not in existing_keys:
                existing_keys[cid] = set()
            if dup_key in existing_keys[cid]:
                continue
            existing_keys[cid].add(dup_key)
            if cid not in all_runs:
                all_runs[cid] = []
            all_runs[cid].append(_past_run_to_dict(pr))
            added += 1

        if verbose and (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            print(f"  {i+1}/{len(found)} 処理, 追加: {added} 走, {elapsed:.1f}s")

    # 保存
    tmp_path = COURSE_DB_PRELOAD_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump({"version": 1, "course_db": all_runs}, f, ensure_ascii=False, indent=0)
    for _retry in range(5):
        try:
            os.replace(tmp_path, COURSE_DB_PRELOAD_PATH)
            break
        except PermissionError:
            if _retry == 4:
                import shutil
                shutil.copy2(tmp_path, COURSE_DB_PRELOAD_PATH)
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            else:
                time.sleep(2)

    elapsed = time.time() - t0
    if verbose:
        venue_counts = {}
        for k in all_runs:
            v = k[:2]
            venue_counts[v] = venue_counts.get(v, 0) + 1
        print(f"\n完了: {added} 走追加, スキップ: {skipped}, {elapsed:.1f}秒")
        print("更新後 course_db:")
        for v in sorted(venue_counts):
            print(f"  venue {v}: {venue_counts[v]} コース")
        print(f"  合計: {len(all_runs)} コース, {sum(len(v) for v in all_runs.values())} 走")
    return added


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--venues", nargs="+", default=["01", "02"],
                    help="補完する venue コード (デフォルト: 01 02)")
    args = ap.parse_args()
    supplement_venues(venues=args.venues)
