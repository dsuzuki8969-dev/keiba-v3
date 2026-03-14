"""
MLデータの pace / first_3f バックフィル

既存の data/ml/*.json に pace="" のレースについて、
キャッシュ済みHTMLから再パースして pace / first_3f を埋める。
新規スクレイピングは行わない（キャッシュのみ使用）。
"""

import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import lz4.frame as lz4f
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

from bs4 import BeautifulSoup
from data.masters.venue_master import JRA_CODES, get_venue_code_from_race_id
from src.scraper.ml_data_collector import _parse_full_lap_data

ML_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "ml")
CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "cache")

# 負荷制御
BATCH_SIZE = 50
SLEEP_SEC = 0.05


def _read_cache(cache_path: str) -> str:
    """lz4 → plain の順でキャッシュを読む"""
    lz4_path = cache_path + ".lz4"
    if HAS_LZ4 and os.path.exists(lz4_path):
        try:
            with open(lz4_path, "rb") as f:
                return lz4f.decompress(f.read()).decode("utf-8")
        except Exception:
            pass
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            pass
    return ""


def _cache_path_for_race(race_id: str) -> str:
    """race_id からキャッシュファイルパスを生成"""
    vc = get_venue_code_from_race_id(race_id)
    is_jra = vc in JRA_CODES
    if is_jra:
        domain = "race.netkeiba.com"
    else:
        domain = "nar.netkeiba.com"
    fname = f"{domain}_race_result.html_race_id={race_id}.html"
    return os.path.join(CACHE_DIR, fname)


def main():
    files = sorted(
        f for f in os.listdir(ML_DIR)
        if f.endswith(".json") and f[:-5].isdigit()
    )
    print(f"ML files: {len(files)}")

    updated_files = 0
    updated_races = 0
    cache_miss = 0
    already_ok = 0
    processed = 0

    for fi, fname in enumerate(files):
        fpath = os.path.join(ML_DIR, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        races = data.get("races", [])
        file_changed = False

        for race in races:
            race_id = race.get("race_id", "")
            old_pace = race.get("pace", "")
            old_3f = race.get("first_3f")

            # 既にペースが入っているレースはスキップ
            if old_pace and old_3f is not None:
                already_ok += 1
                continue

            # キャッシュからHTML読み込み
            cache_path = _cache_path_for_race(race_id)
            html = _read_cache(cache_path)
            if not html:
                cache_miss += 1
                continue

            soup = BeautifulSoup(html, "html.parser")
            distance = race.get("distance", 1600)
            lap_data = _parse_full_lap_data(soup, distance)

            changed = False
            if not old_pace and lap_data["pace"]:
                race["pace"] = lap_data["pace"]
                changed = True
            if old_3f is None and lap_data["first_3f"] is not None:
                race["first_3f"] = lap_data["first_3f"]
                changed = True

            if changed:
                file_changed = True
                updated_races += 1

            processed += 1
            if processed % BATCH_SIZE == 0:
                time.sleep(SLEEP_SEC)

        if file_changed:
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            updated_files += 1

        if (fi + 1) % 100 == 0:
            print(f"  [{fi + 1}/{len(files)}] updated {updated_races} races in {updated_files} files")

    print(f"\nDone.")
    print(f"  Files updated: {updated_files}")
    print(f"  Races updated: {updated_races}")
    print(f"  Already OK: {already_ok}")
    print(f"  Cache miss: {cache_miss}")


if __name__ == "__main__":
    main()
