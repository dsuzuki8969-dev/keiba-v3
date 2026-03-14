"""
人気別・オッズレンジ別・頭数別の勝率/連対率/複勝率統計テーブルを生成する。

出力: data/popularity_rates.json

使い方:
  python scripts/build_popularity_stats.py
"""

import json
import glob
import os
import sys
from collections import defaultdict
from datetime import datetime

# プロジェクトルート
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from config.settings import PREDICTIONS_DIR, RESULTS_DIR, DATA_DIR

# ============================================================
# 定数
# ============================================================

VENUE_NAMES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
    "30": "門別", "31": "帯広", "35": "盛岡", "36": "水沢", "42": "浦和",
    "43": "船橋", "44": "大井", "45": "川崎", "46": "金沢", "47": "笠松",
    "48": "名古屋", "50": "園田", "51": "姫路", "54": "高知", "55": "佐賀",
}

ODDS_RANGES = [
    (1.0, 1.9, "1.0-1.9"),
    (2.0, 2.9, "2.0-2.9"),
    (3.0, 4.9, "3.0-4.9"),
    (5.0, 9.9, "5.0-9.9"),
    (10.0, 19.9, "10.0-19.9"),
    (20.0, 49.9, "20.0-49.9"),
    (50.0, 9999.0, "50.0+"),
]

# 頭数区分
FIELD_SIZE_BINS = [
    (1, 8, "small"),
    (9, 14, "medium"),
    (15, 99, "large"),
]

MAX_POPULARITY = 18


def _venue_code(race_id: str) -> str:
    return race_id[4:6]


def _is_jra(race_id: str) -> bool:
    vc = int(_venue_code(race_id))
    return 1 <= vc <= 10


def _org_key(race_id: str) -> str:
    return "JRA" if _is_jra(race_id) else "NAR"


def _venue_name(race_id: str) -> str:
    vc = _venue_code(race_id)
    return VENUE_NAMES.get(vc, vc)


def _odds_range_key(odds: float) -> str:
    for lo, hi, key in ODDS_RANGES:
        if lo <= odds <= hi:
            return key
    return "50.0+"


def _field_size_bin(n: int) -> str:
    for lo, hi, key in FIELD_SIZE_BINS:
        if lo <= n <= hi:
            return key
    return "large"


def _new_counter():
    return {"win": 0, "top2": 0, "top3": 0, "n": 0}


def build():
    """メイン: 統計テーブルを構築して JSON 出力"""

    # --- 集計用の入れ子 dict ---
    # by_popularity[org][venue_or_overall][pop_str] = counter
    by_pop = defaultdict(lambda: defaultdict(lambda: defaultdict(_new_counter)))
    # by_odds_range[org][venue_or_overall][range_key] = counter
    by_odds = defaultdict(lambda: defaultdict(lambda: defaultdict(_new_counter)))
    # by_field_size[org][size_bin][pop_str] = counter
    by_fs = defaultdict(lambda: defaultdict(lambda: defaultdict(_new_counter)))

    # --- ファイル一覧 ---
    pred_files = sorted(glob.glob(os.path.join(PREDICTIONS_DIR, "*_pred.json")))
    result_map = {}
    for rf in glob.glob(os.path.join(RESULTS_DIR, "*_results.json")):
        date_str = os.path.basename(rf).split("_")[0]
        result_map[date_str] = rf

    processed = 0
    total_entries = 0

    for pf in pred_files:
        date_str = os.path.basename(pf).split("_")[0]
        rf = result_map.get(date_str)
        if not rf:
            continue

        try:
            with open(pf, "r", encoding="utf-8") as f:
                pred = json.load(f)
            with open(rf, "r", encoding="utf-8") as f:
                results = json.load(f)
        except Exception:
            continue

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id or race_id not in results:
                continue

            result = results[race_id]
            finish_map = {r["horse_no"]: r["finish"] for r in result.get("order", [])}
            if not finish_map:
                continue

            org = _org_key(race_id)
            vname = _venue_name(race_id)
            horses = race.get("horses", [])
            field_count = len(horses)
            fs_bin = _field_size_bin(field_count)

            for h in horses:
                pop = h.get("popularity")
                hno = h.get("horse_no")
                odds = h.get("odds")

                if pop is None or hno is None or pop < 1 or pop > MAX_POPULARITY:
                    continue
                finish = finish_map.get(hno)
                if finish is None:
                    continue

                pop_str = str(pop)
                is_win = finish == 1
                is_top2 = finish <= 2
                is_top3 = finish <= 3

                # --- by_popularity ---
                for venue_key in ("_overall", vname):
                    c = by_pop[org][venue_key][pop_str]
                    c["n"] += 1
                    if is_win:
                        c["win"] += 1
                    if is_top2:
                        c["top2"] += 1
                    if is_top3:
                        c["top3"] += 1

                # --- by_odds_range ---
                if odds is not None and odds > 0:
                    range_key = _odds_range_key(odds)
                    for venue_key in ("_overall", vname):
                        c = by_odds[org][venue_key][range_key]
                        c["n"] += 1
                        if is_win:
                            c["win"] += 1
                        if is_top2:
                            c["top2"] += 1
                        if is_top3:
                            c["top3"] += 1

                # --- by_field_size ---
                c = by_fs[org][fs_bin][pop_str]
                c["n"] += 1
                if is_win:
                    c["win"] += 1
                if is_top2:
                    c["top2"] += 1
                if is_top3:
                    c["top3"] += 1

                total_entries += 1

        processed += 1

    # --- カウント → 比率に変換 ---
    def _to_rates(counter_dict):
        result = {}
        for key, venues in counter_dict.items():
            result[key] = {}
            for venue, pops in venues.items():
                result[key][venue] = {}
                for pk, c in pops.items():
                    n = c["n"]
                    if n == 0:
                        continue
                    result[key][venue][pk] = {
                        "n": n,
                        "win": round(c["win"] / n, 4),
                        "top2": round(c["top2"] / n, 4),
                        "top3": round(c["top3"] / n, 4),
                    }
        return result

    pop_rates = _to_rates(by_pop)
    odds_rates = _to_rates(by_odds)
    fs_rates = _to_rates(by_fs)

    output = {
        "version": datetime.now().strftime("%Y-%m-%d"),
        "sample_days": processed,
        "total_entries": total_entries,
        "by_popularity": pop_rates,
        "by_odds_range": odds_rates,
        "by_field_size": fs_rates,
    }

    out_path = os.path.join(DATA_DIR, "popularity_rates.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"統計テーブル生成完了: {out_path}")
    print(f"  処理ファイル: {processed}日分")
    print(f"  エントリ数: {total_entries:,}")
    print(f"  JRA競馬場: {len(pop_rates.get('JRA', {})) - 1}場")
    print(f"  NAR競馬場: {len(pop_rates.get('NAR', {})) - 1}場")

    # サマリー表示
    for org in ["JRA", "NAR"]:
        overall = pop_rates.get(org, {}).get("_overall", {})
        if "1" in overall:
            d = overall["1"]
            print(f"  {org} 1番人気: 勝率{d['win']*100:.1f}% 連対{d['top2']*100:.1f}% 複勝{d['top3']*100:.1f}% (n={d['n']})")


if __name__ == "__main__":
    build()
