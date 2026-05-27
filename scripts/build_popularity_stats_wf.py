"""
WF (Walk-Forward) バックテスト用 期間別 人気別統計テーブルを生成する。

通常の popularity_rates.json は全期間の pred.json で集計するため循環参照になる
(学習リーク L-2 問題)。このスクリプトは各 WF 期間の train_max 以前のデータのみ
集計し、リークなしの統計テーブルを出力する。

出力:
  data/popularity_rates_wf_2024.json  (train_max: 2023-12-31 以前のみ)
  data/popularity_rates_wf_2025.json  (train_max: 2024-12-31 以前のみ)
  data/popularity_rates_wf_2026.json  (train_max: 2025-12-31 以前のみ)

使い方:
  python scripts/build_popularity_stats_wf.py
  python scripts/build_popularity_stats_wf.py --period wf_2025  # 1期間のみ
"""

import argparse
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

# プロジェクトルート
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from config.settings import PREDICTIONS_DIR, RESULTS_DIR, DATA_DIR

# ============================================================
# WF 期間定義 (wf_inference.py と同期)
# ============================================================
WF_PERIODS = {
    "wf_2024": "2023-12-31",  # train_max: この日付以前のデータのみ集計
    "wf_2025": "2024-12-31",
    "wf_2026": "2025-12-31",
}

# ============================================================
# 統計定数 (build_popularity_stats.py と同じ)
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

FIELD_SIZE_BINS = [
    (1, 8, "small"),
    (9, 14, "medium"),
    (15, 99, "large"),
]

MAX_POPULARITY = 18


# ============================================================
# ユーティリティ関数 (build_popularity_stats.py と同等)
# ============================================================

def _venue_code(race_id: str) -> str:
    """race_id から会場コードを取得"""
    return race_id[4:6]


def _is_jra(race_id: str) -> bool:
    """race_id から JRA 判定"""
    vc = int(_venue_code(race_id))
    return 1 <= vc <= 10


def _org_key(race_id: str) -> str:
    """race_id から JRA/NAR キーを取得"""
    return "JRA" if _is_jra(race_id) else "NAR"


def _venue_name(race_id: str) -> str:
    """race_id から競馬場名を取得"""
    vc = _venue_code(race_id)
    return VENUE_NAMES.get(vc, vc)


def _odds_range_key(odds: float) -> str:
    """オッズからレンジキーを取得"""
    for lo, hi, key in ODDS_RANGES:
        if lo <= odds <= hi:
            return key
    return "50.0+"


def _field_size_bin(n: int) -> str:
    """頭数から区分キーを取得"""
    for lo, hi, key in FIELD_SIZE_BINS:
        if lo <= n <= hi:
            return key
    return "large"


def _new_counter():
    """カウンター初期値"""
    return {"win": 0, "top2": 0, "top3": 0, "n": 0}


def _pred_file_date(fpath: str) -> str:
    """pred.json のファイル名から日付文字列 YYYY-MM-DD を取得"""
    basename = os.path.basename(fpath)  # 例: 20240101_pred.json
    date_part = basename[:8]  # 20240101
    if len(date_part) == 8 and date_part.isdigit():
        return f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
    return ""


def _to_rates(counter_dict: dict) -> dict:
    """カウント dict → 比率 dict に変換"""
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


# ============================================================
# メイン集計関数
# ============================================================

def build_wf_stats(wf_name: str, train_max: str) -> dict:
    """指定 WF 期間の popularity 統計テーブルを構築して dict を返す

    Args:
        wf_name: WF 期間名 (例: "wf_2025")
        train_max: この日付以前のデータのみ集計 (例: "2024-12-31")

    Returns:
        dict: popularity_rates.json と同じ構造
    """
    print(f"\n{'='*60}")
    print(f"WF 期間別 popularity stats 構築: {wf_name}")
    print(f"  train_max: {train_max} 以前のデータのみ集計")
    print(f"{'='*60}")

    # --- 集計用の入れ子 dict ---
    by_pop = defaultdict(lambda: defaultdict(lambda: defaultdict(_new_counter)))
    by_odds = defaultdict(lambda: defaultdict(lambda: defaultdict(_new_counter)))
    by_fs = defaultdict(lambda: defaultdict(lambda: defaultdict(_new_counter)))

    # --- ファイル一覧 ---
    all_pred_files = sorted(glob.glob(os.path.join(PREDICTIONS_DIR, "*_pred.json")))

    # train_max 以前のファイルのみ対象
    target_pred_files = []
    for pf in all_pred_files:
        file_date = _pred_file_date(pf)
        if file_date and file_date <= train_max:
            target_pred_files.append(pf)

    print(f"  対象 pred.json: {len(target_pred_files)} ファイル "
          f"(全 {len(all_pred_files)} 中, <= {train_max})")

    if len(target_pred_files) == 0:
        print(f"  WARNING: {train_max} 以前の pred.json が存在しません。")
        print(f"  空の統計テーブルを出力します (WF backtest では全体統計にフォールバック)。")

    # --- results ファイルマップ ---
    result_map = {}
    for rf in glob.glob(os.path.join(RESULTS_DIR, "*_results.json")):
        date_str = os.path.basename(rf).split("_")[0]
        result_map[date_str] = rf

    processed = 0
    total_entries = 0
    skipped_no_result = 0

    for pf in target_pred_files:
        date_str = os.path.basename(pf).split("_")[0]
        rf = result_map.get(date_str)
        if not rf:
            skipped_no_result += 1
            continue

        try:
            with open(pf, "r", encoding="utf-8") as f:
                pred = json.load(f)
            with open(rf, "r", encoding="utf-8") as f:
                results = json.load(f)
        except Exception as e:
            print(f"  WARNING: ファイル読込失敗 ({date_str}): {e}")
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
        if processed % 50 == 0:
            print(f"  [{processed}/{len(target_pred_files)}] {processed/len(target_pred_files)*100:.0f}% "
                  f"- entries={total_entries:,}")

    # --- カウント → 比率に変換 ---
    pop_rates = _to_rates(by_pop)
    odds_rates = _to_rates(by_odds)
    fs_rates = _to_rates(by_fs)

    output = {
        "version": datetime.now().strftime("%Y-%m-%d"),
        "wf_name": wf_name,
        "train_max": train_max,
        "sample_days": processed,
        "skipped_no_result": skipped_no_result,
        "total_entries": total_entries,
        "by_popularity": pop_rates,
        "by_odds_range": odds_rates,
        "by_field_size": fs_rates,
    }

    # サマリー表示
    print(f"\n  集計完了:")
    print(f"    処理ファイル: {processed} 日分")
    print(f"    スキップ (results なし): {skipped_no_result}")
    print(f"    エントリ数: {total_entries:,}")
    if pop_rates:
        jra_venues = len(pop_rates.get("JRA", {})) - 1  # _overall を除く
        nar_venues = len(pop_rates.get("NAR", {})) - 1
        print(f"    JRA競馬場: {max(0, jra_venues)}場")
        print(f"    NAR競馬場: {max(0, nar_venues)}場")
        for org in ["JRA", "NAR"]:
            overall = pop_rates.get(org, {}).get("_overall", {})
            if "1" in overall:
                d = overall["1"]
                print(f"    {org} 1番人気: 勝率{d['win']*100:.1f}% 連対{d['top2']*100:.1f}% "
                      f"複勝{d['top3']*100:.1f}% (n={d['n']})")

    return output


# ============================================================
# メインエントリーポイント
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="WF バックテスト用 期間別 popularity 統計テーブル生成"
    )
    parser.add_argument(
        "--period",
        choices=["wf_2024", "wf_2025", "wf_2026", "all"],
        default="all",
        help="生成する WF 期間 (default: all)",
    )
    args = parser.parse_args()

    periods = (
        WF_PERIODS
        if args.period == "all"
        else {args.period: WF_PERIODS[args.period]}
    )

    print(f"=== WF 期間別 popularity stats 生成 ===")
    print(f"対象期間: {list(periods.keys())}")

    results_summary = {}
    for wf_name, train_max in periods.items():
        stats = build_wf_stats(wf_name, train_max)

        out_path = os.path.join(DATA_DIR, f"popularity_rates_{wf_name}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

        print(f"\n  出力完了: {out_path}")
        print(f"  (sample_days={stats['sample_days']}, total_entries={stats['total_entries']:,})")

        results_summary[wf_name] = {
            "path": out_path,
            "sample_days": stats["sample_days"],
            "total_entries": stats["total_entries"],
        }

    print(f"\n{'='*60}")
    print(f"全期間生成完了:")
    for wf_name, s in results_summary.items():
        print(f"  {wf_name}: {s['sample_days']} 日分, {s['total_entries']:,} entries → {s['path']}")

    # 旧 popularity_rates.json との比較
    old_path = os.path.join(DATA_DIR, "popularity_rates.json")
    if os.path.exists(old_path):
        try:
            with open(old_path, "r", encoding="utf-8") as f:
                old_stats = json.load(f)
            print(f"\n  旧 popularity_rates.json: "
                  f"sample_days={old_stats.get('sample_days')}, "
                  f"total_entries={old_stats.get('total_entries', 0):,}")
            print(f"  (全期間集計のため循環参照あり — WF backtest には使用不可)")
        except Exception:
            pass


if __name__ == "__main__":
    main()
