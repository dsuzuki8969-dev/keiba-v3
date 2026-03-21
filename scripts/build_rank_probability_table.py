"""
composite順位 × 頭数 → (勝率, 連対率, 複勝率) の実績確率テーブルを構築する。

出力: data/rank_probability_table.json

使い方:
  python scripts/build_rank_probability_table.py
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

# 頭数グループ（by_field_count でサンプル不足時のフォールバック用）
FIELD_SIZE_GROUPS = {
    "small": (5, 8),
    "medium": (9, 14),
    "large": (15, 18),
}

# ベイズ縮小推定の事前サンプル数（スムージング強度）
BAYESIAN_PRIOR_N = 50

# サンプル不足閾値（これ以下ならスムージング適用）
MIN_SAMPLE_THRESHOLD = 100


def _venue_code(race_id: str) -> str:
    return race_id[4:6]


def _is_jra(race_id: str) -> bool:
    vc = int(_venue_code(race_id))
    return 1 <= vc <= 10


def _org_key(race_id: str) -> str:
    return "JRA" if _is_jra(race_id) else "NAR"


def _field_group(n: int) -> str:
    for group, (lo, hi) in FIELD_SIZE_GROUPS.items():
        if lo <= n <= hi:
            return group
    if n < 5:
        return "small"
    return "large"


def _new_counter():
    return {"win": 0, "top2": 0, "top3": 0, "n": 0}


def build():
    """メイン: composite順位ベースの確率テーブルを構築"""

    # 集計: by_field_count[org][field_count_str][rank_str] = counter
    by_fc = defaultdict(lambda: defaultdict(lambda: defaultdict(_new_counter)))
    # 集計: by_field_group[org][group][rank_str] = counter
    by_fg = defaultdict(lambda: defaultdict(lambda: defaultdict(_new_counter)))

    # ファイル一覧
    pred_files = sorted(glob.glob(os.path.join(PREDICTIONS_DIR, "*_pred.json")))
    result_map = {}
    for rf in glob.glob(os.path.join(RESULTS_DIR, "*_results.json")):
        date_str = os.path.basename(rf).split("_")[0]
        result_map[date_str] = rf

    processed = 0
    total_races = 0
    total_entries = 0
    skipped_no_result = 0

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
                skipped_no_result += 1
                continue

            # ばんえい除外
            if _venue_code(race_id) == "65":
                continue

            result = results[race_id]
            finish_map = {r["horse_no"]: r["finish"]
                          for r in result.get("order", [])
                          if r.get("finish") is not None}
            if not finish_map:
                continue

            org = _org_key(race_id)
            horses = race.get("horses", [])

            # composite値でソートして順位を付与
            composites = []
            for h in horses:
                comp = h.get("composite")
                hno = h.get("horse_no")
                if comp is not None and hno is not None:
                    composites.append((comp, hno))

            if len(composites) < 3:
                continue

            # composite降順でソート → 順位付与（同値は出現順で処理）
            composites.sort(key=lambda x: -x[0])
            field_count = len(composites)
            fc_str = str(field_count)
            fg = _field_group(field_count)

            total_races += 1

            for rank_0, (comp, hno) in enumerate(composites):
                rank = rank_0 + 1  # 1-indexed
                finish = finish_map.get(hno)
                if finish is None:
                    continue

                rank_str = str(rank)
                is_win = finish == 1
                is_top2 = finish <= 2
                is_top3 = finish <= 3

                # by_field_count に集計
                c = by_fc[org][fc_str][rank_str]
                c["n"] += 1
                if is_win:
                    c["win"] += 1
                if is_top2:
                    c["top2"] += 1
                if is_top3:
                    c["top3"] += 1

                # by_field_group に集計
                c = by_fg[org][fg][rank_str]
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
    def _counters_to_rates(counter_dict):
        """集計カウンターを比率に変換"""
        result = {}
        for org, fc_dict in counter_dict.items():
            result[org] = {}
            for fc, ranks in fc_dict.items():
                result[org][fc] = {}
                for rank_str, c in sorted(ranks.items(), key=lambda x: int(x[0])):
                    n = c["n"]
                    if n == 0:
                        continue
                    result[org][fc][rank_str] = {
                        "win": round(c["win"] / n, 4),
                        "top2": round(c["top2"] / n, 4),
                        "top3": round(c["top3"] / n, 4),
                        "n": n,
                    }
        return result

    fc_rates = _counters_to_rates(by_fc)
    fg_rates = _counters_to_rates(by_fg)

    # --- スムージング: サンプル不足セルはfield_groupの値でベイズ縮小推定 ---
    for org in fc_rates:
        for fc_str, ranks in fc_rates[org].items():
            fg = _field_group(int(fc_str))
            fg_data = fg_rates.get(org, {}).get(fg, {})

            for rank_str, entry in ranks.items():
                if entry["n"] >= MIN_SAMPLE_THRESHOLD:
                    continue
                # ベイズ縮小: smoothed = (n * obs + prior_n * prior) / (n + prior_n)
                prior = fg_data.get(rank_str)
                if prior is None:
                    continue
                n = entry["n"]
                prior_n = BAYESIAN_PRIOR_N
                for target in ("win", "top2", "top3"):
                    observed = entry[target]
                    prior_val = prior[target]
                    smoothed = (n * observed + prior_n * prior_val) / (n + prior_n)
                    entry[target] = round(smoothed, 4)

    # --- 整合性保証 ---
    def _enforce_consistency(rates_dict):
        """個馬制約(win<=top2<=top3)と合計正規化を適用"""
        for org in rates_dict:
            for fc, ranks in rates_dict[org].items():
                # 個馬制約
                for rank_str, entry in ranks.items():
                    entry["top2"] = max(entry["top2"], entry["win"])
                    entry["top3"] = max(entry["top3"], entry["top2"])

                # 合計正規化
                if not ranks:
                    continue
                n_field = max(int(k) for k in ranks.keys())

                for target, expected in [("win", 1.0), ("top2", min(n_field, 2.0)), ("top3", min(n_field, 3.0))]:
                    total = sum(entry[target] for entry in ranks.values())
                    if total > 0 and abs(total - expected) > 0.01:
                        ratio = expected / total
                        for entry in ranks.values():
                            entry[target] = round(entry[target] * ratio, 4)

    _enforce_consistency(fc_rates)
    _enforce_consistency(fg_rates)

    # --- 出力 ---
    output = {
        "meta": {
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sample_days": processed,
            "total_races": total_races,
            "total_entries": total_entries,
            "skipped_no_result": skipped_no_result,
        },
        "by_field_count": fc_rates,
        "by_field_group": fg_rates,
    }

    out_path = os.path.join(DATA_DIR, "rank_probability_table.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"順位ベース確率テーブル生成完了: {out_path}")
    print(f"  処理日数: {processed}日")
    print(f"  レース数: {total_races:,}")
    print(f"  エントリ数: {total_entries:,}")
    print(f"  スキップ(結果なし): {skipped_no_result:,}")

    # サマリー表示
    for org in ["JRA", "NAR"]:
        fg_data = fg_rates.get(org, {})
        for group in ["small", "medium", "large"]:
            data = fg_data.get(group, {})
            if "1" in data:
                d = data["1"]
                print(f"  {org} {group} 1位: "
                      f"勝率{d['win']*100:.1f}% "
                      f"連対{d['top2']*100:.1f}% "
                      f"複勝{d['top3']*100:.1f}% (n={d['n']})")

    # 頭数別詳細（JRA medium のみ表示）
    print("\n  --- JRA medium (9-14頭) 詳細 ---")
    for fc in range(9, 15):
        fc_data = fc_rates.get("JRA", {}).get(str(fc), {})
        if "1" in fc_data:
            d = fc_data["1"]
            print(f"  {fc}頭立て 1位: "
                  f"勝{d['win']*100:.1f}% "
                  f"連{d['top2']*100:.1f}% "
                  f"複{d['top3']*100:.1f}% (n={d['n']})")


if __name__ == "__main__":
    build()
