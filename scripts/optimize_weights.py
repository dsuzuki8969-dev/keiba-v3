"""
重み最適化スクリプト

予想JSONと実際の結果を照合し、
1. 各因子の予測力を分析（相関・的中寄与）
2. Optuna で COMPOSITE_WEIGHTS を最適化
3. 推奨重みを出力

使い方:
  python scripts/optimize_weights.py                 # 分析のみ
  python scripts/optimize_weights.py --optimize      # Optuna 最適化
  python scripts/optimize_weights.py --apply          # 最適重みを settings.py に反映
"""

import json
import os
import sys
from collections import defaultdict
from typing import Dict, List, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import COMPOSITE_WEIGHTS, PREDICTIONS_DIR, RESULTS_DIR, get_composite_weights


def load_all_data() -> List[dict]:
    """全日付の予想+結果を読み込み、馬単位のレコードを返す"""
    if not os.path.isdir(PREDICTIONS_DIR) or not os.path.isdir(RESULTS_DIR):
        return []

    records = []
    pred_files = sorted(f for f in os.listdir(PREDICTIONS_DIR) if f.endswith("_pred.json"))

    for pf in pred_files:
        date_key = pf.replace("_pred.json", "")
        rf = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
        if not os.path.exists(rf):
            continue

        with open(os.path.join(PREDICTIONS_DIR, pf), "r", encoding="utf-8") as f:
            pred = json.load(f)
        with open(rf, "r", encoding="utf-8") as f:
            actual = json.load(f)

        for race in pred.get("races", []):
            race_id = race["race_id"]
            result = actual.get(race_id)
            if not result:
                continue
            finish_map = {r["horse_no"]: r["finish"] for r in result.get("order", [])}
            n_horses = len(race.get("horses", []))

            for h in race["horses"]:
                hno = h["horse_no"]
                finish = finish_map.get(hno)
                if finish is None:
                    continue
                records.append({
                    "date": pred.get("date", ""),
                    "race_id": race_id,
                    "venue": race.get("venue", ""),
                    "surface": race.get("surface", ""),
                    "distance": race.get("distance", 0),
                    "field_count": n_horses,
                    "horse_no": hno,
                    "horse_name": h.get("horse_name", ""),
                    "composite": h.get("composite", 50),
                    "ability": h.get("ability_total", 50),
                    "pace": h.get("pace_total", 50),
                    "course": h.get("course_total", 50),
                    "win_prob": h.get("win_prob", 0),
                    "place3_prob": h.get("place3_prob", 0),
                    "odds": h.get("odds"),
                    "mark": h.get("mark", ""),
                    "finish": finish,
                    "is_winner": finish == 1,
                    "is_placed": finish <= 3,
                    "confidence": race.get("confidence", "B"),
                })
    return records


def analyze_factors(records: List[dict]) -> dict:
    """各因子（ability/pace/course）の予測力を分析"""
    if not records:
        return {"error": "結果データがありません"}

    from statistics import mean, stdev

    race_groups: Dict[str, List[dict]] = defaultdict(list)
    for r in records:
        race_groups[r["race_id"]].append(r)

    factor_names = ["composite", "ability", "pace", "course"]
    correlations = {f: [] for f in factor_names}

    for race_id, horses in race_groups.items():
        if len(horses) < 3:
            continue
        for factor in factor_names:
            vals = [(h[factor], h["finish"]) for h in horses if h[factor] is not None]
            if len(vals) < 3:
                continue
            rank_corr = _spearman_rank_corr(vals)
            correlations[factor].append(rank_corr)

    factor_analysis = {}
    for f in factor_names:
        if correlations[f]:
            avg_corr = mean(correlations[f])
            factor_analysis[f] = {
                "avg_spearman": round(avg_corr, 4),
                "std": round(stdev(correlations[f]), 4) if len(correlations[f]) > 1 else 0,
                "n_races": len(correlations[f]),
            }

    # 印別的中率
    mark_stats = defaultdict(lambda: {"total": 0, "win": 0, "placed": 0})
    for r in records:
        mk = r.get("mark", "")
        if mk in ("◎", "◉", "○", "▲", "△", "☆"):
            mark_stats[mk]["total"] += 1
            if r["is_winner"]:
                mark_stats[mk]["win"] += 1
            if r["is_placed"]:
                mark_stats[mk]["placed"] += 1
    mark_rates = {}
    for mk, st in mark_stats.items():
        t = st["total"]
        mark_rates[mk] = {
            "total": t,
            "win_rate": round(st["win"] / t * 100, 1) if t else 0,
            "place_rate": round(st["placed"] / t * 100, 1) if t else 0,
        }

    # 自信度別 ROI
    conf_stats = defaultdict(lambda: {"races": 0, "wins": 0})
    for r in records:
        if r["mark"] in ("◎", "◉"):
            conf_stats[r["confidence"]]["races"] += 1
            if r["is_winner"]:
                conf_stats[r["confidence"]]["wins"] += 1

    # 距離帯別の因子重要度
    dist_bands = {"短距離(~1400)": (0, 1400), "マイル(1401-1800)": (1401, 1800),
                  "中距離(1801-2400)": (1801, 2400), "長距離(2401~)": (2401, 9999)}
    dist_analysis = {}
    for band_name, (lo, hi) in dist_bands.items():
        band_races = defaultdict(list)
        for r in records:
            if lo <= r.get("distance", 0) <= hi:
                band_races[r["race_id"]].append(r)
        if not band_races:
            continue
        band_corrs = {f: [] for f in ["ability", "pace", "course"]}
        for rid, horses in band_races.items():
            if len(horses) < 3:
                continue
            for f in band_corrs:
                vals = [(h[f], h["finish"]) for h in horses if h[f] is not None]
                if len(vals) >= 3:
                    band_corrs[f].append(_spearman_rank_corr(vals))
        dist_analysis[band_name] = {
            f: round(mean(v), 4) if v else 0 for f, v in band_corrs.items()
        }

    return {
        "total_records": len(records),
        "total_races": len(race_groups),
        "factor_analysis": factor_analysis,
        "mark_rates": mark_rates,
        "confidence_stats": dict(conf_stats),
        "distance_analysis": dist_analysis,
        "current_weights": dict(COMPOSITE_WEIGHTS),
    }


def _spearman_rank_corr(pairs: List[Tuple[float, float]]) -> float:
    """Spearman順位相関係数（高い因子→低い着順＝負の相関が理想）"""
    n = len(pairs)
    if n < 3:
        return 0.0
    x_ranks = _rank_values([p[0] for p in pairs], ascending=False)
    y_ranks = _rank_values([p[1] for p in pairs], ascending=True)
    d_sq = sum((xr - yr) ** 2 for xr, yr in zip(x_ranks, y_ranks))
    return 1 - (6 * d_sq) / (n * (n ** 2 - 1))


def _rank_values(values: list, ascending: bool = True) -> List[float]:
    indexed = sorted(enumerate(values), key=lambda x: x[1], reverse=not ascending)
    ranks = [0.0] * len(values)
    for rank, (idx, _) in enumerate(indexed, 1):
        ranks[idx] = float(rank)
    return ranks


def optimize_weights(records: List[dict], n_trials: int = 200) -> dict:
    """Optuna で最適な COMPOSITE_WEIGHTS を探索"""
    try:
        import optuna
    except ImportError:
        return {"error": "optuna が必要です: pip install optuna"}

    optuna.logging.set_verbosity(optuna.logging.WARNING)

    race_groups: Dict[str, List[dict]] = defaultdict(list)
    for r in records:
        race_groups[r["race_id"]].append(r)

    race_list = [(rid, horses) for rid, horses in race_groups.items() if len(horses) >= 3]
    if len(race_list) < 5:
        return {"error": f"最適化に十分なレース数がありません（{len(race_list)}件）"}

    def objective(trial):
        w_ability = trial.suggest_float("ability", 0.3, 0.8)
        w_pace = trial.suggest_float("pace", 0.05, 0.5)
        w_course = trial.suggest_float("course", 0.02, 0.3)
        total_w = w_ability + w_pace + w_course
        w_ability /= total_w
        w_pace /= total_w
        w_course /= total_w

        total_corr = 0.0
        count = 0
        for rid, horses in race_list:
            scores = []
            finishes = []
            for h in horses:
                s = (h["ability"] * w_ability + h["pace"] * w_pace + h["course"] * w_course)
                scores.append(s)
                finishes.append(h["finish"])
            corr = _spearman_rank_corr(list(zip(scores, finishes)))
            total_corr += corr
            count += 1
        return total_corr / count if count > 0 else 0.0

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=n_trials)

    best = study.best_params
    total_w = best["ability"] + best["pace"] + best["course"]
    optimal = {
        "ability": round(best["ability"] / total_w, 3),
        "pace": round(best["pace"] / total_w, 3),
        "course": round(best["course"] / total_w, 3),
    }
    return {
        "optimal_weights": optimal,
        "best_correlation": round(study.best_value, 4),
        "current_weights": dict(COMPOSITE_WEIGHTS),
        "n_trials": n_trials,
        "n_races": len(race_list),
    }


def apply_weights(weights: dict) -> None:
    """最適重みを config/settings.py に書き込む"""
    settings_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.py")
    with open(settings_path, "r", encoding="utf-8") as f:
        content = f.read()

    import re
    new_block = (
        f'COMPOSITE_WEIGHTS = {{\n'
        f'    "ability": {weights["ability"]},\n'
        f'    "pace": {weights["pace"]},\n'
        f'    "course": {weights["course"]},\n'
        f'}}'
    )
    content = re.sub(
        r'COMPOSITE_WEIGHTS\s*=\s*\{[^}]+\}',
        new_block,
        content,
    )
    with open(settings_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"settings.py に反映: {weights}")


def main():
    try:
        from rich.console import Console
        from rich.table import Table
        console = Console()
    except ImportError:
        console = None

    records = load_all_data()

    if not records:
        print("予想・結果データがありません。")
        print(f"  予想: {PREDICTIONS_DIR}")
        print(f"  結果: {RESULTS_DIR}")
        print("まず run_analysis_date.py で予想 → run_results.py で結果取得してください。")
        return

    analysis = analyze_factors(records)

    if console:
        console.print(f"\n[bold]予測精度分析[/] ({analysis['total_records']}馬 / {analysis['total_races']}R)\n")

        t = Table(title="因子別 Spearman 順位相関（高いほど予測力が高い）")
        t.add_column("因子", style="cyan")
        t.add_column("平均相関", justify="right")
        t.add_column("標準偏差", justify="right")
        t.add_column("レース数", justify="right")
        t.add_column("現在重み", justify="right")
        for f, v in analysis.get("factor_analysis", {}).items():
            w = analysis["current_weights"].get(f, "-")
            t.add_row(f, f"{v['avg_spearman']:.4f}", f"{v['std']:.4f}", str(v['n_races']), str(w))
        console.print(t)

        if analysis.get("mark_rates"):
            mt = Table(title="印別的中率")
            mt.add_column("印")
            mt.add_column("母数", justify="right")
            mt.add_column("勝率", justify="right")
            mt.add_column("複勝率", justify="right")
            for mk in ["◎", "○", "▲", "△", "☆"]:
                if mk in analysis["mark_rates"]:
                    r = analysis["mark_rates"][mk]
                    mt.add_row(mk, str(r["total"]), f"{r['win_rate']}%", f"{r['place_rate']}%")
            console.print(mt)

        if analysis.get("distance_analysis"):
            dt = Table(title="距離帯別 因子相関")
            dt.add_column("距離帯", style="cyan")
            dt.add_column("ability", justify="right")
            dt.add_column("pace", justify="right")
            dt.add_column("course", justify="right")
            for band, corrs in analysis["distance_analysis"].items():
                dt.add_row(band, f"{corrs.get('ability', 0):.4f}",
                           f"{corrs.get('pace', 0):.4f}", f"{corrs.get('course', 0):.4f}")
            console.print(dt)
    else:
        print(json.dumps(analysis, indent=2, ensure_ascii=False))

    if "--optimize" in sys.argv or "--apply" in sys.argv:
        print("\nOptuna 最適化実行中...")
        result = optimize_weights(records)
        if "error" in result:
            print(f"  エラー: {result['error']}")
            return
        print(f"\n最適重み: {result['optimal_weights']}")
        print(f"現在重み: {result['current_weights']}")
        print(f"最良相関: {result['best_correlation']} ({result['n_races']}R)")

        if "--apply" in sys.argv:
            apply_weights(result["optimal_weights"])


if __name__ == "__main__":
    main()
