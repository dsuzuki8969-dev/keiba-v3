"""
パイプライン診断スクリプト — ブレンド各ステージの情報損失を定量分析

各ステージの中間確率（raw_lgbm_prob, ensemble_prob, ml_rule_prob, pre_pop_prob, win_prob）
と実際の着順を突合し、精度指標を算出。

使い方:
  python scripts/pipeline_diagnostic.py                   # 全期間
  python scripts/pipeline_diagnostic.py --year 2026       # 2026年のみ
  python scripts/pipeline_diagnostic.py --year 2026 --jra # JRAのみ
  python scripts/pipeline_diagnostic.py --year 2026 --nar # NARのみ
"""
import argparse
import io
import json
import math
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from config.settings import PREDICTIONS_DIR, RESULTS_DIR

# ============================================================
# ステージ定義
# ============================================================
STAGES = [
    ("raw_lgbm_prob",  "LightGBM生値"),
    ("ensemble_prob",  "アンサンブル後"),
    ("ml_rule_prob",   "ML+Ruleブレンド後"),
    ("pre_pop_prob",   "人気統計ブレンド前"),
    ("win_prob",       "最終値"),
]


def _load_results(date: str) -> Optional[dict]:
    """結果JSONを読み込む"""
    fpath = os.path.join(RESULTS_DIR, f"{date.replace('-', '')}_results.json")
    if os.path.exists(fpath):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _load_prediction(date: str) -> Optional[dict]:
    """予想JSONを読み込む"""
    fpath = os.path.join(PREDICTIONS_DIR, f"{date.replace('-', '')}_pred.json")
    if os.path.exists(fpath):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _list_dates(year_filter: str = "all") -> List[str]:
    """予想済み日付一覧"""
    dates = []
    if not os.path.exists(PREDICTIONS_DIR):
        return dates
    for f in sorted(os.listdir(PREDICTIONS_DIR)):
        if f.endswith("_pred.json"):
            raw = f.replace("_pred.json", "")
            if len(raw) == 8:
                d = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
                if year_filter == "all" or d.startswith(year_filter):
                    dates.append(d)
    return dates


def _is_jra_race(race_id: str) -> bool:
    """JRAレースか判定"""
    try:
        venue_code = race_id[4:6]
        return venue_code in {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
    except Exception:
        return True


def _get_venue_name(race_id: str) -> str:
    """race_idから競馬場名を取得"""
    try:
        from data.masters.venue_master import VENUE_CODE_TO_NAME
        return VENUE_CODE_TO_NAME.get(race_id[4:6], f"不明({race_id[4:6]})")
    except Exception:
        return race_id[4:6]


# ============================================================
# 精度指標
# ============================================================

def brier_score(probs: List[float], actuals: List[int]) -> float:
    """Brier Score (低いほど良い)"""
    if not probs:
        return float("nan")
    return sum((p - a) ** 2 for p, a in zip(probs, actuals)) / len(probs)


def log_loss(probs: List[float], actuals: List[int], eps: float = 1e-7) -> float:
    """Log Loss (低いほど良い)"""
    if not probs:
        return float("nan")
    total = 0.0
    for p, a in zip(probs, actuals):
        p = max(eps, min(1 - eps, p))
        total += -(a * math.log(p) + (1 - a) * math.log(1 - p))
    return total / len(probs)


def top1_accuracy(race_groups: List[dict]) -> Tuple[float, int, int]:
    """各レースでwin_prob最大の馬が1着だった割合"""
    hits = 0
    total = 0
    for rg in race_groups:
        if not rg["horses"]:
            continue
        top_horse = max(rg["horses"], key=lambda h: h["prob"])
        total += 1
        if top_horse["finish"] == 1:
            hits += 1
    rate = hits / total * 100 if total > 0 else 0
    return rate, hits, total


def concentration_ratio(race_groups: List[dict]) -> Tuple[float, float]:
    """1位/2位のwin_prob比率の平均・標準偏差"""
    ratios = []
    for rg in race_groups:
        probs = sorted([h["prob"] for h in rg["horses"]], reverse=True)
        if len(probs) >= 2 and probs[1] > 0.001:
            ratios.append(probs[0] / probs[1])
    if not ratios:
        return 0.0, 0.0
    avg = sum(ratios) / len(ratios)
    std = (sum((r - avg) ** 2 for r in ratios) / len(ratios)) ** 0.5
    return avg, std


def calibration_buckets(probs: List[float], actuals: List[int], n_buckets: int = 10) -> List[dict]:
    """較正曲線: 予測確率バケット vs 実際の勝率"""
    if not probs:
        return []
    pairs = sorted(zip(probs, actuals))
    bucket_size = max(1, len(pairs) // n_buckets)
    buckets = []
    for i in range(0, len(pairs), bucket_size):
        chunk = pairs[i:i + bucket_size]
        if not chunk:
            continue
        avg_pred = sum(p for p, _ in chunk) / len(chunk)
        avg_actual = sum(a for _, a in chunk) / len(chunk)
        buckets.append({
            "pred": round(avg_pred, 4),
            "actual": round(avg_actual, 4),
            "count": len(chunk),
        })
    return buckets


# ============================================================
# メイン集計
# ============================================================

def collect_data(year_filter: str = "all", scope: str = "all") -> dict:
    """全データ収集"""
    dates = _list_dates(year_filter)

    # ステージ別: {stage_key: {"probs": [...], "actuals": [...], "races": [...]}}
    stage_data = {key: {"probs": [], "actuals": [], "races": []} for key, _ in STAGES}
    # 自信度別
    by_conf = defaultdict(lambda: {key: {"probs": [], "actuals": [], "races": []} for key, _ in STAGES})
    # モデルレベル別
    by_level = defaultdict(lambda: {key: {"probs": [], "actuals": [], "races": []} for key, _ in STAGES})
    # 競馬場別
    by_venue = defaultdict(lambda: {"probs": [], "actuals": [], "races": [], "is_jra": True})

    total_races = 0
    total_horses = 0
    has_diagnostic = 0  # 中間値ありのレース数

    for date in dates:
        pred = _load_prediction(date)
        result = _load_results(date)
        if not pred or not result:
            continue

        actual_map = {}
        if isinstance(result, dict) and "races" in result:
            for r in result["races"]:
                rid = r.get("race_id", "")
                if rid:
                    actual_map[rid] = {int(o["horse_no"]): o["finish"] for o in r.get("order", [])}
        elif isinstance(result, dict):
            for rid, rdata in result.items():
                if isinstance(rdata, dict) and "order" in rdata:
                    actual_map[rid] = {r["horse_no"]: r["finish"] for r in rdata["order"]}

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id:
                continue

            # スコープフィルタ
            is_jra = _is_jra_race(race_id)
            if scope == "jra" and not is_jra:
                continue
            if scope == "nar" and is_jra:
                continue

            finish_map = actual_map.get(race_id, {})
            if not finish_map:
                continue

            confidence = race.get("confidence", "B")
            venue_name = race.get("venue", "") or _get_venue_name(race_id)
            total_races += 1

            # レース内の全馬データを収集
            race_horses_by_stage = {key: [] for key, _ in STAGES}
            model_level = None

            for h in race.get("horses", []):
                hno = h.get("horse_no")
                pos = finish_map.get(hno, finish_map.get(str(hno), 99))
                if pos > 30:
                    continue  # 出走取消等
                is_win = 1 if pos == 1 else 0
                total_horses += 1

                ml = h.get("model_level")
                if ml is not None:
                    model_level = ml

                for stage_key, _ in STAGES:
                    prob = h.get(stage_key)
                    if prob is not None:
                        stage_data[stage_key]["probs"].append(prob)
                        stage_data[stage_key]["actuals"].append(is_win)
                        race_horses_by_stage[stage_key].append({"prob": prob, "finish": pos})

                        by_conf[confidence][stage_key]["probs"].append(prob)
                        by_conf[confidence][stage_key]["actuals"].append(is_win)

                        if ml is not None:
                            by_level[ml][stage_key]["probs"].append(prob)
                            by_level[ml][stage_key]["actuals"].append(is_win)

                # 競馬場別（win_probのみ）
                wp = h.get("win_prob")
                if wp is not None and venue_name:
                    by_venue[venue_name]["probs"].append(wp)
                    by_venue[venue_name]["actuals"].append(is_win)
                    by_venue[venue_name]["is_jra"] = is_jra

            # レース単位データ（Top-1精度・集中度用）
            for stage_key, _ in STAGES:
                horses = race_horses_by_stage[stage_key]
                if horses:
                    stage_data[stage_key]["races"].append({"horses": horses, "confidence": confidence})
                    by_conf[confidence][stage_key]["races"].append({"horses": horses})
                    if model_level is not None:
                        by_level[model_level][stage_key]["races"].append({"horses": horses})

            # 競馬場別レース単位（win_prob）
            wp_horses = race_horses_by_stage.get("win_prob", [])
            if wp_horses and venue_name:
                by_venue[venue_name]["races"].append({"horses": wp_horses})

            if any(h.get("raw_lgbm_prob") is not None for h in race.get("horses", [])):
                has_diagnostic += 1

    return {
        "total_races": total_races,
        "total_horses": total_horses,
        "has_diagnostic": has_diagnostic,
        "stage_data": stage_data,
        "by_conf": dict(by_conf),
        "by_level": dict(by_level),
        "by_venue": dict(by_venue),
    }


def print_report(data: dict):
    """診断レポートを出力"""
    print("=" * 70)
    print("パイプライン診断レポート")
    print("=" * 70)
    print(f"総レース数: {data['total_races']}")
    print(f"総馬数: {data['total_horses']}")
    print(f"中間値あり: {data['has_diagnostic']}レース")
    print()

    # ---- ステージ別精度 ----
    print("■ ステージ別精度指標")
    print("-" * 70)
    header = f"{'ステージ':20s} {'Brier':>8s} {'LogLoss':>8s} {'Top1%':>7s} {'集中度':>7s} {'N':>8s}"
    print(header)
    print("-" * 70)

    for stage_key, stage_name in STAGES:
        sd = data["stage_data"][stage_key]
        if not sd["probs"]:
            print(f"{stage_name:20s}   (データなし)")
            continue
        bs = brier_score(sd["probs"], sd["actuals"])
        ll = log_loss(sd["probs"], sd["actuals"])
        t1_rate, _, _ = top1_accuracy(sd["races"])
        cr_avg, _ = concentration_ratio(sd["races"])
        n = len(sd["probs"])
        print(f"{stage_name:20s} {bs:8.4f} {ll:8.4f} {t1_rate:6.1f}% {cr_avg:7.2f} {n:8d}")

    print()

    # ---- 自信度別 ----
    print("■ 自信度別 × ステージ別 Top-1精度(%)")
    print("-" * 70)
    conf_order = ["SS", "S", "A", "B", "C", "D", "E"]
    header = f"{'自信度':>6s}"
    for _, name in STAGES:
        header += f" {name[:6]:>8s}"
    header += f" {'N':>6s}"
    print(header)
    print("-" * 70)

    for conf in conf_order:
        if conf not in data["by_conf"]:
            continue
        cd = data["by_conf"][conf]
        line = f"{conf:>6s}"
        n_races = 0
        for stage_key, _ in STAGES:
            sd = cd[stage_key]
            if sd["races"]:
                t1, _, total = top1_accuracy(sd["races"])
                line += f" {t1:7.1f}%"
                n_races = max(n_races, total)
            else:
                line += f" {'—':>8s}"
        line += f" {n_races:6d}"
        print(line)

    print()

    # ---- モデルレベル別 ----
    print("■ モデルレベル別 × ステージ別 Top-1精度(%)")
    print("-" * 70)
    level_names = {4: "Lv4(競馬場)", 3: "Lv3(SMILE)", 2: "Lv2(JRA/NAR)", 1: "Lv1(馬場)", 0: "Lv0(global)"}
    header = f"{'レベル':>12s}"
    for _, name in STAGES:
        header += f" {name[:6]:>8s}"
    header += f" {'N':>6s}"
    print(header)
    print("-" * 70)

    for level in sorted(data["by_level"].keys(), reverse=True):
        ld = data["by_level"][level]
        lname = level_names.get(level, f"Lv{level}")
        line = f"{lname:>12s}"
        n_races = 0
        for stage_key, _ in STAGES:
            sd = ld[stage_key]
            if sd["races"]:
                t1, _, total = top1_accuracy(sd["races"])
                line += f" {t1:7.1f}%"
                n_races = max(n_races, total)
            else:
                line += f" {'—':>8s}"
        line += f" {n_races:6d}"
        print(line)

    print()

    # ---- 較正曲線（最終値のみ）----
    sd_final = data["stage_data"]["win_prob"]
    if sd_final["probs"]:
        print("■ 較正曲線（最終win_prob）")
        print("-" * 40)
        print(f"{'予測確率':>10s} {'実際勝率':>10s} {'件数':>8s}")
        print("-" * 40)
        for b in calibration_buckets(sd_final["probs"], sd_final["actuals"]):
            print(f"{b['pred']:10.4f} {b['actual']:10.4f} {b['count']:8d}")

    print()

    # ---- 情報損失の定量化 ----
    print("■ 情報損失の定量化（Brier Score の変化）")
    print("-" * 50)
    prev_bs = None
    for stage_key, stage_name in STAGES:
        sd = data["stage_data"][stage_key]
        if not sd["probs"]:
            continue
        bs = brier_score(sd["probs"], sd["actuals"])
        if prev_bs is not None:
            delta = bs - prev_bs
            direction = "↑悪化" if delta > 0 else "↓改善"
            print(f"  {stage_name:20s}: {bs:.4f} ({delta:+.4f} {direction})")
        else:
            print(f"  {stage_name:20s}: {bs:.4f} (基準)")
        prev_bs = bs

    print()

    # ---- 集中度の変化 ----
    print("■ 確信度保持（集中度 = 1位prob/2位prob 比率の変化）")
    print("-" * 50)
    for stage_key, stage_name in STAGES:
        sd = data["stage_data"][stage_key]
        if sd["races"]:
            avg, std = concentration_ratio(sd["races"])
            print(f"  {stage_name:20s}: 平均 {avg:.2f} (σ={std:.2f})")

    print()
    print("=" * 70)
    print("診断完了")


def print_venue_report(data: dict):
    """競馬場別の精度レポート"""
    by_venue = data.get("by_venue", {})
    if not by_venue:
        print("  競馬場別データなし")
        return

    print()
    print("=" * 80)
    print("■ 競馬場別精度（win_prob 最終値ベース）")
    print("=" * 80)

    # JRA / NAR に分けて表示
    for group_label, is_jra_filter in [("JRA", True), ("NAR", False)]:
        venues = {v: d for v, d in by_venue.items() if d["is_jra"] == is_jra_filter}
        if not venues:
            continue

        print(f"\n  【{group_label}】")
        print(f"  {'競馬場':>8s}  {'Brier':>8s}  {'LogLoss':>8s}  {'Top1%':>7s}  {'集中度':>7s}  {'レース':>6s}  {'馬数':>6s}")
        print(f"  {'-' * 60}")

        # Top-1精度でソート
        venue_rows = []
        for vname, vd in sorted(venues.items()):
            n_horses = len(vd["probs"])
            if n_horses < 10:
                continue
            bs = brier_score(vd["probs"], vd["actuals"])
            ll = log_loss(vd["probs"], vd["actuals"])
            t1_rate, _, n_races = top1_accuracy(vd["races"])
            cr_avg, _ = concentration_ratio(vd["races"])
            venue_rows.append((vname, bs, ll, t1_rate, cr_avg, n_races, n_horses))

        # Top1精度降順
        venue_rows.sort(key=lambda r: r[3], reverse=True)
        for vname, bs, ll, t1, cr, nr, nh in venue_rows:
            print(f"  {vname:>8s}  {bs:8.4f}  {ll:8.4f}  {t1:6.1f}%  {cr:7.2f}  {nr:6d}  {nh:6d}")

        # 全体平均
        all_probs = [p for vd in venues.values() for p in vd["probs"]]
        all_actuals = [a for vd in venues.values() for a in vd["actuals"]]
        all_races = [r for vd in venues.values() for r in vd["races"]]
        if all_probs:
            avg_bs = brier_score(all_probs, all_actuals)
            avg_ll = log_loss(all_probs, all_actuals)
            avg_t1, _, avg_nr = top1_accuracy(all_races)
            avg_cr, _ = concentration_ratio(all_races)
            print(f"  {'─' * 60}")
            print(f"  {group_label + '平均':>8s}  {avg_bs:8.4f}  {avg_ll:8.4f}  {avg_t1:6.1f}%  {avg_cr:7.2f}  {avg_nr:6d}  {len(all_probs):6d}")


def main():
    parser = argparse.ArgumentParser(description="パイプライン診断")
    parser.add_argument("--year", default="all", help="年フィルタ (例: 2026)")
    parser.add_argument("--jra", action="store_true", help="JRAのみ")
    parser.add_argument("--nar", action="store_true", help="NARのみ")
    parser.add_argument("--by-venue", action="store_true", help="競馬場別の精度を表示")
    args = parser.parse_args()

    scope = "all"
    if args.jra:
        scope = "jra"
    elif args.nar:
        scope = "nar"

    print(f"データ収集中... (year={args.year}, scope={scope})")
    data = collect_data(args.year, scope)

    if data["total_races"] == 0:
        print("対象レースがありません。")
        return

    print_report(data)

    if args.by_venue:
        print_venue_report(data)


if __name__ == "__main__":
    main()
