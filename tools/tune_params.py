"""
パラメータチューニングスクリプト
予想JSON + 実結果JSONから、異なるパラメータで印・自信度を再計算し最適解を探索。
フル分析パイプラインを再実行せずに高速にバックテスト可能。
"""
import json
import os
import sys
import glob
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PRED_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "predictions")
RESULT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "results")


def load_all_predictions(year: str = "2026") -> List[dict]:
    """全予想JSONを読み込む"""
    preds = []
    pattern = os.path.join(PRED_DIR, f"{year}*_pred.json")
    for fpath in sorted(glob.glob(pattern)):
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
        preds.append(data)
    return preds


def load_result(date_str: str) -> Optional[dict]:
    """日付に対応する実結果JSONを読み込む"""
    # date_str: "2026-03-08" → "20260308"
    ds = date_str.replace("-", "")
    for fname in [f"{ds}_results.json", f"{ds}_actual.json"]:
        fpath = os.path.join(RESULT_DIR, fname)
        if os.path.exists(fpath):
            with open(fpath, "r", encoding="utf-8") as f:
                return json.load(f)
    return None


def reassign_marks_sim(
    horses: List[dict],
    tekipan_gap: float = 5.0,
    tekipan_win_prob: float = 0.30,
    ml_adj_clamp: float = 6.0,
    ml_adj_coeff: float = 2.5,
) -> List[dict]:
    """印を再割り当て（シミュレーション用）

    JSONのcomposite値をそのまま使用。ml_composite_adjの再計算は行わない
    （古いJSONでadj=0の場合に二重カウントになるため）。
    """
    n = len(horses)
    if n < 2:
        return horses

    # compositeをそのまま使用（再計算しない）
    for h in horses:
        h["_sim_composite"] = h.get("composite", 50.0)

    # composite順でソート
    sorted_h = sorted(horses, key=lambda h: h["_sim_composite"], reverse=True)

    # 全印クリア
    for h in horses:
        h["_sim_mark"] = ""

    # ☆/×は元の印を維持
    for h in horses:
        mk = h.get("mark", "")
        if mk in ("☆", "×"):
            h["_sim_mark"] = mk

    # composite順に印付与
    MARK_SEQ = ["○", "▲", "△", "★"]
    mark_idx = 0
    for h in sorted_h:
        if h["_sim_mark"]:
            continue
        if mark_idx == 0:
            c1 = h["_sim_composite"]
            c2 = sorted_h[1]["_sim_composite"] if len(sorted_h) > 1 else 0
            gap = c1 - c2
            wp = h.get("win_prob", 0)
            is_tekipan = gap >= tekipan_gap and wp >= tekipan_win_prob
            h["_sim_mark"] = "◉" if is_tekipan else "◎"
            mark_idx += 1
        elif mark_idx <= len(MARK_SEQ):
            h["_sim_mark"] = MARK_SEQ[mark_idx - 1]
            mark_idx += 1
        else:
            break

    return horses


def calc_confidence_sim(
    horses: List[dict],
    ss_gap_min: float = 4.0,
) -> str:
    """自信度を再計算（シミュレーション用）"""
    sorted_h = sorted(horses, key=lambda h: h.get("_sim_composite", h.get("composite", 0)), reverse=True)
    if len(sorted_h) < 3:
        return "SS" if len(sorted_h) >= 1 else "D"

    top = sorted_h[0]
    top_id = top.get("horse_id", top.get("horse_no"))

    # 1. composite差
    gap = sorted_h[0].get("_sim_composite", 0) - sorted_h[1].get("_sim_composite", 0)
    gap_norm = min(gap / 8.0, 1.0)

    # 2. ML一致度
    sorted_wp = sorted(horses, key=lambda h: h.get("win_prob", 0), reverse=True)
    wp_top_id = sorted_wp[0].get("horse_id", sorted_wp[0].get("horse_no"))
    if wp_top_id == top_id:
        ml_agreement = 1.0
    elif len(sorted_wp) >= 2 and sorted_wp[1].get("horse_id", sorted_wp[1].get("horse_no")) == top_id:
        ml_agreement = 0.5
    else:
        ml_agreement = 0.0

    # 3. 2-3位差
    gap23 = sorted_h[1].get("_sim_composite", 0) - sorted_h[2].get("_sim_composite", 0)
    gap23_norm = min(gap23 / 4.0, 1.0)

    # 4. オッズ一致度
    pop = top.get("popularity")
    if pop is not None and pop <= 1:
        odds_agreement = 1.0
    elif pop is not None and pop <= 2:
        odds_agreement = 0.6
    elif pop is not None and pop <= 3:
        odds_agreement = 0.3
    else:
        odds_agreement = 0.0

    # 5. 因子間合意（JSONから直接は困難なので省略 → compositeスコアで代用）
    multi_factor = 0.5  # デフォルト中間値

    # 6. 信頼度（JSONから直接は困難なので省略）
    reliability_norm = 0.5

    score = (
        gap_norm * 0.20
        + ml_agreement * 0.25
        + gap23_norm * 0.10
        + odds_agreement * 0.20
        + multi_factor * 0.15
        + reliability_norm * 0.10
    )

    # SS硬性条件
    if score >= 0.70:
        ml_match = wp_top_id == top_id
        odds_match = pop is not None and pop <= 2
        gap_check = gap >= ss_gap_min
        if ml_match and odds_match and gap_check:
            return "SS"
        else:
            return "S"
    elif score >= 0.55:
        return "S"
    elif score >= 0.42:
        return "A"
    elif score >= 0.30:
        return "B"
    elif score >= 0.20:
        return "C"
    else:
        return "D"


def evaluate_params(
    tekipan_gap: float,
    tekipan_win_prob: float,
    ss_gap_min: float,
) -> dict:
    """指定パラメータで全予想を再評価し成績を集計"""
    preds = load_all_predictions()

    mark_stats = defaultdict(lambda: {"total": 0, "win": 0, "place2": 0, "placed": 0})
    conf_stats = defaultdict(lambda: {"races": 0, "hits": 0, "stake": 0, "ret": 0})
    total_races = 0

    for pred_data in preds:
        date_str = pred_data.get("date", "")
        result_data = load_result(date_str)
        if not result_data:
            continue

        for race in pred_data.get("races", []):
            race_id = race.get("race_id", "")
            result = result_data.get(race_id)
            if not result:
                continue

            finish_map = {r["horse_no"]: r["finish"] for r in result.get("order", [])}
            if not finish_map:
                continue
            payouts = result.get("payouts", {})

            horses = race.get("horses", [])
            if not horses:
                continue

            total_races += 1

            # 印を再割り当て（compositeはJSON値をそのまま使用）
            reassign_marks_sim(horses, tekipan_gap, tekipan_win_prob)

            # 自信度を再計算
            sim_conf = calc_confidence_sim(horses, ss_gap_min)

            # 印別集計
            honmei_hno = None
            for h in horses:
                mk = h.get("_sim_mark", "")
                pos = finish_map.get(h["horse_no"], 99)

                if mk in ("◉", "◎", "○", "▲", "△", "★", "☆", "×"):
                    mark_stats[mk]["total"] += 1
                    if pos == 1:
                        mark_stats[mk]["win"] += 1
                    if pos <= 2:
                        mark_stats[mk]["place2"] += 1
                    if pos <= 3:
                        mark_stats[mk]["placed"] += 1

                if mk in ("◉", "◎"):
                    honmei_hno = h["horse_no"]

            # 自信度別集計（単勝）
            if honmei_hno is not None:
                pos = finish_map.get(honmei_hno, 99)
                conf_stats[sim_conf]["races"] += 1
                conf_stats[sim_conf]["stake"] += 100
                if pos == 1:
                    conf_stats[sim_conf]["hits"] += 1
                    # 単勝払戻
                    tansho = payouts.get("tansho", payouts.get("単勝", []))
                    if isinstance(tansho, list):
                        for t in tansho:
                            if isinstance(t, dict) and t.get("horse_no") == honmei_hno:
                                conf_stats[sim_conf]["ret"] += t.get("payout", 0)
                                break
                            elif isinstance(t, (list, tuple)) and len(t) >= 2 and t[0] == honmei_hno:
                                conf_stats[sim_conf]["ret"] += t[1]
                                break

    return {
        "total_races": total_races,
        "by_mark": dict(mark_stats),
        "by_confidence": dict(conf_stats),
    }


def score_result(result: dict) -> float:
    """結果のスコアリング（高いほど良い）"""
    bm = result["by_mark"]
    bc = result["by_confidence"]

    score = 0.0
    # ◉ 勝率50%目標（重み3）
    tek = bm.get("◉", {})
    if tek.get("total", 0) >= 5:
        wr = tek["win"] / tek["total"]
        score += 3.0 * min(wr / 0.50, 1.0)  # 50%到達で3点
    # ◉が少なすぎるペナルティ
    tek_ratio = tek.get("total", 0) / max(result["total_races"], 1)
    if tek_ratio < 0.08:
        score -= 1.0  # 8%未満は少なすぎ

    # ◎ 勝率35%目標（重み2）
    hon = bm.get("◎", {})
    if hon.get("total", 0) >= 10:
        wr = hon["win"] / hon["total"]
        score += 2.0 * min(wr / 0.35, 1.0)

    # ○▲△序列ボーナス（重み2）
    o_wr = bm.get("○", {}).get("win", 0) / max(bm.get("○", {}).get("total", 1), 1)
    a_wr = bm.get("▲", {}).get("win", 0) / max(bm.get("▲", {}).get("total", 1), 1)
    d_wr = bm.get("△", {}).get("win", 0) / max(bm.get("△", {}).get("total", 1), 1)
    if o_wr >= a_wr >= d_wr:
        score += 2.0  # 完全序列
    elif o_wr >= a_wr or o_wr >= d_wr:
        score += 1.0  # 部分序列

    # SS的中率80%目標（重み3）
    ss = bc.get("SS", {})
    if ss.get("races", 0) >= 3:
        hr = ss["hits"] / ss["races"]
        score += 3.0 * min(hr / 0.80, 1.0)

    # SS>S>A序列ボーナス（重み1）
    ss_hr = ss.get("hits", 0) / max(ss.get("races", 1), 1) if ss.get("races", 0) > 0 else 0
    s_hr = bc.get("S", {}).get("hits", 0) / max(bc.get("S", {}).get("races", 1), 1) if bc.get("S", {}).get("races", 0) > 0 else 0
    a_hr = bc.get("A", {}).get("hits", 0) / max(bc.get("A", {}).get("races", 1), 1) if bc.get("A", {}).get("races", 0) > 0 else 0
    if ss_hr >= s_hr >= a_hr:
        score += 1.0

    # ☆ 勝率5%目標（重み1）
    ana = bm.get("☆", {})
    if ana.get("total", 0) >= 5:
        wr = ana["win"] / ana["total"]
        score += 1.0 * min(wr / 0.05, 1.0)

    return score


def format_result(result: dict, params: dict) -> str:
    """結果を表形式で表示"""
    lines = []
    lines.append(f"  Params: gap={params['tekipan_gap']:.1f} wp={params['tekipan_win_prob']:.2f} ss_gap={params['ss_gap_min']:.1f}")
    lines.append(f"  Races: {result['total_races']}")

    bm = result["by_mark"]
    for mk, label in [("◉", "TEK"), ("◎", "HON"), ("○", "TAI"), ("▲", "TAN"), ("△", "REN"), ("★", "RE2"), ("☆", "ANA")]:
        s = bm.get(mk, {})
        t = s.get("total", 0)
        if t > 0:
            wr = s["win"] / t * 100
            p2 = s["place2"] / t * 100
            p3 = s["placed"] / t * 100
            lines.append(f"    {label}: {t:4d}heads  W={wr:5.1f}%  P2={p2:5.1f}%  P3={p3:5.1f}%")

    bc = result["by_confidence"]
    for conf in ["SS", "S", "A", "B", "C", "D", "E"]:
        s = bc.get(conf, {})
        r = s.get("races", 0)
        if r > 0:
            hr = s["hits"] / r * 100
            roi = s["ret"] / s["stake"] * 100 if s.get("stake", 0) > 0 else 0
            lines.append(f"    {conf:2s}: {r:3d}R  hit={hr:5.1f}%  roi={roi:6.1f}%")

    sc = score_result(result)
    lines.append(f"  SCORE: {sc:.2f}")
    return "\n".join(lines)


if __name__ == "__main__":
    # パラメータグリッド
    param_grid = [
        # (tekipan_gap, tekipan_win_prob, ss_gap_min)
        # ベースライン（実パイプラインと同一パラメータ）
        {"tekipan_gap": 5.0, "tekipan_win_prob": 0.30, "ss_gap_min": 5.0},  # 現在の設定
        # ◉ gap バリエーション
        {"tekipan_gap": 3.5, "tekipan_win_prob": 0.30, "ss_gap_min": 5.0},
        {"tekipan_gap": 4.0, "tekipan_win_prob": 0.30, "ss_gap_min": 5.0},
        {"tekipan_gap": 4.5, "tekipan_win_prob": 0.30, "ss_gap_min": 5.0},
        {"tekipan_gap": 6.0, "tekipan_win_prob": 0.30, "ss_gap_min": 5.0},
        # win_prob 閾値バリエーション
        {"tekipan_gap": 5.0, "tekipan_win_prob": 0.20, "ss_gap_min": 5.0},
        {"tekipan_gap": 5.0, "tekipan_win_prob": 0.25, "ss_gap_min": 5.0},
        {"tekipan_gap": 5.0, "tekipan_win_prob": 0.35, "ss_gap_min": 5.0},
        # SS gap バリエーション
        {"tekipan_gap": 5.0, "tekipan_win_prob": 0.30, "ss_gap_min": 4.0},
        {"tekipan_gap": 5.0, "tekipan_win_prob": 0.30, "ss_gap_min": 6.0},
        # 組み合わせ
        {"tekipan_gap": 4.5, "tekipan_win_prob": 0.25, "ss_gap_min": 4.0},
        {"tekipan_gap": 5.0, "tekipan_win_prob": 0.25, "ss_gap_min": 4.0},
    ]

    print(f"=== Parameter Tuning ({len(param_grid)} combinations) ===\n")

    results = []
    for i, params in enumerate(param_grid):
        print(f"[{i+1}/{len(param_grid)}] Testing...", flush=True)
        result = evaluate_params(**params)
        sc = score_result(result)
        results.append((sc, params, result))
        print(format_result(result, params))
        print()

    # スコア順でソート
    results.sort(key=lambda x: x[0], reverse=True)
    print("\n=== TOP 3 ===")
    for rank, (sc, params, result) in enumerate(results[:3], 1):
        print(f"\n--- #{rank} (score={sc:.2f}) ---")
        print(format_result(result, params))
