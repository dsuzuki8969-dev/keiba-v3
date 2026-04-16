"""
予想オッズ算出エンジン

ML確率 → 予想オッズ変換、馬連・三連複の組み合わせ確率算出、
実オッズとの乖離検出、期待値ランキングを提供する。

前日時点で実オッズがなくても予想オッズだけで暫定判定が可能。
"""

from itertools import combinations
from typing import Any, Dict, List, Optional, Tuple

from config.settings import (
    DIVERGENCE_SIGNAL,
    EV_THRESHOLD_BUY,
    PAYOUT_RATES,
)
from src.log import get_logger
from src.models import HorseEvaluation, RaceInfo

logger = get_logger(__name__)


def get_payout_rate(is_jra: bool, ticket_type: str) -> float:
    """券種別払戻率を返す"""
    key = "JRA" if is_jra else "NAR"
    return PAYOUT_RATES.get(key, {}).get(ticket_type, 0.75)


# ============================================================
# 単勝予想オッズ
# ============================================================


def calc_predicted_tansho_odds(
    evaluations: List[HorseEvaluation],
    is_jra: bool = True,
) -> Dict[str, float]:
    """
    各馬のML勝率から単勝予想オッズを算出する。

    単純な 1/p × payout ではなく、
    市場の favorite-longshot bias を power law で補正してから変換する。
    - alpha < 1: 確率分布を均一化 → 人気馬オッズ高め・大穴オッズ低め（市場に近い）
    - JRA alpha=0.72, NAR alpha=0.68（NAR は地方で大穴への集中が強め）

    Returns: {horse_id: predicted_tansho_odds}
    """
    payout = get_payout_rate(is_jra, "単勝")
    # 市場歪み補正係数: JRA=0.72, NAR=0.68
    alpha = 0.72 if is_jra else 0.68
    n = len(evaluations)

    raw_probs: Dict[str, float] = {}
    for ev in evaluations:
        wp = ev.ml_win_prob
        if wp is None or wp <= 0:
            wp = ev.win_prob
        if wp is None or wp <= 0:
            wp = 1.0 / max(n, 1)
        raw_probs[ev.horse.horse_id] = max(wp, 1e-6)

    # 合計を1.0に正規化
    total_raw = sum(raw_probs.values())
    raw_probs = {k: v / total_raw for k, v in raw_probs.items()}

    # Power law 補正 (favorite-longshot bias を市場分布に近づける)
    # p_i^alpha / sum(p_j^alpha) → 本命を少し割高に、大穴を少し割安に
    adj = {k: v ** alpha for k, v in raw_probs.items()}
    total_adj = sum(adj.values())
    calibrated = {k: v / total_adj for k, v in adj.items()}

    result: Dict[str, float] = {}
    for hid, wp in calibrated.items():
        if wp > 0:
            result[hid] = round(1.0 / wp * payout, 1)
        else:
            result[hid] = 999.9

    return result


# ============================================================
# 馬連予想オッズ
# ============================================================


def _combination_prob_2(
    prob_a: float,
    prob_b: float,
    n: int,
) -> float:
    """
    馬連の出現確率: P(AとBがともに2着以内)
    独立近似 + 排他補正: P(A top2) × P(B top2) × n/(n-1)
    """
    if n <= 1:
        return 0.0
    correction = n / (n - 1)
    return min(prob_a * prob_b * correction, 0.99)


def calc_predicted_umaren(
    evaluations: List[HorseEvaluation],
    race_info: RaceInfo,
    top_n: int = 30,
) -> List[Dict[str, Any]]:
    """
    馬連の全組み合わせについて予想オッズを算出する。

    Returns: [{"a": horse_no_a, "b": horse_no_b, "prob": float,
               "predicted_odds": float, ...}, ...]
    """
    payout = get_payout_rate(race_info.is_jra, "馬連")
    n = race_info.field_count

    # 連対率を取得
    top2_map: Dict[int, float] = {}
    name_map: Dict[int, str] = {}
    for ev in evaluations:
        p = ev.ml_top2_prob if ev.ml_top2_prob else ev.place2_prob
        if p is None or p <= 0:
            p = 2.0 / max(n, 1)
        top2_map[ev.horse.horse_no] = p
        name_map[ev.horse.horse_no] = ev.horse.horse_name

    results = []
    for (no_a, p_a), (no_b, p_b) in combinations(top2_map.items(), 2):
        prob = _combination_prob_2(p_a, p_b, n)
        if prob <= 0:
            continue
        pred_odds = round(1.0 / prob * payout, 1) if prob > 0 else 999.9
        results.append({
            "type": "馬連",
            "a": min(no_a, no_b),
            "b": max(no_a, no_b),
            "name_a": name_map.get(min(no_a, no_b), ""),
            "name_b": name_map.get(max(no_a, no_b), ""),
            "prob": prob,
            "predicted_odds": pred_odds,
        })

    # 出現率を全組合せ合計で正規化（Σ=1.0）
    total_prob = sum(r["prob"] for r in results)
    if total_prob > 0:
        for r in results:
            r["prob"] = r["prob"] / total_prob

    results.sort(key=lambda x: -x["prob"])  # 出現率降順
    return results[:top_n]


# ============================================================
# 三連複予想オッズ
# ============================================================


def _combination_prob_3(
    prob_a: float,
    prob_b: float,
    prob_c: float,
    n: int,
) -> float:
    """
    三連複の出現確率: P(A,B,Cがともに3着以内)
    独立近似 + 排他補正
    """
    if n <= 2:
        return 0.0
    correction = n * (n - 1) / max(1.0, (n - 2) * (n - 3) * 0.5)
    return min(prob_a * prob_b * prob_c * correction, 0.99)


def calc_predicted_sanrenpuku(
    evaluations: List[HorseEvaluation],
    race_info: RaceInfo,
    top_n: int = 50,
) -> List[Dict[str, Any]]:
    """
    三連複の全組み合わせについて予想オッズを算出する。
    """
    payout = get_payout_rate(race_info.is_jra, "三連複")
    n = race_info.field_count

    top3_map: Dict[int, float] = {}
    name_map: Dict[int, str] = {}
    for ev in evaluations:
        p = ev.ml_place_prob if ev.ml_place_prob else ev.place3_prob
        if p is None or p <= 0:
            p = 3.0 / max(n, 1)
        top3_map[ev.horse.horse_no] = p
        name_map[ev.horse.horse_no] = ev.horse.horse_name

    results = []
    for (no_a, p_a), (no_b, p_b), (no_c, p_c) in combinations(top3_map.items(), 3):
        prob = _combination_prob_3(p_a, p_b, p_c, n)
        if prob <= 0:
            continue
        pred_odds = round(1.0 / prob * payout, 1) if prob > 0 else 999.9
        nos = sorted([no_a, no_b, no_c])
        results.append({
            "type": "三連複",
            "a": nos[0],
            "b": nos[1],
            "c": nos[2],
            "name_a": name_map.get(nos[0], ""),
            "name_b": name_map.get(nos[1], ""),
            "name_c": name_map.get(nos[2], ""),
            "prob": prob,
            "predicted_odds": pred_odds,
        })

    # 出現率を全組合せ合計で正規化（Σ=1.0）
    total_prob = sum(r["prob"] for r in results)
    if total_prob > 0:
        for r in results:
            r["prob"] = r["prob"] / total_prob

    results.sort(key=lambda x: -x["prob"])  # 出現率降順
    return results[:top_n]


# ============================================================
# 乖離検出 + 期待値ランキング
# ============================================================


def calc_divergence_signal(predicted_odds: float, actual_odds: float) -> Tuple[float, str]:
    """
    実オッズ/予想オッズの乖離率とシグナルを返す。

    乖離率 > 1 = 大衆が過小評価 → 妙味あり
    乖離率 < 1 = 大衆が過大評価 → 過剰人気

    Returns: (divergence_ratio, signal_label)
    """
    if predicted_odds <= 0 or actual_odds <= 0:
        return 0.0, "×"

    ratio = actual_odds / predicted_odds

    for label, threshold in sorted(DIVERGENCE_SIGNAL.items(),
                                   key=lambda x: -x[1]):
        if ratio >= threshold:
            return round(ratio, 2), label
    return round(ratio, 2), "×"


def calc_expected_value(prob: float, actual_odds: float) -> float:
    """期待値 = 予測確率 × 実オッズ"""
    return prob * actual_odds


def detect_value_bets(
    evaluations: List[HorseEvaluation],
    race_info: RaceInfo,
    umaren_predicted: Optional[List[Dict]] = None,
    sanrenpuku_predicted: Optional[List[Dict]] = None,
) -> List[Dict[str, Any]]:
    """
    予想オッズと実オッズの乖離から期待値プラスの馬券（バリューベット）を検出する。

    Returns: [{type, combo, predicted_odds, actual_odds, divergence, ev, signal}, ...]
    """
    value_bets = []

    # ── 単勝 ──
    for ev in evaluations:
        if ev.predicted_tansho_odds and ev.horse.odds:
            ratio, signal = calc_divergence_signal(ev.predicted_tansho_odds, ev.horse.odds)
            wp = ev.ml_win_prob if ev.ml_win_prob else ev.win_prob
            expected = calc_expected_value(wp or 0, ev.horse.odds)

            if expected >= EV_THRESHOLD_BUY:
                value_bets.append({
                    "type": "単勝",
                    "combo": str(ev.horse.horse_no),
                    "name": ev.horse.horse_name,
                    "horse_no": ev.horse.horse_no,
                    "predicted_odds": ev.predicted_tansho_odds,
                    "actual_odds": ev.horse.odds,
                    "divergence": ratio,
                    "ev": round(expected, 3),
                    "signal": signal,
                    "prob": round(wp or 0, 4),
                })

    # ── 馬連 ──
    if umaren_predicted:
        ev_map = {e.horse.horse_no: e for e in evaluations}
        for bet in umaren_predicted:
            ev_a = ev_map.get(bet["a"])
            ev_b = ev_map.get(bet["b"])
            if not ev_a or not ev_b:
                continue
            # 実オッズがあれば乖離検出
            oa = ev_a.horse.odds
            ob = ev_b.horse.odds
            if oa and ob:
                from src.calculator.betting import estimate_umaren_odds
                actual_odds = estimate_umaren_odds(oa, ob, race_info.field_count)
                pred = bet["predicted_odds"]
                ratio, signal = calc_divergence_signal(pred, actual_odds)
                expected = calc_expected_value(bet["prob"], actual_odds)

                if expected >= EV_THRESHOLD_BUY:
                    value_bets.append({
                        "type": "馬連",
                        "combo": f"{bet['a']}-{bet['b']}",
                        "name": f"{bet.get('name_a', '')}-{bet.get('name_b', '')}",
                        "predicted_odds": pred,
                        "actual_odds": round(actual_odds, 1),
                        "divergence": ratio,
                        "ev": round(expected, 3),
                        "signal": signal,
                        "prob": round(bet["prob"], 4),
                    })

    # ── 三連複 ──
    if sanrenpuku_predicted:
        ev_map = {e.horse.horse_no: e for e in evaluations}
        for bet in sanrenpuku_predicted:
            ev_a = ev_map.get(bet["a"])
            ev_b = ev_map.get(bet["b"])
            ev_c = ev_map.get(bet["c"])
            if not ev_a or not ev_b or not ev_c:
                continue
            oa = ev_a.horse.odds
            ob = ev_b.horse.odds
            oc = ev_c.horse.odds
            if oa and ob and oc:
                from src.calculator.betting import estimate_sanrenpuku_odds
                actual_odds = estimate_sanrenpuku_odds(oa, ob, oc, race_info.field_count)
                pred = bet["predicted_odds"]
                ratio, signal = calc_divergence_signal(pred, actual_odds)
                expected = calc_expected_value(bet["prob"], actual_odds)

                if expected >= EV_THRESHOLD_BUY:
                    value_bets.append({
                        "type": "三連複",
                        "combo": f"{bet['a']}-{bet['b']}-{bet['c']}",
                        "name": f"{bet.get('name_a', '')}-{bet.get('name_b', '')}-{bet.get('name_c', '')}",
                        "predicted_odds": pred,
                        "actual_odds": round(actual_odds, 1),
                        "divergence": ratio,
                        "ev": round(expected, 3),
                        "signal": signal,
                        "prob": round(bet["prob"], 4),
                    })

    # 期待値降順でソート
    value_bets.sort(key=lambda x: -x["ev"])
    return value_bets


def assign_divergence_to_evaluations(
    evaluations: List[HorseEvaluation],
    is_jra: bool = True,
) -> None:
    """各馬の HorseEvaluation に予想単勝オッズ・乖離率・シグナルを設定する"""
    predicted = calc_predicted_tansho_odds(evaluations, is_jra)

    for ev in evaluations:
        pred = predicted.get(ev.horse.horse_id)
        if pred:
            ev.predicted_tansho_odds = pred
            if ev.horse.odds:
                ratio, signal = calc_divergence_signal(pred, ev.horse.odds)
                ev.odds_divergence = ratio
                ev.divergence_signal = signal
            else:
                ev.divergence_signal = "前日"


def build_pre_day_assessment(
    evaluations: List[HorseEvaluation],
    race_info: RaceInfo,
) -> Dict[str, Any]:
    """
    前日モード: 実オッズなしの状態で予想オッズのみを使った暫定評価を返す。
    """
    assign_divergence_to_evaluations(evaluations, race_info.is_jra)
    umaren = calc_predicted_umaren(evaluations, race_info)
    sanrenpuku = calc_predicted_sanrenpuku(evaluations, race_info)

    return {
        "evaluations": evaluations,
        "predicted_tansho": {ev.horse.horse_no: ev.predicted_tansho_odds
                            for ev in evaluations if ev.predicted_tansho_odds},
        "predicted_umaren": umaren[:20],
        "predicted_sanrenpuku": sanrenpuku[:30],
        "is_pre_day": True,
    }
