"""
競馬解析マスターシステム v3.0 - 買い目・資金配分エンジン (第5章)
"""

import math
from typing import Dict, List, Optional, Tuple

from config.settings import (
    MAX_FORMATION_TICKETS,
    MIN_FORMATION_EV,
    PAYOUT_RATES,
    STAKE_DEFAULT,
)
from src.models import (
    AnaType,
    BakenType,
    ConfidenceLevel,
    HorseEvaluation,
    KikenType,
    Mark,
    RaceInfo,
    Reliability,
)


# ============================================================
# K-1: Fractional Kelly 賭け金配分
# ============================================================


def kelly_fraction(win_prob: float, odds: float, frac: float = 0.25) -> float:
    """
    Fractional Kelly 基準で最適賭け金割合を返す。
    f* = (p × (odds-1) - (1-p)) / (odds-1)
    返値: 0〜1 の範囲（資金に対する割合）
    """
    if odds <= 1.0 or win_prob <= 0:
        return 0.0
    b = odds - 1.0  # 利益倍率
    f_star = (win_prob * b - (1.0 - win_prob)) / b
    return max(0.0, f_star * frac)

# ============================================================
# 5-0: オッズ推定ユーティリティ
# ============================================================


def _head_factor(field_count: int) -> float:
    """
    頭数に応じた馬連・馬単の除数（単勝a×単勝bを何で割るか）
    経験則: 8頭=3.0, 12頭=3.5, 16頭=4.0
    """
    if field_count <= 8:
        return 3.0
    if field_count <= 10:
        return 3.2
    if field_count <= 12:
        return 3.5
    if field_count <= 14:
        return 3.8
    return 4.0


def estimate_umaren_odds(
    odds_a: float, odds_b: float, field_count: int, is_jra: bool = True
) -> float:
    """
    馬連オッズ推定 (K-2: JRA/NAR別払戻率適用)
    実績式: 単勝a × 単勝b / head_factor × (馬連払戻率 / 単勝払戻率)
    """
    payout_win    = PAYOUT_RATES["jra_win"    if is_jra else "nar_win"]
    payout_umaren = PAYOUT_RATES["jra_umaren" if is_jra else "nar_umaren"]
    correction = payout_umaren / payout_win
    return odds_a * odds_b / _head_factor(field_count) * correction


def estimate_umatan_odds(
    odds_a: float, odds_b: float, field_count: int, is_jra: bool = True
) -> float:
    """
    馬単オッズ推定: 馬連 × 1.7〜2.0 (平均1.85)
    """
    return estimate_umaren_odds(odds_a, odds_b, field_count, is_jra) * 1.85


def estimate_wide_odds(
    odds_a: float, odds_b: float, field_count: int, is_jra: bool = True
) -> float:
    """
    ワイドオッズ推定: 馬連 × 0.35
    """
    return estimate_umaren_odds(odds_a, odds_b, field_count, is_jra) * 0.35


def estimate_place_odds(tansho_odds: float, field_count: int) -> float:
    """
    複勝オッズ推定: 単勝から逆算
    理論式: 複勝 ≈ (単勝^0.6) × 補正
    """
    base = tansho_odds**0.55
    # 頭数補正: 頭数が多いほど複勝は低くなる
    correction = max(0.8, 1.0 - (field_count - 8) * 0.015)
    return max(1.1, round(base * correction, 1))


# ============================================================
# 5-1: 出目確率の算出
# ============================================================


def calc_hit_probability(
    prob_a: float,
    prob_b: float,
    ticket_type: str,  # "馬連" / "馬単" / "ワイド"
    field_count: int,
) -> float:
    """
    5-1: 出目確率の算出
    相関補正係数を実際の頭数・連対確率から算出
    """
    n = field_count

    if ticket_type == "馬連":
        # P(A連対) × P(B連対) × 補正
        # 補正: n/(n-2) で残り馬を考慮
        correction = n / (n - 1)
        return min(prob_a * prob_b * correction, 0.99)
    elif ticket_type == "馬単":
        correction = n / (n - 1)
        return min(prob_a * prob_b * correction, 0.99)
    elif ticket_type == "ワイド":
        correction = n / (n - 2) * 1.2
        return min(prob_a * prob_b * correction, 0.99)
    return 0.0


# ============================================================
# 5-1: 予測オッズ算出
# ============================================================


def calc_predicted_odds(
    evaluations: List[HorseEvaluation],
    is_jra: bool = True,
) -> Dict[str, float]:
    """
    予測オッズを全馬分算出して返す (実際のオッズ未設定馬用)
    horse_id → 単勝予測オッズ のDict
    """
    base_scores: Dict[str, float] = {}

    for ev in evaluations:
        # temperature=3 でより尖った分布に
        score = math.exp((ev.composite - 50) / 3.5)

        prev_runs = ev.horse.past_runs
        if prev_runs and prev_runs[0].finish_pos == 1:
            score *= 1.4
        if ev.jockey_stats and ev.jockey_stats.upper_long_dev >= 60:
            score *= 1.2
        if prev_runs and prev_runs[0].finish_pos >= 10:
            score *= 0.6
        from src.calculator.ability import detect_long_break

        if detect_long_break(ev.horse.past_runs, ev.horse.race_date or "")[0]:
            score *= 0.8

        base_scores[ev.horse.horse_id] = max(0.001, score)

    total = sum(base_scores.values())
    support: Dict[str, float] = {hid: s / total for hid, s in base_scores.items()}

    deduction = PAYOUT_RATES.get("jra_win" if is_jra else "nar_win", 0.80)
    result: Dict[str, float] = {}
    for hid, rate in support.items():
        result[hid] = round((1 / rate) * deduction, 1) if rate > 0 else 999.9

    return result


# ============================================================
# 5-2: 期待値算出
# ============================================================


def calc_expected_value(prob: float, odds: float) -> float:
    """期待値 = 出目確率 × オッズ × 100 (%)"""
    return prob * odds * 100


def classify_ev(ev: float) -> str:
    if ev >= 300:
        return "★勝負"
    if ev >= 200:
        return "◎買う"
    if ev >= 150:
        return "○買う"
    if ev >= 100:
        return "△検討"
    return "×買わない"


# ============================================================
# 5-2: 買い目生成
# ============================================================


def _allocate_fixed_tickets_by_ev(tickets: List[Dict], base_stake: int = 1000) -> None:
    """
    K-3: 期待値別に賭け金を配分（固定10点用）。
    ★勝負: 300円 / ◎買う: 200円 / ○買う: 100円 / △検討: 0円 / ×: 0円
    合計が base_stake に近似するよう設計されている。
    """
    EV_STAKES = {
        "★勝負":   300,
        "◎買う":   200,
        "○買う":   100,
        "△検討":     0,
        "×買わない":  0,
    }
    for t in tickets:
        signal = t.get("signal", "○買う")
        t["stake"] = EV_STAKES.get(signal, 100)


def generate_tickets(
    evaluations: List[HorseEvaluation],
    race_info: RaceInfo,
) -> List[Dict]:
    """
    5-2: 固定10点買い目を生成（◎/◉軸 馬連4点 + 三連複6点）
    各点に出現率・期待値を付与。stake はEV別配分（K-3）。
    """
    from itertools import combinations as _comb

    # 少頭数ガード: 軸+相手1頭以上必要
    if len(evaluations) < 2:
        return []

    # 本命（◉または◎）
    honmei = next((e for e in evaluations if e.mark in (Mark.TEKIPAN, Mark.HONMEI)), None)
    if not honmei:
        return []

    # 相手（○▲△★☆）印順
    partner_marks = {Mark.TAIKOU, Mark.TANNUKE, Mark.RENDASHI, Mark.RENDASHI2, Mark.ANA}
    partners = sorted(
        [e for e in evaluations if e.mark in partner_marks and e.horse.horse_id != honmei.horse.horse_id],
        key=lambda e: _MARK_ORDER.get(e.mark.value, 9),
    )[:4]

    # フォールバック: 相手印なし → 複合スコア上位4頭を相手に（全レース買い目生成）
    if not partners:
        partners = sorted(
            [e for e in evaluations if e.horse.horse_id != honmei.horse.horse_id],
            key=lambda e: e.composite,
            reverse=True,
        )[:4]

    if not partners:
        return []

    n = race_info.field_count or len(evaluations)
    # K-2: is_jra フラグを race_info から取得
    is_jra = getattr(race_info, "is_jra", True)
    tickets: List[Dict] = []
    eff_a = honmei.effective_odds or 10.0

    # 全組み合わせ確率合計で正規化（Σ=1.0 にする）
    u_norm, s_norm = _combo_norm_factors(evaluations, n)

    # ─── 馬連4点: ◎/◉-○, ◎/◉-▲, ◎/◉-△, ◎/◉-☆ ───
    for ev_b in partners:
        eff_b = ev_b.effective_odds or 10.0
        umaren = estimate_umaren_odds(eff_a, eff_b, n, is_jra)  # K-2
        raw_prob = calc_hit_probability(honmei.place2_prob, ev_b.place2_prob, "馬連", n)
        prob = raw_prob / u_norm  # 正規化: 全組み合わせΣ=1.0
        ev_val = calc_expected_value(prob, umaren)
        tickets.append({
            "type":   "馬連",
            "a":      honmei.horse.horse_no,
            "b":      ev_b.horse.horse_no,
            "mark_a": honmei.mark.value,
            "mark_b": ev_b.mark.value,
            "combo":  (min(honmei.horse.horse_no, ev_b.horse.horse_no),
                       max(honmei.horse.horse_no, ev_b.horse.horse_no)),
            "ev":         round(ev_val, 1),
            "appearance": round(prob * 100, 2),
            "signal":     classify_ev(ev_val),
            "odds":       umaren,
            "prob":       round(prob, 6),
            "stake":      100,
        })

    # ─── 三連複6点: ◎/◉ + 相手2頭 C(4,2)=6通り ───
    for ev_b, ev_c in _comb(partners, 2):
        eff_b = ev_b.effective_odds or 10.0
        eff_c = ev_c.effective_odds or 10.0
        ordered = sorted(
            [honmei.horse.horse_no, ev_b.horse.horse_no, ev_c.horse.horse_no],
            key=lambda no: _MARK_ORDER.get(
                next((e.mark.value for e in evaluations if e.horse.horse_no == no), ""), 9
            )
        )
        odds3  = estimate_sanrenpuku_odds(eff_a, eff_b, eff_c, n, is_jra)  # K-2
        raw_prob3 = calc_sanrenpuku_prob(honmei.win_prob, ev_b.win_prob, ev_c.win_prob, n)
        prob3 = raw_prob3 / s_norm  # 正規化: 全組み合わせΣ=1.0
        ev_val3 = calc_expected_value(prob3, odds3)
        tickets.append({
            "type":   "三連複",
            "a":      ordered[0],
            "b":      ordered[1],
            "c":      ordered[2],
            "mark_a": next((e.mark.value for e in evaluations if e.horse.horse_no == ordered[0]), "—"),
            "mark_b": next((e.mark.value for e in evaluations if e.horse.horse_no == ordered[1]), "—"),
            "mark_c": next((e.mark.value for e in evaluations if e.horse.horse_no == ordered[2]), "—"),
            "combo":  tuple(ordered),
            "ev":         round(ev_val3, 1),
            "appearance": round(prob3 * 100, 2),
            "signal":     classify_ev(ev_val3),
            "odds":       odds3,
            "prob":       round(prob3, 6),
            "stake":      100,
        })

    # K-3: EV別賭け金配分（固定10点用）
    _allocate_fixed_tickets_by_ev(tickets, base_stake=1000)

    return tickets


# ============================================================
# 5-3: 資金配分
# ============================================================


def allocate_stakes(
    tickets: List[Dict],
    confidence: ConfidenceLevel,
    custom_stake: Optional[int] = None,
) -> List[Dict]:
    """
    5-3: 印の重み + 期待値で資金配分

    配分ルール:
    1. 印の重み（◎-○: 50%, ◎-▲: 30%, ◎-△: 15%, ◎-☆: 5%）
    2. 期待値で補正（EV高いほど増量、上限1.3倍）
    3. 1票あたり50%超えは制限し、バランスを取る
    4. 回収率120%未満は「×買わない」
    5. 「×買わない」分を残りに再配分
    """
    total_stake = (
        custom_stake if custom_stake is not None else STAKE_DEFAULT.get(confidence.value, 0)
    )

    if total_stake == 0 or not tickets:
        return tickets

    # 印の重み（バランスを取るため○を下げる）
    mark_weights = {
        ("◉", "○"): 0.50,
        ("◉", "▲"): 0.30,
        ("◉", "△"): 0.15,
        ("◉", "☆"): 0.05,
        ("◎", "○"): 0.50,
        ("◎", "▲"): 0.30,
        ("◎", "△"): 0.15,
        ("◎", "☆"): 0.05,
    }

    # 各買い目に重みを割り当て（EV補正: 期待値高いほど増量、最大1.2倍）
    for t in tickets:
        mark_pair = (t.get("mark_a", "－"), t.get("mark_b", "－"))
        base = mark_weights.get(mark_pair, 0.05)
        ev_factor = min(1.2, max(0.8, t.get("ev", 100) / 100))
        t["weight"] = base * ev_factor

    # EV100%未満は賭け金0（ただし◎-○は常に買いのため除外）。買い対象のみで重み配分
    buy_signals = ("★勝負", "◎買う", "○買う", "△検討")
    buyable_tickets = [t for t in tickets if t.get("signal") in buy_signals]
    for t in tickets:
        if t.get("signal") not in buy_signals:
            t["stake"] = 0
            t["skip_reason"] = "low_ev"

    total_weight = sum(t["weight"] for t in buyable_tickets)
    if total_weight == 0:
        total_weight = len(buyable_tickets) or 1
        for t in buyable_tickets:
            t["weight"] = 1.0

    # EV>100%が1点だけの場合、その1点に全額を入れない（最大50%まで）
    ev_over_100 = [t for t in buyable_tickets if t.get("ev", 0) >= 100]
    single_ev_cap = 0.5 if len(ev_over_100) == 1 else 1.0

    # 回収率250～500%を目標。◎-○は200%以上で買い（常に買うため緩和）
    buyable = []
    skip_amount = 0
    for t in buyable_tickets:
        raw_stake = (t["weight"] / total_weight) * total_stake
        # 1点だけEV>100%のとき、その1点は最大50%
        if single_ev_cap < 1 and t.get("ev", 0) >= 100:
            raw_stake = min(raw_stake, total_stake * single_ev_cap)
        stake = int(raw_stake / 100) * 100
        stake = max(100, stake)

        is_honmei_taikou = (t.get("mark_a") in ("◎", "◉")) and (t.get("mark_b") == "○")
        min_recovery = 200 if is_honmei_taikou else 250
        recovery_rate = (t["odds"] * stake) / total_stake * 100
        # 回収率下限未満は見送り（◎-○は200%で許容）
        if recovery_rate < min_recovery:
            t["skip_reason"] = "low_recovery"
            t["stake"] = 0
            skip_amount += stake
        else:
            # 500%超はキャップ（1点集中を防ぐ）
            max_recovery = 500
            if recovery_rate > max_recovery:
                # stake = total * 5 / odds で回収率500%。100円単位に丸める
                stake = int(total_stake * max_recovery / t["odds"] / 10000) * 100
                stake = max(100, stake)
                skip_amount += raw_stake - stake
                t["_capped"] = True  # 再配分時に加算しない
            else:
                t["_capped"] = False
            t["stake"] = stake
            buyable.append(t)

    # 「×買わない」分を再配分（キャップ済み票には加算しない→500%超を防ぐ）
    if skip_amount > 0 and buyable:
        uncapped = [t for t in buyable if not t.get("_capped")]
        buyable_weight = sum(t["weight"] for t in uncapped) if uncapped else 1
        remainder = skip_amount
        for i, t in enumerate(uncapped):
            if i == len(uncapped) - 1:
                t["stake"] += remainder
            else:
                add_stake = int((t["weight"] / buyable_weight) * skip_amount / 100) * 100
                t["stake"] += add_stake
                remainder -= add_stake

    # 再配分後、予算ベースで500%超があれば最終キャップ（高オッズ票は最低100円のため500%未満にできない場合あり）
    max_recovery = 500
    for t in buyable:
        if t.get("stake", 0) > 0:
            rr = (t["odds"] * t["stake"]) / total_stake * 100
            if rr > max_recovery:
                t["stake"] = max(100, int(total_stake * max_recovery / t["odds"] / 10000) * 100)

    # 内部フラグ削除
    for t in tickets:
        t.pop("_capped", None)

    return tickets


# ============================================================
# 5-3: 自信度判定
# ============================================================


def _calc_confidence_score(evaluations: List[HorseEvaluation], is_jra: bool = True, is_banei: bool = False) -> float:
    """
    自信度スコアを算出（0.0 ~ 1.0）— 多信号一致方式。

    旧方式: 頭数45% + gap35% + gap23_20% → 頭数依存でSS<S<A序列破綻
    新方式: 複数の独立した予測信号が同じ馬を指しているかで自信度を判定。
    頭数因子を除去し、「予測の確からしさ」を直接測定する。

    v5: 6つの信号（市場フリー — value_score除去）:
    1. composite_gap (20%): 1位-2位のcomposite差 — 本命の優位度
    2. ml_agreement (25%): ML予測1位とcomposite1位の一致 — モデル間合意
    3. gap23 (10%): 2位-3位のcomposite差 — 上位3頭の分離度
    4. multi_factor (20%): ability/pace/courseの1位が同一馬 — 因子間合意
    5. reliability (10%): 上位馬のデータ信頼度 — 予測の確実性
    6. ml_confidence (15%): ML生値の1位-2位gap — ブレンド前の確信度
    """
    sorted_ev = sorted(evaluations, key=lambda e: e.composite, reverse=True)
    if len(sorted_ev) < 3:
        return 1.0 if len(sorted_ev) >= 1 else 0.0

    top = sorted_ev[0]
    top_id = top.horse.horse_id

    # 1. composite差 (20%) — 1位-2位の総合力差。◉判定でも最重要指標
    from config.settings import CONFIDENCE_GAP_DIVISOR_JRA, CONFIDENCE_GAP_DIVISOR_NAR
    gap = sorted_ev[0].composite - sorted_ev[1].composite
    gap_divisor = CONFIDENCE_GAP_DIVISOR_JRA if is_jra else CONFIDENCE_GAP_DIVISOR_NAR
    gap_norm = min(gap / gap_divisor, 1.0)

    # 2. ML一致度 (25%) — win_prob 1位がcomposite 1位と同じか（最重要: 2モデル合意）
    sorted_wp = sorted(evaluations, key=lambda e: e.win_prob, reverse=True)
    wp_top_id = sorted_wp[0].horse.horse_id
    if wp_top_id == top_id:
        ml_agreement = 1.0
    elif len(sorted_wp) >= 2 and sorted_wp[1].horse.horse_id == top_id:
        ml_agreement = 0.5  # ML2位=composite1位 → 部分一致
    else:
        ml_agreement = 0.0

    # 3. 2-3位差 (10%) — 4pt以上で満点
    gap23 = sorted_ev[1].composite - sorted_ev[2].composite
    gap23_norm = min(gap23 / 4.0, 1.0)

    # v5: value_score（市場信号）を除去。市場評価は自信度に不要

    # 4. 因子間合意 (20%) — ability/pace/courseの各1位が同一馬か
    top_ability_id = max(evaluations, key=lambda e: e.ability.total).horse.horse_id
    top_course_id = max(evaluations, key=lambda e: e.course.total).horse.horse_id
    if is_banei:
        # ばんえい: pace_total=50固定で全馬同値 → pace因子を除外し2因子で判定
        factor_match = sum(1 for fid in [top_ability_id, top_course_id] if fid == top_id)
        multi_factor = 1.0 if factor_match == 2 else (0.6 if factor_match == 1 else 0.0)
    else:
        top_pace_id = max(evaluations, key=lambda e: e.pace.total).horse.horse_id
        factor_match = sum(1 for fid in [top_ability_id, top_pace_id, top_course_id] if fid == top_id)
        if factor_match == 3:
            multi_factor = 1.0
        elif factor_match == 2:
            multi_factor = 0.6
        else:
            multi_factor = 0.0

    # 5. 信頼度 (10%) — 上位3馬の信頼度A率
    from src.models import Reliability
    top3_reliable = sum(
        1 for ev in sorted_ev[:3]
        if ev.ability.reliability == Reliability.A
    )
    reliability_norm = top3_reliable / 3.0

    # 6. ML確信度 (15%) — ML生値の1位-2位gap
    from config.settings import PIPELINE_V2_ENABLED
    if PIPELINE_V2_ENABLED:
        raw_ml_top = getattr(sorted_ev[0], '_raw_lgbm_prob', None)
        raw_ml_2nd = getattr(sorted_ev[1], '_raw_lgbm_prob', None) if len(sorted_ev) >= 2 else None
        if raw_ml_top is not None and raw_ml_2nd is not None:
            ml_raw_gap = raw_ml_top - raw_ml_2nd
            ml_confidence = min(ml_raw_gap / 0.10, 1.0) if ml_raw_gap > 0 else 0.0
        else:
            ml_confidence = 0.5  # ML生値不明時は中立

        # v5: 6信号加重合成（市場フリー）
        # value_score除去 → composite_gap/ml_agreement/multi_factorに再配分
        score = (
            gap_norm * 0.20
            + ml_agreement * 0.25
            + gap23_norm * 0.10
            + multi_factor * 0.20
            + reliability_norm * 0.10
            + ml_confidence * 0.15
        )
    else:
        # 旧パイプライン: 5信号（value_score除去版）
        score = (
            gap_norm * 0.25
            + ml_agreement * 0.25
            + gap23_norm * 0.10
            + multi_factor * 0.25
            + reliability_norm * 0.15
        )
    return score


def judge_confidence(
    evaluations: List[HorseEvaluation],
    pace_reliability: ConfidenceLevel,
    predicted_umaren: Optional[List] = None,
    predicted_sanrenpuku: Optional[List] = None,
    is_jra: bool = True,
    is_banei: bool = False,
) -> ConfidenceLevel:
    """
    5-3 自信度判定（多信号一致方式・JRA/NAR分離）。

    v5方式: 6信号スコア（市場フリー）+ パーセンタイル閾値 + win_prob/gapゲート。
    JRA/NARで閾値を分離。市場評価（人気・オッズ）は一切使用しない。

    SS～D: JRA/NAR別パーセンタイル閾値 + SS/Sはwin_prob・gapゲートで降格判定
    """
    if not evaluations:
        return ConfidenceLevel.D

    from config.settings import (
        CONFIDENCE_THRESHOLDS_JRA, CONFIDENCE_THRESHOLDS_NAR,
        CONFIDENCE_WP_GATE_SS_JRA, CONFIDENCE_WP_GATE_SS_NAR,
        CONFIDENCE_GAP_GATE_SS_JRA, CONFIDENCE_GAP_GATE_SS_NAR,
        CONFIDENCE_WP_GATE_S_JRA, CONFIDENCE_WP_GATE_S_NAR,
        CONFIDENCE_GAP_GATE_S_JRA, CONFIDENCE_GAP_GATE_S_NAR,
    )

    # ---- v5: パーセンタイル閾値 + win_prob/gapゲート（市場フリー） ----
    score = _calc_confidence_score(evaluations, is_jra=is_jra, is_banei=is_banei)
    thresholds = CONFIDENCE_THRESHOLDS_JRA if is_jra else CONFIDENCE_THRESHOLDS_NAR

    # ◎（composite1位）のwin_probとgapを取得
    sorted_comp = sorted(evaluations, key=lambda e: e.composite, reverse=True)
    top_wp = sorted_comp[0].win_prob if sorted_comp else 0
    top_gap = (sorted_comp[0].composite - sorted_comp[1].composite) if len(sorted_comp) >= 2 else 0

    # score閾値で初期レベル判定
    if score >= thresholds["SS"]:
        level = ConfidenceLevel.SS
    elif score >= thresholds["S"]:
        level = ConfidenceLevel.S
    elif score >= thresholds["A"]:
        level = ConfidenceLevel.A
    elif score >= thresholds["B"]:
        level = ConfidenceLevel.B
    elif score >= thresholds["C"]:
        level = ConfidenceLevel.C
    else:
        level = ConfidenceLevel.D

    # v5 win_prob/gapゲート: 自モデルの確信度で判定（市場評価は不要）
    wp_gate_ss = CONFIDENCE_WP_GATE_SS_JRA if is_jra else CONFIDENCE_WP_GATE_SS_NAR
    gap_gate_ss = CONFIDENCE_GAP_GATE_SS_JRA if is_jra else CONFIDENCE_GAP_GATE_SS_NAR
    wp_gate_s = CONFIDENCE_WP_GATE_S_JRA if is_jra else CONFIDENCE_WP_GATE_S_NAR
    gap_gate_s = CONFIDENCE_GAP_GATE_S_JRA if is_jra else CONFIDENCE_GAP_GATE_S_NAR

    if level == ConfidenceLevel.SS and (top_wp < wp_gate_ss or top_gap < gap_gate_ss):
        level = ConfidenceLevel.S
    if level == ConfidenceLevel.S and (top_wp < wp_gate_s or top_gap < gap_gate_s):
        level = ConfidenceLevel.A

    return level


# ============================================================
# 5-4: レース選別（買い/見送り）
# ============================================================


def should_buy_race(
    tickets: List[Dict],
    confidence: ConfidenceLevel,
    evaluations: List[HorseEvaluation],
    is_banei: bool = False,
) -> Tuple[bool, str]:
    """5-4: 見送り条件チェック"""

    if not tickets:
        return False, "期待値100%超えの買い目なし"

    # ばんえい: 予測精度に応じて自信度フィルタ
    if is_banei:
        from config.settings import BANEI_MIN_CONFIDENCE
        _banei_allowed = {
            "SS": (ConfidenceLevel.SS,),
            "S": (ConfidenceLevel.SS, ConfidenceLevel.S),
            "A": (ConfidenceLevel.SS, ConfidenceLevel.S, ConfidenceLevel.A),
            "B": (ConfidenceLevel.SS, ConfidenceLevel.S, ConfidenceLevel.A, ConfidenceLevel.B),
        }
        allowed = _banei_allowed.get(BANEI_MIN_CONFIDENCE, (ConfidenceLevel.SS, ConfidenceLevel.S))
        if confidence not in allowed:
            return False, f"ばんえい: 自信度{confidence.value}（{BANEI_MIN_CONFIDENCE}以上のみ）"

    if confidence == ConfidenceLevel.D:
        return False, f"自信度{confidence.value}"

    sorted_ev = sorted(evaluations, key=lambda e: e.composite, reverse=True)

    # ◎本命が決まらない（1位と2位の差が0.5pt未満）→ 見送り
    # ※実データでは偏差値差が10pt以上つく。デモ/サンプル不足時は緩め。
    if len(sorted_ev) >= 2:
        top2_gap = sorted_ev[0].composite - sorted_ev[1].composite
        if top2_gap < 0.5:
            return False, f"1位2位が接近 ({top2_gap:.1f}pt差)"

    # 信頼度C馬が半数以上
    c_count = sum(1 for e in evaluations if e.ability.reliability == Reliability.C)
    # 信頼度C見送りは実データ運用時のみ有効（基準タイムDB 30走以上でA/B判定になる）
    # if c_count >= len(evaluations):
    #     return False, f"全馬信頼度C（データ不足）"

    return True, ""


# ============================================================
# 5-2: 買い目パターン判定
# ============================================================


def classify_buy_pattern(evaluations: List[HorseEvaluation]) -> str:
    sorted_ev = sorted(evaluations, key=lambda e: e.composite, reverse=True)
    if len(sorted_ev) < 2:
        return "A"

    gaps = [
        sorted_ev[i].composite - sorted_ev[i + 1].composite
        for i in range(min(4, len(sorted_ev) - 1))
    ]
    avg_gap = sum(gaps) / len(gaps) if gaps else 0
    has_断層 = any(g > avg_gap * 1.5 and g > 3 for g in gaps[:2])
    top_gap = sorted_ev[0].composite - sorted_ev[1].composite
    has_ana = any(e.ana_type in (AnaType.ANA_A,) for e in evaluations)

    if has_断層 and not has_ana:
        return "A"
    if not has_断層 and has_ana:
        return "B"
    if top_gap <= 2 and not has_ana:
        return "D"
    return "C"


# ============================================================
# フォーメーション買い目生成
# ============================================================

# Stern補正係数: 人気馬の三連複確率を抑制し穴馬の確率を引き上げる。
# γ < 1.0 で人気-穴バイアスを補正。文献値 0.81-0.90、三連複市場は 0.87 が実績良好。
_TRIO_GAMMA = 0.87


def _harville_trio_prob(pa: float, pb: float, pc: float) -> float:
    """Harvilleモデルによる三連複確率（3頭がトップ3に入る確率、順不同）。

    P(A,B,C top3) = Σ(6 permutations) P(X 1st) × P(Y 2nd|X) × P(Z 3rd|X,Y)
    """
    prob = 0.0
    for x, y, z in [(pa, pb, pc), (pa, pc, pb), (pb, pa, pc),
                     (pb, pc, pa), (pc, pa, pb), (pc, pb, pa)]:
        d1 = max(1.0 - x, 1e-6)
        d2 = max(d1 - y, 1e-6)
        prob += x * (y / d1) * (z / d2)
    return prob


def estimate_sanrenpuku_odds(
    odds_a: float, odds_b: float, odds_c: float, field_count: int, is_jra: bool = True,
    *, _all_odds: Optional[List[float]] = None, _recip_sum: float = 0.0,
) -> float:
    """三連複オッズ推定（Harvilleモデル + Stern補正）。

    _all_odds が指定された場合、全馬の単勝オッズから Harville モデルで
    高精度に三連複確率を計算し、そこからオッズを導出する。
    Stern補正（γ=0.87）で人気-穴バイアスを調整。

    _all_odds: 全出走馬の単勝オッズリスト（推奨。netkeiba予想オッズ等）
    _recip_sum: 後方互換用（Dutch Book正規化、_all_odds優先）
    """
    payout = PAYOUT_RATES["jra_sanrenpuku" if is_jra else "nar_sanrenpuku"]
    oa = max(odds_a, 1.1)
    ob = max(odds_b, 1.1)
    oc = max(odds_c, 1.1)

    if _all_odds and len(_all_odds) >= 3:
        # ── Harvilleモデル + Stern補正 ──
        gamma = _TRIO_GAMMA
        # Step 1: 全馬の逆オッズにγ乗 → 再正規化して勝利確率を得る
        adj = [(1.0 / max(o, 1.1)) ** gamma for o in _all_odds]
        total_adj = sum(adj) or 1.0
        # Step 2: 対象3頭の補正後確率
        pa = ((1.0 / oa) ** gamma) / total_adj
        pb = ((1.0 / ob) ** gamma) / total_adj
        pc = ((1.0 / oc) ** gamma) / total_adj
        # Step 3: 6順列のHarville確率を合算
        trio_prob = _harville_trio_prob(pa, pb, pc)
        return round(max(2.0, payout / max(trio_prob, 1e-12)), 1)

    if _recip_sum > 0:
        # ── Dutch Book正規化（後方互換フォールバック）──
        prod = oa * ob * oc
        market_prob = (1.0 / prod) / _recip_sum
        return round(max(2.0, payout / market_prob), 1)

    # ── 最終フォールバック: 頭数ベースHarville概算 ──
    # _all_odds がない場合、残り馬を平均オッズで補完してHarville
    inv_a, inv_b, inv_c = 1.0 / oa, 1.0 / ob, 1.0 / oc
    known_sum = inv_a + inv_b + inv_c
    # 単勝市場の総overround ≈ 1/payout_rate（JRA: ~1.25, NAR: ~1.33）
    payout_win = PAYOUT_RATES["jra_win" if is_jra else "nar_win"]
    total_overround = 1.0 / payout_win
    remaining = max(total_overround - known_sum, 0.01)
    n_rest = max(field_count - 3, 1)
    inv_rest = remaining / n_rest  # 残り馬1頭あたりの平均逆オッズ
    gamma = _TRIO_GAMMA
    adj_all = [inv_a ** gamma, inv_b ** gamma, inv_c ** gamma]
    adj_all += [inv_rest ** gamma] * n_rest
    total_adj = sum(adj_all) or 1.0
    pa = (inv_a ** gamma) / total_adj
    pb = (inv_b ** gamma) / total_adj
    pc = (inv_c ** gamma) / total_adj
    trio_prob = _harville_trio_prob(pa, pb, pc)
    return round(max(2.0, payout / max(trio_prob, 1e-12)), 1)


def calc_sanrenpuku_prob(wp_a: float, wp_b: float, wp_c: float, field_count: int) -> float:
    """三連複出現確率の計算（Harvilleモデル・win_prob版）

    wp_a,b,c は win_prob（勝率、全馬合計≈1.0）を渡すこと。
    place3_prob（複勝率、全馬合計≈3.0）を渡すと分布が平坦化し、
    全組み合わせが同率（≈1/C(n,3)）になるバグの原因となる。

    P(trio) = Σ(6perm) P(X 1st) × P(Y 2nd|X) × P(Z 3rd|X,Y)
    """
    pa = min(max(wp_a, 0.001), 0.95)
    pb = min(max(wp_b, 0.001), 0.95)
    pc = min(max(wp_c, 0.001), 0.95)
    return min(_harville_trio_prob(pa, pb, pc), 0.99)


_MARK_ORDER = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "★": 5, "☆": 6, "×": 7}

def _dedup_sort(ev_list: List[HorseEvaluation]) -> List[HorseEvaluation]:
    seen: set = set()
    result = []
    for e in ev_list:
        if e.horse.horse_no not in seen:
            seen.add(e.horse.horse_no)
            result.append(e)
    return sorted(result, key=lambda e: (_MARK_ORDER.get(e.mark.value, 9), e.horse.horse_no))


def _detect_clusters(comps: List[float], threshold: float) -> List[List[float]]:
    """composite値リストを断層(threshold超)で分割してクラスタリング"""
    if not comps:
        return []
    clusters: List[List[float]] = [[comps[0]]]
    for i in range(1, len(comps)):
        if comps[i - 1] - comps[i] > threshold:
            clusters.append([])
        clusters[-1].append(comps[i])
    return clusters


def build_formation_columns(
    evaluations: List[HorseEvaluation],
    confidence: str,
) -> Tuple[List[HorseEvaluation], List[HorseEvaluation], List[HorseEvaluation]]:
    """P5統合法: 断層+印+自信度でフォーメーション1・2・3列目を決定する。

    confidence（自信度 SS〜E 7段階）に応じて指数差閾値と各列の最大頭数を制御。
    col1: クラスター断層の最上位グループ + ◉/◎
    col2: col1 + 断層2段目 + ○/▲（指数差g2以内）
    col3: col2 + 複勝率10%+かつEV 0.8+（指数差g3以内）
    """
    import statistics as _stats

    # 自信度別パラメータ: (g2, g3, cap1, cap2, cap3)
    #   g2: col2の指数差閾値  g3: col3の指数差閾値
    #   cap1/cap2/cap3: 各列の最大頭数
    CONF_PARAMS = {
        "SS": (10, 18, 2, 5, 8),
        "S":  (9, 16, 2, 5, 8),
        "A":  (8, 14, 2, 5, 9),
        "B":  (10, 16, 2, 6, 10),
        "C":  (12, 18, 2, 6, 10),
        "D":  (6, 8, 2, 4, 6),
        "E":  (6, 8, 2, 4, 6),
    }
    g2, g3, cap1, cap2, cap3 = CONF_PARAMS.get(confidence, (8, 14, 2, 5, 9))

    safe_evs = sorted(
        [ev for ev in evaluations if not getattr(ev, "is_tokusen_kiken", False)],
        key=lambda e: -e.composite,
    )
    if len(safe_evs) < 3:
        return [], [], []

    comps = [e.composite for e in safe_evs]
    sigma = _stats.stdev(comps) if len(comps) > 1 else 5.0
    threshold = max(2.0, sigma * 0.5)
    cls = _detect_clusters(comps, threshold)
    tc = comps[0]

    # ── col1: クラスター最上位 + ◉/◎ ──
    n1 = len(cls[0]) if cls else 2
    c1_idx = set(range(min(n1, cap1)))
    for i, e in enumerate(safe_evs):
        if e.mark in (Mark.TEKIPAN, Mark.HONMEI):
            c1_idx.add(i)
    col1 = [safe_evs[i] for i in sorted(c1_idx) if i < len(safe_evs)][:cap1]

    # ── col2: col1 + 断層2段目 + ○/▲（指数差g2以内）──
    c2_ids = {id(e) for e in col1}
    n2 = n1 + (len(cls[1]) if len(cls) > 1 else 0)
    for i, e in enumerate(safe_evs):
        if i < n2 or e.mark in (Mark.TAIKOU, Mark.TANNUKE):
            if (tc - e.composite) <= g2:
                c2_ids.add(id(e))
    col2 = [e for e in safe_evs if id(e) in c2_ids][:cap2]

    # ── col3: col2 + 複勝率+EV条件（指数差g3以内）──
    c3_ids = {id(e) for e in col2}
    high_conf = confidence in ("SS", "S")
    for e in safe_evs:
        if id(e) in c3_ids:
            continue
        if (tc - e.composite) > g3:
            continue
        if e.place3_prob < 0.08:
            continue
        # SS/S はEVフィルター緩和（place3_probのみ）
        if not high_conf:
            odds = e.effective_odds or 10.0
            wp = getattr(e, "win_prob", 0) or 0
            if wp * odds < 0.8:
                continue
        c3_ids.add(id(e))
    col3 = [e for e in safe_evs if id(e) in c3_ids][:cap3]

    # 最低頭数フォールバック（三連複に3頭必須）
    if len(col1) < 1:
        col1 = safe_evs[:1]
    if len(col2) < 2:
        col2 = safe_evs[:2]
    if len(col3) < 3:
        col3 = safe_evs[:3]

    return _dedup_sort(col1), _dedup_sort(col2), _dedup_sort(col3)


def _allocate_formation(tickets: List[Dict], budget: int) -> None:
    """優先順位（印）順に資金を配分。
    K-1: prob/odds がある場合は Fractional Kelly（25%）で賭け金を決定。
    ない場合は従来の「回収率300%固定」式にフォールバック。
    残高がなくなったらそこで終了（以降のチケットは stake=0）。
    """
    if not tickets or budget <= 0:
        for t in tickets:
            t["stake"] = 0
        return

    # 印の優先順位（低いほど優先）
    MARK_PRIO = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "☆": 5, "穴": 6}

    def _sort_key(t):
        return (
            MARK_PRIO.get(t.get("mark_a", ""), 9),
            MARK_PRIO.get(t.get("mark_b", ""), 9),
            MARK_PRIO.get(t.get("mark_c", ""), 9),
        )

    # 優先順位順に処理（dict オブジェクトは同一参照）
    sorted_tickets = sorted(tickets, key=_sort_key)
    remaining = budget
    allocated_ids: set = set()

    for t in sorted_tickets:
        if remaining <= 0:
            break
        odds = max(t.get("odds", 1.0), 1.0)

        # K-1: Kelly配分（prob と odds がある場合）
        if t.get("prob") and t.get("odds"):
            k = kelly_fraction(t["prob"], t["odds"])
            if k > 0:
                # Kelly比率 × 総予算で賭け金を決定（複数票があるため4倍スケール）
                raw = k * budget * 4
            else:
                # Kelly=0 = 負の期待値 → 配分しない
                continue
        else:
            # 従来式: このチケットが当たれば budget × 3 が返ってくる掛け金
            raw = 3.0 * budget / odds

        stake = max(100, int(raw / 100) * 100)  # 100円単位・最低100円

        if stake >= remaining:
            # 残高を全額投入して終了（100円未満なら直前チケットに乗せて終了）
            last_stake = (remaining // 100) * 100
            if last_stake < 100:
                break
            t["stake"] = last_stake
            remaining = 0
            allocated_ids.add(id(t))
            break

        t["stake"] = stake
        remaining -= stake
        allocated_ids.add(id(t))

    # 未配分（残高切れ）はすべて 0
    for t in tickets:
        if id(t) not in allocated_ids:
            t["stake"] = 0


def _combo_norm_factors(
    evaluations: List[HorseEvaluation],
    field_count: int,
) -> tuple:
    """
    全頭を対象に 馬連・三連複の組み合わせ確率合計を計算し正規化係数を返す。
      Σ(馬連全組み合わせ) → u_norm  (この値で各組み合わせ確率を割ると Σ=1.0)
      Σ(三連複全組み合わせ) → s_norm
    """
    from itertools import combinations as _comb
    u_norm = 0.0
    for ev_a, ev_b in _comb(evaluations, 2):
        u_norm += calc_hit_probability(
            ev_a.place2_prob, ev_b.place2_prob, "馬連", field_count
        )
    s_norm = 0.0
    for ev_a, ev_b, ev_c in _comb(evaluations, 3):
        s_norm += calc_sanrenpuku_prob(
            ev_a.win_prob, ev_b.win_prob, ev_c.win_prob, field_count
        )
    return max(u_norm, 1e-9), max(s_norm, 1e-9)


def generate_formation_tickets(
    evaluations: List[HorseEvaluation],
    race_info: RaceInfo,
    confidence: str,
) -> Dict:
    """三連複フォーメーション買い目を生成する（P5統合法）。

    EV上位 MAX_FORMATION_TICKETS 点に絞り、資金配分を行う。

    戻り値: {
        "col1": [...], "col2": [...], "col3": [...],
        "sanrenpuku": [ticket_dict, ...],
        "s_norm": float,
        "confidence": str,
    }
    """
    col1_raw, col2_raw, col3_raw = build_formation_columns(evaluations, confidence)
    empty = {"col1": [], "col2": [], "col3": [], "sanrenpuku": [],
             "s_norm": 1.0, "confidence": confidence}
    if not col1_raw or not col2_raw or not col3_raw:
        return empty

    # 列を排他化（col2からcol1を除外、col3からcol1+col2を除外）
    c1_nos = {e.horse.horse_no for e in col1_raw}
    col2_excl = [e for e in col2_raw if e.horse.horse_no not in c1_nos]
    c2_nos = c1_nos | {e.horse.horse_no for e in col2_excl}
    col3_excl = [e for e in col3_raw if e.horse.horse_no not in c2_nos]

    col1, col2, col3 = col1_raw, col2_excl, col3_excl
    if not col2 or not col3:
        return empty

    # 全頭ベースの正規化係数（三連複Σ=1.0 にするための除数）
    _, s_norm = _combo_norm_factors(evaluations, race_info.field_count)

    n = race_info.field_count
    is_jra = getattr(race_info, "is_jra", True)
    mark_map = {ev.horse.horse_no: ev.mark.value for ev in evaluations}
    MARK_PRIO = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "☆": 5}

    def _sort_by_mark(horse_nos: List[int]) -> List[int]:
        return sorted(horse_nos, key=lambda no: (MARK_PRIO.get(mark_map.get(no, "—"), 9), no))

    # ─── 全馬の単勝オッズリスト（Harvilleモデル用）───
    ev_map = {e.horse.horse_no: e for e in evaluations}
    all_odds = [max(e.effective_odds or 10.0, 1.1) for e in evaluations]

    # ─── 三連複: col1 × col2 × col3 の全組み合わせ生成（排他列）───
    sanrenpuku_tickets: List[Dict] = []
    seen_3: set = set()
    for ev_a in col1:
        for ev_b in col2:
            for ev_c in col3:
                horse_nos = {ev_a.horse.horse_no, ev_b.horse.horse_no, ev_c.horse.horse_no}
                if len(horse_nos) < 3:
                    continue
                seen_key = tuple(sorted(horse_nos))
                if seen_key in seen_3:
                    continue
                seen_3.add(seen_key)
                ordered = _sort_by_mark(list(horse_nos))
                oa = ev_map[ordered[0]].effective_odds or 10.0
                ob = ev_map[ordered[1]].effective_odds or 10.0
                oc = ev_map[ordered[2]].effective_odds or 10.0
                odds = estimate_sanrenpuku_odds(oa, ob, oc, n, is_jra, _all_odds=all_odds)
                raw_prob = calc_sanrenpuku_prob(
                    ev_map[ordered[0]].win_prob,
                    ev_map[ordered[1]].win_prob,
                    ev_map[ordered[2]].win_prob,
                    n,
                )
                prob = raw_prob / s_norm
                ev_val = calc_expected_value(prob, odds)
                sanrenpuku_tickets.append(
                    {
                        "type": "三連複",
                        "a": ordered[0],
                        "b": ordered[1],
                        "c": ordered[2],
                        "mark_a": mark_map.get(ordered[0], "—"),
                        "mark_b": mark_map.get(ordered[1], "—"),
                        "mark_c": mark_map.get(ordered[2], "—"),
                        "odds": odds,
                        "prob": prob,
                        "ev": ev_val,
                        "appearance": prob * 100,
                        "stake": 0,
                    }
                )

    # ─── EV閾値フィルタ → EV降順ソート → 上位 MAX_FORMATION_TICKETS 点に制限 ───
    sanrenpuku_tickets = [t for t in sanrenpuku_tickets if t.get("ev", 0) >= MIN_FORMATION_EV]
    sanrenpuku_tickets.sort(key=lambda t: -t.get("ev", 0))
    sanrenpuku_tickets = sanrenpuku_tickets[:MAX_FORMATION_TICKETS]

    # ─── 資金配分（全額を三連複に投入）───
    total_stake = STAKE_DEFAULT.get(confidence, 0)
    if total_stake > 0:
        _allocate_formation(sanrenpuku_tickets, total_stake)

    # ─── 回収率・シグナルを付与 ───
    total_inv = sum(t.get("stake", 0) for t in sanrenpuku_tickets)
    for t in sanrenpuku_tickets:
        sk = t.get("stake", 0)
        t["recovery"] = (t["odds"] * sk) / max(total_inv, 1) * 100 if sk > 0 else 0
        t["signal"] = classify_ev(t.get("ev", 0))

    return {
        "col1": col1,
        "col2": col2,
        "col3": col3,
        "sanrenpuku": sanrenpuku_tickets,
        "s_norm": s_norm,
        "confidence": confidence,
    }
