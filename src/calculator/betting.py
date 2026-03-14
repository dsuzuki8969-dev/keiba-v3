"""
競馬解析マスターシステム v3.0 - 買い目・資金配分エンジン (第5章)
"""

import math
from typing import Dict, List, Optional, Tuple

from config.settings import (
    ODDS_DEDUCTION,
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

    deduction = ODDS_DEDUCTION.get("JRA" if is_jra else "JRA", 0.80)
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
        raw_prob3 = calc_sanrenpuku_prob(honmei.place3_prob, ev_b.place3_prob, ev_c.place3_prob, n)
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


def _calc_confidence_score(evaluations: List[HorseEvaluation]) -> float:
    """
    自信度スコアを算出（0.0 ~ 1.0）。
    23,666レースの過去予想 vs 実結果から設計。
    スコアが高い ↔ 的中率が高い（ワイド・3連複で完全単調）。

    主要因子:
    - field_count（頭数）: 最強の予測因子。少頭数ほど的中しやすい
    - gap（1位-2位のcomposite差）: 本命の優位度
    - gap23（2位-3位のcomposite差）: 上位3頭の分離度
    """
    sorted_ev = sorted(evaluations, key=lambda e: e.composite, reverse=True)
    if len(sorted_ev) < 3:
        # 2頭以下は最高自信度（ほぼ当たる）
        return 1.0 if len(sorted_ev) >= 1 else 0.0

    fc = len(sorted_ev)
    gap = sorted_ev[0].composite - sorted_ev[1].composite
    gap23 = sorted_ev[1].composite - sorted_ev[2].composite

    # 各因子を 0-1 に正規化
    gap_norm = min(gap / 8.0, 1.0)
    fc_norm = max(0.0, (16.0 - fc) / 12.0)
    gap23_norm = min(gap23 / 4.0, 1.0)

    # 重み: 頭数 45%, 1-2位差 35%, 2-3位差 20%
    # （23,666R検証: ワイド完全単調、3連複 Q10/Q1 = 5.6倍）
    return gap_norm * 0.35 + fc_norm * 0.45 + gap23_norm * 0.20


def judge_confidence(
    evaluations: List[HorseEvaluation],
    pace_reliability: ConfidenceLevel,
    predicted_umaren: Optional[List] = None,
    predicted_sanrenpuku: Optional[List] = None,
) -> ConfidenceLevel:
    """
    5-3 自信度判定（データドリブン版）。

    23,666レースの過去予想実績から較正。
    スコア = f(頭数, 1-2位差, 2-3位差) → 6段階。

    較正結果（案B: SS厳選型）:
      SS (score≥0.70):  5.8%  ◎1着22.2%  ワイド25.0%  3連複7.9%
      S  (0.55-0.70) : 19.0%  ◎1着19.4%  ワイド18.6%  3連複5.8%
      A  (0.42-0.55) : 25.8%  ◎1着16.2%  ワイド16.4%  3連複4.8%
      B  (0.30-0.42) : 24.9%  ◎1着13.5%  ワイド12.6%  3連複3.2%
      C  (0.20-0.30) : 14.6%  ◎1着12.9%  ワイド10.6%  3連複2.6%
      D  (score<0.20):  9.9%  ◎1着11.6%  ワイド 8.7%  3連複1.4%
    """
    if not evaluations:
        return ConfidenceLevel.D

    score = _calc_confidence_score(evaluations)

    if score >= 0.70:
        return ConfidenceLevel.SS
    elif score >= 0.55:
        return ConfidenceLevel.S
    elif score >= 0.42:
        return ConfidenceLevel.A
    elif score >= 0.30:
        return ConfidenceLevel.B
    elif score >= 0.20:
        return ConfidenceLevel.C
    else:
        return ConfidenceLevel.D


# ============================================================
# 5-4: レース選別（買い/見送り）
# ============================================================


def should_buy_race(
    tickets: List[Dict],
    confidence: ConfidenceLevel,
    evaluations: List[HorseEvaluation],
) -> Tuple[bool, str]:
    """5-4: 見送り条件チェック"""

    if not tickets:
        return False, "期待値100%超えの買い目なし"

    if confidence == ConfidenceLevel.D:
        return False, "自信度D"

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


def estimate_sanrenpuku_odds(
    odds_a: float, odds_b: float, odds_c: float, field_count: int, is_jra: bool = True
) -> float:
    """三連複オッズ推定: 単勝A×B×C ÷ 頭数依存係数 × 払戻率補正 (K-2: JRA/NAR別)"""
    if field_count <= 8:
        factor = 12.0
    elif field_count <= 10:
        factor = 16.0
    elif field_count <= 12:
        factor = 20.0
    elif field_count <= 14:
        factor = 24.0
    else:
        factor = 28.0
    payout_win        = PAYOUT_RATES["jra_win"        if is_jra else "nar_win"]
    payout_sanrenpuku = PAYOUT_RATES["jra_sanrenpuku" if is_jra else "nar_sanrenpuku"]
    deduction = payout_sanrenpuku / payout_win
    return round(max(2.0, odds_a * odds_b * odds_c / factor * deduction), 1)


def calc_sanrenpuku_prob(prob_a: float, prob_b: float, prob_c: float, field_count: int) -> float:
    """三連複出目確率の近似計算"""
    n = field_count
    correction = n * (n - 1) / max(1.0, (n - 2) * (n - 3) * 0.5)
    return min(prob_a * prob_b * prob_c * correction, 0.99)


_MARK_ORDER = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "★": 5, "☆": 6, "×": 7}

def _dedup_sort(ev_list: List[HorseEvaluation]) -> List[HorseEvaluation]:
    seen: set = set()
    result = []
    for e in ev_list:
        if e.horse.horse_no not in seen:
            seen.add(e.horse.horse_no)
            result.append(e)
    return sorted(result, key=lambda e: (_MARK_ORDER.get(e.mark.value, 9), e.horse.horse_no))


def build_formation_columns(
    evaluations: List[HorseEvaluation],
    coverage_grade: str,
) -> Tuple[List[HorseEvaluation], List[HorseEvaluation], List[HorseEvaluation]]:
    """フォーメーションの1・2・3列目を決定する（出現率グレードベース）。

    coverage_grade（三連複+馬連 上位5点 出現率合計）に応じて各列の最大頭数を制御。
    馬の選択は composite 順 + marks（断層を内包）を使用。

    Grade → (col1_max, col2_max, col3_max):
      SS(69%+): 2, 4, 6   S(57%+): 2, 4, 8
      A(44%+) : 2, 5, 9   B(34%+): 2, 6, 12
      C(<34%) : 1, 3, 6
    """
    GRADE_CAPS = {
        "SS": (2, 4, 6),
        "S":  (2, 4, 8),
        "A":  (2, 5, 9),
        "B":  (2, 6, 12),
        "C":  (1, 3, 6),
    }
    cap1, cap2, cap3 = GRADE_CAPS.get(coverage_grade, (2, 5, 9))

    safe_evs = sorted(
        [ev for ev in evaluations if ev.kiken_type == KikenType.NONE],
        key=lambda e: -e.composite,
    )
    if not safe_evs:
        return [], [], []

    # col1（軸）: ◉/◎ のみ、cap1 まで
    col1 = [e for e in safe_evs if e.mark in (Mark.TEKIPAN, Mark.HONMEI)][:cap1]
    if not col1:
        return [], [], []

    col1_ids = {id(e) for e in col1}

    # col2（相手）: col1 + ○/▲ を優先追加、不足時は composite 順で補充、cap2 まで
    col2 = list(col1)
    for e in safe_evs:
        if len(col2) >= cap2:
            break
        if id(e) not in col1_ids and e.mark in (Mark.TAIKOU, Mark.TANNUKE):
            col2.append(e)
    # 不足時: composite 順で残りを補充（col1 除く）
    for e in safe_evs:
        if len(col2) >= cap2:
            break
        if id(e) not in {id(x) for x in col2}:
            col2.append(e)

    col2_ids = {id(e) for e in col2}

    # col3（ヒモ）: col2 + △/★/☆ を優先追加、不足時は composite 順で補充、cap3 まで
    col3 = list(col2)
    for e in safe_evs:
        if len(col3) >= cap3:
            break
        if id(e) not in col2_ids and e.mark in (Mark.RENDASHI, Mark.RENDASHI2, Mark.ANA):
            col3.append(e)
    # 不足時: composite 順で残り全員を補充
    for e in safe_evs:
        if len(col3) >= cap3:
            break
        if id(e) not in {id(x) for x in col3}:
            col3.append(e)

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
                raw = 100  # Kelly=0 なら最低額
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
            ev_a.place3_prob, ev_b.place3_prob, ev_c.place3_prob, field_count
        )
    return max(u_norm, 1e-9), max(s_norm, 1e-9)


def generate_formation_tickets(
    evaluations: List[HorseEvaluation],
    race_info: RaceInfo,
    coverage_grade: str,
) -> Dict:
    """フォーメーション買い目（三連複・馬連）を生成する。

    戻り値: {
        "col1": [...],
        "col2": [...],
        "col3": [...],
        "umaren":      [ticket_dict, ...],   # 馬連 col1×col2
        "sanrenpuku":  [ticket_dict, ...],   # 三連複 col1×col2×col3
        "u_norm":      float,               # 馬連全組み合わせ確率合計
        "s_norm":      float,               # 三連複全組み合わせ確率合計
        "coverage_grade": str,             # 出現率グレード
    }
    """
    col1, col2, col3 = build_formation_columns(evaluations, coverage_grade)
    empty = {"col1": [], "col2": [], "col3": [], "umaren": [], "sanrenpuku": [],
             "u_norm": 1.0, "s_norm": 1.0}
    if not col1 or not col2:
        return empty

    # 全頭ベースの正規化係数（馬連Σ=1.0 / 三連複Σ=1.0 にするための除数）
    u_norm, s_norm = _combo_norm_factors(evaluations, race_info.field_count)

    n = race_info.field_count
    # K-2: is_jra フラグを race_info から取得
    is_jra = getattr(race_info, "is_jra", True)
    mark_map = {ev.horse.horse_no: ev.mark.value for ev in evaluations}
    # 印の優先順位（小さいほど重要 → 表示で左に来る）
    MARK_PRIO = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "☆": 5}

    def _sort_by_mark(horse_nos: List[int]) -> List[int]:
        """印の重要度順（◎が先頭になるよう）に並べ替え"""
        return sorted(horse_nos, key=lambda no: (MARK_PRIO.get(mark_map.get(no, "—"), 9), no))

    # ─── 馬連: col1 × col2 ───
    umaren_tickets: List[Dict] = []
    seen_u: set = set()
    for ev_a in col1:
        for ev_b in col2:
            if ev_a.horse.horse_no == ev_b.horse.horse_no:
                continue
            seen_key = tuple(sorted([ev_a.horse.horse_no, ev_b.horse.horse_no]))
            if seen_key in seen_u:
                continue
            seen_u.add(seen_key)
            ordered = _sort_by_mark([ev_a.horse.horse_no, ev_b.horse.horse_no])
            oa = ev_a.effective_odds or 10.0
            ob = ev_b.effective_odds or 10.0
            odds = estimate_umaren_odds(oa, ob, n, is_jra)  # K-2
            raw_prob = calc_hit_probability(ev_a.place2_prob, ev_b.place2_prob, "馬連", n)
            prob = raw_prob / u_norm        # 正規化: 全組み合わせΣ=1.0
            ev_val = calc_expected_value(prob, odds)
            umaren_tickets.append(
                {
                    "type": "馬連",
                    "a": ordered[0],
                    "b": ordered[1],
                    "mark_a": mark_map.get(ordered[0], "—"),
                    "mark_b": mark_map.get(ordered[1], "—"),
                    "odds": odds,
                    "prob": prob,
                    "ev": ev_val,
                    "appearance": prob * 100,
                    "stake": 0,
                }
            )
    # 優先順（マーク順）でソート。同じマーク組み合わせ内は EV 降順
    MARK_P = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "☆": 5, "穴": 6}
    umaren_tickets.sort(
        key=lambda t: (
            MARK_P.get(t.get("mark_a", ""), 9),
            MARK_P.get(t.get("mark_b", ""), 9),
            -t.get("ev", 0),
        )
    )

    # ─── 三連複: col1 × col2 × col3 ───
    sanrenpuku_tickets: List[Dict] = []
    seen_3: set = set()
    ev_map = {e.horse.horse_no: e for e in evaluations}
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
                odds = estimate_sanrenpuku_odds(oa, ob, oc, n, is_jra)  # K-2
                raw_prob = calc_sanrenpuku_prob(
                    ev_map[ordered[0]].place3_prob,
                    ev_map[ordered[1]].place3_prob,
                    ev_map[ordered[2]].place3_prob,
                    n,
                )
                prob = raw_prob / s_norm    # 正規化: 全組み合わせΣ=1.0
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
    sanrenpuku_tickets.sort(
        key=lambda t: (
            MARK_P.get(t.get("mark_a", ""), 9),
            MARK_P.get(t.get("mark_b", ""), 9),
            MARK_P.get(t.get("mark_c", ""), 9),
            -t.get("ev", 0),
        )
    )

    # ─── 資金配分（軸の連対率/複勝率比で馬連・三連複の割合を決定）───
    # 軸馬の place2_prob / place3_prob = 「複勝圏のうち連対になる割合」
    #   連対率高め → 馬連厚め   複勝率高め・連対率低め → 三連複厚め
    # 割合は [0.3, 0.7] にクランプして極端な偏りを防ぐ
    pivot = col1[0] if col1 else None
    if pivot and (pivot.place3_prob or 0) > 0:
        u_ratio = max(0.3, min(0.7, (pivot.place2_prob or 0) / pivot.place3_prob))
    else:
        u_ratio = 0.5  # データ不足時は均等

    total_stake = STAKE_DEFAULT.get(coverage_grade, 2000)
    if total_stake > 0:
        u_budget = round(total_stake * u_ratio / 100) * 100   # 100円単位
        s_budget = total_stake - u_budget
        _allocate_formation(umaren_tickets, u_budget)
        _allocate_formation(sanrenpuku_tickets, s_budget)

    # ─── 回収率・シグナルを付与 ───
    all_t = umaren_tickets + sanrenpuku_tickets
    total_inv = sum(t.get("stake", 0) for t in all_t)
    for t in all_t:
        sk = t.get("stake", 0)
        # 回収率 = このチケットが当たったとき、投資総額に対して何%回収できるか
        t["recovery"] = (t["odds"] * sk) / max(total_inv, 1) * 100 if sk > 0 else 0
        t["signal"] = classify_ev(t.get("ev", 0))

    return {
        "col1": col1,
        "col2": col2,
        "col3": col3,
        "umaren": umaren_tickets,
        "sanrenpuku": sanrenpuku_tickets,
        "u_norm": u_norm,           # 馬連: 全C(n,2)確率合計（参考値）
        "s_norm": s_norm,           # 三連複: 全C(n,3)確率合計（参考値）
        "coverage_grade": coverage_grade,
        "u_ratio": round(u_ratio, 2),   # 馬連予算割合（参考）
    }
