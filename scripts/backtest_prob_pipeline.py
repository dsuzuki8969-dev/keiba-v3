"""
確率パイプライン最適化バックテスト

問題: composite gap が大きい（一強レース）にもかかわらず、
MLブレンドで確率が均等化されてしまう。

テスト対象:
  A. ML分散ベースの動的ブレンド比率調整
     - ML予測が均等（低分散）→ Rule重視
     - ML予測が偏り（高分散）→ ML重視
  B. gap補正キャップ (RANK_GAP_MULT_MAX) の最適化
  C. popularity_blend の alpha 調整
  D. PROB_SHARPNESS の調整
  E. 複合パターン

評価指標:
  - 確率キャリブレーション（予測確率 vs 実績勝率）
  - 確率差上位の的中率（一強検出力）
  - ◎的中率・回収率（印ベースKPI）
  - Brier Score（確率精度の総合指標）

使い方:
  python scripts/backtest_prob_pipeline.py
  python scripts/backtest_prob_pipeline.py --after 2025-01-01
"""
import argparse
import io
import json
import math
import os
import sys
import statistics as _st
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from rich.console import Console
from rich.progress import Progress, BarColumn, TimeRemainingColumn, TextColumn, MofNCompleteColumn
from rich.table import Table

console = Console()

# ============================================================
# パス設定
# ============================================================
from config.settings import PREDICTIONS_DIR, DATA_DIR

PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
RANK_TABLE_PATH = os.path.join(DATA_DIR, "rank_probability_table.json")
OUTPUT_TXT = os.path.join(PROJECT_ROOT, "data", "backtest_prob_pipeline_summary.txt")
OUTPUT_JSON = os.path.join(PROJECT_ROOT, "data", "backtest_prob_pipeline_results.json")

# ============================================================
# 較正済み重みの読み込み
# ============================================================
_CALIB_VC_TO_NAME = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
    "30": "門別", "35": "盛岡", "36": "水沢",
    "42": "浦和", "43": "船橋", "44": "大井", "45": "川崎",
    "46": "金沢", "47": "笠松", "48": "名古屋",
    "49": "園田", "50": "園田", "51": "姫路",
    "52": "帯広", "65": "帯広",
    "54": "高知", "55": "佐賀",
}

from config.settings import COMPOSITE_WEIGHTS, VENUE_COMPOSITE_WEIGHTS


def _load_calibrated_weights() -> dict:
    calib_path = os.path.join(PROJECT_ROOT, "data", "models", "venue_weights_calibrated.json")
    result = {}
    if not os.path.exists(calib_path):
        return result
    try:
        with open(calib_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for vc, weights in raw.items():
            name = _CALIB_VC_TO_NAME.get(str(vc).zfill(2), "")
            if not name:
                continue
            if all(k in weights for k in ("ability", "pace", "course")):
                w = {
                    "ability": float(weights["ability"]),
                    "pace": float(weights["pace"]),
                    "course": float(weights["course"]),
                }
                if "jockey" not in weights:
                    s = w["ability"] + w["pace"] + w["course"]
                    if s > 0:
                        scale = 0.80 / s
                        w["ability"] *= scale
                        w["pace"] *= scale
                        w["course"] *= scale
                    w["jockey"] = 0.10
                    w["trainer"] = 0.05
                    w["bloodline"] = 0.05
                else:
                    w["jockey"] = float(weights["jockey"])
                    w["trainer"] = float(weights["trainer"])
                    w["bloodline"] = float(weights["bloodline"])
                result[name] = w
    except Exception:
        result = {}
    return result


_CALIB_WEIGHTS = _load_calibrated_weights()


def get_weights(venue_name: str) -> dict:
    if venue_name and venue_name in _CALIB_WEIGHTS:
        return _CALIB_WEIGHTS[venue_name]
    if venue_name and venue_name in VENUE_COMPOSITE_WEIGHTS:
        return VENUE_COMPOSITE_WEIGHTS[venue_name]
    return COMPOSITE_WEIGHTS


# ============================================================
# rank_probability_table 読み込み
# ============================================================
def _load_rank_table() -> Optional[dict]:
    if os.path.exists(RANK_TABLE_PATH):
        try:
            with open(RANK_TABLE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


RANK_TABLE = _load_rank_table()


def _field_group_key(n: int) -> str:
    if n <= 8:
        return "small"
    if n <= 14:
        return "medium"
    return "large"


# ============================================================
# データ読み込み（training_deepと同一構造）
# ============================================================
def load_date_data(date_str: str):
    pred_path = os.path.join(PREDICTIONS_DIR, f"{date_str}_pred.json")
    result_path = os.path.join(RESULTS_DIR, f"{date_str}_results.json")
    if not os.path.exists(pred_path) or not os.path.exists(result_path):
        return None
    try:
        with open(pred_path, "r", encoding="utf-8") as f:
            pred = json.load(f)
        with open(result_path, "r", encoding="utf-8") as f:
            results = json.load(f)
        return pred, results
    except Exception:
        return None


def get_available_dates(after_filter: str = "") -> List[str]:
    dates = []
    for fn in os.listdir(PREDICTIONS_DIR):
        if not fn.endswith("_pred.json") or "_backup" in fn:
            continue
        date_str = fn.replace("_pred.json", "")
        if len(date_str) != 8:
            continue
        date_hyphen = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        if after_filter and date_hyphen < after_filter:
            continue
        result_path = os.path.join(RESULTS_DIR, f"{date_str}_results.json")
        if os.path.exists(result_path):
            dates.append(date_str)
    dates.sort()
    return dates


# ============================================================
# 払戻金取得
# ============================================================
def _extract_payout(payouts: dict, bet_type: str, horse_no: int) -> int:
    data = payouts.get(bet_type)
    if not data:
        return 0
    hno_str = str(horse_no)

    def _match_entry(entry: dict) -> int:
        combo = entry.get("combo")
        if combo is not None and str(combo) == hno_str:
            return int(entry.get("payout", 0) or 0)
        hno = entry.get("horse_no") or entry.get("umaban")
        if hno is not None and int(hno) == horse_no:
            return int(entry.get("payout", 0) or entry.get("払戻", 0) or 0)
        return 0

    if isinstance(data, dict):
        return _match_entry(data)
    elif isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict):
                v = _match_entry(entry)
                if v > 0:
                    return v
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                if int(entry[0]) == horse_no:
                    return int(entry[1])
    return 0


def get_tansho_payout(payouts: dict, horse_no: int) -> int:
    return _extract_payout(payouts, "単勝", horse_no) or _extract_payout(payouts, "tansho", horse_no)


def get_fukusho_payout(payouts: dict, horse_no: int) -> int:
    return _extract_payout(payouts, "複勝", horse_no) or _extract_payout(payouts, "fukusho", horse_no)


# ============================================================
# 全レースデータのメモリロード
# ============================================================
def load_all_race_data(dates: List[str]) -> List[dict]:
    all_races = []
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("データ読み込み中", total=len(dates))
        for date_str in dates:
            data = load_date_data(date_str)
            if data is None:
                progress.advance(task)
                continue
            pred, results = data
            races = pred.get("races", [])

            for race in races:
                race_id = race.get("race_id", "")
                if not race_id:
                    continue
                result = results.get(race_id)
                if not result or not result.get("order"):
                    continue
                finish_map = {
                    r["horse_no"]: r["finish"]
                    for r in result["order"]
                    if isinstance(r, dict) and "horse_no" in r and "finish" in r
                }
                if not finish_map:
                    continue
                horses = race.get("horses", [])
                if len(horses) < 3:
                    continue

                horse_data = []
                for h in horses:
                    hno = h.get("horse_no")
                    if hno is None:
                        continue
                    horse_data.append({
                        "horse_no": hno,
                        "ability": h.get("ability_total", 50.0) or 50.0,
                        "pace": h.get("pace_total", 50.0) or 50.0,
                        "course": h.get("course_total", 50.0) or 50.0,
                        "jockey_dev": h.get("jockey_dev", 50.0) or 50.0,
                        "trainer_dev": h.get("trainer_dev", 50.0) or 50.0,
                        "bloodline_dev": h.get("bloodline_dev", 50.0) or 50.0,
                        "training_dev": h.get("training_dev") if h.get("training_dev") is not None else 50.0,
                        "ml_adj": h.get("ml_composite_adj", 0.0) or 0.0,
                        "odds_adj": h.get("odds_consistency_adj", 0.0) or 0.0,
                        "popularity": h.get("popularity"),
                        "odds": h.get("odds"),
                        # ML予測値: ensemble_prob（LightGBM+PyTorch+LambdaRank後）を使用
                        # ml_win_prob等はブレンド後にNullクリアされるためensemble_probを使う
                        "ensemble_prob": h.get("ensemble_prob"),
                        "model_level": h.get("model_level", 2),
                        # 最終確率値（参照用）
                        "final_win_prob": h.get("win_prob"),
                        "final_place2_prob": h.get("place2_prob"),
                        "final_place3_prob": h.get("place3_prob"),
                    })

                if len(horse_data) < 3:
                    continue

                venue_name = race.get("venue", "")
                venue_code = race.get("venue_code", "")
                is_jra = race.get("is_jra", False)
                if not venue_code and race_id:
                    try:
                        venue_code = race_id[4:6]
                    except Exception:
                        venue_code = ""
                if not isinstance(is_jra, bool):
                    try:
                        vc = int(venue_code)
                        is_jra = (1 <= vc <= 10)
                    except Exception:
                        is_jra = False

                all_races.append({
                    "race_id": race_id,
                    "venue": venue_name,
                    "venue_code": venue_code,
                    "is_jra": is_jra,
                    "field_count": len(horse_data),
                    "horses": horse_data,
                    "finish_map": finish_map,
                    "payouts": result.get("payouts", {}),
                    "weights": get_weights(venue_name),
                })

            progress.advance(task)
    return all_races


# ============================================================
# composite計算
# ============================================================
_TRAINING_ALPHA = 0.006


def calc_composite(hd: dict, weights: dict) -> float:
    """現行composite計算（調教好調ボーナス乗算モデル）"""
    w_ab = weights.get("ability", 0.32)
    w_pa = weights.get("pace", 0.30)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.14)
    w_bl = weights.get("bloodline", 0.05)

    trdev = hd["training_dev"]
    training_multiplier = 1.0
    if trdev > 50:
        training_multiplier = 1.0 + (trdev - 50) * _TRAINING_ALPHA

    v = (
        hd["ability"] * w_ab * training_multiplier
        + hd["pace"] * w_pa * training_multiplier
        + hd["course"] * w_co
        + hd["jockey_dev"] * w_jk
        + hd["trainer_dev"] * w_tr
        + hd["bloodline_dev"] * w_bl
    )
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


# ============================================================
# rank_table ベースの確率推定
# ============================================================
def estimate_rank_probs(
    composite: float,
    all_composites: List[float],
    field_count: int,
    is_jra: bool,
    gap_mult_max: float = 1.2,       # テスト対象パラメータ（旧0.6→1.2）
    gap_threshold: float = 5.0,      # 非1位の減衰閾値
    use_log_gap: bool = True,        # 新対数gap補正を使用
) -> Tuple[float, float, float]:
    """rank_tableから基礎確率を推定し、gap補正を適用"""
    if RANK_TABLE is None:
        return _softmax_fallback(composite, all_composites)

    n = len(all_composites)
    org = "JRA" if is_jra else "NAR"

    sorted_comp = sorted(all_composites, reverse=True)
    rank = sorted_comp.index(composite) + 1

    fc_str = str(field_count)
    fg = _field_group_key(field_count)

    fc_data = RANK_TABLE.get("by_field_count", {}).get(org, {})
    fg_data = RANK_TABLE.get("by_field_group", {}).get(org, {})

    entry = None
    rank_str = str(rank)
    if fc_str in fc_data and rank_str in fc_data[fc_str]:
        entry = fc_data[fc_str][rank_str]
    elif fg in fg_data and rank_str in fg_data[fg]:
        entry = fg_data[fg][rank_str]

    if entry is None:
        return _softmax_fallback(composite, all_composites)

    base_win = entry["win"]
    base_top2 = entry["top2"]
    base_top3 = entry["top3"]

    # gap補正
    gap_1_2 = (sorted_comp[0] - sorted_comp[1]) if n >= 2 else 0.0

    if use_log_gap and rank == 1 and gap_1_2 >= 1.0:
        # 新方式: 連続対数補正（gap 1.0ptから開始）
        raw_bonus = math.log1p(gap_1_2 * 0.25) * 0.40
        # gap 10pt超で減衰
        if gap_1_2 >= 15.0:
            raw_bonus *= 0.45
        elif gap_1_2 >= 10.0:
            raw_bonus *= 0.82
        gap_mult = 1.0 + min(gap_mult_max, raw_bonus)
        base_win *= gap_mult
        top2_gap_mult = 1.0 + (gap_mult - 1.0) * 0.55
        top3_gap_mult = 1.0 + (gap_mult - 1.0) * 0.30
        base_top2 *= top2_gap_mult
        base_top3 *= top3_gap_mult
    elif rank > 1 and gap_1_2 >= gap_threshold:
        # 非1位は一強レース時のみ減衰
        gap_mult = 1.0 - min(0.15, (gap_1_2 - 2.5) * 0.03)
        base_win *= gap_mult
        top2_gap_mult = 1.0 + (gap_mult - 1.0) * 0.5
        top3_gap_mult = 1.0 + (gap_mult - 1.0) * 0.3
        base_top2 *= top2_gap_mult
        base_top3 *= top3_gap_mult
    elif not use_log_gap and gap_1_2 >= gap_threshold:
        # 旧方式（比較用）
        if rank == 1:
            gap_mult = 1.0 + min(gap_mult_max, (gap_1_2 - 2.5) * 0.12)
        else:
            gap_mult = 1.0 - min(0.15, (gap_1_2 - 2.5) * 0.03)
        base_win *= gap_mult
        top2_gap_mult = 1.0 + (gap_mult - 1.0) * 0.5
        top3_gap_mult = 1.0 + (gap_mult - 1.0) * 0.3
        base_top2 *= top2_gap_mult
        base_top3 *= top3_gap_mult
    elif gap_1_2 < 1.0:
        flat_factor = max(0, 1.0 - gap_1_2) * 0.15
        base_win = base_win * (1 - flat_factor) + (1.0 / n) * flat_factor
        base_top2 = base_top2 * (1 - flat_factor) + (2.0 / n) * flat_factor
        base_top3 = base_top3 * (1 - flat_factor) + (3.0 / n) * flat_factor

    win_prob = max(0.01, min(0.85, base_win))
    top2_prob = max(0.02, min(0.92, base_top2))
    top3_prob = max(0.03, min(0.95, base_top3))

    top2_prob = max(top2_prob, win_prob)
    top3_prob = max(top3_prob, top2_prob)

    return win_prob, top2_prob, top3_prob


def _softmax_fallback(composite, all_composites):
    """softmaxフォールバック"""
    T = 8.0
    n = len(all_composites)
    exps = [math.exp((c - max(all_composites)) / T) for c in all_composites]
    s = sum(exps)
    idx = all_composites.index(composite)
    wp = exps[idx] / s
    return wp, min(0.92, wp * 1.6), min(0.95, wp * 2.0)


# ============================================================
# MLブレンドシミュレーション
# ============================================================
def simulate_ml_blend(
    rule_probs: List[Tuple[float, float, float]],   # [(win, p2, p3), ...]
    ml_probs: Optional[List[Tuple[float, float, float]]],  # ML予測があれば
    rule_w: float = 0.55,
    ml_w: float = 0.45,
    # --- 新パラメータ: ML分散ベースの動的調整 ---
    use_variance_adjust: bool = False,
    variance_threshold_low: float = 0.001,   # これ以下は「ML不信」
    variance_threshold_high: float = 0.01,   # これ以上は「ML信頼」
    variance_rule_max: float = 0.90,         # 低分散時のRule最大重み
) -> List[Tuple[float, float, float]]:
    """MLブレンドをシミュレート。ml_probsがNoneの場合はrule_probsをそのまま返す"""
    if ml_probs is None:
        return rule_probs

    n = len(rule_probs)
    actual_rule_w = rule_w
    actual_ml_w = ml_w

    if use_variance_adjust and n >= 3:
        # ML予測の分散を計算
        ml_wins = [p[0] for p in ml_probs]
        ml_var = _st.variance(ml_wins) if len(ml_wins) >= 2 else 0.0

        if ml_var <= variance_threshold_low:
            # MLが完全に均等予測 → Ruleを最大信頼
            actual_rule_w = variance_rule_max
            actual_ml_w = 1.0 - variance_rule_max
        elif ml_var < variance_threshold_high:
            # 中間: 線形補間
            ratio = (ml_var - variance_threshold_low) / (variance_threshold_high - variance_threshold_low)
            actual_rule_w = variance_rule_max - ratio * (variance_rule_max - rule_w)
            actual_ml_w = 1.0 - actual_rule_w
        # else: ml_var >= threshold_high → 元のrule_w, ml_wをそのまま使用

    blended = []
    for i in range(n):
        rw, r2, r3 = rule_probs[i]
        mw, m2, m3 = ml_probs[i]
        bw = actual_rule_w * rw + actual_ml_w * mw
        b2 = actual_rule_w * r2 + actual_ml_w * m2
        b3 = actual_rule_w * r3 + actual_ml_w * m3
        blended.append((bw, b2, b3))

    return blended


# ============================================================
# 正規化
# ============================================================
def normalize_probs(probs: List[Tuple[float, float, float]]) -> List[Tuple[float, float, float]]:
    """確率の合計を正規化"""
    n = len(probs)
    if n == 0:
        return probs

    sum_w = sum(p[0] for p in probs)
    sum_2 = sum(p[1] for p in probs)
    sum_3 = sum(p[2] for p in probs)

    target_w = 1.0
    target_2 = 2.0
    target_3 = 3.0

    result = []
    for w, p2, p3 in probs:
        nw = w / sum_w * target_w if sum_w > 0 else 1.0 / n
        n2 = p2 / sum_2 * target_2 if sum_2 > 0 else 2.0 / n
        n3 = p3 / sum_3 * target_3 if sum_3 > 0 else 3.0 / n
        n2 = max(n2, nw)
        n3 = max(n3, n2)
        result.append((nw, n2, n3))

    return result


# ============================================================
# PROB_SHARPNESS 適用
# ============================================================
def apply_sharpness(probs: List[Tuple[float, float, float]], sharpness: float = 1.0) -> List[Tuple[float, float, float]]:
    if sharpness == 1.0:
        return probs
    result = []
    for w, p2, p3 in probs:
        sw = w ** sharpness if w > 0 else 0
        s2 = p2 ** sharpness if p2 > 0 else 0
        s3 = p3 ** sharpness if p3 > 0 else 0
        result.append((sw, s2, s3))
    # 再正規化
    return normalize_probs(result)


# ============================================================
# パイプライン全体のシミュレーション
# ============================================================
def simulate_pipeline(
    race: dict,
    params: dict,
) -> List[Tuple[int, float, float, float]]:
    """
    1レースの確率パイプラインをシミュレート。

    Returns: [(horse_no, win_prob, place2_prob, place3_prob), ...]
    """
    horses = race["horses"]
    weights = race["weights"]
    field_count = race["field_count"]
    is_jra = race["is_jra"]

    # Step 1: composite計算
    composites = []
    for h in horses:
        c = calc_composite(h, weights)
        composites.append((h["horse_no"], c))

    all_comp = [c for _, c in composites]

    # Step 2: rank_tableから基礎確率
    gap_mult_max = params.get("gap_mult_max", 1.2)
    gap_threshold = params.get("gap_threshold", 5.0)
    use_log_gap = params.get("use_log_gap", True)

    rule_probs = []
    for hno, comp in composites:
        wp, p2, p3 = estimate_rank_probs(
            comp, all_comp, field_count, is_jra,
            gap_mult_max=gap_mult_max,
            gap_threshold=gap_threshold,
            use_log_gap=use_log_gap,
        )
        rule_probs.append((wp, p2, p3))

    # Step 3: MLブレンド（ensemble_probを使用、なければスキップ）
    ml_probs = None
    has_ml = any(h.get("ensemble_prob") is not None for h in horses)
    if has_ml:
        ml_probs = []
        for h in horses:
            ep = h.get("ensemble_prob") or (1.0 / field_count)
            # ensemble_probは勝率のみ。連対率・複勝率は比例拡大で近似
            ml_probs.append((ep, ep * 1.5, ep * 2.0))

    # model_levelに応じたベースブレンド比率
    # 現行engine.pyの_calc_blend_ratio相当
    _BLEND_BY_LEVEL = {
        4: (0.35, 0.65), 3: (0.42, 0.58), 2: (0.55, 0.45),
        1: (0.65, 0.35), 0: (0.75, 0.25),
    }
    avg_level = round(sum(h.get("model_level", 2) for h in horses) / len(horses))
    default_rw, default_mlw = _BLEND_BY_LEVEL.get(avg_level, (0.55, 0.45))

    rule_w = params.get("rule_w") or default_rw
    ml_w = params.get("ml_w") or default_mlw

    # gap連動ブレンド調整（engine.pyの新ロジックを再現）
    use_gap_blend = params.get("use_gap_blend", False)
    if use_gap_blend and len(all_comp) >= 2:
        sorted_c = sorted(all_comp, reverse=True)
        gap_1_2 = sorted_c[0] - sorted_c[1]
        if gap_1_2 >= 2.0:
            gap_boost = min(0.25, math.log1p((gap_1_2 - 2.0) * 0.25) * 0.15)
            if gap_1_2 >= 15.0:
                gap_boost *= 0.40
            elif gap_1_2 >= 10.0:
                gap_boost *= 0.75
            rule_w = min(0.80, rule_w + gap_boost)
            ml_w = 1.0 - rule_w

    blended = simulate_ml_blend(
        rule_probs, ml_probs,
        rule_w=rule_w, ml_w=ml_w,
        use_variance_adjust=params.get("use_variance_adjust", False),
        variance_threshold_low=params.get("variance_threshold_low", 0.001),
        variance_threshold_high=params.get("variance_threshold_high", 0.01),
        variance_rule_max=params.get("variance_rule_max", 0.90),
    )

    # Step 4: 正規化
    blended = normalize_probs(blended)

    # Step 5: PROB_SHARPNESS
    sharpness = params.get("prob_sharpness", 1.45)
    blended = apply_sharpness(blended, sharpness)

    # Step 6: 結果返却
    result = []
    for i, (hno, comp) in enumerate(composites):
        w, p2, p3 = blended[i]
        result.append((hno, w, p2, p3))

    return result


# ============================================================
# KPI集計
# ============================================================
class PipelineKPI:
    """パイプラインパターンごとのKPIを蓄積"""

    def __init__(self, name: str):
        self.name = name
        self.total_races = 0
        # ◎（composite 1位）の成績
        self.honmei_total = 0
        self.honmei_win = 0
        self.honmei_place2 = 0
        self.honmei_place3 = 0
        self.honmei_tansho_stake = 0
        self.honmei_tansho_ret = 0
        self.honmei_fukusho_stake = 0
        self.honmei_fukusho_ret = 0
        # Brier Score（確率精度）
        self.brier_sum_win = 0.0
        self.brier_sum_place3 = 0.0
        self.brier_count = 0
        # キャリブレーション: gap帯別の勝率
        self.gap_bins = defaultdict(lambda: {"total": 0, "win": 0, "place2": 0, "place3": 0,
                                              "pred_win_sum": 0.0, "pred_p2_sum": 0.0, "pred_p3_sum": 0.0})
        # 予測確率帯別の的中率
        self.prob_bins_win = defaultdict(lambda: {"total": 0, "actual": 0})
        self.prob_bins_p3 = defaultdict(lambda: {"total": 0, "actual": 0})
        # 一強検出（gap >= 5pt）
        self.strong_total = 0
        self.strong_win = 0
        self.strong_place3 = 0
        self.strong_pred_win_sum = 0.0
        # 超一強（gap >= 10pt）
        self.very_strong_total = 0
        self.very_strong_win = 0
        self.very_strong_place3 = 0
        self.very_strong_pred_win_sum = 0.0

    def add_race(self, race: dict, predictions: List[Tuple[int, float, float, float]]):
        self.total_races += 1
        finish_map = race["finish_map"]
        payouts = race["payouts"]

        # composite順でソート → 1位が◎
        sorted_preds = sorted(predictions, key=lambda x: -x[1])  # win_probでソートではなくcompositeで
        # compositeを再計算して正しくソート
        horses = race["horses"]
        weights = race["weights"]
        comp_map = {}
        for h in horses:
            comp_map[h["horse_no"]] = calc_composite(h, weights)

        # composite順にソート
        sorted_by_comp = sorted(predictions, key=lambda x: -comp_map.get(x[0], 0))

        if sorted_by_comp:
            top_hno = sorted_by_comp[0][0]
            top_wp, top_p2, top_p3 = sorted_by_comp[0][1], sorted_by_comp[0][2], sorted_by_comp[0][3]
            finish = finish_map.get(top_hno, 99)

            self.honmei_total += 1
            tansho = get_tansho_payout(payouts, top_hno)
            fukusho = get_fukusho_payout(payouts, top_hno)
            self.honmei_tansho_stake += 100
            self.honmei_fukusho_stake += 100

            if finish == 1:
                self.honmei_win += 1
                self.honmei_tansho_ret += tansho
            if finish <= 2:
                self.honmei_place2 += 1
            if finish <= 3:
                self.honmei_place3 += 1
                self.honmei_fukusho_ret += fukusho

            # gap帯別
            all_comp = sorted([comp_map[h["horse_no"]] for h in horses], reverse=True)
            gap = (all_comp[0] - all_comp[1]) if len(all_comp) >= 2 else 0
            gap_bin = _gap_bin(gap)
            self.gap_bins[gap_bin]["total"] += 1
            self.gap_bins[gap_bin]["pred_win_sum"] += top_wp
            self.gap_bins[gap_bin]["pred_p2_sum"] += top_p2
            self.gap_bins[gap_bin]["pred_p3_sum"] += top_p3
            if finish == 1:
                self.gap_bins[gap_bin]["win"] += 1
            if finish <= 2:
                self.gap_bins[gap_bin]["place2"] += 1
            if finish <= 3:
                self.gap_bins[gap_bin]["place3"] += 1

            # 一強
            if gap >= 5.0:
                self.strong_total += 1
                self.strong_pred_win_sum += top_wp
                if finish == 1:
                    self.strong_win += 1
                if finish <= 3:
                    self.strong_place3 += 1
            if gap >= 10.0:
                self.very_strong_total += 1
                self.very_strong_pred_win_sum += top_wp
                if finish == 1:
                    self.very_strong_win += 1
                if finish <= 3:
                    self.very_strong_place3 += 1

        # Brier Score（全馬）
        for hno, wp, p2, p3 in predictions:
            finish = finish_map.get(hno, 99)
            actual_win = 1.0 if finish == 1 else 0.0
            actual_p3 = 1.0 if finish <= 3 else 0.0
            self.brier_sum_win += (wp - actual_win) ** 2
            self.brier_sum_place3 += (p3 - actual_p3) ** 2
            self.brier_count += 1

            # 確率帯別
            prob_bin_w = _prob_bin(wp)
            self.prob_bins_win[prob_bin_w]["total"] += 1
            if finish == 1:
                self.prob_bins_win[prob_bin_w]["actual"] += 1

            prob_bin_3 = _prob_bin(p3)
            self.prob_bins_p3[prob_bin_3]["total"] += 1
            if finish <= 3:
                self.prob_bins_p3[prob_bin_3]["actual"] += 1

    def summary(self) -> dict:
        def _pct(n, d):
            return round(n / d * 100, 1) if d > 0 else 0.0

        s = {
            "name": self.name,
            "total_races": self.total_races,
            "honmei_win_rate": _pct(self.honmei_win, self.honmei_total),
            "honmei_place2_rate": _pct(self.honmei_place2, self.honmei_total),
            "honmei_place3_rate": _pct(self.honmei_place3, self.honmei_total),
            "honmei_tansho_roi": _pct(self.honmei_tansho_ret, self.honmei_tansho_stake),
            "honmei_fukusho_roi": _pct(self.honmei_fukusho_ret, self.honmei_fukusho_stake),
            "brier_win": round(self.brier_sum_win / self.brier_count, 6) if self.brier_count > 0 else 0,
            "brier_place3": round(self.brier_sum_place3 / self.brier_count, 6) if self.brier_count > 0 else 0,
            "strong_total": self.strong_total,
            "strong_win_rate": _pct(self.strong_win, self.strong_total),
            "strong_place3_rate": _pct(self.strong_place3, self.strong_total),
            "strong_avg_pred_win": round(self.strong_pred_win_sum / self.strong_total * 100, 1) if self.strong_total > 0 else 0,
            "very_strong_total": self.very_strong_total,
            "very_strong_win_rate": _pct(self.very_strong_win, self.very_strong_total),
            "very_strong_place3_rate": _pct(self.very_strong_place3, self.very_strong_total),
            "very_strong_avg_pred_win": round(self.very_strong_pred_win_sum / self.very_strong_total * 100, 1) if self.very_strong_total > 0 else 0,
        }

        # gap帯別キャリブレーション
        gap_calib = {}
        for gbin in sorted(self.gap_bins.keys()):
            d = self.gap_bins[gbin]
            gap_calib[gbin] = {
                "total": d["total"],
                "actual_win": _pct(d["win"], d["total"]),
                "actual_p2": _pct(d["place2"], d["total"]),
                "actual_p3": _pct(d["place3"], d["total"]),
                "avg_pred_win": round(d["pred_win_sum"] / d["total"] * 100, 1) if d["total"] > 0 else 0,
                "avg_pred_p3": round(d["pred_p3_sum"] / d["total"] * 100, 1) if d["total"] > 0 else 0,
            }
        s["gap_calibration"] = gap_calib

        return s


def _gap_bin(gap: float) -> str:
    if gap < 1.0:
        return "00-01"
    if gap < 2.0:
        return "01-02"
    if gap < 3.0:
        return "02-03"
    if gap < 5.0:
        return "03-05"
    if gap < 7.5:
        return "05-07"
    if gap < 10.0:
        return "07-10"
    if gap < 15.0:
        return "10-15"
    return "15+"


def _prob_bin(p: float) -> str:
    pct = int(p * 100)
    if pct < 5:
        return "00-05"
    if pct < 10:
        return "05-10"
    if pct < 20:
        return "10-20"
    if pct < 30:
        return "20-30"
    if pct < 40:
        return "30-40"
    if pct < 50:
        return "40-50"
    if pct < 60:
        return "50-60"
    return "60+"


# ============================================================
# テストパターン定義
# ============================================================
def build_test_patterns() -> List[dict]:
    patterns = []

    # ── 旧ベースライン（修正前） ──
    patterns.append({
        "name": "旧BASELINE（修正前）",
        "gap_mult_max": 0.6,
        "gap_threshold": 5.0,
        "use_log_gap": False,
        "use_gap_blend": False,
        "use_variance_adjust": False,
        "prob_sharpness": 1.45,
    })

    # ── 新ベースライン（対数gap補正のみ） ──
    patterns.append({
        "name": "新_対数gap補正のみ",
        "gap_mult_max": 1.2,
        "use_log_gap": True,
        "use_gap_blend": False,
        "use_variance_adjust": False,
        "prob_sharpness": 1.45,
    })

    # ── 新: 対数gap + gap連動ブレンド（本命修正案） ──
    patterns.append({
        "name": "★新_gap補正+gapブレンド",
        "gap_mult_max": 1.2,
        "use_log_gap": True,
        "use_gap_blend": True,
        "use_variance_adjust": False,
        "prob_sharpness": 1.45,
    })

    # ── gap_mult_maxのバリエーション（対数gap + gapブレンド） ──
    for gmax in [0.8, 1.0, 1.5, 2.0]:
        patterns.append({
            "name": f"新_gmax={gmax}+gapblend",
            "gap_mult_max": gmax,
            "use_log_gap": True,
            "use_gap_blend": True,
            "use_variance_adjust": False,
            "prob_sharpness": 1.45,
        })

    # ── sharpnessバリエーション（対数gap + gapブレンド） ──
    for sh in [1.3, 1.5, 1.6, 1.8]:
        patterns.append({
            "name": f"新_gap+blend+sh={sh}",
            "gap_mult_max": 1.2,
            "use_log_gap": True,
            "use_gap_blend": True,
            "use_variance_adjust": False,
            "prob_sharpness": sh,
        })

    # ── gap連動ブレンドのみ（旧gap補正 + gapブレンド） ──
    patterns.append({
        "name": "旧gap+gapブレンド",
        "gap_mult_max": 0.6,
        "gap_threshold": 5.0,
        "use_log_gap": False,
        "use_gap_blend": True,
        "use_variance_adjust": False,
        "prob_sharpness": 1.45,
    })

    # ── ML分散調整も追加 ──
    patterns.append({
        "name": "新_gap+blend+var(0.85)",
        "gap_mult_max": 1.2,
        "use_log_gap": True,
        "use_gap_blend": True,
        "use_variance_adjust": True,
        "variance_threshold_low": 0.001,
        "variance_threshold_high": 0.01,
        "variance_rule_max": 0.85,
        "prob_sharpness": 1.45,
    })

    return patterns


# ============================================================
# メイン実行
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="確率パイプライン最適化バックテスト")
    parser.add_argument("--after", default="", help="この日付以降のデータのみ使用")
    args = parser.parse_args()

    console.print("[bold cyan]確率パイプライン最適化バックテスト[/bold cyan]")
    console.print("=" * 60)

    # データ読み込み
    dates = get_available_dates(after_filter=args.after)
    console.print(f"対象日数: {len(dates)}")

    all_races = load_all_race_data(dates)
    console.print(f"全レース数: {len(all_races)}")

    if not all_races:
        console.print("[red]データが見つかりません[/red]")
        return

    # テストパターン定義
    patterns = build_test_patterns()
    console.print(f"テストパターン数: {len(patterns)}")

    # 全パターン実行
    results = {}
    with Progress(
        TextColumn("[bold green]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("パターンテスト中", total=len(patterns))

        for pat in patterns:
            kpi = PipelineKPI(pat["name"])

            for race in all_races:
                try:
                    preds = simulate_pipeline(race, pat)
                    kpi.add_race(race, preds)
                except Exception:
                    continue

            results[pat["name"]] = kpi.summary()
            progress.advance(task)

    # ============================================================
    # 結果出力
    # ============================================================
    output_lines = []

    def _out(line=""):
        output_lines.append(line)
        console.print(line)

    _out("=" * 80)
    _out("確率パイプライン最適化バックテスト結果")
    _out(f"レース数: {len(all_races)}, パターン数: {len(patterns)}")
    _out("=" * 80)

    # ── ランキング表（◎勝率 + Brier Score + 一強的中率） ──
    _out("\n■ 総合ランキング（◎勝率順）")
    _out("-" * 120)
    _out(f"{'パターン':<45} {'◎勝率':>6} {'◎連対':>6} {'◎複勝':>6} {'単回収':>6} {'複回収':>6} {'Brier_W':>8} {'一強勝率':>8} {'超一強勝率':>10}")
    _out("-" * 120)

    sorted_results = sorted(results.items(), key=lambda x: -x[1]["honmei_win_rate"])
    for name, s in sorted_results:
        _out(
            f"{name:<45} "
            f"{s['honmei_win_rate']:>5.1f}% "
            f"{s['honmei_place2_rate']:>5.1f}% "
            f"{s['honmei_place3_rate']:>5.1f}% "
            f"{s['honmei_tansho_roi']:>5.1f}% "
            f"{s['honmei_fukusho_roi']:>5.1f}% "
            f"{s['brier_win']:>8.6f} "
            f"{s['strong_win_rate']:>6.1f}%({s['strong_total']:>4}) "
            f"{s['very_strong_win_rate']:>6.1f}%({s['very_strong_total']:>4})"
        )

    # ── Brier Score ランキング ──
    _out("\n■ Brier Score ランキング（低いほど良い）")
    _out("-" * 80)
    sorted_brier = sorted(results.items(), key=lambda x: x[1]["brier_win"])
    for name, s in sorted_brier[:15]:
        _out(
            f"  {name:<45} Brier_win={s['brier_win']:.6f}  Brier_p3={s['brier_place3']:.6f}"
        )

    # ── 一強レース（gap>=5pt）ランキング ──
    _out("\n■ 一強レース（gap≥5pt）勝率ランキング")
    _out("-" * 80)
    sorted_strong = sorted(results.items(), key=lambda x: -x[1]["strong_win_rate"])
    for name, s in sorted_strong[:15]:
        _out(
            f"  {name:<45} "
            f"勝率={s['strong_win_rate']:>5.1f}% "
            f"複勝率={s['strong_place3_rate']:>5.1f}% "
            f"予測平均={s['strong_avg_pred_win']:>5.1f}% "
            f"(n={s['strong_total']})"
        )

    # ── 超一強レース（gap>=10pt） ──
    _out("\n■ 超一強レース（gap≥10pt）勝率ランキング")
    _out("-" * 80)
    sorted_vstrong = sorted(results.items(), key=lambda x: -x[1]["very_strong_win_rate"])
    for name, s in sorted_vstrong[:15]:
        _out(
            f"  {name:<45} "
            f"勝率={s['very_strong_win_rate']:>5.1f}% "
            f"複勝率={s['very_strong_place3_rate']:>5.1f}% "
            f"予測平均={s['very_strong_avg_pred_win']:>5.1f}% "
            f"(n={s['very_strong_total']})"
        )

    # ── ベースラインのgap帯別キャリブレーション ──
    baseline = results.get("BASELINE（現行）", {})
    if baseline and "gap_calibration" in baseline:
        _out("\n■ ベースラインのgap帯別キャリブレーション（◎）")
        _out("-" * 100)
        _out(f"  {'gap帯':<8} {'レース数':>6} {'実勝率':>6} {'実連対':>6} {'実複勝':>6} | {'予測勝率':>8} {'予測複勝':>8} | {'差(勝率)':>8}")
        _out("-" * 100)
        for gbin, d in sorted(baseline["gap_calibration"].items()):
            diff = d["actual_win"] - d["avg_pred_win"]
            _out(
                f"  {gbin:<8} {d['total']:>6} "
                f"{d['actual_win']:>5.1f}% {d['actual_p2']:>5.1f}% {d['actual_p3']:>5.1f}% | "
                f"{d['avg_pred_win']:>7.1f}% {d['avg_pred_p3']:>7.1f}% | "
                f"{diff:>+7.1f}%"
            )

    # ── 最優秀パターンのgap帯別キャリブレーション ──
    best_name = sorted_results[0][0] if sorted_results else None
    if best_name and best_name != "BASELINE（現行）":
        best = results[best_name]
        if "gap_calibration" in best:
            _out(f"\n■ 最優秀パターン [{best_name}] のgap帯別キャリブレーション（◎）")
            _out("-" * 100)
            _out(f"  {'gap帯':<8} {'レース数':>6} {'実勝率':>6} {'実連対':>6} {'実複勝':>6} | {'予測勝率':>8} {'予測複勝':>8} | {'差(勝率)':>8}")
            _out("-" * 100)
            for gbin, d in sorted(best["gap_calibration"].items()):
                diff = d["actual_win"] - d["avg_pred_win"]
                _out(
                    f"  {gbin:<8} {d['total']:>6} "
                    f"{d['actual_win']:>5.1f}% {d['actual_p2']:>5.1f}% {d['actual_p3']:>5.1f}% | "
                    f"{d['avg_pred_win']:>7.1f}% {d['avg_pred_p3']:>7.1f}% | "
                    f"{diff:>+7.1f}%"
                )

    # ── 複合スコアランキング（◎勝率 × 一強勝率 × (1/Brier)） ──
    _out("\n■ 複合スコアランキング（◎勝率 × 一強勝率 × 回収率バランス）")
    _out("-" * 100)

    def _composite_score(s):
        """総合評価スコア（大きいほど良い）"""
        # ◎勝率 × 一強勝率 × 複勝回収率のバランス
        hw = s["honmei_win_rate"]
        sw = s["strong_win_rate"] if s["strong_total"] >= 10 else hw
        fr = s["honmei_fukusho_roi"]
        brier = s["brier_win"]
        # Brier低いほど良い → 反転
        brier_inv = 1.0 / (brier + 0.001)
        return hw * 0.3 + sw * 0.25 + fr * 0.002 + brier_inv * 0.0001

    sorted_composite = sorted(results.items(), key=lambda x: -_composite_score(x[1]))
    for i, (name, s) in enumerate(sorted_composite[:20]):
        score = _composite_score(s)
        _out(
            f"  {i+1:>2}. {name:<42} "
            f"◎勝率={s['honmei_win_rate']:>5.1f}% "
            f"一強勝率={s['strong_win_rate']:>5.1f}% "
            f"複回収={s['honmei_fukusho_roi']:>5.1f}% "
            f"Brier={s['brier_win']:.6f} "
            f"Score={score:.2f}"
        )

    # ファイル出力
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    console.print(f"\n[bold green]サマリー出力: {OUTPUT_TXT}[/bold green]")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    console.print(f"[bold green]詳細JSON: {OUTPUT_JSON}[/bold green]")


if __name__ == "__main__":
    main()
