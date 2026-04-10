"""
調教偏差値 composite 組み込み — 深掘りバックテスト

前回の基本バックテスト（30パターン）を拡張し、以下を網羅的に検証:
  A. 細粒度パラメータサーチ（D系列 α=0.001〜0.015, 0.0005刻み）
  B. 非線形モデル（シグモイド/二次関数/閾値型）
  C. 非対称効果（上方α/下方α独立最適化）
  D. 乗算対象の組み合わせ
  E. 条件別効果（JRA/NAR, クラス, 距離, 人気, 自信度）
  F. 能力レベルとの交互作用
  G. 生データ相関分析

使い方:
  python scripts/backtest_training_deep.py
  python scripts/backtest_training_deep.py --year 2026
  python scripts/backtest_training_deep.py --after 2026-03-01
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

from rich.console import Console
from rich.progress import Progress, BarColumn, TimeRemainingColumn, TextColumn, MofNCompleteColumn

console = Console()

# ============================================================
# パス設定
# ============================================================
from config.settings import PREDICTIONS_DIR

PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
OUTPUT_TXT = os.path.join(PROJECT_ROOT, "data", "backtest_training_deep_summary.txt")
OUTPUT_JSON = os.path.join(PROJECT_ROOT, "data", "backtest_training_deep_results.json")

# ============================================================
# 較正済み重みの読み込み（backtest_training_model.py と同一ロジック）
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
# データ読み込み
# ============================================================

def load_date_data(date_str: str) -> Optional[Tuple[dict, dict]]:
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


def get_available_dates(year_filter: str = "", after_filter: str = "") -> List[str]:
    dates = []
    for fn in os.listdir(PREDICTIONS_DIR):
        if not fn.endswith("_pred.json") or "_backup" in fn:
            continue
        date_str = fn.replace("_pred.json", "")
        if len(date_str) != 8:
            continue
        if year_filter and not date_str.startswith(year_filter):
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
# 印割当て
# ============================================================

MARKS = ["◎", "○", "▲", "△", "★"]


def assign_marks(horses_with_composite: List[Tuple[int, float]]) -> Dict[int, str]:
    sorted_horses = sorted(horses_with_composite, key=lambda x: -x[1])
    result = {}
    for i, (hno, _) in enumerate(sorted_horses[:5]):
        result[hno] = MARKS[i]
    return result


# ============================================================
# KPI集計クラス
# ============================================================

def pct(n, d):
    return round(n / d * 100, 1) if d > 0 else 0.0


class KPIAccumulator:
    """パターンごとのKPIを蓄積"""
    __slots__ = ['total', 'win', 'place2', 'placed',
                 'tansho_stake', 'tansho_ret',
                 'fukusho_stake', 'fukusho_ret']

    def __init__(self):
        self.total = 0
        self.win = 0
        self.place2 = 0
        self.placed = 0
        self.tansho_stake = 0
        self.tansho_ret = 0
        self.fukusho_stake = 0
        self.fukusho_ret = 0

    def add(self, finish: int, tansho: int, fukusho: int):
        self.total += 1
        if finish == 1:
            self.win += 1
        if finish <= 2:
            self.place2 += 1
        if finish <= 3:
            self.placed += 1
        self.tansho_stake += 100
        if finish == 1:
            self.tansho_ret += tansho
        self.fukusho_stake += 100
        if finish <= 3:
            self.fukusho_ret += fukusho

    @property
    def win_rate(self):
        return pct(self.win, self.total)

    @property
    def place2_rate(self):
        return pct(self.place2, self.total)

    @property
    def placed_rate(self):
        return pct(self.placed, self.total)

    @property
    def tansho_roi(self):
        return pct(self.tansho_ret, self.tansho_stake)

    @property
    def fukusho_roi(self):
        return pct(self.fukusho_ret, self.fukusho_stake)

    def to_dict(self):
        return {
            "total": self.total,
            "win_rate": self.win_rate,
            "place2_rate": self.place2_rate,
            "placed_rate": self.placed_rate,
            "tansho_roi": self.tansho_roi,
            "fukusho_roi": self.fukusho_roi,
        }


# ============================================================
# composite計算関数群
# ============================================================

def calc_base_6factor(hd: dict, weights: dict) -> float:
    """6因子加重平均（training除く）"""
    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)
    return (hd["ability"] * w_ab + hd["pace"] * w_pa + hd["course"] * w_co
            + hd["jockey_dev"] * w_jk + hd["trainer_dev"] * w_tr
            + hd["bloodline_dev"] * w_bl)


def calc_composite_general(hd: dict, weights: dict, coeff_fn) -> float:
    """汎用composite計算。coeff_fnはtraining_devを受けてdict of 乗数を返す"""
    td = hd["training_dev"]
    coeffs = coeff_fn(td)

    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)

    ab = hd["ability"] * coeffs.get("ability", 1.0)
    pa = hd["pace"] * coeffs.get("pace", 1.0)
    co = hd["course"] * coeffs.get("course", 1.0)
    jk = hd["jockey_dev"] * coeffs.get("jockey", 1.0)
    tr = hd["trainer_dev"]
    bl = hd["bloodline_dev"]

    v = ab * w_ab + pa * w_pa + co * w_co + jk * w_jk + tr * w_tr + bl * w_bl
    v += coeffs.get("additive", 0.0)
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


# ============================================================
# 全レースデータのメモリロード
# ============================================================

def load_all_race_data(dates: List[str]) -> List[dict]:
    """全日付のレースデータをメモリにロードして返す。
    各要素: {race_id, venue, venue_code, is_jra, grade, distance, surface,
             confidence, horses: [{horse_no, ability, pace, course, ...}],
             finish_map, payouts}
    """
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

            # この日付にtraining_devが1つでもあるか確認
            has_training = False
            for race in races:
                for h in race.get("horses", []):
                    if h.get("training_dev") is not None:
                        has_training = True
                        break
                if has_training:
                    break

            if not has_training:
                progress.advance(task)
                continue

            for race in races:
                race_id = race.get("race_id", "")
                if not race_id:
                    continue

                result = results.get(race_id)
                if not result or not result.get("order"):
                    continue

                finish_map = {r["horse_no"]: r["finish"]
                              for r in result["order"]
                              if isinstance(r, dict) and "horse_no" in r and "finish" in r}
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
                    })

                if len(horse_data) < 3:
                    continue

                venue_name = race.get("venue", "")
                # venue_codeからJRA/NAR判定
                venue_code = race.get("venue_code", "")
                is_jra = race.get("is_jra", False)
                # venue_codeがない場合、race_idから推定
                if not venue_code and race_id:
                    try:
                        venue_code = race_id[4:6]
                    except Exception:
                        venue_code = ""
                if not isinstance(is_jra, bool):
                    # venue_codeで判定
                    try:
                        vc = int(venue_code)
                        is_jra = (1 <= vc <= 10)
                    except Exception:
                        is_jra = False

                # 距離カテゴリ
                dist = race.get("distance", 0) or 0
                if dist <= 1200:
                    dist_cat = "短距離"
                elif dist <= 1600:
                    dist_cat = "マイル"
                elif dist <= 2200:
                    dist_cat = "中距離"
                else:
                    dist_cat = "長距離"

                # グレード
                grade = race.get("grade", "") or ""

                all_races.append({
                    "race_id": race_id,
                    "venue": venue_name,
                    "venue_code": venue_code,
                    "is_jra": is_jra,
                    "grade": grade,
                    "distance": dist,
                    "dist_cat": dist_cat,
                    "surface": race.get("surface", ""),
                    "confidence": race.get("confidence", "B"),
                    "horses": horse_data,
                    "finish_map": finish_map,
                    "payouts": result.get("payouts", {}),
                    "weights": get_weights(venue_name),
                })

            progress.advance(task)

    return all_races


# ============================================================
# 汎用バックテスト実行
# ============================================================

def run_patterns(all_races: List[dict], pattern_configs: dict,
                 composite_fn, filter_fn=None) -> dict:
    """
    pattern_configs: {name: config_dict, ...}
    composite_fn(hd, weights, config) -> float
    filter_fn(race) -> bool (Noneなら全レース)
    返値: {name: KPIAccumulator}
    """
    results = {name: KPIAccumulator() for name in pattern_configs}

    for race in all_races:
        if filter_fn and not filter_fn(race):
            continue

        horses = race["horses"]
        finish_map = race["finish_map"]
        payouts = race["payouts"]
        weights = race["weights"]

        for pname, config in pattern_configs.items():
            # composite再計算
            composites = []
            for hd in horses:
                c = composite_fn(hd, weights, config)
                composites.append((hd["horse_no"], c))

            # 印割当て → ◎の成績
            mark_map = assign_marks(composites)
            honmei_hno = None
            for hno, mk in mark_map.items():
                if mk == "◎":
                    honmei_hno = hno
                    break

            if honmei_hno is not None:
                pos = finish_map.get(honmei_hno, 99)
                tansho = get_tansho_payout(payouts, honmei_hno)
                fukusho = get_fukusho_payout(payouts, honmei_hno)
                results[pname].add(pos, tansho, fukusho)

    return results


def run_patterns_all_marks(all_races: List[dict], pattern_configs: dict,
                           composite_fn, filter_fn=None) -> dict:
    """全印の成績を集計（セクションE用）"""
    results = {}
    for pname in pattern_configs:
        results[pname] = {
            "honmei": KPIAccumulator(),
            "by_mark": {m: KPIAccumulator() for m in MARKS},
            "by_pop": {
                "1-3人気": KPIAccumulator(),
                "4-6人気": KPIAccumulator(),
                "7人気以下": KPIAccumulator(),
            },
            "by_conf": defaultdict(KPIAccumulator),
        }

    for race in all_races:
        if filter_fn and not filter_fn(race):
            continue

        horses = race["horses"]
        finish_map = race["finish_map"]
        payouts = race["payouts"]
        weights = race["weights"]
        confidence = race["confidence"]

        for pname, config in pattern_configs.items():
            composites = []
            for hd in horses:
                c = composite_fn(hd, weights, config)
                composites.append((hd["horse_no"], c))

            mark_map = assign_marks(composites)

            for hno, mk in mark_map.items():
                pos = finish_map.get(hno, 99)
                tansho = get_tansho_payout(payouts, hno)
                fukusho = get_fukusho_payout(payouts, hno)
                results[pname]["by_mark"][mk].add(pos, tansho, fukusho)

            honmei_hno = None
            for hno, mk in mark_map.items():
                if mk == "◎":
                    honmei_hno = hno
                    break

            if honmei_hno is not None:
                pos = finish_map.get(honmei_hno, 99)
                tansho = get_tansho_payout(payouts, honmei_hno)
                fukusho = get_fukusho_payout(payouts, honmei_hno)
                results[pname]["honmei"].add(pos, tansho, fukusho)
                results[pname]["by_conf"][confidence].add(pos, tansho, fukusho)

                # 人気別
                pop = None
                for hd in horses:
                    if hd["horse_no"] == honmei_hno:
                        pop = hd.get("popularity")
                        break
                if pop is not None:
                    if pop <= 3:
                        results[pname]["by_pop"]["1-3人気"].add(pos, tansho, fukusho)
                    elif pop <= 6:
                        results[pname]["by_pop"]["4-6人気"].add(pos, tansho, fukusho)
                    else:
                        results[pname]["by_pop"]["7人気以下"].add(pos, tansho, fukusho)

    return results


# ============================================================
# composite計算ファクトリ
# ============================================================

def make_d_composite(hd, weights, config):
    """D系列: 能力+展開に乗算"""
    alpha = config["alpha"]
    td = hd["training_dev"]
    coeff = 1.0 + (td - 50.0) * alpha
    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)
    v = (hd["ability"] * coeff * w_ab + hd["pace"] * coeff * w_pa
         + hd["course"] * w_co + hd["jockey_dev"] * w_jk
         + hd["trainer_dev"] * w_tr + hd["bloodline_dev"] * w_bl)
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


def make_sigmoid_composite(hd, weights, config):
    """シグモイド型: 1 + beta * tanh((td-50)/gamma)"""
    beta = config["beta"]
    gamma = config["gamma"]
    td = hd["training_dev"]
    coeff = 1.0 + beta * math.tanh((td - 50.0) / gamma)
    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)
    v = (hd["ability"] * coeff * w_ab + hd["pace"] * coeff * w_pa
         + hd["course"] * w_co + hd["jockey_dev"] * w_jk
         + hd["trainer_dev"] * w_tr + hd["bloodline_dev"] * w_bl)
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


def make_quadratic_composite(hd, weights, config):
    """二次関数型: 1 + alpha*(td-50) + delta*(td-50)^2"""
    alpha = config["alpha"]
    delta = config["delta"]
    td = hd["training_dev"]
    diff = td - 50.0
    coeff = 1.0 + alpha * diff + delta * diff * diff
    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)
    v = (hd["ability"] * coeff * w_ab + hd["pace"] * coeff * w_pa
         + hd["course"] * w_co + hd["jockey_dev"] * w_jk
         + hd["trainer_dev"] * w_tr + hd["bloodline_dev"] * w_bl)
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


def make_threshold_composite(hd, weights, config):
    """閾値型"""
    ttype = config["threshold_type"]
    td = hd["training_dev"]

    if ttype == "high_only":
        alpha = config["alpha"]
        coeff = 1.0 + alpha * max(0.0, td - 55.0)
    elif ttype == "low_only":
        alpha = config["alpha"]
        coeff = 1.0 - alpha * max(0.0, 45.0 - td)
    elif ttype == "asymmetric":
        alpha_hi = config["alpha_hi"]
        alpha_lo = config["alpha_lo"]
        coeff = 1.0 + alpha_hi * max(0.0, td - 55.0) - alpha_lo * max(0.0, 45.0 - td)
    else:
        coeff = 1.0

    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)
    v = (hd["ability"] * coeff * w_ab + hd["pace"] * coeff * w_pa
         + hd["course"] * w_co + hd["jockey_dev"] * w_jk
         + hd["trainer_dev"] * w_tr + hd["bloodline_dev"] * w_bl)
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


def make_asymmetric_composite(hd, weights, config):
    """非対称: 上方αと下方αを独立"""
    alpha_up = config["alpha_up"]
    alpha_down = config["alpha_down"]
    td = hd["training_dev"]
    diff = td - 50.0
    if diff >= 0:
        coeff = 1.0 + diff * alpha_up
    else:
        coeff = 1.0 + diff * alpha_down  # diffが負なので自動的にペナルティ

    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)
    v = (hd["ability"] * coeff * w_ab + hd["pace"] * coeff * w_pa
         + hd["course"] * w_co + hd["jockey_dev"] * w_jk
         + hd["trainer_dev"] * w_tr + hd["bloodline_dev"] * w_bl)
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


def make_target_composite(hd, weights, config):
    """乗算対象の組み合わせ"""
    alpha = config["alpha"]
    targets = config["targets"]  # list of factor names
    td = hd["training_dev"]
    coeff = 1.0 + (td - 50.0) * alpha

    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)

    ab_c = coeff if "ability" in targets else 1.0
    pa_c = coeff if "pace" in targets else 1.0
    co_c = coeff if "course" in targets else 1.0
    jk_c = coeff if "jockey" in targets else 1.0

    v = (hd["ability"] * ab_c * w_ab + hd["pace"] * pa_c * w_pa
         + hd["course"] * co_c * w_co + hd["jockey_dev"] * jk_c * w_jk
         + hd["trainer_dev"] * w_tr + hd["bloodline_dev"] * w_bl)
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


def make_dynamic_alpha_composite(hd, weights, config):
    """能力レベルに応じた動的α: α = base_α * (ability / 50)"""
    base_alpha = config["base_alpha"]
    td = hd["training_dev"]
    ability = hd["ability"]
    alpha = base_alpha * (ability / 50.0)
    coeff = 1.0 + (td - 50.0) * alpha

    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)
    v = (hd["ability"] * coeff * w_ab + hd["pace"] * coeff * w_pa
         + hd["course"] * w_co + hd["jockey_dev"] * w_jk
         + hd["trainer_dev"] * w_tr + hd["bloodline_dev"] * w_bl)
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


def make_baseline_composite(hd, weights, config):
    """現行baseline（加算 w_tn=0.03）"""
    w_tn = config.get("w_tn", 0.03)
    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)
    v = (hd["ability"] * w_ab + hd["pace"] * w_pa + hd["course"] * w_co
         + hd["jockey_dev"] * w_jk + hd["trainer_dev"] * w_tr
         + hd["bloodline_dev"] * w_bl + hd["training_dev"] * w_tn)
    v += hd["ml_adj"] + hd["odds_adj"]
    return max(20.0, min(100.0, v))


# ============================================================
# セクション実行関数群
# ============================================================

def section_a(all_races, lines, json_out):
    """A. 細粒度パラメータサーチ（D系列 α=0.001〜0.015, 0.0005刻み）"""
    lines.append("=" * 100)
    lines.append("セクション A: 細粒度パラメータサーチ（D系列 α精密探索）")
    lines.append("=" * 100)

    # baseline
    base_configs = {"baseline(w=0.03)": {"w_tn": 0.03}}
    base_results = run_patterns(all_races, base_configs, make_baseline_composite)
    base_kpi = base_results["baseline(w=0.03)"]

    # D系列 29段階
    configs = {}
    alphas = []
    a = 0.001
    while a <= 0.0151:
        alpha = round(a, 4)
        alphas.append(alpha)
        configs[f"D(α={alpha:.4f})"] = {"alpha": alpha}
        a += 0.0005

    results = run_patterns(all_races, configs, make_d_composite)

    lines.append(f"{'パターン':<20} {'◎件数':>6} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7} {'vs現行複勝':>10}")
    lines.append("-" * 100)
    lines.append(f"{'baseline(w=0.03)':<20} {base_kpi.total:>6} {base_kpi.win_rate:>6.1f}% {base_kpi.place2_rate:>6.1f}% {base_kpi.placed_rate:>6.1f}% {base_kpi.tansho_roi:>6.1f}% {base_kpi.fukusho_roi:>6.1f}% {'---':>10}")

    best_placed = ("", 0.0)
    best_roi = ("", 0.0)
    section_results = {}

    for alpha in alphas:
        name = f"D(α={alpha:.4f})"
        kpi = results[name]
        diff = kpi.placed_rate - base_kpi.placed_rate
        lines.append(f"{name:<20} {kpi.total:>6} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}% {diff:>+9.1f}pp")
        section_results[name] = kpi.to_dict()
        if kpi.placed_rate > best_placed[1]:
            best_placed = (name, kpi.placed_rate)
        if kpi.tansho_roi > best_roi[1]:
            best_roi = (name, kpi.tansho_roi)

    lines.append(f"\n>>> 複勝率ベスト: {best_placed[0]} ({best_placed[1]:.1f}%)")
    lines.append(f">>> 単ROIベスト:  {best_roi[0]} ({best_roi[1]:.1f}%)")
    lines.append("")

    json_out["section_a"] = {
        "baseline": base_kpi.to_dict(),
        "patterns": section_results,
        "best_placed": best_placed[0],
        "best_roi": best_roi[0],
    }
    return best_placed, best_roi


def section_b(all_races, lines, json_out):
    """B. 非線形モデル"""
    lines.append("=" * 100)
    lines.append("セクション B: 非線形モデル")
    lines.append("=" * 100)

    # baseline
    base_configs = {"baseline(w=0.03)": {"w_tn": 0.03}}
    base_results = run_patterns(all_races, base_configs, make_baseline_composite)
    base_kpi = base_results["baseline(w=0.03)"]

    all_results = {}

    # B-1: シグモイド型
    lines.append("\n--- B-1: シグモイド型 (1 + β*tanh((td-50)/γ)) ---")
    lines.append(f"{'β':>6} {'γ':>6} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7} {'vs現行':>8}")
    lines.append("-" * 80)

    sig_configs = {}
    for beta in [0.02, 0.03, 0.04, 0.05, 0.06, 0.08]:
        for gamma in [10, 15, 20, 25, 30]:
            name = f"sig(β={beta},γ={gamma})"
            sig_configs[name] = {"beta": beta, "gamma": gamma}

    sig_results = run_patterns(all_races, sig_configs, make_sigmoid_composite)

    best_sig = ("", 0.0)
    for beta in [0.02, 0.03, 0.04, 0.05, 0.06, 0.08]:
        for gamma in [10, 15, 20, 25, 30]:
            name = f"sig(β={beta},γ={gamma})"
            kpi = sig_results[name]
            diff = kpi.placed_rate - base_kpi.placed_rate
            lines.append(f"{beta:>6.2f} {gamma:>6} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}% {diff:>+7.1f}pp")
            all_results[name] = kpi.to_dict()
            if kpi.placed_rate > best_sig[1]:
                best_sig = (name, kpi.placed_rate)

    lines.append(f">>> シグモイド複勝率ベスト: {best_sig[0]} ({best_sig[1]:.1f}%)")

    # B-2: 二次関数型
    lines.append("\n--- B-2: 二次関数型 (1 + α*(td-50) + δ*(td-50)^2) ---")
    lines.append(f"{'α':>8} {'δ':>10} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7} {'vs現行':>8}")
    lines.append("-" * 90)

    quad_configs = {}
    for alpha in [0.0, 0.002, 0.004, 0.006]:
        for delta in [0.00005, 0.0001, 0.0002, 0.0005, 0.001]:
            name = f"quad(α={alpha},δ={delta})"
            quad_configs[name] = {"alpha": alpha, "delta": delta}

    quad_results = run_patterns(all_races, quad_configs, make_quadratic_composite)

    best_quad = ("", 0.0)
    for alpha in [0.0, 0.002, 0.004, 0.006]:
        for delta in [0.00005, 0.0001, 0.0002, 0.0005, 0.001]:
            name = f"quad(α={alpha},δ={delta})"
            kpi = quad_results[name]
            diff = kpi.placed_rate - base_kpi.placed_rate
            lines.append(f"{alpha:>8.3f} {delta:>10.5f} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}% {diff:>+7.1f}pp")
            all_results[name] = kpi.to_dict()
            if kpi.placed_rate > best_quad[1]:
                best_quad = (name, kpi.placed_rate)

    lines.append(f">>> 二次関数複勝率ベスト: {best_quad[0]} ({best_quad[1]:.1f}%)")

    # B-3: 閾値型
    lines.append("\n--- B-3: 閾値型 ---")
    lines.append(f"{'タイプ':<20} {'α/αhi/αlo':>12} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7} {'vs現行':>8}")
    lines.append("-" * 95)

    thresh_configs = {}
    # 好調のみ
    for alpha in [0.002, 0.005, 0.008, 0.010, 0.015]:
        name = f"hi_only(α={alpha})"
        thresh_configs[name] = {"threshold_type": "high_only", "alpha": alpha}
    # 不調のみ
    for alpha in [0.002, 0.005, 0.008, 0.010, 0.015]:
        name = f"lo_only(α={alpha})"
        thresh_configs[name] = {"threshold_type": "low_only", "alpha": alpha}
    # 非対称閾値
    for ahi in [0.003, 0.005, 0.008]:
        for alo in [0.005, 0.008, 0.010]:
            name = f"asym_th(hi={ahi},lo={alo})"
            thresh_configs[name] = {"threshold_type": "asymmetric", "alpha_hi": ahi, "alpha_lo": alo}

    thresh_results = run_patterns(all_races, thresh_configs, make_threshold_composite)

    best_thresh = ("", 0.0)
    for name in thresh_configs:
        kpi = thresh_results[name]
        diff = kpi.placed_rate - base_kpi.placed_rate
        cfg = thresh_configs[name]
        if cfg["threshold_type"] == "asymmetric":
            param_str = f"hi={cfg['alpha_hi']},lo={cfg['alpha_lo']}"
        else:
            param_str = f"α={cfg['alpha']}"
        lines.append(f"{name:<20} {param_str:>12} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}% {diff:>+7.1f}pp")
        all_results[name] = kpi.to_dict()
        if kpi.placed_rate > best_thresh[1]:
            best_thresh = (name, kpi.placed_rate)

    lines.append(f">>> 閾値型複勝率ベスト: {best_thresh[0]} ({best_thresh[1]:.1f}%)")
    lines.append("")

    json_out["section_b"] = {
        "baseline": base_kpi.to_dict(),
        "patterns": all_results,
        "best_sigmoid": best_sig[0],
        "best_quadratic": best_quad[0],
        "best_threshold": best_thresh[0],
    }


def section_c(all_races, lines, json_out):
    """C. 非対称効果"""
    lines.append("=" * 100)
    lines.append("セクション C: 非対称効果（上方α vs 下方α 独立最適化）")
    lines.append("=" * 100)

    base_configs = {"baseline(w=0.03)": {"w_tn": 0.03}}
    base_results = run_patterns(all_races, base_configs, make_baseline_composite)
    base_kpi = base_results["baseline(w=0.03)"]

    configs = {}
    alpha_vals = [0.000, 0.002, 0.004, 0.006, 0.008, 0.010]
    for au in alpha_vals:
        for ad in alpha_vals:
            name = f"asym(up={au:.3f},dn={ad:.3f})"
            configs[name] = {"alpha_up": au, "alpha_down": ad}

    results = run_patterns(all_races, configs, make_asymmetric_composite)

    lines.append(f"{'上方α':>8} {'下方α':>8} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7} {'vs現行':>8}")
    lines.append("-" * 80)

    best = ("", 0.0)
    best_roi = ("", 0.0)
    section_results = {}

    for au in alpha_vals:
        for ad in alpha_vals:
            name = f"asym(up={au:.3f},dn={ad:.3f})"
            kpi = results[name]
            diff = kpi.placed_rate - base_kpi.placed_rate
            lines.append(f"{au:>8.3f} {ad:>8.3f} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}% {diff:>+7.1f}pp")
            section_results[name] = kpi.to_dict()
            if kpi.placed_rate > best[1]:
                best = (name, kpi.placed_rate)
            if kpi.tansho_roi > best_roi[1]:
                best_roi = (name, kpi.tansho_roi)

    lines.append(f"\n>>> 複勝率ベスト: {best[0]} ({best[1]:.1f}%)")
    lines.append(f">>> 単ROIベスト:  {best_roi[0]} ({best_roi[1]:.1f}%)")
    lines.append("")

    json_out["section_c"] = {
        "baseline": base_kpi.to_dict(),
        "patterns": section_results,
        "best_placed": best[0],
        "best_roi": best_roi[0],
    }


def section_d(all_races, lines, json_out):
    """D. 乗算対象の組み合わせ"""
    lines.append("=" * 100)
    lines.append("セクション D: 乗算対象の組み合わせ")
    lines.append("=" * 100)

    base_configs = {"baseline(w=0.03)": {"w_tn": 0.03}}
    base_results = run_patterns(all_races, base_configs, make_baseline_composite)
    base_kpi = base_results["baseline(w=0.03)"]

    # 複数のターゲット組み合わせ × alpha
    target_sets = {
        "能力のみ": ["ability"],
        "能力+展開": ["ability", "pace"],
        "能力+展開+適性": ["ability", "pace", "course"],
        "能力+騎手": ["ability", "jockey"],
        "全因子": ["ability", "pace", "course", "jockey"],
    }
    alpha_vals = [0.003, 0.005, 0.007, 0.010]

    configs = {}
    for tname, targets in target_sets.items():
        for alpha in alpha_vals:
            name = f"{tname}(α={alpha})"
            configs[name] = {"alpha": alpha, "targets": targets}

    results = run_patterns(all_races, configs, make_target_composite)

    lines.append(f"{'パターン':<24} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7} {'vs現行':>8}")
    lines.append("-" * 85)
    lines.append(f"{'baseline(w=0.03)':<24} {base_kpi.win_rate:>6.1f}% {base_kpi.place2_rate:>6.1f}% {base_kpi.placed_rate:>6.1f}% {base_kpi.tansho_roi:>6.1f}% {base_kpi.fukusho_roi:>6.1f}% {'---':>8}")

    best = ("", 0.0)
    section_results = {}

    for tname in target_sets:
        for alpha in alpha_vals:
            name = f"{tname}(α={alpha})"
            kpi = results[name]
            diff = kpi.placed_rate - base_kpi.placed_rate
            lines.append(f"{name:<24} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}% {diff:>+7.1f}pp")
            section_results[name] = kpi.to_dict()
            if kpi.placed_rate > best[1]:
                best = (name, kpi.placed_rate)

    lines.append(f"\n>>> ベスト: {best[0]} ({best[1]:.1f}%)")
    lines.append("")

    json_out["section_d"] = {
        "baseline": base_kpi.to_dict(),
        "patterns": section_results,
        "best": best[0],
    }


def section_e(all_races, lines, json_out):
    """E. 条件別効果分析（最重要）"""
    lines.append("=" * 100)
    lines.append("セクション E: 条件別効果分析")
    lines.append("=" * 100)

    # テストするパターン（baselineとD系列の代表値）
    configs_base = {"baseline(w=0.03)": {"w_tn": 0.03}}
    configs_d = {}
    for alpha in [0.003, 0.005, 0.007, 0.010]:
        configs_d[f"D(α={alpha})"] = {"alpha": alpha}

    # 条件フィルタ
    filters = {
        "JRA": lambda r: r["is_jra"],
        "NAR": lambda r: not r["is_jra"],
        "重賞": lambda r: r["grade"] in ("G1", "G2", "G3", "Jpn1", "Jpn2", "Jpn3"),
        "OP/L": lambda r: r["grade"] in ("OP", "L", "オープン", "リステッド"),
        "条件戦": lambda r: r["grade"] in ("", "1勝", "2勝", "3勝", "500万", "1000万", "1600万",
                                            "C1", "C2", "C3", "B1", "B2", "B3", "A1", "A2"),
        "短距離(〜1200m)": lambda r: r["dist_cat"] == "短距離",
        "マイル(〜1600m)": lambda r: r["dist_cat"] == "マイル",
        "中距離(〜2200m)": lambda r: r["dist_cat"] == "中距離",
        "長距離(2400m〜)": lambda r: r["dist_cat"] == "長距離",
    }

    section_json = {}

    for cond_name, filt in filters.items():
        lines.append(f"\n--- [{cond_name}] ---")

        # 該当レース数カウント
        n_races = sum(1 for r in all_races if filt(r))
        lines.append(f"対象レース数: {n_races}")

        if n_races < 50:
            lines.append("(サンプル不足、スキップ)")
            continue

        lines.append(f"{'パターン':<20} {'◎件数':>6} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7}")
        lines.append("-" * 80)

        # baseline
        br = run_patterns(all_races, configs_base, make_baseline_composite, filter_fn=filt)
        bkpi = br["baseline(w=0.03)"]
        lines.append(f"{'baseline(w=0.03)':<20} {bkpi.total:>6} {bkpi.win_rate:>6.1f}% {bkpi.place2_rate:>6.1f}% {bkpi.placed_rate:>6.1f}% {bkpi.tansho_roi:>6.1f}% {bkpi.fukusho_roi:>6.1f}%")

        # D系列
        dr = run_patterns(all_races, configs_d, make_d_composite, filter_fn=filt)
        cond_results = {"baseline": bkpi.to_dict()}
        best_cond = ("baseline", bkpi.placed_rate)

        for dname in configs_d:
            kpi = dr[dname]
            diff = kpi.placed_rate - bkpi.placed_rate
            lines.append(f"{dname:<20} {kpi.total:>6} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}% {diff:>+.1f}pp")
            cond_results[dname] = kpi.to_dict()
            if kpi.placed_rate > best_cond[1]:
                best_cond = (dname, kpi.placed_rate)

        lines.append(f">>> {cond_name}ベスト: {best_cond[0]}")
        section_json[cond_name] = {
            "n_races": n_races,
            "patterns": cond_results,
            "best": best_cond[0],
        }

    # 人気別分析
    lines.append(f"\n--- [人気別◎成績] ---")
    # 全パターンでの人気別成績
    pop_configs = {"baseline(w=0.03)": {"w_tn": 0.03}}
    pop_configs.update(configs_d)

    pop_filters = {
        "1-3人気": lambda r: True,  # 全レース対象（人気は馬レベルなのでrun_patterns_all_marksで集計）
    }

    # 人気別集計用のrun_patterns_all_marks
    full_results = run_patterns_all_marks(
        all_races,
        {"baseline(w=0.03)": {"w_tn": 0.03}},
        make_baseline_composite,
    )
    d_full = run_patterns_all_marks(all_races, configs_d, make_d_composite)
    full_results.update(d_full)

    for pop_cat in ["1-3人気", "4-6人気", "7人気以下"]:
        lines.append(f"\n  [{pop_cat}]")
        lines.append(f"  {'パターン':<20} {'件数':>6} {'勝率':>7} {'連対':>7} {'複勝':>7} {'単ROI':>7} {'複ROI':>7}")
        for pname in ["baseline(w=0.03)"] + list(configs_d.keys()):
            kpi = full_results[pname]["by_pop"][pop_cat]
            if kpi.total == 0:
                continue
            lines.append(f"  {pname:<20} {kpi.total:>6} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}%")
        section_json[f"pop_{pop_cat}"] = {
            pname: full_results[pname]["by_pop"][pop_cat].to_dict()
            for pname in full_results
        }

    # 自信度別
    lines.append(f"\n  [自信度別◎成績]")
    for conf in ["SS", "S", "A", "B", "C", "D", "E"]:
        has_data = False
        for pname in full_results:
            if full_results[pname]["by_conf"][conf].total > 0:
                has_data = True
                break
        if not has_data:
            continue

        lines.append(f"\n  自信度={conf}")
        lines.append(f"  {'パターン':<20} {'件数':>6} {'勝率':>7} {'連対':>7} {'複勝':>7} {'単ROI':>7} {'複ROI':>7}")
        for pname in ["baseline(w=0.03)"] + list(configs_d.keys()):
            kpi = full_results[pname]["by_conf"][conf]
            if kpi.total == 0:
                continue
            lines.append(f"  {pname:<20} {kpi.total:>6} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}%")

    lines.append("")
    json_out["section_e"] = section_json


def section_f(all_races, lines, json_out):
    """F. 能力レベルとの交互作用"""
    lines.append("=" * 100)
    lines.append("セクション F: 能力レベルとの交互作用")
    lines.append("=" * 100)

    # まず全馬のability分布を把握
    all_abilities = []
    for race in all_races:
        for hd in race["horses"]:
            all_abilities.append(hd["ability"])

    all_abilities.sort()
    n = len(all_abilities)
    q25 = all_abilities[n // 4]
    q50 = all_abilities[n // 2]
    q75 = all_abilities[3 * n // 4]

    lines.append(f"能力偏差値分布: N={n}, Q25={q25:.1f}, Q50={q50:.1f}, Q75={q75:.1f}")
    lines.append("")

    # 四分位別のフィルタ（レース内の◎馬の能力で判定）
    # ここではレース単位ではなく、全レースに対して実行し、
    # ◎の能力レベル別に事後集計する
    base_configs = {"baseline(w=0.03)": {"w_tn": 0.03}}
    d_configs = {}
    for alpha in [0.003, 0.005, 0.007, 0.010]:
        d_configs[f"D(α={alpha})"] = {"alpha": alpha}

    # 四分位別集計
    quartile_stats = {}
    for qname, (lo, hi) in [("Q1(低能力)", (0, q25)), ("Q2", (q25, q50)),
                              ("Q3", (q50, q75)), ("Q4(高能力)", (q75, 999))]:
        quartile_stats[qname] = {pname: KPIAccumulator()
                                  for pname in ["baseline(w=0.03)"] + list(d_configs.keys())}

    for race in all_races:
        horses = race["horses"]
        finish_map = race["finish_map"]
        payouts = race["payouts"]
        weights = race["weights"]

        # baseline
        composites_base = []
        for hd in horses:
            c = make_baseline_composite(hd, weights, {"w_tn": 0.03})
            composites_base.append((hd["horse_no"], c))
        mark_base = assign_marks(composites_base)
        honmei_base = None
        for hno, mk in mark_base.items():
            if mk == "◎":
                honmei_base = hno
                break

        # ◎の能力値でどの四分位か判定
        if honmei_base is not None:
            honmei_ability = None
            for hd in horses:
                if hd["horse_no"] == honmei_base:
                    honmei_ability = hd["ability"]
                    break

            if honmei_ability is not None:
                if honmei_ability < q25:
                    qname = "Q1(低能力)"
                elif honmei_ability < q50:
                    qname = "Q2"
                elif honmei_ability < q75:
                    qname = "Q3"
                else:
                    qname = "Q4(高能力)"

                pos = finish_map.get(honmei_base, 99)
                tansho = get_tansho_payout(payouts, honmei_base)
                fukusho = get_fukusho_payout(payouts, honmei_base)
                quartile_stats[qname]["baseline(w=0.03)"].add(pos, tansho, fukusho)

        # D系列
        for dname, dcfg in d_configs.items():
            composites_d = []
            for hd in horses:
                c = make_d_composite(hd, weights, dcfg)
                composites_d.append((hd["horse_no"], c))
            mark_d = assign_marks(composites_d)
            honmei_d = None
            for hno, mk in mark_d.items():
                if mk == "◎":
                    honmei_d = hno
                    break

            if honmei_d is not None:
                honmei_ability = None
                for hd in horses:
                    if hd["horse_no"] == honmei_d:
                        honmei_ability = hd["ability"]
                        break

                if honmei_ability is not None:
                    if honmei_ability < q25:
                        qn = "Q1(低能力)"
                    elif honmei_ability < q50:
                        qn = "Q2"
                    elif honmei_ability < q75:
                        qn = "Q3"
                    else:
                        qn = "Q4(高能力)"

                    pos = finish_map.get(honmei_d, 99)
                    tansho = get_tansho_payout(payouts, honmei_d)
                    fukusho = get_fukusho_payout(payouts, honmei_d)
                    quartile_stats[qn][dname].add(pos, tansho, fukusho)

    section_json = {}
    for qname in ["Q1(低能力)", "Q2", "Q3", "Q4(高能力)"]:
        lines.append(f"\n--- [{qname}] ---")
        lines.append(f"{'パターン':<20} {'件数':>6} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7}")
        lines.append("-" * 75)

        qj = {}
        for pname in ["baseline(w=0.03)"] + list(d_configs.keys()):
            kpi = quartile_stats[qname][pname]
            if kpi.total == 0:
                continue
            lines.append(f"{pname:<20} {kpi.total:>6} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}%")
            qj[pname] = kpi.to_dict()
        section_json[qname] = qj

    # 動的α
    lines.append("\n--- 動的α (α = base_α * ability/50) ---")
    dyn_configs = {}
    for ba in [0.003, 0.005, 0.007, 0.010]:
        dyn_configs[f"dynα(base={ba})"] = {"base_alpha": ba}

    dyn_results = run_patterns(all_races, dyn_configs, make_dynamic_alpha_composite)
    base_r = run_patterns(all_races, base_configs, make_baseline_composite)
    base_kpi = base_r["baseline(w=0.03)"]

    lines.append(f"{'パターン':<20} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7} {'vs現行':>8}")
    for name in dyn_configs:
        kpi = dyn_results[name]
        diff = kpi.placed_rate - base_kpi.placed_rate
        lines.append(f"{name:<20} {kpi.win_rate:>6.1f}% {kpi.place2_rate:>6.1f}% {kpi.placed_rate:>6.1f}% {kpi.tansho_roi:>6.1f}% {kpi.fukusho_roi:>6.1f}% {diff:>+7.1f}pp")
        section_json[name] = kpi.to_dict()

    lines.append("")
    json_out["section_f"] = section_json


def section_g(all_races, lines, json_out):
    """G. 生データ相関分析"""
    lines.append("=" * 100)
    lines.append("セクション G: training_dev と着順の生データ相関分析")
    lines.append("=" * 100)

    # 全馬のtraining_dev → finish のデータ収集
    all_td_finish = []  # (training_dev, finish, is_jra, dist_cat, grade_cat, ability)
    for race in all_races:
        is_jra = race["is_jra"]
        dist_cat = race["dist_cat"]
        grade = race["grade"]
        # グレードカテゴリ
        if grade in ("G1", "G2", "G3", "Jpn1", "Jpn2", "Jpn3"):
            grade_cat = "重賞"
        elif grade in ("OP", "L", "オープン", "リステッド"):
            grade_cat = "OP/L"
        else:
            grade_cat = "条件戦"

        for hd in race["horses"]:
            hno = hd["horse_no"]
            td = hd["training_dev"]
            finish = race["finish_map"].get(hno)
            if finish is None:
                continue
            all_td_finish.append({
                "td": td,
                "finish": finish,
                "is_placed": 1 if finish <= 3 else 0,
                "is_jra": is_jra,
                "dist_cat": dist_cat,
                "grade_cat": grade_cat,
                "ability": hd["ability"],
            })

    n_total = len(all_td_finish)
    lines.append(f"データ数: {n_total:,}")

    # training_dev帯別の3着内率
    lines.append("\n--- training_dev帯別 3着内率 ---")
    bands = [(0, 35), (35, 40), (40, 45), (45, 48), (48, 50),
             (50, 52), (52, 55), (55, 60), (60, 65), (65, 100)]

    lines.append(f"{'帯':>12} {'件数':>7} {'3着内率':>8} {'平均着順':>8}")
    lines.append("-" * 40)

    band_json = {}
    for lo, hi in bands:
        entries = [e for e in all_td_finish if lo <= e["td"] < hi]
        if not entries:
            continue
        n = len(entries)
        placed_rate = sum(e["is_placed"] for e in entries) / n * 100
        avg_finish = sum(e["finish"] for e in entries) / n
        band_name = f"{lo}-{hi}"
        lines.append(f"{band_name:>12} {n:>7} {placed_rate:>7.1f}% {avg_finish:>7.2f}")
        band_json[band_name] = {"n": n, "placed_rate": round(placed_rate, 1), "avg_finish": round(avg_finish, 2)}

    # 相関係数（ピアソン）
    td_vals = [e["td"] for e in all_td_finish]
    fin_vals = [e["finish"] for e in all_td_finish]
    placed_vals = [e["is_placed"] for e in all_td_finish]

    def pearson(x, y):
        n = len(x)
        if n < 2:
            return 0.0
        mx = sum(x) / n
        my = sum(y) / n
        sxy = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
        sx = (sum((xi - mx) ** 2 for xi in x)) ** 0.5
        sy = (sum((yi - my) ** 2 for yi in y)) ** 0.5
        if sx == 0 or sy == 0:
            return 0.0
        return sxy / (sx * sy)

    corr_finish = pearson(td_vals, fin_vals)
    corr_placed = pearson(td_vals, placed_vals)

    lines.append(f"\nピアソン相関: training_dev vs 着順 = {corr_finish:.4f}")
    lines.append(f"ピアソン相関: training_dev vs 3着内 = {corr_placed:.4f}")

    # 条件別相関
    lines.append("\n--- 条件別 training_dev vs 3着内 相関 ---")
    lines.append(f"{'条件':<16} {'件数':>7} {'相関':>8}")

    cond_corrs = {}
    for label, filt in [
        ("JRA", lambda e: e["is_jra"]),
        ("NAR", lambda e: not e["is_jra"]),
        ("重賞", lambda e: e["grade_cat"] == "重賞"),
        ("OP/L", lambda e: e["grade_cat"] == "OP/L"),
        ("条件戦", lambda e: e["grade_cat"] == "条件戦"),
        ("短距離", lambda e: e["dist_cat"] == "短距離"),
        ("マイル", lambda e: e["dist_cat"] == "マイル"),
        ("中距離", lambda e: e["dist_cat"] == "中距離"),
        ("長距離", lambda e: e["dist_cat"] == "長距離"),
    ]:
        subset = [e for e in all_td_finish if filt(e)]
        if len(subset) < 50:
            continue
        td_s = [e["td"] for e in subset]
        pl_s = [e["is_placed"] for e in subset]
        c = pearson(td_s, pl_s)
        lines.append(f"{label:<16} {len(subset):>7} {c:>+8.4f}")
        cond_corrs[label] = {"n": len(subset), "corr": round(c, 4)}

    # 能力四分位別相関
    all_abilities = sorted([e["ability"] for e in all_td_finish])
    n = len(all_abilities)
    q25 = all_abilities[n // 4]
    q50 = all_abilities[n // 2]
    q75 = all_abilities[3 * n // 4]

    lines.append("\n--- 能力四分位別 training_dev vs 3着内 相関 ---")
    ability_corrs = {}
    for qname, (lo, hi) in [("Q1(低能力)", (0, q25)), ("Q2", (q25, q50)),
                              ("Q3", (q50, q75)), ("Q4(高能力)", (q75, 999))]:
        subset = [e for e in all_td_finish if lo <= e["ability"] < hi]
        if len(subset) < 50:
            continue
        td_s = [e["td"] for e in subset]
        pl_s = [e["is_placed"] for e in subset]
        c = pearson(td_s, pl_s)
        lines.append(f"{qname:<16} {len(subset):>7} {c:>+8.4f}")
        ability_corrs[qname] = {"n": len(subset), "corr": round(c, 4)}

    lines.append("")
    json_out["section_g"] = {
        "n_total": n_total,
        "bands": band_json,
        "corr_finish": round(corr_finish, 4),
        "corr_placed": round(corr_placed, 4),
        "cond_corrs": cond_corrs,
        "ability_corrs": ability_corrs,
    }


# ============================================================
# 総合推奨
# ============================================================

def overall_recommendation(json_out, lines):
    """全セクションの結果から総合推奨を出力"""
    lines.append("=" * 100)
    lines.append("総合推奨")
    lines.append("=" * 100)

    # 全パターンのKPIを集約
    all_patterns = []

    def collect(section_name, patterns_dict):
        if not patterns_dict:
            return
        for pname, kpi in patterns_dict.items():
            if isinstance(kpi, dict) and "placed_rate" in kpi:
                all_patterns.append({
                    "section": section_name,
                    "name": pname,
                    "placed_rate": kpi["placed_rate"],
                    "tansho_roi": kpi["tansho_roi"],
                    "win_rate": kpi.get("win_rate", 0),
                    "fukusho_roi": kpi.get("fukusho_roi", 0),
                    "total": kpi.get("total", 0),
                })

    for sec in ["section_a", "section_b", "section_c", "section_d"]:
        if sec in json_out:
            collect(sec, json_out[sec].get("patterns", {}))

    if not all_patterns:
        lines.append("集計可能なパターンなし")
        return

    # baseline
    base = None
    if "section_a" in json_out:
        base = json_out["section_a"].get("baseline")

    if base:
        lines.append(f"\n現行baseline: 複勝率={base['placed_rate']:.1f}%, 単ROI={base['tansho_roi']:.1f}%")

    # 複勝率TOP5
    lines.append("\n--- 複勝率 TOP5 ---")
    by_placed = sorted(all_patterns, key=lambda x: -x["placed_rate"])[:5]
    for i, p in enumerate(by_placed, 1):
        lines.append(f"  {i}. [{p['section']}] {p['name']}: 複勝率={p['placed_rate']:.1f}%, 単ROI={p['tansho_roi']:.1f}%, 複ROI={p['fukusho_roi']:.1f}%")

    # 単ROI TOP5
    lines.append("\n--- 単勝ROI TOP5 ---")
    by_roi = sorted(all_patterns, key=lambda x: -x["tansho_roi"])[:5]
    for i, p in enumerate(by_roi, 1):
        lines.append(f"  {i}. [{p['section']}] {p['name']}: 単ROI={p['tansho_roi']:.1f}%, 複勝率={p['placed_rate']:.1f}%")

    # バランス（複勝率 × 単ROI のスコア）TOP5
    lines.append("\n--- バランス (複勝率 x 単ROI) TOP5 ---")
    for p in all_patterns:
        p["balance"] = p["placed_rate"] * p["tansho_roi"] / 100.0
    by_balance = sorted(all_patterns, key=lambda x: -x["balance"])[:5]
    for i, p in enumerate(by_balance, 1):
        lines.append(f"  {i}. [{p['section']}] {p['name']}: バランス={p['balance']:.1f}, 複勝率={p['placed_rate']:.1f}%, 単ROI={p['tansho_roi']:.1f}%")

    # 条件別推奨
    if "section_e" in json_out:
        lines.append("\n--- 条件別推奨 ---")
        for cond_name, cond_data in json_out["section_e"].items():
            if isinstance(cond_data, dict) and "best" in cond_data:
                lines.append(f"  {cond_name}: {cond_data['best']}")

    lines.append("")


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="調教偏差値 深掘りバックテスト")
    parser.add_argument("--year", type=str, default="", help="対象年 (例: 2026)")
    parser.add_argument("--after", type=str, default="", help="指定日以降 (例: 2026-03-01)")
    args = parser.parse_args()

    console.print("[bold]調教偏差値 composite 深掘りバックテスト[/bold]")

    dates = get_available_dates(args.year, args.after)
    console.print(f"対象候補日数: {len(dates)}")

    if not dates:
        console.print("[red]対象データなし[/red]")
        return

    # データを1回だけ読み込み
    all_races = load_all_race_data(dates)
    console.print(f"[green]読み込み完了: {len(all_races):,} レース[/green]")

    if not all_races:
        console.print("[red]training_devが存在するレースなし[/red]")
        return

    lines = []
    lines.append("=" * 100)
    lines.append("調教偏差値 composite 深掘りバックテスト結果")
    lines.append(f"対象レース数: {len(all_races):,}")
    lines.append("=" * 100)
    lines.append("")

    json_out = {"total_races": len(all_races)}

    # 各セクション実行（進捗表示）
    sections = [
        ("A: 細粒度パラメータサーチ", section_a),
        ("B: 非線形モデル", section_b),
        ("C: 非対称効果", section_c),
        ("D: 乗算対象の組み合わせ", section_d),
        ("E: 条件別効果分析", section_e),
        ("F: 能力レベルとの交互作用", section_f),
        ("G: 生データ相関分析", section_g),
    ]

    with Progress(
        TextColumn("[bold green]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("セクション実行中", total=len(sections))

        for sec_name, sec_fn in sections:
            progress.update(task, description=f"[bold green]{sec_name}")
            console.print(f"\n[bold cyan]>>> {sec_name}[/bold cyan]")
            sec_fn(all_races, lines, json_out)
            progress.advance(task)

    # 総合推奨
    overall_recommendation(json_out, lines)

    # 出力
    summary = "\n".join(lines)
    console.print(summary)

    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write(summary)
    console.print(f"\n[green]テキストサマリー保存: {OUTPUT_TXT}[/green]")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(json_out, f, ensure_ascii=False, indent=2)
    console.print(f"[green]JSON結果保存: {OUTPUT_JSON}[/green]")


if __name__ == "__main__":
    main()
