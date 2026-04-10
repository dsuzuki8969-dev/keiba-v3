"""
調教偏差値の composite 組み込み方法バックテスト

加算モデル（現行） vs 乗算モデル vs ハイブリッドモデルを
既存の予測JSON + 結果JSONで網羅的に比較する。

使い方:
  python scripts/backtest_training_model.py
  python scripts/backtest_training_model.py --year 2026
  python scripts/backtest_training_model.py --after 2026-03-01
"""
import argparse
import io
import json
import os
import sys
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
from config.settings import PREDICTIONS_DIR

PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
OUTPUT_JSON = os.path.join(PROJECT_ROOT, "data", "backtest_training_results.json")
OUTPUT_TXT = os.path.join(PROJECT_ROOT, "data", "backtest_training_summary.txt")

# ============================================================
# 較正済み重みの読み込み（settings.pyのロジックを再現）
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
    """較正済み重みファイルを読み込む"""
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
    """競馬場名に応じた重みを返す（training除く6因子）"""
    if venue_name and venue_name in _CALIB_WEIGHTS:
        return _CALIB_WEIGHTS[venue_name]
    if venue_name and venue_name in VENUE_COMPOSITE_WEIGHTS:
        return VENUE_COMPOSITE_WEIGHTS[venue_name]
    return COMPOSITE_WEIGHTS


# ============================================================
# パターン定義
# ============================================================

# パターンA: 現行 (w_tn=0.03)
# パターンB: 加算モデル（w_tn を変更）
# パターンC: 乗算モデル（能力のみ）
# パターンD: 乗算モデル（能力+展開）
# パターンE: ハイブリッド（乗算+加算）

PATTERNS = {}

# A: 現行
PATTERNS["A(現行w=0.03)"] = {"type": "additive", "w_tn": 0.03}

# B: 加算モデル改良
for label, wtn in [("B1(w=0.00)", 0.00), ("B2(w=0.02)", 0.02),
                    ("B3(w=0.03)", 0.03), ("B4(w=0.05)", 0.05),
                    ("B5(w=0.07)", 0.07), ("B6(w=0.10)", 0.10)]:
    PATTERNS[label] = {"type": "additive", "w_tn": wtn}

# C: 乗算モデル（能力のみ）
for label, alpha in [("C1(α=0.001)", 0.001), ("C2(α=0.002)", 0.002),
                      ("C3(α=0.003)", 0.003), ("C4(α=0.005)", 0.005),
                      ("C5(α=0.007)", 0.007), ("C6(α=0.010)", 0.010)]:
    PATTERNS[label] = {"type": "mult_ability", "alpha": alpha}

# D: 乗算モデル（能力+展開）
for label, alpha in [("D1(α=0.001)", 0.001), ("D2(α=0.002)", 0.002),
                      ("D3(α=0.003)", 0.003), ("D4(α=0.005)", 0.005),
                      ("D5(α=0.007)", 0.007), ("D6(α=0.010)", 0.010)]:
    PATTERNS[label] = {"type": "mult_ability_pace", "alpha": alpha}

# E: ハイブリッド（乗算+加算）
for label, alpha, wtn in [
    ("E1(α=0.002,w=0.01)", 0.002, 0.01),
    ("E2(α=0.003,w=0.01)", 0.003, 0.01),
    ("E3(α=0.002,w=0.02)", 0.002, 0.02),
    ("E4(α=0.003,w=0.02)", 0.003, 0.02),
    ("E5(α=0.005,w=0.01)", 0.005, 0.01),
    ("E6(α=0.005,w=0.02)", 0.005, 0.02),
]:
    PATTERNS[label] = {"type": "hybrid", "alpha": alpha, "w_tn": wtn}


def calc_composite(
    pattern: dict,
    ability: float,
    pace: float,
    course: float,
    jockey_dev: float,
    trainer_dev: float,
    bloodline_dev: float,
    training_dev: float,
    ml_adj: float,
    odds_adj: float,
    weights: dict,
) -> float:
    """
    パターンに基づいてcompositeを再計算する。

    注意: 馬体重補正・market_anchor_adjは予測JSONに保存されていないため、
    全パターンで同一の未知量として扱う。
    → 現行compositeから既知の成分を引いて「残差」(= weight_adj + market_anchor_adj)を求め、
      全パターンに同一の残差を加算する方式は使わない。
    → 代わりに、6因子加重平均 + training項 + ml_adj + odds_adj のみで計算し、
      全パターン間の「差分」を比較する（絶対値でなく順位が重要）。
    """
    w_ab = weights.get("ability", 0.31)
    w_pa = weights.get("pace", 0.29)
    w_co = weights.get("course", 0.06)
    w_jk = weights.get("jockey", 0.13)
    w_tr = weights.get("trainer", 0.13)
    w_bl = weights.get("bloodline", 0.05)

    ptype = pattern["type"]

    if ptype == "additive":
        w_tn = pattern["w_tn"]
        v = (ability * w_ab + pace * w_pa + course * w_co
             + jockey_dev * w_jk + trainer_dev * w_tr
             + bloodline_dev * w_bl + training_dev * w_tn)

    elif ptype == "mult_ability":
        alpha = pattern["alpha"]
        coeff = 1.0 + (training_dev - 50.0) * alpha
        eff_ability = ability * coeff
        v = (eff_ability * w_ab + pace * w_pa + course * w_co
             + jockey_dev * w_jk + trainer_dev * w_tr
             + bloodline_dev * w_bl)
        # training加算なし

    elif ptype == "mult_ability_pace":
        alpha = pattern["alpha"]
        coeff = 1.0 + (training_dev - 50.0) * alpha
        eff_ability = ability * coeff
        eff_pace = pace * coeff
        v = (eff_ability * w_ab + eff_pace * w_pa + course * w_co
             + jockey_dev * w_jk + trainer_dev * w_tr
             + bloodline_dev * w_bl)

    elif ptype == "hybrid":
        alpha = pattern["alpha"]
        w_tn = pattern["w_tn"]
        coeff = 1.0 + (training_dev - 50.0) * alpha
        eff_ability = ability * coeff
        v = (eff_ability * w_ab + pace * w_pa + course * w_co
             + jockey_dev * w_jk + trainer_dev * w_tr
             + bloodline_dev * w_bl + training_dev * w_tn)

    else:
        raise ValueError(f"不明なパターンタイプ: {ptype}")

    v += ml_adj + odds_adj
    return max(20.0, min(100.0, v))


# ============================================================
# データ読み込み
# ============================================================

def load_date_data(date_str: str) -> Optional[Tuple[dict, dict]]:
    """日付のpred + resultsを読み込み。なければNone"""
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
    """利用可能な日付一覧（pred + results の両方がある日付）"""
    dates = []
    for fn in os.listdir(PREDICTIONS_DIR):
        if not fn.endswith("_pred.json") or "_backup" in fn:
            continue
        date_str = fn.replace("_pred.json", "")
        if len(date_str) != 8:
            continue
        # フィルタ
        if year_filter and not date_str.startswith(year_filter):
            continue
        date_hyphen = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        if after_filter and date_hyphen < after_filter:
            continue
        # 結果ファイルも必要
        result_path = os.path.join(RESULTS_DIR, f"{date_str}_results.json")
        if os.path.exists(result_path):
            dates.append(date_str)
    dates.sort()
    return dates


# ============================================================
# 印割当て（composite順で上位5頭）
# ============================================================

MARKS = ["◎", "○", "▲", "△", "★"]


def assign_marks(horses_with_composite: List[Tuple[int, float]]) -> Dict[int, str]:
    """composite降順で上位5頭に印を割当て。返値: {horse_no: mark}"""
    sorted_horses = sorted(horses_with_composite, key=lambda x: -x[1])
    result = {}
    for i, (hno, _) in enumerate(sorted_horses[:5]):
        result[hno] = MARKS[i]
    return result


# ============================================================
# 単勝払戻金の取得
# ============================================================

def _extract_payout(payouts: dict, bet_type: str, horse_no: int) -> int:
    """
    払戻金を取得する汎用関数。

    payoutsの形式:
    - 形式1: {"combo": "8", "payout": 790}  (単勝: dict)
    - 形式2: [{"combo": "8", "payout": 190}, ...]  (複勝: list of dict)
    - 形式3: [{"horse_no": 8, "payout": 790}, ...]  (旧形式)
    - 形式4: {"horse_no": 8, "payout": 790}  (旧形式 dict)
    """
    data = payouts.get(bet_type)
    if not data:
        return 0

    hno_str = str(horse_no)

    def _match_entry(entry: dict) -> int:
        # combo形式: {"combo": "8", "payout": 790}
        combo = entry.get("combo")
        if combo is not None and str(combo) == hno_str:
            return int(entry.get("payout", 0) or 0)
        # horse_no形式: {"horse_no": 8, "payout": 790}
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
    """payoutsから単勝払戻金を取得（100円あたり）"""
    return _extract_payout(payouts, "単勝", horse_no) or _extract_payout(payouts, "tansho", horse_no)


def get_fukusho_payout(payouts: dict, horse_no: int) -> int:
    """payoutsから複勝払戻金を取得（100円あたり）"""
    return _extract_payout(payouts, "複勝", horse_no) or _extract_payout(payouts, "fukusho", horse_no)


# ============================================================
# メイン集計
# ============================================================

def run_backtest(dates: List[str]):
    """全パターンのバックテストを実行"""
    pattern_names = list(PATTERNS.keys())

    # 各パターンの集計用辞書
    stats = {}
    for pname in pattern_names:
        stats[pname] = {
            "honmei_total": 0, "honmei_win": 0, "honmei_place2": 0, "honmei_placed": 0,
            "honmei_tansho_stake": 0, "honmei_tansho_ret": 0,
            "honmei_fukusho_stake": 0, "honmei_fukusho_ret": 0,
            "top3_in_top3": 0,  # ◎○▲の3頭が全て3着以内
            "top3_races": 0,    # 3頭以上出走のレース数
            "pop1_match": 0,    # ◎と1番人気の一致
            "pop1_total": 0,    # 1番人気が判明したレース数
            # 印別
            "by_mark": {m: {"total": 0, "win": 0, "place2": 0, "placed": 0,
                            "tansho_stake": 0, "tansho_ret": 0,
                            "fukusho_stake": 0, "fukusho_ret": 0}
                        for m in MARKS},
            # 自信度別◎成績
            "by_conf": defaultdict(lambda: {
                "total": 0, "win": 0, "place2": 0, "placed": 0,
                "tansho_stake": 0, "tansho_ret": 0,
            }),
        }

    total_races = 0
    skipped_no_training = 0
    dates_with_training = 0
    dates_without_training = 0

    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("バックテスト実行中", total=len(dates))

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
                dates_without_training += 1
                progress.advance(task)
                continue

            dates_with_training += 1

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

                payouts = result.get("payouts", {})
                confidence = race.get("confidence", "B")
                venue = race.get("venue", "")
                horses = race.get("horses", [])

                if len(horses) < 3:
                    continue

                # 馬のデータ抽出（全パターン共通）
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

                weights = get_weights(venue)
                total_races += 1

                # 各パターンでcomposite再計算 → 印割当て → 成績集計
                for pname, pattern in PATTERNS.items():
                    st = stats[pname]

                    # composite再計算
                    composites = []
                    for hd in horse_data:
                        c = calc_composite(
                            pattern,
                            hd["ability"], hd["pace"], hd["course"],
                            hd["jockey_dev"], hd["trainer_dev"],
                            hd["bloodline_dev"], hd["training_dev"],
                            hd["ml_adj"], hd["odds_adj"],
                            weights,
                        )
                        composites.append((hd["horse_no"], c))

                    # 印割当て
                    mark_map = assign_marks(composites)

                    # ◎の馬番
                    honmei_hno = None
                    for hno, mk in mark_map.items():
                        if mk == "◎":
                            honmei_hno = hno
                            break

                    # 各印の成績集計
                    for hno, mk in mark_map.items():
                        pos = finish_map.get(hno, 99)
                        ms = st["by_mark"][mk]
                        ms["total"] += 1
                        if pos == 1:
                            ms["win"] += 1
                        if pos <= 2:
                            ms["place2"] += 1
                        if pos <= 3:
                            ms["placed"] += 1
                        ms["tansho_stake"] += 100
                        if pos == 1:
                            ms["tansho_ret"] += get_tansho_payout(payouts, hno)
                        ms["fukusho_stake"] += 100
                        if pos <= 3:
                            ms["fukusho_ret"] += get_fukusho_payout(payouts, hno)

                    # ◎の成績
                    if honmei_hno is not None:
                        pos = finish_map.get(honmei_hno, 99)
                        st["honmei_total"] += 1
                        if pos == 1:
                            st["honmei_win"] += 1
                        if pos <= 2:
                            st["honmei_place2"] += 1
                        if pos <= 3:
                            st["honmei_placed"] += 1
                        st["honmei_tansho_stake"] += 100
                        if pos == 1:
                            st["honmei_tansho_ret"] += get_tansho_payout(payouts, honmei_hno)
                        st["honmei_fukusho_stake"] += 100
                        if pos <= 3:
                            st["honmei_fukusho_ret"] += get_fukusho_payout(payouts, honmei_hno)

                        # 1番人気との一致
                        pop1_hno = None
                        for hd in horse_data:
                            if hd["popularity"] == 1:
                                pop1_hno = hd["horse_no"]
                                break
                        if pop1_hno is not None:
                            st["pop1_total"] += 1
                            if honmei_hno == pop1_hno:
                                st["pop1_match"] += 1

                        # 自信度別◎成績
                        cs = st["by_conf"][confidence]
                        cs["total"] += 1
                        if pos == 1:
                            cs["win"] += 1
                        if pos <= 2:
                            cs["place2"] += 1
                        if pos <= 3:
                            cs["placed"] += 1
                        cs["tansho_stake"] += 100
                        if pos == 1:
                            cs["tansho_ret"] += get_tansho_payout(payouts, honmei_hno)

                    # 三連率（◎○▲が全て3着以内）
                    top3_marks = ["◎", "○", "▲"]
                    top3_hnos = [hno for hno, mk in mark_map.items() if mk in top3_marks]
                    if len(top3_hnos) == 3:
                        st["top3_races"] += 1
                        all_in_top3 = all(finish_map.get(hno, 99) <= 3 for hno in top3_hnos)
                        if all_in_top3:
                            st["top3_in_top3"] += 1

            progress.advance(task)

    return stats, total_races, dates_with_training, dates_without_training


def pct(n, d):
    return round(n / d * 100, 1) if d > 0 else 0.0


def format_results(stats, total_races, dates_with_training, dates_without_training):
    """結果をテキストで整形"""
    lines = []
    lines.append("=" * 120)
    lines.append("調教偏差値 composite 組み込み方法 バックテスト結果")
    lines.append("=" * 120)
    lines.append(f"対象日数: {dates_with_training} 日 (training_devなし除外: {dates_without_training} 日)")
    lines.append(f"対象レース数: {total_races:,}")
    lines.append("")

    # ◎の成績比較テーブル
    lines.append("-" * 120)
    header = f"{'パターン':<22} {'◎件数':>6} {'◎勝率':>7} {'◎連対':>7} {'◎複勝':>7} {'単ROI':>7} {'複ROI':>7} {'三連率':>7} {'人気一致':>7} {'vs現行':>7}"
    lines.append(header)
    lines.append("-" * 120)

    baseline_placed_rate = None
    pattern_names = list(stats.keys())

    for pname in pattern_names:
        st = stats[pname]
        ht = st["honmei_total"]
        wr = pct(st["honmei_win"], ht)
        p2r = pct(st["honmei_place2"], ht)
        pr = pct(st["honmei_placed"], ht)
        tansho_roi = pct(st["honmei_tansho_ret"], st["honmei_tansho_stake"])
        fukusho_roi = pct(st["honmei_fukusho_ret"], st["honmei_fukusho_stake"])
        top3_rate = pct(st["top3_in_top3"], st["top3_races"])
        pop1_rate = pct(st["pop1_match"], st["pop1_total"])

        if baseline_placed_rate is None:
            baseline_placed_rate = pr
            vs = "baseline"
        else:
            diff = pr - baseline_placed_rate
            vs = f"{diff:+.1f}pp"

        lines.append(
            f"{pname:<22} {ht:>6} {wr:>6.1f}% {p2r:>6.1f}% {pr:>6.1f}% {tansho_roi:>6.1f}% {fukusho_roi:>6.1f}% {top3_rate:>6.1f}% {pop1_rate:>6.1f}% {vs:>7}"
        )

    # 印別成績（上位パターンのみ表示）
    lines.append("")
    lines.append("=" * 120)
    lines.append("印別成績（全パターン）")
    lines.append("=" * 120)

    for pname in pattern_names:
        st = stats[pname]
        lines.append(f"\n--- {pname} ---")
        lines.append(f"{'印':<4} {'件数':>6} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単ROI':>7} {'複ROI':>7}")

        for mk in MARKS:
            ms = st["by_mark"][mk]
            t = ms["total"]
            if t == 0:
                continue
            wr = pct(ms["win"], t)
            p2r = pct(ms["place2"], t)
            pr = pct(ms["placed"], t)
            t_roi = pct(ms["tansho_ret"], ms["tansho_stake"])
            f_roi = pct(ms["fukusho_ret"], ms["fukusho_stake"])
            lines.append(f"{mk:<4} {t:>6} {wr:>6.1f}% {p2r:>6.1f}% {pr:>6.1f}% {t_roi:>6.1f}% {f_roi:>6.1f}%")

    # 自信度別◎成績
    lines.append("")
    lines.append("=" * 120)
    lines.append("自信度別◎成績（主要パターンのみ）")
    lines.append("=" * 120)

    show_patterns = [pname for pname in pattern_names
                     if pname.startswith("A") or pname in ("B1(w=0.00)", "B4(w=0.05)", "B6(w=0.10)",
                                                            "C2(α=0.002)", "C4(α=0.005)",
                                                            "D2(α=0.002)", "D4(α=0.005)",
                                                            "E2(α=0.003,w=0.01)", "E4(α=0.003,w=0.02)")]

    for pname in show_patterns:
        st = stats[pname]
        lines.append(f"\n--- {pname} ---")
        lines.append(f"{'自信度':<6} {'件数':>6} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単ROI':>7}")

        conf_order = ["SS", "S", "A", "B", "C", "D", "E"]
        for conf in conf_order:
            cs = st["by_conf"].get(conf)
            if not cs or cs["total"] == 0:
                continue
            t = cs["total"]
            wr = pct(cs["win"], t)
            p2r = pct(cs["place2"], t)
            pr = pct(cs["placed"], t)
            t_roi = pct(cs["tansho_ret"], cs["tansho_stake"])
            lines.append(f"{conf:<6} {t:>6} {wr:>6.1f}% {p2r:>6.1f}% {pr:>6.1f}% {t_roi:>6.1f}%")

    return "\n".join(lines)


def save_results_json(stats, total_races, dates_with_training):
    """結果をJSONに保存"""
    output = {
        "total_races": total_races,
        "dates_with_training": dates_with_training,
        "patterns": {},
    }
    for pname, st in stats.items():
        ht = st["honmei_total"]
        # defaultdict を通常dictに変換
        by_conf = {k: dict(v) for k, v in st["by_conf"].items()}
        output["patterns"][pname] = {
            "config": PATTERNS.get(pname, {}),
            "honmei": {
                "total": ht,
                "win_rate": pct(st["honmei_win"], ht),
                "place2_rate": pct(st["honmei_place2"], ht),
                "placed_rate": pct(st["honmei_placed"], ht),
                "tansho_roi": pct(st["honmei_tansho_ret"], st["honmei_tansho_stake"]),
                "fukusho_roi": pct(st["honmei_fukusho_ret"], st["honmei_fukusho_stake"]),
            },
            "top3_rate": pct(st["top3_in_top3"], st["top3_races"]),
            "pop1_match_rate": pct(st["pop1_match"], st["pop1_total"]),
            "by_mark": {mk: dict(ms) for mk, ms in st["by_mark"].items()},
            "by_confidence": by_conf,
        }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="調教偏差値組み込みバックテスト")
    parser.add_argument("--year", type=str, default="", help="対象年 (例: 2026)")
    parser.add_argument("--after", type=str, default="", help="指定日以降 (例: 2026-03-01)")
    args = parser.parse_args()

    console.print("[bold]調教偏差値 composite 組み込みバックテスト[/bold]")
    console.print(f"パターン数: {len(PATTERNS)}")

    dates = get_available_dates(args.year, args.after)
    console.print(f"対象候補日数: {len(dates)}")

    if not dates:
        console.print("[red]対象データなし[/red]")
        return

    stats, total_races, dates_with, dates_without = run_backtest(dates)

    if total_races == 0:
        console.print("[red]training_devが存在するレースなし[/red]")
        return

    # 結果出力
    summary = format_results(stats, total_races, dates_with, dates_without)

    # コンソール出力
    console.print(summary)

    # ファイル出力
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write(summary)
    console.print(f"\n[green]テキストサマリー保存: {OUTPUT_TXT}[/green]")

    save_results_json(stats, total_races, dates_with)
    console.print(f"[green]JSON結果保存: {OUTPUT_JSON}[/green]")

    # ベストパターンのハイライト
    console.print("\n[bold yellow]===ベストパターン===[/bold yellow]")
    best_placed = max(stats.items(), key=lambda x: pct(x[1]["honmei_placed"], x[1]["honmei_total"]))
    best_roi = max(stats.items(), key=lambda x: pct(x[1]["honmei_tansho_ret"], x[1]["honmei_tansho_stake"]))
    best_top3 = max(stats.items(), key=lambda x: pct(x[1]["top3_in_top3"], x[1]["top3_races"]))

    console.print(f"◎複勝率ベスト: {best_placed[0]} ({pct(best_placed[1]['honmei_placed'], best_placed[1]['honmei_total']):.1f}%)")
    console.print(f"単勝ROIベスト: {best_roi[0]} ({pct(best_roi[1]['honmei_tansho_ret'], best_roi[1]['honmei_tansho_stake']):.1f}%)")
    console.print(f"三連率ベスト:  {best_top3[0]} ({pct(best_top3[1]['top3_in_top3'], best_top3[1]['top3_races']):.1f}%)")


if __name__ == "__main__":
    main()
