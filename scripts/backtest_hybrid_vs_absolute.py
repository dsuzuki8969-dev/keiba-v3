"""Plan-γ Phase 6: 絶対指数 vs ハイブリッド指数 バックテスト

絶対指数モード（ability_total ベース印付与）と
ハイブリッドモード（hybrid_total = ability_total*(1-β) + race_relative_dev*β）の
ROI・的中率を比較して最適 β を探索する。

NOTE:
  race_relative_dev は pred.json には保存されていないため、本スクリプトで
  ability_total を同レース内 z-score 正規化して再計算する（engine.py と同一ロジック）。

使い方:
  python scripts/backtest_hybrid_vs_absolute.py
  python scripts/backtest_hybrid_vs_absolute.py --start 2026-01-01 --end 2026-04-28
  python scripts/backtest_hybrid_vs_absolute.py --beta 0.50
  python scripts/backtest_hybrid_vs_absolute.py --venues 帯広
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import date, timedelta
from itertools import permutations
from pathlib import Path
from typing import Optional

# Windows コンソール cp932 対策
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, ".")

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ============================================================
# 定数
# ============================================================
RELATIVE_DEV_MIN_FIELD = 5   # z-score 計算に必要な最小頭数
ABILITY_DEV_MIN = 20.0       # ability/hybrid の下限クランプ
ABILITY_DEV_MAX = 100.0      # ability/hybrid の上限クランプ

# 印優先度（高い=上位）
MARK_PRIORITY = {"◉": 7, "◎": 6, "○": 5, "▲": 4, "△": 3, "★": 2, "☆": 1, "×": -1, "－": 0, "": 0}

# 印付与に使う頭数（上位5頭: ◎○▲△★）
MARK_COUNT = 5

# 三連単フォーメーション: ◎○▲△★ の順で先頭2頭を組合せ、3着は残り3頭
SANRENTAN_FORM_SIZE = 5  # ◎○▲△★
SANRENTAN_STAKE = 100    # 1点 100円


# ============================================================
# race_relative_dev 再計算（engine.py と同一ロジック）
# ============================================================
def calc_race_relative_dev(ability_totals: list[Optional[float]]) -> list[float]:
    """ability_total リストを同レース内 z-score 正規化して race_relative_dev を返す。

    - ability_total が None の馬は 50.0 固定
    - 頭数 < RELATIVE_DEV_MIN_FIELD または σ=0 の場合は全馬 50.0
    """
    valid = [v for v in ability_totals if v is not None]
    if len(valid) < RELATIVE_DEV_MIN_FIELD:
        return [50.0] * len(ability_totals)
    mean = statistics.mean(valid)
    try:
        std = statistics.stdev(valid)
    except statistics.StatisticsError:
        std = 0.0
    if std == 0.0:
        return [50.0] * len(ability_totals)
    result = []
    for v in ability_totals:
        if v is None:
            result.append(50.0)
        else:
            rrd = 50.0 + 10.0 * (v - mean) / std
            result.append(round(max(ABILITY_DEV_MIN, min(ABILITY_DEV_MAX, rrd)), 2))
    return result


def calc_hybrid_total(ability_total: Optional[float],
                      race_relative_dev: float,
                      beta: float) -> float:
    """ハイブリッド合算指数を算出する。"""
    at = ability_total if ability_total is not None else 50.0
    blended = at * (1 - beta) + race_relative_dev * beta
    return round(max(ABILITY_DEV_MIN, min(ABILITY_DEV_MAX, blended)), 2)


# ============================================================
# 仮想印付与
# ============================================================
def assign_virtual_marks(horses: list[dict], scoring_key: str) -> list[str]:
    """scoring_key (ability_total / hybrid_total) でソートして上位5頭に印を付ける。

    返り値: 各馬の印リスト（horses と同順）
    """
    # 取消馬を除外
    active_idx = [i for i, h in enumerate(horses) if not h.get("is_scratched")]
    if not active_idx:
        return ["－"] * len(horses)

    sorted_idx = sorted(
        active_idx,
        key=lambda i: -(horses[i].get(scoring_key) or 50.0),
    )

    marks = ["－"] * len(horses)
    mark_labels = ["◎", "○", "▲", "△", "★"]
    for rank, idx in enumerate(sorted_idx[:MARK_COUNT]):
        marks[idx] = mark_labels[rank]

    return marks


# ============================================================
# 単勝・複勝 的中判定
# ============================================================
def check_tansho_hit(marks: list[str], order: list[dict]) -> tuple[bool, int]:
    """◎ の馬が1着か判定。(hit: bool, payout: int)

    NOTE: 実際の判定は eval_race 内で horse_no ベースで行うため未使用。
    """
    if not order:
        return False, 0
    # ◎ の horse_no を探す（eval_race で直接判定するため dummy を返す）
    return False, 0


def check_fukusho_hit(horse_no: int, order: list[dict], n_horses: int) -> bool:
    """horse_no が3着以内か（頭数によって2着以内の場合あり）"""
    top3 = [o.get("horse_no") for o in order[:3]]
    return horse_no in top3


# ============================================================
# 三連単フォーメーション: 上位5頭（◎○▲△★）の全組合せ
# ============================================================
def build_sanrentan_combos(
    horses: list[dict],
    marks: list[str],
) -> list[tuple[int, int, int]]:
    """◎○▲△★ の馬番で 3 桁順列を生成する。"""
    form_horses = [
        horses[i]
        for i, m in enumerate(marks)
        if m in {"◎", "○", "▲", "△", "★"} and not horses[i].get("is_scratched")
    ]
    if len(form_horses) < 3:
        return []
    nos = [h.get("horse_no") for h in form_horses]
    return list(permutations(nos, 3))


def check_sanrentan_hit(
    combos: list[tuple[int, int, int]],
    order: list[dict],
) -> tuple[bool, int]:
    """三連単コンボが的中したか判定。(hit: bool, payout: int)"""
    if len(order) < 3:
        return False, 0
    top3 = tuple(o.get("horse_no") for o in order[:3])
    return top3 in set(combos), 0  # payout は呼び出し元で処理


# ============================================================
# payout 取得ヘルパー
# ============================================================
def get_tansho_payout(payouts: dict) -> int:
    """単勝払戻金額 (円)"""
    t = payouts.get("単勝")
    if isinstance(t, dict):
        return int(t.get("payout") or 0)
    return 0


def get_fukusho_payout(horse_no: int, payouts: dict) -> int:
    """複勝払戻金額 (円)"""
    items = payouts.get("複勝", [])
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict) and it.get("combo") == str(horse_no):
                return int(it.get("payout") or 0)
    return 0


def get_sanrentan_payout(combo: tuple, payouts: dict) -> int:
    """三連単払戻金額 (円)"""
    t = payouts.get("三連単")
    nos = "-".join(str(x) for x in combo)
    if isinstance(t, dict):
        return int(t.get("payout", 0) or 0) if str(t.get("combo", "")) == nos else 0
    if isinstance(t, list):
        for it in t:
            if isinstance(it, dict) and str(it.get("combo", "")) == nos:
                return int(it.get("payout", 0) or 0)
    return 0


def get_sanrentan_winner_combo(payouts: dict) -> Optional[tuple]:
    """結果の三連単当選組合せを (1着, 2着, 3着) タプルで返す"""
    t = payouts.get("三連単")
    combo_str = None
    if isinstance(t, dict):
        combo_str = t.get("combo")
    elif isinstance(t, list) and t:
        combo_str = t[0].get("combo") if isinstance(t[0], dict) else None
    if combo_str:
        try:
            return tuple(int(x) for x in str(combo_str).split("-"))
        except ValueError:
            return None
    return None


# ============================================================
# 1レース評価
# ============================================================
def eval_race(
    race: dict,
    result_data: dict,
    beta: float,
) -> Optional[dict]:
    """1レースの絶対 vs ハイブリッドを評価して統計ディクトを返す。

    結果データ不在・フォーマットエラーは None を返してスキップ。
    """
    horses = [h for h in race.get("horses", []) if not h.get("is_scratched")]
    if not horses:
        return None

    order = result_data.get("order", [])
    payouts = result_data.get("payouts", {})
    if not order or not payouts:
        return None  # 結果データ不在はスキップ

    n = len(horses)
    race_id = str(race.get("race_id", ""))
    confidence = race.get("confidence", "C")
    venue = race.get("venue", "")
    is_banei = race.get("is_banei", False)

    # ---- ability_total 収集 & race_relative_dev 計算 ----
    ability_totals = [h.get("ability_total") for h in horses]
    rrd_list = calc_race_relative_dev(ability_totals)
    for i, h in enumerate(horses):
        h["_rrd"] = rrd_list[i]
        h["_hybrid"] = calc_hybrid_total(h.get("ability_total"), rrd_list[i], beta)

    # ---- 印付与（2モード）----
    abs_marks = assign_virtual_marks(horses, "ability_total")
    hyb_marks = assign_virtual_marks(horses, "_hybrid")

    # ---- ◎ の horse_no を特定 ----
    abs_honmei_idx = abs_marks.index("◎") if "◎" in abs_marks else None
    hyb_honmei_idx = hyb_marks.index("◎") if "◎" in hyb_marks else None

    if abs_honmei_idx is None or hyb_honmei_idx is None:
        return None

    abs_honmei_no = horses[abs_honmei_idx].get("horse_no")
    hyb_honmei_no = horses[hyb_honmei_idx].get("horse_no")

    # ---- 単勝 ----
    winner_no = order[0].get("horse_no") if order else None
    tansho_payout = get_tansho_payout(payouts)

    abs_tansho_hit = (abs_honmei_no == winner_no)
    hyb_tansho_hit = (hyb_honmei_no == winner_no)
    abs_tansho_payback = tansho_payout if abs_tansho_hit else 0
    hyb_tansho_payback = tansho_payout if hyb_tansho_hit else 0

    # ---- 複勝 ----
    top3_nos = {o.get("horse_no") for o in order[:3]}
    abs_fukusho_hit = (abs_honmei_no in top3_nos)
    hyb_fukusho_hit = (hyb_honmei_no in top3_nos)
    abs_fukusho_payback = get_fukusho_payout(abs_honmei_no, payouts) if abs_fukusho_hit else 0
    hyb_fukusho_payback = get_fukusho_payout(hyb_honmei_no, payouts) if hyb_fukusho_hit else 0

    # ---- 三連単フォーメーション ----
    abs_combos = build_sanrentan_combos(horses, abs_marks)
    hyb_combos = build_sanrentan_combos(horses, hyb_marks)
    winner_combo = get_sanrentan_winner_combo(payouts)

    abs_srt_hit = bool(winner_combo and winner_combo in set(abs_combos))
    hyb_srt_hit = bool(winner_combo and winner_combo in set(hyb_combos))
    srt_payout = (
        get_sanrentan_payout(winner_combo, payouts) if winner_combo else 0
    )
    abs_srt_payback = srt_payout * (SANRENTAN_STAKE // 100) if abs_srt_hit else 0
    hyb_srt_payback = srt_payout * (SANRENTAN_STAKE // 100) if hyb_srt_hit else 0
    abs_srt_stake = len(abs_combos) * SANRENTAN_STAKE if abs_combos else 0
    hyb_srt_stake = len(hyb_combos) * SANRENTAN_STAKE if hyb_combos else 0

    # ---- 印分布変化 ----
    mark_changed = {}
    for label in ["◎", "○", "▲", "△", "★"]:
        a_idx = abs_marks.index(label) if label in abs_marks else None
        h_idx = hyb_marks.index(label) if label in hyb_marks else None
        mark_changed[label] = (a_idx != h_idx)

    # ---- ばんえい張り付き ----
    banei_sticky_abs = 0
    banei_sticky_hyb = 0
    if is_banei:
        # ability_total が 20〜21 に集中している場合を「張り付き」とみなす
        banei_sticky_abs = sum(
            1 for h in horses if abs(( h.get("ability_total") or 50) - 20.0) < 2.0
        )
        hyb_vals = [h.get("_hybrid", 50.0) for h in horses]
        banei_sticky_hyb = sum(1 for v in hyb_vals if abs(v - 20.0) < 2.0)

    return {
        "race_id": race_id,
        "confidence": confidence,
        "venue": venue,
        "is_banei": is_banei,
        "n_horses": n,
        # 単勝
        "abs_tansho_hit": int(abs_tansho_hit),
        "hyb_tansho_hit": int(hyb_tansho_hit),
        "abs_tansho_payback": abs_tansho_payback,
        "hyb_tansho_payback": hyb_tansho_payback,
        "tansho_stake": 100,
        # 複勝
        "abs_fukusho_hit": int(abs_fukusho_hit),
        "hyb_fukusho_hit": int(hyb_fukusho_hit),
        "abs_fukusho_payback": abs_fukusho_payback,
        "hyb_fukusho_payback": hyb_fukusho_payback,
        "fukusho_stake": 100,
        # 三連単
        "abs_srt_hit": int(abs_srt_hit),
        "hyb_srt_hit": int(hyb_srt_hit),
        "abs_srt_stake": abs_srt_stake,
        "hyb_srt_stake": hyb_srt_stake,
        "abs_srt_payback": abs_srt_payback,
        "hyb_srt_payback": hyb_srt_payback,
        "abs_srt_combos": len(abs_combos),
        "hyb_srt_combos": len(hyb_combos),
        # 印分布変化
        "mark_honmei_changed": int(mark_changed.get("◎", False)),
        "mark_taiko_changed": int(mark_changed.get("○", False)),
        "mark_sannban_changed": int(mark_changed.get("▲", False)),
        "mark_yonban_changed": int(mark_changed.get("△", False)),
        "mark_goban_changed": int(mark_changed.get("★", False)),
        # ばんえい張り付き
        "banei_sticky_abs": banei_sticky_abs,
        "banei_sticky_hyb": banei_sticky_hyb,
    }


# ============================================================
# 1日処理
# ============================================================
def process_day(date_str: str, beta: float, venues_filter: Optional[list]) -> list[dict]:
    """1日分の pred.json + results.json を読み込み、レース単位の評価リストを返す。"""
    pred_fp = Path(f"data/predictions/{date_str}_pred.json")
    res_fp = Path(f"data/results/{date_str}_results.json")
    if not pred_fp.exists() or not res_fp.exists():
        return []
    try:
        with pred_fp.open("r", encoding="utf-8") as f:
            pred = json.load(f)
        with res_fp.open("r", encoding="utf-8") as f:
            results = json.load(f)
    except (json.JSONDecodeError, OSError) as ex:
        print(f"[{date_str}] SKIP - JSON パースエラー: {ex}", file=sys.stderr)
        return []

    race_results = []
    for race in pred.get("races", []):
        venue = race.get("venue", "")
        if venues_filter and venue not in venues_filter:
            continue
        race_id = str(race.get("race_id", ""))
        result_data = results.get(race_id)
        if result_data is None:
            # 結果データ不在はスキップ（推定で埋めない）
            continue
        r = eval_race(race, result_data, beta)
        if r is None:
            continue
        r["date"] = date_str
        race_results.append(r)
    return race_results


# ============================================================
# 集計
# ============================================================
def aggregate(records: list[dict]) -> dict:
    """レース単位の評価リストを集計して統計ディクトを返す。"""
    n = len(records)
    if n == 0:
        return {}

    def _roi(payback: int, stake: int) -> float:
        return payback / stake * 100.0 if stake > 0 else 0.0

    # 単勝
    abs_tan_hits = sum(r["abs_tansho_hit"] for r in records)
    hyb_tan_hits = sum(r["hyb_tansho_hit"] for r in records)
    abs_tan_pb = sum(r["abs_tansho_payback"] for r in records)
    hyb_tan_pb = sum(r["hyb_tansho_payback"] for r in records)
    tan_stake = n * 100

    # 複勝
    abs_fuk_hits = sum(r["abs_fukusho_hit"] for r in records)
    hyb_fuk_hits = sum(r["hyb_fukusho_hit"] for r in records)
    abs_fuk_pb = sum(r["abs_fukusho_payback"] for r in records)
    hyb_fuk_pb = sum(r["hyb_fukusho_payback"] for r in records)
    fuk_stake = n * 100

    # 三連単
    abs_srt_hits = sum(r["abs_srt_hit"] for r in records)
    hyb_srt_hits = sum(r["hyb_srt_hit"] for r in records)
    abs_srt_stake = sum(r["abs_srt_stake"] for r in records)
    hyb_srt_stake = sum(r["hyb_srt_stake"] for r in records)
    abs_srt_pb = sum(r["abs_srt_payback"] for r in records)
    hyb_srt_pb = sum(r["hyb_srt_payback"] for r in records)

    # 印変化
    honmei_changed = sum(r["mark_honmei_changed"] for r in records)
    taiko_changed = sum(r["mark_taiko_changed"] for r in records)
    sannban_changed = sum(r["mark_sannban_changed"] for r in records)
    yonban_changed = sum(r["mark_yonban_changed"] for r in records)
    goban_changed = sum(r["mark_goban_changed"] for r in records)

    # ばんえい張り付き
    banei_records = [r for r in records if r["is_banei"]]
    banei_sticky_abs = sum(r["banei_sticky_abs"] for r in banei_records)
    banei_sticky_hyb = sum(r["banei_sticky_hyb"] for r in banei_records)

    # 信頼度別
    by_conf: dict[str, dict] = defaultdict(lambda: {
        "n": 0,
        "abs_tan_hits": 0, "hyb_tan_hits": 0,
        "abs_tan_pb": 0, "hyb_tan_pb": 0,
        "abs_fuk_hits": 0, "hyb_fuk_hits": 0,
        "abs_fuk_pb": 0, "hyb_fuk_pb": 0,
    })
    for r in records:
        c = r.get("confidence", "?")
        by_conf[c]["n"] += 1
        by_conf[c]["abs_tan_hits"] += r["abs_tansho_hit"]
        by_conf[c]["hyb_tan_hits"] += r["hyb_tansho_hit"]
        by_conf[c]["abs_tan_pb"] += r["abs_tansho_payback"]
        by_conf[c]["hyb_tan_pb"] += r["hyb_tansho_payback"]
        by_conf[c]["abs_fuk_hits"] += r["abs_fukusho_hit"]
        by_conf[c]["hyb_fuk_hits"] += r["hyb_fukusho_hit"]
        by_conf[c]["abs_fuk_pb"] += r["abs_fukusho_payback"]
        by_conf[c]["hyb_fuk_pb"] += r["hyb_fukusho_payback"]

    return {
        "n_races": n,
        "n_banei": len(banei_records),
        "tansho": {
            "abs_hits": abs_tan_hits, "hyb_hits": hyb_tan_hits,
            "abs_hit_rate": abs_tan_hits / n * 100,
            "hyb_hit_rate": hyb_tan_hits / n * 100,
            "abs_roi": _roi(abs_tan_pb, tan_stake),
            "hyb_roi": _roi(hyb_tan_pb, tan_stake),
        },
        "fukusho": {
            "abs_hits": abs_fuk_hits, "hyb_hits": hyb_fuk_hits,
            "abs_hit_rate": abs_fuk_hits / n * 100,
            "hyb_hit_rate": hyb_fuk_hits / n * 100,
            "abs_roi": _roi(abs_fuk_pb, fuk_stake),
            "hyb_roi": _roi(hyb_fuk_pb, fuk_stake),
        },
        "sanrentan": {
            "abs_hits": abs_srt_hits, "hyb_hits": hyb_srt_hits,
            "abs_hit_rate": abs_srt_hits / n * 100,
            "hyb_srt_rate": hyb_srt_hits / n * 100,
            "abs_roi": _roi(abs_srt_pb, abs_srt_stake),
            "hyb_roi": _roi(hyb_srt_pb, hyb_srt_stake),
            "abs_stake": abs_srt_stake,
            "hyb_stake": hyb_srt_stake,
        },
        "mark_change": {
            "◎": honmei_changed, "◎_rate": honmei_changed / n * 100,
            "○": taiko_changed, "○_rate": taiko_changed / n * 100,
            "▲": sannban_changed, "▲_rate": sannban_changed / n * 100,
            "△": yonban_changed, "△_rate": yonban_changed / n * 100,
            "★": goban_changed, "★_rate": goban_changed / n * 100,
        },
        "banei_sticky": {
            "abs": banei_sticky_abs,
            "hyb": banei_sticky_hyb,
        },
        "by_conf": {k: v for k, v in sorted(by_conf.items())},
    }


# ============================================================
# サマリ表示
# ============================================================
def print_summary(
    result: dict,
    start_str: str,
    end_str: str,
    beta: float,
    n_days: int,
) -> None:
    """標準出力に表形式サマリを出力する。"""
    n = result.get("n_races", 0)
    if n == 0:
        print("対象レースなし")
        return

    t = result["tansho"]
    f = result["fukusho"]
    s = result["sanrentan"]
    mc = result["mark_change"]
    bs = result["banei_sticky"]

    print()
    print("=" * 70)
    print(f"Plan-γ Phase 6 バックテスト: 絶対 vs ハイブリッド (β={beta:.2f})")
    print("=" * 70)
    print(f"期間: {start_str} 〜 {end_str} ({n_days}日 / {n}レース)")

    def _diff_str(a: float, b: float, fmt: str = ".1f") -> str:
        d = b - a
        sign = "+" if d >= 0 else ""
        return f"[{sign}{d:{fmt}}pt]"

    print()
    print("【単勝 ◎】")
    print(f"  絶対指数:    的中 {t['abs_hits']:>4} / {n} ({t['abs_hit_rate']:.1f}%)  "
          f"回収率 {t['abs_roi']:.1f}%")
    print(f"  ハイブリッド: 的中 {t['hyb_hits']:>4} / {n} ({t['hyb_hit_rate']:.1f}%)  "
          f"回収率 {t['hyb_roi']:.1f}%  "
          f"{_diff_str(t['abs_hit_rate'], t['hyb_hit_rate'])}")

    print()
    print("【複勝 ◎】")
    print(f"  絶対指数:    的中 {f['abs_hits']:>4} / {n} ({f['abs_hit_rate']:.1f}%)  "
          f"回収率 {f['abs_roi']:.1f}%")
    print(f"  ハイブリッド: 的中 {f['hyb_hits']:>4} / {n} ({f['hyb_hit_rate']:.1f}%)  "
          f"回収率 {f['hyb_roi']:.1f}%  "
          f"{_diff_str(f['abs_hit_rate'], f['hyb_hit_rate'])}")

    print()
    print("【三連単フォーメーション ROI】")
    print(f"  絶対指数:    的中 {s['abs_hits']:>3}R  ROI {s['abs_roi']:.1f}%  "
          f"投資 {s['abs_stake']:,}円")
    print(f"  ハイブリッド: 的中 {s['hyb_hits']:>3}R  ROI {s['hyb_roi']:.1f}%  "
          f"投資 {s['hyb_stake']:,}円  "
          f"{_diff_str(s['abs_roi'], s['hyb_roi'])}")

    print()
    print("【印分布変化 (絶対→ハイブリッドで馬が変わったレース数)】")
    for mark in ["◎", "○", "▲", "△", "★"]:
        cnt = mc[mark]
        rate = mc[f"{mark}_rate"]
        print(f"  {mark}: {cnt:>4} / {n} ({rate:.1f}%)")

    print()
    print("【帯広ばんえい 張り付き (ability_total ≒ 20 の馬数)】")
    nb = result.get("n_banei", 0)
    print(f"  ばんえいレース数: {nb}R")
    print(f"  絶対指数 張り付き頭数: {bs['abs']}")
    print(f"  ハイブリッド 張り付き頭数: {bs['hyb']}")

    # 信頼度別
    print()
    print("【信頼度別 単勝 ◎ 回収率】")
    print(f"{'conf':<6}{'N':>5}  {'絶対的中%':>9}  {'絶対ROI':>7}  {'HYB的中%':>9}  {'HYB ROI':>7}")
    for conf in ("SS", "S", "A", "B", "C", "D"):
        cv = result["by_conf"].get(conf)
        if not cv or cv["n"] == 0:
            continue
        cn = cv["n"]
        a_hr = cv["abs_tan_hits"] / cn * 100
        h_hr = cv["hyb_tan_hits"] / cn * 100
        a_roi = cv["abs_tan_pb"] / (cn * 100) * 100
        h_roi = cv["hyb_tan_pb"] / (cn * 100) * 100
        print(f"{conf:<6}{cn:>5}  {a_hr:>8.1f}%  {a_roi:>6.1f}%  {h_hr:>8.1f}%  {h_roi:>6.1f}%")

    print()
    print("=" * 70)


# ============================================================
# メイン
# ============================================================
def parse_args() -> argparse.Namespace:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(
        description="Plan-γ Phase 6: 絶対 vs ハイブリッド指数バックテスト"
    )
    today = date.today()
    one_month_ago = today - timedelta(days=28)
    parser.add_argument(
        "--start",
        default=one_month_ago.strftime("%Y-%m-%d"),
        help="開始日 (YYYY-MM-DD, デフォルト: 28日前)",
    )
    parser.add_argument(
        "--end",
        default=today.strftime("%Y-%m-%d"),
        help="終了日 (YYYY-MM-DD, デフォルト: 今日)",
    )
    parser.add_argument(
        "--beta",
        type=float,
        default=None,
        help="ハイブリッド β 値 (0〜1)。省略時は 0.30 / 0.50 / 0.70 を比較",
    )
    parser.add_argument(
        "--venues",
        default=None,
        nargs="+",
        help="会場絞り込み (例: --venues 帯広 札幌)",
    )
    parser.add_argument(
        "--output-json",
        dest="output_json",
        default=None,
        help="結果 JSON 保存先パス (省略時は data/reports/ 配下に自動生成)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)
    except ValueError as e:
        print(f"日付フォーマットエラー: {e}", file=sys.stderr)
        return 1

    if start_date > end_date:
        print("--start が --end より後の日付です", file=sys.stderr)
        return 1

    venues_filter = args.venues

    # テストする β のリスト
    beta_list = [args.beta] if args.beta is not None else [0.30, 0.50, 0.70]

    # data/reports ディレクトリを作成
    Path("data/reports").mkdir(parents=True, exist_ok=True)

    # ログ出力先
    today_str = date.today().strftime("%Y%m%d")
    log_path = Path(f"log/backtest_hybrid_vs_absolute_{today_str}.log")
    log_path.parent.mkdir(exist_ok=True)

    print(f"[バックテスト開始] 期間: {args.start} 〜 {args.end}")
    if venues_filter:
        print(f"[会場絞り込み] {venues_filter}")
    print(f"[β 一覧] {beta_list}")
    print()

    # 日付リスト生成
    date_list = []
    d = start_date
    while d <= end_date:
        date_list.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)

    all_results: dict[float, dict] = {}
    log_lines: list[str] = []

    for beta in beta_list:
        print(f">>> β={beta:.2f} を処理中...")

        all_records: list[dict] = []

        # tqdm があれば進捗バー表示
        iter_dates = tqdm(date_list, desc=f"β={beta:.2f}") if HAS_TQDM else date_list

        for date_str in iter_dates:
            day_records = process_day(date_str, beta, venues_filter)
            all_records.extend(day_records)

        if not all_records:
            print(f"  [β={beta:.2f}] 対象レースなし（結果データが存在しないか期間外）")
            all_results[beta] = {}
            continue

        n_days_actual = len({r["date"] for r in all_records})
        result = aggregate(all_records)
        all_results[beta] = result

        print_summary(result, args.start, args.end, beta, n_days_actual)

        # ログに記録
        log_lines.append(f"=== β={beta:.2f} | {args.start}〜{args.end} | {result.get('n_races',0)}R ===")
        log_lines.append(f"  単勝ROI 絶対:{result['tansho']['abs_roi']:.1f}% ハイブリッド:{result['tansho']['hyb_roi']:.1f}%")
        log_lines.append(f"  複勝ROI 絶対:{result['fukusho']['abs_roi']:.1f}% ハイブリッド:{result['fukusho']['hyb_roi']:.1f}%")
        log_lines.append(f"  三連単ROI 絶対:{result['sanrentan']['abs_roi']:.1f}% ハイブリッド:{result['sanrentan']['hyb_roi']:.1f}%")
        log_lines.append(f"  ◎変化:{result['mark_change']['◎']}R({result['mark_change']['◎_rate']:.1f}%)")
        log_lines.append("")

    # β 比較サマリ（複数 β の場合）
    if len(beta_list) > 1:
        print()
        print("=" * 70)
        print("【β 比較サマリ】")
        print(f"{'β':>6}  {'単勝HYB_ROI':>12}  {'複勝HYB_ROI':>12}  {'三連単HYB_ROI':>14}  {'◎変化率':>9}")
        print("-" * 70)
        best_beta = None
        best_score = -9999.0
        for b in beta_list:
            r = all_results.get(b, {})
            if not r:
                print(f"  β={b:.2f}  データなし")
                continue
            tan_roi = r["tansho"]["hyb_roi"]
            fuk_roi = r["fukusho"]["hyb_roi"]
            srt_roi = r["sanrentan"]["hyb_roi"]
            honmei_chg = r["mark_change"]["◎_rate"]
            # スコア = 単勝ROI + 三連単ROI (簡易評価)
            score = tan_roi + srt_roi
            if score > best_score:
                best_score = score
                best_beta = b
            print(f"  β={b:.2f}  {tan_roi:>10.1f}%  {fuk_roi:>10.1f}%  {srt_roi:>12.1f}%  {honmei_chg:>8.1f}%")
        print("-" * 70)
        if best_beta is not None:
            print(f"  推奨 β = {best_beta:.2f} (単勝ROI + 三連単ROI が最大)")
        print("=" * 70)

    # JSON 保存
    json_path = args.output_json or f"data/reports/hybrid_backtest_{today_str}.json"
    save_data = {
        "generated_at": today_str,
        "start": args.start,
        "end": args.end,
        "venues_filter": venues_filter,
        "results_by_beta": {str(b): v for b, v in all_results.items()},
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)
    print(f"\n[JSON保存] {json_path}")

    # ログ保存
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))
    print(f"[ログ保存] {log_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
