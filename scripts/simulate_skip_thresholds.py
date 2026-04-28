"""Phase 2 の skip 条件を変えて的中率/ROI のトレードオフを比較する。

複数のシナリオ:
  S0: 現状 (期待値 ≥ 200%)
  S1: 期待値 ≥ 250%
  S2: 期待値 ≥ 300%
  S3: 個別券EV ≥ 150% かつ 期待値 ≥ 200%
  S4: 個別券EV ≥ 200% かつ 期待値 ≥ 200%
  S5: 信頼度 D を強制 skip + 現状条件
  S6: 信頼度 C/D を強制 skip + 現状条件
  S7: 候補チケット数 ≥ 5 かつ 現状条件
  S8: 個別券EV ≥ 150% + 信頼度 D skip + 期待値 ≥ 200%
  S9: 個別券EV ≥ 150% + 信頼度 C/D skip + 期待値 ≥ 200%
"""
from __future__ import annotations
import io
import sys
import json
from pathlib import Path
from itertools import combinations
from collections import defaultdict
from datetime import date, timedelta

import os
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, ".")

from src.calculator.betting import (
    ALLOWED_COL1_MARKS,
    calc_hit_probability,
    calc_expected_value,
    calc_sanrenpuku_prob,
    estimate_umaren_odds,
    estimate_sanrenpuku_odds,
    FIXED_GAP_THRESHOLD,
    FIXED_MAX_UNMARKED_COL3,
    _PARTNER_MARK_PRIO,
    _allocate_fixed_budget,
    _race_expected_ratio,
)
from scripts.monthly_backtest import (
    build_candidates,
    get_payout,
    normalize_combo_str,
)


SCENARIOS = {
    "S0_現状200%":           {"target": 2.0, "min_ticket_ev": 100, "skip_confs": set(), "min_cands": 0},
    "S1_期待値250%":         {"target": 2.5, "min_ticket_ev": 100, "skip_confs": set(), "min_cands": 0},
    "S2_期待値300%":         {"target": 3.0, "min_ticket_ev": 100, "skip_confs": set(), "min_cands": 0},
    "S3_券EV150+200%":       {"target": 2.0, "min_ticket_ev": 150, "skip_confs": set(), "min_cands": 0},
    "S4_券EV200+200%":       {"target": 2.0, "min_ticket_ev": 200, "skip_confs": set(), "min_cands": 0},
    "S5_Dskip+200%":         {"target": 2.0, "min_ticket_ev": 100, "skip_confs": {"D"}, "min_cands": 0},
    "S6_CDskip+200%":        {"target": 2.0, "min_ticket_ev": 100, "skip_confs": {"C", "D"}, "min_cands": 0},
    "S7_候補5+200%":         {"target": 2.0, "min_ticket_ev": 100, "skip_confs": set(), "min_cands": 5},
    "S8_券EV150+Dskip":      {"target": 2.0, "min_ticket_ev": 150, "skip_confs": {"D"}, "min_cands": 0},
    "S9_券EV150+CDskip":     {"target": 2.0, "min_ticket_ev": 150, "skip_confs": {"C", "D"}, "min_cands": 0},
    "SX_券EV200+CDskip+250%": {"target": 2.5, "min_ticket_ev": 200, "skip_confs": {"C", "D"}, "min_cands": 0},
}


FIXED_BUDGET = 2000


def select_with_params(candidates, target_ratio, min_ticket_ev):
    """指定パラメータで候補から採用チケットを選ぶ。"""
    cands = [c for c in candidates if (c.get("ev", 0) or 0) >= min_ticket_ev]
    if not cands:
        return [], 0.0, True
    sorted_c = sorted(cands, key=lambda t: -(t.get("ev", 0) or 0))
    for n_t in range(len(sorted_c), 0, -1):
        trial = [dict(t) for t in sorted_c[:n_t]]
        _allocate_fixed_budget(trial, FIXED_BUDGET)
        active = [t for t in trial if (t.get("stake", 0) or 0) > 0]
        if not active:
            continue
        ratio = _race_expected_ratio(active)
        if ratio >= target_ratio:
            return active, round(ratio, 3), False
    # 単独 EV >= target チェック
    top = sorted_c[0]
    if (top.get("ev", 0) or 0) >= target_ratio * 100:
        chosen = [dict(top)]
        chosen[0]["stake"] = min(FIXED_BUDGET, 1000)
        return chosen, round((top.get("ev", 0) or 0) / 100.0, 3), False
    return [], 0.0, True


def simulate_day(date_str, scenarios):
    """1日分を全シナリオでシミュレート"""
    pred_fp = Path(f"data/predictions/{date_str}_pred.json")
    res_fp = Path(f"data/results/{date_str}_results.json")
    if not pred_fp.exists() or not res_fp.exists():
        return None
    with pred_fp.open("r", encoding="utf-8") as f:
        pred = json.load(f)
    with res_fp.open("r", encoding="utf-8") as f:
        results = json.load(f)

    # 各レースで候補チケットを 1回だけ生成し、シナリオごとに選択を変える
    scenario_stats = {
        sn: {"played": 0, "skipped": 0, "race_hit": 0,
             "points": 0, "hit": 0, "stake": 0, "payback": 0}
        for sn in scenarios
    }
    n_races = len(pred.get("races", []))
    for r in pred.get("races", []):
        race_id = str(r.get("race_id", ""))
        conf = r.get("confidence", "C")
        n = r.get("field_count") or len(r.get("horses", []))
        is_jra = r.get("is_jra", True)
        horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
        if not horses:
            continue
        rdata = results.get(race_id)
        if rdata is None:
            continue
        payouts = rdata.get("payouts", {})
        if not payouts:
            continue
        try:
            cands = build_candidates(horses, n, is_jra)
        except Exception:
            continue

        for sn, cfg in scenarios.items():
            st = scenario_stats[sn]
            # 信頼度 skip
            if conf in cfg["skip_confs"]:
                st["skipped"] += 1
                continue
            # 候補数 skip
            if len(cands) < cfg["min_cands"]:
                st["skipped"] += 1
                continue
            chosen, ratio, skipped = select_with_params(
                cands, cfg["target"], cfg["min_ticket_ev"],
            )
            if skipped:
                st["skipped"] += 1
                continue
            st["played"] += 1
            race_hit = False
            for t in chosen:
                stake = t.get("stake", 0) or 0
                if stake <= 0:
                    continue
                pp100 = get_payout(payouts, t)
                payback = pp100 * (stake // 100)
                hit = 1 if payback > 0 else 0
                st["points"] += 1
                st["hit"] += hit
                st["stake"] += stake
                st["payback"] += payback
                if hit:
                    race_hit = True
            if race_hit:
                st["race_hit"] += 1
    return scenario_stats, n_races


def main():
    if len(sys.argv) < 3:
        print("usage: simulate_skip_thresholds.py 20260301 20260331")
        return 1
    start_str, end_str = sys.argv[1], sys.argv[2]
    start = date(int(start_str[:4]), int(start_str[4:6]), int(start_str[6:8]))
    end = date(int(end_str[:4]), int(end_str[4:6]), int(end_str[6:8]))

    grand = {sn: {"played": 0, "skipped": 0, "race_hit": 0,
                   "points": 0, "hit": 0, "stake": 0, "payback": 0}
             for sn in SCENARIOS}
    total_races = 0

    d = start
    n_days = 0
    while d <= end:
        ds = d.strftime("%Y%m%d")
        result = simulate_day(ds, SCENARIOS)
        if result is not None:
            n_days += 1
            stats, nr = result
            total_races += nr
            for sn in SCENARIOS:
                for k, v in stats[sn].items():
                    grand[sn][k] += v
        d += timedelta(days=1)

    print("=" * 110)
    print(f"期間: {start_str}～{end_str}  処理日 {n_days}日 / 総レース {total_races}R")
    print("=" * 110)
    print()
    header = f"{'シナリオ':<24}{'買R':>6}{'当R':>5}{'R的中率':>9}{'券点数':>8}{'券当':>5}{'券率':>7}{'投資':>11}{'払戻':>11}{'ROI':>8}{'純利益':>12}"
    print(header)
    print("-" * len(header))

    rows = []
    for sn, v in grand.items():
        played = v["played"]
        if played == 0:
            continue
        rate = v["race_hit"] / played * 100
        p_rate = v["hit"] / v["points"] * 100 if v["points"] else 0
        roi = v["payback"] / v["stake"] * 100 if v["stake"] else 0
        net = v["payback"] - v["stake"]
        rows.append((sn, played, v["race_hit"], rate, v["points"], v["hit"],
                     p_rate, v["stake"], v["payback"], roi, net))

    for r in rows:
        sn, played, race_hit, rate, points, hit, p_rate, stake, payback, roi, net = r
        print(f"{sn:<24}{played:>6}{race_hit:>5}{rate:>8.1f}%{points:>8}{hit:>5}{p_rate:>6.1f}%{stake:>11,}{payback:>11,}{roi:>7.1f}%{net:>+12,}")

    # トップ3 評価
    print()
    print("--- 評価軸別 TOP3 ---")
    print()
    print("[ROI 順 (回収率)]")
    for r in sorted(rows, key=lambda x: -x[9])[:3]:
        print(f"  {r[0]:<24} ROI {r[9]:.1f}% / R的中率 {r[3]:.1f}% / 純利益 {r[10]:+,}")
    print()
    print("[レース的中率 順]")
    for r in sorted(rows, key=lambda x: -x[3])[:3]:
        print(f"  {r[0]:<24} R的中率 {r[3]:.1f}% / ROI {r[9]:.1f}% / 純利益 {r[10]:+,}")
    print()
    print("[純利益 順]")
    for r in sorted(rows, key=lambda x: -x[10])[:3]:
        print(f"  {r[0]:<24} 純利益 {r[10]:+,} / ROI {r[9]:.1f}% / R的中率 {r[3]:.1f}%")

    return 0


if __name__ == "__main__":
    sys.exit(main())
