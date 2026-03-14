# -*- coding: utf-8 -*-
"""
出現率計の分布分析スクリプト
  data/predictions/*.json を全て読み込み、
  馬連・三連複ごとの EV≥100% 出現率合計 (%) の分布を出力する。

使い方:
  python tools/analyze_confidence_dist.py [--min-horses N] [--payout-rate 0.75]
"""

import json
import math
import sys
import os
from itertools import combinations
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ── ここだけ inline で計算（engine依存なし）──────────────────────────

def _head_factor(n: int) -> float:
    if n <= 8:  return 3.0
    if n <= 10: return 3.2
    if n <= 12: return 3.5
    if n <= 14: return 3.8
    return 4.0

def estimate_umaren_odds(oa: float, ob: float, n: int) -> float:
    return oa * ob / _head_factor(n) * 0.97

def estimate_sanrenpuku_odds(oa: float, ob: float, oc: float, n: int) -> float:
    if n <= 8:   factor = 12.0
    elif n <= 10: factor = 16.0
    elif n <= 12: factor = 20.0
    elif n <= 14: factor = 24.0
    else:         factor = 28.0
    return max(2.0, oa * ob * oc / factor * (0.75 / 0.80))

def calc_umaren_prob(p2a: float, p2b: float, n: int) -> float:
    return min(p2a * p2b * (n / (n - 1)), 0.99)

def calc_sanrenpuku_prob(p3a: float, p3b: float, p3c: float, n: int) -> float:
    correction = n * (n - 1) / max(1.0, (n - 2) * (n - 3) * 0.5)
    return min(p3a * p3b * p3c * correction, 0.99)


def compute_coverage_ev(horses: list, n: int, bet_type: str,
                         payout_rate: float = 0.75, odds_key: str = "_tansho") -> float:
    """EV≥100% の組み合わせの正規化出現率合計 (0.0〜1.0)"""
    if n < (2 if bet_type == "馬連" else 3):
        return 0.0

    raw_probs, raw_odds = [], []
    if bet_type == "馬連":
        for ha, hb in combinations(horses, 2):
            raw_probs.append(calc_umaren_prob(ha["place2_prob"], hb["place2_prob"], n))
            raw_odds.append(estimate_umaren_odds(ha[odds_key], hb[odds_key], n))
    else:
        for ha, hb, hc in combinations(horses, 3):
            raw_probs.append(calc_sanrenpuku_prob(ha["place3_prob"], hb["place3_prob"], hc["place3_prob"], n))
            raw_odds.append(estimate_sanrenpuku_odds(ha[odds_key], hb[odds_key], hc[odds_key], n))

    norm = sum(raw_probs)
    if norm <= 0:
        return 0.0

    total = sum(
        (rp / norm) for rp, od in zip(raw_probs, raw_odds)
        if (rp / norm) * od * payout_rate * 100 >= 100.0
    )
    return total


def compute_coverage_topn(horses: list, n: int, bet_type: str,
                           top_n: int = 5) -> float:
    """上位N点の正規化出現率合計 (0.0〜1.0)  ― EV不要版"""
    if n < (2 if bet_type == "馬連" else 3):
        return 0.0

    raw_probs = []
    if bet_type == "馬連":
        for ha, hb in combinations(horses, 2):
            raw_probs.append(calc_umaren_prob(ha["place2_prob"], hb["place2_prob"], n))
    else:
        for ha, hb, hc in combinations(horses, 3):
            raw_probs.append(calc_sanrenpuku_prob(ha["place3_prob"], hb["place3_prob"], hc["place3_prob"], n))

    norm = sum(raw_probs)
    if norm <= 0:
        return 0.0

    top = sorted(raw_probs, reverse=True)[:top_n]
    return sum(top) / norm


def _reblend_probs(horses: list, new_ml_w: float = 0.8):
    """
    stored place3_prob (Σ=3.0) と ml_place_prob (Σ=3.0) を直接線形補間して
    新ブレンド確率を書き戻す。両者とも正規化済みなので逆算不要。

    new_place3 = new_ml_w × ml_place_prob + (1-new_ml_w) × place3_prob
    ※ place3_prob はエンジンの旧ブレンド結果（Σ=3.0）
    ※ ml_place_prob は LightGBM単独出力（Σ=3.0）
    """
    rb_w = 1.0 - new_ml_w
    for h in horses:
        for (blended_key, ml_key) in [
            ("place2_prob", "ml_top2_prob"),
            ("place3_prob", "ml_place_prob"),
            ("win_prob",    "ml_win_prob"),
        ]:
            ml_val = h.get(ml_key)
            if ml_val is None:
                continue
            old_val = h.get(blended_key) or 0.0
            h[blended_key] = new_ml_w * ml_val + rb_w * old_val


def _calc_blend_tansho(horses: list, is_jra: bool = True, alpha: float = 0.72) -> None:
    """
    ブレンド後の win_prob → power law補正 → predicted_tansho_odds を再計算して書き戻す。
    """
    payout = 0.80
    raw = {}
    for h in horses:
        wp = h.get("win_prob") or 0.0
        raw[id(h)] = max(wp, 1e-6)

    total = sum(raw.values())
    normed = {k: v / total for k, v in raw.items()}
    adj = {k: v ** alpha for k, v in normed.items()}
    total_adj = sum(adj.values())
    calibrated = {k: v / total_adj for k, v in adj.items()}

    for h in horses:
        cp = calibrated.get(id(h), 0.0)
        h["_tansho"] = round(1.0 / cp * payout, 1) if cp > 0 else 999.9


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-horses", type=int, default=4, help="最少頭数フィルタ")
    parser.add_argument("--payout-rate", type=float, default=0.75,
                        help="払戻率 (default=0.75: JRA三連複)")
    args = parser.parse_args()

    pred_dir = PROJECT_ROOT / "data" / "predictions"
    files = sorted(pred_dir.glob("*_pred.json"))
    if not files:
        print("data/predictions/ に *_pred.json がありません")
        return

    # ---- ml_win_prob がある日だけブレンド比較可能
    BLEND_RATIOS = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]  # LightGBM比率
    blend_t5_s: dict[float, list[float]] = {r: [] for r in BLEND_RATIOS}  # 三連複
    blend_t5_u: dict[float, list[float]] = {r: [] for r in BLEND_RATIOS}  # 馬連
    blend_t5_sum: dict[float, list[float]] = {r: [] for r in BLEND_RATIOS}  # 三連複+馬連合計
    race_count_ml = 0

    results_t5_s:  list[float] = []  # 三連複 上位5点（全レース・現行）
    race_count = 0

    for f in files:
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"skip {f.name}: {e}")
            continue

        for race in d.get("races", []):
            horses = race.get("horses", [])
            n = race.get("field_count", len(horses))
            if n < args.min_horses:
                continue

            for h in horses:
                for key in ("place2_prob", "place3_prob", "win_prob"):
                    if not h.get(key):
                        h[key] = 0.0
                h["_tansho"] = float(h.get("predicted_tansho_odds") or 10.0)

            race_count += 1
            results_t5_s.append(compute_coverage_topn(horses, n, "三連複", 5) * 100)

            # ML有りレースのみブレンド比較
            has_ml = any(h.get("ml_place_prob") is not None for h in horses)
            if not has_ml:
                continue
            race_count_ml += 1

            import copy
            for ml_w in BLEND_RATIOS:
                h_copy = copy.deepcopy(horses)
                _reblend_probs(h_copy, new_ml_w=ml_w)
                _calc_blend_tansho(h_copy, alpha=0.72)
                s_val = compute_coverage_topn(h_copy, n, "三連複", 5) * 100
                u_val = compute_coverage_topn(h_copy, n, "馬連",   5) * 100
                blend_t5_s[ml_w].append(s_val)
                blend_t5_u[ml_w].append(u_val)
                blend_t5_sum[ml_w].append(s_val + u_val)

    if not results_t5_s:
        print("対象レースがありません")
        return

    def pct(lst, p):
        s = sorted(lst)
        idx = max(0, min(len(s)-1, int(len(s) * p / 100)))
        return s[idx]

    def stats(lst, label):
        n = len(lst)
        mean = sum(lst) / n
        print(f"\n{'='*46}")
        print(f"  {label}  (n={n}レース)")
        print(f"{'='*46}")
        print(f"  min      : {min(lst):6.1f}%")
        print(f"  P10      : {pct(lst, 10):6.1f}%")
        print(f"  P20      : {pct(lst, 20):6.1f}%")
        print(f"  P30      : {pct(lst, 30):6.1f}%")
        print(f"  P40      : {pct(lst, 40):6.1f}%")
        P50 = pct(lst, 50)
        print(f"  P50 (中央): {P50:6.1f}%")
        print(f"  P60      : {pct(lst, 60):6.1f}%")
        print(f"  P70      : {pct(lst, 70):6.1f}%")
        P80 = pct(lst, 80)
        print(f"  P80      : {P80:6.1f}%")
        P90 = pct(lst, 90)
        print(f"  P90      : {P90:6.1f}%")
        print(f"  P95      : {pct(lst, 95):6.1f}%")
        print(f"  max      : {max(lst):6.1f}%")
        print(f"  平均     : {mean:6.1f}%")
        print()

        # ヒストグラム (10%刻み)
        bins = list(range(0, 101, 10))
        print("  度数分布 (10%刻み):")
        for lo, hi in zip(bins, bins[1:]):
            cnt = sum(1 for v in lst if lo <= v < hi)
            cnt_hi = sum(1 for v in lst if v >= 100)
            bar = "█" * (cnt * 30 // max(1, n))
            print(f"    {lo:3d}%-{hi:3d}%: {cnt:4d}件  {bar}")
        if cnt_hi:
            print(f"    100%      : {cnt_hi:4d}件")

        # 推奨閾値案
        print()
        print("  【推奨閾値案 — 5段階等分位数ベース】")
        p20 = pct(lst, 20)
        p40 = pct(lst, 40)
        p60 = pct(lst, 60)
        p80 = pct(lst, 80)
        print(f"    SS : {p80:.0f}%以上     (上位20%)")
        print(f"    S  : {p60:.0f}%〜{p80:.0f}%未満  (上位20〜40%)")
        print(f"    A  : {p40:.0f}%〜{p60:.0f}%未満  (上位40〜60%)")
        print(f"    B  : {p20:.0f}%〜{p40:.0f}%未満  (上位60〜80%)")
        print(f"    C  : {p20:.0f}%未満      (下位20%)")

    print(f"\n対象ファイル: {len(files)}日分, 全対象レース: {race_count}件")
    print(f"うちML有りレース: {race_count_ml}件 (ブレンド比較対象)")
    print(f"払戻率: {args.payout_rate:.2f}, 最少頭数: {args.min_horses}頭以上")

    def blend_summary(blend_dict, label):
        if not blend_dict[0.6]:
            return
        print(f"\n{'='*72}")
        print(f"  {label}（ML有り {race_count_ml}レース）")
        print(f"{'='*72}")
        header = f"{'指標':<10}"
        for ml_w in BLEND_RATIOS:
            rb_w = round(1.0 - ml_w, 1)
            header += f"  {int(ml_w*100)}/{int(rb_w*100):>2}"
        print(header)
        print("-"*72)
        for lbl, p in [("中央値(P50)", 50), ("P70", 70), ("P80", 80), ("P90", 90), ("平均", -1)]:
            row = f"  {lbl:<8}"
            for ml_w in BLEND_RATIOS:
                lst = blend_dict[ml_w]
                v = pct(lst, p) if p >= 0 else sum(lst)/len(lst)
                row += f"  {v:6.1f}%"
            print(row)
        print()
        print(f"  {'閾値案':<10}", end="")
        for ml_w in BLEND_RATIOS:
            rb_w = round(1.0 - ml_w, 1)
            print(f"  {int(ml_w*100)}/{int(rb_w*100):>2}", end="")
        print()
        print("-"*72)
        for grade, lo_p, hi_p in [
            ("SS(上20%)",  80, 999),
            ("S (20-40%)", 60, 80),
            ("A (40-60%)", 40, 60),
            ("B (60-80%)", 20, 40),
            ("C (下20%)",  -1, 20),
        ]:
            row = f"  {grade:<10}"
            for ml_w in BLEND_RATIOS:
                lst = blend_dict[ml_w]
                if hi_p == 999:
                    cell = f"{pct(lst, lo_p):.0f}%+"
                elif lo_p == -1:
                    cell = f"<{pct(lst, hi_p):.0f}%"
                else:
                    cell = f"{pct(lst, lo_p):.0f}~{pct(lst, hi_p):.0f}%"
                row += f"  {cell:>8}"
            print(row)

    blend_summary(blend_t5_s,   "三連複 上位5点 ブレンド比較")
    blend_summary(blend_t5_u,   "馬連  上位5点 ブレンド比較")
    blend_summary(blend_t5_sum, "三連複+馬連 合計 ブレンド比較")

    # 全レース 上位5点
    stats(results_t5_s, "三連複 上位5点  [全レース・現行60/40]")


if __name__ == "__main__":
    main()
