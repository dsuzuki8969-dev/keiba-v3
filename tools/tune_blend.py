"""
ブレンド比率グリッドサーチ

pred JSON のブレンド済み確率からAI純粋確率を逆算し、
異なるalpha値で再ブレンドしてKPIを比較。最適ブレンド比率を探索する。

使い方:
  PYTHONIOENCODING=utf-8 python tools/tune_blend.py
  PYTHONIOENCODING=utf-8 python tools/tune_blend.py --org JRA
  PYTHONIOENCODING=utf-8 python tools/tune_blend.py --org NAR
"""

import io
import json
import glob
import os
import sys
from collections import defaultdict

import pandas as pd

# Windows utf-8 出力対応
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.calculator.popularity_blend import _lookup_rates, CONFIDENCE_GAP
from src.results_tracker import _get_fukusho_payout, _safe_tansho_payout
from tools.compare_pop_blend import unblend_race, load_stats, _pct

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
PRED_DIR = os.path.join(DATA_DIR, "predictions")
RESULT_DIR = os.path.join(DATA_DIR, "results")


# ============================================================
# 再ブレンド
# ============================================================

def reblend_race(ai_horses, venue, is_jra, field_count, stats,
                 alpha_min, alpha_max, conf_gap, org_ratio):
    """AI純粋確率を指定alphaで再ブレンドした確率リストを返す"""
    org = "JRA" if is_jra else "NAR"

    # AI純粋確率のgapで動的alpha計算
    all_wp = sorted([h["ai_win_prob"] for h in ai_horses], reverse=True)
    gap = (all_wp[0] - all_wp[1]) if len(all_wp) >= 2 else 0
    confidence = min(1.0, gap / conf_gap)
    alpha_model = alpha_min + confidence * (alpha_max - alpha_min)
    alpha_stats = 1.0 - alpha_model
    alpha_org = alpha_stats * org_ratio
    alpha_venue = alpha_stats * (1.0 - org_ratio)

    result = []
    for h in ai_horses:
        pop = h.get("popularity")
        odds = h.get("odds")
        ai_wp = h["ai_win_prob"]
        ai_p2 = h["ai_place2_prob"]
        ai_p3 = h["ai_place3_prob"]

        if pop is not None and pop >= 1:
            ow, ot2, ot3, vw, vt2, vt3 = _lookup_rates(
                stats, org, venue, pop, odds, field_count
            )
            new_wp = alpha_model * ai_wp + alpha_org * ow + alpha_venue * vw
            new_p2 = alpha_model * ai_p2 + alpha_org * ot2 + alpha_venue * vt2
            new_p3 = alpha_model * ai_p3 + alpha_org * ot3 + alpha_venue * vt3
        else:
            new_wp, new_p2, new_p3 = ai_wp, ai_p2, ai_p3

        result.append({
            **h,
            "rb_win_prob": max(0.001, new_wp),
            "rb_place2_prob": max(0.001, new_p2),
            "rb_place3_prob": max(0.001, new_p3),
        })

    # 正規化
    n = len(result)
    for key, target in [("rb_win_prob", 1.0), ("rb_place2_prob", min(2.0, n)), ("rb_place3_prob", min(3.0, n))]:
        total = sum(r[key] for r in result)
        if total > 0:
            for r in result:
                r[key] = r[key] / total * target
    return result


# ============================================================
# KPI計算
# ============================================================

def evaluate_params(races_cache, params):
    """1パラメータセットで全レース評価 → KPI dict"""
    alpha_min = params["alpha_min"]
    alpha_max = params["alpha_max"]
    conf_gap = params.get("conf_gap", CONFIDENCE_GAP)
    org_ratio = params.get("org_ratio", 0.4)

    top1_win = 0
    top1_total = 0
    sanren_hit = 0
    sanren_total = 0
    sanren_stake = 0
    sanren_ret = 0
    brier_sum = 0.0
    brier_n = 0
    honmei_win = 0
    honmei_total = 0

    for race in races_cache:
        ai_horses = race["ai_horses"]
        stats = race["stats"]
        finish_map = race["finish_map"]
        payouts = race["payouts"]
        order = race["order"]

        # 再ブレンド
        rb = reblend_race(
            ai_horses, race["venue"], race["is_jra"],
            race["field_count"], stats,
            alpha_min, alpha_max, conf_gap, org_ratio,
        )

        # ソート
        rb_sorted = sorted(rb, key=lambda x: x["rb_win_prob"], reverse=True)
        actual_top3 = set(o["horse_no"] for o in order if o["finish"] <= 3)
        winner_hno = next((o["horse_no"] for o in order if o["finish"] == 1), 0)

        # Top1
        top1_total += 1
        if finish_map.get(rb_sorted[0]["horse_no"]) == 1:
            top1_win += 1

        # ◎勝率
        honmei_total += 1
        if finish_map.get(rb_sorted[0]["horse_no"]) == 1:
            honmei_win += 1

        # 三連複BOX TOP5
        top5 = set(h["horse_no"] for h in rb_sorted[:5])
        sanren_total += 1
        sanren_stake += 1000
        if actual_top3.issubset(top5):
            sanren_hit += 1
            sp = payouts.get("三連複", {})
            payout_val = sp.get("payout", 0) if isinstance(sp, dict) else 0
            sanren_ret += payout_val

        # Brier Score
        for h in rb:
            wp = h["rb_win_prob"]
            actual = 1 if finish_map.get(h["horse_no"]) == 1 else 0
            brier_sum += (wp - actual) ** 2
            brier_n += 1

    return {
        "top1_pct": _pct(top1_win, top1_total),
        "sanren_hit_pct": _pct(sanren_hit, sanren_total),
        "sanren_roi": _pct(sanren_ret, sanren_stake),
        "brier": brier_sum / brier_n if brier_n else 0,
        "honmei_pct": _pct(honmei_win, honmei_total),
        "races": top1_total,
    }


def calc_score(kpi):
    """総合スコア（高いほど良い）"""
    return (
        (kpi["top1_pct"] / 40.0) * 0.20
        + (kpi["sanren_hit_pct"] / 50.0) * 0.20
        + min(kpi["sanren_roi"] / 150.0, 1.0) * 0.25
        + max(0, 1.0 - kpi["brier"] / 0.10) * 0.20
        + (kpi["honmei_pct"] / 40.0) * 0.15
    )


# ============================================================
# メイン
# ============================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="ブレンド比率グリッドサーチ")
    parser.add_argument("--start", default="20260101", help="開始日 YYYYMMDD")
    parser.add_argument("--end", default="20260316", help="終了日 YYYYMMDD")
    parser.add_argument("--org", choices=["ALL", "JRA", "NAR"], default=None,
                        help="JRA/NARフィルタ")
    args = parser.parse_args()

    pd.set_option("display.unicode.east_asian_width", True)

    stats = load_stats()
    if not stats:
        print("人気別統計テーブルが見つかりません")
        return

    pred_files = sorted(glob.glob(os.path.join(PRED_DIR, "2026*_pred.json")))
    pred_files = [f for f in pred_files if args.start <= os.path.basename(f)[:8] <= args.end]

    print(f"対象期間: {args.start} ～ {args.end}")
    if args.org:
        print(f"フィルタ: {args.org}")
    print(f"予測ファイル数: {len(pred_files)}")

    # --- フェーズ1: 全レース読み込み + unblend ---
    print("データ読み込み + AI純粋確率の逆算中...", flush=True)
    races_cache = []

    for pf in pred_files:
        date_key = os.path.basename(pf)[:8]
        result_path = os.path.join(RESULT_DIR, f"{date_key}_results.json")
        if not os.path.exists(result_path):
            continue

        with open(pf, "r", encoding="utf-8") as f:
            pred_data = json.load(f)
        with open(result_path, "r", encoding="utf-8") as f:
            result_data = json.load(f)

        for race in pred_data.get("races", []):
            race_id = race.get("race_id", "")
            horses = race.get("horses", [])
            venue = race.get("venue", "")
            is_jra = race.get("is_jra", False)
            field_count = race.get("field_count", len(horses))

            if not horses or not race_id:
                continue

            org = "JRA" if is_jra else "NAR"
            if args.org and args.org != "ALL" and org != args.org:
                continue

            res = result_data.get(race_id, {})
            if not res or "order" not in res:
                continue

            order = res["order"]
            payouts = res.get("payouts", {})
            finish_map = {o["horse_no"]: o["finish"] for o in order}
            valid = [h for h in horses if h.get("horse_no") in finish_map]
            if len(valid) < 3:
                continue

            # AI純粋確率を逆算（現在のパラメータで）
            ai_horses = unblend_race(valid, venue, is_jra, field_count, stats)

            races_cache.append({
                "ai_horses": ai_horses,
                "stats": stats,
                "venue": venue,
                "is_jra": is_jra,
                "field_count": field_count,
                "finish_map": finish_map,
                "payouts": payouts,
                "order": order,
            })

    print(f"対象レース数: {len(races_cache):,}")

    # --- フェーズ2: 静的alphaグリッドサーチ ---
    print("\n■ 静的alpha グリッドサーチ（alpha_min = alpha_max）")
    print("─" * 80)

    static_alphas = [0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]
    rows_static = []
    for alpha in static_alphas:
        params = {"alpha_min": alpha, "alpha_max": alpha, "org_ratio": 0.4}
        kpi = evaluate_params(races_cache, params)
        score = calc_score(kpi)
        label = ""
        if alpha == 0.50:
            label = "←現MIN"
        elif alpha == 0.70:
            label = "←現MAX"
        elif alpha == 1.00:
            label = "←AI純粋"
        rows_static.append({
            "alpha": f"{alpha:.2f}",
            "Top1": f"{kpi['top1_pct']:.1f}%",
            "三連複率": f"{kpi['sanren_hit_pct']:.1f}%",
            "三連複ROI": f"{kpi['sanren_roi']:.0f}%",
            "Brier": f"{kpi['brier']:.5f}",
            "◎勝率": f"{kpi['honmei_pct']:.1f}%",
            "スコア": f"{score:.3f}",
            "": label,
        })

    df_static = pd.DataFrame(rows_static)
    print(df_static.to_string(index=False))

    # --- フェーズ3: 動的alphaグリッドサーチ ---
    print(f"\n■ 動的alpha グリッドサーチ（alpha_min < alpha_max）")
    print("─" * 80)

    alpha_mins = [0.30, 0.40, 0.50, 0.60]
    alpha_maxs = [0.60, 0.70, 0.80, 0.90]
    org_ratios = [0.3, 0.4, 0.5]

    all_dynamic = []
    total_combos = sum(1 for mn in alpha_mins for mx in alpha_maxs if mn < mx) * len(org_ratios)
    done = 0

    for mn in alpha_mins:
        for mx in alpha_maxs:
            if mn >= mx:
                continue
            for oratio in org_ratios:
                done += 1
                if done % 5 == 0:
                    print(f"  [{done}/{total_combos}]...", flush=True)
                params = {"alpha_min": mn, "alpha_max": mx,
                          "conf_gap": CONFIDENCE_GAP, "org_ratio": oratio}
                kpi = evaluate_params(races_cache, params)
                score = calc_score(kpi)
                all_dynamic.append({
                    "params": params,
                    "kpi": kpi,
                    "score": score,
                })

    # スコア順にソート
    all_dynamic.sort(key=lambda x: x["score"], reverse=True)

    rows_dyn = []
    for i, entry in enumerate(all_dynamic[:15]):
        p = entry["params"]
        k = entry["kpi"]
        label = "★BEST" if i == 0 else ""
        # 現在値チェック
        if p["alpha_min"] == 0.50 and p["alpha_max"] == 0.70 and p["org_ratio"] == 0.4:
            label = "←現在値"
        rows_dyn.append({
            "MIN": f"{p['alpha_min']:.2f}",
            "MAX": f"{p['alpha_max']:.2f}",
            "org比": f"{p['org_ratio']:.1f}",
            "Top1": f"{k['top1_pct']:.1f}%",
            "三連複率": f"{k['sanren_hit_pct']:.1f}%",
            "三連複ROI": f"{k['sanren_roi']:.0f}%",
            "Brier": f"{k['brier']:.5f}",
            "◎勝率": f"{k['honmei_pct']:.1f}%",
            "スコア": f"{entry['score']:.3f}",
            "": label,
        })

    df_dyn = pd.DataFrame(rows_dyn)
    print(f"\n上位15件（全{total_combos}パターン）:")
    print(df_dyn.to_string(index=False))

    # --- フェーズ4: 現在値との比較 ---
    print(f"\n{'='*80}")
    print("■ 現在値 vs 最適値 比較")
    print(f"{'='*80}")

    # 現在値
    current_params = {"alpha_min": 0.50, "alpha_max": 0.70, "org_ratio": 0.4}
    current_kpi = evaluate_params(races_cache, current_params)
    current_score = calc_score(current_kpi)

    best = all_dynamic[0]
    bp = best["params"]
    bk = best["kpi"]

    rows_cmp = [
        {"設定": "現在値(MIN=0.50,MAX=0.70)",
         "Top1": f"{current_kpi['top1_pct']:.1f}%",
         "三連複率": f"{current_kpi['sanren_hit_pct']:.1f}%",
         "三連複ROI": f"{current_kpi['sanren_roi']:.0f}%",
         "Brier": f"{current_kpi['brier']:.5f}",
         "スコア": f"{current_score:.3f}"},
        {"設定": f"最適値(MIN={bp['alpha_min']:.2f},MAX={bp['alpha_max']:.2f},org={bp['org_ratio']:.1f})",
         "Top1": f"{bk['top1_pct']:.1f}%",
         "三連複率": f"{bk['sanren_hit_pct']:.1f}%",
         "三連複ROI": f"{bk['sanren_roi']:.0f}%",
         "Brier": f"{bk['brier']:.5f}",
         "スコア": f"{best['score']:.3f}"},
        {"設定": "差分",
         "Top1": f"{bk['top1_pct'] - current_kpi['top1_pct']:+.1f}%",
         "三連複率": f"{bk['sanren_hit_pct'] - current_kpi['sanren_hit_pct']:+.1f}%",
         "三連複ROI": f"{bk['sanren_roi'] - current_kpi['sanren_roi']:+.0f}%",
         "Brier": f"{bk['brier'] - current_kpi['brier']:+.5f}",
         "スコア": f"{best['score'] - current_score:+.3f}"},
    ]
    df_cmp = pd.DataFrame(rows_cmp)
    print(df_cmp.to_string(index=False))


if __name__ == "__main__":
    main()
