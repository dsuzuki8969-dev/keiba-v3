# -*- coding: utf-8 -*-
"""D-1d: 案 5 (戦略 A 合致 JRA 限定) 深掘り検証 + 採用判断

D-1c 結論: 案 5 JRA 限定 ROI 195.9% が現運用 ◎単勝 (案 4) JRA 183.5% に対して +12pt
案 5 の定義: 戦略 A (composite TOP2) と shobu TOP2 の一致馬 (二重一致)

本 diag では採用判断のため以下を検証:
  (1) 期間別 (2024/2025/2026 年別)
  (2) 月別 (各案で月次 ROI 標準偏差)
  (3) 会場別 (JRA 10 場別)
  (4) confidence 別 (SS/S/A/B/C/D)
  (5) サンプル数 + ブートストラップ ROI 95% 信頼区間
  (6) ◎単勝 (案 4) JRA 限定との直接比較
  (7) 案 5 でも単勝 1 点だけにする派生 (overlap & marked のみ など)

D-1b 教訓反映: 短期サンプル分散端問題回避のためブートストラップ CI 必須。
"""
from __future__ import annotations
import json
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.payout_normalizer import normalize_payouts
from data.masters.venue_master import JRA_CODES, get_venue_name

PRED_DIR = PROJECT_ROOT / "data" / "predictions"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"

MARKED = {"◉", "◎", "○", "▲", "△", "★", "☆"}
HONMEI = {"◉", "◎"}


def _get_tansho_payout(payouts: dict, winner_hno: int) -> int:
    normalized = normalize_payouts(payouts)
    tansho_list = normalized.get("tansho", []) or normalized.get("単勝", [])
    if isinstance(tansho_list, dict):
        tansho_list = [tansho_list]
    for entry in tansho_list:
        combo = str(entry.get("combo", ""))
        try:
            combo_int = int(combo)
        except (ValueError, TypeError):
            continue
        if combo_int == winner_hno:
            return int(entry.get("payout", 0) or 0)
    return 0


def _bootstrap_roi_ci(samples: list[tuple[int, int]], n_resample: int = 1000, seed: int = 42) -> tuple[float, float, float]:
    """各 race の (bet, pay) リストから ROI 95% 信頼区間を計算。

    Returns: (lower_2.5%, median, upper_97.5%)
    """
    if not samples:
        return (0.0, 0.0, 0.0)
    rng = random.Random(seed)
    n = len(samples)
    rois = []
    for _ in range(n_resample):
        total_bet = 0
        total_pay = 0
        for _ in range(n):
            idx = rng.randrange(n)
            total_bet += samples[idx][0]
            total_pay += samples[idx][1]
        if total_bet > 0:
            rois.append(total_pay / total_bet * 100)
    if not rois:
        return (0.0, 0.0, 0.0)
    rois.sort()
    return (rois[int(n_resample * 0.025)], rois[n_resample // 2], rois[int(n_resample * 0.975)])


def _race_pick_bet_pay(race: dict, top1: int, tansho_pay: int, plan: str) -> tuple[int, int]:
    """plan に応じて (bet, pay) を返す。買わなければ (0, 0)。"""
    horses = race.get("horses", [])
    active = [h for h in horses
              if not h.get("is_scratched") and (h.get("shobu_score") or 0) > 0]
    if len(active) < 2:
        return (0, 0)

    by_shobu = sorted(active, key=lambda h: h.get("shobu_score", 0), reverse=True)
    by_comp = sorted(active, key=lambda h: h.get("composite", 0), reverse=True)

    shobu_top2 = by_shobu[:2]
    comp_top2_nos = {int(h.get("horse_no", 0)) for h in by_comp[:2]}

    if plan == "plan4_honmei":
        honmei = next((h for h in active if h.get("mark") in HONMEI), None)
        picks = [honmei] if honmei else []
    elif plan == "plan5_overlap":
        picks = [h for h in shobu_top2 if int(h.get("horse_no", 0)) in comp_top2_nos]
    elif plan == "plan5_overlap_marked":
        # 案 5 派生: overlap かつ印付きのみ
        picks = [h for h in shobu_top2
                 if int(h.get("horse_no", 0)) in comp_top2_nos and h.get("mark") in MARKED]
    elif plan == "plan5_overlap_honmei":
        # 案 5 派生: overlap に ◎ が含まれる時のみ ◎ 1 点
        overlap = [h for h in shobu_top2 if int(h.get("horse_no", 0)) in comp_top2_nos]
        honmei = next((h for h in overlap if h.get("mark") in HONMEI), None)
        picks = [honmei] if honmei else []
    else:
        return (0, 0)

    if not picks:
        return (0, 0)

    pick_nos = [int(h.get("horse_no", 0)) for h in picks]
    bet = 100 * len(picks)
    pay = tansho_pay if (top1 in pick_nos and tansho_pay > 0) else 0
    return (bet, pay)


def main():
    plans = ["plan4_honmei", "plan5_overlap", "plan5_overlap_marked", "plan5_overlap_honmei"]
    plan_labels = {
        "plan4_honmei":          "案 4: ◎単勝 1 点 (現運用)",
        "plan5_overlap":         "案 5: 戦略A合致 (二重一致)",
        "plan5_overlap_marked":  "派 5a: 二重一致 ∩ 印付き",
        "plan5_overlap_honmei":  "派 5b: 二重一致 ∩ ◎ のみ",
    }

    # 集計用構造
    # samples[plan][category] = [(bet, pay), ...]
    samples: dict[str, dict[str, list[tuple[int, int]]]] = {
        p: defaultdict(list) for p in plans
    }

    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    total_files = len(pred_files)
    print(f"対象 pred_files: {total_files} 日分", file=sys.stderr)

    for i, pf in enumerate(pred_files):
        if i % 200 == 0:
            print(f"  進捗 {i}/{total_files} ({i*100//max(total_files,1)}%)", file=sys.stderr)
        date_str = pf.name.replace("_pred.json", "")
        rp = RESULTS_DIR / f"{date_str}_results.json"
        if not rp.exists():
            continue
        try:
            results = json.loads(rp.read_text(encoding="utf-8"))
            pred = json.loads(pf.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(results, dict):
            continue

        # 年 / 月
        year = date_str[:4]
        month = date_str[:7]  # YYYY-MM

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id or len(race_id) < 6:
                continue
            vc = race_id[4:6]
            if vc not in JRA_CODES:
                continue  # JRA 限定

            r_result = results.get(race_id)
            if not r_result or not r_result.get("order"):
                continue

            top1 = None
            for o in r_result["order"]:
                fin = o.get("finish", o.get("finish_pos", 99))
                if fin == 1:
                    top1 = o.get("horse_no")
                    break
            if top1 is None:
                continue

            tansho_pay = _get_tansho_payout(r_result.get("payouts", {}), top1)
            conf = race.get("confidence", "") or "?"

            for plan in plans:
                bet, pay = _race_pick_bet_pay(race, top1, tansho_pay, plan)
                if bet == 0:
                    continue
                samples[plan]["all"].append((bet, pay))
                samples[plan][f"year={year}"].append((bet, pay))
                samples[plan][f"month={month}"].append((bet, pay))
                samples[plan][f"vc={vc}"].append((bet, pay))
                samples[plan][f"conf={conf}"].append((bet, pay))

    print("\n", file=sys.stderr)

    # ========== 出力 (1) ALL ==========
    print("=" * 100)
    print("【1】JRA 全期間 ALL (ブートストラップ 95% 信頼区間 CI)")
    print("=" * 100)
    print(f"{'案':<26} | {'races':>6} {'bet':>10} {'pay':>10} {'hits':>5} {'hit%':>5} {'ROI':>7} {'CI 95%':>22}")
    print("-" * 100)
    for plan in plans:
        s = samples[plan]["all"]
        if not s:
            continue
        bet = sum(x[0] for x in s)
        pay = sum(x[1] for x in s)
        hits = sum(1 for x in s if x[1] > 0)
        races = len(s)
        roi = pay / bet * 100 if bet else 0
        hit_rate = hits / races * 100 if races else 0
        ci_lo, ci_md, ci_hi = _bootstrap_roi_ci(s)
        print(f"{plan_labels[plan]:<26} | {races:>6,} {bet:>10,} {pay:>10,} {hits:>5,} {hit_rate:>4.1f}% {roi:>6.1f}% [{ci_lo:>6.1f}, {ci_hi:>6.1f}]")

    # ========== 出力 (2) 年別 ==========
    print()
    print("=" * 100)
    print("【2】年別 (案 4 ◎単勝 vs 案 5 二重一致)")
    print("=" * 100)
    print(f"{'年':<6} | {'案':<26} | {'races':>6} {'bet':>10} {'pay':>10} {'ROI':>7} {'CI 95%':>22}")
    print("-" * 100)
    for year in ["2024", "2025", "2026"]:
        for plan in ["plan4_honmei", "plan5_overlap"]:
            s = samples[plan][f"year={year}"]
            if not s:
                continue
            bet = sum(x[0] for x in s)
            pay = sum(x[1] for x in s)
            roi = pay / bet * 100 if bet else 0
            ci_lo, _, ci_hi = _bootstrap_roi_ci(s)
            print(f"{year:<6} | {plan_labels[plan]:<26} | {len(s):>6,} {bet:>10,} {pay:>10,} {roi:>6.1f}% [{ci_lo:>6.1f}, {ci_hi:>6.1f}]")
        print()

    # ========== 出力 (3) 月別 ROI 標準偏差 ==========
    print("=" * 100)
    print("【3】月別 ROI 標準偏差 (D-1b 教訓: 短期分散端問題のチェック)")
    print("=" * 100)
    print(f"{'案':<26} | {'月数':>4} {'mean ROI':>9} {'median':>8} {'std':>7} {'min':>7} {'max':>7} {'< 100% 月数':>12}")
    print("-" * 100)
    for plan in ["plan4_honmei", "plan5_overlap"]:
        monthly_rois = []
        months_under_100 = 0
        for key in sorted(samples[plan].keys()):
            if not key.startswith("month="):
                continue
            s = samples[plan][key]
            bet = sum(x[0] for x in s)
            pay = sum(x[1] for x in s)
            if bet == 0:
                continue
            roi = pay / bet * 100
            monthly_rois.append(roi)
            if roi < 100:
                months_under_100 += 1
        if not monthly_rois:
            continue
        mean_r = statistics.mean(monthly_rois)
        median_r = statistics.median(monthly_rois)
        std_r = statistics.stdev(monthly_rois) if len(monthly_rois) > 1 else 0
        min_r = min(monthly_rois)
        max_r = max(monthly_rois)
        print(f"{plan_labels[plan]:<26} | {len(monthly_rois):>4} {mean_r:>8.1f}% {median_r:>7.1f}% {std_r:>6.1f}% {min_r:>6.1f}% {max_r:>6.1f}% {months_under_100:>4}/{len(monthly_rois)}")

    # ========== 出力 (4) 会場別 ==========
    print()
    print("=" * 100)
    print("【4】会場別 (JRA 10 場別 / 案 5 vs 案 4)")
    print("=" * 100)
    print(f"{'場':<10} | {'案':<26} | {'races':>6} {'bet':>10} {'pay':>10} {'ROI':>7}")
    print("-" * 100)
    for vc in sorted(JRA_CODES):
        vname = get_venue_name(vc) or vc
        for plan in ["plan4_honmei", "plan5_overlap"]:
            s = samples[plan][f"vc={vc}"]
            if not s:
                continue
            bet = sum(x[0] for x in s)
            pay = sum(x[1] for x in s)
            roi = pay / bet * 100 if bet else 0
            print(f"{vname:<10} | {plan_labels[plan]:<26} | {len(s):>6,} {bet:>10,} {pay:>10,} {roi:>6.1f}%")
        print()

    # ========== 出力 (5) confidence 別 ==========
    print("=" * 100)
    print("【5】confidence 別 (案 5 vs 案 4)")
    print("=" * 100)
    print(f"{'conf':<6} | {'案':<26} | {'races':>6} {'bet':>10} {'pay':>10} {'ROI':>7}")
    print("-" * 100)
    for conf in ["SS", "S", "A", "B", "C", "D", "?"]:
        for plan in ["plan4_honmei", "plan5_overlap"]:
            s = samples[plan][f"conf={conf}"]
            if not s:
                continue
            bet = sum(x[0] for x in s)
            pay = sum(x[1] for x in s)
            roi = pay / bet * 100 if bet else 0
            print(f"{conf:<6} | {plan_labels[plan]:<26} | {len(s):>6,} {bet:>10,} {pay:>10,} {roi:>6.1f}%")
        print()

    # ========== 採用判断サマリ ==========
    print("=" * 100)
    print("【6】採用判断サマリ")
    print("=" * 100)
    s4 = samples["plan4_honmei"]["all"]
    s5 = samples["plan5_overlap"]["all"]
    if s4 and s5:
        bet4, pay4 = sum(x[0] for x in s4), sum(x[1] for x in s4)
        bet5, pay5 = sum(x[0] for x in s5), sum(x[1] for x in s5)
        roi4 = pay4 / bet4 * 100 if bet4 else 0
        roi5 = pay5 / bet5 * 100 if bet5 else 0
        ci4_lo, _, ci4_hi = _bootstrap_roi_ci(s4)
        ci5_lo, _, ci5_hi = _bootstrap_roi_ci(s5)
        print(f"案 4 ◎単勝 JRA  : ROI {roi4:.1f}%  CI [{ci4_lo:.1f}, {ci4_hi:.1f}]  races {len(s4):,}")
        print(f"案 5 二重一致 JRA: ROI {roi5:.1f}%  CI [{ci5_lo:.1f}, {ci5_hi:.1f}]  races {len(s5):,}")
        print(f"差分             : +{roi5-roi4:.1f}pt")
        # CI overlap 判定
        if ci5_lo > ci4_hi:
            print("→ 案 5 が案 4 を 95% CI で完全に上回る (採用推奨)")
        elif ci5_lo > roi4:
            print("→ 案 5 下限 > 案 4 中央値 (採用候補)")
        elif ci4_hi > roi5:
            print("→ CI が重なり統計的有意でない (採用見送り推奨)")
        else:
            print("→ CI 重複あり (慎重判断要)")


if __name__ == "__main__":
    main()
