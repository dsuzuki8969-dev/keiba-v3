# -*- coding: utf-8 -*-
"""D-1c: 戦略 B (shobu_score TOP2) の改善策候補を ROI 試算付きで比較

D-1b 判明: TOP2 印付き 0 (6.8%) + 1 名 (36.1%) = 42.9% が「無印馬を買う」状態
→ LIVE STATS (◎単勝) との不一致発生 (累犯 #14)

改善策候補 5 つを全期間で集計:
  案 0: 現状 (TOP2 全件)                       — 基準
  案 1: TOP2 のうち印付き馬のみ (印フィルタ)
  案 2: TOP2 + shobu_score >= 5.0 閾値        (絶対値フィルタ)
  案 3: 印付き優先 + 不足を補完 (◎○▲△ の shobu TOP1 + shobu TOP2)
  案 4: ◎単勝 1 点のみ (現状運用)
  案 5: 戦略 A (composite TOP2) との一致馬のみ (二重一致)

集計式 (実装と一致):
  - stake = 馬数 × 100
  - payout = 該当馬が 1 着なら単勝払戻 × 1 単位
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.payout_normalizer import normalize_payouts
from data.masters.venue_master import JRA_CODES

PRED_DIR = PROJECT_ROOT / "data" / "predictions"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"

MARKED = {"◉", "◎", "○", "▲", "△", "★", "☆"}


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


def main():
    """全期間 JRA + NAR で 各案 ROI 計算"""
    # 各案ごとに (bet, pay, races, hits) を集計
    plans = ["plan0_top2_all", "plan1_marked_only", "plan2_shobu5plus",
             "plan3_marked_first", "plan4_honmei_only", "plan5_strategy_a_overlap"]
    stats = {p: {"jra": [0, 0, 0, 0], "nar": [0, 0, 0, 0]} for p in plans}

    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    for pf in pred_files:
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

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id or len(race_id) < 6:
                continue
            vc = race_id[4:6]
            is_jra = vc in JRA_CODES
            cat_key = "jra" if is_jra else "nar"

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

            horses = race.get("horses", [])
            active = [h for h in horses
                      if not h.get("is_scratched") and (h.get("shobu_score") or 0) > 0]
            if len(active) < 2:
                continue

            by_shobu = sorted(active, key=lambda h: h.get("shobu_score", 0), reverse=True)
            by_comp = sorted(active, key=lambda h: h.get("composite", 0), reverse=True)

            shobu_top2 = by_shobu[:2]
            shobu_top2_nos = [int(h.get("horse_no", 0)) for h in shobu_top2]

            # 印付き判定
            shobu_top2_marked = [h for h in shobu_top2 if h.get("mark") in MARKED]

            # 単勝払戻取得 (1 着馬)
            tansho_pay = _get_tansho_payout(r_result.get("payouts", {}), top1)

            def _accum(plan_key: str, picks: list):
                """picks 馬を 100 円ずつ買う集計"""
                if not picks:
                    return
                pick_nos = [int(h.get("horse_no", 0)) for h in picks]
                s = stats[plan_key][cat_key]
                s[0] += 100 * len(picks)            # bet
                s[2] += 1                            # races
                if top1 in pick_nos and tansho_pay > 0:
                    s[1] += tansho_pay              # pay (1 着馬 1 馬分の単勝)
                    s[3] += 1                       # hits

            # 案 0: TOP2 全件
            _accum("plan0_top2_all", shobu_top2)

            # 案 1: TOP2 のうち印付きのみ
            _accum("plan1_marked_only", shobu_top2_marked)

            # 案 2: TOP2 + shobu_score >= 5.0
            picks2 = [h for h in shobu_top2 if (h.get("shobu_score") or 0) >= 5.0]
            _accum("plan2_shobu5plus", picks2)

            # 案 3: 印付き優先 + 不足を補完
            #   印付き馬の shobu TOP1 を取り、不足分を shobu TOP2 から補う
            marked_active = [h for h in active if h.get("mark") in MARKED]
            marked_by_shobu = sorted(marked_active, key=lambda h: h.get("shobu_score", 0), reverse=True)
            picks3 = marked_by_shobu[:2]
            if len(picks3) < 2:
                for h in shobu_top2:
                    if h not in picks3:
                        picks3.append(h)
                        if len(picks3) == 2:
                            break
            _accum("plan3_marked_first", picks3[:2])

            # 案 4: ◎単勝 1 点
            honmei = next((h for h in active if h.get("mark") in ("◉", "◎")), None)
            _accum("plan4_honmei_only", [honmei] if honmei else [])

            # 案 5: 戦略 A (composite TOP2) と shobu TOP2 の一致馬
            comp_top2_nos = {int(h.get("horse_no", 0)) for h in by_comp[:2]}
            picks5 = [h for h in shobu_top2 if int(h.get("horse_no", 0)) in comp_top2_nos]
            _accum("plan5_strategy_a_overlap", picks5)

    # 結果出力
    print("=== D-1c: 戦略 B 改善策候補 ROI 試算 ===\n")
    print(f"{'案':<28} | {'区':>3} {'races':>6} {'bet':>10} {'pay':>10} {'hits':>5} {'hit%':>5} {'ROI':>6}")
    print("-" * 88)
    plan_labels = {
        "plan0_top2_all":            "案 0: TOP2 全件 (現状基準)",
        "plan1_marked_only":         "案 1: TOP2 印付きのみ",
        "plan2_shobu5plus":          "案 2: TOP2 + shobu>=5.0",
        "plan3_marked_first":        "案 3: 印付き優先 + 補完",
        "plan4_honmei_only":         "案 4: ◎単勝 1 点 (現運用)",
        "plan5_strategy_a_overlap":  "案 5: 戦略A合致 (二重一致)",
    }
    for pk, label in plan_labels.items():
        for cat in ("jra", "nar"):
            bet, pay, races, hits = stats[pk][cat]
            if bet == 0:
                continue
            hit_rate = hits / races * 100 if races else 0
            roi = pay / bet * 100
            print(f"{label:<28} | {cat:>3} {races:>6,} {bet:>10,} {pay:>10,} {hits:>5,} {hit_rate:>4.1f}% {roi:>5.1f}%")
        print()


if __name__ == "__main__":
    main()
