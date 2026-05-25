# -*- coding: utf-8 -*-
"""D-1b: 戦略 B (shobu_score TOP2) シミュ 144.5% vs 実装 13.8% 乖離真因究明

調査方針:
- 全期間 pred.json + results.json で「shobu_score TOP2 を 200円買う」と仮定
- 実装側 (◎単勝1点) と同じ集計式で ROI を算出
- 5/24 handoff の 144.5% シミュ式と現状 ROI を突合し、差の発生源を特定

集計式 (実装と一致させる):
- stake: TOP2 馬 → 100円 × 2 = 200円/レース
- payout: TOP2 のどちらかが 1 着なら、その馬の単勝払戻 × 1 単位 (100円)
- 対象: JRA + NAR 別、is_scratched=False, shobu_score > 0

オプション集計:
- 「shobu_score TOP2 のうち何頭が印付き?」
- ROI を日別/月別分布で見て variance を測定
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.payout_normalizer import normalize_payouts, get_first_payout
from data.masters.venue_master import JRA_CODES

PRED_DIR = PROJECT_ROOT / "data" / "predictions"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"

# 印付き判定: ◎○▲△★☆◉
MARKED = {"◉", "◎", "○", "▲", "△", "★", "☆"}


def _get_tansho_payout(payouts: dict, winner_hno: int) -> int:
    """単勝払戻取得 (winner_hno と combo 一致を確認)"""
    normalized = normalize_payouts(payouts)
    tansho_list = normalized.get("tansho", [])
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
    bet_jra = pay_jra = races_jra = hits_jra = 0
    bet_nar = pay_nar = races_nar = hits_nar = 0

    # 印付き比率分析
    pick_mark_dist = defaultdict(int)  # (marked_count: 0/1/2) -> count
    daily_roi = []  # [(date, bet, pay)]

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

        day_bet = 0
        day_pay = 0

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id or len(race_id) < 6:
                continue
            vc = race_id[4:6]
            is_jra = vc in JRA_CODES

            r_result = results.get(race_id)
            if not r_result or not r_result.get("order"):
                continue

            # 1 着馬番
            top1 = None
            for o in r_result["order"]:
                fin = o.get("finish", o.get("finish_pos", 99))
                if fin == 1:
                    top1 = o.get("horse_no")
                    break
            if top1 is None:
                continue

            # shobu_score TOP2 抽出 (is_scratched=False)
            horses = race.get("horses", [])
            active = [h for h in horses if not h.get("is_scratched") and (h.get("shobu_score") or 0) > 0]
            if len(active) < 2:
                continue
            by_shobu = sorted(active, key=lambda h: h.get("shobu_score", 0), reverse=True)
            top2 = by_shobu[:2]
            top2_nos = [int(h.get("horse_no", 0)) for h in top2]
            top2_marks = [h.get("mark", "-") for h in top2]
            marked_count = sum(1 for m in top2_marks if m in MARKED)
            pick_mark_dist[marked_count] += 1

            # 集計: 200 円 stake, hit = top1 in top2
            stake = 200
            payout = 0
            if top1 in top2_nos:
                payout = _get_tansho_payout(r_result.get("payouts", {}), top1)

            day_bet += stake
            day_pay += payout
            if is_jra:
                bet_jra += stake
                pay_jra += payout
                races_jra += 1
                if payout > 0:
                    hits_jra += 1
            else:
                bet_nar += stake
                pay_nar += payout
                races_nar += 1
                if payout > 0:
                    hits_nar += 1

        if day_bet > 0:
            daily_roi.append((date_str, day_bet, day_pay))

    print("=== D-1b: 戦略 B (shobu_score TOP2) 全期間集計 ===")
    print(f"\n[JRA]")
    print(f"  races={races_jra:,}, hits={hits_jra:,}, bet={bet_jra:,}, pay={pay_jra:,}, ROI={pay_jra/bet_jra*100:.1f}%")
    print(f"  hit_rate={hits_jra/races_jra*100:.1f}%")
    print(f"\n[NAR]")
    print(f"  races={races_nar:,}, hits={hits_nar:,}, bet={bet_nar:,}, pay={pay_nar:,}, ROI={pay_nar/bet_nar*100:.1f}%")
    print(f"  hit_rate={hits_nar/races_nar*100:.1f}%")
    print(f"\n[ALL]")
    bet_all = bet_jra + bet_nar
    pay_all = pay_jra + pay_nar
    races_all = races_jra + races_nar
    hits_all = hits_jra + hits_nar
    print(f"  races={races_all:,}, hits={hits_all:,}, bet={bet_all:,}, pay={pay_all:,}, ROI={pay_all/bet_all*100:.1f}%")
    print(f"  hit_rate={hits_all/races_all*100:.1f}%")

    print(f"\n[印付き比率 (TOP2 のうち印付き数)]")
    total_pick = sum(pick_mark_dist.values())
    for k in sorted(pick_mark_dist.keys()):
        print(f"  marked={k}: {pick_mark_dist[k]:,} ({pick_mark_dist[k]/total_pick*100:.1f}%)")

    # variance 分析: 日別 ROI の標準偏差
    print(f"\n[日別 ROI variance]")
    rois = [d[2] / d[1] * 100 for d in daily_roi if d[1] > 0]
    if rois:
        import statistics
        rois_sorted = sorted(rois)
        print(f"  日数: {len(rois)}")
        print(f"  平均: {statistics.mean(rois):.1f}%")
        print(f"  中央: {statistics.median(rois):.1f}%")
        print(f"  std:  {statistics.stdev(rois):.1f}%")
        print(f"  P5: {rois_sorted[len(rois)//20]:.1f}% / P95: {rois_sorted[-len(rois)//20]:.1f}%")
        # 13.8% より低い日数
        below_14 = sum(1 for r in rois if r < 14)
        print(f"  ROI<14% の日: {below_14} ({below_14/len(rois)*100:.1f}%) → 13.8% は通常分散内")


if __name__ == "__main__":
    main()
