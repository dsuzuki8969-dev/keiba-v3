# -*- coding: utf-8 -*-
"""C-1b: verify_all_tickets vs analyze_r1 残乖離 18pt 真因究明

両集計を per ticket レベルで突合し、差分発生 race を特定。

差分仮説:
  1. 同着 (multi-entry payout) 集計差
  2. per-race (verify) vs per-ticket (R-1) 集計方式差
  3. R-1 集計範囲の自信度フィルタ差異
  4. payout 取得方式差 (verify: get_first_payout / R-1: combo_match)
"""
import json
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.payout_normalizer import normalize_payouts, combo_match, get_first_payout

PRED_DIR = PROJECT_ROOT / "data" / "predictions"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"
DB_PATH = PROJECT_ROOT / "data" / "keiba.db"


def get_top3_set(result_data):
    order = result_data.get("order", [])
    if not order:
        return set()
    top3 = set()
    for h in order:
        finish = h.get("finish", h.get("finish_pos", 99))
        if finish <= 3:
            top3.add(h.get("horse_no"))
    return top3


def verify_style(pred_data, results):
    """verify_all_tickets 集計方式"""
    bet = 0
    pay = 0
    races = 0
    hits = 0
    for race in pred_data.get("races", []):
        rid = race.get("race_id", "")
        tickets = race.get("tickets", []) or []
        sanren = [t for t in tickets if t.get("type") in ("三連複", "3連複", "sanrenpuku")]
        if not sanren:
            continue
        result = results.get(rid) or results.get(str(rid))
        if not result:
            continue
        top3 = get_top3_set(result)
        if len(top3) < 3:
            continue
        normalized = normalize_payouts(result.get("payouts", {}))
        payout_amount = get_first_payout(normalized, "sanrenpuku")

        races += 1
        race_stake = sum(t.get("stake", 100) for t in sanren)
        bet += race_stake

        hit = False
        for t in sanren:
            if set(t.get("combo", [])) == top3:
                hit = True
                break
        if hit and payout_amount > 0:
            hits += 1
            pay += payout_amount
    return bet, pay, races, hits


def r1_style(pred_data, results):
    """R-1 (analyze_r1) 集計方式"""
    bet = 0
    pay = 0
    tickets_count = 0
    hits = 0
    for race in pred_data.get("races", []):
        rid = race.get("race_id", "")
        tbm = race.get("tickets_by_mode", {})
        tickets = race.get("tickets", []) or []
        if isinstance(tbm, dict) and tbm.get("fixed"):
            tickets = tbm["fixed"]
        sanren = [t for t in tickets if t.get("type") in ("三連複", "3連複", "sanrenpuku")]
        if not sanren:
            continue
        result = results.get(rid) or results.get(str(rid))
        if not result:
            continue
        normalized = normalize_payouts(result.get("payouts", {}))
        pay_list = normalized.get("sanrenpuku", [])

        for t in sanren:
            stake = t.get("stake", 100) or 100
            bet += stake
            tickets_count += 1
            combo = t.get("combo", [])
            for p in pay_list:
                p_combo = p.get("combo", "")
                p_amt = p.get("payout", 0) or 0
                if combo_match(combo, p_combo, "sanrenpuku"):
                    pay += int(p_amt * (stake / 100))
                    hits += 1
                    break
    return bet, pay, tickets_count, hits


def main():
    # 全期間で per race per-ticket 差分究明
    pred_files = sorted(PRED_DIR.glob("*_pred.json"))

    v_bet = v_pay = v_races = v_hits = 0
    r_bet = r_pay = r_ticks = r_hits = 0

    diff_races = []
    print("=== C-1b: verify vs R-1 per race 集計突合 (直近 30 日) ===")
    for pf in pred_files:
        date_part = pf.name.replace("_pred.json", "")
        rp = RESULTS_DIR / f"{date_part}_results.json"
        if not rp.exists():
            continue
        data = json.loads(rp.read_text(encoding="utf-8"))
        if "races" in data and isinstance(data["races"], list):
            results = {r.get("race_id", ""): r for r in data["races"] if r.get("race_id")}
        else:
            results = data
        pred = json.loads(pf.read_text(encoding="utf-8"))

        vb, vp, vr, vh = verify_style(pred, results)
        rb, rp_, rt, rh = r1_style(pred, results)
        v_bet += vb; v_pay += vp; v_races += vr; v_hits += vh
        r_bet += rb; r_pay += rp_; r_ticks += rt; r_hits += rh

        if vp != rp_:
            diff_races.append({
                "date": date_part,
                "v_bet": vb, "v_pay": vp, "v_hits": vh,
                "r_bet": rb, "r_pay": rp_, "r_hits": rh,
                "diff_pay": rp_ - vp,
            })

    print(f"\nverify 集計: bet={v_bet:,} pay={v_pay:,} races={v_races:,} hits={v_hits:,} ROI={v_pay/v_bet*100:.1f}%" if v_bet else "")
    print(f"R-1 集計:    bet={r_bet:,} pay={r_pay:,} tickets={r_ticks:,} hits={r_hits:,} ROI={r_pay/r_bet*100:.1f}%" if r_bet else "")
    print(f"\n差分:")
    print(f"  bet  : {r_bet - v_bet:+,} ({(r_bet-v_bet)/v_bet*100:+.1f}%)" if v_bet else "")
    print(f"  pay  : {r_pay - v_pay:+,} ({(r_pay-v_pay)/v_pay*100:+.1f}%)" if v_pay else "")
    print(f"  hits : {r_hits - v_hits:+,}")
    print(f"  ROI  : {(r_pay/r_bet - v_pay/v_bet)*100:+.1f}pt" if v_bet and r_bet else "")

    print(f"\n差分発生日: {len(diff_races)} 日")
    for d in diff_races[:15]:
        print(f"  {d['date']}: v_pay={d['v_pay']:,} r_pay={d['r_pay']:,} diff={d['diff_pay']:+,} v_hits={d['v_hits']} r_hits={d['r_hits']}")


if __name__ == "__main__":
    main()
