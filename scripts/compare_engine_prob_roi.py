#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
P0-γ: engine印 vs prob印 三連複 ROI 比較スクリプト
- engine印: data/predictions/ の YYYYMMDD_pred.json (WF --composite-marks 出力)
- prob印: data/_diag/p0a_backup/ の YYYYMMDD_pred.json (WF 従来prob印)
- result: data/results/ の YYYYMMDD_results.json
- 同一レース集合で比較(engine印が採用されたレースのみ)
"""
import json
import os
import sys
import glob

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENGINE_PRED_DIR = os.path.join(BASE_DIR, "data", "predictions")
PROB_PRED_DIR = os.path.join(BASE_DIR, "data", "_diag", "p0a_backup")
RESULT_DIR = os.path.join(BASE_DIR, "data", "results")

TARGET_MONTH = "202601"

def load_preds(pred_dir, date_pattern):
    """pred.json を読み込んで race_id -> pred_race dict を返す"""
    races = {}
    files = sorted(glob.glob(os.path.join(pred_dir, f"{date_pattern}*_pred.json")))
    print(f"  pred files: {len(files)} ({pred_dir})")
    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            for race in data.get("races", []):
                rid = race.get("race_id", "")
                if rid:
                    races[rid] = race
        except Exception as e:
            print(f"  WARN: {fp}: {e}")
    return races

def load_results(date_pattern):
    """results.json を読み込んで race_id -> order+payouts dict を返す"""
    results = {}
    files = sorted(glob.glob(os.path.join(RESULT_DIR, f"{date_pattern}*_results.json")))
    print(f"  result files: {len(files)}")
    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            for rid, rv in data.items():
                results[rid] = rv
        except Exception as e:
            print(f"  WARN: {fp}: {e}")
    return results

def normalize_combo(combo_list):
    return "-".join(str(x) for x in sorted(combo_list))

def get_trio_payouts(payouts):
    trio = payouts.get("三連複", payouts.get("trifecta_combo"))
    if trio is None:
        return {}
    if isinstance(trio, dict):
        return {trio["combo"]: trio["payout"]}
    elif isinstance(trio, list):
        return {p["combo"]: p["payout"] for p in trio if isinstance(p, dict)}
    return {}

def get_order_top3(order_list):
    """着順リストから1-3着馬番セットを返す"""
    top3 = []
    for entry in sorted(order_list, key=lambda x: x.get("finish", 99)):
        f = entry.get("finish", 99)
        if f and f <= 3:
            top3.append(entry.get("horse_no"))
    return [x for x in top3 if x is not None][:3]

def calc_roi(pred_races, result_data, label=""):
    """三連複チケットの hit% / ROI を計算"""
    total_invest = 0
    total_return = 0
    total_tickets = 0
    hit_races = 0
    total_bet_races = 0

    for race_id, pred_race in pred_races.items():
        res = result_data.get(race_id)
        if not res:
            continue

        order = res.get("order", [])
        payouts = res.get("payouts", {})
        if not order:
            continue

        top3 = get_order_top3(order)
        if len(top3) < 3:
            continue

        win_combo = normalize_combo(top3)
        trio_payouts = get_trio_payouts(payouts)

        # 三連複チケット
        tickets = pred_race.get("tickets", [])
        trio_tickets = [t for t in tickets if t.get("type") == "三連複"]
        if not trio_tickets:
            continue

        race_invest = 0
        race_return = 0
        for t in trio_tickets:
            stake = t.get("stake", 100)
            combo = t.get("combo", [])
            combo_str = normalize_combo(combo)
            total_invest += stake
            race_invest += stake
            total_tickets += 1
            if combo_str == win_combo:
                payout = trio_payouts.get(win_combo, 0)
                if payout:
                    units = stake // 100
                    ret = int(payout) * units
                    total_return += ret
                    race_return += ret

        total_bet_races += 1
        if race_return > 0:
            hit_races += 1

    roi = total_return / total_invest * 100 if total_invest > 0 else 0
    hit_pct = hit_races / total_bet_races * 100 if total_bet_races > 0 else 0
    print(f"\n--- {label} ---")
    print(f"  購入レース数: {total_bet_races}")
    print(f"  チケット数:   {total_tickets}")
    print(f"  投資額:       {total_invest:,}円")
    print(f"  回収額:       {total_return:,}円")
    print(f"  ROI:          {roi:.1f}%")
    print(f"  hit%:         {hit_pct:.1f}% ({hit_races}/{total_bet_races}R)")
    return {
        "label": label,
        "bet_races": total_bet_races,
        "tickets": total_tickets,
        "invest": total_invest,
        "ret": total_return,
        "roi": roi,
        "hit_pct": hit_pct,
        "hit_races": hit_races,
    }

def main():
    print(f"=== P0-γ engine印 vs prob印 ROI比較 ({TARGET_MONTH}) ===")
    print(f"\n[1] engine印 pred ロード ({ENGINE_PRED_DIR})")
    engine_races = load_preds(ENGINE_PRED_DIR, TARGET_MONTH)
    print(f"  engine印 races: {len(engine_races)}")

    print(f"\n[2] prob印 pred ロード ({PROB_PRED_DIR})")
    prob_races_all = load_preds(PROB_PRED_DIR, TARGET_MONTH)
    print(f"  prob印 races: {len(prob_races_all)}")

    print(f"\n[3] results ロード")
    results = load_results(TARGET_MONTH)
    print(f"  result races: {len(results)}")

    # 母数統一: engine印採用レースのみ (engine_races の race_id に限定)
    engine_race_ids = set(engine_races.keys())
    prob_races_filtered = {rid: pred for rid, pred in prob_races_all.items() if rid in engine_race_ids}
    print(f"\n[4] 母数統一: engine印採用レース {len(engine_race_ids)}R")
    print(f"  prob印版 同一race_id: {len(prob_races_filtered)}R")

    # ROI計算
    engine_stats = calc_roi(engine_races, results, label=f"engine印 (--composite-marks WF)")
    prob_stats = calc_roi(prob_races_filtered, results, label=f"prob印 (p0a_backup 従来WF)")

    # 差分
    print("\n=== 比較表 ===")
    print(f"  {'指標':<20} {'engine印':>12} {'prob印':>12} {'差':>10}")
    print(f"  {'-'*56}")
    print(f"  {'購入レース数':<20} {engine_stats['bet_races']:>12} {prob_stats['bet_races']:>12} {engine_stats['bet_races']-prob_stats['bet_races']:>+10}")
    print(f"  {'ROI%':<20} {engine_stats['roi']:>11.1f}% {prob_stats['roi']:>11.1f}% {engine_stats['roi']-prob_stats['roi']:>+9.1f}%")
    print(f"  {'hit%':<20} {engine_stats['hit_pct']:>11.1f}% {prob_stats['hit_pct']:>11.1f}% {engine_stats['hit_pct']-prob_stats['hit_pct']:>+9.1f}%")
    print(f"  {'投資額':<20} {engine_stats['invest']:>12,} {prob_stats['invest']:>12,}")
    print(f"  {'回収額':<20} {engine_stats['ret']:>12,} {prob_stats['ret']:>12,}")

    print(f"\n  比較ファイル:")
    print(f"    engine印: {ENGINE_PRED_DIR}/202601XXXX_pred.json")
    print(f"    prob印:   {PROB_PRED_DIR}/202601XXXX_pred.json")

if __name__ == "__main__":
    main()
