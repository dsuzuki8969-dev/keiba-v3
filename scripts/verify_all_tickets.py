"""全期間の三連複チケットを実結果と照合し、真の的中率・ROI を検証

偽的中パターン (scrape_failed 馬多数 → 残り数頭で唯一のcombo) を検出し、
正味の ROI を算出する。

出力:
  1. 年別・信頼度別の的中率・ROI
  2. 偽的中 (degenerate hit) の一覧
  3. 高配当 TOP10 (偽的中除外)
"""

import json
import os
import sys
from collections import defaultdict
from glob import glob

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PREDICTIONS_DIR = os.path.join(PROJECT_ROOT, "data", "predictions")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")

_TICKET_KEY_ALIASES = ["三連複", "3連複", "sanrenpuku"]


def load_results(fpath):
    """results JSON をロード → {race_id: {order, payouts}} 形式"""
    with open(fpath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "races" in data and isinstance(data["races"], list):
        result = {}
        for r in data["races"]:
            rid = r.get("race_id", "")
            if rid:
                result[rid] = r
        return result

    return data


def get_sanrenpuku_payout(payouts):
    """三連複の払戻金額を取得"""
    for key in _TICKET_KEY_ALIASES:
        val = payouts.get(key)
        if val is not None:
            if isinstance(val, dict):
                return val.get("combo", ""), val.get("payout", 0)
            elif isinstance(val, list):
                if val:
                    return val[0].get("combo", ""), val[0].get("payout", 0)
    return "", 0


def get_top3_set(result_data):
    """結果データからトップ3の馬番セットを取得"""
    order = result_data.get("order", [])
    if not order:
        horses = result_data.get("horses", [])
        if horses:
            sorted_h = sorted(horses, key=lambda h: h.get("finish_pos", 99))
            return set(h.get("horse_no") for h in sorted_h[:3] if h.get("finish_pos", 99) <= 3)
        return set()

    top3 = set()
    for h in order:
        finish = h.get("finish", h.get("finish_pos", 99))
        if finish <= 3:
            top3.add(h.get("horse_no"))
    return top3


def main():
    pred_files = sorted(glob(os.path.join(PREDICTIONS_DIR, "*_pred.json")))

    stats = defaultdict(lambda: {
        "races": 0, "tickets": 0, "stake": 0, "payout": 0, "hits": 0,
        "degenerate_races": 0, "degenerate_hits": 0, "degenerate_payout": 0,
    })

    all_hits = []
    degenerate_hits_list = []
    no_result_races = 0

    for fi, pred_path in enumerate(pred_files):
        fname = os.path.basename(pred_path)
        date_part = fname.replace("_pred.json", "")
        year = date_part[:4]

        results_path = os.path.join(RESULTS_DIR, f"{date_part}_results.json")
        if not os.path.exists(results_path):
            results_path = os.path.join(RESULTS_DIR, f"{date_part}.json")
        if not os.path.exists(results_path):
            continue

        results = load_results(results_path)

        with open(pred_path, "r", encoding="utf-8") as f:
            pred_data = json.load(f)

        for race in pred_data.get("races", []):
            race_id = race.get("race_id", "")
            horses = race.get("horses", [])
            tickets = race.get("tickets", [])

            if not tickets:
                continue

            result = results.get(race_id) or results.get(str(race_id))
            if result is None:
                no_result_races += 1
                continue

            top3 = get_top3_set(result)
            if len(top3) < 3:
                continue

            payouts_data = result.get("payouts", {})
            combo_str, payout_amount = get_sanrenpuku_payout(payouts_data)

            active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]
            total = len(horses)
            is_degenerate = len(active) < 5 and total >= 5

            confidence = "B"
            tbm = race.get("tickets_by_mode", {})
            meta = tbm.get("_meta", {})
            if meta.get("confidence"):
                confidence = meta["confidence"]
            elif race.get("overall_confidence"):
                confidence = race["overall_confidence"]

            race_stake = sum(t.get("stake", 100) for t in tickets)

            key_year = year
            key_conf = confidence
            key_all = "ALL"

            stats[key_year]["races"] += 1
            stats[key_all]["races"] += 1
            stats[key_year]["tickets"] += len(tickets)
            stats[key_all]["tickets"] += len(tickets)
            stats[key_year]["stake"] += race_stake
            stats[key_all]["stake"] += race_stake

            if is_degenerate:
                stats[key_year]["degenerate_races"] += 1
                stats[key_all]["degenerate_races"] += 1

            hit = False
            for ticket in tickets:
                combo = ticket.get("combo", [])
                if set(combo) == top3:
                    hit = True
                    break

            if hit and payout_amount > 0:
                stats[key_year]["hits"] += 1
                stats[key_all]["hits"] += 1
                stats[key_year]["payout"] += payout_amount
                stats[key_all]["payout"] += payout_amount

                hit_record = {
                    "date": date_part,
                    "race_id": race_id,
                    "race_name": race.get("race_name", ""),
                    "combo": sorted(list(top3)),
                    "payout": payout_amount,
                    "confidence": confidence,
                    "active": len(active),
                    "total": total,
                    "is_degenerate": is_degenerate,
                    "stake": race_stake,
                }
                all_hits.append(hit_record)

                if is_degenerate:
                    stats[key_year]["degenerate_hits"] += 1
                    stats[key_all]["degenerate_hits"] += 1
                    stats[key_year]["degenerate_payout"] += payout_amount
                    stats[key_all]["degenerate_payout"] += payout_amount
                    degenerate_hits_list.append(hit_record)

        if (fi + 1) % 100 == 0:
            print(f"  [{fi+1}/{len(pred_files)}] {(fi+1)/len(pred_files)*100:.0f}%")

    # 結果出力
    print("\n" + "=" * 80)
    print("三連複 全期間照合結果")
    print("=" * 80)

    print(f"\n結果なしレース (results 未取得): {no_result_races}")

    print(f"\n{'年':>6} | {'レース':>7} | {'点数':>7} | {'的中':>5} | {'的中率':>7} | "
          f"{'投資':>12} | {'回収':>12} | {'ROI':>7} | "
          f"{'偽的中':>5} | {'偽回収':>10} | {'正味ROI':>7}")
    print("-" * 120)

    for key in sorted(stats.keys()):
        s = stats[key]
        if s["races"] == 0:
            continue
        hit_rate = s["hits"] / s["races"] * 100 if s["races"] > 0 else 0
        roi = s["payout"] / s["stake"] * 100 if s["stake"] > 0 else 0

        net_payout = s["payout"] - s["degenerate_payout"]
        net_roi = net_payout / s["stake"] * 100 if s["stake"] > 0 else 0

        print(f"{key:>6} | {s['races']:>7,} | {s['tickets']:>7,} | {s['hits']:>5,} | {hit_rate:>6.1f}% | "
              f"{s['stake']:>10,}円 | {s['payout']:>10,}円 | {roi:>6.1f}% | "
              f"{s['degenerate_hits']:>5} | {s['degenerate_payout']:>8,}円 | {net_roi:>6.1f}%")

    # 偽的中一覧
    if degenerate_hits_list:
        print(f"\n{'=' * 80}")
        print(f"偽的中一覧 (active < 5, total >= 5): {len(degenerate_hits_list)} 件")
        print(f"{'=' * 80}")
        for h in sorted(degenerate_hits_list, key=lambda x: -x["payout"]):
            print(f"  {h['date']} | {h['race_id']} | {h['race_name'][:15]:<15} | "
                  f"combo={h['combo']} | {h['payout']:>10,}円 | "
                  f"conf={h['confidence']} | active={h['active']}/{h['total']}")

    # 高配当 TOP10 (偽的中除外)
    genuine_hits = [h for h in all_hits if not h["is_degenerate"]]
    genuine_hits.sort(key=lambda x: -x["payout"])
    print(f"\n{'=' * 80}")
    print(f"高配当 TOP10 (偽的中除外)")
    print(f"{'=' * 80}")
    for i, h in enumerate(genuine_hits[:10]):
        print(f"  #{i+1}: {h['payout']:>10,}円 | {h['date']} | {h['race_name'][:20]:<20} | "
              f"combo={h['combo']} | conf={h['confidence']} | active={h['active']}/{h['total']}")

    # 高配当 TOP10 (全体)
    all_hits.sort(key=lambda x: -x["payout"])
    print(f"\n{'=' * 80}")
    print(f"高配当 TOP10 (偽的中含む)")
    print(f"{'=' * 80}")
    for i, h in enumerate(all_hits[:10]):
        degen_flag = " [DEGENERATE]" if h["is_degenerate"] else ""
        print(f"  #{i+1}: {h['payout']:>10,}円 | {h['date']} | {h['race_name'][:20]:<20} | "
              f"combo={h['combo']} | conf={h['confidence']} | active={h['active']}/{h['total']}{degen_flag}")


if __name__ == "__main__":
    main()
