# -*- coding: utf-8 -*-
"""30 通り (6 自信度 × 5 買い目パターン) ROI 検証スクリプト

マスター指示 (2026-05-25): 自信度 SS〜D × 買い目 A〜E の 30 通りを算出。

各 race を:
  - 自信度 (overall_confidence): SS/S/A/B/C/D
  - 買い目パターン:
    A: ◎-〇▲-〇▲△★☆     (~7 点)
    B: ◎-〇▲△-〇▲△★☆  (~9 点)
    C: ◎-〇-▲△★☆          (~4 点)
    D: ◎〇-◎〇▲-◎〇▲△★☆     (~10 点)
    E: ◎〇-◎〇▲△-◎〇▲△★☆  (~12 点)

出力: 30 通りのマトリックス (ROI / 的中率 / ticket 数 / 純利益)
"""
import json
import sys
from pathlib import Path
from collections import defaultdict
from itertools import combinations

sys.stdout.reconfigure(encoding='utf-8')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.payout_normalizer import normalize_payouts, combo_match

PRED_DIR = PROJECT_ROOT / "data" / "predictions"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"

CONF_LIST = ["SS", "S", "A", "B", "C", "D"]
PATTERN_LIST = ["A", "B", "C", "D", "E"]
STAKE = 100  # 1点 100円


def get_horse_no_by_marks(horses, marks):
    """指定 mark の horse_no を印優先順で返す"""
    mark_priority = {"◉": 0, "◎": 1, "○": 2, "〇": 2, "▲": 3, "△": 4, "★": 5, "☆": 6, "×": 7}
    filtered = [h for h in horses if h.get("mark") in marks and not h.get("is_scratched")]
    filtered.sort(key=lambda h: mark_priority.get(h.get("mark"), 99))
    return [int(h.get("horse_no", 0)) for h in filtered]


def get_tickets_for_pattern(horses, pattern):
    """買い目パターン別に 三連複 combo list を返す"""
    HONMEI = {"◎", "◉"}
    TAIKOU = {"○", "〇"}
    RENKA = {"▲"}
    WIDE = {"△", "★"}
    OANA = {"☆"}

    ho = get_horse_no_by_marks(horses, HONMEI)
    ta = get_horse_no_by_marks(horses, TAIKOU)
    re = get_horse_no_by_marks(horses, RENKA)
    wi = get_horse_no_by_marks(horses, WIDE)
    oa = get_horse_no_by_marks(horses, OANA)

    if pattern == "A":
        # ◎ - 〇▲ - 〇▲△★☆
        col1, col2, col3 = ho, ta + re, ta + re + wi + oa
    elif pattern == "B":
        # ◎ - 〇▲△ - 〇▲△★☆ (△ は wide のうち印優先順 1 頭)
        col1, col2, col3 = ho, ta + re + wi[:1], ta + re + wi + oa
    elif pattern == "C":
        # ◎ - 〇 - ▲△★☆
        col1, col2, col3 = ho, ta, re + wi + oa
    elif pattern == "D":
        # ◎〇 - ◎〇▲ - ◎〇▲△★☆
        col1, col2, col3 = ho + ta, ho + ta + re, ho + ta + re + wi + oa
    elif pattern == "E":
        # ◎〇 - ◎〇▲△ - ◎〇▲△★☆
        col1, col2, col3 = ho + ta, ho + ta + re + wi[:1], ho + ta + re + wi + oa
    else:
        return []

    # C パターン対応: col2=1頭でも OK (◎×〇×▲△★☆ = 4点)
    if not col1 or len(col2) < 1 or len(col3) < 1:
        return []

    col3 = sorted(set(col3))
    seen = set()
    tickets = []
    for a in col1:
        for b in col2:
            if b == a:
                continue
            for c in col3:
                if c == a or c == b:
                    continue
                combo = tuple(sorted([a, b, c]))
                if combo not in seen:
                    seen.add(combo)
                    tickets.append(list(combo))
    return tickets


def main():
    # 結果データロード (results JSON)
    results_cache = {}
    for fp in sorted(RESULTS_DIR.glob("*_results.json")):
        date_part = fp.name.replace("_results.json", "")
        if date_part[:4] not in ("2024", "2025", "2026"):
            continue
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                if "races" in data and isinstance(data["races"], list):
                    for r in data["races"]:
                        rid = r.get("race_id", "")
                        if rid:
                            results_cache[rid] = r
                else:
                    for rid, r in data.items():
                        results_cache[rid] = r
        except Exception:
            continue
    print(f"results race: {len(results_cache):,}", flush=True)

    # 30 通り集計
    # stats[(conf, pattern)] = {bet, pay, races, hits, tickets}
    stats = defaultdict(lambda: {"bet": 0, "pay": 0, "races": 0, "hits": 0, "tickets": 0})

    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    processed = 0
    for pf in pred_files:
        date_part = pf.name.replace("_pred.json", "")
        if date_part[:4] not in ("2024", "2025", "2026"):
            continue
        try:
            pred = json.loads(pf.read_text(encoding="utf-8"))
        except Exception:
            continue
        for race in pred.get("races", []):
            rid = race.get("race_id", "")
            if rid not in results_cache:
                continue
            result = results_cache[rid]

            # top3 取得
            order = result.get("order", [])
            top3 = set()
            for h in order:
                f = h.get("finish") or h.get("finish_pos") or 99
                if f <= 3:
                    top3.add(h.get("horse_no"))
            if len(top3) < 3:
                continue

            # 自信度
            conf = race.get("overall_confidence")
            if conf not in CONF_LIST:
                continue

            # 三連複 payout 取得
            normalized = normalize_payouts(result.get("payouts", {}))
            pay_list = normalized.get("sanrenpuku", [])

            horses = race.get("horses", [])

            # 各パターンで集計
            for pattern in PATTERN_LIST:
                tickets = get_tickets_for_pattern(horses, pattern)
                if not tickets:
                    continue

                key = (conf, pattern)
                stats[key]["races"] += 1
                stats[key]["tickets"] += len(tickets)
                stats[key]["bet"] += len(tickets) * STAKE

                # 的中チェック (top3 と combo 一致 or payout combo と一致)
                hit_pay = 0
                hit_combo = None
                for combo in tickets:
                    if set(combo) == top3:
                        # payout 検索
                        for p in pay_list:
                            if combo_match(combo, p.get("combo", ""), "sanrenpuku"):
                                hit_pay = int(p.get("payout", 0) or 0)
                                hit_combo = combo
                                break
                        if hit_pay > 0:
                            break
                if hit_combo:
                    stats[key]["hits"] += 1
                    stats[key]["pay"] += hit_pay  # per 100 円 stake = STAKE (= 100)
        processed += 1

    print(f"処理: {processed:,} ファイル\n", flush=True)

    # 出力: 30 通りマトリックス
    print("=" * 110)
    print("  30 通り (6 自信度 × 5 買い目パターン) ROI マトリックス")
    print("=" * 110)
    print(f"\n{'自信度':>6s} | {'パターン':>4s} | {'race':>6s} | {'点数':>8s} | {'的中':>5s} | {'的中率':>6s} | {'投資(円)':>10s} | {'回収(円)':>10s} | {'純利益(円)':>10s} | {'ROI':>7s}")
    print("-" * 110)
    grand_bet = grand_pay = 0
    for conf in CONF_LIST:
        for pattern in PATTERN_LIST:
            s = stats.get((conf, pattern), {"bet": 0, "pay": 0, "races": 0, "hits": 0, "tickets": 0})
            if s["bet"] == 0:
                continue
            hr = s["hits"] / s["races"] * 100 if s["races"] else 0
            roi = s["pay"] / s["bet"] * 100 if s["bet"] else 0
            profit = s["pay"] - s["bet"]
            print(f"{conf:>6s} | {pattern:>4s} | {s['races']:>6,d} | {s['tickets']:>8,d} | {s['hits']:>5,d} | {hr:>5.1f}% | {s['bet']:>10,d} | {s['pay']:>10,d} | {profit:>10,d} | {roi:>6.1f}%")
            grand_bet += s["bet"]
            grand_pay += s["pay"]
        print()

    # ベスト 5
    print("\n=== TOP 10 (ROI 順) ===")
    ranked = sorted(
        [(c, p, s) for (c, p), s in stats.items() if s["bet"] > 0],
        key=lambda x: -(x[2]["pay"] / x[2]["bet"]),
    )
    for c, p, s in ranked[:10]:
        hr = s["hits"] / s["races"] * 100 if s["races"] else 0
        roi = s["pay"] / s["bet"] * 100
        profit = s["pay"] - s["bet"]
        print(f"  {c}-{p}: ROI {roi:.1f}% / 的中率 {hr:.1f}% / 純利益 {profit:+,d}円 / {s['races']:,} race")

    # 純利益ベスト 10
    print("\n=== TOP 10 (純利益 順) ===")
    ranked2 = sorted(
        [(c, p, s) for (c, p), s in stats.items() if s["bet"] > 0],
        key=lambda x: -(x[2]["pay"] - x[2]["bet"]),
    )
    for c, p, s in ranked2[:10]:
        hr = s["hits"] / s["races"] * 100 if s["races"] else 0
        roi = s["pay"] / s["bet"] * 100
        profit = s["pay"] - s["bet"]
        print(f"  {c}-{p}: 純利益 {profit:+,d}円 / ROI {roi:.1f}% / 的中率 {hr:.1f}% / {s['races']:,} race")


if __name__ == "__main__":
    main()
