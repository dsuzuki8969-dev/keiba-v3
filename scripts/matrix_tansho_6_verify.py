# -*- coding: utf-8 -*-
"""単勝 6 パターン ROI 検証スクリプト

マスター指示 (2026-05-25): 単勝買い目を 6 パターン比較。
  A: composite (総合指数) TOP1 + shobu_score (勝負気配) TOP1
  B: composite TOP1 + composite TOP2
  C: shobu_score TOP1 + shobu_score TOP2
  D: win_prob (勝率) TOP1 + win_prob TOP2
  E: ability_total (能力) TOP1 + pace_total (展開) TOP1
  F: ability_total TOP1 + course_total (適性) TOP1

各 race で 2 馬選定 (同馬除外で 1 or 2 ticket)、単勝 100 円固定。
"""
import json
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.payout_normalizer import normalize_payouts

PRED_DIR = PROJECT_ROOT / "data" / "predictions"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"

PATTERN_LIST = ["A", "B", "C", "D", "E", "F", "G"]
STAKE = 100


def get_top_n(horses, field, n=2, exclude_scratched=True, exclude_kiken=True):
    """指定 field の値で TOP n の horse_no を返す (降順)"""
    cand = [h for h in horses
            if (not exclude_scratched or not h.get("is_scratched"))
            and (not exclude_kiken or not h.get("is_tokusen_kiken"))]
    cand_sorted = sorted(
        cand,
        key=lambda h: float(h.get(field, 0) or 0),
        reverse=True,
    )
    return [int(h.get("horse_no", 0)) for h in cand_sorted[:n]]


def get_tickets_for_pattern(horses, pattern):
    """単勝 ticket の horse_no list を返す (重複除外)"""
    if pattern == "A":
        # composite TOP1 + shobu_score TOP1
        c1 = get_top_n(horses, "composite", 1)
        s1 = get_top_n(horses, "shobu_score", 1)
        nos = list(dict.fromkeys(c1 + s1))  # 重複除外保持順
    elif pattern == "B":
        # composite TOP1 + TOP2
        nos = get_top_n(horses, "composite", 2)
    elif pattern == "C":
        # shobu_score TOP1 + TOP2
        nos = get_top_n(horses, "shobu_score", 2)
    elif pattern == "D":
        # win_prob TOP1 + TOP2
        nos = get_top_n(horses, "win_prob", 2)
    elif pattern == "E":
        # ability_total TOP1 + pace_total TOP1
        a1 = get_top_n(horses, "ability_total", 1)
        p1 = get_top_n(horses, "pace_total", 1)
        nos = list(dict.fromkeys(a1 + p1))
    elif pattern == "F":
        # ability_total TOP1 + course_total TOP1
        a1 = get_top_n(horses, "ability_total", 1)
        c1 = get_top_n(horses, "course_total", 1)
        nos = list(dict.fromkeys(a1 + c1))
    elif pattern == "G":
        # マスター追加 (2026-05-25): 能力1位 + 展開1位 + 適性1位 (3 馬)
        a1 = get_top_n(horses, "ability_total", 1)
        p1 = get_top_n(horses, "pace_total", 1)
        c1 = get_top_n(horses, "course_total", 1)
        nos = list(dict.fromkeys(a1 + p1 + c1))
    else:
        return []
    return nos


def main():
    # results data load
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

    # 集計
    # stats[(conf, pattern)] = {bet, pay, races, hits, tickets}
    stats = defaultdict(lambda: {"bet": 0, "pay": 0, "races": 0, "hits": 0, "tickets": 0})
    stats_all = defaultdict(lambda: {"bet": 0, "pay": 0, "races": 0, "hits": 0, "tickets": 0})

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
            # winner (1着馬)
            winner = None
            for h in result.get("order", []):
                f = h.get("finish") or h.get("finish_pos") or 99
                if f == 1:
                    winner = h.get("horse_no")
                    break
            if winner is None:
                continue

            # 単勝 payout
            normalized = normalize_payouts(result.get("payouts", {}))
            tansho_pay = 0
            for p in normalized.get("tansho", []):
                p_combo = str(p.get("combo", "")).strip()
                if p_combo == str(winner):
                    tansho_pay = int(p.get("payout", 0) or 0)
                    break

            conf = race.get("overall_confidence") or "?"
            horses = race.get("horses", [])

            for pattern in PATTERN_LIST:
                nos = get_tickets_for_pattern(horses, pattern)
                if not nos:
                    continue

                key = (conf, pattern)
                stats[key]["races"] += 1
                stats[key]["tickets"] += len(nos)
                stats[key]["bet"] += len(nos) * STAKE
                stats_all[pattern]["races"] += 1
                stats_all[pattern]["tickets"] += len(nos)
                stats_all[pattern]["bet"] += len(nos) * STAKE

                if winner in nos and tansho_pay > 0:
                    stats[key]["hits"] += 1
                    stats[key]["pay"] += tansho_pay
                    stats_all[pattern]["hits"] += 1
                    stats_all[pattern]["pay"] += tansho_pay
        processed += 1

    print(f"処理: {processed:,} ファイル\n", flush=True)

    # === 全 race 一律 6 パターン ===
    print("=" * 100)
    print("  単勝 6 パターン 全 race 一律集計")
    print("=" * 100)
    print(f"\n{'パターン':>4s} | {'race':>6s} | {'点数':>8s} | {'的中':>5s} | {'的中率':>6s} | {'投資(円)':>10s} | {'回収(円)':>10s} | {'純利益(円)':>10s} | {'ROI':>7s}")
    print("-" * 100)
    for pattern in PATTERN_LIST:
        s = stats_all.get(pattern, {})
        if not s.get("bet"):
            continue
        hr = s["hits"] / s["races"] * 100 if s["races"] else 0
        roi = s["pay"] / s["bet"] * 100 if s["bet"] else 0
        profit = s["pay"] - s["bet"]
        print(f"{pattern:>4s} | {s['races']:>6,d} | {s['tickets']:>8,d} | {s['hits']:>5,d} | {hr:>5.1f}% | {s['bet']:>10,d} | {s['pay']:>10,d} | {profit:>10,d} | {roi:>6.1f}%")

    # === 自信度別 × パターン ===
    print("\n" + "=" * 100)
    print("  単勝 6 パターン × 自信度別 (TOP 純利益 順)")
    print("=" * 100)
    print(f"\n{'自信度':>6s} | {'パタ':>4s} | {'race':>6s} | {'点数':>8s} | {'的中':>5s} | {'的中率':>6s} | {'投資(円)':>10s} | {'回収(円)':>10s} | {'純利益(円)':>10s} | {'ROI':>7s}")
    print("-" * 100)
    for conf in ["SS", "S", "A", "B", "C", "D"]:
        for pattern in PATTERN_LIST:
            s = stats.get((conf, pattern), {})
            if not s.get("bet"):
                continue
            hr = s["hits"] / s["races"] * 100 if s["races"] else 0
            roi = s["pay"] / s["bet"] * 100 if s["bet"] else 0
            profit = s["pay"] - s["bet"]
            print(f"{conf:>6s} | {pattern:>4s} | {s['races']:>6,d} | {s['tickets']:>8,d} | {s['hits']:>5,d} | {hr:>5.1f}% | {s['bet']:>10,d} | {s['pay']:>10,d} | {profit:>10,d} | {roi:>6.1f}%")
        print()

    # TOP 10 自信度×パターン 純利益順
    print("\n=== TOP 10 (純利益 順) ===")
    ranked = sorted(
        [(c, p, s) for (c, p), s in stats.items() if s.get("bet", 0) > 0],
        key=lambda x: -(x[2]["pay"] - x[2]["bet"]),
    )
    for c, p, s in ranked[:10]:
        hr = s["hits"] / s["races"] * 100 if s["races"] else 0
        roi = s["pay"] / s["bet"] * 100
        profit = s["pay"] - s["bet"]
        print(f"  {c}-{p}: 純利益 {profit:+,d}円 / ROI {roi:.1f}% / 的中率 {hr:.1f}% / {s['races']:,} race")


if __name__ == "__main__":
    main()
