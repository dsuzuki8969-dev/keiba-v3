"""Phase 3 三連単フォーメーション戦略の配当統計（最高/平均）を集計"""
from __future__ import annotations
import io
import sys
import os
import json
from pathlib import Path
from collections import defaultdict
from datetime import date, timedelta

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, ".")

from src.calculator.betting import SANRENTAN_SKIP_CONFIDENCES
from scripts.monthly_backtest import build_sanrentan_tickets, get_payout


def main():
    if len(sys.argv) < 3:
        print("usage: sanrentan_payout_stats.py 20260301 20260331")
        return 1
    s, e = sys.argv[1], sys.argv[2]
    start = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    end = date(int(e[:4]), int(e[4:6]), int(e[6:8]))

    all_hit_payouts = []        # 的中した1点あたりの payout (100円ベース)
    race_paybacks = []           # 的中レースの合計払戻
    race_meta = []               # (date, race_id, conf, total_payback, hit_combos)
    by_conf_payouts = defaultdict(list)
    by_conf_race_payback = defaultdict(list)

    d = start
    while d <= end:
        ds = d.strftime("%Y%m%d")
        pred_fp = Path(f"data/predictions/{ds}_pred.json")
        res_fp = Path(f"data/results/{ds}_results.json")
        if not pred_fp.exists() or not res_fp.exists():
            d += timedelta(days=1)
            continue
        with pred_fp.open("r", encoding="utf-8") as f:
            pred = json.load(f)
        with res_fp.open("r", encoding="utf-8") as f:
            results = json.load(f)

        for r in pred.get("races", []):
            race_id = str(r.get("race_id", ""))
            conf = r.get("confidence", "C")
            n = r.get("field_count") or len(r.get("horses", []))
            is_jra = r.get("is_jra", True)
            horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
            if not horses: continue
            rdata = results.get(race_id)
            if rdata is None: continue
            payouts = rdata.get("payouts", {})
            if not payouts or "三連単" not in payouts: continue
            if conf in SANRENTAN_SKIP_CONFIDENCES: continue

            try:
                tickets = build_sanrentan_tickets(horses, n, is_jra)
            except Exception:
                continue
            if not tickets: continue

            race_total_payback = 0
            hit_combos = []
            for t in tickets:
                stake = t["stake"]
                pp = get_payout(payouts, t)  # 100円ベース
                payback = pp * (stake // 100)
                if payback > 0:
                    all_hit_payouts.append(pp)  # 100円ベースで保存
                    by_conf_payouts[conf].append(pp)
                    race_total_payback += payback
                    hit_combos.append((t["combo"], pp))

            if race_total_payback > 0:
                race_paybacks.append(race_total_payback)
                by_conf_race_payback[conf].append(race_total_payback)
                race_meta.append((ds, race_id, conf, race_total_payback, hit_combos))

        d += timedelta(days=1)

    print()
    print("=" * 80)
    print(f"期間: {s}～{e}  Phase 3 三連単フォーメーション戦略 (SS/C/D skip)")
    print("=" * 80)

    print()
    print("--- 配当統計（券単位 / 100円ベース） ---")
    if all_hit_payouts:
        n_hits = len(all_hit_payouts)
        max_p = max(all_hit_payouts)
        avg_p = sum(all_hit_payouts) / n_hits
        median_p = sorted(all_hit_payouts)[n_hits // 2]
        print(f"  的中券数:        {n_hits} 点")
        print(f"  最高配当:        {max_p:>10,} 円 (100円→{max_p:,}円)")
        print(f"  平均配当:        {avg_p:>10,.0f} 円")
        print(f"  中央値配当:      {median_p:>10,} 円")
        print(f"  最低配当:        {min(all_hit_payouts):>10,} 円")
        # 配当分布
        ranges = [(0, 1000, "<1,000"),
                  (1000, 5000, "1,000-5,000"),
                  (5000, 10000, "5,000-10,000"),
                  (10000, 50000, "10,000-50,000"),
                  (50000, 100000, "50,000-100,000"),
                  (100000, 99999999, "100,000+")]
        print(f"  配当分布（点数 / 比率）:")
        for lo, hi, label in ranges:
            cnt = sum(1 for p in all_hit_payouts if lo <= p < hi)
            pct = cnt / n_hits * 100
            print(f"    {label:<18} {cnt:>5} 点 ({pct:>5.1f}%)")

    print()
    print("--- 配当統計（レース単位 / 1レースあたり払戻） ---")
    if race_paybacks:
        n_r = len(race_paybacks)
        max_r = max(race_paybacks)
        avg_r = sum(race_paybacks) / n_r
        median_r = sorted(race_paybacks)[n_r // 2]
        print(f"  的中レース数:    {n_r} R")
        print(f"  最高払戻 (1R):   {max_r:>10,} 円 (3,000円投資想定)")
        print(f"  平均払戻 (1R):   {avg_r:>10,.0f} 円")
        print(f"  中央値払戻 (1R): {median_r:>10,} 円")
        print(f"  最低払戻 (1R):   {min(race_paybacks):>10,} 円")
        # 1R 投資3000円ベースで純利益
        avg_net = avg_r - 3000
        print(f"  1R 平均純利益:   {avg_net:>+10,.0f} 円 (投資3,000円差引)")

    print()
    print("--- 信頼度別 配当 ---")
    print(f"{'conf':<6}{'的中点':>8}{'最高':>10}{'平均':>10}{'中央値':>10}{'1R最高':>10}{'1R平均':>10}")
    for c in ("S", "A", "B"):
        ps = by_conf_payouts.get(c, [])
        rps = by_conf_race_payback.get(c, [])
        if not ps:
            continue
        max_p = max(ps)
        avg_p = sum(ps) / len(ps)
        med_p = sorted(ps)[len(ps)//2]
        max_r = max(rps) if rps else 0
        avg_r = sum(rps) / len(rps) if rps else 0
        print(f"{c:<6}{len(ps):>8}{max_p:>10,}{avg_p:>10,.0f}{med_p:>10,}{max_r:>10,}{avg_r:>10,.0f}")

    print()
    print("--- 最高配当 TOP5 (1点あたり) ---")
    sorted_hits = sorted(all_hit_payouts, reverse=True)[:5]
    for i, p in enumerate(sorted_hits, 1):
        print(f"  #{i}  {p:>10,} 円")

    print()
    print("--- 1レース最高払戻 TOP5 ---")
    sorted_races = sorted(race_meta, key=lambda x: -x[3])[:5]
    for i, (ds, rid, conf, total, combos) in enumerate(sorted_races, 1):
        combo_str = ", ".join(f"{'-'.join(str(x) for x in c)}({p:,}円)" for c, p in combos[:3])
        print(f"  #{i}  {ds} race_id={rid} conf={conf} → 払戻 {total:,}円 (3,000円投資→純利益 {total-3000:+,}円)")
        print(f"        {combo_str}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
