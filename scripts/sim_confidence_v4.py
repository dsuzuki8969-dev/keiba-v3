#!/usr/bin/env python3
"""
自信度v4シミュレーション — パーセンタイル方式で6レベルの閾値を決定

目標構成比:
  SS: 5%  S: 10%  A: 15%  B: 35%  C: 25%  D: 10%

方式2: 過去データからJRA/NAR別にパーセンタイル閾値を算出し固定
"""
import json, glob, os, sys, io, time
import numpy as np
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True, errors="replace")

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
JRA_VENUES = {"東京", "中山", "阪神", "京都", "中京", "小倉", "新潟", "福島", "函館", "札幌"}

t0 = time.time()

all_results = {}
all_payouts = {}
for fp in sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json"))):
    try:
        d = json.load(open(fp, "r", encoding="utf-8"))
        for rid, data in d.items():
            all_results[rid] = {e["horse_no"]: e.get("finish", 99) for e in data.get("order", []) if "horse_no" in e}
            pay = data.get("payouts", {})
            if pay: all_payouts[rid] = pay
    except: pass


def get_win_pay(pay, hno):
    w = pay.get("単勝", None)
    if w is None: return 0
    hs = str(hno)
    if isinstance(w, dict):
        return (w.get("payout", 0) or 0) if str(w.get("combo", "")) == hs else 0
    if isinstance(w, list):
        for x in w:
            if isinstance(x, dict) and str(x.get("combo", "")) == hs:
                return x.get("payout", 0) or 0
    return 0


def get_place_pay(pay, hno):
    w = pay.get("複勝", None)
    if w is None: return 0
    hs = str(hno)
    if isinstance(w, list):
        for x in w:
            if isinstance(x, dict) and str(x.get("combo", "")) == hs:
                return x.get("payout", 0) or 0
    if isinstance(w, dict):
        return (w.get("payout", 0) or 0) if str(w.get("combo", "")) == hs else 0
    return 0


# データ読込
jra_data, nar_data = [], []
target = [(fp, os.path.basename(fp)[:8]) for fp in sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
          if "_prev" not in os.path.basename(fp) and "_backup" not in os.path.basename(fp)]

for i, (fp, dt) in enumerate(target):
    try: d = json.load(open(fp, encoding="utf-8"))
    except: continue
    for r in d.get("races", []):
        rid = r.get("race_id", "")
        if rid not in all_results: continue
        venue = r.get("venue", "")
        entry = (r, all_results[rid], all_payouts.get(rid, {}))
        if venue in JRA_VENUES: jra_data.append(entry)
        else: nar_data.append(entry)
    if (i + 1) % 200 == 0:
        print(f"  [{(i+1)/len(target)*100:.0f}%] {i+1}/{len(target)}", flush=True)

print(f"  JRA: {len(jra_data)}R  NAR: {len(nar_data)}R  ({time.time()-t0:.1f}s)")


def get_honmei(race):
    for h in race.get("horses", []):
        if h.get("mark", "") in ("◉", "◎"):
            return h
    return None


# 目標構成比（累積）: D(下位10%) → C(10-35%) → B(35-70%) → A(70-85%) → S(85-95%) → SS(95-100%)
# パーセンタイル: P10, P35, P70, P85, P95
TARGET = {"SS": 5, "S": 10, "A": 15, "B": 35, "C": 25, "D": 10}
PERCENTILES = [10, 35, 70, 85, 95]  # D/C, C/B, B/A, A/S, S/SS
LEVEL_ORDER = ["D", "C", "B", "A", "S", "SS"]


def analyze(data, label):
    # 全スコア収集
    scores = []
    race_entries = []
    for race, fm, pay in data:
        score = race.get("confidence_score", 0) or 0
        honmei = get_honmei(race)
        if not honmei: continue
        hno = honmei.get("horse_no")
        fp2 = fm.get(hno, 99)
        if fp2 <= 0 or fp2 >= 90: continue
        pop = honmei.get("popularity") or 99
        scores.append(score)
        race_entries.append((score, fp2, hno, pay, pop))

    scores_arr = np.array(scores)
    total = len(scores_arr)

    # パーセンタイル閾値算出
    thresholds = np.percentile(scores_arr, PERCENTILES)

    print(f"\n{'='*90}")
    print(f"  {label} 自信度v4シミュレーション (N={total})")
    print(f"{'='*90}")
    print(f"\n  ■ パーセンタイル閾値:")
    for p, th in zip(PERCENTILES, thresholds):
        print(f"    P{p:>2d} = {th:.3f}")

    # 各レベルに分類して成績算出
    def classify(score):
        if score >= thresholds[4]: return "SS"
        if score >= thresholds[3]: return "S"
        if score >= thresholds[2]: return "A"
        if score >= thresholds[1]: return "B"
        if score >= thresholds[0]: return "C"
        return "D"

    stats = defaultdict(lambda: {"n": 0, "win": 0, "p3": 0, "win_ret": 0, "place_ret": 0, "scores": []})
    for score, fp2, hno, pay, pop in race_entries:
        lv = classify(score)
        s = stats[lv]
        s["n"] += 1
        s["scores"].append(score)
        if fp2 == 1:
            s["win"] += 1
            s["win_ret"] += get_win_pay(pay, hno)
        if fp2 <= 3:
            s["p3"] += 1
            s["place_ret"] += get_place_pay(pay, hno)

    print(f"\n  ■ パーセンタイル方式（純粋score区切り）:")
    print(f"  {'Lv':>4s} {'閾値':>8s} {'N':>6s} {'構成比':>6s} {'単的中':>6s} {'単回収':>7s} {'複的中':>6s} {'複回収':>7s} {'score範囲':>14s}")
    print(f"  {'-'*4} {'-'*8} {'-'*6} {'-'*6} {'-'*6} {'-'*7} {'-'*6} {'-'*7} {'-'*14}")
    for lv in LEVEL_ORDER:
        s = stats[lv]
        if s["n"] == 0: continue
        n = s["n"]
        pct = n / total * 100
        wr = s["win"] / n * 100
        w_roi = s["win_ret"] / n
        pr = s["p3"] / n * 100
        p_roi = s["place_ret"] / n
        sc = s["scores"]
        sc_min, sc_max = min(sc), max(sc)
        th_str = ""
        if lv == "SS": th_str = f"≥{thresholds[4]:.3f}"
        elif lv == "S": th_str = f"≥{thresholds[3]:.3f}"
        elif lv == "A": th_str = f"≥{thresholds[2]:.3f}"
        elif lv == "B": th_str = f"≥{thresholds[1]:.3f}"
        elif lv == "C": th_str = f"≥{thresholds[0]:.3f}"
        else: th_str = f"<{thresholds[0]:.3f}"
        print(f"  {lv:>4s} {th_str:>8s} {n:>6d} {pct:>5.1f}% {wr:>5.1f}% {w_roi:>6.1f}% {pr:>5.1f}% {p_roi:>6.1f}% {sc_min:.3f}-{sc_max:.3f}")

    # pop併用パターン
    print(f"\n  ■ パーセンタイル + 人気フィルター併用:")
    print(f"    SS: score≥P95 かつ pop≤2")
    print(f"    → pop条件不合格のSS → Sに降格")

    stats2 = defaultdict(lambda: {"n": 0, "win": 0, "p3": 0, "win_ret": 0, "place_ret": 0})
    for score, fp2, hno, pay, pop in race_entries:
        lv = classify(score)
        # SS人気フィルター
        if lv == "SS" and pop > 2:
            lv = "S"  # S降格
        s = stats2[lv]
        s["n"] += 1
        if fp2 == 1:
            s["win"] += 1
            s["win_ret"] += get_win_pay(pay, hno)
        if fp2 <= 3:
            s["p3"] += 1
            s["place_ret"] += get_place_pay(pay, hno)

    print(f"\n  {'Lv':>4s} {'N':>6s} {'構成比':>6s} {'単的中':>6s} {'単回収':>7s} {'複的中':>6s} {'複回収':>7s}")
    print(f"  {'-'*4} {'-'*6} {'-'*6} {'-'*6} {'-'*7} {'-'*6} {'-'*7}")
    for lv in LEVEL_ORDER:
        s = stats2[lv]
        if s["n"] == 0: continue
        n = s["n"]
        pct = n / total * 100
        wr = s["win"] / n * 100
        w_roi = s["win_ret"] / n
        pr = s["p3"] / n * 100
        p_roi = s["place_ret"] / n
        print(f"  {lv:>4s} {n:>6d} {pct:>5.1f}% {wr:>5.1f}% {w_roi:>6.1f}% {pr:>5.1f}% {p_roi:>6.1f}%")

    # SS pop1のみ
    print(f"\n  ■ SS: pop≤1限定パターン:")
    stats3 = defaultdict(lambda: {"n": 0, "win": 0, "p3": 0, "win_ret": 0, "place_ret": 0})
    for score, fp2, hno, pay, pop in race_entries:
        lv = classify(score)
        if lv == "SS" and pop > 1:
            lv = "S"
        s = stats3[lv]
        s["n"] += 1
        if fp2 == 1:
            s["win"] += 1
            s["win_ret"] += get_win_pay(pay, hno)
        if fp2 <= 3:
            s["p3"] += 1
            s["place_ret"] += get_place_pay(pay, hno)

    for lv in ["SS", "S"]:
        s = stats3[lv]
        if s["n"] == 0: continue
        n = s["n"]
        pct = n / total * 100
        wr = s["win"] / n * 100
        pr = s["p3"] / n * 100
        print(f"    {lv}: {n}R ({pct:.1f}%)  単的中{wr:.1f}%  複的中{pr:.1f}%")

    # SS+S人気フィルター
    print(f"\n  ■ SS: pop≤1 + S: pop≤2 パターン:")
    stats4 = defaultdict(lambda: {"n": 0, "win": 0, "p3": 0, "win_ret": 0, "place_ret": 0})
    for score, fp2, hno, pay, pop in race_entries:
        lv = classify(score)
        if lv == "SS" and pop > 1:
            lv = "S"
        if lv == "S" and pop > 2:
            lv = "A"
        s = stats4[lv]
        s["n"] += 1
        if fp2 == 1:
            s["win"] += 1
            s["win_ret"] += get_win_pay(pay, hno)
        if fp2 <= 3:
            s["p3"] += 1
            s["place_ret"] += get_place_pay(pay, hno)

    print(f"\n  {'Lv':>4s} {'N':>6s} {'構成比':>6s} {'単的中':>6s} {'単回収':>7s} {'複的中':>6s} {'複回収':>7s}")
    print(f"  {'-'*4} {'-'*6} {'-'*6} {'-'*6} {'-'*7} {'-'*6} {'-'*7}")
    for lv in LEVEL_ORDER:
        s = stats4[lv]
        if s["n"] == 0: continue
        n = s["n"]
        pct = n / total * 100
        wr = s["win"] / n * 100
        w_roi = s["win_ret"] / n
        pr = s["p3"] / n * 100
        p_roi = s["place_ret"] / n
        print(f"  {lv:>4s} {n:>6d} {pct:>5.1f}% {wr:>5.1f}% {w_roi:>6.1f}% {pr:>5.1f}% {p_roi:>6.1f}%")

    return thresholds


print("\n" + "=" * 90)
print("  自信度v4シミュレーション: 目標 SS5% S10% A15% B35% C25% D10%")
print("=" * 90)

jra_th = analyze(jra_data, "JRA")
nar_th = analyze(nar_data, "NAR")

# 最終まとめ
print(f"\n{'='*90}")
print(f"  最終閾値まとめ")
print(f"{'='*90}")
print(f"\n  JRA パーセンタイル閾値:")
for p, th, lv in zip(PERCENTILES, jra_th, ["D/C", "C/B", "B/A", "A/S", "S/SS"]):
    print(f"    {lv:>5s}: {th:.3f} (P{p})")
print(f"\n  NAR パーセンタイル閾値:")
for p, th, lv in zip(PERCENTILES, nar_th, ["D/C", "C/B", "B/A", "A/S", "S/SS"]):
    print(f"    {lv:>5s}: {th:.3f} (P{p})")

print(f"\n  処理時間: {time.time()-t0:.1f}s")
