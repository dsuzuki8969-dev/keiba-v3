#!/usr/bin/env python3
"""
自信度 正規分布型対称構造シミュレーション

マスター案:
  SS: 5%（出たらほぼ当たる＝60%以上）
  S: 10%（かなり自信ある＝45%以上）
  A: 15%（やや自信ある）
  B: 20%（基本だがプラス）
  C: 20%（基本だがマイナス）
  D: 15%（やや自信ない）
  E: 10%（かなり自信ない＝当たらない率45%以上）
  F: 5%（出たらほぼ外れる＝当たらない率60%以上）

対称: SS↔F, S↔E, A↔D, B↔C
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


# 対称構造: F(5%) D(15%) C(20%) B(20%) A(15%) S(10%) SS(5%)
# 累積パーセンタイル: 5, 15, 35, 55, 70, 85, 95
SYMMETRIC = {
    "labels": ["F", "E", "D", "C", "B", "A", "S", "SS"],
    "pcts":   [ 5,  10,  15,  20,  20,  15,  10,   5],
    "cum_pcts": [5, 15, 30, 50, 70, 85, 95, 100],  # 累積
    "percentiles": [5, 15, 30, 50, 70, 85, 95],  # 区切り点
}


def analyze(data, label):
    # 全エントリ収集
    entries = []
    for race, fm, pay in data:
        score = race.get("confidence_score", 0) or 0
        honmei = get_honmei(race)
        if not honmei: continue
        hno = honmei.get("horse_no")
        fp2 = fm.get(hno, 99)
        if fp2 <= 0 or fp2 >= 90: continue
        pop = honmei.get("popularity") or 99
        entries.append((score, fp2, hno, pay, pop))

    scores = np.array([e[0] for e in entries])
    total = len(scores)

    # パーセンタイル閾値算出
    pcts = SYMMETRIC["percentiles"]
    thresholds = np.percentile(scores, pcts)

    print(f"\n{'='*90}")
    print(f"  {label} 正規分布型対称構造 (N={total})")
    print(f"{'='*90}")

    print(f"\n  ■ パーセンタイル閾値:")
    labels = ["F/E", "E/D", "D/C", "C/B", "B/A", "A/S", "S/SS"]
    for lb, p, th in zip(labels, pcts, thresholds):
        print(f"    {lb:>5s} = {th:.3f} (P{p})")

    def classify(score):
        if score >= thresholds[6]: return "SS"
        if score >= thresholds[5]: return "S"
        if score >= thresholds[4]: return "A"
        if score >= thresholds[3]: return "B"
        if score >= thresholds[2]: return "C"
        if score >= thresholds[1]: return "D"
        if score >= thresholds[0]: return "E"
        return "F"

    # === パターン1: 純粋パーセンタイル ===
    def run_pattern(name, classify_fn):
        stats = defaultdict(lambda: {"n": 0, "win": 0, "p3": 0, "win_ret": 0, "place_ret": 0})
        for score, fp2, hno, pay, pop in entries:
            lv = classify_fn(score, pop)
            s = stats[lv]
            s["n"] += 1
            if fp2 == 1:
                s["win"] += 1
                s["win_ret"] += get_win_pay(pay, hno)
            if fp2 <= 3:
                s["p3"] += 1
                s["place_ret"] += get_place_pay(pay, hno)

        print(f"\n  ■ {name}:")
        print(f"  {'Lv':>4s} {'N':>6s} {'構成比':>6s} {'目標':>5s} {'単的中':>6s} {'単回収':>7s} {'複的中':>6s} {'複回収':>7s} {'◎外し率':>7s}")
        print(f"  {'-'*4} {'-'*6} {'-'*6} {'-'*5} {'-'*6} {'-'*7} {'-'*6} {'-'*7} {'-'*7}")
        target_pcts = {"F": 5, "E": 10, "D": 15, "C": 20, "B": 20, "A": 15, "S": 10, "SS": 5}
        for lv in ["F", "E", "D", "C", "B", "A", "S", "SS"]:
            s = stats[lv]
            if s["n"] == 0: continue
            n = s["n"]
            pct = n / total * 100
            wr = s["win"] / n * 100
            w_roi = s["win_ret"] / n
            pr = s["p3"] / n * 100
            p_roi = s["place_ret"] / n
            miss = (n - s["win"]) / n * 100  # ◎外し率
            tgt = target_pcts.get(lv, 0)
            print(f"  {lv:>4s} {n:>6d} {pct:>5.1f}% {tgt:>4d}% {wr:>5.1f}% {w_roi:>6.1f}% {pr:>5.1f}% {p_roi:>6.1f}% {miss:>6.1f}%")

        return stats

    # パターン1: 純粋パーセンタイル
    run_pattern("パターン1: 純粋パーセンタイル（popフィルターなし）",
                lambda s, p: classify(s))

    # パターン2: SS pop≤1, S pop≤2
    def classify_pop1(score, pop):
        lv = classify(score)
        if lv == "SS" and pop > 1: lv = "S"
        if lv == "S" and pop > 2: lv = "A"
        return lv
    run_pattern("パターン2: SS pop≤1 + S pop≤2",
                classify_pop1)

    # パターン3: SS pop≤2, S pop≤3
    def classify_pop2(score, pop):
        lv = classify(score)
        if lv == "SS" and pop > 2: lv = "S"
        if lv == "S" and pop > 3: lv = "A"
        return lv
    run_pattern("パターン3: SS pop≤2 + S pop≤3",
                classify_pop2)

    # === 対称性検証: F/Eの「外し率」がSS/Sの「当たり率」と対称か ===
    print(f"\n  ■ 対称性チェック:")
    stats_pure = defaultdict(lambda: {"n": 0, "win": 0})
    for score, fp2, hno, pay, pop in entries:
        lv = classify(score)
        stats_pure[lv]["n"] += 1
        if fp2 == 1: stats_pure[lv]["win"] += 1

    for pos, neg in [("SS", "F"), ("S", "E"), ("A", "D"), ("B", "C")]:
        sp = stats_pure[pos]
        sn = stats_pure[neg]
        if sp["n"] > 0 and sn["n"] > 0:
            wr_pos = sp["win"] / sp["n"] * 100
            wr_neg = sn["win"] / sn["n"] * 100
            miss_neg = 100 - wr_neg
            print(f"    {pos}当たり率={wr_pos:.1f}%  ↔  {neg}外し率={miss_neg:.1f}%  差={abs(wr_pos-miss_neg):.1f}pt")

    return thresholds


print("\n" + "=" * 90)
print("  正規分布型対称構造: F5% E10% D15% C20% B20% A15% S10% SS5%")
print("=" * 90)

jra_th = analyze(jra_data, "JRA")
nar_th = analyze(nar_data, "NAR")

# まとめ
print(f"\n{'='*90}")
print(f"  閾値まとめ")
print(f"{'='*90}")
labels = ["F/E", "E/D", "D/C", "C/B", "B/A", "A/S", "S/SS"]
print(f"\n  {'':>5s} {'JRA':>8s} {'NAR':>8s}")
for lb, jt, nt in zip(labels, jra_th, nar_th):
    print(f"  {lb:>5s} {jt:>8.3f} {nt:>8.3f}")

print(f"\n  処理時間: {time.time()-t0:.1f}s")
