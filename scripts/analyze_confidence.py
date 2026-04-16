#!/usr/bin/env python3
"""
自信度（Confidence）精査スクリプト

分析項目:
  1. 自信度レベル別の出現率・的中率・回収率
  2. JRA/NAR別の分布
  3. confidence_score帯別の実績
  4. SS硬性条件の効果検証
  5. 改善可能ポイントの特定
"""
import json
import glob
import os
import sys
import io
import time
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True, errors="replace")

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
JRA_VENUES = {"東京", "中山", "阪神", "京都", "中京", "小倉", "新潟", "福島", "函館", "札幌"}

t0 = time.time()

# === 結果データ読込 ===
all_results = {}
all_payouts = {}
for fp in sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json"))):
    try:
        d = json.load(open(fp, "r", encoding="utf-8"))
        for rid, data in d.items():
            all_results[rid] = {
                e["horse_no"]: e.get("finish", 99)
                for e in data.get("order", [])
                if "horse_no" in e
            }
            pay = data.get("payouts", {})
            if pay:
                all_payouts[rid] = pay
    except Exception:
        pass


def get_win_pay(pay, hno):
    w = pay.get("単勝", None)
    if w is None:
        return 0
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
    if w is None:
        return 0
    hs = str(hno)
    if isinstance(w, list):
        for x in w:
            if isinstance(x, dict) and str(x.get("combo", "")) == hs:
                return x.get("payout", 0) or 0
    if isinstance(w, dict):
        return (w.get("payout", 0) or 0) if str(w.get("combo", "")) == hs else 0
    return 0


# === 予想データ読込 ===
jra_races = []
nar_races = []
target = [
    (fp, os.path.basename(fp)[:8])
    for fp in sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
    if "_prev" not in os.path.basename(fp) and "_backup" not in os.path.basename(fp)
]

for i, (fp, dt) in enumerate(target):
    try:
        d = json.load(open(fp, encoding="utf-8"))
    except Exception:
        continue
    for r in d.get("races", []):
        rid = r.get("race_id", "")
        if rid not in all_results:
            continue
        venue = r.get("venue", "")
        fm = all_results[rid]
        pay = all_payouts.get(rid, {})
        entry = (r, fm, pay)
        if venue in JRA_VENUES:
            jra_races.append(entry)
        else:
            nar_races.append(entry)
    if (i + 1) % 200 == 0:
        print(f"  [{(i+1)/len(target)*100:.0f}%] {i+1}/{len(target)} loaded", flush=True)

print(f"  JRA: {len(jra_races)}R  NAR: {len(nar_races)}R  ({time.time()-t0:.1f}s)")


def analyze_confidence(races, label):
    """自信度レベル別の成績分析"""
    levels = ["SS", "S", "A", "B", "C", "D", "E"]
    stats = defaultdict(lambda: {
        "n": 0, "win_hit": 0, "win_ret": 0,
        "place_hit": 0, "place_ret": 0,
        "scores": [],
    })

    # score帯別（0.1刻み）
    score_stats = defaultdict(lambda: {
        "n": 0, "win_hit": 0, "win_ret": 0,
        "place_hit": 0, "place_ret": 0,
    })

    for race, fm, pay in races:
        conf = race.get("confidence") or race.get("overall_confidence") or "B"
        score = race.get("confidence_score", 0) or 0
        horses = race.get("horses", [])

        # ◎馬（本命）を特定
        honmei = None
        for h in horses:
            mark = h.get("mark", "")
            if mark in ("◉", "◎"):
                honmei = h
                break
        if honmei is None:
            continue

        hno = honmei.get("horse_no")
        fp2 = fm.get(hno, 99)
        if fp2 <= 0 or fp2 >= 90:
            continue

        win_p = get_win_pay(pay, hno)
        place_p = get_place_pay(pay, hno)

        # 自信度レベル別
        s = stats[conf]
        s["n"] += 1
        s["scores"].append(score)
        if fp2 == 1:
            s["win_hit"] += 1
            s["win_ret"] += win_p
        if fp2 <= 3:
            s["place_hit"] += 1
            s["place_ret"] += place_p

        # score帯別
        bucket = f"{int(score * 10) / 10:.1f}"
        ss = score_stats[bucket]
        ss["n"] += 1
        if fp2 == 1:
            ss["win_hit"] += 1
            ss["win_ret"] += win_p
        if fp2 <= 3:
            ss["place_hit"] += 1
            ss["place_ret"] += place_p

    # 出力
    print(f"\n{'='*90}")
    print(f"  {label} 自信度レベル別成績")
    print(f"{'='*90}")
    print(f"  {'レベル':>6s} {'レース数':>7s} {'構成比':>6s} {'単的中':>6s} {'単回収':>7s} {'複的中':>6s} {'複回収':>7s} {'平均score':>9s}")
    print(f"  {'-'*6} {'-'*7} {'-'*6} {'-'*6} {'-'*7} {'-'*6} {'-'*7} {'-'*9}")

    total_n = sum(s["n"] for s in stats.values())
    for lv in levels:
        s = stats[lv]
        if s["n"] == 0:
            continue
        n = s["n"]
        pct = n / total_n * 100
        wr = s["win_hit"] / n * 100
        w_roi = s["win_ret"] / n if n > 0 else 0
        pr = s["place_hit"] / n * 100
        p_roi = s["place_ret"] / n if n > 0 else 0
        avg_sc = sum(s["scores"]) / len(s["scores"]) if s["scores"] else 0
        print(
            f"  {lv:>6s} {n:>7d} {pct:>5.1f}% {wr:>5.1f}% {w_roi:>6.1f}% {pr:>5.1f}% {p_roi:>6.1f}% {avg_sc:>8.3f}"
        )

    print(f"  {'合計':>6s} {total_n:>7d}")

    # score帯別
    print(f"\n  ■ confidence_score帯別成績")
    print(f"  {'score帯':>7s} {'レース数':>7s} {'単的中':>6s} {'単回収':>7s} {'複的中':>6s} {'複回収':>7s}")
    print(f"  {'-'*7} {'-'*7} {'-'*6} {'-'*7} {'-'*6} {'-'*7}")
    for bucket in sorted(score_stats.keys()):
        ss = score_stats[bucket]
        if ss["n"] < 20:
            continue
        n = ss["n"]
        wr = ss["win_hit"] / n * 100
        w_roi = ss["win_ret"] / n if n > 0 else 0
        pr = ss["place_hit"] / n * 100
        p_roi = ss["place_ret"] / n if n > 0 else 0
        print(
            f"  {bucket:>7s} {n:>7d} {wr:>5.1f}% {w_roi:>6.1f}% {pr:>5.1f}% {p_roi:>6.1f}%"
        )

    return stats


jra_stats = analyze_confidence(jra_races, "JRA")
nar_stats = analyze_confidence(nar_races, "NAR")


# === KPI目標との比較 ===
print(f"\n{'='*90}")
print(f"  KPI目標との比較")
print(f"{'='*90}")

kpi_targets = {
    "SS": {"win": 60, "win_roi": 100, "place": 85, "place_roi": 100},
    "S":  {"win": 45, "win_roi": 90, "place": 75, "place_roi": 95},
    "A":  {"win": 35, "win_roi": 85, "place": 65, "place_roi": 90},
    "B":  {"win": 25, "win_roi": 80, "place": 55, "place_roi": 85},
    "C":  {"win": 15, "win_roi": 70, "place": 40, "place_roi": 75},
}

for cat, stats in [("JRA", jra_stats), ("NAR", nar_stats)]:
    print(f"\n  {cat}:")
    print(f"  {'レベル':>6s} {'単的中(実)':>9s} {'(目標)':>6s} {'差':>5s}  {'単回収(実)':>9s} {'(目標)':>6s}  {'複的中(実)':>9s} {'(目標)':>6s} {'差':>5s}  {'複回収(実)':>9s} {'(目標)':>6s}")
    for lv in ["SS", "S", "A", "B", "C"]:
        s = stats[lv]
        if s["n"] == 0:
            continue
        n = s["n"]
        t = kpi_targets.get(lv, {})
        wr = s["win_hit"] / n * 100
        w_roi = s["win_ret"] / n
        pr = s["place_hit"] / n * 100
        p_roi = s["place_ret"] / n
        tw = t.get("win", 0)
        twr = t.get("win_roi", 0)
        tp = t.get("place", 0)
        tpr = t.get("place_roi", 0)
        wd = wr - tw
        pd = pr - tp
        print(
            f"  {lv:>6s} {wr:>8.1f}% {tw:>5.0f}% {wd:>+5.1f}  {w_roi:>8.1f}% {twr:>5.0f}%  {pr:>8.1f}% {tp:>5.0f}% {pd:>+5.1f}  {p_roi:>8.1f}% {tpr:>5.0f}%"
        )

print(f"\n  処理時間: {time.time()-t0:.1f}s")
