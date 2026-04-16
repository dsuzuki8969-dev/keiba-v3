#!/usr/bin/env python3
"""
自信度 深掘り分析 — NAR SS逆転問題の原因特定
"""
import json, glob, os, sys, io, time
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

jra_races, nar_races = [], []
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
        if venue in JRA_VENUES: jra_races.append(entry)
        else: nar_races.append(entry)

print(f"  JRA: {len(jra_races)}R  NAR: {len(nar_races)}R  ({time.time()-t0:.1f}s)")


def get_honmei(race):
    for h in race.get("horses", []):
        if h.get("mark", "") in ("◉", "◎"):
            return h
    return None


# === 1. NAR SS逆転問題: SS内のscore帯別成績 ===
print(f"\n{'='*90}")
print(f"  1. NAR SS逆転問題の原因分析")
print(f"{'='*90}")

# SS内のscore分布
ss_scores = []
for race, fm, pay in nar_races:
    conf = race.get("confidence") or race.get("overall_confidence") or "B"
    if conf != "SS": continue
    score = race.get("confidence_score", 0) or 0
    honmei = get_honmei(race)
    if not honmei: continue
    hno = honmei.get("horse_no")
    fp2 = fm.get(hno, 99)
    if fp2 <= 0 or fp2 >= 90: continue
    ss_scores.append((score, fp2))

print(f"\n  NAR SS内 score帯別成績 (N={len(ss_scores)})")
for s_min, s_max, label in [(0.60, 0.65, "0.60-0.65"), (0.65, 0.70, "0.65-0.70"),
                              (0.70, 0.75, "0.70-0.75"), (0.75, 0.80, "0.75-0.80"),
                              (0.80, 0.85, "0.80-0.85"), (0.85, 1.01, "0.85+")]:
    bucket = [(s,f) for s,f in ss_scores if s_min <= s < s_max]
    if len(bucket) >= 10:
        wr = sum(1 for _,f in bucket if f == 1) / len(bucket) * 100
        pr = sum(1 for _,f in bucket if f <= 3) / len(bucket) * 100
        print(f"    {label}: {len(bucket):>5d}R  単的中{wr:>5.1f}%  複的中{pr:>5.1f}%")


# === 2. 全レベルのscore分布（JRA/NAR）===
print(f"\n{'='*90}")
print(f"  2. レベル別score分布")
print(f"{'='*90}")

for cat, races in [("JRA", jra_races), ("NAR", nar_races)]:
    level_scores = defaultdict(list)
    for race, fm, pay in races:
        conf = race.get("confidence") or race.get("overall_confidence") or "B"
        score = race.get("confidence_score", 0) or 0
        level_scores[conf].append(score)

    print(f"\n  {cat}:")
    for lv in ["SS", "S", "A", "B", "C", "D"]:
        scores = level_scores.get(lv, [])
        if not scores: continue
        scores.sort()
        avg = sum(scores) / len(scores)
        median = scores[len(scores)//2]
        mn, mx = scores[0], scores[-1]
        print(f"    {lv}: N={len(scores):>6d}  avg={avg:.3f}  median={median:.3f}  min={mn:.3f}  max={mx:.3f}")


# === 3. score閾値シミュレーション ===
print(f"\n{'='*90}")
print(f"  3. score閾値シミュレーション（新閾値案）")
print(f"{'='*90}")

for cat, races in [("JRA", jra_races), ("NAR", nar_races)]:
    print(f"\n  {cat}:")

    # 現行: SS=0.65, S=0.45, A=0.35, B=0.25, C=0.15
    # 案: 閾値を変えた場合の各レベルの成績
    thresholds_sets = [
        ("現行", {"SS": 0.65, "S": 0.45, "A": 0.35, "B": 0.25, "C": 0.15}),
        ("案1: SS↑", {"SS": 0.75, "S": 0.50, "A": 0.35, "B": 0.25, "C": 0.15}),
        ("案2: SS↑↑", {"SS": 0.80, "S": 0.55, "A": 0.40, "B": 0.25, "C": 0.15}),
        ("案3: 全↑", {"SS": 0.75, "S": 0.55, "A": 0.40, "B": 0.30, "C": 0.20}),
    ]

    for th_name, thresholds in thresholds_sets:
        sorted_th = sorted(thresholds.items(), key=lambda x: -x[1])

        stats = defaultdict(lambda: {"n": 0, "win": 0, "p3": 0})
        for race, fm, pay in races:
            score = race.get("confidence_score", 0) or 0
            honmei = get_honmei(race)
            if not honmei: continue
            hno = honmei.get("horse_no")
            fp2 = fm.get(hno, 99)
            if fp2 <= 0 or fp2 >= 90: continue

            # scoreからレベル判定
            level = "D"
            for lv, th in sorted_th:
                if score >= th:
                    level = lv
                    break
            # D以下はまとめる
            if level not in thresholds:
                level = "D"

            stats[level]["n"] += 1
            if fp2 == 1: stats[level]["win"] += 1
            if fp2 <= 3: stats[level]["p3"] += 1

        total = sum(s["n"] for s in stats.values())
        line = f"    {th_name:>8s}: "
        for lv in ["SS", "S", "A", "B", "C"]:
            s = stats[lv]
            if s["n"] > 0:
                wr = s["win"] / s["n"] * 100
                pct = s["n"] / total * 100
                line += f"{lv}={wr:.0f}%({pct:.0f}%) "
        print(line)


def _gap(race):
    horses = race.get("horses", [])
    if len(horses) < 2: return 0
    sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
    return (sh[0].get("composite", 0) or 0) - (sh[1].get("composite", 0) or 0)


def _honmei_pop(race):
    honmei = get_honmei(race)
    if not honmei: return 99
    return honmei.get("popularity") or 99

# === 4. NAR SS硬性条件の検証 ===
print(f"\n{'='*90}")
print(f"  4. NAR SS: gap/人気/頭数別の成績")
print(f"{'='*90}")

# SSレースの特徴別成績
for label, filter_fn in [
    ("全SS", lambda r: True),
    ("gap≥5", lambda r: _gap(r) >= 5.0),
    ("gap<5", lambda r: _gap(r) < 5.0),
    ("1人気◎", lambda r: _honmei_pop(r) == 1),
    ("2人気◎", lambda r: _honmei_pop(r) == 2),
    ("3人気以下◎", lambda r: _honmei_pop(r) >= 3),
    ("8頭以下", lambda r: len(r.get("horses", [])) <= 8),
    ("9頭以上", lambda r: len(r.get("horses", [])) >= 9),
    ("score≥0.75", lambda r: (r.get("confidence_score", 0) or 0) >= 0.75),
    ("score<0.75", lambda r: (r.get("confidence_score", 0) or 0) < 0.75),
]:
    b = {"n": 0, "win": 0, "p3": 0}
    for race, fm, pay in nar_races:
        conf = race.get("confidence") or race.get("overall_confidence") or "B"
        if conf != "SS": continue
        if not filter_fn(race): continue
        honmei = get_honmei(race)
        if not honmei: continue
        hno = honmei.get("horse_no")
        fp2 = fm.get(hno, 99)
        if fp2 <= 0 or fp2 >= 90: continue
        b["n"] += 1
        if fp2 == 1: b["win"] += 1
        if fp2 <= 3: b["p3"] += 1
    if b["n"] >= 10:
        print(f"  NAR SS {label:>12s}: {b['n']:>5d}R  単的中{b['win']/b['n']*100:>5.1f}%  複的中{b['p3']/b['n']*100:>5.1f}%")


def _gap(race):
    horses = race.get("horses", [])
    if len(horses) < 2: return 0
    sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
    return (sh[0].get("composite", 0) or 0) - (sh[1].get("composite", 0) or 0)


def _honmei_pop(race):
    honmei = get_honmei(race)
    if not honmei: return 99
    return honmei.get("popularity") or 99


# === 5. JRA SS硬性条件の検証 ===
print()
for label, filter_fn in [
    ("全SS", lambda r: True),
    ("gap≥5", lambda r: _gap(r) >= 5.0),
    ("gap<5", lambda r: _gap(r) < 5.0),
    ("1人気◎", lambda r: _honmei_pop(r) == 1),
    ("2人気◎", lambda r: _honmei_pop(r) == 2),
    ("3人気以下◎", lambda r: _honmei_pop(r) >= 3),
    ("score≥0.75", lambda r: (r.get("confidence_score", 0) or 0) >= 0.75),
    ("score<0.75", lambda r: (r.get("confidence_score", 0) or 0) < 0.75),
]:
    b = {"n": 0, "win": 0, "p3": 0}
    for race, fm, pay in jra_races:
        conf = race.get("confidence") or race.get("overall_confidence") or "B"
        if conf != "SS": continue
        if not filter_fn(race): continue
        honmei = get_honmei(race)
        if not honmei: continue
        hno = honmei.get("horse_no")
        fp2 = fm.get(hno, 99)
        if fp2 <= 0 or fp2 >= 90: continue
        b["n"] += 1
        if fp2 == 1: b["win"] += 1
        if fp2 <= 3: b["p3"] += 1
    if b["n"] >= 10:
        print(f"  JRA SS {label:>12s}: {b['n']:>5d}R  単的中{b['win']/b['n']*100:>5.1f}%  複的中{b['p3']/b['n']*100:>5.1f}%")


# === 6. S vs SS逆転チェック（各score帯） ===
print(f"\n{'='*90}")
print(f"  5. 全レベル score帯別単的中率（逆転チェック）")
print(f"{'='*90}")

for cat, races in [("JRA", jra_races), ("NAR", nar_races)]:
    print(f"\n  {cat}:")
    score_perf = defaultdict(lambda: {"n": 0, "win": 0})
    for race, fm, pay in races:
        score = race.get("confidence_score", 0) or 0
        honmei = get_honmei(race)
        if not honmei: continue
        hno = honmei.get("horse_no")
        fp2 = fm.get(hno, 99)
        if fp2 <= 0 or fp2 >= 90: continue
        bucket = round(score, 2)
        # 0.05刻み
        b5 = int(bucket * 20) / 20
        score_perf[f"{b5:.2f}"]["n"] += 1
        if fp2 == 1: score_perf[f"{b5:.2f}"]["win"] += 1

    print(f"  {'score':>6s} {'N':>6s} {'単的中':>6s}")
    for k in sorted(score_perf.keys()):
        s = score_perf[k]
        if s["n"] >= 30:
            print(f"  {k:>6s} {s['n']:>6d} {s['win']/s['n']*100:>5.1f}%")


print(f"\n  処理時間: {time.time()-t0:.1f}s")
