#!/usr/bin/env python3
"""
×印v2適用後の検証スクリプト
修正後のpred.jsonの×印を結果データと突き合わせて複勝率を確認する。
"""
import json, glob, os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
START = "20240101"
END = "20260412"

print(f"{'='*70}")
print(f"  x印v2 検証  {START} -> {END}")
print(f"{'='*70}")

t0 = time.time()

# 結果データ読込
all_results = {}
for fp in sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json"))):
    dt = os.path.basename(fp)[:8]
    if not (START <= dt <= END):
        continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for race_id, data in d.items():
            fm = {}
            for entry in data.get("order", []):
                hno = entry.get("horse_no")
                fin = entry.get("finish")
                if hno is not None and fin is not None:
                    fm[hno] = fin
            all_results[race_id] = fm
    except Exception:
        pass
print(f"  結果データ: {len(all_results)} races")

# pred.json読込 & ×印集計
x_total = 0
x_win = 0
x_p2 = 0
x_p3 = 0
pop_stats = {1: {"n": 0, "p3": 0}, 2: {"n": 0, "p3": 0}, 3: {"n": 0, "p3": 0}}
x_races = 0
total_races = 0

# 期間別集計
period_stats = {}  # "YYYY-QN" -> {total, win, p3}

for fp in sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json"))):
    bn = os.path.basename(fp)
    dt = bn[:8]
    if not (START <= dt <= END) or '_prev' in bn or '_backup' in bn:
        continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
    except Exception:
        continue

    for race in d.get("races", []):
        total_races += 1
        race_id = race.get("race_id", "")
        finish_map = all_results.get(race_id, {})
        if not finish_map:
            continue

        has_x = False
        for h in race.get("horses", []):
            if h.get("mark") != "\u00d7":
                continue
            has_x = True
            hno = h.get("horse_no")
            fp_val = finish_map.get(hno)
            if fp_val is None or fp_val <= 0:
                continue

            x_total += 1
            pop = h.get("popularity", 0)
            if fp_val == 1: x_win += 1
            if fp_val <= 2: x_p2 += 1
            if fp_val <= 3: x_p3 += 1

            if pop in pop_stats:
                pop_stats[pop]["n"] += 1
                if fp_val <= 3:
                    pop_stats[pop]["p3"] += 1

            # 四半期集計
            year = dt[:4]
            month = int(dt[4:6])
            q = (month - 1) // 3 + 1
            key = f"{year}-Q{q}"
            if key not in period_stats:
                period_stats[key] = {"total": 0, "win": 0, "p3": 0}
            period_stats[key]["total"] += 1
            if fp_val == 1: period_stats[key]["win"] += 1
            if fp_val <= 3: period_stats[key]["p3"] += 1

        if has_x:
            x_races += 1

elapsed = time.time() - t0

print(f"  予想レース: {total_races}R")
print(f"  x付きレース: {x_races}R ({x_races/max(1,total_races)*100:.1f}%)")

print(f"\n{'='*70}")
print(f"  x印v2 成績")
print(f"{'='*70}")
if x_total > 0:
    print(f"  x印数(結果あり): {x_total}")
    print(f"  勝率:   {x_win/x_total*100:.1f}% ({x_win}/{x_total})")
    print(f"  連対率: {x_p2/x_total*100:.1f}% ({x_p2}/{x_total})")
    print(f"  複勝率: {x_p3/x_total*100:.1f}% ({x_p3}/{x_total})")
    print(f"\n  人気別:")
    for pop in [1, 2, 3]:
        s = pop_stats[pop]
        if s["n"] > 0:
            print(f"    {pop}番人気: {s['n']}頭, 複勝率 {s['p3']/s['n']*100:.1f}%")
        else:
            print(f"    {pop}番人気: 0頭")

    print(f"\n  四半期推移:")
    for key in sorted(period_stats.keys()):
        ps = period_stats[key]
        if ps["total"] > 0:
            wr = ps["win"] / ps["total"] * 100
            p3r = ps["p3"] / ps["total"] * 100
            print(f"    {key}: {ps['total']:>4d}頭  勝率{wr:>5.1f}%  複勝率{p3r:>5.1f}%")
else:
    print(f"  x印データなし")

print(f"\n  処理時間: {elapsed:.1f}s")
