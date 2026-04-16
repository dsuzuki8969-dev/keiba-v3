#!/usr/bin/env python3
"""
断層 × 着順 関連性分析（JRA/NAR分離版）
"""
import json, glob, os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
START, END = "20240101", "20260412"
JRA_VENUES = {'東京','中山','阪神','京都','中京','小倉','新潟','福島','函館','札幌'}

FINE_BUCKETS = [
    (0, 1, "0-1pt"), (1, 2, "1-2pt"), (2, 3, "2-3pt"), (3, 4, "3-4pt"),
    (4, 5, "4-5pt"), (5, 7, "5-7pt"), (7, 10, "7-10pt"), (10, 999, "10pt+"),
]
GAP_BUCKETS = [(2.5, 5.0, "2.5-5pt"), (5.0, 7.5, "5-7.5pt"), (7.5, 999, "7.5pt+")]

t0 = time.time()

# === データ読込 ===
all_results = {}
for fp in sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json"))):
    dt = os.path.basename(fp)[:8]
    if not (START <= dt <= END): continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for rid, data in d.items():
            all_results[rid] = {e["horse_no"]: e["finish"] for e in data.get("order", []) if "horse_no" in e and "finish" in e}
    except: pass

jra_races = []
nar_races = []
file_count = 0
pred_files = sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
target = [(fp, os.path.basename(fp)[:8]) for fp in pred_files
          if START <= os.path.basename(fp)[:8] <= END
          and '_prev' not in os.path.basename(fp) and '_backup' not in os.path.basename(fp)]

for i, (fp, dt) in enumerate(target):
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
    except: continue
    for r in d.get("races", []):
        rid = r.get("race_id", "")
        if rid not in all_results: continue
        venue = r.get("venue", "")
        if venue in JRA_VENUES:
            jra_races.append((r, all_results[rid]))
        else:
            nar_races.append((r, all_results[rid]))
    file_count += 1
    if (i+1) % 200 == 0:
        print(f"  [{(i+1)/len(target)*100:.0f}%] {i+1}/{len(target)} loaded", flush=True)

print(f"  JRA: {len(jra_races)}R  NAR: {len(nar_races)}R  ({time.time()-t0:.1f}s)")


def analyze_category(races, label):
    """1カテゴリの全分析"""
    print(f"\n{'='*70}")
    print(f"  ■ {label}  ({len(races):,}R)")
    print(f"{'='*70}")

    # --- 分析1: 1位-2位間の断層サイズ × 1位の成績 ---
    print(f"\n  【1】1位-2位間 断層サイズ × composite1位の成績")
    size_stats = {}
    for race, fm in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
        gap1 = (sh[0].get("composite", 0) or 0) - (sh[1].get("composite", 0) or 0)
        hno = sh[0].get("horse_no")
        fp = fm.get(hno, 99)
        if fp <= 0 or fp >= 90: continue
        for lo, hi, bn in FINE_BUCKETS:
            if lo <= gap1 < hi:
                if bn not in size_stats:
                    size_stats[bn] = {"n": 0, "win": 0, "p2": 0, "p3": 0}
                size_stats[bn]["n"] += 1
                if fp == 1: size_stats[bn]["win"] += 1
                if fp <= 2: size_stats[bn]["p2"] += 1
                if fp <= 3: size_stats[bn]["p3"] += 1
                break

    print(f"  {'断層':>8s} {'R数':>6s} {'勝率':>6s} {'連対':>6s} {'複勝':>6s}")
    print(f"  {'-'*8} {'-'*6} {'-'*6} {'-'*6} {'-'*6}")
    for _, _, bn in FINE_BUCKETS:
        s = size_stats.get(bn)
        if not s or s["n"] < 20: continue
        print(f"  {bn:>8s} {s['n']:>6d} {s['win']/s['n']*100:>5.1f}% {s['p2']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")

    # --- 分析2: レースタイプ × 本命的中率 ---
    print(f"\n  【2】レースタイプ × 本命的中率・上位3頭P3")
    type_stats = {}
    for race, fm in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
        gaps = [(sh[i-1].get("composite",0) or 0) - (sh[i].get("composite",0) or 0) for i in range(1, min(len(sh), 6))]
        if not gaps: continue
        g1, g2, g3 = (gaps + [0,0,0])[:3]
        mg = max(gaps[:5]) if gaps else 0
        if g1 >= 5:       rt = "独走型"
        elif g1 < 2 and g2 >= 4: rt = "2強型"
        elif g1 < 2 and g2 < 2 and g3 >= 3: rt = "3強型"
        elif mg < 2.5:    rt = "全混戦"
        else:             rt = "上位拮抗"

        if rt not in type_stats:
            type_stats[rt] = {"n": 0, "hw": 0, "hp3": 0, "t3_n": 0, "t3_p3": 0}
        type_stats[rt]["n"] += 1
        hno = sh[0].get("horse_no")
        fp = fm.get(hno, 99)
        if fp == 1: type_stats[rt]["hw"] += 1
        if fp <= 3: type_stats[rt]["hp3"] += 1
        for j in range(min(3, len(sh))):
            fp2 = fm.get(sh[j].get("horse_no"), 99)
            type_stats[rt]["t3_n"] += 1
            if fp2 > 0 and fp2 <= 3: type_stats[rt]["t3_p3"] += 1

    print(f"  {'タイプ':<10s} {'R数':>6s} {'本命勝率':>8s} {'本命P3':>7s} {'上位3頭P3':>9s}")
    print(f"  {'-'*10} {'-'*6} {'-'*8} {'-'*7} {'-'*9}")
    for rt in ["独走型", "2強型", "3強型", "上位拮抗", "全混戦"]:
        s = type_stats.get(rt)
        if not s or s["n"] < 10: continue
        print(f"  {rt:<10s} {s['n']:>6d} {s['hw']/s['n']*100:>7.1f}% {s['hp3']/s['n']*100:>6.1f}% {s['t3_p3']/max(1,s['t3_n'])*100:>8.1f}%")

    # --- 分析3: 断層上位群の3着以内独占率 ---
    print(f"\n  【3】最大断層の上位群 → 3着以内独占率")
    occ = {}
    for race, fm in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
        mg, mp = 0, 0
        for i in range(1, min(len(sh), 8)):
            g = (sh[i-1].get("composite",0) or 0) - (sh[i].get("composite",0) or 0)
            if g > mg: mg, mp = g, i
        if mg < 2.5: continue
        above = mp
        if above < 1 or above > 5: continue
        in3 = sum(1 for j in range(above) if 0 < fm.get(sh[j].get("horse_no"), 99) <= 3)
        for lo, hi, gb in GAP_BUCKETS:
            if lo <= mg < hi:
                key = (above, gb)
                if key not in occ: occ[key] = {"t": 0, "o": [0,0,0,0]}
                occ[key]["t"] += 1
                occ[key]["o"][min(in3, 3)] += 1
                break

    print(f"  {'上位':>4s} {'断層':>8s} {'R数':>5s} {'0頭':>5s} {'1頭':>5s} {'2頭':>5s} {'全員':>5s} {'平均':>5s}")
    print(f"  {'-'*4} {'-'*8} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*5}")
    for ab in [1, 2, 3, 4, 5]:
        for _, _, gb in GAP_BUCKETS:
            s = occ.get((ab, gb))
            if not s or s["t"] < 20: continue
            t = s["t"]; o = s["o"]
            avg = (o[1]*1 + o[2]*2 + o[3]*3) / t
            mx = min(ab, 3)
            print(f"  {ab}頭   {gb:>8s} {t:>5d} {o[0]/t*100:>4.1f}% {o[1]/t*100:>4.1f}% {o[2]/t*100:>4.1f}% {o[mx]/t*100:>4.1f}% {avg:>4.2f}")

    # --- 分析4: 断層位置別の上下差 ---
    print(f"\n  【4】断層位置 × サイズ → 上位群P3 vs 下位群P3")
    gs = {}
    for race, fm in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
        for i in range(1, min(len(sh), 6)):
            g = (sh[i-1].get("composite",0) or 0) - (sh[i].get("composite",0) or 0)
            if g < 2.5: continue
            for lo, hi, gb in GAP_BUCKETS:
                if lo <= g < hi:
                    key = (i, gb)
                    if key not in gs:
                        gs[key] = {"r": 0, "an": 0, "ap3": 0, "aw": 0, "bn": 0, "bp3": 0}
                    gs[key]["r"] += 1
                    for j in range(i):
                        fp = fm.get(sh[j].get("horse_no"), 99)
                        if 0 < fp < 90:
                            gs[key]["an"] += 1
                            if fp <= 3: gs[key]["ap3"] += 1
                            if fp == 1: gs[key]["aw"] += 1
                    for j in range(i, min(i+3, len(sh))):
                        fp = fm.get(sh[j].get("horse_no"), 99)
                        if 0 < fp < 90:
                            gs[key]["bn"] += 1
                            if fp <= 3: gs[key]["bp3"] += 1
                    break

    print(f"  {'位置':<10s} {'断層':>8s} {'R数':>5s} {'上位P3':>7s} {'下位P3':>7s} {'差':>6s}")
    print(f"  {'-'*10} {'-'*8} {'-'*5} {'-'*7} {'-'*7} {'-'*6}")
    for pos in range(1, 6):
        for _, _, gb in GAP_BUCKETS:
            s = gs.get((pos, gb))
            if not s or s["r"] < 30: continue
            ap = s["ap3"]/max(1,s["an"])*100
            bp = s["bp3"]/max(1,s["bn"])*100
            print(f"  {pos}位-{pos+1}位  {gb:>8s} {s['r']:>5d} {ap:>6.1f}% {bp:>6.1f}% {ap-bp:>+5.1f}")

    # --- 分析5: 断層の境界 ---
    print(f"\n  【5】断層境界効果（直上最下位 vs 直下最上位）")
    edge = {}
    for race, fm in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
        for i in range(1, min(len(sh), 8)):
            g = (sh[i-1].get("composite",0) or 0) - (sh[i].get("composite",0) or 0)
            if g < 2.5: continue
            for lo, hi, gb in GAP_BUCKETS:
                if lo <= g < hi:
                    if gb not in edge:
                        edge[gb] = {"an": 0, "ap": 0, "bn": 0, "bp": 0}
                    fa = fm.get(sh[i-1].get("horse_no"), 99)
                    fb = fm.get(sh[i].get("horse_no"), 99)
                    if 0 < fa < 90:
                        edge[gb]["an"] += 1
                        if fa <= 3: edge[gb]["ap"] += 1
                    if 0 < fb < 90:
                        edge[gb]["bn"] += 1
                        if fb <= 3: edge[gb]["bp"] += 1
                    break
            break  # 最初の断層のみ

    for gb in ["2.5-5pt", "5-7.5pt", "7.5pt+"]:
        s = edge.get(gb)
        if not s or s["an"] < 20: continue
        ap = s["ap"]/max(1,s["an"])*100
        bp = s["bp"]/max(1,s["bn"])*100
        print(f"    {gb:<10s}  上:{s['an']:>5d}頭 P3={ap:>5.1f}%  下:{s['bn']:>5d}頭 P3={bp:>5.1f}%  差={ap-bp:>+5.1f}")


# === 実行 ===
analyze_category(jra_races, "JRA")
analyze_category(nar_races, "NAR")

print(f"\n  処理時間: {time.time()-t0:.1f}s")
