#!/usr/bin/env python3
"""
断層（composite gap）と着順の関連性分析

分析軸:
  1. 断層の位置（1-2位間, 2-3位間, ...）× 断層サイズ → 断層上位馬の複勝率
  2. レースタイプ分類（独走型/2強型/3強型/全混戦/上位拮抗）× 的中率
  3. 最大断層の上下で着順がどう分かれるか
  4. 断層サイズ別の予測精度（断層が大きいほど予測が当たるか）
"""
import json, glob, os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
START = "20240101"
END = "20260412"

print(f"{'='*70}")
print(f"  断層(composite gap) × 着順 関連性分析")
print(f"  期間: {START} -> {END}")
print(f"{'='*70}")

t0 = time.time()

# === データ読込 ===
all_results = {}
for fp in sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json"))):
    dt = os.path.basename(fp)[:8]
    if not (START <= dt <= END): continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for race_id, data in d.items():
            fm = {e["horse_no"]: e["finish"] for e in data.get("order", []) if "horse_no" in e and "finish" in e}
            all_results[race_id] = fm
    except: pass

races = []
for fp in sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json"))):
    bn = os.path.basename(fp)
    dt = bn[:8]
    if not (START <= dt <= END) or '_prev' in bn or '_backup' in bn: continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for r in d.get("races", []):
            rid = r.get("race_id", "")
            if rid in all_results:
                races.append((r, all_results[rid]))
    except: pass

load_time = time.time() - t0
print(f"  データ読込: {len(races)}R ({load_time:.1f}s)")

# =================================================================
# 分析1: 断層位置 × サイズ → 断層上位馬の成績
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析1】断層の位置×サイズ → 上位群の複勝率")
print(f"{'='*70}")

# gap_position: 1=1位-2位間, 2=2位-3位間, ...
# gap_size_bucket: [2.5, 5), [5, 7.5), [7.5, 10), [10+)
GAP_BUCKETS = [(2.5, 5.0, "2.5-5pt"), (5.0, 7.5, "5-7.5pt"), (7.5, 10.0, "7.5-10pt"), (10.0, 999, "10pt+")]

# 断層上の馬群（断層より上位）が3着以内に何頭入るか
gap_stats = {}  # (position, bucket_name) -> {races, above_in_top3, above_total, below_in_top3, below_total}

for race, finish_map in races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)

    # 全断層を検出
    for i in range(1, min(len(sorted_h), 10)):  # 上位10位まで
        prev_comp = sorted_h[i-1].get("composite", 0) or 0
        cur_comp = sorted_h[i].get("composite", 0) or 0
        gap = prev_comp - cur_comp
        if gap < 2.5: continue

        pos = i  # 断層位置（i位とi+1位の間）
        for lo, hi, bname in GAP_BUCKETS:
            if lo <= gap < hi:
                key = (pos, bname)
                if key not in gap_stats:
                    gap_stats[key] = {"races": 0, "above_p3": 0, "above_n": 0, "below_p3": 0, "below_n": 0, "above_win": 0}
                gap_stats[key]["races"] += 1

                # 断層上位群（0 ~ i-1）
                for j in range(i):
                    hno = sorted_h[j].get("horse_no")
                    fp = finish_map.get(hno, 99)
                    if fp > 0 and fp < 90:
                        gap_stats[key]["above_n"] += 1
                        if fp <= 3: gap_stats[key]["above_p3"] += 1
                        if fp == 1: gap_stats[key]["above_win"] += 1

                # 断層直下（i位 ~ i+2位、3頭分）
                for j in range(i, min(i + 3, len(sorted_h))):
                    hno = sorted_h[j].get("horse_no")
                    fp = finish_map.get(hno, 99)
                    if fp > 0 and fp < 90:
                        gap_stats[key]["below_n"] += 1
                        if fp <= 3: gap_stats[key]["below_p3"] += 1
                break

print(f"\n  {'断層位置':<12s} {'サイズ':<10s} {'レース数':>7s} {'上位群P3':>8s} {'上位群Win':>9s} {'下位群P3':>8s} {'差':>6s}")
print(f"  {'-'*12} {'-'*10} {'-'*7} {'-'*8} {'-'*9} {'-'*8} {'-'*6}")
for pos in range(1, 8):
    for _, _, bname in GAP_BUCKETS:
        key = (pos, bname)
        s = gap_stats.get(key)
        if not s or s["races"] < 30: continue
        above_p3 = s["above_p3"] / max(1, s["above_n"]) * 100
        above_win = s["above_win"] / max(1, s["above_n"]) * 100
        below_p3 = s["below_p3"] / max(1, s["below_n"]) * 100
        diff = above_p3 - below_p3
        print(f"  {pos}位-{pos+1}位間  {bname:<10s} {s['races']:>7d} {above_p3:>7.1f}% {above_win:>8.1f}% {below_p3:>7.1f}% {diff:>+5.1f}")

# =================================================================
# 分析2: レースタイプ分類 × 的中率
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析2】レースタイプ分類 × 本命的中率")
print(f"{'='*70}")

# 分類: 独走型(gap1>=5), 2強型(gap1<2,gap2>=4), 3強型(gap1<2,gap2<2,gap3>=3),
#        全混戦(max_gap<2.5), 上位拮抗(else)
type_stats = {}  # type_name -> {races, honmei_win, honmei_p3, top3_in_top3}

for race, finish_map in races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)

    gaps = []
    for i in range(1, min(len(sorted_h), 6)):
        g = (sorted_h[i-1].get("composite", 0) or 0) - (sorted_h[i].get("composite", 0) or 0)
        gaps.append(g)

    if not gaps: continue
    gap1 = gaps[0] if len(gaps) > 0 else 0
    gap2 = gaps[1] if len(gaps) > 1 else 0
    gap3 = gaps[2] if len(gaps) > 2 else 0
    max_gap = max(gaps[:5]) if gaps else 0

    if gap1 >= 5:
        rtype = "独走型"
    elif gap1 < 2 and gap2 >= 4:
        rtype = "2強型"
    elif gap1 < 2 and gap2 < 2 and gap3 >= 3:
        rtype = "3強型"
    elif max_gap < 2.5:
        rtype = "全混戦"
    else:
        rtype = "上位拮抗"

    if rtype not in type_stats:
        type_stats[rtype] = {"races": 0, "honmei_win": 0, "honmei_p3": 0, "top3_in_top3": 0, "top3_total": 0}
    type_stats[rtype]["races"] += 1

    # composite1位（本命）の成績
    top_hno = sorted_h[0].get("horse_no")
    fp = finish_map.get(top_hno, 99)
    if fp == 1: type_stats[rtype]["honmei_win"] += 1
    if fp <= 3: type_stats[rtype]["honmei_p3"] += 1

    # composite上位3頭が3着以内に入った数
    for j in range(min(3, len(sorted_h))):
        hno = sorted_h[j].get("horse_no")
        fp = finish_map.get(hno, 99)
        type_stats[rtype]["top3_total"] += 1
        if fp > 0 and fp <= 3:
            type_stats[rtype]["top3_in_top3"] += 1

print(f"\n  {'タイプ':<10s} {'レース数':>7s} {'本命勝率':>8s} {'本命P3':>7s} {'上位3頭P3':>9s}")
print(f"  {'-'*10} {'-'*7} {'-'*8} {'-'*7} {'-'*9}")
for rtype in ["独走型", "2強型", "3強型", "上位拮抗", "全混戦"]:
    s = type_stats.get(rtype)
    if not s or s["races"] < 10: continue
    hw = s["honmei_win"] / s["races"] * 100
    hp3 = s["honmei_p3"] / s["races"] * 100
    t3p3 = s["top3_in_top3"] / max(1, s["top3_total"]) * 100
    print(f"  {rtype:<10s} {s['races']:>7d} {hw:>7.1f}% {hp3:>6.1f}% {t3p3:>8.1f}%")

# =================================================================
# 分析3: 最大断層の上下の馬群 → 着順の集中度
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析3】最大断層による馬群分離 → 3着以内独占率")
print(f"{'='*70}")

# 各レースの最大断層位置で上下に分け、上位群が3着以内を何席占めるか
occupy_stats = {}  # above_count -> {total_races, occupy_0, occupy_1, occupy_2, occupy_3}
# above_countは断層上位の馬数

for race, finish_map in races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)

    # 最大断層を検出
    max_gap = 0
    max_gap_pos = 0
    for i in range(1, min(len(sorted_h), 8)):
        g = (sorted_h[i-1].get("composite", 0) or 0) - (sorted_h[i].get("composite", 0) or 0)
        if g > max_gap:
            max_gap = g
            max_gap_pos = i

    if max_gap < 2.5: continue  # 断層なし

    above_count = max_gap_pos  # 断層上位の馬数
    if above_count < 1 or above_count > 5: continue

    # 上位群が3着以内に何頭入ったか
    in_top3 = 0
    for j in range(above_count):
        hno = sorted_h[j].get("horse_no")
        fp = finish_map.get(hno, 99)
        if fp > 0 and fp <= 3:
            in_top3 += 1

    # 断層サイズバケット
    if max_gap < 5:
        gb = "2.5-5pt"
    elif max_gap < 7.5:
        gb = "5-7.5pt"
    else:
        gb = "7.5pt+"

    key = (above_count, gb)
    if key not in occupy_stats:
        occupy_stats[key] = {"total": 0, "occupy": [0, 0, 0, 0]}
    occupy_stats[key]["total"] += 1
    occupy_stats[key]["occupy"][min(in_top3, 3)] += 1

print(f"\n  {'上位馬数':<8s} {'断層':>8s} {'レース数':>7s} {'0頭的中':>7s} {'1頭':>5s} {'2頭':>5s} {'全員':>5s} {'平均':>5s}")
print(f"  {'-'*8} {'-'*8} {'-'*7} {'-'*7} {'-'*5} {'-'*5} {'-'*5} {'-'*5}")
for above in [1, 2, 3, 4, 5]:
    for gb in ["2.5-5pt", "5-7.5pt", "7.5pt+"]:
        key = (above, gb)
        s = occupy_stats.get(key)
        if not s or s["total"] < 20: continue
        t = s["total"]
        occ = s["occupy"]
        # 平均的中数
        avg = (occ[1]*1 + occ[2]*2 + occ[3]*3) / t
        max_possible = min(above, 3)
        print(f"  {above}頭      {gb:>8s} {t:>7d} {occ[0]/t*100:>6.1f}% {occ[1]/t*100:>4.1f}% {occ[2]/t*100:>4.1f}% {occ[min(max_possible,3)]/t*100:>4.1f}% {avg:>4.2f}")

# =================================================================
# 分析4: 断層サイズ別の予測精度（断層が大きいほど信頼できるか）
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析4】1位-2位間の断層サイズ × composite1位の成績")
print(f"{'='*70}")

size_stats = {}  # gap_bucket -> {n, win, p2, p3}
FINE_BUCKETS = [
    (0, 1, "0-1pt"), (1, 2, "1-2pt"), (2, 3, "2-3pt"), (3, 4, "3-4pt"),
    (4, 5, "4-5pt"), (5, 7, "5-7pt"), (7, 10, "7-10pt"), (10, 999, "10pt+"),
]

for race, finish_map in races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)

    gap1 = (sorted_h[0].get("composite", 0) or 0) - (sorted_h[1].get("composite", 0) or 0)
    top_hno = sorted_h[0].get("horse_no")
    fp = finish_map.get(top_hno, 99)
    if fp <= 0 or fp >= 90: continue

    for lo, hi, bname in FINE_BUCKETS:
        if lo <= gap1 < hi:
            if bname not in size_stats:
                size_stats[bname] = {"n": 0, "win": 0, "p2": 0, "p3": 0}
            size_stats[bname]["n"] += 1
            if fp == 1: size_stats[bname]["win"] += 1
            if fp <= 2: size_stats[bname]["p2"] += 1
            if fp <= 3: size_stats[bname]["p3"] += 1
            break

print(f"\n  {'断層サイズ':<10s} {'レース数':>7s} {'1位勝率':>7s} {'1位連対':>7s} {'1位複勝':>7s}")
print(f"  {'-'*10} {'-'*7} {'-'*7} {'-'*7} {'-'*7}")
for _, _, bname in FINE_BUCKETS:
    s = size_stats.get(bname)
    if not s or s["n"] < 20: continue
    print(f"  {bname:<10s} {s['n']:>7d} {s['win']/s['n']*100:>6.1f}% {s['p2']/s['n']*100:>6.1f}% {s['p3']/s['n']*100:>6.1f}%")

# =================================================================
# 分析5: 断層直下の「落とし穴」— 断層直上の最下位馬の成績
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析5】断層直上の最下位馬 vs 断層直下の最上位馬")
print(f"{'='*70}")
# 断層上位群の最下位（ギリギリ上）と断層下位群の最上位（ギリギリ下）の成績比較

edge_stats = {"above_last": {"n": 0, "p3": 0}, "below_first": {"n": 0, "p3": 0}}
edge_by_gap = {}  # gap_bucket -> same

for race, finish_map in races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)

    # 最初の2.5pt以上の断層を検出
    for i in range(1, min(len(sorted_h), 8)):
        g = (sorted_h[i-1].get("composite", 0) or 0) - (sorted_h[i].get("composite", 0) or 0)
        if g < 2.5: continue

        # 断層直上の最下位馬
        above_last = sorted_h[i-1]
        hno_a = above_last.get("horse_no")
        fp_a = finish_map.get(hno_a, 99)
        if fp_a > 0 and fp_a < 90:
            edge_stats["above_last"]["n"] += 1
            if fp_a <= 3: edge_stats["above_last"]["p3"] += 1

        # 断層直下の最上位馬
        below_first = sorted_h[i]
        hno_b = below_first.get("horse_no")
        fp_b = finish_map.get(hno_b, 99)
        if fp_b > 0 and fp_b < 90:
            edge_stats["below_first"]["n"] += 1
            if fp_b <= 3: edge_stats["below_first"]["p3"] += 1

        # サイズ別
        if g < 5: gb = "2.5-5pt"
        elif g < 7.5: gb = "5-7.5pt"
        else: gb = "7.5pt+"
        if gb not in edge_by_gap:
            edge_by_gap[gb] = {"above_n": 0, "above_p3": 0, "below_n": 0, "below_p3": 0}
        if fp_a > 0 and fp_a < 90:
            edge_by_gap[gb]["above_n"] += 1
            if fp_a <= 3: edge_by_gap[gb]["above_p3"] += 1
        if fp_b > 0 and fp_b < 90:
            edge_by_gap[gb]["below_n"] += 1
            if fp_b <= 3: edge_by_gap[gb]["below_p3"] += 1
        break  # 最初の断層のみ

sa = edge_stats["above_last"]
sb = edge_stats["below_first"]
if sa["n"] > 0 and sb["n"] > 0:
    print(f"\n  断層直上の最下位馬: {sa['n']}頭  複勝率 {sa['p3']/sa['n']*100:.1f}%")
    print(f"  断層直下の最上位馬: {sb['n']}頭  複勝率 {sb['p3']/sb['n']*100:.1f}%")
    print(f"  差: {(sa['p3']/sa['n'] - sb['p3']/sb['n'])*100:+.1f}pt")
    print(f"\n  サイズ別:")
    for gb in ["2.5-5pt", "5-7.5pt", "7.5pt+"]:
        s = edge_by_gap.get(gb)
        if not s: continue
        ap3 = s["above_p3"] / max(1, s["above_n"]) * 100
        bp3 = s["below_p3"] / max(1, s["below_n"]) * 100
        print(f"    {gb:<10s}  上:{s['above_n']:>5d}頭 P3={ap3:>5.1f}%  下:{s['below_n']:>5d}頭 P3={bp3:>5.1f}%  差={ap3-bp3:>+5.1f}")

total_time = time.time() - t0
print(f"\n  処理時間: {total_time:.1f}s")
