#!/usr/bin/env python3
"""
断層 × ☆穴馬/×危険馬 深堀分析（JRA限定）

分析軸:
  1. ☆穴馬の断層位置（上/下）× 成績 — 断層下の☆は来るのか？
  2. ☆穴馬が断層を「突破」する条件（サイズ×特徴量）
  3. 断層上にいる人気馬が凡走するケース（×に使える条件）
  4. 断層下の人気馬（2-3番人気）の成績 — 新×候補
  5. 断層位置 × 各印の成績クロス分析
  6. 無印馬の断層位置別成績 — 馬券に入れるべき無印馬
"""
import json, glob, os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
START, END = "20240101", "20260412"
JRA_VENUES = {'東京','中山','阪神','京都','中京','小倉','新潟','福島','函館','札幌'}

t0 = time.time()

# === データ読込 ===
all_results = {}
for fp in sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json"))):
    dt = os.path.basename(fp)[:8]
    if not (START <= dt <= END): continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for rid, data in d.items():
            all_results[rid] = {e["horse_no"]: e.get("finish", 99) for e in data.get("order", []) if "horse_no" in e}
    except: pass

jra_races = []
target = [(fp, os.path.basename(fp)[:8]) for fp in sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
          if START <= os.path.basename(fp)[:8] <= END
          and '_prev' not in os.path.basename(fp) and '_backup' not in os.path.basename(fp)]
for i, (fp, dt) in enumerate(target):
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
    except: continue
    for r in d.get("races", []):
        rid = r.get("race_id", "")
        if rid in all_results and r.get("venue", "") in JRA_VENUES:
            jra_races.append((r, all_results[rid]))
    if (i+1) % 200 == 0:
        print(f"  [{(i+1)/len(target)*100:.0f}%] {i+1}/{len(target)} loaded", flush=True)
print(f"  JRA: {len(jra_races)}R ({time.time()-t0:.1f}s)")


def find_first_gap(sorted_h, min_gap=2.5, max_pos=8):
    """最初の断層の位置とサイズを返す"""
    for i in range(1, min(len(sorted_h), max_pos)):
        g = (sorted_h[i-1].get("composite",0) or 0) - (sorted_h[i].get("composite",0) or 0)
        if g >= min_gap:
            return i, g  # position, size
    return None, 0

def find_max_gap(sorted_h, max_pos=8):
    """最大断層の位置とサイズを返す"""
    mg, mp = 0, 0
    for i in range(1, min(len(sorted_h), max_pos)):
        g = (sorted_h[i-1].get("composite",0) or 0) - (sorted_h[i].get("composite",0) or 0)
        if g > mg: mg, mp = g, i
    return mp, mg

def horse_comp_rank(h, sorted_h):
    """composite順位を返す"""
    hno = h.get("horse_no")
    for i, s in enumerate(sorted_h):
        if s.get("horse_no") == hno:
            return i + 1
    return 99

def gap_bucket(g):
    if g < 2.5: return "gap<2.5"
    if g < 5: return "2.5-5pt"
    if g < 7.5: return "5-7.5pt"
    return "7.5pt+"


# =================================================================
# 分析1: ☆穴馬の断層位置 × 成績
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析1】☆穴馬 × 断層の上/下 × 成績")
print(f"{'='*70}")

ana_pos = {}  # (above/below, gap_bucket) -> {n, win, p3}

for race, fm in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
    gap_pos, gap_size = find_first_gap(sh)
    if gap_pos is None: continue

    for h in horses:
        if h.get("mark") != "\u2606": continue  # ☆
        hno = h.get("horse_no")
        fp = fm.get(hno, 99)
        if fp <= 0 or fp >= 90: continue

        rank = horse_comp_rank(h, sh)
        pos_type = "断層上" if rank <= gap_pos else "断層下"
        gb = gap_bucket(gap_size)

        key = (pos_type, gb)
        if key not in ana_pos:
            ana_pos[key] = {"n": 0, "win": 0, "p3": 0}
        ana_pos[key]["n"] += 1
        if fp == 1: ana_pos[key]["win"] += 1
        if fp <= 3: ana_pos[key]["p3"] += 1

print(f"  {'位置':>6s} {'断層':>8s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
print(f"  {'-'*6} {'-'*8} {'-'*5} {'-'*6} {'-'*6}")
for pos in ["断層上", "断層下"]:
    for gb in ["2.5-5pt", "5-7.5pt", "7.5pt+"]:
        s = ana_pos.get((pos, gb))
        if not s or s["n"] < 20: continue
        print(f"  {pos:>6s} {gb:>8s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")

# 断層なしのレースでの☆
ana_nogap = {"n": 0, "win": 0, "p3": 0}
for race, fm in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
    gap_pos, _ = find_first_gap(sh)
    if gap_pos is not None: continue
    for h in horses:
        if h.get("mark") != "\u2606": continue
        fp = fm.get(h.get("horse_no"), 99)
        if fp <= 0 or fp >= 90: continue
        ana_nogap["n"] += 1
        if fp == 1: ana_nogap["win"] += 1
        if fp <= 3: ana_nogap["p3"] += 1
if ana_nogap["n"] > 0:
    print(f"  {'断層なし':>6s} {'---':>8s} {ana_nogap['n']:>5d} {ana_nogap['win']/ana_nogap['n']*100:>5.1f}% {ana_nogap['p3']/ana_nogap['n']*100:>5.1f}%")


# =================================================================
# 分析2: ☆穴馬が3着以内に来るときの条件（断層突破分析）
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析2】☆穴馬が断層を突破して3着以内に来る条件")
print(f"{'='*70}")

# 断層下の☆が3着以内に来る vs 来ないの特徴比較
ana_break = {"hit": [], "miss": []}

for race, fm in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
    gap_pos, gap_size = find_first_gap(sh)
    if gap_pos is None: continue

    for h in horses:
        if h.get("mark") != "\u2606": continue
        hno = h.get("horse_no")
        fp = fm.get(hno, 99)
        if fp <= 0 or fp >= 90: continue
        rank = horse_comp_rank(h, sh)
        if rank <= gap_pos: continue  # 断層上は対象外

        record = {
            "gap_size": gap_size,
            "comp_rank": rank,
            "wp": h.get("win_prob", 0) or 0,
            "odds": h.get("odds", 0) or 0,
            "pop": h.get("popularity", 0) or 0,
            "ana_score": h.get("ana_score", 0) or h.get("tokusen_score", 0) or 0,
            "field": len(horses),
        }
        if fp <= 3:
            ana_break["hit"].append(record)
        else:
            ana_break["miss"].append(record)

if ana_break["hit"] and ana_break["miss"]:
    print(f"\n  断層下☆の的中: {len(ana_break['hit'])}頭  vs  外れ: {len(ana_break['miss'])}頭")
    for label, data in [("3着以内", ana_break["hit"]), ("4着以下", ana_break["miss"])]:
        avg_gap = sum(r["gap_size"] for r in data) / len(data)
        avg_rank = sum(r["comp_rank"] for r in data) / len(data)
        avg_wp = sum(r["wp"] for r in data) / len(data)
        avg_odds = sum(r["odds"] for r in data) / len(data)
        avg_pop = sum(r["pop"] for r in data) / len(data)
        print(f"\n  {label} ({len(data)}頭):")
        print(f"    平均断層サイズ: {avg_gap:.2f}pt")
        print(f"    平均comp順位:  {avg_rank:.1f}位")
        print(f"    平均win_prob:  {avg_wp*100:.2f}%")
        print(f"    平均オッズ:     {avg_odds:.1f}倍")
        print(f"    平均人気:       {avg_pop:.1f}番人気")

    # 断層サイズ別の突破率
    print(f"\n  断層サイズ別の☆突破率:")
    for lo, hi, gb in [(2.5,4,"2.5-4pt"),(4,6,"4-6pt"),(6,9,"6-9pt"),(9,999,"9pt+")]:
        hit_n = sum(1 for r in ana_break["hit"] if lo <= r["gap_size"] < hi)
        miss_n = sum(1 for r in ana_break["miss"] if lo <= r["gap_size"] < hi)
        total = hit_n + miss_n
        if total >= 10:
            print(f"    {gb:<10s} {total:>4d}頭  突破率 {hit_n/total*100:.1f}%")


# =================================================================
# 分析3: 断層上の人気馬が凡走するケース（×改善の種）
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析3】断層上の2-3番人気が凡走するケース")
print(f"{'='*70}")

# 断層上にいる2-3番人気馬の成績（断層があるのに来ない人気馬）
pop_above = {}  # (pop, gap_bucket) -> {n, win, p3}
pop_below = {}

for race, fm in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
    gap_pos, gap_size = find_first_gap(sh)
    if gap_pos is None: continue

    for h in horses:
        pop = h.get("popularity")
        if pop not in (2, 3): continue
        hno = h.get("horse_no")
        fp = fm.get(hno, 99)
        if fp <= 0 or fp >= 90: continue

        rank = horse_comp_rank(h, sh)
        gb = gap_bucket(gap_size)
        is_above = rank <= gap_pos

        target = pop_above if is_above else pop_below
        key = (pop, gb)
        if key not in target:
            target[key] = {"n": 0, "win": 0, "p3": 0}
        target[key]["n"] += 1
        if fp == 1: target[key]["win"] += 1
        if fp <= 3: target[key]["p3"] += 1

print(f"\n  断層上にいる人気馬の成績（上位群に含まれている）:")
print(f"  {'人気':>4s} {'断層':>8s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
print(f"  {'-'*4} {'-'*8} {'-'*5} {'-'*6} {'-'*6}")
for pop in [2, 3]:
    for gb in ["2.5-5pt", "5-7.5pt", "7.5pt+"]:
        s = pop_above.get((pop, gb))
        if not s or s["n"] < 20: continue
        print(f"  {pop}人気 {gb:>8s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")

print(f"\n  断層下にいる人気馬の成績（下位群に落ちている）:")
print(f"  {'人気':>4s} {'断層':>8s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
print(f"  {'-'*4} {'-'*8} {'-'*5} {'-'*6} {'-'*6}")
for pop in [2, 3]:
    for gb in ["2.5-5pt", "5-7.5pt", "7.5pt+"]:
        s = pop_below.get((pop, gb))
        if not s or s["n"] < 20: continue
        print(f"  {pop}人気 {gb:>8s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")


# =================================================================
# 分析4: 全印 × 断層上下 × 成績（一覧）
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析4】全印 × 断層上/下 × 成績")
print(f"{'='*70}")

mark_pos = {}  # (mark, above/below) -> {n, win, p3}

for race, fm in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
    gap_pos, gap_size = find_first_gap(sh)
    if gap_pos is None: continue

    for h in horses:
        mk = h.get("mark", "")
        if not mk or mk == "\uff0d": continue
        hno = h.get("horse_no")
        fp = fm.get(hno, 99)
        if fp <= 0 or fp >= 90: continue

        rank = horse_comp_rank(h, sh)
        pos_type = "上" if rank <= gap_pos else "下"

        key = (mk, pos_type)
        if key not in mark_pos:
            mark_pos[key] = {"n": 0, "win": 0, "p3": 0}
        mark_pos[key]["n"] += 1
        if fp == 1: mark_pos[key]["win"] += 1
        if fp <= 3: mark_pos[key]["p3"] += 1

print(f"  {'印':>2s} {'位置':>4s} {'頭数':>6s} {'勝率':>6s} {'複勝率':>6s}  {'印':>2s} {'位置':>4s} {'頭数':>6s} {'勝率':>6s} {'複勝率':>6s}")
print(f"  {'-'*2} {'-'*4} {'-'*6} {'-'*6} {'-'*6}  {'-'*2} {'-'*4} {'-'*6} {'-'*6} {'-'*6}")
for mk in ["\u25c9","\u25ce","\u25cb","\u25b2","\u25b3","\u2605","\u2606","\u00d7"]:
    mk_display = mk
    sa = mark_pos.get((mk, "上"))
    sb = mark_pos.get((mk, "下"))
    line_a = f"  {mk_display:>2s} {'上':>4s}"
    line_b = f"  {mk_display:>2s} {'下':>4s}"
    if sa and sa["n"] >= 10:
        line_a += f" {sa['n']:>6d} {sa['win']/sa['n']*100:>5.1f}% {sa['p3']/sa['n']*100:>5.1f}%"
    else:
        line_a += f" {'---':>6s} {'---':>6s} {'---':>6s}"
    if sb and sb["n"] >= 10:
        line_b += f" {sb['n']:>6d} {sb['win']/sb['n']*100:>5.1f}% {sb['p3']/sb['n']*100:>5.1f}%"
    else:
        line_b += f" {'---':>6s} {'---':>6s} {'---':>6s}"
    print(line_a)
    print(line_b)


# =================================================================
# 分析5: 無印馬の断層位置別成績
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析5】無印馬 × 断層上/下 × 人気 × 成績")
print(f"{'='*70}")

nomark = {}  # (above/below, pop_bucket) -> {n, win, p3}

for race, fm in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
    gap_pos, gap_size = find_first_gap(sh)
    if gap_pos is None: continue

    for h in horses:
        mk = h.get("mark", "")
        if mk and mk != "\uff0d": continue  # 無印のみ
        hno = h.get("horse_no")
        fp = fm.get(hno, 99)
        if fp <= 0 or fp >= 90: continue
        pop = h.get("popularity", 99)

        rank = horse_comp_rank(h, sh)
        pos_type = "断層上" if rank <= gap_pos else "断層下"

        if pop <= 3: pb = "1-3人気"
        elif pop <= 6: pb = "4-6人気"
        elif pop <= 9: pb = "7-9人気"
        else: pb = "10+人気"

        key = (pos_type, pb)
        if key not in nomark:
            nomark[key] = {"n": 0, "win": 0, "p3": 0}
        nomark[key]["n"] += 1
        if fp == 1: nomark[key]["win"] += 1
        if fp <= 3: nomark[key]["p3"] += 1

print(f"  {'位置':>6s} {'人気帯':>8s} {'頭数':>6s} {'勝率':>6s} {'複勝率':>6s}")
print(f"  {'-'*6} {'-'*8} {'-'*6} {'-'*6} {'-'*6}")
for pos in ["断層上", "断層下"]:
    for pb in ["1-3人気", "4-6人気", "7-9人気", "10+人気"]:
        s = nomark.get((pos, pb))
        if not s or s["n"] < 20: continue
        print(f"  {pos:>6s} {pb:>8s} {s['n']:>6d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")


# =================================================================
# 分析6: 断層下の人気馬 × ML評価 × 成績（×精度向上）
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析6】断層下の2-3番人気 × ML評価 × 成績（×候補精査）")
print(f"{'='*70}")

# 断層下にいる2-3番人気で、さらにMLのwin_probが低い馬
kiken_deep = {}  # (gap_bucket, wp_bucket) -> {n, p3}

for race, fm in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
    gap_pos, gap_size = find_first_gap(sh)
    if gap_pos is None: continue

    for h in horses:
        pop = h.get("popularity")
        if pop not in (2, 3): continue
        rank = horse_comp_rank(h, sh)
        if rank <= gap_pos: continue  # 断層上は対象外

        hno = h.get("horse_no")
        fp = fm.get(hno, 99)
        if fp <= 0 or fp >= 90: continue

        wp = h.get("win_prob", 0) or 0
        gb = gap_bucket(gap_size)
        if wp < 0.03: wpb = "wp<3%"
        elif wp < 0.06: wpb = "wp3-6%"
        elif wp < 0.10: wpb = "wp6-10%"
        else: wpb = "wp10%+"

        key = (gb, wpb)
        if key not in kiken_deep:
            kiken_deep[key] = {"n": 0, "p3": 0, "win": 0}
        kiken_deep[key]["n"] += 1
        if fp <= 3: kiken_deep[key]["p3"] += 1
        if fp == 1: kiken_deep[key]["win"] += 1

print(f"  断層下 2-3番人気（composite下位群）:")
print(f"  {'断層':>8s} {'ML wp':>8s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}  {'判定':>4s}")
print(f"  {'-'*8} {'-'*8} {'-'*5} {'-'*6} {'-'*6}  {'-'*4}")
for gb in ["2.5-5pt", "5-7.5pt", "7.5pt+"]:
    for wpb in ["wp<3%", "wp3-6%", "wp6-10%", "wp10%+"]:
        s = kiken_deep.get((gb, wpb))
        if not s or s["n"] < 10: continue
        p3r = s["p3"] / s["n"] * 100
        wr = s["win"] / s["n"] * 100
        ok = "◎" if p3r < 15 else "○" if p3r < 25 else "△" if p3r < 35 else "×"
        print(f"  {gb:>8s} {wpb:>8s} {s['n']:>5d} {wr:>5.1f}% {p3r:>5.1f}%  {ok:>4s}")

print(f"\n  処理時間: {time.time()-t0:.1f}s")
