#!/usr/bin/env python3
"""
v3改善のシミュレーション — ×危険馬と☆穴馬の新条件を過去データで検証

検証項目:
1. NAR ×: gap5++ML wp<6%+comp下位30% の複勝率
2. ☆: 断層直下ボーナス+ML乖離ペナルティ後のスコアリング効果
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

jra_races, nar_races = [], []
target = [(fp, os.path.basename(fp)[:8]) for fp in sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
          if START <= os.path.basename(fp)[:8] <= END
          and '_prev' not in os.path.basename(fp) and '_backup' not in os.path.basename(fp)]
for i, (fp, dt) in enumerate(target):
    try: d = json.load(open(fp, 'r', encoding='utf-8'))
    except: continue
    for r in d.get("races", []):
        rid = r.get("race_id", "")
        if rid not in all_results: continue
        venue = r.get("venue", "")
        if venue in JRA_VENUES: jra_races.append((r, all_results[rid]))
        else: nar_races.append((r, all_results[rid]))
    if (i+1) % 200 == 0:
        pct = (i+1)/len(target)*100
        print(f"  [{pct:.0f}%] {i+1}/{len(target)} loaded", flush=True)

print(f"  JRA: {len(jra_races)}R  NAR: {len(nar_races)}R  ({time.time()-t0:.1f}s)")


def find_first_gap(sh, min_gap=2.5, max_pos=8):
    for i in range(1, min(len(sh), max_pos)):
        g = (sh[i-1].get("composite",0) or 0) - (sh[i].get("composite",0) or 0)
        if g >= min_gap: return i, g
    return None, 0

def horse_comp_rank(h, sh):
    hno = h.get("horse_no")
    for i, s in enumerate(sh):
        if s.get("horse_no") == hno: return i + 1
    return 99


def sim_kiken_v3_nar(races, label):
    """NAR ×v3条件: pop1-6 + odds<30 + wp<6% + comp下位30% + gap5pt+下"""
    print(f"\n{'='*70}")
    print(f"  × v3 NAR シミュレーション ({label})")
    print(f"{'='*70}")

    # 比較: 現行v2(pred.jsonの×) vs v3(新条件)
    current = {"n": 0, "win": 0, "p3": 0}
    v3 = {"n": 0, "win": 0, "p3": 0}
    v3_detail = {}  # (pop_bucket, gap_bucket) -> stats

    for race, fm in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        n = len(horses)

        # 現行×
        for h in horses:
            if h.get("mark") == "\u00d7":
                hno = h.get("horse_no")
                fp = fm.get(hno, 99)
                if fp <= 0 or fp >= 90: continue
                current["n"] += 1
                if fp == 1: current["win"] += 1
                if fp <= 3: current["p3"] += 1

        # v3条件
        gap_pos, gap_size = find_first_gap(sh, min_gap=5.0)
        comp_threshold = max(3, int(n * 0.70))

        for h in horses:
            pop = h.get("popularity") or 99
            odds = h.get("odds", 0) or 0
            wp = h.get("win_prob", 0) or 0
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue

            # 条件チェック
            if pop < 1 or pop > 6: continue
            if odds >= 30: continue
            # 人気帯でwp閾値を分離（1-3人気は厳格化）
            if pop <= 3:
                if wp >= 0.03: continue
            else:
                if wp >= 0.06: continue

            rank = horse_comp_rank(h, sh)
            if rank < comp_threshold: continue

            # 断層チェック
            if gap_pos is not None:
                if rank <= gap_pos: continue  # 断層上は対象外

            v3["n"] += 1
            if fp == 1: v3["win"] += 1
            if fp <= 3: v3["p3"] += 1

            # 詳細
            pb = f"{pop}人気" if pop <= 3 else "4-6人気"
            gb = "gap5+" if (gap_pos and gap_size >= 5) else "gap<5/なし"
            key = (pb, gb)
            if key not in v3_detail: v3_detail[key] = {"n": 0, "win": 0, "p3": 0}
            v3_detail[key]["n"] += 1
            if fp == 1: v3_detail[key]["win"] += 1
            if fp <= 3: v3_detail[key]["p3"] += 1

    print(f"\n  現行× ({label}):")
    if current["n"]:
        print(f"    {current['n']}頭  勝率{current['win']/current['n']*100:.1f}%  複勝率{current['p3']/current['n']*100:.1f}%")
    print(f"\n  v3× ({label}):")
    if v3["n"]:
        print(f"    {v3['n']}頭  勝率{v3['win']/v3['n']*100:.1f}%  複勝率{v3['p3']/v3['n']*100:.1f}%")

    print(f"\n  v3 詳細:")
    print(f"    {'人気':>8s} {'断層':>12s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
    for key in sorted(v3_detail.keys()):
        s = v3_detail[key]
        if s["n"] < 5: continue
        print(f"    {key[0]:>8s} {key[1]:>12s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")


def sim_ana_v3(races, label):
    """☆v3: 断層直下ボーナス+ML乖離ペナルティの効果"""
    print(f"\n{'='*70}")
    print(f"  ☆ v3 シミュレーション ({label})")
    print(f"{'='*70}")

    # 現行☆ vs v3スコア調整後の比較
    current = {"n": 0, "win": 0, "p3": 0}
    bonus = {"n": 0, "win": 0, "p3": 0}  # 断層直下ボーナス対象
    penalty = {"n": 0, "win": 0, "p3": 0}  # ML>>オッズペナルティ対象
    neutral = {"n": 0, "win": 0, "p3": 0}  # どちらでもない

    for race, fm in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)

        for h in horses:
            if h.get("mark") != "\u2606": continue
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue

            current["n"] += 1
            if fp == 1: current["win"] += 1
            if fp <= 3: current["p3"] += 1

            wp = h.get("win_prob", 0) or 0
            odds = h.get("odds", 0) or 0

            # 断層直下チェック
            is_bonus = False
            if gap_pos is not None:
                rank = horse_comp_rank(h, sh)
                if rank > gap_pos and rank <= gap_pos + 2:
                    is_bonus = True

            # ML vs オッズ乖離
            is_penalty = False
            if odds > 0 and wp > 0:
                odds_wp = 1.0 / odds * 0.8
                if odds_wp > 0 and wp / odds_wp >= 2.0:
                    is_penalty = True

            if is_bonus and not is_penalty:
                bucket = bonus
            elif is_penalty and not is_bonus:
                bucket = penalty
            else:
                bucket = neutral

            bucket["n"] += 1
            if fp == 1: bucket["win"] += 1
            if fp <= 3: bucket["p3"] += 1

    print(f"\n  現行☆全体: {current['n']}頭  勝率{current['win']/current['n']*100:.1f}%  複勝率{current['p3']/current['n']*100:.1f}%")
    if bonus["n"]:
        print(f"  断層直下ボーナス: {bonus['n']}頭  勝率{bonus['win']/bonus['n']*100:.1f}%  複勝率{bonus['p3']/bonus['n']*100:.1f}%")
    if penalty["n"]:
        print(f"  ML>>オッズペナルティ: {penalty['n']}頭  勝率{penalty['win']/penalty['n']*100:.1f}%  複勝率{penalty['p3']/penalty['n']*100:.1f}%")
    if neutral["n"]:
        print(f"  その他: {neutral['n']}頭  勝率{neutral['win']/neutral['n']*100:.1f}%  複勝率{neutral['p3']/neutral['n']*100:.1f}%")


# === 実行 ===
sim_kiken_v3_nar(nar_races, "NAR")
sim_kiken_v3_nar(jra_races, "JRA")  # JRAも参考に
sim_ana_v3(jra_races, "JRA")
sim_ana_v3(nar_races, "NAR")

print(f"\n  処理時間: {time.time()-t0:.1f}s")
