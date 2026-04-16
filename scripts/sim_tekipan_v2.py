#!/usr/bin/env python3
"""
◉（的パン）閾値シミュレーション v2 — 正しいgap計算（1位-2位差）

gap = composite1位 - composite2位 （formatter.pyと同一ロジック）
"""
import json, glob, os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
START, END = "20240101", "20260413"
JRA_VENUES = {'東京','中山','阪神','京都','中京','小倉','新潟','福島','函館','札幌'}

t0 = time.time()

# === データ読込 ===
all_results = {}
all_payouts = {}
for fp in sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json"))):
    dt = os.path.basename(fp)[:8]
    if not (START <= dt <= END): continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for rid, data in d.items():
            all_results[rid] = {e["horse_no"]: e.get("finish", 99) for e in data.get("order", []) if "horse_no" in e}
            pay = data.get('payouts', {})
            if pay: all_payouts[rid] = pay
    except: pass

def get_win_pay(pay, hno):
    w = pay.get('単勝', None)
    if w is None: return 0
    hs = str(hno)
    if isinstance(w, dict):
        return (w.get('payout', 0) or 0) if str(w.get('combo', '')) == hs else 0
    if isinstance(w, list):
        for x in w:
            if isinstance(x, dict) and str(x.get('combo', '')) == hs:
                return x.get('payout', 0) or 0
    return 0

# レースデータ読込
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
        fm = all_results[rid]
        pay = all_payouts.get(rid, {})
        if venue in JRA_VENUES:
            jra_races.append((r, fm, pay))
        else:
            nar_races.append((r, fm, pay))
    if (i+1) % 200 == 0:
        pct = (i+1)/len(target)*100
        print(f"  [{pct:.0f}%] {i+1}/{len(target)} loaded", flush=True)

print(f"  JRA: {len(jra_races)}R  NAR: {len(nar_races)}R  ({time.time()-t0:.1f}s)")


def sim_tekipan(races, gap_th, wp_th, p3p_th, ev_th):
    """指定閾値で◉をシミュレーション（formatter.pyと同じgap計算）"""
    stats = {"n_races": 0, "n_tekipan": 0, "win": 0, "p2": 0, "p3": 0, "win_ret": 0}

    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        stats["n_races"] += 1

        # composite降順ソート
        sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
        top = sh[0]
        second = sh[1] if len(sh) >= 2 else None

        # gap = 1位 - 2位 （formatter.pyと同一）
        gap = (top.get("composite", 0) or 0) - (second.get("composite", 0) or 0) if second else 99.0

        if gap < gap_th: continue

        # トップ馬の条件
        wp = top.get("win_prob", 0) or 0
        p3p = top.get("place3_prob", 0) or 0
        odds = top.get("odds", 0) or 0

        if wp < wp_th: continue
        if p3p < p3p_th: continue

        # EV計算
        ev = wp * odds * 0.8 if odds > 0 else 0
        if ev < ev_th: continue

        # ◉認定
        hno = top.get("horse_no")
        fp2 = fm.get(hno, 99)
        if fp2 <= 0 or fp2 >= 90: continue

        stats["n_tekipan"] += 1
        if fp2 == 1:
            stats["win"] += 1
            stats["win_ret"] += get_win_pay(pay, hno)
        if fp2 <= 2: stats["p2"] += 1
        if fp2 <= 3: stats["p3"] += 1

    return stats


# === メインシミュレーション ===
for cat, races in [("JRA", jra_races), ("NAR", nar_races)]:
    print(f"\n{'='*90}")
    print(f"  {cat} ◉シミュレーション（{len(races)}R）")
    print(f"{'='*90}")

    # 現行条件
    if cat == "JRA":
        current = {"gap": 5.0, "wp": 0.30, "p3p": 0.65, "ev": 0.80}
    else:
        current = {"gap": 5.0, "wp": 0.25, "p3p": 0.0, "ev": 0.80}

    # 現行結果
    s = sim_tekipan(races, current["gap"], current["wp"], current["p3p"], current["ev"])
    if s["n_tekipan"] > 0:
        wr = s["win"]/s["n_tekipan"]*100
        p2r = s["p2"]/s["n_tekipan"]*100
        p3r = s["p3"]/s["n_tekipan"]*100
        roi = s["win_ret"]/s["n_tekipan"]
        rate = s["n_tekipan"]/s["n_races"]*100
        print(f"\n  【現行】gap≥{current['gap']:.0f} wp≥{current['wp']:.0%} p3p≥{current['p3p']:.0%} ev≥{current['ev']:.1f}")
        print(f"    {s['n_tekipan']}頭/{s['n_races']}R ({rate:.1f}%)  "
              f"勝率{wr:.1f}% 連対{p2r:.1f}% 複勝{p3r:.1f}% 単回収{roi:.1f}%")

    # グリッドサーチ
    results = []
    gap_range = [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 10.0]
    wp_range = [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]
    p3p_range = [0.0, 0.50, 0.60, 0.65, 0.70, 0.75, 0.80]
    ev_range = [0.80, 0.90, 1.00, 1.10, 1.20]

    for gap in gap_range:
        for wp in wp_range:
            for p3p in p3p_range:
                for ev in ev_range:
                    s = sim_tekipan(races, gap, wp, p3p, ev)
                    if s["n_tekipan"] >= 10:
                        wr = s["win"]/s["n_tekipan"]*100
                        p2r = s["p2"]/s["n_tekipan"]*100
                        p3r = s["p3"]/s["n_tekipan"]*100
                        roi = s["win_ret"]/s["n_tekipan"]
                        rate = s["n_tekipan"]/s["n_races"]*100
                        results.append({
                            "gap": gap, "wp": wp, "p3p": p3p, "ev": ev,
                            "n": s["n_tekipan"], "n_races": s["n_races"],
                            "rate": rate, "wr": wr, "p2r": p2r, "p3r": p3r, "roi": roi
                        })

    # 勝率60%以上フィルタ
    elite = [r for r in results if r["wr"] >= 60.0 and r["n"] >= 20]
    if elite:
        print(f"\n  ■ 勝率60%以上 & 20頭以上 ({len(elite)}件)")
    else:
        elite = [r for r in results if r["wr"] >= 50.0 and r["n"] >= 20]
        if elite:
            print(f"\n  ■ 勝率50%以上 & 20頭以上 ({len(elite)}件)")
        else:
            elite = sorted(results, key=lambda r: r["wr"], reverse=True)[:25]
            print(f"\n  ■ 勝率上位25件:")

    elite.sort(key=lambda r: (-r["wr"], -r["n"]))
    print(f"  {'gap':>4s} {'wp':>5s} {'p3p':>5s} {'ev':>5s} {'頭数':>5s} {'出現率':>6s} {'勝率':>6s} {'連対':>6s} {'複勝':>6s} {'単回収':>7s}")
    print(f"  {'-'*4} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*7}")
    for r in elite[:30]:
        print(f"  {r['gap']:>4.0f} {r['wp']:>4.0f}% {r['p3p']:>4.0f}% {r['ev']:>5.1f} "
              f"{r['n']:>5d} {r['rate']:>5.1f}% {r['wr']:>5.1f}% {r['p2r']:>5.1f}% {r['p3r']:>5.1f}% {r['roi']:>6.1f}%")

    # 推奨条件: 勝率≥55% & 頭数≥50 & 出現率3-10%
    recommended = [r for r in results if r["wr"] >= 50.0 and r["n"] >= 50 and 1.0 <= r["rate"] <= 12.0]
    if recommended:
        recommended.sort(key=lambda r: r["wr"] * (1 + r["n"]/500), reverse=True)
        print(f"\n  ★推奨候補（勝率≥50% & 50頭以上 & 出現率1-12%）上位5件:")
        for best in recommended[:5]:
            print(f"    gap≥{best['gap']:.0f} wp≥{best['wp']:.0%} p3p≥{best['p3p']:.0%} ev≥{best['ev']:.1f}")
            print(f"      {best['n']}頭 ({best['rate']:.1f}%)  "
                  f"勝率{best['wr']:.1f}% 連対{best['p2r']:.1f}% 複勝{best['p3r']:.1f}% 単回収{best['roi']:.1f}%")

    # gap大きさ別分析（1位-2位差）
    print(f"\n  ■ gap大きさ別（1位-2位差）トップ馬成績")
    for gap_min, gap_max, label in [(3.0, 5.0, "3-5"), (5.0, 7.0, "5-7"), (7.0, 10.0, "7-10"), (10.0, 15.0, "10-15"), (15.0, 99.0, "15+")]:
        bucket = {"n": 0, "win": 0, "p2": 0, "p3": 0}
        for race, fm, pay in races:
            horses = race.get("horses", [])
            if len(horses) < 5: continue
            sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
            top = sh[0]
            second = sh[1] if len(sh) >= 2 else None
            gap = (top.get("composite", 0) or 0) - (second.get("composite", 0) or 0) if second else 99.0
            if not (gap_min <= gap < gap_max): continue
            hno = top.get("horse_no")
            fp2 = fm.get(hno, 99)
            if fp2 <= 0 or fp2 >= 90: continue
            bucket["n"] += 1
            if fp2 == 1: bucket["win"] += 1
            if fp2 <= 2: bucket["p2"] += 1
            if fp2 <= 3: bucket["p3"] += 1
        if bucket["n"] >= 10:
            print(f"    gap {label:>5s}: {bucket['n']:>5d}頭  "
                  f"勝率{bucket['win']/bucket['n']*100:>5.1f}%  "
                  f"連対{bucket['p2']/bucket['n']*100:>5.1f}%  "
                  f"複勝{bucket['p3']/bucket['n']*100:>5.1f}%")

    # wp帯別分析
    print(f"\n  ■ wp帯別トップ馬成績 (gap≥5)")
    for wp_min, wp_max, label in [(0.20, 0.25, "20-25%"), (0.25, 0.30, "25-30%"), (0.30, 0.35, "30-35%"),
                                   (0.35, 0.40, "35-40%"), (0.40, 0.50, "40-50%"), (0.50, 1.0, "50%+")]:
        bucket = {"n": 0, "win": 0, "p3": 0}
        for race, fm, pay in races:
            horses = race.get("horses", [])
            if len(horses) < 5: continue
            sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
            top = sh[0]
            second = sh[1] if len(sh) >= 2 else None
            gap = (top.get("composite", 0) or 0) - (second.get("composite", 0) or 0) if second else 99.0
            if gap < 5.0: continue
            wp = top.get("win_prob", 0) or 0
            if not (wp_min <= wp < wp_max): continue
            hno = top.get("horse_no")
            fp2 = fm.get(hno, 99)
            if fp2 <= 0 or fp2 >= 90: continue
            bucket["n"] += 1
            if fp2 == 1: bucket["win"] += 1
            if fp2 <= 3: bucket["p3"] += 1
        if bucket["n"] >= 10:
            print(f"    wp {label:>6s}: {bucket['n']:>5d}頭  "
                  f"勝率{bucket['win']/bucket['n']*100:>5.1f}%  "
                  f"複勝{bucket['p3']/bucket['n']*100:>5.1f}%")

print(f"\n  処理時間: {time.time()-t0:.1f}s")
