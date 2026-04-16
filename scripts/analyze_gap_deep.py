#!/usr/bin/env python3
"""
断層 深掘り分析 A～E（JRA/NAR分離版）

A. ☆穴馬の「来る条件」精査（ML×オッズ乖離×断層位置×レースタイプ）
B. ×危険馬の3条件AND精査（特にNAR改善）
C. 断層 × 回収率分析
D. 無印馬の断層上位群分析
E. 複数断層・断層位置の質的分析
"""
import json, glob, os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
START, END = "20240101", "20260412"
JRA_VENUES = {'東京','中山','阪神','京都','中京','小倉','新潟','福島','函館','札幌'}

t0 = time.time()

# === データ読込 ===
print(f"  データ読込開始...", flush=True)
all_results = {}
all_payouts = {}  # race_id -> {win: odds, place: {horse_no: odds}}
for fp in sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json"))):
    dt = os.path.basename(fp)[:8]
    if not (START <= dt <= END): continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for rid, data in d.items():
            all_results[rid] = {e["horse_no"]: e.get("finish", 99) for e in data.get("order", []) if "horse_no" in e}
            # 払戻データ
            payouts = data.get("payouts", {})
            if payouts:
                all_payouts[rid] = payouts
    except: pass

jra_races = []
nar_races = []
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
        entry = (r, all_results[rid], all_payouts.get(rid, {}))
        if venue in JRA_VENUES:
            jra_races.append(entry)
        else:
            nar_races.append(entry)
    if (i+1) % 200 == 0:
        elapsed = time.time() - t0
        pct = (i+1) / len(target) * 100
        eta = elapsed / (i+1) * (len(target) - i - 1)
        print(f"  [{pct:.0f}%] {i+1}/{len(target)} loaded  経過{elapsed:.0f}s  残{eta:.0f}s", flush=True)

print(f"  JRA: {len(jra_races)}R  NAR: {len(nar_races)}R  ({time.time()-t0:.1f}s)")


# === ユーティリティ ===
def find_first_gap(sh, min_gap=2.5, max_pos=8):
    for i in range(1, min(len(sh), max_pos)):
        g = (sh[i-1].get("composite",0) or 0) - (sh[i].get("composite",0) or 0)
        if g >= min_gap:
            return i, g
    return None, 0

def find_all_gaps(sh, min_gap=2.5, max_pos=8):
    """全断層を返す [(position, size), ...]"""
    gaps = []
    for i in range(1, min(len(sh), max_pos)):
        g = (sh[i-1].get("composite",0) or 0) - (sh[i].get("composite",0) or 0)
        if g >= min_gap:
            gaps.append((i, g))
    return gaps

def horse_comp_rank(h, sh):
    hno = h.get("horse_no")
    for i, s in enumerate(sh):
        if s.get("horse_no") == hno:
            return i + 1
    return 99

def gap_bucket(g):
    if g < 2.5: return "gap<2.5"
    if g < 5: return "2.5-5pt"
    if g < 7.5: return "5-7.5pt"
    return "7.5pt+"

def race_type(sh):
    """レースタイプ判定"""
    gaps = [(sh[i-1].get("composite",0) or 0) - (sh[i].get("composite",0) or 0) for i in range(1, min(len(sh), 6))]
    if not gaps: return "不明"
    g1, g2, g3 = (gaps + [0,0,0])[:3]
    mg = max(gaps[:5]) if gaps else 0
    if g1 >= 5:       return "独走型"
    elif g1 < 2 and g2 >= 4: return "2強型"
    elif g1 < 2 and g2 < 2 and g3 >= 3: return "3強型"
    elif mg < 2.5:    return "全混戦"
    else:             return "上位拮抗"

def get_win_payout(payouts, horse_no):
    """単勝払戻を取得（100円あたり）"""
    win = payouts.get("単勝", payouts.get("win", None))
    if win is None: return 0
    hno_str = str(horse_no)
    # 辞書型（単一）の場合
    if isinstance(win, dict):
        if str(win.get("combo", "")) == hno_str:
            return win.get("payout", 0) or 0
        return 0
    # リスト型の場合
    if isinstance(win, list):
        for w in win:
            if isinstance(w, dict) and str(w.get("combo", "")) == hno_str:
                return w.get("payout", 0) or 0
    return 0

def get_place_payout(payouts, horse_no):
    """複勝払戻を取得（100円あたり）"""
    place = payouts.get("複勝", payouts.get("place", None))
    if place is None: return 0
    hno_str = str(horse_no)
    # 辞書型（単一）の場合
    if isinstance(place, dict):
        if str(place.get("combo", "")) == hno_str:
            return place.get("payout", 0) or 0
        return 0
    # リスト型の場合
    if isinstance(place, list):
        for p in place:
            if isinstance(p, dict) and str(p.get("combo", "")) == hno_str:
                return p.get("payout", 0) or 0
    return 0

def print_table(headers, rows, min_n=20):
    """汎用テーブル出力"""
    fmt = "    " + "  ".join(f"{{{i}}}" for i in range(len(headers)))
    print("    " + "  ".join(headers))
    print("    " + "  ".join("-" * len(h) for h in headers))
    for row in rows:
        if len(row) > 0:
            print("    " + "  ".join(str(c) for c in row))


def analyze_all(races, label):
    print(f"\n{'#'*74}")
    print(f"  ■ {label}  ({len(races):,}R)")
    print(f"{'#'*74}")

    # =====================================================================
    # A. ☆穴馬の「来る条件」精査
    # =====================================================================
    print(f"\n{'='*70}")
    print(f"  【A】☆穴馬の「来る条件」精査")
    print(f"{'='*70}")

    # A-1: ☆ × 断層直下 vs 遠い位置
    print(f"\n  [A-1] ☆のcomposite順位（断層からの距離）× 成績")
    dist_stats = {}  # (distance_type) -> {n, win, p3}
    for race, fm, pay in races:
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
            if rank <= gap_pos:
                dt = "断層上"
            elif rank <= gap_pos + 2:
                dt = "断層直下(+1~2)"
            elif rank <= gap_pos + 4:
                dt = "断層やや下(+3~4)"
            else:
                dt = "断層遠い(+5~)"
            if dt not in dist_stats: dist_stats[dt] = {"n": 0, "win": 0, "p3": 0}
            dist_stats[dt]["n"] += 1
            if fp == 1: dist_stats[dt]["win"] += 1
            if fp <= 3: dist_stats[dt]["p3"] += 1

    print(f"    {'位置':<18s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
    print(f"    {'-'*18} {'-'*5} {'-'*6} {'-'*6}")
    for dt in ["断層上", "断層直下(+1~2)", "断層やや下(+3~4)", "断層遠い(+5~)"]:
        s = dist_stats.get(dt)
        if not s or s["n"] < 10: continue
        print(f"    {dt:<18s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")

    # A-2: ☆ × オッズ/ML乖離度
    print(f"\n  [A-2] ☆ × オッズとML win_probの乖離 × 成績")
    div_stats = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        for h in horses:
            if h.get("mark") != "\u2606": continue
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue
            wp = h.get("win_prob", 0) or 0
            odds = h.get("odds", 0) or 0
            if odds <= 0 or wp <= 0: continue
            # ML期待勝率 vs オッズ示唆勝率(1/odds)
            odds_wp = 1.0 / odds * 0.8  # 控除率考慮
            ratio = wp / odds_wp if odds_wp > 0 else 0
            # ratio > 1 = MLがオッズより高評価（過小評価馬）
            if ratio >= 2.0: bucket = "ML>>オッズ(2x+)"
            elif ratio >= 1.5: bucket = "ML>オッズ(1.5-2x)"
            elif ratio >= 1.0: bucket = "ML≒オッズ(1-1.5x)"
            elif ratio >= 0.5: bucket = "ML<オッズ(0.5-1x)"
            else: bucket = "ML<<オッズ(<0.5x)"

            if bucket not in div_stats: div_stats[bucket] = {"n": 0, "win": 0, "p3": 0}
            div_stats[bucket]["n"] += 1
            if fp == 1: div_stats[bucket]["win"] += 1
            if fp <= 3: div_stats[bucket]["p3"] += 1

    print(f"    {'乖離':<22s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
    print(f"    {'-'*22} {'-'*5} {'-'*6} {'-'*6}")
    for b in ["ML>>オッズ(2x+)", "ML>オッズ(1.5-2x)", "ML≒オッズ(1-1.5x)", "ML<オッズ(0.5-1x)", "ML<<オッズ(<0.5x)"]:
        s = div_stats.get(b)
        if not s or s["n"] < 10: continue
        print(f"    {b:<22s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")

    # A-3: ☆ × レースタイプ
    print(f"\n  [A-3] ☆ × レースタイプ × 成績")
    rt_stats = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        rt = race_type(sh)
        for h in horses:
            if h.get("mark") != "\u2606": continue
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue
            if rt not in rt_stats: rt_stats[rt] = {"n": 0, "win": 0, "p3": 0}
            rt_stats[rt]["n"] += 1
            if fp == 1: rt_stats[rt]["win"] += 1
            if fp <= 3: rt_stats[rt]["p3"] += 1

    print(f"    {'タイプ':<10s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
    print(f"    {'-'*10} {'-'*5} {'-'*6} {'-'*6}")
    for rt in ["独走型", "2強型", "3強型", "上位拮抗", "全混戦"]:
        s = rt_stats.get(rt)
        if not s or s["n"] < 10: continue
        print(f"    {rt:<10s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")

    # A-4: ☆ × 複合条件（断層直下 + ML>オッズ + 混戦型）
    print(f"\n  [A-4] ☆の最適条件組み合わせ")
    combo = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)
        rt = race_type(sh)
        for h in horses:
            if h.get("mark") != "\u2606": continue
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue
            wp = h.get("win_prob", 0) or 0
            odds = h.get("odds", 0) or 0
            rank = horse_comp_rank(h, sh)

            conditions = []
            # 条件1: 断層直下
            if gap_pos and rank > gap_pos and rank <= gap_pos + 2:
                conditions.append("直下")
            # 条件2: ML > オッズ
            if odds > 0 and wp > 0:
                odds_wp = 1.0 / odds * 0.8
                if wp / odds_wp >= 1.5:
                    conditions.append("ML高")
            # 条件3: 混戦 or 上位拮抗
            if rt in ("全混戦", "上位拮抗"):
                conditions.append("混戦")
            # 条件4: 人気7以内
            pop = h.get("popularity") or 99
            if pop <= 7:
                conditions.append("人気7内")

            # 各条件数での成績
            nc = len(conditions)
            key = f"{nc}条件"
            if key not in combo: combo[key] = {"n": 0, "win": 0, "p3": 0}
            combo[key]["n"] += 1
            if fp == 1: combo[key]["win"] += 1
            if fp <= 3: combo[key]["p3"] += 1

            # 特定の強力な組み合わせ
            cset = set(conditions)
            for combo_name, required in [
                ("直下+ML高", {"直下","ML高"}),
                ("直下+混戦", {"直下","混戦"}),
                ("ML高+混戦", {"ML高","混戦"}),
                ("直下+ML高+混戦", {"直下","ML高","混戦"}),
                ("ML高+人気7内", {"ML高","人気7内"}),
                ("直下+ML高+人気7内", {"直下","ML高","人気7内"}),
            ]:
                if required.issubset(cset):
                    if combo_name not in combo: combo[combo_name] = {"n": 0, "win": 0, "p3": 0}
                    combo[combo_name]["n"] += 1
                    if fp == 1: combo[combo_name]["win"] += 1
                    if fp <= 3: combo[combo_name]["p3"] += 1

    print(f"    {'条件':<22s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
    print(f"    {'-'*22} {'-'*5} {'-'*6} {'-'*6}")
    for key in ["0条件","1条件","2条件","3条件","4条件",
                "直下+ML高","直下+混戦","ML高+混戦","直下+ML高+混戦",
                "ML高+人気7内","直下+ML高+人気7内"]:
        s = combo.get(key)
        if not s or s["n"] < 10: continue
        print(f"    {key:<22s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")

    # =====================================================================
    # B. ×危険馬の3条件AND精査
    # =====================================================================
    print(f"\n{'='*70}")
    print(f"  【B】×危険馬の条件精査")
    print(f"{'='*70}")

    # B-1: 断層下 × ML低 × オッズ人気乖離
    print(f"\n  [B-1] 断層下 × ML wp × 人気 × 成績（×候補の絞り込み）")
    kiken = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)
        if gap_pos is None: continue
        for h in horses:
            pop = h.get("popularity") or 99
            if pop > 6: continue  # 7人気以下はそもそも人気ないので対象外
            rank = horse_comp_rank(h, sh)
            if rank <= gap_pos: continue  # 断層上は対象外
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue
            wp = h.get("win_prob", 0) or 0

            # 人気帯
            if pop <= 3: pb = "1-3人気"
            else: pb = "4-6人気"

            # ML wp帯
            if wp < 0.03: wpb = "wp<3%"
            elif wp < 0.06: wpb = "wp3-6%"
            elif wp < 0.10: wpb = "wp6-10%"
            else: wpb = "wp10%+"

            # 断層サイズ帯
            gb = gap_bucket(gap_size)

            key = (pb, gb, wpb)
            if key not in kiken: kiken[key] = {"n": 0, "win": 0, "p3": 0}
            kiken[key]["n"] += 1
            if fp == 1: kiken[key]["win"] += 1
            if fp <= 3: kiken[key]["p3"] += 1

    print(f"    {'人気':>8s} {'断層':>8s} {'ML wp':>8s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s} {'判定':>4s}")
    print(f"    {'-'*8} {'-'*8} {'-'*8} {'-'*5} {'-'*6} {'-'*6} {'-'*4}")
    for pb in ["1-3人気", "4-6人気"]:
        for gb in ["2.5-5pt", "5-7.5pt", "7.5pt+"]:
            for wpb in ["wp<3%", "wp3-6%", "wp6-10%", "wp10%+"]:
                s = kiken.get((pb, gb, wpb))
                if not s or s["n"] < 15: continue
                p3r = s["p3"] / s["n"] * 100
                wr = s["win"] / s["n"] * 100
                ok = "◎" if p3r < 12 else "○" if p3r < 20 else "△" if p3r < 30 else "×"
                print(f"    {pb:>8s} {gb:>8s} {wpb:>8s} {s['n']:>5d} {wr:>5.1f}% {p3r:>5.1f}% {ok:>4s}")

    # B-2: 前走成績との掛け合わせ
    print(f"\n  [B-2] 断層下の人気馬 × 前走着順 × 成績")
    prev_stats = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)
        if gap_pos is None: continue
        for h in horses:
            pop = h.get("popularity") or 99
            if pop > 6: continue
            rank = horse_comp_rank(h, sh)
            if rank <= gap_pos: continue
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue

            # 前走着順
            prev_runs = h.get("past_runs", [])
            if prev_runs and isinstance(prev_runs, list) and len(prev_runs) > 0:
                pr = prev_runs[0]
                prev_finish = pr.get("finish", pr.get("着順", 99))
            else:
                prev_finish = None

            if prev_finish is None or not isinstance(prev_finish, (int, float)):
                try: prev_finish = int(prev_finish)
                except: prev_finish = None

            if prev_finish is None: pf_cat = "不明"
            elif prev_finish <= 3: pf_cat = "前走3着内"
            elif prev_finish <= 5: pf_cat = "前走4-5着"
            elif prev_finish <= 9: pf_cat = "前走6-9着"
            else: pf_cat = "前走10着+"

            gb = gap_bucket(gap_size)
            key = (pf_cat, gb)
            if key not in prev_stats: prev_stats[key] = {"n": 0, "win": 0, "p3": 0}
            prev_stats[key]["n"] += 1
            if fp == 1: prev_stats[key]["win"] += 1
            if fp <= 3: prev_stats[key]["p3"] += 1

    print(f"    {'前走':>12s} {'断層':>8s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
    print(f"    {'-'*12} {'-'*8} {'-'*5} {'-'*6} {'-'*6}")
    for pf in ["前走3着内", "前走4-5着", "前走6-9着", "前走10着+"]:
        for gb in ["2.5-5pt", "5-7.5pt", "7.5pt+"]:
            s = prev_stats.get((pf, gb))
            if not s or s["n"] < 15: continue
            print(f"    {pf:>12s} {gb:>8s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")

    # B-3: ×のベスト条件（複合AND）
    print(f"\n  [B-3] ×候補のベスト条件（複合AND）")
    kiken_combo = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)
        if gap_pos is None: continue
        for h in horses:
            pop = h.get("popularity") or 99
            if pop > 6: continue
            rank = horse_comp_rank(h, sh)
            if rank <= gap_pos: continue
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue
            wp = h.get("win_prob", 0) or 0

            conds = []
            # 断層5pt+
            if gap_size >= 5: conds.append("gap5+")
            # ML wp < 6%
            if wp < 0.06: conds.append("ML低")
            # composite下位30%
            comp_pct = rank / len(sh)
            if comp_pct >= 0.70: conds.append("comp下位")
            # 前走凡走
            prev_runs = h.get("past_runs", [])
            if prev_runs and isinstance(prev_runs, list) and len(prev_runs) > 0:
                pr = prev_runs[0]
                pf = pr.get("finish", pr.get("着順", 99))
                try: pf = int(pf)
                except: pf = 99
                if pf >= 6: conds.append("前走凡走")
            # オッズ乖離（人気よりML低い）
            odds = h.get("odds", 0) or 0
            if odds > 0 and wp > 0:
                odds_wp = 1.0 / odds * 0.8
                if wp < odds_wp * 0.7:
                    conds.append("ML<オッズ")

            nc = len(conds)
            for n in range(nc + 1):
                key = f"{n}条件以上"
                if key not in kiken_combo: kiken_combo[key] = {"n": 0, "win": 0, "p3": 0}
                kiken_combo[key]["n"] += 1
                if fp == 1: kiken_combo[key]["win"] += 1
                if fp <= 3: kiken_combo[key]["p3"] += 1

            # 具体的な組み合わせ
            cset = set(conds)
            for cn, req in [
                ("gap5++ML低", {"gap5+","ML低"}),
                ("gap5++comp下位", {"gap5+","comp下位"}),
                ("ML低+前走凡走", {"ML低","前走凡走"}),
                ("gap5++ML低+前走凡走", {"gap5+","ML低","前走凡走"}),
                ("gap5++ML低+comp下位", {"gap5+","ML低","comp下位"}),
                ("ML低+comp下位+前走凡走", {"ML低","comp下位","前走凡走"}),
                ("gap5++ML低+comp下位+前走凡走", {"gap5+","ML低","comp下位","前走凡走"}),
                ("ML<オッズ+gap5+", {"ML<オッズ","gap5+"}),
                ("ML<オッズ+ML低", {"ML<オッズ","ML低"}),
            ]:
                if req.issubset(cset):
                    if cn not in kiken_combo: kiken_combo[cn] = {"n": 0, "win": 0, "p3": 0}
                    kiken_combo[cn]["n"] += 1
                    if fp == 1: kiken_combo[cn]["win"] += 1
                    if fp <= 3: kiken_combo[cn]["p3"] += 1

    print(f"    {'条件':<30s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s}")
    print(f"    {'-'*30} {'-'*5} {'-'*6} {'-'*6}")
    for key in ["0条件以上","1条件以上","2条件以上","3条件以上","4条件以上","5条件以上",
                "gap5++ML低","gap5++comp下位","ML低+前走凡走",
                "gap5++ML低+前走凡走","gap5++ML低+comp下位",
                "ML低+comp下位+前走凡走","gap5++ML低+comp下位+前走凡走",
                "ML<オッズ+gap5+","ML<オッズ+ML低"]:
        s = kiken_combo.get(key)
        if not s or s["n"] < 10: continue
        print(f"    {key:<30s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}%")

    # =====================================================================
    # C. 断層 × 回収率分析
    # =====================================================================
    print(f"\n{'='*70}")
    print(f"  【C】断層 × 回収率分析")
    print(f"{'='*70}")

    # C-1: 印別 × 断層上/下 × 単複回収率
    print(f"\n  [C-1] 印別 × 断層上/下 × 回収率")
    roi_stats = {}
    for race, fm, pay in races:
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
            pos = "上" if rank <= gap_pos else "下"

            win_pay = get_win_payout(pay, hno) if fp == 1 else 0
            place_pay = get_place_payout(pay, hno) if fp <= 3 else 0

            key = (mk, pos)
            if key not in roi_stats: roi_stats[key] = {"n": 0, "win_ret": 0, "place_ret": 0}
            roi_stats[key]["n"] += 1
            roi_stats[key]["win_ret"] += win_pay
            roi_stats[key]["place_ret"] += place_pay

    print(f"    {'印':>2s} {'位置':>4s} {'頭数':>6s} {'単回収':>7s} {'複回収':>7s}")
    print(f"    {'-'*2} {'-'*4} {'-'*6} {'-'*7} {'-'*7}")
    for mk in ["\u25c9","\u25ce","\u25cb","\u25b2","\u25b3","\u2605","\u2606","\u00d7"]:
        for pos in ["上", "下"]:
            s = roi_stats.get((mk, pos))
            if not s or s["n"] < 20: continue
            win_roi = s["win_ret"] / s["n"]
            place_roi = s["place_ret"] / s["n"]
            print(f"    {mk:>2s} {pos:>4s} {s['n']:>6d} {win_roi:>6.1f}% {place_roi:>6.1f}%")

    # C-2: ☆ × 条件別回収率
    print(f"\n  [C-2] ☆穴馬 × 断層位置 × 回収率")
    ana_roi = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)
        for h in horses:
            if h.get("mark") != "\u2606": continue
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue

            if gap_pos is None:
                cat = "断層なし"
            else:
                rank = horse_comp_rank(h, sh)
                if rank <= gap_pos:
                    cat = "断層上"
                elif rank <= gap_pos + 2:
                    cat = "直下(+1~2)"
                else:
                    cat = "断層下(+3~)"

            win_pay = get_win_payout(pay, hno) if fp == 1 else 0
            place_pay = get_place_payout(pay, hno) if fp <= 3 else 0

            if cat not in ana_roi: ana_roi[cat] = {"n": 0, "wr": 0, "pr": 0}
            ana_roi[cat]["n"] += 1
            ana_roi[cat]["wr"] += win_pay
            ana_roi[cat]["pr"] += place_pay

    print(f"    {'条件':<14s} {'頭数':>5s} {'単回収':>7s} {'複回収':>7s}")
    print(f"    {'-'*14} {'-'*5} {'-'*7} {'-'*7}")
    for cat in ["断層上", "直下(+1~2)", "断層下(+3~)", "断層なし"]:
        s = ana_roi.get(cat)
        if not s or s["n"] < 20: continue
        print(f"    {cat:<14s} {s['n']:>5d} {s['wr']/s['n']:>6.1f}% {s['pr']/s['n']:>6.1f}%")

    # =====================================================================
    # D. 無印馬の断層上位群分析
    # =====================================================================
    print(f"\n{'='*70}")
    print(f"  【D】無印馬の断層上位群 — なぜ無印？")
    print(f"{'='*70}")

    # D-1: 断層上の無印馬の特徴
    print(f"\n  [D-1] 断層上の無印馬 × 人気 × ML wp × 成績")
    nomark_detail = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)
        if gap_pos is None: continue
        for h in horses:
            mk = h.get("mark", "")
            if mk and mk != "\uff0d": continue
            rank = horse_comp_rank(h, sh)
            if rank > gap_pos: continue  # 断層上のみ
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue
            pop = h.get("popularity") or 99
            wp = h.get("win_prob", 0) or 0

            if pop <= 3: pb = "1-3人気"
            elif pop <= 6: pb = "4-6人気"
            else: pb = "7+人気"

            if wp >= 0.10: wpb = "wp10%+"
            elif wp >= 0.06: wpb = "wp6-10%"
            else: wpb = "wp<6%"

            key = (pb, wpb)
            if key not in nomark_detail: nomark_detail[key] = {"n": 0, "win": 0, "p3": 0, "wr": 0, "pr": 0}
            nomark_detail[key]["n"] += 1
            if fp == 1: nomark_detail[key]["win"] += 1
            if fp <= 3: nomark_detail[key]["p3"] += 1
            nomark_detail[key]["wr"] += get_win_payout(pay, hno) if fp == 1 else 0
            nomark_detail[key]["pr"] += get_place_payout(pay, hno) if fp <= 3 else 0

    print(f"    {'人気':>8s} {'ML wp':>8s} {'頭数':>5s} {'勝率':>6s} {'複勝率':>6s} {'単回収':>7s} {'複回収':>7s}")
    print(f"    {'-'*8} {'-'*8} {'-'*5} {'-'*6} {'-'*6} {'-'*7} {'-'*7}")
    for pb in ["1-3人気", "4-6人気", "7+人気"]:
        for wpb in ["wp10%+", "wp6-10%", "wp<6%"]:
            s = nomark_detail.get((pb, wpb))
            if not s or s["n"] < 10: continue
            print(f"    {pb:>8s} {wpb:>8s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}% {s['wr']/s['n']:>6.1f}% {s['pr']/s['n']:>6.1f}%")

    # D-2: 断層上の無印 vs 断層下の有印（どっちが強い？）
    print(f"\n  [D-2] 断層上の無印 vs 断層下の有印（同レース比較）")
    compare = {"上無印": {"n": 0, "p3": 0}, "下有印": {"n": 0, "p3": 0}}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)
        if gap_pos is None: continue
        for h in horses:
            mk = h.get("mark", "")
            hno = h.get("horse_no")
            fp = fm.get(hno, 99)
            if fp <= 0 or fp >= 90: continue
            rank = horse_comp_rank(h, sh)
            if rank <= gap_pos and (not mk or mk == "\uff0d"):
                compare["上無印"]["n"] += 1
                if fp <= 3: compare["上無印"]["p3"] += 1
            elif rank > gap_pos and mk and mk != "\uff0d" and mk != "\u00d7":
                compare["下有印"]["n"] += 1
                if fp <= 3: compare["下有印"]["p3"] += 1

    for key in ["上無印", "下有印"]:
        s = compare[key]
        if s["n"] > 0:
            print(f"    {key}: {s['n']:>6d}頭  複勝率 {s['p3']/s['n']*100:.1f}%")

    # =====================================================================
    # E. 複数断層・断層位置の質的分析
    # =====================================================================
    print(f"\n{'='*70}")
    print(f"  【E】断層の質的分析")
    print(f"{'='*70}")

    # E-1: 断層位置（何位-何位間か）× 成績
    print(f"\n  [E-1] 断層位置（何位-何位間か）× 1位の成績")
    pos_stats = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)
        if gap_pos is None or gap_size < 2.5: continue
        hno = sh[0].get("horse_no")
        fp = fm.get(hno, 99)
        if fp <= 0 or fp >= 90: continue
        key = f"{gap_pos}位-{gap_pos+1}位間"
        if key not in pos_stats: pos_stats[key] = {"n": 0, "win": 0, "p3": 0}
        pos_stats[key]["n"] += 1
        if fp == 1: pos_stats[key]["win"] += 1
        if fp <= 3: pos_stats[key]["p3"] += 1

    print(f"    {'断層位置':<12s} {'R数':>5s} {'1位勝率':>7s} {'1位複勝':>7s}")
    print(f"    {'-'*12} {'-'*5} {'-'*7} {'-'*7}")
    for p in range(1, 8):
        key = f"{p}位-{p+1}位間"
        s = pos_stats.get(key)
        if not s or s["n"] < 20: continue
        print(f"    {key:<12s} {s['n']:>5d} {s['win']/s['n']*100:>6.1f}% {s['p3']/s['n']*100:>6.1f}%")

    # E-2: 複数断層レース
    print(f"\n  [E-2] 断層の数 × 3着以内の上位群占有率")
    multi = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gaps = find_all_gaps(sh)
        ng = len(gaps)
        if ng == 0: ng_label = "断層0"
        elif ng == 1: ng_label = "断層1"
        elif ng == 2: ng_label = "断層2"
        else: ng_label = "断層3+"

        # 最初の断層の上位群が3着以内を何頭占めるか
        if ng > 0:
            first_pos = gaps[0][0]
            above_in3 = sum(1 for j in range(first_pos) if 0 < fm.get(sh[j].get("horse_no"), 99) <= 3)
        else:
            above_in3 = 0

        if ng_label not in multi: multi[ng_label] = {"n": 0, "occ": [0,0,0,0]}
        multi[ng_label]["n"] += 1
        multi[ng_label]["occ"][min(above_in3, 3)] += 1

    print(f"    {'断層数':<8s} {'R数':>5s} {'0頭':>5s} {'1頭':>5s} {'2頭':>5s} {'3頭':>5s} {'平均占有':>8s}")
    print(f"    {'-'*8} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*8}")
    for nl in ["断層0", "断層1", "断層2", "断層3+"]:
        s = multi.get(nl)
        if not s or s["n"] < 20: continue
        t = s["n"]; o = s["occ"]
        avg = (o[1]*1 + o[2]*2 + o[3]*3) / t
        print(f"    {nl:<8s} {t:>5d} {o[0]/t*100:>4.1f}% {o[1]/t*100:>4.1f}% {o[2]/t*100:>4.1f}% {o[3]/t*100:>4.1f}% {avg:>7.2f}")

    # E-3: 1-2位間 vs 2-3位間 vs 3-4位間 断層の意味の違い
    print(f"\n  [E-3] 断層位置別 — 上位群全体のP3占有率")
    pos_occupy = {}
    for race, fm, pay in races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite",0) or 0, reverse=True)
        gap_pos, gap_size = find_first_gap(sh)
        if gap_pos is None: continue
        above_in3 = sum(1 for j in range(gap_pos) if 0 < fm.get(sh[j].get("horse_no"), 99) <= 3)
        max_possible = min(gap_pos, 3)

        key = gap_pos
        if key not in pos_occupy: pos_occupy[key] = {"n": 0, "full": 0, "avg_in3": 0}
        pos_occupy[key]["n"] += 1
        if above_in3 >= max_possible: pos_occupy[key]["full"] += 1
        pos_occupy[key]["avg_in3"] += above_in3

    print(f"    {'断層位置':>8s} {'R数':>5s} {'全員3着内':>10s} {'平均占有':>8s}")
    print(f"    {'-'*8} {'-'*5} {'-'*10} {'-'*8}")
    for p in range(1, 8):
        s = pos_occupy.get(p)
        if not s or s["n"] < 30: continue
        print(f"    {p}位-{p+1}位 {s['n']:>5d} {s['full']/s['n']*100:>9.1f}% {s['avg_in3']/s['n']:>7.2f}")


# === 実行 ===
print(f"\n  分析開始... 経過{time.time()-t0:.0f}s", flush=True)
analyze_all(jra_races, "JRA")
print(f"\n  JRA完了 経過{time.time()-t0:.0f}s / NAR分析開始...", flush=True)
analyze_all(nar_races, "NAR")

print(f"\n  全処理完了: {time.time()-t0:.1f}s")
