#!/usr/bin/env python3
"""
断層パターン × ◎◉成績 深堀分析（JRA限定）

分析軸:
  1. 現行◉/◎の断層パターン別成績（gap1サイズ × 勝率/複勝率/回収率）
  2. ◎→◉昇格候補の発掘（断層は大きいが現行では◉にならないケース）
  3. ◉で外れるケース分析（◉なのに好走できないパターン）
  4. レースタイプ × 印 × 成績のクロス分析
  5. 断層+ML合意/不一致 × 成績（最強条件の特定）
  6. ◉昇格シミュレーション（新条件での成績予測）
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
            fm = {e["horse_no"]: e for e in data.get("order", []) if "horse_no" in e and "finish" in e}
            all_results[rid] = {"order": fm, "payouts": data.get("payouts", {})}
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
        if rid not in all_results: continue
        if r.get("venue", "") in JRA_VENUES:
            jra_races.append((r, all_results[rid]))
    if (i+1) % 200 == 0:
        print(f"  [{(i+1)/len(target)*100:.0f}%] {i+1}/{len(target)} loaded", flush=True)

print(f"  JRA: {len(jra_races)}R ({time.time()-t0:.1f}s)")


def get_tansho_payout(payouts, horse_no):
    """単勝払戻金を取得"""
    tp = payouts.get("単勝", {})
    if isinstance(tp, dict):
        combo = tp.get("combo", "")
        if str(horse_no) == str(combo):
            return tp.get("payout", 0)
    return 0


def classify_race(sorted_h):
    """レースタイプ分類"""
    gaps = []
    for i in range(1, min(len(sorted_h), 6)):
        g = (sorted_h[i-1].get("composite",0) or 0) - (sorted_h[i].get("composite",0) or 0)
        gaps.append(g)
    if not gaps: return "不明", gaps
    g1 = gaps[0] if len(gaps) > 0 else 0
    g2 = gaps[1] if len(gaps) > 1 else 0
    g3 = gaps[2] if len(gaps) > 2 else 0
    mg = max(gaps[:5])
    if g1 >= 5:       return "独走型", gaps
    elif g1 < 2 and g2 >= 4: return "2強型", gaps
    elif g1 < 2 and g2 < 2 and g3 >= 3: return "3強型", gaps
    elif mg < 2.5:    return "全混戦", gaps
    else:             return "上位拮抗", gaps


# =================================================================
# 分析1: 現行◉/◎の断層パターン別成績
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析1】◉/◎ × 1-2位間gap × 成績")
print(f"{'='*70}")

GAP_BINS = [(0, 2, "0-2pt"), (2, 4, "2-4pt"), (4, 6, "4-6pt"), (6, 9, "6-9pt"), (9, 999, "9pt+")]

mark_gap = {}  # (mark, gap_bin) -> {n, win, p2, p3, stake, ret}

for race, res in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
    gap1 = (sh[0].get("composite",0) or 0) - (sh[1].get("composite",0) or 0)

    for h in horses:
        mk = h.get("mark", "")
        if mk not in ("\u25c9", "\u25ce"): continue  # ◉ or ◎
        hno = h.get("horse_no")
        entry = res["order"].get(hno)
        if not entry: continue
        fp = entry.get("finish", 99)
        if fp <= 0 or fp >= 90: continue
        payout = get_tansho_payout(res["payouts"], hno)

        for lo, hi, bn in GAP_BINS:
            if lo <= gap1 < hi:
                key = (mk, bn)
                if key not in mark_gap:
                    mark_gap[key] = {"n": 0, "win": 0, "p2": 0, "p3": 0, "stake": 0, "ret": 0}
                s = mark_gap[key]
                s["n"] += 1; s["stake"] += 100
                if fp == 1: s["win"] += 1; s["ret"] += payout
                if fp <= 2: s["p2"] += 1
                if fp <= 3: s["p3"] += 1
                break

for mk_char, mk_name in [("\u25c9", "◉鉄板"), ("\u25ce", "◎本命")]:
    print(f"\n  {mk_name}:")
    print(f"  {'gap1':>8s} {'R数':>5s} {'勝率':>6s} {'連対':>6s} {'複勝':>6s} {'単回収':>7s}")
    print(f"  {'-'*8} {'-'*5} {'-'*6} {'-'*6} {'-'*6} {'-'*7}")
    for _, _, bn in GAP_BINS:
        s = mark_gap.get((mk_char, bn))
        if not s or s["n"] < 10: continue
        roi = s["ret"] / max(1, s["stake"]) * 100
        print(f"  {bn:>8s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p2']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}% {roi:>6.1f}%")

# =================================================================
# 分析2: レースタイプ × ◉◎成績
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析2】レースタイプ × ◉◎成績")
print(f"{'='*70}")

type_mark = {}  # (type, mark) -> {n, win, p3, stake, ret}

for race, res in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
    rtype, _ = classify_race(sh)

    for h in horses:
        mk = h.get("mark", "")
        if mk not in ("\u25c9", "\u25ce"): continue
        hno = h.get("horse_no")
        entry = res["order"].get(hno)
        if not entry: continue
        fp = entry.get("finish", 99)
        if fp <= 0 or fp >= 90: continue
        payout = get_tansho_payout(res["payouts"], hno)

        key = (rtype, mk)
        if key not in type_mark:
            type_mark[key] = {"n": 0, "win": 0, "p3": 0, "stake": 0, "ret": 0}
        s = type_mark[key]
        s["n"] += 1; s["stake"] += 100
        if fp == 1: s["win"] += 1; s["ret"] += payout
        if fp <= 3: s["p3"] += 1

print(f"  {'タイプ':<10s} {'印':>2s} {'R数':>5s} {'勝率':>6s} {'複勝':>6s} {'単回収':>7s}")
print(f"  {'-'*10} {'-'*2} {'-'*5} {'-'*6} {'-'*6} {'-'*7}")
for rt in ["独走型", "2強型", "3強型", "上位拮抗", "全混戦"]:
    for mk_char, mk_sym in [("\u25c9", "◉"), ("\u25ce", "◎")]:
        s = type_mark.get((rt, mk_char))
        if not s or s["n"] < 10: continue
        roi = s["ret"] / max(1, s["stake"]) * 100
        print(f"  {rt:<10s} {mk_sym:>2s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}% {roi:>6.1f}%")

# =================================================================
# 分析3: ML合意 × 断層 × 成績（最強条件の特定）
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析3】ML合意/不一致 × 断層 × ◎◉成績")
print(f"{'='*70}")

# ML合意 = composite1位とwin_prob1位が同一馬
# 断層 = gap1のサイズ
ml_gap = {}  # (ml_agree, gap_bucket) -> {n, win, p3, stake, ret}

for race, res in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
    wp_top = max(horses, key=lambda h: h.get("win_prob", 0) or 0)
    comp_top = sh[0]
    ml_agree = comp_top.get("horse_no") == wp_top.get("horse_no")
    gap1 = (sh[0].get("composite",0) or 0) - (sh[1].get("composite",0) or 0)

    # ◎/◉馬の成績
    for h in horses:
        mk = h.get("mark", "")
        if mk not in ("\u25c9", "\u25ce"): continue
        hno = h.get("horse_no")
        entry = res["order"].get(hno)
        if not entry: continue
        fp = entry.get("finish", 99)
        if fp <= 0 or fp >= 90: continue
        payout = get_tansho_payout(res["payouts"], hno)

        GAP3 = [(0, 3, "gap<3"), (3, 6, "gap3-6"), (6, 999, "gap6+")]
        for lo, hi, gb in GAP3:
            if lo <= gap1 < hi:
                ag_str = "ML合意" if ml_agree else "ML不一致"
                key = (ag_str, gb)
                if key not in ml_gap:
                    ml_gap[key] = {"n": 0, "win": 0, "p3": 0, "stake": 0, "ret": 0}
                s = ml_gap[key]
                s["n"] += 1; s["stake"] += 100
                if fp == 1: s["win"] += 1; s["ret"] += payout
                if fp <= 3: s["p3"] += 1
                break

print(f"  {'ML':>8s} {'断層':>7s} {'R数':>5s} {'勝率':>6s} {'複勝':>6s} {'単回収':>7s}")
print(f"  {'-'*8} {'-'*7} {'-'*5} {'-'*6} {'-'*6} {'-'*7}")
for ag in ["ML合意", "ML不一致"]:
    for _, _, gb in [(0,3,"gap<3"),(3,6,"gap3-6"),(6,999,"gap6+")]:
        s = ml_gap.get((ag, gb))
        if not s or s["n"] < 20: continue
        roi = s["ret"] / max(1, s["stake"]) * 100
        print(f"  {ag:>8s} {gb:>7s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}% {roi:>6.1f}%")

# =================================================================
# 分析4: ◉外れパターン分析（◉で4着以下のケース）
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析4】◉が外れるパターン（4着以下の内訳）")
print(f"{'='*70}")

tekipan_miss = {"total": 0, "by_gap": {}, "by_pop": {}, "by_field": {}, "by_rtype": {}}

for race, res in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
    gap1 = (sh[0].get("composite",0) or 0) - (sh[1].get("composite",0) or 0)
    rtype, _ = classify_race(sh)

    for h in horses:
        if h.get("mark") != "\u25c9": continue
        hno = h.get("horse_no")
        entry = res["order"].get(hno)
        if not entry: continue
        fp = entry.get("finish", 99)
        if fp <= 0 or fp >= 90: continue

        tekipan_miss["total"] += 1
        is_miss = fp >= 4

        # gap別
        for lo, hi, gb in [(0,3,"gap<3"),(3,5,"gap3-5"),(5,7,"gap5-7"),(7,999,"gap7+")]:
            if lo <= gap1 < hi:
                if gb not in tekipan_miss["by_gap"]:
                    tekipan_miss["by_gap"][gb] = {"n": 0, "miss": 0}
                tekipan_miss["by_gap"][gb]["n"] += 1
                if is_miss: tekipan_miss["by_gap"][gb]["miss"] += 1
                break

        # 人気別
        pop = h.get("popularity", 0)
        pb = f"{pop}人気" if pop <= 5 else "6+人気"
        if pb not in tekipan_miss["by_pop"]:
            tekipan_miss["by_pop"][pb] = {"n": 0, "miss": 0}
        tekipan_miss["by_pop"][pb]["n"] += 1
        if is_miss: tekipan_miss["by_pop"][pb]["miss"] += 1

        # 頭数別
        fc = len(horses)
        fb = "~10頭" if fc <= 10 else "11-14頭" if fc <= 14 else "15頭+"
        if fb not in tekipan_miss["by_field"]:
            tekipan_miss["by_field"][fb] = {"n": 0, "miss": 0}
        tekipan_miss["by_field"][fb]["n"] += 1
        if is_miss: tekipan_miss["by_field"][fb]["miss"] += 1

        # レースタイプ別
        if rtype not in tekipan_miss["by_rtype"]:
            tekipan_miss["by_rtype"][rtype] = {"n": 0, "miss": 0}
        tekipan_miss["by_rtype"][rtype]["n"] += 1
        if is_miss: tekipan_miss["by_rtype"][rtype]["miss"] += 1

if tekipan_miss["total"] > 0:
    print(f"  ◉総数: {tekipan_miss['total']}")
    print(f"\n  断層別:")
    for gb in ["gap<3", "gap3-5", "gap5-7", "gap7+"]:
        s = tekipan_miss["by_gap"].get(gb)
        if not s: continue
        miss_r = s["miss"] / max(1, s["n"]) * 100
        hit_r = 100 - miss_r
        print(f"    {gb:<8s} {s['n']:>4d}R  的中{hit_r:.1f}%  外れ{miss_r:.1f}%")

    print(f"\n  人気別:")
    for pb in ["1人気","2人気","3人気","4人気","5人気","6+人気"]:
        s = tekipan_miss["by_pop"].get(pb)
        if not s: continue
        miss_r = s["miss"] / max(1, s["n"]) * 100
        print(f"    {pb:<8s} {s['n']:>4d}R  外れ率{miss_r:.1f}%")

    print(f"\n  頭数別:")
    for fb in ["~10頭", "11-14頭", "15頭+"]:
        s = tekipan_miss["by_field"].get(fb)
        if not s: continue
        miss_r = s["miss"] / max(1, s["n"]) * 100
        print(f"    {fb:<8s} {s['n']:>4d}R  外れ率{miss_r:.1f}%")

    print(f"\n  レースタイプ別:")
    for rt in ["独走型","2強型","3強型","上位拮抗","全混戦"]:
        s = tekipan_miss["by_rtype"].get(rt)
        if not s: continue
        miss_r = s["miss"] / max(1, s["n"]) * 100
        print(f"    {rt:<10s} {s['n']:>4d}R  外れ率{miss_r:.1f}%")

# =================================================================
# 分析5: ◉昇格シミュレーション（現行◎で断層条件を満たすもの）
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析5】◎→◉昇格シミュレーション")
print(f"{'='*70}")

# 現行◉条件: gap>=TEKIPAN_GAP + wp>=TEKIPAN_WP + p3>=TEKIPAN_P3 + EV>=TEKIPAN_MIN_EV
# 新条件案: ML合意 + gap条件を緩和、もしくはML合意のとき断層条件で◉昇格

# 各条件の組み合わせ
SIM_PLANS = {
    "現行◉": {},  # 実データそのまま
    "案A: gap>=4 + ML合意": {"min_gap": 4, "require_ml": True},
    "案B: gap>=3 + ML合意": {"min_gap": 3, "require_ml": True},
    "案C: gap>=5 + ML合意": {"min_gap": 5, "require_ml": True},
    "案D: gap>=3 + ML合意 + 独走or拮抗": {"min_gap": 3, "require_ml": True, "require_type": ["独走型", "上位拮抗"]},
    "案E: gap>=4 + wp>=10%": {"min_gap": 4, "require_wp": 0.10},
    "案F: gap>=3 + ML合意 + wp>=8%": {"min_gap": 3, "require_ml": True, "require_wp": 0.08},
}

sim_results = {}
for plan_name, cond in SIM_PLANS.items():
    stats = {"n": 0, "win": 0, "p3": 0, "stake": 0, "ret": 0}

    for race, res in jra_races:
        horses = race.get("horses", [])
        if len(horses) < 5: continue
        sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
        gap1 = (sh[0].get("composite",0) or 0) - (sh[1].get("composite",0) or 0)
        wp_top = max(horses, key=lambda h: h.get("win_prob", 0) or 0)
        comp_top = sh[0]
        ml_agree = comp_top.get("horse_no") == wp_top.get("horse_no")
        rtype, _ = classify_race(sh)

        if plan_name == "現行◉":
            # 実際に◉が付いている馬
            honmei = None
            for h in horses:
                if h.get("mark") == "\u25c9":
                    honmei = h
                    break
            if not honmei: continue
        else:
            # ◎または◉の馬（composite1位 or win_prob1位）をシミュレーション
            honmei = sh[0]  # composite1位
            # 条件チェック
            if "min_gap" in cond and gap1 < cond["min_gap"]: continue
            if cond.get("require_ml") and not ml_agree: continue
            if "require_wp" in cond and (honmei.get("win_prob", 0) or 0) < cond["require_wp"]: continue
            if "require_type" in cond and rtype not in cond["require_type"]: continue

        hno = honmei.get("horse_no")
        entry = res["order"].get(hno)
        if not entry: continue
        fp = entry.get("finish", 99)
        if fp <= 0 or fp >= 90: continue
        payout = get_tansho_payout(res["payouts"], hno)

        stats["n"] += 1; stats["stake"] += 100
        if fp == 1: stats["win"] += 1; stats["ret"] += payout
        if fp <= 3: stats["p3"] += 1

    sim_results[plan_name] = stats

print(f"\n  {'案名':<34s} {'R数':>5s} {'勝率':>6s} {'複勝':>6s} {'単回収':>7s}")
print(f"  {'-'*34} {'-'*5} {'-'*6} {'-'*6} {'-'*7}")
for name in SIM_PLANS:
    s = sim_results[name]
    if s["n"] < 10: continue
    roi = s["ret"] / max(1, s["stake"]) * 100
    print(f"  {name:<34s} {s['n']:>5d} {s['win']/s['n']*100:>5.1f}% {s['p3']/s['n']*100:>5.1f}% {roi:>6.1f}%")

# =================================================================
# 分析6: ◎の中で◉に「昇格すべきだった」ケースの特徴
# =================================================================
print(f"\n{'='*70}")
print(f"  【分析6】◎の中で1着になったケース — 共通特徴")
print(f"{'='*70}")

honmei_win_features = {"gap": [], "wp": [], "ml_agree": 0, "ml_total": 0, "rtype": {}}
honmei_miss_features = {"gap": [], "wp": [], "ml_agree": 0, "ml_total": 0, "rtype": {}}

for race, res in jra_races:
    horses = race.get("horses", [])
    if len(horses) < 5: continue
    sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
    gap1 = (sh[0].get("composite",0) or 0) - (sh[1].get("composite",0) or 0)
    wp_top = max(horses, key=lambda h: h.get("win_prob", 0) or 0)
    comp_top = sh[0]
    ml_agree = comp_top.get("horse_no") == wp_top.get("horse_no")
    rtype, _ = classify_race(sh)

    for h in horses:
        if h.get("mark") != "\u25ce": continue  # ◎のみ
        hno = h.get("horse_no")
        entry = res["order"].get(hno)
        if not entry: continue
        fp = entry.get("finish", 99)
        if fp <= 0 or fp >= 90: continue

        target = honmei_win_features if fp == 1 else honmei_miss_features
        target["gap"].append(gap1)
        target["wp"].append(h.get("win_prob", 0) or 0)
        target["ml_total"] += 1
        if ml_agree: target["ml_agree"] += 1
        if rtype not in target["rtype"]: target["rtype"][rtype] = 0
        target["rtype"][rtype] += 1

for label, feat in [("◎で1着", honmei_win_features), ("◎で4着以下", honmei_miss_features)]:
    if not feat["gap"]: continue
    avg_gap = sum(feat["gap"]) / len(feat["gap"])
    avg_wp = sum(feat["wp"]) / len(feat["wp"])
    ml_rate = feat["ml_agree"] / max(1, feat["ml_total"]) * 100
    total = feat["ml_total"]
    print(f"\n  {label} ({total}件):")
    print(f"    平均gap1: {avg_gap:.2f}pt")
    print(f"    平均wp:   {avg_wp*100:.2f}%")
    print(f"    ML合意率: {ml_rate:.1f}%")
    print(f"    レースタイプ分布:")
    for rt in ["独走型","2強型","3強型","上位拮抗","全混戦"]:
        n = feat["rtype"].get(rt, 0)
        pct = n / max(1, total) * 100
        print(f"      {rt}: {n:>4d} ({pct:.1f}%)")

total_time = time.time() - t0
print(f"\n  処理時間: {total_time:.1f}s")
