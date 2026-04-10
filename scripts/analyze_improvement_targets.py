"""
改善ターゲット詳細分析スクリプト
◎複勝率54.2%→70%達成のための根本原因特定
"""
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PRED_DIR = Path("data/predictions")
RESULT_DIR = Path("data/results")
JRA_VENUES = {"01","02","03","04","05","06","07","08","09","10"}


def load_all_data():
    results_map = {}
    for f in sorted(RESULT_DIR.glob("*_results.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for race_id, rdata in data.items():
                finish_map = {}
                for o in rdata.get("order", []):
                    finish_map[o["horse_no"]] = o.get("finish", 99)
                results_map[race_id] = finish_map
        except Exception:
            continue

    races = []
    for f in sorted(PRED_DIR.glob("*_pred.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for race in data.get("races", []):
                rid = race.get("race_id", "")
                if rid not in results_map:
                    continue
                res = results_map[rid]
                for h in race.get("horses", []):
                    h["finish_pos"] = res.get(h.get("horse_no"), 99)
                races.append(race)
        except Exception:
            continue
    print(f"照合済みレース: {len(races)}")
    return races


def analyze_honmei_failure(races):
    """◎が外れるパターンの詳細分析"""
    print("\n" + "="*70)
    print("分析1: ◎が外れるパターン")
    print("="*70)

    # ◎が外れたとき、実際の勝ち馬の印
    winner_mark_when_honmei_fails = defaultdict(int)
    # gap分布（外れた時 vs 当たった時）
    gap_hit = []
    gap_miss = []
    # win_prob分布
    wp_hit = []
    wp_miss = []
    # reliability分布
    rel_hit = defaultdict(int)
    rel_miss = defaultdict(int)
    # 自信度別
    conf_stats = defaultdict(lambda: {"total":0, "p3":0})

    for race in races:
        horses = race.get("horses", [])
        sorted_by_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
        if len(sorted_by_comp) < 2:
            continue

        honmei = None
        for h in horses:
            if h.get("mark") in ("◎", "◉"):
                honmei = h
                break
        if not honmei:
            continue

        gap = sorted_by_comp[0].get("composite", 0) - sorted_by_comp[1].get("composite", 0)
        wp = honmei.get("win_prob", 0) or 0
        rel = honmei.get("ability_reliability", "?")
        conf = race.get("confidence", "?")
        fp = honmei["finish_pos"]

        conf_stats[conf]["total"] += 1
        if fp <= 3:
            conf_stats[conf]["p3"] += 1
            gap_hit.append(gap)
            wp_hit.append(wp)
            rel_hit[rel] += 1
        else:
            gap_miss.append(gap)
            wp_miss.append(wp)
            rel_miss[rel] += 1
            # 勝ち馬の印
            for h2 in horses:
                if h2["finish_pos"] == 1:
                    winner_mark_when_honmei_fails[h2.get("mark", "無印")] += 1
                    break

    # 勝ち馬の印分布
    total_fails = sum(winner_mark_when_honmei_fails.values())
    print(f"\n◎が外れた時の勝ち馬の印 (N={total_fails}):")
    for mk in ["◉","◎","○","▲","△","★","☆","×","","無印"]:
        cnt = winner_mark_when_honmei_fails.get(mk, 0)
        if cnt == 0: continue
        label = mk if mk else "無印"
        print(f"  {label}: {cnt} ({cnt/total_fails*100:.1f}%)")

    # gap分布
    if gap_hit and gap_miss:
        import statistics
        print(f"\ncomposite gap (1位-2位):")
        print(f"  的中時: 平均{statistics.mean(gap_hit):.2f} 中央値{statistics.median(gap_hit):.2f}")
        print(f"  外れ時: 平均{statistics.mean(gap_miss):.2f} 中央値{statistics.median(gap_miss):.2f}")
        # gap < 2.0で外れる率
        small_gap_total = sum(1 for g in gap_hit + gap_miss if g < 2.0)
        small_gap_miss = sum(1 for g in gap_miss if g < 2.0)
        if small_gap_total:
            print(f"  gap<2.0pt: {small_gap_total}件中{small_gap_miss}件外れ ({small_gap_miss/small_gap_total*100:.1f}%)")
        large_gap_total = sum(1 for g in gap_hit + gap_miss if g >= 5.0)
        large_gap_miss = sum(1 for g in gap_miss if g >= 5.0)
        if large_gap_total:
            print(f"  gap>=5.0pt: {large_gap_total}件中{large_gap_miss}件外れ ({large_gap_miss/large_gap_total*100:.1f}%)")

    # win_prob分布
    if wp_hit and wp_miss:
        import statistics
        print(f"\nwin_prob (ML勝率):")
        print(f"  的中時: 平均{statistics.mean(wp_hit):.3f}")
        print(f"  外れ時: 平均{statistics.mean(wp_miss):.3f}")
        # win_prob帯別
        for lo, hi, label in [(0, 0.15, "<15%"), (0.15, 0.25, "15-25%"), (0.25, 0.35, "25-35%"), (0.35, 1.0, "35%+")]:
            total = sum(1 for w in wp_hit + wp_miss if lo <= w < hi)
            miss = sum(1 for w in wp_miss if lo <= w < hi)
            if total:
                print(f"  win_prob {label}: {total}件 外れ率{miss/total*100:.1f}%")

    # reliability分布
    print(f"\nability_reliability別:")
    for rel in ["A", "B", "C", "D", "?"]:
        hit = rel_hit.get(rel, 0)
        miss = rel_miss.get(rel, 0)
        total = hit + miss
        if total == 0: continue
        print(f"  {rel}: {total}件 複勝率{hit/total*100:.1f}%")

    # 自信度別◎複勝率
    print(f"\n自信度別◎複勝率:")
    for c in ["SS","S","A","B","C","D","E"]:
        s = conf_stats[c]
        if s["total"] == 0: continue
        print(f"  {c}: {s['total']}件 複勝率{s['p3']/s['total']*100:.1f}%")


def analyze_tekipan_sensitivity(races):
    """◉判定基準の感度分析"""
    print("\n" + "="*70)
    print("分析2: ◉判定基準の感度分析")
    print("="*70)

    # 現在の◉/◎分布
    tekipan_count = 0
    honmei_count = 0
    total_races = 0

    # 基準緩和シミュレーション
    # 各条件レベルでの◉候補数と的中率
    thresholds = [
        ("現在JRA", 1.0, 0.30, 0.60),
        ("緩和A", 1.0, 0.25, 0.50),
        ("緩和B", 0.5, 0.25, 0.50),
        ("緩和C", 0.5, 0.20, 0.40),
        ("NAR現在", 4.0, 0.30, 0.0),
        ("NAR緩和", 3.0, 0.25, 0.0),
        ("NAR大幅緩和", 2.0, 0.20, 0.0),
    ]

    sim_stats = {label: {"total":0,"p3":0} for label, _, _, _ in thresholds}

    for race in races:
        horses = race.get("horses", [])
        sorted_h = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
        if len(sorted_h) < 2:
            continue
        total_races += 1

        top = sorted_h[0]
        gap = top.get("composite", 0) - sorted_h[1].get("composite", 0)
        wp = top.get("win_prob", 0) or 0
        p3p = top.get("place3_prob", 0) or 0
        fp = top["finish_pos"]
        mk = top.get("mark", "")

        if mk == "◉": tekipan_count += 1
        elif mk == "◎": honmei_count += 1

        vc = race.get("venue_code", "")
        if not vc:
            rid = race.get("race_id", "")
            vc = rid[4:6] if len(rid) >= 6 else ""
        is_jra = vc in JRA_VENUES

        for label, g_thr, wp_thr, p3_thr in thresholds:
            if "NAR" in label and is_jra: continue
            if "NAR" not in label and not is_jra: continue
            if gap >= g_thr and wp >= wp_thr and p3p >= p3_thr:
                sim_stats[label]["total"] += 1
                if fp <= 3: sim_stats[label]["p3"] += 1

    print(f"\n現在の◉/◎分布:")
    print(f"  ◉: {tekipan_count}レース ({tekipan_count/total_races*100:.1f}%)")
    print(f"  ◎: {honmei_count}レース ({honmei_count/total_races*100:.1f}%)")

    print(f"\n◉判定基準の感度分析:")
    print(f"{'基準':>12} {'◉候補':>6} {'複勝率':>6}")
    print("-" * 30)
    for label, _, _, _ in thresholds:
        s = sim_stats[label]
        if s["total"] == 0: continue
        print(f"{label:>12} {s['total']:>6} {s['p3']/s['total']*100:>5.1f}%")


def analyze_kiken_improvement(races):
    """×危険馬判定の改善余地"""
    print("\n" + "="*70)
    print("分析3: ×危険馬の判定改善")
    print("="*70)

    # 現在の×が的中/失敗した場合の特徴
    kiken_by_score = defaultdict(lambda: {"total":0,"fell":0})
    kiken_by_pop = defaultdict(lambda: {"total":0,"fell":0})

    for race in races:
        for h in race.get("horses", []):
            if not h.get("is_tokusen_kiken"):
                continue
            fp = h["finish_pos"]
            fell = fp >= 4
            # スコア帯別
            ks = h.get("tokusen_kiken_score", 0) or 0
            if ks >= 6: bucket = "6+"
            elif ks >= 5: bucket = "5-6"
            elif ks >= 4: bucket = "4-5"
            elif ks >= 3: bucket = "3-4"
            else: bucket = "<3"
            kiken_by_score[bucket]["total"] += 1
            if fell: kiken_by_score[bucket]["fell"] += 1
            # 人気帯別
            pop = h.get("popularity") or 99
            if pop <= 1: pbucket = "1人気"
            elif pop <= 2: pbucket = "2人気"
            elif pop <= 3: pbucket = "3人気"
            elif pop <= 5: pbucket = "4-5人気"
            else: pbucket = "6人気+"
            kiken_by_pop[pbucket]["total"] += 1
            if fell: kiken_by_pop[pbucket]["fell"] += 1

    print(f"\n危険馬スコア帯別 4着以下率:")
    for bucket in ["<3", "3-4", "4-5", "5-6", "6+"]:
        s = kiken_by_score[bucket]
        if s["total"] == 0: continue
        print(f"  {bucket:>4}: {s['total']:>5}頭 4着以下率{s['fell']/s['total']*100:.1f}%")

    print(f"\n危険馬の人気別 4着以下率:")
    for bucket in ["1人気", "2人気", "3人気", "4-5人気", "6人気+"]:
        s = kiken_by_pop[bucket]
        if s["total"] == 0: continue
        print(f"  {bucket:>6}: {s['total']:>5}頭 4着以下率{s['fell']/s['total']*100:.1f}%")


def analyze_ss_vs_s(races):
    """SS自信度がSより低い原因分析"""
    print("\n" + "="*70)
    print("分析4: SS vs S 逆転現象の原因")
    print("="*70)

    # SS/S別にJRA/NAR分離
    stats = defaultdict(lambda: {"total":0,"p3":0,"avg_gap":[],"avg_wp":[]})

    for race in races:
        conf = race.get("confidence", "")
        if conf not in ("SS", "S"): continue
        vc = race.get("venue_code", "")
        if not vc:
            rid = race.get("race_id", "")
            vc = rid[4:6] if len(rid) >= 6 else ""
        cat = "JRA" if vc in JRA_VENUES else "NAR"
        key = f"{conf}_{cat}"

        for h in race.get("horses", []):
            if h.get("mark") not in ("◎", "◉"): continue
            fp = h["finish_pos"]
            stats[key]["total"] += 1
            if fp <= 3: stats[key]["p3"] += 1

        # gap計算
        sorted_h = sorted(race.get("horses", []), key=lambda h: h.get("composite", 0), reverse=True)
        if len(sorted_h) >= 2:
            gap = sorted_h[0].get("composite", 0) - sorted_h[1].get("composite", 0)
            stats[key]["avg_gap"].append(gap)
            stats[key]["avg_wp"].append(sorted_h[0].get("win_prob", 0) or 0)

    import statistics as stat_mod
    print(f"\n{'区分':>8} {'◎数':>5} {'複勝率':>6} {'平均gap':>7} {'平均WP':>7}")
    print("-" * 45)
    for key in ["SS_JRA", "S_JRA", "SS_NAR", "S_NAR"]:
        s = stats[key]
        if s["total"] == 0: continue
        avg_g = stat_mod.mean(s["avg_gap"]) if s["avg_gap"] else 0
        avg_w = stat_mod.mean(s["avg_wp"]) if s["avg_wp"] else 0
        print(f"{key:>8} {s['total']:>5} {s['p3']/s['total']*100:>5.1f}% {avg_g:>6.2f} {avg_w:>6.3f}")


def analyze_ml_composite_alignment(races):
    """ML予測とcomposite順位のズレ分析"""
    print("\n" + "="*70)
    print("分析5: ML予測 vs composite のズレ")
    print("="*70)

    agree = 0
    disagree = 0
    agree_p3 = 0
    disagree_p3 = 0
    agree_total = 0
    disagree_total = 0

    for race in races:
        horses = race.get("horses", [])
        if len(horses) < 3: continue
        comp_top = max(horses, key=lambda h: h.get("composite", 0))
        wp_top = max(horses, key=lambda h: h.get("win_prob", 0) or 0)

        if comp_top["horse_no"] == wp_top["horse_no"]:
            agree += 1
            agree_total += 1
            if comp_top["finish_pos"] <= 3: agree_p3 += 1
        else:
            disagree += 1
            disagree_total += 1
            if comp_top["finish_pos"] <= 3: disagree_p3 += 1

    total = agree + disagree
    print(f"\ncomposite1位 = win_prob1位（ML合意）:")
    print(f"  合意: {agree}レース ({agree/total*100:.1f}%)")
    print(f"  不一致: {disagree}レース ({disagree/total*100:.1f}%)")
    if agree_total:
        print(f"\n  合意時の◎複勝率: {agree_p3/agree_total*100:.1f}%")
    if disagree_total:
        print(f"  不一致時の◎複勝率: {disagree_p3/disagree_total*100:.1f}%")
    print(f"\n  → ML不一致時の精度低下: {(agree_p3/agree_total - disagree_p3/disagree_total)*100:.1f}ポイント")


def main():
    print("="*70)
    print("D-AI Keiba v3 — 改善ターゲット詳細分析")
    print("="*70)
    races = load_all_data()
    if not races:
        print("データなし")
        return

    analyze_honmei_failure(races)
    analyze_tekipan_sensitivity(races)
    analyze_kiken_improvement(races)
    analyze_ss_vs_s(races)
    analyze_ml_composite_alignment(races)

    print("\n" + "="*70)
    print("分析完了")
    print("="*70)


if __name__ == "__main__":
    main()
