"""
全仮説テスト検証スクリプト
813日分のpred.json + 810日分のresults.jsonを読み込み、
各仮説を数値で検証してレポート出力する。
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
VENUE_NAMES = {
    "01":"福島","02":"新潟","03":"札幌","04":"函館","05":"東京",
    "06":"中山","07":"中京","08":"京都","09":"阪神","10":"小倉",
    "30":"門別","35":"盛岡","36":"水沢","42":"浦和","43":"船橋",
    "44":"大井","45":"川崎","46":"金沢","47":"笠松","48":"名古屋",
    "50":"園田","51":"姫路","54":"高知","55":"佐賀","65":"帯広",
}

# SMILE距離分類
def smile_zone(dist):
    if dist <= 1000: return "SS"
    if dist <= 1400: return "S"
    if dist <= 1800: return "M"
    if dist <= 2200: return "I"
    if dist <= 2600: return "L"
    return "E"


def load_all_data():
    """全予想+結果を読み込み、race_idでマッチング"""
    # 結果データを全件ロード
    results_map = {}  # race_id -> {horse_no: finish_pos, payouts: {...}}
    result_files = sorted(RESULT_DIR.glob("*_results.json"))
    for f in result_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for race_id, rdata in data.items():
                finish_map = {}
                for o in rdata.get("order", []):
                    finish_map[o["horse_no"]] = o.get("finish", 99)
                results_map[race_id] = {
                    "finish": finish_map,
                    "payouts": rdata.get("payouts", {}),
                }
        except Exception:
            continue
    print(f"結果データ: {len(results_map)}レース")

    # 予想データを全件ロード + 結果マッチング
    races = []  # [{race_data, horses: [{horse_data + finish_pos}]}]
    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    matched = 0
    for f in pred_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for race in data.get("races", []):
                rid = race.get("race_id", "")
                if rid not in results_map:
                    continue
                res = results_map[rid]
                horses = []
                for h in race.get("horses", []):
                    hno = h.get("horse_no")
                    h["finish_pos"] = res["finish"].get(hno, 99)
                    horses.append(h)
                race["_horses"] = horses
                race["_payouts"] = res["payouts"]
                races.append(race)
                matched += 1
        except Exception:
            continue
    print(f"照合済みレース: {matched}")
    return races


def _venue_code(race):
    vc = race.get("venue_code", "")
    if not vc:
        rid = race.get("race_id", "")
        vc = rid[4:6] if len(rid) >= 6 else ""
    return vc


def test1_venue_weights(races):
    """Test 1: 競馬場別重みの異常パターン → 競馬場別◎的中率"""
    print("\n" + "="*70)
    print("Test 1: 競馬場別◎的中率（重み異常の影響）")
    print("="*70)

    from config.settings import get_composite_weights
    venue_stats = defaultdict(lambda: {"total": 0, "win": 0, "place3": 0})

    for race in races:
        vc = _venue_code(race)
        for h in race["_horses"]:
            if h.get("mark") in ("◎", "◉"):
                fp = h["finish_pos"]
                venue_stats[vc]["total"] += 1
                if fp == 1: venue_stats[vc]["win"] += 1
                if fp <= 3: venue_stats[vc]["place3"] += 1

    # 重みデータ
    print(f"\n{'場':>4} {'◎数':>5} {'勝率':>6} {'複勝率':>6} | {'abi':>5} {'pace':>5} {'trn':>5} {'異常':>6}")
    print("-" * 70)
    for vc in sorted(venue_stats.keys()):
        s = venue_stats[vc]
        if s["total"] == 0: continue
        wr = s["win"] / s["total"] * 100
        pr = s["place3"] / s["total"] * 100
        name = VENUE_NAMES.get(vc, vc)
        w = get_composite_weights(name)
        abi = w.get("ability", 0)
        pace = w.get("pace", 0)
        trn = w.get("trainer", 0)
        anomaly = ""
        if pace > abi: anomaly += "pace>abi "
        if trn > 0.12: anomaly += "trn高 "
        print(f"{name:>4} {s['total']:>5} {wr:>5.1f}% {pr:>5.1f}% | {abi:>.3f} {pace:>.3f} {trn:>.3f} {anomaly}")


def test2_jockey_change(races):
    """Test 2: テン乗りペナルティの影響"""
    print("\n" + "="*70)
    print("Test 2: 乗り替わり vs 継続の◎成績")
    print("="*70)

    stats = {"change": {"total":0,"win":0,"p3":0}, "same": {"total":0,"win":0,"p3":0}}
    for race in races:
        for h in race["_horses"]:
            if h.get("mark") not in ("◎", "◉"): continue
            jcs = h.get("jockey_change_score")
            key = "change" if (jcs is not None and jcs != 0) else "same"
            stats[key]["total"] += 1
            if h["finish_pos"] == 1: stats[key]["win"] += 1
            if h["finish_pos"] <= 3: stats[key]["p3"] += 1

    for k, s in stats.items():
        if s["total"] == 0: continue
        label = "乗り替わり" if k == "change" else "継続騎乗"
        print(f"  {label}: {s['total']}頭 勝率{s['win']/s['total']*100:.1f}% 複勝率{s['p3']/s['total']*100:.1f}%")


def test3_model_level(races):
    """Test 3: model_level別の精度"""
    print("\n" + "="*70)
    print("Test 3: model_level別 ◎的中率")
    print("="*70)

    level_stats = defaultdict(lambda: {"total":0,"win":0,"p3":0})
    level_dist = defaultdict(int)

    for race in races:
        for h in race["_horses"]:
            ml = h.get("model_level")
            if ml is not None:
                level_dist[ml] += 1
            if h.get("mark") not in ("◎", "◉"): continue
            if ml is None: ml = -1
            level_stats[ml]["total"] += 1
            if h["finish_pos"] == 1: level_stats[ml]["win"] += 1
            if h["finish_pos"] <= 3: level_stats[ml]["p3"] += 1

    print("\nmodel_level分布:")
    for lv in sorted(level_dist.keys()):
        print(f"  Level {lv}: {level_dist[lv]:>7}頭")

    print(f"\n{'Level':>6} {'◎数':>5} {'勝率':>6} {'複勝率':>6}")
    print("-" * 30)
    for lv in sorted(level_stats.keys()):
        s = level_stats[lv]
        if s["total"] == 0: continue
        print(f"{lv:>6} {s['total']:>5} {s['win']/s['total']*100:>5.1f}% {s['p3']/s['total']*100:>5.1f}%")


def test4_field_size(races):
    """Test 4: 少頭数レースの精度"""
    print("\n" + "="*70)
    print("Test 4: 頭数別◎的中率")
    print("="*70)

    bins = {"3-5頭": (3,5), "6-8頭": (6,8), "9-12頭": (9,12), "13-18頭": (13,18)}
    stats = {k: {"total":0,"win":0,"p3":0,"races":0} for k in bins}

    for race in races:
        fc = race.get("field_count") or len(race["_horses"])
        for label, (lo, hi) in bins.items():
            if lo <= fc <= hi:
                stats[label]["races"] += 1
                for h in race["_horses"]:
                    if h.get("mark") in ("◎", "◉"):
                        stats[label]["total"] += 1
                        if h["finish_pos"] == 1: stats[label]["win"] += 1
                        if h["finish_pos"] <= 3: stats[label]["p3"] += 1
                break

    print(f"\n{'区分':>8} {'レース':>6} {'◎数':>5} {'勝率':>6} {'複勝率':>6}")
    print("-" * 40)
    for label, s in stats.items():
        if s["total"] == 0: continue
        print(f"{label:>8} {s['races']:>6} {s['total']:>5} {s['win']/s['total']*100:>5.1f}% {s['p3']/s['total']*100:>5.1f}%")


def test5_last3f_missing(races):
    """Test 5: 上がり3F欠損の影響"""
    print("\n" + "="*70)
    print("Test 5: 上がり3F欠損率と精度差（JRA vs NAR）")
    print("="*70)

    cats = {"JRA_有": {"total":0,"p3":0}, "JRA_欠": {"total":0,"p3":0},
            "NAR_有": {"total":0,"p3":0}, "NAR_欠": {"total":0,"p3":0}}

    for race in races:
        vc = _venue_code(race)
        is_jra = vc in JRA_VENUES
        for h in race["_horses"]:
            if h.get("mark") not in ("◎", "◉"): continue
            l3f = h.get("pace_estimated_last3f")
            missing = l3f is None or l3f == 0
            prefix = "JRA" if is_jra else "NAR"
            key = f"{prefix}_{'欠' if missing else '有'}"
            cats[key]["total"] += 1
            if h["finish_pos"] <= 3: cats[key]["p3"] += 1

    for k, s in cats.items():
        if s["total"] == 0: continue
        print(f"  {k}: {s['total']}頭 複勝率{s['p3']/s['total']*100:.1f}%")


def test6_mark_performance(races):
    """Test 6: 印別成績（KPI対比）"""
    print("\n" + "="*70)
    print("Test 6: 印別成績 vs KPI目標")
    print("="*70)

    kpi = {
        "◉": {"win":75,"p2":85,"p3":95}, "◎": {"win":50,"p2":60,"p3":70},
        "○": {"win":35,"p2":45,"p3":55}, "▲": {"win":25,"p2":35,"p3":45},
        "△": {"win":15,"p2":25,"p3":35}, "★": {"win":5,"p2":15,"p3":25},
    }
    stats = defaultdict(lambda: {"total":0,"win":0,"p2":0,"p3":0,"tansho_ret":0})

    for race in races:
        for h in race["_horses"]:
            mk = h.get("mark", "")
            if not mk or mk in ("-", "—"): continue
            fp = h["finish_pos"]
            stats[mk]["total"] += 1
            if fp == 1:
                stats[mk]["win"] += 1
                # 単勝払戻
                payouts = race.get("_payouts", {})
                tansho = payouts.get("単勝", {})
                if isinstance(tansho, dict):
                    stats[mk]["tansho_ret"] += tansho.get("payout", 0)
                elif isinstance(tansho, list):
                    for t in tansho:
                        if isinstance(t, dict):
                            stats[mk]["tansho_ret"] += t.get("payout", 0)
            if fp <= 2: stats[mk]["p2"] += 1
            if fp <= 3: stats[mk]["p3"] += 1

    marks_order = ["◉", "◎", "○", "▲", "△", "★", "☆", "×"]
    print(f"\n{'印':>2} {'数':>6} {'勝率':>6} {'連対':>6} {'複勝':>6} | {'目標勝':>6} {'目標複':>6} {'差分':>6}")
    print("-" * 65)
    for mk in marks_order:
        s = stats[mk]
        if s["total"] == 0: continue
        wr = s["win"] / s["total"] * 100
        p2r = s["p2"] / s["total"] * 100
        p3r = s["p3"] / s["total"] * 100
        k = kpi.get(mk, {})
        kw = k.get("win", 0)
        kp = k.get("p3", 0)
        diff = p3r - kp if kp else 0
        print(f"{mk:>2} {s['total']:>6} {wr:>5.1f}% {p2r:>5.1f}% {p3r:>5.1f}% | {kw:>5.1f}% {kp:>5.1f}% {diff:>+5.1f}%")

    # ◎(◉含む)の単勝ROI
    honmei = {"total": stats["◎"]["total"] + stats["◉"]["total"],
              "ret": stats["◎"]["tansho_ret"] + stats["◉"]["tansho_ret"]}
    if honmei["total"]:
        roi = honmei["ret"] / (honmei["total"] * 100) * 100
        print(f"\n◎(◉含む)単勝100円ROI: {roi:.1f}%")


def test7_confidence(races):
    """Test 7: 自信度別成績"""
    print("\n" + "="*70)
    print("Test 7: 自信度別成績")
    print("="*70)

    stats = defaultdict(lambda: {"races":0,"honmei_total":0,"honmei_win":0,"honmei_p3":0})
    for race in races:
        conf = race.get("confidence", "")
        if not conf: continue
        stats[conf]["races"] += 1
        for h in race["_horses"]:
            if h.get("mark") in ("◎", "◉"):
                stats[conf]["honmei_total"] += 1
                if h["finish_pos"] == 1: stats[conf]["honmei_win"] += 1
                if h["finish_pos"] <= 3: stats[conf]["honmei_p3"] += 1

    conf_order = ["SS", "S", "A", "B", "C", "D", "E"]
    print(f"\n{'自信度':>4} {'レース':>6} {'◎数':>5} {'勝率':>6} {'複勝率':>6}")
    print("-" * 35)
    for c in conf_order:
        s = stats[c]
        if s["honmei_total"] == 0: continue
        wr = s["honmei_win"] / s["honmei_total"] * 100
        pr = s["honmei_p3"] / s["honmei_total"] * 100
        print(f"{c:>4} {s['races']:>6} {s['honmei_total']:>5} {wr:>5.1f}% {pr:>5.1f}%")


def test8_ana_kiken(races):
    """Test 8: 穴馬・危険馬の判定精度"""
    print("\n" + "="*70)
    print("Test 8: 穴馬・危険馬の判定精度")
    print("="*70)

    ana = {"total":0,"win":0,"p3":0}
    kiken = {"total":0,"fell":0}
    ana_by_type = defaultdict(lambda: {"total":0,"win":0,"p3":0})

    for race in races:
        for h in race["_horses"]:
            # 穴馬
            if h.get("is_tokusen"):
                ana["total"] += 1
                if h["finish_pos"] == 1: ana["win"] += 1
                if h["finish_pos"] <= 3: ana["p3"] += 1
            at = h.get("ana_type", "")
            if at and at not in ("該当なし", ""):
                ana_by_type[at]["total"] += 1
                if h["finish_pos"] == 1: ana_by_type[at]["win"] += 1
                if h["finish_pos"] <= 3: ana_by_type[at]["p3"] += 1
            # 危険馬
            if h.get("is_tokusen_kiken"):
                kiken["total"] += 1
                if h["finish_pos"] >= 4: kiken["fell"] += 1

    if ana["total"]:
        print(f"  ☆特選穴: {ana['total']}頭 勝率{ana['win']/ana['total']*100:.1f}% 複勝率{ana['p3']/ana['total']*100:.1f}%")
    if kiken["total"]:
        print(f"  ×危険馬: {kiken['total']}頭 4着以下率{kiken['fell']/kiken['total']*100:.1f}%")

    print("\n  穴馬タイプ別:")
    for at in sorted(ana_by_type.keys()):
        s = ana_by_type[at]
        if s["total"] == 0: continue
        print(f"    {at}: {s['total']}頭 勝率{s['win']/s['total']*100:.1f}% 複勝率{s['p3']/s['total']*100:.1f}%")


def test9_jra_nar(races):
    """Test 9: JRA vs NAR 精度差"""
    print("\n" + "="*70)
    print("Test 9: JRA vs NAR 精度差")
    print("="*70)

    cat_stats = defaultdict(lambda: {"total":0,"win":0,"p3":0})
    venue_stats = defaultdict(lambda: {"total":0,"win":0,"p3":0})
    smile_stats = defaultdict(lambda: {"total":0,"win":0,"p3":0})

    for race in races:
        vc = _venue_code(race)
        is_jra = vc in JRA_VENUES
        cat = "JRA" if is_jra else "NAR"
        dist = race.get("distance", 1600)
        sz = smile_zone(dist)

        for h in race["_horses"]:
            if h.get("mark") not in ("◎", "◉"): continue
            fp = h["finish_pos"]
            for bucket in [cat_stats[cat], venue_stats[vc], smile_stats[f"{cat}_{sz}"]]:
                bucket["total"] += 1
                if fp == 1: bucket["win"] += 1
                if fp <= 3: bucket["p3"] += 1

    print("\nJRA vs NAR:")
    for cat in ["JRA", "NAR"]:
        s = cat_stats[cat]
        if s["total"] == 0: continue
        print(f"  {cat}: {s['total']}頭 勝率{s['win']/s['total']*100:.1f}% 複勝率{s['p3']/s['total']*100:.1f}%")

    print("\n競馬場別◎複勝率（ワースト5）:")
    sorted_venues = sorted(venue_stats.items(), key=lambda x: x[1]["p3"]/max(x[1]["total"],1))
    for vc, s in sorted_venues[:5]:
        if s["total"] < 10: continue
        name = VENUE_NAMES.get(vc, vc)
        print(f"  {name}: {s['total']}頭 複勝率{s['p3']/s['total']*100:.1f}%")

    print("\nSMILE距離帯別:")
    for key in sorted(smile_stats.keys()):
        s = smile_stats[key]
        if s["total"] < 10: continue
        print(f"  {key}: {s['total']}頭 勝率{s['win']/s['total']*100:.1f}% 複勝率{s['p3']/s['total']*100:.1f}%")


def test10_composite_power(races):
    """Test 10: composite偏差値の予測力"""
    print("\n" + "="*70)
    print("Test 10: composite偏差値の予測力")
    print("="*70)

    top1_finishes = []
    top3_in_top3 = 0
    total_races = 0

    for race in races:
        horses = race["_horses"]
        if len(horses) < 3: continue
        # compositeでソート
        ranked = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
        # 1位馬の着順
        top1_finishes.append(ranked[0]["finish_pos"])
        # 上位3頭が3着以内に全員入ったか
        top3_fps = [ranked[i]["finish_pos"] for i in range(min(3, len(ranked)))]
        if all(fp <= 3 for fp in top3_fps):
            top3_in_top3 += 1
        total_races += 1

    if top1_finishes:
        win = sum(1 for f in top1_finishes if f == 1)
        p3 = sum(1 for f in top1_finishes if f <= 3)
        print(f"  composite 1位馬: {len(top1_finishes)}レース")
        print(f"    勝率: {win/len(top1_finishes)*100:.1f}%")
        print(f"    複勝率: {p3/len(top1_finishes)*100:.1f}%")
        # 着順分布
        from collections import Counter
        dist = Counter(min(f, 10) for f in top1_finishes)
        print(f"    着順分布: ", end="")
        for pos in range(1, 11):
            cnt = dist.get(pos, 0)
            pct = cnt / len(top1_finishes) * 100
            label = f"{pos}着" if pos < 10 else "10着~"
            print(f"{label}:{pct:.0f}% ", end="")
        print()

    if total_races:
        print(f"\n  上位3頭が全て3着以内: {top3_in_top3}/{total_races} ({top3_in_top3/total_races*100:.1f}%)")


def main():
    print("=" * 70)
    print("D-AI Keiba v3 — 全仮説テスト検証レポート")
    print("=" * 70)

    races = load_all_data()
    if not races:
        print("データなし")
        return

    test1_venue_weights(races)
    test2_jockey_change(races)
    test3_model_level(races)
    test4_field_size(races)
    test5_last3f_missing(races)
    test6_mark_performance(races)
    test7_confidence(races)
    test8_ana_kiken(races)
    test9_jra_nar(races)
    test10_composite_power(races)

    print("\n" + "=" * 70)
    print("検証完了")
    print("=" * 70)


if __name__ == "__main__":
    main()
