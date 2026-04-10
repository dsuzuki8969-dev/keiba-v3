"""
全期間EV・収益性徹底分析スクリプト

predictions(40,464レース) × race_results(38,816レース) をJOINし、
2024-01〜2026-04の全データを使って多角的にEV・収益性を分析する。

分析項目:
1. 基本統計（期間・レース数・データカバー率）
2. 印別成績（勝率・連対率・複勝率・回収率）月次推移
3. EV帯別成績（全印/◉◎限定）
4. win_probキャリブレーション精度（全期間）
5. オッズ帯×win_prob帯クロス分析
6. 自信度別成績（月次推移含む）
7. JRA vs NAR比較（全次元）
8. 馬場・距離帯・人気帯別のEV効果
9. ◉判定条件の最適閾値探索（gap/win_prob/place3_prob/EV）
10. 自信度重み最適化シミュレーション
11. 月次・四半期トレンド
"""

import sqlite3
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

# UTF-8出力
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "keiba.db")

def load_all_data():
    """predictions × race_log を結合して全馬データを構築

    race_logにはfinish_pos + tansho_oddsがあるので、
    回収率 = 的中時 odds×100 / 投資100 で計算可能。
    複勝はオッズがないので finish_pos<=3 かどうかのみ。
    """
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # race_log: (race_id, horse_no) → {finish_pos, tansho_odds, field_count}
    print("race_log読込中...")
    cur.execute("""
        SELECT race_id, horse_no, finish_pos, tansho_odds, field_count
        FROM race_log
        WHERE finish_pos IS NOT NULL AND finish_pos > 0
    """)
    result_map = {}  # (race_id, horse_no) → {finish, tansho_odds, field_count}
    for row in cur:
        key = (row["race_id"], row["horse_no"])
        result_map[key] = {
            "finish": row["finish_pos"],
            "tansho_odds": row["tansho_odds"] or 0,
            "rl_field_count": row["field_count"] or 0,
        }
    print(f"  race_log: {len(result_map)}件")

    # predictions: 全レースの予想データ
    print("predictions読込中...")
    cur.execute("""
        SELECT date, race_id, venue, race_no, surface, distance, grade,
               confidence, field_count, horses_json
        FROM predictions
        ORDER BY date, race_id
    """)

    all_records = []
    race_count = 0
    matched_races = set()

    for row in cur:
        race_id = row["race_id"]
        race_date = row["date"]
        race_count += 1

        try:
            horses = json.loads(row["horses_json"]) if row["horses_json"] else []
        except:
            continue

        # is_jra判定
        is_jra = race_id[4:6] in ["01","02","03","04","05","06","07","08","09","10"]
        field_count = row["field_count"] or len(horses)

        for h in horses:
            horse_no = h.get("horse_no")
            if horse_no is None:
                continue

            key = (race_id, horse_no)
            if key not in result_map:
                continue  # 結果不明

            rl = result_map[key]
            finish = rl["finish"]
            tansho_odds = rl["tansho_odds"]
            matched_races.add(race_id)

            mark = h.get("mark", "")
            win_prob = h.get("win_prob") or 0
            place3_prob = h.get("place3_prob") or 0
            odds = h.get("odds")
            predicted_odds = h.get("predicted_tansho_odds")
            composite = h.get("composite") or 0
            popularity = h.get("popularity")
            ml_win_prob = h.get("ml_win_prob")
            odds_divergence = h.get("odds_divergence")

            # EV計算（予想時のオッズ × win_prob）
            ev = win_prob * odds if odds and odds > 0 and win_prob > 0 else None

            # 単勝払戻: 的中時は tansho_odds × 100（100円賭け前提）
            tansho_payout = int(tansho_odds * 100) if finish == 1 and tansho_odds > 0 else 0

            # value_ratio = win_prob / implied_prob
            implied_prob = 1.0 / odds if odds and odds > 0 else None
            value_ratio = win_prob / implied_prob if implied_prob and implied_prob > 0 else None

            rec = {
                "date": race_date,
                "month": race_date[:7],  # YYYY-MM
                "quarter": f"{race_date[:4]}Q{(int(race_date[5:7])-1)//3+1}",
                "race_id": race_id,
                "venue_code": race_id[4:6],
                "is_jra": is_jra,
                "surface": row["surface"] or "",
                "distance": row["distance"] or 0,
                "grade": row["grade"] or "",
                "field_count": field_count,
                "confidence": row["confidence"] or "",
                "horse_no": horse_no,
                "mark": mark,
                "composite": composite,
                "win_prob": win_prob,
                "place3_prob": place3_prob,
                "odds": odds,
                "tansho_odds": tansho_odds,
                "predicted_odds": predicted_odds,
                "popularity": popularity,
                "ml_win_prob": ml_win_prob,
                "odds_divergence": odds_divergence,
                "ev": ev,
                "value_ratio": value_ratio,
                "finish": finish,
                "is_win": finish == 1,
                "is_place2": finish <= 2,
                "is_place3": finish <= 3,
                "tansho_payout": tansho_payout,
            }
            all_records.append(rec)

    db.close()
    print(f"  predictions: {race_count}レース, うち結果付き: {len(matched_races)}レース")
    print(f"  全馬レコード: {len(all_records)}件")
    return all_records


def calc_stats(records):
    """勝率・連対率・複勝率・単勝回収率を計算"""
    n = len(records)
    if n == 0:
        return {"n": 0, "win_rate": 0, "place2_rate": 0, "place3_rate": 0,
                "tansho_roi": 0}
    wins = sum(1 for r in records if r["is_win"])
    place2 = sum(1 for r in records if r["is_place2"])
    place3 = sum(1 for r in records if r["is_place3"])
    tansho_total = sum(r["tansho_payout"] for r in records)
    return {
        "n": n,
        "win_rate": wins / n * 100,
        "place2_rate": place2 / n * 100,
        "place3_rate": place3 / n * 100,
        "tansho_roi": tansho_total / (n * 100) * 100,  # 100円賭け前提
    }


def print_stats_table(title, groups, sort_key=None):
    """グループ別統計テーブルを出力"""
    print(f"\n{'='*80}")
    print(f" {title}")
    print(f"{'='*80}")
    print(f"{'カテゴリ':<20} {'件数':>6} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単回収':>7}")
    print(f"{'-'*20} {'-'*6} {'-'*7} {'-'*7} {'-'*7} {'-'*7}")

    items = list(groups.items())
    if sort_key:
        items.sort(key=sort_key)

    for label, records in items:
        s = calc_stats(records)
        if s["n"] == 0:
            continue
        print(f"{label:<20} {s['n']:>6} {s['win_rate']:>6.1f}% {s['place2_rate']:>6.1f}% {s['place3_rate']:>6.1f}% {s['tansho_roi']:>6.1f}%")


def analyze_all(records):
    """全分析実行"""

    # ============================================================
    # 1. 基本統計
    # ============================================================
    print("\n" + "=" * 80)
    print(" 1. 基本統計")
    print("=" * 80)
    dates = sorted(set(r["date"] for r in records))
    races = set(r["race_id"] for r in records)
    jra = [r for r in records if r["is_jra"]]
    nar = [r for r in records if not r["is_jra"]]
    print(f"期間: {dates[0]} 〜 {dates[-1]} ({len(dates)}日)")
    print(f"レース数: {len(races)} (JRA: {len(set(r['race_id'] for r in jra))}, NAR: {len(set(r['race_id'] for r in nar))})")
    print(f"馬レコード: {len(records)} (JRA: {len(jra)}, NAR: {len(nar)})")

    # ============================================================
    # 2. 印別成績（全期間）
    # ============================================================
    mark_groups = defaultdict(list)
    for r in records:
        m = r["mark"]
        if m:
            mark_groups[m].append(r)

    # 印の順序
    mark_order = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "★": 5, "☆": 6, "×": 7}
    print_stats_table("2. 印別成績（全期間）", mark_groups,
                      sort_key=lambda x: mark_order.get(x[0], 99))

    # ◎+◉合算
    honmei_all = [r for r in records if r["mark"] in ("◉", "◎")]
    if honmei_all:
        s = calc_stats(honmei_all)
        print(f"{'◎+◉合算':<20} {s['n']:>6} {s['win_rate']:>6.1f}% {s['place2_rate']:>6.1f}% {s['place3_rate']:>6.1f}% {s['tansho_roi']:>6.1f}%")

    # ============================================================
    # 2b. 印別成績（JRA/NAR分離）
    # ============================================================
    for label, subset in [("JRA", jra), ("NAR", nar)]:
        mg = defaultdict(list)
        for r in subset:
            m = r["mark"]
            if m:
                mg[m].append(r)
        print_stats_table(f"2b. 印別成績（{label}）", mg,
                          sort_key=lambda x: mark_order.get(x[0], 99))

    # ============================================================
    # 3. EV帯別成績（全馬）
    # ============================================================
    ev_groups = defaultdict(list)
    for r in records:
        if r["ev"] is None:
            ev_groups["EV不明"].append(r)
        elif r["ev"] < 0.3:
            ev_groups["EV<0.3"].append(r)
        elif r["ev"] < 0.5:
            ev_groups["0.3≤EV<0.5"].append(r)
        elif r["ev"] < 0.7:
            ev_groups["0.5≤EV<0.7"].append(r)
        elif r["ev"] < 0.85:
            ev_groups["0.7≤EV<0.85"].append(r)
        elif r["ev"] < 1.0:
            ev_groups["0.85≤EV<1.0"].append(r)
        elif r["ev"] < 1.2:
            ev_groups["1.0≤EV<1.2"].append(r)
        elif r["ev"] < 1.5:
            ev_groups["1.2≤EV<1.5"].append(r)
        elif r["ev"] < 2.0:
            ev_groups["1.5≤EV<2.0"].append(r)
        else:
            ev_groups["EV≥2.0"].append(r)

    ev_order = ["EV<0.3", "0.3≤EV<0.5", "0.5≤EV<0.7", "0.7≤EV<0.85",
                "0.85≤EV<1.0", "1.0≤EV<1.2", "1.2≤EV<1.5", "1.5≤EV<2.0", "EV≥2.0", "EV不明"]
    sorted_ev_groups = {k: ev_groups[k] for k in ev_order if k in ev_groups}
    print_stats_table("3. EV帯別成績（全馬）", sorted_ev_groups, sort_key=lambda x: ev_order.index(x[0]) if x[0] in ev_order else 99)

    # ============================================================
    # 3b. EV帯別成績（◎/◉限定）
    # ============================================================
    honmei_ev = defaultdict(list)
    for r in honmei_all:
        if r["ev"] is None:
            honmei_ev["EV不明"].append(r)
        elif r["ev"] < 0.5:
            honmei_ev["EV<0.5"].append(r)
        elif r["ev"] < 0.7:
            honmei_ev["0.5≤EV<0.7"].append(r)
        elif r["ev"] < 0.85:
            honmei_ev["0.7≤EV<0.85"].append(r)
        elif r["ev"] < 1.0:
            honmei_ev["0.85≤EV<1.0"].append(r)
        elif r["ev"] < 1.2:
            honmei_ev["1.0≤EV<1.2"].append(r)
        elif r["ev"] < 1.5:
            honmei_ev["1.2≤EV<1.5"].append(r)
        elif r["ev"] < 2.0:
            honmei_ev["1.5≤EV<2.0"].append(r)
        else:
            honmei_ev["EV≥2.0"].append(r)

    honmei_ev_order = ["EV<0.5", "0.5≤EV<0.7", "0.7≤EV<0.85", "0.85≤EV<1.0",
                       "1.0≤EV<1.2", "1.2≤EV<1.5", "1.5≤EV<2.0", "EV≥2.0", "EV不明"]
    sorted_hev = {k: honmei_ev[k] for k in honmei_ev_order if k in honmei_ev}
    print_stats_table("3b. EV帯別成績（◎/◉限定）", sorted_hev,
                      sort_key=lambda x: honmei_ev_order.index(x[0]) if x[0] in honmei_ev_order else 99)

    # ============================================================
    # 3c. EV帯別（JRA/NAR × ◎/◉）
    # ============================================================
    for label, is_jra_val in [("JRA", True), ("NAR", False)]:
        subset = [r for r in honmei_all if r["is_jra"] == is_jra_val]
        eg = defaultdict(list)
        for r in subset:
            if r["ev"] is None:
                eg["EV不明"].append(r)
            elif r["ev"] < 0.7:
                eg["EV<0.7"].append(r)
            elif r["ev"] < 1.0:
                eg["0.7≤EV<1.0"].append(r)
            elif r["ev"] < 1.5:
                eg["1.0≤EV<1.5"].append(r)
            elif r["ev"] < 2.0:
                eg["1.5≤EV<2.0"].append(r)
            else:
                eg["EV≥2.0"].append(r)
        eo = ["EV<0.7", "0.7≤EV<1.0", "1.0≤EV<1.5", "1.5≤EV<2.0", "EV≥2.0", "EV不明"]
        seg = {k: eg[k] for k in eo if k in eg}
        print_stats_table(f"3c. EV帯別（{label}・◎/◉限定）", seg,
                          sort_key=lambda x: eo.index(x[0]) if x[0] in eo else 99)

    # ============================================================
    # 4. win_probキャリブレーション（全期間）
    # ============================================================
    print(f"\n{'='*80}")
    print(f" 4. win_probキャリブレーション（全期間）")
    print(f"{'='*80}")
    wp_bins = [(0, 0.05), (0.05, 0.10), (0.10, 0.15), (0.15, 0.20),
               (0.20, 0.25), (0.25, 0.30), (0.30, 0.40), (0.40, 0.50), (0.50, 1.0)]
    print(f"{'win_prob帯':<16} {'件数':>7} {'予測平均':>8} {'実勝率':>8} {'差':>8}")
    print(f"{'-'*16} {'-'*7} {'-'*8} {'-'*8} {'-'*8}")
    for lo, hi in wp_bins:
        subset = [r for r in records if lo <= r["win_prob"] < hi]
        if not subset:
            continue
        avg_wp = sum(r["win_prob"] for r in subset) / len(subset)
        actual_wr = sum(1 for r in subset if r["is_win"]) / len(subset)
        diff = actual_wr - avg_wp
        print(f"{lo:.2f}-{hi:.2f}       {len(subset):>7} {avg_wp:>7.3f}  {actual_wr:>7.3f}  {diff:>+7.3f}")

    # JRA/NAR別
    for label, is_jra_val in [("JRA", True), ("NAR", False)]:
        print(f"\n--- {label} ---")
        print(f"{'win_prob帯':<16} {'件数':>7} {'予測平均':>8} {'実勝率':>8} {'差':>8}")
        print(f"{'-'*16} {'-'*7} {'-'*8} {'-'*8} {'-'*8}")
        for lo, hi in wp_bins:
            subset = [r for r in records if lo <= r["win_prob"] < hi and r["is_jra"] == is_jra_val]
            if not subset:
                continue
            avg_wp = sum(r["win_prob"] for r in subset) / len(subset)
            actual_wr = sum(1 for r in subset if r["is_win"]) / len(subset)
            diff = actual_wr - avg_wp
            print(f"{lo:.2f}-{hi:.2f}       {len(subset):>7} {avg_wp:>7.3f}  {actual_wr:>7.3f}  {diff:>+7.3f}")

    # ============================================================
    # 5. 人気帯×EV クロス分析（◎/◉限定）
    # ============================================================
    pop_ev_groups = defaultdict(list)
    for r in honmei_all:
        pop = r["popularity"]
        if pop is None:
            pop_label = "不明"
        elif pop <= 1:
            pop_label = "1番人気"
        elif pop <= 2:
            pop_label = "2番人気"
        elif pop <= 3:
            pop_label = "3番人気"
        elif pop <= 5:
            pop_label = "4-5番人気"
        else:
            pop_label = "6番以下"

        ev = r["ev"]
        if ev is None:
            ev_label = "不明"
        elif ev < 1.0:
            ev_label = "EV<1.0"
        elif ev < 1.5:
            ev_label = "1.0≤EV<1.5"
        else:
            ev_label = "EV≥1.5"

        pop_ev_groups[f"{pop_label}×{ev_label}"].append(r)

    print_stats_table("5. 人気帯×EV クロス分析（◎/◉限定）", pop_ev_groups,
                      sort_key=lambda x: x[0])

    # ============================================================
    # 6. 自信度別成績（全期間）
    # ============================================================
    conf_groups = defaultdict(list)
    for r in records:
        c = r["confidence"]
        if c and r["mark"] in ("◉", "◎"):  # 自信度は◎/◉のみ意味がある
            conf_groups[c].append(r)

    conf_order = {"SS": 0, "S": 1, "A": 2, "B": 3, "C": 4, "D": 5, "E": 6}
    print_stats_table("6. 自信度別成績（◎/◉限定）", conf_groups,
                      sort_key=lambda x: conf_order.get(x[0], 99))

    # 自信度 × EV
    for conf in ["SS", "S", "A"]:
        subset = conf_groups.get(conf, [])
        if not subset:
            continue
        ceg = defaultdict(list)
        for r in subset:
            ev = r["ev"]
            if ev is None:
                ceg["不明"].append(r)
            elif ev < 1.0:
                ceg["EV<1.0"].append(r)
            elif ev < 1.5:
                ceg["1.0-1.5"].append(r)
            else:
                ceg["EV≥1.5"].append(r)
        print_stats_table(f"6b. 自信度{conf} × EV帯", ceg,
                          sort_key=lambda x: x[0])

    # ============================================================
    # 7. 月次推移（◎/◉）
    # ============================================================
    month_groups = defaultdict(list)
    for r in honmei_all:
        month_groups[r["month"]].append(r)
    print_stats_table("7. 月次推移（◎/◉）", month_groups,
                      sort_key=lambda x: x[0])

    # ============================================================
    # 8. 馬場別（◎/◉）
    # ============================================================
    surface_groups = defaultdict(list)
    for r in honmei_all:
        s = r["surface"]
        if s:
            surface_groups[s].append(r)
    print_stats_table("8. 馬場別成績（◎/◉）", surface_groups)

    # ============================================================
    # 9. 距離帯別（◎/◉）
    # ============================================================
    dist_groups = defaultdict(list)
    for r in honmei_all:
        d = r["distance"]
        if d <= 0:
            continue
        if d <= 1200:
            dist_groups["短距離(〜1200m)"].append(r)
        elif d <= 1600:
            dist_groups["マイル(1201-1600m)"].append(r)
        elif d <= 2000:
            dist_groups["中距離(1601-2000m)"].append(r)
        elif d <= 2400:
            dist_groups["クラシック(2001-2400m)"].append(r)
        else:
            dist_groups["長距離(2401m〜)"].append(r)
    print_stats_table("9. 距離帯別成績（◎/◉）", dist_groups)

    # ============================================================
    # 10. ◉判定条件の最適閾値探索
    # ============================================================
    print(f"\n{'='*80}")
    print(f" 10. ◉判定条件の最適閾値探索")
    print(f"{'='*80}")

    # ◉馬のデータ
    tekipan = [r for r in records if r["mark"] == "◉"]
    honmei_only = [r for r in records if r["mark"] == "◎"]

    if tekipan:
        print(f"\n◉ 現状: {len(tekipan)}件")
        s = calc_stats(tekipan)
        print(f"  勝率={s['win_rate']:.1f}% 連対={s['place2_rate']:.1f}% 複勝={s['place3_rate']:.1f}% 単回収={s['tansho_roi']:.1f}%")

    if honmei_only:
        print(f"\n◎ 現状: {len(honmei_only)}件")
        s = calc_stats(honmei_only)
        print(f"  勝率={s['win_rate']:.1f}% 連対={s['place2_rate']:.1f}% 複勝={s['place3_rate']:.1f}% 単回収={s['tansho_roi']:.1f}%")

    # EVフィルタシミュレーション（◎/◉全体に対して）
    print(f"\n--- EVフィルタシミュレーション（◎/◉にEV下限を追加した場合） ---")
    print(f"{'EV下限':<10} {'件数':>6} {'勝率':>7} {'複勝率':>7} {'単回収':>7} {'除外数':>6}")
    print(f"{'-'*10} {'-'*6} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*6}")

    for ev_min in [0.0, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.1, 1.2]:
        subset = [r for r in honmei_all if r["ev"] is not None and r["ev"] >= ev_min]
        excluded = len(honmei_all) - len(subset)
        s = calc_stats(subset)
        print(f"EV≥{ev_min:<5.2f}  {s['n']:>6} {s['win_rate']:>6.1f}% {s['place3_rate']:>6.1f}% {s['tansho_roi']:>6.1f}% {excluded:>6}")

    # JRA/NAR別EVフィルタ
    for label, is_jra_val in [("JRA", True), ("NAR", False)]:
        subset_base = [r for r in honmei_all if r["is_jra"] == is_jra_val]
        print(f"\n--- {label} EVフィルタ ---")
        print(f"{'EV下限':<10} {'件数':>6} {'勝率':>7} {'複勝率':>7} {'単回収':>7} {'除外数':>6}")
        print(f"{'-'*10} {'-'*6} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*6}")
        for ev_min in [0.0, 0.5, 0.7, 0.8, 0.85, 0.9, 1.0, 1.2]:
            subset = [r for r in subset_base if r["ev"] is not None and r["ev"] >= ev_min]
            excluded = len(subset_base) - len(subset)
            s = calc_stats(subset)
            print(f"EV≥{ev_min:<5.2f}  {s['n']:>6} {s['win_rate']:>6.1f}% {s['place3_rate']:>6.1f}% {s['tansho_roi']:>6.1f}% {excluded:>6}")

    # ============================================================
    # 11. ◉のEV条件追加シミュレーション
    # ============================================================
    print(f"\n{'='*80}")
    print(f" 11. ◉のEV条件追加シミュレーション")
    print(f"{'='*80}")
    print(f"現在◉={len(tekipan)}件、◎={len(honmei_only)}件")
    print(f"◉にEV条件を追加した場合、条件不足の◉は◎に降格")
    print()

    # ◉をEV条件でフィルタして◎に降格させるシミュレーション
    for ev_th in [0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0]:
        # ◉のうちEV>=thを通過するもの
        pass_tekipan = [r for r in tekipan if r["ev"] is not None and r["ev"] >= ev_th]
        fail_tekipan = [r for r in tekipan if r["ev"] is None or r["ev"] < ev_th]
        # 新◎ = 旧◎ + 降格◉
        new_honmei = honmei_only + fail_tekipan

        st = calc_stats(pass_tekipan)
        sh = calc_stats(new_honmei)
        print(f"EV≥{ev_th:.2f}: ◉{st['n']}件(勝率{st['win_rate']:.1f}% 単回{st['tansho_roi']:.1f}%) | "
              f"◎{sh['n']}件(勝率{sh['win_rate']:.1f}% 単回{sh['tansho_roi']:.1f}%) | 降格{len(fail_tekipan)}件")

    # ============================================================
    # 12. 自信度weight最適化シミュレーション
    # ============================================================
    print(f"\n{'='*80}")
    print(f" 12. value_score重み変更シミュレーション")
    print(f"{'='*80}")
    print("※ 実際のスコア再計算は不可能（個別信号値がpredには保存されていない）")
    print("※ 代わりにvalue_ratio帯別の自信度分布を確認")

    # value_ratio帯 × 自信度
    vr_conf = defaultdict(lambda: defaultdict(int))
    for r in honmei_all:
        vr = r["value_ratio"]
        conf = r["confidence"]
        if vr is None or not conf:
            continue
        if vr < 0.8:
            vr_label = "VR<0.8"
        elif vr < 1.0:
            vr_label = "0.8≤VR<1.0"
        elif vr < 1.2:
            vr_label = "1.0≤VR<1.2"
        elif vr < 1.5:
            vr_label = "1.2≤VR<1.5"
        else:
            vr_label = "VR≥1.5"
        vr_conf[vr_label][conf] += 1

    print(f"\n{'VR帯':<16}", end="")
    for c in ["SS", "S", "A", "B", "C", "D", "E"]:
        print(f" {c:>5}", end="")
    print()
    for vr_label in ["VR<0.8", "0.8≤VR<1.0", "1.0≤VR<1.2", "1.2≤VR<1.5", "VR≥1.5"]:
        print(f"{vr_label:<16}", end="")
        for c in ["SS", "S", "A", "B", "C", "D", "E"]:
            print(f" {vr_conf[vr_label][c]:>5}", end="")
        print()

    # value_ratio帯別の成績
    vr_groups = defaultdict(list)
    for r in honmei_all:
        vr = r["value_ratio"]
        if vr is None:
            vr_groups["VR不明"].append(r)
        elif vr < 0.8:
            vr_groups["VR<0.8"].append(r)
        elif vr < 1.0:
            vr_groups["0.8≤VR<1.0"].append(r)
        elif vr < 1.2:
            vr_groups["1.0≤VR<1.2"].append(r)
        elif vr < 1.5:
            vr_groups["1.2≤VR<1.5"].append(r)
        else:
            vr_groups["VR≥1.5"].append(r)
    print_stats_table("12b. value_ratio帯別成績（◎/◉）", vr_groups,
                      sort_key=lambda x: x[0])

    # ============================================================
    # 13. ×マーク分析（危険馬の精度検証）
    # ============================================================
    kiken = [r for r in records if r["mark"] == "×"]
    if kiken:
        print(f"\n{'='*80}")
        print(f" 13. ×マーク精度分析")
        print(f"{'='*80}")
        s = calc_stats(kiken)
        print(f"×全体: {s['n']}件 勝率={s['win_rate']:.1f}% 複勝率={s['place3_rate']:.1f}% 単回収={s['tansho_roi']:.1f}%")

        # ×の人気帯別
        kiken_pop = defaultdict(list)
        for r in kiken:
            pop = r["popularity"]
            if pop is None:
                kiken_pop["不明"].append(r)
            elif pop <= 3:
                kiken_pop["1-3番人気"].append(r)
            elif pop <= 6:
                kiken_pop["4-6番人気"].append(r)
            else:
                kiken_pop["7番以下"].append(r)
        print_stats_table("13b. ×マーク人気帯別", kiken_pop)

        # ×のEV帯別
        kiken_ev = defaultdict(list)
        for r in kiken:
            ev = r["ev"]
            if ev is None:
                kiken_ev["不明"].append(r)
            elif ev < 1.0:
                kiken_ev["EV<1.0"].append(r)
            elif ev < 1.5:
                kiken_ev["1.0-1.5"].append(r)
            else:
                kiken_ev["EV≥1.5"].append(r)
        print_stats_table("13c. ×マーク EV帯別", kiken_ev)

    # ============================================================
    # 14. composite gap別の◎/◉成績
    # ============================================================
    print(f"\n{'='*80}")
    print(f" 14. composite gap別成績（◎/◉）")
    print(f"{'='*80}")
    print("※ gapデータはpredに保存されていないため、composite 1位-2位差で代用")

    # race_idごとにcomposite順でgapを計算
    races_data = defaultdict(list)
    for r in records:
        races_data[r["race_id"]].append(r)

    gap_groups = defaultdict(list)
    for race_id, horses in races_data.items():
        sorted_h = sorted(horses, key=lambda h: h["composite"], reverse=True)
        if len(sorted_h) < 2:
            continue
        top = sorted_h[0]
        gap = top["composite"] - sorted_h[1]["composite"]

        if top["mark"] not in ("◉", "◎"):
            continue

        if gap < 2:
            gap_groups["gap<2"].append(top)
        elif gap < 4:
            gap_groups["2≤gap<4"].append(top)
        elif gap < 6:
            gap_groups["4≤gap<6"].append(top)
        elif gap < 8:
            gap_groups["6≤gap<8"].append(top)
        elif gap < 10:
            gap_groups["8≤gap<10"].append(top)
        else:
            gap_groups["gap≥10"].append(top)

    gap_order = ["gap<2", "2≤gap<4", "4≤gap<6", "6≤gap<8", "8≤gap<10", "gap≥10"]
    sorted_gap = {k: gap_groups[k] for k in gap_order if k in gap_groups}
    print_stats_table("14. gap帯別成績（◎/◉）", sorted_gap,
                      sort_key=lambda x: gap_order.index(x[0]) if x[0] in gap_order else 99)

    # gap × EV クロス
    print(f"\n--- gap × EV クロス（◎/◉） ---")
    gap_ev = defaultdict(list)
    for race_id, horses in races_data.items():
        sorted_h = sorted(horses, key=lambda h: h["composite"], reverse=True)
        if len(sorted_h) < 2:
            continue
        top = sorted_h[0]
        gap = top["composite"] - sorted_h[1]["composite"]
        ev = top["ev"]

        if top["mark"] not in ("◉", "◎"):
            continue

        gap_label = "gap<5" if gap < 5 else "gap≥5"
        if ev is None:
            ev_label = "EV不明"
        elif ev < 1.0:
            ev_label = "EV<1.0"
        else:
            ev_label = "EV≥1.0"

        gap_ev[f"{gap_label}×{ev_label}"].append(top)
    print_stats_table("14b. gap×EV クロス（◎/◉）", gap_ev, sort_key=lambda x: x[0])

    # ============================================================
    # 15. 四半期トレンド（◎/◉）
    # ============================================================
    q_groups = defaultdict(list)
    for r in honmei_all:
        q_groups[r["quarter"]].append(r)
    print_stats_table("15. 四半期トレンド（◎/◉）", q_groups,
                      sort_key=lambda x: x[0])

    # ============================================================
    # 16. 会場別成績（◎/◉, 上位20会場）
    # ============================================================
    venue_groups = defaultdict(list)
    for r in honmei_all:
        venue_groups[r["venue_code"]].append(r)

    # 件数上位20
    top_venues = sorted(venue_groups.items(), key=lambda x: len(x[1]), reverse=True)[:20]
    print_stats_table("16. 会場別成績（◎/◉, 上位20）",
                      dict(top_venues),
                      sort_key=lambda x: -len(x[1]))

    # ============================================================
    # 17. 頭数帯別成績（◎/◉）
    # ============================================================
    fc_groups = defaultdict(list)
    for r in honmei_all:
        fc = r["field_count"]
        if fc <= 8:
            fc_groups["〜8頭"].append(r)
        elif fc <= 12:
            fc_groups["9-12頭"].append(r)
        elif fc <= 16:
            fc_groups["13-16頭"].append(r)
        else:
            fc_groups["17頭〜"].append(r)
    print_stats_table("17. 頭数帯別成績（◎/◉）", fc_groups)

    # ============================================================
    # 18. odds_divergence帯別成績
    # ============================================================
    div_groups = defaultdict(list)
    for r in honmei_all:
        d = r["odds_divergence"]
        if d is None:
            div_groups["不明"].append(r)
        elif d < -0.3:
            div_groups["大幅低評価(d<-0.3)"].append(r)
        elif d < -0.1:
            div_groups["低評価(-0.3≤d<-0.1)"].append(r)
        elif d < 0.1:
            div_groups["一致(-0.1≤d<0.1)"].append(r)
        elif d < 0.3:
            div_groups["高評価(0.1≤d<0.3)"].append(r)
        else:
            div_groups["大幅高評価(d≥0.3)"].append(r)
    print_stats_table("18. odds_divergence帯別成績（◎/◉）", div_groups,
                      sort_key=lambda x: x[0])

    # ============================================================
    # 19. SS条件value閾値シミュレーション
    # ============================================================
    print(f"\n{'='*80}")
    print(f" 19. SS条件value_ratio閾値シミュレーション")
    print(f"{'='*80}")
    ss_honmei = [r for r in honmei_all if r["confidence"] == "SS"]
    if ss_honmei:
        print(f"現在SS: {len(ss_honmei)}件")
        s = calc_stats(ss_honmei)
        print(f"  勝率={s['win_rate']:.1f}% 複勝率={s['place3_rate']:.1f}% 単回収={s['tansho_roi']:.1f}%")

        # SSのvalue_ratio分布
        print(f"\nSS馬のvalue_ratio分布:")
        for vr_th in [0.8, 0.9, 1.0, 1.1, 1.15, 1.2, 1.25, 1.3]:
            subset = [r for r in ss_honmei if r["value_ratio"] is not None and r["value_ratio"] >= vr_th]
            s2 = calc_stats(subset)
            excluded = len(ss_honmei) - len(subset)
            print(f"  VR≥{vr_th:.2f}: {s2['n']}件 勝率={s2['win_rate']:.1f}% 単回収={s2['tansho_roi']:.1f}% (除外{excluded}件)")

    # ============================================================
    # 20. 最適買い目シミュレーション
    # ============================================================
    print(f"\n{'='*80}")
    print(f" 20. 最適単勝買いシミュレーション")
    print(f"{'='*80}")

    # 条件組み合わせ
    conditions = [
        ("全◎/◉", lambda r: True),
        ("◉のみ", lambda r: r["mark"] == "◉"),
        ("EV≥0.8", lambda r: r["ev"] is not None and r["ev"] >= 0.8),
        ("EV≥0.9", lambda r: r["ev"] is not None and r["ev"] >= 0.9),
        ("EV≥1.0", lambda r: r["ev"] is not None and r["ev"] >= 1.0),
        ("EV≥1.2", lambda r: r["ev"] is not None and r["ev"] >= 1.2),
        ("SS+EV≥1.0", lambda r: r["confidence"] == "SS" and r["ev"] is not None and r["ev"] >= 1.0),
        ("SS/S+EV≥0.9", lambda r: r["confidence"] in ("SS","S") and r["ev"] is not None and r["ev"] >= 0.9),
        ("◉+EV≥0.8", lambda r: r["mark"] == "◉" and r["ev"] is not None and r["ev"] >= 0.8),
        ("◉+EV≥1.0", lambda r: r["mark"] == "◉" and r["ev"] is not None and r["ev"] >= 1.0),
        ("VR≥1.2", lambda r: r["value_ratio"] is not None and r["value_ratio"] >= 1.2),
        ("VR≥1.5", lambda r: r["value_ratio"] is not None and r["value_ratio"] >= 1.5),
        ("◉+VR≥1.2", lambda r: r["mark"] == "◉" and r["value_ratio"] is not None and r["value_ratio"] >= 1.2),
        ("wp≥25%+EV≥1.0", lambda r: r["win_prob"] >= 0.25 and r["ev"] is not None and r["ev"] >= 1.0),
        ("wp≥30%+EV≥1.0", lambda r: r["win_prob"] >= 0.30 and r["ev"] is not None and r["ev"] >= 1.0),
    ]

    print(f"{'条件':<22} {'件数':>6} {'勝率':>7} {'複勝率':>7} {'単回収':>7}")
    print(f"{'-'*22} {'-'*6} {'-'*7} {'-'*7} {'-'*7} {'-'*7}")
    for label, cond in conditions:
        subset = [r for r in honmei_all if cond(r)]
        s = calc_stats(subset)
        if s["n"] == 0:
            continue
        print(f"{label:<22} {s['n']:>6} {s['win_rate']:>6.1f}% {s['place3_rate']:>6.1f}% {s['tansho_roi']:>6.1f}%")

    # JRA/NAR別
    for jra_label, is_jra_val in [("JRA", True), ("NAR", False)]:
        print(f"\n--- {jra_label} ---")
        base = [r for r in honmei_all if r["is_jra"] == is_jra_val]
        print(f"{'条件':<22} {'件数':>6} {'勝率':>7} {'複勝率':>7} {'単回収':>7}")
        print(f"{'-'*22} {'-'*6} {'-'*7} {'-'*7} {'-'*7} {'-'*7}")
        for label, cond in conditions:
            subset = [r for r in base if cond(r)]
            s = calc_stats(subset)
            if s["n"] == 0:
                continue
            print(f"{label:<22} {s['n']:>6} {s['win_rate']:>6.1f}% {s['place3_rate']:>6.1f}% {s['tansho_roi']:>6.1f}%")

    # ============================================================
    # 21. ☆（穴馬）分析
    # ============================================================
    ana = [r for r in records if r["mark"] == "☆"]
    if ana:
        print(f"\n{'='*80}")
        print(f" 21. ☆（穴馬）分析")
        print(f"{'='*80}")
        s = calc_stats(ana)
        print(f"☆全体: {s['n']}件 勝率={s['win_rate']:.1f}% 複勝率={s['place3_rate']:.1f}% 単回収={s['tansho_roi']:.1f}%")

        ana_ev = defaultdict(list)
        for r in ana:
            ev = r["ev"]
            if ev is None:
                ana_ev["不明"].append(r)
            elif ev < 1.0:
                ana_ev["EV<1.0"].append(r)
            elif ev < 2.0:
                ana_ev["1.0-2.0"].append(r)
            else:
                ana_ev["EV≥2.0"].append(r)
        print_stats_table("21b. ☆ EV帯別", ana_ev)

    # ============================================================
    # 22. 複勝回収率最適化（全印×EV）
    # ============================================================
    print(f"\n{'='*80}")
    print(f" 22. 複勝買い最適条件探索")
    print(f"{'='*80}")
    fukusho_conditions = [
        ("◎/◉全体", lambda r: r["mark"] in ("◉","◎")),
        ("◎/◉+p3≥50%", lambda r: r["mark"] in ("◉","◎") and r["place3_prob"] >= 0.50),
        ("◎/◉+p3≥60%", lambda r: r["mark"] in ("◉","◎") and r["place3_prob"] >= 0.60),
        ("◎/◉+p3≥70%", lambda r: r["mark"] in ("◉","◎") and r["place3_prob"] >= 0.70),
        ("◉+p3≥60%", lambda r: r["mark"] == "◉" and r["place3_prob"] >= 0.60),
        ("○以上+p3≥40%", lambda r: r["mark"] in ("◉","◎","○") and r["place3_prob"] >= 0.40),
        ("SS+p3≥60%", lambda r: r["confidence"] == "SS" and r["place3_prob"] >= 0.60),
        ("odds≥3+p3≥50%", lambda r: (r["odds"] or 0) >= 3 and r["place3_prob"] >= 0.50 and r["mark"] in ("◉","◎")),
    ]

    print(f"{'条件':<22} {'件数':>6} {'複勝率':>7} {'単回収':>7}")
    print(f"{'-'*22} {'-'*6} {'-'*7} {'-'*7}")
    for label, cond in fukusho_conditions:
        subset = [r for r in records if cond(r)]
        s = calc_stats(subset)
        if s["n"] == 0:
            continue
        print(f"{label:<22} {s['n']:>6} {s['place3_rate']:>6.1f}% {s['tansho_roi']:>6.1f}%")

    print(f"\n{'='*80}")
    print(f" 分析完了")
    print(f"{'='*80}")


if __name__ == "__main__":
    records = load_all_data()
    analyze_all(records)
