"""
EV変更の過去データ実証検証スクリプト

変更点:
1. ◉ EV≥0.80 下限フィルタ（◉→◎降格）
2. 自信度value_score重み 15%→20%（gap_norm 15→12%, multi 15→13%）
3. ソフト正規化キャリブレータ（±30%許容）

検証方法:
- predictions × race_log の43万馬レコードを使用
- 旧ロジック vs 新ロジックでシミュレーション
- ◉/◎の勝率・回収率・件数を比較
"""

import sqlite3
import json
import os
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "keiba.db")


def load_all_data():
    """predictions × race_log を結合"""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # race_log
    print("race_log読込中...")
    cur.execute("""
        SELECT race_id, horse_no, finish_pos, tansho_odds, field_count
        FROM race_log
        WHERE finish_pos IS NOT NULL AND finish_pos > 0
    """)
    result_map = {}
    for row in cur:
        key = (row["race_id"], row["horse_no"])
        result_map[key] = {
            "finish": row["finish_pos"],
            "tansho_odds": row["tansho_odds"] or 0,
            "rl_field_count": row["field_count"] or 0,
        }
    print(f"  race_log: {len(result_map):,}件")

    # predictions
    print("predictions読込中...")
    cur.execute("""
        SELECT date, race_id, venue, race_no, surface, distance, grade,
               confidence, field_count, horses_json
        FROM predictions
        ORDER BY date, race_id
    """)

    all_races = []
    for row in cur:
        race_id = row["race_id"]
        try:
            horses = json.loads(row["horses_json"]) if row["horses_json"] else []
        except:
            continue

        is_jra = race_id[4:6] in ["01","02","03","04","05","06","07","08","09","10"]
        field_count = row["field_count"] or len(horses)

        race_horses = []
        for h in horses:
            horse_no = h.get("horse_no")
            if horse_no is None:
                continue
            key = (race_id, horse_no)
            if key not in result_map:
                continue
            rl = result_map[key]

            win_prob = h.get("win_prob") or 0
            odds = h.get("odds")
            ev = win_prob * odds if odds and odds > 0 and win_prob > 0 else None
            tansho_payout = int(rl["tansho_odds"] * 100) if rl["finish"] == 1 and rl["tansho_odds"] > 0 else 0

            race_horses.append({
                "date": row["date"],
                "race_id": race_id,
                "is_jra": is_jra,
                "confidence": row["confidence"] or "",
                "field_count": field_count,
                "horse_no": horse_no,
                "horse_name": h.get("horse_name", ""),
                "mark": h.get("mark", ""),
                "composite": h.get("composite") or 0,
                "win_prob": win_prob,
                "place2_prob": h.get("place2_prob") or 0,
                "place3_prob": h.get("place3_prob") or 0,
                "odds": odds,
                "effective_odds": h.get("effective_odds") or odds,
                "ev": ev,
                "finish": rl["finish"],
                "is_win": rl["finish"] == 1,
                "is_place2": rl["finish"] <= 2,
                "is_place3": rl["finish"] <= 3,
                "tansho_payout": tansho_payout,
                "tansho_odds": rl["tansho_odds"],
                "popularity": h.get("popularity"),
                # 7信号の素材
                "ml_win_prob": h.get("ml_win_prob"),
                "value_ratio": h.get("value_ratio"),
                "odds_divergence": h.get("odds_divergence"),
            })

        if race_horses:
            all_races.append({
                "race_id": race_id,
                "date": row["date"],
                "is_jra": is_jra,
                "confidence": row["confidence"] or "",
                "horses": race_horses,
            })

    db.close()
    print(f"  レース数: {len(all_races):,}")
    total_horses = sum(len(r["horses"]) for r in all_races)
    print(f"  馬レコード: {total_horses:,}件")
    return all_races


def calc_stats(records):
    """勝率・連対率・複勝率・単勝回収率"""
    n = len(records)
    if n == 0:
        return {"n": 0, "win_rate": 0, "place2_rate": 0, "place3_rate": 0, "tansho_roi": 0}
    wins = sum(1 for r in records if r["is_win"])
    place2 = sum(1 for r in records if r["is_place2"])
    place3 = sum(1 for r in records if r["is_place3"])
    tansho_total = sum(r["tansho_payout"] for r in records)
    return {
        "n": n,
        "win_rate": wins / n * 100,
        "place2_rate": place2 / n * 100,
        "place3_rate": place3 / n * 100,
        "tansho_roi": tansho_total / (n * 100) * 100,
    }


def print_stats(label, records):
    s = calc_stats(records)
    print(f"  {label:<25} {s['n']:>6,}件 勝率{s['win_rate']:>5.1f}% 複勝率{s['place3_rate']:>5.1f}% 単回収{s['tansho_roi']:>6.1f}%")


# ============================================================
# 検証1: ◉ EV下限フィルタ
# ============================================================

def verify_tekipan_ev_filter(all_races):
    """◉にEV≥0.80フィルタを適用した場合のbefore/after比較"""
    print("\n" + "=" * 80)
    print(" 検証1: ◉ EV≥0.80 下限フィルタ")
    print("=" * 80)

    # 現在◉の馬を抽出
    tekipan_all = []
    tekipan_ev_ok = []  # EV≥0.80
    tekipan_ev_ng = []  # EV<0.80（降格対象）
    honmei_all = []

    for race in all_races:
        for h in race["horses"]:
            if h["mark"] == "◉":
                tekipan_all.append(h)
                eff_odds = h.get("effective_odds") or h.get("odds")
                ev = (h["win_prob"] or 0) * eff_odds if eff_odds and eff_odds > 0 else 1.0
                if ev >= 0.80:
                    tekipan_ev_ok.append(h)
                else:
                    tekipan_ev_ng.append(h)
            elif h["mark"] == "◎":
                honmei_all.append(h)

    print(f"\n  --- 現行◉ ---")
    print_stats("◉全体(変更前)", tekipan_all)

    print(f"\n  --- EV分割 ---")
    print_stats("◉ EV≥0.80(残留)", tekipan_ev_ok)
    print_stats("◉ EV<0.80(降格)", tekipan_ev_ng)
    print_stats("◎(変更前)", honmei_all)

    # 降格後のシミュレーション
    new_honmei = honmei_all + tekipan_ev_ng
    print(f"\n  --- 変更後 ---")
    print_stats("新◉(EV≥0.80)", tekipan_ev_ok)
    print_stats("新◎(旧◎+降格分)", new_honmei)

    # JRA/NAR分割
    print(f"\n  --- JRA/NAR分割 ---")
    for label, is_jra in [("JRA", True), ("NAR", False)]:
        tek_ok = [h for h in tekipan_ev_ok if h["is_jra"] == is_jra]
        tek_ng = [h for h in tekipan_ev_ng if h["is_jra"] == is_jra]
        tek_all_sub = [h for h in tekipan_all if h["is_jra"] == is_jra]
        print(f"\n  [{label}]")
        print_stats(f"  旧◉", tek_all_sub)
        print_stats(f"  新◉(EV≥0.80)", tek_ok)
        print_stats(f"  降格分(EV<0.80)", tek_ng)

    # EV閾値感度分析
    print(f"\n  --- EV閾値感度分析 ---")
    for threshold in [0.50, 0.60, 0.70, 0.80, 0.90, 1.00, 1.10, 1.20]:
        remaining = []
        for h in tekipan_all:
            eff_odds = h.get("effective_odds") or h.get("odds")
            ev = (h["win_prob"] or 0) * eff_odds if eff_odds and eff_odds > 0 else 1.0
            if ev >= threshold:
                remaining.append(h)
        s = calc_stats(remaining)
        marker = " ◀ 選定" if threshold == 0.80 else ""
        print(f"    EV≥{threshold:.2f}: {s['n']:>5,}件 勝率{s['win_rate']:>5.1f}% 複勝率{s['place3_rate']:>5.1f}% 単回収{s['tansho_roi']:>6.1f}%{marker}")

    # 期間別（半期ごと）
    print(f"\n  --- 半期別◉成績(EV≥0.80適用後) ---")
    periods = defaultdict(list)
    for h in tekipan_all:
        year = h["date"][:4]
        half = "H1" if int(h["date"][5:7]) <= 6 else "H2"
        period = f"{year}{half}"
        eff_odds = h.get("effective_odds") or h.get("odds")
        ev = (h["win_prob"] or 0) * eff_odds if eff_odds and eff_odds > 0 else 1.0
        if ev >= 0.80:
            periods[period].append(h)

    for period in sorted(periods.keys()):
        print_stats(f"  {period}", periods[period])


# ============================================================
# 検証2: 自信度value_score重み変更の効果
# ============================================================

def verify_confidence_weight_change(all_races):
    """value_score重み変更の影響をシミュレーション"""
    print("\n" + "=" * 80)
    print(" 検証2: 自信度 value_score 重み変更効果")
    print("=" * 80)
    print(" ※注: 実際の7信号生値はpred.jsonに保存されていないため、")
    print("   既存の自信度 × EV帯のクロス分析で効果を推定する")

    # 自信度 × EV帯のクロス分析
    print(f"\n  --- 自信度 × EV帯 クロス分析 (◎◉限定) ---")
    print(f"  {'自信度':<6} {'EV帯':<12} {'件数':>6} {'勝率':>7} {'複勝率':>7} {'単回収':>7}")
    print(f"  {'-'*6} {'-'*12} {'-'*6} {'-'*7} {'-'*7} {'-'*7}")

    conf_ev_groups = defaultdict(list)
    for race in all_races:
        for h in race["horses"]:
            if h["mark"] not in ("◉", "◎"):
                continue
            ev = h.get("ev")
            if ev is None:
                continue
            conf = race["confidence"]
            if ev < 1.0:
                ev_band = "EV<1.0"
            elif ev < 1.5:
                ev_band = "EV 1.0-1.5"
            else:
                ev_band = "EV≥1.5"
            conf_ev_groups[(conf, ev_band)].append(h)

    for conf in ["SS", "S", "A", "B", "C", "D"]:
        for ev_band in ["EV<1.0", "EV 1.0-1.5", "EV≥1.5"]:
            key = (conf, ev_band)
            if key not in conf_ev_groups:
                continue
            s = calc_stats(conf_ev_groups[key])
            if s["n"] < 10:
                continue
            print(f"  {conf:<6} {ev_band:<12} {s['n']:>6,} {s['win_rate']:>6.1f}% {s['place3_rate']:>6.1f}% {s['tansho_roi']:>6.1f}%")

    # 自信度別の全体成績
    print(f"\n  --- 自信度別 全体成績 ---")
    conf_groups = defaultdict(list)
    for race in all_races:
        for h in race["horses"]:
            if h["mark"] in ("◉", "◎"):
                conf_groups[race["confidence"]].append(h)

    for conf in ["SS", "S", "A", "B", "C", "D", "E"]:
        if conf in conf_groups:
            print_stats(f"  {conf}", conf_groups[conf])

    # value_score重み増加の理論的効果
    print(f"\n  --- 理論的効果 ---")
    print("  value_score (オッズ乖離) 重み: 15% → 20% (+5pt)")
    print("  gap_norm 重み: 15% → 12% (-3pt)")
    print("  multi_factor 重み: 15% → 13% (-2pt)")
    print("  → 高EV馬（オッズ乖離大）の自信度スコアが上昇")
    print("  → B自信度+EV≥1.5（回収率296%）がS/Aに昇格しやすくなる")

    # 高EV + 低自信度の馬を特定（昇格候補）
    promote_candidates = []
    for race in all_races:
        for h in race["horses"]:
            if h["mark"] in ("◉", "◎"):
                ev = h.get("ev")
                if ev and ev >= 1.5 and race["confidence"] in ("B", "C"):
                    promote_candidates.append(h)

    if promote_candidates:
        print(f"\n  昇格候補（B/C自信度 + EV≥1.5）: {len(promote_candidates):,}件")
        print_stats("  昇格候補の成績", promote_candidates)


# ============================================================
# 検証3: ソフト正規化キャリブレータ
# ============================================================

def verify_calibration(all_races):
    """win_probキャリブレーション精度を検証"""
    print("\n" + "=" * 80)
    print(" 検証3: win_probキャリブレーション精度")
    print("=" * 80)
    print(" ※注: 現在のpred.jsonのwin_probは旧正規化で出力されたもの")
    print("   ソフト正規化の効果は今後の新規予想で反映される")

    # win_prob帯別の実勝率
    print(f"\n  --- win_prob帯別 実勝率比較 ---")
    print(f"  {'wp帯':<15} {'件数':>7} {'予測平均':>8} {'実勝率':>7} {'比率':>7} {'判定':<10}")
    print(f"  {'-'*15} {'-'*7} {'-'*8} {'-'*7} {'-'*7} {'-'*10}")

    wp_bands = [
        (0, 0.02), (0.02, 0.05), (0.05, 0.10), (0.10, 0.15),
        (0.15, 0.20), (0.20, 0.25), (0.25, 0.30), (0.30, 0.40),
        (0.40, 0.50), (0.50, 1.0),
    ]

    for lo, hi in wp_bands:
        band_records = []
        for race in all_races:
            for h in race["horses"]:
                wp = h["win_prob"]
                if lo <= wp < hi:
                    band_records.append(h)

        if not band_records:
            continue

        avg_wp = sum(h["win_prob"] for h in band_records) / len(band_records)
        actual_win_rate = sum(1 for h in band_records if h["is_win"]) / len(band_records)
        ratio = actual_win_rate / avg_wp if avg_wp > 0 else 0

        if ratio > 1.3:
            judge = "過小評価"
        elif ratio < 0.7:
            judge = "過大評価"
        else:
            judge = "適正"

        print(f"  {lo*100:>4.0f}-{hi*100:<4.0f}%     {len(band_records):>7,} {avg_wp*100:>7.2f}% {actual_win_rate*100:>6.2f}% {ratio:>6.2f}x {judge}")

    # 理論的影響
    print(f"\n  --- ソフト正規化の影響 ---")
    print("  旧方式: Isotonic変換後に合計1.0に厳密正規化 → キャリブ効果消滅")
    print("  新方式: 合計が±30%を超えた場合のみスケーリング")
    print("  例: 12頭立てで合計1.15 → そのまま（許容範囲内）")
    print("      12頭立てで合計1.45 → 1.30にスケール（上限）")
    print("  → 15-30%帯の過小評価（1.4-1.6x）が改善される見込み")


# ============================================================
# 検証4: 全変更の複合効果
# ============================================================

def verify_combined_effect(all_races):
    """全変更の複合効果シミュレーション"""
    print("\n" + "=" * 80)
    print(" 検証4: 全変更の複合効果サマリ")
    print("=" * 80)

    # ◎◉の現行成績
    tekipan = [h for race in all_races for h in race["horses"] if h["mark"] == "◉"]
    honmei = [h for race in all_races for h in race["horses"] if h["mark"] == "◎"]
    top2 = tekipan + honmei

    print(f"\n  --- 変更前ベースライン ---")
    print_stats("◉", tekipan)
    print_stats("◎", honmei)
    print_stats("◎+◉", top2)

    # 変更1シミュレーション: ◉EV≥0.80フィルタ
    new_tekipan = []
    demoted = []
    for h in tekipan:
        eff_odds = h.get("effective_odds") or h.get("odds")
        ev = (h["win_prob"] or 0) * eff_odds if eff_odds and eff_odds > 0 else 1.0
        if ev >= 0.80:
            new_tekipan.append(h)
        else:
            demoted.append(h)
    new_honmei = honmei + demoted

    print(f"\n  --- 変更後シミュレーション ---")
    print_stats("新◉(EV≥0.80)", new_tekipan)
    print_stats("新◎(旧◎+降格)", new_honmei)
    print_stats("新◎+◉", new_tekipan + new_honmei)

    # 改善量
    old_tek_s = calc_stats(tekipan)
    new_tek_s = calc_stats(new_tekipan)
    print(f"\n  --- ◉改善量 ---")
    print(f"  件数: {old_tek_s['n']:,} → {new_tek_s['n']:,} ({new_tek_s['n'] - old_tek_s['n']:+,})")
    print(f"  勝率: {old_tek_s['win_rate']:.1f}% → {new_tek_s['win_rate']:.1f}% ({new_tek_s['win_rate'] - old_tek_s['win_rate']:+.1f}pt)")
    print(f"  複勝率: {old_tek_s['place3_rate']:.1f}% → {new_tek_s['place3_rate']:.1f}% ({new_tek_s['place3_rate'] - old_tek_s['place3_rate']:+.1f}pt)")
    print(f"  単回収: {old_tek_s['tansho_roi']:.1f}% → {new_tek_s['tansho_roi']:.1f}% ({new_tek_s['tansho_roi'] - old_tek_s['tansho_roi']:+.1f}pt)")

    # EV帯別回収率（全印）
    print(f"\n  --- EV帯別回収率（全◎◉） ---")
    ev_bands = [
        (0, 0.5, "EV<0.5"),
        (0.5, 0.7, "0.5≤EV<0.7"),
        (0.7, 0.85, "0.7≤EV<0.85"),
        (0.85, 1.0, "0.85≤EV<1.0"),
        (1.0, 1.2, "1.0≤EV<1.2"),
        (1.2, 1.5, "1.2≤EV<1.5"),
        (1.5, 2.0, "1.5≤EV<2.0"),
        (2.0, 999, "EV≥2.0"),
    ]
    for lo, hi, label in ev_bands:
        band = [h for h in top2 if h.get("ev") is not None and lo <= h["ev"] < hi]
        if band:
            print_stats(f"  {label}", band)

    # 半期別トレンド（全◎◉）
    print(f"\n  --- 半期別 ◎◉成績推移 ---")
    periods = defaultdict(list)
    for h in top2:
        year = h["date"][:4]
        half = "H1" if int(h["date"][5:7]) <= 6 else "H2"
        periods[f"{year}{half}"].append(h)
    for period in sorted(periods.keys()):
        print_stats(f"  {period}", periods[period])


# ============================================================
# 検証5: 印別成績詳細
# ============================================================

def verify_mark_performance(all_races):
    """全印の成績詳細"""
    print("\n" + "=" * 80)
    print(" 検証5: 印別成績詳細")
    print("=" * 80)

    mark_groups = defaultdict(list)
    for race in all_races:
        for h in race["horses"]:
            if h["mark"]:
                mark_groups[h["mark"]].append(h)

    mark_order = ["◉", "◎", "○", "▲", "△", "★", "☆", "×"]
    for mark in mark_order:
        if mark in mark_groups:
            print_stats(mark, mark_groups[mark])

    # JRA/NAR分割
    for scope, is_jra in [("JRA", True), ("NAR", False)]:
        print(f"\n  [{scope}]")
        for mark in mark_order:
            sub = [h for h in mark_groups.get(mark, []) if h["is_jra"] == is_jra]
            if sub:
                print_stats(f"  {mark}", sub)


# ============================================================
# 検証6: composite gap vs EV クロス分析
# ============================================================

def verify_gap_ev_cross(all_races):
    """composite gap × EV帯のクロス分析"""
    print("\n" + "=" * 80)
    print(" 検証6: composite gap × EV クロス分析 (◉◎)")
    print("=" * 80)

    # 各レースの◉◎のgapを計算
    records_with_gap = []
    for race in all_races:
        sorted_h = sorted(race["horses"], key=lambda h: h["composite"], reverse=True)
        if len(sorted_h) < 2:
            continue
        gap = sorted_h[0]["composite"] - sorted_h[1]["composite"]
        for h in race["horses"]:
            if h["mark"] in ("◉", "◎"):
                h_copy = dict(h)
                h_copy["gap"] = gap
                records_with_gap.append(h_copy)

    if not records_with_gap:
        print("  データなし")
        return

    gap_bands = [(0, 2), (2, 4), (4, 6), (6, 8), (8, 10), (10, 999)]
    ev_bands = [(0, 1.0, "EV<1.0"), (1.0, 1.5, "1.0≤EV<1.5"), (1.5, 999, "EV≥1.5")]

    print(f"\n  {'gap帯':<10}", end="")
    for _, _, ev_label in ev_bands:
        print(f" | {ev_label:<20}", end="")
    print()
    print(f"  {'-'*10}", end="")
    for _ in ev_bands:
        print(f" | {'-'*20}", end="")
    print()

    for g_lo, g_hi in gap_bands:
        g_label = f"gap {g_lo}-{g_hi}" if g_hi < 999 else f"gap≥{g_lo}"
        print(f"  {g_label:<10}", end="")
        for ev_lo, ev_hi, _ in ev_bands:
            subset = [h for h in records_with_gap
                      if g_lo <= h["gap"] < g_hi
                      and h.get("ev") is not None
                      and ev_lo <= h["ev"] < ev_hi]
            if len(subset) >= 10:
                s = calc_stats(subset)
                print(f" | {s['n']:>4} {s['win_rate']:>5.1f}% R{s['tansho_roi']:>5.0f}%", end="")
            else:
                print(f" | {'N/A':>20}", end="")
        print()


# ============================================================
# メイン
# ============================================================

def main():
    print("=" * 80)
    print(" EV変更 過去データ実証検証")
    print(f" 実行日時: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    all_races = load_all_data()

    verify_tekipan_ev_filter(all_races)
    verify_confidence_weight_change(all_races)
    verify_calibration(all_races)
    verify_combined_effect(all_races)
    verify_mark_performance(all_races)
    verify_gap_ev_cross(all_races)

    print("\n" + "=" * 80)
    print(" 検証完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
