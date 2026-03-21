"""
人気ブレンド効果の詳細リサーチ

pred JSON の win_prob（ブレンド済み）から人気統計成分を逆算で除去し、
AI純粋確率での的中率・回収率・Brier Score等を多角的に比較する。

7セクション:
  1. 印別成績（JRA/NAR分離、◉/◎分離）
  2. 実際の払戻を使った回収率（単勝/複勝/三連複BOX）
  3. 確率精度（Brier Score）
  4. Top-N正解率
  5. ディメンション別分解（芝ダ/自信度/頭数/月）
  6. AI vs 市場の乖離分析
  7. 三連複フォーメーション分析

注意:
  - 逆算のalpha計算はブレンド後gapを使用（循環依存あり）
  - place2/place3は0.95クリップ後のため逆算精度が低い
  - 分析はwin_prob（勝率）を中心に解釈すべき

使い方:
  PYTHONIOENCODING=utf-8 python tools/compare_pop_blend.py
  PYTHONIOENCODING=utf-8 python tools/compare_pop_blend.py --start 20260201 --end 20260228
"""

import io
import json
import glob
import os
import re
import sys
from collections import defaultdict

import pandas as pd

# Windows utf-8 出力対応
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.calculator.popularity_blend import (
    _lookup_rates,
    ALPHA_MODEL_MIN,
    ALPHA_MODEL_MAX,
    CONFIDENCE_GAP,
)
from src.results_tracker import _get_fukusho_payout, _safe_tansho_payout

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
PRED_DIR = os.path.join(DATA_DIR, "predictions")
RESULT_DIR = os.path.join(DATA_DIR, "results")

VALID_MARKS = {"◉", "◎", "○", "▲", "△", "★"}
MARK_ORDER = ["◉", "◎", "○", "▲", "△", "★"]
AI_MARK_SEQ = ["◎", "○", "▲", "△", "★"]


# ============================================================
# ヘルパー
# ============================================================

def _new_mark_bucket():
    return {"win": 0, "place2": 0, "place3": 0, "total": 0,
            "tansho_ret": 0, "fukusho_ret": 0, "fukusho_stake": 0}


def _new_dim_bucket():
    return {"top1_win": 0, "top3_contain": 0, "sanren_hit": 0, "total": 0,
            "brier_sum": 0.0, "brier_n": 0}


def _pct(num, den):
    return num / den * 100 if den else 0.0


def _field_size_bin(n):
    if n <= 8: return "small(≤8)"
    if n <= 14: return "medium(9-14)"
    return "large(15+)"


def _pop_band(pop):
    if pop is None or pop < 1: return "不明"
    if pop <= 3: return "1-3人気"
    if pop <= 6: return "4-6人気"
    if pop <= 9: return "7-9人気"
    return "10+人気"


# ============================================================
# 逆算（unblend）
# ============================================================

def load_stats():
    path = os.path.join(DATA_DIR, "popularity_rates.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def unblend_race(horses, venue, is_jra, field_count, stats):
    """ブレンド済み確率から人気統計成分を除去しAI純粋確率を復元"""
    org = "JRA" if is_jra else "NAR"
    all_wp = sorted([h.get("win_prob", 0) for h in horses], reverse=True)
    gap = (all_wp[0] - all_wp[1]) if len(all_wp) >= 2 else 0
    confidence = min(1.0, gap / CONFIDENCE_GAP)
    alpha_model = ALPHA_MODEL_MIN + confidence * (ALPHA_MODEL_MAX - ALPHA_MODEL_MIN)
    alpha_stats = 1.0 - alpha_model
    alpha_org = alpha_stats * 0.4
    alpha_venue = alpha_stats * 0.6

    result = []
    for h in horses:
        pop = h.get("popularity")
        odds = h.get("odds")
        wp = h.get("win_prob", 0)
        p2 = h.get("place2_prob", 0)
        p3 = h.get("place3_prob", 0)

        if pop is not None and pop >= 1:
            ow, ot2, ot3, vw, vt2, vt3 = _lookup_rates(
                stats, org, venue, pop, odds, field_count
            )
            ai_wp = max(0.001, (wp - alpha_org * ow - alpha_venue * vw) / alpha_model)
            ai_p2 = max(0.001, (p2 - alpha_org * ot2 - alpha_venue * vt2) / alpha_model)
            ai_p3 = max(0.001, (p3 - alpha_org * ot3 - alpha_venue * vt3) / alpha_model)
        else:
            ai_wp, ai_p2, ai_p3 = wp, p2, p3

        result.append({
            **h,  # 元データを全て保持
            "ai_win_prob": ai_wp,
            "ai_place2_prob": ai_p2,
            "ai_place3_prob": ai_p3,
        })

    # AI純粋確率を正規化
    n = len(result)
    for key, target in [("ai_win_prob", 1.0), ("ai_place2_prob", min(2.0, n)), ("ai_place3_prob", min(3.0, n))]:
        total = sum(r[key] for r in result)
        if total > 0:
            for r in result:
                r[key] = r[key] / total * target
    return result


def assign_marks_ai(horses):
    """AI確率順で印を割り当て（◎○▲△★の5頭）"""
    sorted_h = sorted(horses, key=lambda h: h.get("ai_win_prob", 0), reverse=True)
    marks = {}
    for i, h in enumerate(sorted_h):
        if i < len(AI_MARK_SEQ):
            marks[h["horse_no"]] = AI_MARK_SEQ[i]
    return marks


# ============================================================
# メイン処理
# ============================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="人気ブレンド効果の詳細リサーチ")
    parser.add_argument("--start", default="20260101", help="開始日 YYYYMMDD")
    parser.add_argument("--end", default="20260316", help="終了日 YYYYMMDD")
    parser.add_argument("--org", choices=["ALL", "JRA", "NAR"], default=None,
                        help="JRA/NARフィルタ（未指定=全体）")
    args = parser.parse_args()

    stats = load_stats()
    if not stats:
        print("人気別統計テーブルが見つかりません")
        return

    pred_files = sorted(glob.glob(os.path.join(PRED_DIR, "2026*_pred.json")))
    pred_files = [f for f in pred_files if args.start <= os.path.basename(f)[:8] <= args.end]

    print(f"対象期間: {args.start} ～ {args.end}")
    if args.org:
        print(f"フィルタ: {args.org}")
    print(f"予測ファイル数: {len(pred_files)}")
    print("※ 逆算はブレンド後gapベースのalpha使用（近似値）")

    # --- 集計コンテナ ---
    # セクション1: 印別  (mode, org, mark)
    mark_st = defaultdict(_new_mark_bucket)
    # セクション2+7: 三連複BOX  (mode, org)
    sanren_st = defaultdict(lambda: {"hit": 0, "total": 0, "stake": 0, "ret": 0, "payouts": []})
    # セクション3: Brier  (mode, org, pop_band)
    brier_st = defaultdict(lambda: {"sum_sq": 0.0, "n": 0})
    # セクション4: TopN  (mode, org)
    topn_st = defaultdict(lambda: {"top1": 0, "top2": 0, "top3": 0, "total": 0})
    # セクション5: ディメンション  (mode, dim_type, dim_value)
    dim_st = defaultdict(_new_dim_bucket)
    # セクション6: 乖離  (mode, org, direction)
    div_st = defaultdict(lambda: {"win": 0, "place3": 0, "total": 0, "tansho_ret": 0})
    # 全体カウント
    cnt = {"races": 0, "horses": 0, "skip_days": 0, "diff_top1": 0}

    for pf in pred_files:
        date_key = os.path.basename(pf)[:8]
        month = f"{date_key[:4]}-{date_key[4:6]}"
        result_path = os.path.join(RESULT_DIR, f"{date_key}_results.json")
        if not os.path.exists(result_path):
            cnt["skip_days"] += 1
            continue

        with open(pf, "r", encoding="utf-8") as f:
            pred_data = json.load(f)
        with open(result_path, "r", encoding="utf-8") as f:
            result_data = json.load(f)

        for race in pred_data.get("races", []):
            race_id = race.get("race_id", "")
            horses = race.get("horses", [])
            venue = race.get("venue", "")
            is_jra = race.get("is_jra", False)
            field_count = race.get("field_count", len(horses))
            surface = race.get("surface", "")
            conf = race.get("confidence", "")

            if not horses or not race_id:
                continue
            res = result_data.get(race_id, {})
            if not res or "order" not in res:
                continue

            order = res["order"]
            payouts = res.get("payouts", {})
            finish_map = {o["horse_no"]: o["finish"] for o in order}
            valid = [h for h in horses if h.get("horse_no") in finish_map]
            if len(valid) < 3:
                continue

            org = "JRA" if is_jra else "NAR"
            if args.org and args.org != "ALL" and org != args.org:
                continue

            cnt["races"] += 1
            cnt["horses"] += len(valid)
            fs_bin = _field_size_bin(field_count)

            # 逆算
            unblended = unblend_race(valid, venue, is_jra, field_count, stats)

            # odds辞書（高速引き用）
            odds_map = {h["horse_no"]: h.get("odds", 0) or 0 for h in valid}

            # ブレンド済み印
            bl_marks = {}
            for h in valid:
                m = h.get("mark", "")
                if m in VALID_MARKS:
                    bl_marks[h["horse_no"]] = m
            # AI印
            ai_marks = assign_marks_ai(unblended)

            # ソート済みリスト
            bl_sorted = sorted(valid, key=lambda x: x.get("win_prob", 0), reverse=True)
            ai_sorted = sorted(unblended, key=lambda x: x.get("ai_win_prob", 0), reverse=True)

            actual_top3 = set(o["horse_no"] for o in order if o["finish"] <= 3)
            winner_hno = next((o["horse_no"] for o in order if o["finish"] == 1), 0)
            tansho_payout = _safe_tansho_payout(payouts, winner_hno)

            # top1が変わるか
            if bl_sorted[0]["horse_no"] != ai_sorted[0]["horse_no"]:
                cnt["diff_top1"] += 1

            # 確率ランク辞書（乖離分析用）
            bl_rank = {h["horse_no"]: i + 1 for i, h in enumerate(bl_sorted)}
            ai_rank = {h["horse_no"]: i + 1 for i, h in enumerate(ai_sorted)}

            # --- 2モード同時集計 ---
            for mode, sorted_list, marks_dict, prob_key in [
                ("bl", bl_sorted, bl_marks, "win_prob"),
                ("ai", ai_sorted, ai_marks, "ai_win_prob"),
            ]:
                # --- セクション1: 印別 ---
                for hno, mark in marks_dict.items():
                    fp = finish_map.get(hno, 99)
                    for o in ["ALL", org]:
                        b = mark_st[(mode, o, mark)]
                        b["total"] += 1
                        if fp == 1:
                            b["win"] += 1
                            b["tansho_ret"] += tansho_payout
                        if fp <= 2:
                            b["place2"] += 1
                        if fp <= 3:
                            b["place3"] += 1
                            fk = _get_fukusho_payout(hno, payouts)
                            if fk:
                                b["fukusho_ret"] += fk
                        b["fukusho_stake"] += 100

                # --- セクション2+7: 三連複BOX TOP5 ---
                top5 = set(h["horse_no"] for h in sorted_list[:5])
                for o in ["ALL", org]:
                    s = sanren_st[(mode, o)]
                    s["total"] += 1
                    s["stake"] += 1000  # 10点 × 100円
                    if actual_top3.issubset(top5):
                        s["hit"] += 1
                        sp = payouts.get("三連複", {})
                        payout_val = sp.get("payout", 0) if isinstance(sp, dict) else 0
                        s["ret"] += payout_val
                        s["payouts"].append(payout_val)

                # --- セクション3: Brier Score ---
                for h in (valid if mode == "bl" else unblended):
                    wp = h.get(prob_key, 0)
                    actual = 1 if finish_map.get(h["horse_no"]) == 1 else 0
                    sq = (wp - actual) ** 2
                    pop = h.get("popularity")
                    pb = _pop_band(pop)
                    for o in ["ALL", org]:
                        brier_st[(mode, o, "全体")]["sum_sq"] += sq
                        brier_st[(mode, o, "全体")]["n"] += 1
                        brier_st[(mode, o, pb)]["sum_sq"] += sq
                        brier_st[(mode, o, pb)]["n"] += 1

                # --- セクション4: TopN ---
                for o in ["ALL", org]:
                    t = topn_st[(mode, o)]
                    t["total"] += 1
                    if finish_map.get(sorted_list[0]["horse_no"]) == 1:
                        t["top1"] += 1
                    actual_top2 = set(o2["horse_no"] for o2 in order if o2["finish"] <= 2)
                    pred_top2 = set(h["horse_no"] for h in sorted_list[:2])
                    if actual_top2.issubset(pred_top2):
                        t["top2"] += 1
                    pred_top3 = set(h["horse_no"] for h in sorted_list[:3])
                    if actual_top3.issubset(pred_top3):
                        t["top3"] += 1

                # --- セクション5: ディメンション ---
                is_top1_win = finish_map.get(sorted_list[0]["horse_no"]) == 1
                is_sanren = actual_top3.issubset(top5)
                # Brier（全馬平均）
                brier_vals = []
                for h in (valid if mode == "bl" else unblended):
                    wp = h.get(prob_key, 0)
                    actual = 1 if finish_map.get(h["horse_no"]) == 1 else 0
                    brier_vals.append((wp - actual) ** 2)
                brier_race = sum(brier_vals)
                brier_n_race = len(brier_vals)

                for dim_type, dim_value in [
                    ("org", org), ("surface", surface), ("confidence", conf),
                    ("field_size", fs_bin), ("month", month),
                ]:
                    if not dim_value:
                        continue
                    d = dim_st[(mode, dim_type, dim_value)]
                    d["total"] += 1
                    if is_top1_win: d["top1_win"] += 1
                    if is_sanren: d["sanren_hit"] += 1
                    d["brier_sum"] += brier_race
                    d["brier_n"] += brier_n_race

                # --- セクション6: 乖離分析 ---
                rank_map = bl_rank if mode == "bl" else ai_rank
                for h in valid:
                    hno = h["horse_no"]
                    pop = h.get("popularity")
                    if pop is None or pop < 1:
                        continue
                    prob_r = rank_map.get(hno, 99)
                    diff = pop - prob_r  # 正=AIが高評価（人気より上位）
                    if abs(diff) < 3:
                        continue
                    direction = "ai_higher" if diff > 0 else "ai_lower"
                    fp = finish_map.get(hno, 99)
                    for o in ["ALL", org]:
                        dv = div_st[(mode, o, direction)]
                        dv["total"] += 1
                        if fp == 1:
                            dv["win"] += 1
                            dv["tansho_ret"] += tansho_payout
                        if fp <= 3:
                            dv["place3"] += 1

    # ============================================================
    # 結果出力（pandas DataFrame）
    # ============================================================
    pd.set_option("display.unicode.east_asian_width", True)
    org_list = [args.org] if args.org and args.org != "ALL" else ["ALL", "JRA", "NAR"]

    R = cnt["races"]
    print(f"\n{'='*80}")
    print(f"分析結果: {R:,}レース / {cnt['horses']:,}頭  (スキップ {cnt['skip_days']}日)")
    print(f"{'='*80}")

    # --- セクション1: 印別成績 ---
    print(f"\n{'─'*80}")
    print("■ セクション1: 印別成績比較")
    print(f"{'─'*80}")
    rows1 = []
    for o in org_list:
        for mark in MARK_ORDER:
            b = mark_st[("bl", o, mark)]
            a = mark_st[("ai", o, mark)]
            if b["total"] == 0 and a["total"] == 0:
                continue
            rows1.append({
                "org": o, "印": mark, "出走": b["total"],
                "勝率_BL": f"{_pct(b['win'], b['total']):.1f}%",
                "連対_BL": f"{_pct(b['place2'], b['total']):.1f}%",
                "複勝_BL": f"{_pct(b['place3'], b['total']):.1f}%",
                "単回_BL": f"{b['tansho_ret'] / b['total']:.0f}%" if b["total"] else "—",
                "複回_BL": f"{_pct(b['fukusho_ret'], b['fukusho_stake']):.0f}%" if b["fukusho_stake"] else "—",
                "勝率_AI": f"{_pct(a['win'], a['total']):.1f}%",
                "連対_AI": f"{_pct(a['place2'], a['total']):.1f}%",
                "複勝_AI": f"{_pct(a['place3'], a['total']):.1f}%",
                "単回_AI": f"{a['tansho_ret'] / a['total']:.0f}%" if a["total"] else "—",
                "複回_AI": f"{_pct(a['fukusho_ret'], a['fukusho_stake']):.0f}%" if a["fukusho_stake"] else "—",
                "勝率差": f"{_pct(a['win'], a['total']) - _pct(b['win'], b['total']):+.1f}%",
            })
    if rows1:
        df1 = pd.DataFrame(rows1)
        print(df1.to_string(index=False))

    # --- セクション2: 三連複BOX回収率 ---
    print(f"\n{'─'*80}")
    print("■ セクション2: 三連複BOX回収率（TOP5 = 10点 × 100円）")
    print(f"{'─'*80}")
    rows2 = []
    for o in org_list:
        b = sanren_st[("bl", o)]
        a = sanren_st[("ai", o)]
        if b["total"] == 0:
            continue
        b_avg = sum(b["payouts"]) / len(b["payouts"]) if b["payouts"] else 0
        a_avg = sum(a["payouts"]) / len(a["payouts"]) if a["payouts"] else 0
        rows2.append({
            "org": o, "件数": b["total"],
            "的中_BL": b["hit"],
            "的中率_BL": f"{_pct(b['hit'], b['total']):.1f}%",
            "回収率_BL": f"{_pct(b['ret'], b['stake']):.0f}%",
            "平均配当_BL": f"{b_avg:,.0f}円",
            "的中_AI": a["hit"],
            "的中率_AI": f"{_pct(a['hit'], a['total']):.1f}%",
            "回収率_AI": f"{_pct(a['ret'], a['stake']):.0f}%",
            "平均配当_AI": f"{a_avg:,.0f}円",
            "的中率差": f"{_pct(a['hit'], a['total']) - _pct(b['hit'], b['total']):+.1f}%",
        })
    if rows2:
        df2 = pd.DataFrame(rows2)
        print(df2.to_string(index=False))

    # --- セクション3: Brier Score ---
    print(f"\n{'─'*80}")
    print("■ セクション3: Brier Score（値が小さいほど精度が高い）")
    print(f"{'─'*80}")
    POP_BANDS = ["全体", "1-3人気", "4-6人気", "7-9人気", "10+人気"]
    rows3 = []
    for o in org_list:
        for pb in POP_BANDS:
            b = brier_st[("bl", o, pb)]
            a = brier_st[("ai", o, pb)]
            if b["n"] == 0:
                continue
            b_bs = b["sum_sq"] / b["n"]
            a_bs = a["sum_sq"] / a["n"]
            diff = a_bs - b_bs
            judge = "AI良" if diff < 0 else "BL良" if diff > 0 else "同等"
            rows3.append({
                "org": o, "人気帯": pb, "サンプル": f"{b['n']:,}",
                "Brier_BL": f"{b_bs:.5f}",
                "Brier_AI": f"{a_bs:.5f}",
                "差": f"{diff:+.5f}",
                "判定": judge,
            })
    if rows3:
        df3 = pd.DataFrame(rows3)
        print(df3.to_string(index=False))

    # --- セクション4: Top-N正解率 ---
    print(f"\n{'─'*80}")
    print("■ セクション4: Top-N正解率")
    print(f"{'─'*80}")
    rows4 = []
    for o in org_list:
        b = topn_st[("bl", o)]
        a = topn_st[("ai", o)]
        if b["total"] == 0:
            continue
        rows4.append({
            "org": o, "件数": b["total"],
            "Top1_BL": f"{_pct(b['top1'], b['total']):.1f}%",
            "Top2_BL": f"{_pct(b['top2'], b['total']):.1f}%",
            "Top3_BL": f"{_pct(b['top3'], b['total']):.1f}%",
            "Top1_AI": f"{_pct(a['top1'], a['total']):.1f}%",
            "Top2_AI": f"{_pct(a['top2'], a['total']):.1f}%",
            "Top3_AI": f"{_pct(a['top3'], a['total']):.1f}%",
            "Top1差": f"{_pct(a['top1'], a['total']) - _pct(b['top1'], b['total']):+.1f}%",
        })
    if rows4:
        df4 = pd.DataFrame(rows4)
        print(df4.to_string(index=False))

    # --- セクション5: ディメンション別 ---
    print(f"\n{'─'*80}")
    print("■ セクション5: ディメンション別分解")
    print(f"{'─'*80}")
    DIM_ORDER = [
        ("org", ["JRA", "NAR"]),
        ("surface", ["芝", "ダート"]),
        ("confidence", ["SS", "S", "A", "B", "C", "D"]),
        ("field_size", ["small(≤8)", "medium(9-14)", "large(15+)"]),
        ("month", sorted(set(k[2] for k in dim_st if k[0] == "bl" and k[1] == "month"))),
    ]
    DIM_LABELS = {"org": "JRA/NAR", "surface": "馬場", "confidence": "自信度",
                  "field_size": "頭数", "month": "月"}
    rows5 = []
    for dim_type, values in DIM_ORDER:
        label = DIM_LABELS.get(dim_type, dim_type)
        for v in values:
            b = dim_st[("bl", dim_type, v)]
            a = dim_st[("ai", dim_type, v)]
            if b["total"] == 0:
                continue
            b_br = b["brier_sum"] / b["brier_n"] if b["brier_n"] else 0
            a_br = a["brier_sum"] / a["brier_n"] if a["brier_n"] else 0
            rows5.append({
                "分類": label, "値": v, "件数": b["total"],
                "Top1_BL": f"{_pct(b['top1_win'], b['total']):.1f}%",
                "三連複_BL": f"{_pct(b['sanren_hit'], b['total']):.1f}%",
                "Brier_BL": f"{b_br:.5f}",
                "Top1_AI": f"{_pct(a['top1_win'], a['total']):.1f}%",
                "三連複_AI": f"{_pct(a['sanren_hit'], a['total']):.1f}%",
                "Brier_AI": f"{a_br:.5f}",
                "三連複差": f"{_pct(a['sanren_hit'], a['total']) - _pct(b['sanren_hit'], b['total']):+.1f}%",
            })
    if rows5:
        df5 = pd.DataFrame(rows5)
        print(df5.to_string(index=False))

    # --- セクション6: 乖離分析 ---
    print(f"\n{'─'*80}")
    print("■ セクション6: AI vs 市場の乖離分析（確率順位 - 人気 の差が3以上）")
    print(f"{'─'*80}")
    rows6 = []
    for o in org_list:
        for direction, dir_label in [("ai_higher", "AI高評価(穴)"), ("ai_lower", "AI低評価(危)")]:
            b = div_st[("bl", o, direction)]
            a = div_st[("ai", o, direction)]
            if b["total"] == 0 and a["total"] == 0:
                continue
            rows6.append({
                "org": o, "方向": dir_label,
                "件数_BL": b["total"],
                "勝率_BL": f"{_pct(b['win'], b['total']):.1f}%",
                "複勝_BL": f"{_pct(b['place3'], b['total']):.1f}%",
                "単回_BL": f"{b['tansho_ret'] / b['total']:.0f}%" if b["total"] else "—",
                "件数_AI": a["total"],
                "勝率_AI": f"{_pct(a['win'], a['total']):.1f}%",
                "複勝_AI": f"{_pct(a['place3'], a['total']):.1f}%",
                "単回_AI": f"{a['tansho_ret'] / a['total']:.0f}%" if a["total"] else "—",
            })
    if rows6:
        df6 = pd.DataFrame(rows6)
        print(df6.to_string(index=False))

    # --- セクション7: 総合サマリー ---
    print(f"\n{'─'*80}")
    print(f"■ セクション7: 1位指名の変動: {cnt['diff_top1']}/{R} ({_pct(cnt['diff_top1'], R):.1f}%)")
    print(f"{'─'*80}")

    print(f"\n{'='*80}")
    print("■ 総合サマリー")
    print(f"{'='*80}")
    rows7 = []
    so = org_list[0]  # サマリー用org（フィルタ時はそのorg、未指定時はALL）
    # Top1
    bt1 = _pct(topn_st[("bl",so)]["top1"], topn_st[("bl",so)]["total"])
    at1 = _pct(topn_st[("ai",so)]["top1"], topn_st[("ai",so)]["total"])
    rows7.append({"指標": "Top1勝率", "ブレンド": f"{bt1:.1f}%", "AI純粋": f"{at1:.1f}%",
                  "差": f"{at1-bt1:+.1f}%", "優位": "BL" if bt1 > at1 else "AI"})
    # 三連複的中率
    bsr = _pct(sanren_st[("bl",so)]["hit"], sanren_st[("bl",so)]["total"])
    asr = _pct(sanren_st[("ai",so)]["hit"], sanren_st[("ai",so)]["total"])
    rows7.append({"指標": "三連複的中率", "ブレンド": f"{bsr:.1f}%", "AI純粋": f"{asr:.1f}%",
                  "差": f"{asr-bsr:+.1f}%", "優位": "BL" if bsr > asr else "AI"})
    # 三連複回収率
    b_roi = _pct(sanren_st[("bl",so)]["ret"], sanren_st[("bl",so)]["stake"])
    a_roi = _pct(sanren_st[("ai",so)]["ret"], sanren_st[("ai",so)]["stake"])
    rows7.append({"指標": "三連複回収率", "ブレンド": f"{b_roi:.0f}%", "AI純粋": f"{a_roi:.0f}%",
                  "差": f"{a_roi-b_roi:+.0f}%", "優位": "BL" if b_roi > a_roi else "AI"})
    # Brier Score
    bb = brier_st[("bl",so,"全体")]
    ab = brier_st[("ai",so,"全体")]
    b_brier = bb["sum_sq"] / bb["n"] if bb["n"] else 0
    a_brier = ab["sum_sq"] / ab["n"] if ab["n"] else 0
    rows7.append({"指標": "Brier Score", "ブレンド": f"{b_brier:.5f}", "AI純粋": f"{a_brier:.5f}",
                  "差": f"{a_brier-b_brier:+.5f}", "優位": "BL" if b_brier < a_brier else "AI"})
    # ◎勝率
    b_honmei = mark_st[("bl", so, "◉")]
    a_honmei = mark_st[("ai", so, "◎")]
    b_hw = _pct(b_honmei["win"], b_honmei["total"])
    a_hw = _pct(a_honmei["win"], a_honmei["total"])
    rows7.append({"指標": "本命勝率", "ブレンド": f"{b_hw:.1f}%", "AI純粋": f"{a_hw:.1f}%",
                  "差": f"{a_hw-b_hw:+.1f}%", "優位": "BL" if b_hw > a_hw else "AI"})
    # ◎複勝率
    b_hr3 = _pct(b_honmei["place3"], b_honmei["total"])
    a_hr3 = _pct(a_honmei["place3"], a_honmei["total"])
    rows7.append({"指標": "本命複勝率", "ブレンド": f"{b_hr3:.1f}%", "AI純粋": f"{a_hr3:.1f}%",
                  "差": f"{a_hr3-b_hr3:+.1f}%", "優位": "BL" if b_hr3 > a_hr3 else "AI"})

    df7 = pd.DataFrame(rows7)
    print(df7.to_string(index=False))


if __name__ == "__main__":
    main()
