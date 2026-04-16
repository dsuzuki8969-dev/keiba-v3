#!/usr/bin/env python
"""
自信度スコア v5 シミュレーション

v4（現行）: 7信号（value_score含む市場信号あり）+ 人気ゲート
v5（提案）: 6信号（value_score除去、市場フリー）+ win_prob/gapゲート

全pred.jsonを読み込み、v4/v5両方のスコア・レベルを計算し、
race_resultsの着順データと突合して的中率・出現率を比較する。
"""

import io
import json
import glob
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

# エンコーディング対策
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8',
                              line_buffering=True, errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8',
                              line_buffering=True, errors='replace')

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "keiba.db"
PRED_DIR = ROOT / "data" / "predictions"

# ── v4 閾値（現行 settings.py） ──
V4_THRESHOLDS_JRA = {"SS": 0.641, "S": 0.537, "A": 0.438, "B": 0.257, "C": 0.134}
V4_THRESHOLDS_NAR = {"SS": 0.761, "S": 0.673, "A": 0.585, "B": 0.390, "C": 0.224}
V4_POP_GATE_SS_JRA = 1
V4_POP_GATE_SS_NAR = 1
V4_POP_GATE_S_JRA = 2
V4_POP_GATE_S_NAR = 2

# ── v5 提案: win_prob/gap ゲート ──
V5_WIN_PROB_GATE_SS = 0.30
V5_GAP_GATE_SS = 5.0
V5_WIN_PROB_GATE_S = 0.22
V5_GAP_GATE_S = 3.0

# v4 gap_divisor
GAP_DIVISOR_JRA = 4.0
GAP_DIVISOR_NAR = 8.0

LEVELS = ["SS", "S", "A", "B", "C", "D"]


def load_results(db_path: str) -> dict:
    """race_results から着順マップを構築: {race_id: {horse_no: finish_pos}}"""
    conn = sqlite3.connect(db_path)
    cur = conn.execute("SELECT race_id, order_json FROM race_results WHERE cancelled=0 AND order_json IS NOT NULL")
    results = {}
    for race_id, order_json in cur:
        try:
            order = json.loads(order_json)
            mapping = {}
            for entry in order:
                hno = entry.get("horse_no") or entry.get("umaban")
                pos = entry.get("finish") or entry.get("order") or entry.get("rank")
                if hno and pos:
                    try:
                        mapping[int(hno)] = int(pos)
                    except (ValueError, TypeError):
                        pass
            if mapping:
                results[race_id] = mapping
        except (json.JSONDecodeError, TypeError):
            pass
    conn.close()
    return results


def calc_v4_score(horses, is_jra: bool) -> float:
    """v4スコア: 7信号（value_score含む）"""
    sorted_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    if len(sorted_comp) < 3:
        return 1.0 if len(sorted_comp) >= 1 else 0.0

    top = sorted_comp[0]
    top_id = top.get("horse_id", "")

    # 1. composite_gap
    gap = sorted_comp[0]["composite"] - sorted_comp[1]["composite"]
    gap_div = GAP_DIVISOR_JRA if is_jra else GAP_DIVISOR_NAR
    gap_norm = min(gap / gap_div, 1.0)

    # 2. ml_agreement
    sorted_wp = sorted(horses, key=lambda h: h.get("win_prob", 0), reverse=True)
    wp_top_id = sorted_wp[0].get("horse_id", "")
    if wp_top_id == top_id:
        ml_agreement = 1.0
    elif len(sorted_wp) >= 2 and sorted_wp[1].get("horse_id", "") == top_id:
        ml_agreement = 0.5
    else:
        ml_agreement = 0.0

    # 3. gap23
    gap23 = sorted_comp[1]["composite"] - sorted_comp[2]["composite"]
    gap23_norm = min(gap23 / 4.0, 1.0)

    # 4. value_score（市場信号）
    odds = top.get("odds") or 0
    wp = top.get("win_prob") or 0
    if odds > 1.0 and wp > 0:
        odds_implied = 1.0 / odds
        vr = wp / odds_implied
        value_score = min(max((vr - 1.0) / 0.5, 0.0), 1.0)
    else:
        value_score = 0.5

    # 5. multi_factor
    top_ability_id = max(horses, key=lambda h: h.get("ability_total", 0)).get("horse_id", "")
    top_pace_id = max(horses, key=lambda h: h.get("pace_total", 0)).get("horse_id", "")
    top_course_id = max(horses, key=lambda h: h.get("course_total", 0)).get("horse_id", "")
    factor_match = sum(1 for fid in [top_ability_id, top_pace_id, top_course_id] if fid == top_id)
    if factor_match == 3:
        multi_factor = 1.0
    elif factor_match == 2:
        multi_factor = 0.6
    else:
        multi_factor = 0.0

    # 6. reliability (上位3馬 → データ品質。pred.jsonにはreliabilityが無いので近似)
    # ability_reliabilityフィールドがあれば利用、なければ0.5
    rel_count = 0
    for ev in sorted_comp[:3]:
        rel = ev.get("ability_reliability", "")
        if rel == "A":
            rel_count += 1
    reliability_norm = rel_count / 3.0

    # 7. ml_confidence
    raw_top = sorted_comp[0].get("raw_lgbm_prob")
    raw_2nd = sorted_comp[1].get("raw_lgbm_prob") if len(sorted_comp) >= 2 else None
    if raw_top is not None and raw_2nd is not None:
        ml_raw_gap = raw_top - raw_2nd
        ml_confidence = min(ml_raw_gap / 0.10, 1.0) if ml_raw_gap > 0 else 0.0
    else:
        ml_confidence = 0.5

    score = (
        gap_norm * 0.12
        + ml_agreement * 0.20
        + gap23_norm * 0.10
        + value_score * 0.20
        + multi_factor * 0.13
        + reliability_norm * 0.10
        + ml_confidence * 0.15
    )
    return score


def calc_v5_score(horses, is_jra: bool) -> float:
    """v5スコア: 6信号（value_score除去、市場フリー）"""
    sorted_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    if len(sorted_comp) < 3:
        return 1.0 if len(sorted_comp) >= 1 else 0.0

    top = sorted_comp[0]
    top_id = top.get("horse_id", "")

    # 1. composite_gap (20%)
    gap = sorted_comp[0]["composite"] - sorted_comp[1]["composite"]
    gap_div = GAP_DIVISOR_JRA if is_jra else GAP_DIVISOR_NAR
    gap_norm = min(gap / gap_div, 1.0)

    # 2. ml_agreement (25%)
    sorted_wp = sorted(horses, key=lambda h: h.get("win_prob", 0), reverse=True)
    wp_top_id = sorted_wp[0].get("horse_id", "")
    if wp_top_id == top_id:
        ml_agreement = 1.0
    elif len(sorted_wp) >= 2 and sorted_wp[1].get("horse_id", "") == top_id:
        ml_agreement = 0.5
    else:
        ml_agreement = 0.0

    # 3. gap23 (10%)
    gap23 = sorted_comp[1]["composite"] - sorted_comp[2]["composite"]
    gap23_norm = min(gap23 / 4.0, 1.0)

    # 4. value_score → 削除

    # 5. multi_factor (20%)
    top_ability_id = max(horses, key=lambda h: h.get("ability_total", 0)).get("horse_id", "")
    top_pace_id = max(horses, key=lambda h: h.get("pace_total", 0)).get("horse_id", "")
    top_course_id = max(horses, key=lambda h: h.get("course_total", 0)).get("horse_id", "")
    factor_match = sum(1 for fid in [top_ability_id, top_pace_id, top_course_id] if fid == top_id)
    if factor_match == 3:
        multi_factor = 1.0
    elif factor_match == 2:
        multi_factor = 0.6
    else:
        multi_factor = 0.0

    # 6. reliability (10%)
    rel_count = 0
    for ev in sorted_comp[:3]:
        rel = ev.get("ability_reliability", "")
        if rel == "A":
            rel_count += 1
    reliability_norm = rel_count / 3.0

    # 7. ml_confidence (15%)
    raw_top = sorted_comp[0].get("raw_lgbm_prob")
    raw_2nd = sorted_comp[1].get("raw_lgbm_prob") if len(sorted_comp) >= 2 else None
    if raw_top is not None and raw_2nd is not None:
        ml_raw_gap = raw_top - raw_2nd
        ml_confidence = min(ml_raw_gap / 0.10, 1.0) if ml_raw_gap > 0 else 0.0
    else:
        ml_confidence = 0.5

    score = (
        gap_norm * 0.20
        + ml_agreement * 0.25
        + gap23_norm * 0.10
        + multi_factor * 0.20
        + reliability_norm * 0.10
        + ml_confidence * 0.15
    )
    return score


def assign_level_v4(score: float, top_pop: int, is_jra: bool) -> str:
    """v4: パーセンタイル閾値 + 人気ゲート"""
    th = V4_THRESHOLDS_JRA if is_jra else V4_THRESHOLDS_NAR
    pg_ss = V4_POP_GATE_SS_JRA if is_jra else V4_POP_GATE_SS_NAR
    pg_s = V4_POP_GATE_S_JRA if is_jra else V4_POP_GATE_S_NAR

    if score >= th["SS"]:
        level = "SS"
    elif score >= th["S"]:
        level = "S"
    elif score >= th["A"]:
        level = "A"
    elif score >= th["B"]:
        level = "B"
    elif score >= th["C"]:
        level = "C"
    else:
        level = "D"

    if level == "SS" and (top_pop or 99) > pg_ss:
        level = "S"
    if level == "S" and (top_pop or 99) > pg_s:
        level = "A"
    return level


def assign_level_v5(score: float, top_wp: float, top_gap: float, is_jra: bool,
                    thresholds: dict) -> str:
    """v5: パーセンタイル閾値 + win_prob/gapゲート（市場フリー）"""
    if score >= thresholds["SS"]:
        level = "SS"
    elif score >= thresholds["S"]:
        level = "S"
    elif score >= thresholds["A"]:
        level = "A"
    elif score >= thresholds["B"]:
        level = "B"
    elif score >= thresholds["C"]:
        level = "C"
    else:
        level = "D"

    # win_prob/gap ゲート
    if level == "SS":
        if top_wp < V5_WIN_PROB_GATE_SS or top_gap < V5_GAP_GATE_SS:
            level = "S"
    if level == "S":
        if top_wp < V5_WIN_PROB_GATE_S or top_gap < V5_GAP_GATE_S:
            level = "A"
    return level


def main():
    print("=" * 70)
    print("自信度スコア v5 シミュレーション")
    print("v4（現行）vs v5（market-free）比較")
    print("=" * 70)

    # 1. 着順データ読み込み
    print("\n[1/4] 着順データ読み込み中...")
    results = load_results(str(DB_PATH))
    print(f"  着順データ: {len(results)} レース")

    # 2. 全pred.json読み込み＋スコア計算
    pred_files = sorted(glob.glob(str(PRED_DIR / "*_pred.json")))
    pred_files = [f for f in pred_files if "_prev" not in f and "_backup" not in f]
    print(f"\n[2/4] pred.json読み込み中... ({len(pred_files)} ファイル)")

    v4_scores_jra = []
    v4_scores_nar = []
    v5_scores_jra = []
    v5_scores_nar = []

    # {version: {jra/nar: {level: {"total": N, "honmei_win": N, "honmei_place3": N}}}}
    stats = {
        "v4": {"jra": defaultdict(lambda: {"total": 0, "honmei_win": 0, "honmei_place3": 0}),
               "nar": defaultdict(lambda: {"total": 0, "honmei_win": 0, "honmei_place3": 0})},
        "v5": {"jra": defaultdict(lambda: {"total": 0, "honmei_win": 0, "honmei_place3": 0}),
               "nar": defaultdict(lambda: {"total": 0, "honmei_win": 0, "honmei_place3": 0})},
    }

    # v4/v5レベル変更の追跡
    transitions = defaultdict(int)  # (v4_level, v5_level) -> count

    processed = 0
    skipped_no_result = 0

    for fi, fn in enumerate(pred_files):
        if fi % 100 == 0:
            pct = fi / len(pred_files) * 100
            print(f"  {fi}/{len(pred_files)} ({pct:.1f}%)")
        try:
            with open(fn, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        for race in data.get("races", []):
            horses = race.get("horses", [])
            if len(horses) < 3:
                continue

            race_id = race.get("race_id", "")
            is_jra = race.get("is_jra", False)
            cat = "jra" if is_jra else "nar"

            # 結果データ確認
            result_map = results.get(race_id, {})

            # ◎馬（composite 1位）特定
            sorted_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
            top = sorted_comp[0]
            top_hno = top.get("horse_no", 0)
            top_pop = top.get("popularity") or 99
            top_wp = top.get("win_prob") or 0
            top_gap = sorted_comp[0].get("composite", 0) - sorted_comp[1].get("composite", 0)

            # v4スコア計算
            v4_score = calc_v4_score(horses, is_jra)
            v4_level = assign_level_v4(v4_score, top_pop, is_jra)

            # v5スコア計算（閾値は後でパーセンタイルから算出するので仮保存）
            v5_score = calc_v5_score(horses, is_jra)

            if is_jra:
                v4_scores_jra.append(v4_score)
                v5_scores_jra.append((v5_score, top_wp, top_gap, race_id, top_hno, result_map))
            else:
                v4_scores_nar.append(v4_score)
                v5_scores_nar.append((v5_score, top_wp, top_gap, race_id, top_hno, result_map))

            # v4集計（結果あり）
            if result_map and top_hno in result_map:
                finish = result_map[top_hno]
                stats["v4"][cat][v4_level]["total"] += 1
                if finish == 1:
                    stats["v4"][cat][v4_level]["honmei_win"] += 1
                if finish <= 3:
                    stats["v4"][cat][v4_level]["honmei_place3"] += 1
            elif not result_map:
                skipped_no_result += 1

            processed += 1

    print(f"  処理完了: {processed} レース (結果なしスキップ: {skipped_no_result})")

    # 3. v5パーセンタイル閾値算出
    print("\n[3/4] v5パーセンタイル閾値算出中...")

    # 目標分布: SS 5%, S 10%, A 15%, B 35%, C 25%, D 10%
    # → SS: 上位5%, S: 5-15%, A: 15-30%, B: 30-65%, C: 65-90%, D: 90-100%
    import numpy as np

    v5_jra_arr = np.array([s[0] for s in v5_scores_jra])
    v5_nar_arr = np.array([s[0] for s in v5_scores_nar])

    # パーセンタイル（上位X%なので100-Xパーセンタイル）
    v5_th_jra = {
        "SS": float(np.percentile(v5_jra_arr, 95)),   # 上位5%
        "S":  float(np.percentile(v5_jra_arr, 85)),    # 上位15%
        "A":  float(np.percentile(v5_jra_arr, 70)),    # 上位30%
        "B":  float(np.percentile(v5_jra_arr, 35)),    # 上位65%
        "C":  float(np.percentile(v5_jra_arr, 10)),    # 上位90%
    }
    v5_th_nar = {
        "SS": float(np.percentile(v5_nar_arr, 95)),
        "S":  float(np.percentile(v5_nar_arr, 85)),
        "A":  float(np.percentile(v5_nar_arr, 70)),
        "B":  float(np.percentile(v5_nar_arr, 35)),
        "C":  float(np.percentile(v5_nar_arr, 10)),
    }

    print(f"  JRA v5閾値: {json.dumps({k: round(v, 4) for k, v in v5_th_jra.items()})}")
    print(f"  NAR v5閾値: {json.dumps({k: round(v, 4) for k, v in v5_th_nar.items()})}")

    # 4. v5レベル割当 + 集計
    print("\n[4/4] v5レベル割当・集計中...")

    for scores_list, th, cat in [
        (v5_scores_jra, v5_th_jra, "jra"),
        (v5_scores_nar, v5_th_nar, "nar"),
    ]:
        is_jra = (cat == "jra")
        for v5_score, top_wp, top_gap, race_id, top_hno, result_map in scores_list:
            v5_level = assign_level_v5(v5_score, top_wp, top_gap, is_jra, th)

            # v4レベルも再計算（比較用）
            v4_score_recalc = None  # 既にstatsに集計済みなので不要
            # transitions追跡用にv4レベルが必要
            # → v4は既に上で計算済みだが保存してない… ここで近似的に再計算
            # pred.jsonの confidence フィールドを使えるが、v4適用前のデータもあるため
            # ここでは v5 の stats だけを集計

            if result_map and top_hno in result_map:
                finish = result_map[top_hno]
                stats["v5"][cat][v5_level]["total"] += 1
                if finish == 1:
                    stats["v5"][cat][v5_level]["honmei_win"] += 1
                if finish <= 3:
                    stats["v5"][cat][v5_level]["honmei_place3"] += 1

    # ── 結果表示 ──
    print("\n" + "=" * 70)
    print("比較結果")
    print("=" * 70)

    for cat_label, cat in [("JRA", "jra"), ("NAR", "nar")]:
        print(f"\n{'─' * 35}")
        print(f"  {cat_label}")
        print(f"{'─' * 35}")
        print(f"{'レベル':>6} │ {'v4 レース数':>10} {'勝率':>7} {'複勝率':>7} │ {'v5 レース数':>10} {'勝率':>7} {'複勝率':>7}")
        print(f"{'─' * 6}─┼{'─' * 28}─┼{'─' * 28}")

        v4_total_all = sum(stats["v4"][cat][l]["total"] for l in LEVELS)
        v5_total_all = sum(stats["v5"][cat][l]["total"] for l in LEVELS)

        for level in LEVELS:
            v4d = stats["v4"][cat][level]
            v5d = stats["v5"][cat][level]

            v4_total = v4d["total"]
            v4_wr = v4d["honmei_win"] / v4_total * 100 if v4_total > 0 else 0
            v4_p3r = v4d["honmei_place3"] / v4_total * 100 if v4_total > 0 else 0
            v4_pct = v4_total / v4_total_all * 100 if v4_total_all > 0 else 0

            v5_total = v5d["total"]
            v5_wr = v5d["honmei_win"] / v5_total * 100 if v5_total > 0 else 0
            v5_p3r = v5d["honmei_place3"] / v5_total * 100 if v5_total > 0 else 0
            v5_pct = v5_total / v5_total_all * 100 if v5_total_all > 0 else 0

            print(f"{level:>6} │ {v4_total:>5}({v4_pct:>4.1f}%) {v4_wr:>6.1f}% {v4_p3r:>6.1f}% │ {v5_total:>5}({v5_pct:>4.1f}%) {v5_wr:>6.1f}% {v5_p3r:>6.1f}%")

        print(f"{'合計':>6} │ {v4_total_all:>5}        {'':>7} {'':>7} │ {v5_total_all:>5}        {'':>7} {'':>7}")

    # ── v5スコア分布 ──
    print(f"\n{'─' * 35}")
    print("v5 スコア分布統計")
    print(f"{'─' * 35}")
    for label, arr in [("JRA", v5_jra_arr), ("NAR", v5_nar_arr)]:
        print(f"  {label}: mean={arr.mean():.4f}, std={arr.std():.4f}, "
              f"min={arr.min():.4f}, max={arr.max():.4f}")
        print(f"       p5={np.percentile(arr,5):.4f}, p25={np.percentile(arr,25):.4f}, "
              f"p50={np.percentile(arr,50):.4f}, p75={np.percentile(arr,75):.4f}, "
              f"p95={np.percentile(arr,95):.4f}")

    # ── v5 ゲート効果分析 ──
    print(f"\n{'─' * 35}")
    print("v5 SS/Sゲート効果分析")
    print(f"{'─' * 35}")

    for cat_label, scores_list, th, cat in [
        ("JRA", v5_scores_jra, v5_th_jra, "jra"),
        ("NAR", v5_scores_nar, v5_th_nar, "nar"),
    ]:
        is_jra = (cat == "jra")
        # ゲート前SS（スコアのみ）vs ゲート後SS
        pre_gate_ss = {"total": 0, "win": 0, "p3": 0}
        post_gate_ss = {"total": 0, "win": 0, "p3": 0}
        pre_gate_s = {"total": 0, "win": 0, "p3": 0}
        post_gate_s = {"total": 0, "win": 0, "p3": 0}

        for v5_score, top_wp, top_gap, race_id, top_hno, result_map in scores_list:
            if not result_map or top_hno not in result_map:
                continue
            finish = result_map[top_hno]

            # ゲート前レベル（スコアのみ）
            if v5_score >= th["SS"]:
                pre_gate_ss["total"] += 1
                if finish == 1: pre_gate_ss["win"] += 1
                if finish <= 3: pre_gate_ss["p3"] += 1

                # ゲート後
                if top_wp >= V5_WIN_PROB_GATE_SS and top_gap >= V5_GAP_GATE_SS:
                    post_gate_ss["total"] += 1
                    if finish == 1: post_gate_ss["win"] += 1
                    if finish <= 3: post_gate_ss["p3"] += 1

            elif v5_score >= th["S"]:
                pre_gate_s["total"] += 1
                if finish == 1: pre_gate_s["win"] += 1
                if finish <= 3: pre_gate_s["p3"] += 1

                if top_wp >= V5_WIN_PROB_GATE_S and top_gap >= V5_GAP_GATE_S:
                    post_gate_s["total"] += 1
                    if finish == 1: post_gate_s["win"] += 1
                    if finish <= 3: post_gate_s["p3"] += 1

        print(f"\n  {cat_label}:")
        for label, pre, post in [("SS", pre_gate_ss, post_gate_ss), ("S", pre_gate_s, post_gate_s)]:
            pre_wr = pre["win"] / pre["total"] * 100 if pre["total"] > 0 else 0
            pre_p3 = pre["p3"] / pre["total"] * 100 if pre["total"] > 0 else 0
            post_wr = post["win"] / post["total"] * 100 if post["total"] > 0 else 0
            post_p3 = post["p3"] / post["total"] * 100 if post["total"] > 0 else 0
            print(f"    {label} ゲート前: {pre['total']:>5}件 勝率{pre_wr:>5.1f}% 複勝{pre_p3:>5.1f}%")
            print(f"    {label} ゲート後: {post['total']:>5}件 勝率{post_wr:>5.1f}% 複勝{post_p3:>5.1f}%")
            if pre["total"] > 0 and post["total"] > 0:
                removed = pre["total"] - post["total"]
                print(f"       → {removed}件除外 (勝率 {pre_wr:.1f}%→{post_wr:.1f}%)")

    # ── v5ゲート閾値感度分析 ──
    print(f"\n{'─' * 35}")
    print("v5 SSゲート閾値感度分析")
    print(f"{'─' * 35}")

    wp_candidates = [0.20, 0.25, 0.30, 0.35, 0.40]
    gap_candidates = [3.0, 4.0, 5.0, 6.0, 7.0]

    for cat_label, scores_list, th in [
        ("JRA", v5_scores_jra, v5_th_jra),
        ("NAR", v5_scores_nar, v5_th_nar),
    ]:
        print(f"\n  {cat_label} SS:")
        header = "wp\\gap"
        print(f"  {header:>8}", end="")
        for g in gap_candidates:
            print(f"  gap>={g:.0f}      ", end="")
        print()

        for wp_th in wp_candidates:
            print(f"  wp>={wp_th:.2f}", end="")
            for gap_th in gap_candidates:
                total = 0
                win = 0
                for v5_score, top_wp, top_gap, race_id, top_hno, result_map in scores_list:
                    if v5_score < th["SS"]:
                        continue
                    if top_wp < wp_th or top_gap < gap_th:
                        continue
                    if not result_map or top_hno not in result_map:
                        continue
                    total += 1
                    if result_map[top_hno] == 1:
                        win += 1
                wr = win / total * 100 if total > 0 else 0
                print(f"  {total:>4}件{wr:>5.1f}%", end="")
            print()

    print("\n" + "=" * 70)
    print("シミュレーション完了")
    print("=" * 70)


if __name__ == "__main__":
    main()
