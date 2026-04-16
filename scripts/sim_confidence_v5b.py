#!/usr/bin/env python
"""
自信度スコア v5b シミュレーション（改良版）

v4（現行）vs v5（market-free）比較
JRA/NAR別ゲート閾値の感度分析を強化。
プログレスバー付き。
"""

import io
import json
import glob
import os
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8',
                              line_buffering=True, errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8',
                              line_buffering=True, errors='replace')

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "keiba.db"
PRED_DIR = ROOT / "data" / "predictions"

# ── v4 閾値（現行） ──
V4_THRESHOLDS_JRA = {"SS": 0.641, "S": 0.537, "A": 0.438, "B": 0.257, "C": 0.134}
V4_THRESHOLDS_NAR = {"SS": 0.761, "S": 0.673, "A": 0.585, "B": 0.390, "C": 0.224}
V4_POP_GATE_SS_JRA = 1
V4_POP_GATE_SS_NAR = 1
V4_POP_GATE_S_JRA = 2
V4_POP_GATE_S_NAR = 2

# ── v5 ゲート候補（JRA/NAR別） ──
V5_GATE_CONFIGS = {
    "v5a_現提案": {
        "jra_ss_wp": 0.30, "jra_ss_gap": 5.0,
        "jra_s_wp": 0.22, "jra_s_gap": 3.0,
        "nar_ss_wp": 0.30, "nar_ss_gap": 5.0,
        "nar_s_wp": 0.22, "nar_s_gap": 3.0,
    },
    "v5b_NAR厳格": {
        "jra_ss_wp": 0.25, "jra_ss_gap": 6.0,
        "jra_s_wp": 0.20, "jra_s_gap": 3.0,
        "nar_ss_wp": 0.35, "nar_ss_gap": 7.0,
        "nar_s_wp": 0.25, "nar_s_gap": 5.0,
    },
    "v5c_両方厳格": {
        "jra_ss_wp": 0.30, "jra_ss_gap": 6.0,
        "jra_s_wp": 0.22, "jra_s_gap": 4.0,
        "nar_ss_wp": 0.35, "nar_ss_gap": 7.0,
        "nar_s_wp": 0.28, "nar_s_gap": 5.0,
    },
    "v5d_超厳格": {
        "jra_ss_wp": 0.30, "jra_ss_gap": 7.0,
        "jra_s_wp": 0.25, "jra_s_gap": 5.0,
        "nar_ss_wp": 0.40, "nar_ss_gap": 7.0,
        "nar_s_wp": 0.30, "nar_s_gap": 5.0,
    },
}

GAP_DIVISOR_JRA = 4.0
GAP_DIVISOR_NAR = 8.0
LEVELS = ["SS", "S", "A", "B", "C", "D"]


def progress_bar(current, total, start_time, prefix=""):
    elapsed = time.time() - start_time
    pct = current / total * 100 if total > 0 else 0
    if current > 0:
        eta = elapsed / current * (total - current)
        eta_str = f"{eta:.0f}秒"
    else:
        eta_str = "計算中"
    bar_len = 30
    filled = int(bar_len * current / total) if total > 0 else 0
    bar = "█" * filled + "░" * (bar_len - filled)
    print(f"\r  {prefix} |{bar}| {pct:5.1f}% ({current}/{total}) 経過{elapsed:.0f}秒 残り{eta_str}  ", end="", flush=True)
    if current == total:
        print()


def load_results(db_path):
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


def calc_v5_score(horses, is_jra):
    """v5スコア: 6信号（market-free）"""
    sorted_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    if len(sorted_comp) < 3:
        return 1.0 if len(sorted_comp) >= 1 else 0.0

    top = sorted_comp[0]
    top_id = top.get("horse_id", "")

    gap = sorted_comp[0]["composite"] - sorted_comp[1]["composite"]
    gap_div = GAP_DIVISOR_JRA if is_jra else GAP_DIVISOR_NAR
    gap_norm = min(gap / gap_div, 1.0)

    sorted_wp = sorted(horses, key=lambda h: h.get("win_prob", 0), reverse=True)
    wp_top_id = sorted_wp[0].get("horse_id", "")
    if wp_top_id == top_id:
        ml_agreement = 1.0
    elif len(sorted_wp) >= 2 and sorted_wp[1].get("horse_id", "") == top_id:
        ml_agreement = 0.5
    else:
        ml_agreement = 0.0

    gap23 = sorted_comp[1]["composite"] - sorted_comp[2]["composite"]
    gap23_norm = min(gap23 / 4.0, 1.0)

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

    rel_count = 0
    for ev in sorted_comp[:3]:
        rel = ev.get("ability_reliability", "")
        if rel == "A":
            rel_count += 1
    reliability_norm = rel_count / 3.0

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


def calc_v4_score(horses, is_jra):
    """v4スコア: 7信号（value_score含む）"""
    sorted_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    if len(sorted_comp) < 3:
        return 1.0 if len(sorted_comp) >= 1 else 0.0

    top = sorted_comp[0]
    top_id = top.get("horse_id", "")

    gap = sorted_comp[0]["composite"] - sorted_comp[1]["composite"]
    gap_div = GAP_DIVISOR_JRA if is_jra else GAP_DIVISOR_NAR
    gap_norm = min(gap / gap_div, 1.0)

    sorted_wp = sorted(horses, key=lambda h: h.get("win_prob", 0), reverse=True)
    wp_top_id = sorted_wp[0].get("horse_id", "")
    if wp_top_id == top_id:
        ml_agreement = 1.0
    elif len(sorted_wp) >= 2 and sorted_wp[1].get("horse_id", "") == top_id:
        ml_agreement = 0.5
    else:
        ml_agreement = 0.0

    gap23 = sorted_comp[1]["composite"] - sorted_comp[2]["composite"]
    gap23_norm = min(gap23 / 4.0, 1.0)

    odds = top.get("odds") or 0
    wp = top.get("win_prob") or 0
    if odds > 1.0 and wp > 0:
        odds_implied = 1.0 / odds
        vr = wp / odds_implied
        value_score = min(max((vr - 1.0) / 0.5, 0.0), 1.0)
    else:
        value_score = 0.5

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

    rel_count = 0
    for ev in sorted_comp[:3]:
        rel = ev.get("ability_reliability", "")
        if rel == "A":
            rel_count += 1
    reliability_norm = rel_count / 3.0

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


def assign_level_v4(score, top_pop, is_jra):
    th = V4_THRESHOLDS_JRA if is_jra else V4_THRESHOLDS_NAR
    pg_ss = V4_POP_GATE_SS_JRA if is_jra else V4_POP_GATE_SS_NAR
    pg_s = V4_POP_GATE_S_JRA if is_jra else V4_POP_GATE_S_NAR

    if score >= th["SS"]: level = "SS"
    elif score >= th["S"]: level = "S"
    elif score >= th["A"]: level = "A"
    elif score >= th["B"]: level = "B"
    elif score >= th["C"]: level = "C"
    else: level = "D"

    if level == "SS" and (top_pop or 99) > pg_ss:
        level = "S"
    if level == "S" and (top_pop or 99) > pg_s:
        level = "A"
    return level


def assign_level_v5(score, top_wp, top_gap, is_jra, thresholds, gate_cfg):
    if score >= thresholds["SS"]: level = "SS"
    elif score >= thresholds["S"]: level = "S"
    elif score >= thresholds["A"]: level = "A"
    elif score >= thresholds["B"]: level = "B"
    elif score >= thresholds["C"]: level = "C"
    else: level = "D"

    prefix = "jra" if is_jra else "nar"
    ss_wp = gate_cfg[f"{prefix}_ss_wp"]
    ss_gap = gate_cfg[f"{prefix}_ss_gap"]
    s_wp = gate_cfg[f"{prefix}_s_wp"]
    s_gap = gate_cfg[f"{prefix}_s_gap"]

    if level == "SS" and (top_wp < ss_wp or top_gap < ss_gap):
        level = "S"
    if level == "S" and (top_wp < s_wp or top_gap < s_gap):
        level = "A"
    return level


def main():
    print("=" * 80)
    print("自信度スコア v5b シミュレーション（JRA/NAR別ゲート比較）")
    print("=" * 80)

    # 1. 着順データ
    t0 = time.time()
    print("\n[1/4] 着順データ読み込み中...")
    results = load_results(str(DB_PATH))
    print(f"  完了: {len(results)} レース ({time.time()-t0:.1f}秒)")

    # 2. 全pred.json読み込み
    pred_files = sorted(glob.glob(str(PRED_DIR / "*_pred.json")))
    pred_files = [f for f in pred_files if "_prev" not in f and "_backup" not in f]
    total_files = len(pred_files)
    print(f"\n[2/4] pred.json読み込み + スコア計算中... ({total_files} ファイル)")

    # レースデータ保存
    race_data = []  # [(is_jra, v4_score, v5_score, top_wp, top_gap, top_pop, top_hno, result_map)]

    t1 = time.time()
    for fi, fn in enumerate(pred_files):
        progress_bar(fi + 1, total_files, t1, prefix="読込")
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
            result_map = results.get(race_id, {})

            sorted_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
            top = sorted_comp[0]
            top_hno = top.get("horse_no", 0)
            top_pop = top.get("popularity") or 99
            top_wp = top.get("win_prob") or 0
            top_gap = sorted_comp[0].get("composite", 0) - sorted_comp[1].get("composite", 0)

            v4_score = calc_v4_score(horses, is_jra)
            v5_score = calc_v5_score(horses, is_jra)

            race_data.append((is_jra, v4_score, v5_score, top_wp, top_gap, top_pop, top_hno, result_map))

    print(f"  合計: {len(race_data)} レース")

    # 3. v5パーセンタイル閾値算出
    print("\n[3/4] v5パーセンタイル閾値算出中...")
    import numpy as np

    jra_scores = np.array([d[2] for d in race_data if d[0]])
    nar_scores = np.array([d[2] for d in race_data if not d[0]])

    v5_th_jra = {
        "SS": float(np.percentile(jra_scores, 95)),
        "S":  float(np.percentile(jra_scores, 85)),
        "A":  float(np.percentile(jra_scores, 70)),
        "B":  float(np.percentile(jra_scores, 35)),
        "C":  float(np.percentile(jra_scores, 10)),
    }
    v5_th_nar = {
        "SS": float(np.percentile(nar_scores, 95)),
        "S":  float(np.percentile(nar_scores, 85)),
        "A":  float(np.percentile(nar_scores, 70)),
        "B":  float(np.percentile(nar_scores, 35)),
        "C":  float(np.percentile(nar_scores, 10)),
    }
    print(f"  JRA: {json.dumps({k: round(v, 4) for k, v in v5_th_jra.items()})}")
    print(f"  NAR: {json.dumps({k: round(v, 4) for k, v in v5_th_nar.items()})}")

    # 4. 各ゲート設定でレベル割当・集計
    print(f"\n[4/4] ゲート設定 {len(V5_GATE_CONFIGS)} パターン比較中...")

    # v4集計
    v4_stats = {"jra": defaultdict(lambda: {"t": 0, "w": 0, "p3": 0}),
                "nar": defaultdict(lambda: {"t": 0, "w": 0, "p3": 0})}

    t4 = time.time()
    total_races = len(race_data)

    for i, (is_jra, v4_score, v5_score, top_wp, top_gap, top_pop, top_hno, result_map) in enumerate(race_data):
        if (i + 1) % 5000 == 0 or i + 1 == total_races:
            progress_bar(i + 1, total_races, t4, prefix="集計")

        cat = "jra" if is_jra else "nar"

        if result_map and top_hno in result_map:
            finish = result_map[top_hno]
            v4_level = assign_level_v4(v4_score, top_pop, is_jra)
            v4_stats[cat][v4_level]["t"] += 1
            if finish == 1: v4_stats[cat][v4_level]["w"] += 1
            if finish <= 3: v4_stats[cat][v4_level]["p3"] += 1

    # v5各設定で集計
    v5_all_stats = {}
    for cfg_name, gate_cfg in V5_GATE_CONFIGS.items():
        stats = {"jra": defaultdict(lambda: {"t": 0, "w": 0, "p3": 0}),
                 "nar": defaultdict(lambda: {"t": 0, "w": 0, "p3": 0})}

        for is_jra, v4_score, v5_score, top_wp, top_gap, top_pop, top_hno, result_map in race_data:
            if not result_map or top_hno not in result_map:
                continue
            cat = "jra" if is_jra else "nar"
            finish = result_map[top_hno]
            th = v5_th_jra if is_jra else v5_th_nar
            level = assign_level_v5(v5_score, top_wp, top_gap, is_jra, th, gate_cfg)
            stats[cat][level]["t"] += 1
            if finish == 1: stats[cat][level]["w"] += 1
            if finish <= 3: stats[cat][level]["p3"] += 1
        v5_all_stats[cfg_name] = stats

    # ── 結果表示 ──
    print("\n\n" + "=" * 80)
    print("比較結果（結果データあるレースのみ）")
    print("=" * 80)

    for cat_label, cat in [("JRA", "jra"), ("NAR", "nar")]:
        print(f"\n{'━' * 80}")
        print(f"  {cat_label}")
        print(f"{'━' * 80}")

        # ヘッダー
        header = f"{'レベル':>6} │ {'v4(現行)':^25}"
        for cfg_name in V5_GATE_CONFIGS:
            short = cfg_name.split("_", 1)[1] if "_" in cfg_name else cfg_name
            header += f" │ {short:^25}"
        print(header)
        print("─" * len(header.encode('ascii', 'replace')))

        v4_total_all = sum(v4_stats[cat][l]["t"] for l in LEVELS)

        for level in LEVELS:
            v4d = v4_stats[cat][level]
            v4_t = v4d["t"]
            v4_pct = v4_t / v4_total_all * 100 if v4_total_all else 0
            v4_wr = v4d["w"] / v4_t * 100 if v4_t else 0
            v4_p3 = v4d["p3"] / v4_t * 100 if v4_t else 0

            line = f"{level:>6} │ {v4_t:>4}({v4_pct:>4.1f}%) W{v4_wr:>4.1f}% P{v4_p3:>4.1f}%"

            for cfg_name in V5_GATE_CONFIGS:
                v5d = v5_all_stats[cfg_name][cat][level]
                v5_t = v5d["t"]
                v5_total = sum(v5_all_stats[cfg_name][cat][l]["t"] for l in LEVELS)
                v5_pct = v5_t / v5_total * 100 if v5_total else 0
                v5_wr = v5d["w"] / v5_t * 100 if v5_t else 0
                v5_p3 = v5d["p3"] / v5_t * 100 if v5_t else 0
                line += f" │ {v5_t:>4}({v5_pct:>4.1f}%) W{v5_wr:>4.1f}% P{v5_p3:>4.1f}%"
            print(line)

        # 合計行
        line = f"{'合計':>6} │ {v4_total_all:>4}"
        for cfg_name in V5_GATE_CONFIGS:
            total = sum(v5_all_stats[cfg_name][cat][l]["t"] for l in LEVELS)
            line += f"{'':>25} │ {total:>4}"
        print(line)

    # ── SS/S序列チェック ──
    print(f"\n{'━' * 80}")
    print("SS/S序列チェック（SS勝率 > S勝率 > A勝率 であるべき）")
    print(f"{'━' * 80}")
    for cat_label, cat in [("JRA", "jra"), ("NAR", "nar")]:
        print(f"\n  {cat_label}:")
        # v4
        v4_wrs = {}
        for l in ["SS", "S", "A"]:
            d = v4_stats[cat][l]
            v4_wrs[l] = d["w"] / d["t"] * 100 if d["t"] > 0 else 0
        ok = "✅" if v4_wrs["SS"] > v4_wrs["S"] > v4_wrs["A"] else "❌"
        print(f"    v4(現行): SS {v4_wrs['SS']:.1f}% > S {v4_wrs['S']:.1f}% > A {v4_wrs['A']:.1f}% {ok}")

        for cfg_name in V5_GATE_CONFIGS:
            wrs = {}
            for l in ["SS", "S", "A"]:
                d = v5_all_stats[cfg_name][cat][l]
                wrs[l] = d["w"] / d["t"] * 100 if d["t"] > 0 else 0
            ok = "✅" if wrs["SS"] > wrs["S"] > wrs["A"] else "❌"
            short = cfg_name.split("_", 1)[1] if "_" in cfg_name else cfg_name
            print(f"    {short}: SS {wrs['SS']:.1f}% > S {wrs['S']:.1f}% > A {wrs['A']:.1f}% {ok}")

    # ── ゲート設定詳細 ──
    print(f"\n{'━' * 80}")
    print("ゲート設定一覧")
    print(f"{'━' * 80}")
    for cfg_name, cfg in V5_GATE_CONFIGS.items():
        short = cfg_name.split("_", 1)[1] if "_" in cfg_name else cfg_name
        print(f"  {short}:")
        print(f"    JRA SS: wp>={cfg['jra_ss_wp']}, gap>={cfg['jra_ss_gap']}  S: wp>={cfg['jra_s_wp']}, gap>={cfg['jra_s_gap']}")
        print(f"    NAR SS: wp>={cfg['nar_ss_wp']}, gap>={cfg['nar_ss_gap']}  S: wp>={cfg['nar_s_wp']}, gap>={cfg['nar_s_gap']}")

    elapsed = time.time() - t0
    print(f"\n{'=' * 80}")
    print(f"シミュレーション完了（{elapsed:.1f}秒）")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
