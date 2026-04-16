#!/usr/bin/env python
"""
自信度 v5 適用スクリプト

全pred.jsonに対して:
1. v5スコア（6信号、市場フリー）を再計算
2. v5閾値 + win_prob/gapゲートで自信度レベルを再判定
3. confidence / confidence_score / overall_confidence を更新

プログレスバー付き。
"""

import io
import json
import glob
import os
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8',
                              line_buffering=True, errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8',
                              line_buffering=True, errors='replace')

ROOT = Path(__file__).resolve().parent.parent
PRED_DIR = ROOT / "data" / "predictions"

# ── v5 設定（settings.pyと同期） ──
V5_THRESHOLDS_JRA = {"SS": 0.7327, "S": 0.6085, "A": 0.4835, "B": 0.2407, "C": 0.0987}
V5_THRESHOLDS_NAR = {"SS": 0.809, "S": 0.7128, "A": 0.61, "B": 0.3501, "C": 0.1361}

# 両方厳格ゲート
V5_WP_GATE_SS_JRA = 0.30
V5_WP_GATE_SS_NAR = 0.35
V5_GAP_GATE_SS_JRA = 6.0
V5_GAP_GATE_SS_NAR = 7.0
V5_WP_GATE_S_JRA = 0.22
V5_WP_GATE_S_NAR = 0.28
V5_GAP_GATE_S_JRA = 4.0
V5_GAP_GATE_S_NAR = 5.0

GAP_DIVISOR_JRA = 6.0
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
    bar = "\u2588" * filled + "\u2591" * (bar_len - filled)
    print(f"\r  {prefix} |{bar}| {pct:5.1f}% ({current}/{total}) 経過{elapsed:.0f}秒 残り{eta_str}  ", end="", flush=True)
    if current == total:
        print()


def calc_v5_score(horses, is_jra):
    """v5スコア: 6信号（市場フリー）"""
    sorted_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    if len(sorted_comp) < 3:
        return 1.0 if len(sorted_comp) >= 1 else 0.0

    top = sorted_comp[0]
    top_id = top.get("horse_id", "")

    # 1. composite_gap (20%)
    gap = sorted_comp[0].get("composite", 0) - sorted_comp[1].get("composite", 0)
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
    gap23 = sorted_comp[1].get("composite", 0) - sorted_comp[2].get("composite", 0)
    gap23_norm = min(gap23 / 4.0, 1.0)

    # 4. multi_factor (20%)
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

    # 5. reliability (10%)
    rel_count = 0
    for ev in sorted_comp[:3]:
        rel = ev.get("ability_reliability", "")
        if rel == "A":
            rel_count += 1
    reliability_norm = rel_count / 3.0

    # 6. ml_confidence (15%)
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


def assign_level(score, top_wp, top_gap, is_jra):
    """v5レベル判定: パーセンタイル閾値 + win_prob/gapゲート"""
    th = V5_THRESHOLDS_JRA if is_jra else V5_THRESHOLDS_NAR

    if score >= th["SS"]: level = "SS"
    elif score >= th["S"]: level = "S"
    elif score >= th["A"]: level = "A"
    elif score >= th["B"]: level = "B"
    elif score >= th["C"]: level = "C"
    else: level = "D"

    # win_prob/gapゲート
    if is_jra:
        if level == "SS" and (top_wp < V5_WP_GATE_SS_JRA or top_gap < V5_GAP_GATE_SS_JRA):
            level = "S"
        if level == "S" and (top_wp < V5_WP_GATE_S_JRA or top_gap < V5_GAP_GATE_S_JRA):
            level = "A"
    else:
        if level == "SS" and (top_wp < V5_WP_GATE_SS_NAR or top_gap < V5_GAP_GATE_SS_NAR):
            level = "S"
        if level == "S" and (top_wp < V5_WP_GATE_S_NAR or top_gap < V5_GAP_GATE_S_NAR):
            level = "A"

    return level


def main():
    print("=" * 70)
    print("自信度 v5 適用（全pred.json更新）")
    print("  6信号スコア（市場フリー）+ win_prob/gapゲート（両方厳格）")
    print("=" * 70)

    pred_files = sorted(glob.glob(str(PRED_DIR / "*_pred.json")))
    pred_files = [f for f in pred_files if "_prev" not in f and "_backup" not in f]
    total_files = len(pred_files)
    print(f"\n対象: {total_files} ファイル")

    t0 = time.time()
    total_races = 0
    changed_races = 0
    level_changes = {}  # (old, new) -> count
    files_modified = 0

    for fi, fn in enumerate(pred_files):
        progress_bar(fi + 1, total_files, t0, prefix="適用")

        try:
            with open(fn, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        modified = False

        for race in data.get("races", []):
            horses = race.get("horses", [])
            if len(horses) < 3:
                continue

            is_jra = race.get("is_jra", False)
            total_races += 1

            # ◎馬（composite 1位）特定
            sorted_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
            top = sorted_comp[0]
            top_wp = top.get("win_prob") or 0
            top_gap = sorted_comp[0].get("composite", 0) - sorted_comp[1].get("composite", 0)

            # v5スコア計算
            v5_score = calc_v5_score(horses, is_jra)
            v5_level = assign_level(v5_score, top_wp, top_gap, is_jra)

            old_conf = race.get("confidence", "")
            old_score = race.get("confidence_score")

            # 更新
            if old_conf != v5_level or old_score != round(v5_score, 3):
                race["confidence"] = v5_level
                race["confidence_score"] = round(v5_score, 3)
                race["overall_confidence"] = v5_level
                modified = True
                changed_races += 1

                key = (old_conf or "?", v5_level)
                level_changes[key] = level_changes.get(key, 0) + 1

        if modified:
            files_modified += 1
            with open(fn, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    elapsed = time.time() - t0
    print(f"\n\n{'=' * 70}")
    print(f"完了（{elapsed:.1f}秒）")
    print(f"{'=' * 70}")
    print(f"  レース合計: {total_races}")
    print(f"  変更レース: {changed_races}")
    print(f"  変更ファイル: {files_modified} / {total_files}")

    # 変更内訳
    print(f"\n{'─' * 40}")
    print("レベル変更内訳（上位20）:")
    print(f"{'─' * 40}")
    sorted_changes = sorted(level_changes.items(), key=lambda x: -x[1])
    for (old, new), count in sorted_changes[:20]:
        arrow = "→"
        print(f"  {old:>3} {arrow} {new:<3}: {count:>5}件")

    # 最終分布
    print(f"\n{'─' * 40}")
    print("v5最終分布:")
    print(f"{'─' * 40}")
    # 再集計のために1回目のループで集計しておけばよかったが、ここでは簡易的に
    from collections import Counter
    jra_dist = Counter()
    nar_dist = Counter()
    for fn in pred_files:
        try:
            with open(fn, "r", encoding="utf-8") as f:
                data = json.load(f)
            for race in data.get("races", []):
                conf = race.get("confidence", "?")
                if race.get("is_jra", False):
                    jra_dist[conf] += 1
                else:
                    nar_dist[conf] += 1
        except:
            pass

    jra_total = sum(jra_dist.values())
    nar_total = sum(nar_dist.values())
    print(f"  JRA ({jra_total}レース):")
    for level in LEVELS:
        cnt = jra_dist.get(level, 0)
        pct = cnt / jra_total * 100 if jra_total else 0
        print(f"    {level:>3}: {cnt:>5}件 ({pct:>5.1f}%)")

    print(f"  NAR ({nar_total}レース):")
    for level in LEVELS:
        cnt = nar_dist.get(level, 0)
        pct = cnt / nar_total * 100 if nar_total else 0
        print(f"    {level:>3}: {cnt:>5}件 ({pct:>5.1f}%)")


if __name__ == "__main__":
    main()
