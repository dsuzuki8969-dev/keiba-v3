#!/usr/bin/env python3
"""
ペース予測精度の検証スクリプト

予測値(data/predictions/*.json) と 実績値(data/ml/*.json) を突き合わせ、
4指標の精度を分析軸ごとに評価する。

4指標:
  1. ペースタイプ (HH/HM/MM/MS/SS vs H/M/S) — 一致率・混同行列
  2. 走破タイム (predicted_race_time vs 1着finish_time_sec) — MAE/RMSE/R2
  3. 前半3F (estimated_front_3f vs first_3f) — MAE/RMSE/R2
  4. 後半3F (estimated_last_3f vs 勝ち馬last_3f_sec) — MAE/RMSE/R2

分析軸:
  - JRA/NAR別
  - 芝/ダート別
  - 距離帯別 (sprint/mile/middle/long)
  - ペースタイプ別（実績ペースごとの予測精度）
  - 展開精度別（S/A/B/Cと実際の予測誤差の相関）
"""

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# プロジェクトルート
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

PRED_DIR = ROOT / "data" / "predictions"
ML_DIR = ROOT / "data" / "ml"


def parse_args():
    parser = argparse.ArgumentParser(description="ペース予測精度検証")
    parser.add_argument("--start", default="2026-01-01", help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-03-22", help="終了日 (YYYY-MM-DD)")
    return parser.parse_args()


def date_range(start_str, end_str):
    """日付範囲をYYYYMMDD文字列のリストとして返す"""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return dates


def load_json(path):
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def pace_predicted_to_3class(pace_pred):
    """予測ペース(H/M/S) → 3クラス(H/M/S)に変換"""
    if pace_pred in ("H", "M", "S"):
        return pace_pred
    return None


def distance_band(dist):
    """距離→距離帯"""
    if dist <= 1400:
        return "sprint"
    elif dist <= 1600:
        return "mile"
    elif dist <= 2200:
        return "middle"
    else:
        return "long"


def calc_stats(errors):
    """MAE, RMSE, R2を計算"""
    if not errors:
        return {"n": 0, "mae": None, "rmse": None, "r2": None}
    n = len(errors)
    mae = sum(abs(e) for e in errors) / n
    rmse = math.sqrt(sum(e * e for e in errors) / n)
    return {"n": n, "mae": round(mae, 3), "rmse": round(rmse, 3)}


def calc_r2(predicted, actual):
    """R2を計算"""
    if len(predicted) < 3:
        return None
    mean_actual = sum(actual) / len(actual)
    ss_res = sum((a - p) ** 2 for a, p in zip(actual, predicted))
    ss_tot = sum((a - mean_actual) ** 2 for a in actual)
    if ss_tot == 0:
        return None
    return round(1 - ss_res / ss_tot, 4)


def collect_data(dates):
    """予測と実績のペアデータを収集"""
    records = []

    for date_str in dates:
        pred_path = PRED_DIR / f"{date_str}_pred.json"
        ml_path = ML_DIR / f"{date_str}.json"

        pred_data = load_json(pred_path)
        ml_data = load_json(ml_path)
        if not pred_data or not ml_data:
            continue

        # 実績をrace_idでインデックス化
        ml_races = {}
        for r in ml_data.get("races", []):
            ml_races[r["race_id"]] = r

        for pred_race in pred_data.get("races", []):
            race_id = pred_race.get("race_id", "")
            ml_race = ml_races.get(race_id)
            if not ml_race:
                continue

            # 予測値
            pace_pred = pred_race.get("pace_predicted")  # HH/HM/MM/MS/SS
            front_3f_pred = pred_race.get("estimated_front_3f")
            last_3f_pred = pred_race.get("estimated_last_3f")
            time_pred = pred_race.get("predicted_race_time")
            reliability = pred_race.get("pace_reliability_label", "")

            # 実績値
            pace_actual = ml_race.get("pace")  # H/M/S
            front_3f_actual = ml_race.get("first_3f")

            # 1着馬の走破タイムと上がり3F
            horses = ml_race.get("horses", [])
            winner = None
            for h in horses:
                fp = h.get("finish_pos")
                if fp == 1 or fp == "1":
                    winner = h
                    break
            if not winner:
                # finish_posがない場合、finish_time_secが最小の馬
                valid = [h for h in horses if h.get("finish_time_sec") and h["finish_time_sec"] > 0]
                if valid:
                    winner = min(valid, key=lambda h: h["finish_time_sec"])

            time_actual = winner.get("finish_time_sec") if winner else None
            last_3f_actual = winner.get("last_3f_sec") if winner else None

            # レース属性
            is_jra = ml_race.get("is_jra", False)
            surface = ml_race.get("surface", "")
            distance = ml_race.get("distance", 0)
            venue = ml_race.get("venue", "")

            rec = {
                "race_id": race_id,
                "date": date_str,
                "is_jra": is_jra,
                "surface": surface,
                "distance": distance,
                "dist_band": distance_band(distance),
                "venue": venue,
                "reliability": reliability,
                # 予測
                "pace_pred": pace_pred,
                "pace_pred_3c": pace_predicted_to_3class(pace_pred) if pace_pred else None,
                "front_3f_pred": front_3f_pred,
                "last_3f_pred": last_3f_pred,
                "time_pred": time_pred,
                # 実績
                "pace_actual": pace_actual,
                "front_3f_actual": front_3f_actual,
                "last_3f_actual": last_3f_actual,
                "time_actual": time_actual,
            }
            records.append(rec)

    return records


def analyze_pace_type(records):
    """ペースタイプの一致率・混同行列"""
    print("\n" + "=" * 70)
    print("【1】ペースタイプ予測精度（予測5分類 → 実績3分類に統合して比較）")
    print("=" * 70)

    valid = [r for r in records if r["pace_pred_3c"] and r["pace_actual"]]
    if not valid:
        print("  データなし")
        return

    # 全体一致率
    match = sum(1 for r in valid if r["pace_pred_3c"] == r["pace_actual"])
    total = len(valid)
    print(f"\n  全体: {match}/{total} = {match/total*100:.1f}%")

    # 混同行列
    labels = ["H", "M", "S"]
    matrix = defaultdict(lambda: defaultdict(int))
    for r in valid:
        matrix[r["pace_pred_3c"]][r["pace_actual"]] += 1

    print(f"\n  {'':>12} {'実績H':>8} {'実績M':>8} {'実績S':>8} {'計':>8} {'精度':>8}")
    print("  " + "-" * 56)
    for pred_label in labels:
        row_total = sum(matrix[pred_label][a] for a in labels)
        correct = matrix[pred_label][pred_label]
        acc = f"{correct/row_total*100:.1f}%" if row_total > 0 else "-"
        row = f"  予測{pred_label:>2}  "
        for act_label in labels:
            row += f"{matrix[pred_label][act_label]:>8}"
        row += f"{row_total:>8}{acc:>8}"
        print(row)

    # 分析軸ごと
    for axis_name, axis_key in [("JRA/NAR", "org"), ("芝/ダート", "surface"), ("距離帯", "dist_band")]:
        print(f"\n  ── {axis_name}別 ──")
        groups = defaultdict(list)
        for r in valid:
            if axis_key == "org":
                g = "JRA" if r["is_jra"] else "NAR"
            elif axis_key == "surface":
                g = r["surface"]
            else:
                g = r["dist_band"]
            groups[g].append(r)

        for g in sorted(groups.keys()):
            recs = groups[g]
            m = sum(1 for r in recs if r["pace_pred_3c"] == r["pace_actual"])
            t = len(recs)
            print(f"    {g:>8}: {m}/{t} = {m/t*100:.1f}%")

    # 展開精度別
    print(f"\n  ── 展開精度別 ──")
    rel_groups = defaultdict(list)
    for r in valid:
        rel = r["reliability"] if r["reliability"] else "不明"
        rel_groups[rel].append(r)
    for rel in ["S", "A", "B", "C", "D", "不明"]:
        if rel not in rel_groups:
            continue
        recs = rel_groups[rel]
        m = sum(1 for r in recs if r["pace_pred_3c"] == r["pace_actual"])
        t = len(recs)
        print(f"    精度{rel:>2}: {m}/{t} = {m/t*100:.1f}%")


def analyze_numeric(records, pred_key, actual_key, label, unit="秒"):
    """数値予測の精度分析 (MAE/RMSE/R2)"""
    print(f"\n{'=' * 70}")
    print(f"【{label}】")
    print("=" * 70)

    valid = [r for r in records
             if r[pred_key] is not None and r[actual_key] is not None
             and r[pred_key] > 0 and r[actual_key] > 0]
    if not valid:
        print("  データなし")
        return

    errors = [r[pred_key] - r[actual_key] for r in valid]
    preds = [r[pred_key] for r in valid]
    actuals = [r[actual_key] for r in valid]

    stats = calc_stats(errors)
    r2 = calc_r2(preds, actuals)
    mean_err = sum(errors) / len(errors)

    print(f"\n  全体 (n={stats['n']}):")
    print(f"    MAE  = {stats['mae']:.3f}{unit}")
    print(f"    RMSE = {stats['rmse']:.3f}{unit}")
    print(f"    R2   = {r2}")
    print(f"    平均誤差 = {mean_err:+.3f}{unit} ({'予測が大きい' if mean_err > 0 else '予測が小さい'})")

    # 誤差分布
    bins = [0, 0.5, 1.0, 2.0, 3.0, 5.0, float("inf")]
    bin_labels = ["<0.5", "0.5-1.0", "1.0-2.0", "2.0-3.0", "3.0-5.0", "5.0+"]
    print(f"\n  誤差分布:")
    for i, lbl in enumerate(bin_labels):
        cnt = sum(1 for e in errors if bins[i] <= abs(e) < bins[i + 1])
        pct = cnt / len(errors) * 100
        bar = "#" * int(pct / 2)
        print(f"    {lbl:>8}{unit}: {cnt:>5} ({pct:>5.1f}%) {bar}")

    # 分析軸ごと
    for axis_name, axis_fn in [
        ("JRA/NAR", lambda r: "JRA" if r["is_jra"] else "NAR"),
        ("芝/ダート", lambda r: r["surface"]),
        ("距離帯", lambda r: r["dist_band"]),
    ]:
        print(f"\n  ── {axis_name}別 ──")
        groups = defaultdict(list)
        for r in valid:
            groups[axis_fn(r)].append(r)

        print(f"    {'':>10} {'n':>6} {'MAE':>8} {'RMSE':>8} {'R2':>8} {'平均誤差':>10}")
        print("    " + "-" * 54)
        for g in sorted(groups.keys()):
            recs = groups[g]
            errs = [r[pred_key] - r[actual_key] for r in recs]
            ps = [r[pred_key] for r in recs]
            acs = [r[actual_key] for r in recs]
            s = calc_stats(errs)
            r2g = calc_r2(ps, acs)
            me = sum(errs) / len(errs)
            r2_str = f"{r2g:.4f}" if r2g is not None else "N/A"
            print(f"    {g:>10} {s['n']:>6} {s['mae']:>8.3f} {s['rmse']:>8.3f} {r2_str:>8} {me:>+10.3f}")

    # 展開精度別
    print(f"\n  ── 展開精度別 ──")
    rel_groups = defaultdict(list)
    for r in valid:
        rel = r["reliability"] if r["reliability"] else "不明"
        rel_groups[rel].append(r)

    print(f"    {'精度':>6} {'n':>6} {'MAE':>8} {'RMSE':>8} {'平均誤差':>10}")
    print("    " + "-" * 42)
    for rel in ["S", "A", "B", "C", "D", "不明"]:
        if rel not in rel_groups:
            continue
        recs = rel_groups[rel]
        errs = [r[pred_key] - r[actual_key] for r in recs]
        s = calc_stats(errs)
        me = sum(errs) / len(errs)
        print(f"    {rel:>6} {s['n']:>6} {s['mae']:>8.3f} {s['rmse']:>8.3f} {me:>+10.3f}")

    # 実績ペース別
    print(f"\n  ── 実績ペース別 ──")
    pace_groups = defaultdict(list)
    for r in valid:
        p = r["pace_actual"] if r["pace_actual"] else "不明"
        pace_groups[p].append(r)

    print(f"    {'ペース':>6} {'n':>6} {'MAE':>8} {'RMSE':>8} {'平均誤差':>10}")
    print("    " + "-" * 42)
    for p in ["H", "M", "S", "不明"]:
        if p not in pace_groups:
            continue
        recs = pace_groups[p]
        errs = [r[pred_key] - r[actual_key] for r in recs]
        s = calc_stats(errs)
        me = sum(errs) / len(errs)
        print(f"    {p:>6} {s['n']:>6} {s['mae']:>8.3f} {s['rmse']:>8.3f} {me:>+10.3f}")


def main():
    args = parse_args()
    dates = date_range(args.start, args.end)
    print(f"ペース予測精度検証: {args.start} ～ {args.end} ({len(dates)}日)")

    records = collect_data(dates)
    print(f"対象レース数: {len(records)}")

    if not records:
        print("データなし。終了。")
        return

    # JRA/NAR内訳
    jra_n = sum(1 for r in records if r["is_jra"])
    nar_n = len(records) - jra_n
    print(f"  JRA: {jra_n}R / NAR: {nar_n}R")

    # 1. ペースタイプ
    analyze_pace_type(records)

    # 2. 走破タイム
    analyze_numeric(records, "time_pred", "time_actual", "2】走破タイム予測精度")

    # 3. 前半3F
    analyze_numeric(records, "front_3f_pred", "front_3f_actual", "3】前半3F予測精度")

    # 4. 後半3F
    analyze_numeric(records, "last_3f_pred", "last_3f_actual", "4】後半3F予測精度")

    # サマリー
    print(f"\n{'=' * 70}")
    print("【総合サマリー】")
    print("=" * 70)

    # ペースタイプ
    valid_pace = [r for r in records if r["pace_pred_3c"] and r["pace_actual"]]
    if valid_pace:
        m = sum(1 for r in valid_pace if r["pace_pred_3c"] == r["pace_actual"])
        print(f"  ペースタイプ一致率: {m}/{len(valid_pace)} = {m/len(valid_pace)*100:.1f}%")

    for pred_key, actual_key, lbl in [
        ("time_pred", "time_actual", "走破タイム"),
        ("front_3f_pred", "front_3f_actual", "前半3F"),
        ("last_3f_pred", "last_3f_actual", "後半3F"),
    ]:
        valid = [r for r in records
                 if r[pred_key] is not None and r[actual_key] is not None
                 and r[pred_key] > 0 and r[actual_key] > 0]
        if valid:
            errors = [r[pred_key] - r[actual_key] for r in valid]
            preds = [r[pred_key] for r in valid]
            actuals = [r[actual_key] for r in valid]
            s = calc_stats(errors)
            r2 = calc_r2(preds, actuals)
            print(f"  {lbl}: MAE={s['mae']:.3f}秒 RMSE={s['rmse']:.3f}秒 R2={r2}")


if __name__ == "__main__":
    main()
