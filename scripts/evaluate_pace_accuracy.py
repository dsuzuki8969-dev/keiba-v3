#!/usr/bin/env python
"""
ペース予測精度評価スクリプト (案D)

ML JSON (data/ml/*.json) に記録された first_3f 実測値から
実際のペースタイプを逆算し、PacePredictor の予測との一致率を計算する。

Usage:
  python scripts/evaluate_pace_accuracy.py
  python scripts/evaluate_pace_accuracy.py --min-year 2024
"""

import argparse
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8")

ML_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "ml")
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


# ── first_3f → PaceType 変換テーブル ──────────────────────────────
# 芝・ダートで閾値が異なる（ダートは約1秒遅め）
_PACE_THRESHOLDS = {
    "芝": [
        ("HH", None, 33.8),
        ("HM", 33.8, 34.8),
        ("MM", 34.8, 36.2),
        ("MS", 36.2, 37.2),
        ("SS", 37.2, None),
    ],
    "ダート": [
        ("HH", None, 34.8),
        ("HM", 34.8, 35.8),
        ("MM", 35.8, 37.2),
        ("MS", 37.2, 38.2),
        ("SS", 38.2, None),
    ],
}


def first3f_to_pace(first3f: float, surface: str) -> str:
    """first_3f 実測値から PaceType 文字列を返す"""
    thresholds = _PACE_THRESHOLDS.get(surface, _PACE_THRESHOLDS["芝"])
    for pace, lo, hi in thresholds:
        if lo is None and first3f < hi:
            return pace
        if hi is None and first3f >= lo:
            return pace
        if lo is not None and hi is not None and lo <= first3f < hi:
            return pace
    return "MM"


def load_races(min_year: int = 2020) -> list:
    """ML JSON からレース一覧を読み込む"""
    races = []
    for fname in sorted(os.listdir(ML_DATA_DIR)):
        if not fname.endswith(".json"):
            continue
        try:
            with open(os.path.join(ML_DATA_DIR, fname), encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        for race in (data if isinstance(data, list) else data.get("races", [])):
            date = race.get("date", "")
            if date[:4].isdigit() and int(date[:4]) >= min_year:
                races.append(race)
    races.sort(key=lambda r: r.get("date", ""))
    return races


def evaluate(races: list) -> dict:
    """PacePredictor の予測 vs 実際の first_3f ベースのペースを比較"""
    from data.masters.course_master import ALL_COURSES
    from src.calculator.pace_course import PacePredictor
    from src.models import PastRun, Horse

    predictor = PacePredictor()
    all_courses = {c.course_id: c for c in ALL_COURSES}

    # コース別ペース傾向DB（案A）
    try:
        from src.database import get_course_pace_tendency
        course_pace_tendency = get_course_pace_tendency()
    except Exception:
        course_pace_tendency = {}

    total = 0
    correct = 0
    adjacent = 0  # 隣接PaceType（1段階ずれ）
    confusion = defaultdict(lambda: defaultdict(int))
    by_surface: dict = defaultdict(lambda: {"total": 0, "correct": 0})

    PACE_ORDER = ["HH", "HM", "MM", "MS", "SS"]

    for race in races:
        first3f = race.get("first_3f")
        if first3f is None:
            continue
        surface = race.get("surface", "芝")
        if "ダ" in surface or "dirt" in surface.lower():
            surface_norm = "ダート"
        else:
            surface_norm = "芝"

        actual_pace = first3f_to_pace(first3f, surface_norm)

        # コースマスター取得
        venue_code = str(race.get("venue_code", ""))
        distance = int(race.get("distance", 1600))
        surface_key = "芝" if surface_norm == "芝" else "ダート"
        course_id = f"{venue_code}_{surface_key}_{distance}"
        course = all_courses.get(course_id)
        if course is None:
            # フォールバック: 距離帯だけ合うコースを探す
            for cid, c in all_courses.items():
                if cid.startswith(venue_code + "_"):
                    course = c
                    break
        if course is None:
            continue

        # 出走馬ダミー（past_runs は省略 → ルールベース部分のみ評価）
        horses_dummy = []
        for h in race.get("horses", []):
            dummy = Horse(
                horse_id=h.get("horse_id", ""),
                horse_no=h.get("horse_no", 1),
                gate_no=h.get("gate_no", 1),
                name="",
                age=h.get("age", 4),
                sex=h.get("sex", "牡"),
                weight_kg=h.get("weight_kg", 55.0),
                horse_weight=h.get("horse_weight", 450),
                weight_change=h.get("weight_change", 0),
                jockey_id=h.get("jockey_id", ""),
                trainer_id=h.get("trainer_id", ""),
                past_runs=[],
            )
            horses_dummy.append(dummy)

        if not horses_dummy:
            continue

        past_runs_map = {h.horse_id: [] for h in horses_dummy}

        try:
            result = predictor.predict_pace(
                horses_dummy, past_runs_map, course,
                course_pace_tendency=course_pace_tendency,
            )
            predicted_pace = result[0].value  # PaceType.value = "HH"/"HM" etc.
        except Exception:
            continue

        total += 1
        confusion[actual_pace][predicted_pace] += 1
        by_surface[surface_norm]["total"] += 1

        if predicted_pace == actual_pace:
            correct += 1
            by_surface[surface_norm]["correct"] += 1
        # 隣接（1段階ずれ）
        if abs(PACE_ORDER.index(predicted_pace) - PACE_ORDER.index(actual_pace)) <= 1:
            adjacent += 1

    return {
        "total": total,
        "correct": correct,
        "adjacent": adjacent,
        "accuracy": correct / total if total else 0.0,
        "adjacent_accuracy": adjacent / total if total else 0.0,
        "confusion": {k: dict(v) for k, v in confusion.items()},
        "by_surface": {k: {**v, "accuracy": v["correct"] / v["total"] if v["total"] else 0}
                       for k, v in by_surface.items()},
    }


def print_report(result: dict) -> None:
    PACE_ORDER = ["HH", "HM", "MM", "MS", "SS"]
    print("\n" + "=" * 55)
    print("ペース予測精度レポート")
    print("=" * 55)
    print(f"評価レース数  : {result['total']:,}")
    print(f"完全一致(正解): {result['correct']:,}  ({result['accuracy']:.1%})")
    print(f"隣接許容一致  : {result['adjacent']:,}  ({result['adjacent_accuracy']:.1%})")

    print("\n── 馬場面別 ───────────────────────────────────────")
    for surf, s in result["by_surface"].items():
        print(f"  {surf:4s}: {s['total']:,}レース  正解率 {s['accuracy']:.1%}")

    print("\n── Confusion Matrix (行=実際, 列=予測) ──────────────")
    header = "      " + "  ".join(f"{p:>4s}" for p in PACE_ORDER)
    print(header)
    conf = result["confusion"]
    for actual in PACE_ORDER:
        row_vals = [conf.get(actual, {}).get(pred, 0) for pred in PACE_ORDER]
        row_str = " ".join(f"{v:4d}" for v in row_vals)
        total_row = sum(row_vals)
        if total_row > 0:
            print(f"  {actual:4s}  {row_str}  (n={total_row})")

    print("\n── 改善のヒント ─────────────────────────────────────")
    conf_all = result["confusion"]
    # 最も多く間違えているペアを探す
    errors = []
    for actual, preds in conf_all.items():
        for pred, cnt in preds.items():
            if actual != pred:
                errors.append((cnt, actual, pred))
    errors.sort(reverse=True)
    for cnt, actual, pred in errors[:3]:
        print(f"  実際{actual}→予測{pred}: {cnt}件")
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-year", type=int, default=2022)
    args = parser.parse_args()

    print(f"ML JSON 読み込み中 (min_year={args.min_year})...")
    races = load_races(min_year=args.min_year)
    print(f"  → {len(races):,} レース読み込み完了")

    print("評価実行中...")
    result = evaluate(races)
    print_report(result)


if __name__ == "__main__":
    main()
