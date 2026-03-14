#!/usr/bin/env python
"""
WA_WEIGHTS（加重平均重みベクトル）の Optuna 最適化スクリプト（案2）

目的関数: Walk-Forward CV の Top3命中率（複勝的中率）
最適化対象: WA_WEIGHTS の5要素（合計1.0 に正規化）

Usage:
  python scripts/optimize_wa_weights.py --n-trials 200
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8")

RESULT_PATH = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
    "data", "models", "best_wa_weights.json"
)


def _evaluate_weights(weights: list, races: list, sire_map: dict,
                      feature_columns: list, categorical_features: list,
                      fold_start: str, fold_end: str) -> float:
    """指定フォールド・重みベクトルでTop3命中率を計算"""
    import numpy as np
    from src.ml.lgbm_model import (
        RollingStatsTracker, RollingSireTracker,
        _extract_features, _add_race_relative_features,
    )

    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    hit3 = 0
    total = 0

    for race in races:
        date_str = race.get("date", "")
        if not date_str or date_str >= fold_end:
            break
        is_valid = date_str >= fold_start

        if not is_valid:
            tracker.update_race(race)
            sire_tracker.update_race(race, sire_map)
            continue

        horses = race.get("horses", [])
        if len(horses) < 3:
            continue

        # WA風スコアを手動計算（past_runs の偏差値直近5走に weights 適用）
        # ここでは簡易版: lgbm_model の dev_run1/dev_run2 特徴量の加重平均で代用
        scores = {}
        for h in horses:
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            feat = _extract_features(dict(h, sire_id=sid, bms_id=bid),
                                      race, tracker, sire_tracker)
            # dev_run1..dev_run5 を weights で加重
            devs = [feat.get(f"dev_run{i+1}") for i in range(5)]
            ws = [w for w, d in zip(weights, devs) if d is not None]
            ds = [d for d in devs if d is not None]
            if ds:
                total_w = sum(ws) or 1.0
                score = sum(w * d for w, d in zip(ws, ds)) / total_w
            else:
                score = 0.5
            scores[hid] = (score, fp)

        if not scores:
            continue

        ranked = sorted(scores.values(), key=lambda x: -x[0])
        predicted_top3 = set(list(scores.keys())[:3])
        actual_top3 = {hid for hid, (_, fp) in scores.items() if fp <= 3}
        if not actual_top3:
            continue

        total += 1
        if predicted_top3 & actual_top3:
            hit3 += 1

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    return hit3 / total if total > 0 else 0.0


def objective(trial, races, sire_map, feature_columns, categorical_features, folds):
    # 5要素の相対重みをサジェスト（合計1に正規化）
    raw = [trial.suggest_float(f"w{i}", 0.05, 0.60) for i in range(5)]
    total = sum(raw)
    weights = [w / total for w in raw]

    # 直近2フォールドで評価
    scores = []
    for fold_start, fold_end in folds[-2:]:
        s = _evaluate_weights(weights, races, sire_map,
                               feature_columns, categorical_features,
                               fold_start, fold_end)
        if s > 0:
            scores.append(s)

    return sum(scores) / len(scores) if scores else 0.0


def run_optimization(n_trials: int = 50):
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    from src.ml.lgbm_model import (
        FEATURE_COLUMNS, CATEGORICAL_FEATURES,
        _load_ml_races, _load_horse_sire_map,
    )
    from scripts.walk_forward_cv import _make_folds

    print(f"=== WA_WEIGHTS 最適化開始 ({n_trials} trials) ===")
    t0 = time.time()

    races = _load_ml_races()
    sire_map = _load_horse_sire_map()
    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    folds = _make_folds(all_dates, fold_months=3, min_train_months=9)

    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: objective(trial, races, sire_map,
                                FEATURE_COLUMNS, CATEGORICAL_FEATURES, folds),
        n_trials=n_trials,
        show_progress_bar=True,
    )

    best = study.best_params
    total = sum(best.values())
    best_weights = [best[f"w{i}"] / total for i in range(5)]
    elapsed = time.time() - t0

    print(f"\n=== 最適化完了 ({elapsed:.0f}秒) ===")
    print(f"Best Top3命中率: {study.best_value:.4f}")
    print(f"最適WA_WEIGHTS: {[round(w, 4) for w in best_weights]}")

    os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
    save_data = {
        "best_weights": best_weights,
        "best_score": study.best_value,
        "n_trials": n_trials,
        "elapsed_sec": elapsed,
    }
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(save_data, f, indent=2, ensure_ascii=False)
    print(f"保存: {RESULT_PATH}")

    # settings.py の WA_WEIGHTS を自動更新
    _update_settings_wa_weights(best_weights)

    return save_data


def _update_settings_wa_weights(weights: list):
    """config/settings.py の WA_WEIGHTS を最適値に更新する"""
    settings_path = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
        "config", "settings.py"
    )
    with open(settings_path, "r", encoding="utf-8") as f:
        content = f.read()

    # WA_WEIGHTS = [...] の行を置換（末尾コメントも含めて対応）
    new_line = f"WA_WEIGHTS = {[round(w, 4) for w in weights]}  # Optuna最適値 (更新: {datetime.now().strftime('%Y-%m-%d')})"
    new_content = re.sub(
        r"WA_WEIGHTS\s*=\s*\[.*?\].*",
        new_line,
        content,
    )
    if new_content == content:
        print("警告: settings.py の WA_WEIGHTS 行を見つけられませんでした（変更なし）")
        return
    with open(settings_path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print(f"settings.py の WA_WEIGHTS を更新しました: {[round(w, 4) for w in weights]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-trials", type=int, default=200)
    args = parser.parse_args()
    run_optimization(n_trials=args.n_trials)
