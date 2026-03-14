#!/usr/bin/env python
"""
Optuna Hyperparameter Optimization for LightGBM

Walk-Forward CV の AUC を目的関数として LightGBM のハイパーパラメータを最適化する。
最適パラメータは data/models/best_lgbm_params.json に保存し、
次回の retrain_all.py で自動的に読み込まれる。

Usage:
  python scripts/optuna_hpo.py --n-trials 50
  python scripts/optuna_hpo.py --n-trials 100 --timeout 3600
"""

import argparse
import json
import math
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8")

BEST_PARAMS_PATH = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
    "data", "models", "best_lgbm_params.json"
)


def _run_single_fold_fast(fold_idx, fold_start, fold_end, races, sire_map,
                           feature_columns, categorical_features, params) -> float:
    """1フォールドを指定パラメータで学習・評価し AUC を返す
    HPO用: 学習データを最大30,000頭にサブサンプリングして高速化"""
    import numpy as np
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score
    from src.ml.lgbm_model import (
        RollingStatsTracker, RollingSireTracker,
        _extract_features, _add_race_relative_features, SURFACE_MAP,
    )

    MAX_TRAIN_HORSES = 30000  # HPO用上限

    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()
    train_feats, train_labels = [], []
    valid_feats, valid_labels = [], []
    _train_full = False  # 学習データが上限に達したフラグ

    for race in races:
        date_str = race.get("date", "")
        if not date_str or date_str >= fold_end:
            break
        is_valid = date_str >= fold_start
        # 学習データが上限に達したら、trackerのみ更新してスキップ
        if not is_valid and _train_full:
            tracker.update_race(race)
            sire_tracker.update_race(race, sire_map)
            continue

        r_feats, r_labels = [], []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            feat = _extract_features(dict(h, sire_id=sid, bms_id=bid),
                                     race, tracker, sire_tracker)
            r_feats.append(feat)
            r_labels.append(1 if fp <= 3 else 0)

        if r_feats:
            _add_race_relative_features(r_feats)
            if is_valid:
                valid_feats.extend(r_feats)
                valid_labels.extend(r_labels)
            else:
                train_feats.extend(r_feats)
                train_labels.extend(r_labels)
                if len(train_feats) >= MAX_TRAIN_HORSES:
                    _train_full = True

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    if len(train_labels) < 2000 or len(valid_labels) < 100:
        return None

    def _to_np(rows):
        return [[float(f[c]) if f[c] is not None else float("nan")
                 for c in feature_columns] for f in rows]

    import numpy as np
    X_train = np.array(_to_np(train_feats), dtype=np.float32)
    y_train = np.array(train_labels, dtype=np.int32)
    X_valid = np.array(_to_np(valid_feats), dtype=np.float32)
    y_valid = np.array(valid_labels, dtype=np.int32)

    dtrain = lgb.Dataset(X_train, label=y_train,
                         feature_name=feature_columns,
                         categorical_feature=categorical_features,
                         free_raw_data=False)
    dvalid = lgb.Dataset(X_valid, label=y_valid,
                         feature_name=feature_columns,
                         categorical_feature=categorical_features,
                         reference=dtrain, free_raw_data=False)

    model = lgb.train(
        params,
        dtrain,
        num_boost_round=500,          # HPO用: 速度優先 (本番は3000)
        valid_sets=[dvalid],
        valid_names=["valid"],
        callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(-1)],
    )
    y_pred = model.predict(X_valid)
    try:
        return roc_auc_score(y_valid, y_pred)
    except Exception:
        return None


def objective(trial, races, sire_map, feature_columns, categorical_features,
              folds) -> float:
    """Optuna 目的関数: 複数フォールドの平均 AUC を返す"""
    params = {
        "objective": "binary",
        "metric": "auc",
        "boosting_type": "gbdt",
        "verbose": -1,
        "seed": 42,
        "is_unbalance": True,
        # チューニング対象パラメータ
        "num_leaves":       trial.suggest_int("num_leaves", 31, 127),
        "learning_rate":    trial.suggest_float("learning_rate", 0.005, 0.05, log=True),
        "feature_fraction": trial.suggest_float("feature_fraction", 0.6, 1.0),
        "bagging_fraction": trial.suggest_float("bagging_fraction", 0.6, 1.0),
        "bagging_freq":     trial.suggest_int("bagging_freq", 1, 10),
        "min_child_samples": trial.suggest_int("min_child_samples", 20, 100),
        "lambda_l1":        trial.suggest_float("lambda_l1", 1e-3, 10.0, log=True),
        "lambda_l2":        trial.suggest_float("lambda_l2", 1e-3, 10.0, log=True),
        "max_depth":        trial.suggest_int("max_depth", 5, 9),
    }

    # 直近 2 フォールドのみ使用（速度重視）
    eval_folds = folds[-2:]
    auc_list = []
    for fold_idx, (fold_start, fold_end) in enumerate(eval_folds, 1):
        auc = _run_single_fold_fast(
            fold_idx, fold_start, fold_end, races, sire_map,
            feature_columns, categorical_features, params
        )
        if auc is not None:
            auc_list.append(auc)

    if not auc_list:
        return 0.5
    return sum(auc_list) / len(auc_list)


def run_hpo(n_trials: int = 50, timeout: int = None):
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    from src.ml.lgbm_model import (
        FEATURE_COLUMNS, CATEGORICAL_FEATURES,
        _load_ml_races, _load_horse_sire_map,
    )
    from scripts.walk_forward_cv import _make_folds

    print(f"=== Optuna HPO 開始 ({n_trials} trials) ===")
    t0 = time.time()

    races = _load_ml_races()
    sire_map = _load_horse_sire_map()
    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    folds = _make_folds(all_dates, fold_months=3, min_train_months=9)
    print(f"利用フォールド数: {len(folds)}  (直近3フォールドで評価)")

    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: objective(trial, races, sire_map,
                                FEATURE_COLUMNS, CATEGORICAL_FEATURES, folds),
        n_trials=n_trials,
        timeout=timeout,
        show_progress_bar=True,
    )

    best = study.best_params
    best_auc = study.best_value
    elapsed = time.time() - t0

    print(f"\n=== HPO 完了 ({elapsed:.0f}秒) ===")
    print(f"Best AUC: {best_auc:.4f}")
    print("Best params:")
    for k, v in best.items():
        print(f"  {k}: {v}")

    # 保存
    os.makedirs(os.path.dirname(BEST_PARAMS_PATH), exist_ok=True)
    save_data = {
        "best_params": best,
        "best_auc": best_auc,
        "n_trials": n_trials,
        "elapsed_sec": elapsed,
    }
    with open(BEST_PARAMS_PATH, "w", encoding="utf-8") as f:
        json.dump(save_data, f, indent=2, ensure_ascii=False)
    print(f"保存: {BEST_PARAMS_PATH}")

    return save_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-trials", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=None, help="秒数")
    args = parser.parse_args()

    run_hpo(n_trials=args.n_trials, timeout=args.timeout)
