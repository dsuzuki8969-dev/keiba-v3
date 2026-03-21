#!/usr/bin/env python
"""
ばんえい（帯広）専用 Optuna HPO

venue_code=65 のレースのみを対象に LightGBM ハイパーパラメータを最適化し、
最適パラメータで venue_65 モデルを再学習する。

Usage:
  python scripts/optuna_banei.py --n-trials 30
  python scripts/optuna_banei.py --n-trials 50 --timeout 1800
"""
import argparse
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8")

BEST_PARAMS_PATH = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
    "data", "models", "best_banei_params.json",
)
MODEL_PATH = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
    "data", "models", "lgbm_place_venue_65.txt",
)


def _load_banei_races():
    """MLデータからばんえい（venue_code=65）のレースのみ読み込む"""
    from src.ml.lgbm_model import _load_ml_races
    from data.masters.venue_master import is_banei

    all_races = _load_ml_races()
    banei = [r for r in all_races if is_banei(str(r.get("venue_code", "")))]
    print(f"ばんえいレース数: {len(banei)} / 全{len(all_races)}")
    return banei


def _run_fold(fold_start, fold_end, races, sire_map, feat_cols, params):
    """1フォールドを学習・評価し AUC を返す"""
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score
    from src.ml.lgbm_model import (
        RollingStatsTracker, RollingSireTracker,
        _extract_features, _add_race_relative_features,
    )

    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()
    train_feats, train_labels = [], []
    valid_feats, valid_labels = [], []

    for race in races:
        date_str = race.get("date", "")
        if not date_str or date_str >= fold_end:
            break
        is_valid = date_str >= fold_start

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

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    # ばんえいはサンプル少ないため閾値を緩和
    if len(train_labels) < 500 or len(valid_labels) < 50:
        return None

    def _to_np(rows):
        return [[float(f.get(c, 0) or 0) if f.get(c) is not None else float("nan")
                 for c in feat_cols] for f in rows]

    X_train = np.array(_to_np(train_feats), dtype=np.float32)
    y_train = np.array(train_labels, dtype=np.int32)
    X_valid = np.array(_to_np(valid_feats), dtype=np.float32)
    y_valid = np.array(valid_labels, dtype=np.int32)

    dtrain = lgb.Dataset(X_train, label=y_train,
                         feature_name=feat_cols, free_raw_data=False)
    dvalid = lgb.Dataset(X_valid, label=y_valid,
                         feature_name=feat_cols, reference=dtrain,
                         free_raw_data=False)

    model = lgb.train(
        params, dtrain,
        num_boost_round=500,
        valid_sets=[dvalid], valid_names=["valid"],
        callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(-1)],
    )
    y_pred = model.predict(X_valid)
    try:
        return roc_auc_score(y_valid, y_pred)
    except Exception:
        return None


def _make_banei_folds(dates, fold_months=4, min_train_months=12):
    """ばんえい用の Walk-Forward フォールド生成（フォールド幅を広めに）"""
    from datetime import datetime, timedelta
    unique = sorted(set(dates))
    if not unique:
        return []
    start = datetime.strptime(unique[0], "%Y-%m-%d")
    end = datetime.strptime(unique[-1], "%Y-%m-%d")
    folds = []
    cursor = start + timedelta(days=min_train_months * 30)
    while cursor < end:
        fold_start = cursor.strftime("%Y-%m-%d")
        fold_end = (cursor + timedelta(days=fold_months * 30)).strftime("%Y-%m-%d")
        if fold_end > end.strftime("%Y-%m-%d"):
            fold_end = end.strftime("%Y-%m-%d")
        folds.append((fold_start, fold_end))
        cursor += timedelta(days=fold_months * 30)
    return folds


def objective(trial, races, sire_map, feat_cols, folds):
    """Optuna 目的関数"""
    params = {
        "objective": "binary",
        "metric": "auc",
        "boosting_type": "gbdt",
        "verbose": -1,
        "seed": 42,
        "is_unbalance": True,
        # ばんえい向けチューニング範囲（サンプル少なめ → 正則化強め）
        "num_leaves":       trial.suggest_int("num_leaves", 15, 63),
        "learning_rate":    trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
        "feature_fraction": trial.suggest_float("feature_fraction", 0.5, 1.0),
        "bagging_fraction": trial.suggest_float("bagging_fraction", 0.5, 1.0),
        "bagging_freq":     trial.suggest_int("bagging_freq", 1, 10),
        "min_child_samples": trial.suggest_int("min_child_samples", 10, 60),
        "lambda_l1":        trial.suggest_float("lambda_l1", 1e-3, 10.0, log=True),
        "lambda_l2":        trial.suggest_float("lambda_l2", 1e-3, 10.0, log=True),
        "max_depth":        trial.suggest_int("max_depth", 3, 7),
    }

    # 直近2フォールドで評価
    eval_folds = folds[-2:] if len(folds) >= 2 else folds
    auc_list = []
    for fold_start, fold_end in eval_folds:
        auc = _run_fold(fold_start, fold_end, races, sire_map, feat_cols, params)
        if auc is not None:
            auc_list.append(auc)

    if not auc_list:
        return 0.5
    return sum(auc_list) / len(auc_list)


def retrain_with_best(races, sire_map, feat_cols, best_params):
    """最適パラメータで最終モデルを学習・保存"""
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score
    from src.ml.lgbm_model import (
        RollingStatsTracker, RollingSireTracker,
        _extract_features, _add_race_relative_features,
    )

    # 直近3ヶ月を検証、それ以前を学習
    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    if not all_dates:
        print("データなし")
        return
    from datetime import datetime, timedelta
    end_dt = datetime.strptime(all_dates[-1], "%Y-%m-%d")
    val_start = (end_dt - timedelta(days=90)).strftime("%Y-%m-%d")

    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()
    train_feats, train_labels = [], []
    valid_feats, valid_labels = [], []

    for race in races:
        date_str = race.get("date", "")
        if not date_str:
            continue
        is_valid = date_str >= val_start

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

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    def _to_np(rows):
        return [[float(f.get(c, 0) or 0) if f.get(c) is not None else float("nan")
                 for c in feat_cols] for f in rows]

    X_train = np.array(_to_np(train_feats), dtype=np.float32)
    y_train = np.array(train_labels, dtype=np.int32)
    X_valid = np.array(_to_np(valid_feats), dtype=np.float32)
    y_valid = np.array(valid_labels, dtype=np.int32)

    print(f"学習: {len(y_train)}頭, 検証: {len(y_valid)}頭")

    params = {
        "objective": "binary",
        "metric": "auc",
        "boosting_type": "gbdt",
        "verbose": -1,
        "seed": 42,
        "is_unbalance": True,
        **best_params,
    }

    dtrain = lgb.Dataset(X_train, label=y_train,
                         feature_name=feat_cols, free_raw_data=False)
    dvalid = lgb.Dataset(X_valid, label=y_valid,
                         feature_name=feat_cols, reference=dtrain,
                         free_raw_data=False)

    model = lgb.train(
        params, dtrain,
        num_boost_round=1000,
        valid_sets=[dvalid], valid_names=["valid"],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(100)],
    )

    # AUC・TOP1計算
    y_pred = model.predict(X_valid)
    auc = roc_auc_score(y_valid, y_pred)

    # TOP1的中率（レース単位）
    val_idx = 0
    correct, total = 0, 0
    for race in races:
        date_str = race.get("date", "")
        if date_str < val_start:
            continue
        n_horses = sum(1 for h in race.get("horses", []) if h.get("finish_pos") is not None)
        if n_horses == 0:
            continue
        preds = y_pred[val_idx:val_idx + n_horses]
        labels = y_valid[val_idx:val_idx + n_horses]
        val_idx += n_horses
        best_idx = np.argmax(preds)
        if labels[best_idx] == 1:
            correct += 1
        total += 1

    top1 = correct / total * 100 if total else 0

    # モデル保存
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    model.save_model(MODEL_PATH)
    print(f"モデル保存: {MODEL_PATH}")
    print(f"AUC: {auc:.4f}, Top1: {top1:.1f}% ({correct}/{total})")

    return auc, top1


def run(n_trials=30, timeout=None):
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    from src.ml.lgbm_model import (
        _load_horse_sire_map, FEATURE_COLUMNS, CATEGORICAL_FEATURES,
    )

    print("=== ばんえい専用 Optuna HPO ===")
    t0 = time.time()

    races = _load_banei_races()
    sire_map = _load_horse_sire_map()
    # 標準特徴量カラムをそのまま使用（ばんえいで無効な列はNaN→LightGBMが自動処理）
    feat_cols = list(FEATURE_COLUMNS)

    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    folds = _make_banei_folds(all_dates)
    print(f"フォールド数: {len(folds)}")
    for i, (s, e) in enumerate(folds):
        print(f"  Fold {i+1}: {s} ~ {e}")

    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: objective(trial, races, sire_map, feat_cols, folds),
        n_trials=n_trials,
        timeout=timeout,
        show_progress_bar=True,
    )

    best = study.best_params
    best_auc = study.best_value
    elapsed = time.time() - t0

    print(f"\n=== HPO 完了 ({elapsed:.0f}秒) ===")
    print(f"Best AUC: {best_auc:.4f}")
    for k, v in best.items():
        print(f"  {k}: {v}")

    # 保存
    os.makedirs(os.path.dirname(BEST_PARAMS_PATH), exist_ok=True)
    save_data = best.copy()
    save_data["_meta"] = {
        "best_auc": best_auc,
        "n_trials": n_trials,
        "elapsed_sec": elapsed,
    }
    with open(BEST_PARAMS_PATH, "w", encoding="utf-8") as f:
        json.dump(save_data, f, indent=2, ensure_ascii=False)
    print(f"パラメータ保存: {BEST_PARAMS_PATH}")

    # 最適パラメータで再学習
    print("\n=== 最適パラメータで venue_65 モデル再学習 ===")
    retrain_with_best(races, sire_map, feat_cols, best)

    return save_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-trials", type=int, default=30)
    parser.add_argument("--timeout", type=int, default=None, help="タイムアウト秒数")
    args = parser.parse_args()

    run(n_trials=args.n_trials, timeout=args.timeout)
