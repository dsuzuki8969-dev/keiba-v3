"""
ばんえい(venue_65)専用 Optuna ハイパーパラメータ探索

使い方:
  python tools/tune_banei.py --trials 50
"""
import sys, io, os, argparse, json, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import lightgbm as lgb
import optuna
from optuna.samplers import TPESampler

from src.ml.lgbm_model import (
    _collect_all_rows, FEATURE_COLUMNS_BANEI, _add_race_relative_features,
)

optuna.logging.set_verbosity(optuna.logging.WARNING)

parser = argparse.ArgumentParser()
parser.add_argument("--trials", type=int, default=50, help="Optuna試行数")
args = parser.parse_args()


def main():
    print("データ収集中...")
    t0 = time.time()
    all_train_rows, all_valid_groups, split_date, tracker, sire_tracker = \
        _collect_all_rows(valid_days=30)
    print(f"データ収集完了: {time.time()-t0:.0f}秒")

    # venue_65 のみフィルタ
    train_rows = [(f, l) for f, l, sv, jv, vv, smv in all_train_rows if vv == "65"]
    valid_groups = [(fs, ls) for fs, ls, sv, jv, vv, smv in all_valid_groups if vv == "65"]

    train_X = [f for f, l in train_rows]
    train_y = [l for f, l in train_rows]
    valid_X = [f for fs, ls in valid_groups for f in fs]
    valid_y_list = [l for fs, ls in valid_groups for l in ls]
    valid_race_sizes = [len(fs) for fs, ls in valid_groups]

    def _to_np(rows):
        mat = []
        for f in rows:
            mat.append([float(f[c]) if f[c] is not None else float("nan")
                        for c in FEATURE_COLUMNS_BANEI])
        return np.array(mat, dtype=np.float32)

    X_train = _to_np(train_X)
    y_train = np.array(train_y, dtype=np.int32)
    X_valid = _to_np(valid_X)
    y_valid = np.array(valid_y_list, dtype=np.int32)

    print(f"train: {len(y_train)}, valid: {len(y_valid)}, features: {len(FEATURE_COLUMNS_BANEI)}")

    dtrain = lgb.Dataset(X_train, label=y_train, feature_name=FEATURE_COLUMNS_BANEI, free_raw_data=False)
    dvalid = lgb.Dataset(X_valid, label=y_valid, feature_name=FEATURE_COLUMNS_BANEI, reference=dtrain, free_raw_data=False)

    best_auc = 0.0

    def objective(trial):
        nonlocal best_auc
        p = {
            "objective": "binary",
            "metric": ["binary_logloss", "auc"],
            "boosting_type": "gbdt",
            "verbose": -1,
            "seed": 42,
            "is_unbalance": True,
            "num_leaves": trial.suggest_int("num_leaves", 8, 128),
            "learning_rate": trial.suggest_float("learning_rate", 0.005, 0.15, log=True),
            "feature_fraction": trial.suggest_float("feature_fraction", 0.4, 1.0),
            "bagging_fraction": trial.suggest_float("bagging_fraction", 0.4, 1.0),
            "bagging_freq": trial.suggest_int("bagging_freq", 1, 10),
            "min_child_samples": trial.suggest_int("min_child_samples", 10, 200),
            "lambda_l1": trial.suggest_float("lambda_l1", 0.01, 20.0, log=True),
            "lambda_l2": trial.suggest_float("lambda_l2", 0.01, 20.0, log=True),
            "max_depth": trial.suggest_int("max_depth", 3, 8),
        }
        model = lgb.train(
            p, dtrain, num_boost_round=3000,
            valid_sets=[dvalid], valid_names=["valid"],
            callbacks=[lgb.early_stopping(100), lgb.log_evaluation(0)],
        )
        from sklearn.metrics import roc_auc_score
        pred = model.predict(X_valid)
        auc = roc_auc_score(y_valid, pred)
        if auc > best_auc:
            best_auc = auc
            print(f"  Trial {trial.number}: AUC={auc:.4f} iter={model.best_iteration} "
                  f"leaves={p['num_leaves']} lr={p['learning_rate']:.4f} "
                  f"depth={p['max_depth']}")
        return auc

    study = optuna.create_study(
        direction="maximize",
        sampler=TPESampler(seed=42),
    )
    print(f"\nOptuna探索開始 ({args.trials}試行)...")
    t1 = time.time()
    study.optimize(objective, n_trials=args.trials, show_progress_bar=False)
    print(f"\n探索完了: {time.time()-t1:.0f}秒")
    print(f"Best AUC: {study.best_value:.4f}")
    print(f"Best params: {study.best_params}")

    # 保存
    out_path = os.path.join("data", "models", "best_banei_params.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "best_params": study.best_params,
            "best_auc": round(study.best_value, 4),
        }, f, indent=2)
    print(f"保存: {out_path}")

    # 最適パラメータで最終学習して詳細出力
    print("\n最適パラメータで最終学習...")
    best_p = dict(study.best_params)
    best_p.update({
        "objective": "binary",
        "metric": ["binary_logloss", "auc"],
        "boosting_type": "gbdt",
        "verbose": -1,
        "seed": 42,
        "is_unbalance": True,
    })
    final_model = lgb.train(
        best_p, dtrain, num_boost_round=3000,
        valid_sets=[dvalid], valid_names=["valid"],
        callbacks=[lgb.early_stopping(100), lgb.log_evaluation(500)],
    )
    from sklearn.metrics import roc_auc_score
    final_pred = final_model.predict(X_valid)
    final_auc = roc_auc_score(y_valid, final_pred)

    # レース単位精度
    correct_top1, correct_top3, total_eval = 0, 0, 0
    idx = 0
    for g in valid_race_sizes:
        if g < 3:
            idx += g
            continue
        rp = final_pred[idx:idx+g]
        rt = y_valid[idx:idx+g]
        if rt[np.argmax(rp)] == 1:
            correct_top1 += 1
        if any(rt[i] == 1 for i in np.argsort(rp)[-3:]):
            correct_top3 += 1
        total_eval += 1
        idx += g

    print(f"\n最終結果:")
    print(f"  AUC: {final_auc:.4f}")
    print(f"  Top1→3着内: {correct_top1/max(total_eval,1)*100:.1f}% ({correct_top1}/{total_eval})")
    print(f"  Top3→3着内: {correct_top3/max(total_eval,1)*100:.1f}% ({correct_top3}/{total_eval})")
    print(f"  Best iteration: {final_model.best_iteration}")

    # 特徴量重要度
    imp = final_model.feature_importance(importance_type="gain")
    imp_pairs = sorted(zip(FEATURE_COLUMNS_BANEI, imp), key=lambda x: -x[1])
    print(f"\n特徴量重要度 (Top 15):")
    for name, score in imp_pairs[:15]:
        print(f"  {name:40s} {score:.0f}")


if __name__ == "__main__":
    main()
