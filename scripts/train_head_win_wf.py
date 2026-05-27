"""WF 期間別 head_win (1着 binary) モデル学習スクリプト

各 WF 期間の train_max 以前データで head_win モデルを学習し、
data/models/{wf_name}/lgbm_win_global.txt に保存する。

MVP 簡略化方針:
  - global モデルのみ (階層モデルは省略)
  - Platt scaling なし (raw 出力を win_prob として使用)
  - ハイパーパラメータは head_top3 と同じ設定を流用

実行例:
  python scripts/train_head_win_wf.py --period wf_2026
  python scripts/train_head_win_wf.py --period all
"""

import argparse
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.log import get_logger

logger = get_logger(__name__)

# WF 期間定義 (wf_inference.py と同一)
WF_PERIODS = {
    "wf_2024": {
        "model_dir": os.path.join(PROJECT_ROOT, "data", "models", "wf_2024"),
        "train_max": "2023-12-31",
    },
    "wf_2025": {
        "model_dir": os.path.join(PROJECT_ROOT, "data", "models", "wf_2025"),
        "train_max": "2024-12-31",
    },
    "wf_2026": {
        "model_dir": os.path.join(PROJECT_ROOT, "data", "models", "wf_2026"),
        "train_max": "2025-12-31",
    },
}

# 最低学習サンプル数
MIN_TRAIN_SAMPLES = 10000


def train_head_win_model(period_name: str, model_dir: str, train_max: str) -> dict:
    """1つの WF 期間で head_win binary モデルを学習する。

    Args:
        period_name: WF 期間名 (ログ表示用)
        model_dir: モデル保存先ディレクトリ
        train_max: 学習データの最大日付 (この日以前のデータを使用)

    Returns:
        metrics dict
    """
    import lightgbm as lgb
    import numpy as np
    from src.ml.lgbm_model import (
        FEATURE_COLUMNS,
        CATEGORICAL_FEATURES,
        _collect_all_rows,
        _smile_key_ml,
        SURFACE_MAP,
        MODEL_DIR as _orig_model_dir,
        STATS_PATH as _orig_stats_path,
        SIRE_STATS_PATH as _orig_sire_stats_path,
        SIRE_MAP_PATH as _orig_sire_map_path,
    )

    logger.info("=" * 60)
    logger.info("[%s] head_win モデル学習開始 (train_max=%s)", period_name, train_max)
    logger.info("  model_dir: %s", model_dir)
    logger.info("=" * 60)

    os.makedirs(model_dir, exist_ok=True)
    save_path = os.path.join(model_dir, "lgbm_win_global.txt")

    t_start = time.time()

    # _collect_all_rows() はグローバル変数 MODEL_DIR / STATS_PATH 等を参照するため
    # 一時的に model_dir に差し替える
    import src.ml.lgbm_model as _lgbm_mod
    _orig_md   = _lgbm_mod.MODEL_DIR
    _orig_sp   = _lgbm_mod.STATS_PATH
    _orig_ssp  = _lgbm_mod.SIRE_STATS_PATH
    _orig_smp  = _lgbm_mod.SIRE_MAP_PATH

    _lgbm_mod.MODEL_DIR    = model_dir
    _lgbm_mod.STATS_PATH   = os.path.join(model_dir, "rolling_stats.pkl")
    _lgbm_mod.SIRE_STATS_PATH = os.path.join(model_dir, "sire_rolling_stats.pkl")
    _lgbm_mod.SIRE_MAP_PATH = os.path.join(model_dir, "horse_sire_map.pkl")

    try:
        logger.info("[%s] 全行収集開始 (max_date=%s)...", period_name, train_max)
        all_train_rows, all_valid_groups, split_date, tracker, sire_tracker = \
            _collect_all_rows(valid_days=30, max_date=train_max)
    finally:
        # グローバル変数を元に戻す
        _lgbm_mod.MODEL_DIR        = _orig_md
        _lgbm_mod.STATS_PATH       = _orig_sp
        _lgbm_mod.SIRE_STATS_PATH  = _orig_ssp
        _lgbm_mod.SIRE_MAP_PATH    = _orig_smp

    logger.info("[%s] 全行収集完了: train=%d rows, valid=%d groups, split_date=%s",
                period_name, len(all_train_rows), len(all_valid_groups), split_date)

    # --- head_win ラベルに変換 (1着 binary) ---
    # all_train_rows は (feat, label_top3, surface_val, is_jra_val, venue_str, smile_cat) タプル
    # label_top3 は 1 if finish_pos <= 3 else 0
    # head_win は finish_pos == 1 なら 1 else 0 だが、ここでは label_top3 を再利用不可
    # → _collect_all_rows() は label_top3 しか返さないため、別途 ML JSON を再走査して
    #   head_win ラベルを構築する必要がある。
    # MVP 方針: _collect_all_rows の行を再利用して特徴量は共用し、
    #           ラベルのみ finish_pos 情報から再構築する。
    # NOTE: all_train_rows の label は top3 binary。head_win 用には別途構築が必要。
    # → 簡略化のため、lgbm_model.py の _collect_all_rows と同等の処理を
    #   この関数内でラベルのみ変えて実行する。

    logger.info("[%s] head_win ラベルで再収集中...", period_name)
    train_rows_win, valid_rows_win = _collect_head_win_rows(
        train_max=train_max,
        all_train_meta=all_train_rows,
    )

    logger.info("[%s] head_win ラベル変換完了: train=%d, valid=%d",
                period_name, len(train_rows_win), len(valid_rows_win))

    if len(train_rows_win) < MIN_TRAIN_SAMPLES:
        logger.warning("[%s] サンプル不足でスキップ (%d < %d)",
                       period_name, len(train_rows_win), MIN_TRAIN_SAMPLES)
        return {"skipped": True, "period": period_name, "train_samples": len(train_rows_win)}

    # --- numpy 変換 ---
    feat_cols = FEATURE_COLUMNS
    cat_feats = [c for c in CATEGORICAL_FEATURES if c in feat_cols]

    def _to_np(rows_with_label):
        feats = [r[0] for r in rows_with_label]
        labels = [r[1] for r in rows_with_label]
        mat = []
        for f in feats:
            mat.append([float(f.get(c)) if f.get(c) is not None else float("nan")
                        for c in feat_cols])
        return np.array(mat, dtype=np.float32), np.array(labels, dtype=np.int32)

    X_train, y_train = _to_np(train_rows_win)
    logger.info("[%s] X_train.shape=%s, y_train.sum()=%d (1着件数)",
                period_name, X_train.shape, int(y_train.sum()))

    # --- LightGBM 学習 ---
    # head_top3 と同一パラメータ (MVP)
    params = {
        "objective": "binary",
        "metric": ["binary_logloss", "auc"],
        "boosting_type": "gbdt",
        "num_leaves": 63,
        "learning_rate": 0.02,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "min_child_samples": 50,
        "lambda_l1": 0.1,
        "lambda_l2": 1.0,
        "max_depth": 7,
        "verbose": -1,
        "seed": 42,
        "is_unbalance": True,  # 1着は全体の 1/field_count ≈ 1/10 程度で不均衡
    }

    dtrain = lgb.Dataset(
        X_train, label=y_train,
        feature_name=feat_cols,
        categorical_feature=cat_feats,
        free_raw_data=False,
    )

    if len(valid_rows_win) >= 1000:
        X_valid, y_valid = _to_np(valid_rows_win)
        logger.info("[%s] X_valid.shape=%s, y_valid.sum()=%d",
                    period_name, X_valid.shape, int(y_valid.sum()))
        dvalid = lgb.Dataset(
            X_valid, label=y_valid,
            feature_name=feat_cols,
            categorical_feature=cat_feats,
            reference=dtrain,
            free_raw_data=False,
        )
        model = lgb.train(
            params,
            dtrain,
            num_boost_round=3000,
            valid_sets=[dtrain, dvalid],
            valid_names=["train", "valid"],
            callbacks=[
                lgb.log_evaluation(period=200),
                lgb.early_stopping(stopping_rounds=100),
            ],
        )
        y_pred = model.predict(X_valid)
        from sklearn.metrics import roc_auc_score, log_loss
        auc = roc_auc_score(y_valid, y_pred)
        logloss = log_loss(y_valid, y_pred)
        logger.info("[%s] 検証 AUC=%.4f  LogLoss=%.4f  best_iter=%d",
                    period_name, auc, logloss, model.best_iteration)
        metrics = {
            "auc": round(auc, 4),
            "logloss": round(logloss, 4),
            "train_samples": len(y_train),
            "valid_samples": len(y_valid),
            "best_iteration": model.best_iteration,
        }
    else:
        # 検証データ不足 → 固定ラウンド
        logger.warning("[%s] 検証データ不足 (%d 件) → 固定 300 ラウンドで学習",
                       period_name, len(valid_rows_win))
        model = lgb.train(
            params,
            dtrain,
            num_boost_round=300,
            callbacks=[lgb.log_evaluation(period=100)],
        )
        metrics = {
            "auc": None,
            "logloss": None,
            "train_samples": len(y_train),
            "valid_samples": 0,
            "best_iteration": 300,
        }

    # 特徴量重要度 Top 15
    imp = model.feature_importance(importance_type="gain")
    imp_pairs = sorted(zip(feat_cols, imp), key=lambda x: -x[1])
    logger.info("[%s] 特徴量重要度 (Top 15):", period_name)
    for name, val in imp_pairs[:15]:
        logger.info("  %-25s %10.0f", name, val)

    # 保存
    model.save_model(save_path)
    elapsed = time.time() - t_start
    logger.info("[%s] head_win モデル保存完了: %s (%.1f 秒)", period_name, save_path, elapsed)

    metrics["period"] = period_name
    metrics["save_path"] = save_path
    metrics["elapsed"] = round(elapsed, 1)
    return metrics


def _collect_head_win_rows(train_max: str, all_train_meta: list):
    """_collect_all_rows の結果から head_win ラベルを付けた行を返す。

    _collect_all_rows が返す all_train_rows は特徴量のみ。
    ラベルは top3 binary だが、head_win (1着) のラベルが必要。

    アプローチ: ML JSON を再走査し finish_pos==1 で win ラベルを生成する。
    特徴量は _collect_all_rows 済みの結果と同一になるため行の順序対応が必要。
    → 簡略のため: ML JSON を再走査して feat+win_label を返す。
      _collect_all_rows 内の特徴量計算をここで再度実行する (2 周になるが MVP は許容)。

    NOTE: _collect_all_rows は split_date を設定して valid 行も作るが、
          head_win は valid を簡易処理する (全期間の valid サンプル)。
    """
    from src.ml.lgbm_model import (
        _load_ml_races,
        RollingStatsTracker,
        RollingSireTracker,
        _load_horse_sire_map,
        _extract_features,
        _add_race_relative_features,
        _load_relative_dev_map,
    )

    races = _load_ml_races(max_date=train_max)
    if not races:
        logger.warning("head_win 用 ML data が見つかりません (max_date=%s)", train_max)
        return [], []

    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    valid_days = 30
    split_idx = max(1, len(all_dates) - valid_days)
    split_date = all_dates[split_idx]

    sire_map = _load_horse_sire_map()
    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()
    relative_dev_map = _load_relative_dev_map()

    # 調教特徴量は head_top3 と共有だが MVP ではスキップ (ゼロになるだけ)
    # (調教特徴量を含める場合は TrainingFeatureExtractor をここでもロードする)
    training_extractor = None
    try:
        from src.ml.training_features import TrainingFeatureExtractor
        training_extractor = TrainingFeatureExtractor()
        training_extractor.load_all()
        logger.info("調教特徴量抽出器ロード完了 (head_win 用)")
    except Exception as e:
        logger.warning("調教特徴量ロード失敗 (head_win 用、スキップ): %s", e)

    train_rows: list = []
    valid_rows: list = []

    for race in races:
        date_str = race.get("date", "")
        is_valid = date_str >= split_date
        race_id  = race.get("race_id", "")

        # 調教特徴量
        train_feats_map = {}
        if training_extractor:
            horse_names = [h.get("horse_name", "") for h in race.get("horses", []) if h.get("horse_name")]
            if horse_names:
                train_feats_map = training_extractor.get_race_training_features(race_id, horse_names, date_str)

        race_feats, race_win_labels = [], []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            _rd_key = (race_id, hid)
            _rel_dev_val = relative_dev_map.get(_rd_key)
            _h_injected = dict(h, sire_id=sid, bms_id=bid)
            if _rel_dev_val is not None:
                _h_injected["relative_dev"] = _rel_dev_val

            feat = _extract_features(_h_injected, race, tracker, sire_tracker)
            hname = h.get("horse_name", "")
            if hname in train_feats_map:
                feat.update(train_feats_map[hname])

            race_feats.append(feat)
            # head_win ラベル: 1着のみ 1
            race_win_labels.append(1 if fp == 1 else 0)

        if race_feats:
            _add_race_relative_features(race_feats)
            if is_valid:
                for feat, lbl in zip(race_feats, race_win_labels):
                    valid_rows.append((feat, lbl))
            else:
                for feat, lbl in zip(race_feats, race_win_labels):
                    train_rows.append((feat, lbl))

        # relative_dev を horse dict に注入 (tracker.update_race 前)
        if relative_dev_map and race_id:
            for h in race.get("horses", []):
                hid_upd = h.get("horse_id", "")
                if hid_upd:
                    _rd = relative_dev_map.get((race_id, hid_upd))
                    if _rd is not None:
                        h["relative_dev"] = _rd

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    return train_rows, valid_rows


def main():
    parser = argparse.ArgumentParser(description="WF 期間別 head_win モデル学習")
    parser.add_argument(
        "--period",
        choices=["wf_2024", "wf_2025", "wf_2026", "all"],
        default="all",
        help="学習対象 WF 期間 (default: all)",
    )
    args = parser.parse_args()

    periods = WF_PERIODS if args.period == "all" else {args.period: WF_PERIODS[args.period]}

    all_metrics = {}
    t_total = time.time()

    for period_name, cfg in periods.items():
        logger.info("\n" + "=" * 60)
        logger.info("head_win 学習開始: %s", period_name)
        metrics = train_head_win_model(
            period_name=period_name,
            model_dir=cfg["model_dir"],
            train_max=cfg["train_max"],
        )
        all_metrics[period_name] = metrics

    logger.info("\n" + "=" * 60)
    logger.info("=== head_win 学習結果サマリー ===")
    logger.info("%-10s %-8s %-10s %-8s %-8s %-10s",
                "期間", "train N", "valid N", "AUC", "LogLoss", "elapsed(s)")
    for name, m in all_metrics.items():
        if m.get("skipped"):
            logger.info("%-10s SKIPPED (train=%d)", name, m.get("train_samples", 0))
        else:
            logger.info(
                "%-10s %-8d %-10d %-8s %-8s %-10.1f",
                name,
                m.get("train_samples", 0),
                m.get("valid_samples", 0),
                f"{m['auc']:.4f}" if m.get("auc") else "N/A",
                f"{m['logloss']:.4f}" if m.get("logloss") else "N/A",
                m.get("elapsed", 0),
            )

    logger.info("全期間合計: %.1f 秒", time.time() - t_total)


if __name__ == "__main__":
    main()
