"""
LightGBM LambdaRank モデル

バイナリ分類 (lgbm_model.py) ではなく lambdarank 目的関数でレース内の
相対的な着順ランクを最適化する補完モデル。

特長:
  - 同じ 68 特徴量を使用（LGBMPredictor との特徴量互換）
  - 目的関数: lambdarank (NDCG@3 最適化)
  - 関連度ラベル: 1着=3, 2着=2, 3着=1, 着外=0
  - 1レース = 1クエリグループ
  - 三連複馬券組み合わせ選択での Top3 精度向上を狙う

学習:
  python -m src.ml.lgbm_ranker

推論:
  from src.ml.lgbm_ranker import LGBMRanker
  ranker = LGBMRanker()
  if ranker.load():
      scores = ranker.predict_race(race_dict, horse_dicts)
      # → {horse_id: float}  大きいほど上位予測
"""

import os
import pickle
from typing import Dict, List

from src.log import get_logger

logger = get_logger(__name__)

_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MODEL_DIR   = os.path.join(_BASE, "data", "models")
RANKER_PATH = os.path.join(MODEL_DIR, "lgbm_ranker.txt")

# 最低学習グループ数 (レース数)
MIN_TRAIN_GROUPS = 500


# ============================================================
# 学習
# ============================================================


def train_ranker(valid_days: int = 30) -> dict:
    """
    LightGBM LambdaRank モデルを学習する。

    学習データ: ML JSON (data/ml/*.json) 全体
    検証データ: 直近 valid_days 日分 (early stopping 用)

    Returns:
        {"ndcg3_train": float, "ndcg3_valid": float,
         "best_iteration": int, "train_groups": int, "valid_groups": int}
    """
    import lightgbm as lgb
    import numpy as np

    from src.ml.lgbm_model import (
        CATEGORICAL_FEATURES,
        FEATURE_COLUMNS,
        RollingSireTracker,
        RollingStatsTracker,
        _add_race_relative_features,
        _extract_features,
        _load_horse_sire_map,
        _load_ml_races,
    )

    logger.info("=" * 55)
    logger.info("LightGBM LambdaRank 学習開始")
    logger.info("=" * 55)

    # ─── データ読み込み ───
    races = _load_ml_races()
    if not races:
        raise ValueError("ML データが見つかりません (data/ml/*.json)")

    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    if len(all_dates) < 14:
        raise ValueError(f"データ不足 ({len(all_dates)} 日)")

    split_idx  = max(1, len(all_dates) - valid_days)
    split_date = all_dates[split_idx]
    logger.info("学習: %s ~ %s / 検証: %s ~ %s",
                all_dates[0], all_dates[split_idx - 1], split_date, all_dates[-1])

    sire_map     = _load_horse_sire_map()
    tracker      = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    # 調教特徴量抽出器をロード
    training_extractor = None
    try:
        from src.ml.training_features import TrainingFeatureExtractor
        training_extractor = TrainingFeatureExtractor()
        training_extractor.load_all()
        logger.info("調教特徴量抽出器ロード完了")
    except Exception as e:
        logger.warning("調教特徴量ロード失敗（スキップ）: %s", e)

    # ─── 特徴量・ラベル収集 ───
    train_X:      List[dict] = []
    train_rels:   List[int]  = []   # 関連度 0/1/2/3
    train_groups: List[int]  = []   # グループサイズ（レースごとの頭数）

    valid_X:      List[dict] = []
    valid_rels:   List[int]  = []
    valid_groups_sz: List[int] = []

    for race in races:
        date_str = race.get("date", "")
        is_valid = date_str >= split_date
        race_id  = race.get("race_id", "")

        # 調教特徴量をレース単位で一括取得
        train_feats_map = {}
        if training_extractor:
            horse_names = [
                h.get("horse_name", "") for h in race.get("horses", [])
                if h.get("horse_name")
            ]
            if horse_names:
                train_feats_map = training_extractor.get_race_training_features(
                    race_id, horse_names, date_str
                )

        r_feats, r_rels = [], []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            feat = _extract_features(
                dict(h, sire_id=sid, bms_id=bid),
                race, tracker, sire_tracker,
            )
            # 調教特徴量をマージ
            hname = h.get("horse_name", "")
            if hname in train_feats_map:
                feat.update(train_feats_map[hname])
            # 関連度ラベル: 1着=3, 2着=2, 3着=1, 着外=0
            rel = max(0, 4 - fp) if fp <= 3 else 0
            r_feats.append(feat)
            r_rels.append(rel)

        if len(r_feats) >= 3:   # 3頭未満のレースはスキップ
            # ① 相対特徴量を一括設定 (2パス)
            _add_race_relative_features(r_feats)
            if is_valid:
                valid_X.extend(r_feats)
                valid_rels.extend(r_rels)
                valid_groups_sz.append(len(r_feats))
            else:
                train_X.extend(r_feats)
                train_rels.extend(r_rels)
                train_groups.append(len(r_feats))

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    logger.info("学習グループ: %d レース (%d 頭) / 検証グループ: %d レース (%d 頭)",
                len(train_groups), len(train_X),
                len(valid_groups_sz), len(valid_X))

    if len(train_groups) < MIN_TRAIN_GROUPS:
        raise ValueError(f"学習グループ不足 ({len(train_groups)} < {MIN_TRAIN_GROUPS})")

    # ─── numpy 変換 ───
    def _to_np(rows):
        mat = []
        for f in rows:
            mat.append([
                float(f.get(c) if f.get(c) is not None else float("nan"))
                for c in FEATURE_COLUMNS
            ])
        return np.array(mat, dtype=np.float32)

    X_train = _to_np(train_X)
    y_train = np.array(train_rels, dtype=np.int32)
    X_valid = _to_np(valid_X)
    y_valid = np.array(valid_rels, dtype=np.int32)

    # ─── LightGBM Dataset ───
    dtrain = lgb.Dataset(
        X_train, label=y_train,
        feature_name=FEATURE_COLUMNS,
        categorical_feature=CATEGORICAL_FEATURES,
        free_raw_data=False,
    )
    dtrain.set_group(train_groups)

    dvalid = lgb.Dataset(
        X_valid, label=y_valid,
        feature_name=FEATURE_COLUMNS,
        categorical_feature=CATEGORICAL_FEATURES,
        reference=dtrain,
        free_raw_data=False,
    )
    dvalid.set_group(valid_groups_sz)

    # ─── LambdaRank パラメータ ───
    params = {
        "objective":       "lambdarank",
        "metric":          "ndcg",
        "ndcg_eval_at":    [3, 5],
        "label_gain":      [0, 1, 3, 7],   # 指数DCGゲイン (2^rel - 1)
        "boosting_type":   "gbdt",
        "num_leaves":      63,
        "learning_rate":   0.02,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq":    5,
        "min_child_samples": 20,
        "lambda_l1":       0.1,
        "lambda_l2":       1.0,
        "max_depth":       7,
        "verbose":         -1,
        "seed":            42,
    }

    model = lgb.train(
        params,
        dtrain,
        num_boost_round=2000,
        valid_sets=[dtrain, dvalid],
        valid_names=["train", "valid"],
        callbacks=[
            lgb.log_evaluation(period=200),
            lgb.early_stopping(stopping_rounds=100),
        ],
    )

    # ─── 評価 ───
    train_preds = model.predict(X_train)
    valid_preds = model.predict(X_valid)

    def _mean_ndcg(preds, rels, groups, k=3):
        import math
        idx, vals = 0, []
        for g in groups:
            p  = preds[idx:idx + g].tolist()
            r  = rels[idx:idx + g].tolist()
            ideal = sorted(r, reverse=True)[:k]
            idcg  = sum(rv / math.log2(i + 2) for i, rv in enumerate(ideal) if rv > 0)
            if idcg > 0:
                order = sorted(range(g), key=lambda i: -p[i])
                dcg   = sum(r[order[i]] / math.log2(i + 2) for i in range(min(k, g)))
                vals.append(dcg / idcg)
            idx += g
        return sum(vals) / len(vals) if vals else None

    ndcg3_train = _mean_ndcg(train_preds, y_train, train_groups)
    ndcg3_valid = _mean_ndcg(valid_preds, y_valid, valid_groups_sz)

    logger.info("")
    logger.info("=" * 50)
    logger.info("LambdaRank 学習結果")
    logger.info("=" * 50)
    logger.info("NDCG@3 (学習): %.4f", ndcg3_train or 0)
    logger.info("NDCG@3 (検証): %.4f", ndcg3_valid or 0)
    logger.info("Best iteration: %d", model.best_iteration)

    # 特徴量重要度 Top15
    imp = model.feature_importance(importance_type="gain")
    imp_pairs = sorted(zip(FEATURE_COLUMNS, imp), key=lambda x: -x[1])
    logger.info("")
    logger.info("特徴量重要度 (Top15):")
    for name, val in imp_pairs[:15]:
        logger.info("  %-25s %10.0f", name, val)

    # ─── 保存 ───
    os.makedirs(MODEL_DIR, exist_ok=True)
    model.save_model(RANKER_PATH)
    logger.info("保存: %s", RANKER_PATH)

    # tracker は binary model と共用（再保存しない）

    return {
        "ndcg3_train":   round(ndcg3_train, 4) if ndcg3_train else None,
        "ndcg3_valid":   round(ndcg3_valid, 4) if ndcg3_valid else None,
        "best_iteration": model.best_iteration,
        "train_groups":  len(train_groups),
        "valid_groups":  len(valid_groups_sz),
    }


# ============================================================
# 推論クラス
# ============================================================


class LGBMRanker:
    """
    学習済み LambdaRank モデルで各馬のランクスコアを返す。

    LGBMPredictor の binary 確率に加算ブレンドして
    レース内ランク品質（三連複精度）を向上させる。
    """

    def __init__(self):
        self._model = None
        self._tracker = None
        self._sire_tracker = None
        self._loaded = False

    def load(self) -> bool:
        if self._loaded:
            return True
        if not os.path.exists(RANKER_PATH):
            return False
        from src.ml.lgbm_model import SIRE_STATS_PATH, STATS_PATH
        if not os.path.exists(STATS_PATH):
            return False
        try:
            import lightgbm as lgb

            self._model = lgb.Booster(model_file=RANKER_PATH)
            with open(STATS_PATH, "rb") as f:
                self._tracker = pickle.load(f)
            if os.path.exists(SIRE_STATS_PATH):
                with open(SIRE_STATS_PATH, "rb") as f:
                    self._sire_tracker = pickle.load(f)
            self._loaded = True
            logger.info("LGBMRanker loaded: %s", RANKER_PATH)
            return True
        except Exception as e:
            logger.warning("LGBMRanker load failed: %s", e, exc_info=True)
            return False

    def predict_race(
        self,
        race_dict: dict,
        horse_dicts: List[dict],
    ) -> Dict[str, float]:
        """
        各馬のランクスコアを返す (高いほど上位予測)。

        Args:
            race_dict:   {date, venue, surface, distance, condition,
                          field_count, is_jra, grade, venue_code}
            horse_dicts: [{horse_id, jockey_id, trainer_id, gate_no, horse_no,
                           sex, age, weight_kg, horse_weight, weight_change,
                           sire_id (optional), bms_id (optional), ...}]

        Returns:
            {horse_id: float}  — raw rank score (シグモイド後に近似確率として使用可)
        """
        if not self._loaded and not self.load():
            return {}

        import numpy as np

        from src.ml.lgbm_model import (
            FEATURE_COLUMNS,
            _add_race_relative_features,
            _extract_features,
            _load_horse_sire_map,
        )

        # sire_map は tracker に含まれないので簡易版で補完
        sire_map_lazy = getattr(self, "_sire_map_cache", None)
        if sire_map_lazy is None:
            try:
                self._sire_map_cache = _load_horse_sire_map()
                sire_map_lazy = self._sire_map_cache
            except Exception:
                sire_map_lazy = {}

        features = []
        ids      = []
        for h in horse_dicts:
            hid = h.get("horse_id", "")
            if not h.get("sire_id") and sire_map_lazy:
                sid, bid = sire_map_lazy.get(hid, ("", ""))
                h = dict(h, sire_id=sid, bms_id=bid)
            feat = _extract_features(h, race_dict, self._tracker, self._sire_tracker)
            features.append(feat)
            ids.append(hid)

        if not features:
            return {}

        # ① 相対特徴量を一括設定 (2パス)
        _add_race_relative_features(features)

        feat_cols = FEATURE_COLUMNS
        n_model = getattr(self._model, "num_feature", lambda: len(feat_cols))()
        if n_model < len(feat_cols):
            feat_cols = feat_cols[:n_model]

        X = np.array(
            [[float(f[c]) if f[c] is not None else float("nan") for c in feat_cols]
             for f in features],
            dtype=np.float32,
        )
        scores = self._model.predict(X)  # raw rank scores

        return {hid: float(s) for hid, s in zip(ids, scores)}


# ============================================================
# __main__: 学習 + 評価
# ============================================================

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    metrics = train_ranker(valid_days=30)
    print("\n=== LambdaRank 学習完了 ===")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
