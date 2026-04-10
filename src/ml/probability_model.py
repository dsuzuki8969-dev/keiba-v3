"""
LightGBM 三連率予測モデル

3つの二値分類モデル（勝率・連対率・複勝率）を同時に学習し、
Platt Scaling でキャリブレーション（確率補正）を行う。

予測確率は予想オッズの算出や期待値計算の基盤となる。
"""

import json
import os
import pickle
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import lightgbm as lgb
import numpy as np

from src.log import get_logger

logger = get_logger(__name__)

_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MODEL_DIR = os.path.join(_BASE, "data", "models")

TARGET_NAMES = ["win", "top2", "top3"]
TARGET_LABELS = {"win": "勝率", "top2": "連対率", "top3": "複勝率"}

LGB_PARAMS = {
    "objective": "binary",
    "metric": ["binary_logloss", "auc"],
    "boosting_type": "gbdt",
    "num_leaves": 31,           # 63→31: 浅い木でツリー数増加を促進
    "learning_rate": 0.005,     # 0.02→0.005: 非常に緩やかに学習
    "feature_fraction": 0.7,    # 0.8→0.7: ランダム性を増やしてツリー分散化
    "bagging_fraction": 0.7,    # 0.8→0.7: 同上
    "bagging_freq": 5,
    "min_child_samples": 100,   # 50→100: 過学習抑制を強化
    "lambda_l1": 1.0,           # 0.1→1.0: 正則化強化
    "lambda_l2": 5.0,           # 1.0→5.0: 正則化強化
    "max_depth": 4,             # 7→4: さらに浅い木
    "verbose": -1,
    "seed": 42,
    # is_unbalance削除: 陽性重み増幅が即過学習の原因
    # Platt Scalingで事後較正するため不要
}


class PlattScaler:
    """Platt Scaling: ロジスティック回帰で確率をキャリブレーション"""

    def __init__(self):
        self.a = 0.0
        self.b = 0.0
        self._fitted = False

    def fit(self, raw_probs: np.ndarray, labels: np.ndarray):
        from sklearn.linear_model import LogisticRegression

        lr = LogisticRegression(C=1e10, solver="lbfgs", max_iter=1000)
        lr.fit(raw_probs.reshape(-1, 1), labels)
        self.a = float(lr.coef_[0][0])
        self.b = float(lr.intercept_[0])
        self._fitted = True

    def transform(self, raw_probs: np.ndarray) -> np.ndarray:
        if not self._fitted:
            return raw_probs
        logit = self.a * raw_probs + self.b
        return 1.0 / (1.0 + np.exp(-logit))

    def to_dict(self):
        return {"a": self.a, "b": self.b, "fitted": self._fitted}

    @classmethod
    def from_dict(cls, d):
        s = cls()
        s.a = d["a"]
        s.b = d["b"]
        s._fitted = d["fitted"]
        return s


def train_probability_models(valid_days: int = 30) -> dict:
    """
    3つのLightGBMモデル（win/top2/top3）を学習し、
    Platt Scaling でキャリブレーションしたモデルを保存する。
    """
    from src.ml.lgbm_model import (
        CATEGORICAL_FEATURES,
        FEATURE_COLUMNS,
        RollingStatsTracker,
        RollingSireTracker,
        _extract_features,
        _load_horse_sire_map,
        _load_ml_races,
    )
    from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

    races = _load_ml_races()
    if not races:
        raise ValueError("ML data not found")

    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    if len(all_dates) < 14:
        raise ValueError(f"Insufficient dates ({len(all_dates)})")

    split_idx = max(1, len(all_dates) - valid_days)
    split_date = all_dates[split_idx]
    cal_split = all_dates[max(1, split_idx - 15)]

    logger.info("=" * 60)
    logger.info("三連率予測モデル学習開始 (win/top2/top3)")
    logger.info("=" * 60)
    logger.info("学習期間: %s ~ %s", all_dates[0], all_dates[split_idx - 1])
    logger.info("キャリブ: %s ~ %s", cal_split, all_dates[split_idx - 1])
    logger.info("検証期間: %s ~ %s", split_date, all_dates[-1])

    # 血統マップをロード（改善C）
    sire_map = _load_horse_sire_map()
    sire_tracker = RollingSireTracker()
    logger.info("血統マップ: %d エントリー", len(sire_map))

    tracker = RollingStatsTracker()
    train_rows, cal_rows, valid_rows = [], [], []
    train_labels = {"win": [], "top2": [], "top3": []}
    cal_labels = {"win": [], "top2": [], "top3": []}
    valid_labels = {"win": [], "top2": [], "top3": []}
    valid_race_sizes = []

    for race in races:
        date_str = race.get("date", "")
        is_valid = date_str >= split_date
        is_cal = not is_valid and date_str >= cal_split

        race_feats = []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            h_with_sire = dict(h, sire_id=sid, bms_id=bid)
            feat = _extract_features(h_with_sire, race, tracker, sire_tracker)
            labels = {
                "win": 1 if fp == 1 else 0,
                "top2": 1 if fp <= 2 else 0,
                "top3": 1 if fp <= 3 else 0,
            }
            race_feats.append((feat, labels))

        if race_feats:
            if is_valid:
                for feat, labels in race_feats:
                    valid_rows.append(feat)
                    for k in TARGET_NAMES:
                        valid_labels[k].append(labels[k])
                valid_race_sizes.append(len(race_feats))
            elif is_cal:
                for feat, labels in race_feats:
                    cal_rows.append(feat)
                    for k in TARGET_NAMES:
                        cal_labels[k].append(labels[k])
            else:
                for feat, labels in race_feats:
                    train_rows.append(feat)
                    for k in TARGET_NAMES:
                        train_labels[k].append(labels[k])

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    if not train_rows:
        raise ValueError("No training samples")

    # キャリブレーションデータが少ない場合、学習データの末尾を使う
    if len(cal_rows) < 500:
        cal_rows = train_rows[-2000:]
        for k in TARGET_NAMES:
            cal_labels[k] = train_labels[k][-2000:]

    logger.info("Train: %d / Calibration: %d / Valid: %d",
                len(train_rows), len(cal_rows), len(valid_rows))

    def _to_np(rows):
        mat = []
        for f in rows:
            mat.append([float(f.get(c)) if f.get(c) is not None else float("nan")
                        for c in FEATURE_COLUMNS])
        return np.array(mat, dtype=np.float32)

    X_train = _to_np(train_rows)
    X_cal = _to_np(cal_rows)
    X_valid = _to_np(valid_rows)

    models = {}
    scalers = {}
    all_metrics = {}

    for target in TARGET_NAMES:
        logger.info("")
        logger.info("━━━ %s モデル学習 ━━━", TARGET_LABELS[target])

        y_train = np.array(train_labels[target], dtype=np.int32)
        y_cal = np.array(cal_labels[target], dtype=np.int32)
        y_valid = np.array(valid_labels[target], dtype=np.int32)

        dtrain = lgb.Dataset(
            X_train, label=y_train,
            feature_name=FEATURE_COLUMNS,
            categorical_feature=CATEGORICAL_FEATURES,
            free_raw_data=False,
        )
        dvalid = lgb.Dataset(
            X_valid, label=y_valid,
            feature_name=FEATURE_COLUMNS,
            categorical_feature=CATEGORICAL_FEATURES,
            reference=dtrain,
            free_raw_data=False,
        )

        model = lgb.train(
            LGB_PARAMS, dtrain,
            num_boost_round=5000,       # 3000→5000: lr低下分を補完
            valid_sets=[dtrain, dvalid],
            valid_names=["train", "valid"],
            callbacks=[
                lgb.log_evaluation(period=200),
                lgb.early_stopping(stopping_rounds=200),  # 100→200: 早期停止を緩和
            ],
        )

        # Platt Scaling on calibration set
        raw_cal = model.predict(X_cal)
        scaler = PlattScaler()
        scaler.fit(raw_cal, y_cal)

        raw_valid = model.predict(X_valid)
        cal_valid = scaler.transform(raw_valid)

        auc = roc_auc_score(y_valid, cal_valid)
        logloss = log_loss(y_valid, cal_valid)
        brier = brier_score_loss(y_valid, cal_valid)

        # キャリブレーション精度
        cal_bins = [(0, 0.10), (0.10, 0.20), (0.20, 0.35), (0.35, 0.50), (0.50, 1.0)]
        cal_lines = []
        for lo, hi in cal_bins:
            mask = (cal_valid >= lo) & (cal_valid < hi)
            if mask.sum() > 0:
                cal_lines.append(
                    f"  {lo:.2f}-{hi:.2f}: pred={cal_valid[mask].mean():.3f}"
                    f" actual={y_valid[mask].mean():.3f} n={mask.sum()}"
                )

        logger.info("AUC: %.4f  LogLoss: %.4f  Brier: %.4f", auc, logloss, brier)
        logger.info("キャリブレーション:")
        for line in cal_lines:
            logger.info(line)

        models[target] = model
        scalers[target] = scaler
        all_metrics[target] = {
            "auc": round(auc, 4),
            "logloss": round(logloss, 4),
            "brier": round(brier, 4),
            "best_iteration": model.best_iteration,
        }

    # レース単位の精度検証
    idx = 0
    correct_top1_win, correct_top1_place, total_eval = 0, 0, 0
    for g in valid_race_sizes:
        if g < 3:
            idx += g
            continue
        win_p = scalers["win"].transform(models["win"].predict(X_valid[idx:idx + g]))
        place_p = scalers["top3"].transform(models["top3"].predict(X_valid[idx:idx + g]))
        y_w = np.array(valid_labels["win"][idx:idx + g])
        y_p = np.array(valid_labels["top3"][idx:idx + g])

        if y_w[np.argmax(win_p)] == 1:
            correct_top1_win += 1
        if y_p[np.argmax(place_p)] == 1:
            correct_top1_place += 1
        total_eval += 1
        idx += g

    logger.info("")
    logger.info("レース単位精度 (%d レース):", total_eval)
    logger.info("  勝率Top1推し → 1着: %.1f%%",
                100 * correct_top1_win / max(total_eval, 1))
    logger.info("  複勝Top1推し → 3着内: %.1f%%",
                100 * correct_top1_place / max(total_eval, 1))

    # 保存
    os.makedirs(MODEL_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for target in TARGET_NAMES:
        models[target].save_model(os.path.join(MODEL_DIR, f"prob_{target}.txt"))

    with open(os.path.join(MODEL_DIR, "prob_scalers.pkl"), "wb") as f:
        pickle.dump({k: v.to_dict() for k, v in scalers.items()}, f)

    with open(os.path.join(MODEL_DIR, "prob_tracker.pkl"), "wb") as f:
        pickle.dump(tracker, f)

    with open(os.path.join(MODEL_DIR, "prob_sire_tracker.pkl"), "wb") as f:
        pickle.dump(sire_tracker, f)

    meta = {
        "created_at": ts,
        "metrics": all_metrics,
        "train_samples": len(train_rows),
        "cal_samples": len(cal_rows),
        "valid_samples": len(valid_rows),
        "valid_races": total_eval,
        "split_date": split_date,
        "top1_win_hit": round(correct_top1_win / max(total_eval, 1), 4),
        "top1_place_hit": round(correct_top1_place / max(total_eval, 1), 4),
    }
    with open(os.path.join(MODEL_DIR, "prob_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    logger.info("")
    logger.info("モデル保存: %s/prob_*.txt", MODEL_DIR)
    return meta


class ProbabilityPredictor:
    """学習済み三連率モデルによる推論"""

    def __init__(self):
        self._models: Dict[str, lgb.Booster] = {}
        self._scalers: Dict[str, PlattScaler] = {}
        self._tracker = None
        self._sire_tracker = None
        self._loaded = False

    def load(self) -> bool:
        if self._loaded:
            return True

        try:
            for target in TARGET_NAMES:
                path = os.path.join(MODEL_DIR, f"prob_{target}.txt")
                if not os.path.exists(path):
                    return False
                self._models[target] = lgb.Booster(model_file=path)

            scalers_path = os.path.join(MODEL_DIR, "prob_scalers.pkl")
            tracker_path = os.path.join(MODEL_DIR, "prob_tracker.pkl")
            if not os.path.exists(scalers_path) or not os.path.exists(tracker_path):
                return False

            with open(scalers_path, "rb") as f:
                raw = pickle.load(f)
                self._scalers = {k: PlattScaler.from_dict(v) for k, v in raw.items()}

            with open(tracker_path, "rb") as f:
                self._tracker = pickle.load(f)

            # 血統トラッカーはオプション
            sire_path = os.path.join(MODEL_DIR, "prob_sire_tracker.pkl")
            if os.path.exists(sire_path):
                with open(sire_path, "rb") as f:
                    self._sire_tracker = pickle.load(f)

            self._loaded = True
            logger.info("三連率モデルロード完了 (win/top2/top3)")
            return True
        except Exception as e:
            logger.warning("三連率モデルロード失敗: %s", e, exc_info=True)
            return False

    def predict_race(self, race_dict: dict, horse_dicts: List[dict]
                     ) -> Dict[str, Dict[str, float]]:
        """
        レース全馬の三連率を予測する。

        Returns:
            {horse_id: {"win": P(1着), "top2": P(2着以内), "top3": P(3着以内)}}
        """
        if not self._loaded and not self.load():
            return {}

        from src.ml.lgbm_model import FEATURE_COLUMNS, _extract_features

        features, ids = [], []
        for h in horse_dicts:
            feat = _extract_features(h, race_dict, self._tracker, self._sire_tracker)
            features.append(feat)
            ids.append(h.get("horse_id", ""))

        if not features:
            return {}

        # 後方互換: モデルの特徴量数とFEATURE_COLUMNSの整合チェック
        feat_cols = FEATURE_COLUMNS
        if self._models:
            sample_model = next(iter(self._models.values()))
            if hasattr(sample_model, "num_feature"):
                n = sample_model.num_feature()
                if n < len(feat_cols):
                    feat_cols = feat_cols[:n]
                elif n > len(feat_cols):
                    # モデルが現在のFEATURE_COLUMNSより多い特徴量で学習されている
                    # → 再学習が必要。predict_disable_shape_checkは使わない
                    logger.warning(
                        "三連率モデル特徴量不整合: モデル=%d, コード=%d → 再学習必要 (retrain_all.py --prob)",
                        n, len(feat_cols),
                    )
                    return {}

        X = np.array(
            [[float(f.get(c)) if f.get(c) is not None else float("nan")
              for c in feat_cols]
             for f in features],
            dtype=np.float32,
        )

        result = {}
        for hid_idx, hid in enumerate(ids):
            result[hid] = {}

        for target in TARGET_NAMES:
            raw = self._models[target].predict(X)
            calibrated = self._scalers[target].transform(raw)
            for i, hid in enumerate(ids):
                result[hid][target] = float(calibrated[i])

        # レース内正規化: 確率の合計を理論値に合わせる
        n = len(ids)
        if n > 0:
            for target, expected_sum in [("win", 1.0), ("top2", min(n, 2)), ("top3", min(n, 3))]:
                total = sum(result[hid][target] for hid in ids)
                if total > 0:
                    ratio = expected_sum / total
                    for hid in ids:
                        result[hid][target] = min(0.95, result[hid][target] * ratio)

        return result

    def predict_from_engine(self, race_info, horses,
                            evaluations=None) -> Dict[str, Dict[str, float]]:
        """RaceAnalysisEngine から呼ぶ用のラッパー"""
        cond = "良"
        if race_info.course.surface == "芝" and race_info.track_condition_turf:
            cond = race_info.track_condition_turf
        elif race_info.track_condition_dirt:
            cond = race_info.track_condition_dirt

        race_dict = {
            "date": race_info.race_date,
            "venue": race_info.venue,
            "surface": race_info.course.surface,
            "distance": race_info.course.distance,
            "condition": cond,
            "field_count": race_info.field_count,
            "is_jra": race_info.is_jra,
            "grade": race_info.grade,
            "venue_code": race_info.course.venue_code,
        }

        # Step2: evaluations から horse_id → (estimated_pos4c, estimated_l3f) マップ
        ev_map: Dict[str, tuple] = {}
        if evaluations:
            for ev in evaluations:
                hid_ev = ev.horse.horse_id
                ev_map[hid_ev] = (
                    getattr(ev.pace, "estimated_position_4c", None),
                    getattr(ev.pace, "estimated_last3f", None),
                )

        horse_dicts = []
        for h in horses:
            pos_est, l3f_est = ev_map.get(h.horse_id, (None, None))
            horse_dicts.append({
                "horse_id": h.horse_id,
                "jockey_id": h.jockey_id,
                "trainer_id": h.trainer_id,
                "gate_no": h.gate_no,
                "horse_no": h.horse_no,
                "sex": h.sex,
                "age": h.age,
                "weight_kg": h.weight_kg,
                "horse_weight": h.horse_weight,
                "weight_change": h.weight_change,
                # 血統特徴量 (改善C)
                "sire_id": getattr(h, "sire_id", "") or "",
                "bms_id": getattr(h, "maternal_grandsire_id", "") or "",
                # Tier1追加特徴量オーバーライド
                "is_jockey_change_override": int(h.is_jockey_change),
                # Step2 スタッキングオーバーライド
                "ml_pos_est_override": pos_est,
                "ml_l3f_est_override": l3f_est,
            })

        return self.predict_race(race_dict, horse_dicts)
