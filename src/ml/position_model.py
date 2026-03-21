"""
600m地点（最終コーナー）位置取り予測 LightGBM モデル  (Phase B)

目的:
  展開偏差値の精度向上のため、各馬の最終コーナー相対位置を予測する。
  既存の固定マッピング (_estimate_position) を置き換える。

使い方:
  python -m src.ml.position_model                    # 学習 + 評価
  python -m src.ml.position_model --evaluate-only    # 保存済みモデルで評価のみ
  python -m src.ml.position_model --importance       # 特徴量重要度を表示

設計:
  - 目的変数: rel_position (最終コーナー位置/頭数, 0.0=先頭, 1.0=最後方)
  - 特徴量: レース条件 + 馬の当日情報 + 過去位置ローリング + 騎手位置傾向 + ペース文脈
  - 時系列分割 (train: ~cutoff / val: cutoff~)
"""

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import lightgbm as lgb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.log import get_logger

logger = get_logger(__name__)

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
ML_DATA_DIR = os.path.join(_PROJECT_ROOT, "data", "ml")
MODEL_PATH = os.path.join(ML_DATA_DIR, "position_model.txt")
META_PATH = os.path.join(ML_DATA_DIR, "position_meta.json")
JOCKEY_CACHE_PATH = os.path.join(ML_DATA_DIR, "position_jockey_cache.json")

WARMUP_DAYS = 90
VAL_MONTHS = 4
DISTANCE_BANDS = [(0, 1399, "sprint"), (1400, 1799, "mile"),
                  (1800, 2199, "middle"), (2200, 9999, "long")]

CATEGORICAL_FEATURES = [
    "venue_code", "surface_enc", "condition_enc", "grade_enc", "sex_enc",
]

LGB_PARAMS = {
    "objective": "regression",
    "metric": "mae",
    "boosting_type": "gbdt",
    "num_leaves": 63,
    "learning_rate": 0.02,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "min_child_samples": 30,
    "reg_alpha": 0.1,
    "reg_lambda": 0.5,
    "verbose": -1,
    "n_jobs": -1,
    "seed": 42,
}

FEATURE_COLUMNS = [
    "venue_code", "surface_enc", "distance", "condition_enc",
    "field_count", "grade_enc", "is_jra",
    "gate_no", "horse_no", "weight_kg", "sex_enc", "age",
    "horse_weight", "weight_change", "relative_horse_no",
    "n_corners",
    "hist_pos_mean3", "hist_pos_mean5", "hist_pos_latest", "hist_pos_trend",
    "hist_pos_std5", "hist_pos_best5", "hist_pos_worst5",
    "hist_finish_mean3", "hist_finish_mean5",
    "hist_l3f_mean5", "hist_run_count",
    "hist_pos_same_surface", "hist_pos_same_venue",
    "hist_win_rate5", "hist_place3_rate5",
    "hist_dist_change", "days_since_last",
    "jockey_avg_pos", "jockey_course_avg_pos", "jockey_ride_count",
    "pace_n_front", "pace_front_ratio", "horse_style_est",
]


def _distance_band(d: int) -> str:
    for lo, hi, label in DISTANCE_BANDS:
        if lo <= d <= hi:
            return label
    return "mile"


def _relative_position(corners: list, field_count: int) -> float:
    if not corners or field_count <= 0:
        return np.nan
    return corners[-1] / max(field_count, 1)


def _encode_surface(s: str) -> int:
    return 0 if "芝" in str(s) else 1


def _encode_condition(c: str) -> int:
    m = {"良": 0, "稍重": 1, "稍": 1, "重": 2, "不良": 3, "不": 3}
    return m.get(str(c), 0)


def _encode_grade(g: str) -> int:
    m = {"新馬": 0, "未勝利": 1, "1勝": 2, "500万": 2, "2勝": 3, "1000万": 3,
         "3勝": 4, "1600万": 4, "OP": 5, "L": 5, "G3": 6, "G2": 7, "G1": 8,
         "交流重賞": 6, "その他": 2}
    return m.get(str(g), 2)


def _encode_sex(s: str) -> int:
    return {"牡": 0, "牝": 1, "セ": 2, "セン": 2}.get(str(s), 0)


def load_ml_data() -> pd.DataFrame:
    """last3f_model と同形式でデータ読込"""
    from src.ml.last3f_model import load_ml_data as _load
    return _load()


def build_feature_table(df: pd.DataFrame) -> pd.DataFrame:
    """位置取り予測用の特徴量を構築"""
    t0 = time.time()
    df = df.copy()
    df["race_date_dt"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df.dropna(subset=["race_date_dt"]).sort_values(["race_date_dt", "race_id", "horse_no"])
    df = df.reset_index(drop=True)

    df["surface_enc"] = df["surface"].apply(_encode_surface)
    df["condition_enc"] = df["condition"].apply(_encode_condition)
    df["grade_enc"] = df["grade"].apply(_encode_grade)
    df["sex_enc"] = df["sex"].apply(_encode_sex)
    df["dist_band"] = df["distance"].apply(_distance_band)

    df["rel_position"] = df.apply(
        lambda r: _relative_position(r["positions_corners"], r["field_count"]), axis=1
    )
    df["relative_horse_no"] = df["horse_no"] / df["field_count"].clip(lower=1)
    df["course_id"] = df["venue_code"] + "_" + df["surface_enc"].astype(str) + "_" + df["distance"].astype(str)
    df["n_corners"] = df["positions_corners"].apply(lambda x: len(x) if isinstance(x, list) else 0)

    logger.info("過去走ローリング特徴量を構築中...")
    _build_horse_history_features(df)

    logger.info("騎手位置特徴量を構築中...")
    _build_jockey_position_features(df)

    logger.info("ペース文脈特徴量を構築中...")
    _build_pace_context_features(df)

    df["days_since_last"] = df.groupby("horse_id")["race_date_dt"].diff().dt.days

    logger.info(f"特徴量構築完了: {len(df):,}行, {time.time()-t0:.1f}秒")
    return df


def _build_horse_history_features(df: pd.DataFrame) -> None:
    df.sort_values(["horse_id", "race_date_dt"], inplace=True)
    g = df.groupby("horse_id")

    pos = g["rel_position"]
    df["hist_pos_mean3"] = pos.transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean())
    df["hist_pos_mean5"] = pos.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df["hist_pos_latest"] = pos.transform(lambda x: x.shift(1))
    df["hist_pos_trend"] = df["hist_pos_latest"] - df["hist_pos_mean5"]
    df["hist_pos_std5"] = pos.transform(lambda x: x.shift(1).rolling(5, min_periods=2).std())
    df["hist_pos_best5"] = pos.transform(lambda x: x.shift(1).rolling(5, min_periods=1).min())
    df["hist_pos_worst5"] = pos.transform(lambda x: x.shift(1).rolling(5, min_periods=1).max())

    fp = g["finish_pos"]
    df["hist_finish_mean3"] = fp.transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean())
    df["hist_finish_mean5"] = fp.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())

    l3f = g["last_3f_sec"]
    df["hist_l3f_mean5"] = l3f.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())

    df["hist_run_count"] = g.cumcount()

    _build_conditional_pos_rolling(df, "hist_pos_same_surface", "surface_enc")
    _build_conditional_pos_rolling(df, "hist_pos_same_venue", "venue_code")

    df["_win"] = (df["finish_pos"] == 1).astype(float)
    df["_place3"] = (df["finish_pos"] <= 3).astype(float)
    gw = df.groupby("horse_id")
    df["hist_win_rate5"] = gw["_win"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df["hist_place3_rate5"] = gw["_place3"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df.drop(columns=["_win", "_place3"], inplace=True)

    dist = g["distance"]
    df["hist_dist_change"] = df["distance"] - dist.transform(lambda x: x.shift(1))

    df.sort_values(["race_date_dt", "race_id", "horse_no"], inplace=True)


def _build_conditional_pos_rolling(df: pd.DataFrame, out_col: str, cond_col: str) -> None:
    results = np.full(len(df), np.nan)
    buf: Dict[Tuple[str, object], List[float]] = defaultdict(list)
    df_sorted = df.sort_values(["horse_id", "race_date_dt"])
    prev_hid = None
    for idx in df_sorted.index:
        hid = df_sorted.at[idx, "horse_id"]
        cond_val = df_sorted.at[idx, cond_col]
        rp = df_sorted.at[idx, "rel_position"]
        if hid != prev_hid:
            buf.clear()
            prev_hid = hid
        key = (hid, cond_val)
        hist = buf[key]
        if hist:
            results[idx] = np.mean(hist[-5:])
        if not np.isnan(rp) and 0 <= rp <= 1:
            buf[key].append(rp)
    df[out_col] = results


def _build_jockey_position_features(df: pd.DataFrame) -> None:
    df.sort_values(["race_date_dt", "race_id"], inplace=True)
    j_pos_sum: Dict[str, float] = defaultdict(float)
    j_pos_cnt: Dict[str, int] = defaultdict(int)
    j_course_pos_sum: Dict[Tuple[str, str], float] = defaultdict(float)
    j_course_pos_cnt: Dict[Tuple[str, str], int] = defaultdict(int)
    j_rides: Dict[str, int] = defaultdict(int)

    j_avg = np.full(len(df), np.nan)
    j_course_avg = np.full(len(df), np.nan)
    j_ride_count = np.full(len(df), np.nan)
    prev_date = None
    date_batch: List[int] = []

    for idx in df.index:
        current_date = df.at[idx, "race_date_dt"]
        if prev_date is not None and current_date != prev_date:
            for i in date_batch:
                jid = df.at[i, "jockey_id"]
                cid = df.at[i, "course_id"]
                rp = df.at[i, "rel_position"]
                j_rides[jid] += 1
                if not np.isnan(rp) and 0 <= rp <= 1:
                    j_pos_sum[jid] += rp
                    j_pos_cnt[jid] += 1
                    j_course_pos_sum[(jid, cid)] += rp
                    j_course_pos_cnt[(jid, cid)] += 1
            date_batch = []

        jid = df.at[idx, "jockey_id"]
        cid = df.at[idx, "course_id"]
        if j_pos_cnt[jid] > 0:
            j_avg[idx] = j_pos_sum[jid] / j_pos_cnt[jid]
        if j_course_pos_cnt[(jid, cid)] > 0:
            j_course_avg[idx] = j_course_pos_sum[(jid, cid)] / j_course_pos_cnt[(jid, cid)]
        j_ride_count[idx] = j_rides[jid]
        date_batch.append(idx)
        prev_date = current_date

    if date_batch:
        for i in date_batch:
            jid = df.at[i, "jockey_id"]
            cid = df.at[i, "course_id"]
            rp = df.at[i, "rel_position"]
            j_rides[jid] += 1
            if not np.isnan(rp) and 0 <= rp <= 1:
                j_pos_sum[jid] += rp
                j_pos_cnt[jid] += 1
                j_course_pos_sum[(jid, cid)] += rp
                j_course_pos_cnt[(jid, cid)] += 1

    df["jockey_avg_pos"] = j_avg
    df["jockey_course_avg_pos"] = j_course_avg
    df["jockey_ride_count"] = j_ride_count
    df.sort_values(["race_date_dt", "race_id", "horse_no"], inplace=True)


def _build_pace_context_features(df: pd.DataFrame) -> None:
    horse_style: Dict[str, float] = {}
    df.sort_values(["race_date_dt", "race_id"], inplace=True)
    n_front = np.full(len(df), np.nan)
    front_ratio = np.full(len(df), np.nan)
    self_style = np.full(len(df), np.nan)

    for race_id, group in df.groupby("race_id"):
        fc = max(group["field_count"].iloc[0], 1)
        front_cnt = 0
        for idx in group.index:
            hid = df.at[idx, "horse_id"]
            avg_pos = horse_style.get(hid, 0.4)
            self_style[idx] = avg_pos
            if avg_pos <= 0.3:
                front_cnt += 1
        for idx in group.index:
            n_front[idx] = front_cnt
            front_ratio[idx] = front_cnt / fc
        for idx in group.index:
            hid = df.at[idx, "horse_id"]
            rp = df.at[idx, "rel_position"]
            if not np.isnan(rp):
                old = horse_style.get(hid)
                horse_style[hid] = rp if old is None else old * 0.7 + rp * 0.3

    df["pace_n_front"] = n_front
    df["pace_front_ratio"] = front_ratio
    df["horse_style_est"] = self_style
    df.sort_values(["race_date_dt", "race_id", "horse_no"], inplace=True)


def prepare_datasets(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    mask_target = df["rel_position"].notna() & (df["rel_position"] >= 0) & (df["rel_position"] <= 1)
    min_date = df["race_date_dt"].min()
    warmup_end = min_date + timedelta(days=WARMUP_DAYS)
    mask_warmup = df["race_date_dt"] >= warmup_end
    mask_hist = df["hist_run_count"] >= 1
    mask_fc = df["field_count"] >= 4

    valid = df[mask_target & mask_warmup & mask_hist & mask_fc].copy()
    logger.info(f"有効データ: {len(valid):,}行")

    max_date = valid["race_date_dt"].max()
    cutoff = max_date - timedelta(days=30 * VAL_MONTHS)
    train = valid[valid["race_date_dt"] < cutoff]
    val = valid[valid["race_date_dt"] >= cutoff]
    logger.info(f"Train: {len(train):,}行, Val: {len(val):,}行")
    return train, val


def train_model(train_df: pd.DataFrame, val_df: pd.DataFrame) -> lgb.Booster:
    X_train = train_df[FEATURE_COLUMNS].copy()
    y_train = train_df["rel_position"]
    X_val = val_df[FEATURE_COLUMNS].copy()
    y_val = val_df["rel_position"]

    for col in CATEGORICAL_FEATURES:
        if col in X_train.columns:
            X_train[col] = X_train[col].astype("category")
            X_val[col] = X_val[col].astype("category")

    dtrain = lgb.Dataset(X_train, label=y_train, categorical_feature=CATEGORICAL_FEATURES)
    dval = lgb.Dataset(X_val, label=y_val, reference=dtrain, categorical_feature=CATEGORICAL_FEATURES)

    model = lgb.train(
        LGB_PARAMS,
        dtrain,
        num_boost_round=3000,
        valid_sets=[dtrain, dval],
        valid_names=["train", "val"],
        callbacks=[lgb.log_evaluation(period=200), lgb.early_stopping(stopping_rounds=100)],
    )
    logger.info(f"学習完了: {model.best_iteration} rounds, best MAE={model.best_score['val']['l1']:.4f}")
    return model


def evaluate_model(model: lgb.Booster, val_df: pd.DataFrame) -> dict:
    X_val = val_df[FEATURE_COLUMNS].copy()
    for col in CATEGORICAL_FEATURES:
        if col in X_val.columns:
            X_val[col] = X_val[col].astype("category")

    y_true = val_df["rel_position"].values
    y_pred = model.predict(X_val, num_iteration=model.best_iteration)
    y_pred = np.clip(y_pred, 0.0, 1.0)

    y_baseline = val_df["hist_pos_mean3"].values.copy()
    bl_mask = np.isnan(y_baseline)
    y_baseline[bl_mask] = val_df.loc[bl_mask, "hist_pos_mean5"].values
    still_nan = np.isnan(y_baseline)
    y_baseline[still_nan] = 0.5

    mae_model = np.mean(np.abs(y_true - y_pred))
    mae_baseline = np.mean(np.abs(y_true - y_baseline))
    within_01 = np.mean(np.abs(y_true - y_pred) <= 0.1) * 100
    within_02 = np.mean(np.abs(y_true - y_pred) <= 0.2) * 100
    improvement_mae = (mae_baseline - mae_model) / mae_baseline * 100 if mae_baseline > 0 else 0.0

    # JRA/NAR別
    jra_mask = val_df["is_jra"].values.astype(bool)
    metrics_jra = _calc_subset_metrics(y_true, y_pred, y_baseline, jra_mask)
    metrics_nar = _calc_subset_metrics(y_true, y_pred, y_baseline, ~jra_mask)

    # 芝/ダート別
    turf_mask = val_df["surface_enc"].values == 0
    metrics_turf = _calc_subset_metrics(y_true, y_pred, y_baseline, turf_mask)
    metrics_dirt = _calc_subset_metrics(y_true, y_pred, y_baseline, ~turf_mask)

    # 距離帯別
    dist_metrics = {}
    for lo, hi, label in DISTANCE_BANDS:
        mask = val_df["distance"].between(lo, hi).values
        if mask.sum() > 0:
            dist_metrics[label] = _calc_subset_metrics(y_true, y_pred, y_baseline, mask)

    return {
        "val_size": len(y_true),
        "mae_model": round(mae_model, 4),
        "mae_baseline": round(mae_baseline, 4),
        "improvement_mae_pct": round(improvement_mae, 2),
        "within_0.1": round(within_01, 2),
        "within_0.2": round(within_02, 2),
        "best_iteration": model.best_iteration,
        "by_org": {"JRA": metrics_jra, "NAR": metrics_nar},
        "by_surface": {"芝": metrics_turf, "ダート": metrics_dirt},
        "by_distance": dist_metrics,
    }


def _calc_subset_metrics(y_true, y_pred, y_baseline, mask) -> dict:
    if mask.sum() == 0:
        return {"n": 0}
    yt, yp, yb = y_true[mask], y_pred[mask], y_baseline[mask]
    return {
        "n": int(mask.sum()),
        "mae_model": round(float(np.mean(np.abs(yt - yp))), 4),
        "mae_baseline": round(float(np.mean(np.abs(yt - yb))), 4),
        "within_0.1": round(float(np.mean(np.abs(yt - yp) <= 0.1) * 100), 2),
        "within_0.2": round(float(np.mean(np.abs(yt - yp) <= 0.2) * 100), 2),
    }


def save_model(model: lgb.Booster, metrics: dict) -> None:
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    model.save_model(MODEL_PATH)
    meta = {
        "feature_columns": FEATURE_COLUMNS,
        "categorical_features": CATEGORICAL_FEATURES,
        "metrics": metrics,
        "trained_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "best_iteration": model.best_iteration,
    }
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    logger.info(f"モデル保存: {MODEL_PATH}")


def load_model() -> Optional[lgb.Booster]:
    if not os.path.exists(MODEL_PATH):
        return None
    return lgb.Booster(model_file=MODEL_PATH)


def load_meta() -> Optional[dict]:
    if not os.path.exists(META_PATH):
        return None
    with open(META_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def build_jockey_cache(df: pd.DataFrame) -> None:
    valid = df[df["rel_position"].notna() & (df["rel_position"] >= 0) & (df["rel_position"] <= 1)]
    avg = valid.groupby("jockey_id")["rel_position"].mean().to_dict()
    course_avg = valid.groupby(["jockey_id", "course_id"])["rel_position"].mean()
    course_avg_dict = {f"{jid}|{cid}": v for (jid, cid), v in course_avg.items()}
    rides = df.groupby("jockey_id").size().to_dict()
    cache = {
        "avg": {k: round(v, 4) for k, v in avg.items()},
        "course_avg": {k: round(v, 4) for k, v in course_avg_dict.items()},
        "rides": rides,
    }
    with open(JOCKEY_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    logger.info(f"騎手キャッシュ保存: {JOCKEY_CACHE_PATH}")


# ============================================================
# 予測インターフェース
# ============================================================

class PositionPredictor:
    """600m地点（最終コーナー）相対位置を予測。PaceDeviationCalculatorから使用"""

    def __init__(self):
        self.model: Optional[lgb.Booster] = None
        self.meta: Optional[dict] = None
        self._jockey_cache: Dict[str, float] = {}
        self._jockey_course_cache: Dict[Tuple[str, str], float] = {}
        self._jockey_ride_cache: Dict[str, int] = {}
        self._loaded = False

    def load(self) -> bool:
        self.model = load_model()
        self.meta = load_meta()
        if self.model is not None:
            self._loaded = True
            self._load_jockey_cache()
            return True
        return False

    @property
    def is_available(self) -> bool:
        return self._loaded and self.model is not None

    def _load_jockey_cache(self) -> None:
        if os.path.exists(JOCKEY_CACHE_PATH):
            with open(JOCKEY_CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._jockey_cache = data.get("avg", {})
            self._jockey_course_cache = {
                tuple(k.split("|")): v for k, v in data.get("course_avg", {}).items()
            }
            self._jockey_ride_cache = data.get("rides", {})

    def predict(self, horse, race_info, pace_context: dict = None) -> Optional[float]:
        if not self.is_available:
            return None
        features = self._build_features(horse, race_info, pace_context)
        if features is None:
            return None
        feat_df = pd.DataFrame([features])[FEATURE_COLUMNS]
        for col in CATEGORICAL_FEATURES:
            if col in feat_df.columns:
                feat_df[col] = feat_df[col].astype("category")
        # None や文字列型（NAR 出走表で horse_weight 等が欠損する場合）を数値に変換
        for col in feat_df.columns:
            if col not in CATEGORICAL_FEATURES:
                feat_df[col] = pd.to_numeric(feat_df[col], errors="coerce")
        pred = self.model.predict(feat_df, num_iteration=self.model.best_iteration)[0]
        return float(np.clip(pred, 0.0, 1.0))

    def _build_features(self, horse, race_info, pace_context) -> Optional[dict]:
        from src.models import PastRun

        course = race_info.course
        runs = horse.past_runs or []
        if not runs:
            return None

        # cornerデータがある走のみ使用（学習データと同じ条件: _relative_positionはcornerなしをNaN除外）
        # position_4c=finish_posフォールバックは学習データに含まれないため、推論時も除外
        pos_runs = [r for r in runs if r.positions_corners]
        pos_vals = [r.relative_position for r in pos_runs if r.relative_position is not None]
        # cornerデータが全くない場合のフォールバック: 全走の相対位置を使用（精度は低い）
        if not pos_vals:
            pos_runs = [r for r in runs if r.relative_position is not None]
            pos_vals = [r.relative_position for r in pos_runs]

        f = {}
        f["venue_code"] = course.venue_code
        f["surface_enc"] = 0 if course.surface == "芝" else 1
        f["distance"] = course.distance
        f["condition_enc"] = _encode_condition(
            race_info.track_condition_turf or race_info.track_condition_dirt or "良"
        )
        f["field_count"] = race_info.field_count
        f["grade_enc"] = _encode_grade(race_info.grade)
        f["is_jra"] = race_info.is_jra

        f["gate_no"] = horse.gate_no
        f["horse_no"] = horse.horse_no
        f["weight_kg"] = horse.weight_kg
        f["sex_enc"] = _encode_sex(horse.sex)
        f["age"] = horse.age
        f["horse_weight"] = horse.horse_weight
        f["weight_change"] = horse.weight_change
        f["relative_horse_no"] = horse.horse_no / max(race_info.field_count, 1)

        n_corners = 2
        if course.distance >= 1800:
            n_corners = 4
        elif course.distance >= 1600:
            n_corners = 3
        f["n_corners"] = n_corners

        f["hist_pos_mean3"] = np.mean(pos_vals[:3]) if pos_vals else np.nan
        f["hist_pos_mean5"] = np.mean(pos_vals[:5]) if pos_vals else np.nan
        f["hist_pos_latest"] = pos_vals[0] if pos_vals else np.nan
        f["hist_pos_trend"] = (
            (f["hist_pos_latest"] - f["hist_pos_mean5"])
            if pos_vals and len(pos_vals) >= 5 else np.nan
        )
        f["hist_pos_std5"] = float(np.std(pos_vals[:5])) if len(pos_vals) >= 2 else np.nan
        f["hist_pos_best5"] = min(pos_vals[:5]) if pos_vals else np.nan
        f["hist_pos_worst5"] = max(pos_vals[:5]) if pos_vals else np.nan

        fp_vals = [r.finish_pos for r in runs]
        f["hist_finish_mean3"] = np.mean(fp_vals[:3]) if fp_vals else np.nan
        f["hist_finish_mean5"] = np.mean(fp_vals[:5]) if fp_vals else np.nan

        l3f_vals = [r.last_3f_sec for r in runs if r.last_3f_sec]
        f["hist_l3f_mean5"] = np.mean(l3f_vals[:5]) if l3f_vals else np.nan
        f["hist_run_count"] = len(runs)

        same_surf = [r.relative_position for r in pos_runs if r.surface == course.surface]
        f["hist_pos_same_surface"] = np.mean(same_surf[:5]) if same_surf else np.nan
        same_venue = [r.relative_position for r in pos_runs if r.venue == course.venue]
        f["hist_pos_same_venue"] = np.mean(same_venue[:5]) if same_venue else np.nan

        recent5 = runs[:5]
        f["hist_win_rate5"] = sum(1 for r in recent5 if r.finish_pos == 1) / max(len(recent5), 1)
        f["hist_place3_rate5"] = sum(1 for r in recent5 if r.finish_pos <= 3) / max(len(recent5), 1)
        f["hist_dist_change"] = course.distance - runs[0].distance if runs else np.nan

        try:
            race_dt = datetime.strptime(race_info.race_date, "%Y-%m-%d")
            last_dt = datetime.strptime(runs[0].race_date, "%Y-%m-%d")
            f["days_since_last"] = (race_dt - last_dt).days
        except Exception:
            f["days_since_last"] = np.nan

        jid = horse.jockey_id or ""
        cid_key = f"{course.venue_code}_{f['surface_enc']}_{course.distance}"
        f["jockey_avg_pos"] = self._jockey_cache.get(jid, np.nan)
        f["jockey_course_avg_pos"] = self._jockey_course_cache.get((jid, cid_key), np.nan)
        f["jockey_ride_count"] = self._jockey_ride_cache.get(jid, 0)

        pc = pace_context or {}
        f["pace_n_front"] = pc.get("n_front", np.nan)
        f["pace_front_ratio"] = pc.get("front_ratio", np.nan)
        f["horse_style_est"] = f["hist_pos_mean5"]

        return f


def print_report(metrics: dict, fi=None) -> None:
    print("\n" + "=" * 60)
    print("  4角位置取り予測モデル  学習レポート")
    print("=" * 60)
    print(f"\n検証データ: {metrics['val_size']:,}行")
    print(f"\n{'指標':<20} {'モデル':>10} {'ベースライン':>12} {'改善':>8}")
    print("-" * 55)
    print(f"{'MAE':<20} {metrics['mae_model']:>10.4f} {metrics['mae_baseline']:>12.4f} "
          f"{metrics.get('improvement_mae_pct', 0):>+7.2f}%")
    print(f"{'±0.1以内 (%)':<20} {metrics['within_0.1']:>10.2f}")
    print(f"{'±0.2以内 (%)':<20} {metrics['within_0.2']:>10.2f}")

    for label, key in [("JRA/NAR別", "by_org"), ("芝/ダート別", "by_surface"), ("距離帯別", "by_distance")]:
        sub = metrics.get(key, {})
        if not sub:
            continue
        print(f"\n  {label}:")
        for name, m in sub.items():
            if m.get("n", 0) == 0:
                continue
            print(f"    {name:<12} MAE={m['mae_model']:.4f} (BL={m['mae_baseline']:.4f}) "
                  f"±0.2={m.get('within_0.2', 0):.1f}%  n={m['n']:,}")

    if fi is not None:
        print(f"\n  特徴量重要度 Top 15:")
        for i, row in fi.head(15).iterrows():
            bar = "#" * int(row["pct"] / 2)
            print(f"    {i + 1:>2}. {row['feature']:<28} {row['pct']:>6.2f}%  {bar}")

    print(f"\n  学習ラウンド: {metrics['best_iteration']}")
    print("=" * 60)


def get_feature_importance(model: lgb.Booster) -> pd.DataFrame:
    imp = model.feature_importance(importance_type="gain")
    names = model.feature_name()
    fi = pd.DataFrame({"feature": names, "importance": imp})
    fi = fi.sort_values("importance", ascending=False).reset_index(drop=True)
    fi["pct"] = (fi["importance"] / fi["importance"].sum() * 100).round(2)
    return fi


def run_training_pipeline() -> dict:
    t_all = time.time()
    logger.info("Step 1/6: データ読込")
    df = load_ml_data()
    logger.info("Step 2/6: 特徴量構築")
    df = build_feature_table(df)
    logger.info("Step 3/6: データ分割")
    train_df, val_df = prepare_datasets(df)
    logger.info("Step 4/6: モデル学習")
    model = train_model(train_df, val_df)
    logger.info("Step 5/6: 評価")
    metrics = evaluate_model(model, val_df)
    fi = get_feature_importance(model)
    logger.info("Step 6/6: 保存")
    save_model(model, metrics)
    build_jockey_cache(df)
    print_report(metrics, fi)
    logger.info(f"パイプライン完了: {time.time()-t_all:.1f}秒")
    return metrics


def main():
    if "--evaluate-only" in sys.argv:
        model = load_model()
        if model is None:
            print("モデルが見つかりません")
            return
        df = load_ml_data()
        df = build_feature_table(df)
        _, val_df = prepare_datasets(df)
        metrics = evaluate_model(model, val_df)
        fi = get_feature_importance(model)
        print_report(metrics, fi)
    elif "--importance" in sys.argv:
        model = load_model()
        if model is None:
            print("モデルが見つかりません")
            return
        fi = get_feature_importance(model)
        print("\n特徴量重要度:")
        for i, row in fi.iterrows():
            bar = "#" * int(row["pct"] / 2)
            print(f"  {i + 1:>2}. {row['feature']:<28} {row['pct']:>6.2f}%  {bar}")
    else:
        run_training_pipeline()


if __name__ == "__main__":
    main()
