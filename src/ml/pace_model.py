"""
前半3F予測 LightGBM モデル  (Phase C)

目的:
  レース前半3Fタイムを予測し、ペースタイプ(HH/HM/MM/MS/SS)推定の精度を上げる。
  ルールベースPacePredictorの補完として使用。

使い方:
  python -m src.ml.pace_model                    # 学習 + 評価
  python -m src.ml.pace_model --evaluate-only    # 評価のみ

設計:
  - 目的変数: first_3f (レース前半3F秒) ※約35%のレースで取得可能
  - 特徴量: レース条件 + 脚質構成(逃げ馬数・先行馬率等)
  - 時系列分割
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple

import lightgbm as lgb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.log import get_logger

logger = get_logger(__name__)

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
ML_DATA_DIR = os.path.join(_PROJECT_ROOT, "data", "ml")
MODEL_PATH = os.path.join(ML_DATA_DIR, "pace_model.txt")
META_PATH = os.path.join(ML_DATA_DIR, "pace_meta.json")

WARMUP_DAYS = 90
VAL_MONTHS = 4
MIN_FIRST3F = 32.0
MAX_FIRST3F = 42.0

CATEGORICAL_FEATURES = ["venue_code", "surface_enc", "condition_enc", "grade_enc"]

FEATURE_COLUMNS = [
    "venue_code", "surface_enc", "distance", "condition_enc",
    "field_count", "grade_enc", "is_jra",
    "n_escape", "n_front", "front_ratio", "avg_hist_pos",
    # 案C追加特徴量
    "n_escape_inner",       # 内枠(1-4番)の逃げ馬候補数
    "avg_leader_first3f",   # 逃げ候補の過去前半3F平均
    "max_escape_strength",  # 最強逃げ馬の実効スコア(0〜1)
]

LGB_PARAMS = {
    "objective": "regression",
    "metric": "mae",
    "boosting_type": "gbdt",
    "num_leaves": 31,
    "learning_rate": 0.05,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "min_child_samples": 50,
    "reg_alpha": 0.2,
    "reg_lambda": 0.5,
    "verbose": -1,
    "n_jobs": -1,
    "seed": 42,
}


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


def load_and_build_race_features() -> pd.DataFrame:
    """馬レベルデータからレースレベル特徴量を構築"""
    from src.ml.last3f_model import build_feature_table, load_ml_data

    df = load_ml_data()
    df = build_feature_table(df)

    # レース単位で集約 (surface_enc等はbuild_feature_tableで既に設定済み) (horse_style_est は過去位置の指数移動平均)
    race_agg = df.groupby("race_id").agg({
        "race_date_dt": "first",
        "venue_code": "first",
        "surface_enc": "first",
        "distance": "first",
        "condition_enc": "first",
        "field_count": "first",
        "grade_enc": "first",
        "is_jra": "first",
        "race_first_3f": "first",
        "race_pace": "first",
    }).reset_index()

    # 脚質構成
    style_est = df.groupby("race_id")["horse_style_est"].agg(["mean", "count"])
    n_escape = df.groupby("race_id").apply(
        lambda g: (g["horse_style_est"].fillna(0.5) <= 0.15).sum()
    )
    n_front = df.groupby("race_id").apply(
        lambda g: (g["horse_style_est"].fillna(0.5) <= 0.35).sum()
    )

    race_agg["n_escape"] = race_agg["race_id"].map(n_escape).fillna(0)
    race_agg["n_front"] = race_agg["race_id"].map(n_front).fillna(0)
    race_agg["front_ratio"] = race_agg["n_front"] / race_agg["field_count"].clip(lower=1)
    race_agg["avg_hist_pos"] = race_agg["race_id"].map(style_est["mean"])

    # 案C: 内枠逃げ馬数（gate_no が利用可能な場合）
    if "gate_no" in df.columns:
        n_escape_inner = df.groupby("race_id").apply(
            lambda g: ((g["horse_style_est"].fillna(0.5) <= 0.15) & (g["gate_no"].fillna(9) <= 4)).sum()
        )
        race_agg["n_escape_inner"] = race_agg["race_id"].map(n_escape_inner).fillna(0)
    else:
        race_agg["n_escape_inner"] = 0.0

    # 案C: 逃げ候補の過去前半3F平均（逃げ候補馬のhist_l3f_mean3で代用）
    # ※ race_first_3f は当日の結果なので、過去走の3F平均を代用として使用
    if "hist_l3f_mean3" in df.columns:
        leader_l3f = df[df["horse_style_est"].fillna(0.5) <= 0.15].groupby("race_id")["hist_l3f_mean3"].mean()
        # 前半3Fは後半3Fより通常速い（芝で1〜2秒程度） - 簡易補正
        race_agg["avg_leader_first3f"] = race_agg["race_id"].map(leader_l3f).fillna(36.0) - 1.5
    else:
        race_agg["avg_leader_first3f"] = 36.0

    # 案C: 最強逃げ馬スコア（gate_noと過去位置から簡易計算）
    if "gate_no" in df.columns:
        def _max_escape_strength(g):
            leaders_g = g[g["horse_style_est"].fillna(0.5) <= 0.15]
            if leaders_g.empty:
                return 0.0
            field = len(g)
            strengths = []
            for _, row in leaders_g.iterrows():
                gno = row.get("gate_no") if pd.notna(row.get("gate_no")) else None
                gate_f = (1.0 - (gno - 1) / 7.0) if gno and 1 <= gno <= 8 else 0.5
                # horse_style_est が小さいほど逃げ傾向が強い
                style_f = max(0.0, (0.15 - row["horse_style_est"]) / 0.15) if pd.notna(row["horse_style_est"]) else 0.5
                strengths.append(gate_f * 0.4 + style_f * 0.6)
            return max(strengths) if strengths else 0.0
        max_esc = df.groupby("race_id").apply(_max_escape_strength)
        race_agg["max_escape_strength"] = race_agg["race_id"].map(max_esc).fillna(0.0)
    else:
        race_agg["max_escape_strength"] = 0.5

    race_agg.rename(columns={"race_first_3f": "first_3f"}, inplace=True)
    logger.info(f"レースレベル: {len(race_agg):,}行 (first_3f有: {race_agg['first_3f'].notna().sum():,})")
    return race_agg


def prepare_datasets(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    mask_target = df["first_3f"].notna() & df["first_3f"].between(MIN_FIRST3F, MAX_FIRST3F)
    min_date = df["race_date_dt"].min()
    warmup_end = min_date + timedelta(days=WARMUP_DAYS)
    mask_warmup = df["race_date_dt"] >= warmup_end

    valid = df[mask_target & mask_warmup].copy()
    logger.info(f"有効レース: {len(valid):,}")

    max_date = valid["race_date_dt"].max()
    cutoff = max_date - timedelta(days=30 * VAL_MONTHS)
    train = valid[valid["race_date_dt"] < cutoff]
    val = valid[valid["race_date_dt"] >= cutoff]
    return train, val


def train_model(train_df: pd.DataFrame, val_df: pd.DataFrame) -> lgb.Booster:
    X_train = train_df[FEATURE_COLUMNS].fillna(0).copy()
    y_train = train_df["first_3f"]
    X_val = val_df[FEATURE_COLUMNS].fillna(0).copy()
    y_val = val_df["first_3f"]

    for col in CATEGORICAL_FEATURES:
        if col in X_train.columns:
            X_train[col] = X_train[col].astype("category")
            X_val[col] = X_val[col].astype("category")

    dtrain = lgb.Dataset(X_train, label=y_train, categorical_feature=CATEGORICAL_FEATURES)
    dval = lgb.Dataset(X_val, label=y_val, reference=dtrain, categorical_feature=CATEGORICAL_FEATURES)

    model = lgb.train(
        LGB_PARAMS,
        dtrain,
        num_boost_round=1000,
        valid_sets=[dtrain, dval],
        valid_names=["train", "val"],
        callbacks=[lgb.log_evaluation(period=100), lgb.early_stopping(stopping_rounds=50)],
    )
    return model


def save_model(model: lgb.Booster, metrics: dict) -> None:
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    model.save_model(MODEL_PATH)
    meta = {
        "feature_columns": FEATURE_COLUMNS,
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


class PacePredictorML:
    """前半3Fを予測し、PaceTypeに変換。PacePredictorの補完用"""

    def __init__(self):
        self.model: Optional[lgb.Booster] = None
        self._loaded = False

    def load(self) -> bool:
        self.model = load_model()
        self._loaded = self.model is not None
        return self._loaded

    @property
    def is_available(self) -> bool:
        return self._loaded and self.model is not None

    def predict_first3f(self, race_info, pace_context: dict) -> Optional[float]:
        """レース前半3Fを予測。特徴量不足時はNone"""
        if not self.is_available:
            return None
        features = self._build_features(race_info, pace_context)
        if features is None:
            return None
        # モデルの特徴量数に合わせて切り詰め（旧モデル互換）
        n_model = self.model.num_feature() if hasattr(self.model, "num_feature") else len(FEATURE_COLUMNS)
        feat_cols = FEATURE_COLUMNS[:n_model]
        feat_df = pd.DataFrame([features])[feat_cols].fillna(0)
        for col in CATEGORICAL_FEATURES:
            if col in feat_df.columns:
                feat_df[col] = feat_df[col].astype("category")
        pred = self.model.predict(feat_df, num_iteration=self.model.best_iteration)[0]
        return float(np.clip(pred, MIN_FIRST3F, MAX_FIRST3F))

    def _build_features(self, race_info, pace_context: dict) -> Optional[dict]:
        course = race_info.course
        f = {
            "venue_code": course.venue_code,
            "surface_enc": 0 if course.surface == "芝" else 1,
            "distance": course.distance,
            "condition_enc": _encode_condition(
                race_info.track_condition_turf or race_info.track_condition_dirt or "良"
            ),
            "field_count": race_info.field_count,
            "grade_enc": _encode_grade(race_info.grade),
            "is_jra": race_info.is_jra,
        }
        pc = pace_context or {}
        f["n_escape"] = pc.get("n_escape", 0)
        f["n_front"] = pc.get("n_front", 0)
        f["front_ratio"] = pc.get("front_ratio", 0.3)
        # avg_hist_pos: pace_context から取得（なければデフォルト 0.4）
        f["avg_hist_pos"] = pc.get("avg_hist_pos", 0.4)
        # 案C追加特徴量
        f["n_escape_inner"]      = pc.get("n_escape_inner", 0)
        f["avg_leader_first3f"]  = pc.get("avg_leader_first3f", 36.0)
        f["max_escape_strength"] = pc.get("max_escape_strength", 0.5)
        return f


def run_training_pipeline() -> dict:
    t0 = time.time()
    logger.info("前半3F予測モデル 学習開始")
    df = load_and_build_race_features()
    train_df, val_df = prepare_datasets(df)
    if len(train_df) < 100:
        logger.warning("学習データ不足のためスキップ")
        return {}
    model = train_model(train_df, val_df)
    X_val = val_df[FEATURE_COLUMNS].fillna(0).copy()
    for col in CATEGORICAL_FEATURES:
        if col in X_val.columns:
            X_val[col] = X_val[col].astype("category")
    y_true = val_df["first_3f"].values
    y_pred = model.predict(X_val, num_iteration=model.best_iteration)
    mae = np.mean(np.abs(y_true - y_pred))
    metrics = {"mae": round(mae, 4), "val_size": len(val_df)}
    save_model(model, metrics)
    print(f"前半3F予測 MAE: {mae:.4f}秒  val={len(val_df):,}レース")
    logger.info(f"完了: {time.time()-t0:.1f}秒")
    return metrics


def main():
    if "--evaluate-only" in sys.argv:
        model = load_model()
        if model is None:
            print("モデルが見つかりません")
            return
        df = load_and_build_race_features()
        _, val_df = prepare_datasets(df)
        X_val = val_df[FEATURE_COLUMNS].fillna(0).copy()
        for col in CATEGORICAL_FEATURES:
            if col in X_val.columns:
                X_val[col] = X_val[col].astype("category")
        y_true = val_df["first_3f"].values
        y_pred = model.predict(X_val, num_iteration=model.best_iteration)
        print(f"MAE: {np.mean(np.abs(y_true - y_pred)):.4f}")
    else:
        run_training_pipeline()


if __name__ == "__main__":
    main()
