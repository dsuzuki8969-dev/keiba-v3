"""
初角（1コーナー）位置取り予測 LightGBM モデル

目的:
  逃げ馬判定の精度向上のため、各馬の初角相対位置を予測する。
  従来の「4角位置×0.85」逆算を置き換える。

学習データ:
  - JRA: positions_corners[0] = 真の初角通過順位
  - NAR: positions_corners[0] = 最終コーナー（小回りで初角プロキシ）
  - corner_count, straight_m 等のコース特性で位置変動を学習

使い方:
  python -m src.ml.first1c_model                    # 学習 + 評価
  python -m src.ml.first1c_model --evaluate-only    # 保存済みモデルで評価のみ
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
MODEL_PATH = os.path.join(ML_DATA_DIR, "first1c_model.txt")
META_PATH = os.path.join(ML_DATA_DIR, "first1c_meta.json")
JOCKEY_CACHE_PATH = os.path.join(ML_DATA_DIR, "first1c_jockey_cache.json")
TRAINER_CACHE_PATH = os.path.join(ML_DATA_DIR, "first1c_trainer_cache.json")

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
    # レース条件
    "venue_code", "surface_enc", "distance", "condition_enc",
    "field_count", "grade_enc", "is_jra",
    # 当日情報
    "gate_no", "horse_no", "weight_kg", "sex_enc", "age",
    "horse_weight", "weight_change", "relative_horse_no",
    # コース特性（1角専用）
    "n_corners", "corner_count", "first_corner_enc", "straight_m",
    "relative_gate_no",
    # 過去位置ローリング（最終コーナー）
    "hist_pos_mean3", "hist_pos_mean5", "hist_pos_latest", "hist_pos_trend",
    "hist_pos_std5", "hist_pos_best5", "hist_pos_worst5",
    # 過去成績
    "hist_finish_mean3", "hist_finish_mean5",
    "hist_l3f_mean5", "hist_run_count",
    "hist_pos_same_surface", "hist_pos_same_venue", "hist_pos_same_distband",
    "hist_win_rate5", "hist_place3_rate5",
    "hist_dist_change", "days_since_last",
    # 騎手
    "jockey_avg_pos", "jockey_course_avg_pos", "jockey_ride_count",
    # 騎手前行き率（Phase10追加）
    "jockey_nige_rate", "jockey_mae_iki_rate", "jockey_ds_mae_iki_rate",
    # 調教師位置取り（Phase10追加）
    "trainer_avg_pos", "trainer_mae_iki_rate", "trainer_ds_mae_iki_rate",
    # 4角版率特徴量（Phase11追加: 勝負所でどこにいるか）
    "jockey_4c_nige_rate", "jockey_4c_mae_iki_rate", "jockey_4c_ds_mae_iki_rate",
    "trainer_4c_nige_rate", "trainer_4c_mae_iki_rate", "trainer_4c_ds_mae_iki_rate",
    # 1角→4角軌跡特徴量（Phase11追加: レース中の位置変化）
    "jockey_pos_delta", "jockey_hold_rate", "jockey_ds_pos_delta",
    "trainer_pos_delta", "trainer_hold_rate", "trainer_ds_pos_delta",
    # ペース文脈
    "pace_n_front", "pace_front_ratio", "horse_style_est",
    # 1角専用ローリング
    "hist_1c_mean3", "hist_1c_mean5", "hist_1c_latest",
    "hist_1c_best5", "hist_escape_rate5", "hist_escape_rate_all",
    # フィールド強度特徴量（Phase9D追加）
    "grade_diff",               # 現レースグレード - 過去平均グレード（昇級=正）
    "hist_finish_rate_mean5",   # 過去5走の正規化着順平均（着順/出走頭数）
]


def _distance_band(d: int) -> str:
    for lo, hi, label in DISTANCE_BANDS:
        if lo <= d <= hi:
            return label
    return "mile"


def _first_corner_position(corners: list, field_count: int) -> float:
    """初角相対位置: positions_corners[0] / field_count"""
    if not corners or field_count <= 0:
        return np.nan
    return corners[0] / max(field_count, 1)


def _encode_condition(cond: str) -> int:
    return {"良": 0, "稍重": 1, "稍": 1, "重": 2, "不良": 3}.get(str(cond), 0)


def _encode_grade(grade: str) -> int:
    g = str(grade) if grade else ""
    for i, pat in enumerate(["新馬", "未勝利", "1勝", "2勝", "3勝",
                             "オープン", "リステッド", "G3", "G2", "G1"]):
        if pat in g:
            return i
    return 2


def _encode_sex(s) -> int:
    return {"牡": 0, "牝": 1, "セ": 2, "セン": 2}.get(str(s), 0)


def _encode_first_corner(fc: str) -> int:
    return {"短い": 0, "普通": 1, "長い": 2, "長いのみ": 2, "なし": 1}.get(str(fc), 1)


# ============================================================
# データ読込: last3f_model.load_ml_data() を再利用
# ============================================================
def load_ml_data():
    from src.ml.last3f_model import load_ml_data as _load
    return _load()


# ============================================================
# 特徴量構築
# ============================================================
def build_feature_table(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrameに1角専用の特徴量を追加"""
    t0 = time.time()

    df = df.copy()
    df["race_date_dt"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df.dropna(subset=["race_date_dt"]).sort_values(["race_date_dt", "race_id", "horse_no"])
    df = df.reset_index(drop=True)

    # course_id 生成
    df["course_id"] = df["venue_code"].astype(str).str.zfill(2) + "_" + df["surface"].astype(str) + "_" + df["distance"].astype(str)

    # 目的変数: 初角相対位置
    df["first1c_rel"] = df.apply(
        lambda r: _first_corner_position(r["positions_corners"], r["field_count"]), axis=1
    )

    # 最終コーナー相対位置（既存position_modelと同じ）
    df["rel_position"] = df.apply(
        lambda r: r["positions_corners"][-1] / max(r["field_count"], 1)
        if r["positions_corners"] and r["field_count"] > 0 else np.nan, axis=1
    )

    # エンコーディング
    df["surface_enc"] = df["surface"].map(lambda s: 0 if s == "芝" else 1)
    df["condition_enc"] = df["condition"].map(_encode_condition)
    df["grade_enc"] = df.get("grade", pd.Series(dtype=str)).fillna("").map(_encode_grade)
    df["sex_enc"] = df["sex"].map(_encode_sex)
    df["is_jra"] = df["is_jra"].astype(int)
    df["relative_horse_no"] = df["horse_no"] / df["field_count"].clip(lower=1)
    df["relative_gate_no"] = df["gate_no"] / df["field_count"].clip(lower=1)

    # コーナー数
    def _calc_n_corners(dist):
        if dist >= 1800:
            return 4
        elif dist >= 1600:
            return 3
        return 2

    df["n_corners"] = df["distance"].apply(_calc_n_corners)

    # CourseMasterからコース特性を取得
    logger.info("コース特性特徴量を構築中...")
    _build_course_features(df)

    # 過去走ローリング
    logger.info("過去走ローリング特徴量を構築中...")
    _build_horse_history_features(df)

    # 1角専用ローリング
    logger.info("1角専用ローリング特徴量を構築中...")
    _build_1c_history_features(df)

    # 条件別位置取り
    logger.info("条件別位置取り特徴量を構築中...")
    _build_conditional_pos_rolling(df, "hist_pos_same_surface", "surface_enc")
    _build_conditional_pos_rolling(df, "hist_pos_same_venue", "venue_code")
    df["_distband"] = df["distance"].apply(_distance_band)
    _build_conditional_pos_rolling(df, "hist_pos_same_distband", "_distband")
    df.drop(columns=["_distband"], inplace=True)

    # 勝率
    df["_win"] = (df["finish_pos"] == 1).astype(float)
    df["_place3"] = (df["finish_pos"] <= 3).astype(float)
    gw = df.groupby("horse_id")
    df["hist_win_rate5"] = gw["_win"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df["hist_place3_rate5"] = gw["_place3"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df.drop(columns=["_win", "_place3"], inplace=True)

    # 距離変更
    g = df.groupby("horse_id")
    dist = g["distance"]
    df["hist_dist_change"] = df["distance"] - dist.transform(lambda x: x.shift(1))

    # Phase9D: フィールド強度特徴量
    df["_finish_rate"] = df["finish_pos"] / df["field_count"].clip(lower=1)
    df["hist_finish_rate_mean5"] = g["_finish_rate"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean()
    )
    df["_grade_enc_hist"] = g["grade_enc"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean()
    )
    df["grade_diff"] = df["grade_enc"] - df["_grade_enc_hist"]
    df.drop(columns=["_finish_rate", "_grade_enc_hist"], inplace=True)

    # 騎手
    logger.info("騎手集約特徴量を構築中...")
    _build_jockey_features(df)

    # 調教師
    logger.info("調教師位置特徴量を構築中...")
    _build_trainer_features(df)

    # ペース文脈
    logger.info("ペース文脈特徴量を構築中...")
    _build_pace_context_features(df)

    # 出走間隔
    df["days_since_last"] = df.groupby("horse_id")["race_date_dt"].diff().dt.days

    df.sort_values(["race_date_dt", "race_id", "horse_no"], inplace=True)

    elapsed = time.time() - t0
    logger.info(f"特徴量構築完了: {len(df):,}行, {elapsed:.1f}秒")
    return df


def _build_course_features(df: pd.DataFrame) -> None:
    """CourseMasterからコース特性を取得"""
    try:
        from data.masters.course_master import get_all_courses
        courses = get_all_courses()
    except Exception:
        logger.warning("CourseMasterの読込に失敗。デフォルト値を使用")
        df["corner_count"] = df["n_corners"]
        df["first_corner_enc"] = 1
        df["straight_m"] = 300
        return

    # course_id → CourseMaster マッピング
    corner_count_arr = np.full(len(df), np.nan)
    first_corner_arr = np.full(len(df), 1.0)
    straight_arr = np.full(len(df), 300.0)

    for idx in df.index:
        vc = str(df.at[idx, "venue_code"]).zfill(2)
        surf = df.at[idx, "surface"]
        dist = df.at[idx, "distance"]
        cid = f"{vc}_{surf}_{dist}"
        cm = courses.get(cid)
        if cm:
            corner_count_arr[idx] = cm.corner_count
            first_corner_arr[idx] = _encode_first_corner(cm.first_corner)
            straight_arr[idx] = cm.straight_m
        else:
            corner_count_arr[idx] = df.at[idx, "n_corners"]

    df["corner_count"] = corner_count_arr
    df["first_corner_enc"] = first_corner_arr
    df["straight_m"] = straight_arr


def _build_horse_history_features(df: pd.DataFrame) -> None:
    """馬の過去走からローリング特徴量を計算"""
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


def _build_1c_history_features(df: pd.DataFrame) -> None:
    """1角専用ローリング特徴量を計算

    過去走の初角相対位置と逃げ成功率を蓄積。
    リーケージ防止のため日付ベースで遅延蓄積。
    """
    df.sort_values(["horse_id", "race_date_dt"], inplace=True)

    hist_1c_mean3 = np.full(len(df), np.nan)
    hist_1c_mean5 = np.full(len(df), np.nan)
    hist_1c_latest = np.full(len(df), np.nan)
    hist_1c_best5 = np.full(len(df), np.nan)
    escape_rate5 = np.full(len(df), np.nan)
    escape_rate_all = np.full(len(df), np.nan)

    # 馬ごとに1角位置履歴を蓄積
    horse_1c_buf: Dict[str, list] = {}  # horse_id → [first1c_rel, ...]
    horse_escape_buf: Dict[str, list] = {}  # horse_id → [is_leader, ...]

    prev_date = None
    date_batch = []

    for idx in df.index:
        current_date = df.at[idx, "race_date_dt"]

        # 日付が変わったら前日分を蓄積（リーケージ防止）
        if prev_date is not None and current_date != prev_date:
            for bi in date_batch:
                hid = df.at[bi, "horse_id"]
                fc_rel = df.at[bi, "first1c_rel"]
                if np.isnan(fc_rel):
                    continue
                horse_1c_buf.setdefault(hid, []).append(fc_rel)
                horse_escape_buf.setdefault(hid, []).append(1.0 if fc_rel <= 1.0 / max(df.at[bi, "field_count"], 1) else 0.0)
            date_batch = []

        hid = df.at[idx, "horse_id"]
        buf = horse_1c_buf.get(hid, [])
        esc = horse_escape_buf.get(hid, [])

        if buf:
            recent5 = buf[-5:]
            recent3 = buf[-3:]
            hist_1c_mean3[idx] = np.mean(recent3)
            hist_1c_mean5[idx] = np.mean(recent5)
            hist_1c_latest[idx] = buf[-1]
            hist_1c_best5[idx] = min(recent5)
        if esc:
            recent5_esc = esc[-5:]
            escape_rate5[idx] = np.mean(recent5_esc)
            escape_rate_all[idx] = np.mean(esc)

        date_batch.append(idx)
        prev_date = current_date

    df["hist_1c_mean3"] = hist_1c_mean3
    df["hist_1c_mean5"] = hist_1c_mean5
    df["hist_1c_latest"] = hist_1c_latest
    df["hist_1c_best5"] = hist_1c_best5
    df["hist_escape_rate5"] = escape_rate5
    df["hist_escape_rate_all"] = escape_rate_all

    df.sort_values(["race_date_dt", "race_id", "horse_no"], inplace=True)


def _build_conditional_pos_rolling(df: pd.DataFrame, out_col: str, cond_col: str) -> None:
    """条件別位置取りローリング"""
    results = np.full(len(df), np.nan)
    buf: Dict[Tuple, List[float]] = defaultdict(list)
    df_sorted = df.sort_values(["horse_id", "race_date_dt"])

    prev_hid = None
    prev_date = None
    pending = []

    for idx in df_sorted.index:
        hid = df_sorted.at[idx, "horse_id"]
        dt = df_sorted.at[idx, "race_date_dt"]

        if hid != prev_hid:
            for pi, pkey, pval in pending:
                if not np.isnan(pval):
                    buf[pkey].append(pval)
            pending = []
            prev_hid = hid
            prev_date = None

        if prev_date is not None and dt != prev_date:
            for pi, pkey, pval in pending:
                if not np.isnan(pval):
                    buf[pkey].append(pval)
            pending = []

        cond_val = df_sorted.at[idx, cond_col]
        key = (hid, cond_val)
        vals = buf.get(key, [])
        if vals:
            results[idx] = np.mean(vals[-5:])

        rp = df_sorted.at[idx, "rel_position"]
        pending.append((idx, key, rp))
        prev_date = dt

    df[out_col] = results


def _get_pos_1c(corners, field_count: int) -> Tuple[Optional[int], float]:
    """1角通過順位と相対位置を取得"""
    if not corners or not isinstance(corners, list) or field_count <= 0:
        return None, np.nan
    pos = corners[0]
    if isinstance(pos, (int, float)) and pos > 0:
        return int(pos), pos / max(field_count, 1)
    return None, np.nan


def _get_pos_4c(corners, field_count: int) -> Tuple[Optional[int], float]:
    """最終コーナー通過順位と相対位置を取得"""
    if not corners or not isinstance(corners, list) or field_count <= 0:
        return None, np.nan
    for pos in reversed(corners):
        if isinstance(pos, (int, float)) and pos > 0:
            return int(pos), pos / max(field_count, 1)
    return None, np.nan


def _get_pos_delta(corners, field_count: int) -> Tuple[float, bool]:
    """1角→最終角の相対位置変化。正=後退、負=前進。"""
    if not corners or not isinstance(corners, list) or len(corners) < 2 or field_count <= 0:
        return np.nan, False
    pos_1c = corners[0]
    pos_4c = None
    for p in reversed(corners):
        if isinstance(p, (int, float)) and p > 0:
            pos_4c = p
            break
    if not (isinstance(pos_1c, (int, float)) and pos_1c > 0) or pos_4c is None:
        return np.nan, False
    delta = (pos_4c / max(field_count, 1)) - (pos_1c / max(field_count, 1))
    return delta, True


def _build_jockey_features(df: pd.DataFrame) -> None:
    """騎手の位置取り傾向（1角率 + 4角率 + 1角→4角軌跡）"""
    df.sort_values(["race_date_dt", "race_id"], inplace=True)

    jockey_pos_sum: Dict[str, float] = {}
    jockey_pos_cnt: Dict[str, int] = {}
    jockey_course_sum: Dict[Tuple[str, str], float] = {}
    jockey_course_cnt: Dict[Tuple[str, str], int] = {}
    # Phase10: 1角
    j_nige_cnt: Dict[str, int] = defaultdict(int)
    j_mae_iki_cnt: Dict[str, int] = defaultdict(int)
    j_1c_valid: Dict[str, int] = defaultdict(int)
    j_ds_mae_iki_cnt: Dict[str, int] = defaultdict(int)
    j_ds_valid: Dict[str, int] = defaultdict(int)
    # Phase11: 4角
    j_4c_nige_cnt: Dict[str, int] = defaultdict(int)
    j_4c_mae_iki_cnt: Dict[str, int] = defaultdict(int)
    j_4c_valid: Dict[str, int] = defaultdict(int)
    j_4c_ds_mae_iki_cnt: Dict[str, int] = defaultdict(int)
    j_4c_ds_valid: Dict[str, int] = defaultdict(int)
    # Phase11: 軌跡
    j_delta_sum: Dict[str, float] = defaultdict(float)
    j_delta_cnt: Dict[str, int] = defaultdict(int)
    j_hold_cnt: Dict[str, int] = defaultdict(int)
    j_ds_delta_sum: Dict[str, float] = defaultdict(float)
    j_ds_delta_cnt: Dict[str, int] = defaultdict(int)

    avg_arr = np.full(len(df), np.nan)
    course_avg_arr = np.full(len(df), np.nan)
    ride_cnt_arr = np.zeros(len(df))
    j_nige_rate_arr = np.full(len(df), np.nan)
    j_mae_iki_rate_arr = np.full(len(df), np.nan)
    j_ds_mae_iki_rate_arr = np.full(len(df), np.nan)
    j_4c_nige_rate_arr = np.full(len(df), np.nan)
    j_4c_mae_iki_rate_arr = np.full(len(df), np.nan)
    j_4c_ds_mae_iki_rate_arr = np.full(len(df), np.nan)
    j_pos_delta_arr = np.full(len(df), np.nan)
    j_hold_rate_arr = np.full(len(df), np.nan)
    j_ds_pos_delta_arr = np.full(len(df), np.nan)

    prev_date = None
    date_batch = []

    def _flush_batch(batch):
        for bi in batch:
            jid = df.at[bi, "jockey_id"]
            rp = df.at[bi, "rel_position"]
            cid = df.at[bi, "course_id"]
            if not np.isnan(rp) and 0 <= rp <= 1:
                jockey_pos_sum[jid] = jockey_pos_sum.get(jid, 0) + rp
                jockey_pos_cnt[jid] = jockey_pos_cnt.get(jid, 0) + 1
                jockey_course_sum[(jid, cid)] = jockey_course_sum.get((jid, cid), 0) + rp
                jockey_course_cnt[(jid, cid)] = jockey_course_cnt.get((jid, cid), 0) + 1
            corners = df.at[bi, "positions_corners"]
            fc = df.at[bi, "field_count"]
            is_ds = not np.isnan(rp) and rp > 0.5
            threshold = 3.0 / max(fc, 1)
            # 1角
            pos_1c, pos_1c_rel = _get_pos_1c(corners, fc)
            if pos_1c is not None:
                j_1c_valid[jid] += 1
                if pos_1c == 1:
                    j_nige_cnt[jid] += 1
                if pos_1c_rel <= threshold:
                    j_mae_iki_cnt[jid] += 1
                if is_ds:
                    j_ds_valid[jid] += 1
                    if pos_1c_rel <= threshold:
                        j_ds_mae_iki_cnt[jid] += 1
            # 4角（Phase11）
            pos_4c, pos_4c_rel = _get_pos_4c(corners, fc)
            if pos_4c is not None:
                j_4c_valid[jid] += 1
                if pos_4c == 1:
                    j_4c_nige_cnt[jid] += 1
                if pos_4c_rel <= threshold:
                    j_4c_mae_iki_cnt[jid] += 1
                if is_ds:
                    j_4c_ds_valid[jid] += 1
                    if pos_4c_rel <= threshold:
                        j_4c_ds_mae_iki_cnt[jid] += 1
            # 軌跡（Phase11）
            delta, delta_valid = _get_pos_delta(corners, fc)
            if delta_valid:
                j_delta_sum[jid] += delta
                j_delta_cnt[jid] += 1
                if delta <= 0:
                    j_hold_cnt[jid] += 1
                if is_ds:
                    j_ds_delta_sum[jid] += delta
                    j_ds_delta_cnt[jid] += 1

    for idx in df.index:
        current_date = df.at[idx, "race_date_dt"]
        if prev_date is not None and current_date != prev_date:
            _flush_batch(date_batch)
            date_batch = []

        jid = df.at[idx, "jockey_id"]
        cid = df.at[idx, "course_id"]

        cnt = jockey_pos_cnt.get(jid, 0)
        if cnt > 0:
            avg_arr[idx] = jockey_pos_sum[jid] / cnt
        ride_cnt_arr[idx] = cnt
        cc = jockey_course_cnt.get((jid, cid), 0)
        if cc > 0:
            course_avg_arr[idx] = jockey_course_sum[(jid, cid)] / cc
        # 1角率
        if j_1c_valid[jid] >= 10:
            j_nige_rate_arr[idx] = j_nige_cnt[jid] / j_1c_valid[jid]
            j_mae_iki_rate_arr[idx] = j_mae_iki_cnt[jid] / j_1c_valid[jid]
        if j_ds_valid[jid] >= 5:
            j_ds_mae_iki_rate_arr[idx] = j_ds_mae_iki_cnt[jid] / j_ds_valid[jid]
        # 4角率（Phase11）
        if j_4c_valid[jid] >= 10:
            j_4c_nige_rate_arr[idx] = j_4c_nige_cnt[jid] / j_4c_valid[jid]
            j_4c_mae_iki_rate_arr[idx] = j_4c_mae_iki_cnt[jid] / j_4c_valid[jid]
        if j_4c_ds_valid[jid] >= 5:
            j_4c_ds_mae_iki_rate_arr[idx] = j_4c_ds_mae_iki_cnt[jid] / j_4c_ds_valid[jid]
        # 軌跡（Phase11）
        if j_delta_cnt[jid] >= 10:
            j_pos_delta_arr[idx] = j_delta_sum[jid] / j_delta_cnt[jid]
            j_hold_rate_arr[idx] = j_hold_cnt[jid] / j_delta_cnt[jid]
        if j_ds_delta_cnt[jid] >= 5:
            j_ds_pos_delta_arr[idx] = j_ds_delta_sum[jid] / j_ds_delta_cnt[jid]

        date_batch.append(idx)
        prev_date = current_date

    if date_batch:
        _flush_batch(date_batch)

    df["jockey_avg_pos"] = avg_arr
    df["jockey_course_avg_pos"] = course_avg_arr
    df["jockey_ride_count"] = ride_cnt_arr
    df["jockey_nige_rate"] = j_nige_rate_arr
    df["jockey_mae_iki_rate"] = j_mae_iki_rate_arr
    df["jockey_ds_mae_iki_rate"] = j_ds_mae_iki_rate_arr
    df["jockey_4c_nige_rate"] = j_4c_nige_rate_arr
    df["jockey_4c_mae_iki_rate"] = j_4c_mae_iki_rate_arr
    df["jockey_4c_ds_mae_iki_rate"] = j_4c_ds_mae_iki_rate_arr
    df["jockey_pos_delta"] = j_pos_delta_arr
    df["jockey_hold_rate"] = j_hold_rate_arr
    df["jockey_ds_pos_delta"] = j_ds_pos_delta_arr


def _build_trainer_features(df: pd.DataFrame) -> None:
    """調教師位置取り特徴量（1角率 + 4角率 + 軌跡）"""
    df.sort_values(["race_date_dt", "race_id"], inplace=True)
    t_pos_sum: Dict[str, float] = defaultdict(float)
    t_pos_cnt: Dict[str, int] = defaultdict(int)
    t_mae_iki_cnt: Dict[str, int] = defaultdict(int)
    t_1c_valid: Dict[str, int] = defaultdict(int)
    t_ds_mae_iki_cnt: Dict[str, int] = defaultdict(int)
    t_ds_valid: Dict[str, int] = defaultdict(int)
    # Phase11
    t_4c_nige_cnt: Dict[str, int] = defaultdict(int)
    t_4c_mae_iki_cnt: Dict[str, int] = defaultdict(int)
    t_4c_valid: Dict[str, int] = defaultdict(int)
    t_4c_ds_mae_iki_cnt: Dict[str, int] = defaultdict(int)
    t_4c_ds_valid: Dict[str, int] = defaultdict(int)
    t_delta_sum: Dict[str, float] = defaultdict(float)
    t_delta_cnt: Dict[str, int] = defaultdict(int)
    t_hold_cnt: Dict[str, int] = defaultdict(int)
    t_ds_delta_sum: Dict[str, float] = defaultdict(float)
    t_ds_delta_cnt: Dict[str, int] = defaultdict(int)

    t_avg = np.full(len(df), np.nan)
    t_mae_iki_rate_arr = np.full(len(df), np.nan)
    t_ds_mae_iki_rate_arr = np.full(len(df), np.nan)
    t_4c_nige_rate_arr = np.full(len(df), np.nan)
    t_4c_mae_iki_rate_arr = np.full(len(df), np.nan)
    t_4c_ds_mae_iki_rate_arr = np.full(len(df), np.nan)
    t_pos_delta_arr = np.full(len(df), np.nan)
    t_hold_rate_arr = np.full(len(df), np.nan)
    t_ds_pos_delta_arr = np.full(len(df), np.nan)
    prev_date = None
    date_batch = []

    def _flush_batch(batch):
        for i in batch:
            tid = df.at[i, "trainer_id"]
            if not tid:
                continue
            rp = df.at[i, "rel_position"]
            if not np.isnan(rp) and 0 <= rp <= 1:
                t_pos_sum[tid] += rp
                t_pos_cnt[tid] += 1
            corners = df.at[i, "positions_corners"]
            fc = df.at[i, "field_count"]
            is_ds = not np.isnan(rp) and rp > 0.5
            threshold = 3.0 / max(fc, 1)
            # 1角
            pos_1c, pos_1c_rel = _get_pos_1c(corners, fc)
            if pos_1c is not None:
                t_1c_valid[tid] += 1
                if pos_1c_rel <= threshold:
                    t_mae_iki_cnt[tid] += 1
                if is_ds:
                    t_ds_valid[tid] += 1
                    if pos_1c_rel <= threshold:
                        t_ds_mae_iki_cnt[tid] += 1
            # 4角
            pos_4c, pos_4c_rel = _get_pos_4c(corners, fc)
            if pos_4c is not None:
                t_4c_valid[tid] += 1
                if pos_4c == 1:
                    t_4c_nige_cnt[tid] += 1
                if pos_4c_rel <= threshold:
                    t_4c_mae_iki_cnt[tid] += 1
                if is_ds:
                    t_4c_ds_valid[tid] += 1
                    if pos_4c_rel <= threshold:
                        t_4c_ds_mae_iki_cnt[tid] += 1
            # 軌跡
            delta, delta_valid = _get_pos_delta(corners, fc)
            if delta_valid:
                t_delta_sum[tid] += delta
                t_delta_cnt[tid] += 1
                if delta <= 0:
                    t_hold_cnt[tid] += 1
                if is_ds:
                    t_ds_delta_sum[tid] += delta
                    t_ds_delta_cnt[tid] += 1

    for idx in df.index:
        current_date = df.at[idx, "race_date_dt"]
        if prev_date is not None and current_date != prev_date:
            _flush_batch(date_batch)
            date_batch = []

        tid = df.at[idx, "trainer_id"]
        if tid and t_pos_cnt[tid] > 0:
            t_avg[idx] = t_pos_sum[tid] / t_pos_cnt[tid]
        if tid and t_1c_valid[tid] >= 20:
            t_mae_iki_rate_arr[idx] = t_mae_iki_cnt[tid] / t_1c_valid[tid]
        if tid and t_ds_valid[tid] >= 10:
            t_ds_mae_iki_rate_arr[idx] = t_ds_mae_iki_cnt[tid] / t_ds_valid[tid]
        if tid and t_4c_valid[tid] >= 20:
            t_4c_nige_rate_arr[idx] = t_4c_nige_cnt[tid] / t_4c_valid[tid]
            t_4c_mae_iki_rate_arr[idx] = t_4c_mae_iki_cnt[tid] / t_4c_valid[tid]
        if tid and t_4c_ds_valid[tid] >= 10:
            t_4c_ds_mae_iki_rate_arr[idx] = t_4c_ds_mae_iki_cnt[tid] / t_4c_ds_valid[tid]
        if tid and t_delta_cnt[tid] >= 20:
            t_pos_delta_arr[idx] = t_delta_sum[tid] / t_delta_cnt[tid]
            t_hold_rate_arr[idx] = t_hold_cnt[tid] / t_delta_cnt[tid]
        if tid and t_ds_delta_cnt[tid] >= 10:
            t_ds_pos_delta_arr[idx] = t_ds_delta_sum[tid] / t_ds_delta_cnt[tid]
        date_batch.append(idx)
        prev_date = current_date

    if date_batch:
        _flush_batch(date_batch)

    df["trainer_avg_pos"] = t_avg
    df["trainer_mae_iki_rate"] = t_mae_iki_rate_arr
    df["trainer_ds_mae_iki_rate"] = t_ds_mae_iki_rate_arr
    df["trainer_4c_nige_rate"] = t_4c_nige_rate_arr
    df["trainer_4c_mae_iki_rate"] = t_4c_mae_iki_rate_arr
    df["trainer_4c_ds_mae_iki_rate"] = t_4c_ds_mae_iki_rate_arr
    df["trainer_pos_delta"] = t_pos_delta_arr
    df["trainer_hold_rate"] = t_hold_rate_arr
    df["trainer_ds_pos_delta"] = t_ds_pos_delta_arr
    df.sort_values(["race_date_dt", "race_id", "horse_no"], inplace=True)


def _build_pace_context_features(df: pd.DataFrame) -> None:
    """ペース文脈特徴量"""
    df.sort_values(["race_date_dt", "race_id"], inplace=True)

    horse_style: Dict[str, float] = {}
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


# ============================================================
# 学習・評価
# ============================================================
def prepare_datasets(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """ウォームアップ除外 + 時系列 train/val 分割"""
    df = df.copy()
    mask_target = df["first1c_rel"].notna() & (df["first1c_rel"] >= 0) & (df["first1c_rel"] <= 1)
    min_date = df["race_date_dt"].min()
    warmup_end = min_date + timedelta(days=WARMUP_DAYS)
    mask_warmup = df["race_date_dt"] >= warmup_end
    mask_hist = df["hist_run_count"] >= 1
    mask_field = df["field_count"] >= 4

    valid = df[mask_target & mask_warmup & mask_hist & mask_field].copy()
    logger.info(f"有効データ: {len(valid):,}行 (除外: 目的変数{(~mask_target).sum():,}, "
                f"ウォームアップ{(~mask_warmup).sum():,}, 初走{(~mask_hist).sum():,})")

    max_date = valid["race_date_dt"].max()
    cutoff = max_date - timedelta(days=30 * VAL_MONTHS)
    train = valid[valid["race_date_dt"] < cutoff]
    val = valid[valid["race_date_dt"] >= cutoff]

    logger.info(f"Train: {len(train):,}行 (~{cutoff.strftime('%Y-%m-%d')})")
    logger.info(f"Val:   {len(val):,}行 ({cutoff.strftime('%Y-%m-%d')}~)")
    return train, val


def train_model(train_df: pd.DataFrame, val_df: pd.DataFrame) -> lgb.Booster:
    X_train = train_df[FEATURE_COLUMNS].copy()
    y_train = train_df["first1c_rel"]
    X_val = val_df[FEATURE_COLUMNS].copy()
    y_val = val_df["first1c_rel"]

    for col in CATEGORICAL_FEATURES:
        if col in X_train.columns:
            X_train[col] = X_train[col].astype("category")
            X_val[col] = X_val[col].astype("category")

    dtrain = lgb.Dataset(X_train, label=y_train, categorical_feature=CATEGORICAL_FEATURES)
    dval = lgb.Dataset(X_val, label=y_val, reference=dtrain, categorical_feature=CATEGORICAL_FEATURES)

    callbacks = [
        lgb.log_evaluation(period=200),
        lgb.early_stopping(stopping_rounds=100),
    ]

    model = lgb.train(
        LGB_PARAMS, dtrain,
        num_boost_round=3000,
        valid_sets=[dtrain, dval],
        valid_names=["train", "val"],
        callbacks=callbacks,
    )

    logger.info(f"学習完了: {model.best_iteration} rounds, best MAE={model.best_score['val']['l1']:.4f}")
    return model


def evaluate_model(model: lgb.Booster, val_df: pd.DataFrame) -> dict:
    X_val = val_df[FEATURE_COLUMNS].copy()
    for col in CATEGORICAL_FEATURES:
        if col in X_val.columns:
            X_val[col] = X_val[col].astype("category")

    y_true = val_df["first1c_rel"].values
    y_pred = model.predict(X_val, num_iteration=model.best_iteration)
    y_pred = np.clip(y_pred, 0.0, 1.0)

    # ベースライン: 過去走平均位置
    y_baseline = val_df["hist_1c_mean3"].values.copy()
    bl_mask = np.isnan(y_baseline)
    y_baseline[bl_mask] = val_df.loc[bl_mask, "hist_pos_mean3"].values
    still_nan = np.isnan(y_baseline)
    y_baseline[still_nan] = 0.5

    mae_model = np.mean(np.abs(y_true - y_pred))
    mae_baseline = np.mean(np.abs(y_true - y_baseline))
    improvement = (mae_baseline - mae_model) / mae_baseline * 100

    within_01 = np.mean(np.abs(y_true - y_pred) <= 0.1) * 100
    within_02 = np.mean(np.abs(y_true - y_pred) <= 0.2) * 100

    # JRA/NAR別
    jra_mask = val_df["is_jra"].values.astype(bool)
    metrics_jra = _calc_subset(y_true, y_pred, y_baseline, jra_mask)
    metrics_nar = _calc_subset(y_true, y_pred, y_baseline, ~jra_mask)

    return {
        "val_size": len(val_df),
        "mae_model": round(float(mae_model), 4),
        "mae_baseline": round(float(mae_baseline), 4),
        "improvement_mae_pct": round(float(improvement), 2),
        "within_0.1": round(float(within_01), 2),
        "within_0.2": round(float(within_02), 2),
        "best_iteration": model.best_iteration,
        "by_org": {"JRA": metrics_jra, "NAR": metrics_nar},
    }


def _calc_subset(y_true, y_pred, y_baseline, mask) -> dict:
    if mask.sum() == 0:
        return {"n": 0}
    yt, yp, yb = y_true[mask], y_pred[mask], y_baseline[mask]
    return {
        "n": int(mask.sum()),
        "mae_model": round(float(np.mean(np.abs(yt - yp))), 4),
        "mae_baseline": round(float(np.mean(np.abs(yt - yb))), 4),
        "within_0.2": round(float(np.mean(np.abs(yt - yp) <= 0.2) * 100), 2),
    }


def print_report(metrics: dict, fi=None) -> None:
    print("\n" + "=" * 60)
    print("  初角位置取り予測モデル  学習レポート")
    print("=" * 60)
    print(f"\n検証データ: {metrics['val_size']:,}行")
    print(f"\n{'指標':<20} {'モデル':>10} {'ベースライン':>12} {'改善':>8}")
    print("-" * 55)
    print(f"{'MAE':<20} {metrics['mae_model']:>10.4f} {metrics['mae_baseline']:>12.4f} "
          f"{metrics.get('improvement_mae_pct', 0):>+7.2f}%")
    print(f"{'±0.1以内 (%)':<20} {metrics['within_0.1']:>10.2f}")
    print(f"{'±0.2以内 (%)':<20} {metrics['within_0.2']:>10.2f}")

    for label, key in [("JRA", "JRA"), ("NAR", "NAR")]:
        m = metrics["by_org"].get(key, {})
        if m.get("n", 0) > 0:
            print(f"\n  {label:>8} MAE={m['mae_model']:.4f} (BL={m['mae_baseline']:.4f}) "
                  f"±0.2={m['within_0.2']:.1f}%  n={m['n']:,}")

    if fi is not None:
        print("\n  特徴量重要度 Top 15:")
        for i, row in fi.head(15).iterrows():
            bar = "#" * int(row["pct"])
            print(f"    {i+1:>2}. {row['feature']:<28} {row['pct']:>6.2f}%  {bar}")

    print(f"\n  学習ラウンド: {metrics['best_iteration']}")
    print("=" * 60)


def get_feature_importance(model: lgb.Booster) -> pd.DataFrame:
    imp = model.feature_importance(importance_type="gain")
    names = model.feature_name()
    fi = pd.DataFrame({"feature": names, "importance": imp})
    fi = fi.sort_values("importance", ascending=False).reset_index(drop=True)
    fi["pct"] = (fi["importance"] / fi["importance"].sum() * 100).round(2)
    return fi


# ============================================================
# 保存・読込
# ============================================================
def save_model(model: lgb.Booster, metrics: dict) -> None:
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    model.save_model(MODEL_PATH)
    meta = {
        "feature_columns": FEATURE_COLUMNS,
        "categorical_features": CATEGORICAL_FEATURES,
        "lgb_params": LGB_PARAMS,
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


# ============================================================
# 予測インターフェース
# ============================================================
class First1CPredictor:
    """初角位置取り予測クラス"""

    def __init__(self):
        self.model = None
        self._loaded = False
        self._jockey_cache = {}
        self._jockey_course_cache = {}
        self._jockey_ride_cache = {}
        # Phase10: 1角前行き率
        self._jockey_nige_rate = {}
        self._jockey_mae_iki_rate = {}
        self._jockey_ds_mae_iki_rate = {}
        # Phase11: 4角版率 + 軌跡
        self._jockey_4c_nige_rate = {}
        self._jockey_4c_mae_iki_rate = {}
        self._jockey_4c_ds_mae_iki_rate = {}
        self._jockey_pos_delta = {}
        self._jockey_hold_rate = {}
        self._jockey_ds_pos_delta = {}
        # 調教師
        self._trainer_cache = {}
        self._trainer_mae_iki_rate = {}
        self._trainer_ds_mae_iki_rate = {}
        self._trainer_4c_nige_rate = {}
        self._trainer_4c_mae_iki_rate = {}
        self._trainer_4c_ds_mae_iki_rate = {}
        self._trainer_pos_delta = {}
        self._trainer_hold_rate = {}
        self._trainer_ds_pos_delta = {}

    def ensure_loaded(self) -> bool:
        if self._loaded:
            return self.model is not None
        self.model = load_model()
        self._loaded = True
        if self.model:
            # 特徴量数整合チェック
            n_model = self.model.num_feature()
            n_code = len(FEATURE_COLUMNS)
            if n_model != n_code:
                logger.warning(
                    "初角モデル特徴量不整合: モデル=%d, コード=%d → 再学習必要",
                    n_model, n_code,
                )
                self.model = None
                return False
            self._load_jockey_cache()
            self._load_trainer_cache()
        return self.model is not None

    @property
    def is_available(self) -> bool:
        return self._loaded and self.model is not None

    def _load_jockey_cache(self) -> None:
        cache_path = os.path.join(ML_DATA_DIR, "position_jockey_cache.json")
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._jockey_cache = data.get("avg", {})
            self._jockey_course_cache = {
                tuple(k.split("|")): v for k, v in data.get("course_avg", {}).items()
            }
            self._jockey_ride_cache = data.get("rides", {})
            self._jockey_nige_rate = data.get("nige_rate", {})
            self._jockey_mae_iki_rate = data.get("mae_iki_rate", {})
            self._jockey_ds_mae_iki_rate = data.get("ds_mae_iki_rate", {})
            # Phase11
            self._jockey_4c_nige_rate = data.get("4c_nige_rate", {})
            self._jockey_4c_mae_iki_rate = data.get("4c_mae_iki_rate", {})
            self._jockey_4c_ds_mae_iki_rate = data.get("4c_ds_mae_iki_rate", {})
            self._jockey_pos_delta = data.get("pos_delta", {})
            self._jockey_hold_rate = data.get("hold_rate", {})
            self._jockey_ds_pos_delta = data.get("ds_pos_delta", {})

    def _load_trainer_cache(self) -> None:
        cache_path = os.path.join(ML_DATA_DIR, "position_trainer_cache.json")
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._trainer_cache = data.get("avg", {})
            self._trainer_mae_iki_rate = data.get("mae_iki_rate", {})
            self._trainer_ds_mae_iki_rate = data.get("ds_mae_iki_rate", {})
            self._trainer_4c_nige_rate = data.get("4c_nige_rate", {})
            self._trainer_4c_mae_iki_rate = data.get("4c_mae_iki_rate", {})
            self._trainer_4c_ds_mae_iki_rate = data.get("4c_ds_mae_iki_rate", {})
            self._trainer_pos_delta = data.get("pos_delta", {})
            self._trainer_hold_rate = data.get("hold_rate", {})
            self._trainer_ds_pos_delta = data.get("ds_pos_delta", {})

    def predict(self, horse, race_info, pace_context: dict = None) -> Optional[float]:
        """初角相対位置(0-1)を予測"""
        if not self.is_available:
            return None
        features = self._build_features(horse, race_info, pace_context)
        if features is None:
            return None

        feat_df = pd.DataFrame([features])[FEATURE_COLUMNS]
        for col in CATEGORICAL_FEATURES:
            if col in feat_df.columns:
                feat_df[col] = feat_df[col].astype("category")
        for col in feat_df.columns:
            if col not in CATEGORICAL_FEATURES:
                feat_df[col] = pd.to_numeric(feat_df[col], errors="coerce")

        pred = self.model.predict(feat_df, num_iteration=self.model.best_iteration)[0]
        return float(np.clip(pred, 0.0, 1.0))

    def _build_features(self, horse, race_info, pace_context) -> Optional[dict]:
        course = race_info.course
        runs = horse.past_runs or []
        if not runs:
            return None

        # 最終コーナー位置データ
        pos_runs = [r for r in runs if r.positions_corners]
        pos_vals = [r.relative_position for r in pos_runs if r.relative_position is not None]
        if not pos_vals:
            pos_runs = [r for r in runs if r.relative_position is not None]
            pos_vals = [r.relative_position for r in pos_runs]

        # 1角位置データ
        # JRA: positions_corners[0] = 初角、NAR: positions_corners[0] = 最終角
        # positions_cornersが空の場合はposition_4c/relative_positionでフォールバック
        fc_vals = []
        fc_escapes = []
        for r in runs:
            if r.positions_corners:
                fc_rel = r.positions_corners[0] / max(r.field_count, 1)
                fc_vals.append(fc_rel)
                fc_escapes.append(1.0 if r.positions_corners[0] == 1 else 0.0)
            elif r.position_4c and r.field_count:
                # positions_cornersが空（DBフォールバック時）→ position_4cを代用
                fc_rel = r.position_4c / max(r.field_count, 1)
                fc_vals.append(fc_rel)
                fc_escapes.append(1.0 if r.position_4c == 1 else 0.0)

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
        f["relative_gate_no"] = horse.gate_no / max(race_info.field_count, 1)

        # コーナー数
        n_corners = 2
        if course.distance >= 1800:
            n_corners = 4
        elif course.distance >= 1600:
            n_corners = 3
        f["n_corners"] = n_corners

        # コース特性
        f["corner_count"] = getattr(course, "corner_count", n_corners)
        f["first_corner_enc"] = _encode_first_corner(getattr(course, "first_corner", "普通"))
        f["straight_m"] = getattr(course, "straight_m", 300)

        # 過去位置ローリング
        f["hist_pos_mean3"] = np.mean(pos_vals[:3]) if pos_vals else np.nan
        f["hist_pos_mean5"] = np.mean(pos_vals[:5]) if pos_vals else np.nan
        f["hist_pos_latest"] = pos_vals[0] if pos_vals else np.nan
        f["hist_pos_trend"] = (f["hist_pos_latest"] - f["hist_pos_mean5"]) if pos_vals and len(pos_vals) >= 5 else np.nan
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
        target_band = _distance_band(course.distance)
        same_distband = [r.relative_position for r in pos_runs if _distance_band(r.distance) == target_band]
        f["hist_pos_same_distband"] = np.mean(same_distband[:5]) if same_distband else np.nan

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

        # 騎手
        jid = horse.jockey_id or ""
        cid_key = f"{course.venue_code}_{f['surface_enc']}_{course.distance}"
        f["jockey_avg_pos"] = self._jockey_cache.get(jid, np.nan)
        f["jockey_course_avg_pos"] = self._jockey_course_cache.get((jid, cid_key), np.nan)
        f["jockey_ride_count"] = self._jockey_ride_cache.get(jid, 0)
        # Phase10: 1角前行き率
        f["jockey_nige_rate"] = self._jockey_nige_rate.get(jid, np.nan)
        f["jockey_mae_iki_rate"] = self._jockey_mae_iki_rate.get(jid, np.nan)
        f["jockey_ds_mae_iki_rate"] = self._jockey_ds_mae_iki_rate.get(jid, np.nan)
        # 調教師
        tid = horse.trainer_id or ""
        f["trainer_avg_pos"] = self._trainer_cache.get(tid, np.nan)
        f["trainer_mae_iki_rate"] = self._trainer_mae_iki_rate.get(tid, np.nan)
        f["trainer_ds_mae_iki_rate"] = self._trainer_ds_mae_iki_rate.get(tid, np.nan)
        # Phase11: 4角版率
        f["jockey_4c_nige_rate"] = self._jockey_4c_nige_rate.get(jid, np.nan)
        f["jockey_4c_mae_iki_rate"] = self._jockey_4c_mae_iki_rate.get(jid, np.nan)
        f["jockey_4c_ds_mae_iki_rate"] = self._jockey_4c_ds_mae_iki_rate.get(jid, np.nan)
        f["trainer_4c_nige_rate"] = self._trainer_4c_nige_rate.get(tid, np.nan)
        f["trainer_4c_mae_iki_rate"] = self._trainer_4c_mae_iki_rate.get(tid, np.nan)
        f["trainer_4c_ds_mae_iki_rate"] = self._trainer_4c_ds_mae_iki_rate.get(tid, np.nan)
        # Phase11: 軌跡
        f["jockey_pos_delta"] = self._jockey_pos_delta.get(jid, np.nan)
        f["jockey_hold_rate"] = self._jockey_hold_rate.get(jid, np.nan)
        f["jockey_ds_pos_delta"] = self._jockey_ds_pos_delta.get(jid, np.nan)
        f["trainer_pos_delta"] = self._trainer_pos_delta.get(tid, np.nan)
        f["trainer_hold_rate"] = self._trainer_hold_rate.get(tid, np.nan)
        f["trainer_ds_pos_delta"] = self._trainer_ds_pos_delta.get(tid, np.nan)

        pc = pace_context or {}
        f["pace_n_front"] = pc.get("n_front", np.nan)
        f["pace_front_ratio"] = pc.get("front_ratio", np.nan)
        f["horse_style_est"] = f["hist_pos_mean5"]

        # 1角専用
        f["hist_1c_mean3"] = np.mean(fc_vals[:3]) if fc_vals else np.nan
        f["hist_1c_mean5"] = np.mean(fc_vals[:5]) if fc_vals else np.nan
        f["hist_1c_latest"] = fc_vals[0] if fc_vals else np.nan
        f["hist_1c_best5"] = min(fc_vals[:5]) if fc_vals else np.nan
        f["hist_escape_rate5"] = np.mean(fc_escapes[:5]) if fc_escapes else np.nan
        f["hist_escape_rate_all"] = np.mean(fc_escapes) if fc_escapes else np.nan

        # Phase9D: フィールド強度特徴量
        finish_rates = [r.finish_pos / max(r.field_count, 1) for r in runs if r.finish_pos and r.field_count]
        f["hist_finish_rate_mean5"] = np.mean(finish_rates[:5]) if finish_rates else np.nan
        past_grades = [_encode_grade(r.grade) for r in runs if hasattr(r, "grade") and r.grade]
        f["grade_diff"] = (
            f["grade_enc"] - np.mean(past_grades[:5])
            if past_grades else np.nan
        )

        return f


# ============================================================
# メイン
# ============================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--evaluate-only", action="store_true")
    parser.add_argument("--importance", action="store_true")
    args = parser.parse_args()

    if args.evaluate_only:
        model = load_model()
        if not model:
            print("モデルが見つかりません")
            sys.exit(1)
        df = load_ml_data()
        df = build_feature_table(df)
        _, val_df = prepare_datasets(df)
        metrics = evaluate_model(model, val_df)
        fi = get_feature_importance(model) if args.importance else None
        print_report(metrics, fi)
    else:
        df = load_ml_data()
        df = build_feature_table(df)
        train_df, val_df = prepare_datasets(df)
        model = train_model(train_df, val_df)
        metrics = evaluate_model(model, val_df)
        fi = get_feature_importance(model)
        print_report(metrics, fi)
        save_model(model, metrics)
