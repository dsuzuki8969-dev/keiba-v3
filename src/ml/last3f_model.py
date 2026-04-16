"""
上がり3F予測 LightGBM モデル  (Phase A)

目的:
  展開偏差値の精度向上のため、各馬の上がり3Fタイムを予測する。
  既存の線形ルールベース (Last3FEvaluator.estimate_last3f) を置き換え可能。

使い方:
  python -m src.ml.last3f_model                    # 学習 + 評価
  python -m src.ml.last3f_model --evaluate-only     # 保存済みモデルで評価のみ
  python -m src.ml.last3f_model --importance         # 特徴量重要度を表示

設計:
  - 目的変数: 実績 last_3f_sec
  - 特徴量: レース条件 + 馬の当日情報 + 過去走ローリング集計 + 騎手集約 + ペース文脈
  - 時系列分割 (train: ~cutoff / val: cutoff~)
  - ウォームアップ期間: データ開始から90日間は特徴量構築のみ (学習対象外)
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

# ============================================================
# パス定数
# ============================================================
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
ML_DATA_DIR = os.path.join(_PROJECT_ROOT, "data", "ml")
MODEL_PATH = os.path.join(ML_DATA_DIR, "last3f_model.txt")
META_PATH = os.path.join(ML_DATA_DIR, "last3f_meta.json")

# ============================================================
# 設定
# ============================================================
WARMUP_DAYS = 90
VAL_MONTHS = 4
MIN_LAST3F = 28.0
MAX_LAST3F = 48.0
DISTANCE_BANDS = [(0, 1399, "sprint"), (1400, 1799, "mile"),
                   (1800, 2199, "middle"), (2200, 9999, "long")]

CATEGORICAL_FEATURES = [
    "venue_code", "surface_enc", "condition_enc", "grade_enc", "sex_enc",
    "corner_type_enc", "slope_type_enc",
]

# LightGBM ハイパーパラメータ
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


# ============================================================
# 1. データ読込
# ============================================================

def load_ml_data() -> pd.DataFrame:
    """data/ml/*.json を読み込み、馬×レース単位のDataFrameを返す"""
    files = sorted(
        f for f in os.listdir(ML_DATA_DIR)
        if f.endswith(".json") and not f.startswith("_")
    )
    if not files:
        raise FileNotFoundError(f"MLデータが見つかりません: {ML_DATA_DIR}")

    rows = []
    for fname in files:
        path = os.path.join(ML_DATA_DIR, fname)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for race in data.get("races", []):
            race_base = {
                "race_id": race.get("race_id", ""),
                "race_date": race.get("date", ""),
                "venue_code": race.get("venue_code", ""),
                "surface": race.get("surface", ""),
                "distance": race.get("distance", 0),
                "condition": race.get("condition", "良"),
                "weather": race.get("weather", ""),
                "field_count": race.get("field_count", 0),
                "grade": race.get("grade", ""),
                "is_jra": race.get("is_jra", False),
                "race_first_3f": race.get("first_3f"),
                "race_pace": race.get("pace", ""),
            }
            for h in race.get("horses", []):
                if h.get("finish_pos") is None:
                    continue
                row = {**race_base}
                row["horse_id"] = h.get("horse_id", "")
                row["finish_pos"] = h.get("finish_pos")
                row["gate_no"] = h.get("gate_no")
                row["horse_no"] = h.get("horse_no")
                row["weight_kg"] = h.get("weight_kg")
                row["sex"] = h.get("sex", "")
                row["age"] = h.get("age")
                row["jockey_id"] = h.get("jockey_id", "")
                row["trainer_id"] = h.get("trainer_id", "")
                row["finish_time_sec"] = h.get("finish_time_sec")
                row["last_3f_sec"] = h.get("last_3f_sec")
                row["positions_corners"] = h.get("positions_corners", [])
                row["odds"] = h.get("odds")
                row["popularity"] = h.get("popularity")
                row["horse_weight"] = h.get("horse_weight")
                row["weight_change"] = h.get("weight_change")
                rows.append(row)

    df = pd.DataFrame(rows)
    logger.info(f"読込完了: {len(df):,}行 / {df['race_id'].nunique():,}レース / {len(files)}ファイル")
    return df


# ============================================================
# 2. 特徴量エンジニアリング
# ============================================================

def _distance_band(d: int) -> str:
    for lo, hi, label in DISTANCE_BANDS:
        if lo <= d <= hi:
            return label
    return "mile"


def _relative_position(corners: list, field_count: int) -> float:
    """通過順位から相対位置を計算 (0.0=先頭, 1.0=最後方)"""
    if not corners or field_count <= 0:
        return np.nan
    last_corner = corners[-1]
    return last_corner / max(field_count, 1)


def _encode_surface(s: str) -> int:
    return 0 if "芝" in str(s) else 1


def _encode_condition(c: str) -> int:
    m = {"良": 0, "稍重": 1, "稍": 1, "重": 2, "不良": 3, "不": 3}
    return m.get(str(c), 0)


def _encode_grade(g: str) -> int:
    m = {"新馬": 0, "未勝利": 1, "1勝": 2, "500万": 2,
         "2勝": 3, "1000万": 3, "3勝": 4, "1600万": 4,
         "OP": 5, "L": 5, "G3": 6, "G2": 7, "G1": 8,
         "交流重賞": 6, "その他": 2}
    return m.get(str(g), 2)


def _encode_sex(s: str) -> int:
    return {"牡": 0, "牝": 1, "セ": 2, "セン": 2}.get(str(s), 0)


def _encode_corner_type(ct: str) -> int:
    """コーナー形態エンコード"""
    return {"大回り": 0, "小回り": 1, "スパイラル": 2}.get(str(ct), 1)


def _encode_slope_type(st: str) -> int:
    """坂エンコード"""
    return {"坂なし": 0, "軽坂": 1, "急坂": 2}.get(str(st), 0)


# コースマスタ辞書 (venue_code + surface_enc + distance → CourseMaster)
_COURSE_MASTER_MAP: dict = {}


def _get_course_master_map() -> dict:
    """コースマスタのルックアップ辞書を遅延構築"""
    if _COURSE_MASTER_MAP:
        return _COURSE_MASTER_MAP
    from data.masters.course_master import ALL_COURSES
    for c in ALL_COURSES:
        surf = 0 if c.surface == "芝" else 1
        key = f"{c.venue_code}_{surf}_{c.distance}"
        _COURSE_MASTER_MAP[key] = c
    return _COURSE_MASTER_MAP


def build_feature_table(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrameに過去走ローリング特徴量・騎手集約・ペース文脈を追加"""
    t0 = time.time()

    df = df.copy()
    df["race_date_dt"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df.dropna(subset=["race_date_dt"]).sort_values(["race_date_dt", "race_id", "horse_no"])
    df = df.reset_index(drop=True)

    # --- 基本エンコード ---
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

    # --- 過去走ローリング特徴量 ---
    logger.info("過去走ローリング特徴量を構築中...")
    _build_horse_history_features(df)

    # --- 騎手集約特徴量 ---
    logger.info("騎手集約特徴量を構築中...")
    _build_jockey_features(df)

    # --- ペース文脈特徴量 (レース内集約) ---
    logger.info("ペース文脈特徴量を構築中...")
    _build_pace_context_features(df)

    # --- 出走間隔 ---
    df["days_since_last"] = df.groupby("horse_id")["race_date_dt"].diff().dt.days

    # --- 脚質別上がり3F特徴量（Phase4追加）---
    logger.info("脚質別上がり3F特徴量を構築中...")
    _build_style_l3f_features(df)

    # --- フィールド強度特徴量（Phase8追加）---
    logger.info("フィールド強度特徴量を構築中...")
    _build_field_strength_features(df)

    # --- コース形状特徴量（Phase5追加）---
    logger.info("コース形状特徴量を構築中...")
    _build_course_geometry_features(df)

    elapsed = time.time() - t0
    logger.info(f"特徴量構築完了: {len(df):,}行, {elapsed:.1f}秒")
    return df


def _build_horse_history_features(df: pd.DataFrame) -> None:
    """馬の過去走からローリング特徴量を計算し、dfに直接カラム追加"""
    df.sort_values(["horse_id", "race_date_dt"], inplace=True)

    g = df.groupby("horse_id")

    # shift(1) で当該レースを除外（リーケージ防止）
    l3f = g["last_3f_sec"]
    df["hist_l3f_mean3"] = l3f.transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean())
    df["hist_l3f_mean5"] = l3f.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df["hist_l3f_best5"] = l3f.transform(lambda x: x.shift(1).rolling(5, min_periods=1).min())
    df["hist_l3f_worst5"] = l3f.transform(lambda x: x.shift(1).rolling(5, min_periods=1).max())
    df["hist_l3f_std5"] = l3f.transform(lambda x: x.shift(1).rolling(5, min_periods=2).std())
    df["hist_l3f_latest"] = l3f.transform(lambda x: x.shift(1))
    df["hist_l3f_trend"] = df["hist_l3f_latest"] - df["hist_l3f_mean5"]

    pos = g["rel_position"]
    df["hist_pos_mean3"] = pos.transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean())
    df["hist_pos_mean5"] = pos.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df["hist_pos_latest"] = pos.transform(lambda x: x.shift(1))

    fp = g["finish_pos"]
    df["hist_finish_mean3"] = fp.transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean())
    df["hist_finish_mean5"] = fp.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())

    hw = g["horse_weight"]
    df["hist_hw_mean"] = hw.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())

    df["hist_run_count"] = g.cumcount()

    # 条件別の上がり3F平均（同馬場・同距離帯・同競馬場・同コース）
    for col_name, group_col in [("hist_l3f_same_surface", "surface_enc"),
                                 ("hist_l3f_same_distband", "dist_band"),
                                 ("hist_l3f_same_venue", "venue_code"),
                                 ("hist_l3f_same_course", "course_id")]:
        _build_conditional_rolling(df, col_name, group_col)

    # 勝率・複勝率
    df["_win"] = (df["finish_pos"] == 1).astype(float)
    df["_place3"] = (df["finish_pos"] <= 3).astype(float)
    gw = df.groupby("horse_id")
    df["hist_win_rate5"] = gw["_win"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df["hist_place3_rate5"] = gw["_place3"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df.drop(columns=["_win", "_place3"], inplace=True)

    # 距離変更
    dist = g["distance"]
    df["hist_dist_change"] = df["distance"] - dist.transform(lambda x: x.shift(1))

    df.sort_values(["race_date_dt", "race_id", "horse_no"], inplace=True)


def _build_conditional_rolling(df: pd.DataFrame, out_col: str, cond_col: str) -> None:
    """条件付きローリング平均（同馬場 or 同距離帯の上がり3F平均）"""
    results = np.full(len(df), np.nan)
    buf: Dict[Tuple[str, object], List[float]] = defaultdict(list)

    df_sorted = df.sort_values(["horse_id", "race_date_dt"])
    prev_hid = None
    for idx in df_sorted.index:
        hid = df_sorted.at[idx, "horse_id"]
        cond_val = df_sorted.at[idx, cond_col]
        l3f = df_sorted.at[idx, "last_3f_sec"]

        if hid != prev_hid:
            buf.clear()
            prev_hid = hid

        key = (hid, cond_val)
        hist = buf[key]
        if hist:
            results[idx] = np.mean(hist[-5:])

        if l3f and not np.isnan(l3f) and MIN_LAST3F <= l3f <= MAX_LAST3F:
            buf[key].append(l3f)

    df[out_col] = results


def _build_jockey_features(df: pd.DataFrame) -> None:
    """騎手の累積統計をrunning aggregateで構築"""
    df.sort_values(["race_date_dt", "race_id"], inplace=True)

    jockey_l3f_sum: Dict[str, float] = defaultdict(float)
    jockey_l3f_cnt: Dict[str, int] = defaultdict(int)
    jockey_course_l3f_sum: Dict[Tuple[str, str], float] = defaultdict(float)
    jockey_course_l3f_cnt: Dict[Tuple[str, str], int] = defaultdict(int)
    jockey_rides: Dict[str, int] = defaultdict(int)

    j_avg = np.full(len(df), np.nan)
    j_course_avg = np.full(len(df), np.nan)
    j_ride_count = np.full(len(df), np.nan)

    prev_date = None
    date_batch: List[int] = []

    for idx in df.index:
        current_date = df.at[idx, "race_date_dt"]

        if prev_date is not None and current_date != prev_date:
            _flush_jockey_batch(df, date_batch,
                                jockey_l3f_sum, jockey_l3f_cnt,
                                jockey_course_l3f_sum, jockey_course_l3f_cnt,
                                jockey_rides)
            date_batch = []

        jid = df.at[idx, "jockey_id"]
        cid = df.at[idx, "course_id"]

        if jockey_l3f_cnt[jid] > 0:
            j_avg[idx] = jockey_l3f_sum[jid] / jockey_l3f_cnt[jid]
        if jockey_course_l3f_cnt[(jid, cid)] > 0:
            j_course_avg[idx] = jockey_course_l3f_sum[(jid, cid)] / jockey_course_l3f_cnt[(jid, cid)]
        j_ride_count[idx] = jockey_rides[jid]

        date_batch.append(idx)
        prev_date = current_date

    if date_batch:
        _flush_jockey_batch(df, date_batch,
                            jockey_l3f_sum, jockey_l3f_cnt,
                            jockey_course_l3f_sum, jockey_course_l3f_cnt,
                            jockey_rides)

    df["jockey_avg_l3f"] = j_avg
    df["jockey_course_avg_l3f"] = j_course_avg
    df["jockey_ride_count"] = j_ride_count

    df.sort_values(["race_date_dt", "race_id", "horse_no"], inplace=True)


def _flush_jockey_batch(df, indices, l3f_sum, l3f_cnt, cl3f_sum, cl3f_cnt, rides):
    """日次バッチの結果を集計に反映"""
    for idx in indices:
        jid = df.at[idx, "jockey_id"]
        cid = df.at[idx, "course_id"]
        l3f = df.at[idx, "last_3f_sec"]
        rides[jid] += 1
        if l3f and not np.isnan(l3f) and MIN_LAST3F <= l3f <= MAX_LAST3F:
            l3f_sum[jid] += l3f
            l3f_cnt[jid] += 1
            cl3f_sum[(jid, cid)] += l3f
            cl3f_cnt[(jid, cid)] += 1


def _build_pace_context_features(df: pd.DataFrame) -> None:
    """レース内の脚質構成からペース文脈特徴量を計算"""
    horse_style: Dict[str, float] = {}

    df.sort_values(["race_date_dt", "race_id"], inplace=True)

    n_front = np.full(len(df), np.nan)
    front_ratio = np.full(len(df), np.nan)
    self_style = np.full(len(df), np.nan)

    for race_id, group in df.groupby("race_id"):
        fc = group["field_count"].iloc[0]
        fc = max(fc, 1)
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


def _build_style_l3f_features(df: pd.DataFrame) -> None:
    """脚質別の上がり3F平均特徴量を計算

    先行型(相対位置<=0.3)と後方型(>=0.5)のレースでの上がり3F平均を分離。
    同じ馬でも脚質により上がり3Fパフォーマンスが異なることを捉える。
    """
    df.sort_values(["horse_id", "race_date_dt"], inplace=True)

    front_mean = np.full(len(df), np.nan)
    rear_mean = np.full(len(df), np.nan)

    # 馬ごとに先行型/後方型の上がり3F履歴を蓄積
    horse_front_buf: Dict[str, list] = {}  # horse_id → [l3f_sec, ...]
    horse_rear_buf: Dict[str, list] = {}

    prev_date = None
    date_batch = []

    for idx in df.index:
        current_date = df.at[idx, "race_date_dt"]

        # 日付が変わったら前日分の結果を蓄積（リーケージ防止）
        if prev_date is not None and current_date != prev_date:
            for bi in date_batch:
                hid = df.at[bi, "horse_id"]
                rp = df.at[bi, "rel_position"]
                l3f = df.at[bi, "last_3f_sec"]
                if np.isnan(rp) or np.isnan(l3f) or l3f < 28 or l3f > 48:
                    continue
                if rp <= 0.3:
                    horse_front_buf.setdefault(hid, []).append(l3f)
                elif rp >= 0.5:
                    horse_rear_buf.setdefault(hid, []).append(l3f)
            date_batch = []

        hid = df.at[idx, "horse_id"]
        fb = horse_front_buf.get(hid, [])
        rb = horse_rear_buf.get(hid, [])
        if fb:
            front_mean[idx] = np.mean(fb[-5:])
        if rb:
            rear_mean[idx] = np.mean(rb[-5:])

        date_batch.append(idx)
        prev_date = current_date

    df["hist_l3f_front_mean"] = front_mean
    df["hist_l3f_rear_mean"] = rear_mean

    df.sort_values(["race_date_dt", "race_id", "horse_no"], inplace=True)


def _build_field_strength_features(df: pd.DataFrame) -> None:
    """フィールド強度特徴量を構築（Phase8）

    各レース内の他馬の過去走上がり3F平均から、フィールドの強さを推定。
    自馬のhist_l3f_mean3が「フィールド平均と比べてどうか」を特徴量化。
    リーケージ防止: hist_l3f_mean3は既にshift(1)済み（過去走のみ）。
    """
    # hist_l3f_mean3が既に構築済みであることが前提
    field_l3f_mean = np.full(len(df), np.nan)
    field_finish_mean = np.full(len(df), np.nan)
    horse_l3f_vs_field = np.full(len(df), np.nan)

    for _, group in df.groupby("race_id"):
        indices = group.index.tolist()
        # 各馬の hist_l3f_mean3 を取得（過去走ベースなのでリーケージなし）
        l3f_vals = group["hist_l3f_mean3"].values
        fin_vals = group["hist_finish_mean3"].values

        for i, idx in enumerate(indices):
            # 自馬を除いた他馬の平均
            other_l3f = [v for j, v in enumerate(l3f_vals) if j != i and not np.isnan(v)]
            other_fin = [v for j, v in enumerate(fin_vals) if j != i and not np.isnan(v)]

            if other_l3f:
                fld_avg = np.mean(other_l3f)
                field_l3f_mean[idx] = fld_avg
                self_l3f = l3f_vals[i]
                if not np.isnan(self_l3f):
                    # 負=自分が速い(強い), 正=自分が遅い(弱い)
                    horse_l3f_vs_field[idx] = self_l3f - fld_avg
            if other_fin:
                field_finish_mean[idx] = np.mean(other_fin)

    df["field_hist_l3f_mean"] = field_l3f_mean
    df["field_hist_finish_mean"] = field_finish_mean
    df["horse_l3f_vs_field"] = horse_l3f_vs_field


def _build_course_geometry_features(df: pd.DataFrame) -> None:
    """コース形状特徴量をcourse_masterから付与

    上がり3F=ラスト600mだが、98%以上のコースでは直線距離<600mのため
    4角前からの計測になる。直線距離・コーナー形態・坂の有無が上がりに直結。
    残り600m地点データ（l3f_corners, l3f_elevation, l3f_hill_start等）も付与。
    """
    cmap = _get_course_master_map()

    n = len(df)
    straight_m_arr = np.full(n, np.nan)
    corner_count_arr = np.full(n, np.nan)
    corner_type_enc_arr = np.full(n, 1.0)  # デフォルト=小回り
    slope_type_enc_arr = np.full(n, 0.0)   # デフォルト=坂なし
    first_corner_m_arr = np.full(n, np.nan)
    # 上がり3Fのうちコーナー区間の距離(m)
    l3f_corner_m_arr = np.full(n, np.nan)
    # 直線距離 / レース距離の比率
    straight_ratio_arr = np.full(n, np.nan)
    # 残り600m地点データ（新規）
    l3f_corners_arr = np.full(n, np.nan)       # コーナー数
    l3f_elevation_arr = np.full(n, 0.0)        # 高低差(m)
    l3f_hill_start_arr = np.full(n, 0.0)       # 坂開始地点(m)
    l3f_straight_pct_arr = np.full(n, np.nan)  # 直線比率

    for i, idx in enumerate(df.index):
        key = df.at[idx, "course_id"]
        cm = cmap.get(key)
        if cm is None:
            continue
        straight_m_arr[i] = cm.straight_m
        corner_count_arr[i] = cm.corner_count
        corner_type_enc_arr[i] = _encode_corner_type(cm.corner_type)
        slope_type_enc_arr[i] = _encode_slope_type(cm.slope_type)
        first_corner_m_arr[i] = cm.first_corner_m
        l3f_corner_m_arr[i] = cm.l3f_corner_m
        straight_ratio_arr[i] = cm.straight_m / max(cm.distance, 1)
        # 残り600m地点データ
        l3f_corners_arr[i] = cm.l3f_corners
        l3f_elevation_arr[i] = cm.l3f_elevation
        l3f_hill_start_arr[i] = cm.l3f_hill_start
        l3f_straight_pct_arr[i] = cm.l3f_straight_pct

    df["straight_m"] = straight_m_arr
    df["corner_count"] = corner_count_arr
    df["corner_type_enc"] = corner_type_enc_arr
    df["slope_type_enc"] = slope_type_enc_arr
    df["first_corner_m"] = first_corner_m_arr
    df["l3f_corner_m"] = l3f_corner_m_arr
    df["straight_ratio"] = straight_ratio_arr
    # 残り600m地点データ（新規）
    df["l3f_corners"] = l3f_corners_arr
    df["l3f_elevation"] = l3f_elevation_arr
    df["l3f_hill_start"] = l3f_hill_start_arr
    df["l3f_straight_pct"] = l3f_straight_pct_arr


# ============================================================
# 3. 特徴量カラム定義
# ============================================================

FEATURE_COLUMNS = [
    # レース条件
    "venue_code", "surface_enc", "distance", "condition_enc",
    "field_count", "grade_enc", "is_jra",
    # 当日情報
    "gate_no", "horse_no", "weight_kg", "sex_enc", "age",
    "horse_weight", "weight_change", "relative_horse_no",
    # 過去走ローリング
    "hist_l3f_mean3", "hist_l3f_mean5", "hist_l3f_best5", "hist_l3f_worst5",
    "hist_l3f_std5", "hist_l3f_latest", "hist_l3f_trend",
    "hist_pos_mean3", "hist_pos_mean5", "hist_pos_latest",
    "hist_finish_mean3", "hist_finish_mean5",
    "hist_hw_mean", "hist_run_count",
    "hist_l3f_same_surface", "hist_l3f_same_distband",
    "hist_l3f_same_venue", "hist_l3f_same_course",
    "hist_win_rate5", "hist_place3_rate5",
    "hist_dist_change", "days_since_last",
    # 騎手
    "jockey_avg_l3f", "jockey_course_avg_l3f", "jockey_ride_count",
    # ペース文脈
    "pace_n_front", "pace_front_ratio", "horse_style_est",
    # 脚質別上がり3F（Phase4追加）
    "hist_l3f_front_mean", "hist_l3f_rear_mean",
    # コース形状（Phase5追加）
    "straight_m", "corner_count", "corner_type_enc", "slope_type_enc",
    "first_corner_m", "l3f_corner_m", "straight_ratio",
    # フィールド強度（Phase8追加）
    "field_hist_l3f_mean", "field_hist_finish_mean", "horse_l3f_vs_field",
]

# 残り600m地点データ（Phase6）— モデル再学習時に有効化
# 学習時のbuild_feature_tableでは構築されるが、推論時のFEATURE_COLUMNSには
# モデル再学習後に追加する（既存モデルとのカラム数不一致を防止）
FEATURE_COLUMNS_NEXT = FEATURE_COLUMNS + [
    "l3f_corners", "l3f_elevation", "l3f_hill_start", "l3f_straight_pct",
]


# ============================================================
# 4. 学習・評価
# ============================================================

def prepare_datasets(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """ウォームアップ除外 + 時系列 train/val 分割"""
    df = df.copy()

    # 有効な目的変数のみ
    mask_target = df["last_3f_sec"].between(MIN_LAST3F, MAX_LAST3F)
    # ウォームアップ除外: データ開始から WARMUP_DAYS 日間
    min_date = df["race_date_dt"].min()
    warmup_end = min_date + timedelta(days=WARMUP_DAYS)
    mask_warmup = df["race_date_dt"] >= warmup_end
    # 最低限の過去走あり
    mask_hist = df["hist_run_count"] >= 1

    valid = df[mask_target & mask_warmup & mask_hist].copy()
    logger.info(f"有効データ: {len(valid):,}行 (除外: 目的変数{(~mask_target).sum():,}, "
                f"ウォームアップ{(~mask_warmup).sum():,}, 初走{(~mask_hist).sum():,})")

    # 時系列分割
    max_date = valid["race_date_dt"].max()
    cutoff = max_date - timedelta(days=30 * VAL_MONTHS)
    train = valid[valid["race_date_dt"] < cutoff]
    val = valid[valid["race_date_dt"] >= cutoff]

    logger.info(f"Train: {len(train):,}行 (~{cutoff.strftime('%Y-%m-%d')})")
    logger.info(f"Val:   {len(val):,}行 ({cutoff.strftime('%Y-%m-%d')}~)")
    return train, val


def train_model(train_df: pd.DataFrame, val_df: pd.DataFrame) -> lgb.Booster:
    """LightGBM モデルを学習"""
    X_train = train_df[FEATURE_COLUMNS].copy()
    y_train = train_df["last_3f_sec"]
    X_val = val_df[FEATURE_COLUMNS].copy()
    y_val = val_df["last_3f_sec"]

    # カテゴリカル特徴量の型変換
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
        LGB_PARAMS,
        dtrain,
        num_boost_round=3000,
        valid_sets=[dtrain, dval],
        valid_names=["train", "val"],
        callbacks=callbacks,
    )

    logger.info(f"学習完了: {model.best_iteration} rounds, best MAE={model.best_score['val']['l1']:.4f}")
    return model


def evaluate_model(
    model: lgb.Booster, val_df: pd.DataFrame, train_df: pd.DataFrame = None
) -> dict:
    """モデルの予測精度を定量評価し、ルールベースとの比較も行う"""
    X_val = val_df[FEATURE_COLUMNS].copy()
    for col in CATEGORICAL_FEATURES:
        if col in X_val.columns:
            X_val[col] = X_val[col].astype("category")

    y_true = val_df["last_3f_sec"].values
    y_pred = model.predict(X_val, num_iteration=model.best_iteration)
    y_pred = np.clip(y_pred, MIN_LAST3F, MAX_LAST3F)

    # ルールベース推定のフォールバック (過去走平均をベースラインとする)
    y_baseline = val_df["hist_l3f_mean3"].values.copy()
    baseline_mask = np.isnan(y_baseline)
    y_baseline[baseline_mask] = val_df.loc[baseline_mask, "hist_l3f_mean5"].values
    still_nan = np.isnan(y_baseline)
    y_baseline[still_nan] = np.nanmean(y_true)

    mae_model = np.mean(np.abs(y_true - y_pred))
    mae_baseline = np.mean(np.abs(y_true - y_baseline))
    rmse_model = np.sqrt(np.mean((y_true - y_pred) ** 2))
    rmse_baseline = np.sqrt(np.mean((y_true - y_baseline) ** 2))
    improvement_mae = (mae_baseline - mae_model) / mae_baseline * 100

    # 1秒以内正解率
    within_1s_model = np.mean(np.abs(y_true - y_pred) <= 1.0) * 100
    within_1s_baseline = np.mean(np.abs(y_true - y_baseline) <= 1.0) * 100

    # 0.5秒以内正解率
    within_05s_model = np.mean(np.abs(y_true - y_pred) <= 0.5) * 100
    within_05s_baseline = np.mean(np.abs(y_true - y_baseline) <= 0.5) * 100

    # JRA/NAR 別
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

    results = {
        "val_size": len(y_true),
        "mae_model": round(mae_model, 4),
        "mae_baseline": round(mae_baseline, 4),
        "improvement_mae_pct": round(improvement_mae, 2),
        "rmse_model": round(rmse_model, 4),
        "rmse_baseline": round(rmse_baseline, 4),
        "within_1s_model": round(within_1s_model, 2),
        "within_1s_baseline": round(within_1s_baseline, 2),
        "within_05s_model": round(within_05s_model, 2),
        "within_05s_baseline": round(within_05s_baseline, 2),
        "best_iteration": model.best_iteration,
        "by_org": {"JRA": metrics_jra, "NAR": metrics_nar},
        "by_surface": {"芝": metrics_turf, "ダート": metrics_dirt},
        "by_distance": dist_metrics,
    }
    return results


def _calc_subset_metrics(y_true, y_pred, y_baseline, mask) -> dict:
    if mask.sum() == 0:
        return {"n": 0}
    yt, yp, yb = y_true[mask], y_pred[mask], y_baseline[mask]
    return {
        "n": int(mask.sum()),
        "mae_model": round(float(np.mean(np.abs(yt - yp))), 4),
        "mae_baseline": round(float(np.mean(np.abs(yt - yb))), 4),
        "within_1s": round(float(np.mean(np.abs(yt - yp) <= 1.0) * 100), 2),
    }


def get_feature_importance(model: lgb.Booster) -> pd.DataFrame:
    """特徴量重要度を DataFrame で返す"""
    imp = model.feature_importance(importance_type="gain")
    names = model.feature_name()
    fi = pd.DataFrame({"feature": names, "importance": imp})
    fi = fi.sort_values("importance", ascending=False).reset_index(drop=True)
    fi["pct"] = (fi["importance"] / fi["importance"].sum() * 100).round(2)
    return fi


# ============================================================
# 5. モデル保存・読込
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
    logger.info(f"メタ保存:   {META_PATH}")


def load_model() -> Optional[lgb.Booster]:
    if not os.path.exists(MODEL_PATH):
        return None
    model = lgb.Booster(model_file=MODEL_PATH)
    logger.info(f"モデル読込: {MODEL_PATH}")
    return model


def load_meta() -> Optional[dict]:
    if not os.path.exists(META_PATH):
        return None
    with open(META_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# 6. 予測インターフェース (既存システム統合用)
# ============================================================

class Last3FPredictor:
    """
    既存システムから呼び出す予測クラス。
    Horse / RaceInfo オブジェクトから特徴量を構築し、予測を返す。
    モデルが未学習の場合は None を返し、呼び出し側でフォールバック。
    """

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
            # 特徴量数整合チェック
            n_model = self.model.num_feature()
            n_code = len(FEATURE_COLUMNS)
            if n_model != n_code:
                logger.warning(
                    "上がり3Fモデル特徴量不整合: モデル=%d, コード=%d → 再学習必要 (retrain_all.py --l3f)",
                    n_model, n_code,
                )
                self.model = None
                return False
            self._loaded = True
            self._load_jockey_cache()
            return True
        return False

    @property
    def is_available(self) -> bool:
        return self._loaded and self.model is not None

    def _load_jockey_cache(self) -> None:
        """学習データから騎手統計キャッシュを構築"""
        cache_path = os.path.join(ML_DATA_DIR, "last3f_jockey_cache.json")
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._jockey_cache = data.get("avg", {})
            self._jockey_course_cache = {
                tuple(k.split("|")): v for k, v in data.get("course_avg", {}).items()
            }
            self._jockey_ride_cache = data.get("rides", {})

    def predict(self, horse, race_info, pace_context: dict = None) -> Optional[float]:
        """
        予測上がり3Fを返す。

        Args:
            horse: src.models.Horse
            race_info: src.models.RaceInfo
            pace_context: {"n_front": int, "front_ratio": float} (optional)

        Returns:
            float or None (モデル未ロード or 特徴量不足)
        """
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
        return float(np.clip(pred, MIN_LAST3F, MAX_LAST3F))

    def _build_features(self, horse, race_info, pace_context) -> Optional[dict]:
        """Horse + RaceInfo から特徴量辞書を構築"""

        course = race_info.course
        runs = horse.past_runs or []
        if not runs:
            return None

        # 上がり3Fの有効な過去走
        l3f_runs = [r for r in runs if r.last_3f_sec and MIN_LAST3F <= r.last_3f_sec <= MAX_LAST3F]

        # 位置取りの有効な過去走
        pos_runs = [r for r in runs if r.relative_position is not None]

        f = {}

        # レース条件
        f["venue_code"] = course.venue_code
        f["surface_enc"] = 0 if course.surface == "芝" else 1
        f["distance"] = course.distance
        f["condition_enc"] = _encode_condition(
            race_info.track_condition_turf or race_info.track_condition_dirt or "良"
        )
        f["field_count"] = race_info.field_count
        f["grade_enc"] = _encode_grade(race_info.grade)
        f["is_jra"] = race_info.is_jra

        # 当日情報
        f["gate_no"] = horse.gate_no
        f["horse_no"] = horse.horse_no
        f["weight_kg"] = horse.weight_kg
        f["sex_enc"] = _encode_sex(horse.sex)
        f["age"] = horse.age
        f["horse_weight"] = horse.horse_weight
        f["weight_change"] = horse.weight_change
        f["relative_horse_no"] = horse.horse_no / max(race_info.field_count, 1)

        # 過去走ローリング
        l3f_vals = [r.last_3f_sec for r in l3f_runs]
        f["hist_l3f_mean3"] = np.mean(l3f_vals[:3]) if l3f_vals else np.nan
        f["hist_l3f_mean5"] = np.mean(l3f_vals[:5]) if l3f_vals else np.nan
        f["hist_l3f_best5"] = min(l3f_vals[:5]) if l3f_vals else np.nan
        f["hist_l3f_worst5"] = max(l3f_vals[:5]) if l3f_vals else np.nan
        f["hist_l3f_std5"] = float(np.std(l3f_vals[:5])) if len(l3f_vals) >= 2 else np.nan
        f["hist_l3f_latest"] = l3f_vals[0] if l3f_vals else np.nan
        f["hist_l3f_trend"] = (
            (f["hist_l3f_latest"] - f["hist_l3f_mean5"])
            if not np.isnan(f["hist_l3f_latest"]) and not np.isnan(f["hist_l3f_mean5"])
            else np.nan
        )

        pos_vals = [r.relative_position for r in pos_runs]
        f["hist_pos_mean3"] = np.mean(pos_vals[:3]) if pos_vals else np.nan
        f["hist_pos_mean5"] = np.mean(pos_vals[:5]) if pos_vals else np.nan
        f["hist_pos_latest"] = pos_vals[0] if pos_vals else np.nan

        fp_vals = [r.finish_pos for r in runs]
        f["hist_finish_mean3"] = np.mean(fp_vals[:3]) if fp_vals else np.nan
        f["hist_finish_mean5"] = np.mean(fp_vals[:5]) if fp_vals else np.nan

        hw_vals = [r.horse_weight for r in runs if r.horse_weight]
        f["hist_hw_mean"] = np.mean(hw_vals[:5]) if hw_vals else np.nan
        f["hist_run_count"] = len(runs)

        # 条件別の上がり3F平均
        same_surf = [r.last_3f_sec for r in l3f_runs if r.surface == course.surface]
        f["hist_l3f_same_surface"] = np.mean(same_surf[:5]) if same_surf else np.nan

        target_band = _distance_band(course.distance)
        same_dist = [r.last_3f_sec for r in l3f_runs if _distance_band(r.distance) == target_band]
        f["hist_l3f_same_distband"] = np.mean(same_dist[:5]) if same_dist else np.nan

        # 同競馬場
        same_venue = [r.last_3f_sec for r in l3f_runs if r.venue == course.venue]
        f["hist_l3f_same_venue"] = np.mean(same_venue[:5]) if same_venue else np.nan

        # 同コース（場+馬場+距離）
        target_cid = course.course_id
        same_course = [r.last_3f_sec for r in l3f_runs if r.course_id == target_cid]
        f["hist_l3f_same_course"] = np.mean(same_course[:5]) if same_course else np.nan

        # 勝率・複勝率
        recent5 = runs[:5]
        f["hist_win_rate5"] = sum(1 for r in recent5 if r.finish_pos == 1) / max(len(recent5), 1)
        f["hist_place3_rate5"] = sum(1 for r in recent5 if r.finish_pos <= 3) / max(len(recent5), 1)

        # 距離変更
        f["hist_dist_change"] = course.distance - runs[0].distance if runs else np.nan

        # 出走間隔
        try:
            race_dt = datetime.strptime(race_info.race_date, "%Y-%m-%d")
            last_dt = datetime.strptime(runs[0].race_date, "%Y-%m-%d")
            f["days_since_last"] = (race_dt - last_dt).days
        except Exception:
            f["days_since_last"] = np.nan

        # 騎手
        jid = horse.jockey_id or ""
        f["jockey_avg_l3f"] = self._jockey_cache.get(jid, np.nan)
        cid_key = f"{course.venue_code}_{f['surface_enc']}_{course.distance}"
        f["jockey_course_avg_l3f"] = self._jockey_course_cache.get((jid, cid_key), np.nan)
        f["jockey_ride_count"] = self._jockey_ride_cache.get(jid, 0)

        # ペース文脈
        pc = pace_context or {}
        f["pace_n_front"] = pc.get("n_front", np.nan)
        f["pace_front_ratio"] = pc.get("front_ratio", np.nan)
        f["horse_style_est"] = f["hist_pos_mean5"]

        # 脚質別上がり3F: 先行型(相対位置<=0.3)と後方型(>=0.5)で分離
        l3f_front = [r.last_3f_sec for r in l3f_runs
                     if r.relative_position is not None and r.relative_position <= 0.3]
        l3f_rear = [r.last_3f_sec for r in l3f_runs
                    if r.relative_position is not None and r.relative_position >= 0.5]
        f["hist_l3f_front_mean"] = np.mean(l3f_front[:5]) if l3f_front else np.nan
        f["hist_l3f_rear_mean"] = np.mean(l3f_rear[:5]) if l3f_rear else np.nan

        # フィールド強度（Phase8追加）
        # pace_contextにフィールド全馬の過去走l3f平均が入っている
        pc_field = pace_context or {}
        f["field_hist_l3f_mean"] = pc_field.get("field_hist_l3f_mean", np.nan)
        f["field_hist_finish_mean"] = pc_field.get("field_hist_finish_mean", np.nan)
        # 自馬の過去走l3f平均 vs フィールド平均
        _fld_l3f = f["field_hist_l3f_mean"]
        _self_l3f = f["hist_l3f_mean3"]
        if not np.isnan(_fld_l3f) and not np.isnan(_self_l3f):
            f["horse_l3f_vs_field"] = _self_l3f - _fld_l3f
        else:
            f["horse_l3f_vs_field"] = np.nan

        # コース形状（Phase5追加）
        f["straight_m"] = course.straight_m
        f["corner_count"] = course.corner_count
        f["corner_type_enc"] = _encode_corner_type(course.corner_type)
        f["slope_type_enc"] = _encode_slope_type(course.slope_type)
        f["first_corner_m"] = course.first_corner_m
        f["l3f_corner_m"] = getattr(course, "l3f_corner_m", max(0, 600 - course.straight_m))
        f["straight_ratio"] = course.straight_m / max(course.distance, 1)
        # 残り600m地点データ（Phase6追加）
        f["l3f_corners"] = getattr(course, "l3f_corners", 1)
        f["l3f_elevation"] = getattr(course, "l3f_elevation", 0.0)
        f["l3f_hill_start"] = getattr(course, "l3f_hill_start", 0)
        f["l3f_straight_pct"] = getattr(course, "l3f_straight_pct", min(1.0, course.straight_m / 600))

        return f


def build_jockey_cache(df: pd.DataFrame) -> None:
    """騎手キャッシュJSONを構築・保存"""
    valid = df[df["last_3f_sec"].between(MIN_LAST3F, MAX_LAST3F)]
    avg = valid.groupby("jockey_id")["last_3f_sec"].mean().to_dict()
    course_avg = valid.groupby(["jockey_id", "course_id"])["last_3f_sec"].mean()
    course_avg_dict = {f"{jid}|{cid}": v for (jid, cid), v in course_avg.items()}
    rides = df.groupby("jockey_id").size().to_dict()

    cache = {
        "avg": {k: round(v, 3) for k, v in avg.items()},
        "course_avg": {k: round(v, 3) for k, v in course_avg_dict.items()},
        "rides": rides,
    }
    path = os.path.join(ML_DATA_DIR, "last3f_jockey_cache.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    logger.info(f"騎手キャッシュ保存: {path} ({len(avg)}騎手)")


# ============================================================
# 7. レポート出力
# ============================================================

def print_report(metrics: dict, fi: pd.DataFrame) -> None:
    """学習結果をコンソールに出力"""
    print("\n" + "=" * 60)
    print("  上がり3F予測モデル  学習レポート")
    print("=" * 60)

    print(f"\n検証データ: {metrics['val_size']:,}行")
    print(f"\n{'指標':<20} {'モデル':>10} {'ベースライン':>12} {'改善':>8}")
    print("-" * 55)
    print(f"{'MAE (秒)':<20} {metrics['mae_model']:>10.4f} {metrics['mae_baseline']:>12.4f} "
          f"{metrics['improvement_mae_pct']:>+7.2f}%")
    print(f"{'RMSE (秒)':<20} {metrics['rmse_model']:>10.4f} {metrics['rmse_baseline']:>12.4f}")
    print(f"{'±1秒以内 (%)':<20} {metrics['within_1s_model']:>10.2f} {metrics['within_1s_baseline']:>12.2f}")
    print(f"{'±0.5秒以内 (%)':<20} {metrics['within_05s_model']:>10.2f} {metrics['within_05s_baseline']:>12.2f}")

    for label, key in [("JRA/NAR別", "by_org"), ("芝/ダート別", "by_surface"),
                        ("距離帯別", "by_distance")]:
        sub = metrics.get(key, {})
        if not sub:
            continue
        print(f"\n  {label}:")
        for name, m in sub.items():
            if m.get("n", 0) == 0:
                continue
            print(f"    {name:<12} MAE={m['mae_model']:.4f} (BL={m['mae_baseline']:.4f}) "
                  f"±1s={m['within_1s']:.1f}%  n={m['n']:,}")

    print("\n  特徴量重要度 Top 15:")
    for i, row in fi.head(15).iterrows():
        bar = "#" * int(row["pct"] / 2)
        print(f"    {i + 1:>2}. {row['feature']:<28} {row['pct']:>6.2f}%  {bar}")

    print(f"\n  学習ラウンド: {metrics['best_iteration']}")
    print("=" * 60)


# ============================================================
# 8. メインパイプライン
# ============================================================

def run_training_pipeline() -> dict:
    """学習パイプライン全体を実行"""
    t_all = time.time()

    # 1. データ読込
    logger.info("Step 1/6: データ読込")
    df = load_ml_data()

    # 2. 特徴量構築
    logger.info("Step 2/6: 特徴量構築")
    df = build_feature_table(df)

    # 3. データ分割
    logger.info("Step 3/6: データ分割")
    train_df, val_df = prepare_datasets(df)

    # 4. 学習
    logger.info("Step 4/6: モデル学習")
    model = train_model(train_df, val_df)

    # 5. 評価
    logger.info("Step 5/6: モデル評価")
    metrics = evaluate_model(model, val_df, train_df)
    fi = get_feature_importance(model)

    # 6. 保存
    logger.info("Step 6/6: モデル保存")
    save_model(model, metrics)
    build_jockey_cache(df)

    print_report(metrics, fi)

    elapsed = time.time() - t_all
    logger.info(f"パイプライン完了: {elapsed:.1f}秒")

    return metrics


def run_evaluation_only() -> None:
    """保存済みモデルで評価のみ"""
    model = load_model()
    if model is None:
        print("モデルが見つかりません。先に学習を実行してください。")
        return

    df = load_ml_data()
    df = build_feature_table(df)
    _, val_df = prepare_datasets(df)
    metrics = evaluate_model(model, val_df)
    fi = get_feature_importance(model)
    print_report(metrics, fi)


def run_importance_only() -> None:
    """特徴量重要度のみ表示"""
    model = load_model()
    if model is None:
        print("モデルが見つかりません。先に学習を実行してください。")
        return

    fi = get_feature_importance(model)
    print("\n特徴量重要度:")
    for i, row in fi.iterrows():
        bar = "#" * int(row["pct"] / 2)
        print(f"  {i + 1:>2}. {row['feature']:<28} {row['pct']:>6.2f}%  {bar}")


# ============================================================
# CLI
# ============================================================

def main():
    if "--evaluate-only" in sys.argv:
        run_evaluation_only()
    elif "--importance" in sys.argv:
        run_importance_only()
    else:
        run_training_pipeline()


if __name__ == "__main__":
    main()
