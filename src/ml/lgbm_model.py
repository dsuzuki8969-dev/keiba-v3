"""
LightGBM 競馬予測モデル

ML学習データ (data/ml/*.json) から特徴量を構築し、
複勝圏内 (3着以内) 確率を予測するモデルを学習・推論する。

特徴量 (68本):
  - レース情報: 馬場・距離・馬場状態・頭数・グレード・会場・月
  - コース構造: 直線長・高低差・初角位置・コーナー種別・回り方向
  - 馬エントリー: 枠番・馬番・性別・年齢・斤量・馬体重
  - 馬ローリング: 勝率・複勝率・平均着順・前走着順・間隔日数
  - 騎手ローリング: 勝率・複勝率・直近90日・会場別・馬場別・距離帯別
  - 調教師ローリング: 勝率・複勝率・直近90日・会場別
  - 騎手×調教師コンビ: 勝率・出走数
  - 競馬場類似度重み付き実績: 類似場複勝率・勝率・平均着順など
  - 血統ローリング (改善C): 父馬産駒勝率・複勝率、母父馬産駒勝率・複勝率
  - Tier1追加特徴量: トレンド・乗り替わり・脚質推定・馬場適性
  - Step2スタッキング: 位置取り推定・上がり3F推定
  - コース分析 (Task#26): 枠×競馬場・脚質×馬場・枠×脚質 複勝率
"""

import json
import math
import os
import pickle
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.log import get_logger

logger = get_logger(__name__)

_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DATA_DIR = os.path.join(_BASE, "data", "ml")
MODEL_DIR = os.path.join(_BASE, "data", "models")
MODEL_PATH  = os.path.join(MODEL_DIR, "lgbm_place.txt")        # 後方互換 alias
STATS_PATH  = os.path.join(MODEL_DIR, "rolling_stats.pkl")

# フィルタキー → モデルファイル名マッピング
# キー命名規則: "global" / "turf" / "dirt" / "jra_turf" / "jra_dirt" / "nar"
#               / "venue_{code}" / "jra_turf_{smile}" / "jra_dirt_{smile}"
def _model_path(key: str) -> str:
    if key == "global":
        return os.path.join(MODEL_DIR, "lgbm_place.txt")
    return os.path.join(MODEL_DIR, f"lgbm_place_{key}.txt")

# 最低学習サンプル数 (これ未満のモデルはスキップ)
MIN_TRAIN_SAMPLES = 4000
SIRE_MAP_PATH = os.path.join(MODEL_DIR, "horse_sire_map.pkl")    # horse_id → (sire_id, bms_id)
SIRE_STATS_PATH = os.path.join(MODEL_DIR, "sire_rolling_stats.pkl")  # RollingSireTracker

FEATURE_COLUMNS = [
    "surface", "distance", "condition", "field_count",
    "is_jra", "grade_code", "venue_code", "month",
    # コース構造4因子 + 回り方向
    "venue_straight_m", "venue_slope", "venue_first_corner",
    "venue_corner_type", "venue_direction",
    # 馬個体
    "gate_no", "horse_no", "sex_code", "age",
    "weight_kg", "horse_weight", "weight_change",
    "horse_win_rate", "horse_place_rate", "horse_runs",
    "horse_avg_finish", "horse_last_finish", "horse_days_since",
    "jockey_win_rate", "jockey_place_rate", "jockey_runs",
    "jockey_win_rate_90d", "jockey_place_rate_90d",
    "jockey_venue_wr", "jockey_surface_wr", "jockey_dist_wr",
    "jockey_surf_dist_wr", "jockey_surf_dist_pr",
    "jockey_sim_venue_wr", "jockey_sim_venue_pr",
    "jockey_sim_venue_dist_wr", "jockey_sim_venue_dist_pr",
    "trainer_win_rate", "trainer_place_rate", "trainer_runs",
    "trainer_win_rate_90d", "trainer_place_rate_90d", "trainer_venue_wr",
    "trainer_surface_wr", "trainer_dist_wr",
    "trainer_surf_dist_wr", "trainer_surf_dist_pr",
    "trainer_sim_venue_wr", "trainer_sim_venue_pr",
    "trainer_sim_venue_dist_wr", "trainer_sim_venue_dist_pr",
    "jt_combo_wr", "jt_combo_runs",
    # 競馬場類似度重み付き実績
    "venue_sim_place_rate", "venue_sim_win_rate", "venue_sim_avg_finish",
    "venue_sim_runs", "venue_sim_n_venues",
    "same_dir_place_rate", "same_dir_runs",
    # 血統ローリング (父馬・母父馬の産駒成績)
    "sire_win_rate", "sire_place_rate",
    "bms_win_rate", "bms_place_rate",
    # Tier1追加特徴量 (Step 1)
    "trend_position_slope", "trend_deviation_slope",
    "dev_run1", "dev_run2", "chakusa_index_avg3",
    "is_jockey_change", "kishu_pattern_code",
    "is_long_break",
    "horse_running_style", "horse_condition_match",
    # Batch1: ②コーナー別位置変化
    "avg_pos_change_3to4c",   # 3角→4角の位置前進量の平均（正=前進=Good）
    "pos_change_3to4c_last",  # 前走の3角→4角位置前進量
    "avg_pos_change_1to4c",   # 1角→4角の総移動量の平均（正=前進）
    "front_hold_rate",        # 1角先頭30%時に4角でも維持できた率
    # Batch1: ③着差指数の再設計
    "margin_norm_last",       # 前走の頭数補正着差（0=勝ち, 負=負けた幅）
    "margin_norm_avg3",       # 直近3走の頭数補正着差平均
    # Batch2: ①タイム指数（走破タイム補正）
    "speed_index_last",       # 前走のタイム指数（正=基準より速い）
    "speed_index_avg3",       # 直近3走タイム指数平均
    "speed_index_best3",      # 直近3走タイム指数最高値
    # Batch4: ⑤道中タイムペース適性
    "place_rate_fast_pace",   # ハイペース（逃げ馬道中タイムが速い）時の複勝率
    "place_rate_slow_pace",   # スローペース時の複勝率
    "pace_pref_score",        # ハイペース複勝率 - スローペース複勝率（展開得意/不得意）
    "pace_count_fast",        # ハイペース出走数（信頼度）
    "pace_count_slow",        # スローペース出走数
    "pace_norm_last",         # 前走のレースペース指標（連続値、正=ハイペース）
    "pace_norm_avg3",         # 直近3走のペース指標平均（連続値）
    # Batch5: 展開予測（フィールド脚質構成）
    "front_runner_count_in_race",  # フィールド内の逃げ・先行馬数（running_style<0.35）
    "pace_pressure_index",         # 逃げ・先行馬比率（0=スロー予測, 1=ハイペース予測）
    "style_pace_affinity",         # 脚質×展開相性スコア（+1=追込でハイペース/最高、-1=逃げでハイペース/最悪）
    # Step 2: サブモデル出力スタッキング特徴量
    "ml_pos_est", "ml_l3f_est",
    # Task #26: コース分析特徴量
    "gate_venue_wr", "style_surface_wr", "gate_style_wr",
    # Task #27: 血統×コンテキスト特徴量
    "sire_surf_wr", "sire_smile_wr", "bms_surf_wr",
    # Batch3: ④ニック理論（父×母父の組み合わせ複勝率）
    "sire_x_bms_place_rate",
    # 類似場加重×条件別: 父馬
    "sire_surf_dist_wr", "sire_surf_dist_pr",
    "sire_sim_venue_wr", "sire_sim_venue_pr",
    "sire_sim_venue_dist_wr", "sire_sim_venue_dist_pr",
    # 類似場加重×条件別: 母父
    "bms_surf_dist_wr", "bms_surf_dist_pr",
    "bms_sim_venue_wr", "bms_sim_venue_pr",
    "bms_sim_venue_dist_wr", "bms_sim_venue_dist_pr",
    # 父×母父 win rate
    "sire_bms_wr",
    # ② クラス変化特徴量
    "class_change", "prev_grade_code",
    # ③ スピード指数 (走破タイム距離補正)
    "speed_sec_per_m_est",
    # ① レース内相対特徴量 (2パス方式)
    "jockey_place_rank_in_race", "trainer_place_rank_in_race",
    "horse_form_rank_in_race", "horse_place_rank_in_race",
    "venue_sim_rank_in_race",
    "jockey_place_zscore_in_race", "horse_form_zscore_in_race",
    "relative_weight_kg",
    "jockey_wp_ratio", "trainer_wp_ratio",
    # ML-1 追加特徴量
    "jt_combo_wr_30d",
    "jt_combo_place_rate_30d",
    "weight_kg_trend_3run",
    # ---- Phase 10B: 展開特徴量追加 ----
    "field_pace_variance",        # フィールド内脚質分散（均等vsペース偏り）
    "early_position_est",         # 序盤位置取り推定（枠番×脚質）
    "last3f_pace_diff",           # 上がり3F推定 - 位置推定の差分
    "pace_horse_match",           # 馬のペース選好×予想ペースの一致度
    # ---- Phase 10B: 血統特徴量追加 ----
    "sire_credibility",           # 父馬の産駒成績信頼度 log(runs+1)
    "bms_credibility",            # 母父の産駒成績信頼度 log(runs+1)
    "sire_surface_pref",          # 父馬の芝/ダ適性差（芝PR - ダートPR）
    "bms_surface_pref",           # 母父の芝/ダ適性差
    "sire_dist_pref",             # 父馬の距離適性差（短距離PR - 長距離PR）
    "sire_recent_trend",          # 父馬の直近産駒成績トレンド
    # ---- Phase 10B: 調教師特徴量追加 ----
    "trainer_class_trend",        # 直近20走のクラスレベル推移
    "trainer_rest_wr",            # 休養明け馬の複勝率
    # ---- Phase 11: タイム指数マルチウィンドウ ----
    "speed_index_avg_1y",         # 過去1年のタイム指数平均
    "speed_index_best_1y",        # 過去1年のタイム指数ベスト
    "speed_index_avg_6m",         # 過去半年のタイム指数平均
    "speed_index_trend",          # 過去1年のタイム指数傾き（正=改善）
    # ---- Phase 11: 馬の条件別複勝率 ----
    "horse_pr_2y",                # 過去2年の複勝率
    "horse_venue_pr",             # 当競馬場での複勝率
    "horse_dist_pr",              # 当距離帯(±200m)の複勝率
    "horse_smile_pr",             # 当SMILE区分の複勝率
    "horse_style_pr",             # 脚質帯(前/中/後)の複勝率
    "horse_gate_pr",              # 枠番帯の複勝率
    "horse_jockey_pr",            # 当騎手との複勝率
    # ---- Phase 11: 騎手の条件別複勝率 + 2年ウィンドウ ----
    "jockey_pr_2y",               # 過去2年の複勝率
    "jockey_venue_pr",            # 当競馬場の複勝率
    "jockey_dist_pr",             # 当距離帯の複勝率
    "jockey_smile_pr",            # 当SMILE区分の複勝率
    "jockey_cond_pr",             # 当馬場状態の複勝率
    # ---- Phase 11: 調教師の条件別複勝率 + 2年ウィンドウ ----
    "trainer_pr_2y",              # 過去2年の複勝率
    "trainer_venue_pr",           # 当競馬場の複勝率
    "trainer_dist_pr",            # 当距離帯の複勝率
    "trainer_smile_pr",           # 当SMILE区分の複勝率
    "trainer_cond_pr",            # 当馬場状態の複勝率
    # ---- Phase 11: 父の条件別複勝率 ----
    "sire_smile_pr",              # SMILE区分の産駒複勝率
    "sire_cond_pr",               # 馬場状態別の産駒複勝率
    "sire_venue_pr",              # 当競馬場の産駒複勝率（非加重）
    # ---- Phase 11: 母父の条件別複勝率 ----
    "bms_smile_pr",               # SMILE区分の産駒複勝率
    "bms_cond_pr",                # 馬場状態別の産駒複勝率
    "bms_venue_pr",               # 当競馬場の産駒複勝率（非加重）
    "bms_dist_pr",                # SMILE区分複勝率（母父独自）
    # ---- Phase 12: 条件別複勝率追加 ----
    "horse_cond_pr",              # 馬の馬場状態別複勝率
    "jockey_pace_pr",             # 騎手のペース別複勝率
    "jockey_style_pr",            # 騎手の脚質別複勝率
    "jockey_gate_pr",             # 騎手の枠番帯別複勝率
    "jockey_horse_pr",            # 騎手の騎乗馬別複勝率
    "trainer_pace_pr",            # 調教師のペース別複勝率
    "trainer_style_pr",           # 調教師の脚質別複勝率
    "trainer_gate_pr",            # 調教師の枠番帯別複勝率
    "trainer_horse_pr",           # 調教師の騎乗馬別複勝率
    "sire_pace_pr",               # 父のペース別産駒複勝率
    "sire_style_pr",              # 父の脚質別産駒複勝率
    "sire_gate_pr",               # 父の枠番帯別産駒複勝率
    "sire_jockey_pr",             # 父×騎手の産駒複勝率
    "sire_trainer_pr",            # 父×調教師の産駒複勝率
    "bms_pace_pr",                # 母父のペース別産駒複勝率
    "bms_style_pr",               # 母父の脚質別産駒複勝率
    "bms_gate_pr",                # 母父の枠番帯別産駒複勝率
    "bms_jockey_pr",              # 母父×騎手の産駒複勝率
    "bms_trainer_pr",             # 母父×調教師の産駒複勝率
]

# ばんえい専用: コーナー/ペース/上がり3F/SMILE/脚質/スピード指数など存在しない概念を除外
# 全0特徴量(trend_deviation_slope, chakusa_index_avg3, margin_norm_*)と100%NaN(trainer_rest_wr)も除外
# 高NaN血統特徴量はLightGBMがNaNをネイティブ処理するため残す
FEATURE_COLUMNS_BANEI = [
    # レース条件（surface/distance/venue_codeは帯広1場固定→除外）
    "condition", "field_count", "month", "grade_code",
    # 馬個体（weight_kg=斤量、horse_weight=馬体重 — ばんえい最重要）
    "gate_no", "horse_no", "sex_code", "age",
    "weight_kg", "horse_weight", "weight_change",
    # 馬の成績
    "horse_win_rate", "horse_place_rate", "horse_runs",
    "horse_avg_finish", "horse_last_finish", "horse_days_since",
    # 騎手の成績
    "jockey_win_rate", "jockey_place_rate", "jockey_runs",
    "jockey_win_rate_90d", "jockey_place_rate_90d",
    "jockey_venue_wr",
    # 調教師の成績
    "trainer_win_rate", "trainer_place_rate", "trainer_runs",
    "trainer_win_rate_90d", "trainer_place_rate_90d",
    "trainer_venue_wr",
    # 騎手×調教師コンボ
    "jt_combo_wr", "jt_combo_runs",
    "jt_combo_wr_30d", "jt_combo_place_rate_30d",
    # 血統ローリング
    "sire_win_rate", "sire_place_rate",
    "bms_win_rate", "bms_place_rate",
    "sire_bms_wr",
    # フォーム・トレンド（全0の trend_deviation_slope, chakusa_index_avg3 は除外）
    "trend_position_slope",
    "dev_run1", "dev_run2",
    "is_jockey_change", "is_long_break",
    # クラス変化
    "class_change", "prev_grade_code",
    # レース内相対特徴量
    "jockey_place_rank_in_race", "trainer_place_rank_in_race",
    "horse_form_rank_in_race", "horse_place_rank_in_race",
    "jockey_place_zscore_in_race", "horse_form_zscore_in_race",
    "relative_weight_kg",
    "jockey_wp_ratio", "trainer_wp_ratio",
    # 斤量トレンド
    "weight_kg_trend_3run",
    # 血統信頼度・トレンド
    "sire_credibility", "bms_credibility",
    "sire_recent_trend",
    # 調教師特徴量（trainer_rest_wr は100%NaN→除外）
    "trainer_class_trend",
    # 馬の条件別複勝率
    "horse_pr_2y", "horse_venue_pr",
    "horse_gate_pr", "horse_jockey_pr",
    "horse_cond_pr",
    # 騎手の条件別複勝率
    "jockey_pr_2y", "jockey_venue_pr", "jockey_cond_pr",
    # 調教師の条件別複勝率
    "trainer_pr_2y", "trainer_venue_pr", "trainer_cond_pr",
    # 血統×競馬場・条件
    "sire_venue_pr", "sire_cond_pr",
    "bms_venue_pr", "bms_cond_pr",
    # 枠番帯別
    "jockey_gate_pr", "trainer_gate_pr",
    "sire_gate_pr", "bms_gate_pr",
    # 個別馬×人コンボ
    "jockey_horse_pr", "trainer_horse_pr",
    "sire_jockey_pr", "sire_trainer_pr",
    "bms_jockey_pr", "bms_trainer_pr",
]

CATEGORICAL_FEATURES = ["venue_code", "venue_direction"]

# 6カテゴリ特徴量グループ定義 (能力/展開/適性/騎手/調教師/血統)
# compositeの6因子と対応。旧「市場」「体型」カテゴリは各カテゴリに吸収済み。
FEATURE_CATEGORY_6: Dict[str, List[str]] = {
    "能力":   ["dev_run1", "dev_run2", "chakusa_index_avg3",
               "avg_pos_change_3to4c", "pos_change_3to4c_last",
               "avg_pos_change_1to4c", "front_hold_rate",
               "margin_norm_last", "margin_norm_avg3",
               "trend_position_slope", "trend_deviation_slope",
               "horse_win_rate", "horse_place_rate", "horse_avg_finish",
               "horse_last_finish", "horse_runs",
               "venue_sim_place_rate", "venue_sim_win_rate",
               "venue_sim_avg_finish", "venue_sim_runs", "venue_sim_n_venues",
               "same_dir_place_rate", "same_dir_runs",
               "horse_form_rank_in_race", "horse_place_rank_in_race",
               "venue_sim_rank_in_race", "horse_form_zscore_in_race",
               "class_change", "prev_grade_code",
               # 旧「体型」から吸収: 馬体・状態・年齢
               "horse_weight", "weight_change", "is_long_break",
               "horse_days_since", "sex_code", "age", "weight_kg",
               "relative_weight_kg", "weight_kg_trend_3run",
               # Phase 11: タイム指数マルチウィンドウ + 馬の条件別複勝率
               "speed_index_avg_1y", "speed_index_best_1y",
               "speed_index_avg_6m", "speed_index_trend",
               "horse_pr_2y", "horse_venue_pr", "horse_dist_pr",
               "horse_smile_pr", "horse_style_pr", "horse_gate_pr",
               "horse_cond_pr"],
    "展開":   ["ml_pos_est", "horse_running_style", "ml_l3f_est", "speed_sec_per_m_est",
               "speed_index_last", "speed_index_avg3", "speed_index_best3",
               "place_rate_fast_pace", "place_rate_slow_pace", "pace_pref_score",
               "pace_count_fast", "pace_count_slow",
               "pace_norm_last", "pace_norm_avg3",
               "front_runner_count_in_race", "pace_pressure_index", "style_pace_affinity",
               # Phase 10B 追加
               "field_pace_variance", "early_position_est",
               "last3f_pace_diff", "pace_horse_match"],
    "適性":   ["surface", "distance", "condition", "is_jra",
               "venue_straight_m", "venue_slope", "venue_first_corner",
               "venue_corner_type", "venue_direction",
               "horse_condition_match", "grade_code", "month",
               "venue_code", "field_count",
               # 旧「体型」から吸収: 枠番
               "gate_no", "horse_no",
               "gate_venue_wr", "style_surface_wr", "gate_style_wr"],
    "騎手":   ["is_jockey_change", "kishu_pattern_code",
               "jockey_win_rate", "jockey_place_rate", "jockey_runs",
               "jockey_win_rate_90d", "jockey_place_rate_90d",
               "jockey_venue_wr", "jockey_surface_wr", "jockey_dist_wr",
               "jockey_surf_dist_wr", "jockey_surf_dist_pr",
               "jockey_sim_venue_wr", "jockey_sim_venue_pr",
               "jockey_sim_venue_dist_wr", "jockey_sim_venue_dist_pr",
               "jockey_place_rank_in_race", "jockey_place_zscore_in_race",
               "jockey_wp_ratio",
               # Phase 11: 騎手の条件別複勝率 + 2年ウィンドウ
               "jockey_pr_2y", "jockey_venue_pr", "jockey_dist_pr",
               "jockey_smile_pr", "jockey_cond_pr",
               "horse_jockey_pr",
               "jockey_pace_pr", "jockey_style_pr", "jockey_gate_pr", "jockey_horse_pr"],
    "調教師": ["trainer_win_rate", "trainer_place_rate", "trainer_runs",
               "trainer_win_rate_90d", "trainer_place_rate_90d",
               "trainer_venue_wr", "trainer_surface_wr", "trainer_dist_wr",
               "trainer_surf_dist_wr", "trainer_surf_dist_pr",
               "trainer_sim_venue_wr", "trainer_sim_venue_pr",
               "trainer_sim_venue_dist_wr", "trainer_sim_venue_dist_pr",
               "jt_combo_wr", "jt_combo_runs",
               "trainer_place_rank_in_race", "trainer_wp_ratio",
               "jt_combo_wr_30d", "jt_combo_place_rate_30d",
               # Phase 10B 追加
               "trainer_class_trend", "trainer_rest_wr",
               # Phase 11: 調教師の条件別複勝率 + 2年ウィンドウ
               "trainer_pr_2y", "trainer_venue_pr", "trainer_dist_pr",
               "trainer_smile_pr", "trainer_cond_pr",
               "trainer_pace_pr", "trainer_style_pr", "trainer_gate_pr", "trainer_horse_pr"],
    "血統":   ["sire_win_rate", "sire_place_rate",
               "bms_win_rate", "bms_place_rate",
               "sire_surf_wr", "sire_smile_wr", "bms_surf_wr",
               "sire_x_bms_place_rate", "sire_bms_wr",
               "sire_surf_dist_wr", "sire_surf_dist_pr",
               "sire_sim_venue_wr", "sire_sim_venue_pr",
               "sire_sim_venue_dist_wr", "sire_sim_venue_dist_pr",
               "bms_surf_dist_wr", "bms_surf_dist_pr",
               "bms_sim_venue_wr", "bms_sim_venue_pr",
               "bms_sim_venue_dist_wr", "bms_sim_venue_dist_pr",
               # Phase 10B 追加
               "sire_credibility", "bms_credibility",
               "sire_surface_pref", "bms_surface_pref",
               "sire_dist_pref", "sire_recent_trend",
               # Phase 11: 父/母父の条件別複勝率
               "sire_smile_pr", "sire_cond_pr", "sire_venue_pr",
               "bms_smile_pr", "bms_cond_pr", "bms_venue_pr", "bms_dist_pr",
               # Phase 12: ペース/脚質/枠番/騎手/調教師別
               "sire_pace_pr", "sire_style_pr", "sire_gate_pr",
               "sire_jockey_pr", "sire_trainer_pr",
               "bms_pace_pr", "bms_style_pr", "bms_gate_pr",
               "bms_jockey_pr", "bms_trainer_pr"],
}
# 後方互換: 旧名称からの参照
SHAP_FEATURE_GROUPS = FEATURE_CATEGORY_6

SURFACE_MAP = {"芝": 0, "ダート": 1, "障害": 2}
CONDITION_MAP = {"良": 0, "稍": 1, "稍重": 1, "重": 2, "不": 3, "不良": 3}
SEX_MAP = {"牡": 0, "牝": 1, "セ": 2, "セン": 2, "騸": 2}
GRADE_MAP = {
    "新馬": 0, "未勝利": 1, "1勝": 2, "2勝": 3, "3勝": 4,
    "OP": 5, "L": 5, "交流重賞": 6, "G3": 6, "G2": 7, "G1": 8,
    "その他": 1,
}


# Batch1 ③: 着差文字列 → 馬身数(float) 変換ヘルパー
_MARGIN_JP_MAP = {
    "ハナ": 0.05, "アタマ": 0.1, "クビ": 0.2, "大差": 10.0, "同着": 0.0,
}

def _parse_margin(raw_margin) -> float:
    """着差（文字列または数値）を馬身数(float)に変換。勝ち・不明は0.0を返す"""
    if raw_margin is None:
        return 0.0
    if isinstance(raw_margin, (int, float)):
        return float(raw_margin)
    if isinstance(raw_margin, str):
        s = raw_margin.strip()
        if not s:
            return 0.0
        if s in _MARGIN_JP_MAP:
            return _MARGIN_JP_MAP[s]
        # "1.1/2" → 1 + 1/2 = 1.5
        if "." in s and "/" in s:
            try:
                parts = s.split(".", 1)
                whole = float(parts[0])
                frac_parts = parts[1].split("/")
                frac = float(frac_parts[0]) / float(frac_parts[1])
                return whole + frac
            except (ValueError, IndexError, ZeroDivisionError):
                pass
        # "1/2" → 0.5
        if "/" in s:
            try:
                a, b = s.split("/", 1)
                return float(a) / float(b)
            except (ValueError, ZeroDivisionError):
                pass
        # plain number "1", "2.5"
        try:
            return float(s)
        except ValueError:
            return 0.0
    return 0.0


def _dist_category(d: int) -> str:
    if d <= 1400:
        return "sprint"
    if d <= 1800:
        return "mile"
    if d <= 2200:
        return "middle"
    return "long"


def _smile_key_ml(dist: int) -> str:
    """距離 → SMILE+SS 6区分 (SS/S/M/I/L/E)"""
    if dist <= 1000: return "ss"
    if dist <= 1400: return "s"
    if dist <= 1800: return "m"
    if dist <= 2200: return "i"
    if dist <= 2600: return "l"
    return "e"


def _safe_int(v, default=0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


# ============================================================
# コース分析ヘルパー
# ============================================================


def _gate_group(gate_no: int) -> str:
    """枠番 → 4グループ (内枠/外枠判定用)"""
    if not gate_no:
        return ""
    if gate_no <= 2:
        return "g12"
    if gate_no <= 4:
        return "g34"
    if gate_no <= 6:
        return "g56"
    return "g78"


def _style_group(rel_pos: float) -> str:
    """4角相対位置 → 脚質グループ (0=先頭, 1=最後方)"""
    if rel_pos <= 0.30:
        return "front"
    if rel_pos <= 0.60:
        return "middle"
    return "rear"


# ============================================================
# ローリング統計トラッカー
# ============================================================


class _EntityStats:
    """騎手・調教師のローリング統計"""

    __slots__ = ("wins", "runs", "places", "recent", "venue", "surface", "dist_cat",
                 "surf_dist", "venue_surf", "venue_surf_dist",
                 "smile", "cond",
                 "pace", "style", "gate", "horse_combo")

    def __init__(self):
        self.wins = 0
        self.runs = 0
        self.places = 0
        self.recent: List[Tuple[str, bool, bool]] = []
        # Phase 11: 全て [wins, places, runs] の3要素に統一
        self.venue: Dict[str, list] = {}               # {venue: [w, p, r]}
        self.surface: Dict[str, list] = {}             # {surface: [w, p, r]}
        self.dist_cat: Dict[str, list] = {}            # {dist_cat: [w, p, r]}
        self.surf_dist: Dict[tuple, list] = {}         # {(surface, dist_cat): [w, p, r]}
        self.venue_surf: Dict[tuple, list] = {}        # {(venue, surface): [w, p, r]}
        self.venue_surf_dist: Dict[tuple, list] = {}   # {(venue, surface, dist_cat): [w, p, r]}
        # Phase 11 新ディメンション
        self.smile: Dict[str, list] = {}               # {smile_cat: [w, p, r]}  SMILE 6区分
        self.cond: Dict[str, list] = {}                # {condition: [w, p, r]}  馬場状態
        # Phase 12: ペース/脚質/枠番/騎乗馬
        self.pace: Dict[str, list] = {}                # {pace_grp(H/M/S): [w, p, r]}
        self.style: Dict[str, list] = {}               # {style_grp(前/中/後): [w, p, r]}
        self.gate: Dict[str, list] = {}                # {gate_grp(g12/g34/g56/g78): [w, p, r]}
        self.horse_combo: Dict[str, list] = {}         # {horse_id: [w, p, r]}

    def update(self, date_str: str, is_win: bool, is_place: bool,
               venue: str = "", surface: str = "", dist_cat: str = "",
               smile_cat: str = "", condition: str = "",
               pace_grp: str = "", style_grp: str = "",
               gate_grp: str = "", horse_id: str = ""):
        self.wins += int(is_win)
        self.runs += 1
        self.places += int(is_place)
        self.recent.append((date_str, is_win, is_place))
        _w, _p = int(is_win), int(is_place)
        if venue:
            v = self.venue.setdefault(venue, [0, 0, 0])
            v[0] += _w; v[1] += _p; v[2] += 1
        if surface:
            v = self.surface.setdefault(surface, [0, 0, 0])
            v[0] += _w; v[1] += _p; v[2] += 1
        if dist_cat:
            v = self.dist_cat.setdefault(dist_cat, [0, 0, 0])
            v[0] += _w; v[1] += _p; v[2] += 1
        if surface and dist_cat:
            sd = self.surf_dist.setdefault((surface, dist_cat), [0, 0, 0])
            sd[0] += _w; sd[1] += _p; sd[2] += 1
        if venue and surface:
            vs = self.venue_surf.setdefault((venue, surface), [0, 0, 0])
            vs[0] += _w; vs[1] += _p; vs[2] += 1
        if venue and surface and dist_cat:
            vsd = self.venue_surf_dist.setdefault((venue, surface, dist_cat), [0, 0, 0])
            vsd[0] += _w; vsd[1] += _p; vsd[2] += 1
        # Phase 11 新ディメンション
        if smile_cat:
            v = self.smile.setdefault(smile_cat, [0, 0, 0])
            v[0] += _w; v[1] += _p; v[2] += 1
        if condition:
            v = self.cond.setdefault(condition, [0, 0, 0])
            v[0] += _w; v[1] += _p; v[2] += 1
        # Phase 12: ペース/脚質/枠番/騎乗馬
        if pace_grp:
            v = getattr(self, 'pace', None)
            if v is None:
                self.pace = v = {}
            vv = v.setdefault(pace_grp, [0, 0, 0])
            vv[0] += _w; vv[1] += _p; vv[2] += 1
        if style_grp:
            v = getattr(self, 'style', None)
            if v is None:
                self.style = v = {}
            vv = v.setdefault(style_grp, [0, 0, 0])
            vv[0] += _w; vv[1] += _p; vv[2] += 1
        if gate_grp:
            v = getattr(self, 'gate', None)
            if v is None:
                self.gate = v = {}
            vv = v.setdefault(gate_grp, [0, 0, 0])
            vv[0] += _w; vv[1] += _p; vv[2] += 1
        if horse_id:
            v = getattr(self, 'horse_combo', None)
            if v is None:
                self.horse_combo = v = {}
            vv = v.setdefault(horse_id, [0, 0, 0])
            vv[0] += _w; vv[1] += _p; vv[2] += 1

    def sim_venue_rate(self, target_venue: str, surface: str, dist_cat: str = None,
                       min_sim: float = 0.35, sim_power: float = 2.0,
                       min_runs: int = 3) -> Tuple[Optional[float], Optional[float]]:
        """類似場加重の(win_rate, place_rate)を返す"""
        from data.masters.venue_similarity import get_venue_similarity
        table = self.venue_surf_dist if dist_cat else self.venue_surf
        w_wins = w_places = w_runs = 0.0
        for key, counts in table.items():
            v, s = key[0], key[1]
            dc = key[2] if len(key) > 2 else None
            if s != surface:
                continue
            if dist_cat and dc != dist_cat:
                continue
            if counts[2] < min_runs:
                continue
            sim = 1.0 if v == target_venue else get_venue_similarity(target_venue, v)
            if sim < min_sim:
                continue
            w = sim ** sim_power
            w_wins += counts[0] * w
            w_places += counts[1] * w
            w_runs += counts[2] * w
        if w_runs == 0:
            return None, None
        return w_wins / w_runs, w_places / w_runs

    @property
    def win_rate(self):
        return self.wins / self.runs if self.runs >= 3 else None

    @property
    def place_rate(self):
        return self.places / self.runs if self.runs >= 3 else None

    def rate_recent(self, cutoff: str, use_place=False):
        w, r = 0, 0
        for d, iw, ip in self.recent:
            if d >= cutoff:
                r += 1
                w += int(ip if use_place else iw)
        return w / r if r >= 3 else None

    def venue_wr(self, venue: str):
        v = self.venue.get(venue)
        if not v:
            return None
        r = v[2] if len(v) >= 3 else v[1]  # 旧2要素互換
        return v[0] / r if r >= 3 else None

    def venue_pr(self, venue: str):
        """Phase 11: 競馬場別複勝率"""
        v = self.venue.get(venue)
        if not v or len(v) < 3:
            return None
        return v[1] / v[2] if v[2] >= 3 else None

    def surface_wr(self, surface: str):
        v = self.surface.get(surface)
        if not v:
            return None
        r = v[2] if len(v) >= 3 else v[1]
        return v[0] / r if r >= 3 else None

    def dist_cat_wr(self, dc: str):
        v = self.dist_cat.get(dc)
        if not v:
            return None
        r = v[2] if len(v) >= 3 else v[1]
        return v[0] / r if r >= 3 else None

    def dist_cat_pr(self, dc: str):
        """Phase 11: 距離帯別複勝率"""
        v = self.dist_cat.get(dc)
        if not v or len(v) < 3:
            return None
        return v[1] / v[2] if v[2] >= 3 else None

    def smile_pr(self, smile_cat: str):
        """Phase 11: SMILE区分別複勝率"""
        v = getattr(self, 'smile', {}).get(smile_cat)
        if not v or len(v) < 3:
            return None
        return v[1] / v[2] if v[2] >= 3 else None

    def cond_pr(self, condition: str):
        """Phase 11: 馬場状態別複勝率"""
        v = getattr(self, 'cond', {}).get(condition)
        if not v or len(v) < 3:
            return None
        return v[1] / v[2] if v[2] >= 3 else None

    def pace_pr(self, pace_grp: str):
        """Phase 12: ペース別複勝率"""
        v = getattr(self, 'pace', {}).get(pace_grp)
        if not v or len(v) < 3:
            return None
        return v[1] / v[2] if v[2] >= 3 else None

    def style_pr(self, style_grp: str):
        """Phase 12: 脚質別複勝率"""
        v = getattr(self, 'style', {}).get(style_grp)
        if not v or len(v) < 3:
            return None
        return v[1] / v[2] if v[2] >= 3 else None

    def gate_pr(self, gate_grp: str):
        """Phase 12: 枠番帯別複勝率"""
        v = getattr(self, 'gate', {}).get(gate_grp)
        if not v or len(v) < 3:
            return None
        return v[1] / v[2] if v[2] >= 3 else None

    def horse_combo_pr(self, horse_id: str):
        """Phase 12: 騎乗馬別複勝率"""
        v = getattr(self, 'horse_combo', {}).get(horse_id)
        if not v or len(v) < 3:
            return None
        return v[1] / v[2] if v[2] >= 3 else None

    def rate_recent_2y(self, date_str: str):
        """Phase 11: 過去2年間の複勝率"""
        try:
            cutoff = (datetime.strptime(date_str, "%Y-%m-%d")
                      - timedelta(days=730)).strftime("%Y-%m-%d")
        except Exception:
            return None
        w, r = 0, 0
        for d, _iw, ip in self.recent:
            if cutoff <= d < date_str:
                r += 1
                w += int(ip)
        return w / r if r >= 3 else None


class _HorseStats:
    """馬のローリング統計"""

    __slots__ = ("wins", "runs", "places", "finishes", "last_date", "last_finish",
                 "venue_runs", "run_details", "last_jockey_id")

    def __init__(self):
        self.wins = 0
        self.runs = 0
        self.places = 0
        self.finishes: List[int] = []
        self.last_date: Optional[str] = None
        self.last_finish: Optional[int] = None
        self.venue_runs: List[Tuple[str, str, int]] = []  # (venue, surface, finish_pos)
        # Tier1特徴量用: (date, finish_pos, field_count, pos4c, margin_behind, condition, jockey_id)
        self.run_details: List[Tuple] = []
        self.last_jockey_id: str = ""

    def update(self, date_str: str, finish_pos: int, is_win: bool, is_place: bool,
               venue: str = "", surface: str = "",
               field_count: int = 0, pos4c: Optional[int] = None,
               margin_behind: float = 0.0, condition: str = "良",
               jockey_id: str = "",
               last_3f_sec: Optional[float] = None, run_distance: int = 0,
               win_odds: Optional[float] = None, grade_code: int = 1,
               finish_time_sec: Optional[float] = None,
               pos1c: Optional[int] = None, pos3c: Optional[int] = None,
               speed_index: Optional[float] = None,
               race_pace_norm: Optional[float] = None,
               gate_no: Optional[int] = None):
        self.wins += int(is_win)
        self.runs += 1
        self.places += int(is_place)
        self.finishes.append(finish_pos)
        self.last_date = date_str
        self.last_finish = finish_pos
        if venue and surface and finish_pos:
            self.venue_runs.append((venue, surface, finish_pos))
            if len(self.venue_runs) > 30:
                self.venue_runs.pop(0)
        # Phase 11: 19-tuple (旧16-tuple + venue, surface, gate_no)
        # idx: 0=date, 1=finish_pos, 2=field_count, 3=pos4c, 4=margin_behind,
        #      5=condition, 6=jockey_id, 7=last_3f_sec, 8=run_distance,
        #      9=win_odds, 10=grade_code, 11=finish_time_sec, 12=pos1c, 13=pos3c,
        #      14=speed_index, 15=race_pace_norm, 16=venue, 17=surface, 18=gate_no
        self.run_details.append((date_str, finish_pos, field_count, pos4c,
                                  margin_behind, condition, jockey_id,
                                  last_3f_sec, run_distance,
                                  win_odds, grade_code, finish_time_sec,
                                  pos1c, pos3c, speed_index, race_pace_norm,
                                  venue, surface, gate_no))
        if len(self.run_details) > 20:
            self.run_details.pop(0)
        if jockey_id:
            self.last_jockey_id = jockey_id

    @property
    def win_rate(self):
        return self.wins / self.runs if self.runs >= 2 else None

    @property
    def place_rate(self):
        return self.places / self.runs if self.runs >= 2 else None

    @property
    def avg_finish(self):
        return sum(self.finishes) / len(self.finishes) if self.finishes else None

    def days_since(self, current_date: str):
        if not self.last_date:
            return None
        try:
            return (datetime.strptime(current_date, "%Y-%m-%d")
                    - datetime.strptime(self.last_date, "%Y-%m-%d")).days
        except Exception:
            return None

    def get_condition_pr_features(self, date_str: str, venue: str, surface: str,
                                   distance: int, smile_cat: str,
                                   gate_no: Optional[int], jockey_id: str,
                                   condition: str = "") -> dict:
        """Phase 11+12: 馬の条件別複勝率を一括計算。run_details から date_str 前 & 2年以内で集計。"""
        result = {
            "horse_pr_2y": None, "horse_venue_pr": None,
            "horse_dist_pr": None, "horse_smile_pr": None,
            "horse_style_pr": None, "horse_gate_pr": None,
            "horse_jockey_pr": None,
            "horse_cond_pr": None,
        }
        if not self.run_details:
            return result
        try:
            cutoff_2y = (datetime.strptime(date_str, "%Y-%m-%d")
                         - timedelta(days=730)).strftime("%Y-%m-%d")
        except Exception:
            cutoff_2y = ""

        # 枠番帯: 1-2=g12, 3-4=g34, 5-6=g56, 7+=g78
        def _gate_grp(g):
            if g is None:
                return None
            if g <= 2: return "g12"
            if g <= 4: return "g34"
            if g <= 6: return "g56"
            return "g78"

        # 脚質帯: pos4c/field_count から
        def _style_grp(d):
            p4 = d[3]   # pos4c
            fc = d[2]   # field_count
            if p4 is None or not fc or fc <= 1:
                return None
            rel = p4 / fc
            if rel <= 0.30: return "front"
            if rel <= 0.60: return "middle"
            return "rear"

        # SMILE区分の推定（run_detailsの距離から）
        def _smile_from_dist(dist):
            if not dist:
                return None
            if dist <= 1000: return "SS"
            if dist <= 1400: return "S"
            if dist <= 1800: return "M"
            if dist <= 2200: return "I"
            if dist <= 2600: return "L"
            return "E"

        target_gate_grp = _gate_grp(gate_no)
        # 当馬の直近脚質帯を推定（最新走の脚質で照合先を決定）
        _last_style = None
        for d in reversed(self.run_details):
            if d[0] < date_str:
                _last_style = _style_grp(d)
                if _last_style:
                    break

        # 集計カウンター
        n_2y = p_2y = 0       # 過去2年全体
        n_v = p_v = 0         # 当競馬場
        n_d = p_d = 0         # 当距離帯(±200m)
        n_sm = p_sm = 0       # 当SMILE区分
        n_st = p_st = 0       # 脚質帯
        n_g = p_g = 0         # 枠番帯
        n_j = p_j = 0         # 当騎手
        n_cond = p_cond = 0   # 当馬場状態

        for d in self.run_details:
            d_date = d[0]
            if d_date >= date_str:
                continue
            if cutoff_2y and d_date < cutoff_2y:
                continue

            fp = d[1]
            if fp is None:
                continue
            is_p = 1 if fp <= 3 else 0

            n_2y += 1; p_2y += is_p

            # 競馬場 (idx 16)
            d_venue = d[16] if len(d) > 16 else None
            if d_venue and d_venue == venue:
                n_v += 1; p_v += is_p

            # 距離 ±200m (idx 8)
            d_dist = d[8]
            if d_dist and distance and abs(d_dist - distance) <= 200:
                n_d += 1; p_d += is_p

            # SMILE区分 (idx 8)
            d_smile = _smile_from_dist(d_dist)
            if d_smile and d_smile == smile_cat:
                n_sm += 1; p_sm += is_p

            # 脚質帯
            d_style = _style_grp(d)
            if d_style and d_style == _last_style:
                n_st += 1; p_st += is_p

            # 枠番帯 (idx 18)
            d_gate = d[18] if len(d) > 18 else None
            d_gate_grp = _gate_grp(d_gate)
            if d_gate_grp and d_gate_grp == target_gate_grp:
                n_g += 1; p_g += is_p

            # 騎手 (idx 6)
            d_jid = d[6]
            if d_jid and d_jid == jockey_id:
                n_j += 1; p_j += is_p

            # Phase 12: 馬場状態 (idx 5)
            d_cond = d[5] if len(d) > 5 else None
            if d_cond and condition and d_cond == condition:
                n_cond += 1; p_cond += is_p

        # min_runs=2 で結果設定
        if n_2y >= 2: result["horse_pr_2y"] = p_2y / n_2y
        if n_v >= 2: result["horse_venue_pr"] = p_v / n_v
        if n_d >= 2: result["horse_dist_pr"] = p_d / n_d
        if n_sm >= 2: result["horse_smile_pr"] = p_sm / n_sm
        if n_st >= 2: result["horse_style_pr"] = p_st / n_st
        if n_g >= 2: result["horse_gate_pr"] = p_g / n_g
        if n_j >= 2: result["horse_jockey_pr"] = p_j / n_j
        if n_cond >= 2: result["horse_cond_pr"] = p_cond / n_cond
        return result

    def get_speed_index_windows(self, date_str: str) -> dict:
        """Phase 11: タイム指数のマルチウィンドウ特徴量"""
        result = {
            "speed_index_avg_1y": None, "speed_index_best_1y": None,
            "speed_index_avg_6m": None, "speed_index_trend": None,
        }
        if not self.run_details:
            return result
        try:
            dt_now = datetime.strptime(date_str, "%Y-%m-%d")
            cutoff_1y = (dt_now - timedelta(days=365)).strftime("%Y-%m-%d")
            cutoff_6m = (dt_now - timedelta(days=183)).strftime("%Y-%m-%d")
        except Exception:
            return result

        si_1y = []  # (date, speed_index) 過去1年
        si_6m = []  # 過去半年
        for d in self.run_details:
            d_date = d[0]
            if d_date >= date_str:
                continue
            si_val = d[14] if len(d) > 14 else None
            if si_val is None:
                continue
            if d_date >= cutoff_1y:
                si_1y.append((d_date, float(si_val)))
            if d_date >= cutoff_6m:
                si_6m.append((d_date, float(si_val)))

        if si_1y:
            vals = [v for _, v in si_1y]
            result["speed_index_avg_1y"] = sum(vals) / len(vals)
            result["speed_index_best_1y"] = max(vals)
            # トレンド: 線形傾き（正=改善）
            if len(si_1y) >= 3:
                n = len(si_1y)
                x_mean = (n - 1) / 2.0
                y_mean = sum(vals) / n
                num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(vals))
                den = sum((i - x_mean) ** 2 for i in range(n))
                result["speed_index_trend"] = num / den if den > 0 else 0.0

        if si_6m:
            vals_6m = [v for _, v in si_6m]
            result["speed_index_avg_6m"] = sum(vals_6m) / len(vals_6m)

        return result


class _ComboStats:
    """騎手×調教師コンビ統計"""

    __slots__ = ("wins", "runs")

    def __init__(self):
        self.wins = 0
        self.runs = 0

    def update(self, is_win: bool):
        self.wins += int(is_win)
        self.runs += 1

    @property
    def win_rate(self):
        return self.wins / self.runs if self.runs >= 2 else None


def build_venue_time_baselines(races: list) -> dict:
    """
    全レースから会場×距離×馬場×馬場状態 別の走破タイム基準値を事前構築（2パス方式）。
    タイム指数の精度向上のため、学習開始前に全データから基準値を算出する。

    Returns:
        baselines dict:
            (venue, distance, surface, condition) → [sum_times, count]   走破タイム
            "_chunkan" key内: (venue, distance, surface) → [sum_chunkan, count] 道中タイム
    """
    baselines: Dict[Tuple, list] = {}
    chunkan: Dict[Tuple, list] = {}
    for race in races:
        venue = race.get("venue", "")
        surface = race.get("surface", "")
        distance = race.get("distance", 0)
        condition = race.get("condition", "良")
        if not (venue and surface and distance):
            continue
        vtb_key = (venue, distance, surface, condition)
        ck_key = (venue, distance, surface)
        for h in race.get("horses", []):
            if h.get("finish_pos") is None:
                continue
            ft = h.get("finish_time_sec")
            if isinstance(ft, (int, float)) and ft > 0:
                if vtb_key not in baselines:
                    baselines[vtb_key] = [0.0, 0]
                baselines[vtb_key][0] += float(ft)
                baselines[vtb_key][1] += 1
                # 道中タイム
                l3f = h.get("last_3f_sec")
                if isinstance(l3f, (int, float)) and l3f > 0 and float(ft) > float(l3f):
                    ck = float(ft) - float(l3f)
                    if ck_key not in chunkan:
                        chunkan[ck_key] = [0.0, 0]
                    chunkan[ck_key][0] += ck
                    chunkan[ck_key][1] += 1
    baselines["_chunkan"] = chunkan  # type: ignore[assignment]
    logger.info("venue_time_baselines構築完了: %d 走破タイムキー, %d 道中タイムキー",
                len(baselines) - 1, len(chunkan))
    return baselines


class RollingStatsTracker:
    """全エンティティのローリング統計を管理"""

    def __init__(self, prebuilt_time_baselines: Optional[Dict] = None):
        self.jockeys: Dict[str, _EntityStats] = {}
        self.trainers: Dict[str, _EntityStats] = {}
        self.horses: Dict[str, _HorseStats] = {}
        self.combos: Dict[str, _ComboStats] = {}
        # コース分析特徴量 (Task #26)
        # key -> [wins, places, runs]
        self._gate_venue: Dict[Tuple, List[int]] = {}   # (gate_group, venue, dist_cat)
        self._style_surface: Dict[Tuple, List[int]] = {}  # (style_group, surface)
        self._gate_style: Dict[Tuple, List[int]] = {}   # (gate_group, style_group)
        # Layer 2: 時系列履歴（集計統計の日付フィルタ用）
        # horse_id -> [(date, finish_pos, field_count, jockey_id), ...]  全レース履歴
        self._horse_history: Dict[str, List[Tuple[str, int, int, str]]] = {}
        # ML-1b: 騎手×調教師コンビ 直近30日成績
        # key (jockey_id_trainer_id) -> {"wins": int, "places": int, "runs": int,
        #                                "records": [(date, is_win, is_place), ...]}
        self.jt_combo_30d: Dict[str, Dict[str, Any]] = {}
        # ML-1c: 馬体重履歴 horse_id -> [weight, ...]  (最新10件)
        self.horse_weight_history: Dict[str, List[float]] = {}
        # Batch2 ①: 会場×距離×馬場×馬場状態 別 走破タイム基準値
        # key: (venue, distance, surface, condition) -> [sum_times, count]
        # prebuilt_time_baselines が渡された場合はそれを初期値として使用（2パス方式）
        self.venue_time_baselines: Dict[Tuple, List] = (
            {k: list(v) for k, v in prebuilt_time_baselines.items() if k != "_chunkan"}
            if prebuilt_time_baselines else {}
        )
        # Batch4 ⑤: 道中タイム（finish_time - last_3f）基準値
        # key: (venue, distance, surface) -> [sum_chunkan, count]
        self.chunkan_baselines: Dict[Tuple, List] = (
            {k: list(v) for k, v in prebuilt_time_baselines.get("_chunkan", {}).items()}
            if prebuilt_time_baselines else {}
        )

    def update_race(self, race: dict):
        date_str = race.get("date", "")
        venue = race.get("venue", "")
        surface = race.get("surface", "")
        distance = race.get("distance", 0)
        dc = _dist_category(distance)

        # Batch2 ①: タイム指数 - 現在の基準タイム取得（ループ前に確定, リーク回避）
        _vtb_key = (venue, distance, surface, race.get("condition", "良"))
        _vtb = self.venue_time_baselines.get(_vtb_key)
        _baseline_time: Optional[float] = None
        # ばんえい（帯広200m）はサンプル数が少ないため閾値を緩和
        from data.masters.venue_master import is_banei as _is_banei_vtb
        _is_banei_race = _is_banei_vtb(str(race.get("venue_code", "")))
        _vtb_min = 3 if _is_banei_race else 20
        _vtb_broad_min = 1 if _is_banei_race else 10
        if _vtb and _vtb[1] >= _vtb_min:
            _baseline_time = _vtb[0] / _vtb[1]
        else:
            # サンプル不足: 馬場状態を無視した広いキーで補完
            _vtb_broad = self.venue_time_baselines.get((venue, distance, surface, "良"))
            if _vtb_broad and _vtb_broad[1] >= _vtb_broad_min:
                _baseline_time = _vtb_broad[0] / _vtb_broad[1]
        # 全馬のspeed_indexを事前計算（ベースライン固定で公平な比較）
        _horse_speed_idx: Dict[str, Optional[float]] = {}
        for _hh in race.get("horses", []):
            _hid_pre = _hh.get("horse_id", "")
            _ft_pre = _hh.get("finish_time_sec")
            _si: Optional[float] = None
            if (_baseline_time is not None and isinstance(_ft_pre, (int, float))
                    and _ft_pre > 0):
                _si = (_baseline_time - float(_ft_pre)) / _baseline_time * 1000.0
            _horse_speed_idx[_hid_pre] = _si

        # Batch4 ⑤: 道中タイムペース計算
        # 逃げた馬（最終コーナー1番手）の道中タイム = finish_time - last_3f を取得
        # これをベース基準値と比較してペース指標（正=ハイペース）を算出
        _race_pace_norm: Optional[float] = None
        _ck_key = (venue, distance, surface)
        _ck_baseline_val = self.chunkan_baselines.get(_ck_key)
        _ck_baseline: Optional[float] = None
        if _ck_baseline_val and _ck_baseline_val[1] >= 15:
            _ck_baseline = _ck_baseline_val[0] / _ck_baseline_val[1]
        if _ck_baseline is not None:
            # 最終コーナー1番手の馬を探す
            _leader_chunkan: Optional[float] = None
            _best_pos4c = 9999
            for _hh in race.get("horses", []):
                _hh_pc = _hh.get("positions_corners") or []
                if isinstance(_hh_pc, list) and _hh_pc:
                    _hh_p4 = _hh_pc[-1]
                    if isinstance(_hh_p4, (int, float)) and _hh_p4 < _best_pos4c:
                        _ft2 = _hh.get("finish_time_sec")
                        _l3f2 = _hh.get("last_3f_sec")
                        if (isinstance(_ft2, (int, float)) and _ft2 > 0
                                and isinstance(_l3f2, (int, float)) and _l3f2 > 0
                                and _ft2 > _l3f2):
                            _best_pos4c = int(_hh_p4)
                            _leader_chunkan = float(_ft2) - float(_l3f2)
            if _leader_chunkan is not None:
                # 正 = リーダーの道中タイムが基準より速い = ハイペース
                _race_pace_norm = (_ck_baseline - _leader_chunkan) / _ck_baseline * 1000.0
                # 外れ値クリップ（データ品質問題による極端値を除去）
                _race_pace_norm = max(-200.0, min(200.0, _race_pace_norm))

        # レースレベルのグレードコード（ループ前に確定）
        _race_grade_code = GRADE_MAP.get(race.get("grade", "その他"), 1)
        # Phase 11: SMILE区分とconditionをループ前に確定
        _smile_cat = _smile_key_ml(distance) if distance else ""
        _condition = race.get("condition", "良")

        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            is_win = fp == 1
            is_place = fp <= 3
            jid = h.get("jockey_id", "")
            tid = h.get("trainer_id", "")
            hid = h.get("horse_id", "")

            # Phase 12: ペース/脚質/枠番グループを算出
            _pace_grp = ""
            if _race_pace_norm is not None:
                if _race_pace_norm >= 0.5:
                    _pace_grp = "H"
                elif _race_pace_norm <= -0.5:
                    _pace_grp = "S"
                else:
                    _pace_grp = "M"
            _pc_h = h.get("positions_corners") or []
            _fc_h = h.get("field_count") or len(race.get("horses", []))
            _style_grp = ""
            if isinstance(_pc_h, list) and _pc_h and _fc_h and _fc_h > 1:
                _p4_h = _pc_h[-1] if _pc_h else None
                if isinstance(_p4_h, (int, float)):
                    _rel_h = _p4_h / _fc_h
                    _style_grp = "front" if _rel_h <= 0.30 else ("middle" if _rel_h <= 0.60 else "rear")
            _gate_no_h = h.get("gate_no")
            _gate_grp = ""
            if _gate_no_h:
                if _gate_no_h <= 2: _gate_grp = "g12"
                elif _gate_no_h <= 4: _gate_grp = "g34"
                elif _gate_no_h <= 6: _gate_grp = "g56"
                else: _gate_grp = "g78"

            if jid:
                if jid not in self.jockeys:
                    self.jockeys[jid] = _EntityStats()
                self.jockeys[jid].update(date_str, is_win, is_place, venue, surface, dc,
                                         smile_cat=_smile_cat, condition=_condition,
                                         pace_grp=_pace_grp, style_grp=_style_grp,
                                         gate_grp=_gate_grp, horse_id=hid)

            if tid:
                if tid not in self.trainers:
                    self.trainers[tid] = _EntityStats()
                self.trainers[tid].update(date_str, is_win, is_place, venue, surface, dc,
                                          smile_cat=_smile_cat, condition=_condition,
                                          pace_grp=_pace_grp, style_grp=_style_grp,
                                          gate_grp=_gate_grp, horse_id=hid)

                # Phase 10B: 調教師のクラスレベル推移トラッキング
                _tg = getattr(self, '_trainer_grades', None)
                if _tg is None:
                    self._trainer_grades = _tg = {}
                if tid not in _tg:
                    _tg[tid] = []
                _tg[tid].append(_race_grade_code)
                if len(_tg[tid]) > 20:
                    _tg[tid].pop(0)

                # Phase 10B: 調教師の休養明け馬成績トラッキング
                # 馬の last_date はまだ更新前（前走日付）なので休養判定可能
                _tr = getattr(self, '_trainer_rest', None)
                if _tr is None:
                    self._trainer_rest = _tr = {}
                hs_pre = self.horses.get(hid)
                if hs_pre and hs_pre.last_date:
                    try:
                        _rest_days = (datetime.strptime(date_str, "%Y-%m-%d")
                                      - datetime.strptime(hs_pre.last_date, "%Y-%m-%d")).days
                        if _rest_days >= 60:  # 60日以上=休養明け
                            if tid not in _tr:
                                _tr[tid] = [0, 0]  # [places, runs]
                            _tr[tid][0] += int(is_place)
                            _tr[tid][1] += 1
                    except Exception:
                        pass

            if hid:
                if hid not in self.horses:
                    self.horses[hid] = _HorseStats()
                # Tier1特徴量用: pos4c, margin_behind, condition, jockey_id
                _pc = h.get("positions_corners") or []
                _pos4c = None
                if isinstance(_pc, list):
                    if len(_pc) >= 4:
                        _pos4c = _pc[3]
                    elif _pc:
                        _pos4c = _pc[-1]
                # Batch1/Batch4 ②: pos1c(最初のコーナー), pos3c(最終前コーナー) を抽出
                # コーナー数に応じて柔軟に取得:
                #   len>=2: _pc[0]=1角相当(最初のコーナー), _pc[-2]=3角相当(最終前コーナー)
                #   len==1: 最終コーナーのみ → pos1c/pos3cともNone
                _pos1c = (_pc[0] if isinstance(_pc, list) and len(_pc) >= 2
                           and isinstance(_pc[0], (int, float)) else None)
                _pos3c = (_pc[-2] if isinstance(_pc, list) and len(_pc) >= 2
                           and isinstance(_pc[-2], (int, float)) else None)
                _raw_margin = h.get("margin")
                _margin = _parse_margin(_raw_margin)  # Batch1③: 日本語着差文字列も変換
                _raw_l3f = h.get("last_3f_sec")
                _l3f = float(_raw_l3f) if isinstance(_raw_l3f, (int, float)) else None
                # ② 前走オッズ・クラス変化用 (フィールド名: "odds" または "win_odds")
                _raw_odds = h.get("odds") or h.get("win_odds")
                _win_odds = float(_raw_odds) if isinstance(_raw_odds, (int, float)) else None
                _grade_code = GRADE_MAP.get(race.get("grade", "その他"), 1)
                # ③ スピード指数用 finish_time_sec
                _raw_ft = h.get("finish_time_sec")
                _finish_time = float(_raw_ft) if isinstance(_raw_ft, (int, float)) else None
                self.horses[hid].update(
                    date_str, fp, is_win, is_place,
                    venue=venue,
                    surface=surface,
                    field_count=race.get("field_count", 0),
                    pos4c=_pos4c,
                    margin_behind=_margin,
                    condition=race.get("condition", "良"),
                    jockey_id=jid,
                    last_3f_sec=_l3f,
                    run_distance=distance,
                    win_odds=_win_odds,
                    grade_code=_grade_code,
                    finish_time_sec=_finish_time,
                    pos1c=_pos1c,
                    pos3c=_pos3c,
                    speed_index=_horse_speed_idx.get(hid),
                    race_pace_norm=_race_pace_norm,
                    gate_no=h.get("gate_no") or h.get("horse_no"),
                )

            if jid and tid:
                ck = f"{jid}_{tid}"
                if ck not in self.combos:
                    self.combos[ck] = _ComboStats()
                self.combos[ck].update(is_win)

            # ML-1b: 騎手×調教師コンビ 直近30日成績トラッカー
            if jid and tid and date_str:
                ck30 = f"{jid}_{tid}"
                if not hasattr(self, 'jt_combo_30d'):
                    self.jt_combo_30d = {}
                if ck30 not in self.jt_combo_30d:
                    self.jt_combo_30d[ck30] = {"wins": 0, "places": 0, "runs": 0, "records": []}
                combo30 = self.jt_combo_30d[ck30]
                combo30["records"].append((date_str, int(is_win), int(is_place)))
                combo30["wins"] += int(is_win)
                combo30["places"] += int(is_place)
                combo30["runs"] += 1

            # ML-1c: 馬体重履歴
            if hid:
                if not hasattr(self, 'horse_weight_history'):
                    self.horse_weight_history = {}
                _hw = h.get("horse_weight")
                if _hw is not None:
                    try:
                        _hw_f = float(_hw)
                        if _hw_f > 0:
                            if hid not in self.horse_weight_history:
                                self.horse_weight_history[hid] = []
                            self.horse_weight_history[hid].append(_hw_f)
                            if len(self.horse_weight_history[hid]) > 10:
                                self.horse_weight_history[hid] = self.horse_weight_history[hid][-10:]
                    except (TypeError, ValueError):
                        pass

            # Layer 2: 時系列履歴に追加（後方互換のため hasattr チェック）
            if hid:
                if not hasattr(self, '_horse_history'):
                    self._horse_history = {}
                if hid not in self._horse_history:
                    self._horse_history[hid] = []
                self._horse_history[hid].append(
                    (date_str, fp, race.get("field_count", 0), jid)
                )

            # コース分析集計 (Task #26)
            gate_grp = _gate_group(h.get("gate_no") or 0)
            # 4角相対位置から脚質グループ
            _pc = h.get("positions_corners") or []
            _fc = race.get("field_count", 0)
            _pos4c_raw = None
            if isinstance(_pc, list):
                if len(_pc) >= 4:
                    _pos4c_raw = _pc[3]
                elif _pc:
                    _pos4c_raw = _pc[-1]
            style_grp = None
            if _pos4c_raw is not None and _fc and _fc > 0:
                style_grp = _style_group(_pos4c_raw / _fc)

            def _inc(d, key):
                if key not in d:
                    d[key] = [0, 0, 0]
                d[key][0] += int(is_win)
                d[key][1] += int(is_place)
                d[key][2] += 1

            if gate_grp and venue and dc:
                _inc(self._gate_venue, (gate_grp, venue, dc))
            if style_grp is not None and surface:
                _inc(self._style_surface, (style_grp, surface))
            if gate_grp and style_grp is not None:
                _inc(self._gate_style, (gate_grp, style_grp))

        # Batch2 ①: ループ後に今走の走破タイムで基準値を更新（次走以降が参照可）
        if venue and distance and surface:
            _cond_for_vtb = race.get("condition", "良")
            for _hh in race.get("horses", []):
                _ft_upd = _hh.get("finish_time_sec")
                if (isinstance(_ft_upd, (int, float)) and _ft_upd > 0
                        and _hh.get("finish_pos") is not None):
                    _k = (venue, distance, surface, _cond_for_vtb)
                    if _k not in self.venue_time_baselines:
                        self.venue_time_baselines[_k] = [0.0, 0]
                    self.venue_time_baselines[_k][0] += float(_ft_upd)
                    self.venue_time_baselines[_k][1] += 1
                    # Batch4 ⑤: 道中タイムbaseline更新 (全馬の道中タイムを蓄積)
                    _l3f_upd = _hh.get("last_3f_sec")
                    if (isinstance(_l3f_upd, (int, float)) and _l3f_upd > 0
                            and float(_ft_upd) > float(_l3f_upd)):
                        _ck_upd = float(_ft_upd) - float(_l3f_upd)
                        _ck_k = (venue, distance, surface)
                        if _ck_k not in self.chunkan_baselines:
                            self.chunkan_baselines[_ck_k] = [0.0, 0]
                        self.chunkan_baselines[_ck_k][0] += _ck_upd
                        self.chunkan_baselines[_ck_k][1] += 1

    def _cutoff_90d(self, date_str: str) -> str:
        try:
            return (datetime.strptime(date_str, "%Y-%m-%d")
                    - timedelta(days=90)).strftime("%Y-%m-%d")
        except Exception:
            return ""

    def get_jockey_features(self, jid: str, venue: str,
                            surface: str, dist_cat: str, date_str: str,
                            smile_cat: str = "", condition: str = "",
                            pace_grp: str = "", style_grp: str = "",
                            gate_grp: str = "", horse_id: str = "") -> dict:
        _phase11_keys = [
            "jockey_pr_2y", "jockey_venue_pr", "jockey_dist_pr",
            "jockey_smile_pr", "jockey_cond_pr",
        ]
        _phase12_keys = [
            "jockey_pace_pr", "jockey_style_pr", "jockey_gate_pr", "jockey_horse_pr",
        ]
        s = self.jockeys.get(jid)
        if not s:
            return dict.fromkeys([
                "jockey_win_rate", "jockey_place_rate", "jockey_runs",
                "jockey_win_rate_90d", "jockey_place_rate_90d",
                "jockey_venue_wr", "jockey_surface_wr", "jockey_dist_wr",
                "jockey_surf_dist_wr", "jockey_surf_dist_pr",
                "jockey_sim_venue_wr", "jockey_sim_venue_pr",
                "jockey_sim_venue_dist_wr", "jockey_sim_venue_dist_pr",
            ] + _phase11_keys + _phase12_keys)
        cutoff = self._cutoff_90d(date_str)
        sd = s.surf_dist.get((surface, dist_cat), [0, 0, 0]) if surface and dist_cat else [0, 0, 0]
        sim_wr, sim_pr = s.sim_venue_rate(venue, surface) if venue and surface else (None, None)
        sim_d_wr, sim_d_pr = (s.sim_venue_rate(venue, surface, dist_cat)
                               if venue and surface and dist_cat else (None, None))
        result = {
            "jockey_win_rate": s.win_rate,
            "jockey_place_rate": s.place_rate,
            "jockey_runs": s.runs,
            "jockey_win_rate_90d": s.rate_recent(cutoff) if cutoff else None,
            "jockey_place_rate_90d": s.rate_recent(cutoff, use_place=True) if cutoff else None,
            "jockey_venue_wr": s.venue_wr(venue),
            "jockey_surface_wr": s.surface_wr(surface),
            "jockey_dist_wr": s.dist_cat_wr(dist_cat),
            "jockey_surf_dist_wr": sd[0]/sd[2] if sd[2] >= 3 else None,
            "jockey_surf_dist_pr": sd[1]/sd[2] if sd[2] >= 3 else None,
            "jockey_sim_venue_wr": sim_wr,
            "jockey_sim_venue_pr": sim_pr,
            "jockey_sim_venue_dist_wr": sim_d_wr,
            "jockey_sim_venue_dist_pr": sim_d_pr,
            # Phase 11: 条件別複勝率 + 2年ウィンドウ
            "jockey_pr_2y": s.rate_recent_2y(date_str),
            "jockey_venue_pr": s.venue_pr(venue) if venue else None,
            "jockey_dist_pr": s.dist_cat_pr(dist_cat) if dist_cat else None,
            "jockey_smile_pr": s.smile_pr(smile_cat) if smile_cat else None,
            "jockey_cond_pr": s.cond_pr(condition) if condition else None,
            # Phase 12: ペース/脚質/枠番/騎乗馬別複勝率
            "jockey_pace_pr": s.pace_pr(pace_grp) if pace_grp else None,
            "jockey_style_pr": s.style_pr(style_grp) if style_grp else None,
            "jockey_gate_pr": s.gate_pr(gate_grp) if gate_grp else None,
            "jockey_horse_pr": s.horse_combo_pr(horse_id) if horse_id else None,
        }
        return result

    def get_trainer_features(self, tid: str, venue: str, date_str: str,
                             surface: str = "", dist_cat: str = "",
                             smile_cat: str = "", condition: str = "",
                             pace_grp: str = "", style_grp: str = "",
                             gate_grp: str = "", horse_id: str = "") -> dict:
        _phase11_keys = [
            "trainer_pr_2y", "trainer_venue_pr", "trainer_dist_pr",
            "trainer_smile_pr", "trainer_cond_pr",
        ]
        _phase12_keys = [
            "trainer_pace_pr", "trainer_style_pr", "trainer_gate_pr", "trainer_horse_pr",
        ]
        s = self.trainers.get(tid)
        if not s:
            return dict.fromkeys([
                "trainer_win_rate", "trainer_place_rate", "trainer_runs",
                "trainer_win_rate_90d", "trainer_place_rate_90d", "trainer_venue_wr",
                "trainer_surface_wr", "trainer_dist_wr",
                "trainer_surf_dist_wr", "trainer_surf_dist_pr",
                "trainer_sim_venue_wr", "trainer_sim_venue_pr",
                "trainer_sim_venue_dist_wr", "trainer_sim_venue_dist_pr",
            ] + _phase11_keys + _phase12_keys)
        cutoff = self._cutoff_90d(date_str)
        sd = s.surf_dist.get((surface, dist_cat), [0, 0, 0]) if surface and dist_cat else [0, 0, 0]
        sim_wr, sim_pr = s.sim_venue_rate(venue, surface) if venue and surface else (None, None)
        sim_d_wr, sim_d_pr = (s.sim_venue_rate(venue, surface, dist_cat)
                               if venue and surface and dist_cat else (None, None))
        return {
            "trainer_win_rate": s.win_rate,
            "trainer_place_rate": s.place_rate,
            "trainer_runs": s.runs,
            "trainer_win_rate_90d": s.rate_recent(cutoff) if cutoff else None,
            "trainer_place_rate_90d": s.rate_recent(cutoff, use_place=True) if cutoff else None,
            "trainer_venue_wr": s.venue_wr(venue),
            # ⑥ 調教師の馬場別・距離帯別勝率
            "trainer_surface_wr": s.surface_wr(surface) if surface else None,
            "trainer_dist_wr": s.dist_cat_wr(dist_cat) if dist_cat else None,
            "trainer_surf_dist_wr": sd[0]/sd[2] if sd[2] >= 3 else None,
            "trainer_surf_dist_pr": sd[1]/sd[2] if sd[2] >= 3 else None,
            "trainer_sim_venue_wr": sim_wr,
            "trainer_sim_venue_pr": sim_pr,
            "trainer_sim_venue_dist_wr": sim_d_wr,
            "trainer_sim_venue_dist_pr": sim_d_pr,
            # Phase 11: 条件別複勝率 + 2年ウィンドウ
            "trainer_pr_2y": s.rate_recent_2y(date_str),
            "trainer_venue_pr": s.venue_pr(venue) if venue else None,
            "trainer_dist_pr": s.dist_cat_pr(dist_cat) if dist_cat else None,
            "trainer_smile_pr": s.smile_pr(smile_cat) if smile_cat else None,
            "trainer_cond_pr": s.cond_pr(condition) if condition else None,
            # Phase 12: ペース/脚質/枠番/管理馬別複勝率
            "trainer_pace_pr": s.pace_pr(pace_grp) if pace_grp else None,
            "trainer_style_pr": s.style_pr(style_grp) if style_grp else None,
            "trainer_gate_pr": s.gate_pr(gate_grp) if gate_grp else None,
            "trainer_horse_pr": s.horse_combo_pr(horse_id) if horse_id else None,
        }

    def get_trainer_phase10b_features(self, tid: str) -> dict:
        """Phase 10B: 調教師のクラスレベル推移 + 休養明け馬複勝率"""
        # クラスレベル推移: 直近20走のグレードコードの線形傾き
        _tg = getattr(self, '_trainer_grades', {})
        grades = _tg.get(tid, [])
        class_trend = None
        if len(grades) >= 5:
            # 最小二乗法で傾き算出（正=上昇クラス傾向）
            n = len(grades)
            x_mean = (n - 1) / 2.0
            y_mean = sum(grades) / n
            num = sum((i - x_mean) * (g - y_mean) for i, g in enumerate(grades))
            denom = sum((i - x_mean) ** 2 for i in range(n))
            class_trend = num / denom if denom > 0 else 0.0

        # 休養明け馬の複勝率（60日以上）
        _tr = getattr(self, '_trainer_rest', {})
        rest_data = _tr.get(tid)
        rest_wr = None
        if rest_data and rest_data[1] >= 3:
            rest_wr = rest_data[0] / rest_data[1]

        return {
            "trainer_class_trend": class_trend,
            "trainer_rest_wr": rest_wr,
        }

    def get_horse_features(self, hid: str, date_str: str) -> dict:
        s = self.horses.get(hid)
        if not s:
            return dict.fromkeys([
                "horse_win_rate", "horse_place_rate", "horse_runs",
                "horse_avg_finish", "horse_last_finish", "horse_days_since",
            ])
        return {
            "horse_win_rate": s.win_rate,
            "horse_place_rate": s.place_rate,
            "horse_runs": s.runs,
            "horse_avg_finish": s.avg_finish,
            "horse_last_finish": s.last_finish,
            "horse_days_since": s.days_since(date_str),
        }

    def get_horse_features_as_of(self, hid: str, date_str: str) -> dict:
        """Layer 2: 集計統計も date_str より前のデータのみで計算（リーク完全排除版）

        _horse_history が利用可能な場合は日付フィルタを適用。
        旧 tracker（_horse_history なし）の場合は get_horse_features にフォールバック。
        """
        hist = getattr(self, '_horse_history', {}).get(hid)
        if not hist:
            # フォールバック: 旧 tracker または未記録馬 → 既存メソッドを使用
            return self.get_horse_features(hid, date_str)

        past = [(d, f, fc, jid_) for d, f, fc, jid_ in hist if d < date_str]
        if not past:
            return dict.fromkeys([
                "horse_win_rate", "horse_place_rate", "horse_runs",
                "horse_avg_finish", "horse_last_finish", "horse_days_since",
            ])

        n      = len(past)
        wins   = sum(1 for _, f, _, _ in past if f == 1)
        places = sum(1 for _, f, _, _ in past if f <= 3)
        avg_f  = sum(f for _, f, _, _ in past) / n
        last_d, last_f, _, _ = past[-1]

        try:
            days = (datetime.strptime(date_str, "%Y-%m-%d")
                    - datetime.strptime(last_d, "%Y-%m-%d")).days
        except Exception:
            days = None

        return {
            "horse_win_rate":   wins   / n if n >= 2 else None,
            "horse_place_rate": places / n if n >= 2 else None,
            "horse_runs":       n,
            "horse_avg_finish": avg_f,
            "horse_last_finish": last_f,
            "horse_days_since": days,
        }

    def get_combo_features(self, jid: str, tid: str) -> dict:
        c = self.combos.get(f"{jid}_{tid}")
        if not c:
            return {"jt_combo_wr": None, "jt_combo_runs": 0}
        return {"jt_combo_wr": c.win_rate, "jt_combo_runs": c.runs}

    def get_combo_30d_features(self, jid: str, tid: str, date_str: str,
                                fallback_jt_wr: Optional[float] = None,
                                fallback_trainer_place_rate: Optional[float] = None) -> dict:
        """ML-1b: 騎手×調教師コンビの直近30日成績 (date_str より前のデータのみ)"""
        _empty = {
            "jt_combo_wr_30d": fallback_jt_wr,
            "jt_combo_place_rate_30d": fallback_trainer_place_rate,
        }
        jt_combo_30d = getattr(self, 'jt_combo_30d', {})
        combo = jt_combo_30d.get(f"{jid}_{tid}")
        if not combo:
            return _empty
        try:
            cutoff = (datetime.strptime(date_str, "%Y-%m-%d")
                      - timedelta(days=30)).strftime("%Y-%m-%d")
        except Exception:
            return _empty
        # date_str より前かつ cutoff 以降の記録をカウント
        wins = places = runs = 0
        for rec_date, rw, rp in combo["records"]:
            if cutoff <= rec_date < date_str:
                wins += rw
                places += rp
                runs += 1
        if runs >= 3:
            return {
                "jt_combo_wr_30d": wins / runs,
                "jt_combo_place_rate_30d": places / runs,
            }
        return _empty

    def get_course_strategy_features(
        self,
        gate_no: int,
        horse_style: Optional[float],
        venue: str,
        dist_cat: str,
        surface: str,
    ) -> dict:
        """コース分析特徴量 (gate×venue / style×surface / gate×style)"""
        MIN_N = 20
        gate_grp = _gate_group(gate_no or 0)
        style_grp = _style_group(horse_style) if horse_style is not None else None

        def _rate(d, key):
            v = d.get(key)
            return v[1] / v[2] if v and v[2] >= MIN_N else None

        gv_wr = _rate(self._gate_venue, (gate_grp, venue, dist_cat)) if gate_grp and venue and dist_cat else None
        ss_wr = _rate(self._style_surface, (style_grp, surface)) if style_grp and surface else None
        gs_wr = _rate(self._gate_style, (gate_grp, style_grp)) if gate_grp and style_grp else None

        return {
            "gate_venue_wr": gv_wr,
            "style_surface_wr": ss_wr,
            "gate_style_wr": gs_wr,
        }

    def get_venue_sim_features(self, hid: str, target_venue: str,
                                target_surface: str) -> dict:
        """競馬場類似度重み付き実績を計算"""
        _empty = {
            "venue_sim_place_rate": None, "venue_sim_win_rate": None,
            "venue_sim_avg_finish": None, "venue_sim_runs": 0,
            "venue_sim_n_venues": 0, "same_dir_place_rate": None, "same_dir_runs": 0,
        }
        s = self.horses.get(hid)
        if not s or not s.venue_runs or not target_venue:
            return _empty

        try:
            from data.masters.venue_similarity import get_venue_similarity
            from src.ml.features import _get_venue_profile, _DIRECTION_SCORE
            target_profile = _get_venue_profile(target_venue)
            if not target_profile:
                return _empty
            target_dir = target_profile.direction
        except Exception:
            return _empty

        VENUE_SIM_THRESHOLD = 0.35
        DIRECTION_DISCOUNT = 0.75
        SIMILARITY_POWER = 2.0

        w_place_sum = w_win_sum = w_finish_sum = w_total = 0.0
        contributing_venues: set = set()
        same_dir_top3 = same_dir_n = 0

        for run_venue, run_surface, fp in s.venue_runs:
            if run_surface != target_surface:
                continue
            sim = 1.0 if run_venue == target_venue else get_venue_similarity(target_venue, run_venue)
            if sim < VENUE_SIM_THRESHOLD:
                continue
            try:
                run_profile = _get_venue_profile(run_venue)
                run_dir = run_profile.direction if run_profile else "両"
            except Exception:
                run_dir = "両"
            if target_dir == run_dir or "両" in (target_dir, run_dir):
                dir_factor = 1.0
                same_dir_n += 1
                if fp <= 3:
                    same_dir_top3 += 1
            else:
                dir_factor = DIRECTION_DISCOUNT
            weight = (sim ** SIMILARITY_POWER) * dir_factor
            w_place_sum += weight * (1 if fp <= 3 else 0)
            w_win_sum += weight * (1 if fp == 1 else 0)
            w_finish_sum += weight * fp
            w_total += weight
            contributing_venues.add(run_venue)

        if w_total == 0:
            return _empty
        return {
            "venue_sim_place_rate": w_place_sum / w_total,
            "venue_sim_win_rate": w_win_sum / w_total,
            "venue_sim_avg_finish": w_finish_sum / w_total,
            "venue_sim_runs": int(w_total * 10) / 10,
            "venue_sim_n_venues": len(contributing_venues),
            "same_dir_place_rate": same_dir_top3 / same_dir_n if same_dir_n > 0 else None,
            "same_dir_runs": same_dir_n,
        }

    def get_horse_extra_features(self, hid: str, date_str: str,
                                  current_condition: str,
                                  current_jockey_id: str) -> dict:
        """Tier1追加特徴量 (Step 1) を計算して返す"""
        _empty = {
            "trend_position_slope": None,
            "trend_deviation_slope": None,
            "dev_run1": None,
            "dev_run2": None,
            "chakusa_index_avg3": None,
            "is_jockey_change": 0,
            "kishu_pattern_code": 0.0,
            "is_long_break": 0,
            "horse_running_style": None,
            "horse_condition_match": None,
            # ② クラス変化
            "class_change": 0,
            "prev_grade_code": None,
            # Batch1: ②コーナー別位置変化
            "avg_pos_change_3to4c": None,
            "pos_change_3to4c_last": None,
            "avg_pos_change_1to4c": None,
            "front_hold_rate": None,
            # Batch1: ③着差指数の再設計
            "margin_norm_last": None,
            "margin_norm_avg3": None,
            # Batch2: ①タイム指数
            "speed_index_last": None,
            "speed_index_avg3": None,
            "speed_index_best3": None,
            # Batch4: ⑤道中タイムペース適性
            "place_rate_fast_pace": None,
            "place_rate_slow_pace": None,
            "pace_pref_score": None,
            "pace_count_fast": None,
            "pace_count_slow": None,
            "pace_norm_last": None,
            "pace_norm_avg3": None,
        }
        s = self.horses.get(hid)
        if not s:
            return _empty

        # is_long_break は run_detailsがなくても days_since から計算
        days = s.days_since(date_str)
        is_lb = 1 if (days is not None and days >= 90) else 0
        _empty["is_long_break"] = is_lb

        # run_details を date_str より前のレースのみに絞る（リーク排除）
        details = [d for d in s.run_details if d[0] < date_str]
        if not details:
            return _empty

        # run_details: (date, finish_pos, field_count, pos4c, margin_behind, condition, jockey_id, ...)
        def _norm_pos(fp, fc):
            """着順を0-1スケールに正規化 (1=1着, 0=最下位)"""
            if fp is None or not fc or fc <= 1:
                return None
            return 1.0 - (fp - 1) / (fc - 1)

        # dev_run1, dev_run2 (直近1走・2走の正規化着順)
        d1 = details[-1]
        d2 = details[-2] if len(details) >= 2 else None
        dev_run1 = _norm_pos(d1[1], d1[2])
        dev_run2 = _norm_pos(d2[1], d2[2]) if d2 else None

        # trend_position_slope: 直近3走の正規化着順の傾き
        recent3_pos = [_norm_pos(d[1], d[2]) for d in details[-3:]]
        recent3_pos = [v for v in recent3_pos if v is not None]
        trend_pos = None
        if len(recent3_pos) >= 2:
            trend_pos = (recent3_pos[-1] - recent3_pos[0]) / max(len(recent3_pos) - 1, 1)

        # trend_deviation_slope: 直近3走の(-margin_behind)の傾き（正=着差縮まる=改善）
        margins3 = [-d[4] for d in details[-3:] if d[4] is not None]
        trend_dev = None
        if len(margins3) >= 2:
            trend_dev = (margins3[-1] - margins3[0]) / max(len(margins3) - 1, 1)

        # chakusa_index_avg3: 直近3走のmargin_behind平均（小=良い）
        chakusa_vals = [d[4] for d in details[-3:] if d[4] is not None]
        chakusa_avg3 = sum(chakusa_vals) / len(chakusa_vals) if chakusa_vals else None

        # is_jockey_change: 現在の騎手IDと前走騎手ID（フィルタ済み最新走）の比較
        last_jockey_id_filtered = d1[6] if len(d1) > 6 else ""
        is_change = 0
        if current_jockey_id and last_jockey_id_filtered:
            is_change = int(current_jockey_id != last_jockey_id_filtered)

        # kishu_pattern_code: 乗り替わり品質スコア（簡易版）
        kishu_code = 0.0
        if is_change and last_jockey_id_filtered:
            last_fp = d1[1]
            last_fc = d1[2]
            if last_fp and last_fc:
                rel = last_fp / last_fc
                if last_fp <= 3:
                    kishu_code = 1.0    # 好走後→乗り替わり、良い文脈
                elif rel >= 0.6:
                    kishu_code = -1.5   # 惨敗後→乗り替わり、見切り
                else:
                    kishu_code = -0.5   # 凡走後→やや負

        # horse_running_style: 直近5走の4角位置/頭数の平均（0=逃げ, 1=追込）
        style_vals = [d[3] / d[2] for d in details[-5:]
                      if d[3] is not None and d[2] and d[2] > 0]
        horse_style = sum(style_vals) / len(style_vals) if style_vals else None

        # horse_condition_match: 今日の馬場状態での過去複勝率
        cond_runs = [(d[1], d[2]) for d in details if d[5] == current_condition]
        cond_match = None
        if len(cond_runs) >= 2:
            top3 = sum(1 for fp, _ in cond_runs if fp is not None and fp <= 3)
            cond_match = top3 / len(cond_runs)

        # ② クラス変化 (index 10=grade_code)
        # class_change: 今走グレード vs 前走グレード (current_grade_code は呼び出し側から渡す)
        # ここでは prev_grade_code のみ返し、class_change は _extract_features で設定
        prev_grade_code = d1[10] if len(d1) > 10 else None
        # class_changeを0にしておく（_extract_featuresで上書き）
        class_change = 0

        # Batch1: ②コーナー別位置変化
        # run_details idx: 3=pos4c, 12=pos1c, 13=pos3c, 2=field_count
        # 位置前進量 = (from角の順位 - to角の順位) / field_count
        # プラス = 前に上がった（例: 5番手→3番手 = 5-3=+2, /fc で正規化）
        def _corner_change(d_item, from_idx, to_idx):
            """コーナー間の位置前進量（正=前進=好ましい）"""
            fc = d_item[2]
            if not fc or fc <= 1:
                return None
            pos_from = d_item[from_idx] if len(d_item) > from_idx else None
            pos_to = d_item[to_idx] if len(d_item) > to_idx else None
            if pos_from is None or pos_to is None:
                return None
            return (pos_from - pos_to) / fc

        # 3角→4角位置変化（前進=プラス）
        chg_3to4_vals = [_corner_change(d, 13, 3) for d in details[-5:]]
        chg_3to4_vals = [v for v in chg_3to4_vals if v is not None]
        avg_pos_change_3to4c = (sum(chg_3to4_vals) / len(chg_3to4_vals)
                                 if chg_3to4_vals else None)
        pos_change_3to4c_last = _corner_change(d1, 13, 3)

        # 1角→4角総移動量（前進=プラス）
        chg_1to4_vals = [_corner_change(d, 12, 3) for d in details[-5:]]
        chg_1to4_vals = [v for v in chg_1to4_vals if v is not None]
        avg_pos_change_1to4c = (sum(chg_1to4_vals) / len(chg_1to4_vals)
                                 if chg_1to4_vals else None)

        # front_hold_rate: 1角で上位30%にいた時に4角でも上位30%を維持した率
        fh_total = 0
        fh_success = 0
        for d_item in details[-10:]:
            fc_item = d_item[2]
            if not fc_item or fc_item <= 1:
                continue
            p1 = d_item[12] if len(d_item) > 12 else None
            p4 = d_item[3]
            if p1 is None or p4 is None:
                continue
            if p1 / fc_item <= 0.30:       # 1角で上位30%
                fh_total += 1
                if p4 / fc_item <= 0.30:   # 4角でも上位30%維持
                    fh_success += 1
        front_hold_rate = fh_success / fh_total if fh_total >= 2 else None

        # Batch1: ③着差指数の再設計
        # margin_norm = -(margin_behind) / (field_count - 1): 勝ち=0, 大敗=負大値
        # 符号: 大きい(0に近い)ほど良い、小さい(負)ほど悪い
        def _margin_norm(d_item):
            fc = d_item[2]
            if not fc or fc <= 1:
                return None
            margin = d_item[4]
            if margin is None:
                return None
            return -float(margin) / (fc - 1)

        margin_norm_last = _margin_norm(d1)
        mn_vals = [_margin_norm(d) for d in details[-3:]]
        mn_vals = [v for v in mn_vals if v is not None]
        margin_norm_avg3 = sum(mn_vals) / len(mn_vals) if mn_vals else None

        # Batch2: ①タイム指数 (run_details idx 14 = speed_index)
        # speed_index: 正 = 基準より速い, 負 = 基準より遅い
        si_last = d1[14] if len(d1) > 14 else None
        si_vals = [d[14] for d in details[-3:] if len(d) > 14 and d[14] is not None]
        si_avg3 = sum(si_vals) / len(si_vals) if si_vals else None
        si_best3 = max(si_vals) if si_vals else None

        # Batch4: ⑤道中タイムペース適性 (run_details idx 15 = race_pace_norm)
        # race_pace_norm: 正 = ハイペース（逃げ馬が基準より速く道中を走った）
        # 分布: mean≈8, std≈34, p25≈-6, p50≈8, p75≈23
        # 閾値: +15 以上 = ハイペース（上位40%相当）, -6以下 = スローペース（下位25%相当）
        PACE_FAST_TH = 15.0   # ハイペース境界
        PACE_SLOW_TH = -6.0   # スローペース境界
        fast_wins, fast_places, fast_runs = 0, 0, 0
        slow_wins, slow_places, slow_runs = 0, 0, 0
        pace_norm_vals = []
        for ditem in details[-15:]:  # 直近15走
            pnorm = ditem[15] if len(ditem) > 15 else None
            if pnorm is None:
                continue
            pace_norm_vals.append(pnorm)
            fp_item = ditem[1]
            is_pl = (fp_item is not None and fp_item <= 3)
            if pnorm >= PACE_FAST_TH:
                fast_runs += 1
                fast_places += int(is_pl)
            elif pnorm <= PACE_SLOW_TH:
                slow_runs += 1
                slow_places += int(is_pl)
        place_rate_fast = fast_places / fast_runs if fast_runs >= 2 else None
        place_rate_slow = slow_places / slow_runs if slow_runs >= 2 else None
        pace_pref = None
        if place_rate_fast is not None and place_rate_slow is not None:
            pace_pref = place_rate_fast - place_rate_slow
        # 連続型ペース特徴量
        _valid_pnorms = [d[15] for d in details if len(d) > 15 and d[15] is not None]
        pace_norm_last = _valid_pnorms[-1] if _valid_pnorms else None
        pace_norm_avg3 = (sum(_valid_pnorms[-3:]) / len(_valid_pnorms[-3:])
                          if len(_valid_pnorms) >= 2 else _valid_pnorms[-1] if _valid_pnorms else None)

        return {
            "trend_position_slope": trend_pos,
            "trend_deviation_slope": trend_dev,
            "dev_run1": dev_run1,
            "dev_run2": dev_run2,
            "chakusa_index_avg3": chakusa_avg3,
            "is_jockey_change": is_change,
            "kishu_pattern_code": kishu_code,
            "is_long_break": is_lb,
            "horse_running_style": horse_style,
            "horse_condition_match": cond_match,
            # ② クラス変化
            "class_change": class_change,
            "prev_grade_code": prev_grade_code,
            # Batch1: ②コーナー別位置変化
            "avg_pos_change_3to4c": avg_pos_change_3to4c,
            "pos_change_3to4c_last": pos_change_3to4c_last,
            "avg_pos_change_1to4c": avg_pos_change_1to4c,
            "front_hold_rate": front_hold_rate,
            # Batch1: ③着差指数の再設計
            "margin_norm_last": margin_norm_last,
            "margin_norm_avg3": margin_norm_avg3,
            # Batch2: ①タイム指数
            "speed_index_last": si_last,
            "speed_index_avg3": si_avg3,
            "speed_index_best3": si_best3,
            # Batch4: ⑤道中タイムペース適性
            "place_rate_fast_pace": place_rate_fast,
            "place_rate_slow_pace": place_rate_slow,
            "pace_pref_score": pace_pref,
            "pace_count_fast": fast_runs if fast_runs > 0 else None,
            "pace_count_slow": slow_runs if slow_runs > 0 else None,
            "pace_norm_last": pace_norm_last,
            "pace_norm_avg3": pace_norm_avg3,
        }

    def get_horse_stacking_features(self, hid: str, date_str: str = "") -> dict:
        """Step 2: スタッキング特徴量 (PositionModel/Last3FModelの訓練時プロキシ)

        run_details tuple index:
          0=date, 1=finish_pos, 2=field_count, 3=pos4c,
          4=margin_behind, 5=condition, 6=jockey_id,
          7=last_3f_sec, 8=run_distance, 9=win_odds, 10=grade_code,
          11=finish_time_sec
        """
        _empty = {"ml_pos_est": None, "ml_l3f_est": None, "speed_sec_per_m_est": None}
        s = self.horses.get(hid)
        if not s or not s.run_details:
            return _empty

        # date_str が指定されている場合は未来データをフィルタ（リーク排除）
        if date_str:
            details = [d for d in s.run_details if d[0] < date_str]
        else:
            details = s.run_details  # 後方互換（date_str なし）

        # ml_pos_est: 直近3走の4角相対位置 (pos4c/field_count) の指数加重平均
        # 0=先頭, 1=最後方 (PositionModelと同じスケール)
        pos_vals = []
        for d in details[-3:]:
            pos4c = d[3]
            fc = d[2]
            if pos4c is not None and fc and fc > 0:
                pos_vals.append(pos4c / fc)

        ml_pos = None
        if pos_vals:
            # 指数加重 (新しい走を重視): 1走=1.0, 2走=[0.35, 0.65], 3走=[0.20, 0.35, 0.45]
            _w3 = [0.20, 0.35, 0.45]
            weights = _w3[-len(pos_vals):]
            w_sum = sum(weights)
            ml_pos = sum(v * w for v, w in zip(pos_vals, weights)) / w_sum

        # ml_l3f_est: 直近5走の距離調整済みlast_3f_secの加重平均
        # 1800m基準に正規化 (距離が長いほど上がりは遅い)
        # len(d) > 7 チェック: 旧7-tupleとの後方互換
        l3f_vals = []
        for d in details[-5:]:
            l3f = d[7] if len(d) > 7 else None
            dist = d[8] if len(d) > 8 else 0
            if l3f is not None and dist and dist > 0:
                adj = l3f * (1800.0 / dist) ** 0.5
                l3f_vals.append(adj)

        ml_l3f = None
        if l3f_vals:
            _w5 = [0.10, 0.15, 0.20, 0.25, 0.30]
            weights = _w5[-len(l3f_vals):]
            w_sum = sum(weights)
            ml_l3f = sum(v * w for v, w in zip(l3f_vals, weights)) / w_sum

        # ③ スピード指数: 直近5走の (走破タイム/距離) の加重平均 (小=速い)
        # 距離が長いほど sec/m は大きい (1200mで0.059, 2400mで0.064程度)
        # 異なる距離の比較のため √distance で補正 (次元的に sec/√m に変換)
        spd_vals = []
        for d in details[-5:]:
            ft   = d[11] if len(d) > 11 else None
            dist = d[8] if len(d) > 8 else 0
            if ft is not None and dist and dist > 0:
                # √distance で正規化 (短距離・長距離の単位を揃える)
                spd_vals.append(ft / (dist ** 0.5))

        ml_spd = None
        if spd_vals:
            _w5 = [0.10, 0.15, 0.20, 0.25, 0.30]
            weights = _w5[-len(spd_vals):]
            w_sum = sum(weights)
            ml_spd = sum(v * w for v, w in zip(spd_vals, weights)) / w_sum

        return {"ml_pos_est": ml_pos, "ml_l3f_est": ml_l3f, "speed_sec_per_m_est": ml_spd}


# ============================================================
# 血統ローリング統計 (改善C)
# ============================================================


class RollingSireTracker:
    """父馬・母父馬の産駒ローリング勝率を追跡（surface×SMILE分解付き）"""

    def __init__(self):
        self._sire: Dict[str, list] = {}  # {sire_id: [wins, places, runs]}
        self._bms: Dict[str, list] = {}
        # surface 別: {(sire_id, surface): [wins, places, runs]}
        self._sire_surf: Dict[Tuple, list] = {}
        self._bms_surf: Dict[Tuple, list] = {}
        # SMILE+SS距離区分別: {(sire_id, smile_cat): [wins, places, runs]}
        self._sire_smile: Dict[Tuple, list] = {}
        self._bms_smile: Dict[Tuple, list] = {}
        # Batch3 ④: ニック理論 sire×bms 組み合わせ複勝率
        self._sire_x_bms: Dict[Tuple, list] = {}  # {(sire_id, bms_id): [wins, places, runs]}
        # 類似場加重用: eid → {条件tuple: [w,p,r]} (nested dict でO(1)lookup)
        self._sire_surf_dist: Dict[str, Dict[Tuple, list]] = {}       # sire_id→{(surf,dc)}
        self._sire_venue_surf: Dict[str, Dict[Tuple, list]] = {}      # sire_id→{(venue,surf)}
        self._sire_venue_surf_dist: Dict[str, Dict[Tuple, list]] = {} # sire_id→{(venue,surf,dc)}
        self._bms_surf_dist: Dict[str, Dict[Tuple, list]] = {}
        self._bms_venue_surf: Dict[str, Dict[Tuple, list]] = {}
        self._bms_venue_surf_dist: Dict[str, Dict[Tuple, list]] = {}
        # Phase 10B: 父馬の直近産駒成績トラッキング（直近30件の is_place リスト）
        self._sire_recent: Dict[str, List[int]] = {}  # sire_id → [is_place, ...]
        # Phase 11: condition別・venue別（非加重）
        self._sire_cond: Dict[Tuple, list] = {}   # {(sire_id, condition): [w,p,r]}
        self._bms_cond: Dict[Tuple, list] = {}
        self._sire_venue: Dict[Tuple, list] = {}  # {(sire_id, venue): [w,p,r]}
        self._bms_venue: Dict[Tuple, list] = {}

    def _update(self, table: dict, eid: str, is_win: bool, is_place: bool):
        if not eid:
            return
        if eid not in table:
            table[eid] = [0, 0, 0]
        s = table[eid]
        s[0] += int(is_win)
        s[1] += int(is_place)
        s[2] += 1

    def _update_key(self, table: dict, key: tuple, is_win: bool, is_place: bool):
        if not key[0]:
            return
        if key not in table:
            table[key] = [0, 0, 0]
        s = table[key]
        s[0] += int(is_win)
        s[1] += int(is_place)
        s[2] += 1

    def _nested_update(self, outer: dict, eid: str, cond_key: tuple,
                       is_win: bool, is_place: bool):
        """eid → {cond_key: [w,p,r]} nested dict を更新"""
        if not eid:
            return
        inner = outer.get(eid)
        if inner is None:
            outer[eid] = inner = {}
        s = inner.get(cond_key)
        if s is None:
            inner[cond_key] = s = [0, 0, 0]
        s[0] += int(is_win); s[1] += int(is_place); s[2] += 1

    def _rate(self, table: dict, eid, idx: int, min_runs: int = 5) -> Optional[float]:
        s = table.get(eid)
        if not s or s[2] < min_runs:
            return None
        return s[idx] / s[2]

    def update_race(self, race: dict, sire_map: dict):
        surface = race.get("surface", "")
        distance = race.get("distance", 0)
        smile_cat = _smile_key_ml(distance) if distance else ""
        venue = race.get("venue", "")
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sire_id, bms_id = sire_map.get(hid, ("", ""))
            is_win, is_place = fp == 1, fp <= 3
            self._update(self._sire, sire_id, is_win, is_place)
            self._update(self._bms, bms_id, is_win, is_place)
            # surface 別
            if surface:
                self._update_key(self._sire_surf, (sire_id, surface), is_win, is_place)
                self._update_key(self._bms_surf, (bms_id, surface), is_win, is_place)
            # SMILE 別
            if smile_cat:
                self._update_key(self._sire_smile, (sire_id, smile_cat), is_win, is_place)
                self._update_key(self._bms_smile, (bms_id, smile_cat), is_win, is_place)
            # Batch3 ④: ニック (sire × bms 組み合わせ)
            if sire_id and bms_id:
                self._update_key(self._sire_x_bms, (sire_id, bms_id), is_win, is_place)
            # 類似場加重用 (nested dict: eid → {condition_key: [w,p,r]})
            if surface and smile_cat:
                self._nested_update(self._sire_surf_dist, sire_id, (surface, smile_cat), is_win, is_place)
                self._nested_update(self._bms_surf_dist,  bms_id,  (surface, smile_cat), is_win, is_place)
            if venue and surface:
                self._nested_update(self._sire_venue_surf, sire_id, (venue, surface), is_win, is_place)
                self._nested_update(self._bms_venue_surf,  bms_id,  (venue, surface), is_win, is_place)
            if venue and surface and smile_cat:
                self._nested_update(self._sire_venue_surf_dist, sire_id, (venue, surface, smile_cat), is_win, is_place)
                self._nested_update(self._bms_venue_surf_dist,  bms_id,  (venue, surface, smile_cat), is_win, is_place)
            # Phase 10B: 父馬の直近産駒成績トラッキング
            _sr = getattr(self, '_sire_recent', None)
            if _sr is None:
                self._sire_recent = _sr = {}
            if sire_id:
                if sire_id not in _sr:
                    _sr[sire_id] = []
                _sr[sire_id].append(int(is_place))
                if len(_sr[sire_id]) > 30:
                    _sr[sire_id].pop(0)
            # Phase 11: condition別・venue別（非加重）
            _cond = race.get("condition", "")
            if _cond:
                _sc = getattr(self, '_sire_cond', None)
                if _sc is None:
                    self._sire_cond = _sc = {}
                _bc = getattr(self, '_bms_cond', None)
                if _bc is None:
                    self._bms_cond = _bc = {}
                self._update_key(_sc, (sire_id, _cond), is_win, is_place)
                self._update_key(_bc, (bms_id, _cond), is_win, is_place)
            if venue:
                _sv = getattr(self, '_sire_venue', None)
                if _sv is None:
                    self._sire_venue = _sv = {}
                _bv = getattr(self, '_bms_venue', None)
                if _bv is None:
                    self._bms_venue = _bv = {}
                self._update_key(_sv, (sire_id, venue), is_win, is_place)
                self._update_key(_bv, (bms_id, venue), is_win, is_place)

            # Phase 12: ペース/脚質/枠番/騎手/調教師 別
            # ペース: レース全体のペースから H/M/S に分類
            _pace_norm = race.get("pace_norm")
            if _pace_norm is not None:
                if _pace_norm >= 0.5:
                    _pg = "H"
                elif _pace_norm <= -0.5:
                    _pg = "S"
                else:
                    _pg = "M"
                _sp = getattr(self, '_sire_pace', None)
                if _sp is None:
                    self._sire_pace = _sp = {}
                _bp = getattr(self, '_bms_pace', None)
                if _bp is None:
                    self._bms_pace = _bp = {}
                self._update_key(_sp, (sire_id, _pg), is_win, is_place)
                self._update_key(_bp, (bms_id, _pg), is_win, is_place)

            # 脚質: 馬の4角位置 / 頭数で前/中/後に分類
            _p4c = h.get("positions_corners")
            _fc = h.get("field_count") or race.get("field_count", 0)
            if _p4c and len(_p4c) >= 4 and _fc and _fc > 1:
                _rel = _p4c[3] / _fc
                _sg = "front" if _rel <= 0.30 else ("middle" if _rel <= 0.60 else "rear")
                _ss = getattr(self, '_sire_style', None)
                if _ss is None:
                    self._sire_style = _ss = {}
                _bs = getattr(self, '_bms_style', None)
                if _bs is None:
                    self._bms_style = _bs = {}
                self._update_key(_ss, (sire_id, _sg), is_win, is_place)
                self._update_key(_bs, (bms_id, _sg), is_win, is_place)

            # 枠番帯
            _gno = h.get("gate_no")
            if _gno:
                if _gno <= 2: _gg = "g12"
                elif _gno <= 4: _gg = "g34"
                elif _gno <= 6: _gg = "g56"
                else: _gg = "g78"
                _sga = getattr(self, '_sire_gate', None)
                if _sga is None:
                    self._sire_gate = _sga = {}
                _bga = getattr(self, '_bms_gate', None)
                if _bga is None:
                    self._bms_gate = _bga = {}
                self._update_key(_sga, (sire_id, _gg), is_win, is_place)
                self._update_key(_bga, (bms_id, _gg), is_win, is_place)

            # 騎手別
            _jid = h.get("jockey_id", "")
            if _jid:
                _sj = getattr(self, '_sire_jockey', None)
                if _sj is None:
                    self._sire_jockey = _sj = {}
                _bj = getattr(self, '_bms_jockey', None)
                if _bj is None:
                    self._bms_jockey = _bj = {}
                self._update_key(_sj, (sire_id, _jid), is_win, is_place)
                self._update_key(_bj, (bms_id, _jid), is_win, is_place)

            # 調教師別
            _tid = h.get("trainer_id", "")
            if _tid:
                _st = getattr(self, '_sire_trainer', None)
                if _st is None:
                    self._sire_trainer = _st = {}
                _bt = getattr(self, '_bms_trainer', None)
                if _bt is None:
                    self._bms_trainer = _bt = {}
                self._update_key(_st, (sire_id, _tid), is_win, is_place)
                self._update_key(_bt, (bms_id, _tid), is_win, is_place)

    def get_features(self, sire_id: str, bms_id: str) -> dict:
        return {
            "sire_win_rate": self._rate(self._sire, sire_id, 0),
            "sire_place_rate": self._rate(self._sire, sire_id, 1),
            "bms_win_rate": self._rate(self._bms, bms_id, 0),
            "bms_place_rate": self._rate(self._bms, bms_id, 1),
            # Batch3 ④: ニック理論
            "sire_x_bms_place_rate": self._rate(
                self._sire_x_bms, (sire_id, bms_id), 1, min_runs=5),
            # 父×母父 win rate（place rateはsire_x_bms_place_rateと同値のため省略）
            "sire_bms_wr": self._rate(self._sire_x_bms, (sire_id, bms_id), 0, min_runs=5),
        }

    def get_phase10b_features(self, sire_id: str, bms_id: str) -> dict:
        """Phase 10B: 信頼度・面適性差・距離適性差・直近トレンド"""
        # 信頼度: log(runs + 1) — サンプル数の対数スケール
        s_stats = self._sire.get(sire_id)
        b_stats = self._bms.get(bms_id)
        sire_cred = math.log(s_stats[2] + 1) if s_stats else 0.0
        bms_cred = math.log(b_stats[2] + 1) if b_stats else 0.0

        # 面適性差: 芝複勝率 - ダート複勝率（正=芝向き）
        def _surf_pref(table, eid):
            t = table.get((eid, "芝"), [0, 0, 0])
            d = table.get((eid, "ダート"), [0, 0, 0])
            t_pr = t[1] / t[2] if t[2] >= 3 else None
            d_pr = d[1] / d[2] if d[2] >= 3 else None
            if t_pr is not None and d_pr is not None:
                return t_pr - d_pr
            return None

        sire_sp = _surf_pref(self._sire_surf, sire_id) if sire_id else None
        bms_sp = _surf_pref(self._bms_surf, bms_id) if bms_id else None

        # 距離適性差: スプリント複勝率 - 長距離複勝率（正=短距離向き）
        sire_dp = None
        if sire_id:
            short_r = [0, 0]  # [places, runs]  ss+s
            long_r = [0, 0]   # [places, runs]  l+e
            for cat in ("ss", "s"):
                v = self._sire_smile.get((sire_id, cat), [0, 0, 0])
                short_r[0] += v[1]; short_r[1] += v[2]
            for cat in ("l", "e"):
                v = self._sire_smile.get((sire_id, cat), [0, 0, 0])
                long_r[0] += v[1]; long_r[1] += v[2]
            if short_r[1] >= 3 and long_r[1] >= 3:
                sire_dp = short_r[0] / short_r[1] - long_r[0] / long_r[1]

        # 直近トレンド: 直近15走複勝率 - それ以前の複勝率
        sire_trend = None
        _sr = getattr(self, '_sire_recent', {})
        recent_list = _sr.get(sire_id, [])
        if len(recent_list) >= 10:
            recent_half = recent_list[-15:]
            older_half = recent_list[:-15] if len(recent_list) > 15 else []
            recent_avg = sum(recent_half) / len(recent_half)
            if older_half:
                older_avg = sum(older_half) / len(older_half)
                sire_trend = recent_avg - older_avg
            elif s_stats and s_stats[2] > len(recent_half):
                # 全体複勝率との差分
                overall_pr = s_stats[1] / s_stats[2]
                sire_trend = recent_avg - overall_pr

        return {
            "sire_credibility": sire_cred,
            "bms_credibility": bms_cred,
            "sire_surface_pref": sire_sp,
            "bms_surface_pref": bms_sp,
            "sire_dist_pref": sire_dp,
            "sire_recent_trend": sire_trend,
        }

    def get_context_features(self, sire_id: str, bms_id: str,
                              surface: str, smile_cat: str) -> dict:
        """surface×SMILE 分解による文脈特徴量"""
        return {
            "sire_surf_wr":  self._rate(self._sire_surf,  (sire_id, surface),    1, min_runs=5),
            "sire_smile_wr": self._rate(self._sire_smile, (sire_id, smile_cat),  1, min_runs=5),
            "bms_surf_wr":   self._rate(self._bms_surf,   (bms_id, surface),     1, min_runs=5),
        }

    def get_phase11_features(self, sire_id: str, bms_id: str,
                              smile_cat: str, condition: str, venue: str) -> dict:
        """Phase 11: 父/母父の条件別複勝率"""
        _sc = getattr(self, '_sire_cond', {})
        _bc = getattr(self, '_bms_cond', {})
        _sv = getattr(self, '_sire_venue', {})
        _bv = getattr(self, '_bms_venue', {})

        def _pr(table, key, min_r=5):
            s = table.get(key)
            if not s or s[2] < min_r:
                return None
            return s[1] / s[2]

        return {
            "sire_smile_pr": self._rate(self._sire_smile, (sire_id, smile_cat), 1, min_runs=5) if smile_cat else None,
            "sire_cond_pr": _pr(_sc, (sire_id, condition)) if condition else None,
            "sire_venue_pr": _pr(_sv, (sire_id, venue)) if venue else None,
            "bms_smile_pr": self._rate(self._bms_smile, (bms_id, smile_cat), 1, min_runs=5) if smile_cat else None,
            "bms_cond_pr": _pr(_bc, (bms_id, condition)) if condition else None,
            "bms_venue_pr": _pr(_bv, (bms_id, venue)) if venue else None,
            "bms_dist_pr": self._rate(self._bms_smile, (bms_id, smile_cat), 1, min_runs=5) if smile_cat else None,
        }

    def get_phase12_features(self, sire_id: str, bms_id: str,
                              pace_grp: str = "", style_grp: str = "",
                              gate_grp: str = "", jockey_id: str = "",
                              trainer_id: str = "") -> dict:
        """Phase 12: 父/母父のペース/脚質/枠番/騎手/調教師別複勝率"""
        def _pr(table, key, min_r=5):
            s = table.get(key) if table else None
            if not s or s[2] < min_r:
                return None
            return s[1] / s[2]

        _sp = getattr(self, '_sire_pace', {})
        _bp = getattr(self, '_bms_pace', {})
        _ss = getattr(self, '_sire_style', {})
        _bs = getattr(self, '_bms_style', {})
        _sg = getattr(self, '_sire_gate', {})
        _bg = getattr(self, '_bms_gate', {})
        _sj = getattr(self, '_sire_jockey', {})
        _bj = getattr(self, '_bms_jockey', {})
        _st = getattr(self, '_sire_trainer', {})
        _bt = getattr(self, '_bms_trainer', {})

        return {
            "sire_pace_pr": _pr(_sp, (sire_id, pace_grp)) if pace_grp else None,
            "sire_style_pr": _pr(_ss, (sire_id, style_grp)) if style_grp else None,
            "sire_gate_pr": _pr(_sg, (sire_id, gate_grp)) if gate_grp else None,
            "sire_jockey_pr": _pr(_sj, (sire_id, jockey_id)) if jockey_id else None,
            "sire_trainer_pr": _pr(_st, (sire_id, trainer_id)) if trainer_id else None,
            "bms_pace_pr": _pr(_bp, (bms_id, pace_grp)) if pace_grp else None,
            "bms_style_pr": _pr(_bs, (bms_id, style_grp)) if style_grp else None,
            "bms_gate_pr": _pr(_bg, (bms_id, gate_grp)) if gate_grp else None,
            "bms_jockey_pr": _pr(_bj, (bms_id, jockey_id)) if jockey_id else None,
            "bms_trainer_pr": _pr(_bt, (bms_id, trainer_id)) if trainer_id else None,
        }

    def _sim_venue_rate_id(self, eid: str,
                           vs_outer: dict, vsd_outer: dict,
                           target_venue: str, surface: str, dist_cat: str = None,
                           min_sim: float = 0.35, sim_power: float = 2.0,
                           min_runs: int = 5) -> Tuple[Optional[float], Optional[float]]:
        """nested dict (eid→{cond:counts}) を使って類似場加重 (win_rate, place_rate) を返す"""
        from data.masters.venue_similarity import get_venue_similarity
        inner = (vsd_outer if dist_cat else vs_outer).get(eid)
        if not inner:
            return None, None
        w_wins = w_places = w_runs = 0.0
        for key, counts in inner.items():
            v, s = key[0], key[1]
            dc = key[2] if len(key) > 2 else None
            if s != surface:
                continue
            if dist_cat and dc != dist_cat:
                continue
            if counts[2] < min_runs:
                continue
            sim = 1.0 if v == target_venue else get_venue_similarity(target_venue, v)
            if sim < min_sim:
                continue
            w = sim ** sim_power
            w_wins += counts[0] * w
            w_places += counts[1] * w
            w_runs += counts[2] * w
        if w_runs == 0:
            return None, None
        return w_wins / w_runs, w_places / w_runs

    def get_sim_venue_features(self, sire_id: str, bms_id: str,
                                target_venue: str, surface: str, smile_cat: str) -> dict:
        """類似場加重 × 条件別 特徴量 (父馬・母父)"""
        # 父馬 surf_dist (非venue, nested dict)
        s_inner = self._sire_surf_dist.get(sire_id, {})
        b_inner = self._bms_surf_dist.get(bms_id, {})
        ssd = s_inner.get((surface, smile_cat), [0, 0, 0])
        bsd = b_inner.get((surface, smile_cat), [0, 0, 0])
        s_sd_wr = ssd[0]/ssd[2] if ssd[2] >= 5 else None
        s_sd_pr = ssd[1]/ssd[2] if ssd[2] >= 5 else None
        b_sd_wr = bsd[0]/bsd[2] if bsd[2] >= 5 else None
        b_sd_pr = bsd[1]/bsd[2] if bsd[2] >= 5 else None
        # 類似場加重 (venue軸, nested dict)
        s_sv_wr, s_sv_pr = self._sim_venue_rate_id(
            sire_id, self._sire_venue_surf, self._sire_venue_surf_dist,
            target_venue, surface)
        s_svd_wr, s_svd_pr = self._sim_venue_rate_id(
            sire_id, self._sire_venue_surf, self._sire_venue_surf_dist,
            target_venue, surface, smile_cat)
        b_sv_wr, b_sv_pr = self._sim_venue_rate_id(
            bms_id, self._bms_venue_surf, self._bms_venue_surf_dist,
            target_venue, surface)
        b_svd_wr, b_svd_pr = self._sim_venue_rate_id(
            bms_id, self._bms_venue_surf, self._bms_venue_surf_dist,
            target_venue, surface, smile_cat)
        return {
            "sire_surf_dist_wr": s_sd_wr, "sire_surf_dist_pr": s_sd_pr,
            "sire_sim_venue_wr": s_sv_wr, "sire_sim_venue_pr": s_sv_pr,
            "sire_sim_venue_dist_wr": s_svd_wr, "sire_sim_venue_dist_pr": s_svd_pr,
            "bms_surf_dist_wr": b_sd_wr, "bms_surf_dist_pr": b_sd_pr,
            "bms_sim_venue_wr": b_sv_wr, "bms_sim_venue_pr": b_sv_pr,
            "bms_sim_venue_dist_wr": b_svd_wr, "bms_sim_venue_dist_pr": b_svd_pr,
        }

    def get_sire_breakdown(self, sire_id: str) -> dict:
        """HTML見える化用: 父馬産駒のsurface×SMILE別複勝率テーブルを返す"""
        breakdown = {"surface": {}, "smile": {}}
        for (sid, surf), v in self._sire_surf.items():
            if sid == sire_id and v[2] >= 5:
                breakdown["surface"][surf] = {
                    "place_rate": round(v[1] / v[2], 3),
                    "win_rate":   round(v[0] / v[2], 3),
                    "runs": v[2],
                }
        for (sid, sm), v in self._sire_smile.items():
            if sid == sire_id and v[2] >= 5:
                breakdown["smile"][sm] = {
                    "place_rate": round(v[1] / v[2], 3),
                    "win_rate":   round(v[0] / v[2], 3),
                    "runs": v[2],
                }
        return breakdown


def _load_horse_sire_map() -> dict:
    """
    horse_id → (sire_id, bms_id) マッピング。
    キャッシュ済みなら pickle から返す、なければ ped HTML を解析して構築する。
    """
    if os.path.exists(SIRE_MAP_PATH):
        try:
            with open(SIRE_MAP_PATH, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass

    import re
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("BeautifulSoup が見つかりません。血統マップをスキップ")
        return {}

    try:
        import lz4.frame as _lz4
    except ImportError:
        _lz4 = None

    cache_dir = os.path.join(_BASE, "data", "cache")
    if not os.path.isdir(cache_dir):
        return {}

    ped_files = [f for f in os.listdir(cache_dir) if "horse_ped_" in f]
    logger.info("血統マップ構築中: %d ped ファイル", len(ped_files))

    result = {}
    for fname in ped_files:
        m = re.search(r"horse_ped_([0-9a-zA-Z]+)_", fname)
        if not m:
            continue
        horse_id = m.group(1)

        path = os.path.join(cache_dir, fname)
        try:
            if fname.endswith(".lz4"):
                if _lz4 is None:
                    continue
                with open(path, "rb") as fh:
                    html = _lz4.decompress(fh.read()).decode("utf-8", errors="replace")
            else:
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    html = fh.read()
        except Exception:
            continue

        try:
            soup = BeautifulSoup(html, "html.parser")
            table = (
                soup.select_one("table.blood_table")
                or soup.select_one("table.db_heredity")
                or soup.select_one("table.pedigree_table")
            )
            if not table:
                continue

            sire_id, bms_id = "", ""
            tds = table.select("td")

            # ヘルパー: tdからhorse IDを抽出
            def _ext_id(td_elem):
                for a in td_elem.select("a[href*='/horse/']"):
                    h = a.get("href", "")
                    if any(x in h for x in ("/horse/ped/", "/horse/sire/", "/horse/mare/")):
                        continue
                    lm = re.search(r"/horse/([0-9a-zA-Z]+)/?", h)
                    if lm and a.get_text(strip=True):
                        return lm.group(1)
                return ""

            # 父: 最初の[SIRE]リンクtd
            for td in tds:
                if td.select_one("a[href*='/horse/sire/']"):
                    sid = _ext_id(td)
                    if sid:
                        sire_id = sid
                        break

            # 父のrowspanを基準値として取得（5代=16, 4代=8 等）
            father_rowspan = int(tds[0].get("rowspan", "1")) if tds else 1

            # 母: 父と同じrowspanの[MARE]td → 次セル=母父(BMS)
            for i, td in enumerate(tds):
                rs = int(td.get("rowspan", "1"))
                if rs != father_rowspan:
                    continue
                if not td.select_one("a[href*='/horse/mare/']"):
                    continue
                # 次セルが母父(BMS)
                if i + 1 < len(tds):
                    bms_id = _ext_id(tds[i + 1])
                break

            # sireが見つからない場合: 1セル目のリンク
            if not sire_id and tds:
                for a in tds[0].select("a[href*='/horse/']"):
                    h = a.get("href", "")
                    if any(x in h for x in ("/horse/ped/", "/horse/sire/", "/horse/mare/")):
                        continue
                    fm = re.search(r"/horse/([0-9a-zA-Z]+)/?", h)
                    if fm and a.get_text(strip=True):
                        sire_id = fm.group(1)
                        break

            if sire_id:
                result[horse_id] = (sire_id, bms_id)
        except Exception:
            continue

    logger.info("血統マップ完成: %d エントリー", len(result))
    os.makedirs(MODEL_DIR, exist_ok=True)
    try:
        with open(SIRE_MAP_PATH, "wb") as f:
            pickle.dump(result, f)
    except Exception:
        pass
    return result


# ============================================================
# 特徴量抽出
# ============================================================


def _extract_features(
    horse: dict,
    race: dict,
    tracker: RollingStatsTracker,
    sire_tracker: Optional["RollingSireTracker"] = None,
) -> dict:
    """1頭分の特徴量辞書を構築"""
    date_str = race.get("date", "")
    venue = race.get("venue", "")
    surface = race.get("surface", "")
    distance = race.get("distance", 0)
    dc = _dist_category(distance)

    try:
        month = int(date_str.split("-")[1])
    except Exception:
        month = 1

    feat = {
        "surface": SURFACE_MAP.get(surface, 0),
        "distance": distance,
        "condition": CONDITION_MAP.get(race.get("condition", "良"), 0),
        "field_count": race.get("field_count", 0),
        "is_jra": int(race.get("is_jra", True)),
        "grade_code": GRADE_MAP.get(race.get("grade", "その他"), 1),
        "venue_code": _safe_int(race.get("venue_code", "0")),
        "month": month,
        "gate_no": horse.get("gate_no"),
        "horse_no": horse.get("horse_no"),
        "sex_code": SEX_MAP.get(horse.get("sex", ""), 0),
        "age": horse.get("age"),
        "weight_kg": horse.get("weight_kg"),
        "horse_weight": horse.get("horse_weight"),
        "weight_change": horse.get("weight_change"),
    }

    jid = horse.get("jockey_id", "")
    tid = horse.get("trainer_id", "")
    hid = horse.get("horse_id", "")

    # コース構造特徴量 (venue_straight_m 等)
    try:
        from src.ml.features import _get_venue_profile
        from data.masters.venue_master import is_banei as _is_banei_check
        _SLOPE_SCORE = {"急坂": 1.0, "軽坂": 0.5, "坂なし": 0.0}
        _CORNER_SCORE = {"大回り": 1.0, "スパイラル": 0.5, "小回り": 0.0}
        _DIRECTION_SCORE = {"右": 0, "左": 1, "両": 2}
        vc_str = str(race.get("venue_code", ""))
        if _is_banei_check(vc_str):
            # ばんえい: 帯広200m直線・坂あり（固定値）
            feat["venue_straight_m"] = 200.0
            feat["venue_slope"] = 1.0
            feat["venue_first_corner"] = 0.0
            feat["venue_corner_type"] = 0.0
            feat["venue_direction"] = 3  # 直線（ばんえい固有コード）
        else:
            vp = _get_venue_profile(venue)
            if vp:
                feat["venue_straight_m"] = vp.avg_straight_m
                feat["venue_slope"] = _SLOPE_SCORE.get(vp.slope_type, 0.0)
                feat["venue_first_corner"] = vp.first_corner_score
                feat["venue_corner_type"] = _CORNER_SCORE.get(vp.corner_type_dominant, 0.5)
                feat["venue_direction"] = _DIRECTION_SCORE.get(vp.direction, 2)
            else:
                feat.update({"venue_straight_m": None, "venue_slope": None,
                             "venue_first_corner": None, "venue_corner_type": None,
                             "venue_direction": None})
    except Exception:
        feat.update({"venue_straight_m": None, "venue_slope": None,
                     "venue_first_corner": None, "venue_corner_type": None,
                     "venue_direction": None})

    _condition = race.get("condition", "良")
    _smile_cat_ef = _smile_key_ml(distance) if distance else ""

    # Phase 12: ペース/脚質/枠番グループを算出（騎手・調教師・血統で共通使用）
    _race_pace_norm = race.get("pace_norm") or race.get("race_pace_norm")
    if _race_pace_norm is not None:
        _pace_grp = "H" if _race_pace_norm >= 0.5 else ("S" if _race_pace_norm <= -0.5 else "M")
    else:
        _pace_grp = ""
    # 脚質: この馬の直近脚質を推定（horse_extra_features の前に必要なので馬統計から取得）
    _hs_for_style = tracker.horses.get(hid)
    _style_grp = ""
    if _hs_for_style and _hs_for_style.run_details:
        for _d in reversed(_hs_for_style.run_details):
            if _d[0] < date_str:
                _p4 = _d[3]
                _fc_s = _d[2]
                if _p4 is not None and _fc_s and _fc_s > 1:
                    _rel = _p4 / _fc_s
                    _style_grp = "front" if _rel <= 0.30 else ("middle" if _rel <= 0.60 else "rear")
                break
    # 枠番帯
    _gate_no = horse.get("gate_no") or 0
    if _gate_no <= 2:
        _gate_grp = "g12"
    elif _gate_no <= 4:
        _gate_grp = "g34"
    elif _gate_no <= 6:
        _gate_grp = "g56"
    elif _gate_no > 0:
        _gate_grp = "g78"
    else:
        _gate_grp = ""

    feat.update(tracker.get_jockey_features(jid, venue, surface, dc, date_str,
                                             smile_cat=_smile_cat_ef, condition=_condition,
                                             pace_grp=_pace_grp, style_grp=_style_grp,
                                             gate_grp=_gate_grp, horse_id=hid))
    feat.update(tracker.get_trainer_features(tid, venue, date_str, surface, dc,
                                              smile_cat=_smile_cat_ef, condition=_condition,
                                              pace_grp=_pace_grp, style_grp=_style_grp,
                                              gate_grp=_gate_grp, horse_id=hid))
    # Layer 2: 日付フィルタ付き集計統計（_horse_history があれば厳密なリーク排除）
    horse_feats = tracker.get_horse_features_as_of(hid, date_str)
    feat.update(horse_feats)
    feat.update(tracker.get_combo_features(jid, tid))

    # 競馬場類似度重み付き実績
    feat.update(tracker.get_venue_sim_features(hid, venue, surface))

    # 血統ローリング (改善C): sire_id/bms_id は horse dict か sire_tracker から
    sire_id = horse.get("sire_id", "")
    bms_id = horse.get("bms_id", "")
    smile_cat = _smile_key_ml(distance) if distance else ""
    if sire_tracker is not None:
        feat.update(sire_tracker.get_features(sire_id, bms_id))
        feat.update(sire_tracker.get_context_features(sire_id, bms_id, surface, smile_cat))
        feat.update(sire_tracker.get_sim_venue_features(sire_id, bms_id, venue, surface, smile_cat))
        # Phase 10B: 血統信頼度・面適性・距離適性・トレンド
        feat.update(sire_tracker.get_phase10b_features(sire_id, bms_id))
        # Phase 11: 条件別複勝率
        feat.update(sire_tracker.get_phase11_features(sire_id, bms_id, smile_cat, _condition, venue))
        # Phase 12: ペース/脚質/枠番/騎手/調教師別複勝率
        feat.update(sire_tracker.get_phase12_features(
            sire_id, bms_id,
            pace_grp=_pace_grp, style_grp=_style_grp, gate_grp=_gate_grp,
            jockey_id=jid, trainer_id=tid))
    else:
        feat.update({"sire_win_rate": None, "sire_place_rate": None,
                     "bms_win_rate": None, "bms_place_rate": None,
                     "sire_surf_wr": None, "sire_smile_wr": None, "bms_surf_wr": None,
                     "sire_x_bms_place_rate": None, "sire_bms_wr": None,
                     "sire_surf_dist_wr": None, "sire_surf_dist_pr": None,
                     "sire_sim_venue_wr": None, "sire_sim_venue_pr": None,
                     "sire_sim_venue_dist_wr": None, "sire_sim_venue_dist_pr": None,
                     "bms_surf_dist_wr": None, "bms_surf_dist_pr": None,
                     "bms_sim_venue_wr": None, "bms_sim_venue_pr": None,
                     "bms_sim_venue_dist_wr": None, "bms_sim_venue_dist_pr": None,
                     # Phase 10B
                     "sire_credibility": 0.0, "bms_credibility": 0.0,
                     "sire_surface_pref": None, "bms_surface_pref": None,
                     "sire_dist_pref": None, "sire_recent_trend": None,
                     # Phase 11
                     "sire_smile_pr": None, "sire_cond_pr": None, "sire_venue_pr": None,
                     "bms_smile_pr": None, "bms_cond_pr": None, "bms_venue_pr": None,
                     "bms_dist_pr": None,
                     # Phase 12
                     "sire_pace_pr": None, "sire_style_pr": None, "sire_gate_pr": None,
                     "sire_jockey_pr": None, "sire_trainer_pr": None,
                     "bms_pace_pr": None, "bms_style_pr": None, "bms_gate_pr": None,
                     "bms_jockey_pr": None, "bms_trainer_pr": None})

    # Tier1追加特徴量 (Step 1) + ② 前走オッズ・クラス変化
    extra = tracker.get_horse_extra_features(
        hid, date_str,
        race.get("condition", "良"),
        jid,
    )
    feat.update(extra)
    # class_change: 今走グレード vs 前走グレード (-1=降格, 0=同, 1=昇格)
    current_gc = feat.get("grade_code", 1)
    prev_gc = extra.get("prev_grade_code")
    if prev_gc is not None:
        feat["class_change"] = int(current_gc > prev_gc) - int(current_gc < prev_gc)
    # 推論時に horse dict から直接オーバーライド（エンジン計算値を優先）
    if horse.get("is_jockey_change_override") is not None:
        feat["is_jockey_change"] = int(horse["is_jockey_change_override"])
    if horse.get("kishu_pattern_code_override") is not None:
        feat["kishu_pattern_code"] = float(horse["kishu_pattern_code_override"])

    # Step 2+③: スタッキング特徴量 + スピード指数 (訓練時プロキシ / 推論時はエンジン推定値でオーバーライド)
    stacking = tracker.get_horse_stacking_features(hid, date_str)
    feat.update(stacking)
    if horse.get("ml_pos_est_override") is not None:
        feat["ml_pos_est"] = float(horse["ml_pos_est_override"])
    if horse.get("ml_l3f_est_override") is not None:
        feat["ml_l3f_est"] = float(horse["ml_l3f_est_override"])

    # Task #26: コース分析特徴量 (gate×venue / style×surface / gate×style)
    horse_style = feat.get("horse_running_style")
    feat.update(tracker.get_course_strategy_features(
        horse.get("gate_no") or 0,
        horse_style,
        venue,
        dc,
        surface,
    ))

    # ML-1b: 騎手×調教師コンビの直近30日成績
    feat.update(tracker.get_combo_30d_features(
        jid, tid, date_str,
        fallback_jt_wr=feat.get("jt_combo_wr"),
        fallback_trainer_place_rate=feat.get("trainer_place_rate"),
    ))

    # ML-1c: 馬体重変化トレンド（直近3走の馬体重の線形傾き）
    hw_hist = getattr(tracker, 'horse_weight_history', {}).get(hid, [])
    if len(hw_hist) >= 3:
        recent3 = hw_hist[-3:]
        # 傾き: (最新 - 3走前) / 2 走  (単純差分の平均, 単位: kg/走)
        feat["weight_kg_trend_3run"] = (recent3[-1] - recent3[0]) / 2.0
    else:
        feat["weight_kg_trend_3run"] = 0.0

    # Phase 10B: 調教師クラスレベル推移 + 休養明け馬複勝率
    feat.update(tracker.get_trainer_phase10b_features(tid))

    # Phase 11: 馬の条件別複勝率（run_detailsから2年以内で集計）
    _hs = tracker.horses.get(hid)
    if _hs:
        feat.update(_hs.get_condition_pr_features(
            date_str, venue, surface, distance,
            _smile_cat_ef, horse.get("gate_no"), jid,
            condition=_condition))
        feat.update(_hs.get_speed_index_windows(date_str))
    else:
        feat.update({"horse_pr_2y": None, "horse_venue_pr": None,
                     "horse_dist_pr": None, "horse_smile_pr": None,
                     "horse_style_pr": None, "horse_gate_pr": None,
                     "horse_jockey_pr": None, "horse_cond_pr": None,
                     "speed_index_avg_1y": None, "speed_index_best_1y": None,
                     "speed_index_avg_6m": None, "speed_index_trend": None})

    # Phase 10B: 展開特徴量（per-horse計算分）
    # early_position_est: 枠番×脚質から序盤位置取りを推定
    # 低枠(1) + 逃げ(0) → 高スコア(前方), 外枠 + 追込(1) → 低スコア(後方)
    _gate = horse.get("gate_no")
    _fc = race.get("field_count", 0) or 1
    _style = feat.get("horse_running_style")
    if _gate is not None and _style is not None:
        feat["early_position_est"] = (1.0 - _style) * (1.0 - (_gate - 1) / max(_fc - 1, 1))
    else:
        feat["early_position_est"] = None

    # last3f_pace_diff: 上がり3F推定 - 位置推定の差分（末脚のキック力を示す）
    _l3f = feat.get("ml_l3f_est")
    _pos = feat.get("ml_pos_est")
    if _l3f is not None and _pos is not None:
        feat["last3f_pace_diff"] = float(_l3f) - float(_pos)
    else:
        feat["last3f_pace_diff"] = None

    # ① レース内相対特徴量: プレースホルダー (後で _add_race_relative_features で設定)
    feat.update({
        "jockey_place_rank_in_race": None,
        "trainer_place_rank_in_race": None,
        "horse_form_rank_in_race": None,
        "horse_place_rank_in_race": None,
        "venue_sim_rank_in_race": None,
        "jockey_place_zscore_in_race": None,
        "horse_form_zscore_in_race": None,
        "relative_weight_kg": None,
        "jockey_wp_ratio": None,
        "trainer_wp_ratio": None,
        # Batch5: 展開予測（フィールド脚質構成） - _add_race_relative_featuresで設定
        "front_runner_count_in_race": None,
        "pace_pressure_index": None,
        "style_pace_affinity": None,
        # Phase 10B: レースレベル展開特徴量 - _add_race_relative_featuresで設定
        "field_pace_variance": None,
        "pace_horse_match": None,
    })

    return feat


# ============================================================
# ① レース内相対特徴量 (2パス方式)
# ============================================================


def _add_race_relative_features(feats: List[dict]) -> None:
    """
    レース内の全馬特徴量リストに対して、レース内相対特徴量を in-place で設定する。
    _extract_features() の後に呼ぶこと (プレースホルダーを上書きする)。

    相対特徴量:
      - *_rank_in_race: 0=最低, 1=最高 (None は除外して計算)
      - *_zscore_in_race: (val - mean) / std (std=0 の場合は 0.0)
      - relative_weight_kg: 自馬斤量 - レース平均斤量
      - jockey_wp_ratio / trainer_wp_ratio: win_rate / max(place_rate, ε)
    """
    n = len(feats)
    if n == 0:
        return

    def _rank_normalize(vals: List[Optional[float]]) -> List[Optional[float]]:
        """None を除いた値でパーセンタイルランクを計算 (0=最低, 1=最高)"""
        indexed = [(i, v) for i, v in enumerate(vals) if v is not None]
        if len(indexed) < 2:
            return [None] * n
        sorted_indices = sorted(indexed, key=lambda x: x[1])
        ranks = [None] * n
        m = len(sorted_indices)
        for rank, (i, _) in enumerate(sorted_indices):
            ranks[i] = rank / (m - 1)
        return ranks

    def _zscore(vals: List[Optional[float]]) -> List[Optional[float]]:
        valid = [v for v in vals if v is not None]
        if len(valid) < 2:
            return [None] * n
        mu = sum(valid) / len(valid)
        sigma = (sum((v - mu) ** 2 for v in valid) / len(valid)) ** 0.5
        return [(v - mu) / sigma if v is not None and sigma > 1e-9 else (0.0 if v is not None else None)
                for v in vals]

    # 各特徴量リスト抽出
    jpr  = [f.get("jockey_place_rate") for f in feats]
    tpr  = [f.get("trainer_place_rate") for f in feats]
    form = [f.get("dev_run1") for f in feats]
    hpr  = [f.get("horse_place_rate") for f in feats]
    vsim = [f.get("venue_sim_place_rate") for f in feats]
    wkg  = [f.get("weight_kg") for f in feats]

    # ランク
    jpr_rank  = _rank_normalize(jpr)
    tpr_rank  = _rank_normalize(tpr)
    form_rank = _rank_normalize(form)
    hpr_rank  = _rank_normalize(hpr)
    vsim_rank = _rank_normalize(vsim)

    # zスコア
    jpr_z  = _zscore(jpr)
    form_z = _zscore(form)

    # 相対体重
    valid_wkg = [v for v in wkg if v is not None]
    wkg_mean = sum(valid_wkg) / len(valid_wkg) if valid_wkg else None

    for i, f in enumerate(feats):
        f["jockey_place_rank_in_race"]   = jpr_rank[i]
        f["trainer_place_rank_in_race"]  = tpr_rank[i]
        f["horse_form_rank_in_race"]     = form_rank[i]
        f["horse_place_rank_in_race"]    = hpr_rank[i]
        f["venue_sim_rank_in_race"]      = vsim_rank[i]
        f["jockey_place_zscore_in_race"] = jpr_z[i]
        f["horse_form_zscore_in_race"]   = form_z[i]
        f["relative_weight_kg"] = (f.get("weight_kg") - wkg_mean
                                   if f.get("weight_kg") is not None and wkg_mean is not None
                                   else None)
        # win/place 比率
        jwr = f.get("jockey_win_rate")
        jpr_v = f.get("jockey_place_rate")
        f["jockey_wp_ratio"] = (jwr / max(jpr_v, 1e-6)
                                if jwr is not None and jpr_v is not None else None)
        twr = f.get("trainer_win_rate")
        tpr_v = f.get("trainer_place_rate")
        f["trainer_wp_ratio"] = (twr / max(tpr_v, 1e-6)
                                 if twr is not None and tpr_v is not None else None)

    # Batch5: 展開予測（フィールド脚質構成）
    # horse_running_style: 0=逃げ, 1=追込 (直近5走の4角位置/頭数の平均)
    FRONT_THRESH = 0.35  # 逃げ・先行馬の閾値（0.35以下）
    styles = [f.get("horse_running_style") for f in feats]
    valid_styles = [s for s in styles if s is not None]
    if valid_styles:
        front_count = sum(1 for s in valid_styles if s < FRONT_THRESH)
        # 有効馬数ベースで正規化（Noneの馬は除外）
        pace_pressure = front_count / len(valid_styles)
    else:
        front_count = None
        pace_pressure = None

    # Phase 10B: フィールド内脚質分散（均等 vs 偏り）
    if len(valid_styles) >= 2:
        _s_mean = sum(valid_styles) / len(valid_styles)
        _s_var = sum((s - _s_mean) ** 2 for s in valid_styles) / len(valid_styles)
        field_pace_var = _s_var ** 0.5  # 標準偏差
    else:
        field_pace_var = None

    for i, f in enumerate(feats):
        f["front_runner_count_in_race"] = front_count
        f["pace_pressure_index"] = pace_pressure
        style = f.get("horse_running_style")
        if style is not None and pace_pressure is not None:
            # 追込(1)でハイペース圧力高い = +, 逃げ(0)でハイペース圧力高い = -
            # 範囲: [-1, +1]
            f["style_pace_affinity"] = (2.0 * style - 1.0) * pace_pressure
        else:
            f["style_pace_affinity"] = None

        # Phase 10B: フィールド脚質分散
        f["field_pace_variance"] = field_pace_var

        # Phase 10B: pace_horse_match — 馬のペース選好×予想ペース圧力の一致度
        # pace_pref_score: 正=ハイペース得意, 負=スロー得意
        # pace_pressure: 高=ハイペース予測
        _pps = f.get("pace_pref_score")
        if _pps is not None and pace_pressure is not None:
            # 正規化: pace_pressure を [-1,1] スケールに変換 (0.5→0)
            _pp_centered = (pace_pressure - 0.3) * 3.0  # 0.3を中立点として±
            f["pace_horse_match"] = _pps * _pp_centered
        else:
            f["pace_horse_match"] = None


# ============================================================
# データ読み込み
# ============================================================


def _load_ml_races() -> List[dict]:
    """全ML JSONファイルを日付順に読み込む"""
    if not os.path.isdir(ML_DATA_DIR):
        return []

    files = sorted(
        f for f in os.listdir(ML_DATA_DIR)
        if f.endswith(".json") and not f.startswith("_")
    )

    all_races = []
    for fname in files:
        try:
            with open(os.path.join(ML_DATA_DIR, fname), "r", encoding="utf-8") as f:
                data = json.load(f)
            all_races.extend(data.get("races", []))
        except Exception:
            continue

    all_races.sort(key=lambda r: r.get("date", ""))
    return all_races


# ============================================================
# モデル学習
# ============================================================


def train_model(
    valid_days: int = 30,
    surface_filter: Optional[int] = None,   # None=全体, 0=芝, 1=ダート
    jra_filter: Optional[bool] = None,       # None=全体, True=JRA, False=NAR
    venue_filter: Optional[str] = None,      # None=全場, "05"=東京 etc
    smile_filter: Optional[str] = None,      # None=全距離, "ss"/"s"/"m"/"i"/"l"/"e"
    model_key: Optional[str] = None,         # Noneなら自動生成
    tracker: Optional["RollingStatsTracker"] = None,
    sire_tracker: Optional["RollingSireTracker"] = None,
    _preloaded_rows: Optional[tuple] = None,  # 事前収集行を再利用する場合
) -> dict:
    """
    LightGBMモデルを時系列分割で学習・検証する。
    tracker/sire_tracker が渡された場合はそれを再利用（複数モデル一括学習の高速化）。
    _preloaded_rows が渡された場合は行収集をスキップ。

    Returns:
        metrics dict  (サンプル不足の場合は {"skipped": True} を返す)
    """
    import lightgbm as lgb
    import numpy as np

    # モデルキー自動生成
    if model_key is None:
        parts = []
        if jra_filter is True:   parts.append("jra")
        elif jra_filter is False: parts.append("nar")
        if surface_filter == 0:  parts.append("turf")
        elif surface_filter == 1: parts.append("dirt")
        if venue_filter:         parts.append(f"venue_{venue_filter}")
        if smile_filter:         parts.append(smile_filter.lower())
        model_key = "_".join(parts) if parts else "global"

    save_path = _model_path(model_key)
    surf_names = {None: "全体", 0: "芝", 1: "ダート", 2: "障害"}
    jra_name   = {None: "", True: "JRA", False: "NAR"}
    label = " ".join(filter(None, [
        jra_name.get(jra_filter, ""),
        surf_names.get(surface_filter, str(surface_filter)),
        f"競馬場{venue_filter}" if venue_filter else "",
        smile_filter.upper() if smile_filter else "",
    ])).strip() or "全体"

    if _preloaded_rows is not None:
        # 事前収集済みの行を再利用
        all_train_rows, all_valid_groups, split_date = _preloaded_rows
    else:
        races = _load_ml_races()
        if not races:
            raise ValueError(f"ML data not found in {ML_DATA_DIR}")
        all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
        if len(all_dates) < 14:
            raise ValueError(f"Insufficient dates ({len(all_dates)})")
        split_idx = max(1, len(all_dates) - valid_days)
        split_date = all_dates[split_idx]

        logger.info("学習期間: %s ~ %s / 検証: %s ~ %s",
                    all_dates[0], all_dates[split_idx - 1],
                    split_date, all_dates[-1])

        need_new_tracker = tracker is None
        if need_new_tracker:
            sire_map_l = _load_horse_sire_map()
            tracker = RollingStatsTracker()
            sire_tracker = RollingSireTracker()
        else:
            sire_map_l = _load_horse_sire_map()

        # (feat, label, surface_val, is_jra_val, venue_str, smile_cat)
        all_train_rows: List[Tuple] = []
        all_valid_groups: List[Tuple] = []

        for race in races:
            date_str    = race.get("date", "")
            is_valid    = date_str >= split_date
            surface_val = SURFACE_MAP.get(race.get("surface", ""), -1)
            is_jra_val  = int(bool(race.get("is_jra", True)))
            venue_str   = str(race.get("venue_code", "") or "").zfill(2)
            distance    = int(race.get("distance") or 0)
            smile_cat   = _smile_key_ml(distance) if distance else ""

            race_feats, race_labels = [], []
            for h in race.get("horses", []):
                fp = h.get("finish_pos")
                if fp is None:
                    continue
                hid = h.get("horse_id", "")
                sid, bid = sire_map_l.get(hid, ("", ""))
                h_with_sire = dict(h, sire_id=sid, bms_id=bid)
                feat = _extract_features(h_with_sire, race, tracker, sire_tracker)
                race_feats.append(feat)
                race_labels.append(1 if fp <= 3 else 0)

            if race_feats:
                # ① 相対特徴量を一括設定 (2パス)
                _add_race_relative_features(race_feats)
                meta = (surface_val, is_jra_val, venue_str, smile_cat)
                if is_valid:
                    all_valid_groups.append((race_feats, race_labels, *meta))
                else:
                    for feat, lbl in zip(race_feats, race_labels):
                        all_train_rows.append((feat, lbl, *meta))

            if need_new_tracker:
                tracker.update_race(race)
                sire_tracker.update_race(race, sire_map_l)

    # ---------- フィルタリング ----------
    def _matches(sv, jv, vv, smv):
        if surface_filter is not None and sv != surface_filter: return False
        if jra_filter is not None and jv != int(jra_filter):    return False
        if venue_filter is not None and vv != venue_filter:      return False
        if smile_filter is not None and smv != smile_filter:     return False
        return True

    train_rows   = [(f, l) for f, l, sv, jv, vv, smv in all_train_rows
                    if _matches(sv, jv, vv, smv)]
    valid_groups = [(fs, ls) for fs, ls, sv, jv, vv, smv in all_valid_groups
                    if _matches(sv, jv, vv, smv)]

    train_X_rows = [f for f, l in train_rows]
    train_y      = [l for f, l in train_rows]
    valid_X_rows = [f for fs, ls in valid_groups for f in fs]
    valid_y      = [l for fs, ls in valid_groups for l in ls]
    valid_race_sizes = [len(fs) for fs, ls in valid_groups]

    # サンプル不足チェック
    if len(train_X_rows) < MIN_TRAIN_SAMPLES:
        logger.info("[%s] サンプル不足でスキップ (%d < %d)", label, len(train_X_rows), MIN_TRAIN_SAMPLES)
        return {"skipped": True, "model_key": model_key, "train_samples": len(train_X_rows)}

    logger.info("=" * 55)
    logger.info("LightGBM [%s]  train=%d  valid=%d", label, len(train_y), len(valid_y))
    logger.info("=" * 55)

    logger.info("Train: %d samples / Valid: %d samples", len(train_y), len(valid_y))
    logger.info("Jockeys: %d / Trainers: %d / Horses: %d / Combos: %d",
                len(tracker.jockeys), len(tracker.trainers),
                len(tracker.horses), len(tracker.combos))
    logger.info("Sires: %d / BMS: %d",
                len(sire_tracker._sire), len(sire_tracker._bms))

    # ばんえい（venue_65）は専用特徴量リストを使用
    _is_banei_model = (venue_filter == "65")
    _feat_cols = FEATURE_COLUMNS_BANEI if _is_banei_model else FEATURE_COLUMNS
    _cat_feats = [c for c in CATEGORICAL_FEATURES if c in _feat_cols]

    def _to_np(rows):
        import numpy as np
        mat = []
        for f in rows:
            mat.append([float(f[c]) if f[c] is not None else float("nan")
                        for c in _feat_cols])
        return np.array(mat, dtype=np.float32)

    X_train, y_train = _to_np(train_X_rows), np.array(train_y, dtype=np.int32)
    X_valid, y_valid = _to_np(valid_X_rows), np.array(valid_y, dtype=np.int32)

    dtrain = lgb.Dataset(
        X_train, label=y_train,
        feature_name=_feat_cols,
        categorical_feature=_cat_feats,
        free_raw_data=False,
    )

    # ④ Optuna HPO: 最適パラメータがあれば読み込む
    _best_params_path = os.path.join(MODEL_DIR, "best_lgbm_params.json")
    _optuna_params = {}
    if os.path.exists(_best_params_path):
        try:
            import json as _json
            with open(_best_params_path, encoding="utf-8") as _f:
                _optuna_params = _json.load(_f).get("best_params", {})
            if _optuna_params:
                logger.info("Optuna 最適パラメータを適用: %s", _optuna_params)
        except Exception:
            _optuna_params = {}

    if _is_banei_model:
        # ばんえい専用パラメータ (data/models/best_banei_params.json があれば使用)
        _banei_params_path = os.path.join(MODEL_DIR, "best_banei_params.json")
        _banei_params = {}
        if os.path.exists(_banei_params_path):
            try:
                import json as _bjson
                with open(_banei_params_path, encoding="utf-8") as _bf:
                    _banei_params = _bjson.load(_bf).get("best_params", {})
                if _banei_params:
                    logger.info("ばんえい専用Optunaパラメータを適用: %s", _banei_params)
            except Exception:
                _banei_params = {}
        params = {
            "objective": "binary",
            "metric": ["binary_logloss", "auc"],
            "boosting_type": "gbdt",
            "num_leaves": _banei_params.get("num_leaves", 31),
            "learning_rate": _banei_params.get("learning_rate", 0.02),
            "feature_fraction": _banei_params.get("feature_fraction", 0.8),
            "bagging_fraction": _banei_params.get("bagging_fraction", 0.7),
            "bagging_freq": _banei_params.get("bagging_freq", 5),
            "min_child_samples": _banei_params.get("min_child_samples", 50),
            "lambda_l1": _banei_params.get("lambda_l1", 1.0),
            "lambda_l2": _banei_params.get("lambda_l2", 2.0),
            "max_depth": _banei_params.get("max_depth", 5),
            "verbose": -1,
            "seed": 42,
            "is_unbalance": True,
        }
        if not _banei_params:
            logger.info("ばんえいデフォルトパラメータを適用")
    else:
        params = {
            "objective": "binary",
            "metric": ["binary_logloss", "auc"],
            "boosting_type": "gbdt",
            "num_leaves": _optuna_params.get("num_leaves", 63),
            "learning_rate": _optuna_params.get("learning_rate", 0.02),
            "feature_fraction": _optuna_params.get("feature_fraction", 0.8),
            "bagging_fraction": _optuna_params.get("bagging_fraction", 0.8),
            "bagging_freq": _optuna_params.get("bagging_freq", 5),
            "min_child_samples": _optuna_params.get("min_child_samples", 50),
            "lambda_l1": _optuna_params.get("lambda_l1", 0.1),
            "lambda_l2": _optuna_params.get("lambda_l2", 1.0),
            "max_depth": _optuna_params.get("max_depth", 7),
            "verbose": -1,
            "seed": 42,
            "is_unbalance": True,
        }

    if len(valid_y) == 0:
        # 検証データなし → 固定ラウンドで学習（early stopping なし）
        logger.warning("[%s] 検証データ0件 → 固定300ラウンドで学習", label)
        model = lgb.train(
            params,
            dtrain,
            num_boost_round=300,
            callbacks=[lgb.log_evaluation(period=100)],
        )
    else:
        dvalid = lgb.Dataset(
            X_valid, label=y_valid,
            feature_name=_feat_cols,
            categorical_feature=_cat_feats,
            reference=dtrain,
            free_raw_data=False,
        )
        model = lgb.train(
            params,
            dtrain,
            num_boost_round=3000,       # 1000→3000: lr低下分を補完
            valid_sets=[dtrain, dvalid],
            valid_names=["train", "valid"],
            callbacks=[
                lgb.log_evaluation(period=200),
                lgb.early_stopping(stopping_rounds=100),  # 50→100: より余裕を持たせる
            ],
        )

    # ---- 評価 ----
    # 特徴量重要度（検証データなしでも計算可能）
    imp = model.feature_importance(importance_type="gain")
    imp_pairs = sorted(zip(_feat_cols, imp), key=lambda x: -x[1])

    if len(valid_y) == 0:
        # 検証データなし → 評価指標はN/A
        logger.info("[%s] 検証データ0件のため評価指標なし", label)
        metrics = {
            "auc": None,
            "logloss": None,
            "brier": None,
            "train_samples": len(train_y),
            "valid_samples": 0,
            "valid_races": 0,
            "top1_hit_rate": None,
            "top3_hit_rate": None,
            "best_iteration": model.best_iteration,
            "split_date": split_date,
        }
    else:
        from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

        y_pred = model.predict(X_valid)
        auc = roc_auc_score(y_valid, y_pred)
        logloss = log_loss(y_valid, y_pred)
        brier = brier_score_loss(y_valid, y_pred)

        # キャリブレーション
        cal_bins = [(0, 0.15), (0.15, 0.25), (0.25, 0.35), (0.35, 0.50), (0.50, 1.0)]
        cal_lines = []
        for lo, hi in cal_bins:
            mask = (y_pred >= lo) & (y_pred < hi)
            if mask.sum() > 0:
                cal_lines.append(
                    f"  {lo:.2f}-{hi:.2f}: pred={y_pred[mask].mean():.3f}"
                    f" actual={y_valid[mask].mean():.3f} n={mask.sum()}"
                )

        # レース単位精度
        correct_top1, correct_top3, total_eval = 0, 0, 0
        idx = 0
        for g in valid_race_sizes:
            if g < 3:
                idx += g
                continue
            rp = y_pred[idx:idx + g]
            rt = y_valid[idx:idx + g]
            if rt[np.argmax(rp)] == 1:
                correct_top1 += 1
            if any(rt[i] == 1 for i in np.argsort(rp)[-3:]):
                correct_top3 += 1
            total_eval += 1
            idx += g

        metrics = {
            "auc": round(auc, 4),
            "logloss": round(logloss, 4),
            "brier": round(brier, 4),
            "train_samples": len(train_y),
            "valid_samples": len(valid_y),
            "valid_races": total_eval,
            "top1_hit_rate": round(correct_top1 / max(total_eval, 1), 4),
            "top3_hit_rate": round(correct_top3 / max(total_eval, 1), 4),
            "best_iteration": model.best_iteration,
            "split_date": split_date,
        }

        logger.info("")
        logger.info("=" * 50)
        logger.info("学習結果")
        logger.info("=" * 50)
        logger.info("AUC:           %.4f", auc)
        logger.info("LogLoss:       %.4f", logloss)
        logger.info("Brier Score:   %.4f", brier)
        logger.info("Best iteration: %d", model.best_iteration)
        logger.info("")
        logger.info("レース単位精度 (検証 %d レース):", total_eval)
        logger.info("  Top1推し → 3着内:  %.1f%% (%d/%d)",
                    metrics["top1_hit_rate"] * 100, correct_top1, total_eval)
        logger.info("  Top3推し → 3着内:  %.1f%% (%d/%d)",
                    metrics["top3_hit_rate"] * 100, correct_top3, total_eval)
        logger.info("")
        logger.info("キャリブレーション:")
        for line in cal_lines:
            logger.info(line)

    logger.info("")
    logger.info("特徴量重要度 (Top 15):")
    for name, val in imp_pairs[:15]:
        logger.info("  %-25s %10.0f", name, val)

    # ---- ⑦ キャリブレーション (Platt scaling) ----
    if len(valid_y) > 0:
        try:
            from sklearn.linear_model import LogisticRegression
            import json as _json
            cal_X = y_pred.reshape(-1, 1)
            cal_model = LogisticRegression(C=1.0, max_iter=200)
            cal_model.fit(cal_X, y_valid)
            cal_a = float(cal_model.coef_[0][0])
            cal_b = float(cal_model.intercept_[0])
            cal_path = save_path.replace(".txt", "_cal.json")
            with open(cal_path, "w") as _cf:
                _json.dump({"a": cal_a, "b": cal_b}, _cf)
            metrics["cal_a"] = round(cal_a, 4)
            metrics["cal_b"] = round(cal_b, 4)
            logger.info("Platt calibration: a=%.4f b=%.4f → saved %s",
                        cal_a, cal_b, os.path.basename(cal_path))
        except Exception as _ce:
            logger.debug("キャリブレーション保存失敗: %s", _ce)

    # ---- 保存 ----
    os.makedirs(MODEL_DIR, exist_ok=True)
    model.save_model(save_path)
    # tracker/sire_tracker は全レース共通のため global モデルのときのみ保存
    if model_key == "global" and _preloaded_rows is None:
        with open(STATS_PATH, "wb") as f:
            pickle.dump(tracker, f)
        with open(SIRE_STATS_PATH, "wb") as f:
            pickle.dump(sire_tracker, f)
        logger.info("統計保存: %s (%.1f MB)",
                    STATS_PATH, os.path.getsize(STATS_PATH) / 1048576)

    logger.info("[%s] 保存: %s", label, os.path.basename(save_path))

    metrics["model_key"] = model_key
    return metrics


def _collect_all_rows(valid_days: int = 30):
    """
    全レースデータを一度だけ走査して行を収集する。
    train_split_models() から呼ばれ、各モデル学習で共有する。
    Returns: (all_train_rows, all_valid_groups, split_date, tracker, sire_tracker)
    """
    races = _load_ml_races()
    if not races:
        raise ValueError(f"ML data not found in {ML_DATA_DIR}")
    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    split_idx  = max(1, len(all_dates) - valid_days)
    split_date = all_dates[split_idx]

    logger.info("全行収集開始: 学習 %s~%s / 検証 %s~%s",
                all_dates[0], all_dates[split_idx - 1], split_date, all_dates[-1])

    sire_map     = _load_horse_sire_map()
    tracker      = RollingStatsTracker()
    sire_tracker = RollingSireTracker()
    all_train_rows: List[Tuple] = []
    all_valid_groups: List[Tuple] = []

    for race in races:
        date_str    = race.get("date", "")
        is_valid    = date_str >= split_date
        surface_val = SURFACE_MAP.get(race.get("surface", ""), -1)
        is_jra_val  = int(bool(race.get("is_jra", True)))
        venue_str   = str(race.get("venue_code", "") or "").zfill(2)
        distance    = int(race.get("distance") or 0)
        smile_cat   = _smile_key_ml(distance) if distance else ""

        race_feats, race_labels = [], []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            feat = _extract_features(dict(h, sire_id=sid, bms_id=bid),
                                     race, tracker, sire_tracker)
            race_feats.append(feat)
            race_labels.append(1 if fp <= 3 else 0)

        if race_feats:
            # ① 相対特徴量を一括設定 (2パス)
            _add_race_relative_features(race_feats)
            meta = (surface_val, is_jra_val, venue_str, smile_cat)
            if is_valid:
                all_valid_groups.append((race_feats, race_labels, *meta))
            else:
                for feat, lbl in zip(race_feats, race_labels):
                    all_train_rows.append((feat, lbl, *meta))

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    logger.info("全行収集完了: train=%d groups, valid=%d groups",
                len(all_train_rows), len(all_valid_groups))

    # tracker を保存（全モデル共有）
    os.makedirs(MODEL_DIR, exist_ok=True)
    with open(STATS_PATH, "wb") as f:
        pickle.dump(tracker, f)
    with open(SIRE_STATS_PATH, "wb") as f:
        pickle.dump(sire_tracker, f)
    logger.info("統計保存: %s (%.1f MB)", STATS_PATH, os.path.getsize(STATS_PATH) / 1048576)

    return all_train_rows, all_valid_groups, split_date, tracker, sire_tracker


def train_split_models(valid_days: int = 30) -> dict:
    """
    全モデルを一括学習する。
    学習順:
      global → turf/dirt → jra_turf/jra_dirt/nar
      → 競馬場別 (venue_XX) → JRA芝×SMILE / JRAダート×SMILE

    Returns:
        {model_key: metrics_dict}
    """
    logger.info("=== 全モデル一括学習開始 ===")

    # 全行を1回だけ収集してキャッシュ
    all_train_rows, all_valid_groups, split_date, tracker, sire_tracker = \
        _collect_all_rows(valid_days=valid_days)
    preloaded = (all_train_rows, all_valid_groups, split_date)

    # 学習するモデル定義: (model_key, kwargs)
    # 競馬場コード: JRA=01~10, NAR上位
    jra_venues = ["01","02","03","04","05","06","07","08","09","10"]
    nar_venues = ["30","35","36","42","43","44","45","46","47","48","50","51","54","55","65"]
    smile_cats = ["ss","s","m","i","l","e"]

    tasks = [
        # Level 0: 全体
        ("global",    dict()),
        # Level 1: 馬場
        ("turf",      dict(surface_filter=0)),
        ("dirt",      dict(surface_filter=1)),
        # Level 2: JRA/NAR × 馬場
        ("jra_turf",  dict(surface_filter=0, jra_filter=True)),
        ("jra_dirt",  dict(surface_filter=1, jra_filter=True)),
        ("nar",       dict(jra_filter=False)),
    ]
    # Level 3: 競馬場別 (JRA全場 + NAR上位)
    for vc in jra_venues:
        tasks.append((f"venue_{vc}", dict(jra_filter=True, venue_filter=vc)))
    for vc in nar_venues:
        tasks.append((f"venue_{vc}", dict(jra_filter=False, venue_filter=vc)))
    # Level 4: JRA × 馬場 × SMILE
    for sm in smile_cats:
        tasks.append((f"jra_turf_{sm}", dict(surface_filter=0, jra_filter=True, smile_filter=sm)))
        tasks.append((f"jra_dirt_{sm}", dict(surface_filter=1, jra_filter=True, smile_filter=sm)))

    results = {}
    total = len(tasks)
    for i, (key, kwargs) in enumerate(tasks, 1):
        logger.info("--- [%d/%d] %s ---", i, total, key)
        m = train_model(
            valid_days=valid_days,
            model_key=key,
            tracker=tracker,
            sire_tracker=sire_tracker,
            _preloaded_rows=preloaded,
            **kwargs,
        )
        results[key] = m

    # サマリー出力
    logger.info("=== 学習完了 ===")
    for key, m in results.items():
        if m.get("skipped"):
            logger.info("[%-22s] SKIPPED (train=%d)", key, m.get("train_samples", 0))
        elif m.get("auc") is None:
            logger.info("[%-22s] no-valid  iter=%d  n=%d",
                        key, m["best_iteration"], m["train_samples"])
        else:
            logger.info("[%-22s] AUC=%.4f  Top1=%.1f%%  Top3=%.1f%%  iter=%d  n=%d",
                        key, m["auc"],
                        m["top1_hit_rate"] * 100, m["top3_hit_rate"] * 100,
                        m["best_iteration"], m["train_samples"])
    return results


# ============================================================
# 推論
# ============================================================


# スレッドローカルストレージ: predict_race() で設定した model_level を
# 同一スレッド内の engine._calc_blend_ratio() から安全に読み取るため
_lgbm_tls = threading.local()


class LGBMPredictor:
    """
    学習済みモデルによる推論。
    4段階フォールバック: 競馬場別 → JRA芝/ダート/NAR×SMILE → JRA×馬場/NAR → 馬場 → 全体
    """

    def __init__(self):
        self._models: Dict[str, object] = {}   # model_key -> lgb.Booster
        self._cal_params: Dict[str, dict] = {}  # model_key -> {a, b} Platt scaling
        self._tracker: Optional[RollingStatsTracker] = None
        self._sire_tracker: Optional[RollingSireTracker] = None
        self._loaded = False
        self._last_model_level: int = 2  # 直近 predict_race() で使用したモデルのレベル (0-4)

    def _select_model(self, surface_val: int, is_jra: bool,
                      venue_code: str = "", smile_cat: str = ""):
        """4段階フォールバックでモデルを選択する。戻り値: (model, level)"""
        m = self._models
        surf = {0: "turf", 1: "dirt"}.get(surface_val, "")

        # Level 4: 競馬場別（品質フィルター付き）
        if venue_code:
            from config.settings import PIPELINE_V2_ENABLED, VENUE_MODEL_SKIP
            if not (PIPELINE_V2_ENABLED and venue_code in VENUE_MODEL_SKIP):
                mdl = m.get(f"venue_{venue_code}")
                if mdl:
                    return mdl, 4

        # Level 3: JRA × 馬場 × SMILE
        if is_jra and surf and smile_cat:
            mdl = m.get(f"jra_{surf}_{smile_cat}")
            if mdl:
                return mdl, 3

        # Level 2: JRA × 馬場  /  NAR
        if is_jra and surf:
            mdl = m.get(f"jra_{surf}")
            if mdl:
                return mdl, 2
        elif not is_jra:
            mdl = m.get("nar")
            if mdl:
                return mdl, 2

        # Level 1: 馬場
        if surf:
            mdl = m.get(surf)
            if mdl:
                return mdl, 1

        # Level 0: 全体
        return m.get("global"), 0

    def load(self) -> bool:
        if self._loaded:
            return True
        if not os.path.exists(STATS_PATH):
            return False
        try:
            import lightgbm as lgb
            loaded_keys = []
            for fname in os.listdir(MODEL_DIR):
                if not fname.startswith("lgbm_place") or not fname.endswith(".txt"):
                    continue
                fpath = os.path.join(MODEL_DIR, fname)
                # キー抽出: lgbm_place.txt → "global",  lgbm_place_jra_turf.txt → "jra_turf"
                stem = fname[len("lgbm_place"):-len(".txt")].lstrip("_")
                key = stem if stem else "global"
                try:
                    self._models[key] = lgb.Booster(model_file=fpath)
                    loaded_keys.append(key)
                except Exception as e:
                    logger.warning("モデル読み込み失敗 [%s]: %s", key, e)

            if not self._models:
                return False

            # ⑦ Platt calibration パラメータ読み込み
            import json as _json
            for fname in os.listdir(MODEL_DIR):
                if not fname.startswith("lgbm_place") or not fname.endswith("_cal.json"):
                    continue
                stem = fname[len("lgbm_place"):-len("_cal.json")].lstrip("_")
                key = stem if stem else "global"
                try:
                    with open(os.path.join(MODEL_DIR, fname)) as _cf:
                        self._cal_params[key] = _json.load(_cf)
                except Exception:
                    pass

            with open(STATS_PATH, "rb") as f:
                self._tracker = pickle.load(f)
            if os.path.exists(SIRE_STATS_PATH):
                with open(SIRE_STATS_PATH, "rb") as f:
                    self._sire_tracker = pickle.load(f)
            self._loaded = True
            logger.info("LightGBM loaded: %d models [%s] (H:%d)",
                        len(loaded_keys), ", ".join(sorted(loaded_keys)),
                        len(self._tracker.horses))
            return True
        except Exception as e:
            logger.warning("LightGBM load failed: %s", e, exc_info=True)
            return False

    @property
    def tracker(self) -> Optional[RollingStatsTracker]:
        """ローリング統計 tracker を返す（表示偏差値算出用）"""
        return self._tracker

    @property
    def sire_tracker(self) -> Optional["RollingSireTracker"]:
        """血統ローリング統計を返す（表示偏差値算出用）"""
        return self._sire_tracker

    def _build_X(self, race_dict, horse_dicts, model):
        import numpy as np
        features, ids = [], []
        for h in horse_dicts:
            feat = _extract_features(h, race_dict, self._tracker, self._sire_tracker)
            features.append(feat)
            ids.append(h.get("horse_id", ""))
        if not features:
            return None, []
        from data.masters.venue_master import is_banei as _is_banei_bx
        _vc = str(race_dict.get("venue_code", "") or "").zfill(2)
        feat_cols = FEATURE_COLUMNS_BANEI if _is_banei_bx(_vc) else FEATURE_COLUMNS
        if hasattr(model, "num_feature"):
            n = model.num_feature()
            if n < len(feat_cols):
                feat_cols = feat_cols[:n]
        X = np.array(
            [[float(f[c]) if f[c] is not None else float("nan") for c in feat_cols]
             for f in features],
            dtype=np.float32,
        )
        return X, ids, feat_cols

    def predict_race(self, race_dict: dict, horse_dicts: List[dict]) -> Dict[str, float]:
        """
        レース情報と出走馬リストから P(3着以内) を返す。

        Args:
            race_dict: {date, venue, surface, distance, condition,
                        field_count, is_jra, grade, venue_code}
            horse_dicts: [{horse_id, jockey_id, trainer_id, gate_no, horse_no,
                          sex, age, weight_kg, horse_weight, weight_change,
                          sire_id (optional), bms_id (optional)}]

        Returns:
            {horse_id: float}  P(top3)
        """
        if not self._loaded and not self.load():
            return {}

        import numpy as np

        surface_val = SURFACE_MAP.get(race_dict.get("surface", ""), -1)
        is_jra      = bool(race_dict.get("is_jra", True))
        venue_code  = str(race_dict.get("venue_code", "") or "").zfill(2)
        distance    = int(race_dict.get("distance") or 0)
        smile_cat   = _smile_key_ml(distance) if distance else ""

        model, level = self._select_model(surface_val, is_jra, venue_code, smile_cat)
        self._last_model_level = level
        _lgbm_tls.last_model_level = level
        if model is None:
            return {}

        features, ids = [], []
        for h in horse_dicts:
            feat = _extract_features(h, race_dict, self._tracker, self._sire_tracker)
            features.append(feat)
            ids.append(h.get("horse_id", ""))

        if not features:
            return {}

        # ① 相対特徴量を一括設定 (2パス)
        _add_race_relative_features(features)

        # ばんえい（venue_65）は専用特徴量リストを使用
        from data.masters.venue_master import is_banei as _is_banei_pred
        _is_banei_vc = _is_banei_pred(venue_code)
        feat_cols = FEATURE_COLUMNS_BANEI if _is_banei_vc else FEATURE_COLUMNS
        if hasattr(model, "num_feature"):
            n = model.num_feature()
            if n < len(feat_cols):
                feat_cols = feat_cols[:n]

        X = np.array(
            [[float(f[c]) if f[c] is not None else float("nan") for c in feat_cols]
             for f in features],
            dtype=np.float32,
        )
        probs = model.predict(X)

        # ⑦ Platt calibration を適用 (存在する場合)
        # 選択モデルのキーを特定してcalibrationを取得
        mdl_key = None
        for k, m in self._models.items():
            if m is model:
                mdl_key = k
                break
        cal = self._cal_params.get(mdl_key) if mdl_key else None
        if cal:
            import math as _math
            a, b = cal["a"], cal["b"]
            probs = [1.0 / (1.0 + _math.exp(-(a * p + b))) for p in probs]

        return {hid: float(p) for hid, p in zip(ids, probs)}

    def predict_from_engine(self, race_info, horses,
                            evaluations=None) -> Dict[str, float]:
        """
        RaceAnalysisEngine から呼ぶ用のラッパー。
        models.RaceInfo と models.Horse のリストを受け取る。

        Args:
            evaluations: HorseEvaluation リスト (Step2スタッキング用)
                         ev.pace.estimated_position_4c, ev.pace.estimated_last3f を使用
        """
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
            # ばんえい水分量（moisture_dirtに格納済み）
            "water_content": race_info.moisture_dirt,
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
            # Step2: エンジン推定値をオーバーライドとして設定
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
                # Tier1追加特徴量オーバーライド (エンジン計算値を優先)
                "is_jockey_change_override": int(h.is_jockey_change),
                # Step2 スタッキングオーバーライド
                "ml_pos_est_override": pos_est,
                "ml_l3f_est_override": l3f_est,
                # 当日市場データ
                "odds": getattr(h, "tansho_odds", None) or getattr(h, "odds", None),
                "popularity": getattr(h, "popularity", None),
            })

        return self.predict_race(race_dict, horse_dicts)

    def compute_shap_groups(
        self,
        race_info,
        horses,
        evaluations=None,
    ) -> Dict[str, Dict[str, float]]:
        """
        Step3: SHAP値をグループ別に集計して返す。

        Returns:
            {horse_id: {group_name: shap_sum, ..., "_base": base_value}}
            shap_sum > 0 は予測確率を上げる方向への寄与
        """
        if not self._loaded and not self.load():
            return {}

        try:
            import shap
            import numpy as np
        except ImportError:
            logger.debug("shap not installed — SHAP computation skipped")
            return {}

        # horse_dicts を構築 (predict_from_engine と同様)
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
            # ばんえい水分量（moisture_dirtに格納済み）
            "water_content": race_info.moisture_dirt,
        }

        ev_map: Dict[str, tuple] = {}
        if evaluations:
            for ev in evaluations:
                ev_map[ev.horse.horse_id] = (
                    getattr(ev.pace, "estimated_position_4c", None),
                    getattr(ev.pace, "estimated_last3f", None),
                )

        horse_dicts = []
        ids = []
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
                "sire_id": getattr(h, "sire_id", "") or "",
                "bms_id": getattr(h, "maternal_grandsire_id", "") or "",
                "is_jockey_change_override": int(h.is_jockey_change),
                "ml_pos_est_override": pos_est,
                "ml_l3f_est_override": l3f_est,
            })
            ids.append(h.horse_id)

        if not horse_dicts:
            return {}

        # 馬場・JRA/NAR・競馬場・SMILE に応じてモデルを選択
        surface_val = SURFACE_MAP.get(race_dict.get("surface", ""), -1)
        is_jra      = bool(race_dict.get("is_jra", True))
        venue_code  = str(race_dict.get("venue_code", "") or "").zfill(2)
        distance    = int(race_dict.get("distance") or 0)
        smile_cat   = _smile_key_ml(distance) if distance else ""
        model, level = self._select_model(surface_val, is_jra, venue_code, smile_cat)
        self._last_model_level = level
        _lgbm_tls.last_model_level = level
        if model is None:
            return {}

        # 特徴量行列構築（ばんえいは専用リスト）
        from data.masters.venue_master import is_banei as _is_banei_shap
        feat_cols = FEATURE_COLUMNS_BANEI if _is_banei_shap(venue_code) else FEATURE_COLUMNS
        if hasattr(model, "num_feature"):
            n = model.num_feature()
            if n < len(feat_cols):
                feat_cols = feat_cols[:n]

        features = []
        for hd in horse_dicts:
            feat = _extract_features(hd, race_dict, self._tracker, self._sire_tracker)
            features.append(feat)

        X = np.array(
            [[float(f[c]) if f.get(c) is not None else float("nan")
              for c in feat_cols]
             for f in features],
            dtype=np.float32,
        )

        try:
            explainer = shap.TreeExplainer(model)
            # check_additivity=False: NaNあり特徴量での整合性エラー回避
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                raw = explainer.shap_values(X, check_additivity=False)
            # shap v0.50+: binary classifierはリスト [neg_class, pos_class] を返す場合がある
            if isinstance(raw, list) and len(raw) == 2:
                shap_vals = raw[1]  # 正クラス (3着以内=1) の寄与
                bv = explainer.expected_value
                base_value = float(bv[1] if hasattr(bv, "__len__") else bv)
            else:
                shap_vals = raw
                base_value = float(explainer.expected_value)
        except Exception as e:
            logger.debug("SHAP computation failed: %s", e)
            return {}

        result: Dict[str, Dict[str, float]] = {}
        for i, hid in enumerate(ids):
            sv = shap_vals[i]  # shape: (len(feat_cols),)
            groups: Dict[str, float] = {"_base": base_value}
            assigned = set()
            for grp_name, grp_feats in SHAP_FEATURE_GROUPS.items():
                total = 0.0
                for feat_name in grp_feats:
                    if feat_name in feat_cols:
                        fi = feat_cols.index(feat_name)
                        sv_val = sv[fi]
                        if not (sv_val != sv_val):  # not NaN
                            total += float(sv_val)
                        assigned.add(fi)
                groups[grp_name] = round(total, 6)
            # 未分類特徴量は "その他" へ
            other = sum(
                float(sv[j]) for j in range(len(feat_cols))
                if j not in assigned and not (sv[j] != sv[j])
            )
            if abs(other) > 1e-6:
                groups["その他"] = round(other, 6)
            result[hid] = groups

        return result

    def get_sire_breakdowns(self, horses) -> Dict[str, dict]:
        """
        父馬の surface×SMILE 別複勝率を返す（見える化用）。
        Returns:
            {horse_id: {"surface": {...}, "smile": {...}}}
        """
        if not self._loaded and not self.load():
            return {}
        if self._sire_tracker is None:
            return {}
        result = {}
        for h in horses:
            sire_id = getattr(h, "sire_id", "") or ""
            if sire_id:
                result[h.horse_id] = self._sire_tracker.get_sire_breakdown(sire_id)
            else:
                result[h.horse_id] = {}
        return result
