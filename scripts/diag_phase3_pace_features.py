"""M-3 Phase 3: ペース・展開特徴量強化 WF 検証

新規ペース特徴量を追加した場合の ROI 改善を WF 6 ヶ月で評価する。

Phase 1 SHAP 結果:
  ペース・展開カテゴリ: 重要度 0.0587 (4.6%) ← 薄い
  popularity/odds: 0.0000 (0.0%) ← 無効

Phase 2 結果:
  +odds (Phase 2a): +5.32pt 改善 (採用確定)
  +odds+pace (本スクリプト): 追加 N pt 改善を期待

Variant 構成:
  baseline_new (108 特徴量)  : argmax(prob)
  +odds (109 特徴量)          : argmax(prob)  ← 基準
  +odds+pace (109+N 特徴量)   : argmax(prob), S3 (gap>=0.10)

新規ペース特徴量 (N=5):
  1. pace_type_encoded      : レースペース H=1.0/M=0.5/S=0.0
  2. style_pace_affinity_v3 : horse の脚質×今走ペースの相性スコア (改良版)
  3. front_runner_ratio_v3  : 逃げ先行馬の割合 (existing front_runner_count_in_race を正規化)
  4. pace_match_rate        : 前 N 走で同じペースタイプ出走時の複勝率
  5. dist_pace_interaction  : 距離×ペースの組合せスコア (長距離ハイペースが不利を表現)

重要な前提 (★★★):
  - src/ml/lgbm_model.py は絶対不変
  - ばんえい (venue_code="65") 除外 (feedback_banei_excluded.md)
  - git commit 禁止
  - 既存ファイル変更禁止
  - Phase 4/5 実施禁止

Usage:
  # フル実行 (6 ヶ月 WF / 約 30 分)
  python scripts/diag_phase3_pace_features.py

  # デバッグモード (1 月のみ)
  python scripts/diag_phase3_pace_features.py --debug

  # 特定月のみ
  python scripts/diag_phase3_pace_features.py --months 2025-12 2025-09

  # baseline_new をスキップ (高速化)
  python scripts/diag_phase3_pace_features.py --skip-baseline
"""

import argparse
import csv
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

import lightgbm as lgb
from sklearn.metrics import roc_auc_score

from src.log import get_logger
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

logger = get_logger(__name__)

# ============================================================
# 定数
# ============================================================

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DIAG_DIR = os.path.join(ROOT, "data", "_diag")

# 既存特徴量リスト
FEATURE_COLUMNS_BASELINE = list(FEATURE_COLUMNS)                        # 108 特徴量
FEATURE_COLUMNS_V2_ODDS  = list(FEATURE_COLUMNS) + ["odds"]             # 109 特徴量

# 新規ペース特徴量名 (N=5)
PACE_FEATURE_NAMES = [
    "pace_type_encoded",       # レースペース H=1.0/M=0.5/S=0.0
    "style_pace_affinity_v3",  # 脚質×ペース相性スコア
    "front_runner_ratio_v3",   # 逃げ先行馬の割合 (正規化)
    "pace_match_rate",         # 前 N 走での同ペースタイプ複勝率
    "dist_pace_interaction",   # 距離×ペース組合せスコア
]

FEATURE_COLUMNS_V3_PACE = list(FEATURE_COLUMNS) + ["odds"] + PACE_FEATURE_NAMES  # 109+5=114

# WF 検証月設定 (6 ヶ月サンプリング) — Phase 2c と同一構成
WF_VALID_MONTHS = [
    {"valid": "2024-09", "train_start": "2024-01", "train_end": "2024-08"},
    {"valid": "2024-12", "train_start": "2024-04", "train_end": "2024-11"},
    {"valid": "2025-03", "train_start": "2024-07", "train_end": "2025-02"},
    {"valid": "2025-06", "train_start": "2024-10", "train_end": "2025-05"},
    {"valid": "2025-09", "train_start": "2025-01", "train_end": "2025-08"},
    {"valid": "2025-12", "train_start": "2025-04", "train_end": "2025-11"},
]

# LightGBM 学習パラメータ — Phase 2c/2b'' と完全同一 (公正比較)
TRAIN_PARAMS = {
    "objective": "binary",
    "metric": "auc",
    "boosting_type": "gbdt",
    "num_leaves": 63,
    "learning_rate": 0.05,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "min_child_samples": 50,
    "lambda_l1": 0.1,
    "lambda_l2": 1.0,
    "verbose": -1,
    "seed": 42,
    "is_unbalance": True,
}

NUM_BOOST_ROUND     = 200
EARLY_STOPPING_ROUNDS = 20

# S3 戦略: gap >= 0.10 (Phase 5 で +2.79pt 改善確認済)
S3_GAP_THRESHOLD = 0.10

# ペースタイプ → 数値変換マップ
# race.get("pace") は H/M/S の文字列
PACE_ENCODE_MAP = {
    "H": 1.0,   # ハイペース
    "M": 0.5,   # ミドルペース
    "S": 0.0,   # スローペース
    # pace_norm ベースの既存 H/M/S グループも対応
    "ハイ":   1.0,
    "ミドル": 0.5,
    "スロー": 0.0,
}

# 距離カテゴリ境界 (m)
DIST_SHORT    = 1200   # 短距離境界
DIST_MIDDLE   = 2000   # 中距離境界
DIST_LONG_TH  = 2400   # 長距離境界


# ============================================================
# 新規ペース特徴量計算 (Phase 3 の核心)
# ============================================================

def _calc_pace_features(
    horse: dict,
    race: dict,
    feat_from_extract: dict,
) -> dict:
    """
    新規ペース・展開特徴量を計算して辞書で返す。

    Args:
        horse:             ML JSON の 1 頭分 dict
        race:              ML JSON の race dict
        feat_from_extract: _extract_features() の戻り値 (既存特徴量を参照するため)

    Returns:
        PACE_FEATURE_NAMES に対応する dict (値が取れない場合は None)

    実装した新規特徴量:
      1. pace_type_encoded      - レースペース H=1.0/M=0.5/S=0.0 (race.get("pace"))
      2. style_pace_affinity_v3 - horse の脚質×ペースの相性 (改良版)
                                   逃げ(0.0) × ハイペース → 不利 (-1.0)
                                   追込(1.0) × ハイペース → 有利 (+1.0)
                                   差 ± 中距離のスケーリング適用
      3. front_runner_ratio_v3  - 逃げ先行馬の割合を正規化
                                   existing front_runner_count_in_race / field_count
                                   ※ _add_race_relative_features 後に呼ぶ前提
                                   _extract_features のプレースホルダー値 (None) を使用
      4. pace_match_rate        - tracker から取得した実績ペース統計
                                   ハイペースなら place_rate_fast_pace, スローなら place_rate_slow_pace
                                   ミドルなら両者の平均
      5. dist_pace_interaction  - 距離×ペース相互作用
                                   長距離×ハイペース → 高リスク (負値)
                                   短距離×ハイペース → 不利なし (0)
                                   差し追込馬×長距離スローペース → 有利 (正値)
    """
    feat = {}

    # ────────────────────────────────────────────────────────────
    # 1. pace_type_encoded: レースペース H=1.0/M=0.5/S=0.0
    # ────────────────────────────────────────────────────────────
    raw_pace = race.get("pace") or ""
    pace_enc = PACE_ENCODE_MAP.get(str(raw_pace).upper(), None)
    # "H"/"M"/"S" 以外は None (信頼できないデータとして除外)
    if pace_enc is None and raw_pace:
        # 小文字も許容
        pace_enc = PACE_ENCODE_MAP.get(str(raw_pace).lower(), None)
    feat["pace_type_encoded"] = pace_enc

    # ────────────────────────────────────────────────────────────
    # 2. style_pace_affinity_v3: 脚質×ペース相性 (改良版)
    # ────────────────────────────────────────────────────────────
    # horse_running_style: 0.0=逃げ, 1.0=追込 (0〜1 の連続値)
    # 既存の style_pace_affinity は pace_pressure_index ベース (前走構成) だが
    # v3 は今走レース pace ベース: 逃げ(0)×ハイ → 不利, 追込(1)×ハイ → 有利
    horse_style = feat_from_extract.get("horse_running_style")
    if horse_style is not None and pace_enc is not None:
        # style: 0=逃げ → 低ペース有利, 1=追込 → 高ペース有利
        # affinity = (2*style - 1) × pace_enc:
        #   逃げ(0): -1.0×ペース_enc → ハイ時 -1.0, スロー時 0.0
        #   追込(1): +1.0×ペース_enc → ハイ時 +1.0, スロー時 0.0
        #   中間(0.5): 0.0 (ペース無関係)
        aff_v3 = (2.0 * float(horse_style) - 1.0) * pace_enc
        feat["style_pace_affinity_v3"] = aff_v3
    else:
        feat["style_pace_affinity_v3"] = None

    # ────────────────────────────────────────────────────────────
    # 3. front_runner_ratio_v3: 逃げ先行馬の割合 (正規化)
    # ────────────────────────────────────────────────────────────
    # _add_race_relative_features() が呼ばれる前はプレースホルダー (None) の場合がある
    # ここでは race 内の有効馬の horse_running_style 統計から直接計算する
    # ← 呼び出し元で race 内全馬の feat リストを渡せないため、race dict を使用
    # field_count から推定 (保守的: None の場合は 0.5 でフォールバック)
    field_count = race.get("field_count") or 0
    # front_runner_count_in_race は _add_race_relative_features 後に設定されるため
    # ここでは feat_from_extract 経由で取得 (None の可能性あり)
    front_cnt = feat_from_extract.get("front_runner_count_in_race")
    if front_cnt is not None and field_count > 0:
        feat["front_runner_ratio_v3"] = float(front_cnt) / float(field_count)
    else:
        feat["front_runner_ratio_v3"] = None

    # ────────────────────────────────────────────────────────────
    # 4. pace_match_rate: 前 N 走での同ペースタイプ複勝率
    # ────────────────────────────────────────────────────────────
    # tracker から既に計算済みの実績ペース統計を利用:
    #   place_rate_fast_pace: ハイペース時の複勝率 (直近15走)
    #   place_rate_slow_pace: スローペース時の複勝率 (直近15走)
    prf = feat_from_extract.get("place_rate_fast_pace")
    prs = feat_from_extract.get("place_rate_slow_pace")
    if pace_enc is not None:
        if pace_enc >= 0.7:  # ハイペース
            feat["pace_match_rate"] = prf  # ハイペース時の実績率
        elif pace_enc <= 0.3:  # スローペース
            feat["pace_match_rate"] = prs  # スローペース時の実績率
        else:  # ミドルペース
            if prf is not None and prs is not None:
                feat["pace_match_rate"] = (prf + prs) / 2.0
            elif prf is not None:
                feat["pace_match_rate"] = prf
            elif prs is not None:
                feat["pace_match_rate"] = prs
            else:
                feat["pace_match_rate"] = None
    else:
        feat["pace_match_rate"] = None

    # ────────────────────────────────────────────────────────────
    # 5. dist_pace_interaction: 距離×ペース相互作用
    # ────────────────────────────────────────────────────────────
    # 仮説:
    #   長距離×ハイペース = 逃げ馬には破滅的 (前崩れ)
    #   短距離×ハイペース = 問題なし
    #   差し/追込×長距離スロー = 末脚が活きない (不利)
    # 数値化:
    #   dist_norm = (distance - 1600) / 1000  [1200m=-0.4, 1600m=0, 2400m=0.8, 3000m=1.4]
    #   pace_centered = pace_enc - 0.5        [-0.5 (スロー) ~ +0.5 (ハイ)]
    #   dist_pace_int = dist_norm × pace_centered × style_factor
    #   style_factor = (1 - horse_style) → 逃げ(0)=1.0, 追込(1)=0.0
    #   → 長距離×ハイペース×逃げ = (大正値 × 0.5 × 1.0) = 大正値 → 不利方向
    #   → 長距離×スロー×追込 = (大正値 × -0.5 × 0.0) = 0 (追込なので無関係)
    #   意味: 「逃げ馬の距離×ペース適正負荷」
    distance = race.get("distance") or 1600
    if pace_enc is not None and horse_style is not None:
        dist_norm = (float(distance) - 1600.0) / 1000.0
        pace_centered = pace_enc - 0.5
        style_factor  = 1.0 - float(horse_style)  # 逃げ=1.0, 追込=0.0
        feat["dist_pace_interaction"] = dist_norm * pace_centered * style_factor
    else:
        feat["dist_pace_interaction"] = None

    return feat


# ============================================================
# _extract_features_v2: odds を追加 (Phase 2c と同じ)
# ============================================================

def _extract_features_v2(
    horse: dict,
    race: dict,
    tracker: RollingStatsTracker,
    sire_tracker: Optional[RollingSireTracker],
    include_odds: bool = True,
) -> dict:
    """既存 _extract_features をラップして odds を追加する"""
    feat = _extract_features(horse, race, tracker, sire_tracker)

    if include_odds:
        odds_val = horse.get("odds")
        if odds_val is None:
            odds_val = horse.get("tansho_odds")
        feat["odds"] = odds_val

    return feat


# ============================================================
# _extract_features_v3_pace: odds + 新規ペース特徴量を追加 (Phase 3)
# ============================================================

def _extract_features_v3_pace(
    horse: dict,
    race: dict,
    tracker: RollingStatsTracker,
    sire_tracker: Optional[RollingSireTracker],
    include_odds: bool = True,
    include_pace: bool = True,
) -> dict:
    """
    既存 _extract_features → odds → 新規ペース特徴量の順で拡張する。

    NOTE: _add_race_relative_features() 呼び出し後に pace 計算を行う
    ため、呼び出し元でこの関数を直接使わず _build_train_valid_data_v3 の
    ループ内で _add_race_relative_features 後に _calc_pace_features を
    呼ぶ設計とする。

    重要: src/ml/lgbm_model.py は絶対不変。
    """
    feat = _extract_features(horse, race, tracker, sire_tracker)

    if include_odds:
        odds_val = horse.get("odds") or horse.get("tansho_odds")
        feat["odds"] = odds_val

    # ペース特徴量は _add_race_relative_features 後に追記するため
    # ここでは初期化プレースホルダーのみ設定
    if include_pace:
        for pf in PACE_FEATURE_NAMES:
            feat.setdefault(pf, None)

    return feat


# ============================================================
# データ構築: 学習/検証 (variant 切り替え対応)
# ============================================================

def _build_train_valid_data(
    train_start_month: str,
    train_end_month: str,
    valid_month: str,
    feature_cols: List[str],
    all_races: list,
    sire_map: dict,
    include_odds: bool = True,
    include_pace: bool = False,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list]:
    """
    学習/検証データを構築する。

    Args:
        train_start_month: '2024-01' (inclusive)
        train_end_month:   '2024-08' (inclusive)
        valid_month:       '2024-09'
        feature_cols:      使用する特徴量列
        all_races:         _load_ml_races() の戻り値
        sire_map:          _load_horse_sire_map() の戻り値
        include_odds:      True なら odds を追加
        include_pace:      True なら Phase 3 ペース特徴量を追加

    Returns:
        X_train, y_train, X_valid, y_valid, valid_races_info
    """
    train_start = f"{train_start_month}-01"
    train_end   = f"{valid_month}-01"    # 検証月の 01 = 学習の上限 (exclusive)

    # 検証月の翌月 (valid_end) を計算
    valid_y, valid_m = int(valid_month[:4]), int(valid_month[5:7])
    if valid_m == 12:
        valid_y += 1
        valid_m = 1
    else:
        valid_m += 1
    valid_end = f"{valid_y:04d}-{valid_m:02d}-01"

    pace_label = "+pace" if include_pace else ""
    logger.info(f"    学習期間: {train_start} 〜 {train_end} (exclusive)")
    logger.info(f"    検証期間: {valid_month} ({train_end} 〜 {valid_end})")
    logger.info(f"    特徴量数: {len(feature_cols)}, odds={include_odds}{pace_label}")

    tracker      = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    # pre-warmup: ばんえい (venue_code="65") 除外
    pre_warmup_races = [
        r for r in all_races
        if r.get("date", "") < train_start
        and str(r.get("venue_code", "")) != "65"
    ]
    logger.info(f"    tracker pre-warmup: {len(pre_warmup_races)} レース (< {train_start}, ばんえい除外済)")
    t0 = time.time()
    for i, race in enumerate(pre_warmup_races):
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)
        if (i + 1) % 10000 == 0:
            pct = (i + 1) / len(pre_warmup_races) * 100
            logger.info(f"      pre-warmup: {i+1}/{len(pre_warmup_races)} ({pct:.0f}%)")
    logger.info(f"    pre-warmup 完了: {time.time()-t0:.1f}秒")

    # 対象レース収集 (ばんえい除外)
    target_races = [
        r for r in all_races
        if train_start <= r.get("date", "") < valid_end
        and str(r.get("venue_code", "")) != "65"
    ]
    logger.info(f"    対象レース数 (学習+検証, ばんえい除外済): {len(target_races)}")

    train_feats, train_labels = [], []
    valid_races_info = []  # (race_dict, horse_dicts, labels, raw_feats)

    t1 = time.time()
    for i, race in enumerate(target_races):
        d = race.get("date", "")
        is_valid = d >= train_end  # 検証月以降 = 検証データ

        r_feats, r_labels, r_horse_dicts = [], [], []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            hd = dict(h, sire_id=sid, bms_id=bid)
            feat = _extract_features_v3_pace(
                hd, race, tracker, sire_tracker,
                include_odds=include_odds,
                include_pace=include_pace,
            )
            r_feats.append(feat)
            r_labels.append(1 if fp <= 3 else 0)
            r_horse_dicts.append(hd)

        if r_feats:
            # Step 1: _add_race_relative_features で front_runner_count 等を設定
            _add_race_relative_features(r_feats)

            # Step 2: include_pace が True なら pace 特徴量を追記
            if include_pace:
                for j, (hd, feat) in enumerate(zip(r_horse_dicts, r_feats)):
                    pace_feats = _calc_pace_features(hd, race, feat)
                    feat.update(pace_feats)

            if is_valid:
                valid_races_info.append((race, r_horse_dicts, r_labels, r_feats))
            else:
                train_feats.extend(r_feats)
                train_labels.extend(r_labels)

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

        if (i + 1) % 2000 == 0:
            pct = (i + 1) / len(target_races) * 100
            elapsed = time.time() - t1
            logger.info(f"      データ収集: {i+1}/{len(target_races)} ({pct:.0f}%) {elapsed:.1f}秒")

    logger.info(f"    データ収集完了: {time.time()-t1:.1f}秒")
    logger.info(f"    学習サンプル数: {len(train_labels)}, 検証レース数: {len(valid_races_info)}")

    def _to_np(rows: list, cols: List[str]) -> np.ndarray:
        mat = []
        for f in rows:
            row = []
            for c in cols:
                v = f.get(c) if isinstance(f, dict) else None
                row.append(float(v) if v is not None else float("nan"))
            mat.append(row)
        return np.array(mat, dtype=np.float32)

    X_train = _to_np(train_feats, feature_cols)
    y_train = np.array(train_labels, dtype=np.int32)

    valid_feats_flat: list = []
    valid_labels_flat: list = []
    for _, _, r_labels, r_feats in valid_races_info:
        valid_feats_flat.extend(r_feats)
        valid_labels_flat.extend(r_labels)

    X_valid = _to_np(valid_feats_flat, feature_cols)
    y_valid = np.array(valid_labels_flat, dtype=np.int32)

    nan_rate = np.isnan(X_train).mean()
    logger.info(f"    X_train shape: {X_train.shape}, NaN率: {nan_rate:.3f}")
    logger.info(f"    X_valid shape: {X_valid.shape}")
    logger.info(f"    y_train 正例率: {y_train.mean():.3f}, y_valid 正例率: {y_valid.mean():.3f}")

    # ペース特徴量の非 NaN 率を確認 (デバッグ用)
    if include_pace and len(valid_feats_flat) > 0:
        X_pace = _to_np(valid_feats_flat, PACE_FEATURE_NAMES)
        for k, pf in enumerate(PACE_FEATURE_NAMES):
            non_nan = np.sum(~np.isnan(X_pace[:, k]))
            pct = non_nan / len(valid_feats_flat) * 100
            logger.info(f"    [pace特徴量] {pf}: 有効率={pct:.1f}% ({non_nan}/{len(valid_feats_flat)})")

    return X_train, y_train, X_valid, y_valid, valid_races_info


# ============================================================
# モデル学習 (Phase 2c と同じ設定)
# ============================================================

def _train_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    feature_cols: List[str],
    variant_name: str,
) -> lgb.Booster:
    """LightGBM モデルを学習して返す (Phase 2c と同じ設定)"""
    logger.info(f"    [学習] variant={variant_name}, 特徴量数={len(feature_cols)}")

    cat_feats = [c for c in CATEGORICAL_FEATURES if c in feature_cols]

    dtrain = lgb.Dataset(
        X_train, label=y_train,
        feature_name=feature_cols,
        categorical_feature=cat_feats if cat_feats else "auto",
        free_raw_data=False,
    )
    dvalid = lgb.Dataset(
        X_valid, label=y_valid,
        feature_name=feature_cols,
        categorical_feature=cat_feats if cat_feats else "auto",
        reference=dtrain,
        free_raw_data=False,
    )

    params = dict(TRAIN_PARAMS)
    callbacks = [
        lgb.early_stopping(stopping_rounds=EARLY_STOPPING_ROUNDS, verbose=False),
        lgb.log_evaluation(period=50),
    ]

    t0 = time.time()
    booster = lgb.train(
        params, dtrain,
        num_boost_round=NUM_BOOST_ROUND,
        valid_sets=[dvalid],
        valid_names=["valid"],
        callbacks=callbacks,
    )
    elapsed = time.time() - t0
    best_iter = booster.best_iteration
    best_auc  = booster.best_score.get("valid", {}).get("auc", float("nan"))
    logger.info(f"    学習完了: {elapsed:.1f}秒, best_iter={best_iter}, valid AUC={best_auc:.4f}")

    return booster


# ============================================================
# 通常評価 (argmax(prob))
# ============================================================

def _evaluate_variant(
    booster: lgb.Booster,
    valid_races_info: list,
    feature_cols: List[str],
    variant_name: str,
    gap_threshold: Optional[float] = None,
) -> dict:
    """
    評価: argmax(prob) で ◎ を選定して ROI 評価。

    gap_threshold が指定された場合は S3 戦略:
      上位 2 頭の prob の差 >= gap_threshold の場合のみ買い。

    Returns:
        dict with variant, strategy, auc, top1_hit_pct, tansho_roi_pct, played, hit, payout_sum
    """
    strategy = "S3_gap010" if gap_threshold is not None else "argmax_prob"
    logger.info(f"    [評価] {variant_name} / strategy={strategy}")

    def _to_np(rows: list, cols: List[str]) -> np.ndarray:
        mat = []
        for f in rows:
            row = []
            for c in cols:
                v = f.get(c) if isinstance(f, dict) else None
                row.append(float(v) if v is not None else float("nan"))
            mat.append(row)
        return np.array(mat, dtype=np.float32)

    all_y_true: list = []
    all_y_pred: list = []
    played = 0
    hit    = 0
    payout_sum = 0.0
    skip_by_gap = 0

    for race, horse_dicts, r_labels, r_feats in valid_races_info:
        if not r_feats:
            continue

        X = _to_np(r_feats, feature_cols)
        raw_preds = booster.predict(X)

        all_y_true.extend(r_labels)
        all_y_pred.extend(raw_preds.tolist())

        if len(raw_preds) == 0:
            continue

        # S3 戦略: gap フィルタ
        if gap_threshold is not None and len(raw_preds) >= 2:
            sorted_preds = np.sort(raw_preds)[::-1]
            gap = sorted_preds[0] - sorted_preds[1]
            if gap < gap_threshold:
                skip_by_gap += 1
                continue

        top_idx = int(np.argmax(raw_preds))
        top_horse = horse_dicts[top_idx]

        top_finish = top_horse.get("finish_pos")
        is_win = (top_finish is not None and top_finish == 1)

        odds_val = top_horse.get("odds") or top_horse.get("tansho_odds")
        tansho_odds = 0.0
        if odds_val is not None:
            try:
                tansho_odds = float(odds_val)
            except (TypeError, ValueError):
                tansho_odds = 0.0

        if tansho_odds <= 0:
            continue

        played += 1
        if is_win:
            hit += 1
            payout_sum += tansho_odds * 100

    auc = float("nan")
    if len(set(all_y_true)) == 2:
        try:
            auc = roc_auc_score(all_y_true, all_y_pred)
        except Exception as e:
            logger.warning(f"    AUC 計算エラー: {e}")

    top1_hit_pct    = (hit / played * 100) if played > 0 else 0.0
    tansho_roi_pct  = (payout_sum / (played * 100) * 100) if played > 0 else 0.0

    logger.info(f"    AUC={auc:.4f}, hit%={top1_hit_pct:.2f}%, ROI={tansho_roi_pct:.2f}%")
    logger.info(f"    played={played}, hit={hit}" + (f", skip_by_gap={skip_by_gap}" if gap_threshold else ""))

    return {
        "variant": variant_name,
        "strategy": strategy,
        "auc": auc,
        "top1_hit_pct": top1_hit_pct,
        "tansho_roi_pct": tansho_roi_pct,
        "played": played,
        "hit": hit,
        "payout_sum": payout_sum,
    }


# ============================================================
# 月別・全期間 出力
# ============================================================

def _output_monthly_results(results_monthly: List[dict]) -> str:
    """月別結果を stdout + CSV に出力する"""
    os.makedirs(DIAG_DIR, exist_ok=True)
    csv_path = os.path.join(DIAG_DIR, "phase3_pace_monthly.csv")

    print()
    print("=" * 110)
    print("【Phase 3 ペース特徴量 WF 月別結果】")
    print("=" * 110)
    header = (
        f"{'月':<12} {'基準 ROI':>10} {'+odds ROI':>10} {'+pace ROI':>10} {'Δ pace-odds':>12} "
        f"{'S3 ROI':>10} {'Δ S3-odds':>12} | {'played+odds':>12}"
    )
    print(header)
    print("-" * 110)

    for r in results_monthly:
        m    = r.get("valid_month", "?")
        br   = r.get("baseline_roi",    0.0)
        or_  = r.get("odds_roi",        0.0)
        pr   = r.get("pace_roi",        0.0)
        s3r  = r.get("pace_s3_roi",     0.0)
        op   = r.get("odds_played",     0)
        dp   = pr - or_
        ds3  = s3r - or_
        print(
            f"{m:<12} {br:>9.2f}% {or_:>9.2f}% {pr:>9.2f}% {dp:>+11.2f}pt "
            f"{s3r:>9.2f}% {ds3:>+11.2f}pt | {op:>11}"
        )

    print("-" * 110)

    # CSV 保存
    fieldnames = [
        "valid_month",
        "variant", "strategy",
        "played", "hit",
        "hit_pct", "roi_pct",
        "payout_sum", "auc",
    ]
    rows_to_write = []
    for r in results_monthly:
        month = r.get("valid_month", "")
        for key in ("baseline", "odds", "pace_argmax", "pace_s3"):
            rows_to_write.append({
                "valid_month": month,
                "variant":    r.get(f"{key}_variant",  ""),
                "strategy":   r.get(f"{key}_strategy", ""),
                "played":     r.get(f"{key}_played",   0),
                "hit":        r.get(f"{key}_hit",      0),
                "hit_pct":    f"{r.get(f'{key}_hit_pct', 0.0):.4f}",
                "roi_pct":    f"{r.get(f'{key}_roi',    0.0):.4f}",
                "payout_sum": f"{r.get(f'{key}_payout', 0.0):.0f}",
                "auc":        f"{r.get(f'{key}_auc', float('nan')):.6f}",
            })

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_to_write)

    logger.info(f"    月別 CSV 保存: {csv_path}")
    return csv_path


def _output_summary(results_monthly: List[dict], skip_baseline: bool) -> str:
    """全期間集計結果を stdout + CSV に出力する"""
    os.makedirs(DIAG_DIR, exist_ok=True)
    csv_path = os.path.join(DIAG_DIR, "phase3_pace_summary.csv")

    if not results_monthly:
        logger.warning("    集計対象データなし")
        return csv_path

    # 集計 (played 加重集約)
    def _agg(key_prefix: str):
        total_played  = sum(r.get(f"{key_prefix}_played",  0)   for r in results_monthly)
        total_hit     = sum(r.get(f"{key_prefix}_hit",     0)   for r in results_monthly)
        total_payout  = sum(r.get(f"{key_prefix}_payout",  0.0) for r in results_monthly)
        rois          = [r.get(f"{key_prefix}_roi",        0.0) for r in results_monthly]
        roi_w = (total_payout / (total_played * 100) * 100) if total_played > 0 else 0.0
        hit_w = (total_hit    / total_played         * 100) if total_played > 0 else 0.0
        roi_mean = float(np.mean(rois)) if rois else 0.0
        return {
            "played_total": total_played,
            "hit_total":    total_hit,
            "hit_pct_w":    hit_w,
            "roi_w":        roi_w,
            "roi_mean":     roi_mean,
        }

    agg_baseline  = _agg("baseline") if not skip_baseline else {}
    agg_odds      = _agg("odds")
    agg_pace      = _agg("pace_argmax")
    agg_pace_s3   = _agg("pace_s3")

    print()
    print("=" * 90)
    print("【Phase 3 ペース特徴量 WF 全期間集計】")
    print("=" * 90)
    print(f"  新規ペース特徴量 ({len(PACE_FEATURE_NAMES)} 個): {', '.join(PACE_FEATURE_NAMES)}")
    print()

    rows = []
    variants_info = []
    if not skip_baseline:
        variants_info.append(("baseline_new (108)",   agg_baseline, "baseline"))
    variants_info.append(("+odds (109) [基準]",       agg_odds,    "odds"))
    variants_info.append((f"+odds+pace ({len(FEATURE_COLUMNS_V3_PACE)}) argmax", agg_pace, "pace_argmax"))
    variants_info.append((f"+odds+pace ({len(FEATURE_COLUMNS_V3_PACE)}) S3_gap010", agg_pace_s3, "pace_s3"))

    print(f"  {'Variant':<42} {'hit%(W)':>9} {'ROI%(W)':>9} {'Δ vs +odds':>12} {'played':>8}")
    print(f"  {'-'*42} {'-'*9} {'-'*9} {'-'*12} {'-'*8}")

    odds_roi_w = agg_odds.get("roi_w", 0.0)
    for label, agg, key in variants_info:
        roi_w   = agg.get("roi_w", 0.0)
        hit_w   = agg.get("hit_pct_w", 0.0)
        played  = agg.get("played_total", 0)
        delta   = roi_w - odds_roi_w if key != "baseline" else float("nan")
        delta_s = f"{delta:+.2f}pt" if not np.isnan(delta) else "  基準外"
        print(f"  {label:<42} {hit_w:>8.2f}% {roi_w:>8.2f}% {delta_s:>12} {played:>8}")
        rows.append({
            "variant":         label,
            "strategy":        "S3_gap010" if "S3" in label else "argmax_prob",
            "played_total":    played,
            "hit_pct_weighted": f"{hit_w:.4f}",
            "roi_pct_weighted": f"{roi_w:.4f}",
            "delta_vs_odds_pt": f"{delta:+.2f}" if not np.isnan(delta) else "N/A",
        })

    print()
    print("=" * 90)

    # 改善判定
    delta_argmax = agg_pace.get("roi_w", 0.0) - odds_roi_w
    delta_s3     = agg_pace_s3.get("roi_w", 0.0) - odds_roi_w

    print()
    print("【Phase 3 改善判定】")
    print(f"  +odds+pace (argmax) ΔROI: {delta_argmax:+.2f}pt vs +odds")
    print(f"  +odds+pace (S3)     ΔROI: {delta_s3:+.2f}pt vs +odds")
    print()

    for delta, label in [(delta_argmax, "argmax"), (delta_s3, "S3")]:
        if delta >= 5.0:
            j = f"✅ {label}: +5pt 以上改善 → Phase 3 ペース特徴量 採用価値高い"
        elif delta >= 1.0:
            j = f"⚠️ {label}: +1〜5pt 改善 → 部分採用・追加特徴量検討"
        elif delta >= -1.0:
            j = f"❌ {label}: ±1pt 以内 → 効果なし → 特徴量見直し or Phase 4/5 へ"
        else:
            j = f"❌❌ {label}: 悪化 → ペース特徴量が過学習リスク → 採用不可"
        print(f"  判定: {j}")

    print()
    # マスター基準: hit% 25%+ AND ROI 110%+
    pace_roi = agg_pace.get("roi_w", 0.0)
    pace_hit = agg_pace.get("hit_pct_w", 0.0)
    s3_roi   = agg_pace_s3.get("roi_w", 0.0)
    s3_hit   = agg_pace_s3.get("hit_pct_w", 0.0)
    target_roi = 110.0
    target_hit = 25.0
    gap_roi_argmax = pace_roi - target_roi
    gap_roi_s3     = s3_roi - target_roi
    print(f"  マスター基準 (hit%>={target_hit}% AND ROI>={target_roi}%):")
    print(f"    +pace argmax: hit={pace_hit:.2f}%, ROI={pace_roi:.2f}% → ROI gap={gap_roi_argmax:+.2f}pt")
    print(f"    +pace S3:     hit={s3_hit:.2f}%, ROI={s3_roi:.2f}%   → ROI gap={gap_roi_s3:+.2f}pt")
    print("=" * 90)

    # CSV 保存
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"    集計 CSV 保存: {csv_path}")
    return csv_path


# ============================================================
# メイン
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 3: ペース・展開特徴量強化 WF 検証 — "
            f"新規 {len(PACE_FEATURE_NAMES)} 個のペース特徴量を追加して ROI 改善を評価"
        )
    )
    parser.add_argument(
        "--months",
        nargs="+",
        metavar="YYYY-MM",
        help="実行する検証月を指定 (例: --months 2025-12 2025-09)。省略時は全 6 ヶ月",
        default=None,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="デバッグモード (1 月のみ: 2025-12)",
    )
    parser.add_argument(
        "--skip-baseline",
        action="store_true",
        help="baseline_new (108 特徴量) の計算をスキップして高速化",
    )
    args = parser.parse_args()

    start_time = time.time()

    logger.info("=" * 70)
    logger.info("[Phase 3 ペース特徴量 WF 検証] 開始")
    logger.info(f"  新規ペース特徴量 ({len(PACE_FEATURE_NAMES)} 個):")
    for pf in PACE_FEATURE_NAMES:
        logger.info(f"    - {pf}")
    logger.info(f"  特徴量数: baseline={len(FEATURE_COLUMNS_BASELINE)}, "
                f"+odds={len(FEATURE_COLUMNS_V2_ODDS)}, "
                f"+odds+pace={len(FEATURE_COLUMNS_V3_PACE)}")
    logger.info(f"  S3 閾値: gap>={S3_GAP_THRESHOLD}")
    logger.info(f"  検証月: {[c['valid'] for c in WF_VALID_MONTHS]}")
    logger.info("=" * 70)

    # 実行月フィルタ
    wf_configs = WF_VALID_MONTHS
    if args.debug:
        wf_configs = [c for c in WF_VALID_MONTHS if c["valid"] == "2025-12"]
        logger.info("  デバッグモード: 2025-12 のみ実行")
    elif args.months:
        wf_configs = [c for c in WF_VALID_MONTHS if c["valid"] in args.months]
        if not wf_configs:
            logger.error(f"  指定月 {args.months} が WF 設定に見つかりません")
            sys.exit(1)
        logger.info(f"  指定月フィルタ: {[c['valid'] for c in wf_configs]}")

    skip_baseline = args.skip_baseline
    if skip_baseline:
        logger.info("  --skip-baseline: baseline_new は計算スキップ")

    # ──────────────────────────────────────────────────────────
    # 全レース + 種牡馬マップ読み込み (全月共通 / 1 回のみ)
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("[データ読み込み] _load_ml_races() + _load_horse_sire_map()...")
    t0 = time.time()
    sire_map  = _load_horse_sire_map()
    all_races = _load_ml_races()
    logger.info(f"  全レース数: {len(all_races)}, 所要時間: {time.time()-t0:.1f}秒")

    if not all_races:
        logger.error("  ❌ レースデータが空です。処理中断。")
        sys.exit(1)

    results_monthly: List[dict] = []

    # ──────────────────────────────────────────────────────────
    # 月別 WF ループ
    # ──────────────────────────────────────────────────────────
    total_months = len(wf_configs)
    for month_idx, month_cfg in enumerate(wf_configs):
        valid_month = month_cfg["valid"]
        train_start = month_cfg["train_start"]
        train_end   = month_cfg["train_end"]

        elapsed_total = time.time() - start_time
        pct = month_idx / total_months * 100
        bar = "█" * (month_idx * 20 // total_months) + "░" * (20 - month_idx * 20 // total_months)
        logger.info("")
        logger.info(f"[{bar}] {pct:.0f}% — 月 {month_idx+1}/{total_months}: valid={valid_month} / elapsed={elapsed_total:.0f}秒")
        logger.info(f"  学習期間: {train_start} 〜 {train_end}")
        logger.info("=" * 70)

        month_result: dict = {"valid_month": valid_month}

        # ────────────────────────────────────────────────
        # A. baseline_new (108 特徴量) — skip_baseline でスキップ可
        # ────────────────────────────────────────────────
        r_baseline = None
        if not skip_baseline:
            logger.info("  [Step A] baseline_new データ構築 (108 特徴量)...")
            t_step = time.time()
            try:
                Xtr_b, ytr_b, Xv_b, yv_b, vri_b = _build_train_valid_data(
                    train_start_month=train_start,
                    train_end_month=train_end,
                    valid_month=valid_month,
                    feature_cols=FEATURE_COLUMNS_BASELINE,
                    all_races=all_races,
                    sire_map=sire_map,
                    include_odds=False,
                    include_pace=False,
                )
                bst_b = _train_model(Xtr_b, ytr_b, Xv_b, yv_b, FEATURE_COLUMNS_BASELINE, f"baseline_{valid_month}")
                r_baseline = _evaluate_variant(bst_b, vri_b, FEATURE_COLUMNS_BASELINE, "baseline_new")
                logger.info(f"  baseline_new 完了: {time.time()-t_step:.1f}秒")
            except Exception as e:
                logger.error(f"  ❌ baseline_new エラー (valid={valid_month}): {e}")
                import traceback
                traceback.print_exc()

        # ────────────────────────────────────────────────
        # B. +odds (109 特徴量) — 基準
        # ────────────────────────────────────────────────
        logger.info("  [Step B] +odds データ構築 (109 特徴量)...")
        t_step = time.time()
        try:
            Xtr_o, ytr_o, Xv_o, yv_o, vri_o = _build_train_valid_data(
                train_start_month=train_start,
                train_end_month=train_end,
                valid_month=valid_month,
                feature_cols=FEATURE_COLUMNS_V2_ODDS,
                all_races=all_races,
                sire_map=sire_map,
                include_odds=True,
                include_pace=False,
            )
        except Exception as e:
            logger.error(f"  ❌ +odds データ構築エラー (valid={valid_month}): {e}")
            import traceback
            traceback.print_exc()
            continue

        if len(ytr_o) == 0 or len(vri_o) == 0:
            logger.warning(f"  ⚠️ valid={valid_month}: +odds 学習/検証データが空。スキップ")
            continue

        bst_o = _train_model(Xtr_o, ytr_o, Xv_o, yv_o, FEATURE_COLUMNS_V2_ODDS, f"+odds_{valid_month}")
        r_odds = _evaluate_variant(bst_o, vri_o, FEATURE_COLUMNS_V2_ODDS, "+odds")
        logger.info(f"  +odds 完了: {time.time()-t_step:.1f}秒")

        # ────────────────────────────────────────────────
        # C. +odds+pace (109+5=114 特徴量)
        # ────────────────────────────────────────────────
        logger.info(f"  [Step C] +odds+pace データ構築 ({len(FEATURE_COLUMNS_V3_PACE)} 特徴量)...")
        t_step = time.time()
        try:
            Xtr_p, ytr_p, Xv_p, yv_p, vri_p = _build_train_valid_data(
                train_start_month=train_start,
                train_end_month=train_end,
                valid_month=valid_month,
                feature_cols=FEATURE_COLUMNS_V3_PACE,
                all_races=all_races,
                sire_map=sire_map,
                include_odds=True,
                include_pace=True,
            )
        except Exception as e:
            logger.error(f"  ❌ +pace データ構築エラー (valid={valid_month}): {e}")
            import traceback
            traceback.print_exc()
            continue

        if len(ytr_p) == 0 or len(vri_p) == 0:
            logger.warning(f"  ⚠️ valid={valid_month}: +pace 学習/検証データが空。スキップ")
            continue

        bst_p = _train_model(Xtr_p, ytr_p, Xv_p, yv_p, FEATURE_COLUMNS_V3_PACE, f"+pace_{valid_month}")
        # C-1: argmax(prob)
        r_pace_argmax = _evaluate_variant(bst_p, vri_p, FEATURE_COLUMNS_V3_PACE, "+odds+pace", gap_threshold=None)
        # C-2: S3 (gap >= 0.10)
        r_pace_s3     = _evaluate_variant(bst_p, vri_p, FEATURE_COLUMNS_V3_PACE, "+odds+pace", gap_threshold=S3_GAP_THRESHOLD)
        logger.info(f"  +odds+pace 完了: {time.time()-t_step:.1f}秒")

        # 月別結果を蓄積
        # baseline
        if r_baseline:
            month_result.update({
                "baseline_variant":   r_baseline["variant"],
                "baseline_strategy":  r_baseline["strategy"],
                "baseline_played":    r_baseline["played"],
                "baseline_hit":       r_baseline["hit"],
                "baseline_hit_pct":   r_baseline["top1_hit_pct"],
                "baseline_roi":       r_baseline["tansho_roi_pct"],
                "baseline_payout":    r_baseline["payout_sum"],
                "baseline_auc":       r_baseline["auc"],
            })
        else:
            month_result.update({
                "baseline_variant": "skipped", "baseline_strategy": "",
                "baseline_played": 0, "baseline_hit": 0,
                "baseline_hit_pct": 0.0, "baseline_roi": 0.0,
                "baseline_payout": 0.0, "baseline_auc": float("nan"),
            })

        # +odds
        month_result.update({
            "odds_variant":   r_odds["variant"],
            "odds_strategy":  r_odds["strategy"],
            "odds_played":    r_odds["played"],
            "odds_hit":       r_odds["hit"],
            "odds_hit_pct":   r_odds["top1_hit_pct"],
            "odds_roi":       r_odds["tansho_roi_pct"],
            "odds_payout":    r_odds["payout_sum"],
            "odds_auc":       r_odds["auc"],
        })

        # +pace argmax
        month_result.update({
            "pace_argmax_variant":  r_pace_argmax["variant"],
            "pace_argmax_strategy": r_pace_argmax["strategy"],
            "pace_argmax_played":   r_pace_argmax["played"],
            "pace_argmax_hit":      r_pace_argmax["hit"],
            "pace_argmax_hit_pct":  r_pace_argmax["top1_hit_pct"],
            "pace_argmax_roi":      r_pace_argmax["tansho_roi_pct"],
            "pace_argmax_payout":   r_pace_argmax["payout_sum"],
            "pace_argmax_auc":      r_pace_argmax["auc"],
            # summary 集計 用エイリアス
            "pace_argmax_played":   r_pace_argmax["played"],
            "pace_argmax_hit":      r_pace_argmax["hit"],
        })

        # +pace S3
        month_result.update({
            "pace_s3_variant":  r_pace_s3["variant"],
            "pace_s3_strategy": r_pace_s3["strategy"],
            "pace_s3_played":   r_pace_s3["played"],
            "pace_s3_hit":      r_pace_s3["hit"],
            "pace_s3_hit_pct":  r_pace_s3["top1_hit_pct"],
            "pace_s3_roi":      r_pace_s3["tansho_roi_pct"],
            "pace_s3_payout":   r_pace_s3["payout_sum"],
            "pace_s3_auc":      r_pace_s3["auc"],
        })

        results_monthly.append(month_result)

        # 月別中間サマリ
        d_pace = r_pace_argmax["tansho_roi_pct"] - r_odds["tansho_roi_pct"]
        d_s3   = r_pace_s3["tansho_roi_pct"]     - r_odds["tansho_roi_pct"]
        logger.info(
            f"  ✅ {valid_month} 完了: "
            f"+odds={r_odds['tansho_roi_pct']:.2f}% → "
            f"+pace={r_pace_argmax['tansho_roi_pct']:.2f}% (Δ{d_pace:+.2f}) / "
            f"S3={r_pace_s3['tansho_roi_pct']:.2f}% (Δ{d_s3:+.2f})"
        )

    # ──────────────────────────────────────────────────────────
    # 全月完了後: 結果出力
    # ──────────────────────────────────────────────────────────
    if not results_monthly:
        logger.error("❌ 有効な月別結果がありません。CSV 出力スキップ。")
        sys.exit(1)

    logger.info("")
    logger.info("[出力] 月別結果 + 全期間集計...")
    csv_monthly = _output_monthly_results(results_monthly)
    csv_summary = _output_summary(results_monthly, skip_baseline)

    total_elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print("【Phase 3 ペース特徴量 WF 検証 完了】")
    print(f"  合計所要時間: {total_elapsed:.1f}秒 ({total_elapsed/60:.1f}分)")
    print(f"  月別 CSV: {csv_monthly}")
    print(f"  集計 CSV: {csv_summary}")
    print(f"  新規ペース特徴量: {PACE_FEATURE_NAMES}")
    print("=" * 70)

    logger.info("[Phase 3 ペース特徴量 WF 検証] 完了")


if __name__ == "__main__":
    main()
