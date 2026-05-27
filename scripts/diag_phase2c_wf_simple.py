"""M-3 Phase 2c: WF 全期間検証 (EV_oddsmax10)

複数の検証月で +odds + EV_oddsmax10 戦略を WF rolling で評価する。

Phase 2b'' 結果:
  +odds+EV_oddsmax10: hit%=14.75%, ROI=85.96% (+6.90pt vs +odds)
  検証期間: 2025-12 のみ → 本スクリプトで 6 ヶ月 WF 検証

WF 検証月:
  2024-09: 学習 2024-01〜08 (8ヶ月)
  2024-12: 学習 2024-04〜11 (8ヶ月)
  2025-03: 学習 2024-07〜2025-02 (8ヶ月)
  2025-06: 学習 2024-10〜2025-05 (8ヶ月)
  2025-09: 学習 2025-01〜08 (8ヶ月)
  2025-12: 学習 2025-04〜11 (8ヶ月) ← Phase 2b'' と同一

重要な前提 (★★★):
  - src/ml/lgbm_model.py は絶対不変
  - ばんえい (venue_65) 除外 (feedback_banei_excluded.md)
  - git commit 禁止
  - 既存ファイル変更禁止
  - Phase 3/4/5 実施禁止

Usage:
  # フル実行 (6 ヶ月 WF / 約 25 分)
  python scripts/diag_phase2c_wf_simple.py

  # --help
  python scripts/diag_phase2c_wf_simple.py --help

  # デバッグモード (1 月のみ)
  python scripts/diag_phase2c_wf_simple.py --debug

  # 特定月のみ
  python scripts/diag_phase2c_wf_simple.py --months 2025-12 2025-09
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

# FEATURE_COLUMNS_V2_ODDS (108 + odds = 109) — Phase 2b'' と同じ
FEATURE_COLUMNS_V2_ODDS = list(FEATURE_COLUMNS) + ["odds"]

# WF 検証月設定 (6 ヶ月サンプリング)
# valid: 検証月, train_start: 学習開始月 (inclusive), train_end: 学習終了月 (inclusive, 検証月の前月)
WF_VALID_MONTHS = [
    {"valid": "2024-09", "train_start": "2024-01", "train_end": "2024-08"},
    {"valid": "2024-12", "train_start": "2024-04", "train_end": "2024-11"},
    {"valid": "2025-03", "train_start": "2024-07", "train_end": "2025-02"},
    {"valid": "2025-06", "train_start": "2024-10", "train_end": "2025-05"},
    {"valid": "2025-09", "train_start": "2025-01", "train_end": "2025-08"},
    {"valid": "2025-12", "train_start": "2025-04", "train_end": "2025-11"},
]

# LightGBM 学習パラメータ — Phase 2b'' と完全に同じ設定 (公正比較)
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

NUM_BOOST_ROUND = 200
EARLY_STOPPING_ROUNDS = 20

# EV_oddsmax10 の制約 — Phase 2b'' で最優秀の設定
EV_ODDS_CAP = 10.0


# ============================================================
# _extract_features_v2: odds を追加するラッパー (Phase 2b'' と同じ)
# ============================================================

def _extract_features_v2(
    horse: dict,
    race: dict,
    tracker: RollingStatsTracker,
    sire_tracker: Optional[RollingSireTracker],
    include_odds: bool = True,
) -> dict:
    """
    既存 _extract_features をラップして odds を追加する。

    重要: src/ml/lgbm_model.py は絶対不変。
    このラッパーは scripts/ 内にのみ存在する。
    """
    feat = _extract_features(horse, race, tracker, sire_tracker)

    if include_odds:
        # odds / tansho_odds どちらでも受け付ける
        odds_val = horse.get("odds")
        if odds_val is None:
            odds_val = horse.get("tansho_odds")
        feat["odds"] = odds_val

    return feat


# ============================================================
# データ構築: 学習/検証 (Phase 2b'' と同じロジック)
# ============================================================

def _build_train_valid_data(
    train_start_month: str,
    train_end_month: str,
    valid_month: str,
    feature_cols: List[str],
    all_races: list,
    sire_map: dict,
    include_odds: bool = True,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list]:
    """
    学習/検証データを構築する。

    Args:
        train_start_month: '2024-01' (inclusive)
        train_end_month:   '2024-08' (inclusive / 検証月の前月)
        valid_month:       '2024-09'
        feature_cols:      使用する特徴量列
        all_races:         _load_ml_races() の戻り値
        sire_map:          _load_horse_sire_map() の戻り値
        include_odds:      True なら odds を追加

    Returns:
        X_train, y_train, X_valid, y_valid, valid_races_info
        valid_races_info: list of (race_dict, horse_dicts, y_labels, raw_feats)
    """
    train_start = f"{train_start_month}-01"
    train_end   = f"{valid_month}-01"   # 検証月の 01 = 学習の上限 (exclusive)

    # 検証月の翌月 (valid_end) を計算
    valid_y, valid_m = int(valid_month[:4]), int(valid_month[5:7])
    if valid_m == 12:
        valid_y += 1
        valid_m = 1
    else:
        valid_m += 1
    valid_end = f"{valid_y:04d}-{valid_m:02d}-01"

    logger.info(f"    学習期間: {train_start} 〜 {train_end} (exclusive)")
    logger.info(f"    検証期間: {valid_month} ({train_end} 〜 {valid_end})")
    logger.info(f"    特徴量数: {len(feature_cols)}, odds={include_odds}")

    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    # tracker を train_start 前のデータで更新 (pre-warmup)
    # ばんえい (venue_code="65") を除外 (feedback_banei_excluded.md)
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

    # ばんえい除外した対象レースを収集
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
            feat = _extract_features_v2(
                hd, race, tracker, sire_tracker,
                include_odds=include_odds,
            )
            r_feats.append(feat)
            r_labels.append(1 if fp <= 3 else 0)
            r_horse_dicts.append(hd)

        if r_feats:
            _add_race_relative_features(r_feats)
            if is_valid:
                valid_races_info.append((race, r_horse_dicts, r_labels, r_feats))
            else:
                train_feats.extend(r_feats)
                train_labels.extend(r_labels)

        # tracker は学習・検証関係なく更新 (時系列漏洩なし)
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

    return X_train, y_train, X_valid, y_valid, valid_races_info


# ============================================================
# モデル学習 (Phase 2b'' と同じ設定)
# ============================================================

def _train_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    feature_cols: List[str],
    variant_name: str,
) -> lgb.Booster:
    """LightGBM モデルを学習して返す (Phase 2b'' と同じ設定)"""
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
    best_auc = booster.best_score.get("valid", {}).get("auc", float("nan"))
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
) -> dict:
    """
    通常評価: argmax(prob) で ◎ を選定して ROI 評価。

    Returns:
        dict with variant, auc, top1_hit_pct, tansho_roi_pct, played, hit, payout_sum
    """
    logger.info(f"    [評価] {variant_name} (通常 argmax(prob))")

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
    hit = 0
    payout_sum = 0.0

    for race, horse_dicts, r_labels, r_feats in valid_races_info:
        if not r_feats:
            continue

        X = _to_np(r_feats, feature_cols)
        raw_preds = booster.predict(X)

        all_y_true.extend(r_labels)
        all_y_pred.extend(raw_preds.tolist())

        if len(raw_preds) == 0:
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

    top1_hit_pct = (hit / played * 100) if played > 0 else 0.0
    tansho_roi_pct = (payout_sum / (played * 100) * 100) if played > 0 else 0.0

    logger.info(f"    AUC={auc:.4f}, hit%={top1_hit_pct:.2f}%, ROI={tansho_roi_pct:.2f}%")
    logger.info(f"    played={played}, hit={hit}")

    return {
        "variant": variant_name,
        "auc": auc,
        "top1_hit_pct": top1_hit_pct,
        "tansho_roi_pct": tansho_roi_pct,
        "played": played,
        "hit": hit,
        "payout_sum": payout_sum,
    }


# ============================================================
# EV 評価 (argmax(prob × odds) with odds_cap)
# ============================================================

def _evaluate_posthoc_ev(
    booster: lgb.Booster,
    valid_races_info: list,
    feature_cols: List[str],
    odds_cap: Optional[float] = None,
    prob_min: Optional[float] = None,
    variant_name: str = "+odds+EV_oddsmax10",
) -> dict:
    """
    post-hoc EV 評価: argmax(prob × odds) で ◎ を選定。

    Phase 2b'' で最優秀の設定: odds_cap=10.0, prob_min=None (EV_oddsmax10)

    Args:
        booster: +odds variant (109 features) の学習済みモデル
        valid_races_info: +odds variant の valid_races_info
        feature_cols: FEATURE_COLUMNS_V2_ODDS (109 features)
        odds_cap: odds 上限 (例: 10.0)。None = 制限なし
        prob_min: prob 下限。None = 制限なし
        variant_name: 結果 dict の variant 名

    Returns:
        dict with variant, auc, top1_hit_pct, tansho_roi_pct, played, hit, payout_sum
    """
    constraints_str = []
    if odds_cap is not None:
        constraints_str.append(f"odds<={odds_cap}")
    if prob_min is not None:
        constraints_str.append(f"prob>={prob_min}")
    constraints_label = " AND ".join(constraints_str) if constraints_str else "なし"
    logger.info(f"    [評価] {variant_name} (argmax(prob × odds) / 制約: {constraints_label})")

    def _to_np(rows: list, cols: List[str]) -> np.ndarray:
        mat = []
        for f in rows:
            row = []
            for c in cols:
                v = f.get(c) if isinstance(f, dict) else None
                row.append(float(v) if v is not None else float("nan"))
            mat.append(row)
        return np.array(mat, dtype=np.float32)

    if "odds" not in feature_cols:
        logger.error("    ❌ feature_cols に odds が含まれていません。EV 評価不可")
        return {
            "variant": variant_name,
            "auc": float("nan"),
            "top1_hit_pct": 0.0,
            "tansho_roi_pct": 0.0,
            "played": 0,
            "hit": 0,
            "payout_sum": 0.0,
        }

    odds_col_idx = list(feature_cols).index("odds")

    all_y_true: list = []
    all_y_pred: list = []
    played = 0
    hit = 0
    payout_sum = 0.0
    ev_fallback_count = 0

    for race, horse_dicts, r_labels, r_feats in valid_races_info:
        if not r_feats:
            continue

        X_race = _to_np(r_feats, feature_cols)
        raw_preds = booster.predict(X_race)

        all_y_true.extend(r_labels)
        all_y_pred.extend(raw_preds.tolist())

        if len(raw_preds) == 0:
            continue

        # 特徴量行列から odds を取得
        odds_array = X_race[:, odds_col_idx]
        nan_mask = np.isnan(odds_array)
        if nan_mask.any():
            ev_fallback_count += int(nan_mask.sum())

        # NaN は 1.0 にフォールバック (prob × 1.0 = prob のみで選定)
        odds_safe = np.where(nan_mask, 1.0, odds_array)

        # 期待値 = prob × odds
        expected_value = raw_preds * odds_safe

        # 制約フィルタ
        candidate_mask = np.ones(len(raw_preds), dtype=bool)
        if odds_cap is not None:
            candidate_mask &= (odds_safe <= odds_cap)
        if prob_min is not None:
            candidate_mask &= (raw_preds >= prob_min)

        # candidate が空 → このレースは買い目なし (ROI 計算から除外)
        if not candidate_mask.any():
            continue

        # candidate 内で argmax(prob × odds)
        ev_filtered = np.where(candidate_mask, expected_value, -np.inf)
        top_idx = int(np.argmax(ev_filtered))
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

    top1_hit_pct = (hit / played * 100) if played > 0 else 0.0
    tansho_roi_pct = (payout_sum / (played * 100) * 100) if played > 0 else 0.0

    logger.info(f"    AUC={auc:.4f}, hit%={top1_hit_pct:.2f}%, ROI={tansho_roi_pct:.2f}%")
    logger.info(f"    played={played}, hit={hit}, NaN fallback={ev_fallback_count} horse")

    return {
        "variant": variant_name,
        "auc": auc,
        "top1_hit_pct": top1_hit_pct,
        "tansho_roi_pct": tansho_roi_pct,
        "played": played,
        "hit": hit,
        "payout_sum": payout_sum,
    }


# ============================================================
# 月別結果出力
# ============================================================

def _output_monthly_results(results_monthly: List[dict]) -> str:
    """月別結果を stdout + CSV に出力する"""
    os.makedirs(DIAG_DIR, exist_ok=True)
    csv_path = os.path.join(DIAG_DIR, "phase2c_wf_monthly.csv")

    print()
    print("=" * 100)
    print("【Phase 2c WF 月別結果】")
    print("=" * 100)
    header = f"{'月':<12} {'+odds hit%':>11} {'EV10 hit%':>11} {'Δhit%':>7} | {'ROI +odds':>10} {'ROI EV10':>10} {'ΔROI':>7} | {'played +odds':>12} {'played EV10':>12}"
    print(header)
    print("-" * 100)

    for r in results_monthly:
        m = r.get("valid_month", "?")
        oh = r.get("+odds_hit_pct", 0.0)
        eh = r.get("+EV10_hit_pct", 0.0)
        dh = eh - oh
        or_ = r.get("+odds_ROI_pct", 0.0)
        er = r.get("+EV10_ROI_pct", 0.0)
        dr = er - or_
        op = r.get("+odds_played", 0)
        ep = r.get("+EV10_played", 0)
        print(f"{m:<12} {oh:>10.2f}% {eh:>10.2f}% {dh:>+7.2f} | {or_:>9.2f}% {er:>9.2f}% {dr:>+6.2f}pt | {op:>11} {ep:>12}")

    print("-" * 100)

    # CSV 保存
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "valid_month",
            "+odds_hit_pct", "+EV10_hit_pct",
            "+odds_ROI_pct", "+EV10_ROI_pct",
            "+odds_played", "+EV10_played",
            "+odds_hit", "+EV10_hit",
            "+odds_payout_sum", "+EV10_payout_sum",
            "+odds_auc",
        ])
        for r in results_monthly:
            writer.writerow([
                r.get("valid_month", ""),
                f"{r.get('+odds_hit_pct', 0.0):.4f}",
                f"{r.get('+EV10_hit_pct', 0.0):.4f}",
                f"{r.get('+odds_ROI_pct', 0.0):.4f}",
                f"{r.get('+EV10_ROI_pct', 0.0):.4f}",
                r.get("+odds_played", 0),
                r.get("+EV10_played", 0),
                r.get("+odds_hit", 0),
                r.get("+EV10_hit", 0),
                f"{r.get('+odds_payout_sum', 0.0):.0f}",
                f"{r.get('+EV10_payout_sum', 0.0):.0f}",
                f"{r.get('+odds_auc', float('nan')):.6f}",
            ])

    logger.info(f"    月別 CSV 保存: {csv_path}")
    return csv_path


def _output_aggregated_results(results_monthly: List[dict]) -> str:
    """全期間集計結果を stdout + CSV に出力する"""
    os.makedirs(DIAG_DIR, exist_ok=True)
    csv_path = os.path.join(DIAG_DIR, "phase2c_wf_summary.csv")

    if not results_monthly:
        logger.warning("    集計対象データなし")
        return csv_path

    # 集計値計算
    odds_rois = [r.get("+odds_ROI_pct", 0.0) for r in results_monthly]
    ev10_rois = [r.get("+EV10_ROI_pct", 0.0) for r in results_monthly]
    odds_hits = [r.get("+odds_hit_pct", 0.0) for r in results_monthly]
    ev10_hits = [r.get("+EV10_hit_pct", 0.0) for r in results_monthly]

    # 全期間集約 (played 加重なし: 各月 1 票)
    odds_roi_mean  = float(np.mean(odds_rois))
    ev10_roi_mean  = float(np.mean(ev10_rois))
    odds_roi_std   = float(np.std(odds_rois))
    ev10_roi_std   = float(np.std(ev10_rois))
    odds_roi_min   = float(np.min(odds_rois))
    ev10_roi_min   = float(np.min(ev10_rois))
    odds_roi_max   = float(np.max(odds_rois))
    ev10_roi_max   = float(np.max(ev10_rois))
    odds_hit_mean  = float(np.mean(odds_hits))
    ev10_hit_mean  = float(np.mean(ev10_hits))

    # played 加重集約 (全期間の真の ROI)
    total_odds_played = sum(r.get("+odds_played", 0) for r in results_monthly)
    total_ev10_played = sum(r.get("+EV10_played", 0) for r in results_monthly)
    total_odds_payout = sum(r.get("+odds_payout_sum", 0.0) for r in results_monthly)
    total_ev10_payout = sum(r.get("+EV10_payout_sum", 0.0) for r in results_monthly)
    total_odds_hit    = sum(r.get("+odds_hit", 0) for r in results_monthly)
    total_ev10_hit    = sum(r.get("+EV10_hit", 0) for r in results_monthly)

    odds_roi_weighted  = (total_odds_payout / (total_odds_played * 100) * 100) if total_odds_played > 0 else 0.0
    ev10_roi_weighted  = (total_ev10_payout / (total_ev10_played * 100) * 100) if total_ev10_played > 0 else 0.0
    odds_hit_weighted  = (total_odds_hit / total_odds_played * 100) if total_odds_played > 0 else 0.0
    ev10_hit_weighted  = (total_ev10_hit / total_ev10_played * 100) if total_ev10_played > 0 else 0.0

    print()
    print("=" * 80)
    print("【Phase 2c WF 全期間集計】")
    print("=" * 80)
    print(f"  {'指標':<25} {'指標2':<20} {'+odds (基準)':>14} {'+EV_oddsmax10':>14} {'差分 pt':>10}")
    print("-" * 80)

    rows = [
        ("hit%_月平均",        "",                    f"{odds_hit_mean:.2f}%",   f"{ev10_hit_mean:.2f}%",   f"{ev10_hit_mean - odds_hit_mean:+.2f}"),
        ("hit%_全期間加重",     "(played 加重)",       f"{odds_hit_weighted:.2f}%", f"{ev10_hit_weighted:.2f}%", f"{ev10_hit_weighted - odds_hit_weighted:+.2f}"),
        ("ROI_月平均",          "",                    f"{odds_roi_mean:.2f}%",   f"{ev10_roi_mean:.2f}%",   f"{ev10_roi_mean - odds_roi_mean:+.2f}"),
        ("ROI_全期間加重",      "(played 加重)",       f"{odds_roi_weighted:.2f}%", f"{ev10_roi_weighted:.2f}%", f"{ev10_roi_weighted - odds_roi_weighted:+.2f}"),
        ("ROI_標準偏差",        "(月別ばらつき)",      f"{odds_roi_std:.2f}",     f"{ev10_roi_std:.2f}",     ""),
        ("ROI_最悪月",          "",                    f"{odds_roi_min:.2f}%",    f"{ev10_roi_min:.2f}%",    f"{ev10_roi_min - odds_roi_min:+.2f}"),
        ("ROI_最良月",          "",                    f"{odds_roi_max:.2f}%",    f"{ev10_roi_max:.2f}%",    f"{ev10_roi_max - odds_roi_max:+.2f}"),
        ("買い目数_合計",       "",                    f"{total_odds_played}",    f"{total_ev10_played}",    ""),
        ("的中数_合計",         "",                    f"{total_odds_hit}",       f"{total_ev10_hit}",       ""),
        ("払戻合計",            "(円)",                f"{total_odds_payout:.0f}", f"{total_ev10_payout:.0f}", ""),
    ]

    for row in rows:
        label, note, v_odds, v_ev10, delta = row
        label_full = f"{label} {note}".strip()
        print(f"  {label_full:<45} {v_odds:>14} {v_ev10:>14} {delta:>10}")

    print("=" * 80)

    # 改善判定
    delta_roi_weighted = ev10_roi_weighted - odds_roi_weighted
    delta_hit_weighted = ev10_hit_weighted - odds_hit_weighted

    print()
    print("【Phase 2c 改善判定】")
    print(f"  ΔROI (全期間加重): {delta_roi_weighted:+.2f}pt")
    print(f"  Δhit% (全期間加重): {delta_hit_weighted:+.2f}pt")
    print()

    if delta_roi_weighted >= 5.0 and ev10_roi_weighted >= 85.0:
        judgment = "✅ 全期間でも +5pt 以上 ROI 改善 → EV_oddsmax10 採用価値高い"
    elif delta_roi_weighted >= 3.0:
        judgment = "⚠️ +3〜5pt 改善 → 部分採用・追加検証推奨"
    elif delta_roi_weighted >= 1.0:
        judgment = "⚠️ +1〜3pt 改善 → 月によりばらつき大きい可能性あり"
    elif delta_roi_weighted >= -1.0:
        judgment = "❌ ±1pt 以内 → Phase 2b'' が 2025-12 限定の過学習の疑い"
    else:
        judgment = "❌❌ 全期間で悪化 → EV_oddsmax10 汎化性能なし・採用不可"

    print(f"  判定: {judgment}")
    print("=" * 80)

    # CSV 保存
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "note", "+odds (基準)", "+odds+EV_oddsmax10", "差分pt"])
        for row in rows:
            writer.writerow(row)
        writer.writerow(["", "", "", "", ""])
        writer.writerow(["ΔROI_加重", "", "", f"{delta_roi_weighted:+.2f}", ""])
        writer.writerow(["Δhit%_加重", "", "", f"{delta_hit_weighted:+.2f}", ""])
        writer.writerow(["判定", "", "", judgment, ""])

    logger.info(f"    集計 CSV 保存: {csv_path}")
    return csv_path


# ============================================================
# メイン
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 2c: WF 全期間検証 (EV_oddsmax10) — 6 ヶ月 rolling WF で +odds vs +EV_oddsmax10 を比較"
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
    args = parser.parse_args()

    start_time = time.time()

    logger.info("=" * 70)
    logger.info("[Phase 2c WF 全期間検証] 開始")
    logger.info(f"  EV_oddsmax10 設定: odds_cap={EV_ODDS_CAP}")
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

    # ──────────────────────────────────────────────────────────
    # 全レース + 種牡馬マップ読み込み (全月共通 / 1 回のみ)
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("[データ読み込み] _load_ml_races() + _load_horse_sire_map()...")
    t0 = time.time()
    sire_map = _load_horse_sire_map()
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
        valid_month   = month_cfg["valid"]
        train_start   = month_cfg["train_start"]
        train_end     = month_cfg["train_end"]

        elapsed_total = time.time() - start_time
        pct = month_idx / total_months * 100
        bar = "█" * (month_idx * 20 // total_months) + "░" * (20 - month_idx * 20 // total_months)
        logger.info("")
        logger.info(f"[{bar}] {pct:.0f}% — 月 {month_idx+1}/{total_months}: valid={valid_month} / elapsed={elapsed_total:.0f}秒")
        logger.info(f"  学習期間: {train_start} 〜 {train_end}")
        logger.info("=" * 70)

        # データ構築
        logger.info("  [Step 1] データ構築 (+odds 109 features)...")
        t_step = time.time()
        try:
            X_train, y_train, X_valid, y_valid, valid_races_info = _build_train_valid_data(
                train_start_month=train_start,
                train_end_month=train_end,
                valid_month=valid_month,
                feature_cols=FEATURE_COLUMNS_V2_ODDS,
                all_races=all_races,
                sire_map=sire_map,
                include_odds=True,
            )
        except Exception as e:
            logger.error(f"  ❌ データ構築エラー (valid={valid_month}): {e}")
            import traceback
            traceback.print_exc()
            continue

        if len(y_train) == 0 or len(valid_races_info) == 0:
            logger.warning(f"  ⚠️ valid={valid_month}: 学習/検証データが空。スキップ")
            continue

        logger.info(f"  データ構築完了: {time.time()-t_step:.1f}秒")

        # +odds モデル学習
        logger.info("  [Step 2] +odds モデル学習...")
        t_step = time.time()
        try:
            booster = _train_model(
                X_train, y_train,
                X_valid, y_valid,
                FEATURE_COLUMNS_V2_ODDS,
                f"+odds_{valid_month}",
            )
        except Exception as e:
            logger.error(f"  ❌ 学習エラー (valid={valid_month}): {e}")
            import traceback
            traceback.print_exc()
            continue

        logger.info(f"  学習完了: {time.time()-t_step:.1f}秒")

        # +odds 評価 (argmax(prob))
        logger.info("  [Step 3a] +odds 評価 (argmax(prob))...")
        t_step = time.time()
        r_odds = _evaluate_variant(
            booster, valid_races_info, FEATURE_COLUMNS_V2_ODDS, "+odds"
        )
        logger.info(f"  +odds 評価完了: {time.time()-t_step:.1f}秒")

        # +odds+EV_oddsmax10 評価 (argmax(prob × odds) / odds<=10)
        logger.info("  [Step 3b] +EV_oddsmax10 評価 (argmax(prob × odds) / odds<=10)...")
        t_step = time.time()
        r_ev10 = _evaluate_posthoc_ev(
            booster, valid_races_info, FEATURE_COLUMNS_V2_ODDS,
            odds_cap=EV_ODDS_CAP, prob_min=None,
            variant_name="+odds+EV_oddsmax10",
        )
        logger.info(f"  +EV_oddsmax10 評価完了: {time.time()-t_step:.1f}秒")

        # 月別結果を蓄積
        month_result = {
            "valid_month":        valid_month,
            "+odds_hit_pct":      r_odds["top1_hit_pct"],
            "+odds_ROI_pct":      r_odds["tansho_roi_pct"],
            "+odds_played":       r_odds["played"],
            "+odds_hit":          r_odds["hit"],
            "+odds_payout_sum":   r_odds["payout_sum"],
            "+odds_auc":          r_odds["auc"],
            "+EV10_hit_pct":      r_ev10["top1_hit_pct"],
            "+EV10_ROI_pct":      r_ev10["tansho_roi_pct"],
            "+EV10_played":       r_ev10["played"],
            "+EV10_hit":          r_ev10["hit"],
            "+EV10_payout_sum":   r_ev10["payout_sum"],
        }
        results_monthly.append(month_result)

        # 月別中間サマリ表示
        d_roi = r_ev10["tansho_roi_pct"] - r_odds["tansho_roi_pct"]
        logger.info(f"  ✅ {valid_month} 完了: +odds ROI={r_odds['tansho_roi_pct']:.2f}% → EV10 ROI={r_ev10['tansho_roi_pct']:.2f}% (Δ{d_roi:+.2f}pt)")

    # ──────────────────────────────────────────────────────────
    # 全月完了後: 結果出力
    # ──────────────────────────────────────────────────────────
    if not results_monthly:
        logger.error("❌ 有効な月別結果がありません。CSV 出力スキップ。")
        sys.exit(1)

    logger.info("")
    logger.info("[出力] 月別結果 + 全期間集計...")
    csv_monthly = _output_monthly_results(results_monthly)
    csv_summary = _output_aggregated_results(results_monthly)

    total_elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print("【Phase 2c 完了】")
    print(f"  合計所要時間: {total_elapsed:.1f}秒 ({total_elapsed/60:.1f}分)")
    print(f"  月別 CSV: {csv_monthly}")
    print(f"  集計 CSV: {csv_summary}")
    print("=" * 70)

    logger.info("[Phase 2c WF 全期間検証] 完了")


if __name__ == "__main__":
    main()
