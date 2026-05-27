"""M-3 Phase 2b' (案 D): post-hoc 期待値補正

LightGBM 学習は Phase 2a と同じ (binary_logloss)。
変更点: 検証時の ◎ 選定を argmax(prob) → argmax(prob × odds) に変更。

variant 構成:
  - baseline_new   (108 features / binary_logloss) — Phase 2a 結果流用
  - +odds          (109 features / binary_logloss) — Phase 2a 結果流用 + 通常 argmax(prob)
  - +odds+posthoc_EV (109 features / binary_logloss + EV 選定) ← 新規 (◎=argmax(prob×odds))

評価指標:
  - AUC (binary classification)
  - TOP1 hit% (◎ 単勝 hit 率 = 1 着)
  - tansho ROI (◎ 単勝回収率 %)

Phase 2b 失敗の教訓:
  - LightGBM custom objective (ROI weighted BCE) は API エラーで不採用
  - sample_weight ROI: 70.31% (大幅悪化)
  → 学習は通常 binary_logloss のまま、選定ロジックのみ変更する方針に切替

改善判定基準:
  - +odds ROI 79.06% 基準
  - +5pt 以上 (84.06%+) → Phase 2c (全モデル run) 進行
  - +1-5pt → 部分採用、他仮説と組合せ検討
  - ±1pt 以内 → アプローチ全体見直し

重要な前提 (★★★):
  - src/ml/lgbm_model.py は絶対不変
  - ばんえい (venue_65) 除外 (feedback_banei_excluded.md)
  - git commit 禁止
  - 既存ファイル変更禁止
  - Phase 2c 実施禁止

Usage:
  # フル実行 (3 variant 比較)
  python scripts/diag_phase2b_alt_posthoc_ev.py

  # デバッグモード (学習期間短縮)
  python scripts/diag_phase2b_alt_posthoc_ev.py --debug

  # Step 1 のみ (odds データ存在確認)
  python scripts/diag_phase2b_alt_posthoc_ev.py --check-only
"""

import argparse
import csv
import os
import sys
import time
from typing import List, Optional, Tuple

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
MODEL_DIR = os.path.join(ROOT, "data", "models", "wf_2026")
DIAG_DIR = os.path.join(ROOT, "data", "_diag")

# 学習期間: 2025-04〜11 (8 ヶ月) — Phase 2a と同じ
TRAIN_START_MONTH = "2025-04"  # inclusive
TRAIN_END_MONTH   = "2025-11"  # inclusive (< 2025-12)

# 検証期間: 2025-12 (1 ヶ月) — Phase 2a と同じ
VALID_MONTH = "2025-12"

# FEATURE_COLUMNS_V2_ODDS (108 + odds = 109) — Phase 2a と同じ
FEATURE_COLUMNS_V2_ODDS = list(FEATURE_COLUMNS) + ["odds"]

# LightGBM 学習パラメータ — Phase 2a と完全に同じ設定 (公正比較)
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

# 学習イテレーション上限 (early_stopping あり) — Phase 2a と同じ
NUM_BOOST_ROUND = 200
EARLY_STOPPING_ROUNDS = 20


# ============================================================
# Step 1: odds データ存在調査 (Phase 2a と同じロジック)
# ============================================================

def step1_check_odds_data(all_races: list) -> dict:
    """
    horse dict に odds が含まれているか確認する。

    Returns:
        {
          'has_odds': bool,
          'sample_horse_keys': list,
          'sample_odds': any,
          'odds_null_rate': float,
          'valid_month_horses': int,
        }
    """
    logger.info("=" * 60)
    logger.info("[Step 1] odds データ存在調査")
    logger.info("=" * 60)

    # 代表 race を 3 つ確認
    sample_indices = [100, 500, 1000]
    result = {}

    for idx in sample_indices:
        if idx < len(all_races):
            race = all_races[idx]
            horses = race.get("horses", [])
            if horses:
                h = horses[0]
                logger.info(f"  race[{idx}] date={race.get('date')} horse_keys={list(h.keys())[:20]}")
                logger.info(f"  race[{idx}] odds={h.get('odds')} tansho_odds={h.get('tansho_odds')}")
                if not result:
                    result["sample_horse_keys"] = list(h.keys())
                    result["sample_odds"] = h.get("odds") or h.get("tansho_odds")

    # NULL 率を 2025-12 月の race で計算
    target_races = [r for r in all_races if r.get("date", "").startswith(VALID_MONTH)]
    total_h = odds_null = 0
    for race in target_races:
        for h in race.get("horses", []):
            if h.get("finish_pos") is None:
                continue
            total_h += 1
            odds_val = h.get("odds") or h.get("tansho_odds")
            if odds_val is None:
                odds_null += 1

    if total_h > 0:
        result["odds_null_rate"] = odds_null / total_h
    else:
        result["odds_null_rate"] = 1.0

    result["has_odds"] = result.get("odds_null_rate", 1.0) < 0.95
    result["valid_month_horses"] = total_h

    logger.info(f"  2025-12 月 horse 数: {total_h}")
    logger.info(f"  odds NULL 率: {result['odds_null_rate']:.3f} (has_odds={result['has_odds']})")

    if not result["has_odds"]:
        logger.error("  ❌ odds データが ML データに含まれていません！")
    else:
        logger.info("  ✅ odds データ確認 OK → Phase 2b' 続行")

    return result


# ============================================================
# Step 2: _extract_features_v2 ラッパー
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

    odds: horse.get("odds") — 当日確定 tansho odds (リーク無)
           horse に odds がない場合は tansho_odds を試みる
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
# Step 3: 学習データ + 検証データ構築 (Phase 2a と同じ)
# ============================================================

def _build_train_valid_data(
    train_start_month: str,
    train_end_month: str,
    valid_month: str,
    feature_cols: List[str],
    all_races: list,
    sire_map: dict,
    include_odds: bool = False,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list]:
    """
    学習/検証データを構築する。

    Args:
        train_start_month: '2025-04' (inclusive)
        train_end_month:   '2025-11' (inclusive)
        valid_month:       '2025-12'
        feature_cols:      使用する特徴量列 (FEATURE_COLUMNS or V2 variants)
        all_races:         _load_ml_races() の戻り値
        sire_map:          _load_horse_sire_map() の戻り値
        include_odds:      True なら odds を追加

    Returns:
        X_train, y_train, X_valid, y_valid, valid_races_info
        valid_races_info: list of (race_dict, horse_dicts, y_labels, raw_feats)
    """
    # 期間境界 (YYYY-MM-DD 形式で比較)
    train_start = f"{train_start_month}-01"
    train_end   = f"{valid_month}-01"   # 2025-12-01 → 学習は < 2025-12-01
    valid_end_y, valid_end_m = int(valid_month[:4]), int(valid_month[5:7])
    if valid_end_m == 12:
        valid_end_y += 1
        valid_end_m = 1
    else:
        valid_end_m += 1
    valid_end = f"{valid_end_y:04d}-{valid_end_m:02d}-01"

    logger.info(f"  学習期間: {train_start} 〜 {train_end} (exclusive)")
    logger.info(f"  検証期間: {valid_month} (〜 {valid_end})")
    logger.info(f"  特徴量数: {len(feature_cols)}")
    logger.info(f"  odds={include_odds}")

    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    # tracker を train_start 前のデータで更新 (pre-warmup)
    # ばんえい (venue_code="65") を除外 (feedback_banei_excluded.md)
    pre_warmup_races = [
        r for r in all_races
        if r.get("date", "") < train_start
        and str(r.get("venue_code", "")) != "65"
    ]
    logger.info(f"  tracker pre-warmup: {len(pre_warmup_races)} レース (< {train_start}, ばんえい除外済)")
    t0 = time.time()
    for i, race in enumerate(pre_warmup_races):
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)
        if (i + 1) % 10000 == 0:
            pct = (i + 1) / len(pre_warmup_races) * 100
            logger.info(f"    pre-warmup: {i+1}/{len(pre_warmup_races)} ({pct:.0f}%)")
    logger.info(f"  pre-warmup 完了: {time.time()-t0:.1f}秒")

    # 学習/検証データ収集 (tracker はここで都度更新)
    train_feats, train_labels = [], []
    valid_races_info = []  # (race_dict, horse_dicts, labels, raw_feats)

    # ばんえい (venue_code="65") を除外 (feedback_banei_excluded.md)
    target_races = [
        r for r in all_races
        if train_start <= r.get("date", "") < valid_end
        and str(r.get("venue_code", "")) != "65"
    ]
    logger.info(f"  対象レース数 (学習+検証, ばんえい除外済): {len(target_races)}")

    t1 = time.time()
    for i, race in enumerate(target_races):
        d = race.get("date", "")
        is_valid = d >= train_end  # 2025-12 以降 = 検証

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
            logger.info(f"    データ収集: {i+1}/{len(target_races)} ({pct:.0f}%) {elapsed:.1f}秒")

    logger.info(f"  データ収集完了: {time.time()-t1:.1f}秒")
    logger.info(f"  学習サンプル数: {len(train_labels)}, 検証レース数: {len(valid_races_info)}")

    # numpy 変換
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

    # 検証データ
    valid_feats = []
    valid_labels = []
    for _, _, r_labels, r_feats in valid_races_info:
        valid_feats.extend(r_feats)
        valid_labels.extend(r_labels)

    X_valid = _to_np(valid_feats, feature_cols)
    y_valid = np.array(valid_labels, dtype=np.int32)

    nan_rate = np.isnan(X_train).mean()
    logger.info(f"  X_train shape: {X_train.shape}, NaN率: {nan_rate:.3f}")
    logger.info(f"  X_valid shape: {X_valid.shape}")
    logger.info(f"  y_train 正例率: {y_train.mean():.3f}, y_valid 正例率: {y_valid.mean():.3f}")

    return X_train, y_train, X_valid, y_valid, valid_races_info


# ============================================================
# Step 4: 各 variant の学習 (Phase 2a と同じ)
# ============================================================

def _train_new_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    feature_cols: List[str],
    variant_name: str,
) -> lgb.Booster:
    """新規 LightGBM モデルを学習して返す (Phase 2a と同じ設定)"""
    logger.info(f"  [学習] variant={variant_name}, 特徴量数={len(feature_cols)}")
    logger.info(f"  X_train={X_train.shape}, X_valid={X_valid.shape}")

    # categorical features (feature_cols にある場合のみ)
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
    logger.info(f"  学習完了: {elapsed:.1f}秒, best_iter={best_iter}, valid AUC={best_auc:.4f}")

    return booster


# ============================================================
# Step 5: 通常評価 (argmax(prob)) — Phase 2a と同じ
# ============================================================

def _evaluate_variant(
    booster: lgb.Booster,
    valid_races_info: list,
    feature_cols: List[str],
    variant_name: str,
) -> dict:
    """
    検証データで AUC + TOP1 hit% + tansho ROI を計算する (通常 argmax(prob))。

    Args:
        booster: 学習済みモデル
        valid_races_info: _build_train_valid_data の戻り値
        feature_cols: このモデルが使う特徴量列
        variant_name: ログ表示用

    Returns:
        {'variant': str, 'auc': float, 'top1_hit_pct': float, 'tansho_roi_pct': float,
         'played': int, 'hit': int, 'payout_sum': float}
    """
    logger.info(f"  [評価] variant={variant_name} (通常 argmax(prob))")

    def _to_np(rows: list, cols: List[str]) -> np.ndarray:
        mat = []
        for f in rows:
            row = []
            for c in cols:
                v = f.get(c) if isinstance(f, dict) else None
                row.append(float(v) if v is not None else float("nan"))
            mat.append(row)
        return np.array(mat, dtype=np.float32)

    all_y_true = []
    all_y_pred = []
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

        # 通常選定: argmax(prob)
        top_idx = int(np.argmax(raw_preds))
        top_horse = horse_dicts[top_idx]

        top_finish = top_horse.get("finish_pos")
        is_win = (top_finish is not None and top_finish == 1)

        # 単勝オッズ取得
        odds_val = top_horse.get("odds")
        if odds_val is None:
            odds_val = top_horse.get("tansho_odds")
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

    # AUC 計算
    auc = float("nan")
    if len(set(all_y_true)) == 2:
        try:
            auc = roc_auc_score(all_y_true, all_y_pred)
        except Exception as e:
            logger.warning(f"  AUC 計算エラー: {e}")

    top1_hit_pct = (hit / played * 100) if played > 0 else 0.0
    tansho_roi_pct = (payout_sum / (played * 100) * 100) if played > 0 else 0.0

    logger.info(f"  AUC={auc:.4f}, TOP1 hit%={top1_hit_pct:.2f}%, tansho ROI={tansho_roi_pct:.2f}%")
    logger.info(f"  played={played}, hit={hit}, payout_sum={payout_sum:.0f}円")

    return {
        "variant": variant_name,
        "features": len(feature_cols),
        "objective": "binary_logloss",
        "auc": auc,
        "top1_hit_pct": top1_hit_pct,
        "tansho_roi_pct": tansho_roi_pct,
        "played": played,
        "hit": hit,
        "payout_sum": payout_sum,
    }


# ============================================================
# Step 6: post-hoc EV 評価 (argmax(prob × odds)) ← Phase 2b' 新規
# ============================================================

def _evaluate_posthoc_ev(
    booster: lgb.Booster,
    valid_races_info: list,
    feature_cols: List[str],
    odds_cap: Optional[float] = None,
    prob_min: Optional[float] = None,
    variant_name: str = "+odds+posthoc_EV",
) -> dict:
    """
    post-hoc EV (期待値) ベースで ◎ を選定して ROI 評価する。

    通常評価との違い:
      - 通常: argmax(prob)
      - EV: argmax(prob × odds)  ← この関数

    Phase 2b'' で制約追加: odds_cap / prob_min で穴馬選びすぎを抑制。

    Args:
        booster: +odds variant (109 features) の学習済みモデル
        valid_races_info: +odds variant の valid_races_info
        feature_cols: FEATURE_COLUMNS_V2_ODDS (109 features)
        odds_cap: odds 上限 (例: 10.0 で odds <= 10 のみ candidate)。None = 制限なし
        prob_min: prob 下限 (例: 0.10 で prob >= 0.10 のみ candidate)。None = 制限なし
        variant_name: 結果 dict の variant 名 (例: "+odds+EV_oddsmax10")

    Returns:
        dict with variant=<variant_name>, auc, top1_hit_pct, tansho_roi_pct, ...
    """
    constraints_str = []
    if odds_cap is not None:
        constraints_str.append(f"odds<={odds_cap}")
    if prob_min is not None:
        constraints_str.append(f"prob>={prob_min}")
    constraints_label = " AND ".join(constraints_str) if constraints_str else "なし"
    logger.info(f"  [評価] {variant_name} (argmax(prob × odds) / 制約: {constraints_label})")

    def _to_np(rows: list, cols: List[str]) -> np.ndarray:
        mat = []
        for f in rows:
            row = []
            for c in cols:
                v = f.get(c) if isinstance(f, dict) else None
                row.append(float(v) if v is not None else float("nan"))
            mat.append(row)
        return np.array(mat, dtype=np.float32)

    # odds 列のインデックスを特定
    if "odds" not in feature_cols:
        logger.error("  ❌ feature_cols に odds が含まれていません。post-hoc EV 評価不可")
        return {
            "variant": variant_name,
            "features": len(feature_cols),
            "objective": "binary_logloss + post-hoc EV",
            "auc": float("nan"),
            "top1_hit_pct": 0.0,
            "tansho_roi_pct": 0.0,
            "played": 0,
            "hit": 0,
            "payout_sum": 0.0,
        }
    odds_col_idx = list(feature_cols).index("odds")
    logger.info(f"  odds 列インデックス: {odds_col_idx}")

    all_y_true = []
    all_y_pred = []
    played = 0
    hit = 0
    payout_sum = 0.0

    # prob × odds が NaN になったレースのカウント (デバッグ用)
    ev_nan_count = 0
    ev_fallback_count = 0

    for race, horse_dicts, r_labels, r_feats in valid_races_info:
        if not r_feats:
            continue

        X_race = _to_np(r_feats, feature_cols)
        raw_preds = booster.predict(X_race)  # shape=(n_horses,) = 確率 (3着内)

        all_y_true.extend(r_labels)
        all_y_pred.extend(raw_preds.tolist())

        if len(raw_preds) == 0:
            continue

        # 各 horse の odds を特徴量行列から取得
        odds_array = X_race[:, odds_col_idx]  # shape=(n_horses,)

        # NaN を 1.0 にフォールバック (= 期待値 = prob × 1.0 ≈ prob のみで選定)
        nan_mask = np.isnan(odds_array)
        if nan_mask.any():
            ev_fallback_count += nan_mask.sum()
        odds_safe = np.where(nan_mask, 1.0, odds_array)

        # 期待値 = prob × odds
        expected_value = raw_preds * odds_safe

        # 全て NaN fallback の場合のガード
        if np.all(odds_safe == 1.0) and nan_mask.all():
            ev_nan_count += 1

        # Phase 2b'' 制約: odds_cap / prob_min で candidate フィルタ
        candidate_mask = np.ones(len(raw_preds), dtype=bool)
        if odds_cap is not None:
            candidate_mask &= (odds_safe <= odds_cap)
        if prob_min is not None:
            candidate_mask &= (raw_preds >= prob_min)

        # 制約後の candidate が空 → このレースは ROI 計算から除外 (買い目なし)
        if not candidate_mask.any():
            continue

        # candidate 内で argmax(prob × odds)
        ev_filtered = np.where(candidate_mask, expected_value, -np.inf)
        top_idx = int(np.argmax(ev_filtered))
        top_horse = horse_dicts[top_idx]

        top_finish = top_horse.get("finish_pos")
        is_win = (top_finish is not None and top_finish == 1)

        # 単勝オッズ取得 (horse_dict の生値から取得)
        odds_val = top_horse.get("odds")
        if odds_val is None:
            odds_val = top_horse.get("tansho_odds")
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

    # AUC 計算 (モデルは +odds と同一なので同じ値になるはず)
    auc = float("nan")
    if len(set(all_y_true)) == 2:
        try:
            auc = roc_auc_score(all_y_true, all_y_pred)
        except Exception as e:
            logger.warning(f"  AUC 計算エラー: {e}")

    top1_hit_pct = (hit / played * 100) if played > 0 else 0.0
    tansho_roi_pct = (payout_sum / (played * 100) * 100) if played > 0 else 0.0

    logger.info(f"  AUC={auc:.4f}, TOP1 hit%={top1_hit_pct:.2f}%, tansho ROI={tansho_roi_pct:.2f}%")
    logger.info(f"  played={played}, hit={hit}, payout_sum={payout_sum:.0f}円")
    logger.info(f"  odds NaN fallback 発生: {ev_fallback_count} horse, 全NaNレース: {ev_nan_count}")

    return {
        "variant": variant_name,
        "features": len(feature_cols),
        "objective": "binary_logloss + post-hoc EV",
        "auc": auc,
        "top1_hit_pct": top1_hit_pct,
        "tansho_roi_pct": tansho_roi_pct,
        "played": played,
        "hit": hit,
        "payout_sum": payout_sum,
    }


# ============================================================
# Step 7: 結果出力
# ============================================================

def _output_results(results: List[dict]) -> None:
    """結果を stdout + CSV に出力する"""
    os.makedirs(DIAG_DIR, exist_ok=True)
    csv_path = os.path.join(DIAG_DIR, "phase2b_alt_summary.csv")

    print()
    print("=" * 90)
    print("【Phase 2b' (案D post-hoc EV) 結果サマリー】")
    print("=" * 90)
    print(f"{'variant':<30} {'features':>8} {'objective':<30} {'AUC':>8} {'TOP1_hit%':>10} {'tansho_ROI%':>12} {'played':>7}")
    print("-" * 90)

    for r in results:
        v = r.get("variant", "?")
        feats = r.get("features", 0)
        obj = r.get("objective", "?")[:28]
        auc = r.get("auc", float("nan"))
        top1 = r.get("top1_hit_pct", 0.0)
        roi = r.get("tansho_roi_pct", 0.0)
        played = r.get("played", 0)
        auc_str = f"{auc:.4f}" if auc == auc else "N/A"
        print(f"{v:<30} {feats:>8} {obj:<30} {auc_str:>8} {top1:>9.2f}% {roi:>11.2f}% {played:>7}")

    print("-" * 90)
    print()

    # CSV 保存
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["variant", "features", "objective", "AUC", "TOP1_hit_pct", "tansho_ROI_pct", "played", "hit", "payout_sum"])
        for r in results:
            auc = r.get("auc", float("nan"))
            auc_str = f"{auc:.6f}" if auc == auc else ""
            writer.writerow([
                r.get("variant", ""),
                r.get("features", 0),
                r.get("objective", ""),
                auc_str,
                f"{r.get('top1_hit_pct', 0.0):.4f}",
                f"{r.get('tansho_roi_pct', 0.0):.4f}",
                r.get("played", 0),
                r.get("hit", 0),
                f"{r.get('payout_sum', 0.0):.0f}",
            ])
    logger.info(f"  CSV 保存: {csv_path}")


# ============================================================
# Step 8: 改善判定
# ============================================================

def _judge_improvement(results: List[dict]) -> str:
    """
    +odds (baseline) vs +odds+posthoc_EV の差分を判定し、Phase 2c 推奨を返す。

    判定基準:
      +5pt 以上 ROI → Phase 2c (全モデル run) 進行価値高い
      +1〜5pt → 部分採用、他仮説と組合せ検討
      ±1pt 以内 → アプローチ全体見直し
    """
    if len(results) < 2:
        return "データ不足で判定不可"

    baseline_new = next((r for r in results if r.get("variant") == "baseline_new"), None)
    plus_odds = next((r for r in results if r.get("variant") == "+odds"), None)
    plus_ev = next((r for r in results if r.get("variant") == "+odds+posthoc_EV"), None)

    if plus_odds is None or plus_ev is None:
        return "+odds または +odds+posthoc_EV 結果なし"

    delta_roi_ev_vs_odds = plus_ev.get("tansho_roi_pct", 0) - plus_odds.get("tansho_roi_pct", 0)
    delta_hit_ev_vs_odds = plus_ev.get("top1_hit_pct", 0) - plus_odds.get("top1_hit_pct", 0)

    print("=" * 80)
    print("【Phase 2b' (案D post-hoc EV) 改善判定】")
    print("=" * 80)

    if baseline_new:
        print(f"  baseline_new ROI={baseline_new.get('tansho_roi_pct', 0):.2f}%  hit%={baseline_new.get('top1_hit_pct', 0):.2f}%")
    print(f"  +odds        ROI={plus_odds.get('tansho_roi_pct', 0):.2f}%  hit%={plus_odds.get('top1_hit_pct', 0):.2f}%")
    print(f"  +odds+posthoc_EV ROI={plus_ev.get('tansho_roi_pct', 0):.2f}%  hit%={plus_ev.get('top1_hit_pct', 0):.2f}%")
    print()
    print(f"  Δ ROI (+odds → +odds+posthoc_EV): {delta_roi_ev_vs_odds:+.2f}pt")
    print(f"  Δ hit% (+odds → +odds+posthoc_EV): {delta_hit_ev_vs_odds:+.2f}pt")
    print()

    # EV 補正は hit% が下がって ROI が上がるのが正常 (高オッズ馬を選びやすくなるため)
    if delta_roi_ev_vs_odds >= 5.0:
        judgment = "✅ +5pt 以上 ROI 改善 → Phase 2c (全モデル run) 進行価値高い"
        recommendation = "Phase 2c 進行 (Opus 判断依頼)"
    elif delta_roi_ev_vs_odds >= 1.0:
        judgment = "⚠️ +1〜5pt 改善 → 部分採用・他仮説 (EV 閾値チューニング等) と組合せ検討"
        recommendation = "閾値チューニング検討 (Opus 判断依頼)"
    elif delta_roi_ev_vs_odds >= -1.0:
        judgment = "❌ ±1pt 以内 → post-hoc EV 補正単体では効果なし。アプローチ見直しが必要"
        recommendation = "Phase 2 方針見直し"
    else:
        judgment = "❌❌ 大幅悪化 → post-hoc EV 補正は逆効果。採用不可"
        recommendation = "採用不可・方針見直し必須"

    print(f"  判定: {judgment}")
    print(f"  推奨アクション: {recommendation}")
    print("=" * 80)

    return judgment


# ============================================================
# メイン
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 2b' (案D post-hoc EV): argmax(prob × odds) による期待値最大化 ROI 評価"
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Step 1 (odds データ存在確認) のみ実行",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="デバッグモード (学習期間短縮 / 動作確認用)",
    )
    args = parser.parse_args()

    start_time = time.time()

    logger.info("=" * 70)
    logger.info("[Phase 2b' post-hoc EV] 開始")
    logger.info(f"  学習期間: {TRAIN_START_MONTH} 〜 {TRAIN_END_MONTH}")
    logger.info(f"  検証期間: {VALID_MONTH}")
    logger.info(f"  variant: baseline_new (108) / +odds (109) / +odds+posthoc_EV (109)")
    logger.info(f"  EV 選定: argmax(prob × odds) ← Phase 2b' の核心変更")
    logger.info("=" * 70)

    # ──────────────────────────────────────────────────────────
    # データ読み込み (全 variant 共通)
    # ──────────────────────────────────────────────────────────
    logger.info("[データ読み込み] _load_ml_races() + _load_horse_sire_map()...")
    t0 = time.time()
    sire_map = _load_horse_sire_map()
    all_races = _load_ml_races()
    logger.info(f"  全レース数: {len(all_races)}, 所要時間: {time.time()-t0:.1f}秒")

    # ──────────────────────────────────────────────────────────
    # Step 1: odds データ存在調査
    # ──────────────────────────────────────────────────────────
    check_result = step1_check_odds_data(all_races)

    if not check_result["has_odds"]:
        logger.error("❌ odds データが ML データに存在しません。Phase 2b' 作業中断。")
        sys.exit(1)

    if args.check_only:
        print("\n[Step 1 結果]")
        print(f"  has_odds: {check_result['has_odds']}")
        print(f"  sample odds: {check_result.get('sample_odds')}")
        print(f"  2025-12 月 horse 数: {check_result.get('valid_month_horses')}")
        print(f"  odds NULL 率: {check_result.get('odds_null_rate', 0):.3f}")
        logger.info("--check-only モード: ここで終了")
        return

    # デバッグモード設定
    train_start_month = TRAIN_START_MONTH
    train_end_month = TRAIN_END_MONTH
    if args.debug:
        train_start_month = "2025-10"  # 2 ヶ月のみ (動作確認用)
        train_end_month = "2025-11"
        logger.info("  デバッグモード: 学習期間 2025-10〜11 (2 ヶ月)")

    results = []

    # ──────────────────────────────────────────────────────────
    # Step 3a: baseline_new (108 features / 公正比較用)
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("[Step 3a] baseline_new 学習/評価 (108 features / binary_logloss)")
    logger.info("=" * 70)

    logger.info("  baseline_new 用学習/検証データ構築 (108 features, odds なし)...")
    X_train_108, y_train_108, X_valid_108, y_valid_108, valid_races_108 = _build_train_valid_data(
        train_start_month=train_start_month,
        train_end_month=train_end_month,
        valid_month=VALID_MONTH,
        feature_cols=list(FEATURE_COLUMNS),
        all_races=all_races,
        sire_map=sire_map,
        include_odds=False,
    )

    t_train = time.time()
    booster_baseline_new = _train_new_model(
        X_train_108, y_train_108,
        X_valid_108, y_valid_108,
        list(FEATURE_COLUMNS),
        "baseline_new (108)",
    )
    train_elapsed_baseline_new = time.time() - t_train

    t_eval = time.time()
    r_baseline_new = _evaluate_variant(
        booster_baseline_new, valid_races_108, list(FEATURE_COLUMNS), "baseline_new"
    )
    r_baseline_new["train_elapsed_sec"] = train_elapsed_baseline_new
    r_baseline_new["eval_elapsed_sec"] = time.time() - t_eval
    results.append(r_baseline_new)
    logger.info(f"  baseline_new 完了: 学習={train_elapsed_baseline_new:.1f}秒, 評価={r_baseline_new['eval_elapsed_sec']:.1f}秒")

    # ──────────────────────────────────────────────────────────
    # Step 3b: +odds (109 features) モデル学習
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("[Step 3b] +odds モデル学習/評価 (109 features / binary_logloss)")
    logger.info("=" * 70)

    logger.info("  +odds 用学習/検証データ構築 (include_odds=True)...")
    X_train_109, y_train_109, X_valid_109, y_valid_109, valid_races_109 = _build_train_valid_data(
        train_start_month=train_start_month,
        train_end_month=train_end_month,
        valid_month=VALID_MONTH,
        feature_cols=FEATURE_COLUMNS_V2_ODDS,
        all_races=all_races,
        sire_map=sire_map,
        include_odds=True,
    )

    t_train = time.time()
    booster_odds = _train_new_model(
        X_train_109, y_train_109,
        X_valid_109, y_valid_109,
        FEATURE_COLUMNS_V2_ODDS,
        "+odds (109)",
    )
    train_elapsed_odds = time.time() - t_train

    # 通常評価 (argmax(prob))
    t_eval = time.time()
    r_odds = _evaluate_variant(
        booster_odds, valid_races_109, FEATURE_COLUMNS_V2_ODDS, "+odds"
    )
    r_odds["train_elapsed_sec"] = train_elapsed_odds
    r_odds["eval_elapsed_sec"] = time.time() - t_eval
    results.append(r_odds)
    logger.info(f"  +odds 完了: 学習={train_elapsed_odds:.1f}秒, 評価={r_odds['eval_elapsed_sec']:.1f}秒")

    # ──────────────────────────────────────────────────────────
    # Step 3c: +odds+posthoc_EV — 同じモデルで argmax(prob × odds)
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("[Step 3c] +odds+posthoc_EV 評価 (109 features / argmax(prob × odds))")
    logger.info("  ← Phase 2b' の核心: 学習は +odds と同じ、選定ロジックのみ変更")
    logger.info("=" * 70)

    t_eval = time.time()
    r_ev = _evaluate_posthoc_ev(
        booster_odds, valid_races_109, FEATURE_COLUMNS_V2_ODDS,
        variant_name="+odds+posthoc_EV_naive",
    )
    r_ev["train_elapsed_sec"] = 0.0  # 学習なし (同じ booster を使い回し)
    r_ev["eval_elapsed_sec"] = time.time() - t_eval
    results.append(r_ev)
    logger.info(f"  +odds+posthoc_EV_naive 評価完了: {r_ev['eval_elapsed_sec']:.1f}秒")

    # ──────────────────────────────────────────────────────────
    # Phase 2b'' 拡張: 制約付き EV 6 variant (穴馬選びすぎ抑制)
    # 同じ booster_odds を使い回し → 学習コストなし、評価のみ
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("[Phase 2b''] 制約付き EV 6 variant 評価 (穴馬抑制)")
    logger.info("=" * 70)

    constraint_variants = [
        {"odds_cap": 10.0, "prob_min": None, "name": "+odds+EV_oddsmax10"},
        {"odds_cap": 5.0,  "prob_min": None, "name": "+odds+EV_oddsmax5"},
        {"odds_cap": None, "prob_min": 0.10, "name": "+odds+EV_probmin10"},
        {"odds_cap": None, "prob_min": 0.15, "name": "+odds+EV_probmin15"},
        {"odds_cap": 10.0, "prob_min": 0.10, "name": "+odds+EV_om10_pm10"},
        {"odds_cap": 5.0,  "prob_min": 0.15, "name": "+odds+EV_om5_pm15"},
    ]
    for v in constraint_variants:
        t_eval = time.time()
        r_cv = _evaluate_posthoc_ev(
            booster_odds, valid_races_109, FEATURE_COLUMNS_V2_ODDS,
            odds_cap=v["odds_cap"], prob_min=v["prob_min"],
            variant_name=v["name"],
        )
        r_cv["train_elapsed_sec"] = 0.0
        r_cv["eval_elapsed_sec"] = time.time() - t_eval
        results.append(r_cv)
        logger.info(f"  {v['name']} 評価完了: {r_cv['eval_elapsed_sec']:.1f}秒")

    # ──────────────────────────────────────────────────────────
    # Step 7: 結果出力
    # ──────────────────────────────────────────────────────────
    _output_results(results)

    # ──────────────────────────────────────────────────────────
    # Step 8: 改善判定
    # ──────────────────────────────────────────────────────────
    judgment = _judge_improvement(results)

    # ──────────────────────────────────────────────────────────
    # 詳細ログ (各 variant の学習所要時間)
    # ──────────────────────────────────────────────────────────
    print()
    print("=" * 80)
    print("【詳細情報】")
    print("=" * 80)
    for r in results:
        v = r.get("variant", "?")
        train_t = r.get("train_elapsed_sec", 0.0)
        eval_t = r.get("eval_elapsed_sec", 0.0)
        print(f"  {v:<35} 学習={train_t:.1f}秒 評価={eval_t:.1f}秒")

    total_elapsed = time.time() - start_time
    print(f"\n  合計所要時間: {total_elapsed:.1f}秒 ({total_elapsed/60:.1f}分)")
    print("=" * 80)

    logger.info("[Phase 2b' post-hoc EV] 完了")


if __name__ == "__main__":
    main()
