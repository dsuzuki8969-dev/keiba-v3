"""M-3 Phase 2a パイロット: odds/popularity 特徴量取込 効果検証

既存 lgbm_place.txt (108 features) をベースラインとし、
odds (+1 feature = 109) / odds+popularity (+2 features = 110) を追加した
モデルを 2025-04〜11 (8 ヶ月) で新規学習し、2025-12 で比較評価する。

評価指標:
  - AUC (binary classification)
  - TOP1 hit% (◎ 単勝 hit 率)
  - tansho ROI (◎ 単勝回収率 %)

重要な前提 (★★★):
  - src/ml/lgbm_model.py は絶対不変
  - odds は当日確定値 = リーク無 (履歴 odds は使わない)
  - 目的変数 = head_top3 (3 着以内 = 1 / それ以外 = 0)
  - subagent は git commit 禁止

Usage:
  # Step 1 のみ (odds データ存在確認)
  python scripts/diag_phase2_pilot_odds_features.py --check-only

  # フル実行 (3 variant 比較)
  python scripts/diag_phase2_pilot_odds_features.py

  # デバッグ (少サンプル / 短学習期間)
  python scripts/diag_phase2_pilot_odds_features.py --debug
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

# Phase 2a パイロット設定
# option A: baseline は wf_2026 の既存 lgbm_place.txt をそのまま使用
BASELINE_MODEL_PATH = os.path.join(MODEL_DIR, "lgbm_place.txt")

# 学習期間: 2025-04〜11 (8 ヶ月)
TRAIN_START_MONTH = "2025-04"  # inclusive
TRAIN_END_MONTH   = "2025-11"  # inclusive (< 2025-12)

# 検証期間: 2025-12 (1 ヶ月)
VALID_MONTH = "2025-12"

# FEATURE_COLUMNS_V2 (108 + odds = 109)
FEATURE_COLUMNS_V2_ODDS = list(FEATURE_COLUMNS) + ["odds"]

# FEATURE_COLUMNS_V2 (108 + odds + popularity = 110)
FEATURE_COLUMNS_V2_FULL = list(FEATURE_COLUMNS) + ["odds", "popularity"]

# LightGBM 学習パラメータ (公正比較のため baseline に近い設定)
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

# 学習イテレーション上限 (early_stopping あり)
NUM_BOOST_ROUND = 200
EARLY_STOPPING_ROUNDS = 20


# ============================================================
# Step 1: odds データ存在調査
# ============================================================

def step1_check_odds_data(all_races: list) -> dict:
    """
    horse dict に odds / popularity が含まれているか確認する。

    Returns:
        {
          'has_odds': bool,
          'has_popularity': bool,
          'sample_horse_keys': list,
          'sample_odds': any,
          'sample_popularity': any,
          'odds_null_rate': float,
          'pop_null_rate': float,
        }
    """
    logger.info("=" * 60)
    logger.info("[Step 1] odds/popularity データ存在調査")
    logger.info("=" * 60)

    # 代表 race を 3 つ確認 (index 100, 500, 1000)
    sample_indices = [100, 500, 1000]
    result = {}

    for idx in sample_indices:
        if idx < len(all_races):
            race = all_races[idx]
            horses = race.get("horses", [])
            if horses:
                h = horses[0]
                logger.info(f"  race[{idx}] date={race.get('date')} horse_keys={list(h.keys())[:20]}")
                logger.info(f"  race[{idx}] odds={h.get('odds')} popularity={h.get('popularity')} tansho_odds={h.get('tansho_odds')}")
                if not result:
                    result["sample_horse_keys"] = list(h.keys())
                    result["sample_odds"] = h.get("odds") or h.get("tansho_odds")
                    result["sample_popularity"] = h.get("popularity")

    # NULL 率を 2025-12 月の race で計算
    target_races = [r for r in all_races if r.get("date", "").startswith(VALID_MONTH)]
    total_h = odds_null = pop_null = 0
    for race in target_races:
        for h in race.get("horses", []):
            if h.get("finish_pos") is None:
                continue
            total_h += 1
            odds_val = h.get("odds") or h.get("tansho_odds")
            if odds_val is None:
                odds_null += 1
            if h.get("popularity") is None:
                pop_null += 1

    if total_h > 0:
        result["odds_null_rate"] = odds_null / total_h
        result["pop_null_rate"] = pop_null / total_h
    else:
        result["odds_null_rate"] = 1.0
        result["pop_null_rate"] = 1.0

    result["has_odds"] = result.get("odds_null_rate", 1.0) < 0.95
    result["has_popularity"] = result.get("pop_null_rate", 1.0) < 0.95
    result["valid_month_horses"] = total_h

    logger.info(f"  2025-12 月 horse 数: {total_h}")
    logger.info(f"  odds NULL 率: {result['odds_null_rate']:.3f} (has_odds={result['has_odds']})")
    logger.info(f"  popularity NULL 率: {result['pop_null_rate']:.3f} (has_popularity={result['has_popularity']})")

    if not result["has_odds"]:
        logger.error("  ❌ odds データが ML データに含まれていません！")
        logger.error("  → Phase 2a 作業中断。Opus に報告してください。")
    else:
        logger.info("  ✅ odds データ確認 OK → Phase 2a 続行")

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
    include_popularity: bool = True,
) -> dict:
    """
    既存 _extract_features をラップして odds/popularity を追加する。

    重要: src/ml/lgbm_model.py は絶対不変。
    このラッパーは scripts/ 内にのみ存在する。

    odds: horse.get("odds") — 当日確定 tansho odds (リーク無)
           horse に odds がない場合は tansho_odds を試みる
    popularity: horse.get("popularity") — 当日確定 人気順位
    """
    feat = _extract_features(horse, race, tracker, sire_tracker)

    if include_odds:
        # odds / tansho_odds どちらでも受け付ける (ML data に格納されているキーに依存)
        odds_val = horse.get("odds")
        if odds_val is None:
            odds_val = horse.get("tansho_odds")
        feat["odds"] = odds_val

    if include_popularity:
        feat["popularity"] = horse.get("popularity")

    return feat


# ============================================================
# Step 3: 学習データ + 検証データ構築
# ============================================================

def _build_train_valid_data(
    train_start_month: str,
    train_end_month: str,
    valid_month: str,
    feature_cols: List[str],
    all_races: list,
    sire_map: dict,
    include_odds: bool = False,
    include_popularity: bool = False,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list]:
    """
    学習/検証データを構築する。

    walk_forward_backtest.py の手法を参考に再実装。
    既存 _process_month とは異なり、DB 保存・pred.json 書き込みは行わない。
    診断スクリプト専用の実装。

    Args:
        train_start_month: '2025-04' (inclusive)
        train_end_month:   '2025-11' (inclusive)
        valid_month:       '2025-12'
        feature_cols:      使用する特徴量列 (FEATURE_COLUMNS or V2 variants)
        all_races:         _load_ml_races() の戻り値
        sire_map:          _load_horse_sire_map() の戻り値
        include_odds:      True なら odds を追加
        include_popularity: True なら popularity を追加

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
    logger.info(f"  odds={include_odds}, popularity={include_popularity}")

    # tracker は学習期間全体を前処理してから使う
    # walk_forward_backtest と同じパターン:
    #   - train/valid race 全てを走査
    #   - train race では tracker.update_race を呼んだ「前」の状態で特徴量を抽出
    #   - (= 未来データ混入なし)
    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    # Step A: tracker を train_start 前のデータで更新 (pre-warmup)
    # ばんえい (venue_code="65") を除外 (CLAUDE.md feedback_banei_excluded.md)
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

    # Step B: 学習/検証データ収集 (tracker はここで都度更新)
    train_feats, train_labels = [], []
    valid_races_info = []  # (race_dict, horse_dicts, labels, raw_feats)

    # ばんえい (venue_code="65") を除外 (CLAUDE.md feedback_banei_excluded.md)
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
                include_popularity=include_popularity,
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

        # tracker は学習・検証関係なく更新 (時系列漏洩なし = 当 race の update は次 race から有効)
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
# Step 4-5: 各 variant の学習
# ============================================================

def _train_new_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    feature_cols: List[str],
    variant_name: str,
) -> lgb.Booster:
    """新規 LightGBM モデルを学習して返す"""
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
# Step 6: ROI 計算
# ============================================================

def _evaluate_variant(
    booster: lgb.Booster,
    valid_races_info: list,
    feature_cols: List[str],
    variant_name: str,
    is_baseline: bool = False,
) -> dict:
    """
    検証データで AUC + TOP1 hit% + tansho ROI を計算する。

    Args:
        booster: 学習済みモデル (baseline の場合は 108 features で学習済み)
        valid_races_info: _build_train_valid_data の戻り値
        feature_cols: このモデルが使う特徴量列
        variant_name: ログ表示用
        is_baseline: True の場合は raw feat から FEATURE_COLUMNS のみ使う

    Returns:
        {'auc': float, 'top1_hit_pct': float, 'tansho_roi_pct': float,
         'played': int, 'hit': int, 'payout_sum': float}
    """
    logger.info(f"  [評価] variant={variant_name}")

    def _to_np(rows: list, cols: List[str]) -> np.ndarray:
        mat = []
        for f in rows:
            row = []
            for c in cols:
                v = f.get(c) if isinstance(f, dict) else None
                row.append(float(v) if v is not None else float("nan"))
            mat.append(row)
        return np.array(mat, dtype=np.float32)

    # 全検証 horse を収集 (AUC 計算用)
    all_y_true = []
    all_y_pred = []

    # ROI 計算用
    played = 0
    hit = 0
    payout_sum = 0.0

    for race, horse_dicts, r_labels, r_feats in valid_races_info:
        if not r_feats:
            continue

        # 特徴量行列構築
        X = _to_np(r_feats, feature_cols)
        raw_preds = booster.predict(X)

        # AUC 用に蓄積
        all_y_true.extend(r_labels)
        all_y_pred.extend(raw_preds.tolist())

        # ROI 計算: prob 最大の horse を ◎ とする
        if len(raw_preds) == 0:
            continue
        top_idx = int(np.argmax(raw_preds))
        top_horse = horse_dicts[top_idx]
        top_label = r_labels[top_idx]  # 1 = 3 着以内

        # ◎ が 1 着かどうか (TOP1 hit = 1 着のみカウント)
        top_finish = top_horse.get("finish_pos")
        is_win = (top_finish is not None and top_finish == 1)

        # 単勝オッズ取得 (修正: or チェーン → None 明示チェックに変更)
        odds_val = top_horse.get("odds")
        if odds_val is None:
            odds_val = top_horse.get("tansho_odds")
        tansho_odds = 0.0
        if odds_val is not None:
            try:
                tansho_odds = float(odds_val)
            except (TypeError, ValueError):
                tansho_odds = 0.0

        # 単勝オッズ取得不可 = ROI 計算から除外 (played にもカウントしない)
        if tansho_odds <= 0:
            continue

        played += 1
        if is_win:
            hit += 1
            # 日本の単勝配当: odds × 100 円投票 (odds は倍率)
            payout_sum += tansho_odds * 100

    # AUC
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
    csv_path = os.path.join(DIAG_DIR, "phase2_pilot_summary.csv")

    print()
    print("=" * 80)
    print("【Phase 2a パイロット 結果サマリー】")
    print("=" * 80)
    print(f"{'variant':<25} {'features':>8} {'AUC':>8} {'TOP1_hit%':>10} {'tansho_ROI%':>12} {'played':>7} {'hit':>5}")
    print("-" * 80)

    for r in results:
        v = r.get("variant", "?")
        feats = r.get("features", 0)
        auc = r.get("auc", float("nan"))
        top1 = r.get("top1_hit_pct", 0.0)
        roi = r.get("tansho_roi_pct", 0.0)
        played = r.get("played", 0)
        hit = r.get("hit", 0)
        auc_str = f"{auc:.4f}" if auc == auc else "N/A"
        print(f"{v:<25} {feats:>8} {auc_str:>8} {top1:>9.2f}% {roi:>11.2f}% {played:>7} {hit:>5}")

    print("-" * 80)
    print()

    # CSV 保存
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["variant", "features", "AUC", "TOP1_hit_pct", "tansho_ROI_pct", "played", "hit", "payout_sum"])
        for r in results:
            auc = r.get("auc", float("nan"))
            auc_str = f"{auc:.6f}" if auc == auc else ""
            writer.writerow([
                r.get("variant", ""),
                r.get("features", 0),
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
    baseline vs +odds の差分を判定し、Phase 2b 推奨を返す。

    判定基準:
      +5pt 以上 (AUC または ROI) → Phase 2b (ROI loss) へ進む価値高い
      +1〜5pt → Phase 2b で更に積み上げる価値あり
      ±1pt 以内 → Phase 2 全体方針見直しが必要
    """
    if len(results) < 2:
        return "データ不足で判定不可"

    # baseline_new を優先使用 (公正比較用・同期間学習); なければ baseline (既存 wf_2026)
    baseline = next((r for r in results if r.get("variant") == "baseline_new"), None) \
            or next((r for r in results if r.get("variant") == "baseline"), None)
    plus_odds = next((r for r in results if r.get("variant") == "+odds"), None)
    plus_full = next((r for r in results if r.get("variant") == "+odds+popularity"), None)

    if baseline is None or plus_odds is None:
        return "baseline または +odds 結果なし"

    delta_auc = (plus_odds.get("auc", 0) - baseline.get("auc", 0)) * 100  # pt
    delta_roi = plus_odds.get("tansho_roi_pct", 0) - baseline.get("tansho_roi_pct", 0)

    # 比較基準として使った baseline 名を明示
    baseline_label = baseline.get("variant", "baseline")

    print("=" * 80)
    print("【Phase 2a 改善判定】")
    print(f"  (比較基準: {baseline_label})")
    print("=" * 80)
    print(f"  {baseline_label:<20} AUC={baseline.get('auc', float('nan')):.4f}  ROI={baseline.get('tansho_roi_pct', 0):.2f}%")
    print(f"  +odds               AUC={plus_odds.get('auc', float('nan')):.4f}  ROI={plus_odds.get('tansho_roi_pct', 0):.2f}%")
    if plus_full:
        print(f"  +odds+pop AUC={plus_full.get('auc', float('nan')):.4f}  ROI={plus_full.get('tansho_roi_pct', 0):.2f}%")
    print()
    print(f"  Δ AUC (baseline → +odds): {delta_auc:+.2f}pt")
    print(f"  Δ ROI (baseline → +odds): {delta_roi:+.2f}pt")
    print()

    max_delta = max(abs(delta_auc), abs(delta_roi))

    if max_delta >= 5.0:
        judgment = "✅ +5pt 以上改善 → Phase 2b (ROI loss custom objective) へ進む価値高い"
        recommendation = "Phase 2b 進行"
    elif max_delta >= 1.0:
        judgment = "⚠️ +1〜5pt 改善 → Phase 2b で更に積み上げる価値あり"
        recommendation = "Phase 2b 進行 (慎重に)"
    else:
        judgment = "❌ ±1pt 以内 → Phase 2 全体方針見直しが必要 (odds 取込だけでは不十分)"
        recommendation = "Phase 2 方針見直し"

    print(f"  判定: {judgment}")
    print(f"  推奨アクション: {recommendation}")
    print("=" * 80)

    return judgment


# ============================================================
# メイン
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 2a パイロット: odds/popularity 特徴量取込検証")
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Step 1 (odds データ存在確認) のみ実行",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="デバッグモード (学習期間短縮 / パラメータ簡略化)",
    )
    parser.add_argument(
        "--skip-baseline",
        action="store_true",
        help="baseline (既存 lgbm_place.txt) の評価をスキップ",
    )
    args = parser.parse_args()

    start_time = time.time()

    logger.info("=" * 60)
    logger.info("[Phase 2a パイロット] 開始")
    logger.info(f"  学習期間: {TRAIN_START_MONTH} 〜 {TRAIN_END_MONTH}")
    logger.info(f"  検証期間: {VALID_MONTH}")
    logger.info(f"  モデル: baseline (lgbm_place.txt) + +odds (109) + +odds+popularity (110)")
    logger.info("=" * 60)

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
        logger.error("❌ odds データが ML データに存在しません。Phase 2a 作業中断。")
        logger.error("Opus に「odds/popularity が ML data に含まれていない」と報告してください。")
        sys.exit(1)

    if args.check_only:
        print("\n[Step 1 結果]")
        print(f"  has_odds: {check_result['has_odds']}")
        print(f"  has_popularity: {check_result['has_popularity']}")
        print(f"  sample odds: {check_result.get('sample_odds')}")
        print(f"  sample popularity: {check_result.get('sample_popularity')}")
        print(f"  2025-12 月 horse 数: {check_result.get('valid_month_horses')}")
        print(f"  odds NULL 率: {check_result.get('odds_null_rate', 0):.3f}")
        print(f"  popularity NULL 率: {check_result.get('pop_null_rate', 0):.3f}")
        logger.info("--check-only モード: ここで終了")
        return

    # デバッグモード設定
    # debug 時は局所変数でモジュール定数を上書き
    train_start_month = TRAIN_START_MONTH
    train_end_month = TRAIN_END_MONTH
    if args.debug:
        train_start_month = "2025-10"  # 2 ヶ月のみ
        train_end_month = "2025-11"
        logger.info("  デバッグモード: 学習期間 2025-10〜11 (2 ヶ月)")

    results = []

    # ──────────────────────────────────────────────────────────
    # Step 4: baseline 評価 (option A: 既存 lgbm_place.txt)
    # ──────────────────────────────────────────────────────────
    if not args.skip_baseline:
        logger.info("")
        logger.info("=" * 60)
        logger.info("[Step 4] baseline 評価 (既存 lgbm_place.txt / 108 features)")
        logger.info("=" * 60)

        if not os.path.exists(BASELINE_MODEL_PATH):
            logger.error(f"  baseline モデルが見つかりません: {BASELINE_MODEL_PATH}")
            sys.exit(1)

        booster_baseline = lgb.Booster(model_file=BASELINE_MODEL_PATH)
        logger.info(f"  baseline モデル読み込み完了: {BASELINE_MODEL_PATH}")

        # baseline 用の検証データ (FEATURE_COLUMNS 108 個のみ)
        logger.info("  baseline 用検証データ構築 (108 features, include_odds=False)...")
        _, _, _, _, valid_races_baseline = _build_train_valid_data(
            train_start_month=train_start_month,
            train_end_month=train_end_month,
            valid_month=VALID_MONTH,
            feature_cols=FEATURE_COLUMNS,
            all_races=all_races,
            sire_map=sire_map,
            include_odds=False,
            include_popularity=False,
        )

        t_eval0 = time.time()
        r_baseline = _evaluate_variant(
            booster_baseline,
            valid_races_baseline,
            FEATURE_COLUMNS,
            "baseline",
            is_baseline=True,
        )
        r_baseline["train_elapsed_sec"] = 0.0  # 学習なし (読み込みのみ)
        r_baseline["eval_elapsed_sec"] = time.time() - t_eval0
        results.append(r_baseline)
        logger.info(f"  baseline 評価完了: {r_baseline['eval_elapsed_sec']:.1f}秒")

    # ──────────────────────────────────────────────────────────
    # Step 4b: baseline_new (公正比較用 / 108 features / 同期間学習)
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("[Step 4b] baseline_new (公正比較用 / 108 features / 同期間学習)")
    logger.info("=" * 60)

    logger.info("  baseline_new 用学習/検証データ構築 (108 features, odds/popularity なし)...")
    X_train_108, y_train_108, X_valid_108, y_valid_108, valid_races_108_new = _build_train_valid_data(
        train_start_month=train_start_month,
        train_end_month=train_end_month,
        valid_month=VALID_MONTH,
        feature_cols=list(FEATURE_COLUMNS),  # 108 features (odds/popularity なし)
        all_races=all_races,
        sire_map=sire_map,
        include_odds=False,
        include_popularity=False,
    )

    t_train_baseline_new = time.time()
    booster_baseline_new = _train_new_model(
        X_train_108, y_train_108,
        X_valid_108, y_valid_108,
        list(FEATURE_COLUMNS),
        "baseline_new (108 / 同期間)",
    )
    train_elapsed_baseline_new = time.time() - t_train_baseline_new

    t_eval_baseline_new = time.time()
    r_baseline_new = _evaluate_variant(
        booster_baseline_new, valid_races_108_new, list(FEATURE_COLUMNS), "baseline_new"
    )
    r_baseline_new["train_elapsed_sec"] = train_elapsed_baseline_new
    r_baseline_new["eval_elapsed_sec"] = time.time() - t_eval_baseline_new
    results.append(r_baseline_new)

    logger.info(f"  baseline_new 学習完了: {train_elapsed_baseline_new:.1f}秒")
    logger.info(f"  baseline_new 評価完了: {r_baseline_new['eval_elapsed_sec']:.1f}秒")

    # ──────────────────────────────────────────────────────────
    # Step 5a: +odds モデル学習 + 評価 (109 features)
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("[Step 5a] +odds モデル学習 (109 features)")
    logger.info("=" * 60)

    logger.info("  +odds 用学習/検証データ構築 (include_odds=True)...")
    X_train_109, y_train_109, X_valid_109, y_valid_109, valid_races_109 = _build_train_valid_data(
        train_start_month=train_start_month,
        train_end_month=train_end_month,
        valid_month=VALID_MONTH,
        feature_cols=FEATURE_COLUMNS_V2_ODDS,
        all_races=all_races,
        sire_map=sire_map,
        include_odds=True,
        include_popularity=False,
    )

    t_train_odds = time.time()
    booster_odds = _train_new_model(
        X_train_109, y_train_109,
        X_valid_109, y_valid_109,
        FEATURE_COLUMNS_V2_ODDS,
        "+odds (109)",
    )
    train_elapsed_odds = time.time() - t_train_odds

    t_eval_odds = time.time()
    r_odds = _evaluate_variant(
        booster_odds, valid_races_109, FEATURE_COLUMNS_V2_ODDS, "+odds"
    )
    r_odds["train_elapsed_sec"] = train_elapsed_odds
    r_odds["eval_elapsed_sec"] = time.time() - t_eval_odds
    results.append(r_odds)

    logger.info(f"  +odds 学習完了: {train_elapsed_odds:.1f}秒")
    logger.info(f"  +odds 評価完了: {r_odds['eval_elapsed_sec']:.1f}秒")

    # ──────────────────────────────────────────────────────────
    # Step 5b: +odds+popularity モデル学習 + 評価 (110 features)
    # ──────────────────────────────────────────────────────────
    if check_result.get("has_popularity", False):
        logger.info("")
        logger.info("=" * 60)
        logger.info("[Step 5b] +odds+popularity モデル学習 (110 features)")
        logger.info("=" * 60)

        logger.info("  +odds+popularity 用学習/検証データ構築...")
        X_train_110, y_train_110, X_valid_110, y_valid_110, valid_races_110 = _build_train_valid_data(
            train_start_month=train_start_month,
            train_end_month=train_end_month,
            valid_month=VALID_MONTH,
            feature_cols=FEATURE_COLUMNS_V2_FULL,
            all_races=all_races,
            sire_map=sire_map,
            include_odds=True,
            include_popularity=True,
        )

        t_train_full = time.time()
        booster_full = _train_new_model(
            X_train_110, y_train_110,
            X_valid_110, y_valid_110,
            FEATURE_COLUMNS_V2_FULL,
            "+odds+popularity (110)",
        )
        train_elapsed_full = time.time() - t_train_full

        t_eval_full = time.time()
        r_full = _evaluate_variant(
            booster_full, valid_races_110, FEATURE_COLUMNS_V2_FULL, "+odds+popularity"
        )
        r_full["train_elapsed_sec"] = train_elapsed_full
        r_full["eval_elapsed_sec"] = time.time() - t_eval_full
        results.append(r_full)

        logger.info(f"  +odds+popularity 学習完了: {train_elapsed_full:.1f}秒")
        logger.info(f"  +odds+popularity 評価完了: {r_full['eval_elapsed_sec']:.1f}秒")
    else:
        logger.warning("  popularity データ NULL 率が高いため +odds+popularity variant をスキップ")

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
        print(f"  {v:<25} 学習={train_t:.1f}秒 評価={eval_t:.1f}秒")

    total_elapsed = time.time() - start_time
    print(f"\n  合計所要時間: {total_elapsed:.1f}秒 ({total_elapsed/60:.1f}分)")
    print("=" * 80)

    logger.info("[Phase 2a パイロット] 完了")


if __name__ == "__main__":
    main()
