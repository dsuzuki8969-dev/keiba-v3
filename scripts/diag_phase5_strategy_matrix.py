"""M-3 Phase 5: 戦略絞り込み WF 検証 (confidence × Gap フィルタリング)

全 race を買わず confidence の高い race だけ買う戦略で ROI 110%+ 達成可能性を検証。
+odds モデル (109 features) の prob 上位馬を絞り込んで買う = 期待値ベース戦略。

Phase 2c 確定結果 (6 ヶ月 WF):
  +odds (全 race): hit% ≈ 42%, ROI ≈ 79% (マスター基準 110% に -31pt 不足)
  EV_oddsmax10: 採用棄却 (全期間 Δ -3.88pt)

Phase 5 仮説:
  prob 閾値 / Gap (1位-2位差) で race を絞り込み → 高信頼 race のみ買う
  = hit% UP + ROI UP の期待値ベース戦略

variant マトリクス (8 戦略):
  S1:  +odds (全 race) — なし (基準)
  S2:  HighConf>=0.60  — prob 最大馬 prob >= 0.60
  S2b: HighConf>=0.50  — prob 最大馬 prob >= 0.50
  S2c: HighConf>=0.45  — prob 最大馬 prob >= 0.45
  S2d: HighConf>=0.40  — prob 最大馬 prob >= 0.40
  S3:  Gap>=0.10       — prob 1位 - prob 2位 >= 0.10
  S3b: Gap>=0.05       — prob 1位 - prob 2位 >= 0.05
  S4:  Conf>=0.45 AND Gap>=0.05 (S2c AND S3b 組合せ)

WF 検証月 (Phase 2c と同じ 6 ヶ月):
  2024-09: 学習 2024-01〜08 (8ヶ月)
  2024-12: 学習 2024-04〜11 (8ヶ月)
  2025-03: 学習 2024-07〜2025-02 (8ヶ月)
  2025-06: 学習 2024-10〜2025-05 (8ヶ月)
  2025-09: 学習 2025-01〜08 (8ヶ月)
  2025-12: 学習 2025-04〜11 (8ヶ月)

出力:
  data/_diag/phase5_matrix_monthly.csv   — 月別 × 8 戦略 (48 行)
  data/_diag/phase5_matrix_summary.csv   — 全期間集計 (8 行)

重要な前提 (★★★):
  - src/ml/lgbm_model.py は絶対不変
  - ばんえい (venue_65) 除外 (feedback_banei_excluded.md)
  - git commit 禁止
  - 既存ファイル変更禁止
  - Phase 3/4 実施禁止

Usage:
  # フル実行 (6 ヶ月 WF × 8 戦略 / 約 25-30 分)
  python scripts/diag_phase5_strategy_matrix.py

  # デバッグモード (1 月のみ: 2025-12)
  python scripts/diag_phase5_strategy_matrix.py --debug

  # 特定月のみ
  python scripts/diag_phase5_strategy_matrix.py --months 2025-12 2025-09

  # --help
  python scripts/diag_phase5_strategy_matrix.py --help
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

# FEATURE_COLUMNS_V2_ODDS (108 + odds = 109) — Phase 2c と同じ
FEATURE_COLUMNS_V2_ODDS = list(FEATURE_COLUMNS) + ["odds"]

# WF 検証月設定 (Phase 2c と完全に同じ 6 ヶ月)
WF_VALID_MONTHS = [
    {"valid": "2024-09", "train_start": "2024-01", "train_end": "2024-08"},
    {"valid": "2024-12", "train_start": "2024-04", "train_end": "2024-11"},
    {"valid": "2025-03", "train_start": "2024-07", "train_end": "2025-02"},
    {"valid": "2025-06", "train_start": "2024-10", "train_end": "2025-05"},
    {"valid": "2025-09", "train_start": "2025-01", "train_end": "2025-08"},
    {"valid": "2025-12", "train_start": "2025-04", "train_end": "2025-11"},
]

# LightGBM 学習パラメータ — Phase 2c と完全に同じ設定 (公正比較)
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

# マスター基準
MASTER_HIT_PCT_THRESHOLD = 25.0   # hit% >= 25.0%
MASTER_ROI_PCT_THRESHOLD = 110.0  # ROI >= 110.0%

# 8 戦略定義
# prob_min: prob 最大馬の prob 下限 (None = 制限なし)
# gap_min:  prob 1位 - prob 2位 の差 下限 (None = 制限なし)
STRATEGIES = [
    {"id": "S1",  "name": "+odds (全race)",               "prob_min": None, "gap_min": None},
    {"id": "S2",  "name": "HighConf>=0.60",               "prob_min": 0.60, "gap_min": None},
    {"id": "S2b", "name": "HighConf>=0.50",               "prob_min": 0.50, "gap_min": None},
    {"id": "S2c", "name": "HighConf>=0.45",               "prob_min": 0.45, "gap_min": None},
    {"id": "S2d", "name": "HighConf>=0.40",               "prob_min": 0.40, "gap_min": None},
    {"id": "S3",  "name": "Gap>=0.10",                    "prob_min": None, "gap_min": 0.10},
    {"id": "S3b", "name": "Gap>=0.05",                    "prob_min": None, "gap_min": 0.05},
    {"id": "S4",  "name": "Conf>=0.45 AND Gap>=0.05",     "prob_min": 0.45, "gap_min": 0.05},
]


# ============================================================
# _extract_features_v2: odds を追加するラッパー (Phase 2c と同じ)
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
# numpy 変換ヘルパー (各評価関数で共通利用)
# ============================================================

def _to_np(rows: list, cols: List[str]) -> np.ndarray:
    """特徴量辞書リストを numpy 行列に変換する"""
    mat = []
    for f in rows:
        row = []
        for c in cols:
            v = f.get(c) if isinstance(f, dict) else None
            row.append(float(v) if v is not None else float("nan"))
        mat.append(row)
    return np.array(mat, dtype=np.float32)


# ============================================================
# データ構築: 学習/検証 (Phase 2c と同じロジック)
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
    best_auc = booster.best_score.get("valid", {}).get("auc", float("nan"))
    logger.info(f"    学習完了: {elapsed:.1f}秒, best_iter={best_iter}, valid AUC={best_auc:.4f}")

    return booster


# ============================================================
# 戦略評価: prob 閾値 × Gap フィルタリング
# ============================================================

def evaluate_strategy(
    booster: lgb.Booster,
    valid_races_info: list,
    feature_cols: List[str],
    prob_min: Optional[float],
    gap_min: Optional[float],
    strategy_id: str,
) -> dict:
    """
    各 race で prob argmax 馬を ◎ 候補とし、フィルタ条件を適用して ROI 評価する。

    ロジック:
      1. 全馬の prob を計算
      2. top1 = argmax(prob), top2 = 2番目最大 prob 馬
      3. フィルタ:
         - prob_min 指定あり → top1.prob >= prob_min
         - gap_min 指定あり → (top1.prob - top2.prob) >= gap_min
      4. フィルタ通過 race のみ: top1 馬を ◎ として単勝買い → hit 判定 + ROI 集計
      5. フィルタ非通過 race は skip (played カウントしない)

    Args:
        booster:          +odds 学習済みモデル (109 features)
        valid_races_info: _build_train_valid_data の戻り値
        feature_cols:     FEATURE_COLUMNS_V2_ODDS (109 features)
        prob_min:         None または float 閾値 (例: 0.45)
        gap_min:          None または float 閾値 (例: 0.05)
        strategy_id:      戦略 ID (ログ表示用)

    Returns:
        dict with:
          strategy_id, played, hit, hit_pct, roi_pct, payout_sum
    """
    constraints = []
    if prob_min is not None:
        constraints.append(f"prob>={prob_min}")
    if gap_min is not None:
        constraints.append(f"gap>={gap_min}")
    constraints_label = " AND ".join(constraints) if constraints else "なし"
    logger.info(f"    [戦略評価] {strategy_id}: {constraints_label}")

    played = 0
    hit = 0
    payout_sum = 0.0

    for race, horse_dicts, r_labels, r_feats in valid_races_info:
        if not r_feats:
            continue

        X_race = _to_np(r_feats, feature_cols)
        probs = booster.predict(X_race)

        n = len(probs)
        if n == 0:
            continue

        # top1 インデックスと prob 取得
        top1_idx = int(np.argmax(probs))
        top1_prob = float(probs[top1_idx])

        # top2 prob 取得 (馬が 1 頭の場合は top1 と同じにする)
        if n >= 2:
            # top1 を除いた最大
            probs_copy = probs.copy()
            probs_copy[top1_idx] = -np.inf
            top2_prob = float(np.max(probs_copy))
        else:
            top2_prob = top1_prob  # 1 頭レースは差 = 0

        # フィルタ判定
        if prob_min is not None and top1_prob < prob_min:
            continue  # このレースは買わない
        if gap_min is not None and (top1_prob - top2_prob) < gap_min:
            continue  # このレースは買わない

        # フィルタ通過: top1 馬を ◎ として単勝買い
        top1_horse = horse_dicts[top1_idx]
        top1_finish = top1_horse.get("finish_pos")
        is_win = (top1_finish is not None and top1_finish == 1)

        # 単勝オッズ取得
        odds_val = top1_horse.get("odds") or top1_horse.get("tansho_odds")
        tansho_odds = 0.0
        if odds_val is not None:
            try:
                tansho_odds = float(odds_val)
            except (TypeError, ValueError):
                tansho_odds = 0.0

        if tansho_odds <= 0:
            # オッズ不明 race は played としてカウントしない
            continue

        played += 1
        if is_win:
            hit += 1
            payout_sum += tansho_odds * 100  # 100 円購入換算の払戻額

    # 集計
    hit_pct = (hit / played * 100) if played > 0 else 0.0
    roi_pct = (payout_sum / (played * 100) * 100) if played > 0 else 0.0

    logger.info(f"    [{strategy_id}] played={played}, hit={hit}, hit%={hit_pct:.2f}%, ROI={roi_pct:.2f}%")

    return {
        "strategy_id":  strategy_id,
        "played":       played,
        "hit":          hit,
        "hit_pct":      hit_pct,
        "roi_pct":      roi_pct,
        "payout_sum":   payout_sum,
    }


# ============================================================
# 月別結果出力
# ============================================================

def _output_monthly_results(results_monthly: List[dict]) -> str:
    """月別結果を stdout + CSV に出力する

    Args:
        results_monthly: list of {valid_month, strategy_id, name, played, hit, hit_pct, roi_pct, payout_sum}

    Returns:
        CSV ファイルパス
    """
    os.makedirs(DIAG_DIR, exist_ok=True)
    csv_path = os.path.join(DIAG_DIR, "phase5_matrix_monthly.csv")

    print()
    print("=" * 110)
    print("【Phase 5 戦略マトリクス 月別結果】")
    print("=" * 110)
    header = (
        f"{'月':<12} {'戦略 ID':<8} {'戦略名':<32} "
        f"{'played':>8} {'hit':>6} {'hit%':>8} {'ROI%':>9}"
    )
    print(header)
    print("-" * 110)

    # 月ごとに表示
    months_seen = []
    for r in results_monthly:
        m = r["valid_month"]
        if m not in months_seen:
            months_seen.append(m)

    for m in months_seen:
        month_rows = [r for r in results_monthly if r["valid_month"] == m]
        for r in month_rows:
            sid = r["strategy_id"]
            sname = r["strategy_name"]
            played = r["played"]
            hit = r["hit"]
            hit_pct = r["hit_pct"]
            roi_pct = r["roi_pct"]

            # マスター基準達成セルのハイライト
            mark = ""
            if hit_pct >= MASTER_HIT_PCT_THRESHOLD and roi_pct >= MASTER_ROI_PCT_THRESHOLD:
                mark = " ✅"
            elif roi_pct >= 100.0:
                mark = " △"

            print(
                f"{m:<12} {sid:<8} {sname:<32} "
                f"{played:>8} {hit:>6} {hit_pct:>7.2f}% {roi_pct:>8.2f}%{mark}"
            )
        print()

    print("=" * 110)

    # CSV 保存
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "valid_month", "strategy", "played", "hit", "hit_pct", "roi_pct",
        ])
        for r in results_monthly:
            writer.writerow([
                r["valid_month"],
                r["strategy_id"],
                r["played"],
                r["hit"],
                f"{r['hit_pct']:.4f}",
                f"{r['roi_pct']:.4f}",
            ])

    logger.info(f"    月別 CSV 保存: {csv_path}")
    return csv_path


# ============================================================
# 全期間集計出力
# ============================================================

def _output_summary_results(results_monthly: List[dict]) -> str:
    """全期間集計結果 (played 加重) を stdout + CSV に出力する

    Args:
        results_monthly: _output_monthly_results と同じフォーマット

    Returns:
        CSV ファイルパス
    """
    os.makedirs(DIAG_DIR, exist_ok=True)
    csv_path = os.path.join(DIAG_DIR, "phase5_matrix_summary.csv")

    print()
    print("=" * 90)
    print("【Phase 5 戦略マトリクス 全期間集計 (played 加重)】")
    print("=" * 90)
    header = (
        f"{'戦略 ID':<8} {'戦略名':<32} "
        f"{'played_total':>13} {'hit_total':>10} {'hit%_加重':>10} {'ROI%_加重':>10} {'判定':>6}"
    )
    print(header)
    print("-" * 90)

    # 戦略 ID 順に集計
    strategy_order = [s["id"] for s in STRATEGIES]
    strategy_name_map = {s["id"]: s["name"] for s in STRATEGIES}

    summary_rows = []

    for sid in strategy_order:
        rows = [r for r in results_monthly if r["strategy_id"] == sid]
        if not rows:
            continue

        played_total = sum(r["played"] for r in rows)
        hit_total    = sum(r["hit"] for r in rows)
        payout_total = sum(r["payout_sum"] for r in rows)

        hit_pct_weighted = (hit_total / played_total * 100) if played_total > 0 else 0.0
        roi_pct_weighted = (payout_total / (played_total * 100) * 100) if played_total > 0 else 0.0

        # マスター基準判定
        if hit_pct_weighted >= MASTER_HIT_PCT_THRESHOLD and roi_pct_weighted >= MASTER_ROI_PCT_THRESHOLD:
            judgment = "✅ 達成"
        elif roi_pct_weighted >= 100.0:
            judgment = "△ ≥100%"
        else:
            judgment = "❌ 損"

        sname = strategy_name_map.get(sid, "?")

        summary_rows.append({
            "strategy_id":      sid,
            "strategy_name":    sname,
            "played_total":     played_total,
            "hit_total":        hit_total,
            "hit_pct_weighted": hit_pct_weighted,
            "roi_pct_weighted": roi_pct_weighted,
            "judgment":         judgment,
        })

        print(
            f"{sid:<8} {sname:<32} "
            f"{played_total:>13} {hit_total:>10} "
            f"{hit_pct_weighted:>9.2f}% {roi_pct_weighted:>9.2f}%  {judgment}"
        )

    print("=" * 90)

    # マスター基準達成セルの確認
    achieved = [r for r in summary_rows if "✅" in r["judgment"]]
    breakeven = [r for r in summary_rows if "△" in r["judgment"]]

    print()
    print("【マスター基準達成確認 (hit% >= 25.0% AND ROI >= 110.0%)】")
    if achieved:
        print(f"  ✅ 達成戦略 ({len(achieved)} 個):")
        for r in achieved:
            print(f"    {r['strategy_id']}: hit%={r['hit_pct_weighted']:.2f}%, ROI={r['roi_pct_weighted']:.2f}%")
    else:
        print("  ❌ 達成戦略なし")
    if breakeven:
        print(f"  △ ブレイクイーブン以上 ({len(breakeven)} 個):")
        for r in breakeven:
            print(f"    {r['strategy_id']}: hit%={r['hit_pct_weighted']:.2f}%, ROI={r['roi_pct_weighted']:.2f}%")
    print("=" * 90)

    # CSV 保存
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "strategy", "played_total", "hit_total",
            "hit_pct_weighted", "roi_pct_weighted", "judgment",
        ])
        for r in summary_rows:
            writer.writerow([
                r["strategy_id"],
                r["played_total"],
                r["hit_total"],
                f"{r['hit_pct_weighted']:.4f}",
                f"{r['roi_pct_weighted']:.4f}",
                r["judgment"],
            ])

    logger.info(f"    集計 CSV 保存: {csv_path}")
    return csv_path


# ============================================================
# メイン
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "M-3 Phase 5: 戦略絞り込み WF 検証 — "
            "confidence (prob 閾値) × Gap (1位-2位差) の組合せで race を絞り込む戦略を WF 6 月で評価する"
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
    args = parser.parse_args()

    start_time = time.time()

    logger.info("=" * 70)
    logger.info("[Phase 5 戦略絞り込み WF 検証] 開始")
    logger.info(f"  戦略数: {len(STRATEGIES)}")
    logger.info(f"  検証月: {[c['valid'] for c in WF_VALID_MONTHS]}")
    logger.info(f"  マスター基準: hit% >= {MASTER_HIT_PCT_THRESHOLD}% AND ROI >= {MASTER_ROI_PCT_THRESHOLD}%")
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

    # 月別 × 戦略 の結果蓄積リスト
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

        # +odds モデル学習 (1 月 1 回のみ / 8 戦略は同じモデルを共有)
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

        # AUC 計算 (参考値)
        valid_feats_flat = []
        valid_labels_flat = []
        for _, _, r_labels, r_feats in valid_races_info:
            valid_feats_flat.extend(r_feats)
            valid_labels_flat.extend(r_labels)
        X_valid_all = _to_np(valid_feats_flat, FEATURE_COLUMNS_V2_ODDS)
        valid_preds_all = booster.predict(X_valid_all)
        auc_val = float("nan")
        if len(set(valid_labels_flat)) == 2:
            try:
                auc_val = roc_auc_score(valid_labels_flat, valid_preds_all)
            except Exception:
                pass
        logger.info(f"  valid AUC={auc_val:.4f}")

        # ─────────────────────────────────────────────────────
        # [Step 3] 8 戦略を評価 (同じ booster / valid_races_info を使い回す)
        # ─────────────────────────────────────────────────────
        logger.info(f"  [Step 3] 8 戦略評価開始 (valid={valid_month})...")
        for strat in STRATEGIES:
            sid       = strat["id"]
            sname     = strat["name"]
            prob_min  = strat["prob_min"]
            gap_min   = strat["gap_min"]

            t_eval = time.time()
            result = evaluate_strategy(
                booster=booster,
                valid_races_info=valid_races_info,
                feature_cols=FEATURE_COLUMNS_V2_ODDS,
                prob_min=prob_min,
                gap_min=gap_min,
                strategy_id=sid,
            )
            logger.info(f"    {sid} 評価完了: {time.time()-t_eval:.1f}秒")

            # 月別結果を蓄積
            results_monthly.append({
                "valid_month":    valid_month,
                "strategy_id":    sid,
                "strategy_name":  sname,
                "played":         result["played"],
                "hit":            result["hit"],
                "hit_pct":        result["hit_pct"],
                "roi_pct":        result["roi_pct"],
                "payout_sum":     result["payout_sum"],
            })

        # 月別サマリ (S1 = 基準 と S4 = 最強候補 を比較表示)
        s1 = next((r for r in results_monthly if r["valid_month"] == valid_month and r["strategy_id"] == "S1"), None)
        s4 = next((r for r in results_monthly if r["valid_month"] == valid_month and r["strategy_id"] == "S4"), None)
        if s1 and s4:
            logger.info(
                f"  [{valid_month} サマリ] "
                f"S1(基準) ROI={s1['roi_pct']:.2f}% | "
                f"S4(複合) ROI={s4['roi_pct']:.2f}% (Δ{s4['roi_pct']-s1['roi_pct']:+.2f}pt) "
                f"played={s4['played']} vs {s1['played']}"
            )

    # ──────────────────────────────────────────────────────────
    # 全月完了後: 結果出力
    # ──────────────────────────────────────────────────────────
    if not results_monthly:
        logger.error("❌ 有効な月別結果がありません。CSV 出力スキップ。")
        sys.exit(1)

    logger.info("")
    logger.info("[出力] 月別結果 + 全期間集計...")
    csv_monthly  = _output_monthly_results(results_monthly)
    csv_summary  = _output_summary_results(results_monthly)

    total_elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print("【Phase 5 完了】")
    print(f"  合計所要時間: {total_elapsed:.1f}秒 ({total_elapsed/60:.1f}分)")
    print(f"  月別 CSV:     {csv_monthly}")
    print(f"  集計 CSV:     {csv_summary}")
    print("=" * 70)

    logger.info("[Phase 5 戦略絞り込み WF 検証] 完了")


if __name__ == "__main__":
    main()
