"""M-3 Phase 2b パイロット: ROI 期待値 custom objective 効果検証

Phase 2a (+odds / 79.06%) をベースに、LightGBM custom objective で
ROI 期待値最大化を試みる。

variant 構成:
  - baseline_new   (108 features / binary_logloss) — Phase 2a 結果流用
  - +odds          (109 features / binary_logloss) — Phase 2a 結果流用
  - +odds+ROI_loss (109 features / ROI weighted BCE custom objective) ← 新規

評価指標:
  - AUC (binary classification)
  - TOP1 hit% (◎ 単勝 hit 率 = 1 着)
  - tansho ROI (◎ 単勝回収率 %)

合格ライン:
  - +odds 79.06% → +5pt 以上 (= 84.06%+) で Phase 2c 進行価値高い

重要な前提 (★★★):
  - src/ml/lgbm_model.py は絶対不変
  - src/ml/lgbm_roi_objective.py のみ追加 (既存ファイル変更禁止)
  - ばんえい (venue_65) 除外 (feedback_banei_excluded.md)
  - git commit 禁止
  - Phase 2c (全モデル run) 禁止

Usage:
  # フル実行 (3 variant)
  python scripts/diag_phase2b_pilot_roi_objective.py

  # デバッグモード (学習期間短縮)
  python scripts/diag_phase2b_pilot_roi_objective.py --debug

  # Phase 2a 結果を流用して ROI_loss のみ追加
  python scripts/diag_phase2b_pilot_roi_objective.py --roi-only
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
from src.ml.lgbm_roi_objective import (
    make_roi_metric,
    make_roi_objective,
    make_roi_objective_log,
    make_sample_weights,
)

logger = get_logger(__name__)

# ============================================================
# 定数
# ============================================================

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODEL_DIR = os.path.join(ROOT, "data", "models", "wf_2026")
DIAG_DIR = os.path.join(ROOT, "data", "_diag")

# Phase 2a/2b パイロット設定 (Phase 2a と同一期間で公正比較)
TRAIN_START_MONTH = "2025-04"   # inclusive
TRAIN_END_MONTH   = "2025-11"   # inclusive (< 2025-12)
VALID_MONTH       = "2025-12"

# 特徴量リスト
FEATURE_COLUMNS_ODDS = list(FEATURE_COLUMNS) + ["odds"]       # 109 features

# LightGBM 学習パラメータ (Phase 2a と同設定)
BASE_PARAMS = {
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


# ============================================================
# _extract_features_v2: odds 列追加ラッパー (Phase 2a から流用)
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
    src/ml/lgbm_model.py は絶対不変のためここで拡張。
    """
    feat = _extract_features(horse, race, tracker, sire_tracker)
    if include_odds:
        odds_val = horse.get("odds")
        if odds_val is None:
            odds_val = horse.get("tansho_odds")
        feat["odds"] = odds_val
    return feat


# ============================================================
# データ構築 (Phase 2a と同ロジック)
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

    Returns:
        X_train, y_train, X_valid, y_valid, valid_races_info
        valid_races_info: list of (race_dict, horse_dicts, y_labels, raw_feats)
    """
    train_start = f"{train_start_month}-01"
    train_end   = f"{valid_month}-01"  # 2025-12-01 → 学習は < 2025-12-01
    valid_end_y, valid_end_m = int(valid_month[:4]), int(valid_month[5:7])
    if valid_end_m == 12:
        valid_end_y += 1
        valid_end_m = 1
    else:
        valid_end_m += 1
    valid_end = f"{valid_end_y:04d}-{valid_end_m:02d}-01"

    logger.info(f"  学習期間: {train_start} 〜 {train_end} (exclusive)")
    logger.info(f"  検証期間: {valid_month} (〜 {valid_end})")
    logger.info(f"  特徴量数: {len(feature_cols)}, include_odds={include_odds}")

    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    # pre-warmup: train_start 以前のデータで tracker を温める
    pre_warmup_races = [
        r for r in all_races
        if r.get("date", "") < train_start
        and str(r.get("venue_code", "")) != "65"  # ばんえい除外
    ]
    logger.info(f"  tracker pre-warmup: {len(pre_warmup_races)} レース (ばんえい除外済)")
    t0 = time.time()
    for i, race in enumerate(pre_warmup_races):
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)
        if (i + 1) % 10000 == 0:
            pct = (i + 1) / len(pre_warmup_races) * 100
            logger.info(f"    pre-warmup: {i+1}/{len(pre_warmup_races)} ({pct:.0f}%)")
    logger.info(f"  pre-warmup 完了: {time.time()-t0:.1f}秒")

    # 学習/検証データ収集
    train_feats, train_labels = [], []
    valid_races_info = []

    target_races = [
        r for r in all_races
        if train_start <= r.get("date", "") < valid_end
        and str(r.get("venue_code", "")) != "65"  # ばんえい除外
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

        # tracker は時系列を保つため学習・検証ともに更新
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

        if (i + 1) % 2000 == 0:
            pct = (i + 1) / len(target_races) * 100
            elapsed = time.time() - t1
            logger.info(f"    データ収集: {i+1}/{len(target_races)} ({pct:.0f}%) {elapsed:.1f}秒")

    logger.info(f"  データ収集完了: {time.time()-t1:.1f}秒")
    logger.info(f"  学習サンプル数: {len(train_labels)}, 検証レース数: {len(valid_races_info)}")

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

    valid_feats_all = []
    valid_labels_all = []
    for _, _, r_labels, r_feats in valid_races_info:
        valid_feats_all.extend(r_feats)
        valid_labels_all.extend(r_labels)

    X_valid = _to_np(valid_feats_all, feature_cols)
    y_valid = np.array(valid_labels_all, dtype=np.int32)

    nan_rate = np.isnan(X_train).mean()
    logger.info(f"  X_train shape: {X_train.shape}, NaN率: {nan_rate:.3f}")
    logger.info(f"  X_valid shape: {X_valid.shape}")
    logger.info(f"  y_train 正例率: {y_train.mean():.3f}, y_valid 正例率: {y_valid.mean():.3f}")

    return X_train, y_train, X_valid, y_valid, valid_races_info


# ============================================================
# モデル学習 (variant 別パラメータ制御)
# ============================================================

def _train_variant(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    feature_cols: List[str],
    variant_name: str,
    odds_col_index: Optional[int] = None,  # odds 列のインデックス (ROI variant 用)
) -> lgb.Booster:
    """
    variant 名に応じて objective / sample_weight を切り替えて学習する。

    Args:
        odds_col_index: X_train の何列目が odds か (ROI variant 用)
                        None の場合は ROI objective を使わない
    """
    logger.info(f"  [学習] variant={variant_name}, 特徴量数={len(feature_cols)}")
    logger.info(f"  X_train={X_train.shape}, X_valid={X_valid.shape}")

    cat_feats = [c for c in CATEGORICAL_FEATURES if c in feature_cols]
    params = dict(BASE_PARAMS)

    # odds 配列を取得 (ROI variant 用)
    if odds_col_index is not None:
        odds_train = X_train[:, odds_col_index].astype(np.float64)
        odds_valid = X_valid[:, odds_col_index].astype(np.float64)
        # NaN → 1.0 (安全化)
        odds_train = np.where(np.isnan(odds_train), 1.0, odds_train)
        odds_valid = np.where(np.isnan(odds_valid), 1.0, odds_valid)
    else:
        odds_train = None
        odds_valid = None

    # ──────────────────────────────────────────────
    # variant 別の設定
    # ──────────────────────────────────────────────
    objective_fn = None
    metric_fn = None
    sample_weight = None

    if "+odds+ROI_loss_log" in variant_name:
        # 案 C: log(odds) weight BCE
        assert odds_train is not None, "ROI_loss_log variant には odds_col_index が必要"
        objective_fn = make_roi_objective_log(odds_train, odds_cap=100.0)
        metric_fn = make_roi_metric(odds_valid)
        params["metric"] = "None"
        logger.info("  → 案 C: log(odds) weight BCE を使用")

    elif "+odds+ROI_loss" in variant_name:
        # 案 A: ROI weighted BCE (メイン)
        assert odds_train is not None, "ROI_loss variant には odds_col_index が必要"
        objective_fn = make_roi_objective(odds_train, odds_cap=50.0)
        metric_fn = make_roi_metric(odds_valid)
        params["metric"] = "None"
        logger.info("  → 案 A: ROI weighted BCE (odds_cap=50) を使用")

    elif "+odds+sample_weight" in variant_name:
        # 案 B: sample_weight のみ (objective は標準 binary)
        assert odds_train is not None, "sample_weight variant には odds_col_index が必要"
        sample_weight = make_sample_weights(y_train, odds_train, odds_cap=50.0)
        params["objective"] = "binary"
        params["metric"] = "auc"
        logger.info("  → 案 B: sample_weight = odds (標準 binary_logloss)")

    else:
        # 標準 binary_logloss (baseline / +odds)
        params["objective"] = "binary"
        params["metric"] = "auc"
        logger.info("  → 標準 binary_logloss を使用")

    # objective を params に設定
    if objective_fn is not None:
        params["objective"] = objective_fn

    # Dataset 構築
    dtrain = lgb.Dataset(
        X_train, label=y_train,
        feature_name=feature_cols,
        categorical_feature=cat_feats if cat_feats else "auto",
        weight=sample_weight,  # 案 B のみ有効、他は None
        free_raw_data=False,
    )
    dvalid = lgb.Dataset(
        X_valid, label=y_valid,
        feature_name=feature_cols,
        categorical_feature=cat_feats if cat_feats else "auto",
        reference=dtrain,
        free_raw_data=False,
    )

    # callbacks
    # ROI custom metric の場合は early_stopping を feval のスコアで判定
    if metric_fn is not None:
        callbacks = [
            lgb.early_stopping(stopping_rounds=EARLY_STOPPING_ROUNDS, verbose=False),
            lgb.log_evaluation(period=50),
        ]
        feval = metric_fn
    else:
        callbacks = [
            lgb.early_stopping(stopping_rounds=EARLY_STOPPING_ROUNDS, verbose=False),
            lgb.log_evaluation(period=50),
        ]
        feval = None

    t0 = time.time()
    booster = lgb.train(
        params, dtrain,
        num_boost_round=NUM_BOOST_ROUND,
        valid_sets=[dvalid],
        valid_names=["valid"],
        feval=feval,
        callbacks=callbacks,
    )
    elapsed = time.time() - t0
    best_iter = booster.best_iteration
    logger.info(f"  学習完了: {elapsed:.1f}秒, best_iter={best_iter}")

    # best_score ログ (AUC または roi_ev)
    best_scores = booster.best_score.get("valid", {})
    for metric_name, val in best_scores.items():
        logger.info(f"  best {metric_name}: {val:.6f}")

    return booster


# ============================================================
# 評価: AUC + TOP1 hit% + tansho ROI
# ============================================================

def _evaluate_variant(
    booster: lgb.Booster,
    valid_races_info: list,
    feature_cols: List[str],
    variant_name: str,
    use_sigmoid: bool = False,
) -> dict:
    """
    検証データで AUC + TOP1 hit% + tansho ROI を計算する。

    Args:
        booster: 学習済みモデル
        valid_races_info: データ構築時の検証情報
        feature_cols: 使用する特徴量列
        variant_name: ログ表示用
        use_sigmoid: True の場合は raw_score に sigmoid を適用して確率化
                     (custom objective 使用時は predict() が確率を返さないため True に設定)
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

        # custom objective の場合は raw_score のまま → sigmoid で確率化
        if use_sigmoid:
            probs = 1.0 / (1.0 + np.exp(-raw_preds))
        else:
            probs = raw_preds

        all_y_true.extend(r_labels)
        all_y_pred.extend(probs.tolist())

        # ROI 計算: prob 最大の horse を ◎ とする
        if len(probs) == 0:
            continue
        top_idx = int(np.argmax(probs))
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

        # odds 取得不可 = ROI 計算から除外
        if tansho_odds <= 0:
            continue

        played += 1
        if is_win:
            hit += 1
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
        "objective": "roi_weighted" if "ROI_loss" in variant_name else (
            "sample_weight" if "sample_weight" in variant_name else "binary_logloss"
        ),
        "auc": auc,
        "top1_hit_pct": top1_hit_pct,
        "tansho_roi_pct": tansho_roi_pct,
        "played": played,
        "hit": hit,
        "payout_sum": payout_sum,
    }


# ============================================================
# Phase 2a 結果を CSV から読み込む
# ============================================================

def _load_phase2a_results() -> List[dict]:
    """
    Phase 2a の結果 CSV (data/_diag/phase2_pilot_summary.csv) を読み込む。
    baseline_new / +odds の 2 行を返す。
    """
    csv_path = os.path.join(DIAG_DIR, "phase2_pilot_summary.csv")
    if not os.path.exists(csv_path):
        logger.warning(f"  Phase 2a CSV が見つかりません: {csv_path}")
        return []

    results = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # baseline_new / +odds のみ取得
            v = row.get("variant", "")
            if v not in ("baseline_new", "+odds"):
                continue
            try:
                results.append({
                    "variant": v,
                    "features": int(row.get("features", 0)),
                    "objective": "binary_logloss",
                    "auc": float(row.get("AUC", "nan")),
                    "top1_hit_pct": float(row.get("TOP1_hit_pct", "0")),
                    "tansho_roi_pct": float(row.get("tansho_ROI_pct", "0")),
                    "played": int(row.get("played", 0)),
                    "hit": int(row.get("hit", 0)),
                    "payout_sum": float(row.get("payout_sum", "0")),
                    "train_elapsed_sec": 0.0,  # 再学習なし
                    "eval_elapsed_sec": 0.0,
                    "note": "Phase 2a 流用",
                })
            except (ValueError, KeyError) as e:
                logger.warning(f"  CSV 解析エラー: {e}")

    logger.info(f"  Phase 2a 結果 {len(results)} 件をロード: {[r['variant'] for r in results]}")
    return results


# ============================================================
# 結果出力
# ============================================================

def _output_results(results: List[dict], output_csv: str) -> None:
    """結果を stdout + CSV に出力する"""
    os.makedirs(DIAG_DIR, exist_ok=True)

    print()
    print("=" * 90)
    print("【Phase 2b パイロット 結果サマリー】")
    print("=" * 90)
    hdr = f"{'variant':<28} {'feat':>4} {'objective':<18} {'AUC':>8} {'TOP1_hit%':>10} {'tansho_ROI%':>12} {'played':>7} {'hit':>5}"
    print(hdr)
    print("-" * 90)

    for r in results:
        v    = r.get("variant", "?")
        feat = r.get("features", 0)
        obj  = r.get("objective", "?")
        auc  = r.get("auc", float("nan"))
        top1 = r.get("top1_hit_pct", 0.0)
        roi  = r.get("tansho_roi_pct", 0.0)
        pl   = r.get("played", 0)
        h    = r.get("hit", 0)
        auc_str = f"{auc:.4f}" if auc == auc else "N/A"
        note = f"  ← {r['note']}" if r.get("note") else ""
        print(f"{v:<28} {feat:>4} {obj:<18} {auc_str:>8} {top1:>9.2f}% {roi:>11.2f}% {pl:>7} {h:>5}{note}")

    print("-" * 90)
    print()

    # CSV 保存
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "variant", "features", "objective", "AUC",
            "TOP1_hit_pct", "tansho_ROI_pct", "played", "hit", "payout_sum"
        ])
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
    logger.info(f"  CSV 保存: {output_csv}")


# ============================================================
# 改善判定
# ============================================================

def _judge_improvement(results: List[dict]) -> str:
    """+odds vs +odds+ROI_loss の差分を判定し、Phase 2c 推奨を返す"""
    if len(results) < 2:
        return "データ不足で判定不可"

    baseline = next((r for r in results if r.get("variant") == "baseline_new"), None)
    plus_odds = next((r for r in results if r.get("variant") == "+odds"), None)
    roi_variants = [r for r in results if "ROI_loss" in r.get("variant", "") or "sample_weight" in r.get("variant", "")]

    print("=" * 90)
    print("【Phase 2b 改善判定】")
    print("=" * 90)

    if baseline:
        print(f"  baseline_new: ROI={baseline.get('tansho_roi_pct', 0):.2f}%  AUC={baseline.get('auc', float('nan')):.4f}")
    if plus_odds:
        print(f"  +odds (2a):   ROI={plus_odds.get('tansho_roi_pct', 0):.2f}%  AUC={plus_odds.get('auc', float('nan')):.4f}")
    print()

    best_roi = -999.0
    best_variant = None
    for r in roi_variants:
        roi = r.get("tansho_roi_pct", 0.0)
        print(f"  {r['variant']:<30} ROI={roi:.2f}%  AUC={r.get('auc', float('nan')):.4f}")
        if roi > best_roi:
            best_roi = roi
            best_variant = r["variant"]

    print()
    if plus_odds and best_variant:
        delta_roi = best_roi - plus_odds.get("tansho_roi_pct", 0.0)
        target = plus_odds.get("tansho_roi_pct", 0.0) + 5.0

        print(f"  ▼ 基準 (+odds):         {plus_odds.get('tansho_roi_pct', 0):.2f}%")
        print(f"  ▼ 最良 ROI variant:     {best_roi:.2f}%  ({best_variant})")
        print(f"  Δ ROI: {delta_roi:+.2f}pt (合格ライン: +5pt 以上 = {target:.2f}%+)")
        print()

        if delta_roi >= 5.0:
            judgment = f"✅ +5pt 以上改善 ({delta_roi:+.2f}pt) → Phase 2c (全 42 モデル) 進行価値高い"
        elif delta_roi >= 1.0:
            judgment = f"⚠️ +{delta_roi:.1f}pt 改善 → Phase 2c は慎重に検討 (ROI loss 調整余地あり)"
        elif delta_roi >= 0.0:
            judgment = f"⚠️ わずかな改善 ({delta_roi:+.2f}pt) → ROI loss 設計見直し必要"
        else:
            judgment = f"❌ 改善なし ({delta_roi:+.2f}pt) → ROI loss 設計に問題あり / H3 ペース特徴量を先に試す"

        print(f"  判定: {judgment}")
    else:
        judgment = "比較データ不足"
        print(f"  判定: {judgment}")

    print("=" * 90)
    return judgment


# ============================================================
# メイン
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 2b パイロット: ROI 期待値 custom objective 検証")
    parser.add_argument("--debug",    action="store_true", help="デバッグモード (学習期間短縮)")
    parser.add_argument("--roi-only", action="store_true", help="ROI variant のみ追加 (Phase 2a 結果を流用)")
    parser.add_argument(
        "--variant",
        choices=["A", "C", "B", "all"],
        default="all",
        help="ROI variant 選択: A=ROI_weighted_BCE / C=log_odds_weight / B=sample_weight / all=3 variant",
    )
    args = parser.parse_args()

    start_time = time.time()
    os.makedirs(DIAG_DIR, exist_ok=True)

    output_csv = os.path.join(DIAG_DIR, "phase2b_pilot_summary.csv")

    logger.info("=" * 60)
    logger.info("[Phase 2b パイロット] 開始")
    logger.info(f"  学習期間: {TRAIN_START_MONTH} 〜 {TRAIN_END_MONTH}")
    logger.info(f"  検証期間: {VALID_MONTH}")
    logger.info(f"  roi-only={args.roi_only}, variant={args.variant}, debug={args.debug}")
    logger.info("=" * 60)

    # ──────────────────────────────────────────────────────────
    # Phase 2a 結果の読み込み (baseline_new / +odds)
    # ──────────────────────────────────────────────────────────
    results = _load_phase2a_results()

    if not args.roi_only and len(results) < 2:
        logger.warning("  Phase 2a CSV なし → baseline_new / +odds を再学習します")
        args.roi_only = False

    # ──────────────────────────────────────────────────────────
    # データ読み込み (全 variant 共通)
    # ──────────────────────────────────────────────────────────
    logger.info("[データ読み込み] _load_ml_races() + _load_horse_sire_map()...")
    t0 = time.time()
    sire_map = _load_horse_sire_map()
    all_races = _load_ml_races()
    logger.info(f"  全レース数: {len(all_races)}, 所要時間: {time.time()-t0:.1f}秒")

    # デバッグモード設定
    train_start_month = TRAIN_START_MONTH
    train_end_month   = TRAIN_END_MONTH
    if args.debug:
        train_start_month = "2025-10"
        train_end_month   = "2025-11"
        logger.info("  デバッグモード: 学習期間 2025-10〜11 (2 ヶ月)")

    # ──────────────────────────────────────────────────────────
    # +odds データ構築 (ROI variant 全てで共通使用)
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("[データ構築] +odds 学習/検証データ (109 features)")
    logger.info("=" * 60)
    t_data = time.time()
    X_train_odds, y_train_odds, X_valid_odds, y_valid_odds, valid_races_odds = _build_train_valid_data(
        train_start_month=train_start_month,
        train_end_month=train_end_month,
        valid_month=VALID_MONTH,
        feature_cols=FEATURE_COLUMNS_ODDS,
        all_races=all_races,
        sire_map=sire_map,
        include_odds=True,
    )
    logger.info(f"  データ構築完了: {time.time()-t_data:.1f}秒")

    # odds 列インデックス (最後の列)
    odds_col_idx = len(FEATURE_COLUMNS_ODDS) - 1  # = 108 (0-indexed)
    logger.info(f"  odds 列インデックス: {odds_col_idx} ('{FEATURE_COLUMNS_ODDS[odds_col_idx]}')")

    # ──────────────────────────────────────────────────────────
    # ROI variant の学習
    # ──────────────────────────────────────────────────────────

    # 試すバリアント名
    roi_variants_to_run = []
    if args.variant in ("A", "all"):
        roi_variants_to_run.append("+odds+ROI_loss")
    if args.variant in ("C", "all"):
        roi_variants_to_run.append("+odds+ROI_loss_log")
    if args.variant in ("B", "all"):
        roi_variants_to_run.append("+odds+sample_weight")

    for v_name in roi_variants_to_run:
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"[学習] {v_name}")
        logger.info("=" * 60)

        # custom objective かどうかで use_sigmoid を切り替え
        is_custom_obj = "ROI_loss" in v_name

        t_train = time.time()
        try:
            booster_roi = _train_variant(
                X_train_odds, y_train_odds,
                X_valid_odds, y_valid_odds,
                FEATURE_COLUMNS_ODDS,
                v_name,
                odds_col_index=odds_col_idx,
            )
            train_elapsed = time.time() - t_train

            t_eval = time.time()
            r_roi = _evaluate_variant(
                booster_roi,
                valid_races_odds,
                FEATURE_COLUMNS_ODDS,
                v_name,
                use_sigmoid=is_custom_obj,
            )
            r_roi["train_elapsed_sec"] = train_elapsed
            r_roi["eval_elapsed_sec"] = time.time() - t_eval
            results.append(r_roi)
            logger.info(f"  {v_name} 学習完了: {train_elapsed:.1f}秒")
            logger.info(f"  {v_name} 評価完了: {r_roi['eval_elapsed_sec']:.1f}秒")

        except Exception as e:
            logger.error(f"  ❌ {v_name} 学習エラー: {e}", exc_info=True)
            results.append({
                "variant": v_name,
                "features": len(FEATURE_COLUMNS_ODDS),
                "objective": "ERROR",
                "auc": float("nan"),
                "top1_hit_pct": 0.0,
                "tansho_roi_pct": 0.0,
                "played": 0,
                "hit": 0,
                "payout_sum": 0.0,
                "train_elapsed_sec": time.time() - t_train,
                "eval_elapsed_sec": 0.0,
                "note": f"ERROR: {e}",
            })

    # ──────────────────────────────────────────────────────────
    # 結果出力
    # ──────────────────────────────────────────────────────────
    _output_results(results, output_csv)

    # ──────────────────────────────────────────────────────────
    # 改善判定
    # ──────────────────────────────────────────────────────────
    judgment = _judge_improvement(results)

    # ──────────────────────────────────────────────────────────
    # 詳細ログ (学習所要時間)
    # ──────────────────────────────────────────────────────────
    print()
    print("=" * 90)
    print("【詳細情報】")
    print("=" * 90)
    for r in results:
        v      = r.get("variant", "?")
        train_t = r.get("train_elapsed_sec", 0.0)
        eval_t  = r.get("eval_elapsed_sec", 0.0)
        note   = f"  ← {r['note']}" if r.get("note") else ""
        print(f"  {v:<30} 学習={train_t:.1f}秒  評価={eval_t:.1f}秒{note}")

    total_elapsed = time.time() - start_time
    print(f"\n  合計所要時間: {total_elapsed:.1f}秒 ({total_elapsed/60:.1f}分)")
    print("=" * 90)

    logger.info("[Phase 2b パイロット] 完了")


if __name__ == "__main__":
    main()
