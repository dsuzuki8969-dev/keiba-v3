"""M-3 Phase 1: SHAP 特徴量診断

LightGBM wf_2026 全 43 モデルの SHAP 値・permute importance・confidence 別差異を計算する。

Usage:
  # Stage 1: 動作確認 (lgbm_place.txt のみ)
  python scripts/diag_shap_analysis.py --model place --n-samples 500 --skip-permute --skip-confidence

  # Stage 2: 全 43 モデル run (Opus 承認後)
  python scripts/diag_shap_analysis.py --all --n-samples 500

  # permute importance あり
  python scripts/diag_shap_analysis.py --model place --n-samples 500
"""

import argparse
import csv
import os
import sys
import time
from glob import glob
from typing import List, Optional, Tuple

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

import lightgbm as lgb
import shap

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

# デフォルトの学習月 (tracker 更新用)
DEFAULT_MONTH = "2025-12"


# ============================================================
# ユーティリティ関数
# ============================================================


# 除外モデル: ばんえい (venue_65) は FEATURE_COLUMNS_BANEI (43列) で学習されており、
# 108列の X とは shape mismatch でクラッシュする。CLAUDE.md (memory/feedback_banei_excluded.md)
# でばんえいは予想対象外と確定済のため SHAP 診断からも除外する。
SKIP_MODEL_NAMES = {"place_venue_65"}


def _list_all_models() -> List[Tuple[str, str]]:
    """wf_2026 の lgbm_*.txt 一覧を (model_name, model_path) で返す (ばんえい除外)"""
    files = sorted(glob(os.path.join(MODEL_DIR, "lgbm_*.txt")))
    result = []
    for fpath in files:
        basename = os.path.basename(fpath)
        name = basename.replace("lgbm_", "").replace(".txt", "")
        if name in SKIP_MODEL_NAMES:
            continue  # venue_65 (ばんえい) はスキップ
        result.append((name, fpath))
    return result


def _get_model_path(model_name: str) -> str:
    """モデル名からパスを返す。'place' → lgbm_place.txt"""
    return os.path.join(MODEL_DIR, f"lgbm_{model_name}.txt")


def _build_X_from_month(month: str, n_samples: int) -> np.ndarray:
    """
    指定月 (YYYY-MM) のレースから特徴量行列 X を構築する。

    手法: A方式 (walk_forward_backtest.py と同じ関数を再利用)
    - _load_ml_races() で全レースを読み込み
    - 指定月の前月末まで tracker を更新
    - 指定月のレースで _extract_features() を呼び出し
    - n_samples 件まで収集して返す

    Args:
        month: 対象月 例: '2025-12'
        n_samples: 最大行数

    Returns:
        X: np.ndarray shape (n, 108), dtype float32
    """
    logger.info(f"[データ準備] 月={month}, 最大サンプル={n_samples}")

    # 月末日を計算 (最大データ読み込み)
    year, mon = int(month.split("-")[0]), int(month.split("-")[1])
    if mon == 12:
        next_year, next_mon = year + 1, 1
    else:
        next_year, next_mon = year, mon + 1
    train_end = f"{year:04d}-{mon:02d}-01"  # 当月初日 (= 前月末まで学習)
    valid_end = f"{next_year:04d}-{next_mon:02d}-01"

    # レース読み込み
    t0 = time.time()
    sire_map = _load_horse_sire_map()
    all_races = _load_ml_races(max_date=valid_end.replace("-", "")[:8])
    train_races = [r for r in all_races if r.get("date", "") < train_end]
    # month は "2025-12" 形式、race.date は "2025-12-01" 形式なので startswith(month) で比較
    target_races = [
        r
        for r in all_races
        if r.get("date", "").startswith(month)
    ]
    logger.info(
        f"  学習期間レース: {len(train_races)}, 対象月レース: {len(target_races)}"
    )

    # tracker 更新 (学習期間)
    logger.info(f"  tracker更新中 (学習期間 {len(train_races)} レース)...")
    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()
    for i, race in enumerate(train_races):
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)
        if (i + 1) % 10000 == 0:
            pct = (i + 1) / len(train_races) * 100
            logger.info(f"    tracker更新: {i+1}/{len(train_races)} ({pct:.0f}%)")
    logger.info(f"  tracker更新完了: {time.time()-t0:.1f}秒")

    # 対象月の特徴量生成
    logger.info(f"  特徴量生成中...")
    all_feats = []
    t1 = time.time()
    for race in target_races:
        if len(all_feats) >= n_samples:
            break
        r_feats = []
        for h in race.get("horses", []):
            if h.get("finish_pos") is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            hd = dict(h, sire_id=sid, bms_id=bid)
            feat = _extract_features(hd, race, tracker, sire_tracker)
            r_feats.append(feat)
        if r_feats:
            _add_race_relative_features(r_feats)
            all_feats.extend(r_feats)

    # numpy 変換
    mat = []
    for f in all_feats[:n_samples]:
        row = [
            float(f.get(c)) if f.get(c) is not None else float("nan")
            for c in FEATURE_COLUMNS
        ]
        mat.append(row)
    X = np.array(mat, dtype=np.float32)
    logger.info(
        f"  特徴量生成完了: shape={X.shape}, NaN率={np.isnan(X).mean():.3f}, {time.time()-t1:.1f}秒"
    )
    return X


# ============================================================
# SHAP 分析
# ============================================================


def run_shap_analysis(
    model_name: str,
    model_path: str,
    X: np.ndarray,
    output_prefix: str,
    top_n: int = 30,
) -> dict:
    """
    1 モデルの SHAP mean(|SHAP|) 重要度を計算して stdout + CSV に出力する。

    Returns:
        {'feat_importance': {feature_name: mean_abs_shap, ...}, 'elapsed_shap': float}
    """
    logger.info(f"[SHAP] モデル: {model_name}")
    booster = lgb.Booster(model_file=model_path)
    feat_names = booster.feature_name()
    n_feat = len(feat_names)
    logger.info(f"  feature_names: {n_feat}")
    logger.info(f"  X_sample shape: {X.shape}")

    # SHAP TreeExplainer
    t0 = time.time()
    explainer = shap.TreeExplainer(booster)
    shap_values = explainer.shap_values(X)

    # LightGBM 二値分類では shap_values が (n, p) のはず
    # shap 0.50 の UserWarning 対応: list の場合は [0] を取る
    if isinstance(shap_values, list):
        sv = shap_values[0]
    else:
        sv = shap_values

    elapsed_shap = time.time() - t0
    logger.info(f"  SHAP TreeExplainer ... {elapsed_shap:.2f}s")

    # mean(|SHAP|) 算出
    mean_shap = np.abs(sv).mean(axis=0)  # shape (n_feat,)
    sorted_idx = np.argsort(-mean_shap)

    # Top 5 をログ表示
    top5_parts = [f"{feat_names[i]}={mean_shap[i]:.3f}" for i in sorted_idx[:5]]
    logger.info(f"  mean(|SHAP|) Top 5: {', '.join(top5_parts)}")

    # stdout に Top 30 を表示
    print()
    print("=" * 70)
    print(f"【SHAP Top {top_n}】 モデル: {model_name}")
    print("=" * 70)
    print(f"{'順位':>3} {'特徴量':<40} {'mean|SHAP|':>12} {'相対%':>8}")
    print("-" * 70)
    total_shap = mean_shap.sum()
    for rank, idx in enumerate(sorted_idx[:top_n], 1):
        pct = mean_shap[idx] / total_shap * 100 if total_shap > 0 else 0
        print(f"{rank:>3}. {feat_names[idx]:<40} {mean_shap[idx]:>12.5f} {pct:>7.2f}%")

    # CSV 保存
    os.makedirs(DIAG_DIR, exist_ok=True)
    csv_path = os.path.join(
        DIAG_DIR, f"shap_top{top_n}_{model_name.upper()}_DEMO.csv"
    )
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "feature_name", "mean_abs_shap", "relative_pct"])
        for rank, idx in enumerate(sorted_idx[:top_n], 1):
            pct = mean_shap[idx] / total_shap * 100 if total_shap > 0 else 0
            writer.writerow(
                [rank, feat_names[idx], f"{mean_shap[idx]:.6f}", f"{pct:.4f}"]
            )
    logger.info(f"  CSV保存: {csv_path}")

    # 全特徴量を dict で返す
    feat_importance = {feat_names[i]: float(mean_shap[i]) for i in range(n_feat)}
    return {"feat_importance": feat_importance, "elapsed_shap": elapsed_shap}


# ============================================================
# Permute importance
# ============================================================


def run_permute_importance(
    model_name: str,
    model_path: str,
    X: np.ndarray,
    top_n: int = 50,
) -> dict:
    """
    上位 top_n 特徴量に対して permute importance を計算する。
    各特徴量を 1 列 shuffle して予測確率の変化幅 (mean abs diff) を測定。

    Returns:
        {'permute_importance': {feature_name: delta, ...}, 'elapsed': float}
    """
    logger.info(f"[Permute Importance] モデル: {model_name}, 上位{top_n}特徴量")
    booster = lgb.Booster(model_file=model_path)
    feat_names = booster.feature_name()

    # まず全特徴量の SHAP で上位 top_n を特定
    explainer = shap.TreeExplainer(booster)
    sv = explainer.shap_values(X)
    if isinstance(sv, list):
        sv = sv[0]
    mean_shap = np.abs(sv).mean(axis=0)
    top_idx = np.argsort(-mean_shap)[:top_n]

    # ベースライン予測
    baseline_pred = booster.predict(X)

    t0 = time.time()
    permute_result = {}
    rng = np.random.default_rng(seed=42)

    for rank, col_idx in enumerate(top_idx):
        fname = feat_names[col_idx]
        X_perm = X.copy()
        X_perm[:, col_idx] = rng.permutation(X_perm[:, col_idx])
        perm_pred = booster.predict(X_perm)
        delta = float(np.abs(baseline_pred - perm_pred).mean())
        permute_result[fname] = delta

    elapsed = time.time() - t0
    logger.info(f"  permute importance 完了: {elapsed:.2f}秒")

    # 上位 20 を表示
    print()
    print("=" * 70)
    print(f"【Permute Importance Top 20】 モデル: {model_name}")
    print("=" * 70)
    sorted_perm = sorted(permute_result.items(), key=lambda x: -x[1])
    print(f"{'順位':>3} {'特徴量':<40} {'delta(mean abs)':>15}")
    print("-" * 70)
    for rank, (fname, delta) in enumerate(sorted_perm[:20], 1):
        print(f"{rank:>3}. {fname:<40} {delta:>15.5f}")

    return {"permute_importance": permute_result, "elapsed": elapsed}


# ============================================================
# confidence 別 SHAP
# ============================================================


def run_confidence_shap(
    model_name: str,
    model_path: str,
    X: np.ndarray,
    top_n: int = 20,
) -> dict:
    """
    X を confidence 別 (高/中/低) に分割して SHAP の差異を測定する。

    confidence 分割方法 (python-reviewer P0-1 fix):
    - 高: 予測確率の 67 percentile 超
    - 中: 33 percentile 超 67 percentile 以下
    - 低: 33 percentile 以下
    ※ 固定閾値 0.50/0.35 では JRA モデル (place_jra_*) で「高」が空になるため動的閾値に変更

    Returns:
        {'confidence_shap': {'high': {...}, 'mid': {...}, 'low': {...}},
         'thresholds': {'p33': float, 'p67': float}}
    """
    logger.info(f"[Confidence 別 SHAP] モデル: {model_name}")
    booster = lgb.Booster(model_file=model_path)
    feat_names = booster.feature_name()

    # 予測確率の分位数でグループ分け (動的閾値: percentile 33/67)
    raw_pred = booster.predict(X)  # shape (n,)
    p33, p67 = float(np.percentile(raw_pred, 33)), float(np.percentile(raw_pred, 67))
    high_idx = np.where(raw_pred > p67)[0]
    mid_idx = np.where((raw_pred > p33) & (raw_pred <= p67))[0]
    low_idx = np.where(raw_pred <= p33)[0]

    logger.info(
        f"  グループ分割 (p33={p33:.3f}, p67={p67:.3f}): "
        f"高={len(high_idx)}, 中={len(mid_idx)}, 低={len(low_idx)}"
    )

    explainer = shap.TreeExplainer(booster)
    result = {}

    groups = {"high": high_idx, "mid": mid_idx, "low": low_idx}
    for group_name, idx in groups.items():
        if len(idx) < 10:
            logger.info(f"  {group_name}: サンプル不足 ({len(idx)}<10) → スキップ")
            continue
        X_sub = X[idx]
        sv = explainer.shap_values(X_sub)
        if isinstance(sv, list):
            sv = sv[0]
        mean_shap = np.abs(sv).mean(axis=0)
        sorted_idx = np.argsort(-mean_shap)
        result[group_name] = {feat_names[i]: float(mean_shap[i]) for i in sorted_idx[:top_n]}

    # 差異を表示 (高/低 が両方とも 10 件以上ある場合のみ)
    if "high" in result and "low" in result:
        print()
        print("=" * 70)
        print(f"【Confidence 別 SHAP 差異 Top 10】 モデル: {model_name} "
              f"(p33={p33:.3f}, p67={p67:.3f})")
        print("=" * 70)
        print(f"{'特徴量':<40} {'高':>10} {'低':>10} {'差(高-低)':>12}")
        print("-" * 70)
        high_d = result["high"]
        low_d = result["low"]
        all_feats_set = set(high_d.keys()) | set(low_d.keys())
        diffs = {
            f: high_d.get(f, 0) - low_d.get(f, 0) for f in all_feats_set
        }
        for fname, diff in sorted(diffs.items(), key=lambda x: -abs(x[1]))[:10]:
            h_val = high_d.get(fname, 0)
            l_val = low_d.get(fname, 0)
            print(f"{fname:<40} {h_val:>10.4f} {l_val:>10.4f} {diff:>12.4f}")

    return {"confidence_shap": result, "thresholds": {"p33": p33, "p67": p67}}


# ============================================================
# 所要時間見積もり
# ============================================================


def estimate_total_time(
    elapsed_tracker: float,
    elapsed_shap_per_model: float,
    n_samples: int,
    n_models: int = 43,
) -> None:
    """全 43 モデル run の所要時間を見積もって表示する"""
    print()
    print("=" * 70)
    print("【Stage 2 全 43 モデル run 所要時間見積もり】")
    print("=" * 70)

    # tracker 更新は 1 回で共有可能
    tracker_time = elapsed_tracker

    # SHAP 計算
    shap_per_model = elapsed_shap_per_model
    shap_all = shap_per_model * n_models

    # permute importance: SHAP + predict × 50 回
    # 実測から推定 (predict は SHAP の約 1/10 の速さ)
    permute_per_model = shap_per_model * 1.5  # 上位 50 特徴量 × shuffle
    permute_all = permute_per_model * n_models

    # confidence 別: 3 グループ × SHAP 計算
    conf_per_model = shap_per_model * 3
    conf_all = conf_per_model * n_models

    total_no_opt = tracker_time + shap_all + permute_all + conf_all
    total_shap_only = tracker_time + shap_all

    print(f"  ● tracker 更新 (1 回のみ共有):  {tracker_time:.0f}秒 ({tracker_time/60:.1f}分)")
    print(f"  ● SHAP 計算 (43 モデル):        {shap_all:.1f}秒 ({shap_all/60:.2f}分) [{shap_per_model:.3f}s/モデル]")
    print(f"  ● permute importance (43 モデル): {permute_all:.1f}秒 ({permute_all/60:.2f}分)")
    print(f"  ● confidence 別 (43 モデル):     {conf_all:.1f}秒 ({conf_all/60:.2f}分)")
    print(f"  ─────────────────────────────────────────────────")
    print(f"  【SHAP のみ合計】:               {total_shap_only:.0f}秒 ({total_shap_only/60:.1f}分)")
    print(f"  【全 3 処理合計】:               {total_no_opt:.0f}秒 ({total_no_opt/60:.1f}分)")
    print()
    print(f"  ※ n_samples={n_samples} での実測値ベース")
    print(f"  ※ tracker 更新が支配的 (全体の {tracker_time/total_no_opt*100:.0f}%)")
    print(f"  ※ Stage 2 推奨: --n-samples 500 で全 43 モデル約 {total_no_opt/60:.0f} 分")


# ============================================================
# メイン
# ============================================================


def main():
    parser = argparse.ArgumentParser(
        description="M-3 Phase 1 SHAP 特徴量診断スクリプト"
    )

    # モデル選択
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--model",
        type=str,
        help="1 モデルだけ実行 (例: place, win_global, place_jra_turf)",
    )
    group.add_argument("--all", action="store_true", help="全 43 モデルを iterate (Stage 2 用)")

    # オプション
    parser.add_argument(
        "--n-samples",
        type=int,
        default=500,
        help="SHAP 計算用サンプル数 (デフォルト: 500)",
    )
    parser.add_argument(
        "--month",
        type=str,
        default=DEFAULT_MONTH,
        help="training data 取得期間 YYYY-MM (デフォルト: 2025-12)",
    )
    parser.add_argument(
        "--skip-permute",
        action="store_true",
        help="permute importance をスキップ (Stage 1 動作確認用)",
    )
    parser.add_argument(
        "--skip-confidence",
        action="store_true",
        help="confidence 別 SHAP をスキップ (Stage 1 動作確認用)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=30,
        help="Top N 特徴量を表示・保存 (デフォルト: 30)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("M-3 Phase 1: SHAP 特徴量診断")
    print("=" * 70)
    logger.info(f"設定: model={args.model or 'ALL'}, n_samples={args.n_samples}, month={args.month}")
    logger.info(f"  skip_permute={args.skip_permute}, skip_confidence={args.skip_confidence}")

    # ============================================================
    # Step 1: 特徴量行列 X を構築
    # ============================================================
    t_tracker_start = time.time()
    X = _build_X_from_month(args.month, args.n_samples)
    elapsed_tracker = time.time() - t_tracker_start
    logger.info(f"X 構築完了: {X.shape}, 所要時間={elapsed_tracker:.1f}秒")

    # ============================================================
    # Step 2: モデル一覧を取得
    # ============================================================
    if args.all:
        models = _list_all_models()
        logger.info(f"全 {len(models)} モデルを処理します")
    else:
        model_path = _get_model_path(args.model)
        if not os.path.exists(model_path):
            logger.error(f"モデルファイルが見つかりません: {model_path}")
            sys.exit(1)
        models = [(args.model, model_path)]

    # ============================================================
    # Step 3: 各モデルで SHAP 分析
    # ============================================================
    all_shap_results = {}
    elapsed_shap_list = []

    for i, (model_name, model_path) in enumerate(models):
        logger.info(f"[{i+1}/{len(models)}] モデル処理中: {model_name}")

        # SHAP 分析
        shap_result = run_shap_analysis(
            model_name=model_name,
            model_path=model_path,
            X=X,
            output_prefix=f"shap_top{args.top_n}",
            top_n=args.top_n,
        )
        all_shap_results[model_name] = shap_result["feat_importance"]
        elapsed_shap_list.append(shap_result["elapsed_shap"])

        # permute importance (オプション)
        if not args.skip_permute:
            run_permute_importance(
                model_name=model_name,
                model_path=model_path,
                X=X,
                top_n=50,
            )

        # confidence 別 SHAP (オプション)
        if not args.skip_confidence:
            run_confidence_shap(
                model_name=model_name,
                model_path=model_path,
                X=X,
                top_n=20,
            )

    # ============================================================
    # Step 4: 所要時間見積もり
    # ============================================================
    avg_shap_time = float(np.mean(elapsed_shap_list)) if elapsed_shap_list else 0.0
    estimate_total_time(
        elapsed_tracker=elapsed_tracker,
        elapsed_shap_per_model=avg_shap_time,
        n_samples=args.n_samples,
    )

    # ============================================================
    # Step 5: 複数モデルの場合は全モデル平均も出力
    # ============================================================
    if len(all_shap_results) > 1:
        from collections import defaultdict
        avg_importance = defaultdict(float)
        for model_result in all_shap_results.values():
            for feat, val in model_result.items():
                avg_importance[feat] += val / len(all_shap_results)

        print()
        print("=" * 70)
        print(f"【全モデル平均 SHAP Top 30】")
        print("=" * 70)
        sorted_avg = sorted(avg_importance.items(), key=lambda x: -x[1])
        print(f"{'順位':>3} {'特徴量':<40} {'平均 mean|SHAP|':>15}")
        print("-" * 70)
        for rank, (feat, val) in enumerate(sorted_avg[:30], 1):
            print(f"{rank:>3}. {feat:<40} {val:>15.5f}")

        # CSV 保存
        os.makedirs(DIAG_DIR, exist_ok=True)
        csv_avg_path = os.path.join(DIAG_DIR, "shap_all_models_avg_top30.csv")
        with open(csv_avg_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["rank", "feature_name", "avg_mean_abs_shap"])
            for rank, (feat, val) in enumerate(sorted_avg[:30], 1):
                writer.writerow([rank, feat, f"{val:.6f}"])
        logger.info(f"全モデル平均 CSV 保存: {csv_avg_path}")

    print()
    print("=" * 70)
    print("完了")
    print("=" * 70)


if __name__ == "__main__":
    main()
