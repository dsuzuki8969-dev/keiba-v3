#!/usr/bin/env python
"""
Walk-Forward Cross-Validation（時系列CV）

拡張ウィンドウ方式のウォークフォワードCVで LightGBM の汎化性能を推定する。
データリークを排除した正しい時系列評価を実施。

フォールド構造 (例: fold_months=3, min_train_months=6):
  Fold 1: 学習=[T0 ~ T0+6ヶ月)  / 検証=[T0+6ヶ月 ~ T0+9ヶ月)
  Fold 2: 学習=[T0 ~ T0+9ヶ月)  / 検証=[T0+9ヶ月 ~ T0+12ヶ月)
  Fold 3: 学習=[T0 ~ T0+12ヶ月) / 検証=[T0+12ヶ月 ~ T0+15ヶ月)
  ...（データ末尾まで）

評価指標:
  - AUC          : 3着内確率の識別力 (0.5=ランダム, 1.0=完璧)
  - NDCG@3       : レース内ランク品質 (1着=3, 2着=2, 3着=1で計算)
  - Top1→3着内率 : 予測1位馬が実際に3着以内に入る割合
  - Top3→3着内率 : 予測Top3に実際の3着以内馬が含まれる割合
  - 単勝ROI      : 予測1位馬に単勝投資した場合の回収率 (オッズあれば)

Usage:
  python scripts/walk_forward_cv.py
  python scripts/walk_forward_cv.py --fold-months 6
  python scripts/walk_forward_cv.py --min-train-months 9
  python scripts/walk_forward_cv.py --fold-months 3 --min-train-months 12
"""

import argparse
import math
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8")


# ============================================================
# ユーティリティ
# ============================================================


def _months_add(ym: str, months: int) -> str:
    """'YYYY-MM' に月数を加算して 'YYYY-MM-01' を返す"""
    y, m = int(ym[:4]), int(ym[5:7])
    y += (m - 1 + months) // 12
    m  = (m - 1 + months) % 12 + 1
    return f"{y:04d}-{m:02d}-01"


def _make_folds(all_dates, fold_months: int, min_train_months: int):
    """
    Returns list of (fold_start, fold_end) — どちらも 'YYYY-MM-DD' 形式。
    学習窓: [all_dates[0], fold_start)
    検証窓: [fold_start, fold_end)
    """
    if not all_dates:
        return []
    first_ym = all_dates[0][:7]   # 'YYYY-MM'
    last_ym  = all_dates[-1][:7]

    fold_start = _months_add(first_ym, min_train_months)
    folds = []
    while fold_start[:7] <= last_ym:
        fold_end = _months_add(fold_start[:7], fold_months)
        folds.append((fold_start, fold_end))
        fold_start = fold_end

    return folds


def _ndcg_at_k(fps, scores, k=3):
    """
    NDCG@k を計算する。
    関連度ラベル: 1着=3, 2着=2, 3着=1, 着外=0
    """
    if not fps or len(fps) < k:
        return None
    rel = [max(0, 4 - fp) if fp and fp <= 3 else 0 for fp in fps]
    ideal = sorted(rel, reverse=True)[:k]
    idcg = sum(r / math.log2(i + 2) for i, r in enumerate(ideal) if r > 0)
    if idcg == 0:
        return None
    order = sorted(range(len(scores)), key=lambda i: -(scores[i] or 0))
    dcg = sum(rel[order[i]] / math.log2(i + 2) for i in range(min(k, len(order))))
    return dcg / idcg


# ============================================================
# フォールドごとの処理
# ============================================================


def _run_fold(
    fold_idx: int,
    fold_start: str,
    fold_end: str,
    races: list,
    sire_map: dict,
    feature_columns: list,
    categorical_features: list,
) -> dict:
    """
    1フォールドの学習・評価を実行する。

    各レースを日付順に処理:
      - 学習レース (date < fold_start): 特徴量抽出 → train_feats に追加 → tracker 更新
      - 検証レース (fold_start <= date < fold_end): 特徴量抽出 → valid に追加 → tracker 更新
      - fold_end 以降: 処理停止

    Returns:
        指標 dict (スキップ時は None)
    """
    import numpy as np
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score

    from src.ml.lgbm_model import (
        RollingStatsTracker, RollingSireTracker,
        _extract_features, _add_race_relative_features,
        _smile_key_ml, SURFACE_MAP,
    )

    t_start = time.time()
    print(f"\n{'─' * 58}")
    print(f"  フォールド {fold_idx}:  検証 {fold_start} ~ {fold_end[:7]}")
    print(f"{'─' * 58}")

    tracker      = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    # 学習データ (フラット: 馬単位)
    train_feats:  list = []
    train_labels: list = []

    # 検証データ (レース単位のグループ保持)
    valid_groups: list = []   # [(feat_list, label_list, fps_list, odds_list)]

    processed = 0
    for race in races:
        date_str = race.get("date", "")
        if not date_str:
            continue
        if date_str >= fold_end:
            break   # 検証窓を超えたら終了

        is_valid = date_str >= fold_start

        r_feats, r_labels, r_fps, r_odds = [], [], [], []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            feat = _extract_features(
                dict(h, sire_id=sid, bms_id=bid),
                race, tracker, sire_tracker,
            )
            r_feats.append(feat)
            r_labels.append(1 if fp <= 3 else 0)
            r_fps.append(fp)
            raw_odds = h.get("win_odds") or h.get("tan_odds")
            try:
                r_odds.append(float(raw_odds) if raw_odds else None)
            except (TypeError, ValueError):
                r_odds.append(None)

        if r_feats:
            # ① 相対特徴量を一括設定 (2パス)
            _add_race_relative_features(r_feats)
            if is_valid:
                valid_groups.append((r_feats, r_labels, r_fps, r_odds))
            else:
                train_feats.extend(r_feats)
                train_labels.extend(r_labels)

        # tracker は学習・検証両方更新（時系列一貫性を保つ）
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)
        processed += 1

    n_train = len(train_labels)
    n_valid = sum(len(g[0]) for g in valid_groups)
    n_valid_races = len(valid_groups)

    print(f"  学習: {n_train:,}頭  /  検証: {n_valid:,}頭  ({n_valid_races}レース)")

    if n_train < 2000:
        print("  ⚠  学習データ不足のためスキップ")
        return None
    if n_valid < 100 or n_valid_races < 10:
        print("  ⚠  検証データ不足のためスキップ")
        return None

    # ---- numpy 変換 ----
    def _to_np(rows):
        mat = []
        for f in rows:
            mat.append([
                float(f[c]) if f[c] is not None else float("nan")
                for c in feature_columns
            ])
        return np.array(mat, dtype=np.float32)

    X_train = _to_np(train_feats)
    y_train = np.array(train_labels, dtype=np.int32)

    valid_feats_flat  = [f for g in valid_groups for f in g[0]]
    valid_labels_flat = [l for g in valid_groups for l in g[1]]
    X_valid = _to_np(valid_feats_flat)
    y_valid = np.array(valid_labels_flat, dtype=np.int32)

    # ---- LightGBM 学習 ----
    dtrain = lgb.Dataset(
        X_train, label=y_train,
        feature_name=feature_columns,
        categorical_feature=categorical_features,
        free_raw_data=False,
    )
    params = {
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
        "seed": 42 + fold_idx,
        "is_unbalance": True,
    }
    print(f"  LightGBM 学習中...", flush=True)
    t_lgb = time.time()
    model = lgb.train(
        params, dtrain,
        num_boost_round=500,
        callbacks=[lgb.log_evaluation(period=200)],
    )
    print(f"  学習完了 ({time.time() - t_lgb:.1f}秒)", flush=True)

    # ---- 評価 ----
    y_pred = model.predict(X_valid)

    # AUC
    auc = None
    if len(set(y_valid)) == 2:
        auc = roc_auc_score(y_valid, y_pred)

    # レース単位指標
    ndcg_vals = []
    top1_hit = top3_hit = total_races = 0
    roi_bets = 0
    roi_return = 0.0

    idx = 0
    for (r_feats, r_labels, r_fps, r_odds) in valid_groups:
        n_h = len(r_feats)
        if n_h < 3:
            idx += n_h
            continue

        preds = y_pred[idx:idx + n_h].tolist()

        # NDCG@3
        ndcg = _ndcg_at_k(r_fps, preds, k=3)
        if ndcg is not None:
            ndcg_vals.append(ndcg)

        # Top-1 推し (3着以内)
        top1_i = max(range(n_h), key=lambda i: preds[i])
        if r_labels[top1_i] == 1:
            top1_hit += 1

        # Top-3 推し (3着以内馬を含む)
        top3_idxs = sorted(range(n_h), key=lambda i: -preds[i])[:3]
        if any(r_labels[i] == 1 for i in top3_idxs):
            top3_hit += 1

        # 単勝ROI シミュレーション
        odds_val = r_odds[top1_i] if r_odds else None
        if odds_val and odds_val > 0:
            roi_bets += 100
            if r_fps[top1_i] == 1:
                roi_return += odds_val * 100

        total_races += 1
        idx += n_h

    ndcg_mean = sum(ndcg_vals) / len(ndcg_vals) if ndcg_vals else None
    roi = (roi_return - roi_bets) / roi_bets if roi_bets > 0 else None
    top1_rate = top1_hit / total_races if total_races else None
    top3_rate = top3_hit / total_races if total_races else None

    elapsed = time.time() - t_start

    # 結果表示
    print(f"  AUC:        {auc:.4f}" if auc is not None else "  AUC:        N/A")
    print(f"  NDCG@3:     {ndcg_mean:.4f}" if ndcg_mean is not None else "  NDCG@3:     N/A")
    print(f"  Top1→3着内: {top1_rate * 100:.1f}%  ({top1_hit}/{total_races})" if top1_rate else "  Top1→3着内: N/A")
    print(f"  Top3→3着内: {top3_rate * 100:.1f}%  ({top3_hit}/{total_races})" if top3_rate else "  Top3→3着内: N/A")
    if roi is not None:
        print(f"  単勝ROI:    {roi * 100:+.1f}%  ({roi_bets // 100:.0f}R / 回収 {roi_return:.0f}円)")
    else:
        print(f"  単勝ROI:    N/A (オッズデータなし)")
    print(f"  経過時間:   {elapsed:.1f}秒")

    return {
        "fold":          fold_idx,
        "fold_start":    fold_start,
        "fold_end":      fold_end,
        "n_train":       n_train,
        "n_valid":       n_valid,
        "n_valid_races": total_races,
        "auc":           round(auc, 4) if auc is not None else None,
        "ndcg3":         round(ndcg_mean, 4) if ndcg_mean is not None else None,
        "top1_rate":     round(top1_rate, 4) if top1_rate is not None else None,
        "top3_rate":     round(top3_rate, 4) if top3_rate is not None else None,
        "roi":           round(roi, 4) if roi is not None else None,
        "elapsed":       round(elapsed, 1),
    }


# ============================================================
# メイン
# ============================================================


def main():
    parser = argparse.ArgumentParser(description="Walk-Forward CV")
    parser.add_argument(
        "--fold-months", type=int, default=3,
        help="検証ウィンドウ幅 (月数, default=3)",
    )
    parser.add_argument(
        "--min-train-months", type=int, default=6,
        help="最低学習期間 (月数, default=6)",
    )
    args = parser.parse_args()

    t_total = time.time()

    from src.ml.lgbm_model import (
        FEATURE_COLUMNS, CATEGORICAL_FEATURES,
        _load_ml_races, _load_horse_sire_map,
    )

    print(f"\n{'=' * 58}")
    print(f"  Walk-Forward Cross-Validation")
    print(f"  フォールド幅:   {args.fold_months} ヶ月")
    print(f"  最低学習期間:   {args.min_train_months} ヶ月")
    print(f"{'=' * 58}\n")

    # ─── データ読み込み ───
    print("[1/3] ML レースデータ読み込み中...", flush=True)
    t0 = time.time()
    races = _load_ml_races()
    if not races:
        print("[ERROR] ML データが見つかりません (data/ml/*.json)")
        sys.exit(1)
    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    print(f"  レース: {len(races):,} 件  /  期間: {all_dates[0]} ~ {all_dates[-1]}  ({time.time() - t0:.1f}秒)")

    print("[2/3] 馬-父馬マップ読み込み中...", flush=True)
    t0 = time.time()
    sire_map = _load_horse_sire_map()
    print(f"  {len(sire_map):,} 頭  ({time.time() - t0:.1f}秒)")

    # ─── フォールド定義 ───
    folds = _make_folds(all_dates, args.fold_months, args.min_train_months)
    if not folds:
        print("[ERROR] フォールドを生成できません (データ不足)")
        sys.exit(1)

    print(f"\n[3/3] {len(folds)} フォールド CV 開始...\n", flush=True)

    # ─── フォールドごとに実行 ───
    results = []
    for i, (fold_start, fold_end) in enumerate(folds, 1):
        r = _run_fold(
            fold_idx=i,
            fold_start=fold_start,
            fold_end=fold_end,
            races=races,
            sire_map=sire_map,
            feature_columns=FEATURE_COLUMNS,
            categorical_features=CATEGORICAL_FEATURES,
        )
        if r:
            results.append(r)

    # ─── サマリー ───
    print(f"\n{'=' * 70}")
    print(f"  Walk-Forward CV サマリー  (フォールド幅 {args.fold_months}ヶ月)")
    print(f"{'=' * 70}")

    if not results:
        print("  有効フォールドなし")
        return

    # テーブルヘッダ
    print(
        f"\n  {'#':>2}  {'検証期間':>20}  {'学習':>8}  {'検証':>7}  "
        f"{'AUC':>7}  {'NDCG@3':>7}  {'Top1%':>6}  {'Top3%':>6}  {'ROI%':>7}"
    )
    print(f"  {'─' * 75}")

    for r in results:
        period   = f"{r['fold_start'][:7]}~{r['fold_end'][:7]}"
        auc_s    = f"{r['auc']:.4f}"   if r['auc']   is not None else "   N/A"
        ndcg_s   = f"{r['ndcg3']:.4f}" if r['ndcg3'] is not None else "   N/A"
        top1_s   = f"{r['top1_rate'] * 100:.1f}" if r['top1_rate'] is not None else "  N/A"
        top3_s   = f"{r['top3_rate'] * 100:.1f}" if r['top3_rate'] is not None else "  N/A"
        roi_s    = f"{r['roi'] * 100:+.1f}"       if r['roi']      is not None else "   N/A"
        print(
            f"  {r['fold']:>2}  {period:>20}  {r['n_train']:>8,}  {r['n_valid']:>7,}  "
            f"{auc_s:>7}  {ndcg_s:>7}  {top1_s:>6}  {top3_s:>6}  {roi_s:>7}"
        )

    # 平均・標準偏差
    def _stats(vals):
        vals = [v for v in vals if v is not None]
        if not vals:
            return None, None
        mu  = sum(vals) / len(vals)
        std = (sum((v - mu) ** 2 for v in vals) / len(vals)) ** 0.5
        return mu, std

    aucs  = [r["auc"]      for r in results]
    ndcgs = [r["ndcg3"]    for r in results]
    top1s = [r["top1_rate"] for r in results]
    top3s = [r["top3_rate"] for r in results]
    rois  = [r["roi"]       for r in results]

    print(f"\n  {'平均指標':─<70}")
    mu, std = _stats(aucs)
    if mu is not None:
        print(f"  AUC:     {mu:.4f} ± {std:.4f}  (n={sum(1 for v in aucs if v)})")
    mu, std = _stats(ndcgs)
    if mu is not None:
        print(f"  NDCG@3:  {mu:.4f} ± {std:.4f}")
    mu, _ = _stats(top1s)
    if mu is not None:
        print(f"  Top1率:  {mu * 100:.1f}%")
    mu, _ = _stats(top3s)
    if mu is not None:
        print(f"  Top3率:  {mu * 100:.1f}%")
    mu, _ = _stats(rois)
    if mu is not None:
        print(f"  単勝ROI: {mu * 100:+.1f}%")

    total_time = time.time() - t_total
    print(f"\n  合計所要時間: {total_time:.0f}秒 ({total_time / 60:.1f}分)")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()
