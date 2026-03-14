"""
LightGBM 学習・評価・特徴量重要度分析

設計方針:
  - 時系列分割（未来の情報で過去を予測しない）
  - オッズ・人気は特徴量に含めない
  - 学習済みモデルは data/ml/model/ に保存
"""

import json
import os
from datetime import datetime

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    log_loss,
    roc_auc_score,
)

from src.ml.features import (
    FEATURE_COLS,
    LABEL_COL,
    build_dataset,
    load_all_races,
)

MODEL_DIR = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")),
    "data",
    "ml",
    "model",
)


DEFAULT_PARAMS = {
    "objective": "binary",
    "metric": "auc",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "max_depth": -1,
    "min_child_samples": 50,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "lambda_l1": 0.1,
    "lambda_l2": 1.0,
    "verbose": -1,
    "n_jobs": -1,
}


def train_and_evaluate(
    start_date: str = "2024-01-01",
    end_date: str = None,
    val_months: int = 3,
    num_boost_round: int = 1000,
    early_stopping_rounds: int = 50,
) -> dict:
    """
    時系列分割で学習 → 評価し、モデルと特徴量重要度を返す。

    train: start_date 〜 (end_date - val_months)
    val:   (end_date - val_months) 〜 end_date

    Returns: {
        "model": lgb.Booster,
        "importance": DataFrame,
        "metrics": dict,
        "feature_cols": list,
    }
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    print("\n[ML] データ読み込み中...")
    all_races = load_all_races(start_date, end_date)
    if not all_races:
        print("[エラー] レースデータがありません")
        return {}

    print("[ML] 特徴量構築中...")
    df = build_dataset(all_races, start_date, end_date, min_past_runs=1)
    if df.empty:
        print("[エラー] 有効なサンプルがありません")
        return {}

    # 時系列分割
    val_start = _subtract_months(end_date, val_months)
    train_df = df[df["date"] < val_start].copy()
    val_df = df[df["date"] >= val_start].copy()

    print(f"\n[ML] データ分割:")
    print(f"  学習: {train_df['date'].min()} 〜 {train_df['date'].max()}  ({len(train_df):,}件)")
    print(f"  検証: {val_df['date'].min()} 〜 {val_df['date'].max()}  ({len(val_df):,}件)")

    if len(train_df) < 100 or len(val_df) < 100:
        print("[エラー] データ不足（学習/検証ともに100件以上必要）")
        return {}

    X_train = train_df[FEATURE_COLS]
    y_train = train_df[LABEL_COL]
    X_val = val_df[FEATURE_COLS]
    y_val = val_df[LABEL_COL]

    print(f"\n  特徴量数: {len(FEATURE_COLS)}")
    print(f"  正例率: 学習={y_train.mean():.3f}  検証={y_val.mean():.3f}")

    # 学習
    print("\n[ML] LightGBM 学習中...")
    train_data = lgb.Dataset(X_train, label=y_train)
    val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)

    model = lgb.train(
        DEFAULT_PARAMS,
        train_data,
        num_boost_round=num_boost_round,
        valid_sets=[train_data, val_data],
        valid_names=["train", "val"],
        callbacks=[
            lgb.early_stopping(early_stopping_rounds),
            lgb.log_evaluation(100),
        ],
    )

    # 評価
    y_pred = model.predict(X_val)
    metrics = _calc_metrics(y_val, y_pred, val_df)

    # 特徴量重要度
    importance = _calc_importance(model, FEATURE_COLS)

    # モデル保存
    _save_model(model, importance, metrics, FEATURE_COLS)

    # 結果表示
    _print_results(metrics, importance)

    return {
        "model": model,
        "importance": importance,
        "metrics": metrics,
        "feature_cols": FEATURE_COLS,
        "val_df": val_df,
        "y_pred": y_pred,
    }


def feature_importance_analysis(
    start_date: str = "2024-01-01",
    end_date: str = None,
) -> pd.DataFrame:
    """
    特徴量重要度のみを分析して表示する（軽量版）。
    """
    result = train_and_evaluate(start_date, end_date)
    if not result:
        return pd.DataFrame()
    return result["importance"]


def _calc_metrics(y_true, y_pred, val_df) -> dict:
    auc = roc_auc_score(y_true, y_pred)
    logloss = log_loss(y_true, y_pred)
    y_binary = (y_pred >= 0.5).astype(int)
    acc = accuracy_score(y_true, y_binary)

    # レース単位の評価: 各レースで最高予測値の馬が3着以内に入ったか
    race_hit = []
    for race_id, group in val_df.groupby("race_id"):
        idx = group.index
        preds = y_pred[idx.get_indexer(idx)]
        pred_series = pd.Series(preds, index=idx)
        top_idx = pred_series.idxmax()
        race_hit.append(int(group.loc[top_idx, LABEL_COL] == 1))
    race_top1_rate = np.mean(race_hit) if race_hit else 0

    # 上位3頭の的中率
    race_top3_hit = []
    for race_id, group in val_df.groupby("race_id"):
        idx = group.index
        preds = y_pred[idx.get_indexer(idx)]
        pred_series = pd.Series(preds, index=idx)
        top3_idx = pred_series.nlargest(3).index
        hits = sum(1 for i in top3_idx if group.loc[i, LABEL_COL] == 1)
        race_top3_hit.append(hits / 3)
    race_top3_rate = np.mean(race_top3_hit) if race_top3_hit else 0

    return {
        "auc": round(auc, 4),
        "logloss": round(logloss, 4),
        "accuracy": round(acc, 4),
        "race_top1_hit_rate": round(race_top1_rate, 4),
        "race_top3_hit_rate": round(race_top3_rate, 4),
        "val_samples": len(y_true),
        "val_races": val_df["race_id"].nunique(),
    }


def _calc_importance(model, feature_cols) -> pd.DataFrame:
    gain = model.feature_importance(importance_type="gain")
    split = model.feature_importance(importance_type="split")

    imp = pd.DataFrame({
        "feature": feature_cols,
        "gain": gain,
        "split": split,
        "gain_pct": (gain / gain.sum() * 100).round(2),
    })
    return imp.sort_values("gain", ascending=False).reset_index(drop=True)


def _save_model(model, importance, metrics, feature_cols):
    os.makedirs(MODEL_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    model.save_model(os.path.join(MODEL_DIR, f"model_{ts}.lgb"))
    importance.to_csv(os.path.join(MODEL_DIR, f"importance_{ts}.csv"), index=False)

    meta = {
        "created_at": ts,
        "metrics": metrics,
        "feature_cols": feature_cols,
        "params": DEFAULT_PARAMS,
    }
    with open(os.path.join(MODEL_DIR, f"meta_{ts}.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # latest シンボリックコピー
    model.save_model(os.path.join(MODEL_DIR, "model_latest.lgb"))
    importance.to_csv(os.path.join(MODEL_DIR, "importance_latest.csv"), index=False)
    with open(os.path.join(MODEL_DIR, "meta_latest.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"\n  モデル保存: {MODEL_DIR}")


def _print_results(metrics, importance):
    print(f"\n{'='*56}")
    print(f"  LightGBM 評価結果")
    print(f"{'='*56}")
    print(f"  AUC:                {metrics['auc']}")
    print(f"  LogLoss:            {metrics['logloss']}")
    print(f"  Accuracy:           {metrics['accuracy']}")
    print(f"  レース単位 TOP1的中: {metrics['race_top1_hit_rate']:.1%}")
    print(f"  レース単位 TOP3的中: {metrics['race_top3_hit_rate']:.1%}")
    print(f"  検証レース数:       {metrics['val_races']}")
    print(f"  検証サンプル数:     {metrics['val_samples']:,}")

    print(f"\n{'='*56}")
    print(f"  特徴量重要度 (gain)")
    print(f"{'='*56}")
    for _, row in importance.head(20).iterrows():
        bar = "#" * int(row["gain_pct"])
        print(f"  {row['feature']:<28s} {row['gain_pct']:6.2f}%  {bar}")

    remaining = importance.iloc[20:]["gain_pct"].sum()
    if remaining > 0:
        print(f"  {'(その他)':<28s} {remaining:6.2f}%")


def train_by_venue(
    start_date: str = "2024-01-01",
    end_date: str = None,
    val_months: int = 3,
    min_samples: int = 500,
) -> dict:
    """
    競馬場ごとに個別モデルを学習し、特徴量重要度を比較する。

    Returns: {venue_name: {"metrics": ..., "importance": ..., "model": ...}, ...}
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    print("\n[ML] データ読み込み中...")
    all_races = load_all_races(start_date, end_date)
    if not all_races:
        print("[エラー] レースデータがありません")
        return {}

    print("[ML] 特徴量構築中...")
    df = build_dataset(all_races, start_date, end_date, min_past_runs=1)
    if df.empty:
        return {}

    val_start = _subtract_months(end_date, val_months)

    # 競馬場ごとにグループ化
    venues = df.groupby("venue_name").size().sort_values(ascending=False)
    print(f"\n[ML] 競馬場別データ量:")
    for vn, cnt in venues.items():
        marker = " *" if cnt >= min_samples else "  (skip)"
        print(f"  {vn or '不明':<8s} {cnt:>7,}件{marker}")

    results = {}
    all_importances = []

    target_venues = [vn for vn, cnt in venues.items() if cnt >= min_samples and vn]
    print(f"\n[ML] {len(target_venues)}場を学習対象とします (閾値: {min_samples}件以上)\n")

    for vn in target_venues:
        vdf = df[df["venue_name"] == vn].copy()
        train_df = vdf[vdf["date"] < val_start]
        val_df = vdf[vdf["date"] >= val_start]

        if len(train_df) < 100 or len(val_df) < 30:
            print(f"  {vn}: データ不足 (学習={len(train_df)}, 検証={len(val_df)})  skip")
            continue

        X_train = train_df[FEATURE_COLS]
        y_train = train_df[LABEL_COL]
        X_val = val_df[FEATURE_COLS]
        y_val = val_df[LABEL_COL]

        train_data = lgb.Dataset(X_train, label=y_train)
        val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)

        model = lgb.train(
            DEFAULT_PARAMS,
            train_data,
            num_boost_round=500,
            valid_sets=[val_data],
            valid_names=["val"],
            callbacks=[
                lgb.early_stopping(30),
                lgb.log_evaluation(0),
            ],
        )

        y_pred = model.predict(X_val)
        try:
            auc = roc_auc_score(y_val, y_pred)
        except ValueError:
            auc = 0.5

        # TOP1 的中率
        race_hit = []
        for rid, group in val_df.groupby("race_id"):
            idx = group.index
            preds = y_pred[idx.get_indexer(idx)]
            pred_series = pd.Series(preds, index=idx)
            top_idx = pred_series.idxmax()
            race_hit.append(int(group.loc[top_idx, LABEL_COL] == 1))
        top1_rate = np.mean(race_hit) if race_hit else 0

        importance = _calc_importance(model, FEATURE_COLS)
        importance["venue"] = vn

        is_jra = vdf["is_jra"].iloc[0] if len(vdf) > 0 else 1
        n_val_races = val_df["race_id"].nunique()

        print(
            f"  {vn:<8s}  AUC={auc:.3f}  TOP1={top1_rate:.1%}"
            f"  (学習={len(train_df):,} 検証={len(val_df):,}/{n_val_races}R)"
        )

        results[vn] = {
            "model": model,
            "importance": importance,
            "auc": auc,
            "top1_rate": top1_rate,
            "train_size": len(train_df),
            "val_size": len(val_df),
            "val_races": n_val_races,
            "is_jra": is_jra,
        }
        all_importances.append(importance)

    if not results:
        print("\n[エラー] 学習できた競馬場がありません")
        return {}

    # 競馬場別の特徴量重要度比較
    _print_venue_comparison(results, all_importances)

    # 保存
    _save_venue_results(results, all_importances)

    return results


def _print_venue_comparison(results: dict, all_importances: list):
    """競馬場別の特徴量重要度を比較表示"""
    print(f"\n{'='*70}")
    print(f"  競馬場別 モデル性能")
    print(f"{'='*70}")
    print(f"  {'場名':<8s} {'AUC':>6s}  {'TOP1':>6s}  {'区分':<4s}  {'学習':>8s}  {'検証':>6s}")
    print(f"  {'-'*50}")

    # JRA → NAR の順、AUC降順
    jra = [(vn, r) for vn, r in results.items() if r["is_jra"]]
    nar = [(vn, r) for vn, r in results.items() if not r["is_jra"]]
    jra.sort(key=lambda x: x[1]["auc"], reverse=True)
    nar.sort(key=lambda x: x[1]["auc"], reverse=True)

    for group_name, group in [("JRA", jra), ("NAR", nar)]:
        if not group:
            continue
        for vn, r in group:
            print(
                f"  {vn:<8s} {r['auc']:6.3f}  {r['top1_rate']:5.1%}"
                f"  {group_name:<4s}  {r['train_size']:>7,}  {r['val_size']:>5,}"
            )

    # 重要度トップ5の比較
    print(f"\n{'='*70}")
    print(f"  競馬場別 重要特徴量 TOP5")
    print(f"{'='*70}")

    top_features_by_venue = {}
    for vn, r in results.items():
        imp = r["importance"]
        top5 = imp.head(5)["feature"].tolist()
        top_features_by_venue[vn] = top5

    # JRA
    if jra:
        print(f"\n  --- JRA ---")
        for vn, _ in jra:
            feats = top_features_by_venue[vn]
            feats_str = " > ".join(feats)
            print(f"  {vn:<6s}: {feats_str}")

    # NAR
    if nar:
        print(f"\n  --- NAR ---")
        for vn, _ in nar:
            feats = top_features_by_venue[vn]
            feats_str = " > ".join(feats)
            print(f"  {vn:<6s}: {feats_str}")

    # 全場で共通して効く特徴量
    if len(results) >= 3:
        from collections import Counter
        all_top5 = []
        for feats in top_features_by_venue.values():
            all_top5.extend(feats)
        common = Counter(all_top5).most_common(5)
        print(f"\n  --- 全場で共通して重要な特徴量 ---")
        for feat, count in common:
            pct = count / len(results) * 100
            print(f"  {feat:<28s}  {count}/{len(results)}場 ({pct:.0f}%)")


def _save_venue_results(results: dict, all_importances: list):
    """競馬場別結果をCSVで保存"""
    os.makedirs(MODEL_DIR, exist_ok=True)

    # 重要度を結合して保存
    if all_importances:
        combined = pd.concat(all_importances, ignore_index=True)
        combined.to_csv(
            os.path.join(MODEL_DIR, "importance_by_venue.csv"), index=False
        )

    # サマリー保存
    summary = []
    for vn, r in results.items():
        summary.append({
            "venue": vn,
            "is_jra": r["is_jra"],
            "auc": r["auc"],
            "top1_rate": r["top1_rate"],
            "train_size": r["train_size"],
            "val_size": r["val_size"],
        })
    pd.DataFrame(summary).to_csv(
        os.path.join(MODEL_DIR, "venue_summary.csv"), index=False
    )
    print(f"\n  結果保存: {MODEL_DIR}/importance_by_venue.csv")


def train_probability_models(
    valid_days: int = 30,
) -> dict:
    """
    三連率予測モデル (win/top2/top3) を学習する。
    Platt Scaling でキャリブレーション付き。
    """
    from src.ml.probability_model import train_probability_models as _train

    print("\n[ML] 三連率予測モデル学習開始...")
    return _train(valid_days=valid_days)


def train_all_models(
    start_date: str = "2024-01-01",
    end_date: str = None,
    val_months: int = 3,
) -> dict:
    """
    全モデルを一括学習する。
    1. 複勝予測モデル (is_top3)
    2. 三連率予測モデル (win/top2/top3 with Platt Scaling)
    """
    print("\n" + "=" * 60)
    print("  全モデル一括学習")
    print("=" * 60)

    results = {}

    print("\n[1/2] 複勝予測モデル (features.py ベース)")
    r1 = train_and_evaluate(start_date, end_date, val_months)
    results["place_model"] = r1.get("metrics", {})

    print("\n[2/2] 三連率予測モデル (lgbm_model.py ベース)")
    r2 = train_probability_models()
    results["probability_models"] = r2

    print("\n" + "=" * 60)
    print("  全モデル学習完了")
    print("=" * 60)
    return results


def _subtract_months(date_str: str, months: int) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.year
    month = dt.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(dt.day, 28)
    return f"{year}-{month:02d}-{day:02d}"
