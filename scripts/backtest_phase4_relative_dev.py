"""
Plan-γ Phase 4: バックテスト — 旧モデル vs 新モデルの AUC / Brier Score 比較

使い方:
  python scripts/backtest_phase4_relative_dev.py [--months 3] [--bak-dir data/models/.bak_pre_relative_dev]

  --months: 遡る月数 (デフォルト 3)
  --bak-dir: バックアップモデルのディレクトリ

判定基準:
  - AUC が 0.005 以上劣化 → Phase 4 撤回を推奨
  - それ以外 → 採用 OK

設計:
  - 学習済み tracker / sire_tracker (rolling_stats.pkl) を使い推論のみ実行
  - 旧モデル: bak-dir/*.txt + rolling_stats.pkl (再学習前)
  - 新モデル: data/models/*.txt + rolling_stats.pkl (再学習後)
  - 過去 N ヶ月の検証データで AUC / Brier Score / Top1 hit rate を比較
"""

import argparse
import os
import sys
import json
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import numpy as np
try:
    from sklearn.metrics import roc_auc_score, brier_score_loss
    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False
    print("[WARNING] scikit-learn が見つかりません。AUC/Brier Score を計算できません。")

from src.ml.lgbm_model import (
    FEATURE_COLUMNS,
    FEATURE_COLUMNS_BANEI,
    SURFACE_MAP,
    GRADE_MAP,
    MODEL_DIR,
    STATS_PATH,
    SIRE_STATS_PATH,
    _load_ml_races,
    _extract_features,
    _add_race_relative_features,
    _load_horse_sire_map,
    _load_relative_dev_map,
    RollingStatsTracker,
)
from data.masters.venue_master import is_banei

_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _subtract_months(date_str: str, months: int) -> str:
    """YYYY-MM-DD から months ヶ月前の日付を返す"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    m = dt.month - months
    y = dt.year
    while m <= 0:
        m += 12
        y -= 1
    return f"{y}-{m:02d}-01"


def _load_model(model_path: str):
    """LightGBM モデルをロード"""
    try:
        import lightgbm as lgb
        if os.path.exists(model_path):
            return lgb.Booster(model_file=model_path)
    except Exception as e:
        print(f"  [WARNING] モデルロード失敗: {model_path} — {e}")
    return None


def _predict_race(race_feats: list, model, is_banei_race: bool) -> List[float]:
    """1レース分の特徴量で予測確率を返す"""
    import pandas as pd
    feat_cols = FEATURE_COLUMNS_BANEI if is_banei_race else FEATURE_COLUMNS
    df = pd.DataFrame(race_feats)[feat_cols]
    # 欠損を 0 fill（LightGBM は NaN ネイティブ対応だが念のため）
    try:
        probs = model.predict(df)
        return list(probs)
    except Exception as e:
        print(f"    [WARNING] 予測失敗: {e}")
        return [0.5] * len(race_feats)


def collect_validation_rows(val_start: str, val_end: str) -> List[dict]:
    """
    検証期間の全レースデータを収集し、特徴量+ラベルのリストを返す。
    tracker は rolling_stats.pkl (再学習後の全データ学習済み) を使用。
    """
    print(f"\n[バックテスト] 検証データ収集: {val_start} 〜 {val_end}")

    # tracker をロード（再学習後）
    if not os.path.exists(STATS_PATH):
        print(f"  [ERROR] {STATS_PATH} が見つかりません。先に retrain_all.py を実行してください。")
        return []

    with open(STATS_PATH, "rb") as f:
        tracker = pickle.load(f)
    with open(SIRE_STATS_PATH, "rb") as f:
        sire_tracker = pickle.load(f)

    sire_map = _load_horse_sire_map()
    relative_dev_map = _load_relative_dev_map()
    races = _load_ml_races(start_date=val_start, end_date=val_end)
    print(f"  レース数: {len(races)}")

    results = []
    for race in races:
        date_str = race.get("date", "")
        if date_str < val_start or date_str > val_end:
            continue
        race_id = race.get("race_id", "")
        is_banei_race = is_banei(str(race.get("venue_code", "")))

        # relative_dev 注入
        if relative_dev_map and race_id:
            for h in race.get("horses", []):
                hid_upd = h.get("horse_id", "")
                if hid_upd:
                    _rd = relative_dev_map.get((race_id, hid_upd))
                    if _rd is not None:
                        h["relative_dev"] = _rd

        race_feats, race_labels = [], []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            _h_inj = dict(h, sire_id=sid, bms_id=bid)
            _rd = relative_dev_map.get((race_id, hid))
            if _rd is not None:
                _h_inj["relative_dev"] = _rd
            feat = _extract_features(_h_inj, race, tracker, sire_tracker)
            race_feats.append(feat)
            race_labels.append(1 if fp <= 3 else 0)

        if race_feats:
            _add_race_relative_features(race_feats)
            results.append({
                "race_id": race_id,
                "date": date_str,
                "is_banei": is_banei_race,
                "feats": race_feats,
                "labels": race_labels,
            })

    print(f"  有効レース数: {len(results)}")
    return results


def evaluate_model(races_data: list, model_dir: str, label: str) -> dict:
    """
    指定モデルディレクトリのモデルで全レースを評価し、メトリクスを返す。
    """
    print(f"\n[{label}] 評価開始: {model_dir}")
    global_model_path = os.path.join(model_dir, "lgbm_place.txt")
    model = _load_model(global_model_path)
    if model is None:
        print(f"  [ERROR] global モデルが見つかりません: {global_model_path}")
        return {}

    all_probs = []
    all_labels = []
    top1_hits = 0
    top1_total = 0

    for race_data in races_data:
        feats = race_data["feats"]
        labels = race_data["labels"]
        is_banei_race = race_data["is_banei"]

        probs = _predict_race(feats, model, is_banei_race)
        all_probs.extend(probs)
        all_labels.extend(labels)

        # Top1 hit rate: 最高確率の馬が1着か
        if labels and probs:
            best_idx = probs.index(max(probs))
            fp_list = [f.get("finish_pos_raw", None) for f in feats]
            # finish_pos は labels から逆算: label=1 かつ それが1着とは限らない
            # シンプルに: probs最高馬のlabelが1（3着以内）かどうか
            if labels[best_idx] == 1:
                top1_hits += 1
            top1_total += 1

    metrics = {}
    if _HAS_SKLEARN and all_labels and len(set(all_labels)) > 1:
        metrics["auc"] = roc_auc_score(all_labels, all_probs)
        metrics["brier"] = brier_score_loss(all_labels, all_probs)
    else:
        metrics["auc"] = None
        metrics["brier"] = None

    metrics["top1_hit_rate"] = top1_hits / top1_total if top1_total > 0 else None
    metrics["total_races"] = len(races_data)
    metrics["total_samples"] = len(all_labels)

    print(f"  サンプル数: {metrics['total_samples']}")
    if metrics["auc"] is not None:
        print(f"  AUC:    {metrics['auc']:.4f}")
        print(f"  Brier:  {metrics['brier']:.4f}")
    if metrics["top1_hit_rate"] is not None:
        print(f"  Top1 hit rate: {metrics['top1_hit_rate']*100:.1f}%")

    return metrics


def main():
    parser = argparse.ArgumentParser(description="Phase 4 バックテスト: 旧 vs 新モデル比較")
    parser.add_argument("--months", type=int, default=3, help="遡る月数 (デフォルト 3)")
    parser.add_argument("--bak-dir", default=os.path.join(_BASE, "data", "models", ".bak_pre_relative_dev"),
                        help="旧モデルのディレクトリ")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    val_start = _subtract_months(today, args.months)
    val_end = today

    print("=" * 60)
    print(f"  Plan-γ Phase 4 バックテスト")
    print(f"  検証期間: {val_start} 〜 {val_end}")
    print(f"  旧モデル: {args.bak_dir}")
    print(f"  新モデル: {MODEL_DIR}")
    print("=" * 60)

    # 検証データ収集
    races_data = collect_validation_rows(val_start, val_end)
    if not races_data:
        print("[ERROR] 検証データが空です。")
        return

    # 新モデル評価
    new_metrics = evaluate_model(races_data, MODEL_DIR, "新モデル (Phase 4)")

    # 旧モデル評価
    if os.path.exists(args.bak_dir):
        old_metrics = evaluate_model(races_data, args.bak_dir, "旧モデル (Phase 3以前)")
    else:
        print(f"\n[WARNING] 旧モデルディレクトリが見つかりません: {args.bak_dir}")
        old_metrics = {}

    # 比較判定
    print("\n" + "=" * 60)
    print("  比較結果")
    print("=" * 60)

    auc_new = new_metrics.get("auc")
    auc_old = old_metrics.get("auc")
    brier_new = new_metrics.get("brier")
    brier_old = old_metrics.get("brier")

    if auc_new is not None and auc_old is not None:
        auc_diff = auc_new - auc_old
        brier_diff = (brier_new or 0) - (brier_old or 0)
        print(f"  AUC:    旧={auc_old:.4f}  新={auc_new:.4f}  差={auc_diff:+.4f}")
        print(f"  Brier:  旧={brier_old:.4f}  新={brier_new:.4f}  差={brier_diff:+.4f}")

        THRESHOLD = -0.005  # AUC が 0.005 以上悪化したら撤回
        if auc_diff < THRESHOLD:
            print(f"\n  [判定] 撤回推奨 — AUC が {abs(auc_diff):.4f} 悪化 (閾値 0.005)")
            print("  撤回手順:")
            print(f"    cp {args.bak_dir}/* {MODEL_DIR}/")
            print("    features.py / lgbm_model.py から relative_dev_* カラムを削除")
        else:
            print(f"\n  [判定] 採用 OK — AUC 劣化は許容範囲内 ({auc_diff:+.4f})")
            if auc_diff >= 0:
                print("  -> 新モデルは旧モデルより良い or 同等の精度を達成")
            else:
                print(f"  -> 微小劣化 ({auc_diff:.4f}) だが閾値 (0.005) 未満のため採用")
    else:
        print("  [WARNING] AUC を計算できませんでした (scikit-learn 不足 or サンプル不足)")

    print("=" * 60)


if __name__ == "__main__":
    main()
