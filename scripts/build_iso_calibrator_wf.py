"""WF 期間別 Isotonic Calibrator 学習スクリプト

各WF期間（wf_2024/2025/2026）について、train_max 直前 N=3000 race を使って
Isotonic regression で P(top3) → calibrated_prob mapping を学習し、
data/models/{wf_name}/iso_cal_top3.pkl として保存する。

これにより「全期間 train → 全期間 test」による時間リークを防ぐ。
各期間の直近レースデータのみで局所 calibration を実現する。

使用方法:
    python scripts/build_iso_calibrator_wf.py
    python scripts/build_iso_calibrator_wf.py --period wf_2024
    python scripts/build_iso_calibrator_wf.py --cal-n-races 5000
    python scripts/build_iso_calibrator_wf.py --dry-run
"""

import argparse
import json
import math
import os
import pickle
import sys
import time
from glob import glob
from typing import Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

ML_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "ml")

# WF 期間定義 (wf_inference.py と同一)
WF_PERIODS = {
    "wf_2024": {
        "model_dir": os.path.join(PROJECT_ROOT, "data", "models", "wf_2024"),
        "train_max": "2023-12-31",
        "cal_n_races": 3000,  # calibration に使う直前レース数
    },
    "wf_2025": {
        "model_dir": os.path.join(PROJECT_ROOT, "data", "models", "wf_2025"),
        "train_max": "2024-12-31",
        "cal_n_races": 3000,
    },
    "wf_2026": {
        "model_dir": os.path.join(PROJECT_ROOT, "data", "models", "wf_2026"),
        "train_max": "2025-12-31",
        "cal_n_races": 3000,
    },
}


def load_calibration_races(train_max: str, cal_n_races: int) -> list:
    """train_max 直前 N races を ML JSON からロードする。

    日付昇順でソートし、最後の cal_n_races 件を返す。
    注意: finish_pos（実結果）が含まれているレースのみ使用可能。

    Args:
        train_max: この日付以前のレースのみ (例: "2023-12-31")
        cal_n_races: 最大で何レース取得するか

    Returns:
        レース dict のリスト (cal_n_races 件以内)
    """
    max_fname = train_max.replace("-", "") + ".json"

    # train_max 以前のファイルを日付昇順でソート
    files = sorted(
        f for f in os.listdir(ML_DATA_DIR)
        if f.endswith(".json") and not f.startswith("_")
        and f[0].isdigit()
        and f <= max_fname
    )

    if not files:
        print(f"  WARNING: train_max={train_max} 以前の ML JSON が見つかりません")
        return []

    # 後ろから読み込み (直近レース優先)
    # まずファイル総数を確認
    all_races = []
    for fname in reversed(files):
        fpath = os.path.join(ML_DATA_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            races = data.get("races", [])
            all_races.extend(reversed(races))  # 後ろから追加
        except Exception as e:
            print(f"  WARNING: {fname} 読み込みエラー: {e}")
            continue

        # cal_n_races 件以上集まったら早期終了
        if len(all_races) >= cal_n_races:
            break

    # 正順に戻す (古い順)
    all_races = list(reversed(all_races[:cal_n_races]))

    print(f"  calibration 候補レース数: {len(all_races)} / {cal_n_races}")
    return all_races


def build_calibration_samples(races: list, model_dir: str, tracker, sire_tracker, sire_map: dict) -> tuple:
    """レースリストから calibration 学習サンプルを構築する。

    各レースを WF モデルで推論し、raw_prob と actual_label (finish_pos <= 3) を収集する。

    Returns:
        (raw_probs: list[float], labels: list[int]) のタプル
    """
    import lightgbm as lgb
    import numpy as np
    from src.ml.lgbm_model import (
        FEATURE_COLUMNS, FEATURE_COLUMNS_BANEI,
        _extract_features, _add_race_relative_features,
        _smile_key_ml, SURFACE_MAP,
    )
    from data.masters.venue_master import is_banei as _is_banei_check

    # モデルロード
    models = {}
    cal_params = {}

    for fname in os.listdir(model_dir):
        if not fname.startswith("lgbm_place") or not fname.endswith(".txt"):
            continue
        fpath = os.path.join(model_dir, fname)
        stem = fname[len("lgbm_place"):-len(".txt")].lstrip("_")
        key = stem if stem else "global"
        try:
            models[key] = lgb.Booster(model_file=fpath)
        except Exception as e:
            print(f"    WARNING: モデル {key} ロード失敗: {e}")

    for fname in os.listdir(model_dir):
        if not fname.startswith("lgbm_place") or not fname.endswith("_cal.json"):
            continue
        stem = fname[len("lgbm_place"):-len("_cal.json")].lstrip("_")
        key = stem if stem else "global"
        try:
            with open(os.path.join(model_dir, fname)) as f:
                cal_params[key] = json.load(f)
        except Exception:
            pass

    print(f"    モデル数: {len(models)}, Platt scaling 数: {len(cal_params)}")

    def select_model(surface_val, is_jra, venue_code, smile_cat):
        """階層モデル選択"""
        if smile_cat:
            sf = "turf" if surface_val == 0 else "dirt"
            k4 = f"jra_{sf}_{smile_cat}" if is_jra else None
            if k4 and k4 in models:
                return models[k4], k4
        k3 = f"venue_{venue_code}"
        if k3 in models:
            return models[k3], k3
        if is_jra:
            sf = "turf" if surface_val == 0 else "dirt"
            k2 = f"jra_{sf}"
            if k2 in models:
                return models[k2], k2
        sf_key = "turf" if surface_val == 0 else "dirt"
        if sf_key in models:
            return models[sf_key], sf_key
        return models.get("global"), "global"

    raw_probs = []
    labels_top3 = []
    skipped_no_result = 0
    skipped_no_model = 0

    for race in races:
        horses = race.get("horses", [])
        if len(horses) < 3:
            continue

        # finish_pos が含まれているか確認 (実結果が必要)
        has_result = any(h.get("finish_pos") is not None for h in horses)
        if not has_result:
            skipped_no_result += 1
            continue

        race_dict = {
            "race_id": race.get("race_id", ""),
            "date": race.get("date", ""),
            "venue": race.get("venue", ""),
            "venue_code": str(race.get("venue_code", "") or "").zfill(2),
            "surface": race.get("surface", ""),
            "distance": race.get("distance", 0),
            "condition": race.get("condition", ""),
            "field_count": race.get("field_count", len(horses)),
            "is_jra": race.get("is_jra", True),
            "grade": race.get("grade", ""),
        }

        surface_val = SURFACE_MAP.get(race_dict["surface"], -1)
        is_jra = bool(race_dict["is_jra"])
        venue_code = race_dict["venue_code"]
        distance = int(race_dict["distance"] or 0)
        smile_cat = _smile_key_ml(distance) if distance else ""

        model, model_key = select_model(surface_val, is_jra, venue_code, smile_cat)
        if model is None:
            skipped_no_model += 1
            continue

        # 特徴量構築
        horse_dicts = []
        finish_pos_map = {}
        for h in horses:
            sid, bid = sire_map.get(h.get("horse_id", ""), ("", ""))
            hdict = {
                "horse_id": h.get("horse_id", ""),
                "horse_name": h.get("horse_name", ""),
                "jockey_id": h.get("jockey_id", h.get("jockey", "")),
                "trainer_id": h.get("trainer_id", h.get("trainer", "")),
                "gate_no": h.get("gate_no", 0),
                "horse_no": h.get("horse_no", 0),
                "sex": h.get("sex", ""),
                "age": h.get("age", 0),
                "weight_kg": h.get("weight_kg", 0.0),
                "horse_weight": h.get("horse_weight", 0),
                "weight_change": h.get("weight_change", 0),
                "sire_id": sid,
                "bms_id": bid,
            }
            horse_dicts.append(hdict)
            fp = h.get("finish_pos")
            if fp is not None:
                finish_pos_map[h.get("horse_id", "")] = int(fp)

        if len(horse_dicts) < 3:
            continue

        features = []
        hids = []
        for h in horse_dicts:
            try:
                feat = _extract_features(h, race_dict, tracker, sire_tracker)
                features.append(feat)
                hids.append(h["horse_id"])
            except Exception:
                continue

        if not features:
            continue

        _add_race_relative_features(features)

        _is_banei_vc = _is_banei_check(venue_code)
        feat_cols = FEATURE_COLUMNS_BANEI if _is_banei_vc else FEATURE_COLUMNS
        if hasattr(model, "num_feature"):
            n = model.num_feature()
            if n < len(feat_cols):
                feat_cols = feat_cols[:n]

        X = np.array(
            [[float(f.get(c)) if f.get(c) is not None else float("nan") for c in feat_cols]
             for f in features],
            dtype=np.float32,
        )
        raw_p = model.predict(X)

        # Platt scaling 適用 (Isotonic はその後に適用するため、Platt 後の確率を学習対象とする)
        cal = cal_params.get(model_key)
        if cal:
            a, b = cal["a"], cal["b"]
            raw_p = [1.0 / (1.0 + math.exp(-(a * p + b))) for p in raw_p]

        # ラベル付け
        for hid, p in zip(hids, raw_p):
            fp = finish_pos_map.get(hid)
            if fp is None:
                continue
            raw_probs.append(float(p))
            labels_top3.append(1 if fp <= 3 else 0)

    print(f"    サンプル収集: {len(raw_probs)} horse-samples, "
          f"実結果なし={skipped_no_result}, モデルなし={skipped_no_model}")

    if not raw_probs:
        return [], []

    pos_rate = sum(labels_top3) / len(labels_top3)
    print(f"    top3 正例率: {pos_rate:.3f} (期待値 ~0.25-0.33)")

    return raw_probs, labels_top3


def train_isotonic_calibrator(raw_probs: list, labels: list) -> object:
    """Isotonic regression で calibrator を学習する。

    Args:
        raw_probs: モデルの raw 確率リスト
        labels: 正解ラベルリスト (0/1)

    Returns:
        学習済 IsotonicRegression オブジェクト
    """
    from sklearn.isotonic import IsotonicRegression
    import numpy as np

    X = np.array(raw_probs, dtype=np.float64)
    y = np.array(labels, dtype=np.float64)

    cal = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    cal.fit(X, y)

    return cal


def log_calibrator_mapping(cal, label: str = "top3") -> None:
    """calibrator の raw → cal 変換マッピングを出力する (検証用)"""
    import numpy as np

    test_raw = np.array([0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.5, 0.6, 0.7])
    test_cal = cal.transform(test_raw)

    print(f"\n    === {label} calibrator mapping ===")
    print(f"    {'raw_prob':>10} → {'cal_prob':>10}")
    print(f"    {'-'*25}")
    for r, c in zip(test_raw, test_cal):
        change = c - r
        sign = "+" if change >= 0 else ""
        print(f"    {r:>10.3f} → {c:>10.3f}  ({sign}{change:.3f})")


def build_calibrators_for_period(
    period_name: str,
    config: dict,
    cal_n_races: Optional[int] = None,
    dry_run: bool = False,
) -> dict:
    """1つの WF 期間の calibrator を学習して保存する。

    Args:
        period_name: "wf_2024" など
        config: WF_PERIODS の1エントリ
        cal_n_races: None の場合は config の値を使用
        dry_run: True の場合は pickle 保存をスキップ

    Returns:
        結果サマリー dict
    """
    from src.ml.lgbm_model import (
        _load_ml_races, RollingStatsTracker, RollingSireTracker,
        _load_horse_sire_map,
    )

    model_dir = config["model_dir"]
    train_max = config["train_max"]
    n_races = cal_n_races if cal_n_races is not None else config["cal_n_races"]

    print(f"\n{'='*60}")
    print(f"WF Period: {period_name}")
    print(f"  model_dir: {model_dir}")
    print(f"  train_max: {train_max}")
    print(f"  cal_n_races: {n_races}")
    print(f"{'='*60}")

    if not os.path.exists(model_dir):
        print(f"  ERROR: model_dir が見つかりません: {model_dir}")
        return {"skipped": True, "reason": "model_dir not found"}

    t0 = time.time()

    # Step 1: rolling stats 構築 (tracker)
    print(f"\n[Step 1] Rolling stats 構築 (max_date={train_max})...")
    all_races = _load_ml_races(max_date=train_max)
    if not all_races:
        print(f"  ERROR: {train_max} 以前の ML data が見つかりません")
        return {"skipped": True, "reason": "no ml data"}

    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()
    sire_map = _load_horse_sire_map()

    all_races.sort(key=lambda r: r.get("date", ""))
    for race in all_races:
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    print(f"  tracker 構築完了: {len(tracker.horses)} horses")

    # Step 2: calibration 用レースをロード (train_max 直前 N 件)
    print(f"\n[Step 2] calibration レースロード (直前 {n_races} races)...")
    cal_races = load_calibration_races(train_max, n_races)
    if not cal_races:
        print(f"  ERROR: calibration レースが取得できません")
        return {"skipped": True, "reason": "no cal races"}

    date_range = (
        cal_races[0].get("date", "?"),
        cal_races[-1].get("date", "?"),
    )
    print(f"  期間: {date_range[0]} ~ {date_range[1]}")

    # Step 3: calibration サンプル構築
    print(f"\n[Step 3] calibration サンプル構築...")
    raw_probs, labels_top3 = build_calibration_samples(
        cal_races, model_dir, tracker, sire_tracker, sire_map
    )

    if not raw_probs:
        print(f"  ERROR: サンプルが収集できません")
        return {"skipped": True, "reason": "no samples"}

    # Step 4: Isotonic regression 学習 (top3 のみ)
    print(f"\n[Step 4] Isotonic regression 学習 (top3)...")
    cal_top3 = train_isotonic_calibrator(raw_probs, labels_top3)

    # mapping 出力
    log_calibrator_mapping(cal_top3, label="top3")

    # Step 5: pickle 保存
    top3_pkl_path = os.path.join(model_dir, "iso_cal_top3.pkl")
    if not dry_run:
        with open(top3_pkl_path, "wb") as f:
            pickle.dump(cal_top3, f)
        print(f"\n  保存: {top3_pkl_path}")
    else:
        print(f"\n  (dry-run) 保存スキップ: {top3_pkl_path}")

    elapsed = time.time() - t0
    result = {
        "period": period_name,
        "train_max": train_max,
        "cal_races": len(cal_races),
        "samples": len(raw_probs),
        "pos_rate": sum(labels_top3) / len(labels_top3),
        "date_range": date_range,
        "elapsed_s": elapsed,
        "skipped": False,
    }

    print(f"\n{period_name} 完了: {elapsed:.1f}s, サンプル={len(raw_probs):,}")
    return result


def main():
    parser = argparse.ArgumentParser(
        description="WF 期間別 Isotonic Calibrator を学習して保存する"
    )
    parser.add_argument(
        "--period",
        choices=["wf_2024", "wf_2025", "wf_2026", "all"],
        default="all",
        help="対象 WF 期間 (default: all)",
    )
    parser.add_argument(
        "--cal-n-races",
        type=int,
        default=None,
        help="calibration に使うレース数 (default: 期間設定値 3000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="学習のみ実行・pickle 保存なし",
    )
    args = parser.parse_args()

    periods = WF_PERIODS if args.period == "all" else {args.period: WF_PERIODS[args.period]}
    total_start = time.time()
    results = []

    for name, config in periods.items():
        r = build_calibrators_for_period(
            name, config,
            cal_n_races=args.cal_n_races,
            dry_run=args.dry_run,
        )
        results.append(r)

    print(f"\n{'='*60}")
    print(f"全期間 calibrator 学習完了: {time.time()-total_start:.1f}s")
    print(f"\n期間別サマリー:")
    print(f"{'期間':<10} {'サンプル':>8} {'正例率':>8} {'レース数':>8} {'期間':>25}")
    for r in results:
        if r.get("skipped"):
            print(f"  {r.get('period', '?'):<10} SKIPPED: {r.get('reason', '')}")
        else:
            print(f"  {r['period']:<10} {r['samples']:>8,} {r['pos_rate']:>8.3f} "
                  f"{r['cal_races']:>8,} {r['date_range'][0]} ~ {r['date_range'][1]}")

    if args.dry_run:
        print("\n(dry-run: pickle 保存なし)")


if __name__ == "__main__":
    main()
