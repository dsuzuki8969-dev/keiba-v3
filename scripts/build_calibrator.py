"""
事後キャリブレータの学習スクリプト (Phase 3: JRA/NAR 分離対応)

使い方:
    python scripts/build_calibrator.py                       # legacy: JRA/NAR 共通
    python scripts/build_calibrator.py --target jra          # JRA 専用 (calibrator_*_jra.pkl)
    python scripts/build_calibrator.py --target nar          # NAR 専用 (calibrator_*_nar.pkl)
    python scripts/build_calibrator.py --target all          # JRA + NAR を順次学習
    python scripts/build_calibrator.py --validate-only       # 既存モデルの検証のみ

data/predictions/*.json と data/results/*.json を突合し、
Isotonic Regression でキャリブレータを学習・保存する。

Phase 3 (2026-05-24): JRA データと NAR データの分布差を反映するため、
JRA/NAR 別 Isotonic モデルを学習可能に。
"""

import json
import os
import pickle
import sys
import glob
import argparse
from collections import defaultdict

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# NAR 14 場 + 帯広 = NAR 判定セット
NAR_VENUES = {
    "大井", "船橋", "川崎", "浦和", "園田", "姫路", "名古屋", "笠松",
    "金沢", "門別", "盛岡", "水沢", "高知", "佐賀", "帯広",
}


def _is_nar_race(race: dict) -> bool:
    """レースが NAR か判定"""
    venue = race.get("venue", "")
    return venue in NAR_VENUES


def collect_pairs(pred_dir: str, result_dir: str, filter_jra: bool = None):
    """予測JSONと結果JSONを突合して (予測確率, 実着順) ペアを収集

    Args:
        filter_jra: None=全件, True=JRAのみ, False=NARのみ
    """
    pred_files = sorted(glob.glob(os.path.join(pred_dir, "*_pred.json")))
    result_map = {}
    for rf in glob.glob(os.path.join(result_dir, "*.json")):
        key = os.path.basename(rf).replace("_result.json", "").replace("_results.json", "").replace(".json", "")
        result_map[key] = rf

    win_pairs = []   # (prob, is_1st)
    top2_pairs = []  # (prob, is_top2)
    top3_pairs = []  # (prob, is_top3)
    dates = []

    skip_count = 0
    for pf in pred_files:
        date_str = os.path.basename(pf).replace("_pred.json", "")
        rf = result_map.get(date_str)
        if rf is None:
            continue

        with open(pf, "r", encoding="utf-8") as f:
            pred = json.load(f)
        with open(rf, "r", encoding="utf-8") as f:
            results = json.load(f)

        for race in pred.get("races", []):
            # Phase 3: JRA/NAR フィルタ
            if filter_jra is not None:
                is_nar = _is_nar_race(race)
                if filter_jra and is_nar:
                    skip_count += 1
                    continue
                if not filter_jra and not is_nar:
                    skip_count += 1
                    continue

            race_id = race.get("race_id", "")
            if race_id not in results:
                continue
            finish_map = {}
            for o in results[race_id].get("order", []):
                finish_map[o["horse_no"]] = o["finish"]

            for h in race.get("horses", []):
                hno = h.get("horse_no")
                if hno is None or hno not in finish_map:
                    continue
                finish = finish_map[hno]
                if finish is None or finish <= 0:
                    continue
                wp = h.get("win_prob")
                p2 = h.get("place2_prob")
                p3 = h.get("place3_prob")
                if wp is not None and p2 is not None and p3 is not None:
                    win_pairs.append((wp, 1 if finish == 1 else 0))
                    top2_pairs.append((p2, 1 if finish <= 2 else 0))
                    top3_pairs.append((p3, 1 if finish <= 3 else 0))
                    dates.append(date_str)

    if filter_jra is not None:
        target_name = "JRA" if filter_jra else "NAR"
        print(f"  {target_name} フィルタ適用: {skip_count} レーススキップ")

    return (
        np.array(win_pairs),
        np.array(top2_pairs),
        np.array(top3_pairs),
        np.array(dates),
    )


def calibration_table(probs, hits, label):
    """5%刻みのキャリブレーション表を出力"""
    print(f"\n=== {label} ===")
    print(f"{'帯域':>10s} | {'件数':>7s} | {'実際':>8s} | {'予測平均':>8s} | {'差分':>7s}")
    print("-" * 55)
    for lo_pct in range(0, 100, 5):
        lo, hi = lo_pct / 100.0, (lo_pct + 5) / 100.0
        mask = (probs >= lo) & (probs < hi)
        n = int(mask.sum())
        if n < 10:
            continue
        actual = float(hits[mask].mean()) * 100
        pred_avg = float(probs[mask].mean()) * 100
        diff = actual - pred_avg
        print(f"  {lo_pct:2d}-{lo_pct+5:2d}%   | {n:7d} | {actual:7.2f}% | {pred_avg:7.2f}% | {diff:+6.2f}%")


def train_isotonic(probs, hits, label):
    """Isotonic Regression を学習"""
    from sklearn.isotonic import IsotonicRegression
    ir = IsotonicRegression(y_min=0.001, y_max=0.999, out_of_bounds="clip")
    ir.fit(probs, hits)
    cal = ir.transform(probs)
    # Brier score
    brier_before = float(np.mean((probs - hits) ** 2))
    brier_after = float(np.mean((cal - hits) ** 2))
    print(f"  {label}: Brier {brier_before:.5f} → {brier_after:.5f} ({(brier_after/brier_before - 1)*100:+.1f}%)")
    return ir


def build_one_target(pred_dir: str, result_dir: str, model_dir: str,
                     target: str, validate_only: bool):
    """1 ターゲット (jra/nar/legacy) の学習・検証・保存

    Args:
        target: "jra" / "nar" / "legacy" (None=全件・JRA/NAR 共通)
    """
    print(f"\n{'='*70}")
    print(f"  ターゲット: {target.upper()}")
    print(f"{'='*70}")

    if target == "jra":
        filter_jra = True
        suffix = "_jra"
    elif target == "nar":
        filter_jra = False
        suffix = "_nar"
    else:
        filter_jra = None
        suffix = ""

    print("データ収集中...")
    win_arr, top2_arr, top3_arr, dates = collect_pairs(pred_dir, result_dir, filter_jra=filter_jra)
    print(f"収集完了: {len(win_arr)} 頭")

    if len(win_arr) < 1000:
        print(f"データ不足（最低1000頭必要、現在 {len(win_arr)}）→ {target} スキップ")
        return False

    # 時系列分割: 前80%学習、後20%検証
    unique_dates = sorted(set(dates))
    split_idx = int(len(unique_dates) * 0.80)
    split_date = unique_dates[split_idx]
    train_mask = dates < split_date
    val_mask = dates >= split_date

    print(f"学習: ~{split_date} ({int(train_mask.sum())}頭)")
    print(f"検証: {split_date}~ ({int(val_mask.sum())}頭)")

    # Before キャリブレーション
    print(f"\n--- {target.upper()} Before (現状) ---")
    calibration_table(win_arr[val_mask, 0], win_arr[val_mask, 1], "勝率")
    calibration_table(top2_arr[val_mask, 0], top2_arr[val_mask, 1], "連対率")
    calibration_table(top3_arr[val_mask, 0], top3_arr[val_mask, 1], "複勝率")

    if validate_only:
        # 既存モデルで変換して検証
        from src.ml.calibrator import (
            CALIBRATOR_PATHS_JRA, CALIBRATOR_PATHS_NAR, CALIBRATOR_PATHS_LEGACY,
        )
        if target == "jra":
            target_paths = CALIBRATOR_PATHS_JRA
        elif target == "nar":
            target_paths = CALIBRATOR_PATHS_NAR
        else:
            target_paths = CALIBRATOR_PATHS_LEGACY

        models = {}
        for key, path in target_paths.items():
            if os.path.exists(path):
                with open(path, "rb") as f:
                    models[key] = pickle.load(f)
        if len(models) == 3:
            print(f"\n--- {target.upper()} After (既存キャリブレータ適用) ---")
            cal_win = models["win"].transform(win_arr[val_mask, 0])
            cal_top2 = models["top2"].transform(top2_arr[val_mask, 0])
            cal_top3 = models["top3"].transform(top3_arr[val_mask, 0])
            calibration_table(cal_win, win_arr[val_mask, 1], "勝率 (calibrated)")
            calibration_table(cal_top2, top2_arr[val_mask, 1], "連対率 (calibrated)")
            calibration_table(cal_top3, top3_arr[val_mask, 1], "複勝率 (calibrated)")
        else:
            print(f"キャリブレータモデルが見つかりません ({target})")
        return True

    # 学習
    print(f"\n--- {target.upper()} Isotonic Regression 学習 ---")
    ir_win = train_isotonic(win_arr[train_mask, 0], win_arr[train_mask, 1], "勝率")
    ir_top2 = train_isotonic(top2_arr[train_mask, 0], top2_arr[train_mask, 1], "連対率")
    ir_top3 = train_isotonic(top3_arr[train_mask, 0], top3_arr[train_mask, 1], "複勝率")

    # 検証セットで After
    print(f"\n--- {target.upper()} After (Isotonic適用 on 検証セット) ---")
    cal_win = ir_win.transform(win_arr[val_mask, 0])
    cal_top2 = ir_top2.transform(top2_arr[val_mask, 0])
    cal_top3 = ir_top3.transform(top3_arr[val_mask, 0])
    calibration_table(cal_win, win_arr[val_mask, 1], "勝率 (calibrated)")
    calibration_table(cal_top2, top2_arr[val_mask, 1], "連対率 (calibrated)")
    calibration_table(cal_top3, top3_arr[val_mask, 1], "複勝率 (calibrated)")

    # 保存
    os.makedirs(model_dir, exist_ok=True)
    for key, model in [("win", ir_win), ("top2", ir_top2), ("top3", ir_top3)]:
        path = os.path.join(model_dir, f"calibrator_{key}{suffix}.pkl")
        with open(path, "wb") as f:
            pickle.dump(model, f)
        print(f"保存: {path}")

    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["jra", "nar", "all", "legacy"], default="legacy",
                        help="学習対象: jra=JRA専用, nar=NAR専用, all=両方順次, legacy=JRA/NAR共通 (デフォルト)")
    parser.add_argument("--validate-only", action="store_true",
                        help="既存モデルの検証のみ")
    args = parser.parse_args()

    proj = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    pred_dir = os.path.join(proj, "data", "predictions")
    result_dir = os.path.join(proj, "data", "results")
    model_dir = os.path.join(proj, "data", "models")

    if args.target == "all":
        # JRA → NAR の順に学習
        targets = ["jra", "nar"]
    else:
        targets = [args.target]

    success_count = 0
    for tgt in targets:
        ok = build_one_target(pred_dir, result_dir, model_dir, tgt, args.validate_only)
        if ok:
            success_count += 1

    print(f"\n{'='*70}")
    print(f"完了: {success_count}/{len(targets)} ターゲット成功")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
