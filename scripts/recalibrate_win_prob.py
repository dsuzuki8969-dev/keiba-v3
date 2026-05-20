"""
win_prob / place2_prob / place3_prob の事後キャリブレーション

既存 pred.json の確率値に直接 Isotonic Regression を適用し、
レース内再正規化を行う。

engine.py のキャリブレータとは独立:
  - engine.py の PostCalibrator は正規化前のraw値に対して学習されたモデルを適用
  - 本スクリプトは正規化済みpred.json値 → 実着順で学習し、同じ分布に適用
  - 分布不一致問題を回避

使い方:
    python scripts/recalibrate_win_prob.py --train    # 学習+検証
    python scripts/recalibrate_win_prob.py --apply     # 全pred.jsonに適用
    python scripts/recalibrate_win_prob.py --train --apply  # 学習→即適用
"""

import argparse
import json
import os
import sys
import time
import pickle
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVAL_CSV = PROJECT_ROOT / "data" / "csv" / "eval_all.csv"
PRED_DIR = PROJECT_ROOT / "data" / "predictions"
CALIBRATOR_PATH = PROJECT_ROOT / "data" / "models" / "recalibrator_v2.pkl"


def load_eval_data():
    """eval_all.csv から学習に必要なデータをロード"""
    print("eval_all.csv ロード中...")
    df = pd.read_csv(
        EVAL_CSV,
        usecols=[
            "date", "race_id", "horse_no", "is_jra", "venue",
            "win_prob", "place2_prob", "place3_prob",
            "finish_pos", "odds", "composite",
            "payout_tansho", "payout_fukusho",
        ],
        dtype={"is_jra": str},
        low_memory=False,
    )
    # is_jra の変換（混合型対策）
    df["is_jra"] = df["is_jra"].map({"True": True, "False": False, "1": True, "0": False}).fillna(False)

    # 有効データのみ
    mask = (
        df["win_prob"].notna() & (df["win_prob"] > 0) &
        df["finish_pos"].notna() & (df["finish_pos"] > 0)
    )
    df = df[mask].copy()
    df["is_win"] = (df["finish_pos"] == 1).astype(int)
    df["is_top2"] = (df["finish_pos"] <= 2).astype(int)
    df["is_top3"] = (df["finish_pos"] <= 3).astype(int)

    print(f"  有効データ: {len(df):,} 頭")
    print(f"  日付範囲: {df['date'].min()} ~ {df['date'].max()}")
    return df


def train_isotonic(df_train, target_col, prob_col, label):
    """Isotonic Regression を学習してBrier scoreを報告"""
    from sklearn.isotonic import IsotonicRegression

    probs = df_train[prob_col].values
    targets = df_train[target_col].values

    ir = IsotonicRegression(y_min=0.001, y_max=0.999, out_of_bounds="clip")
    ir.fit(probs, targets)

    # 学習セットのBrier score
    cal = ir.transform(probs)
    brier_before = float(np.mean((probs - targets) ** 2))
    brier_after = float(np.mean((cal - targets) ** 2))
    print(f"  {label}: Brier {brier_before:.5f} → {brier_after:.5f} ({(brier_after/brier_before - 1)*100:+.1f}%)")

    return ir


def calibration_table(probs, hits, label, bin_size=0.02):
    """キャリブレーション表（2%刻み）"""
    print(f"\n  === {label} ===")
    print(f"  {'帯域':>10s} | {'件数':>7s} | {'実際':>8s} | {'予測平均':>8s} | {'差分':>7s}")
    print("  " + "-" * 55)
    for lo_pct in range(0, 80, int(bin_size * 100)):
        lo = lo_pct / 100.0
        hi = lo + bin_size
        mask = (probs >= lo) & (probs < hi)
        n = int(mask.sum())
        if n < 20:
            continue
        actual = float(hits[mask].mean()) * 100
        pred_avg = float(probs[mask].mean()) * 100
        diff = actual - pred_avg
        print(f"  {lo_pct:3d}-{lo_pct+int(bin_size*100):3d}% | {n:7d} | {actual:7.2f}% | {pred_avg:7.2f}% | {diff:+6.2f}%")


def apply_calibration_to_race(horses, ir_win, ir_p2, ir_p3):
    """1レースの馬リストにキャリブレーションを適用

    手順:
    1. isotonic変換（個々の確率を補正）
    2. レース内Σ=1.0再正規化（相対比率を保持しつつ制約を満たす）
    3. 整合性保証 (win ≤ place2 ≤ place3)
    """
    n = len(horses)
    if n < 2:
        return

    # 元の値を取得
    wps = np.array([h.get("win_prob", 0) or 0 for h in horses], dtype=float)
    p2s = np.array([h.get("place2_prob", 0) or 0 for h in horses], dtype=float)
    p3s = np.array([h.get("place3_prob", 0) or 0 for h in horses], dtype=float)

    # 全ゼロチェック
    if wps.sum() <= 0:
        return

    # 変換前の比率を保存（place2/win, place3/win）
    # これにより、win_prob の変更に連動して place2/place3 も適切にスケール
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio_p2_w = np.where(wps > 0, p2s / wps, 2.0)
        ratio_p3_w = np.where(wps > 0, p3s / wps, 3.0)

    # Isotonic変換
    cal_win = ir_win.transform(wps)
    cal_win = np.clip(cal_win, 0.001, 0.999)

    # win_probの再正規化 (Σ=1.0)
    tw = cal_win.sum()
    if tw > 0:
        cal_win = cal_win / tw

    # place2/place3 は比率保存方式で再構築
    cal_p2 = cal_win * ratio_p2_w
    cal_p3 = cal_win * ratio_p3_w

    # place2/place3 の再正規化
    t2 = cal_p2.sum()
    t3 = cal_p3.sum()
    target_p2 = min(n, 2)
    target_p3 = min(n, 3)
    if t2 > 0:
        cal_p2 = cal_p2 / t2 * target_p2
    if t3 > 0:
        cal_p3 = cal_p3 / t3 * target_p3

    # 整合性制約: win ≤ place2 ≤ place3
    for i in range(n):
        w = cal_win[i]
        cal_p2[i] = max(cal_p2[i], w + 0.003)
        cal_p3[i] = max(cal_p3[i], cal_p2[i] + 0.003)

    # 書き戻し
    for i, h in enumerate(horses):
        h["win_prob"] = round(float(cal_win[i]), 4)
        h["place2_prob"] = round(float(cal_p2[i]), 4)
        h["place3_prob"] = round(float(cal_p3[i]), 4)


def validate_with_renormalization(df_val, ir_win, ir_p2, ir_p3):
    """検証セットでレース内再正規化込みのキャリブレーションを評価"""
    print("\n--- レース内再正規化込み検証 ---")

    # レースごとにグループ化して再正規化
    cal_wps = []
    actual_wins = []

    for race_id, group in df_val.groupby("race_id"):
        wps = group["win_prob"].values
        wins = group["is_win"].values

        if len(wps) < 2 or wps.sum() <= 0:
            continue

        # isotonic変換
        cal = ir_win.transform(wps)
        cal = np.clip(cal, 0.001, 0.999)

        # レース内再正規化
        total = cal.sum()
        if total > 0:
            cal = cal / total

        cal_wps.extend(cal)
        actual_wins.extend(wins)

    cal_wps = np.array(cal_wps)
    actual_wins = np.array(actual_wins)

    print(f"  検証レース馬数: {len(cal_wps):,}")

    # Brier score
    brier_before = float(np.mean((df_val["win_prob"].values[:len(actual_wins)] - actual_wins) ** 2))
    brier_after = float(np.mean((cal_wps - actual_wins) ** 2))
    print(f"  勝率 Brier: {brier_before:.5f} → {brier_after:.5f} ({(brier_after/brier_before - 1)*100:+.1f}%)")

    # キャリブレーション表
    calibration_table(cal_wps, actual_wins, "勝率 (calibrated + renormalized)")

    # Before
    calibration_table(
        df_val["win_prob"].values[:len(actual_wins)],
        actual_wins,
        "勝率 (before, 参考)"
    )

    return cal_wps, actual_wins


def simulate_roi(df_val, ir_win):
    """検証セットで簡易ROIシミュレーション

    ◎（各レースwin_prob最大馬）の単勝的中率とROIを比較
    """
    print("\n--- 簡易ROIシミュレーション (◎単勝) ---")

    before_bets = 0
    before_hits = 0
    before_payout = 0

    after_bets = 0
    after_hits = 0
    after_payout = 0

    for race_id, group in df_val.groupby("race_id"):
        if len(group) < 2:
            continue

        wps = group["win_prob"].values
        wins = group["is_win"].values
        odds = group["odds"].values
        payouts = group["payout_tansho"].values

        # Before: 最大win_prob馬
        best_idx = np.argmax(wps)
        before_bets += 1
        if wins[best_idx] == 1:
            before_hits += 1
            p = payouts[best_idx]
            before_payout += p if pd.notna(p) and p > 0 else (odds[best_idx] * 100 if pd.notna(odds[best_idx]) else 0)

        # After: isotonic変換→再正規化後の最大馬
        cal = ir_win.transform(wps)
        cal = np.clip(cal, 0.001, 0.999)
        total = cal.sum()
        if total > 0:
            cal = cal / total
        after_best_idx = np.argmax(cal)
        after_bets += 1
        if wins[after_best_idx] == 1:
            after_hits += 1
            p = payouts[after_best_idx]
            after_payout += p if pd.notna(p) and p > 0 else (odds[after_best_idx] * 100 if pd.notna(odds[after_best_idx]) else 0)

    # 表示
    before_rate = before_hits / before_bets * 100 if before_bets > 0 else 0
    after_rate = after_hits / after_bets * 100 if after_bets > 0 else 0
    before_roi = before_payout / (before_bets * 100) * 100 if before_bets > 0 else 0
    after_roi = after_payout / (after_bets * 100) * 100 if after_bets > 0 else 0

    # ◎の選択が変わったレース数
    changed = 0
    for race_id, group in df_val.groupby("race_id"):
        if len(group) < 2:
            continue
        wps = group["win_prob"].values
        cal = ir_win.transform(wps)
        cal = np.clip(cal, 0.001, 0.999)
        total = cal.sum()
        if total > 0:
            cal = cal / total
        if np.argmax(wps) != np.argmax(cal):
            changed += 1

    print(f"  Before: {before_hits}/{before_bets} ({before_rate:.1f}%) ROI={before_roi:.1f}%")
    print(f"  After:  {after_hits}/{after_bets} ({after_rate:.1f}%) ROI={after_roi:.1f}%")
    print(f"  ◎選択変更レース数: {changed}")


def train_and_validate():
    """学習+検証"""
    df = load_eval_data()

    # 時系列分割: 2024-2025を学習、2026を検証
    # → 2026年データで out-of-sample 検証
    train_mask = df["date"].astype(str) < "20260101"
    val_mask = df["date"].astype(str) >= "20260101"

    df_train = df[train_mask]
    df_val = df[val_mask]

    print(f"\n学習: {df_train['date'].min()} ~ {df_train['date'].max()} ({len(df_train):,} 頭)")
    print(f"検証: {df_val['date'].min()} ~ {df_val['date'].max()} ({len(df_val):,} 頭)")

    # Before キャリブレーション
    print("\n=== Before (現状) ===")
    calibration_table(df_val["win_prob"].values, df_val["is_win"].values, "勝率")
    calibration_table(df_val["place2_prob"].values, df_val["is_top2"].values, "連対率")
    calibration_table(df_val["place3_prob"].values, df_val["is_top3"].values, "複勝率")

    # Isotonic学習
    print("\n=== Isotonic Regression 学習 (学習セット) ===")
    ir_win = train_isotonic(df_train, "is_win", "win_prob", "勝率")
    ir_p2 = train_isotonic(df_train, "is_top2", "place2_prob", "連対率")
    ir_p3 = train_isotonic(df_train, "is_top3", "place3_prob", "複勝率")

    # レース内再正規化込み検証
    validate_with_renormalization(df_val, ir_win, ir_p2, ir_p3)

    # 簡易ROIシミュレーション
    simulate_roi(df_val, ir_win)

    # モデル保存
    os.makedirs(CALIBRATOR_PATH.parent, exist_ok=True)
    model_data = {
        "win": ir_win,
        "top2": ir_p2,
        "top3": ir_p3,
        "train_count": len(df_train),
        "train_date_range": (str(df_train["date"].min()), str(df_train["date"].max())),
        "val_count": len(df_val),
        "val_date_range": (str(df_val["date"].min()), str(df_val["date"].max())),
    }
    with open(CALIBRATOR_PATH, "wb") as f:
        pickle.dump(model_data, f)
    print(f"\nモデル保存: {CALIBRATOR_PATH}")

    return ir_win, ir_p2, ir_p3


def apply_to_predictions(ir_win=None, ir_p2=None, ir_p3=None):
    """全 pred.json にキャリブレーションを適用"""

    # モデルロード
    if ir_win is None:
        if not CALIBRATOR_PATH.exists():
            print("キャリブレータ未検出。先に --train を実行してください。")
            sys.exit(1)
        with open(CALIBRATOR_PATH, "rb") as f:
            model_data = pickle.load(f)
        ir_win = model_data["win"]
        ir_p2 = model_data["top2"]
        ir_p3 = model_data["top3"]
        print(f"キャリブレータ ロード完了 (学習: {model_data['train_count']:,} 頭)")

    # pred.jsonファイル一覧
    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    print(f"\n適用対象: {len(pred_files)} ファイル")

    total_races = 0
    total_horses = 0
    significant_changes = 0  # win_probが1%以上変化した馬の数
    t0 = time.time()

    for i, pf in enumerate(pred_files):
        with open(pf, "r", encoding="utf-8") as f:
            payload = json.load(f)

        changed = False
        for race in payload.get("races", []):
            horses = race.get("horses", [])
            if not horses:
                continue

            # 変更前のwin_probを記録
            before_wps = [h.get("win_prob", 0) for h in horses]

            # キャリブレーション適用
            apply_calibration_to_race(horses, ir_win, ir_p2, ir_p3)

            total_races += 1
            total_horses += len(horses)

            # 変化量チェック
            for j, h in enumerate(horses):
                diff = abs((h.get("win_prob", 0) or 0) - (before_wps[j] or 0))
                if diff >= 0.01:
                    significant_changes += 1
                    changed = True

        # 書き戻し
        if changed:
            with open(pf, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

        # プログレス (50ファイルごと)
        if (i + 1) % 50 == 0 or i == len(pred_files) - 1:
            pct = (i + 1) / len(pred_files) * 100
            elapsed = time.time() - t0
            eta = elapsed / (i + 1) * (len(pred_files) - i - 1) if i > 0 else 0
            print(f"  [{pct:5.1f}%] {i+1}/{len(pred_files)} ファイル "
                  f"({total_races:,} レース, {total_horses:,} 頭) "
                  f"| 経過 {elapsed:.0f}s | 残 {eta:.0f}s")

    elapsed = time.time() - t0
    print(f"\n完了: {total_races:,} レース, {total_horses:,} 頭")
    print(f"  win_prob ±1%以上変化: {significant_changes:,} 頭")
    print(f"  所要時間: {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="win_prob 事後キャリブレーション (直接pred.json補正)")
    parser.add_argument("--train", action="store_true", help="学習+検証")
    parser.add_argument("--apply", action="store_true", help="全pred.jsonに適用")
    args = parser.parse_args()

    if not args.train and not args.apply:
        parser.print_help()
        sys.exit(0)

    ir_win, ir_p2, ir_p3 = None, None, None
    if args.train:
        ir_win, ir_p2, ir_p3 = train_and_validate()

    if args.apply:
        apply_to_predictions(ir_win, ir_p2, ir_p3)


if __name__ == "__main__":
    main()
