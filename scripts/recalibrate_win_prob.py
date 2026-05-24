"""
win_prob / place2_prob / place3_prob の事後キャリブレーション

Phase 3 (2026-05-24): JRA/NAR 分離対応
- --target jra: JRA レースのみで学習 → recalibrator_v2_jra.pkl
- --target nar: NAR レースのみで学習 → recalibrator_v2_nar.pkl
- --target all: 両方順次学習
- --target legacy: 旧方式 (JRA/NAR 共通) → recalibrator_v2.pkl
- apply 時は recalibrator_v2_jra/nar.pkl が両方揃っていれば自動で切替

既存 pred.json の確率値に直接 Isotonic Regression を適用し、
レース内再正規化を行う。

engine.py のキャリブレータとは独立:
  - engine.py の PostCalibrator は正規化前のraw値に対して学習されたモデルを適用
  - 本スクリプトは正規化済みpred.json値 → 実着順で学習し、同じ分布に適用
  - 分布不一致問題を回避

使い方:
    python scripts/recalibrate_win_prob.py --train --target all       # JRA + NAR 学習
    python scripts/recalibrate_win_prob.py --train --target jra       # JRA のみ学習
    python scripts/recalibrate_win_prob.py --train --target nar       # NAR のみ学習
    python scripts/recalibrate_win_prob.py --apply                    # 全pred.jsonに適用 (自動分岐)
    python scripts/recalibrate_win_prob.py --train --apply --target all  # 学習→即適用
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

# Phase 3 (2026-05-24): JRA/NAR 別ファイル
CALIBRATOR_PATH_LEGACY = PROJECT_ROOT / "data" / "models" / "recalibrator_v2.pkl"
CALIBRATOR_PATH_JRA = PROJECT_ROOT / "data" / "models" / "recalibrator_v2_jra.pkl"
CALIBRATOR_PATH_NAR = PROJECT_ROOT / "data" / "models" / "recalibrator_v2_nar.pkl"

# NAR 14 場 + 帯広 = NAR 判定セット
NAR_VENUES = {
    "大井", "船橋", "川崎", "浦和", "園田", "姫路", "名古屋", "笠松",
    "金沢", "門別", "盛岡", "水沢", "高知", "佐賀", "帯広",
}


def load_eval_data(filter_jra: bool = None):
    """eval_all.csv から学習に必要なデータをロード

    Args:
        filter_jra: None=全件, True=JRAのみ, False=NARのみ
    """
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

    total_before = len(df)
    # Phase 3: JRA/NAR フィルタ
    if filter_jra is True:
        df = df[df["is_jra"] == True].copy()
    elif filter_jra is False:
        df = df[df["is_jra"] == False].copy()

    print(f"  有効データ: {len(df):,} 頭 (全体 {total_before:,} 頭から {filter_jra=} フィルタ後)")
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


def validate_with_renormalization(df_val, ir_win, ir_p2, ir_p3, target_label=""):
    """検証セットでレース内再正規化込みのキャリブレーションを評価"""
    print(f"\n--- {target_label} レース内再正規化込み検証 ---")

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

    return cal_wps, actual_wins


def train_and_validate(target: str = "legacy"):
    """学習+検証

    Args:
        target: "jra" / "nar" / "legacy" — 学習対象
    """
    if target == "jra":
        filter_jra = True
        output_path = CALIBRATOR_PATH_JRA
        label = "JRA"
    elif target == "nar":
        filter_jra = False
        output_path = CALIBRATOR_PATH_NAR
        label = "NAR"
    else:
        filter_jra = None
        output_path = CALIBRATOR_PATH_LEGACY
        label = "LEGACY"

    print(f"\n{'='*70}")
    print(f"  ターゲット: {label}")
    print(f"  出力: {output_path}")
    print(f"{'='*70}")

    df = load_eval_data(filter_jra=filter_jra)

    if len(df) < 1000:
        print(f"データ不足 ({len(df)} 頭, 最低 1,000 必要) → {label} スキップ")
        return None, None, None

    # 時系列分割: 2024-2025を学習、2026を検証
    train_mask = df["date"].astype(str) < "20260101"
    val_mask = df["date"].astype(str) >= "20260101"

    df_train = df[train_mask]
    df_val = df[val_mask]

    print(f"\n学習: {df_train['date'].min()} ~ {df_train['date'].max()} ({len(df_train):,} 頭)")
    print(f"検証: {df_val['date'].min()} ~ {df_val['date'].max()} ({len(df_val):,} 頭)")

    # Before キャリブレーション
    print(f"\n=== {label} Before (現状) ===")
    calibration_table(df_val["win_prob"].values, df_val["is_win"].values, "勝率")

    # Isotonic学習
    print(f"\n=== {label} Isotonic Regression 学習 (学習セット) ===")
    ir_win = train_isotonic(df_train, "is_win", "win_prob", "勝率")
    ir_p2 = train_isotonic(df_train, "is_top2", "place2_prob", "連対率")
    ir_p3 = train_isotonic(df_train, "is_top3", "place3_prob", "複勝率")

    # レース内再正規化込み検証
    validate_with_renormalization(df_val, ir_win, ir_p2, ir_p3, target_label=label)

    # モデル保存
    os.makedirs(output_path.parent, exist_ok=True)
    model_data = {
        "win": ir_win,
        "top2": ir_p2,
        "top3": ir_p3,
        "target": target,
        "train_count": len(df_train),
        "train_date_range": (str(df_train["date"].min()), str(df_train["date"].max())),
        "val_count": len(df_val),
        "val_date_range": (str(df_val["date"].min()), str(df_val["date"].max())),
    }
    with open(output_path, "wb") as f:
        pickle.dump(model_data, f)
    print(f"\n[{label}] モデル保存: {output_path}")

    return ir_win, ir_p2, ir_p3


def _load_recalibrators():
    """JRA/NAR 別 recalibrator をロード。両方揃えば split モード、なければ legacy にフォールバック。

    Returns:
        (mode, models_jra, models_nar, models_legacy)
        mode: "split" / "legacy" / "none"
    """
    models_jra = {}
    models_nar = {}
    models_legacy = {}

    if CALIBRATOR_PATH_JRA.exists() and CALIBRATOR_PATH_NAR.exists():
        with open(CALIBRATOR_PATH_JRA, "rb") as f:
            d = pickle.load(f)
            models_jra = {"win": d["win"], "top2": d["top2"], "top3": d["top3"]}
        with open(CALIBRATOR_PATH_NAR, "rb") as f:
            d = pickle.load(f)
            models_nar = {"win": d["win"], "top2": d["top2"], "top3": d["top3"]}
        print(f"recalibrator 切替: split (JRA + NAR 別)")
        return "split", models_jra, models_nar, models_legacy

    if CALIBRATOR_PATH_LEGACY.exists():
        with open(CALIBRATOR_PATH_LEGACY, "rb") as f:
            d = pickle.load(f)
            models_legacy = {"win": d["win"], "top2": d["top2"], "top3": d["top3"]}
        print(f"recalibrator 切替: legacy (JRA/NAR 共通)")
        return "legacy", models_jra, models_nar, models_legacy

    print("recalibrator 未検出 - 先に --train を実行してください")
    return "none", models_jra, models_nar, models_legacy


def apply_to_predictions(ir_win=None, ir_p2=None, ir_p3=None):
    """全 pred.json にキャリブレーションを適用 (Phase 3: JRA/NAR 自動分岐)"""

    # split / legacy モード判定
    mode, models_jra, models_nar, models_legacy = _load_recalibrators()
    if mode == "none":
        sys.exit(1)

    # pred.jsonファイル一覧
    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    print(f"\n適用対象: {len(pred_files)} ファイル")

    total_races = 0
    jra_races = 0
    nar_races = 0
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

            # Phase 3: JRA/NAR 判定
            venue = race.get("venue", "")
            is_nar_race = venue in NAR_VENUES

            # モデル選択
            if mode == "split":
                if is_nar_race:
                    models = models_nar
                    nar_races += 1
                else:
                    models = models_jra
                    jra_races += 1
            else:
                models = models_legacy
                if is_nar_race:
                    nar_races += 1
                else:
                    jra_races += 1

            # 変更前のwin_probを記録
            before_wps = [h.get("win_prob", 0) for h in horses]

            # キャリブレーション適用
            apply_calibration_to_race(horses, models["win"], models["top2"], models["top3"])

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
    print(f"\n完了: {total_races:,} レース (JRA {jra_races:,} / NAR {nar_races:,}), {total_horses:,} 頭")
    print(f"  win_prob ±1%以上変化: {significant_changes:,} 頭")
    print(f"  所要時間: {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="win_prob 事後キャリブレーション (直接pred.json補正)")
    parser.add_argument("--train", action="store_true", help="学習+検証")
    parser.add_argument("--apply", action="store_true", help="全pred.jsonに適用")
    parser.add_argument("--target", choices=["jra", "nar", "all", "legacy"], default="all",
                        help="学習対象: jra=JRA専用, nar=NAR専用, all=両方順次(推奨), legacy=旧方式(JRA/NAR共通)")
    args = parser.parse_args()

    if not args.train and not args.apply:
        parser.print_help()
        sys.exit(0)

    if args.train:
        if args.target == "all":
            print("=== JRA + NAR 順次学習 ===")
            train_and_validate(target="jra")
            train_and_validate(target="nar")
        else:
            train_and_validate(target=args.target)

    if args.apply:
        apply_to_predictions()


if __name__ == "__main__":
    main()
