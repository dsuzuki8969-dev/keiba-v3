"""
予想オッズ精度検証・期待値ベースバックテスト

過去データを使って:
1. 予想オッズと実オッズの相関を検証
2. 「期待値プラスだけ買う」戦略の回収率をシミュレーション
"""

import json
import os
from collections import defaultdict
from typing import Dict, List, Optional

import numpy as np

from src.log import get_logger

logger = get_logger(__name__)

_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DATA_DIR = os.path.join(_BASE, "data", "ml")
MODEL_DIR = os.path.join(_BASE, "data", "models")


def run_backtest(
    start_date: str = "2024-06-01",
    end_date: str = None,
) -> dict:
    """
    バックテスト: 学習済み三連率モデルで過去データを推論し、
    予想オッズと実オッズの精度を検証する。
    """
    from src.ml.lgbm_model import (
        FEATURE_COLUMNS,
        RollingStatsTracker,
        _extract_features,
        _load_ml_races,
    )
    from src.ml.probability_model import ProbabilityPredictor

    print("\n" + "=" * 60)
    print("  予想オッズ バックテスト")
    print("=" * 60)

    predictor = ProbabilityPredictor()
    if not predictor.load():
        print("[エラー] 三連率モデルが見つかりません。先に --ml_prob を実行してください。")
        return {}

    races = _load_ml_races()
    if not races:
        print("[エラー] MLデータがありません")
        return {}

    if end_date is None:
        from datetime import datetime
        end_date = datetime.now().strftime("%Y-%m-%d")

    target_races = [r for r in races
                    if start_date <= r.get("date", "") <= end_date]
    print(f"\n  対象期間: {start_date} ~ {end_date}")
    print(f"  対象レース数: {len(target_races)}")

    if not target_races:
        print("[エラー] 対象レースがありません")
        return {}

    # 1. 予想勝率 vs 実オッズの相関
    print("\n[1/3] 予想勝率 vs 実オッズ相関分析...")
    win_prob_list = []
    actual_odds_list = []
    actual_win_list = []

    for race in target_races:
        n = race.get("field_count", len(race.get("horses", [])))
        if n < 3:
            continue

        race_dict = {
            "date": race.get("date", ""),
            "venue": race.get("venue", ""),
            "surface": race.get("surface", ""),
            "distance": race.get("distance", 0),
            "condition": race.get("condition", "良"),
            "field_count": n,
            "is_jra": race.get("is_jra", True),
            "grade": race.get("grade", ""),
            "venue_code": race.get("venue_code", "0"),
        }

        horse_dicts = []
        for h in race.get("horses", []):
            if h.get("finish_pos") is None:
                continue
            horse_dicts.append({
                "horse_id": h.get("horse_id", ""),
                "jockey_id": h.get("jockey_id", ""),
                "trainer_id": h.get("trainer_id", ""),
                "gate_no": h.get("gate_no"),
                "horse_no": h.get("horse_no"),
                "sex": h.get("sex", ""),
                "age": h.get("age"),
                "weight_kg": h.get("weight_kg"),
                "odds": h.get("odds"),
                "horse_weight": h.get("horse_weight"),
                "weight_change": h.get("weight_change"),
            })

        if len(horse_dicts) < 3:
            continue

        probs = predictor.predict_race(race_dict, horse_dicts)
        if not probs:
            continue

        for h in race.get("horses", []):
            hid = h.get("horse_id", "")
            odds = h.get("odds")
            fp = h.get("finish_pos")
            p = probs.get(hid, {})
            wp = p.get("win", 0)

            if odds and odds > 0 and wp > 0 and fp:
                win_prob_list.append(wp)
                actual_odds_list.append(odds)
                actual_win_list.append(1 if fp == 1 else 0)

    win_probs = np.array(win_prob_list)
    actual_odds = np.array(actual_odds_list)
    actual_wins = np.array(actual_win_list)
    implied_probs = 1.0 / actual_odds * 0.8  # JRA控除率80%

    if len(win_probs) == 0:
        print("[エラー] 有効なデータがありません")
        return {}

    # 相関係数
    corr = np.corrcoef(win_probs, implied_probs)[0, 1]
    print(f"  サンプル数: {len(win_probs)}")
    print(f"  予想勝率 vs 実勝率(オッズ逆算) 相関: {corr:.4f}")

    # キャリブレーション: 予測確率帯ごとの実際の勝率
    bins = [(0, 0.03), (0.03, 0.06), (0.06, 0.10), (0.10, 0.15),
            (0.15, 0.25), (0.25, 0.50), (0.50, 1.0)]
    print("\n  キャリブレーション:")
    print(f"  {'確率帯':<14s} {'予測平均':>8s} {'実勝率':>8s} {'n':>8s}")
    for lo, hi in bins:
        mask = (win_probs >= lo) & (win_probs < hi)
        if mask.sum() > 0:
            pred_mean = win_probs[mask].mean()
            actual_mean = actual_wins[mask].mean()
            print(f"  {lo:.2f}-{hi:.2f}       {pred_mean:8.4f} {actual_mean:8.4f} {mask.sum():8d}")

    # 2. 期待値ベース回収率シミュレーション
    print("\n[2/3] 期待値ベース回収率シミュレーション...")

    strategies = {
        "全馬購入": lambda wp, ao: True,
        "期待値>1.0": lambda wp, ao: wp * ao >= 1.0,
        "期待値>1.2": lambda wp, ao: wp * ao >= 1.2,
        "期待値>1.5": lambda wp, ao: wp * ao >= 1.5,
        "期待値>2.0": lambda wp, ao: wp * ao >= 2.0,
    }

    print(f"\n  {'戦略':<16s} {'購入数':>8s} {'的中数':>8s} {'投資':>10s} {'回収':>10s} {'回収率':>8s}")
    print(f"  {'-' * 60}")

    results = {}
    for name, cond in strategies.items():
        mask = np.array([cond(wp, ao) for wp, ao in zip(win_probs, actual_odds)])
        if mask.sum() == 0:
            print(f"  {name:<16s} {'0':>8s}")
            continue

        n_buy = mask.sum()
        n_hit = actual_wins[mask].sum()
        investment = n_buy * 100  # 各100円
        payout = (actual_wins[mask] * actual_odds[mask] * 100).sum()
        roi = payout / investment * 100 if investment > 0 else 0

        print(f"  {name:<16s} {n_buy:>8d} {n_hit:>8.0f} {investment:>10,.0f} {payout:>10,.0f} {roi:>7.1f}%")
        results[name] = {"n_buy": int(n_buy), "n_hit": int(n_hit),
                         "investment": int(investment), "payout": int(payout),
                         "roi": round(roi, 1)}

    # 3. 乖離率分析
    print("\n[3/3] 乖離率分析...")
    predicted_odds_vals = 1.0 / np.maximum(win_probs, 0.001) * 0.8
    divergence = actual_odds / np.maximum(predicted_odds_vals, 0.1)

    div_bins = [(0, 0.5), (0.5, 0.8), (0.8, 1.2), (1.2, 1.5),
                (1.5, 2.0), (2.0, 3.0), (3.0, 100)]
    print(f"\n  {'乖離率帯':<16s} {'n':>6s} {'勝率':>8s} {'回収率':>8s}")
    print(f"  {'-' * 40}")
    for lo, hi in div_bins:
        mask = (divergence >= lo) & (divergence < hi)
        if mask.sum() > 0:
            wr = actual_wins[mask].mean() * 100
            roi = (actual_wins[mask] * actual_odds[mask]).mean() * 100
            label = f"{lo:.1f}-{hi:.1f}" if hi < 100 else f"{lo:.1f}+"
            print(f"  {label:<16s} {mask.sum():>6d} {wr:>7.1f}% {roi:>7.1f}%")

    print(f"\n{'=' * 60}")
    print("  バックテスト完了")
    print(f"{'=' * 60}")

    return {
        "correlation": round(corr, 4),
        "total_samples": len(win_probs),
        "strategies": results,
    }
