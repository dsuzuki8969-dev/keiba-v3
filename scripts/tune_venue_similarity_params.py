"""
Grid-search tuner for venue similarity feature parameters.

This script tunes:
  - VENUE_SIM_THRESHOLD
  - DIRECTION_DISCOUNT

using train_and_evaluate() metrics, then applies the best pair
back to src/ml/features.py.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import List

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import src.ml.features as feat_mod
from src.ml.trainer import train_and_evaluate

FEATURES_FILE = ROOT / "src" / "ml" / "features.py"


@dataclass
class TuneResult:
    threshold: float
    discount: float
    auc: float
    top1: float
    top3: float
    score: float


def _score(metrics: dict) -> float:
    # Prioritize AUC, then race-level hit rates.
    auc = float(metrics.get("auc", 0.0))
    top1 = float(metrics.get("race_top1_hit_rate", 0.0))
    top3 = float(metrics.get("race_top3_hit_rate", 0.0))
    return auc * 0.70 + top1 * 0.20 + top3 * 0.10


def _apply_best_params(threshold: float, discount: float) -> None:
    text = FEATURES_FILE.read_text(encoding="utf-8")
    text = text.replace(
        f"VENUE_SIM_THRESHOLD = {feat_mod.VENUE_SIM_THRESHOLD:.2f}",
        f"VENUE_SIM_THRESHOLD = {threshold:.2f}",
    )
    text = text.replace(
        f"DIRECTION_DISCOUNT = {feat_mod.DIRECTION_DISCOUNT:.2f}",
        f"DIRECTION_DISCOUNT = {discount:.2f}",
    )
    FEATURES_FILE.write_text(text, encoding="utf-8")


def main() -> int:
    thresholds = [0.35, 0.40, 0.45]
    discounts = [0.55, 0.65, 0.75]
    combos = list(itertools.product(thresholds, discounts))

    print("=" * 72)
    print("Tune venue similarity parameters")
    print("=" * 72)
    print(f"combos={len(combos)}  thresholds={thresholds}  discounts={discounts}")
    print()

    results: List[TuneResult] = []

    base_threshold = feat_mod.VENUE_SIM_THRESHOLD
    base_discount = feat_mod.DIRECTION_DISCOUNT

    for i, (th, dc) in enumerate(combos, 1):
        print(f"[{i}/{len(combos)}] threshold={th:.2f}, discount={dc:.2f}")
        feat_mod.VENUE_SIM_THRESHOLD = th
        feat_mod.DIRECTION_DISCOUNT = dc

        out = train_and_evaluate(
            start_date="2024-01-01",
            end_date="2026-02-25",
            val_months=3,
            num_boost_round=350,
            early_stopping_rounds=60,
        )
        metrics = out.get("metrics", {})
        if not metrics:
            print("  -> skipped (no metrics)")
            continue

        auc = float(metrics.get("auc", 0.0))
        top1 = float(metrics.get("race_top1_hit_rate", 0.0))
        top3 = float(metrics.get("race_top3_hit_rate", 0.0))
        s = _score(metrics)
        results.append(TuneResult(th, dc, auc, top1, top3, s))
        print(f"  auc={auc:.4f}  top1={top1:.4f}  top3={top3:.4f}  score={s:.5f}")
        print()

    if not results:
        print("No valid runs.")
        feat_mod.VENUE_SIM_THRESHOLD = base_threshold
        feat_mod.DIRECTION_DISCOUNT = base_discount
        return 1

    results.sort(key=lambda r: (r.score, r.auc, r.top1), reverse=True)
    best = results[0]

    print("=" * 72)
    print("Top results")
    print("=" * 72)
    for r in results[:5]:
        print(
            f"th={r.threshold:.2f} dc={r.discount:.2f} | "
            f"auc={r.auc:.4f} top1={r.top1:.4f} top3={r.top3:.4f} score={r.score:.5f}"
        )

    print()
    print(
        f"Best -> threshold={best.threshold:.2f}, discount={best.discount:.2f} "
        f"(score={best.score:.5f})"
    )

    feat_mod.VENUE_SIM_THRESHOLD = base_threshold
    feat_mod.DIRECTION_DISCOUNT = base_discount
    _apply_best_params(best.threshold, best.discount)
    print("Applied best params to src/ml/features.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

