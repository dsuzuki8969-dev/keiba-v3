"""Walk-Forward バックテスト用モデル学習

マスター方針:
  2024予想 ← 2022+2023 データで学習
  2025予想 ← 2022+2023+2024 データで学習
  2026予想 ← 2022+2023+2024+2025 データで学習

各年のモデルは data/models/wf_{year}/ に保存される。
"""

import argparse
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

MODEL_BASE = os.path.join(PROJECT_ROOT, "data", "models")

WALK_FORWARD_PLAN = [
    {
        "target_year": 2024,
        "max_date": "2023-12-31",
        "label": "2024予想用 (学習: 2022+2023)",
    },
    {
        "target_year": 2025,
        "max_date": "2024-12-31",
        "label": "2025予想用 (学習: 2022-2024)",
    },
    {
        "target_year": 2026,
        "max_date": "2025-12-31",
        "label": "2026予想用 (学習: 2022-2025)",
    },
]


def main():
    parser = argparse.ArgumentParser(description="Walk-Forward モデル学習")
    parser.add_argument("--year", type=int, choices=[2024, 2025, 2026],
                        help="特定年のみ学習 (省略時: 全3年)")
    parser.add_argument("--valid-days", type=int, default=30,
                        help="検証セット日数 (default: 30)")
    parser.add_argument("--dry-run", action="store_true",
                        help="学習せず計画のみ表示")
    args = parser.parse_args()

    plans = WALK_FORWARD_PLAN
    if args.year:
        plans = [p for p in plans if p["target_year"] == args.year]

    print("=" * 60)
    print("Walk-Forward バックテスト学習計画")
    print("=" * 60)
    for p in plans:
        model_dir = os.path.join(MODEL_BASE, f"wf_{p['target_year']}")
        print(f"  {p['label']}")
        print(f"    max_date: {p['max_date']}")
        print(f"    保存先:   {model_dir}")
    print("=" * 60)

    if args.dry_run:
        print("(dry-run: 学習なし)")
        return

    from src.ml.lgbm_model import train_split_models

    for p in plans:
        year = p["target_year"]
        model_dir = os.path.join(MODEL_BASE, f"wf_{year}")
        print(f"\n{'='*60}")
        print(f"[{year}] {p['label']}")
        print(f"{'='*60}")

        t0 = time.time()
        results = train_split_models(
            valid_days=args.valid_days,
            max_date=p["max_date"],
            model_dir_override=model_dir,
        )
        elapsed = time.time() - t0

        trained = sum(1 for m in results.values() if not m.get("skipped"))
        skipped = sum(1 for m in results.values() if m.get("skipped"))
        aucs = [m["auc"] for m in results.values() if m.get("auc")]
        avg_auc = sum(aucs) / len(aucs) if aucs else 0

        print(f"\n[{year}] 完了: {trained} モデル学習, {skipped} スキップ, "
              f"平均AUC={avg_auc:.4f}, {elapsed:.0f}秒")

    print("\n全年 Walk-Forward 学習完了")


if __name__ == "__main__":
    main()
