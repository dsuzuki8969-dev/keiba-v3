#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""精度検証: batch_wf_fast.py の推論結果をフルパイプライン結果と比較

フルパイプラインで生成済みの2026年 pred.json の値と、
batch_wf_fast.py が同じデータに対して算出する値を比較する。

ファイルは変更しない（読み取り専用比較）。
"""
import json
import math
import os
import sys
import copy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import PREDICTIONS_DIR

# batch_wf_fast.py の関数を import
from scripts.batch_wf_fast import (
    ModelBundle,
    build_race_dict,
    build_horse_dicts,
    run_inference_for_race,
)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="WF精度検証")
    parser.add_argument("--dates", nargs="+", default=["20260518", "20260517", "20260511"])
    args = parser.parse_args()

    # モデルロード（1回のみ）
    print("=== 精度検証: batch_wf_fast vs フルパイプライン ===\n")
    print("[1/2] モデルロード中...")
    bundle = ModelBundle()
    if not bundle.load():
        print("ERROR: モデルロード失敗")
        return
    print()

    # 検証
    print("[2/2] 比較検証中...\n")

    all_diffs_composite = []
    all_diffs_win = []
    all_diffs_adj = []

    for dt in args.dates:
        pred_path = os.path.join(PREDICTIONS_DIR, f"{dt}_pred.json")
        if not os.path.exists(pred_path):
            print(f"  SKIP: {dt} (ファイルなし)")
            continue

        with open(pred_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        date_str = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"

        print(f"── {dt} ({len(payload.get('races', []))} レース) ──")

        for race in payload.get("races", []):
            # 元の値を保存
            horses = race.get("horses", [])
            active = [h for h in horses if not h.get("is_scratched")]
            if len(active) < 2:
                continue

            original_values = {}
            for h in active:
                hid = h.get("horse_id", "")
                original_values[hid] = {
                    "composite": h.get("composite"),
                    "win_prob": h.get("win_prob"),
                    "place2_prob": h.get("place2_prob"),
                    "place3_prob": h.get("place3_prob"),
                    "ml_composite_adj": h.get("ml_composite_adj"),
                    "mark": h.get("mark"),
                    "horse_name": h.get("horse_name", ""),
                }

            # ディープコピーして推論（元データを壊さない）
            race_copy = copy.deepcopy(race)
            ok = run_inference_for_race(race_copy, date_str, bundle)

            if not ok:
                continue

            # 比較
            active_copy = [h for h in race_copy.get("horses", []) if not h.get("is_scratched")]

            for h in active_copy:
                hid = h.get("horse_id", "")
                orig = original_values.get(hid)
                if not orig:
                    continue

                new_composite = h.get("composite")
                new_win = h.get("win_prob")
                new_adj = h.get("ml_composite_adj")

                orig_composite = orig["composite"]
                orig_win = orig["win_prob"]
                orig_adj = orig["ml_composite_adj"]

                if orig_composite and new_composite:
                    all_diffs_composite.append(abs(new_composite - orig_composite))
                if orig_win and new_win:
                    all_diffs_win.append(abs(new_win - orig_win))
                if orig_adj is not None and new_adj is not None:
                    all_diffs_adj.append(abs(new_adj - orig_adj))

        # 日付ごとのサマリ
        day_comps = [d for d in all_diffs_composite[-500:]]  # 直近データ
        if day_comps:
            avg_c = sum(day_comps) / len(day_comps)
            max_c = max(day_comps)
            print(f"  composite差: 平均={avg_c:.3f}pt, 最大={max_c:.3f}pt")

    # 全体サマリ
    print(f"\n{'='*60}")
    print(f"  全体サマリ ({len(all_diffs_composite)} 頭)")
    print(f"{'='*60}")

    if all_diffs_composite:
        avg_c = sum(all_diffs_composite) / len(all_diffs_composite)
        max_c = max(all_diffs_composite)
        med_c = sorted(all_diffs_composite)[len(all_diffs_composite)//2]
        pct_under_1 = sum(1 for d in all_diffs_composite if d < 1.0) / len(all_diffs_composite) * 100
        pct_under_05 = sum(1 for d in all_diffs_composite if d < 0.5) / len(all_diffs_composite) * 100

        print(f"\n  【composite 差分】")
        print(f"    平均: {avg_c:.4f} pt")
        print(f"    中央値: {med_c:.4f} pt")
        print(f"    最大: {max_c:.4f} pt")
        print(f"    1.0pt未満: {pct_under_1:.1f}%")
        print(f"    0.5pt未満: {pct_under_05:.1f}%")

    if all_diffs_win:
        avg_w = sum(all_diffs_win) / len(all_diffs_win)
        max_w = max(all_diffs_win)
        med_w = sorted(all_diffs_win)[len(all_diffs_win)//2]

        print(f"\n  【win_prob 差分】")
        print(f"    平均: {avg_w:.6f}")
        print(f"    中央値: {med_w:.6f}")
        print(f"    最大: {max_w:.6f}")

    if all_diffs_adj:
        avg_a = sum(all_diffs_adj) / len(all_diffs_adj)
        max_a = max(all_diffs_adj)
        med_a = sorted(all_diffs_adj)[len(all_diffs_adj)//2]

        print(f"\n  【ml_composite_adj 差分】")
        print(f"    平均: {avg_a:.4f} pt")
        print(f"    中央値: {med_a:.4f} pt")
        print(f"    最大: {max_a:.4f} pt")

    # 印の一致率
    # (run_inference_for_race は印再生成しないので、ここでは composite の差分のみ)

    print(f"\n{'='*60}")
    if all_diffs_composite:
        if avg_c < 0.5:
            print("  判定: ✅ 高精度（平均差 0.5pt未満）")
        elif avg_c < 1.0:
            print("  判定: ⚠️ 許容範囲（平均差 0.5-1.0pt）")
        else:
            print("  判定: ❌ 要調査（平均差 1.0pt以上）")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
