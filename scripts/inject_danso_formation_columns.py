#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""N-4: 既存 pred.json の danso レースに formation_columns(列構造) のみを注入する。

印・買い目は一切変更しない（regen_strategy の _regen_marks_for_race を呼ばない）。
compute_danso_columns を既存印・composite に適用し、tickets_by_mode._meta.formation_columns
（col1/col2/col3 = 馬番リスト）を追加するだけ。

用途:
  - バックエンド(engine.py / regen_strategy.py)の N-4 変更は次回バッチから自動適用されるが、
    既にデプロイ済の当日 pred には formation_columns が無い。
  - 当日 pred の印を変えずに N-4 表示を即反映 + 実画面検証するための surgical patch。

使い方:
  python scripts/inject_danso_formation_columns.py --dates 20260621
  python scripts/inject_danso_formation_columns.py --dates 20260621 --dry-run
"""
import argparse
import json
import os
import sys

# プロジェクトルートを import path に追加
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.calculator.betting import compute_danso_columns  # noqa: E402

PREDICTIONS_DIR = os.path.join(_ROOT, "data", "predictions")

# 印→記号変換の必要はない（pred の mark は既に記号 ◎○▲… で格納）


def _build_entries(horses):
    """regen_strategy.py L229-238 と同一の entry 構築（取消馬も渡す）。"""
    return [
        {
            "mark":         h.get("mark", "-") or "-",
            "composite":    float(h.get("composite", 50.0) or 50.0),
            "horse_no":     int(h.get("horse_no", 0)),
            "odds":         float(h.get("odds") or h.get("predicted_tansho_odds") or 10.0),
            "is_scratched": bool(h.get("is_scratched", False)),
        }
        for h in horses
    ]


def process_file(pred_path, dry_run=False):
    with open(pred_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    races = payload.get("races", [])
    n_danso = 0          # danso format のレース数
    n_injected = 0       # formation_columns を入れた（発火した）レース数
    n_skip_meta = 0      # danso だが見送り(発火せず None)
    samples = []         # 検証用サンプル

    for race in races:
        tbm = race.get("tickets_by_mode")
        if not isinstance(tbm, dict):
            continue
        meta = tbm.get("_meta")
        if not isinstance(meta, dict):
            continue
        fmt = str(meta.get("format", "") or "")
        if not fmt.startswith("danso:"):
            continue
        n_danso += 1

        horses = race.get("horses", [])
        entries = _build_entries(horses)
        result = compute_danso_columns(entries)

        if result is None:
            # 断層非発火（見送り）→ formation_columns は None
            meta["formation_columns"] = None
            n_skip_meta += 1
            continue

        cols = {
            "col1": [int(x) for x in result["col1"]],
            "col2": [int(x) for x in result["col2"]],
            "col3": [int(x) for x in result["col3"]],
        }
        meta["formation_columns"] = cols
        n_injected += 1

        # 検証サンプル（最初の5発火レース）
        if len(samples) < 5:
            no_to_mark = {int(h.get("horse_no", 0)): (h.get("mark", "-") or "-") for h in horses}
            fixed = tbm.get("fixed", []) or []
            n_tickets = len([t for t in fixed if t.get("type") == "三連複"])

            def _fmt_col(nos):
                # 印の強い順に並べて 印+馬番 を表示
                order = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "★": 5, "☆": 6}
                s = sorted(nos, key=lambda n: order.get(no_to_mark.get(n, "?"), 9))
                return " ".join(f"{no_to_mark.get(n, '?')}{n}" for n in s)

            samples.append({
                "race_id": race.get("race_id") or race.get("race_no") or "?",
                "venue": race.get("venue_name", race.get("venue_code", "?")),
                "formation": result["formation"],
                "col1": _fmt_col(cols["col1"]),
                "col2": _fmt_col(cols["col2"]),
                "col3": _fmt_col(cols["col3"]),
                "tickets_in_pred": n_tickets,
            })

    if not dry_run:
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    return {
        "path": pred_path,
        "races": len(races),
        "danso": n_danso,
        "injected": n_injected,
        "skip": n_skip_meta,
        "samples": samples,
    }


def main():
    parser = argparse.ArgumentParser(description="N-4 formation_columns 注入（印・買い目不変）")
    parser.add_argument("--dates", nargs="+", required=True, help="対象日付 (例: 20260621)")
    parser.add_argument("--dry-run", action="store_true", help="保存しない")
    args = parser.parse_args()

    for d in args.dates:
        p = os.path.join(PREDICTIONS_DIR, f"{d}_pred.json")
        if not os.path.exists(p):
            print(f"  ⚠ 見つからない: {p}")
            continue
        r = process_file(p, dry_run=args.dry_run)
        tag = "[DRY-RUN] " if args.dry_run else ""
        print(f"=== {tag}{d}: {r['races']}R / danso {r['danso']}R / "
              f"発火(注入) {r['injected']}R / 見送り {r['skip']}R ===")
        for s in r["samples"]:
            print(f"  [{s['formation']}] {s['venue']} {s['race_id']}  "
                  f"({s['tickets_in_pred']}点)")
            print(f"      col1: {s['col1']}")
            print(f"      col2: {s['col2']}")
            print(f"      col3: {s['col3']}")
        if args.dry_run:
            print("  [DRY-RUN] ファイル未更新")


if __name__ == "__main__":
    main()
