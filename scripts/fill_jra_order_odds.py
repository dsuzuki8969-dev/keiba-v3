# -*- coding: utf-8 -*-
"""JRA結果のorderに単勝オッズを補完する。

マスター指摘(2026-06-21): 中央競馬(JRA)の単勝オッズが結果テーブルで拾えていない。
真因: JRA公式の結果ページ着順テーブルには「単勝人気」列はあるが「単勝オッズ」列が
      存在しない（NARの結果ページにはodds列が有る）。そのため
      official_odds._parse_jra_result_order は odds を抽出できず order に odds が入らない。
対処: pred.json の各馬最終単勝オッズ（予想時=締切直前に取得・全馬確実に存在）を
      results.json の order[].odds に補完する。NAR は既に odds があるため
      「odds 欠落の馬のみ」補完（NAR確定オッズは保持）。

使い方:
  python scripts/fill_jra_order_odds.py --date 20260621
  python scripts/fill_jra_order_odds.py --date 20260621 --dry-run
"""
import argparse
import json
import os
import shutil
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def fill_odds(date: str, dry_run: bool = False) -> dict:
    pred_fp = os.path.join("data", "predictions", f"{date}_pred.json")
    res_fp = os.path.join("data", "results", f"{date}_results.json")
    if not (os.path.isfile(pred_fp) and os.path.isfile(res_fp)):
        print(f"ファイルなし: pred={os.path.isfile(pred_fp)} res={os.path.isfile(res_fp)}")
        return {"filled": 0, "races": 0}

    with open(pred_fp, "r", encoding="utf-8") as f:
        pred = json.load(f)
    with open(res_fp, "r", encoding="utf-8") as f:
        res = json.load(f)

    # pred: race_id -> {horse_no: odds}
    pred_odds = {}
    for race in pred.get("races", []):
        rid = race.get("race_id")
        if not rid:
            continue
        pred_odds[rid] = {
            int(h.get("horse_no")): h.get("odds")
            for h in race.get("horses", [])
            if h.get("horse_no") is not None and h.get("odds")
        }

    filled = 0
    races_touched = 0
    for rid, r in res.items():
        if not isinstance(r, dict):
            continue
        om = pred_odds.get(rid, {})
        if not om:
            continue
        race_filled = 0
        for o in r.get("order", []):
            # 既に odds がある馬（NAR確定オッズ等）は触らない
            if o.get("odds"):
                continue
            hno = o.get("horse_no")
            od = om.get(int(hno)) if hno is not None else None
            if od:
                o["odds"] = od
                race_filled += 1
        if race_filled:
            races_touched += 1
            filled += race_filled

    print(f"補完対象: {filled}頭 / {races_touched}レース")
    if dry_run:
        print("(dry-run: 保存せず)")
        return {"filled": filled, "races": races_touched}

    if filled:
        bak = res_fp + ".bak_oddsfill"
        shutil.copy(res_fp, bak)
        print(f"backup -> {bak}")
        with open(res_fp, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)
        print(f"保存完了: {res_fp}")
    return {"filled": filled, "races": races_touched}


def main():
    ap = argparse.ArgumentParser(description="JRA結果order に pred最終単勝オッズを補完")
    ap.add_argument("--date", required=True, help="対象日 (例: 20260621)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    print(f"=== JRA order 単勝オッズ補完 date={args.date} ===")
    fill_odds(args.date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
