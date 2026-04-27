#!/usr/bin/env python
"""
pred.json の past_runs[].speed_dev を race_log から再注入する軽量スクリプト
==========================================================================
backfill_run_dev.py で race_log が更新された後、当日 pred.json の各馬の
past_runs に焼き込まれた speed_dev は古いまま。それを race_log から取り直す。

使い方:
    python scripts/refresh_pred_speed_dev.py             # 今日の pred.json
    python scripts/refresh_pred_speed_dev.py 20260426    # 指定日
    python scripts/refresh_pred_speed_dev.py --dry-run   # 変更カウントのみ

出力:
    - data/predictions/{date}_pred.json.bak_refresh    バックアップ
    - data/predictions/{date}_pred.json                上書き
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    pred_path = PROJECT_ROOT / "data" / "predictions" / f"{args.date}_pred.json"
    if not pred_path.exists():
        print(f"[ERROR] pred.json 不在: {pred_path}", file=sys.stderr)
        return 1

    db_path = PROJECT_ROOT / "data" / "keiba.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    with open(pred_path, encoding="utf-8") as f:
        pred = json.load(f)

    races = pred.get("races", [])
    print(f"[INFO] 対象: {pred_path.name} レース数={len(races)}")

    updated = 0
    unchanged = 0
    no_match = 0
    no_run_dev = 0

    for race in races:
        for h in race.get("horses", []):
            # past_3_runs が実体（past_runs はレガシー）
            for r in h.get("past_3_runs", []) or h.get("past_runs", []):
                rid = r.get("race_id")
                hno = r.get("horse_no")
                # horse_id は race_log 側で形式不一致（旧2019100043 / 新nar_XXX 混在）
                # のため、race_id + horse_no で一意特定する
                if not rid or hno is None:
                    no_match += 1
                    continue
                row = cur.execute(
                    "SELECT run_dev FROM race_log WHERE race_id=? AND horse_no=?",
                    (str(rid), int(hno)),
                ).fetchone()
                if row is None:
                    no_match += 1
                    continue
                run_dev = row[0]
                if run_dev is None:
                    no_run_dev += 1
                    continue
                new_val = round(float(run_dev), 1)
                old_val = r.get("speed_dev")
                if old_val != new_val:
                    r["speed_dev"] = new_val
                    updated += 1
                else:
                    unchanged += 1

    conn.close()

    print(f"[INFO] 更新: {updated} / 変更なし: {unchanged} / マッチなし: {no_match} / run_dev=NULL: {no_run_dev}")

    if args.dry_run:
        print("[INFO] --dry-run のため保存しない")
        return 0

    # バックアップ + 上書き
    backup = pred_path.with_suffix(".json.bak_refresh")
    shutil.copy(pred_path, backup)
    with open(pred_path, "w", encoding="utf-8") as f:
        json.dump(pred, f, ensure_ascii=False, indent=2)
    print(f"[OK] 保存: {pred_path}（バックアップ: {backup.name}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
