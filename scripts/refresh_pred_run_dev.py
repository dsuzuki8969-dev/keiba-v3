#!/usr/bin/env python
"""
pred.json の past_runs[].run_dev / race_level_dev を race_log から再注入する。

backfill_run_dev.py / backfill_race_level_dev.py で race_log が更新された後、
pred.json の各馬の past_runs に焼き込まれた値は古いまま。それを race_log から取り直す。

使い方:
    python scripts/refresh_pred_run_dev.py             # 今日の pred.json
    python scripts/refresh_pred_run_dev.py 20260426    # 指定日
    python scripts/refresh_pred_run_dev.py --dry-run   # 変更カウントのみ
    python scripts/refresh_pred_run_dev.py --recent 7  # 直近 7 日分すべて
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def refresh_one(pred_path: Path, dry_run: bool) -> dict:
    """1 日分の pred.json を refresh して stats を返す。"""
    date_str = pred_path.stem.replace("_pred", "")
    if not pred_path.exists():
        return {"date": date_str, "status": "missing", "updated_run_dev": 0, "updated_race_level_dev": 0}

    # バックアップ作成（本実行時のみ）
    if not dry_run:
        bak = pred_path.with_suffix(f".json.bak_refresh_run_dev_{date.today():%Y%m%d}")
        shutil.copy(pred_path, bak)

    db_path = PROJECT_ROOT / "data" / "keiba.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    with open(pred_path, encoding="utf-8") as f:
        pred = json.load(f)

    races = pred.get("races", [])
    updated_run_dev = 0
    updated_race_level_dev = 0
    unchanged = 0
    no_match = 0

    for race in races:
        for h in race.get("horses", []):
            horse_id = h.get("horse_id")
            if not horse_id:
                continue
            # past_3_runs が実体、past_runs はレガシー互換
            past_runs = h.get("past_3_runs") or h.get("past_runs") or []
            for r in past_runs:
                # race_id または race_date でルックアップ
                race_id = r.get("race_id")
                race_date = r.get("race_date") or r.get("date")

                row = None
                if race_id:
                    row = cur.execute(
                        "SELECT run_dev, race_level_dev FROM race_log "
                        "WHERE race_id = ? AND horse_id = ? LIMIT 1",
                        (str(race_id), str(horse_id)),
                    ).fetchone()
                if row is None and race_date:
                    row = cur.execute(
                        "SELECT run_dev, race_level_dev FROM race_log "
                        "WHERE horse_id = ? AND race_date = ? LIMIT 1",
                        (str(horse_id), str(race_date)),
                    ).fetchone()

                if row is None:
                    no_match += 1
                    continue

                new_run_dev, new_race_level_dev = row
                changed = False

                # run_dev 更新
                if new_run_dev is not None:
                    new_val = round(float(new_run_dev), 1)
                    if r.get("run_dev") != new_val:
                        r["run_dev"] = new_val
                        updated_run_dev += 1
                        changed = True

                # race_level_dev 更新
                if new_race_level_dev is not None:
                    new_val = round(float(new_race_level_dev), 1)
                    if r.get("race_level_dev") != new_val:
                        r["race_level_dev"] = new_val
                        updated_race_level_dev += 1
                        changed = True

                if not changed:
                    unchanged += 1

    # 変更があれば保存（本実行時のみ）
    if not dry_run and (updated_run_dev or updated_race_level_dev):
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(pred, f, ensure_ascii=False, separators=(",", ":"))

    conn.close()
    return {
        "date": date_str,
        "status": "ok",
        "updated_run_dev": updated_run_dev,
        "updated_race_level_dev": updated_race_level_dev,
        "unchanged": unchanged,
        "no_match": no_match,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="pred.json の run_dev / race_level_dev を race_log から再注入する"
    )
    parser.add_argument("date", nargs="?", default=date.today().strftime("%Y%m%d"),
                        help="対象日付 YYYYMMDD（省略時: 今日）")
    parser.add_argument("--dry-run", action="store_true",
                        help="変更カウントのみ表示し保存しない")
    parser.add_argument("--recent", type=int, default=0,
                        help="直近 N 日分すべて refresh（date 指定より優先）")
    args = parser.parse_args()

    pred_dir = PROJECT_ROOT / "data" / "predictions"

    # 対象日付リストを構築
    targets: list[Path] = []
    if args.recent > 0:
        today = date.today()
        for i in range(args.recent):
            d = today - timedelta(days=i)
            targets.append(pred_dir / f"{d.strftime('%Y%m%d')}_pred.json")
    else:
        targets.append(pred_dir / f"{args.date}_pred.json")

    print(f"[INFO] 対象: {len(targets)} ファイル ({'DRY-RUN' if args.dry_run else '本実行'})")

    total_run_dev = 0
    total_race_level_dev = 0
    for path in targets:
        result = refresh_one(path, args.dry_run)
        if result["status"] == "missing":
            print(f"  [SKIP] {result['date']} (ファイル不在)")
            continue
        print(
            f"  [{result['date']}] "
            f"run_dev={result['updated_run_dev']} "
            f"race_level_dev={result['updated_race_level_dev']} "
            f"no_match={result['no_match']}"
        )
        total_run_dev += result["updated_run_dev"]
        total_race_level_dev += result["updated_race_level_dev"]

    print(f"\n[完了] 合計 run_dev={total_run_dev}, race_level_dev={total_race_level_dev}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
