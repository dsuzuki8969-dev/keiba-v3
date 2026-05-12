"""race_results DB → results JSON 再生成

空ファイル (<=10 bytes) の results を race_results テーブルから補完。
payouts_json / order_json がそのまま使える。
"""
import argparse
import json
import os
import sqlite3
import sys
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, "data", "keiba.db")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")


def build_results_for_date(conn, race_date):
    """race_results テーブルから results JSON を構築"""
    c = conn.cursor()
    c.execute("""
        SELECT race_id, order_json, payouts_json, cancelled
        FROM race_results
        WHERE date = ?
        ORDER BY race_id
    """, (race_date,))
    rows = c.fetchall()
    if not rows:
        return None

    result = {}
    for race_id, order_json, payouts_json, cancelled in rows:
        if cancelled:
            continue
        try:
            order = json.loads(order_json) if order_json else []
            payouts = json.loads(payouts_json) if payouts_json else {}
        except (json.JSONDecodeError, TypeError):
            continue

        result[race_id] = {
            "order": order,
            "payouts": payouts,
            "source": "race_results_db",
        }

    return result if result else None


def main():
    parser = argparse.ArgumentParser(description="race_results DB → results JSON 補完")
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default="2026-05-11")
    parser.add_argument("--force", action="store_true", help="既存非空ファイルも上書き")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        "SELECT DISTINCT date FROM race_results WHERE date BETWEEN ? AND ? ORDER BY date",
        (args.start, args.end),
    )
    dates = [r[0] for r in c.fetchall()]
    print(f"DB 内の日付: {len(dates)} ({args.start} ~ {args.end})")

    # 空ファイルの日付を特定
    empty_dates = []
    existing_ok = 0
    no_file = 0

    for dt in dates:
        fname = dt.replace("-", "") + "_results.json"
        fpath = os.path.join(RESULTS_DIR, fname)

        if not os.path.exists(fpath):
            empty_dates.append(dt)
            no_file += 1
        elif os.path.getsize(fpath) <= 10:
            empty_dates.append(dt)
        elif args.force:
            empty_dates.append(dt)
        else:
            existing_ok += 1

    print(f"補完対象: {len(empty_dates)} 日 (既存 OK: {existing_ok}, ファイルなし: {no_file})")

    if args.dry_run:
        print("(dry-run)")
        conn.close()
        return

    created = 0
    for i, dt in enumerate(empty_dates):
        fname = dt.replace("-", "") + "_results.json"
        fpath = os.path.join(RESULTS_DIR, fname)

        data = build_results_for_date(conn, dt)
        if data is None:
            continue

        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

        created += 1
        if (i + 1) % 30 == 0 or i == len(empty_dates) - 1:
            pct = (i + 1) / len(empty_dates) * 100
            print(f"  [{i+1}/{len(empty_dates)}] {pct:.0f}% — {dt} ({len(data)} races)")

    conn.close()
    print(f"\n完了: {created} ファイル生成/補完")


if __name__ == "__main__":
    main()
