# -*- coding: utf-8 -*-
"""G-7: DB race_results.payouts_json が空 '{}' で保存されている race を
results.json (正データ) から backfill する。

修復対象: 91.8% (80,342 件 / 87,532 件) の race で payouts_json='{}'。
影響: dashboard /api/results/detailed の by_conf 集計が異常 (max_payout 1040 / ROI 0.9%)。

修正方針:
- data/results/*_results.json を全件 scan
- 各 race の payouts を DB race_results.payouts_json に UPDATE
- 既に payouts_json が空でない race は touch しない (最小修正原則)
- 終了後 data/cache/agg_daily/detail/ を削除して再生成を促す
"""
from __future__ import annotations
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "keiba.db"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"
DETAIL_CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "agg_daily" / "detail"


def progress_bar(done: int, total: int, label: str = "", width: int = 30) -> None:
    pct = done / total * 100 if total else 0.0
    filled = int(width * done / total) if total else 0
    bar = "█" * filled + "░" * (width - filled)
    print(f"\r[{bar}] {pct:5.1f}% ({done:,}/{total:,}) {label}", end="", flush=True)


def main(execute: bool = False) -> int:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # 影響範囲を先に取得
    total_rows = conn.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
    empty_before = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE payouts_json='{}' OR payouts_json IS NULL OR payouts_json=''"
    ).fetchone()[0]
    print(f"DB race_results 総数: {total_rows:,}")
    print(f"修復対象 (payouts_json 空): {empty_before:,} ({empty_before/total_rows*100:.1f}%)")

    # results.json ファイル一覧 (古い → 新しい)
    result_files = sorted(RESULTS_DIR.glob("*_results.json"))
    print(f"results.json ファイル数: {len(result_files):,}")
    print()

    if not execute:
        print("=== DRY RUN: --execute 指定で本実行 ===")
        # 5 件だけサンプル確認
        for fp in result_files[:5]:
            date_str = fp.stem.replace("_results", "")
            db_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            try:
                data = json.loads(fp.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"  {fp.name}: SKIP (load error: {e})")
                continue
            if not isinstance(data, dict):
                continue
            empty_in_db = 0
            has_payouts_in_json = 0
            for rid in data:
                row = conn.execute(
                    "SELECT payouts_json FROM race_results WHERE date=? AND race_id=?",
                    (db_date, rid),
                ).fetchone()
                if row is None:
                    continue
                pj = row[0]
                if pj in ("{}", "", None):
                    empty_in_db += 1
                if data[rid].get("payouts"):
                    has_payouts_in_json += 1
            print(f"  {db_date}: race数={len(data)} / DB空={empty_in_db} / json有payouts={has_payouts_in_json}")
        return 0

    # 本実行
    t0 = time.time()
    fixed = 0
    skipped_no_payouts = 0  # results.json 側にも payouts なし
    skipped_already_filled = 0  # DB 側にもう正データあり
    skipped_no_db_row = 0  # DB に該当 race row なし
    err = 0

    n_files = len(result_files)
    for i, fp in enumerate(result_files):
        date_str = fp.stem.replace("_results", "")
        if len(date_str) != 8 or not date_str.isdigit():
            continue
        db_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            err += 1
            continue
        if not isinstance(data, dict):
            continue

        # 該当日の DB row を一括取得 (date 形式が with-hyphen / no-hyphen 混在のため両方検索)
        cur_map = {}  # race_id -> (db_date_actual, payouts_json)
        for row in conn.execute(
            "SELECT date, race_id, payouts_json FROM race_results WHERE date IN (?, ?)",
            (db_date, date_str),
        ):
            # 同 race_id が両形式で存在する場合、空 を優先 (修復対象)
            existing = cur_map.get(row[1])
            if existing is None or (existing[1] not in ("{}", "", None) and row[2] in ("{}", "", None)):
                cur_map[row[1]] = (row[0], row[2])

        updates = []  # [(payouts_json, db_date_actual, race_id)]
        for rid, r in data.items():
            if not isinstance(r, dict):
                continue
            payouts = r.get("payouts") or {}
            if not payouts:
                skipped_no_payouts += 1
                continue
            entry = cur_map.get(rid)
            if entry is None:
                skipped_no_db_row += 1
                continue
            db_date_actual, cur = entry
            if cur not in ("{}", "", None):
                skipped_already_filled += 1
                continue
            new_json = json.dumps(payouts, ensure_ascii=False)
            updates.append((new_json, db_date_actual, rid))

        if updates:
            conn.executemany(
                "UPDATE race_results SET payouts_json=? WHERE date=? AND race_id=?",
                updates,
            )
            conn.commit()
            fixed += len(updates)

        if (i + 1) % 30 == 0 or (i + 1) == n_files:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (n_files - i - 1) / rate if rate > 0 else 0
            progress_bar(
                i + 1, n_files,
                f"fixed={fixed:,} skipped={skipped_already_filled+skipped_no_payouts:,} ETA={eta:.0f}s"
            )

    print()
    elapsed = time.time() - t0
    print(f"\n=== 完了 ({elapsed:.1f}s) ===")
    print(f"  修復: {fixed:,} race")
    print(f"  スキップ (DB に既存値あり): {skipped_already_filled:,}")
    print(f"  スキップ (results.json に payouts なし): {skipped_no_payouts:,}")
    print(f"  スキップ (DB に該当 race row なし): {skipped_no_db_row:,}")
    print(f"  ファイル読み込みエラー: {err:,}")

    # 修復後の状態確認
    empty_after = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE payouts_json='{}' OR payouts_json IS NULL OR payouts_json=''"
    ).fetchone()[0]
    print(f"\n空 payouts_json: {empty_before:,} → {empty_after:,} ({(empty_before-empty_after)/empty_before*100:.1f}% 修復)")
    conn.close()

    # detail cache を削除して再生成を促す
    if DETAIL_CACHE_DIR.exists():
        n_cache = len(list(DETAIL_CACHE_DIR.glob("*.json")))
        for cf in DETAIL_CACHE_DIR.glob("*.json"):
            cf.unlink()
        print(f"detail キャッシュ削除: {n_cache:,} ファイル")

    return 0


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--execute", action="store_true", help="本実行 (デフォルトは dry-run)")
    args = p.parse_args()
    sys.exit(main(execute=args.execute))
