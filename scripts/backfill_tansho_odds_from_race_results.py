"""
race_log.tansho_odds バックフィルスクリプト
==========================================
race_results.order_json の odds フィールドから tansho_odds（確定単勝オッズ）を復元する。

netkeiba アクセス不要。既存 DB データのみで完結。

使い方:
    python scripts/backfill_tansho_odds_from_race_results.py --dry-run   # 対象件数確認のみ
    python scripts/backfill_tansho_odds_from_race_results.py             # 実行
    python scripts/backfill_tansho_odds_from_race_results.py --since 2026-04-01  # 特定日以降のみ
"""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

# プロジェクトルートを sys.path に追加
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DATABASE_PATH


def run_backfill(dry_run: bool = False, since: str = None):
    """race_results.order_json.odds → race_log.tansho_odds をバックフィル"""
    t0 = time.time()
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    # 対象件数確認
    query_null = """
        SELECT COUNT(*) FROM race_log
        WHERE tansho_odds IS NULL
    """
    params_null = []
    if since:
        query_null += " AND race_date >= ?"
        params_null.append(since)

    null_total = conn.execute(query_null, params_null).fetchone()[0]
    print(f"tansho_odds NULL 件数: {null_total:,}件", flush=True)

    if null_total == 0:
        print("バックフィル不要。全件 tansho_odds 設定済みです。")
        conn.close()
        return

    # 対象 race_id を取得
    query_rids = """
        SELECT DISTINCT race_id FROM race_log
        WHERE tansho_odds IS NULL
    """
    params_rids = []
    if since:
        query_rids += " AND race_date >= ?"
        params_rids.append(since)

    null_rids = [r[0] for r in conn.execute(query_rids, params_rids).fetchall()]
    print(f"対象 race_id: {len(null_rids):,}件", flush=True)

    # race_results に存在するかチェック
    batch_size = 500
    total_updated = 0
    total_no_rr = 0
    total_no_odds = 0
    processed_races = 0

    for bi in range(0, len(null_rids), batch_size):
        chunk = null_rids[bi:bi + batch_size]
        placeholders = ",".join(["?"] * len(chunk))
        rr_rows = conn.execute(
            f"SELECT race_id, order_json FROM race_results WHERE race_id IN ({placeholders})",
            chunk,
        ).fetchall()

        rr_map = {r["race_id"]: r["order_json"] for r in rr_rows}
        total_no_rr += len(chunk) - len(rr_map)

        updates = []
        for race_id in chunk:
            order_json_raw = rr_map.get(race_id)
            if not order_json_raw:
                continue
            try:
                orders = json.loads(order_json_raw)
            except (json.JSONDecodeError, TypeError):
                continue

            for entry in orders:
                hno = entry.get("horse_no")
                odds_raw = entry.get("odds")
                if hno is None or odds_raw is None:
                    total_no_odds += 1
                    continue
                try:
                    odds_f = float(odds_raw)
                except (ValueError, TypeError):
                    continue
                updates.append((odds_f, race_id, int(hno)))

        processed_races += len(rr_map)

        if not dry_run and updates:
            cur = conn.cursor()
            for odds_f, race_id, hno in updates:
                cur.execute(
                    "UPDATE race_log SET tansho_odds=? WHERE race_id=? AND horse_no=? AND tansho_odds IS NULL",
                    (odds_f, race_id, hno),
                )
                total_updated += cur.rowcount
            conn.commit()
        elif dry_run:
            total_updated += len(updates)  # dry-run では更新予定数としてカウント

        # プログレス表示
        pct = (bi + len(chunk)) / len(null_rids) * 100
        elapsed = time.time() - t0
        print(
            f"  [{pct:5.1f}%] 処理済 race_id: {bi + len(chunk):,}/{len(null_rids):,}"
            f" | {'予定' if dry_run else '更新済'}: {total_updated:,}件"
            f" | 経過: {elapsed:.1f}s",
            flush=True,
        )

    elapsed_total = time.time() - t0
    print("=" * 60, flush=True)
    print(f"完了 ({elapsed_total:.1f}秒)", flush=True)
    print(f"  処理 race_id  : {processed_races:,}件", flush=True)
    print(f"  race_results なし: {total_no_rr:,}件", flush=True)
    print(f"  {'更新予定' if dry_run else '更新実績'}: {total_updated:,}件", flush=True)
    if dry_run:
        print("  ※ --dry-run モード。実際の変更は行っていません。", flush=True)
        print("  ※ 実行するには --dry-run を外してください。", flush=True)

    # 最終確認
    if not dry_run:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE tansho_odds IS NULL" +
            (" AND race_date >= ?" if since else ""),
            ([since] if since else []),
        ).fetchone()[0]
        print(f"  残 NULL 件数  : {remaining:,}件", flush=True)
        if remaining == 0:
            print("  tansho_odds 全件設定完了！", flush=True)
        else:
            print(f"  ※ {remaining:,} 件は race_results に対応データなし（スクレイピング前のデータ等）", flush=True)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="race_log.tansho_odds バックフィル")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="実際の更新は行わず、対象件数のみ確認する",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="指定日以降のみ対象（例: 2026-04-01）。省略時は全件対象",
    )
    args = parser.parse_args()
    run_backfill(dry_run=args.dry_run, since=args.since)
