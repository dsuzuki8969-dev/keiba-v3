"""
race_log.finish_time_sec=0 行を race_results.order_json から復元する。

背景:
- 4 月以降 約 7,000 行 / 全期間 12,200 行が finish_time=0 で run_dev / race_level_dev 計算不能
- race_results テーブルには time_sec/last_3f データが存在
- INSERT OR IGNORE で race_log 既存行が UPDATE されなかった残骸

動作:
- race_log.finish_time_sec=0 の行を抽出
- 同 race_id の race_results.order_json から time_sec, last_3f, margin_ahead, margin_behind を取得
- UPDATE race_log

使い方:
    python scripts/fix_race_log_finish_time.py             # 全期間
    python scripts/fix_race_log_finish_time.py --since 2026-04-01  # 指定日以降
    python scripts/fix_race_log_finish_time.py --dry-run   # 件数のみ
"""
from __future__ import annotations
import argparse, json, sqlite3, sys, time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB = PROJECT_ROOT / "data" / "keiba.db"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default=None, help="YYYY-MM-DD 以降のみ処理")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))

    # 対象抽出: race_log で finish_time_sec=0 の行
    where = ["finish_time_sec = 0"]
    params: list = []
    if args.since:
        where.append("race_date >= ?")
        params.append(args.since)
    sql = f"SELECT race_id, horse_no FROM race_log WHERE {' AND '.join(where)}"
    rows = conn.execute(sql, params).fetchall()
    print(f"[INFO] 対象 race_log 行: {len(rows):,}")

    # race_id ごとに order_json をキャッシュして同一 race_id の重複 SELECT を回避
    by_race: dict = {}
    for race_id, horse_no in rows:
        by_race.setdefault(race_id, []).append(horse_no)
    print(f"[INFO] 対象 race_id 数: {len(by_race):,}")

    if args.dry_run:
        print("[DRY-RUN] UPDATE 実行せず")
        conn.close()
        return 0

    updated = 0
    no_results = 0
    no_match = 0
    t0 = time.time()
    BATCH = 5000

    for i, (race_id, horse_nos) in enumerate(by_race.items()):
        rr = conn.execute(
            "SELECT order_json FROM race_results WHERE race_id=?", (race_id,)
        ).fetchone()
        if not rr or not rr[0]:
            no_results += len(horse_nos)
            continue
        try:
            order = json.loads(rr[0])
        except Exception:
            no_results += len(horse_nos)
            continue
        # horse_no → entry のマップ
        by_no = {int(o.get("horse_no", 0)): o for o in order if o.get("horse_no") is not None}
        # 各馬を UPDATE
        for hno in horse_nos:
            entry = by_no.get(hno)
            if not entry:
                no_match += 1
                continue
            ts = entry.get("time_sec")
            l3f = entry.get("last_3f")
            try:
                ts_f = float(ts) if ts is not None else 0.0
            except (TypeError, ValueError):
                ts_f = 0.0
            if ts_f <= 0:
                no_match += 1
                continue
            try:
                l3f_f = float(l3f) if l3f is not None and float(l3f) > 0 else None
            except (TypeError, ValueError):
                l3f_f = None
            # margin 計算 (1着の time_sec を winner_t として差分)
            try:
                ma_f = float(entry.get("margin_ahead", 0)) if entry.get("margin_ahead") is not None else None
            except (TypeError, ValueError):
                ma_f = None
            try:
                mb_f = float(entry.get("margin_behind", 0)) if entry.get("margin_behind") is not None else None
            except (TypeError, ValueError):
                mb_f = None
            if l3f_f is not None:
                conn.execute(
                    "UPDATE race_log SET finish_time_sec=?, last_3f_sec=? WHERE race_id=? AND horse_no=?",
                    (ts_f, l3f_f, race_id, hno),
                )
            else:
                conn.execute(
                    "UPDATE race_log SET finish_time_sec=? WHERE race_id=? AND horse_no=?",
                    (ts_f, race_id, hno),
                )
            updated += 1

        if (i + 1) % BATCH == 0:
            conn.commit()
            dur = time.time() - t0
            print(f"  {i+1:,}/{len(by_race):,} race_id 処理済 (updated={updated:,} / {dur:.1f}s)")

    conn.commit()
    dur = time.time() - t0
    print(f"\n[完了 {dur:.1f}s]")
    print(f"  updated: {updated:,}")
    print(f"  no_match (race_results に対応 horse_no なし): {no_match:,}")
    print(f"  no_results (race_results 不在 or parse 失敗): {no_results:,}")

    # 残存 finish_time=0 件数
    n = conn.execute("SELECT COUNT(*) FROM race_log WHERE finish_time_sec=0").fetchone()[0]
    print(f"  残存 finish_time=0 件数: {n:,}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
