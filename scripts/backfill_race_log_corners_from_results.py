#!/usr/bin/env python
"""race_resultsのorder_json cornersからrace_logのpositions_cornersをバックフィル

race_logにpositions_cornersが空だがrace_resultsのorder_jsonにcornersデータがある
レコードを一括で埋める。

対象: ~1,460件（ばんえい除外）
"""
import json
import sqlite3
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn

console = Console()


def _parse_corners_num(raw_val: int, field_count: int) -> list:
    """netkeibaの通過順数値(例: 3333→[3,3,3,3])をパース"""
    if not raw_val or raw_val == 0:
        return []
    s = str(raw_val)
    # 全桁1-9 かつ 2-4桁 → 各桁分解で確定
    if all(c in "123456789" for c in s) and 2 <= len(s) <= 4:
        return [int(c) for c in s]
    # 1桁+2桁混在: コーナー数4→3→2で試行
    for nc in (4, 3, 2):
        cands = _dp_corners(s, nc)
        if not cands:
            continue
        valid = [c for c in cands if all(1 <= v <= field_count for v in c)]
        if valid:
            return min(valid, key=lambda c: max(c) - min(c))
    return [int(c) for c in s if c != "0"]


def _dp_corners(s: str, n: int):
    """文字列sをn個の正整数に分割する全パターンを列挙"""
    if n == 0:
        return [[]] if not s else None
    if not s:
        return None
    res = []
    v1 = int(s[0])
    if v1 > 0:
        sub = _dp_corners(s[1:], n - 1)
        if sub:
            res.extend([[v1] + r for r in sub])
    if len(s) >= 2:
        v2 = int(s[:2])
        if v2 >= 10:
            sub = _dp_corners(s[2:], n - 1)
            if sub:
                res.extend([[v2] + r for r in sub])
    return res or None


def main():
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "keiba.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    console.print("[bold cyan]race_results → race_log 通過順バックフィル[/]")

    # 対象レコード取得（ばんえい=65除外）
    rows = conn.execute("""
        SELECT rl.race_id, rl.horse_no, rl.venue_code, rl.field_count, rr.order_json
        FROM race_log rl
        JOIN race_results rr ON rl.race_id = rr.race_id
        WHERE rl.finish_pos < 90
          AND (rl.positions_corners = '' OR rl.positions_corners IS NULL)
          AND rr.order_json IS NOT NULL
          AND rl.venue_code != '65'
    """).fetchall()

    console.print(f"対象レコード: {len(rows)}件")
    if not rows:
        console.print("[green]バックフィル対象なし[/]")
        return

    updated = 0
    skipped = 0
    errors = 0
    t0 = time.time()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("バックフィル中...", total=len(rows))

        batch = []
        for race_id, horse_no, venue_code, field_count, order_json in rows:
            progress.advance(task)
            try:
                orders = json.loads(order_json)
                fc = field_count or len(orders) or 18

                # 対象馬のcornersを探す
                target_corners = None
                for o in orders:
                    if o.get("horse_no") == horse_no:
                        raw = o.get("corners")
                        if raw and isinstance(raw, list) and len(raw) > 0:
                            # リスト形式: [3333] → パース
                            if isinstance(raw[0], int) and raw[0] > 0:
                                parsed = _parse_corners_num(raw[0], fc)
                                if parsed:
                                    target_corners = parsed
                        elif raw and isinstance(raw, int) and raw > 0:
                            parsed = _parse_corners_num(raw, fc)
                            if parsed:
                                target_corners = parsed
                        break

                if target_corners:
                    corners_json = json.dumps(target_corners)
                    p4c = target_corners[-1] if target_corners else 0
                    batch.append((corners_json, p4c, race_id, horse_no))
                    updated += 1
                else:
                    skipped += 1
            except Exception as e:
                errors += 1

        # 一括UPDATE
        if batch:
            conn.executemany(
                "UPDATE race_log SET positions_corners=?, position_4c=? "
                "WHERE race_id=? AND horse_no=?",
                batch,
            )
            conn.commit()

    elapsed = time.time() - t0
    console.print(f"\n[bold green]完了[/]: 更新={updated}, スキップ={skipped}, エラー={errors} ({elapsed:.1f}秒)")

    # 結果確認
    total = conn.execute("SELECT COUNT(*) FROM race_log WHERE finish_pos < 90").fetchone()[0]
    filled = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos < 90 "
        "AND positions_corners != '' AND positions_corners IS NOT NULL"
    ).fetchone()[0]
    console.print(f"充填率: {filled}/{total} ({filled/total*100:.1f}%)")

    conn.close()


if __name__ == "__main__":
    main()
