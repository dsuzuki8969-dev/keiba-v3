#!/usr/bin/env python
"""race_logのmargin_ahead/margin_behindをfinish_time_secから計算してバックフィル

2着以降でmargin_ahead=0のレコードに対し、同レースの1着馬との秒差を計算して投入する。
"""
import sqlite3
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn

console = Console()


def main():
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "keiba.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    console.print("[bold cyan]race_log margin_ahead/behind バックフィル[/]")

    # margin_ahead=0 かつ 2着以降のレコードのrace_idを取得
    target_races = conn.execute("""
        SELECT DISTINCT race_id FROM race_log
        WHERE finish_pos > 1 AND finish_pos < 90
          AND (margin_ahead IS NULL OR margin_ahead = 0)
          AND finish_time_sec > 0
    """).fetchall()
    race_ids = [r[0] for r in target_races]

    console.print(f"対象レース: {len(race_ids)}件")
    if not race_ids:
        console.print("[green]バックフィル対象なし[/]")
        return

    updated = 0
    skipped = 0
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
        task = progress.add_task("margin計算中...", total=len(race_ids))

        batch_ahead = []
        batch_behind = []

        for race_id in race_ids:
            progress.advance(task)

            # 同レースの全馬のfinish_time_secを取得
            horses = conn.execute(
                "SELECT horse_no, finish_pos, finish_time_sec "
                "FROM race_log WHERE race_id=? AND finish_pos < 90 "
                "ORDER BY finish_pos",
                (race_id,),
            ).fetchall()

            if len(horses) < 2:
                skipped += 1
                continue

            # 1着のfinish_time_sec
            winner_time = None
            times_by_pos = {}
            for hno, fpos, ft in horses:
                if ft and ft > 0:
                    times_by_pos[fpos] = (hno, ft)
                    if fpos == 1:
                        winner_time = ft

            if not winner_time:
                skipped += 1
                continue

            # margin_ahead = 自馬time - 1着time（秒差）
            # margin_behind = 次着time - 自馬time
            sorted_positions = sorted(times_by_pos.keys())
            for i, fpos in enumerate(sorted_positions):
                hno, ft = times_by_pos[fpos]
                if fpos == 1:
                    # 1着のmargin_aheadは0
                    continue

                ma = round(ft - winner_time, 2)
                if ma < 0:
                    ma = 0.0

                # margin_behind: 次着との差
                mb = 0.0
                if i + 1 < len(sorted_positions):
                    next_pos = sorted_positions[i + 1]
                    _, next_ft = times_by_pos[next_pos]
                    mb = round(next_ft - ft, 2)
                    if mb < 0:
                        mb = 0.0

                batch_ahead.append((ma, mb, race_id, hno))
                updated += 1

        # 一括UPDATE
        if batch_ahead:
            conn.executemany(
                "UPDATE race_log SET margin_ahead=?, margin_behind=? "
                "WHERE race_id=? AND horse_no=? AND (margin_ahead IS NULL OR margin_ahead = 0)",
                batch_ahead,
            )
            conn.commit()

    elapsed = time.time() - t0
    console.print(f"\n[bold green]完了[/]: 更新={updated}, スキップ={skipped} ({elapsed:.1f}秒)")

    # 結果確認
    total = conn.execute("SELECT COUNT(*) FROM race_log WHERE finish_pos > 1 AND finish_pos < 90").fetchone()[0]
    filled = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos > 1 AND finish_pos < 90 AND margin_ahead > 0"
    ).fetchone()[0]
    console.print(f"margin_ahead充填率(2着以降): {filled}/{total} ({filled/total*100:.1f}%)")

    conn.close()


if __name__ == "__main__":
    main()
