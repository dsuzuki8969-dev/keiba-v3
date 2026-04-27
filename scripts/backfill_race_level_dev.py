"""
race_log.race_level_dev バックフィルスクリプト

レースごとに「1着馬の走破タイム vs 基準タイム」で算出した
レースレベル偏差値（race_level_dev）を race_log に永続化する。

- race_id でグルーピングし、1着馬の finish_time_sec を winner_t とする
- StandardTimeCalculator.calc_standard_time + calc_run_deviation を流用
- 同一 race_id の全馬に同じ race_level_dev を書き込む
- 失敗レースは race_level_dev=NULL のまま残す（後日再実行で拾える）

Usage:
    python scripts/backfill_race_level_dev.py                    # 全レースバックフィル
    python scripts/backfill_race_level_dev.py --dry-run          # 計算のみ、UPDATE しない
    python scripts/backfill_race_level_dev.py --limit 10         # 先頭 10 レースのみ
    python scripts/backfill_race_level_dev.py --since 2026-04-01 # 指定日以降のみ
    python scripts/backfill_race_level_dev.py --force            # race_level_dev IS NOT NULL も再計算

scheduler_tasks 等から呼ぶ場合は run_backfill() 関数を直接インポートする。
"""

from __future__ import annotations

import argparse
import itertools
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

# プロジェクトルートを sys.path に追加
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.calculator.ability import (
    StandardTimeCalculator,
    calc_run_deviation,
)
from src.database import get_db, init_schema
from src.log import get_logger
from src.models import PastRun

logger = get_logger(__name__)
console = Console()
P = console.print

# バッチコミット単位（race_id 数）
COMMIT_BATCH = 1000
# course_db 構築時の上限（上位走者 N 件）
TOP_N_PER_COURSE = 100


def _row_to_minimal_pastrun(row) -> PastRun:
    """race_log 行から StandardTimeCalculator に必要な最小限の PastRun を構築"""
    return PastRun(
        race_date=row["race_date"] or "",
        venue=row["venue_code"] or "",
        course_id=row["course_id"] or "",
        distance=row["distance"] or 0,
        surface=row["surface"] or "",
        condition=row["condition"] or "良",
        class_name=row["race_name"] or "",
        grade=row["grade"] or "",
        field_count=row["field_count"] or 0,
        gate_no=row["gate_no"] or 0,
        horse_no=row["horse_no"] or 0,
        jockey=row["jockey_name"] or "",
        weight_kg=row["weight_kg"] or 55.0,
        position_4c=row["position_4c"] or 0,
        finish_pos=row["finish_pos"] or 99,
        finish_time_sec=row["finish_time_sec"] or 0.0,
        last_3f_sec=row["last_3f_sec"] or 0.0,
        margin_behind=row["margin_behind"] or 0.0,
        margin_ahead=row["margin_ahead"] or 0.0,
    )


def build_course_db() -> Dict[str, List[PastRun]]:
    """全 race_log から各 course_id の top-3 finishers を抽出して course_db を構築"""
    conn = get_db()
    total_n = conn.execute(
        """SELECT COUNT(*) AS n FROM race_log
           WHERE finish_time_sec > 0 AND distance > 0
             AND course_id != '' AND finish_pos <= 3"""
    ).fetchone()["n"]
    P(f"  course_db ソース行: {total_n:,} 行（top-3 finishers）")

    cur = conn.execute(
        """SELECT course_id, race_date, venue_code, surface, distance,
                  field_count, gate_no, horse_no, jockey_name, weight_kg,
                  position_4c, finish_pos, finish_time_sec, last_3f_sec,
                  margin_behind, margin_ahead, condition, race_name, grade
           FROM race_log
           WHERE finish_time_sec > 0 AND distance > 0
             AND course_id != '' AND finish_pos <= 3
           ORDER BY race_date DESC"""
    )

    course_db: Dict[str, List[PastRun]] = defaultdict(list)
    n = 0
    t0 = time.time()
    for row in cur:
        cid = row["course_id"]
        if not cid or len(course_db[cid]) >= TOP_N_PER_COURSE:
            continue
        course_db[cid].append(_row_to_minimal_pastrun(row))
        n += 1
        if n % 50000 == 0:
            P(f"  course_db: {n:,} 行処理 ({time.time() - t0:.1f}s, {len(course_db):,} コース)")
    P(f"  [green]course_db 構築完了: {len(course_db):,} コース / 取込 {n:,} 走 ({time.time() - t0:.1f}s)[/green]")
    return dict(course_db)


def _resolve_course_id(row) -> str:
    """
    course_id='' の行に対して venue_code + surface + distance から course_id を逆算する。
    復元成功時はその course_id 文字列を返す。失敗時は空文字列を返す。
    """
    vc = row["venue_code"] or ""
    sf = row["surface"] or ""
    dist = row["distance"] or 0
    if vc and sf and dist:
        return f"{vc}_{sf}_{dist}"
    return ""


def run_backfill(
    dry_run: bool = False,
    limit: Optional[int] = None,
    since: Optional[str] = None,
    force: bool = False,
    show_progress: bool = True,
) -> dict:
    """レースレベル偏差値バックフィルのコア関数。
    scheduler_tasks 等から直接呼び出せるよう CLI から分離。

    計算単位: race_id（1着馬タイム → 全馬共通の race_level_dev）

    Returns:
        {
            "calc_races": int,   # 計算成功レース数
            "skip_races": int,   # スキップレース数
            "updated_rows": int, # UPDATE 実行行数
            "sample_devs": List[float],  # サンプル偏差値（先頭 20 件）
        }
    """
    P(f"[bold cyan]race_log.race_level_dev バックフィル開始[/bold cyan]")
    P(f"  dry_run={dry_run}, limit={limit}, since={since}, force={force}")

    init_schema()
    conn = get_db()

    # course_db 構築（基準タイム計算用）
    course_db = build_course_db()
    if not course_db:
        P("[red]course_db が空です。race_log にデータがないか確認してください[/red]")
        return {"calc_races": 0, "skip_races": 0, "updated_rows": 0, "sample_devs": []}

    std_calc = StandardTimeCalculator(course_db)

    # 対象行抽出（race_id ごとにグルーピングするため race_id でソート）
    where_clauses = [
        "finish_time_sec > 0",
        "distance > 0",
    ]
    params: list = []

    if not force:
        # 未計算の race_id に絞る（race_id 内で 1 行でも NULL があれば対象）
        where_clauses.append(
            "race_id IN (SELECT DISTINCT race_id FROM race_log WHERE race_level_dev IS NULL)"
        )
    if since:
        where_clauses.append("race_date >= ?")
        params.append(since)

    sql = f"""SELECT race_id, race_date, venue_code, surface, distance, course_id,
                     finish_time_sec, condition, grade, race_name, field_count,
                     finish_pos, gate_no, horse_no, jockey_name, weight_kg,
                     position_4c, last_3f_sec, margin_behind, margin_ahead
              FROM race_log
              WHERE {' AND '.join(where_clauses)}
              ORDER BY race_id, finish_pos"""

    rows = conn.execute(sql, params).fetchall()
    total_rows = len(rows)
    P(f"  対象行: {total_rows:,} 行")

    if total_rows == 0:
        P("[yellow]対象行なし。終了します[/yellow]")
        return {"calc_races": 0, "skip_races": 0, "updated_rows": 0, "sample_devs": []}

    # race_id でグルーピング
    race_groups: Dict[str, list] = {}
    for row in rows:
        rid = row["race_id"]
        if rid not in race_groups:
            race_groups[rid] = []
        race_groups[rid].append(row)

    all_race_ids = list(race_groups.keys())
    total_races = len(all_race_ids)

    # --limit は race 数で適用
    if limit:
        all_race_ids = all_race_ids[:limit]
    P(f"  対象レース数: {total_races:,} レース（limit 適用後: {len(all_race_ids):,}）")

    n_calc = 0   # 計算成功レース数
    n_skip = 0   # スキップレース数
    n_committed = 0  # UPDATE 行数
    update_batch: List[tuple] = []  # (race_level_dev, race_id)
    sample_devs: List[float] = []

    def _process_race(race_id: str, on_progress=None):
        nonlocal n_calc, n_skip, n_committed, update_batch

        race_rows = race_groups[race_id]

        # 1着馬を探す（finish_pos == 1）。なければ先頭行で代用
        winner_row = next(
            (r for r in race_rows if r["finish_pos"] == 1),
            race_rows[0],
        )

        # 1着馬の finish_time_sec から margin_ahead を引いて勝ち馬タイムを算出
        margin = winner_row["margin_ahead"] if winner_row["margin_ahead"] is not None else 0.0
        margin = max(0.0, min(float(margin), 10.0))
        winner_t = float(winner_row["finish_time_sec"] or 0.0) - margin
        if winner_t <= 0:
            n_skip += 1
            if on_progress:
                on_progress()
            return

        # course_id を取得（空の場合は venue+surface+dist から逆算）
        cid = winner_row["course_id"] or ""
        if not cid:
            cid = _resolve_course_id(winner_row)
        if not cid:
            n_skip += 1
            if on_progress:
                on_progress()
            return

        # 基準タイム取得
        std_time, _ = std_calc.calc_standard_time(
            cid,
            winner_row["grade"] or "",
            winner_row["condition"] or "良",
            winner_row["distance"] or 0,
        )
        if std_time is None:
            n_skip += 1
            if on_progress:
                on_progress()
            return

        # race_level_dev 計算
        try:
            lvl = calc_run_deviation(
                winner_t, std_time, winner_row["distance"] or 0,
                venue_code=winner_row["venue_code"] or "",
            )
        except Exception as exc:
            logger.debug("race_level_dev 計算失敗 race_id=%s: %s", race_id, exc)
            n_skip += 1
            if on_progress:
                on_progress()
            return

        # クランプ（異常値を弾く）
        lvl = max(-50.0, min(100.0, float(lvl)))

        if not dry_run:
            update_batch.append((lvl, race_id))

        n_calc += 1
        if len(sample_devs) < 20:
            sample_devs.append(lvl)

        # バッチ commit
        if not dry_run and len(update_batch) >= COMMIT_BATCH:
            conn.executemany(
                "UPDATE race_log SET race_level_dev = ? WHERE race_id = ?",
                update_batch,
            )
            conn.commit()
            n_committed += len(update_batch)
            update_batch.clear()

        if on_progress:
            on_progress()

    # プログレスバー付き実行
    if show_progress:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed:,}/{task.total:,}"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[green]race_level_dev 計算中...", total=len(all_race_ids)
            )
            for race_id in all_race_ids:
                _process_race(race_id, on_progress=lambda: progress.update(task, advance=1))
    else:
        for race_id in all_race_ids:
            _process_race(race_id)

    # 残バッチ commit
    if not dry_run and update_batch:
        conn.executemany(
            "UPDATE race_log SET race_level_dev = ? WHERE race_id = ?",
            update_batch,
        )
        conn.commit()
        n_committed += len(update_batch)
        update_batch.clear()

    # 結果サマリ表示
    P(f"\n[bold green]完了[/bold green]")
    P(f"  計算成功: {n_calc:,} レース")
    P(f"  スキップ: {n_skip:,} レース (基準タイム取得不可など)")
    if not dry_run:
        # UPDATE は race_id 単位なので更新行数を算出
        P(f"  UPDATE  : race_id {n_committed:,} 件（各 race_id の全馬行を更新）")
    if sample_devs:
        P(
            f"  サンプル偏差値: min={min(sample_devs):.1f}, max={max(sample_devs):.1f}, "
            f"avg={sum(sample_devs) / len(sample_devs):.1f}"
        )

    return {
        "calc_races": n_calc,
        "skip_races": n_skip,
        "updated_rows": n_committed,
        "sample_devs": sample_devs,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="race_log.race_level_dev バックフィル")
    ap.add_argument("--dry-run", action="store_true", help="UPDATE せず計算のみ")
    ap.add_argument("--limit", type=int, default=None, help="先頭 N レースのみ処理")
    ap.add_argument("--since", type=str, default=None, help="指定日以降のみ (YYYY-MM-DD)")
    ap.add_argument("--force", action="store_true", help="race_level_dev IS NOT NULL も再計算")
    args = ap.parse_args()

    run_backfill(
        dry_run=args.dry_run,
        limit=args.limit,
        since=args.since,
        force=args.force,
        show_progress=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
