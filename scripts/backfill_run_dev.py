"""
race_log.run_dev バックフィルスクリプト

各 race_log 行に対して走破偏差値（run_dev）を計算して永続化する。
- 既存の StandardTimeCalculator.calc_standard_time + calc_run_deviation を流用
- 重量補正は省略（馬指数グラフ用途では誤差±0.5程度で許容）
- 失敗行は run_dev=NULL のまま残す（後日再実行時に拾える）

Usage:
    python scripts/backfill_run_dev.py                          # 全行バックフィル
    python scripts/backfill_run_dev.py --dry-run                # 計算のみ、UPDATE しない
    python scripts/backfill_run_dev.py --limit 1000             # 先頭1000行のみ
    python scripts/backfill_run_dev.py --horse-id XXX           # 特定馬のみ
    python scripts/backfill_run_dev.py --since 2025-01-01       # 指定日以降のみ
    python scripts/backfill_run_dev.py --force                  # run_dev IS NOT NULL の行も再計算
    python scripts/backfill_run_dev.py --fix-empty-course       # course_id='' 行も処理（venue_code+surface+distance から復元）
    python scripts/backfill_run_dev.py --fix-empty-course --dry-run --limit 100  # ドライラン

scheduler_tasks 等から呼ぶ場合は run_backfill() 関数を直接インポートする。
"""

from __future__ import annotations

import argparse
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

COMMIT_BATCH = 5000
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
    復元に成功した場合はその course_id 文字列を返す。失敗した場合は空文字列を返す。
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
    horse_id: Optional[str] = None,
    since: Optional[str] = None,
    force: bool = False,
    fix_empty_course: bool = False,
    show_progress: bool = True,
) -> dict:
    """走破偏差値バックフィルのコア関数。
    scheduler_tasks 等から直接呼び出せるよう CLI から分離。

    fix_empty_course=True の場合、course_id='' の行も処理対象に含める。
    venue_code + surface + distance から course_id を逆算して計算する。

    Returns: {"calc": int, "skip": int, "updated": int, "sample_devs": List[float]}
    """
    P(f"[bold cyan]race_log.run_dev バックフィル開始[/bold cyan]")
    P(f"  dry_run={dry_run}, limit={limit}, horse_id={horse_id}, since={since}, "
      f"force={force}, fix_empty_course={fix_empty_course}")

    init_schema()
    conn = get_db()

    course_db = build_course_db()
    if not course_db:
        P("[red]course_db が空です。race_log にデータがないか確認してください[/red]")
        return {"calc": 0, "skip": 0, "updated": 0, "sample_devs": []}

    std_calc = StandardTimeCalculator(course_db)

    # 対象行抽出
    # --fix-empty-course ありのときは course_id='' 行も対象に含める
    where_clauses = ["finish_time_sec > 0", "distance > 0"]
    if not fix_empty_course:
        # 既存挙動: course_id!='' の行のみ
        where_clauses.append("course_id != ''")
    params: list = []
    if not force:
        where_clauses.append("run_dev IS NULL")
    if horse_id:
        where_clauses.append("horse_id = ?")
        params.append(horse_id)
    if since:
        where_clauses.append("race_date >= ?")
        params.append(since)
    sql = f"""SELECT id, race_date, venue_code, surface, distance, course_id,
                     finish_time_sec, condition, grade, race_name, field_count,
                     finish_pos, gate_no, horse_no, jockey_name, weight_kg,
                     position_4c, last_3f_sec, margin_behind, margin_ahead
              FROM race_log
              WHERE {' AND '.join(where_clauses)}"""
    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    total = len(rows)
    P(f"  対象行: {total:,} 行")
    if fix_empty_course:
        empty_course_count = sum(1 for r in rows if not (r["course_id"] or "").strip())
        P(f"  うち course_id='' 行: {empty_course_count:,} 行（venue+surface+dist から復元試行）")

    if total == 0:
        P("[yellow]対象行なし。終了します[/yellow]")
        return {"calc": 0, "skip": 0, "updated": 0, "sample_devs": []}

    n_calc = 0
    n_skip = 0
    n_committed = 0
    n_restored = 0  # course_id を逆算復元できた行数
    updates: List[tuple] = []
    sample_devs: List[float] = []

    def _process(row, on_progress=None):
        nonlocal n_calc, n_skip, n_committed, n_restored, updates
        cid = row["course_id"] or ""
        # course_id='' の場合は venue_code+surface+distance から逆算復元
        if not cid and fix_empty_course:
            cid = _resolve_course_id(row)
            if cid:
                n_restored += 1
        if not cid:
            # 復元不可の場合はスキップ
            n_skip += 1
            if on_progress:
                on_progress()
            return
        std_time, _ = std_calc.calc_standard_time(
            cid, row["grade"] or "", row["condition"] or "良", row["distance"] or 0
        )
        if std_time is None:
            n_skip += 1
        else:
            try:
                # venue_code を渡して JRA/NAR 別 k 値を自動適用
                dev = calc_run_deviation(
                    row["finish_time_sec"], std_time, row["distance"],
                    venue_code=row["venue_code"] or "",
                )
            except Exception:
                n_skip += 1
                dev = None
            if dev is not None:
                if not dry_run:
                    updates.append((dev, row["id"]))
                n_calc += 1
                if len(sample_devs) < 20:
                    sample_devs.append(dev)
        if not dry_run and len(updates) >= COMMIT_BATCH:
            conn.executemany("UPDATE race_log SET run_dev = ? WHERE id = ?", updates)
            conn.commit()
            n_committed += len(updates)
            updates.clear()
        if on_progress:
            on_progress()

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
            task = progress.add_task("[green]run_dev 計算中...", total=total)
            for row in rows:
                _process(row, on_progress=lambda: progress.update(task, advance=1))
    else:
        for row in rows:
            _process(row)

    if not dry_run and updates:
        conn.executemany("UPDATE race_log SET run_dev = ? WHERE id = ?", updates)
        conn.commit()
        n_committed += len(updates)
        updates.clear()

    P(f"\n[bold green]完了[/bold green]")
    P(f"  計算成功: {n_calc:,} 行")
    P(f"  スキップ: {n_skip:,} 行 (基準タイム取得不可など)")
    if fix_empty_course:
        P(f"  course_id 復元: {n_restored:,} 行（venue+surface+dist から逆算）")
    if not dry_run:
        P(f"  UPDATE  : {n_committed:,} 行")
    if sample_devs:
        P(f"  サンプル偏差値: min={min(sample_devs):.1f}, max={max(sample_devs):.1f}, "
          f"avg={sum(sample_devs) / len(sample_devs):.1f}")

    return {
        "calc": n_calc,
        "skip": n_skip,
        "updated": n_committed,
        "restored": n_restored,
        "sample_devs": sample_devs,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="race_log.run_dev バックフィル")
    ap.add_argument("--dry-run", action="store_true", help="UPDATE せず計算のみ")
    ap.add_argument("--limit", type=int, default=None, help="先頭 N 行のみ処理")
    ap.add_argument("--horse-id", type=str, default=None, help="特定馬のみ処理")
    ap.add_argument("--since", type=str, default=None, help="指定日以降のみ (YYYY-MM-DD)")
    ap.add_argument("--force", action="store_true", help="run_dev IS NOT NULL も再計算")
    ap.add_argument(
        "--fix-empty-course",
        action="store_true",
        help="course_id='' の行も処理（venue_code+surface+distance から course_id を逆算）",
    )
    args = ap.parse_args()

    run_backfill(
        dry_run=args.dry_run,
        limit=args.limit,
        horse_id=args.horse_id,
        since=args.since,
        force=args.force,
        fix_empty_course=args.fix_empty_course,
        show_progress=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
