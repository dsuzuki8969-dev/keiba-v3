"""
Plan-γ Phase 1: race_log.relative_dev バックフィルスクリプト

同 race_id 内の run_dev を z-score 正規化した値 (relative_dev) を全期間バックフィルする。
帯広ばんえい (venue_code=65) は順位ベースの計算にフォールバック。

使用方法:
    python scripts/backfill_relative_dev.py --dry-run
    python scripts/backfill_relative_dev.py
    python scripts/backfill_relative_dev.py --date-from 2025-01-01 --date-to 2025-12-31
    python scripts/backfill_relative_dev.py --force
"""

import argparse
import math
import shutil
import sqlite3
import statistics
import sys
from datetime import datetime
from pathlib import Path

# プロジェクトルートを PYTHONPATH に追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    DATABASE_PATH,
    BANEI_VENUE_CODE,
    RELATIVE_DEV_MIN_FIELD,
    RELATIVE_DEV_SIGMA_FLOOR,
    RELATIVE_DEV_Z_CLAMP,
)
from src.log import get_logger

logger = get_logger(__name__)

try:
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )
    from rich.console import Console
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False


# ============================================================
# 計算ロジック（仕様書通り）
# ============================================================

def calc_relative_dev(
    values: list[float],
    target: float,
    sigma_floor: float = RELATIVE_DEV_SIGMA_FLOOR,
    z_clamp: float = RELATIVE_DEV_Z_CLAMP,
) -> float:
    """
    同レース内の run_dev 群を z-score 正規化して relative_dev を算出する。

    Args:
        values:     同 race_id 内の全馬の run_dev リスト
        target:     対象馬の run_dev
        sigma_floor: σ の下限（小レース等での安定化）
        z_clamp:    ±z_clamp でクランプ（デフォルト ±3.0σ）

    Returns:
        相対偏差値 (20.0 〜 80.0 の範囲)
    """
    if len(values) < 2:
        return 50.0
    mu = statistics.mean(values)
    sigma = max(statistics.stdev(values), sigma_floor)
    z = (target - mu) / sigma
    z = max(-z_clamp, min(z_clamp, z))
    return 50.0 + 10.0 * z


def calc_rank_based_dev(rank: int, n: int) -> float:
    """
    帯広ばんえい専用: 順位ベースの relative_dev 算出。

    一様分布を仮定し N(50, 10) 近似で偏差値化する。
    Args:
        rank: 着順（1始まり）。n を超える値（99=失格等）は 50.0 固定。
        n:    出走頭数

    Returns:
        順位ベース偏差値 (概ね 17〜83 の範囲)
    """
    # rank > n は失格・取消等の特殊コード（例: 99）→ 中央値 50.0 固定
    if rank < 1 or rank > n:
        return 50.0
    # (n - rank + 0.5) / n → [0, 1] を 0.5 中心に正規化 → x ∈ [-0.5, +0.5]
    x = (n - rank + 0.5) / n - 0.5
    # 一様分布の σ = 1/√12, スケールして σ≈1 にする → ×√12
    return 50.0 + 10.0 * x * math.sqrt(12.0)


# ============================================================
# バックフィル本体
# ============================================================

def run_backfill(
    conn: sqlite3.Connection,
    dry_run: bool = False,
    date_from: str = None,
    date_to: str = None,
    force: bool = False,
) -> dict:
    """
    relative_dev を全 race_id に対してバックフィルする。

    Args:
        conn:       DB 接続（WAL モード）
        dry_run:    True の場合 DB 更新せず計算結果サンプル 10 件を出力
        date_from:  開始日 (YYYY-MM-DD)。省略時は全期間
        date_to:    終了日 (YYYY-MM-DD)。省略時は全期間
        force:      True の場合、既存の relative_dev も上書き

    Returns:
        集計情報の辞書
    """
    # ----------------------------------------------------------
    # 対象 race_id を取得（期間・force フィルタ）
    # ----------------------------------------------------------
    where_clauses = []
    params: list = []

    if date_from:
        where_clauses.append("race_date >= ?")
        params.append(date_from)
    if date_to:
        where_clauses.append("race_date <= ?")
        params.append(date_to)
    if not force:
        where_clauses.append("relative_dev IS NULL")

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # 対象レース一覧（バッチ単位で処理するため race_id を取得）
    race_query = f"""
        SELECT DISTINCT race_id, race_date, venue_code, field_count
        FROM race_log
        {where_sql}
        ORDER BY race_date, race_id
    """
    races = conn.execute(race_query, params).fetchall()

    # force=False かつ期間指定なしの場合、relative_dev IS NULL な race_id に絞れる
    # → すでに全件 UPDATE 済みならゼロ件になる
    logger.info("対象レース数: %d (dry_run=%s, force=%s)", len(races), dry_run, force)

    stats = {
        "races_total": len(races),
        "races_processed": 0,
        "rows_updated": 0,
        "rows_skipped_field_count": 0,
        "rows_skipped_run_dev_null": 0,
        "rows_error": 0,
        "banei_count": 0,
        "sample_rows": [],  # dry-run 用サンプル
    }

    if len(races) == 0:
        logger.info("対象レースが 0 件のため処理をスキップします")
        return stats

    # ----------------------------------------------------------
    # Progress バー設定
    # ----------------------------------------------------------
    if RICH_AVAILABLE and not dry_run:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        )
        task = progress.add_task("relative_dev バックフィル中...", total=len(races))
        progress.start()
    else:
        progress = None
        task = None

    try:
        batch_updates: list[tuple[float, str, int]] = []  # (relative_dev, race_id, horse_no)
        BATCH_SIZE = 500  # 一括コミットサイズ

        for race_row in races:
            race_id = race_row[0]
            race_date = race_row[1]
            venue_code = race_row[2]
            field_count = race_row[3] or 0

            # field_count < MIN のレースはスキップ（relative_dev = NULL のまま）
            if field_count < RELATIVE_DEV_MIN_FIELD:
                # NULL のままにする（行自体はスキップ）
                # 各行をスキップ件数に加算するため行取得も必要
                skipped_cnt = conn.execute(
                    "SELECT COUNT(*) FROM race_log WHERE race_id=?", (race_id,)
                ).fetchone()[0]
                stats["rows_skipped_field_count"] += skipped_cnt
                if progress:
                    progress.advance(task)
                continue

            # 同 race_id 内の全馬データを取得
            rows = conn.execute(
                """
                SELECT horse_no, finish_pos, run_dev, relative_dev
                FROM race_log
                WHERE race_id = ?
                ORDER BY horse_no
                """,
                (race_id,),
            ).fetchall()

            is_banei = (venue_code == BANEI_VENUE_CODE)
            if is_banei:
                stats["banei_count"] += 1

            # run_dev が有効な行のみを収集
            valid_run_devs = [r[2] for r in rows if r[2] is not None]

            for r in rows:
                horse_no = r[0]
                finish_pos = r[1]
                run_dev = r[2]
                current_rel_dev = r[3]

                # force=False の場合、既に値がある行はスキップ
                if not force and current_rel_dev is not None:
                    continue

                # run_dev = NULL の馬はスキップ（relative_dev = NULL のまま）
                if run_dev is None:
                    stats["rows_skipped_run_dev_null"] += 1
                    continue

                try:
                    if is_banei:
                        # 帯広ばんえい: 順位ベースにフォールバック
                        rel_dev = calc_rank_based_dev(finish_pos, field_count)
                    else:
                        # 通常: z-score 正規化
                        rel_dev = calc_relative_dev(valid_run_devs, run_dev)

                    if dry_run:
                        stats["sample_rows"].append({
                            "race_id": race_id,
                            "race_date": race_date,
                            "venue_code": venue_code,
                            "horse_no": horse_no,
                            "run_dev": run_dev,
                            "relative_dev": round(rel_dev, 2),
                            "is_banei": is_banei,
                        })
                    else:
                        batch_updates.append((round(rel_dev, 4), race_id, horse_no))

                    stats["rows_updated"] += 1

                except Exception as e:
                    logger.debug(
                        "計算エラー race_id=%s horse_no=%s: %s",
                        race_id, horse_no, e
                    )
                    stats["rows_error"] += 1

            # バッチコミット
            if not dry_run and len(batch_updates) >= BATCH_SIZE:
                conn.executemany(
                    "UPDATE race_log SET relative_dev=? WHERE race_id=? AND horse_no=?",
                    batch_updates,
                )
                conn.commit()
                batch_updates.clear()

            stats["races_processed"] += 1
            if progress:
                progress.advance(task)

        # 残余バッチのコミット
        if not dry_run and batch_updates:
            conn.executemany(
                "UPDATE race_log SET relative_dev=? WHERE race_id=? AND horse_no=?",
                batch_updates,
            )
            conn.commit()
            batch_updates.clear()

    finally:
        if progress:
            progress.stop()

    return stats


# ============================================================
# 分布レポート出力
# ============================================================

def print_distribution_report(conn: sqlite3.Connection) -> None:
    """
    全 venue / 帯広(65) / 帯広以外 / 各 venue の relative_dev 分布を表示する。
    """
    if RICH_AVAILABLE:
        console.print("\n[bold cyan]===== relative_dev 分布レポート =====[/bold cyan]")
    else:
        print("\n===== relative_dev 分布レポート =====")

    # SQLite に組み込みの STDEV はないため Python 側で計算
    # まず venue 別に集計
    rows = conn.execute("""
        SELECT
            venue_code,
            COUNT(*) AS cnt,
            SUM(CASE WHEN relative_dev IS NOT NULL THEN 1 ELSE 0 END) AS not_null_cnt,
            SUM(CASE WHEN relative_dev IS NULL THEN 1 ELSE 0 END) AS null_cnt,
            MIN(relative_dev) AS min_v,
            MAX(relative_dev) AS max_v,
            AVG(relative_dev) AS avg_v
        FROM race_log
        GROUP BY venue_code
        ORDER BY cnt DESC
    """).fetchall()

    # 全体
    total_row = conn.execute("""
        SELECT
            COUNT(*) AS cnt,
            SUM(CASE WHEN relative_dev IS NOT NULL THEN 1 ELSE 0 END) AS not_null_cnt,
            SUM(CASE WHEN relative_dev IS NULL THEN 1 ELSE 0 END) AS null_cnt,
            MIN(relative_dev) AS min_v,
            MAX(relative_dev) AS max_v,
            AVG(relative_dev) AS avg_v
        FROM race_log
    """).fetchone()

    banei_row = conn.execute("""
        SELECT
            COUNT(*) AS cnt,
            SUM(CASE WHEN relative_dev IS NOT NULL THEN 1 ELSE 0 END) AS not_null_cnt,
            MIN(relative_dev) AS min_v,
            MAX(relative_dev) AS max_v,
            AVG(relative_dev) AS avg_v
        FROM race_log
        WHERE venue_code = ?
    """, (BANEI_VENUE_CODE,)).fetchone()

    non_banei_row = conn.execute("""
        SELECT
            COUNT(*) AS cnt,
            SUM(CASE WHEN relative_dev IS NOT NULL THEN 1 ELSE 0 END) AS not_null_cnt,
            MIN(relative_dev) AS min_v,
            MAX(relative_dev) AS max_v,
            AVG(relative_dev) AS avg_v
        FROM race_log
        WHERE venue_code != ?
    """, (BANEI_VENUE_CODE,)).fetchone()

    # σ（stdev）は別途計算（SQLite 非対応のため）
    def calc_stdev_from_db(where_clause: str = "", params: list = []) -> float:
        vals = conn.execute(
            f"SELECT relative_dev FROM race_log WHERE relative_dev IS NOT NULL {where_clause}",
            params,
        ).fetchall()
        if len(vals) < 2:
            return 0.0
        flat = [v[0] for v in vals]
        return statistics.stdev(flat)

    total_stdev = calc_stdev_from_db()
    banei_stdev = calc_stdev_from_db("AND venue_code=?", [BANEI_VENUE_CODE])
    non_banei_stdev = calc_stdev_from_db("AND venue_code!=?", [BANEI_VENUE_CODE])

    header = f"{'区分':<12} {'総件数':>8} {'有値':>8} {'NULL':>8} {'avg':>7} {'σ':>6} {'min':>7} {'max':>7}"
    print(header)
    print("-" * len(header))

    def fmt_row(label, cnt, not_null, null, avg, stdev, min_v, max_v):
        avg_s = f"{avg:.1f}" if avg is not None else "-"
        stdev_s = f"{stdev:.1f}"
        min_s = f"{min_v:.1f}" if min_v is not None else "-"
        max_s = f"{max_v:.1f}" if max_v is not None else "-"
        print(f"{label:<12} {cnt:>8,} {not_null:>8,} {null:>8,} {avg_s:>7} {stdev_s:>6} {min_s:>7} {max_s:>7}")

    fmt_row(
        "全体",
        total_row[0], total_row[1], total_row[2],
        total_row[5], total_stdev, total_row[3], total_row[4]
    )
    fmt_row(
        "帯広(65)",
        banei_row[0], banei_row[1], banei_row[0] - banei_row[1],
        banei_row[4], banei_stdev, banei_row[2], banei_row[3]
    )
    fmt_row(
        "帯広以外",
        non_banei_row[0], non_banei_row[1], non_banei_row[0] - non_banei_row[1],
        non_banei_row[4], non_banei_stdev, non_banei_row[2], non_banei_row[3]
    )

    print()
    print(f"{'venue':>8} {'cnt':>8} {'not_null':>9} {'null':>7} {'avg':>7} {'min':>7} {'max':>7}")
    print("-" * 65)
    for r in rows:
        venue_code = r[0] or "?"
        cnt = r[1]
        not_null = r[2]
        null = r[3]
        avg_v = r[6]
        min_v = r[4]
        max_v = r[5]
        avg_s = f"{avg_v:.1f}" if avg_v is not None else "-"
        min_s = f"{min_v:.1f}" if min_v is not None else "-"
        max_s = f"{max_v:.1f}" if max_v is not None else "-"
        print(f"{venue_code:>8} {cnt:>8,} {not_null:>9,} {null:>7,} {avg_s:>7} {min_s:>7} {max_s:>7}")


# ============================================================
# 張り付き確認レポート
# ============================================================

def print_clamp_check(conn: sqlite3.Connection) -> None:
    """
    venue=65 の relative_dev 張り付き状態を確認する。
    """
    if RICH_AVAILABLE:
        console.print("\n[bold yellow]===== 張り付き解消確認 =====[/bold yellow]")
    else:
        print("\n===== 張り付き解消確認 =====")

    # 旧 run_dev 張り付き（確認のみ）
    old_top = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE venue_code=? AND run_dev >= 99.9",
        (BANEI_VENUE_CODE,),
    ).fetchone()[0]
    old_bot = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE venue_code=? AND run_dev <= -49.9",
        (BANEI_VENUE_CODE,),
    ).fetchone()[0]

    # 新 relative_dev 張り付き（帯広: 順位ベースなので理論上ゼロ）
    new_top = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE venue_code=? AND relative_dev >= 79.9",
        (BANEI_VENUE_CODE,),
    ).fetchone()[0]
    new_bot = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE venue_code=? AND relative_dev <= 20.1",
        (BANEI_VENUE_CODE,),
    ).fetchone()[0]
    new_null = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE venue_code=? AND relative_dev IS NULL",
        (BANEI_VENUE_CODE,),
    ).fetchone()[0]

    print(f"帯広(65) run_dev >=100 張り付き (旧): {old_top:,} 件")
    print(f"帯広(65) run_dev <=-50 張り付き (旧): {old_bot:,} 件")
    print(f"帯広(65) relative_dev >= 80 件 (新):  {new_top:,} 件  ← 0 が理想")
    print(f"帯広(65) relative_dev <= 20 件 (新):  {new_bot:,} 件  ← 0 が理想")
    print(f"帯広(65) relative_dev NULL 件 (新):   {new_null:,} 件  (field_count<5 or run_dev=NULL)")

    # 帯広以外の張り付き
    other_top = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE venue_code!=? AND relative_dev >= 79.9",
        (BANEI_VENUE_CODE,),
    ).fetchone()[0]
    other_bot = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE venue_code!=? AND relative_dev <= 20.1",
        (BANEI_VENUE_CODE,),
    ).fetchone()[0]
    print(f"\n帯広以外 relative_dev >= 80 件:       {other_top:,} 件  ← 外れ値のみ許容")
    print(f"帯広以外 relative_dev <= 20 件:       {other_bot:,} 件  ← 外れ値のみ許容")


# ============================================================
# バックアップ
# ============================================================

def create_backup(db_path: str) -> str:
    """
    DB ファイルのバックアップを作成する。
    Returns: バックアップファイルパス
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak_path = str(Path(db_path).parent / f"keiba.db.bak_relative_dev_{ts}")
    try:
        shutil.copy2(db_path, bak_path)
        logger.info("DB バックアップ作成: %s", bak_path)
        print(f"[バックアップ] {bak_path}")
    except Exception as e:
        logger.error("バックアップ失敗: %s", e)
        raise
    return bak_path


# ============================================================
# メイン
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="race_log.relative_dev を z-score 正規化でバックフィルする"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB 更新せず計算結果サンプル 10 件を出力",
    )
    parser.add_argument(
        "--date-from",
        metavar="YYYY-MM-DD",
        help="処理開始日（省略時は全期間）",
    )
    parser.add_argument(
        "--date-to",
        metavar="YYYY-MM-DD",
        help="処理終了日（省略時は全期間）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="既存の relative_dev も上書き（省略時は NULL のみ更新）",
    )
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  Plan-γ Phase 1: relative_dev バックフィル")
    print(f"  DB: {DATABASE_PATH}")
    print(f"  dry_run={args.dry_run}, force={args.force}")
    if args.date_from:
        print(f"  date_from={args.date_from}")
    if args.date_to:
        print(f"  date_to={args.date_to}")
    print(f"{'='*60}\n")

    # DB 接続（WAL モード）
    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-65536")  # 64MB キャッシュ

    # relative_dev カラムの存在確認（なければ migration）
    cols = [r[1] for r in conn.execute("PRAGMA table_info(race_log)").fetchall()]
    if "relative_dev" not in cols:
        print("[マイグレーション] relative_dev カラムを追加します...")
        conn.execute("ALTER TABLE race_log ADD COLUMN relative_dev REAL")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_racelog_relative_dev ON race_log(relative_dev)"
        )
        conn.commit()
        print("[マイグレーション] 完了")
    else:
        print("[マイグレーション] relative_dev カラムは既に存在します（スキップ）")

    # バックアップ（dry-run 時は不要）
    if not args.dry_run:
        create_backup(DATABASE_PATH)

    # バックフィル実行
    start_dt = datetime.now()
    stats = run_backfill(
        conn=conn,
        dry_run=args.dry_run,
        date_from=args.date_from,
        date_to=args.date_to,
        force=args.force,
    )
    elapsed = (datetime.now() - start_dt).total_seconds()

    # ----------------------------------------------------------
    # 結果サマリ出力
    # ----------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  バックフィル完了 ({elapsed:.1f}秒)")
    print(f"  対象レース数:             {stats['races_total']:>8,}")
    print(f"  処理レース数:             {stats['races_processed']:>8,}")
    print(f"  UPDATE 行数:              {stats['rows_updated']:>8,}")
    print(f"  スキップ (field_count<{RELATIVE_DEV_MIN_FIELD}): {stats['rows_skipped_field_count']:>8,}")
    print(f"  スキップ (run_dev=NULL):  {stats['rows_skipped_run_dev_null']:>8,}")
    print(f"  帯広レース数:             {stats['banei_count']:>8,}")
    print(f"  計算エラー数:             {stats['rows_error']:>8,}")

    if args.dry_run:
        print(f"\n[dry-run] サンプル {min(10, len(stats['sample_rows']))} 件:")
        print(
            f"  {'race_id':<30} {'venue':>6} {'horse':>5} "
            f"{'run_dev':>8} {'rel_dev':>8} {'banei':>6}"
        )
        for s in stats["sample_rows"][:10]:
            print(
                f"  {s['race_id']:<30} {s['venue_code']:>6} {s['horse_no']:>5} "
                f"{s['run_dev']:>8.2f} {s['relative_dev']:>8.2f} {str(s['is_banei']):>6}"
            )
    else:
        # NULL 残数確認
        null_count = conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE relative_dev IS NULL"
        ).fetchone()[0]
        print(f"  relative_dev NULL 残数: {null_count:>8,}")

        # 分布レポート
        print_distribution_report(conn)
        print_clamp_check(conn)

    print(f"\n{'='*60}\n")
    conn.close()


if __name__ == "__main__":
    main()
