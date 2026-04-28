#!/usr/bin/env python3
"""
course_id='' 行の修正スクリプト

【背景】
race_log テーブルに course_id='' の行が 22,398 件発生している。
これらは venue_code, surface, distance が正しく入っているにもかかわらず
course_id だけが空のまま挿入された行（source='' の旧スクレイプ行）。

【修正方針】
  1. course_id = venue_code + '_' + surface + '_' + distance で直接 UPDATE
     （全 22,398 行は venue/surface/distance が揃っており復元可能）
  2. run_dev も NULL なので、course_id 復元後に backfill_run_dev を実行

【使い方】
  python scripts/refix_empty_course_id.py --dry-run --limit 100   # ドライラン（100件）
  python scripts/refix_empty_course_id.py                         # 本実行（全件）
  python scripts/refix_empty_course_id.py --skip-run-dev          # course_id のみ（run_dev は後で）
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.database import get_db, init_schema

# ログ設定
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
log_handler = logging.FileHandler(LOG_DIR / "refix_empty_course_id.log", encoding="utf-8")
log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger = logging.getLogger("refix_empty_course_id")
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)
logger.addHandler(logging.StreamHandler(sys.stdout))


def count_empty_course(conn) -> int:
    """course_id='' の件数を返す"""
    return conn.execute("SELECT COUNT(*) FROM race_log WHERE course_id = ''").fetchone()[0]


def refix_course_id(
    dry_run: bool = False,
    limit: int | None = None,
    skip_run_dev: bool = False,
) -> dict:
    """
    course_id='' 行を venue_code + '_' + surface + '_' + distance で埋める。

    Args:
        dry_run: True の場合、SELECT のみで UPDATE しない
        limit: 処理上限件数（テスト用）
        skip_run_dev: True の場合、run_dev の再計算をスキップする

    Returns:
        {"checked": int, "updated": int, "skipped": int, "elapsed": float}
    """
    init_schema()
    conn = get_db()

    t0 = time.time()

    # -----------------------------------------------------------
    # Step 1: course_id='' の件数確認
    # -----------------------------------------------------------
    before_count = count_empty_course(conn)
    logger.info(f"[Step 1] 修正前 course_id='' 件数: {before_count:,}")
    print(f"\n[Step 1] 修正前 course_id='' 件数: {before_count:,}")

    if before_count == 0:
        logger.info("course_id='' の行がありません。終了します")
        print("course_id='' の行がありません。終了します")
        return {"checked": 0, "updated": 0, "skipped": 0, "elapsed": 0.0}

    # -----------------------------------------------------------
    # Step 2: 復元可能性の確認
    # -----------------------------------------------------------
    restorable_count = conn.execute("""
        SELECT COUNT(*) FROM race_log
        WHERE course_id = ''
          AND venue_code != ''
          AND surface != ''
          AND distance > 0
    """).fetchone()[0]
    not_restorable = before_count - restorable_count
    logger.info(f"[Step 2] 復元可能: {restorable_count:,} / 不可: {not_restorable:,}")
    print(f"[Step 2] 復元可能: {restorable_count:,} / 不可: {not_restorable:,}")

    # サンプル確認（ドライラン含む常時実行）
    sample_rows = conn.execute("""
        SELECT race_id, race_date, venue_code, surface, distance
        FROM race_log
        WHERE course_id = '' AND venue_code != '' AND surface != '' AND distance > 0
        ORDER BY race_id
        LIMIT 5
    """).fetchall()
    print("  [サンプル]")
    for row in sample_rows:
        expected_cid = f"{row['venue_code']}_{row['surface']}_{row['distance']}"
        print(f"    race_id={row['race_id']} race_date={row['race_date']} → course_id={expected_cid}")
        logger.info(f"  sample: race_id={row['race_id']} → course_id={expected_cid}")

    # -----------------------------------------------------------
    # Step 3: course_id の UPDATE
    # -----------------------------------------------------------
    # limit 指定時は対象 id を絞る
    if limit:
        target_ids = conn.execute("""
            SELECT id FROM race_log
            WHERE course_id = ''
              AND venue_code != ''
              AND surface != ''
              AND distance > 0
            ORDER BY id
            LIMIT ?
        """, (limit,)).fetchall()
        target_ids = [r["id"] for r in target_ids]
        checked = len(target_ids)
        logger.info(f"[Step 3] limit={limit} 指定。対象 id: {checked:,} 件")
        print(f"[Step 3] limit={limit} 指定。対象 id: {checked:,} 件")

        if not dry_run:
            # バッチ UPDATE（SQLite は UPDATE WHERE id IN (...) が効率的）
            # 1000件ずつ分割
            BATCH = 1000
            updated_total = 0
            for i in range(0, len(target_ids), BATCH):
                batch = target_ids[i:i+BATCH]
                placeholders = ",".join("?" * len(batch))
                cur = conn.execute(f"""
                    UPDATE race_log
                    SET course_id = venue_code || '_' || surface || '_' || distance
                    WHERE id IN ({placeholders})
                """, batch)
                updated_total += cur.rowcount
                conn.commit()
            updated = updated_total
        else:
            updated = 0
    else:
        checked = restorable_count
        if not dry_run:
            cur = conn.execute("""
                UPDATE race_log
                SET course_id = venue_code || '_' || surface || '_' || distance
                WHERE course_id = ''
                  AND venue_code != ''
                  AND surface != ''
                  AND distance > 0
            """)
            conn.commit()
            updated = cur.rowcount
        else:
            updated = 0

    skipped = not_restorable

    if dry_run:
        logger.info(f"[Step 3] DRY RUN: 更新予定={checked:,} 件（実際には更新しない）")
        print(f"[Step 3] DRY RUN: 更新予定={checked:,} 件（実際には更新しない）")
    else:
        logger.info(f"[Step 3] UPDATE 完了: {updated:,} 件")
        print(f"[Step 3] UPDATE 完了: {updated:,} 件")

    # -----------------------------------------------------------
    # Step 4: 修正後の件数確認
    # -----------------------------------------------------------
    after_count = count_empty_course(conn)
    logger.info(f"[Step 4] 修正後 course_id='' 件数: {after_count:,}")
    print(f"[Step 4] 修正後 course_id='' 件数: {after_count:,}")

    # -----------------------------------------------------------
    # Step 5: run_dev の再計算
    # -----------------------------------------------------------
    if not skip_run_dev and not dry_run:
        print(f"\n[Step 5] run_dev 再計算開始（backfill_run_dev --fix-empty-course 相当）")
        logger.info("[Step 5] run_dev 再計算開始")

        try:
            from scripts.backfill_run_dev import run_backfill
            result = run_backfill(
                dry_run=False,
                fix_empty_course=True,
                show_progress=True,
            )
            logger.info(f"[Step 5] run_dev 再計算完了: {result}")
            print(f"[Step 5] run_dev 再計算完了: {result}")
        except ImportError:
            # モジュールインポートが失敗した場合はサブプロセスで実行
            import subprocess
            proc = subprocess.run(
                [sys.executable, str(ROOT / "scripts" / "backfill_run_dev.py"), "--fix-empty-course"],
                capture_output=False,
                text=True,
            )
            logger.info(f"[Step 5] subprocess 完了: returncode={proc.returncode}")
    elif skip_run_dev:
        print(f"\n[Step 5] --skip-run-dev 指定のため run_dev 計算をスキップ")
        logger.info("[Step 5] skip_run_dev=True のためスキップ")
    else:
        print(f"\n[Step 5] DRY RUN のため run_dev 計算をスキップ")

    # -----------------------------------------------------------
    # Step 6: 最終サマリ
    # -----------------------------------------------------------
    elapsed = time.time() - t0

    # run_dev NULL 件数の確認
    run_dev_null = conn.execute("SELECT COUNT(*) FROM race_log WHERE run_dev IS NULL").fetchone()[0]
    run_dev_null_empty_course = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE run_dev IS NULL AND course_id = ''"
    ).fetchone()[0]

    summary = {
        "checked": checked,
        "updated": updated,
        "skipped": skipped,
        "elapsed": round(elapsed, 2),
        "course_id_empty_after": after_count,
        "run_dev_null": run_dev_null,
        "run_dev_null_empty_course": run_dev_null_empty_course,
    }
    logger.info(f"[完了] {summary}")
    print(f"\n{'='*60}")
    print(f"[完了] elapsed={elapsed:.1f}秒")
    print(f"  course_id 修正: {updated:,} 件")
    print(f"  course_id='' 残り: {after_count:,} 件")
    print(f"  run_dev NULL 全体: {run_dev_null:,} 件")
    print(f"  run_dev NULL (course_id=''): {run_dev_null_empty_course:,} 件")
    print(f"{'='*60}")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="race_log の course_id='' 行を venue_code+surface+distance から復元する"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="SELECT のみ。DB は更新しない",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="処理件数の上限（テスト用）",
    )
    parser.add_argument(
        "--skip-run-dev", action="store_true",
        help="run_dev の再計算をスキップする（course_id 修正のみ）",
    )
    args = parser.parse_args()

    if args.dry_run:
        print("[DRY RUN モード] DB は更新しません")
    refix_course_id(
        dry_run=args.dry_run,
        limit=args.limit,
        skip_run_dev=args.skip_run_dev,
    )


if __name__ == "__main__":
    main()
