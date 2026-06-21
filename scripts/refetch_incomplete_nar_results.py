# -*- coding: utf-8 -*-
"""time/last_3f が欠落した NAR レースを公式(keiba.go.jp)から再取得して補完する。

マスター指摘(2026-06-21): レースによって着差・後半3Fタイム・走破タイムが無い。
真因: NAR レース直後の速報取得時は keiba.go.jp にタイム/上がり3F が未掲載で
      time_sec=0.0 / last_3f=0.0 で保存される。確定後は掲載されるが、payouts が
      完整なため fetch_actual_results のキャッシュ再fetch がトリガーされず欠落が残る。
対処: order の半数以上が time_sec 欠落の NAR レースを OfficialNARScraper.get_result で
      再取得し、time/last_3f/margin/corners を含む order に上書きする。JRA は結果ページに
      タイム列があり欠落しないため対象外。

使い方:
  python scripts/refetch_incomplete_nar_results.py --date 20260621
  python scripts/refetch_incomplete_nar_results.py --date 20260621 --dry-run
"""
import argparse
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def refetch(date: str, dry_run: bool = False) -> dict:
    from src.results_tracker import refetch_incomplete_nar_times
    stats = refetch_incomplete_nar_times(date, dry_run=dry_run)
    print(f"再取得対象(NAR・time欠落): {stats.get('targets', 0)}レース / 補完: {stats.get('fixed', 0)}")
    return stats


def main():
    ap = argparse.ArgumentParser(description="time欠落NARレースを公式再取得")
    ap.add_argument("--date", required=True, help="対象日 (例: 20260621)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    print(f"=== NAR time欠落レース 再取得 date={args.date} ===")
    stats = refetch(args.date, dry_run=args.dry_run)
    print(f"完了: 補完={stats.get('fixed', 0)} / 対象={stats.get('targets', 0)}")


if __name__ == "__main__":
    main()
