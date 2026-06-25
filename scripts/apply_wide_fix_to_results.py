#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ワイド払戻バグ修正 → results/ ファイルへマージ適用スクリプト

【動作】
  data/results_fixed/ にある正しいワイド払戻を
  data/results/ の対応ファイルのワイドのみに差し替える。
  ワイド以外のキー (order / payouts 他券種 / source 等) は
  results/ 側を完全保持する (丸ごとコピー禁止)。

【バックアップ】
  本番書き換え前に data/results/ → data/results_backup_20260625/ にフルコピーを取得。
  バックアップ済なら再取得しない。

【使い方】
  # バックアップ + dry-run (差し替わる件数を確認)
  python scripts/apply_wide_fix_to_results.py --dry-run

  # 本番適用 (バックアップ済であることを前提)
  python scripts/apply_wide_fix_to_results.py

  # 年を指定して適用
  python scripts/apply_wide_fix_to_results.py --year 2025
  python scripts/apply_wide_fix_to_results.py --year 2026
"""
import argparse
import glob
import json
import logging
import os
import shutil
import sys
import time
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
FIXED_DIR = os.path.join(PROJECT_ROOT, "data", "results_fixed")
BACKUP_DIR = os.path.join(PROJECT_ROOT, "data", "results_backup_20260625")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def is_wide_dup(wide_list: list) -> bool:
    """ワイドが複数エントリで全部同額 → バグ (同額複製) と判定"""
    if not isinstance(wide_list, list) or len(wide_list) < 2:
        return False
    payouts = [w.get("payout") for w in wide_list if isinstance(w, dict)]
    if len(payouts) < 2:
        return False
    return len(set(payouts)) == 1


def take_backup() -> bool:
    """data/results/ を data/results_backup_20260625/ にコピー (既存なら skip)"""
    if os.path.exists(BACKUP_DIR):
        existing = len(glob.glob(os.path.join(BACKUP_DIR, "*.json")))
        logger.info(
            "バックアップ済のため skip: %s (%d ファイル)", BACKUP_DIR, existing
        )
        return True

    logger.info("バックアップ開始: %s → %s", RESULTS_DIR, BACKUP_DIR)
    try:
        shutil.copytree(RESULTS_DIR, BACKUP_DIR)
        count = len(glob.glob(os.path.join(BACKUP_DIR, "*.json")))
        logger.info("バックアップ完了: %d ファイルをコピー", count)
        return True
    except Exception as e:
        logger.error("バックアップ失敗: %s", e)
        return False


def process_file(
    results_path: str,
    fixed_path: str,
    dry_run: bool = False,
) -> dict:
    """1 ファイルを処理してワイドのみ差し替える。統計 dict を返す"""
    stats = {
        "total_races": 0,
        "dup_replaced": 0,      # 同額複製 → 正常データに差し替え
        "dup_no_fixed": 0,      # 同額複製だが fixed に対応 race なし
        "skip_normal": 0,       # 正常 (バグなし)
        "skip_no_wide": 0,      # ワイドデータなし
        "already_correct": 0,   # fixed 側も同額複製 (念のため skip)
    }

    with open(results_path, "r", encoding="utf-8") as f:
        results_data = json.load(f)

    with open(fixed_path, "r", encoding="utf-8") as f:
        fixed_data = json.load(f)

    changed = False
    new_results_data = {}

    for race_id, race in results_data.items():
        stats["total_races"] += 1
        payouts = race.get("payouts", {})
        wide_current = payouts.get("ワイド")

        if not wide_current:
            # ワイドデータなし (3 頭立て等)
            new_results_data[race_id] = race
            stats["skip_no_wide"] += 1
            continue

        if not is_wide_dup(wide_current):
            # 現在のワイドは正常
            new_results_data[race_id] = race
            stats["skip_normal"] += 1
            continue

        # 同額複製バグ確定 → fixed から取得
        if race_id not in fixed_data:
            logger.debug("fixed に存在しない race_id: %s (スキップ)", race_id)
            new_results_data[race_id] = race
            stats["dup_no_fixed"] += 1
            continue

        fixed_wide = fixed_data[race_id].get("payouts", {}).get("ワイド")

        if not fixed_wide:
            logger.debug("fixed のワイドが空: %s (スキップ)", race_id)
            new_results_data[race_id] = race
            stats["dup_no_fixed"] += 1
            continue

        if is_wide_dup(fixed_wide):
            # fixed 側も同額複製なら差し替えを skip (安全策)
            logger.debug("fixed 側も同額複製のため skip: %s", race_id)
            new_results_data[race_id] = race
            stats["already_correct"] += 1
            continue

        # 正常な fixed ワイドが取得できた → ワイドのみ差し替え
        import copy
        new_race = copy.deepcopy(race)
        new_race["payouts"]["ワイド"] = fixed_wide
        new_results_data[race_id] = new_race
        changed = True
        stats["dup_replaced"] += 1

        logger.debug(
            "差し替え: %s  旧=%s → 新=%s",
            race_id,
            [w.get("payout") for w in wide_current],
            [w.get("payout") for w in fixed_wide],
        )

    if not dry_run and changed:
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(new_results_data, f, ensure_ascii=False, separators=(",", ":"))
        logger.debug("書き込み完了: %s", results_path)

    return stats


def compute_dup_rate(year_str: str) -> tuple[int, int, float]:
    """指定年の results/ ファイルから同額複製率を計算"""
    pattern = os.path.join(RESULTS_DIR, f"{year_str}*_results.json")
    files = sorted(glob.glob(pattern))
    total = 0
    dup = 0
    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        for race in d.values():
            wide = race.get("payouts", {}).get("ワイド")
            if isinstance(wide, list) and len(wide) >= 2:
                total += 1
                if is_wide_dup(wide):
                    dup += 1
    rate = dup / total * 100 if total > 0 else 0.0
    return total, dup, rate


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ワイド払戻バグ修正を results/ に適用 (ワイドのみ差し替え)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="実際の書き込みを行わず差し替わる件数のみ表示",
    )
    parser.add_argument(
        "--year",
        choices=["2025", "2026"],
        help="処理対象年 (省略時は 2025+2026 両方)",
    )
    parser.add_argument(
        "--skip-backup",
        action="store_true",
        help="バックアップ取得を skip (既にバックアップ済の場合に使用)",
    )
    args = parser.parse_args()

    logger.info("=== ワイド払戻バグ修正 apply スクリプト開始 ===")
    logger.info("dry_run=%s, year=%s", args.dry_run, args.year or "2025+2026")

    # --- ステップ 1: バックアップ取得 ---
    if not args.dry_run and not args.skip_backup:
        if not take_backup():
            logger.error("バックアップ失敗。処理を中断します")
            sys.exit(1)
    elif args.dry_run:
        logger.info("[dry-run] バックアップは skip (実際の書き込みなし)")

    # --- ステップ 2: before 同額複製率計算 ---
    years_to_process = [args.year] if args.year else ["2025", "2026"]
    logger.info("--- before: 同額複製率 ---")
    before_stats = {}
    for yr in years_to_process:
        tot, dup, rate = compute_dup_rate(yr)
        before_stats[yr] = (tot, dup, rate)
        logger.info(
            "  %s年: ワイドあり=%d, 同額複製=%d (%.1f%%)", yr, tot, dup, rate
        )

    # --- ステップ 3: 対象ファイル収集 ---
    target_years = years_to_process
    target_files = []
    for yr in target_years:
        pattern = os.path.join(FIXED_DIR, f"{yr}*_results.json")
        fixed_files = sorted(glob.glob(pattern))
        for fixed_path in fixed_files:
            fname = os.path.basename(fixed_path)
            results_path = os.path.join(RESULTS_DIR, fname)
            if os.path.exists(results_path):
                target_files.append((results_path, fixed_path))
            else:
                logger.debug("results/ 対応なし: %s", fname)

    logger.info("対象ファイル数: %d", len(target_files))

    # --- ステップ 4: ファイル処理ループ ---
    total_stats: dict = defaultdict(int)
    start_time = time.time()

    for i, (results_path, fixed_path) in enumerate(target_files):
        stats = process_file(results_path, fixed_path, dry_run=args.dry_run)
        for k, v in stats.items():
            total_stats[k] += v

        elapsed = time.time() - start_time
        pct = (i + 1) / len(target_files) * 100
        bar_filled = int(pct / 5)
        bar = "#" * bar_filled + "." * (20 - bar_filled)
        rate_per_sec = (i + 1) / elapsed if elapsed > 0 else 0
        eta = (len(target_files) - i - 1) / rate_per_sec if rate_per_sec > 0 else 0
        print(
            f"\r[{bar}] {pct:5.1f}% ({i+1}/{len(target_files)}) "
            f"差し替え={total_stats['dup_replaced']} "
            f"fixed無={total_stats['dup_no_fixed']} "
            f"ETA={eta:.0f}s",
            end="",
            flush=True,
        )

    print()  # 改行
    elapsed_total = time.time() - start_time

    # --- ステップ 5: after 同額複製率計算 ---
    if not args.dry_run:
        logger.info("--- after: 同額複製率 ---")
        for yr in years_to_process:
            tot, dup, rate = compute_dup_rate(yr)
            b_tot, b_dup, b_rate = before_stats[yr]
            logger.info(
                "  %s年: ワイドあり=%d, 同額複製=%d (%.1f%%)  [before: %d (%.1f%%)]",
                yr, tot, dup, rate, b_dup, b_rate
            )

    # --- サマリ ---
    logger.info("=== 処理サマリ ===")
    logger.info("  所要時間          : %.1f 秒", elapsed_total)
    logger.info("  処理ファイル数    : %d", len(target_files))
    logger.info("  処理 race 数      : %d", total_stats["total_races"])
    logger.info("  ワイドなし        : %d", total_stats["skip_no_wide"])
    logger.info("  正常 (バグなし)   : %d", total_stats["skip_normal"])
    logger.info("  同額複製→差し替え : %d (%s)",
                total_stats["dup_replaced"],
                "dry-run=実際には書き込まず" if args.dry_run else "本番適用済")
    logger.info("  同額複製→fixed無  : %d", total_stats["dup_no_fixed"])
    logger.info("  fixed側も同額複製 : %d", total_stats["already_correct"])

    if args.dry_run:
        logger.info("[dry-run] 実際の書き込みは行っていません")
        logger.info("  本番適用: python scripts/apply_wide_fix_to_results.py --skip-backup")

    # --- 202535060201 サンプル検証 (本番適用後のみ) ---
    if not args.dry_run:
        logger.info("=== サンプル検証 (202535060201: 2025-06-02 R1) ===")
        sample_file = os.path.join(RESULTS_DIR, "20250602_results.json")
        if os.path.exists(sample_file):
            with open(sample_file, "r", encoding="utf-8") as f:
                check = json.load(f)
            race = check.get("202535060201", {})
            wide = race.get("payouts", {}).get("ワイド", [])
            if wide:
                for w in wide:
                    logger.info("  %s = %d 円", w.get("combo"), w.get("payout"))
                payouts_list = [w.get("payout") for w in wide]
                ok = len(set(payouts_list)) > 1
                logger.info("  -> %s", "OK (3通り別々)" if ok else "NG (まだ同額複製)")
            else:
                logger.info("  ワイドデータなし")
        else:
            logger.warning("  サンプルファイルなし: %s", sample_file)

    # --- バックアップ情報 ---
    if not args.dry_run:
        logger.info("=== バックアップ ===")
        logger.info("  場所: %s", BACKUP_DIR)
        if os.path.exists(BACKUP_DIR):
            cnt = len(glob.glob(os.path.join(BACKUP_DIR, "*.json")))
            logger.info("  ファイル数: %d", cnt)
        logger.info("  ロールバック: robocopy %s %s /E /XO", BACKUP_DIR, RESULTS_DIR)


if __name__ == "__main__":
    main()
