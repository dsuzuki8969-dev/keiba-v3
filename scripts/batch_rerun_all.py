# -*- coding: utf-8 -*-
"""全日程フルパイプライン再実行スクリプト

6施策(#1 B_prefix, #2 VENUE_SPEED_TABLE, #3 ベイズ収縮,
       #4 TEKIPAN動的化, #5 坂データ, #6 PACE_CAP分離)
を完全反映するため、2026年全日程をフルパイプラインで再実行する。

使い方:
  python scripts/batch_rerun_all.py
  python scripts/batch_rerun_all.py --start 20260301   # 途中から再開
  python scripts/batch_rerun_all.py --dry-run           # 対象一覧のみ表示
"""
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

pred_dir = Path("data/predictions")


def get_target_dates(start_from: str = "") -> list:
    """再実行対象の日付リストを返す"""
    files = sorted(pred_dir.glob("2026*_pred.json"))
    dates = []
    for f in files:
        if "_prev" in f.name or ".bak" in f.name:
            continue
        date_str = f.name[:8]
        if start_from and date_str < start_from:
            continue
        dates.append(date_str)
    return dates


def format_date(d: str) -> str:
    """20260101 → 2026-01-01"""
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def run_one_day(date_str: str) -> tuple:
    """1日分のフルパイプライン実行。(成功, 所要秒, stderr抜粋) を返す"""
    formatted = format_date(date_str)
    cmd = [
        sys.executable, "run_analysis_date.py",
        formatted,
        "--force", "--no-html", "--race-ids-from-pred",
    ]
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=3600,  # 60分タイムアウト（大規模日程対応）
        )
        elapsed = time.time() - t0
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "")[-300:]
            return (False, elapsed, err)
        return (True, elapsed, "")
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        return (False, elapsed, "TIMEOUT (3600s)")
    except Exception as e:
        elapsed = time.time() - t0
        return (False, elapsed, str(e))


def main():
    start_from = ""
    dry_run = False

    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--start" and i < len(sys.argv) - 1:
            start_from = sys.argv[i + 1]
        if arg == "--dry-run":
            dry_run = True

    dates = get_target_dates(start_from)
    total = len(dates)
    print(f"=== フルパイプライン再実行 ===")
    print(f"対象: {total}日分 ({dates[0]}〜{dates[-1]})")
    print(f"6施策: B_prefix/VENUE_SPEED/ベイズ収縮/TEKIPAN/坂データ/PACE_CAP")
    print()

    if dry_run:
        for d in dates:
            print(f"  {format_date(d)}")
        print(f"\n合計: {total}日 (--dry-run: 実行なし)")
        return

    success_count = 0
    fail_count = 0
    total_elapsed = 0
    failed_dates = []

    for i, date_str in enumerate(dates):
        # プログレスバー
        pct = (i / total) * 100
        bar_len = 30
        filled = int(bar_len * i / total)
        bar = "█" * filled + "░" * (bar_len - filled)

        if i > 0 and total_elapsed > 0:
            avg_per_day = total_elapsed / i
            remaining = avg_per_day * (total - i)
            eta_min = remaining / 60
            print(f"[{bar}] {pct:5.1f}% ({i}/{total}) "
                  f"経過{total_elapsed/60:.0f}分 残{eta_min:.0f}分 "
                  f"成功{success_count} 失敗{fail_count} | {format_date(date_str)}")
        else:
            print(f"[{bar}] {pct:5.1f}% ({i}/{total}) | {format_date(date_str)}")

        ok, elapsed, err = run_one_day(date_str)
        total_elapsed += elapsed

        if ok:
            success_count += 1
            print(f"  ✅ {format_date(date_str)} ({elapsed:.0f}秒)")
        else:
            fail_count += 1
            failed_dates.append((date_str, err))
            print(f"  ❌ 失敗: {format_date(date_str)} ({elapsed:.0f}秒) {err[:100]}")

        # netkeiba負荷軽減: 日と日の間に10秒インターバル
        if i < total - 1:
            time.sleep(10)

        # 10日ごとに中間報告
        if (i + 1) % 10 == 0:
            avg = total_elapsed / (i + 1)
            print(f"  --- 中間: {i+1}/{total} 平均{avg:.1f}秒/日 "
                  f"成功{success_count} 失敗{fail_count} ---")

    # 最終レポート
    print(f"\n{'='*60}")
    print(f"完了: {total}日処理")
    print(f"  成功: {success_count}")
    print(f"  失敗: {fail_count}")
    print(f"  総所要: {total_elapsed/60:.1f}分 ({total_elapsed/3600:.1f}時間)")
    print(f"  平均: {total_elapsed/total:.1f}秒/日")

    if failed_dates:
        print(f"\n失敗一覧:")
        for d, e in failed_dates:
            print(f"  {format_date(d)}: {e[:100]}")

    # 失敗日の自動リトライ（最大1回）
    if failed_dates:
        print(f"\n=== 失敗日のリトライ ({len(failed_dates)}日) ===")
        retry_failed = []
        for d, _ in failed_dates:
            print(f"  リトライ: {format_date(d)}")
            ok2, elapsed2, err2 = run_one_day(d)
            total_elapsed += elapsed2
            if ok2:
                success_count += 1
                fail_count -= 1
                print(f"  ✅ リトライ成功: {format_date(d)} ({elapsed2:.0f}秒)")
            else:
                retry_failed.append((d, err2))
                print(f"  ❌ リトライ失敗: {format_date(d)} ({elapsed2:.0f}秒)")
            time.sleep(10)
        failed_dates = retry_failed

    # 完了マーカー
    marker = Path("data/logs/batch_rerun_complete.txt")
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        f"completed: {datetime.now().isoformat()}\n"
        f"total: {total}\n"
        f"success: {success_count}\n"
        f"failed: {fail_count}\n"
        f"elapsed_min: {total_elapsed/60:.1f}\n",
        encoding="utf-8",
    )
    print(f"\n完了マーカー: {marker}")


if __name__ == "__main__":
    main()
