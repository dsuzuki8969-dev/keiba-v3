# -*- coding: utf-8 -*-
"""batch_rerun 再開スクリプト (失敗日リトライ + 未処理日)

PID 617 がハングしたため、失敗7日 + 未処理57日 = 64日分を再開する。
margin修復済DBで能力値を正しく再計算。

使い方:
  python scripts/batch_rerun_resume.py
"""
import os
import sys
import time
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

pred_dir = Path("data/predictions")
log_path = Path("data/logs/batch_rerun_2026_resume.log")


def get_target_dates() -> list:
    """失敗日 + 未処理日を合算して返す"""
    # 前回TIMEOUT失敗した日
    failed = ["20260125", "20260126", "20260301", "20260308",
              "20260312", "20260315", "20260322"]
    # 未処理日 (3/23〜)
    remaining = []
    for f in sorted(pred_dir.glob("2026*_pred.json")):
        if "_prev" in f.name or ".bak" in f.name:
            continue
        d = f.name[:8]
        if d >= "20260323":
            remaining.append(d)
    return sorted(set(failed + remaining))


def format_date(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def run_one_day(date_str: str) -> tuple:
    """1日分のフルパイプライン実行 (os.system版 — subprocess.runハング回避)
    --offline: netkeiba完全スキップ、キャッシュ/DBのみでML推論実行"""
    formatted = format_date(date_str)
    python_exe = sys.executable.replace("/", "\\")
    # 中断マーカーを削除して全レース再処理を強制
    done_marker = Path(f"output/.done_{date_str}.txt")
    if done_marker.exists():
        done_marker.unlink()
    cmd = (
        f'"{python_exe}" run_analysis_date.py {formatted}'
        f' --offline --no-html --race-ids-from-pred'
        f' > data/logs/_batch_day_{date_str}.log 2>&1'
    )
    t0 = time.time()
    try:
        rc = os.system(cmd)
        elapsed = time.time() - t0
        if rc != 0:
            # エラー内容を取得
            err_log = Path(f"data/logs/_batch_day_{date_str}.log")
            err = ""
            if err_log.exists():
                err = err_log.read_text(encoding="utf-8", errors="replace")[-300:]
            return (False, elapsed, f"rc={rc} {err}")
        return (True, elapsed, "")
    except Exception as e:
        elapsed = time.time() - t0
        return (False, elapsed, str(e))


def log(msg: str):
    """ログ出力 + ファイル書込"""
    print(msg, flush=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def main():
    dates = get_target_dates()
    total = len(dates)
    log(f"=== batch_rerun 再開 ({datetime.now().isoformat()}) ===")
    log(f"対象: {total}日分 ({dates[0]}〜{dates[-1]})")
    log(f"margin修復済DBで能力値を正しく再計算")
    log("")

    success_count = 0
    fail_count = 0
    total_elapsed = 0
    failed_dates = []

    for i, date_str in enumerate(dates):
        pct = (i / total) * 100
        bar_len = 30
        filled = int(bar_len * i / total)
        bar = "█" * filled + "░" * (bar_len - filled)

        if i > 0 and total_elapsed > 0:
            avg_per_day = total_elapsed / i
            remaining = avg_per_day * (total - i)
            eta_min = remaining / 60
            log(f"[{bar}] {pct:5.1f}% ({i}/{total}) "
                f"経過{total_elapsed/60:.0f}分 残{eta_min:.0f}分 "
                f"成功{success_count} 失敗{fail_count} | {format_date(date_str)}")
        else:
            log(f"[{bar}] {pct:5.1f}% ({i}/{total}) | {format_date(date_str)}")

        ok, elapsed, err = run_one_day(date_str)
        total_elapsed += elapsed

        if ok:
            success_count += 1
            log(f"  ✅ {format_date(date_str)} ({elapsed:.0f}秒)")
        else:
            fail_count += 1
            failed_dates.append((date_str, err))
            log(f"  ❌ 失敗: {format_date(date_str)} ({elapsed:.0f}秒) {err[:100]}")

        if i < total - 1:
            time.sleep(10)

        if (i + 1) % 10 == 0:
            avg = total_elapsed / (i + 1)
            log(f"  --- 中間: {i+1}/{total} 平均{avg:.1f}秒/日 "
                f"成功{success_count} 失敗{fail_count} ---")

    # 最終レポート
    log(f"\n{'='*60}")
    log(f"完了: {total}日処理")
    log(f"  成功: {success_count}")
    log(f"  失敗: {fail_count}")
    log(f"  総所要: {total_elapsed/60:.1f}分 ({total_elapsed/3600:.1f}時間)")
    if total > 0:
        log(f"  平均: {total_elapsed/total:.1f}秒/日")

    if failed_dates:
        log(f"\n失敗一覧:")
        for d, e in failed_dates:
            log(f"  {format_date(d)}: {e[:100]}")

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
    log(f"\n完了マーカー: {marker}")


if __name__ == "__main__":
    main()
