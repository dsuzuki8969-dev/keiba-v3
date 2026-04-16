"""
バッチ再分析スクリプト — 過去の予想JSONをPhase 2-5適用済みで再生成

使い方:
  # 既存pred.jsonの再分析
  python scripts/batch_reanalyze.py --start 2026-01-01 --end 2026-03-18
  python scripts/batch_reanalyze.py --start 2026-01-01 --end 2026-03-18 --parallel 3

  # pred.jsonが存在しない日付を新規生成
  python scripts/batch_reanalyze.py --start 2024-11-19 --end 2025-01-27 --generate-missing

  # ドライラン
  python scripts/batch_reanalyze.py --start 2024-11-19 --end 2025-01-27 --generate-missing --dry-run
"""
import argparse
import datetime
import os
import shutil
import subprocess
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from config.settings import PREDICTIONS_DIR

# 進捗表示用ロック
_print_lock = threading.Lock()
_counter = {"ok": 0, "ng": 0, "done": 0}


def list_dates_with_predictions(start: str, end: str) -> list:
    """指定範囲内でpred.jsonが存在する日付リストを返す"""
    dates = []
    if not os.path.isdir(PREDICTIONS_DIR):
        return dates
    for fname in sorted(os.listdir(PREDICTIONS_DIR)):
        if not fname.endswith("_pred.json"):
            continue
        if "_backup" in fname:
            continue
        dstr = fname[:8]
        try:
            d = datetime.date(int(dstr[:4]), int(dstr[4:6]), int(dstr[6:8]))
        except ValueError:
            continue
        iso = d.isoformat()
        if start <= iso <= end:
            dates.append(iso)
    return dates


def list_missing_dates(start: str, end: str) -> list:
    """指定範囲内でpred.jsonが存在しない日付リストを返す"""
    existing = set(list_dates_with_predictions(start, end))
    dates = []
    d = datetime.date.fromisoformat(start)
    end_d = datetime.date.fromisoformat(end)
    while d <= end_d:
        iso = d.isoformat()
        if iso not in existing:
            dates.append(iso)
        d += datetime.timedelta(days=1)
    return dates


def backup_prediction(date: str):
    """既存のpred.jsonをバックアップ"""
    dstr = date.replace("-", "")
    src = os.path.join(PREDICTIONS_DIR, f"{dstr}_pred.json")
    dst = os.path.join(PREDICTIONS_DIR, f"{dstr}_pred_backup.json")
    if os.path.exists(src) and not os.path.exists(dst):
        shutil.copy2(src, dst)


def run_reanalysis(date: str, workers: int = 6, use_pred: bool = True) -> bool:
    """1日分の再分析を実行
    use_pred=False: pred.jsonが存在しない新規日付用（通常フェッチ）
    """
    cmd = [
        sys.executable, "run_analysis_date.py", date,
        "--no-html", "--force",
        "--workers", str(workers),
    ]
    if use_pred:
        cmd.append("--race-ids-from-pred")
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "log", "batch")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{date.replace('-','')}.log")
    try:
        # capture_output=True はstdout/stderrバッファのデッドロック原因
        # ファイルに直接出力してデッドロックを回避
        with open(log_file, "w", encoding="utf-8") as f:
            result = subprocess.run(
                cmd,
                cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."),
                stdout=f,
                stderr=subprocess.STDOUT,
                timeout=3600,  # 60分タイムアウト
            )
        # returncode非0でもpredファイルが更新されていれば成功とみなす
        if result.returncode == 0:
            return True
        dstr = date.replace("-", "")
        pred_path = os.path.join(PREDICTIONS_DIR, f"{dstr}_pred.json")
        if os.path.exists(pred_path):
            import datetime as _dt
            mtime = _dt.datetime.fromtimestamp(os.path.getmtime(pred_path))
            if (datetime.datetime.now() - mtime).total_seconds() < 3600:
                return True  # 直近1時間以内に更新 = 実質成功
        return False
    except subprocess.TimeoutExpired:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n=== {date} TIMEOUT ===\n")
        return False
    except Exception as e:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n=== {date} EXCEPTION: {e} ===\n")
        return False


def _process_date(date: str, workers: int, total: int, use_pred: bool = True) -> tuple:
    """1日分を処理（並列実行用）"""
    backup_prediction(date)
    success = run_reanalysis(date, workers=workers, use_pred=use_pred)

    with _print_lock:
        _counter["done"] += 1
        if success:
            _counter["ok"] += 1
        else:
            _counter["ng"] += 1
        done = _counter["done"]
        elapsed = time.time() - _counter["t0"]
        avg = elapsed / done
        eta = avg * (total - done)
        status = "OK" if success else "NG"
        print(f"  [{done}/{total}] {date} {status}  "
              f"(経過{elapsed/60:.0f}分 / 残{eta/60:.0f}分)", flush=True)

    return date, success


def main():
    parser = argparse.ArgumentParser(description="バッチ再分析スクリプト")
    parser.add_argument("--start", required=True, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=2,
                        help="レース内並列ワーカー数 (デフォルト: 2)")
    parser.add_argument("--parallel", type=int, default=6,
                        help="同時処理日数 (デフォルト: 6)")
    parser.add_argument("--dry-run", action="store_true",
                        help="実行せず対象日付を表示")
    parser.add_argument("--resume-after", default="",
                        help="指定日時(ISO)以降に更新済みのpred.jsonをスキップ (例: 2026-03-18T21:00)")
    parser.add_argument("--generate-missing", action="store_true",
                        help="pred.jsonが存在しない日付を新規生成（通常フェッチ使用）")
    args = parser.parse_args()

    use_pred = not args.generate_missing

    if args.generate_missing:
        dates = list_missing_dates(args.start, args.end)
        if not dates:
            print(f"対象なし: {args.start} 〜 {args.end} に欠落日はありません")
            return
    else:
        dates = list_dates_with_predictions(args.start, args.end)
        if not dates:
            print(f"対象なし: {args.start} 〜 {args.end} にpred.jsonが見つかりません")
            return

    # --resume-after: 指定日時以降に更新済みのpred.jsonをスキップ
    if args.resume_after:
        import datetime as _dt
        _cutoff_dt = _dt.datetime.fromisoformat(args.resume_after)
        _skipped = 0
        _remaining = []
        for d in dates:
            dstr = d.replace("-", "")
            pred_path = os.path.join(PREDICTIONS_DIR, f"{dstr}_pred.json")
            if os.path.exists(pred_path):
                mtime = _dt.datetime.fromtimestamp(os.path.getmtime(pred_path))
                if mtime >= _cutoff_dt:
                    _skipped += 1
                    continue
            _remaining.append(d)
        dates = _remaining
        print(f"resume: {_skipped}日スキップ（{args.resume_after}以降に更新済み）")

    total = len(dates)
    if total == 0:
        print("全日付が処理済みです")
        return
    print(f"バッチ再分析: {total}日間 ({dates[0]} 〜 {dates[-1]})")
    print(f"  workers={args.workers}, parallel={args.parallel}")

    if args.dry_run:
        for d in dates:
            print(f"  {d}")
        print(f"\n合計: {total}日 (--dry-run: 実行なし)")
        return

    _counter["t0"] = time.time()

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = [
            pool.submit(_process_date, d, args.workers, total, use_pred)
            for d in dates
        ]
        # as_completed で結果回収（エラーハンドリング）
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                with _print_lock:
                    _counter["ng"] += 1
                    print(f"  [例外] {e}", flush=True)

    elapsed = time.time() - _counter["t0"]
    print(f"\n完了: {_counter['ok']}成功 / {_counter['ng']}失敗  "
          f"({elapsed/60:.1f}分)")


if __name__ == "__main__":
    main()
