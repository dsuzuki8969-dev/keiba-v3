"""
過去日付バッチ処理スクリプト
使い方:
  python run_batch_past.py --start 2026-01-01 --end 2026-02-24
  python run_batch_past.py --start 2026-01-01 --end 2026-02-24 --no-html
  python run_batch_past.py --start 2026-01-01 --end 2026-02-24 --results-only
  python run_batch_past.py --start 2026-01-01 --end 2026-02-24 --skip-existing
  python run_batch_past.py --start 2024-01-01 --end 2026-03-11 --from-db --skip-existing

オプション:
  --no-html       : HTML生成をスキップ（JSON保存＋結果照合のみ）高速版
  --results-only  : 予想生成をスキップして結果照合のみ実行（予想JSONが既にある場合）
  --skip-existing : 予想JSONが既にある日付をスキップ
  --delay N       : レース間の待機秒数（デフォルト: 5）
  --from-db       : race_logからレースIDを取得（netkeibaアクセス不要）
                    自動的に --race-ids-from-db --ignore-ttl --no-purge を付与
"""
import sys, io, os, subprocess, time, argparse
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log", "batch")
os.makedirs(LOG_DIR, exist_ok=True)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _date_range(start: str, end: str):
    """start～endの日付リストを返す"""
    cur = datetime.strptime(start, "%Y-%m-%d")
    fin = datetime.strptime(end, "%Y-%m-%d")
    while cur <= fin:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


def _get_racing_dates(start: str, end: str):
    """race_logから実際に開催がある日付のみ返す"""
    import sqlite3
    db_path = os.path.join(SCRIPT_DIR, "data", "keiba.db")
    if not os.path.exists(db_path):
        return None
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT DISTINCT race_date FROM race_log WHERE race_date >= ? AND race_date <= ? ORDER BY race_date",
        (start, end),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def run_predict_for_date(date: str, delay: int = 5, no_html: bool = False,
                         from_db: bool = False) -> bool:
    log_path = os.path.join(LOG_DIR, f"predict_{date.replace('-', '')}.log")
    cmd = [sys.executable, "run_analysis_date.py", date]
    if no_html:
        cmd.append("--no-html")
    if from_db:
        cmd.extend(["--race-ids-from-db", "--ignore-ttl", "--no-purge"])
    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as lf:
        lf.write(f"=== 予想生成 {date} ===\n開始: {datetime.now()}\n\n")
        lf.flush()
        proc = subprocess.run(cmd, cwd=SCRIPT_DIR, stdout=lf, stderr=lf, encoding="utf-8")
    elapsed = time.time() - t0
    ok = proc.returncode == 0
    print(f"  {'✓' if ok else '✗'} 予想生成 {date}  {elapsed:.0f}秒  log: {log_path}")
    if delay > 0:
        time.sleep(delay)
    return ok


def run_results_for_date(date: str, delay: int = 3) -> bool:
    from src.results_tracker import load_prediction
    if not load_prediction(date):
        print(f"  - 結果照合スキップ {date}（予想JSONなし）")
        return False

    log_path = os.path.join(LOG_DIR, f"results_{date.replace('-', '')}.log")
    cmd = [sys.executable, "run_results.py", date]
    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as lf:
        lf.write(f"=== 結果照合 {date} ===\n開始: {datetime.now()}\n\n")
        lf.flush()
        proc = subprocess.run(cmd, cwd=SCRIPT_DIR, stdout=lf, stderr=lf, encoding="utf-8")
    elapsed = time.time() - t0
    ok = proc.returncode == 0
    print(f"  {'✓' if ok else '✗'} 結果照合 {date}  {elapsed:.0f}秒  log: {log_path}")
    if delay > 0:
        time.sleep(delay)
    return ok


def main():
    parser = argparse.ArgumentParser(description="D-AI競馬 過去日付バッチ処理")
    parser.add_argument("--start",        required=True, help="開始日 YYYY-MM-DD")
    parser.add_argument("--end",          required=True, help="終了日 YYYY-MM-DD")
    parser.add_argument("--no-html",      action="store_true", help="HTML生成スキップ（JSON+結果照合のみ）高速版")
    parser.add_argument("--results-only", action="store_true", help="結果照合のみ（予想生成スキップ）")
    parser.add_argument("--skip-existing",action="store_true", help="予想JSON既存の日付をスキップ")
    parser.add_argument("--delay",        type=int, default=5, help="待機秒数（デフォルト: 5）")
    parser.add_argument("--from-db",      action="store_true",
                        help="race_logからレースID取得（--race-ids-from-db --ignore-ttl --no-purge 自動付与）")
    args = parser.parse_args()

    # --from-db 指定時は開催日のみに絞る（非開催日をスキップ）
    if args.from_db and not args.results_only:
        racing_dates = _get_racing_dates(args.start, args.end)
        if racing_dates is not None:
            dates = racing_dates
            print(f"  [from-db] race_logから開催日を取得: {len(dates)}日（非開催日をスキップ）")
        else:
            dates = list(_date_range(args.start, args.end))
    else:
        dates = list(_date_range(args.start, args.end))

    mode_str = "結果照合のみ" if args.results_only else ("予想生成(HTML無し) + 結果照合" if args.no_html else "予想生成(HTML有り) + 結果照合")
    if args.from_db:
        mode_str += " [from-db]"
    print(f"\n{'='*60}")
    print(f"  D-AI競馬 バッチ処理開始")
    print(f"  期間: {args.start} ～ {args.end}  ({len(dates)}日)")
    print(f"  モード: {mode_str}")
    print(f"{'='*60}\n")

    predict_ok = predict_ng = results_ok = results_ng = 0
    t_total = time.time()

    for i, date in enumerate(dates, 1):
        print(f"[{i}/{len(dates)}] {date}")

        if not args.results_only:
            from src.results_tracker import load_prediction
            if args.skip_existing and load_prediction(date):
                print(f"  → 予想JSON既存のためスキップ")
            else:
                ok = run_predict_for_date(date, delay=args.delay,
                                          no_html=args.no_html, from_db=args.from_db)
                if ok:
                    predict_ok += 1
                else:
                    predict_ng += 1

        ok = run_results_for_date(date, delay=args.delay)
        if ok:
            results_ok += 1
        else:
            results_ng += 1

        elapsed_total = time.time() - t_total
        remaining = len(dates) - i
        if i > 0 and remaining > 0:
            avg = elapsed_total / i
            eta = avg * remaining
            eta_str = f"{int(eta//3600)}h{int((eta%3600)//60)}m"
            print(f"  経過: {elapsed_total/60:.1f}分  残り推定: {eta_str}")

    print(f"\n{'='*60}")
    print(f"  バッチ処理完了")
    if not args.results_only:
        print(f"  予想生成: 成功{predict_ok} / 失敗{predict_ng}")
    print(f"  結果照合: 成功{results_ok} / 失敗{results_ng}")
    total_min = (time.time()-t_total)/60
    print(f"  総時間: {total_min:.1f}分 ({total_min/60:.1f}時間)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
