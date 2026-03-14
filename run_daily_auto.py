"""
日次自動運用スクリプト
使い方:
  python run_daily_auto.py --predict           # 今日の予想生成
  python run_daily_auto.py --results           # 昨日の結果照合
  python run_daily_auto.py --predict --date YYYY-MM-DD  # 指定日の予想生成
  python run_daily_auto.py --results --date YYYY-MM-DD  # 指定日の結果照合
  python run_daily_auto.py --predict --date YYYY-MM-DD --official  # 公式のみで予想生成

Windowsタスクスケジューラ設定例:
  朝8:00  → python run_daily_auto.py --predict
  夕17:00 → python run_daily_auto.py --predict --date <翌日> --official
  夜22:00 → python run_daily_auto.py --results
"""
import sys, io, os, subprocess, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
from datetime import datetime, timedelta

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
os.makedirs(LOG_DIR, exist_ok=True)


def _log_path(mode: str, date: str) -> str:
    return os.path.join(LOG_DIR, f"{mode}_{date.replace('-', '')}.log")


def run_predict(date: str, official: bool = False):
    """指定日の予想生成を実行"""
    mode_label = "公式のみ" if official else "通常"
    print(f"[PREDICT] {date} の予想生成を開始... ({mode_label})")
    log_path = _log_path("predict", date)

    if official:
        # --official モード: main.py --analyze_date 経由
        cmd = [
            sys.executable, "main.py",
            "--analyze_date", date,
            "--official",
            "--no_open",
            "--workers", "3",
        ]
    else:
        # 通常モード: run_analysis_date.py 経由
        cmd = [sys.executable, "run_analysis_date.py", date]

    with open(log_path, "w", encoding="utf-8") as lf:
        lf.write(f"=== 予想生成 {date} ({mode_label}) ===\n開始: {datetime.now()}\n\n")
        lf.flush()
        proc = subprocess.run(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=lf, stderr=lf,
            encoding="utf-8",
        )
    status = "完了" if proc.returncode == 0 else f"エラー(code={proc.returncode})"
    print(f"[PREDICT] {date} {status}  ログ: {log_path}")
    return proc.returncode == 0


def run_results(date: str):
    """指定日の結果照合を実行"""
    print(f"[RESULTS] {date} の結果照合を開始...")
    log_path = _log_path("results", date)

    # 予想JSONが存在するか確認
    from src.results_tracker import load_prediction
    if not load_prediction(date):
        print(f"[RESULTS] {date} の予想JSONが存在しません（スキップ）")
        return False

    cmd = [sys.executable, "run_results.py", date]
    with open(log_path, "w", encoding="utf-8") as lf:
        lf.write(f"=== 結果照合 {date} ===\n開始: {datetime.now()}\n\n")
        lf.flush()
        proc = subprocess.run(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=lf, stderr=lf,
            encoding="utf-8",
        )
    status = "完了" if proc.returncode == 0 else f"エラー(code={proc.returncode})"
    print(f"[RESULTS] {date} {status}  ログ: {log_path}")
    return proc.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="D-AI競馬 日次自動運用")
    parser.add_argument("--predict", action="store_true", help="予想生成")
    parser.add_argument("--results", action="store_true", help="結果照合")
    parser.add_argument("--date",    type=str, default=None, help="対象日付(YYYY-MM-DD)")
    parser.add_argument("--official", action="store_true",
                        help="JRA/NAR公式のみで予想生成（netkeiba不使用）")
    args = parser.parse_args()

    today     = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    if args.predict:
        date = args.date or today
        ok = run_predict(date, official=args.official)
        sys.exit(0 if ok else 1)

    if args.results:
        date = args.date or yesterday
        ok = run_results(date)
        sys.exit(0 if ok else 1)

    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
