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
import sys, io, os, subprocess, time, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
from datetime import datetime, timedelta

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
os.makedirs(LOG_DIR, exist_ok=True)


def _log_path(mode: str, date: str) -> str:
    return os.path.join(LOG_DIR, f"{mode}_{date.replace('-', '')}.log")


def _notify(title: str, message: str, icon: str = "info"):
    """Windowsトースト通知を送信（winotifyが利用可能な場合のみ）"""
    try:
        from winotify import Notification, audio
        toast = Notification(
            app_id="D-AI Keiba v3",
            title=title,
            msg=message,
            duration="short",
        )
        if icon == "error":
            toast.set_audio(audio.Default, loop=False)
        toast.show()
    except ImportError:
        pass  # winotify 未インストール時はスキップ
    except Exception:
        pass


def _run_predict_mode(date: str, official: bool) -> bool:
    """指定モードで予想生成を実行する（内部関数）"""
    mode_label = "公式のみ" if official else "通常"
    log_path = _log_path("predict", date)

    if official:
        cmd = [
            sys.executable, "main.py",
            "--analyze_date", date,
            "--official",
            "--no_open",
            "--workers", "3",
        ]
    else:
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
    return proc.returncode == 0


def run_predict(date: str, official: bool = False):
    """指定日の予想生成を実行（失敗時に公式モードへ自動フォールバック）"""
    mode_label = "公式のみ" if official else "通常"
    print(f"[PREDICT] {date} の予想生成を開始... ({mode_label})")

    ok = _run_predict_mode(date, official)

    # 通常モードで失敗した場合、公式モードにフォールバック
    if not ok and not official:
        print(f"[PREDICT] 通常モード失敗 → 公式モードにフォールバック...")
        _notify("予想生成フォールバック", f"{date}: 通常モード失敗。公式モードで再試行中...", "error")
        ok = _run_predict_mode(date, official=True)
        if ok:
            print(f"[PREDICT] {date} 公式モードで完了（フォールバック成功）")
            _notify("予想生成完了", f"{date}: 公式モードで予想生成完了（フォールバック）")
        else:
            print(f"[PREDICT] {date} 公式モードでもエラー")
            _notify("予想生成失敗", f"{date}: 通常/公式両モードとも失敗", "error")
    elif ok:
        status = "完了"
        print(f"[PREDICT] {date} {status}")
        _notify("予想生成完了", f"{date}: {mode_label}で予想生成完了")
    else:
        print(f"[PREDICT] {date} エラー")
        _notify("予想生成失敗", f"{date}: {mode_label}で予想生成失敗", "error")

    log_path = _log_path("predict", date)
    print(f"  ログ: {log_path}")
    return ok


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
    ok = proc.returncode == 0
    status = "完了" if ok else f"エラー(code={proc.returncode})"
    print(f"[RESULTS] {date} {status}  ログ: {log_path}")

    if ok:
        _notify("結果照合完了", f"{date}: 結果照合が完了しました")
    else:
        _notify("結果照合失敗", f"{date}: 結果照合でエラーが発生しました", "error")

    return ok


def run_maintenance():
    """
    メンテナンスタスク（23:00 スケジューラ）:
    - 過去7日分の未照合レースの払戻バックフィル
    - 結果取得成功率ログ
    """
    print("[MAINTENANCE] メンテナンスタスクを開始...")
    from src.results_tracker import load_prediction, compare_and_aggregate

    backfill_count = 0
    for days_ago in range(1, 8):
        d = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        pred = load_prediction(d)
        if not pred:
            continue
        # 結果ファイルが存在しない or 空結果 → 再取得を試行
        from config.settings import RESULTS_DIR
        fpath = os.path.join(RESULTS_DIR, f"{d.replace('-', '')}_results.json")
        if os.path.exists(fpath):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                has_any_order = any(
                    v.get("order") for v in cached.values()
                    if isinstance(v, dict)
                )
                if has_any_order:
                    continue  # 既に結果あり → スキップ
                # 結果なし → 再取得のためファイルを削除
                os.remove(fpath)
            except Exception:
                pass

        # 結果再取得
        print(f"  [BACKFILL] {d} の結果を再取得中...")
        ok = run_results(d)
        if ok:
            backfill_count += 1

    print(f"[MAINTENANCE] 完了 (バックフィル: {backfill_count}件)")
    _notify("メンテナンス完了", f"バックフィル: {backfill_count}件")
    return True


def main():
    parser = argparse.ArgumentParser(description="D-AI競馬 日次自動運用")
    parser.add_argument("--predict", action="store_true", help="予想生成")
    parser.add_argument("--results", action="store_true", help="結果照合")
    parser.add_argument("--maintenance", action="store_true", help="メンテナンス（払戻バックフィル）")
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

    if args.maintenance:
        ok = run_maintenance()
        sys.exit(0 if ok else 1)

    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
