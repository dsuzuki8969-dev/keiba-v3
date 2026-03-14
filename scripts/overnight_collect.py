"""
夜間用スクレイピングバッチ
2024-01-01 から今日までの基準タイムDBを収集。
PC電源ONのまま放置して実行。resume対応で途中再開可能。
"""

import sys
import os
import datetime

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    COURSE_DB_PRELOAD_PATH,
    COURSE_DB_COLLECTOR_STATE_PATH,
    PROJECT_ROOT,
)


class TeeWriter:
    """標準出力とログファイルの両方に書き出す"""

    def __init__(self, log_path: str):
        self.log_path = log_path
        self.stdout = sys.stdout
        self.file = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.log_path) or ".", exist_ok=True)
        self.file = open(self.log_path, "a", encoding="utf-8")
        return self

    def __exit__(self, *args):
        sys.stdout = self.stdout
        if self.file:
            self.file.close()

    def write(self, s):
        self.stdout.write(s)
        if self.file:
            self.file.write(s)
            self.file.flush()

    def flush(self):
        self.stdout.flush()
        if self.file:
            self.file.flush()


def main():
    from src.scraper.netkeiba import NetkeibaClient, RaceListScraper
    from src.scraper.course_db_collector import collect_course_db_from_results

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    start = "2024-01-01"
    end = today

    log_dir = os.path.join(PROJECT_ROOT, "log")
    log_path = os.path.join(
        log_dir,
        f"overnight_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )

    client = NetkeibaClient(no_cache=True)
    race_list = RaceListScraper(client)

    start_time = None
    last_pct_logged = -5

    def progress(day_i, total_days, total_runs, cur_date, st):
        nonlocal start_time, last_pct_logged
        if start_time is None:
            start_time = datetime.datetime.now()

        ts = datetime.datetime.now().strftime("%H:%M:%S")
        base_msg = f"[{ts}] {day_i}/{total_days}日 ({total_runs}走) {cur_date}"

        # 5%刻みで進捗・残り時間を表示
        if total_days > 0:
            pct = 100.0 * day_i / total_days
            if pct >= last_pct_logged + 5 or st in ("completed", "already_done"):
                last_pct_logged = int(pct // 5) * 5
                elapsed_sec = (datetime.datetime.now() - start_time).total_seconds()
                elapsed_min = int(elapsed_sec // 60)

                if pct > 0 and st == "running":
                    total_est_sec = elapsed_sec / (pct / 100)
                    remaining_sec = max(0, total_est_sec - elapsed_sec)
                    remaining_min = int(remaining_sec // 60)
                    msg = f"{base_msg}  |  {pct:.1f}%完了  経過:{elapsed_min}分  残り約:{remaining_min}分"
                elif st in ("completed", "already_done"):
                    msg = f"{base_msg}  |  100%完了  経過:{elapsed_min}分  {st}"
                else:
                    msg = f"{base_msg}  |  {pct:.1f}%完了  経過:{elapsed_min}分"
                print(msg)
        else:
            print(f"{base_msg}  {st}")
        sys.stdout.flush()

    try:
        with TeeWriter(log_path) as tee:
            sys.stdout = tee
            print("=" * 60)
            print("夜間スクレイピング: 基準タイムDB収集")
            print(f"  期間: {start} ～ {end}")
            print(f"  保存先: {COURSE_DB_PRELOAD_PATH}")
            print(f"  ログ: {log_path}")
            print("  モード: resume（途中再開対応）")
            print("=" * 60)
            print("PCの電源を入れたまま放置してください。")
            print()
            n = collect_course_db_from_results(
                client, race_list, start, end,
                COURSE_DB_PRELOAD_PATH,
                state_path=COURSE_DB_COLLECTOR_STATE_PATH,
                mode="resume",
                progress_callback=progress,
            )
            print()
            print("=" * 60)
            print(f"完了: 今回 {n}走 を追加しました")
            print("=" * 60)
    except KeyboardInterrupt:
        print("\n[中断] Ctrl+C で停止。再度実行で続きから再開できます。")
        sys.exit(130)
    except Exception as e:
        print(f"\n[エラー] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
