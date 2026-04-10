"""
自動スケジューラー — 予想作成・オッズ更新・結果取得・DB更新

Usage:
  python scheduler.py                    # フォアグラウンド実行
  python scheduler.py --status           # 次回実行予定を表示
  python scheduler.py --run prediction   # 手動: 翌日の予想作成
  python scheduler.py --run odds         # 手動: 当日のオッズ更新
  python scheduler.py --run results      # 手動: 前日の結果取得+DB更新
"""
import argparse
import json
import os
import subprocess
import sys
import io
import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from config.settings import PROJECT_ROOT

# ── ログ設定 ──
LOG_DIR = os.path.join(PROJECT_ROOT, "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("scheduler")
logger.setLevel(logging.INFO)
# ファイルハンドラ（日次ローテーション、7日分保持）
fh = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, "scheduler.log"),
    when="midnight", backupCount=7, encoding="utf-8",
)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh)
# コンソール
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)


# ================================================================
# ジョブ関数
# ================================================================

def job_prediction():
    """翌日の予想を作成（開催日のみ）"""
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info("━━ 予想作成ジョブ開始: %s ━━", tomorrow)

    from src.scheduler_tasks import get_race_ids
    race_ids = get_race_ids(tomorrow)
    if not race_ids:
        logger.info("%s: 開催なし、スキップ", tomorrow)
        return

    logger.info("%s: %dレース検出、分析開始", tomorrow, len(race_ids))
    result = subprocess.run(
        [sys.executable, "run_analysis_date.py", tomorrow],
        cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode == 0:
        logger.info("予想作成完了: %s", tomorrow)
    else:
        logger.error("予想作成失敗: %s\n%s", tomorrow, result.stderr[-500:] if result.stderr else "")


def job_odds_morning():
    """当日6:00のオッズ更新 + 発走前ジョブ動的登録"""
    today = datetime.now().strftime("%Y-%m-%d")
    date_key = today.replace("-", "")
    logger.info("━━ オッズ更新(定時)ジョブ開始: %s ━━", today)

    from src.scheduler_tasks import run_odds_update
    count = run_odds_update(date_key)
    if count == 0:
        logger.info("オッズ更新: 対象なし")
        return

    logger.info("オッズ更新完了: %dレース", count)

    # 発走15分前ジョブを動的登録
    _schedule_pre_race_odds(date_key)


def _schedule_pre_race_odds(date_key: str):
    """各レースの発走15分前にオッズ更新ジョブを登録"""
    from src.scheduler_tasks import get_post_times
    post_times = get_post_times(date_key)
    if not post_times:
        return

    now = datetime.now()
    registered = 0
    for race_id, pt_str in post_times.items():
        try:
            h, m = map(int, pt_str.split(":"))
            target = now.replace(hour=h, minute=m, second=0, microsecond=0) - timedelta(minutes=15)
            if target <= now:
                continue
            _scheduler.add_job(
                _job_odds_pre_race,
                "date", run_date=target,
                args=[date_key, race_id],
                id=f"odds_pre_{race_id}",
                replace_existing=True,
            )
            registered += 1
        except Exception as e:
            logger.debug("発走前ジョブ登録失敗 %s: %s", race_id, e)

    logger.info("発走15分前ジョブ: %d/%d件登録", registered, len(post_times))


def _job_odds_pre_race(date_key: str, race_id: str):
    """発走15分前の個別レースオッズ更新"""
    logger.info("━━ オッズ更新(発走前): %s ━━", race_id)
    from src.scheduler_tasks import run_odds_update
    # 全レース更新（個別フィルタは将来対応）
    run_odds_update(date_key)


def job_results_and_db():
    """前日の結果取得 + DB更新"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    date_key = yesterday.replace("-", "")
    pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")

    if not os.path.exists(pred_file):
        logger.info("結果取得: %s の予想データなし、スキップ", yesterday)
        return

    logger.info("━━ 結果取得+DB更新ジョブ開始: %s ━━", yesterday)

    # 結果取得
    result = subprocess.run(
        [sys.executable, "run_results.py", yesterday],
        cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode == 0:
        logger.info("結果取得完了: %s", yesterday)
    else:
        logger.error("結果取得失敗: %s\n%s", yesterday, result.stderr[-500:] if result.stderr else "")

    # DB更新
    from src.scheduler_tasks import run_db_update
    logs = run_db_update(yesterday)
    for entry in logs:
        logger.info("  %s", entry)


# ================================================================
# スケジューラー
# ================================================================

_scheduler: BlockingScheduler = None


def _on_job_event(event):
    """ジョブ実行後のログ出力"""
    if event.exception:
        logger.error("ジョブ失敗 [%s]: %s", event.job_id, event.exception)
    else:
        logger.info("ジョブ完了 [%s]", event.job_id)


def build_scheduler() -> BlockingScheduler:
    """スケジューラーを構築して返す"""
    global _scheduler
    sched = BlockingScheduler(timezone="Asia/Tokyo")

    # 1. 予想作成: 毎日 17:00
    sched.add_job(job_prediction, "cron", hour=17, minute=0, id="prediction",
                  name="予想作成(翌日)", misfire_grace_time=3600)

    # 2. オッズ更新(定時): 毎日 6:00
    sched.add_job(job_odds_morning, "cron", hour=6, minute=0, id="odds_morning",
                  name="オッズ更新(定時6:00)", misfire_grace_time=3600)

    # 3. 結果取得 + DB更新: 毎日 0:00
    sched.add_job(job_results_and_db, "cron", hour=0, minute=0, id="results_db",
                  name="結果取得+DB更新(前日)", misfire_grace_time=3600)

    sched.add_listener(_on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    _scheduler = sched
    return sched


def show_status():
    """次回実行予定を表示"""
    # BackgroundScheduler で起動してnext_run_timeを取得
    sched = BackgroundScheduler(timezone="Asia/Tokyo")
    sched.add_job(job_prediction, "cron", hour=17, minute=0, id="prediction",
                  name="予想作成(翌日)", misfire_grace_time=3600)
    sched.add_job(job_odds_morning, "cron", hour=6, minute=0, id="odds_morning",
                  name="オッズ更新(定時6:00)", misfire_grace_time=3600)
    sched.add_job(job_results_and_db, "cron", hour=0, minute=0, id="results_db",
                  name="結果取得+DB更新(前日)", misfire_grace_time=3600)
    sched.start()
    jobs = sched.get_jobs()
    print(f"\n{'='*60}")
    print("  keiba-v3 自動スケジューラー — ジョブ一覧")
    print(f"{'='*60}")
    if not jobs:
        print("  登録ジョブなし")
    else:
        for job in sorted(jobs, key=lambda j: str(j.next_run_time or "")):
            nrt = job.next_run_time
            nrt_str = nrt.strftime("%Y-%m-%d %H:%M:%S") if nrt else "—"
            print(f"  [{job.id:20s}] {job.name:20s}  次回: {nrt_str}")
    print(f"{'='*60}\n")
    sched.shutdown(wait=False)


def run_manual(task_name: str):
    """手動でジョブを実行"""
    tasks = {
        "prediction": ("予想作成", job_prediction),
        "odds": ("オッズ更新", job_odds_morning),
        "results": ("結果取得+DB更新", job_results_and_db),
    }
    if task_name not in tasks:
        print(f"不明なタスク: {task_name}")
        print(f"利用可能: {', '.join(tasks.keys())}")
        return

    label, func = tasks[task_name]
    logger.info("手動実行: %s", label)
    try:
        func()
        logger.info("手動実行完了: %s", label)
    except Exception as e:
        logger.error("手動実行失敗: %s — %s", label, e, exc_info=True)


# ================================================================
# メイン
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="keiba-v3 自動スケジューラー")
    parser.add_argument("--status", action="store_true", help="次回実行予定を表示")
    parser.add_argument("--run", type=str, metavar="TASK",
                        help="手動実行 (prediction/odds/results)")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.run:
        run_manual(args.run)
        return

    # フォアグラウンド実行
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  keiba-v3 自動スケジューラー起動")
    logger.info("  予想作成: 毎日 17:00")
    logger.info("  オッズ更新: 毎日 6:00 + 発走15分前")
    logger.info("  結果取得+DB更新: 毎日 0:00")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    sched = build_scheduler()
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("スケジューラー停止")


if __name__ == "__main__":
    main()
