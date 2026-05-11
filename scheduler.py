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
import io
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler  # --status の next_run_time 取得用 (L264)
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

import atexit

from config.settings import PROJECT_ROOT

# プロセス終了時に DB 接続を確実にクローズ（コネクションリーク防止）
from src.database import close_db as _close_db
atexit.register(_close_db)

# netkeiba cooldown 感知ユーティリティ (フェーズ D 段階 1)
from src.scraper.netkeiba import _is_netkeiba_cooldown_active, _get_netkeiba_cooldown_remaining
# Slack 通知 (フェーズ D 段階 2-B)
from src.slack_notify import send_slack

# DAG 依存管理 (フェーズ D 段階 2-A)
from src.scheduler_dag import (
    register_task,
    can_run as dag_can_run,
    mark_done as dag_mark_done,
    mark_failed as dag_mark_failed,
    reset_state as dag_reset_state,
    get_dag_state,
    validate_dag,
    topological_order,
)

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
# netkeiba cooldown ヘルパ
# ================================================================

# DAG 待機の最大リトライ回数 (30 秒 × 20 = 10 分上限)
# 日次 reset 後に依存タスクが永続的に未完了になる場合の無限ループ防止
_DAG_WAIT_MAX_RETRIES = 20


def _reschedule_dag_wait(
    job_name: str,
    original_job_func,
    *args,
    _retry_count: int = 0,
) -> None:
    """DAG 依存未完了時に 30 秒後へ再予約する。

    cooldown 延期と同じパターンで replace_existing=True により積み上がりを防ぐ。

    無限ループ対策: 日次 reset 後に依存タスクが永続的に未完了になっても、
    _DAG_WAIT_MAX_RETRIES (10 分) で打ち切って ERROR ログ + slack 通知。
    """
    if _retry_count >= _DAG_WAIT_MAX_RETRIES:
        logger.error(
            "DAG 待機タイムアウト: ジョブ %s が %d 回リトライ後も依存未解決。手動確認が必要です",
            job_name, _DAG_WAIT_MAX_RETRIES,
        )
        try:
            send_slack(
                message=(
                    f"ジョブ '{job_name}' が DAG 依存待機で {_DAG_WAIT_MAX_RETRIES} 回 "
                    f"リトライ後もタイムアウトしました。"
                    f"依存タスクの状態を確認してください。"
                ),
                level="critical",
                title="[scheduler] DAG 待機タイムアウト",
            )
        except Exception as _se:
            logger.warning("Slack 通知失敗 (無視して続行): %s", _se)
        return

    run_at = datetime.now() + timedelta(seconds=30)
    run_at_str = run_at.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(
        "DAG 待機: ジョブ %s を %s に再予約 (30 秒後・retry %d/%d)",
        job_name, run_at_str, _retry_count + 1, _DAG_WAIT_MAX_RETRIES,
    )
    if _scheduler is not None:
        try:
            _scheduler.add_job(
                original_job_func,
                trigger="date",
                run_date=run_at,
                args=list(args),
                kwargs={"_retry_count": _retry_count + 1},
                id=f"{job_name}_dag_wait",
                replace_existing=True,
                misfire_grace_time=600,
            )
        except Exception as e:
            logger.warning("ジョブ %s の DAG 待機再予約失敗: %s", job_name, e)
    else:
        logger.warning(
            "ジョブ %s: スケジューラ未初期化のため DAG 待機再予約不可。依存解消後に手動再実行してください",
            job_name,
        )


def _check_cooldown_and_reschedule(job_name: str, original_job_func, *args) -> bool:
    """
    netkeiba cooldown 中なら True を返してジョブを延期予約し、呼び出し元はすぐ return する。
    cooldown なし → False を返すので通常処理を続行する。

    延期タイミング: cooldown 残り秒数 + 5 秒後 ('date' トリガー)
    """
    if not _is_netkeiba_cooldown_active():
        return False  # クールダウンなし → 通常実行

    remaining = _get_netkeiba_cooldown_remaining()
    run_at = datetime.now() + timedelta(seconds=remaining + 5)
    run_at_str = run_at.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(
        "netkeiba cooldown 中 (残 %ds) のためジョブ %s を %s に延期しました",
        remaining, job_name, run_at_str,
    )

    if _scheduler is not None:
        try:
            _scheduler.add_job(
                original_job_func,
                trigger="date",
                run_date=run_at,
                args=list(args),
                # ID は固定 ("_retry" 含めるが timestamp 含めない):
                # 24h cooldown 中に複数回 fire しても retry job が積み上がらず、
                # 常に最後の 1 件だけが有効 (replace_existing=True で上書き)。
                id=f"{job_name}_retry",
                replace_existing=True,
                misfire_grace_time=600,
            )
        except Exception as e:
            logger.warning("ジョブ %s の延期予約失敗: %s", job_name, e)
    else:
        # スケジューラ未初期化 (CLI --run 等の単発実行) では延期予約不可。
        # 呼び出し元がジョブを実行せず終了するため、運用者に手動再実行を促す。
        logger.warning(
            "ジョブ %s: スケジューラ未初期化のため延期不可。"
            " cooldown 解除後に手動再実行してください (残 %ds)",
            job_name, remaining,
        )

    # Slack 通知: 10分以上の延期のみ通知 (spam 防止)
    if remaining >= 600:
        try:
            send_slack(
                message=(
                    f"netkeiba cooldown 中 (残 {remaining} 秒) のため"
                    f"ジョブ '{job_name}' を {run_at_str} に延期しました。"
                ),
                level="warning",
                title="[scheduler] netkeiba cooldown のためジョブ延期",
            )
        except Exception as _se:
            logger.warning("Slack 通知失敗 (無視して続行): %s", _se)

    return True  # クールダウン中 → 呼び出し元は即 return


# ================================================================
# ジョブ関数
# ================================================================

def job_prediction():
    """翌日の予想を作成（開催日のみ）"""
    # netkeiba cooldown 中なら延期して即 return
    if _check_cooldown_and_reschedule("job_prediction", job_prediction):
        return

    # DAG 依存チェック (依存なしだが一貫性のためチェック)
    if not dag_can_run("job_prediction"):
        _reschedule_dag_wait("job_prediction", job_prediction)
        return

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
        dag_mark_done("job_prediction")
    else:
        logger.error("予想作成失敗: %s\n%s", tomorrow, result.stderr[-500:] if result.stderr else "")
        dag_mark_failed("job_prediction")


def job_odds_morning():
    """当日6:00のオッズ更新 + 発走前ジョブ動的登録"""
    # netkeiba cooldown 中なら延期して即 return
    if _check_cooldown_and_reschedule("job_odds_morning", job_odds_morning):
        return

    # DAG 依存チェック (依存なしだが一貫性のためチェック)
    if not dag_can_run("job_odds_morning"):
        _reschedule_dag_wait("job_odds_morning", job_odds_morning)
        return

    today = datetime.now().strftime("%Y-%m-%d")
    date_key = today.replace("-", "")
    logger.info("━━ オッズ更新(定時)ジョブ開始: %s ━━", today)

    from src.scheduler_tasks import run_odds_update
    try:
        count = run_odds_update(date_key)
    except Exception as e:
        logger.error("オッズ更新例外: %s", e)
        dag_mark_failed("job_odds_morning")
        return
    if count == 0:
        logger.info("オッズ更新: 対象なし")
        dag_mark_done("job_odds_morning")
        return

    logger.info("オッズ更新完了: %dレース", count)
    dag_mark_done("job_odds_morning")

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
    # netkeiba cooldown 中なら延期して即 return
    if _check_cooldown_and_reschedule("job_results_and_db", job_results_and_db):
        return

    # DAG 依存チェック: job_odds_morning 完了待ち
    if not dag_can_run("job_results_and_db"):
        from src.scheduler_dag import get_pending_deps
        pending = get_pending_deps("job_results_and_db")
        logger.info("DAG 依存未完了のため job_results_and_db を 30 秒後に延期: pending=%s", pending)
        _reschedule_dag_wait("job_results_and_db", job_results_and_db)
        return

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
        # DB 更新は結果取得が成功したときだけ実施 (失敗時の不正データで unblock 防止)
        from src.scheduler_tasks import run_db_update
        logs = run_db_update(yesterday)
        for entry in logs:
            logger.info("  %s", entry)
        # ジョブ完了 → DAG mark_done で下流タスクを unblock
        dag_mark_done("job_results_and_db")
    else:
        logger.error("結果取得失敗: %s\n%s", yesterday, result.stderr[-500:] if result.stderr else "")
        dag_mark_failed("job_results_and_db")


# ================================================================
# スケジューラー
# ================================================================

_scheduler: Optional[BlockingScheduler] = None


def _on_job_event(event):
    """ジョブ実行後のログ出力"""
    if event.exception:
        logger.error("ジョブ失敗 [%s]: %s", event.job_id, event.exception)
    else:
        logger.info("ジョブ完了 [%s]", event.job_id)


def _register_dag_tasks() -> None:
    """主要ジョブを DAG に登録する (モジュールロード時 / build_scheduler 時に呼ぶ)。

    既に登録済みの場合は上書き (register_task の仕様) するが実害なし。
    """
    register_task("job_prediction", deps=[])
    register_task("job_odds_morning", deps=[])
    # job_results_and_db はオッズ確定後に実行 (job_odds_morning 完了待ち)
    register_task("job_results_and_db", deps=["job_odds_morning"])
    logger.info("DAG: 主要ジョブ登録完了 (order=%s)", topological_order())


def build_scheduler() -> BlockingScheduler:
    """スケジューラーを構築して返す"""
    global _scheduler
    sched = BlockingScheduler(timezone="Asia/Tokyo")

    # DAG タスク登録
    _register_dag_tasks()

    # 1. 予想作成: 毎日 17:05 (Windows TS Predict_Tomorrow 17:00 の 5 分後・競合回避)
    sched.add_job(job_prediction, "cron", hour=17, minute=5, id="prediction",
                  name="予想作成(翌日)", misfire_grace_time=3600)

    # 2. オッズ更新(定時): 毎日 6:05 (Windows TS Predict 06:00 の 5 分後・競合回避)
    sched.add_job(job_odds_morning, "cron", hour=6, minute=5, id="odds_morning",
                  name="オッズ更新(定時6:05)", misfire_grace_time=3600)

    # 3. 結果取得 + DB更新: 毎日 0:00
    sched.add_job(job_results_and_db, "cron", hour=0, minute=0, id="results_db",
                  name="結果取得+DB更新(前日)", misfire_grace_time=3600)

    # 4. DAG 完了状態リセット: 毎日 0:01 (結果取得ジョブの直後)
    sched.add_job(dag_reset_state, "cron", hour=0, minute=1, id="dag_reset",
                  name="DAG 完了状態リセット(日次)", misfire_grace_time=3600)

    sched.add_listener(_on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    _scheduler = sched
    return sched


def show_status():
    """次回実行予定を表示"""
    # BackgroundScheduler で起動してnext_run_timeを取得
    sched = BackgroundScheduler(timezone="Asia/Tokyo")
    sched.add_job(job_prediction, "cron", hour=17, minute=5, id="prediction",
                  name="予想作成(翌日)", misfire_grace_time=3600)
    sched.add_job(job_odds_morning, "cron", hour=6, minute=5, id="odds_morning",
                  name="オッズ更新(定時6:05)", misfire_grace_time=3600)
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

    # ---- DAG 状態表示 (B-3) ----
    _register_dag_tasks()
    dag_state = get_dag_state()
    problems = validate_dag()
    order = topological_order()
    print(f"{'='*60}")
    print("  DAG 依存関係ステータス")
    print(f"{'='*60}")
    print(f"  登録タスク数  : {dag_state['registered_count']}")
    print(f"  完了タスク数  : {dag_state['completed_count']}")
    print(f"  循環依存      : {'なし' if not problems else f'あり ({problems})'}")
    print(f"  実行順序      : {' → '.join(order)}")
    for task in dag_state["registered"]:
        deps = dag_state["deps"].get(task, [])
        completed = task in dag_state["completed"]
        pending = [d for d in deps if d not in dag_state["completed"]]
        status = "完了" if completed else ("実行可" if not pending else f"待機中({pending})")
        print(f"  [{task:30s}] deps={str(deps or 'なし'):20}  状態: {status}")
    print(f"{'='*60}\n")


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

    # 二重起動防止 (pidfile ロック)
    pidfile = os.path.join(PROJECT_ROOT, "data", "scheduler.pid")
    if os.path.isfile(pidfile):
        try:
            old_pid = int(open(pidfile, "r").read().strip())
            # プロセスが生存しているか確認
            import psutil
            if psutil.pid_exists(old_pid):
                try:
                    proc = psutil.Process(old_pid)
                    if "python" in proc.name().lower():
                        logger.warning("スケジューラー既に起動中 (PID %d)、終了します", old_pid)
                        return
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except (ValueError, ImportError):
            pass

    # PID 記録
    with open(pidfile, "w") as f:
        f.write(str(os.getpid()))

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  keiba-v3 自動スケジューラー起動 (PID %d)", os.getpid())
    logger.info("  予想作成: 毎日 17:05")
    logger.info("  オッズ更新: 毎日 6:05 + 発走15分前")
    logger.info("  結果取得+DB更新: 毎日 0:00")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    sched = build_scheduler()
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("スケジューラー停止")
    finally:
        try:
            os.remove(pidfile)
        except OSError:
            pass


if __name__ == "__main__":
    main()
