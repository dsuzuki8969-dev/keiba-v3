"""
自動スケジューラー — 予想作成・オッズ更新・パラフレーズ・結果取得・DB更新・メンテナンス

スケジュール (v3 — 最適時刻版):
  夜間サイクル:
    23:00  当日結果取得+DB更新           job_results_and_db
    23:30  翌日予想作成(当日結果反映)     job_prediction      [deps: results_and_db]
    00:00  パラフレーズ(翌日)            job_paraphrase_tomorrow [deps: prediction]
    01:00  日次メンテナンス              job_maintenance     [deps: results_and_db]
    04:00  DAG+リトライ リセット
  朝〜日中サイクル:
    06:00  当日予想作成                  job_predict_today
    06:00  オッズ一括更新(初回)          job_odds_first_batch [deps: predict_today]
    06:30  パラフレーズ(当日)            job_paraphrase_today [deps: predict_today, odds_first_batch]
    09:00  オッズ一括更新(2回目)         job_odds_batch
    12:00  オッズ一括更新(3回目)         job_odds_batch
    15:00  オッズ一括更新(4回目)         job_odds_batch
    18:00  オッズ一括更新(5回目)         job_odds_batch
  動的レースオッズ (06:00 初回更新時に登録):
    T-15min  発走15分前オッズ更新
    T-0min   発走時刻オッズ更新 (最終オッズ)

Usage:
  python scheduler.py                            # フォアグラウンド実行
  python scheduler.py --status                   # 次回実行予定を表示
  python scheduler.py --run predict_today        # 手動: 当日の予想作成
  python scheduler.py --run prediction           # 手動: 翌日の予想作成
  python scheduler.py --run odds                 # 手動: オッズ一括更新
  python scheduler.py --run paraphrase_today     # 手動: 当日パラフレーズ
  python scheduler.py --run paraphrase_tomorrow  # 手動: 翌日パラフレーズ
  python scheduler.py --run results              # 手動: 当日の結果取得+DB更新
  python scheduler.py --run maintenance          # 手動: 日次メンテナンス
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
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

import atexit

from config.settings import PROJECT_ROOT

from src.database import close_db as _close_db
atexit.register(_close_db)

from src.scraper.netkeiba import _is_netkeiba_cooldown_active, _get_netkeiba_cooldown_remaining
from src.slack_notify import send_slack

from src.scheduler_dag import (
    register_task,
    can_run as dag_can_run,
    mark_done as dag_mark_done,
    mark_failed as dag_mark_failed,
    reset_state as dag_reset_state,
    get_dag_state,
    validate_dag,
    topological_order,
    is_blocked as dag_is_blocked,
    load_state as dag_load_state,
    clear_failure as dag_clear_failure,
)

# ── ログ設定 ──
LOG_DIR = os.path.join(PROJECT_ROOT, "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("scheduler")
logger.setLevel(logging.INFO)
fh = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, "scheduler.log"),
    when="midnight", backupCount=7, encoding="utf-8",
)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)


# ================================================================
# ヘルパー
# ================================================================

# DAG 待機の最大リトライ回数 (30 秒 × 40 = 20 分上限)
_DAG_WAIT_MAX_RETRIES = 40

# 失敗リトライ遅延 (秒): 5分 → 15分 → 30分
_RETRY_DELAYS = [300, 900, 1800]
_RETRY_COUNTS: dict = {}


def _reschedule_dag_wait(
    job_name: str,
    original_job_func,
    *args,
    _retry_count: int = 0,
) -> None:
    """DAG 依存未完了時に 30 秒後へ再予約する。"""
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


def _reschedule_retry(job_name: str, job_func) -> None:
    """ジョブ失敗時にリトライを予約する (5分→15分→30分)。"""
    dag_clear_failure(job_name)

    count = _RETRY_COUNTS.get(job_name, 0)
    if count < len(_RETRY_DELAYS):
        delay = _RETRY_DELAYS[count]
        run_at = datetime.now() + timedelta(seconds=delay)
        run_at_str = run_at.strftime("%Y-%m-%d %H:%M:%S")
        _RETRY_COUNTS[job_name] = count + 1
        logger.info(
            "リトライ予約: ジョブ %s を %s に再実行 (遅延 %d秒・retry %d/%d)",
            job_name, run_at_str, delay, count + 1, len(_RETRY_DELAYS),
        )
        if _scheduler is not None:
            try:
                _scheduler.add_job(
                    job_func,
                    trigger="date",
                    run_date=run_at,
                    id=f"{job_name}_failure_retry",
                    replace_existing=True,
                    misfire_grace_time=3600,
                )
            except Exception as e:
                logger.warning("ジョブ %s のリトライ予約失敗: %s", job_name, e)
        else:
            logger.warning(
                "ジョブ %s: スケジューラ未初期化のためリトライ予約不可。手動再実行してください",
                job_name,
            )
        try:
            send_slack(
                message=(
                    f"ジョブ '{job_name}' が失敗しました。"
                    f"{delay // 60} 分後 ({run_at_str}) にリトライ予約済 "
                    f"(retry {count + 1}/{len(_RETRY_DELAYS)})"
                ),
                level="warning",
                title="[scheduler] ジョブ失敗 — リトライ予約",
            )
        except Exception as _se:
            logger.warning("Slack 通知失敗 (無視して続行): %s", _se)
    else:
        logger.error(
            "最大リトライ回数超過: ジョブ %s は %d 回リトライ済。手動介入が必要です",
            job_name, len(_RETRY_DELAYS),
        )
        try:
            send_slack(
                message=(
                    f"ジョブ '{job_name}' が {len(_RETRY_DELAYS)} 回リトライしても失敗しました。"
                    f"手動介入が必要です。"
                ),
                level="critical",
                title="[scheduler] 最大リトライ超過 — 手動介入必要",
            )
        except Exception as _se:
            logger.warning("Slack 通知失敗 (無視して続行): %s", _se)


def _check_cooldown_and_reschedule(job_name: str, original_job_func, *args) -> bool:
    """netkeiba cooldown 中なら True を返してジョブを延期予約し、呼び出し元はすぐ return する。"""
    if not _is_netkeiba_cooldown_active():
        return False

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
                id=f"{job_name}_retry",
                replace_existing=True,
                misfire_grace_time=600,
            )
        except Exception as e:
            logger.warning("ジョブ %s の延期予約失敗: %s", job_name, e)
    else:
        logger.warning(
            "ジョブ %s: スケジューラ未初期化のため延期不可。"
            " cooldown 解除後に手動再実行してください (残 %ds)",
            job_name, remaining,
        )

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

    return True


# ================================================================
# ジョブ関数
# ================================================================

def job_predict_today():
    """当日の予想を作成"""
    if _check_cooldown_and_reschedule("job_predict_today", job_predict_today):
        return
    blocked_by = dag_is_blocked("job_predict_today")
    if blocked_by:
        logger.warning("上流タスク %s の失敗により job_predict_today をスキップ", blocked_by)
        dag_mark_failed("job_predict_today")
        return
    if not dag_can_run("job_predict_today"):
        _reschedule_dag_wait("job_predict_today", job_predict_today)
        return

    today = datetime.now().strftime("%Y-%m-%d")
    logger.info("━━ 当日予想作成ジョブ開始: %s ━━", today)

    from src.scheduler_tasks import get_race_ids
    race_ids = get_race_ids(today)
    if not race_ids:
        logger.info("%s: 開催なし、スキップ", today)
        dag_mark_done("job_predict_today")
        _RETRY_COUNTS.pop("job_predict_today", None)
        return

    logger.info("%s: %dレース検出、分析開始", today, len(race_ids))
    result = subprocess.run(
        [sys.executable, "run_analysis_date.py", today],
        cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode == 0:
        logger.info("当日予想作成完了: %s", today)
        dag_mark_done("job_predict_today")
        _RETRY_COUNTS.pop("job_predict_today", None)
    else:
        logger.error("当日予想作成失敗: %s\n%s", today, result.stderr[-500:] if result.stderr else "")
        dag_mark_failed("job_predict_today")
        _reschedule_retry("job_predict_today", job_predict_today)


def job_prediction():
    """翌日の予想を作成 (deps: job_results_and_db — 当日結果反映済)"""
    if _check_cooldown_and_reschedule("job_prediction", job_prediction):
        return
    blocked_by = dag_is_blocked("job_prediction")
    if blocked_by:
        logger.warning("上流タスク %s の失敗により job_prediction をスキップ", blocked_by)
        dag_mark_failed("job_prediction")
        return
    if not dag_can_run("job_prediction"):
        _reschedule_dag_wait("job_prediction", job_prediction)
        return

    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info("━━ 予想作成ジョブ開始: %s ━━", tomorrow)

    from src.scheduler_tasks import get_race_ids
    race_ids = get_race_ids(tomorrow)
    if not race_ids:
        logger.info("%s: 開催なし、スキップ", tomorrow)
        dag_mark_done("job_prediction")
        _RETRY_COUNTS.pop("job_prediction", None)
        return

    logger.info("%s: %dレース検出、分析開始", tomorrow, len(race_ids))
    result = subprocess.run(
        [sys.executable, "run_analysis_date.py", tomorrow],
        cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode == 0:
        logger.info("予想作成完了: %s", tomorrow)
        dag_mark_done("job_prediction")
        _RETRY_COUNTS.pop("job_prediction", None)
    else:
        logger.error("予想作成失敗: %s\n%s", tomorrow, result.stderr[-500:] if result.stderr else "")
        dag_mark_failed("job_prediction")
        _reschedule_retry("job_prediction", job_prediction)


def job_odds_first_batch():
    """当日初回オッズ一括更新 + 個別レースオッズジョブ登録 (deps: job_predict_today)"""
    if _check_cooldown_and_reschedule("job_odds_first_batch", job_odds_first_batch):
        return
    blocked_by = dag_is_blocked("job_odds_first_batch")
    if blocked_by:
        logger.warning("上流タスク %s の失敗により job_odds_first_batch をスキップ", blocked_by)
        dag_mark_failed("job_odds_first_batch")
        return
    if not dag_can_run("job_odds_first_batch"):
        _reschedule_dag_wait("job_odds_first_batch", job_odds_first_batch)
        return

    today = datetime.now().strftime("%Y-%m-%d")
    date_key = today.replace("-", "")
    logger.info("━━ オッズ一括更新(初回 06:00)ジョブ開始: %s ━━", today)

    from src.scheduler_tasks import run_odds_update
    try:
        count = run_odds_update(date_key)
    except Exception as e:
        logger.error("オッズ更新例外: %s", e)
        dag_mark_failed("job_odds_first_batch")
        _reschedule_retry("job_odds_first_batch", job_odds_first_batch)
        return
    if count == 0:
        logger.info("オッズ更新: 対象なし")
        dag_mark_done("job_odds_first_batch")
        _RETRY_COUNTS.pop("job_odds_first_batch", None)
        return

    logger.info("オッズ一括更新(初回)完了: %dレース", count)
    dag_mark_done("job_odds_first_batch")
    _RETRY_COUNTS.pop("job_odds_first_batch", None)

    # 発走15分前 + 発走時刻のオッズジョブを動的登録
    _schedule_race_odds(date_key)


def _schedule_race_odds(date_key: str):
    """各レースの発走15分前 + 発走時刻にオッズ更新ジョブを登録"""
    from src.scheduler_tasks import get_post_times
    post_times = get_post_times(date_key)
    if not post_times:
        return

    now = datetime.now()
    registered = 0
    for race_id, pt_str in post_times.items():
        try:
            h, m = map(int, pt_str.split(":"))
            post_time = now.replace(hour=h, minute=m, second=0, microsecond=0)

            # T-15min: 発走15分前
            pre_target = post_time - timedelta(minutes=15)
            if pre_target > now:
                _scheduler.add_job(
                    _job_odds_race_event, "date", run_date=pre_target,
                    args=[date_key, race_id, "pre"],
                    id=f"odds_pre_{race_id}", replace_existing=True,
                )
                registered += 1

            # T-0min: 発走時刻 (最終オッズ)
            if post_time > now:
                _scheduler.add_job(
                    _job_odds_race_event, "date", run_date=post_time,
                    args=[date_key, race_id, "post"],
                    id=f"odds_post_{race_id}", replace_existing=True,
                )
                registered += 1
        except Exception as e:
            logger.debug("レースオッズジョブ登録失敗 %s: %s", race_id, e)

    logger.info("個別レースオッズジョブ: %d件登録 (T-15min + T-0min)", registered)


def _job_odds_race_event(date_key: str, race_id: str, event_type: str = "pre"):
    """個別レースオッズ更新 (発走15分前 or 発走時刻)"""
    label = "発走前" if event_type == "pre" else "発走時"
    logger.info("━━ オッズ更新(%s): %s ━━", label, race_id)
    from src.scheduler_tasks import run_odds_update
    run_odds_update(date_key)


def job_odds_batch():
    """定時オッズ一括更新 (09:00/12:00/15:00/18:00、DAG管理なし)"""
    today = datetime.now().strftime("%Y%m%d")
    pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{today}_pred.json")
    if not os.path.exists(pred_file):
        return

    logger.info("━━ オッズ一括更新(定時)ジョブ開始 ━━")
    from src.scheduler_tasks import run_odds_update
    try:
        count = run_odds_update(today)
        if count > 0:
            logger.info("オッズ一括更新(定時)完了: %dレース", count)
        else:
            logger.info("オッズ一括更新: 対象なし")
    except Exception as e:
        logger.warning("オッズ一括更新エラー: %s", e)


def job_paraphrase_today():
    """当日のパラフレーズを生成 (deps: job_predict_today, job_odds_first_batch)"""
    blocked_by = dag_is_blocked("job_paraphrase_today")
    if blocked_by:
        logger.warning("上流タスク %s の失敗により job_paraphrase_today をスキップ", blocked_by)
        dag_mark_failed("job_paraphrase_today")
        return
    if not dag_can_run("job_paraphrase_today"):
        _reschedule_dag_wait("job_paraphrase_today", job_paraphrase_today)
        return

    date_key = datetime.now().strftime("%Y%m%d")
    logger.info("━━ パラフレーズ(当日)ジョブ開始: %s ━━", date_key)

    script_path = os.path.join(PROJECT_ROOT, "scripts", "local_llm_paraphrase.py")
    result = subprocess.run(
        [sys.executable, script_path, date_key],
        cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode == 0:
        logger.info("パラフレーズ(当日)完了: %s", date_key)
        dag_mark_done("job_paraphrase_today")
        _RETRY_COUNTS.pop("job_paraphrase_today", None)
    else:
        logger.error("パラフレーズ(当日)失敗: %s\n%s", date_key, result.stderr[-500:] if result.stderr else "")
        dag_mark_failed("job_paraphrase_today")
        _reschedule_retry("job_paraphrase_today", job_paraphrase_today)


def job_paraphrase_tomorrow():
    """翌日のパラフレーズを生成 (deps: job_prediction)"""
    blocked_by = dag_is_blocked("job_paraphrase_tomorrow")
    if blocked_by:
        logger.warning("上流タスク %s の失敗により job_paraphrase_tomorrow をスキップ", blocked_by)
        dag_mark_failed("job_paraphrase_tomorrow")
        return
    if not dag_can_run("job_paraphrase_tomorrow"):
        _reschedule_dag_wait("job_paraphrase_tomorrow", job_paraphrase_tomorrow)
        return

    date_key = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
    logger.info("━━ パラフレーズ(翌日)ジョブ開始: %s ━━", date_key)

    script_path = os.path.join(PROJECT_ROOT, "scripts", "local_llm_paraphrase.py")
    result = subprocess.run(
        [sys.executable, script_path, date_key],
        cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode == 0:
        logger.info("パラフレーズ(翌日)完了: %s", date_key)
        dag_mark_done("job_paraphrase_tomorrow")
        _RETRY_COUNTS.pop("job_paraphrase_tomorrow", None)
    else:
        logger.error("パラフレーズ(翌日)失敗: %s\n%s", date_key, result.stderr[-500:] if result.stderr else "")
        dag_mark_failed("job_paraphrase_tomorrow")
        _reschedule_retry("job_paraphrase_tomorrow", job_paraphrase_tomorrow)


def job_maintenance():
    """日次メンテナンス (deps: job_results_and_db)"""
    blocked_by = dag_is_blocked("job_maintenance")
    if blocked_by:
        logger.warning("上流タスク %s の失敗により job_maintenance をスキップ", blocked_by)
        dag_mark_failed("job_maintenance")
        return
    if not dag_can_run("job_maintenance"):
        _reschedule_dag_wait("job_maintenance", job_maintenance)
        return

    logger.info("━━ 日次メンテナンスジョブ開始 ━━")

    bat_path = os.path.join(PROJECT_ROOT, "scripts", "daily_maintenance.bat")
    result = subprocess.run(
        ["cmd", "/c", bat_path],
        cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=14400,
    )
    if result.returncode == 0:
        logger.info("日次メンテナンス完了")
        dag_mark_done("job_maintenance")
        _RETRY_COUNTS.pop("job_maintenance", None)
    else:
        logger.error("日次メンテナンス失敗:\n%s", result.stderr[-500:] if result.stderr else "")
        dag_mark_failed("job_maintenance")
        _reschedule_retry("job_maintenance", job_maintenance)


def job_results_and_db():
    """当日の結果取得 + DB更新 (23:00 実行)"""
    if _check_cooldown_and_reschedule("job_results_and_db", job_results_and_db):
        return
    blocked_by = dag_is_blocked("job_results_and_db")
    if blocked_by:
        logger.warning("上流タスク %s の失敗により job_results_and_db をスキップ", blocked_by)
        dag_mark_failed("job_results_and_db")
        _reschedule_retry("job_results_and_db", job_results_and_db)
        return
    if not dag_can_run("job_results_and_db"):
        _reschedule_dag_wait("job_results_and_db", job_results_and_db)
        return

    # 23:00 実行時は当日、手動実行時は正午を境に当日/前日を自動判定
    now = datetime.now()
    if now.hour < 12:
        target_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        target_date = now.strftime("%Y-%m-%d")
    date_key = target_date.replace("-", "")
    pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")

    if not os.path.exists(pred_file):
        logger.info("結果取得: %s の予想データなし、スキップ", target_date)
        dag_mark_done("job_results_and_db")
        _RETRY_COUNTS.pop("job_results_and_db", None)
        return

    logger.info("━━ 結果取得+DB更新ジョブ開始: %s ━━", target_date)

    result = subprocess.run(
        [sys.executable, "run_results.py", target_date],
        cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode == 0:
        logger.info("結果取得完了: %s", target_date)
        from src.scheduler_tasks import run_db_update
        logs = run_db_update(target_date)
        for entry in logs:
            logger.info("  %s", entry)
        dag_mark_done("job_results_and_db")
        _RETRY_COUNTS.pop("job_results_and_db", None)
    else:
        logger.error("結果取得失敗: %s\n%s", target_date, result.stderr[-500:] if result.stderr else "")
        dag_mark_failed("job_results_and_db")
        _reschedule_retry("job_results_and_db", job_results_and_db)


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
    """主要ジョブを DAG に登録する。"""
    # 夜間サイクル (23:00-01:00)
    register_task("job_results_and_db", deps=[])
    register_task("job_prediction", deps=["job_results_and_db"])
    register_task("job_paraphrase_tomorrow", deps=["job_prediction"])
    register_task("job_maintenance", deps=["job_results_and_db"])
    # 朝サイクル (06:00-06:30)
    register_task("job_predict_today", deps=[])
    register_task("job_odds_first_batch", deps=["job_predict_today"])
    register_task("job_paraphrase_today", deps=["job_predict_today", "job_odds_first_batch"])
    logger.info("DAG: 全ジョブ登録完了 (order=%s)", topological_order())


def _dag_reset_and_clear_retries():
    """DAG 状態 + リトライカウントを日次リセット"""
    dag_reset_state()
    _RETRY_COUNTS.clear()
    logger.info("リトライカウント クリア")


def build_scheduler() -> BlockingScheduler:
    """スケジューラーを構築して返す"""
    global _scheduler
    sched = BlockingScheduler(timezone="Asia/Tokyo")

    _register_dag_tasks()
    dag_load_state()

    # ── 夜間サイクル ──

    # 1. 当日結果取得+DB更新: 毎日 23:00
    sched.add_job(job_results_and_db, "cron", hour=23, minute=0, id="results_db",
                  name="結果取得+DB更新(当日)", misfire_grace_time=3600)

    # 2. 翌日予想作成: 毎日 23:30 (当日結果反映済)
    sched.add_job(job_prediction, "cron", hour=23, minute=30, id="prediction",
                  name="予想作成(翌日)", misfire_grace_time=3600)

    # 3. パラフレーズ(翌日): 毎日 00:00
    sched.add_job(job_paraphrase_tomorrow, "cron", hour=0, minute=0, id="paraphrase_tomorrow",
                  name="パラフレーズ(翌日)", misfire_grace_time=3600)

    # 4. 日次メンテナンス: 毎日 01:00
    sched.add_job(job_maintenance, "cron", hour=1, minute=0, id="maintenance",
                  name="日次メンテナンス", misfire_grace_time=3600)

    # 5. DAG + リトライ リセット: 毎日 04:00
    sched.add_job(_dag_reset_and_clear_retries, "cron", hour=4, minute=0, id="dag_reset",
                  name="DAG+リトライ リセット(日次)", misfire_grace_time=3600)

    # ── 朝〜日中サイクル ──

    # 6. 当日予想作成: 毎日 06:00
    sched.add_job(job_predict_today, "cron", hour=6, minute=0, id="predict_today",
                  name="予想作成(当日)", misfire_grace_time=3600)

    # 7. オッズ一括更新(初回 06:00): predict_today 完了後 (DAG待機)
    sched.add_job(job_odds_first_batch, "cron", hour=6, minute=0, id="odds_0600",
                  name="オッズ一括(06:00)", misfire_grace_time=3600)

    # 8. パラフレーズ(当日): 毎日 06:30
    sched.add_job(job_paraphrase_today, "cron", hour=6, minute=30, id="paraphrase_today",
                  name="パラフレーズ(当日)", misfire_grace_time=3600)

    # 9-12. オッズ定時一括更新: 09:00 / 12:00 / 15:00 / 18:00
    for hour in [9, 12, 15, 18]:
        sched.add_job(job_odds_batch, "cron", hour=hour, minute=0,
                      id=f"odds_{hour:02d}00",
                      name=f"オッズ一括({hour:02d}:00)",
                      misfire_grace_time=1800)

    sched.add_listener(_on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    _scheduler = sched
    return sched


def show_status():
    """次回実行予定を表示"""
    sched = BackgroundScheduler(timezone="Asia/Tokyo")
    # 夜間サイクル
    sched.add_job(job_results_and_db, "cron", hour=23, minute=0, id="results_db",
                  name="結果取得+DB更新(当日)", misfire_grace_time=3600)
    sched.add_job(job_prediction, "cron", hour=23, minute=30, id="prediction",
                  name="予想作成(翌日)", misfire_grace_time=3600)
    sched.add_job(job_paraphrase_tomorrow, "cron", hour=0, minute=0, id="paraphrase_tomorrow",
                  name="パラフレーズ(翌日)", misfire_grace_time=3600)
    sched.add_job(job_maintenance, "cron", hour=1, minute=0, id="maintenance",
                  name="日次メンテナンス", misfire_grace_time=3600)
    # 朝〜日中サイクル
    sched.add_job(job_predict_today, "cron", hour=6, minute=0, id="predict_today",
                  name="予想作成(当日)", misfire_grace_time=3600)
    sched.add_job(job_odds_first_batch, "cron", hour=6, minute=0, id="odds_0600",
                  name="オッズ一括(06:00)", misfire_grace_time=3600)
    sched.add_job(job_paraphrase_today, "cron", hour=6, minute=30, id="paraphrase_today",
                  name="パラフレーズ(当日)", misfire_grace_time=3600)
    for hour in [9, 12, 15, 18]:
        sched.add_job(job_odds_batch, "cron", hour=hour, minute=0,
                      id=f"odds_{hour:02d}00",
                      name=f"オッズ一括({hour:02d}:00)",
                      misfire_grace_time=1800)
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
            print(f"  [{job.id:20s}] {job.name:25s}  次回: {nrt_str}")
    print(f"{'='*60}\n")
    sched.shutdown(wait=False)

    # DAG 状態表示
    _register_dag_tasks()
    dag_state = get_dag_state()
    problems = validate_dag()
    order = topological_order()
    print(f"{'='*60}")
    print("  DAG 依存関係ステータス")
    print(f"{'='*60}")
    print(f"  登録タスク数  : {dag_state['registered_count']}")
    print(f"  完了タスク数  : {dag_state['completed_count']}")
    print(f"  失敗タスク数  : {dag_state['failed_count']}")
    print(f"  循環依存      : {'なし' if not problems else f'あり ({problems})'}")
    print(f"  実行順序      : {' → '.join(order)}")
    for task in dag_state["registered"]:
        deps = dag_state["deps"].get(task, [])
        completed = task in dag_state["completed"]
        failed = task in dag_state.get("failed", [])
        pending = [d for d in deps if d not in dag_state["completed"]]
        if failed:
            status = "失敗"
        elif completed:
            status = "完了"
        elif not pending:
            status = "実行可"
        else:
            status = f"待機中({pending})"
        print(f"  [{task:30s}] deps={str(deps or 'なし'):40s}  状態: {status}")
    print(f"{'='*60}\n")


def run_manual(task_name: str):
    """手動でジョブを実行"""
    tasks = {
        "predict_today": ("当日予想作成", job_predict_today),
        "prediction": ("翌日予想作成", job_prediction),
        "odds": ("オッズ一括更新", job_odds_first_batch),
        "paraphrase_today": ("パラフレーズ(当日)", job_paraphrase_today),
        "paraphrase_tomorrow": ("パラフレーズ(翌日)", job_paraphrase_tomorrow),
        "results": ("結果取得+DB更新", job_results_and_db),
        "maintenance": ("日次メンテナンス", job_maintenance),
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
                        help="手動実行 (predict_today/prediction/odds/paraphrase_today/paraphrase_tomorrow/results/maintenance)")
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

    with open(pidfile, "w") as f:
        f.write(str(os.getpid()))

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  keiba-v3 自動スケジューラー起動 (PID %d)", os.getpid())
    logger.info("  ── 夜間サイクル ──")
    logger.info("  結果取得+DB更新: 毎日 23:00 (当日)")
    logger.info("  翌日予想作成: 毎日 23:30 (当日結果反映)")
    logger.info("  パラフレーズ(翌日): 毎日 00:00")
    logger.info("  日次メンテナンス: 毎日 01:00")
    logger.info("  DAGリセット: 毎日 04:00")
    logger.info("  ── 朝〜日中サイクル ──")
    logger.info("  当日予想作成: 毎日 06:00")
    logger.info("  オッズ一括更新: 06:00 / 09:00 / 12:00 / 15:00 / 18:00")
    logger.info("  パラフレーズ(当日): 毎日 06:30")
    logger.info("  個別レースオッズ: 発走15分前 + 発走時刻 (動的登録)")
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
