"""
netkeiba_checks.py — netkeiba アクセス安全チェックモジュール

背景:
  netkeiba 並列アクセスによる 403 多発事故 (2026-04-28・10,398 件)
  危険時間帯 (06:00-06:30 / 22:00-23:30) にスケジューラが netkeiba にアクセスするため、
  手動スクリプトとの競合を防ぐためのユーティリティを集約する。

  詳細: memory/feedback_netkeiba_concurrent_throttle.md

公開 API:
  DANGER_HOURS       — 危険時間帯定義 (定数)
  CONFLICT_PROCESSES — 競合スクリプト名リスト (定数)
  is_danger_time()   — 現在が危険時間帯か判定
  check_conflict_processes() — 競合プロセスを検出してリストを返す
  assert_safe_to_proceed()   — 危険 or 競合時に RuntimeError raise

対応OS: Windows + Unix (psutil 優先・ImportError 時は subprocess fallback)
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# ログ出力 (プロジェクト共通ロガー)
# scraper モジュール内で使う想定なので get_logger を使う
try:
    from src.log import get_logger
    logger = get_logger(__name__)
except Exception:
    import logging
    logger = logging.getLogger(__name__)


# ============================================================
# 定数: 危険時間帯
# ============================================================

# (開始_時, 開始_分, 終了_時, 終了_分) のリスト
# 終了時刻は「含まない」 — start_min <= now_min < end_min で判定
DANGER_HOURS: List[Tuple[int, int, int, int]] = [
    (6,  0,  6, 30),   # 06:00-06:30  DAI_Keiba_Predict スケジューラ
    (22, 0, 23, 30),   # 22:00-23:30  DAI_Keiba_Results + Maintenance スケジューラ
]


# ============================================================
# 定数: 競合プロセスキーワード
# ============================================================

# これらのキーワードを含むプロセスが動いていたら abort 対象とする
# 各 backfill スクリプトの CONFLICT_PROCESSES / CONFLICT_KEYWORDS を統合した共通リスト
CONFLICT_PROCESSES: List[str] = [
    # 予測・分析パイプライン
    "run_analysis_date.py",
    "predict_tomorrow_runner",
    "auto_fetch_odds",
    "auto_fetch",
    # スケジューラ
    "scheduler_tasks",
    "Predict_Tomorrow",
    "results_tracker",
    # backfill 系 (同種同士の重複防止)
    "backfill_race_log",
    "backfill_horses_2023h",
    "backfill_b_prefix",
    "backfill_all_payouts",
    "backfill_payouts",
    "backfill_recent_days",
    "backfill_2026_gaps",
]


# ============================================================
# 公開 API
# ============================================================

def is_danger_time(now: Optional[datetime] = None) -> bool:
    """
    現在時刻が netkeiba 危険時間帯かどうかを返す。

    Args:
        now: 判定する日時。None の場合は datetime.now() を使用。

    Returns:
        True  — 危険時間帯 (アクセス禁止)
        False — 安全時間帯 (アクセス可)

    Examples:
        >>> is_danger_time(datetime(2026, 5, 7, 6, 15))
        True   # 06:15 は危険
        >>> is_danger_time(datetime(2026, 5, 7, 7, 0))
        False  # 07:00 は安全
        >>> is_danger_time(datetime(2026, 5, 7, 22, 30))
        True   # 22:30 は危険
        >>> is_danger_time(datetime(2026, 5, 7, 23, 45))
        False  # 23:45 は 23:30 終了後なので安全
    """
    if now is None:
        now = datetime.now()
    current_min = now.hour * 60 + now.minute
    for (sh, sm, eh, em) in DANGER_HOURS:
        start_min = sh * 60 + sm
        end_min   = eh * 60 + em
        if start_min <= current_min < end_min:
            return True
    return False


def check_conflict_processes(allow_self: bool = True) -> List[Dict]:
    """
    競合する netkeiba アクセス系プロセスが動いているか検出する。

    psutil が利用可能な場合は psutil 経由で取得。
    ImportError の場合は subprocess (Windows: wmic / Unix: ps -ef) でフォールバック。

    Args:
        allow_self: True のとき自プロセス (os.getpid()) は除外する。

    Returns:
        検出した競合プロセスの辞書リスト。空リストなら競合なし。
        各辞書は {"keyword": str, "cmdline": str, "pid": Optional[int]}
    """
    my_pid = os.getpid()
    found: List[Dict] = []

    # ── psutil 優先 ──────────────────────────────────────────
    try:
        import psutil  # type: ignore

        for proc in psutil.process_iter(["pid", "cmdline"]):
            try:
                pid = proc.info["pid"]
                if allow_self and pid == my_pid:
                    continue
                cmdline = " ".join(proc.info["cmdline"] or [])
                for kw in CONFLICT_PROCESSES:
                    if kw in cmdline:
                        found.append({"keyword": kw, "cmdline": cmdline[:120], "pid": pid})
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return found

    except ImportError:
        # psutil なし → subprocess fallback
        logger.debug("psutil が未インストールのため subprocess フォールバックで競合チェックします")

    # ── subprocess fallback (Windows + Unix 両対応) ──────────
    # 注意: psutil が利用可能なら自プロセス除外が確実 (PID 抽出ミスなし)。
    # subprocess fallback は psutil 未インストール時の暫定経路で、
    # wmic 出力フォーマットによっては PID 抽出が失敗する可能性がある (自プロセス誤検出のリスク)。
    # 推奨: requirements.txt に追加済の psutil>=5.9.0 を実環境にインストール。
    try:
        if sys.platform == "win32":
            # Windows: wmic でコマンドライン一覧取得
            result = subprocess.run(
                ["wmic", "process", "get", "ProcessId,CommandLine"],
                capture_output=True, text=True, timeout=10,
                encoding="utf-8", errors="replace",
            )
            raw_lines = result.stdout.splitlines()
            # wmic 出力は "CommandLine                                  ProcessId" 形式
            for line in raw_lines:
                line = line.strip()
                if not line:
                    continue
                for kw in CONFLICT_PROCESSES:
                    if kw in line:
                        # wmic 出力の末尾が PID (数字) の場合は抽出を試みる
                        # PID 抽出失敗時は pid_val=None となり、allow_self チェックが空振りする可能性あり
                        parts = line.rsplit(None, 1)
                        pid_str = parts[-1] if len(parts) > 1 and parts[-1].isdigit() else None
                        pid_val = int(pid_str) if pid_str else None
                        if pid_val is None:
                            logger.debug(
                                "wmic 経路: PID 抽出失敗 (自プロセス誤検出の可能性あり)。"
                                " psutil インストールで解消可能: %s", line[:80]
                            )
                        if allow_self and pid_val == my_pid:
                            break
                        found.append({"keyword": kw, "cmdline": line[:120], "pid": pid_val})
                        break
        else:
            # Unix: ps -ef
            result = subprocess.run(
                ["ps", "-ef"],
                capture_output=True, text=True, timeout=10,
            )
            for line in result.stdout.splitlines():
                # grep 自身は除外
                if "grep" in line:
                    continue
                for kw in CONFLICT_PROCESSES:
                    if kw in line:
                        # ps -ef の2列目が PID
                        parts = line.split()
                        pid_val = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
                        if allow_self and pid_val == my_pid:
                            break
                        found.append({"keyword": kw, "cmdline": line[:120], "pid": pid_val})
                        break

    except Exception as e:
        # プロセスチェックに失敗しても実行を止めない (安全側に倒しすぎない)
        logger.warning(f"競合プロセスチェックに失敗しました (スキップ): {e}")

    return found


def assert_safe_to_proceed(force: bool = False) -> None:
    """
    netkeiba アクセス前の安全確認を一括実施する。

    危険時間帯 または 競合プロセス検出時に RuntimeError を raise する。
    force=True で両チェックをバイパスする (緊急実行用)。

    Args:
        force: True のとき危険時間帯・競合プロセスチェックをスキップ。

    Raises:
        RuntimeError: 危険時間帯 or 競合プロセスが検出された場合。

    Examples:
        # 安全時間帯 (12:00 など) なら何も raise しない
        assert_safe_to_proceed()

        # force=True なら危険時間帯でも通る
        assert_safe_to_proceed(force=True)
    """
    if force:
        logger.warning(
            "[netkeiba_checks] force=True でチェックをバイパスします。"
            " netkeiba 並列アクセス禁止ルール (違反歴 1 回・業務影響大) を承知の上で実行してください。"
        )
        return

    # ── 危険時間帯チェック ────────────────────────────────────
    now = datetime.now()
    if is_danger_time(now):
        now_str = now.strftime("%H:%M")
        raise RuntimeError(
            f"[ABORT] 危険時間帯 ({now_str}) のため実行を中止します。\n"
            f"  安全な実行時間帯: 06:31〜21:59 / 23:31〜05:59\n"
            f"  詳細: memory/feedback_netkeiba_concurrent_throttle.md"
        )

    # ── 競合プロセスチェック ──────────────────────────────────
    conflicts = check_conflict_processes(allow_self=True)
    if conflicts:
        details = "; ".join(f"[PID={c['pid']}] {c['keyword']}" for c in conflicts)
        raise RuntimeError(
            f"[ABORT] 競合する netkeiba アクセス系プロセスが検出されました。\n"
            f"  検出: {details}\n"
            f"  netkeiba 並列アクセス禁止 (違反歴 1 回・業務影響大)\n"
            f"  競合プロセスが終了してから再実行してください。"
        )

    logger.debug("[netkeiba_checks] 安全確認 OK — 危険時間帯なし・競合プロセスなし")
