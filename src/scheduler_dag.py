"""
scheduler_dag.py — 軽量 DAG モジュール (フェーズ D 段階 1)

依存関係予約の最小実装。フル機能の DAG エンジン (APScheduler 統合等) は
次セッションで本格実装する。本モジュールは「タスク完了状態の記録」+
「依存解決判定」を提供する基盤。

使い方:
  from src.scheduler_dag import register_task, can_run, mark_done

  # 起動時にタスクを登録
  register_task("odds_morning", deps=[])
  register_task("results_db", deps=["odds_morning"])

  # 各ジョブ冒頭で依存解決チェック
  if can_run("results_db"):
      run_results_db()
      mark_done("results_db")
  else:
      logger.info("依存タスク未完のため skip: results_db")

設計方針:
  - thread-safe (全 API が _LOCK で保護)
  - 永続化なし (プロセス再起動で reset・日次バッチで reset_state() 推奨)
  - 循環依存検査なし (登録順で線形チェック・将来 v2 で拡張)
"""

import threading
from typing import Dict, List, Set

from src.log import get_logger

logger = get_logger(__name__)

# タスク名 → 依存タスクのリスト
_DAG: Dict[str, List[str]] = {}

# 完了したタスク名のセット
_COMPLETED: Set[str] = set()

# 全 API 共通 Lock
_LOCK = threading.Lock()


def register_task(name: str, deps: List[str]) -> None:
    """タスクと依存関係を DAG に登録する。

    Args:
        name: タスク識別子 (例: "odds_morning" / "results_db")
        deps: このタスクが依存するタスク名のリスト (空なら依存なし)

    既存の同名タスクは上書きされる。
    """
    with _LOCK:
        _DAG[name] = list(deps)
        logger.debug("DAG: タスク登録 name=%s deps=%s", name, deps)


def can_run(name: str) -> bool:
    """指定タスクの全依存関係が完了済みか判定する。

    Args:
        name: 判定対象タスク名

    Returns:
        True: 依存タスク全完了 (or 未登録) → 実行可能
        False: 依存タスクに未完了あり → 待機すべき
    """
    with _LOCK:
        if name not in _DAG:
            # 未登録タスクは依存なしとして扱う (互換性確保)
            return True
        return all(dep in _COMPLETED for dep in _DAG[name])


def mark_done(name: str) -> None:
    """タスク完了を記録する。"""
    with _LOCK:
        _COMPLETED.add(name)
        logger.info("DAG: タスク完了マーク name=%s (累計完了 %d 件)", name, len(_COMPLETED))


def reset_state() -> None:
    """完了状態をリセットする (日次バッチで翌日タスクのため呼ぶ想定)。"""
    with _LOCK:
        _COMPLETED.clear()
        logger.info("DAG: 完了状態をリセット (登録タスク %d 件は維持)", len(_DAG))


def get_dag_state() -> dict:
    """DAG 全体状態を診断用辞書で返す。"""
    with _LOCK:
        return {
            "registered_count": len(_DAG),
            "completed_count": len(_COMPLETED),
            "registered": sorted(_DAG.keys()),
            "completed": sorted(_COMPLETED),
            "deps": dict(_DAG),
        }


def get_pending_deps(name: str) -> List[str]:
    """指定タスクの未完了依存を返す (診断・log 用)。"""
    with _LOCK:
        if name not in _DAG:
            return []
        return [d for d in _DAG[name] if d not in _COMPLETED]
