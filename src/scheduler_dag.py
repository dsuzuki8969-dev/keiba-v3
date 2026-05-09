"""
scheduler_dag.py — DAG モジュール (フェーズ D 段階 2-A)

依存関係予約 + 循環依存検査 + トポロジカルソート。

使い方:
  from src.scheduler_dag import register_task, can_run, mark_done, validate_dag, topological_order

  # 起動時にタスクを登録 (サイクルがあれば ValueError)
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
  - DFS ベースの循環依存検査 (register_task 時に即チェック)
"""

import threading
from collections import deque
from typing import Dict, List, Optional, Set

from src.log import get_logger

logger = get_logger(__name__)

# タスク名 → 依存タスクのリスト
_DAG: Dict[str, List[str]] = {}

# 完了したタスク名のセット
_COMPLETED: Set[str] = set()

# 全 API 共通 Lock
_LOCK = threading.Lock()


# ================================================================
# 内部ユーティリティ — 循環依存検査 (ロック外で呼ぶこと)
# ================================================================

def _detect_cycle_from(
    start: str,
    dag: Dict[str, List[str]],
) -> Optional[List[str]]:
    """DFS でサイクルを検出し、サイクル経路を返す。なければ None。

    Args:
        start: 探索を始めるノード
        dag:   検査対象の有向グラフ (変更しない)

    Returns:
        サイクルを構成するパス (リスト) か None
    """
    # visit=True: 全サブツリー探索済み / on_stack=True: 現在の探索パスに乗っている
    visited: Set[str] = set()
    on_stack: Set[str] = set()
    path: List[str] = []

    def dfs(node: str) -> Optional[List[str]]:
        visited.add(node)
        on_stack.add(node)
        path.append(node)

        for neighbor in dag.get(node, []):
            if neighbor not in visited:
                result = dfs(neighbor)
                if result is not None:
                    return result
            elif neighbor in on_stack:
                # サイクル発見: パスの先頭から neighbor まで + 末尾に neighbor を加えてループを示す
                cycle_start = path.index(neighbor)
                return path[cycle_start:] + [neighbor]

        path.pop()
        on_stack.discard(node)
        return None

    return dfs(start)


def _validate_no_cycle(dag: Dict[str, List[str]]) -> None:
    """DAG 全体を全ノード起点で DFS してサイクルがあれば ValueError を raise。

    新規登録ノードを起点にする DFS だけでは、既存タスクから新規ノードへの
    間接的なサイクルが見逃されるため、全ノードを順に起点として検査する。
    O(V * (V + E)) だが、本プロジェクトの DAG 規模 (3-10 タスク) では実用上問題なし。
    """
    for node in dag:
        result = _detect_cycle_from(node, dag)
        if result is not None:
            cycle_str = " → ".join(result)
            raise ValueError(f"循環依存を検出: {cycle_str}")


# ================================================================
# 公開 API
# ================================================================

def register_task(name: str, deps: List[str]) -> None:
    """タスクと依存関係を DAG に登録する。

    Args:
        name: タスク識別子 (例: "odds_morning" / "results_db")
        deps: このタスクが依存するタスク名のリスト (空なら依存なし)

    Raises:
        ValueError: 登録によって循環依存が生じる場合

    既存の同名タスクは上書きされる。
    """
    with _LOCK:
        # 仮に登録してサイクル検査 → 失敗なら巻き戻す
        # 全ノード起点 DFS で間接サイクル (既存 → 新規 → 既存) も検出
        prev_deps = _DAG.get(name)
        _DAG[name] = list(deps)
        try:
            _validate_no_cycle(_DAG)
        except ValueError:
            # 巻き戻し
            if prev_deps is None:
                del _DAG[name]
            else:
                _DAG[name] = prev_deps
            raise
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
    """タスク完了を記録する。完了によって unblock されるタスクをログ出力する。"""
    with _LOCK:
        _COMPLETED.add(name)
        # name の完了によって can_run が True になるタスクを列挙 (ログ用)
        unblocked = [
            t for t, deps in _DAG.items()
            if t not in _COMPLETED
            and name in deps
            and all(d in _COMPLETED for d in deps)
        ]
        if unblocked:
            logger.info(
                "DAG: タスク完了 name=%s → unblock: %s (累計完了 %d 件)",
                name, unblocked, len(_COMPLETED),
            )
        else:
            logger.info("DAG: タスク完了 name=%s (累計完了 %d 件)", name, len(_COMPLETED))


def mark_failed(name: str) -> None:
    """タスク失敗を記録する。依存タスクの待機タイムアウトを早めるためログ出力のみ。
    _COMPLETED には追加しない (依存タスクは実行されない)。"""
    with _LOCK:
        blocked = [
            t for t, deps in _DAG.items()
            if t not in _COMPLETED and name in deps
        ]
        if blocked:
            logger.warning(
                "DAG: タスク失敗 name=%s → ブロック中: %s (dag_reset_state まで待機)",
                name, blocked,
            )
        else:
            logger.warning("DAG: タスク失敗 name=%s (依存タスクなし)", name)


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


def validate_dag() -> List[str]:
    """全登録タスクで循環依存を検査し、問題タスク名リストを返す。

    Returns:
        循環に関与するタスク名のリスト (空なら問題なし)
    """
    with _LOCK:
        dag_snapshot = dict(_DAG)

    problem_tasks: List[str] = []
    for name in dag_snapshot:
        cycle = _detect_cycle_from(name, dag_snapshot)
        if cycle is not None:
            logger.warning("DAG: 循環依存検出 task=%s cycle=%s", name, " → ".join(cycle))
            # サイクルに含まれるノードを問題タスクとして収集
            for node in cycle:
                if node not in problem_tasks:
                    problem_tasks.append(node)

    return problem_tasks


def topological_order() -> List[str]:
    """トポロジカルソート結果 (依存少ない順) を返す。

    Returns:
        タスク名リスト (依存関係の昇順)

    Raises:
        ValueError: 循環依存がある場合
    """
    with _LOCK:
        dag_snapshot = dict(_DAG)

    # Kahn's algorithm (in-degree ベース BFS)
    in_degree: Dict[str, int] = {name: 0 for name in dag_snapshot}
    for deps in dag_snapshot.values():
        for dep in deps:
            # dep は in_degree に存在しない可能性 (未登録依存) → 0 で初期化
            if dep not in in_degree:
                in_degree[dep] = 0

    # 各ノードが依存される回数をカウント (逆グラフの in-degree)
    # 「name が deps を必要とする」= deps → name の辺なので
    # in_degree[name] = name に依存されている deps の数 ではなく
    # in_degree[name] = name が依存している deps の未完了数
    # → 素直に依存リストの長さ
    for name, deps in dag_snapshot.items():
        in_degree[name] = len(deps)

    # in_degree == 0 のノードをキューに積む (deque はファイル先頭で import 済み)
    queue: deque = deque(sorted(n for n, d in in_degree.items() if d == 0))
    result: List[str] = []

    while queue:
        node = queue.popleft()
        result.append(node)
        # node を依存に持つタスクの in_degree を減らす
        for name, deps in dag_snapshot.items():
            if node in deps:
                in_degree[name] -= 1
                if in_degree[name] == 0:
                    queue.append(name)

    if len(result) < len(in_degree):
        # 全ノードを処理できなかった = サイクルあり
        unprocessed = [n for n in in_degree if n not in result]
        raise ValueError(f"循環依存によりトポロジカルソート不能: {unprocessed}")

    logger.debug("DAG: トポロジカルソート結果 %s", result)
    return result
