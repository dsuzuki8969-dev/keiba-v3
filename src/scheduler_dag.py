"""
scheduler_dag.py — DAG モジュール (フェーズ D 段階 2-A)

依存関係予約 + 循環依存検査 + トポロジカルソート + カスケード中断 + 状態永続化。

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
  - 状態永続化: tmp/dag_state.json に completed/failed を保存 (best-effort)
  - カスケード中断: mark_failed() で下流タスクも _FAILED に伝播
  - DFS ベースの循環依存検査 (register_task 時に即チェック)
"""

import json
import os
import tempfile
import threading
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional, Set

from config.settings import PROJECT_ROOT
from src.log import get_logger

logger = get_logger(__name__)

# タスク名 → 依存タスクのリスト
_DAG: Dict[str, List[str]] = {}

# 完了したタスク名のセット
_COMPLETED: Set[str] = set()

# 失敗したタスク名のセット (カスケード伝播先含む)
_FAILED: Set[str] = set()

# 全 API 共通 Lock
_LOCK = threading.Lock()

# 状態永続化ファイルパス
_STATE_FILE = os.path.join(PROJECT_ROOT, "tmp", "dag_state.json")


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
# 内部ユーティリティ — 下流タスク探索 (ロック外で呼ぶこと)
# ================================================================

def _find_all_downstream(name: str, dag: Dict[str, List[str]]) -> Set[str]:
    """指定タスクの全下流タスクを DFS で探索して返す。

    「下流」= name を (直接・間接に) 依存するタスク群。
    name 自身は含まない。
    """
    downstream: Set[str] = set()
    stack = [name]
    while stack:
        current = stack.pop()
        for task, deps in dag.items():
            if current in deps and task not in downstream:
                downstream.add(task)
                stack.append(task)
    return downstream


# ================================================================
# 内部ユーティリティ — 状態永続化 (ロック内から呼ぶこと)
# ================================================================

def _save_state() -> None:
    """_COMPLETED と _FAILED を JSON ファイルに保存する (best-effort)。

    atomic write: tmpfile → os.replace で中途半端な状態を防ぐ。
    _LOCK 保持中に呼ぶ想定。
    """
    try:
        state_dir = os.path.dirname(_STATE_FILE)
        os.makedirs(state_dir, exist_ok=True)
        data = {
            "completed": sorted(_COMPLETED),
            "failed": sorted(_FAILED),
            "saved_at": datetime.now().isoformat(),
        }
        # 同じディレクトリに tmpfile を作って atomic replace
        fd, tmp_path = tempfile.mkstemp(
            dir=state_dir, suffix=".tmp", prefix="dag_state_"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, _STATE_FILE)
        except Exception:
            # tmpfile の後始末
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        logger.debug("DAG: 状態を永続化 completed=%d failed=%d", len(_COMPLETED), len(_FAILED))
    except Exception as e:
        logger.warning("DAG: 状態永続化に失敗 (best-effort): %s", e)


def _load_state() -> None:
    """JSON ファイルから _COMPLETED と _FAILED を復元する (best-effort)。

    ファイルが存在しなければ何もしない。_LOCK 保持中に呼ぶ想定。
    """
    try:
        if not os.path.exists(_STATE_FILE):
            return
        with open(_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        _COMPLETED.update(data.get("completed", []))
        _FAILED.update(data.get("failed", []))
        logger.info(
            "DAG: 状態を復元 completed=%d failed=%d (from %s)",
            len(_COMPLETED), len(_FAILED), data.get("saved_at", "unknown"),
        )
    except Exception as e:
        logger.warning("DAG: 状態復元に失敗 (best-effort): %s", e)


def load_state() -> None:
    """外部から呼べる状態復元 API。プロセス起動時に 1 度呼ぶ想定。"""
    with _LOCK:
        _load_state()


def save_state() -> None:
    """外部から呼べる状態保存 API。明示的に保存したい場合に使用。"""
    with _LOCK:
        _save_state()


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
        True: 依存タスク全完了 (or 未登録) かつ自身・依存先が失敗していない → 実行可能
        False: 依存タスクに未完了/失敗あり → 待機すべき
    """
    with _LOCK:
        # 自身が _FAILED に入っている場合 (カスケード伝播済み) → 実行不可
        if name in _FAILED:
            return False
        if name not in _DAG:
            # 未登録タスクは依存なしとして扱う (互換性確保)
            return True
        # 依存先に失敗タスクがあれば実行不可
        if any(dep in _FAILED for dep in _DAG[name]):
            return False
        return all(dep in _COMPLETED for dep in _DAG[name])


def mark_done(name: str) -> None:
    """タスク完了を記録する。完了によって unblock されるタスクをログ出力する。"""
    with _LOCK:
        _COMPLETED.add(name)
        # name の完了によって can_run が True になるタスクを列挙 (ログ用)
        # 失敗タスクは unblock 対象外
        unblocked = [
            t for t, deps in _DAG.items()
            if t not in _COMPLETED
            and t not in _FAILED
            and name in deps
            and all(d in _COMPLETED for d in deps)
            and not any(d in _FAILED for d in deps)
        ]
        if unblocked:
            logger.info(
                "DAG: タスク完了 name=%s → unblock: %s (累計完了 %d 件)",
                name, unblocked, len(_COMPLETED),
            )
        else:
            logger.info("DAG: タスク完了 name=%s (累計完了 %d 件)", name, len(_COMPLETED))
        _save_state()


def mark_failed(name: str) -> None:
    """タスク失敗を記録し、全下流タスクにカスケード伝播する。

    _COMPLETED には追加しない。_FAILED に自身 + 全下流を追加して
    can_run() が即座に False を返すようにする。
    """
    with _LOCK:
        _FAILED.add(name)
        # 全下流タスクを DFS で探索してカスケード伝播
        downstream = _find_all_downstream(name, _DAG)
        _FAILED.update(downstream)
        if downstream:
            logger.warning(
                "DAG: タスク失敗 name=%s → カスケード中断: %s (計 %d 件ブロック)",
                name, sorted(downstream), len(downstream),
            )
        else:
            logger.warning("DAG: タスク失敗 name=%s (下流タスクなし)", name)
        _save_state()


def reset_state() -> None:
    """完了・失敗状態をリセットする (日次バッチで翌日タスクのため呼ぶ想定)。"""
    with _LOCK:
        _COMPLETED.clear()
        _FAILED.clear()
        logger.info("DAG: 完了・失敗状態をリセット (登録タスク %d 件は維持)", len(_DAG))
        _save_state()


def get_dag_state() -> dict:
    """DAG 全体状態を診断用辞書で返す。"""
    with _LOCK:
        return {
            "registered_count": len(_DAG),
            "completed_count": len(_COMPLETED),
            "failed_count": len(_FAILED),
            "registered": sorted(_DAG.keys()),
            "completed": sorted(_COMPLETED),
            "failed": sorted(_FAILED),
            "deps": dict(_DAG),
        }


def get_pending_deps(name: str) -> List[str]:
    """指定タスクの未完了依存を返す (診断・log 用)。"""
    with _LOCK:
        if name not in _DAG:
            return []
        return [d for d in _DAG[name] if d not in _COMPLETED]


def clear_failure(name: str) -> None:
    """リトライ前に失敗状態をクリアする。自身 + カスケード下流を _FAILED から除去。"""
    with _LOCK:
        if name not in _FAILED:
            return
        _FAILED.discard(name)
        downstream = _find_all_downstream(name, _DAG)
        for task in downstream:
            _FAILED.discard(task)
        _save_state()
        logger.info("DAG: 失敗状態クリア name=%s (下流 %d 件も解除)", name, len(downstream))


def is_blocked(name: str) -> Optional[str]:
    """指定タスクが失敗した依存先によってブロックされているか判定する。

    Args:
        name: 判定対象タスク名

    Returns:
        ブロック原因の失敗タスク名 (最初に見つかったもの)。ブロックなしなら None。
    """
    with _LOCK:
        # 自身が _FAILED に入っている場合 (カスケード伝播済み)
        if name in _FAILED:
            # 直接の依存先で失敗しているものを返す (あれば)
            if name in _DAG:
                for dep in _DAG[name]:
                    if dep in _FAILED:
                        return dep
            # 自身が直接 mark_failed された場合
            return name
        if name not in _DAG:
            return None
        for dep in _DAG[name]:
            if dep in _FAILED:
                return dep
        return None


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
