"""scheduler_dag モジュールのテスト。

register_task / can_run / mark_done / mark_failed / reset_state /
validate_dag / topological_order / get_dag_state / get_pending_deps を網羅。
"""

import pytest

from src.scheduler_dag import (
    _COMPLETED,
    _DAG,
    can_run,
    get_dag_state,
    get_pending_deps,
    mark_done,
    mark_failed,
    register_task,
    reset_state,
    topological_order,
    validate_dag,
)


@pytest.fixture(autouse=True)
def _clean_dag():
    """各テスト前後に DAG 状態をクリア。"""
    _DAG.clear()
    _COMPLETED.clear()
    yield
    _DAG.clear()
    _COMPLETED.clear()


# ── register_task ──────────────────────────────────────────

class TestRegisterTask:
    def test_register_no_deps(self):
        register_task("a", deps=[])
        assert "a" in _DAG
        assert _DAG["a"] == []

    def test_register_with_deps(self):
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        assert _DAG["b"] == ["a"]

    def test_overwrite_existing(self):
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        register_task("b", deps=[])
        assert _DAG["b"] == []

    def test_cycle_detection_direct(self):
        register_task("a", deps=["b"])
        with pytest.raises(ValueError, match="循環依存"):
            register_task("b", deps=["a"])

    def test_cycle_detection_indirect(self):
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        register_task("c", deps=["b"])
        with pytest.raises(ValueError, match="循環依存"):
            register_task("a", deps=["c"])

    def test_cycle_rollback(self):
        """循環依存で ValueError 後、DAG は変更前の状態を保持。"""
        register_task("a", deps=["b"])
        with pytest.raises(ValueError):
            register_task("b", deps=["a"])
        assert "b" not in _DAG

    def test_self_cycle(self):
        with pytest.raises(ValueError, match="循環依存"):
            register_task("x", deps=["x"])


# ── can_run / mark_done ────────────────────────────────────

class TestCanRunMarkDone:
    def test_no_deps_can_run(self):
        register_task("a", deps=[])
        assert can_run("a") is True

    def test_unregistered_can_run(self):
        assert can_run("unknown") is True

    def test_pending_dep_blocks(self):
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        assert can_run("b") is False

    def test_mark_done_unblocks(self):
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        mark_done("a")
        assert can_run("b") is True

    def test_multiple_deps_all_needed(self):
        register_task("a", deps=[])
        register_task("b", deps=[])
        register_task("c", deps=["a", "b"])
        mark_done("a")
        assert can_run("c") is False
        mark_done("b")
        assert can_run("c") is True

    def test_chain_deps(self):
        """a → b → c の依存チェーン。"""
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        register_task("c", deps=["b"])
        assert can_run("c") is False
        mark_done("a")
        assert can_run("c") is False
        mark_done("b")
        assert can_run("c") is True


# ── mark_failed ────────────────────────────────────────────

class TestMarkFailed:
    def test_failed_does_not_complete(self):
        """mark_failed は _COMPLETED に追加しない → 依存タスクは永久にブロック。"""
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        mark_failed("a")
        assert "a" not in _COMPLETED
        assert can_run("b") is False

    def test_failed_no_deps_logged(self):
        """依存タスクがないタスクの失敗でもエラーにならない。"""
        register_task("a", deps=[])
        mark_failed("a")
        assert "a" not in _COMPLETED

    def test_failed_then_reset_allows_retry(self):
        """失敗後に reset_state → 依存関係はそのまま、再実行可能。"""
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        mark_failed("a")
        assert can_run("b") is False
        reset_state()
        # a を再実行して成功させる
        mark_done("a")
        assert can_run("b") is True


# ── reset_state ────────────────────────────────────────────

class TestResetState:
    def test_clears_completed(self):
        register_task("a", deps=[])
        mark_done("a")
        assert "a" in _COMPLETED
        reset_state()
        assert len(_COMPLETED) == 0

    def test_keeps_dag_structure(self):
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        reset_state()
        assert "a" in _DAG
        assert "b" in _DAG
        assert _DAG["b"] == ["a"]


# ── get_dag_state / get_pending_deps ───────────────────────

class TestDiagnostics:
    def test_get_dag_state(self):
        register_task("x", deps=[])
        register_task("y", deps=["x"])
        mark_done("x")
        state = get_dag_state()
        assert state["registered_count"] == 2
        assert state["completed_count"] == 1
        assert "x" in state["completed"]
        assert "y" in state["registered"]

    def test_get_pending_deps(self):
        register_task("a", deps=[])
        register_task("b", deps=[])
        register_task("c", deps=["a", "b"])
        mark_done("a")
        pending = get_pending_deps("c")
        assert pending == ["b"]

    def test_get_pending_deps_unknown(self):
        assert get_pending_deps("unknown") == []


# ── validate_dag ───────────────────────────────────────────

class TestValidateDag:
    def test_no_cycle(self):
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        assert validate_dag() == []

    def test_empty_dag(self):
        assert validate_dag() == []


# ── topological_order ──────────────────────────────────────

class TestTopologicalOrder:
    def test_simple_order(self):
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        register_task("c", deps=["b"])
        order = topological_order()
        assert order.index("a") < order.index("b") < order.index("c")

    def test_parallel_tasks(self):
        register_task("a", deps=[])
        register_task("b", deps=[])
        register_task("c", deps=["a", "b"])
        order = topological_order()
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("c")

    def test_empty_dag(self):
        assert topological_order() == []

    def test_single_task(self):
        register_task("only", deps=[])
        assert topological_order() == ["only"]

    def test_diamond_dependency(self):
        """ダイヤモンド依存: a → b, a → c, b+c → d"""
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        register_task("c", deps=["a"])
        register_task("d", deps=["b", "c"])
        order = topological_order()
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")
