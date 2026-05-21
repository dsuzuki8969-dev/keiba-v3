"""scheduler_dag モジュールのテスト。

register_task / can_run / mark_done / mark_failed / reset_state /
validate_dag / topological_order / get_dag_state / get_pending_deps /
is_blocked / save_state / load_state を網羅。
"""

import json
import os
import tempfile

import pytest

from src.scheduler_dag import (
    _COMPLETED,
    _DAG,
    _FAILED,
    _STATE_FILE,
    can_run,
    get_dag_state,
    get_pending_deps,
    is_blocked,
    mark_done,
    mark_failed,
    register_task,
    reset_state,
    topological_order,
    validate_dag,
)
import src.scheduler_dag as _dag_module


@pytest.fixture(autouse=True)
def _clean_dag():
    """各テスト前後に DAG 状態をクリア。"""
    _DAG.clear()
    _COMPLETED.clear()
    _FAILED.clear()
    yield
    _DAG.clear()
    _COMPLETED.clear()
    _FAILED.clear()


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


# ── カスケード中断 (Fix 1) ────────────────────────────────────

class TestCascadeFailure:
    def test_mark_failed_cascades_to_downstream(self):
        """mark_failed が全下流タスクにカスケード伝播すること。
        a → b → c のチェーンで a が失敗すると b, c も _FAILED に入る。"""
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        register_task("c", deps=["b"])
        mark_failed("a")
        assert "a" in _FAILED
        assert "b" in _FAILED
        assert "c" in _FAILED
        # _COMPLETED には入らない
        assert "a" not in _COMPLETED

    def test_is_blocked_returns_failed_dep(self):
        """is_blocked が失敗した依存先の名前を返すこと。"""
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        register_task("c", deps=["b"])
        mark_failed("a")
        # b は a に依存しているので、a が返る
        result = is_blocked("b")
        assert result == "a"
        # c はカスケードで _FAILED に入っているが、直接の依存は b
        result_c = is_blocked("c")
        assert result_c == "b"
        # ブロックされていないタスクは None
        register_task("d", deps=[])
        assert is_blocked("d") is None
        # 未登録タスクも None
        assert is_blocked("unknown") is None

    def test_can_run_false_when_dep_failed(self):
        """依存タスクが失敗していれば can_run=False。"""
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        # a が失敗 → b は実行不可
        mark_failed("a")
        assert can_run("b") is False
        # b 自身もカスケードで _FAILED に入っているので実行不可
        assert can_run("b") is False
        # a 自身も実行不可
        assert can_run("a") is False

    def test_reset_clears_failed(self):
        """reset_state が _FAILED もクリアすること。"""
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        mark_failed("a")
        assert len(_FAILED) > 0
        reset_state()
        assert len(_FAILED) == 0
        assert len(_COMPLETED) == 0
        # リセット後は再実行可能
        mark_done("a")
        assert can_run("b") is True

    def test_get_dag_state_includes_failed(self):
        """get_dag_state に failed が含まれること。"""
        register_task("a", deps=[])
        register_task("b", deps=["a"])
        mark_failed("a")
        state = get_dag_state()
        assert "failed" in state
        assert "a" in state["failed"]
        assert "b" in state["failed"]
        assert "failed_count" in state
        assert state["failed_count"] == 2


# ── 状態永続化 (Fix 2) ────────────────────────────────────────

class TestStatePersistence:
    def test_save_load_state(self, tmp_path):
        """永続化の round-trip テスト: save → 状態クリア → load で復元。"""
        # 一時ファイルパスに差し替え
        original_state_file = _dag_module._STATE_FILE
        test_state_file = os.path.join(str(tmp_path), "dag_state.json")
        _dag_module._STATE_FILE = test_state_file
        try:
            register_task("a", deps=[])
            register_task("b", deps=["a"])
            register_task("c", deps=["b"])
            # a を完了、c を失敗としてマーク
            mark_done("a")
            mark_failed("c")
            # ファイルが作成されたことを確認
            assert os.path.exists(test_state_file)
            # ファイル内容を検証
            with open(test_state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            assert "a" in data["completed"]
            assert "c" in data["failed"]
            assert "saved_at" in data
            # 状態をクリアして復元
            _COMPLETED.clear()
            _FAILED.clear()
            assert len(_COMPLETED) == 0
            assert len(_FAILED) == 0
            _dag_module._load_state()
            assert "a" in _COMPLETED
            assert "c" in _FAILED
        finally:
            # 元に戻す
            _dag_module._STATE_FILE = original_state_file

    def test_load_state_no_file(self, tmp_path):
        """永続化ファイルが存在しない場合、load_state は何もしない。"""
        original_state_file = _dag_module._STATE_FILE
        _dag_module._STATE_FILE = os.path.join(str(tmp_path), "nonexistent.json")
        try:
            # エラーにならずに何もしない
            _dag_module._load_state()
            assert len(_COMPLETED) == 0
            assert len(_FAILED) == 0
        finally:
            _dag_module._STATE_FILE = original_state_file
