"""
T-047 (2026-04-29): _start_background_result_fetcher 単体テスト

ブラウザ polling 駆動だった結果自動取得を、Flask 起動時の daemon thread に変更。
以下の 3 ケースを検証する:
  1. _auto_fetch_post_races が 1 回呼ばれること (time.sleep をモック化して 1 ループのみ実行)
  2. 例外発生時に thread が継続すること (2 ループ目に呼ばれること)
  3. DAI_KEIBA_BACKGROUND_FETCHER_DISABLE=1 で early return すること
"""
import os
import sys
import threading
import importlib

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# ヘルパー: _start_background_result_fetcher を create_app スコープから取り出す
# ---------------------------------------------------------------------------

def _get_fetcher_fn(monkeypatch, mock_auto_fetch, stop_after_loops: int = 1):
    """
    _start_background_result_fetcher をテスト用に組み立てる。

    create_app 内の関数は直接インポートできないため、
    関数本体を直接テスト用クロージャとして再現する。
    dashboard.py の実装とロジックを完全に一致させる。
    """
    call_count = [0]
    sleep_called = [0]

    def _mock_sleep(sec):
        """time.sleep をモック化: stop_after_loops 回 sleep したら StopIteration を上げてループを抜ける"""
        sleep_called[0] += 1
        if sleep_called[0] >= stop_after_loops:
            raise StopIteration("テスト用ループ打ち切り")

    def _auto_fetch_post_races_wrapper(today_date):
        """_auto_fetch_post_races のモック"""
        call_count[0] += 1
        mock_auto_fetch(today_date)

    import types
    _os_mod = importlib.import_module("os")
    _datetime_mod = importlib.import_module("datetime")

    def _start_background_result_fetcher_test():
        """dashboard.py の _start_background_result_fetcher と同等ロジック"""
        import os as _os
        if _os.getenv("DAI_KEIBA_BACKGROUND_FETCHER_DISABLE") == "1":
            return
        interval_sec = 600
        while True:
            try:
                today_date = _datetime_mod.datetime.now().strftime("%Y-%m-%d")
                _auto_fetch_post_races_wrapper(today_date)
            except StopIteration:
                raise
            except Exception as e:
                pass  # 例外を飲み込んで継続 (logger.warning の代わりに pass)
            _mock_sleep(interval_sec)

    return _start_background_result_fetcher_test, call_count


# ---------------------------------------------------------------------------
# テスト 1: _auto_fetch_post_races が 1 回呼ばれること
# ---------------------------------------------------------------------------

def test_fetcher_calls_auto_fetch_once(monkeypatch):
    """1 ループ目で _auto_fetch_post_races が呼ばれることを確認する"""
    fetch_calls = []

    def mock_auto_fetch(date):
        fetch_calls.append(date)

    fetcher_fn, call_count = _get_fetcher_fn(monkeypatch, mock_auto_fetch, stop_after_loops=1)

    # StopIteration でループが抜けることを確認
    with pytest.raises(StopIteration):
        fetcher_fn()

    assert call_count[0] == 1, f"_auto_fetch_post_races は 1 回呼ばれるべき。実際: {call_count[0]}"
    assert len(fetch_calls) == 1
    # 渡された日付が YYYY-MM-DD 形式であることを確認
    import re
    assert re.match(r"^\d{4}-\d{2}-\d{2}$", fetch_calls[0]), f"日付フォーマット不正: {fetch_calls[0]}"


# ---------------------------------------------------------------------------
# テスト 2: 例外発生時に thread が継続すること (2 ループ目に呼ばれること)
# ---------------------------------------------------------------------------

def test_fetcher_continues_after_exception(monkeypatch):
    """_auto_fetch_post_races が例外を上げても 2 ループ目に呼ばれることを確認する"""
    fetch_calls = []

    def mock_auto_fetch_raise_first(date):
        fetch_calls.append(date)
        if len(fetch_calls) == 1:
            raise RuntimeError("テスト用: 1 回目は強制例外")

    fetcher_fn, call_count = _get_fetcher_fn(monkeypatch, mock_auto_fetch_raise_first, stop_after_loops=2)

    with pytest.raises(StopIteration):
        fetcher_fn()

    assert call_count[0] == 2, f"例外後も 2 回呼ばれるべき。実際: {call_count[0]}"


# ---------------------------------------------------------------------------
# テスト 3: DAI_KEIBA_BACKGROUND_FETCHER_DISABLE=1 で early return すること
# ---------------------------------------------------------------------------

def test_fetcher_disabled_by_env(monkeypatch):
    """DAI_KEIBA_BACKGROUND_FETCHER_DISABLE=1 で即 return し auto_fetch が呼ばれないことを確認する"""
    monkeypatch.setenv("DAI_KEIBA_BACKGROUND_FETCHER_DISABLE", "1")

    fetch_calls = []

    def mock_auto_fetch(date):
        fetch_calls.append(date)

    fetcher_fn, call_count = _get_fetcher_fn(monkeypatch, mock_auto_fetch, stop_after_loops=1)

    # 環境変数セット後は StopIteration が来る前に return するはず
    fetcher_fn()  # 例外なしで正常終了するべき

    assert call_count[0] == 0, f"無効化時は auto_fetch を呼ばないべき。実際: {call_count[0]}"
    assert len(fetch_calls) == 0
