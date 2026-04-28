"""三連単集計ヘルパー (_collect_sanrentan_tickets / _check_sanrentan_hit) 単体テスト."""
import pytest
from src.dashboard import _collect_sanrentan_tickets, _check_sanrentan_hit


def test_collect_from_all_sources():
    """tickets / formation_tickets / tickets_by_mode の全領域を網羅すること"""
    race = {
        "tickets": [{"type": "三連単", "combo": [1, 2, 3]}],
        "formation_tickets": [{"type": "三連単", "combo": [1, 3, 2]}],
        "tickets_by_mode": {
            "fixed": [{"type": "三連単", "combo": [2, 1, 3]}],
            "accuracy": [{"type": "馬連", "combo": [1, 2]}],  # 三連単以外は除外
        },
    }
    tix = _collect_sanrentan_tickets(race)
    assert len(tix) == 3
    assert all(t["type"] == "三連単" for t in tix)


def test_collect_empty_race():
    """全フィールド欠損でも例外を出さず空リストを返す"""
    assert _collect_sanrentan_tickets({}) == []
    assert _collect_sanrentan_tickets({"tickets_by_mode": None}) == []


def test_hit_exact_match():
    """combo == top3_ordered で True"""
    tix = [{"type": "三連単", "combo": [1, 2, 3]}]
    assert _check_sanrentan_hit(tix, [1, 2, 3]) is True


def test_hit_no_tickets_returns_none():
    """tix 空 → None (= 三連単対象外レース)"""
    assert _check_sanrentan_hit([], [1, 2, 3]) is None


def test_hit_top3_incomplete_returns_none():
    """top3_ordered < 3 → None (結果未確定)"""
    tix = [{"type": "三連単", "combo": [1, 2, 3]}]
    assert _check_sanrentan_hit(tix, [1, 2]) is None


def test_hit_no_match_returns_false():
    """tix あるが一致なし → False"""
    tix = [{"type": "三連単", "combo": [1, 2, 3]}]
    assert _check_sanrentan_hit(tix, [4, 5, 6]) is False


def test_hit_combo_str_normalized():
    """combo の int/str 揺れに対応 (pred.json は str で保存される場合がある)"""
    tix = [{"type": "三連単", "combo": ["1", "2", "3"]}]
    assert _check_sanrentan_hit(tix, [1, 2, 3]) is True
