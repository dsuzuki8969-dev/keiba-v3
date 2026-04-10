"""
margin_ahead/behind 計算ロジックのテスト

対象: database.py の race_log INSERT時のmargin計算
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


class TestMarginCalculation:
    """finish_time_secからのmargin計算テスト"""

    def _calc_margins(self, orders: list) -> dict:
        """database.py の margin計算ロジックを再現"""
        _time_entries = []
        for _e in orders:
            _hno = _e.get("horse_no")
            _fp = _e.get("finish")
            _ts = 0.0
            try:
                _ts_raw = _e.get("time_sec")
                if _ts_raw is not None:
                    _ts = float(_ts_raw)
            except (ValueError, TypeError):
                pass
            if _hno is not None and _fp is not None and int(_fp) < 90 and _ts > 0:
                _time_entries.append((int(_hno), int(_fp), _ts))
        _time_entries.sort(key=lambda x: x[1])
        _winner_time = _time_entries[0][2] if _time_entries else 0
        _margin_map = {}
        for _idx, (_hno, _fp, _ft) in enumerate(_time_entries):
            _ma = round(_ft - _winner_time, 3) if _winner_time > 0 else 0.0
            _mb = 0.0
            if _idx + 1 < len(_time_entries):
                _next_t = _time_entries[_idx + 1][2]
                if _next_t > _ft:
                    _mb = round(_next_t - _ft, 3)
            _margin_map[_hno] = (_ma, _mb)
        return _margin_map

    def test_winner_margin_zero(self):
        """1着のmargin_aheadは0.0"""
        orders = [
            {"horse_no": 1, "finish": 1, "time_sec": 95.0},
            {"horse_no": 2, "finish": 2, "time_sec": 95.5},
            {"horse_no": 3, "finish": 3, "time_sec": 96.0},
        ]
        margins = self._calc_margins(orders)
        assert margins[1][0] == 0.0  # margin_ahead = 0

    def test_second_place_margin(self):
        """2着のmargin_aheadは1着との差"""
        orders = [
            {"horse_no": 1, "finish": 1, "time_sec": 95.0},
            {"horse_no": 2, "finish": 2, "time_sec": 95.5},
            {"horse_no": 3, "finish": 3, "time_sec": 96.0},
        ]
        margins = self._calc_margins(orders)
        assert margins[2][0] == 0.5  # 95.5 - 95.0

    def test_margin_behind(self):
        """margin_behindは次着との差"""
        orders = [
            {"horse_no": 1, "finish": 1, "time_sec": 95.0},
            {"horse_no": 2, "finish": 2, "time_sec": 95.5},
            {"horse_no": 3, "finish": 3, "time_sec": 96.2},
        ]
        margins = self._calc_margins(orders)
        assert margins[1][1] == 0.5    # 95.5 - 95.0
        assert margins[2][1] == 0.7    # 96.2 - 95.5

    def test_last_place_margin_behind_zero(self):
        """最下位のmargin_behindは0.0"""
        orders = [
            {"horse_no": 1, "finish": 1, "time_sec": 95.0},
            {"horse_no": 2, "finish": 2, "time_sec": 95.5},
        ]
        margins = self._calc_margins(orders)
        assert margins[2][1] == 0.0

    def test_same_time_margin(self):
        """同タイムの場合margin=0"""
        orders = [
            {"horse_no": 1, "finish": 1, "time_sec": 95.0},
            {"horse_no": 2, "finish": 2, "time_sec": 95.0},
        ]
        margins = self._calc_margins(orders)
        assert margins[2][0] == 0.0  # 同タイム
        assert margins[1][1] == 0.0  # 次の馬とも同タイム

    def test_dnf_excluded(self):
        """finish >= 90は除外"""
        orders = [
            {"horse_no": 1, "finish": 1, "time_sec": 95.0},
            {"horse_no": 2, "finish": 99, "time_sec": 0},  # 競走中止
        ]
        margins = self._calc_margins(orders)
        assert 1 in margins
        assert 2 not in margins

    def test_no_time_excluded(self):
        """time_sec=0は除外"""
        orders = [
            {"horse_no": 1, "finish": 1, "time_sec": 95.0},
            {"horse_no": 2, "finish": 2, "time_sec": 0},
        ]
        margins = self._calc_margins(orders)
        assert 1 in margins
        assert 2 not in margins

    def test_empty_orders(self):
        """空のorders"""
        margins = self._calc_margins([])
        assert margins == {}

    def test_precision(self):
        """小数点3位で丸め"""
        orders = [
            {"horse_no": 1, "finish": 1, "time_sec": 95.0},
            {"horse_no": 2, "finish": 2, "time_sec": 95.1234},
        ]
        margins = self._calc_margins(orders)
        assert margins[2][0] == 0.123  # 3位で丸め


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
