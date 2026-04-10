"""
sec_per_rank 距離バンド別テーブルのテスト

対象: pace_course.py の POSITION_SEC_BY_SURFACE_DIST_PACE
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


class TestSecPerRankTable:
    """POSITION_SEC_BY_SURFACE_DIST_PACE テーブルの構造・値テスト"""

    @pytest.fixture(autouse=True)
    def setup(self):
        from src.calculator.pace_course import PaceDeviationCalculator, PaceType
        self.table = PaceDeviationCalculator.POSITION_SEC_BY_SURFACE_DIST_PACE
        self.PaceType = PaceType

    def test_all_8_entries_exist(self):
        """芝×4距離バンド + ダート×4距離バンド = 8エントリ"""
        expected_keys = [
            ("芝", "sprint"), ("芝", "mile"), ("芝", "middle"), ("芝", "long"),
            ("ダート", "sprint"), ("ダート", "mile"), ("ダート", "middle"), ("ダート", "long"),
        ]
        for key in expected_keys:
            assert key in self.table, f"キー {key} がテーブルにない"

    def test_each_entry_has_3_pace_types(self):
        """各エントリにH/M/Sの3ペースタイプが存在"""
        for key, paces in self.table.items():
            for pt in [self.PaceType.H, self.PaceType.M, self.PaceType.S]:
                assert pt in paces, f"{key} に {pt} がない"

    def test_all_values_positive(self):
        """全値が正"""
        for key, paces in self.table.items():
            for pt, val in paces.items():
                assert val > 0, f"{key}[{pt}] = {val} <= 0"

    def test_turf_less_than_dirt(self):
        """同距離バンドで芝 < ダート（一般的にダートの方が位置取り差が大きい）"""
        for dist in ["sprint", "mile", "middle", "long"]:
            for pt in [self.PaceType.H, self.PaceType.M, self.PaceType.S]:
                turf = self.table[("芝", dist)][pt]
                dirt = self.table[("ダート", dist)][pt]
                assert turf < dirt, \
                    f"芝({dist},{pt})={turf} >= ダート({dist},{pt})={dirt}"

    def test_turf_values_reasonable_range(self):
        """芝のsec_per_rankは0.05-0.15の範囲"""
        for dist in ["sprint", "mile", "middle", "long"]:
            for pt in [self.PaceType.H, self.PaceType.M, self.PaceType.S]:
                val = self.table[("芝", dist)][pt]
                assert 0.05 <= val <= 0.15, \
                    f"芝({dist},{pt})={val} が範囲外(0.05-0.15)"

    def test_dirt_values_reasonable_range(self):
        """ダートのsec_per_rankは0.20-0.40の範囲"""
        for dist in ["sprint", "mile", "middle", "long"]:
            for pt in [self.PaceType.H, self.PaceType.M, self.PaceType.S]:
                val = self.table[("ダート", dist)][pt]
                assert 0.20 <= val <= 0.40, \
                    f"ダート({dist},{pt})={val} が範囲外(0.20-0.40)"


class TestDistanceBandClassification:
    """距離バンドの分類テスト"""

    def _classify(self, distance: int) -> str:
        """pace_course.pyの距離バンド分類を再現"""
        if distance <= 1400:
            return "sprint"
        elif distance <= 1800:
            return "mile"
        elif distance <= 2200:
            return "middle"
        else:
            return "long"

    def test_sprint(self):
        assert self._classify(1000) == "sprint"
        assert self._classify(1200) == "sprint"
        assert self._classify(1400) == "sprint"

    def test_mile(self):
        assert self._classify(1600) == "mile"
        assert self._classify(1800) == "mile"

    def test_middle(self):
        assert self._classify(2000) == "middle"
        assert self._classify(2200) == "middle"

    def test_long(self):
        assert self._classify(2400) == "long"
        assert self._classify(3000) == "long"
        assert self._classify(3600) == "long"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
