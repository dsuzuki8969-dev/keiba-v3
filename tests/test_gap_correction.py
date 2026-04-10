"""
Gap補正ロジックのテスト

対象: jockey_trainer.py の rank_table → gap_mult 計算
"""
import math
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from config.settings import RANK_GAP_MULT_MAX


class TestGapCorrection:
    """Gap補正の連続対数関数テスト"""

    def _calc_gap_mult(self, gap_1_2: float) -> float:
        """jockey_trainer.py の gap_mult 計算を再現"""
        if gap_1_2 < 1.0:
            return 1.0
        raw_bonus = math.log1p(gap_1_2 * 0.25) * 0.40
        if gap_1_2 >= 15.0:
            raw_bonus *= 0.45
        elif gap_1_2 >= 10.0:
            raw_bonus *= 0.82
        return 1.0 + min(RANK_GAP_MULT_MAX, raw_bonus)

    def test_no_gap_no_correction(self):
        """gap < 1.0: 補正なし"""
        assert self._calc_gap_mult(0.0) == 1.0
        assert self._calc_gap_mult(0.5) == 1.0
        assert self._calc_gap_mult(0.99) == 1.0

    def test_small_gap_small_correction(self):
        """gap 1.0-3.0: 小さな補正"""
        mult = self._calc_gap_mult(1.0)
        assert 1.0 < mult < 1.15, f"gap=1.0: {mult}"

        mult = self._calc_gap_mult(3.0)
        assert 1.05 < mult < 1.25, f"gap=3.0: {mult}"

    def test_medium_gap(self):
        """gap 5.0-9.0: 中程度の補正"""
        mult5 = self._calc_gap_mult(5.0)
        mult9 = self._calc_gap_mult(9.0)
        # 単調増加
        assert mult5 < mult9
        # 合理的な範囲
        assert 1.10 < mult5 < 1.40, f"gap=5.0: {mult5}"

    def test_large_gap_dampened(self):
        """gap >= 10: 減衰適用"""
        mult9 = self._calc_gap_mult(9.0)
        mult10 = self._calc_gap_mult(10.0)
        # gap=10は減衰×0.82が適用されるので、gap=9より小さくなる可能性あり
        # （減衰なしの生値は大きいが、×0.82で抑制）
        assert mult10 > 1.0

    def test_extreme_gap_heavily_dampened(self):
        """gap >= 15: 強い減衰適用（弱いフィールドの乱数性対応）"""
        mult15 = self._calc_gap_mult(15.0)
        mult10 = self._calc_gap_mult(10.0)
        # gap=15は×0.45、gap=10は×0.82 → 15の方が小さいはず
        assert mult15 < mult10, f"gap=15: {mult15} should be < gap=10: {mult10}"

    def test_monotonic_increase_before_dampening(self):
        """gap 1-9: 単調増加"""
        prev = 1.0
        for gap in [1.0, 2.0, 3.0, 5.0, 7.0, 9.0]:
            curr = self._calc_gap_mult(gap)
            assert curr >= prev, f"gap={gap}: {curr} < {prev}"
            prev = curr

    def test_max_mult_cap(self):
        """RANK_GAP_MULT_MAX上限を超えない"""
        for gap in [1.0, 5.0, 10.0, 20.0, 50.0]:
            mult = self._calc_gap_mult(gap)
            assert mult <= 1.0 + RANK_GAP_MULT_MAX, \
                f"gap={gap}: {mult} > max {1.0 + RANK_GAP_MULT_MAX}"


class TestGapBlendRatio:
    """Engine.py の gap-based ML blend ratio テスト"""

    def _calc_gap_boost(self, gap_1_2: float) -> float:
        """engine.py のgap boost計算を再現"""
        if gap_1_2 < 2.0:
            return 0.0
        _gap_boost = min(0.25, math.log1p((gap_1_2 - 2.0) * 0.25) * 0.15)
        if gap_1_2 >= 15.0:
            _gap_boost *= 0.40
        elif gap_1_2 >= 10.0:
            _gap_boost *= 0.75
        return _gap_boost

    def test_no_boost_small_gap(self):
        """gap < 2.0: ブースト0"""
        assert self._calc_gap_boost(0.0) == 0.0
        assert self._calc_gap_boost(1.9) == 0.0

    def test_boost_increases_with_gap(self):
        """gap 2-9: 単調増加"""
        prev = 0.0
        for gap in [2.0, 3.0, 5.0, 7.0, 9.0]:
            curr = self._calc_gap_boost(gap)
            assert curr >= prev, f"gap={gap}: {curr} < {prev}"
            prev = curr

    def test_boost_capped_at_025(self):
        """最大0.25に制限"""
        for gap in [5.0, 10.0, 20.0, 50.0]:
            boost = self._calc_gap_boost(gap)
            assert boost <= 0.25, f"gap={gap}: {boost} > 0.25"

    def test_dampening_at_extreme_gaps(self):
        """gap >= 15: 強い減衰"""
        boost9 = self._calc_gap_boost(9.0)
        boost15 = self._calc_gap_boost(15.0)
        assert boost15 < boost9, f"gap=15 boost ({boost15}) should be < gap=9 ({boost9})"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
