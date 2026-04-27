"""
Plan-γ Phase 1: backfill_relative_dev の単体テスト

calc_relative_dev / calc_rank_based_dev のロジックを pytest で検証する。
ネットワーク不要・DB 不要で実行可能。
"""

import math
import sys
from pathlib import Path

import pytest

# プロジェクトルートを PYTHONPATH に追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backfill_relative_dev import calc_relative_dev, calc_rank_based_dev


# ============================================================
# calc_relative_dev のテスト
# ============================================================


class TestCalcRelativeDev:
    """z-score 正規化の検証"""

    def test_returns_50_when_all_same_value(self):
        """
        全馬が同偏差値ならば z-score = 0 → relative_dev = 50.0
        全頭同一タイム → μ=60, σ=sigma_floor=5.0, z=0 → 50.0
        """
        values = [60.0, 60.0, 60.0, 60.0, 60.0]
        result = calc_relative_dev(values, 60.0)
        assert result == pytest.approx(50.0, abs=0.01)

    def test_above_mean_returns_above_50(self):
        """μ より高い run_dev は relative_dev > 50.0 になること"""
        values = [40.0, 45.0, 50.0, 55.0, 60.0]  # μ=50
        result = calc_relative_dev(values, 60.0)
        assert result > 50.0

    def test_below_mean_returns_below_50(self):
        """μ より低い run_dev は relative_dev < 50.0 になること"""
        values = [40.0, 45.0, 50.0, 55.0, 60.0]  # μ=50
        result = calc_relative_dev(values, 40.0)
        assert result < 50.0

    def test_sigma_floor_applied_when_stdev_zero(self):
        """
        全頭同タイム → stdev=0 → sigma_floor=5.0 が使われること
        対象馬もμ=同タイムならz=0 → 50.0
        """
        values = [50.0, 50.0, 50.0, 50.0, 50.0]
        result = calc_relative_dev(values, 50.0, sigma_floor=5.0)
        assert result == pytest.approx(50.0, abs=0.01)

    def test_upper_clamp_at_plus_3sigma(self):
        """
        z が +10 になるような外れ値 → +3σ クランプで 80.0
        """
        values = [40.0, 45.0, 50.0, 55.0, 60.0]
        result = calc_relative_dev(values, 500.0)
        assert result == pytest.approx(80.0, abs=0.01)

    def test_lower_clamp_at_minus_3sigma(self):
        """
        z が -10 になるような外れ値 → -3σ クランプで 20.0
        """
        values = [40.0, 45.0, 50.0, 55.0, 60.0]
        result = calc_relative_dev(values, -500.0)
        assert result == pytest.approx(20.0, abs=0.01)

    def test_single_value_returns_50(self):
        """head数 < 2 は標準偏差が計算できないため固定 50.0 を返す"""
        result = calc_relative_dev([60.0], 60.0)
        assert result == 50.0

    def test_empty_list_returns_50(self):
        """空リストは固定 50.0 を返す"""
        result = calc_relative_dev([], 60.0)
        assert result == 50.0

    def test_known_zscore_values(self):
        """
        μ=50, σ=10 の状態で target=60 → z=+1.0 → relative_dev=60.0
        target=40 → z=-1.0 → relative_dev=40.0
        """
        # stdev([40,50,60]) = 10.0
        values = [40.0, 50.0, 60.0]
        result_60 = calc_relative_dev(values, 60.0)
        result_40 = calc_relative_dev(values, 40.0)
        assert result_60 == pytest.approx(60.0, abs=0.5)
        assert result_40 == pytest.approx(40.0, abs=0.5)

    def test_output_range_is_20_to_80(self):
        """
        ±3σ クランプにより出力は [20.0, 80.0] に収まること
        """
        values = list(range(1, 20))
        for target in [-1000, 0, 50, 100, 200, 1000]:
            result = calc_relative_dev(values, float(target))
            assert 20.0 <= result <= 80.0, f"target={target} → {result} は [20, 80] 外"


# ============================================================
# calc_rank_based_dev のテスト（帯広ばんえい専用）
# ============================================================


class TestCalcRankBasedDev:
    """帯広ばんえい順位ベースの検証"""

    def test_rank1_returns_above_50(self):
        """rank=1, n=8 → 最高 relative_dev（> 50）"""
        result = calc_rank_based_dev(1, 8)
        assert result > 50.0, f"rank=1 は 50 超のはず: {result}"

    def test_last_rank_returns_below_50(self):
        """rank=n, n=8 → 最低 relative_dev（< 50）"""
        result = calc_rank_based_dev(8, 8)
        assert result < 50.0, f"rank=8/8 は 50 未満のはず: {result}"

    def test_rank1_and_last_rank_are_symmetric(self):
        """
        rank=1 と rank=n は 50 に対して対称的な値になること
        """
        n = 8
        r1 = calc_rank_based_dev(1, n)
        rn = calc_rank_based_dev(n, n)
        # 対称性チェック
        assert r1 + rn == pytest.approx(100.0, abs=0.01), (
            f"rank=1: {r1}, rank={n}: {rn} → 合計 {r1+rn} != 100"
        )

    def test_middle_rank_near_50(self):
        """
        n=8 の場合、中間の着順は 50 付近になること
        """
        n = 8
        r4 = calc_rank_based_dev(4, n)
        r5 = calc_rank_based_dev(5, n)
        assert abs(r4 - 50.0) < 10.0, f"rank=4: {r4} は 50 付近のはず"
        assert abs(r5 - 50.0) < 10.0, f"rank=5: {r5} は 50 付近のはず"

    def test_n1_returns_50(self):
        """
        出走馬 1 頭のみ: rank=1, n=1 → 50.0
        """
        result = calc_rank_based_dev(1, 1)
        # (n - rank + 0.5) / n = (1 - 1 + 0.5) / 1 = 0.5 → x = 0.5 - 0.5 = 0 → 50
        assert result == pytest.approx(50.0, abs=0.01)

    def test_special_rank_99_returns_50(self):
        """
        失格・取消等の特殊着順 (rank=99) は 50.0 固定になること
        rank > n の場合は中央値 50.0 を返す（帯広の finish_pos=99 対応）
        """
        result = calc_rank_based_dev(99, 7)
        assert result == pytest.approx(50.0, abs=0.01)

    def test_rank_zero_returns_50(self):
        """
        rank=0 は無効値 → 50.0 固定になること
        """
        result = calc_rank_based_dev(0, 8)
        assert result == pytest.approx(50.0, abs=0.01)

    def test_output_range_15_to_85_for_typical_field(self):
        """
        ばんえいの出走頭数は通常 4〜10 頭。全パターンで範囲確認
        設計書: 概ね 17〜83 の範囲
        """
        for n in range(4, 11):
            for rank in range(1, n + 1):
                result = calc_rank_based_dev(rank, n)
                assert 15.0 <= result <= 85.0, (
                    f"n={n}, rank={rank} → {result} は [15, 85] 外"
                )

    def test_higher_rank_always_higher_dev(self):
        """
        n=8 で着順が増えるにつれて偏差値が下がること
        """
        n = 8
        results = [calc_rank_based_dev(r, n) for r in range(1, n + 1)]
        for i in range(len(results) - 1):
            assert results[i] > results[i + 1], (
                f"rank={i+1}: {results[i]} <= rank={i+2}: {results[i+1]}"
            )


# ============================================================
# 統合的なプロパティテスト
# ============================================================


class TestIntegration:
    """calc_relative_dev と calc_rank_based_dev の整合性検証"""

    def test_race_avg_relative_dev_near_50(self):
        """
        同 race 内の relative_dev の平均は 50 付近に収まること（z-score の性質）
        """
        import statistics

        race_samples = [
            [40.0, 50.0, 60.0, 55.0, 45.0],
            [70.0, 65.0, 80.0, 50.0, 60.0, 55.0, 75.0],
            [20.0, 30.0, 40.0, 25.0, 35.0, 30.0],
        ]
        for values in race_samples:
            rel_devs = [calc_relative_dev(values, v) for v in values]
            mu = statistics.mean(rel_devs)
            # 平均は sigma_floor の影響で厳密に 50.0 にはならないが、近い値になる
            assert 40.0 <= mu <= 60.0, (
                f"relative_dev 平均 {mu:.2f} は [40, 60] 外: values={values}"
            )
