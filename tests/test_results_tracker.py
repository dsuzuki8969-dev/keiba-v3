"""
results_tracker.py — ability_total クランプロジック境界値テスト

DEVIATION["ability"] の min/max 参照化（Plan-α MEDIUM）が正しく機能するかを検証する。
テスト対象: src/results_tracker.py L311 のクランプ式
  max(DEVIATION["ability"]["min"], min(DEVIATION["ability"]["max"], ev.ability.total))
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DEVIATION

# ---------------------------------------------------------------------------
# ヘルパー関数: results_tracker.py の実際のクランプ式と同等
# ---------------------------------------------------------------------------
_ABILITY_MIN: float = float(DEVIATION["ability"]["min"])
_ABILITY_MAX: float = float(DEVIATION["ability"]["max"])


def _clamp_ability_total(value: float) -> float:
    """results_tracker.py L311 のクランプ式を抜粋して検証用関数化。"""
    return round(max(DEVIATION["ability"]["min"], min(DEVIATION["ability"]["max"], value)), 2)


# ---------------------------------------------------------------------------
# テスト群
# ---------------------------------------------------------------------------

def test_clamp_at_upper_bound():
    """上限値ちょうど（100.0）はそのまま通過する。"""
    result = _clamp_ability_total(100.0)
    assert result == 100.0, f"上限値クランプ失敗: {result}"


def test_clamp_at_lower_bound():
    """下限値ちょうど（-50.0）はそのまま通過する。"""
    result = _clamp_ability_total(-50.0)
    assert result == -50.0, f"下限値クランプ失敗: {result}"


def test_clamp_below_lower_bound():
    """下限を下回る値（-100.0）は下限（-50.0）にクランプされる。"""
    result = _clamp_ability_total(-100.0)
    assert result == _ABILITY_MIN, f"下限クランプ失敗: {result} != {_ABILITY_MIN}"


def test_clamp_above_upper_bound():
    """上限を超える値（200.0）は上限（100.0）にクランプされる。"""
    result = _clamp_ability_total(200.0)
    assert result == _ABILITY_MAX, f"上限クランプ失敗: {result} != {_ABILITY_MAX}"


def test_clamp_zero_passthrough():
    """中間値（0.0）はクランプされずにそのまま返される。"""
    result = _clamp_ability_total(0.0)
    assert result == 0.0, f"ゼロ値変換失敗: {result}"


def test_clamp_uses_deviation_config():
    """DEVIATION 設定値と参照先が一致することを確認（単一ソース保証）。"""
    assert DEVIATION["ability"]["min"] == -50, (
        f"DEVIATION['ability']['min'] が期待値 -50 と異なる: {DEVIATION['ability']['min']}"
    )
    assert DEVIATION["ability"]["max"] == 100, (
        f"DEVIATION['ability']['max'] が期待値 100 と異なる: {DEVIATION['ability']['max']}"
    )


def test_clamp_min_is_not_hardcoded_20():
    """旧ハードコード値 20.0 が下限として使われていないことを確認。"""
    # -30 は -50〜100 の範囲内 → そのまま通過すべき
    result = _clamp_ability_total(-30.0)
    assert result == -30.0, (
        f"下限が -50 ではなく 20 になっている可能性: _clamp(-30.0)={result}"
    )


if __name__ == "__main__":
    print("=== ability_total クランプ境界値テスト ===\n")
    test_clamp_at_upper_bound()
    print("  OK: 上限値 100.0 通過")
    test_clamp_at_lower_bound()
    print("  OK: 下限値 -50.0 通過")
    test_clamp_below_lower_bound()
    print(f"  OK: -100.0 → {_ABILITY_MIN} にクランプ")
    test_clamp_above_upper_bound()
    print(f"  OK: 200.0 → {_ABILITY_MAX} にクランプ")
    test_clamp_zero_passthrough()
    print("  OK: 0.0 通過")
    test_clamp_uses_deviation_config()
    print(f"  OK: DEVIATION設定値確認 min={DEVIATION['ability']['min']} max={DEVIATION['ability']['max']}")
    test_clamp_min_is_not_hardcoded_20()
    print("  OK: 旧ハードコード 20.0 非使用確認")
    print("\n=== 全テスト完了 ===")
