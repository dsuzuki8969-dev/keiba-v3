# -*- coding: utf-8 -*-
"""A-3e Step 1 検証: shobu_score Lv1 (簡易) vs Lv2 (engine 直呼び) 出力比較

mock tracker / race / horse でいくつかの代表ケースを生成し、
Lv1 と Lv2 の出力差 (絶対差 / 一致率) を測定。

期待: 因子の主要部分は両者同じ加点ロジックだが、調教師偏差値の判定
(Lv1: win_rate 4 段階 / Lv2: Z 変換 -> calc_shobu_score 内部 4 段階)
で微差が発生する想定。
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# walk_forward_backtest.py から関数を借りる
from scripts.walk_forward_backtest import _calc_shobu_score_wf, _calc_shobu_score_wf_lv2


class MockTracker:
    """RollingStatsTracker の最小 mock"""

    def __init__(self, jockey_features=None, trainer_features=None):
        self._j = jockey_features or {}
        self._t = trainer_features or {}

    def get_jockey_features(self, jid, venue, _a, _b, date_str):
        return self._j

    def get_trainer_features(self, tid, venue, date_str):
        return self._t


CASES = [
    {
        "name": "Case 1: 強い騎手 + 好調厩舎 + 高偏差値 + 休み明け",
        "h": {
            "jockey_id": "J001", "trainer_id": "T001",
            "is_jockey_change": True,
            "last_grade": "1勝",
            "days_since_last_run": 90,
        },
        "race": {"venue": "東京", "date": "2026-05-26", "grade": "3勝"},
        "j": {"jockey_win_rate_90d": 0.22},
        "t": {"trainer_win_rate": 0.20, "trainer_win_rate_90d": 0.28},
    },
    {
        "name": "Case 2: 低成績騎手 + 不調厩舎",
        "h": {
            "jockey_id": "J002", "trainer_id": "T002",
            "is_jockey_change": False,
            "last_grade": "1勝", "days_since_last_run": 14,
        },
        "race": {"venue": "中山", "date": "2026-05-26", "grade": "1勝"},
        "j": {"jockey_win_rate_90d": 0.05},
        "t": {"trainer_win_rate": 0.05, "trainer_win_rate_90d": 0.03},
    },
    {
        "name": "Case 3: 中位騎手 + 平均厩舎",
        "h": {
            "jockey_id": "J003", "trainer_id": "T003",
            "is_jockey_change": False,
            "last_grade": "2勝", "days_since_last_run": 30,
        },
        "race": {"venue": "京都", "date": "2026-05-26", "grade": "2勝"},
        "j": {"jockey_win_rate_90d": 0.12},
        "t": {"trainer_win_rate": 0.10, "trainer_win_rate_90d": 0.10},
    },
    {
        "name": "Case 4: 強い騎手のみ (休み明けなし)",
        "h": {
            "jockey_id": "J004", "trainer_id": "T004",
            "is_jockey_change": False,
            "last_grade": "OP", "days_since_last_run": 21,
        },
        "race": {"venue": "阪神", "date": "2026-05-26", "grade": "OP"},
        "j": {"jockey_win_rate_90d": 0.18},
        "t": {"trainer_win_rate": 0.11, "trainer_win_rate_90d": 0.11},
    },
    {
        "name": "Case 5: 格上げ + 厩舎好調 + 高偏差値厩舎",
        "h": {
            "jockey_id": "J005", "trainer_id": "T005",
            "is_jockey_change": False,
            "last_grade": "2勝", "days_since_last_run": 21,
        },
        "race": {"venue": "東京", "date": "2026-05-26", "grade": "3勝"},
        "j": {"jockey_win_rate_90d": 0.10},
        "t": {"trainer_win_rate": 0.19, "trainer_win_rate_90d": 0.25},
    },
    {
        "name": "Case 6: tracker なし (フォールバック)",
        "h": {
            "jockey_id": "", "trainer_id": "",
            "is_jockey_change": False,
            "last_grade": "", "days_since_last_run": None,
        },
        "race": {"venue": "", "date": "", "grade": ""},
        "j": {},
        "t": {},
    },
]


def main():
    print("=" * 90)
    print("A-3e Step 1: shobu_score Lv1 vs Lv2 出力比較")
    print("=" * 90)
    print(f"{'ケース':<45} | {'Lv1':>6} | {'Lv2':>6} | {'差分':>6} | {'一致':<4}")
    print("-" * 90)

    same_count = 0
    diff_count = 0
    total_abs_diff = 0.0

    for case in CASES:
        tracker = MockTracker(jockey_features=case["j"], trainer_features=case["t"])
        v1 = _calc_shobu_score_wf(case["h"], case["race"], tracker)
        v2 = _calc_shobu_score_wf_lv2(case["h"], case["race"], tracker)
        diff = round(v2 - v1, 2)
        is_same = "OK" if abs(diff) < 0.01 else "差"
        if is_same == "OK":
            same_count += 1
        else:
            diff_count += 1
        total_abs_diff += abs(diff)
        print(f"{case['name']:<45} | {v1:>6.2f} | {v2:>6.2f} | {diff:>+6.2f} | {is_same:<4}")

    print("-" * 90)
    n = len(CASES)
    print(f"一致: {same_count}/{n} ({same_count/n*100:.0f}%) / 平均絶対差: {total_abs_diff/n:.3f}")
    print()
    print("=== 分析 ===")
    print("- 一致ケースが多いほど Lv1 ≈ Lv2 (実運用差小)")
    print("- 差分が大きいケース = 調教師偏差値/short_momentum 判定式の違いによる")
    print("- Lv3 では tracker 拡張で KishuPattern / recovery_break も完全一致を目指す")


if __name__ == "__main__":
    main()
