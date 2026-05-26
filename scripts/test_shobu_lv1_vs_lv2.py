# -*- coding: utf-8 -*-
"""A-3e Step 1+2 検証: shobu_score Lv1 (簡易) vs Lv2 (engine 直呼び) vs Lv3 (engine 完全互換)

mock tracker / race / horse でいくつかの代表ケースを生成し、
Lv1 / Lv2 / Lv3 の出力差 (絶対差 / 一致率) を測定。

期待:
  - Lv1 vs Lv2: 主要因子は近いが、調教師偏差値の判定式違い等で微差
  - Lv2 vs Lv3: KishuPattern.A 判定 + recovery_break 推定 で差分発生
  - Lv3 は engine の calc_shobu_score 仕様に最も忠実
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# walk_forward_backtest.py から関数を借りる
from scripts.walk_forward_backtest import (
    _calc_shobu_score_wf,
    _calc_shobu_score_wf_lv2,
    _calc_shobu_score_wf_lv3,
)


class MockTracker:
    """RollingStatsTracker の最小 mock (Lv3 対応: horse_history + phase10b)"""

    def __init__(self, jockey_features=None, trainer_features=None,
                 horse_history=None, phase10b=None, all_jockeys=None):
        # jockey_features: 主馬の現騎手 features
        # all_jockeys: {jid: features_dict} 前走 jockey 用
        self._j = jockey_features or {}
        self._t = trainer_features or {}
        self._all_j = all_jockeys or {}
        self._horse_history = horse_history or {}
        self._phase10b = phase10b or {}

    def get_jockey_features(self, jid, venue, _a, _b, date_str):
        # 主馬の jid なら _j、その他は all_jockeys から
        if jid in self._all_j:
            return self._all_j[jid]
        return self._j

    def get_trainer_features(self, tid, venue, date_str):
        return self._t

    def get_trainer_phase10b_features(self, tid):
        return self._phase10b


CASES = [
    {
        "name": "Case 1: 強騎手+好調厩舎+高偏差値+休み明け",
        "h": {
            "jockey_id": "J001", "trainer_id": "T001", "horse_id": "H001",
            "is_jockey_change": True, "last_grade": "1勝",
            "days_since_last_run": 90,
        },
        "race": {"venue": "東京", "date": "2026-05-26", "grade": "3勝"},
        "j": {"jockey_win_rate": 0.22, "jockey_win_rate_90d": 0.22},
        "t": {"trainer_win_rate": 0.20, "trainer_win_rate_90d": 0.28},
        "phase10b": {"trainer_class_trend": 0.8, "trainer_rest_wr": 0.45},
        "horse_history": {"H001": [("2026-04-01", 5, 12, "J999")]},  # 前走別騎手
        "all_jockeys": {"J999": {"jockey_win_rate": 0.05}},  # 前走騎手は弱
    },
    {
        "name": "Case 2: 低成績騎手+不調厩舎",
        "h": {
            "jockey_id": "J002", "trainer_id": "T002", "horse_id": "H002",
            "is_jockey_change": False, "last_grade": "1勝",
            "days_since_last_run": 14,
        },
        "race": {"venue": "中山", "date": "2026-05-26", "grade": "1勝"},
        "j": {"jockey_win_rate": 0.05, "jockey_win_rate_90d": 0.05},
        "t": {"trainer_win_rate": 0.05, "trainer_win_rate_90d": 0.03},
        "phase10b": {"trainer_class_trend": -0.3, "trainer_rest_wr": 0.10},
    },
    {
        "name": "Case 3: 中位騎手+平均厩舎",
        "h": {
            "jockey_id": "J003", "trainer_id": "T003", "horse_id": "H003",
            "is_jockey_change": False, "last_grade": "2勝",
            "days_since_last_run": 30,
        },
        "race": {"venue": "京都", "date": "2026-05-26", "grade": "2勝"},
        "j": {"jockey_win_rate": 0.12, "jockey_win_rate_90d": 0.12},
        "t": {"trainer_win_rate": 0.10, "trainer_win_rate_90d": 0.10},
        "phase10b": {"trainer_class_trend": 0.0, "trainer_rest_wr": 0.30},
    },
    {
        "name": "Case 4: 強い騎手のみ (休み明けなし・乗り替わりなし)",
        "h": {
            "jockey_id": "J004", "trainer_id": "T004", "horse_id": "H004",
            "is_jockey_change": False, "last_grade": "OP",
            "days_since_last_run": 21,
        },
        "race": {"venue": "阪神", "date": "2026-05-26", "grade": "OP"},
        "j": {"jockey_win_rate": 0.18, "jockey_win_rate_90d": 0.18},
        "t": {"trainer_win_rate": 0.11, "trainer_win_rate_90d": 0.11},
        "phase10b": {"trainer_class_trend": 0.1, "trainer_rest_wr": 0.30},
    },
    {
        "name": "Case 5: 格上げ+厩舎好調+高偏差値+rest_wr 高",
        "h": {
            "jockey_id": "J005", "trainer_id": "T005", "horse_id": "H005",
            "is_jockey_change": False, "last_grade": "2勝",
            "days_since_last_run": 90,
        },
        "race": {"venue": "東京", "date": "2026-05-26", "grade": "3勝"},
        "j": {"jockey_win_rate": 0.10, "jockey_win_rate_90d": 0.10},
        "t": {"trainer_win_rate": 0.19, "trainer_win_rate_90d": 0.25},
        "phase10b": {"trainer_class_trend": 1.2, "trainer_rest_wr": 0.50},  # rest_wr 高 → recovery_break ~ 150
    },
    {
        "name": "Case 6: tracker なし (フォールバック)",
        "h": {"jockey_id": "", "trainer_id": "", "horse_id": "",
              "is_jockey_change": False, "last_grade": "",
              "days_since_last_run": None},
        "race": {"venue": "", "date": "", "grade": ""},
        "j": {}, "t": {}, "phase10b": {},
    },
    {
        "name": "Case 7: 乗り替わり (弱→強) Lv3 で KishuPattern.A 検出",
        "h": {
            "jockey_id": "J007", "trainer_id": "T007", "horse_id": "H007",
            "is_jockey_change": True, "last_grade": "2勝",
            "days_since_last_run": 21,
        },
        "race": {"venue": "東京", "date": "2026-05-26", "grade": "2勝"},
        "j": {"jockey_win_rate": 0.13, "jockey_win_rate_90d": 0.13},  # new_dev = 56 (not >=60)
        "t": {"trainer_win_rate": 0.10, "trainer_win_rate_90d": 0.10},
        "phase10b": {"trainer_class_trend": 0.0, "trainer_rest_wr": 0.30},
        "horse_history": {"H007": [("2026-04-01", 8, 14, "J999")]},
        "all_jockeys": {"J999": {"jockey_win_rate": 0.03}},  # prev_dev = 36
        # diff = 56-36 = 20 >= 8 → KishuPattern.A (Lv3)
        # Lv1/Lv2 は 90d > 0.15 で判定 → 0.13 で発火しない、差出る
    },
    {
        "name": "Case 8: 乗り替わり (強→弱) Lv3 で KishuPattern.A 不検出",
        "h": {
            "jockey_id": "J008", "trainer_id": "T008", "horse_id": "H008",
            "is_jockey_change": True, "last_grade": "2勝",
            "days_since_last_run": 21,
        },
        "race": {"venue": "東京", "date": "2026-05-26", "grade": "2勝"},
        "j": {"jockey_win_rate": 0.08, "jockey_win_rate_90d": 0.08},
        "t": {"trainer_win_rate": 0.10, "trainer_win_rate_90d": 0.10},
        "phase10b": {"trainer_class_trend": 0.0, "trainer_rest_wr": 0.30},
        "horse_history": {"H008": [("2026-04-01", 3, 14, "J999")]},
        "all_jockeys": {"J999": {"jockey_win_rate": 0.20}},  # 前走 dev=70
    },
]


def main():
    print("=" * 110)
    print("A-3e Step 1+2: shobu_score Lv1 vs Lv2 vs Lv3 出力比較")
    print("=" * 110)
    print(f"{'ケース':<58} | {'Lv1':>6} | {'Lv2':>6} | {'Lv3':>6} | {'L1-L3':>6} | {'L2-L3':>6}")
    print("-" * 110)

    diffs_12 = []
    diffs_23 = []
    diffs_13 = []
    same_12 = 0
    same_23 = 0
    same_13 = 0

    for case in CASES:
        tracker = MockTracker(
            jockey_features=case["j"],
            trainer_features=case["t"],
            horse_history=case.get("horse_history"),
            phase10b=case.get("phase10b", {}),
            all_jockeys=case.get("all_jockeys"),
        )
        v1 = _calc_shobu_score_wf(case["h"], case["race"], tracker)
        v2 = _calc_shobu_score_wf_lv2(case["h"], case["race"], tracker)
        v3 = _calc_shobu_score_wf_lv3(case["h"], case["race"], tracker)
        d12 = round(v2 - v1, 2)
        d23 = round(v3 - v2, 2)
        d13 = round(v3 - v1, 2)
        diffs_12.append(abs(d12))
        diffs_23.append(abs(d23))
        diffs_13.append(abs(d13))
        if abs(d12) < 0.01: same_12 += 1
        if abs(d23) < 0.01: same_23 += 1
        if abs(d13) < 0.01: same_13 += 1
        print(f"{case['name']:<58} | {v1:>6.2f} | {v2:>6.2f} | {v3:>6.2f} | {d13:>+6.2f} | {d23:>+6.2f}")

    print("-" * 110)
    n = len(CASES)
    print(f"Lv1≈Lv2 一致: {same_12}/{n} ({same_12/n*100:.0f}%) | 平均絶対差: {sum(diffs_12)/n:.3f}")
    print(f"Lv2≈Lv3 一致: {same_23}/{n} ({same_23/n*100:.0f}%) | 平均絶対差: {sum(diffs_23)/n:.3f}")
    print(f"Lv1≈Lv3 一致: {same_13}/{n} ({same_13/n*100:.0f}%) | 平均絶対差: {sum(diffs_13)/n:.3f}")
    print()
    print("=== 分析 ===")
    print("- Lv3 は engine の calc_shobu_score 仕様 (KishuPattern.A + recovery_break) に最も忠実")
    print("- Case 7 (乗り替わり 弱→強) で Lv3 のみ KishuPattern.A 検出 → +2.0 加算")
    print("- Case 1/5 で Lv3 の recovery_break 推定が calc_break_adjustment に反映")


if __name__ == "__main__":
    main()
