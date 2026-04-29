"""tests/test_dispatch_strategy.py
3 券種ハイブリッド振り分け方式 — 8 組合せパターン × 5 サンプル R 検証
(plan: wiggly-cake / Step 5)

テスト対象: scripts/dispatch_backtest.py
"""
from __future__ import annotations

import sys
import os
from pathlib import Path
from typing import Optional

import pytest

# sys.path に keiba-v3 ルートを追加
sys.path.insert(0, str(Path(__file__).parent.parent))
os.environ["PYTHONIOENCODING"] = "utf-8"

from scripts.dispatch_backtest import (
    layer1_sanrenpuku,
    layer1_umatan,
    layer1_tansho,
    apply_layer2_case_c,
    apply_layer2_case_a,
    build_sanrenpuku_tickets,
    build_umatan_tickets,
    build_tansho_tickets,
    GUARD_DEFS,
    CASE_DEFS,
)


# ────────────────────────────────────────────────────────────────
# サンプル馬リスト生成ヘルパー
# ────────────────────────────────────────────────────────────────

def make_horse(horse_no: int, mark: str, win_prob: float, place3_prob: float,
               odds: float, ev: float) -> dict:
    return {
        "horse_no":       horse_no,
        "mark":           mark,
        "win_prob":       win_prob,
        "place3_prob":    place3_prob,
        "odds":           odds,
        "ev":             ev,
        "composite":      50.0,
        "is_scratched":   None,
        "is_tokusen_kiken": False,
    }


def make_payout(sanrenpuku_combo: str, sanrenpuku_payout: int,
                umatan_combo: str, umatan_payout: int,
                tansho_combo: str, tansho_payout: int) -> dict:
    return {
        "三連複": {"combo": sanrenpuku_combo, "payout": sanrenpuku_payout},
        "馬単":   {"combo": umatan_combo,    "payout": umatan_payout},
        "単勝":   {"combo": tansho_combo,    "payout": tansho_payout},
    }


# ────────────────────────────────────────────────────────────────
# サンプル R (5 パターン)
# ────────────────────────────────────────────────────────────────

# R1: 超強気 — 絞り発動 (◎ EV≥1.8, place3≥0.65, ○ place3≥0.50)
RACE_R1_HORSES = [
    make_horse(1, "◎", 0.45, 0.68, 2.2, 1.95),   # 軸
    make_horse(2, "○", 0.25, 0.55, 4.5, 1.12),   # 対抗
    make_horse(3, "▲", 0.10, 0.30, 9.0, 0.82),
    make_horse(4, "△", 0.05, 0.20, 20.0, 0.95),
    make_horse(5, "★", 0.05, 0.18, 22.0, 1.10),
    make_horse(6, "☆", 0.04, 0.16, 28.0, 1.05),
    make_horse(7, "",   0.03, 0.10, 35.0, 0.40),
    make_horse(8, "",   0.03, 0.08, 38.0, 0.35),
]
RACE_R1_PAYOUTS = make_payout(
    "1-2-4", 3800,
    "1-2", 4200,
    "1",   300,
)

# R2: 本命確信ボーナス (◎ EV≥1.3, win_prob≥0.5, place3≥0.55, ○ place3≥0.40)
RACE_R2_HORSES = [
    make_horse(1, "◎", 0.55, 0.72, 1.8, 1.45),   # 軸 win_prob高
    make_horse(2, "○", 0.18, 0.42, 5.5, 1.15),
    make_horse(3, "▲", 0.10, 0.28, 10.0, 0.90),
    make_horse(4, "△", 0.05, 0.20, 20.0, 0.95),
    make_horse(5, "★", 0.04, 0.15, 25.0, 0.85),
    make_horse(6, "☆", 0.03, 0.12, 32.0, 1.08),
    make_horse(7, "",   0.03, 0.08, 38.0, 0.30),
]
RACE_R2_PAYOUTS = make_payout(
    "1-2-3", 2600,
    "1-2", 3000,
    "1",   200,
)

# R3: 上位固定強 (◎ EV≥1.3, win_prob<0.5, ○ place3≥0.40 → Rule 3)
RACE_R3_HORSES = [
    make_horse(1, "◎", 0.35, 0.58, 2.9, 1.38),   # 軸 win_prob<0.5
    make_horse(2, "○", 0.22, 0.44, 4.5, 1.20),
    make_horse(3, "▲", 0.12, 0.32, 8.5, 0.95),
    make_horse(4, "△", 0.07, 0.22, 15.0, 1.01),
    make_horse(5, "★", 0.05, 0.18, 21.0, 0.88),
    make_horse(6, "☆", 0.04, 0.14, 28.0, 1.03),
    make_horse(7, "",   0.03, 0.09, 40.0, 0.25),
    make_horse(8, "",   0.02, 0.07, 55.0, 0.22),
]
RACE_R3_PAYOUTS = make_payout(
    "1-2-3", 4500,
    "1-2", 5500,
    "1",   320,
)

# R4: デフォルト OR 結合 (◎ EV=1.05, 中発動せず → 広+馬単+単勝)
RACE_R4_HORSES = [
    make_horse(1, "◎", 0.28, 0.50, 3.5, 1.05),
    make_horse(2, "○", 0.18, 0.38, 6.0, 1.08),
    make_horse(3, "▲", 0.12, 0.28, 9.0, 0.82),
    make_horse(4, "△", 0.08, 0.22, 14.0, 0.95),
    make_horse(5, "★", 0.06, 0.18, 18.0, 0.90),
    make_horse(6, "☆", 0.04, 0.13, 28.0, 1.02),
    make_horse(7, "",   0.04, 0.10, 30.0, 0.28),
    make_horse(8, "",   0.03, 0.08, 42.0, 0.22),
]
RACE_R4_PAYOUTS = make_payout(
    "1-2-6", 7200,
    "1-2", 8100,
    "1",   380,
)

# R5: 見送り (◎ EV < 1.0)
RACE_R5_HORSES = [
    make_horse(1, "◎", 0.20, 0.45, 3.0, 0.85),   # EV < 1.0
    make_horse(2, "○", 0.18, 0.40, 5.5, 0.78),
    make_horse(3, "▲", 0.12, 0.30, 8.5, 0.65),
    make_horse(4, "△", 0.08, 0.22, 13.0, 0.75),
    make_horse(5, "★", 0.06, 0.18, 18.0, 0.60),
    make_horse(6, "",   0.05, 0.14, 25.0, 0.55),
]
RACE_R5_PAYOUTS = make_payout(
    "1-2-3", 3000,
    "1-2", 3500,
    "1",   280,
)


# ────────────────────────────────────────────────────────────────
# Layer 1 テスト
# ────────────────────────────────────────────────────────────────

class TestLayer1Sanrenpuku:
    def test_r1_strict(self):
        """R1: EV≥1.8 AND place3≥0.65 AND ○ place3≥0.50 → 絞り"""
        assert layer1_sanrenpuku(RACE_R1_HORSES) == "絞り"

    def test_r2_mid(self):
        """R2: EV≥1.3 AND place3≥0.55 AND ○ place3≥0.40 → 中 (win_prob条件はLayer2)"""
        assert layer1_sanrenpuku(RACE_R2_HORSES) == "中"

    def test_r3_mid(self):
        """R3: EV≥1.3 AND place3≥0.55 AND ○ place3≥0.40 → 中"""
        assert layer1_sanrenpuku(RACE_R3_HORSES) == "中"

    def test_r4_wide(self):
        """R4: EV≥1.0 だが中条件未達 → 広"""
        assert layer1_sanrenpuku(RACE_R4_HORSES) == "広"

    def test_r5_skip(self):
        """R5: EV < 1.0 → None (見送り)"""
        assert layer1_sanrenpuku(RACE_R5_HORSES) is None


class TestLayer1Umatan:
    def test_r1_active(self):
        """R1: EV≥1.3 AND win_prob≥0.35 → True"""
        assert layer1_umatan(RACE_R1_HORSES) is True

    def test_r2_active(self):
        """R2: EV≥1.3 AND win_prob≥0.35 → True"""
        assert layer1_umatan(RACE_R2_HORSES) is True

    def test_r5_inactive(self):
        """R5: EV < 1.3 → False"""
        assert layer1_umatan(RACE_R5_HORSES) is False


class TestLayer1Tansho:
    def test_r1_active(self):
        """R1: ◎ EV≥1.0 → True"""
        assert layer1_tansho(RACE_R1_HORSES) is True

    def test_r5_inactive(self):
        """R5: ◎ EV < 1.0 AND ○ EV < 1.0 → False"""
        assert layer1_tansho(RACE_R5_HORSES) is False


# ────────────────────────────────────────────────────────────────
# Layer 2 案 C 組合せルールテスト (8 組合せパターン)
# ────────────────────────────────────────────────────────────────

class TestLayer2CaseC:
    """案 C 5 ルールの分岐テスト (8 組合せ検証)"""

    def test_rule1_strict_only(self):
        """Rule 1: 絞り発動 → 三連複絞りのみ (馬単・単勝は空)"""
        sr_case  = "絞り"
        um_act   = True
        ts_act   = True
        result = apply_layer2_case_c(sr_case, um_act, ts_act,
                                     RACE_R1_HORSES, RACE_R1_PAYOUTS)
        assert len(result["sanrenpuku"]) > 0, "三連複は買う"
        assert len(result["umatan"])     == 0, "馬単は除外"
        assert len(result["tansho"])     == 0, "単勝は除外"

    def test_rule2_honmei_bonus(self):
        """Rule 2: win_prob≥0.5 AND 三連複中 → 三連複中+単勝 (馬単除外)"""
        sr_case  = "中"
        um_act   = True
        ts_act   = True
        result = apply_layer2_case_c(sr_case, um_act, ts_act,
                                     RACE_R2_HORSES, RACE_R2_PAYOUTS)
        assert len(result["sanrenpuku"]) > 0, "三連複は買う"
        assert len(result["umatan"])     == 0, "馬単は除外"
        assert len(result["tansho"])     > 0, "単勝は買う"

    def test_rule3_umatan_plus_sanrenpuku(self):
        """Rule 3: 馬単中 AND 三連複中 → 三連複中+馬単 (単勝カット)"""
        sr_case  = "中"
        um_act   = True
        ts_act   = True
        result = apply_layer2_case_c(sr_case, um_act, ts_act,
                                     RACE_R3_HORSES, RACE_R3_PAYOUTS)
        assert len(result["sanrenpuku"]) > 0, "三連複は買う"
        assert len(result["umatan"])     > 0, "馬単は買う"
        assert len(result["tansho"])     == 0, "単勝はカット"

    def test_rule4_default_wide_all(self):
        """Rule 4: デフォルト (広+馬単+単勝 全部)"""
        sr_case  = "広"
        um_act   = True
        ts_act   = True
        result = apply_layer2_case_c(sr_case, um_act, ts_act,
                                     RACE_R4_HORSES, RACE_R4_PAYOUTS)
        assert len(result["sanrenpuku"]) > 0, "三連複は買う"
        assert len(result["umatan"])     > 0, "馬単は買う"
        assert len(result["tansho"])     > 0, "単勝は買う"

    def test_rule5_skip_all_empty(self):
        """Rule 5: Layer1 全候補空 → 全券種空"""
        sr_case  = None
        um_act   = False
        ts_act   = False
        result = apply_layer2_case_c(sr_case, um_act, ts_act,
                                     RACE_R5_HORSES, RACE_R5_PAYOUTS)
        assert len(result["sanrenpuku"]) == 0
        assert len(result["umatan"])     == 0
        assert len(result["tansho"])     == 0

    def test_rule4_sanrenpuku_only(self):
        """Rule 4 (三連複のみ発動, 馬単・単勝未発動)"""
        sr_case  = "広"
        um_act   = False
        ts_act   = False
        result = apply_layer2_case_c(sr_case, um_act, ts_act,
                                     RACE_R4_HORSES, RACE_R4_PAYOUTS)
        assert len(result["sanrenpuku"]) > 0
        assert len(result["umatan"])     == 0
        assert len(result["tansho"])     == 0

    def test_rule4_tansho_only(self):
        """Rule 4 (単勝のみ発動)"""
        sr_case  = None
        um_act   = False
        ts_act   = True
        result = apply_layer2_case_c(sr_case, um_act, ts_act,
                                     RACE_R1_HORSES, RACE_R1_PAYOUTS)
        assert len(result["sanrenpuku"]) == 0
        assert len(result["umatan"])     == 0
        assert len(result["tansho"])     > 0

    def test_rule4_umatan_only(self):
        """Rule 4 (馬単のみ発動)"""
        sr_case  = None
        um_act   = True
        ts_act   = False
        result = apply_layer2_case_c(sr_case, um_act, ts_act,
                                     RACE_R1_HORSES, RACE_R1_PAYOUTS)
        assert len(result["sanrenpuku"]) == 0
        assert len(result["umatan"])     > 0
        assert len(result["tansho"])     == 0


# ────────────────────────────────────────────────────────────────
# Layer 2 案 A テスト (OR 結合 = 全部買う)
# ────────────────────────────────────────────────────────────────

class TestLayer2CaseA:
    def test_all_active_buys_all(self):
        """案 A: 全候補発動 → 全券種買う"""
        sr_case  = "中"
        um_act   = True
        ts_act   = True
        result = apply_layer2_case_a(sr_case, um_act, ts_act,
                                     RACE_R3_HORSES, RACE_R3_PAYOUTS)
        assert len(result["sanrenpuku"]) > 0
        assert len(result["umatan"])     > 0
        assert len(result["tansho"])     > 0

    def test_strict_active_buys_strict_and_umatan_tansho(self):
        """案 A: 絞り+馬単+単勝 全部発動 → 全部買う (Rule 1 抑制なし)"""
        sr_case  = "絞り"
        um_act   = True
        ts_act   = True
        result = apply_layer2_case_a(sr_case, um_act, ts_act,
                                     RACE_R1_HORSES, RACE_R1_PAYOUTS)
        assert len(result["sanrenpuku"]) > 0
        assert len(result["umatan"])     > 0
        assert len(result["tansho"])     > 0

    def test_skip_all(self):
        """案 A: 全候補未発動 → 全空"""
        result = apply_layer2_case_a(None, False, False,
                                     RACE_R5_HORSES, RACE_R5_PAYOUTS)
        assert len(result["sanrenpuku"]) == 0
        assert len(result["umatan"])     == 0
        assert len(result["tansho"])     == 0


# ────────────────────────────────────────────────────────────────
# チケット生成 基本テスト
# ────────────────────────────────────────────────────────────────

class TestTicketBuilders:
    def test_sanrenpuku_strict_max4(self):
        """三連複絞り: 2頭軸+3着流し → 最大4点"""
        tickets = build_sanrenpuku_tickets(RACE_R1_HORSES, RACE_R1_PAYOUTS, "絞り")
        assert 0 < len(tickets) <= 4

    def test_sanrenpuku_mid_max7(self):
        """三連複中: 最大7点"""
        tickets = build_sanrenpuku_tickets(RACE_R2_HORSES, RACE_R2_PAYOUTS, "中")
        assert 0 < len(tickets) <= 7

    def test_sanrenpuku_wide_combos(self):
        """三連複広: ◎→5頭2頭組 = C(5,2) = 10点"""
        tickets = build_sanrenpuku_tickets(RACE_R4_HORSES, RACE_R4_PAYOUTS, "広")
        assert 0 < len(tickets) <= 10

    def test_umatan_max7(self):
        """馬単中拡張: 最大7点 (5頭流し+逆2点)"""
        tickets = build_umatan_tickets(RACE_R1_HORSES, RACE_R1_PAYOUTS)
        assert 0 < len(tickets) <= 7

    def test_tansho_max2(self):
        """単勝 T-4: ◎+○ = 2点"""
        tickets = build_tansho_tickets(RACE_R1_HORSES, RACE_R1_PAYOUTS)
        assert 1 <= len(tickets) <= 2

    def test_sanrenpuku_combo_sorted(self):
        """三連複の combo は昇順ソート済み"""
        tickets = build_sanrenpuku_tickets(RACE_R3_HORSES, RACE_R3_PAYOUTS, "中")
        for combo, _ in tickets:
            assert combo == tuple(sorted(combo)), f"combo が昇順でない: {combo}"


# ────────────────────────────────────────────────────────────────
# 信頼度ガード テスト
# ────────────────────────────────────────────────────────────────

class TestGuardDefs:
    def test_g_s_only_s_passes(self):
        """G-S: S 以外はスキップ"""
        guard = GUARD_DEFS["G-S"]
        assert "S" not in guard
        assert "A" in guard
        assert "B" in guard

    def test_g_sa_s_and_a_pass(self):
        """G-SA: S と A は通過"""
        guard = GUARD_DEFS["G-SA"]
        assert "S" not in guard
        assert "A" not in guard
        assert "B" in guard
        assert "D" in guard

    def test_g_none_all_pass(self):
        """G-NONE: 全帯通過"""
        guard = GUARD_DEFS["G-NONE"]
        assert len(guard) == 0


# ────────────────────────────────────────────────────────────────
# 案 C vs 案 A の差分テスト
# ────────────────────────────────────────────────────────────────

class TestCaseAVsCaseC:
    def test_strict_c_suppresses_umatan_a_does_not(self):
        """絞り発動時: 案 C は馬単を除外、案 A は馬単を買う"""
        sr_case = "絞り"
        um_act  = True
        ts_act  = True

        result_c = apply_layer2_case_c(sr_case, um_act, ts_act,
                                       RACE_R1_HORSES, RACE_R1_PAYOUTS)
        result_a = apply_layer2_case_a(sr_case, um_act, ts_act,
                                       RACE_R1_HORSES, RACE_R1_PAYOUTS)

        assert len(result_c["umatan"]) == 0, "案 C: 絞りで馬単を除外"
        assert len(result_a["umatan"]) > 0,  "案 A: 馬単も買う"

    def test_rule2_c_suppresses_umatan_a_does_not(self):
        """Rule 2 発動時: 案 C は馬単を除外、案 A は馬単を買う"""
        sr_case = "中"
        um_act  = True
        ts_act  = True

        result_c = apply_layer2_case_c(sr_case, um_act, ts_act,
                                       RACE_R2_HORSES, RACE_R2_PAYOUTS)
        result_a = apply_layer2_case_a(sr_case, um_act, ts_act,
                                       RACE_R2_HORSES, RACE_R2_PAYOUTS)

        assert len(result_c["umatan"]) == 0, "案 C Rule2: 馬単除外"
        assert len(result_a["umatan"]) > 0,  "案 A: 馬単も買う"

    def test_rule3_c_suppresses_tansho_a_does_not(self):
        """Rule 3 発動時: 案 C は単勝をカット、案 A は単勝も買う"""
        sr_case = "中"
        um_act  = True
        ts_act  = True

        result_c = apply_layer2_case_c(sr_case, um_act, ts_act,
                                       RACE_R3_HORSES, RACE_R3_PAYOUTS)
        result_a = apply_layer2_case_a(sr_case, um_act, ts_act,
                                       RACE_R3_HORSES, RACE_R3_PAYOUTS)

        assert len(result_c["tansho"]) == 0, "案 C Rule3: 単勝カット"
        assert len(result_a["tansho"]) > 0,  "案 A: 単勝も買う"
