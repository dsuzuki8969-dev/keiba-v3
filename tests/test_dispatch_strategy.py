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
    process_day,
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


# ────────────────────────────────────────────────────────────────
# --no-umatan フラグ機能テスト
# ────────────────────────────────────────────────────────────────

class TestNoUmatanFlag:
    """--no-umatan フラグ: force_disabled=True で layer1_umatan が常に False を返すことを確認"""

    def test_force_disabled_returns_false_when_would_be_true(self):
        """通常は True になる馬 (EV≥1.3, win_prob≥0.35) で force_disabled=True → False"""
        # R1 馬: ◎ EV=1.95, win_prob=0.45 → 通常なら True
        assert layer1_umatan(RACE_R1_HORSES, force_disabled=False) is True, \
            "前提確認: force_disabled=False なら True"
        assert layer1_umatan(RACE_R1_HORSES, force_disabled=True) is False, \
            "--no-umatan 相当の force_disabled=True なら False"

    def test_force_disabled_returns_false_when_would_also_be_false(self):
        """通常も False になる馬でも force_disabled=True → False (副作用なし)"""
        # R5 馬: ◎ EV=0.85 < 1.3 → 通常でも False
        assert layer1_umatan(RACE_R5_HORSES, force_disabled=False) is False, \
            "前提確認: force_disabled=False でも False"
        assert layer1_umatan(RACE_R5_HORSES, force_disabled=True) is False, \
            "force_disabled=True でも False のまま"

    def test_force_disabled_r2_active(self):
        """R2 馬 (EV=1.45, win_prob=0.55) でも force_disabled=True → False"""
        assert layer1_umatan(RACE_R2_HORSES, force_disabled=False) is True, \
            "前提確認: 通常は True"
        assert layer1_umatan(RACE_R2_HORSES, force_disabled=True) is False, \
            "force_disabled=True なら False"

    def test_apply_layer2_case_a_no_umatan_never_buys_umatan(self):
        """案 A で umatan_active=False の場合 → 馬単チケット生成されない (no-umatan 状態の模擬)"""
        # force_disabled=True により umatan_active=False になった状態を模擬
        sr_case   = "中"
        um_act    = layer1_umatan(RACE_R3_HORSES, force_disabled=True)  # False
        ts_act    = layer1_tansho(RACE_R3_HORSES)

        assert um_act is False, "no-umatan で馬単候補は False"
        result = apply_layer2_case_a(sr_case, um_act, ts_act,
                                     RACE_R3_HORSES, RACE_R3_PAYOUTS)
        assert len(result["umatan"]) == 0, "馬単チケット生成されない"
        assert len(result["sanrenpuku"]) > 0, "三連複は生成される"

    def test_apply_layer2_case_c_no_umatan_never_buys_umatan(self):
        """案 C で umatan_active=False の場合 → Rule 3 が非発動、馬単チケット生成されない"""
        # R3 は通常 Rule 3 (三連複中+馬単) だが no_umatan=True で馬単 False
        sr_case   = layer1_sanrenpuku(RACE_R3_HORSES)  # "中"
        um_act    = layer1_umatan(RACE_R3_HORSES, force_disabled=True)  # False
        ts_act    = layer1_tansho(RACE_R3_HORSES)

        assert sr_case == "中", "前提: 三連複中"
        assert um_act is False, "no-umatan で馬単候補は False"

        result = apply_layer2_case_c(sr_case, um_act, ts_act,
                                     RACE_R3_HORSES, RACE_R3_PAYOUTS)
        assert len(result["umatan"]) == 0, "馬単チケット生成されない"
        # Rule 3 非発動 → Rule 4 (デフォルト) へフォールバック、三連複+単勝
        assert len(result["sanrenpuku"]) > 0, "三連複は生成される"


# ────────────────────────────────────────────────────────────────
# T-050: src/calculator/betting.py 新規 3 関数テスト
# build_sanrenpuku_dynamic_tickets / build_tansho_t4_tickets / dispatch_tickets
# ────────────────────────────────────────────────────────────────

from src.calculator.betting import (
    build_sanrenpuku_dynamic_tickets,
    build_tansho_t4_tickets,
    dispatch_tickets,
)
from src.models import Mark


# ── HorseEvaluation モック ──
class _MockHorse:
    """HorseEvaluation.horse の簡易モック"""
    def __init__(self, horse_no: int, odds: float):
        self.horse_no = horse_no
        self.odds = odds  # effective_odds の基礎値


class _MockEval:
    """HorseEvaluation の簡易モック (T-050 テスト用)"""
    def __init__(
        self,
        horse_no: int,
        mark_val: str,
        win_prob: float,
        place3_prob: float,
        odds: float,
    ):
        self.horse = _MockHorse(horse_no, odds)
        # Mark Enum から value でルックアップ
        _mark_map = {m.value: m for m in Mark}
        self.mark = _mark_map.get(mark_val, Mark.NONE)
        self.win_prob      = win_prob
        self.place3_prob   = place3_prob
        self.is_tokusen_kiken = False
        self.is_scratched  = False
        self.composite     = 50.0
        self.predicted_tansho_odds = odds

    @property
    def effective_odds(self) -> float:
        return float(self.horse.odds or self.predicted_tansho_odds or 0.0)


class _MockRaceInfo:
    """RaceInfo の簡易モック"""
    def __init__(self, field_count: int = 8, is_jra: bool = True):
        self.field_count = field_count
        self.is_jra = is_jra


# ── サンプルレース定義 (HorseEvaluation ベース) ──

def _make_ev_race_strict():
    """絞り発動条件: ◎ EV=1.98 (0.45×4.4), place3=0.68, ○ place3=0.55"""
    return [
        _MockEval(1, "◎", 0.45, 0.68, 4.4),   # 軸: EV=1.98, place3=0.68
        _MockEval(2, "○", 0.25, 0.55, 4.5),   # 対抗: place3=0.55≥0.50
        _MockEval(3, "▲", 0.10, 0.30, 9.0),
        _MockEval(4, "△", 0.05, 0.20, 20.0),
        _MockEval(5, "★", 0.05, 0.18, 22.0),
        _MockEval(6, "☆", 0.04, 0.16, 28.0),
        _MockEval(7, "－", 0.03, 0.10, 35.0),
        _MockEval(8, "－", 0.03, 0.08, 38.0),
    ]


def _make_ev_race_mid():
    """中発動条件: ◎ EV=1.44 (0.55×2.6), place3=0.60, ○ place3=0.44≥0.40"""
    return [
        _MockEval(1, "◎", 0.55, 0.60, 2.6),   # 軸: EV=1.43
        _MockEval(2, "○", 0.18, 0.44, 5.5),   # 対抗: place3=0.44≥0.40
        _MockEval(3, "▲", 0.10, 0.28, 10.0),
        _MockEval(4, "△", 0.05, 0.20, 20.0),
        _MockEval(5, "★", 0.04, 0.15, 25.0),
        _MockEval(6, "☆", 0.03, 0.12, 32.0),
        _MockEval(7, "－", 0.03, 0.08, 38.0),
    ]


def _make_ev_race_wide():
    """広発動条件: ◎ EV=1.05, 中条件未達 (place3=0.50<0.55)"""
    return [
        _MockEval(1, "◎", 0.28, 0.50, 3.8),   # 軸: EV=1.064
        _MockEval(2, "○", 0.18, 0.38, 6.0),
        _MockEval(3, "▲", 0.12, 0.28, 9.0),
        _MockEval(4, "△", 0.08, 0.22, 14.0),
        _MockEval(5, "★", 0.06, 0.18, 18.0),
        _MockEval(6, "☆", 0.04, 0.13, 28.0),
        _MockEval(7, "－", 0.04, 0.10, 30.0),
        _MockEval(8, "－", 0.03, 0.08, 42.0),
    ]


def _make_ev_race_skip():
    """見送り条件: ◎ EV<1.0"""
    return [
        _MockEval(1, "◎", 0.20, 0.45, 4.0),   # 軸: EV=0.80<1.0
        _MockEval(2, "○", 0.18, 0.40, 4.5),
        _MockEval(3, "▲", 0.12, 0.30, 8.5),
        _MockEval(4, "△", 0.08, 0.22, 13.0),
        _MockEval(5, "★", 0.06, 0.18, 18.0),
    ]


def _make_ev_race_no_honmei():
    """◎◉なし: 軸馬が存在しない"""
    return [
        _MockEval(1, "○", 0.30, 0.55, 3.5),
        _MockEval(2, "▲", 0.20, 0.40, 5.0),
        _MockEval(3, "△", 0.15, 0.30, 7.5),
    ]


_MOCK_RACE = _MockRaceInfo(field_count=8, is_jra=True)


class TestBuildSanrenpukuDynamicTickets:
    """build_sanrenpuku_dynamic_tickets の単体テスト"""

    def test_strict_pattern_max4(self):
        """絞り: ◎◉-○ 2頭軸 + {▲△★☆} → 最大4点"""
        tickets = build_sanrenpuku_dynamic_tickets(_make_ev_race_strict(), _MOCK_RACE)
        assert len(tickets) > 0, "絞り発動で最低1点"
        assert len(tickets) <= 4, "絞りは最大4点"
        for t in tickets:
            assert t["type"] == "三連複"
            assert t["pattern"] == "絞り"
            assert t["stake"] == 100
            assert len(t["combo"]) == 3

    def test_mid_pattern_max7(self):
        """中: 最大7点"""
        tickets = build_sanrenpuku_dynamic_tickets(_make_ev_race_mid(), _MOCK_RACE)
        assert len(tickets) > 0, "中発動で最低1点"
        assert len(tickets) <= 7, "中は最大7点"
        for t in tickets:
            assert t["type"] == "三連複"
            assert t["pattern"] == "中"
            assert t["stake"] == 100

    def test_wide_pattern_max10(self):
        """広: ◎軸 + サブ馬 C(n,2) 組合せ → 最大10点"""
        tickets = build_sanrenpuku_dynamic_tickets(_make_ev_race_wide(), _MOCK_RACE)
        assert len(tickets) > 0, "広発動で最低1点"
        assert len(tickets) <= 10, "広は最大10点"
        for t in tickets:
            assert t["type"] == "三連複"
            assert t["pattern"] == "広"

    def test_skip_when_ev_too_low(self):
        """EV<1.0 → 空リスト (見送り)"""
        tickets = build_sanrenpuku_dynamic_tickets(_make_ev_race_skip(), _MOCK_RACE)
        assert tickets == []

    def test_no_honmei_returns_empty(self):
        """◎◉なし → 空リスト"""
        tickets = build_sanrenpuku_dynamic_tickets(_make_ev_race_no_honmei(), _MOCK_RACE)
        assert tickets == []

    def test_combo_is_sorted(self):
        """combo は昇順ソート済み"""
        tickets = build_sanrenpuku_dynamic_tickets(_make_ev_race_wide(), _MOCK_RACE)
        for t in tickets:
            assert t["combo"] == sorted(t["combo"]), f"combo が昇順でない: {t['combo']}"

    def test_no_umatan_ticket_generated(self):
        """馬単チケットは生成されない (type='馬単' は存在しない)"""
        tickets = build_sanrenpuku_dynamic_tickets(_make_ev_race_strict(), _MOCK_RACE)
        for t in tickets:
            assert t.get("type") != "馬単", "馬単チケットが混入している"


class TestBuildTanshoT4Tickets:
    """build_tansho_t4_tickets の単体テスト"""

    def test_both_honmei_and_taikou(self):
        """◎ + ○ 両方あり → 2 点"""
        evals = _make_ev_race_strict()  # ◎馬1番 + ○馬2番
        tickets = build_tansho_t4_tickets(evals, _MOCK_RACE)
        assert len(tickets) == 2
        assert tickets[0]["type"] == "単勝"
        assert tickets[1]["type"] == "単勝"
        marks = {t["mark"] for t in tickets}
        assert "◎" in marks or "◉" in marks, "◎◉が含まれる"
        assert "○" in marks or "〇" in marks, "○が含まれる"

    def test_only_honmei_no_taikou(self):
        """◎のみ (○なし) → 1 点"""
        evals = [
            _MockEval(1, "◎", 0.40, 0.60, 3.0),
            _MockEval(2, "▲", 0.15, 0.30, 8.0),
            _MockEval(3, "△", 0.10, 0.25, 12.0),
        ]
        tickets = build_tansho_t4_tickets(evals, _MOCK_RACE)
        assert len(tickets) == 1
        assert tickets[0]["mark"] in ("◎", "◉")

    def test_only_taikou_no_honmei(self):
        """○のみ (◎◉なし) → 1 点"""
        evals = [
            _MockEval(1, "○", 0.30, 0.50, 4.0),
            _MockEval(2, "▲", 0.15, 0.30, 8.0),
        ]
        tickets = build_tansho_t4_tickets(evals, _MOCK_RACE)
        assert len(tickets) == 1
        assert tickets[0]["mark"] in ("○", "〇")

    def test_no_honmei_no_taikou(self):
        """◎◉○なし → 空リスト"""
        evals = [
            _MockEval(1, "▲", 0.30, 0.50, 4.0),
            _MockEval(2, "△", 0.20, 0.40, 6.0),
        ]
        tickets = build_tansho_t4_tickets(evals, _MOCK_RACE)
        assert tickets == []

    def test_ticket_fields(self):
        """返却チケットのフィールド確認"""
        tickets = build_tansho_t4_tickets(_make_ev_race_strict(), _MOCK_RACE)
        for t in tickets:
            assert "horse_no" in t
            assert "mark" in t
            assert "odds" in t
            assert t["stake"] == 100
            assert t["type"] == "単勝"


class TestDispatchTickets:
    """dispatch_tickets の統合テスト"""

    def test_returns_sanrenpuku_and_tansho(self):
        """通常レース: 三連複 + 単勝 が返る"""
        tickets = dispatch_tickets(_make_ev_race_strict(), _MOCK_RACE)
        types = {t["type"] for t in tickets}
        assert "三連複" in types, "三連複チケットが含まれる"
        assert "単勝" in types, "単勝チケットが含まれる"

    def test_no_umatan_in_any_case(self):
        """馬単チケットは生成されない (A-NONE 確定)"""
        for race_evals in [
            _make_ev_race_strict(),
            _make_ev_race_mid(),
            _make_ev_race_wide(),
        ]:
            tickets = dispatch_tickets(race_evals, _MOCK_RACE)
            for t in tickets:
                assert t.get("type") != "馬単", \
                    f"馬単チケットが混入している (race={race_evals[0].horse.horse_no})"

    def test_skip_when_ev_too_low(self):
        """EV<1.0 レース: 三連複なし、単勝も空 → 全空リスト"""
        tickets = dispatch_tickets(_make_ev_race_skip(), _MOCK_RACE)
        sanrenpuku_tickets = [t for t in tickets if t["type"] == "三連複"]
        assert sanrenpuku_tickets == [], "三連複なし"
        # 単勝は EV に関わらず ◎○ の存在で生成 (設計通り)
        # EV<1.0 でも ◎ が存在すれば単勝は生成される仕様

    def test_flat_list_structure(self):
        """戻り値はフラットなリスト"""
        tickets = dispatch_tickets(_make_ev_race_wide(), _MOCK_RACE)
        assert isinstance(tickets, list), "list 型であること"
        for t in tickets:
            assert isinstance(t, dict), "各要素が dict であること"

    def test_stake_all_100(self):
        """全チケットの stake が 100 円"""
        tickets = dispatch_tickets(_make_ev_race_mid(), _MOCK_RACE)
        for t in tickets:
            assert t["stake"] == 100, f"stake が 100 でない: {t}"
