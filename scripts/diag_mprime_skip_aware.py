# -*- coding: utf-8 -*-
"""真の M' 戦略 自信度別集計 (skip=False のみ / 4 馬券並列)

【バグ修正の背景】
  既存の backtest_master_strategy.py / dashboard.py は bet_decision.skip=True の race
  (期待値不足 etc. で「見送り」判定) も played カウントに含めていた。
  本スクリプトは bet_decision.skip=False の race のみを集計対象とし、
  真の運用 ROI を算出する。

【4 馬券並列集計】
  #1 単勝  : ◎/◉ × 100 円 (1 点 / race)
  #2 三連複 : M' tickets (自信度別 4-10 点) × 100 円 / 点
  #3 馬連  : ◎/◉-○ + ◎/◉-▲ (2 点 × 100 円 = 200 円 / race)
  #4 ワイド : ◎/◉-○ + ◎/◉-▲ (2 点 × 100 円 = 200 円 / race)

【集計軸】
  overall_confidence: SS / S / A / B / C / D (E は skip / F は事実上存在しない)
  bet_decision.skip=False のみカウント (★★★ バグ修正の核心)

【除外ルール】
  - ばんえい (venue_code="65") 除外 (feedback_banei_excluded.md 準拠)
  - bet_decision.skip=True の race は完全除外
  - ◎/◉ が存在しない race はスキップ (単勝・馬連・ワイドが構築不能)
  - M' tickets が空の race は三連複スキップ (単勝/馬連/ワイドは継続集計)

Usage:
  python scripts/diag_mprime_skip_aware.py                          # フル実行
  python scripts/diag_mprime_skip_aware.py --debug                  # 最初の 10 ファイル
  python scripts/diag_mprime_skip_aware.py --start 2025-01-01 --end 2025-12-31
  python scripts/diag_mprime_skip_aware.py --start 2025-01-01 --end 2026-05-29

制約:
  git commit 禁止 / 既存ファイル変更禁止 / 新規スクリプトのみ
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

# プロジェクトルートをパスに追加
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)  # type: ignore[attr-defined]

from src.utils.payout_normalizer import (
    get_payout_for_combo,
    normalize_payouts,
)

# ============================================================
# ロガー設定
# ============================================================
def _setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("diag_mprime_skip_aware")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

    fh = logging.FileHandler(log_path, encoding="utf-8", mode="w")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ============================================================
# 定数
# ============================================================
DIAG_DIR          = os.path.join(ROOT, "data", "_diag")
PRED_DIR          = os.path.join(ROOT, "data", "predictions")
RESULTS_DIR       = os.path.join(ROOT, "data", "results")
RESULTS_FIXED_DIR = os.path.join(ROOT, "data", "results_fixed")

# 集計対象 confidence 順序
CONFIDENCE_ORDER = ["SS", "S", "A", "B", "C", "D", "E", "F"]
# M' 戦略: E/F は skip
MPRIME_BUY  = {"SS", "S", "A", "B", "C", "D"}
MPRIME_SKIP = {"E", "F"}

# 印マーク定義
MARK_HONMEI  = "◎"   # ord=9678
MARK_TEKIPAN = "◉"   # ord=9673
MARK_TAIKOU  = "○"   # ord=9675
MARK_TANNUKE = "▲"   # ord=9650
JIKU_MARKS   = {MARK_HONMEI, MARK_TEKIPAN}

# ばんえい venue_code (feedback_banei_excluded.md 準拠)
BANEI_VENUE_CODE = "65"

# デフォルト日付範囲 (2024-01 〜 2026-05-29 まで)
DEFAULT_START = "2024-01-01"
DEFAULT_END   = "2026-05-29"  # exclusive

# マスター基準
MASTER_HIT_PCT = 25.0   # race ベース hit% >= 25%
MASTER_ROI_PCT = 110.0  # ROI >= 110%

# 単勝投資 (1 点 × 100 円)
TANSHO_STAKE = 100
# 馬連/ワイド投資 (2 点 × 100 円 = 200 円)
UMAREN_WIDE_STAKE = 200

# M' 三連複 パターン定義 (backtest_master_strategy.py STRATEGY_M_PRIME に準拠)
STRATEGY_M_PRIME: Dict[str, Optional[str]] = {
    "SS": "E",   # E パターン (4点)
    "S":  "C",   # C パターン (7点)
    "A":  "C",   # C パターン (7点)
    "B":  "D",   # D パターン (10点)
    "C":  "D",   # D パターン (10点)
    "D":  "D",   # D パターン (10点) ★ M との差分
    "E":  None,  # skip
    "F":  None,  # skip
}

# パターン定義 (三連複フォーメーション m1/m2/m3)
HONMEI_MARKS        = {"◉", "◎"}
HONMEI_TAIKOU_MARKS = {"◉", "◎", "○", "〇"}
C_2ND   = {"○", "〇", "▲"}
D_2ND   = {"◉", "◎", "○", "〇", "▲"}
E_2ND   = {"○", "〇"}
ABC_3RD = {"○", "〇", "▲", "△", "★", "☆"}
E_3RD   = {"▲", "△", "★", "☆"}

PATTERNS: Dict[str, Tuple[frozenset, frozenset, frozenset]] = {
    "C": (frozenset(HONMEI_MARKS),        frozenset(C_2ND),   frozenset(ABC_3RD)),
    "D": (frozenset(HONMEI_TAIKOU_MARKS), frozenset(D_2ND),   frozenset(ABC_3RD)),
    "E": (frozenset(HONMEI_MARKS),        frozenset(E_2ND),   frozenset(E_3RD)),
}

# 印優先度
MARK_PRIORITY: Dict[str, int] = {
    "◉": 0, "◎": 1, "○": 2, "〇": 2, "▲": 3, "△": 4, "★": 5, "☆": 6
}


# ============================================================
# 三連複チケット構築 (backtest_master_strategy.py と同ロジック)
# ============================================================
def _filter_active(horses: List[Dict]) -> List[Dict]:
    """取消・特殊競争除外の出走馬だけ返す"""
    return [h for h in horses
            if not h.get("is_tokusen_kiken") and not h.get("is_scratched")]


def _horse_mark(h: Dict) -> str:
    return (h.get("mark") or "").strip()


def _horses_by_marks(horses: List[Dict], marks: frozenset) -> List[Dict]:
    """指定印の出走馬リスト (重複除去・印優先度昇順 + composite 降順)"""
    cands = [h for h in horses if _horse_mark(h) in marks]
    cands.sort(key=lambda h: (MARK_PRIORITY.get(_horse_mark(h), 99),
                               -(h.get("composite") or 0)))
    seen, out = set(), []
    for h in cands:
        no = h.get("horse_no")
        if no and no not in seen:
            seen.add(no)
            out.append(h)
    return out


def build_sanrenpuku_tickets(
    horses: List[Dict],
    m1: frozenset,
    m2: frozenset,
    m3: frozenset,
) -> List[Tuple[int, int, int]]:
    """三連複フォーメーション買い目 (unordered sorted tuple)"""
    h1 = _horses_by_marks(horses, m1)
    h2 = _horses_by_marks(horses, m2)
    h3 = _horses_by_marks(horses, m3)
    seen, tickets = set(), []
    for ha in h1:
        a_no = ha.get("horse_no")
        for hb in h2:
            b_no = hb.get("horse_no")
            if b_no == a_no:
                continue
            for hc in h3:
                c_no = hc.get("horse_no")
                if c_no == a_no or c_no == b_no:
                    continue
                key = tuple(sorted([a_no, b_no, c_no]))
                if key in seen:
                    continue
                seen.add(key)
                tickets.append(key)
    return tickets


# ============================================================
# 印取得 (単勝・馬連・ワイド用)
# ============================================================
def _extract_marks(
    horses: List[Dict],
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """horses リストから (軸馬 horse_no, ○ horse_no, ▲ horse_no) を取得

    is_scratched=True の馬は除外。
    Returns: (jiku, taikou, tannuke) の horse_no タプル。None は未検出。
    """
    jiku    = None  # ◎ or ◉
    taikou  = None  # ○
    tannuke = None  # ▲

    for h in horses:
        if h.get("is_scratched") is True:
            continue
        mark = h.get("mark", "")
        hno  = h.get("horse_no")
        if hno is None:
            continue
        if mark in JIKU_MARKS:
            jiku = hno
        elif mark == MARK_TAIKOU:
            taikou = hno
        elif mark == MARK_TANNUKE:
            tannuke = hno

    return jiku, taikou, tannuke


# ============================================================
# results.json 読み込み
# ============================================================
def _load_results_payouts(
    date_start: str,
    date_end: str,
    logger: logging.Logger,
) -> Dict[str, Dict]:
    """results.json から race_id → 正規化 payouts をロード
    results_fixed/ を優先使用 (ワイド払戻バグ修正版)
    """
    result_map: Dict[str, Dict] = {}

    start_yyyymmdd = date_start.replace("-", "")
    end_yyyymmdd   = date_end.replace("-", "")

    pattern = os.path.join(RESULTS_DIR, "*_results.json")
    files   = sorted(glob.glob(pattern))

    fixed_pattern = os.path.join(RESULTS_FIXED_DIR, "*_results.json")
    fixed_files_map = {os.path.basename(f): f for f in glob.glob(fixed_pattern)}
    logger.info(f"results_fixed/ ファイル数: {len(fixed_files_map)} (ワイド払戻バグ修正版優先)")

    loaded_files = 0
    fixed_used   = 0
    for fp in files:
        fn       = os.path.basename(fp)
        date_str = fn[:8]
        if not date_str.isdigit() or len(date_str) != 8:
            continue
        if not (start_yyyymmdd <= date_str < end_yyyymmdd):
            continue

        actual_fp = fixed_files_map.get(fn, fp)
        if actual_fp != fp:
            fixed_used += 1

        try:
            with open(actual_fp, encoding="utf-8") as f:
                day_data = json.load(f)
        except Exception as e:
            logger.warning(f"results.json 読み込みエラー: {fp} — {e}")
            continue

        for race_id_str, rdata in day_data.items():
            raw_p  = rdata.get("payouts", {})
            norm_p = normalize_payouts(raw_p)
            result_map[str(race_id_str)] = norm_p

        loaded_files += 1

    logger.info(
        f"results.json ロード: {loaded_files} ファイル / {len(result_map)} レース "
        f"({date_start}〜{date_end}) / results_fixed 優先: {fixed_used} ファイル"
    )
    return result_map


# ============================================================
# pred.json ファイルリスト収集
# ============================================================
def _collect_pred_files(
    date_start: str,
    date_end: str,
    debug: bool,
    logger: logging.Logger,
) -> List[str]:
    """指定期間の pred.json ファイルリストを返す"""
    start_yyyymmdd = date_start.replace("-", "")
    end_yyyymmdd   = date_end.replace("-", "")

    pattern = os.path.join(PRED_DIR, "*_pred.json")
    files   = sorted(glob.glob(pattern))

    filtered = []
    for fp in files:
        fn = os.path.basename(fp)
        if fn.endswith(".bak"):
            continue
        date_str = fn[:8]
        if not date_str.isdigit() or len(date_str) != 8:
            continue
        if not (start_yyyymmdd <= date_str < end_yyyymmdd):
            continue
        filtered.append(fp)

    if debug:
        filtered = filtered[:10]
        logger.info(f"[デバッグモード] 最初の {len(filtered)} ファイルのみ処理")

    logger.info(f"pred.json 対象ファイル数: {len(filtered)} ({date_start}〜{date_end})")
    return filtered


# ============================================================
# 単勝払戻取得
# ============================================================
def _get_tansho_payout(norm_payouts: Dict, horse_no: int) -> int:
    """単勝払戻取得 (combo='1' 等)"""
    return get_payout_for_combo(norm_payouts, "tansho", [horse_no])


# ============================================================
# 三連複払戻取得
# ============================================================
def _get_sanrenpuku_payout(
    norm_payouts: Dict,
    combo: Tuple[int, int, int],
) -> int:
    """三連複払戻取得 (sorted tuple)"""
    return get_payout_for_combo(norm_payouts, "sanrenpuku", list(combo))


# ============================================================
# 集計構造
# ============================================================
def _new_agg() -> Dict:
    """confidence 別 × 4 馬券の集計構造"""
    return {
        # 単勝
        "tansho_played":  0,
        "tansho_hit":     0,
        "tansho_payout":  0,
        # 三連複
        "sanren_played":  0,
        "sanren_hit":     0,
        "sanren_stake":   0,
        "sanren_payout":  0,
        # 馬連 ◎-○
        "umaren_a_played": 0,
        "umaren_a_hit":    0,
        "umaren_a_payout": 0,
        # 馬連 ◎-▲
        "umaren_b_played": 0,
        "umaren_b_hit":    0,
        "umaren_b_payout": 0,
        # ワイド ◎-○
        "wide_a_played": 0,
        "wide_a_hit":    0,
        "wide_a_payout": 0,
        # ワイド ◎-▲
        "wide_b_played": 0,
        "wide_b_hit":    0,
        "wide_b_payout": 0,
        # race 集計
        "race_played":   0,   # skip=False race 数
        "race_hit_any":  0,   # いずれかの馬券的中 race 数
    }


# ============================================================
# メイン集計ループ
# ============================================================
def run(
    date_start: str,
    date_end: str,
    debug: bool,
    logger: logging.Logger,
) -> Tuple[Dict, Dict, Dict]:
    """4 馬券並列集計 (skip=False のみ)

    Returns:
        (conf_agg, monthly_agg, counters)
    """
    # 集計初期化
    conf_agg: Dict[str, Dict]    = {c: _new_agg() for c in CONFIDENCE_ORDER}
    monthly_agg: Dict[Tuple[str, str], Dict] = defaultdict(lambda: {**_new_agg()})

    # results.json 全件ロード
    logger.info("=== results.json ロード開始 ===")
    results_map = _load_results_payouts(date_start, date_end, logger)
    logger.info(f"results_map: {len(results_map):,} races")

    # pred.json ファイルリスト
    pred_files  = _collect_pred_files(date_start, date_end, debug, logger)
    total_files = len(pred_files)

    # カウンタ
    cnt_total         = 0   # 全 race 数
    cnt_banei         = 0   # ばんえい除外
    cnt_skip_true     = 0   # bet_decision.skip=True 除外 (★★★ 今回のバグ修正核心)
    cnt_no_conf       = 0   # confidence 不明
    cnt_no_marks      = 0   # ◎/◉ 不在
    cnt_no_results    = 0   # results なし
    cnt_played        = 0   # 集計対象 (skip=False + marks あり + results あり)

    logger.info("=== 4 馬券並列集計ループ開始 ===")
    logger.info("  【バグ修正の核心】bet_decision.skip=True race を除外して真の運用 ROI を算出")
    start_time = time.time()

    for file_idx, fp in enumerate(pred_files):
        fn       = os.path.basename(fp)
        date_str = fn[:8]   # YYYYMMDD
        yyyymm   = date_str[:6]  # YYYYMM

        # 進捗バー (50 ファイル毎)
        if file_idx % 50 == 0:
            elapsed = time.time() - start_time
            pct     = (file_idx / total_files * 100) if total_files > 0 else 0
            bar     = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            eta     = (elapsed / max(file_idx, 1)) * (total_files - file_idx)
            logger.info(
                f"[{bar}] {pct:.1f}% ({file_idx}/{total_files}) "
                f"played={cnt_played:,} skip_true={cnt_skip_true:,} "
                f"経過{elapsed:.0f}s 残{eta:.0f}s"
            )

        try:
            with open(fp, encoding="utf-8") as f:
                pred_data = json.load(f)
        except Exception as e:
            logger.warning(f"pred.json 読み込みエラー: {fp} — {e}")
            continue

        races = pred_data.get("races", [])

        for race in races:
            cnt_total += 1

            # ─────────────────────────────────────────
            # 1. ばんえい除外
            # ─────────────────────────────────────────
            venue_code = str(race.get("venue_code") or "")
            if not venue_code:
                race_id_str = str(race.get("race_id", ""))
                if len(race_id_str) >= 6:
                    venue_code = race_id_str[4:6]
            if venue_code == BANEI_VENUE_CODE:
                cnt_banei += 1
                continue

            # ─────────────────────────────────────────
            # 2. ★★★ bet_decision.skip = True は完全除外
            #    (見送り判定された race / played カウントしない)
            # ─────────────────────────────────────────
            bet_decision = race.get("bet_decision") or {}
            # skip フィールドが存在しない場合 (旧形式) は True とみなす
            # → 旧形式 pred.json は bet_decision 自体なし = skip とみなす
            if bet_decision:
                bd_skip = bet_decision.get("skip", True)
            else:
                # bet_decision フィールドなし (旧形式 pred.json)
                # → M' 戦略マッピングで buy/skip を判断
                bd_skip = None  # 後で confidence から判断

            # confidence 取得
            confidence = race.get("overall_confidence") or race.get("confidence", "")
            if confidence not in CONFIDENCE_ORDER:
                cnt_no_conf += 1
                continue

            # bet_decision がない旧形式の場合: M' 戦略マッピングで skip 判断
            if bd_skip is None:
                bd_skip = (STRATEGY_M_PRIME.get(confidence) is None)

            if bd_skip:
                cnt_skip_true += 1
                continue

            # ─────────────────────────────────────────
            # 3. 印取得 (単勝・馬連・ワイド用)
            # ─────────────────────────────────────────
            horses = race.get("horses", [])
            jiku, taikou, tannuke = _extract_marks(horses)

            if jiku is None:
                # ◎/◉ がない race は全馬券スキップ
                cnt_no_marks += 1
                continue

            # ─────────────────────────────────────────
            # 4. results 取得
            # ─────────────────────────────────────────
            race_id_str  = str(race.get("race_id", ""))
            norm_payouts = results_map.get(race_id_str, {})
            if not norm_payouts:
                cnt_no_results += 1
                continue

            cnt_played += 1

            # ─────────────────────────────────────────
            # 5. 三連複チケット構築 (M' 戦略パターン)
            # ─────────────────────────────────────────
            pattern_name = STRATEGY_M_PRIME.get(confidence)
            sanren_tickets: List[Tuple[int, int, int]] = []
            if pattern_name and pattern_name in PATTERNS:
                m1, m2, m3 = PATTERNS[pattern_name]
                active_horses = _filter_active(horses)
                sanren_tickets = build_sanrenpuku_tickets(active_horses, m1, m2, m3)

            # ─────────────────────────────────────────
            # 6. 4 馬券 払戻計算
            # ─────────────────────────────────────────

            # === 単勝 ===
            tansho_payout = _get_tansho_payout(norm_payouts, jiku)
            tansho_hit    = 1 if tansho_payout > 0 else 0

            # === 三連複 ===
            sanren_stake  = len(sanren_tickets) * 100
            sanren_payout = 0
            sanren_hit    = 0
            for combo in sanren_tickets:
                p = _get_sanrenpuku_payout(norm_payouts, combo)
                sanren_payout += p
                if p > 0:
                    sanren_hit = 1  # race ベース hit (1 点でも的中で OK)

            # === 馬連 ◎-○ (taikou があれば) ===
            umaren_a_payout = 0
            umaren_a_hit    = 0
            umaren_a_played = 0
            if taikou is not None:
                umaren_a_played = 1
                umaren_a_payout = get_payout_for_combo(norm_payouts, "umaren", [jiku, taikou])
                umaren_a_hit    = 1 if umaren_a_payout > 0 else 0

            # === 馬連 ◎-▲ (tannuke があれば) ===
            umaren_b_payout = 0
            umaren_b_hit    = 0
            umaren_b_played = 0
            if tannuke is not None:
                umaren_b_played = 1
                umaren_b_payout = get_payout_for_combo(norm_payouts, "umaren", [jiku, tannuke])
                umaren_b_hit    = 1 if umaren_b_payout > 0 else 0

            # === ワイド ◎-○ ===
            wide_a_payout = 0
            wide_a_hit    = 0
            wide_a_played = 0
            if taikou is not None:
                wide_a_played = 1
                wide_a_payout = get_payout_for_combo(norm_payouts, "wide", [jiku, taikou])
                wide_a_hit    = 1 if wide_a_payout > 0 else 0

            # === ワイド ◎-▲ ===
            wide_b_payout = 0
            wide_b_hit    = 0
            wide_b_played = 0
            if tannuke is not None:
                wide_b_played = 1
                wide_b_payout = get_payout_for_combo(norm_payouts, "wide", [jiku, tannuke])
                wide_b_hit    = 1 if wide_b_payout > 0 else 0

            # race ベース any_hit
            any_hit = (
                tansho_hit or sanren_hit or
                umaren_a_hit or umaren_b_hit or
                wide_a_hit or wide_b_hit
            )

            # ─────────────────────────────────────────
            # 7. confidence 別集計に加算
            # ─────────────────────────────────────────
            agg = conf_agg[confidence]
            agg["race_played"]    += 1
            agg["race_hit_any"]   += 1 if any_hit else 0

            agg["tansho_played"]  += 1
            agg["tansho_hit"]     += tansho_hit
            agg["tansho_payout"]  += tansho_payout

            if sanren_tickets:
                agg["sanren_played"]  += 1
                agg["sanren_hit"]     += sanren_hit
                agg["sanren_stake"]   += sanren_stake
                agg["sanren_payout"]  += sanren_payout

            agg["umaren_a_played"] += umaren_a_played
            agg["umaren_a_hit"]    += umaren_a_hit
            agg["umaren_a_payout"] += umaren_a_payout

            agg["umaren_b_played"] += umaren_b_played
            agg["umaren_b_hit"]    += umaren_b_hit
            agg["umaren_b_payout"] += umaren_b_payout

            agg["wide_a_played"]   += wide_a_played
            agg["wide_a_hit"]      += wide_a_hit
            agg["wide_a_payout"]   += wide_a_payout

            agg["wide_b_played"]   += wide_b_played
            agg["wide_b_hit"]      += wide_b_hit
            agg["wide_b_payout"]   += wide_b_payout

            # ─────────────────────────────────────────
            # 8. 月別集計
            # ─────────────────────────────────────────
            mkey = (yyyymm, confidence)
            magg = monthly_agg[mkey]
            magg["race_played"]    += 1
            magg["race_hit_any"]   += 1 if any_hit else 0

            magg["tansho_played"]  += 1
            magg["tansho_hit"]     += tansho_hit
            magg["tansho_payout"]  += tansho_payout

            if sanren_tickets:
                magg["sanren_played"]  += 1
                magg["sanren_hit"]     += sanren_hit
                magg["sanren_stake"]   += sanren_stake
                magg["sanren_payout"]  += sanren_payout

            magg["umaren_a_played"] += umaren_a_played
            magg["umaren_a_hit"]    += umaren_a_hit
            magg["umaren_a_payout"] += umaren_a_payout

            magg["umaren_b_played"] += umaren_b_played
            magg["umaren_b_hit"]    += umaren_b_hit
            magg["umaren_b_payout"] += umaren_b_payout

            magg["wide_a_played"]   += wide_a_played
            magg["wide_a_hit"]      += wide_a_hit
            magg["wide_a_payout"]   += wide_a_payout

            magg["wide_b_played"]   += wide_b_played
            magg["wide_b_hit"]      += wide_b_hit
            magg["wide_b_payout"]   += wide_b_payout

    # 最終進捗
    elapsed = time.time() - start_time
    logger.info(
        f"[████████████████████] 100.0% ({total_files}/{total_files}) "
        f"完了 経過{elapsed:.1f}s"
    )
    logger.info("=== 4 馬券並列集計完了 ===")
    logger.info(f"  総 race 数:                  {cnt_total:,}")
    logger.info(f"  ばんえい除外:                 {cnt_banei:,}")
    logger.info(f"  bet_decision.skip=True 除外:  {cnt_skip_true:,}  ← バグ修正の核心")
    logger.info(f"  confidence 不明:              {cnt_no_conf:,}")
    logger.info(f"  ◎/◉ なし:                   {cnt_no_marks:,}")
    logger.info(f"  results なし:                 {cnt_no_results:,}")
    logger.info(f"  集計対象 race 数 (skip=False): {cnt_played:,}")

    counters = {
        "total_races":   cnt_total,
        "banei_skip":    cnt_banei,
        "skip_true":     cnt_skip_true,
        "no_conf":       cnt_no_conf,
        "no_marks":      cnt_no_marks,
        "no_results":    cnt_no_results,
        "played":        cnt_played,
    }
    return conf_agg, dict(monthly_agg), counters


# ============================================================
# ユーティリティ
# ============================================================
def _roi(payout: int, stake: int) -> float:
    if stake == 0:
        return 0.0
    return payout / stake * 100.0


def _hit_pct(hit: int, played: int) -> float:
    if played == 0:
        return 0.0
    return hit / played * 100.0


# ============================================================
# CSV 出力: サマリ (confidence × 4 馬券 = 最大 24 セル)
# ============================================================
def write_summary_csv(
    conf_agg: Dict[str, Dict],
    output_path: str,
    logger: logging.Logger,
) -> int:
    """confidence 別サマリ CSV 出力

    列:
      confidence, skip_aware_strategy,
      race_played, race_hit_any, race_hit_pct,
      tansho_played, tansho_hit, tansho_hit_pct, tansho_stake, tansho_payout, tansho_roi,
      sanren_played,  sanren_hit,  sanren_hit_pct, sanren_stake, sanren_payout, sanren_roi,
      umaren_ab_played, umaren_ab_hit, umaren_ab_hit_pct, umaren_ab_stake, umaren_ab_payout, umaren_ab_roi,
      wide_ab_played, wide_ab_hit, wide_ab_hit_pct, wide_ab_stake, wide_ab_payout, wide_ab_roi,
      master_tansho, master_sanren, master_umaren, master_wide
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    header = [
        "confidence", "skip_aware_strategy",
        "race_played", "race_hit_any", "race_hit_pct",
        # 単勝
        "tansho_played", "tansho_hit", "tansho_hit_pct",
        "tansho_stake", "tansho_payout", "tansho_roi",
        # 三連複
        "sanren_played", "sanren_hit", "sanren_hit_pct",
        "sanren_stake", "sanren_payout", "sanren_roi",
        # 馬連合計 (◎-○ + ◎-▲)
        "umaren_ab_played", "umaren_ab_hit", "umaren_ab_hit_pct",
        "umaren_ab_stake", "umaren_ab_payout", "umaren_ab_roi",
        # ワイド合計 (◎-○ + ◎-▲)
        "wide_ab_played", "wide_ab_hit", "wide_ab_hit_pct",
        "wide_ab_stake", "wide_ab_payout", "wide_ab_roi",
        # マスター基準達成
        "master_tansho", "master_sanren", "master_umaren", "master_wide",
    ]

    rows = []
    master_total = 0

    for conf in CONFIDENCE_ORDER:
        agg = conf_agg.get(conf, _new_agg())
        strategy = "BUY" if conf in MPRIME_BUY else "SKIP"

        rp = agg["race_played"]

        # 単勝
        t_p   = agg["tansho_played"]
        t_h   = agg["tansho_hit"]
        t_pay = agg["tansho_payout"]
        t_stk = t_p * TANSHO_STAKE
        t_hp  = _hit_pct(t_h, t_p)
        t_roi = _roi(t_pay, t_stk)

        # 三連複
        s_p   = agg["sanren_played"]
        s_h   = agg["sanren_hit"]
        s_stk = agg["sanren_stake"]
        s_pay = agg["sanren_payout"]
        s_hp  = _hit_pct(s_h, s_p)
        s_roi = _roi(s_pay, s_stk)

        # 馬連合計 (◎-○ + ◎-▲ を合算)
        ua_p   = agg["umaren_a_played"] + agg["umaren_b_played"]
        ua_h   = agg["umaren_a_hit"]    + agg["umaren_b_hit"]
        ua_pay = agg["umaren_a_payout"] + agg["umaren_b_payout"]
        ua_stk = ua_p * 100
        ua_hp  = _hit_pct(ua_h, ua_p)
        ua_roi = _roi(ua_pay, ua_stk)

        # ワイド合計 (◎-○ + ◎-▲ を合算)
        wa_p   = agg["wide_a_played"] + agg["wide_b_played"]
        wa_h   = agg["wide_a_hit"]    + agg["wide_b_hit"]
        wa_pay = agg["wide_a_payout"] + agg["wide_b_payout"]
        wa_stk = wa_p * 100
        wa_hp  = _hit_pct(wa_h, wa_p)
        wa_roi = _roi(wa_pay, wa_stk)

        # マスター基準達成判定 (race ベース hit% は tansho と同一 played で判定)
        def _ach(hp: float, roi: float) -> bool:
            return (strategy == "BUY") and (hp >= MASTER_HIT_PCT) and (roi >= MASTER_ROI_PCT)

        m_tan = _ach(t_hp, t_roi)
        m_san = _ach(s_hp, s_roi)
        m_uma = _ach(ua_hp, ua_roi)
        m_wid = _ach(wa_hp, wa_roi)
        cell_achieved = sum([m_tan, m_san, m_uma, m_wid])
        master_total += cell_achieved

        rows.append({
            "confidence":        conf,
            "skip_aware_strategy": strategy,
            "race_played":       rp,
            "race_hit_any":      agg["race_hit_any"],
            "race_hit_pct":      f"{_hit_pct(agg['race_hit_any'], rp):.2f}",
            # 単勝
            "tansho_played":     t_p,
            "tansho_hit":        t_h,
            "tansho_hit_pct":    f"{t_hp:.2f}",
            "tansho_stake":      t_stk,
            "tansho_payout":     t_pay,
            "tansho_roi":        f"{t_roi:.2f}",
            # 三連複
            "sanren_played":     s_p,
            "sanren_hit":        s_h,
            "sanren_hit_pct":    f"{s_hp:.2f}",
            "sanren_stake":      s_stk,
            "sanren_payout":     s_pay,
            "sanren_roi":        f"{s_roi:.2f}",
            # 馬連合計
            "umaren_ab_played":  ua_p,
            "umaren_ab_hit":     ua_h,
            "umaren_ab_hit_pct": f"{ua_hp:.2f}",
            "umaren_ab_stake":   ua_stk,
            "umaren_ab_payout":  ua_pay,
            "umaren_ab_roi":     f"{ua_roi:.2f}",
            # ワイド合計
            "wide_ab_played":    wa_p,
            "wide_ab_hit":       wa_h,
            "wide_ab_hit_pct":   f"{wa_hp:.2f}",
            "wide_ab_stake":     wa_stk,
            "wide_ab_payout":    wa_pay,
            "wide_ab_roi":       f"{wa_roi:.2f}",
            # マスター基準
            "master_tansho":     "YES" if m_tan else "",
            "master_sanren":     "YES" if m_san else "",
            "master_umaren":     "YES" if m_uma else "",
            "master_wide":       "YES" if m_wid else "",
        })

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"サマリ CSV 出力: {output_path} ({len(rows)} 行) マスター基準達成: {master_total} セル")
    return master_total


# ============================================================
# CSV 出力: 月別 (yyyymm × confidence × 4 馬券)
# ============================================================
def write_monthly_csv(
    monthly_agg: Dict,
    output_path: str,
    logger: logging.Logger,
) -> None:
    """月別 × confidence CSV 出力"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # 月リスト生成 (2024-01 〜 2026-05)
    months_order = []
    for year in range(2024, 2027):
        for month in range(1, 13):
            yyyymm = f"{year:04d}{month:02d}"
            if "202401" <= yyyymm <= "202605":
                months_order.append(yyyymm)

    header = [
        "yyyymm", "confidence", "skip_aware_strategy",
        "race_played",
        "tansho_played", "tansho_hit", "tansho_hit_pct", "tansho_roi",
        "sanren_played",  "sanren_hit",  "sanren_hit_pct",  "sanren_roi",
        "umaren_ab_played", "umaren_ab_hit", "umaren_ab_hit_pct", "umaren_ab_roi",
        "wide_ab_played",   "wide_ab_hit",   "wide_ab_hit_pct",   "wide_ab_roi",
    ]

    rows = []
    for yyyymm in months_order:
        for conf in CONFIDENCE_ORDER:
            agg = monthly_agg.get((yyyymm, conf))
            if agg is None:
                continue

            rp     = agg["race_played"]
            if rp == 0:
                continue

            strategy = "BUY" if conf in MPRIME_BUY else "SKIP"

            t_p   = agg["tansho_played"]
            t_h   = agg["tansho_hit"]
            t_pay = agg["tansho_payout"]
            t_stk = t_p * TANSHO_STAKE

            s_p   = agg["sanren_played"]
            s_h   = agg["sanren_hit"]
            s_stk = agg["sanren_stake"]
            s_pay = agg["sanren_payout"]

            ua_p   = agg["umaren_a_played"] + agg["umaren_b_played"]
            ua_h   = agg["umaren_a_hit"]    + agg["umaren_b_hit"]
            ua_pay = agg["umaren_a_payout"] + agg["umaren_b_payout"]
            ua_stk = ua_p * 100

            wa_p   = agg["wide_a_played"] + agg["wide_b_played"]
            wa_h   = agg["wide_a_hit"]    + agg["wide_b_hit"]
            wa_pay = agg["wide_a_payout"] + agg["wide_b_payout"]
            wa_stk = wa_p * 100

            rows.append({
                "yyyymm":           yyyymm,
                "confidence":       conf,
                "skip_aware_strategy": strategy,
                "race_played":      rp,
                "tansho_played":    t_p,
                "tansho_hit":       t_h,
                "tansho_hit_pct":   f"{_hit_pct(t_h, t_p):.2f}",
                "tansho_roi":       f"{_roi(t_pay, t_stk):.2f}",
                "sanren_played":    s_p,
                "sanren_hit":       s_h,
                "sanren_hit_pct":   f"{_hit_pct(s_h, s_p):.2f}",
                "sanren_roi":       f"{_roi(s_pay, s_stk):.2f}",
                "umaren_ab_played": ua_p,
                "umaren_ab_hit":    ua_h,
                "umaren_ab_hit_pct":f"{_hit_pct(ua_h, ua_p):.2f}",
                "umaren_ab_roi":    f"{_roi(ua_pay, ua_stk):.2f}",
                "wide_ab_played":   wa_p,
                "wide_ab_hit":      wa_h,
                "wide_ab_hit_pct":  f"{_hit_pct(wa_h, wa_p):.2f}",
                "wide_ab_roi":      f"{_roi(wa_pay, wa_stk):.2f}",
            })

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"月別 CSV 出力: {output_path} ({len(rows)} 行)")


# ============================================================
# CSV 出力: 全期間加重 4 馬券 × 6 confidence
# ============================================================
def write_all_periods_csv(
    conf_agg: Dict[str, Dict],
    output_path: str,
    logger: logging.Logger,
) -> None:
    """全期間 4 馬券 × confidence 横断 CSV (最終サマリ用)"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    header = [
        "confidence", "bet_type",
        "played", "hit", "hit_pct", "stake", "payout", "balance", "roi",
        "master_achieved",
    ]

    rows = []

    for conf in CONFIDENCE_ORDER:
        agg      = conf_agg.get(conf, _new_agg())
        strategy = "BUY" if conf in MPRIME_BUY else "SKIP"

        def _row(bet: str, p: int, h: int, stk: int, pay: int) -> Dict:
            hp  = _hit_pct(h, p)
            roi = _roi(pay, stk)
            ach = (strategy == "BUY") and (hp >= MASTER_HIT_PCT) and (roi >= MASTER_ROI_PCT)
            return {
                "confidence":      conf,
                "bet_type":        bet,
                "played":          p,
                "hit":             h,
                "hit_pct":         f"{hp:.2f}",
                "stake":           stk,
                "payout":          pay,
                "balance":         pay - stk,
                "roi":             f"{roi:.2f}",
                "master_achieved": "YES" if ach else "",
            }

        t_p   = agg["tansho_played"]
        s_p   = agg["sanren_played"]
        ua_p  = agg["umaren_a_played"] + agg["umaren_b_played"]
        wa_p  = agg["wide_a_played"]   + agg["wide_b_played"]

        rows.append(_row(
            "単勝 ◎/◉",
            t_p, agg["tansho_hit"], t_p * TANSHO_STAKE, agg["tansho_payout"]
        ))
        rows.append(_row(
            "三連複 M'",
            s_p, agg["sanren_hit"], agg["sanren_stake"], agg["sanren_payout"]
        ))
        rows.append(_row(
            "馬連 ◎-○+◎-▲",
            ua_p,
            agg["umaren_a_hit"] + agg["umaren_b_hit"],
            ua_p * 100,
            agg["umaren_a_payout"] + agg["umaren_b_payout"],
        ))
        rows.append(_row(
            "ワイド ◎-○+◎-▲",
            wa_p,
            agg["wide_a_hit"] + agg["wide_b_hit"],
            wa_p * 100,
            agg["wide_a_payout"] + agg["wide_b_payout"],
        ))

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"全期間 CSV 出力: {output_path} ({len(rows)} 行)")


# ============================================================
# 最終レポート出力
# ============================================================
def print_report(
    conf_agg: Dict[str, Dict],
    counters: Dict,
    master_total: int,
    logger: logging.Logger,
) -> None:
    """最終集計結果をログ出力"""
    logger.info("")
    logger.info("=" * 78)
    logger.info("【真の M' 戦略 集計結果 (bet_decision.skip=False のみ / 4 馬券並列)】")
    logger.info("  バグ修正: skip=True race を除外した真の運用 ROI")
    logger.info("=" * 78)
    logger.info(f"  総 race 数:                     {counters['total_races']:,}")
    logger.info(f"  ばんえい除外:                    {counters['banei_skip']:,}")
    logger.info(f"  bet_decision.skip=True 除外:     {counters['skip_true']:,}  ← 今回の修正核心")
    logger.info(f"  confidence 不明:                 {counters['no_conf']:,}")
    logger.info(f"  ◎/◉ なし:                      {counters['no_marks']:,}")
    logger.info(f"  results なし:                    {counters['no_results']:,}")
    logger.info(f"  集計対象 race 数 (skip=False):   {counters['played']:,}")
    logger.info("")

    # 全体合計 (買う confidence 合算)
    total_rp         = sum(conf_agg[c]["race_played"]    for c in MPRIME_BUY if c in conf_agg)

    total_t_p   = sum(conf_agg[c]["tansho_played"]  for c in MPRIME_BUY if c in conf_agg)
    total_t_h   = sum(conf_agg[c]["tansho_hit"]     for c in MPRIME_BUY if c in conf_agg)
    total_t_pay = sum(conf_agg[c]["tansho_payout"]  for c in MPRIME_BUY if c in conf_agg)
    total_t_stk = total_t_p * TANSHO_STAKE

    total_s_p   = sum(conf_agg[c]["sanren_played"]  for c in MPRIME_BUY if c in conf_agg)
    total_s_h   = sum(conf_agg[c]["sanren_hit"]     for c in MPRIME_BUY if c in conf_agg)
    total_s_stk = sum(conf_agg[c]["sanren_stake"]   for c in MPRIME_BUY if c in conf_agg)
    total_s_pay = sum(conf_agg[c]["sanren_payout"]  for c in MPRIME_BUY if c in conf_agg)

    total_ua_p   = sum(conf_agg[c]["umaren_a_played"] + conf_agg[c]["umaren_b_played"] for c in MPRIME_BUY if c in conf_agg)
    total_ua_h   = sum(conf_agg[c]["umaren_a_hit"]    + conf_agg[c]["umaren_b_hit"]    for c in MPRIME_BUY if c in conf_agg)
    total_ua_pay = sum(conf_agg[c]["umaren_a_payout"] + conf_agg[c]["umaren_b_payout"] for c in MPRIME_BUY if c in conf_agg)
    total_ua_stk = total_ua_p * 100

    total_wa_p   = sum(conf_agg[c]["wide_a_played"] + conf_agg[c]["wide_b_played"] for c in MPRIME_BUY if c in conf_agg)
    total_wa_h   = sum(conf_agg[c]["wide_a_hit"]    + conf_agg[c]["wide_b_hit"]    for c in MPRIME_BUY if c in conf_agg)
    total_wa_pay = sum(conf_agg[c]["wide_a_payout"] + conf_agg[c]["wide_b_payout"] for c in MPRIME_BUY if c in conf_agg)
    total_wa_stk = total_wa_p * 100

    logger.info("【全体合計 (SS/S/A/B/C/D 合算)】")
    logger.info(f"  race played: {total_rp:,}")
    logger.info("")
    logger.info(
        f"  {'馬券':12s} | {'played':>7s} | {'hit':>6s} | {'hit%':>7s} | "
        f"{'投資':>11s} | {'払戻':>11s} | {'収支':>11s} | {'ROI%':>8s}"
    )
    logger.info("  " + "-" * 82)

    def _fmtrow(bet: str, p: int, h: int, stk: int, pay: int) -> str:
        hp  = _hit_pct(h, p)
        roi = _roi(pay, stk)
        bal = pay - stk
        return (
            f"  {bet:12s} | {p:>7,} | {h:>6,} | {hp:>6.1f}% | "
            f"{stk:>11,} | {pay:>11,} | {bal:>+11,} | {roi:>7.1f}%"
        )

    logger.info(_fmtrow("単勝 ◎/◉",       total_t_p,  total_t_h,  total_t_stk,  total_t_pay))
    logger.info(_fmtrow("三連複 M'",       total_s_p,  total_s_h,  total_s_stk,  total_s_pay))
    logger.info(_fmtrow("馬連 ◎-○+◎-▲", total_ua_p, total_ua_h, total_ua_stk, total_ua_pay))
    logger.info(_fmtrow("ワイド ◎-○+◎-▲", total_wa_p, total_wa_h, total_wa_stk, total_wa_pay))
    logger.info("")

    # confidence 別詳細
    logger.info("【confidence 別 ROI (SS/S/A/B/C/D × 4 馬券 = 24 セル / E/F は SKIP)】")
    logger.info(
        f"  {'conf':4s} | {'race_played':>11s} | "
        f"{'単勝 ROI':>9s} | {'三連複 ROI':>11s} | "
        f"{'馬連 ROI':>9s} | {'ワイド ROI':>10s}"
    )
    logger.info("  " + "-" * 75)

    master_achieved = 0
    for conf in CONFIDENCE_ORDER:
        agg      = conf_agg.get(conf, _new_agg())
        strategy = "BUY" if conf in MPRIME_BUY else "SKIP"
        rp       = agg["race_played"]

        if strategy == "SKIP":
            logger.info(f"  {conf:4s} | [SKIP] bet_decision.skip=True が大半")
            continue

        t_roi  = _roi(agg["tansho_payout"],  agg["tansho_played"] * TANSHO_STAKE)
        s_roi  = _roi(agg["sanren_payout"],  agg["sanren_stake"])
        ua_roi = _roi(
            agg["umaren_a_payout"] + agg["umaren_b_payout"],
            (agg["umaren_a_played"] + agg["umaren_b_played"]) * 100,
        )
        wa_roi = _roi(
            agg["wide_a_payout"] + agg["wide_b_payout"],
            (agg["wide_a_played"] + agg["wide_b_played"]) * 100,
        )

        def _ach(hp_: float, roi_: float) -> str:
            if (hp_ >= MASTER_HIT_PCT) and (roi_ >= MASTER_ROI_PCT):
                nonlocal master_achieved
                master_achieved += 1
                return "★"
            return " "

        t_hp   = _hit_pct(agg["tansho_hit"], agg["tansho_played"])
        s_hp   = _hit_pct(agg["sanren_hit"], agg["sanren_played"])
        ua_hp  = _hit_pct(
            agg["umaren_a_hit"] + agg["umaren_b_hit"],
            agg["umaren_a_played"] + agg["umaren_b_played"],
        )
        wa_hp  = _hit_pct(
            agg["wide_a_hit"] + agg["wide_b_hit"],
            agg["wide_a_played"] + agg["wide_b_played"],
        )

        t_m  = _ach(t_hp,  t_roi)
        s_m  = _ach(s_hp,  s_roi)
        ua_m = _ach(ua_hp, ua_roi)
        wa_m = _ach(wa_hp, wa_roi)

        logger.info(
            f"  {conf:4s} | {rp:>11,} | "
            f"{t_roi:>8.1f}%{t_m} | {s_roi:>10.1f}%{s_m} | "
            f"{ua_roi:>8.1f}%{ua_m} | {wa_roi:>9.1f}%{wa_m}"
        )

    logger.info("")
    logger.info(
        f"マスター基準達成 (hit% >={MASTER_HIT_PCT}% AND ROI >={MASTER_ROI_PCT}%): "
        f"{master_achieved} / 24 セル ★"
    )
    logger.info("")


# ============================================================
# CLI エントリポイント
# ============================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "真の M' 戦略 集計 (bet_decision.skip=False のみ / 4 馬券並列)\n"
            "バグ修正: skip=True (見送り) race を除外した真の運用 ROI を算出"
        )
    )
    parser.add_argument("--start",  default=DEFAULT_START,
                        help=f"開始日 (YYYY-MM-DD, デフォルト: {DEFAULT_START})")
    parser.add_argument("--end",    default=DEFAULT_END,
                        help=f"終了日 exclusive (YYYY-MM-DD, デフォルト: {DEFAULT_END})")
    parser.add_argument("--debug",  action="store_true",
                        help="デバッグモード (最初の 10 ファイルのみ)")
    args = parser.parse_args()

    os.makedirs(DIAG_DIR, exist_ok=True)
    log_path = os.path.join(DIAG_DIR, "mprime_skip_aware_run.log")
    logger   = _setup_logger(log_path)

    logger.info("=== diag_mprime_skip_aware.py 開始 ===")
    logger.info("  【バグ修正】bet_decision.skip=True race を除外して真の運用 ROI 算出")
    logger.info(f"  期間: {args.start} 〜 {args.end} (exclusive)")
    logger.info(f"  デバッグ: {args.debug}")

    # 集計実行
    conf_agg, monthly_agg, counters = run(args.start, args.end, args.debug, logger)

    # CSV 出力
    summary_path     = os.path.join(DIAG_DIR, "mprime_skip_aware_summary.csv")
    monthly_path     = os.path.join(DIAG_DIR, "mprime_skip_aware_monthly.csv")
    all_periods_path = os.path.join(DIAG_DIR, "mprime_skip_aware_all_periods.csv")

    master_total = write_summary_csv(conf_agg, summary_path, logger)
    write_monthly_csv(monthly_agg, monthly_path, logger)
    write_all_periods_csv(conf_agg, all_periods_path, logger)

    # 最終レポート
    print_report(conf_agg, counters, master_total, logger)

    logger.info("=== 完了 ===")
    logger.info(f"  summary CSV:     {summary_path}")
    logger.info(f"  monthly CSV:     {monthly_path}")
    logger.info(f"  all_periods CSV: {all_periods_path}")
    logger.info(f"  log:             {log_path}")


if __name__ == "__main__":
    main()
