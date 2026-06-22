#!/usr/bin/env python
"""
Walk-Forward バックテスト予想生成
各月ごとにローリング1年間の学習データのみでモデルを訓練し、
その月のレースを予想してDBに保存する。データリークなし。

Usage:
  python scripts/walk_forward_backtest.py
  python scripts/walk_forward_backtest.py --start 2024-06 --end 2026-03
  python scripts/walk_forward_backtest.py --train-months 12 --force
"""
import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from itertools import combinations
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

import numpy as np
import lightgbm as lgb

from src.ml.lgbm_model import (
    FEATURE_COLUMNS,
    CATEGORICAL_FEATURES,
    RollingStatsTracker,
    RollingSireTracker,
    _extract_features,
    _add_race_relative_features,
    _load_ml_races,
    _load_horse_sire_map,
)
from src import database as db

BEST_PARAMS_PATH = "data/models/best_lgbm_params.json"


# ============================================================
# 印・自信度・穴危険ロジック (bulk_backfill と同一)
# ============================================================

def _assign_marks(probs: Dict[str, float]) -> Dict[str, str]:
    sorted_list = sorted(probs.items(), key=lambda x: -x[1])
    vals = [p for _, p in sorted_list]
    total = sum(vals) or 1.0
    n = len(vals)
    norm_gap = (vals[0] - vals[1]) * n / total if len(vals) > 1 else 0.0
    marks = {}
    for i, (hid, _) in enumerate(sorted_list):
        if i == 0:
            marks[hid] = "◉" if norm_gap >= 0.5 else "◎"
        elif i == 1:
            marks[hid] = "○"
        elif i == 2:
            marks[hid] = "▲"
        elif i == 3:
            marks[hid] = "△"
        elif i == 4:
            marks[hid] = "☆"
        else:
            marks[hid] = "-"
    return marks


def _judge_confidence(probs: Dict[str, float]) -> str:
    if not probs:
        return "C"
    vals = sorted(probs.values(), reverse=True)
    total = sum(vals) or 1.0
    n = len(vals)
    norm_top = vals[0] * n / total
    norm_gap = (vals[0] - vals[1]) * n / total if len(vals) > 1 else norm_top
    if norm_top >= 2.0 and norm_gap >= 0.4:
        return "SS"
    if norm_top >= 1.7 and norm_gap >= 0.3:
        return "S"
    if norm_top >= 1.5 and norm_gap >= 0.2:
        return "A"
    if norm_top >= 1.3:
        return "B"
    return "C"


def _get_ana_kiken(rank_u: int, mk: str, h: dict, field_count: int):
    if mk != "-":
        return ("none", "none")
    odds = h.get("odds") or 0
    pop = h.get("popularity") or 99
    if odds >= 10.0 and pop >= 5 and rank_u <= 1:
        return ("穴厚切り" if odds >= 30 else "穴", "none")
    lower_threshold = max(5, field_count * 2 // 3)
    if odds < 10.0 and pop <= 4 and rank_u > lower_threshold:
        if pop <= 2 and odds <= 3.0:
            return ("none", "人気危険")
        return ("none", "危険")
    return ("none", "none")


# ============================================================
# ユーティリティ
# ============================================================

def _months_add(ym: str, months: int) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    y += (m - 1 + months) // 12
    m = (m - 1 + months) % 12 + 1
    return f"{y:04d}-{m:02d}-01"


def _ym(date_str: str) -> str:
    return date_str[:7]


def _load_best_params() -> dict:
    base = {
        "objective": "binary",
        "metric": "auc",
        "boosting_type": "gbdt",
        "num_leaves": 63,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "min_child_samples": 50,
        "lambda_l1": 0.1,
        "lambda_l2": 1.0,
        "verbose": -1,
        "seed": 42,
        "is_unbalance": True,
    }
    if os.path.exists(BEST_PARAMS_PATH):
        with open(BEST_PARAMS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        bp = data.get("best_params", {})
        base.update(bp)
        print(f"  Optuna最適パラメータ読み込み (AUC={data.get('best_auc', '?')})")
    else:
        print(f"  デフォルトパラメータ使用 ({BEST_PARAMS_PATH} なし)")
    return base


def _calc_shobu_score_wf(h: dict, race: dict, tracker) -> float:
    """A-3d Lv1 (2026-05-26 マスター承認): WF backtest 用簡易 shobu_score。

    src/calculator/jockey_trainer.py の calc_shobu_score の主要因子を
    WF backtest 内で再現する。フル engine 経由ではないため、TrainerStats /
    JockeyStats オブジェクトは構築せず、RollingStatsTracker から得られる
    集計値で代替する。

    実装する因子 (フル engine の calc_shobu_score と対応):
      - 騎手強化 (+2.0): jockey 90日 win_rate > 15%
      - 初コンビ (+0.5): is_jockey_change フラグ
      - 格上げ (+1.5): race.grade が hd.last_grade より上位
      - 厩舎好調 (+1.5): trainer 90日 win_rate > 12%
      - 休み明け回収率高 (+1.5): days_since_last_run >= 60 で簡易判定
      - 調教師偏差値: trainer win_rate ベースの 4 段階加減算 (+1.5/+0.8/0/-0.5)

    フル engine 値との一致率は 8-9 割見込み。完全一致は A-3e (Lv2/Lv3) 以降で対応。
    """
    score = 0.0
    if tracker is None:
        return 0.0

    jid = h.get("jockey_id", "") or ""
    tid = h.get("trainer_id", "") or ""
    venue = race.get("venue", "") or ""
    date_str = race.get("date", "") or ""

    # 騎手強化 (90日 win_rate ベース)
    if jid:
        try:
            j_feat = tracker.get_jockey_features(jid, venue, "", "", date_str)
            j_wr_90d = j_feat.get("jockey_win_rate_90d")
            if j_wr_90d is not None and j_wr_90d > 0.15:
                score += 2.0
        except Exception:
            pass

    # 初コンビ
    if h.get("is_jockey_change"):
        score += 0.5

    # 格上げ
    class_order = ["新馬", "未勝利", "1勝", "2勝", "3勝", "OP", "G3", "G2", "G1"]
    grade = race.get("grade", "") or ""
    last_grade = h.get("last_grade", "") or ""
    try:
        if grade in class_order and last_grade in class_order:
            if class_order.index(grade) > class_order.index(last_grade):
                score += 1.5
    except ValueError:
        pass

    # 厩舎好調 + 調教師偏差値
    if tid:
        try:
            t_feat = tracker.get_trainer_features(tid, venue, date_str)
            t_wr_90d = t_feat.get("trainer_win_rate_90d")
            if t_wr_90d is not None and t_wr_90d > 0.12:
                score += 1.5  # 厩舎好調
            t_wr = t_feat.get("trainer_win_rate") or 0
            if t_wr >= 0.18:
                score += 1.5  # 高偏差値厩舎
            elif t_wr >= 0.13:
                score += 0.8
            elif 0 < t_wr < 0.07:
                score -= 0.5
        except Exception:
            pass

    # 休み明け簡易判定
    days_since = h.get("days_since_last_run")
    if days_since is not None and days_since >= 60:
        score += 1.5

    return round(score, 2)


def _calc_shobu_score_wf_lv2(h: dict, race: dict, tracker) -> float:
    """A-3e Step 1 (Lv2): フル engine `calc_shobu_score` 直接呼び。

    tracker の win_rate を擬似偏差値 (平均 50, 標準偏差 10) に変換し、
    最低限の Horse / TrainerStats / JockeyStats を構築して engine の
    calc_shobu_score をそのまま呼ぶ。

    Lv1 (簡易再現) との差:
      - 厩舎好調判定: 90d - 全期間 win_rate 差 (engine 互換) ← Lv1 は 90d 単体閾値
      - 調教師偏差値: Z 変換 (engine 互換) ← Lv1 は win_rate 4 段階
      - 休み明け精密: calc_break_adjustment 関数呼び (engine 互換) ← Lv1 は単一閾値

    Lv1 → Lv2 で残る乖離 (Lv3 で対応):
      - KishuPattern.A: 偏差値ベース判定が必要だが、Lv2 では win_rate > 15% で代用
      - recovery_break: tracker 未集計、Lv2 では 0.0 固定 → calc_break_adjustment が
        recovery_break=0 で動く挙動 (中央テーブル参照) に依存
    """
    from types import SimpleNamespace
    from src.models import TrainerStats, JockeyStats, KishuPattern
    from src.calculator.jockey_trainer import calc_shobu_score

    if tracker is None:
        return 0.0

    jid = h.get("jockey_id", "") or ""
    tid = h.get("trainer_id", "") or ""
    venue = race.get("venue", "") or ""
    date_str = race.get("date", "") or ""
    grade = race.get("grade", "") or ""
    last_grade = h.get("last_grade", "") or ""
    days_since = h.get("days_since_last_run")

    # Horse 最小オブジェクト (calc_shobu_score は is_jockey_change のみ参照)
    horse_obj = SimpleNamespace(is_jockey_change=bool(h.get("is_jockey_change")))

    # TrainerStats 構築 (win_rate → 偏差値 Z 変換)
    t_dev = 50.0
    t_short_momentum = ""
    if tid:
        try:
            t_feat = tracker.get_trainer_features(tid, venue, date_str)
            t_wr = t_feat.get("trainer_win_rate") or 0
            t_wr_90d = t_feat.get("trainer_win_rate_90d") or 0
            # win_rate 0.10 を中央 50 とする簡易 Z (1σ=0.05)
            t_dev = 50.0 + (t_wr - 0.10) * 200.0
            t_dev = max(20.0, min(80.0, t_dev))  # 極端値クリップ
            # short_momentum: 90d - 全期間 >= +0.05 で好調 / <= -0.05 で不調
            diff = t_wr_90d - t_wr
            if diff >= 0.05:
                t_short_momentum = "好調"
            elif diff <= -0.05:
                t_short_momentum = "不調"
        except Exception:
            pass

    trainer_obj = TrainerStats(
        trainer_id=tid, trainer_name="", stable_name="", location="",
        short_momentum=t_short_momentum,
        recovery_break=0.0,  # Lv3 で tracker から取得
        deviation=t_dev,
    )

    # JockeyStats は calc_shobu_score では未使用だがシグネチャ上必要
    jockey_obj = JockeyStats(jockey_id=jid, jockey_name="")

    # KishuPattern (Lv2 では Lv1 と同様 jockey 90d win_rate > 15% で代用)
    j_pattern = None
    if jid:
        try:
            j_feat = tracker.get_jockey_features(jid, venue, "", "", date_str)
            j_wr_90d = j_feat.get("jockey_win_rate_90d")
            if j_wr_90d is not None and j_wr_90d > 0.15:
                j_pattern = KishuPattern.A
        except Exception:
            pass

    is_long_break = bool(days_since is not None and days_since >= 60)

    try:
        score = calc_shobu_score(
            horse=horse_obj,
            trainer=trainer_obj,
            jockey=jockey_obj,
            jockey_change_pattern=j_pattern,
            is_long_break=is_long_break,
            grade=grade,
            last_grade=last_grade,
            days_since_last_run=days_since,
        )
        return round(float(score), 2)
    except Exception:
        # Lv2 で engine 呼び出しに失敗した場合は Lv1 にフォールバック
        return _calc_shobu_score_wf(h, race, tracker)


def _jockey_winrate_to_dev(wr) -> float:
    """A-3e Lv3 helper: jockey win_rate → 偏差値 Z 変換 (中央 0.10, 1σ=0.05)

    engine の JockeyStats.get_deviation 互換の擬似偏差値。
    """
    if wr is None or wr <= 0:
        return 50.0
    dev = 50.0 + (float(wr) - 0.10) * 200.0
    return max(20.0, min(80.0, dev))


def _calc_shobu_score_wf_lv3(h: dict, race: dict, tracker) -> float:
    """A-3e Lv3 (本セッション追加実装): engine 完全互換版。

    Lv2 からの改善:
      - KishuPattern.A 完全再現: engine 仕様 `new_dev >= 60 or new_dev - prev_dev >= 8`
        - new_dev: 現騎手の Z 変換偏差値 (winrate ベース)
        - prev_dev: 前走騎手の Z 変換偏差値 (`tracker._horse_history` から取得)
      - recovery_break 推定: `tracker.trainer_rest_wr` (60 日以上休養明け複勝率) を
        回収率にスケール変換 (中央 0.30 → 90)
      - short_momentum 判定に class_trend も加味

    残課題 (本セッション内では妥協):
      - recovery_break のスケール変換は経験則 (rest_wr × 300)。真の値は実回収率データ必要。
      - JockeyStats.get_deviation の本式 (上位/下位 × 短期/長期 4 象限) は未実装
        (calc_shobu_score では JockeyStats 自体使われないため影響なし)。
    """
    from types import SimpleNamespace
    from src.models import TrainerStats, JockeyStats, KishuPattern
    from src.calculator.jockey_trainer import calc_shobu_score

    if tracker is None:
        return 0.0

    jid = h.get("jockey_id", "") or ""
    tid = h.get("trainer_id", "") or ""
    hid = h.get("horse_id", "") or ""
    venue = race.get("venue", "") or ""
    date_str = race.get("date", "") or ""
    grade = race.get("grade", "") or ""
    last_grade = h.get("last_grade", "") or ""
    days_since = h.get("days_since_last_run")

    horse_obj = SimpleNamespace(is_jockey_change=bool(h.get("is_jockey_change")))

    # ===== TrainerStats 構築 (Lv3 拡張) =====
    t_dev = 50.0
    t_short_momentum = ""
    t_recovery_break = 0.0
    if tid:
        try:
            t_feat = tracker.get_trainer_features(tid, venue, date_str)
            t_wr = t_feat.get("trainer_win_rate") or 0
            t_wr_90d = t_feat.get("trainer_win_rate_90d") or 0
            t_dev = 50.0 + (t_wr - 0.10) * 200.0
            t_dev = max(20.0, min(80.0, t_dev))

            diff = t_wr_90d - t_wr
            if diff >= 0.05:
                t_short_momentum = "好調"
            elif diff <= -0.05:
                t_short_momentum = "不調"

            # Lv3 追加: class_trend と rest_wr を加味
            try:
                phase10b = tracker.get_trainer_phase10b_features(tid)
                class_trend = phase10b.get("trainer_class_trend")
                rest_wr = phase10b.get("trainer_rest_wr")
                # class_trend > 0.5 (明確な上昇) + 不調でなければ "好調" に格上げ
                if class_trend is not None and class_trend > 0.5 and t_short_momentum != "不調":
                    t_short_momentum = "好調"
                # recovery_break: rest_wr × 300 で回収率スケール (経験則)
                if rest_wr is not None and rest_wr > 0:
                    t_recovery_break = max(0.0, min(200.0, rest_wr * 300.0))
            except Exception:
                pass
        except Exception:
            pass

    trainer_obj = TrainerStats(
        trainer_id=tid, trainer_name="", stable_name="", location="",
        short_momentum=t_short_momentum,
        recovery_break=t_recovery_break,
        deviation=t_dev,
    )
    jockey_obj = JockeyStats(jockey_id=jid, jockey_name="")

    # ===== KishuPattern.A 完全再現 (Lv3 改善) =====
    j_pattern = None
    if jid:
        try:
            j_feat = tracker.get_jockey_features(jid, venue, "", "", date_str)
            new_wr = j_feat.get("jockey_win_rate")
            new_dev = _jockey_winrate_to_dev(new_wr)

            # 前走騎手の偏差値 (horse_history から)
            prev_dev = None
            if hid and h.get("is_jockey_change"):
                hist = getattr(tracker, "_horse_history", {}).get(hid, [])
                past = [r for r in hist if r[0] < date_str]
                if past:
                    # 最新の前走 record
                    prev_jid = past[-1][3]  # (date, finish_pos, field_count, jockey_id)
                    if prev_jid and prev_jid != jid:
                        try:
                            prev_j_feat = tracker.get_jockey_features(
                                prev_jid, "", "", "", date_str
                            )
                            prev_wr = prev_j_feat.get("jockey_win_rate")
                            prev_dev = _jockey_winrate_to_dev(prev_wr)
                        except Exception:
                            pass

            # engine 仕様完全再現
            if new_dev >= 60:
                j_pattern = KishuPattern.A
            elif prev_dev is not None and new_dev - prev_dev >= 8:
                j_pattern = KishuPattern.A
        except Exception:
            pass

    is_long_break = bool(days_since is not None and days_since >= 60)

    try:
        score = calc_shobu_score(
            horse=horse_obj,
            trainer=trainer_obj,
            jockey=jockey_obj,
            jockey_change_pattern=j_pattern,
            is_long_break=is_long_break,
            grade=grade,
            last_grade=last_grade,
            days_since_last_run=days_since,
        )
        return round(float(score), 2)
    except Exception:
        return _calc_shobu_score_wf_lv2(h, race, tracker)


# A-3e 切替フラグ (CLI から設定)
SHOBU_SCORE_LV = 1


def _calc_shobu_score_dispatch(h: dict, race: dict, tracker) -> float:
    """SHOBU_SCORE_LV に応じて Lv1 / Lv2 / Lv3 を呼び分け"""
    if SHOBU_SCORE_LV >= 3:
        return _calc_shobu_score_wf_lv3(h, race, tracker)
    if SHOBU_SCORE_LV >= 2:
        return _calc_shobu_score_wf_lv2(h, race, tracker)
    return _calc_shobu_score_wf(h, race, tracker)


def _build_horse_entry(h: dict, hid: str, prob: float, mk: str,
                       rank_u: int, field_count: int,
                       race: dict | None = None, tracker=None) -> dict:
    """1頭分の予想データ構造

    A-3d Lv1 (2026-05-26): race + tracker を渡せば shobu_score を計算。
    省略時は 0.0 (後方互換)。
    """
    ana_type, kiken_type = _get_ana_kiken(rank_u, mk, h, field_count)
    # A-3e (2026-05-26): SHOBU_SCORE_LV で Lv1 (簡易) / Lv2 (engine 直呼び) を dispatch
    shobu_score = _calc_shobu_score_dispatch(h, race, tracker) if (race is not None and tracker is not None) else 0.0
    return {
        "horse_no": h.get("horse_no"),
        "horse_name": h.get("horse_name", ""),
        "horse_id": hid,
        "sex": h.get("sex", ""),
        "age": h.get("age"),
        "gate_no": h.get("gate_no"),
        "jockey": h.get("jockey", ""),
        "jockey_id": h.get("jockey_id", ""),
        "trainer": h.get("trainer", ""),
        "horse_weight": h.get("horse_weight"),
        "weight_change": h.get("weight_change"),
        "odds": h.get("odds"),
        "popularity": h.get("popularity"),
        "mark": mk,
        "composite": round(prob * 100, 2),
        "ml_place_prob": round(prob, 4),
        "win_prob": round(prob * 0.40, 4),
        "place2_prob": round(prob * 0.70, 4),
        "place3_prob": round(prob, 4),
        "ana_type": ana_type,
        "kiken_type": kiken_type,
        "ability_total": 0.0, "ability_max": 0.0, "ability_wa": 0.0,
        "ability_alpha": 0.0, "ability_trend": "stable",
        "ability_reliability": "B",
        "ability_class_adj": 0.0, "ability_bloodline_adj": 0.0,
        "ability_chakusa_pattern": "",
        "pace_total": 0.0, "pace_base": 0.0, "pace_last3f_eval": 0.0,
        "pace_position_balance": 0.0, "pace_gate_bias": 0.0,
        "pace_course_style_bias": 0.0, "pace_jockey": 0.0,
        "pace_estimated_pos4c": None, "pace_estimated_last3f": None,
        "running_style": "",
        "course_total": 0.0, "course_record": 0.0,
        "course_venue_apt": 0.0, "course_venue_level": "",
        "course_jockey": 0.0,
        "ml_win_prob": round(prob * 0.40, 4),
        "ml_top2_prob": round(prob * 0.70, 4),
        "jockey_change_score": 0.0,
        # A-3d Lv1 (2026-05-26 マスター承認): _calc_shobu_score_wf で簡易再現。
        # フル engine の calc_shobu_score の主要因子 (騎手強化/初コンビ/格上げ/厩舎好調/
        # 休み明け/調教師偏差値) を RollingStatsTracker 経由で計算。8-9 割の一致見込み。
        "shobu_score": shobu_score,
        "odds_consistency_adj": 0.0,
        "ana_score": 0.0, "kiken_score": 0.0,
        "predicted_tansho_odds": None, "odds_divergence": None,
        "divergence_signal": "", "training_intensity": None,
    }


def _make_tickets(pred_horses: list) -> list:
    """簡易チケット: ◎軸馬連4点 + 三連複6点 = 計10点×100円"""
    honmei_no = next(
        (h["horse_no"] for h in pred_horses if h["mark"] in ("◉", "◎")), None
    )
    others = [h["horse_no"] for h in pred_horses if h["mark"] in ("○", "▲", "△", "☆")]
    tickets = []
    if honmei_no and others:
        for o in others[:4]:
            tickets.append({
                "type": "馬連", "combo": [honmei_no, o],
                "ev": 0, "stake": 100, "signal": "簡易",
            })
        for b, c in combinations(others[:4], 2):
            tickets.append({
                "type": "三連複", "combo": [honmei_no, b, c],
                "ev": 0, "stake": 100, "signal": "簡易",
            })
    return tickets


def _make_tickets_by_mode(pred_horses: list) -> dict:
    """簡易 3モード別チケット生成（walk_forward バックテスト用）

    engine を通さないため EV・Kelly 配分・トリガミ回避は行わず、
    点数レンジだけモード別に変えて的中率/回収率の相対比較を可能にする。

    - accuracy: ◎軸 馬連5点 + 三連複10点 = 15点
    - balanced: ◎軸 馬連4点 + 三連複6点  = 10点
    - recovery: ◎軸 馬連2点 + 三連複3点  =  5点
    """
    honmei_no = next(
        (h["horse_no"] for h in pred_horses if h["mark"] in ("◉", "◎")), None
    )
    others = [h["horse_no"] for h in pred_horses if h["mark"] in ("○", "▲", "△", "☆")]

    modes = {
        "accuracy": {"umaren": 5, "sanfuku_base": 5},
        "balanced": {"umaren": 4, "sanfuku_base": 4},
        "recovery": {"umaren": 2, "sanfuku_base": 3},
    }
    result = {"accuracy": [], "balanced": [], "recovery": []}
    if not (honmei_no and others):
        return result
    for mode, cfg in modes.items():
        bet_list = []
        for o in others[: cfg["umaren"]]:
            bet_list.append({
                "type": "馬連", "combo": [honmei_no, o],
                "ev": 0, "stake": 100, "signal": "簡易", "mode": mode,
            })
        for b, c in combinations(others[: cfg["sanfuku_base"]], 2):
            bet_list.append({
                "type": "三連複", "combo": [honmei_no, b, c],
                "ev": 0, "stake": 100, "signal": "簡易", "mode": mode,
            })
        result[mode] = bet_list
    return result


# ============================================================
# 1ヶ月分の処理
# ============================================================

def _process_month(
    target_ym: str,
    races: list,
    sire_map: dict,
    params: dict,
    tracker: RollingStatsTracker,
    sire_tracker: RollingSireTracker,
    force: bool,
) -> dict:
    """
    target_ym: 'YYYY-MM' (予想対象月)
    ローリング1年ウィンドウで学習 → 予想 → DB保存

    Returns: 統計dict
    """
    train_start = _months_add(target_ym, -12)
    train_end = f"{target_ym}-01"
    valid_start = train_end
    valid_end = _months_add(target_ym, 1)

    # 学習/検証データ収集
    train_feats, train_labels = [], []
    valid_races_data = []  # (race_dict, feat_list, label_list, horse_dicts)

    for race in races:
        d = race.get("date", "")
        if not d or d >= valid_end:
            break
        if d < train_start:
            continue

        is_valid = d >= valid_start

        r_feats, r_labels = [], []
        horse_dicts = []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            hd = dict(h, sire_id=sid, bms_id=bid)
            feat = _extract_features(hd, race, tracker, sire_tracker)
            r_feats.append(feat)
            r_labels.append(1 if fp <= 3 else 0)
            horse_dicts.append(hd)

        if r_feats:
            _add_race_relative_features(r_feats)
            if is_valid:
                valid_races_data.append((race, r_feats, r_labels, horse_dicts))
            else:
                train_feats.extend(r_feats)
                train_labels.extend(r_labels)

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    n_train = len(train_labels)
    n_valid_races = len(valid_races_data)

    if n_train < 1000:
        return {"month": target_ym, "status": "skip_train", "n_train": n_train}
    if n_valid_races == 0:
        return {"month": target_ym, "status": "no_valid", "n_train": n_train}

    # numpy変換 (2026-05-25 修正: FEATURE_COLUMNS と f の key 不一致をガード)
    def _to_np(rows):
        mat = []
        for f in rows:
            row = []
            for c in FEATURE_COLUMNS:
                v = f.get(c) if isinstance(f, dict) else (f[c] if c in f else None)
                row.append(float(v) if v is not None else float("nan"))
            mat.append(row)
        return np.array(mat, dtype=np.float32)

    X_train = _to_np(train_feats)
    y_train = np.array(train_labels, dtype=np.int32)

    # LightGBM学習
    dtrain = lgb.Dataset(
        X_train, label=y_train,
        feature_name=FEATURE_COLUMNS,
        categorical_feature=CATEGORICAL_FEATURES,
        free_raw_data=False,
    )
    p = dict(params)
    p["seed"] = 42 + hash(target_ym) % 1000
    model = lgb.train(p, dtrain, num_boost_round=500)

    # 検証レースを予測 → 予想データ生成 → DB保存
    dates_data = defaultdict(lambda: {"races": [], "results": {}})
    preds_saved = 0
    results_saved = 0

    # 統計
    top1_hit = top1_total = 0
    # P0 (2026-06-23): 確率較正指標 — ML複勝確率 vs 実複勝(fp<=3)。印ロジック不変・測定のみ
    import math
    brier_sum = logloss_sum = 0.0
    calib_n = 0

    for (race, r_feats, r_labels, horse_dicts) in valid_races_data:
        race_id = race.get("race_id", "")
        date_str = race.get("date", "")
        if not race_id or not date_str:
            continue

        X = _to_np(r_feats)
        raw_preds = model.predict(X)

        # horse_id → prob マッピング
        probs = {}
        for i, hd in enumerate(horse_dicts):
            hid = hd.get("horse_id", "")
            if hid:
                probs[hid] = float(raw_preds[i])

        if not probs:
            continue

        # P0: 較正指標 accumulate（ML複勝確率の Brier/logloss・複勝 label=fp<=3）
        for _hd in horse_dicts:
            _hid = _hd.get("horse_id", "")
            _fp = _hd.get("finish_pos")
            if not _hid or _hid not in probs or _fp is None:
                continue
            _p = min(1.0 - 1e-9, max(1e-9, probs[_hid]))
            _y = 1.0 if _fp <= 3 else 0.0
            brier_sum += (_p - _y) ** 2
            logloss_sum += -(_y * math.log(_p) + (1.0 - _y) * math.log(1.0 - _p))
            calib_n += 1

        marks = _assign_marks(probs)
        confidence = _judge_confidence(probs)

        sorted_ids = [hid for hid, _ in sorted(probs.items(), key=lambda x: -x[1])]
        rank_map = {hid: i for i, hid in enumerate(sorted_ids)}
        field_count = len(horse_dicts)

        unmarked_sorted = [hid for hid in sorted_ids if marks.get(hid, "-") == "-"]
        rank_u_map = {hid: i for i, hid in enumerate(unmarked_sorted)}

        # 予想馬データ
        # A-3d Lv1 (2026-05-26): race + tracker を渡し shobu_score を簡易計算
        pred_horses = []
        for hd in horse_dicts:
            hid = hd.get("horse_id", "")
            prob = probs.get(hid, 0.0)
            mk = marks.get(hid, "-")
            ru = rank_u_map.get(hid, field_count)
            pred_horses.append(_build_horse_entry(hd, hid, prob, mk, ru, field_count, race=race, tracker=tracker))

        tickets = _make_tickets(pred_horses)
        tickets_by_mode = _make_tickets_by_mode(pred_horses)

        pred_race = {
            "race_id": race_id,
            "venue": race.get("venue", ""),
            "race_no": race.get("race_no", 0),
            "race_name": race.get("race_name", ""),
            "surface": race.get("surface", ""),
            "distance": race.get("distance", 0),
            "direction": race.get("direction", ""),
            "is_jra": race.get("is_jra", True),
            "field_count": len(horse_dicts),
            "grade": race.get("grade", ""),
            "confidence": confidence,
            "pace_predicted": "",
            "horses": pred_horses,
            "tickets": tickets,
            "formation_tickets": [],
            "value_bets": [],
            "tickets_by_mode": tickets_by_mode,
        }
        dates_data[date_str]["races"].append(pred_race)

        # 着順結果 (MLデータから)
        order = []
        for hd in horse_dicts:
            fp = hd.get("finish_pos")
            if fp is not None:
                order.append({
                    "horse_no": hd.get("horse_no"),
                    "finish": fp,
                    "odds": hd.get("odds"),
                })
        if order:
            dates_data[date_str]["results"][race_id] = {
                "order": order, "payouts": {},
            }

        # Top1統計
        if sorted_ids:
            top1_hid = sorted_ids[0]
            top1_idx = next((i for i, hd in enumerate(horse_dicts)
                            if hd.get("horse_id") == top1_hid), None)
            if top1_idx is not None:
                top1_total += 1
                fp = horse_dicts[top1_idx].get("finish_pos")
                if fp and fp <= 3:
                    top1_hit += 1

    # DB保存 (日付ごと)
    # A-3b 修正 (2026-05-25): pred.json にも export して regen_strategy/verify/r1 が
    # WF 結果を読めるようにする (旧: DB のみ保存 → pred.json に WF 効果反映されず)
    import os as _os
    pred_json_dir = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "data", "predictions")
    _os.makedirs(pred_json_dir, exist_ok=True)

    for date_str in sorted(dates_data.keys()):
        dd = dates_data[date_str]
        # A-3b 修正 (2026-05-25): date_str がハイフン形式 (YYYY-MM-DD) で来る
        # 既存 pred.json/DB はハイフンなし (YYYYMMDD) 形式 → 変換必要
        date_compact = date_str.replace("-", "")
        if not force:
            existing = db.load_prediction(date_compact)
            if existing and existing.get("races"):
                continue
        payload = {"date": date_compact, "version": 2, "races": dd["races"]}
        try:
            db.save_prediction(date_compact, payload)
            preds_saved += len(dd["races"])
        except Exception as e:
            print(f"    [WARN] pred save {date_compact}: {e}")

        # A-3b 修正: pred.json export (regen/verify/r1 が WF 予想を読めるように)
        # ファイル名は既存形式 (YYYYMMDD_pred.json) に合わせる
        try:
            pred_json_path = _os.path.join(pred_json_dir, f"{date_compact}_pred.json")
            with open(pred_json_path, "w", encoding="utf-8") as _f:
                json.dump(payload, _f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"    [WARN] pred.json export {date_compact}: {e}")

        if dd["results"]:
            try:
                db.save_results(date_compact, dd["results"])
                results_saved += len(dd["results"])
            except Exception as e:
                print(f"    [WARN] result save {date_compact}: {e}")

    top1_rate = top1_hit / top1_total if top1_total else 0

    return {
        "month": target_ym,
        "status": "ok",
        "n_train": n_train,
        "n_valid_races": n_valid_races,
        "preds_saved": preds_saved,
        "results_saved": results_saved,
        "top1_rate": top1_rate,
        "brier": brier_sum / calib_n if calib_n else None,
        "logloss": logloss_sum / calib_n if calib_n else None,
        "calib_n": calib_n,
    }


# ============================================================
# メイン (逐次tracker更新方式)
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Walk-Forward バックテスト予想生成")
    parser.add_argument("--start", default="2024-01",
                        help="開始年月 YYYY-MM (default: 2024-01)")
    parser.add_argument("--end", default="2026-03",
                        help="終了年月 YYYY-MM (default: 2026-03)")
    parser.add_argument("--train-months", type=int, default=12,
                        help="学習ウィンドウ(月数, default: 12)")
    parser.add_argument("--force", action="store_true",
                        help="既存予想データも上書き")
    parser.add_argument("--shobu-lv", type=int, choices=[1, 2, 3], default=1,
                        help="A-3e shobu_score 計算レベル: 1=簡易(A-3d Lv1) / 2=engine 直呼び(A-3e Lv2) / 3=engine 完全互換(Lv3: KishuPattern完全再現+recovery_break推定)")
    args = parser.parse_args()

    # A-3e: shobu_score 計算レベルを反映 (グローバル切替)
    global SHOBU_SCORE_LV
    SHOBU_SCORE_LV = args.shobu_lv
    if SHOBU_SCORE_LV >= 2:
        print(f"  shobu_score: Lv{SHOBU_SCORE_LV} (engine 直呼び / A-3e Step 1)")
    else:
        print(f"  shobu_score: Lv{SHOBU_SCORE_LV} (簡易再現 / A-3d)")

    t_total = time.time()

    print(f"\n{'=' * 62}")
    print(f"  Walk-Forward バックテスト予想生成")
    print(f"  期間: {args.start} ~ {args.end}")
    print(f"  学習ウィンドウ: {args.train_months}ヶ月 (ローリング)")
    print(f"  force: {args.force}")
    print(f"{'=' * 62}\n")

    # DB初期化
    db.init_schema()

    # データ読み込み
    print("[1/3] ML レースデータ読み込み中...", flush=True)
    t0 = time.time()
    races = _load_ml_races()
    if not races:
        print("[ERROR] MLデータなし (data/ml/*.json)")
        sys.exit(1)
    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    print(f"  {len(races):,}レース / {all_dates[0]}~{all_dates[-1]} ({time.time()-t0:.1f}秒)")

    print("[2/3] 血統マップ読み込み中...", flush=True)
    t0 = time.time()
    sire_map = _load_horse_sire_map()
    print(f"  {len(sire_map):,}頭 ({time.time()-t0:.1f}秒)")

    print("[3/3] LightGBMパラメータ読み込み...", flush=True)
    params = _load_best_params()

    # 月リスト生成
    months = []
    ym = args.start
    while ym <= args.end:
        months.append(ym)
        y, m = int(ym[:4]), int(ym[5:7])
        m += 1
        if m > 12:
            m = 1
            y += 1
        ym = f"{y:04d}-{m:02d}"

    print(f"\n{len(months)}ヶ月分を処理します...\n")

    # 各月を処理
    # 重要: tracker は時系列順に全レースを更新し続ける必要がある
    # そのため、月ごとに独立したtrainerを使わず、
    # 学習データ期間のレースからtrackerを構築する
    summary = []
    total_preds = 0
    total_results = 0

    for i, target_ym in enumerate(months, 1):
        t_month = time.time()

        # 各月ごとに新しいtracker を構築
        # (ローリングウィンドウの学習期間開始前のデータからtrackerをウォームアップ)
        train_start = _months_add(target_ym, -args.train_months)

        tracker = RollingStatsTracker()
        sire_tracker = RollingSireTracker()

        # train_start より前のレースでtrackerをウォームアップ
        warmup_start = _months_add(target_ym, -(args.train_months + 6))  # 6ヶ月余分
        for race in races:
            d = race.get("date", "")
            if not d:
                continue
            if d >= train_start:
                break
            if d >= warmup_start:
                tracker.update_race(race)
                sire_tracker.update_race(race, sire_map)

        result = _process_month(
            target_ym, races, sire_map, params,
            tracker, sire_tracker, args.force,
        )
        elapsed = time.time() - t_month

        status = result.get("status", "?")
        if status == "ok":
            p = result.get("preds_saved", 0)
            r = result.get("results_saved", 0)
            top1 = result.get("top1_rate", 0) * 100
            total_preds += p
            total_results += r
            eta = elapsed / i * (len(months) - i)
            print(f"  [{i:2d}/{len(months)}] {target_ym}  "
                  f"学習:{result['n_train']:>6,}  予想:{p:>3}R  "
                  f"Top1→3着内:{top1:>5.1f}%  "
                  f"({elapsed:.0f}秒 残{eta/60:.1f}分)")
        else:
            print(f"  [{i:2d}/{len(months)}] {target_ym}  → {status} "
                  f"(学習:{result.get('n_train', 0):,})")

        summary.append(result)

    # サマリー
    elapsed_total = time.time() - t_total
    ok_months = [s for s in summary if s.get("status") == "ok"]
    avg_top1 = (
        sum(s.get("top1_rate", 0) for s in ok_months) / len(ok_months) * 100
        if ok_months else 0
    )
    # P0 (2026-06-23): 較正指標の加重平均(calib_n 重み)
    _cn = sum(s.get("calib_n", 0) for s in ok_months)
    avg_brier = (
        sum((s.get("brier") or 0) * s.get("calib_n", 0) for s in ok_months) / _cn
        if _cn else 0
    )
    avg_logloss = (
        sum((s.get("logloss") or 0) * s.get("calib_n", 0) for s in ok_months) / _cn
        if _cn else 0
    )

    print(f"\n{'=' * 62}")
    print(f"  Walk-Forward バックテスト完了!")
    print(f"  処理月数:   {len(ok_months)}/{len(months)}")
    print(f"  予想レース: {total_preds:,}")
    print(f"  結果レース: {total_results:,}")
    print(f"  平均Top1率: {avg_top1:.1f}%")
    print(f"  較正Brier:  {avg_brier:.4f} (低=良・複勝確率の二乗誤差)")
    print(f"  較正LogLoss:{avg_logloss:.4f} (低=良)")
    print(f"  合計時間:   {elapsed_total/60:.1f}分")
    print(f"{'=' * 62}\n")
    print("ダッシュボード「結果分析」タブで確認してください。")


if __name__ == "__main__":
    main()
