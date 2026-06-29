#!/usr/bin/env python
"""
Walk-Forward バックテスト予想生成
各月ごとにローリング1年間の学習データのみでモデルを訓練し、
その月のレースを予想してDBに保存する。データリークなし。

Usage:
  python scripts/walk_forward_backtest.py
  python scripts/walk_forward_backtest.py --start 2024-06 --end 2026-03
  python scripts/walk_forward_backtest.py --train-months 12 --force
  python scripts/walk_forward_backtest.py --start 2026-01 --end 2026-01 --composite-probe
  python scripts/walk_forward_backtest.py --start 2026-01 --end 2026-01 --composite-marks --composite-probe-max-races 35 --force
"""
import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import date, timedelta, datetime as _dt
from itertools import combinations
from pathlib import Path
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

# T-7 Phase1: WF版較正テーブル生成用 — bin定義を本番版から流用
from scripts.build_calibration_composite import composite_to_bin, BIN_LABELS

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

# P0-b Step3-1 (2026-06-24): 本番engine composite 並行測定フラグ (CLI から設定)
# OFF 時: 従来と完全同一動作 / ON 時: 印は変更せず composite を parallel 測定
COMPOSITE_PROBE = False

# P0-b Step3-1: 1ヶ月の測定レース上限 (0=無制限)。smoke run 時はここを調整
_COMPOSITE_PROBE_MAX_RACES = 0  # 0=無制限。smoke run 時は一時的に 3〜10 に変更

# P0-α (2026-06-24): engine印/composite を採用に昇格するフラグ (CLI から設定)
# ADOPT_COMPOSITE_MARKS=False → 従来と完全同一動作 (COMPOSITE_PROBE=False でも同じ)
# ADOPT_COMPOSITE_MARKS=True  → probe成功レースは engine印/composite を採用
#                                probe失敗レースは prob印に fallback (従来動作)
# 注意: --composite-marks は --composite-probe を含意 (main() で強制 ON)
ADOPT_COMPOSITE_MARKS = False

# --no-save フラグ (CLI から設定)
# True 時: DB への save_prediction / save_results および pred.json export をスキップ
# calib_by_bin / brier / logloss / probe 統計集計は --no-save でも従来通り動作する
# False (デフォルト): 従来通り保存する (既存動作非破壊)
_NO_SAVE = False


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
# P0-b Step3-1: composite probe 用月次 engine ビルド関数
# P0-γ OOM対策: prefetchを全月一括保持せず小バッチ単位で処理・解放する設計に変更
# ============================================================

# OOM対策バッチサイズ: 一度に prefetch→probe→del する最大レース数
_COMPOSITE_PROBE_BATCH_SIZE = 25


def _build_composite_probe_db_state(
    target_ym: str,
    scraper_state: dict,
) -> dict | None:
    """P0-γ OOM対策: Phase A のみ (DB構築) — prefetched を含まない軽量 state を返す。

    - 月1回呼ぶ。prefetched は含まない → メモリ枯渇の主因を排除
    - 返値: {
        'course_db_base', 'l3f_db', 'course_style_db', 'gate_bias_db',
        'position_sec_db', 'trainer_baseline_db', 'all_courses', 'target_ym_first_day',
        'jockey_db', 'trainer_db',   # ← 月内全レースprefetch後に後から補完
      }
      または None (例外時)
    """
    try:
        from src.scraper.race_results import (
            StandardTimeDBBuilder, Last3FDBBuilder,
            build_course_style_stats_db, build_gate_bias_db,
            build_position_sec_per_rank_db,
            load_trainer_baseline_db,
        )
        from src.scraper.course_db_collector import load_preload_course_db
        from src.database import get_course_db as _get_sqlite_course_db
        from src.scraper.course_db_collector import _dict_to_past_run
        from src.engine import reset_engine_caches
        from config.settings import COURSE_DB_PRELOAD_PATH, TRAINER_BASELINE_DB_PATH

        reset_engine_caches()

        # window_end = 月初日 - 1日 (leak-safe)
        _first_day = f"{target_ym}-01"
        window_end = (_dt.strptime(_first_day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        window_start = (_dt.strptime(_first_day, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")

        print(f"  [composite-probe] course_db window: {window_start} 〜 {window_end}", flush=True)

        # ── Phase A: 基準タイムDB + SQLite 過去走 merge (window 限定) ──
        std_db = StandardTimeDBBuilder()
        course_db_base = std_db.get_course_db()

        _sqlite_db = _get_sqlite_course_db()
        for _cid, _recs in _sqlite_db.items():
            for _r in _recs:
                _rd = _r.get("race_date", "")
                if _rd and window_start <= _rd <= window_end:
                    course_db_base.setdefault(_cid, []).append(_dict_to_past_run(_r))

        preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH, target_date=_first_day)
        for cid, runs in preload.items():
            course_db_base.setdefault(cid, []).extend(runs)

        # 派生DB (course_style / gate_bias / position_sec / l3f)
        course_style_db = build_course_style_stats_db(course_db_base, target_date=_first_day)
        gate_bias_db    = build_gate_bias_db(course_db_base, target_date=_first_day)
        position_sec_db = build_position_sec_per_rank_db(course_db_base, target_date=_first_day)
        l3f_db_base     = Last3FDBBuilder().build(course_db_base)
        trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)
        print(f"  [composite-probe] DB 構築完了 ({target_ym})", flush=True)

        all_courses = scraper_state["all_courses"]

        return {
            "course_db_base": course_db_base,   # read-only ベース
            "l3f_db": l3f_db_base,
            "course_style_db": course_style_db,
            "gate_bias_db": gate_bias_db,
            "position_sec_db": position_sec_db,
            "trainer_baseline_db": trainer_baseline_db,
            "all_courses": all_courses,
            "target_ym_first_day": _first_day,
            "jockey_db": {},    # バッチ prefetch 後に補完
            "trainer_db": {},   # バッチ prefetch 後に補完
        }
    except Exception as e:
        print(f"  [composite-probe][WARN] monthly DB build 失敗 ({target_ym}): {e}", flush=True)
        return None


def _prefetch_batch_and_build_personnel(
    race_ids_batch: list,
    db_state: dict,
    scraper_state: dict,
) -> dict:
    """P0-γ OOM対策: race_ids_batch 分のみ prefetch → jockey_db/trainer_db 補完 → prefetched 返す。

    - 呼び出し元が del + gc.collect() して都度解放する責任を持つ
    - 返値: prefetched dict (race_id -> (race_info, horses) または (None, []))
    """
    import gc as _gc
    try:
        from src.scraper.race_results import (
            build_course_db_from_past_runs,
            build_trainer_baseline_db,
            merge_trainer_baseline,
        )
        from src.scraper.personnel import PersonnelDBManager
        from src.scraper.improvement_dbs import build_bloodline_db
        from config.settings import BLOODLINE_DB_PATH

        scraper = scraper_state["scraper"]
        _first_day = db_state["target_ym_first_day"]

        prefetched: dict = {}
        fetch_ok = fetch_skip = 0
        for race_id in race_ids_batch:
            _race_date_compact = race_id[:8] if len(race_id) >= 8 else ""
            _race_date = (
                f"{_race_date_compact[:4]}-{_race_date_compact[4:6]}-{_race_date_compact[6:8]}"
                if len(_race_date_compact) == 8 else _first_day
            )
            try:
                ri, hs = scraper.fetch_race(
                    race_id,
                    fetch_history=True,
                    fetch_odds=True,
                    fetch_training=True,
                    target_date=_race_date,
                    prefer_cache=True,
                )
                prefetched[race_id] = (ri, hs)
                fetch_ok += 1
            except Exception:
                prefetched[race_id] = (None, [])
                fetch_skip += 1

        # バッチ内の全馬から personnel 補完
        all_horses = []
        for _ri, _hs in prefetched.values():
            if _hs:
                all_horses.extend(_hs)

        if all_horses:
            _bl = build_trainer_baseline_db(all_horses)
            db_state["trainer_baseline_db"] = merge_trainer_baseline(
                _bl, db_state["trainer_baseline_db"])

            _course_db_for_shared = {k: list(v) for k, v in db_state["course_db_base"].items()}
            shared_course_db = build_course_db_from_past_runs(
                all_horses, _course_db_for_shared, target_date=_first_day)

            build_bloodline_db(all_horses, netkeiba_client=None, cache_path=BLOODLINE_DB_PATH)

            pmgr = PersonnelDBManager(cache_days=999999)
            batch_jockey_db, batch_trainer_db = pmgr.build_from_horses(
                all_horses, scraper.client, course_db=shared_course_db, save=False)
            # db_state へマージ (バッチ間で蓄積)
            db_state["jockey_db"].update(batch_jockey_db)
            db_state["trainer_db"].update(batch_trainer_db)

            del shared_course_db, all_horses, _course_db_for_shared, batch_jockey_db, batch_trainer_db
            _gc.collect()

        return prefetched

    except Exception as e:
        print(f"  [composite-probe][WARN] バッチ prefetch 失敗: {e}", flush=True)
        return {}


def _build_composite_probe_state(
    target_ym: str,
    scraper_state: dict,
    valid_race_ids: list,
) -> dict | None:
    """後方互換ラッパー (旧インタフェース維持・内部は OOM 対策済バッチ版を呼ぶ)。

    ※ この関数は prefetched を全件保持するため月100R超では OOM リスクあり。
    P0-γ 以降は main() から _build_composite_probe_db_state + バッチ処理を直接使う。
    小規模テスト (35R以下) では引き続きこちらが使える。
    """
    import gc as _gc
    db_state = _build_composite_probe_db_state(target_ym, scraper_state)
    if db_state is None:
        return None

    # 全件を1バッチで prefetch (旧挙動: 後方互換)
    prefetched = _prefetch_batch_and_build_personnel(valid_race_ids, db_state, scraper_state)
    print(
        f"  [composite-probe] prefetch 完了: {len([v for v in prefetched.values() if v[0] is not None])}"
        f"ok / {len(valid_race_ids)}R",
        flush=True,
    )
    _gc.collect()

    db_state["prefetched"] = prefetched
    return db_state


def _run_composite_probe_race(
    race: dict,
    prob_marks: dict,       # horse_id → prob 印 (str)
    probe_state: dict,      # _build_composite_probe_state の返値
) -> dict | None:
    """P0-b: 1レース分の composite/mark を engine で取得し一致率計算に使う情報を返す。

    - probe_state 内の prefetched / jockey_db / trainer_db を利用 (月1回 build 済み)
    - 各レースでは course_db を course_db_base から deepcopy して point-in-time 構築のみ
    - P0-3: course_db_base のリスト値まで独立コピー ({k: list(v)}) で汚染防止
    - 印崩壊防止: prob 印は変更しない。composite + engine 印を新フィールドとして返すのみ
    - キャッシュミス / analyze 失敗時は None を返す (fetch_miss / analyze_error を分離)
    """
    try:
        from src.scraper.race_results import (
            Last3FDBBuilder,
            build_course_db_from_past_runs,
        )
        from src.scraper.personnel import enrich_personnel_with_condition_records
        from src.engine import RaceAnalysisEngine, enrich_course_aptitude_with_style_bias
        import gc

        race_id = race.get("race_id", "")
        race_date = race.get("date", "")
        if not race_id or not race_date:
            return None

        all_courses = probe_state["all_courses"]

        # prefetched から (race_info, horses) を取得 (月1回 build 済み)
        # ★ deepcopy: engine.analyze() が past_runs を in-place 変更するため、
        # 各 _run_composite_probe_race 呼び出しで独立したオブジェクトを使う。
        # これにより前レースの変更が後レースの course_db / composite 計算に波及しない。
        import copy as _copy
        _fetched = probe_state.get("prefetched", {}).get(race_id, (None, []))
        _race_info_orig, _horses_orig = _fetched
        if not _race_info_orig or not _horses_orig:
            # fetch キャッシュなし (キャッシュミス)
            return {"fetch_miss": 1}
        race_info = _race_info_orig  # race_info は変更しないのでコピー不要
        horses = _copy.deepcopy(_horses_orig)

        # 🛡️ P0-b (2026-06-24): offline_mode で過去走 netkeiba 取得を遮断したため、past_runs
        # キャッシュ欠落レースは ability/composite が崩れる (2025-12 ◎一致率14.3%異常の真因)。
        # 過去走カバー率 < 50% (新馬戦 or キャッシュ不完全) は測定除外し、信頼レースのみ集計する。
        _active = [h for h in horses if not getattr(h, "is_scratched", False)]
        _with_runs = sum(1 for h in _active if getattr(h, "past_runs", None))
        if _active and _with_runs / len(_active) < 0.5:
            print(f"  [composite-probe][SKIP] past_runs欠落 {_with_runs}/{len(_active)}頭: {race_id}", flush=True)
            return {"fetch_miss": 1}

        # P0-3: course_db_base からリスト値まで独立コピーして course_db を point-in-time 構築
        # build_course_db_from_past_runs は course_db を inplace .append するため、
        # シャローコピー dict(course_db_base) ではリスト値が共有され月内レース順に汚染される
        _course_db_for_race = {k: list(v) for k, v in probe_state["course_db_base"].items()}
        shared_course_db = build_course_db_from_past_runs(
            horses, _course_db_for_race, target_date=race_date)
        shared_l3f_db = Last3FDBBuilder().build(shared_course_db)

        # jockey_db / trainer_db は月1回 build 済みのものをレース馬で絞る (probe 231-235行 相当)
        _race_jids = {h.jockey_id for h in horses if h.jockey_id}
        _race_tids = {h.trainer_id for h in horses if h.trainer_id}
        jockey_db  = {k: v for k, v in probe_state["jockey_db"].items() if k in _race_jids}
        trainer_db = {k: v for k, v in probe_state["trainer_db"].items() if k in _race_tids}
        enrich_personnel_with_condition_records(jockey_db, trainer_db, shared_course_db)

        # engine 生成 + analyze (probe 237-258行 相当)
        try:
            engine = RaceAnalysisEngine(
                course_db=shared_course_db,
                all_courses=all_courses,
                jockey_db=jockey_db,
                trainer_db=trainer_db,
                trainer_baseline_db=probe_state["trainer_baseline_db"],
                pace_last3f_db=shared_l3f_db,
                course_style_stats_db=probe_state["course_style_db"],
                gate_bias_db=probe_state["gate_bias_db"],
                position_sec_per_rank_db=probe_state["position_sec_db"],
                is_jra=race_info.is_jra,
                target_date=race_date,
            )

            analysis = engine.analyze(race_info, horses, custom_stake=None, netkeiba_client=None)
            analysis = enrich_course_aptitude_with_style_bias(engine, analysis)

            del engine
            gc.collect()
        except Exception as ae:
            print(f"  [composite-probe][WARN] analyze 失敗 ({race_id}): {ae}", flush=True)
            return {"analyze_error": 1}

        # composite / engine 印 を horse_id で収集
        engine_composites: dict = {}   # horse_id → composite
        engine_marks: dict = {}        # horse_id → engine 印 (str)
        for ev in analysis.evaluations:
            h_obj = ev.horse
            h_id = getattr(h_obj, "horse_id", None)
            if h_id:
                engine_composites[h_id] = ev.composite
                engine_marks[h_id] = ev.mark.value if ev.mark else "-"

        # prob 印 vs engine 印 一致率計算
        # ◎ (1位) 一致: prob_marks で ◎/◎ の馬が engine でも ◎/◉ か
        prob_top1 = next((hid for hid, mk in prob_marks.items() if mk in ("◎", "◉")), None)
        engine_top1 = next((hid for hid, mk in engine_marks.items() if mk in ("◎", "◉")), None)
        top1_match = int(prob_top1 is not None and prob_top1 == engine_top1)

        # 上位3頭 ({◎,○,▲}) 集合一致率
        prob_top3 = {hid for hid, mk in prob_marks.items() if mk in ("◉", "◎", "○", "▲")}
        engine_top3 = {hid for hid, mk in engine_marks.items() if mk in ("◉", "◎", "○", "▲")}
        top3_intersect = len(prob_top3 & engine_top3)
        top3_union = len(prob_top3 | engine_top3)
        top3_iou = top3_intersect / top3_union if top3_union else 0.0

        return {
            "engine_composites": engine_composites,
            "engine_marks": engine_marks,
            "top1_match": top1_match,       # 0 or 1
            "top3_iou": top3_iou,           # 0.0〜1.0 (Jaccard)
            "measured": 1,
        }

    except Exception as e:
        print(f"  [composite-probe][WARN] 予期しない失敗 ({race.get('race_id','?')}): {e}", flush=True)
        return {"analyze_error": 1}


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
    composite_probe_state: dict | None = None,
    composite_probe_db_state: dict | None = None,
    composite_probe_scraper_state: dict | None = None,
) -> dict:
    """
    target_ym: 'YYYY-MM' (予想対象月)
    ローリング1年ウィンドウで学習 → 予想 → DB保存

    composite_probe_state: P0-b --composite-probe ON 時のみ非 None。
      _build_composite_probe_state() の返値 (prefetched 含む旧形式)。
      既存の印・買い目・ROI は不変のまま、composite を parallel 測定。

    composite_probe_db_state: P0-γ OOM対策 — prefetched を含まない DB 状態。
      composite_probe_scraper_state と併用してバッチ25件ずつprefetch→probe→del。
      composite_probe_state より優先される。

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
    # (import math はモジュール先頭に既存 — F811 回避のためここでは import しない)
    brier_sum = logloss_sum = 0.0
    calib_n = 0
    # T-7 Phase1: WF版較正テーブル用 bin別カウント (probe成功時のみ集計)
    # {org: {bin_label: {n, win, place2, place3}}} — org は 'JRA'/'NAR'/'ALL'
    calib_by_bin: dict = defaultdict(lambda: defaultdict(lambda: {"n": 0, "win": 0, "place2": 0, "place3": 0}))
    # P0-b Step3-1 (2026-06-24): composite probe 統計 — 印崩壊防止・parallel 測定のみ
    probe_top1_match = probe_top3_iou_sum = 0.0
    probe_measured = 0      # engine.analyze が完走したレース数
    probe_fetch_miss = 0    # fetch キャッシュなし (prefetch 時点でスキップ)
    probe_analyze_error = 0 # analyze 失敗 (engine 例外等)
    probe_analyze_sec = 0.0  # analyze 秒計
    _probe_race_count = 0  # 上限チェック用 (_COMPOSITE_PROBE_MAX_RACES)

    # P0-γ OOM対策: バッチ probe 用変数
    # composite_probe_db_state が渡されたときはバッチ25件ずつprefetch→probe→del
    _use_batch_probe = (
        composite_probe_db_state is not None
        and composite_probe_scraper_state is not None
        and COMPOSITE_PROBE
        and composite_probe_state is None  # 旧形式 (prefetched含む) が渡された場合は旧ロジック優先
    )
    _batch_probe_prefetched: dict = {}   # 現バッチの prefetched キャッシュ
    _batch_probe_race_ids_remaining: list = []  # 未処理 race_id リスト (バッチ分割用)
    if _use_batch_probe:
        # 上限適用後の race_id を事前収集 (probe 対象のみ)
        _all_valid_race_ids = [
            r[0].get("race_id", "")
            for r in valid_races_data
            if r[0].get("race_id", "")
        ]
        # 上限クリップ
        if _COMPOSITE_PROBE_MAX_RACES > 0:
            _batch_probe_race_ids_remaining = _all_valid_race_ids[:_COMPOSITE_PROBE_MAX_RACES]
        else:
            _batch_probe_race_ids_remaining = _all_valid_race_ids
        print(
            f"  [composite-probe][batch] {target_ym} OOM対策バッチ処理: "
            f"{len(_batch_probe_race_ids_remaining)}R / batch={_COMPOSITE_PROBE_BATCH_SIZE}R",
            flush=True,
        )
        # 最初のバッチをプリフェッチ
        import gc as _gc_batch
        _first_batch = _batch_probe_race_ids_remaining[:_COMPOSITE_PROBE_BATCH_SIZE]
        _batch_probe_prefetched = _prefetch_batch_and_build_personnel(
            _first_batch, composite_probe_db_state, composite_probe_scraper_state)
        _batch_probe_race_ids_remaining = _batch_probe_race_ids_remaining[_COMPOSITE_PROBE_BATCH_SIZE:]
        composite_probe_db_state["prefetched"] = _batch_probe_prefetched
        print(
            f"  [composite-probe][batch] 初回バッチprefetch完了: {len(_batch_probe_prefetched)}R",
            flush=True,
        )

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

        # P0-b Step3-1: composite probe — 印は変更しない・parallel 測定のみ
        # P0-α (2026-06-24): ADOPT_COMPOSITE_MARKS=True 時は engine印/composite を採用
        # P0-γ (2026-06-24): OOM対策バッチ処理対応 — _use_batch_probe=True 時はバッチ管理
        _probe_result_for_adopt: dict | None = None  # 採用用probe結果 (None=probe未実行 or 失敗)

        # P0-γ: バッチ probe モードのとき、現レースのrace_idが現バッチ内に無ければ次バッチをロード
        if _use_batch_probe and COMPOSITE_PROBE:
            _cur_race_id = race.get("race_id", "")
            if _cur_race_id and _cur_race_id not in composite_probe_db_state.get("prefetched", {}):
                # 現バッチに含まれていない → 次バッチが必要
                if _batch_probe_race_ids_remaining:
                    import gc as _gc_inner
                    # 前バッチの prefetched を解放
                    _old_pf = composite_probe_db_state.get("prefetched")
                    composite_probe_db_state["prefetched"] = {}
                    if _old_pf is not None:
                        del _old_pf
                    _gc_inner.collect()
                    # 次バッチをロード
                    _next_batch = _batch_probe_race_ids_remaining[:_COMPOSITE_PROBE_BATCH_SIZE]
                    _batch_probe_prefetched = _prefetch_batch_and_build_personnel(
                        _next_batch, composite_probe_db_state, composite_probe_scraper_state)
                    _batch_probe_race_ids_remaining = _batch_probe_race_ids_remaining[_COMPOSITE_PROBE_BATCH_SIZE:]
                    composite_probe_db_state["prefetched"] = _batch_probe_prefetched
                    print(
                        f"  [composite-probe][batch] 次バッチ: {len(_batch_probe_prefetched)}R, "
                        f"残={len(_batch_probe_race_ids_remaining)}R",
                        flush=True,
                    )

        # probe 実行 (旧形式 composite_probe_state または バッチ版 composite_probe_db_state)
        _active_probe_state = composite_probe_state if composite_probe_state is not None else (
            composite_probe_db_state if _use_batch_probe else None
        )
        if _active_probe_state is not None and COMPOSITE_PROBE:
            _do_probe = (_COMPOSITE_PROBE_MAX_RACES <= 0 or _probe_race_count < _COMPOSITE_PROBE_MAX_RACES)
            if _do_probe:
                _probe_race_count += 1
                _t_probe = time.time()
                _probe_result = _run_composite_probe_race(
                    race, marks,
                    _active_probe_state,
                )
                probe_analyze_sec += time.time() - _t_probe
                if _probe_result is None:
                    probe_analyze_error += 1
                elif _probe_result.get("fetch_miss"):
                    probe_fetch_miss += 1
                elif _probe_result.get("analyze_error"):
                    probe_analyze_error += 1
                else:
                    probe_top1_match += _probe_result["top1_match"]
                    probe_top3_iou_sum += _probe_result["top3_iou"]
                    probe_measured += 1
                    # P0-α: probe 成功時のみ採用候補として保持
                    _probe_result_for_adopt = _probe_result
                    # T-7 Phase1: probe成功レースのみ偏差値bin別カウント集計
                    _ec = _probe_result.get("engine_composites") or {}
                    _is_banei = race.get("is_banei", False) or race.get("venue_code") == "65"
                    if _ec and not _is_banei:
                        _is_jra = race.get("is_jra", False)
                        _org = "JRA" if _is_jra else "NAR"
                        for _hd in horse_dicts:
                            _hid = _hd.get("horse_id", "")
                            _fp = _hd.get("finish_pos")
                            if not _hid or _fp is None or _hid not in _ec:
                                continue
                            _comp_val = _ec[_hid]
                            if _comp_val is None:
                                continue
                            _bin = composite_to_bin(float(_comp_val))
                            if _bin is None:
                                continue
                            for _o in (_org, "ALL"):
                                _rec = calib_by_bin[_o][_bin]
                                _rec["n"] += 1
                                if _fp == 1:
                                    _rec["win"] += 1
                                if _fp <= 2:
                                    _rec["place2"] += 1
                                if _fp <= 3:
                                    _rec["place3"] += 1

        # P0-α: engine印/composite を採用 (ADOPT_COMPOSITE_MARKS=True かつ probe成功)
        if ADOPT_COMPOSITE_MARKS and _probe_result_for_adopt is not None:
            _engine_marks = _probe_result_for_adopt.get("engine_marks")
            if _engine_marks:
                # engine印を採用 (本番印: TEKIPAN/穴/☆6位固定/×危険/抑 等の複雑ロジック)
                marks = _engine_marks
            else:
                # engine_marks が空なら prob印 fallback (安全策)
                print(f"  [composite-marks][fallback] engine_marks 空 → prob印使用: {race_id}", flush=True)
        elif ADOPT_COMPOSITE_MARKS and _probe_result_for_adopt is None:
            # probe 未実行 or 失敗 → prob印 fallback ログ
            # (上限超過 or fetch_miss / analyze_error 時)
            print(f"  [composite-marks][fallback] probe未成功 → prob印使用: {race_id}", flush=True)

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
            entry = _build_horse_entry(hd, hid, prob, mk, ru, field_count, race=race, tracker=tracker)
            # P0-α: ADOPT_COMPOSITE_MARKS=True かつ engine composite が取得できた場合に上書き
            # _build_horse_entry 内の composite = round(prob * 100, 2) を engine composite に差し替え
            if (
                ADOPT_COMPOSITE_MARKS
                and _probe_result_for_adopt is not None
                and hid
            ):
                _ec = _probe_result_for_adopt.get("engine_composites", {}).get(hid)
                if _ec is not None:
                    entry["composite"] = round(float(_ec), 2)
            pred_horses.append(entry)

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
    # --no-save 時: DB/ファイル保存を全スキップ (calib_by_bin/probe 集計には不要)
    import os as _os
    pred_json_dir = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "data", "predictions")
    if not _NO_SAVE:
        _os.makedirs(pred_json_dir, exist_ok=True)

    for date_str in sorted(dates_data.keys()):
        dd = dates_data[date_str]
        # A-3b 修正 (2026-05-25): date_str がハイフン形式 (YYYY-MM-DD) で来る
        # 既存 pred.json/DB はハイフンなし (YYYYMMDD) 形式 → 変換必要
        date_compact = date_str.replace("-", "")

        if _NO_SAVE:
            # --no-save: 本番 predictions/results テーブルおよび pred.json を汚染しない
            # calib_by_bin 集計は dates_data から直接行われるためスキップ不要
            continue

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

    # P0-γ OOM対策: バッチ probe 終了後に残 prefetched を解放
    if _use_batch_probe and composite_probe_db_state is not None:
        import gc as _gc_cleanup
        _old_pf = composite_probe_db_state.get("prefetched")
        composite_probe_db_state["prefetched"] = {}
        if _old_pf is not None:
            del _old_pf
        _gc_cleanup.collect()

    # P0-b Step3-1: composite probe 集計
    probe_top1_rate = probe_top1_match / probe_measured if probe_measured else None
    probe_top3_iou  = probe_top3_iou_sum / probe_measured if probe_measured else None
    avg_probe_sec   = probe_analyze_sec / probe_measured if probe_measured else None

    if (composite_probe_state is not None or _use_batch_probe) and COMPOSITE_PROBE:
        if probe_measured > 0:
            print(
                f"  [composite-probe] {target_ym}: "
                f"◎一致率={probe_top1_rate*100:.1f}%  "
                f"上位3 IoU={probe_top3_iou:.3f}  "
                f"測定={probe_measured}R  "
                f"fetch_miss={probe_fetch_miss}R  analyze_err={probe_analyze_error}R  "
                f"avg={avg_probe_sec:.1f}秒/R",
                flush=True,
            )
            # 注記: WF印=ML複勝確率ランク印(◉=norm_gap≥0.5/TEKIPAN無) vs engine印=本番印
            # (TEKIPAN/穴/危険/reassign 含む) — 一致率は概念の違いを含む
            # 注記: course_db=月初前日時点固定のため月内後半レースは最大30日分履歴が少ない
        else:
            print(
                f"  [composite-probe] {target_ym}: 測定レースなし "
                f"(fetch_miss={probe_fetch_miss} analyze_err={probe_analyze_error})",
                flush=True,
            )

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
        # P0-b Step3-1: composite probe 指標 (COMPOSITE_PROBE=False 時は None)
        "probe_top1_rate": probe_top1_rate,
        "probe_top3_iou": probe_top3_iou,
        "probe_measured": probe_measured,
        "probe_fetch_miss": probe_fetch_miss,
        "probe_analyze_error": probe_analyze_error,
        "probe_avg_sec": avg_probe_sec,
        # T-7 Phase1: WF版較正テーブル用 bin別 raw counts (probe成功時のみ・空なら空dict)
        "calib_by_bin": {o: dict(bins) for o, bins in calib_by_bin.items()},
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
    parser.add_argument("--composite-probe", action="store_true", default=False,
                        help="P0-b: 本番engine composite を並行計算し prob印との一致率を測定（測定のみ・印は不変）")
    parser.add_argument("--composite-probe-max-races", type=int, default=0,
                        help="P0-b: composite-probe で測定するレース数上限/月 (0=無制限・1月サンプル測定で使用)")
    parser.add_argument("--composite-marks", action="store_true", default=False,
                        help="P0-α: 本番engine印/composite を WF pred に採用 (--composite-probe を含意)")
    parser.add_argument("--no-save", action="store_true", default=False,
                        help="DB save_prediction/save_results および pred.json export をスキップ"
                             " (calib_by_bin/brier/logloss/probe 統計は通常通り集計)")
    args = parser.parse_args()

    # A-3e: shobu_score 計算レベルを反映 (グローバル切替)
    global SHOBU_SCORE_LV, COMPOSITE_PROBE, _COMPOSITE_PROBE_MAX_RACES, ADOPT_COMPOSITE_MARKS, _NO_SAVE
    SHOBU_SCORE_LV = args.shobu_lv
    if SHOBU_SCORE_LV >= 2:
        print(f"  shobu_score: Lv{SHOBU_SCORE_LV} (engine 直呼び / A-3e Step 1)")
    else:
        print(f"  shobu_score: Lv{SHOBU_SCORE_LV} (簡易再現 / A-3d)")

    ADOPT_COMPOSITE_MARKS = args.composite_marks
    # --composite-marks は --composite-probe を含意 (probe なしに adopt は不可)
    if ADOPT_COMPOSITE_MARKS:
        COMPOSITE_PROBE = True
        print(f"  composite-marks: ON (P0-α 本番engine印/composite 採用 → probe 強制ON)")
    else:
        COMPOSITE_PROBE = args.composite_probe

    _NO_SAVE = args.no_save
    if _NO_SAVE:
        print("  --no-save: ON (DB save_prediction/save_results + pred.json export をスキップ)")

    _COMPOSITE_PROBE_MAX_RACES = args.composite_probe_max_races
    if COMPOSITE_PROBE:
        _lim = f"上限{_COMPOSITE_PROBE_MAX_RACES}R/月" if _COMPOSITE_PROBE_MAX_RACES > 0 else "全レース"
        if ADOPT_COMPOSITE_MARKS:
            print(f"  composite-probe: ON (採用モード・{_lim})")
        else:
            print(f"  composite-probe: ON (P0-b 本番engine 並行測定・印不変・{_lim})")
    else:
        print("  composite-probe: OFF")

    t_total = time.time()

    print(f"\n{'=' * 62}")
    print(f"  Walk-Forward バックテスト予想生成")
    print(f"  期間: {args.start} ~ {args.end}")
    print(f"  学習ウィンドウ: {args.train_months}ヶ月 (ローリング)")
    print(f"  force: {args.force}")
    print(f"{'=' * 62}\n")

    # P0-b Step3-1: composite probe 用スクレイパー/MLモデル初期化 (全期間1回・フラグON時のみ)
    _scraper_state: dict | None = None
    if COMPOSITE_PROBE:
        print("\n[P0-b composite-probe] スクレイパー・MLモデル初期化中...", flush=True)
        _t_init = time.time()
        try:
            from data.masters.course_master import get_all_courses
            from src.scraper.auth import PremiumNetkeibaScraper
            from src.engine import reset_engine_caches, RaceAnalysisEngine
            import src.engine as _eng
            from src.ml.lgbm_ranker import LGBMRanker as _LR

            _all_courses = get_all_courses()
            _scraper = PremiumNetkeibaScraper(_all_courses, ignore_ttl=True)
            _scraper.login()
            _scraper.training.login()
            # 🛡️ P0-b netkeiba遮断 (2026-06-24 事故対応): past_runs キャッシュ欠落時に
            # fetch_race が ThreadPoolExecutor(max_workers=3) で netkeiba を並列取得する経路
            # (auth.py:860) を _offline_mode=True で完全遮断 (auth.py:857 で未キャッシュ馬の
            # 過去走取得をスキップ)。WF は「キャッシュにある分だけ」で leak-free かつ完全オフライン測定。
            _scraper._offline_mode = True
            reset_engine_caches()

            # MLモデルウォームアップ (engine 初回 analyze の遅延を減らす)
            _warmup = RaceAnalysisEngine(
                course_db={}, all_courses=_all_courses,
                jockey_db={}, trainer_db={},
                trainer_baseline_db={},
                pace_last3f_db={},
                course_style_stats_db={},
                gate_bias_db={},
                position_sec_per_rank_db={},
                is_jra=True, target_date=args.start + "-01",
            )
            del _warmup

            # ランカーモデル強制リロード
            _eng._CACHE_RANKER_LOADED = False
            _eng._CACHE_LGBM_RANKER = None
            _lr = _LR()
            if _lr.load():
                _eng._CACHE_LGBM_RANKER = _lr
                _eng._CACHE_RANKER_LOADED = True
                print(f"  ランカーモデル強制リロード完了 (features={_lr._model.num_feature()})", flush=True)
            else:
                print("  [WARN] ランカーモデルロード失敗 — composite probe は動作するが精度低下の可能性", flush=True)

            _scraper_state = {"scraper": _scraper, "all_courses": _all_courses}
            print(f"  [P0-b] 初期化完了: {time.time()-_t_init:.1f}秒", flush=True)

        except Exception as _e:
            print(f"  [P0-b][WARN] スクレイパー初期化失敗: {_e} → composite-probe を無効化", flush=True)
            COMPOSITE_PROBE = False

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

        # P0-γ OOM対策: 月次 engine build (composite-probe ON 時のみ)
        # DB状態のみ構築 (prefetched 含まず) → _process_month 内でバッチ25件ずつ処理
        _month_probe_state: dict | None = None         # 旧形式 (35R以下の後方互換)
        _month_probe_db_state: dict | None = None       # 新バッチ形式
        if COMPOSITE_PROBE and _scraper_state is not None:
            _t_build = time.time()
            # 月の検証レース件数を確認
            _valid_start = f"{target_ym}-01"
            _valid_end   = _months_add(target_ym, 1)
            _month_race_count = sum(
                1 for r in races
                if _valid_start <= r.get("date", "") < _valid_end
                   and r.get("race_id", "")
            )
            _capped_count = min(_month_race_count, _COMPOSITE_PROBE_MAX_RACES) if _COMPOSITE_PROBE_MAX_RACES > 0 else _month_race_count
            if _COMPOSITE_PROBE_MAX_RACES > 0:
                print(
                    f"  [composite-probe] {target_ym} 上限適用: {_COMPOSITE_PROBE_MAX_RACES}R ({_month_race_count}R中)",
                    flush=True,
                )
            else:
                print(
                    f"  [composite-probe] {target_ym} 全レース: {_month_race_count}R",
                    flush=True,
                )

            # P0-γ: DB状態のみ構築 → _process_month にバッチ分割を委ねる
            _month_probe_db_state = _build_composite_probe_db_state(target_ym, _scraper_state)
            print(f"  [composite-probe] {target_ym} DB build: {time.time()-_t_build:.1f}秒", flush=True)

        result = _process_month(
            target_ym, races, sire_map, params,
            tracker, sire_tracker, args.force,
            composite_probe_state=_month_probe_state,
            composite_probe_db_state=_month_probe_db_state,
            composite_probe_scraper_state=_scraper_state if _month_probe_db_state else None,
        )

        # P0-γ: 月処理後に db_state を明示 del (course_db_base が大きい)
        if _month_probe_db_state is not None:
            import gc as _gc_month
            del _month_probe_db_state
            _month_probe_db_state = None
            _gc_month.collect()
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

    # P0-b Step3-1: composite probe 全期間加重平均 (probe_measured 重み)
    _pm_total = sum(s.get("probe_measured", 0) for s in ok_months)
    avg_probe_top1 = (
        sum((s.get("probe_top1_rate") or 0) * s.get("probe_measured", 0) for s in ok_months) / _pm_total
        if _pm_total else None
    )
    avg_probe_top3_iou = (
        sum((s.get("probe_top3_iou") or 0) * s.get("probe_measured", 0) for s in ok_months) / _pm_total
        if _pm_total else None
    )
    _pm_fetch_miss  = sum(s.get("probe_fetch_miss", 0) for s in ok_months)
    _pm_analyze_err = sum(s.get("probe_analyze_error", 0) for s in ok_months)
    avg_probe_sec = (
        sum((s.get("probe_avg_sec") or 0) * s.get("probe_measured", 0) for s in ok_months) / _pm_total
        if _pm_total else None
    )

    print(f"\n{'=' * 62}")
    print(f"  Walk-Forward バックテスト完了!")
    print(f"  処理月数:   {len(ok_months)}/{len(months)}")
    print(f"  予想レース: {total_preds:,}")
    print(f"  結果レース: {total_results:,}")
    print(f"  平均Top1率: {avg_top1:.1f}%")
    print(f"  較正Brier:  {avg_brier:.4f} (低=良・複勝確率の二乗誤差)")
    print(f"  較正LogLoss:{avg_logloss:.4f} (低=良)")
    # P0-b Step3-1: composite probe サマリー
    if COMPOSITE_PROBE and _pm_total:
        print(f"  composite-probe:")
        print(f"    ◎一致率:     {avg_probe_top1*100:.1f}%  (WF印 vs engine印・1位一致)")
        print(f"    上位3 IoU:   {avg_probe_top3_iou:.3f}  (Jaccard係数・◎○▲3頭集合)")
        print(f"    測定レース:  {_pm_total}R  fetch_miss={_pm_fetch_miss}R  analyze_err={_pm_analyze_err}R")
        print(f"    avg分析速度: {avg_probe_sec:.1f}秒/R")
        print(f"    注記: WF印=ML複勝確率ランク印 / engine印=本番印(TEKIPAN/穴/reassign含む)")
        print(f"    注記: course_db=月初前日時点固定(月内後半レースは最大30日分履歴少)")
    print(f"  合計時間:   {elapsed_total/60:.1f}分")
    print(f"{'=' * 62}\n")

    # T-7 Phase1: WF版較正テーブル raw counts を dump
    # COMPOSITE_PROBE=True かつ probe成功月が1件以上ある場合のみ出力
    _wf_calib_months = [s for s in ok_months if s.get("calib_by_bin")]
    if _wf_calib_months:
        # 全月の counts をカウント単位で合算
        _merged_calib: dict = defaultdict(lambda: defaultdict(lambda: {"n": 0, "win": 0, "place2": 0, "place3": 0}))
        for _ms in _wf_calib_months:
            for _org, _bins in _ms["calib_by_bin"].items():
                for _bin_label, _cnt in _bins.items():
                    _mrec = _merged_calib[_org][_bin_label]
                    _mrec["n"] += _cnt.get("n", 0)
                    _mrec["win"] += _cnt.get("win", 0)
                    _mrec["place2"] += _cnt.get("place2", 0)
                    _mrec["place3"] += _cnt.get("place3", 0)
        # 通常dict化 (JSONシリアライズ可能に)
        _merged_calib_plain = {o: {b: dict(c) for b, c in bins.items()} for o, bins in _merged_calib.items()}

        # 出力先: data/_diag/wf_calib_raw/<start>_<end>.json
        _raw_dir = Path("data/_diag/wf_calib_raw")
        _raw_dir.mkdir(parents=True, exist_ok=True)
        _raw_fname = _raw_dir / f"{args.start.replace('-','')}_to_{args.end.replace('-','')}.json"
        with open(_raw_fname, "w", encoding="utf-8") as _rf:
            json.dump(_merged_calib_plain, _rf, ensure_ascii=False, indent=2)

        _total_n = sum(b["n"] for b in _merged_calib_plain.get("ALL", {}).values())
        print(f"[T-7 Phase1] WF較正 raw counts 出力: {_raw_fname}")
        print(f"  合算月数: {len(_wf_calib_months)} / 総馬数(ALL): {_total_n:,}")
        print(f"  (scripts/build_calibration_composite_wf.py で率化・比較出力)")
    else:
        if COMPOSITE_PROBE:
            print("[T-7 Phase1] calib_by_bin: probe成功月なし — raw counts 未出力")

    print("ダッシュボード「結果分析」タブで確認してください。")


if __name__ == "__main__":
    main()
