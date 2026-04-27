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


def _build_horse_entry(h: dict, hid: str, prob: float, mk: str,
                       rank_u: int, field_count: int) -> dict:
    """1頭分の予想データ構造"""
    ana_type, kiken_type = _get_ana_kiken(rank_u, mk, h, field_count)
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
        "jockey_change_score": 0.0, "shobu_score": 0.0,
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

    # numpy変換
    def _to_np(rows):
        mat = []
        for f in rows:
            mat.append([
                float(f[c]) if f[c] is not None else float("nan")
                for c in FEATURE_COLUMNS
            ])
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

        marks = _assign_marks(probs)
        confidence = _judge_confidence(probs)

        sorted_ids = [hid for hid, _ in sorted(probs.items(), key=lambda x: -x[1])]
        rank_map = {hid: i for i, hid in enumerate(sorted_ids)}
        field_count = len(horse_dicts)

        unmarked_sorted = [hid for hid in sorted_ids if marks.get(hid, "-") == "-"]
        rank_u_map = {hid: i for i, hid in enumerate(unmarked_sorted)}

        # 予想馬データ
        pred_horses = []
        for hd in horse_dicts:
            hid = hd.get("horse_id", "")
            prob = probs.get(hid, 0.0)
            mk = marks.get(hid, "-")
            ru = rank_u_map.get(hid, field_count)
            pred_horses.append(_build_horse_entry(hd, hid, prob, mk, ru, field_count))

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
    for date_str in sorted(dates_data.keys()):
        dd = dates_data[date_str]
        if not force:
            existing = db.load_prediction(date_str)
            if existing and existing.get("races"):
                continue
        payload = {"date": date_str, "version": 2, "races": dd["races"]}
        try:
            db.save_prediction(date_str, payload)
            preds_saved += len(dd["races"])
        except Exception as e:
            print(f"    [WARN] pred save {date_str}: {e}")

        if dd["results"]:
            try:
                db.save_results(date_str, dd["results"])
                results_saved += len(dd["results"])
            except Exception as e:
                print(f"    [WARN] result save {date_str}: {e}")

    top1_rate = top1_hit / top1_total if top1_total else 0

    return {
        "month": target_ym,
        "status": "ok",
        "n_train": n_train,
        "n_valid_races": n_valid_races,
        "preds_saved": preds_saved,
        "results_saved": results_saved,
        "top1_rate": top1_rate,
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
    args = parser.parse_args()

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

    print(f"\n{'=' * 62}")
    print(f"  Walk-Forward バックテスト完了!")
    print(f"  処理月数:   {len(ok_months)}/{len(months)}")
    print(f"  予想レース: {total_preds:,}")
    print(f"  結果レース: {total_results:,}")
    print(f"  平均Top1率: {avg_top1:.1f}%")
    print(f"  合計時間:   {elapsed_total/60:.1f}分")
    print(f"{'=' * 62}\n")
    print("ダッシュボード「結果分析」タブで確認してください。")


if __name__ == "__main__":
    main()
