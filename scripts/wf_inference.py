"""Walk-Forward ML推論スクリプト

各WF期間のモデルを使って pred.json に ml_composite_adj を再設定する。

手順:
  1. 訓練期間のデータから rolling stats (tracker) を構築
  2. WF モデルをロード
  3. 推論期間のレースを日付順に処理:
     - predict_race → ML win_prob
     - ml_composite_adj 計算 (engine.py Step 5.6 ロジック)
     - tracker 更新 (次レースの rolling stats に反映)
  4. pred.json 更新: composite 再計算 + marks + tickets 再生成
     - popularity_blend を適用 (期間別 popularity_rates_{wf_name}.json を使用)
     - L-2 修正: 循環参照リークなしの統計テーブルを使用
"""

import argparse
import json
import math
import os
import pickle
import sys
import time
from glob import glob
from itertools import combinations
from typing import Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

ML_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "ml")
PREDICTIONS_DIR = os.path.join(PROJECT_ROOT, "data", "predictions")

WF_PERIODS = {
    "wf_2024": {
        "model_dir": os.path.join(PROJECT_ROOT, "data", "models", "wf_2024"),
        "train_max": "2023-12-31",
        "infer_start": "20240101",
        "infer_end": "20241231",
    },
    "wf_2025": {
        "model_dir": os.path.join(PROJECT_ROOT, "data", "models", "wf_2025"),
        "train_max": "2024-12-31",
        "infer_start": "20250101",
        "infer_end": "20251231",
    },
    "wf_2026": {
        "model_dir": os.path.join(PROJECT_ROOT, "data", "models", "wf_2026"),
        "train_max": "2025-12-31",
        "infer_start": "20260101",
        "infer_end": "20261231",
    },
}

MARK_ORDER = ["◎", "○", "▲", "△", "★", "☆"]
PATTERN_MAP = {"SS": "E", "S": "C", "A": "C", "B": "D", "C": "D", "D": "D", "E": "skip"}
M_PRIME_FORMAT = "M': 自信度別 三連複 (SS=E/S=C/A=C/B/C/D=D/E=skip)"


def build_tracker(max_date: str):
    """_collect_all_rows の簡易版: tracker と sire_tracker のみ構築 (特徴量抽出スキップ)"""
    from src.ml.lgbm_model import (
        _load_ml_races, RollingStatsTracker, RollingSireTracker,
        SIRE_MAP_PATH,
    )

    races = _load_ml_races(max_date=max_date)
    if not races:
        raise ValueError(f"ML data not found for max_date={max_date}")

    tracker = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    from src.ml.lgbm_model import _load_horse_sire_map
    sire_map = _load_horse_sire_map()

    races.sort(key=lambda r: r.get("date", ""))
    for race in races:
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    print(f"  tracker 構築完了: {len(tracker.horses)} horses, max_date={max_date}")
    return tracker, sire_tracker, sire_map


def load_iso_calibrators(model_dir: str) -> dict:
    """期間別 Isotonic calibrators をロードする。

    build_iso_calibrator_wf.py が生成した iso_cal_top3.pkl をロードして返す。
    ファイルが存在しない場合は空 dict を返す (calibration スキップ)。

    Args:
        model_dir: WF モデルディレクトリのパス

    Returns:
        {"top3": IsotonicRegression} の dict。ファイルなし時は {}
    """
    out = {}
    for target in ["top3"]:  # top3 のみ (win は方針 1 で活用予定)
        path = os.path.join(model_dir, f"iso_cal_{target}.pkl")
        if os.path.exists(path):
            with open(path, "rb") as f:
                out[target] = pickle.load(f)
            print(f"  Isotonic calibrator ロード: {path}")
        else:
            print(f"  INFO: {path} が見つかりません。Isotonic calibration をスキップします。")
            print(f"  先に `python scripts/build_iso_calibrator_wf.py` を実行してください。")
    return out


def load_wf_predictor(model_dir: str, tracker, sire_tracker, iso_calibrators: Optional[dict] = None):
    """WF モデルディレクトリから LGBMPredictor 相当のオブジェクトを構築

    Args:
        model_dir: WF モデルディレクトリのパス
        tracker: RollingStatsTracker オブジェクト
        sire_tracker: RollingSireTracker オブジェクト
        iso_calibrators: load_iso_calibrators() の戻り値。
                         None または {} の場合は Isotonic calibration をスキップ。
    """
    import lightgbm as lgb
    from src.ml.lgbm_model import FEATURE_COLUMNS, FEATURE_COLUMNS_BANEI, _extract_features, _add_race_relative_features, _smile_key_ml, SURFACE_MAP
    import numpy as np

    # iso_calibrators が None の場合は空 dict として扱う
    _iso_calibrators = iso_calibrators if iso_calibrators is not None else {}

    models = {}
    cal_params = {}

    for fname in os.listdir(model_dir):
        if not fname.startswith("lgbm_place") or not fname.endswith(".txt"):
            continue
        fpath = os.path.join(model_dir, fname)
        stem = fname[len("lgbm_place"):-len(".txt")].lstrip("_")
        key = stem if stem else "global"
        try:
            models[key] = lgb.Booster(model_file=fpath)
        except Exception as e:
            print(f"  WARNING: {key} load failed: {e}")

    for fname in os.listdir(model_dir):
        if not fname.startswith("lgbm_place") or not fname.endswith("_cal.json"):
            continue
        stem = fname[len("lgbm_place"):-len("_cal.json")].lstrip("_")
        key = stem if stem else "global"
        try:
            with open(os.path.join(model_dir, fname)) as f:
                cal_params[key] = json.load(f)
        except Exception:
            pass

    print(f"  WF model loaded: {len(models)} models from {os.path.basename(model_dir)}")

    def select_model(surface_val, is_jra, venue_code, smile_cat):
        """階層モデル選択 (LGBMPredictor._select_model と同等)"""
        if smile_cat:
            sf = "turf" if surface_val == 0 else "dirt"
            k4 = f"jra_{sf}_{smile_cat}" if is_jra else None
            if k4 and k4 in models:
                return models[k4], k4
        k3 = f"venue_{venue_code}"
        if k3 in models:
            return models[k3], k3
        if is_jra:
            sf = "turf" if surface_val == 0 else "dirt"
            k2 = f"jra_{sf}"
            if k2 in models:
                return models[k2], k2
        sf_key = "turf" if surface_val == 0 else "dirt"
        if sf_key in models:
            return models[sf_key], sf_key
        return models.get("global"), "global"

    def predict_race(race_dict, horse_dicts):
        """predict_race の軽量版 (トレーニング特徴量なし)"""
        surface_val = SURFACE_MAP.get(race_dict.get("surface", ""), -1)
        is_jra = bool(race_dict.get("is_jra", True))
        venue_code = str(race_dict.get("venue_code", "") or "").zfill(2)
        distance = int(race_dict.get("distance") or 0)
        smile_cat = _smile_key_ml(distance) if distance else ""

        model, model_key = select_model(surface_val, is_jra, venue_code, smile_cat)
        if model is None:
            return {}

        features, ids = [], []
        for h in horse_dicts:
            feat = _extract_features(h, race_dict, tracker, sire_tracker)
            features.append(feat)
            ids.append(h.get("horse_id", ""))

        if not features:
            return {}

        _add_race_relative_features(features)

        from data.masters.venue_master import is_banei as _is_banei_pred
        _is_banei_vc = _is_banei_pred(venue_code)
        feat_cols = FEATURE_COLUMNS_BANEI if _is_banei_vc else FEATURE_COLUMNS
        if hasattr(model, "num_feature"):
            n = model.num_feature()
            if n < len(feat_cols):
                feat_cols = feat_cols[:n]

        X = np.array(
            [[float(f.get(c)) if f.get(c) is not None else float("nan") for c in feat_cols]
             for f in features],
            dtype=np.float32,
        )
        probs = model.predict(X)

        # Platt scaling (既存処理)
        cal = cal_params.get(model_key)
        if cal:
            a, b = cal["a"], cal["b"]
            probs = [1.0 / (1.0 + math.exp(-(a * p + b))) for p in probs]

        # Isotonic calibration 適用 (Platt → Isotonic の二段階)
        # build_iso_calibrator_wf.py が生成した iso_cal_top3.pkl を使用
        iso_top3 = _iso_calibrators.get("top3")
        if iso_top3 is not None:
            probs = iso_top3.transform(np.array(probs))

        return {hid: float(p) for hid, p in zip(ids, probs)}

    return predict_race


def calc_ml_composite_adj(horses_data: list, ml_probs: dict):
    """engine.py Step 5.6 と同等の ml_composite_adj 計算

    horses_data: pred.json の horses リスト (composite, horse_id, odds 等を含む)
    ml_probs: {horse_id: P(top3)}
    """
    active = [h for h in horses_data if not h.get("is_scratched") and not h.get("scrape_failed")]
    if len(active) < 3:
        return

    # ML確率を win_prob に変換 (softmax正規化)
    ml_vals = []
    for h in active:
        hid = str(h.get("horse_id", ""))
        p = ml_probs.get(hid, 0.0)
        ml_vals.append((h, p))

    probs = [p for _, p in ml_vals]
    avg_p = sum(probs) / len(probs)
    std_p = (sum((p - avg_p) ** 2 for p in probs) / len(probs)) ** 0.5

    if std_p <= 0.001:
        return

    # composite 順位
    comp_vals = [(h.get("composite", 50.0), i) for i, (h, _) in enumerate(ml_vals)]
    comp_vals.sort(key=lambda x: -x[0])
    comp_ranks = [0] * len(ml_vals)
    for rank, (_, idx) in enumerate(comp_vals):
        comp_ranks[idx] = rank + 1

    # ML prob 順位
    wp_vals = [(p, i) for i, (_, p) in enumerate(ml_vals)]
    wp_vals.sort(key=lambda x: -x[0])
    wp_ranks = [0] * len(ml_vals)
    for rank, (_, idx) in enumerate(wp_vals):
        wp_ranks[idx] = rank + 1

    for i, (h, p) in enumerate(ml_vals):
        z = (p - avg_p) / std_p
        raw_adj = max(-5.0, min(5.0, z * 1.5))

        rank_gap = wp_ranks[i] - comp_ranks[i]
        if rank_gap >= 3:
            rank_penalty = min(3.0, (rank_gap - 2) * 0.5)
            raw_adj -= rank_penalty

        odds = h.get("odds") or h.get("tansho_odds")
        if odds is not None and odds >= 30.0 and raw_adj > 0:
            raw_adj *= 0.3
        elif odds is not None and odds >= 15.0 and raw_adj > 0:
            raw_adj *= 0.5

        h["ml_composite_adj"] = round(raw_adj, 4)
        h["ml_win_prob"] = round(p, 6)


def softmax_win_probs(horses):
    """composite ベースの softmax で win_prob を再計算"""
    active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]
    if not active:
        return
    composites = [h.get("composite", 50.0) for h in active]
    max_c = max(composites)
    exps = [math.exp((c - max_c) / 10.0) for c in composites]
    total = sum(exps)
    if total <= 0:
        return
    probs = [e / total for e in exps]
    active_map = {}
    for h, p in zip(active, probs):
        active_map[h.get("horse_no", -1)] = p
    for h in horses:
        hno = h.get("horse_no", -1)
        if hno in active_map:
            h["win_prob"] = round(active_map[hno], 6)
        elif h.get("is_scratched") or h.get("scrape_failed"):
            h["win_prob"] = 0.0


def reassign_marks(horses):
    """composite 順位で印を再割り当て"""
    active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]
    active.sort(key=lambda h: h.get("composite", 0), reverse=True)
    for h in horses:
        h["mark"] = ""
    for i, h in enumerate(active):
        if i < len(MARK_ORDER):
            h["mark"] = MARK_ORDER[i]


def regenerate_tickets(horses, confidence):
    """三連複チケット再生成 (patch_pred_walk_forward.py と同等)"""
    mark_to_no = {}
    for h in horses:
        m = h.get("mark", "")
        if m and m not in ("", "-", "－", "×"):
            mark_to_no[m] = h.get("horse_no")

    pivot_no = mark_to_no.get("◎")
    if pivot_no is None:
        return []

    taikou_no = mark_to_no.get("○")
    tannuke_no = mark_to_no.get("▲")
    rendashi_no = mark_to_no.get("△")
    rendashi2_no = mark_to_no.get("★")
    ana_no = mark_to_no.get("☆")

    partners = [n for n in [taikou_no, tannuke_no, rendashi_no, rendashi2_no, ana_no] if n is not None]
    if len(partners) < 2:
        return []

    tickets = []
    if confidence == "SS":
        if taikou_no is None:
            return []
        thirds = [n for n in [tannuke_no, rendashi_no, rendashi2_no, ana_no] if n is not None]
        for t_no in thirds:
            combo = sorted([pivot_no, taikou_no, t_no])
            tickets.append({"type": "三連複", "combo": combo, "pattern": "M'-E", "stake": 100})
    elif confidence in ("S", "A"):
        seconds = [n for n in [taikou_no, tannuke_no] if n is not None]
        all_thirds = [n for n in [taikou_no, tannuke_no, rendashi_no, rendashi2_no, ana_no] if n is not None]
        seen = set()
        for s_no in seconds:
            for t_no in all_thirds:
                if t_no == pivot_no or t_no == s_no:
                    continue
                combo = tuple(sorted([pivot_no, s_no, t_no]))
                if combo in seen:
                    continue
                seen.add(combo)
                tickets.append({"type": "三連複", "combo": list(combo), "pattern": "M'-C", "stake": 100})
    else:
        for p1, p2 in combinations(partners, 2):
            combo = sorted([pivot_no, p1, p2])
            tickets.append({"type": "三連複", "combo": combo, "pattern": "M'-D", "stake": 100})

    return tickets


def load_pop_stats_for_period(wf_name: str) -> Optional[dict]:
    """期間別 popularity_rates をロード

    build_popularity_stats_wf.py が生成した
    data/popularity_rates_{wf_name}.json をロードして返す。
    ファイルが存在しない場合は None を返す。
    """
    data_dir = os.path.join(PROJECT_ROOT, "data")
    path = os.path.join(data_dir, f"popularity_rates_{wf_name}.json")
    if not os.path.exists(path):
        print(f"  WARNING: 期間別 popularity stats が見つかりません: {path}")
        print(f"  先に python scripts/build_popularity_stats_wf.py を実行してください。")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            stats = json.load(f)
        total = stats.get("total_entries", 0)
        sample_days = stats.get("sample_days", 0)
        if total == 0:
            print(f"  WARNING: {path} の total_entries=0 (データなし)")
            print(f"  popularity_blend は適用しません (train_max 以前の pred.json が存在しない)。")
            return None
        print(f"  期間別 popularity stats ロード: {path}")
        print(f"    sample_days={sample_days}, total_entries={total:,}")
        return stats
    except Exception as e:
        print(f"  ERROR: 期間別 popularity stats ロード失敗: {e}")
        return None


def update_pred_file(fpath, race_updates, dry_run=False, pop_stats=None):
    """pred.json を更新: ml_composite_adj → composite 再計算 → marks → tickets

    Args:
        fpath: pred.json のパス
        race_updates: {race_id: {horse_id: ML prob}} の dict
        dry_run: True の場合は書き込みしない
        pop_stats: 期間別 popularity_rates dict。指定時は softmax 後に
                   popularity_blend を適用する。None の場合はスキップ。
    """
    with open(fpath, "r", encoding="utf-8") as f:
        data = json.load(f)

    updated = 0
    for race in data.get("races", []):
        race_id = race.get("race_id", "")
        if race_id not in race_updates:
            continue

        ml_probs = race_updates[race_id]

        # ml_composite_adj 計算
        calc_ml_composite_adj(race.get("horses", []), ml_probs)

        # composite に ml_composite_adj を加算
        for h in race.get("horses", []):
            adj = h.get("ml_composite_adj", 0.0) or 0.0
            old_comp = h.get("composite", 50.0)
            # 旧 adj は既に 0 なので、新 adj をそのまま加算
            h["composite"] = round(old_comp + adj, 4)

        # marks 再割当
        reassign_marks(race.get("horses", []))

        # win_prob 再計算 (softmax: composite ベース)
        softmax_win_probs(race.get("horses", []))

        # 期間別 popularity_blend を適用 (L-2 リーク修正)
        # softmax_win_probs() で win_prob を確定した後に blend する
        if pop_stats is not None:
            _apply_popularity_blend_wf(race, pop_stats)

        # tickets 再生成
        confidence = race.get("overall_confidence", "") or race.get("confidence", "B") or "B"
        new_tickets = regenerate_tickets(race.get("horses", []), confidence)
        race["tickets"] = new_tickets
        race["formation_tickets"] = []

        pat = PATTERN_MAP.get(confidence, "D")
        race["tickets_by_mode"] = {
            "fixed": new_tickets,
            "accuracy": [],
            "balanced": [],
            "recovery": [],
            "_meta": {
                "format": M_PRIME_FORMAT,
                "confidence": confidence,
                "pattern": pat,
                "skipped": pat == "skip",
                "skip_reason": "E rank" if pat == "skip" else "",
                "race_ev_ratio": 0.0,
            },
        }

        updated += 1

    if updated > 0 and not dry_run:
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    return updated


_WF_VENUE_NAMES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
    "30": "門別", "31": "帯広", "35": "盛岡", "36": "水沢", "42": "浦和",
    "43": "船橋", "44": "大井", "45": "川崎", "46": "金沢", "47": "笠松",
    "48": "名古屋", "50": "園田", "51": "姫路", "54": "高知", "55": "佐賀",
}


def _apply_popularity_blend_wf(race: dict, pop_stats: dict) -> None:
    """WF backtest 用の popularity_blend を race dict に適用 (in-place)

    DISABLE_POPULARITY_BLEND フラグに関わらず WF 専用の統計テーブルで
    win_prob / place2_prob / place3_prob を補正する。

    適用手順:
      1. softmax_win_probs() 後に呼ぶこと (win_prob 正規化済みが前提)
      2. ml_win_prob (P(top3)) から place2/3 を補完
      3. _lookup_rates() で統計レート取得
      4. 動的 alpha でブレンド
      5. 正規化

    popularity フィールドが存在しない馬はスキップされる。
    """
    from src.calculator.popularity_blend import (
        _lookup_rates,
        _normalize_dict_probs,
        ALPHA_MODEL_MIN,
        ALPHA_MODEL_MAX,
        CONFIDENCE_GAP,
    )

    horses = race.get("horses", [])
    active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]
    if not active:
        return

    # race 情報を取得
    race_id = race.get("race_id", "")
    venue_code = race_id[4:6] if len(race_id) >= 6 else "01"
    vc_int = int(venue_code) if venue_code.isdigit() else 0
    is_jra = 1 <= vc_int <= 10
    venue_name = _WF_VENUE_NAMES.get(venue_code, venue_code)
    org = "JRA" if is_jra else "NAR"
    field_count = len(active)

    # place2_prob / place3_prob を ml_win_prob から補完
    # (softmax_win_probs は win_prob のみ設定するため)
    for h in active:
        ml_p3 = h.get("ml_win_prob", 0.0) or 0.0  # P(top3) = ML の生確率
        wp = h.get("win_prob", 0.0) or 0.0
        # place2/3 が未設定の場合のみ補完
        if not h.get("place2_prob"):
            # 3着内確率から2着内確率を近似 (比率 0.75)
            h["place2_prob"] = min(1.0, ml_p3 * 0.75) if ml_p3 > 0 else min(1.0, wp * 2.0)
        if not h.get("place3_prob"):
            # 3着内確率をそのまま使用
            h["place3_prob"] = ml_p3 if ml_p3 > 0 else min(1.0, wp * 3.0)

    # 動的 alpha 計算 (blend_probabilities_dict と同等)
    all_wp = sorted([h.get("win_prob", 0) for h in active], reverse=True)
    gap = (all_wp[0] - all_wp[1]) if len(all_wp) >= 2 else 0
    confidence = min(1.0, gap / CONFIDENCE_GAP)
    alpha_model = ALPHA_MODEL_MIN + confidence * (ALPHA_MODEL_MAX - ALPHA_MODEL_MIN)
    alpha_stats = 1.0 - alpha_model
    alpha_org = alpha_stats * 0.4
    alpha_venue = alpha_stats * 0.6

    # 各馬にブレンドを適用 (DISABLE_POPULARITY_BLEND を無視して強制実行)
    blended = 0
    for h in active:
        pop = h.get("popularity")
        if pop is None or pop < 1:
            continue

        odds = h.get("odds")

        org_win, org_top2, org_top3, ven_win, ven_top2, ven_top3 = _lookup_rates(
            pop_stats, org, venue_name, pop, odds, field_count
        )

        h["win_prob"] = (
            alpha_model * h.get("win_prob", 1.0 / field_count)
            + alpha_org * org_win
            + alpha_venue * ven_win
        )
        h["place2_prob"] = (
            alpha_model * h.get("place2_prob", 2.0 / field_count)
            + alpha_org * org_top2
            + alpha_venue * ven_top2
        )
        h["place3_prob"] = (
            alpha_model * h.get("place3_prob", 3.0 / field_count)
            + alpha_org * org_top3
            + alpha_venue * ven_top3
        )
        blended += 1

    # 正規化
    if blended > 0:
        _normalize_dict_probs(active, field_count)


def load_ml_races_for_inference(start_date: str, end_date: str):
    """推論期間の ML JSON をロード (tracker 更新用)"""
    files = sorted(glob(os.path.join(ML_DATA_DIR, "*.json")))
    all_races = []
    for fpath in files:
        fname = os.path.basename(fpath)
        if not fname[:8].isdigit():
            continue
        if not (start_date <= fname[:8] <= end_date):
            continue
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            for r in data.get("races", data if isinstance(data, list) else []):
                all_races.append(r)
        except Exception:
            continue
    return all_races


def run_wf_period(period_name: str, config: dict, dry_run: bool = False):
    """1つの WF 期間の推論を実行"""
    model_dir = config["model_dir"]
    train_max = config["train_max"]
    infer_start = config["infer_start"]
    infer_end = config["infer_end"]

    print(f"\n{'='*60}")
    print(f"WF Period: {period_name}")
    print(f"  model_dir: {model_dir}")
    print(f"  train_max: {train_max}")
    print(f"  inference: {infer_start} - {infer_end}")
    print(f"{'='*60}")

    if not os.path.exists(model_dir):
        print(f"  ERROR: model_dir not found: {model_dir}")
        return {"races": 0, "horses": 0, "files": 0}

    # Step 1: rolling stats 構築
    t0 = time.time()
    print(f"\n[Step 1] Rolling stats 構築 (max_date={train_max})...")
    tracker, sire_tracker, sire_map = build_tracker(train_max)

    # stats を WF model_dir に保存
    stats_path = os.path.join(model_dir, "rolling_stats.pkl")
    sire_stats_path = os.path.join(model_dir, "sire_rolling_stats.pkl")
    sire_map_path = os.path.join(model_dir, "horse_sire_map.pkl")
    if not dry_run:
        with open(stats_path, "wb") as f:
            pickle.dump(tracker, f)
        with open(sire_stats_path, "wb") as f:
            pickle.dump(sire_tracker, f)
        with open(sire_map_path, "wb") as f:
            pickle.dump(sire_map, f)
        print(f"  stats 保存: {stats_path}")
    print(f"  Step 1 完了: {time.time()-t0:.1f}s")

    # Step 2: WF モデルロード
    t1 = time.time()
    print(f"\n[Step 2] WF model ロード...")

    # Step 2.5: Isotonic calibrators ロード (build_iso_calibrator_wf.py が事前に生成)
    print(f"\n[Step 2.5] Isotonic calibrators ロード...")
    iso_calibrators = load_iso_calibrators(model_dir)
    if iso_calibrators:
        print(f"  Isotonic calibration 有効: {list(iso_calibrators.keys())}")
    else:
        print(f"  Isotonic calibration 無効 (calibrator なし)")

    predict_fn = load_wf_predictor(model_dir, tracker, sire_tracker, iso_calibrators)
    print(f"  Step 2 完了: {time.time()-t1:.1f}s")

    # Step 3: 推論期間の ML JSON ロード (日付順)
    t2 = time.time()
    print(f"\n[Step 3] ML JSON ロード ({infer_start}-{infer_end})...")
    infer_races = load_ml_races_for_inference(infer_start, infer_end)
    infer_races.sort(key=lambda r: (r.get("date", ""), r.get("race_id", "")))
    print(f"  {len(infer_races)} races loaded")

    # 日付→race_id→ML prob のマップ構築
    # 同時に tracker を更新 (推論期間のレース結果で rolling stats を更新)
    date_race_probs = {}  # {date_str: {race_id: {horse_id: prob}}}
    total_horses = 0
    total_predicted = 0

    for i, race in enumerate(infer_races):
        race_id = race.get("race_id", "")
        date_str = race.get("date", "")
        horses = race.get("horses", [])

        # predict_race 用の辞書構築
        race_dict = {
            "race_id": race_id,
            "date": date_str,
            "venue": race.get("venue", ""),
            "venue_code": str(race.get("venue_code", "") or "").zfill(2),
            "surface": race.get("surface", ""),
            "distance": race.get("distance", 0),
            "condition": race.get("condition", ""),
            "field_count": race.get("field_count", len(horses)),
            "is_jra": race.get("is_jra", True),
            "grade": race.get("grade", ""),
        }

        horse_dicts = []
        for h in horses:
            sid, bid = sire_map.get(h.get("horse_id", ""), ("", ""))
            horse_dicts.append({
                "horse_id": h.get("horse_id", ""),
                "horse_name": h.get("horse_name", ""),
                "jockey_id": h.get("jockey_id", h.get("jockey", "")),
                "trainer_id": h.get("trainer_id", h.get("trainer", "")),
                "gate_no": h.get("gate_no", 0),
                "horse_no": h.get("horse_no", 0),
                "sex": h.get("sex", ""),
                "age": h.get("age", 0),
                "weight_kg": h.get("weight_kg", 0.0),
                "horse_weight": h.get("horse_weight", 0),
                "weight_change": h.get("weight_change", 0),
                "sire_id": sid,
                "bms_id": bid,
            })

        total_horses += len(horse_dicts)

        # ML 推論
        try:
            probs = predict_fn(race_dict, horse_dicts)
            if probs:
                fname_date = date_str.replace("-", "")
                if fname_date not in date_race_probs:
                    date_race_probs[fname_date] = {}
                date_race_probs[fname_date][race_id] = probs
                total_predicted += 1
        except Exception as e:
            if i < 5:
                print(f"  WARNING predict failed [{race_id}]: {e}")

        # tracker 更新 (次レースの rolling stats に反映)
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

        if (i + 1) % 2000 == 0 or i == len(infer_races) - 1:
            pct = (i + 1) / len(infer_races) * 100
            elapsed = time.time() - t2
            print(f"  [{i+1}/{len(infer_races)}] {pct:.0f}% - predicted={total_predicted}, "
                  f"horses={total_horses}, elapsed={elapsed:.0f}s")

    print(f"  Step 3 完了: {total_predicted} races predicted, {time.time()-t2:.1f}s")

    # Step 3.5: 期間別 popularity_blend stats のロード (L-2 リーク修正)
    print(f"\n[Step 3.5] 期間別 popularity stats ロード ({period_name})...")
    pop_stats = load_pop_stats_for_period(period_name)
    if pop_stats is None:
        print(f"  popularity_blend をスキップします (stats が利用不可)。")
        print(f"  先に `python scripts/build_popularity_stats_wf.py --period {period_name}` を実行してください。")
    else:
        print(f"  popularity_blend 有効: {pop_stats.get('total_entries', 0):,} entries の統計を使用")

    # Step 4: pred.json 更新
    t3 = time.time()
    print(f"\n[Step 4] pred.json 更新...")
    files_updated = 0
    races_updated = 0

    pred_files = sorted(glob(os.path.join(PREDICTIONS_DIR, "*_pred.json")))
    for fpath in pred_files:
        fname = os.path.basename(fpath)
        date_key = fname[:8]
        if date_key not in date_race_probs:
            continue

        race_updates = date_race_probs[date_key]
        n = update_pred_file(fpath, race_updates, dry_run=dry_run, pop_stats=pop_stats)
        if n > 0:
            files_updated += 1
            races_updated += n

    print(f"  Step 4 完了: {files_updated} files, {races_updated} races updated, "
          f"{time.time()-t3:.1f}s")

    total_time = time.time() - t0
    print(f"\n{period_name} 完了: {total_time:.1f}s "
          f"(predicted={total_predicted}, updated={races_updated})")

    return {
        "races_predicted": total_predicted,
        "races_updated": races_updated,
        "files_updated": files_updated,
        "horses": total_horses,
        "time": total_time,
    }


def main():
    parser = argparse.ArgumentParser(description="Walk-Forward ML 推論")
    parser.add_argument("--period", choices=["wf_2024", "wf_2025", "wf_2026", "all"],
                        default="all", help="推論期間 (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="書き込みなし")
    args = parser.parse_args()

    periods = WF_PERIODS if args.period == "all" else {args.period: WF_PERIODS[args.period]}
    total_start = time.time()

    results = {}
    for name, config in periods.items():
        r = run_wf_period(name, config, dry_run=args.dry_run)
        results[name] = r

    print(f"\n{'='*60}")
    print(f"全 WF 推論完了: {time.time()-total_start:.1f}s")
    for name, r in results.items():
        print(f"  {name}: predicted={r['races_predicted']}, updated={r['races_updated']}, "
              f"files={r['files_updated']}, time={r['time']:.0f}s")
    if args.dry_run:
        print("(dry-run: 書き込みなし)")


if __name__ == "__main__":
    main()
