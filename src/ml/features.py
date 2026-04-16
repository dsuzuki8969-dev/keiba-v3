"""
ML特徴量エンジニアリング

収集済み結果データ (data/ml/*.json) から、
LightGBM 学習用の特徴量行列 + ラベルを構築する。

設計方針:
  - オッズ・人気は特徴量から除外（データとしては保持）
  - 各馬の「レース前に知り得た情報」のみを特徴量にする
  - 過去走の集計は時系列を厳密に守る（未来情報リーク防止）
  - コース構造4因子 + 競馬場類似度重み付き実績を含む
"""

import json
import os
from datetime import datetime
from typing import Dict, Optional

import numpy as np
import pandas as pd

ML_DATA_DIR = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")),
    "data",
    "ml",
)

# 芝/ダート → 数値
SURFACE_MAP = {"芝": 0, "ダート": 1, "障害": 2}
# 馬場状態 → 数値（重いほど大きい）
CONDITION_MAP = {"良": 0, "稍": 1, "稍重": 1, "重": 2, "不良": 3}
# 性別 → 数値
SEX_MAP = {"牡": 0, "牝": 1, "セ": 2}
# ペース → 数値
PACE_MAP = {"S": -2, "MS": -1, "M": 0, "MH": 1, "H": 2, "SS": -2, "HH": 2, "MM": 0}

from data.masters.venue_master import is_banei

# 類似度特徴量のチューニング用パラメータ
VENUE_SIM_THRESHOLD = 0.35
DIRECTION_DISCOUNT = 0.75
SIMILARITY_POWER = 2.0
# Step7: 距離考慮 + 時間減衰パラメータ
VENUE_SIM_DIST_THRESHOLDS = [(200, 1.0), (400, 0.7)]  # (差m, 係数), それ以上は0.4
VENUE_SIM_HALF_LIFE_DAYS = 365  # 半減期365日


def load_all_races(start_date: str = None, end_date: str = None) -> list:
    """日別JSONを全読み込みしてレースのフラットリストを返す"""
    if not os.path.exists(ML_DATA_DIR):
        return []

    files = sorted(f for f in os.listdir(ML_DATA_DIR) if f.endswith(".json") and not f.startswith("_"))

    if start_date:
        files = [f for f in files if f.replace(".json", "") >= start_date.replace("-", "")]
    if end_date:
        files = [f for f in files if f.replace(".json", "") <= end_date.replace("-", "")]

    all_races = []
    for fname in files:
        fpath = os.path.join(ML_DATA_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            all_races.extend(data.get("races", []))
        except Exception:
            continue
    return all_races


def _build_horse_history(all_races: list) -> dict:
    """
    全レースを日付順に走査し、各馬の過去走履歴を構築する。
    Returns: {horse_id: [(race_date, race_dict, horse_dict), ...]}  日付昇順
    """
    history = {}
    sorted_races = sorted(all_races, key=lambda r: r.get("date", ""))
    for race in sorted_races:
        race_date = race.get("date", "")
        for h in race.get("horses", []):
            hid = h.get("horse_id", "")
            if not hid:
                continue
            if hid not in history:
                history[hid] = []
            history[hid].append((race_date, race, h))
    return history


def build_dataset(
    all_races: list = None,
    start_date: str = None,
    end_date: str = None,
    min_past_runs: int = 1,
    banei_only: bool = False,
) -> pd.DataFrame:
    """
    全レースデータから特徴量行列を構築する。

    Args:
        all_races: レースリスト（Noneなら自動読込）
        start_date: 学習対象の開始日
        end_date: 学習対象の終了日
        min_past_runs: 最低過去走数（これ未満の馬はスキップ）
        banei_only: Trueならばんえい(帯広)レースのみ抽出

    Returns:
        DataFrame（1行=1出走、特徴量 + ラベル列を含む）
    """
    if all_races is None:
        all_races = load_all_races(start_date, end_date)

    if not all_races:
        return pd.DataFrame()

    # ばんえい専用モード: venue_code="65" のみ抽出
    if banei_only:
        all_races = [r for r in all_races if is_banei(r.get("venue_code", ""))]
        if not all_races:
            print("  ばんえいレースが見つかりません")
            return pd.DataFrame()

    print(f"  レース数: {len(all_races)}")

    history = _build_horse_history(all_races)
    print(f"  ユニーク馬数: {len(history)}")

    # 調教特徴量抽出器をロード
    training_extractor = None
    try:
        from src.ml.training_features import TrainingFeatureExtractor
        training_extractor = TrainingFeatureExtractor()
        training_extractor.load_all()
    except Exception:
        import logging
        logging.getLogger(__name__).warning("調教特徴量ロード失敗（スキップ）", exc_info=True)

    rows = []
    sorted_races = sorted(all_races, key=lambda r: r.get("date", ""))

    for race in sorted_races:
        race_date = race.get("date", "")
        if start_date and race_date < start_date:
            continue
        if end_date and race_date > end_date:
            continue

        race_features = _extract_race_features(race)
        race_id = race.get("race_id", "")

        # 調教特徴量をレース単位で一括取得（レース内相対値計算のため）
        train_feats_map = {}
        if training_extractor:
            horse_names = [
                h.get("horse_name", "") for h in race.get("horses", [])
                if h.get("horse_name")
            ]
            if horse_names:
                train_feats_map = training_extractor.get_race_training_features(
                    race_id, horse_names, race_date
                )

        for h in race.get("horses", []):
            if h.get("finish_pos") is None:
                continue

            hid = h.get("horse_id", "")
            past = _get_past_runs_before(history.get(hid, []), race_date)
            if len(past) < min_past_runs:
                continue

            horse_features = _extract_horse_features(h, race)
            past_features = _extract_past_run_features(past, race, h)
            label = _extract_label(h)

            # 調教特徴量
            hname = h.get("horse_name", "")
            train_feats = train_feats_map.get(hname, {})

            row = {**race_features, **horse_features, **past_features, **train_feats, **label}
            row["race_id"] = race_id
            row["date"] = race_date
            row["horse_id"] = hid
            rows.append(row)

    df = pd.DataFrame(rows)
    print(f"  有効サンプル数: {len(df)}")
    return df


_SLOPE_SCORE = {"急坂": 1.0, "軽坂": 0.5, "坂なし": 0.0}
_CORNER_SCORE = {"大回り": 1.0, "スパイラル": 0.5, "小回り": 0.0}
_DIRECTION_SCORE = {"右": 0, "左": 1, "両": 2}

_venue_profile_cache: Dict = {}


def _get_venue_profile(venue_name: str):
    """VenueProfile をキャッシュ付きで取得"""
    if not _venue_profile_cache:
        try:
            from data.masters.venue_similarity import get_all_profiles
            _venue_profile_cache.update(get_all_profiles())
        except Exception:
            pass
    return _venue_profile_cache.get(venue_name)


def _extract_race_features(race: dict) -> dict:
    """レースレベルの特徴量（コース構造4因子 + 回り方向を含む）"""
    venue_name = race.get("venue", "")
    profile = _get_venue_profile(venue_name)

    base = {
        "venue_code": race.get("venue_code", ""),
        "venue_name": venue_name,
        "surface": SURFACE_MAP.get(race.get("surface", ""), 0),
        "distance": race.get("distance", 1600),
        "condition": CONDITION_MAP.get(race.get("condition", "良"), 0),
        "field_count": race.get("field_count", 12),
        "is_jra": int(race.get("is_jra", True)),
        "race_first_3f": race.get("first_3f"),
    }

    _is_banei = is_banei(race.get("venue_code", ""))

    if _is_banei:
        # ばんえい: コース構造は固定値（帯広200m直線・坂あり）
        base["venue_straight_m"] = 200.0
        base["venue_slope"] = 1.0  # 坂あり
        base["venue_first_corner"] = 0.0
        base["venue_corner_type"] = 0.0
        base["venue_direction"] = 3  # 直線（ばんえい固有コード）
        # ばんえい固有: 水分量
        base["water_content"] = race.get("water_content")
    elif profile:
        base["venue_straight_m"] = profile.avg_straight_m
        base["venue_slope"] = _SLOPE_SCORE.get(profile.slope_type, 0.0)
        base["venue_first_corner"] = profile.first_corner_score
        base["venue_corner_type"] = _CORNER_SCORE.get(profile.corner_type_dominant, 0.5)
        base["venue_direction"] = _DIRECTION_SCORE.get(profile.direction, 2)
    else:
        base["venue_straight_m"] = None
        base["venue_slope"] = None
        base["venue_first_corner"] = None
        base["venue_corner_type"] = None
        base["venue_direction"] = None

    # first_corner_m: コース別の初角距離（実距離m）を特徴量として追加
    _fcm = 0
    try:
        from data.masters.course_master import get_all_courses
        _vc = race.get("venue_code", "")
        _sf = race.get("surface", "")
        _dist = race.get("distance", 0)
        _cid = f"{_vc}_{_sf}_{_dist}"
        _cm = get_all_courses().get(_cid)
        if _cm and _cm.first_corner_m > 0:
            _fcm = _cm.first_corner_m
    except Exception:
        pass
    base["first_corner_m"] = _fcm

    return base


def _extract_horse_features(h: dict, race: dict) -> dict:
    """馬個体の特徴量（レース前に知り得る情報）"""
    sex = SEX_MAP.get(h.get("sex", ""), 0)
    age = h.get("age") or 3
    gate_no = h.get("gate_no") or 1
    horse_no = h.get("horse_no") or 1
    weight_kg = h.get("weight_kg") or 55.0
    horse_weight = h.get("horse_weight")
    weight_change = h.get("weight_change")
    field_count = race.get("field_count", 12)

    result = {
        "sex": sex,
        "age": age,
        "gate_no": gate_no,
        "horse_no": horse_no,
        "weight_kg": weight_kg,
        "horse_weight": horse_weight,
        "weight_change": weight_change,
        "gate_relative": gate_no / max(field_count, 1),
    }
    # ばんえい固有: 負担重量/馬体重比（重いほど不利）
    if horse_weight and horse_weight > 0:
        result["weight_kg_ratio"] = weight_kg / horse_weight
    else:
        result["weight_kg_ratio"] = None
    return result


def _get_past_runs_before(history: list, current_date: str) -> list:
    """current_date より前の出走履歴を返す（未来リーク防止）"""
    return [(d, r, h) for d, r, h in history if d < current_date]


def _extract_past_run_features(past: list, current_race: dict, horse: dict = None) -> dict:
    """過去走から集計した特徴量"""
    if not past:
        return _empty_past_features()

    recent = past[-5:]  # 直近5走
    current_surface = current_race.get("surface", "")
    current_distance = current_race.get("distance", 1600)

    # 基本集計（取消・除外=着順90以上を除外）
    finish_positions = [h.get("finish_pos") for _, _, h in recent if h.get("finish_pos") and h.get("finish_pos") < 90]
    last3fs = [h.get("last_3f_sec") for _, _, h in recent if h.get("last_3f_sec")]
    corners = []
    for _, _, h in recent:
        pc = h.get("positions_corners", [])
        if pc:
            corners.append(pc[-1])

    # 勝率・複勝率
    n_runs = len(finish_positions)
    wins = sum(1 for p in finish_positions if p == 1)
    top3 = sum(1 for p in finish_positions if p <= 3)
    win_rate = wins / n_runs if n_runs else 0
    place_rate = top3 / n_runs if n_runs else 0

    # 平均着順
    avg_finish = np.mean(finish_positions) if finish_positions else 8.0
    best_finish = min(finish_positions) if finish_positions else 18

    # 上がり3F
    avg_last3f = np.mean(last3fs) if last3fs else None
    best_last3f = min(last3fs) if last3fs else None

    # 平均4角位置
    avg_pos4c = np.mean(corners) if corners else None

    # 距離ロス（コーナーロス）特徴量
    _SEC_PER_RANK_TURF = 0.08   # 芝M相当
    _SEC_PER_RANK_DIRT = 0.30   # ダートM相当
    _outer_ratios = []
    _corner_loss_secs = []
    _pos_spreads = []
    for _, r, h in recent:
        pc = h.get("positions_corners", [])
        fc = r.get("field_count", 0)
        if not pc or not isinstance(pc, list) or not fc or fc <= 1:
            continue
        valid_c = [c for c in pc if isinstance(c, (int, float)) and c > 0]
        if not valid_c:
            continue
        avg_rel = sum(c / fc for c in valid_c) / len(valid_c)
        _outer_ratios.append(avg_rel)
        # 推定ロス秒
        surf = r.get("surface", "")
        spr = _SEC_PER_RANK_DIRT if surf == "ダート" else _SEC_PER_RANK_TURF
        loss = (avg_rel - 0.5) * len(valid_c) * spr * fc
        _corner_loss_secs.append(loss)
        # 変動幅
        _pos_spreads.append(max(valid_c) - min(valid_c))
    past_avg_outer_ratio = np.mean(_outer_ratios) if _outer_ratios else None
    past_outer_ratio_last = _outer_ratios[-1] if _outer_ratios else None
    past_corner_loss_sec_avg = np.mean(_corner_loss_secs) if _corner_loss_secs else None
    past_corner_loss_sec_last = _corner_loss_secs[-1] if _corner_loss_secs else None
    past_pos_spread = np.mean(_pos_spreads) if _pos_spreads else None

    # 間隔日数（直近走からの日数）
    last_date_str = past[-1][0]
    current_date_str = current_race.get("date", "")
    days_since = _days_between(last_date_str, current_date_str)

    # 同面（芝/ダート）実績
    same_surface = [(d, r, h) for d, r, h in past if r.get("surface") == current_surface]
    ss_positions = [h.get("finish_pos") for _, _, h in same_surface if h.get("finish_pos")]
    same_surface_rate = (sum(1 for p in ss_positions if p <= 3) / len(ss_positions)) if ss_positions else None
    same_surface_runs = len(ss_positions)

    # 同距離帯（±200m）実績
    near_dist = [(d, r, h) for d, r, h in past
                 if abs(r.get("distance", 0) - current_distance) <= 200]
    nd_positions = [h.get("finish_pos") for _, _, h in near_dist if h.get("finish_pos")]
    near_dist_rate = (sum(1 for p in nd_positions if p <= 3) / len(nd_positions)) if nd_positions else None
    near_dist_runs = len(nd_positions)

    # 走破タイム偏差（距離で正規化した秒/200m）
    time_per_200 = []
    for _, r, h in recent:
        t = h.get("finish_time_sec")
        d = r.get("distance", 1600)
        if t and t > 0 and d > 0:
            time_per_200.append(t / d * 200)
    avg_time_per_200 = np.mean(time_per_200) if time_per_200 else None

    # トレンド（直近3走の着順が改善傾向か悪化傾向か）
    trend = 0.0
    if len(finish_positions) >= 3:
        r3 = finish_positions[-3:]
        trend = r3[0] - r3[-1]  # 正 = 改善（着順下がり）

    # 馬場状態別実績
    current_cond = CONDITION_MAP.get(current_race.get("condition", "良"), 0)
    heavy_runs = [(d, r, h) for d, r, h in past if CONDITION_MAP.get(r.get("condition", "良"), 0) >= 2]
    heavy_positions = [h.get("finish_pos") for _, _, h in heavy_runs if h.get("finish_pos")]
    heavy_place_rate = (sum(1 for p in heavy_positions if p <= 3) / len(heavy_positions)) if heavy_positions else None

    # 人気バイアス特徴量: 過去走の人気 vs 着順の乖離
    popularity_gap_list = []
    for _, r, h in recent:
        pop = h.get("popularity")
        fp_val = h.get("finish_pos")
        if pop and fp_val:
            popularity_gap_list.append(pop - fp_val)
    avg_popularity_gap = np.mean(popularity_gap_list) if popularity_gap_list else None

    # 前走着順と前走人気の差（穴馬検出に有効）
    last_run = recent[-1] if recent else None
    last_pop_gap = None
    if last_run:
        _, _, lh = last_run
        lp = lh.get("popularity")
        lfp = lh.get("finish_pos")
        if lp and lfp:
            last_pop_gap = lp - lfp

    # ── 競馬場類似度重み付き実績 ──
    venue_sim_feats = _extract_venue_similarity_features(past, current_race)

    result = {
        "past_runs": n_runs,
        "past_win_rate": win_rate,
        "past_place_rate": place_rate,
        "past_avg_finish": avg_finish,
        "past_best_finish": best_finish,
        "past_avg_last3f": avg_last3f,
        "past_best_last3f": best_last3f,
        "past_avg_pos4c": avg_pos4c,
        # 距離ロス（コーナーロス）特徴量
        "past_avg_outer_ratio": past_avg_outer_ratio,
        "past_outer_ratio_last": past_outer_ratio_last,
        "past_corner_loss_sec_avg": past_corner_loss_sec_avg,
        "past_corner_loss_sec_last": past_corner_loss_sec_last,
        "past_pos_spread": past_pos_spread,
        "days_since_last": days_since,
        "same_surface_place_rate": same_surface_rate,
        "same_surface_runs": same_surface_runs,
        "near_dist_place_rate": near_dist_rate,
        "near_dist_runs": near_dist_runs,
        "past_avg_time_per_200": avg_time_per_200,
        "past_trend": trend,
        "heavy_track_place_rate": heavy_place_rate,
        "current_condition": current_cond,
        "avg_popularity_gap": avg_popularity_gap,
        "last_popularity_gap": last_pop_gap,
        **venue_sim_feats,
    }

    # ── ばんえい固有特徴量 ──
    _is_banei_race = is_banei(current_race.get("venue_code", ""))
    if _is_banei_race:
        # 走破タイム系（ばんえいは200m直線のためタイムが重要指標）
        finish_times = [h.get("finish_time_sec") for _, _, h in recent if h.get("finish_time_sec")]
        result["past_avg_finish_time"] = np.mean(finish_times) if finish_times else None
        result["past_best_finish_time"] = min(finish_times) if finish_times else None
        if len(finish_times) >= 2 and np.mean(finish_times) > 0:
            result["time_consistency"] = np.std(finish_times) / np.mean(finish_times)
        else:
            result["time_consistency"] = None

        # 斤量トレンド（直近3走の斤量変化）
        weight_kgs = [h.get("weight_kg") for _, _, h in recent[-3:] if h.get("weight_kg")]
        if len(weight_kgs) >= 2:
            result["weight_trend"] = weight_kgs[-1] - weight_kgs[0]
        else:
            result["weight_trend"] = None

        # 高水分(2.0%超)時の複勝率
        heavy_water = [(d, r, h) for d, r, h in past if (r.get("water_content") or 0) >= 2.0]
        hw_pos = [h.get("finish_pos") for _, _, h in heavy_water if h.get("finish_pos")]
        result["heavy_water_rate"] = (sum(1 for p in hw_pos if p <= 3) / len(hw_pos)) if hw_pos else None

        # ---- Phase 5 追加: ばんえい固有特徴量強化 ----

        # 斤量×水分量の交互作用項（非線形関係の捕捉）
        cur_wt = current_race.get("weight_kg") or (horse or {}).get("weight_kg")
        cur_wc = current_race.get("water_content")
        if cur_wt and cur_wc is not None:
            result["weight_kg_x_water"] = cur_wt * cur_wc
        else:
            result["weight_kg_x_water"] = None

        # 斤量帯カテゴリ（5段階: 0=~580, 1=580-620, 2=620-660, 3=660-700, 4=700+）
        wk = cur_wt or (horse or {}).get("weight_kg")
        if wk:
            if wk < 580: result["weight_kg_band"] = 0
            elif wk < 620: result["weight_kg_band"] = 1
            elif wk < 660: result["weight_kg_band"] = 2
            elif wk < 700: result["weight_kg_band"] = 3
            else: result["weight_kg_band"] = 4
        else:
            result["weight_kg_band"] = None

        # 直近3走の出走間隔平均（ばんえいは連闘が多い）
        race_dates = [d for d, _, _ in recent[:3]]
        if len(race_dates) >= 2:
            intervals = []
            for i in range(len(race_dates) - 1):
                try:
                    d1 = datetime.strptime(race_dates[i], "%Y-%m-%d")
                    d2 = datetime.strptime(race_dates[i + 1], "%Y-%m-%d")
                    intervals.append(abs((d1 - d2).days))
                except (ValueError, TypeError):
                    pass
            result["recent_interval_avg"] = np.mean(intervals) if intervals else None
        else:
            result["recent_interval_avg"] = None

        # 直近走の着順/頭数比（ばんえいの形勢判断）
        if recent:
            _, _, last_h = recent[0]
            lp = last_h.get("finish_pos")
            lfc = last_h.get("field_count") or current_race.get("field_count")
            result["last_pos_ratio"] = lp / lfc if lp and lfc and lfc > 0 else None
        else:
            result["last_pos_ratio"] = None

    return result


def _extract_venue_similarity_features(past: list, current_race: dict) -> dict:
    """競馬場類似度で重み付けした過去走実績

    LightGBM に渡す特徴量:
      - venue_sim_place_rate:  類似場での重み付き複勝率
      - venue_sim_win_rate:    類似場での重み付き勝率
      - venue_sim_avg_finish:  類似場での重み付き平均着順
      - venue_sim_runs:        類似場での有効走数
      - venue_sim_n_venues:    貢献場数 (Solo=1, Pair=2, ...)
      - same_dir_place_rate:   同回り場での複勝率
      - same_dir_runs:         同回り場での走数
    """
    empty = {
        "venue_sim_place_rate": None,
        "venue_sim_win_rate": None,
        "venue_sim_avg_finish": None,
        "venue_sim_runs": 0,
        "venue_sim_n_venues": 0,
        "same_dir_place_rate": None,
        "same_dir_runs": 0,
    }

    target_venue = current_race.get("venue", "")
    target_surface = current_race.get("surface", "")
    target_distance = current_race.get("distance", 0)
    current_date = current_race.get("date", "")
    if not target_venue or not past:
        return empty

    try:
        from data.masters.venue_similarity import get_venue_similarity
    except ImportError:
        return empty

    target_profile = _get_venue_profile(target_venue)
    if not target_profile:
        return empty

    target_dir = target_profile.direction

    w_place_sum = 0.0
    w_win_sum = 0.0
    w_finish_sum = 0.0
    w_total = 0.0
    contributing_venues: set = set()

    same_dir_top3 = 0
    same_dir_n = 0

    for run_date, r, h in past:
        if r.get("surface") != target_surface:
            continue
        fp = h.get("finish_pos")
        if not fp:
            continue

        run_venue = r.get("venue", "")
        if not run_venue:
            continue

        sim = 1.0 if run_venue == target_venue else get_venue_similarity(target_venue, run_venue)
        if sim < VENUE_SIM_THRESHOLD:
            continue

        run_profile = _get_venue_profile(run_venue)
        run_dir = run_profile.direction if run_profile else "両"
        if target_dir == run_dir or "両" in (target_dir, run_dir):
            dir_factor = 1.0
            same_dir_n += 1
            if fp <= 3:
                same_dir_top3 += 1
        else:
            dir_factor = DIRECTION_DISCOUNT

        # Step7: 距離類似度（±200m=1.0, ±400m=0.7, それ以上=0.4）
        run_dist = r.get("distance", 0)
        dist_diff = abs(run_dist - target_distance) if run_dist and target_distance else 0
        dist_factor = 0.4  # デフォルト
        for threshold, factor in VENUE_SIM_DIST_THRESHOLDS:
            if dist_diff <= threshold:
                dist_factor = factor
                break

        # Step7: 時間減衰（半減期365日）
        recency_factor = 1.0
        if current_date and run_date and current_date > run_date:
            try:
                from datetime import datetime as _dt
                days_ago = (_dt.strptime(current_date, "%Y-%m-%d")
                            - _dt.strptime(run_date, "%Y-%m-%d")).days
                if days_ago > 0:
                    recency_factor = 0.5 ** (days_ago / VENUE_SIM_HALF_LIFE_DAYS)
            except Exception:
                pass

        weight = (sim ** SIMILARITY_POWER) * dir_factor * dist_factor * recency_factor

        w_place_sum += weight * (1 if fp <= 3 else 0)
        w_win_sum += weight * (1 if fp == 1 else 0)
        w_finish_sum += weight * fp
        w_total += weight
        contributing_venues.add(run_venue)

    if w_total == 0:
        return empty

    return {
        "venue_sim_place_rate": w_place_sum / w_total,
        "venue_sim_win_rate": w_win_sum / w_total,
        "venue_sim_avg_finish": w_finish_sum / w_total,
        "venue_sim_runs": int(w_total * 10) / 10,
        "venue_sim_n_venues": len(contributing_venues),
        "same_dir_place_rate": same_dir_top3 / same_dir_n if same_dir_n > 0 else None,
        "same_dir_runs": same_dir_n,
    }


def _empty_past_features() -> dict:
    return {
        "past_runs": 0,
        "past_win_rate": None,
        "past_place_rate": None,
        "past_avg_finish": None,
        "past_best_finish": None,
        "past_avg_last3f": None,
        "past_best_last3f": None,
        "past_avg_pos4c": None,
        # 距離ロス（コーナーロス）特徴量
        "past_avg_outer_ratio": None,
        "past_outer_ratio_last": None,
        "past_corner_loss_sec_avg": None,
        "past_corner_loss_sec_last": None,
        "past_pos_spread": None,
        "days_since_last": None,
        "same_surface_place_rate": None,
        "same_surface_runs": 0,
        "near_dist_place_rate": None,
        "near_dist_runs": 0,
        "past_avg_time_per_200": None,
        "past_trend": 0.0,
        "heavy_track_place_rate": None,
        "current_condition": 0,
        "avg_popularity_gap": None,
        "last_popularity_gap": None,
        "venue_sim_place_rate": None,
        "venue_sim_win_rate": None,
        "venue_sim_avg_finish": None,
        "venue_sim_runs": 0,
        "venue_sim_n_venues": 0,
        "same_dir_place_rate": None,
        "same_dir_runs": 0,
        # ばんえい固有（存在しない場合もNone）
        "past_avg_finish_time": None,
        "past_best_finish_time": None,
        "time_consistency": None,
        "weight_trend": None,
        "heavy_water_rate": None,
    }


def _days_between(d1: str, d2: str) -> Optional[int]:
    try:
        dt1 = datetime.strptime(d1, "%Y-%m-%d")
        dt2 = datetime.strptime(d2, "%Y-%m-%d")
        return (dt2 - dt1).days
    except Exception:
        return None


# 特徴量カラム一覧（学習時に使用、オッズ・人気は意図的に除外）
FEATURE_COLS = [
    # レース条件
    "surface", "distance", "condition", "field_count", "is_jra",
    # コース構造4因子 + 回り方向 + 初角実距離
    "venue_straight_m", "venue_slope", "venue_first_corner",
    "venue_corner_type", "venue_direction", "first_corner_m",
    # 馬個体
    "sex", "age", "gate_no", "weight_kg", "horse_weight", "weight_change",
    "gate_relative",
    # 過去走集計
    "past_runs", "past_win_rate", "past_place_rate",
    "past_avg_finish", "past_best_finish",
    "past_avg_last3f", "past_best_last3f",
    "past_avg_pos4c",
    # 距離ロス（コーナーロス）特徴量
    "past_avg_outer_ratio",
    "past_outer_ratio_last",
    "past_corner_loss_sec_avg",
    "past_corner_loss_sec_last",
    "past_pos_spread",
    "days_since_last",
    "same_surface_place_rate", "same_surface_runs",
    "near_dist_place_rate", "near_dist_runs",
    "past_avg_time_per_200",
    "past_trend",
    "heavy_track_place_rate", "current_condition",
    "avg_popularity_gap", "last_popularity_gap",
    # 競馬場類似度重み付き実績
    "venue_sim_place_rate", "venue_sim_win_rate", "venue_sim_avg_finish",
    "venue_sim_runs", "venue_sim_n_venues",
    "same_dir_place_rate", "same_dir_runs",
    # 調教特徴量 [24本]
    "train_final_4f", "train_final_3f_self_best_ratio",
    "train_final_3f_trend", "train_final_3f_rank_in_race",
    "train_final_3f_dev", "train_final_1f_dev", "train_final_1f_trend",
    "train_first1f_pace",
    "train_intensity_max", "train_3f_per_intensity",
    "train_efficiency_self_diff", "train_narinori_3f",
    "train_3f_trainer_dev", "train_1f_trainer_dev",
    "train_trainer_intensity_diff",
    "train_volume_self_diff", "train_intensity_pattern", "train_course_primary",
    "train_partner_margin", "train_partner_win_rate",
    "train_stable_mark", "train_comment_sentiment",
    "train_state_score", "train_readiness_index",
]

# ばんえい専用特徴量カラム（使えない特徴量を除外 + 固有特徴量を追加）
FEATURE_COLS_BANEI = [
    # レース条件（コース構造・venue_sim は不要: 帯広1場のみ）
    "condition", "field_count",
    # ばんえい固有: 馬場水分量
    "water_content",
    # 馬個体
    "sex", "age", "gate_no", "weight_kg", "horse_weight", "weight_change",
    "gate_relative",
    # ばんえい固有: 負担重量/馬体重比
    "weight_kg_ratio",
    # 過去走集計（last3f/corners/venue_sim 除外）
    "past_runs", "past_win_rate", "past_place_rate",
    "past_avg_finish", "past_best_finish",
    "days_since_last",
    "same_surface_place_rate", "same_surface_runs",
    "near_dist_place_rate", "near_dist_runs",
    "past_avg_time_per_200",
    "past_trend",
    "heavy_track_place_rate", "current_condition",
    "avg_popularity_gap", "last_popularity_gap",
    # ばんえい固有: 走破タイム系
    "past_avg_finish_time", "past_best_finish_time",
    "time_consistency",
    # ばんえい固有: 斤量トレンド・高水分適性
    "weight_trend", "heavy_water_rate",
]

LABEL_COL = "is_top3"


def _extract_label(h: dict) -> dict:
    fp = h.get("finish_pos")
    return {
        "is_top3": int(fp <= 3) if fp else 0,
        "is_top2": int(fp <= 2) if fp else 0,
        "is_win": int(fp == 1) if fp else 0,
        "finish_pos": fp,
    }
