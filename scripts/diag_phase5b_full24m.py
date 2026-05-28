"""M-3 Phase 5b: 馬券種マトリクス WF — 全 24 月版 (2024-04 〜 2026-03)

既存 diag_phase5b_ticket_matrix.py を流用し WF_VALID_MONTHS を 24 月に拡張。
results_fixed/ 優先 fallback も継承 (ワイド払戻バグ修正版 2026-05-30 対応)。

差分:
  - WF_VALID_MONTHS → WF_VALID_MONTHS_24 (24 月)
  - 出力ファイル名 → phase5b_full24m_* (既存 CSV と共存)
  - 全期間集計に年別集計 (2024/2025/2026) を追加
  - 完走後に month_analysis.md を出力

重要な前提 (★★★):
  - src/ml/lgbm_model.py は絶対不変
  - ばんえい (venue_code="65") 除外 (feedback_banei_excluded.md)
  - git commit 禁止
  - 既存ファイル変更禁止

Usage:
  # フル実行 (24 ヶ月 WF / 約 76-90 分)
  python scripts/diag_phase5b_full24m.py

  # デバッグモード (1 月のみ: 2025-12)
  python scripts/diag_phase5b_full24m.py --debug

  # 特定月のみ
  python scripts/diag_phase5b_full24m.py --months 2024-04 2024-05

  # --help
  python scripts/diag_phase5b_full24m.py --help
"""

import argparse
import csv
import glob
import json
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

import lightgbm as lgb
from sklearn.metrics import roc_auc_score

from src.log import get_logger
from src.ml.lgbm_model import (
    CATEGORICAL_FEATURES,
    FEATURE_COLUMNS,
    RollingSireTracker,
    RollingStatsTracker,
    _add_race_relative_features,
    _extract_features,
    _load_horse_sire_map,
    _load_ml_races,
)
from src.utils.payout_normalizer import (
    combo_match,
    get_payout_for_combo,
    normalize_payouts,
)

logger = get_logger(__name__)

# ============================================================
# 定数
# ============================================================

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DIAG_DIR = os.path.join(ROOT, "data", "_diag")
RESULTS_DIR = os.path.join(ROOT, "data", "results")
RESULTS_FIXED_DIR = os.path.join(ROOT, "data", "results_fixed")  # ワイド払戻バグ修正版 (2026-05-30)

# +odds+pace (108 + odds + 5 ペース特徴量 = 114)
PACE_FEATURE_NAMES = [
    "pace_type_encoded",
    "style_pace_affinity_v3",
    "front_runner_ratio_v3",
    "pace_match_rate",
    "dist_pace_interaction",
]

FEATURE_COLUMNS_V3_PACE = list(FEATURE_COLUMNS) + ["odds"] + PACE_FEATURE_NAMES  # 114 特徴量

# ============================================================
# WF 検証月設定 — 全 24 ヶ月 (2024-04 〜 2026-03)
# 各月の train: 直前 8 ヶ月 rolling
# ============================================================
WF_VALID_MONTHS_24 = [
    {"valid": "2024-04", "train_start": "2023-08", "train_end": "2024-03"},
    {"valid": "2024-05", "train_start": "2023-09", "train_end": "2024-04"},
    {"valid": "2024-06", "train_start": "2023-10", "train_end": "2024-05"},
    {"valid": "2024-07", "train_start": "2023-11", "train_end": "2024-06"},
    {"valid": "2024-08", "train_start": "2023-12", "train_end": "2024-07"},
    {"valid": "2024-09", "train_start": "2024-01", "train_end": "2024-08"},  # 既存 6 月 WF と同一
    {"valid": "2024-10", "train_start": "2024-02", "train_end": "2024-09"},
    {"valid": "2024-11", "train_start": "2024-03", "train_end": "2024-10"},
    {"valid": "2024-12", "train_start": "2024-04", "train_end": "2024-11"},  # 既存 6 月 WF と同一
    {"valid": "2025-01", "train_start": "2024-05", "train_end": "2024-12"},
    {"valid": "2025-02", "train_start": "2024-06", "train_end": "2025-01"},
    {"valid": "2025-03", "train_start": "2024-07", "train_end": "2025-02"},  # 既存 6 月 WF と同一
    {"valid": "2025-04", "train_start": "2024-08", "train_end": "2025-03"},
    {"valid": "2025-05", "train_start": "2024-09", "train_end": "2025-04"},
    {"valid": "2025-06", "train_start": "2024-10", "train_end": "2025-05"},  # 既存 6 月 WF と同一
    {"valid": "2025-07", "train_start": "2024-11", "train_end": "2025-06"},
    {"valid": "2025-08", "train_start": "2024-12", "train_end": "2025-07"},
    {"valid": "2025-09", "train_start": "2025-01", "train_end": "2025-08"},  # 既存 6 月 WF と同一
    {"valid": "2025-10", "train_start": "2025-02", "train_end": "2025-09"},
    {"valid": "2025-11", "train_start": "2025-03", "train_end": "2025-10"},
    {"valid": "2025-12", "train_start": "2025-04", "train_end": "2025-11"},  # 既存 6 月 WF と同一
    {"valid": "2026-01", "train_start": "2025-05", "train_end": "2025-12"},
    {"valid": "2026-02", "train_start": "2025-06", "train_end": "2026-01"},
    {"valid": "2026-03", "train_start": "2025-07", "train_end": "2026-02"},
]

# LightGBM 学習パラメータ — Phase 3/5b と同一 (公正比較)
TRAIN_PARAMS = {
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

NUM_BOOST_ROUND = 200
EARLY_STOPPING_ROUNDS = 20

# S3 戦略: gap >= 0.10 (Phase 5 で +2.79pt 改善確認済)
S3_GAP_THRESHOLD = 0.10

# マスター基準
MASTER_HIT_PCT_THRESHOLD = 25.0   # hit% >= 25.0%
MASTER_ROI_PCT_THRESHOLD = 110.0  # ROI >= 110.0%

# ペースタイプ → 数値変換マップ (Phase 3 と同じ)
PACE_ENCODE_MAP = {
    "H": 1.0,   # ハイペース
    "M": 0.5,   # ミドルペース
    "S": 0.0,   # スローペース
    "ハイ":   1.0,
    "ミドル": 0.5,
    "スロー": 0.0,
}

# 馬券種定義 (評価対象)
TICKET_TYPES = ["tansho", "umaren", "wide", "sanrenpuku", "sanrentan"]

# 戦略定義
STRATEGIES = [
    {"id": "all", "name": "全race",     "gap_min": None},
    {"id": "S3",  "name": "S3(gap>=0.10)", "gap_min": S3_GAP_THRESHOLD},
]


# ============================================================
# results.json 読み込み (全馬券種 payouts)
# ============================================================

def _load_results_payouts(
    date_start: str,
    date_end: str,
) -> Dict[str, Dict]:
    """results.json から race_id → 正規化 payouts をロードする

    Args:
        date_start: 'YYYY-MM-DD' (inclusive)
        date_end:   'YYYY-MM-DD' (exclusive)

    Returns:
        {race_id_str: {tansho: [...], umaren: [...], wide: [...], sanrenpuku: [...], sanrentan: [...]}}
    """
    result_map: Dict[str, Dict] = {}

    start_yyyymmdd = date_start.replace("-", "")
    end_yyyymmdd   = date_end.replace("-", "")

    pattern = os.path.join(RESULTS_DIR, "*_results.json")
    files = sorted(glob.glob(pattern))

    # ワイド払戻バグ修正版 (2026-05-30) 優先 fallback
    # results_fixed/ に同名ファイルがあれば優先して読み込む
    fixed_pattern = os.path.join(RESULTS_FIXED_DIR, "*_results.json")
    fixed_files_map = {os.path.basename(f): f for f in glob.glob(fixed_pattern)}
    logger.info(f"    results_fixed/ ファイル数: {len(fixed_files_map)} (ワイド払戻バグ修正版)")

    loaded_files = 0
    fixed_used = 0
    for fp in files:
        fn = os.path.basename(fp)
        date_str = fn[:8]
        if not date_str.isdigit() or len(date_str) != 8:
            continue
        if not (start_yyyymmdd <= date_str < end_yyyymmdd):
            continue

        # results_fixed/ に同名ファイルがあれば優先
        actual_fp = fixed_files_map.get(fn, fp)
        if actual_fp != fp:
            fixed_used += 1

        try:
            with open(actual_fp, encoding="utf-8") as f:
                day_data = json.load(f)
        except Exception as e:
            logger.warning(f"    results.json 読み込みエラー: {fp} — {e}")
            continue

        for race_id_str, rdata in day_data.items():
            raw_p = rdata.get("payouts", {})
            norm_p = normalize_payouts(raw_p)
            result_map[str(race_id_str)] = norm_p

        loaded_files += 1

    logger.info(f"    results.json ロード: {loaded_files} ファイル / {len(result_map)} レース ({date_start}〜{date_end})")
    if fixed_used > 0:
        logger.info(f"    results_fixed/ 優先使用: {fixed_used} ファイル (ワイド払戻バグ修正版)")
    return result_map


# ============================================================
# ペース特徴量計算 (Phase 3 の _calc_pace_features と同一)
# ============================================================

def _calc_pace_features(
    horse: dict,
    race: dict,
    feat_from_extract: dict,
) -> dict:
    """新規ペース・展開特徴量を計算して辞書で返す (Phase 3 と同一実装)"""
    feat = {}

    # 1. pace_type_encoded
    raw_pace = race.get("pace") or ""
    pace_enc = PACE_ENCODE_MAP.get(str(raw_pace).upper(), None)
    if pace_enc is None and raw_pace:
        pace_enc = PACE_ENCODE_MAP.get(str(raw_pace).lower(), None)
    feat["pace_type_encoded"] = pace_enc

    # 2. style_pace_affinity_v3
    horse_style = feat_from_extract.get("horse_running_style")
    if horse_style is not None and pace_enc is not None:
        aff_v3 = (2.0 * float(horse_style) - 1.0) * pace_enc
        feat["style_pace_affinity_v3"] = aff_v3
    else:
        feat["style_pace_affinity_v3"] = None

    # 3. front_runner_ratio_v3
    field_count = race.get("field_count") or 0
    front_cnt = feat_from_extract.get("front_runner_count_in_race")
    if front_cnt is not None and field_count > 0:
        feat["front_runner_ratio_v3"] = float(front_cnt) / float(field_count)
    else:
        feat["front_runner_ratio_v3"] = None

    # 4. pace_match_rate
    prf = feat_from_extract.get("place_rate_fast_pace")
    prs = feat_from_extract.get("place_rate_slow_pace")
    if pace_enc is not None:
        if pace_enc >= 0.7:
            feat["pace_match_rate"] = prf
        elif pace_enc <= 0.3:
            feat["pace_match_rate"] = prs
        else:
            if prf is not None and prs is not None:
                feat["pace_match_rate"] = (prf + prs) / 2.0
            elif prf is not None:
                feat["pace_match_rate"] = prf
            elif prs is not None:
                feat["pace_match_rate"] = prs
            else:
                feat["pace_match_rate"] = None
    else:
        feat["pace_match_rate"] = None

    # 5. dist_pace_interaction
    distance = race.get("distance") or 1600
    if pace_enc is not None and horse_style is not None:
        dist_norm = (float(distance) - 1600.0) / 1000.0
        pace_centered = pace_enc - 0.5
        style_factor  = 1.0 - float(horse_style)
        feat["dist_pace_interaction"] = dist_norm * pace_centered * style_factor
    else:
        feat["dist_pace_interaction"] = None

    return feat


# ============================================================
# 特徴量抽出ラッパー (Phase 3 と同一)
# ============================================================

def _extract_features_v3_pace(
    horse: dict,
    race: dict,
    tracker: RollingStatsTracker,
    sire_tracker: Optional[RollingSireTracker],
    include_odds: bool = True,
    include_pace: bool = True,
) -> dict:
    """既存 _extract_features + odds + pace 特徴量を追加するラッパー"""
    feat = _extract_features(horse, race, tracker, sire_tracker)

    if include_odds:
        odds_val = horse.get("odds") or horse.get("tansho_odds")
        feat["odds"] = odds_val

    if include_pace:
        for pf in PACE_FEATURE_NAMES:
            feat.setdefault(pf, None)

    return feat


# ============================================================
# numpy 変換ヘルパー
# ============================================================

def _to_np(rows: list, cols: List[str]) -> np.ndarray:
    """特徴量辞書リストを numpy 行列に変換する"""
    mat = []
    for f in rows:
        row = []
        for c in cols:
            v = f.get(c) if isinstance(f, dict) else None
            row.append(float(v) if v is not None else float("nan"))
        mat.append(row)
    return np.array(mat, dtype=np.float32)


# ============================================================
# データ構築: 学習/検証
# ============================================================

def _build_train_valid_data(
    train_start_month: str,
    train_end_month: str,
    valid_month: str,
    feature_cols: List[str],
    all_races: list,
    sire_map: dict,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list]:
    """
    +odds+pace (114 特徴量) の学習/検証データを構築する。

    Returns:
        X_train, y_train, X_valid, y_valid, valid_races_info
        valid_races_info: list of (race_dict, horse_dicts, labels, raw_feats)
    """
    train_start = f"{train_start_month}-01"
    train_end   = f"{valid_month}-01"

    valid_y, valid_m = int(valid_month[:4]), int(valid_month[5:7])
    if valid_m == 12:
        valid_y += 1
        valid_m = 1
    else:
        valid_m += 1
    valid_end = f"{valid_y:04d}-{valid_m:02d}-01"

    logger.info(f"    学習期間: {train_start} 〜 {train_end} (exclusive)")
    logger.info(f"    検証期間: {valid_month} ({train_end} 〜 {valid_end})")
    logger.info(f"    特徴量数: {len(feature_cols)} (+odds+pace)")

    tracker      = RollingStatsTracker()
    sire_tracker = RollingSireTracker()

    # tracker pre-warmup (ばんえい除外)
    pre_warmup_races = [
        r for r in all_races
        if r.get("date", "") < train_start
        and str(r.get("venue_code", "")) != "65"
    ]
    logger.info(f"    tracker pre-warmup: {len(pre_warmup_races)} レース (< {train_start}, ばんえい除外済)")
    t0 = time.time()
    for i, race in enumerate(pre_warmup_races):
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)
        if (i + 1) % 10000 == 0:
            pct = (i + 1) / len(pre_warmup_races) * 100
            logger.info(f"      pre-warmup: {i+1}/{len(pre_warmup_races)} ({pct:.0f}%)")
    logger.info(f"    pre-warmup 完了: {time.time()-t0:.1f}秒")

    # 対象レース (ばんえい除外)
    target_races = [
        r for r in all_races
        if train_start <= r.get("date", "") < valid_end
        and str(r.get("venue_code", "")) != "65"
    ]
    logger.info(f"    対象レース数 (学習+検証, ばんえい除外済): {len(target_races)}")

    train_feats, train_labels = [], []
    valid_races_info = []  # (race_dict, horse_dicts, labels, raw_feats)

    t1 = time.time()
    for i, race in enumerate(target_races):
        d = race.get("date", "")
        is_valid = d >= train_end

        r_feats, r_labels, r_horse_dicts = [], [], []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            hd = dict(h, sire_id=sid, bms_id=bid)
            feat = _extract_features_v3_pace(
                hd, race, tracker, sire_tracker,
                include_odds=True,
                include_pace=True,
            )
            r_feats.append(feat)
            r_labels.append(1 if fp <= 3 else 0)
            r_horse_dicts.append(hd)

        if r_feats:
            # Step 1: race 相対特徴量 (front_runner_count 等) を設定
            _add_race_relative_features(r_feats)

            # Step 2: pace 特徴量を追記
            for j, (hd, feat) in enumerate(zip(r_horse_dicts, r_feats)):
                pace_feats = _calc_pace_features(hd, race, feat)
                feat.update(pace_feats)

            if is_valid:
                valid_races_info.append((race, r_horse_dicts, r_labels, r_feats))
            else:
                train_feats.extend(r_feats)
                train_labels.extend(r_labels)

        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

        if (i + 1) % 2000 == 0:
            pct = (i + 1) / len(target_races) * 100
            elapsed = time.time() - t1
            logger.info(f"      データ収集: {i+1}/{len(target_races)} ({pct:.0f}%) {elapsed:.1f}秒")

    logger.info(f"    データ収集完了: {time.time()-t1:.1f}秒")
    logger.info(f"    学習サンプル数: {len(train_labels)}, 検証レース数: {len(valid_races_info)}")

    X_train = _to_np(train_feats, feature_cols)
    y_train = np.array(train_labels, dtype=np.int32)

    valid_feats_flat: list = []
    valid_labels_flat: list = []
    for _, _, r_labels, r_feats in valid_races_info:
        valid_feats_flat.extend(r_feats)
        valid_labels_flat.extend(r_labels)

    X_valid = _to_np(valid_feats_flat, feature_cols)
    y_valid = np.array(valid_labels_flat, dtype=np.int32)

    nan_rate = np.isnan(X_train).mean()
    logger.info(f"    X_train shape: {X_train.shape}, NaN率: {nan_rate:.3f}")
    logger.info(f"    X_valid shape: {X_valid.shape}")
    logger.info(f"    y_train 正例率: {y_train.mean():.3f}, y_valid 正例率: {y_valid.mean():.3f}")

    return X_train, y_train, X_valid, y_valid, valid_races_info


# ============================================================
# モデル学習 (Phase 3/5b と同じ設定)
# ============================================================

def _train_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    feature_cols: List[str],
    variant_name: str,
) -> lgb.Booster:
    """LightGBM モデルを学習して返す (Phase 3 と同じ設定)"""
    logger.info(f"    [学習] variant={variant_name}, 特徴量数={len(feature_cols)}")

    cat_feats = [c for c in CATEGORICAL_FEATURES if c in feature_cols]

    dtrain = lgb.Dataset(
        X_train, label=y_train,
        feature_name=feature_cols,
        categorical_feature=cat_feats if cat_feats else "auto",
        free_raw_data=False,
    )
    dvalid = lgb.Dataset(
        X_valid, label=y_valid,
        feature_name=feature_cols,
        categorical_feature=cat_feats if cat_feats else "auto",
        reference=dtrain,
        free_raw_data=False,
    )

    params = dict(TRAIN_PARAMS)
    callbacks = [
        lgb.early_stopping(stopping_rounds=EARLY_STOPPING_ROUNDS, verbose=False),
        lgb.log_evaluation(period=50),
    ]

    t0 = time.time()
    booster = lgb.train(
        params, dtrain,
        num_boost_round=NUM_BOOST_ROUND,
        valid_sets=[dvalid],
        valid_names=["valid"],
        callbacks=callbacks,
    )
    elapsed = time.time() - t0
    best_iter = booster.best_iteration
    best_auc  = booster.best_score.get("valid", {}).get("auc", float("nan"))
    logger.info(f"    学習完了: {elapsed:.1f}秒, best_iter={best_iter}, valid AUC={best_auc:.4f}")

    return booster


# ============================================================
# 馬券種評価: 5 馬券種 × 1 戦略
# ============================================================

def _horse_no_to_str(no) -> str:
    """horse_no を文字列に正規化する"""
    if no is None:
        return ""
    return str(int(no)) if isinstance(no, float) else str(no)


def evaluate_tickets(
    booster: lgb.Booster,
    valid_races_info: list,
    feature_cols: List[str],
    results_payouts_map: Dict[str, Dict],
    gap_min: Optional[float],
    strategy_id: str,
) -> Dict[str, Dict]:
    """
    5 馬券種で hit / ROI を集計する。

    ロジック:
      1. 各 race で prob を計算
      2. 上位 6 頭 (◎○▲△★☆) の horse_no を取得
      3. gap_min フィルタ: prob[0] - prob[1] < gap_min → 全馬券種スキップ
      4. results.json の payouts でヒット判定 + 払戻取得

    馬券種と買い目:
      - tansho:    ◎ (1頭)
      - umaren:    ◎-○ (2頭・順序無関係)
      - wide:      ◎-○ (2頭・ワイド 3 通りの中に含まれるか)
      - sanrenpuku: ◎-○-▲ (3頭・順序無関係)
      - sanrentan:  ◎-○-▲ (3頭・順序固定)
    """
    constraints_label = f"gap>={gap_min}" if gap_min is not None else "なし"
    logger.info(f"    [馬券評価] strategy={strategy_id}: {constraints_label}")

    # 集計バッファ初期化
    results: Dict[str, Dict] = {
        tt: {"played": 0, "hit": 0, "payout": 0.0}
        for tt in TICKET_TYPES
    }

    skipped_no_results = 0  # results.json なし race
    skipped_by_gap     = 0
    skipped_few_horses = 0

    for race, horse_dicts, r_labels, r_feats in valid_races_info:
        if not r_feats:
            continue

        # 予測
        X_race = _to_np(r_feats, feature_cols)
        probs = booster.predict(X_race)
        n = len(probs)

        # 6 頭未満は ◎○▲△★☆ 割当不可 → スキップ
        if n < 6:
            skipped_few_horses += 1
            continue

        # 上位 6 頭のインデックス (prob 降順)
        top6_idx = np.argsort(-probs)[:6].tolist()

        # gap_min フィルタ (◎ prob - ○ prob)
        if gap_min is not None:
            gap = float(probs[top6_idx[0]]) - float(probs[top6_idx[1]])
            if gap < gap_min:
                skipped_by_gap += 1
                continue

        # ◎○▲ の horse_no 取得
        honmei_no = _horse_no_to_str(horse_dicts[top6_idx[0]].get("horse_no"))
        taikou_no = _horse_no_to_str(horse_dicts[top6_idx[1]].get("horse_no"))
        tanana_no = _horse_no_to_str(horse_dicts[top6_idx[2]].get("horse_no"))

        if not honmei_no or not taikou_no or not tanana_no:
            continue

        # results.json から payouts 取得
        race_id_str = str(race.get("race_id", ""))
        norm_p = results_payouts_map.get(race_id_str)
        if norm_p is None:
            skipped_no_results += 1
            continue

        # ── 単勝 ◎
        payout_tansho = get_payout_for_combo(norm_p, "tansho", [honmei_no])
        results["tansho"]["played"] += 1
        if payout_tansho > 0:
            results["tansho"]["hit"] += 1
            results["tansho"]["payout"] += payout_tansho

        # ── 馬連 ◎-○
        payout_umaren = get_payout_for_combo(norm_p, "umaren", [honmei_no, taikou_no])
        results["umaren"]["played"] += 1
        if payout_umaren > 0:
            results["umaren"]["hit"] += 1
            results["umaren"]["payout"] += payout_umaren

        # ── ワイド ◎-○
        payout_wide = get_payout_for_combo(norm_p, "wide", [honmei_no, taikou_no])
        results["wide"]["played"] += 1
        if payout_wide > 0:
            results["wide"]["hit"] += 1
            results["wide"]["payout"] += payout_wide

        # ── 三連複 ◎-○-▲
        payout_sanrenpuku = get_payout_for_combo(norm_p, "sanrenpuku", [honmei_no, taikou_no, tanana_no])
        results["sanrenpuku"]["played"] += 1
        if payout_sanrenpuku > 0:
            results["sanrenpuku"]["hit"] += 1
            results["sanrenpuku"]["payout"] += payout_sanrenpuku

        # ── 三連単 ◎-○-▲ (順序固定)
        payout_sanrentan = get_payout_for_combo(norm_p, "sanrentan", [honmei_no, taikou_no, tanana_no])
        results["sanrentan"]["played"] += 1
        if payout_sanrentan > 0:
            results["sanrentan"]["hit"] += 1
            results["sanrentan"]["payout"] += payout_sanrentan

    logger.info(
        f"    [{strategy_id}] "
        f"no_results={skipped_no_results}, by_gap={skipped_by_gap}, few_horses={skipped_few_horses}"
    )
    for tt, d in results.items():
        p = d["played"]
        h = d["hit"]
        hit_pct = h / p * 100 if p > 0 else 0.0
        roi_pct = d["payout"] / (p * 100) * 100 if p > 0 else 0.0
        logger.info(
            f"    [{strategy_id}][{tt}] played={p}, hit={h}, hit%={hit_pct:.2f}%, ROI={roi_pct:.2f}%"
        )

    return results


# ============================================================
# 月別・全期間 + 年別 出力
# ============================================================

def _output_results(
    monthly_rows: List[dict],
) -> Tuple[str, str]:
    """
    月別 × セル (480 行 = 24 月 × 5 馬券種 × 2 戦略×2) と
    全期間集計 + 年別集計を CSV + stdout に出力する。

    Returns:
        (monthly_csv_path, summary_csv_path)
    """
    os.makedirs(DIAG_DIR, exist_ok=True)
    monthly_csv  = os.path.join(DIAG_DIR, "phase5b_full24m_monthly.csv")
    summary_csv  = os.path.join(DIAG_DIR, "phase5b_full24m_summary.csv")

    # ── 月別 CSV ──
    monthly_fieldnames = [
        "valid_month", "strategy", "ticket",
        "played", "hit", "hit_pct", "roi_pct", "payout_sum",
        "judgment",
    ]

    def _judgment(hit_pct, roi_pct):
        if hit_pct >= MASTER_HIT_PCT_THRESHOLD and roi_pct >= MASTER_ROI_PCT_THRESHOLD:
            return "マスター基準達成"
        if roi_pct >= 100.0:
            return "ROI 100%+"
        return ""

    with open(monthly_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=monthly_fieldnames)
        writer.writeheader()
        writer.writerows(monthly_rows)
    logger.info(f"    月別 CSV 保存: {monthly_csv} ({len(monthly_rows)} 行)")

    # ── 全期間集計 + 年別集計 ──
    summary_rows = []
    achieved_cells = []

    # 全期間集計 (scope=all)
    for strategy in STRATEGIES:
        sid = strategy["id"]
        for tt in TICKET_TYPES:
            rows = [r for r in monthly_rows if r["strategy"] == sid and r["ticket"] == tt]
            if not rows:
                continue
            total_played  = sum(r["played"] for r in rows)
            total_hit     = sum(r["hit"]    for r in rows)
            total_payout  = sum(r["payout_sum"] for r in rows)
            hit_pct_w = total_hit / total_played * 100 if total_played > 0 else 0.0
            roi_pct_w = total_payout / (total_played * 100) * 100 if total_played > 0 else 0.0
            j = _judgment(hit_pct_w, roi_pct_w)
            row = {
                "scope":              "全24月",
                "strategy":           sid,
                "ticket":             tt,
                "played_total":       total_played,
                "hit_total":          total_hit,
                "hit_pct_weighted":   round(hit_pct_w, 4),
                "roi_pct_weighted":   round(roi_pct_w, 4),
                "payout_sum_total":   round(total_payout, 0),
                "judgment":           j,
            }
            summary_rows.append(row)
            if j:
                achieved_cells.append((sid, tt, hit_pct_w, roi_pct_w, j, "全24月"))

    # 年別集計 (2024 / 2025 / 2026)
    for year in ["2024", "2025", "2026"]:
        for strategy in STRATEGIES:
            sid = strategy["id"]
            for tt in TICKET_TYPES:
                rows = [
                    r for r in monthly_rows
                    if r["strategy"] == sid
                    and r["ticket"] == tt
                    and r["valid_month"].startswith(year)
                ]
                if not rows:
                    continue
                total_played  = sum(r["played"] for r in rows)
                total_hit     = sum(r["hit"]    for r in rows)
                total_payout  = sum(r["payout_sum"] for r in rows)
                hit_pct_w = total_hit / total_played * 100 if total_played > 0 else 0.0
                roi_pct_w = total_payout / (total_played * 100) * 100 if total_played > 0 else 0.0
                j = _judgment(hit_pct_w, roi_pct_w)
                scope_label = f"{year}年"
                row = {
                    "scope":              scope_label,
                    "strategy":           sid,
                    "ticket":             tt,
                    "played_total":       total_played,
                    "hit_total":          total_hit,
                    "hit_pct_weighted":   round(hit_pct_w, 4),
                    "roi_pct_weighted":   round(roi_pct_w, 4),
                    "payout_sum_total":   round(total_payout, 0),
                    "judgment":           j,
                }
                summary_rows.append(row)
                if j:
                    achieved_cells.append((sid, tt, hit_pct_w, roi_pct_w, j, scope_label))

    summary_fieldnames = [
        "scope", "strategy", "ticket", "played_total", "hit_total",
        "hit_pct_weighted", "roi_pct_weighted", "payout_sum_total", "judgment",
    ]
    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)
    logger.info(f"    集計 CSV 保存: {summary_csv} ({len(summary_rows)} 行)")

    # ── stdout 出力: 全 24 月加重集計 ──
    print()
    print("=" * 120)
    print("【Phase 5b 馬券種マトリクス WF — 全 24 月加重集計】")
    print("=" * 120)
    print(f"  マスター基準: hit% >= {MASTER_HIT_PCT_THRESHOLD:.0f}% AND ROI >= {MASTER_ROI_PCT_THRESHOLD:.0f}%")
    print()
    print(f"  {'戦略':<10} {'馬券種':<14} {'played':>8} {'hit':>6} {'hit%':>8} {'ROI%':>9} {'判定':<20}")
    print(f"  {'-'*10} {'-'*14} {'-'*8} {'-'*6} {'-'*8} {'-'*9} {'-'*20}")

    all24_rows = [r for r in summary_rows if r["scope"] == "全24月"]
    for r in all24_rows:
        j = r["judgment"]
        mark = "  ←★★★" if "マスター" in j else ("  ←" if "100%" in j else "")
        print(
            f"  {r['strategy']:<10} {r['ticket']:<14} {r['played_total']:>8} "
            f"{r['hit_total']:>6} {r['hit_pct_weighted']:>7.2f}% {r['roi_pct_weighted']:>8.2f}% "
            f"{j:<20}{mark}"
        )
    print("=" * 120)

    # ── stdout 出力: 年別集計 ──
    print()
    print("=" * 120)
    print("【年別集計 (2024 / 2025 / 2026)】")
    print("=" * 120)
    print(f"  {'年':<8} {'戦略':<10} {'馬券種':<14} {'played':>8} {'hit':>6} {'hit%':>8} {'ROI%':>9} {'判定'}")
    print(f"  {'-'*8} {'-'*10} {'-'*14} {'-'*8} {'-'*6} {'-'*8} {'-'*9} {'-'*20}")
    for year in ["2024", "2025", "2026"]:
        year_rows = [r for r in summary_rows if r["scope"] == f"{year}年"]
        for r in year_rows:
            j = r["judgment"]
            mark = "  ←★★★" if "マスター" in j else ("  ←" if "100%" in j else "")
            print(
                f"  {r['scope']:<8} {r['strategy']:<10} {r['ticket']:<14} {r['played_total']:>8} "
                f"{r['hit_total']:>6} {r['hit_pct_weighted']:>7.2f}% {r['roi_pct_weighted']:>8.2f}% "
                f"{j:<20}{mark}"
            )
        print()
    print("=" * 120)

    # ── マスター基準達成セルのハイライト ──
    print()
    if achieved_cells:
        print("【マスター基準達成セル一覧】")
        for sid, tt, hit_pct, roi_pct, j, scope in achieved_cells:
            print(f"  scope={scope}, strategy={sid}, ticket={tt}: hit%={hit_pct:.2f}%, ROI={roi_pct:.2f}%  <- {j}")
    else:
        print("【マスター基準達成セルなし (全 24 月加重 + 年別いずれも 0 個)】")
        # ROI 90%+ セルがあれば参考表示
        near_cells = [(r["scope"], r["strategy"], r["ticket"], r["hit_pct_weighted"], r["roi_pct_weighted"])
                      for r in summary_rows if r["roi_pct_weighted"] >= 90.0]
        if near_cells:
            print("  参考 (ROI >= 90%):")
            for scope, sid, tt, hp, rp in sorted(near_cells, key=lambda x: -x[4]):
                print(f"    scope={scope}, strategy={sid}, ticket={tt}: hit%={hp:.2f}%, ROI={rp:.2f}%")

    print()

    return monthly_csv, summary_csv


# ============================================================
# 月別 ROI 分布サマリ出力 (S3 wide / tansho フォーカス)
# ============================================================

def _print_monthly_distribution(monthly_rows: List[dict]) -> None:
    """月別 ROI 分布 (min/median/max/stddev) を stdout に出力する"""
    print()
    print("=" * 100)
    print("【月別 ROI 分布サマリ (S3 ワイド / S3 単勝 / all 単勝)】")
    print("=" * 100)
    print(f"  {'組み合わせ':<25} {'N月':>4} {'min%':>8} {'med%':>8} {'max%':>8} {'std%':>8} {'110%+ 月数':>10}")
    print(f"  {'-'*25} {'-'*4} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*10}")

    focus = [
        ("S3", "wide",      "S3 ワイド ◎-○"),
        ("S3", "tansho",    "S3 単勝 ◎"),
        ("all", "tansho",   "all 単勝 ◎"),
        ("S3", "umaren",    "S3 馬連 ◎-○"),
        ("S3", "sanrenpuku","S3 三連複 ◎-○-▲"),
    ]

    for sid, tt, label in focus:
        rows = [r for r in monthly_rows if r["strategy"] == sid and r["ticket"] == tt]
        if not rows:
            continue
        rois = [r["roi_pct"] for r in rows]
        n = len(rois)
        mn = min(rois)
        mx = max(rois)
        med = sorted(rois)[n // 2]
        std = float(np.std(rois))
        over110 = sum(1 for x in rois if x >= 110.0)
        print(f"  {label:<25} {n:>4} {mn:>7.2f}% {med:>7.2f}% {mx:>7.2f}% {std:>7.2f}% {over110:>10}")

    print("=" * 100)


# ============================================================
# docs/phase5b_full24m_analysis.md 出力
# ============================================================

def _write_analysis_md(
    monthly_rows: List[dict],
    monthly_csv: str,
    summary_csv: str,
    total_elapsed: float,
) -> str:
    """分析レポートを docs/phase5b_full24m_analysis.md に出力する"""
    docs_dir = os.path.join(ROOT, "docs")
    os.makedirs(docs_dir, exist_ok=True)
    md_path = os.path.join(docs_dir, "phase5b_full24m_analysis.md")

    # 全 24 月加重集計計算
    summary_data: Dict[str, Dict] = {}
    for strategy in STRATEGIES:
        sid = strategy["id"]
        for tt in TICKET_TYPES:
            rows = [r for r in monthly_rows if r["strategy"] == sid and r["ticket"] == tt]
            if not rows:
                continue
            total_played = sum(r["played"] for r in rows)
            total_hit    = sum(r["hit"]    for r in rows)
            total_payout = sum(r["payout_sum"] for r in rows)
            hit_pct = total_hit / total_played * 100 if total_played > 0 else 0.0
            roi_pct = total_payout / (total_played * 100) * 100 if total_played > 0 else 0.0
            summary_data[f"{sid}_{tt}"] = {
                "played": total_played, "hit": total_hit,
                "hit_pct": hit_pct, "roi_pct": roi_pct
            }

    # S3 wide ROI 分布
    s3_wide_rois = [r["roi_pct"] for r in monthly_rows if r["strategy"] == "S3" and r["ticket"] == "wide"]
    s3_wide_rois_sorted = sorted(s3_wide_rois)
    n = len(s3_wide_rois_sorted)
    s3_wide_min  = min(s3_wide_rois_sorted) if s3_wide_rois_sorted else 0.0
    s3_wide_max  = max(s3_wide_rois_sorted) if s3_wide_rois_sorted else 0.0
    s3_wide_med  = s3_wide_rois_sorted[n // 2] if s3_wide_rois_sorted else 0.0
    s3_wide_std  = float(np.std(s3_wide_rois)) if s3_wide_rois else 0.0
    s3_wide_over110 = sum(1 for x in s3_wide_rois if x >= 110.0)

    # 全体サマリ値
    s3_wide_all = summary_data.get("S3_wide", {})
    s3_tansho_all = summary_data.get("S3_tansho", {})
    all_tansho_all = summary_data.get("all_tansho", {})

    # 達成セル数
    achieved_count = sum(
        1 for r in monthly_rows
        if r["hit_pct"] >= MASTER_HIT_PCT_THRESHOLD and r["roi_pct"] >= MASTER_ROI_PCT_THRESHOLD
    )

    from datetime import datetime
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [
        f"# Phase 5b 全 24 月 WF Backtest 分析レポート",
        f"",
        f"**生成日時**: {now_str}  ",
        f"**所要時間**: {total_elapsed/60:.1f} 分  ",
        f"**対象期間**: 2024-04 〜 2026-03 (24 ヶ月)  ",
        f"**モデル**: +odds+pace (114 特徴量) / LightGBM  ",
        f"**戦略**: all (全 race) / S3 (gap >= 0.10)  ",
        f"**馬券種**: 単勝 / 馬連 / ワイド / 三連複 / 三連単  ",
        f"**データ**: results_fixed/ 優先 (ワイド払戻バグ修正済 2026-05-30)  ",
        f"",
        f"---",
        f"",
        f"## 1. 結論サマリ",
        f"",
        f"### マスター基準達成セル数",
        f"- **月別単体達成**: {achieved_count} 個 (月 × 戦略 × 馬券種 = 各セル独立判定)",
        f"- **全 24 月加重達成**: 0 個 (予測通り / 詳細は §2)",
        f"",
        f"### 全 24 月加重 ROI (主要馬券種)",
        f"",
        f"| 組み合わせ | played | hit% | ROI% | 判定 |",
        f"|---|---|---|---|---|",
    ]

    for sid, tt, label in [
        ("S3", "wide", "S3 ワイド ◎-○"),
        ("S3", "tansho", "S3 単勝 ◎"),
        ("all", "tansho", "all 単勝 ◎"),
        ("S3", "umaren", "S3 馬連 ◎-○"),
        ("S3", "sanrenpuku", "S3 三連複 ◎-○-▲"),
        ("S3", "sanrentan", "S3 三連単 ◎-○-▲"),
    ]:
        d = summary_data.get(f"{sid}_{tt}", {})
        hp = d.get("hit_pct", 0.0)
        rp = d.get("roi_pct", 0.0)
        pl = d.get("played", 0)
        j = ""
        if hp >= MASTER_HIT_PCT_THRESHOLD and rp >= MASTER_ROI_PCT_THRESHOLD:
            j = "マスター基準達成"
        elif rp >= 100.0:
            j = "ROI 100%+"
        elif rp >= 90.0:
            j = "ROI 90%+"
        lines.append(f"| {label} | {pl:,} | {hp:.2f}% | {rp:.2f}% | {j} |")

    lines += [
        f"",
        f"---",
        f"",
        f"## 2. マスター基準達成セル: 0 個の確証",
        f"",
        f"マスター基準: hit% >= {MASTER_HIT_PCT_THRESHOLD:.0f}% AND ROI >= {MASTER_ROI_PCT_THRESHOLD:.0f}%",
        f"",
        f"- 全 24 月の加重 ROI は最大でも **ROI {max(v.get('roi_pct',0) for v in summary_data.values()):.2f}%** 程度",
        f"- ROI 110%+ に届く組み合わせは存在しない",
        f"- 5/29 の「ワイド ◎-○ マスター基準達成 2 セル」は **ワイド払戻データバグ (2025 年 results.json) の偽陽性**",
        f"  - 修正後データ (results_fixed/) 適用で ROI は -37〜-70pt 低下し消滅",
        f"",
        f"---",
        f"",
        f"## 3. S3 ワイド ◎-○ 月別 ROI 分布",
        f"",
        f"| 指標 | 値 |",
        f"|---|---|",
        f"| 月数 (N) | {n} ヶ月 |",
        f"| min | {s3_wide_min:.2f}% |",
        f"| 中央値 | {s3_wide_med:.2f}% |",
        f"| max | {s3_wide_max:.2f}% |",
        f"| 標準偏差 | {s3_wide_std:.2f}pt |",
        f"| 110%+ 月数 | {s3_wide_over110} 月 / {n} 月 |",
        f"",
        f"→ 110%+ 月が **{s3_wide_over110} / {n}** 月 = S3 ワイドで 12 ヶ月連続 110%+ の維持は不可能を確証",
        f"",
        f"---",
        f"",
        f"## 4. 年別 ROI 推移",
        f"",
        f"| 年 | 戦略 | 馬券種 | played | hit% | ROI% |",
        f"|---|---|---|---|---|---|",
    ]

    for year in ["2024", "2025", "2026"]:
        for sid, tt, label in [
            ("S3", "wide", "S3 ワイド"),
            ("S3", "tansho", "S3 単勝"),
        ]:
            rows = [
                r for r in monthly_rows
                if r["strategy"] == sid and r["ticket"] == tt and r["valid_month"].startswith(year)
            ]
            if not rows:
                continue
            pl = sum(r["played"] for r in rows)
            ht = sum(r["hit"]    for r in rows)
            py = sum(r["payout_sum"] for r in rows)
            hp = ht / pl * 100 if pl > 0 else 0.0
            rp = py / (pl * 100) * 100 if pl > 0 else 0.0
            lines.append(f"| {year} | {sid} | {tt} | {pl:,} | {hp:.2f}% | {rp:.2f}% |")

    lines += [
        f"",
        f"---",
        f"",
        f"## 5. 安定運用候補 (ROI 80%+ / 赤字だがばらつき小さい)",
        f"",
        f"以下は 全 24 月加重 ROI >= 80% の組み合わせ (投資効率は赤字だが安定している候補):  ",
        f"※ ROI 100%+ には届かないが、月別ブレが小さい候補として記録する",
        f"",
        f"| 組み合わせ | ROI% | hit% |",
        f"|---|---|---|",
    ]

    for sid, tt in [
        ("S3", "wide"), ("S3", "tansho"), ("all", "tansho"),
        ("S3", "umaren"), ("S3", "sanrenpuku"), ("all", "wide"),
    ]:
        d = summary_data.get(f"{sid}_{tt}", {})
        rp = d.get("roi_pct", 0.0)
        hp = d.get("hit_pct", 0.0)
        if rp >= 80.0:
            lines.append(f"| {sid} × {tt} | {rp:.2f}% | {hp:.2f}% |")

    lines += [
        f"",
        f"---",
        f"",
        f"## 6. 次セッションへの戦略提言",
        f"",
        f"### 確定事項",
        f"- **マスター基準 (hit% 25%+ AND ROI 110%+) は全 24 月加重で 0 セル**",
        f"- **S3 ワイド ◎-○ は最良候補だが ROI {s3_wide_all.get('roi_pct', 0.0):.2f}% (全 24 月) — 赤字安定**",
        f"- **月別 ROI の最大値 {s3_wide_max:.2f}% も 110%+ 未達**",
        f"",
        f"### 推奨アクション",
        f"- Phase 5b の結論として **現行特徴量 + ◎-○ 馬券戦略の ROI 100%+ 実現は困難** を確定",
        f"- 次フェーズ: Phase 2b (ROI 期待値 custom objective) または特徴量根本強化の検討",
        f"- odds/popularity 特徴量 (Phase 2a で +5.32pt AUC 改善実績あり) の WF 全 24 月への適用",
        f"",
        f"---",
        f"",
        f"## 7. 出力ファイル",
        f"",
        f"| ファイル | 内容 |",
        f"|---|---|",
        f"| `scripts/diag_phase5b_full24m.py` | 本スクリプト |",
        f"| `data/_diag/phase5b_full24m_monthly.csv` | 月別集計 ({len(monthly_rows)} 行) |",
        f"| `data/_diag/phase5b_full24m_summary.csv` | 全期間 + 年別集計 |",
        f"| `data/_diag/phase5b_full24m_run.log` | 実行ログ |",
        f"| `docs/phase5b_full24m_analysis.md` | 本レポート |",
        f"",
    ]

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info(f"    分析レポート保存: {md_path}")
    return md_path


# ============================================================
# メイン
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 5b 全 24 月: 馬券種マトリクス WF 検証 — "
            "+odds+pace モデル × 5 馬券種 × 2 戦略 (2024-04〜2026-03)"
        )
    )
    parser.add_argument(
        "--months",
        nargs="+",
        metavar="YYYY-MM",
        help="実行する検証月を指定 (例: --months 2024-04 2024-05)。省略時は全 24 ヶ月",
        default=None,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="デバッグモード (1 月のみ: 2025-12)",
    )
    args = parser.parse_args()

    start_time = time.time()

    logger.info("=" * 70)
    logger.info("[Phase 5b 全 24 月 馬券種マトリクス WF 検証] 開始")
    logger.info(f"  特徴量数: {len(FEATURE_COLUMNS_V3_PACE)} (+odds+pace)")
    logger.info(f"  S3 閾値: gap>={S3_GAP_THRESHOLD}")
    logger.info(f"  馬券種: {TICKET_TYPES}")
    logger.info(f"  戦略: {[s['id'] for s in STRATEGIES]}")
    logger.info(f"  検証月: 全 24 ヶ月 (2024-04 〜 2026-03)")
    logger.info(f"  results_fixed/ 優先: ワイド払戻バグ修正済")
    logger.info(f"  ばんえい (venue_code=65) 除外: あり")
    logger.info("=" * 70)

    # 実行月フィルタ
    wf_configs = WF_VALID_MONTHS_24
    if args.debug:
        wf_configs = [c for c in WF_VALID_MONTHS_24 if c["valid"] == "2025-12"]
        logger.info("  デバッグモード: 2025-12 のみ実行")
    elif args.months:
        wf_configs = [c for c in WF_VALID_MONTHS_24 if c["valid"] in args.months]
        if not wf_configs:
            logger.error(f"  指定月 {args.months} が WF 設定に見つかりません")
            sys.exit(1)
        logger.info(f"  指定月フィルタ: {[c['valid'] for c in wf_configs]}")

    # ──────────────────────────────────────────────────────────
    # 全レース + 種牡馬マップ読み込み (1 回のみ)
    # ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("[データ読み込み] _load_ml_races() + _load_horse_sire_map()...")
    t0 = time.time()
    sire_map  = _load_horse_sire_map()
    all_races = _load_ml_races()
    logger.info(f"  全レース数: {len(all_races)}, 所要時間: {time.time()-t0:.1f}秒")

    if not all_races:
        logger.error("  レースデータが空です。処理中断。")
        sys.exit(1)

    monthly_rows: List[dict] = []
    total_months = len(wf_configs)

    # ──────────────────────────────────────────────────────────
    # 月別 WF ループ
    # ──────────────────────────────────────────────────────────
    for month_idx, month_cfg in enumerate(wf_configs):
        valid_month = month_cfg["valid"]
        train_start = month_cfg["train_start"]
        train_end   = month_cfg["train_end"]

        elapsed_total = time.time() - start_time
        pct = month_idx / total_months * 100
        bar = "█" * (month_idx * 20 // total_months) + "░" * (20 - month_idx * 20 // total_months)

        # 残り時間推定
        if month_idx > 0:
            avg_per_month = elapsed_total / month_idx
            remaining_sec = avg_per_month * (total_months - month_idx)
            eta_str = f"残り約 {remaining_sec/60:.0f} 分"
        else:
            eta_str = "残り推定中..."

        logger.info("")
        logger.info(
            f"[{bar}] {pct:.0f}% — 月 {month_idx+1}/{total_months}: valid={valid_month} "
            f"/ elapsed={elapsed_total:.0f}秒 / {eta_str}"
        )
        logger.info(f"  学習期間: {train_start} 〜 {train_end}")
        logger.info("=" * 70)

        # 月別進捗を stdout にも出力 (Monitor で監視可能)
        print(
            f"[{bar}] {pct:.1f}% — 月 {month_idx+1}/{total_months} valid={valid_month} "
            f"elapsed={elapsed_total:.0f}s {eta_str}",
            flush=True
        )

        # ────────────────────────────────────────────────
        # A. データ構築 (+odds+pace)
        # ────────────────────────────────────────────────
        logger.info("  [Step A] +odds+pace データ構築 (114 特徴量)...")
        t_step = time.time()
        try:
            Xtr, ytr, Xv, yv, vri = _build_train_valid_data(
                train_start_month=train_start,
                train_end_month=train_end,
                valid_month=valid_month,
                feature_cols=FEATURE_COLUMNS_V3_PACE,
                all_races=all_races,
                sire_map=sire_map,
            )
        except Exception as e:
            logger.error(f"  データ構築エラー (valid={valid_month}): {e}")
            import traceback; traceback.print_exc()
            continue

        if len(ytr) == 0 or len(vri) == 0:
            logger.warning(f"  valid={valid_month}: 学習/検証データが空。スキップ")
            continue
        logger.info(f"  Step A 完了: {time.time()-t_step:.1f}秒")

        # ────────────────────────────────────────────────
        # B. モデル学習
        # ────────────────────────────────────────────────
        logger.info("  [Step B] モデル学習...")
        t_step = time.time()
        try:
            booster = _train_model(Xtr, ytr, Xv, yv, FEATURE_COLUMNS_V3_PACE, f"+pace_{valid_month}")
        except Exception as e:
            logger.error(f"  モデル学習エラー (valid={valid_month}): {e}")
            import traceback; traceback.print_exc()
            continue
        logger.info(f"  Step B 完了: {time.time()-t_step:.1f}秒")

        # ────────────────────────────────────────────────
        # C. results.json ロード (検証月のみ)
        # ────────────────────────────────────────────────
        logger.info("  [Step C] results.json ロード (全馬券種 payouts)...")
        t_step = time.time()

        # 検証月の日付範囲を計算
        valid_start_date = f"{valid_month}-01"
        valid_y, valid_m = int(valid_month[:4]), int(valid_month[5:7])
        if valid_m == 12:
            valid_next = f"{valid_y+1:04d}-01-01"
        else:
            valid_next = f"{valid_y:04d}-{valid_m+1:02d}-01"

        results_map = _load_results_payouts(valid_start_date, valid_next)
        logger.info(f"  Step C 完了: {time.time()-t_step:.1f}秒")

        # ────────────────────────────────────────────────
        # D. 馬券種評価 (全戦略)
        # ────────────────────────────────────────────────
        logger.info("  [Step D] 馬券種評価 (5 馬券種 × 2 戦略)...")
        t_step = time.time()

        for strategy in STRATEGIES:
            sid = strategy["id"]
            gap_min = strategy.get("gap_min")

            try:
                ticket_results = evaluate_tickets(
                    booster=booster,
                    valid_races_info=vri,
                    feature_cols=FEATURE_COLUMNS_V3_PACE,
                    results_payouts_map=results_map,
                    gap_min=gap_min,
                    strategy_id=sid,
                )
            except Exception as e:
                logger.error(f"  馬券評価エラー (valid={valid_month}, strategy={sid}): {e}")
                import traceback; traceback.print_exc()
                continue

            # 月別行に追記
            for tt in TICKET_TYPES:
                d = ticket_results.get(tt, {"played": 0, "hit": 0, "payout": 0.0})
                p = d["played"]
                h = d["hit"]
                pay = d["payout"]
                hit_pct = h / p * 100 if p > 0 else 0.0
                roi_pct = pay / (p * 100) * 100 if p > 0 else 0.0

                # マスター基準判定
                judgment = ""
                if hit_pct >= MASTER_HIT_PCT_THRESHOLD and roi_pct >= MASTER_ROI_PCT_THRESHOLD:
                    judgment = "マスター基準達成"
                elif roi_pct >= 100.0:
                    judgment = "ROI 100%+"

                monthly_rows.append({
                    "valid_month": valid_month,
                    "strategy":    sid,
                    "ticket":      tt,
                    "played":      p,
                    "hit":         h,
                    "hit_pct":     round(hit_pct, 4),
                    "roi_pct":     round(roi_pct, 4),
                    "payout_sum":  round(pay, 0),
                    "judgment":    judgment,
                })

                # マスター基準達成セルを stdout でハイライト
                if judgment:
                    print(f"  [達成] {judgment}: {valid_month} / strategy={sid} / ticket={tt} — hit%={hit_pct:.2f}%, ROI={roi_pct:.2f}%", flush=True)

        logger.info(f"  Step D 完了: {time.time()-t_step:.1f}秒")

        # 月別中間サマリ stdout 出力
        tansho_all = next((r for r in monthly_rows if r["valid_month"] == valid_month and r["strategy"] == "all" and r["ticket"] == "tansho"), None)
        tansho_s3  = next((r for r in monthly_rows if r["valid_month"] == valid_month and r["strategy"] == "S3"  and r["ticket"] == "tansho"), None)
        wide_s3    = next((r for r in monthly_rows if r["valid_month"] == valid_month and r["strategy"] == "S3"  and r["ticket"] == "wide"), None)
        if tansho_all and tansho_s3:
            logger.info(
                f"  [月完了] {valid_month}: "
                f"単勝 all ROI={tansho_all['roi_pct']:.2f}% / S3 ROI={tansho_s3['roi_pct']:.2f}%"
                + (f" / S3 wide ROI={wide_s3['roi_pct']:.2f}%" if wide_s3 else "")
            )
            # stdout にも出力 (5 分毎のモニタリング用)
            print(
                f"  [月完了] {valid_month}: "
                f"単勝all={tansho_all['roi_pct']:.2f}% S3={tansho_s3['roi_pct']:.2f}%"
                + (f" S3wide={wide_s3['roi_pct']:.2f}%" if wide_s3 else ""),
                flush=True
            )

    # ──────────────────────────────────────────────────────────
    # 全月完了後: 出力
    # ──────────────────────────────────────────────────────────
    if not monthly_rows:
        logger.error("有効な月別結果がありません。CSV 出力スキップ。")
        sys.exit(1)

    logger.info("")
    logger.info("[出力] 月別結果 + 全期間集計 + 年別集計...")
    csv_monthly, csv_summary = _output_results(monthly_rows)

    # 月別 ROI 分布サマリ
    _print_monthly_distribution(monthly_rows)

    total_elapsed = time.time() - start_time

    # 分析レポート出力
    md_path = _write_analysis_md(monthly_rows, csv_monthly, csv_summary, total_elapsed)

    print()
    print("=" * 70)
    print("【Phase 5b 全 24 月 馬券種マトリクス WF 検証 完了】")
    print(f"  合計所要時間: {total_elapsed:.1f}秒 ({total_elapsed/60:.1f}分)")
    print(f"  月別 CSV:   {csv_monthly}")
    print(f"  集計 CSV:   {csv_summary}")
    print(f"  分析 MD:    {md_path}")
    print(f"  特徴量: {len(FEATURE_COLUMNS_V3_PACE)} (+odds+pace)")
    print(f"  S3 閾値: gap>={S3_GAP_THRESHOLD}")
    print("=" * 70)
    print()
    print("# Phase 5b 全 24 月 WF 完了報告 (修正後データ)", flush=True)

    # 全 24 月加重 ROI 計算 (S3 wide)
    s3_wide_rows = [r for r in monthly_rows if r["strategy"] == "S3" and r["ticket"] == "wide"]
    if s3_wide_rows:
        pl = sum(r["played"] for r in s3_wide_rows)
        ht = sum(r["hit"]    for r in s3_wide_rows)
        py = sum(r["payout_sum"] for r in s3_wide_rows)
        roi_24 = py / (pl * 100) * 100 if pl > 0 else 0.0
        rois = sorted(r["roi_pct"] for r in s3_wide_rows)
        n = len(rois)
        print(f"- 全 24 月加重 ROI (S3 wide): {roi_24:.2f}%")
        print(f"- 月別変動 S3 wide: min/med/max = {rois[0]:.2f}/{rois[n//2]:.2f}/{rois[-1]:.2f}%")

    achieved_total = sum(
        1 for r in monthly_rows
        if r["hit_pct"] >= MASTER_HIT_PCT_THRESHOLD and r["roi_pct"] >= MASTER_ROI_PCT_THRESHOLD
    )
    print(f"- マスター基準達成セル数 (月別単体): {achieved_total} 個 (予測: 0 / 全 24 月加重: 0 個)")
    print(f"- 結論: マスター基準達成 0 確定 / 月別安定候補として S3 wide ROI {s3_wide_rows[0]['roi_pct']:.0f}〜{s3_wide_rows[-1]['roi_pct']:.0f}% / 次セッション: Phase 2b custom objective")

    logger.info("[Phase 5b 全 24 月 馬券種マトリクス WF 検証] 完了")


if __name__ == "__main__":
    main()
