"""M-3 Phase 5b: 馬券種マトリクス WF (単勝/馬連/ワイド/三連複/三連単)

+odds+pace モデル (114 特徴量) で予測 → ◎○▲△★☆ 割当 → 5 馬券種 × 2 戦略でROI評価。
WF 6 ヶ月 × 5 馬券種 × 2 戦略 = 60 セルで配当倍率効果を確認。

背景:
  前セッション (5/28) 本番 pred.json で SS×◎-○-△ 三連複 ROI 119.87% を発見したが
  学習リーク疑い。本スクリプトで WF backtest に再現するか確認する。

最良戦略 (Phase 5 確定):
  +odds+pace + S3 (gap>=0.10) = ROI 81.91% / hit% 53.03%
  → 本スクリプトで馬連/三連複の配当倍率効果を評価

データソース:
  - _load_ml_races(): 特徴量計算用 race dict (payouts: 単勝/複勝/馬連のみ)
  - data/results/*.json: 全馬券種 payouts (三連複/三連単/ワイドも含む)

重要な前提 (★★★):
  - src/ml/lgbm_model.py は絶対不変
  - ばんえい (venue_code="65") 除外 (feedback_banei_excluded.md)
  - git commit 禁止
  - 既存ファイル変更禁止

Usage:
  # フル実行 (6 ヶ月 WF / 約 25-30 分)
  python scripts/diag_phase5b_ticket_matrix.py

  # デバッグモード (1 月のみ: 2025-12)
  python scripts/diag_phase5b_ticket_matrix.py --debug

  # 特定月のみ
  python scripts/diag_phase5b_ticket_matrix.py --months 2025-12 2025-09

  # --help
  python scripts/diag_phase5b_ticket_matrix.py --help
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

# WF 検証月設定 (Phase 3 と同一 6 ヶ月)
WF_VALID_MONTHS = [
    {"valid": "2024-09", "train_start": "2024-01", "train_end": "2024-08"},
    {"valid": "2024-12", "train_start": "2024-04", "train_end": "2024-11"},
    {"valid": "2025-03", "train_start": "2024-07", "train_end": "2025-02"},
    {"valid": "2025-06", "train_start": "2024-10", "train_end": "2025-05"},
    {"valid": "2025-09", "train_start": "2025-01", "train_end": "2025-08"},
    {"valid": "2025-12", "train_start": "2025-04", "train_end": "2025-11"},
]

# LightGBM 学習パラメータ — Phase 3 と同一 (公正比較)
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

    # ファイル名: YYYYMMDD_results.json
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
# モデル学習 (Phase 3 と同じ設定)
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

    Args:
        booster:             学習済みモデル
        valid_races_info:    _build_train_valid_data の戻り値
        feature_cols:        特徴量列
        results_payouts_map: _load_results_payouts の戻り値 (race_id → norm payouts)
        gap_min:             S3 戦略の gap 閾値 (None = 全 race)
        strategy_id:         ログ表示用

    Returns:
        {ticket_type: {played, hit, payout_sum}}
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

        # ◎○▲△★☆ の horse_no 取得
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

        # ────────────────────────────────────────────────────────────
        # 単勝 ◎: 投資 100 円 / 1 点
        # ────────────────────────────────────────────────────────────
        payout_tansho = get_payout_for_combo(norm_p, "tansho", [honmei_no])
        results["tansho"]["played"] += 1
        if payout_tansho > 0:
            results["tansho"]["hit"] += 1
            results["tansho"]["payout"] += payout_tansho

        # ────────────────────────────────────────────────────────────
        # 馬連 ◎-○: 投資 100 円 / 1 点 (順序無関係)
        # ────────────────────────────────────────────────────────────
        payout_umaren = get_payout_for_combo(norm_p, "umaren", [honmei_no, taikou_no])
        results["umaren"]["played"] += 1
        if payout_umaren > 0:
            results["umaren"]["hit"] += 1
            results["umaren"]["payout"] += payout_umaren

        # ────────────────────────────────────────────────────────────
        # ワイド ◎-○: 投資 100 円 / 1 点
        # ワイドは 3 種類存在するが ◎-○ 1 点のみ購入。
        # payouts のワイドエントリー (最大 3 行) の中に ◎-○ combo が含まれるか確認。
        # ────────────────────────────────────────────────────────────
        payout_wide = get_payout_for_combo(norm_p, "wide", [honmei_no, taikou_no])
        results["wide"]["played"] += 1
        if payout_wide > 0:
            results["wide"]["hit"] += 1
            results["wide"]["payout"] += payout_wide

        # ────────────────────────────────────────────────────────────
        # 三連複 ◎-○-▲: 投資 100 円 / 1 点 (順序無関係)
        # ────────────────────────────────────────────────────────────
        payout_sanrenpuku = get_payout_for_combo(norm_p, "sanrenpuku", [honmei_no, taikou_no, tanana_no])
        results["sanrenpuku"]["played"] += 1
        if payout_sanrenpuku > 0:
            results["sanrenpuku"]["hit"] += 1
            results["sanrenpuku"]["payout"] += payout_sanrenpuku

        # ────────────────────────────────────────────────────────────
        # 三連単 ◎-○-▲: 投資 100 円 / 1 点 (順序固定: 1着◎ 2着○ 3着▲)
        # ────────────────────────────────────────────────────────────
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
# 月別・全期間 出力
# ============================================================

def _output_results(
    monthly_rows: List[dict],
) -> Tuple[str, str]:
    """
    月別 × セル (60 行) と全期間集計 (10 行) を CSV + stdout に出力する。

    Returns:
        (monthly_csv_path, summary_csv_path)
    """
    os.makedirs(DIAG_DIR, exist_ok=True)
    monthly_csv  = os.path.join(DIAG_DIR, "phase5b_ticket_monthly.csv")
    summary_csv  = os.path.join(DIAG_DIR, "phase5b_ticket_summary.csv")

    # ── 月別 CSV ──
    monthly_fieldnames = [
        "valid_month", "strategy", "ticket",
        "played", "hit", "hit_pct", "roi_pct", "payout_sum",
        "judgment",
    ]

    def _judgment(hit_pct, roi_pct):
        if hit_pct >= MASTER_HIT_PCT_THRESHOLD and roi_pct >= MASTER_ROI_PCT_THRESHOLD:
            return "✅ マスター基準達成"
        if roi_pct >= 100.0:
            return "△ ROI 100%+"
        return ""

    with open(monthly_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=monthly_fieldnames)
        writer.writeheader()
        writer.writerows(monthly_rows)
    logger.info(f"    月別 CSV 保存: {monthly_csv} ({len(monthly_rows)} 行)")

    # ── 全期間集計 ──
    summary_rows = []
    achieved_cells = []

    for strategy in STRATEGIES:
        sid = strategy["id"]
        for tt in TICKET_TYPES:
            # この strategy × ticket の全月行を抽出
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
                achieved_cells.append((sid, tt, hit_pct_w, roi_pct_w, j))

    summary_fieldnames = [
        "strategy", "ticket", "played_total", "hit_total",
        "hit_pct_weighted", "roi_pct_weighted", "payout_sum_total", "judgment",
    ]
    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)
    logger.info(f"    集計 CSV 保存: {summary_csv} ({len(summary_rows)} 行)")

    # ── stdout 出力 ──
    print()
    print("=" * 110)
    print("【Phase 5b 馬券種マトリクス WF — 全期間集計】")
    print("=" * 110)
    print(f"  マスター基準: hit% >= {MASTER_HIT_PCT_THRESHOLD:.0f}% AND ROI >= {MASTER_ROI_PCT_THRESHOLD:.0f}%")
    print()
    print(f"  {'戦略':<10} {'馬券種':<14} {'played':>8} {'hit':>6} {'hit%':>8} {'ROI%':>9} {'判定':<20}")
    print(f"  {'-'*10} {'-'*14} {'-'*8} {'-'*6} {'-'*8} {'-'*9} {'-'*20}")

    for r in summary_rows:
        j = r["judgment"]
        mark = "  ←★★★" if "✅" in j else ("  ←" if "△" in j else "")
        print(
            f"  {r['strategy']:<10} {r['ticket']:<14} {r['played_total']:>8} "
            f"{r['hit_total']:>6} {r['hit_pct_weighted']:>7.2f}% {r['roi_pct_weighted']:>8.2f}% "
            f"{j:<20}{mark}"
        )
    print("=" * 110)

    # ── マスター基準達成セルのハイライト ──
    print()
    if achieved_cells:
        print("【✅ マスター基準達成セル一覧】")
        for sid, tt, hit_pct, roi_pct, j in achieved_cells:
            print(f"  strategy={sid}, ticket={tt}: hit%={hit_pct:.2f}%, ROI={roi_pct:.2f}%  ← {j}")
    else:
        print("【❌ マスター基準達成セルなし】")
        # ROI 100%+ セルがあれば参考表示
        near_cells = [(r["strategy"], r["ticket"], r["hit_pct_weighted"], r["roi_pct_weighted"])
                      for r in summary_rows if r["roi_pct_weighted"] >= 90.0]
        if near_cells:
            print("  参考 (ROI >= 90%):")
            for sid, tt, hp, rp in sorted(near_cells, key=lambda x: -x[3]):
                print(f"    strategy={sid}, ticket={tt}: hit%={hp:.2f}%, ROI={rp:.2f}%")

    print()

    return monthly_csv, summary_csv


# ============================================================
# メイン
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 5b: 馬券種マトリクス WF 検証 — "
            "+odds+pace モデル × 5 馬券種 × 2 戦略 (60 セル)"
        )
    )
    parser.add_argument(
        "--months",
        nargs="+",
        metavar="YYYY-MM",
        help="実行する検証月を指定 (例: --months 2025-12 2025-09)。省略時は全 6 ヶ月",
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
    logger.info("[Phase 5b 馬券種マトリクス WF 検証] 開始")
    logger.info(f"  特徴量数: {len(FEATURE_COLUMNS_V3_PACE)} (+odds+pace)")
    logger.info(f"  S3 閾値: gap>={S3_GAP_THRESHOLD}")
    logger.info(f"  馬券種: {TICKET_TYPES}")
    logger.info(f"  戦略: {[s['id'] for s in STRATEGIES]}")
    logger.info(f"  検証月: {[c['valid'] for c in WF_VALID_MONTHS]}")
    logger.info("=" * 70)

    # 実行月フィルタ
    wf_configs = WF_VALID_MONTHS
    if args.debug:
        wf_configs = [c for c in WF_VALID_MONTHS if c["valid"] == "2025-12"]
        logger.info("  デバッグモード: 2025-12 のみ実行")
    elif args.months:
        wf_configs = [c for c in WF_VALID_MONTHS if c["valid"] in args.months]
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
        logger.error("  ❌ レースデータが空です。処理中断。")
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
        logger.info("")
        logger.info(
            f"[{bar}] {pct:.0f}% — 月 {month_idx+1}/{total_months}: valid={valid_month} / elapsed={elapsed_total:.0f}秒"
        )
        logger.info(f"  学習期間: {train_start} 〜 {train_end}")
        logger.info("=" * 70)

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
            logger.error(f"  ❌ データ構築エラー (valid={valid_month}): {e}")
            import traceback; traceback.print_exc()
            continue

        if len(ytr) == 0 or len(vri) == 0:
            logger.warning(f"  ⚠️ valid={valid_month}: 学習/検証データが空。スキップ")
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
            logger.error(f"  ❌ モデル学習エラー (valid={valid_month}): {e}")
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
                logger.error(f"  ❌ 馬券評価エラー (valid={valid_month}, strategy={sid}): {e}")
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
                    judgment = "✅ マスター基準達成"
                elif roi_pct >= 100.0:
                    judgment = "△ ROI 100%+"

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
                    print(f"  ★ {judgment}: {valid_month} / strategy={sid} / ticket={tt} — hit%={hit_pct:.2f}%, ROI={roi_pct:.2f}%")

        logger.info(f"  Step D 完了: {time.time()-t_step:.1f}秒")

        # 月別中間サマリ (単勝 all vs S3 を基準にプログレス確認)
        tansho_all = next((r for r in monthly_rows if r["valid_month"] == valid_month and r["strategy"] == "all" and r["ticket"] == "tansho"), None)
        tansho_s3  = next((r for r in monthly_rows if r["valid_month"] == valid_month and r["strategy"] == "S3"  and r["ticket"] == "tansho"), None)
        if tansho_all and tansho_s3:
            logger.info(
                f"  ✅ {valid_month} 完了: "
                f"単勝 all ROI={tansho_all['roi_pct']:.2f}% / S3 ROI={tansho_s3['roi_pct']:.2f}%"
            )

    # ──────────────────────────────────────────────────────────
    # 全月完了後: 出力
    # ──────────────────────────────────────────────────────────
    if not monthly_rows:
        logger.error("❌ 有効な月別結果がありません。CSV 出力スキップ。")
        sys.exit(1)

    logger.info("")
    logger.info("[出力] 月別結果 + 全期間集計...")
    csv_monthly, csv_summary = _output_results(monthly_rows)

    total_elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print("【Phase 5b 馬券種マトリクス WF 検証 完了】")
    print(f"  合計所要時間: {total_elapsed:.1f}秒 ({total_elapsed/60:.1f}分)")
    print(f"  月別 CSV: {csv_monthly}")
    print(f"  集計 CSV: {csv_summary}")
    print(f"  特徴量: {len(FEATURE_COLUMNS_V3_PACE)} (+odds+pace)")
    print(f"  S3 閾値: gap>={S3_GAP_THRESHOLD}")
    print("=" * 70)

    logger.info("[Phase 5b 馬券種マトリクス WF 検証] 完了")


if __name__ == "__main__":
    main()
