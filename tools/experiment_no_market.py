# -*- coding: utf-8 -*-
"""オッズ・人気関連特徴量を全て除外した場合のモデル比較実験"""
import sys, os, json, pickle, warnings
warnings.filterwarnings("ignore")

import numpy as np
import lightgbm as lgb
from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.ml.lgbm_model import (
    FEATURE_COLUMNS, CATEGORICAL_FEATURES,
    _load_ml_races, _load_horse_sire_map, _extract_features,
    _add_race_relative_features,
    RollingStatsTracker, RollingSireTracker,
    SURFACE_MAP, MODEL_DIR, _smile_key_ml,
    build_venue_time_baselines,
)

# オッズ・人気に関連する全特徴量
ODDS_POPULARITY_FEATURES = [
    "current_odds_log",       # 当日単勝オッズ(対数)
    "current_popularity",     # 当日人気順位
    "odds_popularity_gap",    # オッズ-人気乖離
    "odds_log_drift",         # オッズ変動(対数)
    "prev_odds_1",            # 前走オッズ
    "prev_odds_2",            # 前々走オッズ
]
FEAT_NO_MARKET = [f for f in FEATURE_COLUMNS if f not in ODDS_POPULARITY_FEATURES]
CAT_NO_MARKET = [f for f in CATEGORICAL_FEATURES if f not in ODDS_POPULARITY_FEATURES]

# --- SHAP group定義 (オッズ・人気除外) ---
SHAP_GROUPS = {
    "能力":   ["dev_run1","dev_run2","chakusa_index_avg3","trend_position_slope","trend_deviation_slope",
               "horse_win_rate","horse_place_rate","horse_avg_finish","horse_last_finish","horse_runs",
               "venue_sim_place_rate","venue_sim_win_rate","venue_sim_avg_finish","venue_sim_runs","venue_sim_n_venues",
               "same_dir_place_rate","same_dir_runs","horse_form_rank_in_race","horse_place_rank_in_race",
               "venue_sim_rank_in_race","horse_form_zscore_in_race","class_change","prev_grade_code",
               # Batch1: ②コーナー別位置変化 + ③着差指数の再設計
               "avg_pos_change_3to4c","pos_change_3to4c_last","avg_pos_change_1to4c","front_hold_rate",
               "margin_norm_last","margin_norm_avg3"],
    "展開":   ["ml_pos_est","horse_running_style","ml_l3f_est","speed_sec_per_m_est",
               "speed_index_last","speed_index_avg3","speed_index_best3",
               "place_rate_fast_pace","place_rate_slow_pace","pace_pref_score",
               "pace_count_fast","pace_count_slow",
               "pace_norm_last","pace_norm_avg3",
               "front_runner_count_in_race","pace_pressure_index","style_pace_affinity"],
    "騎手":   ["is_jockey_change","kishu_pattern_code","jockey_win_rate","jockey_place_rate","jockey_runs",
               "jockey_win_rate_90d","jockey_place_rate_90d","jockey_venue_wr","jockey_surface_wr","jockey_dist_wr",
               "jockey_surf_dist_wr","jockey_surf_dist_pr",
               "jockey_sim_venue_wr","jockey_sim_venue_pr",
               "jockey_sim_venue_dist_wr","jockey_sim_venue_dist_pr",
               "jockey_place_rank_in_race","jockey_place_zscore_in_race","jockey_wp_ratio"],
    "調教師": ["trainer_win_rate","trainer_place_rate","trainer_runs","trainer_win_rate_90d","trainer_place_rate_90d",
               "trainer_venue_wr","trainer_surface_wr","trainer_dist_wr","jt_combo_wr","jt_combo_runs",
               "trainer_surf_dist_wr","trainer_surf_dist_pr",
               "trainer_sim_venue_wr","trainer_sim_venue_pr",
               "trainer_sim_venue_dist_wr","trainer_sim_venue_dist_pr",
               "trainer_place_rank_in_race","trainer_wp_ratio"],
    "コース": ["surface","distance","condition","is_jra","venue_straight_m","venue_slope","venue_first_corner",
               "venue_corner_type","venue_direction","horse_condition_match","grade_code","month",
               "venue_code","venue_sim_runs","field_count","gate_venue_wr","style_surface_wr","gate_style_wr"],
    "体型":   ["horse_weight","weight_change","is_long_break","horse_days_since","gate_no","horse_no",
               "sex_code","age","weight_kg"],
    "血統":   ["sire_win_rate","sire_place_rate","bms_win_rate","bms_place_rate","sire_surf_wr","sire_smile_wr","bms_surf_wr",
               "sire_x_bms_place_rate","sire_bms_wr",
               "sire_surf_dist_wr","sire_surf_dist_pr",
               "sire_sim_venue_wr","sire_sim_venue_pr",
               "sire_sim_venue_dist_wr","sire_sim_venue_dist_pr",
               "bms_surf_dist_wr","bms_surf_dist_pr",
               "bms_sim_venue_wr","bms_sim_venue_pr",
               "bms_sim_venue_dist_wr","bms_sim_venue_dist_pr"],
}

JP_NAMES = {
    "current_odds_log": "単勝オッズ(対数)", "current_popularity": "人気順位",
    "odds_popularity_gap": "オッズ-人気乖離", "odds_log_drift": "オッズ変動(対数)",
    "venue_sim_rank_in_race": "競馬場適性レース内順位", "venue_sim_place_rate": "類似競馬場複勝率",
    "horse_form_zscore_in_race": "調子Zスコア(レース内)", "horse_form_rank_in_race": "調子順位(レース内)",
    "prev_odds_1": "前走オッズ", "horse_place_rate": "馬複勝率",
    "horse_place_rank_in_race": "馬複勝率レース内順位", "horse_runs": "馬出走回数",
    "field_count": "出走頭数", "same_dir_place_rate": "同回り方向複勝率",
    "jockey_place_rate": "騎手複勝率", "dev_run1": "偏差値(直近1走)",
    "jockey_place_rate_90d": "騎手複勝率(90日)", "venue_sim_avg_finish": "類似競馬場平均着順",
    "horse_last_finish": "前走着順", "jockey_place_zscore_in_race": "騎手Zスコア(レース内)",
    "venue_sim_runs": "類似競馬場出走数", "horse_running_style": "脚質",
    "trainer_runs": "調教師出走回数", "venue_code": "競馬場コード",
    "same_dir_runs": "同回り方向出走数", "jockey_venue_wr": "騎手競馬場別勝率",
    "prev_odds_2": "前々走オッズ", "trainer_place_rate": "調教師複勝率",
    "horse_days_since": "前走からの日数", "horse_avg_finish": "馬平均着順",
    "age": "年齢", "gate_venue_wr": "枠×競馬場勝率",
    "trend_position_slope": "着順トレンド傾き", "jockey_dist_wr": "騎手距離別勝率",
    "horse_win_rate": "馬勝率", "grade_code": "クラスコード",
    "jt_combo_place_rate_30d": "騎手×調教師複勝率(30日)", "dev_run2": "偏差値(直近2走)",
    "ml_pos_est": "ML位置取り推定", "venue_sim_win_rate": "類似競馬場勝率",
    "trainer_place_rate_90d": "調教師複勝率(90日)", "jockey_runs": "騎手出走回数",
    "jockey_win_rate_90d": "騎手勝率(90日)", "venue_straight_m": "直線距離(m)",
    "is_jra": "JRA/地方区分", "jockey_place_rank_in_race": "騎手複勝率レース内順位",
    "style_surface_wr": "脚質×馬場勝率", "weight_change": "馬体重増減",
    "is_jockey_change": "騎手乗替フラグ", "trainer_dist_wr": "調教師距離別勝率",
    "trainer_venue_wr": "調教師競馬場別勝率", "sire_surf_wr": "父馬場別勝率",
    "jockey_wp_ratio": "騎手勝率/複勝率比", "speed_sec_per_m_est": "スピード指数(秒/m推定)",
    "jockey_surface_wr": "騎手馬場別勝率", "horse_weight": "馬体重",
    "sire_place_rate": "父産駒複勝率", "class_change": "クラス変動",
    "gate_style_wr": "枠×脚質勝率", "trainer_win_rate_90d": "調教師勝率(90日)",
    "relative_weight_kg": "相対斤量", "jt_combo_wr": "騎手×調教師勝率",
    "bms_win_rate": "母父産駒勝率", "bms_surf_wr": "母父馬場別勝率",
    "weight_kg_trend_3run": "斤量トレンド(3走)", "sire_win_rate": "父産駒勝率",
    "sire_smile_wr": "父距離別勝率", "venue_first_corner": "最初のコーナーまでの距離",
    "condition": "馬場状態", "horse_no": "馬番", "jockey_win_rate": "騎手勝率",
    "jt_combo_runs": "騎手×調教師コンビ出走数", "jt_combo_wr_30d": "騎手×調教師勝率(30日)",
    "kishu_pattern_code": "騎手パターンコード", "ml_l3f_est": "ML上がり3F推定",
    "trainer_wp_ratio": "調教師勝率/複勝率比", "prev_grade_code": "前走クラスコード",
    "month": "月", "horse_condition_match": "馬場適性一致",
    "surface": "芝/ダート", "distance": "距離", "venue_slope": "コース勾配",
    "venue_corner_type": "コーナー形状", "venue_direction": "回り方向",
    "gate_no": "枠番", "sex_code": "性別", "weight_kg": "斤量",
    "trainer_win_rate": "調教師勝率", "trainer_surface_wr": "調教師馬場別勝率",
    "venue_sim_n_venues": "類似競馬場数", "bms_place_rate": "母父産駒複勝率",
    "trend_deviation_slope": "偏差値トレンド傾き", "chakusa_index_avg3": "着差指数(3走平均)",
    "is_long_break": "長期休養フラグ", "trainer_place_rank_in_race": "調教師複勝率レース内順位",
    # Batch1: ②コーナー別位置変化
    "avg_pos_change_3to4c": "3角→4角位置前進量(平均)",
    "pos_change_3to4c_last": "前走3角→4角位置前進量",
    "avg_pos_change_1to4c": "1角→4角総移動量(平均)",
    "front_hold_rate": "先行維持率(1角→4角)",
    # Batch1: ③着差指数の再設計
    "margin_norm_last": "正規化着差(前走)",
    "margin_norm_avg3": "正規化着差(3走平均)",
    # Batch2: ①タイム指数
    "speed_index_last": "タイム指数(前走)",
    "speed_index_avg3": "タイム指数(3走平均)",
    "speed_index_best3": "タイム指数(3走最高)",
    # Batch3: ④ニック理論
    "sire_x_bms_place_rate": "父×母父ニック複勝率",
    "sire_bms_wr": "父×母父ニック勝率",
    # 類似場加重×条件別: 騎手
    "jockey_surf_dist_wr": "騎手(馬場×距離)勝率", "jockey_surf_dist_pr": "騎手(馬場×距離)複勝率",
    "jockey_sim_venue_wr": "騎手(類似場加重)勝率", "jockey_sim_venue_pr": "騎手(類似場加重)複勝率",
    "jockey_sim_venue_dist_wr": "騎手(類似場×距離)勝率", "jockey_sim_venue_dist_pr": "騎手(類似場×距離)複勝率",
    # 類似場加重×条件別: 調教師
    "trainer_surf_dist_wr": "調教師(馬場×距離)勝率", "trainer_surf_dist_pr": "調教師(馬場×距離)複勝率",
    "trainer_sim_venue_wr": "調教師(類似場加重)勝率", "trainer_sim_venue_pr": "調教師(類似場加重)複勝率",
    "trainer_sim_venue_dist_wr": "調教師(類似場×距離)勝率", "trainer_sim_venue_dist_pr": "調教師(類似場×距離)複勝率",
    # 類似場加重×条件別: 父馬
    "sire_surf_dist_wr": "父産駒(馬場×距離)勝率", "sire_surf_dist_pr": "父産駒(馬場×距離)複勝率",
    "sire_sim_venue_wr": "父産駒(類似場加重)勝率", "sire_sim_venue_pr": "父産駒(類似場加重)複勝率",
    "sire_sim_venue_dist_wr": "父産駒(類似場×距離)勝率", "sire_sim_venue_dist_pr": "父産駒(類似場×距離)複勝率",
    # 類似場加重×条件別: 母父
    "bms_surf_dist_wr": "母父産駒(馬場×距離)勝率", "bms_surf_dist_pr": "母父産駒(馬場×距離)複勝率",
    "bms_sim_venue_wr": "母父産駒(類似場加重)勝率", "bms_sim_venue_pr": "母父産駒(類似場加重)複勝率",
    "bms_sim_venue_dist_wr": "母父産駒(類似場×距離)勝率", "bms_sim_venue_dist_pr": "母父産駒(類似場×距離)複勝率",
    # Batch4: ⑤道中タイムペース適性
    "place_rate_fast_pace": "ハイペース時複勝率",
    "place_rate_slow_pace": "スローペース時複勝率",
    "pace_pref_score": "展開適性スコア(ハイ-スロー)",
    "pace_count_fast": "ハイペース出走数",
    "pace_count_slow": "スローペース出走数",
    "pace_norm_last": "前走ペース指標(連続値)",
    "pace_norm_avg3": "直近3走ペース指標平均",
    "front_runner_count_in_race": "逃げ先行馬数(フィールド内)",
    "pace_pressure_index": "ペース圧力指数(逃げ比率)",
    "style_pace_affinity": "脚質×展開相性スコア",
}


def main():
    print("=" * 60)
    print("  オッズ・人気関連 全除外 比較実験")
    print("=" * 60)
    print(f"全特徴量: {len(FEATURE_COLUMNS)}個")
    print(f"除外後: {len(FEAT_NO_MARKET)}個 (除外: {ODDS_POPULARITY_FEATURES})")
    print()

    # データ読み込み
    races = _load_ml_races()
    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    split_idx = max(1, len(all_dates) - 30)
    split_date = all_dates[split_idx]

    print(f"学習期間: {all_dates[0]} ~ {all_dates[split_idx-1]}")
    print(f"検証期間: {split_date} ~ {all_dates[-1]}")

    sire_map = _load_horse_sire_map()
    # タイム指数2パス化: 全データから事前に基準値を構築 → 精度向上
    print("タイム指数・道中タイム基準値を全データから事前構築中...")
    prebuilt = build_venue_time_baselines(races)
    print(f"  走破タイムキー数: {len(prebuilt)-1}, 道中タイムキー数: {len(prebuilt.get('_chunkan', {}))}")
    tracker = RollingStatsTracker(prebuilt_time_baselines=prebuilt)
    sire_tracker = RollingSireTracker()

    all_train_rows = []
    all_valid_groups = []

    for race in races:
        date_str = race.get("date", "")
        is_valid = date_str >= split_date
        race_feats, race_labels = [], []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            h_with_sire = dict(h, sire_id=sid, bms_id=bid)
            feat = _extract_features(h_with_sire, race, tracker, sire_tracker)
            race_feats.append(feat)
            race_labels.append(1 if fp <= 3 else 0)
        if race_feats:
            _add_race_relative_features(race_feats)
            if is_valid:
                all_valid_groups.append((race_feats, race_labels))
            else:
                for feat, lbl in zip(race_feats, race_labels):
                    all_train_rows.append((feat, lbl))
        tracker.update_race(race)
        sire_tracker.update_race(race, sire_map)

    train_X_rows = [f for f, l in all_train_rows]
    train_y = np.array([l for f, l in all_train_rows], dtype=np.int32)
    valid_X_rows = [f for fs, ls in all_valid_groups for f in fs]
    valid_y = np.array([l for fs, ls in all_valid_groups for l in ls], dtype=np.int32)
    valid_race_sizes = [len(fs) for fs, ls in all_valid_groups]

    print(f"Train: {len(train_y)} samples / Valid: {len(valid_y)} samples")
    print()

    # Optunaパラメータ
    with open(os.path.join(MODEL_DIR, "best_lgbm_params.json"), encoding="utf-8") as f:
        _optuna_params = json.load(f).get("best_params", {})

    params = {
        "objective": "binary",
        "metric": ["binary_logloss", "auc"],
        "boosting_type": "gbdt",
        "num_leaves": _optuna_params.get("num_leaves", 63),
        "learning_rate": _optuna_params.get("learning_rate", 0.02),
        "feature_fraction": _optuna_params.get("feature_fraction", 0.8),
        "bagging_fraction": _optuna_params.get("bagging_fraction", 0.8),
        "bagging_freq": _optuna_params.get("bagging_freq", 5),
        "min_child_samples": _optuna_params.get("min_child_samples", 50),
        "lambda_l1": _optuna_params.get("lambda_l1", 0.1),
        "lambda_l2": _optuna_params.get("lambda_l2", 1.0),
        "max_depth": _optuna_params.get("max_depth", 7),
        "verbose": -1,
        "seed": 42,
        "is_unbalance": True,
    }

    def to_np(rows, feat_cols):
        mat = []
        for f in rows:
            mat.append([float(f[c]) if f[c] is not None else float("nan") for c in feat_cols])
        return np.array(mat, dtype=np.float32)

    def evaluate(model, X_valid, y_valid, valid_race_sizes, label):
        y_pred = model.predict(X_valid)
        auc = roc_auc_score(y_valid, y_pred)
        logloss = log_loss(y_valid, y_pred)
        brier = brier_score_loss(y_valid, y_pred)
        correct_top1, correct_top3, total_eval = 0, 0, 0
        idx = 0
        for g in valid_race_sizes:
            if g < 3:
                idx += g
                continue
            rp = y_pred[idx:idx + g]
            rt = y_valid[idx:idx + g]
            if rt[np.argmax(rp)] == 1:
                correct_top1 += 1
            if any(rt[i] == 1 for i in np.argsort(rp)[-3:]):
                correct_top3 += 1
            total_eval += 1
            idx += g
        print(f"=== {label} ===")
        print(f"AUC:         {auc:.4f}")
        print(f"LogLoss:     {logloss:.4f}")
        print(f"Brier Score: {brier:.4f}")
        print(f"Best Iter:   {model.best_iteration}")
        print(f"Top1的中率:  {correct_top1/max(total_eval,1)*100:.1f}% ({correct_top1}/{total_eval})")
        print(f"Top3的中率:  {correct_top3/max(total_eval,1)*100:.1f}% ({correct_top3}/{total_eval})")
        print()
        return auc, logloss, brier, correct_top1/max(total_eval,1), correct_top3/max(total_eval,1)

    # (A) 全特徴量モデル
    print("=" * 55)
    print("(A) 全特徴量モデル 学習中...")
    X_tr_full = to_np(train_X_rows, FEATURE_COLUMNS)
    X_va_full = to_np(valid_X_rows, FEATURE_COLUMNS)
    dtrain_full = lgb.Dataset(X_tr_full, label=train_y, feature_name=FEATURE_COLUMNS,
                              categorical_feature=CATEGORICAL_FEATURES, free_raw_data=False)
    dvalid_full = lgb.Dataset(X_va_full, label=valid_y, feature_name=FEATURE_COLUMNS,
                              categorical_feature=CATEGORICAL_FEATURES, reference=dtrain_full, free_raw_data=False)
    model_full = lgb.train(params, dtrain_full, num_boost_round=3000,
                           valid_sets=[dtrain_full, dvalid_full], valid_names=["train", "valid"],
                           callbacks=[lgb.log_evaluation(period=9999), lgb.early_stopping(stopping_rounds=100)])
    res_full = evaluate(model_full, X_va_full, valid_y, valid_race_sizes, "(A) 全特徴量")

    # (B) 市場特徴量なしモデル
    print("=" * 55)
    print("(B) 市場特徴量なしモデル 学習中...")
    X_tr_nm = to_np(train_X_rows, FEAT_NO_MARKET)
    X_va_nm = to_np(valid_X_rows, FEAT_NO_MARKET)
    dtrain_nm = lgb.Dataset(X_tr_nm, label=train_y, feature_name=FEAT_NO_MARKET,
                            categorical_feature=CAT_NO_MARKET, free_raw_data=False)
    dvalid_nm = lgb.Dataset(X_va_nm, label=valid_y, feature_name=FEAT_NO_MARKET,
                            categorical_feature=CAT_NO_MARKET, reference=dtrain_nm, free_raw_data=False)
    model_nm = lgb.train(params, dtrain_nm, num_boost_round=3000,
                         valid_sets=[dtrain_nm, dvalid_nm], valid_names=["train", "valid"],
                         callbacks=[lgb.log_evaluation(period=9999), lgb.early_stopping(stopping_rounds=100)])
    res_nm = evaluate(model_nm, X_va_nm, valid_y, valid_race_sizes, "(B) 市場特徴量なし")

    # 比較
    print("=" * 60)
    print("  比較サマリー")
    print("=" * 60)
    labels_list = ["AUC", "LogLoss", "Brier", "Top1的中率", "Top3的中率"]
    for i, name in enumerate(labels_list):
        a, b = res_full[i], res_nm[i]
        diff = b - a
        if i >= 3:
            print(f"  {name:<12} (A){a*100:>8.1f}%   (B){b*100:>8.1f}%   差分{diff*100:>+7.1f}%")
        else:
            print(f"  {name:<12} (A){a:>8.4f}    (B){b:>8.4f}    差分{diff:>+8.4f}")

    # (B) 特徴量重要度 - 全件
    print()
    print("=" * 60)
    print("  (B) 市場なしモデル 全特徴量重要度")
    print("=" * 60)
    imp_nm = model_nm.feature_importance(importance_type="gain")
    imp_nm_split = model_nm.feature_importance(importance_type="split")
    total_gain = sum(imp_nm)

    results = sorted(zip(FEAT_NO_MARKET, imp_nm, imp_nm_split), key=lambda x: -x[1])

    # カテゴリ別
    feat_pct = {n: g / total_gain * 100 if total_gain > 0 else 0 for n, g in zip(FEAT_NO_MARKET, imp_nm)}
    print()
    print("--- カテゴリ別寄与度 ---")
    group_sums = []
    for g, feats in SHAP_GROUPS.items():
        total = sum(feat_pct.get(f, 0) for f in feats)
        group_sums.append((g, len(feats), total))
    group_sums.sort(key=lambda x: x[2], reverse=True)
    for g, n, t in group_sums:
        print(f"  {g:<8} {t:>7.2f}%  ({n}特徴量)")

    # 全件リスト
    print()
    print("--- 全特徴量リスト ---")
    # カテゴリ逆引き
    feat_to_group = {}
    for g, feats in SHAP_GROUPS.items():
        for f in feats:
            feat_to_group[f] = g

    zero_count = 0
    for i, (name, gain, split) in enumerate(results, 1):
        pct = gain / total_gain * 100 if total_gain > 0 else 0
        jp = JP_NAMES.get(name, name)
        grp = feat_to_group.get(name, "?")
        if split == 0:
            zero_count += 1
        print(f"  {i:>3}. {jp:<25} ({name:<35}) [{grp}] {pct:>7.2f}%  split={int(split)}")

    print(f"\n  Gain=0: {zero_count}個 / 実効: {len(FEAT_NO_MARKET)-zero_count}個")


if __name__ == "__main__":
    main()
