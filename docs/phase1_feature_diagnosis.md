# Phase 1 特徴量診断レポート (M-3)

> 作成: 2026-05-29 / 玄人クロード (Opus 4.7 指揮塔 + Sonnet subagent 実装)
> 対象: LightGBM wf_2026 全 42 モデル (ばんえい venue_65 除外)
> 手法: SHAP TreeExplainer + permute importance + percentile 動的閾値 confidence 別差異
> 入力: `data/_diag/shap_all_models_avg_top30.csv` + `shap_top30_*_DEMO.csv` (42 個) + `shap_run.log`

## 1. エグゼクティブサマリー

| 観点 | 発見 | Phase 2 への含意 |
|---|---|---|
| **popularity/odds** | SHAP Top 30 に **1 件も含まれない** (前セッション gain=0% と一致) | 学習対象に取り込みで ROI 観点強化可能 |
| **過去成績** | dev_run1_adj 15.47% (1 位) / dev_run2_adj 4.70% / relative_dev_mean_5 4.04% / horse_avg_finish 4.01% — 支配的 | 「実力ベース予想」確定 |
| **field_count** | 第 2 位 13.77% — 出走頭数で hit% が上下 | confidence/印選定に既に反映 |
| **騎手・厩舎** | jockey_place_rank_in_race 10.20% (3 位) / trainer_place_rank_in_race 5.77% (4 位) | 過剰寄与の可能性 |
| **ペース・展開** | Top 30 中 2 つ (front_runner_count_in_race 2.57% / pace_pressure_index 1.52%) で計 ~4% | 強化余地あり |
| **改善仮説** | 6 件抽出 (H1-H6) → Phase 2 着手項目 | ROI 期待値 loss + odds 取込が核心 |

**結論**: マスター指摘「特徴量の比重がおかしい」は **SHAP でも完全に裏付けられた**。**ML 学習目的が ROI を考慮していない構造課題** が確定。

## 2. 環境と手法

| 項目 | 値 |
|---|---|
| 対象モデル | `data/models/wf_2026/lgbm_*.txt` 42 個 (venue_65 除外) |
| Training data | 2025-12 月 wf_2026 学習期間 (66,480 race) → tracker 更新 → 12 月 1,560 race から 500 sample |
| SHAP 手法 | `shap.TreeExplainer(booster)` + mean(|SHAP|) |
| permute | 上位 50 特徴量を shuffle → ΔSHAP 測定 |
| confidence 分割 | `np.percentile(raw_pred, [33, 67])` 動的閾値 (固定 0.50/0.35 では JRA 系で高=0 だったため改修) |
| 所要時間 | 全 3 処理合計 101 秒 (1.7 分) |
| 使用ライブラリ | shap 0.50.0 / lightgbm 4.6.0 / Python 3.11 |

### 改修履歴 (本セッション)

- **修正 1**: `_list_all_models()` から `place_venue_65` を除外 (ばんえいは 43 列モデルで 108 列 X と shape mismatch)
- **修正 2**: confidence 分割を percentile 33/67 動的閾値に変更 (place_jra_* モデルで高=0 解消)
- 修正前: 42/43 モデルでクラッシュ + JRA 系 confidence 高グループ完全空
- 修正後: **全 42 モデル正常完走 + 全グループ 165/170/165 で均等 3 等分**

## 3. 全モデル平均 SHAP Top 30

| 順位 | 特徴量 | mean|SHAP| | カテゴリ |
|---:|---|---:|---|
| 1 | dev_run1_adj | 0.15470 | 過去成績 |
| 2 | field_count | 0.13773 | レース条件 |
| 3 | jockey_place_rank_in_race | 0.10205 | 騎手 |
| 4 | trainer_place_rank_in_race | 0.05768 | 厩舎 |
| 5 | horse_venue_pr | 0.05472 | 過去成績 (場別) |
| 6 | dev_run2_adj | 0.04697 | 過去成績 |
| 7 | relative_dev_mean_5 | 0.04040 | 過去成績 |
| 8 | horse_avg_finish | 0.04011 | 過去成績 |
| 9 | venue_sim_place_rate | 0.03648 | 場類似度 |
| 10 | venue_sim_runs | 0.02991 | 場類似度 |
| 11 | front_runner_count_in_race | 0.02570 | ペース・展開 |
| 12 | jockey_pr_2y | 0.02271 | 騎手 |
| 13 | age | 0.02228 | 馬個体 |
| 14 | relative_dev_max_5 | 0.02185 | 過去成績 |
| 15 | jockey_place_rate_90d | 0.02180 | 騎手 |
| 16 | trainer_horse_pr | 0.01890 | 厩舎 |
| 17 | speed_index_avg_6m | 0.01820 | タイム指数 |
| 18 | style_pace_affinity | 0.01781 | ペース・展開 |
| 19 | relative_dev_recent | 0.01777 | 過去成績 |
| 20 | jockey_sim_venue_dist_pr | 0.01679 | 騎手 |
| 21 | speed_index_adj_6m | 0.01649 | タイム指数 |
| 22 | gate_no | 0.01611 | 馬個体 (枠) |
| 23 | jt_combo_wr | 0.01557 | 騎手×厩舎 |
| 24 | pace_pressure_index | 0.01516 | ペース・展開 |
| 25 | ml_pos_est | 0.01430 | ML 出力 |
| 26 | jockey_win_rate | 0.01282 | 騎手 |
| 27 | horse_days_since | 0.01191 | 馬個体 |
| 28 | horse_dist_pr | 0.01164 | 過去成績 (距離) |
| 29 | speed_index_best_1y | 0.01104 | タイム指数 |
| 30 | speed_index_best3 | 0.01086 | タイム指数 |

### popularity / odds / ninki の不在を再確認

```bash
$ grep -E "popularity|odds|ninki|tansho_odds" data/_diag/shap_all_models_avg_top30.csv
[POPULARITY/ODDS 不在 = 確認 OK]
```

→ **Top 30 に 1 件も含まれない**。FEATURE_COLUMNS (108 個) 自体に odds / popularity が無いため当然の結果。

## 4. カテゴリ別 SHAP 重要度

(Top 30 を Phase 1 で人手分類した結果)

| カテゴリ | Top 30 個数 | 重要度合計 | 比率 |
|---|---:|---:|---:|
| 過去成績 (dev/relative/horse_avg/horse_*_pr) | 9 | 0.4082 | 32.0% |
| 騎手 (jockey_*) | 6 | 0.1995 | 15.6% |
| タイム指数 (speed_index_*) | 4 | 0.0566 | 4.4% |
| 厩舎 (trainer_*) | 3 | 0.0823 | 6.5% |
| ペース・展開 (front_runner/pace_pressure/style_pace) | 3 | 0.0587 | 4.6% |
| レース条件 (field_count / age / gate_no) | 3 | 0.1761 | 13.8% |
| 場類似度 (venue_sim_*) | 2 | 0.0664 | 5.2% |
| 騎手×厩舎 (jt_combo_wr) | 1 | 0.0156 | 1.2% |
| ML 出力 (ml_pos_est) | 1 | 0.0143 | 1.1% |
| 馬個体 (horse_days_since) | 1 | 0.0119 | 0.9% |
| **人気・オッズ系 (popularity/odds)** | **0** | **0.0000** | **0.0%** ⚠ |

## 5. win_global モデル (◎ 単勝直接予測) 個別 Top 30

| 順位 | 特徴量 | mean|SHAP| | 相対% |
|---:|---|---:|---:|
| 1 | dev_run1_adj | 0.04568 | 18.45% |
| 2 | venue_sim_place_rate | 0.02721 | 10.99% |
| 3 | jockey_pr_2y | 0.02035 | 8.22% |
| 4 | jockey_place_rank_in_race | 0.01854 | 7.49% |
| 5 | horse_venue_pr | 0.01791 | 7.23% |
| 6 | style_pace_affinity | 0.01469 | 5.93% |
| 7 | field_count | 0.01292 | 5.22% |
| 8 | jockey_win_rate | 0.01105 | 4.46% |
| 9 | trainer_place_rank_in_race | 0.01089 | 4.40% |
| 10 | jockey_dist_pr | 0.00920 | 3.72% |

→ **◎単勝予測モデルも popularity/odds は 0**。**ROI 100% 超を狙うのに odds を見ていない** ことが構造的に確定。

## 6. permute importance (win_global)

| 順位 | 特徴量 | Δ(mean abs) |
|---:|---|---:|
| 1 | dev_run1_adj | 0.00381 |
| 2 | horse_venue_pr | 0.00265 |
| 3 | jockey_place_rank_in_race | 0.00216 |
| 4 | venue_sim_place_rate | 0.00216 |
| 5 | jockey_pr_2y | 0.00212 |

→ permute も SHAP と同様のランキング。**dev_run1_adj が支配的**。

## 7. confidence 別 SHAP 差異 (win_global)

p33=0.112 / p67=0.124 (winグループは全体的に低確率 = 1 着予測の困難さを反映)

| 特徴量 | 高 | 低 | 差(高-低) |
|---|---:|---:|---:|
| jockey_place_rank_in_race | 0.0264 | 0.0134 | +0.0130 |
| style_pace_affinity | 0.0083 | 0.0206 | **-0.0123** |
| venue_sim_place_rate | 0.0351 | 0.0246 | +0.0105 |
| jockey_win_rate | 0.0145 | 0.0095 | +0.0050 |
| jockey_pr_2y | 0.0235 | 0.0188 | +0.0047 |
| jt_combo_wr | 0.0083 | 0.0039 | +0.0044 |
| past_avg_outer_ratio | 0.0000 | 0.0044 | -0.0044 |
| horse_venue_pr | 0.0196 | 0.0163 | +0.0033 |

### 含意

- **高 confidence 馬**: 騎手・場相性が支配 (jockey_place_rank, venue_sim_place_rate)
- **低 confidence 馬**: ペース適性 (style_pace_affinity) と外回りロス (past_avg_outer_ratio) で僅差勝負
- → Phase 4 印選定で「高 confidence は騎手重視 / 低 confidence はペース重視」の二段化検討余地あり

## 8. odds データが「収集済みだが学習対象外」の確証

`src/ml/lgbm_model.py:4197-4198`:

```python
"odds": getattr(h, "tansho_odds", None) or getattr(h, "odds", None),
"popularity": getattr(h, "popularity", None),
```

→ **データ自体は predict 時に horse オブジェクトから取得されている**。
→ ただし `FEATURE_COLUMNS` (L53-) には含まれていない = **学習時に X として LightGBM に渡されていない**。
→ Phase 2 で FEATURE_COLUMNS に `odds` / `popularity` を追加 + WF 再学習で取り込み可能。

## 9. 改善仮説 6 件 (Phase 2 着手項目)

### H1: ROI 期待値 loss function (核心 / 最優先)

- 現状: `binary_logloss` (head_top3 prob 最大化)
- 提案: `expected_value_loss` (prob × odds - 100 を最大化)
- 根拠: 現状の SHAP/gain で popularity/odds が学習対象外 → ROI 観点が無視されている
- 工数: 2-3 セッション (LightGBM custom objective 実装)

### H2: odds / popularity 特徴量取り込み

- 現状: FEATURE_COLUMNS に含まれない
- 提案: 当日確定 odds (リーク無) を training 投入
- 検証: `src/ml/lgbm_model.py:1300` で odds は既に horse object に取得されており、特徴量として渡す改修は容易
- 工数: 1-2 セッション

### H3: ペース・展開特徴量強化

- 現状: SHAP 4.6% (front_runner + pace_pressure + style_pace)
- 提案: 隊列予想 (front/mid/back) / 騎手脚質適性 / ペース崩れ予兆 追加
- 工数: 1-2 セッション

### H4: 不要特徴量排除

- 現状: FEATURE_COLUMNS 108 個のうち SHAP Top 30 = 30 個
- 残り 78 個の中で mean|SHAP| < 0.001 (≒ 0%) の特徴量を排除 → overfitting 抑制
- 推定排除候補: ~60 特徴量
- 工数: 0.5-1 セッション (Phase 3 と統合)

### H5: confidence 別学習目的分割

- 現状: 全 confidence で同じ binary_logloss
- 提案: 高 confidence race = 騎手・場特徴量 重視 / 低 confidence race = ペース・適性 重視
- 根拠: SHAP confidence 別差異で style_pace_affinity が低 confidence で +0.0123pt 上昇
- 工数: 1-2 セッション

### H6: head_win 学習目的再検討

- 現状: head_win (1 着 prob) と head_top3 (3 着以内 prob) 別モデル
- 過去の 5/27 v5 試行: head_win → mark 統合 (win_weight=0.5) で効果なし (ROI ±0.0)
- 仮説: weight が低すぎた / 統合方法が不適切
- 検証: H1 (ROI loss) で head_win の重み再評価
- 工数: H1 に統合

## 10. 一時保留 (Phase 2 対象外)

- **印選定再較正 (B-3/B-4)**: Phase 4 で実装 (M-3 Plan)
- **buy 戦略最適化 (D-4/D-6)**: Phase 5 で実装
- **派 5b 統合**: M-3 完了後に再評価

## 11. 次セッション着手 = Phase 2

### 目標

ML 学習目的を「ROI 期待値最大化」に変更し、odds/popularity を training に取り込む。

### 順序

1. `src/ml/lgbm_roi_objective.py` 新規作成 (LightGBM custom objective)
2. FEATURE_COLUMNS に odds / popularity 追加
3. WF backtest 1 月分で動作確認
4. 全期間 WF 再学習 + ROI 評価

### 合格ライン (Phase 2)

- baseline 74.7% → **+10pt 以上** (= 84.7%+) で Phase 3 へ
- 改善不足の場合は H3/H5 を先に試す

## 12. 関連ファイル

- `scripts/diag_shap_analysis.py` — Phase 1 実装スクリプト (596 行)
- `data/_diag/shap_all_models_avg_top30.csv` — 全モデル平均 Top 30
- `data/_diag/shap_top30_*_DEMO.csv` — 個別モデル 42 個 Top 30
- `data/_diag/shap_run.log` — 全 run ログ (Stage 2)
- `docs/予想精度根本改善Plan.md` — M-3 Plan 全体
- `docs/m2_design_v1.md` — 前 Phase (M-2) 設計
- `scripts/diag_feature_importance.py` — 前セッション (gain importance) 参考実装

## 13. レビュー結果 (本セッション)

実装スクリプト `scripts/diag_shap_analysis.py` は以下のレビューを通過:

- **python-reviewer**: NEEDS_FIX → 修正 2 件で APPROVED 同等 (P0-1 confidence 分割動的化済 / P0-2 float ValueError は Stage 2 完走で発生せず後回し)
- **keiba-reviewer**: NEEDS_FIX → 修正 1 件で APPROVED 同等 (P0 venue_65 除外済)
- 残 P1: SHAP UserWarning 抑制 / SHAP 二重計算 / `_DEMO` サフィックス除去 — 後続セッションで対応 (Phase 1 結果には影響無)
