# M-2 設計書 v1 — ML 根本再設計 (WF ROI 100% 超 目標)

> 作成日: 2026-05-27
> 前提: handoff_2026-05-26_v5.md (L-1 真因確定) + 本セッション L-2 詳細検証
> 目標: WF backtest 真の ROI **72.7% → 100% 超**
> 工数見積: **2-3 週** (複数セッション分割実装)

---

## 1. 背景 (なぜ M-2 が必要か)

### 1.1 L-1 学習リーク真因確定 (handoff_v5)

`data/predictions/*_pred_backup.json` (2025-03-19 生成) は 2024 race を後追い予想 = 時系列リーク。
本番運用 pred.json で観測される hit% 39.3% は異常値。

### 1.2 L-2 詳細検証結果 (本セッション)

| Layer | リーク | 根拠 |
|---|---|---|
| L1 学習データ | **無し** | `wf_inference.py:64` `_load_ml_races(max_date)` で train_max まで限定 |
| L2 集計統計 (`_horse_history`) | **無し** | `lgbm_model.py:1604` `d < date_str` 厳密フィルタ |
| L3 popularity_blend | **適用されない** | `wf_inference.py` に popularity_blend 呼出無し |
| L4 sire_tracker / jt_combo_30d | **無し** | update_race は predict 後置 + 日付フィルタ厳密 |

→ **WF backtest ROI 72.7% は時系列リーク完全排除済の真の値**。

### 1.3 L-2 第二リーク発見 (popularity_blend 循環参照)

```
本番運用 pred.json (リーク有 = hit% 39.3%)
        ↓ build_popularity_stats.py で集計
popularity_rates.json (リーク有統計テーブル)
        ↓ engine.py:1921 で popularity_blend 適用
本番運用 pred.json (二重リーク累積)
```

`scripts/build_popularity_stats.py:103` は全期間 `*_pred.json` を glob しているため、評価対象期間自身を含む循環参照。

### 1.4 v5+γ 3 試行 全失敗 (handoff_v5 から)

| 試行 | 戦略 | ROI |
|---|---|---:|
| 1 | EV ベース買い目選定 | 67.9% |
| 2 | 動的フィルター 11 種 (最高戦略) | 78.0% |
| 3 | Isotonic calibration | 66.2% |

→ 表層的フィルター・calibration では ROI 100% 不可能。**ML 学習目的の本質改革が必要**。

---

## 2. M-2 設計方針 (4 方針)

### 方針 1: ML 学習目的の二段化 (handoff_v5 起点)

**現状**:
- LightGBM head 1 つ (3 着内 binary)
- WF inference では `win_prob ≈ top3_prob × 0.40` という単純近似で win_prob 推定
- → win_prob 数値が ROI 観点で信頼できない (v5+γ 試行 3 で確認)

**設計**:
```
LightGBM model
├── head_top3 (既存): 3 着内 binary
└── head_win  (新規): 単勝 1 着 binary  ← 別 loss / 別 calibration
```

- 学習: 2 head それぞれ独立 loss (BCE) で学習
- 推論: head_top3 → top3_prob / head_win → win_prob を別々に取得
- 利点: win_prob が単勝 1 着確率として直接最適化される

**実装場所**:
- `src/ml/lgbm_model.py` `train_split_models()` で head 別モデル学習
- `scripts/wf_inference.py` `predict_race()` で 2 head 推論統合
- `FEATURE_COLUMNS` 共通だが、ラベル定義を head 別に分離

**工数**: 約 1 週

### 方針 2: WF 内部 Isotonic calibration 統合 (handoff_v5 起点)

**現状**:
- WF backtest の win_prob は Platt scaling (`a*p+b` sigmoid) のみ
- v5+γ 試行 3 で raw win_prob >= 0.40 が cal wp = 1.0 に上限飽和を確認 = Platt のみでは不十分

**設計**:
```python
for wf_period in [wf_2024, wf_2025, wf_2026]:
    # 1. train_max 以前で head_win 学習
    model_win = train_head_win(train_data_until=wf_period.train_max)

    # 2. 直前 N race (例: 3000 race) で Isotonic calibrator 学習
    cal_data = sample_recent_races(N=3000, before=wf_period.infer_start)
    iso_cal = IsotonicRegression().fit(model_win.predict(cal_data.X), cal_data.y_win)

    # 3. 推論時に Isotonic 適用
    for race in wf_period.infer_races:
        raw_p = model_win.predict(race.X)
        cal_p = iso_cal.transform(raw_p)
        race.win_prob = cal_p
```

**実装場所**:
- `scripts/wf_inference.py` `load_wf_predictor()` 内 Isotonic calibrator ロード追加
- `scripts/walk_forward_backtest.py` Isotonic calibrator 生成パイプライン追加
- 各 WF 期間ディレクトリに `iso_cal_win.pkl` 保存

**工数**: 約 3 日

### 方針 3: ROI 観点 特徴量再選定 (handoff_v5 起点)

**現状**:
- 159 特徴量は「3 着内予測精度最大化」用に設計
- ROI 最大化観点では不要・有害な特徴量が混在の可能性

**設計**:

#### 3.1 SHAP 値 + ROI 寄与度 分析
- 各特徴量の SHAP 値 を計算
- 各特徴量を 1 つだけ permute (シャッフル) してオッズ × win_prob の ROI 変化を測定
- ROI 寄与度 = (元 ROI - permuted ROI)

#### 3.2 オッズ依存特徴量の見直し
- `prev_tansho_odds_dev` 等オッズ系特徴量を `win_prob` 推定に使うと:
  - オッズ低い馬 = popular = win_prob 高くなる傾向 → 期待値ベース戦略では負ける
- → オッズ系を head_win 学習から除外する選択肢を検討

#### 3.3 競争馬個体特徴量の補強
- `horse_win_rate` 等 N=10 程度の少サンプル特徴量を信頼区間付きに変更
- ベイズ縮小推定で N 不足時は全体平均に引き寄せる

**実装場所**:
- 新規 `scripts/feature_roi_importance.py` (SHAP + permute 分析)
- `src/ml/lgbm_model.py` `FEATURE_COLUMNS` を head 別に分離
  - `FEATURE_COLUMNS_TOP3` (3 着内予測用、既存維持)
  - `FEATURE_COLUMNS_WIN` (新規、ROI 観点で選別)

**工数**: 約 1 週

### 方針 4: popularity_blend を WF に組込み (本セッション新発見対応)

**現状**:
- 本番運用 `engine.py:1921` でのみ popularity_blend 適用
- WF backtest は popularity_blend 無しで評価
- → 本番運用と WF 評価が構造的に違う

**問題**:
- `popularity_rates.json` は全期間 pred.json で集計 = 評価対象期間自身を含む循環参照 (L-2 発見)
- そのまま WF に組み込むと第二リーク発生

**設計**:

#### 4.1 期間別 popularity_rates 生成
```python
# scripts/build_popularity_stats_wf.py (新規)
for wf_period in [wf_2024, wf_2025, wf_2026]:
    train_max = wf_period.train_max  # 例: 2023-12-31
    # train_max 以前の race results のみで集計
    out_path = f"data/popularity_rates_{wf_period.name}.json"
    build_stats(date_max=train_max, out=out_path)
```

#### 4.2 WF inference に popularity_blend 組込み
```python
# scripts/wf_inference.py 修正
def predict_race(race, horses, wf_period_name):
    raw_probs = model.predict(X)  # 既存
    # popularity_blend 適用 (期間別 stats 使用)
    pop_stats = load_popularity_stats_wf(wf_period_name)
    blended_probs = blend_probabilities_pure(
        raw_probs, race, horses, pop_stats
    )
    return blended_probs
```

#### 4.3 本番運用 popularity_rates.json の再構築
- 既存 `scripts/build_popularity_stats.py` を **WF backtest pred.json から集計** に変更
- 本番運用 pred.json (リーク有) からの集計を停止
- これで本番運用 popularity_blend も真の値ベースになる

**実装場所**:
- 新規 `scripts/build_popularity_stats_wf.py`
- 既存 `scripts/build_popularity_stats.py` 修正 (集計対象を WF pred.json に変更)
- `scripts/wf_inference.py` popularity_blend 組込み

**工数**: 約 3 日

---

## 3. 実装順序 (優先度)

| 優先 | 方針 | 工数 | 効果見込 | 理由 |
|---:|---|---|---|---|
| **1** | 方針 4 (期間別 popularity_blend) | 3 日 | +5〜15pt | 軽量・本番運用と WF の整合性回復・即評価可能 |
| **2** | 方針 2 (calibration 統合) | 3 日 | +5〜10pt | 軽量・既存パイプライン拡張 |
| **3** | 方針 1 (二段化) | 1 週 | +10〜20pt | 中規模改革・win_prob 直接最適化 |
| **4** | 方針 3 (特徴量再選定) | 1 週 | +5〜15pt | 最大改革・最後の伸びしろ |

**合計**: 約 2.5 週 / 効果見込 +25〜60pt (ROI 72.7% → 97.7〜132.7%)

---

## 4. 検証指標 (各方針実装後)

各方針実装後に全期間 WF backtest 再実行し、以下を測定:

| 指標 | 現在値 (WF) | 目標 |
|---|---:|---|
| **hit% (TOP1 → 1 着)** | 31.3% | 35% 超 |
| **◎単勝 JRA ROI** | 72.7% | **100% 超** |
| **派 5b JRA ROI** | 79.0% | **120% 超** |
| **MAE (win_prob vs 実勝率)** | 未測定 | 各方針で 10% 改善 |

---

## 5. リスク・前提

### 5.1 構造的限界の可能性

全方針実装後も ROI 100% 未達のリスク:
- 競馬市場が効率的すぎる (Sharpe 比 0 の世界)
- 控除率 20-25% の参入障壁
- → 「ROI 100% 超」は理論上可能だが市場効率仮説下では難しい

### 5.2 既存 WF backtest との整合性

方針 4 (popularity_blend 組込み) で過去 WF backtest 結果がすべて再評価必須:
- 派 5b ROI 79.0% は popularity_blend 無し前提 → 再評価で変動
- 既存ドキュメント (`b3_strategy5b_integration_supplement.md` 等) との整合性確認必要

### 5.3 凍結戦略の再評価

L-1 真因確定後に凍結された戦略 (B-3 / B-4 / D-4 / D-6 / 派 5b 統合) は、M-2 完了後に再評価:
- popularity_blend WF 組込み後 → 各戦略 ROI 再計算
- 再評価で実用範囲なら順次解凍

---

## 6. セッション分割計画

| Session | 内容 | 工数 |
|---|---|---|
| **S1** (今後) | 方針 4 期間別 popularity_blend 実装 + WF 再評価 | 1-2 セッション |
| S2 | 方針 2 Isotonic calibration 統合 + WF 再評価 | 1 セッション |
| S3-S4 | 方針 1 ML 学習目的二段化 (head_win 追加) + 学習 + WF 再評価 | 2-3 セッション |
| S5-S6 | 方針 3 特徴量再選定 (SHAP+ROI 寄与度 + FEATURE_COLUMNS_WIN 分離) + WF 再評価 | 2-3 セッション |
| S7 | 全方針統合 WF backtest + 凍結戦略再評価 + handoff | 1 セッション |

**合計**: 約 7-11 セッション (連続 2-3 週 相当)

---

## 7. 失敗時のフォールバック

各方針実装後の WF ROI が現状 (72.7%) を下回った場合:
1. 該当方針を rollback
2. 失敗要因を docs/m2_failure_log_*.md に記録
3. 次方針へ進む or 設計改訂

ROI 100% 超達成後の戦略採用判断は別途 (handoff で記録)。

---

## 8. 参考

- handoff_2026-05-26_v5.md (L-1 真因確定 + L-2 暫定 + v5+γ 3 試行)
- feedback_production_vs_wf_pred_distinction.md (本番運用 vs WF 区別)
- feedback_no_meaningless_judgement_task.md (思考停止判断タスク禁止)
- 本セッション L-2 詳細: WF 構造的リーク排除確定 + popularity_blend 第二リーク発見
