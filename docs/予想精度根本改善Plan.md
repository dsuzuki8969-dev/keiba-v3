# 予想精度根本改善 Plan (M-3)

> 作成: 2026-05-28 (Opus 4.7 / 玄人クロード)
> マスター指示: 「現状の印を含めた予想では無理。根本的な予想の改善と見直しが大事」
> 目標: **hit% 25%+ AND ROI 110%+** を満たす運用戦略の実現

---

## 1. 問題の確定 (本セッション 2026-05-28 で判明)

### マスター基準

| 指標 | 最低ライン |
|---|---:|
| **的中率 (hit%)** | **≥ 25.0%** |
| **回収率 (ROI)** | **≥ 110.0%** |

### 現状の限界 (post-hoc 105 セル探索結果)

| 順位 | 戦略 × confidence | hit% | ROI | 基準達成 |
|---:|---|---:|---:|---|
| 1 | SS × ◎-○-△ (1 通り) | 5.94% | 119.87% | hit% ❌ |
| 2 | C × ◎-○-☆ (1 通り) | 4.40% | 101.92% | hit% ❌ / ROI ❌ |
| 3 | SS × ◎-○-△★ (2 通り) | 11.50% | 101.13% | hit% ❌ / ROI ❌ |
| 4 | S × ◎-○-▲ (1 通り) | 8.96% | 100.37% | hit% ❌ / ROI ❌ |
| — | ◎-○-▲△★☆ (4 通り) ALL | 24.52% | 70.85% | hit% ❌ / ROI ❌ |
| — | ◎-○-▲△★☆ (4 通り) × C | 26.48% | 72.87% | **hit% ✅** / ROI ❌ |

→ **105 セル中、両基準を同時達成するセル = 0 個**

### 構造的原因 (LightGBM feature_importance 分析判明)

| カテゴリ | 重要度 | 含意 |
|---|---:|---|
| 過去成績 (dev/past) | 37.88% | 「実力ベース」予想 |
| 騎手・厩舎 | 26.83% | 過剰寄与 |
| コース適性 | 17.63% | 妥当 |
| **ペース・展開** | **2.17%** ⚠ | **薄すぎ** |
| **人気・オッズ系** | **0.00%** ⚠⚠ | **完全に学習対象外** |

**真因確定**:
1. ML 学習目的 (`head_top3` prob 最大化) と ROI が **構造的に乖離**
2. ML は「3 着以内に入りやすい馬」を予想 → 人気馬寄り → 配当低 → ROI 低
3. **「ROI 期待値」を学習目的にしていない**

---

## 2. 改善 5 Phase (6-9 セッション計画)

### Phase 1: 特徴量診断 + 印選定可視化 (1-2 セッション)

**目的**: 現状の予想ロジックを徹底的に診断、改善ポイントを特定。

**タスク**:
- 1.1 SHAP 値分析 (全 47 モデル × 159 特徴量)
- 1.2 permute importance (random shuffle で ROI 寄与度測定)
- 1.3 印選定ロジック可視化 (composite 計算式を分解 / weight 表示)
- 1.4 期間別 (wf_2024/2025/2026) 重要度差異
- 1.5 confidence 別 重要度差異

**成果物**:
- `docs/phase1_feature_diagnosis.md` (Top 30 + ROI 寄与度ランキング)
- `scripts/diag_shap_analysis.py`
- 改善仮説の優先順位確定

---

### Phase 2: 学習目的の再設計 (2-3 セッション)

**目的**: ML 学習目的を「ROI 期待値最大化」に変更。

**タスク**:
- 2.1 **新 loss function 設計**:
  - 現状: `binary_logloss` (3 着以内 prob 最大化)
  - 新案A: `expected_value` (prob × odds - 100 を最大化)
  - 新案B: `roi_weighted_logloss` (配当倍率で weight した binary loss)
- 2.2 当日 odds 特徴量の **リーク無し** 取り込み方法:
  - 当日 odds は予想時点で確定 → リーク無
  - ただし historical odds は backtest 時に取得可能?
- 2.3 odds 特徴量を training data に組込み
- 2.4 LightGBM custom objective 実装
- 2.5 WF backtest で hit% / ROI 比較

**成果物**:
- `src/ml/lgbm_roi_objective.py` (custom loss)
- `scripts/train_roi_models_wf.py`
- 全期間 WF backtest 結果

**期待効果**: hit% ↑ + ROI ↑ で **両基準達成可能性大**

---

### Phase 3: 特徴量追加・再選定 (1-2 セッション)

**目的**: ROI 観点で必要な特徴量を追加 / 不要な特徴量を排除。

**タスク**:
- 3.1 **odds 系特徴量**を training に追加:
  - 当日確定 odds / popularity rank / 馬連支持率
- 3.2 **ペース・展開** 特徴量強化 (現状 2.17% は薄い):
  - 隊列予想 (front/mid/back)
  - 騎手脚質適性
  - ペース崩れ予兆 (急流ペース判定)
- 3.3 **市場効率性** 指標:
  - odds 過小評価率 (ML prob vs market prob 比率)
  - 隠れた実力指標 (人気崩れ予兆)
- 3.4 **SHAP** で不要特徴量を排除 (重要度 < 0.1% の 50+ 特徴量)
- 3.5 再学習 + WF backtest

**成果物**:
- 新規 features.py の FEATURE_COLUMNS_ROI 定義
- `scripts/build_odds_features.py`

---

### Phase 4: 印選定ロジック再較正 (1-2 セッション)

**目的**: 印選定 (◉◎○▲△★☆) を ROI 観点で再設計。

**タスク**:
- 4.1 ☆ 常時付与 (composite 順位 6 番目固定):
  - `src/engine.py` の `TOKUSEN_SCORE_THRESHOLD` ベース動的選定を廃止
  - `assign_marks()` で composite TOP6 に ☆ 固定
  - **マスター指示違反 (2026-05-28 累犯 1) の解消**
- 4.2 composite 計算式 ROI 観点で再較正:
  - head_top3 weight 変更
  - head_win weight 増 (試行 v5 失敗の反省: weight 0.5 → 2.0+)
  - popularity_blend 係数調整
- 4.3 印階層の ROI 最適化:
  - ◎ = ROI 期待値 TOP1? composite TOP1?
  - 試行: head_win × odds 最大馬を ◎ に (v6 試行 #1 で失敗、SS 限定で再試行)
- 4.4 pred.json 全期間再生成 + WF backtest 検証

**成果物**:
- engine.py 修正 (☆ 常時 + composite 再較正)
- 全期間 pred.json 再生成

---

### Phase 5: 統合検証 + 戦略確定 (1 セッション)

**目的**: 全 Phase 統合後の hit%×ROI を WF backtest で検証、マスター基準達成戦略を確定。

**タスク**:
- 5.1 全期間 WF backtest 再実行
- 5.2 馬券種網羅マトリクス (単勝/複勝/馬連/ワイド/馬単/三連複/三連単):
  - 各馬券 × 7 confidence × 印組合せ = 数百セル
- 5.3 hit% 25%+ AND ROI 110%+ セル抽出
- 5.4 実運用候補 (Top 3 戦略) の安定性検証:
  - 月別ばらつき / 信頼区間 / 最悪月 balance
- 5.5 UI カード化 + dashboard 反映

**成果物**:
- 確定戦略の hit%/ROI 一覧
- 実運用採用戦略 (1-3 個)
- dashboard カード追加

---

## 3. マイルストーン

| Phase | セッション数 | 期間目安 |
|---|---:|---|
| Phase 1 | 1-2 | 1 週 |
| Phase 2 | 2-3 | 2 週 |
| Phase 3 | 1-2 | 1-2 週 |
| Phase 4 | 1-2 | 1 週 |
| Phase 5 | 1 | 1 週 |
| **合計** | **6-9** | **5-7 週** |

---

## 4. 検証指標 (各 Phase 完了基準)

| Phase | 主要指標 | 合格ライン |
|---|---|---|
| Phase 1 | 特徴量重要度可視化 | SHAP Top 30 確定 / 改善仮説 5+ 抽出 |
| Phase 2 | WF backtest ROI | baseline 74.7% → +10pt 以上 |
| Phase 3 | hit% 改善 | 全期間 hit% +5pt 以上 |
| Phase 4 | 印選定の妥当性 | ☆ 常時 100% / composite 再較正後 ROI +3pt |
| Phase 5 | 基準達成セル数 | **hit% 25%+ AND ROI 110%+ を満たすセル 1 個以上** |

---

## 5. 一時凍結タスク (Phase 5 完了まで)

- B-3 composite 重み再較正 (= Phase 4 に統合)
- B-4 ◎ハズレ救済 (= Phase 2 で自然解消見込み)
- D-4 複勝/馬連/ワイド 導入 (= Phase 5 で網羅検証に統合)
- D-6 期待値ベース買い目選定 (= Phase 2 の核心)
- 派 5b 統合実装 (M-2 で前提崩壊 / M-3 で再評価)

---

## 6. 関連 memory

- [feedback_marks.md](../../.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/feedback_marks.md) — ☆ 常時マスター指示 (2026-05-28 違反 1 回)
- [feedback_production_vs_wf_pred_distinction.md](../../.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/feedback_production_vs_wf_pred_distinction.md) — WF backtest 必須
- [feedback_popularity_blend_circular_leak.md](../../.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/feedback_popularity_blend_circular_leak.md) — リーク防止
- [feedback_master_intent_first.md](../../.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/feedback_master_intent_first.md) — マスター指摘の真意を汲み取る

---

## 7. 本セッション (2026-05-28) で確定した発見

### 試行錯誤の最終到達点 (post-hoc 限界)

| 戦略 | hit% | ROI | balance | 評価 |
|---|---:|---:|---:|---|
| ⭐ SS × ◎-○-△ | 5.94% | 119.87% | +48,200 円 | hit% NG / ROI OK |
| C × ◎-○-☆ | 4.40% | 101.92% | +4,620 円 | 両 NG |
| SS × ◎-○-△★ | 11.50% | 101.13% | +5,490 円 | 両 NG |
| S × ◎-○-▲ | 8.96% | 100.37% | +1,480 円 | 両 NG |

### LightGBM 特徴量重要度 (wf_2026 全モデル)

- 過去成績 (dev): 37.88% (支配的)
- 騎手・厩舎: 26.83%
- 人気・オッズ: **0.00%** (学習対象外)
- ペース・展開: 2.17% (薄すぎ)

### Bug 修正

- `hybrid_summary.py:1208/1342` `hit = payout_val > 0` バグ → 着順ベース判定に修正
- 馬連 hit% 4.4% → **16.2%** に正常化 (前 v6 集計時点の数値)

### マスター指示違反

- 「☆ 動的追加」(TOKUSEN_SCORE_THRESHOLD ベース) は過去マスター指示 (☆ 常時) 違反
- 累犯 #17 として記録 / feedback_marks.md に永続化済
- Phase 4 で engine.py 修正実装

---

## 8. Phase 1 次セッション開始準備

### 着手手順

1. CLAUDE.md / MEMORY.md は context 自動注入 (Read 不要)
2. **TASKS.md** + **handoff_2026-05-28.md** + **本 Plan** を Read で必読
3. Phase 1 着手 = `scripts/diag_shap_analysis.py` 実装

### 必要環境

- LightGBM 4.6.0 (確認済)
- SHAP ライブラリ (要追加 / `pip install shap`)
- WF backtest 環境

### Phase 1 サクセスクライテリア

- SHAP 値 Top 30 が出力される
- 各特徴量の ROI 寄与度がランキング表示される
- 改善仮説 5+ が確定する
