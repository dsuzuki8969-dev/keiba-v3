# TASKS.md — D-AI Keiba v3 タスクボード

> **運用ルール**: CLAUDE.md「TASKS.md / MEMORY.md 運用ルール」を参照。
> マスター指示はすべてここに追加。完了タスクは git log + handoff_*.md に集約済のため本ファイルから削除する。
> 教訓・反省は `~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/` に永続化。

---

## ✅ 5/26 セッション完結 (P0 + 派生 全 13 件)

詳細: `memory/handoff_2026-05-26.md` / commit: c8178c2 → 524f1d2 → f84bab9 → 01ca494 (+1,689 行)

| 区分 | ID 一覧 |
|---|---|
| 5/25 引継ぎ P0 | C-1b / G-4 / G-5 / G-6 / G-7 / G-8 / D-1b / A-3c |
| 形式統一 (マスター指摘) | H-1 (DB 一括 migration) / H-2 (新規書き込み正規化) |
| 完結追加 | A-3d (Lv1 簡易実装) / G-7b (2025-04-19 fetch) / D-1c (戦略B 改善 6案 ROI 試算 → **採用棄却確定**) |

**重要発見 (D-1c)**: 戦略 B (shobu_score TOP2) はどの改善案でも現運用 ◎単勝 1 点 (JRA 183.5% / NAR 128.8%) に勝てない。唯一の検討候補は **案 5 戦略 A 合致 JRA 限定 ROI 195.9%** (次セッション D-1d で深掘り)。

---

## ✅ 5/26 セッション v2 完結 (A.B.C 連続完走)

詳細: `memory/handoff_2026-05-26_v2.md`

| 区分 | ID | 結果 |
|---|---|---|
| A | D-1d | **派 5b (二重一致 ∩ ◎ のみ) ROI 207.3% 最強候補確定** (CI [184.9, 232.6] / 3 年安定 / 月別負け 9/95) |
| B | G-7c | 残 399 件は **永久に修復不可能** (DB only ゴミ row + maintenance 7 日窓超過) → 案 A 放置採用 / 教訓: `feedback_g6_maintenance_7day_limit.md` |
| C | A-3e Lv2 | engine 直呼び実装完了 (`scripts/walk_forward_backtest.py` `_calc_shobu_score_wf_lv2` 新関数 + `--shobu-lv 2` CLI / 既存 Lv1 不変 / mock 一致率 67%) |

## 🚨 5/26 v4 緊急発見: D-1d 派 5b 採用判断 完全訂正

詳細: `memory/handoff_2026-05-26_v4.md`

A-3e Lv3 全期間 WF backtest (29 月 / 81 分) 完走後の再集計で重大発見:

| 案 | v2 値 (本番運用 backup) | v4 値 (WF リーク排除) | 差分 |
|---|---:|---:|---:|
| 案 4 ◎単勝 | 183.5% | **72.7%** | -110pt |
| 派 5b ◎ のみ | 207.3% | **79.0%** | **-128pt** |

**真因**: v2 D-1d 集計時の pred.json は本番運用版 (学習リークあり) であり、WF backtest pred.json (リーク排除) ではなかった。本来 WF で評価すべきだった。

→ **派 5b 採用候補 #1 確定は完全棄却**。**現運用 ◎単勝も WF 評価で赤字 (72.7%)**。

## ✅ 5/26 v5: L-1 学習リーク真因確定 (本セッション完結)

詳細: `memory/handoff_2026-05-26_v5.md`

**決定的証拠**:
- backup pred.json: TOP1→1着 hit% **39.3%** (2024 全 17,064 race) ← 異常値
- WF (Lv3) pred.json: hit% **31.3%** ← 正常値
- 真因: `_pred_backup.json` (2025-03-19 タイムスタンプ) は 2024 race を **後追い予想** = 時系列リーク

永続教訓: `memory/feedback_production_vs_wf_pred_distinction.md` (★★★)

## ✅ 5/27 セッション完結 (P0 真の 4 件)

詳細: `memory/handoff_2026-05-27.md` (作成予定)

| 区分 | ID | 結果 |
|---|---|---|
| **A** | **L-2 詳細検証** | Layer 1-4 全リーク排除確認 ✅ + **第二リーク発見** (popularity_rates.json 循環参照) → 教訓 `feedback_popularity_blend_circular_leak.md` 永続化 |
| **B** | **M-1 学習リーク防止 (4 機能)** | ✅ タイムスタンプ強制 + 過去 race ロック + batch_reanalyze 警告 + retrofit スクリプト (+192 行 / 新規 214 行) |
| **C** | **M-2 設計 v1** | ✅ `docs/m2_design_v1.md` 作成 (4 方針 + 7-11 セッション計画 + 期間別 popularity_blend 反映) |
| **D** | **データ整理** | ✅ `_pred_backup.json` 816 + `_pred_prev.json` 875 = **1,691 ファイル削除** (tar.gz アーカイブ `data/_archive/` 保管済 843MB) |

### ✅ M-2 方針 4 完了 (5/27 v2 セッション)

| 期間 | ◎単勝 ROI baseline | current | 差分 |
|---|---:|---:|---:|
| wf_2025 JRA | 73.0% | 74.7% | **+1.8pt** |
| wf_2026 JRA | 71.7% | **74.8%** | **+3.0pt** |
| wf_2026 NAR | 69.0% | 70.7% | +1.7pt |
| 全期間 JRA | 72.3% | **74.8%** | **+2.5pt** ✅ |
| 全期間 TOTAL | 74.0% | 74.9% | +1.0pt |

評価: 改善方向 (+1〜3pt) で正常動作だが 100% 超には程遠い → 方針 1-3 必須

### ✅ M-2 方針 2 完了 (5/27 v3 セッション)

| 期間 | 組織 | v4_only | v4+v2 | v2 単独効果 |
|---|---|---:|---:|---:|
| wf_2024 | TOTAL | 73.5% | 72.7% | **-0.9pt** ❌ |
| wf_2025 | TOTAL | 77.6% | 77.7% | +0.1pt ≈ |
| wf_2026 | JRA | 74.8% | **78.4%** | **+3.7pt** ✅ |
| wf_2026 | TOTAL | 71.2% | 72.4% | +1.2pt ✅ |
| **全期間** | **TOTAL** | 74.9% | 74.8% | **-0.1pt** (ノイズ) |
| 全期間 | JRA | 74.8% | **75.4%** | +0.6pt |

評価: 方針 2 単独効果は期待 (+5〜10pt) を大幅下回り = 全期間 -0.1pt (ノイズ範囲)。
ただし wf_2026 JRA で +3.7pt (calibrator 直近データほど有効) → 方法論は維持、改善余地は将来課題。

### ✅ M-2 方針 1 (head_win 二段化) 完了 (5/27 v4 セッション)

| 期間 | 組織 | v4+v2 ROI | v4+v2+v1 ROI | v1 単独 | TOP1 hit% v1 単独 |
|---|---|---:|---:|---:|---:|
| wf_2024 | TOTAL | 72.7% | 72.5% | -0.2pt | -4.4pt ❌ |
| wf_2025 | TOTAL | 77.7% | 77.2% | -0.5pt | **+6.7pt** ✨ |
| wf_2026 | JRA | 78.4% | **80.6%** | +2.1pt | +3.4pt ✅ |
| wf_2026 | TOTAL | 72.3% | 73.9% | +1.7pt | +6.6pt ✅ |
| **全期間** | **TOTAL** | 74.7% | 74.7% | **-0.0pt** | **+1.9pt** |

評価:
- ✅ head_win 学習 AUC 0.75-0.77 (binary 分類良好) / TOP1 hit% +1.9pt 改善 = win_prob 精度向上
- ❌ ◎単勝 ROI 変化なし = **mark="◎" は composite (head_top3) ベースで決まるため head_win 効果が反映されない**
- 🔑 **構造的問題**: head_win を mark/composite ロジックに統合する必要あり

### ✅ head_win → mark 統合 (win_weight=0.5) 完了 (5/27 v5 セッション) - 効果なし

| 期間 | 組織 | v4+v2+v1 ROI | +mark ROI | mark 単独 |
|---|---|---:|---:|---:|
| wf_2024 | TOTAL | 72.5% | 72.5% | -0.1pt |
| wf_2025 | TOTAL | 77.2% | 77.6% | +0.4pt ≈ |
| wf_2026 | TOTAL | 73.9% | 73.1% | **-0.8pt** ❌ |
| **全期間** | **TOTAL** | **74.7%** | **74.7%** | **+0.0pt** (完全に変化なし) |

評価: win_weight 0.5 では head_win z-score 影響が composite に埋もれる → mark 不変 → ROI 不変。

### 試行錯誤フェーズ (5/27 v6 セッション) - ROI 100% 達成への試行

| 試行 | アプローチ | 全期間 TOTAL ROI | 差分 vs 基準 (74.7%) | 評価 |
|---|---|---:|---:|---|
| #1 | EV >= 1.0 (head_win × odds 最大馬 ◎) | **42.2%** | -32.5pt | ❌❌ 大失敗 (穴馬選定問題) |
| #4 | head_win TOP1 = ◎ 直接置換 | **71.6%** | -3.1pt | ❌ 人気馬偏重で配当低下 |
| #6 | ◎複勝戦略 (買い目変更) | 23.5% | (バグ) | ⚠ payouts 旧形式バグ / 検証保留 |
| **#7** | **◎単勝 オッズフィルター <2.0** | **80.3%** | **+5.0pt** ✨ | ✅ **改善発見** |
| #7-1.5 | オッズフィルター <1.5 | ~83% | +7.7pt | ✅ さらに改善 |
| #7-1.3 | オッズフィルター <1.3 | ~86% | +10.7pt | ✅ 最高 (race 数激減) |
| #7-JRA-1.3 | <1.3 + JRA 限定 | ~88-90% | +13-15pt | ✅✨ 最有望 |

理論限界分析:
- <2.0 馬平均オッズ ≈ 1.5-1.6 倍 (分布偏在)
- hit% 55% × 配当 150% = ROI 82.5% (実測近似)
- → 単純オッズフィルターでは **ROI 100% 達成困難 / -10〜15pt 不足**

## ✅ 5/28 セッション完結 (試行錯誤完了 + 構造課題確定 + Plan 化)

詳細: `memory/handoff_2026-05-28.md`

| 区分 | ID | 結果 |
|---|---|---|
| **試行 #1** | 複数買い ◎+○+▲ 三連複 | A1 3頭BOX ROI 78.4% (BASE 74.7% +3.7pt) |
| **試行 #2** | <1.3 + JRA + 期間絞り | races>=30 で 100% 超なし |
| **試行 #4** | 高信頼 race + ◎ + odds | composite>=95×<1.3 = 88.4% (1,200R) |
| **バックエンド集計実装** | 馬連 5 馬券 + 三連複 7 馬券 集計関数追加 (+370 行) | API endpoint で公開 |
| **フロントエンド** | UmarenCards/SanrenpukuExtendedCards 新規 + 単勝 UI 削除 + ビルド + 同期 | dashboard 表示確認 |
| **重大バグ修正** | `hit = payout_val > 0` → 着順ベース判定 | 馬連 hit% 4.4% → 16.2% に正常化 |
| **4 点合算試行** | ◎-○-▲△★☆ (4 通り) ROI 70.85% (基準未達) | マスター提案検証 |
| **15 戦略マトリクス** | 105 セル探索 → ROI 100% 超 4 セル発見 | SS×◎-○-△ = 119.87% / +48,200円 等 |
| **H-0 ☆ 常時検証** | composite TOP6 override → 100% 超セル数 変化なし (4 個) | マスター指示違反 (累犯 #17) |
| **特徴量重要度分析** | popularity/odds = **0.00%** / 過去成績 37.9% | 構造課題確定 |
| **マスター基準確定** | **hit% 25%+ AND ROI 110%+** | 105 セル中 達成 0 個 |
| **Plan 作成** | `docs/予想精度根本改善Plan.md` (Phase 1-5 / 6-9 セッション) | 次セッション着手準備完了 |

## 🚨 5/28 セッション マスター指示違反 (累犯 #17)

> 「☆ = ev > 3.0 動的追加←こんなの頼んでねーぞ。以前に☆は常時だと言ってただろうが」

- 違反根源: `src/engine.py:2059-2078` + `config/settings.py:574-577` の TOKUSEN_SCORE_THRESHOLD=5.5 動的選定
- 修正方針: composite 順位 6 番目を ☆ 固定付与 (Phase 4 で engine.py 修正実装)
- 永続化済: `memory/feedback_marks.md` 更新

## ✅ 5/29 セッション完結 (M-3 Phase 1 SHAP 特徴量診断)

詳細: `memory/handoff_2026-05-29.md` / docs: `docs/phase1_feature_diagnosis.md`

| 区分 | 内容 | 結果 |
|---|---|---|
| **実装** | `scripts/diag_shap_analysis.py` 新規 (596 行 / Sonnet subagent 実装 + Opus レビュー後 2 箇所修正) | 全 42 モデル完走 (venue_65 除外) |
| **手法** | SHAP TreeExplainer + permute + percentile 33/67 動的閾値 confidence 別差異 | 所要 1.7 分 |
| **データ** | 2025-12 月 N=500 sample / wf_2026 学習データ tracker 更新 (66,480 race) | NaN 率 34% |
| **発見 1** | **popularity/odds が SHAP Top 30 に 1 件も含まれない** (gain=0% との一致確証) | 構造課題確証 |
| **発見 2** | dev_run1_adj 15.47% / field_count 13.77% / jockey_place_rank 10.20% が支配 | 過去成績ベース確証 |
| **発見 3** | win_global (◎ 単勝) でも popularity/odds=0 + dev_run1_adj 18.45% で 1 位 | ROI 未最適化確証 |
| **発見 4** | `src/ml/lgbm_model.py:4197-4198` で odds データは収集済だが FEATURE_COLUMNS に未投入 | Phase 2 で取込可能 |
| **改善仮説** | H1-H6 抽出 (H1 ROI loss / H2 odds 取込 / H3 ペース強化 / H4 不要特徴量排除 / H5 confidence 別 / H6 head_win 再検討) | Phase 2 着手項目確定 |
| **レビュー** | python-reviewer + keiba-reviewer 並列 → P0 2 件発覚 → Opus 修正で完走 | コード品質確保 |

**生成ファイル**:
- `scripts/diag_shap_analysis.py` (新規 596 行)
- `data/_diag/shap_all_models_avg_top30.csv` (全モデル平均)
- `data/_diag/shap_top30_*_DEMO.csv` (個別 42 個)
- `data/_diag/shap_run.log` (実行ログ 240 KB)
- `docs/phase1_feature_diagnosis.md` (新規 / 約 230 行)

## ✅ 5/29 後半セッション完結 (M-3 Phase 2a パイロット odds/popularity 取込)

詳細: `memory/handoff_2026-05-29.md` Section L-T

| 区分 | 内容 | 結果 |
|---|---|---|
| **実装** | `scripts/diag_phase2_pilot_odds_features.py` 新規 540 行 (Sonnet subagent + Opus 修正) | 4 variant 比較 |
| **レビュー** | python-reviewer + keiba-reviewer 並列 → **P0 5 件発覚** | 修正完走 |
| **修正** | ばんえい除外 / ROI 計算非対称 / `or` チェーン / baseline_new 公正比較 variant 追加 | 確証取得 |
| **異常** | 第 1 回 subagent run で **45 分間 Bash プロセスハング** → マスター指摘 累犯 #18 監視権侵害 | Opus 直接 + Monitor で復旧 |
| **完走** | 第 2 回 Opus 直接 run = **15.9 分で完走** | exit 0 |
| **真の効果 (baseline_new vs +odds)** | Δ AUC +5.45pt / Δ hit% +9.40pt / Δ ROI **+5.32pt** | ✅ +5pt 達成 → Phase 2b 進行 |
| **popularity** | +odds と ROI ほぼ同値 (79.06% vs 78.94%) | 冗長性確認 |

**Phase 2a 主要数値**:

| variant | features | AUC | TOP1 hit% | tansho ROI |
|---|---:|---:|---:|---:|
| baseline_new (公正比較) | 108 | 0.7783 | 34.00% | 73.73% |
| **+odds** | 109 | **0.8328** | **43.40%** | **79.06%** |
| +odds+popularity | 110 | 0.8336 | 43.40% | 78.94% |

**🚨 マスター基準 110% にはまだ +30pt 不足** → Phase 2b/3/4/5 の合算改善必須。

## 📋 次セッション P0 — M-3 Phase 2b (ROI 期待値 custom objective)

**マスター基準**: hit% ≥ 25.0% AND ROI ≥ 110.0% (両方同時達成)

**Phase 2a 確証**: odds 取込で +5.32pt 改善 = 構造課題は **odds 取込だけでは +5pt 止まり**。**+30pt 不足分は ROI 期待値 loss + 他仮説で積み上げ必要**。

**Phase 2b 合格ライン**: +odds 79.06% → **+5pt 以上** (= 84.06%+) で Phase 2c (全 42 モデル) へ。

| 順 | Phase | 内容 | 工数 |
|---:|---|---|---|
| ~~1~~ | ~~Phase 1 特徴量診断~~ | ✅ 完了 (5/29 前半) — 改善仮説 H1-H6 抽出 | ✅ 1 セッション |
| ~~2a~~ | ~~Phase 2a odds 取込パイロット~~ | ✅ 完了 (5/29 後半) — Δ ROI +5.32pt 達成 | ✅ 1 セッション |
| ~~2b~~ | ~~Phase 2b ROI 期待値 custom objective~~ | ❌ **失敗** (5/29 自走) — 3 試行全て基準未達 / LightGBM custom objective API + sample_weight 両アプローチ崩壊 | ✅ 1 セッション |
| ~~2b'~~ | ~~Phase 2b' 案 D naive post-hoc EV~~ | ❌ 失敗 — argmax(prob×odds) 単純適用で穴馬選びすぎ (ROI 36.40% / -42.66pt) | ✅ 1 セッション |
| ~~2b''~~ | ~~Phase 2b'' 制約付き EV~~ | ✅ **成功** — odds<=10 制約で ROI **85.96%** (+6.90pt) 達成 | ✅ 1 セッション |
| ~~2c~~ | ~~Phase 2c WF 全期間検証~~ | ❌ **EV_oddsmax10 採用棄却** (6 月 WF 加重平均 Δ -3.88pt / 2025-12 +6.90pt は outlier 確定 / 累犯 #19) | ✅ 1 セッション |
| **Phase 2 確定** | **+odds (Phase 2a) +5.32pt が唯一の確実改善** | マスター基準 110% に +25pt 不足 → Phase 3 必須 | — |
| 3 | Phase 3 特徴量追加・再選定 | odds/ペース/隊列強化 + 不要特徴量排除 + WF 再学習 | 1-2 セッション |
| 4 | Phase 4 印選定再較正 | ☆ 常時 + composite 重み変更 + pred.json 再生成 | 1-2 セッション |
| 5 | Phase 5 統合検証 | 馬券種網羅マトリクス + マスター基準達成セル抽出 + UI 反映 | 1 セッション |
| **合計** | | **6-9 セッション / 5-7 週** | |

### L-3 廃止 (マスター指摘 5/26)

> 「WF 真の ROI 72.7% で運用妥当性判断 ← こんなのただのクソ回収率。存在意義なし」

72.7% は明らかに赤字 = 判断するまでもなく不採用。**真の問いは「妥当性」ではなく「どう作り直すか (M-3)」**。

### 一時凍結 (M-3 Phase 5 完了まで)

- **B-3 composite 重み再較正** (= M-3 Phase 4 に統合)
- **B-4 ◎ハズレ救済** (= M-3 Phase 2 で自然解消見込み)
- **D-4 複勝/馬連/ワイド** (= M-3 Phase 5 で網羅検証)
- **D-6 期待値ベース買い目選定** (= M-3 Phase 2 の核心)
- **派 5b 統合実装** (`docs/b3_strategy5b_integration_supplement.md`)

これらは「現状の予想ロジック」前提だったが、マスター指摘 (5/28) で根本改善必須 → M-3 完了後に再評価。

## 📐 P1 設計確定済 (A-3e 完了後実装) — `docs/future_changes_post_a3.md`

- **B-3** composite 重み ROI ベース再較正 (A-3e + 会場別) — 2-3 日
- **B-4** ◎ハズレ無印 20.3% 救済 (B-3 統合可) — 1-2 日
- **D-4** 複勝/馬連/ワイド 導入 (R-1 sim → 実装) — 2-3 日
- **D-6** 期待値ベース買い目選定 (Phase 3 統合後) — 3-5 日
- **A-4** 金沢 -41.5pt 個別調査 (A-3e 反映後再評価)

## ⏸ P3 自然消化

- D-5 三連単高配当 (6 月実運用安定後)
- E-3 NAR L (2201-2600m) backfill (サンプル蓄積)
- E-4 6 月実運用 Phase 効果測定 (時間経過)

## ❌ 棄却タスク

- **戦略 B (shobu_score TOP2) 採用** — D-1c で「現運用 ◎単勝に勝てない」と確定 (5/26)
- **G-1** 印の表示順序最適化 — 「印=実力評価」哲学に一致するため不要 (5/25)

---

## 🟢 過去の完了タスク

過去セッションの完了タスクは git log + handoff_*.md に集約済。本ファイルからは削除した。

参照先:
- 5/26: `memory/handoff_2026-05-26.md` (P0 + 形式統一 + 完結追加 13 件)
- 5/25: `memory/handoff_2026-05-25.md` (大波乱・累犯 #12-15・30/42 通り検証実装)
- 5/24: `memory/handoff_2026-05-24.md` (Phase 1+2-A+3 NAR ML 較正 + 28 件引継ぎ TASKS 化)
- 5/22: `memory/handoff_2026-05-22*.md` (フロントエンド大規模クリーンアップ + 夜間メンテ)
- 5/21: `memory/handoff_2026-05-21.md` (3 頭バグ防御 + M' ダッシュボード改善)
- 5/13: `memory/handoff_2026-05-13.md` (P1 ml_composite_adj WF 再推論)
- 5/12: `memory/handoff_2026-05-12.md` (バックテスト嘘問題 + Walk-Forward 再構築方針)
- 5/11: `memory/handoff_2026-05-11.md` (残タスク 7 件一括 + BAT CRLF 修正)
- 5/9: `memory/handoff_2026-05-09.md` (P2 三件 + P3-2 APScheduler 常駐化)
- 5/6: `memory/handoff_2026-05-06_session_complete.md` (NAR 3 頭立てバグ全修復・19 時間自走)
- 5/5: `memory/handoff_2026-05-05_data_quality_emergency.md` (データ品質緊急修復)
- 5/3-5/4: `memory/handoff_2026-05-04.md` (M' 戦略本実装 + γ案修正)
