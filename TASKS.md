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

### 🚨 真の P0 次セッション (M-2 残 方針 3 + 構造修正)

| 順 | ID | 内容 | 工数 |
|---:|---|---|---|
| **1** | **head_win → mark 統合** | ml_composite_adj に head_win z-score 加算 or mark を head_win ベースで再割当 | 1 日 / 1 セッション |
| **2** | **M-2 方針 3** | ROI 観点 特徴量再選定 (SHAP + permute) + head_win 用 FEATURE_COLUMNS_WIN | 1 週 / 2-3 セッション |
| 統合 | 全方針 + WF backtest + 凍結戦略再評価 | 1 セッション |
| **派生課題** | wf_2024 stats 空問題 (2023 以前データ無し) | 1-2 日 |
| **派生課題** | wf_2024 hit% +0.5pt 説明不能変化 (tracker 決定論性) | 1 日 |
| **派生課題** | 方針 2 calibrator 通年化 | 半日 |
| **派生課題** | 方針 4 blend 係数調整 | 半日 |
| **派生課題** | 方針 1 階層モデル化 (現状 global のみ / 期待 +α) | 1-2 日 |

### L-3 廃止 (マスター指摘 5/26)

> 「WF 真の ROI 72.7% で運用妥当性判断 ← こんなのただのクソ回収率。存在意義なし」

72.7% は明らかに赤字 = 判断するまでもなく不採用。**真の問いは「妥当性」ではなく「どう作り直すか (M-2)」**。

### 一時凍結 (L-1 究明完了まで)

- **B-3 composite 重み再較正**
- **B-4 ◎ハズレ救済**
- **D-4 複勝/馬連/ワイド**
- **D-6 期待値ベース買い目選定**
- **派 5b 統合実装** (`docs/b3_strategy5b_integration_supplement.md`)

これらは全て「本番運用 pred.json は信頼できる」前提で計画されていたが、その前提が崩れた。L-1 完了後に再評価。

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
