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

## 🚨 次セッション P0 (v5 確定)

| 順 | ID | 内容 | 工数 |
|---:|---|---|---|
| **1** | **L-2** | WF backtest 自体の bug 検証 (Layer 2 _horse_history 日付フィルタ等) | 1-2 日 |
| **2** | **L-3** | WF 真の ROI 72.7% で運用妥当性判断 (継続 or ML 再設計) | 半日 |
| **3** | **M-1** | 本番運用 pred.json 運用見直し (タイムスタンプ強制 + 後追い再生成禁止) | 1 日 |
| 4 | データ整理 `_pred_backup.json` / `_pred_prev.json` 削除 or アーカイブ | 半日 |

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
