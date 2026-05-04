# TASKS.md — D-AI Keiba v3 タスクボード

> **運用ルール**: CLAUDE.md「TASKS.md / MEMORY.md 運用ルール」を参照。
> マスター指示はすべてここに追加。完了したタスクは git log + handoff_*.md に集約済みのため本ファイルから削除する。
> 教訓・反省は `~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/` に永続化。

---

## 🔴 作業中のタスク

### T-068 (P0) — 本セッション最終 commit + push
- 5/4 朝時点 未 commit 修正 (`src/frontend/` 配下 untracked):
  - frontend/src/api/client.ts (MPrimeTopPayout 型追加)
  - frontend/src/components/keiba/RaceCard.tsx (単勝バッジ削除)
  - frontend/src/components/keiba/StatsCard.tsx (三連複表記修正)
  - frontend/src/pages/ResultsPage/SummaryCards.tsx (三連複 TOP10 + balance 修正)
  - src/analytics/hybrid_summary.py (top_payouts 集計)
  - src/dashboard.py (HTML 欠損 race_id 補完)
  - src/static/* (再ビルド)
- 工数: 5 分

### T-054 (P0・継続) — DAI_Keiba_Dashboard タスクスケジューラ起動経路修復
- 5/1 タスクスケジューラ再登録済み (setup_scheduler.ps1 完了)
- DAI_Keiba_Dashboard が「logon」trigger で起動・現状動作中
- 残課題: T-046 経路 (vbs/bat) は不要になった可能性 (要確認)
- 工数: 15 分 (確認のみ)

### T-046 Phase 2 (P0・継続) — bat_trace.log 確認
- 診断装置 commit 72d18c6 で仕込み済 → 5/1 06:00 で初検証可能 (済 → 結果未確認)
- 確認手順は handoff_2026-04-30.md「Step 1-4」参照
- 工数: 15 分 + 修正

### T-065 (P0・継続) — 能力指数 構造的過小評価の調査
- γ案で海外除外は対応したが ability_total そのものが G1 実績馬で低い問題は残る
- クロワデュノール (G1 3 勝) ability_max=55.67 / total=54.80
  → タガノデュード (実績下) ability_max=62.25 / total=58.89 が上回る
- 真因仮説: G1/G2/G3 勝利数の直接ボーナス未加算 / クラス補正の効きが弱い
- 工数: 90-120 分 (調査) + 実装

### T-047 (P1・継続) — 結果自動取得不全 構造修正
- 真因 (b) 確定: `_auto_fetch_post_races` がブラウザ polling 依存の fire-and-forget で無人放置時に完全停止
- 暫定対応 (30 分): `DAI_Keiba_Watchdog` (5 分間隔) に `/api/home/today_stats` 自己 HTTP リクエスト追加
- 構造修正 (60 分・推奨): `src/dashboard.py` に `_start_background_result_fetcher` スレッド追加 (Flask 起動時 10 分間隔で自律実行)

### T-058 (P1・継続) — engine.py running_style バグ恒久対策
- 現象: 5/1 4頭で running_style/predicted_corners 空
- 暫定: results_tracker フォールバック追加済 (commit d622506)
- 真因仮説: engine.py L1466 `ev.pace=None` または `_style_map` 欠落
- 工数: 60-90 分

### T-063 (P1・継続) — Backfill 完了済データの最終検証
- 全期間で異常値ないか確認 (現在 ROI 200%+ 確認済)
- 月別・印別・自信度別の整合性チェック
- 工数: 30 分

---

## 🟡 将来課題（次セッション以降）

### P1

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| P1 | 取消馬誤検知 (水沢 12R 等) 真因究明 | 「取消馬により買い目無効」テキストの出所特定 (frontend/src grep ヒットなし、src/output/ や別経路の可能性) |
| P1 | build_sanrentan_tickets vs pred.json fixed 整合性検証 | LIVE STATS 修正完了後にバックテストで突合 |
| P1 | B_prefix 1,253 件の対応 | NAR 公式コードとの突合 or netkeiba 馬詳細スクレイピング等、別アプローチ要検討 |
| P1 | 2023 年生まれ若駒 339 件 | netkeiba 403 エラー → 自動補完待ち（馬 DB に存在しない可能性あり） |
| P1 | B skipped 6,609 件の再 apply | キャッシュ蓄積後に `restart_backfill_b.ps1` で再実行（2023-10〜12月が主体） |
| P1 | ML 47 モデル再学習 (retrain_all.py) | B 完走 +34,477 行で AUC 向上余地、半日〜1日タスク。GPU 計算 + 旧モデル比較バックテスト要 |

### P2

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| P2 | netkeiba 並列リクエスト禁止の構造強化 | feedback_netkeiba_concurrent_throttle 関連 |
| P2 | B_prefix race_log 残存 33,779 件 | 整合済みだが将来的に netkeiba_id 統合の余地あり |

---

## 🟢 過去の完了タスク

過去セッションの完了タスクは git log + handoff_*.md に集約済み。本ファイルからは削除した。

参照先:
- 5/3-5/4: `memory/handoff_2026-05-04.md` (M' 戦略本実装 + γ案修正 + Phase 3c + 三連複高配当 TOP10)
- 5/2: `memory/handoff_2026-05-02.md` (バックフィル復旧 + 9 パターン三連複バックテスト)
- 5/1: `memory/handoff_2026-05-01.md` (LIVE STATS 三連複切替 + paraphrase)
- 4/30: `memory/handoff_2026-04-30.md` (T-050 採用戦略確定 A-NONE)
- 4/29: `memory/handoff_2026-04-29.md` (3 券種ハイブリッド戦略確定)
- 4/28: `memory/handoff_2026-04-28_v2.md` (T-039 + 自己修繕プロトコル)
- 4/26-27: `memory/handoff_2026-04-27_v5.md` (Plan-α/γ + 自走 14 commits)
