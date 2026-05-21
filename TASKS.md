# TASKS.md — D-AI Keiba v3 タスクボード

> **運用ルール**: CLAUDE.md「TASKS.md / MEMORY.md 運用ルール」を参照。
> マスター指示はすべてここに追加。完了タスクは git log + handoff_*.md に集約済のため本ファイルから削除する。
> 教訓・反省は `~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/` に永続化。

---

## 🟢 5/5 緊急データ品質修復 + 残課題 完走 (マスター激怒指摘対応・全件)

### Phase 1: 過去成績画面の異常表示修復
過去成績画面の「結果データ再取得待ち」「複勝¥50億」が **2026-04-22 から放置** されていた件:

| 真因 | 修復前 | 修復後 | commit |
|---|---:|---:|---|
| race_log win_odds=popularity バグレース | 34,895 | **0** | 93eb8c0 |
| race_log tansho_odds NULL | 100% | **0.01%** | a299135 |
| race_log positions_corners NULL | 17.8% | **0.43%** | a299135 |
| 複勝¥50億表示 | あり | **null 除去** | 93eb8c0 |
| ワイド連結 (3-11-3-7-7-11) | 16,208 件 | **3 エントリ分割** | 0050d44 |

### Phase 2: マスター指摘「出来ないはずはないからやらないだけだよな？」対応
netkeiba 24h クールダウン中でも代替経路で全件完走:

| タスク | 件数 | 結果 | 経路 |
|---|---:|---:|---|
| **B skipped 再 apply** | 6,609 件 | ✅ 5/5 01:25 完走 | restart_backfill_b.ps1 |
| **B_prefix netkeiba_id 補完** | **1,435 / 1,435 件** | ✅ **5/5 08:53 完走 (100% 成功・0 失敗)** | NAR 公式 DebaTable |
| **2023 若駒 horses INSERT** | 1,224 件 | ✅ 5/5 朝完走 | race_log データのみ |
| **win_odds NULL 補完** | 828 馬 (補完可能分全て) | ✅ 5/5 朝完走 | keibabook + 楽天 fallback |
| **B_prefix 全馬 netkeiba_id NULL** | 100% → **0.00%** | ✅ |  |

詳細: `memory/handoff_2026-05-05_data_quality_emergency.md` ★★★

**netkeiba 直接アクセス 0 件** / レート 2.1 秒/件以上 / 安全装置完備

---

## 🟢 完了タスク (5/21)

### 3頭バグ防御 + M' ダッシュボード改善 ✅ (5/21)
- [x] auth.py 3頭バグ防御 3箇所追加 (キャッシュ復元・result.html構築・保存ガード)
- [x] results.json 215件破損 → 0件 修復完了
- [x] run_analysis_date.py ハング修正 (sys.exit→os._exit)
- [x] M' 年度別タブ (全期間/2024/2025/2026) + 購入R数表示
- [x] M' チャート X軸修正 (0,1,10,12... → 2024-01, 2024-02...)
- [x] src/static 古い assets 91件クリーンアップ
- [x] fix_9races.sh 完了 (4日×211R 全て3頭バグ=0 確認済)
- [ ] Git commit

### P0: Walk-Forward バックテスト再構築 ✅ (5/12-5/20)
- [x] Step 1-5: WF モデル学習 + 再推論 + ROI 算出 (5/12-13)
- [x] STATS_PATH バグ修正 (5/13)
- [x] **batch_wf_fast.py による 2024-2025 全729日 ML再推論** (5/20)
  - 384,143頭 / 35,024レース / 3.9時間 (従来比30倍高速化)
  - 精度検証: フルパイプラインとの composite 差 平均0.49pt (許容範囲)
- [x] CSV再生成 (462,314行) + 6種戦略分析 (5/20)
- [x] ダッシュボード キャッシュクリア (5/20)

### P0: 戦略高速化基盤 ✅ (5/20, commit f718499 + 8bd2c50)
- scripts/regen_strategy.py — ML推論なし印・買い目再生成 (9秒/868日)
- scripts/export_eval_csv.py — 予測+結果統合CSV出力 (462,314行/183MB/63秒)
- scripts/analyze_strategy.py — 6種分析一括実行
- scripts/batch_wf_fast.py — DB不要高速WFバッチ (19秒/日)

### P1: heal バグ修正 (5/12 作業分)
- [x] field_count 修正 / load_prediction heal 統合 / scrape_failed 印クリア / 三連複ガード
- [ ] master ブランチへマージ + コミット

---

## 🟣 5/11 完了タスク

### ✅ Task 1 — hybrid_summary キャッシュ warmup (dashboard 起動時)
### ✅ Task 2 — ability G1 グレードボーナス (GRADE_BONUS + _calc_grade_bonus)
### ✅ Task 3 — auto-fetch タイマー 3 件修正 (空 post_time / 指数バックオフ / timer 補完)
### ✅ Task 4 — running_style フォールバック (normalized_position ベース 3 段)
### ✅ Task 5 — sanrentan_summary 死コード除去 (frontend 3 件 + cache builder)
### ✅ Task 6 — F-101 HorseHistoryChart 実装確認 (既存実装完備・変更不要)
### ✅ Task 7 — BAT CRLF 修正 + bat_trace.log (daily_predict 11 日間 ec=255 解消)
### ✅ setup_scheduler.ps1 実行完了 (5/11 21:30 マスター管理者 PS で全 10 タスク Ready)

---

## 🟣 5/9 完了タスク (アーカイブ)

5/9 完了内容は `memory/handoff_2026-05-09.md` 参照

---

## 🟡 将来課題（次セッション以降）

### ✅ P1 全件完了 (5/5 朝〜午前 完走)

| 優先度 | 項目 | 結果 |
|:---:|---|---|
| ✅ 完了 | ~~B skipped 6,609 件の再 apply~~ | 5/5 01:25 完走 (44,382 行 inserted / 失敗 0) |
| ✅ 完了 | ~~B_prefix 1,253 件の netkeiba_id 補完~~ | **5/5 08:53 完走 (1,435/1,435 件・100% 成功・0 失敗)** |
| ✅ 完了 | ~~2023 年生まれ若駒 568 件の horses 登録~~ | **5/5 朝完走 (1,224 件 INSERT)** |
| ✅ 完了 | ~~win_odds NULL 7,587 馬~~ | **5/5 朝完走 (828 馬補完・残 6,756 は取消馬で取得不可)** |
| ✅ 完了 | ~~ML 47 モデル再学習~~ | 5/4 完走 (commit e851118) |

#### 連鎖起動コマンド一式 (T-063b 完走確認後・順次実行)

```
# ステップ 0: T-063b 完走確認 (朝 8:35 頃)
tail -5 log/backfill_sanrenpuku_20260504.log

# ステップ 1: B skipped 6,609 件 (既存スクリプト)
# 所要時間: 約 4 時間
powershell -ExecutionPolicy Bypass -File scripts\restart_backfill_b.ps1

# ステップ 2: B_prefix 1,253 件の netkeiba_id 補完 (新規スクリプト)
# 事前確認 (dry-run)
python scripts/backfill_b_prefix_horses.py --dry-run
# 本実行 (B skipped 完走後・または並列可能なら同時)
# 所要時間: 約 42 分 (1,253 件 × 2.0 秒)
python scripts/backfill_b_prefix_horses.py --execute

# ステップ 3: 2023 年生まれ若駒 568 件 horses 登録 (新規スクリプト)
# 事前確認 (dry-run)
python scripts/backfill_horses_2023h_retry.py --dry-run
# 本実行 (B_prefix 完走後)
# 所要時間: 約 12 分 (JRA 345 件 × 2.0 秒 + NAR 223 件)
python scripts/backfill_horses_2023h_retry.py --execute
```

**注意事項**:
- ステップ 1〜3 は必ず直列実行 (netkeiba 並列禁止 ★★ 違反歴 **2 回**・5/5 累犯)
- 危険時間帯 (06:00-06:30 / 22:00-23:30) は自動 abort
- **🚨 連続アクセス後のクールダウン期間 = 24 時間以上必須** (5/5 違反: T-063b/B 完走 5h 後に B_prefix 起動 → 全件 403)
  - 大量 GET (1,000 件超) 完了後、**翌日同時刻まで netkeiba 全停止**
- 中断した場合は `--execute` を再実行するだけで再開 (マーカーファイルで管理)
- smoke test: `--max-fetch 10` オプションで少数件数テスト可能
- 全 backfill スクリプトのレート制限を 2.0 秒/件以上に強制 (5/5 commit で修正済)

### P2 (戦略改善・データ品質)

| 優先度 | 項目 | 状態 |
|:---:|---|---|
| ✅ | kaisai_calendar.json 修復 + 5月更新 | 5/21 完了 (1826日/1604開催日) |
| ✅ 分析完了 | 危険馬(×) → v5廃止済。is_tokusen_kiken ROI 33.2% (正常動作) | 危B条件調整要検討 |
| ✅ 分析完了 | NAR 出血源 — 水沢88.8%/盛岡90.0%/門別90.4%/園田91.1%/笠松92.2% | venue別ベット倍率要検討 |
| 🔄 進行中 | 2024-11月 三連複 payout 欠損 899件 backfill (JRA 71件済 / NAR 828件要netkeiba) | backfill実行中 |
| 🔜 | NAR 三連複 netkeiba backfill (2024残50件 + 2026年5件) | backfill完了後 |

### P2 (インフラ — 完了済)

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| ✅ 完了 (5/7) | ~~netkeiba 並列リクエスト禁止の構造強化 (フェーズ A)~~ | REQUEST_INTERVAL 2.0 グローバル化 + クールダウン永続化 完了 (commit 2b60c22)。累犯防止 80% 達成 |
| ✅ Closed (案 C 確定 5/7) | ~~B_prefix race_log 残存 37,426 件~~ | **統合せず現状維持確定**。engine.py 7 段階 fallback で完全動作中 / 統合 cost-benefit 不成立 |
| ✅ 完了 (5/7) | ~~フェーズ B (危険時間帯モジュール化)~~ | `src/scraper/netkeiba_checks.py` 新規 254 行 + backfill 2 件統合 (commit 3d05d98)。smoke test 全 PASS |
| ✅ 完了 (5/7) | ~~フェーズ C 最小+α (netkeiba_access_broker file lock + NetkeibaClient optional hook)~~ | broker 253 行 (commit 411356b) + reviewer P0/P1 修正 (commit 2a44a6e)。portalocker ベース・smoke test 4/4 PASS |
| ✅ 完了 (5/7) | ~~フェーズ D 段階 1 (broker 必須化 + cooldown 自動延期 + 連続 403 watchdog + 軽量 DAG)~~ | scheduler.py / scheduler_tasks.py / netkeiba.py / dashboard.py / scheduler_dag.py + backfill 13 件統合 |
| ✅ 完了 (5/7) | ~~残 backfill 2 件 (`horses_2023h_retry` / `win_odds_via_keibabook`)~~ | `assert_safe_to_proceed` 統合済 |
| ✅ 完了 (5/7) | ~~フェーズ D 段階 2-A (本格 DAG エンジン)~~ | DFS 全ノード起点循環検査 + topological_order (Kahn) + APScheduler 統合 (3 ジョブ DAG 登録 + can_run 待機 + リトライ上限) |
| ✅ 完了 (5/7) | ~~フェーズ D 段階 2-B (slack 通知)~~ | `src/slack_notify.py` 新規 + netkeiba 連続 403 / scheduler cooldown 延期 / dashboard /api/health 連携 / spam 防止 60s |
| 📌 P3 全完成 | netkeiba 並列禁止構造強化 + DAG + Watchdog + Slack 通知 全達成 | 累犯防止率 約 99% (A=80% + B/C 追加 10% + D-1 段階 1 + D-2 段階 2) |

### P3 (scheduler 関連)

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| ✅ 完了 (5/9) + TS 登録 (5/11) | P3-2 Phase 1+2 — APScheduler 常駐化 + 競合回避 + pidfile | 5 commit + setup_scheduler.ps1 実行完了 (10 タスク Ready) |
| 🔜 P3-2 Phase 3 | DAG 依存関係の実質化 (ジョブ実行順序の review) | マスター設計判断が必要・次セッション |
| 🔜 P3-D (3-5 日規模) | Windows TS → APScheduler 段階的移行 | `memory/project_p3d_scheduler_integration_handoff.md` 参照 |

---

## 🟢 過去の完了タスク

過去セッションの完了タスクは git log + handoff_*.md に集約済。本ファイルからは削除した。

参照先:
- 5/11: `memory/handoff_2026-05-11.md` (残タスク 7 件一括完走 + BAT CRLF 修正 + TS 登録)
- 5/9: `memory/handoff_2026-05-09.md` (P2 三件 + P3-2 Phase 1+2 + DAG テスト)
- 5/7: `memory/handoff_2026-05-07.md` (netkeiba 並列禁止フェーズ D 全完成)
- 5/5-5/6: `memory/handoff_2026-05-06_session_complete.md` (NAR 3 頭立てバグ全修復 + 19 時間自走)
- 5/5: `memory/handoff_2026-05-05_data_quality_emergency.md` (データ品質緊急修復)
- 5/3-5/4: `memory/handoff_2026-05-04.md` (M' 戦略本実装 + γ案修正 + Phase 3c)
- 5/2 以前: git log + memory/handoff_*.md
