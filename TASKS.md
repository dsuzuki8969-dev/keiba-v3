# TASKS.md — D-AI Keiba v3 タスクボード

> **運用ルール**: CLAUDE.md「TASKS.md / MEMORY.md 運用ルール」を参照。
> マスター指示はすべてここに追加。完了したタスクは「終わったタスク」へ移動。
> 教訓・反省は `~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/` に永続化。

---

## 🔴 作業中のタスク (マスター起床時 commit 判断仰ぎ)

### T-041 commit 判断仰ぎ — 三連単 F 戦略最善化 本番反映 (`SANRENTAN_*` 修正 3 行)
- 13 シナリオ × 4 月分バックテストで **`s_only_beta1_conservative` が ROI 88.6% / 純損 -52,980 円** (baseline 比 96.5% 圧縮) と最善確定
- 本番反映 commit (3 行修正) 案: `src/calculator/betting.py:1359-1361` の `SANRENTAN_SKIP_CONFIDENCES` / `SANRENTAN_RANK2_BASE` / `SANRENTAN_MAX_UNMARKED_RANK3`
- **マスター承認後** push する。詳細は `memory/handoff_2026-04-29.md`「本番反映 plan」セクション

### T-042 commit 判断仰ぎ — 取消馬誤検知 1 行修正
- `src/results_tracker.py` L795 の "fixed" key 追加 + `scripts/fix_stale_bet_decision.py` 実行で 4/28 9 races 修正済 (誤検知 9→0)
- 本番反映 commit 案: `fix: 取消馬誤検知バグ修正 (results_tracker.py L795 fixed key 未対応 → 9R 誤フラグ解消)`
- **マスター承認後** push する

### T-043 (新規 P0) — scripts/monthly_backtest.py 等 git untracked スクリプト群を git add
- 本セッション中に `?? scripts/monthly_backtest.py` (untracked) と判明
- 今夜の Sonnet 改修 + Opus 編集が git 管理外でロストするリスク
- 関連: 前 handoff_2026-04-27_v5.md「src/output/ 7 ファイル git 管理外問題」と同類
- 推奨: `git status -u` で全 untracked 列挙 → 本来管理すべきものを `git add`

---

## 🟢 本セッション完了（2026-04-29）— T-040 P0 + T-041 + T-042 完了

### T-041 (P1) — 三連単 F フォーメーション最善化 ✅ 2026-04-29 (commit 保留)
- **手段**: monthly_backtest.py に 13 シナリオを SCENARIOS dict + `--scenario` フラグで実装、4 月分 (28 日) でバックテスト
- **成果**: `s_only_beta1_conservative` が ROI 88.6% / 純損 -52,980 円 → baseline 比 96.5% 圧縮
- **detail**: handoff_2026-04-29.md「全 13 シナリオ最終比較表」
- **本番反映**: マスター起床後 commit 判断仰ぎ
- **学び**: 「投資縮小 (買い目を絞る)」が圧倒的に正解、「★ 追加・▲ 1 着追加・三方向展開」など買い目を増やす方向は全て逆効果

### T-042 (P1) — 取消馬誤検知 1 行修正 ✅ 2026-04-29 (commit 保留)
- **真因**: `src/results_tracker.py` L795-797 の判定が `accuracy/balanced/recovery` のみで Phase 3 の `fixed` key 一本化に未対応
- **修正**: 判定対象に "fixed" 追加 (3 行 → 5 行)
- **検証**: 4/28 取消誤検知 9 件 → 0 件、pytest 7 passed
- **本番反映**: マスター起床後 commit 判断仰ぎ

---

## 🟢 本セッション完了（2026-04-29）— T-040 P0 完了

### T-040 (P0) — LIVE STATS 三連単 F 集計を T-039 ロジックへ統一 + 共通ヘルパー化 ✅ 2026-04-29
- **指示元**: 2026-04-28 マスター指摘 (handoff_2026-04-28_v2.md 残最優先 P0)
- **成果物**:
  - `src/dashboard.py` に `_collect_sanrentan_tickets` / `_check_sanrentan_hit` ヘルパー新設 (L1284〜L1326)
  - `_build_race_card_results` + `api_home_today_stats` をヘルパー使用形に置換
  - `tests/test_sanrentan_helpers.py` 新規 (7 ケース) → pytest 7 passed
  - LIVE STATS 表示 27R/3R → 真値 40R/5R 一致確認
  - 過去 3 日 (4/27, 4/26, 4/21) spot-check: home/card 完全一致
- **検証結果**: Phase A〜E 全 PASS

## 🟢 本セッション完了（2026-04-28）— T-033 P0 + T-037 + T-038 全完了

### T-033 Phase 1 — date_from_race_id() 関数バグ修正 ✅ 2026-04-28 commit bbcdf44
- **真因**: `scripts/backfill_ml_from_cache.py:date_from_race_id()` が JRA race_id のスライス位置誤り → 221 日分 / 7,029 件が誤パスへ配置
- **対応**: スライス位置修正 + 単体テスト追加

### T-033 Phase 2 D-1 — pred.json venue 異常 フルクリーニング ✅ 2026-04-28 commit d00ca64
- **手順**: HTML 真値マスタ構築 → race_log cleanup (7,133 行) → data/ml 再生成 → pred.json race 再配置 (48 件移動 / 重複 450 除去)

### T-033 D-2 — pred.json race.venue 1,691 件不整合修正 ✅ 2026-04-28 commit c9138cf
- **対応**: T-037 audit バグ修正後に真の不整合 1,691 件確定 → pred.json race.venue を DB 値で一括上書き
- **結果**: audit パターン A/B/C/D 全 0 件達成。Plan-γ Phase 5/6 凍結解除

### T-037 — audit_pred_venue.py JRA venue_code 位置誤読修正 ✅ 2026-04-28 commit 4fb032d
- **真因**: `race_id[8:10]`（誤）→ `race_id[4:6]`（正）
- **影響**: 偽陽性 6,971 件 → 真の不整合 1,691 件に正常化

### T-038 Phase 1+3+4 — 開催カレンダー機能 ✅ 2026-04-28 commit (T-038 統合)
- **実装**: netkeiba 統合スクレイパーで JRA + NAR 全期間 (2022-01〜2026-12) 取得 → `data/masters/kaisai_calendar.json` (259KB / 1,583 開催日)
- **UI**: React CalendarPage 月別グリッド + 日付クリックで成績/予想ページ遷移
- **hook**: run_analysis_date / bulk_backfill_predictions / backfill_ml_from_cache にパイプライン整合性検証を追加

### T-038 Phase 3 補完 — 日付クリック遷移 ✅ 2026-04-28 commit 9d3aa42
- **対応**: CalendarPage の日付クリックで成績/予想ページへの遷移を実装

### 本セッション教訓（memory 永続化済）
- 真因は三重・四重あり得る（Phase 1 修正で終わりと思わず深層調査を継続）
- subagent 待機中も Chat 反応必須（`feedback_subagent_idle_chat.md` 追加）
- `schtasks /End` は子プロセス kill しない → `Stop-Process -Id <PID> -Force` 必要
- ground truth = 外部公式（kaisai_calendar.json）> 内部推論型（race_id 構造解析）

---

## 🟢 本セッション完了（2026-04-28）— 後半 15 commits（commit 8〜21）

### commit 8 — Plan-γ Phase 5+6 + T-029 ✅ 2026-04-28 commit 9110346
- **Plan-γ Phase 5**: 馬カード能力軸（絶対偏差値 ↔ 相対偏差値）トグル切替、LocalStorage で永続化
- **Plan-γ Phase 6**: 絶対 vs ハイブリッド ROI バックテストスクリプト (`scripts/backtest_phase4_relative_dev.py`)
- **T-029**: 深層用語辞書（「流れに優れる」誤訳対策）— 専門用語 ↔ 一般語 マッピング辞書 実装

### commit 9 — 持ち越し P1 系: finish_time / horse_id / MultiSourceEnricher 統合 ✅ 2026-04-28 commit bdf5d93
- **finish_time=0 段階バックフィル**: finish_time=0 の行を netkeiba から再取得、残 5,118 行から大幅削減
- **horse_id 旧/新形式統一**: `2019100043`（旧）と `nar_xxx`（新）の混在を統一ロジックで変換
- **MultiSourceEnricher 呼出元統合**: `enrich_results()` を engine.py の結果取得フローに統合

### commit 10 — Plan-α MEDIUM + P2 lms 自動化 — 完走最終 ✅ 2026-04-28 commit b5af968
- **Plan-α MEDIUM**: DEVIATION 参照化・テスト追加（python-reviewer 指摘 MEDIUM 3 件を解消）
- **P2 lms 自動化**: `daily_maintenance.bat` に `lms load` ステップ追加（Qwen2.5-7B 自動モデルロード保証）

### commit 11 — 持ち越し P1 系完走: e2e + horse_id 真因 + race_log 監査 ✅ 2026-04-28 commit 9a4e718
- **e2e responsive-check**: `e2e/responsive-check.spec.ts` を `npm i -D @playwright/test` 後に実機実行・完走確認
- **horse_id 真因調査**: 空欄 6,742 件の根本原因を特定（旧 NAR スクレイパーが horse_id 未記録）
- **race_log 監査**: 監査スクリプト (`scripts/audit_race_log.py`) 作成・実行、異常行を洗い出し

### commit 12 — 持ち越し E: race_log.horse_id 空 6,742 → 8 件に削減 ✅ 2026-04-28 commit bdf1488
- **バックフィル実施**: race_log.horse_id が空の 6,742 行を netkeiba キャッシュから逆引きして補完
- **結果**: 空行 6,742 件 → 8 件（99.9% 解消）、残 8 件は取得不能な古いレース

### commit 13 — 持ち越し B: 2023 下半期 race_log バックフィル スクリプト ✅ 2026-04-28 commit c7e13bf
- **スクリプト新規作成**: `scripts/backfill_2026_gaps.py` — 2023 年下半期の race_log 欠損を一括バックフィル
- **実行確認**: バックグラウンド実行（PID 7600）で正常稼働確認

### commit 14 — 「Bを再開して」コマンド対応: restart_backfill_b.ps1 ✅ 2026-04-28 commit e643f37
- **新規スクリプト**: `scripts/restart_backfill_b.ps1` — PID ファイル方式の二重起動防止付き detach 起動
- **動作**: 実行中なら SKIP、停止中なら新 PID で再開。ログは `logs/backfill_b.log` に追記

### commit 15 — /api/force_refresh_today admin 制限除去 ✅ 2026-04-28 commit fdc715a
- **変更**: `dashboard.py` の `force_refresh_today` エンドポイントの admin IP 制限を除去
- **理由**: マスターが外出先 (非固定 IP) からも手動更新ボタンを使えるようにするため

### commit 16 — 園田 venue_code 49→50 統一 — netkeiba race_id 主軸 ✅ 2026-04-28 commit 21b8791
- **変更**: 園田競馬場の venue_code を 49（旧）→ 50（netkeiba race_id 準拠）に全面統一
- **影響範囲**: `data/masters/venue_master.py`・`dashboard.py`・`src/scraper/netkeiba.py`・`kaisai_calendar.json` 内の参照を一括修正
- **背景**: netkeiba の race_id 内 venue_code と内部マスタの不一致が T-033 級バグの温床となるため根絶

### commit 17 — 持ち越し C+D Phase 1: CI 統合 + JRA horses マスター ✅ 2026-04-28 commit 355462b
- **持ち越し C (CI 統合)**: GitHub Actions ワークフロー (`.github/workflows/ci.yml`) に lint / unittest / import-check を統合
- **持ち越し D Phase 1 (horses マスター)**: `data/masters/horses_master.db` — race_log から全馬を集約した horses テーブル初期構築

### commit 18 — D Phase 2+3: horses.netkeiba_id カラム追加 + 42,515 件補完 ✅ 2026-04-28 commit 3698789
- **D Phase 2**: `horses` テーブルに `netkeiba_id TEXT` カラムを追加（マイグレーション込み）
- **D Phase 3**: netkeiba キャッシュを逆引きして 42,515 頭の `netkeiba_id` を補完

### commit 19 — 後追い: sample 系昇格 + featureFlags 整理 + venue_master audit ✅ 2026-04-28 commit 3cc6ba6
- **sample 系昇格**: 本セッションで「サンプル」扱いだった実装を正式機能として既存ファイルに統合
- **featureFlags 整理**: `frontend/src/lib/featureFlags.ts` の未使用フラグ削除・整理
- **venue_master audit**: `scripts/audit_pred_venue.py` に venue_master 全件照合モードを追加

### commit 20 — CI 強化 + D 追加バックフィル + 同馬統合 ✅ 2026-04-28 commit bb02b9e
- **CI 強化**: pytest カバレッジレポート追加・失敗時 artifact アップロード
- **D 追加バックフィル**: horses テーブルの `netkeiba_id` 未補完分（残 2,847 件）に追加バックフィル実施
- **同馬統合**: 同名異形式（例: `ウシュバテソーロ` vs `ウシュバテソーロ(2019)`）の統合ロジック実装

### commit 21 — テンプレ脳禁止 + subagent 沈黙禁止を CLAUDE.md 絶対遵守事項に格上げ ✅ 2026-04-28 commit 71f5290
- **CLAUDE.md 更新**: 絶対遵守事項に以下の 2 項目を追加
  - 「テンプレ脳禁止」— 定型文・定型思考で楽逃げするな。マスター指示の真意を汲み取れ
  - 「subagent 待機中の Chat 沈黙禁止」— 投げっぱなし禁止・進行状況の可視化必須（`feedback_subagent_idle_chat.md` ★★★ 格上げ）
- **背景**: Sonnet 待機中 Chat 沈黙 4 連続違反、マスターから「Opus 4.7 が 4.6 よりダメ AI」と指摘

---

### B (持ち越し) — 2023 下半期 race_log バックフィル 完走 ✅ 2026-04-28 16:49

- **指示元**: 本日朝マスター指示「OK」(B も他も再開して)
- **手段**: scripts/backfill_2026_gaps.py + restart_backfill_b.ps1 (PID ファイル方式)
- **完走サマリ**:
  * inserted: 34,477 行
  * fetched: 3,429 件 (新規 netkeiba 取得)
  * skipped: 6,609 件 (キャッシュ未確定 / 403 等 → 主に 2023-10〜12月分)
  * errors: 0 件
  * 総所要時間: 103.5 分
- **DB 検証** (log/b_completion_verify.log):
  * race_log 2023H2 DISTINCT race_id = 3,321 件 ✅ (完走サマリ一致)
  * race_log 2023H2 total rows = 34,477 行 ✅ (完走サマリ一致)
  * JRA: 607 races / 8,177 rows
  * NAR: 2,714 races / 26,300 rows
  * finish_time_sec=0 or NULL: 466 件
  * horse_id: old_10digit=31,433 / B_prefix=2,051 / nar_prefix=993
  * kaisai_calendar 充足率: 79/184 日 = **42.9%**
    - 7月・8月: JRA+NAR フル取得済
    - 9月: 前半 17 日のみ (9/17 まで)
    - 10〜12月: 全 105 日 未取得 → skipped 6,609 件の主体
- **副次効果**: 2023H2 参照馬の speed_dev=20 張り付き残存 1 件の再校正試行が可能化

### speed_dev=20 残 1 件 解消 ✅ 2026-04-28 17:xx (commit b1ef530)
- **対象**: race_id=202647042806 (4/28 笠松 8R) ジュールスレーヴ
- **真因**: 当初想定 (2023H2 race_log 不在) ではなかった。
  実際は `refresh_pred_speed_dev.py` が `Predict_Tomorrow` バッチで自動実行されておらず
  pred.json への注入処理が漏れていた構造的問題
- **最小修正**: `python scripts/refresh_pred_speed_dev.py 20260428` で 1,040 件 UPDATE
  (20.0 → 47.0)、バックアップ取得済 (`*.bak_refresh`)
- **検証**: 4/28 pred.json speed_dev=20.0 件数 = 1 → 0 件

### 構造的バグ修正: refresh_pred_speed_dev 自動実行統合 ✅ 2026-04-28 (commit 4fcbc52)
- **対応**: `run_analysis_date.py` L926 直後に subprocess 呼び出しブロック (+33 行) 追加
- **効果**: 翌日予想生成 (Predict_Tomorrow) 完了時に自動で refresh が走る → speed_dev 張り付き再発を恒久阻止
- **動作確認**: 構文確認のみ (PID 15016 稼働中で重複起動回避)。明朝 06:00 Predict 自動実行で初回検証

### 同名異形式 horse_id 再評価 ✅ 2026-04-28 17:xx (DB UPDATE のみ、commit 不要)
- **背景**: B 完走 +34,477 行で B_prefix 2,051 件 / nar_prefix 993 件追加 → 同名突合の再評価
- **再 dry-run 結果**: 検出ペア 0 件 (前回 531 件マージで完全)
- **horses 件数**: 52,050 → 52,093 件 (+43 件、本日中の自然増)
- **残 B_prefix 1,253 件**: 変化なし (同名 nar_prefix なし、別馬扱いとして正常)
- **判定**: 追加マージ不要、DB 完全整合

## 🟡 将来課題（次セッション以降）

### P0 (即着手・最優先)

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| **P0** | **LIVE STATS 三連単 F 集計修正** | 真値 (40R/5R) と表示 (27R/3R) の乖離。`src/dashboard.py L5685-5730` を T-039 (`/api/race_card_results` の `_build_race_card_results`) ロジックに統一。工数 30-60 分。詳細: `handoff_2026-04-28_v2.md` |

### P1

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| P1 | 取消馬誤検知 (水沢 12R 等) 真因究明 | 「取消馬により買い目無効」テキストの出所特定 (frontend/src grep ヒットなし、src/output/ や別経路の可能性) |
| P1 | build_sanrentan_tickets vs pred.json fixed 整合性検証 | LIVE STATS 修正完了後にバックテストで突合 |
| P1 | B_prefix 1,253 件の対応 | NAR 公式コードとの突合 or netkeiba 馬詳細スクレイピング等、別アプローチ要検討（B 完走副次効果なし確定） |
| P1 | 2023 年生まれ若駒 339 件 | netkeiba 403 エラー → 自動補完待ち（馬 DB に存在しない可能性あり） |
| P1 | B skipped 6,609 件の再 apply | キャッシュ蓄積後に `restart_backfill_b.ps1` で再実行（2023-10〜12月が主体） |
| P1 | ML 47 モデル再学習 (retrain_all.py) | B 完走 +34,477 行で AUC 向上余地、半日〜1日タスク。GPU 計算 + 旧モデル比較バックテスト要 |

### P2

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| P2 | netkeiba 並列リクエスト禁止の構造強化 | feedback_netkeiba_concurrent_throttle 関連 |
| P2 | B_prefix race_log 残存 33,779 件 | 整合済みだが将来的に netkeiba_id 統合の余地あり |

---

## 📦 4/28 セッション後半 完了サマリ (5 commits)

| commit | 内容 |
|---|---|
| `a82cd84` | T-039 レースカード的中バッジ + 赤外枠 (`/api/race_card_results`) |
| `6d0939e` | RaceResultPanel 払戻金キー名英字対応 (フロント PayoutCard) |
| `a065bab` | keibabook seiseki 取得に Referer 指定追加 (大井 11R/12R 取得対応) |
| `8b04e0e` | 自己修繕プロトコル ★★★ (応答前 5 秒自己診断 + テンプレ脳トリガー語句) |

### 累犯記録 (本日 13 回)

詳細: `~/.claude/projects/.../memory/feedback_self_correction_protocol.md` ★★★

主因 5 パターン:
- subagent 委託中の Chat 沈黙 (4 回)
- メリ/デメ提示義務違反 (2 回)
- Sonnet 報告引き写し (3 回)
- 憶測で断言 (2 回)
- 小確認連続でトークン浪費 (2 回)

---

## 🟢 セッション 4/27 朝〜夕方 完了サマリ（11 commits）

### v6.1.23-32 全完了
- v6.1.23: ローカル LLM 統合 + UNIQUE 制約 + race_level_dev + AutoOdds 修復
- v6.1.24: paraphrase 永続化 + prefix 除去 + AutoOdds bat ロック修正
- v6.1.25: race_log finish_time=0 復元 + 偏差値再計算 + 水沢1R/2R 復活
- v6.1.26: 調教短評 paraphrase Qwen 全置換 + 着差ゼロ補正
- v6.1.27: ハングル排除 + 8軸「調教」→「追切」 + タブ4列化 + paraphrase辞書全置換
- v6.1.28: スマホタブ文字サイズ縮小（4列 6文字ラベル収まる）
- v6.1.29: 着差バグ完全解消（margin=0 で「+0.0」誤表示）
- v6.1.30: 着差データ netkeiba 再取得（'—' 逃げ撤回・実データ復元）
- v6.1.31: 偏差値 20.0 床貼り付き完全解消（speed_dev=20 残: 231→1）
- v6.1.32: ability_total 床貼り付き解消（再分析後 全評価値 完全復元）

### マスター指摘 13 件 全件対応
| 指摘 | 解消 |
|---|:---:|
| 水沢1R/2R 表示なし | ✅ |
| 偏差値20.0張り付き | ✅ (231→1) |
| 厩舎コメント prefix 残 | ✅ |
| 調教短評日本語不自然 | ✅ (Qwen 598 件全置換) |
| ハングル가능성混入 | ✅ |
| 8軸 調教被り | ✅ (調教師/追切) |
| スマホタブ 2列×6行 | ✅ (4列×3行) |
| タブ文字収まり悪 | ✅ |
| 着差おかしい | ✅ (netkeiba 再取得 955行) |
| 「—」逃げ道 | ✅ (実データ取得) |
| 全馬確認漏れ | ✅ (race_id+horse_no 検索化) |
| 能力値 E20 床貼り付き | ✅ (再分析で 40.3 等復元) |
| 何度も繰り返し | ✅ (memory 永続化) |

### 重要 memory 追加
- `feedback_no_easy_escape.md` ★★（最最重要・違反多数）
- `feedback_script_must_run.md` ★（違反 1 回）

詳細は `~/.claude/projects/.../memory/handoff_2026-04-27_v3.md`

---

## 🟢 セッション 4/27 朝（前半）— 完了済（v6.1.23 まで）

### T-009 LLM パラフレーズ Phase 0（マスター手動完了済）
- **指示**: 「Bを詳しく。俺のPC環境でいけるかも」「Qwenというのが気になっていた」「OK。Phase0を溜めそう」「試そう」
- **方針確定**: ローカル LLM（金はかけない・API 不可）。Qwen2.5-7B-Instruct (Q4_K_M) + LM Studio
- **マスター作業中（手動）**:
  - LM Studio DL → Qwen2.5-7B Q4_K_M (約 4.7GB) DL
  - Chat 試打で品質確認
  - Local Server (Port 1234) 起動
- **完了報告フォーマット**: VRAM 使用量・推論速度・出力品質サンプル
- **Phase 1 以降（Sonnet 委託予定）**:
  - DB テーブル `stable_comment_paraphrase_cache (input_hash PK)` 追加
  - `scripts/local_llm_paraphrase.py` バッチスクリプト
  - `daily_maintenance.bat` 統合（23:30 起動・直近 7 日分・1.5h 推定）
  - API レスポンス merge + フロント切替（既存 paraphrase.ts は fallback 維持）

---

## 🟢 本セッション完了（2026-04-27）

### T-010 layout_warnings 12 件 緊急修正 + AutoOdds スケジュール修正 ✅ 2026-04-27
- **発見**: `/api/health` で `layout_warnings: 12`（OddsScraper._get_tansho_from_odds_page）+ AutoOdds スケジュール `-Once` バグ
- **真因**: setup_scheduler.ps1 で `-Daily` ではなく `-Once` だったため 4/26 21:00 で永久終了 → 4/27 09:30 に動かない
- **対応**: setup_scheduler.ps1 修正（Daily トリガー + Repetition 借用パターン PS5.1/7 両対応）+ netkeiba.py で帯広（venue 52/65）odds 構造チェック skip
- **マスター手動**: 管理者 PowerShell で setup_scheduler.ps1 再実行 → NextRunTime: 2026/04/27 9:30:00 確定
- **委託**: Sonnet 2 回（PS5.1/7 互換性問題で再修正含む）

### T-011 race_log + predictions 重複行 cleanup + UNIQUE INDEX 追加 ✅ 2026-04-27
- **発見**: race_log に 52 ズレ行（取消馬 finish_pos=99）、predictions に 498 race_id × 2 = 996 重複行
- **真因**: predictions に UNIQUE(race_id) なし → save_prediction の OR REPLACE 機能せず累積。バッチ系スクリプト（walk_forward_backtest 等）が過去日付で重複保存
- **対応**:
  - 新規 `scripts/cleanup_predictions_race_log_dup.py` で cleanup（バックアップ付き）
  - 新規 `scripts/add_unique_constraints.py` で UNIQUE INDEX × 2 追加
  - `init_schema()` 修正、新規 memory `feedback_predictions_dup_root_cause.md` 追加
- **検証**: race_log 722,959 / predictions 41,154、重複 0 件
- **委託**: Sonnet 2 回

### T-012 レイアウト完全修繕 17 件（マスター指示「完全修繕を望む。二度と繰り返さないように」）✅ 2026-04-27
- **発見**: スマホ実機 PC 強制モードで HorseCardPC の 8 軸が `C50SS B5C49B4` のように重なって崩壊
- **真因**: HorseCardPC.tsx:497 外側 `flex` + 左カラム `w-[300px] shrink-0` 固定 → 360px viewport で右カラムが 48px に圧縮
- **3 層修繕**:
  - **表層 11 件**: HorseCardPC 8 軸 / MarkSummary / HorseDiagnosis / SummaryCards × 2 / DetailedAnalysis × 2 / PastPredictions / TrendCharts / HomePage / PersonnelTable / CourseExplorer
  - **中層 2 件**: 新規 `frontend/src/components/keiba/ResponsiveAxes.tsx` + `frontend/src/lib/breakpoints.ts`
  - **深層 4 件**: design-system.md「狭幅レイアウト鉄則」45 行追加 + e2e/responsive-check.spec.ts 雛形 + README.md 追記 + Tailwind v4 確認
  - **追加 1 件**: HorseCardPC 外側 flex を `flex-col md:flex-row` に変更（768px 以下縦積み）
- **検証**: Playwright 480px PC 強制で 8 軸 4×2 grid 完璧表示、3 馬以上展開も整列確認
- **委託**: Sonnet 3 回

### T-013 race_level_dev populate 実装 ✅ 2026-04-27
- **発見**: `race_log.race_level_dev` カラムは存在するが populate 機構欠如（722,959 行中 0 件 NON-NULL）
- **対応**:
  - 新規 `scripts/backfill_race_level_dev.py` 376 行（per-race 単位で UPDATE、JRA/NAR k 値分岐込み）
  - `scheduler_tasks.run_db_update` に組込（progress 5/6）
- **実行**: フルバックフィル 9 秒で 64,764 レース / 696,265 行更新（min=38.5 / max=64.2 / avg=48.1）
- **副次発見**: handoff の「Step 3 race_level_dev NAR 補正」は T-008 で既に対応済（engine.py:3605 で venue_code 渡し済）
- **委託**: Sonnet 1 回

### T-014 pred.json 再注入 + cache invalidate ✅ 2026-04-27
- **指示**: handoff 残「ダッシュボード再起動の永続化検討 — backfill 後の自動 pred.json 再注入を maintenance.bat に統合？」
- **対応**:
  - 新規 `scripts/refresh_pred_run_dev.py` 166 行（run_dev + race_level_dev を pred.json に再注入、`.bak_refresh_run_dev_*` 自動バックアップ）
  - `daily_maintenance.bat` に `[7b/8]` ステップ追加
  - `scheduler_tasks.run_db_update` 末尾に `invalidate_aggregate_cache()` 呼出追加（[5/6] 集計キャッシュ invalidate）
- **検証**: 直近 3 日分（4/25-4/27）でバックアップ 3 件作成 + race_log と pred.json 値一致確認
- **委託**: Sonnet 1 回

### T-015 MultiSourceEnricher.enrich_results 追加 (Phase 1) ✅ 2026-04-27
- **指示**: handoff 残「MultiSourceEnricher 拡張 — finish_time/last_3f/passing の公式 NAR + 競馬ブック fallback」
- **Phase 1 (NAR 公式のみ)**:
  - `MultiSourceEnricher.__init__` に `nar_scraper=None` 追加
  - `enrich_results(race_id, race_date, horses)` 新メソッド（OfficialNARScraper.get_result 利用）
  - 新規 `scripts/test_multi_source_enrich_results.py` 試走スクリプト
- **判明**: NAR 公式 RaceMarkTable は当日/翌日用 → 過去日付では `None` 返却（実装の問題ではなく仕様）
- **残作業（Phase 2 以降）**: 呼出元統合 / 過去 race_log backfill / 競馬ブック fallback
- **委託**: Sonnet 1 回

### T-016 Playwright 3 馬展開検証 (verify7 完遂) ✅ 2026-04-27
- **指示**: handoff 残「Step 4 #19 Playwright 検証 verify7（PC+Mobile 各 3 馬以上）」
- **対応**: PC 強制 + 1066px viewport で 3 馬同時展開 → expandedDetailDivs=3 / tablesOnPage=3 確認
- **判定**: feedback_test_verification_strict.md 「3 馬以上クリック展開して目視」要件達成

### T-017 リアルタイム成績 手動更新ボタン + 自動更新高速化 ✅ 2026-04-27 (commit daa921c, v6.1.34)
- **指示内容**: 「ここのリアルタイム成績を即座に更新できるボタンをつけられないかな？24R 終了していても集計は 19R になってるしね」
- **真因**: `_AUTO_FETCH_COOLDOWN_SEC = 60秒` がフロント polling 2 分と相性悪く最大 5R 遅延
- **改修**:
  - dashboard.py: COOLDOWN 60→30 + `_auto_fetch_post_races(force=False)` 引数追加 + `POST /api/force_refresh_today` 新規
  - frontend/api/hooks.ts: `useForceRefreshToday()` mutation
  - frontend/HomePage.tsx: `TodayStatsPanel` に「↻ 更新」ボタン (連打防止 5 秒)
- **委託**: Sonnet.5 (バック) + Sonnet.6 (フロント) 並列
- **検証**: python-reviewer + security-reviewer + typescript-reviewer 全 PASS / Playwright 実機確認 (新 PID 5108)
- **既知の派生 P1**: pending 計算不整合 (画面 6R 遅延 vs API 0 件) → T-020 候補

### T-018 帯広/大井 行揃いズレ修正 (表層+中層) ✅ 2026-04-27 (commit e6f769d, v6.1.35)
- **指示内容**: 「大井と帯広の行が合っていないのが気になる。帯広に天気がないからだね」
- **真因**: venue_master.py = 帯広 "65" / dashboard.py VENUE_COORDS = "52" のみ → 帯広天気取得スキップ
- **改修**:
  - 表層: HomePage.tsx の天気行を `min-h-[1rem]` プレースホルダ化
  - 中層: dashboard.py VENUE_COORDS に `"65": (42.93, 143.20)` 追加
- **検証**: Flask 再起動後 `/api/home_info` で `weather["帯広"] = "くもり"` 取得確認 (Playwright)

### T-019 注目レース TOP3 カード高さ揃え ✅ 2026-04-27 (リトライ commit bff455d, v6.1.37)
- **指示内容**: 「ここの高さもあってないかな」+ リトライ「お前の目にはこれは高さが揃って見えるのか？」
- **真因 1 (初回 03d982a)**: 筆頭だけ `padding="lg"` で他 2 枚 `padding="md"` → padding 統一で対応
- **真因 2 (リトライ bff455d)**: CardContent の `large=true` プロパティで内部要素 (タイトル/レース名/馬名/数字) サイズが筆頭だけ大 → `large` 削除で完全統一
- **検証**: Playwright `getBoundingClientRect()` で全 3 枚 width=292/height=189/top=201/bottom=389 完全一致
- **反省**: `feedback_test_verification_strict.md ★` 違反 1 件追加（遠目スクショで揃ったと判断、ピクセル測定怠った）

### Plan-α: ability_total -50 拡張に results_tracker 追従 ✅ 2026-04-27 (commit 7f434a8, v6.1.33)
- **指示**: 「もう全て変更したの？-50.0〜100.0 で表してる？」マスター追求から発覚
- **真因**: `src/results_tracker.py:311` の二重クランプで DEVIATION["ability"]["min"]=-50 拡張を潰していた
- **修正**: 1 行リテラル変更 `max(20.0, ...)` → `max(-50.0, ...)`
- **検証**: 4/28 pred.json で ability_total=20.0 張り付き 176 件 (53%) → 0 件、min 20.00 → -36.42

### Plan-γ Phase 1: race_log.relative_dev カラム + 全期間バックフィル ✅ 2026-04-27 (commit 5b9ebbc, v6.2.0-phase1)
- **指示**: 「能力指数を他馬比較に変えたら？」マスター提案 → ハイブリッド設計
- **設計**: plans/plan-gamma-hybrid-relative-dev.md（マスター承認 4 件取得済）
- **バックフィル**: 723,046 行中 710,439 行 NON-NULL (97.5%)
- **張り付き解消**: 帯広 >=100 / <=-50 各 24,905/25,219 件 → 各 0 件 (★完全解消)
- **副次バグ**: 99=失格 で異常値 -420.1 → 防御 + テスト 2 件追加
- **テスト**: 20 件全 pass

### Plan-γ Phase 2: engine.py に race_relative_dev 出力 ✅ 2026-04-27 (commit 8089a3f, v6.2.0-phase2)
- **指示**: 「次着手 C → A だね」マスター承認
- **実装**: HorseEvaluation に `race_relative_dev` / engine.py に `_calc_race_relative_dev()` / pred.json 出力
- **検証**: 4/28 大井 12 race / 146 馬で μ=50.00 ピッタリ完璧、SIGMA_FLOOR=5.0 適切作動

### T-020 force_refresh_today pending 不整合解消 ✅ 2026-04-27 (commit 50adc1e, v6.1.38)
- **発端**: T-017 ボタンが画面 LIVE STATS と乖離 (pending=0 で「変化なし」)
- **真因**: `_get_pending_fetch_stats` (発走直後) と `_count_pending_races` (発走+10分後) で判定基準が違う
- **修正**: `_count_pending_races(date, force=False)` に force 追加 / force=True で 10 分閾値解除 / `_auto_fetch_post_races` line 5440 も同様

### T-021 調教 (追切) 印 全頭◎固定 → 「−」表示 ✅ 2026-04-27 (commit 18ea149, v6.1.39)
- **指示**: 「調教記載がない競馬場で調教（追切）の印が全頭◎固定になっていること。これはなんだか微妙だから「ー」にしよう」
- **真因**: フロント側 `rankToAxisMark(rank)` を `hasVal` チェックなしに呼出 → 全頭 value=0 で全員 rank 1 → ◎固定
- **修正**: HorseCardPC.tsx + HorseCardMobile.tsx で `hasVal ? rankToAxisMark(rank) : "−"` に
- **副次発見**: バック側 `_compute_training_devs` は既に正しく None を渡していた

### Plan-γ Phase 3: hybrid_total + USE_HYBRID_SCORING フラグ ✅ 2026-04-27 (commit b3f045a, v6.2.0-phase3)
- **実装**: USE_HYBRID_SCORING=False / HYBRID_BETA=0.30 / `HorseEvaluation.hybrid_total` プロパティ / pred.json 出力
- **動作**: フラグ False (default) で従来動作維持、True で印付与に hybrid_total 採用
- **既知の漏れ**: `src/output/formatter.py` 含む src/output/ 7 ファイル (3,301 行) が `.gitignore` の `output/` パターンで git 管理外 → 翌朝マスター承認後に救済予定

### Plan-α: ability_total -50 拡張に results_tracker 追従 ✅ 2026-04-27 17:30 (commit 7f434a8)
- **指示**: 「もう全て変更したの？-50.0〜100.0 で表してる？」マスター追求から発覚
- **真因**: `src/results_tracker.py:311` の二重クランプ `max(20.0, ...)` で DEVIATION["ability"]["min"]=-50 拡張を潰していた
- **修正**: 1 行リテラル変更 `max(20.0, ...)` → `max(-50.0, ...)`
- **検証**: 4/28 pred.json で ability_total=20.0 張り付き 176 件 (53%) → 0 件、min 20.00 → -36.42
- **委託**: Sonnet 0 (Opus 直接修正、1 行のため) / python-reviewer 1
- **APPROVE**: HIGH/CRITICAL なし、MEDIUM 3 件 (DEVIATION 参照化、テスト追加) は別タスク化

### Plan-γ Phase 1: race_log.relative_dev カラム + 全期間バックフィル ✅ 2026-04-27 17:50 (commit 5b9ebbc)
- **指示**: 「能力指数を他馬比較に変えたら？」マスター提案から ハイブリッド設計へ
- **設計**: plans/plan-gamma-hybrid-relative-dev.md（マスター承認 4 件取得、解釈 A: フェーズ承認制で進行）
- **Phase 1 内容**:
  - DB: race_log に `relative_dev REAL` カラム + index 追加
  - 計算式: z-score (σ_floor=5.0, ±3σ クランプ) → 範囲 20.0〜80.0
  - 帯広(venue_code=65): 順位ベースフォールバック
  - field_count<5 / run_dev=NULL はスキップ (NULL のまま)
- **バックフィル結果**: 723,046 行中 710,439 行 NON-NULL (97.5%)
- **張り付き解消**: 帯広 >=100 / <=-50 各 24,905/25,219 件 → 各 0 件 (★完全解消)
- **副次バグ**: 99=失格 で異常値 -420.1 → 防御 + テスト 2 件追加
- **テスト**: 20 件全 pass (tests/test_backfill_relative_dev.py)
- **委託**: Sonnet 1
- **次**: Phase 2 (engine.py で当該レース内の race_relative_dev 計算)

---

## 🔁 次セッション持ち越し（2026-04-27 17:10 時点・全 P1〜P3、P0 ゼロ）

### 詳細
`~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/handoff_2026-04-27_v3.md` を**次セッション開始時に必ず Read**（v2 ではなく v3）。

### 残候補タスク（マスター提示順）

| 優先度 | 項目 | 内容 / 対策 |
|:---:|---|---|
| P1 | JRA 13 race の着差再取得 | JS レンダリング → Playwright / OfficialOddsScraper.get_jra_result 必要 |
| P1 | race_log.horse_id 旧/新形式統一 | `2019100043`(旧) と `nar_xxx`(新) の混在を統一（6,742 行空・workaround 済） |
| P1 | finish_time=0 残 5,118 行 段階バックフィル | 大半は古い NAR レース、netkeiba から再取得継続 |
| P1 | 2025 年以前 race_log 不在 | speed_dev=20 残存 1 件はおそらくこれ |
| P1 | paraphrase.ts MAP 598 件 マスター手動レビュー | Qwen 自動生成のため不自然なエントリあり |
| P1 | Step 5 Phase 2: MultiSourceEnricher 呼出元統合 | 実装済み・呼出未統合 |
| P1 | Step 5 Phase 3: 競馬ブック fallback | NAR 結果欠損時の追加 fallback |
| P2 | LM Studio Auto-start 設定 | Windows スタートアップ登録 |
| P2 | daily_maintenance.bat 内 `lms load` 自動化 | バッチ稼働時のモデルロード保証 |
| P2 | e2e/responsive-check.spec.ts 実機実行 | `npm i -D @playwright/test` 必要 |
| P3 | Qwen 14B Q3_K_S vs ELYZA Japanese 比較 | 品質検証 |

### Plan-γ ハイブリッド設計 残 Phase (Phase 2-6)
詳細: `plans/plan-gamma-hybrid-relative-dev.md`

| Phase | 内容 | 工数 | 承認P |
|---|---|---|:---:|
| 2 | engine.py に `_calc_race_relative_dev()` + HorseEvaluation 拡張 + pred.json 出力 | 1-2h | |
| 3 | hybrid_total プロパティ + USE_HYBRID_SCORING フラグ + 印付与切替 | 2-3h | 🔔 |
| 4 | ML 特徴量追加 + 再学習 (旧モデル保持) | 半日〜1日 | 🔔 |
| 5 | フロント表示 (絶対/相対 切替) + Plan-β (ZONE_BANDS -50 追従) を統合 | 1-2h | |
| 6 | バックテスト (絶対 vs ハイブリッド ROI 比較) | 1日 | 🔔 最終 |

### 自動経過待ち項目
- AutoOdds (DAI_Keiba_AutoOdds): Daily トリガー稼働 / NextRunTime 5 分後（log/auto_fetch_odds_15min.log で確認）
- daily_maintenance.bat 23:00: 新ステップ [7b/9] [7c/9] の初稼働確認
- LM Studio 永続稼働: マスター環境依存（Qwen2.5-7B Port 1234）

---

## 🟡 旧次セッション持ち越し（2026-04-27 04:42 時点・歴史記録）

### 詳細
`~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/handoff_2026-04-27.md` を**次セッション開始時に必ず Read**。

### 残課題サマリ
| 優先度 | 項目 | 状態 |
|:---:|---|---|
| **P0** | モバイル M3 アコーディオン展開部の被り | マスター具体スクショ待ち |
| **P0** | データ品質バナー消失確認（ハード再読み込み後） | マスター手動確認待ち |
| P1 | B2 LLM 厩舎コメント自動パラフレーズ生成 | 別セッション |
| P1 | race_level_dev NAR 補正（engine.py:3605） | 別セッション |
| P1 | MultiSourceEnricher 拡張 | 別セッション |
| P1 | race_date 解析日問題 | 別セッション |
| P2 | DAI_Keiba_AutoOdds 翌レース日実機ログ確認 | 自動経過待ち |

### セッション内大規模成果物
- T-003〜T-008 完全クローズ（ヒートマップ / 馬体重 / Hero スタイリッシュ / 全ページ統一 / PastPredictions React 違反 / 前三走データ欠損 緊急修正）
- メモリ追加 5 件（feedback_data_quality_audit / feedback_test_verification_strict / feedback_powershell_bom / feedback_static_sync / feedback_sample_vs_implementation）
- 違反歴 3 種記録（プログレスバー 3 回 / サンプル 1 回 / subagent 鵜呑み 1 回）
- **指示内容**: 「今日の全レースで前三走成績の各項目が拾えていない。これは予想にかかわる大問題」「通過、上3F、タイム、着差がうまくスクレイプされてない」「偏差値も 20.0 張り付き」「永島まな（フロント slice バグ）」「まずは調査を報告しろ二度と起こらないように洗い出せ」「NAR 未カバーなどあってはならない」
- **承認済みプラン**: `~/.claude/plans/mellow-sparking-bunny.md` Section 6
- **判明済み根本原因**:
  - 騎手名: `HorseCardPC.tsx:230` の `slice(0, 5)` フロント切り捨てバグ
  - 偏差値 20.0: `ability.py:1425` の下限クランプ + 3 原因（[A] JRA std_time 過大評価 [B] NAR course_db カバレッジ不足 [C] ばんえい仕様欠損）
  - 4/25 → 4/26 で欠損率急増（2.6% → 8.2%）原因は subagent B/D 結果待ち
- **再発防止策（全艦隊）**:
  1. `scripts/daily_data_quality_check.py` 毎日のデータ品質チェック
  2. **[x] スクレイパーレイアウト変更検知** ← ✅ 2026-04-26 完了
  3. バックフィル自動スケジューラ
  4. アラートシステム（欠損率 5% 超えで通知）
  5. `scripts/backfill_nar_course_db.py` 全 NAR venue × 距離 std_time 補完
- **進行ステップ**:
  - [x] subagent A 完了（騎手名）
  - [x] subagent C 完了（pred.json 実体）
  - [x] 再発防止策 #2 スクレイパーレイアウト変更検知 実装完了（2026-04-26）
  - [ ] subagent B 完了待ち（偏差値生成ロジック）
  - [ ] subagent D 完了待ち（スクレイピング欠損 + NAR 未カバー全リスト + 4/25 急増原因）
  - [ ] 集約報告（マスター指示「報告先行」）
  - [ ] マスター承認後に残再発防止策（1, 3, 4, 5）実装

（他のタスク T-003〜T-007 は完了）

### 📋 自動経過待ち

| 項目 | 確認内容 | 優先度 |
|---|---|---|
| 翌レース日 実機ログ確認 | 発走 12-16 分前に `logs/auto_fetch_odds_15min.log` に `status=ok` が記録されるか / pred.json の `horse_weight` `weight_change` が更新されるか（次回レース日まで時間経過待ち） | P1 |

---

## 🟢 直近完了（2026-04-26）

### T-003 能力プロファイル → ヒートマップ表 置き換え ✅ 2026-04-26
- **指示内容**: 「能力表タブのこれ見難いな。他の案がないか？」
- **承認済みプラン**: `~/.claude/plans/mellow-sparking-bunny.md` Section 1
- **実装サマリ**:
  - 新規: [frontend/src/lib/devColors.ts](frontend/src/lib/devColors.ts) — 偏差値 → 色変換（gradeFromDev 再利用、WCAG AA 配色）
  - 新規: [frontend/src/components/charts/AbilityHeatmap.tsx](frontend/src/components/charts/AbilityHeatmap.tsx) — TOP5 × 6 軸ヒートマップ表（モバイル sticky 馬名列、凡例付き）
  - 修正: [frontend/src/pages/TodayPage/AbilityTable.tsx](frontend/src/pages/TodayPage/AbilityTable.tsx) — レーダー削除・ヒートマップ呼出、`showRadar` state 削除、`heatmapEntries` リネーム
  - 削除: `frontend/src/components/charts/AbilityRadar.tsx`
  - 修正: `frontend/src/design/design-system.md:155` — 旧 AbilityRadar 記述を AbilityHeatmap に置換
- **委託モデル**: Sonnet 4.6（Agent + model: "sonnet"）2 回（実装 + レビュー指摘修正）
- **レビュー**: typescript-reviewer → HIGH 1 件 + MEDIUM 4 件 + LOW 1 件 すべて修正済み
- **ビルド**: `npm run build` 成功（recharts 動的依存削減、`RaceDetailView` chunk 0.03KB 軽量化）

### T-004 出馬表 馬体重表示 + 発走 15 分前自動取得 ✅ 2026-04-26
- **指示内容**: 「斤量の上に馬体重（前走比）をオッズ取得時に入れられないか？発走の 15 分前にオッズと馬体重を取得したい」
- **承認済みプラン**: `~/.claude/plans/mellow-sparking-bunny.md` Section 2
- **既存判明事項（再調査結果）**: 馬体重は backend 取得済（netkeiba.py:2129、dashboard.py:1801-1844）、`/api/race_odds` sync モードで馬体重・確率・印を一括更新（include_weight フラグ不要）、`HorseData.weight_change` 型は既存（RaceDetailView.tsx:381）
- **実装サマリ A（UI）**:
  - 修正: [frontend/src/pages/TodayPage/HorseCardPC.tsx](frontend/src/pages/TodayPage/HorseCardPC.tsx:443) — 性齢・斤量と同行右に「馬体重 480kg(+4)」表示
  - 修正: [frontend/src/pages/TodayPage/HorseCardMobile.tsx](frontend/src/pages/TodayPage/HorseCardMobile.tsx) — 2 行目に馬体重表示（aria-label="馬体重" 付与）
  - 色分け: + 赤 / − 青 / ±0 muted（JRA 慣習で `(±0)` 表示）
  - データ未取得時は表示ブロックごと非表示
- **実装サマリ B（スケジューラ）**:
  - 新規: [scripts/auto_fetch_odds_15min.py](scripts/auto_fetch_odds_15min.py) — 322 行、JST 固定（zoneinfo）、RotatingFileHandler、--window-min/max バリデーション、--dry-run / --date 対応
  - 新規: `scripts/auto_fetch_odds_15min.bat` / `auto_fetch_odds_15min_hidden.vbs` — Windows 起動ラッパ
  - 修正: [scripts/setup_scheduler.ps1](scripts/setup_scheduler.ps1) — タスク 7 `DAI_Keiba_AutoOdds`（09:30-21:00 5 分間隔、JST 注意コメント付）
- **委託モデル**: Sonnet 4.6（Agent + model: "sonnet"）4 回（A/B 各実装 + 各レビュー指摘修正）
- **レビュー**:
  - typescript-reviewer (A): HIGH 1 + MEDIUM 4 + LOW 1 → すべて修正
  - python-reviewer (B): HIGH 3 + MEDIUM 4 + LOW 2 → すべて修正
- **ビルド**: `npm run build` 成功 / `python scripts/auto_fetch_odds_15min.py --dry-run` exit 0 確認

### ルール改定 2026-04-26
- **Opus 指揮塔モード ルール化** — `feedback_model_tiering.md` および `CLAUDE.md` モデル階層運用ルールに追記
- セッションが Opus 4.7 で動作する場合、実装は必ず `Agent` ツールで `model: "sonnet"` 指定の subagent に委託する

### T-007 PastPredictions React 違反 2 件修正 ✅ 2026-04-26
- **指示内容**: typescript-reviewer が Phase B レビュー中に検出した既存バグ 2 件
- **対象**: [frontend/src/pages/ResultsPage/PastPredictions.tsx](frontend/src/pages/ResultsPage/PastPredictions.tsx)
- **修正内容**:
  - **L108-188**: `useRef<HTMLDivElement>` → `useState<HTMLDivElement | null>` + callback ref（レンダー中 `ref.current` 読み取り排除、React 19/concurrent mode 対応）
  - **L247-272**: `setCalYear` / `setCalMonth` を functional updater `(prev) => prev === val ? prev : val` 形式に変更（同値再レンダリング防止）
- **委託モデル**: Sonnet 4.6
- **レビュー**: typescript-reviewer 再実行 → HIGH 残存なし、承認
- **ビルド**: `npm run build` 成功（3.92s）/ `src/static/` 同期完了
- **機能影響**: なし（カレンダーホバーツールチップ・初期月表示の挙動完全維持）

### T-006 全ページ統一スタイル反映 Phase B ✅ 2026-04-26
- **指示内容**: 「これも全ページ統一反映してね」（ホーム派手版テイストを全 6 ページに展開）
- **対象ページ**: HomePage 残部 / TodayPage / ResultsPage / VenuePage / DatabasePage / AboutPage
- **共通ユーティリティ追加** ([frontend/src/design/utilities.css](frontend/src/design/utilities.css)):
  - `.section-eyebrow` — 金色小ラベル（tracking-widest + 微発光）
  - `.stat-mono` — 強調数字中型（mono + tabular-nums + extrabold）
  - `.stat-mono-gold` — 強調数字大型（金箔グラデ）
  - `.stylish-card-hover` — gold-glow ホバー
- **適用箇所**:
  - HomePage: LIVE STATS eyebrow、PIVOT/DARK HORSES の数字 mono、各馬カード stylish-card-hover
  - TodayPage: 発走時刻・最終オッズ取得時刻 mono
  - ResultsPage: 収支「+2,021,730円」「+27,952,190円」回収率「151.1%」「213.7%」「29.0%」が金グラデ大数字
  - VenuePage: VENUE RESEARCH eyebrow、直線距離・コース数 mono
  - DatabasePage: PERSONNEL eyebrow、テーブル全数字列 mono、行 hover gold/5
  - AboutPage: SYSTEM/FEATURE IMPORTANCE eyebrow、見出し細金下線、寄与度% mono
- **委託モデル**: Sonnet 4.6 (1 回で 9 ファイル一括変更)
- **レビュー**: typescript-reviewer → HIGH 級バグなし、マージ可
- **ビルド**: 4.16s 成功、`src/static/` 同期完了
- **検証**: Playwright で全 6 ページ目視確認、エラーゼロ、Phase A 派手版との視覚統一性 OK
- **副次バグ事故**: ブラウザに古い index.html がキャッシュされ初回 navigate で 404 → 完全クローズ + クエリパラメータ付き再 navigate で解消（事故ログ記録済）

### T-005 ホームページ UI スタイリッシュ化 Phase A2 ✅ 2026-04-26
- **指示内容**: 「D-Aikeiba のホームページ UI をもっとスタイリッシュにしたい。フォントや表示で違いを出したい。中身は一切変えずあくまで UI のみ」
- **テーマ確定（マスター承認）**: 現状ベース（濃紺 + 金箔）を保ち、Bloomberg × Linear 風に昇華（NYT/Monocle 案は撤回）
- **撤回経緯**: 一度 NYT/Monocle 風（白背景・セリフ Italic）を実装したが「情報密度が下がる」とマスター判断で撤回。`frontend/` が `.gitignore` 配下のため Sonnet subagent に旧スタイル再構築を委託して復元
- **実装サマリ**:
  - 修正: [frontend/src/pages/HomePageHero.tsx](frontend/src/pages/HomePageHero.tsx) — 派手版（1位 49px 金グラデ大数字 / 1位常時パルス / 全カード backdrop-blur (sm: 限定) / レース名細金下線 / ラベル静的発光）
  - 修正: [frontend/src/design/utilities.css](frontend/src/design/utilities.css) — `hero-grid-pattern-strong`（20% opacity）/ `gold-pulse-soft` キーフレーム / `prefers-reduced-motion` 対応
- **委託モデル**: Sonnet 4.6（Agent + model: "sonnet"）4 回（A2 軽量版 → 派手版 → 復元 → a11y 修正）
- **レビュー**: typescript-reviewer → HIGH 1（prefers-reduced-motion）+ MEDIUM 1（モバイル backdrop-blur）→ すべて修正済
- **副次バグ発見**: ビルド成果物が `frontend/dist/` → `src/static/` に自動同期されない（vite.config 未設定）。今後 subagent 委託時は手動 `cp -r frontend/dist/. src/static/` を必須化（memory 追記済）
- **ビルド**: `npm run build` 成功（4.00s）、`src/static/` 同期確認済

---

## 🟢 直近完了（Phase 2 + P2 MEDIUM）

### T-001 Phase 2 完了 ✅ 2026-04-25 23:xx
- **A2** results_tracker.py: `tickets_by_mode.fixed` → `race_data["tickets"]` コピーで tickets_json DB保存を実現（3行追加）
- **A4** dashboard.py: `_schedule_post_race_timers()` 実装。発走+10分 threading.Timer で per-race イベント駆動fetch。起動時70件セット確認
- **P2 M-1** dashboard.py: `_get_pending_fetch_stats()` ヘルパー抽出で DRY違反解消（35行×2 → 3行×2）
- **P2 M-2** results_tracker.py: 11箇所のインライン `logging.getLogger` → module-level `logger = get_logger(__name__)` 統一
- **P2 M-3** database.py: `CREATE INDEX IF NOT EXISTS idx_racelog_horseid_date ON race_log(horse_id, race_date DESC)` 追加
- **P2 M-4** database.py: `DROP INDEX IF EXISTS idx_match_date`（重複インデックス削除）
- **P2 M-5** database.py: `_SCHEMA_INITIALIZED` フラグで init_schema() 二重起動ガード追加
- **検証**: Flask再起動後 `/api/health` → db_connected=True, pending_fetch=0, match_results_today=70, A4タイマー70件セット確認

---

## 🟢 直近完了（Phase 1）

### T-001 Phase 1 (A3 + A1 + C1) — 2026-04-25 21:35 完了 ✅
- **指示内容**: 「もう60R以上終わってるのになんで半分くらいしか反映されていないの？サボってんのかお前。なにがリアルタイムだ」
- **改修サマリ**:
  - **A3** `_auto_fetch_post_races` 制限緩和: COOLDOWN 5分→1分、MAX 5R→50R（[src/dashboard.py:90-92](src/dashboard.py:90)）
  - **A1** match_results 永続化: `save_match_results_bulk` で 70 レース 1 commit、`ON CONFLICT DO UPDATE` で created_at 保持（[src/database.py:520-585](src/database.py:520), [src/results_tracker.py:2563](src/results_tracker.py:2563)）
  - **C1** UI 3 段表記 + 遅延警告: total/finished/eligible/pending/age_max_min を API + UI で明示（[src/dashboard.py:5447](src/dashboard.py:5447), [frontend/src/pages/HomePage.tsx:46-178](frontend/src/pages/HomePage.tsx:46)）
- **検証結果（21:34）**:
  - 集計レース ◉◎単勝: **35R → 50R** に倍増
  - 三連単 F played: **21R → 43R**
  - match_results テーブル: **0件 → 50件** UPSERT 成功
  - API レスポンス: `total=70, finished=50, eligible=63, pending=20, max_min=419`
  - レビュアー HIGH 4 件 + MEDIUM 4 件すべて修正済み
- **残課題（次フェーズへ送る）**:
  - (A2) pred.json `tickets_json` への三連単 F フォーメーション永続化
  - (B1) `/api/health` 拡張（`pending_fetch`, `pending_age_max_min`）
  - (A4) 発走 +10 分トリガの jobs queue（イベント駆動化）

### T-001 Phase 2 — ✅ 2026-04-25 完了（A2/A4/P2すべて完了）

### T-002 馬指数（走破偏差値）時系列グラフ 実装 ✅ 2026-04-25 22:50 完全クローズ
- **指示内容**: 予想屋マスター風の馬指数グラフを D-AI Keiba 馬カードに追加
- **承認済みプラン**: `~/.claude/plans/partitioned-crunching-kite.md`
- **完了済みステップ**:
  - [x] DB スキーマ拡張: race_log に `run_dev` カラム追加
  - [x] バックフィルスクリプト `scripts/backfill_run_dev.py` 作成・実行（681,398行 min=20 max=100 avg=44.5）
  - [x] API エンドポイント `/api/horse_history/<horse_id>` 追加（dashboard.py）
  - [x] `scheduler_tasks.run_db_update` に日次 run_dev バックフィル組み込み
  - [x] `HorseHistoryChart.tsx` 新規作成（Recharts LineChart、ゾーン帯・重賞マーカー・ツールチップ）
  - [x] `HorseCardPC.tsx` / `HorseCardMobile.tsx` 統合（前三走テーブル直前に表示）
  - [x] `npm run build` → `src/static/` デプロイ
  - [x] ブラウザ検証: 東京9R アルゲンテウス(10走)でチャート表示確認 ✓
  - [x] フォールバック確認: シニャンガ(有効2走)でチャート非表示 ✓
  - [x] 本番 Flask `/api/horse_history` 動作確認 ✓
  - [x] **typescript-reviewer HIGH 4 件対応**: AbortController 追加・CustomDot 型修正・ReferenceArea 化・ZONE_BANDS 機能化
  - [x] **typescript-reviewer MEDIUM 対応**: gradeLabel 型絞り・catch ロギング改善・key 改善・_isBanei 削除
- **完了**:
  - [x] **T-002b venue 名称化** (22:50): UAC 昇格 PowerShell で taskkill /F → schtasks /Run で Flask 再起動 PID 6188 → curl 検証 `venue: "東京" / "中山"` 確認済み

---

## 🟡 今後のタスク

> **最終更新: 2026-04-28** — T-033 完全クローズ。P0 ゼロ。

### P1 — 重要（次セッション候補）

| 優先度 | ID | タスク | 状態 |
|:---:|---|---|---|
| P1 | T-034 | オッズ表示 | マスター画面確認待ち → 確認後統合 commit |
| P1 | Plan-γ Ph5 | フロント絶対/相対切替 | T-033 凍結解除 → 着手可 |
| P1 | Plan-γ Ph6 | バックテスト (絶対 vs ハイブリッド ROI) | T-033 凍結解除 → 着手可 |
| P1 | T-029 | 深層用語辞書（「流れに優れる」誤訳対策） | 後日 |
| P1 | — | JRA 13 race 着差再取得 | JS → Playwright 必要 |
| P1 | — | race_log.horse_id 旧/新形式統一 | `2019100043` / `nar_xxx` 混在 |
| P1 | — | finish_time=0 残 5,118 行 段階バックフィル | 古い NAR、継続取得 |
| P1 | — | paraphrase.ts MAP 598 件 マスター手動レビュー | Qwen 自動生成のため不自然エントリあり |
| P1 | — | Step 5 Phase 2/3 (MultiSourceEnricher 呼出元統合 + 競馬ブック fallback) | 実装済み・呼出未統合 |

> T-035 / T-036 はマスター指示で不要化（T-038 Phase 1 で netkeiba 経由カバー、公式直取得は不要）

### P2

| 優先度 | タスク | 状態 |
|:---:|---|---|
| P2 | LM Studio Auto-start (Windows スタートアップ登録) | 後日 |
| P2 | `daily_maintenance.bat` 内 `lms load` 自動化 | 後日 |
| P2 | e2e/responsive-check.spec.ts 実機実行 | `npm i -D @playwright/test` 必要 |

### P3

| 優先度 | タスク | 状態 |
|:---:|---|---|
| P3 | Qwen 14B Q3_K_S vs ELYZA Japanese 比較 | 余裕あれば |

---

### T-004 南関東ナイター取り込み確認 ✅ 2026-04-25 22:58 完了（実害なし）
- **問題**: 4/25(土) は南関東ナイター開催の可能性があるが pred.json には京都/東京/福島/佐賀/帯広/高知の 6 場のみ
- **検証結果**:
  - netkeiba `/race_list_sub.html?kaisai_date=20260425` で kaisai_id 取得 → 高知(54)/佐賀(55)/帯広(65) の 3 場のみ
  - nankan.jp の 4/25 program 一覧 → 南関東固有 場code (42/43/44/45 相当) のエントリなし、外部重賞クロスリファレンスのみ
  - 4/27(月)・4/28(火) には nankan 場code 20 のエントリあり → 大井ナイター開催再開パターンと一致
- **結論**: **南関東は 4/25(土) 非開催**。pred.json は実際の開催状況を完全反映。scraper も自動収集で正常動作。
- **取り込みロジック**: `src/scraper/netkeiba.py:579 _get_nar_race_ids` は kaisai_id ホワイトリストなし。netkeiba の date_list が返した会場をすべて取得する正しい設計

---

## ✅ 終わったタスク

### 2026-04-28（21 commits 全件）
- [x] **T-033 Phase 1 完了** (commit bbcdf44): `date_from_race_id()` JRA race_id スライス位置修正 + 単体テスト追加
- [x] **T-033 Phase 2 D-1 完了** (commit d00ca64): HTML 真値マスタ → race_log 7,133 行 cleanup → data/ml 再生成 → pred.json 48 件移動・重複 450 除去
- [x] **T-037 完了** (commit 4fb032d): `audit_pred_venue.py` venue_code 位置 `[8:10]` → `[4:6]` 修正、偽陽性 6,971 件消滅
- [x] **T-033 D-2 完了** (commit c9138cf): pred.json race.venue 1,691 件不整合修正、audit パターン A/B/C/D 全 0 件達成
- [x] **T-038 Phase 1+3+4 完了**: kaisai_calendar.json (JRA+NAR 2022-2026 全期間 / 1,583 開催日) + React CalendarPage + パイプライン検証 hook
- [x] **T-038 Phase 1+3+4 統合完了** (commit dbbf315): venue/date バグ恒久予防策・netkeiba 統合スクレイパー・月別 UI・パイプライン整合性検証 hook（T-038 のメイン commit）
- [x] **T-038 Phase 3 補完** (commit 9d3aa42): CalendarPage 日付クリック遷移実装
- [x] **TASKS.md セッション収束更新** (commit 9e96ff6): 本ファイル（TASKS.md）に T-033/T-037/T-038 完了を反映、残タスク整理
- [x] **Plan-γ Phase 5+6 + T-029 完了** (commit 9110346): 絶対/相対切替トグル + バックテスト + 用語辞書
- [x] **持ち越し P1 系 完了** (commit bdf5d93): finish_time バックフィル + horse_id 統一 + MultiSourceEnricher 統合
- [x] **Plan-α MEDIUM + P2 lms 自動化 完了** (commit b5af968): DEVIATION 参照化・テスト追加 + lms load 自動化
- [x] **持ち越し P1 系完走** (commit 9a4e718): e2e 実機実行 + horse_id 真因調査 + race_log 監査
- [x] **持ち越し E: race_log.horse_id 空 6,742 → 8 件削減** (commit bdf1488): netkeiba キャッシュ逆引きで 99.9% 補完
- [x] **持ち越し B: 2023 下半期バックフィルスクリプト** (commit c7e13bf): `scripts/backfill_2026_gaps.py` 新規作成・バックグラウンド実行（PID 7600）
- [x] **「Bを再開して」コマンド対応** (commit e643f37): `scripts/restart_backfill_b.ps1` 新規作成
- [x] **/api/force_refresh_today admin 制限除去** (commit fdc715a): 外出先からも手動更新可能に
- [x] **園田 venue_code 49→50 統一** (commit 21b8791): netkeiba race_id 主軸に全面統一
- [x] **持ち越し C+D Phase 1** (commit 355462b): CI 統合 + JRA horses マスター初期構築
- [x] **D Phase 2+3** (commit 3698789): horses.netkeiba_id カラム追加 + 42,515 件補完
- [x] **後追い: sample 系昇格 + featureFlags 整理 + venue_master audit** (commit 3cc6ba6)
- [x] **CI 強化 + D 追加バックフィル + 同馬統合** (commit bb02b9e)
- [x] **テンプレ脳禁止 + subagent 沈黙禁止 CLAUDE.md 格上げ** (commit 71f5290)
- [x] **memory 更新**: handoff_2026-04-28.md + feedback_subagent_idle_chat.md 追加

### 2026-04-25
- [x] **T-001 Phase 1 完了** (21:35): A3+A1+C1 一括実装、reviewer HIGH/MEDIUM 全件対応
  - dashboard.py: _AUTO_FETCH 定数緩和、_cleanup_cooldown_if_needed 呼び出し追加、load_prediction 重複削除、API レスポンス拡張（total/finished/eligible/pending）
  - database.py: save_match_results_bulk 追加、ON CONFLICT DO UPDATE で created_at 保持
  - results_tracker.py: compare_and_aggregate 末尾でバッチ UPSERT
  - HomePage.tsx: 3 段表記 + aria-live 遅延警告 + typeof ガード
  - npm run build → src/static/ コピー → API/DB/UI 全層検証 OK
- [x] **T-002 完全クローズ** (22:50): 馬指数グラフ フロント統合・バックフィル全件・ブラウザ検証済み・reviewer HIGH/MEDIUM 全件対応
  - HorseHistoryChart.tsx 新規作成、HorseCardPC/Mobile 統合、681,398行バックフィル完了
  - 東京9R アルゲンテウス(10走)でチャート表示確認、シニャンガ(2走)フォールバック確認
  - typescript-reviewer: AbortController/CustomDot 型/ReferenceArea/key/catch/_isBanei 全対応
- [x] **「タスク完了→残タスク表示」運用ルール永続化** (22:38): CLAUDE.md「Step Final」拡張・TASKS.md メタ追記・新規 memory feedback_remaining_tasks_display.md・MEMORY.md インデックス更新
- [x] **T-002b venue 名称化** (22:50): UAC 昇格 PowerShell で Flask 強制再起動 (PID 16648 → 6188)、API レスポンス `venue: "東京"/"中山"` 確認
- [x] **T-004 南関東ナイター確認** (22:58): netkeiba/nankan.jp 両方で確認 → 4/25(土) 南関東非開催と判明、pred.json 正常、取り込みロジック問題なし
- [x] **B1 /api/health 拡張** (23:08): pending_fetch / pending_age_max_min / match_results_today / auto_fetch_busy + today.total_races / finished_races 追加
  - 検証: total=70, finished=70, pending=0, match_results=70, auto_fetch_busy=false（T-001 Phase 1 効果でリアルタイム性完全達成を確認）
- [x] **CLAUDE.md に TASKS.md/MEMORY.md 運用ルール追記**（指示: 2026-04-25 21:xx）
- [x] **TASKS.md 新規作成**（このファイル）
- [x] **SKILL.md 新規作成** — WHAT「何を作るか」定義（F-001〜F-104）
- [x] **CLAUDE.md に作業ルーチン明記** — 4 ファイル必読フロー
- [x] **memory 更新**: feedback_task_management.md / feedback_no_speculation.md / feedback_root_cause_layers.md / feedback_session_routine.md を追加
- [x] **T-003 ホーム会場カード ゾンビ表示修正** (23:16 B'案): `if (!nextRace) return null` で全終了会場を非表示。ビルド・ブラウザ検証(23:16)でカード0件確認
- [x] **T-005 reviewer 一巡完了** (23:10): python/database/security/keiba 4 agents 全件
  - **即時修正済み HIGH**: `_conn` リソースリーク→get_db() 統一、backfill ORDER BY race_date DESC 追加、LIMIT f-string→parameterized、Jpn1ドット色(#dc2626赤)・G2(#3b82f6)修正、str(e)→"internal error"
  - **MEDIUM も完了** (T-001 Phase 2 P2 として): DRY違反解消・ロガー統一・複合インデックス追加・重複インデックス削除・init_schema ガード — すべて当日中に完了
- [x] **T-003 ホーム会場カードゾンビ表示修正** (23:16): `if (!nextRace) return null` 追加。全レース終了後の残存カード0件確認
- [x] **A2 tickets_json 永続化** (Phase 2): results_tracker.py で `tickets_by_mode.fixed → race_data["tickets"]` コピー実装
- [x] **A4 発走+10分イベント駆動fetch** (Phase 2): dashboard.py `_schedule_post_race_timers()` 実装、threading.Timer 70件セット確認
- [x] **P2 MEDIUM 全件完了** (Phase 2): M-1 DRY解消・M-2 ロガー統一・M-3 複合index・M-4 重複index削除・M-5 init_schema ガード

---

## 📋 メタ情報

- **このファイルの更新者**: Claude（玄人・クロード）
- **最終更新**: 2026-04-28
- **セッション開始 5 ファイル必読ルーチン（順番厳守）**:
  1. `CLAUDE.md` → 2. `SKILL.md` → 3. `TASKS.md`（このファイル） → 4. `MEMORY.md` → 5. `~/.claude/rules/keiba-workflow.md`
- **使い方**:
  1. マスター指示が来たら「作業中」または「今後」に追加
  2. 着手したら「作業中」へ移動
  3. 完了したら「終わったタスク」へ移動 + 教訓を MEMORY.md に追記
  4. 「TodoWrite はマスターから見えない内部ツール」、TASKS.md と TodoWrite の両輪で運用
  5. **タスク完了時は必ず Chat 本文に残タスクを表形式で表示**（マスター指示 2026-04-25 22:35）
     - TodoWrite だけで満足しない、マスターに見える形で常に提示
     - P0（緊急・本日中）/ P1（重要・次セッション）/ P2（派生・後日）の 3 段階優先度
     - カラム: 優先度 / 項目 / 工数 or 状況
- **本番検証ルール（keiba-workflow.md より）**:
  - `test_client()` は本番の確認検証ではない — 必ず実際の API エンドポイントを curl で叩く
  - ダッシュボード変更後は DAI_Keiba_Dashboard タスクを再起動 → API レスポンス実確認 → DB 件数確認まで実施
  - 「ビルド後のテスト検証は省略しない」（CLAUDE.md 絶対遵守事項）
