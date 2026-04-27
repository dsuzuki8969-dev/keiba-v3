# D-AI Keiba v3 — CHANGELOG

## v6.2.0-phase1 （2026-04-27）— Plan-γ Phase 1: ハイブリッド能力指数（絶対 × 他馬比較）

**commit**: 5b9ebbc

### 🎯 背景
能力偏差値が完全タイム比較（コース基準タイム vs 走破タイム）であるため、帯広ばんえい 200m で上限/下限張り付き 50,124 件発生（全張り付きの 98%）。マスター発案「能力指数を他馬比較に変えたら？」を契機にハイブリッド設計（絶対指標を維持しつつ相対指標を追加）を採用。

### ✨ 新機能 (Phase 1: データ層)
- **`race_log.relative_dev` カラム新設**: 過去走 `run_dev` を同 race_id 内で z-score 正規化
- **計算式**: `50 + 10 × clip((run_dev − μ) / max(σ, 5.0), -3, 3)` → 範囲 20.0〜80.0
- **帯広(venue_code=65) 順位ベースフォールバック**: ばんえい特殊仕様に対応
- **field_count<5 / run_dev=NULL はスキップ**: 信頼性なし時は NULL 維持
- **`scripts/backfill_relative_dev.py`**: 全期間バックフィル（--dry-run/--date-from/--date-to/--force）
- **`tests/test_backfill_relative_dev.py`**: 単体テスト 20 件全 pass
- **設定追加** (`config/settings.py`): `RELATIVE_DEV_SIGMA_FLOOR=5.0`, `Z_CLAMP=3.0`, `BANEI_VENUE_CODE=65`, `MIN_FIELD=5`

### 📊 バックフィル結果
- 全期間 723,046 行中 710,439 行 NON-NULL（97.5%）
- **帯広(65) >= 100 張り付き: 24,905 件 → 0 件** ★完全解消
- **帯広(65) <= -50 張り付き: 25,219 件 → 0 件** ★完全解消
- 範囲外異常値（<0 OR >100）: 0 件
- 副次バグ修正: 99=失格コード防御（異常値 -420.1 検出 → 50.0 固定）+ テスト 2 件追加

### 🚧 残 Phase
詳細プラン: `plans/plan-gamma-hybrid-relative-dev.md`
- Phase 2: engine.py で当該レース内 race_relative_dev 計算 + pred.json 出力
- Phase 3: hybrid_total プロパティ + USE_HYBRID_SCORING フラグ + 印付与切替
- Phase 4: ML 特徴量追加 + 再学習（旧モデル保持）
- Phase 5: フロント表示（絶対/相対 切替）+ Plan-β（ZONE_BANDS -50 追従）統合
- Phase 6: バックテスト（絶対 vs ハイブリッド ROI 比較）

---

## v6.1.34 （2026-04-27）— T-017: リアルタイム成績 手動更新ボタン + 自動更新高速化

**commit**: daa921c

### 🎯 背景
マスター指摘「ホーム LIVE STATS の集計が 19R で、終了 24R に対して 5R 遅延している。即座に更新できるボタンが欲しい」。
真因: `_AUTO_FETCH_COOLDOWN_SEC = 60秒` がフロント polling 2 分と相性悪く、連続終了時に未集計レースが最大 5R 積み上がっていた。

### ✨ 新機能
- **「↻ 更新」ボタン** (HomePage TodayStatsPanel): クリック 1 回で未集計レース全取得 + 即集計
  - 5 秒連打防止 / Loading state / aria-label / `useEffect` クリーンアップ
- **`POST /api/force_refresh_today`** 新規 endpoint:
  - threading.Lock 連打防止 (BUSY 409)
  - IP 単位 5 秒 rate limit (RATE_LIMITED 429)
  - 127.0.0.1 + Cloudflare IP のみ許可 (FORBIDDEN 403)
  - date 形式 validation (INVALID_DATE 400)
- **`useForceRefreshToday()` hook** (TanStack Query mutation)

### ⚡ 性能改善
- **`_AUTO_FETCH_COOLDOWN_SEC` 60 → 30 秒**: 自動更新も高速化（マスターがボタン押さなくても遅延 30 秒以内）
- **`_auto_fetch_post_races(force=False)` 引数追加**: cooldown bypass オプション
- **戻り値 dict 化**: {fetched, aggregated, skipped, errors, elapsed_ms}
- **`_count_pending_races(date)` ヘルパー**: DRY 化

### 🔒 セキュリティレビュー (security-reviewer 全 PASS)
- CSRF: 既存 POST endpoint と同一ポリシー (127.0.0.1 のみ)
- IP 偽装防止: CF-Connecting-IP/CF-Ray/X-Forwarded-For 複合チェック (v6.1.19 機構流用)
- date validation: regex `^\d{4}-\d{2}-\d{2}$` + strptime パースのみ (SQL/シェル無し)
- Exception 漏洩なし: クライアント側は `"internal error"` のみ

### 📋 委託体制
- Sonnet.5 (バック) + Sonnet.6 (フロント) を Plan-γ Phase 1 と並列起動
- Sonnet.片付け 3 体（verify / TASKS 更新 / etc）も同時進行 = 同時最大 5 体並列

---

## v6.1.33 （2026-04-27）— Plan-α: ability_total -50 拡張 results_tracker 追従

**commit**: 7f434a8

### 🎯 背景
マスター追求「もう全て変更したの？-50.0〜100.0 で表してる？」から発覚。
真因: `src/results_tracker.py:311` の二重クランプ `max(20.0, ...)` が DEVIATION["ability"]["min"]=-50 拡張（2026-04-26 承認済み）を潰していた。

### 🐛 バグ修正
- **`src/results_tracker.py:311`** の `max(20.0, ...)` → `max(-50.0, ...)` に修正（1 行リテラル変更）
- `AbilityDeviation.total` プロパティは既に -50 でクランプ済み → results_tracker 側の二重クランプが真因

### 📊 検証結果
- 4/28 pred.json で `ability_total = 20.0` 張り付き **176 件 (53%) → 0 件**
- min: 20.00 → **-36.42** で連続的分布回復
- python-reviewer APPROVE / MEDIUM 3 件 (DEVIATION 参照化、calibration の or 0 パターン、テスト追加) は別タスク化

---

## v6 （2026-04-23）

### 🎯 主要成果
**Phase 3 リリース後の集中メンテナンスセッション**。
買い目指南・リアルタイム成績・オッズ反映・スクレイプ信頼性・著作権配慮を網羅的に強化。

### ✨ 新機能
- **ホーム リアルタイム成績パネル**: ◉◎単勝 X-X-X-X + 三連単F 予想/的中/回収率を 2分間隔で自動更新
- **レース詳細タブ固定**: 買い目指南を開いた状態で他Rを押しても買い目指南のまま（sessionStorage 永続化）
- **レース結果・オッズ自動取得**:
  - `/api/home/today_stats` で発走+10分経過レースを裏で自動 fetch
  - `/api/results/race` に同様の auto-fetch 前処理
  - `/api/race_odds` に `auto=true` fire-and-forget モード追加（cooldown 付き）
- **◉ / ◎ 分離集計**: 印別成績で ◉鉄板 を独立行表示（234件/勝率 68%）

### 🐛 バグ修正
- **◉鉄板 誤付与漏れ修復**: 過去 840 日で条件を満たす 234 レースを ◎→◉ に昇格
- **誤 is_scratched 復元**: ML予測ありの 3,439 馬を `is_scratched=False` に復元 + 確率再正規化
- **園田 調教・厩舎コメント取得**: venue_code `50→49` 変更未対応バグを修正（今年から新コード）
- **三連単F セクション欠落（2024/2025/全期間）**: sanrentan_summary キャッシュと `invalidate_aggregate_cache` を連動
- **auto-odds 不完全**: 初期実装が `get_tansho` のみで ticket odds 更新されず → 自己POST で全 tickets_by_mode 更新
- **netkeiba HTTP 429/503 検知 + cooldown**: レート制限時は即フォールバック（threading.Lock 付き）
- **stale bet_decision 修復**: 取消馬ゼロなのに「取消馬により買い目無効」表示残留バグ
- **empty tickets_by_mode.fixed**: 誤取消で tickets が空になったレースを `build_sanrentan_tickets` で再生成
- **三連単F チャート 2024-11 起点問題**: 2024-01〜10 の results.json 欠損を backfill で修復 (OK=2319/15047)
- **白味バグ**: `dashboard.py` で ML予測持ち馬を自動取消扱いしないガード条件追加
- **タイムアウトの UI 固まり**: Home 画面の `useHomeTodayStats` polling は 2分間隔・refetchOnWindowFocus
- **20240108_pred.json 破損修復**

### 🏗️ インフラ改善
- **バックフィル 3 本**: `backfill_recent_days.py` / `backfill_2026_gaps.py` / `backfill_payouts.py` で 2024-01〜2026-04 の欠損を全件補完
- **チェックポイント中断再開**: backfill は `tmp/*_checkpoint.json` で再開可
- **Rate limit**: netkeiba 1.5秒 / 公式 3秒間隔、Lock 付き cooldown

### 🧑‍⚖️ 著作権配慮
- **`bulletize_stable_comments.py` 新規**: 厩舎コメント完コピを規則ベースで bullets 変換（最大 5 要素 × 28 字）
- **調教短評**: 辞書ベース `paraphraseTrainingComment` で意味保持の言い換え（TOP150 フレーズ）
- **規正表現**: JRA/NAR 両形式のプレフィックス除去（`○馬名【調教師】` / `○馬名(短評) 調教師師――`）

### 🛠️ 運用ルール（CLAUDE.md / memory 追加）
- **モデル階層 2 階層**: 既定 Sonnet、Opus は (1) アーキ判断 (2) 複雑バグ根本原因 (3) 並列Explore集約 のみ
- **並列 Explore 推奨**: 未知領域の調査は 1 つずつ grep しない、2-3 並列 Task で一発特定
- **専門 reviewer**: python / typescript / database / security / keiba-reviewer を実装直後に呼ぶ
- **プログレスバー可視化**: Chat 本文に必ず `[████░░░░] X%` + タスクリスト表示（違反歴 2 回目・最重要）

### 🔧 修正ファイル（主要）
#### Backend Python
- `src/scraper/netkeiba.py`（429/503 検知 + Lock cooldown）
- `src/scraper/keibabook_training.py`（園田 venue_code 49 対応）
- `src/scraper/auth.py`（園田 venue 対応に追随）
- `src/dashboard.py`（auto-fetch, auto-odds, ticket 更新拡張）
- `src/results_tracker.py`（invalidate 連動、◉/◎ 分離集計、ML予測ガード）
- `src/engine.py`（bet_decision 最新印で再生成）
- `src/models.py`、`src/calculator/betting.py`、`src/calculator/popularity_blend.py`
- `src/analytics/sanrentan_summary.py`（invalidate_cache）

#### Frontend TS/React
- `frontend/src/api/hooks.ts`（useHomeTodayStats, useSanrentanSummary staleTime 短縮）
- `frontend/src/api/client.ts`（homeTodayStats, OddsRequest に auto 追加）
- `frontend/src/pages/HomePage.tsx`（TodayStatsPanel 追加）
- `frontend/src/pages/ResultsPage.tsx` + `ResultsPage/{SummaryCards,TrendCharts,DetailedAnalysis}.tsx`（三連単F 対応）
- `frontend/src/pages/TodayPage/RaceDetailView.tsx`（auto-trigger useEffect）
- `frontend/src/pages/TodayPage/TabGroup3Horse.tsx`（タブ永続化 sessionStorage）

#### 新規スクリプト
- `scripts/patch_tekipan.py`（◉鉄板 過去遡及付与）
- `scripts/patch_false_scratched.py`（誤取消復元）
- `scripts/renormalize_probs.py`（確率再正規化）
- `scripts/fix_stale_bet_decision.py`（bet_decision 取消理由残留解消）
- `scripts/regen_tickets_today.py`（空 tickets 再生成）
- `scripts/bulletize_stable_comments.py`（規則ベース箇条書き）
- `scripts/inject_training_for_date.py`（調教 training 強制注入）
- `scripts/backfill_payouts.py`（2024-01〜11 全 15047R 払戻再取得）
- `scripts/backfill_recent_days.py`（直近6日分）
- `scripts/backfill_2026_gaps.py`（2026-02〜04 不完全日）

#### 運用・ドキュメント
- `CLAUDE.md`（モデル階層 + Chat可視化ルール追加）
- `~/.claude/projects/.../memory/feedback_model_tiering.md`（新規）
- `~/.claude/projects/.../memory/feedback_progress_bar.md`（違反歴 2 回目記録）
- `~/.claude/agents/keiba-reviewer.md`（新規ドメイン専門レビュアー）
- `~/.claude/agents/security-reviewer.md`（model: sonnet → opus 昇格）

### 📊 データ修復サマリー
| 項目 | 件数 |
|---|---|
| ◉鉄板 追加 | 234 races |
| is_scratched 復元 | 3,439 horses |
| 確率再正規化 | 36,722 races |
| 園田 training 再取得 | 134 horses |
| 厩舎コメント bullets 生成 | 251 horses |
| 2024 payouts backfill | 2,319 三連単取得成功 / 15,047 scanned |
| bet_decision 取消残留修正 | 17 races |
| 空 tickets 再生成 | 7 races |

### ⚠️ 既知の残課題
- 名古屋の調教データは競馬ブック側が「非対応」（構造的制約・対応不可）
- 2026-04 の 10 レースで place2_prob 異常（全馬 0 または 1 頭集中・ML モデル側要調査）
- オッズ 4 段フォールバック（公式→netkeiba→競馬ブック→楽天）は競馬ブック/楽天のオッズスクレイパー未実装のため 2 段まで
- 過去 predictions/_prev.json バックアップ一覧の整備は未対応
