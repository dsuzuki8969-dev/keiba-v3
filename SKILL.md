# SKILL.md — D-AI Keiba v3 機能・成果物定義（What）

> **役割**: 「**何を作るか**」の定義。プロダクトが提供する機能・成果物・到達目標を網羅。
> 進行中の作業手順は `TASKS.md`（DO）、進め方のルールは `CLAUDE.md`（HOW）、達成済みの履歴は `MEMORY.md`（DONE）を参照。

---

## 🎯 プロダクト到達目標

D-Aikeiba は、文字や数字の羅列しかない競馬情報を、各ファクタで評価基準を設けて全頭見える化し、市場に騙されない本当の馬の力をはかるシステム。

**KPI 必達ライン**（詳細: memory/project_kpi_targets.md）

| 自信度 | 的中率 | 単勝回収率 |
|---|---|---|
| SS | 60.0% | 150.0% |
| S | 50.0% | 120.0% |
| A | 40.0% | 100.0% |
| B | 25.0% | 90.0% |

---

## 📦 既存機能（実装済み・運用中）

### F-001 全頭見える化エンジン
- **責務**: netkeiba/JRA/NAR から収集 → SQLite 永続化 → ML 分析 → 印付与
- **主要ファイル**: `src/engine.py`（RaceAnalysisEngine）、`src/ml/lgbm_model.py`（159 特徴量・47 モデル階層）
- **出力**: `data/predictions/YYYYMMDD_pred.json`

### F-002 印付与・偏差値ロジック
- **責務**: ◉◎○▲△★×☆ の印を全レース全馬に付与、20-100 統一偏差値レンジ
- **主要ファイル**: `src/calculator/grades.py`（dev_to_grade）、`src/calculator/ability.py`（calc_run_deviation）
- **永続化**: pred.json `horses[].mark` / `composite_dev` 等

### F-003 三連単 F フォーメーション買い目生成
- **責務**: confidence S/A/B のレースに対し、SS/C/D を除いて買い目フォーメーションを生成
- **主要ファイル**: `src/calculator/betting.py`（generate_sanrentan_formation）、`scripts/monthly_backtest.py`（build_sanrentan_tickets）
- **定数**: `SANRENTAN_SKIP_CONFIDENCES = {"SS", "C", "D"}`（src/calculator/betting.py:1359）
- **⚠️ 既知の弱点**: pred.json の `tickets_json` に永続化されていない（T-001 で改修予定）

### F-004 LIVE STATS（本日のリアルタイム成績）
- **責務**: 当日の◉◎単勝成績と三連単F収支をホーム画面で逐次表示
- **主要ファイル**: `src/dashboard.py:5324`（api_home_today_stats）、`frontend/src/pages/HomePage.tsx`（TodayStatsPanel）
- **集計関数**: `src/results_tracker.py:2505`（compare_and_aggregate）
- **⚠️ 既知の弱点**: 「リアルタイム」を謳いながら結果取り込みが追いつかない（T-001 で改修予定）

### F-005 馬詳細カード（HorseCard）
- **責務**: 1 頭ずつの偏差値・前 3 走・厩舎コメント・調教情報を展開表示
- **主要ファイル**: `frontend/src/pages/TodayPage/HorseCardPC.tsx` / `HorseCardMobile.tsx`
- **データソース**: `/api/race_prediction` の `horses[]`

### F-006 React + Vite SPA ダッシュボード
- **責務**: Flask API + React SPA で全機能を Web 化、Cloudflare Tunnel で公開
- **主要ファイル**: `src/dashboard.py`（Flask）、`frontend/src/`（React、`npm run build` → `src/static/`）
- **インフラ**: DAI_Keiba_Dashboard / DAI_Keiba_Tunnel / DAI_Keiba_Watchdog タスク

### F-007 結果照合・回収率トラッキング
- **責務**: レース結果と予想を突合、券種別・印別の的中率・回収率を集計
- **主要ファイル**: `src/results_tracker.py`、`scripts/monthly_backtest.py`
- **DB**: `match_results` テーブル（⚠️ 現在 INSERT が機能していない）

### F-008 全走行データ永続化
- **責務**: race_log テーブル（49 カラム）に走行履歴を全件保存
- **主要ファイル**: `src/database.py`（schema 定義）
- **カラム例**: finish_time_sec, last_3f_sec, race_level_dev, run_dev（馬指数用、追加済み）

### F-009 開催カレンダー（T-038）
- **責務**: JRA + NAR 全開催日（2022-01〜2026-12）を外部公式情報として取得・管理し、パイプライン整合性の ground truth として活用
- **主要ファイル**: `data/masters/kaisai_calendar.json`（259KB / 1,583 開催日）、`frontend/src/pages/CalendarPage.tsx`
- **UI**: 月別グリッド表示 + 日付クリックで成績/予想ページへ遷移
- **パイプライン hook**: `run_analysis_date.py` / `bulk_backfill_predictions.py` / `backfill_ml_from_cache.py` にカレンダーマスタとの照合を追加
- **設計思想**: race_id 構造解析（内部推論型）は脆弱。外部公式 = ground truth の原則を徹底

### F-010 馬指数絶対/相対切替（Plan-γ Phase 5）
- **責務**: 馬カード能力軸の表示を「絶対偏差値（全馬比較）」と「相対偏差値（当該レース内比較）」でトグル切替
- **主要ファイル**: `frontend/src/pages/TodayPage/HorseCardPC.tsx` / `HorseCardMobile.tsx`、`frontend/src/lib/featureFlags.ts`
- **永続化**: LocalStorage に切替状態を保存し、ページリロード後も維持

### F-011 オッズ表示（T-034）
- **責務**: レースカードに馬名・勝率・オッズ・人気を併記。本命◎かつ 5 番人気以下で金色強調表示（穴馬ハイライト）
- **主要ファイル**: `frontend/src/pages/TodayPage/HorseCardPC.tsx` / `HorseCardMobile.tsx`
- **データソース**: `/api/race_odds` レスポンスの `tansho_odds` / `popularity` フィールド

### F-012 horses マスター（D Phase 1+2+3）
- **責務**: race_log から全競走馬を集約した horses マスターテーブルを構築し、netkeiba_id で外部 ID 紐付け・同名異形式の統合を実現
- **主要ファイル**: `data/masters/horses_master.db`（horses テーブル）
- **規模**: 42,515 頭の `netkeiba_id` 補完済み（残 2,847 件は追加バックフィル継続中）
- **統合ロジック**: 同名異形式（例: `ウシュバテソーロ` vs `ウシュバテソーロ(2019)`）を horse_id で統合

### F-013 「Bを再開して」コマンド（restart_backfill_b.ps1）
- **責務**: バックフィルプロセス（backfill_b）の中断再開を単一コマンドで安全に実行
- **主要ファイル**: `scripts/restart_backfill_b.ps1`
- **機能**: PID ファイル方式の二重起動防止 + detach 起動。実行中なら SKIP、停止中なら新 PID で再開。ログは `logs/backfill_b.log` に追記

### F-014 深層用語辞書（T-029）
- **責務**: 調教コメント・レース展開記述の誤訳・不自然訳を防ぐための競馬専門用語 ↔ 一般語マッピング辞書
- **目的**: 「流れに優れる」等の誤訳パターンを辞書ベースで修正し、Qwen paraphrase の品質を向上
- **主要ファイル**: `src/nlp/racing_term_dict.py`（新規）

---

## 🧪 計画中・実装途中の機能

### F-101 馬指数（走破偏差値）時系列グラフ（T-002 進行中）
- **何を作るか**: 馬カード展開時に「走破偏差値の時系列推移」LineChart を表示。予想屋マスター MI 風だが Y 軸は D-AI 統一偏差値レンジ
- **主要設計**: `~/.claude/plans/partitioned-crunching-kite.md`
- **進捗**: バックエンド完了、フロントエンド未着手

### F-102 LIVE STATS リアルタイム性 根本改善（T-001 進行中）
- **何を作るか**:
  - イベント駆動アーキテクチャ（発走 +10 分トリガで結果取得 → match_results UPSERT → キャッシュ無効化）
  - SLA 定義（「結果反映は発走 +10 分以内」）
  - `/api/health` 拡張（`pending_fetch`, `pending_age_max_min`）
  - UI 分母明示（「集計 N / 終了 M / 対象 K」）
  - pred.json `tickets_json` への買い目永続化
- **進捗**: 設計提案済み、マスター優先度判断待ち

### F-103 ホーム画面「次レース」表示改善（T-003 計画中）
- **何を作るか**: 各会場「次の 1 レース」だけでなく「進行中／未発走」を全件タイル表示

### F-104 南関東ナイター取り込み確認（T-004 調査中）
- **何を作るか**: 大井・川崎・船橋・浦和の取り込み有無を確認、漏れていればスクレイパー設定修正

---

## 🏗️ アーキテクチャ全景

```
[netkeiba/JRA/NAR]
        ↓ scraper（src/scraper/）
   [SQLite DB]（src/database.py）
        ↓ engine（src/engine.py）
   [ML 分析]（src/ml/, src/calculator/）
        ↓
   [pred.json]（data/predictions/）
        ↓ dashboard（src/dashboard.py）
   [Flask API + React SPA]
        ↓ Cloudflare Tunnel
   [マスター閲覧]
```

---

## 📋 制約・前提

- **netkeiba レート制限**: time.sleep() でレート制限。並列リクエスト禁止
- **SQLite WAL モード**: 読み取りは並列安全、書き込みはシリアル
- **MLモデル**: ~2GB メモリ。ProcessPoolExecutor 非推奨
- **データ取得優先順位**: 公式（JRA/NAR）→ 競馬ブック → netkeiba

---

## 📝 メタ情報

- **このファイルの目的**: プロダクトとして「何を提供するか」を一元管理。新セッションでマスター・Claude の双方が「現在の機能セット」と「将来の機能計画」を即把握するため
- **追記ルール**: 新機能の計画が立ったら `F-XXX` 番号で追加。実装完了したら「既存機能」セクションへ昇格
- **最終更新**: 2026-04-28
