# D-AI Keiba v3 システム完全仕様書

**作成日**: 2026-04-01
**バージョン**: v3.0.0
**Python**: >= 3.11
**プロジェクト総サイズ**: 34.0 GB（331,795ファイル）

---

## 目次

1. [システム概要](#1-システム概要)
2. [タイムライン（日次ワークフロー）](#2-タイムライン)
3. [エントリーポイント一覧](#3-エントリーポイント)
4. [スケジューラーシステム](#4-スケジューラー)
5. [データ収集（スクレイパー）](#5-データ収集)
6. [データベース](#6-データベース)
7. [データマスター](#7-データマスター)
8. [補助DB構築](#8-補助DB)
9. [分析エンジン](#9-分析エンジン)
10. [MLパイプライン](#10-MLパイプライン)
11. [確率・オッズ計算](#11-確率オッズ)
12. [印判定・出力生成](#12-印判定出力)
13. [結果照合・KPI](#13-結果照合)
14. [フロントエンド（React SPA）](#14-フロントエンド)
15. [レガシーフロントエンド](#15-レガシーフロントエンド)
16. [Dashboard API全エンドポイント](#16-API)
17. [インフラ・運用](#17-インフラ)
18. [スクリプト一覧](#18-スクリプト)
19. [ツール一覧](#19-ツール)
20. [テスト](#20-テスト)
21. [データファイル一覧](#21-データファイル)
22. [設定ファイル](#22-設定)
23. [ドキュメント](#23-ドキュメント)
24. [現在のKPI](#24-KPI)

---

## 1. システム概要

競馬（JRA中央・NAR地方）の予想・分析システム。
LightGBM 47分割モデル + PyTorch Neural Ranker のアンサンブルを中核に、
人気・オッズの影響を排除した純粋なデータ分析で全レースを予想する。

```
scraper (データ収集) → database (SQLite) → engine (分析) → output (HTML/JSON)
                                             ↑
                                          ml (LightGBM/PyTorch予測)
                                          calculator (能力値・ペース計算)
                                             ↓
                                        dashboard (Flask + React SPA)
```

---

## 2. タイムライン（日次ワークフロー）

```
[T-1日 17:00] 予想生成（scheduler: job_prediction）
  ├─ レースID取得（netkeiba/NAR公式）
  ├─ 出馬表・過去走・血統データ収集
  ├─ 補助DB事前構築（8種類）
  ├─ 各レース並列分析（ThreadPoolExecutor）
  │   ├─ Phase A: 能力値（ability）
  │   ├─ Phase B: 展開予測（pace）
  │   ├─ Phase C: コース適性（course）
  │   ├─ Phase D: 騎手評価（jockey）
  │   ├─ Phase E: 調教師評価（trainer）
  │   ├─ Phase F: 血統評価（bloodline）
  │   ├─ Phase G: ML確率予測（5モデルアンサンブル）
  │   ├─ Phase H: コンポジット算出（6因子加重平均）
  │   └─ Phase I: 印判定（◉◎○▲△★☆×）
  ├─ pred.json 保存
  ├─ HTML出力生成
  └─ 発走前オッズ更新ジョブ登録

[T日 06:00] 朝オッズ更新（scheduler: job_odds_morning）
  ├─ 最新オッズ取得（netkeiba/公式API/楽天競馬）
  ├─ 馬体重反映
  ├─ 3連複オッズ取得
  ├─ 予測オッズ乖離シグナル計算（S/A/B/C/×）
  ├─ 確率再計算（人気統計ブレンド）
  └─ pred.json 上書き

[T日 発走15分前] 直前オッズ更新（scheduler: _job_odds_pre_race）
  ├─ 最終オッズ更新
  └─ 印フリーズ（確定）

[T日] レース実施

[T日 23:00] 結果取得・DB更新（scheduler: job_results_and_db）
  ├─ レース結果取得（netkeiba公式結果）
  ├─ race_results テーブルに保存
  ├─ 的中照合（compare_and_aggregate）
  ├─ race_log テーブルに全馬投入
  ├─ course_db 更新
  └─ match_results 集計

[定期] モデル再学習（retrain_all.py 手動実行）
```

---

## 3. エントリーポイント一覧

### メインスクリプト（ルート）

| ファイル | 用途 | 実行例 |
|---------|------|--------|
| `run_analysis_date.py` | 日付指定で全レース分析（メイン） | `python run_analysis_date.py 2026-04-02` |
| `run_analysis.py` | 単一レース分析（race_id指定） | `python run_analysis.py 202501050511` |
| `main.py` | マスターCLI（--analyze_date等） | `python main.py --analyze_date 2026-03-15` |
| `scheduler.py` | 自動スケジューラー（常駐） | `python scheduler.py` |
| `retrain_all.py` | 全MLモデル再学習 | `python retrain_all.py` |
| `build_horse_db.py` | 馬DB一括構築 | `python build_horse_db.py` |
| `run_results.py` | 結果取得・照合 | `python run_results.py 2026-04-01` |
| `run_daily_auto.py` | 日次自動運用 | `python run_daily_auto.py --predict` |
| `run_batch_past.py` | 過去日付一括バッチ | `python run_batch_past.py --start 2026-01-01 --end 2026-02-24` |
| `run_batch_regenerate.py` | 予想一括再生成 | `python run_batch_regenerate.py` |
| `run_export_daily.py` | 配布用1ファイル統合HTML | `python run_export_daily.py 2026-02-25` |
| `test_integration.py` | 結合テスト | `python test_integration.py` |

---

## 4. スケジューラーシステム

### scheduler.py の関数

| 関数 | 実行タイミング | 処理内容 |
|------|--------------|---------|
| `job_prediction()` | 毎日17:00 | 翌日全レースの予想生成 |
| `job_odds_morning()` | 毎日06:00 | 朝一括オッズ更新 |
| `_schedule_pre_race_odds()` | 予想生成後 | 各レースの発走15分前ジョブを登録 |
| `_job_odds_pre_race()` | 発走15分前 | 直前オッズ更新・印フリーズ |
| `job_results_and_db()` | 毎日23:00 | 結果取得・race_log投入・course_db更新 |
| `build_scheduler()` | 起動時 | APSchedulerのジョブ登録 |
| `show_status()` | — | 次回実行予定を表示 |
| `run_manual()` | — | 手動トリガー実行 |

### src/scheduler_tasks.py の関数

| 関数 | 用途 |
|------|------|
| `get_auth_client()` | netkeiba認証済みクライアント取得 |
| `get_official_odds_scraper()` | 公式オッズスクレイパー取得 |
| `get_race_ids()` | 指定日のレースID一覧取得 |
| `get_post_times()` | 各レースの発走時刻取得 |
| `run_odds_update()` | オッズ更新メイン処理 |
| `run_db_update()` | DB更新メイン処理 |
| `recalc_divergence()` | 予測オッズ乖離シグナル再計算 |
| `is_marks_frozen()` | 印フリーズ判定 |

---

## 5. データ収集（スクレイパー）

### src/scraper/ 全ファイル

| ファイル | 役割 | データソース |
|---------|------|-------------|
| `netkeiba.py` | メインスクレイパー（出馬表・過去走・血統） | netkeiba.com |
| `auth.py` | netkeiba認証・セッション管理 | netkeiba.com |
| `race_cache.py` | HTMLキャッシュ読み書き（lz4圧縮） | ローカル |
| `multi_source.py` | 複数データソースのフォールバック統合 | 複数 |
| `official_odds.py` | JRA公式オッズAPI | JRA公式 |
| `official_nar.py` | NAR公式サイトからのデータ取得 | NAR公式 |
| `rakuten_keiba.py` | 楽天競馬からのオッズ取得 | 楽天競馬 |
| `keibabook_training.py` | 競馬ブックから調教データ取得 | 競馬ブック |
| `training_collector.py` | 調教データ収集バッチ | 競馬ブック |
| `horse_db_builder.py` | 馬DB構築（過去走全データ） | netkeiba |
| `personnel.py` | 騎手/調教師DB管理（PersonnelDBManager） | netkeiba/race_log |
| `course_db_collector.py` | course_db事前構築・更新 | race_log |
| `improvement_dbs.py` | 補助DB構築（gate_bias, course_style等） | race_log |
| `race_results.py` | レース結果取得・各種DB構築 | netkeiba |
| `ml_data_collector.py` | ML学習用データ収集（data/ml/*.json） | race_log |
| `sire_stats.py` | 種牡馬成績収集 | netkeiba |
| `nar_id_mapper.py` | NAR馬ID⇔netkeiba ID変換 | netkeiba |

### キャッシュディレクトリ（data/cache/ — 17.9 GB）

| ディレクトリ | ファイル数 | 内容 |
|-------------|----------|------|
| ルート直下 | 190,213 | 馬ページHTML（lz4圧縮） |
| `races/` | 47,211 | レース結果HTML |
| `keibabook/` | 79,267 | 競馬ブック調教データ |
| `course_db_parsed/` | 5,921 | パース済みコースDB |
| `agg_daily/` | 2 | 日次集計キャッシュ |
| `personnel_stats/` | 2 | 騎手/調教師統計キャッシュ |

---

## 6. データベース

### SQLite: data/keiba.db（3,141 MB）

| テーブル | レコード数 | 用途 |
|---------|----------|------|
| `race_log` | 704,840 | 全馬全レースの走行記録（ML学習ソース） |
| `training_records` | 424,923 | 調教・厩舎コメント |
| `predictions` | 40,241 | 予想結果JSON（horses_json, tickets_json） |
| `race_results` | 38,581 | レース確定結果（order_json, payouts_json） |
| `personnel` | 977 | 騎手/調教師データ（data_json） |
| `course_db` | 246 | コース別過去データ（data_json） |
| `match_results` | 0 | 的中照合結果（未投入） |

### race_log カラム（49カラム）

```
id, race_date, race_id, venue_code, surface, distance,
horse_no, finish_pos, jockey_id, jockey_name, trainer_id, trainer_name,
field_count, is_jra, win_odds, running_style, sire_name, bms_name,
condition, horse_id, horse_name, gate_no, sex, age, weight_kg,
odds, tansho_odds, popularity, horse_weight, weight_change,
position_4c, positions_corners, finish_time_sec, last_3f_sec, first_3f_sec,
margin_ahead, margin_behind, status, course_id, grade, race_name,
weather, direction, race_first_3f, race_pace, pace,
is_generation, race_level_dev, source
```

### race_log データ品質（2026-04-01時点）

| カラム | 充填率 |
|--------|--------|
| 通過順(positions_corners) | 99.9% |
| 着差(margin_ahead, 2着以降) | 98.2% |
| 走破タイム(finish_time_sec) | 99.8% |
| 上がり3F(last_3f_sec) | 92.2% |
| 馬ID(horse_id) | 99.0% |

### src/database.py 主要関数

| 関数 | 用途 |
|------|------|
| `populate_race_log_from_predictions()` | predictions×race_resultsからrace_logに投入 |
| `collect_course_db_from_results()` | レース結果からcourse_dbを更新 |
| `HorseDB` クラス | SQLite DB操作ラッパー |

---

## 7. データマスター

| ファイル | 内容 |
|---------|------|
| `data/masters/venue_master.py` | 会場名⇔コード変換（VENUE_NAME_TO_CODE等） |
| `data/masters/course_master.py` | コース情報（コーナー数、直線長、高低差等） |
| `data/masters/course_master_generated.py` | ML生成コースマスター（自動生成） |
| `data/masters/venue_similarity.py` | 会場間類似度マトリクス |

### 会場コード体系

- **JRA**: 01(札幌), 02(函館), 03(福島), 04(新潟), 05(東京), 06(中山), 07(中京), 08(京都), 09(阪神), 10(小倉)
- **NAR**: 30,35,36,42,43,44,45,46,47,48,50,51,54,55,65(ばんえい)

---

## 8. 補助DB構築（分析前に事前準備）

| DB | 構築元 | 用途 | 保存先 |
|----|--------|------|--------|
| StandardTimeDB | race_log | コース別基準タイム | メモリ |
| Last3FDB | race_log | 上がり3F統計（平均/σ） | メモリ |
| CourseStyleDB | race_log | 脚質別勝率 | メモリ |
| GateBiasDB | race_log | 枠番別成績 | メモリ |
| TrainerBaselineDB | race_log | 調教師ベースライン | `data/trainer_baseline_db.json` |
| PersonnelDB | race_log + スクレイプ | 騎手/調教師最新成績 | `data/personnel_db.json` |
| BloodlineDB | race_log + スクレイプ | 種牡馬/母父の距離別成績 | `data/bloodline_db.json` |
| CourseDB | race_log + preload | 77,000+レース過去データ | `data/course_db_preload.json` |

---

## 9. 分析エンジン

### src/engine.py — RaceAnalysisEngine

#### analyze() メソッドの処理フロー

**Phase A: 能力値（ability）** — デフォルト重み 32%
- ファイル: `src/calculator/ability.py`
- 過去走の走破タイムを基準タイムと比較
- max_dev（最高偏差値）、wa_dev（WA偏差値）を算出
- トレンド判定（上昇/安定/下降）
- グレード補正、長期休養ペナルティ
- 出力: `HorseEvaluation.ability.total`（20-100スケール）

**Phase B: 展開予測（pace）** — デフォルト重み 30%
- ファイル: `src/calculator/pace_analysis.py`, `src/calculator/pace_course.py`
- 全馬の脚質分類（逃げ/先行/差し/追込）
- ペース予測（ハイ/ミドル/スロー）
- ML位置予測で4角通過位置を推定
- 枠順バイアス・コース脚質バイアス
- 騎手のペース傾向
- 上がり3FのML予測
- 出力: `HorseEvaluation.pace.total`

**Phase C: コース適性（course）** — デフォルト重み 6%
- ファイル: `src/calculator/pace_course.py` (CourseAptitudeCalculator)
- 同コース/同距離の過去成績
- 会場適性（芝/ダート、回り）
- 騎手×コース相性
- 出力: `HorseEvaluation.course.total`

**Phase D: 騎手評価（jockey）** — デフォルト重み 13%
- ファイル: `src/calculator/jockey_trainer.py`
- 勝率/連対率/複勝率、直近30日の調子
- コース別・馬場別の得意不得意
- 乗り替わり評価
- 出力: 騎手偏差値

**Phase E: 調教師評価（trainer）** — デフォルト重み 14%
- ファイル: `src/calculator/jockey_trainer.py`
- 勝率/連対率、厩舎の好調度
- 勝負気配スコア（shobu_score）
- 特選/危険フラグ
- 出力: 調教師偏差値

**Phase F: 血統評価（bloodline）** — デフォルト重み 5%
- ファイル: `src/scraper/improvement_dbs.py` (BloodlineDB)
- 父馬の距離別成績
- 母父の特性
- 芝/ダート適性
- 出力: 血統偏差値

**Phase G: ML確率予測**
- 5モデルアンサンブル（後述）
- 人気統計ブレンド（ML 95-98%、人気 2-5%）
- 出力: win_prob, place2_prob, place3_prob

**Phase H: コンポジット算出**
```
composite = ability×W_a + pace×W_p + course×W_c
          + jockey×W_j + trainer×W_t + bloodline×W_b
          + ML調整(±6pt)
```
- 会場別カスタム重み（VENUE_COMPOSITE_WEIGHTS）あり
- 出力: `HorseEvaluation.composite`（20-100スケール）

**Phase I: 印判定**
- `src/output/formatter.py::assign_marks()`
- ML合議チェック（composite1位 vs win_prob1位）
- 5段階基本印 + 特殊印（☆穴、×危険）

### 特殊スコア

| スコア | 意味 | 用途 |
|--------|------|------|
| `shobu_score` | 勝負気配 | 調教師の本気度 |
| `tokusen_score` | 特選 | 隠れ有力馬 |
| `tokusen_kiken_score` | 特選危険 | 凡走リスク大 |
| `ana_score` | 穴 | 低人気だがML高評価 |
| `kiken_score` | 危険 | 回避推奨 |

### src/utils/pace_inference.py
- ペース推論ユーティリティ

---

## 10. MLパイプライン

### モデル一覧（8種類）

| モデル | ファイル | タイプ | 入力 | 出力 | モデルファイル |
|--------|---------|--------|------|------|---------------|
| LightGBM 47分割 | `src/ml/lgbm_model.py` | LightGBM Binary | 199特徴量 | 複勝確率 | `data/models/lgbm_place*.txt` (42) |
| PyTorch Ranker | `src/ml/torch_model.py` | Neural Network | 199特徴量 | 着順確率 | `data/models/torch_nn.pt` |
| LightGBM Ranker | `src/ml/lgbm_ranker.py` | LambdaRank | 199特徴量 | ランキングスコア | `data/models/lgbm_ranker.txt` |
| 確率予測(3種) | `src/ml/probability_model.py` | LightGBM Binary×3 | 199特徴量 | win/top2/top3確率 | `data/models/prob_*.txt` |
| 位置予測 | `src/ml/position_model.py` | LightGBM Regression | 特徴量 | 4角通過位置 | `data/models/` 内 |
| 上がり3F予測 | `src/ml/last3f_model.py` | LightGBM Regression | 特徴量 | 上がり3Fタイム | `data/models/` 内 |
| ペースML | `src/ml/pace_model.py` | LightGBM Regression | 特徴量 | 前半3Fタイム | `data/models/` 内 |
| 初角位置予測 | `src/ml/first1c_model.py` | LightGBM Regression | 特徴量 | 初コーナー通過位置 | `data/models/` 内 |

### LightGBM 47分割モデルの構成

| レベル | モデル名 | 条件 |
|--------|---------|------|
| Lv1 global | `lgbm_place.txt` | 全レース共通 |
| Lv2 surface | `lgbm_place_turf.txt`, `lgbm_place_dirt.txt` | 芝/ダート別 |
| Lv2 org | `lgbm_place_jra_turf.txt`, `lgbm_place_jra_dirt.txt`, `lgbm_place_nar.txt` | JRA芝/JRA ダート/NAR |
| Lv3 SMILE | `lgbm_place_jra_turf_s.txt` 等 | JRA芝/ダート×距離区分(S/M/I/L) |
| Lv4 venue | `lgbm_place_venue_01.txt` 〜 `venue_65.txt` | 競馬場別(25会場) |

推論時フォールバック: venue → SMILE → JRA/NAR → surface → global

### 較正パラメータ

各モデルに `*_cal.json` ファイルが付属（Isotonic Regression較正用）

### モデルファイル（data/models/ — 382 MB, 95ファイル）

| 拡張子 | ファイル数 | 内容 |
|--------|----------|------|
| `.txt` | 42 | LightGBMモデル本体 |
| `.json` | 39 | 較正パラメータ + メタデータ |
| `.pkl` | 12 | rolling_stats, tracker, calibrator, sire_map等 |
| `.pt` | 1 | PyTorch Neural Rankerの重み |
| `.log` | 1 | 学習ログ |

### 主要pklファイル

| ファイル | 内容 |
|---------|------|
| `rolling_stats.pkl` | 騎手/調教師/馬のローリング統計 |
| `prob_tracker.pkl` | 確率モデル用ローリングトラッカー |
| `torch_tracker.pkl` | PyTorch用ローリングトラッカー |
| `torch_norm.pkl` | PyTorch入力正規化パラメータ |
| `prob_scalers.pkl` | 確率モデル用スケーラー |
| `prob_sire_tracker.pkl` | 種牡馬ローリング統計 |
| `sire_rolling_stats.pkl` | 種牡馬ローリング統計(別形式) |
| `sire_name_map.pkl` | 種牡馬名マッピング |
| `horse_sire_map.pkl` | 馬→種牡馬マッピング（31,291件） |
| `calibrator_win/top2/top3.pkl` | Isotonic Regression較正器 |

### ML学習データ（data/ml/ — 0.4 GB, 1,353ファイル）

- 日付別JSON: `20220101.json` 〜 `20260402.json`（1,640日分）
- 各JSONにレース情報 + 全馬の特徴量 + 実績結果を格納
- 収集元: `src/scraper/ml_data_collector.py`

### 特徴量エンジニアリング

- ファイル: `src/ml/features.py`
- 199特徴量（FEATURE_COLS定義）
- ラベル: `LABEL_COL`（複勝/勝利フラグ）

### 学習パイプライン

- ファイル: `src/ml/trainer.py`, `retrain_all.py`
- 時系列分割（train/validation）
- Optuna HPO: `scripts/optuna_hpo.py`, `scripts/optuna_banei.py`
- Walk-Forward CV: `scripts/walk_forward_cv.py`, `scripts/walk_forward_backtest.py`

### その他MLモジュール

| ファイル | 用途 |
|---------|------|
| `src/ml/__init__.py` | パッケージ初期化 |
| `src/ml/calibrator.py` | Isotonic Regression較正 |
| `src/ml/backtest.py` | 予測オッズ検証・期待値バックテスト |

---

## 11. 確率・オッズ計算

### src/calculator/popularity_blend.py — 人気統計ブレンド

- ALPHA_MODEL_MIN = 0.95（ML重み下限）
- ALPHA_MODEL_MAX = 0.98（ML重み上限）
- 人気統計の影響は2-5%のみ
- データ: `data/popularity_rates.json`

### src/calculator/predicted_odds.py — 予測オッズ

| 関数 | 用途 |
|------|------|
| `calc_predicted_tansho()` | 予測単勝オッズ |
| `calc_predicted_umaren()` | 予測馬連オッズ |
| `calc_predicted_sanrenpuku()` | 予測3連複オッズ（LGBMRanker使用） |

### 乖離シグナル

| シグナル | 意味 | 条件 |
|---------|------|------|
| S | 強い過小評価 | 実オッズ ≥ 2.0× 予測 |
| A | 良い妙味 | 実オッズ ≥ 1.5× 予測 |
| B | やや過小評価 | 実オッズ ≥ 1.2× 予測 |
| C | 適正 | 0.8 ≤ 比率 ≤ 1.2 |
| × | 過大評価 | 実オッズ < 0.8× 予測 |

### src/calculator/calibration.py — 重み較正
- venue_weights較正（ML特徴量重要度ベース）
- 出力: `data/models/venue_weights_calibrated.json`

---

## 12. 印判定・出力生成

### src/output/ 全ファイル

| ファイル | 役割 |
|---------|------|
| `formatter.py` | メインフォーマッター（HTMLFormatter） |
| `marks.py` | 印判定ロジック（assign_marks分離レイヤー） |
| `betting.py` | HTML内の馬券セクション生成 |
| `css.py` | HTML出力用CSSスタイル定義 |
| `grade_helpers.py` | グレード表示ヘルパー |
| `narrative.py` | ルールベース解説テンプレート |
| `llm_narrative.py` | Claude API（claude-haiku-4-5）による展開解説・穴馬見解の自動生成 |
| `past_runs.py` | 前走テーブルHTML生成 |
| `__init__.py` | パッケージ初期化 |

### 印判定ルール

| 印 | 条件 |
|----|------|
| ◉ | composite1位 + 自信度SS条件（gap≥tekipan_gap, win_prob≥閾値, place3_prob≥閾値） |
| ◎ | composite1位（◉条件未達時） |
| ○ | composite2位 |
| ▲ | composite3位 |
| △ | composite4位 |
| ★ | composite5位 or 特選フラグ |
| ☆ | 穴馬（低人気だがML高評価、ana_score高） |
| × | 危険馬（tokusen_kiken_score高） |

ML合議: composite1位 ≠ win_prob1位 の場合、composite差2pt以内 かつ win_prob比1.5倍以上ならwin_prob側を◎に昇格

### 自信度

| 等級 | 条件概要 |
|------|---------|
| SS | ◉かつ圧倒的gap |
| S | ◉ or composite gap大 |
| A | 標準的な◎ |
| B | composite差が小さい |
| C | 混戦 |
| D | 判定困難 |
| E | データ不足 |

### 出力ファイル

| ファイル | 内容 |
|---------|------|
| `data/predictions/YYYYMMDD_pred.json` | 全レース全馬の評価値JSON |
| `output/YYYYMMDD_会場NR.html` | 個別レース分析HTML |
| `output/YYYYMMDD_live_odds.json` | ライブオッズJSON |

### 馬券生成

- ファイル: `src/calculator/betting.py`
- 単勝/複勝/馬連/馬単/3連複/3連単フォーメーション
- 期待値（EV）ベースの推奨
- `data/rank_probability_table.json` — 順位別確率テーブル

### グレード体系

- ファイル: `src/calculator/grades.py`
- 偏差値 → グレード変換（SS/S/A/B/C/D/E）
- 全項目30-70統一レンジ（表示用）

---

## 13. 結果照合・KPI

### src/results_tracker.py

| 関数 | 用途 |
|------|------|
| `fetch_actual_results()` | netkeiba結果ページスクレイプ |
| `compare_and_aggregate()` | 予測と実績の照合・集計 |
| `_extract_past_runs()` | 前走データ抽出（通過順フォールバック3層） |
| `_get_corners_from_race_log()` | race_logから通過順取得 |
| `_parse_corners_from_race_results()` | race_resultsのorder_jsonから通過順パース |
| `_get_corners_for_run()` | HTMLキャッシュから通過順取得 |
| `_parse_corners_num()` | netkeiba数値形式（3333→[3,3,3,3]）パーサー |
| `_dp_corners()` | 通過順DPパーサー（1桁+2桁混在対応） |
| `_get_l3f_rank_for_run()` | 上がり3Fランク取得 |

### 通過順取得の3層フォールバック

1. **PastRunオブジェクト** → positions_corners属性
2. **race_log DB** → positions_cornersカラム（race_id, horse_id+date, venue+date+distance）
3. **race_results** → order_json内cornersフィールド → DPパーサー
4. **HTMLキャッシュ** → race_idでHTML読み込み → Corner_Numテーブルパース

### KPI評価

- スクリプト: `scripts/evaluate_kpi.py`
- 評価項目: 印別成績、自信度別成績、単勝ROI、モデルレベル別的中率

---

## 14. フロントエンド（React SPA）

### 技術スタック

- React 19 + Vite + TypeScript
- TailwindCSS + shadcn/ui
- TanStack Query（データフェッチ）
- Recharts（グラフ）
- React Router DOM（ルーティング）

### ページ構成

| ページ | ファイル | サブコンポーネント | 用途 |
|--------|---------|-------------------|------|
| ホーム | `HomePage.tsx` | — | 当日レース一覧、高自信度ピックアップ |
| 予想詳細 | `TodayPage.tsx` | HorseTable, HorseCardMobile, HorseDiagnosis, MarkSummary, OperationsPanel, PaceFormation, RaceDetailView, TicketSection | メインUI（予想閲覧・操作） |
| 成績分析 | `ResultsPage.tsx` | SummaryCards, TrendCharts, DetailedAnalysis, PastPredictions | 的中率・回収率トレンド |
| 競馬場研究 | `VenuePage.tsx` | VenueListView, VenueDetailView, VenueProfileTab, VenueBiasTab, VenueCourseTab, VenueRankingTab, VenueResultsTab | 会場別分析 |
| DB閲覧 | `DatabasePage.tsx` | CourseExplorer, PersonnelTable | 騎手/調教師/コースデータ |
| システム情報 | `AboutPage.tsx` | — | システム概要 + 特徴量重要度 |

### 共通コンポーネント

| ディレクトリ | コンポーネント |
|-------------|---------------|
| `components/keiba/` | AiCommentBlock, BreakdownTable, ConfidenceBadge, GradeBadge, MarkBadge, ProgressTracker, RaceCard, SurfaceBadge, VenueTabs |
| `components/layout/` | AppShell, Clock, TopNav |
| `components/ui/` | badge, button, card, dialog, input, select, separator, table, tabs |

### hooks

| ファイル | 用途 |
|---------|------|
| `hooks/useAuth.ts` | 認証状態管理 |
| `hooks/useClock.ts` | リアルタイム時計 |
| `hooks/useTheme.ts` | ダーク/ライトテーマ |
| `hooks/useViewMode.tsx` | 表示モード切替 |

### API層

| ファイル | 用途 |
|---------|------|
| `api/client.ts` | API呼び出し関数 + 型定義 |
| `api/hooks.ts` | TanStack Queryカスタムフック |

### ビルド

```bash
cd frontend && npm run build  # → frontend/dist/ に出力
```

---

## 15. レガシーフロントエンド

### src/frontend/（旧Flask テンプレート）

| ディレクトリ | 内容 |
|-------------|------|
| `templates/index.html` | レガシーHTMLテンプレート |
| `templates/about_content.html` | About内容 |
| `static/js/api.js` | API呼び出し |
| `static/js/app.js` | メインアプリ |
| `static/js/home.js` | ホームページ（未使用） |
| `static/js/results.js` | 成績ページ |
| `static/js/database.js` | DB閲覧 |
| `static/js/utils.js` | ユーティリティ |
| `static/css/` | design-tokens, components, animations, pages |
| `static/` | favicon, apple-touch-icon等 |

**注**: React SPAが主UI。レガシーJSは段階的に移行中だが残存。

---

## 16. Dashboard API 全エンドポイント（53個）

### src/dashboard.py

#### 静的ファイル配信
| Route | 用途 |
|-------|------|
| `/` | React SPA index.html |
| `/assets/<path>` | ビルド済みJS/CSS |
| `/favicon.*` | ファビコン |
| `/new`, `/new/<path>` | React SPA ルーティング |
| `/logos/<filename>` | ロゴ画像 |
| `/output/<filename>` | 生成HTML配信 |

#### 認証
| Route | 用途 |
|-------|------|
| `/api/auth_mode` | 認証モード確認 |

#### ホーム・予想
| Route | 用途 |
|-------|------|
| `/api/home_info` | ホーム情報（当日レース概要） |
| `/api/today_predictions` | 当日全レース予想データ |
| `/api/race_prediction` | 個別レース予想詳細 |
| `/api/race_odds` | レースオッズ情報 |
| `/api/share_url` | 共有URL生成 |
| `/api/portfolio` | ポートフォリオ（運用成績） |
| `/api/home/high_confidence` | 高自信度レースピックアップ |

#### 分析実行
| Route | 用途 |
|-------|------|
| `/api/analyze` | 分析開始（POST） |
| `/api/analyze_status` | 分析進捗確認 |
| `/api/analyze_cancel` | 分析中断 |
| `/api/state` | システム状態 |
| `/api/start` | — |
| `/api/status` | — |

#### オッズ更新
| Route | 用途 |
|-------|------|
| `/api/odds_update` | オッズ更新開始（POST） |
| `/api/odds_update_status` | 更新進捗 |
| `/api/odds_update_cancel` | 更新中断 |
| `/api/odds_schedule_status` | スケジュール状態 |
| `/api/odds/unfetched_dates` | 未取得日一覧 |
| `/api/predictions/unfetched_dates` | 未予想日一覧 |
| `/api/predicted_odds` | 予測オッズ |
| `/api/ev_map` | 期待値マップ |

#### スケジューラー状態
| Route | 用途 |
|-------|------|
| `/api/predict_schedule_status` | 予想スケジュール状態 |
| `/api/results_schedule_status` | 結果スケジュール状態 |

#### 結果・成績
| Route | 用途 |
|-------|------|
| `/api/results/dates` | 結果取得済み日付一覧 |
| `/api/results/summary` | 年別サマリー |
| `/api/results/detailed` | 詳細成績 |
| `/api/results/trend` | 月別トレンド |
| `/api/results/fetch` | 結果取得開始（POST） |
| `/api/results/fetch_batch` | 一括結果取得（POST） |
| `/api/results/fetch_status` | 取得進捗 |
| `/api/results/fetch_cancel` | 取得中断 |
| `/api/results/unmatched_dates` | 未照合日一覧 |
| `/api/results/unmatched_dates_db` | DB未更新日一覧 |
| `/api/generate_simple_html` | 簡易HTML生成（POST） |

#### DB管理
| Route | 用途 |
|-------|------|
| `/api/db/update` | DB更新開始（POST） |
| `/api/db/update_status` | 更新進捗 |
| `/api/db/update_cancel` | 更新中断 |
| `/api/db/personnel` | 騎手/調教師データ |
| `/api/db/personnel_agg` | 騎手/調教師集計 |
| `/api/db/course` | コースデータ一覧 |
| `/api/db/course_stats` | コース別統計 |

#### 競馬場研究
| Route | 用途 |
|-------|------|
| `/api/venue/profile` | 会場プロフィール |
| `/api/venue/bias` | 会場バイアス |

#### システム情報
| Route | 用途 |
|-------|------|
| `/api/feature_importance` | 特徴量重要度（About用） |

---

## 17. インフラ・運用

### src/ ユーティリティ

| ファイル | 役割 |
|---------|------|
| `src/log.py` | ロガー設定（sys.stdout=None対応） |
| `src/models.py` | データクラス定義（RaceInfo, HorseData, PastRun, HorseEvaluation等） |
| `src/collector_ui.py` | DB収集管理Web UI（Flask別アプリ） |
| `src/setup_credentials.py` | netkeiba/競馬ブック認証情報セットアップ |

### バッチファイル

| ファイル | 用途 |
|---------|------|
| `setup.bat` / `setup.sh` | 環境セットアップ |

### scripts/ スケジューラ関連

| ファイル | 用途 |
|---------|------|
| `scripts/setup_scheduler.ps1` | タスクスケジューラー登録（管理者権限） |
| `scripts/daily_predict.bat` + `*_hidden.vbs` | 日次予想バッチ（06:00） |
| `scripts/daily_predict_tomorrow.bat` + `*_hidden.vbs` | 翌日予想バッチ（17:00） |
| `scripts/daily_results.bat` + `*_hidden.vbs` | 日次結果バッチ（22:00） |
| `scripts/daily_maintenance.bat` + `*_hidden.vbs` | 日次メンテナンス（23:00） |
| `scripts/start_dashboard.bat` + `*_hidden.vbs` | ダッシュボード起動（ログオン時） |
| `scripts/watchdog_check.bat` + `*_hidden.vbs` | Watchdog（5分間隔） |

※ `*_hidden.vbs` はウィンドウ非表示で実行するためのラッパー

### Cloudflare Tunnel

- 設定: `~/.cloudflared/config.yml`
- 用途: 外出先からダッシュボードにアクセス
- 起動: `DAI_Keiba_Tunnel` タスク（ログオン時自動起動）
- ポート: 5051

### タスクスケジューラー

- `DAI_Keiba_Predict`: 毎朝06:00 予想生成
- `DAI_Keiba_Predict_Tomorrow`: 毎夕17:00 翌日予想生成
- `DAI_Keiba_Results`: 毎夜22:00 結果照合
- `DAI_Keiba_Maintenance`: 毎夜23:00 メンテナンス
- `DAI_Keiba_Dashboard`: ログオン時ダッシュボード起動（自動再起動付き）
- `DAI_Keiba_Tunnel`: Cloudflare Tunnel起動
- `DAI_Keiba_Watchdog`: 5分間隔でDashboard+cloudflared監視

### PC環境

- CPU: Ryzen 5 7600 (6C/12T)
- RAM: 32GB DDR5-4800
- GPU: RX 7600 8GB
- SSD: KIOXIA EXCERIA PLUS G3 1TB + Predator GM6 1TB

---

## 18. スクリプト一覧（scripts/）

### データバックフィル

| ファイル | 用途 |
|---------|------|
| `backfill_race_log.py` | race_log基本バックフィル |
| `backfill_race_log_full.py` | race_logフルバックフィル（data/ml/*.jsonから全カラム） |
| `backfill_race_log_corners_from_results.py` | race_resultsからcorners投入（1,460件） |
| `backfill_race_log_margins.py` | finish_time_secからmargin計算投入（88,266件） |
| `backfill_ml_from_cache.py` | HTMLキャッシュからML学習データ生成 |
| `backfill_pace.py` | ペースデータバックフィル |
| `backfill_payouts_from_html.py` | HTML結果から払戻データバックフィル |
| `backfill_missing_ids.py` | 不足IDバックフィル |
| `bulk_backfill_predictions.py` | predictions一括バックフィル |

### データ再構築

| ファイル | 用途 |
|---------|------|
| `rebuild_race_log_corners.py` | 通過順データ再構築 |
| `rebuild_race_log_l3f_corners.py` | 上がり3F+通過順再構築 |
| `rebuild_race_log_margins.py` | 着差データ再構築 |
| `rebuild_ml_corners.py` | MLデータの通過順再構築 |
| `rebuild_course_db_from_ml.py` | MLデータからcourse_db再構築 |
| `rebuild_personnel_from_race_log.py` | race_logから騎手/調教師DB再構築 |

### DB構築・メンテナンス

| ファイル | 用途 |
|---------|------|
| `build_dbs_from_cache.py` | キャッシュから各種DB構築 |
| `build_course_draft.py` | コースマスタードラフト生成 |
| `build_popularity_stats.py` | 人気統計テーブル構築 |
| `build_rank_probability_table.py` | 順位確率テーブル構築 |
| `build_slope_from_csv.py` | 坂路データCSVからDB構築 |
| `build_calibrator.py` | Isotonic Regression較正器構築 |
| `draft_to_course_master.py` | ドラフト→コースマスター適用 |
| `db_maintenance.py` | DBメンテナンス |
| `check_data_integrity.py` | データ整合性チェック |
| `cache_compress.py` | キャッシュlz4圧縮 |
| `cache_dedup.py` | キャッシュ重複排除 |
| `supplement_course_db_from_cache.py` | キャッシュからcourse_db補完 |

### 分析・評価

| ファイル | 用途 |
|---------|------|
| `evaluate_kpi.py` | KPI評価（印別成績、自信度別、ROI） |
| `evaluate_factor_grades.py` | 因子グレード評価 |
| `evaluate_pace_accuracy.py` | ペース予測精度評価 |
| `evaluate_position_accuracy.py` | 位置予測精度評価 |
| `analyze_all_facts.py` | 全因子分析 |
| `analyze_all_venues.py` | 全会場分析 |
| `analyze_ana_features.py` | 穴馬特徴分析 |
| `analyze_condition.py` | 馬場状態分析 |
| `analyze_condition_by_dist.py` | 距離別馬場分析 |
| `analyze_db_detail.py` | DB詳細分析 |
| `analyze_improvement_targets.py` | 改善対象分析 |
| `analyze_oi_venue.py` | OI(Odds Imbalance)会場分析 |
| `audit_race_log_quality.py` | race_logデータ品質監査 |
| `pipeline_diagnostic.py` | パイプライン診断 |

### バッチ処理

| ファイル | 用途 |
|---------|------|
| `batch_reanalyze.py` | 一括再分析 |
| `post_reanalysis.py` | 再分析後処理 |
| `postprocess_predictions.py` | 予想後処理 |
| `overnight_collect.py` | 夜間データ収集 |

### 最適化

| ファイル | 用途 |
|---------|------|
| `optimize_weights.py` | コンポジット重み最適化 |
| `optimize_wa_weights.py` | WA偏差値重み最適化 |
| `calibrate_venue_weights.py` | 会場別重み較正 |
| `optuna_hpo.py` | Optunaハイパーパラメータ最適化 |
| `optuna_banei.py` | ばんえい専用Optuna |
| `tune_venue_similarity_params.py` | 会場類似度パラメータ調整 |
| `simulate_improvements.py` | 改善シミュレーション |

### 検証

| ファイル | 用途 |
|---------|------|
| `validate_hypotheses.py` | 仮説検証 |
| `validate_pace_accuracy.py` | ペース精度検証 |
| `validate_sec_per_rank.py` | sec_per_rank検証 |
| `verify_bias_values.py` | バイアス値検証 |
| `walk_forward_backtest.py` | ウォークフォワードバックテスト |
| `walk_forward_cv.py` | ウォークフォワードCV |
| `test_corner_parser.py` | 通過順パーサーテスト |

### エクスポート

| ファイル | 用途 |
|---------|------|
| `export_results_csv.py` | 成績CSV出力 |
| `export_stats_csv.py` | 統計CSV出力 |
| `generate_portfolio.py` | ポートフォリオ生成 |

### 修正・更新

| ファイル | 用途 |
|---------|------|
| `fix_trainer_names.py` | 調教師名修正 |
| `update_running_style.py` | 脚質更新 |
| `update_win_odds.py` | 単勝オッズ更新 |
| `fetch_keibabook_nouryoku.py` | 競馬ブック能力値取得 |

### ランチャー（_プレフィックス）

| ファイル | 用途 |
|---------|------|
| `_auto_chain.py` | 自動チェイン実行 |
| `_batch_2025_2024.py` | 2024-2025バッチ |
| `_batch_all_years.py` | 全年バッチ |
| `_diag_and_run.py` | 診断+実行 |
| `_diag_batch.py` | 診断バッチ |
| `_fix_0318.py` | 0318修正 |
| `_launch_*.py` | 各種ランチャー |

---

## 19. ツール一覧（tools/）

| ファイル | 用途 |
|---------|------|
| `analyze_confidence_dist.py` | 自信度分布分析 |
| `backfill_sanrenpuku.py` | 3連複バックフィル |
| `batch_history.py` | バッチ履歴 |
| `bt5_compare.py` | BT5比較 |
| `cleanup_course_db.py` | course_dbクリーンアップ |
| `compare_pop_blend.py` | 人気ブレンド比較 |
| `dedup_course_db.py` | course_db重複排除 |
| `experiment_no_market.py` | 市場情報なし実験 |
| `regrade_course_db.py` | course_db再グレーディング |
| `reparse_payouts.py` | 払戻再パース |
| `run_race.py` | 単一レース実行 |
| `save_report.py` | レポート保存 |
| `scrape_ped_overnight.py` | 血統情報夜間収集 |
| `tune_banei.py` | ばんえいチューニング |
| `tune_blend.py` | ブレンドチューニング |
| `tune_params.py` | パラメータチューニング |
| `watch_collect.py` | 収集監視 |

---

## 20. テスト

| ファイル | 用途 |
|---------|------|
| `test_integration.py` | 結合テスト |
| `tests/test_mark_composite.py` | 印+コンポジットのユニットテスト |
| `tests/test_parsers.py` | パーサーのユニットテスト |

---

## 21. データファイル一覧

### data/ ルート

| ファイル | サイズ | 内容 |
|---------|--------|------|
| `keiba.db` | 3,141 MB | メインSQLiteデータベース |
| `keiba_v3.db` | 0 MB | 旧バージョンDB（空） |
| `course_db_preload.json` | — | 事前構築コースDB（77K+レース） |
| `personnel_db.json` | — | 騎手/調教師パフォーマンスDB |
| `bloodline_db.json` | — | 血統DB |
| `popularity_rates.json` | — | 人気別実績統計テーブル |
| `rank_probability_table.json` | — | 順位別確率テーブル |
| `trainer_baseline_db.json` | — | 調教師ベースラインDB |
| `nar_id_map.json` | — | NAR馬ID変換マップ |
| `name_map_cache.json` | — | 馬名⇔ID変換キャッシュ |
| `kb_venue_cache.json` | — | 競馬ブック会場キャッシュ |
| `confidence_analysis.json` | — | 自信度分析結果 |
| `course_db_collector_state.json` | — | course_db収集進捗状態 |

### data/bloodline/

| ファイル | 内容 |
|---------|------|
| `sire_name_to_id.json` | 種牡馬名→ID変換 |

### data/models/ — 382 MB, 95ファイル
（詳細は10章参照）

### data/ml/ — 0.4 GB, 1,353ファイル
- `YYYYMMDD.json` × 1,640日分（2022-01-01〜2026-04-02）

### data/predictions/ — 1,640ファイル
- `YYYYMMDD_pred.json` × 1,640日分

### data/cache/ — 17.9 GB
（詳細は5章参照）

### output/
- HTML: レース分析ページ
- JSON: live_odds（30日分）

---

## 22. 設定ファイル

| ファイル | 用途 |
|---------|------|
| `config/settings.py` | 全設定値（パス、重み、ML設定、閾値） |
| `pyproject.toml` | プロジェクトメタデータ、Ruff設定 |
| `.mcp.json` | MCP サーバー設定 |
| `.vscode/settings.json` | VSCode設定 |
| `.vscode/launch.json` | VSCodeデバッグ設定 |
| `.claude/settings.json` | Claude Code設定 |
| `.claude/settings.local.json` | Claude Codeローカル設定 |
| `.claude/launch.json` | Claude Code起動設定 |
| `frontend/package.json` | フロントエンド依存関係 |
| `frontend/tsconfig.json` | TypeScript設定 |
| `frontend/vite.config.ts` | Viteビルド設定 |
| `frontend/components.json` | shadcn/ui設定 |

### config/settings.py 主要設定

```python
# 6因子デフォルト重み
COMPOSITE_WEIGHTS = {
    "ability": 0.32, "pace": 0.30, "course": 0.06,
    "jockey": 0.13, "trainer": 0.14, "bloodline": 0.05,
}

# 会場別カスタム重み（ML特徴量重要度ベース）
VENUE_COMPOSITE_WEIGHTS = { "東京": {...}, "阪神": {...}, ... }

# ML設定
ALPHA_MODEL_MIN = 0.95  # 人気統計の影響を5%以下に
ALPHA_MODEL_MAX = 0.98

# 偏差値レンジ
DEVIATION = {"ability": {"min": 20, "max": 100}, ...}
```

---

## 23. ドキュメント

| ファイル | 内容 |
|---------|------|
| `README.md` | プロジェクト概要 |
| `QUICKSTART.md` | クイックスタートガイド |
| `CLAUDE.md` | Claude Code用プロジェクトガイド |
| `WINDOWS_GUIDE.md` | Windows環境ガイド |
| `COMPLETION_REPORT.md` | 完了レポート |
| `FILELIST.md` | ファイル一覧 |
| `docs/improvement_ideas_next.md` | 次期改善アイデア |
| `docs/improvement_opportunities.md` | 改善機会 |
| `docs/bloodline_pace_implementation.md` | 血統×ペース実装ドキュメント |
| `docs/component_implementation.md` | コンポーネント実装 |
| `docs/dashboard_audit_report.md` | ダッシュボード監査レポート |
| `docs/analysis_output_audit_report.md` | 分析出力監査レポート |
| `docs/preload_impact.md` | プリロード影響分析 |
| `docs/keibabook_nouryoku_verification.md` | 競馬ブック能力値検証 |
| `docs/claude_code_handoff_template.md` | Claude Code引き継ぎテンプレート |
| `docs/CLAUDE_CODE_IN_CURSOR.md` | Cursor環境でのClaude Code |
| `docs/コースマスタ修正提案.md` | コースマスター修正提案 |
| `docs/コース形状調査レポート.md` | コース形状調査 |
| `data/masters/COURSE_DRAFT_APPLY.md` | コースドラフト適用 |
| `data/masters/race_id_reference.md` | race_idリファレンス |

---

## 24. 現在のKPI（2026年 4,124レース）

### 全体サマリー

| 指標 | 値 |
|------|------|
| 本命勝率 | 40.0% |
| 本命連対率 | 62.3% |
| 本命複勝率 | 75.2% |
| 単勝回収率 | 97.2% |
| ☆穴馬 単勝ROI | 137.5% |
| ×危険馬 凡走率 | 85.2% |
| ◎と1番人気の一致率 | 45.1% |
| ◎の平均人気 | 2.6番人気 |

### 印別成績

| 印 | 勝率 | 連対率 | 複勝率 | 単勝ROI |
|----|------|--------|--------|---------|
| ◉ | 51.8% | 72.3% | 82.7% | 87.8% |
| ◎ | 34.7% | 57.8% | 71.8% | 101.4% |
| ○ | 20.8% | 45.4% | 62.7% | 85.8% |
| ▲ | 13.7% | 30.6% | 48.5% | 83.1% |
| △ | 8.9% | 20.4% | 35.5% | 72.0% |
| ★ | 5.9% | 14.0% | 26.0% | 63.1% |
| ☆ | 4.9% | 12.7% | 23.6% | 125.7% |
| × | 3.9% | 8.3% | 14.8% | 30.9% |

### 自信度別成績

| 自信度 | レース | 的中率 | 回収率 |
|--------|--------|--------|--------|
| SS | 381 | 52.8% | 144.1% |
| S | 2,039 | 44.9% | 98.7% |
| A | 894 | 38.8% | 82.4% |
| B | 492 | 23.4% | 71.7% |
| C | 222 | 23.4% | 122.9% |
| D | 82 | 18.3% | 86.1% |
| E | 14 | 35.7% | 94.3% |

---

## Python依存関係（主要）

- `lightgbm` — ML分割モデル
- `torch` — Neural Ranker
- `scikit-learn` — Isotonic Regression較正
- `pandas`, `numpy` — データ処理
- `flask` — Webダッシュボード
- `apscheduler` — タスクスケジューリング
- `beautifulsoup4`, `lxml` — HTMLパース
- `requests` — HTTP通信
- `lz4` — キャッシュ圧縮
- `rich` — CLI出力（進捗バー等）
- `anthropic` — Claude API（LLM解説生成）
- `optuna` — ハイパーパラメータ最適化

## フロントエンド依存関係

- `react`, `react-dom` — UI
- `react-router-dom` — ルーティング
- `@tanstack/react-query` — データフェッチ
- `recharts` — グラフ
- `tailwindcss` — スタイリング
- `shadcn` — UIコンポーネント
- `lucide-react` — アイコン
- `vite` — ビルドツール

---

*この文書は D-AI Keiba v3 の全コンポーネント・データ・ワークフローを網羅した完全仕様書です。*
*最終更新: 2026-04-01*
