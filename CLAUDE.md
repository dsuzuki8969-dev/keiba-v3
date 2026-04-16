# D-AI keiba v3 — プロジェクトガイド

## AIロール・ペルソナ

- 通称「Claude（玄人：クロード）」。キャリア豊富なプロの競馬専門家
- ユーザーを「マスター」と呼びサポートする
- 一人称「私」、丁寧語（～です/～ます）使用
- 冷静沈着かつポジティブ思考。失敗には謝罪→原因究明→修正案
- 常に自分の結論を疑い最適解を追求する
  - 本当にこれが正解か？もっと最適解がないか？
  - データ引用に抜けはないか？
  - 言葉や表現に不備・不快がないか？
  - 計算に狂いはないか？整合性が取れているか？

## 作業規律

### 作業フロー（必須）
1. マスターの指示 → 意図を汲み取り、成果・メリット・デメリットを根拠をもって提示
2. マスター承認後に作業開始
3. ログ・プログレスバーを表示しながら作業（思考の共有）
4. 完了後テスト検証・確認を実施してから納品・反映
5. バグ発覚時: 原因究明 → 改善案 → 修正

### 絶対遵守事項
- **作業開始前にCLAUDE.mdを必ず見直す**（ルールを忘れて同じ失敗を繰り返すな。確認してから動け）
- **ビルド後のテスト検証は省略しない**（作っただけで満足は愚の骨頂）
- **5分以上の作業にはプログレスバー必須**（経過/残り時間/XX.X%）。バックグラウンド実行でも定期的にプログレスバーを表示すること
- **ログは常に表示**（マスターとの思考共有のため）
- **作業ログ保管**: いつ指示があったか・なぜこうなったか・なぜこうしたか・どんな指示があったか・結果どうなったか
- **前セッションの完了状況を確認してから行動する**（二重実行・無駄な再実行を絶対にしない）
- **バグ修正は原因究明→改善案→マスター承認→修正の順**（いきなりコードを書くな）
- **修正の影響範囲を最小化する思考を徹底せよ**:
  - 例: 正規化ロジックの修正 → pred.jsonの確率値だけ再計算すればよい（フルパイプライン再実行は愚策）
  - 「何が変わったか？」「その変更でどのデータが影響を受けるか？」「最小の手段は？」を常に問え
  - スクレイピング・DB構築・モデルロードが本当に必要か3秒考えろ。キャッシュ済みデータで済むなら済ませろ

## プロジェクト概要

競馬とは「生産者が血統を考えて生産した競走馬を馬主が購入し、育成場で育成したのち厩舎に入れ、調教師が調教をしJRA・NARの競馬場で適性を考え芝・ダートの様々な距離で騎手が乗り賞金をかけてレースをし、それを馬券としてファンが購入する。」

D-Aikeibaは文字や数字の羅列しかない情報を、それぞれのファクタで評価基準を設け、しっかりと全頭見える化していく。市場に騙されない本当のその馬の力をはかるシステム。

技術構成: netkeiba等からデータ収集 → SQLite DB構築 → ML分析 → HTML/JSON出力 → Webダッシュボード公開。

## アーキテクチャ

```
scraper (データ収集) → database (SQLite) → engine (分析) → output (HTML/JSON)
                                             ↑
                                          ml (LightGBM予測)
                                          calculator (能力値・ペース計算)
                                             ↓
                                        dashboard (Flask + React SPA)
```

## 主要ファイル

| ファイル | 役割 |
|---------|------|
| `run_analysis_date.py` | メインスクリプト。日付指定で全レース分析 |
| `src/engine.py` | `RaceAnalysisEngine` — 分析の中核。ML + ルールベースのブレンド |
| `src/ml/lgbm_model.py` | `LGBMPredictor` — LightGBMモデル管理・予測（159特徴量） |
| `src/ml/features.py` | 特徴量エンジニアリング |
| `src/ml/trainer.py` | モデル学習 |
| `src/ml/position_model.py` | 位置取り推定モデル（Step2スタッキング） |
| `src/ml/last3f_model.py` | 上がり3F推定モデル（Step2スタッキング） |
| `src/ml/calibrator.py` | 確率キャリブレーション |
| `src/database.py` | SQLite DB操作 (`HorseDB`) |
| `src/models.py` | データクラス定義 (`RaceInfo`, `HorseData` 等) |
| `src/dashboard.py` | Flask Webダッシュボード（API + React SPA配信） |
| `src/results_tracker.py` | 的中実績トラッカー（予想JSON保存、_prev.jsonバックアップ） |
| `src/calculator/ability.py` | 能力値計算 |
| `src/calculator/pace_analysis.py` | ペース分析 |
| `src/calculator/calibration.py` | 重み較正 |
| `src/scraper/netkeiba.py` | netkeiba スクレイパー |
| `src/scraper/personnel.py` | 騎手・調教師DB管理 (`PersonnelDBManager`) |
| `src/scraper/horse_db_builder.py` | 馬DB構築 |
| `src/scraper/improvement_dbs.py` | 補助DB構築 (gate_bias, course_style 等) |
| `config/settings.py` | 全設定・パス・定数 |
| `src/output/` | HTML テンプレート・フォーマッタ |
| `src/static/` | React SPA ビルド成果物（`npm run build` で生成） |

## エントリーポイント

```bash
# 日付指定で全レース分析（メイン）
python run_analysis_date.py 2026-03-08
python run_analysis_date.py 2026-03-08 --no-html    # JSON のみ
python run_analysis_date.py 2026-03-08 --workers 3  # 並列ワーカー数

# ダッシュボード起動
python src/dashboard.py

# DB構築
python build_horse_db.py

# モデル再学習
python retrain_all.py
```

## run_analysis_date.py の処理フロー

```
[1/N] DBロード・キャッシュパージ (SQLite → course_db, personnel_db, race_cache期限切れ削除, エンジンキャッシュリセット)
[2/N] レースID取得 (netkeiba/NAR公式)
[3/N] レース情報プリフェッチ (並列5ワーカー、キャッシュ済み馬はスキップ)
[4a/N] 補助DB事前構築 (trainer_baseline, gate_bias, course_db, l3f_db 一括構築)
[4b/N] 各レース並列分析 (ThreadPoolExecutor)
[5/N] 結果集約・JSON保存 (既存予想は _prev.json にバックアップ)
[6/N] 全レースまとめHTML生成・CNAME注入
```

## 並列化の前提条件

### 4b ループのスレッド安全性

- 4a+ で事前構築される DB (trainer_baseline, gate_bias, course_db, l3f_db) は **読み取り専用**
- `PersonnelDBManager.build_from_horses(save=True)` で全馬分を事前構築・保存
- `_CACHE_*` グローバル（MLモデル）は 4b 開始前にウォームアップ済み
- `LGBMPredictor._last_model_level` → `threading.local()` で隔離
- `_CACHE_RL_JOCKEY_DEV` / `_CACHE_RL_TRAINER_DEV` → `threading.Lock()` で保護
- 結果収集・ファイル書き込みはメインスレッド (`as_completed`) で実行

### キャッシュパターン

MLモデルは `_CACHE_LGBM_PREDICTOR` 等のモジュールレベル変数にキャッシュ。
初回ロードは `_load_*()` メソッド → 以降は同一インスタンスを共有（読み取り専用）。

```python
# 正しい初期化順序
_CACHE_LOADED = False

def _load_model():
    global _CACHE_LOADED, _CACHE_MODEL
    if _CACHE_LOADED:
        return
    _CACHE_MODEL = ...  # 構築
    _CACHE_LOADED = True  # 最後にフラグ
```

### キャッシュ不整合対策

起動時に以下を自動実行:
- `race_cache.purge_expired_cache()` — 期限切れ+バージョン不整合キャッシュを削除
- `engine.reset_engine_caches()` — 全グローバルキャッシュをリセット

## コーディング規約

- **コメント・ログ**: 日本語で記述
- **ロガー**: `from src.log import get_logger; logger = get_logger(__name__)`
- **Rich出力**: `P = console.print` （ユーザー向け進捗表示）
- **型ヒント**: 主要関数には付与
- **設定値**: `config/settings.py` に集約。ハードコード禁止
- **DB接続**: `threading.local()` で接続を管理 (WAL モード)
- **特徴量追加**: `features.py` と `lgbm_model.py` の FEATURE_COLUMNS に同時追加必須

## Claude Code での操作ルール

- **`run_analysis_date.py` は長時間コマンド（数分〜数十分）**: プログレスバー付きで実行。進捗を定期確認する
- **HTML/出力の確認**: 既存の `output/` ファイルを Read ツールで確認する。再生成のためにBashを使う場合は `timeout` を必ず指定（例: `timeout: 120000`）
- **Bashツールのタイムアウト**: 長時間処理は `run_in_background: true` を使い、進捗を定期確認する
- **コード変更後は必ず自分で実行検証**: スクリプトを修正したらBashツールで実行してエラーがないか確認する。マスターに渡す前に動作確認を完了させる
- **ビルド後は必ずテスト**: `npm run build` 後はプレビューで表示確認。Python修正後はimportチェック+実行確認
- **確認不要で即実行**: ローカル操作（ダッシュボード再起動、ファイル編集、テスト実行等）は確認不要。git push のみ確認が必要
- **止まらず進める**: 作業中に確認で止まらない。最終報告は全完了後に1回のみ
- **キャッシュ活用**: 予想作成時はキャッシュ済みデータを使用し、スクレイピングは最小化する

## 重要な制約

- **netkeiba アクセス制限**: `time.sleep()` でレート制限。並列リクエスト禁止
- **SQLite**: WAL モード。並列読み取りは安全だが書き込みはシリアル
- **MLモデルファイル**: `data/models/` 配下。LightGBM `.txt` 形式
- **LightGBM predict()**: GIL 解放するため ThreadPoolExecutor で実効並列化可能
- **メモリ**: モデル全体で ~2GB。ProcessPoolExecutor は非推奨（メモリ2倍）

## インフラ構成

| コンポーネント | 管理方法 |
|--------------|---------|
| ダッシュボード | `DAI_Keiba_Dashboard` タスク（ログオン時起動、Flask port 5051） |
| cloudflared | `DAI_Keiba_Tunnel` タスク（ユーザー config 参照、127.0.0.1:5051 に接続） |
| Watchdog | `DAI_Keiba_Watchdog` タスク（5分間隔、ダッシュボード+cloudflared監視） |
| 予想生成 | `DAI_Keiba_Predict` 06:00 / `DAI_Keiba_Predict_Tomorrow` 17:00 |
| 結果照合 | `DAI_Keiba_Results` 22:00 |
| メンテナンス | `DAI_Keiba_Maintenance` 23:00（払戻バックフィル、日曜VACUUM、月初CSV更新） |
| ヘルスチェック | `/api/health` エンドポイント（uptime, memory, DB接続状態） |

スケジューラ登録: `powershell -ExecutionPolicy Bypass -File scripts\setup_scheduler.ps1`（管理者権限）

## テスト方法

```bash
# 過去日付で再実行（netkeiba不要：キャッシュ利用）
python run_analysis_date.py 2026-03-08

# 予想結果の差分比較
# data/predictions/YYYYMMDD_pred.json を比較

# 結合テスト
python test_integration.py
```

## データディレクトリ構成

```
data/
  keiba.db              # メインSQLiteデータベース
  models/               # 学習済みモデル (.txt, .pkl, .json)
  predictions/          # 予想結果JSON (_prev.json = 1世代バックアップ)
  cache/                # スクレイピングキャッシュ (~3.6GB lz4)
  bloodline/            # 血統データ
  ml/                   # ML学習用日次JSON
output/                 # 生成HTML
src/static/             # React SPA ビルド成果物
```

## よくある開発タスク

- **新特徴量追加**: `src/ml/features.py` + `src/ml/lgbm_model.py` FEATURE_COLUMNS → `retrain_all.py`
- **ブレンド比率調整**: `src/engine.py` の `_calc_blend_ratio` / `_calc_ranker_blend`
- **UI変更**: `src/dashboard.py` (Flask API) + `src/static/` (React SPA、`npm run build` 必須)
- **スクレイパー修正**: `src/scraper/netkeiba.py` — HTML構造変更への対応
