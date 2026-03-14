# D-AI keiba v3 — プロジェクトガイド

## 概要

競馬（JRA中央・NAR地方）の予想・分析システム。
netkeiba等からデータ収集 → SQLite DB構築 → ML分析 → HTML/JSON出力 → Webダッシュボード公開。

## アーキテクチャ

```
scraper (データ収集) → database (SQLite) → engine (分析) → output (HTML/JSON)
                                             ↑
                                          ml (LightGBM/PyTorch予測)
                                          calculator (能力値・ペース計算)
                                             ↓
                                        dashboard (Streamlit UI)
```

## 主要ファイル

| ファイル | 役割 |
|---------|------|
| `run_analysis_date.py` | メインスクリプト。日付指定で全レース分析 |
| `src/engine.py` | `RaceAnalysisEngine` — 分析の中核。ML + ルールベースのブレンド |
| `src/ml/lgbm_model.py` | `LGBMPredictor` — LightGBMモデル管理・予測 |
| `src/ml/torch_model.py` | PyTorch (Neural Ranker) モデル |
| `src/ml/lgbm_ranker.py` | LightGBM Ranker モデル |
| `src/ml/features.py` | 特徴量エンジニアリング |
| `src/ml/trainer.py` | モデル学習 |
| `src/database.py` | SQLite DB操作 (`HorseDB`) |
| `src/models.py` | データクラス定義 (`RaceInfo`, `HorseData` 等) |
| `src/dashboard.py` | Streamlit Webダッシュボード |
| `src/results_tracker.py` | 的中実績トラッカー |
| `src/calculator/ability.py` | 能力値計算 |
| `src/calculator/pace_analysis.py` | ペース分析 |
| `src/calculator/calibration.py` | 重み較正 |
| `src/scraper/netkeiba.py` | netkeiba スクレイパー |
| `src/scraper/personnel.py` | 騎手・調教師DB管理 (`PersonnelDBManager`) |
| `src/scraper/horse_db_builder.py` | 馬DB構築 |
| `src/scraper/improvement_dbs.py` | 補助DB構築 (gate_bias, course_style 等) |
| `config/settings.py` | 全設定・パス・定数 |
| `src/output/` | HTML テンプレート・フォーマッタ |

## エントリーポイント

```bash
# 日付指定で全レース分析（メイン）
python run_analysis_date.py 2026-03-08
python run_analysis_date.py 2026-03-08 --no-html    # JSON のみ
python run_analysis_date.py 2026-03-08 --workers 3  # 並列ワーカー数

# ダッシュボード起動
streamlit run src/dashboard.py

# DB構築
python build_horse_db.py

# モデル再学習
python retrain_all.py
```

## run_analysis_date.py の処理フロー

```
[1/N] DBロード (SQLite → course_db, personnel_db 等)
[2/N] レースID取得 (netkeiba/NAR公式)
[3/N] レース情報プリフェッチ (並列)
[4a/N] 補助DB事前構築 (trainer_baseline, gate_bias 等)
[4b/N] 各レース並列分析 (ThreadPoolExecutor)
[5/N] 結果集約・JSON保存・全レースHTML生成
```

## 並列化の前提条件

### 4b ループのスレッド安全性

- 4a+ で事前構築される DB (trainer_baseline, gate_bias 等) は **読み取り専用**
- `PersonnelDBManager.build_from_horses(save=False)` で DB保存をスキップ
- `_CACHE_*` グローバル（MLモデル）は 4b 開始前にウォームアップ済み
- `LGBMPredictor._last_model_level` → `threading.local()` で隔離
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

## コーディング規約

- **コメント・ログ**: 日本語で記述
- **ロガー**: `from src.log import get_logger; logger = get_logger(__name__)`
- **Rich出力**: `P = console.print` （ユーザー向け進捗表示）
- **型ヒント**: 主要関数には付与
- **設定値**: `config/settings.py` に集約。ハードコード禁止
- **DB接続**: `threading.local()` で接続を管理 (WAL モード)

## 重要な制約

- **netkeiba アクセス制限**: `time.sleep()` でレート制限。並列リクエスト禁止
- **SQLite**: WAL モード。並列読み取りは安全だが書き込みはシリアル
- **MLモデルファイル**: `data/models/` 配下。LightGBM `.bin` + PyTorch `.pt`
- **LightGBM/PyTorch predict()**: GIL 解放するため ThreadPoolExecutor で実効並列化可能
- **メモリ**: モデル全体で ~2GB。ProcessPoolExecutor は非推奨（メモリ2倍）

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
  models/               # 学習済みモデル
  predictions/          # 予想結果JSON
  cache/                # スクレイピングキャッシュ
  bloodline/            # 血統データ
output/                 # 生成HTML
```

## よくある開発タスク

- **新特徴量追加**: `src/ml/features.py` → `src/ml/trainer.py` → `retrain_all.py`
- **ブレンド比率調整**: `src/engine.py` の `_calc_blend_ratio` / `_calc_ranker_blend`
- **UI変更**: `src/dashboard.py` (Streamlit) or `src/output/` (HTML テンプレート)
- **スクレイパー修正**: `src/scraper/netkeiba.py` — HTML構造変更への対応
