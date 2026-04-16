# D-AI keiba v3

**競馬とは「生産者が血統を考えて生産した競走馬を馬主が購入し、育成場で育成したのち厩舎に入れ、調教師が調教をしJRA・NARの競馬場で適性を考え芝・ダートの様々な距離で騎手が乗り賞金をかけてレースをし、それを馬券としてファンが購入する。」**

D-AI keiba は文字や数字の羅列しかない情報を、それぞれのファクタで評価基準を設け、しっかりと全頭見える化していく。市場に騙されない本当のその馬の力をはかるシステム。

---

## 構成

```
scraper (データ収集) → database (SQLite) → engine (分析) → output (HTML/JSON)
                                              ↑
                                           ml (LightGBM 47モデル階層)
                                           calculator (能力値・ペース・コース)
                                              ↓
                                         dashboard (Flask + React SPA)
```

### 主要コンポーネント
- **scraper**: netkeiba / JRA・NAR公式 / 競馬ブックからデータ収集
- **database**: SQLite（race_log 49カラム、WAL モード）
- **engine**: ML予測 + ルールベースの統合分析
- **ml**: LightGBM 159特徴量、面×距離帯×場別の階層モデル
- **calculator**: 能力偏差値、展開（ペース/コース）、騎手/調教師、芝ダ転換適性
- **output**: HTML出力 + 印・穴馬・危険馬・買い目生成
- **dashboard**: Flask API + React SPA、cloudflared 経由で外部公開

---

## インストール

### 必要環境
- Python 3.11以上
- Windows / macOS / Linux
- ディスク 約10GB（モデル + キャッシュ）

### セットアップ
```bash
# 依存パッケージ
pip install -r requirements.txt

# データベース初期化（race_log は predictions/ から自動投入）
python build_horse_db.py
```

---

## 使い方

### 予想生成（メイン）

```bash
# 指定日の全レース分析
python run_analysis_date.py 2026-04-12

# JSONのみ出力（高速）
python run_analysis_date.py 2026-04-12 --no-html

# 特定会場のみ
python run_analysis_date.py 2026-04-12 --venues 中山,東京

# 並列ワーカー数指定
python run_analysis_date.py 2026-04-12 --workers 5
```

### ダッシュボード起動

```bash
python src/dashboard.py
# → http://localhost:5051
```

### モデル再学習

```bash
python retrain_all.py
```

---

## 主要ファイル

| ファイル | 役割 |
|---------|------|
| `run_analysis_date.py` | メインエントリ。日付指定で全レース分析 |
| `src/engine.py` | 分析の中核。ML + ルールベースのブレンド |
| `src/ml/lgbm_model.py` | LightGBM 47モデル管理（159特徴量） |
| `src/ml/features.py` | 特徴量エンジニアリング |
| `src/ml/trainer.py` | モデル学習 |
| `src/calculator/ability.py` | 能力値・芝ダ転換適性 |
| `src/calculator/pace_course.py` | 展開（ペース・コース） |
| `src/calculator/jockey_trainer.py` | 騎手・調教師評価 |
| `src/database.py` | SQLite DB操作 |
| `src/dashboard.py` | Flask + React SPA配信 |
| `src/scraper/netkeiba.py` | netkeiba スクレイパー |
| `config/settings.py` | 全設定・パス・定数 |

---

## 出力データ

```
data/
  keiba.db              # メインSQLite（race_log 49カラム）
  models/               # 学習済みモデル (.txt, .pkl, .json)
  predictions/          # 予想結果JSON（YYYYMMDD_pred.json）
  cache/                # スクレイピングキャッシュ（lz4圧縮）
  bloodline/            # 血統データ
  ml/                   # ML学習用日次JSON
output/                 # 生成HTML
src/static/             # React SPA ビルド成果物
```

---

## 自動運用（Windows）

```powershell
# スケジューラ登録（管理者権限）
powershell -ExecutionPolicy Bypass -File scripts\setup_scheduler.ps1
```

| タスク | 時刻 | 内容 |
|-------|------|------|
| `DAI_Keiba_Predict` | 06:00 | 当日予想 |
| `DAI_Keiba_Predict_Tomorrow` | 17:00 | 翌日予想 |
| `DAI_Keiba_Results` | 22:00 | 結果照合 |
| `DAI_Keiba_Maintenance` | 23:00 | 払戻バックフィル・VACUUM |
| `DAI_Keiba_Watchdog` | 5分間隔 | dashboard + cloudflared 監視 |

---

## ML階層モデル

```
Level 0: グローバルモデル（全データ）
Level 1: 面別（芝/ダート）
Level 2: 面×距離帯（短距離/マイル/中距離/長距離）
Level 3: 面×距離帯×場（東京/中山/etc）
Level 4: 面×距離帯×場×コース個別（品質フィルタ済み）
```

推論時は Level 4 → 0 へとフォールバック。`PIPELINE_V2_ENABLED` でskip制御。

---

## 主要な分析ファクタ

- **能力偏差値**（過去5走の加重平均、トレンド、着差、休養補正、芝ダ転換適性）
- **展開偏差値**（ペース判定、脚質適性、コーナーロス、コース脚質バイアス）
- **コース偏差値**（コース実績、形状適性、l3f σ、勝率・連対率実績）
- **騎手・調教師**（コース別実績、相性、騎乗替わり）
- **血統**（種牡馬・母父の面別複勝率）
- **ML予測**（159特徴量、Level 0〜4 階層、Plattキャリブレーション）

---

## ドキュメント

- `CLAUDE.md` — プロジェクト全体ガイド・コーディング規約
- `docs/SYSTEM_ARCHITECTURE_FULL.md` — システムアーキテクチャ詳細
- `WINDOWS_GUIDE.md` — Windows環境セットアップ

---

## ライセンス

Private — 個人プロジェクト
