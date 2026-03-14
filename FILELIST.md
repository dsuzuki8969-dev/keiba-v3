# 競馬解析マスターシステム v3.0 - ファイル構成

## 配布ファイル: keiba-v3-final.zip

---

## ディレクトリ構成

```
keiba-v3/
├── README.md                    # システム概要・使い方
├── QUICKSTART.md                # 5分でできるクイックスタート
├── requirements.txt             # Pythonパッケージ一覧
├── setup.bat                    # Windowsインストーラー
├── setup.sh                     # Mac/Linuxインストーラー
├── demo.py                      # デモ実行スクリプト
├── analyze.py                   # レース分析スクリプト
│
├── config/
│   └── settings.py              # 設定ファイル（賭け金、重みなど）
│
├── src/
│   ├── setup_credentials.py     # 認証情報設定
│   ├── engine.py                # メインエンジン
│   ├── models/                  # データモデル
│   │   ├── __init__.py
│   │   ├── horse.py             # 馬・レース情報
│   │   ├── evaluation.py        # 評価結果
│   │   └── enums.py             # 列挙型定義
│   ├── calculator/              # 偏差値計算ロジック
│   │   ├── __init__.py
│   │   ├── ability.py           # A-J章: 能力偏差値
│   │   ├── pace_course.py       # E-G章: 展開・コース偏差値
│   │   ├── jockey_trainer.py    # H-I章: 騎手・厩舎・穴馬・危険馬
│   │   ├── betting.py           # 5章: 買い目生成
│   │   └── calibration.py       # 4章: 自動調整・診断
│   ├── scraper/                 # データ取得
│   │   ├── __init__.py
│   │   ├── netkeiba.py          # netkeibaスクレイパー
│   │   ├── keibaguide.py        # 競馬ブックスクレイパー
│   │   └── adapters.py          # アダプター
│   └── output/
│       ├── __init__.py
│       └── formatter.py         # HTML生成
│
└── data/
    ├── masters/
    │   ├── venue_master.py      # 競馬場マスタ
    │   └── race_id_reference.md # race_id形式（netkeiba/競馬ブック）リファレンス
    ├── course_master.csv        # コースマスター（189コース）
    ├── standard_times.csv       # 基準タイムDB（自動生成）
    ├── jockey_stats.csv         # 騎手統計DB（自動生成）
    ├── trainer_stats.csv        # 厩舎統計DB（自動生成）
    ├── trainer_baseline.csv     # 厩舎ベースライン（自動生成）
    ├── pace_last3f.csv          # ペース末脚DB（自動生成）
    └── course_style_stats.csv   # コース脚質統計（自動生成）
```

---

## ファイルサイズ

- **ZIP圧縮後**: 約105KB
- **展開後**: 約376KB

---

## 主要ファイルの説明

### ユーザー向けファイル
- `README.md`: システム全体の説明書
- `QUICKSTART.md`: 5分で動かせる手順
- `demo.py`: デモ実行（サンプルレースの分析）
- `analyze.py`: 実際のレース分析
- `config/settings.py`: 賭け金・重みの設定

### システムコア
- `src/engine.py`: 分析エンジン本体
- `src/calculator/*.py`: 偏差値計算ロジック（設計書A-J章を実装）
- `src/output/formatter.py`: HTML生成（プロ級の出力）

### データベース
- `data/course_master.csv`: 189コースの詳細情報（手動作成済み）
- `data/*.csv`: 自動生成される統計DB（初回実行時に作成）

---

## 生成されるファイル

### 初回実行時
- `~/.keiba_credentials.json`: 認証情報（netkeiba + 競馬ブック）

### レース分析時
- `output/keiba_demo.html`: デモ出力
- `output/[レースID]_[競馬場][R番].html`: レース分析結果（例: 202506021011_東京11R.html）

### 統計DB（自動生成）
- `data/standard_times.csv`: 過去レースから基準タイムを学習
- `data/jockey_stats.csv`: 騎手の実績統計
- `data/trainer_stats.csv`: 厩舎の実績統計
- など

---

## ライセンス

- **利用形態**: 個人利用のみ
- **再配布**: 禁止
- **商用利用**: 禁止

---

## 更新履歴

### v3.0 (2026-02-18)
- 初回リリース
- 10章構成の偏差値計算システム
- 指数ベース予測（オッズを無視）
- プロ級HTML出力
- 穴馬・危険馬の詳細根拠
- 賢い買い目生成（回収率300%基準）

---

**圧縮率**: 約72%（376KB → 105KB）
