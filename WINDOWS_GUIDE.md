# Windows11での使い方（超簡単版）

## 📌 必要なもの
- Windows11
- インターネット接続
- それだけ！

---

## 🚀 ステップ1: 準備（初回のみ）

### 1-1: ファイルを解凍
`keiba-v3-final.zip` を解凍して `keiba-v3` フォルダを作る

### 1-2: setup.batを実行
`setup.bat` をダブルクリック
→ 自動的にPythonパッケージがインストールされます

---

## 📊 ステップ2: データベース構築（初回のみ）

### 2-1: build_db.batを実行
`build_db.bat` をダブルクリック

**何が起こるか:**
- netkeibaから過去のG1レース結果を取得
- 基準タイムDBを自動作成
- 数分かかります

**結果:**
- `data/standard_times.csv` が作成されます

---

## 🏇 ステップ3: デモ実行

### 3-1: demo.batを実行（まだ無い場合は後で作ります）
または、コマンドプロンプトで:
```
python demo.py
```

**結果:**
- `keiba_demo.html` が作成されます
- ブラウザで開いて確認！

---

## 🎯 ステップ4: 実際のレース分析

### 4-1: netkeibaでレースIDを確認
例: https://race.netkeiba.com/race/result.html?race_id=202506021011
                                                        ^^^^^^^^^^^^
                                                        これがレースID

### 4-2: analyze.batを実行（まだ無い場合は後で作ります）
または、コマンドプロンプトで:
```
python analyze.py
```

レースIDを入力:
```
レースIDを入力してください: 202506021011
```

**結果:**
- `keiba_202506021011.html` が作成されます

---

## ❓ トラブルシューティング

### エラー: 'python' は、内部コマンドまたは外部コマンド...
**解決策:**
1. Pythonがインストールされていません
2. Microsoft StoreでPythonをインストール
3. もう一度 `setup.bat` を実行

### エラー: ModuleNotFoundError
**解決策:**
```
python -m pip install -r requirements.txt
```

### データベース構築が失敗する
**解決策:**
1. インターネット接続を確認
2. netkeibaにアクセスできるか確認
3. しばらく待ってから再実行

---

## 📁 作成されるファイル

```
keiba-v3/
├── build_db.bat          ← これをダブルクリック（DB構築）
├── demo.py               ← デモ実行
├── analyze.py            ← レース分析
└── data/
    └── standard_times.csv  ← 自動作成されるDB
```

---

## 🎉 完了！

あとは `demo.py` や `analyze.py` を使って
競馬予想を楽しんでください！

**Good Luck! 🏇**
