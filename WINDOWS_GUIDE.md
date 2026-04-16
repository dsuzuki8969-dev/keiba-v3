# Windows11での使い方

## 必要なもの
- Windows11
- Python 3.11+
- インターネット接続

---

## ステップ1: 環境セットアップ（初回のみ）

### 1-1: setup.batを実行
`setup.bat` をダブルクリック
→ Pythonパッケージが自動インストールされます

### 1-2: 認証情報セットアップ
```
python src/setup_credentials.py
```
→ netkeiba/競馬ブックのログイン情報を設定

### 1-3: DB構築
```
python build_horse_db.py
```
→ 馬DB・基準タイムDBを構築（数分〜数十分）

---

## ステップ2: タスクスケジューラ登録

管理者権限のPowerShellで:
```powershell
powershell -ExecutionPolicy Bypass -File scripts\setup_scheduler.ps1
```

以下のタスクが自動登録されます:
| タスク | 時刻 | 内容 |
|--------|------|------|
| DAI_Keiba_Predict | 06:00 | 当日予想生成 |
| DAI_Keiba_Predict_Tomorrow | 17:00 | 翌日予想生成 |
| DAI_Keiba_Results | 22:00 | 結果照合 |
| DAI_Keiba_Maintenance | 23:00 | メンテナンス |
| DAI_Keiba_Dashboard | ログオン時 | ダッシュボード常駐 |
| DAI_Keiba_Watchdog | 5分間隔 | 監視・自動再起動 |

※ 全タスクはウィンドウ非表示で実行されます

---

## ステップ3: 手動実行

### 予想生成（日付指定）
```
python run_analysis_date.py 2026-04-12
```

### ダッシュボード起動
```
python src/dashboard.py
```
→ http://localhost:5051 でアクセス

### モデル再学習
```
python retrain_all.py
```

---

## トラブルシューティング

### エラー: ModuleNotFoundError
```
python -m pip install -r requirements.txt
```

### ダッシュボードが起動しない
ポート5051が既に使用中の場合:
```
netstat -an | find "5051"
```

### タスクスケジューラの状態確認
```powershell
Get-ScheduledTask -TaskName "DAI_Keiba_*" | Select TaskName, State
```
