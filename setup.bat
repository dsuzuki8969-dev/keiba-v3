@echo off
chcp 65001 > nul
echo ================================================
echo 競馬解析マスターシステム v3.0 - セットアップ
echo ================================================
echo.

python --version > nul 2>&1
if errorlevel 1 (
    echo [エラー] Python がインストールされていません。
    echo Python 3.9 以上をインストールしてください。
    echo   https://www.python.org/downloads/
    pause
    exit /b 1
)

echo 依存パッケージをインストールしています...
python -m pip install -r requirements.txt
if errorlevel 1 (
    echo [エラー] インストールに失敗しました。
    pause
    exit /b 1
)

echo.
echo 動作確認中...
python -c "from src.scraper.netkeiba import NetkeibaClient; print('  OK')" 2>nul
if errorlevel 1 (
    echo [警告] インポートテストに失敗しました。プロジェクトルートで実行していますか？
)

echo ================================================
echo セットアップ完了！
echo ================================================
echo.
echo 次のステップ:
echo   1. 認証設定: python -m src.setup_credentials
echo   2. デモ実行:  python demo.py
echo   3. 日付分析:  python daily.py または python main.py --analyze_date 2025-12-28
echo   4. 1レース:   python main.py --race_id 202506050801
echo   5. 基準タイム事前収集:
echo      python main.py --collect_course_db --start 2024-01-01 --end 2025-12-31
echo      python main.py --resume  途中再開  /  --append  新規分のみ
echo   6. Web管理画面（ブラウザで操作）:
echo      python main.py --serve
echo.
pause
