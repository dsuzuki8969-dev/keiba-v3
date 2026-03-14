@echo off
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
echo [D-AI競馬予想] ダッシュボード ウォッチドッグ起動
echo.

REM ==================================================
REM サーバー設定
REM ==================================================
set KEIBA_HOST=0.0.0.0
set KEIBA_PORT=5051
set KEIBA_TUNNEL=0

REM ウォッチドッグ設定
set MAX_RESTART=50
set RESTART_DELAY=10
set /a RESTART_COUNT=0

REM ログファイル
set WATCHDOG_LOG=data\watchdog.log

echo [ウォッチドッグ] 最大再起動回数: %MAX_RESTART%
echo [ウォッチドッグ] 再起動間隔: %RESTART_DELAY%秒
echo [ウォッチドッグ] ログ: %WATCHDOG_LOG%
echo.

REM 既に起動中か確認
netstat -an | find "0.0.0.0:%KEIBA_PORT%" | find "LISTENING" > nul 2>&1
if %errorlevel% equ 0 (
    echo [WARNING] ポート %KEIBA_PORT% は既に使用中です。
    echo [WARNING] 既存プロセスを停止してから再実行してください。
    pause
    exit /b 1
)

:restart_loop
set /a RESTART_COUNT+=1
if %RESTART_COUNT% gtr %MAX_RESTART% goto max_reached

echo ============================================================>> %WATCHDOG_LOG%
echo [%date% %time%] ダッシュボード起動 (試行 %RESTART_COUNT%/%MAX_RESTART%)>> %WATCHDOG_LOG%
echo ============================================================>> %WATCHDOG_LOG%

if %RESTART_COUNT% equ 1 (
    echo [ウォッチドッグ] ダッシュボード初回起動中...
    timeout /t 3 /nobreak > nul
    start "" "http://127.0.0.1:%KEIBA_PORT%/d-ai-keiba/"
) else (
    echo [ウォッチドッグ] ダッシュボード再起動中... (試行 %RESTART_COUNT%/%MAX_RESTART%)
    echo [%date% %time%] 再起動待機 %RESTART_DELAY%秒>> %WATCHDOG_LOG%
    timeout /t %RESTART_DELAY% /nobreak > nul
)

REM ダッシュボードをフォアグラウンドで実行（終了を検知するため）
echo [%date% %time%] python src/dashboard.py 開始>> %WATCHDOG_LOG%
python src\dashboard.py
set EXIT_CODE=%errorlevel%

echo [%date% %time%] ダッシュボード終了 (exit code: %EXIT_CODE%)>> %WATCHDOG_LOG%
echo.
echo [ウォッチドッグ] ダッシュボードが停止しました (exit code: %EXIT_CODE%)

REM 正常終了(0)の場合は再起動しない
if %EXIT_CODE% equ 0 (
    echo [ウォッチドッグ] 正常終了のため再起動しません。
    echo [%date% %time%] 正常終了 — 再起動不要>> %WATCHDOG_LOG%
    goto end
)

echo [ウォッチドッグ] 異常終了を検知。%RESTART_DELAY%秒後に再起動します...
goto restart_loop

:max_reached
echo [ERROR] 最大再起動回数 (%MAX_RESTART%) に達しました。>> %WATCHDOG_LOG%
echo [ERROR] 最大再起動回数 (%MAX_RESTART%) に達しました。
echo [ERROR] ログを確認してください: %WATCHDOG_LOG%
pause
exit /b 1

:end
echo [ウォッチドッグ] 終了しました。
pause
exit /b 0
