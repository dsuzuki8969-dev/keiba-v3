@echo off
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
echo [D-AI競馬予想] Cloudflare Tunnel 起動中...
echo.

set KEIBA_PORT=5051
set TUNNEL_LOG=data\tunnel.log

REM ダッシュボードが起動済みか確認
netstat -an | find "0.0.0.0:%KEIBA_PORT%" | find "LISTENING" > nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] ダッシュボードが起動していません。
    echo [ERROR] 先に start_dashboard.bat を実行してください。
    pause
    exit /b 1
)

echo [OK] ダッシュボード確認済み (port %KEIBA_PORT%)
echo.
echo [Cloudflare Tunnel] 外部公開URLを取得中...（約10〜30秒かかります）
echo.

REM 古いログを削除
if exist %TUNNEL_LOG% del %TUNNEL_LOG%

REM トンネルをバックグラウンドで起動
start "D-AI-Tunnel" cloudflared.exe tunnel --url http://127.0.0.1:%KEIBA_PORT% --logfile %TUNNEL_LOG%

REM URLが出るまで待機（最大30秒）
set /a WAIT=0
:wait_loop
timeout /t 2 /nobreak > nul
set /a WAIT+=2
findstr /C:"trycloudflare.com" %TUNNEL_LOG% > nul 2>&1
if %errorlevel% equ 0 goto found_url
if %WAIT% geq 30 goto timeout_err
goto wait_loop

:found_url
echo ============================================================
echo  外出先からアクセスできるURLが発行されました:
echo.
for /f "tokens=*" %%a in ('findstr /C:"trycloudflare.com" %TUNNEL_LOG%') do (
    echo  %%a
)
echo.
echo  このURLをスマホ・外部PCのブラウザで開いてください。
echo  ※ このウィンドウを閉じるとトンネルが停止します。
echo ============================================================
echo.
echo [Ctrl+C] でトンネルを停止できます
pause
exit /b 0

:timeout_err
echo [ERROR] URLの取得がタイムアウトしました。
echo [INFO] ログを確認してください: %TUNNEL_LOG%
pause
exit /b 1
