@echo off
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
echo [D-AI競馬予想] ダッシュボード起動中...

REM ==================================================
REM サーバー設定（変更可能）
REM --------------------------------------------------
REM KEIBA_HOST=0.0.0.0  → LAN全体に公開（スマホ・PC からアクセス可）
REM KEIBA_HOST=127.0.0.1 → このPC のみ（ローカル専用・デフォルト）
REM --------------------------------------------------
set KEIBA_HOST=0.0.0.0
set KEIBA_PORT=5051

REM 認証設定（LAN公開時はコメントアウトを外して設定推奨）
REM set KEIBA_AUTH=true
REM set KEIBA_USER=admin
REM set KEIBA_PASS=yourpassword

REM ==================================================
REM Cloudflare Tunnel設定
REM  KEIBA_TUNNEL=1  → 外出先からアクセス用のトンネルを起動
REM  KEIBA_TUNNEL=0  → LAN内のみ（デフォルト）
REM --------------------------------------------------
set KEIBA_TUNNEL=0
REM ==================================================

REM ==================================================
REM ウォッチドッグ設定（自動再起動）
REM  KEIBA_WATCHDOG=1  → 落ちたら自動再起動（推奨）
REM  KEIBA_WATCHDOG=0  → 従来通り（デフォルト）
REM --------------------------------------------------
set KEIBA_WATCHDOG=1
REM ==================================================

REM 既に起動中か確認
netstat -an | find "0.0.0.0:%KEIBA_PORT%" | find "LISTENING" > nul 2>&1
if %errorlevel% equ 0 (
    echo [INFO] ダッシュボードは既に起動中です
    start "" "http://127.0.0.1:%KEIBA_PORT%/d-ai-keiba/"
    exit /b 0
)

REM Cloudflare Tunnelを起動（KEIBA_TUNNEL=1 のとき）
if "%KEIBA_TUNNEL%"=="1" (
    echo [Cloudflare Tunnel] 外部公開URLを取得中...
    start "D-AI-Tunnel" cloudflared.exe tunnel --url http://127.0.0.1:%KEIBA_PORT% --logfile data\tunnel.log
    echo [Cloudflare Tunnel] URLはしばらく後に data\tunnel.log に記録されます
    echo [Cloudflare Tunnel] 確認コマンド: type data\tunnel.log ^| findstr trycloudflare
)

REM ウォッチドッグモードの場合
if "%KEIBA_WATCHDOG%"=="1" (
    echo [ウォッチドッグ] 自動再起動モードで起動します
    echo [ウォッチドッグ] このウィンドウを閉じるとダッシュボードも停止します
    echo.
    timeout /t 3 /nobreak > nul
    start "" "http://127.0.0.1:%KEIBA_PORT%/d-ai-keiba/"
    goto watchdog_loop
)

REM 従来モード（バックグラウンド起動）
start "D-AI-Dashboard" python src\dashboard.py
timeout /t 5 /nobreak > nul
start "" "http://127.0.0.1:%KEIBA_PORT%/d-ai-keiba/"
exit /b 0

REM ============ ウォッチドッグループ ============
:watchdog_loop
set /a WD_COUNT=0
set WD_MAX=50
set WD_DELAY=10

:wd_restart
set /a WD_COUNT+=1
if %WD_COUNT% gtr %WD_MAX% (
    echo [ERROR] 最大再起動回数 (%WD_MAX%) に達しました。
    pause
    exit /b 1
)

if %WD_COUNT% gtr 1 (
    echo.
    echo [ウォッチドッグ] ダッシュボードが停止しました。%WD_DELAY%秒後に再起動... (試行 %WD_COUNT%/%WD_MAX%)
    timeout /t %WD_DELAY% /nobreak > nul
)

echo [ウォッチドッグ] ダッシュボード起動中... (試行 %WD_COUNT%/%WD_MAX%)
python src\dashboard.py
set WD_EXIT=%errorlevel%

if %WD_EXIT% equ 0 (
    echo [ウォッチドッグ] 正常終了
    goto wd_end
)
goto wd_restart

:wd_end
