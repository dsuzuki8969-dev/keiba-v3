@echo off
chcp 65001 > nul
cd /d "%~dp0.."

REM ダッシュボードサーバー(port 5051)が既に起動中か確認
netstat -ano | findstr ":5051" | findstr "LISTENING" > nul 2>&1
if %errorlevel% == 0 (
    echo ダッシュボードは既に起動中です。ブラウザで開きます...
    start http://localhost:5051
    exit /b 0
)

REM 起動していなければサーバーをバックグラウンドで起動
echo ダッシュボードサーバーを起動中...
start "D-AI競馬ダッシュボード" /min python src\dashboard.py

REM サーバーが応答するまで待機
:WAIT
timeout /t 2 /nobreak > nul
netstat -ano | findstr ":5051" | findstr "LISTENING" > nul 2>&1
if %errorlevel% neq 0 goto WAIT

REM ブラウザで開く
echo 起動完了。ブラウザを開きます...
start http://localhost:5051
