@echo off
chcp 65001 > nul
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
set PYTHONUNBUFFERED=1
set KEIBA_HOST=0.0.0.0
set KEIBA_PORT=5051

REM ポート重複チェック
netstat -an | find "0.0.0.0:5051" | find "LISTENING" > nul 2>&1
if %errorlevel% equ 0 (
    echo %date% %time% [SKIP] ポート5051は既に使用中 >> "data\logs\dashboard.log"
    exit /b 0
)

python src/dashboard.py >> "data\logs\dashboard.log" 2>&1
