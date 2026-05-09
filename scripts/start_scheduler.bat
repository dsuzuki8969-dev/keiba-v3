@echo off
chcp 65001 > nul
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
set PYTHONUNBUFFERED=1

REM 二重起動防止: scheduler.py が既に動いていたらスキップ
tasklist /FI "IMAGENAME eq python.exe" /V 2>nul | find "scheduler.py" > nul 2>&1
if %errorlevel% equ 0 (
    echo %date% %time% [SKIP] scheduler.py は既に実行中 >> "data\logs\scheduler_launcher.log"
    exit /b 0
)

python scheduler.py >> "data\logs\scheduler_launcher.log" 2>&1
