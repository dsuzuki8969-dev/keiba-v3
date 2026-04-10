@echo off
chcp 65001 > NUL
cd /d "c:\Users\dsuzu\keiba\keiba-v3"

REM Dashboard check
netstat -an | find "0.0.0.0:5051" | find "LISTENING" > NUL 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] Dashboard down, restarting... >> data\watchdog.log
    start "D-AI-Dashboard-WD" python src\dashboard.py
)

REM cloudflared check
sc query cloudflared | find "RUNNING" > NUL 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] cloudflared down, restarting... >> data\watchdog.log
    net start cloudflared > NUL 2>&1
)