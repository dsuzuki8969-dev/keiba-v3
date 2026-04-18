@echo off
chcp 65001 > NUL
cd /d "c:\Users\dsuzu\keiba\keiba-v3"

REM Dashboard check (port 5051 LISTENING)
netstat -an | find "0.0.0.0:5051" | find "LISTENING" > NUL 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] Dashboard down, restarting... >> data\watchdog.log
    REM pythonw.exe is console-less; /B avoids spawning a new cmd window
    start "" /B "C:\Program Files\Python311\pythonw.exe" src\dashboard.py
)

REM cloudflared check (process exists)
tasklist /FI "IMAGENAME eq cloudflared.exe" | find /I "cloudflared.exe" > NUL 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] cloudflared down, restarting... >> data\watchdog.log
    REM /B reuses parent console instead of opening a new one
    start "" /B "c:\Users\dsuzu\keiba\keiba-v3\cloudflared.exe" tunnel --config "C:\Users\dsuzu\.cloudflared\config.yml" run keiba-dash
)
