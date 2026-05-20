@echo off
chcp 65001 >nul 2>&1
cd /d C:\Users\dsuzu\keiba\keiba-v3
"C:\Program Files\Python311\python.exe" scripts\batch_rerun_resume.py
pause
