@echo off
chcp 65001 > nul
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
set PYTHONIOENCODING=utf-8
for /f %%d in ('powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd'"') do set TODAY=%%d
echo [%date% %time%] 厩舎コメント paraphrase 開始: %TODAY% >> "log\daily_paraphrase.log"
python scripts\local_llm_paraphrase.py %TODAY% >> "log\daily_paraphrase.log" 2>&1
echo [%date% %time%] 完了 >> "log\daily_paraphrase.log"
