@echo off
chcp 65001 > nul
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
set PYTHONIOENCODING=utf-8
for /f %%d in ('powershell -NoProfile -Command "(Get-Date).AddDays(1).ToString('yyyyMMdd')"') do set TOMORROW=%%d
echo [%date% %time%] 厩舎コメント paraphrase (翌日) 開始: %TOMORROW% >> "log\daily_paraphrase.log"
python scripts\local_llm_paraphrase.py %TOMORROW% >> "log\daily_paraphrase.log" 2>&1
echo [%date% %time%] 完了 >> "log\daily_paraphrase.log"
