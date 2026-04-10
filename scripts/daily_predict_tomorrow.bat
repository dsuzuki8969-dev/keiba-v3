@echo off
chcp 65001 > nul
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
rem 翌日の日付を YYYY-MM-DD 形式で取得
for /f %%d in ('powershell -NoProfile -Command "(Get-Date).AddDays(1).ToString('yyyy-MM-dd')"') do set TOMORROW=%%d
echo [%date% %time%] 翌日予想生成開始: %TOMORROW% >> "log\daily_predict_tomorrow.log"
python run_daily_auto.py --predict --date %TOMORROW% --official >> "log\daily_predict_tomorrow.log" 2>&1
