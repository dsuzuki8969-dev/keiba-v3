@echo off
chcp 65001 > nul
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
if errorlevel 1 (
    echo [%date% %time%] cd /d 失敗 >> "log\daily_predict.log"
    exit /b 1
)
echo [%date% %time%] 当日予想生成開始 PID=%RANDOM% PWD=%CD% >> "log\daily_predict.log"
python --version >> "log\daily_predict.log" 2>&1
if errorlevel 1 (
    echo [%date% %time%] python 不在 / PATH 不足 >> "log\daily_predict.log"
    exit /b 1
)
python run_daily_auto.py --predict >> "log\daily_predict.log" 2>&1
echo [%date% %time%] 当日予想生成終了 ec=%ERRORLEVEL% >> "log\daily_predict.log"
