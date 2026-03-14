@echo off
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
python run_daily_auto.py --results >> "log\daily_results.log" 2>&1
