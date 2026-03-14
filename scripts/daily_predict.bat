@echo off
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
python run_daily_auto.py --predict >> "log\daily_predict.log" 2>&1
