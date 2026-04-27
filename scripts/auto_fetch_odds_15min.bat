@echo off
REM Python スクリプト側で RotatingFileHandler が logs\auto_fetch_odds_15min.log に書込むため、
REM bat 側でのリダイレクトは禁止（二重ロックで PermissionError が発生する。2026-04-27 修正）
cd /d "c:\Users\dsuzu\keiba\keiba-v3"
python scripts\auto_fetch_odds_15min.py
