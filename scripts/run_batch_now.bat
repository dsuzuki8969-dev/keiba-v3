@echo off
cd /d C:\Users\dsuzu\keiba\keiba-v3
python -X utf8 -u scripts/batch_reanalyze.py --start 2026-01-01 --end 2026-03-28 --parallel 3 --workers 2 --resume-after 2026-03-29T23:50 > batch_log.txt 2>&1
echo DONE >> batch_log.txt
pause
