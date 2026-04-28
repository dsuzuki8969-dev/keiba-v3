@echo off
cd /d "C:\Users\dsuzu\keiba\keiba-v3\frontend"
call npm run build
echo EXIT_CODE=%errorlevel%
