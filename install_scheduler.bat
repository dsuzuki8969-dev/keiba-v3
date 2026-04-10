@echo off
REM keiba-scheduler をタスクスケジューラーに登録（管理者権限で実行）
schtasks /create /tn "keiba-scheduler" /tr "\"C:\Program Files\Python311\python.exe\" C:\Users\dsuzu\keiba\keiba-v3\scheduler.py" /sc onlogon /rl highest /f
if %errorlevel% equ 0 (
    echo タスク登録成功: keiba-scheduler
) else (
    echo 登録失敗 - 管理者として実行してください
)
pause
