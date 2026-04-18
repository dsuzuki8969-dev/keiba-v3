@echo off
REM ダッシュボード強制再起動（管理者権限必須）
REM 右クリック → 管理者として実行

echo [1/3] 既存ダッシュボードプロセスを停止...
taskkill /F /IM python.exe /FI "WINDOWTITLE eq dashboard*" 2>nul
for /f "tokens=2 delims=," %%a in ('tasklist /FI "IMAGENAME eq python.exe" /FO CSV /NH ^| findstr python') do (
    taskkill /F /PID %%~a 2>nul
)

echo [2/3] 3秒待機...
timeout /t 3 /nobreak >nul

echo [3/3] タスクスケジューラ経由で再起動...
schtasks /Run /TN DAI_Keiba_Dashboard

echo.
echo 完了。5秒後にヘルスチェックを実行します...
timeout /t 5 /nobreak >nul
curl -s http://localhost:5051/api/health
echo.
pause
