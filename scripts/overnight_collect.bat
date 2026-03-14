@echo off
REM 夜間スクレイピング: 2024-01-01 からの基準タイムDB収集
REM PCを起動したまま実行。中断しても再度実行で続きから再開。

cd /d "%~dp0.."
if not exist log mkdir log

set STAMP=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set STAMP=%STAMP: =0%
set LOG=log\overnight_%STAMP%.log

echo ログ: %LOG%
echo.

python scripts\overnight_collect.py > "%LOG%" 2>&1
echo 標準出力・エラーは %LOG% に保存されています。
echo.
type "%LOG%" | more
if errorlevel 1 (
  echo.
  echo エラーが発生しました。ログを確認してください。
  pause
)
