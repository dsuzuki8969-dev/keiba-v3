@echo off
chcp 65001 > nul
REM ============================================================
REM D-AI Keiba 日次メンテナンスバッチ
REM  - 払戻データのバックフィル（当日・翌日のHTMLキャッシュから）
REM  - データ整合性チェック
REM  - 週1回のDBメンテナンス（日曜日のみ）
REM  - 月次CSVエクスポート更新（月初のみ）
REM 推奨実行時刻: 毎日23:00（レース結果確定後）
REM ============================================================

cd /d "c:\Users\dsuzu\keiba\keiba-v3"
if not exist log mkdir log

set STAMP=%date:~0,4%%date:~5,2%%date:~8,2%
set LOG=log\maintenance_%STAMP%.log

echo [%date% %time%] D-AI Keiba 日次メンテナンス開始 >> "%LOG%"
echo.

REM ── 1. 払戻バックフィル（当年のみ）────────────────────────
echo [1/4] 払戻バックフィル...
python scripts\backfill_payouts_from_html.py --year %date:~0,4% >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] 払戻バックフィルでエラーが発生しました
) else (
    echo  [OK] 払戻バックフィル完了
)

REM ── 2. データ整合性チェック ────────────────────────────────
echo [2/4] 整合性チェック...
python scripts\check_data_integrity.py --start 2025-01-01 >> "%LOG%" 2>&1
echo  [OK] 整合性チェック完了

REM ── 3. 週1回DBメンテナンス（日曜日のみ）────────────────────
for /f "tokens=*" %%d in ('powershell -command "(Get-Date).DayOfWeek"') do set DOW=%%d
if "%DOW%"=="Sunday" (
    echo [3/4] DBメンテナンス (日曜日)...
    python scripts\db_maintenance.py --keep 7 >> "%LOG%" 2>&1
    echo  [OK] DBメンテナンス完了
) else (
    echo [3/4] DBメンテナンス: スキップ (日曜のみ実行)
)

REM ── 4. 月初CSVエクスポート更新 ─────────────────────────────
REM  毎月1日のみ全期間CSVを再生成
if "%date:~8,2%"=="01" (
    echo [4/4] 月初CSVエクスポート更新...
    python scripts\export_results_csv.py >> "%LOG%" 2>&1
    python scripts\export_stats_csv.py   >> "%LOG%" 2>&1
    echo  [OK] CSV更新完了
) else (
    echo [4/4] CSVエクスポート: スキップ (月初のみ実行)
)

echo.
echo [%date% %time%] 完了 >> "%LOG%"
echo 完了。ログ: %LOG%
