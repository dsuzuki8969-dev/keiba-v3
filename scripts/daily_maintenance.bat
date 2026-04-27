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
echo [1/5] 払戻バックフィル...
python scripts\backfill_payouts_from_html.py --year %date:~0,4% >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] 払戻バックフィルでエラーが発生しました
) else (
    echo  [OK] 払戻バックフィル完了
)

REM ── 2. データ整合性チェック ────────────────────────────────
echo [2/5] 整合性チェック...
python scripts\check_data_integrity.py --start 2025-01-01 >> "%LOG%" 2>&1
echo  [OK] 整合性チェック完了

REM ── 3. 週1回DBメンテナンス（日曜日のみ）────────────────────
for /f "tokens=*" %%d in ('powershell -command "(Get-Date).DayOfWeek"') do set DOW=%%d
if "%DOW%"=="Sunday" (
    echo [3/5] DBメンテナンス (日曜日)...
    python scripts\db_maintenance.py --keep 7 >> "%LOG%" 2>&1
    echo  [OK] DBメンテナンス完了
) else (
    echo [3/5] DBメンテナンス: スキップ (日曜のみ実行)
)

REM ── 4. 月初CSVエクスポート更新 ─────────────────────────────
REM  毎月1日のみ全期間CSVを再生成
if "%date:~8,2%"=="01" (
    echo [4/5] 月初CSVエクスポート更新...
    python scripts\export_results_csv.py >> "%LOG%" 2>&1
    python scripts\export_stats_csv.py   >> "%LOG%" 2>&1
    echo  [OK] CSV更新完了
) else (
    echo [4/5] CSVエクスポート: スキップ (月初のみ実行)
)

REM ── 5. 成績ページ用サマリJSONキャッシュ再生成 ─────────────
REM  /api/results/{summary,sanrentan_summary,detailed,trend} の応答を
REM  234秒→<100ms に短縮するためのキャッシュ事前生成
echo [5/5] 成績サマリキャッシュ再生成...
python scripts\build_results_cache.py --workers 4 --force >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] サマリキャッシュ生成でエラーが発生しました
) else (
    echo  [OK] サマリキャッシュ生成完了
)

REM ── 6. l3f_sec・corners バックフィル（直近7日分）──────────────
echo [6/7] l3f_sec + corners バックフィル（直近7日）...
python scripts\rebuild_race_log_l3f_corners.py --year %date:~0,4% >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] l3f+corners バックフィルでエラーが発生しました
) else (
    echo  [OK] l3f+corners バックフィル完了
)

REM ── 7. run_dev バックフィル（直近7日分）──────────────────────
echo [7/8] run_dev バックフィル（直近7日）...
python scripts\backfill_run_dev.py --since %date:~0,4%-%date:~5,2%-%date:~8,2% >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] run_dev バックフィルでエラーが発生しました
) else (
    echo  [OK] run_dev バックフィル完了
)

REM ── 7b. pred.json 再注入（run_dev/race_level_dev、直近7日）───
echo [7b/9] pred.json 再注入...
python scripts\refresh_pred_run_dev.py --recent 7 >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] pred.json 再注入でエラーが発生しました
) else (
    echo  [OK] pred.json 再注入完了
)

REM ── 7c. ローカル LLM 厩舎コメントパラフレーズ（直近 7 日） ──────
echo [7c/9] LLM パラフレーズ...
python scripts\local_llm_paraphrase.py --recent 7 >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] LLM パラフレーズでエラー（LM Studio が稼働していない可能性）
) else (
    echo  [OK] LLM パラフレーズ完了
)

REM ── データ品質チェック（最後に実行、閾値超えで exit 1）────────
echo [QC] データ品質チェック...
python scripts\daily_data_quality_check.py >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] データ品質チェック: 閾値超えあり（ログ確認要）
    REM exit /b 1 で呼び出し元にも失敗を伝える（タスクスケジューラ失敗扱い）
    exit /b 1
) else (
    echo  [OK] データ品質チェック: 全指標正常
)

echo.
echo [%date% %time%] 完了 >> "%LOG%"
echo 完了。ログ: %LOG%
