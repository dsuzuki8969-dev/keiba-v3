@echo off
chcp 65001 > nul
REM ============================================================
REM D-AI Keiba 夜間処理バッチ v2 (改善施策 全実装版)
REM  1. Optuna HPO (50 trials, ~60分)
REM  2. LightGBM 全モデル再学習 (最適パラメータ適用, ~40分)
REM  3. LambdaRank モデル学習 (~20分)
REM  4. Walk-Forward CV 評価 (~30分)
REM ============================================================

cd /d "C:\Users\dsuzu\keiba\keiba-v3"

set STAMP=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%
set STAMP=%STAMP: =0%
set LOG=log\overnight_%STAMP%.log

if not exist log mkdir log

echo [%date% %time%] === 夜間処理開始 (v2) === >> "%LOG%"
echo 夜間処理ログ: %LOG%
echo.

REM ── 1. Optuna HPO ────────────────────────────────────────────
echo [1/4] Optuna HPO 実行中... (50 trials, ~60分)
echo [%date% %time%] [1/4] Optuna HPO 開始 >> "%LOG%"
python scripts\optuna_hpo.py --n-trials 50 >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] Optuna HPO でエラーが発生しました (既存パラメータで継続)
    echo [%date% %time%] [1/4] Optuna HPO WARN >> "%LOG%"
) else (
    echo  [OK] Optuna HPO 完了
    echo [%date% %time%] [1/4] Optuna HPO OK >> "%LOG%"
)

REM ── 2. LightGBM 全モデル再学習 ──────────────────────────────
echo [2/4] LightGBM 全モデル再学習中... (30〜60分)
echo [%date% %time%] [2/4] LightGBM 全モデル再学習開始 >> "%LOG%"
python retrain_all.py --lgbm >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] LightGBM 再学習でエラーが発生しました
    echo [%date% %time%] [2/4] LightGBM 再学習 WARN >> "%LOG%"
) else (
    echo  [OK] LightGBM 再学習完了
    echo [%date% %time%] [2/4] LightGBM 再学習 OK >> "%LOG%"
)

REM ── 3. LambdaRank 学習 ────────────────────────────────────────
echo [3/4] LambdaRank モデル学習中... (10〜20分)
echo [%date% %time%] [3/4] LambdaRank 学習開始 >> "%LOG%"
python -c "import sys; sys.stdout.reconfigure(encoding='utf-8'); from src.ml.lgbm_ranker import train_ranker; m=train_ranker(); print('NDCG@3(valid):', m['ndcg3_valid'])" >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] LambdaRank 学習でエラーが発生しました
    echo [%date% %time%] [3/4] LambdaRank 学習 WARN >> "%LOG%"
) else (
    echo  [OK] LambdaRank 学習完了
    echo [%date% %time%] [3/4] LambdaRank 学習 OK >> "%LOG%"
)

REM ── 4. Walk-Forward CV ────────────────────────────────────────
echo [4/4] Walk-Forward CV 実行中... (30〜60分)
echo [%date% %time%] [4/4] Walk-Forward CV 開始 >> "%LOG%"
python scripts\walk_forward_cv.py --fold-months 3 --min-train-months 6 >> "%LOG%" 2>&1
if errorlevel 1 (
    echo  [WARN] Walk-Forward CV でエラーが発生しました
    echo [%date% %time%] [4/4] Walk-Forward CV WARN >> "%LOG%"
) else (
    echo  [OK] Walk-Forward CV 完了
    echo [%date% %time%] [4/4] Walk-Forward CV OK >> "%LOG%"
)

echo.
echo [%date% %time%] === 夜間処理完了 === >> "%LOG%"
echo 完了。ログ: %LOG%
