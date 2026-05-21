# D-AI競馬 タスクスケジューラ登録スクリプト (v3 — 最適時刻版)
# 管理者権限で実行してください
# 使い方: powershell -ExecutionPolicy Bypass -File scripts\setup_scheduler.ps1
#
# v3 変更点:
#   全ジョブを APScheduler (scheduler.py) に統合。WTS は 3 タスクのみ。
#   結果取得を 23:00 に前倒し、翌日予想は 23:30 で当日結果反映。
#   オッズ更新は 5 回定時 + 個別レース (T-15min + T-0min)。
#
#   APScheduler 管理 (scheduler.py):
#     ── 夜間サイクル ──
#     23:00  当日結果取得+DB更新       (job_results_and_db)
#     23:30  翌日予想作成(結果反映)     (job_prediction)        [deps: results_and_db]
#     00:00  パラフレーズ(翌日)         (job_paraphrase_tomorrow) [deps: prediction]
#     01:00  日次メンテナンス           (job_maintenance)       [deps: results_and_db]
#     04:00  DAG+リトライ リセット
#     ── 朝〜日中サイクル ──
#     06:00  当日予想作成               (job_predict_today)
#     06:00  オッズ一括更新(初回)       (job_odds_first_batch)  [deps: predict_today]
#     06:30  パラフレーズ(当日)         (job_paraphrase_today)  [deps: predict_today, odds_first_batch]
#     09:00  オッズ一括更新(2回目)      (job_odds_batch)
#     12:00  オッズ一括更新(3回目)      (job_odds_batch)
#     15:00  オッズ一括更新(4回目)      (job_odds_batch)
#     18:00  オッズ一括更新(5回目)      (job_odds_batch)
#     動的   発走15分前オッズ更新       (T-15min, 06:00時に登録)
#     動的   発走時刻オッズ更新         (T-0min, 06:00時に登録)
#
#   Windows TS 残留:
#     logon  ダッシュボード常駐         (DAI_Keiba_Dashboard)
#     logon  APScheduler 常駐          (DAI_Keiba_Scheduler)
#     5min   Watchdog                  (DAI_Keiba_Watchdog)

$ErrorActionPreference = "Stop"
$ScriptDir = "c:\Users\dsuzu\keiba\keiba-v3"

Write-Host "D-AI Keiba Task Scheduler Setup (v3 - 最適時刻版)" -ForegroundColor Cyan
Write-Host "作業ディレクトリ: $ScriptDir" -ForegroundColor Gray
Write-Host ""

# ── 移行済みタスクの削除 ─────────────────────────────────────────
Write-Host "APScheduler に移行済みのタスクを削除中..." -ForegroundColor Yellow

$MigratedTasks = @(
    "keiba-scheduler",              # 旧タスク (install_scheduler.bat)
    "DAI_Keiba_Predict",            # → job_predict_today (06:00)
    "DAI_Keiba_Results",            # → job_results_and_db (23:00)
    "DAI_Keiba_Maintenance",        # → job_maintenance (01:00)
    "DAI_Keiba_Predict_Tomorrow",   # → job_prediction (23:30)
    "DAI_Keiba_AutoOdds",           # → job_odds_batch (5回定時)
    "DAI_Keiba_Paraphrase_Today",   # → job_paraphrase_today (06:30)
    "DAI_Keiba_Paraphrase_Tomorrow" # → job_paraphrase_tomorrow (00:00)
)

foreach ($task in $MigratedTasks) {
    Unregister-ScheduledTask -TaskName $task -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "  削除: $task" -ForegroundColor Gray
}

Write-Host ""

# ── 1. ダッシュボード常駐（ログオン時起動・自動再起動）────────────
$DashSettings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

$DashAction = New-ScheduledTaskAction `
    -Execute "wscript.exe" `
    -Argument "`"$ScriptDir\scripts\start_dashboard_hidden.vbs`"" `
    -WorkingDirectory $ScriptDir

$DashTrigger = New-ScheduledTaskTrigger -AtLogon

Register-ScheduledTask `
    -TaskName "DAI_Keiba_Dashboard" `
    -Description "D-AI Keiba: ダッシュボード常駐 (ログオン時起動, 失敗時自動再起動)" `
    -Action $DashAction `
    -Trigger $DashTrigger `
    -Settings $DashSettings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "OK: DAI_Keiba_Dashboard (at logon, auto-restart)" -ForegroundColor Green

# ── 2. Watchdog (every 5 min: dashboard + cloudflared) ──
$WdSettings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 2) `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

$WdAction = New-ScheduledTaskAction `
    -Execute "wscript.exe" `
    -Argument "`"$ScriptDir\scripts\watchdog_check_hidden.vbs`"" `
    -WorkingDirectory $ScriptDir

$WdTrigger = New-ScheduledTaskTrigger -Once -At "00:00" -RepetitionInterval (New-TimeSpan -Minutes 5)

Register-ScheduledTask `
    -TaskName "DAI_Keiba_Watchdog" `
    -Description "D-AI Keiba: Watchdog every 5min (dashboard + cloudflared)" `
    -Action $WdAction `
    -Trigger $WdTrigger `
    -Settings $WdSettings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "OK: DAI_Keiba_Watchdog (every 5 min)" -ForegroundColor Green

# ── 3. APScheduler (DAG エンジン) 常駐（ログオン時起動・自動再起動）──
$SchedSettings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

$SchedAction = New-ScheduledTaskAction `
    -Execute "wscript.exe" `
    -Argument "`"$ScriptDir\scripts\start_scheduler_hidden.vbs`"" `
    -WorkingDirectory $ScriptDir

$SchedTrigger = New-ScheduledTaskTrigger -AtLogon

Register-ScheduledTask `
    -TaskName "DAI_Keiba_Scheduler" `
    -Description "D-AI Keiba: APScheduler + DAG エンジン常駐 (v3 最適時刻版, 12ジョブ管理)" `
    -Action $SchedAction `
    -Trigger $SchedTrigger `
    -Settings $SchedSettings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "OK: DAI_Keiba_Scheduler (at logon, auto-restart, 12 jobs)" -ForegroundColor Green

# ── 登録済みタスク一覧 ────────────────────────────────────────
Write-Host ""
Write-Host "登録済みタスク:" -ForegroundColor Yellow
Get-ScheduledTask -TaskName "DAI_Keiba_*" | Select-Object TaskName, State, @{N="NextRunTime";E={
    (Get-ScheduledTaskInfo -TaskName $_.TaskName).NextRunTime
}}

Write-Host ""
Write-Host "Windows TS (残留 3 タスク):" -ForegroundColor Cyan
Write-Host "  logon  DAI_Keiba_Dashboard   - ダッシュボード常駐 (自動再起動)" -ForegroundColor White
Write-Host "  logon  DAI_Keiba_Scheduler   - APScheduler + DAG エンジン常駐" -ForegroundColor White
Write-Host "  5min   DAI_Keiba_Watchdog    - dashboard+cloudflared watchdog" -ForegroundColor White
Write-Host ""
Write-Host "APScheduler 管理 (scheduler.py --status で確認):" -ForegroundColor Cyan
Write-Host "  ── 夜間サイクル ──" -ForegroundColor DarkCyan
Write-Host "  23:00  結果取得+DB更新(当日)  job_results_and_db" -ForegroundColor White
Write-Host "  23:30  翌日予想作成(結果反映)  job_prediction        [deps: results_and_db]" -ForegroundColor White
Write-Host "  00:00  パラフレーズ(翌日)      job_paraphrase_tomorrow [deps: prediction]" -ForegroundColor White
Write-Host "  01:00  日次メンテナンス        job_maintenance       [deps: results_and_db]" -ForegroundColor White
Write-Host "  04:00  DAG+リトライ リセット" -ForegroundColor White
Write-Host "  ── 朝〜日中サイクル ──" -ForegroundColor DarkCyan
Write-Host "  06:00  当日予想作成            job_predict_today" -ForegroundColor White
Write-Host "  06:00  オッズ一括(初回)        job_odds_first_batch  [deps: predict_today]" -ForegroundColor White
Write-Host "  06:30  パラフレーズ(当日)      job_paraphrase_today  [deps: predict_today, odds_first_batch]" -ForegroundColor White
Write-Host "  09:00  オッズ一括(2回目)       job_odds_batch" -ForegroundColor White
Write-Host "  12:00  オッズ一括(3回目)       job_odds_batch" -ForegroundColor White
Write-Host "  15:00  オッズ一括(4回目)       job_odds_batch" -ForegroundColor White
Write-Host "  18:00  オッズ一括(5回目)       job_odds_batch" -ForegroundColor White
Write-Host "  動的   発走15分前+発走時刻     個別レースオッズ (06:00時に登録)" -ForegroundColor White
