# D-AI競馬 タスクスケジューラ登録スクリプト
# 管理者権限で実行してください
# 使い方: powershell -ExecutionPolicy Bypass -File scripts\setup_scheduler.ps1

$ErrorActionPreference = "Stop"
$ScriptDir = "c:\Users\dsuzu\keiba\keiba-v3"

Write-Host "D-AI Keiba Task Scheduler Setup" -ForegroundColor Cyan
Write-Host "作業ディレクトリ: $ScriptDir" -ForegroundColor Gray

$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -WakeToRun

# ── 1. 予想生成タスク（毎朝6:00）────────────────────────────
$PredictAction = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$ScriptDir\scripts\daily_predict.bat`"" `
    -WorkingDirectory $ScriptDir

$PredictTrigger = New-ScheduledTaskTrigger -Daily -At "06:00"

Register-ScheduledTask `
    -TaskName "DAI_Keiba_Predict" `
    -Description "D-AI Keiba: 予想生成 毎朝06:00" `
    -Action $PredictAction `
    -Trigger $PredictTrigger `
    -Settings $Settings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "OK: DAI_Keiba_Predict (daily 06:00)" -ForegroundColor Green

# ── 2. 結果照合タスク（毎夜22:00）───────────────────────────
$ResultsAction = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$ScriptDir\scripts\daily_results.bat`"" `
    -WorkingDirectory $ScriptDir

$ResultsTrigger = New-ScheduledTaskTrigger -Daily -At "22:00"

Register-ScheduledTask `
    -TaskName "DAI_Keiba_Results" `
    -Description "D-AI Keiba: 結果照合 毎夜22:00" `
    -Action $ResultsAction `
    -Trigger $ResultsTrigger `
    -Settings $Settings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "OK: DAI_Keiba_Results (daily 22:00)" -ForegroundColor Green

# ── 3. 日次メンテナンス（毎夜23:00）─────────────────────────
$MaintAction = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$ScriptDir\scripts\daily_maintenance.bat`"" `
    -WorkingDirectory $ScriptDir

$MaintTrigger = New-ScheduledTaskTrigger -Daily -At "23:00"

Register-ScheduledTask `
    -TaskName "DAI_Keiba_Maintenance" `
    -Description "D-AI Keiba: 払戻バックフィル+整合性チェック 毎夜23:00 (日曜VACUUM/月初CSV更新)" `
    -Action $MaintAction `
    -Trigger $MaintTrigger `
    -Settings $Settings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "OK: DAI_Keiba_Maintenance (daily 23:00)" -ForegroundColor Green

# ── 4. 翌日予想生成タスク（毎夕17:00）─────────────────────────
$PredictTomorrowAction = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$ScriptDir\scripts\daily_predict_tomorrow.bat`"" `
    -WorkingDirectory $ScriptDir

$PredictTomorrowTrigger = New-ScheduledTaskTrigger -Daily -At "17:00"

Register-ScheduledTask `
    -TaskName "DAI_Keiba_Predict_Tomorrow" `
    -Description "D-AI Keiba: 翌日予想生成（公式のみ）毎夕17:00" `
    -Action $PredictTomorrowAction `
    -Trigger $PredictTomorrowTrigger `
    -Settings $Settings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "OK: DAI_Keiba_Predict_Tomorrow (daily 17:00)" -ForegroundColor Green

# ── 登録済みタスク一覧 ────────────────────────────────────────
Write-Host ""
Write-Host "登録済みタスク:" -ForegroundColor Yellow
Get-ScheduledTask -TaskName "DAI_Keiba_*" | Select-Object TaskName, State, @{N="NextRunTime";E={
    (Get-ScheduledTaskInfo -TaskName $_.TaskName).NextRunTime
}}

Write-Host ""
Write-Host "スケジュール:" -ForegroundColor Cyan
Write-Host "  06:00  DAI_Keiba_Predict          - 当日予想生成" -ForegroundColor White
Write-Host "  17:00  DAI_Keiba_Predict_Tomorrow - 翌日予想生成（公式のみ）" -ForegroundColor White
Write-Host "  22:00  DAI_Keiba_Results          - 結果照合" -ForegroundColor White
Write-Host "  23:00  DAI_Keiba_Maintenance      - 払戻バックフィル + 整合性チェック" -ForegroundColor White
Write-Host "                                      (日曜: VACUUM, 月初: CSV更新)" -ForegroundColor Gray
