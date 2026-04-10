# D-AI競馬 タスクスケジューラ登録スクリプト
# 管理者権限で実行してください
# 使い方: powershell -ExecutionPolicy Bypass -File scripts\setup_scheduler.ps1

$ErrorActionPreference = "Stop"
$ScriptDir = "c:\Users\dsuzu\keiba\keiba-v3"

Write-Host "D-AI Keiba Task Scheduler Setup" -ForegroundColor Cyan
Write-Host "作業ディレクトリ: $ScriptDir" -ForegroundColor Gray

# 旧タスク削除（install_scheduler.bat で登録されたもの）
Unregister-ScheduledTask -TaskName "keiba-scheduler" -Confirm:$false -ErrorAction SilentlyContinue
Write-Host "旧タスク keiba-scheduler を削除" -ForegroundColor Gray

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

# ── 5. ダッシュボード常駐（ログオン時起動・自動再起動）────────────
$DashSettings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

$DashAction = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$ScriptDir\scripts\start_dashboard.bat`"" `
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

# ── 6. Watchdog (every 5 min: dashboard + cloudflared) ──
$WatchdogBat = "$ScriptDir\scripts\watchdog_check.bat"
$WatchdogText = "@echo off`r`nchcp 65001 > NUL`r`ncd /d `"c:\Users\dsuzu\keiba\keiba-v3`"`r`n`r`nREM Dashboard check`r`nnetstat -an | find `"0.0.0.0:5051`" | find `"LISTENING`" > NUL 2>&1`r`nif %errorlevel% neq 0 (`r`n    echo [%date% %time%] Dashboard down, restarting... >> data\watchdog.log`r`n    start `"D-AI-Dashboard-WD`" python src\dashboard.py`r`n)`r`n`r`nREM cloudflared check`r`nsc query cloudflared | find `"RUNNING`" > NUL 2>&1`r`nif %errorlevel% neq 0 (`r`n    echo [%date% %time%] cloudflared down, restarting... >> data\watchdog.log`r`n    net start cloudflared > NUL 2>&1`r`n)"
[System.IO.File]::WriteAllText($WatchdogBat, $WatchdogText)
Write-Host "  watchdog_check.bat generated" -ForegroundColor Gray

$WdSettings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 2) `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

$WdAction = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$ScriptDir\scripts\watchdog_check.bat`"" `
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
Write-Host "  logon  DAI_Keiba_Dashboard        - ダッシュボード常駐 (自動再起動)" -ForegroundColor White
Write-Host "  5min   DAI_Keiba_Watchdog         - dashboard+cloudflared watchdog" -ForegroundColor White
