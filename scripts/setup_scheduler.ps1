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
    -Execute "wscript.exe" `
    -Argument "`"$ScriptDir\scripts\daily_predict_hidden.vbs`"" `
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
    -Execute "wscript.exe" `
    -Argument "`"$ScriptDir\scripts\daily_results_hidden.vbs`"" `
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
    -Execute "wscript.exe" `
    -Argument "`"$ScriptDir\scripts\daily_maintenance_hidden.vbs`"" `
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
    -Execute "wscript.exe" `
    -Argument "`"$ScriptDir\scripts\daily_predict_tomorrow_hidden.vbs`"" `
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

# ── 6. Watchdog (every 5 min: dashboard + cloudflared) ──
# watchdog_check.bat は scripts/ に直接管理。動的生成しない

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

# ── 7. 発走 15 分前 オッズ+馬体重自動取得（09:30〜21:00 の間 5 分間隔）──
# 09:30 開始、Duration=11.5h(41400s)、RepetitionInterval=5min
# MultipleInstances=IgnoreNew でスクリプト重複起動を防止
# ※ Windows Task Scheduler は「UTC で実行」オプションを使わない（スクリプト内部が JST 前提）
$AutoOddsSettings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 2) `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

$AutoOddsAction = New-ScheduledTaskAction `
    -Execute "wscript.exe" `
    -Argument "`"$ScriptDir\scripts\auto_fetch_odds_15min_hidden.vbs`"" `
    -WorkingDirectory $ScriptDir

# 09:30 を開始点とし、毎日 11.5 時間（09:30〜21:00）繰り返す（PS 5.1/7 両対応）
$AutoOddsTrigger = New-ScheduledTaskTrigger -Daily -At "09:30"
$RepetitionTemplate = New-ScheduledTaskTrigger -Once -At "09:30" `
    -RepetitionInterval (New-TimeSpan -Minutes 5) `
    -RepetitionDuration (New-TimeSpan -Hours 11 -Minutes 30)
$AutoOddsTrigger.Repetition = $RepetitionTemplate.Repetition

Register-ScheduledTask `
    -TaskName "DAI_Keiba_AutoOdds" `
    -Description "D-AI Keiba: 発走15分前オッズ+馬体重自動取得 09:30〜21:00 の間 5分間隔" `
    -Action $AutoOddsAction `
    -Trigger $AutoOddsTrigger `
    -Settings $AutoOddsSettings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "OK: DAI_Keiba_AutoOdds (09:30-21:00 every 5 min)" -ForegroundColor Green

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
Write-Host "  5min   DAI_Keiba_AutoOdds         - 発走15分前オッズ+馬体重 (09:30-21:00)" -ForegroundColor White
