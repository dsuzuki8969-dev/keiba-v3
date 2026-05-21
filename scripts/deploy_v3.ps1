# D-AI競馬 v3 一括デプロイスクリプト
# 管理者権限で実行: powershell -ExecutionPolicy Bypass -File scripts\deploy_v3.ps1
#
# 実行内容:
#   1. 管理者権限チェック
#   2. 旧スケジューラ停止 (PID ファイル + プロセス名検索)
#   3. 旧 WTS タスク削除 + 新 WTS タスク登録 (setup_scheduler.ps1 相当)
#   4. 新スケジューラ起動
#   5. 動作検証 (scheduler.py --status)

$ErrorActionPreference = "Stop"
$ScriptDir = "c:\Users\dsuzu\keiba\keiba-v3"
$PidFile = "$ScriptDir\data\scheduler.pid"
$LogFile = "$ScriptDir\data\logs\deploy_v3.log"

function Write-Log {
    param([string]$Msg, [string]$Color = "White")
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] $Msg"
    Write-Host $line -ForegroundColor $Color
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  D-AI Keiba v3 一括デプロイ" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# ── Step 1: 管理者権限チェック ──────────────────────────────
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "[ERROR] 管理者権限が必要です。右クリック→「管理者として実行」で起動してください。" -ForegroundColor Red
    Write-Host ""
    Write-Host "自動昇格を試みます..." -ForegroundColor Yellow
    Start-Process powershell.exe -ArgumentList "-ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}
Write-Log "Step 1/5: 管理者権限 OK" "Green"

# ── Step 2: 旧スケジューラ停止 ─────────────────────────────
Write-Log "Step 2/5: 旧スケジューラ停止..." "Yellow"

$killed = $false

# 2a: PID ファイルから停止
if (Test-Path $PidFile) {
    $oldPid = [int](Get-Content $PidFile -Raw).Trim()
    Write-Log "  PID ファイル検出: $oldPid" "Gray"
    try {
        $proc = Get-Process -Id $oldPid -ErrorAction Stop
        Stop-Process -Id $oldPid -Force
        Start-Sleep -Seconds 2
        Write-Log "  PID $oldPid を停止しました" "Green"
        $killed = $true
    } catch {
        Write-Log "  PID $oldPid は既に停止済み" "Gray"
    }
}

# 2b: プロセス名で残存チェック (scheduler.py を含む python プロセス)
$remaining = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "scheduler\.py" }

foreach ($p in $remaining) {
    Write-Log "  残存プロセス検出: PID=$($p.ProcessId) CMD=$($p.CommandLine)" "Yellow"
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    $killed = $true
    Write-Log "  PID $($p.ProcessId) を強制停止" "Green"
}

if (-not $killed) {
    Write-Log "  旧スケジューラは動作していません (スキップ)" "Gray"
}

# PID ファイル削除
if (Test-Path $PidFile) {
    Remove-Item $PidFile -Force
    Write-Log "  PID ファイル削除" "Gray"
}

Start-Sleep -Seconds 1

# ── Step 3: WTS タスク再登録 ───────────────────────────────
Write-Log "Step 3/5: WTS タスク再登録..." "Yellow"

# 3a: 移行済みタスク削除
$MigratedTasks = @(
    "keiba-scheduler",
    "DAI_Keiba_Predict",
    "DAI_Keiba_Results",
    "DAI_Keiba_Maintenance",
    "DAI_Keiba_Predict_Tomorrow",
    "DAI_Keiba_AutoOdds",
    "DAI_Keiba_Paraphrase_Today",
    "DAI_Keiba_Paraphrase_Tomorrow"
)

foreach ($task in $MigratedTasks) {
    $exists = Get-ScheduledTask -TaskName $task -ErrorAction SilentlyContinue
    if ($exists) {
        Unregister-ScheduledTask -TaskName $task -Confirm:$false
        Write-Log "  削除: $task" "Gray"
    }
}

# 3b: Dashboard (ログオン時起動・自動再起動)
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

Write-Log "  登録: DAI_Keiba_Dashboard (logon, auto-restart)" "Green"

# 3c: Watchdog (5分おき)
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

Write-Log "  登録: DAI_Keiba_Watchdog (every 5min)" "Green"

# 3d: APScheduler (ログオン時起動・自動再起動)
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

Write-Log "  登録: DAI_Keiba_Scheduler (logon, auto-restart, 12 jobs)" "Green"

# 登録結果表示
Write-Host ""
Get-ScheduledTask -TaskName "DAI_Keiba_*" | Format-Table TaskName, State -AutoSize

# ── Step 4: 新スケジューラ起動 ─────────────────────────────
Write-Log "Step 4/5: 新スケジューラ起動..." "Yellow"

Start-ScheduledTask -TaskName "DAI_Keiba_Scheduler"
Start-Sleep -Seconds 5

# 起動確認
$running = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "scheduler\.py" }

if ($running) {
    Write-Log "  スケジューラ起動確認: PID=$($running.ProcessId)" "Green"
} else {
    Write-Log "  [WARN] プロセス検出できず。数秒後に再確認してください" "Yellow"
}

# ── Step 5: 動作検証 ──────────────────────────────────────
Write-Log "Step 5/5: 動作検証..." "Yellow"
Write-Host ""

Set-Location $ScriptDir
$ErrorActionPreference = "Continue"
& python scheduler.py --status 2>&1 | Tee-Object -Variable statusOutput
$ErrorActionPreference = "Stop"

# ログにも保存
$statusOutput | Add-Content -Path $LogFile -Encoding UTF8

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  デプロイ完了" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "ログ: $LogFile" -ForegroundColor Gray
Write-Host ""