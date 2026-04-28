# D-AI Keiba 持ち越し B: 2023 年下半期 race_log バックフィル 再開スクリプト
# 「B を再開して」コマンド時に呼ばれる
#
# 二重起動防止: PID ファイル方式 (data/backfill_b.pid)
#   - 起動時に PID 書き込み、終了時 (or 異常時) に削除
#   - PID ファイルあり + 該当 PID 生存 → SKIP
#   - PID ファイルなし or 死亡プロセス → 新規起動 + PID 書き込み
# detach 起動: Start-Process -WindowStyle Hidden で親 exit 後も生存
# PowerShell 7.x (pwsh) / 5.1 両対応

$ErrorActionPreference = "Stop"
Set-Location "C:\Users\dsuzu\keiba\keiba-v3"
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED = "1"

$RestartLog = "log\backfill_b_restart.log"
$PidFile    = "data\backfill_b.pid"

function Write-Log($msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Add-Content -Path $RestartLog -Value $line -Encoding UTF8
    Write-Host $line
}

# ──────────────────────────────────────────
# 既存 backfill 稼働チェック (PID ファイル方式)
# ──────────────────────────────────────────
if (Test-Path $PidFile) {
    $existingPid = (Get-Content $PidFile -ErrorAction SilentlyContinue).Trim()
    if ($existingPid -match '^\d+$') {
        $proc = Get-Process -Id ([int]$existingPid) -ErrorAction SilentlyContinue
        if ($proc -and $proc.ProcessName -eq "python") {
            Write-Log "SKIP: backfill 既に稼働中 PID=$existingPid"
            exit 0
        } else {
            Write-Log "INFO: PID ファイル古い PID=$existingPid (該当プロセスなし)、削除して再起動"
            Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        }
    }
}

# ──────────────────────────────────────────
# 起動
# ──────────────────────────────────────────
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$logPath = "log\backfill_2023h2_$ts.log"
$errPath = "log\backfill_2023h2_$ts.err"

$proc = Start-Process -FilePath "python" `
    -ArgumentList "scripts\backfill_race_log_2023h2.py","--apply" `
    -WindowStyle Hidden `
    -RedirectStandardOutput $logPath `
    -RedirectStandardError $errPath `
    -PassThru

# PID ファイル書き込み
$proc.Id | Out-File -FilePath $PidFile -Encoding ASCII -NoNewline

# 起動確認 (3 秒待機)
Start-Sleep -Seconds 3
$alive = Get-Process -Id $proc.Id -ErrorAction SilentlyContinue
if ($alive) {
    Write-Log "STARTED PID=$($proc.Id) log=$logPath"
} else {
    Write-Log "WARN: 起動確認できず (3秒以内に exit) PID=$($proc.Id)"
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    exit 1
}
