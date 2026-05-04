# DAI_Keiba_Predict 06:00 タスクの未実行問題対応
# 原因: タスクのPrincipal.LogonTypeがInteractive（ユーザーがアクティブにログオン中のみ実行）
#        06:00はPCが省電力/スリープ状態、もしくは画面ロック中で実行が失敗する
#
# 解決: schtasks /change /rp 経路で「Run whether user is logged on or not」を有効化
#       Set-ScheduledTask -Principal 経路はパスワード保存ができないため schtasks ネイティブを使用
#
# 使い方: 管理者権限 PowerShell で実行
#   .\scripts\fix_predict_task_logon.ps1
#
# 動作:
#   1. 起動時に Get-Credential で Windows ログオンパスワードを 1 回だけ入力
#   2. schtasks /change /tn <task> /ru <user> /rp <password> で資格情報を保存
#   3. 4 タスク全てに同じ資格情報を適用

$ErrorActionPreference = "Stop"
$UserId = "$env:USERDOMAIN\$env:USERNAME"

Write-Host "DAI_Keiba タスクを「ログオン時以外も実行」に変更します" -ForegroundColor Cyan
Write-Host "対象ユーザー: $UserId" -ForegroundColor Gray
Write-Host ""
Write-Host "Windows ログオンパスワードを入力してください" -ForegroundColor Yellow
Write-Host "(普段 PC ログオン時に入力しているパスワード。1 回だけ入力すれば 4 タスクに適用されます)" -ForegroundColor Gray

# 資格情報を 1 回だけ取得
$Cred = Get-Credential -UserName $UserId -Message "Windows ログオンパスワードを入力してください"
if (-not $Cred) {
    Write-Host "中断: パスワード入力がキャンセルされました" -ForegroundColor Red
    exit 1
}
$PlainPassword = $Cred.GetNetworkCredential().Password

# 対象タスク
$Tasks = @(
    "DAI_Keiba_Predict",
    "DAI_Keiba_Predict_Tomorrow",
    "DAI_Keiba_Results",
    "DAI_Keiba_Maintenance"
)

$SuccessCount = 0
$FailCount = 0

foreach ($TaskName in $Tasks) {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $Task) {
        Write-Host "SKIP: $TaskName (未登録)" -ForegroundColor Yellow
        continue
    }
    Write-Host "UPDATE: $TaskName を Password ログオンタイプに変更中..." -ForegroundColor Green

    # schtasks /change /ru /rp で資格情報を保存 (Set-ScheduledTask では不可)
    $output = & schtasks /change /tn $TaskName /ru $UserId /rp $PlainPassword 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  OK" -ForegroundColor Green
        $SuccessCount++
    } else {
        Write-Host "  エラー (exit=$LASTEXITCODE):" -ForegroundColor Red
        Write-Host "  $output" -ForegroundColor Red
        $FailCount++
    }
}

Write-Host ""
Write-Host "完了: 成功 $SuccessCount 件 / 失敗 $FailCount 件" -ForegroundColor Cyan
Write-Host ""
Write-Host "確認方法:" -ForegroundColor Cyan
Write-Host "  Get-ScheduledTask -TaskName 'DAI_Keiba_Predict' | Select-Object -ExpandProperty Principal" -ForegroundColor Gray
Write-Host "  → LogonType が Password になっていれば成功" -ForegroundColor Gray

# パスワード変数をクリア (メモリ残留を最小化)
$PlainPassword = $null
$Cred = $null
[System.GC]::Collect()
