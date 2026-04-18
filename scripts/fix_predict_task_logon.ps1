# DAI_Keiba_Predict 06:00 タスクの未実行問題対応
# 原因: タスクのPrincipal.LogonTypeがInteractive（ユーザーがアクティブにログオン中のみ実行）
#        06:00はPCが省電力/スリープ状態、もしくは画面ロック中で実行が失敗する
#
# 解決: LogonType=Passwordに変更し、ユーザーがログオンしていなくても実行できるようにする
#       ※ 登録時にログインパスワードの入力が必要
#
# 使い方: 管理者権限 PowerShell で実行
#   powershell -ExecutionPolicy Bypass -File scripts\fix_predict_task_logon.ps1

$ErrorActionPreference = "Stop"
$UserId = "$env:USERDOMAIN\$env:USERNAME"

Write-Host "DAI_Keiba タスクを「ログオン時以外も実行」に変更します" -ForegroundColor Cyan
Write-Host "対象ユーザー: $UserId" -ForegroundColor Gray
Write-Host ""

# Principal を作成（-RunLevel Highest + -LogonType Password）
# Password は登録時に別ダイアログで要求される
$Principal = New-ScheduledTaskPrincipal -UserId $UserId -LogonType Password -RunLevel Highest

# 対象タスク
$Tasks = @(
    "DAI_Keiba_Predict",
    "DAI_Keiba_Predict_Tomorrow",
    "DAI_Keiba_Results",
    "DAI_Keiba_Maintenance"
)

foreach ($TaskName in $Tasks) {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $Task) {
        Write-Host "SKIP: $TaskName（未登録）" -ForegroundColor Yellow
        continue
    }
    Write-Host "UPDATE: $TaskName を Password ログオンタイプに変更中..." -ForegroundColor Green
    try {
        Set-ScheduledTask -TaskName $TaskName -Principal $Principal -ErrorAction Stop | Out-Null
        Write-Host "  OK" -ForegroundColor Green
    }
    catch {
        Write-Host "  エラー: $_" -ForegroundColor Red
        Write-Host "  (管理者権限が必要、またはパスワード保存を許可する必要があります)" -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "完了後、各タスクのプロパティで「ユーザーがログオンしているかどうかにかかわらず実行する」が有効になっていることを確認してください" -ForegroundColor Cyan
