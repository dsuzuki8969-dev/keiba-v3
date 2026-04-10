$ErrorActionPreference = "Stop"
Write-Host "cloudflared service fix" -ForegroundColor Cyan

$UserDir = "C:\Users\dsuzu\.cloudflared"
$SysDir = "C:\Windows\System32\config\systemprofile\.cloudflared"
$TunnelId = "91a05bc2-dbd9-4ede-8496-c15ad8c461d2"

$CfPath = (Get-ChildItem "C:\Users\dsuzu\AppData\Local\Microsoft\WinGet\Packages\Cloudflare*\cloudflared.exe" | Select-Object -First 1).FullName
Write-Host "  exe: $CfPath"

New-Item -ItemType Directory -Force -Path $SysDir | Out-Null
Copy-Item "$UserDir\$TunnelId.json" "$SysDir\$TunnelId.json" -Force
Write-Host "  credentials copied"

$cfg = "tunnel: $TunnelId`ncredentials-file: $SysDir\$TunnelId.json`nloglevel: info`n`ningress:`n  - hostname: d-aikeiba.com`n    service: http://127.0.0.1:5051`n  - hostname: www.d-aikeiba.com`n    service: http://127.0.0.1:5051`n  - hostname: dash.d-aikeiba.com`n    service: http://127.0.0.1:5051`n  - service: http_status:404"
[System.IO.File]::WriteAllText("$SysDir\config.yml", $cfg)
Write-Host "  config.yml written (loglevel: info, 127.0.0.1 明示)"

try { & $CfPath service uninstall 2>&1 | Out-Null } catch { }
Start-Sleep -Seconds 2
& $CfPath service install
Start-Sleep -Seconds 3

# 自動再起動ポリシー設定（1分後に再起動、最大3回）
Write-Host "  自動再起動ポリシーを設定中..."
sc.exe failure cloudflared reset= 3600 actions= restart/60000/restart/60000/restart/60000 | Out-Null

try {
    Start-Service cloudflared
    Start-Sleep -Seconds 5
    $s = (Get-Service cloudflared).Status
    Write-Host "  service: $s" -ForegroundColor Green
} catch {
    Write-Host "  Start-Service failed, trying direct run..." -ForegroundColor Yellow
    # サービスとして起動できない場合、直接プロセスとして起動
    Start-Process -FilePath $CfPath -ArgumentList "tunnel","run" -WindowStyle Hidden
    Start-Sleep -Seconds 3
    Write-Host "  cloudflared started as process" -ForegroundColor Green
}
try { & $CfPath tunnel info keiba-dash 2>&1 } catch { }
