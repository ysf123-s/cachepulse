# 本机一键停止：按启动的相反顺序停掉 start.ps1 记录的进程（网关先停，Nacos 最后）。
# 用法：pwsh scripts/local/stop.ps1 [-KeepNacos]
param([switch] $KeepNacos)
. (Join-Path $PSScriptRoot 'common.ps1')
$pids = Read-Pids
$order = @($Services.Name)
[array]::Reverse($order)
if (-not $KeepNacos) { $order += 'nacos' }

foreach ($name in $order) {
    if (-not $pids.ContainsKey($name)) { continue }
    $process = Get-Process -Id $pids[$name] -ErrorAction SilentlyContinue
    # 只停 start.ps1 启动的 java 进程；进程号已被别的程序复用时跳过
    if ($process -and $process.ProcessName -eq 'java') {
        Stop-Process -Id $process.Id
        Write-Host "已停止 $name（进程 $($process.Id)）"
    }
    $pids.Remove($name)
}
Write-Pids $pids
