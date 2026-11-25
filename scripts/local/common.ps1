# 本机启停脚本的公共部分：读取 cachepulse-local.env，服务列表和端口。
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$RunDir = Join-Path $Root 'logs\local'
$PidFile = Join-Path $RunDir 'pids.json'

# 启动顺序：同步、归档、投递先起（消费者），接入方其次，网关最后
$Services = @(
    @{ Name = 'cache-sync-service';     Port = 18091; Heap = '256m' },
    @{ Name = 'archive-service';        Port = 18092; Heap = '192m' },
    @{ Name = 'delivery-service';       Port = 18093; Heap = '192m' },
    @{ Name = 'demo-user-service';      Port = 18094; Heap = '256m' },
    @{ Name = 'demo-inventory-service'; Port = 18095; Heap = '192m' },
    @{ Name = 'gateway';                Port = 18090; Heap = '192m' }
)

function Import-LocalEnv {
    $file = Join-Path $Root 'cachepulse-local.env'
    if (-not (Test-Path $file)) {
        throw "缺少 $file：复制 cachepulse-local.env.example 后填写"
    }
    foreach ($line in Get-Content $file -Encoding utf8) {
        $text = $line.Trim()
        if ($text -eq '' -or $text.StartsWith('#')) { continue }
        $index = $text.IndexOf('=')
        if ($index -lt 1) { continue }
        [Environment]::SetEnvironmentVariable($text.Substring(0, $index).Trim(), $text.Substring($index + 1).Trim(), 'Process')
    }
}

function Require-Env([string[]] $Names) {
    $missing = $Names | Where-Object { -not [Environment]::GetEnvironmentVariable($_) }
    if ($missing) { throw "cachepulse-local.env 缺少：$($missing -join ', ')" }
}

function Test-Port([int] $Port) {
    $client = [Net.Sockets.TcpClient]::new()
    try { $client.ConnectAsync('127.0.0.1', $Port).Wait(300) -and $client.Connected } catch { $false } finally { $client.Dispose() }
}

function Read-Pids {
    if (Test-Path $PidFile) { Get-Content $PidFile -Raw | ConvertFrom-Json -AsHashtable } else { @{} }
}

function Write-Pids([hashtable] $Pids) {
    New-Item -ItemType Directory -Force $RunDir | Out-Null
    $Pids | ConvertTo-Json | Set-Content $PidFile -Encoding utf8
}
