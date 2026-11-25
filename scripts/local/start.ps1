# 本机一键启动：Nacos（可选）→ 发布配置 → 6 个服务。日志和进程号在 logs/local/。
# 用法：pwsh scripts/local/start.ps1 [-Build] [-Only gateway,demo-user-service]
#   -Build  先执行 mvn -o -q -DskipTests package
#   -Only   只启动列出的服务
# 前提：已复制并填写 cachepulse-local.env；中间件可达（经 SSH 隧道访问 Instabox 时先建隧道，见 AGENTS.md）。
param([switch] $Build, [string[]] $Only)
. (Join-Path $PSScriptRoot 'common.ps1')
Import-LocalEnv
Require-Env @('MYSQL_PASSWORD', 'NACOS_PASSWORD', 'CACHEPULSE_INTERNAL_TOKEN', 'GATEWAY_ADMIN_PASSWORD_HASH', 'GATEWAY_JWT_SECRET')
New-Item -ItemType Directory -Force $RunDir | Out-Null
$env:LOG_DIR = $RunDir
$pids = Read-Pids

if ($Build) {
    Push-Location $Root
    try { mvn -o -q -B -DskipTests package -pl ($Services.Name -join ',') -am; if ($LASTEXITCODE) { throw 'mvn package 失败' } }
    finally { Pop-Location }
}

# --- Nacos ---
$nacosPort = [int]($env:NACOS_ADDR -split ':')[1]
if (-not (Test-Port $nacosPort)) {
    if (-not $env:NACOS_HOME) { throw "Nacos 没有在 $env:NACOS_ADDR 运行，且未设置 NACOS_HOME" }
    Require-Env @('NACOS_TOKEN_SECRET', 'NACOS_IDENTITY_KEY', 'NACOS_IDENTITY_VALUE')
    $jar = Join-Path $env:NACOS_HOME 'target\nacos-server.jar'
    # 参数照 bin/startup.cmd；PowerShell 里每个 -D 参数都要加引号
    $nacosArgs = @(
        '-Xms512m', '-Xmx512m', '-Dnacos.standalone=true', "-Dnacos.home=$env:NACOS_HOME",
        "-Dnacos.server.main.port=$nacosPort", '-Dnacos.console.port=18850',
        '-Dnacos.core.auth.enabled=true', '-Dnacos.core.auth.console.enabled=true', '-Dnacos.core.auth.admin.enabled=true',
        "-Dnacos.core.auth.plugin.nacos.token.secret.key=$env:NACOS_TOKEN_SECRET",
        "-Dnacos.core.auth.server.identity.key=$env:NACOS_IDENTITY_KEY",
        "-Dnacos.core.auth.server.identity.value=$env:NACOS_IDENTITY_VALUE",
        '-jar', $jar, ('--spring.config.additional-location=file:' + (Join-Path $env:NACOS_HOME 'conf').Replace('\', '/') + '/')
    )
    $process = Start-Process java -ArgumentList $nacosArgs -WorkingDirectory $env:NACOS_HOME -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $RunDir 'nacos.out') -RedirectStandardError (Join-Path $RunDir 'nacos.err')
    $pids['nacos'] = $process.Id
    Write-Pids $pids
    Write-Host "Nacos 启动中（进程 $($process.Id)），约 30 秒"
    $deadline = (Get-Date).AddSeconds(90)
    while (-not (Test-Port $nacosPort)) {
        if ((Get-Date) -gt $deadline) { throw "Nacos 90 秒内没有启动，见 $RunDir\nacos.out" }
        Start-Sleep -Seconds 2
    }
    # 首次启动初始化管理员密码；已初始化时返回错误，忽略
    try {
        Invoke-RestMethod -Method Post "http://$env:NACOS_ADDR/nacos/v3/auth/user/admin" -Body @{ password = $env:NACOS_PASSWORD } | Out-Null
        Write-Host '已初始化 Nacos 管理员密码'
    } catch { }
}

& (Join-Path $PSScriptRoot 'nacos-publish.ps1')

# --- 服务 ---
foreach ($service in $Services) {
    if ($Only -and $Only -notcontains $service.Name) { continue }
    if (Test-Port $service.Port) { Write-Host "$($service.Name) 已在端口 $($service.Port) 运行，跳过"; continue }
    $jar = Join-Path $Root "$($service.Name)\target\$($service.Name)-0.1.0-SNAPSHOT.jar"
    if (-not (Test-Path $jar)) { throw "找不到 $jar，先加 -Build" }
    $javaArgs = @("-Xms$($service.Heap)", "-Xmx$($service.Heap)", '-XX:+UseSerialGC', '-jar', $jar)
    $process = Start-Process java -ArgumentList $javaArgs -WorkingDirectory $Root -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $RunDir "$($service.Name).out") -RedirectStandardError (Join-Path $RunDir "$($service.Name).err")
    $pids[$service.Name] = $process.Id
    Write-Pids $pids
    Write-Host "$($service.Name) 启动中（进程 $($process.Id)，端口 $($service.Port)）"
}

Write-Host "全部已提交启动。就绪检查：http://127.0.0.1:<端口>/actuator/health/readiness；管理页面：http://127.0.0.1:18090/"
