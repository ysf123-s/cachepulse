# 把 config/nacos/*.yaml 发布到 Nacos 的 cachepulse 命名空间（已存在则覆盖）。用 Nacos 3 的 Admin API。
# 用法：pwsh scripts/local/nacos-publish.ps1
. (Join-Path $PSScriptRoot 'common.ps1')
Import-LocalEnv
Require-Env @('NACOS_ADDR', 'NACOS_USERNAME', 'NACOS_PASSWORD')

$base = "http://$env:NACOS_ADDR/nacos"
$namespace = if ($env:NACOS_NAMESPACE) { $env:NACOS_NAMESPACE } else { 'cachepulse' }

$login = Invoke-RestMethod -Method Post "$base/v3/auth/user/login" -Body @{ username = $env:NACOS_USERNAME; password = $env:NACOS_PASSWORD }
$headers = @{ accessToken = $login.accessToken }

$existing = $null
try { $existing = Invoke-RestMethod -Headers $headers "$base/v3/admin/core/namespace?namespaceId=$namespace" } catch { }
if (-not $existing -or $existing.code -ne 0) {
    $created = Invoke-RestMethod -Method Post -Headers $headers "$base/v3/admin/core/namespace" -Body @{
        namespaceId = $namespace; namespaceName = $namespace; namespaceDesc = 'CachePulse microservices'
    }
    if ($created.code -ne 0) { throw "创建命名空间失败：$($created.message)" }
    Write-Host "已创建命名空间 $namespace"
}

foreach ($file in Get-ChildItem (Join-Path $Root 'config\nacos') -Filter *.yaml) {
    $result = Invoke-RestMethod -Method Post -Headers $headers "$base/v3/admin/cs/config" -Body @{
        namespaceId = $namespace; groupName = 'DEFAULT_GROUP'; dataId = $file.Name; type = 'yaml'
        content = (Get-Content $file.FullName -Raw -Encoding utf8)
    }
    if ($result.code -ne 0 -or -not $result.data) { throw "发布 $($file.Name) 失败：$($result.message)" }
    Write-Host "已发布 $($file.Name)"
}
