$ErrorActionPreference = "Stop"

function Get-RequiredEnv {
    param([string]$Name)
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Missing required environment variable: $Name"
    }
    return $value.Trim()
}

$envId = Get-RequiredEnv "TCB_ENV_ID"
$functionUrl = Get-RequiredEnv "TCB_FUNCTION_URL"
$publishableKey = Get-RequiredEnv "TCB_PUBLISHABLE_KEY"

$outputPath = Join-Path $PSScriptRoot "..\\cloudrun\\runtime-config.js"
$content = @"
window.__QUANT_TRADE_CONFIG__ = Object.freeze({
  TCB_ENV_ID: "$envId",
  TCB_FUNCTION_URL: "$functionUrl",
  TCB_PUBLISHABLE_KEY: "$publishableKey",
});
"@

Set-Content -LiteralPath $outputPath -Value $content -Encoding UTF8
Write-Output "Generated runtime config: $outputPath"
