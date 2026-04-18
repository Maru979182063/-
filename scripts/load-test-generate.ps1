param(
    [string]$BaseUrl = "http://127.0.0.1:8111",
    [ValidateSet("standard", "forced_user_material")]
    [string]$Preset = "forced_user_material",
    [int]$Requests = 10,
    [int]$Concurrency = 5,
    [double]$TimeoutSeconds = 300,
    [string]$PayloadFile = "",
    [string]$OutputJson = "",
    [string]$AuthToken = "",
    [int]$Warmup = 1,
    [switch]$AllowUnready
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
$script = Join-Path $root "scripts\load_test_generate.py"

if (-not (Test-Path $python)) {
    throw "Python runtime not found at $python"
}

$arguments = @(
    $script,
    "--base-url", $BaseUrl,
    "--preset", $Preset,
    "--requests", "$Requests",
    "--concurrency", "$Concurrency",
    "--timeout-seconds", "$TimeoutSeconds",
    "--warmup", "$Warmup"
)

if ($PayloadFile) {
    $arguments += @("--payload-file", $PayloadFile)
}
if ($OutputJson) {
    $arguments += @("--output-json", $OutputJson)
}
if ($AuthToken) {
    $arguments += @("--auth-token", $AuthToken)
}
if ($AllowUnready) {
    $arguments += "--allow-unready"
}

& $python @arguments
