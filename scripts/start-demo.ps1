param(
    [ValidateSet("default", "mvp", "dev", "uat")]
    [string]$Profile = "default",
    [int]$PromptPort = 0,
    [int]$PassagePort = 0,
    [switch]$StartNgrok,
    [switch]$Reload,
    [string]$NgrokPath = "ngrok",
    [string]$NgrokAuthtoken = "",
    [string]$PromptEnvFile = "",
    [string]$PassageEnvFile = ""
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$profileName = if ($Profile -eq "uat") { "mvp" } else { $Profile }
$profileDefaults = @{
    "default" = @{
        PromptPort = 8000
        PassagePort = 8001
        PromptEnvFiles = @(
            (Join-Path $root ".env.demo")
        )
        PassageEnvFiles = @(
            (Join-Path $root "passage_service\.env")
        )
    }
    "mvp" = @{
        PromptPort = 8011
        PassagePort = 8001
        PromptEnvFiles = @(
            (Join-Path $root ".env.demo"),
            (Join-Path $root ".env.mvp")
        )
        PassageEnvFiles = @(
            (Join-Path $root ".env.demo"),
            (Join-Path $root "passage_service\.env.mvp")
        )
    }
    "dev" = @{
        PromptPort = 8111
        PassagePort = 8101
        PromptEnvFiles = @(
            (Join-Path $root ".env.demo"),
            (Join-Path $root ".env.dev")
        )
        PassageEnvFiles = @(
            (Join-Path $root ".env.demo"),
            (Join-Path $root "passage_service\.env.dev")
        )
    }
}

if (-not $profileDefaults.ContainsKey($profileName)) {
    throw "Unsupported profile: $Profile"
}

$activeProfile = $profileDefaults[$profileName]
if ($PromptPort -le 0) {
    $PromptPort = [int]$activeProfile.PromptPort
}
if ($PassagePort -le 0) {
    $PassagePort = [int]$activeProfile.PassagePort
}

$promptPython = Join-Path $root ".venv\Scripts\python.exe"
$passagePython = Join-Path $root "passage_service\.venv\Scripts\python.exe"

function Wait-HttpReady {
    param(
        [string]$Url,
        [int]$TimeoutSeconds = 60,
        [string]$Name = "service"
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                Write-Host "$Name is ready: $Url" -ForegroundColor Green
                return
            }
        } catch {
            Start-Sleep -Milliseconds 800
        }
    } while ((Get-Date) -lt $deadline)

    throw "$Name did not become ready within $TimeoutSeconds seconds: $Url"
}

function Assert-PortAvailable {
    param(
        [int]$Port,
        [string]$Name
    )

    $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $listener) {
        return
    }

    $processSummary = "unknown process"
    $processInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $($listener.OwningProcess)" -ErrorAction SilentlyContinue
    if ($processInfo) {
        $processSummary = if ($processInfo.CommandLine) { $processInfo.CommandLine } else { $processInfo.Name }
    }

    throw "$Name cannot start because port $Port is already in use by PID $($listener.OwningProcess): $processSummary"
}

function Start-UvicornService {
    param(
        [string]$PythonPath,
        [string]$WorkingDirectory,
        [int]$Port,
        [hashtable]$EnvMap
    )

    $arguments = @(
        "-m",
        "uvicorn",
        "app.main:app",
        "--app-dir",
        $WorkingDirectory,
        "--host",
        "127.0.0.1",
        "--port",
        "$Port"
    )
    if ($Reload) {
        $arguments += "--reload"
    }

    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = $PythonPath
    $psi.WorkingDirectory = $WorkingDirectory
    $psi.UseShellExecute = $false
    $quotedArgs = foreach ($argument in $arguments) {
        if ($argument -match '\s') {
            '"' + ($argument -replace '"', '\"') + '"'
        } else {
            $argument
        }
    }
    $psi.Arguments = [string]::Join(' ', $quotedArgs)

    foreach ($entry in Get-ChildItem Env:) {
        $psi.Environment[$entry.Name] = $entry.Value
    }

    foreach ($name in @(
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "PASSAGE_OPENAI_API_KEY",
        "PASSAGE_OPENAI_BASE_URL",
        "PASSAGE_DATABASE_URL",
        "PASSAGE_ALLOW_NON_PRIMARY_DATABASE",
        "PASSAGE_DISABLE_SCHEDULER",
        "PROMPT_SERVICE_SECURITY_ENABLED",
        "PROMPT_SERVICE_API_TOKEN",
        "PROMPT_SERVICE_RATE_LIMIT_PER_MINUTE",
        "PROMPT_RUNTIME_CONFIG_PATH",
        "PROMPT_TEMPLATE_CONFIG_PATH",
        "PROMPT_DATA_DIR",
        "PROMPT_QUESTION_DB_PATH"
    )) {
        if ($psi.Environment.ContainsKey($name)) {
            [void]$psi.Environment.Remove($name)
        }
    }

    if ($EnvMap) {
        foreach ($key in $EnvMap.Keys) {
            $psi.Environment[$key] = [string]$EnvMap[$key]
        }
    }

    [System.Diagnostics.Process]::Start($psi) | Out-Null
}

function Read-DemoEnvFile {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        Write-Host "Env file not found at $Path, skipping dotenv load." -ForegroundColor DarkYellow
        return @{}
    }

    Write-Host "Loading env vars from $Path" -ForegroundColor DarkGray
    $envMap = @{}
    foreach ($rawLine in Get-Content $Path) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            continue
        }

        $parts = $line.Split("=", 2)
        if ($parts.Count -ne 2) {
            continue
        }

        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        if ($name) {
            $envMap[$name] = $value
        }
    }
    return $envMap
}

function Resolve-EnvFileList {
    param(
        [string]$ExplicitPath,
        [object[]]$DefaultPaths
    )

    if ($ExplicitPath) {
        return @($ExplicitPath)
    }
    return @($DefaultPaths | Where-Object { $_ })
}

function Read-DemoEnvFiles {
    param([string[]]$Paths)

    $merged = @{}
    $loadedPaths = @()
    foreach ($path in $Paths) {
        if (-not $path) {
            continue
        }
        if (-not (Test-Path $path)) {
            Write-Host "Env file not found at $path, skipping dotenv load." -ForegroundColor DarkYellow
            continue
        }
        $loadedPaths += $path
        $envMap = Read-DemoEnvFile -Path $path
        foreach ($key in $envMap.Keys) {
            $merged[$key] = $envMap[$key]
        }
    }
    return @{
        Map = $merged
        LoadedPaths = $loadedPaths
    }
}

$promptEnvFiles = Resolve-EnvFileList -ExplicitPath $PromptEnvFile -DefaultPaths $activeProfile.PromptEnvFiles
$passageEnvFiles = Resolve-EnvFileList -ExplicitPath $PassageEnvFile -DefaultPaths $activeProfile.PassageEnvFiles
$promptEnvBundle = Read-DemoEnvFiles -Paths $promptEnvFiles
$passageEnvBundle = Read-DemoEnvFiles -Paths $passageEnvFiles
$promptEnv = $promptEnvBundle.Map
$passageEnv = $passageEnvBundle.Map

if (-not (Test-Path $promptPython)) {
    throw "Prompt service Python runtime not found at $promptPython"
}

if (-not (Test-Path $passagePython)) {
    $passagePython = $promptPython
}

$promptDir = Join-Path $root "prompt_skeleton_service"
$passageDir = Join-Path $root "passage_service"
Write-Host "Profile: $profileName" -ForegroundColor DarkCyan
Assert-PortAvailable -Port $PassagePort -Name "passage_service"
Write-Host "Starting passage_service on http://127.0.0.1:$PassagePort" -ForegroundColor Cyan
Start-UvicornService -PythonPath $passagePython -WorkingDirectory $passageDir -Port $PassagePort -EnvMap $passageEnv
Wait-HttpReady -Url "http://127.0.0.1:$PassagePort/readyz" -TimeoutSeconds 90 -Name "passage_service"

Assert-PortAvailable -Port $PromptPort -Name "prompt_skeleton_service"
Write-Host "Starting prompt_skeleton_service on http://127.0.0.1:$PromptPort" -ForegroundColor Cyan
Start-UvicornService -PythonPath $promptPython -WorkingDirectory $promptDir -Port $PromptPort -EnvMap $promptEnv
Wait-HttpReady -Url "http://127.0.0.1:$PromptPort/readyz" -TimeoutSeconds 90 -Name "prompt_skeleton_service"

if ($StartNgrok) {
    if (-not $NgrokAuthtoken) {
        if ($promptEnv.ContainsKey("NGROK_AUTHTOKEN")) {
            $NgrokAuthtoken = $promptEnv["NGROK_AUTHTOKEN"]
        } elseif ($passageEnv.ContainsKey("NGROK_AUTHTOKEN")) {
            $NgrokAuthtoken = $passageEnv["NGROK_AUTHTOKEN"]
        } else {
            $NgrokAuthtoken = $env:NGROK_AUTHTOKEN
        }
    }
    if ($NgrokAuthtoken) {
        Write-Host "Configuring ngrok authtoken" -ForegroundColor DarkYellow
        & $NgrokPath config add-authtoken $NgrokAuthtoken | Out-Null
    }
    Write-Host "Starting ngrok for prompt_skeleton_service on port $PromptPort" -ForegroundColor Yellow
    Start-Process -FilePath $NgrokPath -ArgumentList @("http", "$PromptPort") | Out-Null
}

Write-Host ""
Write-Host "Demo services launched." -ForegroundColor Green
Write-Host "prompt_skeleton_service: http://127.0.0.1:$PromptPort"
Write-Host "passage_service: http://127.0.0.1:$PassagePort"
Write-Host "prompt diagnostics: http://127.0.0.1:$PromptPort/api/v1/diagnostics/dependencies"
Write-Host "passage diagnostics: http://127.0.0.1:$PassagePort/api/v1/diagnostics/runtime"
Write-Host "prompt env files: $([string]::Join(', ', $promptEnvBundle.LoadedPaths))"
Write-Host "passage env files: $([string]::Join(', ', $passageEnvBundle.LoadedPaths))"
Write-Host ""
Write-Host "Optional security env vars before startup:" -ForegroundColor DarkGray
Write-Host "  PROMPT_SERVICE_API_TOKEN"
Write-Host "  PROMPT_SERVICE_SECURITY_ENABLED=true"
Write-Host "  PROMPT_SERVICE_RATE_LIMIT_PER_MINUTE=120"
Write-Host "  PROMPT_GENERATION_MAX_CONCURRENT=2"
Write-Host "  PROMPT_GENERATION_MAX_QUEUE=12"
Write-Host "  PROMPT_GENERATION_ACQUIRE_TIMEOUT_SECONDS=240"
Write-Host "  NGROK_AUTHTOKEN=<your-ngrok-token>"
