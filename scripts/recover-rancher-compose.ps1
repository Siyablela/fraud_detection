param(
    [string]$ComposeArgs = "up -d --build",
    [int]$DockerReadyRetries = 30,
    [int]$ComposeRetries = 3
)

$ErrorActionPreference = "Stop"

$rdCtl = "C:\Program Files\Rancher Desktop\resources\resources\win32\bin\rdctl.exe"
$rdDocker = "C:\Program Files\Rancher Desktop\resources\resources\win32\bin\docker.exe"

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Test-RdTools {
    if (-not (Test-Path $rdCtl)) {
        throw "rdctl not found at $rdCtl"
    }
    if (-not (Test-Path $rdDocker)) {
        throw "Rancher docker.exe not found at $rdDocker"
    }
}

function Set-KubeContext {
    Write-Step "Switching kubectl context to rancher-desktop"
    if (Get-Command kubectl -ErrorAction SilentlyContinue) {
        kubectl config use-context rancher-desktop | Out-Host
    }
    else {
        Write-Warning "kubectl not found in PATH; skipping context switch"
    }
}

function Restart-Rancher {
    Write-Step "Restarting Rancher Desktop backend"
    & $rdCtl shutdown | Out-Host

    # Force-close WSL distros for a clean backend restart.
    wsl --terminate rancher-desktop 2>$null
    wsl --terminate rancher-desktop-data 2>$null

    & $rdCtl start --container-engine.name moby --no-modal-dialogs | Out-Host
}

function Wait-DockerReady {
    Write-Step "Waiting for Rancher Docker daemon readiness"

    for ($i = 1; $i -le $DockerReadyRetries; $i++) {
        try {
            $ver = (& $rdDocker version --format "Client={{.Client.Version}} Server={{.Server.Version}}" 2>$null).Trim()
            if ($ver -match "Server=\d") {
                Write-Host "Docker is ready: $ver" -ForegroundColor Green
                return
            }
        }
        catch {
            # Keep retrying until timeout.
        }

        Write-Host "Attempt $i/$DockerReadyRetries: daemon not ready yet..."
        Start-Sleep -Seconds 3
    }

    throw "Rancher Docker daemon did not become ready in time"
}

function Invoke-ComposeWithRetry {
    Write-Step "Running compose with Rancher docker CLI"

    for ($i = 1; $i -le $ComposeRetries; $i++) {
        Write-Host "Compose attempt $i/$ComposeRetries: docker compose $ComposeArgs" -ForegroundColor Yellow

        $output = & $rdDocker compose $ComposeArgs 2>&1
        $exitCode = $LASTEXITCODE
        $output | Out-Host

        if ($exitCode -eq 0) {
            Write-Host "Compose completed successfully" -ForegroundColor Green
            return
        }

        $text = ($output | Out-String)
        if ($text -match "timed out dialing Hyper-V socket") {
            Write-Warning "Hit transient Rancher backend socket timeout; retrying after short delay"
            Start-Sleep -Seconds 4
            continue
        }

        throw "Compose failed with a non-retryable error"
    }

    throw "Compose failed after $ComposeRetries attempts"
}

try {
    Write-Step "Preflight checks"
    Test-RdTools

    Set-KubeContext
    Restart-Rancher
    Wait-DockerReady
    Invoke-ComposeWithRetry

    Write-Host "`nAll done." -ForegroundColor Green
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
