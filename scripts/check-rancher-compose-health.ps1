param(
    [int]$Tail = 80,
    [string[]]$Services = @("postgres", "redis", "kafka", "api", "worker", "keycloak", "kafka-ui")
)

$ErrorActionPreference = "Stop"

$rdDocker = "C:\Program Files\Rancher Desktop\resources\resources\win32\bin\docker.exe"

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Require-RancherDocker {
    if (-not (Test-Path $rdDocker)) {
        throw "Rancher docker.exe not found at $rdDocker"
    }

    $ver = (& $rdDocker version --format "Client={{.Client.Version}} Server={{.Server.Version}}" 2>$null).Trim()
    if (-not ($ver -match "Server=\d")) {
        throw "Rancher Docker daemon is not reachable from Windows"
    }

    Write-Host "Docker endpoint: $ver" -ForegroundColor Green
}

function Show-RancherContext {
    Write-Step "Rancher Desktop context check"

    try {
        $settings = & "C:\Program Files\Rancher Desktop\resources\resources\win32\bin\rdctl.exe" list-settings | ConvertFrom-Json
        Write-Host ("Container engine: {0}" -f $settings.containerEngine.name) -ForegroundColor Green
    }
    catch {
        Write-Warning "Could not read Rancher Desktop settings: $($_.Exception.Message)"
    }
}

function Show-ComposePs {
    Write-Step "Compose service status"
    & $rdDocker compose ps
}

function Show-ContainerHealth {
    Write-Step "Container health/status summary"

    $rows = & $rdDocker ps -a --format "{{.Names}}|{{.Status}}|{{.Image}}"
    if (-not $rows) {
        Write-Warning "No containers found"
        return
    }

    $objects = foreach ($row in $rows) {
        $parts = $row -split "\|", 3
        [PSCustomObject]@{
            Name = $parts[0]
            Status = $parts[1]
            Image = $parts[2]
        }
    }

    $objects | Format-Table -AutoSize

    $bad = $objects | Where-Object {
        $_.Status -match "Exited|Dead|Restarting|unhealthy"
    }

    if ($bad) {
        Write-Warning "Detected containers that are exited/restarting/unhealthy:"
        $bad | Format-Table -AutoSize
    }
    else {
        Write-Host "No exited/restarting/unhealthy containers detected" -ForegroundColor Green
    }
}

function Show-ServiceLogs {
    Write-Step "Recent logs for selected services"

    foreach ($service in $Services) {
        Write-Host "`n--- $service (last $Tail lines) ---" -ForegroundColor Yellow
        try {
            & $rdDocker compose logs --no-color --tail $Tail $service
        }
        catch {
            Write-Warning "Could not read logs for service '$service'"
        }
    }
}

function Show-QuickChecks {
    Write-Step "Quick endpoint checks"

    $checks = @(
        @{ Name = "API health"; Url = "http://127.0.0.1:8000/health" },
        @{ Name = "Keycloak"; Url = "http://127.0.0.1:8081" },
        @{ Name = "Kafka UI"; Url = "http://127.0.0.1:8080" }
    )

    foreach ($check in $checks) {
        try {
            $response = Invoke-WebRequest -Uri $check.Url -UseBasicParsing -TimeoutSec 5
            Write-Host ("{0}: HTTP {1}" -f $check.Name, $response.StatusCode) -ForegroundColor Green
        }
        catch {
            Write-Warning ("{0}: FAILED ({1})" -f $check.Name, $_.Exception.Message)
        }
    }
}

try {
    Write-Step "Preflight"
    Require-RancherDocker
    Show-RancherContext

    Show-ComposePs
    Show-ContainerHealth
    Show-QuickChecks
    Show-ServiceLogs

    Write-Host "`nHealth check complete." -ForegroundColor Green
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
