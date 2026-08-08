param(
    [int]$TimeoutSeconds = 60
)

$ErrorActionPreference = "Stop"

function Get-RepoRoot {
    return Split-Path -Parent $PSScriptRoot
}

function Import-DotEnv {
    param([string]$Path)

    $values = @{}
    if (-not (Test-Path $Path)) {
        throw "Missing environment file at $Path"
    }

    foreach ($line in Get-Content -Path $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) {
            continue
        }

        $key, $value = $trimmed -split "=", 2
        $values[$key.Trim()] = $value.Trim().Trim('"').Trim("'")
    }

    return $values
}

function Wait-ForTransaction {
    param(
        [string]$TransactionId,
        [hashtable]$EnvValues,
        [datetime]$Deadline
    )

    $query = @"
SELECT
  CASE WHEN EXISTS (
    SELECT 1 FROM transactions WHERE transaction_id = '$TransactionId'
  ) THEN '1' ELSE '0' END AS latest_exists,
  (
    SELECT COUNT(*) FROM transaction_history WHERE transaction_id = '$TransactionId'
  ) AS history_count;
"@

    while ((Get-Date) -lt $Deadline) {
        $result = docker exec fraud_postgres psql -U $EnvValues.POSTGRES_USER -d $EnvValues.POSTGRES_DB -t -A -F "|" -c $query
        $result = ($result | Out-String).Trim()
        if ($result) {
            $latestExists, $historyCount = $result -split "\|", 2
            if ($latestExists -eq "1" -and [int]$historyCount -ge 1) {
                return [PSCustomObject]@{
                    LatestExists = $true
                    HistoryCount = [int]$historyCount
                }
            }
        }

        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for transaction '$TransactionId' to appear in both transactions and transaction_history tables."
}

function Ensure-KafkaTopics {
    param([hashtable]$EnvValues)

    docker exec fraud_kafka_broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic $EnvValues.KAFKA_TOPIC_NAME | Out-Null
    docker exec fraud_kafka_broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic $EnvValues.KAFKA_DLQ_TOPIC_NAME | Out-Null
}

function Wait-ForHttpJson {
    param(
        [string]$Name,
        [scriptblock]$Request,
        [datetime]$Deadline
    )

    while ((Get-Date) -lt $Deadline) {
        try {
            $result = & $Request
            if ($LASTEXITCODE -ne 0) {
                throw "Request command exited with code $LASTEXITCODE"
            }
            return $result
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }

    throw "Timed out waiting for $Name"
}

function Wait-ForWorkerMetrics {
    param([datetime]$Deadline)

    while ((Get-Date) -lt $Deadline) {
        try {
            docker exec fraud_worker python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9100/metrics', timeout=5).read()" | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Worker metrics command exited with code $LASTEXITCODE"
            }
            return
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }

    throw "Timed out waiting for worker metrics inside fraud_worker"
}

$repoRoot = Get-RepoRoot
$envPath = Join-Path $repoRoot ".env"
$envValues = Import-DotEnv -Path $envPath

Write-Host "Ensuring API and worker services are started..." -ForegroundColor Cyan
docker compose up -d api worker | Out-Null
Write-Host "Core services are requested." -ForegroundColor Green

$transactionId = "submission-" + [guid]::NewGuid().ToString("N")
$payload = @{
    transaction_id = $transactionId
    user_id = "submission-user"
    amount = 15000
    category = "GAMBLING"
    timestamp = [int][DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
} | ConvertTo-Json -Compress

Write-Host "Checking API health..." -ForegroundColor Cyan
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$health = Wait-ForHttpJson -Name "API health inside fraud_api" -Deadline $deadline -Request {
    docker exec fraud_api python -c "import json, urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=5).read().decode())"
}
Write-Host ("API health: {0}" -f ($health | ConvertTo-Json -Compress)) -ForegroundColor Green

Write-Host "Checking worker metrics..." -ForegroundColor Cyan
Wait-ForWorkerMetrics -Deadline $deadline
Write-Host "Worker metrics endpoint is available." -ForegroundColor Green

Write-Host "Ensuring Kafka topics exist..." -ForegroundColor Cyan
Ensure-KafkaTopics -EnvValues $envValues
Write-Host "Kafka topics are ready." -ForegroundColor Green

Write-Host "Publishing submission payload to Kafka topic '$($envValues.KAFKA_TOPIC_NAME)'..." -ForegroundColor Cyan
$payload | docker exec -i fraud_kafka_broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic $envValues.KAFKA_TOPIC_NAME | Out-Null
Write-Host ("Published transaction_id={0}" -f $transactionId) -ForegroundColor Green

$result = Wait-ForTransaction -TransactionId $transactionId -EnvValues $envValues -Deadline $deadline

Write-Host "Smoke test passed." -ForegroundColor Green
Write-Host ("Latest-state row present: {0}" -f $result.LatestExists) -ForegroundColor Green
Write-Host ("History rows present: {0}" -f $result.HistoryCount) -ForegroundColor Green
Write-Host ("Verified transaction_id={0}" -f $transactionId) -ForegroundColor Green
