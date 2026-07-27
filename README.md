# Fraud Detection

A containerized fraud-detection service that accepts transaction events, evaluates configurable rules, and stores the results for later lookup.

The project currently supports:

- Local development on your machine with Docker Compose.
- Production deployment with Kubernetes and CI-built container images.
- Runtime rule changes through a mounted JSON file or Kubernetes ConfigMap.

The Kubernetes deployment now lives in the Helm chart at [fraud-detection](fraud-detection). The intended end state is to move the infrastructure code into a separate repository and keep this repo focused on the application.

## Architecture

```text
Client
	│
	│ POST /api/v1/transactions
	▼
Transaction Producer :8001
	│
	│ Kafka produce transactions_topic
	▼
Kafka
	▼
Fraud Worker
	│
	├── Evaluates configurable rules
	└── Stores results in PostgreSQL
	│
	▼
Query API :8000
	│
	│ GET /api/v1/transactions/{transaction_id}
	▼
Client
```

The producer and query API are HTTP services. The worker is a background process that consumes Kafka messages. PostgreSQL is the system of record; Redis is internal infrastructure for short-lived velocity state.

## Project structure

```text
fraud_detection/
├── app/
│   ├── api.py                    # Query API
│   ├── config.py                 # Runtime rule configuration
│   ├── producer.py               # Transaction ingestion API
│   ├── rule.py                   # Transaction model and fraud rules
│   └── worker.py                 # Kafka consumer and transaction processor
├── fraud-detection/              # Helm chart for Kubernetes deployment
├── infra/
│   └── README.md                 # Infrastructure runbook and split guidance
├── tests/                        # Automated tests
├── Dockerfile                    # Application image definition
├── docker-compose.yml            # Local multi-container deployment
├── rules.json                    # Default local rule configuration
├── requirements.txt              # Python dependencies
└── .gitignore
```

## Transaction format

The producer accepts JSON with these fields:

```json
{
	"transaction_id": "tx-1001",
	"user_id": "user-42",
	"amount": 12500,
	"category": "RETAIL"
}
```

`timestamp` is optional and is generated automatically when omitted.

## Fraud rules

The default configuration is in [rules.json](rules.json):

```json
{
	"high_value_threshold": 10000,
	"velocity_threshold": 5,
	"restricted_categories": {
		"GAMBLING": 5000,
		"CRYPTO": 5000
	}
}
```

The rules currently flag:

- `HIGH_VALUE_TRANSACTION` when the amount exceeds `high_value_threshold`.
- `VELOCITY_LIMIT_EXCEEDED` when a user's count exceeds `velocity_threshold` within the worker's 60-second Redis window.
- `RISKY_CATEGORY_LIMIT` when a configured restricted category exceeds its amount limit.

The worker reloads the rule file when its modification time changes. In Kubernetes, update the `fraud-rules` ConfigMap and restart the relevant pods if the cluster's ConfigMap projection does not refresh the file quickly enough.

## Run with Docker Compose

### Prerequisites

- Docker Desktop with the Docker engine running.
- Docker Compose v2.

### Start the application

From the repository root:

```powershell
$env:REDIS_PASSWORD = "use-a-strong-local-password"
docker compose up -d --build
```

Verify startup and health checks:

```powershell
docker compose ps
```

The services are:

| Service | Address | Purpose |
|---|---|---|
| Producer | `http://127.0.0.1:8001` | Accepts transactions |
| Query API | `http://127.0.0.1:8000` | Retrieves processed transactions |
| Prometheus | `http://127.0.0.1:9090` | Metrics scraping and query UI |
| Grafana | `http://127.0.0.1:3000` | Dashboards (`admin/admin` by default) |
| Kafka UI | `http://127.0.0.1:8080` | Kafka cluster/topic inspection |
| PostgreSQL | Internal only | Durable transaction storage |
| Redis | Internal only | Velocity counters and short-lived state |
| Worker metrics | `http://127.0.0.1:9100/metrics` | Worker Prometheus metrics |

### Send a transaction

With Compose running, open another PowerShell window:

```powershell
$body = @{
		transaction_id = "compose-001"
		user_id        = "user-1"
		amount         = 12500
		category       = "RETAIL"
} | ConvertTo-Json

Invoke-RestMethod `
		-Uri http://127.0.0.1:8001/api/v1/transactions `
		-Method Post `
		-ContentType "application/json" `
		-Body $body
```

### Query the result

```powershell
Start-Sleep -Seconds 2
Invoke-RestMethod `
		http://127.0.0.1:8000/api/v1/transactions/compose-001 | `
		ConvertTo-Json -Depth 5
```

Health checks:

```powershell
Invoke-RestMethod http://127.0.0.1:8001/health
Invoke-RestMethod http://127.0.0.1:8000/health
```

Metrics endpoints:

```powershell
Invoke-WebRequest http://127.0.0.1:8001/metrics
Invoke-WebRequest http://127.0.0.1:8000/metrics
Invoke-WebRequest http://127.0.0.1:9100/metrics
```

Stop the application with `Ctrl+C`, or run:

```powershell
docker compose down
```

Use `docker compose down -v` only when you also want to remove the Redis data volume.

## Run locally on your machine

Use Docker Compose for local development. Quick start:

```powershell
$env:REDIS_PASSWORD = "use-a-strong-local-password"
docker compose up -d --build
docker compose ps
```

Open a browser or second terminal to verify the health endpoints:

```powershell
Invoke-RestMethod http://127.0.0.1:8001/health
Invoke-RestMethod http://127.0.0.1:8000/health
```

Optional observability checks:

```powershell
Invoke-WebRequest http://127.0.0.1:8001/metrics
Invoke-WebRequest http://127.0.0.1:8000/metrics
Invoke-WebRequest http://127.0.0.1:9100/metrics
```

Use `docker compose down` to stop the stack, or `docker compose down -v` to remove the Redis volume too.

## Infrastructure

Kubernetes and cluster operations live in [infra/README.md](infra/README.md).

The app repository keeps the local developer workflow, while the infra docs cover Helm-based deployment, dev/prod values files, rollout validation, and the eventual split into a separate infrastructure repository.

## Configuration

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection URL | `postgresql://fraud_user:change-me@localhost:5432/fraud_detection` |
| `REDIS_URL` | Redis connection URL | `redis://localhost:6379` |
| `FRAUD_RULES_CONFIG_PATH` | Rules JSON path | `rules.json` |
| `REDIS_PASSWORD` | Compose Redis password | `change-me` in Compose only |
| `POSTGRES_PASSWORD` | Compose PostgreSQL password | `change-me` in Compose only |
| `OBSERVABILITY_LOG_LEVEL` | Service log level | `INFO` |
| `OBSERVABILITY_ENABLE_TRACING` | Enables OTLP tracing setup when dependencies are present | `false` |
| `WORKER_METRICS_PORT` | Worker Prometheus endpoint port | `9100` |

Do not commit real passwords. Use Kubernetes Secrets, Docker secrets, or an external secret manager in production.

## Observability

This repository includes a baseline observability stack for local development:

- Structured service logs with request IDs (`x-request-id`) on API and producer.
- Prometheus metrics on:
	- Producer: `http://127.0.0.1:8001/metrics`
	- Query API: `http://127.0.0.1:8000/metrics`
	- Worker: `http://127.0.0.1:9100/metrics`
- Local Prometheus UI: `http://127.0.0.1:9090`
- Local Grafana UI: `http://127.0.0.1:3000` (default `admin/admin` unless overridden)

Start the full local stack:

```powershell
$env:REDIS_PASSWORD = "use-a-strong-local-password"
docker compose up -d --build
```

Optional distributed tracing can be enabled by setting:

- `OBSERVABILITY_ENABLE_TRACING=true`
- `OTEL_EXPORTER_OTLP_ENDPOINT=http://<collector-host>:4318/v1/traces`

Tracing setup is optional, so local development still runs without an OpenTelemetry collector.

## Testing

Configure the project virtual environment, then run:

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The tests cover loading rules from a file and evaluating a transaction with configured thresholds.

## Queue behavior

The current implementation uses Kafka for the transaction queue:

- Producer: writes to `transactions_topic`
- Worker: consumes from `transactions_topic`
- Persistence: PostgreSQL `transactions` table

Consumer-group behavior, retries, and partition ordering are handled by Kafka rather than Redis list operations.

## Production considerations

Before treating this as production-ready, add or confirm:

- CI tests, image builds, vulnerability scanning, and registry publishing.
- Immutable image tags or image digests.
- Managed PostgreSQL with backups, migrations, and a high-availability strategy.
- Managed Redis or a highly available Redis deployment for queueing and velocity state.
- External Secret management.
- Ingress/API gateway authentication, TLS, rate limiting, and request authorization.
- Redis and application NetworkPolicies.
- Alerting and SLO dashboards for business and infrastructure signals.
- Dead-letter handling and retry policy for malformed or failed messages.
- Database/data retention and disaster-recovery policies.
- Horizontal scaling and load testing for the producer and worker.

## Repository workflow

The Kubernetes work is developed on the `feature/initial_k8_addition` branch and can be merged through a GitHub pull request. The production image should be built by CI after code review rather than built manually on a cluster node.
