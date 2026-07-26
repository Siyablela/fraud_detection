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
# Create local config from the template the first time.
Copy-Item .env.example .env

docker compose up --build
```

The services are:

| Service | Address | Purpose |
|---|---|---|
| Producer | `http://127.0.0.1:8001` | Accepts transactions |
| Query API | `http://127.0.0.1:8000` | Retrieves processed transactions |
| PostgreSQL | Internal only | Durable transaction storage |
| Redis | Internal only | Velocity counters and short-lived state |
| Worker | Internal only | Evaluates transactions |

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

Stop the application with `Ctrl+C`, or run:

```powershell
docker compose down
```

Use `docker compose down -v` only when you also want to remove the Redis data volume.

## Run locally on your machine

Use Docker Compose for local development. The short version is:

```powershell
Copy-Item .env.example .env
docker compose up --build
```

Open a browser or second terminal to verify the health endpoints:

```powershell
Invoke-RestMethod http://127.0.0.1:8001/health
Invoke-RestMethod http://127.0.0.1:8000/health
```

Use `docker compose down` to stop the stack, or `docker compose down -v` to remove the Redis volume too.

## Infrastructure

Kubernetes and cluster operations live in [infra/README.md](infra/README.md).

The app repository keeps the local developer workflow, while the infra docs cover Helm-based deployment, dev/prod values files, rollout validation, and the eventual split into a separate infrastructure repository.

## Configuration

All runtime configuration is loaded from `.env` (for Python services) and from the same `.env` file by Docker Compose variable substitution.

Start by creating your local file:

```powershell
Copy-Item .env.example .env
```

| Variable | Purpose | Default |
|---|---|---|
| `POSTGRES_DB` | PostgreSQL database name | in `.env.example` |
| `POSTGRES_USER` | PostgreSQL username | in `.env.example` |
| `POSTGRES_PASSWORD` | PostgreSQL password | in `.env.example` |
| `REDIS_PASSWORD` | Redis password | in `.env.example` |
| `DATABASE_URL` | App PostgreSQL connection URL | in `.env.example` |
| `REDIS_URL` | App Redis connection URL | in `.env.example` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | in `.env.example` |
| `KAFKA_TOPIC_NAME` | Kafka topic for transactions | in `.env.example` |
| `KAFKA_CONSUMER_GROUP_ID` | Kafka consumer group id | in `.env.example` |
| `FRAUD_RULES_CONFIG_PATH` | Rules JSON path | in `.env.example` |
| `DB_POOL_MIN_SIZE` | PostgreSQL pool minimum size | in `.env.example` |
| `DB_POOL_MAX_SIZE` | PostgreSQL pool maximum size | in `.env.example` |
| `VELOCITY_WINDOW_SECONDS` | Redis velocity counter TTL window | in `.env.example` |
| `DEFAULT_HIGH_VALUE_THRESHOLD` | Default amount threshold when rules file is missing value | in `.env.example` |
| `DEFAULT_VELOCITY_THRESHOLD` | Default velocity threshold when rules file is missing value | in `.env.example` |
| `DEFAULT_RESTRICTED_CATEGORIES` | Default JSON object for restricted categories when rules file is missing value | in `.env.example` |
| `INTEGRATION_PRODUCER_URL` | Producer endpoint used by `integration_test.py` | in `.env.example` |
| `KC_BOOTSTRAP_ADMIN_USERNAME` | Keycloak admin username for local development | in `.env.example` |
| `KC_BOOTSTRAP_ADMIN_PASSWORD` | Keycloak admin password for local development | in `.env.example` |

Do not commit real passwords. Use Kubernetes Secrets, Docker secrets, or an external secret manager in production.

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
- Structured logs, metrics, tracing, and alerting.
- Dead-letter handling and retry policy for malformed or failed messages.
- Database/data retention and disaster-recovery policies.
- Horizontal scaling and load testing for the producer and worker.

## Repository workflow

The Kubernetes work is developed on the `feature/initial_k8_addition` branch and can be merged through a GitHub pull request. The production image should be built by CI after code review rather than built manually on a cluster node.
