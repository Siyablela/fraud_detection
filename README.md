# Fraud Detection

A Python-based fraud-detection platform that ingests transaction events through Kafka, evaluates them against configurable fraud rules, stores the results in PostgreSQL, and exposes a secured query API through FastAPI.

This repository is structured as an application plus supporting infrastructure assets. The application code lives under `app/`, while the Helm chart and operational docs live under `fraud-detection/` and `infra/`.

## What this project does

- Accepts transaction events from Kafka and applies fraud rules.
- Persists transaction records and fraud decisions in PostgreSQL.
- Provides a read-only API for authorized callers.
- Protects the API with JWT validation and scope checks.
- Supports local Docker Compose development, including Kafka, PostgreSQL, Keycloak, and the worker/API services.
- Supports Kubernetes deployment via the Helm chart in `fraud-detection/`.

## Architecture

```text
External producer / client
          |
          v
      Kafka topic
          |
          v
  Fraud worker (app.worker)
          |
          +--> evaluates rules
          +--> stores results in PostgreSQL
          |
          v
    Query API (FastAPI)
          |
          +--> GET /api/v1/transactions/{transaction_id}
          +--> GET /api/v1/categories/{category_name}
          |
          v
  Authorized client with JWT
```

The repo does not ship a standalone HTTP producer service. In practice, you either publish messages to Kafka from another app, use the provided integration smoke script, or write a small producer around the same topic contract.

## Project structure

```text
fraud_detection/
├── app/
│   ├── api.py                 # FastAPI query API + auth endpoints
│   ├── batch_worker.py       # Scheduled batch summary worker
│   ├── config.py             # Rules configuration loader
│   ├── database.py           # SQLAlchemy/PostgreSQL persistence
│   ├── kafka_security.py     # Kafka TLS/SASL client configuration
│   ├── keycloak_bootstrap.py # Keycloak bootstrap helpers
│   ├── main.py               # ASGI entry point
│   ├── observability.py      # Logging, tracing, metrics
│   ├── rate_limit.py         # API rate limiting middleware
│   ├── rule.py               # Transaction model and fraud evaluation logic
│   ├── security.py           # JWT validation + scopes
│   ├── settings.py           # Environment-backed runtime settings
│   ├── token_service.py      # Keycloak token exchange helpers
│   ├── worker.py             # Kafka consumer and fraud processor
│   └── ...
├── fraud-detection/          # Helm chart for Kubernetes deployment
├── infra/
│   └── README.md             # Infrastructure runbook and split guidance
├── tests/                   # Automated unit tests
├── .env.example             # Example runtime environment variables
├── Dockerfile               # Application image
├── docker-compose.yml       # Local multi-container stack
├── integration_test.py      # Kafka + API smoke test
├── rules.json               # Default local rules file
├── requirements.txt         # Python dependencies
├── SECURITY.md              # Security controls and auth scope layout
├── alembic.ini              # Database migration configuration
├── README.md                # Project docs
├── .gitignore
└── .github/workflows/       # CI/CD workflow definitions
```

## Transaction format

Transactions are represented as JSON payloads that the fraud worker evaluates. The current transaction model is:

```json
{
  "transaction_id": "tx-1001",
  "user_id": "user-42",
  "amount": 12500,
  "category": "RETAIL",
  "timestamp": 1720000000
}
```

The `timestamp` field is optional at the application boundary and is auto-populated when omitted.

## Fraud rules

The default configuration is in [rules.json](rules.json):

```json
{
  "high_value_threshold": 10000,
  "restricted_categories": {
    "GAMBLING": 5000,
    "CRYPTO": 5000
  },
  "velocity_window_seconds": 60,
  "velocity_max_count": 5,
  "velocity_max_amount": 5000
}
```

The current engine flags transactions when:

- `HIGH_VALUE_THRESHOLD` is triggered by an amount greater than or equal to `high_value_threshold`.
- `RISKY_CATEGORY_LIMIT` is triggered when a restricted category exceeds its configured amount threshold.
- `VELOCITY_COUNT_EXCEEDED` and `VELOCITY_AMOUNT` are triggered when the caller supplies recent user activity inside the configured velocity window.

The rules are reloaded when the configured file changes, which makes runtime updates possible through a mounted file or ConfigMap.

## Local development with Docker Compose

### Prerequisites

- Docker Desktop or a compatible Docker engine
- Docker Compose v2
- A local `.env` file based on `.env.example`

### 1) Configure environment

Copy the example environment file:

```powershell
Copy-Item .env.example .env
```

Then review the values in `.env` and adjust any required secrets or ports.

### 2) Start the stack

From the repository root:

```powershell
docker compose up --build
```

This starts the following services:

| Service | Address | Purpose |
|---|---|---|
| `postgres` | internal only | Durable transaction storage |
| `kafka` | internal only | Transaction event bus |
| `kafka-ui` | `http://127.0.0.1:8080` | Kafka inspection and topic management |
| `keycloak` | `http://127.0.0.1:8081` | Identity provider for JWT-based auth |
| `worker` | internal only | Consumes Kafka messages and evaluates fraud rules |
| `api` | `http://127.0.0.1:8000` | Query API and auth endpoints |
| `batch-worker` | internal only | Periodic summary job |

### 3) Check service health

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
Invoke-RestMethod http://127.0.0.1:8000/ready
Invoke-RestMethod http://127.0.0.1:8000/live
```

The worker exposes Prometheus metrics at:

```powershell
Invoke-RestMethod http://127.0.0.1:9100/metrics
```

### 4) Query transactions

Use a valid JWT access token that includes the required scope (`fraud:transactions:read`) and call:

```powershell
$headers = @{ Authorization = "Bearer <token>" }
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/v1/transactions/tx-1001" `
  -Headers $headers
```

You can also query category pages:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/v1/categories/RETAIL?page=1&page_size=20" `
  -Headers $headers
```

### 5) Stop the stack

```powershell
docker compose down
```

Use `docker compose down -v` only if you also want to remove the PostgreSQL volume.

## Authentication and authorization

The API validates bearer tokens against the configured Keycloak/OIDC issuer and checks the required scope before allowing reads.

Key settings in `.env.example` include:

```env
JWT_ISSUER=http://localhost:8081/realms/fraud
JWT_AUDIENCE=fraud-api
JWT_JWKS_URL=http://localhost:8081/realms/fraud/protocol/openid-connect/certs
JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ=fraud:transactions:read
AUTH_TOKEN_ENDPOINT_ENABLED=true
```

When `AUTH_TOKEN_ENDPOINT_ENABLED=true`, the API will expose test helpers:

- `POST /api/v1/auth/token`
- `POST /api/v1/auth/service-token`

These endpoints are intended for local testing and bootstrap workflows, not as production authentication surfaces.

## Configuration

The runtime configuration is environment-driven via `.env` and `.env.example`. Important values include:

| Variable | Purpose | Example |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://postgres:postgres@postgres:5432/fraud_detection` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers | `kafka:9092` |
| `KAFKA_TOPIC_NAME` | Source topic for transactions | `transactions_topic` |
| `KAFKA_DLQ_TOPIC_NAME` | Dead-letter topic | `transactions_topic.dlq` |
| `FRAUD_RULES_CONFIG_PATH` | Rules JSON path | `/config/rules.json` |
| `DEFAULT_HIGH_VALUE_THRESHOLD` | Default high-value threshold | `10000` |
| `DEFAULT_RESTRICTED_CATEGORIES` | Default risky-category thresholds | `{"GAMBLING": 5000, "CRYPTO": 5000}` |
| `OBSERVABILITY_LOG_LEVEL` | Structured logging verbosity | `INFO` |
| `WORKER_METRICS_PORT` | Worker metrics port | `9100` |

Do not commit real secrets. Prefer environment injection, Docker secrets, Kubernetes Secrets, or an external secret manager in non-local environments.

## Testing

Run the repository test suite with:

```powershell
.venv\Scripts\python.exe -m pytest -q
```

The tests cover:

- rules loading and reload behavior
- API auth and authorization
- pagination and query endpoints
- security middleware and observability
- Kafka security helpers
- worker + DLQ processing

## Running a smoke test

The repo includes an end-to-end smoke test script at [integration_test.py](integration_test.py). It publishes a record to Kafka and waits for it to appear via the secured query API.

Before running it, set the required integration variables in `.env`:

```env
INTEGRATION_API_URL=http://localhost:8000
INTEGRATION_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
INTEGRATION_KAFKA_TOPIC_NAME=transactions_topic
INTEGRATION_ACCESS_TOKEN=<JWT access token>
```

Then run:

```powershell
.venv\Scripts\python.exe integration_test.py
```

## Kubernetes and deployment

The deployment manifests live in the Helm chart under [fraud-detection](fraud-detection). Operational guidance is documented in [infra/README.md](infra/README.md).

The project is intended to keep application logic and infrastructure concerns separated, with the Helm chart treated as infrastructure and the Python app as the primary source of business logic.

## Production considerations

Before using this in production, confirm or add:

- CI pipeline with tests, image builds, and vulnerability scanning
- Immutable tags or digests for image releases
- Managed PostgreSQL and backups
- Managed Kafka or a Kafka broker with proper HA and partition planning
- Secret management for Keycloak, database, and broker credentials
- Ingress/API gateway auth, TLS, and rate limiting
- Structured logging, tracing, and alerting
- Dead-letter handling and retry policy for malformed or failed messages
- User and service identity policies for least privilege
- Horizontal scaling and load testing for the worker and API

## Repository workflow

Use feature branches for app changes and merge them through the standard GitHub workflow. The Helm chart and cluster-specific operational guidance are kept alongside the repo, but they should be treated as infrastructure concerns rather than as part of the application runtime itself.
