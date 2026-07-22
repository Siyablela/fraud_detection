# Fraud Detection

A containerized fraud-detection service that accepts transaction events, evaluates configurable rules, and stores the results for later lookup.

The project supports:

- Local development with Docker Compose.
- Local Kubernetes testing with Minikube.
- Production Kubernetes deployment using CI-built container images.
- Runtime rule changes through a mounted JSON file or Kubernetes ConfigMap.

## Architecture

```text
Client
	│
	│ POST /api/v1/transactions
	▼
Transaction Producer :8001
	│
	│ Redis LPUSH transactions_queue
	▼
Redis
	│
	│ Worker BLPOP
	▼
Fraud Worker
	│
	├── Evaluates configurable rules
	├── Stores tx:<transaction_id>
	└── Updates category:<category>
	│
	▼
Query API :8000
	│
	│ GET /api/v1/transactions/{transaction_id}
	▼
Client
```

The producer and query API are HTTP services. The worker is a background process that consumes Redis queue messages. PostgreSQL is the system of record; Redis is internal infrastructure for queueing and short-lived velocity state.

## Project structure

```text
fraud_detection/
├── app/
│   ├── api.py                    # Query API
│   ├── config.py                 # Runtime rule configuration
│   ├── producer.py               # Transaction ingestion API
│   ├── rule.py                   # Transaction model and fraud rules
│   └── worker.py                 # Redis queue consumer
├── k8s/
│   ├── base/                     # Shared Kubernetes resources
│   ├── overlays/local/           # Local image deployment
│   ├── overlays/production/      # CI image deployment
│   └── README.md                 # Kubernetes runbook
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
docker compose up --build
```

The services are:

| Service | Address | Purpose |
|---|---|---|
| Producer | `http://127.0.0.1:8001` | Accepts transactions |
| Query API | `http://127.0.0.1:8000` | Retrieves processed transactions |
| PostgreSQL | Internal only | Durable transaction storage |
| Redis | Internal only | Queue and velocity counters |
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

## Run with local Kubernetes

The local overlay is documented in [k8s/README.md](k8s/README.md). The short version for Minikube is:

```powershell
minikube start --driver=docker
docker build -t fraud-detection:dev .
minikube image load fraud-detection:dev
kubectl apply -k k8s/overlays/local
```

Verify the deployment:

```powershell
kubectl -n fraud-detection get pods
kubectl -n fraud-detection rollout status statefulset/redis
kubectl -n fraud-detection rollout status deployment/fraud-api
kubectl -n fraud-detection rollout status deployment/fraud-producer
kubectl -n fraud-detection rollout status deployment/fraud-worker
```

Forward the HTTP services in separate terminal windows:

```powershell
kubectl -n fraud-detection port-forward service/fraud-producer 8001:8001
kubectl -n fraud-detection port-forward service/fraud-api 8000:8000
```

Then use the same producer and query commands shown in the Docker Compose section.

Remove the local deployment with:

```powershell
kubectl delete -k k8s/overlays/local
minikube stop
```

## Kubernetes production deployment

The production overlay expects an image built and published by CI:

```text
ghcr.io/siyablela/fraud_detection:<immutable-tag>
```

Render the manifests before deployment:

```powershell
kubectl kustomize k8s/overlays/production
```

Before applying production resources:

1. Replace the placeholder Redis values in `k8s/base/redis-secret.yaml`, or create the Secret through an external secret manager.
2. Configure the CI pipeline to replace `IMAGE_TAG` with a commit SHA or immutable image tag.
3. Ensure the cluster has a default StorageClass, or configure the Redis volume claim for the cluster.
4. Expose only the producer through an Ingress or API gateway as required. Keep the query API and Redis internal by default.

Apply and verify:

```powershell
kubectl apply -k k8s/overlays/production
kubectl -n fraud-detection rollout status statefulset/redis
kubectl -n fraud-detection rollout status deployment/fraud-api
kubectl -n fraud-detection rollout status deployment/fraud-producer
kubectl -n fraud-detection rollout status deployment/fraud-worker
```

See [k8s/README.md](k8s/README.md) for the full Kubernetes deployment runbook.

## Configuration

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection URL | `postgresql://fraud_user:change-me@localhost:5432/fraud_detection` |
| `REDIS_URL` | Redis connection URL | `redis://localhost:6379` |
| `FRAUD_RULES_CONFIG_PATH` | Rules JSON path | `rules.json` |
| `REDIS_PASSWORD` | Compose Redis password | `change-me` in Compose only |
| `POSTGRES_PASSWORD` | Compose PostgreSQL password | `change-me` in Compose only |

Do not commit real passwords. Use Kubernetes Secrets, Docker secrets, or an external secret manager in production.

## Testing

Configure the project virtual environment, then run:

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The tests cover loading rules from a file and evaluating a transaction with configured thresholds.

## Queue behavior

The current implementation uses a Redis list only as a transient work queue:

- Producer: `LPUSH transactions_queue`
- Worker: blocking `BLPOP transactions_queue`
- Persistence: PostgreSQL `transactions` table

This processes newer messages first. If FIFO ordering is required, change the producer to `RPUSH` while keeping `BLPOP` in the worker.

For production workloads requiring acknowledgements, retries, consumer groups, and replay, Redis Streams or a managed messaging service should be considered instead of a basic Redis list.

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
