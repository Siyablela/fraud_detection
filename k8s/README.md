# Kubernetes deployment

The manifests are split into a reusable base, a local overlay, and a production overlay.

## Prerequisites

- A Kubernetes cluster.
- `kubectl` with Kustomize support.
- Access to `ghcr.io/siyablela/fraud_detection`.
- A real Redis password supplied before deployment.
- A real PostgreSQL password supplied before deployment.

## Configure the Redis Secret

Replace both placeholder values in `base/redis-secret.yaml` before applying, or create the Secret separately and remove `redis-secret.yaml` from the Kustomization resources. Do not commit real credentials.

The `REDIS_URL` value must use the same password as `REDIS_PASSWORD`:

```text
redis://:<password>@redis:6379
```

## Configure the PostgreSQL Secret

Replace the placeholders in `base/postgres-secret.yaml`, or create the Secret through an external Secret manager. The `DATABASE_URL` must use the same PostgreSQL password as `POSTGRES_PASSWORD`:

```text
postgresql://fraud_user:<password>@postgres:5432/fraud_detection
```

## Local Kubernetes testing

The local overlay uses the locally built `fraud-detection:dev` image. The following steps assume Minikube is using Docker as its driver.

### 1. Start Minikube

```powershell
minikube start --driver=docker
kubectl config use-context minikube
kubectl get nodes
```

If `kubectl apply` reports that the API server at `127.0.0.1` is unavailable, Minikube is stopped. Run the start command again.

### 2. Build and load the application image

```powershell
docker build -t fraud-detection:dev .
minikube image load fraud-detection:dev
```

Minikube has its own container runtime, so loading the image is required even when the image exists in the host Docker engine.

### 3. Deploy the local overlay

```powershell
kubectl apply -k k8s/overlays/local
```

The local overlay sets `imagePullPolicy: Never`, so Kubernetes uses the loaded local image instead of pulling from a registry.

### 4. Verify the deployment

```powershell
kubectl -n fraud-detection get pods
kubectl -n fraud-detection rollout status statefulset/redis
kubectl -n fraud-detection rollout status deployment/fraud-api
kubectl -n fraud-detection rollout status deployment/fraud-producer
kubectl -n fraud-detection rollout status deployment/fraud-worker
```

All pods should show `Running` and ready containers, for example `1/1` or `2/2`.

### 5. Forward the application ports

Run each command in a separate PowerShell window and leave both running:

```powershell
kubectl -n fraud-detection port-forward service/fraud-producer 8001:8001
```

```powershell
kubectl -n fraud-detection port-forward service/fraud-api 8000:8000
```

The producer accepts transactions on `http://127.0.0.1:8001`; the query API is available on `http://127.0.0.1:8000`.

### 6. Send a transaction

In a third PowerShell window:

```powershell
$body = @{
	transaction_id = "local-k8s-001"
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

Expected response:

```json
{
  "status": "queued",
  "transaction_id": "local-k8s-001",
	"topic": "transactions_topic"
}
```

### 7. Query the processed result

Wait a second for the worker, then run:

```powershell
Invoke-RestMethod `
	http://127.0.0.1:8000/api/v1/transactions/local-k8s-001 | `
	ConvertTo-Json -Depth 5
```

The high-value transaction should contain `"is_fraud": true` and `HIGH_VALUE_TRANSACTION` in `triggered_rules`.

Check service health directly:

```powershell
Invoke-RestMethod http://127.0.0.1:8001/health
Invoke-RestMethod http://127.0.0.1:8000/health
```

### Stop the local deployment

```powershell
kubectl delete -k k8s/overlays/local
minikube stop
```

## Render the production manifests

CI replaces `IMAGE_TAG` with the immutable image tag or commit SHA:

```powershell
kubectl kustomize k8s/overlays/production
```

## Deploy manually with a CI-built image

```powershell
kubectl apply -k k8s/overlays/production
kubectl -n fraud-detection rollout status statefulset/redis
kubectl -n fraud-detection rollout status deployment/fraud-api
kubectl -n fraud-detection rollout status deployment/fraud-producer
kubectl -n fraud-detection rollout status deployment/fraud-worker
```

Before applying manually, replace `IMAGE_TAG` in the production overlay with the image tag pushed by CI.

## Services

- `fraud-api`: internal ClusterIP on port `8000`.
- `fraud-producer`: internal ClusterIP on port `8001`; expose it through an Ingress or API gateway when external access is required.
- `postgres`: internal ClusterIP on port `5432`; durable transaction storage.
- `redis`: internal ClusterIP on port `6379`; velocity and short-lived cache state.

The rules are stored in `base/rules-configmap.yaml` and mounted at `/config/rules.json`. Updating the ConfigMap allows the application rule provider to reload the rules without rebuilding the image.

Transactions are stored in PostgreSQL. Redis is used for the work queue and short-lived velocity counters, not as the transaction database. For production, prefer managed PostgreSQL and Redis services plus an external Secret manager over running stateful databases in the cluster with repository-managed Secret templates.
