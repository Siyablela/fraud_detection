# Infrastructure Runbook

This directory is the home for Helm-based Kubernetes operations. Helm is the only deployment source; Kustomize has been removed.

Application workflow: [README.md](../README.md) and Docker Compose.
Infrastructure workflow: the Helm chart in [fraud-detection](../fraud-detection) and the deployment notes in this file.

## Current scope

- Helm-based local parity checks with Minikube.
- Helm-based production deployment with CI-built images.
- Phase-specific values files for `dev` and `prod`.
- Cluster-specific secrets and rollout checks.

## Chart locations

- Helm chart: [fraud-detection](../fraud-detection)
- Common values: [fraud-detection/values.yaml](../fraud-detection/values.yaml)
- Dev values: [fraud-detection/values-dev.yaml](../fraud-detection/values-dev.yaml)
- Prod values: [fraud-detection/values-prod.yaml](../fraud-detection/values-prod.yaml)

## Local deployment

Build the image once and load it into Minikube:

```powershell
docker build -t fraud-detection:dev .
minikube image load fraud-detection:dev
helm upgrade --install fraud-system .\fraud-detection -f .\fraud-detection\values.yaml -f .\fraud-detection\values-dev.yaml --namespace fraud-detection --create-namespace
```

Verify the deployment:

```powershell
kubectl -n fraud-detection get pods
kubectl -n fraud-detection get svc postgres redis kafka api producer kong
```

Forward the HTTP services in separate terminal windows:

```powershell
kubectl -n fraud-detection port-forward service/producer 8001:8001
kubectl -n fraud-detection port-forward service/api 8000:8000
kubectl -n fraud-detection port-forward service/kong 8088:80
```

For local parity, dev values keep Kong ingress disabled and expose Kong by service type.

## Production deployment

Use the production values file when deploying to a cluster:

```powershell
helm upgrade --install fraud-system .\fraud-detection -f .\fraud-detection\values.yaml -f .\fraud-detection\values-prod.yaml --namespace fraud-detection --create-namespace
kubectl -n fraud-detection rollout status statefulset/redis
kubectl -n fraud-detection rollout status deployment/fraud-api
kubectl -n fraud-detection rollout status deployment/fraud-producer
kubectl -n fraud-detection rollout status deployment/fraud-worker
kubectl -n fraud-detection rollout status deployment/kong
kubectl -n fraud-detection get ingress
```

Before applying production resources:

1. Replace the placeholder Redis and PostgreSQL values in the release pipeline, or source them from an external secret manager.
2. Configure the CI pipeline to replace `IMAGE_TAG` with a commit SHA or immutable image tag.
3. Ensure stable PostgreSQL and Redis connectivity. For production, prefer managed services and store full connection URLs in Secrets.
4. Configure `kong.ingress.hosts`, TLS, and ingress annotations in `values-prod.yaml` for your cluster and domain.
5. Keep `api`, `producer`, and `redis` internal; expose traffic through Kong ingress only.

## Split plan

When this infrastructure is moved to its own repository, keep the Helm chart and this runbook together and leave the application repository focused on:

- Docker Compose
- application code under [app/](../app)
- tests under [tests/](../tests)
- local developer documentation
