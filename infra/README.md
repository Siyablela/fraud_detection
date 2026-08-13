# Infrastructure Runbook

This repository is intentionally simple: the application is a FastAPI API plus a Kafka consumer worker. It does not manage Kafka broker lifecycle or topic provisioning; those are platform concerns.

Local development uses Docker Compose from the repository root. Production uses the Helm chart in [fraud-detection](../fraud-detection).

## What is live vs. optional

Live runtime components:

- FastAPI API on port 8000
- Kafka worker process for transaction processing
- PostgreSQL for latest-state and history tables
- Keycloak for JWT validation and local auth bootstrap only

Optional or external concerns:

- Kafka cluster/topic provisioning
- ingress or gateway routing
- platform secrets injection and rotation
- GitOps deployment automation

## Chart locations

- Helm chart: [fraud-detection](../fraud-detection)
- Common values: [fraud-detection/values.yaml](../fraud-detection/values.yaml)
- Dev values: [fraud-detection/values-dev.yaml](../fraud-detection/values-dev.yaml)
- Prod values: [fraud-detection/values-prod.yaml](../fraud-detection/values-prod.yaml)

## Production deployment

Use the production values file when deploying to a cluster:

```powershell
helm upgrade --install fraud-system .\fraud-detection `
  -f .\fraud-detection\values.yaml `
  -f .\fraud-detection\values-prod.yaml `
  --namespace fraud-detection --create-namespace

kubectl -n fraud-detection rollout status deployment/fraud-api
kubectl -n fraud-detection rollout status deployment/fraud-worker
kubectl -n fraud-detection get svc
```

Production notes:

1. Use immutable image tags or commit SHAs for deployment stability.
2. Keep application credentials and secret material out of Helm values; provision them as Kubernetes Secrets from an external system.
3. Kafka topics must already exist before the app is expected to consume from them.
4. The application is not responsible for Kafka cluster lifecycle, permissions, or topic management.

## Local development notes

Use Docker Compose from the repository root:

```powershell
docker compose down --remove-orphans
docker compose up -d --build --force-recreate
docker exec fraud_api python -m alembic upgrade head
```

This is a convenience environment for testing the app, not a deployment model for Kafka as a managed service.

## CI to Argo CD release flow

- CI validation workflow: [.github/workflows/ci.yml](../.github/workflows/ci.yml)
- GitOps release workflow: [.github/workflows/cd-gitops.yml](../.github/workflows/cd-gitops.yml)
- Argo application definition: [argocd/applications/fraud-detection.yaml](../argocd/applications/fraud-detection.yaml)

How it works:

1. CI runs unit tests and template validation for pull requests and main.
2. On merge to main, CD builds and pushes an immutable image tag to GHCR.
3. CD updates [fraud-detection/values-prod.yaml](../fraud-detection/values-prod.yaml) with the image version.
4. Argo CD reconciles the cluster to the new commit.

## Operational boundaries

The app should be responsible for:

- API request handling
- JWT validation
- Kafka event consumption
- fraud evaluation
- persistence to PostgreSQL
- dead-letter publication on failure

The platform or infrastructure should be responsible for:

- Kafka broker availability and sizing
- topic creation and ACLs
- ingress and gateway configuration
- secret injection and rotation
- cluster deployment automation

This keeps the service focused and avoids coupling application logic to platform administration.
