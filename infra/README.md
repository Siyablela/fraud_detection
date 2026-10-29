# Infrastructure runbook

This directory documents the deployment and operational model for the Fraud Detection stack. The application runtime is defined by the Python code in [app/](../app), while the infrastructure layer is managed through the Helm chart in [fraud-detection/](../fraud-detection).

## Current scope

- Local parity checks with Docker Compose and Helm-based development overlays.
- Kubernetes deployment using the chart under [fraud-detection/](../fraud-detection).
- Environment-specific values files for development and production.
- Operational guidance for secrets, storage, and rollout validation.

## Chart locations

- Helm chart: [fraud-detection/](../fraud-detection)
- Values file: [fraud-detection/values.yaml](../fraud-detection/values.yaml)
- Dev overlay: [fraud-detection/values-dev.yaml](../fraud-detection/values-dev.yaml)
- Prod overlay: [fraud-detection/values-prod.yaml](../fraud-detection/values-prod.yaml)

## Local deployment

Build the image and install or upgrade the chart in a local cluster:

```bash
helm upgrade --install fraud-system ./fraud-detection \
  -f ./fraud-detection/values.yaml \
  -f ./fraud-detection/values-dev.yaml \
  --namespace fraud-detection \
  --create-namespace
```

Check rollout state:

```bash
kubectl -n fraud-detection get pods
kubectl -n fraud-detection get svc
kubectl -n fraud-detection rollout status deployment/fraud-api
kubectl -n fraud-detection rollout status deployment/fraud-worker
```

## Production deployment

Use the production overlay for cluster deployment:

```bash
helm upgrade --install fraud-system ./fraud-detection \
  -f ./fraud-detection/values.yaml \
  -f ./fraud-detection/values-prod.yaml \
  --namespace fraud-detection \
  --create-namespace
```

Before promoting the release, make sure that:

1. Redis and PostgreSQL connection values are supplied securely through a secret manager or injected secret.
2. Image tags are pinned to a stable build ID instead of a floating tag.
3. database and messaging connectivity are validated from the cluster environment.
4. Only the intended public endpoint is exposed, while internal services remain restricted.

## Operational model

The deployed stack includes the main application workloads plus the supporting platform components:

- API service and worker deployment
- Batch worker when enabled
- PostgreSQL, Redis, Kafka, and related support services
- Keycloak and UI components used for local auth and inspection
- Optional resilience and monitoring resources defined in the chart

## Relationship to other docs

- [README.md](../README.md) — project overview and local development usage
- [app/README.md](../app/README.md) — runtime module map
- [fraud-detection/README.md](../fraud-detection/README.md) — chart-specific deployment summary
