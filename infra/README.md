# Infrastructure Runbook

Local development uses Docker Compose from the application repository. Production uses the Helm chart in [fraud-detection](../fraud-detection).

## Chart locations

- Helm chart: [fraud-detection](../fraud-detection)
- Common values: [fraud-detection/values.yaml](../fraud-detection/values.yaml)
- Dev values: [fraud-detection/values-dev.yaml](../fraud-detection/values-dev.yaml)
- Prod values: [fraud-detection/values-prod.yaml](../fraud-detection/values-prod.yaml)

## Production deployment

Use the production values file when deploying to a cluster:

```powershell
helm upgrade --install fraud-system .\fraud-detection -f .\fraud-detection\values.yaml -f .\fraud-detection\values-prod.yaml --namespace fraud-detection --create-namespace
kubectl -n fraud-detection rollout status deployment/fraud-api
kubectl -n fraud-detection rollout status deployment/fraud-producer
kubectl -n fraud-detection rollout status deployment/fraud-worker
kubectl -n fraud-detection rollout status deployment/kong
kubectl -n fraud-detection get ingress
```

Production notes:

1. Replace `IMAGE_TAG` with a commit SHA or immutable image tag.
2. Keep application secrets out of Helm values and provision them externally as Kubernetes Secrets before Helm deploys.
3. Configure `kong.ingress.hosts`, TLS, and ingress annotations in `values-prod.yaml` for your cluster and domain.
4. Keep `api`, `producer`, and `redis` internal; expose traffic through Kong ingress only.
