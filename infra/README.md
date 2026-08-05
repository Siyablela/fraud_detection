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
kubectl -n fraud-detection rollout status deployment/fraud-worker
kubectl -n fraud-detection rollout status deployment/kong
kubectl -n fraud-detection get ingress
```

Production notes:

1. Replace `IMAGE_TAG` with a commit SHA or immutable image tag.
2. Keep application secrets out of Helm values and provision them externally as Kubernetes Secrets before Helm deploys.
3. Configure `kong.ingress.hosts`, TLS, and ingress annotations in `values-prod.yaml` for your cluster and domain.
4. Keep `api` and `worker` internal; expose traffic through Kong ingress only.

## CI to Argo CD release flow

- CI validation workflow: [.github/workflows/ci.yml](../.github/workflows/ci.yml)
- GitOps release workflow: [.github/workflows/cd-gitops.yml](../.github/workflows/cd-gitops.yml)
- Argo application definition: [argocd/applications/fraud-detection.yaml](../argocd/applications/fraud-detection.yaml)

How it works:

1. CI runs tests and Helm validation on pull requests and main.
2. On merge to main, CD builds and pushes an immutable SHA image to GHCR.
3. CD updates [fraud-detection/values-prod.yaml](../fraud-detection/values-prod.yaml) image tags and commits the change.
4. Argo CD auto-sync reconciles the cluster to that commit.
