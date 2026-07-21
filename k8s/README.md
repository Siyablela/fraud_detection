# Kubernetes deployment

The manifests are split into a reusable base and a production overlay.

## Prerequisites

- A Kubernetes cluster with a default StorageClass.
- `kubectl` with Kustomize support.
- Access to `ghcr.io/siyablela/fraud_detection`.
- A real Redis password supplied before deployment.

## Configure the Redis Secret

Replace both placeholder values in `base/redis-secret.yaml` before applying, or create the Secret separately and remove `redis-secret.yaml` from the Kustomization resources. Do not commit real credentials.

The `REDIS_URL` value must use the same password as `REDIS_PASSWORD`:

```text
redis://:<password>@redis:6379
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
- `redis`: internal ClusterIP on port `6379` with a persistent volume.

The rules are stored in `base/rules-configmap.yaml` and mounted at `/config/rules.json`. Updating the ConfigMap allows the application rule provider to reload the rules without rebuilding the image.

For production, prefer a managed Redis service and an external Secret manager over running Redis in the cluster with a repository-managed Secret template.
