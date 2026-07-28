# Infrastructure Runbook

This directory is the home for Helm-based Kubernetes operations. Helm is the only deployment source; Kustomize has been removed.

Application workflow: [README.md](../README.md) and Docker Compose.
Infrastructure workflow: the Helm chart in [fraud-detection](../fraud-detection) and the deployment notes in this file.

## Current scope

- Rancher-managed Kubernetes cluster operations.
- Argo CD GitOps deployment from this repository.
- Helm chart environment overlays for `dev` and `prod`.
- Instana-friendly application observability toggles (logs + tracing endpoint wiring).

## Chart locations

- Helm chart: [fraud-detection](../fraud-detection)
- Common values: [fraud-detection/values.yaml](../fraud-detection/values.yaml)
- Dev values: [fraud-detection/values-dev.yaml](../fraud-detection/values-dev.yaml)
- Prod values: [fraud-detection/values-prod.yaml](../fraud-detection/values-prod.yaml)

## Rancher cluster deployment

1. Configure `kubectl` context to the target Rancher cluster and namespace:

```powershell
kubectl config current-context
kubectl create namespace fraud-detection --dry-run=client -o yaml | kubectl apply -f -
```

2. Apply runtime secrets (or map them from your external secret manager):

```powershell
kubectl -n fraud-detection create secret generic fraud-secrets `
	--from-literal=postgres-password="<postgres-password>" `
	--from-literal=redis-password="<redis-password>" `
	--dry-run=client -o yaml | kubectl apply -f -
```

3. Validate the chart locally before Argo sync:

```powershell
helm template fraud-system .\fraud-detection -f .\fraud-detection\values.yaml -f .\fraud-detection\values-prod.yaml > $null
```

4. (Optional) Manual Helm deploy for smoke checks before GitOps handoff:

```powershell
helm upgrade --install fraud-system .\fraud-detection -f .\fraud-detection\values.yaml -f .\fraud-detection\values-prod.yaml --namespace fraud-detection --create-namespace
kubectl -n fraud-detection get pods
```

5. For direct endpoint debugging from your workstation:

```powershell
kubectl -n fraud-detection port-forward service/producer 8001:8001
kubectl -n fraud-detection port-forward service/api 8000:8000
```

## Argo CD GitOps workflow

Argo install manifest is committed at [argocd-install.yaml](../argocd-install.yaml).

Application resource for this chart is committed at [argocd/applications/fraud-detection.yaml](../argocd/applications/fraud-detection.yaml).

1. Install or update Argo CD in your Rancher cluster:

```powershell
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n argocd -f .\argocd-install.yaml
```

2. Deploy this application through Argo CD:

```powershell
kubectl apply -f .\argocd\applications\fraud-detection.yaml
kubectl -n argocd get applications.argoproj.io fraud-detection
```

3. In Argo CD UI, confirm:

- `Sync Status: Synced`
- `Health Status: Healthy`

If your default branch or repo URL differs, update `targetRevision` and `repoURL` in [argocd/applications/fraud-detection.yaml](../argocd/applications/fraud-detection.yaml).

## Instana integration notes

The chart now supports environment-driven tracing setup via values:

- `observability.enableTracing`
- `observability.otlpEndpoint`
- `instana.enabled`
- `instana.otlpEndpoint`

Recommended production approach:

1. Install Instana agent/operator in the Rancher cluster.
2. Set `instana.enabled=true` in your environment values file.
3. Set `instana.otlpEndpoint` to your Instana OTLP ingest endpoint.
4. Keep `observability.logLevel=INFO` (or tighter in high-volume workloads).

You can override these values at deploy time:

```powershell
helm upgrade --install fraud-system .\fraud-detection `
	-f .\fraud-detection\values.yaml `
	-f .\fraud-detection\values-prod.yaml `
	--set instana.enabled=true `
	--set instana.otlpEndpoint="http://instana-agent.instana-agent.svc.cluster.local:4318/v1/traces" `
	--namespace fraud-detection --create-namespace
```

## Production deployment

Use production values with immutable image tags:

```powershell
helm upgrade --install fraud-system .\fraud-detection -f .\fraud-detection\values.yaml -f .\fraud-detection\values-prod.yaml --namespace fraud-detection --create-namespace
kubectl -n fraud-detection rollout status statefulset/redis
kubectl -n fraud-detection rollout status deployment/fraud-api
kubectl -n fraud-detection rollout status deployment/fraud-producer
kubectl -n fraud-detection rollout status deployment/fraud-worker
```

Before applying production resources:

1. Replace the placeholder Redis and PostgreSQL values in the release pipeline, or source them from an external secret manager.
2. Configure the CI pipeline to replace `IMAGE_TAG` with a commit SHA or immutable image tag.
3. Ensure stable PostgreSQL and Redis connectivity. For production, prefer managed services and store full connection URLs in Secrets.
4. Expose only the producer through an Ingress or API gateway as required. Keep the query API and Redis internal by default.
5. Confirm Argo CD `automated.prune` and `automated.selfHeal` align with your change-control policy.

## Split plan

When this infrastructure is moved to its own repository, keep the Helm chart and this runbook together and leave the application repository focused on:

- Docker Compose
- application code under [app/](../app)
- tests under [tests/](../tests)
- local developer documentation
