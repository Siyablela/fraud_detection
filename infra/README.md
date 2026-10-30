# Kubernetes runbook

Deploying with the Helm chart in [fraud-detection/](../fraud-detection).

## Values files

| File | For |
|---|---|
| [values.yaml](../fraud-detection/values.yaml) | Defaults: a dev/test cluster with in-cluster PostgreSQL, Redis, Kafka and Keycloak |
| [values-dev.yaml](../fraud-detection/values-dev.yaml) | Local cluster (Rancher Desktop, kind) with a locally built image |
| [values-prod.yaml](../fraud-detection/values-prod.yaml) | Production: managed data stores, SASL_SSL Kafka, corporate IdP, autoscaling, TLS ingress |

The in-cluster dependencies are single replicas for development. Production sets
`inClusterDependencies.enabled: false` and points the Secret's URLs at managed,
replicated services.

## Secret

Create the Secret named by `secrets.existingSecretName` (default `fraud-secrets`)
before installing, from your secret store (External Secrets, Vault, sealed secrets):

| Key | When |
|---|---|
| `database-url` | always - e.g. `postgresql://fraud:...@postgres:5432/fraud_detection` |
| `redis-url` | always - e.g. `redis://:...@redis:6379/0` |
| `kafka-oauth-client-secret` | `config.kafkaSecurityProtocol` is `SASL_*` |
| `postgres-password`, `redis-password`, `keycloak-admin-password`, `fraud-analyst-client-secret`, `fraud-rules-admin-client-secret` | `inClusterDependencies.enabled` |

Local example only:

```bash
kubectl create namespace fraud-detection
kubectl -n fraud-detection create secret generic fraud-secrets \
  --from-literal=postgres-password=change-me --from-literal=redis-password=change-me \
  --from-literal=database-url='postgresql://fraud:change-me@postgres:5432/fraud_detection' \
  --from-literal=redis-url='redis://:change-me@redis:6379/0' \
  --from-literal=keycloak-admin-password=change-me \
  --from-literal=fraud-analyst-client-secret=change-me \
  --from-literal=fraud-rules-admin-client-secret=change-me
```

## Install or upgrade

```bash
docker build -t fraud-detection:dev .            # local clusters only
helm upgrade --install fraud-system ./fraud-detection \
  -n fraud-detection -f fraud-detection/values.yaml -f fraud-detection/values-dev.yaml --wait
```

Order of operations:

1. **Migrations**: the `fraud-migrate` Job (`alembic upgrade head`) is a Helm hook.
   It runs *before* the new pods on every upgrade (post-install on the very first
   install with in-cluster dependencies). If it fails, the release fails and the
   old pods keep running.
2. **Topics**: with in-cluster Kafka, the `kafka-topics` hook creates `transactions`
   (`inClusterDependencies.kafka.partitions`) and `transactions.dlq`. With external
   Kafka, the platform team creates them; the worker never auto-creates topics.
3. **Rollout**: `maxUnavailable: 0` for the API and worker. API pods take traffic only
   once `/ready` passes; a new worker joins the consumer group before an old one leaves.

## Rollback

`helm rollback fraud-system <revision>` restores the previous pods. Migrations
are forward-only in production practice: write each one so that the previous release
still works against the new schema (add columns before using them, drop them a
release later), so that rolling back the code never needs a schema downgrade.

## Scaling

- API: stateless; scale by CPU (HPA in values-prod).
- Worker: one consumer per partition at most. The chart refuses
  `resilience.autoscaling.worker.maxReplicas` above the partition count. To go wider,
  add partitions first. That changes which partition a key maps to, so drain the
  topic first if strict per-user ordering matters during the change.

## Verifying a deployment

```bash
kubectl -n fraud-detection get pods
kubectl -n fraud-detection port-forward svc/kong 8000:80
curl -s localhost:8000/api/v1/transactions -H "Authorization: Bearer $TOKEN"
kubectl -n fraud-detection logs deploy/fraud-worker | grep transaction_processed | head
```

CI renders the chart for every values file and validates it with kubeconform
(see [.github/workflows/ci.yml](../.github/workflows/ci.yml)).
