# fraud-detection Helm chart

Deploys the fraud API, worker, daily summary CronJob and migration Job from one
image, behind a Kong gateway, with optional in-cluster PostgreSQL, Redis, Kafka and
Keycloak for development.

Installation, secrets, upgrades, rollback and scaling: [infra/README.md](../infra/README.md).

| Template | Objects |
|---|---|
| `apps.yaml` | Migration Job (hook), worker and API Deployments, API Service, summary CronJob |
| `infrastructure.yaml` | In-cluster PostgreSQL, Redis, Kafka (StatefulSets), topic Job, Keycloak with the realm in `files/fraud-realm.json` |
| `gateway.yaml` | Kong (DB-less) Deployment, Service, optional Ingress |
| `configmaps-secrets.yaml` | Kong declarative config |
| `networkpolicy.yaml` | Ingress policies for the API, worker and data stores |
| `resilience.yaml` | PodDisruptionBudgets, HPAs, cert-manager Certificate |
| `alerting.yaml`, `alertmanager.yaml` | PrometheusRule alerts (when the CRD exists) and Alertmanager config |

`files/fraud-realm.json` is also mounted by `docker-compose.yml`, so local and
cluster identity configuration cannot drift.
