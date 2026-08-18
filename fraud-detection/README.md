# Fraud Detection Helm Chart

This chart deploys the Fraud Detection application to Kubernetes using a small, opinionated set of resources:

- API deployment for query endpoints
- Kafka worker deployment for realtime transaction processing
- Scheduled batch CronJob for deferred processing
- PostgreSQL database deployment and PVC
- Kafka broker deployment and service
- Keycloak bootstrap job and related settings
- Kong gateway configuration
- Kafka UI deployment
- Prometheus alerting resources

The chart is designed to run the same application architecture you use locally in Docker Compose, but in a Kubernetes-friendly layout.

---

## 1. What this chart deploys

At a high level, the chart creates the following components:

### Application runtime
- fraud-api
  - FastAPI service for read-only query endpoints
  - reachable over HTTP and exposed through Kong
- fraud-worker
  - consumes Kafka messages from the live fraud topic
  - validates transaction payloads
  - applies configured fraud rules
  - stores the latest record in PostgreSQL
  - writes event history and pushes poisoned messages to the DLQ topic
- fraud-batch-worker
  - implemented as a Kubernetes CronJob
  - runs the deferred scheduled process, usually for non-urgent analysis or maintenance work
  - not queue-driven in the hot path

### Supporting infrastructure
- postgres
  - PostgreSQL database for transaction state and history
- kafka
  - Kafka broker for the fraud event stream
- keycloak
  - authentication and token validation support
- kong
  - gateway for routing external requests to the API service
- kafka-ui
  - browser-based Kafka view for debugging and operational visibility

### Additional operational resources
- alerting rules
- Prometheus alert manager configuration
- resilience settings such as anti-affinity and pod disruption budgets
- config maps and secrets wiring

---

## 2. Chart layout

The chart contains the following files:

- Chart.yaml
  - metadata describing the chart
- values.yaml
  - default deployment settings
- values-dev.yaml
  - development profile
- values-prod.yaml
  - production profile
- templates/
  - Kubernetes manifests

The main template files are:

- apps.yaml
  - main application deployments and batch CronJob
- infrastructure.yaml
  - PostgreSQL, Kafka, and associated services
- configmaps-secrets.yaml
  - config map and secret attachments
- gateways-ui.yaml
  - gateway and UI resources
- alerting.yaml
  - alert definitions
- alertmanager.yaml
  - Alertmanager deployment/configuration
- resilience.yaml
  - PD, HPA, and resilience settings
- _helpers.tpl
  - Helm template helper functions

---

## 3. High-level architecture inside Kubernetes

The chart follows the same model as the application itself:

### Realtime path
- requests reach the API service through Kong
- the API validates JWTs and serves transaction queries
- Kafka worker consumes the transaction stream and performs fraud checks
- results are written to PostgreSQL
- history is appended for auditability
- messages that fail validation or processing are sent to the DLQ topic

### Batch path
- the batch job is scheduled using Kubernetes CronJob
- it runs at a configured hour and minute
- it is intended for deferred processing such as cleanup, recalculation, historical review, or heavier maintenance work
- it does not sit in the low-latency request path

This is important: the design intentionally avoids making the real-time API depend on a queue-based batch pipeline.

---

## 4. Core values file

The chart defaults are defined in values.yaml. It is the main place where you tune deployment behavior.

### 4.1 Global values
- global.environment
  - used to identify the runtime environment

### 4.2 Application image settings
The chart defines:
- worker.image.repository
- worker.image.tag
- worker.image.pullPolicy
- api.image.repository
- api.image.tag
- api.image.pullPolicy

These determine which container image is used for the worker and API pods.

### 4.3 Batch settings
The batch job is configured here:

```yaml
batch:
  enabled: true
  runHour: 2
  runMinute: 0
  suspend: false
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
```

These values map to the time-based batch scheduler:
- runHour: hour of day to run
- runMinute: minute of day to run
- enabled: whether the CronJob is included
- suspend: pauses the job schedule when true

The generated CronJob schedule looks like this:

```yaml
schedule: "0 2 * * *"
```

This means "at minute 0, hour 2, every day".

### 4.4 Security values
These values configure JWT validation and Keycloak integration, including:

- jwtIssuer
- jwtAudience
- jwtJwksUrl
- jwtPublicKeyPath
- jwtAlgorithms
- requiredReadScope
- authTokenEndpointEnabled
- keycloak* values for admin and service clients

Important: these values are sensitive in real deployment, so production secrets should be supplied through Kubernetes Secret objects rather than plaintext values.

### 4.5 Kafka and app config values
Important app configuration is also provided here:

- kafkaTopicName
- kafkaDlqTopicName
- kafkaConsumerGroupId
- kafkaProducerAcks
- kafkaProducerEnableIdempotence
- dbPoolMinSize
- dbPoolMaxSize
- defaultHighValueThreshold
- defaultRestrictedCategoriesJson

These settings correspond to the application runtime values used by the FastAPI service and worker.

### 4.6 Observability values
These values control runtime diagnostics and metrics:

- observability.logLevel
- observability.enableTracing
- observability.workerMetricsPort

### 4.7 Persistence values
The chart allows PostgreSQL and Kafka storage to be enabled or disabled and also allows custom storage sizing:

- persistence.postgres.enabled
- persistence.postgres.size
- persistence.postgres.storageClassName
- persistence.kafka.enabled
- persistence.kafka.size
- persistence.kafka.storageClassName

This makes the chart flexible for cluster environments with different storage classes or non-persistent dev environments.

---

## 5. Values profiles

### values-dev.yaml
Used for local development and lower-resource testing.

Key characteristics:
- single replica deployments
- image pull policy: Never for local builds
- NodePort rather than cluster ingress for Kong
- fewer replicas and simpler setup

### values-prod.yaml
Used for production-style deployment.

Key characteristics:
- multiple API and worker replicas
- stricter ingress settings
- cert-manager enabled
- production alerting and higher resilience settings
- anti-affinity and scaling rules enabled

---

## 6. Secret model

The chart expects a Kubernetes Secret called `fraud-secrets` by default via:

```yaml
secrets:
  existingSecretName: fraud-secrets
```

This secret is expected to contain values such as:

- postgres-password
- keycloak-token-client-secret
- keycloak-service-client-secret
- keycloak-demo-password

This is a good pattern because sensitive runtime values should not be committed to Git or placed directly into Helm values files.

When you deploy in a real cluster, you should create the secret outside the chart with a secure source such as:

- Kubernetes Secret manifest
- Vault
- external secret controller
- sealed secrets

---

## 7. Secret and config map wiring

The chart uses ConfigMap and Secret injection for configuration:

### ConfigMap
The file templates/configmaps-secrets.yaml creates a ConfigMap called fraud-configs that includes:
- rules.json
- kong.yml.tpl

These files are mounted into the application containers so runtime behavior can be configured without rebuilding the image.

### Secret injection
The application containers reference values from the existing secret name, especially for:
- database password
- Keycloak client secrets
- demo user password

This prevents secrets from being visible directly in YAML manifests.

---

## 8. How the workloads are defined

### 8.1 API Deployment
The API deployment:
- runs uvicorn app.main:app
- exposes port 8000
- has liveness and readiness probes over /health
- injects environment variables for Kafka, JWT, and Database settings
- mounts the rules JSON file

This is the user-facing service used for transaction query endpoints.

### 8.2 Worker Deployment
The worker deployment:
- starts with the command: python -u -m app.worker
- reads Kafka configuration and app config
- consumes fraud-messages topic events
- performs real-time fraud evaluation
- writes to the PostgreSQL database and DLQ topic
- exports metrics on the worker metrics port

### 8.3 Batch CronJob
The batch worker is configured separately as a CronJob, not a long-running deployment.

This matches the Python implementation in app/batch_worker.py, which:
- reads the batch settings from environment
- exits if disabled
- otherwise waits until the configured run time and then runs once per day

This is the correct pattern when the job is scheduled, not when it is a realtime streaming consumer.

---

## 9. Infrastructure resources

### PostgreSQL
Defined in templates/infrastructure.yaml.

It creates:
- a PVC if persistence is enabled
- a PostgreSQL deployment
- a service named postgres

The database is used for:
- latest fraud state
- transaction history
- supporting query endpoints

### Kafka
Also defined in templates/infrastructure.yaml.

It creates:
- PVC if enabled
- Kafka broker deployment
- Kafka service named kafka

The broker is configured with the expected Kafka durability settings and exposes ports 9092 and 9093 internally.

### Kong
The chart includes a gateway layer to route external traffic to the API service.

The gateway configuration is defined in:
- templates/gateways-ui.yaml
- values.yaml under kong

It enables:
- route definitions
- rate limiting
- correlation-id plugin setup

This is useful for exposing the service behind a single API front door without exposing the app directly.

### Kafka UI
The chart includes a UI for Kafka debugging and operational review. It is useful for inspecting topics and messages without needing a separate local admin tool.

---

## 10. Resilience and availability

The chart includes resilience settings in values.yaml and templates/resilience.yaml.

Examples include:
- pod anti-affinity preference
- pod disruption budgets
- autoscaling settings
- topology preferences

This is important for production use because it keeps the API and worker workloads spread across nodes and reduces disruption risk.

---

## 11. Alerting and operations

The chart includes alerting definitions in:
- alerting.yaml
- alertmanager.yaml

These are designed to monitor:
- PostgreSQL health
- Kafka health
- application error conditions
- alert routing for support or developers

This is aligned with the monitoring model described in the project README and helps the service remain operationally observable.

---

## 12. How to install the chart

From the repository root:

```powershell
helm dependency update .\fraud-detection
helm upgrade --install fraud-system .\fraud-detection `
  -f .\fraud-detection\values.yaml `
  -f .\fraud-detection\values-dev.yaml `
  -n fraud-system --create-namespace
```

For production:

```powershell
helm upgrade --install fraud-system .\fraud-detection `
  -f .\fraud-detection\values.yaml `
  -f .\fraud-detection\values-prod.yaml `
  -n fraud-system --create-namespace
```

---

## 13. How to check what was deployed

Useful commands:

```powershell
kubectl get pods -n fraud-system
kubectl get cronjobs -n fraud-system
kubectl get deployments -n fraud-system
kubectl get services -n fraud-system
kubectl get pvc -n fraud-system
```

To see the batch schedule:

```powershell
kubectl get cronjob fraud-batch-worker -n fraud-system -o yaml
```

To view logs:

```powershell
kubectl logs -n fraud-system deploy/fraud-api
kubectl logs -n fraud-system deploy/fraud-worker
kubectl logs -n fraud-system job/<batch-job-name>
```

---

## 14. Image registry configuration for production

The chart supports pulling images from GitHub Container Registry (ghcr.io) or other container registries using Kubernetes image pull secrets.

### GitHub Container Registry (ghcr.io) setup

For production deployments using GitHub Container Registry:

1. **Build and push images to ghcr.io:**

```bash
# Build and push fraud-worker image
docker build -f Dockerfile.worker -t ghcr.io/siyablela/fraud-detection/fraud-worker:1.0.0 .
docker push ghcr.io/siyablela/fraud-detection/fraud-worker:1.0.0

# Build and push fraud-api image
docker build -f Dockerfile.api -t ghcr.io/siyablela/fraud-detection/fraud-api:1.0.0 .
docker push ghcr.io/siyablela/fraud-detection/fraud-api:1.0.0
```

2. **For private ghcr.io repos, create image pull secret in your cluster:**

```bash
kubectl create secret docker-registry ghcr-credentials \
  --docker-server=ghcr.io \
  --docker-username=<github-username> \
  --docker-password=<github-token> \
  --docker-email=<email> \
  -n fraud-system
```

3. **Update values-prod.yaml (if using private repos):**

```yaml
imagePullSecrets:
  - name: ghcr-credentials

worker:
  image:
    repository: ghcr.io/siyablela/fraud-detection/fraud-worker
    tag: "1.0.0"
    pullPolicy: Always

api:
  image:
    repository: ghcr.io/siyablela/fraud-detection/fraud-api
    tag: "1.0.0"
    pullPolicy: Always
```

4. **Deploy with Helm:**

```bash
helm upgrade --install fraud-system ./fraud-detection \
  -f ./fraud-detection/values.yaml \
  -f ./fraud-detection/values-prod.yaml \
  -n fraud-system --create-namespace
```

### Alternative: Private registries (ACR, ECR, etc.)

For Azure Container Registry or other private registries, update `values-prod.yaml`:

```yaml
imagePullSecrets:
  - name: acr-credentials  # or your registry credentials secret name

worker:
  image:
    repository: myregistry.azurecr.io/fraud-worker  # Update to your registry
    tag: "1.0.0"
    pullPolicy: Always

api:
  image:
    repository: myregistry.azurecr.io/fraud-api  # Update to your registry
    tag: "1.0.0"
    pullPolicy: Always
```

### Important notes

- **Public ghcr.io repos**: If your repository is public, `imagePullSecrets` can remain empty (`[]`)
- **Private repos**: Only required if your ghcr.io repo is private
- **Use semantic versioning**: Always use versioned tags (e.g., `v1.0.0`) in production, never `latest`
- **Use `pullPolicy: Always`**: Ensures images are always pulled from the registry for consistency
- **Third-party images**: Postgres, Kafka, Keycloak, Kong are pulled from public registries (Docker Hub, Quay.io, etc.) automatically
- **Image pull secret must exist** before deployment if configured

---

## 15. Common customization points

If you want to tune the deployment, the most common places to edit are:

- image repository and tag in values.yaml
- replica counts for API and worker
- batch schedule in the batch block
- security and Keycloak values
- Kafka topic names and DB pool settings
- persistence sizes and storage class
- ingress and TLS settings for production

---

## 15. Why this chart is structured this way

The chart mirrors the application design rather than creating a generic Kubernetes deployment for everything. That is intentional.

It keeps the separation clear:
- hot path is the API + worker + Kafka
- deferred path is the CronJob
- state lives in PostgreSQL
- infrastructure lives in the supporting K8s resources

This makes the system easier to reason about, easier to operate, and easier to discuss in interviews or architecture reviews.

---

## 16. Practical advice

If you are new to Helm, think of the chart like this:

- values.yaml = the configuration you control
- templates/*.yaml = the actual Kubernetes objects that get rendered
- helm install/upgrade = turns configuration into live cluster objects

The most important thing to remember is that Helm does not magically know your app; it only renders YAML based on the values you give it. That is why the chart needs careful values for secrets, runtime settings, and scheduling.

---

## 17. Summary

This Helm chart is meant to package the Fraud Detection service into a deployable Kubernetes environment while preserving the core architecture:

- realtime processing remains in the live worker path
- scheduled work is handled by a CronJob
- database and Kafka are deployed as first-class infrastructure
- auth and secrets are treated as externalized runtime configuration

That gives you a clean production-ready shape without overcomplicating the application.

---

## 18. Recommended next improvements

If you want to mature this chart further, the next great improvements would be:

- add explicit resource requests and limits for all pods
- add namespace-aware values and helper templates
- add a secret template for `fraud-secrets`
- add network policies
- add readiness checks for batch jobs and worker health
- split dev and prod profiles into clearer environment-specific defaults
- add a chart-level README generated from values documentation

This would make the Helm chart more production-safe and easier for someone new to Kubernetes to understand.
