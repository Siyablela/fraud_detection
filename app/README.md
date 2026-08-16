# Application package overview

This package contains the Fraud Detection service runtime. It is split into a small set of clear responsibilities:

- API service for read-only transaction lookups and auth validation
- Kafka worker for real-time transaction fraud evaluation
- scheduled batch worker for deferred processing
- database layer for Postgres persistence and querying
- rules engine for fraud logic
- security and token handling for JWT validation
- observability and metrics wiring

---

## 1. Runtime model

The application has two distinct execution paths:

### Realtime path
- API receives a request
- auth is validated against Keycloak via JWT/JWKS
- a read request is served from PostgreSQL
- Kafka worker consumes live transaction events
- rules are evaluated immediately
- transaction state is stored and history is appended

### Deferred path
- batch worker runs at a configured clock time
- it is deliberately separate from the live hot path
- it is intended for scheduled maintenance or heavier analysis jobs

This separation keeps latency low for user-facing requests and keeps batch work predictable and operationally explicit.

---

## 2. Key modules

### api.py
Handles the HTTP layer and authentication flow. It exposes the transaction endpoints and validates access scopes.

### worker.py
Runs the Kafka consumer loop. It reads transaction events, applies fraud policies, writes results to Postgres, and routes bad messages to the DLQ topic.

### batch_worker.py
Runs a time-triggered batch loop. It waits until the configured time, executes the batch job, and then repeats on the next day.

### database.py
Contains the async SQLAlchemy engine, session factory, transaction upsert logic, history logging, and health checks.

### rule.py
Defines the transaction schema and the fraud evaluation logic.

### security.py
Responsible for JWT validation and incoming principal extraction.

### token_service.py
Talks to Keycloak or the configured identity provider for token exchange and auth flows.

### settings.py
Loads runtime configuration from environment variables and validates the app configuration at startup.

### observability.py
Sets log formatting, tracing hooks, metrics labels, and health status reporting.

### dlq.py
Builds and encodes payloads that are sent to the dead-letter topic.

### kafka_security.py
Builds secure Kafka client configuration for the producer and consumer.

---

## 3. Important design choices

### Database is shared by realtime and query paths
The API and worker both use the same PostgreSQL instance. This is a practical design for a small service but it means both live processing and read queries compete for the same storage layer.

That is acceptable here because the service is intentionally narrow and low scale. If the workload grows, a separate reporting/analytics path or read replica would be a good next step.

### Kafka is used for realtime processing, not the query path
The API is not queue-driven. It reads directly from Postgres. Kafka is used for the worker pipeline and event propagation, not for user-facing transactions.

### Batch job is explicit and time-bound
The batch worker is not intended to be part of the user latency path. It is scheduled and runs on a clock, which makes it easier to reason about and easier to operate in production.

---

## 4. Startup and runtime assumptions

This service assumes:

- PostgreSQL is available and reachable
- Kafka broker is available for the worker
- Keycloak is available for JWT validation and token exchange
- environment variables are populated from .env or the deployment environment
- the app config is validated before startup

---

## 5. Typical workflow

1. Application starts and loads settings.
2. API initializes and checks database readiness.
3. Worker initializes and subscribes to the Kafka fraud topic.
4. A transaction event is consumed.
5. The event is validated and scored.
6. The latest transaction state is upserted into PostgreSQL.
7. The immutable history record is appended.
8. The API serves queries from PostgreSQL.
9. If processing fails, the message is routed to the DLQ topic.

---

## 6. When to change what

Edit this package when you need to change:

- app behavior
- auth rules
- fraud logic
- processing flow
- observability or metrics
- database tables or persistence strategy
- Kafka handling or DLQ behavior

---

## 7. Operational notes

The app is intentionally small and clear, which makes it a good candidate for interviews and architecture discussions. The main tradeoff is that it keeps a simple shared database and a single app boundary. That is a strength for a demonstration project, but it is not a massive-scale distributed design.

---

## 8. Summary

This application package is designed around a simple and understandable architecture:

- FastAPI for access and reads
- Kafka worker for realtime fraud processing
- Postgres for state and audit history
- CronJob for deferred scheduled work
- Keycloak for authentication

The code is intentionally readable and modular so the core logic can be explained clearly and discussed in interviews.
