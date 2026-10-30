# app/

| Module | Responsibility |
|---|---|
| [main.py](main.py) | ASGI entry point (`uvicorn app.main:app`) |
| [api.py](api.py) | FastAPI app: routes, lifespan, uniform error handling |
| [schemas.py](schemas.py) | API request/response models |
| [security.py](security.py) | JWT verification, scope checks |
| [rate_limit.py](rate_limit.py) | Redis-backed per-caller rate limit |
| [worker.py](worker.py) | Kafka consumer: per-partition ordering, retries, DLQ, backpressure, rebalancing |
| [processing.py](processing.py) | Parse -> score -> persist for one message; shared with the API dry run |
| [events.py](events.py) | `TransactionCreated` v1 contract |
| [rules.py](rules.py) | Rule-set model, JsonLogic evaluator, scoring and verdicts, default rule set |
| [cache.py](cache.py) | Redis client; layered active-rule-set provider |
| [database.py](database.py) | Schema, engine, repository functions |
| [dlq.py](dlq.py) | Dead-letter publishing (original bytes + failure headers) |
| [kafka_config.py](kafka_config.py) | librdkafka consumer/producer configuration and security |
| [errors.py](errors.py) | Typed errors and transient/permanent/unexpected classification |
| [observability.py](observability.py) | JSON logging, correlation IDs, Prometheus metrics, tracing |
| [settings.py](settings.py) | Environment configuration with fail-fast validation |
| [batch_worker.py](batch_worker.py) | Daily summary (`--once` for a CronJob, or resident) |
| [healthcheck.py](healthcheck.py) | Container health probe for the API and worker |

Dependencies point one way: `api` / `worker` -> `processing` -> `rules`, `database`,
`cache` -> `settings`, `errors`, `observability`. `rules` and `events` have no I/O.
