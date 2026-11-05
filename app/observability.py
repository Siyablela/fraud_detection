"""Structured logging, correlation IDs, Prometheus metrics, and optional tracing.

Logs are one JSON object per line on stdout, for the platform to collect. Metrics
are only touched through the small functions at the bottom of this module, so
metric names and labels live in one place, and every label has a small, fixed set
of values (route templates, never raw paths or ids).
"""

import logging
import re
import time
from collections.abc import Iterable
from contextvars import ContextVar
from typing import Any
from uuid import uuid4

import structlog
from fastapi import FastAPI, Request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from starlette.routing import Match, Route

CORRELATION_ID_HEADER = "x-correlation-id"
REQUEST_ID_HEADER = "x-request-id"
# Caller-supplied ids are echoed into logs and response headers; this pattern keeps
# out newlines and other characters that could forge log lines or headers.
_SAFE_ID = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")
_REDACTED = "[REDACTED]"

# A ContextVar is per asyncio task, so concurrent requests and partition tasks each
# see their own correlation id.
_correlation_id_context: ContextVar[str | None] = ContextVar("correlation_id", default=None)

_http_requests_total = Counter(
    "fraud_http_requests_total",
    "Total HTTP requests handled by fraud services",
    ["service", "method", "route", "status_code"],
)
_http_request_latency_seconds = Histogram(
    "fraud_http_request_duration_seconds", "HTTP request latency in seconds", ["service", "method", "route"]
)
_worker_messages_total = Counter(
    "fraud_worker_messages_total",
    "Kafka messages finished by the worker, by outcome (evaluated, duplicate, dlq)",
    ["service", "topic", "status"],
)
_worker_retries_total = Counter(
    "fraud_worker_retries_total", "Processing attempts that failed and were retried", ["service", "failure_class"]
)
_worker_decisions_total = Counter(
    "fraud_worker_decisions_total", "Fraud decisions made by the worker, by verdict", ["service", "verdict"]
)
_worker_rule_triggers_total = Counter(
    "fraud_worker_rule_triggers_total", "Times each fraud rule fired", ["service", "rule"]
)
_worker_message_latency_seconds = Histogram(
    "fraud_worker_message_duration_seconds", "Worker message processing time in seconds", ["service", "topic"]
)
_worker_partition_backlog = Gauge(
    "fraud_worker_partition_backlog", "Messages queued in the worker per partition", ["service", "partition"]
)
_worker_partition_blocked_seconds = Gauge(
    "fraud_worker_partition_blocked_seconds",
    "How long the partition's in-flight message has been failing; 0 when it is not",
    ["service", "partition"],
)
_rule_cache_lookups_total = Counter(
    "fraud_rule_cache_lookups_total", "Active rule-set lookups by the layer that answered", ["layer"]
)
_rule_set_rejected_total = Counter(
    "fraud_rule_set_rejected_total",
    "Loads of the active rule set refused by the integrity check; scoring stops while this rises",
    ["rule_set_key"],
)
_rate_limiter_errors_total = Counter(
    "fraud_rate_limiter_errors_total", "Rate-limit checks that failed open because Redis was unavailable"
)
_database_up = Gauge("fraud_database_up", "Database reachability (1=up, 0=down)", ["service"])
_kafka_up = Gauge("fraud_kafka_up", "Kafka connectivity (1=up, 0=down)", ["service", "component"])


# --- Correlation IDs ----------------------------------------------------------


def sanitise_correlation_id(value: object) -> str | None:
    """Accept a caller-supplied ID only if it is short and log-safe."""
    if isinstance(value, str) and _SAFE_ID.match(value):
        return value
    return None


def get_correlation_id() -> str | None:
    """The correlation id bound to the current request or message, if any."""
    return _correlation_id_context.get()


def bind_correlation_id(correlation_id: str | None = None) -> str:
    """Bind ``correlation_id`` (or a new one) to the current context and its logs."""
    resolved = sanitise_correlation_id(correlation_id) or str(uuid4())
    _correlation_id_context.set(resolved)
    structlog.contextvars.bind_contextvars(correlation_id=resolved)
    return resolved


def bind_log_context(**fields: str) -> None:
    """Add ``fields`` to every log line written in the current context until it is cleared."""
    structlog.contextvars.bind_contextvars(**fields)


def clear_log_context() -> None:
    """Forget the previous request's or message's context; called at the start of each one."""
    _correlation_id_context.set(None)
    structlog.contextvars.clear_contextvars()


# --- Logging ------------------------------------------------------------------


def _redaction_processor(sensitive_keys: Iterable[str]):
    """Mask the value of any key named in ``sensitive_keys`` (case-insensitive), at any depth."""
    keys = frozenset(key.lower() for key in sensitive_keys)

    def redact(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _REDACTED if str(k).lower() in keys else redact(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [redact(item) for item in value]
        return value

    def processor(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        return redact(event_dict)

    return processor


def _service_processor(service_name: str):
    """Stamp ``service`` on every record.

    A processor rather than a context variable: the worker and API clear the log
    context for every message and request, which would otherwise drop it too.
    """

    def processor(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        event_dict["service"] = service_name
        return event_dict

    return processor


def configure_logging(service_name: str, level: str = "INFO", redact_fields: Iterable[str] = ()) -> None:
    """Emit one JSON object per line on stdout, for collection by the platform.

    Every record carries ``service``, ``level``, ``timestamp`` and, when bound,
    ``correlation_id``. Keys listed in ``redact_fields`` are masked at any depth.
    """
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        _service_processor(service_name),
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _redaction_processor(redact_fields),
    ]
    handler = logging.StreamHandler()
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
    )
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    # Uvicorn's access log duplicates http_request_completed without correlation IDs.
    logging.getLogger("uvicorn.access").disabled = True

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared_processors,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str):
    """A structlog logger; output format and processors come from :func:`configure_logging`."""
    return structlog.get_logger(name)


# --- HTTP instrumentation -----------------------------------------------------


def _resolved_route_template(request: Request) -> str:
    """Return the matched route template (``/x/{id}``), never the raw path.

    Raw paths would put identifiers into metric labels: a data leak via /metrics and
    unbounded label cardinality. Unmatched paths share one ``<unmatched>`` bucket.
    """
    scoped_route = request.scope.get("route")
    if isinstance(scoped_route, Route):
        return scoped_route.path
    for route in request.app.routes:
        if isinstance(route, Route):
            match, _ = route.matches(request.scope)
            if match == Match.FULL:
                return route.path
    return "<unmatched>"


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Bind request/correlation IDs, and record a metric and a log line per request."""

    def __init__(self, app: FastAPI, service_name: str):
        super().__init__(app)
        self.service_name = service_name

    async def dispatch(self, request: Request, call_next):
        clear_log_context()
        request_id = sanitise_correlation_id(request.headers.get(REQUEST_ID_HEADER)) or str(uuid4())
        correlation_id = bind_correlation_id(request.headers.get(CORRELATION_ID_HEADER))
        structlog.contextvars.bind_contextvars(request_id=request_id)
        start_time = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers[REQUEST_ID_HEADER] = request_id
            response.headers[CORRELATION_ID_HEADER] = correlation_id
            return response
        finally:
            elapsed = time.perf_counter() - start_time
            route = _resolved_route_template(request)
            _http_requests_total.labels(self.service_name, request.method, route, str(status_code)).inc()
            _http_request_latency_seconds.labels(self.service_name, request.method, route).observe(elapsed)
            log = get_logger("fraud.http")
            log_method = log.warning if status_code >= 500 else log.info
            if route not in {"/live", "/ready", "/metrics"} or status_code >= 400:
                log_method(
                    "http_request_completed",
                    method=request.method,
                    route=route,
                    status_code=status_code,
                    duration_ms=round(elapsed * 1000, 2),
                )
            clear_log_context()


def install_fastapi_observability(app: FastAPI, service_name: str) -> None:
    """Add per-request logging and metrics, and expose ``/metrics`` for Prometheus.

    ``/metrics`` has no authentication: it is scraped inside the cluster, and the Kong
    gateway does not route it from outside.
    """
    app.add_middleware(RequestContextMiddleware, service_name=service_name)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


def setup_tracing(service_name: str, endpoint: str, app: FastAPI | None = None) -> None:
    """Export OpenTelemetry traces over OTLP/HTTP when an endpoint is configured."""
    if not endpoint:
        get_logger(__name__).warning("tracing_enabled_without_endpoint", service_name=service_name)
        return
    # Imported here, not at module level: tracing is off by default, and these
    # imports are only worth their startup cost when it is switched on.
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    if not isinstance(trace.get_tracer_provider(), TracerProvider):
        provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
        trace.set_tracer_provider(provider)
    if app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)


# --- Worker and dependency metrics -------------------------------------------
# Thin wrappers, so the rest of the code never handles Prometheus objects or label names.


def worker_message_timer(service_name: str, topic: str):
    return _worker_message_latency_seconds.labels(service=service_name, topic=topic).time()


def worker_message_outcome(service_name: str, topic: str, status: str) -> None:
    _worker_messages_total.labels(service=service_name, topic=topic, status=status).inc()


def worker_retry(service_name: str, failure_class: str) -> None:
    _worker_retries_total.labels(service=service_name, failure_class=failure_class).inc()


def worker_fraud_decision(service_name: str, verdict: str, triggered_rules: Iterable[str]) -> None:
    _worker_decisions_total.labels(service=service_name, verdict=verdict).inc()
    for rule in triggered_rules:
        _worker_rule_triggers_total.labels(service=service_name, rule=rule).inc()


def set_partition_backlog(service_name: str, partition: int, size: int) -> None:
    _worker_partition_backlog.labels(service=service_name, partition=str(partition)).set(size)


def set_partition_blocked(service_name: str, partition: int, seconds: float) -> None:
    _worker_partition_blocked_seconds.labels(service=service_name, partition=str(partition)).set(seconds)


def remove_partition_metrics(service_name: str, partition: int) -> None:
    """Drop a released partition's series so a stale value cannot keep an alert firing."""
    for gauge in (_worker_partition_backlog, _worker_partition_blocked_seconds):
        try:
            gauge.remove(service_name, str(partition))
        except KeyError:
            pass


def record_rule_cache_lookup(layer: str) -> None:
    _rule_cache_lookups_total.labels(layer=layer).inc()


def record_rule_set_rejected(rule_set_key: str) -> None:
    _rule_set_rejected_total.labels(rule_set_key=rule_set_key).inc()


def record_rate_limiter_error() -> None:
    _rate_limiter_errors_total.inc()


def set_database_health(service_name: str, is_healthy: bool) -> None:
    _database_up.labels(service=service_name).set(1 if is_healthy else 0)


def set_kafka_health(service_name: str, component: str, is_healthy: bool) -> None:
    _kafka_up.labels(service=service_name, component=component).set(1 if is_healthy else 0)
