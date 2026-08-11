import logging
import os
import time
from contextvars import ContextVar
from uuid import uuid4

from fastapi import FastAPI, Request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import structlog

_request_id_context: ContextVar[str] = ContextVar("request_id", default="-")
_correlation_id_context: ContextVar[str] = ContextVar("correlation_id", default="-")

_http_requests_total = Counter(
    "fraud_http_requests_total",
    "Total HTTP requests handled by fraud services",
    ["service", "method", "route", "status_code"],
)

_http_request_latency_seconds = Histogram(
    "fraud_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["service", "method", "route"],
)

_worker_messages_total = Counter(
    "fraud_worker_messages_total",
    "Total worker messages processed",
    ["service", "topic", "status"],
)

_worker_decisions_total = Counter(
    "fraud_worker_decisions_total",
    "Total worker fraud decisions",
    ["service", "is_fraud"],
)

_worker_message_latency_seconds = Histogram(
    "fraud_worker_message_duration_seconds",
    "Worker message processing time in seconds",
    ["service", "topic"],
)


class RequestContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id_context.get()
        record.correlation_id = _correlation_id_context.get()
        return True


def get_request_id() -> str:
    return _request_id_context.get()


def get_correlation_id() -> str:
    return _correlation_id_context.get()


def ensure_correlation_id() -> str:
    existing = _correlation_id_context.get()
    if existing not in (None, "-", ""):
        return existing

    correlation_id = str(uuid4())
    _correlation_id_context.set(correlation_id)
    structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
    return correlation_id


def attach_correlation_id(payload: dict | None) -> dict:
    if not isinstance(payload, dict):
        return payload if payload is not None else {}

    correlation_id = get_correlation_id()
    if not correlation_id or correlation_id in (None, "-", ""):
        correlation_id = ensure_correlation_id()

    payload.setdefault("correlation_id", correlation_id)
    return payload


def extract_correlation_id(payload: dict | None) -> str | None:
    if not isinstance(payload, dict):
        return None

    correlation_id = payload.get("correlation_id")
    if correlation_id not in (None, "", "-"):
        return str(correlation_id)
    return None


class ServiceNameFilter(logging.Filter):
    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "service"):
            record.service = self.service_name
        return True


class MetricsAndRequestIdMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI, service_name: str):
        super().__init__(app)
        self.service_name = service_name

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id") or str(uuid4())
        correlation_id = str(uuid4())

        request_token = _request_id_context.set(request_id)
        correlation_token = _correlation_id_context.set(correlation_id)
        structlog.contextvars.bind_contextvars(request_id=request_id, correlation_id=correlation_id)
        start_time = time.perf_counter()
        status_code = 500
        route = request.url.path
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["x-request-id"] = request_id
            response.headers["x-correlation-id"] = correlation_id
            return response
        finally:
            elapsed = time.perf_counter() - start_time
            if request.scope.get("route") and getattr(request.scope["route"], "path", None):
                route = request.scope["route"].path

            _http_requests_total.labels(
                service=self.service_name,
                method=request.method,
                route=route,
                status_code=str(status_code),
            ).inc()
            _http_request_latency_seconds.labels(
                service=self.service_name,
                method=request.method,
                route=route,
            ).observe(elapsed)
            get_logger("fraud.http").info(
                "http_request_completed",
                method=request.method,
                route=route,
                status_code=status_code,
                duration_ms=round(elapsed * 1000, 2),
                request_id=request_id,
                correlation_id=correlation_id,
            )
            structlog.contextvars.clear_contextvars()
            _request_id_context.reset(request_token)
            _correlation_id_context.reset(correlation_token)


def configure_logging(service_name: str, level: str = "INFO") -> None:
    root_logger = logging.getLogger()
    if getattr(root_logger, "_fraud_logging_configured", False):
        return

    log_level = getattr(logging, level.upper(), logging.INFO)

    # Keep the processor list shared between structlog and standard-library logging output.
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
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
    handler.addFilter(RequestContextFilter())
    handler.addFilter(ServiceNameFilter(service_name))

    root_logger.handlers = [handler]
    root_logger.setLevel(log_level)
    root_logger._fraud_logging_configured = True

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

    structlog.contextvars.bind_contextvars(service=service_name)


def get_logger(name: str):
    return structlog.get_logger(name)


def install_fastapi_observability(app: FastAPI, service_name: str) -> None:
    app.add_middleware(MetricsAndRequestIdMiddleware, service_name=service_name)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        payload = generate_latest()
        return Response(content=payload, media_type=CONTENT_TYPE_LATEST)


def setup_tracing(service_name: str) -> None:
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if not endpoint:
        return

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except Exception:
        get_logger(__name__).warning(
            "tracing_dependencies_missing",
            detail="Tracing is enabled through environment variables, but OpenTelemetry dependencies are missing.",
            service_name=service_name,
        )
        return

    if isinstance(trace.get_tracer_provider(), TracerProvider):
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)

    # Return an instrument helper so API apps can opt-in without importing otel directly.
    def _instrument_fastapi(app: FastAPI) -> None:
        FastAPIInstrumentor.instrument_app(app)

    setattr(apply_tracing, "instrument_fastapi", _instrument_fastapi)


def apply_tracing(app: FastAPI) -> None:
    instrument = getattr(apply_tracing, "instrument_fastapi", None)
    if callable(instrument):
        instrument(app)


def worker_message_timer(service_name: str, topic: str):
    return _worker_message_latency_seconds.labels(service=service_name, topic=topic).time()


def worker_message_outcome(service_name: str, topic: str, status: str) -> None:
    _worker_messages_total.labels(service=service_name, topic=topic, status=status).inc()


def worker_fraud_decision(service_name: str, is_fraud: bool) -> None:
    _worker_decisions_total.labels(service=service_name, is_fraud=str(is_fraud).lower()).inc()