"""HTTP API (v1): read fraud decisions and manage versioned rule sets."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from app.cache import create_redis_client, ping_redis, write_rule_set_cache
from app.database import (
    PageCursor,
    TransactionFilters,
    create_engine,
    create_rule_set_version,
    create_session_factory,
    get_latest_rule_set,
    get_rule_set_version,
    get_transaction,
    list_rule_set_versions,
    list_transactions,
    ping_database,
)
from app.errors import (
    FailureClass,
    InvalidRuleSetError,
    RuleSetNotFoundError,
    RuleSetVersionConflictError,
    TransactionNotFoundError,
    classify_failure,
)
from app.events import Category
from app.observability import (
    configure_logging,
    get_correlation_id,
    get_logger,
    install_fastapi_observability,
    setup_tracing,
)
from app.processing import score_transaction
from app.rate_limit import RateLimiter
from app.rules import Verdict, content_hash, parse_rule_set_definition
from app.schemas import (
    CreateRuleSetVersionRequest,
    ErrorResponse,
    EvaluateRequest,
    EvaluateResponse,
    RuleSetVersionPage,
    RuleSetVersionResponse,
    TransactionPage,
    TransactionResponse,
    VelocityContext,
)
from app.security import AuthenticatedPrincipal, get_token_verifier, require_scope
from app.settings import get_settings

SERVICE_NAME = "fraud-query-api"
logger = get_logger(__name__)

RuleSetKey = Annotated[str, Path(pattern=r"^[a-z0-9][a-z0-9\-]{1,62}[a-z0-9]$", description="Rule-set key")]

_ERROR_CODES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "conflict",
    415: "unsupported_media_type",
    422: "validation_error",
    429: "rate_limited",
    503: "service_unavailable",
}
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    code: {"model": ErrorResponse, "description": HTTPStatus(code).phrase} for code in (401, 403, 422, 429, 503)
}


def error_response(
    status_code: int,
    message: str,
    details: list[dict[str, str]] | None = None,
    headers: dict[str, str] | None = None,
    code: str | None = None,
) -> JSONResponse:
    body = {
        "error": {
            "code": code or _ERROR_CODES.get(status_code, "error"),
            "message": message,
            "details": details or [],
            "correlation_id": get_correlation_id(),
        }
    }
    return JSONResponse(status_code=status_code, content=body, headers=headers)


class UnhandledErrorMiddleware(BaseHTTPMiddleware):
    """Turn anything unhandled into the standard error body. Stack traces are logged, never returned."""

    async def dispatch(self, request: Request, call_next):
        try:
            return await call_next(request)
        except Exception as exc:
            if classify_failure(exc) is FailureClass.TRANSIENT:
                logger.warning("dependency_unavailable", error_type=type(exc).__name__, path=request.url.path)
                return error_response(
                    503, "A dependency is temporarily unavailable. Retry shortly.", headers={"Retry-After": "5"}
                )
            logger.exception("unhandled_exception", path=request.url.path)
            return error_response(500, "An internal error occurred.", code="internal_error")


def _install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return error_response(exc.status_code, str(exc.detail), headers=getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
        details = [
            {"field": ".".join(str(part) for part in error["loc"]), "message": error["msg"]} for error in exc.errors()
        ]
        return error_response(422, "Request validation failed.", details)

    @app.exception_handler(InvalidRuleSetError)
    async def invalid_rule_set(_request: Request, exc: InvalidRuleSetError) -> JSONResponse:
        return error_response(422, str(exc), exc.details)

    @app.exception_handler(TransactionNotFoundError)
    @app.exception_handler(RuleSetNotFoundError)
    async def not_found(_request: Request, exc: Exception) -> JSONResponse:
        return error_response(404, str(exc))

    @app.exception_handler(RuleSetVersionConflictError)
    async def conflict(_request: Request, exc: RuleSetVersionConflictError) -> JSONResponse:
        return error_response(409, str(exc))


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(SERVICE_NAME, settings.observability_log_level, settings.logs_redact_fields)
    if settings.observability_enable_tracing:
        setup_tracing(SERVICE_NAME, settings.otel_exporter_otlp_endpoint, app)
    get_token_verifier()  # fail at startup, not on the first request, if auth is misconfigured

    engine = create_engine(settings)
    redis_client = create_redis_client(settings)
    app.state.session_factory = create_session_factory(engine)
    app.state.redis = redis_client
    app.state.rate_limiter = (
        RateLimiter(redis_client, settings.rate_limit_requests, settings.rate_limit_window_seconds)
        if settings.rate_limit_enabled
        else None
    )
    logger.info("api_started")
    try:
        yield
    finally:
        await redis_client.aclose()
        await engine.dispose()
        logger.info("api_stopped")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Fraud Detection API",
        version="1.0.0",
        description="Read fraud decisions for processed transactions and manage the rule sets that produce them.",
        lifespan=lifespan,
    )
    _install_error_handlers(app)
    app.add_middleware(UnhandledErrorMiddleware)
    install_fastapi_observability(app, SERVICE_NAME)
    if settings.cors_allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_allowed_origins,
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type", "X-Correlation-ID"],
            expose_headers=["X-Correlation-ID", "X-Request-ID", "Retry-After"],
        )
    app.include_router(health_routes())
    app.include_router(v1_routes())
    return app


# --- Health -------------------------------------------------------------------


def health_routes():
    router = APIRouter(tags=["health"])

    @router.get("/live", summary="Liveness: the process is serving requests")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @router.get(
        "/ready",
        summary="Readiness: PostgreSQL and Redis are reachable",
        responses={503: {"model": ErrorResponse}},
    )
    async def ready(request: Request) -> dict[str, Any]:
        checks: dict[str, str] = {}
        try:
            await ping_database(request.app.state.session_factory, SERVICE_NAME)
            checks["database"] = "ok"
        except Exception as exc:
            logger.warning("readiness_database_failed", error_type=type(exc).__name__)
            checks["database"] = "unavailable"
        try:
            await ping_redis(request.app.state.redis)
            checks["redis"] = "ok"
        except Exception as exc:
            logger.warning("readiness_redis_failed", error_type=type(exc).__name__)
            checks["redis"] = "unavailable"
        failing = [name for name, state in checks.items() if state != "ok"]
        if failing:
            raise HTTPException(status_code=503, detail=f"Not ready: {', '.join(failing)} unavailable.")
        return {"status": "ready", "checks": checks}

    return router


# --- API v1 -------------------------------------------------------------------


def v1_routes():
    # Prefix on the router itself (not include_router) so each route's own path is
    # the full template used for metric labels.
    router = APIRouter(prefix="/api/v1", responses=_ERROR_RESPONSES)
    read_transactions = Depends(require_scope("scope_transactions_read"))
    read_rules = Depends(require_scope("scope_rules_read"))
    write_rules = Depends(require_scope("scope_rules_write"))

    @router.get(
        "/transactions/{transaction_id}",
        response_model=TransactionResponse,
        tags=["transactions"],
        summary="Get the fraud decision for one transaction",
        responses={404: {"model": ErrorResponse}},
    )
    async def get_transaction_by_id(
        request: Request, transaction_id: UUID, _principal: AuthenticatedPrincipal = read_transactions
    ) -> Any:
        record = await get_transaction(request.app.state.session_factory, transaction_id)
        if record is None:
            raise TransactionNotFoundError(f"Transaction {transaction_id} was not found")
        return record

    @router.get(
        "/transactions",
        response_model=TransactionPage,
        tags=["transactions"],
        summary="List fraud decisions, newest first, with optional filters",
        responses={400: {"model": ErrorResponse}},
    )
    async def list_transaction_decisions(
        request: Request,
        user_id: UUID | None = None,
        category: Annotated[Category | None, Query(description="Case-insensitive category")] = None,
        is_fraud: bool | None = None,
        verdict: Verdict | None = None,
        occurred_from: Annotated[AwareDatetime | None, Query(description="Inclusive lower bound (ISO-8601)")] = None,
        occurred_to: Annotated[AwareDatetime | None, Query(description="Exclusive upper bound (ISO-8601)")] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query(max_length=256, description="`next_cursor` from the previous page")] = None,
        _principal: AuthenticatedPrincipal = read_transactions,
    ) -> Any:
        try:
            page_cursor = PageCursor.decode(cursor) if cursor else None
        except ValueError:
            raise HTTPException(status_code=400, detail="cursor is malformed or from a different query.") from None
        filters = TransactionFilters(
            user_id=user_id,
            category=category,
            is_fraud=is_fraud,
            verdict=verdict.value if verdict else None,
            occurred_from=occurred_from,
            occurred_to=occurred_to,
        )
        rows, next_cursor = await list_transactions(request.app.state.session_factory, filters, limit, page_cursor)
        return {"data": rows, "limit": limit, "next_cursor": next_cursor.encode() if next_cursor else None}

    @router.get(
        "/rule-sets/{key}/versions",
        response_model=RuleSetVersionPage,
        tags=["rule sets"],
        summary="List versions of a rule set, newest first",
    )
    async def list_versions(
        request: Request,
        key: RuleSetKey,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
        before_version: Annotated[int | None, Query(ge=2)] = None,
        _principal: AuthenticatedPrincipal = read_rules,
    ) -> Any:
        versions = await list_rule_set_versions(request.app.state.session_factory, key, limit, before_version)
        if not versions and before_version is None:
            raise RuleSetNotFoundError(f"No rule set exists for key '{key}'")
        next_before = versions[-1].version if len(versions) == limit and versions[-1].version > 1 else None
        return {
            "data": [RuleSetVersionResponse.from_rule_set(version) for version in versions],
            "limit": limit,
            "next_before_version": next_before,
        }

    @router.get(
        "/rule-sets/{key}/active",
        response_model=RuleSetVersionResponse,
        tags=["rule sets"],
        summary="Get the active (latest) version of a rule set",
        responses={404: {"model": ErrorResponse}},
    )
    async def get_active_version(
        request: Request, key: RuleSetKey, _principal: AuthenticatedPrincipal = read_rules
    ) -> Any:
        return RuleSetVersionResponse.from_rule_set(await get_latest_rule_set(request.app.state.session_factory, key))

    @router.get(
        "/rule-sets/{key}/versions/{version}",
        response_model=RuleSetVersionResponse,
        tags=["rule sets"],
        summary="Get one version of a rule set",
        responses={404: {"model": ErrorResponse}},
    )
    async def get_version(
        request: Request,
        key: RuleSetKey,
        version: Annotated[int, Path(ge=1)],
        _principal: AuthenticatedPrincipal = read_rules,
    ) -> Any:
        rule_set = await get_rule_set_version(request.app.state.session_factory, key, version)
        return RuleSetVersionResponse.from_rule_set(rule_set)

    @router.post(
        "/rule-sets/{key}/versions",
        response_model=RuleSetVersionResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["rule sets"],
        summary="Publish a new version; it becomes active immediately",
        responses={409: {"model": ErrorResponse}},
    )
    async def create_version(
        request: Request,
        response: Response,
        key: RuleSetKey,
        body: CreateRuleSetVersionRequest,
        principal: AuthenticatedPrincipal = write_rules,
    ) -> Any:
        definition = parse_rule_set_definition(body.definition)
        rule_set = await create_rule_set_version(
            request.app.state.session_factory,
            key=key,
            definition=definition,
            definition_hash=content_hash(definition),
            description=body.description,
            created_by=principal.subject,
            expected_current_version=body.expected_current_version,
        )
        settings = get_settings()
        await write_rule_set_cache(request.app.state.redis, rule_set, settings.rule_redis_cache_ttl_seconds)
        logger.info(
            "rule_set_version_created",
            rule_set=rule_set.label,
            content_hash=rule_set.content_hash,
            created_by=principal.subject,
        )
        response.headers["Location"] = f"/api/v1/rule-sets/{key}/versions/{rule_set.version}"
        return RuleSetVersionResponse.from_rule_set(rule_set)

    @router.post(
        "/rule-sets/{key}/evaluate",
        response_model=EvaluateResponse,
        tags=["rule sets"],
        summary="Dry-run: score a hypothetical transaction against the active version without storing it",
        responses={404: {"model": ErrorResponse}},
    )
    async def evaluate(
        request: Request, key: RuleSetKey, body: EvaluateRequest, _principal: AuthenticatedPrincipal = read_rules
    ) -> Any:
        session_factory = request.app.state.session_factory
        rule_set = await get_latest_rule_set(session_factory, key)
        score = await score_transaction(
            session_factory,
            rule_set,
            transaction_id=body.transaction_id,
            user_id=body.user_id,
            occurred_at=body.occurred_at or datetime.now(UTC),
            amount=body.amount,
            currency=body.currency,
            category=body.category,
            channel=body.channel,
            country=body.country,
            enrichment=body.enrichment.model_dump(exclude_none=True) if body.enrichment else None,
        )
        assessment = score.assessment
        return EvaluateResponse(
            rule_set=rule_set.label,
            risk_score=assessment.risk_score,
            verdict=assessment.verdict,
            is_fraud=assessment.is_fraud,
            triggered_rules=assessment.triggered_rules,
            velocity=VelocityContext(
                window_seconds=rule_set.definition.velocity_window_seconds,
                prior_count=score.prior_count,
                prior_amount=score.prior_amount,
            ),
        )

    return router
