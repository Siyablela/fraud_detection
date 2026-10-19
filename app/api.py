from contextlib import asynccontextmanager
from math import ceil
from types import SimpleNamespace
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.rate_limit import PrincipalRateLimitMiddleware
from app.database import database_pool, get_transaction as find_transaction
from app.database import get_transactions_by_category as find_by_category
from app.database import ping_database
from app.observability import (
    apply_tracing,
    configure_logging,
    get_correlation_id,
    get_logger,
    install_fastapi_observability,
    setup_tracing,
)
from app.security import AuthenticatedPrincipal, get_current_principal
from app.settings import get_settings
from app.token_service import (
    IdentityProviderRejectedError,
    IdentityProviderUnavailableError,
    InvalidClientCredentialsError,
    InvalidCredentialsError,
    KeycloakTokenService,
)

SERVICE_NAME = "fraud-query-api"
router = APIRouter()

logger = get_logger(__name__)


class TransactionResponse(BaseModel):
    transaction_id: str
    user_id: str
    amount: Decimal
    category: str
    timestamp: float
    is_fraud: bool
    triggered_rules: list[str]
    correlation_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CategoryPageResponse(BaseModel):
    category: str
    page: int
    page_size: int
    count: int
    total_count: int
    total_pages: int
    has_previous: bool
    has_more: bool
    previous_page: int | None = None
    next_page: int | None = None
    data: list[TransactionResponse]
    correlation_id: str | None = None


class TokenExchangeRequest(BaseModel):
    username: str
    password: str
    client_id: str | None = None
    scope: str | None = None


class TokenExchangeResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int | None = None
    refresh_token: str | None = None
    refresh_expires_in: int | None = None
    scope: str | None = None


class ServiceTokenRequest(BaseModel):
    client_id: str | None = None
    client_secret: str | None = None
    scope: str | None = None


async def require_transaction_read_scope(
    principal: AuthenticatedPrincipal = Depends(get_current_principal),
) -> AuthenticatedPrincipal:
    """Ensure the caller has the transaction read scope required by the API.

    Args:
        principal: The authenticated caller principal extracted from the bearer token.

    Returns:
        AuthenticatedPrincipal: The validated principal after confirming the required
            scope is present.

    Raises:
        HTTPException: If the token is missing the required read scope.
    """
    required_scope = get_settings().jwt_required_scope_for_transaction_read
    if required_scope not in principal.scopes:
        raise HTTPException(
            status_code=403,
            detail="Access token does not include the required scope.",
            headers={
                "WWW-Authenticate": f'Bearer error="insufficient_scope", scope="{required_scope}"'
            },
        )
    return principal

@asynccontextmanager 
async def lifespan(app: FastAPI):
    """Initiates app state and optional tracing on startup, and cleans up resources on shutdown.

    Args: 
        app: The FastAPI application instance being started. 

    Yields: 
        None: The application remains running while its startup resources are available. 
    """ 
    settings = get_settings() 
    configure_logging( SERVICE_NAME, settings.observability_log_level, ) 
    if settings.observability_enable_tracing: 
        setup_tracing(SERVICE_NAME)
        apply_tracing(app)
    async with database_pool() as pool:  
        app.state.database = pool
        try:  
            await _validate_database_ready(pool) 
            logger.info("startup_dependency_check_passed") 
        except Exception:
            logger.exception( "startup_dependency_check_failed", detail="database is unavailable", ) 
            raise 
        yield


def create_app() -> FastAPI:
    """Build and configure the FastAPI application instance.

    Returns:
        FastAPI: A configured application with middleware, tracing, and route registration.
    """
    settings = get_settings()
    app = FastAPI(title="Fraud Detection Query Engine API", lifespan=lifespan)
    install_fastapi_observability(app, SERVICE_NAME)
    # Rate limiting middleware (inner). Added before CORS to ensure that rate limits are applied before any CORS headers are added to the response.
    if settings.rate_limit_enabled:
        app.add_middleware(
            PrincipalRateLimitMiddleware,
            max_requests=settings.rate_limit_max_requests,
            window_seconds=settings.rate_limit_window_seconds,
            burst_capacity=settings.rate_limit_burst_capacity,
        )
    # CORS middleware (outer). Added after rate limiting to ensure that CORS headers are applied to all responses, including those that may be rate-limited.
    # Credentials (cookies/basic-auth/client-certs) are intentionally not enabled.
    # This is a stateless Bearer-token API, amd a Bearer header set by js is a 
    # normal header (allowed via allow_headers), not a CORS "credential".
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allowed_origins,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    app.include_router(router)
    return app

def _database_from_request(request: Request | SimpleNamespace):
    """Return the shared database pool from a FastAPI request or test double.

    Args:
        request: A FastAPI request object or a lightweight test double exposing an app
            state.

    Returns:
        Any: The application's database session factory attached to request state.
    """
    return request.app.state.database


async def _validate_database_ready(database) -> None:
    """Check that the database is reachable before the app is considered ready for traffic."""
    await ping_database(database)


@router.get("/live")
async def live() -> dict[str, str]:
    """Return a lightweight liveness signal indicating the process is running."""
    logger.info("liveness_check_passed")
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request):
    """Return the service readiness state only when the database is reachable."""
    try:
        await _validate_database_ready(_database_from_request(request))
        logger.info("readiness_check_passed")
        return {"status": "ready"}
    except Exception:
        logger.exception("readiness_check_failed", detail="database is unavailable")
        raise HTTPException(status_code=503, detail="Database is unavailable")


@router.get("/health")
async def health(request: Request):
    """Return the application health status after confirming the database is reachable.

    Args:
        request: The incoming HTTP request.

    Returns:
        dict[str, str]: A health payload with the service status.

    Raises:
        HTTPException: If the database cannot be reached and the service is unhealthy.
    """
    try:
        await _validate_database_ready(_database_from_request(request))
        logger.info("health_check_passed")
        return {"status": "ok"}
    except Exception:
        logger.exception("health_check_failed", detail="database is unavailable")
        raise HTTPException(status_code=503, detail="Database is unavailable")

@router.get("/api/v1/transactions/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(
    transaction_id: str,
    request: Request,
    _principal=Depends(require_transaction_read_scope),
):
    """Return a single transaction by identifier for authorized callers.

    Args:
        transaction_id: The unique ID of the transaction to fetch.
        request: The incoming HTTP request.
        _principal: The caller principal validated by the read-scope dependency.

    Returns:
        TransactionResponse: The transaction payload with the active correlation ID attached.

    Raises:
        HTTPException: If no matching transaction is found.
    """
    # The scope dependency enforces read access before any lookup runs.
    logger.info("fetch_transaction", transaction_id=transaction_id)
    transaction = await find_transaction(_database_from_request(request), transaction_id)
    if not transaction:
        logger.warning("transaction_not_found", transaction_id=transaction_id)
        raise HTTPException(status_code=404, detail="Transaction records not located.")
    response_payload = dict(transaction)
    response_payload["correlation_id"] = get_correlation_id()
    return response_payload

@router.get("/api/v1/categories/{category_name}", response_model=CategoryPageResponse)
async def get_transactions_by_category(
    category_name: str,
    request: Request,
    page: int = Query(1, ge=1, description="1-based page number to fetch."),
    page_size: int = Query(100, ge=1, le=1000, description="Number of records to fetch per page."),
    _principal=Depends(require_transaction_read_scope),
):
    """Return a paginated category view with metadata and correlation details.

    Args:
        category_name: The category to filter transactions by.
        request: The incoming HTTP request.
        page: The 1-based page number to fetch.
        page_size: Max number of records requested per page.
        _principal: The caller principal validated by the read-scope dependency.

    Returns:
        CategoryPageResponse: Paginated category result metadata and the matching transaction rows.
    """
    offset = (page - 1) * page_size
    logger.info(
        "fetch_category_transactions",
        category_name=category_name,
        page=page,
        page_size=page_size,
    )
    results, total_count = await find_by_category(
        _database_from_request(request),
        category_name,
        offset,
        page_size,
    )
    total_pages = ceil(total_count / page_size) if total_count else 0
    has_more = offset + len(results) < total_count
    return {
        "category": category_name,
        "page": page,
        "page_size": page_size,
        "count": len(results),
        "total_count": total_count,
        "total_pages": total_pages,
        "has_previous": page > 1,
        "has_more": has_more,
        "previous_page": page - 1 if page > 1 else None,
        "next_page": page + 1 if has_more else None,
        "data": results,
        "correlation_id": get_correlation_id(),
    }


@router.post("/api/v1/auth/token", response_model=TokenExchangeResponse)
async def exchange_token_for_testing(payload: TokenExchangeRequest) -> TokenExchangeResponse:
    """Exchange a username/password pair for a Keycloak access token when enabled.

    Args:
        payload: A request containing the username, password, optional client ID, and scope.

    Returns:
        TokenExchangeResponse: The access token payload from the identity provider.

    Raises:
        HTTPException: If the endpoint is disabled or the identity provider rejects the login.
    """
    settings = get_settings()
    if not settings.auth_token_endpoint_enabled:
        raise HTTPException(status_code=404, detail="Not found")

    service = KeycloakTokenService()
    try:
        token_result = await service.exchange_token(
            username=payload.username,
            password=payload.password,
            client_id=payload.client_id,
            scope=payload.scope,
        )
    except InvalidCredentialsError:
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    except IdentityProviderUnavailableError:
        raise HTTPException(
            status_code=502,
            detail="Unable to reach identity provider token endpoint.",
        )
    except IdentityProviderRejectedError:
        raise HTTPException(status_code=502, detail="Identity provider rejected token request.")

    return TokenExchangeResponse(
        access_token=token_result.access_token,
        token_type=token_result.token_type,
        expires_in=token_result.expires_in,
        refresh_token=token_result.refresh_token,
        refresh_expires_in=token_result.refresh_expires_in,
        scope=token_result.scope,
    )


@router.post("/api/v1/auth/service-token", response_model=TokenExchangeResponse)
async def exchange_service_token_for_testing(payload: ServiceTokenRequest) -> TokenExchangeResponse:
    """Exchange client credentials for a service token when the testing endpoint is enabled.

    Args:
        payload: A request containing the optional client ID, client secret, and scope.

    Returns:
        TokenExchangeResponse: The service access token payload from the identity provider.

    Raises:
        HTTPException: If the endpoint is disabled or the identity provider rejects the request.
    """
    settings = get_settings()
    if not settings.auth_token_endpoint_enabled:
        raise HTTPException(status_code=404, detail="Not found")

    service = KeycloakTokenService()
    try:
        token_result = await service.exchange_service_token(
            client_id=payload.client_id,
            client_secret=payload.client_secret,
            scope=payload.scope or settings.keycloak_service_token_scope,
        )
    except InvalidClientCredentialsError:
        raise HTTPException(status_code=401, detail="Invalid client credentials.")
    except IdentityProviderUnavailableError:
        raise HTTPException(
            status_code=502,
            detail="Unable to reach identity provider token endpoint.",
        )
    except IdentityProviderRejectedError:
        raise HTTPException(status_code=502, detail="Identity provider rejected token request.")

    return TokenExchangeResponse(
        access_token=token_result.access_token,
        token_type=token_result.token_type,
        expires_in=token_result.expires_in,
        refresh_token=token_result.refresh_token,
        refresh_expires_in=token_result.refresh_expires_in,
        scope=token_result.scope,
    )
