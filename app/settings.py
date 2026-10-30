import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

# Asymmetric algorithms only: accepting HS* would let anyone holding the (public)
# verification material mint tokens, and "none" disables verification entirely.
_ALLOWED_JWT_ALGORITHMS = frozenset({"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512"})


def _read_env(name: str) -> str | None:
    """Read an environment variable, honouring the ``<NAME>_FILE`` secret convention.

    ``<NAME>`` wins when both are set. ``<NAME>_FILE`` lets orchestrators mount
    secrets as files (Docker/Kubernetes secrets) instead of plain environment values.

    Args:
        name: The environment variable name to resolve.

    Returns:
        str | None: The resolved value, or ``None`` when neither form is set.

    Raises:
        RuntimeError: If ``<NAME>_FILE`` points at a file that cannot be read.
    """
    value = os.getenv(name)
    if value not in (None, ""):
        return value

    file_path = os.getenv(f"{name}_FILE")
    if file_path not in (None, ""):
        try:
            return Path(file_path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise RuntimeError(f"Unable to read secret file for {name}_FILE") from exc
    return None


def _split_csv(value: object) -> object:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return value


class Settings(BaseModel):
    """Runtime configuration for every fraud-detection process.

    Required values have no default and fail fast at startup when missing. Nothing
    secret has a default: credentials only ever come from the environment or a
    mounted secret file.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    # --- PostgreSQL -------------------------------------------------------------
    database_url: str = Field(..., alias="DATABASE_URL")
    db_pool_size: int = Field(default=10, ge=1, alias="DB_POOL_SIZE")
    db_pool_max_overflow: int = Field(default=5, ge=0, alias="DB_POOL_MAX_OVERFLOW")
    db_pool_timeout_seconds: float = Field(default=5.0, gt=0, alias="DB_POOL_TIMEOUT_SECONDS")
    db_statement_timeout_ms: int = Field(default=5000, ge=100, alias="DB_STATEMENT_TIMEOUT_MS")

    # --- Redis (rule-set cache and API rate limiting) ---------------------------
    redis_url: str = Field(..., alias="REDIS_URL")
    redis_max_connections: int = Field(default=50, ge=1, alias="REDIS_MAX_CONNECTIONS")
    redis_socket_timeout_seconds: float = Field(default=2.0, gt=0, alias="REDIS_SOCKET_TIMEOUT_SECONDS")

    # --- Kafka ------------------------------------------------------------------
    kafka_bootstrap_servers: str = Field(..., alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_name: str = Field(..., alias="KAFKA_TOPIC_NAME")
    kafka_dlq_topic_name: str = Field(default="", alias="KAFKA_DLQ_TOPIC_NAME")
    kafka_consumer_group_id: str = Field(..., alias="KAFKA_CONSUMER_GROUP_ID")
    kafka_security_protocol: str = Field(default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    kafka_ssl_ca_location: str = Field(default="", alias="KAFKA_SSL_CA_LOCATION")
    kafka_ssl_certificate_location: str = Field(default="", alias="KAFKA_SSL_CERTIFICATE_LOCATION")
    kafka_ssl_key_location: str = Field(default="", alias="KAFKA_SSL_KEY_LOCATION")
    kafka_ssl_key_password: str = Field(default="", alias="KAFKA_SSL_KEY_PASSWORD")
    kafka_oauth_token_url: str = Field(default="", alias="KAFKA_OAUTH_TOKEN_URL")
    kafka_oauth_client_id: str = Field(default="", alias="KAFKA_OAUTH_CLIENT_ID")
    kafka_oauth_client_secret: str = Field(default="", alias="KAFKA_OAUTH_CLIENT_SECRET")
    kafka_oauth_scope: str = Field(default="", alias="KAFKA_OAUTH_SCOPE")

    # --- Worker -----------------------------------------------------------------
    worker_metrics_port: int = Field(default=9100, ge=1, le=65535, alias="WORKER_METRICS_PORT")
    worker_heartbeat_path: str = Field(default="/tmp/fraud-worker-heartbeat", alias="WORKER_HEARTBEAT_PATH")
    worker_partition_high_watermark: int = Field(default=500, ge=1, alias="WORKER_PARTITION_HIGH_WATERMARK")
    worker_max_unexpected_error_attempts: int = Field(default=3, ge=1, alias="WORKER_MAX_UNEXPECTED_ERROR_ATTEMPTS")
    worker_retry_initial_backoff_seconds: float = Field(default=0.5, gt=0, alias="WORKER_RETRY_INITIAL_BACKOFF_SECONDS")
    worker_retry_max_backoff_seconds: float = Field(default=30.0, gt=0, alias="WORKER_RETRY_MAX_BACKOFF_SECONDS")
    worker_max_group_absence_seconds: float = Field(default=120.0, gt=0, alias="WORKER_MAX_GROUP_ABSENCE_SECONDS")
    worker_shutdown_timeout_seconds: float = Field(default=25.0, gt=0, alias="WORKER_SHUTDOWN_TIMEOUT_SECONDS")

    # --- Rules ------------------------------------------------------------------
    rule_set_key: str = Field(default="fraud-default", alias="RULE_SET_KEY")
    rule_local_cache_ttl_seconds: float = Field(default=5.0, ge=0, alias="RULE_LOCAL_CACHE_TTL_SECONDS")
    rule_redis_cache_ttl_seconds: int = Field(default=60, ge=1, alias="RULE_REDIS_CACHE_TTL_SECONDS")

    # --- API authentication / authorisation -------------------------------------
    jwt_issuer: str = Field(..., alias="JWT_ISSUER")
    jwt_audience: str = Field(..., alias="JWT_AUDIENCE")
    jwt_jwks_url: str = Field(default="", alias="JWT_JWKS_URL")
    jwt_public_key_path: str = Field(default="", alias="JWT_PUBLIC_KEY_PATH")
    jwt_algorithms: list[str] = Field(default_factory=lambda: ["RS256"], alias="JWT_ALGORITHMS")
    jwt_leeway_seconds: int = Field(default=30, ge=0, le=300, alias="JWT_LEEWAY_SECONDS")
    jwt_jwks_min_refresh_interval_seconds: float = Field(
        default=60.0, ge=0, alias="JWT_JWKS_MIN_REFRESH_INTERVAL_SECONDS"
    )
    scope_transactions_read: str = Field(default="fraud:transactions:read", alias="SCOPE_TRANSACTIONS_READ")
    scope_rules_read: str = Field(default="fraud:rules:read", alias="SCOPE_RULES_READ")
    scope_rules_write: str = Field(default="fraud:rules:write", alias="SCOPE_RULES_WRITE")

    # --- API behaviour ----------------------------------------------------------
    # Empty means no cross-origin browser access (the API is for back-office callers).
    cors_allowed_origins: list[str] = Field(default_factory=list, alias="CORS_ALLOWED_ORIGINS")
    rate_limit_enabled: bool = Field(default=True, alias="RATE_LIMIT_ENABLED")
    rate_limit_requests: int = Field(default=100, ge=1, alias="RATE_LIMIT_REQUESTS")
    rate_limit_window_seconds: int = Field(default=60, ge=1, alias="RATE_LIMIT_WINDOW_SECONDS")

    # --- Batch summary job ------------------------------------------------------
    batch_job_enabled: bool = Field(default=False, alias="BATCH_JOB_ENABLED")
    batch_run_hour: int = Field(default=0, ge=0, le=23, alias="BATCH_RUN_HOUR")
    batch_run_minute: int = Field(default=0, ge=0, le=59, alias="BATCH_RUN_MINUTE")
    batch_summary_window_hours: int = Field(default=24, ge=1, alias="BATCH_SUMMARY_WINDOW_HOURS")

    # --- Observability ----------------------------------------------------------
    observability_log_level: str = Field(default="INFO", alias="OBSERVABILITY_LOG_LEVEL")
    observability_enable_tracing: bool = Field(default=False, alias="OBSERVABILITY_ENABLE_TRACING")
    otel_exporter_otlp_endpoint: str = Field(default="", alias="OTEL_EXPORTER_OTLP_ENDPOINT")
    logs_redact_fields: list[str] = Field(
        default_factory=lambda: [
            "authorization",
            "password",
            "client_secret",
            "access_token",
            "refresh_token",
            "token",
        ],
        alias="LOGS_REDACT_FIELDS",
    )

    @field_validator("cors_allowed_origins", "logs_redact_fields", mode="before")
    @classmethod
    def _parse_csv_lists(cls, value: object) -> object:
        return _split_csv(value)

    @field_validator("jwt_algorithms", mode="before")
    @classmethod
    def _parse_jwt_algorithms(cls, value: object) -> object:
        return _split_csv(value)

    @field_validator("jwt_algorithms")
    @classmethod
    def _require_asymmetric_algorithms(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("JWT_ALGORITHMS must list at least one algorithm")
        rejected = sorted(set(value) - _ALLOWED_JWT_ALGORITHMS)
        if rejected:
            raise ValueError(f"JWT_ALGORITHMS contains unsupported algorithms: {rejected}")
        return value

    @field_validator("observability_log_level")
    @classmethod
    def _validate_log_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("OBSERVABILITY_LOG_LEVEL must be a standard logging level")
        return normalized

    @model_validator(mode="after")
    def _derive_defaults(self) -> Settings:
        if not self.kafka_dlq_topic_name:
            object.__setattr__(self, "kafka_dlq_topic_name", f"{self.kafka_topic_name}.dlq")
        if self.worker_retry_max_backoff_seconds < self.worker_retry_initial_backoff_seconds:
            raise ValueError("WORKER_RETRY_MAX_BACKOFF_SECONDS must be >= WORKER_RETRY_INITIAL_BACKOFF_SECONDS")
        return self

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from the process environment and mounted secret files.

        Raises:
            RuntimeError: If a required variable is missing or any value is invalid.
                The message names the variables but never echoes their values.
        """
        data: dict[str, object] = {}
        missing: list[str] = []
        for field_name, field_info in cls.model_fields.items():
            env_name = field_info.alias or field_name
            value = _read_env(env_name)
            if value is None:
                if field_info.is_required():
                    missing.append(env_name)
                continue
            data[env_name] = value

        if missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            problems = "; ".join(
                f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
                for error in exc.errors(include_input=False)
            )
            raise RuntimeError(f"Invalid configuration: {problems}") from None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings, loaded once from the environment."""
    return Settings.from_env()


def clear_settings_cache() -> None:
    """Forget the cached settings so the next call re-reads the environment (tests)."""
    get_settings.cache_clear()
