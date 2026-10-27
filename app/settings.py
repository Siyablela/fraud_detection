import json
import os
from pathlib import Path
from datetime import datetime, timezone, timezone

from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv_values() -> dict[str, str]:
    """Load environment values from the repository's .env and .env.example files.

    Returns:
        dict[str, str]: A mapping of environment keys to their values, using the
            first available values from the repository-local dotenv files.
    """
    values: dict[str, str] = {}
    for env_file in (_REPO_ROOT / ".env", _REPO_ROOT / ".env.example"):
        if not env_file.exists():
            continue
        parsed_values = dotenv_values(env_file)
        if not isinstance(parsed_values, dict):
            continue
        for key, value in parsed_values.items():
            if isinstance(value, str) and value.strip():
                values[key] = value
    return values


_DOTENV_VALUES = _load_dotenv_values()
_PLACEHOLDER_ENV_VALUES = {"JWT_ISSUER": {"issuer"}, "JWT_AUDIENCE": {"aud"}}


def _read_env(name: str, default: str = "") -> str:
    """Read an environment variable, honoring *_FILE secret paths and placeholder fallbacks.

    Args:
        name: The environment variable name to resolve.
        default: A fallback value returned when the variable is unset or empty.

    Returns:
        str: The resolved environment value, including file-backed secret content.
    """
    current_value = os.getenv(name)
    if current_value not in (None, ""):
        placeholder_values = _PLACEHOLDER_ENV_VALUES.get(name, set())
        if current_value in placeholder_values:
            dotenv_value = _DOTENV_VALUES.get(name, "")
            if isinstance(dotenv_value, str) and dotenv_value.strip():
                return dotenv_value.strip()
        return current_value

    file_env_name = f"{name}_FILE"
    file_value = os.getenv(file_env_name)
    if file_value not in (None, ""):
        try:
            return Path(file_value).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise RuntimeError(
                f"Unable to read secret from {file_env_name}: {file_value}"
            ) from exc

    dotenv_value = _DOTENV_VALUES.get(name, "")
    if isinstance(dotenv_value, str) and dotenv_value.strip():
        return dotenv_value.strip()
    return default


class Settings(BaseModel):
    database_url: str = Field(..., alias="DATABASE_URL")
    kafka_bootstrap_servers: str = Field(..., alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_name: str = Field(..., alias="KAFKA_TOPIC_NAME")
    kafka_dlq_topic_name: str = Field(default="", alias="KAFKA_DLQ_TOPIC_NAME")
    kafka_consumer_group_id: str = Field(..., alias="KAFKA_CONSUMER_GROUP_ID")
    batch_job_enabled: bool = Field(default=False, alias="BATCH_JOB_ENABLED")
    batch_run_hour: int = Field(default=0, alias="BATCH_RUN_HOUR")
    batch_run_minute: int = Field(default=0, alias="BATCH_RUN_MINUTE")
    fraud_rules_config_path: str = Field(..., alias="FRAUD_RULES_CONFIG_PATH")

    jwt_issuer: str = Field(..., alias="JWT_ISSUER")
    jwt_audience: str = Field(..., alias="JWT_AUDIENCE")
    jwt_jwks_url: str = Field(default="", alias="JWT_JWKS_URL")
    jwt_jwks_refresh_interval_seconds: int = Field(
        default=300, ge=0, alias="JWT_JWKS_REFRESH_INTERVAL_SECONDS"
    )
    jwt_public_key_path: str = Field(default="", alias="JWT_PUBLIC_KEY_PATH")
    jwt_algorithms: list[str] = Field(
        default_factory=lambda: ["RS256"], alias="JWT_ALGORITHMS"
    )
    jwt_required_scope_for_transaction_read: str = Field(
        default="fraud:transactions:read",
        alias="JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ",
    )
    jwt_required_scope_for_rules_write: str = Field(
            default="fraud:transactions:read",
            alias="JWT_REQUIRED_SCOPE_FOR_RULES_WRITE",
        )
    auth_token_endpoint_enabled: bool = Field(
        default=False, alias="AUTH_TOKEN_ENDPOINT_ENABLED"
    )
    keycloak_token_client_id: str = Field(
        default="fraud-cli", alias="KEYCLOAK_TOKEN_CLIENT_ID"
    )
    keycloak_token_client_secret: str = Field(
        default="", alias="KEYCLOAK_TOKEN_CLIENT_SECRET"
    )
    keycloak_service_client_id: str = Field(
        default="fraud-service-cli", alias="KEYCLOAK_SERVICE_CLIENT_ID"
    )
    keycloak_service_client_secret: str = Field(
        default="", alias="KEYCLOAK_SERVICE_CLIENT_SECRET"
    )
    keycloak_service_token_scope: str = Field(
        default="", alias="KEYCLOAK_SERVICE_TOKEN_SCOPE"
    )

    kafka_security_protocol: str = Field(
        default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL"
    )
    kafka_producer_acks: str = Field(default="all", alias="KAFKA_PRODUCER_ACKS")
    kafka_producer_enable_idempotence: bool = Field(
        default=True, alias="KAFKA_PRODUCER_ENABLE_IDEMPOTENCE"
    )
    # TLS/mTLS  material for SSL and  SASL brokers (libdkafka property names).
    # When left empty, no ssl.* options are added and behaviour is unchanged.
    kafka_ssl_ca_location: str = Field(default="", alias="KAFKA_SSL_CA_LOCATION")
    kafka_ssl_certificate_location: str = Field(
        default="", alias="KAFKA_SSL_CERTIFICATE_LOCATION"
    )
    kafka_ssl_key_location: str = Field(default="", alias="KAFKA_SSL_KEY_LOCATION")
    kafka_ssl_key_password: str = Field(default="", alias="KAFKA_SSL_KEY_PASSWORD")

    db_pool_min_size: int = Field(..., alias="DB_POOL_MIN_SIZE")
    db_pool_max_size: int = Field(..., alias="DB_POOL_MAX_SIZE")

    default_high_value_threshold: float = Field(
        ..., alias="DEFAULT_HIGH_VALUE_THRESHOLD"
    )
    default_restricted_categories: dict[str, float] = Field(
        ..., alias="DEFAULT_RESTRICTED_CATEGORIES"
    )
    # Velocity (stateful) fraud rule defaults. A window or threshold of 0 disables
    # the corresponding velocity check.
    default_velocity_window_seconds: int = Field(
        default=60, ge=0, alias="DEFAULT_VELOCITY_WINDOW_SECONDS"
    )
    default_velocity_max_count: int = Field(
        default=5, ge=0, alias="DEFAULT_VELOCITY_MAX_COUNT"
    )
    default_velocity_max_amount: float = Field(
        default=5000.0, ge=0, alias="DEFAULT_VELOCITY_MAX_AMOUNT"
    )
    batch_summary_window_hours: int = Field(
        default=24, ge=1, alias="BATCH_SUMMARY_WINDOW_HOURS"
    )

    observability_log_level: str = Field(
        default="INFO", alias="OBSERVABILITY_LOG_LEVEL"
    )
    observability_enable_tracing: bool = Field(
        default=False, alias="OBSERVABILITY_ENABLE_TRACING"
    )
    observability_enable_otel_logs: bool = Field(
        default=False, alias="OBSERVABILITY_ENABLE_OTEL_LOGS"
    )
    otel_exporter_otlp_endpoint: str = Field(
        default="", alias="OTEL_EXPORTER_OTLP_ENDPOINT"
    )
    otel_exporter_otlp_logs_endpoint: str = Field(
        default="", alias="OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"
    )
    otel_logs_max_queue_size: int = Field(
        default=2048, ge=1, alias="OTEL_LOGS_MAX_QUEUE_SIZE"
    )
    otel_logs_max_exporter_batch_size: int = Field(
        default=512, ge=1, alias="OTEL_LOGS_MAX_EXPORTER_BATCH_SIZE"
    )
    otel_logs_schedule_delay_millis: int = Field(
        default=5000, ge=1, alias="OTEL_LOGS_SCHEDULE_DELAY_MILLIS"
    )
    otel_logs_export_timeout_millis: int = Field(
        default=5000, ge=1, alias="OTEL_LOGS_EXPORT_TIMEOUT_MILLIS"
    )
    otel_logs_export_timeout_seconds: int = Field(
        default=10, ge=1, alias="OTEL_LOGS_EXPORT_TIMEOUT_SECONDS"
    )
    logs_redact_fields: list[str] = Field(
        default_factory=lambda: [
            "authorization",
            "password",
            "client_secret",
            "access_token",
            "refresh_token",
        ],
        alias="LOGS_REDACT_FIELDS",
    )
    worker_metrics_port: int = Field(default=9100, alias="WORKER_METRICS_PORT")

    # CORS empty list means no cross-origin access is permitted (most restrictive)
    cors_allowed_origins: list[str] = Field(default=["*"], alias="CORS_ALLOWED_ORIGINS")
    # Per instance fixed-window rate limiting on the API
    rate_limit_enabled: bool = Field(default=True, alias="RATE_LIMIT_ENABLED")
    rate_limit_requests: int = Field(default=100, ge=1, alias="RATE_LIMIT_REQUESTS")
    rate_limit_window_seconds: int = Field(
        default=60, ge=1, alias="RATE_LIMIT_WINDOW_SECONDS"
    )
    # Token-bucket burst capacity. 0 means "use rate_limit_requests"
    rate_limit_burst: int = Field(default=0, ge=0, alias="RATE_LIMIT_BURST")

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("cors_allowed_origins", mode="before")
    @classmethod
    def parse_cors_allowed_origins(cls, value: object) -> object:
        """Parse the allowed origins for CORS.

        Args:
            value: A list of allowed origins or a comma-separated string.

        Returns:
            object: A list of allowed origins.
        """
        if value is None:
            return []
        if isinstance(value, list):
            return [str(origin).strip() for origin in value]
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        raise ValueError(
            "CORS_ALLOWED_ORIGINS must be a list or a comma-separated string"
        )

    @field_validator("jwt_algorithms", mode="before")
    @classmethod
    def parse_jwt_algorithms(cls, value: object) -> object:
        """Normalize the configured JWT algorithms into a list of strings.

        Args:
            value: A configured algorithm value from the environment, either as a
                comma-separated string or a list.

        Returns:
            object: A cleaned list of algorithm strings or the original value when
                already valid.
        """
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [
                algorithm.strip() for algorithm in value.split(",") if algorithm.strip()
            ]
        return value

    @field_validator("logs_redact_fields", mode="before")
    @classmethod
    def parse_log_redact_fields(cls, value: object) -> object:
        """Normalize configured log redaction fields into a list of strings."""
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [field.strip() for field in value.split(",") if field.strip()]
        return value

    @field_validator("default_restricted_categories", mode="before")
    @classmethod
    def parse_restricted_categories(cls, value: object) -> object:
        """Parse a restricted category map from JSON or a dictionary.

        Args:
            value: A dictionary of category names to thresholds or a JSON string
                encoding the same structure.

        Returns:
            object: A normalized dictionary keyed by uppercase category names.
        """
        if isinstance(value, dict):
            return {str(key).upper(): float(limit) for key, limit in value.items()}
        if isinstance(value, str):
            parsed_value = json.loads(value)
            if not isinstance(parsed_value, dict):
                raise ValueError("DEFAULT_RESTRICTED_CATEGORIES must be a JSON object")
            return {
                str(key).upper(): float(limit) for key, limit in parsed_value.items()
            }
        raise ValueError("DEFAULT_RESTRICTED_CATEGORIES must be a JSON object")

    @classmethod
    def from_env(cls) -> "Settings":
        """Build the settings model from the current environment and file-based secrets.

        Returns:
            Settings: The validated application settings instance for the current
                runtime environment.

        Raises:
            RuntimeError: If required settings are missing or invalid.
        """
        data: dict[str, object] = {}
        for field_name, field_info in cls.model_fields.items():
            env_name = field_info.alias or field_name
            value = _read_env(env_name)

            if value in (None, ""):
                if field_info.is_required():
                    raise RuntimeError(
                        f"missing required environment variable: {env_name}"
                    )
                # Let default/default_factory apply default optional settings.
                continue

            data[field_name] = value

        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            raise RuntimeError(str(exc)) from exc


def get_settings() -> Settings:
    """Build and return the active application settings from the current environment.

    Returns:
        Settings: The runtime configuration for the app.
    """
    return Settings.from_env()


def clear_settings_cache() -> None:
    """Clear any cached settings state; currently retained for compatibility with callers.

    Returns:
        None: This helper intentionally performs no cache invalidation today.
    """
    return None
