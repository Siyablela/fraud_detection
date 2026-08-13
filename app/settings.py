import json
import os
from pathlib import Path

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
            raise RuntimeError(f"Unable to read secret from {file_env_name}: {file_value}") from exc

    dotenv_value = _DOTENV_VALUES.get(name, "")
    if isinstance(dotenv_value, str) and dotenv_value.strip():
        return dotenv_value.strip()
    return default


def _required_env(name: str) -> str:
    """Return a required environment value or raise if it is missing.

    Args:
        name: The environment variable name to read.

    Returns:
        str: The resolved value.

    Raises:
        RuntimeError: If the required environment variable is unset or empty.
    """
    value = _read_env(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _optional_env(name: str, default: str) -> str:
    """Return an optional environment value with a default if it is not set.

    Args:
        name: The environment variable name to read.
        default: The fallback value used when the variable is empty or absent.

    Returns:
        str: The resolved environment value or the supplied default.
    """
    value = _read_env(name)
    if value is None or value == "":
        return default
    return value


class Settings(BaseModel):
    database_url: str = Field(..., alias="DATABASE_URL")
    kafka_bootstrap_servers: str = Field(..., alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_name: str = Field(..., alias="KAFKA_TOPIC_NAME")
    kafka_dlq_topic_name: str = Field(default="", alias="KAFKA_DLQ_TOPIC_NAME")
    kafka_consumer_group_id: str = Field(..., alias="KAFKA_CONSUMER_GROUP_ID")
    fraud_rules_config_path: str = Field(..., alias="FRAUD_RULES_CONFIG_PATH")

    jwt_issuer: str = Field(..., alias="JWT_ISSUER")
    jwt_audience: str = Field(..., alias="JWT_AUDIENCE")
    jwt_jwks_url: str = Field(default="", alias="JWT_JWKS_URL")
    jwt_public_key_path: str = Field(default="", alias="JWT_PUBLIC_KEY_PATH")
    jwt_algorithms: list[str] = Field(default_factory=lambda: ["RS256"], alias="JWT_ALGORITHMS")
    jwt_required_scope_for_transaction_read: str = Field(
        default="fraud:transactions:read",
        alias="JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ",
    )
    auth_token_endpoint_enabled: bool = Field(default=False, alias="AUTH_TOKEN_ENDPOINT_ENABLED")
    keycloak_token_client_id: str = Field(default="fraud-cli", alias="KEYCLOAK_TOKEN_CLIENT_ID")
    keycloak_token_client_secret: str = Field(default="", alias="KEYCLOAK_TOKEN_CLIENT_SECRET")
    keycloak_service_client_id: str = Field(
        default="fraud-service-cli", alias="KEYCLOAK_SERVICE_CLIENT_ID"
    )
    keycloak_service_client_secret: str = Field(default="", alias="KEYCLOAK_SERVICE_CLIENT_SECRET")
    keycloak_service_token_scope: str = Field(default="", alias="KEYCLOAK_SERVICE_TOKEN_SCOPE")

    kafka_security_protocol: str = Field(default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    kafka_producer_acks: str = Field(default="all", alias="KAFKA_PRODUCER_ACKS")
    kafka_producer_enable_idempotence: bool = Field(default=True, alias="KAFKA_PRODUCER_ENABLE_IDEMPOTENCE")

    db_pool_min_size: int = Field(..., alias="DB_POOL_MIN_SIZE")
    db_pool_max_size: int = Field(..., alias="DB_POOL_MAX_SIZE")

    default_high_value_threshold: float = Field(..., alias="DEFAULT_HIGH_VALUE_THRESHOLD")
    default_restricted_categories: dict[str, float] = Field(..., alias="DEFAULT_RESTRICTED_CATEGORIES")

    observability_log_level: str = Field(default="INFO", alias="OBSERVABILITY_LOG_LEVEL")
    observability_enable_tracing: bool = Field(default=False, alias="OBSERVABILITY_ENABLE_TRACING")
    worker_metrics_port: int = Field(default=9100, alias="WORKER_METRICS_PORT")

    model_config = ConfigDict(populate_by_name=True)

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
            return [algorithm.strip() for algorithm in value.split(",") if algorithm.strip()]
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
            return {str(key).upper(): float(limit) for key, limit in parsed_value.items()}
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
        data = {
            "database_url": _required_env("DATABASE_URL"),
            "kafka_bootstrap_servers": _required_env("KAFKA_BOOTSTRAP_SERVERS"),
            "kafka_topic_name": _required_env("KAFKA_TOPIC_NAME"),
            "kafka_dlq_topic_name": _optional_env("KAFKA_DLQ_TOPIC_NAME", ""),
            "kafka_consumer_group_id": _required_env("KAFKA_CONSUMER_GROUP_ID"),
            "fraud_rules_config_path": _required_env("FRAUD_RULES_CONFIG_PATH"),
            "jwt_issuer": _required_env("JWT_ISSUER"),
            "jwt_audience": _required_env("JWT_AUDIENCE"),
            "jwt_jwks_url": _optional_env("JWT_JWKS_URL", ""),
            "jwt_public_key_path": _optional_env("JWT_PUBLIC_KEY_PATH", ""),
            "jwt_algorithms": _optional_env("JWT_ALGORITHMS", "RS256"),
            "jwt_required_scope_for_transaction_read": _optional_env(
                "JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ", "fraud:transactions:read"
            ),
            "auth_token_endpoint_enabled": _optional_env("AUTH_TOKEN_ENDPOINT_ENABLED", "False"),
            "keycloak_token_client_id": _optional_env("KEYCLOAK_TOKEN_CLIENT_ID", "fraud-cli"),
            "keycloak_token_client_secret": _optional_env("KEYCLOAK_TOKEN_CLIENT_SECRET", ""),
            "keycloak_service_client_id": _optional_env(
                "KEYCLOAK_SERVICE_CLIENT_ID", "fraud-service-cli"
            ),
            "keycloak_service_client_secret": _optional_env("KEYCLOAK_SERVICE_CLIENT_SECRET", ""),
            "keycloak_service_token_scope": _optional_env("KEYCLOAK_SERVICE_TOKEN_SCOPE", ""),
            "kafka_security_protocol": _optional_env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            "kafka_producer_acks": _optional_env("KAFKA_PRODUCER_ACKS", "all"),
            "kafka_producer_enable_idempotence": _optional_env("KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", "True"),
            "db_pool_min_size": _required_env("DB_POOL_MIN_SIZE"),
            "db_pool_max_size": _required_env("DB_POOL_MAX_SIZE"),
            "default_high_value_threshold": _required_env("DEFAULT_HIGH_VALUE_THRESHOLD"),
            "default_restricted_categories": _required_env("DEFAULT_RESTRICTED_CATEGORIES"),
            "observability_log_level": _optional_env("OBSERVABILITY_LOG_LEVEL", "INFO"),
            "observability_enable_tracing": _optional_env("OBSERVABILITY_ENABLE_TRACING", "False"),
            "worker_metrics_port": _optional_env("WORKER_METRICS_PORT", "9100"),
        }
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