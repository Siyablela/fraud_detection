import json
import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


_REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_REPO_ROOT / ".env", override=False)


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _optional_env(name: str, default: str) -> str:
    value = os.getenv(name)
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
    jwt_required_scope_for_transaction_write: str = Field(
        default="fraud:transactions:write",
        alias="JWT_REQUIRED_SCOPE_FOR_TRANSACTION_WRITE",
    )

    kafka_security_protocol: str = Field(default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    kafka_ssl_truststore_path: str = Field(default="", alias="KAFKA_SSL_TRUSTSTORE_PATH")
    kafka_ssl_keystore_cert_path: str = Field(default="", alias="KAFKA_SSL_KEYSTORE_CERT_PATH")
    kafka_ssl_keystore_key_path: str = Field(default="", alias="KAFKA_SSL_KEYSTORE_KEY_PATH")
    kafka_ssl_keystore_password: str = Field(default="", alias="KAFKA_SSL_KEYSTORE_PASSWORD")
    kafka_producer_acks: str = Field(default="all", alias="KAFKA_PRODUCER_ACKS")
    kafka_producer_enable_idempotence: bool = Field(default=True, alias="KAFKA_PRODUCER_ENABLE_IDEMPOTENCE")
    kafka_producer_max_in_flight: int = Field(default=5, alias="KAFKA_PRODUCER_MAX_IN_FLIGHT")

    db_pool_min_size: int = Field(..., alias="DB_POOL_MIN_SIZE")
    db_pool_max_size: int = Field(..., alias="DB_POOL_MAX_SIZE")

    default_high_value_threshold: float = Field(..., alias="DEFAULT_HIGH_VALUE_THRESHOLD")
    default_velocity_threshold: int = Field(..., alias="DEFAULT_VELOCITY_THRESHOLD")
    default_restricted_categories: dict[str, float] = Field(..., alias="DEFAULT_RESTRICTED_CATEGORIES")

    observability_log_level: str = Field(default="INFO", alias="OBSERVABILITY_LOG_LEVEL")
    observability_enable_tracing: bool = Field(default=False, alias="OBSERVABILITY_ENABLE_TRACING")
    worker_metrics_port: int = Field(default=9100, alias="WORKER_METRICS_PORT")

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("jwt_algorithms", mode="before")
    @classmethod
    def parse_jwt_algorithms(cls, value: object) -> object:
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
            "jwt_required_scope_for_transaction_write": _optional_env(
                "JWT_REQUIRED_SCOPE_FOR_TRANSACTION_WRITE", "fraud:transactions:write"
            ),
            "kafka_security_protocol": _optional_env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            "kafka_ssl_truststore_path": _optional_env("KAFKA_SSL_TRUSTSTORE_PATH", ""),
            "kafka_ssl_keystore_cert_path": _optional_env("KAFKA_SSL_KEYSTORE_CERT_PATH", ""),
            "kafka_ssl_keystore_key_path": _optional_env("KAFKA_SSL_KEYSTORE_KEY_PATH", ""),
            "kafka_ssl_keystore_password": _optional_env("KAFKA_SSL_KEYSTORE_PASSWORD", ""),
            "kafka_producer_acks": _optional_env("KAFKA_PRODUCER_ACKS", "all"),
            "kafka_producer_enable_idempotence": _optional_env("KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", "True"),
            "kafka_producer_max_in_flight": _optional_env("KAFKA_PRODUCER_MAX_IN_FLIGHT", "5"),
            "db_pool_min_size": _required_env("DB_POOL_MIN_SIZE"),
            "db_pool_max_size": _required_env("DB_POOL_MAX_SIZE"),
            "default_high_value_threshold": _required_env("DEFAULT_HIGH_VALUE_THRESHOLD"),
            "default_velocity_threshold": _required_env("DEFAULT_VELOCITY_THRESHOLD"),
            "default_restricted_categories": _required_env("DEFAULT_RESTRICTED_CATEGORIES"),
            "observability_log_level": _optional_env("OBSERVABILITY_LOG_LEVEL", "INFO"),
            "observability_enable_tracing": _optional_env("OBSERVABILITY_ENABLE_TRACING", "False"),
            "worker_metrics_port": _optional_env("WORKER_METRICS_PORT", "9100"),
        }
        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            raise RuntimeError(str(exc)) from exc


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()


def clear_settings_cache() -> None:
    get_settings.cache_clear()