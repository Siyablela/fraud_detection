import json
import os
from pathlib import Path

from dotenv import load_dotenv


_REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_REPO_ROOT / ".env", override=False)


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _required_env_float(name: str) -> float:
    return float(_required_env(name))


def _required_env_int(name: str) -> int:
    return int(_required_env(name))


def _required_env_json_object(name: str) -> dict[str, float]:
    value = json.loads(_required_env(name))
    if not isinstance(value, dict):
        raise RuntimeError(f"Environment variable {name} must be a JSON object")
    return {str(key).upper(): float(limit) for key, limit in value.items()}


def _optional_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _optional_env_int(name: str, default: int) -> int:
    return int(_optional_env(name, str(default)))


def _optional_env_bool(name: str, default: bool) -> bool:
    value = _optional_env(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "on"}


DATABASE_URL = _required_env("DATABASE_URL")
REDIS_URL = _required_env("REDIS_URL")
KAFKA_BOOTSTRAP_SERVERS = _required_env("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_NAME = _required_env("KAFKA_TOPIC_NAME")
KAFKA_DLQ_TOPIC_NAME = _optional_env("KAFKA_DLQ_TOPIC_NAME", f"{KAFKA_TOPIC_NAME}.dlq")
KAFKA_CONSUMER_GROUP_ID = _required_env("KAFKA_CONSUMER_GROUP_ID")
FRAUD_RULES_CONFIG_PATH = _required_env("FRAUD_RULES_CONFIG_PATH")

JWT_ISSUER = _required_env("JWT_ISSUER")
JWT_AUDIENCE = _required_env("JWT_AUDIENCE")
JWT_JWKS_URL = _optional_env("JWT_JWKS_URL", "")
JWT_PUBLIC_KEY_PATH = _optional_env("JWT_PUBLIC_KEY_PATH", "")
JWT_ALGORITHMS = [
    algorithm.strip()
    for algorithm in _optional_env("JWT_ALGORITHMS", "RS256").split(",")
    if algorithm.strip()
]
JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ = _optional_env(
    "JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ", "fraud:transactions:read"
)
JWT_REQUIRED_SCOPE_FOR_TRANSACTION_WRITE = _optional_env(
    "JWT_REQUIRED_SCOPE_FOR_TRANSACTION_WRITE", "fraud:transactions:write"
)

KAFKA_SECURITY_PROTOCOL = _optional_env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SSL_TRUSTSTORE_PATH = _optional_env("KAFKA_SSL_TRUSTSTORE_PATH", "")
KAFKA_SSL_KEYSTORE_CERT_PATH = _optional_env("KAFKA_SSL_KEYSTORE_CERT_PATH", "")
KAFKA_SSL_KEYSTORE_KEY_PATH = _optional_env("KAFKA_SSL_KEYSTORE_KEY_PATH", "")
KAFKA_SSL_KEYSTORE_PASSWORD = _optional_env("KAFKA_SSL_KEYSTORE_PASSWORD", "")
KAFKA_PRODUCER_ACKS = _optional_env("KAFKA_PRODUCER_ACKS", "all")
KAFKA_PRODUCER_ENABLE_IDEMPOTENCE = _optional_env_bool(
    "KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", True
)
KAFKA_PRODUCER_MAX_IN_FLIGHT = _optional_env_int("KAFKA_PRODUCER_MAX_IN_FLIGHT", 5)

DB_POOL_MIN_SIZE = _required_env_int("DB_POOL_MIN_SIZE")
DB_POOL_MAX_SIZE = _required_env_int("DB_POOL_MAX_SIZE")
VELOCITY_WINDOW_SECONDS = _required_env_int("VELOCITY_WINDOW_SECONDS")

DEFAULT_HIGH_VALUE_THRESHOLD = _required_env_float("DEFAULT_HIGH_VALUE_THRESHOLD")
DEFAULT_VELOCITY_THRESHOLD = _required_env_int("DEFAULT_VELOCITY_THRESHOLD")
DEFAULT_RESTRICTED_CATEGORIES = _required_env_json_object(
    "DEFAULT_RESTRICTED_CATEGORIES"
)

OBSERVABILITY_LOG_LEVEL = _optional_env("OBSERVABILITY_LOG_LEVEL", "INFO")
OBSERVABILITY_ENABLE_TRACING = _optional_env_bool("OBSERVABILITY_ENABLE_TRACING", False)
WORKER_METRICS_PORT = _optional_env_int("WORKER_METRICS_PORT", 9100)