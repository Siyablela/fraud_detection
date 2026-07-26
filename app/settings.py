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


DATABASE_URL = _required_env("DATABASE_URL")
REDIS_URL = _required_env("REDIS_URL")
KAFKA_BOOTSTRAP_SERVERS = _required_env("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_NAME = _required_env("KAFKA_TOPIC_NAME")
KAFKA_CONSUMER_GROUP_ID = _required_env("KAFKA_CONSUMER_GROUP_ID")
FRAUD_RULES_CONFIG_PATH = _required_env("FRAUD_RULES_CONFIG_PATH")

DB_POOL_MIN_SIZE = _required_env_int("DB_POOL_MIN_SIZE")
DB_POOL_MAX_SIZE = _required_env_int("DB_POOL_MAX_SIZE")
VELOCITY_WINDOW_SECONDS = _required_env_int("VELOCITY_WINDOW_SECONDS")

DEFAULT_HIGH_VALUE_THRESHOLD = _required_env_float("DEFAULT_HIGH_VALUE_THRESHOLD")
DEFAULT_VELOCITY_THRESHOLD = _required_env_int("DEFAULT_VELOCITY_THRESHOLD")
DEFAULT_RESTRICTED_CATEGORIES = _required_env_json_object(
    "DEFAULT_RESTRICTED_CATEGORIES"
)