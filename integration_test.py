import asyncio
import json
import os
import random
import uuid
from pathlib import Path

import httpx
from confluent_kafka import Producer

from app.kafka_security import build_kafka_client_config
from app.observability import configure_logging, get_logger
from app.time_utils import utc_now_unix_epoch

configure_logging("integration-test")
logger = get_logger(__name__)


def load_env_file() -> None:
    """Load values from the repository .env file when present."""
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()


def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip()


API_URL = required_env("INTEGRATION_API_URL")
KAFKA_BOOTSTRAP_SERVERS = required_env("INTEGRATION_KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_NAME = required_env("INTEGRATION_KAFKA_TOPIC_NAME")
ACCESS_TOKEN = required_env("INTEGRATION_ACCESS_TOKEN")


def generate_mock_transaction(user_id: int, amount: float | None = None) -> dict:
    """Generates a structured dictionary matching your Pydantic Transaction model."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(user_id),
        "amount": amount if amount is not None else round(random.uniform(5.0, 2000.0), 2),
        "timestamp": int(utc_now_unix_epoch()),
        "category": random.choice(["GROCERIES", "ELECTRONICS", "ENTERTAINMENT", "GAMING"]),
    }


async def publish_transaction(producer: Producer, payload: dict) -> None:
    producer.produce(
        KAFKA_TOPIC_NAME,
        json.dumps(payload).encode("utf-8"),
    )
    producer.flush(timeout=30)


async def wait_for_transaction(client: httpx.AsyncClient, transaction_id: str) -> None:
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

    for attempt in range(1, 21):
        response = await client.get(
            f"{API_URL}/api/v1/transactions/{transaction_id}",
            headers=headers,
            timeout=5.0,
        )
        if response.status_code == 200:
            logger.info(
                "integration_transaction_verified",
                transaction_id=transaction_id,
                attempt=attempt,
                status_code=response.status_code,
            )
            return
        if response.status_code == 401:
            raise RuntimeError("Integration token is missing or invalid for the protected query API.")
        if response.status_code != 404:
            raise RuntimeError(
                f"Unexpected response while polling transaction {transaction_id}: {response.status_code} {response.text}"
            )
        logger.info(
            "integration_transaction_pending",
            transaction_id=transaction_id,
            attempt=attempt,
            status_code=response.status_code,
        )
        await asyncio.sleep(1.0)

    raise RuntimeError(f"Transaction {transaction_id} was not visible through the API before timeout.")


async def main():
    payload = generate_mock_transaction(user_id=random.randint(1000, 2000))

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            **build_kafka_client_config(),
        }
    )
    try:
        logger.info(
            "integration_smoke_test_started",
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            kafka_topic_name=KAFKA_TOPIC_NAME,
            api_url=API_URL,
            transaction_id=payload["transaction_id"],
        )

        await publish_transaction(producer, payload)
        logger.info(
            "integration_transaction_published",
            transaction_id=payload["transaction_id"],
            kafka_topic_name=KAFKA_TOPIC_NAME,
        )

        async with httpx.AsyncClient() as client:
            await wait_for_transaction(client, payload["transaction_id"])

        logger.info(
            "integration_smoke_test_passed",
            transaction_id=payload["transaction_id"],
        )
    finally:
        producer.flush(timeout=30)


if __name__ == "__main__":
    asyncio.run(main())
