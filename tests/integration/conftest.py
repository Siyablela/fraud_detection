"""Real PostgreSQL, Redis and Kafka in containers (requires Docker).

Containers start once per session; each test gets clean tables. The schema comes
from ``alembic upgrade head``, so these tests also prove the migrations.
"""

import os
import time

import pytest
from alembic import command
from alembic.config import Config
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from app.database import create_engine, create_session_factory
from app.settings import Settings, clear_settings_cache

pytestmark = pytest.mark.integration

KAFKA_HOST_PORT = 59092
TOPIC = "transactions-it"
DLQ_TOPIC = f"{TOPIC}.dlq"


@pytest.fixture(scope="session")
def postgres_url():
    with PostgresContainer("postgres:16.4-alpine", driver=None) as postgres:
        url = postgres.get_connection_url()
        os.environ["DATABASE_URL"] = url
        command.upgrade(Config("alembic.ini"), "head")
        yield url


@pytest.fixture(scope="session")
def redis_url():
    with RedisContainer("redis:7.4.1-alpine") as redis_container:
        yield f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"


@pytest.fixture(scope="session")
def kafka_bootstrap():
    container = (
        DockerContainer("apache/kafka:3.9.0")
        .with_bind_ports(9092, KAFKA_HOST_PORT)
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://localhost:{KAFKA_HOST_PORT}")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .waiting_for(LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(120))
    )
    with container:
        bootstrap = f"localhost:{KAFKA_HOST_PORT}"
        admin = AdminClient({"bootstrap.servers": bootstrap})
        futures = admin.create_topics(
            [NewTopic(TOPIC, num_partitions=3, replication_factor=1), NewTopic(DLQ_TOPIC, 1, 1)]
        )
        for future in futures.values():
            future.result(timeout=30)
        time.sleep(1)
        yield bootstrap


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def integration_env(postgres_url, redis_url):
    os.environ.update(
        {
            "DATABASE_URL": postgres_url,
            "REDIS_URL": redis_url,
            "KAFKA_TOPIC_NAME": TOPIC,
            "RULE_LOCAL_CACHE_TTL_SECONDS": "0",
        }
    )
    clear_settings_cache()
    return Settings.from_env()


@pytest.fixture
async def session_factory(integration_env):
    engine = create_engine(integration_env)
    async with engine.begin() as connection:
        await connection.exec_driver_sql("TRUNCATE transactions, transaction_history RESTART IDENTITY CASCADE")
        await connection.exec_driver_sql(
            "DELETE FROM rule_set_versions WHERE NOT (rule_set_key = 'fraud-default' AND version = 1)"
        )
    yield create_session_factory(engine)
    await engine.dispose()
