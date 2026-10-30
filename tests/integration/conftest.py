"""Real PostgreSQL, Redis and Kafka in containers (requires Docker).

Containers start once per session; each test gets clean tables. The schema comes
from ``alembic upgrade head``, so these tests also prove the migrations.
"""

import os
import socket
import time

import docker
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

TOPIC = "transactions-it"
DLQ_TOPIC = f"{TOPIC}.dlq"


def _docker_problem() -> str | None:
    """Why Docker cannot be used, or None when it answers."""
    try:
        client = docker.from_env(timeout=10)
        client.ping()
        client.close()
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


def pytest_collection_modifyitems(config, items):
    """Skip integration tests with one clear reason when Docker is not running.

    CI sets REQUIRE_DOCKER=1, so there a missing Docker fails the run instead of
    silently skipping the tests that matter most.
    """
    integration = [item for item in items if item.get_closest_marker("integration")]
    if not integration:
        return
    problem = _docker_problem()
    if problem is None:
        return
    if os.getenv("REQUIRE_DOCKER") == "1":
        raise pytest.UsageError(f"Integration tests need Docker, which is unavailable: {problem}")
    skip = pytest.mark.skip(reason=f"Docker is not running ({problem}); start it to run integration tests")
    for item in integration:
        item.add_marker(skip)


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


def _free_port() -> int:
    """A host port that is free right now; Kafka must advertise it before it starts."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture(scope="session")
def kafka_bootstrap():
    host_port = _free_port()
    container = (
        DockerContainer("apache/kafka:3.9.0")
        .with_bind_ports(9092, host_port)
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://localhost:{host_port}")
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
        bootstrap = f"localhost:{host_port}"
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
