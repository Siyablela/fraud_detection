"""Measure the scoring pipeline: events/second and publish-to-decision latency.

Publishes a batch of TransactionCreated events tagged with one correlation id, waits
until the worker has stored a decision for every one, then reads the timings the
worker already records per message in ``transaction_history``:

* ``source_timestamp`` - when the producer created the Kafka record (host clock)
* ``processed_at``     - when the decision was written (database clock)

Latency = processed_at - source_timestamp, corrected for the measured offset between
the two clocks. Run it inside the Compose network (below) for millisecond-accurate
latency: from a Windows or macOS host the database clock is in a VM, and the
host clock's ~15 ms resolution makes the offset correction approximate.

    # In-network (recommended): the app image already has the dependencies.
    docker run --rm --network fraud-detection_default -v "$PWD/scripts:/scripts:ro" \\
      -e LOADTEST_DATABASE_URL=postgresql://<user>:<password>@postgres:5432/<db> \\
      fraud-detection:local python /scripts/loadtest/pipeline_load.py \\
      --env-file /nonexistent --bootstrap-servers kafka:9092 --topic transactions --rate 50 --events 3000

    # From the host, reading .env:
    python scripts/loadtest/pipeline_load.py --events 20000

Two modes answer different questions:

* ``--rate 0`` (default) publishes as fast as possible: the worker's drain rate is
  its maximum throughput, and latency is mostly time spent queued behind earlier
  events, so it grows with --events.
* ``--rate N`` below capacity measures latency at that load. The first seconds
  include client warm-up (connections, metadata), so expect a higher p99 on short runs.

Events are spread over --users users (velocity rules query each user's history), in
the transactions topic of the running stack, so they show up in the API too.
"""

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import asyncpg
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from smoke_test import load_env

CATEGORIES = ["GROCERIES", "FUEL", "RESTAURANTS", "TRAVEL", "ELECTRONICS", "GAMBLING", "CRYPTO"]


async def clock_offset_seconds(connection: asyncpg.Connection, samples: int = 7) -> float:
    """Database clock minus host clock, from the sample with the shortest round trip."""
    readings = []
    for _ in range(samples):
        before = time.time()
        db_now = await connection.fetchval("SELECT extract(epoch FROM clock_timestamp())::float8")
        after = time.time()
        readings.append((after - before, db_now - (before + after) / 2))
    return min(readings)[1]


def build_event(user_id: str, correlation_id: str) -> dict:
    """A random valid event for ``user_id``, tagged with the run's correlation id."""
    return {
        "schema_version": 1,
        "event_type": "TransactionCreated",
        "transaction_id": str(uuid4()),
        "user_id": user_id,
        "amount": f"{random.uniform(10, 12000):.2f}",
        "currency": "ZAR",
        "category": random.choice(CATEGORIES),
        "occurred_at": datetime.now(UTC).isoformat(),
        "correlation_id": correlation_id,
    }


def publish(producer: Producer, topic: str, events: int, users: list[str], rate: float, correlation_id: str) -> float:
    """Publish ``events`` events (paced to ``rate`` per second when > 0); returns the achieved rate."""
    started = time.perf_counter()
    for sent in range(events):
        if rate > 0:
            delay = started + sent / rate - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
        user_id = random.choice(users)
        payload = json.dumps(build_event(user_id, correlation_id)).encode()
        while True:
            try:
                producer.produce(topic, key=user_id.encode(), value=payload)
                break
            except BufferError:
                producer.poll(0.1)  # local queue full: let deliveries drain
        producer.poll(0)
    if producer.flush(60) != 0:
        sys.exit("Kafka did not acknowledge every event")
    return events / (time.perf_counter() - started)


async def wait_for_decisions(connection, correlation_id: str, expected: int, deadline: float) -> None:
    """Poll until ``expected`` decisions are stored, or exit at ``deadline`` (``time.monotonic()``)."""
    last_report = 0.0
    while True:
        done = await connection.fetchval(
            "SELECT count(*) FROM transaction_history WHERE correlation_id = $1", correlation_id
        )
        if done >= expected:
            return
        if time.monotonic() > deadline:
            sys.exit(f"timed out: only {done}/{expected} decisions stored")
        if time.monotonic() - last_report > 5:
            print(f"  ... {done}/{expected} decisions stored", flush=True)
            last_report = time.monotonic()
        await asyncio.sleep(0.5)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--events", type=int, default=20000)
    parser.add_argument("--users", type=int, default=2000)
    parser.add_argument(
        "--rate", type=float, default=0, help="Events per second to publish at; 0 = as fast as possible"
    )
    parser.add_argument("--timeout", type=float, default=900, help="Seconds to wait for the worker to finish")
    parser.add_argument(
        "--database-url",
        default=os.getenv("LOADTEST_DATABASE_URL"),
        help="Overrides the POSTGRES_* values in the env file",
    )
    parser.add_argument("--topic", help="Overrides KAFKA_TOPIC_NAME in the env file")
    parser.add_argument("--bootstrap-servers", help="Overrides localhost:KAFKA_HOST_PORT, e.g. kafka:9092 in-network")
    args = parser.parse_args()
    env = load_env(args.env_file) if args.env_file.exists() else {}

    dsn = args.database_url or (
        f"postgresql://{env['POSTGRES_USER']}:{env['POSTGRES_PASSWORD']}"
        f"@localhost:{env.get('POSTGRES_HOST_PORT', '5432')}/{env['POSTGRES_DB']}"
    )
    topic = args.topic or env.get("KAFKA_TOPIC_NAME", "transactions")
    connection = await asyncpg.connect(dsn)
    try:
        offset = await clock_offset_seconds(connection)
        correlation_id = f"load-{uuid4()}"
        users = [str(uuid4()) for _ in range(args.users)]
        producer = Producer(
            {
                "bootstrap.servers": args.bootstrap_servers or f"localhost:{env.get('KAFKA_HOST_PORT', '29092')}",
                # localhost resolves to ::1 first on Windows, and each failed IPv6 attempt
                # stalls delivery ~2 s, which would be measured as pipeline latency.
                "broker.address.family": "v4",
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 5,
            }
        )
        mode = f"{args.rate:g}/s" if args.rate > 0 else "as fast as possible"
        print(f"Publishing {args.events} events to {topic} for {args.users} users ({mode}); run {correlation_id}")
        print(f"Clock offset (database - host): {offset * 1000:+.1f} ms, corrected for below")
        published_rate = publish(producer, topic, args.events, users, args.rate, correlation_id)
        print(f"Published at {published_rate:.0f} events/s; waiting for the worker...")
        await wait_for_decisions(connection, correlation_id, args.events, time.monotonic() + args.timeout)

        rows = await connection.fetch(
            """
            SELECT extract(epoch FROM processed_at)::float8 - $2::float8 - source_timestamp / 1000.0 AS latency,
                   extract(epoch FROM processed_at)::float8 AS done, verdict
            FROM transaction_history WHERE correlation_id = $1
            """,
            correlation_id,
            offset,
        )
    finally:
        await connection.close()

    latencies = sorted(row["latency"] * 1000 for row in rows)
    done = sorted(row["done"] for row in rows)
    drain_seconds = done[-1] - done[0]
    quantiles = statistics.quantiles(latencies, n=100, method="inclusive")
    verdicts = dict(sorted(Counter(row["verdict"] for row in rows).items()))

    print(f"\nDecisions stored:      {len(rows)}  {verdicts}")
    print(f"Worker throughput:     {(len(rows) - 1) / drain_seconds:.0f} events/s "
          f"(first to last decision, {drain_seconds:.1f} s)")  # fmt: skip
    print("Publish-to-decision latency (ms):")
    print(f"  p50 {quantiles[49]:.1f}   p95 {quantiles[94]:.1f}   p99 {quantiles[98]:.1f}   max {latencies[-1]:.1f}")
    if args.rate <= 0:
        print("  (published as fast as possible: these include time queued behind earlier events)")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
