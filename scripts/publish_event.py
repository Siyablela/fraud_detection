"""Publish TransactionCreated events to Kafka, keyed by user_id.

Examples (against the local compose stack):
    python scripts/publish_event.py --amount 15000 --category CRYPTO
    python scripts/publish_event.py --user-id 0192f3c4-0000-7000-8000-000000000001 --count 6 --amount 10
    python scripts/publish_event.py --raw '{"not": "a valid event"}'      # lands in the DLQ
"""

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from confluent_kafka import Producer


def build_event(args: argparse.Namespace, index: int, user_id: str) -> dict:
    occurred_at = (args.occurred_at or datetime.now(UTC)) + timedelta(seconds=index)
    return {
        "schema_version": 1,
        "event_type": "TransactionCreated",
        "transaction_id": args.transaction_id or str(uuid4()),
        "user_id": user_id,
        "amount": args.amount,
        "category": args.category,
        "occurred_at": occurred_at.isoformat(),
        "correlation_id": f"cli-{uuid4()}",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--user-id", default=None, help="defaults to a new random UUID")
    parser.add_argument("--transaction-id", default=None, help="reuse to simulate a duplicate publish")
    parser.add_argument("--amount", default="125.50", help="decimal string, e.g. 10000.00")
    parser.add_argument("--category", default="GROCERIES")
    parser.add_argument("--occurred-at", type=datetime.fromisoformat, default=None, help="ISO-8601 with offset")
    parser.add_argument("--count", type=int, default=1, help="events to send, one second apart in event time")
    parser.add_argument("--raw", default=None, help="send this exact payload instead of a generated event")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap_servers, "acks": "all", "enable.idempotence": True})
    user_id = args.user_id or str(uuid4())
    if args.raw is not None:
        producer.produce(args.topic, key=b"raw", value=args.raw.encode("utf-8"))
        print(json.dumps({"published": "raw", "topic": args.topic}))
    else:
        for index in range(args.count):
            event = build_event(args, index, user_id)
            producer.produce(args.topic, key=user_id.encode("utf-8"), value=json.dumps(event).encode("utf-8"))
            print(json.dumps(event))
    if producer.flush(15) != 0:
        print("error: not every event was acknowledged by Kafka", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
