"""Copy dead-lettered messages back to their source topic, byte for byte.

Fix the cause first (deploy the code fix, or correct the producer), then replay.
Messages are replayed with their original key, so per-user ordering is kept
relative to each other. The replay group's offsets are committed only after
each message is re-published, so an interrupted run can simply be re-run.

    python scripts/replay_dlq.py --dry-run          # list what would be replayed
    python scripts/replay_dlq.py --max 100
"""

import argparse
import sys

from confluent_kafka import Consumer, Producer


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--dlq-topic", default="transactions.dlq")
    parser.add_argument("--group-id", default="fraud-dlq-replay")
    parser.add_argument("--max", type=int, default=1000, help="stop after this many messages")
    parser.add_argument("--dry-run", action="store_true", help="print messages without replaying or committing")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": args.bootstrap_servers, "acks": "all", "enable.idempotence": True})
    consumer.subscribe([args.dlq_topic])
    replayed = 0
    try:
        while replayed < args.max:
            message = consumer.poll(5.0)
            if message is None:
                break
            if message.error():
                print(f"error: {message.error()}", file=sys.stderr)
                return 1
            headers = {name: value.decode("utf-8", "replace") for name, value in (message.headers() or [])}
            source_topic = headers.get("dlq.source.topic")
            summary = (
                f"{source_topic}[{headers.get('dlq.source.partition')}]@{headers.get('dlq.source.offset')} "
                f"{headers.get('dlq.error.type')}: {headers.get('dlq.error.message')}"
            )
            if args.dry_run:
                print(f"would replay {summary}")
            else:
                if not source_topic:
                    print(f"skipping message without dlq.source.topic at offset {message.offset()}", file=sys.stderr)
                    continue
                producer.produce(source_topic, key=message.key(), value=message.value())
                if producer.flush(15) != 0:
                    print("error: replay was not acknowledged; stopping without committing", file=sys.stderr)
                    return 1
                consumer.commit(message=message, asynchronous=False)
                print(f"replayed {summary}")
            replayed += 1
    finally:
        consumer.close()
    print(f"{'listed' if args.dry_run else 'replayed'} {replayed} message(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
