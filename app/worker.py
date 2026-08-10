import asyncio
import inspect
import json
from typing import Any

from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server

from app.database import create_pool, save_transaction
from app.dlq import build_dlq_payload, encode_dlq_payload
from app.kafka_security import build_kafka_client_config
from app.observability import (
    configure_logging,
    get_logger,
    setup_tracing,
    worker_fraud_decision,
    worker_message_outcome,
    worker_message_timer,
)
from app.rule import Transaction, evaluate_transaction
from app.settings import get_settings

SERVICE_NAME = "fraud-worker"

logger = get_logger(__name__)


class TopicPartition(tuple):
    def __new__(cls, topic: str, partition: int):
        return super().__new__(cls, (topic, partition))

    @property
    def topic(self) -> str:
        return self[0]

    @property
    def partition(self) -> int:
        return self[1]


def _message_value(msg: Any, attribute: str) -> Any:
    value = getattr(msg, attribute, None)
    if callable(value):
        return value()
    return value


async def _invoke_callable(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    return await asyncio.to_thread(fn, *args, **kwargs)


async def commit_processed_message(consumer: Consumer, msg: Any) -> None:
    commit_method = getattr(consumer, "commit", None)
    if commit_method is None:
        return

    if inspect.iscoroutinefunction(commit_method):
        try:
            await commit_method(message=msg, asynchronous=False)
        except TypeError:
            topic = _message_value(msg, "topic")
            offset = _message_value(msg, "offset") + 1
            partition = _message_value(msg, "partition")
            if isinstance(offset, int) and isinstance(partition, int):
                await commit_method({TopicPartition(topic, partition): offset})
            else:
                await commit_method({topic: offset})
        return

    await _invoke_callable(commit_method, message=msg, asynchronous=False)


async def route_message_to_dlq(
    consumer: Consumer,
    producer: Producer | None = None,
    msg: Any = None,
    raw_event: str = "",
    error: Exception | None = None,
    topic_name: str = "",
    dlq_topic_name: str = "",
    dlq_producer: Producer | None = None,
) -> None:
    timestamp_value = _message_value(msg, "timestamp")
    if isinstance(timestamp_value, tuple) and len(timestamp_value) >= 2:
        source_timestamp = timestamp_value[1]
    else:
        source_timestamp = timestamp_value

    dlq_payload = build_dlq_payload(
        source_topic=topic_name,
        source_partition=_message_value(msg, "partition"),
        source_offset=_message_value(msg, "offset"),
        source_timestamp=source_timestamp,
        raw_payload=raw_event,
        error=error,
    )

    target_producer = dlq_producer or producer
    if hasattr(target_producer, "produce"):
        await _invoke_callable(
            target_producer.produce,
            dlq_topic_name,
            value=encode_dlq_payload(dlq_payload),
            key=_message_value(msg, "key"),
        )
        await _invoke_callable(target_producer.flush, 30)
    else:
        await _invoke_callable(
            target_producer.send_and_wait,
            topic=dlq_topic_name,
            key=_message_value(msg, "key"),
            value=encode_dlq_payload(dlq_payload),
        )
    worker_message_outcome(SERVICE_NAME, topic_name, "dlq")
    logger.exception(
        "transaction_routed_to_dlq",
        dlq_topic=dlq_topic_name,
        source_offset=_message_value(msg, "offset"),
        source_partition=_message_value(msg, "partition"),
    )
    await commit_processed_message(consumer, msg)


async def main() -> None:
    settings = get_settings()
    topic_name = settings.kafka_topic_name
    dlq_topic_name = settings.kafka_dlq_topic_name
    consumer_group_id = settings.kafka_consumer_group_id
    configure_logging(SERVICE_NAME, settings.observability_log_level)
    if settings.observability_enable_tracing:
        setup_tracing(SERVICE_NAME)

    database_engine, database = create_pool()
    start_http_server(settings.worker_metrics_port)
    logger.info("worker_metrics_started", port=settings.worker_metrics_port)

    consumer_config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": consumer_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        **build_kafka_client_config(),
    }
    producer_config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": settings.kafka_producer_acks,
        "enable.idempotence": settings.kafka_producer_enable_idempotence,
        **build_kafka_client_config(),
    }

    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)

    try:
        consumer.subscribe([topic_name])
        logger.info(
            "worker_started",
            topic=topic_name,
            dlq_topic=dlq_topic_name,
            group_id=consumer_group_id,
        )

        while True:
            msg = await asyncio.to_thread(consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning("kafka_poll_error", error=str(msg.error()))
                continue

            raw_event = msg.value().decode("utf-8")

            try:
                with worker_message_timer(SERVICE_NAME, topic_name):
                    event_data = json.loads(raw_event)
                    transaction = Transaction(**event_data)

                    result = evaluate_transaction(transaction)
                    await save_transaction(
                        database,
                        result,
                        source_metadata={
                            "source_topic": msg.topic(),
                            "source_partition": msg.partition(),
                            "source_offset": msg.offset(),
                            "source_timestamp": msg.timestamp()[1],
                        },
                    )

                    worker_fraud_decision(SERVICE_NAME, bool(result["is_fraud"]))
                    worker_message_outcome(SERVICE_NAME, topic_name, "success")
                    await commit_processed_message(consumer, msg)

            except Exception as exc:
                try:
                    await route_message_to_dlq(
                        consumer=consumer,
                        producer=producer,
                        msg=msg,
                        raw_event=raw_event,
                        error=exc,
                        topic_name=topic_name,
                        dlq_topic_name=dlq_topic_name,
                    )
                except Exception:
                    logger.exception(
                        "dlq_publish_failed",
                        dlq_topic=dlq_topic_name,
                        source_offset=msg.offset(),
                        source_partition=msg.partition(),
                    )
                    worker_message_outcome(SERVICE_NAME, topic_name, "error")
                    await asyncio.sleep(1)

    finally:
        logger.info("worker_shutdown")
        consumer.close()
        producer.flush(30)
        await asyncio.to_thread(lambda: None)
        await database_engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
