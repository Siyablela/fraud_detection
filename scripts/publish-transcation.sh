#!/usr/bin/env bash

set -euo pipefail

TOPIC="transactions_topic"
BROKER_CONTAINER="fraud_kafka_broker"
BOOTSTRAP_SERVER="kafka:9092"
TRANSACTION_ID="demo-$(date +%s)"
USER_ID="user-42"
AMOUNT="12500"
CATEGORY="RETAIL"
TIMESTAMP="$(date +%s)"

usage() {
    cat <<'USAGE'
Publish a transaction event to the Kafka topic used by the fraud worker.

Usage:
  ./scripts/publish-transaction.sh [options]

Options:
  --transaction-id <value>    Transaction id (default: demo-<epoch>)
  --user-id <value>           User id (default: user-42)
  --amount <number>           Amount (default: 12500)
  --category <value>          Category (default: RETAIL)
  --timestamp <epoch>         Unix timestamp (default: now)
  --topic <value>             Kafka topic (default: transactions_topic)
  --broker-container <value>  Kafka broker container (default: fraud_kafka_broker)
  --bootstrap-server <value>  Kafka bootstrap server inside broker container (default: kafka:9092)
  -h, --help                  Show help

Examples:
  ./scripts/publish-transaction.sh --transaction-id demo-123
  ./scripts/publish-transaction.sh --user-id user-99 --amount 250 --category TRAVEL

Rule scenario scripts:
  ./scripts/publish-rule-high-value.sh
  ./scripts/publish-rule-risky-category.sh
  ./scripts/publish-rule-velocity-count.sh
  ./scripts/publish-rule-velocity-amount.sh
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --transaction-id)
            TRANSACTION_ID="$2"
            shift 2
            ;;
        --user-id)
            USER_ID="$2"
            shift 2
            ;;
        --amount)
            AMOUNT="$2"
            shift 2
            ;;
        --category)
            CATEGORY="$2"
            shift 2
            ;;
        --timestamp)
            TIMESTAMP="$2"
            shift 2
            ;;
        --topic)
            TOPIC="$2"
            shift 2
            ;;
        --broker-container)
            BROKER_CONTAINER="$2"
            shift 2
            ;;
        --bootstrap-server)
            BOOTSTRAP_SERVER="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required but not found on PATH." >&2
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$BROKER_CONTAINER"; then
    echo "Broker container '$BROKER_CONTAINER' is not running." >&2
    echo "Start the stack first with: docker compose --env-file .demo.env up -d" >&2
    exit 1
fi

PAYLOAD=$(cat <<EOF
{"transaction_id":"$TRANSACTION_ID","user_id":"$USER_ID","amount":$AMOUNT,"category":"$CATEGORY","timestamp":$TIMESTAMP}
EOF
)

printf '%s\n' "$PAYLOAD" | docker exec -i "$BROKER_CONTAINER" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --topic "$TOPIC" >/dev/null

echo "Published transaction to topic '$TOPIC': $TRANSACTION_ID"
s