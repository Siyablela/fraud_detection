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
