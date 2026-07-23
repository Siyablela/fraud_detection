import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer
import redis.asyncio as aioredis
from app.database import create_pool, save_transaction
from app.rule import Transaction, evaluate_transaction

# Configuration variables from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TOPIC_NAME = "transactions_topic"
CONSUMER_GROUP_ID = "fraud-worker-group"

async def main():
    # Initialize infrastructure connections
    database = await create_pool()
    
    # Redis remains in place *only* as a high-speed state store for velocity checks
    redis_conn = aioredis.from_url(REDIS_URL, decode_responses=True)
    
    # Configure the asynchronous Kafka consumer
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",  # Read from start if no committed offsets exist
        enable_auto_commit=False        # Manual commit for At-Least-Once delivery guarantees
    )
    
    await consumer.start()
    print("Fraud Processing Worker is active and streaming events from Kafka...")
    
    try:
        # Loop over the consumer stream. It automatically waits/polls internally.
        async for msg in consumer:
            raw_event = msg.value.decode("utf-8")
            
            try:
                event_data = json.loads(raw_event)
                
                # Validate input structure using Pydantic
                transaction = Transaction(**event_data)
                
                # Velocity calculations stay in Redis for microsecond performance
                velocity_key = f"velocity:{transaction.user_id}"
                current_velocity = await redis_conn.incr(velocity_key)
                if current_velocity == 1:
                    await redis_conn.expire(velocity_key, 60)

                # Execute fraud logic
                result = evaluate_transaction(transaction, current_velocity)
                
                # Write to persistent database storage
                await save_transaction(database, result)
                
                print(f"Processed Tx: {transaction.transaction_id} | Fraud: {result['is_fraud']}")
                
                # Commit offset only AFTER successful storage in PostgreSQL.
                # This prevents message loss if the pod crashes mid-execution.
                await consumer.commit()
                
            except Exception as e:
                print(f"Error processing transaction element: {e}")
                # In production, route unparseable messages to a Kafka Dead-Letter Topic here.
                # We skip manual offset commit here to allow investigation or processing retries.
                await asyncio.sleep(1)
                
    finally:
        # Clean shutdown of engine dependencies
        print("Shutting down worker clients...")
        await consumer.stop()
        await redis_conn.aclose()

if __name__ == "__main__":
    asyncio.run(main())
