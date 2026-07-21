import asyncio
import json
import os
import redis.asyncio as aioredis
from redis.exceptions import TimeoutError as RedisTimeoutError
from app.rule import Transaction, evaluate_transaction

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

async def main():
    # Establish async connection to Redis
    redis_conn = aioredis.from_url(REDIS_URL, decode_responses=True)
    print("Fraud Processing Worker is active and waiting for events...")
    
    while True:
        try:
            # Atomic blocking pop from the 'transactions_queue'
            # Blpop acts as our lightweight, scalable ingestion processor
            message = await redis_conn.blpop("transactions_queue", timeout=5)
            if message is None:
                continue

            _, raw_event = message
            event_data = json.loads(raw_event)
            
            # Validate input structure using Pydantic
            transaction = Transaction(**event_data)
            
            # Scalability optimization: Use a Redis sliding/fixed window for velocity check.
            # Increment user activity counter for the last 60 seconds
            velocity_key = f"velocity:{transaction.user_id}"
            current_velocity = await redis_conn.incr(velocity_key)
            if current_velocity == 1:
                await redis_conn.expire(velocity_key, 60) # Window resets every 60 seconds

            # Pass the velocity data directly into the evaluator
            result = evaluate_transaction(transaction, current_velocity)
            
            # Store processed transactional state for API retrieval
            # Storing under a hash table "tx:<id>" gives O(1) retrieval time
            await redis_conn.set(f"tx:{transaction.transaction_id}", json.dumps(result))
            
            # Secondary Indexing: Append to a category list to allow API filtering
            await redis_conn.sadd(f"category:{transaction.category.lower()}", transaction.transaction_id)
            
            print(f"Processed Tx: {transaction.transaction_id} | Fraud: {result['is_fraud']}")
            
        except RedisTimeoutError:
            # A finite BLPOP wait can time out while the queue is idle.
            continue
        except Exception as e:
            print(f"Error processing transaction element: {e}")
            # In a production context, you would push the raw_event to a dead-letter queue here
            await redis_conn.aclose()
            redis_conn = aioredis.from_url(REDIS_URL, decode_responses=True)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
