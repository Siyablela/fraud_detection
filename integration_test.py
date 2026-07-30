import asyncio
import os
import random
import uuid
import time
import httpx

PRODUCER_URL = os.getenv("INTEGRATION_PRODUCER_URL")
if not PRODUCER_URL:
    raise RuntimeError("Missing required environment variable: INTEGRATION_PRODUCER_URL")
API_KEY = os.getenv("INTEGRATION_API_KEY")

def generate_mock_transaction(user_id: int, amount: float = None) -> dict:
    """Generates a structured dictionary matching your Pydantic Transaction model."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(user_id),
        "amount": amount if amount else round(random.uniform(5.0, 2000.0), 2),
        "timestamp": int(time.time()),
        "category": random.choice(["GROCERIES", "ELECTRONICS", "ENTERTAINMENT", "GAMING"])
    }

async def send_transaction(client: httpx.AsyncClient, payload: dict, scenario: str):
    """Sends the JSON payload to the FastAPI producer endpoint."""
    try:
        headers = {"x-api-key": API_KEY} if API_KEY else None
        response = await client.post(PRODUCER_URL, json=payload, headers=headers, timeout=5.0)
        if response.status_code == 202:
            print(f"[{scenario}] Sent Tx: {payload['transaction_id']} | User: {payload['user_id']} | Amt: ${payload['amount']}")
        else:
            print(f"❌ Failed to queue transaction. Status: {response.status_code}")
    except Exception as e:
        print(f"💥 Connection Error: {e}")

async def main():
    async with httpx.AsyncClient() as client:
        print("🚀 Starting Fraud Detection System Integration Test...")
        print(f"Target Producer Endpoint: {PRODUCER_URL}\n")
        
        # --- SCENARIO 1: Standard organic traffic stream ---
        print("--- Running Scenario 1: Simulating Normal User Traffic ---")
        for _ in range(5):
            random_user = random.randint(1000, 2000)
            payload = generate_mock_transaction(user_id=random_user)
            await send_transaction(client, payload, "NORMAL")
            await asyncio.sleep(1.5)  # Moderate delay between users
            
        print("\n--- Running Scenario 2: High-Velocity Fraud Trigger ---")
        # Target a single specific user to trip the Redis velocity sliding window counter
        target_fraud_user = 9999
        print(f"Targeting User {target_fraud_user} with 6 rapid transactions in under 2 seconds...")
        
        tasks = []
        for i in range(6):
            # Escalate the amounts to simulate a card testing/emptying pattern
            amount = 100.0 * (i + 1)
            payload = generate_mock_transaction(user_id=target_fraud_user, amount=amount)
            tasks.append(send_transaction(client, payload, "VELOCITY_BURST"))
            
        # Fire them concurrently using asyncio to simulate an explosive bottleneck
        await asyncio.gather(*tasks)

        print("\n✅ Integration stream simulation complete.")
        print("Check your 'worker' container logs to verify Kafka ingestion and database persistence!")

if __name__ == "__main__":
    asyncio.run(main())
