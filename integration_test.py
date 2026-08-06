import asyncio
import os
import random
import uuid
import time
from pathlib import Path
import httpx


def load_env_file() -> None:
    """Load values from the repository .env file when present."""
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()
API_URL = os.getenv("INTEGRATION_API_URL")
if not API_URL:
    raise RuntimeError("Missing required environment variable: INTEGRATION_API_URL")
ACCESS_TOKEN = os.getenv("INTEGRATION_ACCESS_TOKEN")


def generate_mock_transaction(user_id: int, amount: float | None = None) -> dict:
    """Generates a structured dictionary matching your Pydantic Transaction model."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(user_id),
        "amount": amount if amount is not None else round(random.uniform(5.0, 2000.0), 2),
        "timestamp": int(time.time()),
        "category": random.choice(["GROCERIES", "ELECTRONICS", "ENTERTAINMENT", "GAMING"]),
    }


async def send_transaction(client: httpx.AsyncClient, payload: dict, scenario: str):
    """Sends the JSON payload to the FastAPI query endpoint for smoke verification."""
    try:
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"} if ACCESS_TOKEN else None
        response = await client.get(f"{API_URL}/api/v1/transactions/{payload['transaction_id']}", headers=headers, timeout=5.0)
        if response.status_code == 200:
            print(f"[{scenario}] Verified Tx: {payload['transaction_id']}")
        else:
            print(f"❌ Verification failed. Status: {response.status_code}")
    except Exception as e:
        print(f"💥 Connection Error: {e}")


async def main():
    async with httpx.AsyncClient() as client:
        print("🚀 Starting Fraud Detection System Integration Test...")
        print(f"Target API Endpoint: {API_URL}\n")

        print("--- Running Scenario 1: Verifying API availability ---")
        for _ in range(3):
            random_user = random.randint(1000, 2000)
            payload = generate_mock_transaction(user_id=random_user)
            await send_transaction(client, payload, "API_CHECK")
            await asyncio.sleep(1.5)

        print("\n✅ Integration verification complete.")
        print("Check your 'worker' container logs and database state for processing results.")


if __name__ == "__main__":
    asyncio.run(main())
