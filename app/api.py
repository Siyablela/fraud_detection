import os
import json
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Fraud Detection Query Engine API")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Global async Redis pool initiated at startup
@app.on_event("startup")
async def startup_event():
    app.state.redis = await aioredis.from_url(REDIS_URL, decode_responses=True)

@app.get("/api/v1/transactions/{transaction_id}")
async def get_transaction(transaction_id: str):
    tx_data = await app.state.redis.get(f"tx:{transaction_id}")
    if not tx_data:
        raise HTTPException(status_code=404, detail="Transaction records not located.")
    return json.loads(tx_data)

@app.get("/api/v1/categories/{category_name}")
async def get_transactions_by_category(category_name: str, limit: int = 100):
    # Retrieve transactional IDs matching this category using our secondary index
    tx_ids = await app.state.redis.smembers(f"category:{category_name.lower()}")
    
    results = []
    for tx_id in list(tx_ids)[:limit]:
        tx_data = await app.state.redis.get(f"tx:{tx_id}")
        if tx_data:
            results.append(json.loads(tx_data))
            
    return {"category": category_name, "count": len(results), "data": results}
