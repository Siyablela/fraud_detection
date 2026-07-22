from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from app.database import database_pool, get_transaction as find_transaction
from app.database import get_transactions_by_category as find_by_category

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with database_pool() as pool:
        app.state.database = pool
        yield


app = FastAPI(title="Fraud Detection Query Engine API", lifespan=lifespan)


@app.get("/health")
async def health():
    try:
        await app.state.database.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="Database is unavailable")

@app.get("/api/v1/transactions/{transaction_id}")
async def get_transaction(transaction_id: str):
    transaction = await find_transaction(app.state.database, transaction_id)
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction records not located.")
    return transaction

@app.get("/api/v1/categories/{category_name}")
async def get_transactions_by_category(category_name: str, limit: int = 100):
    limit = max(1, min(limit, 1000))
    results = await find_by_category(app.state.database, category_name, limit)
    return {"category": category_name, "count": len(results), "data": results}
