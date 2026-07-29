import time
from pydantic import BaseModel, Field
from app.config import RulesConfigProvider

_rules = RulesConfigProvider()

# Define the expected structure of incoming transaction events
class TransactionRequest(BaseModel):
    correlation_id: str | None = None
    user_id: str
    amount: float
    category: str
    timestamp: float = Field(default_factory=time.time)


class Transaction(BaseModel):
    audit_id: str
    correlation_id: str | None = None
    user_id: str
    amount: float
    category: str
    timestamp: float = Field(default_factory=time.time)
    actor_id: str | None = None
    actor_type: str | None = None
    source_ip: str | None = None
    user_agent: str | None = None
    request_id: str | None = None
    ingest_path: str | None = None

# Rules engine executing synchronous, high-speed checks
def evaluate_transaction(transaction: Transaction) -> dict:
    flags = []
    config = _rules.get()
    
    # Rule 1: High value transaction check
    if transaction.amount > config.high_value_threshold:
        flags.append("HIGH_VALUE_TRANSACTION")
        
    # Rule 2: Restricted Category Risk
    category_limit = config.restricted_categories.get(transaction.category.upper())
    if category_limit is not None and transaction.amount > category_limit:
        flags.append("RISKY_CATEGORY_LIMIT")

    is_fraud = len(flags) > 0
    
    return {
        "audit_id": transaction.audit_id,
        "correlation_id": transaction.correlation_id,
        "user_id": transaction.user_id,
        "amount": transaction.amount,
        "category": transaction.category,
        "timestamp": transaction.timestamp,
        "actor_id": transaction.actor_id,
        "actor_type": transaction.actor_type,
        "source_ip": transaction.source_ip,
        "user_agent": transaction.user_agent,
        "request_id": transaction.request_id,
        "ingest_path": transaction.ingest_path,
        "ruleset_hash": config.fingerprint(),
        "is_fraud": is_fraud,
        "triggered_rules": flags
    }
