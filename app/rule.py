import time
from pydantic import BaseModel, Field
from app.config import RulesConfigProvider

_rules = RulesConfigProvider()

# Define the expected structure of incoming transaction events
class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    category: str
    timestamp: float = Field(default_factory=time.time)

# Rules engine executing synchronous, high-speed checks
def evaluate_transaction(transaction: Transaction) -> dict:
    flags = []
    config = _rules.get()
        
    # Rule 1: Restricted Category Risk
    category_limit = config.restricted_categories.get(transaction.category.upper())
    if category_limit is not None and transaction.amount > category_limit:
        flags.append("RISKY_CATEGORY_LIMIT")

    is_fraud = len(flags) > 0
    
    return {
        "transaction_id": transaction.transaction_id,
        "user_id": transaction.user_id,
        "amount": transaction.amount,
        "category": transaction.category,
        "timestamp": transaction.timestamp,
        "is_fraud": is_fraud,
        "triggered_rules": flags
    }
