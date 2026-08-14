from pydantic import BaseModel, Field

from app.config import RulesConfigProvider
from app.time_utils import utc_now_unix_epoch

_rules = RulesConfigProvider()

# Define the expected structure of incoming transaction events
class Transaction(BaseModel):
    """Represents an incoming transaction event evaluated by the fraud rules engine.

    Attributes:
        transaction_id: Unique identifier for the transaction event.
        user_id: ID of the user associated with the transaction.
        amount: Monetary amount involved in the transaction.
        category: Category classification for the transaction.
        timestamp: Event timestamp in seconds since the Unix epoch.
    """

    transaction_id: str
    user_id: str
    amount: float
    category: str
    timestamp: float = Field(default_factory=utc_now_unix_epoch)

# Rules engine executing synchronous, high-speed checks
def evaluate_transaction(transaction: Transaction) -> dict:
    """Evaluate a transaction against the configured fraud thresholds and risky categories.

    Args:
        transaction: The transaction payload to evaluate.

    Returns:
        dict: A transaction result dictionary with the original fields, fraud status, and triggered rules.
    """
    flags = []
    config = _rules.get()

    if transaction.amount >= config.high_value_threshold:
        flags.append("HIGH_VALUE_THRESHOLD")

    # Rule 2: Restricted Category Risk
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
