from decimal import Decimal

from pydantic import BaseModel, Field

from app.config import RulesConfigProvider
from app.time_utils import utc_now_unix_epoch

_rules = RulesConfigProvider()

def get_active_rules_config():
    """Return the current live rules configuration (reloaded if the file changed)"""
    return _rules.get()


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
    amount: Decimal
    category: str
    timestamp: float = Field(default_factory=utc_now_unix_epoch)

# Rules engine executing synchronous, high-speed checks
def evaluate_transaction(
        transaction: Transaction,
        recent_count: int | None = None,
        recent_amount_sum: Decimal | None = None,
) -> dict:
    """Evaluate a transaction against the configured fraud thresholds and risky categories.

    Args:
        transaction: The transaction payload to evaluate.
        recent_count: Count of the same user's prior transactions inside the configured
            velocity window (excluding this one) When ``None`` the velocity-count check
            is skipped, keeping the function pure for stateless callers.
        recent_amount_sum: Some of the User's prior transaction amounts inside the
            velocitywindow (excluding this one). When ``None`` the velocity amount check
            is skipped.

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

    # Rule 3: Velocity (stateful) - too many transactions, or too much spend, by the
    # same user inside a short window. Only evaluated whe the caller supplies recent
    # activity, so stateless callers keep the original two-rule behaviour/
    if config.velocity_window_seconds:
        if (
            config.velocity_max_amount
            and recent_count is not None
            and recent_count + 1 > config.velocity_max_count
        ):
            flags.append("VELOCITY_COUNT")
        if (
            config.velocity_max_amount
            and recent_amount_sum is not None
            and recent_amount_sum + transaction.amount > Decimal(str(config.velocity_max_amount))
        ):
            flags.append("VELOCITY_AMOUNT")
            
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
