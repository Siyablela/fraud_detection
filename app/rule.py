from decimal import Decimal
from typing import Any, Dict, Set
from uuid import uuid7 as uuid
import json_logic
from pydantic import BaseModel, Field, field_validator

from app.config import RulesConfigProvider
from app.time_utils import utc_now_unix_epoch

_rules = RulesConfigProvider()


def get_active_rules_config():
    """Return the current live rules configuration (reloaded if the file changed)"""
    return _rules.get()


# Define the expected structure of incoming transaction events
class TransactionIngestSchema(BaseModel):
    """Represents an incoming transaction event evaluated by the fraud rules engine.

    Attributes:
        transaction_id: Unique identifier for the transaction event.
        user_id: ID of the user associated with the transaction.
        amount: Monetary amount involved in the transaction.
        category: Category classification for the transaction.
        timestamp: Event timestamp in seconds since the Unix epoch.
    """

    transaction_id: uuid
    user_id: uuid
    amount: Decimal
    category: str
    timestamp: float = Field(default_factory=utc_now_unix_epoch)

# Reflect property names into an unmodifiable lookup set
ALLOWED_TRANSACTION_FIELDS: Set[str] = set(TransactionIngestSchema.__annotations__.keys())

class RuleValidationSchema(BaseModel):
    """Validates metadata and ensures JsonLogic syntax complies with our runtime domain object."""
    rule_name: str = Field(..., min_length=3, max_length=100)
    description: str | None = None
    root_condition: Dict[str, Any] = Field(
        ..., 
        description="A valid JsonLogic statement mapping exclusively to Transaction attributes."
    )
    actions: list[Dict[str, Any]] = Field(..., min_length=1)

    @field_validator('root_condition')
    @classmethod
    def validate_json_logic_syntax_and_attributes(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Check for basic dictionary structural requirements
        if not isinstance(v, dict) or not v:
            raise ValueError("Root condition must be a non-empty JSON object mapping.")

        # 2. Structural Compile-Time Validation Check via json_logic package
        try:
            json_logic.jsonLogic(v, {})
        except Exception as e:
            raise ValueError(f"Malformed JsonLogic AST structure: {str(e)}")

        # 3. Static Attribute Enforcement: Crawl AST dictionary tree to isolate 'var' keywords
        def inspect_ast_nodes(node: Any):
            if isinstance(node, dict):
                for key, value in node.items():
                    if key == "var":
                        # JsonLogic extracts keys natively via {"var": "field_name"} or {"var": ["field_name", default]}
                        target_field = value[0] if isinstance(value, list) and value else value
                        if isinstance(target_field, str) and target_field not in ALLOWED_TRANSACTION_FIELDS:
                            raise ValueError(
                                f"Unauthorized attribute tracking attempted: '{target_field}'. "
                                f"Must be one of: {ALLOWED_TRANSACTION_FIELDS}"
                            )
                    else:
                        inspect_ast_nodes(value)
            elif isinstance(node, list):
                for item in node:
                    inspect_ast_nodes(item)

        inspect_ast_nodes(v)
        return v


# Rules engine executing synchronous, high-speed checks
def evaluate_transaction(
    transaction: TransactionIngestSchema,
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
            config.velocity_max_amount is not None
            and recent_count is not None
            and recent_count + 1 > config.velocity_max_count
        ):
            flags.append("VELOCITY_COUNT_EXCEEDED")
        if (
            config.velocity_max_amount is not None
            and recent_amount_sum is not None
            and recent_amount_sum + transaction.amount
            > Decimal(str(config.velocity_max_amount))
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
        "triggered_rules": flags,
        "rules_version": config.rules_version,
        "rules_hash": config.rules_hash,
    }
