"""Request and response models for the HTTP API (v1)."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from app.events import Category, Channel, CountryCode, CurrencyCode, Enrichment, Money
from app.rules import RuleSet, Verdict


class ErrorDetail(BaseModel):
    field: str = Field(examples=["query.limit"])
    message: str = Field(examples=["Input should be less than or equal to 100"])


class ErrorBody(BaseModel):
    code: str = Field(examples=["validation_error"], description="Stable, machine-readable error code.")
    message: str = Field(examples=["Request validation failed."])
    details: list[ErrorDetail] = Field(default_factory=list)
    correlation_id: str | None = Field(default=None, description="Quote this when reporting a problem.")


class ErrorResponse(BaseModel):
    """Every non-2xx response has this shape."""

    error: ErrorBody


class TransactionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: UUID
    user_id: UUID
    amount: Decimal = Field(description="Exact decimal, serialised as a string.", examples=["125.50"])
    currency: str = Field(examples=["ZAR"])
    category: str
    occurred_at: datetime
    channel: str | None = Field(default=None, examples=["ONLINE"])
    country: str | None = Field(default=None, examples=["ZA"])
    enrichment: dict[str, Any] | None = Field(default=None, description="Upstream features, as received.")
    risk_score: int = Field(description="Sum of the scores of the rules that fired.", examples=[90])
    verdict: Verdict = Field(
        description="APPROVED / REVIEW / REJECTED from the rule set's thresholds. Scored after authorisation, "
        "so REJECTED means 'restrict and investigate', not a declined payment."
    )
    is_fraud: bool = Field(description="True when the verdict is REVIEW or REJECTED.")
    triggered_rules: list[str] = Field(examples=[["HIGH_VALUE_THRESHOLD"]])
    rules_version: str = Field(
        description="Rule set and version that produced the decision.", examples=["fraud-default@v1"]
    )
    rules_hash: str = Field(description="SHA-256 of the rule-set definition used.")
    correlation_id: str | None
    created_at: datetime = Field(description="When the decision was recorded.")


class TransactionPage(BaseModel):
    data: list[TransactionResponse]
    limit: int
    next_cursor: str | None = Field(description="Pass as `cursor` to fetch the next page; null on the last page.")


class RuleSetVersionResponse(BaseModel):
    key: str
    version: int
    label: str = Field(examples=["fraud-default@v2"])
    description: str
    definition: dict[str, Any]
    content_hash: str
    created_by: str
    created_at: datetime

    @classmethod
    def from_rule_set(cls, rule_set: RuleSet) -> RuleSetVersionResponse:
        return cls(
            key=rule_set.key,
            version=rule_set.version,
            label=rule_set.label,
            description=rule_set.description,
            definition=rule_set.definition.model_dump(mode="json"),
            content_hash=rule_set.content_hash,
            created_by=rule_set.created_by,
            created_at=rule_set.created_at,
        )


class RuleSetVersionPage(BaseModel):
    data: list[RuleSetVersionResponse]
    limit: int
    next_before_version: int | None = Field(description="Pass as `before_version` for older versions.")


class CreateRuleSetVersionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: str = Field(min_length=1, max_length=500, examples=["Lower high-value threshold to 8,000"])
    definition: dict[str, Any] = Field(description="Rule-set body; see README 'Rule sets'.")
    expected_current_version: int | None = Field(
        default=None,
        ge=0,
        description="Optimistic-concurrency guard. 0 = the key must not exist yet. Omit to append unconditionally.",
    )


class EvaluateRequest(BaseModel):
    """A hypothetical transaction to score without storing anything."""

    model_config = ConfigDict(extra="forbid")

    user_id: UUID
    amount: Money
    currency: CurrencyCode = Field(examples=["ZAR"])
    category: Category
    occurred_at: AwareDatetime | None = Field(default=None, description="Defaults to now.")
    channel: Channel | None = None
    country: CountryCode | None = None
    enrichment: Enrichment | None = None
    transaction_id: UUID | None = Field(default=None, description="Excluded from the velocity look-back if given.")


class VelocityContext(BaseModel):
    window_seconds: int
    prior_count: int
    prior_amount: Decimal


class EvaluateResponse(BaseModel):
    rule_set: str = Field(examples=["fraud-default@v1"])
    risk_score: int
    verdict: Verdict
    is_fraud: bool
    triggered_rules: list[str]
    velocity: VelocityContext
