"""Versioned fraud rule sets expressed in a safe subset of JsonLogic.

Why a local evaluator instead of a JsonLogic package: rule sets are written over
the API, so the evaluator is an untrusted-input interpreter. This one

* only knows an explicit operator allowlist (no ``log``/side effects),
* bounds expression depth and size, so a stored rule cannot exhaust the worker,
* checks at write time that every ``var`` names a field the context provides,
* does all arithmetic in ``Decimal``: money must never pass through binary floats,
  where ``0.1 + 0.2 > 0.3`` and threshold boundaries become unreliable.
"""

import hashlib
import itertools
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError, field_validator, model_validator

from app.errors import CorruptRuleSetError, InvalidRuleSetError, RuleEvaluationError

# Limits on one rule's condition. Real rules use a handful of nodes; the caps stop a
# published rule from being a recursion bomb or making every evaluation slow.
MAX_EXPRESSION_DEPTH = 24
MAX_EXPRESSION_NODES = 400


class ValueType(StrEnum):
    """The type of a value in a condition, as reported in validation errors."""

    NUMBER = "a number"
    TEXT = "text"
    BOOLEAN = "true/false"
    NULL = "null"


@dataclass(frozen=True)
class Variable:
    """A value a rule can read about a transaction."""

    type: ValueType
    required: bool  # False: only present when the producer sends it, so rules need a default


# Every variable a rule can read. All but ``velocity.*`` are fields of the event
# (``TransactionCreatedV1`` and its ``Enrichment``) with the same type and the same
# required/optional status; tests/unit/test_rules.py checks that they stay in step.
# ``velocity.*`` are computed by the worker from the same user's *other* transactions
# whose event time falls inside the rule set's velocity window; ``prior_amount`` only
# sums transactions in the same currency.
VARIABLES: dict[str, Variable] = {
    "amount": Variable(ValueType.NUMBER, required=True),
    "currency": Variable(ValueType.TEXT, required=True),
    "category": Variable(ValueType.TEXT, required=True),
    "velocity.prior_count": Variable(ValueType.NUMBER, required=True),
    "velocity.prior_amount": Variable(ValueType.NUMBER, required=True),
    "channel": Variable(ValueType.TEXT, required=False),
    "country": Variable(ValueType.TEXT, required=False),
    "enrichment.distance_from_last_tx_km": Variable(ValueType.NUMBER, required=False),
    "enrichment.minutes_since_last_tx": Variable(ValueType.NUMBER, required=False),
    "enrichment.user_historical_avg_spend": Variable(ValueType.NUMBER, required=False),
    "enrichment.is_new_merchant_for_user": Variable(ValueType.BOOLEAN, required=False),
    "enrichment.is_travel_itinerary_active": Variable(ValueType.BOOLEAN, required=False),
}
# A default of the variable's type, suggested when a rule leaves one out.
_EXAMPLE_DEFAULTS = {ValueType.NUMBER: "0", ValueType.TEXT: '"UNKNOWN"', ValueType.BOOLEAN: "false"}

# Distinguishes "variable absent" from a variable whose value is None/False/0.
_MISSING = object()


def _to_decimal(value: Any, operator: str) -> Decimal:
    """Coerce a number to Decimal, rejecting everything else (including bool, an int subclass)."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise RuleEvaluationError(f"'{operator}' expects numbers, got {type(value).__name__}")
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _numbers(args: list[Any], operator: str) -> list[Decimal]:
    return [_to_decimal(arg, operator) for arg in args]


def _compare(operator: str, check: Callable[[Decimal, Decimal], bool]):
    """Build a numeric comparison operator; comparing non-numbers is an evaluation error."""

    def apply(*args: Any) -> bool:
        values = _numbers(list(args), operator)
        # JsonLogic "between": {"<": [a, b, c]} means a < b < c.
        return all(check(left, right) for left, right in itertools.pairwise(values))

    return apply


def _equals(left: Any, right: Any) -> bool:
    """Numbers compare by value (5 == 5.0); anything else compares as-is."""
    if isinstance(left, (int, float, Decimal)) and not isinstance(left, bool):
        if isinstance(right, (int, float, Decimal)) and not isinstance(right, bool):
            return _to_decimal(left, "==") == _to_decimal(right, "==")
    return left == right


def _membership(needle: Any, haystack: Any) -> bool:
    """JsonLogic ``in``: an item in a list, or a substring of a string."""
    if isinstance(haystack, str):
        return isinstance(needle, str) and needle in haystack
    if isinstance(haystack, list):
        return any(_equals(needle, item) for item in haystack)
    raise RuleEvaluationError("'in' expects a list or string as its second argument")


def _subtract(*args: Any) -> Decimal:
    values = _numbers(list(args), "-")
    return -values[0] if len(values) == 1 else values[0] - values[1]


def _multiply(*args: Any) -> Decimal:
    product = Decimal(1)
    for value in _numbers(list(args), "*"):
        product *= value
    return product


# operator -> (minimum args, maximum args or None for unbounded, implementation)
_OPERATORS: dict[str, tuple[int, int | None, Callable[..., Any]]] = {
    "==": (2, 2, _equals),
    "!=": (2, 2, lambda left, right: not _equals(left, right)),
    ">": (2, 2, _compare(">", lambda left, right: left > right)),
    ">=": (2, 2, _compare(">=", lambda left, right: left >= right)),
    "<": (2, 3, _compare("<", lambda left, right: left < right)),
    "<=": (2, 3, _compare("<=", lambda left, right: left <= right)),
    "!": (1, 1, lambda value: not value),
    "in": (2, 2, _membership),
    "+": (1, None, lambda *args: sum(_numbers(list(args), "+"), Decimal(0))),
    "-": (1, 2, _subtract),
    "*": (1, None, _multiply),
    "min": (1, None, lambda *args: min(_numbers(list(args), "min"))),
    "max": (1, None, lambda *args: max(_numbers(list(args), "max"))),
}
# Operators evaluated lazily (short-circuit) are handled in ``_evaluate``.
_LAZY_OPERATORS: dict[str, tuple[int, int | None]] = {
    "and": (1, None),
    "or": (1, None),
    "if": (3, 3),
    "var": (1, 2),
}
SUPPORTED_OPERATORS: frozenset[str] = frozenset(_OPERATORS) | frozenset(_LAZY_OPERATORS)


def _normalise_literal(value: Any) -> Any:
    """Convert JSON numbers to Decimal so literals and context values compare exactly."""
    if isinstance(value, bool) or value is None or isinstance(value, (str, Decimal)):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_normalise_literal(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalise_literal(item) for key, item in value.items()}
    raise InvalidRuleSetError(f"Unsupported literal of type {type(value).__name__}")


def _operator_args(node: dict[str, Any]) -> tuple[str, list[Any]]:
    ((operator, raw_args),) = node.items()
    return operator, raw_args if isinstance(raw_args, list) else [raw_args]


def _list_of(element: str | None) -> str:
    """The type of a list literal; ``element`` is None for an empty list."""
    return f"a list of {element or 'anything'}"


def _literal_type(value: Any) -> str:
    if value is None:
        return ValueType.NULL
    if isinstance(value, bool):  # before numbers: bool is a subclass of int
        return ValueType.BOOLEAN
    if isinstance(value, (int, float, Decimal)):
        return ValueType.NUMBER
    return ValueType.TEXT


def validate_expression(expression: Any, path: str = "condition") -> None:
    """Statically check a condition before it is stored, including its types.

    Every node's type is worked out from its literals and the declared type of each
    variable, and checked against what its operator needs. This catches type mistakes
    anywhere in the tree, including branches a short-circuiting ``and``/``or``/``if``
    would skip on any particular transaction.

    Raises:
        InvalidRuleSetError: For unknown operators, wrong arity, unknown variables, a
            missing or wrongly typed default, a type mismatch, a condition that is not
            true/false, or an expression that is too deep or too large.
    """
    node_count = 0

    def visit(node: Any, location: str, depth: int) -> str:
        """Check ``node`` and return its type."""
        nonlocal node_count
        node_count += 1
        if node_count > MAX_EXPRESSION_NODES:
            raise InvalidRuleSetError(f"{path}: expression has more than {MAX_EXPRESSION_NODES} nodes")
        if depth > MAX_EXPRESSION_DEPTH:
            raise InvalidRuleSetError(f"{location}: expression is nested deeper than {MAX_EXPRESSION_DEPTH}")
        if isinstance(node, list):
            element_types = {visit(item, f"{location}[{index}]", depth + 1) for index, item in enumerate(node)}
            if len(element_types) > 1:
                raise InvalidRuleSetError(
                    f"{location}: a list must hold values of one type, got {sorted(element_types)}"
                )
            return _list_of(next(iter(element_types), None))
        if not isinstance(node, dict):
            if not (node is None or isinstance(node, (bool, int, float, str, Decimal))):
                raise InvalidRuleSetError(f"{location}: unsupported literal")
            if isinstance(node, (float, Decimal)) and not Decimal(str(node)).is_finite():
                raise InvalidRuleSetError(f"{location}: numbers must be finite")
            return _literal_type(node)
        if len(node) != 1:
            raise InvalidRuleSetError(f"{location}: an operation must have exactly one operator key")
        operator, args = _operator_args(node)
        if operator not in SUPPORTED_OPERATORS:
            raise InvalidRuleSetError(f"{location}: unsupported operator '{operator}'")
        minimum, maximum = _LAZY_OPERATORS[operator] if operator in _LAZY_OPERATORS else _OPERATORS[operator][:2]
        if len(args) < minimum or (maximum is not None and len(args) > maximum):
            expected = f"{minimum}" if minimum == maximum else f"{minimum}..{maximum or 'n'}"
            raise InvalidRuleSetError(f"{location}: '{operator}' takes {expected} arguments, got {len(args)}")

        if operator == "var":
            return check_variable(args, location, depth)
        types = [visit(arg, f"{location}.{operator}[{index}]", depth + 1) for index, arg in enumerate(args)]

        def require(expected: str, purpose: str) -> None:
            for index, actual in enumerate(types):
                if actual != expected:
                    raise InvalidRuleSetError(
                        f"{location}.{operator}[{index}]: '{operator}' {purpose}, but this is {actual}"
                    )

        if operator in {">", ">=", "<", "<=", "+", "-", "*", "min", "max"}:
            require(ValueType.NUMBER, "works on numbers")
            return ValueType.BOOLEAN if operator in {">", ">=", "<", "<="} else ValueType.NUMBER
        if operator in {"and", "or", "!"}:
            require(ValueType.BOOLEAN, "combines true/false conditions")
            return ValueType.BOOLEAN
        if operator == "if":
            condition, when_true, when_false = types
            if condition != ValueType.BOOLEAN:
                raise InvalidRuleSetError(
                    f"{location}.if[0]: the condition of 'if' must be true/false, not {condition}"
                )
            if when_true != when_false:
                raise InvalidRuleSetError(
                    f"{location}: both branches of 'if' must have the same type, got {when_true} and {when_false}"
                )
            return when_true
        if operator in {"==", "!="}:
            left, right = types
            if left != right and ValueType.NULL not in (left, right):
                raise InvalidRuleSetError(
                    f"{location}: '{operator}' compares {left} with {right}, which can never be equal"
                )
            return ValueType.BOOLEAN
        # "in": a value in a list of the same type, or text inside text.
        needle, haystack = types
        if haystack == ValueType.TEXT:
            if needle != ValueType.TEXT:
                raise InvalidRuleSetError(f"{location}: 'in' looks for text inside text, but the value is {needle}")
        elif haystack.startswith("a list of "):
            element = haystack.removeprefix("a list of ")
            if element != "anything" and element != needle:
                raise InvalidRuleSetError(f"{location}: 'in' looks for {needle} in {haystack}, which can never match")
        else:
            raise InvalidRuleSetError(f"{location}.in[1]: 'in' needs a list or text, but this is {haystack}")
        return ValueType.BOOLEAN

    def check_variable(args: list[Any], location: str, depth: int) -> str:
        name = args[0]
        if not isinstance(name, str) or name not in VARIABLES:
            raise InvalidRuleSetError(f"{location}: unknown variable {name!r}; allowed: {sorted(VARIABLES)}")
        variable = VARIABLES[name]
        if len(args) == 1:
            if not variable.required:
                raise InvalidRuleSetError(
                    f"{location}: '{name}' is optional in events, so it needs a default of type "
                    f'{variable.type}, e.g. {{"var": ["{name}", {_EXAMPLE_DEFAULTS[variable.type]}]}}'
                )
            return variable.type
        default_type = visit(args[1], f"{location}.var[1]", depth + 1)
        if default_type != variable.type:
            raise InvalidRuleSetError(
                f"{location}: the default for '{name}' must be {variable.type} (its type), "
                f'not {default_type}, e.g. {{"var": ["{name}", {_EXAMPLE_DEFAULTS[variable.type]}]}}'
            )
        return variable.type

    result = visit(expression, path, 1)
    if result != ValueType.BOOLEAN:
        raise InvalidRuleSetError(f"{path}: a condition must be true/false, but this one is {result}")


def _lookup(context: dict[str, Any], dotted_name: str) -> Any:
    """Resolve ``velocity.prior_count``-style names; ``_MISSING`` if any part is absent."""
    current: Any = context
    for part in dotted_name.split("."):
        if not isinstance(current, dict) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _evaluate(node: Any, context: dict[str, Any]) -> Any:
    """Evaluate one already-validated expression against the transaction's variables.

    ``and``, ``or`` and ``if`` are handled here rather than in ``_OPERATORS`` because
    they must not evaluate every argument: ``or`` stops at the first truthy one, and
    ``if`` evaluates only the branch it takes.
    """
    if isinstance(node, list):
        return [_evaluate(item, context) for item in node]
    if not isinstance(node, dict):
        return node
    operator, args = _operator_args(node)
    if operator == "var":
        value = _lookup(context, args[0])
        if value is _MISSING:
            if len(args) == 2:
                return _evaluate(args[1], context)
            raise RuleEvaluationError(f"variable '{args[0]}' is not available in this context")
        return value
    if operator == "and":
        return all(bool(_evaluate(arg, context)) for arg in args)
    if operator == "or":
        return any(bool(_evaluate(arg, context)) for arg in args)
    if operator == "if":
        condition, when_true, when_false = args
        return _evaluate(when_true if _evaluate(condition, context) else when_false, context)
    _, _, implementation = _OPERATORS[operator]
    try:
        return implementation(*(_evaluate(arg, context) for arg in args))
    except (InvalidOperation, ArithmeticError) as exc:
        raise RuleEvaluationError(f"'{operator}' failed: {exc}") from exc


class RuleDefinition(BaseModel):
    """One rule: if ``condition`` is truthy for a transaction, add ``score`` to its risk."""

    model_config = ConfigDict(extra="forbid")

    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{2,63}$", description="Stable code reported when the rule fires.")
    description: str = Field(min_length=1, max_length=500)
    score: int = Field(ge=0, le=1000, description="Risk points added to the total when the rule fires.")
    condition: dict[str, Any] = Field(description="JsonLogic expression that is truthy when the rule fires.")

    @field_validator("condition")
    @classmethod
    def _validate_condition(cls, value: dict[str, Any]) -> dict[str, Any]:
        try:
            validate_expression(value)
        except InvalidRuleSetError as exc:
            raise ValueError(str(exc)) from None
        return value


class RuleSetDefinition(BaseModel):
    """The stored, versioned body of a rule set."""

    model_config = ConfigDict(extra="forbid")

    velocity_window_seconds: int = Field(
        ge=1, le=86_400, description="Look-back window, in event time, for the velocity.* variables."
    )
    review_threshold: int = Field(ge=1, description="Total score at or above which the verdict is REVIEW.")
    reject_threshold: int = Field(ge=1, description="Total score at or above which the verdict is REJECTED.")
    rules: list[RuleDefinition] = Field(min_length=1, max_length=50)

    # (code, score, condition with Decimal literals), computed once rather than per message.
    _compiled: list[tuple[str, int, Any]] = PrivateAttr(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        self._compiled = [(rule.code, rule.score, _normalise_literal(rule.condition)) for rule in self.rules]

    @model_validator(mode="after")
    def _ordered_thresholds(self) -> RuleSetDefinition:
        if self.reject_threshold < self.review_threshold:
            raise ValueError("reject_threshold must be greater than or equal to review_threshold")
        return self

    def verdict_for(self, risk_score: int) -> Verdict:
        """Map a total score to a verdict using this version's thresholds."""
        if risk_score >= self.reject_threshold:
            return Verdict.REJECTED
        if risk_score >= self.review_threshold:
            return Verdict.REVIEW
        return Verdict.APPROVED

    @field_validator("rules")
    @classmethod
    def _unique_codes(cls, rules: list[RuleDefinition]) -> list[RuleDefinition]:
        codes = [rule.code for rule in rules]
        duplicates = sorted({code for code in codes if codes.count(code) > 1})
        if duplicates:
            raise ValueError(f"rule codes must be unique; duplicated: {duplicates}")
        return rules


def parse_rule_set_definition(raw: Any) -> RuleSetDefinition:
    """Validate a raw rule-set body.

    Raises:
        InvalidRuleSetError: With field-level details when validation fails.
    """
    try:
        definition = RuleSetDefinition.model_validate(raw)
    except ValidationError as exc:
        details = [
            {"field": ".".join(str(part) for part in error["loc"]), "message": error["msg"]}
            for error in exc.errors(include_input=False, include_url=False)
        ]
        raise InvalidRuleSetError("Rule set definition is invalid", details) from None

    # Static checks cannot catch type errors such as comparing ``category`` with a
    # number. A dry run against a representative context rejects those at write
    # time, instead of every transaction failing once the rule set is live.
    try:
        evaluate_rule_set(definition, _DRY_RUN_CONTEXT)
    except RuleEvaluationError as exc:
        raise InvalidRuleSetError("Rule set definition is invalid", [{"field": "rules", "message": str(exc)}]) from None
    return definition


def definition_json_hash(raw: Any) -> str:
    """SHA-256 of a definition's canonical JSON form (sorted keys, no whitespace)."""
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def content_hash(definition: RuleSetDefinition) -> str:
    """The hash stored with a new version; identical rules always hash identically.

    It covers exactly the JSON that is stored (``model_dump(mode="json")``), so
    :func:`load_stored_definition` can recheck it from the stored JSON alone.
    """
    return definition_json_hash(definition.model_dump(mode="json"))


@dataclass(frozen=True)
class RuleSet:
    """One stored version of a rule set, with the metadata recorded on every decision."""

    key: str
    version: int
    definition: RuleSetDefinition
    content_hash: str
    description: str
    created_by: str
    created_at: datetime

    @property
    def label(self) -> str:
        """The value stored on each decision to record which rules produced it."""
        return f"{self.key}@v{self.version}"


class Verdict(StrEnum):
    """Recommended action. Scoring runs after authorisation, so REJECTED means
    "highest risk: restrict and investigate", not a declined payment."""

    APPROVED = "APPROVED"
    REVIEW = "REVIEW"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class EvaluationContext:
    """Everything a rule can read about one transaction, including its velocity."""

    amount: Decimal
    currency: str
    category: str
    prior_count: int
    prior_amount: Decimal
    channel: str | None = None
    country: str | None = None
    enrichment: dict[str, Any] = field(default_factory=dict)

    def as_variables(self) -> dict[str, Any]:
        """The nested dict that ``var`` paths such as ``velocity.prior_count`` resolve against."""
        variables: dict[str, Any] = {
            "amount": self.amount,
            "currency": self.currency,
            "category": self.category,
            "velocity": {"prior_count": Decimal(self.prior_count), "prior_amount": self.prior_amount},
            # Absent optional values are left out so the rule's default applies.
            "enrichment": {
                name: _normalise_literal(value) for name, value in self.enrichment.items() if value is not None
            },
        }
        if self.channel is not None:
            variables["channel"] = self.channel
        if self.country is not None:
            variables["country"] = self.country
        return variables


@dataclass(frozen=True)
class Assessment:
    """The outcome of scoring one transaction."""

    triggered_rules: list[str]
    risk_score: int
    verdict: Verdict

    @property
    def is_fraud(self) -> bool:
        """Flagged as potential fraud: anything that is not APPROVED."""
        return self.verdict is not Verdict.APPROVED


def evaluate_rule_set(definition: RuleSetDefinition, context: EvaluationContext) -> Assessment:
    """Fire every rule, sum the scores of those that match, and map the total to a verdict.

    Raises:
        RuleEvaluationError: If a rule cannot be evaluated against this context.
    """
    variables = context.as_variables()
    # Every rule is evaluated, even once the reject threshold is reached, so the
    # decision lists all the reasons it was flagged.
    triggered: list[str] = []
    risk_score = 0
    for code, score, condition in definition._compiled:
        try:
            fired = _evaluate(condition, variables)
        except RuleEvaluationError as exc:
            raise RuleEvaluationError(f"rule {code}: {exc}") from exc
        if fired:
            triggered.append(code)
            risk_score += score
    return Assessment(triggered_rules=triggered, risk_score=risk_score, verdict=definition.verdict_for(risk_score))


_DRY_RUN_CONTEXT = EvaluationContext(
    amount=Decimal("100.00"), currency="ZAR", category="GROCERIES", prior_count=1, prior_amount=Decimal("100.00")
)


def load_stored_definition(raw: Any, stored_hash: str, label: str) -> RuleSetDefinition:
    """Rebuild a stored definition, refusing one that fails any integrity check.

    Run on every load (from PostgreSQL or Redis), because the stored row can be
    changed outside the API. The hash is checked against the stored JSON itself,
    not a re-serialised model, so a later change to the model (a new optional
    field, say) cannot make old versions fail.

    Raises:
        CorruptRuleSetError: The JSON does not match ``stored_hash`` (edited in
            place: decisions would record rules that did not produce them), no
            longer validates, or fails the publish-time dry run.
    """
    if definition_json_hash(raw) != stored_hash:
        raise CorruptRuleSetError(f"rule set {label} does not match its content hash; it was changed after publishing")
    try:
        definition = RuleSetDefinition.model_validate(raw)
    except ValidationError as exc:
        reason = exc.errors(include_input=False, include_url=False)[0]["msg"]
        raise CorruptRuleSetError(f"rule set {label} is not a valid definition: {reason}") from None
    try:
        evaluate_rule_set(definition, _DRY_RUN_CONTEXT)
    except RuleEvaluationError as exc:
        raise CorruptRuleSetError(f"rule set {label} fails the dry run: {exc}") from None
    return definition


# The default rule set: version 1 of "fraud-default", seeded by the first migration.
# The migration keeps its own frozen copy; a test checks the two have the same hash.
# Change live rules by publishing a new version through the API, not by editing this.
#
# Rules carry no type annotations: each variable's type is declared once in VARIABLES,
# and validate_expression infers the rest (``amount >= 10000`` is number >= number,
# giving true/false). A test checks every rule here passes that type check.
#
# Amounts are in ZAR. Scores: one strong signal reaches REVIEW (>= 30); two
# signals together, or one very strong one, reach REJECTED (>= 75).
DEFAULT_RULE_SET_DEFINITION: dict[str, Any] = {
    "velocity_window_seconds": 60,
    "review_threshold": 30,
    "reject_threshold": 75,
    "rules": [
        {
            "code": "HIGH_VALUE_THRESHOLD",
            "description": "A single transaction of R10,000.00 or more.",
            "score": 40,
            "condition": {">=": [{"var": "amount"}, 10000]},
        },
        {
            "code": "RISKY_CATEGORY_LIMIT",
            "description": "More than R5,000.00 in a high-risk category (GAMBLING, CRYPTO).",
            "score": 50,
            "condition": {
                "and": [
                    {"in": [{"var": "category"}, ["GAMBLING", "CRYPTO"]]},
                    {">": [{"var": "amount"}, 5000]},
                ]
            },
        },
        {
            "code": "VELOCITY_COUNT_EXCEEDED",
            "description": "More than 5 transactions by the same user within the velocity window.",
            "score": 40,
            "condition": {">": [{"+": [{"var": "velocity.prior_count"}, 1]}, 5]},
        },
        {
            "code": "VELOCITY_AMOUNT",
            "description": "More than R50,000.00 spent by the same user within the velocity window.",
            "score": 50,
            "condition": {">": [{"+": [{"var": "velocity.prior_amount"}, {"var": "amount"}]}, 50000]},
        },
        {
            "code": "FOREIGN_CURRENCY",
            "description": "Not in ZAR: the amount thresholds above do not apply, so send it for review.",
            "score": 30,
            "condition": {"!=": [{"var": "currency"}, "ZAR"]},
        },
    ],
}
