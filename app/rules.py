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
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError, field_validator

from app.errors import InvalidRuleSetError, RuleEvaluationError

MAX_EXPRESSION_DEPTH = 24
MAX_EXPRESSION_NODES = 400

# Variables a rule may read. ``velocity.*`` describe the same user's *other*
# transactions whose event time falls inside the rule set's velocity window.
CONTEXT_VARIABLES: frozenset[str] = frozenset({"amount", "category", "velocity.prior_count", "velocity.prior_amount"})

_MISSING = object()


def _to_decimal(value: Any, operator: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise RuleEvaluationError(f"'{operator}' expects numbers, got {type(value).__name__}")
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _numbers(args: list[Any], operator: str) -> list[Decimal]:
    return [_to_decimal(arg, operator) for arg in args]


def _compare(operator: str, check: Callable[[Decimal, Decimal], bool]):
    def apply(*args: Any) -> bool:
        values = _numbers(list(args), operator)
        # JsonLogic "between": {"<": [a, b, c]} means a < b < c.
        return all(check(left, right) for left, right in itertools.pairwise(values))

    return apply


def _equals(left: Any, right: Any) -> bool:
    if isinstance(left, (int, float, Decimal)) and not isinstance(left, bool):
        if isinstance(right, (int, float, Decimal)) and not isinstance(right, bool):
            return _to_decimal(left, "==") == _to_decimal(right, "==")
    return left == right


def _membership(needle: Any, haystack: Any) -> bool:
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


def validate_expression(expression: Any, path: str = "condition") -> None:
    """Statically check an expression before it is stored.

    Raises:
        InvalidRuleSetError: For unknown operators, wrong arity, unknown variables,
            or expressions that are too deep or too large.
    """
    node_count = 0

    def visit(node: Any, location: str, depth: int) -> None:
        nonlocal node_count
        node_count += 1
        if node_count > MAX_EXPRESSION_NODES:
            raise InvalidRuleSetError(f"{path}: expression has more than {MAX_EXPRESSION_NODES} nodes")
        if depth > MAX_EXPRESSION_DEPTH:
            raise InvalidRuleSetError(f"{location}: expression is nested deeper than {MAX_EXPRESSION_DEPTH}")
        if isinstance(node, list):
            for index, item in enumerate(node):
                visit(item, f"{location}[{index}]", depth + 1)
            return
        if not isinstance(node, dict):
            if not (node is None or isinstance(node, (bool, int, float, str, Decimal))):
                raise InvalidRuleSetError(f"{location}: unsupported literal")
            if isinstance(node, (float, Decimal)) and not Decimal(str(node)).is_finite():
                raise InvalidRuleSetError(f"{location}: numbers must be finite")
            return
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
            name = args[0]
            if not isinstance(name, str) or name not in CONTEXT_VARIABLES:
                raise InvalidRuleSetError(
                    f"{location}: unknown variable {name!r}; allowed: {sorted(CONTEXT_VARIABLES)}"
                )
            if len(args) == 2:
                visit(args[1], f"{location}.var[1]", depth + 1)
            return
        for index, arg in enumerate(args):
            visit(arg, f"{location}.{operator}[{index}]", depth + 1)

    visit(expression, path, 1)


def _lookup(context: dict[str, Any], dotted_name: str) -> Any:
    current: Any = context
    for part in dotted_name.split("."):
        if not isinstance(current, dict) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _evaluate(node: Any, context: dict[str, Any]) -> Any:
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
    model_config = ConfigDict(extra="forbid")

    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{2,63}$", description="Stable code reported when the rule fires.")
    description: str = Field(min_length=1, max_length=500)
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
    rules: list[RuleDefinition] = Field(min_length=1, max_length=50)

    # (code, condition with Decimal literals), computed once rather than per message.
    _compiled: list[tuple[str, Any]] = PrivateAttr(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        self._compiled = [(rule.code, _normalise_literal(rule.condition)) for rule in self.rules]

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


def content_hash(definition: RuleSetDefinition) -> str:
    """SHA-256 of the canonical JSON form; identical rules always hash identically."""
    canonical = json.dumps(definition.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RuleSet:
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


@dataclass(frozen=True)
class EvaluationContext:
    amount: Decimal
    category: str
    prior_count: int
    prior_amount: Decimal

    def as_variables(self) -> dict[str, Any]:
        return {
            "amount": self.amount,
            "category": self.category,
            "velocity": {"prior_count": Decimal(self.prior_count), "prior_amount": self.prior_amount},
        }


def evaluate_rule_set(definition: RuleSetDefinition, context: EvaluationContext) -> list[str]:
    """Return the codes of every rule that fires, in definition order.

    Raises:
        RuleEvaluationError: If a rule cannot be evaluated against this context.
    """
    variables = context.as_variables()
    triggered: list[str] = []
    for code, condition in definition._compiled:
        try:
            fired = _evaluate(condition, variables)
        except RuleEvaluationError as exc:
            raise RuleEvaluationError(f"rule {code}: {exc}") from exc
        if fired:
            triggered.append(code)
    return triggered


_DRY_RUN_CONTEXT = EvaluationContext(
    amount=Decimal("100.00"), category="GROCERIES", prior_count=1, prior_amount=Decimal("100.00")
)


DEFAULT_RULE_SET_DEFINITION: dict[str, Any] = {
    "velocity_window_seconds": 60,
    "rules": [
        {
            "code": "HIGH_VALUE_THRESHOLD",
            "description": "A single transaction of 10,000.00 or more.",
            "condition": {">=": [{"var": "amount"}, 10000]},
        },
        {
            "code": "RISKY_CATEGORY_LIMIT",
            "description": "More than 5,000.00 in a high-risk category (GAMBLING, CRYPTO).",
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
            "condition": {">": [{"+": [{"var": "velocity.prior_count"}, 1]}, 5]},
        },
        {
            "code": "VELOCITY_AMOUNT",
            "description": "More than 50,000.00 spent by the same user within the velocity window.",
            "condition": {">": [{"+": [{"var": "velocity.prior_amount"}, {"var": "amount"}]}, 50000]},
        },
    ],
}
