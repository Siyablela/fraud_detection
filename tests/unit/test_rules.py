from copy import deepcopy
from decimal import Decimal

import pytest

from app.errors import InvalidRuleSetError, RuleEvaluationError
from app.rules import (
    DEFAULT_RULE_SET_DEFINITION,
    MAX_EXPRESSION_DEPTH,
    EvaluationContext,
    content_hash,
    evaluate_rule_set,
    parse_rule_set_definition,
    validate_expression,
)

DEFAULT = parse_rule_set_definition(DEFAULT_RULE_SET_DEFINITION)


def context(amount="100.00", category="GROCERIES", prior_count=0, prior_amount="0"):
    return EvaluationContext(
        amount=Decimal(amount), category=category, prior_count=prior_count, prior_amount=Decimal(prior_amount)
    )


def single_rule(condition, window=60):
    return parse_rule_set_definition(
        {
            "velocity_window_seconds": window,
            "rules": [{"code": "TEST_RULE", "description": "t", "condition": condition}],
        }
    )


class TestDefaultRuleBoundaries:
    """Each default rule at, just below and just above its threshold."""

    @pytest.mark.parametrize(("amount", "fires"), [("9999.99", False), ("10000.00", True), ("10000.01", True)])
    def test_high_value_threshold_is_inclusive_at_10000(self, amount, fires):
        assert ("HIGH_VALUE_THRESHOLD" in evaluate_rule_set(DEFAULT, context(amount=amount))) is fires

    @pytest.mark.parametrize(
        ("category", "amount", "fires"),
        [
            ("GAMBLING", "5000.00", False),
            ("GAMBLING", "5000.01", True),
            ("CRYPTO", "5000.01", True),
            ("GROCERIES", "9000.00", False),
        ],
    )
    def test_risky_category_limit_is_exclusive_at_5000(self, category, amount, fires):
        triggered = evaluate_rule_set(DEFAULT, context(amount=amount, category=category))
        assert ("RISKY_CATEGORY_LIMIT" in triggered) is fires

    @pytest.mark.parametrize(("prior_count", "fires"), [(4, False), (5, True)])
    def test_velocity_count_fires_on_the_sixth_transaction_in_window(self, prior_count, fires):
        triggered = evaluate_rule_set(DEFAULT, context(prior_count=prior_count))
        assert ("VELOCITY_COUNT_EXCEEDED" in triggered) is fires

    @pytest.mark.parametrize(("amount", "fires"), [("0.01", False), ("0.02", True)])
    def test_velocity_amount_is_exclusive_at_50000_with_exact_decimals(self, amount, fires):
        triggered = evaluate_rule_set(DEFAULT, context(amount=amount, prior_amount="49999.99"))
        assert ("VELOCITY_AMOUNT" in triggered) is fires

    def test_multiple_rules_report_in_definition_order(self):
        triggered = evaluate_rule_set(
            DEFAULT, context(amount="20000.00", category="CRYPTO", prior_count=9, prior_amount="40000")
        )
        assert triggered == [
            "HIGH_VALUE_THRESHOLD",
            "RISKY_CATEGORY_LIMIT",
            "VELOCITY_COUNT_EXCEEDED",
            "VELOCITY_AMOUNT",
        ]

    def test_ordinary_transaction_triggers_nothing(self):
        assert evaluate_rule_set(DEFAULT, context()) == []


class TestOperators:
    @pytest.mark.parametrize(
        ("condition", "expected"),
        [
            ({"==": [{"+": [0.1, 0.2]}, 0.3]}, True),  # exact decimals, unlike float
            ({"!=": [{"var": "category"}, "GROCERIES"]}, False),
            ({"<": [1, {"var": "amount"}, 200]}, True),  # between
            ({"<=": [{"var": "amount"}, 100]}, True),
            ({"!": [{">": [{"var": "amount"}, 100]}]}, True),
            ({"or": [False, {"in": ["GROC", {"var": "category"}]}]}, True),
            ({"if": [{">": [{"var": "amount"}, 50]}, True, False]}, True),
            ({">": [{"*": [{"var": "amount"}, 2]}, {"-": [300, 101]}]}, True),
            ({"==": [{"min": [3, 1, 2]}, {"-": [{"max": [0, 2]}, 1]}]}, True),
            ({"==": [{"-": [5]}, -5]}, True),
        ],
    )
    def test_operator_semantics(self, condition, expected):
        assert bool(evaluate_rule_set(single_rule(condition), context())) is expected

    def test_var_default_applies_only_when_variable_is_missing(self):
        assert evaluate_rule_set(single_rule({"==": [{"var": ["amount", 0]}, 100]}), context()) == ["TEST_RULE"]

    def test_type_error_at_runtime_raises_rule_evaluation_error(self):
        # Passes the dry run because "and" short-circuits on the GROCERIES sample.
        rule = single_rule({"and": [{"==": [{"var": "category"}, "CRYPTO"]}, {">": [{"var": "category"}, 1]}]})
        with pytest.raises(RuleEvaluationError, match="TEST_RULE"):
            evaluate_rule_set(rule, context(category="CRYPTO"))


class TestValidation:
    @pytest.mark.parametrize(
        ("condition", "message"),
        [
            ({"log": ["x"]}, "unsupported operator 'log'"),
            ({"var": "user_id"}, "unknown variable"),
            ({">": [1]}, "takes 2 arguments"),
            ({">": [1, 2], "<": [1, 2]}, "exactly one operator"),
            ({">": [{"var": "amount"}, float("inf")]}, "finite"),
            ({">": [{"var": "category"}, 5]}, "expects numbers"),  # caught by the dry run
        ],
    )
    def test_invalid_conditions_are_rejected_at_write_time(self, condition, message):
        with pytest.raises(InvalidRuleSetError) as error:
            single_rule(condition)
        assert message in str(error.value.details)

    def test_depth_limit(self):
        condition: dict = {"var": "amount"}
        for _ in range(MAX_EXPRESSION_DEPTH + 1):
            condition = {"!": [condition]}
        with pytest.raises(InvalidRuleSetError, match="deeper"):
            validate_expression(condition)

    def test_node_limit(self):
        with pytest.raises(InvalidRuleSetError, match="nodes"):
            validate_expression({"+": list(range(500))})

    def test_duplicate_rule_codes_rejected(self):
        body = deepcopy(DEFAULT_RULE_SET_DEFINITION)
        body["rules"].append(deepcopy(body["rules"][0]))
        with pytest.raises(InvalidRuleSetError) as error:
            parse_rule_set_definition(body)
        assert "HIGH_VALUE_THRESHOLD" in str(error.value.details)

    @pytest.mark.parametrize(
        "body",
        [
            {"rules": []},
            {"velocity_window_seconds": 0, "rules": DEFAULT_RULE_SET_DEFINITION["rules"]},
            {**DEFAULT_RULE_SET_DEFINITION, "unexpected": True},
            {"velocity_window_seconds": 60, "rules": [{"code": "lower", "description": "x", "condition": {}}]},
        ],
    )
    def test_structural_errors_reported_with_field_paths(self, body):
        with pytest.raises(InvalidRuleSetError) as error:
            parse_rule_set_definition(body)
        assert error.value.details and all("field" in item for item in error.value.details)


def test_content_hash_is_independent_of_key_order():
    reordered = {"rules": DEFAULT_RULE_SET_DEFINITION["rules"], "velocity_window_seconds": 60}
    assert content_hash(parse_rule_set_definition(reordered)) == content_hash(DEFAULT)


def test_migration_seed_matches_application_default():
    """The migration keeps a frozen copy; it must hash identically to the app's default."""
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parents[2] / "alembic/versions/20261029_0001_initial_schema.py"
    spec = importlib.util.spec_from_file_location("initial_schema", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module._content_hash(module.DEFAULT_RULE_SET) == content_hash(DEFAULT)
