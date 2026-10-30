from copy import deepcopy
from decimal import Decimal

import pytest

from app.errors import InvalidRuleSetError, RuleEvaluationError
from app.rules import (
    DEFAULT_RULE_SET_DEFINITION,
    MAX_EXPRESSION_DEPTH,
    EvaluationContext,
    Verdict,
    content_hash,
    evaluate_rule_set,
    parse_rule_set_definition,
    validate_expression,
)

DEFAULT = parse_rule_set_definition(DEFAULT_RULE_SET_DEFINITION)


def context(amount="100.00", category="GROCERIES", prior_count=0, prior_amount="0", currency="ZAR", **optional):
    return EvaluationContext(
        amount=Decimal(amount),
        currency=currency,
        category=category,
        prior_count=prior_count,
        prior_amount=Decimal(prior_amount),
        **optional,
    )


def fired(definition, ctx):
    return evaluate_rule_set(definition, ctx).triggered_rules


def single_rule(condition, window=60, score=10):
    return parse_rule_set_definition(
        {
            "velocity_window_seconds": window,
            "review_threshold": 30,
            "reject_threshold": 75,
            "rules": [{"code": "TEST_RULE", "description": "t", "score": score, "condition": condition}],
        }
    )


class TestDefaultRuleBoundaries:
    """Each default rule at, just below and just above its threshold."""

    @pytest.mark.parametrize(("amount", "fires"), [("9999.99", False), ("10000.00", True), ("10000.01", True)])
    def test_high_value_threshold_is_inclusive_at_10000(self, amount, fires):
        assert ("HIGH_VALUE_THRESHOLD" in fired(DEFAULT, context(amount=amount))) is fires

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
        triggered = fired(DEFAULT, context(amount=amount, category=category))
        assert ("RISKY_CATEGORY_LIMIT" in triggered) is fires

    @pytest.mark.parametrize(("prior_count", "fires"), [(4, False), (5, True)])
    def test_velocity_count_fires_on_the_sixth_transaction_in_window(self, prior_count, fires):
        triggered = fired(DEFAULT, context(prior_count=prior_count))
        assert ("VELOCITY_COUNT_EXCEEDED" in triggered) is fires

    @pytest.mark.parametrize(("amount", "fires"), [("0.01", False), ("0.02", True)])
    def test_velocity_amount_is_exclusive_at_50000_with_exact_decimals(self, amount, fires):
        triggered = fired(DEFAULT, context(amount=amount, prior_amount="49999.99"))
        assert ("VELOCITY_AMOUNT" in triggered) is fires

    def test_multiple_rules_report_in_definition_order(self):
        triggered = fired(DEFAULT, context(amount="20000.00", category="CRYPTO", prior_count=9, prior_amount="40000"))
        assert triggered == [
            "HIGH_VALUE_THRESHOLD",
            "RISKY_CATEGORY_LIMIT",
            "VELOCITY_COUNT_EXCEEDED",
            "VELOCITY_AMOUNT",
        ]

    def test_ordinary_transaction_triggers_nothing(self):
        assert fired(DEFAULT, context()) == []


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
        assert bool(fired(single_rule(condition), context())) is expected

    def test_var_default_applies_only_when_variable_is_missing(self):
        assert fired(single_rule({"==": [{"var": ["amount", 0]}, 100]}), context()) == ["TEST_RULE"]

    def test_type_error_at_runtime_raises_rule_evaluation_error(self):
        # Passes the dry run because "and" short-circuits on the GROCERIES sample.
        rule = single_rule({"and": [{"==": [{"var": "category"}, "CRYPTO"]}, {">": [{"var": "category"}, 1]}]})
        with pytest.raises(RuleEvaluationError, match="TEST_RULE"):
            fired(rule, context(category="CRYPTO"))


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
            {**DEFAULT_RULE_SET_DEFINITION, "velocity_window_seconds": 0},
            {**DEFAULT_RULE_SET_DEFINITION, "unexpected": True},
            {
                **DEFAULT_RULE_SET_DEFINITION,
                "rules": [{"code": "lower", "description": "x", "score": 1, "condition": {}}],
            },
            {**DEFAULT_RULE_SET_DEFINITION, "review_threshold": 80, "reject_threshold": 50},
            {
                **DEFAULT_RULE_SET_DEFINITION,
                "rules": [{"code": "NO_SCORE", "description": "x", "condition": {"!": [0]}}],
            },
        ],
    )
    def test_structural_errors_reported_with_field_paths(self, body):
        with pytest.raises(InvalidRuleSetError) as error:
            parse_rule_set_definition(body)
        assert error.value.details and all("field" in item for item in error.value.details)


def test_content_hash_is_independent_of_key_order():
    reordered = dict(reversed(list(DEFAULT_RULE_SET_DEFINITION.items())))
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


class TestScoringAndVerdicts:
    @pytest.mark.parametrize(
        ("kwargs", "score", "verdict"),
        [
            ({}, 0, Verdict.APPROVED),
            ({"amount": "10000.00"}, 40, Verdict.REVIEW),
            ({"amount": "5000.01", "category": "CRYPTO"}, 50, Verdict.REVIEW),
            ({"amount": "10000.00", "category": "CRYPTO"}, 90, Verdict.REJECTED),
            ({"amount": "30000.00", "prior_amount": "25000.00"}, 90, Verdict.REJECTED),
            ({"currency": "USD"}, 30, Verdict.REVIEW),
        ],
    )
    def test_scores_sum_and_map_to_bands(self, kwargs, score, verdict):
        assessment = evaluate_rule_set(DEFAULT, context(**kwargs))
        assert (assessment.risk_score, assessment.verdict) == (score, verdict)
        assert assessment.is_fraud is (verdict is not Verdict.APPROVED)

    @pytest.mark.parametrize(
        ("score", "verdict"),
        [(29, Verdict.APPROVED), (30, Verdict.REVIEW), (74, Verdict.REVIEW), (75, Verdict.REJECTED)],
    )
    def test_band_boundaries_are_inclusive_lower_bounds(self, score, verdict):
        rule = single_rule({"==": [1, 1]}, score=score)
        assert evaluate_rule_set(rule, context()).verdict is verdict


class TestOptionalVariables:
    def test_optional_variable_without_default_is_rejected(self):
        with pytest.raises(InvalidRuleSetError) as error:
            single_rule({"==": [{"var": "channel"}, "ATM"]})
        assert "needs a default" in str(error.value.details)

    @pytest.mark.parametrize(
        ("optional", "fires"),
        [
            ({}, False),  # absent -> default 0
            ({"enrichment": {"distance_from_last_tx_km": Decimal("850.5")}}, True),
            ({"enrichment": {"distance_from_last_tx_km": Decimal("4")}}, False),
        ],
    )
    def test_enrichment_feature_uses_default_when_absent(self, optional, fires):
        rule = single_rule({">": [{"var": ["enrichment.distance_from_last_tx_km", 0]}, 500]})
        assert bool(fired(rule, context(**optional))) is fires

    def test_channel_and_country_are_available_when_sent(self):
        rule = single_rule(
            {"and": [{"==": [{"var": ["channel", "UNKNOWN"]}, "ONLINE"]}, {"!=": [{"var": ["country", "ZA"]}, "ZA"]}]}
        )
        assert fired(rule, context(channel="ONLINE", country="NG")) == ["TEST_RULE"]
        assert fired(rule, context(channel="ONLINE")) == []
