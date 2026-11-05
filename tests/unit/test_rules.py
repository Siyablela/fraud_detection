from copy import deepcopy
from decimal import Decimal
from types import UnionType
from typing import Annotated, Union, get_args, get_origin

import pytest

from app import rules
from app.errors import CorruptRuleSetError, InvalidRuleSetError, RuleEvaluationError
from app.events import Enrichment, TransactionCreatedV1
from app.rules import (
    DEFAULT_RULE_SET_DEFINITION,
    MAX_EXPRESSION_DEPTH,
    VARIABLES,
    EvaluationContext,
    ValueType,
    Verdict,
    content_hash,
    definition_json_hash,
    evaluate_rule_set,
    load_stored_definition,
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

    def test_type_mistake_behind_a_short_circuit_is_refused_at_publish(self):
        # The dry run would never reach the ">" (the sample is GROCERIES, so "and"
        # stops early); the static type check still sees it.
        with pytest.raises(InvalidRuleSetError) as error:
            single_rule({"and": [{"==": [{"var": "category"}, "CRYPTO"]}, {">": [{"var": "category"}, 1]}]})
        assert "'>' works on numbers, but this is text" in str(error.value.details)

    def test_evaluator_still_refuses_a_type_mistake_it_is_given(self):
        # Defence in depth: the evaluator checks types itself if handed an unchecked tree.
        with pytest.raises(RuleEvaluationError, match="expects numbers"):
            rules._evaluate({">": [{"var": "category"}, 1]}, context().as_variables())


class TestValidation:
    @pytest.mark.parametrize(
        ("condition", "message"),
        [
            ({"log": ["x"]}, "unsupported operator 'log'"),
            ({"var": "user_id"}, "unknown variable"),
            ({">": [1]}, "takes 2 arguments"),
            ({">": [1, 2], "<": [1, 2]}, "exactly one operator"),
            ({">": [{"var": "amount"}, float("inf")]}, "finite"),
            ({">": [{"var": "category"}, 5]}, "'>' works on numbers, but this is text"),
        ],
    )
    def test_invalid_conditions_are_rejected_at_write_time(self, condition, message):
        with pytest.raises(InvalidRuleSetError) as error:
            single_rule(condition)
        assert message in " ".join(detail["message"] for detail in error.value.details)

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


class TestLoadStoredDefinition:
    """Every load re-checks the stored JSON, because the row can be edited outside the API."""

    def stored(self, **changes):
        raw = deepcopy(DEFAULT_RULE_SET_DEFINITION)
        raw.update(changes)
        return raw

    def test_an_untouched_version_loads(self):
        raw = self.stored()
        assert load_stored_definition(raw, content_hash(DEFAULT), "fraud-default@v1") == DEFAULT

    def test_the_stored_hash_covers_the_stored_json_not_the_model(self):
        # The invariant the check relies on: hashing what is stored reproduces what was
        # stored at publish time, so later model changes cannot invalidate old versions.
        assert definition_json_hash(DEFAULT.model_dump(mode="json")) == content_hash(DEFAULT)

    def test_a_valid_edit_made_in_place_is_refused(self):
        tampered = self.stored(review_threshold=60)  # still a valid rule set
        with pytest.raises(CorruptRuleSetError, match=r"fraud-default@v1 does not match its content hash"):
            load_stored_definition(tampered, content_hash(DEFAULT), "fraud-default@v1")

    def test_an_invalid_definition_is_refused_even_with_a_matching_hash(self):
        broken = self.stored(rules=[])
        with pytest.raises(CorruptRuleSetError, match="is not a valid definition"):
            load_stored_definition(broken, definition_json_hash(broken), "fraud-default@v2")

    def test_a_definition_that_fails_the_dry_run_is_refused(self, monkeypatch):
        # The static type check now stops anything that would fail evaluation, so the
        # load-time dry run is a second line of defence; force it to fail.
        def failing_evaluation(*_args):
            raise RuleEvaluationError("rule X: boom")

        monkeypatch.setattr(rules, "evaluate_rule_set", failing_evaluation)
        with pytest.raises(CorruptRuleSetError, match="fails the dry run"):
            load_stored_definition(self.stored(), content_hash(DEFAULT), "fraud-default@v3")


class TestTypeChecking:
    """Types are checked when publishing, across the whole tree."""

    @pytest.mark.parametrize(
        ("condition", "message"),
        [
            # a default must have its variable's type
            (
                {">": [{"var": ["enrichment.is_new_merchant_for_user", 0]}, 0]},
                "the default for 'enrichment.is_new_merchant_for_user' must be true/false (its type), not a number",
            ),
            ({"==": [{"var": ["channel", 0]}, "ONLINE"]}, "the default for 'channel' must be text"),
            # comparisons, arithmetic and logic need the right types
            ({"==": [{"var": "category"}, 7995]}, "'==' compares text with a number, which can never be equal"),
            ({">=": [{"var": "amount"}, "5000"]}, "'>=' works on numbers, but this is text"),
            ({"==": [{"+": [{"var": "currency"}, 1]}, 2]}, "'+' works on numbers, but this is text"),
            ({"and": [{"var": "amount"}, True]}, "'and' combines true/false conditions, but this is a number"),
            ({"!": [{"var": "category"}]}, "'!' combines true/false conditions, but this is text"),
            ({"if": [{"var": "amount"}, True, False]}, "the condition of 'if' must be true/false"),
            ({"if": [True, 1, "x"]}, "both branches of 'if' must have the same type"),
            # "in"
            (
                {"in": [{"var": "amount"}, ["A", "B"]]},
                "'in' looks for a number in a list of text, which can never match",
            ),
            ({"in": [{"var": "category"}, 5]}, "'in' needs a list or text, but this is a number"),
            ({"in": [{"var": "category"}, ["A", 1]]}, "a list must hold values of one type"),
            # the whole condition must be true/false
            ({"+": [{"var": "amount"}, 1]}, "a condition must be true/false, but this one is a number"),
        ],
    )
    def test_type_mistakes_are_refused_at_publish(self, condition, message):
        with pytest.raises(InvalidRuleSetError) as error:
            single_rule(condition)
        assert message in " ".join(detail["message"] for detail in error.value.details)

    @pytest.mark.parametrize(
        ("name", "suggestion"),
        [
            ("channel", '{"var": ["channel", "UNKNOWN"]}'),
            ("enrichment.minutes_since_last_tx", '{"var": ["enrichment.minutes_since_last_tx", 0]}'),
            ("enrichment.is_travel_itinerary_active", '{"var": ["enrichment.is_travel_itinerary_active", false]}'),
        ],
    )
    def test_missing_default_suggests_one_of_the_right_type(self, name, suggestion):
        with pytest.raises(InvalidRuleSetError) as error:
            single_rule({"==": [{"var": name}, None]})
        assert suggestion in " ".join(detail["message"] for detail in error.value.details)

    @pytest.mark.parametrize(
        "condition",
        [
            {"==": [{"var": ["channel", "UNKNOWN"]}, None]},  # null may be compared with anything
            {"in": ["GROC", {"var": "category"}]},  # text inside text
            {"in": [{"var": "category"}, []]},  # an empty list matches nothing, but is well typed
            {"!": {"var": ["enrichment.is_new_merchant_for_user", False]}},
            {"if": [{">": [{"var": "amount"}, 1]}, {"==": [{"var": "currency"}, "ZAR"]}, False]},
        ],
    )
    def test_well_typed_conditions_are_accepted(self, condition):
        single_rule(condition)


class TestDefaultRuleSet:
    @pytest.mark.parametrize("rule", DEFAULT_RULE_SET_DEFINITION["rules"], ids=lambda rule: rule["code"])
    def test_every_default_rule_passes_the_type_check(self, rule):
        validate_expression(rule["condition"])  # raises, naming the problem, if a type is wrong


class TestVariables:
    """The variables rules can read must match the event model and what the worker supplies."""

    @staticmethod
    def _type_of(annotation: object) -> ValueType:
        """The rule type of a model field's annotation, e.g. ``Annotated[str, ...] | None`` -> text."""
        python_type = annotation
        while get_origin(python_type) in (Annotated, UnionType, Union):
            python_type = next(arg for arg in get_args(python_type) if arg is not type(None))
        if python_type is bool:
            return ValueType.BOOLEAN
        if python_type in (int, Decimal):
            return ValueType.NUMBER
        assert python_type is str, f"no rule type for {python_type}"
        return ValueType.TEXT

    def test_event_variables_exist_on_the_event_model_with_the_same_type(self):
        for name, variable in VARIABLES.items():
            if name.startswith("velocity."):
                continue  # computed by the worker, not sent in the event
            model, field = (
                (Enrichment, name.removeprefix("enrichment."))
                if name.startswith("enrichment.")
                else (
                    TransactionCreatedV1,
                    name,
                )
            )
            assert field in model.model_fields, f"{name} is not a field of {model.__name__}"
            info = model.model_fields[field]
            assert self._type_of(info.annotation) == variable.type, f"{name}: type differs from the event model"
            required_in_event = info.is_required() and model is TransactionCreatedV1
            assert required_in_event == variable.required, f"{name}: required/optional differs from the event model"

    def test_the_worker_supplies_every_variable_with_its_declared_type(self):
        full = context(
            channel="ONLINE",
            country="ZA",
            enrichment={
                "distance_from_last_tx_km": Decimal("5.2"),
                "minutes_since_last_tx": 45,
                "user_historical_avg_spend": Decimal("1200.00"),
                "is_new_merchant_for_user": True,
                "is_travel_itinerary_active": False,
            },
        ).as_variables()
        for name, variable in VARIABLES.items():
            value = rules._lookup(full, name)
            assert value is not rules._MISSING, f"{name} is not provided to rules"
            assert rules._literal_type(value) == variable.type, f"{name}: runtime value is not {variable.type}"
