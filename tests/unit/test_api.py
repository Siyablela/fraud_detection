from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import fakeredis
import pytest
from fastapi.testclient import TestClient

from app import api
from app.database import PageCursor
from app.errors import RuleSetNotFoundError, RuleSetVersionConflictError
from app.rate_limit import RateLimiter
from app.rules import DEFAULT_RULE_SET_DEFINITION
from tests.conftest import default_rule_set

READ = "fraud:transactions:read"
RULES_READ = "fraud:rules:read"
RULES_WRITE = "fraud:rules:write"


def stored_transaction(**overrides):
    values = dict(
        transaction_id=uuid4(),
        user_id=uuid4(),
        amount=Decimal("15000.00"),
        category="CRYPTO",
        occurred_at=datetime(2026, 10, 29, 12, tzinfo=UTC),
        is_fraud=True,
        triggered_rules=["HIGH_VALUE_THRESHOLD", "RISKY_CATEGORY_LIMIT"],
        rules_version="fraud-default@v1",
        rules_hash="a" * 64,
        correlation_id="corr-1",
        created_at=datetime(2026, 10, 29, 12, 0, 1, tzinfo=UTC),
    )
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.fixture
def client(monkeypatch):
    async def ok(*_args, **_kwargs):
        return None

    monkeypatch.setattr(api, "ping_database", ok)
    monkeypatch.setattr(api, "ping_redis", ok)
    with TestClient(api.create_app(), raise_server_exceptions=False) as test_client:
        test_client.app.state.redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        yield test_client


@pytest.fixture
def auth(make_token):
    def headers(scopes=READ, **claims):
        return {"Authorization": f"Bearer {make_token(scopes=scopes, **claims)}"}

    return headers


def assert_error(response, status_code, code):
    assert response.status_code == status_code, response.text
    body = response.json()
    assert set(body) == {"error"}
    assert body["error"]["code"] == code
    assert body["error"]["correlation_id"] == response.headers["x-correlation-id"]
    return body["error"]


class TestTransactions:
    def test_get_returns_decision_with_exact_decimal_amount(self, client, auth, monkeypatch):
        record = stored_transaction()

        async def fake_get(_sf, transaction_id):
            return record if transaction_id == record.transaction_id else None

        monkeypatch.setattr(api, "get_transaction", fake_get)
        response = client.get(f"/api/v1/transactions/{record.transaction_id}", headers=auth())
        assert response.status_code == 200
        body = response.json()
        assert body["amount"] == "15000.00"
        assert body["triggered_rules"] == ["HIGH_VALUE_THRESHOLD", "RISKY_CATEGORY_LIMIT"]
        assert body["rules_version"] == "fraud-default@v1"

    def test_unknown_transaction_is_404(self, client, auth, monkeypatch):
        async def fake_get(_sf, _id):
            return None

        monkeypatch.setattr(api, "get_transaction", fake_get)
        assert_error(client.get(f"/api/v1/transactions/{uuid4()}", headers=auth()), 404, "not_found")

    def test_non_uuid_id_is_a_field_level_validation_error(self, client, auth):
        error = assert_error(client.get("/api/v1/transactions/tx-1", headers=auth()), 422, "validation_error")
        assert error["details"][0]["field"] == "path.transaction_id"

    def test_list_passes_filters_and_returns_next_cursor(self, client, auth, monkeypatch):
        captured = {}
        rows = [stored_transaction(), stored_transaction()]

        async def fake_list(_sf, filters, limit, cursor):
            captured.update(filters=filters, limit=limit, cursor=cursor)
            return rows, PageCursor(rows[-1].occurred_at, rows[-1].transaction_id)

        monkeypatch.setattr(api, "list_transactions", fake_list)
        user_id = uuid4()
        response = client.get(
            "/api/v1/transactions",
            params={"user_id": str(user_id), "category": "crypto", "is_fraud": "true", "limit": 2},
            headers=auth(),
        )
        assert response.status_code == 200
        body = response.json()
        assert len(body["data"]) == 2 and body["limit"] == 2
        assert PageCursor.decode(body["next_cursor"]).transaction_id == rows[-1].transaction_id
        assert captured["filters"].user_id == user_id
        assert captured["filters"].category == "CRYPTO"
        assert captured["filters"].is_fraud is True

        cursor = body["next_cursor"]
        client.get("/api/v1/transactions", params={"cursor": cursor}, headers=auth())
        assert captured["cursor"].transaction_id == rows[-1].transaction_id

    @pytest.mark.parametrize(
        ("params", "status_code", "code"),
        [
            ({"limit": 0}, 422, "validation_error"),
            ({"limit": 101}, 422, "validation_error"),
            ({"occurred_from": "2026-10-29T00:00:00"}, 422, "validation_error"),  # naive datetime
            ({"cursor": "not-a-cursor"}, 400, "bad_request"),
        ],
    )
    def test_list_rejects_bad_query_parameters(self, client, auth, params, status_code, code):
        assert_error(client.get("/api/v1/transactions", params=params, headers=auth()), status_code, code)


class TestAuthorisation:
    def test_missing_token_is_401_with_challenge(self, client):
        response = client.get(f"/api/v1/transactions/{uuid4()}")
        assert_error(response, 401, "unauthorized")
        assert response.headers["www-authenticate"].startswith("Bearer")

    def test_wrong_scope_is_403(self, client, auth):
        response = client.get(f"/api/v1/transactions/{uuid4()}", headers=auth(scopes=RULES_READ))
        assert_error(response, 403, "forbidden")
        assert 'scope="fraud:transactions:read"' in response.headers["www-authenticate"]

    def test_reader_cannot_publish_rules(self, client, auth):
        response = client.post(
            "/api/v1/rule-sets/fraud-default/versions",
            json={"description": "x", "definition": DEFAULT_RULE_SET_DEFINITION},
            headers=auth(scopes=f"{READ} {RULES_READ}"),
        )
        assert_error(response, 403, "forbidden")

    def test_rate_limit_returns_429_with_retry_after(self, client, auth, monkeypatch):
        async def fake_get(_sf, _id):
            return None

        monkeypatch.setattr(api, "get_transaction", fake_get)
        client.app.state.rate_limiter = RateLimiter(fakeredis.FakeAsyncRedis(), max_requests=2, window_seconds=60)
        for _ in range(2):
            client.get(f"/api/v1/transactions/{uuid4()}", headers=auth())
        response = client.get(f"/api/v1/transactions/{uuid4()}", headers=auth())
        assert_error(response, 429, "rate_limited")
        assert int(response.headers["retry-after"]) >= 1


class TestRuleSets:
    def test_create_version_returns_201_with_location_and_records_author(self, client, auth, monkeypatch):
        captured = {}

        async def fake_create(_sf, **kwargs):
            captured.update(kwargs)
            return default_rule_set(version=2)

        monkeypatch.setattr(api, "create_rule_set_version", fake_create)
        response = client.post(
            "/api/v1/rule-sets/fraud-default/versions",
            json={"description": "tighten", "definition": DEFAULT_RULE_SET_DEFINITION, "expected_current_version": 1},
            headers=auth(scopes=RULES_WRITE, sub="rules-admin"),
        )
        assert response.status_code == 201, response.text
        assert response.headers["location"] == "/api/v1/rule-sets/fraud-default/versions/2"
        assert response.json()["label"] == "fraud-default@v2"
        assert captured["created_by"] == "rules-admin"
        assert captured["expected_current_version"] == 1

    def test_invalid_definition_is_422_with_details(self, client, auth):
        response = client.post(
            "/api/v1/rule-sets/fraud-default/versions",
            json={
                "description": "bad",
                "definition": {
                    "velocity_window_seconds": 60,
                    "rules": [{"code": "BAD_RULE", "description": "x", "condition": {"log": [1]}}],
                },
            },
            headers=auth(scopes=RULES_WRITE),
        )
        error = assert_error(response, 422, "validation_error")
        assert "unsupported operator 'log'" in error["details"][0]["message"]

    def test_version_conflict_is_409(self, client, auth, monkeypatch):
        async def fake_create(_sf, **_kwargs):
            raise RuleSetVersionConflictError("Current version of 'fraud-default' is 3, expected 1")

        monkeypatch.setattr(api, "create_rule_set_version", fake_create)
        response = client.post(
            "/api/v1/rule-sets/fraud-default/versions",
            json={"description": "x", "definition": DEFAULT_RULE_SET_DEFINITION, "expected_current_version": 1},
            headers=auth(scopes=RULES_WRITE),
        )
        assert_error(response, 409, "conflict")

    def test_list_versions_and_unknown_key(self, client, auth, monkeypatch):
        async def fake_list(_sf, key, limit, before_version):
            return [default_rule_set(2), default_rule_set(1)] if key == "fraud-default" else []

        monkeypatch.setattr(api, "list_rule_set_versions", fake_list)
        response = client.get("/api/v1/rule-sets/fraud-default/versions", headers=auth(scopes=RULES_READ))
        assert [item["version"] for item in response.json()["data"]] == [2, 1]
        assert_error(
            client.get("/api/v1/rule-sets/nope-key/versions", headers=auth(scopes=RULES_READ)), 404, "not_found"
        )

    def test_invalid_rule_set_key_is_422(self, client, auth):
        assert_error(
            client.get("/api/v1/rule-sets/BAD_KEY/active", headers=auth(scopes=RULES_READ)), 422, "validation_error"
        )

    def test_missing_version_is_404(self, client, auth, monkeypatch):
        async def fake_get(_sf, key, version):
            raise RuleSetNotFoundError(f"Rule set '{key}' has no version {version}")

        monkeypatch.setattr(api, "get_rule_set_version", fake_get)
        assert_error(
            client.get("/api/v1/rule-sets/fraud-default/versions/9", headers=auth(scopes=RULES_READ)), 404, "not_found"
        )

    def test_evaluate_dry_run_reports_rules_and_velocity(self, client, auth, monkeypatch):
        async def fake_latest(_sf, key):
            return default_rule_set()

        async def fake_velocity(_sf, **_kwargs):
            return 5, Decimal("100.00")

        monkeypatch.setattr(api, "get_latest_rule_set", fake_latest)
        monkeypatch.setattr("app.processing.get_velocity_aggregates", fake_velocity)
        response = client.post(
            "/api/v1/rule-sets/fraud-default/evaluate",
            json={"user_id": str(uuid4()), "amount": "12000.00", "category": "gambling"},
            headers=auth(scopes=RULES_READ),
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["triggered_rules"] == ["HIGH_VALUE_THRESHOLD", "RISKY_CATEGORY_LIMIT", "VELOCITY_COUNT_EXCEEDED"]
        assert body["velocity"] == {"window_seconds": 60, "prior_count": 5, "prior_amount": "100.00"}


class TestOperationalBehaviour:
    def test_ready_reports_each_dependency(self, client, monkeypatch):
        assert client.get("/ready").json() == {"status": "ready", "checks": {"database": "ok", "redis": "ok"}}

        async def down(*_args):
            raise ConnectionError("down")

        monkeypatch.setattr(api, "ping_redis", down)
        error = assert_error(client.get("/ready"), 503, "service_unavailable")
        assert "redis" in error["message"]

    def test_live_needs_no_dependencies(self, client):
        assert client.get("/live").json() == {"status": "ok"}

    def test_unhandled_error_is_500_without_internals(self, client, auth, monkeypatch):
        async def explode(_sf, _id):
            raise RuntimeError("secret internal detail at /srv/app.py line 12")

        monkeypatch.setattr(api, "get_transaction", explode)
        response = client.get(f"/api/v1/transactions/{uuid4()}", headers=auth())
        error = assert_error(response, 500, "internal_error")
        assert "secret" not in response.text and "Traceback" not in response.text
        assert error["message"] == "An internal error occurred."

    def test_dependency_outage_is_503_with_retry_after(self, client, auth, monkeypatch):
        async def db_down(_sf, _id):
            raise ConnectionRefusedError("connection refused")

        monkeypatch.setattr(api, "get_transaction", db_down)
        response = client.get(f"/api/v1/transactions/{uuid4()}", headers=auth())
        assert_error(response, 503, "service_unavailable")
        assert response.headers["retry-after"] == "5"

    def test_caller_correlation_id_is_echoed_and_unsafe_ones_replaced(self, client):
        assert client.get("/live", headers={"X-Correlation-ID": "trace-123"}).headers["x-correlation-id"] == "trace-123"
        replaced = client.get("/live", headers={"X-Correlation-ID": "bad id\r\n"}).headers["x-correlation-id"]
        assert replaced != "bad id" and len(replaced) == 36

    def test_metrics_label_routes_by_template_not_raw_path(self, client, auth, monkeypatch):
        async def fake_get(_sf, _id):
            return None

        monkeypatch.setattr(api, "get_transaction", fake_get)
        transaction_id = uuid4()
        client.get(f"/api/v1/transactions/{transaction_id}", headers=auth())
        client.get("/definitely/not/a/route")
        metrics = client.get("/metrics").text
        assert 'route="/api/v1/transactions/{transaction_id}"' in metrics
        assert 'route="<unmatched>"' in metrics
        assert str(transaction_id) not in metrics

    def test_openapi_documents_error_schema(self, client):
        schema = client.get("/openapi.json").json()
        assert "ErrorResponse" in schema["components"]["schemas"]
        assert "/api/v1/transactions/{transaction_id}" in schema["paths"]
