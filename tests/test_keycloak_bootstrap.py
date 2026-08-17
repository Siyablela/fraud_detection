from app.keycloak_bootstrap import build_admin_token_url, build_client_payload, build_user_payload


def test_public_client_payload_enables_oauth_flows_and_audience_mapper() -> None:
    payload = build_client_payload(
        client_id="fraud-cli",
        public_client=True,
        service_account=False,
        client_secret="",
        audience="fraud-api",
    )

    assert payload["publicClient"] is True
    assert payload["standardFlowEnabled"] is True
    assert payload["directAccessGrantsEnabled"] is True
    assert payload["attributes"]["pkce.code.challenge.method"] == "S256"
    assert payload["protocolMappers"][0]["config"]["included.client.audience"] == "fraud-api"


def test_service_client_payload_enables_service_account_and_secret() -> None:
    payload = build_client_payload(
        client_id="fraud-service-cli",
        public_client=False,
        service_account=True,
        client_secret="service-secret",
        audience="fraud-api",
    )

    assert payload["secret"] == "service-secret"
    assert payload["serviceAccountsEnabled"] is True
    assert payload["standardFlowEnabled"] is False
    assert payload["protocolMappers"][0]["config"]["included.client.audience"] == "fraud-api"


def test_user_payload_sets_enabled_and_credentials() -> None:
    payload = build_user_payload(
        username="demo",
        password="demo",
        email="demo@example.com",
    )

    assert payload["username"] == "demo"
    assert payload["enabled"] is True
    assert payload["email"] == "demo@example.com"
    assert payload["credentials"][0]["value"] == "demo"


def test_build_admin_token_url_targets_master_realm() -> None:
    assert (
        build_admin_token_url("http://keycloak:8080")
        == "http://keycloak:8080/realms/master/protocol/openid-connect/token"
    )


def test_build_admin_token_url_strips_trailing_slash() -> None:
    assert (
        build_admin_token_url("http://keycloak:8080/")
        == "http://keycloak:8080/realms/master/protocol/openid-connect/token"
    )
