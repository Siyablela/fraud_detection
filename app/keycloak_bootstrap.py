from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import httpx

from app.observability import get_logger
from app.settings import get_settings

logger = get_logger(__name__)


def build_client_payload(
    *,
    client_id: str,
    public_client: bool,
    service_account: bool,
    client_secret: str,
    audience: str,
) -> dict[str, Any]:
    return {
        "clientId": client_id,
        "name": client_id,
        "enabled": True,
        "publicClient": public_client,
        "redirectUris": ["http://localhost/*", "http://127.0.0.1/*"],
        "webOrigins": ["+"],
        "standardFlowEnabled": public_client,
        "implicitFlowEnabled": False,
        "directAccessGrantsEnabled": public_client,
        "serviceAccountsEnabled": service_account,
        "authorizationServicesEnabled": False,
        "secret": client_secret if client_secret else "",
        "protocol": "openid-connect",
        "attributes": {
            "pkce.code.challenge.method": "S256",
            "access.token.lifespan": "300",
        },
        "protocolMappers": [
            {
                "name": "audience-mapper",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "consent": "false",
                "config": {
                    "included.client.audience": audience,
                    "access.token.claim": "true",
                    "id.token.claim": "true",
                },
            }
        ],
    }


def build_user_payload(*, username: str, password: str, email: str) -> dict[str, Any]:
    return {
        "username": username,
        "enabled": True,
        "email": email,
        "emailVerified": True,
        "firstName": username.capitalize(),
        "lastName": "User",
        "credentials": [
            {
                "type": "password",
                "value": password,
                "temporary": False,
            }
        ],
    }


async def bootstrap_keycloak() -> None:
    settings = get_settings()
    issuer = settings.jwt_issuer.rstrip("/")
    realm_name = issuer.rsplit("/realms/", 1)[-1] if "/realms/" in issuer else "fraud"
    admin_username = os.getenv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
    admin_password = os.getenv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
    keycloak_base_url = os.getenv("KEYCLOAK_ADMIN_URL", "").strip()
    if not keycloak_base_url:
        keycloak_base_url = issuer.rsplit("/realms/", 1)[0]
    token_url = f"{keycloak_base_url.rstrip('/')}/protocol/openid-connect/token"

    auth_payload = {
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": admin_username,
        "password": admin_password,
    }

    last_error: Exception | None = None
    for attempt in range(1, 31):
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                token_response = await client.post(token_url, data=auth_payload)
                token_response.raise_for_status()
                token_payload = token_response.json()
                access_token = token_payload.get("access_token")
                if not access_token:
                    raise RuntimeError("Keycloak admin access token could not be obtained")

                headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
                realm_url = f"{keycloak_base_url}/admin/realms"
                realm_exists_response = await client.get(realm_url, headers=headers)
                realm_exists_response.raise_for_status()
                existing_realms = realm_exists_response.json()
                realm_exists = any(item.get("realm") == realm_name for item in existing_realms)

                if not realm_exists:
                    await client.post(
                        realm_url,
                        headers=headers,
                        content=json.dumps({"realm": realm_name, "enabled": True}),
                    )

                realm_admin_url = f"{realm_url}/{realm_name}"
                realm_admin_headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
                realm_response = await client.get(realm_admin_url, headers=realm_admin_headers)
                realm_response.raise_for_status()

                clients_url = f"{realm_admin_url}/clients"
                existing_clients_response = await client.get(clients_url, headers=realm_admin_headers)
                existing_clients_response.raise_for_status()
                existing_clients = existing_clients_response.json()

                public_client = build_client_payload(
                    client_id=settings.keycloak_token_client_id,
                    public_client=True,
                    service_account=False,
                    client_secret="",
                    audience=settings.jwt_audience,
                )
                service_client = build_client_payload(
                    client_id=settings.keycloak_service_client_id,
                    public_client=False,
                    service_account=True,
                    client_secret=settings.keycloak_service_client_secret or "service-secret",
                    audience=settings.jwt_audience,
                )

                if not any(item.get("clientId") == settings.keycloak_token_client_id for item in existing_clients):
                    await client.post(clients_url, headers=realm_admin_headers, content=json.dumps(public_client))
                if not any(item.get("clientId") == settings.keycloak_service_client_id for item in existing_clients):
                    await client.post(clients_url, headers=realm_admin_headers, content=json.dumps(service_client))

                users_url = f"{realm_admin_url}/users"
                existing_users_response = await client.get(users_url, headers=realm_admin_headers)
                existing_users_response.raise_for_status()
                existing_users = existing_users_response.json()
                demo_username = os.getenv("KEYCLOAK_DEMO_USERNAME", "demo")
                demo_password = os.getenv("KEYCLOAK_DEMO_PASSWORD", "demo")
                if not any(item.get("username") == demo_username for item in existing_users):
                    await client.post(
                        users_url,
                        headers=realm_admin_headers,
                        content=json.dumps(
                            build_user_payload(
                                username=demo_username,
                                password=demo_password,
                                email=f"{demo_username}@example.com",
                            )
                        ),
                    )
        except Exception as exc:  # pragma: no cover - exercised during startup retries
            last_error = exc
            if attempt == 30:
                raise
            await asyncio.sleep(2)
            continue

        logger.info("keycloak_bootstrap_complete", realm=realm_name)
        return

    if last_error is not None:
        raise last_error


async def main() -> None:
    await bootstrap_keycloak()


if __name__ == "__main__":
    asyncio.run(main())
