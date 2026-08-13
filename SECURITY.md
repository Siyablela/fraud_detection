# Security Architecture

This application uses a split security model:

- Synchronous API traffic is protected with OAuth 2.0 / JWT access tokens.
- Asynchronous Kafka traffic is protected with SASL/OAUTHBEARER and Kafka ACLs.

That separation is intentional. Low-latency financial systems should not reuse a single security mechanism for both request/response APIs and high-throughput streaming pipelines.
JWT gives fine-grained caller identity and scope enforcement for REST/gRPC calls. SASL/OAUTHBEARER plus ACLs gives strong service identity and broker-level authorization for Kafka producers and consumers without unnecessary client certificate complexity.

## Synchronous API security

The FastAPI services validate JWT access tokens using either:

- a JWKS endpoint, or
- a trusted public key file

The application does not use shared symmetric secrets for token verification.

Required scope for transaction lookup:

- `fraud:transactions:read`

Expected behavior:

- Missing or invalid token -> `401 Unauthorized`
- Valid token without required scope -> `403 Forbidden`

## Keycloak integration guidance

For this service, the standard pattern is:

- Human users or developer tooling should authenticate through Keycloak using a public client with PKCE.
- Backend microservices should authenticate with a confidential client using the client-credentials grant.
- FastAPI validates the resulting bearer token locally by checking the JWT signature against Keycloak JWKS and by enforcing the required scope.

That keeps the API stateless while aligning the auth flow with the two audiences this service serves: human users and service-to-service callers.

## Kafka stream security

Kafka clients use SASL/OAUTHBEARER with service-account credentials and do not require client certificates or mTLS.

Python client configuration snippet:

```python
from confluent_kafka import Consumer, Producer

common_security = {
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanisms": "OAUTHBEARER",
    "sasl.oauthbearer.client.id": "fraud-service-cli",
    "sasl.oauthbearer.client.secret": "secret",
    "oauth_cb": oauth_callback,
}

producer = Producer({"bootstrap.servers": "kafka:9092", **common_security})
consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "fraud-worker-group",
    **common_security,
})
```

This app intentionally avoids certificate-based Kafka authentication to keep the runtime lean and operationally simple.

Kafka ACL example:

```bash
kafka-acls --bootstrap-server kafka:9092 \
  --command-config client.properties \
  --add \
  --allow-principal User:fraud-service-cli \
  --operation READ \
  --topic transactions_topic \
  --group fraud-worker-group
```

`client.properties` should contain the broker bootstrap and SASL/OAUTHBEARER client settings for the admin client that runs `kafka-acls`.

## Local development

For Compose-based local development:

- Store non-committed runtime values in [`.env`](.env)
- Keep [`.env.example`](.env.example) as the source-of-truth template
- Do not commit real private keys, public keys for non-test environments, or broker credentials

## Production notes

- Validate JWT signatures with JWKS or a public key from the identity provider.
- Keep scopes narrow and route-specific.
- Use SASL/OAUTHBEARER for Kafka producer/consumer identity.
- Use ACLs to bind service principals to specific topics and consumer groups.
- Rotate service credentials and OAuth client secrets on a defined schedule.