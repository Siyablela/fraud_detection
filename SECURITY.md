# Security Architecture

This application uses a split security model:

- Synchronous API traffic is protected with OAuth 2.0 / JWT access tokens.
- Asynchronous Kafka traffic is protected with mTLS and Kafka ACLs.

That separation is intentional. Low-latency financial systems should not reuse a single security mechanism for both request/response APIs and high-throughput streaming pipelines.
JWT gives fine-grained caller identity and scope enforcement for REST/gRPC calls. mTLS plus ACLs gives strong service identity and broker-level authorization for Kafka producers and consumers.

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

Kafka clients use TLS client authentication instead of SASL.

Python client configuration snippet:

```python
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

common_security = {
    "security_protocol": "SSL",
    "ssl_context": ssl_context,
}

producer = AIOKafkaProducer(
    bootstrap_servers="kafka:9093",
    **common_security,
)

consumer = AIOKafkaConsumer(
    "transactions.raw",
    bootstrap_servers="kafka:9093",
    group_id="fraud-worker-group",
    **common_security,
)
```

Environment-driven TLS file inputs:

- `KAFKA_SSL_TRUSTSTORE_PATH`
- `KAFKA_SSL_KEYSTORE_CERT_PATH`
- `KAFKA_SSL_KEYSTORE_KEY_PATH`
- `KAFKA_SSL_KEYSTORE_PASSWORD`

In Python, these map to the CA bundle, client certificate, client private key, and key passphrase used to build the SSL context.

Kafka ACL example:

```bash
kafka-acls --bootstrap-server kafka:9093 \
  --command-config client-ssl.properties \
  --add \
  --allow-principal User:fraud-real-time-consumer \
  --operation READ \
  --topic transactions.raw \
  --group fraud-worker-group
```

`client-ssl.properties` should contain the TLS bootstrap settings for the admin client that runs `kafka-acls`.

## Local development

For Compose-based local development:

- Store non-committed runtime values in [`.env`](.env)
- Keep [`.env.example`](.env.example) as the source-of-truth template
- Do not commit real private keys, public keys for non-test environments, or broker credentials

## Production notes

- Validate JWT signatures with JWKS or a public key from the identity provider.
- Keep scopes narrow and route-specific.
- Use mTLS for Kafka producer/consumer identity.
- Use ACLs to bind service principals to specific topics and consumer groups.
- Rotate keys and certificates on a defined schedule.