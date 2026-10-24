# Security model

This documents describes the authentication, authorization, and transport-security model for Fraud Detection service. It is referenced from the root [README](README.md).

## Authentication (JWT / OIDC)

The API is pure **resource server**. It does not issue tokens; it validates Keycloak-issued JWTs on every protected request.

Validation (see [app/security.py](app/security.py)) enforces:

- **Signature** via issuer's JWKS endpoint (`JWT_JWKS_URL`) or a pinned PEM public key (`JWT_PUBLIC_KEY_PATH`). Resolved signing keys are cached and
  refreshed every `JWT_JWKS_REFRESH_INTERVAL_SECONDS` so rotated keys are picked
  up without restart.
- **Algorithm allow-list** from `JWT_ALGORITHMS` (default `RS256`). Symmetric
  algorithms and `none` are never accepted, preventing algorithm-confusion
  downgrade attacks.
- **Required claims:** `exp`, `iss`, `aud`, `sub` must all be present and valid
  (`options={require: [...]}`), with `issuer` and `audience` checked against
  `JWT_ISSUER` / `JWT_AUDIENCE`.

Failures map to precise responses:

| Failure | HTTP | `WWW-Authenticate` |
|---|---|---|
| Missing / non-Bearer token | 401 | `Bearer realm="fraud-detection"` |
| Expired token | 401 | `Bearer error="invalid_token"` |
| Invalid signature / claims | 401 | `Bearer error="invalid_token"` |
| Missing subject | 401 | `Bearer error="invalid_token"` |
| Insufficient scope | 403 | `Bearer error="insufficient_scope"` |

## Authorization (scope model)

Access is scope-gated per route. The protected query endpoints require scope
in `JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ` (default **`fraud:transaction:read`**).
Both the OAuth2 `scope` (space-delimited string) and `scp` (list) claim shapes are
accepted.

| Endpoint | Required scope |
|---|---|
| `GET /api/v1/transactions/{transaction_id}` | `fraud:transaction:read` |
| `GET /api/v1/categories/{category_name}` | `fraud:transaction:read` |
| `GET /health`, `/live`, `/ready`, `/metric` | none (unauthenticated) |

### Keycloak clients

- **Human-facing clients** (web UI, CLI, dashboard): public client with **PKCE**,
  client id `fraud-cli`.
- **Backend microservices**: confidential client using the **client-credentials**
  grant, client id `fraud-service-cli`.

- The local-only token helper endpoints (`KAFKA_SECURITY_PROTOCOL`)
  (`SASL_SSL` for authenticated, encrypted broker traffic).
- Authentication uses **SASL/AUTHBEARER** with Keycloak client-credentials
  token callback (see [app/kafka_security.py](app/kafka_security.py)).
- The producer runs with `acks=all` and idempotent enabled.

### Recommended broker ACLs (least privilege)

| Principal | Topic | Operations |
|---|---|---|
| fraud worker (consumer) | `transactions_topic` | `Read`, `Describe` |
| fraud worker (producer) | `transactions_topic.dlq` | `Write`, `Describe` |
| consumer group `fraud-worker-group` | - | `Read` |

> For production, enable TLS at the broker and configure a CA truststore so the
> SASL/AUTHBEARER credential is exchanged over an encrypted channel.

## CORS and rate limiting

- **CORS** is deny-by-default. Cross-origin browser access is only granted to the
  exact origins listed in `CORS_ALLOWED_ORIGINS` (comma-separated); an empty list
  permits no cross-origin requests.
- **Rate limiting** applied a per-instance **token-bucket** budget - a sustained
  rate of `RATE_LIMIT_REQUESTS` per `RATE_LIMIT_WINDOW_SECONDS` with a burst
  capacity of `RATE_LIMIT_BURST` (defaults to the request budget) - and returns
  `429` with a `Retry-After` header when exceeded. It is keyed by the
  authenticated principal (JWT `sub` falling back to `azp` / `client_id`), not by
  IP.

## Secret management

- No secrets are committed; `.gitignore` excludes `.env`, `.env.*`
- Configuration supports file-backed `*_FILE` convention for
  Kubernetes/Vault-injected secret mounts.
- In Kubernetes, credentials are provided from an external `Secret` via
  `secretKeyRef`; the Helm values reference paths rather than literals.

## Reporting a vulnerability

Report suspected vulnerabilities privately to the maintainer rather than opening
a public issue. Please provide reproduction steps and affected endpoints/versions.
