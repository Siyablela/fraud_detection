# Security model

How the fraud-detection service authenticates callers, authorises them, protects
data and handles secrets. Referenced from the [README](README.md).

## Authentication

The API is an OAuth 2.0 resource server: it never issues tokens, it verifies
Keycloak-issued JWTs on every protected request ([app/security.py](app/security.py)).

- **Signature** with keys from the issuer's JWKS endpoint (`JWT_JWKS_URL`) or a pinned
  PEM key (`JWT_PUBLIC_KEY_PATH`). Keys are cached; a token naming an unknown key id
  (rotation) triggers a refresh **at most once per
  `JWT_JWKS_MIN_REFRESH_INTERVAL_SECONDS`** (60 s), so forged `kid`s cannot turn every
  request into a call to the identity provider.
- **Algorithms**: only asymmetric ones (`RS*`, `PS*`, `ES*`) are accepted, and the
  service refuses to start with anything else configured. This blocks `none` and the
  HS256-with-the-public-key confusion attack (tested).
- **Claims**: `exp`, `iat`, `sub`, `iss`, `aud` are required; issuer and audience must
  match `JWT_ISSUER` / `JWT_AUDIENCE`; 30 s clock leeway.
- Verification runs off the event loop, so a slow JWKS fetch cannot stall other requests.

| Failure | Status | `WWW-Authenticate` |
|---|---|---|
| Missing or non-Bearer token | 401 | `Bearer realm="fraud-detection"` |
| Expired, bad signature or claims, unknown key | 401 | `Bearer realm="fraud-detection", error="invalid_token"` |
| Missing the route's scope | 403 | `Bearer error="insufficient_scope", scope="..."` |

## Authorisation

Callers are back-office services and fraud analysts, not the cardholders whose
transactions are stored, so access is granted per capability (scope):

| Scope | Grants |
|---|---|
| `fraud:transactions:read` | Read decisions |
| `fraud:rules:read` | Read rule-set versions; dry-run evaluation |
| `fraud:rules:write` | Publish rule-set versions (recorded with the caller's `sub` as `created_by`) |

Scope names are configurable (`SCOPE_*`), and each defaults to a distinct value, so
read access can never imply write access. The local realm defines `fraud-analyst` (read scopes) and
`fraud-rules-admin` (all three), both client-credentials only (no passwords, no
browser flows).

## Abuse protection

- Per-caller rate limit (`RATE_LIMIT_REQUESTS` per `RATE_LIMIT_WINDOW_SECONDS`) keyed
  on the **verified** `sub`, stored in Redis so it holds across replicas and cannot be
  dodged with self-made tokens. If Redis is down it fails open (counted in
  `fraud_rate_limiter_errors_total`, alerted) rather than taking the API down.
- In Kubernetes, Kong adds a per-IP limit and a 64 KB request-size limit in front.
- Rule sets are untrusted input to an interpreter: operators are allow-listed, depth
  and size are bounded, variables are checked, and the set is dry-run before storing.
- Events above 64 KB are rejected before parsing.

## Data protection

- No PII in logs: user ids and amounts are not logged; sensitive keys
  (`LOGS_REDACT_FIELDS`) are masked at any depth. Metrics label routes by template,
  never raw paths.
- Errors return a stable code, a message and a correlation id, never stack traces or
  exception text (tested).
- The DLQ keeps raw payloads for replay; restrict its ACLs to operators.
- All queries are built with SQLAlchemy expressions and bound parameters; there is
  no string-built SQL.

## Secrets

- No credential in source and no default for any secret: missing configuration stops
  the process at startup, naming the variable but not printing values.
- Values come from the environment or a mounted file (`<NAME>_FILE`).
- Kubernetes: everything comes from one existing Secret (see [infra/README.md](infra/README.md)), populated by your secret store.
- `.env.example` holds clearly labelled local-only values; `.env` is git-ignored.

## Transport

Local Compose binds every port to 127.0.0.1 and uses plaintext inside the Docker
network. Elsewhere: TLS at the ingress (cert-manager), `KAFKA_SECURITY_PROTOCOL=SSL`
or `SASL_SSL` with OAuth client credentials (`KAFKA_OAUTH_*`) and `KAFKA_SSL_*`
certificates, and TLS URLs for Redis (`rediss://`) and PostgreSQL (`?ssl=require`; asyncpg
does not read libpq's `sslmode`).

## Runtime hardening

Containers run as UID 10001 with a read-only root filesystem, no Linux
capabilities, no privilege escalation and the RuntimeDefault seccomp profile; service
account tokens are not mounted. Network policies admit traffic to the API only from
the gateway and Prometheus, and to the data stores only from the application.
Dependencies are pinned with hashes (`requirements.lock`); CI runs `pip-audit` and a
Trivy image scan and fails on fixable HIGH/CRITICAL findings.

## Reporting a vulnerability

Please report privately to the maintainers rather than opening a public issue.
