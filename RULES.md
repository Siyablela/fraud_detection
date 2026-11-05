# Fraud rules

How transactions are scored, how to write and publish rules, and how the rules engine
works inside. For fraud analysts who change rules, developers who change the engine,
and anyone reviewing the design.

Every request, response and error message below was produced by the running service.

## Contents

- [How scoring works](#how-scoring-works)
- [The default rule set](#the-default-rule-set)
- [Writing rules](#writing-rules): conditions, variables, operators, values, examples, common mistakes
- [Publishing and managing rule sets](#publishing-and-managing-rule-sets): access, dry runs, publishing, rollback, audit
- [How the engine works](#how-the-engine-works): for developers
- [Extending the engine](#extending-the-engine)
- [Design decisions and trade-offs](#design-decisions-and-trade-offs)
- [Known limitations](#known-limitations)

---

## How scoring works

A **rule** is a condition plus a score. A **rule set** is a list of rules plus two
thresholds. For every transaction:

1. **Every** rule's condition is evaluated against the transaction.
2. The scores of the rules whose condition is true are added up: the **risk score**.
3. The risk score is compared with the thresholds to give the **verdict**.

```
 transaction (R14,500.00, CRYPTO, ZAR, no recent activity)
      │
      ├─ HIGH_VALUE_THRESHOLD      amount ≥ 10,000                      ✓ +40
      ├─ RISKY_CATEGORY_LIMIT      GAMBLING/CRYPTO and amount > 5,000   ✓ +50
      ├─ VELOCITY_COUNT_EXCEEDED   > 5 transactions in 60 s             ✗
      ├─ VELOCITY_AMOUNT           > R50,000 in 60 s                    ✗
      └─ FOREIGN_CURRENCY          currency is not ZAR                  ✗
                                                           risk score = 90
      verdict:   0 ──── 29 │ 30 ──── 74 │ 75 ────
                 APPROVED  │  REVIEW    │ REJECTED   ← 90
```

| Verdict | Risk score (default thresholds) | `is_fraud` | Meaning |
|---|---|---|---|
| `APPROVED` | below `review_threshold` (0–29) | `false` | No action |
| `REVIEW` | from `review_threshold` up to `reject_threshold` (30–74) | `true` | One strong signal: queue for an analyst |
| `REJECTED` | `reject_threshold` and above (75+) | `true` | Two or more signals: restrict and investigate |

Scoring happens **after** the payment has been authorised (the service consumes events
from Kafka), so `REJECTED` is a recommendation to act on the account, not a declined
payment.

Every decision records the rules that fired (`triggered_rules`), the rule-set version
that produced it (`rules_version`, e.g. `fraud-default@v1`) and that version's SHA-256
fingerprint (`rules_hash`). Every rule is evaluated even once the score passes the
reject threshold, so a decision lists **all** the reasons it was flagged.

---

## The default rule set

Seeded by the first database migration as version 1 of the key `fraud-default`. The
worker scores with the latest version of the key named by `RULE_SET_KEY` (default
`fraud-default`). Amounts are in ZAR.

| Rule | Fires when | Score |
|---|---|---|
| `HIGH_VALUE_THRESHOLD` | amount ≥ R10,000.00 | 40 |
| `RISKY_CATEGORY_LIMIT` | category is `GAMBLING` or `CRYPTO` **and** amount > R5,000.00 | 50 |
| `VELOCITY_COUNT_EXCEEDED` | more than 5 transactions by the user within 60 s, this one included | 40 |
| `VELOCITY_AMOUNT` | more than R50,000.00 by the user within 60 s, same currency, this one included | 50 |
| `FOREIGN_CURRENCY` | currency is not ZAR, so the ZAR thresholds above don't apply | 30 |

<details>
<summary>The full definition (JSON)</summary>

```json
{
  "velocity_window_seconds": 60,
  "review_threshold": 30,
  "reject_threshold": 75,
  "rules": [
    {"code": "HIGH_VALUE_THRESHOLD", "score": 40,
     "description": "A single transaction of R10,000.00 or more.",
     "condition": {">=": [{"var": "amount"}, 10000]}},
    {"code": "RISKY_CATEGORY_LIMIT", "score": 50,
     "description": "More than R5,000.00 in a high-risk category (GAMBLING, CRYPTO).",
     "condition": {"and": [{"in": [{"var": "category"}, ["GAMBLING", "CRYPTO"]]},
                           {">": [{"var": "amount"}, 5000]}]}},
    {"code": "VELOCITY_COUNT_EXCEEDED", "score": 40,
     "description": "More than 5 transactions by the same user within the velocity window.",
     "condition": {">": [{"+": [{"var": "velocity.prior_count"}, 1]}, 5]}},
    {"code": "VELOCITY_AMOUNT", "score": 50,
     "description": "More than R50,000.00 spent by the same user within the velocity window.",
     "condition": {">": [{"+": [{"var": "velocity.prior_amount"}, {"var": "amount"}]}, 50000]}},
    {"code": "FOREIGN_CURRENCY", "score": 30,
     "description": "Not in ZAR: the amount thresholds above do not apply, so send it for review.",
     "condition": {"!=": [{"var": "currency"}, "ZAR"]}}
  ]
}
```

</details>

The scores and thresholds are starting values. Without labelled fraud outcomes they
can't be calibrated yet; publish new versions as outcomes are learned.

---

## Writing rules

### The rule set

| Field | Rules | Meaning |
|---|---|---|
| `velocity_window_seconds` | 1–86,400 | How far back, in event time, the `velocity.*` variables 
look |
| `review_threshold` | ≥ 1 | Risk score at or above which the verdict is `REVIEW` |
| `reject_threshold` | ≥ `review_threshold` | Risk score at or above which the verdict is `REJECTED` |
| `rules` | 1–50 rules, unique codes | The rules |

Each rule:

| Field | Rules | Meaning |
|---|---|---|
| `code` | `A-Z`, `0-9`, `_`; starts with a letter; 3–64 characters | Stable name, reported in `triggered_rules` and metrics. Keep it unchanged across versions so trends stay comparable |
| `description` | 1–500 characters | What the rule means, in plain words |
| `score` | 0–1,000 | Points added when the condition is true |
| `condition` | a JsonLogic expression | True when the rule should fire |

Unknown fields are refused, so a misspelt field name (`"treshold"`) is an error, not
silently ignored.

### Conditions are expression trees

Conditions are written in [JsonLogic](https://jsonlogic.com): JSON that is directly the
expression's **tree** (its abstract syntax tree). Every operation has the same shape,
one operator whose value is the list of its arguments:

```json
{ "operator": [ argument1, argument2, ... ] }
```

and arguments can be operations themselves. `RISKY_CATEGORY_LIMIT`:

```json
{"and": [
  {"in": [{"var": "category"}, ["GAMBLING", "CRYPTO"]]},
  {">":  [{"var": "amount"}, 5000]}
]}
```

```
level 1                and
                    /       \
level 2           in           >
                /    \       /    \
level 3       var    list   var    5000
            category  / \   amount
level 4        "GAMBLING" "CRYPTO"

9 nodes, 4 levels deep
```

| Kind of node | Example | Meaning |
|---|---|---|
| Operation | `{">": [a, b]}` | Computes something from its arguments |
| Variable | `{"var": "amount"}` | Reads a value from the transaction |
| List | `["GAMBLING", "CRYPTO"]` | A list of values, for `in` |
| Value | `5000`, `"CRYPTO"`, `true` | A fixed value |

A condition may have at most **400 nodes** and be at most **24 levels** deep. Real
rules use about ten nodes; the limits stop a published rule from slowing down or
crashing scoring for every transaction.

### Variables

What a condition can read about a transaction:

| Variable | Type | Always present? | Meaning |
|---|---|---|---|
| `amount` | number | yes | Transaction amount, exactly as sent (2 decimal places) |
| `currency` | text | yes | ISO 4217 code, upper case: `"ZAR"`, `"USD"` |
| `category` | text | yes | Upper case: `"CRYPTO"`, `"GAMBLING"`, `"7995"` |
| `velocity.prior_count` | number | yes | The user's **other** transactions in the window, any currency |
| `velocity.prior_amount` | number | yes | Their total amount, **same currency only** |
| `channel` | text | no | Upper case: `"ONLINE"`, `"POS"` |
| `country` | text | no | ISO 3166 alpha-2, upper case: `"ZA"`, `"GB"` |
| `enrichment.distance_from_last_tx_km` | number | no | From an upstream feature service |
| `enrichment.minutes_since_last_tx` | number | no | ″ |
| `enrichment.user_historical_avg_spend` | number | no | ″ |
| `enrichment.is_new_merchant_for_user` | true/false | no | ″ |
| `enrichment.is_travel_itinerary_active` | true/false | no | ″ |

These are the only variables. Each one except `velocity.*` is a field of the event
contract ([app/events.py](app/events.py)) with the same type and the same
always-present/optional status; `velocity.*` are computed by the worker. Tests check
both that the list matches the event model and that the worker supplies every
variable with its declared type.

**Velocity** is measured in **event time**: the user's other stored transactions with
`occurred_at` in `[occurred_at − velocity_window_seconds, occurred_at]`, both ends
included. "This one included" in the default rules is why they add 1 (or the current
amount): `prior_count + 1 > 5` is true for the 6th transaction in the window. Because the
window is event time, consumer lag or a replay gives the same answer; a transaction
that arrives late counts towards later decisions but doesn't change earlier ones.

**Optional variables must have a default**, written as a second argument:
`{"var": ["channel", "UNKNOWN"]}`. Without one, every event that lacks the field would
fail. Publishing refuses an optional variable without a default, and refuses a
default of a different type from the variable: text for `channel`/`country`
(`"UNKNOWN"`), a number for the numeric enrichment fields (`0`), `true`/`false` for
the boolean ones (`false`).

### Operators

| Operator | Arguments | Meaning | Example |
|---|---|---|---|
| `==` `!=` | 2 | equal / not equal; numbers compare by value (`5 == 5.0`) | `{"==": [{"var": "currency"}, "ZAR"]}` |
| `>` `>=` | 2 | greater (or equal); numbers only | `{">=": [{"var": "amount"}, 10000]}` |
| `<` `<=` | 2 or 3 | less (or equal); **with 3 arguments: between** | `{"<=": [1000, {"var": "amount"}, 2000]}` = 1,000 ≤ amount ≤ 2,000 |
| `and` `or` | 1 or more | all / any true; stop at the first deciding argument | |
| `!` | 1 | not | `{"!": {"var": ["enrichment.is_new_merchant_for_user", false]}}` |
| `if` | 3 | if, then, else | `{"if": [condition, value_if_true, value_if_false]}` |
| `in` | 2 | value in a **list**, or text **inside** text | `{"in": [{"var": "category"}, ["GAMBLING", "CRYPTO"]]}` |
| `+` `*` | 1 or more | add / multiply | `{"+": [{"var": "velocity.prior_count"}, 1]}` |
| `-` | 1 or 2 | subtract, or negate with one argument | |
| `min` `max` | 1 or more | smallest / largest | |
| `var` | 1 or 2 | read a variable; the second argument is its default | `{"var": ["country", "ZA"]}` |

Nothing else is allowed. There is no division, no date arithmetic, and no operator
with side effects. Publishing refuses anything else.

### Values

- **Numbers** are JSON numbers: `5000`, `5000.50`. All arithmetic is exact decimal
  arithmetic, so a transaction of exactly R50,000.00 compares correctly with `50000`.
  A number written as text (`"5000"`) is refused: `'>' works on numbers, but this is text`.
- **Text** comparisons are exact and case-sensitive. Incoming `category`, `currency`,
  `channel` and `country` values are upper-cased before scoring, so **write those
  literals in upper case**.
- **`true` / `false`** are not numbers: `{">": [true, 0]}` is an error, not "1 > 0".

### Types are checked when publishing

Every value in a condition has a type: **a number**, **text**, **true/false**, or a
**list** of one of those. Publishing works out the type of every part of every
condition and refuses a rule whose types don't fit:

| Rule | Example that is refused | Message |
|---|---|---|
| A condition must produce true/false | `{"+": [{"var": "amount"}, 1]}` | `a condition must be true/false, but this one is a number` |
| `>` `>=` `<` `<=` `+` `-` `*` `min` `max` need numbers | `{">": [{"var": "amount"}, "5000"]}` | `'>' works on numbers, but this is text` |
| `and` `or` `!` need true/false | `{"and": [{"var": "amount"}, true]}` | `'and' combines true/false conditions, but this is a number` |
| `if`: the condition must be true/false, and both branches the same type | `{"if": [true, 1, "x"]}` | `both branches of 'if' must have the same type, got a number and text` |
| `==` `!=` compare values of the same type (`null` may be compared with anything) | `{"==": [{"var": "category"}, 7995]}` | `'==' compares text with a number, which can never be equal` |
| `in` looks for a value in a list of the same type, or text inside text | `{"in": [{"var": "amount"}, ["A", "B"]]}` | `'in' looks for a number in a list of text, which can never match` |
| A list holds values of one type | `["CRYPTO", 1]` | `a list must hold values of one type` |
| A default has its variable's type | `{"var": ["enrichment.is_new_merchant_for_user", 0]}` | `the default for 'enrichment.is_new_merchant_for_user' must be true/false (its type), not a number, e.g. {"var": ["enrichment.is_new_merchant_for_user", false]}` |

The check covers the **whole** condition, including parts a particular transaction would
never reach. In `{"and": [{"==": [{"var": "category"}, "CRYPTO"]}, {">": [{"var": "category"}, 1]}]}`
the `>` only runs for crypto transactions, but it is refused at publish all the same
(`condition.and[1].>[0]: '>' works on numbers, but this is text`), rather than failing
later on the first crypto transaction.

The `"7995"` case is the one to remember: merchant category codes are **text**, so a
rule must compare `category` with `"7995"`, not `7995`.

### Examples

Each of these passes publishing and fires exactly as described.

| Rule | Condition | Fires for |
|---|---|---|
| Online gambling | `{"and": [{"==": [{"var": "category"}, "GAMBLING"]}, {"==": [{"var": ["channel", "UNKNOWN"]}, "ONLINE"]}]}` | gambling with `channel` = `ONLINE`; not when the channel isn't sent |
| Transaction abroad | `{"!=": [{"var": ["country", "ZA"]}, "ZA"]}` | any country other than ZA; not when the country isn't sent |
| Spending spike | `{"and": [{">": [{"var": ["enrichment.user_historical_avg_spend", 0]}, 0]}, {">": [{"var": "amount"}, {"*": [{"var": ["enrichment.user_historical_avg_spend", 0]}, 5]}]}]}` | more than 5× the user's average spend (R6,000 against R1,000); not when no average is sent |
| Large first purchase at a merchant | `{"and": [{"==": [{"var": ["enrichment.is_new_merchant_for_user", false]}, true]}, {">=": [{"var": "amount"}, 3000]}]}` | ≥ R3,000 at a merchant new to the user |
| Price band | `{"and": [{"==": [{"var": "category"}, "ELECTRONICS"]}, {"<=": [1000, {"var": "amount"}, 2000]}]}` | electronics from R1,000.00 to R2,000.00 inclusive; not R2,000.01 |
| Burst of small transactions | `{"and": [{"<": [{"var": "amount"}, 50]}, {">": [{"+": [{"var": "velocity.prior_count"}, 1]}, 10]}]}` | the 11th sub-R50 transaction within the window (card testing) |
| Far away, not travelling | `{"if": [{"==": [{"var": ["enrichment.is_travel_itinerary_active", false]}, true]}, false, {">": [{"var": ["enrichment.distance_from_last_tx_km", 0]}, 500]}]}` | more than 500 km from the last transaction, unless a trip is booked |

The guard in "Spending spike" (`average > 0` before comparing) is deliberate: without
it, a missing average defaults to 0, and every positive amount is "more than 5× zero".

### Common mistakes

| Mistake | Example | What happens |
|---|---|---|
| Lower-case text | `{"==": [{"var": "category"}, "crypto"]}` | **Accepted, but never fires**: categories arrive upper-cased |
| Text inside text instead of a list | `{"in": [{"var": "category"}, "CRYPTO_EXCHANGE"]}` | **Substring match**: `CRYPTO` matches, which is probably not intended. Use a list: `["CRYPTO_EXCHANGE"]` |
| Default of the wrong type | `{"var": ["enrichment.is_new_merchant_for_user", 0]}` | Refused, with a default of the right type suggested |
| Number as text | `{">": [{"var": "amount"}, "5000"]}` | Refused: `'>' works on numbers, but this is text` |
| Category code as a number | `{"==": [{"var": "category"}, 7995]}` | Refused: `'==' compares text with a number, which can never be equal` |
| Optional variable without a default | `{"==": [{"var": "channel"}, "ONLINE"]}` | Refused: `'channel' is optional in events, so it needs a default of type text, e.g. {"var": ["channel", "UNKNOWN"]}` |
| Unknown variable | `{"var": "merchant_id"}` | Refused, with the list of allowed variables |
| Thresholds the wrong way round | `review_threshold: 80, reject_threshold: 50` | Refused |
| Two rules with the same code | | Refused |
| A misspelt operator | `{"=>": [...]}` | Refused: `unsupported operator` |

---

## Publishing and managing rule sets

### Access

All rule-set endpoints need a bearer token (OAuth 2.0 client credentials from Keycloak)
with the right scope:

| Action | Scope | Local client |
|---|---|---|
| Read versions, dry-run a transaction | `fraud:rules:read` | `fraud-analyst`, `fraud-rules-admin` |
| Publish a version | `fraud:rules:write` | `fraud-rules-admin` only |

```bash
ADMIN_TOKEN=$(curl -s http://localhost:8080/realms/fraud/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=fraud-rules-admin \
  -d client_secret=local-dev-rules-admin-secret | python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
```

(The secret is the local one from `.env.example`. Tokens last 5 minutes.) A token
without the write scope gets:

```json
{"error": {"code": "forbidden", "message": "Access token does not include the required scope 'fraud:rules:write'.", ...}}
```

### Endpoints

| Method and path | Scope | Purpose |
|---|---|---|
| `GET /api/v1/rule-sets/{key}/active` | read | The version currently used for scoring |
| `GET /api/v1/rule-sets/{key}/versions?limit=20&before_version=` | read | Version history, newest first; page with `next_before_version` |
| `GET /api/v1/rule-sets/{key}/versions/{n}` | read | One version |
| `POST /api/v1/rule-sets/{key}/evaluate` | read | Dry-run a transaction against the active version; stores nothing |
| `POST /api/v1/rule-sets/{key}/versions` | write | Publish a new version; **active immediately** |

`{key}`: lower-case letters, digits and hyphens, 3–64 characters. The worker only
scores with the key in its `RULE_SET_KEY` setting (`fraud-default`); publishing to
another key creates a separate rule set that nothing scores with until the worker is
pointed at it.

### Dry-run a transaction

Score a hypothetical transaction against the **active** version, including the real
velocity of `user_id` from stored transactions. Nothing is stored.

```bash
curl -s -X POST http://localhost:8000/api/v1/rule-sets/fraud-default/evaluate \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"user_id": "8d6f...", "amount": "14500.00", "currency": "ZAR", "category": "CRYPTO"}'
```

```json
{"rule_set": "fraud-default@v1", "risk_score": 90, "verdict": "REJECTED", "is_fraud": true,
 "triggered_rules": ["HIGH_VALUE_THRESHOLD", "RISKY_CATEGORY_LIMIT"],
 "velocity": {"window_seconds": 60, "prior_count": 0, "prior_amount": "0"}}
```

The same user paying `"120.00"` `"USD"` for `"TRAVEL"` returns `"risk_score": 30`,
`"verdict": "REVIEW"`, `"triggered_rules": ["FOREIGN_CURRENCY"]`.

| Request field | Required | Notes |
|---|---|---|
| `user_id` | yes | UUID; whose history the velocity look-back uses |
| `amount` | yes | > 0, at most 2 decimal places (`99.999` is refused) |
| `currency`, `category` | yes | as in the event contract |
| `occurred_at` | no | ISO-8601 with a time zone; defaults to now. Set it to place the dry run inside a past window |
| `channel`, `country`, `enrichment` | no | as in the event contract |
| `transaction_id` | no | excluded from the velocity look-back, to re-score a stored transaction without counting itself |

### Publish a version

```bash
curl -s -X POST http://localhost:8000/api/v1/rule-sets/fraud-default/versions \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"description": "Lower the high-value threshold to R8,000",
       "expected_current_version": 1,
       "definition": { ...the complete rule set... }}'
```

- Send the **complete** definition. A version replaces the whole rule set; there is no
  "change one rule". The easiest way: `GET .../active`, edit its `definition`, publish it.
- `expected_current_version` guards against two people publishing at once: the publish
  only succeeds if the current version is the one you edited. Omit it to append
  unconditionally; use `0` when creating a new key.
- Success: `201 Created`, the new version in the body, and a `Location` header
  (`/api/v1/rule-sets/fraud-default/versions/2`).

Before anything is stored, the version is checked: the field rules above, every
condition's shape (operators, argument counts, variables, defaults, size, depth) and
[types](#types-are-checked-when-publishing), and finally a **dry run** of every rule
against a sample transaction (R100.00 ZAR `GROCERIES`, one prior transaction of
R100.00). Refusals (all real):

| Status | Cause | Response |
|---|---|---|
| 422 | Unsupported operator | `{"field": "rules.5.condition", "message": "Value error, condition: unsupported operator 'log'"}` |
| 422 | Unknown variable | `{"field": "rules.5.condition", "message": "Value error, condition.>[0]: unknown variable 'merchant_id'; allowed: [...]"}` |
| 422 | Optional variable without a default | `{"field": "rules.5.condition", "message": "Value error, condition.==[0]: 'channel' is optional in events, so it needs a default of type text, e.g. {\"var\": [\"channel\", \"UNKNOWN\"]}"}` |
| 422 | Type mistake | `{"field": "rules.5.condition", "message": "Value error, condition.and[1].>[0]: '>' works on numbers, but this is text"}` |
| 422 | Default of the wrong type | `{"field": "rules.5.condition", "message": "Value error, condition.>[0]: the default for 'enrichment.is_new_merchant_for_user' must be true/false (its type), not a number, e.g. {...false]}"}` |
| 422 | Thresholds reversed | `{"field": "", "message": "Value error, reject_threshold must be greater than or equal to review_threshold"}` |
| 422 | Duplicate rule code | `{"field": "rules", "message": "Value error, rule codes must be unique; duplicated: ['HIGH_VALUE_THRESHOLD']"}` |
| 409 | Someone published first | `"message": "Current version of 'fraud-default' is 1, expected 0"` |
| 403 | Token lacks `fraud:rules:write` | see [Access](#access) |

Reading the error location: `rules.5.condition` is the condition of the **sixth** rule
(counting starts at 0); inside the message, `condition.and[1].>[0]` is the first
argument of the `>` that is the second argument of the `and`. An empty `field` means
the problem is in the rule set as a whole.

### When a new version takes effect

| Where | When |
|---|---|
| API (`/active`, `/evaluate`) | Immediately: reads go to PostgreSQL |
| Worker | Within about 5 s (its in-process cache), because publishing also overwrites the shared Redis entry |
| Worker, if that Redis write failed | Within about 65 s (5 s local + 60 s Redis) |

### Roll back

Versions are append-only and never edited or deleted. To roll back, publish the old
definition as a **new** version:

```bash
OLD=$(curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/rule-sets/fraud-default/versions/1)
# POST {"description": "Roll back to v1", "definition": <OLD.definition>, "expected_current_version": 2}
```

History is kept, and the rollback itself is recorded.

### Audit trail

- Each version stores who published it (`created_by`: the token's subject; for
  client credentials, the client's service account), when (`created_at`), the
  description, and the SHA-256 fingerprint of its definition (`content_hash`).
- Each decision stores `rules_version` and `rules_hash`, so any decision can be traced
  to the exact rules that produced it, even after later versions.

### Never edit rule sets in the database

Every time a version is loaded (by the worker or the API), it is re-checked: its stored
JSON must still match the fingerprint recorded when it was published, and it must still
validate and pass the dry run. A row changed directly in PostgreSQL fails that check,
and then:

- the worker **stops scoring**: it holds its messages (nothing is lost or dead-lettered)
  instead of scoring with rules other than the ones the decision would record;
- the API returns `500` naming the version, e.g. `rule set fraud-default@v1 does not
  match its content hash; it was changed after publishing`;
- the `FraudRuleSetRejected` alert fires, and the Grafana tile "Rule set rejected"
  turns red.

To recover, publish a valid version through the API; held messages are then scored
with it. Always change rules through the API.

---

## How the engine works

For developers. The engine is [app/rules.py](app/rules.py); the worker calls it from
[app/processing.py](app/processing.py), and storage and caching are in
[app/database.py](app/database.py) and [app/cache.py](app/cache.py).

### Map of `rules.py`

```
rules.py
├─ Variables and limits      VARIABLES (name → type, always present?), MAX_EXPRESSION_DEPTH/NODES
├─ Operators                 _OPERATORS (13, evaluated eagerly), _LAZY_OPERATORS (and, or, if, var)
├─ validate_expression()     walk 1: checks a condition's SHAPE and TYPES    (publish and load)
├─ _evaluate()               walk 2: computes a condition's VALUE            (every transaction)
├─ RuleDefinition            one rule (pydantic model)
├─ RuleSetDefinition         the rule set; builds `_compiled` once (numbers → Decimal)
├─ parse_rule_set_definition()   publishing: validate + dry run   → InvalidRuleSetError (422)
├─ load_stored_definition()      loading: hash + validate + dry run → CorruptRuleSetError
├─ content_hash(), definition_json_hash()   the fingerprint
├─ EvaluationContext.as_variables()         a transaction as the dict `var` reads
├─ evaluate_rule_set()       every rule → Assessment(triggered_rules, risk_score, verdict)
└─ DEFAULT_RULE_SET_DEFINITION              the default rules (tests; the migration has its own frozen copy)
```

### Two walks over the same tree

| | `validate_expression` | `_evaluate` |
|---|---|---|
| When | Publishing, and every load | Every transaction |
| Looks at values? | No: works out each node's **type** | Yes |
| Checks | known operator, argument count, known variable, optional variables have a default of the right type, every operator gets the types it needs, the condition is true/false, supported literals, finite numbers, ≤ 24 levels, ≤ 400 nodes | nothing: shape and types are already known to be valid |
| Fails with | `InvalidRuleSetError` | `RuleEvaluationError` |

Types are worked out from the bottom up: a literal's type is obvious, a variable's comes
from `VARIABLES`, and each operator checks its arguments' types and reports its own
(`>` gives true/false, `+` gives a number). Because this covers every branch, it
catches mistakes a dry run can't: a dry run only evaluates the branches its sample
transaction takes, and `and`, `or` and `if` skip the rest. The dry run is still run on
publish and load, as a second line of defence.

### Evaluation

`_evaluate` is recursive: to compute a node it first computes its arguments, so
values flow from the leaves up to the root. A real trace of `RISKY_CATEGORY_LIMIT`
for R14,500.00 `CRYPTO` (indentation is depth; each line prints when that node finishes):

```
    var "category"         -> 'CRYPTO'
      'GAMBLING'           -> 'GAMBLING'
      'CRYPTO'             -> 'CRYPTO'
    list                   -> ['GAMBLING', 'CRYPTO']
  in                       -> True
    var "amount"           -> Decimal('14500.00')
    Decimal('5000')        -> Decimal('5000')
  >                        -> True
and                        -> True
```

For R14,500.00 `GROCERIES`, `in` returns `False` and `and` stops: the `>` branch is never
evaluated. That's why `and`, `or`, `if` and `var` are handled in `_evaluate` itself
rather than in `_OPERATORS`, which evaluates every argument first: they must evaluate
only what they need (and `var` must only evaluate its default when the variable is missing).

Two deliberate details:

- **Decimal, never float.** With floats, `0.1 + 0.2 != 0.3`, so an amount exactly at a
  threshold could compare the wrong way. Event amounts are parsed as `Decimal`, and a
  rule set's numeric literals are converted to `Decimal` once when it's built (`_compiled`).
- **Booleans aren't numbers.** Python treats `True` as `1`; `_to_decimal` refuses it.

### The life of a rule set

```
 PUBLISH  POST /versions
   body → parse_rule_set_definition()
            model_validate: field rules + validate_expression() on every condition
            build _compiled (numbers → Decimal)
            dry run: evaluate_rule_set() on a sample transaction
          → content_hash() → INSERT version n+1 (with expected_current_version guard)
          → overwrite the Redis cache entry
   any failure → InvalidRuleSetError → 422, nothing stored

 LOAD  (worker cache miss, or any API read)
   local copy (5 s) → Redis (60 s) → PostgreSQL
   every copy → load_stored_definition():
            stored JSON must hash to the stored content_hash
            model_validate (same checks as publishing) + dry run
   any failure → CorruptRuleSetError → worker stops scoring and retries; API 500

 SCORE  (every transaction)
   cached RuleSet → evaluate_rule_set(definition, context) using _compiled
```

The fingerprint hashes the **stored JSON** (keys sorted, no whitespace), not a
re-serialised model, so adding an optional field to the model later can't make every
existing version fail its check.

### Failure behaviour

| Where it fails | Error | What happens |
|---|---|---|
| Publishing an invalid version | `InvalidRuleSetError` | 422 with field details; nothing stored |
| Loading a version that fails its integrity check | `CorruptRuleSetError` (transient) | Worker holds every message and retries until a valid version is published; API returns 500; `FraudRuleSetRejected` alert |
| No version exists for the key | `RuleSetNotFoundError` (transient) | Worker holds and retries; API returns 404 |
| A rule fails on one live transaction | `RuleEvaluationError` (unexpected) | Not expected for a published rule set, since types are checked statically; if it happens, that message is retried 3 times, then dead-lettered with the reason; `FraudMessagesDeadLettered` alert |

---

## Extending the engine

**Add a variable** (for example, a new event field):

1. Add it to `VARIABLES` in `app/rules.py`, with its type and whether it is always
   present. The type and presence must match the event field; a test fails if they don't.
2. Provide the value in `EvaluationContext` and `as_variables()`; numbers as `Decimal`.
3. Fill it in `score_transaction` (`app/processing.py`), and in the dry-run request
   (`EvaluateRequest` in `app/schemas.py`, `evaluate` in `app/api.py`) if analysts
   should be able to set it.
4. If it comes from the event: add it to `TransactionCreatedV1` (`app/events.py`). An
   optional field needs no new `schema_version`; a required one does.
5. Run `tests/unit/test_rules.py`: `TestVariables` checks the new variable exists on the
   event model with the same type, and that the worker supplies it. Add it to the
   [variables](#variables) table.

Existing versions keep their fingerprints: the hash covers stored JSON, not code.

**Add an operator:**

1. Implement it as a pure function: no I/O, no side effects, numbers through `_numbers`.
2. Register it in `_OPERATORS` with its minimum and maximum argument count. Only use
   `_LAZY_OPERATORS` if it must not evaluate all of its arguments.
3. Give it a type rule in `validate_expression`: which argument types it accepts and
   what type it produces. Publishing would otherwise refuse every use of it.
4. Test it in `tests/unit/test_rules.py` (including wrong types and argument counts),
   and add it to the [operators](#operators) table.

Never add an operator that reads files, calls the network or changes state: conditions
are untrusted input published over the API.

**Change the default rules:** publish a new version through the API. Don't edit the
migration: it is a frozen record of version 1, and a test checks its fingerprint
matches `DEFAULT_RULE_SET_DEFINITION`. Change that constant only in step with a new
migration, if new environments should start from different rules.

---

## Design decisions and trade-offs

**Rules as versioned data, not code.** Changing a threshold is a publish, not a
deploy, and each decision records exactly which version produced it. *Cost:* rules
are a second thing to test and review, which is why publishing validates and dry-runs
them, and why `/evaluate` exists.

**A small interpreter, not `eval` or a library.** Rule sets arrive over the API, so the
engine is an interpreter for untrusted input. An allow-list of 17 operators, bounded size
and depth, and `Decimal` arithmetic come from writing about 200 lines; `eval` would let
a rule run arbitrary code, and general JsonLogic libraries allow more operators, have
no limits and use floats.

**Static type checking at publish time.** Every variable has a declared type that
matches the event model, and every condition is type-checked as a whole before it
can go live. A type mistake is a 422 with its exact location, rather than a
failure on live traffic, including in branches only some transactions reach.

**Additive scores, not first-match.** Every rule contributes, and the total maps to a
verdict, so independent weak signals can add up to a strong one, and a decision lists
every reason. *Cost:* scores need calibration against outcomes; they are starting values.

**Append-only versions with an optimistic-concurrency guard.** Nothing is ever lost,
rollback is a publish, and two analysts can't silently overwrite each other.

**Re-verify on every load, and stop rather than fall back.** A version edited in the
database is refused. The worker stops scoring instead of using an older version,
because the decision would then record rules that didn't produce it. *Cost:* a bad
edit halts scoring rather than degrading it; the alert and the hold make that visible
and lossless.

**Event-time velocity.** Replays and consumer lag give the same answer as live
processing. *Cost:* late events aren't re-scored into earlier decisions.

---

## Known limitations

- **`/evaluate` uses the active version.** A draft can't be dry-run before publishing;
  publish it (it is validated and dry-run first), check it with `/evaluate`, and roll
  back if needed.
- **Text comparisons are case-sensitive**, and a lower-case literal (`"crypto"`) is
  accepted but never matches.
- **No arithmetic on time or division.** Rules can't express "more than 3 transactions
  between midnight and 4 a.m." or ratios except as multiplication (`amount > avg * 5`).
- **Thresholds are in ZAR.** Non-ZAR transactions score `FOREIGN_CURRENCY` instead of
  being converted.
