"""End-to-end check of a running stack (docker compose up) - every README scenario.

    python scripts/smoke_test.py                 # reads secrets and ports from .env
    python scripts/smoke_test.py --env-file other.env

Exits non-zero if any scenario fails. Publishes real events and appends rule-set
versions (it restores the original rules afterwards as a new version).
"""

import argparse
import json
import sys
import time
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
from confluent_kafka import Consumer, Producer


def load_env(path: Path) -> dict[str, str]:
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


class Smoke:
    def __init__(self, env: dict[str, str]):
        self.api = f"http://localhost:{env.get('API_HOST_PORT', '8000')}"
        self.token_url = (
            f"http://localhost:{env.get('KEYCLOAK_HOST_PORT', '8080')}/realms/fraud/protocol/openid-connect/token"
        )
        self.metrics_url = f"http://localhost:{env.get('WORKER_METRICS_HOST_PORT', '9100')}/metrics"
        self.bootstrap = f"localhost:{env.get('KAFKA_HOST_PORT', '29092')}"
        self.topic = env.get("KAFKA_TOPIC_NAME", "transactions")
        self.env = env
        self.http = httpx.Client(timeout=30)  # first Keycloak token on a cold start is slow
        self.producer = Producer({"bootstrap.servers": self.bootstrap, "acks": "all", "enable.idempotence": True})
        self.results: list[tuple[str, bool, str]] = []

    # --- helpers -----------------------------------------------------------------

    def token(self, client_id: str, secret_name: str) -> str:
        response = self.http.post(
            self.token_url,
            data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": self.env[secret_name]},
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def publish(
        self,
        user_id: str,
        amount: str,
        category: str = "GROCERIES",
        occurred_at: datetime | None = None,
        transaction_id: str | None = None,
    ) -> str:
        event = {
            "schema_version": 1,
            "event_type": "TransactionCreated",
            "transaction_id": transaction_id or str(uuid4()),
            "user_id": user_id,
            "amount": amount,
            "category": category,
            "occurred_at": (occurred_at or datetime.now(UTC)).isoformat(),
            "correlation_id": f"smoke-{uuid4()}",
        }
        self.producer.produce(self.topic, key=user_id.encode(), value=json.dumps(event).encode())
        return event["transaction_id"]

    def flush(self) -> None:
        if self.producer.flush(15) != 0:
            raise RuntimeError("Kafka did not acknowledge every event")

    def decision(self, transaction_id: str, timeout: float = 60) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self.http.get(f"{self.api}/api/v1/transactions/{transaction_id}", headers=self.reader)
            if response.status_code == 200:
                return response.json()
            time.sleep(0.5)
        raise AssertionError(f"no decision for {transaction_id} within {timeout}s")

    def check(self, name: str, condition: bool, detail: str = "") -> None:
        self.results.append((name, bool(condition), detail))

    # --- scenarios -----------------------------------------------------------

    def run(self) -> bool:
        ready = self.http.get(f"{self.api}/ready")
        self.check("API readiness", ready.status_code == 200, ready.text)
        self.reader = {"Authorization": f"Bearer {self.token('fraud-analyst', 'FRAUD_ANALYST_CLIENT_SECRET')}"}
        self.admin = {"Authorization": f"Bearer {self.token('fraud-rules-admin', 'FRAUD_RULES_ADMIN_CLIENT_SECRET')}"}

        unauthenticated = self.http.get(f"{self.api}/api/v1/transactions")
        self.check(
            "No token -> 401 with error schema",
            unauthenticated.status_code == 401 and unauthenticated.json()["error"]["code"] == "unauthorized",
        )
        forbidden = self.http.post(
            f"{self.api}/api/v1/rule-sets/fraud-default/versions",
            headers=self.reader,
            json={"description": "x", "definition": {}},
        )
        self.check("Analyst cannot publish rules -> 403", forbidden.status_code == 403)

        now = datetime.now(UTC)
        cases = {
            "ordinary purchase": (self.publish(str(uuid4()), "125.50"), []),
            "high value at 10,000.00": (self.publish(str(uuid4()), "10000.00"), ["HIGH_VALUE_THRESHOLD"]),
            "crypto at 5,000.01": (self.publish(str(uuid4()), "5000.01", "crypto"), ["RISKY_CATEGORY_LIMIT"]),
            "gambling at 5,000.00 (boundary)": (self.publish(str(uuid4()), "5000.00", "GAMBLING"), []),
        }
        velocity_user = str(uuid4())
        velocity_ids = [self.publish(velocity_user, "10.00", occurred_at=now + timedelta(seconds=i)) for i in range(6)]
        amount_user = str(uuid4())
        amount_ids = [
            self.publish(amount_user, "30000.00", occurred_at=now),
            self.publish(amount_user, "25000.00", occurred_at=now + timedelta(seconds=5)),
        ]
        duplicate_id = self.publish(str(uuid4()), "42.00")
        self.flush()
        self.publish(
            self.decision(duplicate_id)["user_id"],
            "42.00",
            transaction_id=duplicate_id,
            occurred_at=datetime.fromisoformat(self.decision(duplicate_id)["occurred_at"]),
        )
        poison_marker = str(uuid4())
        self.producer.produce(
            self.topic,
            key=b"smoke-poison",
            value=json.dumps(
                {"schema_version": 1, "event_type": "TransactionCreated", "poison": poison_marker}
            ).encode(),
        )
        self.flush()

        for name, (transaction_id, expected) in cases.items():
            decision = self.decision(transaction_id)
            self.check(f"Rule: {name}", decision["triggered_rules"] == expected, f"got {decision['triggered_rules']}")
        velocity = [self.decision(i)["triggered_rules"] for i in velocity_ids]
        self.check(
            "Rule: 6th transaction in 60s trips VELOCITY_COUNT_EXCEEDED",
            velocity == [[]] * 5 + [["VELOCITY_COUNT_EXCEEDED"]],
            str(velocity),
        )
        amounts = [self.decision(i)["triggered_rules"] for i in amount_ids]
        self.check(
            "Rule: 55,000 in 60s trips VELOCITY_AMOUNT",
            amounts == [["HIGH_VALUE_THRESHOLD"], ["HIGH_VALUE_THRESHOLD", "VELOCITY_AMOUNT"]],
            str(amounts),
        )

        page = self.http.get(
            f"{self.api}/api/v1/transactions", headers=self.reader, params={"user_id": velocity_user, "limit": 4}
        ).json()
        rest = self.http.get(
            f"{self.api}/api/v1/transactions",
            headers=self.reader,
            params={"user_id": velocity_user, "limit": 4, "cursor": page["next_cursor"]},
        ).json()
        ids = [row["transaction_id"] for row in page["data"] + rest["data"]]
        self.check(
            "Cursor pagination returns every row once",
            sorted(ids) == sorted(velocity_ids) and rest["next_cursor"] is None,
        )

        time.sleep(3)
        listing = self.http.get(
            f"{self.api}/api/v1/transactions",
            headers=self.reader,
            params={"user_id": self.decision(duplicate_id)["user_id"]},
        ).json()
        self.check("Duplicate publish keeps a single decision", len(listing["data"]) == 1)
        self.check("Poison message reaches the DLQ with failure headers", self.dead_lettered(poison_marker))

        self.rule_update_scenario()

        metrics = self.http.get(self.metrics_url).text
        self.check("Worker exposes processing metrics", "fraud_worker_messages_total{" in metrics)
        self.report()
        return all(passed for _, passed, _ in self.results)

    def dead_lettered(self, marker: str) -> bool:
        consumer = Consumer(
            {"bootstrap.servers": self.bootstrap, "group.id": f"smoke-{uuid4()}", "auto.offset.reset": "earliest"}
        )
        consumer.subscribe([f"{self.topic}.dlq"])
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                message = consumer.poll(1.0)
                if message is None or message.error():
                    continue
                if marker.encode() in (message.value() or b""):
                    headers = dict(message.headers() or [])
                    return headers.get("dlq.error.class") == b"permanent"
            return False
        finally:
            consumer.close()

    def rule_update_scenario(self) -> None:
        base = f"{self.api}/api/v1/rule-sets/fraud-default"
        active = self.http.get(f"{base}/active", headers=self.reader).json()
        lowered = deepcopy(active["definition"])
        lowered["rules"][0]["condition"] = {">=": [{"var": "amount"}, 8000]}
        created = self.http.post(
            f"{base}/versions",
            headers=self.admin,
            json={
                "description": "smoke test: lower high-value threshold to 8,000",
                "definition": lowered,
                "expected_current_version": active["version"],
            },
        )
        self.check("Publish rule-set version -> 201", created.status_code == 201, created.text)
        stale = self.http.post(
            f"{base}/versions",
            headers=self.admin,
            json={"description": "stale writer", "definition": lowered, "expected_current_version": active["version"]},
        )
        self.check("Stale concurrent rule write -> 409", stale.status_code == 409)

        dry_run = self.http.post(
            f"{base}/evaluate",
            headers=self.reader,
            json={"user_id": str(uuid4()), "amount": "8000.00", "category": "GROCERIES"},
        ).json()
        self.check(
            "Dry-run uses the new version", dry_run.get("triggered_rules") == ["HIGH_VALUE_THRESHOLD"], str(dry_run)
        )

        time.sleep(7)  # the worker's in-process rule cache refreshes every 5s
        flagged = self.decision(self.publish(str(uuid4()), "8000.00") if self.flush() is None else "")
        self.check(
            "Worker applies the new version",
            flagged["triggered_rules"] == ["HIGH_VALUE_THRESHOLD"]
            and flagged["rules_version"] == created.json()["label"],
            str(flagged["rules_version"]),
        )

        restored = self.http.post(
            f"{base}/versions",
            headers=self.admin,
            json={
                "description": "smoke test: restore previous rules",
                "definition": active["definition"],
                "expected_current_version": created.json()["version"],
            },
        )
        self.check("Restore previous rules as a new version", restored.status_code == 201)

    def report(self) -> None:
        width = max(len(name) for name, _, _ in self.results)
        for name, passed, detail in self.results:
            line = f"{'PASS' if passed else 'FAIL'}  {name.ljust(width)}"
            print(line + (f"  ({detail})" if not passed and detail else ""))
        failed = sum(1 for _, passed, _ in self.results if not passed)
        print(f"\n{len(self.results) - failed}/{len(self.results)} scenarios passed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    return 0 if Smoke(load_env(args.env_file)).run() else 1


if __name__ == "__main__":
    sys.exit(main())
