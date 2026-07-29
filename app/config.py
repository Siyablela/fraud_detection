import json
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from app.settings import (
	DEFAULT_HIGH_VALUE_THRESHOLD,
	DEFAULT_RESTRICTED_CATEGORIES,
	FRAUD_RULES_CONFIG_PATH,
)


@dataclass(frozen=True)
class RulesConfig:
	high_value_threshold: float = DEFAULT_HIGH_VALUE_THRESHOLD
	restricted_categories: dict[str, float] | None = None

	def __post_init__(self):
		if self.restricted_categories is None:
			object.__setattr__(
				self,
				"restricted_categories",
				DEFAULT_RESTRICTED_CATEGORIES,
			)

	def to_dict(self) -> dict[str, object]:
		return {
			"high_value_threshold": self.high_value_threshold,
			"restricted_categories": dict(sorted(self.restricted_categories.items())),
		}

	def fingerprint(self) -> str:
		payload = json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)
		return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_rules_config(path: str | None = None) -> RulesConfig:
	config_path = Path(path or os.getenv("FRAUD_RULES_CONFIG_PATH", FRAUD_RULES_CONFIG_PATH))
	if not config_path.exists():
		return RulesConfig()

	with config_path.open(encoding="utf-8") as config_file:
		values = json.load(config_file)

	return RulesConfig(
		high_value_threshold=float(values.get("high_value_threshold", DEFAULT_HIGH_VALUE_THRESHOLD)),
		restricted_categories={
			str(category).upper(): float(limit)
			for category, limit in values.get(
				"restricted_categories", DEFAULT_RESTRICTED_CATEGORIES
			).items()
		},
	)


class RulesConfigProvider:
	"""Reload rules when the mounted config file changes."""

	def __init__(self, path: str | None = None):
		self._configured_path = path
		self.path = Path(path or os.getenv("FRAUD_RULES_CONFIG_PATH", FRAUD_RULES_CONFIG_PATH))
		self._modified_at = None
		self._config = RulesConfig()

	def get(self) -> RulesConfig:
		configured_path = self._configured_path or os.getenv(
			"FRAUD_RULES_CONFIG_PATH", FRAUD_RULES_CONFIG_PATH
		)
		if str(self.path) != configured_path:
			self.path = Path(configured_path)
			self._modified_at = None
		modified_at = self.path.stat().st_mtime_ns if self.path.exists() else None
		if modified_at != self._modified_at:
			self._config = load_rules_config(str(self.path))
			self._modified_at = modified_at
		return self._config
