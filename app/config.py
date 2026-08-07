import json
import os
from dataclasses import dataclass
from pathlib import Path

from app.settings import get_settings


@dataclass(frozen=True)
class RulesConfig:
	high_value_threshold: float | None = None
	restricted_categories: dict[str, float] | None = None

	def __post_init__(self):
		settings = get_settings()
		if self.high_value_threshold is None:
			object.__setattr__(self, "high_value_threshold", settings.default_high_value_threshold)
		if self.restricted_categories is None:
			object.__setattr__(
				self,
				"restricted_categories",
				settings.default_restricted_categories,
			)


def load_rules_config(path: str | None = None) -> RulesConfig:
	settings = get_settings()
	config_path = Path(path or os.getenv("FRAUD_RULES_CONFIG_PATH", settings.fraud_rules_config_path))
	if not config_path.exists():
		return RulesConfig()

	with config_path.open(encoding="utf-8") as config_file:
		values = json.load(config_file)

	return RulesConfig(
		high_value_threshold=float(values.get("high_value_threshold", settings.default_high_value_threshold)),
		restricted_categories={
			str(category).upper(): float(limit)
			for category, limit in values.get(
				"restricted_categories", settings.default_restricted_categories
			).items()
		},
	)


class RulesConfigProvider:
	"""Reload rules when the mounted config file changes."""

	def __init__(self, path: str | None = None):
		settings = get_settings()
		self._configured_path = path
		self.path = Path(path or os.getenv("FRAUD_RULES_CONFIG_PATH", settings.fraud_rules_config_path))
		self._modified_at = None
		self._config = RulesConfig()

	def get(self) -> RulesConfig:
		settings = get_settings()
		configured_path = self._configured_path or os.getenv(
			"FRAUD_RULES_CONFIG_PATH", settings.fraud_rules_config_path
		)
		if str(self.path) != configured_path:
			self.path = Path(configured_path)
			self._modified_at = None
		modified_at = self.path.stat().st_mtime_ns if self.path.exists() else None
		if modified_at != self._modified_at:
			self._config = load_rules_config(str(self.path))
			self._modified_at = modified_at
		return self._config
