from decimal import Decimal
import json
import os
from dataclasses import dataclass
from pathlib import Path

from app.settings import get_settings


@dataclass(frozen=True)
class RulesConfig:
    high_value_threshold: Decimal | None = None
    restricted_categories: dict[str, float] | None = None
    velocity_window_seconds: int | None = None
    velocity_max_count: int | None = None
    velocity_max_amount: Decimal | None = None

    def __post_init__(self):
        """Populate missing rule settings from the active application settings.

        Returns:
            None: The dataclass fields are updated in place with the default runtime values.
        """
        settings = get_settings()
        if self.high_value_threshold is None:
            object.__setattr__(
                self, "high_value_threshold", settings.default_high_value_threshold
            )
        if self.restricted_categories is None:
            object.__setattr__(
                self,
                "restricted_categories",
                settings.default_restricted_categories,
            )
        if self.velocity_window_seconds is None:
            object.__setattr__(
                self,
                "velocity_window_seconds",
                settings.default_velocity_window_seconds,
            )
        if self.velocity_max_count is None:
            object.__setattr__(
                self, "velocity_max_count", settings.default_velocity_max_count
            )
        if self.velocity_max_amount is None:
            object.__setattr__(
                self, "velocity_max_amount", settings.default_velocity_max_amount
            )

    @property
    def velocity_enabled(self) -> bool:
        """Determine if the velocity rules are enabled based on the current configuration."""
        return bool(self.velocity_window_seconds) and (
            bool(self.velocity_max_count) and bool(self.velocity_max_amount)
        )


def load_rules_config(path: str | None = None) -> RulesConfig:
    """Load the JSON rule configuration from the configured file path.

    Args:
        path: Optional override for the rules file path.

    Returns:
        RulesConfig: A validated rules configuration built from the file or the app defaults.
    """
    settings = get_settings()
    config_path = Path(
        path or os.getenv("FRAUD_RULES_CONFIG_PATH", settings.fraud_rules_config_path)
    )
    if not config_path.exists():
        return RulesConfig()

    with config_path.open(encoding="utf-8") as config_file:
        values = json.load(config_file)

    return RulesConfig(
        high_value_threshold=float(
            values.get("high_value_threshold", settings.default_high_value_threshold)
        ),
        restricted_categories={
            str(category).upper(): float(limit)
            for category, limit in values.get(
                "restricted_categories", settings.default_restricted_categories
            ).items()
        },
        velocity_window_seconds=int(
            values.get(
                "velocity_window_seconds", settings.default_velocity_window_seconds
            )
        ),
        velocity_max_count=Decimal(
            values.get("velocity_max_count", settings.default_velocity_max_count)
        ),
        velocity_max_amount=Decimal(
            values.get("velocity_max_amount", settings.default_velocity_max_amount)
        ),
    )


class RulesConfigProvider:
    """Reload rules when the mounted config file changes."""

    def __init__(self, path: str | None = None):
        """Create a configuration provider that reloads rules when the file changes.

        Args:
            path: Optional path override for the rule configuration file.
        """
        settings = get_settings()
        self._configured_path = path
        self.path = Path(
            path
            or os.getenv("FRAUD_RULES_CONFIG_PATH", settings.fraud_rules_config_path)
        )
        self._modified_at = None
        self._config = RulesConfig()

    def get(self) -> RulesConfig:
        """Return the current in-memory rules configuration, reloading when the file changes.

        Returns:
            RulesConfig: The live rules configuration, reloaded if the source file changed.
        """
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
