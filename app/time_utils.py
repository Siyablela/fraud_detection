from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    """Return the current instant as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def utc_now_unix_epoch() -> float:
    """Return the current Unix timestamp in UTC.

    Unix timestamps represent seconds since the UTC epoch and are therefore consistent
    across local timezone configurations.
    """
    return time.time()


def normalize_utc_datetime(value: Any) -> str | None:
    """Convert a datetime-like value to an ISO 8601 UTC string."""
    if value is None:
        return None

    if not hasattr(value, "utcoffset"):
        return None

    aware_value = value
    if aware_value.tzinfo is None:
        aware_value = aware_value.replace(tzinfo=timezone.utc)
    return aware_value.astimezone(timezone.utc).isoformat()
