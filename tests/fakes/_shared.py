"""Shared time helpers for the fakes package."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

# Single source of truth for Perth-local bucketing inside the fake.
# Mirrors `(first_seen_at AT TIME ZONE 'Australia/Perth')::date` in
# `lib/pipeline_db/dashboard.py::get_peer_metrics`. Using UTC
# bucketing instead would silently disagree with prod by 8h at the
# day boundary.
_PERTH_TZ = ZoneInfo("Australia/Perth")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


def _as_datetime(value: Any) -> datetime:
    """Normalise a timestamp-ish value to an aware ``datetime``.

    Most test rows now carry real datetimes via ``make_request_row``,
    but older hand-rolled fixtures still use ISO strings. Sorting with a
    mixed key would raise ``TypeError``; this helper collapses both
    shapes to a comparable datetime (aware, UTC) and uses ``_EPOCH`` as
    the sentinel for missing values so ordering stays deterministic.
    """
    if value is None:
        return _EPOCH
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return _EPOCH
        return parsed if parsed.tzinfo else parsed.replace(
            tzinfo=timezone.utc)
    return _EPOCH


