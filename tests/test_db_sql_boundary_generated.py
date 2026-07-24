#!/usr/bin/env python3
"""Generated fail-closed tests for the attempt SQL boundary.

The deterministic pin lives in ``tests/test_pipeline_db.py``. This property
drives the real mixin with arbitrary invalid attempt types and asserts that
validation raises before the SQL execution seam is reached.
"""

from __future__ import annotations

import unittest

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from hypothesis import given
from hypothesis import strategies as st

from lib.pipeline_db.requests import _RequestsMixin


class _NoSqlAttempts(_RequestsMixin):
    """Records an execution attempt while driving the production mixin."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def _execute(self, sql: str, params: object = ()) -> object:
        self.executed.append((sql, params))
        raise AssertionError("unvalidated attempt type reached SQL")


_ATTEMPT_TYPE_TEXT = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-;=()'",
    min_size=0,
    max_size=48,
)
_INVALID_ATTEMPT_TYPES = _ATTEMPT_TYPE_TEXT.filter(
    lambda value: value not in {"search", "download", "validation"})


def assert_attempt_type_rejected_before_execution(
    executed: list[tuple[str, object]],
) -> None:
    if executed:
        raise AssertionError("unapproved attempt type reached the execution seam")


class TestRecordAttemptSqlBoundaryProperties(unittest.TestCase):
    @given(attempt_type=_INVALID_ATTEMPT_TYPES)
    def test_unknown_record_attempt_types_cannot_reach_sql(
        self, attempt_type: str,
    ) -> None:
        db = _NoSqlAttempts()

        with self.assertRaises(ValueError):
            db.record_attempt(1, attempt_type, expected_status="wanted")

        assert_attempt_type_rejected_before_execution(db.executed)


class TestAttemptTypeBoundaryOracleKnownBad(unittest.TestCase):
    def test_oracle_trips_when_unapproved_sql_reaches_execution(self) -> None:
        with self.assertRaisesRegex(AssertionError, "unapproved attempt type"):
            assert_attempt_type_rejected_before_execution([("SELECT injected", ())])
