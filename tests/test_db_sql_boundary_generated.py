#!/usr/bin/env python3
"""Generated fail-closed tests for dashboard and attempt SQL boundaries.

The deterministic pins live in ``tests/test_pipeline_db.py``.  These
properties drive the real mixin methods with arbitrary non-allowlisted text
and assert that validation raises before their SQL execution seam is reached.
"""

from __future__ import annotations

import unittest

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from hypothesis import given
from hypothesis import strategies as st

from lib.pipeline_db.dashboard import _DashboardMixin
from lib.pipeline_db.requests import _RequestsMixin


class _NoSqlDashboard(_DashboardMixin):
    """Records an execution attempt while driving the production mixin."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def _execute(self, sql: str, params: object = ()) -> object:
        self.executed.append((sql, params))
        raise AssertionError("unvalidated dashboard fragment reached SQL")


class _NoSqlAttempts(_RequestsMixin):
    """Records an execution attempt while driving the production mixin."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def _execute(self, sql: str, params: object = ()) -> object:
        self.executed.append((sql, params))
        raise AssertionError("unvalidated attempt type reached SQL")


_FRAGMENT_TEXT = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-;=()'",
    min_size=0,
    max_size=48,
)
_INVALID_ATTEMPT_TYPES = _FRAGMENT_TEXT.filter(
    lambda value: value not in {"search", "download", "validation"})
_INVALID_ORDER_BY = _FRAGMENT_TEXT.filter(
    lambda value: value not in {"created_at DESC", "cycle_total_s DESC"})
_INVALID_WHERE = _FRAGMENT_TEXT.filter(
    lambda value: value != "created_at >= NOW() - %s::interval")


def assert_sql_rejected_before_execution(executed: list[tuple[str, object]]) -> None:
    if executed:
        raise AssertionError("unapproved SQL fragment reached the execution seam")


class TestSqlBoundaryProperties(unittest.TestCase):
    @given(attempt_type=_INVALID_ATTEMPT_TYPES)
    def test_unknown_attempt_types_cannot_reach_sql(self, attempt_type: str) -> None:
        db = _NoSqlAttempts()

        with self.assertRaises(ValueError):
            db.record_attempt(1, attempt_type, expected_status="wanted")

        assert_sql_rejected_before_execution(db.executed)

    @given(order_by=_INVALID_ORDER_BY)
    def test_unknown_order_by_cannot_reach_sql(self, order_by: str) -> None:
        db = _NoSqlDashboard()

        with self.assertRaises(ValueError):
            db._dashboard_cycle_rows(order_by=order_by, limit=1)

        assert_sql_rejected_before_execution(db.executed)

    @given(where=_INVALID_WHERE)
    def test_unknown_where_cannot_reach_sql(self, where: str) -> None:
        db = _NoSqlDashboard()

        with self.assertRaises(ValueError):
            db._dashboard_cycle_rows(
                order_by="created_at DESC", limit=1, where=where)

        assert_sql_rejected_before_execution(db.executed)


class TestSqlBoundaryOracleKnownBad(unittest.TestCase):
    def test_oracle_trips_when_unapproved_sql_reaches_execution(self) -> None:
        with self.assertRaisesRegex(AssertionError, "reached the execution seam"):
            assert_sql_rejected_before_execution([("SELECT injected", ())])
