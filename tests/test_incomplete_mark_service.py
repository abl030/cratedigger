"""Outcome coverage for the operator incomplete-mark service (issue #1241)."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.incomplete_mark_service import (
    INCOMPLETE_MARK_EXIT_CODES,
    INCOMPLETE_MARK_HTTP_STATUS,
    IncompleteMarkResult,
    set_incomplete_mark,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestSetIncompleteMark(unittest.TestCase):
    """Authoritative branch coverage — both operator surfaces wrap this."""

    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(id=7, status="imported"))

    def test_mark_sets_the_timestamp(self) -> None:
        result = set_incomplete_mark(self.db, 7, marked=True)
        self.assertEqual(
            result, IncompleteMarkResult(outcome="marked", request_id=7)
        )
        row = self.db.get_request(7)
        assert row is not None
        self.assertIsNotNone(row["marked_incomplete_at"])

    def test_mark_is_idempotent(self) -> None:
        set_incomplete_mark(self.db, 7, marked=True)
        row_after_first = self.db.get_request(7)
        assert row_after_first is not None
        first_stamp = row_after_first["marked_incomplete_at"]
        result = set_incomplete_mark(self.db, 7, marked=True)
        self.assertEqual(result.outcome, "already_marked")
        row = self.db.get_request(7)
        assert row is not None
        # The original stamp survives — re-marking never re-stamps.
        self.assertEqual(row["marked_incomplete_at"], first_stamp)

    def test_clear_nulls_the_timestamp(self) -> None:
        set_incomplete_mark(self.db, 7, marked=True)
        result = set_incomplete_mark(self.db, 7, marked=False)
        self.assertEqual(result.outcome, "cleared")
        row = self.db.get_request(7)
        assert row is not None
        self.assertIsNone(row["marked_incomplete_at"])

    def test_clear_on_unmarked_row_is_already_clear(self) -> None:
        result = set_incomplete_mark(self.db, 7, marked=False)
        self.assertEqual(result.outcome, "already_clear")

    def test_unknown_request_is_not_found(self) -> None:
        result = set_incomplete_mark(self.db, 999, marked=True)
        self.assertEqual(result.outcome, "not_found")

    def test_replaced_rows_are_frozen(self) -> None:
        self.db.seed_request(make_request_row(id=8, status="replaced"))
        result = set_incomplete_mark(self.db, 8, marked=True)
        self.assertEqual(result.outcome, "replaced")
        row = self.db.get_request(8)
        assert row is not None
        self.assertIsNone(row["marked_incomplete_at"])

    def test_unknown_db_outcome_raises(self) -> None:
        class _BrokenDB:
            def set_marked_incomplete(
                self, request_id: int, *, marked: bool
            ) -> str:
                del request_id, marked
                return "banana"

        with self.assertRaisesRegex(ValueError, "unknown outcome 'banana'"):
            set_incomplete_mark(_BrokenDB(), 7, marked=True)


class TestOutcomeMapsAgree(unittest.TestCase):
    """CLI ⇄ API symmetry: the exit map is derived from the HTTP map
    (``lib/surface_outcomes.py``), so key/polarity agreement holds by
    construction and ``tests/test_surface_outcomes.py`` audits it; the
    values below pin the convention semantics this action relies on."""

    def test_convention_codes(self) -> None:
        # 404/2 not found, 409/4 wrong state — the repository convention.
        self.assertEqual(INCOMPLETE_MARK_EXIT_CODES["not_found"], 2)
        self.assertEqual(INCOMPLETE_MARK_HTTP_STATUS["not_found"], 404)
        self.assertEqual(INCOMPLETE_MARK_EXIT_CODES["replaced"], 4)
        self.assertEqual(INCOMPLETE_MARK_HTTP_STATUS["replaced"], 409)


if __name__ == "__main__":
    unittest.main()
