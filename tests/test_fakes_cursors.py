"""Self-tests for ``tests/fakes/cursors.py``'s FakeCursor.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest

from tests.fakes import (
    FakeCursor,
    FakePipelineDB,
)


class TestFakeCursor(unittest.TestCase):
    """FakeCursor pairs with FakePipelineDB.queue_execute_results for
    raw-SQL seams (web.overlay.check_pipeline et al.). Consumption
    semantics mirror real psycopg2 cursors (test-fidelity Rule B)."""

    def test_fetchall_returns_rows(self):
        rows = [{"id": 1}, {"id": 2}]
        self.assertEqual(FakeCursor(rows).fetchall(), rows)

    def test_fetchone_consumes_like_a_real_cursor(self):
        cur = FakeCursor([{"id": 1}, {"id": 2}])
        self.assertEqual(cur.fetchone(), {"id": 1})
        self.assertEqual(cur.fetchone(), {"id": 2})
        self.assertIsNone(cur.fetchone())
        self.assertIsNone(FakeCursor().fetchone())

    def test_fetchall_after_fetchone_returns_remainder(self):
        cur = FakeCursor([{"id": 1}, {"id": 2}, {"id": 3}])
        cur.fetchone()
        self.assertEqual(cur.fetchall(), [{"id": 2}, {"id": 3}])
        self.assertEqual(cur.fetchall(), [])

    def test_while_fetchone_loop_terminates(self):
        cur = FakeCursor([{"id": 1}, {"id": 2}])
        drained = []
        while (row := cur.fetchone()) is not None:
            drained.append(row)
        self.assertEqual(len(drained), 2)

    def test_empty_default_fetchall(self):
        self.assertEqual(FakeCursor().fetchall(), [])

    def test_queued_through_fake_pipeline_db_execute(self):
        db = FakePipelineDB()
        db.queue_execute_results(FakeCursor([{"id": 7}]))
        cur = db._execute("SELECT 1", ())
        self.assertEqual(cur.fetchall(), [{"id": 7}])

    def test_unqueued_execute_returns_empty_cursor_not_none(self):
        """Production _execute always returns a cursor; the unqueued
        fake degrades to "query ran, zero rows" instead of a None that
        AttributeErrors at the caller's fetchall()."""
        db = FakePipelineDB()
        cur = db._execute("SELECT 1", ())
        self.assertEqual(cur.fetchall(), [])
        self.assertIsNone(cur.fetchone())


if __name__ == "__main__":
    unittest.main()
