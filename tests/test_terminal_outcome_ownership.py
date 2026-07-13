"""Structural ratchets for DB-owned terminal outcome transactions."""

from __future__ import annotations

import ast
import inspect
import textwrap
import unittest

from lib.pipeline_db.terminal_outcomes import _TerminalOutcomesMixin
from scripts import import_preview_worker, importer


TERMINAL_WRITERS = (
    "persist_import_success",
    "persist_importer_rejection",
    "persist_preview_measurement_failure",
)
COMMITTING_HELPERS = frozenset({
    "add_denylist",
    "finalize_request",
    "log_download",
    "mark_import_job_completed",
    "mark_import_job_failed",
    "mark_import_job_preview_failed",
    "mark_imported_with_rescue",
    "reset_downloading_to_wanted",
    "reset_to_wanted",
})


def called_attribute_names(source: str) -> set[str]:
    tree = ast.parse(textwrap.dedent(source))
    return {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }


def assert_db_owned_terminal_writer(source: str) -> None:
    calls = called_attribute_names(source)
    if "_atomic" not in calls:
        raise AssertionError("terminal writer does not own an explicit transaction")
    composed = sorted(calls & COMMITTING_HELPERS)
    if composed:
        raise AssertionError(
            "terminal writer composes public committing helpers: "
            + ", ".join(composed)
        )


class TestTerminalOutcomeOwnership(unittest.TestCase):
    def test_every_terminal_writer_owns_transaction_without_public_helpers(self) -> None:
        for method_name in TERMINAL_WRITERS:
            with self.subTest(method=method_name):
                method = getattr(_TerminalOutcomesMixin, method_name)
                assert_db_owned_terminal_writer(inspect.getsource(method))

    def test_preview_worker_has_one_terminal_write_and_no_suppression(self) -> None:
        source = textwrap.dedent(
            inspect.getsource(import_preview_worker._handle_measurement_failed)
        )
        tree = ast.parse(source)
        self.assertNotIn("mark_import_job_preview_failed", called_attribute_names(source))
        self.assertFalse(
            any(isinstance(node, ast.Try) for node in ast.walk(tree)),
            "preview terminal failures must propagate and roll back",
        )
        self.assertEqual(source.count("_record_preview_measurement_failed("), 1)

    def test_importer_observes_terminal_job_before_legacy_finalizers(self) -> None:
        source = textwrap.dedent(inspect.getsource(importer.process_claimed_job))
        observed_at = source.index("persisted = db.get_import_job(job.id)")
        completed_at = source.index("return db.mark_import_job_completed(")
        failed_at = source.rindex("return db.mark_import_job_failed(")
        self.assertLess(observed_at, completed_at)
        self.assertLess(observed_at, failed_at)
        self.assertIn('persisted.status in ("completed", "failed")', source)


class TestTerminalOwnershipCheckerTrips(unittest.TestCase):
    def test_checker_rejects_planted_public_helper_composition(self) -> None:
        planted = """
        def persist_broken(self, outcome):
            with self._atomic():
                self.log_download(request_id=outcome.request_id)
        """
        with self.assertRaisesRegex(AssertionError, "log_download"):
            assert_db_owned_terminal_writer(planted)

    def test_checker_rejects_planted_missing_transaction(self) -> None:
        planted = """
        def persist_broken(self, outcome):
            self._execute("UPDATE album_requests SET status = 'wanted'")
        """
        with self.assertRaisesRegex(AssertionError, "explicit transaction"):
            assert_db_owned_terminal_writer(planted)


if __name__ == "__main__":
    unittest.main()
