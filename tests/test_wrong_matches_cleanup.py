"""Tests for shared Wrong Matches cleanup helpers."""

import os
import shutil
import tempfile
import unittest
from typing import TYPE_CHECKING
from unittest.mock import patch

from lib.fs_authority import observe_directory
from lib.wrong_match_delete_service import (
    OUTCOME_SKIPPED_PATH_UNAVAILABLE,
    delete_wrong_match,
)
from lib.wrong_matches import cleanup_wrong_match_source
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def make_failed_import_source() -> tuple[str, str]:
    root = tempfile.mkdtemp()
    source = os.path.join(root, "failed_imports", "Album")
    os.makedirs(source)
    return root, source


class TestWrongMatchCleanup(unittest.TestCase):
    def _make_db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            artist_name="Artist",
            album_title="Album",
            mb_release_id="mbid-1",
            status="unsearchable",
        ))
        return db

    def _log_rejected(
        self,
        db: FakePipelineDB,
        *,
        failed_path: str,
        request_id: int = 1,
        username: str = "alice",
    ) -> int:
        db.log_download(
            request_id,
            soulseek_username=username,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "failed_path": failed_path,
            },
        )
        return db.download_logs[-1].id

    def test_deletes_directory_and_clears_original_wrong_match_row(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_rejected(db, failed_path=source)

            result = cleanup_wrong_match_source(db, log_id)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 1)
            self.assertEqual(result.deleted_path, os.path.abspath(source))
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_deletes_directory_from_dedicated_wrong_matches_root(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        root = tempfile.mkdtemp()
        source = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(source)
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_rejected(db, failed_path=source)

            result = cleanup_wrong_match_source(db, log_id)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 1)
            self.assertEqual(result.deleted_path, os.path.abspath(source))
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_clears_relative_and_absolute_duplicate_rows(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            raw_path = "failed_imports/Artist - Album"
            original_id = self._log_rejected(
                db, failed_path=raw_path, username="old")
            self._log_rejected(
                db, failed_path=os.path.abspath(source), username="new")

            result = cleanup_wrong_match_source(
                db, original_id, failed_path_hint=source)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 2)
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_clears_relative_duplicate_when_absolute_row_is_deleted(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            raw_path = "failed_imports/Album"
            self._log_rejected(db, failed_path=raw_path, username="old")
            absolute_id = self._log_rejected(
                db,
                failed_path=os.path.abspath(source),
                username="new",
            )

            def fake_observe(path):
                if path == raw_path and os.path.isdir(source):
                    return observe_directory(source)
                return observe_directory(path)

            with patch("lib.wrong_matches.observe_failed_path",
                       side_effect=fake_observe):
                result = cleanup_wrong_match_source(db, absolute_id)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 2)
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_missing_directory_still_clears_stale_pointer(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            shutil.rmtree(source)
            log_id = self._log_rejected(db, failed_path=source)

            result = cleanup_wrong_match_source(db, log_id)

            self.assertTrue(result.success)
            self.assertTrue(result.path_missing)
            self.assertIsNone(result.deleted_path)
            self.assertEqual(result.cleared_rows, 1)
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_missing_directory_can_preserve_pointer_for_service_policy(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            shutil.rmtree(source)
            log_id = self._log_rejected(db, failed_path=source)

            result = cleanup_wrong_match_source(
                db,
                log_id,
                clear_missing=False,
            )

            self.assertTrue(result.success)
            self.assertTrue(result.path_missing)
            self.assertEqual(result.cleared_rows, 0)
            self.assertEqual(len(db.get_wrong_matches()), 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_delete_race_still_clears_stale_pointer(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            log_id = self._log_rejected(db, failed_path=source)

            with patch("lib.wrong_matches.shutil.rmtree",
                       side_effect=FileNotFoundError(source)):
                result = cleanup_wrong_match_source(db, log_id)

            self.assertTrue(result.success)
            self.assertTrue(result.path_missing)
            self.assertIsNone(result.deleted_path)
            self.assertEqual(result.resolved_path, os.path.abspath(source))
            self.assertEqual(result.cleared_rows, 1)
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_delete_error_reports_failure_and_keeps_pointer(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            log_id = self._log_rejected(db, failed_path=source)

            with patch("lib.wrong_matches.shutil.rmtree",
                       side_effect=OSError("permission denied")):
                result = cleanup_wrong_match_source(db, log_id)

            self.assertFalse(result.success)
            self.assertIn("permission denied", result.error or "")
            self.assertEqual(result.cleared_rows, 0)
            self.assertEqual(len(db.get_wrong_matches()), 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_cleanup_refuses_directory_outside_failed_imports(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_rejected(db, failed_path=source)

            result = cleanup_wrong_match_source(db, log_id)

            self.assertFalse(result.success)
            self.assertIn("unsafe_failed_import_path", result.error or "")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    # `dismiss_wrong_match_source` (clear pointers, never delete) is gone —
    # its only production caller, `scripts/importer.py::_dismiss_successful_
    # force_import`, now calls `cleanup_wrong_match_source` instead (issue
    # #1077, D7: force-import success consumes its source folder rather than
    # merely dismissing it from the actionable list). The DB-pointer-clearing
    # behaviour these tests covered — relative/absolute duplicate rows, a
    # missing directory, a missing entry — is still exercised by
    # `cleanup_wrong_match_source`'s own tests above (which additionally
    # assert the file deletion), and D7's dedicated pin/property in
    # `tests/test_wrong_match_post_commit_generated.py` drives
    # `_dismiss_successful_force_import` itself end to end.

class TestWrongMatchDeleteService(unittest.TestCase):
    def _make_db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            artist_name="Artist",
            album_title="Album",
            mb_release_id="mbid-1",
            status="unsearchable",
        ))
        return db

    def _log_download(
        self,
        db: FakePipelineDB,
        *,
        failed_path: str,
        outcome: str = "rejected",
        request_id: int = 1,
    ) -> int:
        db.log_download(
            request_id,
            soulseek_username="alice",
            outcome=outcome,
            validation_result={
                "scenario": "high_distance",
                "failed_path": failed_path,
            },
        )
        return db.download_logs[-1].id

    def test_manual_delete_requires_visible_wrong_match_row(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_NOT_VISIBLE,
            delete_wrong_match,
        )

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_download(
                db,
                failed_path=source,
                outcome="success",
            )

            result = delete_wrong_match(db, log_id, require_visible=True)

            self.assertFalse(result.success)
            self.assertEqual(result.outcome, OUTCOME_SKIPPED_NOT_VISIBLE)
            self.assertTrue(os.path.isdir(source))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_delete_skips_when_another_active_job_owns_same_source(self):
        from lib.import_queue import IMPORT_JOB_FORCE
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_ACTIVE_JOB,
            delete_wrong_match,
        )

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_download(db, failed_path=source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=1,
                payload={"download_log_id": 1, "failed_path": source},
            )

            result = delete_wrong_match(db, log_id, require_visible=True)

            self.assertFalse(result.success)
            self.assertEqual(result.outcome, OUTCOME_SKIPPED_ACTIVE_JOB)
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_delete_ignores_current_job_but_blocks_other_matching_job(self):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_payload,
        )
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_ACTIVE_JOB,
            delete_wrong_match,
        )

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_download(db, failed_path=source)
            current = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=1,
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                ),
            )
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=1,
                payload=force_import_payload(
                    download_log_id=log_id + 100,
                    failed_path=source,
                ),
            )

            result = delete_wrong_match(
                db,
                log_id,
                ignore_import_job_id=current.id,
                require_visible=False,
            )

            self.assertFalse(result.success)
            self.assertEqual(result.outcome, OUTCOME_SKIPPED_ACTIVE_JOB)
            self.assertTrue(os.path.isdir(source))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_delete_uses_cleanup_lock(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_LOCKED,
            delete_wrong_match,
        )

        db = self._make_db()
        db.set_advisory_lock_result(False)
        root, source = make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_download(db, failed_path=source)

            result = delete_wrong_match(db, log_id, require_visible=True)

            self.assertEqual(result.outcome, OUTCOME_SKIPPED_LOCKED)
            self.assertTrue(os.path.isdir(source))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_delete_group_deletes_current_request_rows_only(self):
        from lib.wrong_match_delete_service import delete_wrong_match_group

        db = self._make_db()
        db.seed_request(make_request_row(
            id=2,
            artist_name="Other",
            album_title="Album",
            mb_release_id="mbid-2",
            status="unsearchable",
        ))
        root1, source1 = make_failed_import_source()
        root2, source2 = make_failed_import_source()
        root3, source3 = make_failed_import_source()
        try:
            for source in (source1, source2, source3):
                with open(os.path.join(source, "01.mp3"), "wb") as f:
                    f.write(b"audio")
            self._log_download(db, failed_path=source1)
            self._log_download(db, failed_path=source2)
            self._log_download(db, failed_path=source3, request_id=2)

            summary = delete_wrong_match_group(db, 1)

            self.assertTrue(summary.success)
            self.assertEqual(summary.outcome, "deleted")
            self.assertEqual(summary.processed, 2)
            self.assertEqual(summary.deleted, 2)
            self.assertEqual(summary.deleted_paths, 2)
            self.assertEqual(summary.cleared, 2)
            self.assertEqual(summary.skipped, 0)
            self.assertEqual(summary.errors, 0)
            self.assertTrue(summary.group_empty)
            self.assertFalse(os.path.exists(source1))
            self.assertFalse(os.path.exists(source2))
            self.assertTrue(os.path.isdir(source3))
            remaining = db.get_wrong_matches()
            self.assertEqual(len(remaining), 1)
            self.assertEqual(remaining[0]["request_id"], 2)
        finally:
            shutil.rmtree(root1, ignore_errors=True)
            shutil.rmtree(root2, ignore_errors=True)
            shutil.rmtree(root3, ignore_errors=True)

    def test_delete_group_counts_a_proven_absence_as_cleared_missing(self):
        """A pointer-only clear is NOT a deletion, and the count says so.

        The route ships ``cleared_missing`` to the toast; every other
        test builds a ``WrongMatchDeleteSummary`` by hand, so nothing
        proved the REAL group service routes a ``path_missing`` success
        into that field instead of ``deleted`` — the exact overclaim
        issue #1063 removed from the single-delete path.
        """
        from lib.wrong_match_delete_service import (
            OUTCOME_DELETED,
            OUTCOME_PATH_MISSING,
            delete_wrong_match_group,
        )

        db = self._make_db()
        root_present, present = make_failed_import_source()
        root_absent, absent = make_failed_import_source()
        try:
            with open(os.path.join(present, "01.mp3"), "wb") as f:
                f.write(b"audio")
            self._log_download(db, failed_path=present)
            self._log_download(db, failed_path=absent)
            # Prove the absence rather than asserting it: the folder is
            # removed before the service ever looks.
            shutil.rmtree(absent)
            self.assertFalse(os.path.exists(absent))

            summary = delete_wrong_match_group(db, 1)

            self.assertTrue(summary.success)
            self.assertEqual(summary.processed, 2)
            self.assertEqual(summary.deleted, 1)
            self.assertEqual(summary.cleared_missing, 1)
            self.assertEqual(summary.deleted_paths, 1)
            self.assertEqual(summary.cleared, 2)
            self.assertEqual(
                sorted(result.outcome for result in summary.results),
                sorted((OUTCOME_DELETED, OUTCOME_PATH_MISSING)),
            )
            self.assertFalse(os.path.exists(present))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root_present, ignore_errors=True)
            shutil.rmtree(root_absent, ignore_errors=True)

    def test_group_success_always_means_nothing_remains(self):
        """``success`` implies ``remaining == 0`` — pinned where it is defined.

        ``MbidReplaceService._finalize_replace`` warns on
        ``not summary.success`` and states in a comment that success
        already covers ``remaining``. That is a real coupling with no
        test behind it: a future edit to the ``success`` expression that
        dropped ``remaining == 0`` would make Replace claim a clean
        supersede over surviving Wrong Matches sources — the incident in
        issue #1063's audit section.
        """
        from lib.wrong_match_delete_service import delete_wrong_match_group

        db = self._make_db()
        root_ok, deletable = make_failed_import_source()
        root_locked, blocked = make_failed_import_source()
        try:
            for source in (deletable, blocked):
                with open(os.path.join(source, "01.mp3"), "wb") as f:
                    f.write(b"audio")
            self._log_download(db, failed_path=deletable)
            blocked_id = self._log_download(db, failed_path=blocked)
            job = db.enqueue_import_job(
                "force_import",
                request_id=1,
                payload={"download_log_id": blocked_id,
                         "failed_path": blocked},
            )
            assert job is not None

            summary = delete_wrong_match_group(db, 1)

            # The world the invariant is about: one row survives.
            self.assertGreater(summary.remaining, 0)
            self.assertFalse(summary.success)
            self.assertTrue(os.path.isdir(blocked))
            # Must still work: with the job terminal, nothing remains and
            # success becomes available again.
            db.mark_import_job_failed(job.id, error="operator cancelled")
            second = delete_wrong_match_group(db, 1)
            self.assertEqual(second.remaining, 0)
            self.assertTrue(second.success)
        finally:
            shutil.rmtree(root_ok, ignore_errors=True)
            shutil.rmtree(root_locked, ignore_errors=True)

    def test_delete_refuses_directory_outside_failed_imports(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_UNSAFE_PATH,
            delete_wrong_match,
        )

        db = self._make_db()
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_download(db, failed_path=source)

            result = delete_wrong_match(db, log_id, require_visible=True)

            self.assertFalse(result.success)
            self.assertEqual(result.outcome, OUTCOME_SKIPPED_UNSAFE_PATH)
            self.assertIn("unsafe_failed_import_path", result.reason or "")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
        finally:
            shutil.rmtree(source, ignore_errors=True)


if TYPE_CHECKING:
    from typing import cast

    from lib.pipeline_db import PipelineDB
    from lib.wrong_match_delete_service import WrongMatchDeleteDB as _DeleteDB
    from lib.wrong_matches import WrongMatchSourceDB as _SourceDB

    # Static parity proof — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    _pipeline_db_satisfies_delete_protocol: _DeleteDB = cast("PipelineDB", None)
    _fake_db_satisfies_delete_protocol: _DeleteDB = cast("FakePipelineDB", None)
    _pipeline_db_satisfies_source_protocol: _SourceDB = cast("PipelineDB", None)
    _fake_db_satisfies_source_protocol: _SourceDB = cast("FakePipelineDB", None)


class TestDeleteDBProtocolParity(unittest.TestCase):
    """#409: PipelineDB and FakePipelineDB must satisfy WrongMatchDeleteDB."""

    def test_pipeline_db_satisfies_protocol(self) -> None:
        from lib.pipeline_db import PipelineDB
        from lib.wrong_match_delete_service import WrongMatchDeleteDB

        self.assertTrue(issubclass(PipelineDB, WrongMatchDeleteDB))

    def test_fake_pipeline_db_satisfies_protocol(self) -> None:
        from lib.wrong_match_delete_service import WrongMatchDeleteDB

        self.assertTrue(issubclass(FakePipelineDB, WrongMatchDeleteDB))


class TestSourceDBProtocolParity(unittest.TestCase):
    """#409: PipelineDB and FakePipelineDB must satisfy WrongMatchSourceDB."""

    def test_pipeline_db_satisfies_protocol(self) -> None:
        from lib.pipeline_db import PipelineDB
        from lib.wrong_matches import WrongMatchSourceDB

        self.assertTrue(issubclass(PipelineDB, WrongMatchSourceDB))

    def test_fake_pipeline_db_satisfies_protocol(self) -> None:
        from lib.wrong_matches import WrongMatchSourceDB

        self.assertTrue(issubclass(FakePipelineDB, WrongMatchSourceDB))

    def test_service_protocols_extend_source_protocol(self) -> None:
        """The services forward their handle into wrong_matches helpers, so
        their protocols must declare the source surface too."""
        from lib.wrong_match_cleanup_service import WrongMatchCleanupDB
        from lib.wrong_match_delete_service import WrongMatchDeleteDB
        from lib.wrong_matches import WrongMatchSourceDB

        self.assertTrue(issubclass(WrongMatchCleanupDB, WrongMatchSourceDB))
        self.assertTrue(issubclass(WrongMatchDeleteDB, WrongMatchSourceDB))


class TestRefusedCandidateOutranksAbsentCandidate(unittest.TestCase):
    """One refused candidate name poisons the whole aggregate (#1063).

    Several names describe one source: the converge/cleanup hint, the
    row's own ``failed_path``, and its equivalent aliases. If ANY of them
    could not be observed, the aggregate is indeterminate — otherwise the
    exact live shape returns: a legacy relative row whose direct probe
    says ENOENT while the slskd-root probe says EACCES, laundered into
    ``path_missing=True`` and a cleared pointer over an intact folder.

    Both candidate orderings are pinned because the two mutants that
    survived review differed only in which end of the sequence won.
    """

    def _world(self, tmp: str) -> tuple[FakePipelineDB, str, str]:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="mbid-1"))
        unreadable_parent = os.path.join(tmp, "unreadable", "wrong_matches")
        unreadable = os.path.join(unreadable_parent, "Album")
        os.makedirs(unreadable)
        with open(os.path.join(unreadable, "01.mp3"), "wb") as handle:
            handle.write(b"audio")
        absent = os.path.join(tmp, "gone", "wrong_matches", "Album")
        os.makedirs(os.path.dirname(absent))
        os.chmod(unreadable_parent, 0o000)
        self.addCleanup(os.chmod, unreadable_parent, 0o700)
        return db, unreadable, absent

    def _log(self, db: FakePipelineDB, failed_path: str) -> int:
        return db.log_download(
            1,
            outcome="rejected",
            validation_result={
                "scenario": "wrong_match",
                "failed_path": failed_path,
            },
        )

    def test_refused_hint_with_absent_row_path_refuses(self) -> None:
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        db, unreadable, absent = self._world(tmp)
        log_id = self._log(db, absent)

        result = delete_wrong_match(
            db, log_id, failed_path_hint=unreadable, require_visible=True)

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_PATH_UNAVAILABLE)
        self.assertFalse(result.success)
        self.assertFalse(result.path_missing)
        self.assertIsNone(result.deleted_path)
        self.assertEqual(result.cleared_rows, 0)
        os.chmod(os.path.dirname(unreadable), 0o700)
        self.assertTrue(os.path.isdir(unreadable))
        self.assertEqual(
            [row["download_log_id"] for row in db.get_wrong_matches()],
            [log_id],
        )

    def test_absent_hint_with_refused_row_path_refuses(self) -> None:
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        db, unreadable, absent = self._world(tmp)
        log_id = self._log(db, unreadable)

        result = delete_wrong_match(
            db, log_id, failed_path_hint=absent, require_visible=True)

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_PATH_UNAVAILABLE)
        self.assertEqual(result.cleared_rows, 0)
        os.chmod(os.path.dirname(unreadable), 0o700)
        self.assertTrue(os.path.isdir(unreadable))
        self.assertEqual(len(db.get_wrong_matches()), 1)

    def test_all_candidates_absent_still_clears(self) -> None:
        """Must still work: with no refusal anywhere, absence clears."""
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        db, _unreadable, absent = self._world(tmp)
        other_absent = os.path.join(tmp, "gone", "wrong_matches", "Other")
        log_id = self._log(db, absent)

        result = delete_wrong_match(
            db, log_id, failed_path_hint=other_absent, require_visible=True)

        self.assertTrue(result.success)
        self.assertTrue(result.path_missing)
        self.assertEqual(result.cleared_rows, 1)
        self.assertEqual(db.get_wrong_matches(), [])

    def test_relative_legacy_row_under_an_unreadable_search_dir_refuses(self) -> None:
        """The exact #1063 relative shape, through the real cleanup helper."""
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="mbid-1"))
        base = os.path.join(tmp, "slskd")
        quarantine = os.path.join(base, "failed_imports")
        album = os.path.join(quarantine, "Album")
        os.makedirs(album)
        log_id = self._log(db, "failed_imports/Album")
        os.chmod(quarantine, 0o000)
        self.addCleanup(os.chmod, quarantine, 0o700)

        with patch("lib.util.FAILED_IMPORT_SEARCH_DIRS", (base,)):
            result = cleanup_wrong_match_source(db, log_id)

        self.assertTrue(result.path_unavailable)
        self.assertFalse(result.path_missing)
        self.assertFalse(result.success)
        self.assertEqual(result.cleared_rows, 0)
        os.chmod(quarantine, 0o700)
        self.assertTrue(os.path.isdir(album))
        self.assertEqual(len(db.get_wrong_matches()), 1)


if __name__ == "__main__":
    unittest.main()
