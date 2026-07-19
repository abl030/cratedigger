"""Tests for shared Wrong Matches cleanup helpers."""

import os
import shutil
import tempfile
import unittest
from typing import TYPE_CHECKING
from unittest.mock import patch

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
            status="manual",
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

            def fake_resolve(path):
                if path == raw_path and os.path.isdir(source):
                    return os.path.abspath(source)
                if os.path.isdir(path):
                    return os.path.abspath(path)
                return None

            with patch("lib.wrong_matches.resolve_failed_path",
                       side_effect=fake_resolve):
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

    def test_dismiss_clears_pointer_without_deleting_directory(self):
        from lib.wrong_matches import dismiss_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_rejected(db, failed_path=source)

            result = dismiss_wrong_match_source(db, log_id)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 1)
            self.assertEqual(result.resolved_path, os.path.abspath(source))
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_dismiss_clears_relative_and_absolute_duplicate_rows(self):
        from lib.wrong_matches import dismiss_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            raw_path = "failed_imports/Artist - Album"
            original_id = self._log_rejected(
                db, failed_path=raw_path, username="old")
            self._log_rejected(
                db, failed_path=os.path.abspath(source), username="new")

            result = dismiss_wrong_match_source(
                db, original_id, failed_path_hint=source)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 2)
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_dismiss_missing_directory_still_clears_stale_pointer(self):
        from lib.wrong_matches import dismiss_wrong_match_source

        db = self._make_db()
        root, source = make_failed_import_source()
        try:
            shutil.rmtree(source)
            log_id = self._log_rejected(db, failed_path=source)

            result = dismiss_wrong_match_source(db, log_id)

            self.assertTrue(result.success)
            self.assertIsNone(result.resolved_path)
            self.assertEqual(result.cleared_rows, 1)
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_dismiss_missing_entry_reports_failure(self):
        from lib.wrong_matches import dismiss_wrong_match_source

        db = self._make_db()

        result = dismiss_wrong_match_source(db, 99999)

        self.assertFalse(result.success)
        self.assertFalse(result.entry_found)
        self.assertEqual(result.cleared_rows, 0)
        self.assertIn("99999", result.error or "")


class TestWrongMatchDeleteService(unittest.TestCase):
    def _make_db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            artist_name="Artist",
            album_title="Album",
            mb_release_id="mbid-1",
            status="manual",
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
                payload={"failed_path": source},
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
            status="manual",
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


if __name__ == "__main__":
    unittest.main()
