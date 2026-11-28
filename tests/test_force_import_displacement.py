"""Importer half of Replace ``--adopt --after-import``.

Replace keeps the old release's installed album and hands its exact Beets id
to the adopted candidate's force-import job (``ForceImportDisplacement``).
The importer removes that album only after the job completes AND the new
release resolves in Beets. The result lands as a ``displacement`` receipt,
and startup replays every completed job whose receipt is not proven done.
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
)

from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteFailed,
    BeetsDeleteFailureReason,
    BeetsDeleteOutcome,
    BeetsDeleteRequest,
)
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    ForceImportDisplacement,
    ImportJob,
    force_import_dedupe_key,
    force_import_payload,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row

OLD_MBID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
NEW_MBID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

DISPLACE = ForceImportDisplacement(
    album_id=77,
    expected_release_id=OLD_MBID,
    library_db_path="/tmp/fake-beets-library.db",
    library_root="/tmp/fake-beets-library",
)


def _completed(request: BeetsDeleteRequest) -> BeetsDeleteCompleted:
    return BeetsDeleteCompleted(
        album_id=request.album_id,
        album_name="Old Pressing",
        artist_name="Pet Grief",
        former_album_path="/tmp/fake-beets-library/Pet Grief/Old Pressing",
        deleted_tracks=12,
        deleted_artifacts=1,
        preserved_paths=(),
    )


def _failed(reason: BeetsDeleteFailureReason, *, still_present: bool = True):
    def delete(request: BeetsDeleteRequest) -> BeetsDeleteFailed:
        return BeetsDeleteFailed(
            album_id=request.album_id,
            reason=reason,
            detail=f"simulated {reason}",
            album_still_present=still_present,
        )
    return delete


class _Case(unittest.TestCase):
    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=43, mb_release_id=NEW_MBID, status="imported",
        ))
        self.beets = FakeBeetsDB()
        self.beets.set_album_ids_for_release(NEW_MBID, [88])
        self.beets.set_item_paths(
            NEW_MBID, [(880, "/tmp/fake-beets-library/new/01.flac")],
        )
        self.deletes: list[BeetsDeleteRequest] = []

    def _job(
        self,
        *,
        displace: ForceImportDisplacement | None = DISPLACE,
        completed: bool = True,
        result_extra: dict[str, object] | None = None,
    ) -> ImportJob:
        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=43,
            dedupe_key=force_import_dedupe_key(9),
            payload=force_import_payload(
                download_log_id=9,
                failed_path="/tmp/wrong_matches/sister",
                displace=displace,
            ),
        )
        result: dict[str, object] = {
            "success": completed, "message": "m", "deferred": False,
            "code": None, "post_commit_wrong_match_scenario": None,
        }
        result.update(result_extra or {})
        if completed:
            done = self.db.mark_import_job_completed(
                job.id, result=result, message="imported",
            )
        else:
            done = self.db.mark_import_job_failed(
                job.id, error="nope", result=result, message="nope",
            )
        assert done is not None
        return done

    def _recording(self, outcome_fn=_completed):
        def delete(request: BeetsDeleteRequest) -> BeetsDeleteOutcome:
            self.deletes.append(request)
            return outcome_fn(request)
        return delete

    def _displace(self, job: ImportJob, outcome_fn=_completed):
        from scripts.importer import _displace_after_force_import

        return _displace_after_force_import(
            self.db,
            job,
            beets_db_factory=lambda db_path, root: self.beets,
            beets_delete_fn=self._recording(outcome_fn),
        )


class TestDisplaceAfterForceImport(_Case):
    def test_removes_the_exact_old_album_once_the_new_one_is_in(self):
        receipt = self._displace(self._job())

        self.assertEqual(
            self.deletes,
            [BeetsDeleteRequest(
                album_id=77,
                expected_release_id=OLD_MBID,
                library_db_path=DISPLACE.library_db_path,
                library_root=DISPLACE.library_root,
            )],
        )
        assert receipt is not None
        self.assertTrue(receipt["success"])
        self.assertEqual(receipt["outcome"], "removed")
        self.assertEqual(receipt["album_id"], 77)
        self.assertEqual(receipt["new_album_id"], 88)

    def test_ordinary_force_import_has_no_receipt(self):
        self.assertIsNone(self._displace(self._job(displace=None)))
        self.assertEqual(self.deletes, [])

    def test_failed_import_never_displaces(self):
        self.assertIsNone(self._displace(self._job(completed=False)))
        self.assertEqual(self.deletes, [])

    def test_new_copy_missing_keeps_the_old_one(self):
        self.beets = FakeBeetsDB()

        receipt = self._displace(self._job())

        self.assertEqual(self.deletes, [])
        assert receipt is not None
        self.assertFalse(receipt["success"])
        self.assertEqual(receipt["outcome"], "new_copy_not_in_library")

    def test_old_album_already_gone_counts_as_done(self):
        for reason in ("album_not_found", "release_mismatch"):
            with self.subTest(reason=reason):
                receipt = self._displace(
                    self._job(), _failed(reason, still_present=False),
                )
                assert receipt is not None
                self.assertTrue(receipt["success"])
                self.assertEqual(receipt["outcome"], "not_present")
                self.assertEqual(receipt["reason"], reason)

    def test_delete_failure_is_recorded_for_retry(self):
        receipt = self._displace(self._job(), _failed("filesystem_error"))

        assert receipt is not None
        self.assertFalse(receipt["success"])
        self.assertEqual(receipt["outcome"], "failed")
        self.assertEqual(receipt["reason"], "filesystem_error")

    def test_unexpected_error_is_recorded_not_raised(self):
        def boom(request: BeetsDeleteRequest) -> BeetsDeleteOutcome:
            raise RuntimeError("harness exploded")

        receipt = self._displace(self._job(), boom)

        assert receipt is not None
        self.assertFalse(receipt["success"])
        self.assertEqual(receipt["outcome"], "failed")
        self.assertIn("harness exploded", str(receipt["error"]))


class TestDisplacementReplay(_Case):
    def _replay(self, outcome_fn=_completed) -> None:
        from scripts import importer

        with patch.object(
            importer, "_open_displacement_beets",
            lambda db_path, root: self.beets,
        ), patch.object(
            importer, "run_beets_delete", self._recording(outcome_fn),
        ):
            importer.recover_abandoned_running_jobs(self.db)

    def _receipt(self, job_id: int) -> dict[str, object]:
        job = self.db.get_import_job(job_id)
        assert job is not None and job.result is not None
        receipt = job.result["displacement"]
        assert isinstance(receipt, dict)
        return receipt

    def test_crash_before_the_receipt_is_replayed_at_startup(self):
        job = self._job()

        self._replay()

        self.assertEqual([r.album_id for r in self.deletes], [77])
        self.assertTrue(self._receipt(job.id)["success"])

    def test_failed_receipt_is_retried_and_a_done_one_is_not(self):
        retry = self._job(result_extra={
            "displacement": {"success": False, "outcome": "failed"},
        })

        self._replay()
        self.assertEqual([r.album_id for r in self.deletes], [77])
        self.assertTrue(self._receipt(retry.id)["success"])

        self._replay()
        self.assertEqual(len(self.deletes), 1)

    def test_selection_matches_the_real_predicate(self):
        """Fake predicate pinned against the same matrix the real-PG test
        in ``tests/test_pipeline_db.py`` uses."""
        wanted = self._job()
        failed_receipt = self._job(result_extra={
            "displacement": {"success": False},
        })
        done = self._job(result_extra={"displacement": {"success": True}})
        plain = self._job(displace=None)
        failed_import = self._job(completed=False)

        selected = {
            job.id for job in self.db.list_terminal_force_displacement_jobs()
        }

        self.assertEqual(selected, {wanted.id, failed_receipt.id})
        self.assertNotIn(done.id, selected)
        self.assertNotIn(plain.id, selected)
        self.assertNotIn(failed_import.id, selected)


if __name__ == "__main__":
    unittest.main()
