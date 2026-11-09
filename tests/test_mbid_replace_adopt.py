"""Replace ``--adopt``: supersede onto the pressing a Wrong Matches
candidate actually is, keep that candidate, and force-import it there.

The operator flow this serves: a download lands in Wrong Matches because it
is a sibling pressing of the requested release (one track different). The
operator does not want the original pressing; they want the download kept as
what it is. Replace already supersedes the request and removes the old
library album; ``adopt`` makes the candidate survive the old request's Wrong
Matches cleanup and queues its import against the new request.

Runs the REAL ``delete_wrong_match_group`` over real folders so "the adopted
folder survives while its siblings go" is observed on disk, not stubbed.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
)

from lib.config import CratediggerConfig
from lib.import_queue import IMPORT_JOB_FORCE, ForceImportPayload
from lib.mbid_replace_service import (
    RESULT_REPLACED,
    RESULT_TARGET_INVALID,
)
from lib.replace_status import REPLACE_REASON_ADOPT_CANDIDATE_INVALID
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row, seed_visible_wrong_match
from tests.test_mbid_replace_service import NEW_MBID, _ServiceCase


class TestReplaceAdopt(_ServiceCase):
    def setUp(self) -> None:
        for target in (
            "lib.mbid_replace_service.trigger_plex_scan",
            "lib.mbid_replace_service.trigger_jellyfin_scan",
        ):
            p = patch(target)
            p.start()
            self.addCleanup(p.stop)
        root = self.enterContext(tempfile.TemporaryDirectory())
        self.processing = os.path.join(root, "processing")
        self.albums = os.path.join(self.processing, "albums")
        os.makedirs(self.albums)
        self.cfg = CratediggerConfig(processing_dir=self.processing)
        self.db = FakePipelineDB()
        self._seed_old(self.db)

    def _seed(self, name: str):
        return seed_visible_wrong_match(
            self.db, self.albums, request_id=42, name=name,
        )

    def _replace(self, adopt: int | None):
        svc = self._make_service(self.db, cfg=self.cfg)
        return svc.replace_request_mbid(
            42, target_mb_release_id=NEW_MBID, adopt_download_log_id=adopt,
        )

    def _assert_nothing_superseded(self) -> None:
        old = self.db.get_request(42)
        assert old is not None
        self.assertEqual(old["status"], "wanted")
        self.assertIsNone(self.db.get_request_by_mb_release_id(NEW_MBID))
        self.assertEqual(self.db.list_import_jobs(), [])

    def test_adopted_candidate_moves_survives_and_is_queued(self):
        adopted = self._seed("Sibling Pressing [aaaa]")
        # An older download_log row naming the SAME folder: left on the old
        # request it would carry the folder into the group delete.
        older = self.db.log_download(
            42, outcome="rejected",
            validation_result={"scenario": "wrong_match",
                               "failed_path": adopted.path},
        )
        other = self._seed("Unrelated Junk [bbbb]")

        result = self._replace(adopted.download_log_id)

        self.assertEqual(result.outcome, RESULT_REPLACED, result)
        new_id = result.new_request_id
        assert new_id is not None
        self.assertEqual(result.adopt_download_log_id, adopted.download_log_id)
        self.assertIsNotNone(result.adopt_import_job_id)
        self.assertEqual(result.warnings, ())

        # The adopted folder survives; the source's other candidate does not.
        self.assertTrue(os.path.isdir(adopted.path))
        self.assertFalse(os.path.isdir(other.path))

        # Every row naming the adopted folder now belongs to the new request.
        for log_id in (adopted.download_log_id, older):
            entry = self.db.get_download_log_entry(log_id)
            assert entry is not None
            self.assertEqual(entry["request_id"], new_id)

        [job] = self.db.list_import_jobs()
        self.assertEqual(job.id, result.adopt_import_job_id)
        self.assertEqual(job.job_type, IMPORT_JOB_FORCE)
        self.assertEqual(job.request_id, new_id)
        assert isinstance(job.payload, ForceImportPayload)
        self.assertEqual(job.payload.download_log_id, adopted.download_log_id)
        self.assertEqual(
            os.path.realpath(job.payload.failed_path),
            os.path.realpath(adopted.path),
        )

    def test_other_spelling_of_the_same_folder_moves_too(self):
        """Wrong Matches groups by raw path text, so a second spelling of
        the same folder is a separate source candidate. Left behind, the
        group cleanup would resolve it and rmtree the adopted folder."""
        adopted = self._seed("Sibling Pressing [aaaa]")
        alias = os.path.join(
            self.albums, "wrong_matches", ".", "Sibling Pressing [aaaa]")
        alias_log = self.db.log_download(
            42, outcome="rejected",
            validation_result={"scenario": "wrong_match",
                               "failed_path": alias},
        )

        result = self._replace(adopted.download_log_id)

        self.assertEqual(result.outcome, RESULT_REPLACED)
        self.assertEqual(result.warnings, ())
        self.assertIsNotNone(result.adopt_import_job_id)
        self.assertTrue(os.path.isdir(adopted.path))
        entry = self.db.get_download_log_entry(alias_log)
        assert entry is not None
        self.assertEqual(entry["request_id"], result.new_request_id)

    def test_candidate_with_an_active_import_job_is_refused(self):
        """A live job for the candidate would be deduped onto and stay
        bound to the soon-``replaced`` source request, never claimable."""
        adopted = self._seed("Sibling Pressing [aaaa]")
        self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=f"force_import:{adopted.download_log_id}",
            payload={"download_log_id": adopted.download_log_id,
                     "failed_path": adopted.path},
        )

        result = self._replace(adopted.download_log_id)

        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_ADOPT_CANDIDATE_INVALID)
        assert result.error_message is not None
        self.assertIn("active import job", result.error_message)
        old = self.db.get_request(42)
        assert old is not None
        self.assertEqual(old["status"], "wanted")
        self.assertIsNone(self.db.get_request_by_mb_release_id(NEW_MBID))

    def test_candidate_whose_folder_is_gone_is_refused(self):
        adopted = self._seed("Sibling Pressing [aaaa]")
        os.rename(adopted.path, adopted.path + ".moved")

        result = self._replace(adopted.download_log_id)

        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_ADOPT_CANDIDATE_INVALID)
        self._assert_nothing_superseded()

    def test_without_adopt_the_group_cleanup_takes_every_candidate(self):
        """Control: the same world without ``adopt`` loses the folder —
        the behaviour ``adopt`` exists to avoid."""
        candidate = self._seed("Sibling Pressing [aaaa]")

        result = self._replace(None)

        self.assertEqual(result.outcome, RESULT_REPLACED)
        self.assertIsNone(result.adopt_download_log_id)
        self.assertIsNone(result.adopt_import_job_id)
        self.assertFalse(os.path.isdir(candidate.path))
        self.assertEqual(self.db.list_import_jobs(), [])

    def test_candidate_of_another_request_is_refused_before_any_mutation(self):
        self.db.seed_request(make_request_row(
            id=77, status="wanted",
            mb_release_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
        ))
        foreign = seed_visible_wrong_match(
            self.db, self.albums, request_id=77, name="Foreign [cccc]",
        )

        result = self._replace(foreign.download_log_id)

        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_ADOPT_CANDIDATE_INVALID)
        assert result.error_message is not None
        self.assertIn("belongs to request 77", result.error_message)
        self._assert_nothing_superseded()
        self.assertTrue(os.path.isdir(foreign.path))

    def test_missing_candidate_is_refused(self):
        result = self._replace(999_999)

        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_ADOPT_CANDIDATE_INVALID)
        self._assert_nothing_superseded()

    def test_candidate_without_a_folder_is_refused(self):
        candidate = self._seed("Sibling Pressing [aaaa]")
        self.assertTrue(self.db.clear_wrong_match_path(candidate.download_log_id))

        result = self._replace(candidate.download_log_id)

        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_ADOPT_CANDIDATE_INVALID)
        self._assert_nothing_superseded()

    def test_non_rejected_row_is_refused(self):
        log_id = self.db.log_download(
            42, outcome="success",
            validation_result={"failed_path": "/x"},
        )

        result = self._replace(log_id)

        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_ADOPT_CANDIDATE_INVALID)
        self._assert_nothing_superseded()

    def test_move_failure_keeps_the_whole_group_and_queues_nothing(self):
        adopted = self._seed("Sibling Pressing [aaaa]")
        other = self._seed("Unrelated Junk [bbbb]")

        with patch.object(
            self.db, "reassign_wrong_match_source",
            side_effect=RuntimeError("db down"),
        ):
            result = self._replace(adopted.download_log_id)

        self.assertEqual(result.outcome, RESULT_REPLACED)
        self.assertIsNone(result.adopt_import_job_id)
        self.assertTrue(any("db down" in w for w in result.warnings))
        self.assertTrue(any(
            "wrong-matches cleanup skipped" in w for w in result.warnings))
        self.assertTrue(os.path.isdir(adopted.path))
        self.assertTrue(os.path.isdir(other.path))
        self.assertEqual(self.db.list_import_jobs(), [])

    def test_unauthorized_folder_warns_instead_of_queueing(self):
        """The candidate moves (its folder is kept) but force-import's own
        quarantine authority refuses the path: the warning says so."""
        outside = self.enterContext(tempfile.TemporaryDirectory())
        adopted = seed_visible_wrong_match(
            self.db, outside, request_id=42, name="Elsewhere [dddd]",
        )

        result = self._replace(adopted.download_log_id)

        self.assertEqual(result.outcome, RESULT_REPLACED)
        self.assertIsNone(result.adopt_import_job_id)
        self.assertTrue(any(
            "not queued: unauthorized_path" in w for w in result.warnings),
            result.warnings)
        self.assertTrue(os.path.isdir(adopted.path))
        entry = self.db.get_download_log_entry(adopted.download_log_id)
        assert entry is not None
        self.assertEqual(entry["request_id"], result.new_request_id)


class TestReassignWrongMatchSourceFake(unittest.TestCase):
    def test_moves_only_rejected_rows_of_the_source_at_that_path(self):
        db = FakePipelineDB()
        keep_other_path = db.log_download(
            1, outcome="rejected", validation_result={"failed_path": "/b"})
        move = db.log_download(
            1, outcome="rejected", validation_result={"failed_path": "/a"})
        keep_other_request = db.log_download(
            2, outcome="rejected", validation_result={"failed_path": "/a"})
        keep_success = db.log_download(
            1, outcome="success", validation_result={"failed_path": "/a"})

        moved = db.reassign_wrong_match_source(
            "/a", from_request_id=1, to_request_id=9)

        self.assertEqual(moved, 1)
        owners = {
            log_id: (db.get_download_log_entry(log_id) or {}).get("request_id")
            for log_id in (keep_other_path, move, keep_other_request,
                           keep_success)
        }
        self.assertEqual(owners, {
            keep_other_path: 1, move: 9, keep_other_request: 2,
            keep_success: 1,
        })


if __name__ == "__main__":
    unittest.main()
