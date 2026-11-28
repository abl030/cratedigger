"""Replace ``--adopt --after-import``: import as the sister release first,
remove the old copy only once that import has landed.

The operator flow this serves: a Wrong Matches candidate is a sibling
pressing of the request, and the operator does not care which pressing they
hold as long as it is a good copy. Plain Replace removes the installed album
up front, so a failed import of the adopted candidate leaves nothing. With
``displace_after_import`` the installed album stays; its exact Beets id and
release id ride on the force-import job, and the importer removes that exact
album only after the job completes (see ``tests/test_force_import_displacement.py``).
"""

from __future__ import annotations

import os
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
)

from lib.beets_delete import BeetsDeleteRequest
from lib.config import CratediggerConfig
from lib.import_queue import ForceImportDisplacement, ForceImportPayload
from lib.mbid_replace_service import RESULT_REPLACED, RESULT_TARGET_INVALID
from lib.replace_status import REPLACE_REASON_DISPLACE_REQUIRES_ADOPT
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import seed_visible_wrong_match
from tests.test_mbid_replace_service import NEW_MBID, OLD_MBID, _ServiceCase


class TestReplaceDisplaceAfterImport(_ServiceCase):
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
        self.deletes: list[BeetsDeleteRequest] = []

    def _replace(
        self,
        *,
        adopt: int | None,
        beets: FakeBeetsDB,
        displace: bool = True,
    ):
        svc = self._make_service(
            self.db,
            cfg=self.cfg,
            beets_db_factory=lambda: beets,
            beets_delete_fn=self._record_delete,
        )
        return svc.replace_request_mbid(
            42,
            target_mb_release_id=NEW_MBID,
            adopt_download_log_id=adopt,
            displace_after_import=displace,
        )

    def _record_delete(self, request: BeetsDeleteRequest):
        self.deletes.append(request)
        return self._completed_delete(request)

    def _seed(self, name: str):
        return seed_visible_wrong_match(
            self.db, self.albums, request_id=42, name=name,
        )

    def test_installed_album_is_kept_and_rides_on_the_job(self):
        adopted = self._seed("Sister Pressing [aaaa]")
        beets = self._installed_beets(album_id=77)

        result = self._replace(adopt=adopted.download_log_id, beets=beets)

        self.assertEqual(result.outcome, RESULT_REPLACED, result)
        self.assertEqual(result.warnings, ())
        # Nothing left the library yet.
        self.assertEqual(self.deletes, [])
        self.assertEqual(result.displace_album_id, 77)

        [job] = self.db.list_import_jobs()
        self.assertEqual(job.id, result.adopt_import_job_id)
        assert isinstance(job.payload, ForceImportPayload)
        self.assertEqual(
            job.payload.displace,
            ForceImportDisplacement(
                album_id=77,
                expected_release_id=OLD_MBID,
                library_db_path=beets.library_db_path,
                library_root=beets.library_root,
            ),
        )

    def test_a_retried_force_import_keeps_the_displacement(self):
        """Review of #1481: the adopted import fails, the operator retries
        it with the ordinary Force Import button. That retry must still
        remove the old copy once it lands, or the old album is stranded with
        no request and no warning."""
        from lib.force_import_service import (
            RESULT_QUEUED,
            enqueue_force_import,
        )

        adopted = self._seed("Sister Pressing [aaaa]")
        beets = self._installed_beets(album_id=77)
        result = self._replace(adopt=adopted.download_log_id, beets=beets)
        first = self.db.get_import_job(result.adopt_import_job_id or 0)
        assert first is not None
        self.db.mark_import_job_failed(
            first.id, error="beets refused", result={"success": False},
            message="beets refused",
        )

        retry = enqueue_force_import(self.db, self.cfg, adopted.download_log_id)

        self.assertEqual(retry.outcome, RESULT_QUEUED, retry)
        assert retry.job is not None
        self.assertNotEqual(retry.job.id, first.id)
        assert isinstance(retry.job.payload, ForceImportPayload)
        assert isinstance(first.payload, ForceImportPayload)
        self.assertEqual(retry.job.payload.displace, first.payload.displace)
        self.assertEqual(self.deletes, [])

    def test_an_unrelated_force_import_carries_nothing(self):
        other = self._seed("Unrelated [cccc]")
        from lib.force_import_service import enqueue_force_import

        queued = enqueue_force_import(self.db, self.cfg, other.download_log_id)

        assert queued.job is not None
        assert isinstance(queued.job.payload, ForceImportPayload)
        self.assertIsNone(queued.job.payload.displace)

    def test_plain_adopt_still_removes_up_front(self):
        """The default is unchanged: Replace removes the old album now."""
        adopted = self._seed("Sister Pressing [aaaa]")
        beets = self._installed_beets(album_id=77)

        result = self._replace(
            adopt=adopted.download_log_id, beets=beets, displace=False,
        )

        self.assertEqual(result.outcome, RESULT_REPLACED, result)
        self.assertEqual([r.album_id for r in self.deletes], [77])
        self.assertIsNone(result.displace_album_id)
        [job] = self.db.list_import_jobs()
        assert isinstance(job.payload, ForceImportPayload)
        self.assertIsNone(job.payload.displace)

    def test_nothing_installed_means_nothing_to_displace(self):
        adopted = self._seed("Sister Pressing [aaaa]")

        result = self._replace(
            adopt=adopted.download_log_id, beets=FakeBeetsDB(),
        )

        self.assertEqual(result.outcome, RESULT_REPLACED, result)
        self.assertEqual(result.warnings, ())
        self.assertEqual(self.deletes, [])
        self.assertIsNone(result.displace_album_id)
        [job] = self.db.list_import_jobs()
        assert isinstance(job.payload, ForceImportPayload)
        self.assertIsNone(job.payload.displace)

    def test_requires_a_candidate_to_adopt(self):
        """Without a candidate there is no import to wait for; refuse
        before anything changes."""
        beets = self._installed_beets(album_id=77)

        result = self._replace(adopt=None, beets=beets)

        self.assertEqual(result.outcome, RESULT_TARGET_INVALID, result)
        self.assertEqual(result.reason, REPLACE_REASON_DISPLACE_REQUIRES_ADOPT)
        old = self.db.get_request(42)
        assert old is not None
        self.assertEqual(old["status"], "wanted")
        self.assertIsNone(self.db.get_request_by_mb_release_id(NEW_MBID))
        self.assertEqual(self.deletes, [])


if __name__ == "__main__":
    import unittest

    unittest.main()
