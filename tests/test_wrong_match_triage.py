"""Tests for preview-driven Wrong Matches triage."""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from lib.import_preview import ImportPreviewResult
from lib.wrong_match_triage import triage_wrong_match
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestWrongMatchTriage(unittest.TestCase):
    def _db_with_wrong_match(self, source: str) -> tuple[FakePipelineDB, int]:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            status="manual",
            mb_release_id="mbid-1",
        ))
        db.log_download(
            1,
            outcome="rejected",
            validation_result={
                "scenario": "wrong_match",
                "failed_path": source,
            },
        )
        return db, db.download_logs[-1].id

    def test_confident_cleanup_eligible_reject_deletes_and_clears(self):
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db, log_id = self._db_with_wrong_match(source)

            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="confident_reject",
                    confident_reject=True,
                    cleanup_eligible=True,
                    decision="downgrade",
                    reason="downgrade",
                    source_path=source,
                ),
            ):
                result = triage_wrong_match(db, log_id)

            self.assertEqual(result.action, "deleted_reject")
            self.assertTrue(result.success)
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            audit = vr["wrong_match_triage"]
            assert isinstance(audit, dict)
            self.assertEqual(audit["action"], "deleted_reject")
            self.assertEqual(audit["preview_decision"], "downgrade")
            self.assertEqual(audit["reason"], "downgrade")
            self.assertNotIn("failed_path", vr)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_would_import_leaves_candidate_visible(self):
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._db_with_wrong_match(source)

            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="would_import",
                    would_import=True,
                    decision="import",
                    reason="import",
                    source_path=source,
                ),
            ):
                result = triage_wrong_match(db, log_id)

            self.assertEqual(result.action, "kept_would_import")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            audit = vr["wrong_match_triage"]
            assert isinstance(audit, dict)
            self.assertEqual(audit["action"], "kept_would_import")
            self.assertEqual(audit["preview_verdict"], "would_import")
            self.assertEqual(audit["reason"], "import")
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_missing_path_clears_stale_pointer_without_successful_reject(self):
        source = tempfile.mkdtemp()
        shutil.rmtree(source)
        db, log_id = self._db_with_wrong_match(source)

        with patch(
            "lib.wrong_match_triage.preview_import_from_download_log",
            return_value=ImportPreviewResult(
                mode="download_log",
                verdict="uncertain",
                uncertain=True,
                decision="path_missing",
                reason="path_missing",
                source_path=source,
            ),
        ):
            result = triage_wrong_match(db, log_id)

        self.assertEqual(result.action, "stale_path_cleared")
        self.assertTrue(result.success)
        self.assertEqual(db.get_wrong_matches(), [])
        entry = db.get_download_log_entry(log_id)
        assert entry is not None
        vr = entry["validation_result"]
        assert isinstance(vr, dict)
        audit = vr["wrong_match_triage"]
        assert isinstance(audit, dict)
        self.assertEqual(audit["action"], "stale_path_cleared")
        self.assertEqual(audit["reason"], "path_missing")
        self.assertNotIn("failed_path", vr)


if __name__ == "__main__":
    unittest.main()
