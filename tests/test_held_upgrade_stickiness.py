"""A held upgrade stays held after a failed force import (issue #1445 item 3).

``get_wrong_matches`` keeps the NEWEST row per folder. A force import of a
held row that the manifest guard refuses writes a newer row naming the same
folder under another scenario. The folder must still be a held upgrade —
untouched by delete-group and the triage batch, still rendered with its
stored comparison — and the card must say truthfully that the last attempt
failed and why. (Converge is pinned in ``tests/web/test_routes_imports.py``.)
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest

from lib.dispatch.manifest_guard import _guard_reject
from lib.dispatch.types import ImportAttemptResult
from lib.wrong_match_cleanup_service import cleanup_all_wrong_matches
from lib.wrong_match_delete_service import delete_wrong_match_group
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_held_upgrade_view import fairways_validation_result
from web.wrong_match_queue_view import build_held_upgrades


def _cfg() -> object:
    from tests.test_wrong_match_cleanup_service import _cfg as cleanup_cfg

    return cleanup_cfg()


class TestHeldUpgradeSurvivesAFailedForceImport(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, True)
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=42, status="wanted",
            mb_release_id="4c6f4a7b-c093-4006-a50e-3d6dfbdd2645",
        ))
        self.folder = os.path.join(self.root, "wrong_matches", "The Fairways")
        os.makedirs(self.folder)
        with open(os.path.join(self.folder, "02.flac"), "wb") as fh:
            fh.write(b"audio")
        payload = json.loads(fairways_validation_result())
        payload["failed_path"] = self.folder
        self.held_id = self.db.log_download(
            42, outcome="rejected", soulseek_username="natetheneet",
            validation_result=payload,
        )
        # The operator chose "Use new copy"; the force-import manifest guard
        # refused the folder and wrote its own truthful row.
        _guard_reject(
            self.db,
            request_id=42,
            failed_path=self.folder,
            source_username="natetheneet",
            attempt_result=ImportAttemptResult(None),
            detail="Import folder has audio the selected manifest does not",
            scenario="untracked_audio",
        )
        self.failed_id = self.db.download_logs[-1].id

    def test_the_newer_failure_row_is_recorded_truthfully(self) -> None:
        (row,) = self.db.get_wrong_matches()
        self.assertEqual(row["download_log_id"], self.failed_id)
        vr = row["validation_result"]
        assert vr is not None
        self.assertEqual(vr["scenario"], "untracked_audio")
        self.assertEqual(row["held_download_log_id"], self.held_id)

    def test_delete_group_still_skips_the_folder(self) -> None:
        summary = delete_wrong_match_group(self.db, 42)
        self.assertTrue(summary.success)
        self.assertEqual(summary.processed, 0)
        self.assertTrue(os.path.isdir(self.folder))

    def test_triage_batch_still_refuses_the_folder(self) -> None:
        summary = cleanup_all_wrong_matches(
            self.db, confirm_all_wrong_matches=True, cfg=_cfg(),
        )
        self.assertEqual(summary.skipped_held_upgrade, 1)
        self.assertTrue(os.path.isdir(self.folder))

    def test_card_keeps_the_comparison_and_says_the_last_try_failed(self) -> None:
        (card,) = build_held_upgrades(db=self.db)
        self.assertEqual(card["held_download_log_id"], self.held_id)
        self.assertEqual(card["download_log_id"], self.failed_id)
        self.assertIn("odd one out", str(card["sentence"]))
        self.assertEqual(
            card["last_attempt_failure"],
            "The last try to use the new copy failed: Import folder has "
            "audio the selected manifest does not",
        )

    def test_operator_delete_ends_the_hold(self) -> None:
        from lib.wrong_match_delete_service import delete_wrong_match

        result = delete_wrong_match(self.db, self.failed_id, require_visible=True)
        self.assertTrue(result.success)
        self.assertFalse(os.path.exists(self.folder))
        self.assertEqual(build_held_upgrades(db=self.db), [])


if __name__ == "__main__":
    unittest.main()
