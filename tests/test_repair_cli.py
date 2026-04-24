"""Tests for scripts/repair.py CLI wiring."""

from __future__ import annotations

import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.quality import OrphanInfo
from scripts import repair


class TestCmdFix(unittest.TestCase):
    @patch("lib.transitions.finalize_request")
    @patch("scripts.repair._collect_issues")
    def test_reset_to_wanted_routes_through_shared_finalizer(
        self,
        mock_collect_issues,
        mock_finalize,
    ) -> None:
        db = MagicMock()
        mock_collect_issues.return_value = [
            OrphanInfo(
                request_id=17,
                issue_type="orphaned_download",
                detail="transfers gone",
            )
        ]

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            repair.cmd_fix(db)

        called_db, request_id, transition = mock_finalize.call_args.args
        self.assertIs(called_db, db)
        self.assertEqual(request_id, 17)
        self.assertEqual(transition.target_status, "wanted")
        self.assertEqual(transition.from_status, "downloading")
        self.assertIn("Reset to wanted", stdout.getvalue())


class TestCollectIssues(unittest.TestCase):
    def test_dedupe_issues_drops_exact_duplicates(self) -> None:
        detail = "persisted processing path missing after local processing: /tmp/path"
        issues = repair._dedupe_issues(
            [
                OrphanInfo(
                    request_id=17,
                    issue_type="blocked_post_move",
                    detail=detail,
                ),
                OrphanInfo(
                    request_id=17,
                    issue_type="blocked_post_move",
                    detail=detail,
                ),
                OrphanInfo(
                    request_id=17,
                    issue_type="blocked_post_move",
                    detail="different detail",
                ),
            ]
        )

        self.assertEqual(len(issues), 2)
        self.assertEqual(issues[0].detail, detail)
        self.assertEqual(issues[1].detail, "different detail")

    def test_auto_import_in_progress_returns_true_when_lock_is_held(self) -> None:
        db = MagicMock()
        db._execute.return_value.fetchone.return_value = {"held": True}

        result = repair._auto_import_in_progress(db, 17, "test-mbid")

        self.assertTrue(result)
        db._execute.assert_called_once()
        sql, params = db._execute.call_args[0]
        self.assertIn("FROM pg_locks", sql)
        self.assertIn("objsubid = 2", sql)
        self.assertEqual(params[0], repair.ADVISORY_LOCK_NAMESPACE_RELEASE)

    def test_auto_import_in_progress_returns_false_when_no_lock_is_held(self) -> None:
        db = MagicMock()
        db._execute.return_value.fetchone.return_value = {"held": False}

        result = repair._auto_import_in_progress(db, 17, "test-mbid")

        self.assertFalse(result)

    def test_auto_import_in_progress_returns_false_without_mbid(self) -> None:
        db = MagicMock()

        result = repair._auto_import_in_progress(db, 17, None)

        self.assertFalse(result)
        db._execute.assert_not_called()

    def test_auto_import_in_progress_reports_probe_failure(self) -> None:
        db = MagicMock()
        db._execute.side_effect = RuntimeError("db boom")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            result = repair._auto_import_in_progress(db, 17, "test-mbid")

        self.assertIsNone(result)
        self.assertIn("could not probe auto-import lock for request 17", stdout.getvalue())

    @patch("scripts.repair._get_slskd_active_transfers", return_value=set())
    @patch("scripts.repair.read_runtime_config", side_effect=RuntimeError("cfg boom"))
    @patch("scripts.repair._get_all_rows", return_value=[])
    def test_collect_issues_reports_runtime_config_failure_separately(
        self,
        _mock_get_rows,
        _mock_read_runtime_config,
        _mock_active_transfers,
    ) -> None:
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            issues = repair._collect_issues(
                MagicMock(),
                slskd_host="http://slskd",
                slskd_key="secret",
            )

        self.assertEqual(issues, [])
        self.assertIn(
            "could not load runtime config for local-path checks: cfg boom",
            stdout.getvalue(),
        )

    @patch("scripts.repair.find_orphaned_downloads")
    @patch("scripts.repair._get_slskd_active_transfers", return_value=set())
    @patch("scripts.repair.read_runtime_config", side_effect=RuntimeError("cfg boom"))
    @patch("scripts.repair._get_all_rows", return_value=[])
    def test_collect_issues_keeps_orphans_when_runtime_config_load_fails(
        self,
        _mock_get_rows,
        _mock_read_runtime_config,
        _mock_active_transfers,
        mock_find_orphaned,
    ) -> None:
        mock_find_orphaned.return_value = [
            OrphanInfo(
                request_id=17,
                issue_type="orphaned_download",
                detail="no active slskd transfers (users: user1)",
            ),
        ]

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            issues = repair._collect_issues(
                MagicMock(),
                slskd_host="http://slskd",
                slskd_key="secret",
            )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "orphaned_download")
        self.assertIn(
            "could not load runtime config for local-path checks: cfg boom",
            stdout.getvalue(),
        )

    @patch("scripts.repair.find_blocked_recovery_issues", return_value=[])
    @patch(
        "scripts.repair.find_blocked_processing_path_issues",
        side_effect=PermissionError("perm boom"),
    )
    @patch("scripts.repair.find_orphaned_downloads")
    @patch("scripts.repair._get_slskd_active_transfers", return_value=set())
    @patch("scripts.repair.read_runtime_config")
    @patch("scripts.repair._get_all_rows", return_value=[])
    def test_collect_issues_keeps_orphans_when_local_path_probe_fails(
        self,
        _mock_get_rows,
        mock_read_runtime_config,
        _mock_active_transfers,
        mock_find_orphaned,
        _mock_find_blocked_processing,
        _mock_find_blocked_recovery,
    ) -> None:
        mock_read_runtime_config.return_value = SimpleNamespace(
            beets_staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
        )
        mock_find_orphaned.return_value = [
            OrphanInfo(
                request_id=17,
                issue_type="orphaned_download",
                detail="no active slskd transfers (users: user1)",
            )
        ]

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            issues = repair._collect_issues(
                MagicMock(),
                slskd_host="http://slskd",
                slskd_key="secret",
            )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "orphaned_download")
        self.assertIn(
            "could not inspect local recovery paths: perm boom",
            stdout.getvalue(),
        )


if __name__ == "__main__":
    unittest.main()
