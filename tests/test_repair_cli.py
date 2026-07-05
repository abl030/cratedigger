"""Tests for scripts/repair.py CLI wiring."""

from __future__ import annotations

import io
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from typing import Any, cast

from lib.quality import OrphanInfo, SlskdOrphanTransfer
from scripts import repair
from tests.fakes import FakePipelineDB
from tests.helpers import make_download_directory, make_download_user, make_transfer_snapshot


class TestCmdFix(unittest.TestCase):
    @patch("scripts.repair.finalize_request")
    @patch("scripts.repair._collect_issues")
    def test_reset_to_wanted_routes_through_shared_finalizer(
        self,
        mock_collect_issues,
        mock_finalize,
    ) -> None:
        db = FakePipelineDB()
        mock_collect_issues.return_value = repair.CollectedIssues(
            issues=[
                OrphanInfo(
                    request_id=17,
                    issue_type="orphaned_download",
                    detail="transfers gone",
                )
            ],
            slskd_orphans=[],
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            repair.cmd_fix(cast(Any, db))

        called_db, request_id, transition = mock_finalize.call_args.args
        self.assertIs(called_db, db)
        self.assertEqual(request_id, 17)
        self.assertEqual(transition.target_status, "wanted")
        self.assertEqual(transition.from_status, "downloading")
        self.assertIn("Reset to wanted", stdout.getvalue())


class TestActiveTransferPairs(unittest.TestCase):
    """#507: _active_transfer_pairs walks the typed downloads envelope."""

    def test_flattens_nested_envelope_to_pairs(self) -> None:
        downloads = [
            make_download_user(username="peer1", directories=[
                make_download_directory(directory="Music\\Album", files=[
                    make_transfer_snapshot(filename="Music\\Album\\01.flac"),
                    make_transfer_snapshot(filename="Music\\Album\\02.flac"),
                ]),
            ]),
            make_download_user(username="peer2", directories=[
                make_download_directory(directory="Music\\Other", files=[
                    make_transfer_snapshot(filename="Music\\Other\\01.flac"),
                ]),
            ]),
        ]

        pairs = repair._active_transfer_pairs(downloads)

        self.assertEqual(pairs, {
            ("peer1", "Music\\Album\\01.flac"),
            ("peer1", "Music\\Album\\02.flac"),
            ("peer2", "Music\\Other\\01.flac"),
        })

    def test_file_without_filename_excluded(self) -> None:
        downloads = [
            make_download_user(username="peer1", directories=[
                make_download_directory(directory="d", files=[
                    make_transfer_snapshot(filename=""),
                ]),
            ]),
        ]

        self.assertEqual(repair._active_transfer_pairs(downloads), set())

    def test_empty_downloads_returns_empty_set(self) -> None:
        self.assertEqual(repair._active_transfer_pairs([]), set())


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
        db = FakePipelineDB()
        cur = MagicMock()
        cur.fetchone.return_value = {"held": True}
        db.queue_execute_results(cur)

        result = repair._auto_import_in_progress(cast(Any, db), 17, "test-mbid")

        self.assertTrue(result)
        self.assertEqual(len(db.execute_calls), 1)
        sql, params = db.execute_calls[0]
        self.assertIn("FROM pg_locks", sql)
        self.assertIn("objsubid = 2", sql)
        self.assertEqual(params[0], repair.ADVISORY_LOCK_NAMESPACE_RELEASE)

    def test_auto_import_in_progress_returns_false_when_no_lock_is_held(self) -> None:
        db = FakePipelineDB()
        cur = MagicMock()
        cur.fetchone.return_value = {"held": False}
        db.queue_execute_results(cur)

        result = repair._auto_import_in_progress(cast(Any, db), 17, "test-mbid")

        self.assertFalse(result)

    def test_auto_import_in_progress_returns_false_without_mbid(self) -> None:
        db = FakePipelineDB()

        result = repair._auto_import_in_progress(cast(Any, db), 17, None)

        self.assertFalse(result)
        self.assertEqual(db.execute_calls, [])

    def test_auto_import_in_progress_reports_probe_failure(self) -> None:
        db = FakePipelineDB()
        db.queue_execute_results(RuntimeError("db boom"))

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            result = repair._auto_import_in_progress(cast(Any, db), 17, "test-mbid")

        self.assertIsNone(result)
        self.assertIn("could not probe auto-import lock for request 17", stdout.getvalue())

    @patch("scripts.repair._fetch_slskd_downloads", return_value=[])
    @patch("scripts.repair.read_runtime_config", side_effect=RuntimeError("cfg boom"))
    @patch("scripts.repair._get_all_rows", return_value=[])
    def test_collect_issues_reports_runtime_config_failure_separately(
        self,
        _mock_get_rows,
        _mock_read_runtime_config,
        _mock_fetch_downloads,
    ) -> None:
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            collected = repair._collect_issues(
                MagicMock(),
                slskd_host="http://slskd",
                slskd_key="secret",
            )

        self.assertEqual(collected.issues, [])
        self.assertEqual(collected.slskd_orphans, [])
        self.assertIn(
            "could not load runtime config for local-path checks: cfg boom",
            stdout.getvalue(),
        )

    @patch("scripts.repair._fetch_slskd_downloads", return_value=[])
    @patch("scripts.repair.read_runtime_config", side_effect=RuntimeError("cfg boom"))
    @patch("scripts.repair._get_all_rows", return_value=[])
    def test_collect_issues_keeps_orphans_when_runtime_config_load_fails(
        self,
        _mock_get_rows,
        _mock_read_runtime_config,
        _mock_fetch_downloads,
    ) -> None:
        orphans = [
            OrphanInfo(
                request_id=17,
                issue_type="orphaned_download",
                detail="no active slskd transfers (users: user1)",
            ),
        ]

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            collected = repair._collect_issues(
                MagicMock(),
                slskd_host="http://slskd",
                slskd_key="secret",
                find_orphaned_fn=lambda rows, active, existing_local_paths=None: orphans,
            )

        self.assertEqual(len(collected.issues), 1)
        self.assertEqual(collected.issues[0].issue_type, "orphaned_download")
        self.assertIn(
            "could not load runtime config for local-path checks: cfg boom",
            stdout.getvalue(),
        )

    @patch(
        "scripts.repair.find_blocked_processing_path_issues",
        side_effect=PermissionError("perm boom"),
    )
    @patch("scripts.repair._fetch_slskd_downloads", return_value=[])
    @patch("scripts.repair.read_runtime_config")
    @patch("scripts.repair._get_all_rows", return_value=[])
    def test_collect_issues_keeps_orphans_when_local_path_probe_fails(
        self,
        _mock_get_rows,
        mock_read_runtime_config,
        _mock_fetch_downloads,
        _mock_find_blocked_processing,
    ) -> None:
        mock_read_runtime_config.return_value = SimpleNamespace(
            beets_staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
        )
        orphans = [
            OrphanInfo(
                request_id=17,
                issue_type="orphaned_download",
                detail="no active slskd transfers (users: user1)",
            )
        ]

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            collected = repair._collect_issues(
                MagicMock(),
                slskd_host="http://slskd",
                slskd_key="secret",
                find_orphaned_fn=lambda rows, active, existing_local_paths=None: orphans,
                find_blocked_recovery_fn=lambda *args, **kwargs: [],
            )

        self.assertEqual(len(collected.issues), 1)
        self.assertEqual(collected.issues[0].issue_type, "orphaned_download")
        self.assertIn(
            "could not inspect local recovery paths: perm boom",
            stdout.getvalue(),
        )


class TestCollectIssuesSlskdOrphanReport(unittest.TestCase):
    """#479 item 1: scan surfaces slskd-side orphans (read-only).

    ``find_slskd_orphans`` runs for real against the same raw snapshot
    ``_fetch_slskd_downloads`` already fetched for the forward
    (``orphaned_download``) check — no second network round-trip, and no
    ``fix`` action is derived from it (the #278 convergence in
    ``lib.slskd_transfers.converge_slskd_orphans`` is the only thing that
    ever cancels these).
    """

    RAW_SNAPSHOT = [
        make_download_user(username="peer1", directories=[
            make_download_directory(directory="Music\\Orphan", files=[
                make_transfer_snapshot(
                    id="t-orphan",
                    filename="Music\\Orphan\\01.flac",
                    state="InProgress",
                ),
            ]),
        ]),
    ]

    @patch("scripts.repair._fetch_slskd_downloads", return_value=RAW_SNAPSHOT)
    @patch("scripts.repair.read_runtime_config")
    @patch("scripts.repair._get_all_rows", return_value=[])
    def test_collect_issues_derives_slskd_orphans_from_the_same_snapshot(
        self,
        _mock_get_rows,
        mock_read_runtime_config,
        _mock_fetch_downloads,
    ) -> None:
        mock_read_runtime_config.return_value = SimpleNamespace(
            beets_staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
        )

        collected = repair._collect_issues(
            MagicMock(),
            slskd_host="http://slskd",
            slskd_key="secret",
            find_blocked_recovery_fn=lambda *args, **kwargs: [],
        )

        self.assertEqual(collected.issues, [])
        self.assertEqual(len(collected.slskd_orphans), 1)
        orphan = collected.slskd_orphans[0]
        self.assertEqual(orphan.username, "peer1")
        self.assertEqual(orphan.transfer_id, "t-orphan")
        self.assertEqual(orphan.filename, "Music\\Orphan\\01.flac")
        self.assertEqual(orphan.state, "InProgress")

    @patch("scripts.repair._fetch_slskd_downloads", return_value=[])
    @patch("scripts.repair._get_all_rows", return_value=[])
    def test_collect_issues_reports_no_slskd_orphans_without_slskd_args(
        self,
        _mock_get_rows,
        _mock_fetch_downloads,
    ) -> None:
        collected = repair._collect_issues(MagicMock(), None, None)

        self.assertEqual(collected.slskd_orphans, [])
        _mock_fetch_downloads.assert_not_called()


class TestCmdScanSlskdOrphanReport(unittest.TestCase):
    """#479 item 1: cmd_scan prints the slskd orphan report read-only."""

    @patch("scripts.repair._collect_issues")
    def test_scan_prints_slskd_orphans_without_touching_actionable_issues(
        self, mock_collect,
    ) -> None:
        mock_collect.return_value = repair.CollectedIssues(
            issues=[],
            slskd_orphans=[
                SlskdOrphanTransfer(
                    username="peer1",
                    transfer_id="t-orphan",
                    filename="Music\\Orphan\\01.flac",
                    state="InProgress",
                ),
            ],
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            issues = repair.cmd_scan(cast(Any, FakePipelineDB()))

        output = stdout.getvalue()
        self.assertEqual(issues, [])
        self.assertIn("peer1", output)
        self.assertIn("t-orphan", output)
        self.assertIn("read-only", output.lower())

    @patch("scripts.repair._collect_issues")
    def test_scan_slskd_orphan_report_does_not_affect_returned_issues(
        self, mock_collect,
    ) -> None:
        mock_collect.return_value = repair.CollectedIssues(
            issues=[
                OrphanInfo(
                    request_id=17,
                    issue_type="orphaned_download",
                    detail="transfers gone",
                ),
            ],
            slskd_orphans=[
                SlskdOrphanTransfer(
                    username="peer1",
                    transfer_id="t-orphan",
                    filename="Music\\Orphan\\01.flac",
                    state="InProgress",
                ),
            ],
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            issues = repair.cmd_scan(cast(Any, FakePipelineDB()))

        # The read-only slskd-orphan report is additive — it never changes
        # the actionable issue list cmd_fix would later consume.
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].request_id, 17)


class TestDefaultDsnFailsLoud(unittest.TestCase):
    """#479 item 2: no hardcoded fallback — fail loud instead."""

    @patch.object(repair, "DEFAULT_DSN", None)
    def test_main_fails_loud_when_dsn_is_not_configured(self) -> None:
        with patch.object(sys, "argv", ["repair.py", "scan"]):
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as cm:
                    repair.main()

        self.assertEqual(cm.exception.code, 2)
        self.assertIn("PIPELINE_DB_DSN", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
