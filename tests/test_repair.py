"""Tests for repair/orphan-recovery pure functions."""

import unittest

from lib.quality import (
    OrphanInfo,
    find_inconsistencies,
    find_orphaned_downloads,
    suggest_repair,
)


class TestFindInconsistencies(unittest.TestCase):
    """Detect inconsistent pipeline DB rows."""

    def test_downloading_no_state(self):
        rows = [{"id": 1, "status": "downloading", "active_download_state": None,
                 "imported_path": None}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "corrupt_downloading")
        self.assertEqual(issues[0].request_id, 1)

    def test_downloading_with_state_is_fine(self):
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {"filetype": "flac"},
                 "imported_path": None}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0)

    def test_wanted_with_imported_path_is_fine(self):
        """Issue #93: transcode_upgrade / quality-gate upgrade flows
        mark_done (persisting imported_path to the real beets destination),
        then re-queue the row to ``wanted`` to search for something better.
        The files genuinely live at imported_path during that search, so
        flagging the row as stale would wipe correct data on the next
        ``repair.py fix``.
        """
        rows = [{"id": 2, "status": "wanted",
                 "active_download_state": None,
                 "imported_path": "/Beets/Artist/Album"}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0,
                         "wanted + imported_path is a valid upgrade-search state")

    def test_manual_with_imported_path_is_fine(self):
        """Same rationale as wanted: manual status after a force-import
        could legitimately carry imported_path until the row is cleared."""
        rows = [{"id": 4, "status": "manual",
                 "active_download_state": None,
                 "imported_path": "/Beets/Artist/Album"}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0)

    def test_imported_with_path_is_fine(self):
        rows = [{"id": 3, "status": "imported",
                 "active_download_state": None,
                 "imported_path": "/some/path"}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0)

    def test_multiple_issues(self):
        rows = [
            {"id": 1, "status": "downloading", "active_download_state": None,
             "imported_path": None},
            {"id": 2, "status": "downloading", "active_download_state": None,
             "imported_path": None},
        ]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 2)

    def test_clean_rows(self):
        rows = [
            {"id": 1, "status": "wanted", "active_download_state": None,
             "imported_path": None},
            {"id": 2, "status": "imported", "active_download_state": None,
             "imported_path": "/valid"},
        ]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0)


class TestFindOrphanedDownloads(unittest.TestCase):
    """Detect downloading rows whose slskd transfers no longer exist."""

    def test_orphaned_when_no_transfers_match(self):
        """All files missing from slskd → orphaned."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        active = set()  # no active transfers
        issues = find_orphaned_downloads(rows, active)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "orphaned_download")
        self.assertEqual(issues[0].request_id, 1)

    def test_not_orphaned_when_transfer_exists(self):
        """At least one file still active → not orphaned."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        active = {("user1", "track.flac")}
        issues = find_orphaned_downloads(rows, active)
        self.assertEqual(len(issues), 0)

    def test_skips_non_downloading_rows(self):
        """Only downloading rows should be checked."""
        rows = [{"id": 1, "status": "wanted",
                 "active_download_state": None}]
        issues = find_orphaned_downloads(rows, set())
        self.assertEqual(len(issues), 0)

    def test_skips_downloading_without_state(self):
        """corrupt_downloading (no state) handled by find_inconsistencies."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": None}]
        issues = find_orphaned_downloads(rows, set())
        self.assertEqual(len(issues), 0)

    def test_partial_match_not_orphaned(self):
        """Some files transferred, some still active → not orphaned."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "files": [
                         {"username": "user1", "filename": "01.flac"},
                         {"username": "user1", "filename": "02.flac"},
                     ]}}]
        active = {("user1", "02.flac")}  # only 1 of 2 still active
        issues = find_orphaned_downloads(rows, active)
        self.assertEqual(len(issues), 0)

    def test_skips_local_processing_rows_without_active_transfers(self):
        """Rows already in local processing are not orphaned downloads."""
        current_path = "/tmp/staging/auto-import/Test/Album [request-1]"
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "processing_started_at": "2026-04-22T00:00:00+00:00",
                     "current_path": current_path,
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        issues = find_orphaned_downloads(
            rows,
            set(),
            existing_local_paths={current_path},
        )
        self.assertEqual(len(issues), 0)

    def test_processing_started_without_current_path_is_still_orphaned(self):
        """Rows that never persisted a local path should still be repairable."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "processing_started_at": "2026-04-22T00:00:00+00:00",
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        issues = find_orphaned_downloads(rows, set())
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "orphaned_download")

    def test_reports_missing_local_processing_path_for_manual_review(self):
        """Blocked post-move rows should be surfaced to repair tooling."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "processing_started_at": "2026-04-22T00:00:00+00:00",
                     "current_path": "/tmp/staging/auto-import/Test/Album [request-1]",
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        issues = find_orphaned_downloads(rows, set(), existing_local_paths=set())
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "blocked_post_move")

    def test_suggest_repair_orphaned(self):
        """Orphaned download should suggest reset_to_wanted."""
        issue = OrphanInfo(request_id=1, issue_type="orphaned_download",
                           detail="transfers gone")
        action = suggest_repair(issue)
        self.assertEqual(action.action, "reset_to_wanted")

    def test_suggest_repair_blocked_post_move(self):
        issue = OrphanInfo(
            request_id=1,
            issue_type="blocked_post_move",
            detail="missing staged path",
        )
        action = suggest_repair(issue)
        self.assertEqual(action.action, "manual_review")


class TestSuggestRepair(unittest.TestCase):
    """Map issues to repair actions."""

    def test_corrupt_downloading(self):
        issue = OrphanInfo(request_id=1, issue_type="corrupt_downloading",
                           detail="no active_download_state")
        action = suggest_repair(issue)
        self.assertEqual(action.action, "reset_to_wanted")

    def test_unknown_issue_type(self):
        issue = OrphanInfo(request_id=3, issue_type="unknown",
                           detail="something")
        action = suggest_repair(issue)
        self.assertEqual(action.action, "manual_review")


if __name__ == "__main__":
    unittest.main()
