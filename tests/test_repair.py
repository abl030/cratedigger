"""Tests for repair/orphan-recovery pure functions."""

import unittest

from lib.quality import (
    OrphanInfo,
    RepairAction,
    find_inconsistencies,
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

    def test_wanted_with_stale_imported_path(self):
        rows = [{"id": 2, "status": "wanted",
                 "active_download_state": None,
                 "imported_path": "/some/path"}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "stale_imported_path")

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
            {"id": 2, "status": "wanted", "active_download_state": None,
             "imported_path": "/stale"},
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


class TestSuggestRepair(unittest.TestCase):
    """Map issues to repair actions."""

    def test_corrupt_downloading(self):
        issue = OrphanInfo(request_id=1, issue_type="corrupt_downloading",
                           detail="no active_download_state")
        action = suggest_repair(issue)
        self.assertEqual(action.action, "reset_to_wanted")

    def test_stale_imported_path(self):
        issue = OrphanInfo(request_id=2, issue_type="stale_imported_path",
                           detail="wanted but has imported_path")
        action = suggest_repair(issue)
        self.assertEqual(action.action, "clear_imported_path")

    def test_unknown_issue_type(self):
        issue = OrphanInfo(request_id=3, issue_type="unknown",
                           detail="something")
        action = suggest_repair(issue)
        self.assertEqual(action.action, "manual_review")


if __name__ == "__main__":
    unittest.main()
