"""Tests for the shared download recovery seam."""

import unittest

from lib.download_recovery import (
    find_blocked_processing_path_issues,
    find_blocked_recovery_issues,
    classify_processing_path,
    resolve_missing_current_path,
)
from lib.processing_paths import canonical_processing_path


class TestClassifyProcessingPath(unittest.TestCase):

    def test_classifies_canonical_path(self):
        location = classify_processing_path(
            current_path="/tmp/downloads/Test Artist - Test Album (2020)",
            artist="Test Artist",
            title="Test Album",
            year="2020",
            request_id=1,
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
        )

        self.assertEqual(location.kind, "canonical")
        self.assertEqual(location.display_name, "canonical processing path")

    def test_classifies_request_scoped_auto_import_stage(self):
        location = classify_processing_path(
            current_path="/tmp/staging/auto-import/Test Artist/Test Album [request-1]",
            artist="Test Artist",
            title="Test Album",
            year="2020",
            request_id=1,
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
        )

        self.assertEqual(location.kind, "request_scoped_auto_import_staged")
        self.assertTrue(location.blocks_post_move_retry)
        self.assertTrue(location.blocks_auto_import_dispatch)

    def test_classifies_request_scoped_post_validation_stage(self):
        location = classify_processing_path(
            current_path="/tmp/staging/post-validation/Test Artist/Test Album [request-1]",
            artist="Test Artist",
            title="Test Album",
            year="2020",
            request_id=1,
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
        )

        self.assertEqual(location.kind, "request_scoped_post_validation_staged")
        self.assertFalse(location.blocks_post_move_retry)
        self.assertFalse(location.blocks_auto_import_dispatch)

    def test_classifies_legacy_shared_stage(self):
        location = classify_processing_path(
            current_path="/tmp/staging/Test Artist/Test Album",
            artist="Test Artist",
            title="Test Album",
            year="2020",
            request_id=1,
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
        )

        self.assertEqual(location.kind, "legacy_shared_staged")
        self.assertTrue(location.blocks_auto_import_dispatch)

    def test_does_not_treat_other_request_stage_as_current_request(self):
        location = classify_processing_path(
            current_path="/tmp/staging/auto-import/Test Artist/Test Album [request-99]",
            artist="Test Artist",
            title="Test Album",
            year="2020",
            request_id=1,
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
        )

        self.assertEqual(location.kind, "external")


class TestResolveMissingCurrentPath(unittest.TestCase):

    def test_falls_back_to_canonical_when_no_candidates_exist(self):
        decision = resolve_missing_current_path(
            artist="Test Artist",
            title="Test Album",
            year="2020",
            request_id=1,
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
            has_entries=lambda _path: False,
        )

        assert decision.selected_location is not None
        self.assertEqual(decision.blocked_reason, None)
        self.assertEqual(decision.selected_location.kind, "canonical")
        self.assertEqual(
            decision.selected_location.path,
            canonical_processing_path(
                artist="Test Artist",
                title="Test Album",
                year="2020",
                slskd_download_dir="/tmp/downloads",
            ),
        )

    def test_picks_request_scoped_stage_when_it_is_the_only_populated_candidate(self):
        candidate = "/tmp/staging/auto-import/Test Artist/Test Album [request-1]"
        decision = resolve_missing_current_path(
            artist="Test Artist",
            title="Test Album",
            year="2020",
            request_id=1,
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
            has_entries=lambda path: path == candidate,
        )

        assert decision.selected_location is not None
        self.assertEqual(decision.blocked_reason, None)
        self.assertEqual(
            decision.selected_location.kind,
            "request_scoped_auto_import_staged",
        )
        self.assertEqual(decision.selected_location.path, candidate)

    def test_blocks_legacy_shared_only_recovery(self):
        candidate = "/tmp/staging/Test Artist/Test Album"
        decision = resolve_missing_current_path(
            artist="Test Artist",
            title="Test Album",
            year="2020",
            request_id=1,
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
            has_entries=lambda path: path == candidate,
        )

        self.assertEqual(decision.blocked_reason, "legacy_shared_only")
        self.assertIsNone(decision.selected_location)

    def test_blocks_multiple_populated_candidates(self):
        candidates = {
            "/tmp/downloads/Test Artist - Test Album (2020)",
            "/tmp/staging/Test Artist/Test Album",
        }
        decision = resolve_missing_current_path(
            artist="Test Artist",
            title="Test Album",
            year="2020",
            request_id=1,
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
            has_entries=lambda path: path in candidates,
        )

        self.assertEqual(decision.blocked_reason, "multiple_populated_paths")
        self.assertIsNone(decision.selected_location)
        self.assertEqual(
            [location.short_label for location in decision.populated_locations],
            ["canonical", "legacy-shared"],
        )


class TestFindBlockedRecoveryIssues(unittest.TestCase):

    def test_reports_legacy_shared_only_recovery_as_blocked(self):
        issues = find_blocked_recovery_issues(
            [{
                "id": 1,
                "status": "downloading",
                "artist_name": "Test Artist",
                "album_title": "Test Album",
                "year": 2020,
                "active_download_state": {
                    "filetype": "flac",
                    "processing_started_at": "2026-04-22T00:00:00+00:00",
                    "files": [{
                        "username": "user1",
                        "filename": "track.flac",
                    }],
                },
            }],
            set(),
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
            has_entries=lambda path: path == "/tmp/staging/Test Artist/Test Album",
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].request_id, 1)
        self.assertIn("ambiguous legacy shared staged path", issues[0].detail)

    def test_skips_rows_with_recoverable_request_scoped_stage(self):
        issues = find_blocked_recovery_issues(
            [{
                "id": 1,
                "status": "downloading",
                "artist_name": "Test Artist",
                "album_title": "Test Album",
                "year": 2020,
                "active_download_state": {
                    "filetype": "flac",
                    "processing_started_at": "2026-04-22T00:00:00+00:00",
                    "files": [{
                        "username": "user1",
                        "filename": "track.flac",
                    }],
                },
            }],
            set(),
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
            has_entries=lambda path: (
                path
                == "/tmp/staging/auto-import/Test Artist/Test Album [request-1]"
            ),
        )

        self.assertEqual(issues, [])


class TestFindBlockedProcessingPathIssues(unittest.TestCase):

    def test_reports_legacy_shared_current_path_as_blocked(self):
        issues = find_blocked_processing_path_issues(
            [{
                "id": 1,
                "status": "downloading",
                "artist_name": "Test Artist",
                "album_title": "Test Album",
                "year": 2020,
                "active_download_state": {
                    "filetype": "flac",
                    "processing_started_at": "2026-04-22T00:00:00+00:00",
                    "current_path": "/tmp/staging/Test Artist/Test Album",
                    "files": [{
                        "username": "user1",
                        "filename": "track.flac",
                    }],
                },
            }],
            set(),
            existing_local_paths={"/tmp/staging/Test Artist/Test Album"},
            staging_dir="/tmp/staging",
            slskd_download_dir="/tmp/downloads",
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].request_id, 1)
        self.assertIn("legacy shared staged path", issues[0].detail)
