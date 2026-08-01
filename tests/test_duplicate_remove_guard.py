"""Focused tests for guarded Beets duplicate replacement."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace

from lib.duplicate_remove_guard import quarantine_duplicate_remove_guard_source
from lib.quality import DuplicateRemoveCandidate


class TestDuplicateCandidateSerialization(unittest.TestCase):
    def test_serializes_beets_duplicate_album_shape(self):
        # beets isn't installed in the test shell; the config guard tests install
        # compatible module fakes before importing harness.beets_harness.
        import tests.test_harness_config_guard  # noqa: F401
        from harness import beets_harness

        item = SimpleNamespace(path=b"/Beets/Artist/Album/01 Track.mp3")
        album = SimpleNamespace(
            id=42,
            mb_albumid="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
            discogs_albumid=12856590,
            albumartist="Artist",
            album="Album",
            items=lambda: [item],
        )

        payload = beets_harness._serialize_duplicate_album(album)

        self.assertEqual(payload["beets_album_id"], 42)
        self.assertEqual(payload["mb_albumid"],
                         "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb")
        self.assertEqual(payload["discogs_albumid"], "12856590")
        self.assertEqual(payload["album_path"], "/Beets/Artist/Album")
        self.assertEqual(payload["item_count"], 1)
        self.assertEqual(payload["albumartist"], "Artist")
        self.assertEqual(payload["album"], "Album")


class TestDuplicateRemoveGuardEvaluation(unittest.TestCase):
    def test_allows_exact_musicbrainz_duplicate(self):
        from harness import import_one

        failure = import_one._duplicate_remove_guard_failure(
            target_release_id="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
            candidates=[
                DuplicateRemoveCandidate(
                    beets_album_id=42,
                    mb_albumid="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
                ),
            ],
        )

        self.assertIsNone(failure)

    def test_allows_exact_discogs_duplicate(self):
        from harness import import_one

        failure = import_one._duplicate_remove_guard_failure(
            target_release_id="12856590",
            candidates=[
                DuplicateRemoveCandidate(
                    beets_album_id=42,
                    discogs_albumid="12856590",
                ),
            ],
        )

        self.assertIsNone(failure)

    def test_fails_multiple_duplicates(self):
        from harness import import_one

        failure = import_one._duplicate_remove_guard_failure(
            target_release_id="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
            candidates=[
                DuplicateRemoveCandidate(beets_album_id=42),
                DuplicateRemoveCandidate(beets_album_id=43),
            ],
        )

        assert failure is not None
        self.assertEqual(failure.reason, "duplicate_count_not_one")
        self.assertEqual(failure.duplicate_count, 2)

    def test_fails_mismatched_duplicate(self):
        from harness import import_one

        failure = import_one._duplicate_remove_guard_failure(
            target_release_id="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
            candidates=[
                DuplicateRemoveCandidate(
                    beets_album_id=42,
                    mb_albumid="cccccccc-4444-5555-6666-dddddddddddd",
                ),
            ],
        )

        assert failure is not None
        self.assertEqual(failure.reason, "release_identity_mismatch")


class TestDuplicateRemoveGuardQuarantine(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_moves_source_to_separate_incoming_quarantine(self):
        source = os.path.join(self.tmpdir, "auto-import", "Artist", "Album")
        os.makedirs(source)
        with open(os.path.join(source, "track.mp3"), "w", encoding="utf-8") as f:
            f.write("x")

        result = quarantine_duplicate_remove_guard_source(
            source_path=source,
            staging_dir=self.tmpdir,
            request_id=42,
        )

        self.assertTrue(result.success)
        self.assertFalse(os.path.exists(source))
        assert result.quarantine_path is not None
        self.assertIn("duplicate-remove-guard", result.quarantine_path)
        self.assertTrue(os.path.exists(os.path.join(
            result.quarantine_path, "track.mp3")))

    def test_collision_preserves_both_quarantined_candidates(self):
        existing = os.path.join(
            self.tmpdir,
            "duplicate-remove-guard",
            "request-42 - Album",
        )
        os.makedirs(existing)
        source = os.path.join(self.tmpdir, "auto-import", "Artist", "Album")
        os.makedirs(source)

        result = quarantine_duplicate_remove_guard_source(
            source_path=source,
            staging_dir=self.tmpdir,
            request_id=42,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.quarantine_path, f"{existing}-2")
        self.assertTrue(os.path.isdir(existing))
        self.assertTrue(os.path.isdir(f"{existing}-2"))


if __name__ == "__main__":
    unittest.main()
