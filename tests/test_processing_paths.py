"""Tests for ``lib/processing_paths.py``."""

import os
import tempfile
import unittest

from lib.grab_list import DownloadFile, GrabListEntry
from lib.processing_paths import (
    attempt_fingerprint,
    attempt_fingerprint_of_files,
    attempt_fingerprint_or_none,
    canonical_folder_for_row,
    canonical_processing_path,
    processing_albums_dir,
    protected_staging_roots,
    stage_to_ai_path,
    stage_to_ai_root,
)


def _row(*, files: list[DownloadFile]) -> GrabListEntry:
    return GrabListEntry(
        album_id=1,
        files=files,
        filetype="flac",
        title="Test Album",
        artist="Test Artist",
        year="2020",
        mb_release_id="release-id",
    )


class TestAttemptFingerprintOfFiles(unittest.TestCase):
    """Issue #1278: one projection from files to an attempt fingerprint."""

    def test_projects_exactly_the_identity_pair(self):
        files = [
            DownloadFile(
                filename="Music/01.flac", id="t1", file_dir="Music",
                username="user1", size=111),
            DownloadFile(
                filename="Music/02.flac", id="t2", file_dir="Music",
                username="user2", size=222),
        ]

        self.assertEqual(
            attempt_fingerprint_of_files(files),
            attempt_fingerprint([
                ("user1", "Music/01.flac"),
                ("user2", "Music/02.flac"),
            ]),
        )

    def test_non_identity_fields_never_reach_the_digest(self):
        """slskd re-issues transfer IDs and revises sizes/retries while
        one durable queue key is still in flight, so widening the
        projection to any of them would make an attempt's fingerprint
        change under it — stranding its own canonical folder."""
        before = [DownloadFile(
            filename="Music/01.flac", id="t1", file_dir="Music",
            username="user1", size=111)]
        after = [DownloadFile(
            filename="Music/01.flac", id="REISSUED", file_dir="Music",
            username="user1", size=999, retry=3,
            bytes_transferred=4096, last_state="InProgress")]

        self.assertEqual(
            attempt_fingerprint_of_files(before),
            attempt_fingerprint_of_files(after),
        )

    def test_empty_attempt_has_no_identity(self):
        """``None``, not the empty-set digest: a file-less claim must not
        mint an identity a later non-empty attempt could match against
        (the cross-request guard joins on exact equality)."""
        self.assertIsNone(attempt_fingerprint_or_none([]))

    def test_non_empty_attempt_uses_the_shared_derivation(self):
        files = [DownloadFile(
            filename="Music/01.flac", id="t1", file_dir="Music",
            username="user1", size=111)]

        self.assertEqual(
            attempt_fingerprint_or_none(files),
            attempt_fingerprint_of_files(files),
        )

    def test_canonical_folder_uses_the_shared_derivation(self):
        """The folder name carries the same identity the ledger row and
        the persisted download state carry."""
        files = [DownloadFile(
            filename="Music/01.flac", id="t1", file_dir="Music",
            username="user1", size=111)]

        folder = canonical_folder_for_row(_row(files=files), "/root")

        self.assertTrue(
            folder.endswith(f" [{attempt_fingerprint_of_files(files)}]"),
            folder,
        )


class TestAttemptFingerprint(unittest.TestCase):
    """Issue #550 phase 2: attempt-scoped canonical processing folders."""

    def test_empty_set_hashes_the_empty_json_array(self):
        import hashlib
        import json

        expected = hashlib.sha256(
            json.dumps([], separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:8]

        self.assertEqual(attempt_fingerprint([]), expected)

    def test_order_independent(self):
        forward = attempt_fingerprint([
            ("user1", "Music/01.flac"),
            ("user2", "Music/02.flac"),
        ])
        backward = attempt_fingerprint([
            ("user2", "Music/02.flac"),
            ("user1", "Music/01.flac"),
        ])

        self.assertEqual(forward, backward)

    def test_sensitive_to_username_change(self):
        pairs_a = attempt_fingerprint([("user1", "Music/01.flac")])
        pairs_b = attempt_fingerprint([("user2", "Music/01.flac")])

        self.assertNotEqual(pairs_a, pairs_b)

    def test_sensitive_to_filename_change(self):
        pairs_a = attempt_fingerprint([("user1", "Music/01.flac")])
        pairs_b = attempt_fingerprint([("user1", "Music/02.flac")])

        self.assertNotEqual(pairs_a, pairs_b)

    def test_sensitive_to_file_count(self):
        one_file = attempt_fingerprint([("user1", "Music/01.flac")])
        two_files = attempt_fingerprint([
            ("user1", "Music/01.flac"),
            ("user1", "Music/02.flac"),
        ])

        self.assertNotEqual(one_file, two_files)

    def test_deterministic_across_calls(self):
        pairs = [("user1", "Music/01.flac"), ("user2", "Music/02.flac")]

        self.assertEqual(attempt_fingerprint(pairs), attempt_fingerprint(pairs))

    def test_is_short_hex(self):
        fp = attempt_fingerprint([("user1", "Music/01.flac")])

        self.assertEqual(len(fp), 8)
        int(fp, 16)  # raises ValueError if not hex


class TestCanonicalProcessingPathFingerprint(unittest.TestCase):
    """``canonical_processing_path``'s optional ``attempt_fingerprint`` param."""

    def test_empty_fingerprint_appends_nothing(self):
        path = canonical_processing_path(
            artist="Test Artist",
            title="Test Album",
            year="2020",
            slskd_download_dir="/tmp/downloads",
        )

        self.assertEqual(path, "/tmp/downloads/Test Artist - Test Album (2020)")

    def test_nonempty_fingerprint_appends_bracket_suffix(self):
        path = canonical_processing_path(
            artist="Test Artist",
            title="Test Album",
            year="2020",
            slskd_download_dir="/tmp/downloads",
            attempt_fingerprint="deadbeef",
        )

        self.assertEqual(
            path,
            "/tmp/downloads/Test Artist - Test Album (2020) [deadbeef]",
        )

    def test_different_fingerprints_produce_different_paths(self):
        base_kwargs = {
            "artist": "Test Artist",
            "title": "Test Album",
            "year": "2020",
            "slskd_download_dir": "/tmp/downloads",
        }

        path_a = canonical_processing_path(attempt_fingerprint="aaaaaaaa", **base_kwargs)
        path_b = canonical_processing_path(attempt_fingerprint="bbbbbbbb", **base_kwargs)

        self.assertNotEqual(path_a, path_b)


class TestCanonicalFolderForRow(unittest.TestCase):
    """The row-to-folder projection has one leaf implementation (#573 W1)."""

    def test_derives_folder_from_row_fields_and_exact_file_identity_set(self):
        files = [
            DownloadFile(
                filename="peer\\Album\\01.flac",
                id="transfer-1",
                file_dir="peer\\Album",
                username="peer",
                size=123,
            ),
            DownloadFile(
                filename="peer\\Album\\02.flac",
                id="transfer-2",
                file_dir="peer\\Album",
                username="peer",
                size=456,
            ),
        ]
        fingerprint = attempt_fingerprint([
            (file.username, file.filename) for file in files
        ])

        self.assertEqual(
            canonical_folder_for_row(_row(files=files), "/tmp/downloads"),
            "/tmp/downloads/Test Artist - Test Album (2020) "
            f"[{fingerprint}]",
        )


class TestStageToAiPathComponentLimit(unittest.TestCase):
    """Long Unicode metadata must remain stageable on ext4."""

    def test_four_tet_style_title_fits_and_can_be_created(self):
        artist = "⣎⡇ꉺლ༽இ•̛)ྀ◞ ༎ຶ ༽ৣৢ؞ৢ؞ؖ ꉺლ"
        # The real release title is 365 UTF-8 bytes and is dominated by
        # combining marks. This smaller readable fixture preserves that
        # filesystem shape without copying the whole title into the test.
        title = "ʅ" + "͡" * 182

        with tempfile.TemporaryDirectory() as staging_dir:
            path = stage_to_ai_path(
                artist=artist,
                title=title,
                staging_dir=staging_dir,
                request_id=42,
                auto_import=True,
            )
            album_component = os.path.basename(path)

            self.assertLessEqual(len(album_component.encode("utf-8")), 255)
            self.assertTrue(album_component.endswith(" [request-42]"))
            os.makedirs(path)
            self.assertTrue(os.path.isdir(path))

    def test_long_titles_with_the_same_prefix_remain_distinct(self):
        common = "ʅ" + "͡" * 180
        first = stage_to_ai_path(
            artist="Artist", title=f"{common}A", staging_dir="/staging",
            request_id=42, auto_import=True,
        )
        second = stage_to_ai_path(
            artist="Artist", title=f"{common}B", staging_dir="/staging",
            request_id=42, auto_import=True,
        )

        self.assertNotEqual(first, second)

    def test_short_path_is_unchanged(self):
        self.assertEqual(
            stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir="/staging",
                request_id=42,
                auto_import=True,
            ),
            "/staging/auto-import/Test Artist/Test Album [request-42]",
        )


if __name__ == "__main__":
    unittest.main()


class TestFingerprintSuffixNameLimit(unittest.TestCase):
    """The fingerprint suffix must never push the folder name past ext4's
    255-byte filename limit (codex review r2: near-limit names that fit
    before would MaterializeFailed at os.makedirs forever)."""

    def _name(self, artist: str) -> str:
        path = canonical_processing_path(
            artist=artist, title="T", year="2024",
            slskd_download_dir="/dl",
            attempt_fingerprint="aabbccdd",
        )
        return path.rsplit("/", 1)[-1]

    def test_near_limit_ascii_name_stays_within_255_bytes(self):
        name = self._name("a" * 250)
        self.assertLessEqual(len(name.encode("utf-8")), 255)
        self.assertTrue(name.endswith(" [aabbccdd]"))

    def test_multibyte_name_truncates_on_character_boundary(self):
        name = self._name("\u97f3" * 120)  # 3 bytes each -> 360 bytes
        self.assertLessEqual(len(name.encode("utf-8")), 255)
        self.assertTrue(name.endswith(" [aabbccdd]"))

    def test_short_names_are_untouched(self):
        self.assertEqual(
            self._name("Artist"), "Artist - T (2024) [aabbccdd]")


class TestProtectedStagingRoots(unittest.TestCase):
    """Issue #1122, review round 2: ONE derivation owner for every root a
    staged-dir empty-parent prune (``lib.dispatch.helpers._cleanup_staged_
    dir`` and its ``harness.import_one`` twin) must never remove."""

    def test_returns_both_shared_roots(self):
        roots = protected_staging_roots(
            processing_dir="/processing", beets_staging_dir="/incoming",
        )
        self.assertEqual(
            roots,
            frozenset({
                processing_albums_dir("/processing"),
                stage_to_ai_root(staging_dir="/incoming", auto_import=True),
            }),
        )

    def test_exactly_two_distinct_roots(self):
        roots = protected_staging_roots(
            processing_dir="/processing", beets_staging_dir="/incoming",
        )
        self.assertEqual(len(roots), 2)

    def test_result_is_a_frozenset(self):
        roots = protected_staging_roots(
            processing_dir="/processing", beets_staging_dir="/incoming",
        )
        self.assertIsInstance(roots, frozenset)
