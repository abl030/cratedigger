"""Tests for ``lib/processing_paths.py``."""

import unittest

from lib.grab_list import DownloadFile, GrabListEntry
from lib.processing_paths import (
    attempt_fingerprint,
    canonical_folder_for_row,
    canonical_processing_path,
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
        base_kwargs = dict(
            artist="Test Artist",
            title="Test Album",
            year="2020",
            slskd_download_dir="/tmp/downloads",
        )

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
