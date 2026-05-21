"""Tests for ``snapshot_fingerprint`` in ``lib.quality_evidence``.

The fingerprint is the addressing key for the post-rekey
``album_quality_evidence`` table (plan U1/U2/U3 in
``docs/plans/2026-05-16-002-refactor-evidence-canonical-cleanup-plan.md``).
The formula is load-bearing: U2's SQL migration computes the same hash from
the existing ``album_quality_evidence_files`` rows, so a Python-vs-SQL drift
would scramble the post-deploy lookup.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest

from lib.quality import AlbumQualityEvidenceFile
from lib.quality_evidence import (
    snapshot_audio_files,
    snapshot_fingerprint,
)


def _make_file(
    *,
    relative_path: str = "track01.flac",
    size_bytes: int = 12345,
    mtime_ns: int = 1_700_000_000_000_000_000,
    extension: str = "flac",
    container: str = "flac",
    codec: str | None = "flac",
    decode_ok: bool = True,
) -> AlbumQualityEvidenceFile:
    return AlbumQualityEvidenceFile(
        relative_path=relative_path,
        size_bytes=size_bytes,
        mtime_ns=mtime_ns,
        extension=extension,
        container=container,
        codec=codec,
        decode_ok=decode_ok,
    )


class TestSnapshotFingerprintFormula(unittest.TestCase):
    """Pin the exact formula so U2's SQL migration can mirror it."""

    def test_formula_matches_documented_recipe(self):
        """SHA-256 of compact-JSON-encoded sorted list of per-file tuples."""

        files = [
            _make_file(relative_path="track02.flac", size_bytes=222, codec="flac"),
            _make_file(relative_path="track01.flac", size_bytes=111, codec=None),
        ]
        expected_payload = [
            ["track01.flac", 111, "flac", "flac", None],
            ["track02.flac", 222, "flac", "flac", "flac"],
        ]
        expected_json = json.dumps(
            expected_payload,
            sort_keys=False,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        expected_digest = hashlib.sha256(expected_json.encode("utf-8")).hexdigest()
        self.assertEqual(snapshot_fingerprint(files), expected_digest)

    def test_input_order_independence(self):
        """Two identical lists in different input order produce the same hash."""

        a = _make_file(relative_path="a.flac", size_bytes=10)
        b = _make_file(relative_path="b.flac", size_bytes=20)
        c = _make_file(relative_path="c.flac", size_bytes=30)
        self.assertEqual(
            snapshot_fingerprint([a, b, c]),
            snapshot_fingerprint([c, a, b]),
        )

    def test_relative_path_changes_fingerprint(self):
        baseline = snapshot_fingerprint([_make_file(relative_path="track01.flac")])
        changed = snapshot_fingerprint([_make_file(relative_path="track02.flac")])
        self.assertNotEqual(baseline, changed)

    def test_size_bytes_changes_fingerprint(self):
        baseline = snapshot_fingerprint([_make_file(size_bytes=111)])
        changed = snapshot_fingerprint([_make_file(size_bytes=222)])
        self.assertNotEqual(baseline, changed)

    def test_extension_changes_fingerprint(self):
        baseline = snapshot_fingerprint([_make_file(extension="flac")])
        changed = snapshot_fingerprint([_make_file(extension="mp3")])
        self.assertNotEqual(baseline, changed)

    def test_container_changes_fingerprint(self):
        baseline = snapshot_fingerprint([_make_file(container="flac")])
        changed = snapshot_fingerprint([_make_file(container="ogg")])
        self.assertNotEqual(baseline, changed)

    def test_codec_changes_fingerprint(self):
        baseline = snapshot_fingerprint([_make_file(codec="flac")])
        changed = snapshot_fingerprint([_make_file(codec="alac")])
        self.assertNotEqual(baseline, changed)

    def test_mtime_does_not_change_fingerprint(self):
        """Regression guard: re-introducing mtime would break dedupe.

        ``_snapshot_match_key`` deliberately excludes ``mtime_ns`` (see its
        docstring — ID3 tagging + virtiofs flake). The fingerprint must
        agree.
        """

        baseline = snapshot_fingerprint([_make_file(mtime_ns=1)])
        changed = snapshot_fingerprint([_make_file(mtime_ns=999_999_999)])
        self.assertEqual(baseline, changed)

    def test_decode_ok_does_not_change_fingerprint(self):
        """``decode_ok`` is per-file evidence, not identity. Not in the formula."""

        baseline = snapshot_fingerprint([_make_file(decode_ok=True)])
        changed = snapshot_fingerprint([_make_file(decode_ok=False)])
        self.assertEqual(baseline, changed)

    def test_null_codec_handled_consistently(self):
        """Two files with codec=None hash equal; differ from codec set."""

        a = snapshot_fingerprint([_make_file(codec=None)])
        b = snapshot_fingerprint([_make_file(codec=None)])
        c = snapshot_fingerprint([_make_file(codec="flac")])
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)

    def test_empty_list_is_stable_and_distinct(self):
        """Empty fileset produces a defined hash distinct from any single-file hash."""

        empty1 = snapshot_fingerprint([])
        empty2 = snapshot_fingerprint([])
        self.assertEqual(empty1, empty2)
        # SHA-256 hex digest is 64 chars
        self.assertEqual(len(empty1), 64)
        single = snapshot_fingerprint([_make_file()])
        self.assertNotEqual(empty1, single)

    def test_empty_list_fingerprint_matches_explicit_empty_json(self):
        """Empty list hashes the JSON encoding of an empty list, ``"[]"``."""

        expected = hashlib.sha256(b"[]").hexdigest()
        self.assertEqual(snapshot_fingerprint([]), expected)


if __name__ == "__main__":
    unittest.main()
