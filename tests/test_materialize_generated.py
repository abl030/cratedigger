#!/usr/bin/env python3
"""Generated attempt-scoped materialize tests — issue #548 method, #550
phase 2 follow-up.

PR #560 shipped the invariant "every download attempt materializes into
its own attempt-scoped folder derived from the manifest fingerprint — no
attempt ever validates against files another attempt placed" with only
hand-picked deterministic tests
(``tests/test_download.py::TestAttemptScopedCanonicalFolder``). Per
docs/generated-testing.md's invariant-first rule, this module adds the
Hypothesis property coverage.

Two properties, driving REAL production functions
(``lib/processing_paths.py::attempt_fingerprint`` /
``canonical_processing_path`` and
``lib/download_materialization.py::_materialize_processing_dir``):

1. **Fingerprint properties** — ``attempt_fingerprint`` over generated
   ``(username, filename)`` sets is permutation-invariant, deterministic,
   and distinguishes different sets (an 8-hex sha256 prefix collision
   between two generated worlds would be a genuine finding); the empty
   set hashes to a stable, defined digest. ``canonical_processing_path``
   appends the fingerprint suffix iff the fingerprint is non-empty, and
   the resulting basename never exceeds the ext4 255-byte cap even with
   adversarially long generated unicode artist/title strings (PR #560's
   r2 truncation guard).

2. **Materialize isolation** — two download attempts for the SAME
   artist/title/year, with independently generated (sometimes
   overlapping, sometimes identical, sometimes disjoint)
   ``(username, filename)`` manifests, materialized in sequence via the
   real ``_materialize_processing_dir`` against a real tempdir: attempt
   B's folder contains EXACTLY B's manifest files (never any of A's),
   attempt A's folder is untouched by B's materialize, and when the two
   manifests are IDENTICAL sets, both attempts resolve to the SAME
   folder (resume stability). Generalizes the setup of
   ``tests/test_download.py::TestAttemptScopedCanonicalFolder
   .test_materialize_never_blends_files_from_a_different_attempt`` over
   a generated space of manifest pairs instead of one hand-picked pair.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import hashlib
import os
import sys
import tempfile
import unittest
from typing import Any
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import assume, example, given
from hypothesis import strategies as st

from lib.download_materialization import (
    Materialized,
    _materialize_processing_dir,
)
from lib.grab_list import DownloadFile, GrabListEntry
from lib.processing_paths import (
    attempt_fingerprint,
    canonical_folder_for_row,
    canonical_processing_path,
    sanitize_processing_folder_name,
)
from lib.staged_album import StagedAlbum, staged_filename
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db, make_grab_list_entry

# ============================================================================
# Property 1 — attempt_fingerprint over generated (username, filename) sets
# ============================================================================

_UNICODE_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), max_codepoint=0x2FFFF),
    max_size=24,
)
_fp_pairs_strategy = st.lists(
    st.tuples(_UNICODE_TEXT, _UNICODE_TEXT), min_size=0, max_size=8, unique=True,
)
_fp_pairs_nonempty_strategy = st.lists(
    st.tuples(_UNICODE_TEXT, _UNICODE_TEXT), min_size=1, max_size=8, unique=True,
)


def assert_fingerprint_equal(a: str, b: str, *, context: str) -> None:
    """Two fingerprint computations expected to coincide (a reordering of
    the same pair set, or hashing the same input twice) must produce the
    identical digest (module-level for the known-bad self-test)."""
    if a != b:
        raise AssertionError(f"{context}: fingerprints diverged: {a!r} != {b!r}")


def assert_fingerprints_distinct(
    fp_a: str,
    fp_b: str,
    pairs_a: "list[tuple[str, str]]",
    pairs_b: "list[tuple[str, str]]",
) -> None:
    """Two DIFFERENT (username, filename) sets must not share a
    fingerprint. An 8-hex sha256 prefix collision between two generated
    worlds is practically impossible and WOULD be worth knowing — this
    checker does not swallow it (module-level for the self-test)."""
    if fp_a == fp_b:
        raise AssertionError(
            "different pair sets collided on fingerprint "
            f"{fp_a!r}: a={sorted(pairs_a)} b={sorted(pairs_b)}")


class TestFingerprintProperties(unittest.TestCase):
    """Property 1: attempt_fingerprint is permutation-invariant,
    deterministic, and distinguishes different sets; the empty set is a
    stable, defined digest — not an error."""

    @given(pairs=_fp_pairs_strategy, data=st.data())
    def test_permutation_invariant(self, pairs, data):
        shuffled = data.draw(st.permutations(pairs))
        assert_fingerprint_equal(
            attempt_fingerprint(pairs), attempt_fingerprint(shuffled),
            context="permutation invariance")

    @given(pairs=_fp_pairs_strategy)
    def test_deterministic_across_calls(self, pairs):
        assert_fingerprint_equal(
            attempt_fingerprint(list(pairs)), attempt_fingerprint(list(pairs)),
            context="determinism across calls")

    @given(pairs_a=_fp_pairs_strategy, pairs_b=_fp_pairs_strategy)
    def test_distinguishes_different_sets(self, pairs_a, pairs_b):
        assume(set(pairs_a) != set(pairs_b))
        assert_fingerprints_distinct(
            attempt_fingerprint(pairs_a), attempt_fingerprint(pairs_b),
            pairs_a, pairs_b)

    def test_empty_set_is_a_stable_defined_digest(self):
        fp = attempt_fingerprint([])
        self.assertEqual(len(fp), 8)
        self.assertEqual(fp, hashlib.sha256(b"[]").hexdigest()[:8])


# ============================================================================
# Property 1b — canonical_processing_path suffix + the r2 truncation guard
# ============================================================================

_LONG_UNICODE_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), max_codepoint=0x2FFFF),
    max_size=300,
)
_SHORT_UNICODE_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), max_codepoint=0x2FFFF),
    max_size=12,
)


def assert_canonical_basename_bounded(basename: str, fp: str) -> None:
    """A fingerprinted canonical folder's basename must (a) end with the
    ' [<fp>]' suffix and (b) never exceed the ext4 255-byte filename cap,
    no matter how long the sanitized artist/title base was (PR #560's r2
    truncation guard — module-level for the known-bad self-test)."""
    suffix = f" [{fp}]"
    if not basename.endswith(suffix):
        raise AssertionError(
            f"basename {basename!r} does not end with fingerprint suffix {suffix!r}")
    encoded_len = len(basename.encode("utf-8"))
    if encoded_len > 255:
        raise AssertionError(
            f"basename {basename!r} is {encoded_len} bytes, exceeds the "
            "255-byte ext4 filename cap")


def assert_no_suffix_when_fp_empty(basename: str, expected_bare_name: str) -> None:
    """When attempt_fingerprint is empty, canonical_processing_path must
    return the bare sanitized 'Artist - Title (Year)' folder name
    unchanged — no suffix, no truncation (module-level for the self-test)."""
    if basename != expected_bare_name:
        raise AssertionError(
            f"basename {basename!r} != expected bare name "
            f"{expected_bare_name!r} when attempt_fingerprint is empty")


class TestCanonicalPathProperties(unittest.TestCase):
    """Property 1b: the fingerprint suffix is present iff the fingerprint
    is non-empty, and the fingerprinted basename is always ≤255 bytes —
    driven with adversarially long generated unicode artist/title."""

    @given(artist=_LONG_UNICODE_TEXT, title=_LONG_UNICODE_TEXT,
           year=_SHORT_UNICODE_TEXT, pairs=_fp_pairs_nonempty_strategy)
    def test_suffix_present_and_bounded_when_fingerprinted(
            self, artist, title, year, pairs):
        fp = attempt_fingerprint(pairs)
        path = canonical_processing_path(
            artist=artist, title=title, year=year,
            slskd_download_dir="/tmp/downloads", attempt_fingerprint=fp)
        assert_canonical_basename_bounded(os.path.basename(path), fp)

    @given(artist=_LONG_UNICODE_TEXT, title=_LONG_UNICODE_TEXT,
           year=_SHORT_UNICODE_TEXT)
    def test_no_suffix_when_fingerprint_empty(self, artist, title, year):
        path = canonical_processing_path(
            artist=artist, title=title, year=year,
            slskd_download_dir="/tmp/downloads", attempt_fingerprint="")
        expected = sanitize_processing_folder_name(f"{artist} - {title} ({year})")
        assert_no_suffix_when_fp_empty(os.path.basename(path), expected)


# ============================================================================
# Property 2 — materialize isolation (issue #550 phase 2)
# ============================================================================
#
# _materialize_processing_dir's canonical folder is keyed by
# attempt_fingerprint(files) (via canonical_folder_for_row). Two
# different download attempts for the SAME artist/title/year must
# materialize into DIFFERENT folders whenever their (username, filename)
# manifests differ, and into the SAME folder when the manifests are
# identical (resume stability) — the exact seam PR #560 fixed. This
# drives the real production function twice against a real tempdir,
# generalizing
# tests/test_download.py::TestAttemptScopedCanonicalFolder
# .test_materialize_never_blends_files_from_a_different_attempt over a
# generated space of overlapping/disjoint/identical manifests.

_ATTEMPT_PAIR_POOL: tuple[tuple[str, str], ...] = (
    ("peerA", "peerA\\Music\\01 Track.flac"),
    ("peerA", "peerA\\Music\\02 Ûnïcode.mp3"),
    ("peerB", "peerB\\Music\\01 曲.opus"),
    ("péer♪", "péer♪\\Music\\01 ☆Star☆.flac"),
    ("USER_X", "USER_X\\Music\\01 de Français.mp3"),
    ("USER_X", "USER_X\\Music\\02 B-Side.wav"),
)
# Every basename (the part staged_filename keeps) is unique across the
# whole pool by construction, so any subset's staged basenames form a set
# with no collisions regardless of which pairs are drawn.

_manifest_pairs_strategy = st.sets(
    st.sampled_from(_ATTEMPT_PAIR_POOL),
    min_size=1, max_size=len(_ATTEMPT_PAIR_POOL))

# Pinned worlds guaranteeing the resume-stability, partial-overlap, and
# fully-disjoint branches all run even at the bounded suite tier.
_IDENTICAL_PAIRS = set(_ATTEMPT_PAIR_POOL[:2])
_PARTIAL_A_PAIRS = set(_ATTEMPT_PAIR_POOL[:2])
_PARTIAL_B_PAIRS = set(_ATTEMPT_PAIR_POOL[1:3])
_DISJOINT_A_PAIRS = set(_ATTEMPT_PAIR_POOL[:2])
_DISJOINT_B_PAIRS = set(_ATTEMPT_PAIR_POOL[3:5])


def _build_attempt_entry(
    pairs: "set[tuple[str, str]]", *, src_root: str,
) -> GrabListEntry:
    """A GrabListEntry whose files are real on-disk DownloadFiles stamped
    with local_path — the event-stamped shape _materialize_processing_dir
    requires (issue #146). Each pair gets its own physical source file
    under ``src_root``, so attempt A and attempt B never share a source
    path even when their (username, filename) identity overlaps."""
    files: list[DownloadFile] = []
    for username, filename in sorted(pairs):
        file = DownloadFile(
            filename=filename, id=f"{username}:{filename}",
            file_dir=f"{username}\\Music", username=username, size=16,
        )
        basename = staged_filename(file)
        src_dir = os.path.join(src_root, username)
        os.makedirs(src_dir, exist_ok=True)
        src_path = os.path.join(src_dir, basename)
        with open(src_path, "wb") as fp:
            fp.write(f"{username}:{filename}".encode("utf-8"))
        file.local_path = src_path
        files.append(file)
    return make_grab_list_entry(
        files=files, artist="Test Artist", title="Test Album", year="2020",
        mb_release_id="")


def assert_folder_contents_match_manifest(
    actual_basenames: "frozenset[str]",
    expected_basenames: "frozenset[str]",
    *,
    label: str,
) -> None:
    """A materialized attempt folder must contain EXACTLY the staged
    basenames its own manifest implies — never files another attempt
    placed, never a subset of its own (module-level for the self-test)."""
    if actual_basenames != expected_basenames:
        raise AssertionError(
            f"{label}: folder contents diverged from its manifest "
            f"(missing={sorted(expected_basenames - actual_basenames)} "
            f"extra={sorted(actual_basenames - expected_basenames)})")


def assert_resume_stability(
    path_a: str, path_b: str, *, manifests_equal: bool,
) -> None:
    """Two attempts with an IDENTICAL manifest must resolve to the SAME
    canonical folder (resume stability); two attempts with a DIFFERENT
    manifest must resolve to DIFFERENT folders (attempt isolation, #550
    phase 2 — module-level for the known-bad self-test)."""
    if manifests_equal and path_a != path_b:
        raise AssertionError(
            "identical manifests produced different canonical folders "
            f"(resume stability broken): {path_a!r} != {path_b!r}")
    if not manifests_equal and path_a == path_b:
        raise AssertionError(
            "different manifests collided on the same canonical folder: "
            f"{path_a!r}")


class TestMaterializeAttemptIsolation(unittest.TestCase):
    """Property 2: attempt-scoped materialize isolation and resume
    stability, driven against the real _materialize_processing_dir."""

    def _materialize(
        self,
        pairs: "set[tuple[str, str]]",
        src_root: str,
        ctx: Any,
        download_root: str,
    ) -> "tuple[StagedAlbum, frozenset[str]]":
        album = _build_attempt_entry(pairs, src_root=src_root)
        expected_basenames = frozenset(staged_filename(f) for f in album.files)
        staged = StagedAlbum.from_entry(
            album,
            default_path=canonical_folder_for_row(album, download_root))
        result = _materialize_processing_dir(album, staged, ctx)
        self.assertIsInstance(result, Materialized)
        return staged, expected_basenames

    @given(pairs_a=_manifest_pairs_strategy, pairs_b=_manifest_pairs_strategy)
    @example(pairs_a=_IDENTICAL_PAIRS, pairs_b=_IDENTICAL_PAIRS)
    @example(pairs_a=_PARTIAL_A_PAIRS, pairs_b=_PARTIAL_B_PAIRS)
    @example(pairs_a=_DISJOINT_A_PAIRS, pairs_b=_DISJOINT_B_PAIRS)
    def test_materialize_never_blends_files_across_attempts(
            self, pairs_a, pairs_b):
        with tempfile.TemporaryDirectory(
                prefix="cratedigger-materialize-gen-") as tmpdir:
            download_root = os.path.join(tmpdir, "downloads")
            os.makedirs(download_root)
            cfg = MagicMock()
            cfg.slskd_download_dir = download_root
            cfg.beets_staging_dir = os.path.join(tmpdir, "staging")
            ctx = make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg)

            staged_a, expected_a = self._materialize(
                pairs_a, os.path.join(tmpdir, "src-a"), ctx, download_root)
            staged_b, expected_b = self._materialize(
                pairs_b, os.path.join(tmpdir, "src-b"), ctx, download_root)

            manifests_equal = pairs_a == pairs_b
            assert_resume_stability(
                staged_a.current_path, staged_b.current_path,
                manifests_equal=manifests_equal)

            actual_b = frozenset(os.listdir(staged_b.current_path))
            assert_folder_contents_match_manifest(
                actual_b, expected_b, label="attempt B")

            if not manifests_equal:
                actual_a = frozenset(os.listdir(staged_a.current_path))
                assert_folder_contents_match_manifest(
                    actual_a, expected_a, label="attempt A")


# ============================================================================
# Property 3 — known-bad self-tests for the invariant checkers
# ============================================================================

class TestMaterializeCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: every checker above must trip on a planted
    violation of the invariant it claims to enforce."""

    def test_fingerprint_equal_checker_trips_on_divergence(self):
        with self.assertRaises(AssertionError):
            assert_fingerprint_equal("aaaa1111", "bbbb2222", context="test")

    def test_fingerprints_distinct_checker_trips_on_collision(self):
        with self.assertRaises(AssertionError):
            assert_fingerprints_distinct(
                "cafe1234", "cafe1234",
                [("peer0", "a.flac")], [("peer1", "b.flac")])

    def test_canonical_basename_bounded_checker_trips_on_missing_suffix(self):
        with self.assertRaises(AssertionError):
            assert_canonical_basename_bounded("Artist - Title (2020)", "abcd1234")

    def test_canonical_basename_bounded_checker_trips_on_overlength(self):
        overlong = ("x" * 250) + " [abcd1234]"
        with self.assertRaises(AssertionError):
            assert_canonical_basename_bounded(overlong, "abcd1234")

    def test_no_suffix_checker_trips_on_mismatch(self):
        with self.assertRaises(AssertionError):
            assert_no_suffix_when_fp_empty(
                "Artist - Title (2020) [abcd1234]", "Artist - Title (2020)")

    def test_folder_contents_checker_trips_on_missing_file(self):
        with self.assertRaises(AssertionError):
            assert_folder_contents_match_manifest(
                frozenset(), frozenset({"01 Track.flac"}), label="attempt B")

    def test_folder_contents_checker_trips_on_extra_file(self):
        with self.assertRaises(AssertionError):
            assert_folder_contents_match_manifest(
                frozenset({"01 Track.flac", "alien-track.flac"}),
                frozenset({"01 Track.flac"}), label="attempt B")

    def test_resume_stability_checker_trips_when_identical_manifests_diverge(self):
        with self.assertRaises(AssertionError):
            assert_resume_stability(
                "/tmp/downloads/Album [aaaa1111]",
                "/tmp/downloads/Album [bbbb2222]",
                manifests_equal=True)

    def test_resume_stability_checker_trips_when_different_manifests_collide(self):
        with self.assertRaises(AssertionError):
            assert_resume_stability(
                "/tmp/downloads/Album [aaaa1111]",
                "/tmp/downloads/Album [aaaa1111]",
                manifests_equal=False)


if __name__ == "__main__":
    unittest.main()
