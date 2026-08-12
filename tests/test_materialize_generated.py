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
import re
import sys
import tempfile
import unittest
from typing import Any
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import assume, example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
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
    pairs_a: list[tuple[str, str]],
    pairs_b: list[tuple[str, str]],
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

# ``max_size=300`` alone never reached the guard it was written for: over a
# whole gating run the longest basename this property ever built was 97 of
# the 255 bytes ``assert_canonical_basename_bounded`` caps (issue #1094
# per-clause audit — 0/150 examples within 158 bytes of the limit), so
# ``canonical_processing_path``'s truncation branch never executed and
# deleting it outright changed nothing. Half the draws are now explicitly
# near-limit, and the decisive boundary worlds are pinned below.
_ANY_UNICODE = st.characters(blacklist_categories=("Cs",), max_codepoint=0x2FFFF)
_NEAR_LIMIT_UNICODE_TEXT = st.text(
    alphabet=_ANY_UNICODE, min_size=100, max_size=300,
)
_LONG_UNICODE_TEXT = st.one_of(
    st.text(alphabet=_ANY_UNICODE, max_size=300),
    _NEAR_LIMIT_UNICODE_TEXT,
)
_SHORT_UNICODE_TEXT = st.text(
    alphabet=_ANY_UNICODE, max_size=12,
)
# ``"x" * 240 - ()`` sanitizes to 246 bytes: over the 244-byte base budget
# the 11-byte `` [<fp>]`` suffix leaves, so the truncation branch runs and
# the published basename lands exactly on the 255-byte cap. Its 238-byte
# sibling lands on the same cap WITHOUT truncating — the two sides of the
# boundary the clause legislates.
_TRUNCATING_BASE = "x" * 240
_EXACT_LIMIT_BASE = "y" * 238
_ONE_PAIR: list[tuple[str, str]] = [("peer0", "peer0\\Music\\01 Track.flac")]


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
    @example(artist=_TRUNCATING_BASE, title="", year="", pairs=_ONE_PAIR)
    @example(artist=_EXACT_LIMIT_BASE, title="", year="", pairs=_ONE_PAIR)
    @example(artist="漢" * 120, title="Ünïcödé" * 20, year="2026",
             pairs=_ONE_PAIR)
    def test_suffix_present_and_bounded_when_fingerprinted(
            self, artist, title, year, pairs):
        fp = attempt_fingerprint(pairs)
        path = canonical_processing_path(
            artist=artist, title=title, year=year,
            slskd_download_dir="/tmp/downloads", attempt_fingerprint=fp)
        assert_canonical_basename_bounded(os.path.basename(path), fp)

    @given(artist=_LONG_UNICODE_TEXT, title=_LONG_UNICODE_TEXT,
           year=_SHORT_UNICODE_TEXT)
    @example(artist=_TRUNCATING_BASE, title="", year="")
    @example(artist="漢" * 120, title="Ünïcödé" * 20, year="2026")
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
    pairs: set[tuple[str, str]], *, src_root: str,
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
            fp.write(f"{username}:{filename}".encode())
        file.local_path = src_path
        files.append(file)
    return make_grab_list_entry(
        files=files, artist="Test Artist", title="Test Album", year="2020",
        mb_release_id="")


def assert_folder_contents_match_manifest(
    actual_basenames: frozenset[str],
    expected_basenames: frozenset[str],
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
        pairs: set[tuple[str, str]],
        src_root: str,
        ctx: Any,
        download_root: str,
    ) -> tuple[StagedAlbum, frozenset[str]]:
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
            processing_root = os.path.join(tmpdir, "processing")
            os.mkdir(processing_root, 0o700)
            os.mkdir(os.path.join(processing_root, "albums"), 0o700)
            os.mkdir(os.path.join(processing_root, "preview"), 0o700)
            cfg = MagicMock()
            cfg.slskd_download_dir = download_root
            cfg.processing_dir = processing_root
            cfg.beets_staging_dir = os.path.join(tmpdir, "staging")
            ctx = make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg)

            staged_a, expected_a = self._materialize(
                pairs_a, os.path.join(download_root, "src-a"), ctx,
                os.path.join(processing_root, "albums"))
            staged_b, expected_b = self._materialize(
                pairs_b, os.path.join(download_root, "src-b"), ctx,
                os.path.join(processing_root, "albums"))

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
#
# Per-clause proof (issue #1094, docs/generated-testing.md § "Per-clause
# proof"): each clause gets the minimal world that makes ITS condition
# true while every earlier clause in the same function passes, and asserts
# that clause's own message anchored end to end. ``assert_canonical_
# basename_bounded`` and ``assert_resume_stability`` each own two clauses;
# a bare ``assertRaises`` could not tell which of the two answered.


def _exactly(message: str) -> str:
    """Anchor one clause's complete message for ``assertRaisesRegex``."""
    return f"^{re.escape(message)}$"


class TestMaterializeCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: every CLAUSE above must trip on a planted
    violation of the invariant it claims to enforce, with its own message."""

    def test_fingerprint_equal_clause_names_both_digests(self):
        with self.assertRaisesRegex(AssertionError, _exactly(
                "test: fingerprints diverged: 'aaaa1111' != 'bbbb2222'")):
            assert_fingerprint_equal("aaaa1111", "bbbb2222", context="test")

    def test_fingerprints_distinct_clause_names_the_colliding_sets(self):
        with self.assertRaisesRegex(AssertionError, _exactly(
                "different pair sets collided on fingerprint 'cafe1234': "
                "a=[('peer0', 'a.flac')] b=[('peer1', 'b.flac')]")):
            assert_fingerprints_distinct(
                "cafe1234", "cafe1234",
                [("peer0", "a.flac")], [("peer1", "b.flac")])

    def test_each_canonical_basename_clause_fires_on_its_own_world(self):
        overlong = ("x" * 250) + " [abcd1234]"
        cases = (
            (
                "1: the fingerprint suffix is absent",
                "Artist - Title (2020)",
                ("basename 'Artist - Title (2020)' does not end with "
                "fingerprint suffix ' [abcd1234]'"),
            ),
            (
                # The suffix IS present here, so clause 1 passes and only
                # the byte cap can answer — the PR #560 r2 truncation guard.
                "2: suffixed, but over the 255-byte ext4 cap",
                overlong,
                (f"basename {overlong!r} is {len(overlong.encode('utf-8'))} "
                "bytes, exceeds the 255-byte ext4 filename cap"),
            ),
        )
        for clause, basename, message in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, _exactly(message),
            ):
                assert_canonical_basename_bounded(basename, "abcd1234")

    def test_a_basename_exactly_on_the_cap_passes_every_clause(self):
        """The must-still-work control: 255 bytes is legal, 256 is not."""
        at_cap = ("z" * 244) + " [abcd1234]"
        self.assertEqual(len(at_cap.encode("utf-8")), 255)
        assert_canonical_basename_bounded(at_cap, "abcd1234")

    def test_no_suffix_clause_names_the_expected_bare_name(self):
        with self.assertRaisesRegex(AssertionError, _exactly(
                "basename 'Artist - Title (2020) [abcd1234]' != expected bare "
                "name 'Artist - Title (2020)' when attempt_fingerprint is "
                "empty")):
            assert_no_suffix_when_fp_empty(
                "Artist - Title (2020) [abcd1234]", "Artist - Title (2020)")

    def test_folder_contents_clause_names_the_exact_divergence(self):
        cases = (
            (
                "a manifest file never landed",
                frozenset(),
                ("attempt B: folder contents diverged from its manifest "
                "(missing=['01 Track.flac'] extra=[])"),
            ),
            (
                "another attempt's file blended in (the #550 shape)",
                frozenset({"01 Track.flac", "alien-track.flac"}),
                ("attempt B: folder contents diverged from its manifest "
                "(missing=[] extra=['alien-track.flac'])"),
            ),
        )
        for label, actual, message in cases:
            with self.subTest(world=label), self.assertRaisesRegex(
                AssertionError, _exactly(message),
            ):
                assert_folder_contents_match_manifest(
                    actual, frozenset({"01 Track.flac"}), label="attempt B")

    def test_each_resume_stability_clause_fires_on_its_own_world(self):
        cases = (
            (
                "1: identical manifests split across two folders",
                "/tmp/downloads/Album [aaaa1111]",
                "/tmp/downloads/Album [bbbb2222]",
                True,
                ("identical manifests produced different canonical folders "
                "(resume stability broken): '/tmp/downloads/Album [aaaa1111]' "
                "!= '/tmp/downloads/Album [bbbb2222]'"),
            ),
            (
                "2: different manifests collapsed onto one folder",
                "/tmp/downloads/Album [aaaa1111]",
                "/tmp/downloads/Album [aaaa1111]",
                False,
                ("different manifests collided on the same canonical folder: "
                "'/tmp/downloads/Album [aaaa1111]'"),
            ),
        )
        for clause, path_a, path_b, manifests_equal, message in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, _exactly(message),
            ):
                assert_resume_stability(
                    path_a, path_b, manifests_equal=manifests_equal)


if __name__ == "__main__":
    unittest.main()
