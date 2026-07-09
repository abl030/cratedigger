#!/usr/bin/env python3
"""Generated round-trip tests for ``PersistedYoutubeRow`` writes — #546 W3.

Invariant: every field of a persisted ``PersistedYoutubeRow`` round-trips
through real PostgreSQL unchanged — no field is silently dropped on write.
This is the property half of the Rule A pair
(``.claude/rules/test-fidelity.md``); the deterministic pin lives in
``tests/test_pipeline_db.py::TestYoutubeAlbumMappings::
test_upsert_round_trip_preserves_every_field``.

The bug class this guards against is migration 036's round 2 P0-1: the
hand-written INSERT column list in ``upsert_youtube_album_mapping`` omitted
``album_title`` (and would have omitted ``album_artist`` the same way), so
``psycopg2.extras.execute_values`` silently dropped the field on every
production write. #546 W3 made the column list DERIVE from
``msgspec.structs.fields(PersistedYoutubeRow)`` so that specific drift is
now structurally impossible — this generated test patrols the world space
around the deterministic pin to prove the derivation actually holds for
every field shape (missing optionals, unicode text, nested JSONB lists of
varying size), not just the one fixture the pin exercises.

Runs against a real ``PipelineDB`` (via the ephemeral-PostgreSQL harness in
``tests/conftest.py``) rather than ``FakePipelineDB`` — the fake stores
whatever it's handed, so it cannot catch a write-side column-list drift
(the exact test-fidelity gap Rule A exists to close). Each Hypothesis
example targets a fresh, uniquely-generated ``release_group_identifier`` so
examples never collide and no per-example TRUNCATE is needed.

No skip gate: if ``TEST_DB_DSN`` is genuinely unset, ``setUpClass``'s
connection attempt fails loudly rather than skipping (CLAUDE.md §
"Skipped tests are an anti-pattern"; mirrors
``tests/test_pipeline_db_column_contract.py``'s convention for new
real-PG test modules, this repo's dev shell always has ``TEST_DB_DSN`` set
by ``tests/conftest.py``'s ephemeral PostgreSQL bootstrap).

Profiles and promotion policy: ``tests/_hypothesis_profiles.py`` and
``docs/generated-testing.md``.
"""

from __future__ import annotations

import os
import sys
import unittest
import uuid
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

import msgspec
from hypothesis import given, settings
from hypothesis import strategies as st

from lib.pipeline_db import (
    PersistedDistance,
    PersistedTrack,
    PersistedYoutubeRow,
    PipelineDB,
)

TEST_DSN = os.environ.get("TEST_DB_DSN")

# The two JSONB columns — compared structurally via ``msgspec.to_builtins``
# rather than by identity, mirroring ``upsert_youtube_album_mapping``'s own
# JSONB-vs-scalar split.
_JSONB_FIELDS = frozenset({"yt_tracks", "distances"})


# ===========================================================================
# Strategies — deliberately unconstrained beyond what the schema/Struct
# requires (no plausibility filters): arbitrary-ish unicode text, absent vs.
# present optionals, empty vs. multi-element JSONB lists.
# ===========================================================================

# Printable-ish unicode, but excludes control characters (category "Cc",
# which includes NUL) — PostgreSQL's TEXT type rejects embedded NUL bytes
# outright, which would fail for a reason unrelated to the invariant under
# test. Surrogates ("Cs") are excluded because they aren't valid standalone
# codepoints for UTF-8 encoding.
def _text(min_size: int = 0, max_size: int = 40) -> st.SearchStrategy[str]:
    return st.text(
        alphabet=st.characters(
            blacklist_categories=("Cs", "Cc"), max_codepoint=0x2FFFF),
        min_size=min_size, max_size=max_size,
    )


_FLOAT = st.floats(
    min_value=-1_000.0, max_value=1_000.0,
    allow_nan=False, allow_infinity=False,
)
_SMALL_INT = st.integers(min_value=0, max_value=999)


@st.composite
def persisted_tracks(draw: st.DrawFn) -> PersistedTrack:
    return PersistedTrack(
        title=draw(st.one_of(st.none(), _text(max_size=60))),
        artists=draw(st.one_of(
            st.none(),
            st.lists(
                st.fixed_dictionaries({"name": _text(min_size=1, max_size=30)}),
                max_size=3),
        )),
        length_seconds=draw(st.one_of(st.none(), _FLOAT)),
        track_number=draw(st.one_of(st.none(), _SMALL_INT)),
        disc_number=draw(st.one_of(st.none(), _SMALL_INT)),
        video_id=draw(st.one_of(st.none(), _text(min_size=1, max_size=20))),
    )


@st.composite
def persisted_distances(draw: st.DrawFn) -> PersistedDistance:
    return PersistedDistance(
        mbid=draw(st.one_of(st.none(), _text(min_size=1, max_size=40))),
        outcome=draw(st.one_of(
            st.none(), st.sampled_from(["ok", "no_mb_tracks", "error"]))),
        distance=draw(st.one_of(
            st.none(), st.floats(
                min_value=0.0, max_value=1.0,
                allow_nan=False, allow_infinity=False))),
        components=draw(st.one_of(
            st.none(),
            st.dictionaries(_text(min_size=1, max_size=10), _FLOAT, max_size=3),
        )),
        matched_tracks=draw(st.one_of(st.none(), _SMALL_INT)),
        total_local_tracks=draw(st.one_of(st.none(), _SMALL_INT)),
        total_mb_tracks=draw(st.one_of(st.none(), _SMALL_INT)),
        extra_local_tracks=draw(st.one_of(st.none(), _SMALL_INT)),
        extra_mb_tracks=draw(st.one_of(st.none(), _SMALL_INT)),
        error_message=draw(st.one_of(st.none(), _text(max_size=100))),
    )


@st.composite
def persisted_youtube_rows(draw: st.DrawFn) -> PersistedYoutubeRow:
    return PersistedYoutubeRow(
        yt_browse_id=draw(_text(min_size=1, max_size=40)),
        yt_url=draw(_text(min_size=1, max_size=100)),
        yt_track_count=draw(_SMALL_INT),
        yt_audio_playlist_id=draw(st.one_of(st.none(), _text(min_size=1, max_size=40))),
        yt_year=draw(st.one_of(st.none(), st.integers(min_value=1900, max_value=2100))),
        album_title=draw(st.one_of(st.none(), _text(max_size=100))),
        album_artist=draw(st.one_of(st.none(), _text(max_size=100))),
        yt_tracks=draw(st.lists(persisted_tracks(), max_size=4)),
        distances=draw(st.lists(persisted_distances(), max_size=4)),
    )


# ===========================================================================
# Invariant checker — module-level function so the known-bad self-test can
# call it directly (pattern: TestInvariantCheckersTripOnViolations in
# tests/test_quality_generated.py).
# ===========================================================================

def assert_row_round_trips(
    struct: PersistedYoutubeRow, returned_row: dict[str, Any],
) -> None:
    """Assert every ``PersistedYoutubeRow`` field is present and equal in
    ``returned_row`` (the dict shape ``get_youtube_album_mapping`` returns).

    JSONB fields (``yt_tracks``, ``distances``) compare structurally via
    ``msgspec.to_builtins`` rather than by identity; every other field
    compares directly.
    """
    for f in msgspec.structs.fields(PersistedYoutubeRow):
        name = f.name
        if name not in returned_row:
            raise AssertionError(
                f"field {name!r} missing from returned row entirely")
        expected: Any = getattr(struct, name)
        if name in _JSONB_FIELDS:
            expected = msgspec.to_builtins(expected)
        actual = returned_row[name]
        if actual != expected:
            raise AssertionError(
                f"field {name!r} round-trip mismatch: "
                f"expected {expected!r}, got {actual!r}")


class TestAssertRowRoundTripsCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-test: proves ``assert_row_round_trips`` has teeth."""

    def test_trips_on_missing_field(self) -> None:
        struct = PersistedYoutubeRow(
            yt_browse_id="x", yt_url="u", yt_track_count=1)
        returned = msgspec.to_builtins(struct)
        del returned["album_title"]
        with self.assertRaises(AssertionError):
            assert_row_round_trips(struct, returned)

    def test_trips_on_scalar_value_drift(self) -> None:
        struct = PersistedYoutubeRow(
            yt_browse_id="x", yt_url="u", yt_track_count=1)
        returned = msgspec.to_builtins(struct)
        returned["yt_track_count"] = 999
        with self.assertRaises(AssertionError):
            assert_row_round_trips(struct, returned)

    def test_trips_on_jsonb_field_drift(self) -> None:
        struct = PersistedYoutubeRow(
            yt_browse_id="x", yt_url="u", yt_track_count=1,
            yt_tracks=[PersistedTrack(title="Track 1")])
        returned = msgspec.to_builtins(struct)
        returned["yt_tracks"] = []
        with self.assertRaises(AssertionError):
            assert_row_round_trips(struct, returned)

    def test_passes_on_faithful_round_trip(self) -> None:
        struct = PersistedYoutubeRow(
            yt_browse_id="x", yt_url="u", yt_track_count=1,
            yt_tracks=[PersistedTrack(title="Track 1")],
            distances=[PersistedDistance(mbid="mb-1", distance=0.1)])
        returned = msgspec.to_builtins(struct)
        assert_row_round_trips(struct, returned)  # must not raise


class TestYoutubeMappingWriteRoundTripGenerated(unittest.TestCase):
    """Drives the REAL ``upsert_youtube_album_mapping`` /
    ``get_youtube_album_mapping`` against real PostgreSQL over generated
    ``PersistedYoutubeRow`` worlds."""

    db: PipelineDB

    @classmethod
    def setUpClass(cls) -> None:
        cls.db = PipelineDB(TEST_DSN)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()

    @settings(max_examples=40, deadline=None)
    @given(row=persisted_youtube_rows())
    def test_every_field_round_trips_through_real_postgres(
        self, row: PersistedYoutubeRow,
    ) -> None:
        # A fresh release_group_identifier per example means examples never
        # collide — no per-example TRUNCATE needed to keep them isolated.
        rg_id = f"rg-fuzz-{uuid.uuid4()}"
        self.db.upsert_youtube_album_mapping(rg_id, "mb", [row])
        got = self.db.get_youtube_album_mapping(rg_id, "mb")
        self.assertIsNotNone(got)
        assert got is not None
        self.assertEqual(len(got), 1)
        assert_row_round_trips(row, got[0])


if __name__ == "__main__":
    unittest.main()
