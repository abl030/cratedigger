#!/usr/bin/env python3
"""Generated property for the Discogs id neutralizer (issue #570 defect 2).

INVARIANT: An applied Discogs match leaves `mb_albumid` and
`mb_releasegroupid` empty — never a bare-numeric Discogs id. The identifier
lives ONLY in `discogs_albumid`. MusicBrainz (and any other non-Discogs)
matches keep their `album_id` unchanged.

`harness.beets_harness._neutralize_discogs_provider_ids` is the fix;
`tests.test_harness_discogs_neutralize.discogs_provider_ids_neutralized` is
the invariant checker it exists to satisfy (test-only — production never
reads its return value, so it lives in the test layer, not in the harness
module; this module imports the single definition rather than duplicating
it). This module patrols the invariant over a generated candidate world
space (data_source x id-shape), pins the deterministic scenarios from
tests/test_harness_discogs_neutralize.py as `@example`s, and proves the
checker actually trips via a known-bad self-test — the pattern established
in tests/test_disk_reaper_generated.py.

The real-beets subprocess contract (real `AlbumInfo`, real
`MEDIA_FIELD_MAP`, real `item_data`) lives in
tests/test_harness_beets2_contract.py — this module only exercises the pure
duck-typed helper, so it mocks `beets` in sys.modules like every other
harness unit test (tests/test_harness_serialization.py).
"""

from __future__ import annotations

import os
import sys
import unittest
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given
from hypothesis import strategies as st

_beets_mocks = {
    "beets": MagicMock(),
    "beets.config": MagicMock(),
    "beets.library": MagicMock(),
    "beets.plugins": MagicMock(),
    "beets.importer": MagicMock(),
    "beets.importer.actions": MagicMock(),
    "beets.importer.session": MagicMock(),
    "beets.importer.tasks": MagicMock(),
    "beets.autotag": MagicMock(),
    "beets.dbcore": MagicMock(),
    "beets.util": MagicMock(),
}
for name, mock in _beets_mocks.items():
    sys.modules.setdefault(name, mock)

setattr(sys.modules["beets.importer.session"], "ImportSession",
        type("ImportSession", (object,), {}))

from harness import beets_harness  # noqa: E402
from tests.test_harness_discogs_neutralize import (  # noqa: E402
    discogs_provider_ids_neutralized,
)


# ============================================================================
# World space
# ============================================================================

_DATA_SOURCES = ("MusicBrainz", "Discogs", "", "iTunes")
_ID_SHAPES = (
    "",  # absent
    "1505049",  # bare-numeric Discogs id
    "11111111-2222-3333-4444-555555555555",  # MusicBrainz-shaped UUID
)


@dataclass(frozen=True)
class _CandidateWorld:
    data_source: str
    album_id: str
    releasegroup_id: str


@st.composite
def _candidate_worlds(draw) -> _CandidateWorld:
    return _CandidateWorld(
        data_source=draw(st.sampled_from(_DATA_SOURCES)),
        album_id=draw(st.sampled_from(_ID_SHAPES)),
        releasegroup_id=draw(st.sampled_from(_ID_SHAPES)),
    )


def _build_candidate(world: _CandidateWorld) -> SimpleNamespace:
    info = SimpleNamespace(
        data_source=world.data_source, album_id=world.album_id,
        releasegroup_id=world.releasegroup_id)
    return SimpleNamespace(info=info)


# ============================================================================
# Invariant checker (module-level so the known-bad self-test can call it
# directly — CLAUDE.md "Bug Hunting — Generated-First" / code-quality.md
# Red/Green TDD)
# ============================================================================

def assert_discogs_neutralize_invariant(
    candidate, original_data_source: str, original_album_id: str,
) -> None:
    """After `_neutralize_discogs_provider_ids(candidate)` has run:

    * `discogs_provider_ids_neutralized(candidate.info)` must be True for
      EVERY candidate, regardless of data_source.
    * a non-Discogs candidate's `album_id` must be byte-identical to what it
      was before neutralization (the must-still-work guard: MusicBrainz
      candidates are never touched).
    * a Discogs candidate must never carry a truthy `album_id` or
      `releasegroup_id` after neutralization (the broader end-invariant —
      restated directly rather than only through the checker function, so a
      bug in the checker itself can't hide a real violation).
    """
    info = candidate.info
    if not discogs_provider_ids_neutralized(info):
        raise AssertionError(
            "discogs_provider_ids_neutralized reports unsafe after "
            f"neutralization: data_source={info.data_source!r} "
            f"album_id={info.album_id!r} "
            f"releasegroup_id={info.releasegroup_id!r}")

    if original_data_source != "Discogs" and info.album_id != original_album_id:
        raise AssertionError(
            f"non-Discogs album_id must remain unchanged: "
            f"data_source={original_data_source!r} "
            f"before={original_album_id!r} after={info.album_id!r}")

    if original_data_source == "Discogs" and (
            info.album_id or info.releasegroup_id):
        raise AssertionError(
            "Discogs info must never carry a truthy album_id/"
            f"releasegroup_id after neutralization: "
            f"album_id={info.album_id!r} "
            f"releasegroup_id={info.releasegroup_id!r}")


# ============================================================================
# Generated property
# ============================================================================

class TestGeneratedDiscogsNeutralizeInvariant(unittest.TestCase):
    """Property: over the full data_source x id-shape world space,
    neutralization always leaves the candidate safe, and never touches a
    non-Discogs candidate's album_id."""

    @given(world=_candidate_worlds())
    @example(world=_CandidateWorld(
        data_source="Discogs", album_id="1505049", releasegroup_id="339103"))
    @example(world=_CandidateWorld(
        data_source="MusicBrainz",
        album_id="11111111-2222-3333-4444-555555555555",
        releasegroup_id=""))
    def test_every_candidate_is_safe_after_neutralization(self, world):
        candidate = _build_candidate(world)
        original_album_id = candidate.info.album_id
        beets_harness._neutralize_discogs_provider_ids(candidate)
        assert_discogs_neutralize_invariant(
            candidate, world.data_source, original_album_id)


# ============================================================================
# Known-bad self-tests
# ============================================================================

class TestDiscogsNeutralizeCheckerTripsOnViolations(unittest.TestCase):
    """Plant violations that skip (or undo) the fix and prove the checker
    trips on each — the discipline that would have caught #570 before it
    shipped."""

    def test_trips_when_discogs_candidate_never_neutralized(self):
        """The exact live-bug shape: a Discogs candidate whose numeric ids
        were never blanked (e.g. the fix wasn't called at all)."""
        candidate = _build_candidate(_CandidateWorld(
            data_source="Discogs", album_id="1505049",
            releasegroup_id="339103"))
        # Deliberately skip calling _neutralize_discogs_provider_ids.
        with self.assertRaises(AssertionError):
            assert_discogs_neutralize_invariant(candidate, "Discogs", "1505049")

    def test_trips_when_only_album_id_was_blanked(self):
        """A partial fix that forgets releasegroup_id must still trip."""
        candidate = _build_candidate(_CandidateWorld(
            data_source="Discogs", album_id="", releasegroup_id="339103"))
        with self.assertRaises(AssertionError):
            assert_discogs_neutralize_invariant(candidate, "Discogs", "1505049")

    def test_trips_when_non_discogs_album_id_mutated(self):
        """A regression that blanks EVERY candidate's album_id (not just
        Discogs) must trip the must-still-work guard."""
        candidate = _build_candidate(_CandidateWorld(
            data_source="MusicBrainz", album_id="", releasegroup_id=""))
        original_album_id = "11111111-2222-3333-4444-555555555555"
        with self.assertRaises(AssertionError):
            assert_discogs_neutralize_invariant(
                candidate, "MusicBrainz", original_album_id)


if __name__ == "__main__":
    unittest.main()
