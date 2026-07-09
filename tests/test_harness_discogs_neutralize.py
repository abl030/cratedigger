#!/usr/bin/env python3
"""Deterministic pin for harness/beets_harness.py's Discogs id neutralizer
(issue #570 defect 2).

INVARIANT: An applied Discogs match leaves `mb_albumid` and
`mb_releasegroupid` empty — never a bare-numeric Discogs id. The identifier
lives ONLY in `discogs_albumid`. MusicBrainz matches keep their UUID
`album_id`/`mb_albumid` unchanged.

Root cause: beets' `AlbumInfo.MEDIA_FIELD_MAP` maps `album_id -> mb_albumid`
and `releasegroup_id -> mb_releasegroupid`. The Discogs plugin fills those
fields with NUMERIC Discogs ids, so an un-neutralized apply writes a bare
integer into `MUSICBRAINZ_ALBUMID` — Jellyfin's `new Guid(tag)` throws
`FormatException` and aborts the album's whole MusicBrainz metadata fetch.

The generated property (over a wider candidate world space) and the
real-beets subprocess contract for the same invariant live in
tests/test_harness_discogs_neutralize_generated.py and
tests/test_harness_beets2_contract.py respectively.

The harness imports `beets` at module top-level, so we mock those modules
in sys.modules before importing it — same preamble as
tests/test_harness_serialization.py.
"""

from __future__ import annotations

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

_beets_mocks = {
    "beets": MagicMock(),
    "beets.config": MagicMock(),
    "beets.library": MagicMock(),
    "beets.plugins": MagicMock(),
    "beets.importer": MagicMock(),
    "beets.importer.actions": MagicMock(),
    "beets.importer.session": MagicMock(),
    "beets.importer.tasks": MagicMock(),
}
for name, mock in _beets_mocks.items():
    sys.modules.setdefault(name, mock)

# ImportSession needs to be a class so subclassing works.
setattr(sys.modules["beets.importer.session"], "ImportSession",
        type("ImportSession", (object,), {}))

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from harness import beets_harness  # noqa: E402


def discogs_provider_ids_neutralized(info) -> bool:
    """True iff `info` is safe to apply: a Discogs AlbumInfo must not carry a
    numeric album_id/releasegroup_id that beets would map into mb_albumid/
    mb_releasegroupid. Non-Discogs infos are always considered safe.

    Test-only invariant checker for `harness.beets_harness
    ._neutralize_discogs_provider_ids` (production never reads this return
    value, so it does not belong in the production module — see issue #570
    review). The single definition lives here; other test modules
    (tests/test_harness_discogs_neutralize_generated.py) import it from this
    module rather than defining a divergent copy.
    """
    if (getattr(info, "data_source", "") or "") != "Discogs":
        return True
    return not (getattr(info, "album_id", "") or "") and not (getattr(info, "releasegroup_id", "") or "")


def _candidate(data_source: str, album_id, releasegroup_id) -> SimpleNamespace:
    """A lightweight AlbumMatch-like stub — the helper is duck-typed and
    only ever reads/writes `.info.data_source` / `.info.album_id` /
    `.info.releasegroup_id`."""
    info = SimpleNamespace(
        data_source=data_source, album_id=album_id,
        releasegroup_id=releasegroup_id)
    return SimpleNamespace(info=info)


class TestNeutralizeDiscogsProviderIds(unittest.TestCase):
    """Pin: a Discogs candidate's numeric ids get blanked; a MusicBrainz
    candidate is left untouched (must-still-work guard)."""

    def test_discogs_candidate_numeric_ids_blanked(self):
        cand = _candidate("Discogs", "1505049", "339103")
        neutralized = beets_harness._neutralize_discogs_provider_ids(cand)
        self.assertTrue(neutralized)
        self.assertEqual(cand.info.album_id, "")
        self.assertEqual(cand.info.releasegroup_id, "")
        self.assertTrue(discogs_provider_ids_neutralized(cand.info))

    def test_musicbrainz_candidate_unchanged(self):
        uuid = "11111111-2222-3333-4444-555555555555"
        cand = _candidate("MusicBrainz", uuid, "")
        neutralized = beets_harness._neutralize_discogs_provider_ids(cand)
        self.assertFalse(neutralized)
        self.assertEqual(cand.info.album_id, uuid)
        self.assertTrue(discogs_provider_ids_neutralized(cand.info))

    def test_missing_info_returns_false(self):
        cand = SimpleNamespace(info=None)
        self.assertFalse(beets_harness._neutralize_discogs_provider_ids(cand))

    def test_blank_data_source_left_untouched(self):
        """An empty data_source (never happens for a real candidate, but the
        helper must not misclassify it as Discogs)."""
        cand = _candidate("", "some-id", "")
        neutralized = beets_harness._neutralize_discogs_provider_ids(cand)
        self.assertFalse(neutralized)
        self.assertEqual(cand.info.album_id, "some-id")


class TestDiscogsProviderIdsNeutralizedChecker(unittest.TestCase):
    """Known-bad self-test: `discogs_provider_ids_neutralized` — the
    invariant checker itself — must trip (return False) on exactly the
    un-neutralized shape that shipped the #570 bug."""

    def test_trips_on_unneutralized_discogs_album_id(self):
        info = SimpleNamespace(
            data_source="Discogs", album_id="1505049", releasegroup_id="")
        self.assertFalse(discogs_provider_ids_neutralized(info))

    def test_trips_on_unneutralized_discogs_releasegroup_id(self):
        info = SimpleNamespace(
            data_source="Discogs", album_id="", releasegroup_id="99999")
        self.assertFalse(discogs_provider_ids_neutralized(info))

    def test_passes_on_neutralized_discogs_info(self):
        info = SimpleNamespace(
            data_source="Discogs", album_id="", releasegroup_id="")
        self.assertTrue(discogs_provider_ids_neutralized(info))

    def test_non_discogs_always_considered_safe(self):
        info = SimpleNamespace(
            data_source="MusicBrainz", album_id="1234", releasegroup_id="5678")
        self.assertTrue(discogs_provider_ids_neutralized(info))


if __name__ == "__main__":
    unittest.main()
