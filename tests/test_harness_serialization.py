#!/usr/bin/env python3
"""Tests for harness/_serialize_*() — wire-boundary type contract.

The harness emits JSON over stdout for consumers in lib/. Every ID-like
field (album_id, releasegroup_id, track_id, release_track_id) MUST be a
str regardless of what beets returns.

Beets' MusicBrainz plugin returns IDs as UUID strings. Beets' Discogs
plugin returns the same fields as integers (because the Discogs API
returns them as JSON numbers). If the harness leaks an int through,
downstream `==` comparisons in lib/beets.py against DB-stored str
mb_release_ids fail silently — the live "mbid_not_found" bug.

The harness imports `beets` at module top-level so we mock those modules
before importing it.
"""

from __future__ import annotations

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock


# beets isn't installed in the test nix-shell — mock it before import.
_beets_mocks = {
    "beets": MagicMock(),
    "beets.config": MagicMock(),
    "beets.library": MagicMock(),
    "beets.plugins": MagicMock(),
    "beets.importer": MagicMock(),
    "beets.importer.session": MagicMock(),
    "beets.importer.tasks": MagicMock(),
    "beets.ui": MagicMock(),
}
for name, mock in _beets_mocks.items():
    sys.modules.setdefault(name, mock)

# ImportSession needs to be a class so subclassing works.
setattr(sys.modules["beets.importer.session"], "ImportSession",
        type("ImportSession", (object,), {}))

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from harness import beets_harness  # noqa: E402


# ---------------------------------------------------------------------------
# _serialize_album_candidate — AlbumInfo IDs (album_id, releasegroup_id)
# ---------------------------------------------------------------------------

class TestAlbumCandidateIdCoercion(unittest.TestCase):
    """Album-level IDs must always emit as str on the JSON wire."""

    def _candidate(self, **info_overrides):
        """Build a fake AlbumMatch with the given AlbumInfo attributes."""
        info_attrs = dict(
            artist="Test Artist", album="Test Album",
            album_id="default-id", albumdisambig="",
            year=2020, original_year=2020, country="US",
            label="", catalognum="", media="", mediums=1,
            albumtype="", albumtypes=[], albumstatus="Official",
            releasegroup_id="default-rg-id", release_group_title="",
            va=False, language=None, script=None,
            data_source="MusicBrainz", barcode="", asin="",
            tracks=[],
        )
        info_attrs.update(info_overrides)
        info = SimpleNamespace(**info_attrs)

        # Minimal AlbumMatch shape — distance, info, mapping, extras
        distance = MagicMock()
        distance.__float__ = lambda _: 0.05
        distance.items = lambda: [("album", 0.0)]
        candidate = SimpleNamespace(
            distance=distance, info=info,
            mapping={}, extra_items=[], extra_tracks=[],
        )
        # `float(candidate.distance)` is called inside the serializer;
        # supply it via the magic method.
        return candidate

    def test_int_album_id_emitted_as_str(self):
        """Discogs plugin gives album_id as int → wire format must be str."""
        cand = self._candidate(album_id=2085134)
        out = beets_harness._serialize_album_candidate(0, cand)
        self.assertEqual(out["album_id"], "2085134")
        self.assertIsInstance(out["album_id"], str)

    def test_str_album_id_unchanged(self):
        """MusicBrainz UUID stays as-is."""
        uuid = "f100b6b0-6daa-4c9b-b33a-3e14c564cf58"
        cand = self._candidate(album_id=uuid)
        out = beets_harness._serialize_album_candidate(0, cand)
        self.assertEqual(out["album_id"], uuid)
        self.assertIsInstance(out["album_id"], str)

    def test_none_album_id_becomes_empty_string(self):
        cand = self._candidate(album_id=None)
        out = beets_harness._serialize_album_candidate(0, cand)
        self.assertEqual(out["album_id"], "")

    def test_int_releasegroup_id_emitted_as_str(self):
        cand = self._candidate(releasegroup_id=339103)
        out = beets_harness._serialize_album_candidate(0, cand)
        self.assertEqual(out["releasegroup_id"], "339103")
        self.assertIsInstance(out["releasegroup_id"], str)

    def test_none_releasegroup_id_becomes_empty_string(self):
        cand = self._candidate(releasegroup_id=None)
        out = beets_harness._serialize_album_candidate(0, cand)
        self.assertEqual(out["releasegroup_id"], "")


# ---------------------------------------------------------------------------
# _serialize_track_info — TrackInfo IDs (track_id, release_track_id)
# ---------------------------------------------------------------------------

class TestTrackInfoIdCoercion(unittest.TestCase):

    def _track_info(self, **overrides):
        attrs = dict(
            title="X", artist="A", index=1, medium=1, medium_index=1,
            medium_total=1, length=200.0,
            track_id="default-tid", release_track_id="default-rtid",
            track_alt=None, disctitle=None, data_source="MusicBrainz",
        )
        attrs.update(overrides)
        return SimpleNamespace(**attrs)

    def test_int_track_id_emitted_as_str(self):
        ti = self._track_info(track_id=12345678)
        out = beets_harness._serialize_track_info(ti)
        self.assertEqual(out["track_id"], "12345678")
        self.assertIsInstance(out["track_id"], str)

    def test_int_release_track_id_emitted_as_str(self):
        ti = self._track_info(release_track_id=87654321)
        out = beets_harness._serialize_track_info(ti)
        self.assertEqual(out["release_track_id"], "87654321")
        self.assertIsInstance(out["release_track_id"], str)

    def test_str_track_id_unchanged(self):
        ti = self._track_info(track_id="abc-tid-1")
        out = beets_harness._serialize_track_info(ti)
        self.assertEqual(out["track_id"], "abc-tid-1")

    def test_none_track_ids_become_empty_string(self):
        ti = self._track_info(track_id=None, release_track_id=None)
        out = beets_harness._serialize_track_info(ti)
        self.assertEqual(out["track_id"], "")
        self.assertEqual(out["release_track_id"], "")


# ---------------------------------------------------------------------------
# _serialize_track_candidate — singleton TrackMatch IDs
# ---------------------------------------------------------------------------

class TestTrackCandidateIdCoercion(unittest.TestCase):

    def _track_candidate(self, **info_overrides):
        info_attrs = dict(
            title="X", artist="A", track_id="default-tid", length=200.0,
        )
        info_attrs.update(info_overrides)
        info = SimpleNamespace(**info_attrs)
        distance = MagicMock()
        distance.__float__ = lambda _: 0.05
        return SimpleNamespace(distance=distance, info=info)

    def test_int_track_id_emitted_as_str(self):
        cand = self._track_candidate(track_id=12345678)
        out = beets_harness._serialize_track_candidate(0, cand)
        self.assertEqual(out["track_id"], "12345678")
        self.assertIsInstance(out["track_id"], str)

    def test_str_track_id_unchanged(self):
        cand = self._track_candidate(track_id="rec-uuid-abc")
        out = beets_harness._serialize_track_candidate(0, cand)
        self.assertEqual(out["track_id"], "rec-uuid-abc")

    def test_none_track_id_becomes_empty_string(self):
        cand = self._track_candidate(track_id=None)
        out = beets_harness._serialize_track_candidate(0, cand)
        self.assertEqual(out["track_id"], "")


if __name__ == "__main__":
    unittest.main()
