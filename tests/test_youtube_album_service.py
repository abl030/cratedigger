"""Tests for ``lib.youtube_album_service.resolve_youtube_album``.

Covers the resolve flow end-to-end against ``FakeYTMusic`` +
``FakePipelineDB`` + injected MB / Discogs lookup lambdas. The
integration slice at the bottom exercises the real
``compute_beets_distance`` against real beets primitives so the N×M
scoring layer is proven correct without coupling the unit tests to it.

Outcome vocabulary is pinned via ``test_outcome_set_is_stable`` per
``.claude/rules/code-quality.md``.
"""

from __future__ import annotations

import unittest
from typing import Any, Callable, Optional
from unittest.mock import patch

import msgspec
import requests
from ytmusicapi.exceptions import YTMusicServerError, YTMusicUserError

from lib.beets_distance import (
    BeetsDistanceResult,
    SyntheticItem,
    compute_beets_distance,
)
from lib.youtube_album_service import (
    SERVICE_OUTCOMES,
    ResolvedDistance,
    ResolvedYoutubeRelease,
    YoutubeAlbumResolverResult,
    resolve_youtube_album,
)
from tests.fakes import FakePipelineDB, FakeYTMusic


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

# UUID-shaped MB identifiers (must satisfy ``detect_release_source``).
MB_RG = "11111111-1111-1111-1111-111111111111"
MB_REL_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
MB_REL_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
MB_REL_C = "cccccccc-cccc-cccc-cccc-cccccccccccc"
MB_RG_MISSING = "22222222-2222-2222-2222-222222222222"
MB_NO_RG = "33333333-3333-3333-3333-333333333333"


def _ok_mb_release(
    *,
    mbid: str,
    rg: str = MB_RG,
    title: str = "Dr. Octagonecologyst",
    artist: str = "Dr. Octagon",
    year: Optional[int] = 1996,
    tracks: Optional[list[dict]] = None,
) -> dict:
    return {
        "id": mbid,
        "title": title,
        "artist_name": artist,
        "artist_id": "artist-1",
        "release_group_id": rg,
        "date": f"{year}-01-01" if year is not None else "",
        "year": year,
        "country": "US",
        "status": "Official",
        "tracks": tracks if tracks is not None else [
            {"disc_number": 1, "track_number": 1,
             "title": "Intro", "length_seconds": 60.0},
            {"disc_number": 1, "track_number": 2,
             "title": "3000",  "length_seconds": 180.0},
        ],
    }


def _ok_mb_rg_releases(*release_ids_with_year: tuple[str, Optional[int]]) -> dict:
    """Build a slim release-group-releases payload mirroring web/mb.py."""
    return {
        "title": "Dr. Octagonecologyst",
        "type": "Album",
        "releases": [
            {
                "id": rid,
                "title": "Dr. Octagonecologyst",
                "date": f"{y}-01-01" if y is not None else "",
                "country": "US",
                "status": "Official",
                "track_count": 2,
                "format": "CD",
                "media_count": 1,
            }
            for rid, y in release_ids_with_year
        ],
    }


def _yt_search_album_result(
    browse_id: str,
    *,
    title: str = "Dr. Octagonecologyst",
    artists: Optional[list[dict]] = None,
    year: Optional[str] = "1996",
    track_count: int = 2,
) -> dict:
    """One entry in ``YTMusic.search(filter='albums')`` results."""
    return {
        "browseId": browse_id,
        "resultType": "album",
        "title": title,
        "artists": artists or [{"name": "Dr. Octagon", "id": "UCx"}],
        "year": year,
        "type": "Album",
        "thumbnails": [],
        "isExplicit": False,
        "playlistId": None,
        "trackCount": track_count,
    }


def _yt_tracks(titles: list[str], *, duration_seconds: int = 60) -> list[dict]:
    """Synthesize ytmusicapi-shaped track entries."""
    return [
        {
            "videoId": f"vid-{i}",
            "title": t,
            "artists": [{"name": "Dr. Octagon", "id": "UCx"}],
            "album": {"name": "Dr. Octagonecologyst", "id": "MPREb-na"},
            "duration": f"{duration_seconds // 60}:{duration_seconds % 60:02d}",
            "duration_seconds": duration_seconds,
            "trackNumber": i + 1,
            "isAvailable": True,
            "isExplicit": False,
        }
        for i, t in enumerate(titles)
    ]


def _yt_other_version(browse_id: str, *, year: str = "1996",
                      title: str = "Dr. Octagonecologyst") -> dict:
    return {
        "browseId": browse_id,
        "title": title,
        "artists": [{"name": "Dr. Octagon", "id": "UCx"}],
        "year": year,
        "thumbnails": [],
        "isExplicit": False,
    }


def _canned_distance(
    *,
    outcome: str = "ok",
    distance: Optional[float] = 0.12,
    components: Optional[dict[str, float]] = None,
    candidate_mbid: Optional[str] = None,
    error_message: Optional[str] = None,
) -> Callable[..., BeetsDistanceResult]:
    """Build a ``distance_fn`` stub that always returns the same result.

    For per-pair tests we map by ``mbid`` so different MBIDs can surface
    different outcomes — see ``_canned_distance_by_mbid``.
    """
    if components is None:
        components = {"tracks": 0.05, "album": 0.07}

    def _fn(*, mbid: str, **_: Any) -> BeetsDistanceResult:
        return BeetsDistanceResult(
            outcome=outcome,
            distance=distance,
            matched_tracks=2,
            total_local_tracks=2,
            total_mb_tracks=2,
            extra_local_tracks=0,
            extra_mb_tracks=0,
            components=dict(components),
            candidate_mbid=candidate_mbid or mbid,
            candidate_release_group_id=MB_RG,
            request_release_group_id=MB_RG,
            error_message=error_message,
        )
    return _fn


def _canned_distance_by_mbid(
    overrides: dict[str, dict[str, Any]],
) -> Callable[..., BeetsDistanceResult]:
    """Distance stub that returns different results per candidate MBID."""
    def _fn(*, mbid: str, **_: Any) -> BeetsDistanceResult:
        cfg = overrides.get(mbid, {"outcome": "ok", "distance": 0.1})
        return BeetsDistanceResult(
            outcome=cfg.get("outcome", "ok"),
            distance=cfg.get("distance"),
            matched_tracks=cfg.get("matched_tracks", 2),
            total_local_tracks=cfg.get("total_local_tracks", 2),
            total_mb_tracks=cfg.get("total_mb_tracks", 2),
            extra_local_tracks=cfg.get("extra_local_tracks", 0),
            extra_mb_tracks=cfg.get("extra_mb_tracks", 0),
            components=cfg.get("components"),
            candidate_mbid=mbid,
            candidate_release_group_id=cfg.get("candidate_release_group_id"),
            request_release_group_id=MB_RG,
            error_message=cfg.get("error_message"),
        )
    return _fn


class _LookupSpy:
    """Helper to wrap a dict-of-id-to-payload as a tracked callable."""

    def __init__(self, table: dict[str, Optional[dict]]):
        self._table = table
        self.calls: list[str] = []

    def __call__(self, identifier: str) -> Optional[dict]:
        self.calls.append(identifier)
        return self._table.get(identifier)


def _empty_lookup() -> _LookupSpy:
    """Lookup that always returns None (404 simulation)."""
    return _LookupSpy({})


# Tests pass this in place of the production ``time.sleep`` so the
# jitter helper between consecutive ``get_album`` calls doesn't
# actually pause the suite by 1-3s per YT sibling.
def _noop_sleep(_seconds: float) -> None:
    return None


# Module-level patch: the default ``sleep_fn`` on
# ``resolve_youtube_album`` is ``time.sleep`` via
# ``_default_jitter_sleep_fn``. Replacing the module's ``time.sleep``
# binding with a no-op means tests that don't explicitly pass
# ``sleep_fn=`` still skip the 1-3s pauses. ``time.sleep`` is a leaf
# seam per the mock-audit allowlist.
_TIME_SLEEP_PATCH = patch("lib.youtube_album_service.time.sleep", lambda _s: None)


def setUpModule() -> None:
    _TIME_SLEEP_PATCH.start()


def tearDownModule() -> None:
    _TIME_SLEEP_PATCH.stop()


# ---------------------------------------------------------------------------
# Outcome vocabulary contract test
# ---------------------------------------------------------------------------


class TestServiceOutcomeContract(unittest.TestCase):
    """Pin the service-level outcome frozenset — wire contract for CLI + API."""

    def test_outcome_set_is_stable(self) -> None:
        self.assertEqual(
            set(SERVICE_OUTCOMES),
            {
                "ok",
                "not_found",
                "mb_no_release_group",
                "unresolved_4xx_client",
                "unresolved_mirror_unavailable",
                "unresolved_timeout",
                "youtube_parse_failed",
                "transient",
            },
        )
        # Frozenset (immutable) prevents accidental mutation downstream.
        self.assertIsInstance(SERVICE_OUTCOMES, frozenset)


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


class TestResolveYoutubeAlbumHappyPath(unittest.TestCase):
    """AE1: 3 MB siblings × 2 YT siblings → 6 distance entries."""

    def _build_yt(
        self,
        seed_browse: str = "MPREb-seed",
        other_browse: str = "MPREb-other",
    ) -> FakeYTMusic:
        yt = FakeYTMusic()
        # Search returns one top result matching the seed.
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result(seed_browse, year="1996", track_count=2)],
        )
        # Seed album response contains other_versions.
        yt.set_album(
            seed_browse,
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-seed",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="1996",
                tracks=_yt_tracks(["Intro", "3000"]),
                other_versions=[_yt_other_version(other_browse, year="2008")],
            ),
        )
        # Each sibling has its own get_album response.
        yt.set_album(
            other_browse,
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-other",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="2008",
                tracks=_yt_tracks(["Intro", "3000"]),
            ),
        )
        return yt

    def test_release_group_input_returns_full_matrix(self) -> None:
        """Input is an MB release-group MBID; auto-widen via leaf-then-group."""
        rg = MB_RG
        # Leaf call returns None (RG MBID isn't a release).
        mb_leaf = _LookupSpy({rg: None})
        mb_group = _LookupSpy({
            rg: _ok_mb_rg_releases((MB_REL_A, 1996), (MB_REL_B, 2000), (MB_REL_C, 2008)),
        })
        # Per-sibling release fetch.
        mb_release_lookup = _LookupSpy({
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
            MB_REL_B: _ok_mb_release(mbid=MB_REL_B, rg=rg, year=2000),
            MB_REL_C: _ok_mb_release(mbid=MB_REL_C, rg=rg, year=2008),
        })

        # Service uses mb_leaf for the kind-disambiguation + per-sibling
        # fetches. We layer them via a combined spy.
        combined = _LookupSpy({**mb_leaf._table, **mb_release_lookup._table})

        pdb = FakePipelineDB()
        yt = self._build_yt()
        result = resolve_youtube_album(
            rg,
            pdb=pdb,
            mb_get_release=combined,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )

        self.assertEqual(result.outcome, "ok")
        self.assertEqual(result.source, "mb")
        self.assertEqual(result.release_group_identifier, rg)
        self.assertFalse(result.from_cache)
        self.assertEqual(len(result.youtube_releases), 2)
        # Each YT release has one distance per MB sibling (3 each).
        for yt_rel in result.youtube_releases:
            self.assertEqual(len(yt_rel.distances), 3)
            mbids = {d.mbid for d in yt_rel.distances}
            self.assertEqual(mbids, {MB_REL_A, MB_REL_B, MB_REL_C})
            for d in yt_rel.distances:
                self.assertEqual(d.outcome, "ok")
        # Cache was persisted.
        cached_rows = pdb.get_youtube_album_mapping(rg, "mb")
        assert cached_rows is not None
        self.assertEqual(len(cached_rows), 2)

    def test_ae3_release_level_mbid_auto_widens(self) -> None:
        """AE3: Input is a release-level MBID; service extracts RG from it."""
        rg = MB_RG
        mb_leaf = _LookupSpy({
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg),
            MB_REL_B: _ok_mb_release(mbid=MB_REL_B, rg=rg),
        })
        mb_group = _LookupSpy({
            rg: _ok_mb_rg_releases((MB_REL_A, 1996), (MB_REL_B, 2008)),
        })
        yt = self._build_yt()
        pdb = FakePipelineDB()
        result = resolve_youtube_album(
            MB_REL_A,
            pdb=pdb,
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )

        self.assertEqual(result.outcome, "ok")
        self.assertEqual(result.release_group_identifier, rg)
        # Service hit the leaf first, then the group fetch.
        self.assertIn(MB_REL_A, mb_leaf.calls)
        self.assertIn(rg, mb_group.calls)

    def test_ae4_discogs_release_input_auto_widens_to_master(self) -> None:
        """AE4: Input is a Discogs release ID; auto-widen to master."""
        master_id = "12345"
        discogs_leaf = _LookupSpy({
            "67890": {
                "id": "67890",
                "title": "Dr. Octagonecologyst",
                "artist_name": "Dr. Octagon",
                "artist_id": "1",
                "release_group_id": master_id,
                "date": "1996",
                "year": 1996,
                "country": "US",
                "status": "Official",
                "tracks": [
                    {"disc_number": 1, "track_number": 1,
                     "title": "Intro", "length_seconds": 60.0},
                    {"disc_number": 1, "track_number": 2,
                     "title": "3000",  "length_seconds": 180.0},
                ],
            },
            "98765": {
                "id": "98765",
                "title": "Dr. Octagonecologyst",
                "artist_name": "Dr. Octagon",
                "artist_id": "1",
                "release_group_id": master_id,
                "date": "2008",
                "year": 2008,
                "country": "US",
                "status": "Official",
                "tracks": [
                    {"disc_number": 1, "track_number": 1,
                     "title": "Intro", "length_seconds": 60.0},
                    {"disc_number": 1, "track_number": 2,
                     "title": "3000",  "length_seconds": 180.0},
                ],
            },
        })
        discogs_master = _LookupSpy({
            master_id: {
                "title": "Dr. Octagonecologyst",
                "type": "Album",
                "releases": [
                    {"id": "67890", "title": "Dr. Octagonecologyst",
                     "date": "1996", "country": "US", "status": "Official",
                     "track_count": 2, "format": "CD", "media_count": 1},
                    {"id": "98765", "title": "Dr. Octagonecologyst",
                     "date": "2008", "country": "US", "status": "Official",
                     "track_count": 2, "format": "CD", "media_count": 1},
                ],
            },
        })
        yt = self._build_yt()
        pdb = FakePipelineDB()
        result = resolve_youtube_album(
            "67890",  # Discogs release ID
            pdb=pdb,
            mb_get_release=_empty_lookup(),
            mb_get_release_group_releases=_empty_lookup(),
            discogs_get_release=discogs_leaf,
            discogs_get_master_releases=discogs_master,
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )

        self.assertEqual(result.outcome, "ok")
        self.assertEqual(result.source, "discogs")
        self.assertEqual(result.release_group_identifier, master_id)

    def test_auto_widen_mb_release_group_mbid(self) -> None:
        """Leaf returns None, fall back to group fetch directly."""
        rg = MB_RG
        mb_leaf = _LookupSpy({rg: None})  # leaf 404
        mb_group = _LookupSpy({
            rg: _ok_mb_rg_releases((MB_REL_A, 1996)),
        })
        combined_leaf = _LookupSpy({**mb_leaf._table,
                                    MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg)})

        yt = self._build_yt()
        pdb = FakePipelineDB()
        result = resolve_youtube_album(
            rg,
            pdb=pdb,
            mb_get_release=combined_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )

        self.assertEqual(result.outcome, "ok")
        # Service fell through to the group path.
        self.assertEqual(mb_group.calls, [rg])

    def test_auto_widen_discogs_master_id(self) -> None:
        master_id = "999"
        discogs_master = _LookupSpy({
            master_id: {
                "title": "Album",
                "type": "Album",
                "releases": [
                    {"id": "100", "title": "Album",
                     "date": "2000", "country": "US", "status": "Official",
                     "track_count": 2, "format": "CD", "media_count": 1},
                ],
            },
        })
        # Per-sibling lookups for the inner loop.
        combined_leaf = _LookupSpy({
            master_id: None,  # leaf miss
            "100": {
                "id": "100",
                "title": "Album",
                "artist_name": "Dr. Octagon",
                "artist_id": "1",
                "release_group_id": master_id,
                "date": "2000",
                "year": 2000,
                "country": "US",
                "status": "Official",
                "tracks": [
                    {"disc_number": 1, "track_number": 1,
                     "title": "Intro", "length_seconds": 60.0},
                    {"disc_number": 1, "track_number": 2,
                     "title": "3000",  "length_seconds": 180.0},
                ],
            },
        })

        yt = self._build_yt()
        pdb = FakePipelineDB()
        result = resolve_youtube_album(
            master_id,
            pdb=pdb,
            mb_get_release=_empty_lookup(),
            mb_get_release_group_releases=_empty_lookup(),
            discogs_get_release=combined_leaf,
            discogs_get_master_releases=discogs_master,
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )

        self.assertEqual(result.outcome, "ok")
        self.assertEqual(result.source, "discogs")
        self.assertEqual(discogs_master.calls, [master_id])

    def test_auto_widen_neither_leaf_nor_group_resolves(self) -> None:
        """Both leaf and group return None → not_found."""
        rg = MB_RG_MISSING
        pdb = FakePipelineDB()
        yt = FakeYTMusic()
        result = resolve_youtube_album(
            rg,
            pdb=pdb,
            mb_get_release=_LookupSpy({rg: None}),
            mb_get_release_group_releases=_LookupSpy({rg: None}),
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "not_found")
        self.assertEqual(len(result.youtube_releases), 0)
        # Never touched YT Music.
        self.assertEqual(yt.search_calls, [])
        self.assertEqual(yt.get_album_calls, [])

    def test_mb_leaf_raises_http_error_falls_through_to_group_path(self) -> None:
        """Finding #1: passing an MB release-group MBID through
        ``web.mb.get_release`` raises ``urllib.error.HTTPError`` because
        RG MBIDs aren't releases. The resolver must catch and fall
        through to the group-releases endpoint, not 500.
        """
        import urllib.error

        rg = MB_RG

        def _raising_mb_leaf(identifier: str) -> Optional[dict]:
            # AE3 mirrors the live behaviour: web.mb.get_release raises
            # urllib.error.HTTPError when handed a non-release MBID.
            raise urllib.error.HTTPError(
                f"http://mb-mirror/release/{identifier}",
                404, "Not Found", {}, None,  # type: ignore[arg-type]
            )

        mb_group = _LookupSpy({
            rg: _ok_mb_rg_releases((MB_REL_A, 1996)),
        })
        # The mb_get_release sibling lookup also gets the raising
        # contract — when the resolver re-uses it for per-sibling
        # fetches, the leaf-miss-tolerant path treats raises as misses.
        # Successful sibling fetches must still return real records,
        # so we wrap with a per-identifier dispatch.
        sibling_records = {
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
        }
        leaf_calls: list[str] = []

        def _dispatching_mb_leaf(identifier: str) -> Optional[dict]:
            leaf_calls.append(identifier)
            if identifier == rg:
                # The RG MBID raises like the real adapter.
                raise urllib.error.HTTPError(
                    f"http://mb-mirror/release/{identifier}",
                    404, "Not Found", {}, None,  # type: ignore[arg-type]
                )
            return sibling_records.get(identifier)

        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result("MPREb-seed", year="1996", track_count=2)],
        )
        yt.set_album(
            "MPREb-seed",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-seed",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="1996",
                tracks=_yt_tracks(["Intro", "3000"]),
                other_versions=[],
            ),
        )

        # Confirm the assertion above: _raising_mb_leaf actually raises.
        with self.assertRaises(urllib.error.HTTPError):
            _raising_mb_leaf(rg)

        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=_dispatching_mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        # The resolver fell through to the group path cleanly.
        self.assertEqual(result.outcome, "ok",
                         msg=f"expected ok fall-through, got "
                             f"{result.outcome}: {result.error_message}")
        self.assertEqual(result.release_group_identifier, rg)
        # The leaf was tried (raised) AND the group fetch succeeded.
        self.assertIn(rg, leaf_calls)
        self.assertEqual(mb_group.calls, [rg])

    def test_mb_group_lookup_raises_url_error_falls_through_to_not_found(self) -> None:
        """When the group-level fetch raises (mirror down), the auto-widen
        treats it as a miss and returns not_found rather than 500.
        """
        import urllib.error

        rg = MB_RG_MISSING

        def _raising_mb_leaf(identifier: str) -> Optional[dict]:
            raise urllib.error.HTTPError(
                f"http://mb-mirror/release/{identifier}",
                404, "Not Found", {}, None,  # type: ignore[arg-type]
            )

        def _raising_group(rg_id: str) -> Optional[dict]:
            raise urllib.error.URLError("mirror unreachable")

        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=_raising_mb_leaf,
            mb_get_release_group_releases=_raising_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=FakeYTMusic(),
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "not_found")

    def test_discogs_leaf_raises_value_error_falls_through_cleanly(self) -> None:
        """The Discogs adapter does ``int(d)`` on the identifier; a UUID
        pasted into the Discogs path would raise ``ValueError``. The
        resolver must treat that as a miss, not a 500.
        """
        master_id = "999"

        def _raising_discogs_leaf(_identifier: str) -> Optional[dict]:
            raise ValueError("invalid literal for int()")

        discogs_master = _LookupSpy({
            master_id: {
                "title": "Album",
                "type": "Album",
                "releases": [
                    {"id": "100", "title": "Album",
                     "date": "2000", "country": "US", "status": "Official",
                     "track_count": 2, "format": "CD", "media_count": 1},
                ],
            },
        })
        per_id = {
            "100": {
                "id": "100",
                "title": "Album",
                "artist_name": "Dr. Octagon",
                "artist_id": "1",
                "release_group_id": master_id,
                "date": "2000",
                "year": 2000,
                "country": "US",
                "status": "Official",
                "tracks": [
                    {"disc_number": 1, "track_number": 1,
                     "title": "Intro", "length_seconds": 60.0},
                ],
            },
        }

        def _dispatching_leaf(identifier: str) -> Optional[dict]:
            if identifier == master_id:
                raise ValueError("invalid literal for int()")
            return per_id.get(identifier)

        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Album",
            [_yt_search_album_result("MPREb-master", year="2000", track_count=1)],
        )
        yt.set_album(
            "MPREb-master",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-master",
                title="Album",
                artists=[{"name": "Dr. Octagon"}],
                year="2000",
                tracks=_yt_tracks(["Intro"]),
                other_versions=[],
            ),
        )

        # Confirm the leaf raises on the master_id.
        with self.assertRaises(ValueError):
            _raising_discogs_leaf(master_id)

        result = resolve_youtube_album(
            master_id,
            pdb=FakePipelineDB(),
            mb_get_release=_empty_lookup(),
            mb_get_release_group_releases=_empty_lookup(),
            discogs_get_release=_dispatching_leaf,
            discogs_get_master_releases=discogs_master,
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "ok",
                         msg=f"expected ok fall-through, got "
                             f"{result.outcome}: {result.error_message}")
        self.assertEqual(result.source, "discogs")
        self.assertEqual(result.release_group_identifier, master_id)


# ---------------------------------------------------------------------------
# Empty / not-found / mb_no_release_group
# ---------------------------------------------------------------------------


class TestEmptyAndNotFound(unittest.TestCase):

    def test_ae2_yt_search_empty_returns_ok_empty_matrix(self) -> None:
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg),
        })
        mb_group = _LookupSpy({rg: _ok_mb_rg_releases((MB_REL_A, 2000))})
        yt = FakeYTMusic()
        yt.set_search("Dr. Octagon Dr. Octagonecologyst", [])
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "ok")
        self.assertEqual(result.youtube_releases, [])

    def test_mb_no_release_group_at_input(self) -> None:
        """Leaf returns a release but it has no release_group_id."""
        mbid = MB_NO_RG
        mb_leaf = _LookupSpy({
            mbid: {
                "id": mbid,
                "title": "Whatever",
                "artist_name": "X",
                "artist_id": None,
                "release_group_id": None,
                "year": None,
                "tracks": [],
            },
        })
        result = resolve_youtube_album(
            mbid,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=_empty_lookup(),
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=FakeYTMusic(),
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "mb_no_release_group")


# ---------------------------------------------------------------------------
# Cache behavior
# ---------------------------------------------------------------------------


class TestCacheBehavior(unittest.TestCase):

    def _full_stack(self, pdb: FakePipelineDB, yt: FakeYTMusic,
                    *, refresh: bool = False) -> YoutubeAlbumResolverResult:
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
            MB_REL_B: _ok_mb_release(mbid=MB_REL_B, rg=rg, year=2008),
        })
        mb_group = _LookupSpy({
            rg: _ok_mb_rg_releases((MB_REL_A, 1996), (MB_REL_B, 2008)),
        })
        return resolve_youtube_album(
            rg,
            pdb=pdb,
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
            refresh=refresh,
        )

    def _make_yt(self) -> FakeYTMusic:
        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result("MPREb-seed")],
        )
        yt.set_album(
            "MPREb-seed",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-seed",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="1996",
                tracks=_yt_tracks(["Intro", "3000"]),
                other_versions=[],
            ),
        )
        return yt

    def test_ae5_second_call_returns_cached_with_zero_yt_traffic(self) -> None:
        pdb = FakePipelineDB()
        yt1 = self._make_yt()
        r1 = self._full_stack(pdb, yt1)
        self.assertEqual(r1.outcome, "ok")
        self.assertFalse(r1.from_cache)
        self.assertGreater(len(yt1.search_calls), 0)

        yt2 = self._make_yt()  # fresh fake — no canned data needed
        r2 = self._full_stack(pdb, yt2)
        self.assertEqual(r2.outcome, "ok")
        self.assertTrue(r2.from_cache)
        # YT was not touched on the cache-hit path.
        self.assertEqual(yt2.search_calls, [])
        self.assertEqual(yt2.get_album_calls, [])
        # Matrix preserved through the cache round-trip.
        self.assertEqual(len(r2.youtube_releases), len(r1.youtube_releases))

    def test_refresh_bypasses_cache_and_requeries_yt(self) -> None:
        pdb = FakePipelineDB()
        yt1 = self._make_yt()
        self._full_stack(pdb, yt1)

        yt2 = self._make_yt()
        r2 = self._full_stack(pdb, yt2, refresh=True)
        self.assertEqual(r2.outcome, "ok")
        self.assertFalse(r2.from_cache)
        self.assertGreater(len(yt2.search_calls), 0)

    def test_empty_search_result_caches_empty_matrix_no_repoll(self) -> None:
        """Finding #3: an empty YT search result must be cached as
        ``[]`` and short-circuit on the next resolve. Previously the
        falsy ``cached_rows`` gate ignored ``[]`` and re-polled YT
        every time, defeating R14 for empty-search release groups.
        """
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
        })
        mb_group = _LookupSpy({rg: _ok_mb_rg_releases((MB_REL_A, 1996))})

        yt1 = FakeYTMusic()
        yt1.set_search("Dr. Octagon Dr. Octagonecologyst", [])
        pdb = FakePipelineDB()

        r1 = resolve_youtube_album(
            rg,
            pdb=pdb,
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt1,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(r1.outcome, "ok")
        self.assertEqual(r1.youtube_releases, [])
        # Empty matrix is persisted: contract says get_youtube_album_mapping
        # returns [] (not None) on the next read.
        self.assertEqual(pdb.get_youtube_album_mapping(rg, "mb"), [])

        # Second resolve: the cache gate should fire on the empty list
        # and the resolver should NOT re-call YT.
        yt2 = FakeYTMusic()
        # No canned search response — if the resolver tries to call,
        # ``FakeYTMusic.search`` will raise / return missing data.
        r2 = resolve_youtube_album(
            rg,
            pdb=pdb,
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt2,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(r2.outcome, "ok")
        self.assertTrue(r2.from_cache,
                        msg="empty matrix should be served from cache, "
                            "not re-fetched from YT")
        self.assertEqual(r2.youtube_releases, [])
        # Zero YT traffic on the second resolve.
        self.assertEqual(yt2.search_calls, [])
        self.assertEqual(yt2.get_album_calls, [])

    def test_get_youtube_album_mapping_distinguishes_none_from_empty(self) -> None:
        """Contract: FakePipelineDB (and the real PipelineDB) must
        return ``None`` for an unresolved pair and ``[]`` for a pair
        that was resolved to an empty matrix. Finding #3 hinges on
        this distinction.
        """
        pdb = FakePipelineDB()
        self.assertIsNone(pdb.get_youtube_album_mapping("never-resolved", "mb"))
        pdb.upsert_youtube_album_mapping("resolved-to-empty", "mb", [])
        self.assertEqual(
            pdb.get_youtube_album_mapping("resolved-to-empty", "mb"), [])


class TestJitterBetweenSiblingGetAlbumCalls(unittest.TestCase):
    """Finding #2: 1-3s jitter between consecutive ``get_album`` calls.

    The seed ``get_album`` doesn't jitter (it's the first YT call after
    search); each additional sibling fetch sleeps once before the
    request. For N total YT siblings, ``sleep_fn`` is called N-1 times.
    """

    def _resolve_with_n_siblings(
        self, *, n_siblings: int,
        sleep_fn: Callable[[float], None],
    ) -> YoutubeAlbumResolverResult:
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
        })
        mb_group = _LookupSpy({rg: _ok_mb_rg_releases((MB_REL_A, 1996))})

        seed_browse = "MPREb-seed"
        other_versions = [
            _yt_other_version(f"MPREb-other-{i}", year="2008")
            for i in range(n_siblings - 1)
        ]

        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result(seed_browse)],
        )
        yt.set_album(
            seed_browse,
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-seed",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="1996",
                tracks=_yt_tracks(["Intro", "3000"]),
                other_versions=other_versions,
            ),
        )
        for i in range(n_siblings - 1):
            yt.set_album(
                f"MPREb-other-{i}",
                FakeYTMusic.make_album_fixture(
                    audio_playlist_id=f"OLAK5uy-other-{i}",
                    title="Dr. Octagonecologyst",
                    artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                    year="2008",
                    tracks=_yt_tracks(["Intro", "3000"]),
                ),
            )

        return resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
            sleep_fn=sleep_fn,
        )

    def test_jitter_fires_once_per_extra_sibling(self) -> None:
        # 4 YT siblings (seed + 3 others) → 3 jitter sleeps.
        sleeps: list[float] = []

        def _spy(seconds: float) -> None:
            sleeps.append(seconds)

        r = self._resolve_with_n_siblings(n_siblings=4, sleep_fn=_spy)
        self.assertEqual(r.outcome, "ok",
                         msg=f"expected ok, got {r.outcome}: {r.error_message}")
        self.assertEqual(len(r.youtube_releases), 4)
        # Seed get_album is the first YT call after search; only the 3
        # subsequent get_albums pay the jitter.
        self.assertEqual(len(sleeps), 3,
                         msg=f"expected 3 jitter sleeps (N-1 for N=4 siblings), "
                             f"got {len(sleeps)} (durations={sleeps})")
        # Each sleep duration in the 1-3s band per Key Technical Decisions.
        for d in sleeps:
            self.assertGreaterEqual(d, 1.0)
            self.assertLessEqual(d, 3.0)

    def test_single_sibling_resolve_does_not_jitter(self) -> None:
        # When there are no other_versions, only the seed get_album
        # runs and no jitter sleep fires.
        sleeps: list[float] = []
        r = self._resolve_with_n_siblings(
            n_siblings=1, sleep_fn=lambda s: sleeps.append(s))
        self.assertEqual(r.outcome, "ok")
        self.assertEqual(len(r.youtube_releases), 1)
        self.assertEqual(sleeps, [],
                         msg="single-sibling resolve must not jitter")


class TestPayloadCoercionResilience(unittest.TestCase):
    """Finding #5: YT payloads occasionally carry numeric fields as
    strings ("01", "60.0") or ``None``; bare ``int()`` / ``float()``
    would raise and produce a 500. The resolver routes coercion through
    ``_safe_int`` / ``_safe_float`` helpers so unexpected shapes degrade
    gracefully (the track ends up with a defaulted value, the resolve
    succeeds).
    """

    def test_string_track_number_does_not_500(self) -> None:
        """A YT album whose tracks report ``trackNumber: "01"`` (string,
        not int) must not 500 the resolver — coercion is tolerant.
        """
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
        })
        mb_group = _LookupSpy({rg: _ok_mb_rg_releases((MB_REL_A, 1996))})

        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result("MPREb-stringy", year="1996",
                                     track_count=2)],
        )
        stringy_tracks = [
            {"videoId": "vid-0", "title": "Intro",
             "artists": [{"name": "Dr. Octagon"}],
             "album": {"name": "Dr. Octagonecologyst", "id": "MPREb-na"},
             "duration": "1:00", "duration_seconds": "60",  # string
             "trackNumber": "01",                            # string
             "isAvailable": True, "isExplicit": False},
            {"videoId": "vid-1", "title": "3000",
             "artists": [{"name": "Dr. Octagon"}],
             "album": {"name": "Dr. Octagonecologyst", "id": "MPREb-na"},
             "duration": "3:00", "duration_seconds": None,  # None
             "trackNumber": "02",
             "isAvailable": True, "isExplicit": False},
        ]
        yt.set_album(
            "MPREb-stringy",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-stringy",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon"}],
                year="1996",
                tracks=stringy_tracks,
                other_versions=[],
            ),
        )

        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        # The resolver gracefully handled the string/None coercion.
        self.assertEqual(result.outcome, "ok",
                         msg=f"expected ok, got {result.outcome}: "
                             f"{result.error_message}")
        self.assertEqual(len(result.youtube_releases), 1)
        # Tracks were synthesised — _safe_int defaulted the bogus
        # values but didn't crash the resolve.
        self.assertEqual(len(result.youtube_releases[0].tracks), 2)


# ---------------------------------------------------------------------------
# Partial / per-pair failure modes
# ---------------------------------------------------------------------------


class TestPartialPairFailures(unittest.TestCase):

    def test_ae7_one_mb_sibling_lookup_failure_preserved_per_pair(self) -> None:
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
            MB_REL_B: _ok_mb_release(mbid=MB_REL_B, rg=rg, year=2008),
            MB_REL_C: None,  # mb mirror miss
        })
        mb_group = _LookupSpy({
            rg: _ok_mb_rg_releases((MB_REL_A, 1996), (MB_REL_B, 2008), (MB_REL_C, 2014)),
        })
        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result("MPREb-seed")],
        )
        yt.set_album(
            "MPREb-seed",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-seed",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="1996",
                tracks=_yt_tracks(["Intro", "3000"]),
                other_versions=[],
            ),
        )
        distance_fn = _canned_distance_by_mbid({
            MB_REL_A: {"outcome": "ok", "distance": 0.05},
            MB_REL_B: {"outcome": "ok", "distance": 0.10},
            MB_REL_C: {"outcome": "mb_lookup_failed",
                      "error_message": "MB lookup for rel-c returned empty"},
        })
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=distance_fn,
            cache=None,
        )
        # Service-level still ok despite per-pair failure.
        self.assertEqual(result.outcome, "ok")
        self.assertEqual(len(result.youtube_releases), 1)
        outcomes_by_mbid = {d.mbid: d.outcome
                            for d in result.youtube_releases[0].distances}
        self.assertEqual(outcomes_by_mbid, {
            MB_REL_A: "ok", MB_REL_B: "ok", MB_REL_C: "mb_lookup_failed"})

    def test_get_album_failure_for_one_sibling_excludes_it_from_matrix(self) -> None:
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
        })
        mb_group = _LookupSpy({rg: _ok_mb_rg_releases((MB_REL_A, 1996))})
        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result("MPREb-seed")],
        )
        yt.set_album(
            "MPREb-seed",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-seed",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="1996",
                tracks=_yt_tracks(["Intro", "3000"]),
                other_versions=[_yt_other_version("MPREb-broken", year="2008")],
            ),
        )
        # One sibling fails on get_album.
        yt.set_album_error(
            "MPREb-broken",
            YTMusicServerError("Server returned HTTP 500: oops"),
        )
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "ok")
        self.assertEqual(len(result.youtube_releases), 1)
        self.assertEqual(result.youtube_releases[0].yt_browse_id, "MPREb-seed")


# ---------------------------------------------------------------------------
# Failure modes with and without cache fallback
# ---------------------------------------------------------------------------


class TestYoutubeFailureModes(unittest.TestCase):

    def _basic_lookups(self):
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg),
        })
        mb_group = _LookupSpy({rg: _ok_mb_rg_releases((MB_REL_A, 1996))})
        return rg, mb_leaf, mb_group

    def test_ae6_yt_429_with_no_cache_returns_unresolved_4xx(self) -> None:
        rg, mb_leaf, mb_group = self._basic_lookups()
        yt = FakeYTMusic()
        yt.set_search_error(
            "Dr. Octagon Dr. Octagonecologyst",
            YTMusicServerError("Server returned HTTP 429: rate limited"),
        )
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "unresolved_4xx_client")
        self.assertEqual(result.youtube_releases, [])
        assert result.error_message is not None
        self.assertIn("429", result.error_message)

    def test_ae6_yt_5xx_returns_unresolved_mirror_unavailable(self) -> None:
        rg, mb_leaf, mb_group = self._basic_lookups()
        yt = FakeYTMusic()
        yt.set_search_error(
            "Dr. Octagon Dr. Octagonecologyst",
            YTMusicServerError("Server returned HTTP 503: service down"),
        )
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "unresolved_mirror_unavailable")

    def test_ae6_yt_failure_with_cache_returns_cached_with_error_message(self) -> None:
        rg, mb_leaf, mb_group = self._basic_lookups()
        pdb = FakePipelineDB()
        # Seed the cache directly.
        pdb.seed_youtube_album_mapping(rg, "mb", [{
            "yt_browse_id": "MPREb-seed",
            "yt_audio_playlist_id": "OLAK5uy-seed",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy-seed",
            "yt_year": 1996,
            "yt_track_count": 2,
            "yt_tracks": [
                {"title": "Intro", "artists": [{"name": "Dr. Octagon"}],
                 "length_seconds": 60.0, "track_number": 1,
                 "disc_number": 1, "video_id": "vid-0"},
                {"title": "3000", "artists": [{"name": "Dr. Octagon"}],
                 "length_seconds": 180.0, "track_number": 2,
                 "disc_number": 1, "video_id": "vid-1"},
            ],
            "distances": [
                {"mbid": MB_REL_A, "outcome": "ok", "distance": 0.05,
                 "components": {"tracks": 0.05},
                 "matched_tracks": 2, "total_local_tracks": 2,
                 "total_mb_tracks": 2, "extra_local_tracks": 0,
                 "extra_mb_tracks": 0, "error_message": None},
            ],
        }])
        # Force a refresh so the service hits YT, which throws.
        yt = FakeYTMusic()
        yt.set_search_error(
            "Dr. Octagon Dr. Octagonecologyst",
            YTMusicServerError("Server returned HTTP 429: throttled"),
        )
        result = resolve_youtube_album(
            rg,
            pdb=pdb,
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
            refresh=True,
        )
        # Cache fallback: ok outcome, from_cache=True, error_message present.
        self.assertEqual(result.outcome, "ok")
        self.assertTrue(result.from_cache)
        assert result.error_message is not None
        self.assertIn("unresolved_4xx_client", result.error_message)
        self.assertIn("serving from cache", result.error_message)
        self.assertEqual(len(result.youtube_releases), 1)

    def test_yt_timeout_returns_unresolved_timeout(self) -> None:
        rg, mb_leaf, mb_group = self._basic_lookups()
        yt = FakeYTMusic()
        yt.set_search_error(
            "Dr. Octagon Dr. Octagonecologyst",
            requests.Timeout("timed out"),
        )
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "unresolved_timeout")

    def test_yt_connection_error_returns_unresolved_timeout(self) -> None:
        rg, mb_leaf, mb_group = self._basic_lookups()
        yt = FakeYTMusic()
        yt.set_search_error(
            "Dr. Octagon Dr. Octagonecologyst",
            requests.ConnectionError("conn refused"),
        )
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "unresolved_timeout")

    def test_yt_user_error_returns_unresolved_4xx(self) -> None:
        rg, mb_leaf, mb_group = self._basic_lookups()
        yt = FakeYTMusic()
        yt.set_search_error(
            "Dr. Octagon Dr. Octagonecologyst",
            YTMusicUserError("invalid filter"),
        )
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "unresolved_4xx_client")

    def test_yt_parse_keyerror_returns_youtube_parse_failed(self) -> None:
        rg, mb_leaf, mb_group = self._basic_lookups()
        yt = FakeYTMusic()
        yt.set_search_error(
            "Dr. Octagon Dr. Octagonecologyst",
            KeyError("musicShelfRenderer"),
        )
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "youtube_parse_failed")


# ---------------------------------------------------------------------------
# Seed-pick heuristic
# ---------------------------------------------------------------------------


class TestSeedPickHeuristic(unittest.TestCase):

    def test_mb_seed_picked_by_lowest_year_then_first_by_id(self) -> None:
        """The release-group sibling with the lowest year is the query source.

        We verify by checking ``yt.search_calls`` carries the expected
        query string derived from that sibling's artist + album fields.
        """
        rg = MB_RG
        # Two siblings with the same year — first-by-id tiebreak picks MB_REL_A.
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(
                mbid=MB_REL_A, rg=rg, year=1996,
                title="Lowest Year Title",
                artist="The Original"),
            MB_REL_B: _ok_mb_release(
                mbid=MB_REL_B, rg=rg, year=2008,
                title="Reissue Title", artist="Different Reissue Credit"),
            MB_REL_C: _ok_mb_release(
                mbid=MB_REL_C, rg=rg, year=1996,
                title="Other 1996 Title", artist="Other Original"),
        })
        mb_group = _LookupSpy({
            rg: _ok_mb_rg_releases(
                (MB_REL_C, 1996), (MB_REL_A, 1996), (MB_REL_B, 2008),
            ),
        })
        yt = FakeYTMusic()
        # The query should be derived from MB_REL_A (first-by-id at lowest year).
        yt.set_search("The Original Lowest Year Title", [])
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "ok")
        self.assertEqual(yt.search_calls[0]["query"],
                         "The Original Lowest Year Title")

    def test_yt_seed_picked_by_year_and_trackcount_proximity_to_mb(self) -> None:
        """Among 3 YT search results, the one closest to the MB seed's
        ``(year, trackCount)`` is selected."""
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996,
                                    tracks=[
                                        {"disc_number": 1, "track_number": i + 1,
                                         "title": f"T{i}", "length_seconds": 60.0}
                                        for i in range(10)
                                    ]),
        })
        mb_group = _LookupSpy({rg: _ok_mb_rg_releases((MB_REL_A, 1996))})
        yt = FakeYTMusic()
        yt.set_search("Dr. Octagon Dr. Octagonecologyst", [
            # Far from MB seed (year=2020, 14 tracks)
            _yt_search_album_result("MPREb-far", year="2020", track_count=14),
            # Closest match (year=1996, 10 tracks)
            _yt_search_album_result("MPREb-near", year="1996", track_count=10),
            # Medium (year=2008, 10 tracks)
            _yt_search_album_result("MPREb-mid", year="2008", track_count=10),
        ])
        # All three need a get_album so the chosen seed flows through.
        yt.set_album(
            "MPREb-near",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-near",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon"}],
                year="1996",
                tracks=_yt_tracks([f"T{i}" for i in range(10)]),
                other_versions=[],
            ),
        )

        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "ok")
        # The "near" browse_id was the one expanded.
        called_browse_ids = [c["browseId"] for c in yt.get_album_calls]
        self.assertIn("MPREb-near", called_browse_ids)

    def test_yt_seed_falls_back_to_top_when_equidistant(self) -> None:
        """When all candidates are equidistant on (year, trackCount),
        the top-ranked search result wins."""
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=2000),
        })
        mb_group = _LookupSpy({rg: _ok_mb_rg_releases((MB_REL_A, 2000))})
        yt = FakeYTMusic()
        # All three have identical (year, trackCount) → distance=0 from MB.
        yt.set_search("Dr. Octagon Dr. Octagonecologyst", [
            _yt_search_album_result("MPREb-first", year="2000", track_count=2),
            _yt_search_album_result("MPREb-second", year="2000", track_count=2),
            _yt_search_album_result("MPREb-third", year="2000", track_count=2),
        ])
        yt.set_album(
            "MPREb-first",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-first",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon"}],
                year="2000",
                tracks=_yt_tracks(["Intro", "3000"]),
                other_versions=[],
            ),
        )
        result = resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )
        self.assertEqual(result.outcome, "ok")
        # Top-ranked (first in the list) was expanded.
        self.assertEqual(yt.get_album_calls[0]["browseId"], "MPREb-first")


# ---------------------------------------------------------------------------
# URL synthesis
# ---------------------------------------------------------------------------


class TestUrlSynthesis(unittest.TestCase):

    def _resolve_single_yt_album(self, *, audio_playlist_id: Optional[str]
                                 ) -> YoutubeAlbumResolverResult:
        rg = MB_RG
        mb_leaf = _LookupSpy({
            rg: None,
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
        })
        mb_group = _LookupSpy({rg: _ok_mb_rg_releases((MB_REL_A, 1996))})
        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result("MPREb-seed")],
        )
        yt.set_album(
            "MPREb-seed",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id=audio_playlist_id,
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon"}],
                year="1996",
                tracks=_yt_tracks(["Intro", "3000"]),
                other_versions=[],
            ),
        )
        return resolve_youtube_album(
            rg,
            pdb=FakePipelineDB(),
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=None,
        )

    def test_url_uses_playlist_when_audio_playlist_id_present(self) -> None:
        r = self._resolve_single_yt_album(audio_playlist_id="OLAK5uy-xyz")
        self.assertEqual(r.outcome, "ok")
        self.assertEqual(len(r.youtube_releases), 1)
        self.assertEqual(
            r.youtube_releases[0].yt_url,
            "https://music.youtube.com/playlist?list=OLAK5uy-xyz",
        )

    def test_url_falls_back_to_browse_when_audio_playlist_id_missing(self) -> None:
        r = self._resolve_single_yt_album(audio_playlist_id=None)
        self.assertEqual(r.outcome, "ok")
        self.assertEqual(
            r.youtube_releases[0].yt_url,
            "https://music.youtube.com/browse/MPREb-seed",
        )
        self.assertIsNone(r.youtube_releases[0].yt_audio_playlist_id)


# ---------------------------------------------------------------------------
# Integration slice — real compute_beets_distance, real beets, fake YT + DB
# ---------------------------------------------------------------------------


class TestYoutubeAlbumResolverIntegrationSlice(unittest.TestCase):
    """End-to-end through the real distance function.

    No mocking of ``compute_beets_distance``. ``FakeYTMusic`` provides
    deterministic YT responses; ``FakePipelineDB`` provides the cache
    surface; MB lookups are simple lambdas. Asserts the matrix is shaped
    correctly AND that matching pairs score within a reasonable tolerance.
    """

    def test_realistic_matrix_through_real_beets_distance(self) -> None:
        rg = MB_RG
        mb_releases = {
            MB_REL_A: _ok_mb_release(
                mbid=MB_REL_A, rg=rg, year=1996,
                tracks=[
                    {"disc_number": 1, "track_number": 1,
                     "title": "Intro", "length_seconds": 60.0},
                    {"disc_number": 1, "track_number": 2,
                     "title": "3000", "length_seconds": 180.0},
                ],
            ),
            MB_REL_B: _ok_mb_release(
                mbid=MB_REL_B, rg=rg, year=2008,
                tracks=[
                    {"disc_number": 1, "track_number": 1,
                     "title": "Intro", "length_seconds": 60.0},
                    {"disc_number": 1, "track_number": 2,
                     "title": "3000", "length_seconds": 180.0},
                    # Reissue bonus track.
                    {"disc_number": 1, "track_number": 3,
                     "title": "Bonus", "length_seconds": 200.0},
                ],
            ),
        }
        mb_leaf = _LookupSpy({rg: None, **mb_releases})  # type: ignore[arg-type]
        mb_group = _LookupSpy({
            rg: _ok_mb_rg_releases((MB_REL_A, 1996), (MB_REL_B, 2008)),
        })

        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result("MPREb-original", year="1996",
                                     track_count=2)],
        )
        # YT "original" = 2 tracks matching the 1996 MB sibling.
        original_tracks = [
            {**t, "duration_seconds": int(t["duration_seconds"])}
            for t in [
                {"videoId": "vid-0", "title": "Intro",
                 "artists": [{"name": "Dr. Octagon", "id": "UCx"}],
                 "album": {"name": "Dr. Octagonecologyst", "id": "MPREb-na"},
                 "duration": "1:00", "duration_seconds": 60,
                 "trackNumber": 1, "isAvailable": True,
                 "isExplicit": False},
                {"videoId": "vid-1", "title": "3000",
                 "artists": [{"name": "Dr. Octagon", "id": "UCx"}],
                 "album": {"name": "Dr. Octagonecologyst", "id": "MPREb-na"},
                 "duration": "3:00", "duration_seconds": 180,
                 "trackNumber": 2, "isAvailable": True,
                 "isExplicit": False},
            ]
        ]
        yt.set_album(
            "MPREb-original",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-orig",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="1996",
                tracks=original_tracks,
                other_versions=[
                    {"browseId": "MPREb-reissue", "title": "Dr. Octagonecologyst",
                     "artists": [{"name": "Dr. Octagon", "id": "UCx"}],
                     "year": "2008", "thumbnails": [], "isExplicit": False},
                ],
            ),
        )
        # YT "reissue" = 3 tracks matching the 2008 MB sibling.
        reissue_tracks = original_tracks + [
            {"videoId": "vid-2", "title": "Bonus",
             "artists": [{"name": "Dr. Octagon", "id": "UCx"}],
             "album": {"name": "Dr. Octagonecologyst", "id": "MPREb-na"},
             "duration": "3:20", "duration_seconds": 200,
             "trackNumber": 3, "isAvailable": True, "isExplicit": False},
        ]
        yt.set_album(
            "MPREb-reissue",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-reissue",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="2008",
                tracks=reissue_tracks,
                other_versions=[],
            ),
        )

        pdb = FakePipelineDB()
        result = resolve_youtube_album(
            rg,
            pdb=pdb,
            mb_get_release=mb_leaf,
            mb_get_release_group_releases=mb_group,
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=compute_beets_distance,  # real beets
            cache=None,
        )

        self.assertEqual(result.outcome, "ok", msg=result.error_message)
        self.assertEqual(len(result.youtube_releases), 2)
        # Map by browse_id for clarity.
        by_browse = {r.yt_browse_id: r for r in result.youtube_releases}

        original = by_browse["MPREb-original"]
        # Distances against both MB siblings.
        self.assertEqual(len(original.distances), 2)
        original_by_mbid = {d.mbid: d for d in original.distances}
        for d in original.distances:
            self.assertEqual(d.outcome, "ok",
                             msg=f"mbid={d.mbid} error={d.error_message}")
            assert d.distance is not None
            self.assertGreaterEqual(d.distance, 0.0)
        # Matching pair (YT orig vs MB 1996) should score lower than the
        # mismatched pair (YT orig vs MB 2008 reissue with bonus track).
        d_match = original_by_mbid[MB_REL_A].distance
        d_mismatch = original_by_mbid[MB_REL_B].distance
        assert d_match is not None and d_mismatch is not None
        self.assertLess(d_match, d_mismatch + 0.0001,
                        msg=f"expected matching pair to score <= mismatched pair, "
                            f"got matching={d_match} mismatched={d_mismatch}")

        reissue = by_browse["MPREb-reissue"]
        reissue_by_mbid = {d.mbid: d for d in reissue.distances}
        # Symmetric: reissue should match MB-b (3 tracks) better than MB-a.
        d_reissue_match = reissue_by_mbid[MB_REL_B].distance
        d_reissue_mismatch = reissue_by_mbid[MB_REL_A].distance
        assert d_reissue_match is not None and d_reissue_mismatch is not None
        self.assertLess(d_reissue_match, d_reissue_mismatch + 0.0001)


# ---------------------------------------------------------------------------
# Result struct shapes
# ---------------------------------------------------------------------------


class TestResultStructShape(unittest.TestCase):
    """Smoke test that the typed structs round-trip via msgspec."""

    def test_result_round_trips_via_msgspec(self) -> None:
        import msgspec
        r = YoutubeAlbumResolverResult(
            outcome="ok",
            release_group_identifier="rg",
            source="mb",
            from_cache=False,
            youtube_releases=[
                ResolvedYoutubeRelease(
                    yt_browse_id="MPREb-x",
                    yt_audio_playlist_id="OLAK5uy-x",
                    yt_url="https://music.youtube.com/playlist?list=OLAK5uy-x",
                    year=1996,
                    track_count=2,
                    tracks=[
                        SyntheticItem(
                            title="t", artist="a", album="al", albumartist="aa",
                            track=1, tracktotal=2, disc=1, disctotal=1,
                            length=60.0,
                        )
                    ],
                    distances=[
                        ResolvedDistance(
                            mbid=MB_REL_A, outcome="ok", distance=0.1,
                            components={"tracks": 0.05}, matched_tracks=2,
                            total_local_tracks=2, total_mb_tracks=2,
                            extra_local_tracks=0, extra_mb_tracks=0,
                            error_message=None,
                        ),
                    ],
                )
            ],
            error_message=None,
            duration_ms=12,
        )
        blob = msgspec.json.encode(r)
        decoded = msgspec.json.decode(blob, type=YoutubeAlbumResolverResult)
        self.assertEqual(decoded.outcome, "ok")
        self.assertEqual(decoded.youtube_releases[0].yt_browse_id, "MPREb-x")
        self.assertEqual(decoded.youtube_releases[0].distances[0].mbid, MB_REL_A)


class _DictCache:
    """In-memory ``BeetsDistanceCache`` for testing the YT cache wiring.

    Mirrors the same-name fake in tests/test_beets_distance.py; kept
    local so this module stays self-contained.
    """

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self.get_calls: list[str] = []
        self.set_calls: list[tuple[str, int]] = []

    def get(self, key: str) -> bytes | None:
        self.get_calls.append(key)
        return self._store.get(key)

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        self.set_calls.append((key, ttl_seconds))
        self._store[key] = value


class _ReadOnlyDictCache(_DictCache):
    """A cache where ``set`` always raises — simulates Redis being down
    on the write side."""

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        del key, value, ttl_seconds  # signature must match base class
        raise RuntimeError("Redis SET failed")


class TestRedisCacheWiring(unittest.TestCase):
    """Verifies the service uses the injected ``cache`` to accelerate
    YouTube Music HTTP calls (`search` + `get_album`) per the plan's
    cache-flow design. Existing tests pass ``cache=None`` and verify
    the durable-table path; these tests verify the HTTP-accelerator
    layer that the durable table sits on top of.
    """

    def _build_yt(self) -> FakeYTMusic:
        yt = FakeYTMusic()
        yt.set_search(
            "Dr. Octagon Dr. Octagonecologyst",
            [_yt_search_album_result("MPREb-seed", year="1996", track_count=2)],
        )
        yt.set_album(
            "MPREb-seed",
            FakeYTMusic.make_album_fixture(
                audio_playlist_id="OLAK5uy-seed",
                title="Dr. Octagonecologyst",
                artists=[{"name": "Dr. Octagon", "id": "UCx"}],
                year="1996",
                tracks=_yt_tracks(["Intro", "3000"]),
            ),
        )
        return yt

    def _resolve(self, *, pdb, yt, cache):
        rg = MB_RG
        mb_release_lookup = _LookupSpy({
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
        })
        return resolve_youtube_album(
            MB_REL_A,
            pdb=pdb,
            mb_get_release=mb_release_lookup,
            mb_get_release_group_releases=_LookupSpy({
                rg: _ok_mb_rg_releases((MB_REL_A, 1996)),
            }),
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=cache,
        )

    def test_first_call_populates_cache_with_search_and_get_album(self) -> None:
        cache = _DictCache()
        result = self._resolve(pdb=FakePipelineDB(), yt=self._build_yt(), cache=cache)

        self.assertEqual(result.outcome, "ok")
        # Search + one get_album each got written to cache.
        keys = {k for k, _ttl in cache.set_calls}
        self.assertIn("youtube:search:Dr. Octagon Dr. Octagonecologyst:albums:10", keys)
        self.assertIn("youtube:album:MPREb-seed", keys)
        # TTL is the forever sentinel.
        for _key, ttl in cache.set_calls:
            self.assertEqual(ttl, 2**31 - 1)

    def test_second_call_with_warm_cache_skips_yt_client(self) -> None:
        cache = _DictCache()
        # Prime the cache by running once.
        yt_first = self._build_yt()
        self._resolve(pdb=FakePipelineDB(), yt=yt_first, cache=cache)
        self.assertGreater(len(yt_first.search_calls), 0)
        self.assertGreater(len(yt_first.get_album_calls), 0)

        # Second run with a fresh YT client + the warm cache + a fresh
        # PipelineDB so the durable-table cache is also cold. The
        # service must read everything from Redis.
        yt_second = self._build_yt()
        self._resolve(pdb=FakePipelineDB(), yt=yt_second, cache=cache)
        self.assertEqual(yt_second.search_calls, [],
                         "warm cache must skip YT search entirely")
        self.assertEqual(yt_second.get_album_calls, [],
                         "warm cache must skip YT get_album entirely")

    def test_corrupt_cache_entry_falls_through_to_fresh_fetch(self) -> None:
        cache = _DictCache()
        # Plant a deliberately-corrupt blob at the search key.
        cache._store[
            "youtube:search:Dr. Octagon Dr. Octagonecologyst:albums:10"
        ] = b"not-valid-json{{"
        yt = self._build_yt()
        result = self._resolve(pdb=FakePipelineDB(), yt=yt, cache=cache)

        # Resolution succeeds and yt_client was hit (cache miss fell
        # through to fresh fetch).
        self.assertEqual(result.outcome, "ok")
        self.assertGreater(len(yt.search_calls), 0)

    def test_cache_write_failure_does_not_break_resolution(self) -> None:
        cache = _ReadOnlyDictCache()
        yt = self._build_yt()
        # Resolution should succeed despite every cache.set raising.
        result = self._resolve(pdb=FakePipelineDB(), yt=yt, cache=cache)
        self.assertEqual(result.outcome, "ok")

    def test_refresh_true_bypasses_redis_read_but_still_writes(self) -> None:
        """Finding #6: ``refresh=True`` must skip the Redis cache READ
        (forcing a fresh YT fetch) but STILL update the cache with the
        new response so subsequent non-refresh resolves can hit it.
        """
        rg = MB_RG
        mb_release_lookup = _LookupSpy({
            MB_REL_A: _ok_mb_release(mbid=MB_REL_A, rg=rg, year=1996),
        })
        # Build the canonical search-key the cache will use.
        search_key = (
            "youtube:search:Dr. Octagon Dr. Octagonecologyst:albums:10"
        )
        album_key = "youtube:album:MPREb-seed"

        cache = _DictCache()
        # Prime the cache with a STALE result the resolver should ignore.
        # If refresh=True doesn't bypass the Redis read, this stale entry
        # would route through and yt_client.search would never be called.
        stale_search = msgspec.json.encode([
            _yt_search_album_result(
                "MPREb-STALE", year="1990", track_count=99),
        ])
        cache._store[search_key] = stale_search

        # The "fresh" YT contains the real seed.
        yt = self._build_yt()
        result = resolve_youtube_album(
            MB_REL_A,
            pdb=FakePipelineDB(),
            mb_get_release=mb_release_lookup,
            mb_get_release_group_releases=_LookupSpy({
                rg: _ok_mb_rg_releases((MB_REL_A, 1996)),
            }),
            discogs_get_release=_empty_lookup(),
            discogs_get_master_releases=_empty_lookup(),
            yt_client=yt,
            distance_fn=_canned_distance(),
            cache=cache,
            refresh=True,
        )
        self.assertEqual(result.outcome, "ok")
        # refresh=True bypassed the stale cache entry — YT.search ran.
        self.assertGreater(len(yt.search_calls), 0,
                           msg="refresh=True must bypass the Redis read "
                               "and call yt_client.search")
        # The fresh response replaced the stale cache entry. The
        # subsequent (non-refresh) resolve would now hit the cache.
        self.assertIn(search_key, cache._store)
        self.assertIn(album_key, cache._store)
        # Confirm the cached search payload is the fresh one (not stale).
        decoded_search = msgspec.json.decode(cache._store[search_key])
        browse_ids = {
            r.get("browseId") for r in decoded_search if isinstance(r, dict)
        }
        self.assertIn("MPREb-seed", browse_ids)
        self.assertNotIn("MPREb-STALE", browse_ids)


if __name__ == "__main__":
    unittest.main()
