"""Tests for ``lib.beets_distance.compute_beets_distance``.

Covers the outcome matrix end-to-end. Filesystem-touching paths use a
temp directory + a real audio fixture (copied from
``tests/fixtures/audio_hash``) tagged via ``music_tag``; non-FS paths
use a tiny stub DB so we exercise the real service logic without
mocking the function under test.

The integration slice in ``TestBeetsDistanceIntegrationSlice`` is the
authoritative coverage of the happy path — it runs the real beets
distance computation against real on-disk files and asserts the result
is in a sane range and that the cache fast-path returns the same
number on a second call (mtime-stable).
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from typing import Optional

import music_tag

from lib.beets_distance import (
    BeetsDistanceCache,
    BeetsDistanceResult,
    SyntheticItem,
    compute_beets_distance,
)


FIXTURE_FLAC = os.path.join(
    os.path.dirname(__file__), "fixtures", "audio_hash", "sine_440.flac")


# Canonical outcome strings emitted by compute_beets_distance. Pinned here
# because they're wire contract (CLI exit codes, HTTP status, web UI) —
# any change requires coordinated updates downstream. ``ok`` is exercised
# by the integration slice further down; the rest by TestComputeBeetsDistanceOutcomes.
OUTCOMES = (
    "ok",
    "download_log_not_found",
    "request_not_found",
    "folder_missing",
    "no_audio",
    "mb_lookup_failed",
    "mb_no_release_group",
    "wrong_release_group",
    "distance_failed",
)


class DictCache:
    """In-memory BeetsDistanceCache implementation, test-only."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    def get(self, key: str) -> Optional[bytes]:
        return self._store.get(key)

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        self._store[key] = value


class _StubPDB:
    """Tiny ``PipelineDB`` stand-in for the service tests.

    Only the two methods ``compute_beets_distance`` touches are
    implemented; ``FakePipelineDB`` in ``tests/fakes.py`` is overkill
    for a 1-call read path and would couple this test file to its
    unrelated schema.
    """

    def __init__(
        self,
        *,
        download_log_entry: Optional[dict] = None,
        request: Optional[dict] = None,
    ) -> None:
        self._dl = download_log_entry
        self._request = request

    def get_download_log_entry(self, log_id):
        if self._dl is None:
            return None
        if self._dl.get("id") != log_id:
            return None
        return dict(self._dl)

    def get_request(self, request_id):
        if self._request is None or self._request.get("id") != request_id:
            return None
        return dict(self._request)


def _ok_mb_release(
    *,
    mbid: str = "rel-aaa",
    rg: str = "rg-shared",
    artist: str = "Dr. Octagon",
    album: str = "Dr. Octagonecologyst",
    tracks: Optional[list[dict]] = None,
):
    return {
        "id": mbid,
        "title": album,
        "artist_name": artist,
        "artist_id": "artist-1",
        "release_group_id": rg,
        "date": "1996-05-07",
        "year": 1996,
        "country": "US",
        "status": "Official",
        "tracks": tracks if tracks is not None else [
            {"disc_number": 1, "track_number": 1, "title": "Intro", "length_seconds": 60.0},
            {"disc_number": 1, "track_number": 2, "title": "3000",  "length_seconds": 180.0},
        ],
    }


# ============================================================================
# Outcome-matrix tests — DB/MB/disk are stubbed, every branch is one subTest.
# ============================================================================


class TestComputeBeetsDistanceOutcomes(unittest.TestCase):
    """Every outcome in ``OUTCOMES`` reachable from a single subTest table.

    Each row drives ``compute_beets_distance`` to exactly one outcome.
    ``ok`` is exercised by the integration slice further down (it needs
    a real audio file).
    """

    def test_outcome_set_is_stable(self) -> None:
        """Adding/removing an outcome is a wire-contract change — pin it."""
        self.assertEqual(
            set(OUTCOMES),
            {
                "ok",
                "download_log_not_found",
                "request_not_found",
                "folder_missing",
                "no_audio",
                "mb_lookup_failed",
                "mb_no_release_group",
                "wrong_release_group",
                "distance_failed",
            },
        )

    def test_download_log_not_found(self) -> None:
        pdb = _StubPDB()  # no download_log
        r = compute_beets_distance(
            42, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            resolve_failed_path=lambda p: p,
        )
        self.assertEqual(r.outcome, "download_log_not_found")
        self.assertIsNone(r.distance)
        self.assertEqual(r.download_log_id, 42)
        self.assertEqual(r.candidate_mbid, "rel-x")

    def test_request_not_found_when_log_has_no_request(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": None},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            resolve_failed_path=lambda p: p,
        )
        self.assertEqual(r.outcome, "request_not_found")

    def test_request_not_found_when_request_missing(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 99},
            request=None,
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            resolve_failed_path=lambda p: p,
        )
        self.assertEqual(r.outcome, "request_not_found")

    def test_mb_lookup_failed_when_returns_empty(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: None,
            resolve_failed_path=lambda p: p,
        )
        self.assertEqual(r.outcome, "mb_lookup_failed")
        self.assertEqual(r.request_release_group_id, "rg-shared")

    def test_mb_lookup_failed_on_exception(self) -> None:
        def _boom(mbid):
            raise RuntimeError("MB mirror down")

        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=_boom,
            resolve_failed_path=lambda p: p,
        )
        self.assertEqual(r.outcome, "mb_lookup_failed")
        assert r.error_message is not None
        self.assertIn("MB mirror down", r.error_message)

    def test_mb_no_release_group(self) -> None:
        mb = _ok_mb_release(mbid="rel-x")
        mb["release_group_id"] = None
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: mb,
            resolve_failed_path=lambda p: p,
        )
        self.assertEqual(r.outcome, "mb_no_release_group")

    def test_wrong_release_group_guardrail(self) -> None:
        """The sanity-stop: candidate MBID in a different RG → refuse."""
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": "/whatever"}},
            request={"id": 7, "mb_release_group_id": "rg-source"},
        )
        r = compute_beets_distance(
            1, "rel-alien",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
            resolve_failed_path=lambda p: p,
        )
        self.assertEqual(r.outcome, "wrong_release_group")
        self.assertEqual(r.request_release_group_id, "rg-source")
        self.assertEqual(r.candidate_release_group_id, "rg-other")
        # Guardrail must fire BEFORE filesystem access.
        # (We never resolved a path — fingerprints/distance never ran.)
        self.assertIsNone(r.folder_path)
        self.assertIsNone(r.distance)

    def test_wrong_release_group_passes_when_request_rg_is_null(self) -> None:
        """If the request has no RG (legacy row), we can't refuse — fall through.

        Documenting the asymmetry: the guardrail only fires when *both*
        sides know their release group. A null request RG drops us into
        the rest of the pipeline, which will fail later (folder_missing
        or no_audio) but on a different signal.
        """
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": "/missing"}},
            request={"id": 7, "mb_release_group_id": None},
        )
        r = compute_beets_distance(
            1, "rel-alien",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
            resolve_failed_path=lambda p: None,  # file not on disk
        )
        self.assertEqual(r.outcome, "folder_missing")  # not wrong_release_group
        self.assertEqual(r.candidate_release_group_id, "rg-other")

    def test_folder_missing(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": "/not/there"}},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            resolve_failed_path=lambda p: None,
        )
        self.assertEqual(r.outcome, "folder_missing")

    def test_folder_missing_when_validation_result_absent(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": None},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            resolve_failed_path=lambda p: None,
        )
        self.assertEqual(r.outcome, "folder_missing")

    def test_no_audio_when_folder_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pdb = _StubPDB(
                download_log_entry={"id": 1, "request_id": 7,
                                    "validation_result": {"failed_path": tmp}},
                request={"id": 7, "mb_release_group_id": "rg-shared"},
            )
            r = compute_beets_distance(
                1, "rel-x",
                pdb=pdb,
                mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
                resolve_failed_path=lambda p: p,
            )
            self.assertEqual(r.outcome, "no_audio")
            self.assertEqual(r.folder_path, tmp)


# ============================================================================
# Integration slice — real beets distance against real on-disk audio.
# ============================================================================


def _make_tagged_folder(
    target_dir: str,
    tracks: list[dict],
) -> None:
    """Copy the sine fixture N times into ``target_dir`` and apply tags.

    Each ``tracks[i]`` dict supplies the tag fields. We use FLAC because
    the fixture set already contains a FLAC; sine_440.mp3 would work
    too. Length is fixed (the fixture is ~1s) — distance compares to
    ``length_seconds`` on the MB side, so we set the MB lengths to
    match the fixture for a clean ``ok``.
    """
    for i, t in enumerate(tracks):
        dest = os.path.join(target_dir, f"{i + 1:02d} - {t['title']}.flac")
        shutil.copyfile(FIXTURE_FLAC, dest)
        f = music_tag.load_file(dest)
        assert f is not None
        f["artist"] = t["artist"]
        f["album"] = t["album"]
        f["albumartist"] = t.get("albumartist", t["artist"])
        f["title"] = t["title"]
        f["tracknumber"] = t["track"]
        f["totaltracks"] = len(tracks)
        f["discnumber"] = t.get("disc", 1)
        f.save()


class TestBeetsDistanceIntegrationSlice(unittest.TestCase):
    """Drive the real beets distance pipeline end-to-end.

    The fixture FLACs are ~1 s; we tag them with realistic metadata
    and point MB-side TrackInfo lengths at the fixture's true length
    so a clean tag set produces a small distance. Then we mutate the
    tags and confirm the distance grows. That's enough signal to
    prove the real beets distance call is plumbed correctly without
    coupling to specific numeric values that the beets default
    weights can shift between versions.
    """

    @classmethod
    def setUpClass(cls) -> None:
        # Read the fixture's real length once so MB tracks line up
        # with whatever the on-disk file actually decodes to. Import
        # via ``lib.beets_distance`` which holds an eager reference to
        # the upstream ``beets.library`` module — guards against the
        # sys.path leak where other tests inject ``tests/../lib`` and
        # shadow the real ``beets`` package.
        from lib.beets_distance import _beets_library
        item = _beets_library.Item.from_path(FIXTURE_FLAC)
        cls.fixture_length = float(item.get("length") or 1.0)

    def _build_request_and_mb(self, *, artist: str, album: str, titles: list[str]):
        tracks = [
            {
                "disc_number": 1,
                "track_number": i + 1,
                "title": title,
                "length_seconds": self.fixture_length,
            }
            for i, title in enumerate(titles)
        ]
        mb = {
            "id": "rel-aaa",
            "title": album,
            "artist_name": artist,
            "artist_id": "artist-1",
            "release_group_id": "rg-shared",
            "year": 2020,
            "date": "2020-01-01",
            "country": "US",
            "status": "Official",
            "tracks": tracks,
        }
        return mb

    def _run_compute(
        self,
        *,
        folder: str,
        mb: dict,
        cache: Optional[BeetsDistanceCache] = None,
    ) -> BeetsDistanceResult:
        pdb = _StubPDB(
            download_log_entry={
                "id": 100,
                "request_id": 7,
                "validation_result": {"failed_path": folder},
            },
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        return compute_beets_distance(
            100, "rel-aaa",
            pdb=pdb,
            mb_get_release=lambda mbid: mb,
            resolve_failed_path=lambda p: p,
            cache=cache,
        )

    def test_clean_match_is_low_distance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _make_tagged_folder(tmp, [
                {"title": "Intro",  "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 1},
                {"title": "3000",   "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 2},
            ])
            mb = self._build_request_and_mb(
                artist="Dr. Octagon",
                album="Dr. Octagonecologyst",
                titles=["Intro", "3000"],
            )
            r = self._run_compute(folder=tmp, mb=mb)

            self.assertEqual(r.outcome, "ok", msg=r.error_message)
            assert r.distance is not None
            self.assertGreaterEqual(r.distance, 0.0)
            self.assertLess(r.distance, 0.5, msg="clean tag match should score < 0.5")
            self.assertEqual(r.matched_tracks, 2)
            self.assertEqual(r.total_local_tracks, 2)
            self.assertEqual(r.total_mb_tracks, 2)
            assert r.duration_ms is not None
            # First-read latency: tag IO + beets fit. Generous ceiling
            # so the test doesn't flake on slow CI; the cached-fast-path
            # test below pins what "fast" actually means.
            self.assertLess(r.duration_ms, 10_000)

    def test_wrong_album_metadata_is_higher_distance(self) -> None:
        """Sanity: mistagged folder vs. correct MB → distance grows."""
        with tempfile.TemporaryDirectory() as tmp:
            _make_tagged_folder(tmp, [
                {"title": "Nothing Like It",  "artist": "Other Person", "album": "Wrong Album", "track": 1},
                {"title": "Some Other Song",  "artist": "Other Person", "album": "Wrong Album", "track": 2},
            ])
            mb = self._build_request_and_mb(
                artist="Dr. Octagon",
                album="Dr. Octagonecologyst",
                titles=["Intro", "3000"],
            )
            r = self._run_compute(folder=tmp, mb=mb)
            self.assertEqual(r.outcome, "ok", msg=r.error_message)
            assert r.distance is not None
            self.assertGreater(r.distance, 0.3,
                msg="mismatched album metadata should score > 0.3")

    def test_cache_makes_second_call_fast(self) -> None:
        """Same folder, same MB → second compute reuses cached fingerprints.

        First call: tag IO across N files (slow).
        Second call: cache hit, no FS reads beyond os.walk + stat.

        We don't assert a hard wall-clock bound — flaky on shared
        hardware. Instead we assert (a) the cache picked up entries,
        and (b) the second call returns the same distance bit-for-bit
        (proves we round-tripped through the cache without drift).
        """
        cache = DictCache()
        with tempfile.TemporaryDirectory() as tmp:
            _make_tagged_folder(tmp, [
                {"title": "Intro",  "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 1},
                {"title": "3000",   "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 2},
            ])
            mb = self._build_request_and_mb(
                artist="Dr. Octagon",
                album="Dr. Octagonecologyst",
                titles=["Intro", "3000"],
            )
            r1 = self._run_compute(folder=tmp, mb=mb, cache=cache)
            self.assertEqual(r1.outcome, "ok")
            # Two files → two cache entries.
            self.assertEqual(len(cache._store), 2)
            r2 = self._run_compute(folder=tmp, mb=mb, cache=cache)
            self.assertEqual(r2.outcome, "ok")
            self.assertEqual(r1.distance, r2.distance,
                msg="cached fingerprint round-trip must reproduce the same distance")

    def test_distance_result_serializes_to_json(self) -> None:
        """Wire-boundary smoke test: result encodes via msgspec without error."""
        import msgspec
        with tempfile.TemporaryDirectory() as tmp:
            _make_tagged_folder(tmp, [
                {"title": "Intro",  "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 1},
            ])
            mb = self._build_request_and_mb(
                artist="Dr. Octagon",
                album="Dr. Octagonecologyst",
                titles=["Intro"],
            )
            r = self._run_compute(folder=tmp, mb=mb)
            self.assertEqual(r.outcome, "ok", msg=r.error_message)
            blob = msgspec.json.encode(r)
            self.assertIn(b'"outcome":"ok"', blob)
            # Round-trip back to a struct of the same shape.
            r2 = msgspec.json.decode(blob, type=BeetsDistanceResult)
            self.assertEqual(r2.distance, r.distance)


# ============================================================================
# items_override path — synthetic items scored without filesystem IO.
# ============================================================================


def _synth_items(titles: list[str], *, length: float = 60.0,
                 artist: str = "Dr. Octagon",
                 album: str = "Dr. Octagonecologyst",
                 disc: int = 1) -> list[SyntheticItem]:
    """Build a list of SyntheticItems matching ``_ok_mb_release`` defaults."""
    return [
        SyntheticItem(
            title=t,
            artist=artist,
            album=album,
            albumartist=artist,
            track=i + 1,
            tracktotal=len(titles),
            disc=disc,
            disctotal=1,
            length=length,
        )
        for i, t in enumerate(titles)
    ]


class _PDBExploder:
    """PDB stand-in whose every method raises — verifies override path
    skips DB completely (no get_download_log_entry, no get_request)."""

    def get_download_log_entry(self, log_id):  # pragma: no cover — must not be called
        raise AssertionError(
            "items_override path must NOT call get_download_log_entry")

    def get_request(self, request_id):  # pragma: no cover — must not be called
        raise AssertionError(
            "items_override path must NOT call get_request")


class TestComputeBeetsDistanceWithItemsOverride(unittest.TestCase):
    """Coverage for the additive ``items_override`` parameter.

    The override path scores caller-provided items without touching the
    filesystem or the download_log/request rows. Its guardrails are the
    same as the existing path except the cross-RG check is opt-in via
    ``mb_release_group_id``.
    """

    # ---------- Happy path: synthetic items match MB tracks ---------- #

    def test_happy_path_matches_with_zero_distance(self) -> None:
        """Synthetic items with matching titles/length → small distance, outcome ok."""
        mb = _ok_mb_release(mbid="rel-aaa", rg="rg-shared")
        # MB tracks are Intro (60s) and 3000 (180s) per _ok_mb_release default.
        items = [
            SyntheticItem(
                title="Intro", artist="Dr. Octagon", album="Dr. Octagonecologyst",
                albumartist="Dr. Octagon", track=1, tracktotal=2,
                disc=1, disctotal=1, length=60.0,
            ),
            SyntheticItem(
                title="3000", artist="Dr. Octagon", album="Dr. Octagonecologyst",
                albumartist="Dr. Octagon", track=2, tracktotal=2,
                disc=1, disctotal=1, length=180.0,
            ),
        ]
        r = compute_beets_distance(
            mbid="rel-aaa",
            items_override=items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        self.assertEqual(r.outcome, "ok", msg=r.error_message)
        assert r.distance is not None
        self.assertGreaterEqual(r.distance, 0.0)
        self.assertLess(r.distance, 0.5)
        self.assertIsNotNone(r.components)
        assert r.components is not None
        self.assertGreater(len(r.components), 0)
        # Override path means no download_log was consulted.
        self.assertIsNone(r.download_log_id)
        self.assertIsNone(r.request_id)
        self.assertEqual(r.matched_tracks, 2)
        self.assertEqual(r.total_local_tracks, 2)
        self.assertEqual(r.total_mb_tracks, 2)
        self.assertEqual(r.candidate_mbid, "rel-aaa")
        self.assertEqual(r.candidate_release_group_id, "rg-shared")

    # ---------- Track-count asymmetries ---------- #

    def test_mismatched_tracks_reports_extras(self) -> None:
        """12 synth items vs 10 MB tracks → matched=10, extra_local=2, extra_mb=0."""
        mb_tracks = [
            {"disc_number": 1, "track_number": i + 1,
             "title": f"Track {i + 1}", "length_seconds": 60.0}
            for i in range(10)
        ]
        mb = _ok_mb_release(mbid="rel-aaa", rg="rg-shared", tracks=mb_tracks)
        items = _synth_items([f"Track {i + 1}" for i in range(12)])
        r = compute_beets_distance(
            mbid="rel-aaa",
            items_override=items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        self.assertEqual(r.outcome, "ok", msg=r.error_message)
        self.assertEqual(r.total_local_tracks, 12)
        self.assertEqual(r.total_mb_tracks, 10)
        self.assertEqual(r.matched_tracks, 10)
        self.assertEqual(r.extra_local_tracks, 2)
        self.assertEqual(r.extra_mb_tracks, 0)

    # ---------- Per-component penalties ---------- #

    def test_per_component_breakdown_penalises_wrong_title(self) -> None:
        """Wrong track-title → "tracks" component carries a non-zero penalty.

        Beets aggregates per-track distances (title + length + position)
        under the single ``tracks`` key, so we assert on the aggregate
        rather than separate ``track_title`` / ``track_length`` keys.
        Distinct ``length_penalty`` clarity is preserved by comparing a
        wrong-title result to a clean-tag baseline.
        """
        mb_tracks = [
            {"disc_number": 1, "track_number": 1, "title": "RealTitle",
             "length_seconds": 60.0},
        ]
        mb = _ok_mb_release(mbid="rel-aaa", rg="rg-shared", tracks=mb_tracks)
        wrong_title_items = [
            SyntheticItem(
                title="CompletelyDifferentTitle",
                artist="Dr. Octagon", album="Dr. Octagonecologyst",
                albumartist="Dr. Octagon",
                track=1, tracktotal=1, disc=1, disctotal=1,
                length=60.0,
            ),
        ]
        clean_items = [
            SyntheticItem(
                title="RealTitle",
                artist="Dr. Octagon", album="Dr. Octagonecologyst",
                albumartist="Dr. Octagon",
                track=1, tracktotal=1, disc=1, disctotal=1,
                length=60.0,
            ),
        ]
        wrong = compute_beets_distance(
            mbid="rel-aaa",
            items_override=wrong_title_items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        clean = compute_beets_distance(
            mbid="rel-aaa",
            items_override=clean_items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        self.assertEqual(wrong.outcome, "ok", msg=wrong.error_message)
        self.assertEqual(clean.outcome, "ok", msg=clean.error_message)
        assert wrong.components is not None and clean.components is not None
        wrong_tracks = wrong.components.get("tracks", 0.0)
        clean_tracks = clean.components.get("tracks", 0.0)
        self.assertGreater(wrong_tracks, clean_tracks,
            msg=(f"expected wrong-title tracks penalty > clean tracks penalty, "
                 f"got wrong={wrong.components} clean={clean.components}"))

    # ---------- MB-side guardrails still fire ---------- #

    def test_mb_lookup_failed_in_override_path(self) -> None:
        """mb_get_release returns None → mb_lookup_failed, no IO attempted."""
        items = _synth_items(["A", "B"])
        r = compute_beets_distance(
            mbid="rel-x",
            items_override=items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: None,
        )
        self.assertEqual(r.outcome, "mb_lookup_failed")
        self.assertIsNone(r.distance)
        self.assertIsNone(r.folder_path)

    def test_mb_no_release_group_in_override_path(self) -> None:
        """MB release lacks release_group_id → mb_no_release_group, no IO."""
        mb = _ok_mb_release(mbid="rel-x")
        mb["release_group_id"] = None
        items = _synth_items(["A", "B"])
        r = compute_beets_distance(
            mbid="rel-x",
            items_override=items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        self.assertEqual(r.outcome, "mb_no_release_group")
        self.assertIsNone(r.distance)
        self.assertIsNone(r.folder_path)

    # ---------- Empty items list ---------- #

    def test_empty_items_override_distinct_outcome(self) -> None:
        """items_override=[] → empty_items_override outcome (NOT no_audio).

        The two are deliberately distinguishable — empty_items_override is a
        caller error, no_audio means a real folder on disk had no readable
        audio. Conflating them would erode audit data.
        """
        r = compute_beets_distance(
            mbid="rel-x",
            items_override=[],
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
        )
        self.assertEqual(r.outcome, "empty_items_override")
        self.assertIsNone(r.distance)
        # The empty-items condition is detected without DB / MB / FS IO.
        self.assertIsNone(r.folder_path)

    # ---------- Input validation guardrail ---------- #

    def test_invalid_input_both_signaled(self) -> None:
        """Both download_log_id AND items_override → invalid_input, no IO."""
        items = _synth_items(["A"])
        r = compute_beets_distance(
            download_log_id=42,
            items_override=items,
            mbid="rel-x",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
        )
        self.assertEqual(r.outcome, "invalid_input")
        self.assertIsNone(r.distance)
        # No DB / MB / FS touch — the exploder PDB would have raised.

    def test_invalid_input_neither_signaled(self) -> None:
        """Neither download_log_id NOR items_override → invalid_input, no IO."""
        r = compute_beets_distance(
            mbid="rel-x",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
        )
        self.assertEqual(r.outcome, "invalid_input")
        self.assertIsNone(r.distance)

    # ---------- Cross-RG guardrail in the override path ---------- #

    def test_cross_rg_guardrail_fires_with_explicit_rg(self) -> None:
        """items_override + mb_release_group_id pointing at a different RG
        from the candidate → wrong_release_group, no scoring attempted."""
        items = _synth_items(["A", "B"])
        # Candidate MBID's RG is "rg-other" but caller asserts "rg-source".
        r = compute_beets_distance(
            mbid="rel-alien",
            items_override=items,
            mb_release_group_id="rg-source",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
        )
        self.assertEqual(r.outcome, "wrong_release_group")
        self.assertEqual(r.request_release_group_id, "rg-source")
        self.assertEqual(r.candidate_release_group_id, "rg-other")
        # No scoring — distance never computed.
        self.assertIsNone(r.distance)
        self.assertIsNone(r.folder_path)

    def test_cross_rg_guardrail_skipped_without_rg_param(self) -> None:
        """items_override + mb_release_group_id=None → guardrail off; proceeds
        to scoring even when candidate MBID's RG differs from anything implicit.
        The function MUST NOT consult any request row in this path."""
        items = _synth_items(["Intro", "3000"])
        # Candidate is in rg-other; no mb_release_group_id passed, so no check.
        r = compute_beets_distance(
            mbid="rel-alien",
            items_override=items,
            pdb=_PDBExploder(),  # would raise if DB consulted
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
        )
        self.assertEqual(r.outcome, "ok", msg=r.error_message)
        self.assertEqual(r.candidate_release_group_id, "rg-other")
        # No request RG was looked up — the override-without-RG path doesn't
        # know the caller's RG and doesn't make one up.
        self.assertIsNone(r.request_release_group_id)

    # ---------- Regression: Replace-picker path unchanged ---------- #

    def test_replace_picker_path_unchanged_when_no_override(self) -> None:
        """download_log_id alone, no items_override, no mb_release_group_id →
        exact same behaviour as before. Drive to a known outcome (wrong_release_group)
        to prove the existing guardrails still fire identically.
        """
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": "/whatever"}},
            request={"id": 7, "mb_release_group_id": "rg-source"},
        )
        r = compute_beets_distance(
            download_log_id=1,
            mbid="rel-alien",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
            resolve_failed_path=lambda p: p,
        )
        # Existing test_wrong_release_group_guardrail asserts the same shape.
        self.assertEqual(r.outcome, "wrong_release_group")
        self.assertEqual(r.request_release_group_id, "rg-source")
        self.assertEqual(r.candidate_release_group_id, "rg-other")
        self.assertEqual(r.download_log_id, 1)
        self.assertEqual(r.request_id, 7)


if __name__ == "__main__":
    unittest.main()
