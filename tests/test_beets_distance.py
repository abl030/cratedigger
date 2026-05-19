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
    DictCache,
    OUTCOMES,
    compute_beets_distance,
)


FIXTURE_FLAC = os.path.join(
    os.path.dirname(__file__), "fixtures", "audio_hash", "sine_440.flac")


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


if __name__ == "__main__":
    unittest.main()
