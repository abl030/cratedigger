#!/usr/bin/env python3
"""Routing-cache behaviour: overlay freshness + analysis skeleton caching.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import os
import sys
import unittest
from unittest.mock import patch


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _FakeDbWebServerCase, _WebServerCase

from lib.artist_catalogue import ArtistCatalogueRow
from tests.helpers import make_request_row


def _catalogue_row(
    *, source: str, row_id: str, primary_artist_id: str,
) -> ArtistCatalogueRow:
    return ArtistCatalogueRow(
        id=row_id,
        title="OK Computer",
        type="Album",
        source="mb" if source == "mb" else "discogs",
        identity_kind="work",
        primary_types=["Album"],
        secondary_types=[],
        format_qualifiers=[],
        provenance=["ordinary"],
        first_release_date="1997",
        artist_credit="Radiohead",
        primary_artist_id=primary_artist_id,
        is_appearance=False,
    )


class TestOverlayNotBakedIntoRoutingCache(_WebServerCase):
    """Issue #101: endpoints that enrich MB/Discogs metadata with per-user
    pipeline/library overlay state MUST NOT be cached at the routing level.

    Pre-fix, /api/release/<id> and friends were cached under web:<url> at
    TTL_LIBRARY=300s. A pipeline-side UPDATE (e.g. status wanted→downloading)
    bypasses the web UI's POST-invalidation paths, so a second GET in the
    300s window returned a stale pipeline_status baked into the cached
    payload.

    Fix: drop every overlay-baking endpoint from Handler._CACHE_TTLS and
    move pure MB/Discogs metadata into a separate meta: namespace at the
    API helper layer (web/mb.py, web/discogs.py). Local DB lookups
    (check_pipeline, check_beets_library) run on every request — cheap.
    """

    # The exact endpoint prefixes proven to bake overlay state — every
    # single one of these was confirmed by the Explore audit to mutate
    # the response with at least one of: pipeline_status, pipeline_id,
    # in_library, library_rank, library_format, library_min_bitrate,
    # beets_album_id, beets_tracks, upgrade_queued, in_beets, library_status.
    FORBIDDEN_ROUTING_CACHE_PREFIXES = (
        "/api/release-group",
        "/api/release",
        "/api/discogs/master",
        "/api/discogs/release",
        "/api/discogs/artist",
        "/api/artist",              # /api/artist/<id> + /api/artist/<id>/disambiguate + /api/artist/compare
        "/api/library",             # /api/library/artist
        "/api/beets",               # /api/beets/album
        "/api/pipeline/all",
        "/api/pipeline/log",
        "/api/pipeline/status",
        "/api/pipeline/dashboard",
    )

    def test_forbidden_prefixes_are_not_in_routing_cache_ttls(self) -> None:
        """Handler._CACHE_TTLS must not contain any overlay-baking prefix."""
        import web.server as srv
        ttls: dict[str, int] = getattr(srv.Handler, "_CACHE_TTLS", {})
        leaked = set(ttls) & set(self.FORBIDDEN_ROUTING_CACHE_PREFIXES)
        self.assertFalse(
            leaked,
            f"Overlay-baking prefixes must not be in _CACHE_TTLS — "
            f"they would bake per-user pipeline/library state into Redis "
            f"and leak stale badges when the pipeline writes to Postgres "
            f"outside the web UI's POST paths. Offenders: {sorted(leaked)}")


class _CachedServerCase(_FakeDbWebServerCase):
    """Shared harness: _WebServerCase but with a FakeRedis wired up so we
    can observe routing-cache behaviour in isolation. Pre-fix this would
    exhibit the stale-badge bug; post-fix it proves the overlay recomputes."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        import web.cache as cache
        from tests.test_web_cache import FakeRedis
        cls._cache = cache
        cls._saved_redis = cache._redis
        cache._redis = FakeRedis()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._cache._redis = cls._saved_redis
        super().tearDownClass()


class TestReleaseEndpointReflectsPipelineWrite(_CachedServerCase):
    """Regression test for issue #101.

    The bug: /api/release/<id> cached the full response including
    pipeline_status. When the pipeline wrote status='downloading'
    directly to Postgres (outside the web UI's POST invalidation
    paths), a second GET within 300s returned the stale 'wanted'
    status. Badges lagged by up to 5 minutes.

    Post-fix: the overlay is recomputed on every request, so external
    DB writes show up immediately.
    """

    RELEASE_ID = "c6cd62c4-da2a-4a89-a219-adba66d6c7d4"

    def setUp(self) -> None:
        super().setUp()
        # Clear any state left behind by a previous test that shares the
        # FakeRedis instance, so each scenario starts cold. `_redis` is
        # typed `object | None` on the module; narrow to FakeRedis here.
        from tests.test_web_cache import FakeRedis
        fake = self._cache._redis
        assert isinstance(fake, FakeRedis)
        fake._store.clear()

    def _call_release_detail(self) -> dict:
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()):
            mock_mb.get_release.return_value = {
                "id": self.RELEASE_ID,
                "title": "Test Album",
                "tracks": [],
            }
            _status, data = self._get(f"/api/release/{self.RELEASE_ID}")
            return data

    def test_release_reflects_external_status_write(self) -> None:
        """Pipeline writes status='downloading' directly to Postgres
        between two GETs. The second GET must see 'downloading'."""
        self.db.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id=self.RELEASE_ID,
        ))
        first = self._call_release_detail()
        self.assertEqual(first["pipeline_status"], "wanted")

        # Simulate cratedigger pipeline flipping status outside the web UI.
        # No POST to /api/cache/invalidate, no web-UI cache-group flush —
        # this is the exact sequence that produced the stale-badge bug.
        self.db.update_status(42, "downloading")
        second = self._call_release_detail()
        self.assertEqual(
            second["pipeline_status"], "downloading",
            "Second GET must see the fresh DB state, not a baked-in "
            "pipeline_status from a cached response. If this fails, the "
            "routing-level cache is still capturing the overlay.")

    def test_release_reflects_external_library_state_flip(self) -> None:
        """Same bug for the in_library flag. After an album is imported
        the 'in_library' flag flips true in beets; a second GET within
        the cache window must reflect that without an explicit flush."""
        self.db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()):
            mock_mb.get_release.return_value = {
                "id": self.RELEASE_ID, "title": "T", "tracks": [],
            }
            _s, first = self._get(f"/api/release/{self.RELEASE_ID}")
        self.assertFalse(first["in_library"])

        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library",
                      return_value={self.RELEASE_ID}):
            mock_mb.get_release.return_value = {
                "id": self.RELEASE_ID, "title": "T", "tracks": [],
            }
            _s, second = self._get(f"/api/release/{self.RELEASE_ID}")
        self.assertTrue(
            second["in_library"],
            "Second GET must recompute the overlay against current beets "
            "state instead of returning a cached in_library=False.")


class TestAnalysisSkeletonCachedSeparately(_CachedServerCase):
    """Issue #101 Codex round 3 — the `/api/artist/<id>/disambiguate`
    and `/api/artist/compare` endpoints run expensive pure analysis on
    top of MB metadata (`filter_non_live` + `analyse_artist_releases`,
    `merge_discographies`). After the response-cache removal, naïvely
    running that analysis on every request regresses warm-load latency
    from ~5ms (full response cached) to ~50-300ms (analysis re-runs).

    Fix: cache the pre-overlay skeleton separately under `meta:`. It's
    a pure function of pure-metadata inputs — safe. Overlay (live DB
    state) still runs on every request.

    These tests pin the split: skeleton is cached across calls, and
    the overlay reflects live DB state even when the skeleton is warm.
    """

    ARTIST_ID = "664c3e0e-42d8-48c1-b209-1efca19c0325"
    RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    RG_ID = "11111111-1111-1111-1111-111111111111"

    _RAW_RELEASES = [
        {
            "id": RELEASE_ID,
            "title": "Album",
            "date": "2020-01-01",
            "country": "US",
            "status": "Official",
            "release-group": {
                "id": RG_ID,
                "title": "Album",
                "primary-type": "Album",
                "secondary-types": [],
            },
            "media": [{
                "position": 1, "format": "CD", "track-count": 1,
                "tracks": [{
                    "position": 1, "number": "1", "title": "Track",
                    "recording": {"id": "rec-1", "title": "Track"},
                }],
            }],
        },
    ]

    def setUp(self) -> None:
        super().setUp()
        from tests.test_web_cache import FakeRedis
        fake = self._cache._redis
        assert isinstance(fake, FakeRedis)
        fake._store.clear()

    # -- Disambiguate ------------------------------------------------

    def test_disambiguate_skeleton_cached_in_meta_namespace(self) -> None:
        """First GET computes the skeleton; second GET reuses it. We
        assert the skeleton ended up under `meta:` and the pure-
        analysis fetch is only issued once across both requests."""
        from tests.test_web_cache import FakeRedis
        fake = self._cache._redis
        assert isinstance(fake, FakeRedis)

        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"

            s1, _ = self._get(f"/api/artist/{self.ARTIST_ID}/disambiguate")
            s2, _ = self._get(f"/api/artist/{self.ARTIST_ID}/disambiguate")

            self.assertEqual(s1, 200)
            self.assertEqual(s2, 200)
            # The pure MB fetch helper was called once — either this is
            # the first call (skeleton miss) or the route's own meta-
            # cached skeleton short-circuited to avoid re-calling it.
            self.assertEqual(
                mock_mb.get_artist_releases_with_recordings.call_count, 1,
                "skeleton caching must reuse the analysis across calls "
                "— the expensive pure-python analysis should NOT re-run "
                "on warm loads")

        # Skeleton key is in the meta: namespace — not web:, so it
        # survives pipeline/library group invalidations.
        meta_keys = [k for k in fake._store
                     if k.startswith("meta:") and self.ARTIST_ID in k]
        self.assertTrue(
            meta_keys,
            f"expected a meta: key for artist {self.ARTIST_ID}, got: "
            f"{sorted(fake._store.keys())}")

    def test_disambiguate_overlay_reflects_live_state_across_skeleton_cache(
            self) -> None:
        """Skeleton cache is warm; change live DB state; next GET must
        still reflect the new pipeline_status via overlay."""
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline",
                      return_value={self.RELEASE_ID: {"id": 42, "status": "wanted"}}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"
            _s, first = self._get(
                f"/api/artist/{self.ARTIST_ID}/disambiguate")

        self.assertEqual(
            first["release_groups"][0]["pressings"][0]["pipeline_status"],
            "wanted")

        # External DB write — status flips to 'downloading'. No POST
        # invalidation (same bug class as the release-detail test).
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline",
                      return_value={self.RELEASE_ID: {"id": 42, "status": "downloading"}}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"
            _s, second = self._get(
                f"/api/artist/{self.ARTIST_ID}/disambiguate")

        self.assertEqual(
            second["release_groups"][0]["pressings"][0]["pipeline_status"],
            "downloading",
            "Even with the skeleton cached in meta:, the overlay must "
            "recompute against current DB state — otherwise the skeleton "
            "cache reintroduces the stale-badge bug.")
        # RG-level pipeline_status must also flip.
        self.assertEqual(
            second["release_groups"][0]["pipeline_status"], "downloading")

    def test_disambiguate_overlay_reflects_library_flip(self) -> None:
        """Same guarantee for in_library — beets state flips, overlay must
        see it without invalidating the skeleton cache."""
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"
            _s, first = self._get(
                f"/api/artist/{self.ARTIST_ID}/disambiguate")
        self.assertFalse(
            first["release_groups"][0]["pressings"][0]["in_library"])

        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library",
                      return_value={self.RELEASE_ID}), \
                patch("web.server.check_pipeline", return_value={}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"
            _s, second = self._get(
                f"/api/artist/{self.ARTIST_ID}/disambiguate")
        self.assertTrue(
            second["release_groups"][0]["pressings"][0]["in_library"])
        self.assertEqual(
            second["release_groups"][0]["library_status"], "in_library")

    # -- Compare -----------------------------------------------------

    def test_compare_skeleton_cached_in_meta_namespace(self) -> None:
        """merge_discographies is pure — its output is cacheable."""
        from tests.test_web_cache import FakeRedis
        fake = self._cache._redis
        assert isinstance(fake, FakeRedis)

        mb_rg = _catalogue_row(
            source="mb", row_id=self.RG_ID,
            primary_artist_id=self.ARTIST_ID,
        )
        discogs_rg = _catalogue_row(
            source="discogs", row_id="21491", primary_artist_id="3840",
        )

        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.get_library_artist", return_value=[]):
            mock_mb.search_artists.return_value = [
                {"id": self.ARTIST_ID, "name": "Radiohead"}]
            mock_mb.get_artist_release_groups.return_value = [mb_rg]
            mock_mb.get_artist_name.return_value = "Radiohead"
            mock_dg.search_artists.return_value = [
                {"id": "3840", "name": "Radiohead"}]
            mock_dg.get_artist_releases.return_value = [discogs_rg]
            mock_dg.get_artist_name.return_value = "Radiohead"

            s1, _ = self._get("/api/artist/compare?name=Radiohead")
            s2, _ = self._get("/api/artist/compare?name=Radiohead")
            self.assertEqual(s1, 200)
            self.assertEqual(s2, 200)
            # Pure MB/Discogs discography fetches are called once across
            # both requests — their outputs went into the skeleton cache.
            self.assertEqual(mock_mb.get_artist_release_groups.call_count, 1)
            self.assertEqual(mock_dg.get_artist_releases.call_count, 1)

        meta_keys = [k for k in fake._store if k.startswith("meta:")
                     and "compare" in k]
        self.assertEqual(
            meta_keys,
            [f"meta:artist:compare:v6:{self.ARTIST_ID}:3840"],
            "the bulk-consumer deployment must create a naturally cold "
            "compare key without reusing a pre-bulk skeleton",
        )

    def test_compare_artist_names_are_canonical_not_user_supplied(self) -> None:
        """Codex round 4: previously the compare skeleton cached
        user-supplied artist names inside the response body, so the
        first request's `name=` query param won for 24h. Canonical
        names from the MB/Discogs API must be used instead.
        """
        mb_rg = _catalogue_row(
            source="mb", row_id=self.RG_ID,
            primary_artist_id=self.ARTIST_ID,
        )
        discogs_rg = _catalogue_row(
            source="discogs", row_id="21491", primary_artist_id="3840",
        )

        # First request — misspelled name. Skeleton gets cached.
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.get_library_artist", return_value=[]):
            mock_mb.search_artists.return_value = [
                {"id": self.ARTIST_ID, "name": "Radiohead"}]
            mock_mb.get_artist_release_groups.return_value = [mb_rg]
            mock_mb.get_artist_name.return_value = "Radiohead"
            mock_dg.search_artists.return_value = [
                {"id": "3840", "name": "Radiohead"}]
            mock_dg.get_artist_releases.return_value = [discogs_rg]
            mock_dg.get_artist_name.return_value = "Radiohead"
            _s, first = self._get(
                "/api/artist/compare?name=Radiohea&"
                f"mbid={self.ARTIST_ID}&discogs_id=3840")

        # mb_artist name must be canonical from MB, not the typo.
        self.assertEqual(
            (first["mb_artist"] or {}).get("name"), "Radiohead",
            "mb_artist.name must be the canonical name from MB, not "
            "the user-supplied ?name= query param — otherwise a typo "
            "on the first request poisons the 24h skeleton cache.")

        # Second request — different (correct) name. Must STILL return
        # the canonical Radiohead, and the skeleton cache must have been
        # reused (no re-fetch of the release-group metadata).
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.get_library_artist", return_value=[]):
            mock_mb.search_artists.return_value = [
                {"id": self.ARTIST_ID, "name": "Radiohead"}]
            mock_mb.get_artist_release_groups.return_value = [mb_rg]
            mock_mb.get_artist_name.return_value = "Radiohead"
            mock_dg.search_artists.return_value = [
                {"id": "3840", "name": "Radiohead"}]
            mock_dg.get_artist_releases.return_value = [discogs_rg]
            mock_dg.get_artist_name.return_value = "Radiohead"
            _s, second = self._get(
                "/api/artist/compare?name=Radiohead&"
                f"mbid={self.ARTIST_ID}&discogs_id=3840")
            # Expensive metadata fetch was served from cache (skeleton
            # still reusable despite different ?name=).
            self.assertEqual(mock_mb.get_artist_release_groups.call_count, 0)

        self.assertEqual(
            (second["mb_artist"] or {}).get("name"), "Radiohead")

    def test_compare_overlay_reflects_library_flip(self) -> None:
        """Even with the compare skeleton cached, annotate_in_library
        must run on every request so badges flip with beets state."""
        mb_rg = _catalogue_row(
            source="mb", row_id=self.RG_ID,
            primary_artist_id=self.ARTIST_ID,
        )
        discogs_rg = _catalogue_row(
            source="discogs", row_id="21491", primary_artist_id="3840",
        )

        def _run(lib_albums: list[dict]) -> dict:
            with patch("web.server.mb_api") as mock_mb, \
                    patch("web.routes.browse.discogs_api") as mock_dg, \
                    patch("web.server.get_library_artist",
                          return_value=lib_albums):
                mock_mb.search_artists.return_value = [
                    {"id": self.ARTIST_ID, "name": "Radiohead"}]
                mock_mb.get_artist_release_groups.return_value = [mb_rg]
                mock_mb.get_artist_name.return_value = "Radiohead"
                mock_dg.search_artists.return_value = [
                    {"id": "3840", "name": "Radiohead"}]
                mock_dg.get_artist_releases.return_value = [discogs_rg]
                mock_dg.get_artist_name.return_value = "Radiohead"
                _s, data = self._get("/api/artist/compare?name=Radiohead")
                return data

        first = _run([])
        self.assertFalse(first["both"][0]["mb"].get("in_library"))

        # Library flips — beets now holds this album.
        lib_album = {
            "mb_albumid": self.RELEASE_ID,
            "mb_releasegroupid": self.RG_ID,
            "album": "OK Computer",
            "formats": "MP3",
            "min_bitrate": 320000,
            "avg_bitrate": 320000,
        }
        second = _run([lib_album])
        self.assertTrue(
            second["both"][0]["mb"].get("in_library"),
            "Compare overlay must run per-request — a warm skeleton "
            "cache must not mask a library-state change.")

if __name__ == "__main__":
    unittest.main()
