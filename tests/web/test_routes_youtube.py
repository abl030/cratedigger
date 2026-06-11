#!/usr/bin/env python3
"""Contract tests for web/routes/youtube.py + pipeline youtube-rescue.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import contextlib
import json
import os
import sys
import unittest
from unittest.mock import patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _WebServerCase



class TestYoutubeRouteContracts(_WebServerCase):
    """U8 contract for ``GET /api/youtube-album?identifier=<id>``.

    Mirrors the CLI surface ``pipeline-cli youtube-album`` (U7); the
    route is the HTTP adapter wrapping
    ``lib.youtube_album_service.resolve_youtube_album``. The
    service-layer behaviour is the authority — these tests pin the
    HTTP-side contract: required response fields, the
    ``OUTCOME_HTTP_STATUS`` mapping (re-exported from the service),
    400 on missing ``identifier``, and the ``?refresh=true`` query
    forwarded to the service as ``refresh=True``.

    The service is patched at the route module's import site
    (``web.routes.youtube.resolve_youtube_album``) with fixture
    ``YoutubeAlbumResolverResult`` instances — production-shaped per
    the contract-test-mocks-must-mirror-production-shape rule (real
    typed Structs, not bare dicts).
    """

    REQUIRED_FIELDS = {
        "outcome",
        "release_group_identifier",
        "source",
        "from_cache",
        "youtube_releases",
        "error_message",
        "duration_ms",
    }

    REQUIRED_RELEASE_FIELDS = {
        "yt_browse_id",
        "yt_audio_playlist_id",
        "yt_url",
        "year",
        "track_count",
        "tracks",
        "distances",
    }

    REQUIRED_DISTANCE_FIELDS = {
        "mbid",
        "outcome",
        "distance",
        "components",
        "matched_tracks",
        "total_local_tracks",
        "total_mb_tracks",
        "extra_local_tracks",
        "extra_mb_tracks",
        "error_message",
    }

    UUID_A = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    UUID_B = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"

    def setUp(self) -> None:
        self.mock_db.reset_mock()
        from lib.youtube_album_service import (
            ResolvedDistance,
            ResolvedYoutubeRelease,
            YoutubeAlbumResolverResult,
        )
        from lib.beets_distance import SyntheticItem
        self._Result = YoutubeAlbumResolverResult
        self._Release = ResolvedYoutubeRelease
        self._Distance = ResolvedDistance
        self._SyntheticItem = SyntheticItem

    def _ok_result(self, *, from_cache: bool = False,
                   error_message: str | None = None):
        """Production-shaped ``ok`` result with one YT release × one MB
        sibling — exercises every required field on the wire."""
        track = self._SyntheticItem(
            title="Reckoner", artist="Radiohead", album="In Rainbows",
            albumartist="Radiohead", track=8, tracktotal=10,
            disc=1, disctotal=1, length=290.0,
        )
        distance = self._Distance(
            mbid=self.UUID_A,
            outcome="ok",
            distance=0.05,
            components={"album": 0.0, "artist": 0.0, "tracks": 0.05},
            matched_tracks=10,
            total_local_tracks=10,
            total_mb_tracks=10,
            error_message=None,
        )
        release = self._Release(
            yt_browse_id="MPREb_aaa",
            yt_audio_playlist_id="OLAK5uy_aaa",
            yt_url="https://music.youtube.com/playlist?list=OLAK5uy_aaa",
            year=2007,
            track_count=10,
            tracks=[track],
            distances=[distance],
        )
        return self._Result(
            outcome="ok",
            release_group_identifier="rg-1234",
            source="mb",
            from_cache=from_cache,
            youtube_releases=[release],
            error_message=error_message,
            duration_ms=42,
        )

    def _bare_result(self, outcome: str, *,
                     error_message: str | None = None):
        """Outcome-only result (no matrix) for failure-mode tests."""
        return self._Result(
            outcome=outcome,
            release_group_identifier=None,
            source=None,
            from_cache=False,
            youtube_releases=[],
            error_message=error_message,
            duration_ms=12,
        )

    @contextlib.contextmanager
    def _patch_service(self, return_value):
        """Patch the resolver call AND the collaborator constructors.

        The route handler constructs ``YTMusic`` and ``_RedisYoutubeCache``
        *before* calling ``resolve_youtube_album`` so the production path
        wires everything up cleanly. In the contract test we only care
        about the service call's return value, so we stub the
        construction helpers to return harmless sentinels. This also
        makes the test robust to other tests that may have
        monkey-patched ``requests.Session`` (ytmusicapi runs
        ``isinstance(requests_session, requests.Session)`` at
        construction time and crashes when the Session class is not a
        real type).

        ``_build_youtube_client`` returns ``(yt_client, session)`` so the
        route can close the session in ``finally`` (finding #18 — Session
        leak). The test stub mimics that shape; the fake session exposes
        a no-op ``close()`` method.
        """
        class _FakeSession:
            close_calls = 0

            def close(self) -> None:
                # Class-level counter so the test fixture can assert
                # close() was actually called (round 2 P2-2). Without
                # this, the close() helper in the route module could
                # be deleted and no test would catch it.
                type(self).close_calls += 1
                return None

        # Reset the counter for each ``_patch_service`` invocation so
        # close-count assertions are scoped to one test call.
        _FakeSession.close_calls = 0
        self._fake_session_cls = _FakeSession

        with patch(
            "web.routes.youtube._build_youtube_client",
            return_value=(object(), _FakeSession()),
        ), patch(
            "web.routes.youtube._RedisYoutubeCache",
            return_value=object(),
        ), patch(
            "web.routes.youtube.resolve_youtube_album",
            return_value=return_value,
        ) as mock_resolve:
            yield mock_resolve

    def test_status_mapping_is_imported_from_service(self):
        """``web.routes.youtube`` must re-export ``OUTCOME_HTTP_STATUS``
        from ``lib.youtube_album_service`` — single source of truth per
        the PR #381 lesson."""
        from web.routes import youtube as route_mod
        from lib import youtube_album_service as svc_mod
        self.assertIs(
            route_mod.OUTCOME_HTTP_STATUS,
            svc_mod.OUTCOME_HTTP_STATUS,
        )

    def test_ok_returns_200_with_required_fields(self):
        with self._patch_service(self._ok_result()):
            status, data = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS,
                                "youtube-album ok response")
        self.assertEqual(data["outcome"], "ok")
        self.assertEqual(data["source"], "mb")
        self.assertFalse(data["from_cache"])
        self.assertEqual(len(data["youtube_releases"]), 1)
        _assert_required_fields(
            self, data["youtube_releases"][0],
            self.REQUIRED_RELEASE_FIELDS,
            "youtube_releases[0] entry",
        )
        self.assertEqual(len(data["youtube_releases"][0]["distances"]), 1)
        _assert_required_fields(
            self, data["youtube_releases"][0]["distances"][0],
            self.REQUIRED_DISTANCE_FIELDS,
            "distances[0] entry",
        )

    def test_ok_from_cache_with_error_message_still_200(self):
        """AE6: cache fallback path — service returns ``ok`` with
        ``from_cache=True`` and a non-empty ``error_message`` (the YT
        upstream failed but the cache served a useful result). The
        route returns 200 because the matrix is real."""
        with self._patch_service(self._ok_result(
                from_cache=True,
                error_message="YT 429 — served from cache")):
            status, data = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        self.assertTrue(data["from_cache"])
        self.assertEqual(data["error_message"],
                         "YT 429 — served from cache")

    def test_missing_identifier_returns_400(self):
        status, data = self._get("/api/youtube-album")
        self.assertEqual(status, 400)
        self.assertEqual(
            data.get("error"),
            "identifier query parameter is required",
        )

    def test_empty_identifier_returns_400(self):
        status, data = self._get("/api/youtube-album?identifier=")
        self.assertEqual(status, 400)
        self.assertEqual(
            data.get("error"),
            "identifier query parameter is required",
        )

    def test_not_found_returns_404(self):
        with self._patch_service(self._bare_result(
                "not_found",
                error_message="identifier 'nope' is neither MB nor Discogs")):
            status, data = self._get(
                "/api/youtube-album?identifier=nope")
        self.assertEqual(status, 404)
        self.assertEqual(data["outcome"], "not_found")

    def test_unresolved_4xx_client_returns_503(self):
        with self._patch_service(self._bare_result(
                "unresolved_4xx_client",
                error_message="YT 429 throttled")):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 503)

    def test_unresolved_mirror_unavailable_returns_503(self):
        with self._patch_service(self._bare_result(
                "unresolved_mirror_unavailable",
                error_message="YT 503")):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 503)

    def test_unresolved_timeout_returns_503(self):
        with self._patch_service(self._bare_result(
                "unresolved_timeout",
                error_message="requests.Timeout")):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 503)

    def test_youtube_parse_failed_returns_503(self):
        with self._patch_service(self._bare_result(
                "youtube_parse_failed",
                error_message="ytmusicapi parse error")):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 503)

    def test_refresh_true_is_forwarded_to_service(self):
        """AE5: ``?refresh=true`` must reach the service as
        ``refresh=True`` so the cache bypass actually happens."""
        with self._patch_service(self._ok_result()) as mock_resolve:
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}&refresh=true")
        self.assertEqual(status, 200)
        self.assertEqual(mock_resolve.call_count, 1)
        kwargs = mock_resolve.call_args.kwargs
        self.assertIs(kwargs["refresh"], True)

    def test_refresh_omitted_defaults_to_false(self):
        with self._patch_service(self._ok_result()) as mock_resolve:
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        kwargs = mock_resolve.call_args.kwargs
        self.assertIs(kwargs["refresh"], False)

    def test_refresh_false_string_is_not_truthy(self):
        with self._patch_service(self._ok_result()) as mock_resolve:
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}&refresh=false")
        self.assertEqual(status, 200)
        kwargs = mock_resolve.call_args.kwargs
        self.assertIs(kwargs["refresh"], False)

    def test_identifier_is_forwarded_to_service(self):
        with self._patch_service(self._ok_result()) as mock_resolve:
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        # First positional arg is the identifier.
        self.assertEqual(mock_resolve.call_args.args[0], self.UUID_A)

    def test_session_close_called_on_happy_path(self):
        """Round 2 P2-2: the route's ``finally`` block must call
        ``session.close()`` so the requests Session's connection pool
        is released (finding #18). Without an assertion here, a
        regression that removed the close call would not trip any
        existing test.
        """
        with self._patch_service(self._ok_result()):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        self.assertEqual(
            self._fake_session_cls.close_calls, 1,
            msg="route must call session.close() exactly once on "
                "happy-path resolves (round 2 P2-2)",
        )

    def test_session_close_called_when_service_raises(self):
        """If ``resolve_youtube_album`` raises mid-request, the
        ``finally`` clause still releases the session — the route
        must not leak a connection pool because of an exception.
        """
        from lib.youtube_album_service import OUTCOME_HTTP_STATUS  # noqa: F401

        class _FakeSession:
            close_calls = 0

            def close(self) -> None:
                type(self).close_calls += 1
                return None

        _FakeSession.close_calls = 0

        def _raising_resolver(*_a, **_kw):
            raise RuntimeError("simulated mid-request failure")

        with patch(
            "web.routes.youtube._build_youtube_client",
            return_value=(object(), _FakeSession()),
        ), patch(
            "web.routes.youtube._RedisYoutubeCache",
            return_value=object(),
        ), patch(
            "web.routes.youtube.resolve_youtube_album",
            side_effect=_raising_resolver,
        ):
            # The route will 500 because the resolver raised; we only
            # care that the session was still closed.
            try:
                self._get(f"/api/youtube-album?identifier={self.UUID_A}")
            except Exception:
                pass

        self.assertEqual(
            _FakeSession.close_calls, 1,
            msg="route must close the session even when the resolver "
                "raises mid-request (round 2 P2-2)",
        )


class TestPipelineYoutubeRescueContract(_WebServerCase):
    """U5 contract for ``POST /api/pipeline/<id>/youtube-rescue``.

    The endpoint wraps ``YoutubeIngestService.submit``. Both the CLI
    (``pipeline-cli youtube-rescue``, U4) and the API live or die on
    the same service contract — see ``CLAUDE.md`` § "CLI ⇄ API surface
    symmetry". Status-code mapping is imported from
    ``lib.youtube_ingest_service.OUTCOME_HTTP_STATUS`` (single source
    of truth shared with the CLI's ``OUTCOME_EXIT_CODE``).
    """

    REQUIRED_FIELDS = {"download_log_id", "outcome", "detail"}

    def _patch_service(self, **result_kwargs):
        """Patch ``YoutubeIngestService.submit`` to return a canned
        :class:`SubmitResult`.

        Production-shape fidelity: ``SubmitResult`` is the real
        ``msgspec.Struct`` returned by the service; we construct it
        with the exact field types the service does so JSON encoding
        through ``msgspec.to_builtins`` round-trips faithfully.
        """
        from unittest.mock import patch as _patch
        from lib.youtube_ingest_service import SubmitResult
        return _patch(
            "lib.youtube_ingest_service.YoutubeIngestService.submit",
            return_value=SubmitResult(**result_kwargs),
        )

    # ----- happy path -----

    def test_accepted_returns_200_with_required_fields(self):
        with self._patch_service(
                outcome="accepted", download_log_id=42, detail=None):
            status, data = self._post(
                "/api/pipeline/100/youtube-rescue",
                {"browse_id": "MPREb_abc"})
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS,
                                "youtube-rescue accepted response")
        self.assertEqual(data["outcome"], "accepted")
        self.assertEqual(data["download_log_id"], 42)
        # 200 responses do NOT carry a top-level ``error`` field.
        self.assertNotIn("error", data)

    # ----- outcome → HTTP status subTest table -----

    def test_status_codes_match_service_table_for_every_outcome(self):
        """Every ``SubmitOutcome`` maps to its ``OUTCOME_HTTP_STATUS``
        entry — single-source-of-truth contract.

        Mirrors the CLI's ``test_exit_codes_match_service_table_for_every_outcome``
        so the two surfaces stay in lockstep.
        """
        from lib.youtube_ingest_service import OUTCOME_HTTP_STATUS as TABLE
        cases = [
            ("accepted",                    200),
            ("request_not_found",           404),
            ("wrong_state",                 409),
            ("in_flight",                   409),
            ("no_resolver_mapping",         422),
            ("track_count_precheck_failed", 422),
            ("transient",                   503),
        ]
        # The table itself must match the literal coverage above —
        # forces future contributors to keep the subTest table and
        # the service map aligned.
        self.assertEqual(set(o for o, _ in cases), set(TABLE),
                         "subTest cases drifted from OUTCOME_HTTP_STATUS")
        for outcome, expected_status in cases:
            with self.subTest(outcome=outcome):
                # ``accepted`` / ``in_flight`` carry a populated
                # ``download_log_id``; the rest leave it None.
                dl_id = 7 if outcome in ("accepted", "in_flight") else None
                with self._patch_service(
                        outcome=outcome,
                        download_log_id=dl_id,
                        detail=f"detail for {outcome}"):
                    status, data = self._post(
                        "/api/pipeline/100/youtube-rescue",
                        {"browse_id": "MPREb_abc"})
                self.assertEqual(status, expected_status)
                self.assertEqual(status, TABLE[outcome])
                _assert_required_fields(
                    self, data, self.REQUIRED_FIELDS,
                    f"{outcome} response shape")
                self.assertEqual(data["outcome"], outcome)
                if outcome != "accepted":
                    # Non-2xx responses carry the legacy ``error`` field
                    # for older frontend toasts that grep on it.
                    self.assertIn("error", data)
                    # ``detail`` is also surfaced verbatim in ``detail``.
                    self.assertIsNotNone(data["detail"])

    # ----- in_flight surfaces existing id (Covers AE3 from API side) -----

    def test_in_flight_returns_existing_download_log_id(self):
        with self._patch_service(
                outcome="in_flight",
                download_log_id=99,
                detail="existing download_log_id=99 is in youtube_running "
                "state for request 100"):
            status, data = self._post(
                "/api/pipeline/100/youtube-rescue",
                {"browse_id": "MPREb_abc"})
        self.assertEqual(status, 409)
        self.assertEqual(data["outcome"], "in_flight")
        self.assertEqual(data["download_log_id"], 99)
        self.assertIn("99", data["error"])

    # ----- body validation (parse_body / Pydantic) -----

    def test_missing_browse_id_returns_400(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.youtube_ingest_service.YoutubeIngestService.submit",
        ) as mock_submit:
            status, data = self._post(
                "/api/pipeline/100/youtube-rescue", {})
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertIn("browse_id", data["error"])
        # Service must NOT have been called — parse_body short-circuited.
        mock_submit.assert_not_called()

    def test_non_string_browse_id_returns_400(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.youtube_ingest_service.YoutubeIngestService.submit",
        ) as mock_submit:
            status, data = self._post(
                "/api/pipeline/100/youtube-rescue",
                {"browse_id": 12345})
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        mock_submit.assert_not_called()

    def test_non_object_body_returns_400(self):
        """``parse_body`` rejects non-object JSON bodies (string / list /
        number) with 400 — same shape as ``test_advance_rejects_non_int_ordinal``.
        The route's body parser is the single adapter, not inline."""
        from unittest.mock import patch as _patch
        raw_body = b'"hello"'  # valid JSON but not an object
        req = Request(
            f"{self.base}/api/pipeline/100/youtube-rescue",
            data=raw_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with _patch(
            "lib.youtube_ingest_service.YoutubeIngestService.submit",
        ) as mock_submit:
            try:
                resp = urlopen(req, timeout=5)
                status = resp.status
                data = json.loads(resp.read())
            except HTTPError as e:
                status = e.code
                data = json.loads(e.read())
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        mock_submit.assert_not_called()

    # ----- URL path validation -----

    def test_passes_url_request_id_and_body_browse_id_to_service(self):
        """The service must receive (request_id from URL, browse_id from
        body). Regression guard against a future refactor that picks
        ``request_id`` off the body."""
        from unittest.mock import patch as _patch
        from lib.youtube_ingest_service import SubmitResult
        with _patch(
            "lib.youtube_ingest_service.YoutubeIngestService.submit",
            return_value=SubmitResult(
                outcome="accepted", download_log_id=1, detail=None),
        ) as mock_submit:
            status, _ = self._post(
                "/api/pipeline/12345/youtube-rescue",
                {"browse_id": "MPREb_xyz"})
        self.assertEqual(status, 200)
        mock_submit.assert_called_once()
        # ``submit(request_id, browse_id)`` — positional args.
        args = mock_submit.call_args.args
        self.assertEqual(args[0], 12345)
        self.assertEqual(args[1], "MPREb_xyz")

if __name__ == "__main__":
    unittest.main()
