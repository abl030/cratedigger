"""Offline contract tests for the production YouTube Requests transport.

The loopback server drives the actual ``Session`` and ``HTTPAdapter`` returned
by the shared production factory. Only urllib3's backoff sleep is suppressed:
Requests still prepares, sends, retries, and raises its public ``RetryError``.
"""

from __future__ import annotations

import copy
import threading
import unittest
from contextlib import AbstractContextManager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Literal, Self, cast
from unittest.mock import patch

import msgspec
import requests
from requests.adapters import HTTPAdapter

from lib.beets_distance import BeetsDistanceResult
from lib.pipeline_db import PersistedYoutubeRow
from lib.youtube_album_service import (
    OUTCOME_EXIT_CODE,
    OUTCOME_HTTP_STATUS,
    ResolvedYoutubeRelease,
    YoutubeAlbumResolverResult,
    resolve_youtube_album,
)
from lib.youtube_transport import build_youtube_client
from tests.fakes import (
    FakeDiscogsLookup,
    FakeMBLookup,
    FakePipelineDB,
    FakeYTMusic,
)

RETRYABLE_STATUSES = (429, 500, 502, 503, 504)
RETRYABLE_METHODS = ("GET", "POST")

_RG = "11111111-1111-1111-1111-111111111111"
_RELEASE = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_SEED_BROWSE_ID = "MPREb-seed"
_BROKEN_BROWSE_ID = "MPREb-broken"
_HEALTHY_BROWSE_ID = "MPREb-healthy"
_CACHED_BROWSE_ID = "MPREb-cached"

CachePosture = Literal["absent", "empty", "nonempty"]
OperationSite = Literal["search", "seed", "sibling", "siblings_all"]
RequestMethod = Literal["GET", "POST"]


@dataclass(frozen=True)
class RetryWorld:
    status: int
    method: RequestMethod
    cache_posture: CachePosture
    refresh: bool
    operation_site: OperationSite


@dataclass
class RetryObservation:
    world: RetryWorld
    result: object | None
    escaped_exception: Exception | None
    attempts: int
    upsert_calls: int
    durable_before: list[dict[str, Any]] | None
    durable_after: list[dict[str, Any]] | None


class _RetryHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, status: int) -> None:
        self.status = status
        self.attempts = 0
        self.methods: list[str] = []
        super().__init__(("127.0.0.1", 0), _RetryHandler)


class _RetryHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _reply(self) -> None:
        server = cast(_RetryHTTPServer, self.server)
        server.attempts += 1
        server.methods.append(self.command)
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length:
            self.rfile.read(content_length)
        self.send_response(server.status)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()

    do_GET = _reply
    do_POST = _reply

    def log_message(self, format: str, *args: object) -> None:
        del format, args


class LoopbackRetryServer(AbstractContextManager["LoopbackRetryServer"]):
    """Always return one retryable status and record every real attempt."""

    def __init__(self, status: int) -> None:
        self._server = _RetryHTTPServer(status)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )

    @property
    def url(self) -> str:
        host, port = cast(tuple[str, int], self._server.server_address)
        return f"http://{host}:{port}/youtube"

    @property
    def attempts(self) -> int:
        return self._server.attempts

    @property
    def methods(self) -> list[str]:
        return list(self._server.methods)

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        del exc_info
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


class _ObservedYoutubeDB(FakePipelineDB):
    def __init__(self) -> None:
        super().__init__()
        self.youtube_upsert_calls: list[
            tuple[str, str, list[PersistedYoutubeRow]]
        ] = []

    def upsert_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
        rows: list[PersistedYoutubeRow],
    ) -> None:
        self.youtube_upsert_calls.append(
            (release_group_identifier, source, copy.deepcopy(rows)))
        super().upsert_youtube_album_mapping(
            release_group_identifier, source, rows)


def _track(title: str, video_id: str) -> dict[str, Any]:
    return {
        "videoId": video_id,
        "title": title,
        "artists": [{"name": "Dr. Octagon", "id": "UCx"}],
        "album": {"name": "Dr. Octagonecologyst", "id": _SEED_BROWSE_ID},
        "duration": "1:00",
        "duration_seconds": 60,
        "trackNumber": 1,
        "isAvailable": True,
        "isExplicit": False,
    }


def _album(
    browse_id: str,
    *,
    other_versions: list[str] | None = None,
) -> dict[str, Any]:
    return FakeYTMusic.make_album_fixture(
        audio_playlist_id=f"OLAK-{browse_id}",
        title="Dr. Octagonecologyst",
        artists=[{"name": "Dr. Octagon", "id": "UCx"}],
        year="1996",
        tracks=[_track("Intro", f"vid-{browse_id}")],
        other_versions=[
            {
                "browseId": sibling,
                "title": "Dr. Octagonecologyst",
                "artists": [{"name": "Dr. Octagon", "id": "UCx"}],
                "year": "1996",
                "thumbnails": [],
                "isExplicit": False,
            }
            for sibling in (other_versions or [])
        ],
    )


class LoopbackYTClient:
    """Minimal YT collaborator whose selected operation uses real Requests."""

    def __init__(
        self,
        session: requests.Session,
        url: str,
        *,
        method: RequestMethod,
        operation_site: OperationSite,
    ) -> None:
        self._session = session
        self._url = url
        self._method = method
        self._operation_site = operation_site

    def _exhaust(self) -> None:
        response = self._session.request(self._method, self._url)
        response.raise_for_status()
        raise AssertionError("retryable response unexpectedly succeeded")

    def search(self, *_args: object, **_kwargs: object) -> list[dict[str, Any]]:
        if self._operation_site == "search":
            self._exhaust()
        return [{
            "browseId": _SEED_BROWSE_ID,
            "resultType": "album",
            "title": "Dr. Octagonecologyst",
            "artists": [{"name": "Dr. Octagon", "id": "UCx"}],
            "year": "1996",
            "type": "Album",
            "thumbnails": [],
            "isExplicit": False,
            "playlistId": None,
            "trackCount": 1,
        }]

    def get_album(self, browse_id: str) -> dict[str, Any]:
        if self._operation_site == "seed" and browse_id == _SEED_BROWSE_ID:
            self._exhaust()
        if self._operation_site in ("sibling", "siblings_all"):
            if browse_id == _SEED_BROWSE_ID:
                return _album(
                    browse_id,
                    other_versions=[_BROKEN_BROWSE_ID, _HEALTHY_BROWSE_ID],
                )
            if browse_id == _BROKEN_BROWSE_ID:
                self._exhaust()
            if browse_id == _HEALTHY_BROWSE_ID:
                if self._operation_site == "siblings_all":
                    self._exhaust()
                return _album(browse_id)
        return _album(browse_id)


_CACHED_ROWS: list[dict[str, Any]] = [{
    "yt_browse_id": _CACHED_BROWSE_ID,
    "yt_audio_playlist_id": "OLAK-cached",
    "yt_url": "https://music.youtube.com/playlist?list=OLAK-cached",
    "yt_year": 1996,
    "yt_track_count": 1,
    "album_title": "Dr. Octagonecologyst",
    "album_artist": "Dr. Octagon",
    "yt_tracks": [],
    "distances": [],
}]


def expected_cached_releases() -> list[ResolvedYoutubeRelease]:
    return [ResolvedYoutubeRelease(
        yt_browse_id=_CACHED_BROWSE_ID,
        yt_audio_playlist_id="OLAK-cached",
        yt_url="https://music.youtube.com/playlist?list=OLAK-cached",
        year=1996,
        track_count=1,
        tracks=[],
        distances=[],
    )]


def _distance(*, mbid: str, **_kwargs: object) -> BeetsDistanceResult:
    return BeetsDistanceResult(
        outcome="ok",
        distance=0.05,
        matched_tracks=1,
        total_local_tracks=1,
        total_mb_tracks=1,
        extra_local_tracks=0,
        extra_mb_tracks=0,
        components={"tracks": 0.05},
        candidate_mbid=mbid,
        candidate_release_group_id=_RG,
        request_release_group_id=_RG,
    )


def _mb_release() -> dict[str, Any]:
    return {
        "id": _RELEASE,
        "title": "Dr. Octagonecologyst",
        "artist_name": "Dr. Octagon",
        "artist_id": "artist-1",
        "release_group_id": _RG,
        "date": "1996-01-01",
        "year": 1996,
        "country": "US",
        "status": "Official",
        "tracks": [{
            "disc_number": 1,
            "track_number": 1,
            "title": "Intro",
            "length_seconds": 60.0,
        }],
    }


def _mb_group() -> dict[str, Any]:
    return {
        "title": "Dr. Octagonecologyst",
        "type": "Album",
        "releases": [{
            "id": _RELEASE,
            "title": "Dr. Octagonecologyst",
            "date": "1996-01-01",
            "country": "US",
            "status": "Official",
            "track_count": 1,
            "format": "CD",
            "media_count": 1,
        }],
    }


def run_retry_world(
    world: RetryWorld,
) -> RetryObservation:
    """Drive one generated world through the shared transport and resolver."""
    db = _ObservedYoutubeDB()
    if world.cache_posture == "empty":
        db.seed_youtube_album_mapping(_RG, "mb", [])
    elif world.cache_posture == "nonempty":
        db.seed_youtube_album_mapping(_RG, "mb", _CACHED_ROWS)

    durable_before = db.get_youtube_album_mapping(_RG, "mb")
    mb_leaf = FakeMBLookup({_RELEASE: _mb_release()})
    mb_group = FakeMBLookup({_RG: _mb_group()})
    discogs = FakeDiscogsLookup()

    result: object | None = None
    escaped: Exception | None = None
    with LoopbackRetryServer(world.status) as server:
        _unused_client, session = build_youtube_client()
        session.trust_env = False
        try:
            yt_client = LoopbackYTClient(
                session,
                server.url,
                method=world.method,
                operation_site=world.operation_site,
            )
            with patch("urllib3.util.retry.time.sleep", return_value=None):
                try:
                    result = resolve_youtube_album(
                        _RELEASE,
                        pdb=db,
                        mb_get_release=mb_leaf,
                        mb_get_release_group_releases=mb_group,
                        discogs_get_release=discogs,
                        discogs_get_master_releases=discogs,
                        yt_client=yt_client,
                        distance_fn=_distance,
                        cache=None,
                        refresh=world.refresh,
                        sleep_fn=lambda _seconds: None,
                    )
                except Exception as exc:  # noqa: BLE001 - checker records boundary escapes
                    escaped = exc
        finally:
            session.close()
        attempts = server.attempts

    return RetryObservation(
        world=world,
        result=result,
        escaped_exception=escaped,
        attempts=attempts,
        upsert_calls=len(db.youtube_upsert_calls),
        durable_before=durable_before,
        durable_after=db.get_youtube_album_mapping(_RG, "mb"),
    )


def assert_retry_invariants(observation: RetryObservation) -> None:
    """Check retry count plus the world-specific service/cache contract."""
    world = observation.world
    cache_short_circuit = (
        not world.refresh and world.cache_posture != "absent")
    expected_attempts = (
        0 if cache_short_circuit
        else 8 if world.operation_site == "siblings_all"
        else 4
    )
    if observation.attempts != expected_attempts:
        raise AssertionError(
            f"expected {expected_attempts} {world.method} attempts; "
            f"observed {observation.attempts}")
    if observation.escaped_exception is not None:
        raise AssertionError(
            f"{type(observation.escaped_exception).__module__}."
            f"{type(observation.escaped_exception).__name__} escaped resolver")
    if not isinstance(observation.result, YoutubeAlbumResolverResult):
        raise TypeError(
            "resolver did not return YoutubeAlbumResolverResult")

    result = observation.result
    if cache_short_circuit:
        if result.outcome != "ok" or not result.from_cache:
            raise AssertionError(
                "non-refresh durable hit must return ok/from_cache")
        expected_releases = (
            expected_cached_releases()
            if world.cache_posture == "nonempty"
            else []
        )
        if msgspec.to_builtins(result.youtube_releases) != (
                msgspec.to_builtins(expected_releases)):
            raise AssertionError(
                "non-refresh durable hit returned the wrong matrix")
        if observation.upsert_calls != 0:
            raise AssertionError(
                "non-refresh durable hit must not upsert")
        if observation.durable_after != observation.durable_before:
            raise AssertionError(
                "non-refresh durable hit rewrote durable state")
    elif world.operation_site == "sibling":
        if result.outcome != "ok" or result.from_cache:
            raise AssertionError(
                "one exhausting sibling must retain a fresh ok result")
        browse_ids = [r.yt_browse_id for r in result.youtube_releases]
        expected_ids = [_SEED_BROWSE_ID, _HEALTHY_BROWSE_ID]
        if browse_ids != expected_ids:
            raise AssertionError(
                f"sibling isolation mismatch: expected {expected_ids!r}, "
                f"got {browse_ids!r}")
        if observation.upsert_calls != 1:
            raise AssertionError(
                "successful partial sibling matrix must be upserted once")
        if observation.durable_after is None:
            raise AssertionError(
                "successful partial sibling matrix was not durable")
        durable_ids = [
            row.get("yt_browse_id") for row in observation.durable_after]
        if (
            len(durable_ids) != len(expected_ids)
            or set(durable_ids) != set(expected_ids)
        ):
            raise AssertionError(
                f"partial durable matrix mismatch: expected {expected_ids!r}, "
                f"got {durable_ids!r}")
    else:
        if observation.upsert_calls != 0:
            raise AssertionError(
                "retry exhaustion must never upsert durable mappings")
        if observation.durable_after != observation.durable_before:
            raise AssertionError(
                "retry exhaustion rewrote durable state")

        if world.cache_posture == "nonempty":
            if result.outcome != "ok" or not result.from_cache:
                raise AssertionError(
                    "nonempty refresh exhaustion must return ok/from_cache")
            if msgspec.to_builtins(result.youtube_releases) != (
                    msgspec.to_builtins(expected_cached_releases())):
                raise AssertionError(
                    "nonempty refresh did not return the exact durable matrix")
            if (
                result.error_message is None
                or "unresolved_mirror_unavailable" not in result.error_message
            ):
                raise AssertionError(
                    "cache fallback did not record availability failure")
        else:
            if (
                result.outcome != "unresolved_mirror_unavailable"
                or result.from_cache
                or result.youtube_releases
            ):
                raise AssertionError(
                    "empty/absent refresh exhaustion must stay unavailable")

    expected_http = 200 if result.outcome == "ok" else 503
    expected_exit = 0 if result.outcome == "ok" else 5
    if OUTCOME_HTTP_STATUS[result.outcome] != expected_http:
        raise AssertionError("resolver outcome has the wrong HTTP mapping")
    if OUTCOME_EXIT_CODE[result.outcome] != expected_exit:
        raise AssertionError("resolver outcome has the wrong CLI mapping")


class TestProductionYoutubeRetryPolicy(unittest.TestCase):
    def test_cli_and_web_bind_the_shared_factory(self) -> None:
        from lib.youtube_transport import build_youtube_client
        from scripts.pipeline_cli.youtube import _build_youtube_client as cli_builder
        from web.routes.youtube import _build_youtube_client as web_builder

        self.assertIs(cli_builder, build_youtube_client)
        self.assertIs(web_builder, build_youtube_client)

    def test_shared_factory_retries_every_configured_status_and_method_four_times(
        self,
    ) -> None:
        _client, session = build_youtube_client()
        session.trust_env = False
        try:
            adapter = cast(
                HTTPAdapter, session.get_adapter("http://127.0.0.1/"))
            self.assertEqual(adapter.max_retries.total, 3)
            self.assertEqual(
                tuple(adapter.max_retries.status_forcelist),
                RETRYABLE_STATUSES,
            )
            self.assertEqual(
                adapter.max_retries.allowed_methods,
                frozenset(RETRYABLE_METHODS),
            )
            for status in RETRYABLE_STATUSES:
                for method in RETRYABLE_METHODS:
                    with self.subTest(
                        status=status, method=method,
                    ), LoopbackRetryServer(status) as server, patch(
                        "urllib3.util.retry.time.sleep", return_value=None,
                    ):
                        with self.assertRaises(
                            requests.exceptions.RetryError
                        ):
                            session.request(method, server.url)
                        self.assertEqual(server.attempts, 4)
                        self.assertEqual(server.methods, [method] * 4)
        finally:
            session.close()


class TestRetryExhaustionResolverIntegration(unittest.TestCase):
    def test_real_503_get_uncached_exhaustion_is_typed(self) -> None:
        observation = run_retry_world(RetryWorld(
            status=503,
            method="GET",
            cache_posture="absent",
            refresh=False,
            operation_site="search",
        ))
        assert_retry_invariants(observation)

    def test_real_503_get_absent_cache_refresh_is_unavailable_without_write(
        self,
    ) -> None:
        observation = run_retry_world(RetryWorld(
            status=503,
            method="GET",
            cache_posture="absent",
            refresh=True,
            operation_site="search",
        ))
        assert_retry_invariants(observation)
        result = cast(YoutubeAlbumResolverResult, observation.result)
        self.assertEqual(result.outcome, "unresolved_mirror_unavailable")
        self.assertEqual(observation.upsert_calls, 0)
        self.assertIsNone(observation.durable_after)

    def test_real_429_post_refresh_uses_exact_nonempty_fallback(self) -> None:
        observation = run_retry_world(RetryWorld(
            status=429,
            method="POST",
            cache_posture="nonempty",
            refresh=True,
            operation_site="seed",
        ))
        assert_retry_invariants(observation)

    def test_real_502_post_empty_refresh_stays_unavailable(self) -> None:
        observation = run_retry_world(RetryWorld(
            status=502,
            method="POST",
            cache_posture="empty",
            refresh=True,
            operation_site="seed",
        ))
        assert_retry_invariants(observation)

    def test_real_503_sibling_exhaustion_excludes_only_that_sibling(self) -> None:
        observation = run_retry_world(RetryWorld(
            status=503,
            method="GET",
            cache_posture="absent",
            refresh=False,
            operation_site="sibling",
        ))
        assert_retry_invariants(observation)

    def test_real_503_all_siblings_exhaust_with_cache_preserves_durable_rows(
        self,
    ) -> None:
        observation = run_retry_world(RetryWorld(
            status=503,
            method="GET",
            cache_posture="nonempty",
            refresh=True,
            operation_site="siblings_all",
        ))
        assert_retry_invariants(observation)

    def test_real_503_all_siblings_exhaust_absent_cache_is_unavailable(
        self,
    ) -> None:
        observation = run_retry_world(RetryWorld(
            status=503,
            method="GET",
            cache_posture="absent",
            refresh=True,
            operation_site="siblings_all",
        ))
        assert_retry_invariants(observation)


if __name__ == "__main__":
    unittest.main()
