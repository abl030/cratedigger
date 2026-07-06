"""YouTube Music album resolver — HTTP route module (U8).

Single GET endpoint, ``/api/youtube-album?identifier=<id>&refresh=<bool>``,
that wraps ``lib.youtube_album_service.resolve_youtube_album``. The CLI
counterpart (U7) lives at ``scripts/pipeline_cli/youtube.py::cmd_youtube_album``;
both surfaces share the same service + outcome vocabulary per
``CLAUDE.md`` § "CLI ⇄ API surface symmetry".

The outcome → HTTP status mapping is imported directly from the service
module (``OUTCOME_HTTP_STATUS``) — one source of truth, per the PR #381
lesson. Do not redefine it locally.

The Redis cache adapter is the same shape as
``_RedisFingerprintCache`` in ``web/routes/pipeline.py`` — bytes
``get`` / ``set`` with a long-sentinel TTL since the YT response cache
lives forever absent explicit ``refresh=true``.
"""

from __future__ import annotations

import logging

import msgspec
from pydantic import BaseModel

from lib.youtube_album_service import (
    OUTCOME_HTTP_STATUS,
    resolve_youtube_album,
)
from lib.youtube_ingest_service import (
    OUTCOME_HTTP_STATUS as YOUTUBE_INGEST_HTTP_STATUS,
    default_youtube_ingest_service_factory,
)
from web import discogs as discogs_api
from web import mb as mb_api
from web.routes._pydantic import parse_body
from web.routes._registry import RouteRegistration, pattern_route, route
from web.routes._server_access import _server


log = logging.getLogger(__name__)


# Re-export from the service module so callers (tests, downstream
# wrappers) can `from web.routes.youtube import OUTCOME_HTTP_STATUS`
# without reaching into ``lib.youtube_album_service``. The ``is``
# identity is asserted in the contract test — there is no second source
# of truth.
__all__ = [
    "ROUTES",
    "OUTCOME_HTTP_STATUS",
    "YOUTUBE_INGEST_HTTP_STATUS",
    "YoutubeRescueRequest",
    "get_youtube_album",
    "post_pipeline_youtube_rescue",
]


class _RedisYoutubeCache:
    """Adapt ``web/cache.py``'s Redis client to the
    ``BeetsDistanceCache`` protocol.

    The service-side keys already carry the ``youtube:album:`` /
    ``youtube:search:`` namespace; this adapter does NOT prefix them
    again (review finding #17 — the old ``_NAMESPACE`` wrapper produced
    ``youtube:album:youtube:album:<browse_id>`` keys).

    Mirrors ``_RedisFingerprintCache`` in ``web/routes/pipeline.py``
    (and ``scripts/pipeline_cli/youtube.py::_RedisYoutubeCache`` on the CLI
    side) — bytes get/set with a long sentinel TTL. Falls back to a
    no-op when Redis is unavailable so single-shot dev shells still
    work without the in-process accelerator.
    """

    def __init__(self) -> None:
        try:
            from web import cache as _cache_mod
            self._redis = getattr(_cache_mod, "_redis", None)
        except Exception:
            self._redis = None

    def get(self, key: str):
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(key)  # type: ignore[union-attr]
        except Exception:
            return None
        if raw is None:
            return None
        # web/cache.py initialises Redis with ``decode_responses=True``,
        # so ``get`` returns str. Encode to bytes for the protocol.
        if isinstance(raw, str):
            return raw.encode("utf-8")
        return raw

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        if self._redis is None:
            return
        try:
            self._redis.setex(  # type: ignore[union-attr]
                key, ttl_seconds, value)
        except Exception:
            pass


def _build_youtube_client():
    """Construct a ``YTMusic`` client with retry + jittered desktop
    headers per the Key Technical Decisions (R5 / external research).

    Lazy-imports ``requests``, ``urllib3``, and ``ytmusicapi`` so the
    web server's startup cost stays low and unrelated routes don't
    pay for unused HTTP machinery. Mirrors
    ``scripts/pipeline_cli/youtube.py::_build_youtube_client``.

    Returns a ``(yt_client, session)`` tuple so the caller can close
    the session in a ``finally`` block — without that, every YT route
    invocation leaks a TCP connection pool (finding #18).

    The session also binds a default ``(connect, read)`` timeout of
    ``(5, 30)`` so unresponsive YT endpoints can't pin a worker
    forever (finding #4). ``requests`` exposes no Session-level
    timeout config; ``functools.partial`` on ``session.request`` is
    the established pattern.
    """
    from functools import partial
    import requests
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
    from ytmusicapi import YTMusic

    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    # Bind a default (connect, read) timeout so unresponsive remotes
    # don't pin the worker forever. Per-call ``timeout=`` kwargs
    # still override this default.
    session.request = partial(  # type: ignore[method-assign]
        session.request, timeout=(5, 30))
    return YTMusic(requests_session=session, language="en"), session


def _parse_bool(raw: str | None) -> bool:
    """Strict boolean parse for query params.

    ``"true"`` and ``"1"`` (case-insensitive) → True; anything else
    (including ``None``, empty string, ``"false"``, ``"0"``) → False.
    Same shape the CLI's ``--refresh`` flag produces.
    """
    if raw is None:
        return False
    return raw.strip().lower() in ("true", "1")


def get_youtube_album(h, params: dict[str, list[str]]) -> None:
    """``GET /api/youtube-album?identifier=<id>&refresh=<true|false>``.

    Resolves any MB / Discogs release-or-group identifier into the
    YouTube Music distance matrix. Counterpart of ``pipeline-cli
    youtube-album`` (U7). Both surfaces wrap
    ``lib.youtube_album_service.resolve_youtube_album`` and share the
    ``OUTCOME_HTTP_STATUS`` / ``OUTCOME_EXIT_CODE`` vocabulary exported
    by the service.

    Status mapping (from ``OUTCOME_HTTP_STATUS``):
      * 200 — ``ok``
      * 400 — missing / empty ``identifier`` query parameter
      * 404 — ``not_found``
      * 503 — ``unresolved_4xx_client`` / ``unresolved_mirror_unavailable``
              / ``unresolved_timeout`` / ``youtube_parse_failed`` /
              ``transient``
      * 500 — any unknown outcome (safety net)

    AE5 cache fallback: when the service returns ``ok`` with
    ``from_cache=True`` (and a non-empty ``error_message`` describing
    the upstream YT failure), the route still returns 200 — the matrix
    is real, the cache served, the caller got a useful result.
    """
    identifier_raw = params.get("identifier", [""])[0]
    identifier = (identifier_raw or "").strip()
    if not identifier:
        h._error("identifier query parameter is required", 400)
        return

    refresh = _parse_bool(params.get("refresh", [None])[0])

    yt, session = _build_youtube_client()
    cache = _RedisYoutubeCache()

    # Lazy-import compute_beets_distance to mirror the CLI's lazy
    # composition (the heavy beets machinery only loads if the route is
    # actually exercised).
    from lib.beets_distance import compute_beets_distance

    s = _server()
    try:
        result = resolve_youtube_album(
            identifier,
            pdb=s._db(),
            mb_get_release=lambda m: mb_api.get_release(m, fresh=False),
            mb_get_release_group_releases=mb_api.get_release_group_releases,
            discogs_get_release=lambda d: discogs_api.get_release(
                int(d), fresh=False),
            discogs_get_master_releases=lambda m: discogs_api.get_master_releases(
                int(m)),
            yt_client=yt,
            distance_fn=compute_beets_distance,
            cache=cache,
            refresh=refresh,
        )
    finally:
        # Close the requests.Session to release its connection pool.
        # Without this, every YT route invocation leaks a pool (finding
        # #18). ``Session.close`` is idempotent and safe to call after
        # ``YTMusic`` is done with the session.
        try:
            session.close()
        except Exception:
            pass

    status = OUTCOME_HTTP_STATUS.get(result.outcome, 500)
    payload = msgspec.to_builtins(result)
    h._json(payload, status=status)


class YoutubeRescueRequest(BaseModel):
    """HTTP body for ``POST /api/pipeline/<id>/youtube-rescue``.

    The ``request_id`` is taken from the URL path, NOT the body — only
    the ``browse_id`` (the YouTube Music album browseId, the same value
    the resolver returns in its ``yt_browse_id`` column) is body-side.
    """

    browse_id: str


def post_pipeline_youtube_rescue(h, body: dict, req_id_str: str) -> None:
    """``POST /api/pipeline/<id>/youtube-rescue``.

    Submit a YouTube-Music rescue ingest for one album request.
    Counterpart of ``pipeline-cli youtube-rescue`` (U4). Both surfaces
    wrap ``YoutubeIngestService.submit`` — keep them in sync (see
    ``CLAUDE.md`` § "CLI ⇄ API surface symmetry"). The outcome → HTTP
    status mapping is imported directly from
    ``lib.youtube_ingest_service.OUTCOME_HTTP_STATUS`` (aliased as
    ``YOUTUBE_INGEST_HTTP_STATUS`` to disambiguate from the resolver's
    ``OUTCOME_HTTP_STATUS``) so the CLI, HTTP route, and service share
    one source of truth.

    Body: ``{"browse_id": "<MPREb_...>"}``.

    Status mapping (from ``YOUTUBE_INGEST_HTTP_STATUS``):
      * 200 — ``accepted``
      * 400 — body validation failure (missing ``browse_id`` etc.) or
        invalid URL ``request_id``
      * 404 — ``request_not_found``
      * 409 — ``wrong_state`` (request is not ``wanted`` / ``manual``),
              ``in_flight`` (an existing ``youtube_running`` row already
              owns this request — re-issue once it's terminal)
      * 422 — ``no_resolver_mapping`` (run the YouTube album resolver
              first), ``track_count_precheck_failed`` (resolver cache
              vs. MB mirror disagree — refresh first)
      * 503 — ``transient`` (DB / MB-mirror hiccup; retry)

    The response payload always carries the typed
    ``{"download_log_id", "outcome", "detail"}`` shape so frontend
    consumers can render every outcome uniformly. ``download_log_id``
    is populated on ``accepted`` (the new row's id) and on
    ``in_flight`` (the existing in-flight row's id, so callers can
    render "you already have a rescue running, check id=N").
    """
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return

    req = parse_body(h, body or {}, YoutubeRescueRequest)
    if req is None:
        return

    s = _server()
    svc = default_youtube_ingest_service_factory(s._db())
    result = svc.submit(request_id, req.browse_id)

    payload = msgspec.to_builtins(result)
    status = YOUTUBE_INGEST_HTTP_STATUS.get(result.outcome, 500)
    if result.outcome != "accepted":
        # Mirror the search-plan-advance convention: non-2xx responses
        # carry both the structured ``detail`` field and the legacy
        # top-level ``error`` field for older frontend toasts that
        # grep the ``error`` string.
        payload["error"] = result.detail or result.outcome
    h._json(payload, status=status)


# ── Route tables ─────────────────────────────────────────────────────

ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/youtube-album", get_youtube_album,
        "YouTube Music album resolver — given an MB or Discogs "
        "release-or-group identifier, returns the typed "
        "(yt_release × mb_release) distance matrix. "
        "?refresh=true bypasses BOTH the durable cache "
        "(youtube_album_mappings) and the in-process Redis HTTP "
        "accelerator, forcing a fresh YT Music fetch; the fresh "
        "response is then written back to both layers.",
        classified=True,
    ),
    pattern_route(
        "POST", r"^/api/pipeline/(\d+)/youtube-rescue$",
        post_pipeline_youtube_rescue,
        "Submit a YouTube-Music rescue ingest for one album request. "
        "Counterpart of ``pipeline-cli youtube-rescue``; both surfaces "
        "wrap ``YoutubeIngestService.submit``. Body: {\"browse_id\": "
        "\"<MPREb_...>\"}. Returns the new (or existing in-flight) "
        "``download_log_id`` plus a structured outcome.",
        classified=True,
    ),
]
