"""YouTube Music album resolver — HTTP route module (U8).

Single POST endpoint, ``/api/youtube-album``, with ``identifier`` and
``refresh`` in its JSON body. It wraps
``lib.youtube_album_service.resolve_youtube_album``. The CLI
counterpart (U7) lives at ``scripts/pipeline_cli/youtube.py::cmd_youtube_album``;
both surfaces share the same service + outcome vocabulary per
``CLAUDE.md`` § "CLI ⇄ API surface symmetry".

The outcome → HTTP status mapping is imported directly from the service
module (``OUTCOME_HTTP_STATUS``) — one source of truth, per the PR #381
lesson. Do not redefine it locally.

The Redis cache adapter is the same shape as
``_RedisFingerprintCache`` in ``web/routes/beets_distance.py`` — bytes
``get`` / ``set`` with a long-sentinel TTL since the YT response cache
lives forever absent explicit ``refresh=true``.
"""

from __future__ import annotations

import logging

import msgspec
from pydantic import BaseModel, Field

from lib.youtube_album_service import (
    OUTCOME_HTTP_STATUS,
    resolve_youtube_album,
)
from lib.youtube_ingest_service import (
    OUTCOME_HTTP_STATUS as YOUTUBE_INGEST_HTTP_STATUS,
)
from lib.youtube_ingest_service import (
    default_youtube_ingest_service_factory,
)
from lib.youtube_transport import build_youtube_client as _build_youtube_client
from web import discogs as discogs_api
from web import mb as mb_api
from web.routes._pydantic import parse_body
from web.routes._registry import (
    RouteHandler,
    RouteRegistration,
    pattern_route,
    route,
)
from web.routes._server_access import _server

log = logging.getLogger(__name__)


# Re-export from the service module so callers (tests, downstream
# wrappers) can `from web.routes.youtube import OUTCOME_HTTP_STATUS`
# without reaching into ``lib.youtube_album_service``. The ``is``
# identity is asserted in the contract test — there is no second source
# of truth.
__all__ = [
    "OUTCOME_HTTP_STATUS",
    "ROUTES",
    "YOUTUBE_INGEST_HTTP_STATUS",
    "YoutubeAlbumRequest",
    "YoutubeRescueRequest",
    "post_pipeline_youtube_rescue",
    "post_youtube_album",
]


class _RedisYoutubeCache:
    """Adapt ``web/cache.py``'s Redis client to the
    ``BeetsDistanceCache`` protocol.

    The service-side keys already carry the ``youtube:album:`` /
    ``youtube:search:`` namespace; this adapter does NOT prefix them
    again (review finding #17 — the old ``_NAMESPACE`` wrapper produced
    ``youtube:album:youtube:album:<browse_id>`` keys).

    Mirrors ``_RedisFingerprintCache`` in ``web/routes/beets_distance.py``
    (and ``scripts/pipeline_cli/youtube.py::_RedisYoutubeCache`` on the CLI
    side) — bytes get/set with a long sentinel TTL. Falls back to a
    no-op when Redis is unavailable so single-shot dev shells still
    work without the in-process accelerator.
    """

    def __init__(self) -> None:
        try:
            from web import cache as _cache_mod
            self._redis = getattr(_cache_mod, "_redis", None)
        except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            self._redis = None

    def get(self, key: str):
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(key)
        except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
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
            self._redis.setex(
                key, ttl_seconds, value)
        except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
            pass


class YoutubeAlbumRequest(BaseModel):
    """Cache-writing resolver request body."""

    identifier: str
    refresh: bool = Field(default=False, strict=True)


def post_youtube_album(
    h: RouteHandler,
    body: dict[str, object],
) -> None:
    """``POST /api/youtube-album``.

    Resolves any MB / Discogs release-or-group identifier into the
    YouTube Music distance matrix. Counterpart of ``pipeline-cli
    youtube-album`` (U7). Both surfaces wrap
    ``lib.youtube_album_service.resolve_youtube_album`` and share the
    ``OUTCOME_HTTP_STATUS`` / ``OUTCOME_EXIT_CODE`` vocabulary exported
    by the service.

    Status mapping (from ``OUTCOME_HTTP_STATUS``):
      * 200 — ``ok``
      * 400 — invalid JSON body or missing / empty ``identifier``
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
    req = parse_body(h, body, YoutubeAlbumRequest)
    if req is None:
        return
    identifier = req.identifier.strip()
    if not identifier:
        h._error("identifier is required", 400)
        return

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
            refresh=req.refresh,
        )
    finally:
        # Close the requests.Session to release its connection pool.
        # Without this, every YT route invocation leaks a pool (finding
        # #18). ``Session.close`` is idempotent and safe to call after
        # ``YTMusic`` is done with the session.
        try:
            session.close()
        except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
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


def post_pipeline_youtube_rescue(
    h: RouteHandler, body: dict[str, object], req_id_str: str,
) -> None:
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
      * 409 — ``wrong_state`` (request is not ``wanted`` / ``unsearchable``),
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
        "POST", "/api/youtube-album", post_youtube_album,
        "YouTube Music album resolver — given an MB or Discogs "
        "release-or-group identifier, returns the typed "
        "(yt_release × mb_release) distance matrix. Body: "
        "{\"identifier\": \"<release-or-group-id>\", \"refresh\": "
        "false}. refresh=true bypasses BOTH the durable cache "
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
