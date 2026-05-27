"""YouTube Music album resolver — HTTP route module (U8).

Single GET endpoint, ``/api/youtube-album?identifier=<id>&refresh=<bool>``,
that wraps ``lib.youtube_album_service.resolve_youtube_album``. The CLI
counterpart (U7) lives at ``scripts/pipeline_cli.py::cmd_youtube_album``;
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
import re

import msgspec

from lib.youtube_album_service import (
    OUTCOME_HTTP_STATUS,
    resolve_youtube_album,
)
from web import discogs as discogs_api
from web import mb as mb_api


log = logging.getLogger(__name__)


# Re-export from the service module so callers (tests, downstream
# wrappers) can `from web.routes.youtube import OUTCOME_HTTP_STATUS`
# without reaching into ``lib.youtube_album_service``. The ``is``
# identity is asserted in the contract test — there is no second source
# of truth.
__all__ = [
    "GET_ROUTES",
    "POST_ROUTES",
    "GET_PATTERNS",
    "POST_PATTERNS",
    "GET_DESCRIPTIONS",
    "POST_DESCRIPTIONS",
    "PATTERN_DESCRIPTIONS",
    "OUTCOME_HTTP_STATUS",
    "get_youtube_album",
]


# Effectively forever — Redis SETEX accepts up to ``2**63 - 1`` seconds
# but ``2**31 - 1`` (~68 years) is the conservative limit honoured by
# all Redis clients and is what ``_RedisFingerprintCache`` callers use
# elsewhere. The cache is content-addressed per release group, so a
# stale entry can only be cleared via ``?refresh=true``.
_FOREVER_TTL_SECONDS = 2**31 - 1


class _RedisYoutubeCache:
    """Adapt ``web/cache.py``'s Redis client to the
    ``BeetsDistanceCache`` protocol, keyed under ``youtube:album:<key>``.

    Mirrors ``_RedisFingerprintCache`` in ``web/routes/pipeline.py``
    (and ``scripts/pipeline_cli.py::_RedisYoutubeCache`` on the CLI
    side) — bytes get/set with a long sentinel TTL. Falls back to a
    no-op when Redis is unavailable so single-shot dev shells still
    work without the in-process accelerator.
    """

    _NAMESPACE = "youtube:album:"

    def __init__(self) -> None:
        try:
            from web import cache as _cache_mod
            self._redis = getattr(_cache_mod, "_redis", None)
        except Exception:
            self._redis = None

    def _ns(self, key: str) -> str:
        return f"{self._NAMESPACE}{key}"

    def get(self, key: str):
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(self._ns(key))  # type: ignore[union-attr]
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
                self._ns(key), ttl_seconds, value)
        except Exception:
            pass


def _build_youtube_client():
    """Construct a ``YTMusic`` client with retry + jittered desktop
    headers per the Key Technical Decisions (R5 / external research).

    Lazy-imports ``requests``, ``urllib3``, and ``ytmusicapi`` so the
    web server's startup cost stays low and unrelated routes don't
    pay for unused HTTP machinery. Mirrors
    ``scripts/pipeline_cli.py::_build_youtube_client``.
    """
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
    return YTMusic(requests_session=session, language="en")


def _server():
    """Deferred import to avoid circular deps (mirrors the convention
    used in every other ``web/routes/*.py`` module)."""
    from web import server
    return server


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
      * 422 — ``mb_no_release_group``
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

    yt = _build_youtube_client()
    cache = _RedisYoutubeCache()

    # Lazy-import compute_beets_distance to mirror the CLI's lazy
    # composition (the heavy beets machinery only loads if the route is
    # actually exercised).
    from lib.beets_distance import compute_beets_distance

    s = _server()
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

    status = OUTCOME_HTTP_STATUS.get(result.outcome, 500)
    payload = msgspec.to_builtins(result)
    h._json(payload, status=status)


# ── Route tables ─────────────────────────────────────────────────────

GET_ROUTES: dict[str, object] = {
    "/api/youtube-album": get_youtube_album,
}

POST_ROUTES: dict[str, object] = {}

GET_PATTERNS: list[tuple[re.Pattern[str], object]] = []

POST_PATTERNS: list[tuple[re.Pattern[str], object]] = []

GET_DESCRIPTIONS: dict[str, str] = {
    "/api/youtube-album": (
        "YouTube Music album resolver — given an MB or Discogs "
        "release-or-group identifier, returns the typed "
        "(yt_release × mb_release) distance matrix with optional "
        "?refresh=true to bypass the in-process Redis cache."
    ),
}

POST_DESCRIPTIONS: dict[str, str] = {}

PATTERN_DESCRIPTIONS: list[tuple[re.Pattern[str], str]] = []
