"""Real beets match distance route.

Split from web/routes/pipeline.py (#522).
"""

import msgspec

from lib.release_identity import detect_release_source
from web import discogs as discogs_api
from web import mb as mb_api
from web.routes._registry import RouteRegistration, pattern_route
from web.routes._server_access import _server


class _RedisFingerprintCache:
    """Adapt ``web/cache.py``'s Redis client to the ``BeetsDistanceCache`` protocol.

    Our fingerprints are msgspec-encoded bytes, while ``web/cache.py``
    targets JSON-serialisable dicts/lists — so we bypass the JSON
    wrapping and talk to the Redis client directly. Falls back to a
    no-op cache when Redis is unavailable so single-call dev shells
    still work (just without the cached fast-path).
    """

    def __init__(self) -> None:
        from web import cache as _cache_mod
        self._redis = getattr(_cache_mod, "_redis", None)

    def get(self, key: str):
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(key)
        except Exception:
            return None
        if raw is None:
            return None
        # web/cache.py initialises Redis with ``decode_responses=True``,
        # so ``get`` returns str. msgspec.json.decode handles bytes;
        # encoding is cheap.
        if isinstance(raw, str):
            return raw.encode("utf-8")
        return raw

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        if self._redis is None:
            return
        try:
            self._redis.setex(key, ttl_seconds, value)
        except Exception:
            pass


_BEETS_DISTANCE_OUTCOME_STATUS: dict[str, int] = {
    "ok": 200,
    "download_log_not_found": 404,
    "request_not_found": 404,
    "folder_missing": 410,
    "no_audio": 410,
    "mb_lookup_failed": 503,
    "mb_no_release_group": 422,
    "wrong_release_group": 422,
    "distance_failed": 500,
}


def get_beets_distance(
    h, params: dict[str, list[str]],
    download_log_id_str: str, mbid: str,
) -> None:
    """``GET /api/beets-distance/<download_log_id>/<mbid>``.

    Real beets match distance for one ``(download_log_id, mbid)``
    pair. The service does the heavy lifting (see
    ``lib/beets_distance.compute_beets_distance``); this handler is a
    thin adapter that maps the typed ``BeetsDistanceResult`` outcomes
    to HTTP status codes per the CLI ⇄ API symmetry rule.

    ``mbid`` may be an MB release UUID or a bare Discogs numeric release
    id (#530 — Discogs siblings, e.g. surfaced by the Replace picker
    against a Discogs-sourced request per #501). Dispatch on the id
    shape the same way ``browse.py::get_release_group`` does: numeric ⇒
    Discogs. ``compute_beets_distance`` is source-agnostic (the
    YouTube resolver already scores Discogs releases through it via the
    same ``get_release``-shaped callable) — no MB<->Discogs adapter
    needed.

    Status-code mapping:
      * 200 — ``ok`` (distance is in ``response.distance``)
      * 404 — ``download_log_not_found`` / ``request_not_found``
      * 410 — ``folder_missing`` / ``no_audio`` (the data the caller
              wanted to compare against is gone)
      * 422 — ``mb_no_release_group`` / ``wrong_release_group``
              (semantic input violations — including the
              cross-release-group guardrail)
      * 503 — ``mb_lookup_failed`` (MB mirror transient)
      * 500 — ``distance_failed`` (unexpected beets error)
    """
    from lib.beets_distance import compute_beets_distance

    try:
        download_log_id = int(download_log_id_str)
    except (TypeError, ValueError):
        h._error("Invalid download_log_id")
        return

    if detect_release_source(mbid) == "discogs":
        get_release_fn = lambda m: discogs_api.get_release(int(m), fresh=False)
    else:
        get_release_fn = lambda m: mb_api.get_release(m, fresh=False)

    s = _server()
    result = compute_beets_distance(
        download_log_id,
        mbid,
        pdb=s._db(),
        mb_get_release=get_release_fn,
        cache=_RedisFingerprintCache(),
    )

    status = _BEETS_DISTANCE_OUTCOME_STATUS.get(result.outcome, 500)
    payload = msgspec.to_builtins(result)
    h._json(payload, status=status)


ROUTES: list[RouteRegistration] = [
    # /api/beets-distance/<download_log_id>/<mbid> — real beets distance
    # for one (download_log_id, mbid) pair. mbid may be an MB UUID or a
    # bare Discogs numeric id (#530). See get_beets_distance above.
    pattern_route(
        "GET", r"^/api/beets-distance/(\d+)/([a-f0-9-]{36}|\d+)$",
        get_beets_distance,
        "Real beets match distance for one (download_log_id, mbid) pair "
        "(mbid may be an MB UUID or a Discogs numeric id); refuses "
        "cross-release-group comparisons.",
        classified=True,
    ),
]
