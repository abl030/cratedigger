"""pipeline-cli YouTube resolver + rescue-ingest commands (#495 carve).

``youtube-album`` — resolve an MB/Discogs identifier into the YT Music
distance matrix. ``youtube-rescue`` — submit a rescue ingest for one
request. Both wrap the U7/U4 service layer (CLI ⇄ API surface symmetry).
"""

import argparse
import sys

import msgspec

# CLI ⇄ API symmetry: import the service entrypoint + outcome → exit-code
# mapping directly so the test, the CLI, and the U8 route share one
# source of truth (PR #381 lesson). Do NOT redefine the mapping here.
from lib.youtube_album_service import (
    OUTCOME_EXIT_CODE,
    resolve_youtube_album,
)
# U4 / CLI ⇄ API symmetry: import the YT-rescue ingest service's outcome
# → exit-code mapping with an alias (the youtube_album_service one above
# is already bound). Keep this the single source of truth for the CLI; the
# U5 route imports OUTCOME_HTTP_STATUS from the same module for HTTP-side
# mapping.
from lib.youtube_ingest_service import (
    OUTCOME_EXIT_CODE as YOUTUBE_INGEST_EXIT_CODE,
    default_youtube_ingest_service_factory,
)


class _RedisYoutubeCache:
    """Adapt ``web/cache.py``'s Redis client to the ``BeetsDistanceCache``
    protocol.

    The service-side keys already carry the ``youtube:album:`` /
    ``youtube:search:`` namespace; this adapter does NOT prefix them
    again (review finding #17 — the old ``_NAMESPACE`` wrapper produced
    ``youtube:album:youtube:album:<browse_id>`` keys).

    Mirrors ``_RedisFingerprintCache`` in ``web/routes/beets_distance.py`` —
    bytes get/set with a long sentinel TTL (cache lives forever absent
    explicit refresh per Key Technical Decisions). Falls back to a
    no-op when Redis isn't available so the CLI works without the
    in-process accelerator.
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
    CLI's startup cost stays low and the rest of the script doesn't
    pay for unused HTTP machinery.

    Returns ``(yt_client, session)`` so the caller can close the
    session in a ``finally`` block — without that, every CLI
    invocation leaks the requests Session's connection pool. Round 2
    P2-2: the web-route side already paired finding #18's close in a
    ``finally``; this brings the CLI surface into parity per the
    CLI ⇄ API symmetry rule.

    The session binds a default ``(connect, read)`` timeout of
    ``(5, 30)`` so an unresponsive YT endpoint can't pin the CLI
    invocation forever (finding #4). ``requests`` exposes no
    Session-level timeout config; ``functools.partial`` on
    ``session.request`` is the established pattern.
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
    session.request = partial(  # type: ignore[method-assign]
        session.request, timeout=(5, 30))
    return YTMusic(requests_session=session, language="en"), session


def cmd_youtube_album(db, args):
    """``pipeline-cli youtube-album <identifier> [--refresh] [--json]``.

    Resolves any MB / Discogs release-or-group identifier into the
    YouTube Music distance matrix. Counterpart of ``GET
    /api/youtube-album`` (U8). Both surfaces wrap
    ``lib.youtube_album_service.resolve_youtube_album`` — keep them in
    sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry"). The
    outcome → exit-code mapping is imported directly from the service
    module (``OUTCOME_EXIT_CODE``) to keep a single source of truth.

    Exit codes (from ``lib.youtube_album_service.OUTCOME_EXIT_CODE``):
      * 0 — ``ok``
      * 2 — ``not_found``
      * 5 — ``unresolved_4xx_client`` / ``unresolved_mirror_unavailable``
            / ``unresolved_timeout`` / ``youtube_parse_failed`` /
            ``transient``
      * 1 — unknown outcome (safety net)
    """
    from lib.beets_distance import compute_beets_distance
    from web import mb as mb_api
    from web import discogs as discogs_api

    yt, session = _build_youtube_client()
    cache = _RedisYoutubeCache()

    try:
        result = resolve_youtube_album(
            args.identifier,
            pdb=db,
            mb_get_release=lambda m: mb_api.get_release(m, fresh=False),
            mb_get_release_group_releases=mb_api.get_release_group_releases,
            discogs_get_release=lambda d: discogs_api.get_release(
                int(d), fresh=False),
            discogs_get_master_releases=lambda m: discogs_api.get_master_releases(
                int(m)),
            yt_client=yt,
            distance_fn=compute_beets_distance,
            cache=cache,
            refresh=bool(getattr(args, "refresh", False)),
        )
    finally:
        # Close the requests Session even when the resolver raises so
        # the connection pool doesn't leak. Mirrors the web-route side
        # (finding #18). Round 2 P2-2 — closes the CLI ⇄ API symmetry
        # gap.
        try:
            session.close()
        except Exception:
            pass

    if getattr(args, "json", False):
        print(msgspec.json.encode(result).decode())
    else:
        print(f"  identifier:             {args.identifier}")
        print(f"  outcome:                {result.outcome}")
        if result.release_group_identifier:
            print(f"  release group:          "
                  f"{result.release_group_identifier} ({result.source})")
        print(f"  from cache:             {result.from_cache}")
        if result.error_message:
            print(f"  error:                  {result.error_message}")
        if result.duration_ms is not None:
            print(f"  latency:                {result.duration_ms} ms")
        if result.youtube_releases:
            print(f"  matrix ({len(result.youtube_releases)} YT release(s)):")
            for yt_rel in result.youtube_releases:
                year = yt_rel.year if yt_rel.year is not None else "—"
                print(f"    - {yt_rel.yt_browse_id}  "
                      f"year={year}  tracks={yt_rel.track_count}")
                print(f"      url: {yt_rel.yt_url}")
                for d in yt_rel.distances:
                    if d.distance is not None:
                        dist_label = f"{d.distance:.4f}"
                    else:
                        dist_label = "n/a"
                    suffix = ""
                    if d.matched_tracks is not None \
                            and d.total_mb_tracks is not None:
                        suffix = (
                            f"  matched={d.matched_tracks}/"
                            f"{d.total_mb_tracks}")
                    err_suffix = (
                        f"  err={d.error_message}" if d.error_message else "")
                    print(f"      · {d.mbid}  outcome={d.outcome}  "
                          f"dist={dist_label}{suffix}{err_suffix}")
        else:
            # AE2 / R11 — empty matrix is a normal response, not an error.
            print("  matrix:                 (empty)")

    return OUTCOME_EXIT_CODE.get(result.outcome, 1)


def cmd_youtube_rescue(db, args, *, service_factory=None):
    """``pipeline-cli youtube-rescue <request_id> <browse_id> [--json]``.

    Submit a YouTube-Music rescue ingest for one album request. Counterpart
    of ``POST /api/pipeline/<id>/youtube-rescue`` (U5). Both surfaces wrap
    ``YoutubeIngestService.submit`` — keep them in sync (see ``CLAUDE.md``
    § "CLI ⇄ API surface symmetry"). The outcome → exit-code mapping is
    imported directly from the service module
    (``YOUTUBE_INGEST_EXIT_CODE``) to keep a single source of truth.

    Exit codes (from ``lib.youtube_ingest_service.OUTCOME_EXIT_CODE``):
      * 0 — ``accepted``
      * 2 — ``request_not_found``
      * 3 — ``no_resolver_mapping``, ``track_count_precheck_failed``
            (semantic input violations)
      * 4 — ``wrong_state`` (request is not ``wanted`` / ``unsearchable``),
            ``in_flight`` (an existing ``youtube_running`` row already
            owns this request — re-issue once it's terminal)
      * 5 — ``transient`` (DB / MB-mirror hiccup; retry)
      * 1 — unknown outcome (safety net)
    """
    factory = service_factory or default_youtube_ingest_service_factory
    svc = factory(db)
    result = svc.submit(int(args.request_id), str(args.browse_id))

    if getattr(args, "json", False):
        print(msgspec.json.encode(result).decode())
    else:
        if result.outcome == "accepted":
            print(
                f"accepted: download_log_id={result.download_log_id}")
        else:
            # Failure paths print classified outcome + detail to stderr
            # so success-only consumers can pipe stdout without noise.
            sys.stderr.write(
                f"{result.outcome}"
                f"{f': {result.detail}' if result.detail else ''}\n"
            )
            if result.download_log_id is not None:
                # ``in_flight`` carries the existing log id; surface so
                # the operator knows where to look.
                sys.stderr.write(
                    f"  existing download_log_id={result.download_log_id}\n"
                )

    return YOUTUBE_INGEST_EXIT_CODE.get(result.outcome, 1)


def add_youtube_subparsers(sub: argparse._SubParsersAction) -> None:
    """Add ``youtube-album`` / ``youtube-rescue`` (#521 carve out of
    ``routes_meta._build_parser``, verbatim argument definitions)."""
    # youtube-album (U7): MBID/Discogs ID → YT Music album matrix.
    # Counterpart of ``GET /api/youtube-album`` (U8).
    p_ya = sub.add_parser(
        "youtube-album",
        help="Resolve MBID/Discogs ID → YouTube Music album matrix "
             "(auto-widens to release group; N×M beets distances per "
             "YT sibling × MB sibling)",
    )
    p_ya.add_argument(
        "identifier",
        help="MB release/release-group MBID OR Discogs release/master ID "
             "(service auto-discriminates via leaf-then-group fallback)",
    )
    p_ya.add_argument(
        "--refresh", action="store_true",
        help="Bypass BOTH the durable cache (youtube_album_mappings) "
             "AND the in-process Redis HTTP accelerator, forcing a "
             "fresh YouTube Music fetch. The fresh response is then "
             "written back to both layers. (Default: serve from cache.)",
    )
    p_ya.add_argument(
        "--json", action="store_true",
        help="Print structured JSON instead of human-readable matrix",
    )

    # youtube-rescue (U4): submit a YouTube Music rescue ingest for one
    # request. Counterpart of ``POST /api/pipeline/<id>/youtube-rescue``
    # (U5). Both surfaces wrap ``YoutubeIngestService.submit``.
    p_yr = sub.add_parser(
        "youtube-rescue",
        help="Submit a YouTube Music rescue ingest for one request "
             "(requires a resolver mapping; emits a youtube_running "
             "download_log row).",
    )
    p_yr.add_argument(
        "request_id", type=int,
        help="album_requests.id to attach the rescue to",
    )
    p_yr.add_argument(
        "browse_id",
        help="YouTube Music browse_id (e.g. MPREb_...); must already "
             "be cached in youtube_album_mappings for this request's "
             "release group",
    )
    p_yr.add_argument(
        "--json", action="store_true",
        help="Print structured JSON ({outcome, download_log_id, detail}) "
             "instead of plain text.",
    )
