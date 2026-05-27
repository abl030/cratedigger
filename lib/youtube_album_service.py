"""YouTube Music album resolver service.

Given any MB or Discogs release identifier (release-level OR
release-group/master-level), the resolver:

1. Auto-widens the input to a release-group / master via leaf-then-group
   fallback against the local MB / Discogs mirrors.
2. Enumerates the sibling releases in that group.
3. Searches YouTube Music for the album, picks a seed YT result by
   ``(year, trackCount)`` proximity to the lowest-year MB sibling, and
   expands via the seed's ``other_versions[]`` array.
4. Synthesizes ``SyntheticItem`` lists per YT sibling and runs
   ``compute_beets_distance(items_override=…)`` for every
   ``(yt_sibling × mb_sibling)`` pair — N×M scoring.
5. Persists the matrix to ``youtube_album_mappings`` for content-
   addressed reuse, and returns the typed
   ``YoutubeAlbumResolverResult``.

The service is pure of HTTP / CLI concerns — wrappers in
``web/routes/youtube.py`` (U8) and ``scripts/pipeline_cli.py`` (U7) map
``result.outcome`` to status / exit codes via the
``OUTCOME_HTTP_STATUS`` / ``OUTCOME_EXIT_CODE`` dicts exported below
(one source of truth, per the PR #381 lesson).

Every collaborator is injected so the service is testable without
network IO. Tests pass ``FakeYTMusic`` + ``FakePipelineDB`` + small
lambdas for the MB / Discogs lookups; the integration slice passes the
real ``compute_beets_distance``.
"""

from __future__ import annotations

import logging
import random
import time
import urllib.error
from typing import Any, Callable, Optional

import msgspec
import requests

from ytmusicapi.exceptions import YTMusicError, YTMusicServerError, YTMusicUserError

from lib.beets_distance import (
    BeetsDistanceCache,
    BeetsDistanceResult,
    SyntheticItem,
    compute_beets_distance as _default_distance_fn,
)
from lib.release_identity import detect_release_source


# Exception classes that the MB / Discogs adapters in ``web/mb.py`` and
# ``web/discogs.py`` raise on miss / mirror outage. ``HTTPError`` and
# ``URLError`` come from ``urllib`` (MB adapter); ``requests.HTTPError``
# and ``requests.RequestException`` come from any future Discogs adapter
# changeover. ``ValueError`` covers ``int()`` coercion failures on Discogs
# IDs (e.g. when the operator pastes a UUID into the Discogs path). All
# get treated as "leaf miss" — the auto-widen falls through to the group
# path.
_AUTO_WIDEN_MISS_EXCS: tuple[type[BaseException], ...] = (
    urllib.error.HTTPError,
    urllib.error.URLError,
    requests.HTTPError,
    requests.RequestException,
    ValueError,
)

log = logging.getLogger(__name__)


# Redis cache TTL for cached YouTube Music HTTP responses. Effectively
# forever: Redis ``SETEX`` accepts up to ``2**63 - 1``, but ``2**31 - 1``
# (~68 years) is the conservative limit honoured by all Redis clients
# and matches the pattern ``_RedisFingerprintCache`` callers use
# elsewhere. The durable cache is ``pdb.youtube_album_mappings``; this
# constant only governs the in-process HTTP-accelerator layer.
_FOREVER_TTL_SECONDS = 2**31 - 1


def _cached_search(
    yt_client: Any,
    cache: Optional[BeetsDistanceCache],
    query: str,
    filter_str: str,
    limit: int,
    *,
    refresh: bool = False,
) -> list[dict[str, Any]]:
    """Wrap ``yt_client.search(...)`` with cache-check-then-store.

    Cache entries are msgspec-encoded JSON. Decode failures (e.g. a
    corrupt entry, a schema change) fall through silently to a fresh
    HTTP call. Cache writes are best-effort — if Redis is down, the
    error is swallowed and the result still surfaces to the caller.

    When ``refresh=True``, the cache read is skipped but the cache is
    still updated with the fresh response — refresh is "bust then
    refill", not "bust and forget".
    """
    if cache is not None and not refresh:
        key = f"youtube:search:{query}:{filter_str}:{limit}"
        blob = cache.get(key)
        if blob is not None:
            try:
                return msgspec.json.decode(blob)
            except msgspec.DecodeError:
                pass
    results = yt_client.search(query, filter=filter_str, limit=limit)
    if cache is not None:
        try:
            cache.set(
                f"youtube:search:{query}:{filter_str}:{limit}",
                msgspec.json.encode(results),
                _FOREVER_TTL_SECONDS,
            )
        except Exception:  # noqa: BLE001 — cache writes are best-effort
            pass
    return results


def _cached_get_album(
    yt_client: Any,
    cache: Optional[BeetsDistanceCache],
    browse_id: str,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    """Wrap ``yt_client.get_album(browse_id)`` with cache-check-then-store.

    Same semantics as ``_cached_search``: cache-first read, msgspec
    JSON encoding, swallow decode/write failures so cache problems
    never block resolution. ``refresh=True`` skips the cache read but
    still updates it with the fresh response.
    """
    if cache is not None and not refresh:
        key = f"youtube:album:{browse_id}"
        blob = cache.get(key)
        if blob is not None:
            try:
                return msgspec.json.decode(blob)
            except msgspec.DecodeError:
                pass
    album = yt_client.get_album(browse_id)
    if cache is not None:
        try:
            cache.set(
                f"youtube:album:{browse_id}",
                msgspec.json.encode(album),
                _FOREVER_TTL_SECONDS,
            )
        except Exception:  # noqa: BLE001
            pass
    return album


# Jitter range between consecutive ``get_album`` calls. Mirrors the
# Key Technical Decisions doc (1-3s) and the YT scraping guidance —
# small randomized pauses reduce the chance of being throttled when
# expanding a release group with N siblings. Service injects the
# sleep function so tests can pass a no-op.
_JITTER_MIN_SECONDS = 1.0
_JITTER_MAX_SECONDS = 3.0


def _default_jitter_sleep_fn(seconds: float) -> None:
    """Default sleep used between YT calls. Tests pass ``lambda _: None``."""
    time.sleep(seconds)


def _jitter(sleep_fn: Callable[[float], None]) -> None:
    """Sleep a uniform-random duration in ``[1.0, 3.0]`` seconds."""
    sleep_fn(random.uniform(_JITTER_MIN_SECONDS, _JITTER_MAX_SECONDS))


# ---------------------------------------------------------------------------
# Outcome vocabulary — shared with CLI (U7) and web route (U8).
# ---------------------------------------------------------------------------


SERVICE_OUTCOMES: frozenset[str] = frozenset({
    "ok",
    "not_found",
    "mb_no_release_group",
    "unresolved_4xx_client",
    "unresolved_mirror_unavailable",
    "unresolved_timeout",
    "youtube_parse_failed",
    "transient",
})
"""Service-level outcomes. Per-pair outcomes (``ok``,
``wrong_release_group``, ``mb_lookup_failed``, ``mb_no_release_group``,
``no_audio``, ``empty_items_override``, ``invalid_input``,
``distance_failed``) flow through from ``compute_beets_distance``
verbatim inside ``ResolvedDistance.outcome``."""


OUTCOME_HTTP_STATUS: dict[str, int] = {
    "ok": 200,
    "not_found": 404,
    "mb_no_release_group": 422,
    "unresolved_4xx_client": 503,
    "unresolved_mirror_unavailable": 503,
    "unresolved_timeout": 503,
    "youtube_parse_failed": 503,
    "transient": 503,
}
"""Service outcome → HTTP status. U8 imports this directly."""


OUTCOME_EXIT_CODE: dict[str, int] = {
    "ok": 0,
    "not_found": 2,
    "mb_no_release_group": 3,
    "unresolved_4xx_client": 5,
    "unresolved_mirror_unavailable": 5,
    "unresolved_timeout": 5,
    "youtube_parse_failed": 5,
    "transient": 5,
}
"""Service outcome → CLI exit code. U7 imports this directly."""


# ---------------------------------------------------------------------------
# Result structs (wire boundary — crosses HTTP + CLI JSON).
# ---------------------------------------------------------------------------


class ResolvedDistance(msgspec.Struct, kw_only=True):
    """One distance entry inside a YT release's ``distances[]`` array.

    Flattens ``BeetsDistanceResult`` to the subset the resolver matrix
    surfaces — per-pair outcomes preserve partial failures verbatim
    (R17). Optional numeric fields stay ``None`` when ``outcome != "ok"``.
    """

    mbid: str
    outcome: str
    distance: Optional[float] = None
    components: Optional[dict[str, float]] = None
    matched_tracks: Optional[int] = None
    total_local_tracks: Optional[int] = None
    total_mb_tracks: Optional[int] = None
    extra_local_tracks: Optional[int] = None
    extra_mb_tracks: Optional[int] = None
    error_message: Optional[str] = None


class ResolvedYoutubeRelease(msgspec.Struct, kw_only=True):
    """One YT Music album sibling in the resolved matrix."""

    yt_browse_id: str
    yt_audio_playlist_id: Optional[str] = None
    yt_url: str
    year: Optional[int] = None
    track_count: int
    tracks: list[SyntheticItem]
    distances: list[ResolvedDistance]


class YoutubeAlbumResolverResult(msgspec.Struct, kw_only=True):
    """Top-level result. Crosses the wire (CLI JSON + HTTP response)."""

    outcome: str
    release_group_identifier: Optional[str] = None
    source: Optional[str] = None  # "mb" | "discogs" | None
    from_cache: bool = False
    youtube_releases: list[ResolvedYoutubeRelease] = msgspec.field(
        default_factory=list)
    error_message: Optional[str] = None
    duration_ms: Optional[int] = None


# Type aliases for clarity.
MBLookup = Callable[[str], Optional[dict]]
"""``mb_get_release(id) -> slim release dict | None`` (web/mb.py shape)."""

MBRGReleases = Callable[[str], Optional[dict]]
"""``mb_get_release_group_releases(rg) -> {title, type, releases[]}``."""

DiscogsLookup = Callable[[str], Optional[dict]]
"""``discogs_get_release(id) -> slim release dict | None``."""

DiscogsMasterReleases = Callable[[str], Optional[dict]]
"""``discogs_get_master_releases(master_id) -> {title, type, releases[]}``."""

DistanceFn = Callable[..., BeetsDistanceResult]
"""``compute_beets_distance(...)`` shape — service injects this so tests
can supply canned results."""


# ---------------------------------------------------------------------------
# Public service entrypoint.
# ---------------------------------------------------------------------------


def resolve_youtube_album(
    identifier: str,
    *,
    pdb: Any,
    mb_get_release: MBLookup,
    mb_get_release_group_releases: MBRGReleases,
    discogs_get_release: DiscogsLookup,
    discogs_get_master_releases: DiscogsMasterReleases,
    yt_client: Any,
    distance_fn: DistanceFn = _default_distance_fn,
    cache: Optional[BeetsDistanceCache] = None,
    refresh: bool = False,
    sleep_fn: Callable[[float], None] = _default_jitter_sleep_fn,
) -> YoutubeAlbumResolverResult:
    """Resolve a release identifier to the YT Music distance matrix.

    See module docstring for the full flow. ``cache`` is for the
    upstream HTTP responses (Redis adapter, ``None`` = no caching). The
    durable cache is ``pdb.youtube_album_mappings``.

    ``sleep_fn`` is the jitter hook injected between consecutive YT
    ``get_album`` calls. Production defaults to ``time.sleep``; tests
    pass ``lambda _: None`` so they don't pay the 1-3s pause per
    sibling.
    """
    started = time.monotonic()

    source_label = _classify_source(identifier)
    if source_label is None:
        return _final(
            outcome="not_found",
            error_message=f"identifier {identifier!r} is neither an MB UUID "
                          f"nor a Discogs ID",
            started=started,
        )

    # Step 1+2: auto-widen via leaf-then-group fallback.
    if source_label == "mb":
        rg_id, sibling_summaries = _resolve_mb_group(
            identifier, mb_get_release, mb_get_release_group_releases)
    else:
        rg_id, sibling_summaries = _resolve_discogs_group(
            identifier, discogs_get_release, discogs_get_master_releases)

    if rg_id is None:
        # `sibling_summaries` here actually carries the failure outcome
        # the helper produced ("not_found" or "mb_no_release_group").
        failure = (sibling_summaries or ["not_found"])[0]
        return _final(
            outcome=failure if isinstance(failure, str) else "not_found",
            error_message=f"could not resolve identifier {identifier!r} to a "
                          f"release group",
            started=started,
        )

    # Step 3: cache-first read.
    #
    # ``get_youtube_album_mapping`` returns ``None`` when the (rg, source)
    # pair has NEVER been resolved, and ``[]`` when it has been resolved
    # but YT had no albums to surface (AE2). The distinction matters:
    # ``None`` means "we have nothing — go ask YT"; ``[]`` means "we
    # checked and there's nothing there — don't re-poll YT". Without
    # this, an empty-search release group would re-fetch YT on every
    # resolve, defeating R14.
    cached_rows = pdb.get_youtube_album_mapping(rg_id, source_label)
    if not refresh and cached_rows is not None:
        return _final(
            outcome="ok",
            release_group_identifier=rg_id,
            source=source_label,
            from_cache=True,
            youtube_releases=_rows_to_youtube_releases(cached_rows),
            started=started,
        )

    # Step 4: collect the full set of sibling MBIDs (every entry in the
    # group), plus a fetched-record subset for seed-picking. Per R17,
    # misses against the local mirror don't shrink the matrix — they
    # appear as ``mb_lookup_failed`` per-pair outcomes when the distance
    # function re-fetches them.
    sibling_ids = _extract_sibling_ids(sibling_summaries)
    if not sibling_ids:
        return _final(
            outcome="not_found",
            release_group_identifier=rg_id,
            source=source_label,
            error_message="release group resolved but carries no sibling IDs",
            started=started,
        )

    fetched_siblings = _fetch_mb_siblings(
        sibling_summaries, source_label,
        mb_get_release, discogs_get_release,
    )
    if not fetched_siblings:
        # We have IDs but couldn't fetch any record — can't pick a seed
        # query without an artist + title. Surface as not_found rather
        # than ok-with-empty.
        return _final(
            outcome="not_found",
            release_group_identifier=rg_id,
            source=source_label,
            error_message="release group resolved but no sibling records "
                          "could be fetched for seed selection",
            started=started,
        )

    seed_release = _pick_mb_seed(fetched_siblings)
    query = _build_search_query(seed_release)

    # Step 5-8: search YT, expand siblings, fetch per-YT-album track lists.
    yt_failure: Optional[tuple[str, str]] = None
    seed_browse_id: Optional[str] = None
    yt_album_responses: dict[str, dict] = {}
    try:
        search_results = _cached_search(
            yt_client, cache, query, "albums", 10, refresh=refresh)
        seed_browse_id = _pick_yt_seed(search_results, seed_release)
        if seed_browse_id is None:
            # AE2: search returned empty → ok + empty matrix.
            pdb.upsert_youtube_album_mapping(rg_id, source_label, [])
            return _final(
                outcome="ok",
                release_group_identifier=rg_id,
                source=source_label,
                from_cache=False,
                youtube_releases=[],
                started=started,
            )
        seed_album = _cached_get_album(
            yt_client, cache, seed_browse_id, refresh=refresh)
        yt_album_responses[seed_browse_id] = seed_album
        for other in seed_album.get("other_versions") or []:
            other_browse_id = other.get("browseId")
            if not other_browse_id or other_browse_id in yt_album_responses:
                continue
            # Jitter between consecutive ``get_album`` calls — 1-3s
            # randomized pause to avoid YT throttling on large release
            # groups. The first ``get_album`` (seed) doesn't sleep.
            _jitter(sleep_fn)
            # Per-sibling get_album failures don't abort the whole resolve;
            # exclude the broken sibling instead.
            try:
                yt_album_responses[other_browse_id] = _cached_get_album(
                    yt_client, cache, other_browse_id, refresh=refresh)
            except (YTMusicServerError, YTMusicUserError, YTMusicError,
                    requests.Timeout, requests.ConnectionError,
                    KeyError, IndexError) as exc:
                log.warning(
                    "youtube_album_service: sibling get_album(%s) failed: %s",
                    other_browse_id, exc,
                )
                continue
    except requests.Timeout as exc:
        yt_failure = ("unresolved_timeout", f"YT timeout: {exc}")
    except requests.ConnectionError as exc:
        yt_failure = ("unresolved_timeout", f"YT connection error: {exc}")
    except YTMusicUserError as exc:
        yt_failure = ("unresolved_4xx_client", f"YT user error: {exc}")
    except YTMusicServerError as exc:
        yt_failure = (_classify_server_error(exc), f"YT server error: {exc}")
    except YTMusicError as exc:
        yt_failure = ("unresolved_mirror_unavailable", f"YT error: {exc}")
    except (KeyError, IndexError, ValueError, TypeError) as exc:
        # ``ValueError`` and ``TypeError`` cover residual int/str coercion
        # failures from within the YT response — e.g. an unexpected
        # ``None`` slipping past ``_safe_int`` because we missed a code
        # path. Treating these as parse failures (not 500s) keeps the
        # resolver resilient to schema drift in ytmusicapi outputs.
        yt_failure = ("youtube_parse_failed", f"YT parse failed: {exc}")

    if yt_failure is not None:
        failure_outcome, failure_msg = yt_failure
        if cached_rows:
            # Cache fallback: outcome stays ok, but flag the upstream failure.
            return _final(
                outcome="ok",
                release_group_identifier=rg_id,
                source=source_label,
                from_cache=True,
                youtube_releases=_rows_to_youtube_releases(cached_rows),
                error_message=f"{failure_outcome}: serving from cache "
                              f"({failure_msg})",
                started=started,
            )
        return _final(
            outcome=failure_outcome,
            release_group_identifier=rg_id,
            source=source_label,
            youtube_releases=[],
            error_message=failure_msg,
            started=started,
        )

    # Step 9-10: synthesize items per YT sibling and score N×M.
    youtube_releases: list[ResolvedYoutubeRelease] = []
    persistable_rows: list[dict[str, Any]] = []

    for browse_id, album_resp in yt_album_responses.items():
        synth_items = _synthesize_items(album_resp)
        if not synth_items:
            # An empty track list defeats scoring; skip the sibling so the
            # matrix doesn't carry a row with no data to compare against.
            continue
        distances = _score_against_siblings(
            synth_items, sibling_ids, rg_id, distance_fn, pdb,
            mb_get_release if source_label == "mb" else discogs_get_release,
        )
        yt_url = _compose_url(browse_id, album_resp.get("audioPlaylistId"))
        year = _parse_year(album_resp.get("year"))
        yt_rel = ResolvedYoutubeRelease(
            yt_browse_id=browse_id,
            yt_audio_playlist_id=album_resp.get("audioPlaylistId"),
            yt_url=yt_url,
            year=year,
            track_count=_safe_int(album_resp.get("trackCount"), 0)
                or len(synth_items),
            tracks=synth_items,
            distances=distances,
        )
        youtube_releases.append(yt_rel)
        persistable_rows.append({
            "yt_browse_id": browse_id,
            "yt_audio_playlist_id": album_resp.get("audioPlaylistId"),
            "yt_url": yt_url,
            "yt_year": year,
            "yt_track_count": yt_rel.track_count,
            "yt_tracks": [
                {
                    "title": si.title,
                    "artists": [{"name": si.artist}],
                    "length_seconds": si.length,
                    "track_number": si.track,
                    "disc_number": si.disc,
                    "video_id": _video_id_for_track(album_resp, si.track),
                }
                for si in synth_items
            ],
            "distances": [
                {
                    "mbid": d.mbid,
                    "outcome": d.outcome,
                    "distance": d.distance,
                    "components": d.components,
                    "matched_tracks": d.matched_tracks,
                    "total_local_tracks": d.total_local_tracks,
                    "total_mb_tracks": d.total_mb_tracks,
                    "extra_local_tracks": d.extra_local_tracks,
                    "extra_mb_tracks": d.extra_mb_tracks,
                    "error_message": d.error_message,
                }
                for d in distances
            ],
        })

    # Step 11: persist + return.
    pdb.upsert_youtube_album_mapping(rg_id, source_label, persistable_rows)
    return _final(
        outcome="ok",
        release_group_identifier=rg_id,
        source=source_label,
        from_cache=False,
        youtube_releases=youtube_releases,
        started=started,
    )


# ---------------------------------------------------------------------------
# Auto-widen helpers (MB + Discogs leaf-then-group).
# ---------------------------------------------------------------------------


def _classify_source(identifier: str) -> Optional[str]:
    """Map ``detect_release_source`` outputs to the table-discriminator string.

    The migration stores ``'mb'`` / ``'discogs'`` (CHECK constraint),
    while ``detect_release_source`` returns ``'musicbrainz'`` /
    ``'discogs'`` / ``'unknown'``. We map here so the persisted source
    column stays aligned with the schema.
    """
    source = detect_release_source(identifier)
    if source == "musicbrainz":
        return "mb"
    if source == "discogs":
        return "discogs"
    return None


def _safe_leaf_lookup(
    lookup: Callable[[str], Optional[dict]],
    identifier: str,
) -> Optional[dict]:
    """Call a leaf lookup, treating HTTP / URL / value errors as a miss.

    Real adapters (``web.mb.get_release`` / ``web.discogs.get_release``)
    raise ``urllib.error.HTTPError`` on 404 and ``URLError`` on transport
    failure; the Discogs adapter additionally raises ``ValueError`` if
    ``int(identifier)`` fails (e.g. when the operator pastes a UUID into
    the Discogs path). All those shapes mean "this identifier isn't a
    release at this leaf" — auto-widen falls through to the group path.
    """
    try:
        return lookup(identifier)
    except _AUTO_WIDEN_MISS_EXCS as exc:
        log.debug(
            "youtube_album_service: leaf lookup raised %s for %r — treating as miss",
            type(exc).__name__, identifier,
        )
        return None


def _safe_group_lookup(
    lookup: Callable[[str], Optional[dict]],
    identifier: str,
) -> Optional[dict]:
    """Call a release-group / master lookup with the same miss tolerance."""
    try:
        return lookup(identifier)
    except _AUTO_WIDEN_MISS_EXCS as exc:
        log.debug(
            "youtube_album_service: group lookup raised %s for %r — treating as miss",
            type(exc).__name__, identifier,
        )
        return None


def _resolve_mb_group(
    identifier: str,
    mb_get_release: MBLookup,
    mb_get_release_group_releases: MBRGReleases,
) -> tuple[Optional[str], list[Any]]:
    """Resolve an MB identifier to ``(rg_id, sibling_summaries)``.

    Leaf-then-group fallback. Returns ``(None, [failure_outcome])`` if
    neither path resolves; ``failure_outcome`` is ``"not_found"`` or
    ``"mb_no_release_group"`` so the caller can surface the precise
    failure.

    Adapter errors (``urllib.error.HTTPError`` on 404,
    ``urllib.error.URLError`` on transport failure) are caught and treated
    as a leaf miss — passing a release-group MBID through ``web.mb.get_release``
    will 404 because RG MBIDs aren't releases, and we want to fall
    through to the group path rather than 500.
    """
    leaf = _safe_leaf_lookup(mb_get_release, identifier)
    if leaf:
        rg_id = leaf.get("release_group_id")
        if not rg_id:
            return None, ["mb_no_release_group"]
        group = _safe_group_lookup(mb_get_release_group_releases, rg_id)
        if not group:
            return None, ["not_found"]
        return rg_id, list(group.get("releases") or [])

    # Leaf miss → treat identifier as RG MBID.
    group = _safe_group_lookup(mb_get_release_group_releases, identifier)
    if not group:
        return None, ["not_found"]
    return identifier, list(group.get("releases") or [])


def _resolve_discogs_group(
    identifier: str,
    discogs_get_release: DiscogsLookup,
    discogs_get_master_releases: DiscogsMasterReleases,
) -> tuple[Optional[str], list[Any]]:
    """Resolve a Discogs identifier to ``(master_id, sibling_summaries)``.

    Same shape as ``_resolve_mb_group``. Adapter raises (HTTPError /
    URLError / ValueError from ``int()`` coercion) are caught and
    treated as a leaf miss so the auto-widen falls through cleanly.
    """
    leaf = _safe_leaf_lookup(discogs_get_release, identifier)
    if leaf:
        master_id = leaf.get("release_group_id")
        if not master_id:
            return None, ["mb_no_release_group"]
        group = _safe_group_lookup(discogs_get_master_releases, master_id)
        if not group:
            return None, ["not_found"]
        return master_id, list(group.get("releases") or [])

    group = _safe_group_lookup(discogs_get_master_releases, identifier)
    if not group:
        return None, ["not_found"]
    return identifier, list(group.get("releases") or [])


# ---------------------------------------------------------------------------
# Sibling enumeration + seed-pick heuristic.
# ---------------------------------------------------------------------------


def _extract_sibling_ids(summaries: list[Any]) -> list[str]:
    """Return every sibling MBID/release-ID from the group's release summaries."""
    out: list[str] = []
    for s in summaries:
        if not isinstance(s, dict):
            continue
        sid = s.get("id")
        if sid:
            out.append(str(sid))
    return out


def _fetch_mb_siblings(
    summaries: list[Any],
    source_label: str,
    mb_get_release: MBLookup,
    discogs_get_release: DiscogsLookup,
) -> list[dict]:
    """Resolve each sibling-summary into a full release record.

    Misses (mirror 404 / network error) are skipped silently — the
    matrix's per-pair distance entry will still surface
    ``mb_lookup_failed`` later when ``compute_beets_distance`` re-fetches
    the same MBID. (Per R17, partial mirror coverage shouldn't fail the
    whole resolve.)
    """
    out: list[dict] = []
    fetcher = mb_get_release if source_label == "mb" else discogs_get_release
    for summary in summaries:
        if not isinstance(summary, dict):
            continue
        sid = summary.get("id")
        if not sid:
            continue
        try:
            full = fetcher(str(sid))
        except Exception as exc:  # noqa: BLE001 — mirror errors vary
            log.warning(
                "youtube_album_service: %s sibling fetch failed for %s: %s",
                source_label, sid, exc)
            continue
        if not full:
            continue
        out.append(full)
    return out


def _pick_mb_seed(siblings: list[dict]) -> dict:
    """Lowest-year sibling, first-by-id tiebreak."""
    def _sort_key(s: dict) -> tuple[int, str]:
        year = s.get("year")
        return (year if isinstance(year, int) else 9999, str(s.get("id") or ""))
    return sorted(siblings, key=_sort_key)[0]


def _build_search_query(seed_release: dict) -> str:
    """Form the YT search query from the seed release.

    ``f"{artist_name} {album_title}"`` — the simplest deterministic
    string that pins both axes of the album.
    """
    artist = str(seed_release.get("artist_name") or "").strip()
    title = str(seed_release.get("title") or "").strip()
    return f"{artist} {title}".strip()


def _pick_yt_seed(
    search_results: list[dict],
    mb_seed: dict,
) -> Optional[str]:
    """Pick the YT search result whose ``(year, trackCount)`` is closest
    to the MB seed. Fall back to the top-ranked result on ties.

    Returns the chosen ``browseId`` or ``None`` if ``search_results`` is
    empty.
    """
    if not search_results:
        return None
    mb_year = mb_seed.get("year") if isinstance(mb_seed.get("year"), int) else None
    mb_track_count = len(mb_seed.get("tracks") or [])

    best_idx: Optional[int] = None
    best_score: Optional[tuple[int, int]] = None
    for idx, r in enumerate(search_results):
        if not isinstance(r, dict):
            continue
        if not r.get("browseId"):
            continue
        year_dist = (
            abs(_parse_year(r.get("year")) - mb_year)
            if (mb_year is not None and _parse_year(r.get("year")) is not None)
            else 9999
        ) if mb_year is not None else 0
        # Cast _parse_year(...) None → 9999 so the comparison stays
        # bounded.
        yt_year_parsed = _parse_year(r.get("year"))
        if mb_year is not None and yt_year_parsed is None:
            year_dist = 9999
        track_dist = abs(_safe_int(r.get("trackCount"), 0) - mb_track_count)
        score = (year_dist, track_dist)
        if best_score is None or score < best_score:
            best_score = score
            best_idx = idx

    if best_idx is None:
        # No entry had a browseId. Fall back to the top-ranked result
        # only if it carries one.
        for r in search_results:
            if isinstance(r, dict) and r.get("browseId"):
                return str(r["browseId"])
        return None
    chosen = search_results[best_idx]
    return str(chosen["browseId"])


def _parse_year(raw: Any) -> Optional[int]:
    """YT Music carries year as ``str`` ("1996"); MB carries int. Normalise."""
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    try:
        return int(str(raw)[:4])
    except (TypeError, ValueError):
        return None


def _safe_int(raw: Any, default: int) -> int:
    """Coerce ``raw`` to ``int``; return ``default`` on missing/garbage.

    YT payloads occasionally carry numeric fields as strings ("01"),
    floats, or ``None``. Bare ``int()`` would raise — and a single
    raised ValueError inside the per-track loop would cascade to a
    youtube_parse_failed 503 for the whole resolve. Tolerant coercion
    keeps the resolve in the happy path and lets the track end up with
    the defaulted value.
    """
    if raw is None:
        return default
    if isinstance(raw, bool):
        # ``bool`` is an ``int`` subclass but coercing True/False isn't
        # what the caller asked for; treat as garbage.
        return default
    if isinstance(raw, int):
        return raw
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _safe_float(raw: Any, default: float) -> float:
    """Coerce ``raw`` to ``float``; return ``default`` on missing/garbage."""
    if raw is None:
        return default
    if isinstance(raw, bool):
        return default
    if isinstance(raw, (int, float)):
        return float(raw)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Synthetic item construction + scoring.
# ---------------------------------------------------------------------------


def _synthesize_items(album_resp: dict) -> list[SyntheticItem]:
    """Build a ``SyntheticItem`` list from a ``ytmusicapi.get_album`` response.

    Beets's ``distance()`` reads ``title``, ``artist``, ``album``,
    ``albumartist``, ``track``, ``tracktotal``, ``disc``, ``disctotal``,
    ``length``. We populate all of them from the YT response. Missing
    fields fall back to empty / zero — beets skips missing-field
    comparisons rather than penalising them.
    """
    album_title = str(album_resp.get("title") or "")
    album_artists = album_resp.get("artists") or []
    albumartist = (
        str(album_artists[0].get("name") or "")
        if album_artists and isinstance(album_artists[0], dict)
        else ""
    )
    tracks = album_resp.get("tracks") or []
    total = len(tracks)
    out: list[SyntheticItem] = []
    for idx, t in enumerate(tracks):
        if not isinstance(t, dict):
            continue
        title = str(t.get("title") or "")
        t_artists = t.get("artists") or []
        artist = (
            str(t_artists[0].get("name") or "")
            if t_artists and isinstance(t_artists[0], dict)
            else albumartist
        )
        # ytmusicapi sometimes returns ``trackNumber`` 0-indexed; if so the
        # first track will be 0 and shift everything by 1. Beets's
        # ``assign_items`` uses 1-indexed track positions for fingerprint
        # matching, so we normalise: when the very first track reports
        # ``trackNumber == 0`` we treat the column as 0-indexed.
        raw_tn = t.get("trackNumber")
        if isinstance(raw_tn, int):
            track_num = raw_tn
        else:
            track_num = idx + 1
        if idx == 0 and track_num == 0:
            # 0-indexed payload — shift all subsequent values by 1 via
            # the closure below.
            track_num = 1
        elif idx > 0 and track_num == idx:
            # When the first track was 0-indexed (i.e. idx0 reported 0),
            # subsequent tracks will report idx as their value. Bump to
            # idx+1 so we match the typical 1-indexed convention.
            # (No-op when the payload is already 1-indexed.)
            track_num = idx + 1
        length = _safe_float(t.get("duration_seconds"), 0.0)
        out.append(SyntheticItem(
            title=title,
            artist=artist,
            album=album_title,
            albumartist=albumartist,
            track=track_num,
            tracktotal=total,
            disc=_safe_int(t.get("disc_number"), 1) or 1,
            disctotal=1,
            length=length,
        ))
    return out


def _score_against_siblings(
    synth_items: list[SyntheticItem],
    sibling_mbids: list[str],
    rg_id: str,
    distance_fn: DistanceFn,
    pdb: Any,
    mb_fetcher: Callable[[str], Optional[dict]],
) -> list[ResolvedDistance]:
    """Run ``distance_fn`` for every sibling MBID and collect typed results.

    Each call passes ``items_override=synth`` and ``mb_release_group_id=rg``
    so the cross-RG guardrail fires in the override path (R17 / U2
    contract). Per-pair failures are preserved as the entry's outcome
    rather than aborting the matrix — including ``mb_lookup_failed`` for
    siblings the local mirror doesn't carry.
    """
    out: list[ResolvedDistance] = []
    for mbid in sibling_mbids:
        if not mbid:
            continue
        try:
            r = distance_fn(
                mbid=mbid,
                items_override=synth_items,
                mb_release_group_id=rg_id,
                pdb=pdb,
                mb_get_release=mb_fetcher,
                cache=None,
            )
        except Exception as exc:  # noqa: BLE001 — surface as per-pair failure
            log.warning(
                "youtube_album_service: distance_fn raised for mbid=%s: %s",
                mbid, exc)
            out.append(ResolvedDistance(
                mbid=mbid,
                outcome="distance_failed",
                error_message=f"distance_fn raised: {exc}",
            ))
            continue
        out.append(ResolvedDistance(
            mbid=mbid,
            outcome=r.outcome,
            distance=r.distance,
            components=r.components,
            matched_tracks=r.matched_tracks,
            total_local_tracks=r.total_local_tracks,
            total_mb_tracks=r.total_mb_tracks,
            extra_local_tracks=r.extra_local_tracks,
            extra_mb_tracks=r.extra_mb_tracks,
            error_message=r.error_message,
        ))
    return out


# ---------------------------------------------------------------------------
# URL synthesis + small helpers.
# ---------------------------------------------------------------------------


def _compose_url(browse_id: str, audio_playlist_id: Any) -> str:
    """Prefer the playlist URL (the public handle); fall back to browse."""
    if audio_playlist_id:
        return f"https://music.youtube.com/playlist?list={audio_playlist_id}"
    return f"https://music.youtube.com/browse/{browse_id}"


def _video_id_for_track(album_resp: dict, track_number: int) -> Optional[str]:
    """Best-effort lookup of the videoId for a given track number."""
    for t in album_resp.get("tracks") or []:
        if isinstance(t, dict) and int(t.get("trackNumber") or 0) == track_number:
            return t.get("videoId")
    return None


def _classify_server_error(exc: Exception) -> str:
    """Map a ``YTMusicServerError`` message to 4xx vs 5xx outcome.

    ``ytmusicapi`` doesn't carry the HTTP status as a typed attribute on
    the exception — we string-match on the message it produces (which
    typically contains "Server returned HTTP <code>"). 429 is treated as
    4xx for the purposes of this mapping (rate-limit / captcha).
    """
    msg = str(exc)
    # The library emits "Server returned HTTP NNN" — pull the digits out.
    for code_str in ("400", "401", "403", "404", "429"):
        if code_str in msg:
            return "unresolved_4xx_client"
    for code_str in ("500", "502", "503", "504"):
        if code_str in msg:
            return "unresolved_mirror_unavailable"
    # Default for unparseable / unknown server-side messages: assume the
    # mirror is unavailable rather than blaming the caller.
    return "unresolved_mirror_unavailable"


def _rows_to_youtube_releases(
    rows: list[dict],
) -> list[ResolvedYoutubeRelease]:
    """Deserialize cached DB rows back into typed structs."""
    out: list[ResolvedYoutubeRelease] = []
    for row in rows:
        tracks_raw = row.get("yt_tracks") or []
        synth_tracks: list[SyntheticItem] = []
        total_local = len(tracks_raw)
        for t in tracks_raw:
            if not isinstance(t, dict):
                continue
            artists = t.get("artists") or []
            primary_artist = (
                str(artists[0].get("name") or "")
                if artists and isinstance(artists[0], dict)
                else ""
            )
            synth_tracks.append(SyntheticItem(
                title=str(t.get("title") or ""),
                artist=primary_artist,
                album="",  # not persisted per-track; recoverable from row
                albumartist=primary_artist,
                track=int(t.get("track_number") or 0),
                tracktotal=total_local,
                disc=int(t.get("disc_number") or 1),
                disctotal=1,
                length=float(t.get("length_seconds") or 0.0),
            ))

        distances_raw = row.get("distances") or []
        distances: list[ResolvedDistance] = []
        for d in distances_raw:
            if not isinstance(d, dict):
                continue
            distances.append(ResolvedDistance(
                mbid=str(d.get("mbid") or ""),
                outcome=str(d.get("outcome") or ""),
                distance=d.get("distance"),
                components=d.get("components"),
                matched_tracks=d.get("matched_tracks"),
                total_local_tracks=d.get("total_local_tracks"),
                total_mb_tracks=d.get("total_mb_tracks"),
                extra_local_tracks=d.get("extra_local_tracks"),
                extra_mb_tracks=d.get("extra_mb_tracks"),
                error_message=d.get("error_message"),
            ))

        out.append(ResolvedYoutubeRelease(
            yt_browse_id=str(row.get("yt_browse_id") or ""),
            yt_audio_playlist_id=row.get("yt_audio_playlist_id"),
            yt_url=str(row.get("yt_url") or ""),
            year=row.get("yt_year"),
            track_count=int(row.get("yt_track_count") or 0),
            tracks=synth_tracks,
            distances=distances,
        ))
    return out


def _final(
    *,
    outcome: str,
    release_group_identifier: Optional[str] = None,
    source: Optional[str] = None,
    from_cache: bool = False,
    youtube_releases: Optional[list[ResolvedYoutubeRelease]] = None,
    error_message: Optional[str] = None,
    started: float,
) -> YoutubeAlbumResolverResult:
    return YoutubeAlbumResolverResult(
        outcome=outcome,
        release_group_identifier=release_group_identifier,
        source=source,
        from_cache=from_cache,
        youtube_releases=youtube_releases or [],
        error_message=error_message,
        duration_ms=int((time.monotonic() - started) * 1000),
    )
