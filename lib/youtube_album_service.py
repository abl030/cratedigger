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
import re
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
from lib.release_identity import detect_release_source, normalize_release_id


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


OUTCOME_HTTP_STATUS: dict[str, int] = {
    "ok": 200,
    "not_found": 404,
    "no_release_group": 422,
    "unresolved_4xx_client": 503,
    "unresolved_mirror_unavailable": 503,
    "unresolved_timeout": 503,
    "youtube_parse_failed": 503,
    "transient": 503,
}
"""Service outcome → HTTP status. U8 imports this directly.

The outcome set is pinned by the test
``test_outcome_set_is_stable`` which asserts
``set(OUTCOME_HTTP_STATUS) == set(OUTCOME_EXIT_CODE)`` — keep these
two dicts in sync. Per-pair outcomes (``ok``, ``wrong_release_group``,
``mb_lookup_failed``, ``mb_no_release_group``, ``no_audio``,
``empty_items_override``, ``invalid_input``, ``distance_failed``) flow
through from ``compute_beets_distance`` verbatim inside
``ResolvedDistance.outcome`` — they are NOT service-level outcomes.

``no_release_group`` (renamed from ``mb_no_release_group``) covers
both the MB leaf-with-no-release-group case AND the Discogs
leaf-with-no-master case (R12 / U2). The MB-specific name was
misleading because the Discogs path was also using it."""


OUTCOME_EXIT_CODE: dict[str, int] = {
    "ok": 0,
    "not_found": 2,
    "no_release_group": 3,
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


# ---------------------------------------------------------------------------
# Persisted JSONB shapes — wire-boundary structs for the durable cache.
# ---------------------------------------------------------------------------
#
# ``youtube_album_mappings.yt_tracks`` and ``.distances`` are JSONB
# columns. Per the wire-boundary rule in ``.claude/rules/code-quality.md``,
# anything that crosses JSON gets a typed Struct and validates at the
# decode site — we cannot rely on Pyright seeing into ``dict.get()``.
# ``msgspec.convert`` is the read-side detector for malformed rows.


class PersistedTrack(msgspec.Struct, kw_only=True):
    """One persisted track inside ``yt_tracks`` JSONB."""

    title: Optional[str] = None
    artists: Optional[list[dict[str, Any]]] = None
    length_seconds: Optional[float] = None
    track_number: Optional[int] = None
    disc_number: Optional[int] = None
    video_id: Optional[str] = None


class PersistedDistance(msgspec.Struct, kw_only=True):
    """One persisted per-pair distance inside ``distances`` JSONB."""

    mbid: Optional[str] = None
    outcome: Optional[str] = None
    distance: Optional[float] = None
    components: Optional[dict[str, float]] = None
    matched_tracks: Optional[int] = None
    total_local_tracks: Optional[int] = None
    total_mb_tracks: Optional[int] = None
    extra_local_tracks: Optional[int] = None
    extra_mb_tracks: Optional[int] = None
    error_message: Optional[str] = None


class PersistedYoutubeRow(msgspec.Struct, kw_only=True):
    """One persisted row in ``youtube_album_mappings``.

    Outer columns (``id``, ``release_group_identifier``, ``source``,
    ``resolved_at``) aren't carried here — the read path
    (``get_youtube_album_mapping``) keys by
    ``(release_group_identifier, source)`` so those fields are
    redundant. JSONB columns are decoded via ``msgspec.convert``;
    everything else is row metadata.
    """

    yt_browse_id: Optional[str] = None
    yt_audio_playlist_id: Optional[str] = None
    yt_url: Optional[str] = None
    yt_year: Optional[int] = None
    yt_track_count: Optional[int] = None
    album_title: Optional[str] = None
    yt_tracks: list[PersistedTrack] = msgspec.field(default_factory=list)
    distances: list[PersistedDistance] = msgspec.field(default_factory=list)


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
    deadline_seconds: int = 60,
) -> YoutubeAlbumResolverResult:
    """Resolve a release identifier to the YT Music distance matrix.

    See module docstring for the full flow. ``cache`` is for the
    upstream HTTP responses (Redis adapter, ``None`` = no caching). The
    durable cache is ``pdb.youtube_album_mappings``.

    ``sleep_fn`` is the jitter hook injected between consecutive YT
    ``get_album`` calls. Production defaults to ``time.sleep``; tests
    pass ``lambda _: None`` so they don't pay the 1-3s pause per
    sibling.

    ``deadline_seconds`` is a soft deadline. After each external call
    we check ``time.monotonic() - started`` against this budget; on
    breach the service returns the partial matrix collected so far with
    ``outcome="ok"`` and an ``error_message`` describing the breach.
    The deadline is "best effort" — we never abort a call mid-flight,
    only skip the next one.
    """
    started = time.monotonic()
    identifier = normalize_release_id(identifier)

    source_label = _classify_source(identifier)
    if source_label is None:
        return _final(
            outcome="not_found",
            error_message=f"identifier {identifier!r} is neither an MB UUID "
                          f"nor a Discogs ID",
            started=started,
        )

    # Step 0.5: speculative cache lookup before auto-widen.
    #
    # The normal flow runs the mirror-side auto-widen first to resolve
    # the release-group identifier, then reads the cache. That means a
    # mirror outage breaks resolves even for release groups whose
    # matrix is already cached. When the caller already passed an
    # rg-MBID (or Discogs master-id), the cache is keyed on exactly
    # that value — we can answer without touching the mirror at all.
    #
    # When the caller passed a release-level identifier (the common
    # case), this speculative lookup misses and we fall through to the
    # auto-widen below. The cost is one extra indexed PG query per
    # release-level resolve — sub-millisecond.
    if not refresh:
        speculative = pdb.get_youtube_album_mapping(identifier, source_label)
        if speculative is not None:
            return _final(
                outcome="ok",
                release_group_identifier=identifier,
                source=source_label,
                from_cache=True,
                youtube_releases=_rows_to_youtube_releases(speculative),
                started=started,
            )

    # Step 1+2: auto-widen via leaf-then-group fallback.
    if source_label == "mb":
        widen = _resolve_mb_group(
            identifier, mb_get_release, mb_get_release_group_releases)
    else:
        widen = _resolve_discogs_group(
            identifier, discogs_get_release, discogs_get_master_releases)

    if widen.rg_id is None:
        return _final(
            outcome=widen.failure_outcome or "not_found",
            error_message=f"could not resolve identifier {identifier!r} to a "
                          f"release group",
            started=started,
        )
    rg_id = widen.rg_id
    sibling_summaries = widen.sibling_summaries

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
    deadline_message: Optional[str] = None

    def _deadline_breached() -> bool:
        return (time.monotonic() - started) > float(deadline_seconds)

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
            if _deadline_breached():
                deadline_message = (
                    f"deadline exceeded after "
                    f"{len(yt_album_responses)} YT siblings; "
                    f"returning partial matrix"
                )
                log.warning(
                    "youtube_album_service: %s", deadline_message)
                break
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

    # If we exited the YT loop because of a deadline breach but with
    # at least the seed in hand, fall through and synthesize the
    # partial matrix; the deadline message is attached at the final
    # return below.

    # Step 9-10: synthesize items per YT sibling and score N×M.
    youtube_releases: list[ResolvedYoutubeRelease] = []
    persistable_rows: list[dict[str, Any]] = []

    for browse_id, album_resp in yt_album_responses.items():
        synth_items = _synthesize_items(album_resp)
        if not synth_items:
            # An empty track list defeats scoring; skip the sibling so the
            # matrix doesn't carry a row with no data to compare against.
            continue
        # ``raw_tracks`` is paired by index with ``synth_items`` — a
        # malformed entry that was filtered out of ``_synthesize_items``
        # would break this pairing, but ``_synthesize_items`` accepts
        # every entry the YT API returns (only the per-entry ``isinstance``
        # check skips non-dict rows, which is also enforced here).
        raw_tracks = [
            t for t in (album_resp.get("tracks") or []) if isinstance(t, dict)
        ]
        distances = _score_against_siblings(
            synth_items, sibling_ids, rg_id, distance_fn, pdb,
            mb_get_release if source_label == "mb" else discogs_get_release,
        )
        yt_url = _compose_url(browse_id, album_resp.get("audioPlaylistId"))
        year = _parse_year(album_resp.get("year"))
        album_title = str(album_resp.get("title") or "")
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
        # Pair ``video_id`` by list index, not by track-number lookup —
        # YT payloads occasionally carry duplicate ``trackNumber`` values
        # (e.g. multiple "track 1" entries on a multi-disc album, or
        # 0-indexed payloads) and looking up by number drops the wrong
        # videoId. Indexed pairing is unambiguous.
        persistable_tracks: list[dict[str, Any]] = []
        for synth_i, si in enumerate(synth_items):
            raw = raw_tracks[synth_i] if synth_i < len(raw_tracks) else {}
            persistable_tracks.append({
                "title": si.title,
                "artists": [{"name": si.artist}],
                "length_seconds": si.length,
                "track_number": si.track,
                "disc_number": si.disc,
                "video_id": raw.get("videoId"),
            })
        persistable_rows.append({
            "yt_browse_id": browse_id,
            "yt_audio_playlist_id": album_resp.get("audioPlaylistId"),
            "yt_url": yt_url,
            "yt_year": year,
            "yt_track_count": yt_rel.track_count,
            # ``album_title`` is persisted per row so the cache-fallback
            # ``_rows_to_youtube_releases`` can rehydrate ``SyntheticItem.album``
            # — without it, cached rows always returned the lossy
            # ``album=""`` placeholder (review finding #15). Stored at the
            # row scope (not per track) since it's a row-level fact.
            "album_title": album_title,
            "yt_tracks": persistable_tracks,
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
        error_message=deadline_message,
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


class _GroupResolution(msgspec.Struct, kw_only=True):
    """Result of leaf-then-group auto-widen.

    Replaces the overloaded ``tuple[Optional[str], list[Any]]`` return
    shape where ``list[Any]`` was either the sibling summaries (on
    success) or a single-element list carrying the failure outcome (on
    failure). Two-way overloads on container shape are a smell — this
    Struct names each axis explicitly so the caller can branch on
    ``failure_outcome`` instead of reverse-engineering the list.
    """

    rg_id: Optional[str] = None
    sibling_summaries: list[Any] = msgspec.field(default_factory=list)
    failure_outcome: Optional[str] = None


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
) -> _GroupResolution:
    """Resolve an MB identifier to a typed group result.

    Leaf-then-group fallback. ``failure_outcome`` is populated when
    neither path resolves; the caller checks ``rg_id is None`` to
    branch.

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
            return _GroupResolution(failure_outcome="no_release_group")
        group = _safe_group_lookup(mb_get_release_group_releases, rg_id)
        if not group:
            return _GroupResolution(failure_outcome="not_found")
        return _GroupResolution(
            rg_id=rg_id,
            sibling_summaries=list(group.get("releases") or []),
        )

    # Leaf miss → treat identifier as RG MBID.
    group = _safe_group_lookup(mb_get_release_group_releases, identifier)
    if not group:
        return _GroupResolution(failure_outcome="not_found")
    return _GroupResolution(
        rg_id=identifier,
        sibling_summaries=list(group.get("releases") or []),
    )


def _resolve_discogs_group(
    identifier: str,
    discogs_get_release: DiscogsLookup,
    discogs_get_master_releases: DiscogsMasterReleases,
) -> _GroupResolution:
    """Resolve a Discogs identifier to a typed group result.

    Same shape as ``_resolve_mb_group``. The Discogs leaf-failure outcome
    is ``"no_release_group"`` since Discogs has no concept of release
    groups — the analogous concept is a master, but the failure
    semantically means "the upstream release doesn't point at a
    group / master we can widen to".
    """
    leaf = _safe_leaf_lookup(discogs_get_release, identifier)
    if leaf:
        master_id = leaf.get("release_group_id")
        if not master_id:
            return _GroupResolution(failure_outcome="no_release_group")
        group = _safe_group_lookup(discogs_get_master_releases, master_id)
        if not group:
            return _GroupResolution(failure_outcome="not_found")
        return _GroupResolution(
            rg_id=master_id,
            sibling_summaries=list(group.get("releases") or []),
        )

    group = _safe_group_lookup(discogs_get_master_releases, identifier)
    if not group:
        return _GroupResolution(failure_outcome="not_found")
    return _GroupResolution(
        rg_id=identifier,
        sibling_summaries=list(group.get("releases") or []),
    )


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
        # Compute year proximity once. ``_parse_year`` normalises both
        # int and str forms; ``None`` (missing or unparseable year on
        # the YT side) is bumped to a large sentinel so the comparison
        # stays bounded but still loses to any real match.
        yt_year_parsed = _parse_year(r.get("year"))
        if mb_year is None:
            year_dist = 0
        elif yt_year_parsed is None:
            year_dist = 9999
        else:
            year_dist = abs(yt_year_parsed - mb_year)
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


def _normalize_track_number(idx: int, raw_tn: Any, *, zero_indexed: bool) -> int:
    """Return a 1-indexed track number from a YT payload entry.

    The YT API mostly emits 1-indexed ``trackNumber``, but some payloads
    are 0-indexed (the first track reports ``trackNumber == 0``). Beets's
    ``assign_items`` is 1-indexed, so the resolver normalises before
    storage. ``zero_indexed`` is detected once on the first track and
    propagated to every subsequent track in the same payload — without
    that flag, a 1-indexed payload that happens to repeat
    ``trackNumber: 1`` on multiple discs would false-fire the heuristic.
    """
    if isinstance(raw_tn, int):
        if zero_indexed:
            return raw_tn + 1
        return raw_tn
    # Missing / non-int trackNumber falls back to positional.
    return idx + 1


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

    # Detect 0-indexed payload on the first usable track only. Without
    # this flag we'd misclassify a 1-indexed payload that legitimately
    # repeats ``trackNumber: 1`` (e.g. duplicate or malformed entries)
    # as 0-indexed.
    zero_indexed = False
    for t in tracks:
        if isinstance(t, dict):
            raw_first = t.get("trackNumber")
            if isinstance(raw_first, int) and raw_first == 0:
                zero_indexed = True
            break

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
        raw_tn = t.get("trackNumber")
        track_num = _normalize_track_number(
            idx, raw_tn, zero_indexed=zero_indexed)
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


_HTTP_CODE_RE = re.compile(r"\bHTTP\s+(\d{3})\b")


def _classify_server_error(exc: Exception) -> str:
    """Map a ``YTMusicServerError`` message to 4xx vs 5xx outcome.

    ``ytmusicapi`` doesn't carry the HTTP status as a typed attribute on
    the exception — we regex-extract it from the message (which the
    library emits as ``"Server returned HTTP NNN: ..."``). 429 is
    treated as 4xx for the purposes of this mapping (rate-limit /
    captcha).

    Previously this scanned for substrings like ``"500"`` in the message,
    which false-fires when the error body contains a stray digit triplet
    (e.g. ``"HTTP 200 OK, but body had error 500-ish content"``). The
    regex pins the ``HTTP <code>`` shape so composite messages can't
    mis-classify the outcome.
    """
    msg = str(exc)
    m = _HTTP_CODE_RE.search(msg)
    if m is None:
        # Default for unparseable / unknown server-side messages: assume
        # the mirror is unavailable rather than blaming the caller.
        return "unresolved_mirror_unavailable"
    code = int(m.group(1))
    if code in (400, 401, 403, 404, 429):
        return "unresolved_4xx_client"
    if 500 <= code < 600:
        return "unresolved_mirror_unavailable"
    return "unresolved_mirror_unavailable"


def _rows_to_youtube_releases(
    rows: list[dict],
) -> list[ResolvedYoutubeRelease]:
    """Deserialize cached DB rows back into typed structs.

    Wire-boundary decode via ``msgspec.convert``: every JSONB row is
    validated against ``PersistedYoutubeRow`` before we touch its
    fields. Malformed JSONB (drifted shape, schema change, manual
    insert) raises ``msgspec.ValidationError`` at this seam rather
    than silently producing a ``SyntheticItem`` with garbage values.

    The ``album`` field on each ``SyntheticItem`` is rehydrated from
    the row-level ``album_title`` field — older cached rows
    (pre-album_title) fall back to an empty string. New writes
    always include it.
    """
    out: list[ResolvedYoutubeRelease] = []
    for raw_row in rows:
        # ``msgspec.convert`` validates the JSONB shape; bare dicts /
        # decoded JSON are accepted, ``DictRow`` rows from psycopg2 are
        # also accepted via ``dict(row)`` upstream.
        row = msgspec.convert(raw_row, type=PersistedYoutubeRow)
        album_title = row.album_title or ""
        total_local = len(row.yt_tracks)
        synth_tracks: list[SyntheticItem] = []
        for t in row.yt_tracks:
            artists = t.artists or []
            primary_artist = (
                str(artists[0].get("name") or "")
                if artists and isinstance(artists[0], dict)
                else ""
            )
            synth_tracks.append(SyntheticItem(
                title=t.title or "",
                artist=primary_artist,
                album=album_title,
                albumartist=primary_artist,
                track=t.track_number or 0,
                tracktotal=total_local,
                disc=t.disc_number or 1,
                disctotal=1,
                length=t.length_seconds or 0.0,
            ))

        distances: list[ResolvedDistance] = []
        for d in row.distances:
            distances.append(ResolvedDistance(
                mbid=d.mbid or "",
                outcome=d.outcome or "",
                distance=d.distance,
                components=d.components,
                matched_tracks=d.matched_tracks,
                total_local_tracks=d.total_local_tracks,
                total_mb_tracks=d.total_mb_tracks,
                extra_local_tracks=d.extra_local_tracks,
                extra_mb_tracks=d.extra_mb_tracks,
                error_message=d.error_message,
            ))

        out.append(ResolvedYoutubeRelease(
            yt_browse_id=row.yt_browse_id or "",
            yt_audio_playlist_id=row.yt_audio_playlist_id,
            yt_url=row.yt_url or "",
            year=row.yt_year,
            track_count=row.yt_track_count or 0,
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
