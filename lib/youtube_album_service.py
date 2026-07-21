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
import socket
import time
import urllib.error
from typing import Any, Callable, Optional, Protocol, runtime_checkable

import msgspec
import requests

from ytmusicapi.exceptions import YTMusicError, YTMusicServerError, YTMusicUserError

from lib.beets_distance import (
    BeetsDistanceCache,
    BeetsDistanceResult,
    SyntheticItem,
    compute_beets_distance as _default_distance_fn,
)
from lib.pipeline_db import PersistedDistance, PersistedTrack, PersistedYoutubeRow
from lib.release_identity import detect_release_source, normalize_release_id


# Exception classes that the MB / Discogs adapters in ``web/mb.py`` and
# ``web/discogs.py`` raise on miss / mirror outage.
#
# Round 2 P1-1: this tuple used to catch every ``URLError`` /
# ``requests.RequestException`` and swallow it as "leaf miss," which
# misclassified mirror outages, transport timeouts, and 5xx responses
# as 404. The resolver then fabricated a release-group identifier from
# the operator's input — producing wrong matrices keyed to a missing
# release. The narrowed contract:
#
# * ``HTTPError(404)`` (urllib) and ``HTTPError`` (requests) with
#   ``response.status_code == 404`` are **the only** "leaf miss"
#   signal — see ``_is_leaf_miss``.
# * ``ValueError`` keeps its place because the Discogs adapter does
#   ``int(identifier)`` and a UUID pasted into the Discogs path raises
#   ``ValueError`` — semantically "not a release at this leaf."
# * Every other ``URLError`` / ``RequestException`` / ``Timeout``
#   propagates up to the top-level handler in ``resolve_youtube_album``
#   which classifies it as ``unresolved_mirror_unavailable`` /
#   ``unresolved_timeout`` rather than silently fabricating data.
_AUTO_WIDEN_MISS_EXCS: tuple[type[BaseException], ...] = (
    urllib.error.HTTPError,
    requests.HTTPError,
    ValueError,
)


def _is_leaf_miss(exc: BaseException) -> bool:
    """Return True when ``exc`` is a real 404 (or equivalent), False otherwise.

    A 404 means "this identifier isn't a release at this leaf" — fall
    through to the group path. Anything else (transport failure, 5xx,
    URLError without an HTTP status) is a mirror / network outage that
    must propagate so the resolver returns ``unresolved_*`` instead of
    fabricating the rg_id from the input.

    ``ValueError`` is treated as a leaf miss unconditionally (Discogs
    ``int()`` coercion failure on a UUID); see the docstring on
    ``_AUTO_WIDEN_MISS_EXCS`` for the rationale.
    """
    if isinstance(exc, ValueError):
        return True
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code == 404
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        return getattr(resp, "status_code", None) == 404
    return False

log = logging.getLogger(__name__)


def _json_list(value: object) -> list[object]:
    """Narrow an untyped ytmusicapi/MB/Discogs JSON value to a plain list.

    ``isinstance(value, list)`` alone leaves pyright with a partially-
    unknown ``list[Unknown]`` even when ``value`` was already fully
    known — strict mode never lets an ``isinstance`` narrowing inherit
    a generic's type argument. Routing through ``msgspec.convert`` gives
    every caller a fully known ``list[object]`` back, with no change to
    the elements themselves (each stays the exact same object —
    verified: ``msgspec.convert`` does not copy or coerce elements at
    ``object`` value type). A non-list value returns ``[]`` — graceful
    narrowing, never an assertion, per this module's external-JSON
    contract (a malformed field degrades to absent, it never crashes
    the resolver).

    Callers must pass a freshly-evaluated expression (e.g. a ``dict``
    subscript/``.get()``), not an already ``isinstance``-narrowed local
    — the narrowing taint survives even at this declared ``object``
    parameter, same as it survives at the call site itself.
    """
    if not isinstance(value, list):
        return []
    return msgspec.convert(value, type=list[object])


def _json_dict(value: object) -> dict[str, object]:
    """Narrow an untyped ytmusicapi/MB/Discogs JSON value to a plain
    string-keyed dict.

    Dict counterpart of ``_json_list`` — see its docstring for why the
    ``msgspec.convert`` indirection is needed, why callers must pass a
    fresh expression rather than an already-narrowed local, and why a
    non-dict value gracefully returns ``{}`` rather than asserting.
    """
    if not isinstance(value, dict):
        return {}
    return msgspec.convert(value, type=dict[str, object])


def _is_dict_like(value: object) -> bool:
    """``isinstance(value, dict)`` behind a plain function boundary.

    A loop that needs to gate on "is this entry a dict" *before* calling
    ``_json_dict`` (rather than letting ``_json_dict`` itself degrade a
    non-dict to ``{}``) can't use a bare ``isinstance`` check as the
    gate: pyright narrows the loop variable to a partially-unknown
    ``dict[Unknown, Unknown]``, which then taints the ``_json_dict``
    call even at its declared ``object`` parameter. A plain (non-
    ``TypeGuard``) function does the identical runtime check without
    pyright narrowing the caller's variable, so the loop variable stays
    cleanly ``object``-typed all the way into ``_json_dict``.
    """
    return isinstance(value, dict)


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
#
# Round 2 P2-4: defaults shrunk from 1.0-3.0s to 0.5-1.5s. The
# previous 1-3s range added up to 30s of pure jitter on a 10-sibling
# cold resolve, which is operator-perceptible latency on top of the
# 60s deadline. Tighten the band; operators who want the old behaviour
# can pass ``jitter_range`` explicitly. The cumulative jitter is also
# logged so operators see how much of a resolve's wall-clock is the
# anti-throttle pause vs real network work.
_JITTER_MIN_SECONDS_DEFAULT = 0.5
_JITTER_MAX_SECONDS_DEFAULT = 1.5


def _default_jitter_sleep_fn(seconds: float) -> None:
    """Default sleep used between YT calls. Tests pass ``lambda _: None``."""
    time.sleep(seconds)


def _jitter(
    sleep_fn: Callable[[float], None],
    *,
    min_seconds: float = _JITTER_MIN_SECONDS_DEFAULT,
    max_seconds: float = _JITTER_MAX_SECONDS_DEFAULT,
) -> float:
    """Sleep a uniform-random duration; return the sleep we requested.

    Returning the sleep duration lets the caller accumulate it for
    operator observability — see ``cumulative_jitter`` in the
    resolver's main loop. Tests that pass ``lambda _: None`` get the
    same return value, so cumulative accounting stays test-stable.
    """
    seconds = random.uniform(min_seconds, max_seconds)
    sleep_fn(seconds)
    return seconds


# ---------------------------------------------------------------------------
# Outcome vocabulary — shared with CLI (U7) and web route (U8).
# ---------------------------------------------------------------------------


OUTCOME_HTTP_STATUS: dict[str, int] = {
    "ok": 200,
    "not_found": 404,
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

Orphan releases (Discogs releases with no master, legacy MB releases
with no release group) used to surface as ``no_release_group``; they
now resolve as one-element matrices via ``_GroupResolution.is_orphan``
— see #384. The ``no_release_group`` outcome is no longer emitted at
the service level."""


OUTCOME_EXIT_CODE: dict[str, int] = {
    "ok": 0,
    "not_found": 2,
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
        default_factory=list[ResolvedYoutubeRelease])
    error_message: Optional[str] = None
    duration_ms: Optional[int] = None


# ---------------------------------------------------------------------------
# Persisted JSONB shapes — wire-boundary structs for the durable cache.
# ---------------------------------------------------------------------------
#
# ``PersistedTrack`` / ``PersistedDistance`` / ``PersistedYoutubeRow`` live
# in ``lib.pipeline_db`` (moved there in #546 W3) because
# ``PipelineDB.upsert_youtube_album_mapping`` derives its INSERT column
# list from ``msgspec.structs.fields(PersistedYoutubeRow)`` at runtime —
# the DB layer needs the type. This module is the producer (the resolver
# builds the rows) and a read-hydrator (``_rows_to_youtube_releases``
# decodes cached rows via ``msgspec.convert``), so the names stay bound
# here via this import.


# Type aliases for clarity.
MBLookup = Callable[[str], Optional[dict[str, object]]]
"""``mb_get_release(id) -> slim release dict | None`` (web/mb.py shape)."""

MBRGReleases = Callable[[str], Optional[dict[str, object]]]
"""``mb_get_release_group_releases(rg) -> {title, type, releases[]}``."""

DiscogsLookup = Callable[[str], Optional[dict[str, object]]]
"""``discogs_get_release(id) -> slim release dict | None``."""

DiscogsMasterReleases = Callable[[str], Optional[dict[str, object]]]
"""``discogs_get_master_releases(master_id) -> {title, type, releases[]}``."""

_ReleaseOrGroupLookup = Callable[[str], Optional[dict[str, object]]]
"""Structurally identical to ``MBLookup``/``MBRGReleases``/
``DiscogsLookup``/``DiscogsMasterReleases`` above — used as the
parameter type for the two source-agnostic helpers
(``_safe_leaf_lookup``/``_safe_group_lookup``) that accept either an MB
or a Discogs callable interchangeably, so the name doesn't imply one
source over the other."""

DistanceFn = Callable[..., BeetsDistanceResult]
"""``compute_beets_distance(...)`` shape — service injects this so tests
can supply canned results."""


@runtime_checkable
class YoutubeResolverDB(Protocol):
    """The PipelineDB surface the resolver uses directly (#409).

    The handle is also forwarded into ``distance_fn`` (a ``Callable[...]``
    — ``compute_beets_distance`` types its own db param). Parity tests
    live in ``tests/test_youtube_album_service.py``.
    """

    def get_youtube_album_mapping(
        self, release_group_identifier: str, source: str,
    ) -> Optional[list[dict[str, Any]]]: ...

    def upsert_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
        rows: list[PersistedYoutubeRow],
    ) -> None: ...


# ---------------------------------------------------------------------------
# Public service entrypoint.
# ---------------------------------------------------------------------------


def resolve_youtube_album(
    identifier: str,
    *,
    pdb: YoutubeResolverDB,
    mb_get_release: MBLookup,
    mb_get_release_group_releases: MBRGReleases,
    discogs_get_release: DiscogsLookup,
    discogs_get_master_releases: DiscogsMasterReleases,
    yt_client: Any,
    distance_fn: DistanceFn = _default_distance_fn,
    cache: Optional[BeetsDistanceCache] = None,
    refresh: bool = False,
    sleep_fn: Callable[[float], None] = _default_jitter_sleep_fn,
    jitter_range: tuple[float, float] = (
        _JITTER_MIN_SECONDS_DEFAULT, _JITTER_MAX_SECONDS_DEFAULT),
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
    #
    # Round 2 P1-2: SKIP this short-circuit for ``source_label ==
    # "discogs"``. Discogs release-ids and master-ids share the integer
    # namespace (``release-12345`` is a different entity from
    # ``master-12345``), so a cached matrix written under the master ID
    # would be served for an unrelated release whose ID collides. MB
    # UUIDs don't have this problem — release-MBIDs and release-group-
    # MBIDs are drawn from disjoint UUID space. The asymmetry is
    # documented and tested below; the cost is one extra mirror call on
    # cold-cache Discogs resolves.
    if not refresh and source_label == "mb":
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
    #
    # Round 2 P1-1: only 404s (and the Discogs ValueError) are caught
    # inside the safe-lookup helpers; mirror outages now propagate and
    # surface here as ``unresolved_mirror_unavailable`` /
    # ``unresolved_timeout`` instead of silently falling through to
    # "not_found" with a fabricated rg_id.
    try:
        if source_label == "mb":
            widen = _resolve_mb_group(
                identifier, mb_get_release, mb_get_release_group_releases)
        else:
            widen = _resolve_discogs_group(
                identifier, discogs_get_release,
                discogs_get_master_releases)
    except (requests.Timeout, TimeoutError, socket.timeout) as exc:
        return _final(
            outcome="unresolved_timeout",
            error_message=f"mirror timeout while widening "
                          f"{identifier!r}: {exc}",
            started=started,
        )
    except (urllib.error.HTTPError, urllib.error.URLError,
            requests.RequestException) as exc:
        return _final(
            outcome="unresolved_mirror_unavailable",
            error_message=f"mirror unavailable while widening "
                          f"{identifier!r}: {exc}",
            started=started,
        )

    if widen.rg_id is None:
        return _final(
            outcome=widen.failure_outcome or "not_found",
            error_message=f"could not resolve identifier {identifier!r} to a "
                          f"release group",
            started=started,
        )
    rg_id = widen.rg_id
    sibling_summaries = widen.sibling_summaries
    # For orphan releases (no MB release-group / no Discogs master), the
    # per-pair distance call passes ``mb_release_group_id=None`` so
    # ``compute_beets_distance`` skips its cross-RG guardrail — there's
    # nothing to compare against. ``rg_id`` still keys the persistence
    # cache (using the leaf's own identifier).
    distance_rg_id: Optional[str] = None if widen.is_orphan else rg_id

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
    yt_album_responses: dict[str, dict[str, Any]] = {}
    deadline_message: Optional[str] = None
    # Round 2 P2-4: accumulate the jitter so operators can see how
    # much of the wall-clock time is the anti-throttle pause vs real
    # network work. Logged at the resolve boundary and attached to
    # ``error_message`` when non-zero.
    cumulative_jitter_seconds = 0.0
    jitter_min, jitter_max = jitter_range

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
        for other_raw in _json_list(seed_album.get("other_versions")):
            if _deadline_breached():
                deadline_message = (
                    f"deadline exceeded after "
                    f"{len(yt_album_responses)} YT siblings; "
                    f"returning partial matrix"
                )
                log.warning(
                    "youtube_album_service: %s", deadline_message)
                break
            other = _json_dict(other_raw)
            other_browse_id = other.get("browseId")
            if (
                not isinstance(other_browse_id, str)
                or not other_browse_id
                or other_browse_id in yt_album_responses
            ):
                continue
            # Jitter between consecutive ``get_album`` calls. Default
            # 0.5-1.5s (round 2 P2-4 — tightened from 1-3s) so the
            # cumulative pause is bounded on large release groups; the
            # operator can pass ``jitter_range`` to widen it back out.
            # The first ``get_album`` (seed) doesn't sleep.
            cumulative_jitter_seconds += _jitter(
                sleep_fn,
                min_seconds=jitter_min,
                max_seconds=jitter_max,
            )
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
    #
    # Round 2 P1-3: the deadline check fires not just between YT
    # ``get_album`` calls but also between scoring iterations. Previously
    # one slow ``distance_fn`` call could overshoot the budget; now we
    # break early and persist the partial matrix we have. Deadline
    # status is attached to ``error_message`` at the final return below.
    youtube_releases: list[ResolvedYoutubeRelease] = []
    persistable_rows: list[PersistedYoutubeRow] = []

    for browse_id, album_resp in yt_album_responses.items():
        if _deadline_breached():
            if deadline_message is None:
                deadline_message = (
                    f"deadline exceeded after "
                    f"{len(youtube_releases)} YT siblings scored; "
                    f"returning partial matrix"
                )
                log.warning("youtube_album_service: %s", deadline_message)
            break
        synth_items = _synthesize_items(album_resp)
        if not synth_items:
            # An empty track list defeats scoring; skip the sibling so the
            # matrix doesn't carry a row with no data to compare against.
            continue
        distances = _score_against_siblings(
            synth_items, sibling_ids, distance_rg_id, distance_fn, pdb,
            mb_get_release if source_label == "mb" else discogs_get_release,
            deadline_breached=_deadline_breached,
        )
        yt_url = _compose_url(browse_id, album_resp.get("audioPlaylistId"))
        year = _parse_year(album_resp.get("year"))
        album_title = str(album_resp.get("title") or "")
        # Sourced from the album-level ``artists[0]`` (not the per-track
        # artist) so a Various Artists release stamps "Various" once at
        # row scope, distinct from each track's individual credit. Pulled
        # off ``synth_items[0].albumartist`` since ``_synthesize_items``
        # already did the album-vs-track-artist resolution.
        album_artist = synth_items[0].albumartist if synth_items else ""
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
        # Pair by index rather than track number because YT payloads may
        # carry duplicate or zero-indexed trackNumber values. The synth
        # list was built from the same input order immediately above.
        persistable_tracks: list[PersistedTrack] = []
        raw_tracks = _json_list(album_resp.get("tracks"))
        for idx, si in enumerate(synth_items):
            raw_track = (
                _json_dict(raw_tracks[idx]) if idx < len(raw_tracks) else {}
            )
            video_id = raw_track.get("videoId")
            persistable_tracks.append(PersistedTrack(
                title=si.title,
                artists=[{"name": si.artist}],
                length_seconds=si.length,
                track_number=si.track,
                disc_number=si.disc,
                video_id=video_id if isinstance(video_id, str) else None,
            ))
        persistable_rows.append(PersistedYoutubeRow(
            yt_browse_id=browse_id,
            yt_audio_playlist_id=album_resp.get("audioPlaylistId"),
            yt_url=yt_url,
            yt_year=year,
            yt_track_count=yt_rel.track_count,
            # ``album_title`` + ``album_artist`` are persisted per row so
            # the cache-fallback ``_rows_to_youtube_releases`` rehydrates
            # ``SyntheticItem.album`` and ``SyntheticItem.albumartist``
            # structurally identical to the fresh path. Without them,
            # cached reads collapsed to ``album=""`` (#15) and
            # ``albumartist=per-track-artist`` (round 2 maintainability-5
            # — the Various Artists case). Both stored at row scope
            # (not per track) since they are album-level facts.
            album_title=album_title,
            album_artist=album_artist,
            yt_tracks=persistable_tracks,
            distances=[
                PersistedDistance(
                    mbid=d.mbid,
                    outcome=d.outcome,
                    distance=d.distance,
                    components=d.components,
                    matched_tracks=d.matched_tracks,
                    total_local_tracks=d.total_local_tracks,
                    total_mb_tracks=d.total_mb_tracks,
                    extra_local_tracks=d.extra_local_tracks,
                    extra_mb_tracks=d.extra_mb_tracks,
                    error_message=d.error_message,
                )
                for d in distances
            ],
        ))

    # Step 11: persist + return.
    pdb.upsert_youtube_album_mapping(rg_id, source_label, persistable_rows)

    # Round 2 P2-4: log + surface the cumulative jitter so operators
    # can see how much of the resolve's wall-clock was anti-throttle
    # pause vs real network work. Attached to ``error_message`` only
    # when non-zero (no jitter on cache-hit / empty-search paths).
    final_error_message = deadline_message
    if cumulative_jitter_seconds > 0:
        jitter_note = (
            f"jitter={cumulative_jitter_seconds:.2f}s cumulative")
        log.info(
            "youtube_album_service: resolve(%s, %s) %s",
            rg_id, source_label, jitter_note,
        )
        if final_error_message is None:
            final_error_message = jitter_note
        else:
            final_error_message = f"{final_error_message}; {jitter_note}"

    return _final(
        outcome="ok",
        release_group_identifier=rg_id,
        source=source_label,
        from_cache=False,
        youtube_releases=youtube_releases,
        error_message=final_error_message,
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

    ``is_orphan=True`` flags the leaf-has-no-rg case (Discogs release
    with no master, legacy MB release without a release group). The
    resolver treats the leaf as its own pseudo-sibling — ``rg_id``
    carries the leaf's own identifier, ``sibling_summaries`` has one
    entry pointing back at it, and the per-pair distance call passes
    ``mb_release_group_id=None`` so ``compute_beets_distance``'s
    cross-RG guardrail is skipped.
    """

    rg_id: Optional[str] = None
    sibling_summaries: list[object] = msgspec.field(
        default_factory=list[object]
    )
    failure_outcome: Optional[str] = None
    is_orphan: bool = False


def _safe_leaf_lookup(
    lookup: _ReleaseOrGroupLookup,
    identifier: str,
) -> Optional[dict[str, object]]:
    """Call a leaf lookup, treating ONLY 404 (and Discogs ValueError) as a miss.

    Real adapters (``web.mb.get_release`` / ``web.discogs.get_release``)
    raise:

    * ``urllib.error.HTTPError(404)`` — release isn't at this leaf. Miss.
    * ``urllib.error.URLError`` (no ``.code``) — transport failure. Outage.
    * ``urllib.error.HTTPError(5xx)`` — mirror error. Outage.
    * ``requests.Timeout`` / ``requests.ConnectionError`` — Outage.
    * ``ValueError`` (Discogs ``int()``) — operator pasted a UUID. Miss.

    Round 2 P1-1: previously every URLError / RequestException was
    swallowed as a miss; the auto-widen then fabricated the rg_id from
    the operator's input. Now only 404 falls through; outages propagate
    to the top-level handler which classifies them as
    ``unresolved_mirror_unavailable`` / ``unresolved_timeout``.
    """
    try:
        return lookup(identifier)
    except _AUTO_WIDEN_MISS_EXCS as exc:
        if _is_leaf_miss(exc):
            log.debug(
                "youtube_album_service: leaf lookup raised %s for %r — treating as miss",
                type(exc).__name__, identifier,
            )
            return None
        raise


def _safe_group_lookup(
    lookup: _ReleaseOrGroupLookup,
    identifier: str,
) -> Optional[dict[str, object]]:
    """Call a release-group / master lookup with the same 404-only tolerance."""
    try:
        return lookup(identifier)
    except _AUTO_WIDEN_MISS_EXCS as exc:
        if _is_leaf_miss(exc):
            log.debug(
                "youtube_album_service: group lookup raised %s for %r — treating as miss",
                type(exc).__name__, identifier,
            )
            return None
        raise


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
        if not isinstance(rg_id, str) or not rg_id:
            # Orphan release: legacy MB row without a release group. Treat
            # the leaf as its own one-element matrix — sibling enumeration
            # isn't possible, but YT search + N×1 scoring still is. See
            # ``_GroupResolution`` docstring.
            return _GroupResolution(
                rg_id=identifier,
                sibling_summaries=[{"id": identifier}],
                is_orphan=True,
            )
        group = _safe_group_lookup(mb_get_release_group_releases, rg_id)
        if not group:
            return _GroupResolution(failure_outcome="not_found")
        return _GroupResolution(
            rg_id=rg_id,
            sibling_summaries=_json_list(group.get("releases")),
        )

    # Leaf miss → treat identifier as RG MBID.
    group = _safe_group_lookup(mb_get_release_group_releases, identifier)
    if not group:
        return _GroupResolution(failure_outcome="not_found")
    return _GroupResolution(
        rg_id=identifier,
        sibling_summaries=_json_list(group.get("releases")),
    )


def _resolve_discogs_group(
    identifier: str,
    discogs_get_release: DiscogsLookup,
    discogs_get_master_releases: DiscogsMasterReleases,
) -> _GroupResolution:
    """Resolve a Discogs identifier to a typed group result.

    Same shape as ``_resolve_mb_group``. Orphan Discogs releases (no
    master record — small labels routinely skip the master step) are
    handled the same way: the leaf becomes its own one-element matrix
    via ``is_orphan=True``. The long-tail archival case these were
    built for explicitly includes orphan pressings.
    """
    leaf = _safe_leaf_lookup(discogs_get_release, identifier)
    if leaf:
        master_id = leaf.get("release_group_id")
        if not isinstance(master_id, str) or not master_id:
            # Orphan Discogs release: no master record. Score against the
            # leaf alone — sibling enumeration via the master is not
            # possible, but YT search + N×1 distance is.
            return _GroupResolution(
                rg_id=identifier,
                sibling_summaries=[{"id": identifier}],
                is_orphan=True,
            )
        group = _safe_group_lookup(discogs_get_master_releases, master_id)
        if not group:
            return _GroupResolution(failure_outcome="not_found")
        return _GroupResolution(
            rg_id=master_id,
            sibling_summaries=_json_list(group.get("releases")),
        )

    group = _safe_group_lookup(discogs_get_master_releases, identifier)
    if not group:
        return _GroupResolution(failure_outcome="not_found")
    return _GroupResolution(
        rg_id=identifier,
        sibling_summaries=_json_list(group.get("releases")),
    )


# ---------------------------------------------------------------------------
# Sibling enumeration + seed-pick heuristic.
# ---------------------------------------------------------------------------


def _extract_sibling_ids(summaries: list[object]) -> list[str]:
    """Return every sibling MBID/release-ID from the group's release summaries."""
    out: list[str] = []
    for s_raw in summaries:
        # ``_json_dict`` already returns ``{}`` for a non-dict entry, so
        # a preceding ``isinstance`` gate would be redundant.
        sid = _json_dict(s_raw).get("id")
        if sid:
            out.append(str(sid))
    return out


def _fetch_mb_siblings(
    summaries: list[object],
    source_label: str,
    mb_get_release: MBLookup,
    discogs_get_release: DiscogsLookup,
) -> list[dict[str, object]]:
    """Resolve each sibling-summary into a full release record.

    Misses (mirror 404 / network error) are skipped silently — the
    matrix's per-pair distance entry will still surface
    ``mb_lookup_failed`` later when ``compute_beets_distance`` re-fetches
    the same MBID. (Per R17, partial mirror coverage shouldn't fail the
    whole resolve.)
    """
    out: list[dict[str, object]] = []
    fetcher = mb_get_release if source_label == "mb" else discogs_get_release
    for summary_raw in summaries:
        # ``_json_dict`` already returns ``{}`` for a non-dict entry, so
        # a preceding ``isinstance`` gate would be redundant.
        sid = _json_dict(summary_raw).get("id")
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


def _pick_mb_seed(siblings: list[dict[str, object]]) -> dict[str, object]:
    """Lowest-year sibling, first-by-id tiebreak."""
    def _sort_key(s: dict[str, object]) -> tuple[int, str]:
        year = s.get("year")
        return (year if isinstance(year, int) else 9999, str(s.get("id") or ""))
    return sorted(siblings, key=_sort_key)[0]


def _build_search_query(seed_release: dict[str, object]) -> str:
    """Form the YT search query from the seed release.

    ``f"{artist_name} {album_title}"`` — the simplest deterministic
    string that pins both axes of the album.
    """
    artist = str(seed_release.get("artist_name") or "").strip()
    title = str(seed_release.get("title") or "").strip()
    return f"{artist} {title}".strip()


def _pick_yt_seed(
    search_results: list[dict[str, Any]],
    mb_seed: dict[str, object],
) -> Optional[str]:
    """Pick the YT search result whose ``(year, trackCount)`` is closest
    to the MB seed. Fall back to the top-ranked result on ties.

    Returns the chosen ``browseId`` or ``None`` if ``search_results`` is
    empty.
    """
    if not search_results:
        return None
    mb_year_raw = mb_seed.get("year")
    mb_year = mb_year_raw if isinstance(mb_year_raw, int) else None
    mb_track_count = len(_json_list(mb_seed.get("tracks")))

    best_idx: Optional[int] = None
    best_score: Optional[tuple[int, int]] = None
    for idx, r in enumerate(search_results):
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
            if r.get("browseId"):
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


def _synthesize_items(album_resp: dict[str, Any]) -> list[SyntheticItem]:
    """Build a ``SyntheticItem`` list from a ``ytmusicapi.get_album`` response.

    Beets's ``distance()`` reads ``title``, ``artist``, ``album``,
    ``albumartist``, ``track``, ``tracktotal``, ``disc``, ``disctotal``,
    ``length``. We populate all of them from the YT response. Missing
    fields fall back to empty / zero — beets skips missing-field
    comparisons rather than penalising them.
    """
    album_title = str(album_resp.get("title") or "")
    album_artists = _json_list(album_resp.get("artists"))
    # ``_json_dict`` already returns ``{}`` for a non-dict entry (and
    # ``.get`` then degrades to the ``or ""`` fallback below), so a
    # preceding ``isinstance`` check on ``album_artists[0]`` would be
    # redundant.
    albumartist = (
        str(_json_dict(album_artists[0]).get("name") or "")
        if album_artists
        else ""
    )
    tracks = _json_list(album_resp.get("tracks"))
    total = len(tracks)

    # Detect 0-indexed payload on the first usable (dict) track only —
    # non-dict entries are skipped, not treated as the "first" track.
    # Without this flag we'd misclassify a 1-indexed payload that
    # legitimately repeats ``trackNumber: 1`` (e.g. duplicate or
    # malformed entries) as 0-indexed.
    zero_indexed = False
    for t_raw in tracks:
        if not _is_dict_like(t_raw):
            continue
        raw_first = _json_dict(t_raw).get("trackNumber")
        if isinstance(raw_first, int) and raw_first == 0:
            zero_indexed = True
        break

    out: list[SyntheticItem] = []
    for idx, t_raw in enumerate(tracks):
        if not _is_dict_like(t_raw):
            continue
        t = _json_dict(t_raw)
        title = str(t.get("title") or "")
        t_artists = _json_list(t.get("artists"))
        artist = (
            str(_json_dict(t_artists[0]).get("name") or "")
            if t_artists
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
    mb_release_group_id: Optional[str],
    distance_fn: DistanceFn,
    pdb: YoutubeResolverDB,
    mb_fetcher: _ReleaseOrGroupLookup,
    deadline_breached: Optional[Callable[[], bool]] = None,
) -> list[ResolvedDistance]:
    """Run ``distance_fn`` for every sibling MBID and collect typed results.

    Each call passes ``items_override=synth`` and the caller-supplied
    ``mb_release_group_id`` so the cross-RG guardrail fires in the
    override path (R17 / U2 contract). ``None`` is passed for orphan
    releases — the guardrail is skipped and the single-element matrix
    flows through. Per-pair failures are preserved as the entry's
    outcome rather than aborting the matrix — including
    ``mb_lookup_failed`` for siblings the local mirror doesn't carry.

    Round 2 P1-3: an optional ``deadline_breached`` callback breaks the
    scoring loop early when the resolver's soft deadline is exhausted,
    so one slow ``distance_fn`` call can't overshoot the budget.
    Remaining unscored siblings are NOT inserted as placeholders — the
    caller sees the partial matrix and the deadline message in
    ``YoutubeAlbumResolverResult.error_message``.
    """
    out: list[ResolvedDistance] = []
    for mbid in sibling_mbids:
        if deadline_breached is not None and deadline_breached():
            log.warning(
                "youtube_album_service: deadline breached after %d/%d "
                "siblings scored; returning partial distance row",
                len(out), len(sibling_mbids),
            )
            break
        if not mbid:
            continue
        try:
            r = distance_fn(
                mbid=mbid,
                items_override=synth_items,
                mb_release_group_id=mb_release_group_id,
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
    rows: list[dict[str, Any]],
) -> list[ResolvedYoutubeRelease]:
    """Deserialize cached DB rows back into typed structs.

    Wire-boundary decode via ``msgspec.convert``: every JSONB row is
    validated against ``PersistedYoutubeRow`` before we touch its
    fields. Malformed JSONB (drifted shape, schema change, manual
    insert) raises ``msgspec.ValidationError`` at this seam rather
    than silently producing a ``SyntheticItem`` with garbage values.

    Album-level fields (``album``, ``albumartist``) are rehydrated
    from the row-level ``album_title`` / ``album_artist`` columns —
    older cached rows (pre-036) fall back to empty strings (for
    ``album``) and to the per-track artist (for ``albumartist``, so
    the rehydrated item still scores against beets). New writes
    always include both fields.
    """
    out: list[ResolvedYoutubeRelease] = []
    for raw_row in rows:
        # ``msgspec.convert`` validates the JSONB shape; bare dicts /
        # decoded JSON are accepted, ``DictRow`` rows from psycopg2 are
        # also accepted via ``dict(row)`` upstream.
        row = msgspec.convert(raw_row, type=PersistedYoutubeRow)
        album_title = row.album_title or ""
        album_artist = row.album_artist or ""
        total_local = len(row.yt_tracks)
        synth_tracks: list[SyntheticItem] = []
        for t in row.yt_tracks:
            artists = t.artists or []
            primary_artist = (
                str(artists[0].get("name") or "") if artists else ""
            )
            # When the row was written pre-036 (no ``album_artist`` column),
            # fall back to the per-track artist so beets can still score.
            # Post-036 rows carry the album-level credit verbatim — for
            # Various Artists rows this preserves "Various" instead of
            # silently substituting the first track's artist.
            effective_albumartist = album_artist or primary_artist
            synth_tracks.append(SyntheticItem(
                title=t.title or "",
                artist=primary_artist,
                album=album_title,
                albumartist=effective_albumartist,
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
            # ``yt_browse_id`` / ``yt_url`` / ``yt_track_count`` are
            # declared required on ``PersistedYoutubeRow`` so
            # ``msgspec.convert`` above rejects malformed JSONB at the
            # boundary; defensive ``or ""`` fallbacks are no longer
            # necessary (round 2 maintainability-2).
            yt_browse_id=row.yt_browse_id,
            yt_audio_playlist_id=row.yt_audio_playlist_id,
            yt_url=row.yt_url,
            year=row.yt_year,
            track_count=row.yt_track_count,
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
