"""MusicBrainz API helpers — shared between pipeline_cli and web server.

All queries hit the MusicBrainz API at MB_API_BASE (public MB by default; the local mirror in production). The pure-metadata
responses are memoized via `cache.memoize_meta()` at 24h TTL so the
web UI can render multiple cards per page without hammering the mirror.

The cache layer intentionally sits here — not at the HTTP routing
level — because route handlers enrich each response with per-user
pipeline/library overlay state (`pipeline_status`, `in_library`, …).
Caching the post-overlay response baked that state into Redis and
leaked stale badges when the pipeline updated Postgres outside the
web UI's POST invalidation paths. See issue #101.
"""

import concurrent.futures
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any, TypedDict

import msgspec

from lib.artist_catalogue import (
    ArtistCatalogueRow,
    ArtistProvenance,
    ArtistStructuralType,
)

# Use the `web.` package-qualified path to keep the web metadata cache
# separate from the pipeline's peer-cache implementation.
from web import cache as _cache
from web.api_bases import PUBLIC_MB_ORIGIN, PUBLIC_MB_WS2_BASE
from web.artist_search import merge_exact_artist_identities

# Default: public MusicBrainz (functional but rate-limited ~1 req/s).
# Production points this at the local mirror via the module's
# services.cratedigger.musicbrainz.apiBase -> config.ini [MusicBrainz]
# api_base -> configure_api_bases_from_runtime_config() at cratedigger-web
# startup (tier-2 plan U6, R13/KTD6; issue #497 dropped the module's
# --mb-api flag in favor of config.ini as the one production source — the
# flag itself survives as a dev-only override). The value includes the
# /ws/2 prefix.
MB_API_BASE = PUBLIC_MB_WS2_BASE
USER_AGENT = "cratedigger-web/1.0"

# A large artist exercises several independent browse families at once.  Keep
# the limit at the HTTP boundary, rather than per caller, so nesting one
# paginator inside another can never make the local MusicBrainz mirror see an
# accidental fan-out.  The key is the mirror origin: development and tests may
# point this module at several mirrors in one process.
_MB_MIRROR_CONCURRENCY = 4
_mb_mirror_semaphores: dict[str, threading.BoundedSemaphore] = {}
_mb_mirror_semaphores_lock = threading.Lock()
_mb_public_next_request_at: dict[str, float] = {}
_PUBLIC_MB_REQUEST_INTERVAL_SECONDS = 1.0
# Named seams keep the real public-MB policy testable without making every
# mock-backed unit test sleep in real time.
_monotonic = time.monotonic
_sleep = time.sleep
# Development benchmark hook. Production leaves it unset; count at the HTTP
# attempt boundary so a retry is represented as the second upstream request.
_on_mirror_request: Callable[[], None] | None = None


class MusicBrainzArtistCatalogueIncomplete(RuntimeError):
    """Raised when a counted browse response cannot conserve its identities."""


def _mirror_origin(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _mirror_concurrency(url: str) -> int:
    """Keep public MusicBrainz serial; LAN/custom mirrors may fan out."""
    hostname = urllib.parse.urlsplit(url).hostname
    if hostname == urllib.parse.urlsplit(PUBLIC_MB_ORIGIN).hostname:
        return 1
    return _MB_MIRROR_CONCURRENCY


def _mirror_semaphore(url: str) -> threading.BoundedSemaphore:
    origin = _mirror_origin(url)
    with _mb_mirror_semaphores_lock:
        semaphore = _mb_mirror_semaphores.get(origin)
        if semaphore is None:
            semaphore = threading.BoundedSemaphore(_mirror_concurrency(url))
            _mb_mirror_semaphores[origin] = semaphore
        return semaphore


def _wait_for_public_musicbrainz(url: str) -> None:
    """Preserve MusicBrainz's public one-request-per-second etiquette."""
    if _mirror_concurrency(url) != 1:
        return
    origin = _mirror_origin(url)
    with _mb_mirror_semaphores_lock:
        now = _monotonic()
        request_at = max(now, _mb_public_next_request_at.get(origin, now))
        _mb_public_next_request_at[origin] = request_at + _PUBLIC_MB_REQUEST_INTERVAL_SECONDS
    _sleep(max(0, request_at - now))

# Canonical Various Artists MBID. Used by the resolver and the browse-tab
# VA short-circuit (web/js/browse.js) to keep VA off the artist-view path
# (the MB artist→release-group endpoint takes ~23s for VA). Single
# declaration site at ``lib/va_identity.py`` — re-exported here so the
# existing ``from web.mb import VA_ARTIST_MBID`` imports keep working.
from lib.va_identity import (
    MB_VA_ARTIST_MBID as VA_ARTIST_MBID,
)
from lib.va_identity import (
    split_va_query,
)


def _quote_mb_identifier(identifier: str) -> str:
    """Encode one MusicBrainz identifier without URL syntax leakage."""
    return urllib.parse.quote(identifier, safe="")


def _get(url: str) -> Any:
    """Fetch and JSON-decode one MB API URL.

    Returns ``Any`` — this is the raw external-JSON boundary; callers
    immediately assign the result to a locally-scoped ``_MB*JSON``
    TypedDict-annotated variable (untyped/unvalidated at runtime, same
    ``.get(..., default)`` tolerance as before) so every downstream field
    access is precisely typed without this module ever validating the
    external response's shape.
    """
    def request_once() -> object:
        # Pace each HTTP attempt, including the retry. A retry is still a
        # request received by public MusicBrainz and must not bypass etiquette.
        _wait_for_public_musicbrainz(url)
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        req.add_header("Connection", "close")
        if _on_mirror_request is not None:
            _on_mirror_request()
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())

    # One slot spans a retry too.  Releasing it between attempts would let a
    # transient mirror failure briefly exceed the promised per-mirror cap.
    with _mirror_semaphore(url):
        try:
            return request_once()
        except urllib.error.URLError:
            # Retry once — MB mirror may have closed a keep-alive connection
            return request_once()


def _parallel_results[Key, Result](
    jobs: dict[Key, Callable[[], Result]], *, max_workers: int,
) -> dict[Key, Result]:
    """Return concurrent results, surfacing one failure without sibling wait."""
    if not jobs:
        return {}
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    futures = {key: executor.submit(job) for key, job in jobs.items()}
    try:
        done, _pending = concurrent.futures.wait(
            futures.values(), return_when=concurrent.futures.FIRST_EXCEPTION,
        )
        # A completed exception wins immediately; do not call the context
        # manager's waiting shutdown path while another mirror request hangs.
        for future in done:
            future.result()
        results = {key: future.result() for key, future in futures.items()}
    except BaseException:
        for future in futures.values():
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)
        return results


def _fetch_browse_pages[MBPage, MBItem](
    fetch_page: Callable[[int, int], MBPage],
    page_total: Callable[[MBPage], int],
    page_items: Callable[[MBPage], list[MBItem]],
    item_id: Callable[[MBItem], str],
) -> list[MBPage]:
    """Fetch a counted MB browse endpoint in stable, identity-safe order.

    Every 100-row segment is filled with the exact remaining limit.  A short
    page is therefore followed at ``offset + len(page)`` with only that
    segment's remainder, never skipped by a fixed ``offset += 100`` jump.
    """
    first = fetch_page(0, 100)
    total = page_total(first)
    if total < 0:
        raise MusicBrainzArtistCatalogueIncomplete("negative browse total")
    if total == 0 and page_items(first):
        raise MusicBrainzArtistCatalogueIncomplete("zero-count browse returned identities")

    def fill_segment(start: int, initial: MBPage | None = None) -> list[MBPage]:
        end = min(start + 100, total)
        offset = start
        pages: list[MBPage] = []
        page = initial
        while offset < end:
            current = page if page is not None else fetch_page(offset, end - offset)
            page = None
            items = page_items(current)
            if not items:
                raise MusicBrainzArtistCatalogueIncomplete(
                    f"browse ended at {offset} before declared total {total}",
                )
            if len(items) > end - offset:
                raise MusicBrainzArtistCatalogueIncomplete("browse exceeded its requested segment")
            pages.append(current)
            offset += len(items)
        return pages

    starts = range(0, total, 100)
    jobs = {
        start: (lambda start=start: fill_segment(start, first if start == 0 else None))
        for start in starts
    }
    segments = _parallel_results(jobs, max_workers=_MB_MIRROR_CONCURRENCY)
    pages = [page for start in starts for page in segments[start]]
    ids = [item_id(item) for page in pages for item in page_items(page)]
    if len(ids) != total or any(not identity for identity in ids) or len(ids) != len(set(ids)):
        raise MusicBrainzArtistCatalogueIncomplete(
            f"browse did not conserve {total} unique identities",
        )
    return pages


class _MBArtistRefJSON(TypedDict, total=False):
    """MB nested ``artist`` object inside an artist-credit entry or relation."""
    id: str
    name: str
    disambiguation: str


_EMPTY_MB_ARTIST_REF: _MBArtistRefJSON = {}
"""Typed empty fallback for "no artist credited" — a reusable NAMED
constant rather than an inline ``{}`` literal so ``.get(key, default)`` /
ternary-fallback call sites resolve the exact-TypedDict ``get()`` overload
instead of unifying with a fresh ``dict[Unknown, Unknown]`` literal."""


class _MBArtistCreditNameJSON(TypedDict, total=False):
    """One MB ``artist-credit`` array entry."""
    name: str
    artist: _MBArtistRefJSON


_MBReleaseGroupRefJSON = TypedDict("_MBReleaseGroupRefJSON", {
    "id": str,
    "title": str | None,
    "primary-type": str | None,
    "secondary-types": list[str] | None,
    "first-release-date": str | None,
    "artist-credit": list[_MBArtistCreditNameJSON],
}, total=False)
"""Slice of a MusicBrainz ``release-group`` object — nested inside a release
(``release-group`` in a release lookup) or fetched directly (a
``/release-group?artist=`` browse hit, or the top-level shape of
``/release-group/<mbid>?inc=artist-credits``). Different endpoints
populate different subsets of these fields; ``total=False`` covers that."""


_MBReleaseSearchHitJSON = TypedDict("_MBReleaseSearchHitJSON", {
    "id": str,
    "title": str,
    "date": str,
    "score": int,
    "release-group": _MBReleaseGroupRefJSON,
    "artist-credit": list[_MBArtistCreditNameJSON],
}, total=False)
"""Slice of one ``/release?query=`` search-result hit."""


class _MBReleaseSearchResponseJSON(TypedDict, total=False):
    """Slice of the ``/release?query=`` search response."""
    releases: list[_MBReleaseSearchHitJSON]


class _MBArtistSearchHitJSON(TypedDict, total=False):
    """Slice of one ``/artist?query=`` search-result hit."""
    id: str
    name: str
    disambiguation: str
    score: int


class _MBArtistSearchResponseJSON(TypedDict, total=False):
    """Slice of the ``/artist?query=`` search response."""
    artists: list[_MBArtistSearchHitJSON]


_MBArtistRelationJSON = TypedDict("_MBArtistRelationJSON", {
    "type": str,
    "direction": str,
    "artist": _MBArtistRefJSON,
}, total=False)
"""Slice of one entry in an ``/artist/<mbid>?inc=artist-rels`` response."""


class _MBArtistDetailJSON(TypedDict, total=False):
    """Slice of the ``/artist/<mbid>?inc=artist-rels`` response."""
    relations: list[_MBArtistRelationJSON]


_MBReleaseGroupBrowseResponseJSON = TypedDict(
    "_MBReleaseGroupBrowseResponseJSON", {
        "release-groups": list[_MBReleaseGroupRefJSON],
        "release-group-count": int,
    }, total=False)
"""Slice of the ``/release-group?artist=`` browse response."""


_MBReleaseBrowseHitJSON = TypedDict("_MBReleaseBrowseHitJSON", {
    "id": str,
    "release-group": _MBReleaseGroupRefJSON,
    "status": str,
}, total=False)
"""Slice of one ``/release?artist=`` / ``/release?track_artist=`` hit."""


_MBReleaseBrowseResponseJSON = TypedDict("_MBReleaseBrowseResponseJSON", {
    "releases": list[_MBReleaseBrowseHitJSON],
    "release-count": int,
}, total=False)
"""Slice of the ``/release?artist=`` / ``/release?track_artist=`` browse
response."""


_MBMediaSummaryJSON = TypedDict("_MBMediaSummaryJSON", {
    "format": str,
    "track-count": int,
}, total=False)
"""Slice of one ``media`` entry in a release-group's release summary."""


_MBReleaseGroupReleaseSummaryJSON = TypedDict(
    "_MBReleaseGroupReleaseSummaryJSON", {
        "id": str,
        "title": str,
        "date": str,
        "country": str,
        "status": str,
        "media": list[_MBMediaSummaryJSON],
    }, total=False)
"""Slice of one release summary inside ``/release?release-group=``."""


_MBReleaseGroupReleasesResponseJSON = TypedDict(
    "_MBReleaseGroupReleasesResponseJSON", {
        "releases": list[_MBReleaseGroupReleaseSummaryJSON],
        "release-count": int,
    }, total=False)
"""Slice of the ``/release?release-group=`` browse response."""


class _MBRecordingRefJSON(TypedDict, total=False):
    """Slice of a MusicBrainz ``recording`` object this module reads.

    ``id`` isn't read by anything in this module today but is declared here
    (issue #784) so ``lib.artist_releases`` — which needs the recording id
    for cross-release-group coverage analysis — can reuse this exact shape
    via a ``TYPE_CHECKING`` import instead of maintaining a parallel type.
    """
    id: str
    length: int


_MBTrackFullJSON = TypedDict("_MBTrackFullJSON", {
    "position": int,
    "number": int,
    "title": str,
    "length": int,
    "recording": _MBRecordingRefJSON,
}, total=False)
"""Slice of a full-release-lookup ``track`` object."""


_MBPregapJSON = TypedDict("_MBPregapJSON", {
    "title": str,
    "length": int,
    "recording": _MBRecordingRefJSON,
}, total=False)
"""Slice of a full-release-lookup medium's ``pregap`` object."""


_MBFullMediumJSON = TypedDict("_MBFullMediumJSON", {
    "position": int,
    "format": str,
    "pregap": _MBPregapJSON,
    "tracks": list[_MBTrackFullJSON],
}, total=False)
"""Slice of a full-release-lookup ``medium`` object. ``format`` isn't read
by anything in this module today but is declared here (issue #784) so
``lib.artist_releases`` can reuse this exact shape — see
``_MBRecordingRefJSON``'s docstring."""


_MBReleaseFullJSON = TypedDict("_MBReleaseFullJSON", {
    "id": str,
    "title": str,
    "date": str,
    "country": str,
    "status": str,
    "artist-credit": list[_MBArtistCreditNameJSON],
    "release-group": _MBReleaseGroupRefJSON,
    "media": list[_MBFullMediumJSON],
}, total=False)
"""Slice of the ``/release/<mbid>?inc=recordings+artist-credits+media+
release-groups+labels`` response this module reads. Untyped beyond this
slice (structural-only, no runtime validation) — mirrors the pre-existing
``.get(..., default)`` tolerance for an external API response, not a
wire-boundary Struct. Consumers needing wider fields (label-info,
per-track artist-credit) read the ``dict[str, object]`` returned by
``get_release_raw`` directly instead of this internal slice."""


_MBArtistReleasesWithRecordingsResponseJSON = TypedDict(
    "_MBArtistReleasesWithRecordingsResponseJSON", {
        "releases": list[_MBReleaseFullJSON],
        "release-count": int,
    }, total=False)
"""Slice of the ``/release?artist=...&inc=recordings+media+release-groups``
response — full per-release shape (unlike ``_MBReleaseBrowseResponseJSON``,
whose hits carry only ``id``/``release-group``/``status``)."""


def search_release_groups(query: str) -> list[dict[str, object]]:
    """Search releases by title, deduplicate by release group. Returns list with artist info.

    Uses /release search (not /release-group) because the local MB mirror's
    search index only covers releases.

    "Various Artists" tokens in the query are rewritten to a Lucene
    ``arid:`` pin on the canonical VA artist (#199) — as title terms they
    only match albums literally titled "Various Artists". A VA-only query
    (no title remainder) keeps the raw passthrough: an arid-only pin
    would return 25 arbitrary VA releases, no more useful than today.
    """
    remainder, is_va = split_va_query(query)
    if is_va and remainder:
        query = f"arid:{VA_ARTIST_MBID} AND ({remainder})"

    def _fetch() -> list[dict[str, object]]:
        q = urllib.parse.quote(query, safe="")
        data: _MBReleaseSearchResponseJSON = _get(
            f"{MB_API_BASE}/release?query={q}&fmt=json&limit=25")
        seen_rg: set[str] = set()
        results: list[dict[str, object]] = []
        for r in data.get("releases", []):
            rg = r.get("release-group", {})
            rg_id = rg.get("id", "")
            if not rg_id or rg_id in seen_rg:
                continue
            seen_rg.add(rg_id)
            artist_credit = r.get("artist-credit", [{}])
            artist = (
                artist_credit[0].get("artist", _EMPTY_MB_ARTIST_REF)
                if artist_credit else _EMPTY_MB_ARTIST_REF
            )
            results.append({
                "id": rg_id,
                "title": rg.get("title", r.get("title", "")),
                "primary_type": rg.get("primary-type", ""),
                "first_release_date": rg.get("first-release-date", r.get("date", "")),
                "artist_id": artist.get("id", ""),
                "artist_name": artist.get("name", ""),
                "artist_disambiguation": artist.get("disambiguation", ""),
                "score": r.get("score", 0),
            })
        return results

    return _cache.memoize_meta(f"mb:search:release_groups:{query}", _fetch)


def _artist_search_hit(
    artist: _MBArtistRefJSON, *, score: int,
) -> dict[str, object]:
    return {
        "id": artist.get("id", ""),
        "name": artist.get("name", ""),
        "disambiguation": artist.get("disambiguation", ""),
        "score": score,
    }


def _related_artist_identity_hits(
    artist_id: str, *, score: int,
) -> list[dict[str, object]]:
    """Resolve canonical MusicBrainz ``is person`` identity siblings."""
    detail: _MBArtistDetailJSON = _get(
        f"{MB_API_BASE}/artist/{_quote_mb_identifier(artist_id)}?inc=artist-rels&fmt=json"
    )
    relations = detail.get("relations", [])
    identity_artists: list[_MBArtistRefJSON] = []

    # A persona such as Four Tet points backward to the underlying person.
    person = next((
        rel.get("artist")
        for rel in relations
        if rel.get("type") == "is person"
        and rel.get("direction") == "backward"
        and rel.get("artist")
    ), None)
    if person:
        identity_artists.append(person)
        detail = _get(
            f"{MB_API_BASE}/artist/{_quote_mb_identifier(person.get('id', ''))}?inc=artist-rels&fmt=json"
        )
        relations = detail.get("relations", [])

    # The person entity points forward to each separately catalogued persona.
    identity_artists.extend(
        forward_artist
        for rel in relations
        if rel.get("type") == "is person"
        and rel.get("direction") == "forward"
        for forward_artist in [rel.get("artist")]
        if forward_artist
    )
    return [
        _artist_search_hit(artist, score=score)
        for artist in identity_artists
    ]


def search_artists(query: str) -> list[dict[str, object]]:
    """Search for artists by name. Returns list of {id, name, disambiguation, score}."""
    def _fetch() -> list[dict[str, object]]:
        q = urllib.parse.quote(query, safe="")
        data: _MBArtistSearchResponseJSON = _get(
            f"{MB_API_BASE}/artist?query={q}&fmt=json&limit=20")
        results = [
            _artist_search_hit(a, score=a.get("score", 0))
            for a in data.get("artists", [])
        ]
        exact = next((
            row for row in results
            if str(row["name"]).casefold() == query.casefold()
        ), None)
        if exact is None:
            return results
        exact_id = str(exact["id"])
        exact_score = exact["score"]
        exact_score = exact_score if isinstance(exact_score, int) else 0
        try:
            related = _related_artist_identity_hits(
                exact_id, score=max(0, exact_score - 1),
            )
        except (urllib.error.HTTPError, urllib.error.URLError):
            return results
        return merge_exact_artist_identities(
            results, exact_id=exact_id, related=related,
        )

    return _cache.memoize_meta(f"mb:search:artists:v2:{query}", _fetch)


def _normalize_artist_release_group(
    rg: _MBReleaseGroupRefJSON,
    *,
    is_appearance: bool,
) -> ArtistCatalogueRow:
    """Shape direct and track-appearance MB rows into one artist-page contract."""
    ac = rg.get("artist-credit", [])
    credit_name = " / ".join(a.get("name", "?") for a in ac) if ac else ""
    primary_artist_id = (
        ac[0].get("artist", _EMPTY_MB_ARTIST_REF).get("id") if ac else None
    )
    # MusicBrainz represents an unclassified release group with JSON null,
    # not only by omitting the field. The shared catalogue contract keeps
    # display text non-null and carries structural knowledge separately.
    primary_type = rg.get("primary-type") or ""
    _structural: dict[str, ArtistStructuralType] = {
        "Album": "Album", "EP": "EP", "Single": "Single",
    }
    primary_types: list[ArtistStructuralType] = []
    structural_type = _structural.get(primary_type)
    if structural_type is not None:
        primary_types.append(structural_type)
    return ArtistCatalogueRow(
        id=rg.get("id", ""),
        title=rg.get("title") or "",
        type=primary_type,
        source="mb",
        identity_kind="work",
        primary_types=primary_types,
        secondary_types=rg.get("secondary-types") or [],
        format_qualifiers=[],
        # Release status is unioned set-wise inside get_artist_release_groups
        # before rows leave this adapter.
        provenance=[],
        first_release_date=rg.get("first-release-date") or "",
        artist_credit=credit_name,
        primary_artist_id=primary_artist_id or "",
        is_appearance=is_appearance,
    )


def get_artist_release_groups(artist_mbid: str) -> list[ArtistCatalogueRow]:
    """Get directly credited release groups plus track-level appearances.

    MusicBrainz has no combined artist-discography endpoint. Direct work comes
    from the release-group artist browse; VA compilations and guest spots come
    from the release ``track_artist`` browse. Release status evidence is
    projected here, not in the route: both direct-artist and track-artist
    release browses contribute to a per-release-group union. Direct rows win
    identity deduplication so a release group is never downgraded merely
    because another pressing also contains an appearance.
    """
    def _fetch() -> list[ArtistCatalogueRow]:
        entries: dict[str, ArtistCatalogueRow] = {}
        provenance_by_rg: dict[str, set[ArtistProvenance]] = {}

        def collect_release_provenance(release: _MBReleaseBrowseHitJSON) -> None:
            rg = release.get("release-group")
            if not isinstance(rg, dict):
                return
            rg_id = rg.get("id")
            if not isinstance(rg_id, str) or not rg_id:
                return
            status = release.get("status")
            provenance: ArtistProvenance | None = None
            if status == "Official":
                provenance = "ordinary"
            elif status == "Promotion":
                provenance = "promo"
            elif status == "Bootleg":
                provenance = "unofficial"
            if provenance is not None:
                provenance_by_rg.setdefault(rg_id, set()).add(provenance)

        def fetch_release_groups(
            offset: int, limit: int,
        ) -> _MBReleaseGroupBrowseResponseJSON:
            return _get(
                f"{MB_API_BASE}/release-group?artist={_quote_mb_identifier(artist_mbid)}"
                f"&inc=artist-credits&fmt=json&limit={limit}&offset={offset}"
            )

        def fetch_direct_releases(
            offset: int, limit: int,
        ) -> _MBReleaseBrowseResponseJSON:
            return _get(
                f"{MB_API_BASE}/release?artist={_quote_mb_identifier(artist_mbid)}"
                f"&inc=release-groups&fmt=json&limit={limit}&offset={offset}"
            )

        def fetch_track_appearances(
            offset: int, limit: int,
        ) -> _MBReleaseBrowseResponseJSON:
            return _get(
                f"{MB_API_BASE}/release?track_artist={_quote_mb_identifier(artist_mbid)}"
                "&inc=release-groups+artist-credits"
                f"&fmt=json&limit={limit}&offset={offset}"
            )

        # The three families are independent.  Each helper's remaining pages
        # fan out after its own first page, and _get keeps their combined load
        # within one mirror-wide budget.
        families = _parallel_results({
            "release_groups": lambda: _fetch_browse_pages(
                fetch_release_groups,
                lambda page: page.get("release-group-count", 0),
                lambda page: page.get("release-groups", []),
                lambda item: item.get("id", ""),
            ),
            "direct_releases": lambda: _fetch_browse_pages(
                fetch_direct_releases,
                lambda page: page.get("release-count", 0),
                lambda page: page.get("releases", []),
                lambda item: item.get("id", ""),
            ),
            "track_appearances": lambda: _fetch_browse_pages(
                fetch_track_appearances,
                lambda page: page.get("release-count", 0),
                lambda page: page.get("releases", []),
                lambda item: item.get("id", ""),
            ),
        }, max_workers=3)
        release_group_pages = families["release_groups"]
        direct_release_pages = families["direct_releases"]
        track_appearance_pages = families["track_appearances"]

        for data in release_group_pages:
            for rg in data.get("release-groups", []):
                entry = _normalize_artist_release_group(
                    rg, is_appearance=False,
                )
                entries.setdefault(entry.id, entry)

        # A release group browse carries no child release statuses. Fetch the
        # directly credited releases without a status filter so mixed
        # Official/Promotion/Bootleg evidence survives as a set.
        for release_data in direct_release_pages:
            for release in release_data.get("releases", []):
                collect_release_provenance(release)

        for track_data in track_appearance_pages:
            for release in track_data.get("releases", []):
                collect_release_provenance(release)
                rg = release.get("release-group")
                if rg is None or not rg.get("id"):
                    continue
                entry = _normalize_artist_release_group(
                    rg, is_appearance=True,
                )
                entries.setdefault(entry.id, entry)

        for rg_id, entry in entries.items():
            entry.provenance = sorted(provenance_by_rg.get(rg_id, set()))

        rows = sorted(
            entries.values(),
            key=lambda row: (
                row.first_release_date,
                row.id,
            ),
        )
        return msgspec.to_builtins(rows)

    cached = _cache.memoize_meta(
        f"mb:artist:{artist_mbid}:release_groups:v5", _fetch,
    )
    return msgspec.convert(cached, type=list[ArtistCatalogueRow])


def get_release_group(rg_mbid: str) -> dict[str, object]:
    """Get release-group metadata + primary artist credit.

    Distinct from `get_release_group_releases` (which paginates child
    releases). The resolver (`web/routes/browse.py:resolve_id`) needs
    just the parent group's metadata + artist to render the artist-view
    drop-in target.
    """
    def _fetch() -> dict[str, object]:
        data: _MBReleaseGroupRefJSON = _get(
            f"{MB_API_BASE}/release-group/{_quote_mb_identifier(rg_mbid)}?inc=artist-credits&fmt=json")
        ac = data.get("artist-credit", [{}])
        artist = (
            ac[0].get("artist", _EMPTY_MB_ARTIST_REF)
            if ac else _EMPTY_MB_ARTIST_REF
        )
        return {
            "id": data.get("id", ""),
            "title": data.get("title", ""),
            "type": data.get("primary-type", ""),
            "first_release_date": data.get("first-release-date", ""),
            "artist_id": artist.get("id", ""),
            "artist_name": artist.get("name", ""),
        }

    return _cache.memoize_meta(f"mb:release-group:{rg_mbid}:meta", _fetch)


def get_release_group_year(rg_mbid: str) -> int | None:
    """Return the release-group's first-release year as an int, or None.

    Used by the U3/U4 release-group-year backfill + enqueue path. The
    MB ``/release-group/<mbid>`` endpoint returns ``first-release-date``
    directly (verified against the local mirror at 2026-05-19), so a
    single fetch is enough — no need to paginate child releases and
    derive ``min(release.date)``.

    Returns ``None`` only when the release-group record exists but
    carries no parseable year. ``urllib.error.HTTPError(code=404)``
    propagates so the resolver service can disambiguate "MBID does not
    exist" from "exists but missing year" — the former routes to
    ``unresolved_404`` (sticky), the latter to
    ``unresolved_field_missing_upstream``. Other HTTPErrors and
    network-style errors propagate too; callers classify them via
    ``lib.field_resolver_service._classify_lookup_exception``.
    """
    def _fetch() -> int | None:
        data: _MBReleaseGroupRefJSON = _get(
            f"{MB_API_BASE}/release-group/{_quote_mb_identifier(rg_mbid)}?fmt=json")
        from lib.util import parse_mb_first_release_year
        return parse_mb_first_release_year(dict(data))

    return _cache.memoize_meta(
        f"mb:release-group:{rg_mbid}:year", _fetch)


def get_release_group_releases(rg_mbid: str) -> dict[str, object]:
    """Get all releases for a release group. Returns list of release summaries."""
    def _fetch() -> dict[str, object]:
        # First get the release group metadata
        rg_data: _MBReleaseGroupRefJSON = _get(
            f"{MB_API_BASE}/release-group/{_quote_mb_identifier(rg_mbid)}?fmt=json")

        # Then browse all releases (paginated — the lookup endpoint caps at 25)
        releases: list[dict[str, object]] = []
        offset = 0
        while True:
            data: _MBReleaseGroupReleasesResponseJSON = _get(
                f"{MB_API_BASE}/release?release-group={_quote_mb_identifier(rg_mbid)}"
                f"&inc=media&fmt=json&limit=100&offset={offset}"
            )
            for r in data.get("releases", []):
                track_count = sum(m.get("track-count", 0) for m in r.get("media", []))
                formats = [(m.get("format") or "?") for m in r.get("media", [])]
                releases.append({
                    "id": r.get("id", ""),
                    "title": r.get("title", ""),
                    "date": r.get("date", ""),
                    "country": r.get("country", ""),
                    "status": r.get("status", ""),
                    "track_count": track_count,
                    "format": ", ".join(formats) if formats else "?",
                    "media_count": len(r.get("media", [])),
                })
            total = data.get("release-count", 0)
            offset += 100
            if offset >= total:
                break

        return {
            "title": rg_data.get("title", ""),
            "type": rg_data.get("primary-type", ""),
            "releases": releases,
        }

    return _cache.memoize_meta(f"mb:release-group:{rg_mbid}:releases", _fetch)


def _fetch_release_raw(
    release_mbid: str, *, fresh: bool = False,
) -> _MBReleaseFullJSON:
    """Shared fetch+cache path for ``get_release_raw`` and ``get_release``.

    One network/cache round trip regardless of which shape the caller
    wants: ``get_release_raw`` widens the result to ``dict[str, object]``
    for external raw-field consumers; ``get_release`` (via
    ``_strip_release``) consumes this module's internal typed slice
    directly. Never duplicate this fetch — add a second caller instead.
    """
    def _fetch() -> _MBReleaseFullJSON:
        return _get(
            f"{MB_API_BASE}/release/{_quote_mb_identifier(release_mbid)}"
            f"?inc=recordings+artist-credits+media+release-groups+labels&fmt=json"
        )
    return _cache.memoize_meta(
        f"mb:release_raw:{release_mbid}", _fetch, fresh=fresh)


def get_release_raw(
    release_mbid: str, *, fresh: bool = False,
) -> dict[str, object]:
    """Raw MB release JSON with the full inc clause preserved.

    Returned shape is the literal MB API response — `media[]`,
    `artist-credit[]` (album and per-track), `release-group`,
    `label-info`, etc. Cached at its own key so multiple consumers
    (resolver service + frontend stripping) share a single
    cache+network round trip.

    `fresh=True` bypasses the cache.

    Consumers that need a slimmed shape (frontend rendering, pipeline
    DB inserts) call `get_release` which strips this via
    `_strip_release`. Consumers that need raw fields (the field
    resolver service for label-info / per-track artist-credit /
    release-group primary-type) call this directly — the return type here
    is a plain ``dict[str, object]`` (not the narrower ``_MBReleaseFullJSON``
    this module uses internally) precisely so those wider fields stay
    readable via ``.get()`` without this module's internal slice gating them.
    """
    return dict(_fetch_release_raw(release_mbid, fresh=fresh))


def _strip_release(data: _MBReleaseFullJSON) -> dict[str, object]:
    """Slim a raw MB release JSON down to the shape the frontend +
    pipeline DB inserts want. Pure function over `data`."""
    artist_credit = data.get("artist-credit", [{}])
    artist_name = artist_credit[0].get("name", "Unknown") if artist_credit else "Unknown"
    artist_id = (artist_credit[0].get("artist", {}).get("id") if artist_credit else None)
    rg_id = (data.get("release-group") or {}).get("id")

    tracks: list[dict[str, object]] = []
    for medium in data.get("media", []):
        disc = medium.get("position", 1)
        if "pregap" in medium:
            pg = medium["pregap"]
            length_ms = pg.get("length") or (pg.get("recording") or {}).get("length")
            tracks.append({
                "disc_number": disc,
                "track_number": 0,
                "title": pg.get("title", ""),
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })
        for track in medium.get("tracks", []):
            length_ms = track.get("length") or (track.get("recording") or {}).get("length")
            tracks.append({
                "disc_number": disc,
                "track_number": track.get("position", track.get("number", 0)),
                "title": track.get("title", ""),
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })

    year = None
    release_date = data.get("date")
    if release_date:
        try:
            year = int(release_date[:4])
        except (ValueError, IndexError):
            pass

    return {
        "id": data.get("id", ""),
        "title": data.get("title", ""),
        "artist_name": artist_name,
        "artist_id": artist_id,
        "release_group_id": rg_id,
        "date": data.get("date", ""),
        "year": year,
        "country": data.get("country", ""),
        "status": data.get("status", ""),
        "tracks": tracks,
    }


def get_release(
    release_mbid: str, *, fresh: bool = False,
) -> dict[str, object]:
    """Get full release details with tracks (slimmed shape).

    `fresh=True` bypasses the cache. Used by POST handlers in
    `web/routes/pipeline.py` that persist this metadata into the
    pipeline DB — a 24h cache hit would silently write stale
    artist/title/track data into `album_requests` / `request_tracks`.

    Built on top of the shared ``_fetch_release_raw`` fetch+cache path so
    the raw MB JSON is the single cached truth; this just re-derives the
    slim shape per call. The re-derivation is a pure traversal, ~microseconds.
    """
    raw = _fetch_release_raw(release_mbid, fresh=fresh)
    return _strip_release(raw)


def get_artist_name(artist_mbid: str) -> str:
    """Look up an artist's name by MBID."""
    def _fetch() -> str:
        data: _MBArtistRefJSON = _get(
            f"{MB_API_BASE}/artist/{_quote_mb_identifier(artist_mbid)}?fmt=json")
        return data.get("name", "")

    return _cache.memoize_meta(f"mb:artist:{artist_mbid}:name", _fetch)


def assert_exact_release_id_order(
    expected_ids: list[str], releases: list[_MBReleaseFullJSON],
) -> None:
    """Fail closed unless detailed pagination preserves canonical identity/order.

    The no-``inc`` artist browse is the direct-release identity authority.
    Nested recording pages are only enrichment: changing their page limit can
    alter membership, so they must not be allowed to add, lose, duplicate, or
    reorder a release.
    """
    actual_ids = [release.get("id", "") for release in releases]
    if actual_ids != expected_ids:
        raise MusicBrainzArtistCatalogueIncomplete(
            "recording browse did not conserve canonical direct-release IDs: "
            f"expected {len(expected_ids)}, got {len(actual_ids)}"
        )


def get_artist_releases_with_recordings(
    artist_mbid: str,
) -> list[_MBReleaseFullJSON]:
    """Paginated fetch of all releases for an artist with recordings and release-group info.

    Returns raw MB release dicts with media[].tracks[].recording and release-group fields.
    """
    def _fetch() -> list[_MBReleaseFullJSON]:
        def fetch_canonical(
            offset: int, limit: int,
        ) -> _MBReleaseBrowseResponseJSON:
            return _get(
                f"{MB_API_BASE}/release?artist={_quote_mb_identifier(artist_mbid)}"
                f"&fmt=json&limit={limit}&offset={offset}"
            )

        canonical_pages = _fetch_browse_pages(
            fetch_canonical, lambda page: page.get("release-count", 0),
            lambda page: page.get("releases", []),
            lambda item: item.get("id", ""),
        )
        canonical_ids = [
            release.get("id", "")
            for page in canonical_pages
            for release in page.get("releases", [])
        ]
        if len(canonical_ids) != len(set(canonical_ids)):
            raise MusicBrainzArtistCatalogueIncomplete(
                "canonical direct-release browse contained duplicate IDs",
            )

        def fetch_segment(start: int) -> list[_MBReleaseFullJSON]:
            """Fill one fixed canonical offset segment using its exact remainder.

            MusicBrainz may return fewer releases than requested when media and
            recordings are included.  Advancing by a short page with the old
            *100* limit causes an overlap; reducing the next request to the
            exact remainder keeps the segment's offset semantics stable.
            """
            end = min(start + 100, len(canonical_ids))
            offset = start
            segment: list[_MBReleaseFullJSON] = []
            while offset < end:
                data: _MBArtistReleasesWithRecordingsResponseJSON = _get(
                    f"{MB_API_BASE}/release?artist={_quote_mb_identifier(artist_mbid)}"
                    "&inc=recordings+media+release-groups&fmt=json"
                    f"&limit={end - offset}&offset={offset}"
                )
                page = data.get("releases", [])
                if not page:
                    break
                segment.extend(page)
                offset += len(page)
            return segment

        starts = range(0, len(canonical_ids), 100)
        segments = _parallel_results(
            {start: (lambda start=start: fetch_segment(start)) for start in starts},
            max_workers=_MB_MIRROR_CONCURRENCY,
        )
        detailed_pages = [
            release
            for start in starts
            for release in segments[start]
        ]

        canonical_id_set = set(canonical_ids)
        by_id: dict[str, _MBReleaseFullJSON] = {}
        for release in detailed_pages:
            release_id = release.get("id", "")
            if release_id in canonical_id_set:
                by_id.setdefault(release_id, release)

        missing_ids = [release_id for release_id in canonical_ids if release_id not in by_id]
        if missing_ids:
            # A mirror may still surface an overlap across two fixed segments.
            # Completeness outranks the fast path: fetch only those canonical
            # leaves, still under the shared mirror cap, rather than silently
            # dropping a pressing or guessing from an adjacent row.
            def fetch_missing(release_id: str) -> _MBReleaseFullJSON:
                return _get(
                    f"{MB_API_BASE}/release/{_quote_mb_identifier(release_id)}"
                    "?inc=recordings+media+release-groups&fmt=json",
                )

            repairs = _parallel_results(
                {release_id: (lambda release_id=release_id: fetch_missing(release_id))
                 for release_id in missing_ids},
                max_workers=_MB_MIRROR_CONCURRENCY,
            )
            for release_id in missing_ids:
                repaired = repairs[release_id]
                if repaired.get("id", "") != release_id:
                    raise MusicBrainzArtistCatalogueIncomplete(
                        "recording repair returned a different release ID",
                    )
                by_id[release_id] = repaired
        releases = [by_id[release_id] for release_id in canonical_ids]
        assert_exact_release_id_order(canonical_ids, releases)
        return releases

    cached = _cache.memoize_meta(
        f"mb:artist:{artist_mbid}:releases_with_recordings:v2", _fetch)
    return [{**item} for item in cached]
