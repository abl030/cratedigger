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
from typing import Any, Protocol, TypedDict

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

    Returns ``Any`` — this is the raw external-JSON boundary. Callers that
    need real validation immediately decode the result via
    ``msgspec.convert(raw, type=...)`` into one of the strict wire Structs
    below; ``get_release_raw`` is the one deliberate exception (see its
    docstring) that keeps the untouched ``dict[str, object]`` for consumers
    reading fields wider than any narrow Struct this module declares.
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


# ── Wire-boundary Structs (msgspec) ──────────────────────────────────────
#
# Strict decode of every general-purpose MB endpoint this module calls
# (issue #1355 item 5). ``rename="kebab"`` maps a snake_case Python field
# to MB's kebab-case JSON key (``artist_credit`` -> ``artist-credit``,
# ``first_release_date`` -> ``first-release-date``, ...) without a
# per-field ``msgspec.field(name=...)``. Every field carries a default
# (mirroring the previous TypedDicts' ``total=False``) so an ``inc=``
# clause that omits a field, or an endpoint returning a bare ``{}`` (as
# the URL-quoting seam tests do), still decodes; a field MB can send as
# JSON ``null`` (proven against live mirror samples and the existing
# ``test_null_primary_type_normalizes_to_empty_structural_evidence`` /
# ``test_unknown_or_null_release_status_does_not_become_unofficial``
# fixtures) is typed ``X | None`` rather than given a non-None default,
# so an explicit null is never confused with a genuinely-absent key.
#
# ``get_release_raw`` deliberately stays OUTSIDE this boundary: it hands
# real callers (``lib/field_resolver_service.py``'s label-info and
# per-track ``artist-credit``/``joinphrase`` reads, ``lib/
# library_completeness.py``'s ``recording.video`` and ``pregap`` reads,
# ``web/routes/pipeline_mutations.py``'s VA Rule 2 nested
# ``release-group`` primary-type read) fields wider than any narrow slice
# this module declares — narrowing that contract to a Struct would
# silently drop fields those callers need. See its own docstring.


class _MBArtistRef(msgspec.Struct, rename="kebab"):
    """Strict slice of an MB nested ``artist`` object."""
    id: str = ""
    name: str = ""
    disambiguation: str = ""


_EMPTY_MB_ARTIST_REF = _MBArtistRef()
"""Reusable empty fallback for "no artist credited"."""


class _MBArtistCreditName(msgspec.Struct, rename="kebab"):
    """One MB ``artist-credit`` array entry."""
    name: str = ""
    artist: _MBArtistRef = msgspec.field(default_factory=_MBArtistRef)


class _MBReleaseGroupRef(msgspec.Struct, rename="kebab"):
    """Slice of a MusicBrainz ``release-group`` object — nested inside a
    release (``release-group`` in a release lookup) or fetched directly (a
    ``/release-group?artist=`` browse hit, or the top-level shape of
    ``/release-group/<mbid>?inc=artist-credits``). Different endpoints
    populate different subsets of these fields."""
    id: str = ""
    title: str | None = None
    primary_type: str | None = None
    secondary_types: list[str] | None = None
    first_release_date: str | None = None
    artist_credit: list[_MBArtistCreditName] = msgspec.field(default_factory=list[_MBArtistCreditName])


class _MBReleaseSearchHit(msgspec.Struct, rename="kebab"):
    """Slice of one ``/release?query=`` search-result hit."""
    id: str = ""
    title: str = ""
    date: str = ""
    score: int = 0
    release_group: _MBReleaseGroupRef = msgspec.field(default_factory=_MBReleaseGroupRef)
    artist_credit: list[_MBArtistCreditName] = msgspec.field(default_factory=list[_MBArtistCreditName])


class _MBReleaseSearchResponse(msgspec.Struct, rename="kebab"):
    """Slice of the ``/release?query=`` search response."""
    releases: list[_MBReleaseSearchHit] = msgspec.field(default_factory=list[_MBReleaseSearchHit])


class _MBArtistSearchHit(msgspec.Struct, rename="kebab"):
    """Slice of one ``/artist?query=`` search-result hit."""
    id: str = ""
    name: str = ""
    disambiguation: str = ""
    score: int = 0


class _MBArtistSearchResponse(msgspec.Struct, rename="kebab"):
    """Slice of the ``/artist?query=`` search response."""
    artists: list[_MBArtistSearchHit] = msgspec.field(default_factory=list[_MBArtistSearchHit])


class _MBArtistRelation(msgspec.Struct, rename="kebab"):
    """Slice of one entry in an ``/artist/<mbid>?inc=artist-rels`` response."""
    type: str = ""
    direction: str = ""
    artist: _MBArtistRef | None = None


class _MBArtistDetail(msgspec.Struct, rename="kebab"):
    """Slice of the ``/artist/<mbid>?inc=artist-rels`` response."""
    relations: list[_MBArtistRelation] = msgspec.field(default_factory=list[_MBArtistRelation])


class _MBReleaseGroupBrowseResponse(msgspec.Struct, rename="kebab"):
    """Slice of the ``/release-group?artist=`` browse response."""
    release_groups: list[_MBReleaseGroupRef] = msgspec.field(default_factory=list[_MBReleaseGroupRef])
    release_group_count: int = 0


class _MBReleaseBrowseHit(msgspec.Struct, rename="kebab"):
    """Slice of one ``/release?artist=`` / ``/release?track_artist=`` hit."""
    id: str = ""
    release_group: _MBReleaseGroupRef | None = None
    status: str | None = None


class _MBReleaseBrowseResponse(msgspec.Struct, rename="kebab"):
    """Slice of the ``/release?artist=`` / ``/release?track_artist=`` /
    plain ``/release?artist=`` (no ``inc=``) browse response — the last of
    these hits may carry only ``id`` per release."""
    releases: list[_MBReleaseBrowseHit] = msgspec.field(default_factory=list[_MBReleaseBrowseHit])
    release_count: int = 0


class _MBMediaSummary(msgspec.Struct, rename="kebab"):
    """Slice of one ``media`` entry in a release-group's release summary."""
    format: str | None = None
    track_count: int = 0


class _MBReleaseGroupReleaseSummary(msgspec.Struct, rename="kebab"):
    """Slice of one release summary inside ``/release?release-group=``.

    ``country``/``status`` are typed nullable (proven against live mirror
    samples: both arrive as JSON ``null`` on real releases) — the
    TypedDict this Struct replaces declared them plain ``str``, so a
    null value used to pass ``None`` straight into the frontend-facing
    dict silently via ``.get("status", "")`` (a present key with a null
    value returns the null, not the fallback)."""
    id: str = ""
    title: str = ""
    date: str = ""
    country: str | None = None
    status: str | None = None
    media: list[_MBMediaSummary] = msgspec.field(default_factory=list[_MBMediaSummary])


class _MBReleaseGroupReleasesResponse(msgspec.Struct, rename="kebab"):
    """Slice of the ``/release?release-group=`` browse response."""
    releases: list[_MBReleaseGroupReleaseSummary] = msgspec.field(default_factory=list[_MBReleaseGroupReleaseSummary])
    release_count: int = 0


class _MBRecordingRef(msgspec.Struct, rename="kebab"):
    """Slice of a MusicBrainz ``recording`` object this module reads."""
    id: str = ""
    length: int | None = None


class _MBTrackFull(msgspec.Struct, rename="kebab"):
    """Slice of a full-release-lookup ``track`` object.

    ``number`` is MB's free-form printed label (``"1"``, ``"A1"``, ``"B2"``
    for vinyl) — a **string** at the wire, confirmed against live mirror
    samples. The historical TypedDict this Struct replaces declared it
    ``int``, an unvalidated annotation nothing ever checked; that was
    exactly the kind of drift the wire-boundary rule exists to catch."""
    position: int | None = None
    number: str | None = None
    title: str = ""
    length: int | None = None
    recording: _MBRecordingRef | None = None


class _MBPregap(msgspec.Struct, rename="kebab"):
    """Slice of a full-release-lookup medium's ``pregap`` object."""
    title: str = ""
    length: int | None = None
    recording: _MBRecordingRef | None = None


class _MBFullMedium(msgspec.Struct, rename="kebab"):
    """Slice of a full-release-lookup ``medium`` object."""
    position: int = 1
    format: str | None = None
    pregap: _MBPregap | None = None
    tracks: list[_MBTrackFull] = msgspec.field(default_factory=list[_MBTrackFull])


class _MBReleaseFullStruct(msgspec.Struct, rename="kebab"):
    """Strict decode counterpart of ``_MBReleaseFullJSON`` below — the
    slice of the ``/release/<mbid>?inc=recordings+artist-credits+media+
    release-groups+labels`` response this module reads for its own
    internal use (``_strip_release`` / ``get_artist_releases_with_
    recordings``). Kept as a separate name from ``_MBReleaseFullJSON``
    because that TypedDict remains the public return-type shape
    ``get_artist_releases_with_recordings`` hands to
    ``lib.artist_releases`` (a plain dict, produced here via
    ``msgspec.to_builtins`` after this Struct validates it) — see that
    function's docstring."""
    id: str = ""
    title: str = ""
    date: str = ""
    country: str = ""
    status: str = ""
    artist_credit: list[_MBArtistCreditName] = msgspec.field(default_factory=list[_MBArtistCreditName])
    release_group: _MBReleaseGroupRef | None = None
    media: list[_MBFullMedium] = msgspec.field(default_factory=list[_MBFullMedium])


class _MBArtistReleasesWithRecordingsResponse(msgspec.Struct, rename="kebab"):
    """Slice of the ``/release?artist=...&inc=recordings+media+release-groups``
    response — full per-release shape (unlike ``_MBReleaseBrowseResponse``,
    whose hits carry only ``id``/``release-group``/``status``)."""
    releases: list[_MBReleaseFullStruct] = msgspec.field(default_factory=list[_MBReleaseFullStruct])
    release_count: int = 0


# ── TypedDict shapes still used as plain-dict type hints ────────────────
#
# ``get_release_raw`` returns the untouched wide dict (see its docstring);
# ``get_artist_releases_with_recordings`` returns a plain dict per release
# (via ``msgspec.to_builtins`` on the validated ``_MBReleaseFullStruct``
# above) so ``lib.artist_releases`` — deliberately decoupled from any
# ``web.mb`` runtime import — keeps consuming ``.get(...)``-style dicts
# exactly as before.


class _MBArtistRefJSON(TypedDict, total=False):
    """Plain-dict shape of an MB nested ``artist`` object — the
    ``_MBArtistRef`` Struct above validates the wire; this TypedDict
    types the dict ``msgspec.to_builtins`` hands back out to
    ``lib.artist_releases`` (see ``_MBReleaseFullJSON`` below)."""
    id: str
    name: str
    disambiguation: str


class _MBArtistCreditNameJSON(TypedDict, total=False):
    """Plain-dict shape of one MB ``artist-credit`` array entry."""
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
"""Plain-dict shape of a MusicBrainz ``release-group`` object nested
inside ``_MBReleaseFullJSON``."""


_MBRecordingRefJSON = TypedDict("_MBRecordingRefJSON", {
    "id": str,
    "length": int,
}, total=False)
"""Plain-dict shape of a MusicBrainz ``recording`` object."""


_MBTrackFullJSON = TypedDict("_MBTrackFullJSON", {
    "position": int,
    "number": str,
    "title": str,
    "length": int,
    "recording": _MBRecordingRefJSON,
}, total=False)
"""Plain-dict shape of a full-release-lookup ``track`` object. ``number``
is a string at the wire (MB's printed label, e.g. ``"A1"`` for vinyl) —
see ``_MBTrackFull``'s docstring above for the drift this replaced."""


_MBPregapJSON = TypedDict("_MBPregapJSON", {
    "title": str,
    "length": int,
    "recording": _MBRecordingRefJSON,
}, total=False)
"""Plain-dict shape of a full-release-lookup medium's ``pregap`` object."""


_MBFullMediumJSON = TypedDict("_MBFullMediumJSON", {
    "position": int,
    "format": str,
    "pregap": _MBPregapJSON,
    "tracks": list[_MBTrackFullJSON],
}, total=False)
"""Plain-dict shape of a full-release-lookup ``medium`` object."""


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
"""Plain-dict shape returned by ``get_artist_releases_with_recordings`` —
each entry is validated on ingest via ``_MBReleaseFullStruct`` and handed
out as a dict via ``msgspec.to_builtins``. ``lib.artist_releases`` imports
this under ``TYPE_CHECKING`` for its own type hints (zero runtime coupling
to this module) and needs this exact nested-TypedDict shape, not a
flattened ``dict[str, object]``, to keep its own chained ``.get()`` reads
strict-pyright-clean."""


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
        raw = _get(f"{MB_API_BASE}/release?query={q}&fmt=json&limit=25")
        data = msgspec.convert(raw, type=_MBReleaseSearchResponse)
        seen_rg: set[str] = set()
        results: list[dict[str, object]] = []
        for r in data.releases:
            rg = r.release_group
            rg_id = rg.id
            if not rg_id or rg_id in seen_rg:
                continue
            seen_rg.add(rg_id)
            artist_credit = r.artist_credit
            artist = artist_credit[0].artist if artist_credit else _EMPTY_MB_ARTIST_REF
            results.append({
                "id": rg_id,
                "title": rg.title or r.title or "",
                "primary_type": rg.primary_type or "",
                "first_release_date": rg.first_release_date or r.date or "",
                "artist_id": artist.id,
                "artist_name": artist.name,
                "artist_disambiguation": artist.disambiguation,
                "score": r.score,
            })
        return results

    return _cache.memoize_meta(f"mb:search:release_groups:{query}", _fetch)


class _MBArtistIdentity(Protocol):
    """Structural shape shared by ``_MBArtistRef`` and ``_MBArtistSearchHit``
    — both carry ``id``/``name``/``disambiguation``, the fields
    ``_artist_search_hit`` projects, without both types needing a common
    base class."""
    id: str
    name: str
    disambiguation: str


def _artist_search_hit(
    artist: _MBArtistIdentity, *, score: int,
) -> dict[str, object]:
    return {
        "id": artist.id,
        "name": artist.name,
        "disambiguation": artist.disambiguation,
        "score": score,
    }


def _related_artist_identity_hits(
    artist_id: str, *, score: int,
) -> list[dict[str, object]]:
    """Resolve canonical MusicBrainz ``is person`` identity siblings."""
    raw = _get(
        f"{MB_API_BASE}/artist/{_quote_mb_identifier(artist_id)}?inc=artist-rels&fmt=json"
    )
    detail = msgspec.convert(raw, type=_MBArtistDetail)
    relations = detail.relations
    identity_artists: list[_MBArtistRef] = []

    # A persona such as Four Tet points backward to the underlying person.
    person = next((
        rel.artist
        for rel in relations
        if rel.type == "is person"
        and rel.direction == "backward"
        and rel.artist is not None
        and rel.artist.id
    ), None)
    if person is not None:
        identity_artists.append(person)
        raw = _get(
            f"{MB_API_BASE}/artist/{_quote_mb_identifier(person.id)}?inc=artist-rels&fmt=json"
        )
        detail = msgspec.convert(raw, type=_MBArtistDetail)
        relations = detail.relations

    # The person entity points forward to each separately catalogued persona.
    identity_artists.extend(
        rel.artist
        for rel in relations
        if rel.type == "is person"
        and rel.direction == "forward"
        and rel.artist is not None
        and rel.artist.id
    )
    return [
        _artist_search_hit(artist, score=score)
        for artist in identity_artists
    ]


def search_artists(query: str) -> list[dict[str, object]]:
    """Search for artists by name. Returns list of {id, name, disambiguation, score}."""
    def _fetch() -> list[dict[str, object]]:
        q = urllib.parse.quote(query, safe="")
        raw = _get(f"{MB_API_BASE}/artist?query={q}&fmt=json&limit=20")
        data = msgspec.convert(raw, type=_MBArtistSearchResponse)
        results = [
            _artist_search_hit(a, score=a.score)
            for a in data.artists
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
    rg: _MBReleaseGroupRef,
    *,
    is_appearance: bool,
) -> ArtistCatalogueRow:
    """Shape direct and track-appearance MB rows into one artist-page contract."""
    ac = rg.artist_credit
    credit_name = " / ".join(a.name or "?" for a in ac) if ac else ""
    primary_artist_id = ac[0].artist.id if ac else None
    # MusicBrainz represents an unclassified release group with JSON null,
    # not only by omitting the field. The shared catalogue contract keeps
    # display text non-null and carries structural knowledge separately.
    primary_type = rg.primary_type or ""
    _structural: dict[str, ArtistStructuralType] = {
        "Album": "Album", "EP": "EP", "Single": "Single",
    }
    primary_types: list[ArtistStructuralType] = []
    structural_type = _structural.get(primary_type)
    if structural_type is not None:
        primary_types.append(structural_type)
    return ArtistCatalogueRow(
        id=rg.id,
        title=rg.title or "",
        type=primary_type,
        source="mb",
        identity_kind="work",
        primary_types=primary_types,
        secondary_types=rg.secondary_types or [],
        format_qualifiers=[],
        # Release status is unioned set-wise inside get_artist_release_groups
        # before rows leave this adapter.
        provenance=[],
        first_release_date=rg.first_release_date or "",
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

        def collect_release_provenance(release: _MBReleaseBrowseHit) -> None:
            rg = release.release_group
            if rg is None or not rg.id:
                return
            status = release.status
            provenance: ArtistProvenance | None = None
            if status == "Official":
                provenance = "ordinary"
            elif status == "Promotion":
                provenance = "promo"
            elif status == "Bootleg":
                provenance = "unofficial"
            if provenance is not None:
                provenance_by_rg.setdefault(rg.id, set()).add(provenance)

        def fetch_release_groups(
            offset: int, limit: int,
        ) -> _MBReleaseGroupBrowseResponse:
            raw = _get(
                f"{MB_API_BASE}/release-group?artist={_quote_mb_identifier(artist_mbid)}"
                f"&inc=artist-credits&fmt=json&limit={limit}&offset={offset}"
            )
            return msgspec.convert(raw, type=_MBReleaseGroupBrowseResponse)

        def fetch_direct_releases(
            offset: int, limit: int,
        ) -> _MBReleaseBrowseResponse:
            raw = _get(
                f"{MB_API_BASE}/release?artist={_quote_mb_identifier(artist_mbid)}"
                f"&inc=release-groups&fmt=json&limit={limit}&offset={offset}"
            )
            return msgspec.convert(raw, type=_MBReleaseBrowseResponse)

        def fetch_track_appearances(
            offset: int, limit: int,
        ) -> _MBReleaseBrowseResponse:
            raw = _get(
                f"{MB_API_BASE}/release?track_artist={_quote_mb_identifier(artist_mbid)}"
                "&inc=release-groups+artist-credits"
                f"&fmt=json&limit={limit}&offset={offset}"
            )
            return msgspec.convert(raw, type=_MBReleaseBrowseResponse)

        # The three families are independent.  Each helper's remaining pages
        # fan out after its own first page, and _get keeps their combined load
        # within one mirror-wide budget.
        families = _parallel_results({
            "release_groups": lambda: _fetch_browse_pages(
                fetch_release_groups,
                lambda page: page.release_group_count,
                lambda page: page.release_groups,
                lambda item: item.id,
            ),
            "direct_releases": lambda: _fetch_browse_pages(
                fetch_direct_releases,
                lambda page: page.release_count,
                lambda page: page.releases,
                lambda item: item.id,
            ),
            "track_appearances": lambda: _fetch_browse_pages(
                fetch_track_appearances,
                lambda page: page.release_count,
                lambda page: page.releases,
                lambda item: item.id,
            ),
        }, max_workers=3)
        # _parallel_results' return type is homogeneous per its shared
        # ``Result`` TypeVar, so a dict fanning out THREE genuinely
        # different per-key result types cannot statically discriminate
        # `families["release_groups"]` from `families["direct_releases"]`
        # by key alone — pyright infers every entry as the union of all
        # three. The `isinstance` asserts below are real narrowing (each
        # job always returns its own declared type; this can never fail
        # at runtime) rather than a `cast`/`type: ignore` escape hatch.
        release_group_pages = families["release_groups"]
        direct_release_pages = families["direct_releases"]
        track_appearance_pages = families["track_appearances"]

        for data in release_group_pages:
            assert isinstance(data, _MBReleaseGroupBrowseResponse)
            for rg in data.release_groups:
                entry = _normalize_artist_release_group(
                    rg, is_appearance=False,
                )
                entries.setdefault(entry.id, entry)

        # A release group browse carries no child release statuses. Fetch the
        # directly credited releases without a status filter so mixed
        # Official/Promotion/Bootleg evidence survives as a set.
        for release_data in direct_release_pages:
            assert isinstance(release_data, _MBReleaseBrowseResponse)
            for release in release_data.releases:
                collect_release_provenance(release)

        for track_data in track_appearance_pages:
            assert isinstance(track_data, _MBReleaseBrowseResponse)
            for release in track_data.releases:
                collect_release_provenance(release)
                rg = release.release_group
                if rg is None or not rg.id:
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
        raw = _get(
            f"{MB_API_BASE}/release-group/{_quote_mb_identifier(rg_mbid)}?inc=artist-credits&fmt=json")
        data = msgspec.convert(raw, type=_MBReleaseGroupRef)
        ac = data.artist_credit
        artist = ac[0].artist if ac else _EMPTY_MB_ARTIST_REF
        return {
            "id": data.id,
            "title": data.title or "",
            "type": data.primary_type or "",
            "first_release_date": data.first_release_date or "",
            "artist_id": artist.id,
            "artist_name": artist.name,
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
        raw = _get(f"{MB_API_BASE}/release-group/{_quote_mb_identifier(rg_mbid)}?fmt=json")
        data = msgspec.convert(raw, type=_MBReleaseGroupRef)
        from lib.util import parse_mb_first_release_year
        return parse_mb_first_release_year(msgspec.to_builtins(data))

    return _cache.memoize_meta(
        f"mb:release-group:{rg_mbid}:year", _fetch)


def get_release_group_releases(rg_mbid: str) -> dict[str, object]:
    """Get all releases for a release group. Returns list of release summaries."""
    def _fetch() -> dict[str, object]:
        # First get the release group metadata
        raw_meta = _get(
            f"{MB_API_BASE}/release-group/{_quote_mb_identifier(rg_mbid)}?fmt=json")
        rg_data = msgspec.convert(raw_meta, type=_MBReleaseGroupRef)

        # Then browse all releases (paginated — the lookup endpoint caps at 25)
        releases: list[dict[str, object]] = []
        offset = 0
        while True:
            raw = _get(
                f"{MB_API_BASE}/release?release-group={_quote_mb_identifier(rg_mbid)}"
                f"&inc=media&fmt=json&limit=100&offset={offset}"
            )
            data = msgspec.convert(raw, type=_MBReleaseGroupReleasesResponse)
            for r in data.releases:
                track_count = sum(m.track_count for m in r.media)
                formats = [(m.format or "?") for m in r.media]
                releases.append({
                    "id": r.id,
                    "title": r.title,
                    "date": r.date,
                    "country": r.country or "",
                    "status": r.status or "",
                    "track_count": track_count,
                    "format": ", ".join(formats) if formats else "?",
                    "media_count": len(r.media),
                })
            total = data.release_count
            offset += 100
            if offset >= total:
                break

        return {
            "title": rg_data.title or "",
            "type": rg_data.primary_type or "",
            "releases": releases,
        }

    return _cache.memoize_meta(f"mb:release-group:{rg_mbid}:releases", _fetch)


def _fetch_release_raw(
    release_mbid: str, *, fresh: bool = False,
) -> dict[str, object]:
    """Shared fetch+cache path for ``get_release_raw`` and ``get_release``.

    One network/cache round trip regardless of which shape the caller
    wants: ``get_release_raw`` returns this untouched wide dict directly;
    ``get_release`` decodes it into ``_MBReleaseFullStruct`` (validated)
    before calling ``_strip_release``. Never duplicate this fetch — add a
    second caller instead.
    """
    def _fetch() -> dict[str, object]:
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
    is a plain ``dict[str, object]``, deliberately never decoded into any
    of this module's strict Structs, precisely because real callers
    (``lib.field_resolver_service``'s ``joinphrase``/label-info reads,
    ``lib.library_completeness``'s ``recording.video``/``pregap`` reads,
    ``web.routes.pipeline_mutations``'s nested release-group primary-type
    read for VA Rule 2) read fields wider than any narrow slice this
    module could safely declare without becoming a moving target every
    time one of them needs one more field.
    """
    return dict(_fetch_release_raw(release_mbid, fresh=fresh))


def _strip_release(data: _MBReleaseFullStruct) -> dict[str, object]:
    """Slim a validated MB release Struct down to the shape the frontend +
    pipeline DB inserts want. Pure function over `data`."""
    artist_credit = data.artist_credit
    artist_name = (artist_credit[0].name or "Unknown") if artist_credit else "Unknown"
    artist_id = (artist_credit[0].artist.id or None) if artist_credit else None
    rg_id = (data.release_group.id or None) if data.release_group is not None else None

    tracks: list[dict[str, object]] = []
    for medium in data.media:
        disc = medium.position
        if medium.pregap is not None:
            pg = medium.pregap
            length_ms = pg.length
            if length_ms is None and pg.recording is not None:
                length_ms = pg.recording.length
            tracks.append({
                "disc_number": disc,
                "track_number": 0,
                "title": pg.title,
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })
        for track in medium.tracks:
            length_ms = track.length
            if length_ms is None and track.recording is not None:
                length_ms = track.recording.length
            if track.position is not None:
                track_number: int | str = track.position
            elif track.number is not None:
                try:
                    track_number = int(track.number)
                except ValueError:
                    track_number = track.number
            else:
                track_number = 0
            tracks.append({
                "disc_number": disc,
                "track_number": track_number,
                "title": track.title,
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })

    year = None
    release_date = data.date
    if release_date:
        try:
            year = int(release_date[:4])
        except (ValueError, IndexError):
            pass

    return {
        "id": data.id,
        "title": data.title,
        "artist_name": artist_name,
        "artist_id": artist_id,
        "release_group_id": rg_id,
        "date": data.date,
        "year": year,
        "country": data.country,
        "status": data.status,
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
    the raw MB JSON is the single cached truth; this decodes that same
    cached dict into ``_MBReleaseFullStruct`` (validated) and re-derives
    the slim shape per call — the re-derivation plus decode is a pure
    traversal, still ~microseconds.
    """
    raw = _fetch_release_raw(release_mbid, fresh=fresh)
    data = msgspec.convert(raw, type=_MBReleaseFullStruct)
    return _strip_release(data)


def get_artist_name(artist_mbid: str) -> str:
    """Look up an artist's name by MBID."""
    def _fetch() -> str:
        raw = _get(f"{MB_API_BASE}/artist/{_quote_mb_identifier(artist_mbid)}?fmt=json")
        data = msgspec.convert(raw, type=_MBArtistRef)
        return data.name

    return _cache.memoize_meta(f"mb:artist:{artist_mbid}:name", _fetch)


def assert_exact_release_id_order(
    expected_ids: list[str], releases: list[_MBReleaseFullStruct],
) -> None:
    """Fail closed unless detailed pagination preserves canonical identity/order.

    The no-``inc`` artist browse is the direct-release identity authority.
    Nested recording pages are only enrichment: changing their page limit can
    alter membership, so they must not be allowed to add, lose, duplicate, or
    reorder a release.
    """
    actual_ids = [release.id for release in releases]
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
    Each release is validated on ingest via ``_MBReleaseFullStruct`` and
    handed back through ``msgspec.to_builtins`` so ``lib.artist_releases``
    keeps consuming plain dicts (see ``_MBReleaseFullJSON``'s docstring).
    """
    def _fetch() -> list[_MBReleaseFullJSON]:
        def fetch_canonical(
            offset: int, limit: int,
        ) -> _MBReleaseBrowseResponse:
            raw = _get(
                f"{MB_API_BASE}/release?artist={_quote_mb_identifier(artist_mbid)}"
                f"&fmt=json&limit={limit}&offset={offset}"
            )
            return msgspec.convert(raw, type=_MBReleaseBrowseResponse)

        canonical_pages = _fetch_browse_pages(
            fetch_canonical, lambda page: page.release_count,
            lambda page: page.releases,
            lambda item: item.id,
        )
        canonical_ids = [
            release.id
            for page in canonical_pages
            for release in page.releases
        ]
        if len(canonical_ids) != len(set(canonical_ids)):
            raise MusicBrainzArtistCatalogueIncomplete(
                "canonical direct-release browse contained duplicate IDs",
            )

        def fetch_segment(start: int) -> list[_MBReleaseFullStruct]:
            """Fill one fixed canonical offset segment using its exact remainder.

            MusicBrainz may return fewer releases than requested when media and
            recordings are included.  Advancing by a short page with the old
            *100* limit causes an overlap; reducing the next request to the
            exact remainder keeps the segment's offset semantics stable.
            """
            end = min(start + 100, len(canonical_ids))
            offset = start
            segment: list[_MBReleaseFullStruct] = []
            while offset < end:
                raw = _get(
                    f"{MB_API_BASE}/release?artist={_quote_mb_identifier(artist_mbid)}"
                    "&inc=recordings+media+release-groups&fmt=json"
                    f"&limit={end - offset}&offset={offset}"
                )
                data = msgspec.convert(raw, type=_MBArtistReleasesWithRecordingsResponse)
                page = data.releases
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
        by_id: dict[str, _MBReleaseFullStruct] = {}
        for release in detailed_pages:
            if release.id in canonical_id_set:
                by_id.setdefault(release.id, release)

        missing_ids = [release_id for release_id in canonical_ids if release_id not in by_id]
        if missing_ids:
            # A mirror may still surface an overlap across two fixed segments.
            # Completeness outranks the fast path: fetch only those canonical
            # leaves, still under the shared mirror cap, rather than silently
            # dropping a pressing or guessing from an adjacent row.
            def fetch_missing(release_id: str) -> _MBReleaseFullStruct:
                raw = _get(
                    f"{MB_API_BASE}/release/{_quote_mb_identifier(release_id)}"
                    "?inc=recordings+media+release-groups&fmt=json",
                )
                return msgspec.convert(raw, type=_MBReleaseFullStruct)

            repairs = _parallel_results(
                {release_id: (lambda release_id=release_id: fetch_missing(release_id))
                 for release_id in missing_ids},
                max_workers=_MB_MIRROR_CONCURRENCY,
            )
            for release_id in missing_ids:
                repaired = repairs[release_id]
                if repaired.id != release_id:
                    raise MusicBrainzArtistCatalogueIncomplete(
                        "recording repair returned a different release ID",
                    )
                by_id[release_id] = repaired
        releases = [by_id[release_id] for release_id in canonical_ids]
        assert_exact_release_id_order(canonical_ids, releases)
        return [msgspec.to_builtins(release) for release in releases]

    cached = _cache.memoize_meta(
        f"mb:artist:{artist_mbid}:releases_with_recordings:v3", _fetch)
    return [{**item} for item in cached]
