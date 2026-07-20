"""Discogs mirror API helpers — shared between pipeline_cli and web server.

All queries hit the local Discogs mirror (DISCOGS_API_BASE; mirror-required,
see require_mirror_configured).
Response shapes are normalized to match what the frontend expects,
mirroring web/mb.py where possible.

Pure-metadata responses are memoized via `cache.memoize_meta()` at
24h TTL. See web/mb.py and web/cache.py for rationale — the cache
layer sits at the API helper level, not at the HTTP routing level,
so that per-user pipeline / library overlay state is never baked
into Redis (issue #101).
"""

import hashlib
import json
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Literal, TypedDict

import msgspec

from lib.artist_catalogue import ArtistCatalogueRow
from web import cache as _cache
from web.artist_search import ArtistHit, merge_exact_artist_identities

# Mirror-REQUIRED (tier-2 plan U6, R13): these endpoints (/api/search,
# /api/masters/<id>, ...) and the msgspec response Structs are the Rust
# Discogs mirror's shape — public api.discogs.com does not serve them, so
# there is no functional public fallback. None = Discogs browse is off;
# the server wires this from config.ini [Discogs] api_base via
# configure_api_bases_from_runtime_config() at cratedigger-web startup
# (services.cratedigger.discogs.apiBase; issue #497 dropped the module's
# --discogs-api flag in favor of config.ini as the one production source
# — the flag itself survives as a dev-only override).
DISCOGS_API_BASE: str | None = None


class DiscogsMirrorNotConfigured(RuntimeError):
    """Raised at URL-construction time when no Discogs mirror is configured.

    web/server.py maps this to a 503 with the message, so browse callers
    get a clear mirror-required response instead of a broken upstream
    fetch."""


def require_mirror_configured() -> str:
    """Return the mirror base or reject before any cached work is dispatched."""
    if not DISCOGS_API_BASE:
        raise DiscogsMirrorNotConfigured(
            "Discogs browse requires a Discogs mirror: this API speaks the "
            "Rust mirror's endpoint shape, which public api.discogs.com "
            "does not serve. Set services.cratedigger.discogs.apiBase. "
            "Without a mirror, browse via MusicBrainz only."
        )
    return DISCOGS_API_BASE
USER_AGENT = "cratedigger-web/1.0"
SEARCH_CACHE_QUERY_PREFIX_CHARS = 200
DEFAULT_HTTP_TIMEOUT_SECONDS = 60
LABEL_RELEASES_INCLUDE_TIMEOUT_SECONDS = 20

# Canonical Various Artists artist_id sentinel. The Discogs CC0 dump uses
# 194 as the foreign key in `release_artist` for VA-credited releases, but
# does NOT include a matching row in the `artist` table — so the mirror's
# `/api/artists/194` returns 404 and the artist view cannot be rendered.
# The resolver short-circuits to a single-release / single-master fallback
# card when a release credits this ID. Stored as a string for consistent
# comparison against `artist_id` fields, which are always normalised to str.
# Single declaration site at ``lib/va_identity.py`` — re-exported here so
# the existing ``from web.discogs import VA_ARTIST_ID`` imports keep working.
from lib.va_identity import (  # noqa: E402
    DISCOGS_VA_ARTIST_ID as VA_ARTIST_ID,
    split_va_query,
)


def _get(url: str, *, timeout: int = DEFAULT_HTTP_TIMEOUT_SECONDS) -> Any:
    """Fetch and JSON-decode one Discogs mirror URL.

    Returns ``Any`` — the raw external-JSON boundary; callers immediately
    assign the result to a locally-scoped ``_Discogs*JSON`` TypedDict-
    annotated variable (untyped/unvalidated at runtime — same
    ``.get(..., default)`` tolerance as before) so downstream field access
    is precisely typed without this function validating the response shape.
    The label endpoints below decode through ``msgspec.convert`` instead
    (strict wire-boundary Structs), since they need real validation.
    """
    req = urllib.request.Request(url)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Connection", "close")
    # Single-word release searches against ~19M rows can take 15-30s on the
    # mirror; the request always succeeds eventually. Generous timeout so the
    # web UI doesn't 500 on broad queries (use the in-flight Redis cache to
    # short-circuit repeats).
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _is_timeout_error(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return True
        reason_text = str(reason).lower()
        return "timed out" in reason_text or "timeout" in reason_text
    return False


def _search_cache_query_part(query: str) -> str:
    """Bound user-controlled search text before embedding it in cache keys."""
    if len(query) <= SEARCH_CACHE_QUERY_PREFIX_CHARS:
        return query
    digest = hashlib.sha256(query.encode("utf-8")).hexdigest()[:12]
    return f"{query[:SEARCH_CACHE_QUERY_PREFIX_CHARS]}:#{len(query)}:{digest}"


def _assert_discogs_label_id(label_id: int | str) -> str:
    label_id_str = str(label_id)
    assert label_id_str.isdigit()
    return label_id_str


def _parse_duration(duration_str: str) -> float | None:
    """Parse Discogs duration string (e.g. '4:44') to seconds."""
    if not duration_str:
        return None
    parts = duration_str.split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        return None
    return None


def _parse_year(date_str: str) -> int | None:
    """Extract year from Discogs date string (e.g. '1997-06-16' or '1997')."""
    if not date_str:
        return None
    try:
        return int(date_str[:4])
    except (ValueError, IndexError):
        return None


class _DiscogsArtistRefJSON(TypedDict, total=False):
    """Slice of a Discogs ``artists[]`` credit entry (search hit, release,
    or master-release row)."""
    id: int
    name: str


def _primary_artist_name(artists: list[_DiscogsArtistRefJSON]) -> str:
    """Extract the display artist name from a Discogs artists array."""
    if not artists:
        return "Unknown"
    return artists[0].get("name", "Unknown")


def _primary_artist_id(artists: list[_DiscogsArtistRefJSON]) -> int | None:
    """Extract the primary artist ID from a Discogs artists array."""
    if not artists:
        return None
    return artists[0].get("id")


def _parse_position(position: str) -> tuple[int, int]:
    """Parse a Discogs track position like '1', 'A1', '1-3' into (disc, track).

    Simple numeric: disc=1, track=N
    Letter prefix (vinyl): disc=ord(letter)-ord('A')+1, track from digits
    Disc-track (CD): split on '-'
    """
    if not position:
        return 1, 0
    m = re.match(r"^(\d+)-(\d+)$", position)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.match(r"^([A-Za-z])(\d+)$", position)
    if m:
        disc = ord(m.group(1).upper()) - ord("A") + 1
        return disc, int(m.group(2))
    m = re.match(r"^(\d+)$", position)
    if m:
        return 1, int(m.group(1))
    return 1, 0


class _DiscogsSearchHitJSON(TypedDict, total=False):
    """Slice of one ``/api/search`` result hit."""
    id: int
    master_id: int
    title: str
    master_title: str
    primary_type: str
    released: str
    master_first_released: str
    artists: list[_DiscogsArtistRefJSON]
    score: float


class _DiscogsSearchResponseJSON(TypedDict, total=False):
    """Slice of the ``/api/search`` response."""
    results: list[_DiscogsSearchHitJSON]


def search_releases(query: str) -> list[dict[str, object]]:
    """Search releases by query string. Returns list of release summaries grouped by master.

    Deduplicates by master_id (like MB's release-group dedup) and surfaces
    master-level metadata (master_title, master_first_released, primary_type, score)
    that the mirror provides on each search hit.

    "Various Artists" tokens are stripped from the title and routed to
    the mirror's ``artist_id`` exact filter (#199) — the dump's VA artist
    (id 194) has no name row, so the tokens can never match the text
    index and pre-fix VA queries returned zero results. Pinning
    ``artist_id=194`` makes the mirror return only VA-credited releases.
    The pinned fetch is a distinct upstream query, so it gets its own
    cache entry (the bare-title fetch returns a different, unfiltered
    set). A VA query with no title remainder ("Various Artists" alone)
    keeps the raw passthrough — an artist_id pin with no title would make
    the mirror scan every one of artist 194's releases.
    """
    api_base = require_mirror_configured()
    remainder, is_va = split_va_query(query)
    va_pin = is_va and bool(remainder)
    effective_query = remainder if va_pin else query

    def _fetch() -> list[dict[str, object]]:
        q = urllib.parse.quote(effective_query)
        url = f"{api_base}/api/search?title={q}&per_page=25"
        if va_pin:
            url += f"&artist_id={VA_ARTIST_ID}"
        data: _DiscogsSearchResponseJSON = _get(url)
        seen_master: set[int] = set()
        results: list[dict[str, object]] = []
        for r in data.get("results", []):
            master_id = r.get("master_id")
            release_id = r.get("id", 0)
            artists = r.get("artists", [])
            if master_id and master_id in seen_master:
                continue
            if master_id:
                seen_master.add(master_id)
            title = r.get("master_title") or r.get("title", "") if master_id else r.get("title", "")
            first_released = r.get("master_first_released") or r.get("released", "") if master_id else r.get("released", "")
            results.append({
                "id": str(master_id) if master_id else str(release_id),
                "title": title,
                "primary_type": r.get("primary_type", ""),
                "first_release_date": first_released,
                "artist_id": str(_primary_artist_id(artists) or ""),
                "artist_name": _primary_artist_name(artists),
                "artist_disambiguation": "",
                "score": int(r.get("score", 0) * 100),
                "is_master": bool(master_id),
                "discogs_release_id": str(release_id),
            })
        return results

    # The va flag sits in a FIXED position before the (unbounded) user
    # query text, so a plain query can never forge the VA-pinned key by
    # ending in the discriminator — the two fetches hit different upstream
    # URLs and must never share a cache entry.
    cache_query = _search_cache_query_part(effective_query)
    cache_key = f"discogs:search:releases:va={int(va_pin)}:{cache_query}"
    return _cache.memoize_meta(cache_key, _fetch)


class _DiscogsArtistSearchHitJSON(TypedDict, total=False):
    """Slice of one ``/api/artists?name=`` result hit."""
    id: int
    name: str
    score: float


class _DiscogsArtistSearchResponseJSON(TypedDict, total=False):
    """Slice of the ``/api/artists?name=`` response."""
    results: list[_DiscogsArtistSearchHitJSON]


class _DiscogsArtistDetailJSON(TypedDict, total=False):
    """Slice of the ``/api/artists/{id}`` response."""
    aliases: list[_DiscogsArtistRefJSON]


def search_artists(query: str) -> list[ArtistHit]:
    """Search for artists by name via the mirror's artist-name index.

    Uses /api/artists?name=, which is a real ts_rank artist-name search —
    parity with MB's /ws/2/artist?query=.
    """
    api_base = require_mirror_configured()

    def _fetch() -> list[ArtistHit]:
        q = urllib.parse.quote(query)
        data: _DiscogsArtistSearchResponseJSON = _get(
            f"{api_base}/api/artists?name={q}&per_page=20")
        results: list[ArtistHit] = [
            {
                "id": str(r.get("id", 0)),
                "name": r.get("name", ""),
                "disambiguation": "",
                "score": int(r.get("score", 0) * 100),
            }
            for r in data.get("results", [])
        ]
        exact = next((
            row for row in results
            if row["name"].casefold() == query.casefold()
        ), None)
        if exact is None:
            return results
        try:
            detail: _DiscogsArtistDetailJSON = _get(
                f"{api_base}/api/artists/{exact['id']}")
        except (urllib.error.HTTPError, urllib.error.URLError):
            return results
        related: list[ArtistHit] = [
            {
                "id": str(alias.get("id", 0)),
                "name": alias.get("name", ""),
                "disambiguation": "",
                "score": max(0, exact["score"] - 1),
            }
            for alias in detail.get("aliases", [])
        ]
        return merge_exact_artist_identities(
            results, exact_id=exact["id"], related=related,
        )

    cache_query = _search_cache_query_part(query)
    return _cache.memoize_meta(
        f"discogs:search:artists:v2:{cache_query}", _fetch,
    )


_DiscogsStructuralType = Literal["Album", "EP", "Single"]
_DiscogsProvenance = Literal["ordinary", "promo", "unofficial"]
_DISCOGS_RELEASE_ROW_ID_RE = re.compile(r"^release-([1-9]\d*)$")


class _DiscogsArtistMasterEntry(msgspec.Struct):
    """Strict row from an artist ``masters`` or ``appearances`` response."""
    id: int | str
    title: str
    type: str
    primary_types: list[_DiscogsStructuralType]
    format_qualifiers: list[str]
    provenance: list[_DiscogsProvenance]
    first_release_date: str
    artist_credit: str
    primary_artist_id: int | None
    is_masterless: bool


class _DiscogsArtistMastersResponse(msgspec.Struct):
    """Strict response shared by artist masters and appearances endpoints."""
    results: list[_DiscogsArtistMasterEntry]
    total: int
    page: int
    per_page: int


class DiscogsArtistCatalogueIncomplete(RuntimeError):
    """A bulk artist endpoint returned only part of its claimed catalogue."""


def _require_complete_artist_catalogue(
    response: _DiscogsArtistMastersResponse, *, endpoint: str,
) -> None:
    """Reject semantic pagination/truncation that still passes wire typing."""
    row_count = len(response.results)
    if response.page != 1 or row_count != response.total:
        raise DiscogsArtistCatalogueIncomplete(
            f"incomplete Discogs artist {endpoint} response: "
            f"page={response.page}, rows={row_count}, total={response.total}"
        )


def _normalize_artist_master_entry(
    r: _DiscogsArtistMasterEntry,
    *,
    is_appearance: bool,
) -> ArtistCatalogueRow:
    """Shape one row from /api/artists/{id}/{masters,appearances} into our schema.

    Masterless releases come back with id ``release-<n>``; we strip the prefix
    and normalize them as release identities so downstream code cannot confuse
    a leaf pressing with a master/work row.
    """
    for field_name, values in (
        ("primary_types", r.primary_types),
        ("format_qualifiers", r.format_qualifiers),
        ("provenance", r.provenance),
    ):
        if values != sorted(set(values)):
            raise ValueError(
                f"Discogs artist row {field_name} must be sorted and deduplicated"
            )

    raw_id = r.id
    is_masterless = r.is_masterless
    release_match = (
        _DISCOGS_RELEASE_ROW_ID_RE.fullmatch(raw_id)
        if isinstance(raw_id, str)
        else None
    )
    if is_masterless and release_match is None:
        raise ValueError(
            "Discogs masterless artist row id must be release-<positive integer>"
        )
    if not is_masterless and (
        not isinstance(raw_id, int) or isinstance(raw_id, bool) or raw_id <= 0
    ):
        raise ValueError(
            "Discogs master artist row id must be a positive integer"
        )
    if is_masterless:
        assert release_match is not None
        bare_id = release_match.group(1)
        return ArtistCatalogueRow(
            id=bare_id,
            title=r.title,
            type=r.type,
            source="discogs",
            identity_kind="release",
            primary_types=list(r.primary_types),
            secondary_types=[],
            format_qualifiers=list(r.format_qualifiers),
            provenance=list(r.provenance),
            first_release_date=r.first_release_date,
            artist_credit=r.artist_credit,
            primary_artist_id=(
                str(r.primary_artist_id) if r.primary_artist_id is not None else ""
            ),
            is_appearance=is_appearance,
            discogs_release_id=bare_id,
        )
    return ArtistCatalogueRow(
        id=str(raw_id),
        title=r.title,
        type=r.type,
        source="discogs",
        identity_kind="work",
        primary_types=list(r.primary_types),
        secondary_types=[],
        format_qualifiers=list(r.format_qualifiers),
        provenance=list(r.provenance),
        first_release_date=r.first_release_date,
        artist_credit=r.artist_credit,
        primary_artist_id=(
            str(r.primary_artist_id) if r.primary_artist_id is not None else ""
        ),
        is_appearance=is_appearance,
    )


def get_artist_releases(artist_id: int) -> list[ArtistCatalogueRow]:
    """Get an artist's discography grouped by master. Mirrors mb.get_artist_release_groups().

    Merges two mirror endpoints:

    * ``/api/artists/{id}/masters/all`` — every release where the artist is
      on the master/release-level credit, in one fail-loud bulk response.
    * ``/api/artists/{id}/appearances`` — releases where the artist appears
      only via a track-level credit (compilations, guest spots, samplers).
      Single response, no pagination.

    Dedupe inside the separate master/release id namespaces: a primary-credit
    identity from ``/masters/all`` wins over the same appearance identity, so
    we don't downgrade own work when it also has track-only credits. A master
    and a masterless release with the same numeric id remain distinct.
    """
    api_base = require_mirror_configured()

    def _fetch() -> list[ArtistCatalogueRow]:
        entries: dict[tuple[str, str], ArtistCatalogueRow] = {}

        masters = msgspec.convert(
            _get(f"{api_base}/api/artists/{artist_id}/masters/all"),
            type=_DiscogsArtistMastersResponse,
        )
        _require_complete_artist_catalogue(masters, endpoint="masters/all")
        for r in masters.results:
            entry = _normalize_artist_master_entry(
                r, is_appearance=False,
            )
            namespace = entry.identity_kind
            entries.setdefault((namespace, entry.id), entry)

        appearances = msgspec.convert(
            _get(f"{api_base}/api/artists/{artist_id}/appearances"),
            type=_DiscogsArtistMastersResponse,
        )
        _require_complete_artist_catalogue(appearances, endpoint="appearances")
        for r in appearances.results:
            entry = _normalize_artist_master_entry(
                r, is_appearance=True,
            )
            namespace = entry.identity_kind
            entries.setdefault((namespace, entry.id), entry)

        rows = sorted(
            entries.values(),
            key=lambda e: (e.first_release_date, e.id),
        )
        return msgspec.to_builtins(rows)

    cached = _cache.memoize_meta(
        f"discogs:artist:{artist_id}:releases:v7", _fetch,
    )
    return msgspec.convert(cached, type=list[ArtistCatalogueRow])


class _DiscogsFormatJSON(TypedDict, total=False):
    """Slice of one Discogs release/master ``formats[]`` entry."""
    name: str
    # Mirror serde emits ``null`` for an absent Option<Vec<String>>, so the
    # value is genuinely nullable at the wire — the reader guards for it.
    descriptions: str | list[str] | None


class _DiscogsMasterReleaseEntryJSON(TypedDict, total=False):
    """Slice of one ``/api/masters/{id}`` ``releases[]`` entry."""
    id: int
    title: str
    released: str
    country: str
    track_count: int
    formats: list[_DiscogsFormatJSON]
    labels: list[dict[str, object]]


class _DiscogsMasterDetailJSON(TypedDict, total=False):
    """Slice of the ``/api/masters/{id}`` response."""
    title: str
    primary_type: str
    first_release_date: str
    artist_credit: str
    primary_artist_id: int
    releases: list[_DiscogsMasterReleaseEntryJSON]


class _DiscogsTrackJSON(TypedDict, total=False):
    """Slice of one Discogs release ``tracks[]`` entry."""
    position: str
    title: str
    duration: str


class _DiscogsReleaseDetailJSON(TypedDict, total=False):
    """Slice of the ``/api/releases/{id}`` response."""
    id: int
    title: str
    artists: list[_DiscogsArtistRefJSON]
    tracks: list[_DiscogsTrackJSON]
    released: str
    master_id: int
    country: str
    formats: list[_DiscogsFormatJSON]
    labels: list[dict[str, object]]


def _status_from_formats(formats: list[_DiscogsFormatJSON]) -> str:
    """Project Discogs format descriptions into truthful display status."""
    qualifiers: set[str] = set()
    for format_ in formats:
        descriptions = format_.get("descriptions", "")
        if isinstance(descriptions, str):
            qualifiers.update(
                value.strip() for value in descriptions.split(",") if value.strip()
            )
        elif isinstance(descriptions, list):
            # Mirror JSON may carry ``"descriptions": null``; a bare ``else``
            # would iterate None. Preserve the parent's null-safe skip.
            qualifiers.update(
                value.strip() for value in descriptions if value.strip()
            )
    unofficial = "Unofficial Release" in qualifiers
    promo = "Promo" in qualifiers
    if unofficial and promo:
        return "Bootleg / Promo"
    if unofficial:
        return "Bootleg"
    if promo:
        return "Promotion"
    return "Official"


def get_master_releases(master_id: int) -> dict[str, object]:
    """Get all releases (pressings) for a master. Mirrors mb.get_release_group_releases()."""
    api_base = require_mirror_configured()

    def _fetch() -> dict[str, object]:
        data: _DiscogsMasterDetailJSON = _get(f"{api_base}/api/masters/{master_id}")
        releases: list[dict[str, object]] = []
        for r in data.get("releases", []):
            formats = r.get("formats", [])
            format_names = [f.get("name", "?") for f in formats]
            releases.append({
                "id": str(r.get("id", 0)),
                "title": r.get("title", data.get("title", "")),
                "date": r.get("released", ""),
                "country": r.get("country", ""),
                "status": _status_from_formats(formats),
                "track_count": r.get("track_count", 0),
                "format": ", ".join(format_names) if format_names else "?",
                "media_count": len(formats),
                "labels": r.get("labels", []),
            })
        return {
            "title": data.get("title", ""),
            "type": data.get("primary_type", ""),
            "first_release_date": data.get("first_release_date", ""),
            "artist_credit": data.get("artist_credit", ""),
            "primary_artist_id": str(data.get("primary_artist_id") or ""),
            "releases": releases,
        }

    return _cache.memoize_meta(f"discogs:master:v2:{master_id}", _fetch)


def get_release(release_id: int, *, fresh: bool = False) -> dict[str, object]:
    """Get full release details with tracks. Mirrors mb.get_release().

    `fresh=True` bypasses the cache. Used by POST handlers in
    `web/routes/pipeline.py` that persist this metadata into the
    pipeline DB — a 24h cache hit would silently write stale
    artist/title/track data into `album_requests` / `request_tracks`.
    """
    api_base = require_mirror_configured()

    def _fetch() -> dict[str, object]:
        data: _DiscogsReleaseDetailJSON = _get(f"{api_base}/api/releases/{release_id}")
        artists = data.get("artists", [])
        artist_name = _primary_artist_name(artists)
        artist_id = _primary_artist_id(artists)

        tracks: list[dict[str, object]] = []
        for track in data.get("tracks", []):
            disc, track_num = _parse_position(track.get("position", ""))
            tracks.append({
                "disc_number": disc,
                "track_number": track_num,
                "title": track.get("title", ""),
                "length_seconds": _parse_duration(track.get("duration", "")),
            })

        year = _parse_year(data.get("released", ""))

        return {
            "id": str(data.get("id", 0)),
            "title": data.get("title", ""),
            "artist_name": artist_name,
            "artist_id": str(artist_id) if artist_id else None,
            "release_group_id": str(data.get("master_id", "")) if data.get("master_id") else None,
            "date": data.get("released", ""),
            "year": year,
            "country": data.get("country", ""),
            "status": _status_from_formats(data.get("formats", [])),
            "tracks": tracks,
            "labels": data.get("labels", []),
            "formats": data.get("formats", []),
        }

    return _cache.memoize_meta(
        f"discogs:release:v2:{release_id}", _fetch, fresh=fresh)


def get_artist_name(artist_id: int) -> str:
    """Look up an artist's name by Discogs ID."""
    api_base = require_mirror_configured()

    def _fetch() -> str:
        data: _DiscogsArtistRefJSON = _get(f"{api_base}/api/artists/{artist_id}")
        return data.get("name", "")

    return _cache.memoize_meta(f"discogs:artist:{artist_id}:name", _fetch)


# ── Label adapter (U3) ──────────────────────────────────────────────────
#
# Wire-boundary types. The Rust mirror at `discogs.ablz.au` returns label
# search/detail/release JSON shaped exactly per these Structs (see the
# reference definitions in `discogs-api/src/types.rs`). They are decoded at
# the `_get` boundary via `msgspec.convert(...)` — any int-vs-str drift,
# missing required field, or wrong nested shape raises
# `msgspec.ValidationError` immediately. Per
# `.claude/rules/code-quality.md` § "Wire-boundary types": these are
# `msgspec.Struct`, not `@dataclass`. Strict ≠ coerce — declare the field
# as the type the wire actually carries.
#
# These are private to this module (leading underscore) — the public
# contract is `LabelEntity`, defined below, which is the source-agnostic
# shape consumed by the route layer (U4) and frontend.


class _DiscogsLabelHit(msgspec.Struct):
    """One hit from `GET /api/labels?name=...`."""
    id: int
    name: str
    profile: str | None
    parent_label_id: int | None
    parent_label_name: str | None
    release_count: int
    score: float


class _DiscogsLabelSearchResponse(msgspec.Struct):
    results: list[_DiscogsLabelHit]
    total: int
    page: int
    per_page: int


class _DiscogsSubLabel(msgspec.Struct):
    id: int
    name: str
    release_count: int


class _DiscogsLabelDetail(msgspec.Struct):
    """Response of `GET /api/labels/{id}`. Discogs labels have NO country
    column upstream — the `LabelEntity.country` field is `None` for the
    Discogs adapter; the future MB adapter will populate it."""
    id: int
    name: str
    profile: str | None
    contactinfo: str | None
    data_quality: str | None
    parent_label_id: int | None
    parent_label_name: str | None
    total_releases: int
    sub_labels: list[_DiscogsSubLabel]


class _DiscogsApiArtistCredit(msgspec.Struct):
    id: int
    name: str
    role: str
    anv: str


class _DiscogsApiLabel(msgspec.Struct):
    id: int
    name: str
    catno: str


class _DiscogsApiFormat(msgspec.Struct):
    name: str
    qty: int
    descriptions: str
    free_text: str


class _DiscogsLabelReleaseEntry(msgspec.Struct):
    """One release row from `/api/labels/{id}/releases`."""
    id: int
    title: str
    country: str
    released: str
    master_id: int | None
    primary_type: str
    artists: list[_DiscogsApiArtistCredit]
    labels: list[_DiscogsApiLabel]
    formats: list[_DiscogsApiFormat]
    # Rollout compatibility: Plan 004 renamed the wire field to `label_id`,
    # but older mirror deployments emit `via_label_id`. Accept both while
    # cratedigger and discogs-api can be deployed independently.
    label_id: int | None = None
    via_label_id: int | None = None
    # Optional fields (skipped on the Rust side when None) — declared with
    # a default so msgspec accepts payloads where the key is omitted entirely.
    master_title: str | None = None
    master_first_released: str | None = None
    sub_label_name: str | None = None


class _DiscogsLabelPagination(msgspec.Struct):
    page: int
    per_page: int
    pages: int
    items: int


class _DiscogsLabelReleasesResponse(msgspec.Struct):
    results: list[_DiscogsLabelReleaseEntry]
    pagination: _DiscogsLabelPagination
    include_sublabels: bool


def _label_release_label_id(row: _DiscogsLabelReleaseEntry) -> int:
    if row.label_id is not None:
        return row.label_id
    if row.via_label_id is not None:
        return row.via_label_id
    raise msgspec.ValidationError("label release row missing label_id/via_label_id")


# ── Source-agnostic public contract ─────────────────────────────────────


class LabelEntity(msgspec.Struct):
    """Cross-source label representation. Populated by the Discogs adapter
    today, and by the planned MusicBrainz adapter in Phase B.

    Field design notes:
    - `source` distinguishes downstream routing (e.g. "view label" links).
    - `id` is stringified at the adapter boundary so route/JSON callers
      see one consistent shape regardless of upstream id type
      (Discogs int, MB UUID).
    - `country` is `Optional` because the Discogs label table has no
      country column; MB labels do, so the field stays in the contract.
    - `parent_label_id` / `parent_label_name` are `Optional` to handle
      both top-level labels (no parent) and sub-labels.
    - `release_count` is the rolled-up count the upstream provided —
      Discogs reports `total_releases` on detail and `release_count` on
      search hits; both map here.
    """
    source: Literal["discogs", "musicbrainz"]
    id: str
    name: str
    country: str | None
    profile: str | None
    parent_label_id: str | None
    parent_label_name: str | None
    release_count: int
    sub_labels: list[dict[str, object]] = msgspec.field(default_factory=lambda: [])


def _label_entity_from_hit(hit: _DiscogsLabelHit) -> LabelEntity:
    return LabelEntity(
        source="discogs",
        id=str(hit.id),
        name=hit.name,
        country=None,  # Discogs label table has no country column
        profile=hit.profile or None,
        parent_label_id=str(hit.parent_label_id) if hit.parent_label_id is not None else None,
        parent_label_name=hit.parent_label_name,
        release_count=hit.release_count,
        sub_labels=[],
    )


def _label_entity_from_detail(detail: _DiscogsLabelDetail) -> LabelEntity:
    return LabelEntity(
        source="discogs",
        id=str(detail.id),
        name=detail.name,
        country=None,
        profile=detail.profile or None,
        parent_label_id=str(detail.parent_label_id) if detail.parent_label_id is not None else None,
        parent_label_name=detail.parent_label_name,
        release_count=detail.total_releases,
        sub_labels=[msgspec.to_builtins(s) for s in detail.sub_labels],
    )


def search_labels(query: str, *, page: int = 1, per_page: int = 25) -> list[LabelEntity]:
    """Search labels by name via `/api/labels?name=`.

    Decodes the response at the wire boundary into a typed
    `_DiscogsLabelSearchResponse`; any drift (e.g. `release_count`
    arriving as a string instead of int) raises
    `msgspec.ValidationError` from inside `_fetch`.

    Returns a list of `LabelEntity` (typed). The cache layer stores
    the dict form (via `msgspec.to_builtins`) so Redis JSON encoding
    stays lossless; on read we rehydrate via `msgspec.convert`. Per
    `.claude/rules/code-quality.md` § "Wire-boundary types".
    """
    api_base = require_mirror_configured()

    def _fetch() -> list[dict[str, object]]:
        q = urllib.parse.quote(query)
        raw = _get(
            f"{api_base}/api/labels"
            f"?name={q}&page={page}&per_page={per_page}"
        )
        decoded = msgspec.convert(raw, type=_DiscogsLabelSearchResponse)
        return [msgspec.to_builtins(_label_entity_from_hit(h))
                for h in decoded.results]

    cache_query = _search_cache_query_part(query)
    cache_key = f"discogs:search:labels:{cache_query}:p={page}:pp={per_page}"
    cached = _cache.memoize_meta(cache_key, _fetch)
    return [msgspec.convert(d, type=LabelEntity) for d in cached]


def get_label(label_id: int | str) -> LabelEntity:
    """Fetch a single label by Discogs ID via `/api/labels/{id}`.

    Mirrors `get_release` / `get_artist_name`: relies on `_get` raising
    HTTPError on 404 (the caller surfaces the 404 as needed). Returns
    a typed `LabelEntity` on success.
    """
    api_base = require_mirror_configured()
    label_id_str = _assert_discogs_label_id(label_id)

    def _fetch() -> dict[str, object]:
        raw = _get(f"{api_base}/api/labels/{label_id_str}")
        decoded = msgspec.convert(raw, type=_DiscogsLabelDetail)
        return msgspec.to_builtins(_label_entity_from_detail(decoded))

    cached = _cache.memoize_meta(f"discogs:label:v2:{label_id_str}", _fetch)
    return msgspec.convert(cached, type=LabelEntity)


def get_label_releases(label_id: int | str, *, include_sublabels: bool = True,
                       page: int = 1, per_page: int = 100) -> dict[str, object]:
    """Fetch a label's releases via `/api/labels/{id}/releases`.

    Returns a dict shaped to match the existing release-row contract
    used elsewhere in this module (`get_master_releases`,
    `get_artist_releases`) so the U4 route layer can overlay
    library/pipeline state with the same field names — `id` (str),
    `title`, `country`, `date` (released), `year` (parsed from date),
    `primary_type`, `release_group_id` (str | None), `artist_name`,
    `artist_id` (str | None), `format` (joined names) — plus
    label-specific fields `label_id` (str), `sub_label_name`
    (str | None), `master_title`, `master_first_released`, `labels`,
    `formats`.
    """
    api_base = require_mirror_configured()
    label_id_str = _assert_discogs_label_id(label_id)

    def _fetch() -> dict[str, object]:
        sub_flag = "true" if include_sublabels else "false"
        raw = _get(
            f"{api_base}/api/labels/{label_id_str}/releases"
            f"?include_sublabels={sub_flag}&page={page}&per_page={per_page}",
            timeout=(
                LABEL_RELEASES_INCLUDE_TIMEOUT_SECONDS
                if include_sublabels else DEFAULT_HTTP_TIMEOUT_SECONDS
            ),
        )
        decoded = msgspec.convert(raw, type=_DiscogsLabelReleasesResponse)
        rows: list[dict[str, object]] = []
        for r in decoded.results:
            artist_name = r.artists[0].name if r.artists else "Unknown"
            artist_id = r.artists[0].id if r.artists else None
            format_names = [f.name for f in r.formats]
            label_id_for_row = str(_label_release_label_id(r))
            rows.append({
                "id": str(r.id),
                "title": r.title,
                "country": r.country,
                "date": r.released,
                "year": _parse_year(r.released),
                "primary_type": r.primary_type,
                "release_group_id": str(r.master_id) if r.master_id else None,
                "master_title": r.master_title,
                "master_first_released": r.master_first_released,
                "artist_name": artist_name,
                "artist_id": str(artist_id) if artist_id is not None else None,
                "label_id": label_id_for_row,
                "via_label_id": label_id_for_row,
                "sub_label_name": r.sub_label_name,
                "format": ", ".join(format_names) if format_names else "?",
                "media_count": len(r.formats),
                "labels": [
                    {"id": lab.id, "name": lab.name, "catno": lab.catno}
                    for lab in r.labels
                ],
                "formats": [
                    {"name": f.name, "qty": f.qty,
                     "descriptions": f.descriptions, "free_text": f.free_text}
                    for f in r.formats
                ],
            })
        return {
            "results": rows,
            "pagination": {
                "page": decoded.pagination.page,
                "per_page": decoded.pagination.per_page,
                "pages": decoded.pagination.pages,
                "items": decoded.pagination.items,
            },
            "include_sublabels": decoded.include_sublabels,
            # Always present so the contract is stable. The 503-fallback
            # branch below overrides this to True; the wire payload itself
            # never carries a stale True (the upstream doesn't know).
            "sub_labels_dropped": False,
        }

    sub_flag = "true" if include_sublabels else "false"
    cache_key = (
        f"discogs:label:v2:{label_id_str}:releases"
        f":sub={sub_flag}:p={page}:pp={per_page}"
    )

    def _fallback_without_sublabels() -> dict[str, object]:
        fallback = get_label_releases(
            label_id, include_sublabels=False,
            page=page, per_page=per_page)
        # Don't mutate the fallback dict in place — it is the cached
        # value for a different cache key, and direct callers of
        # `include_sublabels=False` would see a false-positive
        # `sub_labels_dropped` if we wrote through.
        return {**fallback, "sub_labels_dropped": True}

    try:
        return _cache.memoize_meta(cache_key, _fetch)
    except urllib.error.HTTPError as e:
        # Plan 002 U3. The discogs-api mirror returns 503 when the
        # recursive sub-label CTE exceeds its statement_timeout (P0 plan).
        # Retry once with sub-labels disabled so the user sees the direct
        # catalogue rather than a hard error. The fallback uses its own
        # cache key (`sub=false`), so the direct-label fallback can be
        # reused while future full-rollup attempts still probe upstream.
        if e.code == 503 and include_sublabels:
            return _fallback_without_sublabels()
        raise
    except (TimeoutError, socket.timeout, urllib.error.URLError) as e:
        if include_sublabels and _is_timeout_error(e):
            return _fallback_without_sublabels()
        raise
