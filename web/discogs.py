"""Discogs mirror API helpers — shared between pipeline_cli and web server.

All queries hit the local Discogs mirror at DISCOGS_API_BASE.
Response shapes are normalized to match what the frontend expects,
mirroring web/mb.py where possible.

Pure-metadata responses are memoized via `cache.memoize_meta()` at
24h TTL. See web/mb.py and web/cache.py for rationale — the cache
layer sits at the API helper level, not at the HTTP routing level,
so that per-user pipeline / library overlay state is never baked
into Redis (issue #101).
"""

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Literal

import msgspec

from web import cache as _cache

DISCOGS_API_BASE = "https://discogs.ablz.au"
USER_AGENT = "cratedigger-web/1.0"


def _get(url: str) -> dict:
    req = urllib.request.Request(url)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Connection", "close")
    # Single-word release searches against ~19M rows can take 15-30s on the
    # mirror; the request always succeeds eventually. Generous timeout so the
    # web UI doesn't 500 on broad queries (use the in-flight Redis cache to
    # short-circuit repeats).
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


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


def _primary_artist_name(artists: list[dict]) -> str:
    """Extract the display artist name from a Discogs artists array."""
    if not artists:
        return "Unknown"
    return artists[0].get("name", "Unknown")


def _primary_artist_id(artists: list[dict]) -> int | None:
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


def search_releases(query: str) -> list[dict]:
    """Search releases by query string. Returns list of release summaries grouped by master.

    Deduplicates by master_id (like MB's release-group dedup) and surfaces
    master-level metadata (master_title, master_first_released, primary_type, score)
    that the mirror provides on each search hit.
    """
    def _fetch() -> list[dict]:
        q = urllib.parse.quote(query)
        data = _get(f"{DISCOGS_API_BASE}/api/search?title={q}&per_page=25")
        seen_master: set[int] = set()
        results = []
        for r in data.get("results", []):
            master_id = r.get("master_id")
            artists = r.get("artists", [])
            if master_id and master_id in seen_master:
                continue
            if master_id:
                seen_master.add(master_id)
            title = r.get("master_title") or r.get("title", "") if master_id else r.get("title", "")
            first_released = r.get("master_first_released") or r.get("released", "") if master_id else r.get("released", "")
            results.append({
                "id": str(master_id) if master_id else str(r["id"]),
                "title": title,
                "primary_type": r.get("primary_type", ""),
                "first_release_date": first_released,
                "artist_id": str(_primary_artist_id(artists) or ""),
                "artist_name": _primary_artist_name(artists),
                "artist_disambiguation": "",
                "score": int(r.get("score", 0) * 100),
                "is_master": bool(master_id),
                "discogs_release_id": str(r["id"]),
            })
        return results

    return _cache.memoize_meta(f"discogs:search:releases:{query}", _fetch)


def search_artists(query: str) -> list[dict]:
    """Search for artists by name via the mirror's artist-name index.

    Uses /api/artists?name=, which is a real ts_rank artist-name search —
    parity with MB's /ws/2/artist?query=.
    """
    def _fetch() -> list[dict]:
        q = urllib.parse.quote(query)
        data = _get(f"{DISCOGS_API_BASE}/api/artists?name={q}&per_page=20")
        return [
            {
                "id": str(r["id"]),
                "name": r.get("name", ""),
                "disambiguation": "",
                "score": int(r.get("score", 0) * 100),
            }
            for r in data.get("results", [])
        ]

    return _cache.memoize_meta(f"discogs:search:artists:{query}", _fetch)


def get_artist_releases(artist_id: int) -> list[dict]:
    """Get an artist's discography grouped by master. Mirrors mb.get_artist_release_groups().

    Uses /api/artists/{id}/masters which returns master-level entries with
    inferred type (Album/Single/EP/Other). Masterless releases come back with
    id "release-<n>" and is_masterless=True; we strip the prefix so the bare
    release ID is usable for downstream lookups.
    """
    def _fetch() -> list[dict]:
        entries: list[dict] = []
        page = 1
        while True:
            data = _get(
                f"{DISCOGS_API_BASE}/api/artists/{artist_id}/masters?per_page=100&page={page}"
            )
            results = data.get("results", [])
            if not results:
                break
            for r in results:
                raw_id = r.get("id")
                is_masterless = bool(r.get("is_masterless"))
                if is_masterless and isinstance(raw_id, str) and raw_id.startswith("release-"):
                    bare_id = raw_id[len("release-"):]
                    entry = {
                        "id": bare_id,
                        "title": r.get("title", ""),
                        "type": r.get("type", ""),
                        "secondary_types": [],
                        "first_release_date": r.get("first_release_date", ""),
                        "artist_credit": r.get("artist_credit", ""),
                        "primary_artist_id": str(r.get("primary_artist_id") or ""),
                        "is_masterless": True,
                        "discogs_release_id": bare_id,
                    }
                else:
                    entry = {
                        "id": str(raw_id),
                        "title": r.get("title", ""),
                        "type": r.get("type", ""),
                        "secondary_types": [],
                        "first_release_date": r.get("first_release_date", ""),
                        "artist_credit": r.get("artist_credit", ""),
                        "primary_artist_id": str(r.get("primary_artist_id") or ""),
                    }
                entries.append(entry)
            total = data.get("total", 0)
            if page * data.get("per_page", 100) >= total:
                break
            if len(entries) >= 500:
                break
            page += 1
        return entries

    return _cache.memoize_meta(f"discogs:artist:{artist_id}:releases", _fetch)


def get_master_releases(master_id: int) -> dict:
    """Get all releases (pressings) for a master. Mirrors mb.get_release_group_releases()."""
    def _fetch() -> dict:
        data = _get(f"{DISCOGS_API_BASE}/api/masters/{master_id}")
        releases = []
        for r in data.get("releases", []):
            formats = r.get("formats", [])
            format_names = [f.get("name", "?") for f in formats]
            releases.append({
                "id": str(r["id"]),
                "title": r.get("title", data.get("title", "")),
                "date": r.get("released", ""),
                "country": r.get("country", ""),
                "status": "Official",
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

    return _cache.memoize_meta(f"discogs:master:{master_id}", _fetch)


def get_release(release_id: int, *, fresh: bool = False) -> dict:
    """Get full release details with tracks. Mirrors mb.get_release().

    `fresh=True` bypasses the cache. Used by POST handlers in
    `web/routes/pipeline.py` that persist this metadata into the
    pipeline DB — a 24h cache hit would silently write stale
    artist/title/track data into `album_requests` / `request_tracks`.
    """
    def _fetch() -> dict:
        data = _get(f"{DISCOGS_API_BASE}/api/releases/{release_id}")
        artists = data.get("artists", [])
        artist_name = _primary_artist_name(artists)
        artist_id = _primary_artist_id(artists)

        tracks = []
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
            "id": str(data["id"]),
            "title": data.get("title", ""),
            "artist_name": artist_name,
            "artist_id": str(artist_id) if artist_id else None,
            "release_group_id": str(data.get("master_id", "")) if data.get("master_id") else None,
            "date": data.get("released", ""),
            "year": year,
            "country": data.get("country", ""),
            "status": "Official",
            "tracks": tracks,
            "labels": data.get("labels", []),
            "formats": data.get("formats", []),
        }

    return _cache.memoize_meta(
        f"discogs:release:{release_id}", _fetch, fresh=fresh)


def get_artist_name(artist_id: int) -> str:
    """Look up an artist's name by Discogs ID."""
    def _fetch() -> str:
        data = _get(f"{DISCOGS_API_BASE}/api/artists/{artist_id}")
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
    profile: str
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
    profile: str
    contactinfo: str
    data_quality: str
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
    via_label_id: int
    artists: list[_DiscogsApiArtistCredit]
    labels: list[_DiscogsApiLabel]
    formats: list[_DiscogsApiFormat]
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
    def _fetch() -> list[dict]:
        q = urllib.parse.quote(query)
        raw = _get(
            f"{DISCOGS_API_BASE}/api/labels"
            f"?name={q}&page={page}&per_page={per_page}"
        )
        decoded = msgspec.convert(raw, type=_DiscogsLabelSearchResponse)
        return [msgspec.to_builtins(_label_entity_from_hit(h))
                for h in decoded.results]

    cache_key = f"discogs:search:labels:{query}:p={page}:pp={per_page}"
    cached = _cache.memoize_meta(cache_key, _fetch)
    return [msgspec.convert(d, type=LabelEntity) for d in cached]


def get_label(label_id: int | str) -> LabelEntity:
    """Fetch a single label by Discogs ID via `/api/labels/{id}`.

    Mirrors `get_release` / `get_artist_name`: relies on `_get` raising
    HTTPError on 404 (the caller surfaces the 404 as needed). Returns
    a typed `LabelEntity` on success.
    """
    def _fetch() -> dict:
        raw = _get(f"{DISCOGS_API_BASE}/api/labels/{label_id}")
        decoded = msgspec.convert(raw, type=_DiscogsLabelDetail)
        return msgspec.to_builtins(_label_entity_from_detail(decoded))

    cached = _cache.memoize_meta(f"discogs:label:{label_id}", _fetch)
    return msgspec.convert(cached, type=LabelEntity)


def get_label_releases(label_id: int | str, *, include_sublabels: bool = True,
                       page: int = 1, per_page: int = 100) -> dict:
    """Fetch a label's releases via `/api/labels/{id}/releases`.

    Returns a dict shaped to match the existing release-row contract
    used elsewhere in this module (`get_master_releases`,
    `get_artist_releases`) so the U4 route layer can overlay
    library/pipeline state with the same field names — `id` (str),
    `title`, `country`, `date` (released), `year` (parsed from date),
    `primary_type`, `release_group_id` (str | None), `artist_name`,
    `artist_id` (str | None), `format` (joined names) — plus
    label-specific fields `via_label_id` (str), `sub_label_name`
    (str | None), `master_title`, `master_first_released`, `labels`,
    `formats`.
    """
    def _fetch() -> dict:
        sub_flag = "true" if include_sublabels else "false"
        raw = _get(
            f"{DISCOGS_API_BASE}/api/labels/{label_id}/releases"
            f"?include_sublabels={sub_flag}&page={page}&per_page={per_page}"
        )
        decoded = msgspec.convert(raw, type=_DiscogsLabelReleasesResponse)
        rows: list[dict] = []
        for r in decoded.results:
            artist_name = r.artists[0].name if r.artists else "Unknown"
            artist_id = r.artists[0].id if r.artists else None
            format_names = [f.name for f in r.formats]
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
                "via_label_id": str(r.via_label_id),
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
        f"discogs:label:{label_id}:releases"
        f":sub={sub_flag}:p={page}:pp={per_page}"
    )
    try:
        return _cache.memoize_meta(cache_key, _fetch)
    except urllib.error.HTTPError as e:
        # Plan 003 U4. The discogs-api mirror returns 503 when the
        # recursive sub-label CTE exceeds its statement_timeout (P0 plan).
        # Retry once with sub-labels disabled so the user sees the direct
        # catalogue rather than a hard error. The fallback uses its own
        # cache key (`sub=false`), so a successful retry is memoized
        # independently — repeat hits skip the failing call entirely.
        if e.code == 503 and include_sublabels:
            fallback = get_label_releases(
                label_id, include_sublabels=False,
                page=page, per_page=per_page)
            # Don't mutate the fallback dict in place — it is the cached
            # value for a different cache key, and direct callers of
            # `include_sublabels=False` would see a false-positive
            # `sub_labels_dropped` if we wrote through.
            return {**fallback, "sub_labels_dropped": True}
        raise
