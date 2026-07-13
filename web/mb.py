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

import json
import urllib.parse
import urllib.request
import urllib.error

# Use the `web.` package-qualified path to keep the web metadata cache
# separate from the pipeline's peer-cache implementation.
from web import cache as _cache

# Default: public MusicBrainz (functional but rate-limited ~1 req/s).
# Production points this at the local mirror via the module's
# services.cratedigger.musicbrainz.apiBase -> config.ini [MusicBrainz]
# api_base -> configure_api_bases_from_runtime_config() at cratedigger-web
# startup (tier-2 plan U6, R13/KTD6; issue #497 dropped the module's
# --mb-api flag in favor of config.ini as the one production source — the
# flag itself survives as a dev-only override). The value includes the
# /ws/2 prefix.
MB_API_BASE = "https://musicbrainz.org/ws/2"
USER_AGENT = "cratedigger-web/1.0"

# Canonical Various Artists MBID. Used by the resolver and the browse-tab
# VA short-circuit (web/js/browse.js) to keep VA off the artist-view path
# (the MB artist→release-group endpoint takes ~23s for VA). Single
# declaration site at ``lib/va_identity.py`` — re-exported here so the
# existing ``from web.mb import VA_ARTIST_MBID`` imports keep working.
from lib.va_identity import (  # noqa: E402
    MB_VA_ARTIST_MBID as VA_ARTIST_MBID,
    split_va_query,
)


def _get(url):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Connection", "close")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError:
        # Retry once — MB mirror may have closed a keep-alive connection
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        req.add_header("Connection", "close")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())


def search_release_groups(query):
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

    def _fetch() -> list[dict]:
        q = urllib.parse.quote(query)
        data = _get(f"{MB_API_BASE}/release?query={q}&fmt=json&limit=25")
        seen_rg: set[str] = set()
        results = []
        for r in data.get("releases", []):
            rg = r.get("release-group", {})
            rg_id = rg.get("id", "")
            if not rg_id or rg_id in seen_rg:
                continue
            seen_rg.add(rg_id)
            artist_credit = r.get("artist-credit", [{}])
            artist = artist_credit[0].get("artist", {}) if artist_credit else {}
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


def search_artists(query):
    """Search for artists by name. Returns list of {id, name, disambiguation, score}."""
    def _fetch() -> list[dict]:
        q = urllib.parse.quote(query)
        data = _get(f"{MB_API_BASE}/artist?query={q}&fmt=json&limit=20")
        return [
            {
                "id": a["id"],
                "name": a.get("name", ""),
                "disambiguation": a.get("disambiguation", ""),
                "score": a.get("score", 0),
            }
            for a in data.get("artists", [])
        ]

    return _cache.memoize_meta(f"mb:search:artists:{query}", _fetch)


def _normalize_artist_release_group(
    rg: dict,
    *,
    is_appearance: bool,
) -> dict:
    """Shape direct and track-appearance MB rows into one artist-page contract."""
    ac = rg.get("artist-credit", [])
    credit_name = " / ".join(a.get("name", "?") for a in ac) if ac else ""
    primary_artist_id = ac[0].get("artist", {}).get("id") if ac else None
    return {
        "id": rg["id"],
        "title": rg.get("title", ""),
        "type": rg.get("primary-type", ""),
        "secondary_types": rg.get("secondary-types", []),
        "first_release_date": rg.get("first-release-date", ""),
        "artist_credit": credit_name,
        "primary_artist_id": primary_artist_id,
        "is_appearance": is_appearance,
    }


def get_artist_release_groups(artist_mbid):
    """Get directly credited release groups plus track-level appearances.

    MusicBrainz has no combined artist-discography endpoint. Direct work comes
    from the release-group artist browse; VA compilations and guest spots come
    from the release ``track_artist`` browse. Direct rows win deduplication so
    a release group is never downgraded merely because another pressing also
    contains an appearance.
    """
    def _fetch() -> list[dict]:
        entries: dict[str, dict] = {}
        offset = 0
        while True:
            data = _get(
                f"{MB_API_BASE}/release-group?artist={artist_mbid}"
                f"&inc=artist-credits&fmt=json&limit=100&offset={offset}"
            )
            for rg in data.get("release-groups", []):
                entry = _normalize_artist_release_group(
                    rg, is_appearance=False,
                )
                entries.setdefault(entry["id"], entry)
            total = data.get("release-group-count", 0)
            offset += 100
            if offset >= total:
                break

        offset = 0
        while True:
            data = _get(
                f"{MB_API_BASE}/release?track_artist={artist_mbid}"
                "&inc=release-groups+artist-credits"
                f"&fmt=json&limit=100&offset={offset}"
            )
            for release in data.get("releases", []):
                rg = release.get("release-group")
                if not isinstance(rg, dict) or not rg.get("id"):
                    continue
                entry = _normalize_artist_release_group(
                    rg, is_appearance=True,
                )
                entries.setdefault(entry["id"], entry)
            total = data.get("release-count", 0)
            offset += 100
            if offset >= total:
                break

        return sorted(
            entries.values(),
            key=lambda row: (
                row.get("first_release_date") or "",
                row.get("id") or "",
            ),
        )

    return _cache.memoize_meta(
        f"mb:artist:{artist_mbid}:release_groups:v2", _fetch,
    )


def get_official_release_group_ids(artist_mbid):
    """Get the set of release group IDs that have at least one official release."""
    # JSON cannot serialize a set, so we cache the sorted list and
    # rebuild the set on the caller's side. Callers use `x in` which
    # works on either, but set semantics are preserved here for clarity.
    def _fetch() -> list[str]:
        rg_ids: set[str] = set()
        offset = 0
        while True:
            data = _get(
                f"{MB_API_BASE}/release?artist={artist_mbid}"
                f"&status=official&inc=release-groups&fmt=json&limit=100&offset={offset}"
            )
            for r in data.get("releases", []):
                rg_id = r.get("release-group", {}).get("id")
                if rg_id:
                    rg_ids.add(rg_id)
            total = data.get("release-count", 0)
            offset += 100
            if offset >= total:
                break
        return sorted(rg_ids)

    cached = _cache.memoize_meta(
        f"mb:artist:{artist_mbid}:official_rg_ids", _fetch)
    return set(cached)


def get_release_group(rg_mbid):
    """Get release-group metadata + primary artist credit.

    Distinct from `get_release_group_releases` (which paginates child
    releases). The resolver (`web/routes/browse.py:resolve_id`) needs
    just the parent group's metadata + artist to render the artist-view
    drop-in target.
    """
    def _fetch() -> dict:
        data = _get(f"{MB_API_BASE}/release-group/{rg_mbid}?inc=artist-credits&fmt=json")
        ac = data.get("artist-credit", [{}])
        artist = ac[0].get("artist", {}) if ac else {}
        return {
            "id": data.get("id", ""),
            "title": data.get("title", ""),
            "type": data.get("primary-type", ""),
            "first_release_date": data.get("first-release-date", ""),
            "artist_id": artist.get("id", ""),
            "artist_name": artist.get("name", ""),
        }

    return _cache.memoize_meta(f"mb:release-group:{rg_mbid}:meta", _fetch)


def get_release_group_year(rg_mbid):
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
        data = _get(f"{MB_API_BASE}/release-group/{rg_mbid}?fmt=json")
        from lib.util import parse_mb_first_release_year
        return parse_mb_first_release_year(data)

    return _cache.memoize_meta(
        f"mb:release-group:{rg_mbid}:year", _fetch)


def get_release_group_releases(rg_mbid):
    """Get all releases for a release group. Returns list of release summaries."""
    def _fetch() -> dict:
        # First get the release group metadata
        rg_data = _get(f"{MB_API_BASE}/release-group/{rg_mbid}?fmt=json")

        # Then browse all releases (paginated — the lookup endpoint caps at 25)
        releases = []
        offset = 0
        while True:
            data = _get(
                f"{MB_API_BASE}/release?release-group={rg_mbid}"
                f"&inc=media&fmt=json&limit=100&offset={offset}"
            )
            for r in data.get("releases", []):
                track_count = sum(m.get("track-count", 0) for m in r.get("media", []))
                formats = [(m.get("format") or "?") for m in r.get("media", [])]
                releases.append({
                    "id": r["id"],
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


def get_release_raw(release_mbid, *, fresh: bool = False) -> dict:
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
    release-group primary-type) call this directly.
    """
    def _fetch() -> dict:
        return _get(
            f"{MB_API_BASE}/release/{release_mbid}"
            f"?inc=recordings+artist-credits+media+release-groups+labels&fmt=json"
        )
    return _cache.memoize_meta(
        f"mb:release_raw:{release_mbid}", _fetch, fresh=fresh)


def _strip_release(data: dict) -> dict:
    """Slim a raw MB release JSON down to the shape the frontend +
    pipeline DB inserts want. Pure function over `data`."""
    artist_credit = data.get("artist-credit", [{}])
    artist_name = artist_credit[0].get("name", "Unknown") if artist_credit else "Unknown"
    artist_id = (artist_credit[0].get("artist", {}).get("id") if artist_credit else None)
    rg_id = (data.get("release-group") or {}).get("id")

    tracks = []
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
    if data.get("date"):
        try:
            year = int(data["date"][:4])
        except (ValueError, IndexError):
            pass

    return {
        "id": data["id"],
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


def get_release(release_mbid, *, fresh: bool = False):
    """Get full release details with tracks (slimmed shape).

    `fresh=True` bypasses the cache. Used by POST handlers in
    `web/routes/pipeline.py` that persist this metadata into the
    pipeline DB — a 24h cache hit would silently write stale
    artist/title/track data into `album_requests` / `request_tracks`.

    Built on top of `get_release_raw` so the raw MB JSON is the single
    cached truth; this just re-derives the slim shape per call. The
    re-derivation is a pure traversal, ~microseconds.
    """
    raw = get_release_raw(release_mbid, fresh=fresh)
    return _strip_release(raw)


def get_artist_name(artist_mbid):
    """Look up an artist's name by MBID."""
    def _fetch() -> str:
        data = _get(f"{MB_API_BASE}/artist/{artist_mbid}?fmt=json")
        return data.get("name", "")

    return _cache.memoize_meta(f"mb:artist:{artist_mbid}:name", _fetch)


def get_artist_releases_with_recordings(artist_mbid):
    """Paginated fetch of all releases for an artist with recordings and release-group info.

    Returns raw MB release dicts with media[].tracks[].recording and release-group fields.
    """
    def _fetch() -> list[dict]:
        releases = []
        offset = 0
        while True:
            data = _get(
                f"{MB_API_BASE}/release?artist={artist_mbid}"
                f"&inc=recordings+media+release-groups&fmt=json&limit=100&offset={offset}"
            )
            page = data.get("releases", [])
            releases.extend(page)
            total = data.get("release-count", 0)
            offset += len(page)
            if not page or offset >= total:
                break
        return releases

    return _cache.memoize_meta(
        f"mb:artist:{artist_mbid}:releases_with_recordings", _fetch)
