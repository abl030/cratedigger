"""MusicBrainz API helpers — shared between pipeline_cli and web server.

All queries hit the local MB mirror at MB_API_BASE. The pure-metadata
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

# Disambiguate from lib/cache.py (per-user folder cache). Use the
# `web.` package-qualified path so pyright resolves to web/cache.py,
# and so there's no ambiguity with `lib/cache.py` (which pyright sees
# via `extraPaths: ["lib", ...]` in pyrightconfig.json).
from web import cache as _cache  # type: ignore[import-not-found]

MB_API_BASE = "http://192.168.1.35:5200/ws/2"
USER_AGENT = "soularr-web/1.0"


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
    """
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


def get_artist_release_groups(artist_mbid):
    """Get all release groups for an artist. Returns list of {id, title, type, first_release_date}."""
    def _fetch() -> list[dict]:
        results = []
        offset = 0
        while True:
            data = _get(
                f"{MB_API_BASE}/release-group?artist={artist_mbid}"
                f"&inc=artist-credits&fmt=json&limit=100&offset={offset}"
            )
            for rg in data.get("release-groups", []):
                ac = rg.get("artist-credit", [])
                credit_name = " / ".join(a.get("name", "?") for a in ac) if ac else ""
                # Extract primary artist ID from credit for reliable own-work detection
                primary_artist_id = ac[0].get("artist", {}).get("id") if ac else None
                results.append({
                    "id": rg["id"],
                    "title": rg.get("title", ""),
                    "type": rg.get("primary-type", ""),
                    "secondary_types": rg.get("secondary-types", []),
                    "first_release_date": rg.get("first-release-date", ""),
                    "artist_credit": credit_name,
                    "primary_artist_id": primary_artist_id,
                })
            total = data.get("release-group-count", 0)
            offset += 100
            if offset >= total:
                break
        return results

    return _cache.memoize_meta(f"mb:artist:{artist_mbid}:release_groups", _fetch)


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


def get_release(release_mbid, *, fresh: bool = False):
    """Get full release details with tracks.

    `fresh=True` bypasses the cache. Used by POST handlers in
    `web/routes/pipeline.py` that persist this metadata into the
    pipeline DB — a 24h cache hit would silently write stale
    artist/title/track data into `album_requests` / `request_tracks`.
    """
    def _fetch() -> dict:
        data = _get(
            f"{MB_API_BASE}/release/{release_mbid}"
            f"?inc=recordings+artist-credits+media&fmt=json"
        )
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

    return _cache.memoize_meta(
        f"mb:release:{release_mbid}", _fetch, fresh=fresh)


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
