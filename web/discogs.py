"""Discogs mirror API helpers — shared between pipeline_cli and web server.

All queries hit the local Discogs mirror at DISCOGS_API_BASE.
Response shapes are normalized to match what the frontend expects,
mirroring web/mb.py where possible.
"""

import json
import re
import urllib.parse
import urllib.request

DISCOGS_API_BASE = "https://discogs.ablz.au"
USER_AGENT = "soularr-web/1.0"


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


def search_artists(query: str) -> list[dict]:
    """Search for artists by name via the mirror's artist-name index.

    Uses /api/artists?name=, which is a real ts_rank artist-name search —
    parity with MB's /ws/2/artist?query=.
    """
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


def get_artist_releases(artist_id: int) -> list[dict]:
    """Get an artist's discography grouped by master. Mirrors mb.get_artist_release_groups().

    Uses /api/artists/{id}/masters which returns master-level entries with
    inferred type (Album/Single/EP/Other). Masterless releases come back with
    id "release-<n>" and is_masterless=True; we strip the prefix so the bare
    release ID is usable for downstream lookups.
    """
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


def get_master_releases(master_id: int) -> dict:
    """Get all releases (pressings) for a master. Mirrors mb.get_release_group_releases()."""
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


def get_release(release_id: int) -> dict:
    """Get full release details with tracks. Mirrors mb.get_release()."""
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


def get_artist_name(artist_id: int) -> str:
    """Look up an artist's name by Discogs ID."""
    data = _get(f"{DISCOGS_API_BASE}/api/artists/{artist_id}")
    return data.get("name", "")
