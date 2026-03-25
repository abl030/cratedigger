"""MusicBrainz API helpers — shared between pipeline_cli and web server.

All queries hit the local MB mirror at MB_API_BASE.
"""

import json
import urllib.request
import urllib.error

MB_API_BASE = "http://192.168.1.35:5200/ws/2"
USER_AGENT = "soularr-web/1.0"


def _get(url):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", USER_AGENT)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def search_artists(query):
    """Search for artists by name. Returns list of {id, name, disambiguation, score}."""
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


def get_artist_release_groups(artist_mbid):
    """Get all release groups for an artist. Returns list of {id, title, type, first_release_date}."""
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
            results.append({
                "id": rg["id"],
                "title": rg.get("title", ""),
                "type": rg.get("primary-type", ""),
                "secondary_types": rg.get("secondary-types", []),
                "first_release_date": rg.get("first-release-date", ""),
                "artist_credit": credit_name,
            })
        total = data.get("release-group-count", 0)
        offset += 100
        if offset >= total:
            break
    return results


def get_official_release_group_ids(artist_mbid):
    """Get the set of release group IDs that have at least one official release."""
    rg_ids = set()
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
    return rg_ids


def get_release_group_releases(rg_mbid):
    """Get all releases for a release group. Returns list of release summaries."""
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


def get_release(release_mbid):
    """Get full release details with tracks."""
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


# Keep urllib.parse available for the quote() call above
import urllib.parse  # noqa: E402
