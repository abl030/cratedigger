"""Browse/MusicBrainz GET route handlers extracted from server.py."""
from __future__ import annotations

import os
import re
import sys
from typing import TYPE_CHECKING

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "lib"))

import mb as mb_api  # noqa: E402
import discogs as discogs_api  # noqa: E402

if TYPE_CHECKING:
    from http.server import BaseHTTPRequestHandler


def _server():
    """Lazy import to avoid circular dependency with server.py.

    Returns the server module. All access to mb_api, _db(), _beets_db(),
    check_beets_library(), check_pipeline() goes through this so that
    test mocks on web.server.* are respected.
    """
    from web import server  # type: ignore[import-not-found]
    return server


def get_search(h: BaseHTTPRequestHandler, params: dict[str, list[str]]) -> None:
    srv = _server()
    q = params.get("q", [""])[0].strip()
    if not q:
        h._error("Missing query parameter 'q'")  # type: ignore[attr-defined]
        return
    search_type = params.get("type", ["artist"])[0]
    if search_type == "release":
        results = srv.mb_api.search_release_groups(q)
        h._json({"release_groups": results})  # type: ignore[attr-defined]
    else:
        artists = srv.mb_api.search_artists(q)
        h._json({"artists": artists})  # type: ignore[attr-defined]


def get_library_artist(h: BaseHTTPRequestHandler, params: dict[str, list[str]]) -> None:
    srv = _server()
    name = params.get("name", [""])[0].strip()
    mbid = params.get("mbid", [""])[0].strip()
    if not name:
        h._error("Missing parameter 'name'")  # type: ignore[attr-defined]
        return
    albums = srv.get_library_artist(name, mbid)
    h._json({"albums": albums})  # type: ignore[attr-defined]


def get_artist(h: BaseHTTPRequestHandler, params: dict[str, list[str]], artist_id: str) -> None:
    srv = _server()
    rgs = srv.mb_api.get_artist_release_groups(artist_id)
    official_rg_ids = srv.mb_api.get_official_release_group_ids(artist_id)
    for rg in rgs:
        rg["has_official"] = rg["id"] in official_rg_ids
    h._json({"release_groups": rgs})  # type: ignore[attr-defined]


def get_artist_disambiguate(h: BaseHTTPRequestHandler, params: dict[str, list[str]], artist_id: str) -> None:
    srv = _server()
    from lib.artist_releases import (
        filter_non_live,
        analyse_artist_releases,
    )

    raw_releases = srv.mb_api.get_artist_releases_with_recordings(artist_id)
    filtered = filter_non_live(raw_releases)
    rg_infos = analyse_artist_releases(filtered)

    # Cross-reference library and pipeline status using all release IDs
    all_mbids: list[str] = []
    for rg in rg_infos:
        all_mbids.extend(rg.release_ids)
    in_library = srv.check_beets_library(all_mbids) if all_mbids else set()
    in_pipeline = srv.check_pipeline(all_mbids) if all_mbids else {}

    rgs_json: list[dict] = []
    for rg in rg_infos:
        # A release group is "in library" if ANY pressing is
        lib_status = "in_library" if any(rid in in_library for rid in rg.release_ids) else None
        # Pipeline status: find the first pressing that's in the pipeline
        pip_status: str | None = None
        pip_id: int | None = None
        for rid in rg.release_ids:
            pip = in_pipeline.get(rid)
            if pip:
                pip_status = pip["status"]
                pip_id = pip["id"]
                break

        # Look up beets album IDs for in-library pressings
        lib_mbids = [p.release_id for p in rg.pressings if p.release_id in in_library]
        b = srv._beets_db()
        beets_ids = b.get_album_ids_by_mbids(lib_mbids) if lib_mbids and b else {}

        pressings_json = []
        for p in rg.pressings:
            p_lib = p.release_id in in_library
            p_pip = in_pipeline.get(p.release_id)
            pressings_json.append({
                "release_id": p.release_id,
                "title": p.title,
                "date": p.date,
                "format": p.format,
                "track_count": p.track_count,
                "country": p.country,
                "recording_ids": p.recording_ids,
                "in_library": p_lib,
                "beets_album_id": beets_ids.get(p.release_id),
                "pipeline_status": p_pip["status"] if p_pip else None,
                "pipeline_id": p_pip["id"] if p_pip else None,
            })

        rgs_json.append({
            "release_group_id": rg.release_group_id,
            "title": rg.title,
            "primary_type": rg.primary_type,
            "first_date": rg.first_date,
            "release_ids": rg.release_ids,
            "pressings": pressings_json,
            "track_count": rg.track_count,
            "unique_track_count": rg.unique_track_count,
            "covered_by": rg.covered_by,
            "library_status": lib_status,
            "pipeline_status": pip_status,
            "pipeline_id": pip_id,
            "tracks": [
                {
                    "recording_id": t.recording_id,
                    "title": t.title,
                    "unique": t.unique,
                    "also_on": t.also_on,
                }
                for t in rg.tracks
            ],
        })

    artist_name = srv.mb_api.get_artist_name(artist_id)
    h._json({  # type: ignore[attr-defined]
        "artist_id": artist_id,
        "artist_name": artist_name,
        "release_groups": rgs_json,
    })


def get_release_group(h: BaseHTTPRequestHandler, params: dict[str, list[str]], rg_id: str) -> None:
    srv = _server()
    data = srv.mb_api.get_release_group_releases(rg_id)
    # Check which releases are in pipeline/library
    mbids = [r["id"] for r in data["releases"]]
    in_library = srv.check_beets_library(mbids)
    in_pipeline = srv.check_pipeline(mbids)
    for r in data["releases"]:
        r["in_library"] = r["id"] in in_library
        pi = in_pipeline.get(r["id"])
        r["pipeline_status"] = pi["status"] if pi else None
        r["pipeline_id"] = pi["id"] if pi else None
    h._json(data)  # type: ignore[attr-defined]


def get_release(h: BaseHTTPRequestHandler, params: dict[str, list[str]], release_id: str) -> None:
    srv = _server()
    data = srv.mb_api.get_release(release_id)
    data["in_library"] = bool(srv.check_beets_library([release_id]))
    req = srv._db().get_request_by_mb_release_id(release_id)
    data["pipeline_status"] = req["status"] if req else None
    data["pipeline_id"] = req["id"] if req else None
    # Include beets track info if in library
    b = srv._beets_db()
    if data["in_library"] and b:
        tracks = b.get_tracks_by_mb_release_id(release_id)
        if tracks is not None:
            data["beets_tracks"] = tracks
    h._json(data)  # type: ignore[attr-defined]


# ── Discogs route handlers ───────────────────────────────────────────


def get_discogs_search(h: BaseHTTPRequestHandler, params: dict[str, list[str]]) -> None:
    q = params.get("q", [""])[0].strip()
    if not q:
        h._error("Missing query parameter 'q'")  # type: ignore[attr-defined]
        return
    search_type = params.get("type", ["release"])[0]
    if search_type == "artist":
        artists = discogs_api.search_artists(q)
        h._json({"artists": artists})  # type: ignore[attr-defined]
    else:
        results = discogs_api.search_releases(q)
        h._json({"release_groups": results})  # type: ignore[attr-defined]


def get_discogs_artist(h: BaseHTTPRequestHandler, params: dict[str, list[str]], artist_id: str) -> None:
    srv = _server()
    artist_name = discogs_api.get_artist_name(int(artist_id))
    masters = discogs_api.get_artist_masters(artist_name)
    # Enrich with library/pipeline status via master IDs as release group equivalents
    h._json({  # type: ignore[attr-defined]
        "artist_id": artist_id,
        "artist_name": artist_name,
        "release_groups": masters,
    })


def get_discogs_master(h: BaseHTTPRequestHandler, params: dict[str, list[str]], master_id: str) -> None:
    srv = _server()
    data = discogs_api.get_master_releases(int(master_id))
    # Enrich releases with pipeline/library status
    release_ids = [r["id"] for r in data["releases"]]
    in_library = srv.check_beets_library(release_ids)
    in_pipeline = srv.check_pipeline(release_ids)
    for r in data["releases"]:
        r["in_library"] = r["id"] in in_library
        pi = in_pipeline.get(r["id"])
        r["pipeline_status"] = pi["status"] if pi else None
        r["pipeline_id"] = pi["id"] if pi else None
    h._json(data)  # type: ignore[attr-defined]


def get_discogs_release(h: BaseHTTPRequestHandler, params: dict[str, list[str]], release_id: str) -> None:
    srv = _server()
    data = discogs_api.get_release(int(release_id))
    data["in_library"] = bool(srv.check_beets_library([release_id]))
    req = srv._db().get_request_by_mb_release_id(release_id)
    if not req:
        req = srv._db().get_request_by_discogs_release_id(release_id)
    data["pipeline_status"] = req["status"] if req else None
    data["pipeline_id"] = req["id"] if req else None
    b = srv._beets_db()
    if data["in_library"] and b:
        tracks = b.get_tracks_by_mb_release_id(release_id)
        if tracks is not None:
            data["beets_tracks"] = tracks
    h._json(data)  # type: ignore[attr-defined]


# ── Route tables ─────────────────────────────────────────────────────

GET_ROUTES: dict[str, object] = {
    "/api/search": get_search,
    "/api/library/artist": get_library_artist,
    "/api/discogs/search": get_discogs_search,
}

GET_PATTERNS: list[tuple[re.Pattern[str], object]] = [
    (re.compile(r"^/api/artist/([a-f0-9-]+)$"), get_artist),
    (re.compile(r"^/api/artist/([a-f0-9-]+)/disambiguate$"), get_artist_disambiguate),
    (re.compile(r"^/api/release-group/([a-f0-9-]+)$"), get_release_group),
    (re.compile(r"^/api/release/([a-f0-9-]+)$"), get_release),
    (re.compile(r"^/api/discogs/artist/(\d+)$"), get_discogs_artist),
    (re.compile(r"^/api/discogs/master/(\d+)$"), get_discogs_master),
    (re.compile(r"^/api/discogs/release/(\d+)$"), get_discogs_release),
]
