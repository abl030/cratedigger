"""Browse GET route handlers — MusicBrainz and Discogs.

MusicBrainz routes use UUID-based IDs (/api/artist/<uuid>, /api/release/<uuid>).
Discogs routes use numeric IDs (/api/discogs/artist/<int>, /api/discogs/release/<int>).
Both are enriched with library/pipeline status via check_beets_library() and check_pipeline().
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

from web import discogs as discogs_api
from lib.artist_compare import annotate_in_library, merge_discographies

if TYPE_CHECKING:
    from http.server import BaseHTTPRequestHandler


def _server():
    """Lazy import to avoid circular dependency with server.py.

    Returns the server module. All access to mb_api, _db(), _beets_db(),
    check_beets_library(), check_pipeline() goes through this so that
    test mocks on web.server.* are respected.
    """
    from web import server
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
    # Enrich with pipeline_status / pipeline_id / upgrade_queued so the
    # standardised action toolbar (Acquire / Remove from beets) shows
    # accurate per-row state. Also compute library_rank so the unified
    # badge renderer can colour the in-library badge by codec-aware tier.
    mbids = [str(a["mb_albumid"]) for a in albums if a.get("mb_albumid")]
    in_pipeline = srv.check_pipeline(mbids) if mbids else {}
    for a in albums:
        pi = in_pipeline.get(str(a.get("mb_albumid", "")))
        if pi:
            a["pipeline_status"] = pi["status"]
            a["pipeline_id"] = pi["id"]
            if pi.get("status") == "wanted" and (pi.get("search_filetype_override") or pi.get("target_format")):
                a["upgrade_queued"] = True
        else:
            a["pipeline_status"] = None
            a["pipeline_id"] = None
        # Codec-aware rank for the in-library badge.
        fmt = a.get("formats") or ""
        br_bps = a.get("min_bitrate") or 0
        kbps = (br_bps // 1000) if br_bps else 0
        a["library_rank"] = srv.compute_library_rank(fmt, kbps)
    h._json({"albums": albums})  # type: ignore[attr-defined]


def get_artist(h: BaseHTTPRequestHandler, params: dict[str, list[str]], artist_id: str) -> None:
    srv = _server()
    rgs = srv.mb_api.get_artist_release_groups(artist_id)
    official_rg_ids = srv.mb_api.get_official_release_group_ids(artist_id)
    for rg in rgs:
        rg["has_official"] = rg["id"] in official_rg_ids
    # Row-level in-library badge: requires the artist's library albums.
    # Frontend passes ?name= to avoid an extra MB lookup; without it the
    # name-fallback in get_albums_by_artist won't catch Discogs-tagged
    # rows but UUID-tagged ones still match. Backwards-compatible: name
    # is optional.
    name = params.get("name", [""])[0].strip()
    if name:
        lib = srv.get_library_artist(name, artist_id)
        annotate_in_library(rgs, [], lib, rank_fn=srv.compute_library_rank)
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

        # Look up beets album IDs + on-disk quality for in-library pressings
        lib_mbids = [p.release_id for p in rg.pressings if p.release_id in in_library]
        b = srv._beets_db()
        beets_ids = b.get_album_ids_by_mbids(lib_mbids) if lib_mbids and b else {}
        quality = b.check_mbids_detail(lib_mbids) if lib_mbids and b else {}

        # RG-level quality: pick the first in-library pressing's quality
        # so the disambiguate row badge shows on-disk format/rank too.
        rg_quality = None
        for rid in rg.release_ids:
            if rid in quality:
                rg_quality = quality[rid]
                break

        pressings_json = []
        for p in rg.pressings:
            p_lib = p.release_id in in_library
            p_pip = in_pipeline.get(p.release_id)
            pq = quality.get(p.release_id) or {}
            entry = {
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
            }
            if pq:
                entry["library_format"] = pq.get("beets_format") or ""
                entry["library_min_bitrate"] = pq.get("beets_bitrate") or 0
                entry["library_rank"] = srv.compute_library_rank(
                    entry["library_format"], entry["library_min_bitrate"])
            pressings_json.append(entry)

        rg_dict = {
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
        }
        if rg_quality:
            rg_dict["library_format"] = rg_quality.get("beets_format") or ""
            rg_dict["library_min_bitrate"] = rg_quality.get("beets_bitrate") or 0
            rg_dict["library_rank"] = srv.compute_library_rank(
                rg_dict["library_format"], rg_dict["library_min_bitrate"])
        rgs_json.append(rg_dict)

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
    # Map mbid -> beets album id + on-disk quality so the standard
    # toolbar (Remove from beets) and badge renderer (in library +
    # codec-aware rank) can render without extra round-trips.
    b = srv._beets_db()
    beets_ids = b.get_album_ids_by_mbids(list(in_library)) if in_library and b else {}
    quality = b.check_mbids_detail(list(in_library)) if in_library and b else {}
    for r in data["releases"]:
        rid = r["id"]
        r["in_library"] = rid in in_library
        r["beets_album_id"] = beets_ids.get(rid)
        q = quality.get(rid)
        if q:
            r["library_format"] = q.get("beets_format") or ""
            r["library_min_bitrate"] = q.get("beets_bitrate") or 0
            r["library_rank"] = srv.compute_library_rank(
                r["library_format"], r["library_min_bitrate"])
        pi = in_pipeline.get(rid)
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
    # Include beets track info + album id + on-disk quality if in library
    b = srv._beets_db()
    if data["in_library"] and b:
        beets_ids = b.get_album_ids_by_mbids([release_id])
        data["beets_album_id"] = beets_ids.get(release_id)
        quality = b.check_mbids_detail([release_id]).get(release_id) or {}
        data["library_format"] = quality.get("beets_format") or ""
        data["library_min_bitrate"] = quality.get("beets_bitrate") or 0
        data["library_rank"] = srv.compute_library_rank(
            data["library_format"], data["library_min_bitrate"])
        tracks = b.get_tracks_by_mb_release_id(release_id)
        if tracks is not None:
            data["beets_tracks"] = tracks
    else:
        data["beets_album_id"] = None
    h._json(data)  # type: ignore[attr-defined]


# ── Discogs route handlers ───────────────────────────────────────────


def get_discogs_search(h: BaseHTTPRequestHandler, params: dict[str, list[str]]) -> None:
    q = params.get("q", [""])[0].strip()
    if not q:
        h._error("Missing query parameter 'q'")  # type: ignore[attr-defined]
        return
    search_type = params.get("type", ["artist"])[0]
    if search_type == "release":
        results = discogs_api.search_releases(q)
        h._json({"release_groups": results})  # type: ignore[attr-defined]
    else:
        artists = discogs_api.search_artists(q)
        h._json({"artists": artists})  # type: ignore[attr-defined]


def get_discogs_artist(h: BaseHTTPRequestHandler, params: dict[str, list[str]], artist_id: str) -> None:
    srv = _server()
    artist_name = discogs_api.get_artist_name(int(artist_id))
    masters = discogs_api.get_artist_releases(int(artist_id))
    # Discogs has no bootleg/official distinction — mark all as official
    for m in masters:
        m["has_official"] = True
    # Row-level in-library badge: same pattern as MB. Frontend passes
    # ?name=; without it we still get the canonical name from Discogs API.
    name = params.get("name", [""])[0].strip() or artist_name
    if name:
        lib = srv.get_library_artist(name, "")
        annotate_in_library([], masters, lib, rank_fn=srv.compute_library_rank)
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
    b = srv._beets_db()
    beets_ids = b.get_album_ids_by_mbids(list(in_library)) if in_library and b else {}
    quality = b.check_mbids_detail(list(in_library)) if in_library and b else {}
    for r in data["releases"]:
        rid = r["id"]
        r["in_library"] = rid in in_library
        r["beets_album_id"] = beets_ids.get(rid)
        q = quality.get(rid)
        if q:
            r["library_format"] = q.get("beets_format") or ""
            r["library_min_bitrate"] = q.get("beets_bitrate") or 0
            r["library_rank"] = srv.compute_library_rank(
                r["library_format"], r["library_min_bitrate"])
        pi = in_pipeline.get(rid)
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
        beets_ids = b.get_album_ids_by_mbids([release_id])
        data["beets_album_id"] = beets_ids.get(release_id)
        quality = b.check_mbids_detail([release_id]).get(release_id) or {}
        data["library_format"] = quality.get("beets_format") or ""
        data["library_min_bitrate"] = quality.get("beets_bitrate") or 0
        data["library_rank"] = srv.compute_library_rank(
            data["library_format"], data["library_min_bitrate"])
        tracks = b.get_tracks_by_mb_release_id(release_id)
        if tracks is not None:
            data["beets_tracks"] = tracks
    else:
        data["beets_album_id"] = None
    h._json(data)  # type: ignore[attr-defined]


def get_artist_compare(h: BaseHTTPRequestHandler, params: dict[str, list[str]]) -> None:
    """Side-by-side discography from both MB and Discogs for one artist.

    Resolves both source artist IDs from the supplied name (and optional
    explicit IDs to skip the lookup), fetches each source's discography,
    and fuzzy-merges by title+year via lib.artist_compare.merge_discographies.

    Returns three buckets so the UI can show what each source uniquely
    contributes plus the matched-on-both core catalog.
    """
    srv = _server()
    name = params.get("name", [""])[0].strip()
    if not name:
        h._error("Missing parameter 'name'")  # type: ignore[attr-defined]
        return
    mbid = params.get("mbid", [""])[0].strip()
    discogs_id = params.get("discogs_id", [""])[0].strip()

    mb_artist: dict | None = None
    discogs_artist: dict | None = None

    if not mbid:
        hits = srv.mb_api.search_artists(name)
        for a in hits:
            if (a.get("name") or "").lower() == name.lower():
                mbid = a["id"]
                mb_artist = {"id": a["id"], "name": a["name"]}
                break
        if not mbid and hits:
            mbid = hits[0]["id"]
            mb_artist = {"id": hits[0]["id"], "name": hits[0]["name"]}
    else:
        mb_artist = {"id": mbid, "name": name}

    if not discogs_id:
        hits = discogs_api.search_artists(name)
        for a in hits:
            if (a.get("name") or "").lower() == name.lower():
                discogs_id = a["id"]
                discogs_artist = {"id": a["id"], "name": a["name"]}
                break
        if not discogs_id and hits:
            discogs_id = hits[0]["id"]
            discogs_artist = {"id": hits[0]["id"], "name": hits[0]["name"]}
    else:
        discogs_artist = {"id": discogs_id, "name": name}

    mb_groups: list[dict] = []
    if mbid:
        mb_groups = srv.mb_api.get_artist_release_groups(mbid)
        # Mark bootleg status on MB rows so the frontend can split them
        # into a Bootleg-only collapsible section like the Discography
        # sub-tab. Discogs has no official/bootleg concept in the CC0
        # dump, so Discogs-only rows are always treated as official.
        official_rg_ids = srv.mb_api.get_official_release_group_ids(mbid)
        for rg in mb_groups:
            rg["has_official"] = rg["id"] in official_rg_ids

    discogs_groups: list[dict] = []
    if discogs_id:
        discogs_groups = discogs_api.get_artist_releases(int(discogs_id))

    # Row-level in-library badge — same as Discography sub-tab. Beets
    # query is keyed by name with mbid for the UUID-match fast path.
    if name:
        lib = srv.get_library_artist(name, mbid)
        annotate_in_library(mb_groups, discogs_groups, lib, rank_fn=srv.compute_library_rank)

    merged = merge_discographies(mb_groups, discogs_groups)

    h._json({  # type: ignore[attr-defined]
        "mb_artist": mb_artist,
        "discogs_artist": discogs_artist,
        "both": merged.both,
        "mb_only": merged.mb_only,
        "discogs_only": merged.discogs_only,
    })


# ── Route tables ─────────────────────────────────────────────────────

GET_ROUTES: dict[str, object] = {
    "/api/search": get_search,
    "/api/library/artist": get_library_artist,
    "/api/artist/compare": get_artist_compare,
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
