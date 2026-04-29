"""Browse GET route handlers — MusicBrainz and Discogs.

MusicBrainz routes use UUID-based IDs (/api/artist/<uuid>, /api/release/<uuid>).
Discogs routes use numeric IDs (/api/discogs/artist/<int>, /api/discogs/release/<int>).
Both are enriched with library/pipeline status via check_beets_library() and check_pipeline().
"""
from __future__ import annotations

import copy
import re
from typing import TYPE_CHECKING

from lib.release_identity import (
    ReleaseIdentity,
    normalize_release_id,
)
from web import cache as _cache
from web import discogs as discogs_api
from lib.artist_compare import annotate_in_library, merge_discographies
from web.library_artist_service import list_library_artist_rows
from web.routes._overlay import overlay_release_rows_in_place

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

    albums = list_library_artist_rows(
        library_lookup=srv,
        pipeline_db=srv.db,
        artist_name=name,
        mb_artist_id=mbid,
        rank_fn=srv.compute_library_rank,
    )
    h._json({"albums": [row.to_dict() for row in albums]})  # type: ignore[attr-defined]


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


def _build_disambiguate_skeleton(artist_id: str) -> dict:
    """Pure-metadata skeleton of the disambiguate response (no overlay).

    Runs the expensive `analyse_artist_releases` pass on cached MB
    metadata and returns a JSON-serializable dict. Callers cache this
    under `meta:` and then layer pipeline / library state on top per-
    request. The analysis is a pure function of pure-metadata inputs,
    so its output is semantically part of the metadata cache.
    """
    srv = _server()
    from lib.artist_releases import (  # local to avoid heavy import at route-load
        filter_non_live,
        analyse_artist_releases,
    )

    raw_releases = srv.mb_api.get_artist_releases_with_recordings(artist_id)
    filtered = filter_non_live(raw_releases)
    rg_infos = analyse_artist_releases(filtered)

    rgs_skeleton: list[dict] = []
    for rg in rg_infos:
        rgs_skeleton.append({
            "release_group_id": rg.release_group_id,
            "title": rg.title,
            "primary_type": rg.primary_type,
            "first_date": rg.first_date,
            "release_ids": list(rg.release_ids),
            "pressings": [
                {
                    "release_id": p.release_id,
                    "title": p.title,
                    "date": p.date,
                    "format": p.format,
                    "track_count": p.track_count,
                    "country": p.country,
                    "recording_ids": list(p.recording_ids),
                }
                for p in rg.pressings
            ],
            "track_count": rg.track_count,
            "unique_track_count": rg.unique_track_count,
            "covered_by": rg.covered_by,
            "tracks": [
                {
                    "recording_id": t.recording_id,
                    "title": t.title,
                    "unique": t.unique,
                    "also_on": list(t.also_on),
                }
                for t in rg.tracks
            ],
        })

    return {
        "artist_id": artist_id,
        "artist_name": srv.mb_api.get_artist_name(artist_id),
        "release_groups": rgs_skeleton,
    }


def _overlay_disambiguate(skeleton: dict) -> dict:
    """Apply per-request pipeline / library overlay to the cached
    skeleton. Returns a new dict — does NOT mutate the cached value."""
    srv = _server()
    response = copy.deepcopy(skeleton)
    b = srv._beets_db()

    all_mbids: list[str] = []
    for rg in response["release_groups"]:
        all_mbids.extend(rg["release_ids"])
    in_library = srv.check_beets_library(all_mbids) if all_mbids else set()
    in_pipeline = srv.check_pipeline(all_mbids) if all_mbids else {}

    for rg in response["release_groups"]:
        rg["library_status"] = (
            "in_library"
            if any(rid in in_library for rid in rg["release_ids"])
            else None
        )
        rg_pip_status: str | None = None
        rg_pip_id: int | None = None
        for rid in rg["release_ids"]:
            pip = in_pipeline.get(rid)
            if pip:
                rg_pip_status = pip["status"]
                rg_pip_id = pip["id"]
                break
        rg["pipeline_status"] = rg_pip_status
        rg["pipeline_id"] = rg_pip_id

        lib_mbids = [p["release_id"] for p in rg["pressings"]
                     if p["release_id"] in in_library]
        beets_ids = b.get_album_ids_by_mbids(lib_mbids) if lib_mbids and b else {}
        quality = b.check_mbids_detail(lib_mbids) if lib_mbids and b else {}

        rg_quality = None
        for rid in rg["release_ids"]:
            if rid in quality:
                rg_quality = quality[rid]
                break

        for p in rg["pressings"]:
            rid = p["release_id"]
            p["in_library"] = rid in in_library
            p["beets_album_id"] = beets_ids.get(rid)
            p_pip = in_pipeline.get(rid)
            p["pipeline_status"] = p_pip["status"] if p_pip else None
            p["pipeline_id"] = p_pip["id"] if p_pip else None
            pq = quality.get(rid) or {}
            if pq:
                p["library_format"] = pq.get("beets_format") or ""
                p["library_min_bitrate"] = pq.get("beets_bitrate") or 0
                p["library_rank"] = srv.compute_library_rank(
                    p["library_format"], p["library_min_bitrate"])

        if rg_quality:
            rg["library_format"] = rg_quality.get("beets_format") or ""
            rg["library_min_bitrate"] = rg_quality.get("beets_bitrate") or 0
            rg["library_rank"] = srv.compute_library_rank(
                rg["library_format"], rg["library_min_bitrate"])

    return response


def get_artist_disambiguate(h: BaseHTTPRequestHandler, params: dict[str, list[str]], artist_id: str) -> None:
    # Cache the pure-metadata skeleton (analyse_artist_releases output
    # serialized to JSON-safe dicts) under meta:. Overlay runs per
    # request — see issue #101 Codex round 3 for why the split matters.
    skeleton = _cache.memoize_meta(
        f"mb:artist:{artist_id}:disambiguate",
        lambda: _build_disambiguate_skeleton(artist_id),
    )
    h._json(_overlay_disambiguate(skeleton))  # type: ignore[attr-defined]


def get_release_group(h: BaseHTTPRequestHandler, params: dict[str, list[str]], rg_id: str) -> None:
    srv = _server()
    data = srv.mb_api.get_release_group_releases(rg_id)
    # Standard toolbar (Remove from beets) and badge renderer (in library
    # + codec-aware rank) read these overlay fields per row, so route
    # them through the shared helper.
    overlay_release_rows_in_place(data["releases"], [r["id"] for r in data["releases"]])
    h._json(data)  # type: ignore[attr-defined]


def get_release(h: BaseHTTPRequestHandler, params: dict[str, list[str]], release_id: str) -> None:
    srv = _server()
    normalized_id = normalize_release_id(release_id) or release_id.strip()
    identity = ReleaseIdentity.from_id(normalized_id)
    if identity and identity.source == "discogs":
        get_discogs_release(h, params, identity.release_id)
        return

    data = srv.mb_api.get_release(normalized_id)
    data["in_library"] = bool(srv.check_beets_library([normalized_id]))
    req = srv._db().get_request_by_release_id(normalized_id)
    data["pipeline_status"] = req["status"] if req else None
    data["pipeline_id"] = req["id"] if req else None
    # Include beets track info + album id + on-disk quality if in library
    b = srv._beets_db()
    if data["in_library"] and b:
        beets_ids = b.get_album_ids_by_mbids([normalized_id])
        data["beets_album_id"] = beets_ids.get(normalized_id)
        quality = b.check_mbids_detail([normalized_id]).get(normalized_id) or {}
        fmt_raw = quality.get("beets_format")
        fmt = fmt_raw if isinstance(fmt_raw, str) else ""
        br_raw = quality.get("beets_bitrate")
        br = br_raw if isinstance(br_raw, int) else 0
        data["library_format"] = fmt
        data["library_min_bitrate"] = br
        data["library_rank"] = srv.compute_library_rank(fmt, br)
        tracks = b.get_tracks_by_mb_release_id(normalized_id)
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
    data = discogs_api.get_master_releases(int(master_id))
    overlay_release_rows_in_place(data["releases"], [r["id"] for r in data["releases"]])
    h._json(data)  # type: ignore[attr-defined]


def get_discogs_release(h: BaseHTTPRequestHandler, params: dict[str, list[str]], release_id: str) -> None:
    srv = _server()
    normalized_id = normalize_release_id(release_id) or release_id.strip()
    data = discogs_api.get_release(int(normalized_id))
    data["in_library"] = bool(srv.check_beets_library([normalized_id]))
    req = srv._db().get_request_by_release_id(normalized_id)
    data["pipeline_status"] = req["status"] if req else None
    data["pipeline_id"] = req["id"] if req else None
    b = srv._beets_db()
    if data["in_library"] and b:
        beets_ids = b.get_album_ids_by_mbids([normalized_id])
        data["beets_album_id"] = beets_ids.get(normalized_id)
        quality = b.check_mbids_detail([normalized_id]).get(normalized_id) or {}
        fmt_raw = quality.get("beets_format")
        fmt = fmt_raw if isinstance(fmt_raw, str) else ""
        br_raw = quality.get("beets_bitrate")
        br = br_raw if isinstance(br_raw, int) else 0
        data["library_format"] = fmt
        data["library_min_bitrate"] = br
        data["library_rank"] = srv.compute_library_rank(fmt, br)
        tracks = b.get_tracks_by_mb_release_id(normalized_id)
        if tracks is not None:
            data["beets_tracks"] = tracks
    else:
        data["beets_album_id"] = None
    h._json(data)  # type: ignore[attr-defined]


def _resolve_compare_artist_ids(name: str, mbid: str,
                                discogs_id: str) -> tuple[str, str]:
    """Resolve MB / Discogs artist IDs from `name` when not passed
    explicitly. Returns the (mbid, discogs_id) pair. Display names
    are resolved separately from the canonical APIs — keeping them
    out of the cache key means a `?name=` typo doesn't produce a
    different cache entry for the same underlying artist pair."""
    srv = _server()
    if not mbid:
        hits = srv.mb_api.search_artists(name)
        for a in hits:
            if (a.get("name") or "").lower() == name.lower():
                mbid = a["id"]
                break
        if not mbid and hits:
            mbid = hits[0]["id"]

    if not discogs_id:
        hits = discogs_api.search_artists(name)
        for a in hits:
            if (a.get("name") or "").lower() == name.lower():
                discogs_id = a["id"]
                break
        if not discogs_id and hits:
            discogs_id = hits[0]["id"]

    return mbid, discogs_id


def _build_compare_skeleton(mbid: str, discogs_id: str) -> dict:
    """Pure-metadata compare skeleton — no in_library overlay and
    deliberately no artist labels either.

    Display names (`mb_artist`, `discogs_artist`) are resolved from the
    canonical MB / Discogs helpers in `_canonical_artist_labels()`,
    outside this cached value. Codex round 4 on PR #104 flagged that
    baking the request's `?name=` into the cache meant a typo on the
    first request served for the next 24h.

    Safe to cache under `meta:` — the output depends only on the
    resolved `(mbid, discogs_id)` pair and pure-metadata inputs.
    """
    srv = _server()
    mb_groups: list[dict] = []
    if mbid:
        mb_groups = srv.mb_api.get_artist_release_groups(mbid)
        official_rg_ids = srv.mb_api.get_official_release_group_ids(mbid)
        for rg in mb_groups:
            rg["has_official"] = rg["id"] in official_rg_ids

    discogs_groups: list[dict] = []
    if discogs_id:
        discogs_groups = discogs_api.get_artist_releases(int(discogs_id))

    merged = merge_discographies(mb_groups, discogs_groups)
    return {
        "both": merged.both,
        "mb_only": merged.mb_only,
        "discogs_only": merged.discogs_only,
    }


def _canonical_artist_labels(mbid: str, discogs_id: str) -> tuple[
        dict | None, dict | None]:
    """Resolve `{id, name}` for each source from the canonical API
    helpers. Names come back from `mb_api.get_artist_name` /
    `discogs_api.get_artist_name`, which are themselves memoized in
    `meta:`, so this is cheap — and it guarantees the display name is
    the same across requests regardless of whatever `?name=` spelling
    a given client happened to use.
    """
    srv = _server()
    mb_artist: dict | None = None
    if mbid:
        mb_artist = {"id": mbid, "name": srv.mb_api.get_artist_name(mbid) or ""}
    discogs_artist: dict | None = None
    if discogs_id:
        discogs_artist = {
            "id": discogs_id,
            "name": discogs_api.get_artist_name(int(discogs_id)) or "",
        }
    return mb_artist, discogs_artist


def _overlay_compare(skeleton: dict, name: str, mbid: str) -> dict:
    """Apply per-request `in_library` overlay to a cached compare
    skeleton. Returns a new dict — does not mutate the cached value.

    annotate_in_library mutates row dicts in place. We deep-copy the
    skeleton first so the cached dict stays clean for the next request.
    """
    srv = _server()
    response = copy.deepcopy(skeleton)
    if not name:
        return response

    lib = srv.get_library_artist(name, mbid)

    # Reconstruct flat mb_groups / discogs_groups lists that reference
    # the dict instances inside the three buckets, so annotate_in_library
    # mutates them in place (the 'both' bucket holds pairs, not flat rows).
    mb_groups: list[dict] = []
    discogs_groups: list[dict] = []
    for pair in response["both"]:
        if isinstance(pair.get("mb"), dict):
            mb_groups.append(pair["mb"])
        if isinstance(pair.get("discogs"), dict):
            discogs_groups.append(pair["discogs"])
    mb_groups.extend(response["mb_only"])
    discogs_groups.extend(response["discogs_only"])

    annotate_in_library(mb_groups, discogs_groups, lib,
                        rank_fn=srv.compute_library_rank)
    return response


def get_artist_compare(h: BaseHTTPRequestHandler, params: dict[str, list[str]]) -> None:
    """Side-by-side discography from both MB and Discogs for one artist.

    Resolves both source artist IDs from the supplied name (and optional
    explicit IDs to skip the lookup), fetches each source's discography,
    and fuzzy-merges by title+year via lib.artist_compare.merge_discographies.

    Returns three buckets so the UI can show what each source uniquely
    contributes plus the matched-on-both core catalog.

    Pure-metadata skeleton (both discographies + merge output) is cached
    under `meta:` — the expensive merge doesn't re-run on warm loads.
    The `in_library` overlay runs per-request on a deep-copied skeleton.
    """
    name = params.get("name", [""])[0].strip()
    if not name:
        h._error("Missing parameter 'name'")  # type: ignore[attr-defined]
        return
    mbid = params.get("mbid", [""])[0].strip()
    discogs_id = params.get("discogs_id", [""])[0].strip()

    mbid, discogs_id = _resolve_compare_artist_ids(name, mbid, discogs_id)

    # Skeleton key is the resolved (mbid, discogs_id) pair — display
    # names are stamped on outside the cache from the canonical APIs.
    cache_key = f"artist:compare:{mbid or 'none'}:{discogs_id or 'none'}"
    skeleton = _cache.memoize_meta(
        cache_key,
        lambda: _build_compare_skeleton(mbid, discogs_id),
    )
    response = _overlay_compare(skeleton, name, mbid)
    mb_artist, discogs_artist = _canonical_artist_labels(mbid, discogs_id)
    response["mb_artist"] = mb_artist
    response["discogs_artist"] = discogs_artist
    h._json(response)  # type: ignore[attr-defined]


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
