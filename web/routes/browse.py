"""Browse GET route handlers — MusicBrainz and Discogs.

MusicBrainz routes use UUID-based IDs (/api/artist/<uuid>, /api/release/<uuid>).
Discogs routes use numeric IDs (/api/discogs/artist/<int>, /api/discogs/release/<int>).
Both are enriched with library/pipeline status via check_beets_library() and check_pipeline().
"""
from __future__ import annotations

import copy
import urllib.error

import msgspec

from lib.artist_catalogue import (
    ArtistCatalogueRow,
    ArtistCompareSkeleton,
)
from lib.release_identity import (
    ReleaseIdentity,
    normalize_release_id,
)
from web import cache as _cache
from web import discogs as discogs_api
from web.discogs import VA_ARTIST_ID as _DISCOGS_VA_ARTIST_ID
from web.mb import VA_ARTIST_MBID as _MB_VA_ARTIST_MBID
# VA constants are imported directly so that test patches of `discogs_api`
# (web.routes.browse.discogs_api) and `mb_api` (web.server.mb_api) don't
# replace the constants with auto-generated Mock attributes.
from lib.artist_compare import annotate_in_library, merge_discographies
from lib.banding import current_library_bitrate
from web.library_artist_service import list_library_artist_rows
from web.routes._overlay import overlay_release_rows_in_place
from web.routes._registry import (
    RouteHandler,
    RouteRegistration,
    pattern_route,
    route,
)
from web.routes._server_access import _server


def get_search(h: RouteHandler, params: dict[str, list[str]]) -> None:
    srv = _server()
    q = params.get("q", [""])[0].strip()
    if not q:
        h._error("Missing query parameter 'q'")
        return
    search_type = params.get("type", ["artist"])[0]
    if search_type == "release":
        results = srv.mb_api.search_release_groups(q)
        h._json({"release_groups": results})
    else:
        artists = srv.mb_api.search_artists(q)
        h._json({"artists": artists})


def get_library_artist(h: RouteHandler, params: dict[str, list[str]]) -> None:
    srv = _server()
    name = params.get("name", [""])[0].strip()
    mbid = params.get("mbid", [""])[0].strip()
    if not name:
        h._error("Missing parameter 'name'")
        return

    albums = list_library_artist_rows(
        library_lookup=srv,
        pipeline_db=srv._db(),
        artist_name=name,
        mb_artist_id=mbid,
        rank_fn=srv.compute_library_rank,
    )
    h._json({"albums": [row.to_dict() for row in albums]})


# Badge priority when several requests map to one release group — show
# the most active state.
_PIPELINE_BADGE_PRIORITY = {
    "downloading": 0, "wanted": 1, "unsearchable": 2, "imported": 3}


ArtistPipelineKey = tuple[str, str, str]
ArtistPipelineMap = dict[ArtistPipelineKey, dict]


def _artist_pipeline_map(name: str, mb_artist_id: str = "") -> ArtistPipelineMap:
    """Best non-replaced request keyed by source + identity unit + id.

    Discogs requests persist their exact master in ``mb_release_group_id``
    and exact leaf in ``discogs_release_id``. Keeping those namespaces in the
    key lets a work row receive its exact master overlay without allowing a
    numerically equal leaf release to badge it.
    """
    srv = _server()
    by_identity: ArtistPipelineMap = {}

    def keep_best(key: ArtistPipelineKey, hit: dict) -> None:
        current = by_identity.get(key)
        if current is None or hit["_prio"] < current["_prio"]:
            by_identity[key] = hit

    for row in srv.list_artist_requests(name, mb_artist_id):
        status = str(row["status"])
        if status == "replaced":
            continue
        prio = _PIPELINE_BADGE_PRIORITY.get(status, 9)
        hit = {"status": status, "id": row["id"], "_prio": prio}
        release_identity = ReleaseIdentity.from_fields(
            row.get("mb_release_id"), row.get("discogs_release_id"),
        )
        group_id = row.get("mb_release_group_id")
        source = (
            "discogs"
            if release_identity and release_identity.source == "discogs"
            else "mb"
        )
        if group_id:
            keep_best((source, "work", str(group_id)), hit)
        if release_identity:
            release_source = (
                "discogs" if release_identity.source == "discogs" else "mb"
            )
            keep_best(
                (release_source, "release", release_identity.release_id), hit,
            )
    return by_identity


def _apply_rg_pipeline_overlay(
    rows: list[ArtistCatalogueRow], by_identity: ArtistPipelineMap,
) -> None:
    """Badge rows only through an exact source/unit/id identity key."""
    for row in rows:
        hit = by_identity.get((row.source, row.identity_kind, row.id))
        if hit:
            row.pipeline_status = hit["status"]
            row.pipeline_id = hit["id"]


def get_artist(h: RouteHandler, params: dict[str, list[str]], artist_id: str) -> None:
    srv = _server()
    try:
        rgs = srv.mb_api.get_artist_release_groups(artist_id)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            status = 404
            payload = {
                "error": "MusicBrainz artist not found",
                "retryable": False,
            }
        elif exc.code == 429 or 500 <= exc.code <= 599:
            status = 503
            payload = {
                "error": "MusicBrainz fallback unavailable, retry",
                "retryable": True,
            }
        elif 400 <= exc.code <= 499:
            status = exc.code
            payload = {
                "error": "MusicBrainz request rejected",
                "retryable": False,
            }
        else:
            raise
        h._json(payload, status=status)
        return
    except urllib.error.URLError:
        h._json({
            "error": "MusicBrainz fallback unavailable, retry",
            "retryable": True,
        }, status=503)
        return
    # Row-level in-library badge: requires the artist's library albums.
    # Frontend passes ?name= to avoid an extra MB lookup; without it the
    # name-fallback in get_albums_by_artist won't catch Discogs-tagged
    # rows but UUID-tagged ones still match. Backwards-compatible: name
    # is optional.
    name = params.get("name", [""])[0].strip()
    if name:
        lib = srv.get_library_artist(name, artist_id)
        annotate_in_library(rgs, [], lib, rank_fn=srv.compute_library_rank)
        by_identity = _artist_pipeline_map(name, artist_id)
        _apply_rg_pipeline_overlay(rgs, by_identity)
    h._json({
        "release_groups": msgspec.to_builtins(rgs),
        "ungrouped_releases": [],
    })


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
                p["library_avg_bitrate"] = current_library_bitrate(pq)
                p["library_rank"] = srv.compute_library_rank(
                    p["library_format"], p["library_avg_bitrate"])

        if rg_quality:
            rg["library_format"] = rg_quality.get("beets_format") or ""
            rg["library_min_bitrate"] = rg_quality.get("beets_bitrate") or 0
            rg["library_avg_bitrate"] = current_library_bitrate(rg_quality)
            rg["library_rank"] = srv.compute_library_rank(
                rg["library_format"], rg["library_avg_bitrate"])

    return response


def get_artist_disambiguate(h: RouteHandler, params: dict[str, list[str]], artist_id: str) -> None:
    # Cache the pure-metadata skeleton (analyse_artist_releases output
    # serialized to JSON-safe dicts) under meta:. Overlay runs per
    # request — see issue #101 Codex round 3 for why the split matters.
    skeleton = _cache.memoize_meta(
        f"mb:artist:{artist_id}:disambiguate",
        lambda: _build_disambiguate_skeleton(artist_id),
    )
    h._json(_overlay_disambiguate(skeleton))


def get_release_group(h: RouteHandler, params: dict[str, list[str]], rg_id: str) -> None:
    srv = _server()
    normalized_id = normalize_release_id(rg_id) or rg_id.strip()
    identity = ReleaseIdentity.from_id(normalized_id)
    if identity and identity.source == "discogs":
        # A numeric id here is a Discogs master, not an MB release-group
        # UUID — dispatch server-side the same way get_release() forwards
        # numeric release ids to get_discogs_release(). get_master_releases
        # deliberately mirrors mb.get_release_group_releases()'s shape
        # (web/discogs.py), so get_discogs_master's overlay is the same
        # contract the frontend already reads for MB rows (#501 item 1).
        get_discogs_master(h, params, identity.release_id)
        return

    data = srv.mb_api.get_release_group_releases(normalized_id)
    # Standard toolbar (Remove from beets) and badge renderer (in library
    # + codec-aware rank) read these overlay fields per row, so route
    # them through the shared helper.
    overlay_release_rows_in_place(data["releases"], [r["id"] for r in data["releases"]])
    h._json(data)


def get_release(h: RouteHandler, params: dict[str, list[str]], release_id: str) -> None:
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
        avg_br = current_library_bitrate(quality)
        data["library_format"] = fmt
        data["library_min_bitrate"] = br
        data["library_avg_bitrate"] = avg_br
        data["library_rank"] = srv.compute_library_rank(fmt, avg_br)
        tracks = b.get_tracks_by_mb_release_id(normalized_id)
        if tracks is not None:
            data["beets_tracks"] = tracks
    else:
        data["beets_album_id"] = None
    h._json(data)


# ── Discogs route handlers ───────────────────────────────────────────


def get_discogs_search(h: RouteHandler, params: dict[str, list[str]]) -> None:
    q = params.get("q", [""])[0].strip()
    if not q:
        h._error("Missing query parameter 'q'")
        return
    search_type = params.get("type", ["artist"])[0]
    if search_type == "release":
        results = discogs_api.search_releases(q)
        h._json({"release_groups": results})
    else:
        artists = discogs_api.search_artists(q)
        h._json({"artists": artists})


def get_discogs_artist(h: RouteHandler, params: dict[str, list[str]], artist_id: str) -> None:
    srv = _server()
    artist_name = discogs_api.get_artist_name(int(artist_id))
    catalogue = discogs_api.get_artist_releases(int(artist_id))
    # Row-level in-library badge: same pattern as MB. Frontend passes
    # ?name=; without it we still get the canonical name from Discogs API.
    name = params.get("name", [""])[0].strip() or artist_name
    if name:
        lib = srv.get_library_artist(name, "")
        annotate_in_library([], catalogue, lib, rank_fn=srv.compute_library_rank)
        by_identity = _artist_pipeline_map(name)
        _apply_rg_pipeline_overlay(catalogue, by_identity)
    works = [row for row in catalogue if row.identity_kind == "work"]
    ungrouped = [row for row in catalogue if row.identity_kind == "release"]
    h._json({
        "artist_id": artist_id,
        "artist_name": artist_name,
        "release_groups": msgspec.to_builtins(works),
        "ungrouped_releases": msgspec.to_builtins(ungrouped),
    })


def get_discogs_master(h: RouteHandler, params: dict[str, list[str]], master_id: str) -> None:
    data = discogs_api.get_master_releases(int(master_id))
    overlay_release_rows_in_place(data["releases"], [r["id"] for r in data["releases"]])
    h._json(data)


def get_discogs_release(h: RouteHandler, params: dict[str, list[str]], release_id: str) -> None:
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
        avg_br = current_library_bitrate(quality)
        data["library_format"] = fmt
        data["library_min_bitrate"] = br
        data["library_avg_bitrate"] = avg_br
        data["library_rank"] = srv.compute_library_rank(fmt, avg_br)
        tracks = b.get_tracks_by_mb_release_id(normalized_id)
        if tracks is not None:
            data["beets_tracks"] = tracks
    else:
        data["beets_album_id"] = None
    h._json(data)


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


def _build_compare_skeleton(
    mbid: str, discogs_id: str,
) -> ArtistCompareSkeleton:
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
    mb_groups: list[ArtistCatalogueRow] = []
    if mbid:
        mb_groups = srv.mb_api.get_artist_release_groups(mbid)

    discogs_groups: list[ArtistCatalogueRow] = []
    if discogs_id:
        discogs_groups = discogs_api.get_artist_releases(int(discogs_id))

    merged = merge_discographies(mb_groups, discogs_groups)
    return ArtistCompareSkeleton(
        both=merged.both,
        mb_unpaired=merged.mb_unpaired,
        discogs_unpaired=merged.discogs_unpaired,
        discogs_ungrouped_releases=merged.discogs_ungrouped_releases,
    )


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


def _overlay_compare(
    skeleton: ArtistCompareSkeleton, name: str, mbid: str,
) -> ArtistCompareSkeleton:
    """Apply per-request `in_library` overlay to a cached compare
    skeleton. Returns a new struct — does not mutate the cached value.

    annotate_in_library mutates typed rows in place. We deep-copy the
    skeleton first so the cached value stays clean for the next request.
    """
    srv = _server()
    response = copy.deepcopy(skeleton)
    if not name:
        return response

    lib = srv.get_library_artist(name, mbid)

    # Reconstruct flat MB / Discogs lists that reference the row instances
    # inside each bucket, so annotate_in_library mutates them in place.
    mb_groups: list[ArtistCatalogueRow] = []
    discogs_groups: list[ArtistCatalogueRow] = []
    for pair in response.both:
        mb_groups.append(pair.mb)
        discogs_groups.append(pair.discogs)
    mb_groups.extend(response.mb_unpaired)
    discogs_groups.extend(response.discogs_unpaired)
    discogs_groups.extend(response.discogs_ungrouped_releases)

    annotate_in_library(mb_groups, discogs_groups, lib,
                        rank_fn=srv.compute_library_rank)
    by_identity = _artist_pipeline_map(name, mbid)
    _apply_rg_pipeline_overlay(mb_groups, by_identity)
    _apply_rg_pipeline_overlay(discogs_groups, by_identity)
    return response


def get_artist_compare(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """Side-by-side discography from both MB and Discogs for one artist.

    Resolves both source artist IDs from the supplied name (and optional
    explicit IDs to skip the lookup), fetches each source's discography,
    and conservatively pairs rows via lib.artist_compare.merge_discographies:
    normalized title and appearance provenance must agree, known structural
    Album/EP/Single evidence cannot conflict, and a one-year source-date
    difference is accepted only when both sources positively overlap on type.

    Returns internal association diagnostics while conserving every source
    identity. A paired Discogs row may be a master or a masterless release;
    the latter retains ``identity_kind='release'``. Unmatched masters and
    releases remain in their established wire buckets for conservation checks,
    but those buckets are not page taxonomy.

    Pure-metadata skeleton (both discographies + merge output) is cached
    under `meta:` — the expensive merge doesn't re-run on warm loads.
    The `in_library` overlay runs per-request on a deep-copied skeleton.
    """
    name = params.get("name", [""])[0].strip()
    if not name:
        h._error("Missing parameter 'name'")
        return
    discogs_api.require_mirror_configured()
    mbid = params.get("mbid", [""])[0].strip()
    discogs_id = params.get("discogs_id", [""])[0].strip()

    mbid, discogs_id = _resolve_compare_artist_ids(name, mbid, discogs_id)

    # Skeleton key is the resolved (mbid, discogs_id) pair — display
    # names are stamped on outside the cache from the canonical APIs.
    cache_key = f"artist:compare:v8:{mbid or 'none'}:{discogs_id or 'none'}"
    cached = _cache.memoize_meta(
        cache_key,
        lambda: msgspec.to_builtins(
            _build_compare_skeleton(mbid, discogs_id)
        ),
    )
    skeleton = msgspec.convert(cached, type=ArtistCompareSkeleton)
    response = _overlay_compare(skeleton, name, mbid)
    mb_artist, discogs_artist = _canonical_artist_labels(mbid, discogs_id)
    payload = msgspec.to_builtins(response)
    payload["mb_artist"] = mb_artist
    payload["discogs_artist"] = discogs_artist
    h._json(payload)


# ── Search-by-ID resolver ────────────────────────────────────────────

_RESOLVE_VALID_KINDS = {"release", "release-group", "master", "unknown"}


def _resolve_mb(srv, raw_id: str, kind: str) -> dict:
    """Resolve an MB UUID into the resolver response shape.

    Tries the leaf (release) endpoint first when kind ∈ {release, unknown}.
    Falls back to the group (release-group) endpoint only when kind=='unknown'
    and the leaf attempt 404s — kind=='release' explicit is honored and
    surfaces the 404 to the caller (the URL path said 'release', trust it).
    Raises HTTPError for the caller to translate to HTTP status.
    """
    if kind in ("release", "unknown"):
        try:
            data = srv.mb_api.get_release(raw_id)
            artist_id = data.get("artist_id") or ""
            return {
                "source": "mb",
                "kind": "release",
                "artist_id": artist_id,
                "artist_name": data.get("artist_name") or "",
                "is_va": artist_id == _MB_VA_ARTIST_MBID,
                "target_identity_kind": "work",
                "expand_id": data.get("release_group_id") or raw_id,
                "leaf_id": raw_id,
            }
        except urllib.error.HTTPError as e:
            if e.code != 404 or kind == "release":
                raise

    # kind == 'release-group' OR (kind=='unknown' and release attempt 404'd)
    rg = srv.mb_api.get_release_group(raw_id)
    artist_id = rg.get("artist_id") or ""
    return {
        "source": "mb",
        "kind": "release-group",
        "artist_id": artist_id,
        "artist_name": rg.get("artist_name") or "",
        "is_va": artist_id == _MB_VA_ARTIST_MBID,
        "target_identity_kind": "work",
        "expand_id": raw_id,
        "leaf_id": None,
    }


def _resolve_discogs(raw_id: str, kind: str) -> dict:
    """Resolve a Discogs numeric ID into the resolver response shape.

    Same leaf-first / group-fallback pattern as `_resolve_mb`.

    Caller is responsible for validating that `raw_id` parses as int —
    the route handler does this before dispatching. ValueError here
    indicates a programmer bug, not user input.
    """
    numeric = int(raw_id)
    if kind in ("release", "unknown"):
        try:
            data = discogs_api.get_release(numeric)
            # discogs_api.get_release returns artist_id and release_group_id as
            # str-or-None; release_group_id is master_id (None when masterless).
            artist_id = data.get("artist_id") or ""
            rg_id = data.get("release_group_id")
            # Masterless release: ring it in place — no parent master to
            # expand. Identity kind comes from nullability, never numeric
            # comparison: master 122 and grouped release 122 may coexist.
            is_masterless = rg_id is None
            expand_id = raw_id if is_masterless else str(rg_id)
            return {
                "source": "discogs",
                "kind": "release",
                "artist_id": artist_id,
                "artist_name": data.get("artist_name") or "",
                "is_va": artist_id == _DISCOGS_VA_ARTIST_ID,
                "target_identity_kind": (
                    "release" if is_masterless else "work"
                ),
                "expand_id": expand_id,
                "leaf_id": raw_id,
            }
        except urllib.error.HTTPError as e:
            if e.code != 404 or kind == "release":
                raise

    # kind == 'master' OR (kind=='unknown' and release attempt 404'd)
    master = discogs_api.get_master_releases(numeric)
    artist_id = master.get("primary_artist_id") or ""
    return {
        "source": "discogs",
        "kind": "master",
        "artist_id": artist_id,
        "artist_name": master.get("artist_credit") or "",
        "is_va": artist_id == _DISCOGS_VA_ARTIST_ID,
        "target_identity_kind": "work",
        "expand_id": raw_id,
        "leaf_id": None,
    }


def get_browse_resolve(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """Resolve a pasted MBID / Discogs ID / URL-extracted ID into the
    artist-view drop-in target. See docs/plans/2026-05-01-002-feat-search-by-id-plan.md.
    """
    srv = _server()
    raw_id = (params.get("id", [""])[0]).strip()
    source = (params.get("source", [""])[0]).strip()
    kind = (params.get("kind", ["unknown"])[0]).strip() or "unknown"

    if not raw_id:
        h._error("Missing 'id' parameter")
        return
    if source not in ("mb", "discogs"):
        h._error("Missing or invalid 'source' (must be 'mb' or 'discogs')")
        return
    if kind not in _RESOLVE_VALID_KINDS:
        h._error(f"Invalid 'kind' (must be one of {sorted(_RESOLVE_VALID_KINDS)})")
        return
    # Discogs IDs must be all-digit. Frontend parsePastedId already enforces
    # this, but defense-in-depth so the resolver never hits int() on garbage.
    if source == "discogs" and not raw_id.isdigit():
        h._error("Invalid Discogs ID (must be numeric)")
        return
    if source == "discogs":
        discogs_api.require_mirror_configured()

    cache_key = f"browse-resolve:v2:{source}:{kind}:{raw_id}"

    def _run() -> dict:
        if source == "mb":
            return _resolve_mb(srv, raw_id, kind)
        return _resolve_discogs(raw_id, kind)

    try:
        # 24h TTL via memoize_meta default — IDs are stable; rename incidents
        # are rare enough that staleness here doesn't justify a shorter TTL.
        result = _cache.memoize_meta(cache_key, _run)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            h._error("not_found", 404)
        else:
            h._error(f"upstream_error: HTTP {e.code}", 502)
        return
    except urllib.error.URLError as e:
        h._error(f"upstream_unreachable: {e}", 502)
        return

    h._json(result)


# ── Route tables ─────────────────────────────────────────────────────

ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/search", get_search,
        "MusicBrainz search by artist (default) or release group "
        "(type=release).",
        classified=True,
    ),
    route(
        "GET", "/api/browse/resolve", get_browse_resolve,
        "Resolve a pasted MBID / Discogs ID / URL into the artist-view "
        "drop-in target (source, kind, target_identity_kind, expand_id, "
        "leaf_id).",
        classified=True,
    ),
    route(
        "GET", "/api/library/artist", get_library_artist,
        "Library albums by artist (beets-backed), pipeline-status enriched.",
        classified=True,
    ),
    route(
        "GET", "/api/artist/compare", get_artist_compare,
        "Side-by-side MB + Discogs discographies and track appearances for "
        "one artist, fuzzy-merged with in-library overlay.",
        classified=True,
    ),
    route(
        "GET", "/api/discogs/search", get_discogs_search,
        "Discogs search by artist (default) or release (type=release).",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/artist/([a-f0-9-]+)$", get_artist,
        "MB artist detail — direct release groups plus track appearances "
        "with library/pipeline overlay.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/artist/([a-f0-9-]+)/disambiguate$",
        get_artist_disambiguate,
        "MB artist disambiguate view — per-release-group pressing analysis "
        "with in-library + pipeline overlay.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/release-group/([a-f0-9-]+)$", get_release_group,
        "MB release group detail (auto-routes to the Discogs master "
        "endpoint for numeric IDs) — releases in this group with overlay.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/release/([a-f0-9-]+)$", get_release,
        "MB release detail (auto-routes to Discogs for numeric IDs); "
        "library + pipeline status and beets tracks if present.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/discogs/artist/(\d+)$", get_discogs_artist,
        "Discogs artist detail — masters with in-library overlay.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/discogs/master/(\d+)$", get_discogs_master,
        "Discogs master detail — releases under this master with overlay.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/discogs/release/(\d+)$", get_discogs_release,
        "Discogs release detail — library + pipeline status and beets "
        "tracks if present.",
        classified=True,
    ),
]
