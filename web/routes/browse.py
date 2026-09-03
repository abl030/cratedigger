"""Browse GET route handlers — MusicBrainz and Discogs.

MusicBrainz routes use UUID-based IDs (/api/artist/<uuid>, /api/release/<uuid>).
Discogs routes use numeric IDs (/api/discogs/artist/<int>, /api/discogs/release/<int>).
Both are enriched with library/pipeline status via check_beets_library() and check_pipeline().
"""
from __future__ import annotations

import copy
import urllib.error
import uuid
from typing import NotRequired, TypedDict, TypeGuard

import msgspec

from lib.artist_catalogue import (
    ArtistCatalogueRow,
    ArtistCompareSkeleton,
)

# VA constants are imported directly so that test patches of the two
# mirror modules (web.routes.browse.discogs_api, web.routes.browse.mb_api)
# don't replace the constants with auto-generated Mock attributes.
from lib.artist_compare import annotate_in_library, merge_discographies
from lib.banding import current_library_bitrate
from lib.json_narrow import is_object_list, is_str_object_dict
from lib.pipeline_db._shared import ProcessingOwnerProjection
from lib.release_identity import (
    ReleaseIdentity,
    normalize_release_id,
)
from web import cache as _cache
from web import discogs as discogs_api
from web import mb as mb_api
from web.discogs import VA_ARTIST_ID as _DISCOGS_VA_ARTIST_ID
from web.library_album_row import AmbiguousLibraryRequestAttachmentError
from web.library_artist_service import list_library_artist_rows
from web.mb import VA_ARTIST_MBID as _MB_VA_ARTIST_MBID
from web.overlay import compute_library_rank
from web.parallel_fanout import parallel_results
from web.routes._overlay import overlay_release_rows_in_place
from web.routes._registry import (
    RouteHandler,
    RouteRegistration,
    pattern_route,
    route,
)
from web.runtime import runtime


def get_search(h: RouteHandler, params: dict[str, list[str]]) -> None:
    q = params.get("q", [""])[0].strip()
    if not q:
        h._error("Missing query parameter 'q'")
        return
    search_type = params.get("type", ["artist"])[0]
    if search_type == "release":
        results = mb_api.search_release_groups(q)
        h._json({"release_groups": results})
    else:
        artists = mb_api.search_artists(q)
        h._json({"artists": artists})


def get_library_artist(h: RouteHandler, params: dict[str, list[str]]) -> None:
    rt = runtime()
    name = params.get("name", [""])[0].strip()
    mbid = params.get("mbid", [""])[0].strip()
    if not name:
        h._error("Missing parameter 'name'")
        return

    try:
        albums = list_library_artist_rows(
            library_lookup=rt,
            pipeline_db=rt.db(),
            artist_name=name,
            mb_artist_id=mbid,
            rank_fn=compute_library_rank,
        )
    except AmbiguousLibraryRequestAttachmentError as exc:
        h._json({
            "error": "ambiguous_library_request_identity",
            "request_ids": list(exc.request_ids),
            "release_ids": [
                identity.release_id for identity in exc.identities
            ],
        }, status=409)
        return
    h._json({"albums": [row.to_dict() for row in albums]})


# Badge priority when several requests map to one release group — show
# the most active state.
_PIPELINE_BADGE_PRIORITY = {
    "processing": 0,
    "downloading": 1,
    "wanted": 2,
    "unsearchable": 3,
    "imported": 4,
    "replaced": 5,
}


ArtistPipelineKey = tuple[str, str, str]


def _is_canonical_mbid(value: str) -> bool:
    """Whether a resolver-supplied MusicBrainz ID has canonical UUID form."""
    try:
        return len(value) == 36 and str(uuid.UUID(value)) == value
    except ValueError:
        return False


class _PipelineHit(TypedDict):
    """One badge-worthy request hit — status, request id, and its badge
    priority (lower wins) for `keep_best`'s tie-break."""

    status: str
    id: int
    processing_owner: dict[str, object] | None
    has_captured_history: bool
    verified_lossless: bool
    provisional_lossless: bool
    _prio: int


ArtistPipelineMap = dict[ArtistPipelineKey, _PipelineHit]


def _catalogue_payload(value: object) -> object:
    """Serialize catalogue structs with an explicit nullable owner field."""
    payload: object = msgspec.to_builtins(value)

    def stamp(node: object) -> None:
        if is_str_object_dict(node):
            if "source" in node and "identity_kind" in node:
                node.setdefault("processing_owner", None)
                node.setdefault("has_captured_history", False)
                node.setdefault("pipeline_verified_lossless", False)
                node.setdefault("pipeline_provisional", False)
            for child in node.values():
                stamp(child)
        elif is_object_list(node):
            for child in node:
                stamp(child)

    stamp(payload)
    return payload


def _artist_pipeline_map(name: str, mb_artist_id: str = "") -> ArtistPipelineMap:
    """Best exact request, including frozen history, keyed by identity.

    Discogs requests persist their exact master in ``mb_release_group_id``
    and exact leaf in ``discogs_release_id``. Keeping those namespaces in the
    key lets a work row receive its exact master overlay without allowing a
    numerically equal leaf release to badge it.
    """
    rt = runtime()
    by_identity: ArtistPipelineMap = {}

    def keep_best(key: ArtistPipelineKey, hit: _PipelineHit) -> None:
        current = by_identity.get(key)
        if current is None or hit["_prio"] < current["_prio"]:
            by_identity[key] = hit

    for row in rt.list_artist_requests(name, mb_artist_id):
        status = str(row["status"])
        prio = _PIPELINE_BADGE_PRIORITY.get(status, 9)
        owner = dict(row).get("processing_owner")
        hit: _PipelineHit = {
            "status": status,
            "id": row["id"],
            "processing_owner": owner if isinstance(owner, dict) else None,
            "has_captured_history": row["has_captured_history"],
            "verified_lossless": bool(row["verified_lossless"]),
            "provisional_lossless": row["provisional_lossless"],
            "_prio": prio,
        }
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
            owner = hit["processing_owner"]
            row.processing_owner = (
                msgspec.convert(owner, type=ProcessingOwnerProjection)
                if owner is not None
                else None
            )
            if row.identity_kind == "release":
                row.has_captured_history = hit["has_captured_history"]
                row.pipeline_verified_lossless = hit["verified_lossless"]
                row.pipeline_provisional = hit["provisional_lossless"]


def get_artist(h: RouteHandler, params: dict[str, list[str]], artist_id: str) -> None:
    try:
        rgs = mb_api.get_artist_release_groups(artist_id)
        name = params.get("name", [""])[0].strip()
        if not name:
            name = mb_api.get_artist_name(artist_id).strip()
        if not name:
            raise ValueError("MusicBrainz artist response has no name")
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
    # Artist metadata chooses the page; current Beets and pipeline authority
    # always annotate it. ``?name=`` avoids one MB lookup but never changes
    # whether overlays are projected.
    by_identity = _artist_pipeline_map(name, artist_id)
    lib = runtime().get_library_artist(name, artist_id)
    annotate_in_library(rgs, [], lib, rank_fn=compute_library_rank)
    _apply_rg_pipeline_overlay(rgs, by_identity)
    h._json({
        "release_groups": _catalogue_payload(rgs),
        "ungrouped_releases": [],
    })


class _DisambiguatePressing(TypedDict):
    """One pressing within a disambiguate release group — pure-metadata
    fields from `analyse_artist_releases`, plus per-request overlay
    fields added later by `_overlay_disambiguate`."""

    release_id: str
    title: str
    date: str
    format: str
    track_count: int
    country: str
    recording_ids: list[str]
    in_library: NotRequired[bool]
    beets_album_id: NotRequired[int | None]
    pipeline_status: NotRequired[str | None]
    pipeline_id: NotRequired[int | None]
    processing_owner: NotRequired[dict[str, object] | None]
    library_format: NotRequired[str]
    library_min_bitrate: NotRequired[int]
    library_avg_bitrate: NotRequired[int]
    library_rank: NotRequired[str]
    has_captured_history: NotRequired[bool]
    pipeline_verified_lossless: NotRequired[bool]
    pipeline_provisional: NotRequired[bool]


class _DisambiguateTrack(TypedDict):
    recording_id: str
    title: str
    unique: bool
    also_on: list[str]


class _DisambiguateReleaseGroup(TypedDict):
    """A disambiguate release group — pure-metadata fields, plus the
    same per-request overlay fields as `_DisambiguatePressing`."""

    release_group_id: str
    title: str
    primary_type: str
    first_date: str
    release_ids: list[str]
    pressings: list[_DisambiguatePressing]
    track_count: int
    unique_track_count: int
    covered_by: str | None
    tracks: list[_DisambiguateTrack]
    library_status: NotRequired[str | None]
    pipeline_status: NotRequired[str | None]
    pipeline_id: NotRequired[int | None]
    processing_owner: NotRequired[dict[str, object] | None]
    library_format: NotRequired[str]
    library_min_bitrate: NotRequired[int]
    library_avg_bitrate: NotRequired[int]
    library_rank: NotRequired[str]
    has_captured_history: NotRequired[bool]
    pipeline_verified_lossless: NotRequired[bool]
    pipeline_provisional: NotRequired[bool]


class _DisambiguateSkeleton(TypedDict):
    artist_id: str
    artist_name: str
    release_groups: list[_DisambiguateReleaseGroup]


def _build_disambiguate_skeleton(artist_id: str) -> _DisambiguateSkeleton:
    """Pure-metadata skeleton of the disambiguate response (no overlay).

    Runs the expensive `analyse_artist_releases` pass on cached MB
    metadata and returns a JSON-serializable dict. Callers cache this
    under `meta:` and then layer pipeline / library state on top per-
    request. The analysis is a pure function of pure-metadata inputs,
    so its output is semantically part of the metadata cache.
    """
    from lib.artist_releases import (  # local to avoid heavy import at route-load
        analyse_artist_releases,
        filter_non_live,
    )

    raw_releases = mb_api.get_artist_releases_with_recordings(artist_id)
    filtered = filter_non_live(raw_releases)
    rg_infos = analyse_artist_releases(filtered)

    rgs_skeleton: list[_DisambiguateReleaseGroup] = []
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
        "artist_name": mb_api.get_artist_name(artist_id),
        "release_groups": rgs_skeleton,
    }


def _overlay_disambiguate(skeleton: _DisambiguateSkeleton) -> _DisambiguateSkeleton:
    """Apply per-request pipeline / library overlay to the cached
    skeleton. Returns a new dict — does NOT mutate the cached value."""
    rt = runtime()
    response = copy.deepcopy(skeleton)

    all_mbids: list[str] = []
    for rg in response["release_groups"]:
        all_mbids.extend(rg["release_ids"])
    in_pipeline: dict[str, dict[str, object]] = (
        rt.check_pipeline(all_mbids) if all_mbids else {}
    )
    in_library: set[str] = (
        rt.check_beets_library(all_mbids) if all_mbids else set()
    )
    b = rt.beets_db()

    for rg in response["release_groups"]:
        rg["library_status"] = (
            "in_library"
            if any(rid in in_library for rid in rg["release_ids"])
            else None
        )
        rg_pip_status: str | None = None
        rg_pip_id: int | None = None
        rg_processing_owner: dict[str, object] | None = None
        rg_has_captured_history = False
        rg_verified_lossless = False
        rg_provisional = False
        for rid in rg["release_ids"]:
            pip = in_pipeline.get(rid)
            if pip:
                status_raw = pip.get("status")
                id_raw = pip.get("id")
                owner_raw = pip.get("processing_owner")
                rg_pip_status = status_raw if isinstance(status_raw, str) else None
                rg_pip_id = id_raw if isinstance(id_raw, int) else None
                rg_processing_owner = (
                    owner_raw if is_str_object_dict(owner_raw) else None
                )
                rg_has_captured_history = bool(pip["has_captured_history"])
                rg_verified_lossless = bool(pip["verified_lossless"])
                rg_provisional = bool(pip["provisional_lossless"])
                break
        rg["pipeline_status"] = rg_pip_status
        rg["pipeline_id"] = rg_pip_id
        rg["processing_owner"] = rg_processing_owner
        rg["has_captured_history"] = rg_has_captured_history
        rg["pipeline_verified_lossless"] = rg_verified_lossless
        rg["pipeline_provisional"] = rg_provisional

        lib_mbids = [p["release_id"] for p in rg["pressings"]
                     if p["release_id"] in in_library]
        beets_ids: dict[str, int] = (
            b.get_album_ids_by_mbids(lib_mbids) if lib_mbids and b else {}
        )
        quality: dict[str, dict[str, object]] = (
            b.check_mbids_detail(lib_mbids) if lib_mbids and b else {}
        )

        rg_quality: dict[str, object] | None = None
        for rid in rg["release_ids"]:
            if rid in quality:
                rg_quality = quality[rid]
                break

        for p in rg["pressings"]:
            rid = p["release_id"]
            p["in_library"] = rid in in_library
            p["beets_album_id"] = beets_ids.get(rid)
            p_pip = in_pipeline.get(rid)
            if p_pip:
                p_status_raw = p_pip.get("status")
                p_id_raw = p_pip.get("id")
                p["pipeline_status"] = (
                    p_status_raw if isinstance(p_status_raw, str) else None
                )
                p["pipeline_id"] = p_id_raw if isinstance(p_id_raw, int) else None
                p_owner_raw = p_pip.get("processing_owner")
                p["processing_owner"] = (
                    p_owner_raw if isinstance(p_owner_raw, dict) else None
                )
                p["has_captured_history"] = bool(
                    p_pip["has_captured_history"]
                )
                p["pipeline_verified_lossless"] = bool(
                    p_pip["verified_lossless"]
                )
                p["pipeline_provisional"] = bool(
                    p_pip["provisional_lossless"]
                )
            else:
                p["pipeline_status"] = None
                p["pipeline_id"] = None
                p["processing_owner"] = None
                p["has_captured_history"] = False
                p["pipeline_verified_lossless"] = False
                p["pipeline_provisional"] = False
            pq: dict[str, object] = quality.get(rid) or {}
            if pq:
                fmt_raw = pq.get("beets_format")
                fmt = fmt_raw if isinstance(fmt_raw, str) else ""
                br_raw = pq.get("beets_bitrate")
                br = br_raw if isinstance(br_raw, int) else 0
                p["library_format"] = fmt
                p["library_min_bitrate"] = br
                p["library_avg_bitrate"] = current_library_bitrate(pq)
                p["library_rank"] = compute_library_rank(
                    p["library_format"], p["library_avg_bitrate"])

        if rg_quality:
            fmt_raw = rg_quality.get("beets_format")
            fmt = fmt_raw if isinstance(fmt_raw, str) else ""
            br_raw = rg_quality.get("beets_bitrate")
            br = br_raw if isinstance(br_raw, int) else 0
            rg["library_format"] = fmt
            rg["library_min_bitrate"] = br
            rg["library_avg_bitrate"] = current_library_bitrate(rg_quality)
            rg["library_rank"] = compute_library_rank(
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


def _as_release_rows(value: object) -> TypeGuard[list[dict[str, object]]]:
    """Narrow an adapter envelope's ``releases``/``results`` value.

    `web.mb` / `web.discogs` return `dict[str, object]` envelopes, so a
    nested list is `object`-typed at the type-checker boundary even
    though it is always built as `list[dict[str, object]]`. Overlay
    helpers mutate rows in place, so this narrows the existing list
    rather than reconstructing a copy (e.g. via `msgspec.convert`) that
    would silently detach the overlay mutation from the enclosing
    envelope dict.
    """
    return isinstance(value, list)


def get_release_group(h: RouteHandler, params: dict[str, list[str]], rg_id: str) -> None:
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

    data = mb_api.get_release_group_releases(normalized_id)
    # Standard toolbar (Remove from beets) and badge renderer (in library
    # + codec-aware rank) read these overlay fields per row, so route
    # them through the shared helper.
    releases = data["releases"]
    assert _as_release_rows(releases)
    overlay_release_rows_in_place(releases, [str(r["id"]) for r in releases])
    h._json(data)


def get_release(h: RouteHandler, params: dict[str, list[str]], release_id: str) -> None:
    rt = runtime()
    normalized_id = normalize_release_id(release_id) or release_id.strip()
    identity = ReleaseIdentity.from_id(normalized_id)
    if identity and identity.source == "discogs":
        get_discogs_release(h, params, identity.release_id)
        return

    data = mb_api.get_release(normalized_id)
    overlay_release_rows_in_place([data], [normalized_id])
    # Include current Beets tracks after the shared overlay supplies presence,
    # album id, quality, lifecycle, history, and proof.
    b = rt.beets_db()
    if data["in_library"] and b:
        tracks = b.get_tracks_by_mb_release_id(normalized_id)
        if tracks is not None:
            data["beets_tracks"] = tracks
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
    rt = runtime()
    artist_name = discogs_api.get_artist_name(int(artist_id))
    catalogue = discogs_api.get_artist_releases(int(artist_id))
    # Row-level in-library badge: same pattern as MB. Frontend passes
    # ?name=; without it we still get the canonical name from Discogs API.
    name = params.get("name", [""])[0].strip() or artist_name
    if name:
        by_identity = _artist_pipeline_map(name)
        lib = rt.get_library_artist(name, "")
        annotate_in_library([], catalogue, lib, rank_fn=compute_library_rank)
        _apply_rg_pipeline_overlay(catalogue, by_identity)
    works = [row for row in catalogue if row.identity_kind == "work"]
    ungrouped = [row for row in catalogue if row.identity_kind == "release"]
    h._json({
        "artist_id": artist_id,
        "artist_name": artist_name,
        "release_groups": _catalogue_payload(works),
        "ungrouped_releases": _catalogue_payload(ungrouped),
    })


def get_discogs_master(h: RouteHandler, params: dict[str, list[str]], master_id: str) -> None:
    data = discogs_api.get_master_releases(int(master_id))
    releases = data["releases"]
    assert _as_release_rows(releases)
    overlay_release_rows_in_place(releases, [str(r["id"]) for r in releases])
    h._json(data)


def get_discogs_release(h: RouteHandler, params: dict[str, list[str]], release_id: str) -> None:
    rt = runtime()
    normalized_id = normalize_release_id(release_id) or release_id.strip()
    data = discogs_api.get_release(int(normalized_id))
    overlay_release_rows_in_place([data], [normalized_id])
    b = rt.beets_db()
    if data["in_library"] and b:
        tracks = b.get_tracks_by_mb_release_id(normalized_id)
        if tracks is not None:
            data["beets_tracks"] = tracks
    h._json(data)


def _resolve_compare_artist_ids(name: str, mbid: str,
                                discogs_id: str) -> tuple[str, str]:
    """Resolve MB / Discogs artist IDs from `name` when not passed
    explicitly. Returns the (mbid, discogs_id) pair. Display names
    are resolved separately from the canonical APIs — keeping them
    out of the cache key means a `?name=` typo doesn't produce a
    different cache entry for the same underlying artist pair."""
    if not mbid:
        mb_hits = mb_api.search_artists(name)
        for mb_hit in mb_hits:
            name_raw = mb_hit.get("name")
            hit_name = name_raw if isinstance(name_raw, str) else ""
            if hit_name.lower() == name.lower():
                id_raw = mb_hit.get("id")
                if isinstance(id_raw, str):
                    mbid = id_raw
                break
        if not mbid and mb_hits:
            id_raw = mb_hits[0].get("id")
            if isinstance(id_raw, str):
                mbid = id_raw

    if not discogs_id:
        discogs_hits = discogs_api.search_artists(name)
        for discogs_hit in discogs_hits:
            if discogs_hit["name"].lower() == name.lower():
                discogs_id = discogs_hit["id"]
                break
        if not discogs_id and discogs_hits:
            discogs_id = discogs_hits[0]["id"]

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
    # The source catalogues have no dependency on each other.  Starting them
    # together removes a cold compare waterfall while each adapter retains its
    # own mirror-specific fan-out limits and cache/singleflight semantics.
    sources = parallel_results({
        **({"mb": lambda: mb_api.get_artist_release_groups(mbid)} if mbid else {}),
        **({"discogs": lambda: discogs_api.get_artist_releases(int(discogs_id))}
           if discogs_id else {}),
    }, max_workers=2)
    mb_groups = sources.get("mb", [])
    discogs_groups = sources.get("discogs", [])

    merged = merge_discographies(mb_groups, discogs_groups)
    return ArtistCompareSkeleton(
        both=merged.both,
        mb_unpaired=merged.mb_unpaired,
        discogs_unpaired=merged.discogs_unpaired,
        discogs_ungrouped_releases=merged.discogs_ungrouped_releases,
    )


def _canonical_artist_labels(mbid: str, discogs_id: str) -> tuple[
        dict[str, str] | None, dict[str, str] | None]:
    """Resolve `{id, name}` for each source from the canonical API
    helpers. Names come back from `mb_api.get_artist_name` /
    `discogs_api.get_artist_name`, which are themselves memoized in
    `meta:`, so this is cheap — and it guarantees the display name is
    the same across requests regardless of whatever `?name=` spelling
    a given client happened to use.
    """
    mb_artist: dict[str, str] | None = None
    if mbid:
        mb_artist = {"id": mbid, "name": mb_api.get_artist_name(mbid) or ""}
    discogs_artist: dict[str, str] | None = None
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
    rt = runtime()
    response = copy.deepcopy(skeleton)
    if not name:
        return response

    by_identity = _artist_pipeline_map(name, mbid)
    lib = rt.get_library_artist(name, mbid)

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
                        rank_fn=compute_library_rank)
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
    raw_payload = _catalogue_payload(response)
    if not is_str_object_dict(raw_payload):
        raise TypeError("artist compare response must serialize to an object")
    raw_payload["mb_artist"] = mb_artist
    raw_payload["discogs_artist"] = discogs_artist
    h._json(raw_payload)


# ── Search-by-ID resolver ────────────────────────────────────────────

_RESOLVE_VALID_KINDS_BY_SOURCE = {
    "mb": frozenset(("release", "release-group", "unknown")),
    "discogs": frozenset(("release", "master", "unknown")),
}


def _is_canonical_discogs_id(value: str) -> bool:
    """Whether a resolver-supplied Discogs ID has one canonical spelling."""
    return (
        value.isascii()
        and value.isdigit()
        and 1 <= len(value) <= 12
        and value[0] != "0"
    )


def _resolve_mb(raw_id: str, kind: str) -> dict[str, object]:
    """Resolve an MB UUID into the resolver response shape.

    Tries the leaf (release) endpoint first when kind ∈ {release, unknown}.
    Falls back to the group (release-group) endpoint only when kind=='unknown'
    and the leaf attempt 404s — kind=='release' explicit is honored and
    surfaces the 404 to the caller (the URL path said 'release', trust it).
    Raises HTTPError for the caller to translate to HTTP status.
    """
    if kind in ("release", "unknown"):
        try:
            data = mb_api.get_release(raw_id)
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
    rg = mb_api.get_release_group(raw_id)
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


def _resolve_discogs(raw_id: str, kind: str) -> dict[str, object]:
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
    raw_id = (params.get("id", [""])[0]).strip()
    source = (params.get("source", [""])[0]).strip()
    kind = (params.get("kind", ["unknown"])[0]).strip() or "unknown"

    if not raw_id:
        h._error("Missing 'id' parameter")
        return
    valid_kinds = _RESOLVE_VALID_KINDS_BY_SOURCE.get(source)
    if valid_kinds is None:
        h._error("Missing or invalid 'source' (must be 'mb' or 'discogs')")
        return
    if kind not in valid_kinds:
        h._error("Invalid kind for source")
        return
    if source == "mb" and not _is_canonical_mbid(raw_id):
        h._error("Invalid MusicBrainz ID (must be a canonical UUID)")
        return
    # The browser parser is convenience, never authority. Require a positive,
    # bounded canonical ASCII spelling before the cache or mirror boundary so
    # zero/leading-zero aliases and arbitrary-size integers cannot fan out.
    if source == "discogs" and not _is_canonical_discogs_id(raw_id):
        h._error("Invalid Discogs ID (must be 1-12 canonical digits)")
        return
    if source == "discogs":
        discogs_api.require_mirror_configured()

    cache_key = f"browse-resolve:v2:{source}:{kind}:{raw_id}"

    def _run() -> dict[str, object]:
        if source == "mb":
            return _resolve_mb(raw_id, kind)
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
    except urllib.error.URLError:
        h._json({
            "error": "Resolver upstream unavailable, retry",
            "retryable": True,
        }, 502)
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
