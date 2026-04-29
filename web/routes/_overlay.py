"""Shared release-row overlay helpers for browse / label routes.

Every "list of releases" route (release-group pressings, Discogs master
releases, label catalogue) overlays the same library + pipeline state
onto each row. The exact shape of those fields — `in_library`,
`beets_album_id`, `library_format`, `library_min_bitrate`,
`library_rank`, `pipeline_status`, `pipeline_id` — is the contract the
frontend reads (see `web/js/badges.js`). Keeping a single helper
prevents drift across routes when new fields are added.

The helper mutates rows in place. Callers that need to preserve a
cached input must deep-copy first (mirrors `_overlay_disambiguate`'s
contract in `web/routes/browse.py`).
"""

from __future__ import annotations

from typing import Iterable


def overlay_release_rows_in_place(rows: list[dict], release_ids: Iterable[str]) -> None:
    """Annotate each release row with library + pipeline state in place.

    Parameters
    ----------
    rows
        Mutable list of release-row dicts. Each row must have an `id`
        key (string release id, MB UUID or stringified Discogs id).
        After overlay each row carries:
        `in_library`, `beets_album_id`, `library_format`,
        `library_min_bitrate`, `library_rank`, `pipeline_status`,
        `pipeline_id`. Library quality fields are only set when the
        release is in the beets library AND the beets DB returned
        details for it.
    release_ids
        Iterable of release ids to batch-query against beets / pipeline.
        Typically `[r["id"] for r in rows]`; passed in so callers that
        need to filter (e.g. skip empty ids) control the input.
    """
    # Local import keeps the routes._overlay → server.py edge consistent
    # with the rest of routes/* (which all use the lazy `_server()` shim).
    from web import server as srv

    ids_list = list(release_ids)
    in_library = srv.check_beets_library(ids_list) if ids_list else set()
    in_pipeline = srv.check_pipeline(ids_list) if ids_list else {}
    b = srv._beets_db()
    beets_ids = b.get_album_ids_by_mbids(list(in_library)) if in_library and b else {}
    quality = b.check_mbids_detail(list(in_library)) if in_library and b else {}

    for r in rows:
        rid = r["id"]
        r["in_library"] = rid in in_library
        r["beets_album_id"] = beets_ids.get(rid)
        q = quality.get(rid)
        if q:
            fmt_raw = q.get("beets_format")
            fmt = fmt_raw if isinstance(fmt_raw, str) else ""
            br_raw = q.get("beets_bitrate")
            br = br_raw if isinstance(br_raw, int) else 0
            r["library_format"] = fmt
            r["library_min_bitrate"] = br
            r["library_rank"] = srv.compute_library_rank(fmt, br)
        pi = in_pipeline.get(rid)
        r["pipeline_status"] = pi["status"] if pi else None
        r["pipeline_id"] = pi["id"] if pi else None
