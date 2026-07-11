"""Shared release-row overlay helpers for browse / label routes.

Every "list of releases" route (release-group pressings, Discogs master
releases, label catalogue) overlays the same library + pipeline state
onto each row. The exact shape of those fields — `in_library`,
`beets_album_id`, `library_format`, `library_min_bitrate`,
`library_avg_bitrate`,
`library_rank`, `pipeline_status`, `pipeline_id` — is the contract the
frontend reads (see `web/js/badges.js`). Keeping a single helper
prevents drift across routes when new fields are added.

The helper mutates rows in place. Callers that need to preserve a
cached input must deep-copy first (mirrors `_overlay_disambiguate`'s
contract in `web/routes/browse.py`).
"""

from __future__ import annotations

import logging
from typing import Iterable

log = logging.getLogger(__name__)


def _band_from_detail(
    rid: str,
    in_library: set[str],
    quality: dict[str, dict],
) -> str:
    """Three-way band for one release id given already-fetched membership
    + ``check_mbids_detail`` output (KTD1).

    The single banding decision both ``band_release_ids`` and
    ``overlay_release_rows_in_place`` route through — header, list, and
    sibling panel can never diverge because they share this function. The
    membership / detail queries are the caller's responsibility (batched
    once); this is pure given them.
    """
    from web import overlay
    from lib.banding import band_from_detail

    # Delegate to the shared lib decision, supplying the web process's cached
    # rank cfg. The decision lives in lib/ so the CLI bands without importing
    # web (no parallel three-way logic).
    return band_from_detail(rid, in_library, quality, overlay._rank_cfg())


def band_release_ids(release_ids: Iterable[str]) -> dict[str, str]:
    """Map each release id to its beets-library quality band.

    The beets-only banding core, factored out of
    ``overlay_release_rows_in_place`` so the long-tail worklist (U1) and
    the overlay both band through one function — header and list always
    agree. Three-way (KTD1):

    * release id absent from the beets membership set → ``"missing"``.
    * present but no detail row / ``compute_library_rank`` returns
      ``"unknown"`` → ``"unknown"`` (has audio, never ``"missing"``).
    * otherwise → the lowercase ``QualityRank`` band.

    Bounded query fan-out: one membership query (``check_beets_library``)
    + one ``check_mbids_detail`` batch over the in-library subset — never
    per row. Skips the overlay's ``check_pipeline`` query: the long-tail
    cohort row already carries the pipeline columns, and the band depends
    only on the on-disk copy.

    Returns a dict keyed by the release id string. Ids that are
    ``"missing"`` ARE present in the dict (banded ``"missing"``) so a
    caller can distinguish "banded missing" from "not asked about" — but
    the long-tail service treats both the same (absent → ``Missing``).
    """
    from web import server as srv

    ids_list = [str(rid) for rid in release_ids]
    if not ids_list:
        return {}
    try:
        in_library = srv.check_beets_library(ids_list)
        b = srv._beets_db()
        quality = b.check_mbids_detail(list(in_library)) if in_library and b else {}
    except Exception:
        # Beets unavailable (locked / missing DB) — degrade to all-"missing"
        # rather than 500-ing the whole worklist (matches the CLI's
        # _cli_band_fn fallback). "No clean copy to upgrade" is the honest
        # default; the long-tail view still renders.
        log.warning(
            "band_release_ids: beets unavailable, banding all-missing",
            exc_info=True,
        )
        return {rid: "missing" for rid in ids_list}

    return {rid: _band_from_detail(rid, in_library, quality) for rid in ids_list}


def overlay_release_rows_in_place(rows: list[dict], release_ids: Iterable[str]) -> None:
    """Annotate each release row with library + pipeline state in place.

    Parameters
    ----------
    rows
        Mutable list of release-row dicts. Each row must have an `id`
        key (string release id, MB UUID or stringified Discogs id).
        After overlay each row carries:
        `in_library`, `beets_album_id`, `library_format`,
        `library_min_bitrate`, `library_avg_bitrate`, `library_rank`,
        `pipeline_status`, `pipeline_id`. Library quality fields are only set when the
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
    from lib.banding import current_library_bitrate

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
            avg_br = current_library_bitrate(q)
            r["library_format"] = fmt
            r["library_min_bitrate"] = br
            r["library_avg_bitrate"] = avg_br
            # Band through the one shared decision so the overlay's
            # ``library_rank`` and the long-tail worklist's band can
            # never diverge. ``rid`` is in ``in_library`` here (we're
            # inside ``if q:`` on a detail row), so this returns the
            # lowercase QualityRank, identical to the prior inline call.
            r["library_rank"] = _band_from_detail(rid, in_library, quality)
        pi = in_pipeline.get(rid)
        r["pipeline_status"] = pi["status"] if pi else None
        r["pipeline_id"] = pi["id"] if pi else None
