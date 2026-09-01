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

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

import msgspec

from web.runtime import runtime

if TYPE_CHECKING:
    from lib.convergence_service import ConvergenceSignal


def _pipeline_request_id(row: dict[str, object]) -> int:
    value = row["id"]
    if not isinstance(value, int):
        raise TypeError("pipeline overlay request id must be an integer")
    return value


def _band_from_detail(
    rid: str,
    in_library: set[str],
    quality: dict[str, dict[str, object]],
) -> str:
    """Three-way band for one release id given already-fetched membership
    + ``check_mbids_detail`` output (KTD1).

    Legacy browse/label row overlays route through this pure adapter over
    their already-fetched membership and detail projections. Long-tail
    banding uses the exact-resolution snapshot path below.
    """
    from lib.banding import band_from_detail
    from web import overlay

    # Delegate to the shared lib decision, supplying the web process's cached
    # rank cfg. The decision lives in lib/ so the CLI bands without importing
    # web (no parallel three-way logic).
    return band_from_detail(rid, in_library, quality, overlay._rank_cfg())


def band_release_ids(release_ids: Iterable[str]) -> dict[str, str]:
    """Map each release id to its beets-library quality band.

    The long-tail web adapter over the shared exact-resolution decision.
    Three-way (KTD1):

    * ``CurrentBeetsMissing`` → ``"missing"``.
    * a unique unrankable item snapshot → ``"unknown"``.
    * a unique rankable item snapshot → the lowercase ``QualityRank`` band.
    * any ambiguous exact resolution → a shared typed exception and no map.

    Bounded query fan-out: one ``resolve_current_releases`` batch over the
    complete cohort, never a membership projection followed by a detail
    projection. Skips ``check_pipeline`` because the cohort rows already
    carry their pipeline columns.

    Returns a complete dict keyed by the release id string. Ids that are
    explicitly absent from Beets ARE present with the ``"missing"`` band;
    omitting a requested identity is an authority failure, never an implicit
    claim that the pressing is missing.
    """
    from lib.banding import (
        CurrentBeetsBandingUnavailableError,
        resolve_current_release_bands,
    )
    from web import overlay

    ids_list = [str(rid) for rid in release_ids]
    if not ids_list:
        return {}
    b = runtime().beets_db()
    if b is None:
        raise CurrentBeetsBandingUnavailableError(
            "current Beets authority is unavailable"
        )
    return resolve_current_release_bands(b, ids_list, overlay._rank_cfg())


def overlay_release_rows_in_place(
    rows: list[dict[str, object]],
    release_ids: Iterable[str],
    *,
    convergence_fn: Callable[
        [list[int]], dict[int, ConvergenceSignal]
    ] | None = None,
) -> None:
    """Annotate each release row with library + pipeline state in place.

    Parameters
    ----------
    rows
        Mutable list of release-row dicts. Each row must have an `id`
        key (string release id, MB UUID or stringified Discogs id).
        After overlay each row carries:
        `in_library`, `beets_album_id`, `library_format`,
        `library_min_bitrate`, `library_avg_bitrate`, `library_rank`,
        `pipeline_status`, `pipeline_id`, `has_captured_history`,
        `pipeline_verified_lossless`, `pipeline_provisional`,
        `processing_owner`, `convergence`. Library quality fields are
        only set when the
        release is in the beets library AND the beets DB returned
        details for it. The identity pair derives from the request's
        linked current evidence (verified proof / unverified
        lossless-source anchor) — see `PipelineDB.get_pipeline_overlay`.
    release_ids
        Iterable of release ids to batch-query against beets / pipeline.
        Typically `[r["id"] for r in rows]`; passed in so callers that
        need to filter (e.g. skip empty ids) control the input.
    """
    from lib.banding import current_library_bitrate

    rt = runtime()
    ids_list = list(release_ids)
    in_pipeline: dict[str, dict[str, object]] = (
        rt.check_pipeline(ids_list) if ids_list else {}
    )
    request_ids = [_pipeline_request_id(row) for row in in_pipeline.values()]
    get_convergence = convergence_fn or rt.get_convergence_signals
    convergence = get_convergence(request_ids)
    in_library: set[str] = (
        rt.check_beets_library(ids_list) if ids_list else set()
    )
    b = rt.beets_db()
    beets_ids: dict[str, int] = (
        b.get_album_ids_by_mbids(list(in_library)) if in_library and b else {}
    )
    quality: dict[str, dict[str, object]] = (
        b.check_mbids_detail(list(in_library)) if in_library and b else {}
    )

    for r in rows:
        rid = str(r["id"])
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
        pipeline_id = _pipeline_request_id(pi) if pi else None
        r["pipeline_id"] = pipeline_id
        r["processing_owner"] = (
            pi.get("processing_owner") if pi else None
        )
        r["has_captured_history"] = (
            bool(pi["has_captured_history"]) if pi else False
        )
        r["pipeline_verified_lossless"] = (
            bool(pi["verified_lossless"]) if pi else False
        )
        r["pipeline_provisional"] = (
            bool(pi["provisional_lossless"]) if pi else False
        )
        signal = (
            convergence.get(pipeline_id) if pipeline_id is not None else None
        )
        r["convergence"] = (
            msgspec.to_builtins(signal) if signal is not None else None
        )
