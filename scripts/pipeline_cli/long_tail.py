"""pipeline-cli ``long-tail`` command (#495 carve).

The long-tail worklist read — every ``wanted`` request pre-banded by
on-disk quality and stamped with ``in_flight_rescue``. Counterpart of
``GET /api/pipeline/long-tail`` (U1).
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING, Any, Protocol

import msgspec

from lib.long_tail_service import classify_long_tail_failure
from scripts.pipeline_cli._format import _json_default, _truncate

if TYPE_CHECKING:
    from lib.long_tail_service import BandFn


class _LongTailDB(Protocol):
    """Narrow ``db`` shape ``cmd_long_tail`` touches (#409 pattern) —
    mirrors ``lib.long_tail_service._PipelineDB`` structurally so
    ``FakePipelineDB`` conforms without importing that private symbol."""

    def get_long_tail_cohort(self) -> list[dict[str, Any]]: ...

    def get_long_tail_request(
        self, request_id: int,
    ) -> dict[str, Any] | None: ...


def _cli_band_fn(rows: list[dict[str, object]]) -> dict[str, str]:
    """Build the long-tail band map for the CLI.

    Reuses the SAME exact-resolution banding decision the web route uses,
    but sources Beets from a directly-opened ``BeetsDB`` rather than the web
    server's module-level ``_beets`` global (which the CLI process never
    sets). No parallel membership/detail projection exists; only the
    composition root and rank-config source differ between the two surfaces.

    Takes request rows, not bare ids, so each resolves over its own
    identity union (#1059) — the key stays the acquisition release id.

    Returns ``{release_id: band}`` (``"missing"`` / a lowercase
    ``QualityRank`` / ``"unknown"``). Beets authority/read failures
    propagate to the CLI boundary; an unavailable library must never be
    presented as an actionable all-Missing cohort.
    """
    from lib.banding import load_rank_config, resolve_current_release_bands
    from lib.beets_db import open_beets_db

    if not rows:
        return {}
    cfg = load_rank_config()
    with open_beets_db() as beets:
        return resolve_current_release_bands(beets, rows, cfg)


def _render_long_tail_failure(exc: Exception, *, json_mode: bool) -> int | None:
    """Render one expected service failure through the shared public map."""
    failure = classify_long_tail_failure(exc)
    if failure is None:
        return None
    if json_mode:
        print(json.dumps(
            msgspec.to_builtins(failure),
            indent=2,
            sort_keys=True,
        ))
    else:
        print(f"{failure.error}: {failure.message}", file=sys.stderr)
    return failure.cli_exit_code


def cmd_long_tail(
    db: _LongTailDB,
    args: argparse.Namespace,
    *,
    band_fn: BandFn | None = None,
) -> int:
    """``pipeline-cli long-tail [--band=<band>] [--json]``.

    The long-tail worklist read — every ``wanted`` request pre-banded by
    on-disk quality (``missing`` / a lowercase ``QualityRank`` band /
    ``unknown``) and stamped with ``in_flight_rescue``. Counterpart of
    ``GET /api/pipeline/long-tail`` (U1). Both surfaces wrap
    ``lib.long_tail_service.list_long_tail`` — keep them in sync
    (CLI ⇄ API symmetry).

    ``--id`` requests a single banded row (KTD8 — the post-action
    refetch counterpart of ``GET /api/pipeline/long-tail?id=``); exits 2
    when the id doesn't exist or is no longer ``wanted``.

    ``band_fn`` is a kwarg-DI seam (defaults to ``_cli_band_fn``, the
    real BeetsDB-backed banding); tests inject a deterministic fake so
    they don't need a live beets library.

    Exit codes:
      * 0 — success (empty cohort is a valid state)
      * 2 — ``--id`` not found / not ``wanted``
      * 4 — ambiguous, malformed, or conflicting exact authority
      * 5 — expected Beets authority/read unavailability

    JSON envelope (mirrors the API):
        ``{"results": [...], "band": <str|null>, "count": <int>}``
    Single-id JSON (mirrors the API):
        ``{"result": <row>, "id": <int>}``
    Expected-failure JSON (mirrors the API):
        ``{"category": <conflict|unavailable>, "error": <stable-code>,
        "message": <stable-message>}``
    """
    from lib.long_tail_service import band_one_long_tail, list_long_tail

    json_mode = bool(getattr(args, "json", False))
    resolved_band_fn = band_fn if band_fn is not None else _cli_band_fn

    request_id = getattr(args, "id", None)
    if request_id is not None:
        try:
            row = band_one_long_tail(db, resolved_band_fn, int(request_id))
        except Exception as exc:
            exit_code = _render_long_tail_failure(exc, json_mode=json_mode)
            if exit_code is None:
                raise
            return exit_code
        if row is None:
            msg = f"request {int(request_id)} not found or not wanted"
            if json_mode:
                print(json.dumps(
                    {"error": "Not found", "id": int(request_id)},
                    indent=2, sort_keys=True))
            else:
                print(msg, file=sys.stderr)
            return 2
        if json_mode:
            print(json.dumps(
                {"result": msgspec.to_builtins(row), "id": int(request_id)},
                indent=2, sort_keys=True, default=_json_default))
        else:
            print(f"  [{row.id}] {row.artist_name} - {row.album_title}")
            print(f"  band:            {row.band}")
            print(f"  in_flight_rescue: {row.in_flight_rescue}")
        return 0

    band = getattr(args, "band", None)
    if band == "":
        band = None

    try:
        result = list_long_tail(db, resolved_band_fn, band=band)
    except Exception as exc:
        exit_code = _render_long_tail_failure(exc, json_mode=json_mode)
        if exit_code is None:
            raise
        return exit_code

    if json_mode:
        payload = {
            "results": msgspec.to_builtins(result.rows),
            "band": result.band_filter,
            "count": len(result.rows),
        }
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
        return 0

    if not result.rows:
        suffix = f" for band={band!r}" if band else ""
        print(f"  No wanted rows{suffix}.")
        return 0

    header_cols = (
        ("id", 6),
        ("artist", 25),
        ("album", 25),
        ("band", 12),
        ("rescue", 7),
        ("category", 22),
    )
    print("  ".join(name.ljust(width) for name, width in header_cols))
    print("  ".join("-" * width for _, width in header_cols))
    for r in result.rows:
        row_cells = (
            str(r.id),
            _truncate(r.artist_name, 25),
            _truncate(r.album_title, 25),
            _truncate(r.band, 12),
            "yes" if r.in_flight_rescue else "-",
            _truncate(r.unfindable_category or "-", 22),
        )
        print("  ".join(
            cell.ljust(width)
            for cell, (_, width) in zip(row_cells, header_cols, strict=True)
        ))
    print(f"  ({len(result.rows)} rows)")
    return 0


def add_long_tail_subparser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add ``long-tail`` (#521 carve out of ``routes_meta._build_parser``,
    verbatim argument definitions)."""
    p_long_tail = sub.add_parser(
        "long-tail",
        help="Long-tail worklist — wanted cohort pre-banded by on-disk "
             "quality (missing / QualityRank / unknown) + in_flight_rescue")
    p_long_tail.add_argument(
        "--band", default=None,
        help="Filter to a single band: missing | transparent | excellent "
             "| good | acceptable | poor | unknown")
    p_long_tail.add_argument(
        "--id", type=int, default=None,
        help="Band a single request by id (post-action refetch); "
             "exits 2 if not found / not wanted")
    p_long_tail.add_argument("--json", action="store_true",
                             help="Print structured JSON instead of text")
