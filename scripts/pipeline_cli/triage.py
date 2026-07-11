"""pipeline-cli ``triage`` command family (#495 carve; issue #U16).

The request/cohort subcommands wrap the U15 triage service:

  * ``pipeline-cli triage show <id>`` — per-request composition.
  * ``pipeline-cli triage list --filter=<spec>`` — cohort listing.

Issue #573 W2 adds ``pipeline-cli triage quarantine``. It wraps the separate
read-only ``lib.quarantine_triage_service`` lifecycle view and mirrors
``GET /api/triage/quarantine``.

Both adhere to CLAUDE.md § "CLI ⇄ API surface symmetry": each one is a
thin wrapper around ``lib.triage_service``; the matching HTTP routes
(U17) wrap the same service with the same outcome → exit-code /
status-code mapping.
"""

import argparse
import json
import sys

import msgspec

# The canonical machine-parseable forms come from the service-layer
# ``VALID_FILTER_FORMS`` (single source of truth across CLI and HTTP);
# the prose variants below are CLI-only embellishments to help operators
# remember the parameterised vocab. New filter forms get added at the
# service layer; both wrappers auto-pick them up.
from lib.triage_service import VALID_FILTER_FORMS as _TRIAGE_VALID_FILTER_FORMS_BASE
from scripts.pipeline_cli._format import _format_dt, _json_default, _truncate

_TRIAGE_VALID_FILTER_FORMS = (
    "all",
    "unfindable",
    "unfindable:<category>  (category ∈ "
    "{artist_absent, album_absent_artist_present, "
    "one_track_structural, wrong_pressing_available})",
    "data_quality",
    "data_quality:<field_name>  (field ∈ "
    "{release_group_year, release_group_id, track_artist, catalog_number})",
    "data_quality:status=<resolver_status>  (e.g. "
    "unresolved_4xx_client, unresolved_404, unresolved_timeout)",
    "data_quality:reason=<reason_code>  (e.g. http_400, http_410, "
    "http_422)",
    "search_not_converting",
)


def cmd_triage_show(db, args):
    """``pipeline-cli triage show <id>`` — per-request triage composition.

    Wraps ``compose_triage_for_request`` and renders the full payload
    (request meta + unfindable + field-quality + search forensics +
    recent search_log slice). Mirrors the human/JSON conventions
    established by ``cmd_search_plan_show`` and the U17 API.

    Exit codes:
      * 0 — success
      * 2 — request not found
    """
    from lib.triage_service import compose_triage_for_request

    rid = int(args.id)
    result = compose_triage_for_request(rid, db)
    if result is None:
        if getattr(args, "json", False):
            print(json.dumps({
                "error": "Not found",
                "request_id": rid,
            }, indent=2, sort_keys=True))
        else:
            print(f"  Request {rid} not found.", file=sys.stderr)
        return 2

    if getattr(args, "json", False):
        payload = msgspec.to_builtins(result)
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
        return 0

    # Human-readable rendering.
    meta = result.request_meta
    print(f"  Request ID:        {meta.id}")
    print(f"  Artist / Album:    {meta.artist_name} — {meta.album_title}")
    if meta.year is not None:
        print(f"  Year:              {meta.year}")
    print(f"  Status:            {meta.status}")
    print(f"  Source:            {meta.source or '-'}")
    if meta.mb_release_id:
        print(f"  MB release id:     {meta.mb_release_id}")
    if meta.discogs_release_id:
        print(f"  Discogs id:        {meta.discogs_release_id}")
    if meta.failure_class:
        print(f"  Failure class:     {meta.failure_class}")
    if meta.search_filetype_override:
        print(f"  Search filetype:   {meta.search_filetype_override}")

    # Unfindable cohort state.
    if result.unfindable is None:
        print("  Unfindable:        (no signals)")
    else:
        u = result.unfindable
        print("  Unfindable:")
        print(f"    category:                  {u.category or '-'}")
        print(f"    categorised_at:            {_format_dt(u.categorised_at)}")
        print(
            f"    last_artist_probe_at:      "
            f"{_format_dt(u.last_artist_probe_at)}"
        )
        if u.last_artist_probe_match_count is not None:
            print(
                f"    last_probe_match_count:    "
                f"{u.last_artist_probe_match_count}"
            )
        if u.rescued_at is not None:
            print(f"    rescued_at:                {_format_dt(u.rescued_at)}")
            print(
                f"    prior_unfindable_category: "
                f"{u.prior_unfindable_category or '-'}"
            )

    # Field-quality rows (the resolver side table, U2).
    if not result.field_quality:
        print("  Field quality:     (no resolutions)")
    else:
        print(f"  Field quality:     {len(result.field_quality)} resolution(s)")
        for fr in result.field_quality:
            reason = fr.reason_code or "-"
            print(
                f"    [{fr.field_name}] status={fr.status} reason={reason} "
                f"attempts={fr.attempts} resolved_at={_format_dt(fr.resolved_at)}"
            )

    # Search forensics summary + recent entries.
    sf = result.search_forensics
    print("  Search forensics:")
    print(f"    total_searches:            {sf.total_searches}")
    print(f"    with_cands_count:          {sf.with_cands_count}")
    print(f"    found_count:               {sf.found_count}")
    print(f"    near_cap_count:            {sf.near_cap_count}")
    print(f"    zero_results_count:        {sf.zero_results_count}")
    print(f"    pre_filter_skips_total:    {sf.pre_filter_skips_total}")
    print(
        f"    first_strategy_with_cands: "
        f"{sf.first_strategy_with_cands or '-'}"
    )
    print(
        f"    dominant_rejection_reason: "
        f"{sf.dominant_rejection_reason or '-'}"
    )
    print(f"    last_search_at:            {_format_dt(sf.last_search_at)}")

    if not sf.recent_entries:
        print("    recent_entries:            (none)")
    else:
        print(f"    recent_entries:            {len(sf.recent_entries)} "
              f"(newest first, max 10)")
        for entry in sf.recent_entries:
            strategy = entry.plan_strategy or "(legacy)"
            reason = entry.rejection_reason or "-"
            matcher = (
                f"{entry.matcher_score_top1:.2f}"
                if entry.matcher_score_top1 is not None else "-"
            )
            rc = (
                str(entry.result_count) if entry.result_count is not None
                else "-"
            )
            query = entry.query or ""
            print(
                f"      [{_format_dt(entry.created_at)}] id={entry.id} "
                f"{entry.outcome} {strategy} rc={rc} reject={reason} "
                f"matcher={matcher} query={query!r}"
            )

    return 0


def cmd_triage_list(db, args):
    """``pipeline-cli triage list --filter=<spec>`` — cohort listing.

    Wraps ``list_triage``. Default page size is 50. Use ``--after=<id>``
    to resume; the page footer prints the next ``--after`` value when
    the returned page is exactly ``--limit`` long.

    Exit codes:
      * 0 — success (empty list is a valid cohort state)
      * 3 — invalid filter spec (``InvalidFilterError``) or out-of-range
        ``--limit`` / ``--after``

    JSON envelope (mirrors the API):
        ``{"results": [...], "next_after": <int|null>,
           "page_size": <int>, "filter": <spec>}``
    """
    from lib.triage_service import (
        InvalidFilterError,
        TRIAGE_AFTER_MIN,
        TRIAGE_LIMIT_MAX,
        TRIAGE_LIMIT_MIN,
        list_triage,
    )

    json_mode = bool(getattr(args, "json", False))
    limit = int(args.limit) if args.limit is not None else 50
    after = int(args.after) if args.after is not None else None

    # Bounds — mirrors the API's [1..200] / [>=1] check so the two
    # surfaces reject the same set of out-of-range values.
    if not (TRIAGE_LIMIT_MIN <= limit <= TRIAGE_LIMIT_MAX):
        msg = (
            f"--limit must be in [{TRIAGE_LIMIT_MIN}, {TRIAGE_LIMIT_MAX}]; "
            f"got {limit}"
        )
        if json_mode:
            print(json.dumps({"error": msg}, indent=2, sort_keys=True))
        else:
            print(msg, file=sys.stderr)
        return 3
    if after is not None and after < TRIAGE_AFTER_MIN:
        msg = f"--after must be >= {TRIAGE_AFTER_MIN}; got {after}"
        if json_mode:
            print(json.dumps({"error": msg}, indent=2, sort_keys=True))
        else:
            print(msg, file=sys.stderr)
        return 3

    try:
        results = list_triage(
            args.filter, db,
            page_size=limit,
            after_request_id=after,
        )
    except InvalidFilterError as exc:
        from lib.triage_service import (
            VALID_DATA_QUALITY_FIELD_NAMES,
            VALID_UNFINDABLE_CATEGORIES,
        )
        if json_mode:
            # JSON-mode error path — emit a structured payload on stdout
            # so callers piping ``--json | jq`` keep parsing. Mirrors
            # cmd_triage_show's 404 JSON path and the API 400 envelope.
            print(json.dumps(
                {
                    "error": str(exc),
                    "valid_filters": list(_TRIAGE_VALID_FILTER_FORMS_BASE),
                    "valid_unfindable_categories": sorted(
                        VALID_UNFINDABLE_CATEGORIES
                    ),
                    "valid_data_quality_fields": sorted(
                        VALID_DATA_QUALITY_FIELD_NAMES
                    ),
                },
                indent=2, sort_keys=True,
            ))
        else:
            message = (
                f"Invalid filter spec: {exc}\n"
                "Valid forms:\n"
                + "\n".join(f"  - {form}" for form in _TRIAGE_VALID_FILTER_FORMS)
            )
            print(message, file=sys.stderr)
        return 3

    # ``next_after`` matches the API's ``>= limit`` predicate so the
    # CLI and HTTP surfaces report identical pagination state on the
    # same data.
    next_after: int | None = None
    if results and len(results) >= limit:
        next_after = results[-1].request_meta.id

    if json_mode:
        # Envelope wrap matches the API shape so agents pipe-and-jq the
        # same way against both surfaces.
        payload = {
            "results": msgspec.to_builtins(results),
            "next_after": next_after,
            "page_size": limit,
            "filter": args.filter,
        }
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
        return 0

    if not results:
        print(f"  No results for filter={args.filter!r}.")
        return 0

    # Human table.
    header_cols = (
        ("id", 6),
        ("artist", 25),
        ("album", 25),
        ("status", 12),
        ("category/failure", 28),
        ("last_search_at", 20),
    )
    header_line = "  ".join(name.ljust(width) for name, width in header_cols)
    print(header_line)
    print("  ".join("-" * width for _, width in header_cols))

    for r in results:
        meta = r.request_meta
        category_or_failure = (
            (r.unfindable.category if r.unfindable is not None else None)
            or meta.failure_class
            or "-"
        )
        last_search = _format_dt(r.search_forensics.last_search_at)
        row_cells = (
            str(meta.id),
            _truncate(meta.artist_name, 25),
            _truncate(meta.album_title, 25),
            _truncate(meta.status, 12),
            _truncate(category_or_failure, 28),
            _truncate(last_search, 20),
        )
        print("  ".join(
            cell.ljust(width) for cell, (_, width) in zip(row_cells, header_cols)
        ))

    print(f"  ({len(results)} rows)")
    if next_after is not None:
        print(
            f"  next page: pipeline-cli triage list --filter={args.filter} "
            f"--limit={limit} --after={next_after}"
        )
    return 0


def cmd_triage_quarantine(db, args):
    """List unreferenced immediate folders under ``failed_imports``.

    Exit codes:
      * 0 — complete read-only scan (including an empty result)
      * 5 — configuration, DB, decode, or filesystem scan unavailable
    """
    from lib.quarantine_triage_service import (
        QuarantineScanError,
        list_unreferenced_quarantine_folders,
    )

    json_mode = bool(getattr(args, "json", False))
    try:
        result = list_unreferenced_quarantine_folders(db)
    except QuarantineScanError as exc:
        if json_mode:
            print(json.dumps({"error": str(exc)}, indent=2, sort_keys=True))
        else:
            print(f"  Quarantine scan unavailable: {exc}", file=sys.stderr)
        return 5

    if json_mode:
        print(json.dumps(
            msgspec.to_builtins(result),
            indent=2,
            sort_keys=True,
        ))
        return 0

    print(f"  Quarantine root: {result.quarantine_root}")
    if not result.folders:
        print("  No unreferenced quarantine folders.")
        return 0
    for folder in result.folders:
        print(f"  {folder.name}  mtime_ns={folder.mtime_ns}")
        print(f"    {folder.path}")
    print(f"  ({len(result.folders)} folders)")
    return 0


def add_triage_subparser(
    sub: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    """Add ``triage`` + its nested subcommands (#521 carve out of
    ``routes_meta._build_parser``, verbatim argument definitions).

    Returns the ``triage`` subparser itself so ``main()`` can print its
    help when invoked without a nested subcommand.
    """
    # triage (U16 + #573 W2) — operator-facing composition of unfindable +
    # field-quality + search-forensics, plus read-only quarantine lifecycle.
    # Nested under a
    # subparser for the same reason ``search-plan`` is: the per-request view
    # and the cohort list share enough state to benefit from a shared
    # namespace, and the convention is consistent with the rest of this CLI.
    p_triage_op = sub.add_parser(
        "triage",
        help="Operator triage — compose request/search forensics, list a "
             "cohort, or surface unreferenced quarantine folders")
    tr_sub = p_triage_op.add_subparsers(dest="triage_command")

    p_tr_show = tr_sub.add_parser(
        "show",
        help="Per-request triage composition (request meta + unfindable + "
             "field-quality + search forensics + last 10 search_log rows). "
             "Note: subcommand form mirrors `search-plan show <id>`; bare "
             "`triage <id>` is not accepted.")
    p_tr_show.add_argument("id", type=int, help="Request ID")
    p_tr_show.add_argument("--json", action="store_true",
                            help="Print structured JSON instead of text")

    p_tr_list = tr_sub.add_parser(
        "list",
        help="Cohort listing by filter spec")
    p_tr_list.add_argument(
        "--filter", default="all",
        help="Filter spec: all | unfindable[:<category>] | "
             "data_quality[:<field>] | data_quality:status=<status> | "
             "data_quality:reason=<code> | search_not_converting")
    p_tr_list.add_argument(
        "--limit", type=int, default=50,
        help="Page size (default 50)")
    p_tr_list.add_argument(
        "--after", type=int, default=None,
        help="Resume cursor: last request_id from prior page")
    p_tr_list.add_argument("--json", action="store_true",
                            help="Print structured JSON instead of text")

    p_tr_quarantine = tr_sub.add_parser(
        "quarantine",
        help="Read-only list of immediate failed_imports album folders with "
             "no visible Wrong Matches reference",
    )
    p_tr_quarantine.add_argument(
        "--json", action="store_true",
        help="Print structured JSON instead of text",
    )
    return p_triage_op
