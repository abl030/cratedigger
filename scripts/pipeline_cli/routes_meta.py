"""pipeline-cli argparse construction + self-documentation (#495 carve).

``_build_parser`` builds the entire subcommand tree (pure argparse
construction — no handler-function references, so this module has no
dependency on the command-family modules). ``cmd_routes`` walks that same
parser to self-document the CLI surface, mirroring ``GET /api/_index`` on
the web side.
"""

import argparse
import json
import os

from lib.pipeline_db import DEFAULT_DSN
from scripts.pipeline_cli.imports import SPECTRAL_GRADE_CHOICES
from scripts.pipeline_cli.album_requests import VALID_STATUSES


def _build_parser() -> tuple[
    argparse.ArgumentParser, argparse.ArgumentParser, argparse.ArgumentParser
]:
    """Build the full pipeline-cli argument parser.

    Returned tuple is ``(top_level, search_plan_subparser,
    triage_subparser)``; ``main()`` uses the nested subparsers to print
    helpful errors when an operator runs ``search-plan`` / ``triage``
    without a subcommand, and ``cmd_routes`` uses the top-level parser
    to introspect every registered subcommand.
    """
    parser = argparse.ArgumentParser(description="Pipeline CLI — manage download pipeline DB")
    parser.add_argument("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
    sub = parser.add_subparsers(dest="command")

    # list
    p_list = sub.add_parser("list", help="List album requests")
    p_list.add_argument("filter_status", nargs="?", help="Filter by status")
    p_list.add_argument(
        "--search",
        help="Case-insensitive substring match on artist or album "
             "(mirrors GET /api/pipeline/search)",
    )

    # add
    p_add = sub.add_parser("add", help="Add a new request by MBID or Discogs ID")
    p_add.add_argument("mbid", help="MusicBrainz release UUID or Discogs numeric release ID")
    p_add.add_argument("--source", default="request", choices=["request", "redownload", "manual"],
                       help="Source type (default: request)")

    # query
    p_query = sub.add_parser("query", help="Run a read-only SQL query for debugging")
    p_query.add_argument("sql", help="SQL query string, or '-' to read SQL from stdin")
    p_query.add_argument("--json", action="store_true", help="Print rows as JSON")

    # status
    sub.add_parser("status", help="Show counts by status")

    # disk-coverage
    p_disk = sub.add_parser(
        "disk-coverage",
        help="Show which active pipeline rows are actually present in beets",
    )
    p_disk.add_argument(
        "--beets-db",
        default=os.environ.get("BEETS_DB", "/mnt/virtio/Music/beets-library.db"),
        help="Path to beets SQLite DB (default: BEETS_DB or production path)",
    )
    p_disk.add_argument(
        "--counts-only",
        action="store_true",
        help="Suppress the off-disk row list and print counts only",
    )
    p_disk.add_argument(
        "--include-inverse",
        action="store_true",
        help="Also include beets albums with no active pipeline row",
    )

    # retry
    p_retry = sub.add_parser("retry", help="Reset a failed request to wanted")
    p_retry.add_argument("id", type=int, help="Request ID")

    # cancel
    p_cancel = sub.add_parser("cancel", help="Cancel a request (set to skipped)")
    p_cancel.add_argument("id", type=int, help="Request ID")

    # set
    p_set = sub.add_parser("set", help="Change the status of a request")
    p_set.add_argument("id", type=int, help="Request ID")
    p_set.add_argument("status", choices=VALID_STATUSES, help="New status")

    # show
    p_show = sub.add_parser("show", help="Show full details of a request")
    p_show.add_argument("id", type=int, help="Request ID")

    # search-plan
    p_sp = sub.add_parser(
        "search-plan",
        help="Inspect persisted search plans (read-only, U6)")
    sp_sub = p_sp.add_subparsers(dest="search_plan_command")
    p_sp_show = sp_sub.add_parser(
        "show",
        help="Show active/failed plans, cursor, items, provenance, "
             "legacy logs for one request")
    p_sp_show.add_argument("id", type=int, help="Request ID")
    p_sp_show.add_argument("--json", action="store_true",
                            help="Print structured JSON instead of text")
    p_sp_show.add_argument("--no-stats", action="store_true",
                            dest="no_stats",
                            help="Suppress per-slot/query usefulness stats")
    p_sp_regen = sp_sub.add_parser(
        "regenerate",
        help="Regenerate the search plan for a request (U8)")
    p_sp_regen.add_argument("id", type=int, help="Request ID")
    p_sp_regen.add_argument("--prepend-artist", action="store_true",
                             dest="prepend_artist", default=None,
                             help="Prepend artist name to album title in "
                             "generated queries (overrides config; absent "
                             "means use config's album_prepend_artist)")
    p_sp_regen.add_argument("--json", action="store_true",
                             help="Print structured JSON instead of text")
    p_sp_advance = sp_sub.add_parser(
        "advance",
        help="Forward-only operator advance of the cursor (e.g. skip "
             "collapsed default-strategy slots on a self-titled release)")
    p_sp_advance.add_argument("id", type=int, help="Request ID")
    sp_target = p_sp_advance.add_mutually_exclusive_group(required=True)
    sp_target.add_argument(
        "--to-ordinal", type=int, dest="to_ordinal",
        help="Absolute target ordinal in [0, plan_item_count)")
    sp_target.add_argument(
        "--to-strategy", dest="to_strategy",
        help="Strategy prefix; advance to the first plan item past the "
             "current cursor whose strategy starts with this string "
             "(e.g. `track`, `unwild_year`)")
    p_sp_advance.add_argument("--json", action="store_true",
                              help="Print structured JSON instead of text")
    p_sp_dry_run = sp_sub.add_parser(
        "dry-run",
        help="Run the generator against the request's snapshot without "
             "persisting (U6 simulator)")
    p_sp_dry_run.add_argument("id", type=int, help="Request ID")
    p_sp_dry_run.add_argument("--prepend-artist", action="store_true",
                              dest="prepend_artist", default=None,
                              help="Prepend artist name to album title in "
                              "generated queries (overrides config; absent "
                              "means use config's album_prepend_artist)")
    p_sp_dry_run.add_argument("--json", action="store_true",
                              help="Print structured JSON instead of text")
    p_sp_saturation = sp_sub.add_parser(
        "saturation",
        help="Show per-request saturation rate + pre-filter skip total "
             "over the recent search_log window (U7 telemetry)")
    p_sp_saturation.add_argument("id", type=int, help="Request ID")
    p_sp_saturation.add_argument(
        "--window-days", type=int, default=None, dest="window_days",
        help="Window in days; defaults to 14; valid range [1, 90]")
    p_sp_saturation.add_argument(
        "--json", action="store_true",
        help="Print structured JSON instead of text")
    p_sp_history = sp_sub.add_parser(
        "history",
        help="Cursor-paginated read of one request's search_log rows "
             "(per-attempt forensics)")
    p_sp_history.add_argument("id", type=int, help="Request ID")
    p_sp_history.add_argument(
        "--limit", type=int, default=None,
        help="Rows per page; defaults to 50; valid range [1, 200]")
    p_sp_history.add_argument(
        "--before-id", type=int, default=None, dest="before_id",
        help="Resume cursor: pass the previous page's next_before_id")
    p_sp_history.add_argument("--json", action="store_true",
                              help="Print structured JSON instead of text")

    # triage (U16) — operator-facing composition of unfindable + field-quality
    # + search-forensics. Wraps ``lib.triage_service`` (U15). Nested under a
    # subparser for the same reason ``search-plan`` is: the per-request view
    # and the cohort list share enough state to benefit from a shared
    # namespace, and the convention is consistent with the rest of this CLI.
    p_triage_op = sub.add_parser(
        "triage",
        help="Operator triage (U16) — compose unfindable + field-quality + "
             "search-forensics for one request, or list a cohort by filter")
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

    # long-tail
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

    # quality
    p_quality = sub.add_parser("quality", help="Show quality state and simulate decisions")
    p_quality.add_argument("id", type=int, help="Request ID")

    # set-intent
    p_intent = sub.add_parser("set-intent", help="Toggle lossless-on-disk for a request")
    p_intent.add_argument("id", type=int, help="Request ID")
    p_intent.add_argument("intent", choices=["lossless", "default"],
                          help="'lossless' = keep lossless on disk, 'default' = pipeline decides")

    # force-import
    p_force = sub.add_parser("force-import", help="Force-import a rejected download by download_log ID")
    p_force.add_argument("download_log_id", type=int, help="Download log ID")
    p_force.add_argument("--verified-lossless-target",
                         help="Override the runtime verified-lossless target for this import")

    # manual-import
    p_manual = sub.add_parser("manual-import", help="Import a local folder as a pipeline request")
    p_manual.add_argument("id", type=int, help="Pipeline request ID")
    p_manual.add_argument("path", help="Path to album folder")
    p_manual.add_argument("--verified-lossless-target",
                          help="Override the runtime verified-lossless target for this import")

    # import-jobs
    p_jobs = sub.add_parser("import-jobs", help="List recent import queue jobs")
    p_jobs.add_argument("--status", choices=["queued", "running", "completed", "failed"])
    p_jobs.add_argument("--limit", type=int, default=20)

    # import-preview
    p_preview = sub.add_parser("import-preview", help="Preview whether an import would pass")
    p_preview.add_argument("--download-log-id", type=int,
                           help="Preview the failed_path from a download_log row")
    p_preview.add_argument("--request-id", type=int,
                           help="Request ID for --path preview")
    p_preview.add_argument("--path", help="Preview a real folder for a request")
    p_preview.add_argument("--no-force", action="store_true",
                           help="Do not pass --force to import_one.py preview")
    p_preview.add_argument("--values", action="store_true",
                           help="Preview typed override values instead of a real folder")
    p_preview.add_argument("--values-json",
                           help="JSON object with ImportPreviewValues fields")
    p_preview.add_argument("--json", action="store_true",
                           help="Print the common preview result as JSON")
    p_preview.add_argument("--is-flac", action="store_true", default=None)
    p_preview.add_argument("--min-bitrate", type=int)
    p_preview.add_argument("--is-cbr", action="store_true", default=None)
    p_preview.add_argument("--is-vbr", action="store_true", default=None)
    p_preview.add_argument("--avg-bitrate", type=int)
    p_preview.add_argument("--spectral-grade", choices=SPECTRAL_GRADE_CHOICES)
    p_preview.add_argument("--spectral-bitrate", type=int)
    p_preview.add_argument("--existing-min-bitrate", type=int)
    p_preview.add_argument("--existing-avg-bitrate", type=int)
    p_preview.add_argument("--existing-spectral-bitrate", type=int)
    p_preview.add_argument("--existing-spectral-grade", choices=SPECTRAL_GRADE_CHOICES)
    p_preview.add_argument("--override-min-bitrate", type=int)
    p_preview.add_argument("--existing-format")
    p_preview.add_argument("--existing-is-cbr", action="store_true", default=None)
    p_preview.add_argument("--post-conversion-min-bitrate", type=int)
    p_preview.add_argument("--converted-count", type=int)
    p_preview.add_argument("--verified-lossless", action="store_true", default=None)
    p_preview.add_argument("--verified-lossless-target")
    p_preview.add_argument("--target-format")
    p_preview.add_argument("--new-format")
    p_preview.add_argument("--audio-check-mode")
    p_preview.add_argument("--audio-corrupt", action="store_true", default=None)
    p_preview.add_argument("--import-mode")
    p_preview.add_argument("--has-nested-audio", action="store_true", default=None)

    # wrong-match-triage
    p_triage = sub.add_parser(
        "wrong-match-triage",
        help="Clean the full Wrong Matches queue using existing evidence",
    )
    p_triage.add_argument("--apply", action="store_true",
                          help="Allow destructive full-queue cleanup")
    p_triage.add_argument("--json", action="store_true")

    # wrong-match-delete
    p_wm_delete = sub.add_parser(
        "wrong-match-delete",
        help="Delete one visible Wrong Matches source folder",
    )
    p_wm_delete.add_argument("download_log_id", type=int)
    p_wm_delete.add_argument("--apply", action="store_true",
                             help="Allow destructive source deletion")
    p_wm_delete.add_argument("--json", action="store_true")

    # wrong-match-delete-group
    p_wm_delete_group = sub.add_parser(
        "wrong-match-delete-group",
        help="Delete visible Wrong Matches source folders for one request",
    )
    p_wm_delete_group.add_argument("request_id", type=int)
    p_wm_delete_group.add_argument("--apply", action="store_true",
                                   help="Allow destructive source deletion")
    p_wm_delete_group.add_argument("--json", action="store_true")

    # repair-spectral
    p_repair = sub.add_parser("repair-spectral",
                              help="Fix albums stuck by stale current_spectral_bitrate (#18)")
    p_repair.add_argument("--dry-run", action="store_true",
                          help="Show what would be repaired without changing anything")

    # replace
    p_replace = sub.add_parser(
        "replace",
        help="Supersede a request with a new row at a different release id "
             "in the same release group/master (same pathway as the source)")
    p_replace.add_argument("id", type=int, help="Source request ID")
    p_replace.add_argument(
        "--to", dest="target_mb_release_id", required=True,
        help="Target release id — MB UUID or Discogs numeric id; must "
             "share the source's pathway and release group/master")
    p_replace.add_argument("--json", action="store_true",
                           help="Print structured JSON instead of text")

    # beets-distance
    p_bd = sub.add_parser(
        "beets-distance",
        help="Real beets-distance between a download_log's audio and an MBID "
             "(refuses if MBID is outside the request's release group)")
    p_bd.add_argument("download_log_id", type=int,
                      help="download_log row id (see `pipeline-cli show <req>`)")
    p_bd.add_argument("mbid",
                      help="Candidate release id — MB UUID or Discogs numeric id")
    p_bd.add_argument("--json", action="store_true",
                      help="Print structured JSON instead of text")

    # youtube-album (U7): MBID/Discogs ID → YT Music album matrix.
    # Counterpart of ``GET /api/youtube-album`` (U8).
    p_ya = sub.add_parser(
        "youtube-album",
        help="Resolve MBID/Discogs ID → YouTube Music album matrix "
             "(auto-widens to release group; N×M beets distances per "
             "YT sibling × MB sibling)",
    )
    p_ya.add_argument(
        "identifier",
        help="MB release/release-group MBID OR Discogs release/master ID "
             "(service auto-discriminates via leaf-then-group fallback)",
    )
    p_ya.add_argument(
        "--refresh", action="store_true",
        help="Bypass BOTH the durable cache (youtube_album_mappings) "
             "AND the in-process Redis HTTP accelerator, forcing a "
             "fresh YouTube Music fetch. The fresh response is then "
             "written back to both layers. (Default: serve from cache.)",
    )
    p_ya.add_argument(
        "--json", action="store_true",
        help="Print structured JSON instead of human-readable matrix",
    )

    # youtube-rescue (U4): submit a YouTube Music rescue ingest for one
    # request. Counterpart of ``POST /api/pipeline/<id>/youtube-rescue``
    # (U5). Both surfaces wrap ``YoutubeIngestService.submit``.
    p_yr = sub.add_parser(
        "youtube-rescue",
        help="Submit a YouTube Music rescue ingest for one request "
             "(requires a resolver mapping; emits a youtube_running "
             "download_log row).",
    )
    p_yr.add_argument(
        "request_id", type=int,
        help="album_requests.id to attach the rescue to",
    )
    p_yr.add_argument(
        "browse_id",
        help="YouTube Music browse_id (e.g. MPREb_...); must already "
             "be cached in youtube_album_mappings for this request's "
             "release group",
    )
    p_yr.add_argument(
        "--json", action="store_true",
        help="Print structured JSON ({outcome, download_log_id, detail}) "
             "instead of plain text.",
    )

    # routes (U18 step 3): self-document the CLI surface. Mirrors
    # ``GET /api/_index`` on the web side; both are read-only and zero-arg.
    p_routes = sub.add_parser(
        "routes",
        help="Self-document the CLI surface — every subcommand, "
             "its args, and its description.",
    )
    p_routes.add_argument(
        "--json", action="store_true",
        help="Emit JSON instead of human-readable text.",
    )

    return parser, p_sp, p_triage_op


def _describe_argparse_action(
    action: argparse.Action,
) -> str | None:
    """Render one argparse Action as a human-readable arg label.

    Returns the metavar / option-string form with a hint for type or
    choices when present. Returns None for the subparsers placeholder
    (those are sibling subcommands, not arguments).
    """
    if isinstance(action, argparse._SubParsersAction):  # noqa: SLF001
        return None
    if isinstance(action, argparse._HelpAction):  # noqa: SLF001
        return None
    label: str
    if action.option_strings:
        label = action.option_strings[0]
    else:
        metavar = action.metavar
        # ``metavar`` is typed ``str | tuple[str, ...] | None`` upstream;
        # nargs forms like ``nargs="?"`` can yield a tuple here. Render
        # tuples by joining for a stable string representation.
        if isinstance(metavar, tuple):
            label = " ".join(str(m) for m in metavar)
        else:
            label = metavar or action.dest
    type_hint: str | None = None
    if action.choices:
        type_hint = "{" + ",".join(str(c) for c in action.choices) + "}"
    elif action.type is int:
        type_hint = "int"
    elif action.type is float:
        type_hint = "float"
    if type_hint:
        return f"{label} ({type_hint})"
    return label


def _collect_cli_routes(
    parser: argparse.ArgumentParser,
) -> list[dict[str, object]]:
    """Walk a parser's subparsers and emit one entry per leaf subcommand.

    Returns a list of ``{subcommand, args, description}`` rows sorted by
    ``subcommand``. Nested subparsers (e.g. ``search-plan show``) emit
    one row per leaf path; the parent ``search-plan`` itself is not
    emitted because its help is already covered by its children.
    """
    rows: list[dict[str, object]] = []

    def _walk(p: argparse.ArgumentParser, prefix: str) -> None:
        sub_actions = [
            a for a in p._actions  # noqa: SLF001
            if isinstance(a, argparse._SubParsersAction)  # noqa: SLF001
        ]
        if not sub_actions:
            return
        for sub_action in sub_actions:
            for name, sub_parser in sub_action.choices.items():
                label = f"{prefix} {name}".strip()
                # Recurse first to detect leaves.
                nested = [
                    a for a in sub_parser._actions  # noqa: SLF001
                    if isinstance(a, argparse._SubParsersAction)  # noqa: SLF001
                    and a.choices
                ]
                if nested:
                    _walk(sub_parser, label)
                    continue
                args: list[str] = []
                for action in sub_parser._actions:  # noqa: SLF001
                    rendered = _describe_argparse_action(action)
                    if rendered is not None:
                        args.append(rendered)
                description = ""
                # Recover the parent's help text for this subcommand.
                # argparse stores per-choice help on
                # ``_SubParsersAction._choices_actions`` (a private list of
                # ``_ChoicesPseudoAction`` whose ``dest`` matches the name).
                choices_actions = getattr(
                    sub_action, "_choices_actions", []) or []
                for ca in choices_actions:
                    if ca.dest == name:
                        description = ca.help or ""
                        break
                rows.append({
                    "subcommand": label,
                    "args": args,
                    "description": description,
                })

    _walk(parser, "")
    rows.sort(key=lambda r: str(r["subcommand"]))
    return rows


def cmd_routes(db, args) -> int:
    """Emit every CLI subcommand with its args and description.

    Reads the parser from ``_build_parser`` so the listing cannot drift
    from the actual surface — adding a subparser anywhere updates this
    output automatically.
    """
    parser, _, _ = _build_parser()
    rows = _collect_cli_routes(parser)
    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2))
        return 0
    for row in rows:
        raw_args = row["args"]
        args_list = raw_args if isinstance(raw_args, list) else []
        args_str = " ".join(str(a) for a in args_list) if args_list else ""
        if args_str:
            print(f"{row['subcommand']}  [{args_str}]")
        else:
            print(f"{row['subcommand']}")
        desc = row["description"]
        if desc:
            print(f"    {desc}")
    return 0
