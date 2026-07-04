"""pipeline-cli entry point + subcommand dispatch (#495 carve).

``main()`` builds the parser (``routes_meta._build_parser``), parses argv,
and dispatches to the handler functions defined across the command-family
modules. This is the one place that imports every ``cmd_*`` — the
dispatch dicts are the "shared registry" argparse-construction and
``routes`` self-doc both key off of, alongside the parser tree itself.
"""

import sys

from lib.pipeline_db import PipelineDB

from scripts.pipeline_cli.beets_distance import cmd_beets_distance
from scripts.pipeline_cli.imports import (
    cmd_force_import,
    cmd_import_jobs,
    cmd_import_preview,
    cmd_manual_import,
)
from scripts.pipeline_cli.long_tail import cmd_long_tail
from scripts.pipeline_cli.quality import cmd_quality, cmd_repair_spectral
from scripts.pipeline_cli.replace import cmd_replace
from scripts.pipeline_cli.album_requests import (
    cmd_add,
    cmd_cancel,
    cmd_disk_coverage,
    cmd_list,
    cmd_retry,
    cmd_set,
    cmd_set_intent,
    cmd_status,
)
from scripts.pipeline_cli.query import cmd_query
from scripts.pipeline_cli.routes_meta import _build_parser, cmd_routes
from scripts.pipeline_cli.search_plan import (
    cmd_search_plan_advance,
    cmd_search_plan_dry_run,
    cmd_search_plan_history,
    cmd_search_plan_regenerate,
    cmd_search_plan_saturation,
    cmd_search_plan_show,
)
from scripts.pipeline_cli.show import cmd_show
from scripts.pipeline_cli.triage import cmd_triage_list, cmd_triage_show
from scripts.pipeline_cli.wrong_match import (
    cmd_wrong_match_delete,
    cmd_wrong_match_delete_group,
    cmd_wrong_match_triage,
)
from scripts.pipeline_cli.youtube import cmd_youtube_album, cmd_youtube_rescue


def main():
    # Mirror origins for every web.mb / web.discogs consumer in this
    # process (add --discogs, youtube-album, distance, Replace, field
    # resolution). Without this, the CLI silently runs against public MB
    # and an unset Discogs base (tier-2 U6 wiring).
    from web.api_bases import configure_api_bases_from_runtime_config
    configure_api_bases_from_runtime_config()

    parser, p_sp, p_triage_op = _build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    if args.command == "search-plan" and not getattr(
            args, "search_plan_command", None):
        p_sp.print_help()
        sys.exit(1)
    if args.command == "triage" and not getattr(
            args, "triage_command", None):
        p_triage_op.print_help()
        sys.exit(1)

    # ``routes`` is the only subcommand that doesn't require a DB
    # connection — short-circuit before constructing PipelineDB so the
    # command works without a reachable database.
    if args.command == "routes":
        rc = cmd_routes(None, args)
        if isinstance(rc, int):
            sys.exit(rc)
        return

    db = PipelineDB(args.dsn)

    commands = {
        "list": cmd_list,
        "add": cmd_add,
        "query": cmd_query,
        "status": cmd_status,
        "disk-coverage": cmd_disk_coverage,
        "retry": cmd_retry,
        "cancel": cmd_cancel,
        "set": cmd_set,
        "set-intent": cmd_set_intent,
        "show": cmd_show,
        "quality": cmd_quality,
        "force-import": cmd_force_import,
        "manual-import": cmd_manual_import,
        "import-jobs": cmd_import_jobs,
        "import-preview": cmd_import_preview,
        "wrong-match-triage": cmd_wrong_match_triage,
        "wrong-match-delete": cmd_wrong_match_delete,
        "wrong-match-delete-group": cmd_wrong_match_delete_group,
        "repair-spectral": cmd_repair_spectral,
        "replace": cmd_replace,
        "beets-distance": cmd_beets_distance,
        "youtube-album": cmd_youtube_album,
        "youtube-rescue": cmd_youtube_rescue,
        "long-tail": cmd_long_tail,
    }
    search_plan_commands = {
        "show": cmd_search_plan_show,
        "regenerate": cmd_search_plan_regenerate,
        "advance": cmd_search_plan_advance,
        "history": cmd_search_plan_history,
        "dry-run": cmd_search_plan_dry_run,
        "saturation": cmd_search_plan_saturation,
    }
    triage_commands = {
        "show": cmd_triage_show,
        "list": cmd_triage_list,
    }
    try:
        if args.command == "search-plan":
            rc = search_plan_commands[args.search_plan_command](db, args)
        elif args.command == "triage":
            rc = triage_commands[args.triage_command](db, args)
        else:
            rc = commands[args.command](db, args)
    finally:
        db.close()
    if isinstance(rc, int):
        sys.exit(rc)
