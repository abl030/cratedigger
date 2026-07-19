"""pipeline-cli argparse construction + self-documentation (#495 carve;
per-family split #521).

``_build_parser`` builds the entire subcommand tree. The actual
``sub.add_parser(...)`` argument definitions are colocated with each
command family's handler module (``add_<family>_subparsers`` functions);
this module just creates the top-level parser + subparsers object and
calls each family's builder in turn — the "shared registry" every family
plugs into, mirroring ``cli.py``'s dispatch dict. ``cmd_routes`` walks
the assembled parser to self-document the CLI surface, mirroring
``GET /api/_index`` on the web side.
"""

import argparse
import json

from lib.pipeline_db import DEFAULT_DSN
from scripts.pipeline_cli.album_requests import add_album_requests_subparsers
from scripts.pipeline_cli.beets_distance import add_beets_distance_subparser
from scripts.pipeline_cli.destructive import add_destructive_subparsers
from scripts.pipeline_cli.imports import add_imports_subparsers
from scripts.pipeline_cli.long_tail import add_long_tail_subparser
from scripts.pipeline_cli.quality import add_quality_subparsers
from scripts.pipeline_cli.query import add_query_subparser
from scripts.pipeline_cli.replace import add_replace_subparser
from scripts.pipeline_cli.search_plan import add_search_plan_subparser
from scripts.pipeline_cli.show import add_show_subparser
from scripts.pipeline_cli.triage import add_triage_subparser
from scripts.pipeline_cli.wrong_match import add_wrong_match_subparsers
from scripts.pipeline_cli.youtube import add_youtube_subparsers


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

    # list / add / status / disk-coverage / retry / cancel / set / set-intent
    add_album_requests_subparsers(sub)

    # query
    add_query_subparser(sub)

    # show
    add_show_subparser(sub)

    # search-plan (+ nested show/regenerate/advance/dry-run/saturation/history)
    p_sp = add_search_plan_subparser(sub)

    # triage (+ nested show/list/quarantine)
    p_triage_op = add_triage_subparser(sub)

    # long-tail
    add_long_tail_subparser(sub)

    # quality / repair-spectral
    add_quality_subparsers(sub)

    # force-import / import-jobs / import-preview
    add_imports_subparsers(sub)

    # wrong-match-triage / wrong-match-delete / wrong-match-delete-group
    add_wrong_match_subparsers(sub)

    # replace
    add_replace_subparser(sub)

    # beets-distance
    add_beets_distance_subparser(sub)

    # ban-source / library-delete
    add_destructive_subparsers(sub)

    # youtube-album / youtube-rescue
    add_youtube_subparsers(sub)

    # routes (U18 step 3): self-document the CLI surface. Mirrors
    # ``GET /api/_index`` on the web side; both are read-only and zero-arg.
    # Stays inline here (rather than its own family module) since it's
    # the self-documentation command for this very module.
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
