"""pipeline-cli ``other-pressings`` command (issue #1445).

Does one download_log row's copy match another pressing in its release
group, and do the target's listed lengths come from a real disc? A thin
adapter over ``GET /api/other-pressings/<download_log_id>`` (the one code
path, ``lib/other_pressing_check.py``). Prints the operator sentences;
``--json`` prints the typed result.

Exit codes follow the route's status: 0 answered, 2 unknown row, 5 the
mirror could not be asked.
"""

from __future__ import annotations

import argparse

from lib.json_narrow import is_str_object_dict, json_list
from scripts.pipeline_cli.api_mutations import (
    TIMEOUT_PRESSING_CHECK_SECONDS,
    _ApiMutation,
    relay_rendered,
    render_api_error,
)


def _render_other_pressings(status: int, payload: dict[str, object]) -> None:
    sentences = json_list(payload.get("sentences"))
    if not sentences:
        render_api_error(status, payload)
        return
    for sentence in sentences:
        print(sentence)
    target_url = payload.get("target_url")
    if isinstance(target_url, str):
        print(f"  release: {target_url}")
    for match in json_list(payload.get("matching_pressings")):
        if is_str_object_dict(match) and isinstance(match.get("url"), str):
            print(f"  matching pressing: {match.get('description')} "
                  f"{match.get('url')}")


def cmd_other_pressings(_db: object, args: argparse.Namespace) -> int:
    return relay_rendered(
        args.api_endpoint,
        _ApiMutation(
            path=f"/api/other-pressings/{int(args.download_log_id)}",
            body={},
            method="GET",
        ),
        render=_render_other_pressings,
        json_output=getattr(args, "json", False),
        timeout_seconds=TIMEOUT_PRESSING_CHECK_SECONDS,
    )


def add_other_pressings_subparser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    p = sub.add_parser(
        "other-pressings",
        help="Check whether a download_log row's files match another "
             "pressing in its MusicBrainz release group (read-only)")
    p.add_argument("download_log_id", type=int,
                   help="download_log row id (see `pipeline-cli show <req>`)")
    p.add_argument("--json", action="store_true",
                   help="Print the typed result as JSON")
