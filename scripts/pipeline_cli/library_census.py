"""pipeline-cli ``library-census`` command (issue #1472).

Lists what the library-completeness census found, the same projection the
dashboard card renders, without its per-category cap. A thin adapter over
``GET /api/pipeline/library-census`` (the one code path,
``web.routes.pipeline_dashboard.library_completeness_view``).
``--category track_length`` is the track-length worklist; each row names the
request the existing actions take (``mark-incomplete``, ``mark-complete``,
``ban-source``, ``replace``).

Exit codes follow the route's status: 0 listed, 2 unknown category, 5 the
web API could not be reached.
"""

from __future__ import annotations

import argparse
from urllib.parse import urlencode

from lib.json_narrow import is_str_object_dict, json_list
from scripts.pipeline_cli.api_mutations import (
    _ApiMutation,
    relay_rendered,
    render_api_error,
)

CATEGORIES = (
    "missing_source_audio", "catalog_drift", "unknown",
    "track_length", "track_length_harmless",
)


def _finding_text(finding: dict[str, object]) -> str:
    sentence = finding.get("sentence")
    if isinstance(sentence, str) and sentence:
        return sentence
    detail = finding.get("detail")
    return detail if isinstance(detail, str) else ""


def _render_library_census(status: int, payload: dict[str, object]) -> None:
    if status != 200:
        render_api_error(status, payload)
        return
    state = payload.get("state")
    if state != "ok":
        print(f"census: {state}" + (
            f" ({payload['error']})" if payload.get("error") else ""))
        return
    snapshot = payload.get("snapshot")
    report = snapshot.get("report") if is_str_object_dict(snapshot) else None
    if not is_str_object_dict(report) or not is_str_object_dict(snapshot):
        render_api_error(status, payload)
        return
    counts = report.get("counts")
    if is_str_object_dict(counts):
        print(
            f"census {snapshot.get('generated_at')}: "
            f"{counts.get('albums_scanned')} albums; track lengths measured "
            f"for {counts.get('track_length_measured', 0)}, "
            f"{counts.get('track_length_pending', 0)} waiting, "
            f"{counts.get('track_length_unlisted', 0)} with no listed lengths")
    albums = [a for a in json_list(report.get("albums")) if is_str_object_dict(a)]
    if not albums:
        print("no albums listed")
    for album in albums:
        request_id = album.get("request_id")
        marks = [
            label for flag, label in (
                ("marked_complete", "marked complete"),
                ("marked_incomplete", "marked incomplete"),
            ) if album.get(flag) is True
        ]
        request = f"request {request_id}" if request_id is not None else "no request"
        suffix = f" [{', '.join(marks)}]" if marks else ""
        print(f"#{album.get('album_id')} {album.get('artist')} — "
              f"{album.get('title')} ({request}){suffix}")
        for finding in json_list(album.get("findings")):
            if is_str_object_dict(finding):
                print(f"  {finding.get('kind')}: {_finding_text(finding)}")


def cmd_library_census(_db: object, args: argparse.Namespace) -> int:
    category = getattr(args, "category", None)
    query = f"?{urlencode({'category': category})}" if category else ""
    return relay_rendered(
        args.api_endpoint,
        _ApiMutation(
            path=f"/api/pipeline/library-census{query}", body={}, method="GET"),
        render=_render_library_census,
        json_output=getattr(args, "json", False),
    )


def add_library_census_subparser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    p = sub.add_parser(
        "library-census",
        help="List what the library-completeness census found, including "
             "installed copies whose track lengths contradict their release "
             "(read-only)")
    p.add_argument(
        "--category", choices=CATEGORIES, default=None,
        help="Only this band (track_length is the track-length worklist)")
    p.add_argument("--json", action="store_true",
                   help="Print the route's payload as JSON")
