"""pipeline-cli ``beets-distance`` command (#495 carve).

Real beets-distance between a download_log's failed_path and an MBID.

The folder it reads lives under the private ``0700`` processing tree, so
the command executes through the canonical web route over the
permissioned Unix socket (issue #1063) instead of reading the tree as the
invoking operator, where every protected candidate answered
``folder_missing``.
"""

from __future__ import annotations

import argparse
import urllib.parse

from lib.json_narrow import is_str_object_dict
from scripts.pipeline_cli.api_mutations import (
    TIMEOUT_FOLDER_READ_SECONDS,
    _ApiMutation,
    relay_rendered,
    render_api_error,
)


def _render_beets_distance(status: int, payload: dict[str, object]) -> None:
    if payload.get("outcome") is None:
        render_api_error(status, payload)
        return
    print(f"  download_log_id:        {payload.get('download_log_id')}")
    print(f"  request_id:             {payload.get('request_id')}")
    print(f"  candidate_mbid:         {payload.get('candidate_mbid')}")
    print(f"  outcome:                {payload.get('outcome')}")
    distance = payload.get("distance")
    if isinstance(distance, (int, float)):
        print(f"  distance:               {float(distance):.4f}")
    matched = payload.get("matched_tracks")
    if isinstance(matched, int):
        print(f"  matched tracks:         "
              f"{matched} / {payload.get('total_mb_tracks')} "
              f"({payload.get('total_local_tracks')} local)")
    components = payload.get("components")
    if is_str_object_dict(components) and components:
        print("  components:")
        for key, value in sorted(components.items()):
            if isinstance(value, (int, float)):
                print(f"    {key:<24} {float(value):.4f}")
    if payload.get("folder_path"):
        print(f"  folder:                 {payload['folder_path']}")
    if payload.get("duration_ms") is not None:
        print(f"  latency:                {payload['duration_ms']} ms")
    if payload.get("error_message"):
        print(f"  error:                  {payload['error_message']}")


def cmd_beets_distance(_db: object, args: argparse.Namespace) -> int:
    """Real beets-distance between a download_log's failed_path and an MBID.

    Thin adapter over ``GET /api/beets-distance/<download_log_id>/<mbid>``,
    which is the one execution path for both surfaces (see ``CLAUDE.md``
    § "CLI ⇄ API surface symmetry").

    ``args.mbid`` may be an MB release UUID or a bare Discogs numeric
    release id (#530); the route owns that dispatch.

    Exit codes, derived from that route's status codes:
      * 0 — 200 ``ok``
      * 2 — 404 ``download_log_not_found``, ``request_not_found`` (and an
            id shape the route does not accept)
      * 3 — 422 ``mb_no_release_group``, ``wrong_release_group``
      * 4 — 410 ``folder_missing``, ``no_audio`` (the artifacts we wanted
            to compare are gone)
      * 5 — 503 ``mb_lookup_failed`` (transient mirror failure) or
            ``folder_unavailable`` (the folder could not be observed)
      * 1 — 500 ``distance_failed`` / unknown outcome
    """
    return relay_rendered(
        args.api_endpoint,
        _ApiMutation(
            path=(
                f"/api/beets-distance/{int(args.download_log_id)}/"
                f"{urllib.parse.quote(str(args.mbid), safe='')}"
            ),
            body={},
            method="GET",
        ),
        render=_render_beets_distance,
        json_output=getattr(args, "json", False),
        timeout_seconds=TIMEOUT_FOLDER_READ_SECONDS,
        # 410 Gone is this route's "the artifacts we wanted to compare
        # are gone" status; the in-process command has always exited 4
        # for it. Scoped here rather than made global so a future route
        # using 410 cannot silently inherit "wrong state" (#1063 T4.4).
        exit_overrides={410: 4, 500: 1},
    )


def add_beets_distance_subparser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add ``beets-distance`` (#521 carve out of
    ``routes_meta._build_parser``, verbatim argument definitions)."""
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
