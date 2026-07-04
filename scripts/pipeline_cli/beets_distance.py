"""pipeline-cli ``beets-distance`` command (#495 carve).

Real beets-distance between a download_log's failed_path and an MBID.
"""

import json

from scripts.pipeline_cli._format import _json_default


def cmd_beets_distance(db, args):
    """Real beets-distance between a download_log's failed_path and an MBID.

    Counterpart of ``GET /api/beets-distance/<download_log_id>/<mbid>``.
    Both surfaces wrap ``lib.beets_distance.compute_beets_distance`` —
    keep them in sync (see ``CLAUDE.md`` § "CLI ⇄ API surface
    symmetry").

    Exit codes:
      * 0 — ``ok``
      * 2 — ``download_log_not_found``, ``request_not_found``
      * 3 — ``mb_no_release_group``, ``wrong_release_group`` (semantic
            input violations, including the cross-RG guardrail)
      * 4 — ``folder_missing``, ``no_audio`` (the artifacts we wanted
            to compare are gone)
      * 5 — ``mb_lookup_failed`` (transient MB-mirror failure)
      * 1 — ``distance_failed`` / unknown outcome
    """
    from lib.beets_distance import compute_beets_distance
    from web import mb as mb_api

    result = compute_beets_distance(
        int(args.download_log_id),
        args.mbid,
        pdb=db,
        mb_get_release=lambda m: mb_api.get_release(m, fresh=False),
        cache=None,
    )

    payload = {
        "outcome": result.outcome,
        "distance": result.distance,
        "matched_tracks": result.matched_tracks,
        "total_local_tracks": result.total_local_tracks,
        "total_mb_tracks": result.total_mb_tracks,
        "extra_local_tracks": result.extra_local_tracks,
        "extra_mb_tracks": result.extra_mb_tracks,
        "components": result.components,
        "request_release_group_id": result.request_release_group_id,
        "candidate_release_group_id": result.candidate_release_group_id,
        "candidate_mbid": result.candidate_mbid,
        "download_log_id": result.download_log_id,
        "request_id": result.request_id,
        "folder_path": result.folder_path,
        "error_message": result.error_message,
        "duration_ms": result.duration_ms,
    }
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  download_log_id:        {result.download_log_id}")
        print(f"  request_id:             {result.request_id}")
        print(f"  candidate_mbid:         {result.candidate_mbid}")
        print(f"  outcome:                {result.outcome}")
        if result.distance is not None:
            print(f"  distance:               {result.distance:.4f}")
        if result.matched_tracks is not None:
            print(f"  matched tracks:         "
                  f"{result.matched_tracks} / {result.total_mb_tracks} "
                  f"({result.total_local_tracks} local)")
        if result.components:
            print("  components:")
            for k, v in sorted(result.components.items()):
                print(f"    {k:<24} {v:.4f}")
        if result.folder_path:
            print(f"  folder:                 {result.folder_path}")
        if result.duration_ms is not None:
            print(f"  latency:                {result.duration_ms} ms")
        if result.error_message:
            print(f"  error:                  {result.error_message}")

    if result.outcome == "ok":
        return 0
    if result.outcome in ("download_log_not_found", "request_not_found"):
        return 2
    if result.outcome in ("mb_no_release_group", "wrong_release_group"):
        return 3
    if result.outcome in ("folder_missing", "no_audio"):
        return 4
    if result.outcome == "mb_lookup_failed":
        return 5
    return 1
