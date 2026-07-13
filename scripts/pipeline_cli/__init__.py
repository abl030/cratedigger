"""scripts.pipeline_cli — the ``pipeline-cli`` operator CLI (issue #495).

Split from the 3,810-line ``scripts/pipeline_cli.py`` monolith into one
module per command family, mirroring the ``web/routes/`` layout (same
mechanical pattern as the ``lib/quality/`` split, issue #477):

    album_requests.py   list / add / status / retry / cancel / set /
                       set-intent / disk-coverage + the MB-fetch helpers
                       (named album_requests, not requests, to avoid
                       shadowing the third-party ``requests`` package —
                       see the __main__.py docstring)
    query.py            query (read-only SQL escape hatch)
    show.py             show (full per-request detail dump)
    quality.py           quality / repair-spectral (quality-gate debug)
    imports.py           force-import / manual-import / import-jobs /
                       import-preview
    wrong_match.py       wrong-match-triage / -delete / -delete-group
    search_plan.py       search-plan show / regenerate / dry-run /
                       saturation / advance / history
    replace.py           replace
    beets_distance.py    beets-distance
    destructive.py       ban-source / library-delete
    youtube.py           youtube-album / youtube-rescue
    triage.py             triage show / list / quarantine
    long_tail.py          long-tail
    routes_meta.py       argparse tree construction (``_build_parser``)
                       + ``routes`` self-documentation
    cli.py               ``main()`` + the command dispatch dicts
    __main__.py           thin script-mode entry shim (nix wrappers exec
                       this file directly)

The package root exposes the supported operator and API surface listed in
``__all__``. Private helpers live only in their defining command-family
modules; tests and patch sites import those owners directly.

Note on ``finalize_request``: it's a module-level DI seam
(``finalize_request = transitions.finalize_request``) bound
independently in BOTH ``album_requests.py`` (retry/cancel/set/set-intent)
and ``quality.py`` (repair-spectral) — same pattern as
``web.routes.pipeline_mutations.finalize_request`` / ``harness.import_one.finalize_request``.
Patches targeting a specific command's test must patch the module that
actually calls it (e.g. ``scripts.pipeline_cli.album_requests.finalize_request``),
not this re-export. This package re-exports the ``album_requests.py``
binding as part of its supported public surface.
"""

import logging
import sys

# Surface INFO-level log lines (e.g. the [import] stderr passthrough from
# dispatch_import_core) so force-import / manual-import failures are visible to
# the user instead of silently swallowed by Python's default WARNING-only
# logger configuration.
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stderr,
)

from scripts.pipeline_cli.album_requests import (
    VALID_STATUSES,
    cmd_add,
    cmd_cancel,
    cmd_disk_coverage,
    cmd_list,
    cmd_retry,
    cmd_set,
    cmd_set_intent,
    cmd_status,
    fetch_mb_release,
    finalize_request,
    tracks_from_mb_release,
)
from scripts.pipeline_cli.query import cmd_query
from scripts.pipeline_cli.show import cmd_show
from scripts.pipeline_cli.quality import (
    cmd_quality,
    cmd_repair_spectral,
)
from scripts.pipeline_cli.imports import (
    SLSKD_DOWNLOAD_DIRS,
    SPECTRAL_GRADE_CHOICES,
    cmd_force_import,
    cmd_import_jobs,
    cmd_import_preview,
    cmd_manual_import,
)
from scripts.pipeline_cli.wrong_match import (
    cmd_wrong_match_delete,
    cmd_wrong_match_delete_group,
    cmd_wrong_match_triage,
)
from scripts.pipeline_cli.search_plan import (
    cmd_search_plan_advance,
    cmd_search_plan_dry_run,
    cmd_search_plan_history,
    cmd_search_plan_regenerate,
    cmd_search_plan_saturation,
    cmd_search_plan_show,
)
from scripts.pipeline_cli.replace import cmd_replace
from scripts.pipeline_cli.beets_distance import cmd_beets_distance
from scripts.pipeline_cli.destructive import cmd_ban_source, cmd_library_delete
from scripts.pipeline_cli.youtube import (
    OUTCOME_EXIT_CODE,
    cmd_youtube_album,
    cmd_youtube_rescue,
    resolve_youtube_album,
)
from scripts.pipeline_cli.triage import (
    cmd_triage_list,
    cmd_triage_quarantine,
    cmd_triage_show,
)
from scripts.pipeline_cli.long_tail import cmd_long_tail
from scripts.pipeline_cli.routes_meta import cmd_routes
from scripts.pipeline_cli.cli import PipelineDB, main

__all__ = [
    "OUTCOME_EXIT_CODE",
    "PipelineDB",
    "SLSKD_DOWNLOAD_DIRS",
    "SPECTRAL_GRADE_CHOICES",
    "VALID_STATUSES",
    "cmd_add",
    "cmd_beets_distance",
    "cmd_ban_source",
    "cmd_cancel",
    "cmd_disk_coverage",
    "cmd_force_import",
    "cmd_import_jobs",
    "cmd_import_preview",
    "cmd_list",
    "cmd_library_delete",
    "cmd_long_tail",
    "cmd_manual_import",
    "cmd_quality",
    "cmd_query",
    "cmd_repair_spectral",
    "cmd_replace",
    "cmd_retry",
    "cmd_routes",
    "cmd_search_plan_advance",
    "cmd_search_plan_dry_run",
    "cmd_search_plan_history",
    "cmd_search_plan_regenerate",
    "cmd_search_plan_saturation",
    "cmd_search_plan_show",
    "cmd_set",
    "cmd_set_intent",
    "cmd_show",
    "cmd_status",
    "cmd_triage_list",
    "cmd_triage_quarantine",
    "cmd_triage_show",
    "cmd_wrong_match_delete",
    "cmd_wrong_match_delete_group",
    "cmd_wrong_match_triage",
    "cmd_youtube_album",
    "cmd_youtube_rescue",
    "fetch_mb_release",
    "finalize_request",
    "main",
    "resolve_youtube_album",
    "tracks_from_mb_release",
]
