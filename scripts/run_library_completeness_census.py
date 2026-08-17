"""Daily, read-only full-library completeness census (#1149)."""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.beets_db import open_beets_db
from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from lib.config import resolve_startup_config_paths
from lib.library_completeness import CompletenessBeets, scan_library_completeness
from lib.library_completeness_snapshot import (
    LibraryCompletenessSnapshot,
    library_completeness_snapshot_path,
    write_library_completeness_snapshot,
)
from lib.mb_canonical import (
    TaggedCanonicalReleaseFn,
    configure_canonical_release_lookup,
    production_tagged_canonical_release_fn,
)
from web import discogs, mb
from web.api_bases import configure_api_bases_from_runtime_config

logger = logging.getLogger("cratedigger-library-completeness")
EXIT_BEETS_UNAVAILABLE = 1
EXIT_CONFIG_ABORT = 2
EXIT_RUN_FAILED = 3


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def run_library_completeness_census(
    beets: CompletenessBeets, *,
    fetch_musicbrainz_raw: Callable[[str], dict[str, object]],
    fetch_discogs_raw: Callable[[str], dict[str, object]],
    resolve_musicbrainz_redirect: TaggedCanonicalReleaseFn | None = None,
    time_fn: Callable[[], float] = time.monotonic,
    now_fn: Callable[[], str] = _now_iso,
) -> LibraryCompletenessSnapshot:
    generated_at = now_fn()
    started = time_fn()
    report = scan_library_completeness(
        beets, fetch_musicbrainz_raw=fetch_musicbrainz_raw,
        fetch_discogs_raw=fetch_discogs_raw,
        resolve_musicbrainz_redirect=resolve_musicbrainz_redirect,
    )
    return LibraryCompletenessSnapshot(generated_at, time_fn() - started, report)


def publish_library_completeness_census(
    path: str, beets: CompletenessBeets, *,
    fetch_musicbrainz_raw: Callable[[str], dict[str, object]],
    fetch_discogs_raw: Callable[[str], dict[str, object]],
    resolve_musicbrainz_redirect: TaggedCanonicalReleaseFn | None = None,
) -> LibraryCompletenessSnapshot:
    snapshot = run_library_completeness_census(
        beets, fetch_musicbrainz_raw=fetch_musicbrainz_raw,
        fetch_discogs_raw=fetch_discogs_raw,
        resolve_musicbrainz_redirect=resolve_musicbrainz_redirect,
    )
    # Do not replace yesterday's real computation with an all-zero unavailable
    # report. Per-album unknowns are deliberately publishable.
    if snapshot.report.status != "beets_unavailable":
        write_library_completeness_snapshot(path, snapshot)
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Cratedigger daily library completeness census")
    parser.add_argument("--config", default=None)
    parser.add_argument("--runtime-dir", default=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config_path, runtime_dir = resolve_startup_config_paths(config_path=args.config, runtime_dir=args.runtime_dir)
    try:
        cfg = enforce_beets_startup(role="web", config_path=config_path, runtime_dir=runtime_dir, logger=logger)
    except BeetsStartupError:
        return EXIT_CONFIG_ABORT
    # The admitted runtime config is the production source for both mirrors;
    # no hard-coded local endpoint enters the census.
    configure_api_bases_from_runtime_config()
    configure_canonical_release_lookup(cfg)
    path = library_completeness_snapshot_path(cfg.var_dir)
    try:
        with open_beets_db(config=cfg) as beets:
            snapshot = publish_library_completeness_census(
                path, beets, fetch_musicbrainz_raw=mb.get_release_raw,
                fetch_discogs_raw=lambda release_id: discogs.get_release_raw(int(release_id)),
                resolve_musicbrainz_redirect=production_tagged_canonical_release_fn(),
            )
    except Exception:
        logger.exception("library completeness census failed; prior snapshot at %s is preserved", path)
        return EXIT_RUN_FAILED
    if snapshot.report.status == "beets_unavailable":
        logger.error("Beets unavailable; prior snapshot at %s is preserved", path)
        return EXIT_BEETS_UNAVAILABLE
    logger.info("published library completeness status=%s albums=%d duration=%.1fs -> %s", snapshot.report.status, snapshot.report.counts.albums_scanned, snapshot.duration_seconds, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
