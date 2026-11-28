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

from lib import discogs_api, mb_api
from lib.api_bases import configure_api_bases_from_runtime_config
from lib.beets_db import open_beets_db
from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from lib.config import resolve_startup_config_paths
from lib.library_completeness import CompletenessBeets, scan_library_completeness
from lib.library_completeness_snapshot import (
    LibraryCompletenessSnapshot,
    library_completeness_snapshot_path,
    write_library_completeness_snapshot,
)
from lib.library_track_lengths import (
    DEFAULT_LENGTH_BUDGET_SECONDS,
    DEFAULT_LENGTH_READS_PER_SECOND,
    TrackLengthCheck,
    TrackLengthMeasurer,
    read_track_length_cache,
    track_length_cache_path,
    write_track_length_cache,
)
from lib.mb_canonical import (
    TaggedCanonicalReleaseFn,
    configure_canonical_release_lookup,
    production_tagged_canonical_release_fn,
)

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
    track_lengths: TrackLengthCheck | None = None,
    time_fn: Callable[[], float] = time.monotonic,
    now_fn: Callable[[], str] = _now_iso,
) -> LibraryCompletenessSnapshot:
    generated_at = now_fn()
    started = time_fn()
    report = scan_library_completeness(
        beets, fetch_musicbrainz_raw=fetch_musicbrainz_raw,
        fetch_discogs_raw=fetch_discogs_raw,
        resolve_musicbrainz_redirect=resolve_musicbrainz_redirect,
        track_lengths=track_lengths,
    )
    return LibraryCompletenessSnapshot(generated_at, time_fn() - started, report)


def publish_library_completeness_census(
    path: str, beets: CompletenessBeets, *,
    fetch_musicbrainz_raw: Callable[[str], dict[str, object]],
    fetch_discogs_raw: Callable[[str], dict[str, object]],
    resolve_musicbrainz_redirect: TaggedCanonicalReleaseFn | None = None,
    length_cache_path: str | None = None,
    length_budget_seconds: float = DEFAULT_LENGTH_BUDGET_SECONDS,
    length_reads_per_second: float = DEFAULT_LENGTH_READS_PER_SECOND,
) -> LibraryCompletenessSnapshot:
    """Run, then publish the snapshot and (issue #1472) the length cache.

    ``length_cache_path=None`` skips the track-length step entirely.
    """
    check = None
    if length_cache_path is not None:
        check = TrackLengthCheck(TrackLengthMeasurer(
            read_track_length_cache(length_cache_path),
            budget_seconds=length_budget_seconds,
            reads_per_second=length_reads_per_second,
        ))
    snapshot = run_library_completeness_census(
        beets, fetch_musicbrainz_raw=fetch_musicbrainz_raw,
        fetch_discogs_raw=fetch_discogs_raw,
        resolve_musicbrainz_redirect=resolve_musicbrainz_redirect,
        track_lengths=check,
    )
    # Do not replace yesterday's real computation with an all-zero unavailable
    # report. Per-album unknowns are deliberately publishable. The same holds
    # for the length cache: an unavailable Beets saw no albums, and writing
    # its empty view would throw every measurement away.
    if snapshot.report.status != "beets_unavailable":
        write_library_completeness_snapshot(path, snapshot)
        if check is not None and length_cache_path is not None:
            write_track_length_cache(length_cache_path, check.measurer.updated_cache())
            logger.info("track lengths: read %d files this run", check.measurer.files_read)
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Cratedigger daily library completeness census")
    parser.add_argument("--config", default=None)
    parser.add_argument("--runtime-dir", default=None)
    parser.add_argument(
        "--length-budget-seconds", type=float, default=DEFAULT_LENGTH_BUDGET_SECONDS,
        help="Time this run may spend reading track lengths from files "
             "(0 uses only cached measurements)")
    parser.add_argument(
        "--length-reads-per-second", type=float, default=DEFAULT_LENGTH_READS_PER_SECOND,
        help="Upper bound on track-length file reads per second")
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
                path, beets, fetch_musicbrainz_raw=mb_api.get_release_raw,
                fetch_discogs_raw=lambda release_id: discogs_api.get_release_raw(int(release_id)),
                resolve_musicbrainz_redirect=production_tagged_canonical_release_fn(),
                length_cache_path=track_length_cache_path(cfg.var_dir),
                length_budget_seconds=args.length_budget_seconds,
                length_reads_per_second=args.length_reads_per_second,
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
