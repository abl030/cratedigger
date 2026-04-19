#!/usr/bin/env python
from __future__ import annotations

import argparse
import configparser
import logging
import os
import sys
import time
from typing import Any, Sequence, TYPE_CHECKING, TypedDict

import slskd_api

if TYPE_CHECKING:
    from album_source import DatabaseSource
    from lib.config import SoularrConfig
    from lib.context import SoularrContext


class TrackRecord(TypedDict):
    """Track dict from pipeline DB — shape used by matching functions."""
    albumId: int
    title: str
    mediumNumber: int


class _SlskdFileRequired(TypedDict):
    filename: str

class SlskdFile(_SlskdFileRequired, total=False):
    """File dict from slskd directory browse. Only filename is required."""
    size: int
    bitRate: int
    sampleRate: int
    bitDepth: int
    isVariableBitRate: bool


class SlskdDirectory(TypedDict):
    """Directory dict from slskd users.directory() API."""
    directory: str
    files: list[SlskdFile]


# === Typed Config (populated in main() via SoularrConfig.from_ini()) ===
cfg: SoularrConfig = None  # type: ignore[assignment]  # Set in main()

# === API Clients & Logging ===
slskd: slskd_api.SlskdClient = None  # type: ignore[assignment]  # Set in main()
logger = logging.getLogger("soularr")

# === API client instances (set in main()) ===
pipeline_db_source: "DatabaseSource" = None  # type: ignore[assignment]  # Set in main()

# === Runtime context (populated in main()) ===
# Module-level reference for thin wrappers that can't receive ctx as a parameter.
# All matching/search functions receive ctx explicitly.
_module_ctx: Any = None  # SoularrContext — set in main()

from lib.browse import (
    _browse_directories,
    _browse_one,
    download_filter,
    rank_candidate_dirs,
)
from lib.enqueue import (
    _get_denied_users,
    _get_user_dirs,
    _prefixed_directory_files,
    _try_filetype,
    choose_release,
    find_download,
    get_album_tracks,
    release_trackcount_mode,
    try_enqueue,
    try_multi_enqueue,
)
from lib.matching import (
    album_match,
    album_track_num,
    check_for_match,
    check_ratio,
    get_album_by_id,
)


def filter_list(albums: Sequence[Any], filter_cfg: SoularrConfig) -> list[Any] | None:
    """Filter albums against the title blacklist. Returns None if nothing passes."""
    result = []
    for album in albums:
        title_lower = album.title.lower()
        blocked = next(
            (w for w in filter_cfg.title_blacklist if w and w.lower() in title_lower),
            None,
        )
        if blocked:
            logger.info(f"Skipping blacklisted album: {album.artist_name} - {album.title} (word: {blocked})")
        else:
            result.append(album)
    return result or None


def _build_search_cache(
    search_results: list[Any],
    filter_specs: list[tuple[str, Any]],
) -> tuple[dict[str, dict[str, list[str]]], dict[str, int], dict[str, dict[str, int]]]:
    """Build cache dicts from raw slskd search results.

    Returns (cache_entries, upload_speeds, dir_audio_counts).
    Pure — no I/O, no ctx writes.
    """
    from lib.quality import file_identity, filetype_matches

    cache_entries: dict[str, dict[str, list[str]]] = {}
    upload_speeds: dict[str, int] = {}
    dir_audio_counts: dict[str, dict[str, int]] = {}

    for result in search_results:
        username = result["username"]
        if username not in cache_entries:
            cache_entries[username] = {}
        if username not in dir_audio_counts:
            dir_audio_counts[username] = {}
        user_dir_counts = dir_audio_counts[username]
        speed = result.get("uploadSpeed", 0)
        if speed and (username not in upload_speeds or speed > upload_speeds[username]):
            upload_speeds[username] = speed
        for file in result["files"]:
            file_dir = file["filename"].rsplit("\\", 1)[0]
            identity = file_identity(file)
            matched = False
            for allowed_filetype, spec in filter_specs:
                if filetype_matches(identity, spec):
                    matched = True
                    if allowed_filetype not in cache_entries[username]:
                        cache_entries[username][allowed_filetype] = []
                    if file_dir not in cache_entries[username][allowed_filetype]:
                        cache_entries[username][allowed_filetype].append(file_dir)
            if matched:
                user_dir_counts[file_dir] = user_dir_counts.get(file_dir, 0) + 1

    return cache_entries, upload_speeds, dir_audio_counts


def search_for_album(album, ctx):
    """Search slskd for an album. Returns SearchResult (always non-None)."""
    from lib.search import build_query, SearchResult

    album_title = album.title
    artist_name = album.artist_name
    album_id = album.id
    t0 = time.time()
    query = build_query(artist_name, album_title, prepend_artist=cfg.album_prepend_artist)

    if not query:
        logger.warning(f"Cannot build search query for '{artist_name} - {album_title}'")
        return SearchResult(album_id=album_id, success=False, outcome="empty_query")

    logger.info(f"Searching for album: {query} "
                f"(from '{artist_name} - {album_title}')")
    try:
        search = slskd.searches.search_text(
            searchText=query,
            searchTimeout=cfg.search_timeout,
            filterResponses=True,
            maximumPeerQueueLength=cfg.maximum_peer_queue,
            minimumPeerUploadSpeed=cfg.minimum_peer_upload_speed,
        )
    except Exception:
        logger.exception(f"Failed to perform search via SLSKD: {query}")
        return SearchResult(album_id=album_id, success=False, query=query,
                            elapsed_s=time.time() - t0, outcome="error")

    # Wait for slskd to process the search. Searches go through:
    #   Queued -> InProgress -> Completed, (TimedOut|ResponseLimitReached|Errored)
    # We must wait while state is Queued OR InProgress.
    # slskd's searchTimeout is "time since last response", not absolute.
    # Our poll timeout must be longer — let slskd complete on its own.
    slskd_timeout_s = cfg.search_timeout / 1000 if cfg.search_timeout > 1000 else cfg.search_timeout
    poll_timeout_s = slskd_timeout_s * 2 + 15
    start_time = time.time()
    while True:
        state = slskd.searches.state(search["id"], False)["state"]
        if "Completed" in state or ("InProgress" not in state and "Queued" not in state):
            break
        time.sleep(1)
        if (time.time() - start_time) > poll_timeout_s:
            logger.error("Failed to perform search via SLSKD due to timeout on search results.")
            return SearchResult(album_id=album_id, success=False, query=query,
                                elapsed_s=time.time() - t0, outcome="timeout")

    search_results = slskd.searches.search_responses(search["id"])
    elapsed = time.time() - t0
    logger.info(f"Search returned {len(search_results)} results")
    if cfg.delete_searches:
        slskd.searches.delete(search["id"])

    if not len(search_results) > 0:
        return SearchResult(album_id=album_id, success=False, query=query,
                            result_count=0, elapsed_s=elapsed, outcome="no_results")

    filter_specs = list(zip(cfg.allowed_filetypes, cfg.allowed_specs))
    cache_entries, upload_speeds, dir_audio_counts = _build_search_cache(
        search_results, filter_specs
    )
    for username in cache_entries:
        logger.info(f"Caching and truncating results for user: {username}")

    result = SearchResult(
        album_id=album_id, success=True,
        cache_entries=cache_entries,
        upload_speeds=upload_speeds,
        dir_audio_counts=dir_audio_counts,
        query=query,
        result_count=len(search_results),
        elapsed_s=elapsed,
    )
    # Reuse the same merge path as the parallel pipeline
    _merge_search_result(result, ctx)
    return result


def _submit_search(album, search_cfg, slskd_client):
    """Submit a search to slskd and return the search ID (no waiting).

    slskd has a SemaphoreSlim(1,1) on POST /searches — only one submission
    at a time. The semaphore releases after the search is queued (~100ms),
    so we submit sequentially but wait for results in parallel.

    Returns (search_id, query, album_id) or None on failure.
    """
    from lib.search import build_query
    import requests

    album_title = album.title
    artist_name = album.artist_name
    album_id = album.id
    query = build_query(artist_name, album_title, prepend_artist=search_cfg.album_prepend_artist)

    if not query:
        logger.warning(f"Cannot build search query for '{artist_name} - {album_title}'")
        return None

    logger.info(f"Submitting search: {query} "
                f"(from '{artist_name} - {album_title}')")

    # Retry on 429 (rate limit) or 409 (semaphore busy) with backoff.
    # slskd has SemaphoreSlim(1,1) — 409 means another search is still being submitted.
    for attempt in range(6):
        try:
            search = slskd_client.searches.search_text(
                searchText=query,
                searchTimeout=search_cfg.search_timeout,
                filterResponses=True,
                maximumPeerQueueLength=search_cfg.maximum_peer_queue,
                minimumPeerUploadSpeed=search_cfg.minimum_peer_upload_speed,
            )
            return (search["id"], query, album_id)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status in (429, 409) and attempt < 5:
                wait = min(2 ** attempt, 8)  # 1, 2, 4, 8, 8s
                logger.warning(f"{status} on search submit for {query}, "
                               f"retrying in {wait}s (attempt {attempt + 1}/6)")
                time.sleep(wait)
            else:
                logger.exception(f"Failed to submit search via SLSKD: {query}")
                return None
        except Exception:
            logger.exception(f"Failed to submit search via SLSKD: {query}")
            return None
    return None


def _collect_search_results(search_id, query, album_id, search_cfg, slskd_client):
    """Wait for a submitted search to complete and collect results.

    This is the part that can run in parallel — it's just polling + reading.
    """
    from lib.search import SearchResult

    t0 = time.time()

    # Wait for search to complete. slskd search states:
    #   Queued -> InProgress -> Completed, (TimedOut|ResponseLimitReached|Errored)
    # We must wait while state is Queued OR InProgress.
    # NOTE: slskd's searchTimeout is "time since last response", not absolute.
    # A 30s timeout means slskd waits 30s after the last peer responds. Our
    # poll timeout must be longer — slskd will complete the search on its own.
    slskd_timeout_s = search_cfg.search_timeout / 1000 if search_cfg.search_timeout > 1000 else search_cfg.search_timeout
    timeout_s = slskd_timeout_s + slskd_timeout_s + 15  # worst case: responses arrive at T=timeout, then wait another timeout
    start_time = time.time()
    while True:
        try:
            state_resp = slskd_client.searches.state(search_id, False)
            state = state_resp["state"]
            if "Completed" in state or ("InProgress" not in state and "Queued" not in state):
                break
        except Exception:
            logger.warning(f"Failed to poll search state for {query}")
            break
        time.sleep(1)
        if (time.time() - start_time) > timeout_s:
            logger.error(f"Search timed out for {query}")
            return SearchResult(album_id=album_id, success=False, query=query,
                                elapsed_s=time.time() - t0, outcome="timeout")

    search_results = slskd_client.searches.search_responses(search_id)
    elapsed = time.time() - t0
    logger.info(f"Search returned {len(search_results)} results in {elapsed:.1f}s for: {query}")
    if search_cfg.delete_searches:
        slskd_client.searches.delete(search_id)

    if not len(search_results) > 0:
        return SearchResult(album_id=album_id, success=False, query=query,
                            result_count=0, elapsed_s=elapsed, outcome="no_results")

    filter_specs = list(zip(search_cfg.allowed_filetypes, search_cfg.allowed_specs))
    cache_entries, upload_speeds, dir_audio_counts = _build_search_cache(
        search_results, filter_specs
    )

    return SearchResult(
        album_id=album_id,
        success=True,
        cache_entries=cache_entries,
        upload_speeds=upload_speeds,
        dir_audio_counts=dir_audio_counts,
        query=query,
        result_count=len(search_results),
        elapsed_s=elapsed,
    )


def _merge_search_result(result, ctx):
    """Merge a SearchResult into ctx caches.

    Called only from the main thread — no locking needed.
    """
    album_id = result.album_id
    if album_id not in ctx.search_cache:
        ctx.search_cache[album_id] = {}

    for username, filetypes in result.cache_entries.items():
        if username not in ctx.search_cache[album_id]:
            ctx.search_cache[album_id][username] = {}
        for filetype, dirs in filetypes.items():
            if filetype not in ctx.search_cache[album_id][username]:
                ctx.search_cache[album_id][username][filetype] = []
            for d in dirs:
                if d not in ctx.search_cache[album_id][username][filetype]:
                    ctx.search_cache[album_id][username][filetype].append(d)

    for username, speed in result.upload_speeds.items():
        if username not in ctx.user_upload_speed or speed > ctx.user_upload_speed[username]:
            ctx.user_upload_speed[username] = speed
            ctx._upload_speed_ts[username] = time.time()

    for username, dir_counts in result.dir_audio_counts.items():
        if username not in ctx.search_dir_audio_count:
            ctx.search_dir_audio_count[username] = {}
        for d, count in dir_counts.items():
            existing = ctx.search_dir_audio_count[username].get(d, 0)
            ctx.search_dir_audio_count[username][d] = max(existing, count)
            ctx._dir_audio_count_ts.setdefault(username, {})[d] = time.time()


def _log_search_result(album, result, ctx) -> None:
    """Persist search outcome to search_log and record_attempt on failure."""
    request_id = getattr(album, "db_request_id", None)
    if not request_id:
        return
    db = ctx.pipeline_db_source._get_db()
    db.log_search(
        request_id=request_id,
        query=result.query or None,
        result_count=result.result_count,
        elapsed_s=result.elapsed_s or None,
        outcome=result.outcome or "error",
    )
    # Increment search_attempts + backoff for any non-found outcome
    if result.outcome != "found":
        db.record_attempt(request_id, "search")


def _apply_find_download_result(album, result, find_result, failed_grab) -> None:
    """Translate matching/enqueue outcome into search_log telemetry."""
    if find_result.outcome == "found":
        result.outcome = "found"
        return
    result.outcome = "error" if find_result.outcome == "enqueue_failed" else "no_match"
    failed_grab.append(album)


def search_and_queue(albums, ctx):
    if cfg.parallel_searches > 1 and len(albums) > 1:
        return _search_and_queue_parallel(albums, ctx)
    grab_list = {}
    failed_grab = []
    failed_search = []
    total = len(albums)
    for i, album in enumerate(albums, 1):
        logger.info(f"Album {i}/{total}: {album.artist_name} - {album.title}")
        result = search_for_album(album, ctx)
        if result.success:
            find_result = find_download(album, grab_list, ctx)
            _apply_find_download_result(album, result, find_result, failed_grab)
        else:
            failed_search.append(album)
        _log_search_result(album, result, ctx)
    return grab_list, failed_search, failed_grab


def _search_and_queue_parallel(albums, ctx):
    """Pipeline searches with result processing: always 2 searches in flight.

    slskd constraints (from source code):
    - SemaphoreSlim(1,1) on POST /searches: one submission at a time
    - maximumConcurrentSearches=2 in Soulseek.NET: only 2 active on network

    We keep 2 searches running on the network at all times. When one completes,
    we process its results (browse dirs, match tracks, enqueue) AND submit the
    next search — so network wait and result processing overlap.

    Timeline:
      search_1 ─────────> process_1    search_5 ──────> process_5 ...
      search_2 ─────────> process_2    search_6 ──────> ...
        search_3 ─────────> process_3
        search_4 ─────────> process_4
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    MAX_INFLIGHT = 2  # matches slskd maximumConcurrentSearches

    grab_list: dict[Any, Any] = {}
    failed_grab: list[Any] = []
    failed_search: list[Any] = []
    total = len(albums)
    album_queue = list(albums)  # mutable copy we pop from

    logger.info(f"Pipelined search: {total} albums, {MAX_INFLIGHT} in flight")
    wall_start = time.time()

    def _submit_next() -> tuple[Any, Any] | None:
        """Submit the next album from the queue. Returns (future, album) or None."""
        while album_queue:
            album = album_queue.pop(0)
            submit_result = _submit_search(album, cfg, slskd)
            if submit_result is None:
                # Log the submission failure — reconstruct query for the log
                from lib.search import build_query, SearchResult
                query = build_query(album.artist_name, album.title,
                                    prepend_artist=cfg.album_prepend_artist)
                sr = SearchResult(
                    album_id=album.id, success=False,
                    query=query or "",
                    outcome="empty_query" if not query else "error",
                )
                _log_search_result(album, sr, ctx)
                failed_search.append(album)
                continue
            search_id, query, album_id = submit_result
            future = pool.submit(
                _collect_search_results, search_id, query, album_id, cfg, slskd
            )
            return (future, album)
        return None

    with ThreadPoolExecutor(max_workers=MAX_INFLIGHT) as pool:
        # Seed the pipeline with initial searches
        inflight: dict[Any, Any] = {}
        for _ in range(min(MAX_INFLIGHT, len(album_queue))):
            submitted = _submit_next()
            if submitted:
                future, album = submitted
                inflight[future] = album

        # Process completions and refill the pipeline
        while inflight:
            for future in as_completed(inflight):
                album = inflight.pop(future)
                try:
                    result = future.result()
                except Exception:
                    logger.exception(f"Search collection crashed for {album.title}")
                    from lib.search import SearchResult
                    sr = SearchResult(album_id=album.id, success=False, outcome="error")
                    _log_search_result(album, sr, ctx)
                    failed_search.append(album)
                else:
                    done_count = len(grab_list) + len(failed_grab) + len(failed_search)
                    logger.info(
                        f"Search {done_count + 1}/{total} done: {result.query} "
                        f"({result.result_count if result.result_count is not None else 'n/a'} results, "
                        f"{result.elapsed_s:.1f}s)"
                    )
                    if result.success:
                        _merge_search_result(result, ctx)
                        find_result = find_download(album, grab_list, ctx)
                        _apply_find_download_result(album, result, find_result, failed_grab)
                    else:
                        failed_search.append(album)
                    _log_search_result(album, result, ctx)

                # Refill: submit next search to keep pipeline full
                submitted = _submit_next()
                if submitted:
                    new_future, new_album = submitted
                    inflight[new_future] = new_album

                # Break out of the as_completed loop to re-enter with updated dict
                break

    wall_elapsed = time.time() - wall_start
    logger.info(f"Pipelined search complete: {total} albums in {wall_elapsed:.1f}s "
                f"(found={len(grab_list)}, no_match={len(failed_grab)}, "
                f"no_results={len(failed_search)})")

    return grab_list, failed_search, failed_grab


from lib.download import (cancel_and_delete as _cancel_and_delete_impl,
                          slskd_do_enqueue as _slskd_do_enqueue_impl,
                          grab_most_wanted as _grab_most_wanted_impl)


def _make_ctx():
    """Return the module-level SoularrContext (created in main())."""
    return _module_ctx


def cancel_and_delete(files):
    _cancel_and_delete_impl(files, _make_ctx())


def slskd_do_enqueue(username, files, file_dir):
    return _slskd_do_enqueue_impl(username, files, file_dir, _make_ctx())


def grab_most_wanted(albums):
    return _grab_most_wanted_impl(albums, lambda albs: search_and_queue(albs, _module_ctx), _module_ctx)


from lib.util import (_track_titles_cross_check,
                      setup_logging)


def main():
    global \
        cfg, \
        slskd, \
        pipeline_db_source, \
        _module_ctx

    # Belt-and-suspenders for systemd's UMask=0000 — see lib/permissions.py / GH #84.
    from lib.permissions import reset_umask
    reset_umask()

    parser = argparse.ArgumentParser(description="Soularr music download pipeline")
    parser.add_argument("-c", "--config-dir", default=os.getcwd(),
                        help="Config directory (default: cwd)")
    parser.add_argument("-v", "--var-dir", default=os.getcwd(),
                        help="Var directory for lock file and caches (default: cwd)")
    parser.add_argument("--no-lock-file", action="store_true",
                        help="Disable lock file creation")
    args = parser.parse_args()

    lock_file_path = os.path.join(args.var_dir, ".soularr.lock")
    config_file_path = os.path.join(args.config_dir, "config.ini")

    if not args.no_lock_file and os.path.exists(lock_file_path):
        logger.info("Soularr instance is already running.")
        sys.exit(1)

    try:
        if not args.no_lock_file:
            with open(lock_file_path, "w") as f:
                f.write("locked")

        config = configparser.RawConfigParser()

        if os.path.exists(config_file_path):
            config.read(config_file_path)
        else:
            logger.error(
                f"Config file not found at {config_file_path}. "
                "Pass --config-dir to specify its location. "
                "Under the upstream NixOS module, /var/lib/soularr/config.ini "
                "is rendered by preStartScript at boot."
            )
            sys.exit(1)

        # --- Parse config into typed dataclass ---
        from lib.config import SoularrConfig
        cfg = SoularrConfig.from_ini(config, config_dir=args.config_dir, var_dir=args.var_dir)

        setup_logging(config)

        if cfg.beets_validation_enabled:
            logger.info(f"Beets validation ENABLED: harness={cfg.beets_harness_path}, "
                        f"threshold={cfg.beets_distance_threshold}, staging={cfg.beets_staging_dir}")

        # --- Soft warning for sub-gate verified_lossless_target (issue #60) ---
        # When the configured verified_lossless_target has a declared rank
        # below gate_min_rank, the resulting imports will fail the quality
        # gate and be re-queued for upgrade — meaning they'll never stabilize
        # as "imported". Log loudly at startup so operators see this before
        # it surprises them downstream.
        if cfg.verified_lossless_target:
            try:
                from lib.quality import quality_rank, QualityRank
                target_rank = quality_rank(
                    cfg.verified_lossless_target,
                    bitrate_kbps=None, is_cbr=False, cfg=cfg.quality_ranks)
                if (target_rank != QualityRank.UNKNOWN
                        and target_rank < cfg.quality_ranks.gate_min_rank):
                    logger.warning(
                        f"verified_lossless_target={cfg.verified_lossless_target!r} "
                        f"has rank {target_rank.name}, below configured "
                        f"gate_min_rank={cfg.quality_ranks.gate_min_rank.name}. "
                        f"Files converted to this target will fail the quality "
                        f"gate and be re-queued for upgrade. Either raise the "
                        f"target format or lower gate_min_rank in config.ini "
                        f"[Quality Ranks]."
                    )
            except Exception as exc:
                logger.debug(f"verified_lossless_target rank check failed: {exc}")

        from album_source import DatabaseSource
        pipeline_db_source = DatabaseSource(cfg.pipeline_db_dsn)
        logger.info(f"Pipeline DB: {cfg.pipeline_db_dsn}")

        if cfg.meelo_url:
            logger.info(f"Meelo post-import scan ENABLED: {cfg.meelo_url}")

        slskd = slskd_api.SlskdClient(host=cfg.slskd_host_url, api_key=cfg.slskd_api_key, url_base=cfg.slskd_url_base)

        # Build context with fresh caches for this cycle
        from lib.context import SoularrContext
        _module_ctx = SoularrContext(cfg=cfg, slskd=slskd, pipeline_db_source=pipeline_db_source)

        # Load persisted caches from previous runs
        from lib.cache import load_caches
        load_caches(_module_ctx, cfg.var_dir)

        # Populate global user cooldowns (issue #39)
        try:
            db = pipeline_db_source._get_db()
            cooled = db.get_cooled_down_users()
            _module_ctx.cooled_down_users = set(cooled)
            if cooled:
                logger.info(f"User cooldowns active: {', '.join(sorted(cooled))}")
        except Exception as e:
            logger.warning(f"Failed to load user cooldowns: {e}")

        cycle_start = time.time()

        # --- Phase 1 + Phase 2 run concurrently ---
        # Phase 1 (poll downloads) operates on status='downloading' rows.
        # Phase 2 (search + enqueue) operates on status='wanted' rows.
        # Disjoint status buckets — the set_downloading() guard prevents
        # Phase 2 from overwriting Phase 1's transitions.
        # Phase 1 gets its own DatabaseSource (psycopg2 is not thread-safe).
        from concurrent.futures import ThreadPoolExecutor
        from lib.download import poll_active_downloads as _poll_impl

        def _run_phase1():
            """Run Phase 1 in a background thread with its own DB connection."""
            phase1_source = DatabaseSource(cfg.pipeline_db_dsn)
            phase1_ctx = SoularrContext(
                cfg=cfg,
                slskd=slskd,
                pipeline_db_source=phase1_source,
                cooled_down_users=_module_ctx.cooled_down_users,
            )
            try:
                _poll_impl(phase1_ctx)
            finally:
                phase1_source.close()

        logger.info("Starting Phase 1 (poll downloads) in background...")
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="phase1") as pool:
            phase1_future = pool.submit(_run_phase1)

            # --- Phase 2: Search and enqueue new downloads (main thread) ---
            logger.info("Getting wanted records from pipeline DB...")
            wanted_records = pipeline_db_source.get_wanted(limit=cfg.page_size)
            logger.info(f"Pipeline DB: {len(wanted_records)} wanted record(s)")

            failed = 0
            if len(wanted_records) > 0:
                try:
                    filtered = filter_list(wanted_records, cfg)
                    if filtered is not None:
                        failed = grab_most_wanted(filtered)
                    else:
                        logger.info("No releases wanted that aren't on the deny list and/or blacklisted")
                except Exception:
                    logger.exception("Fatal error in search phase!")
                if failed == 0:
                    logger.info("Soularr finished. Exiting...")
                else:
                    logger.info(f"{failed}: releases failed to find a match in the search results and are still wanted.")
            else:
                logger.info("No releases wanted. Exiting...")

            # Wait for Phase 1 to finish before cleanup
            try:
                phase1_future.result()
                logger.info("Phase 1 (poll downloads) completed.")
            except Exception:
                logger.exception("Phase 1 (poll downloads) failed — continuing to cleanup")

        # Clean up completed transfer UI entries
        slskd.transfers.remove_completed_downloads()

        elapsed = time.time() - cycle_start
        logger.info(f"Soularr cycle complete in {elapsed:.1f}s")

    finally:
        # Save caches for next run
        try:
            from lib.cache import save_caches as _save
            _save(_module_ctx, cfg.var_dir)
        except Exception:
            pass

        # Bust web UI cache so freshly imported albums appear immediately
        try:
            import urllib.request
            req = urllib.request.Request(
                "http://localhost:8085/api/cache/invalidate",
                data=b'{"groups": ["pipeline", "library"]}',
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass  # web UI may be down — that's fine

        # Clean up pipeline DB connection
        if pipeline_db_source is not None:
            try:
                pipeline_db_source.close()
            except Exception:
                pass
        # Remove the lock file after activity is done
        if not args.no_lock_file and os.path.exists(lock_file_path):
            os.remove(lock_file_path)


if __name__ == "__main__":
    main()
