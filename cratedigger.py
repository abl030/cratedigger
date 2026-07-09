#!/usr/bin/env python
from __future__ import annotations

import argparse
import configparser
import logging
import os
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Sequence, TYPE_CHECKING, TypedDict

from lib.slskd_client import (
    SLSKD_HTTP_TIMEOUT_S,
    SlskdClient,
    derive_slskd_http_pool_size,
)
# Unified slskd search lifecycle (issue #466).
from lib.search_exec import (
    SearchSubmitError,
    execute_search,
)

if TYPE_CHECKING:
    from album_source import DatabaseSource
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext


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


# === Typed Config (populated in main() via CratediggerConfig.from_ini()) ===
cfg: CratediggerConfig = None  # type: ignore[assignment]  # Set in main()

# === API Clients & Logging ===
slskd: SlskdClient = None  # type: ignore[assignment]  # Set in main()
logger = logging.getLogger("cratedigger")

# Per-search progress watchdog (issue #212) + response-settle (issue #242)
# constants and helpers now live in ``lib/search_exec.py`` alongside the
# unified ``execute_search`` lifecycle they govern (issue #466). Imported at
# the top of this module.

# === API client instances (set in main()) ===
pipeline_db_source: "DatabaseSource" = None  # type: ignore[assignment]  # Set in main()

# === Runtime context (populated in main()) ===
# Module-level reference for thin wrappers that can't receive ctx as a parameter.
# All matching/search functions receive ctx explicitly.
_module_ctx: Any = None  # CratediggerContext — set in main()


def _create_slskd_client(client_cfg: CratediggerConfig) -> SlskdClient:
    """Create the slskd client with a pool sized for the pipeline width."""
    return SlskdClient(
        host=client_cfg.slskd_host_url,
        api_key=client_cfg.resolved_slskd_api_key(),
        url_base=client_cfg.slskd_url_base,
        timeout=SLSKD_HTTP_TIMEOUT_S,
        pool_size=derive_slskd_http_pool_size(client_cfg),
    )

from lib.browse import (
    _browse_directories,
    _browse_one,
    download_filter,
    rank_candidate_dirs,
    shutdown_browse_coordinator,
)
from lib.enqueue import (
    FindDownloadResult,
    FindDownloadOwnerPathError,
    _get_denied_users,
    _get_user_dirs,
    _prefixed_directory_files,
    _try_filetype,
    choose_release,
    find_download,
    get_album_tracks,
    prepare_find_download_context,
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
from lib.quality import top_candidates_with_skip_split


def filter_list(albums: Sequence[Any], filter_cfg: CratediggerConfig) -> list[Any] | None:
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
    from lib.quality import (
        file_identity,
        filetype_matches,
        search_cache_keys_for_identity,
    )

    def cache_dir(username: str, filetype_key: str, file_dir: str) -> None:
        if filetype_key not in cache_entries[username]:
            cache_entries[username][filetype_key] = []
        if file_dir not in cache_entries[username][filetype_key]:
            cache_entries[username][filetype_key].append(file_dir)

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
            configured_matches: list[str] = []
            for allowed_filetype, spec in filter_specs:
                if filetype_matches(identity, spec):
                    configured_matches.append(allowed_filetype)
            cache_keys = search_cache_keys_for_identity(
                identity, configured_matches
            )
            for cache_key in cache_keys:
                cache_dir(username, cache_key, file_dir)
            if cache_keys:
                user_dir_counts[file_dir] = user_dir_counts.get(file_dir, 0) + 1

    return cache_entries, upload_speeds, dir_audio_counts


def _select_active_plan_item_for_album(album, db):
    """Return ``(query, PlanExecutionContext)`` for the next plan-item to run.

    Plan-driven replacement for ``_select_variant_for_album``.
    Reads the request's active search plan, picks the item at
    ``next_plan_ordinal``, and snapshots the ``plan_cycle_count`` so the
    consumed-attempt write can detect stale completions after mid-flight
    regeneration.

    Returns ``None`` when:
      - the album has no ``db_request_id`` (legacy flow without a pipeline
        request — should not happen for Phase 2 input from
        ``get_wanted_searchable``);
      - the request has no active plan (also unexpected after U4 — but we
        skip rather than crash);
      - the active plan's generator id does not match
        ``SEARCH_PLAN_GENERATOR_ID`` (defensive re-check; the Phase 2
        filter already excludes these);
      - the active plan has no items at the next ordinal (pathological).

    The returned query is taken straight from the plan-item -- the
    generator already produced a runnable, normalized, repeat-aware
    string.
    """
    from lib.search import PlanExecutionContext, SEARCH_PLAN_GENERATOR_ID
    from lib.pipeline_db import PLAN_STATUS_ACTIVE

    request_id = getattr(album, "db_request_id", None)
    if not request_id:
        return None
    active = db.get_active_search_plan(request_id)
    if active is None:
        return None
    if active.plan.generator_id != SEARCH_PLAN_GENERATOR_ID:
        # Phase 2 already filters to current-generator rows; this is
        # defense in depth for anything that might reach here outside
        # ``get_wanted_searchable``.
        return None
    if active.plan.status != PLAN_STATUS_ACTIVE:
        return None
    if not active.items:
        return None

    next_ordinal = active.next_ordinal
    item = next(
        (it for it in active.items if it.ordinal == next_ordinal),
        None,
    )
    if item is None:
        # Cursor points at an ordinal that does not exist on this plan.
        # Treat as un-runnable; startup reconciliation will repair it.
        logger.warning(
            "PLAN_ITEM_LOOKUP_MISS request_id=%s plan_id=%s next_ordinal=%s; "
            "cursor does not point at a known plan item",
            request_id, active.plan.id, next_ordinal,
        )
        return None
    if not item.query:
        logger.warning(
            "PLAN_ITEM_EMPTY_QUERY request_id=%s plan_id=%s ordinal=%s",
            request_id, active.plan.id, item.ordinal,
        )
        return None

    return (
        item.query,
        PlanExecutionContext(
            plan_id=active.plan.id,
            plan_item_id=item.id,
            plan_ordinal=item.ordinal,
            plan_strategy=item.strategy,
            plan_canonical_query_key=item.canonical_query_key,
            plan_repeat_group=item.repeat_group,
            plan_generator_id=active.plan.generator_id,
            plan_item_count=len(active.items),
            cycle_count_snapshot=active.cycle_count,
        ),
    )


def _plan_search_submit_kwargs(query, search_cfg):
    """Build the ``searches.search_text`` kwargs for a plan-item search.

    Single source of truth for the pipeline's search params, shared by the
    serial (`search_for_album`) and parallel (`_submit_plan_search`) adapters.
    Query construction stays upstream (`lib.search`); this only carries the
    slskd tuning knobs.
    """
    return {
        "searchText": query,
        "searchTimeout": search_cfg.search_timeout,
        "filterResponses": True,
        "maximumPeerQueueLength": search_cfg.maximum_peer_queue,
        "minimumPeerUploadSpeed": search_cfg.minimum_peer_upload_speed,
        "responseLimit": search_cfg.search_response_limit,
        "fileLimit": search_cfg.search_file_limit,
    }


def _search_result_from_execution(
    exec_result, *, album_id, query, variant_tag, plan_execution, search_cfg,
):
    """Build a pipeline ``SearchResult`` from a completed ``execute_search``.

    Shared by the serial (`search_for_album`) and parallel
    (`_collect_search_results`) adapters so harvest classification and the
    audio-count cache build live in exactly one place. Does NOT merge into
    ctx caches — that stays with each adapter (main thread for serial, owner
    thread for parallel).
    """
    from lib.search import SearchResult

    responses = exec_result.responses
    if not len(responses) > 0:
        return SearchResult(
            album_id=album_id, success=False, query=query,
            result_count=0, elapsed_s=exec_result.elapsed_s, outcome="no_results",
            variant_tag=variant_tag, final_state=exec_result.final_state,
            watchdog_fired=exec_result.watchdog_fired,
            plan_execution=plan_execution,
            result_count_uncapped=exec_result.response_count_terminal,
        )

    filter_specs = list(zip(search_cfg.allowed_filetypes, search_cfg.allowed_specs))
    cache_entries, upload_speeds, dir_audio_counts = _build_search_cache(
        responses, filter_specs
    )
    return SearchResult(
        album_id=album_id, success=True,
        cache_entries=cache_entries,
        upload_speeds=upload_speeds,
        dir_audio_counts=dir_audio_counts,
        query=query,
        result_count=len(responses),
        elapsed_s=exec_result.elapsed_s,
        variant_tag=variant_tag,
        final_state=exec_result.final_state,
        watchdog_fired=exec_result.watchdog_fired,
        plan_execution=plan_execution,
        result_count_uncapped=exec_result.response_count_terminal,
    )


def search_for_album(album, ctx):
    """Search slskd for an album. Returns SearchResult (always non-None).

    Thin adapter over the unified lifecycle (``lib.search_exec.execute_search``,
    issue #466): plan-driven query selection (U5), then submit → poll (with the
    #212 progress watchdog) → settle-harvest (#242) → optional delete, then
    build the pipeline's ``SearchResult`` and merge caches on the main thread.
    Gains the watchdog + settle that the serial path previously lacked.
    """
    from lib.search import SearchResult

    album_title = album.title
    artist_name = album.artist_name
    album_id = album.id
    t0 = time.time()

    db = ctx.pipeline_db_source._get_db()
    selection = _select_active_plan_item_for_album(album, db)
    if selection is None:
        # No active current plan / no runnable item. After U4 reconciliation
        # this should be very rare; emit empty_query so the SearchResult
        # carrier is well-typed and the executor's bookkeeping stays
        # consistent. _log_search_result will record this as a
        # non-consuming pre-attempt failure (no plan context).
        logger.warning(
            f"No active plan for '{artist_name} - {album_title}'; "
            f"skipping slskd search and recording non-consuming telemetry")
        return SearchResult(
            album_id=album_id, success=False, query="",
            outcome="empty_query",
            variant_tag=None,
            plan_execution=None,
        )
    query, plan_execution = selection
    variant_tag = plan_execution.plan_strategy

    logger.info(f"Searching for album: {query} "
                f"(from '{artist_name} - {album_title}', "
                f"variant={variant_tag}, ordinal={plan_execution.plan_ordinal})")
    try:
        exec_result = execute_search(
            slskd,
            submit_kwargs=_plan_search_submit_kwargs(query, cfg),
            delete=cfg.delete_searches,
        )
    except SearchSubmitError:
        # Pre-accepted-search failure: non-consuming (final_state stays None).
        logger.exception(f"Failed to perform search via SLSKD: {query}")
        return SearchResult(
            album_id=album_id, success=False, query=query,
            elapsed_s=time.time() - t0, outcome="error",
            variant_tag=variant_tag,
            plan_execution=plan_execution,
        )
    except Exception:
        # slskd already accepted this search id. Treat collection failures
        # as consumed attempts so the cursor and telemetry stay in lockstep.
        logger.exception(
            f"Failed to collect search results via SLSKD: {query}")
        return SearchResult(
            album_id=album_id, success=False, query=query,
            elapsed_s=time.time() - t0, outcome="error",
            variant_tag=variant_tag, final_state="collection_crash",
            plan_execution=plan_execution,
        )

    logger.info(f"Search returned {len(exec_result.responses)} results")
    result = _search_result_from_execution(
        exec_result, album_id=album_id, query=query,
        variant_tag=variant_tag, plan_execution=plan_execution, search_cfg=cfg,
    )
    if result.success:
        # Reuse the same merge path as the parallel pipeline
        _merge_search_result(result, ctx)
    return result


def _submit_plan_search(album, query, strategy_tag, search_cfg, slskd_client):
    """Submit a plan-item search to slskd and return ``(search_id, query, album_id, tag)``.

    slskd has a SemaphoreSlim(1,1) on POST /searches — one submission at a
    time. The semaphore releases after the search is queued (~100ms), so we
    submit sequentially but wait for results in parallel.

    Returns ``None`` on submission failure (pre-attempt, non-consuming):
    the caller already has the plan-execution context to record a
    non-consuming telemetry row.
    """
    import requests

    album_title = album.title
    artist_name = album.artist_name
    album_id = album.id
    if not query:
        # Defensive: caller should have caught this, but never submit empty.
        logger.warning(f"Cannot build search query for '{artist_name} - {album_title}'")
        return None

    logger.info(f"Submitting search: {query} "
                f"(from '{artist_name} - {album_title}', variant={strategy_tag})")

    # Retry on 429 (rate limit) or 409 (semaphore busy) with backoff.
    # slskd has SemaphoreSlim(1,1) — 409 means another search is still being submitted.
    for attempt in range(6):
        try:
            search = slskd_client.searches.search_text(
                **_plan_search_submit_kwargs(query, search_cfg)
            )
            return (search["id"], query, album_id, strategy_tag)
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


def _collect_search_results(search_id, query, album_id, search_cfg, slskd_client,
                            variant_tag=None, clock_fn=time.monotonic):
    """Wait for an already-submitted search to complete and collect results.

    Thin adapter over the unified lifecycle (``lib.search_exec.execute_search``,
    issue #466). The parallel pipeline submits sequentially under slskd's
    ``SemaphoreSlim(1,1)`` via ``_submit_plan_search`` and hands the accepted
    ``search_id`` here; this is the part that runs in parallel — poll (with the
    #212 progress watchdog) → settle-harvest (#242) → optional delete → build
    the ``SearchResult``.

    ``variant_tag`` is the persisted tag chosen at submit and is plumbed onto
    the returned ``SearchResult`` so ``_log_search_result`` can persist it
    without re-running variant selection. A poll/harvest transport failure
    propagates to the caller, which classifies it as a consumed
    collection-crash (the search id was already accepted).

    ``clock_fn`` is injected for test determinism; production callers omit it
    (defaults to ``time.monotonic``).
    """
    exec_result = execute_search(
        slskd_client, search_id=search_id,
        delete=search_cfg.delete_searches, clock_fn=clock_fn,
    )
    logger.info(
        f"Search returned {len(exec_result.responses)} results in "
        f"{exec_result.elapsed_s:.1f}s for: {query}")
    return _search_result_from_execution(
        exec_result, album_id=album_id, query=query,
        variant_tag=variant_tag, plan_execution=None, search_cfg=search_cfg,
    )


def _merge_search_result(result, ctx):
    """Merge a SearchResult into ctx caches.

    Called only from the main thread — no locking needed.
    """
    from lib.peer_cache import drain_stats_into_context

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

    peer_cache = getattr(ctx, "peer_cache", None)
    if peer_cache is not None:
        for username, filetypes in result.cache_entries.items():
            cached_speed = peer_cache.get_upload_speed(username)
            drain_stats_into_context(ctx, peer_cache)
            if cached_speed is not None:
                existing_speed = ctx.user_upload_speed.get(username, 0)
                if cached_speed > existing_speed:
                    ctx.user_upload_speed[username] = cached_speed
            for dirs in filetypes.values():
                for d in dirs:
                    cached_count = peer_cache.get_dir_audio_count(username, d)
                    drain_stats_into_context(ctx, peer_cache)
                    if cached_count is None:
                        continue
                    counts = ctx.search_dir_audio_count.setdefault(username, {})
                    existing_count = counts.get(d, 0)
                    if cached_count > existing_count:
                        counts[d] = cached_count

    for username, speed in result.upload_speeds.items():
        if username not in ctx.user_upload_speed or speed > ctx.user_upload_speed[username]:
            ctx.user_upload_speed[username] = speed
            if peer_cache is not None:
                peer_cache.set_upload_speed(username, speed)
                drain_stats_into_context(ctx, peer_cache)

    for username, dir_counts in result.dir_audio_counts.items():
        if username not in ctx.search_dir_audio_count:
            ctx.search_dir_audio_count[username] = {}
        for d, count in dir_counts.items():
            existing = ctx.search_dir_audio_count[username].get(d, 0)
            merged = max(existing, count)
            ctx.search_dir_audio_count[username][d] = merged
            if peer_cache is not None and merged > existing:
                peer_cache.set_dir_audio_count(username, d, merged)
                drain_stats_into_context(ctx, peer_cache)


def _log_search_result(album, result, ctx) -> None:
    """Persist a search outcome via the plan-aware DB seams.

    Routes every SearchResult through one of two atomic DB methods:

      * ``record_consumed_search_attempt`` for accepted-search outcomes
        (found, no_results, no_match, error after acceptance, collection
        crash). Atomically inserts the search_log row AND advances/wraps
        the cursor, with a stale-completion guard against mid-flight
        regeneration.

      * ``record_non_consuming_search_attempt`` for pre-attempt failures
        (slskd submit failed, no plan/empty_query). Logs and applies
        scheduler backoff but never advances the cursor.

    No new ``outcome='exhausted'`` rows are emitted by this seam; plan
    wrap on the final ordinal increments ``plan_cycle_count`` instead
    (the consumed-attempt write owns that bookkeeping).

    U11 R22-R27: synthesises the forensics scalars on every write —
    query token / distinct-token counts (computed from ``result.query``),
    query template (mapped from ``plan_strategy``), expected track count
    (taken from ``album.releases[0].track_count`` so a single-disc
    request without a populated executor field still gets the column
    populated), and the matcher-derived ``rejection_reason`` +
    ``matcher_score_top1`` (computed from ``result.candidates`` via
    :func:`lib.matching.classify_rejection_from_log_inputs` /
    :func:`lib.matching.matcher_score_top1_for`). ``result.result_count_
    uncapped`` is captured upstream from the slskd state response.
    """
    import json as _json

    from lib.matching import (
        classify_rejection_from_log_inputs,
        matcher_score_top1_for,
    )
    from lib.pipeline_db import (
        ConsumedAttemptInput,
        NonConsumingAttemptInput,
    )
    from lib.search import query_template_for_strategy

    # Per-cycle watchdog instrumentation (issue #212). Every SearchResult
    # passes through here, so this is the single increment site for both
    # the parallel pipeline and the serial fallback.
    if getattr(result, "watchdog_fired", False):
        ctx.cycle_searches_watchdog_killed += 1
    request_id = getattr(album, "db_request_id", None)
    if not request_id:
        return
    db = ctx.pipeline_db_source._get_db()
    plan_execution = getattr(result, "plan_execution", None)
    outcome = result.outcome or "error"

    # Candidate JSONB: same contract as before — outcomes where slskd
    # actually ran but produced 0 hits ("no_results", "no_match") write
    # candidates=[] (empty list, not NULL) so downstream readers can
    # distinguish "search ran, found nothing" from "search never produced
    # a candidate concept" (error, timeout, empty_query — those write NULL).
    OUTCOMES_WITH_CANDIDATE_CONCEPT = ("no_results", "no_match", "found")
    if result.candidates:
        # U2 of search-plan-entropy: split into scored + up to 5
        # pre-filter-skip samples so the blob keeps room for both
        # without exceeding the historical top-20 cap.
        top: list | None = top_candidates_with_skip_split(result.candidates)
    elif outcome in OUTCOMES_WITH_CANDIDATE_CONCEPT:
        top = []
    else:
        top = None
    candidates_json = (
        _json.dumps([_candidate_to_jsonable(c) for c in top])
        if top is not None else None
    )

    # An attempt is "consumed" iff the executor reached an accepted slskd
    # search id (or terminal slskd state). Concretely: outcome is one of
    # found / no_results / no_match / error_after_accept (the parallel
    # path only sets outcome="error" on a *collection* crash; pre-submit
    # errors set outcome="error" but plan_execution may still be present).
    # The pre-attempt path is identified by the absence of post-accept
    # signals: query was never submitted (query=="") OR result_count is
    # None and no slskd telemetry present. We branch on the call site,
    # which sets ``result.success`` truthfully and uses outcomes coming
    # from `_collect_search_results` for accepted searches.
    is_consumed = _is_consumed_outcome(result, plan_execution)

    # U11 R24/R25/R26/R27: derive scalar forensics shared between the
    # consumed and non-consuming write paths. All defensive against
    # absent values — token counts only when ``result.query`` is set;
    # ``expected_track_count`` only when the album exposes a release
    # ``track_count``; ``query_template`` only when ``plan_execution``
    # supplied a strategy label. R22 (``rejection_reason``) and R26
    # (``matcher_score_top1``) compute off ``result.candidates`` via
    # the shared pure helpers — same source of truth as the matcher.
    query_text = result.query or ""
    query_tokens = query_text.split() if query_text else []
    query_token_count: int | None = len(query_tokens) if query_text else None
    query_distinct_token_count: int | None = (
        len({t.lower() for t in query_tokens}) if query_text else None
    )
    expected_track_count: int | None = None
    if getattr(album, "releases", None):
        first_release = album.releases[0]
        track_count_attr = getattr(first_release, "track_count", None)
        if isinstance(track_count_attr, int):
            expected_track_count = track_count_attr
    query_template: str | None = None
    if plan_execution is not None:
        query_template = query_template_for_strategy(
            plan_execution.plan_strategy,
        )
    candidates_seq = result.candidates or ()
    pre_filter_skip_count = getattr(result, "pre_filter_skip_count", 0) or 0
    rejection_reason = classify_rejection_from_log_inputs(
        list(candidates_seq), pre_filter_skip_count, outcome,
    )
    matcher_score_top1 = matcher_score_top1_for(list(candidates_seq))
    result_count_uncapped = getattr(result, "result_count_uncapped", None)

    if is_consumed and plan_execution is not None:
        scheduler_success = (outcome == "found")
        try:
            db.record_consumed_search_attempt(
                ConsumedAttemptInput(
                    request_id=request_id,
                    plan_id=plan_execution.plan_id,
                    plan_item_id=plan_execution.plan_item_id,
                    plan_ordinal=plan_execution.plan_ordinal,
                    plan_strategy=plan_execution.plan_strategy,
                    plan_canonical_query_key=(
                        plan_execution.plan_canonical_query_key),
                    plan_repeat_group=plan_execution.plan_repeat_group,
                    plan_generator_id=plan_execution.plan_generator_id,
                    plan_item_count=plan_execution.plan_item_count,
                    cycle_count_snapshot=(
                        plan_execution.cycle_count_snapshot),
                    query=result.query or "",
                    outcome=outcome,
                    result_count=result.result_count,
                    elapsed_s=result.elapsed_s or None,
                    candidates_json=candidates_json,
                    variant=result.variant_tag,
                    final_state=result.final_state,
                    browse_time_s=result.browse_time_s,
                    match_time_s=result.match_time_s,
                    peers_browsed=result.peers_browsed,
                    peers_browsed_lazy=result.peers_browsed_lazy,
                    fanout_waves=result.fanout_waves,
                    pre_filter_skip_count=result.pre_filter_skip_count,
                    apply_scheduler_attempt=True,
                    scheduler_success=scheduler_success,
                    rejection_reason=rejection_reason,
                    result_count_uncapped=result_count_uncapped,
                    query_token_count=query_token_count,
                    query_distinct_token_count=query_distinct_token_count,
                    expected_track_count=expected_track_count,
                    matcher_score_top1=matcher_score_top1,
                    query_template=query_template,
                )
            )
        except Exception:
            logger.exception(
                "record_consumed_search_attempt failed for request %s "
                "(plan_id=%s ordinal=%s outcome=%s)",
                request_id, plan_execution.plan_id,
                plan_execution.plan_ordinal, outcome,
            )
        return

    # Non-consuming pre-attempt path.
    plan_kwargs: dict[str, Any] = {}
    if plan_execution is not None:
        plan_kwargs = {
            "plan_id": plan_execution.plan_id,
            "plan_item_id": plan_execution.plan_item_id,
            "plan_ordinal": plan_execution.plan_ordinal,
            "plan_strategy": plan_execution.plan_strategy,
            "plan_canonical_query_key": plan_execution.plan_canonical_query_key,
            "plan_repeat_group": plan_execution.plan_repeat_group,
            "plan_generator_id": plan_execution.plan_generator_id,
        }
    try:
        db.record_non_consuming_search_attempt(
            NonConsumingAttemptInput(
                request_id=request_id,
                outcome=outcome,
                **plan_kwargs,
                query=result.query or None,
                result_count=result.result_count,
                elapsed_s=result.elapsed_s or None,
                final_state=result.final_state,
                apply_scheduler_attempt=True,
                pre_filter_skip_count=result.pre_filter_skip_count,
                rejection_reason=rejection_reason,
                result_count_uncapped=result_count_uncapped,
                query_token_count=query_token_count,
                query_distinct_token_count=query_distinct_token_count,
                expected_track_count=expected_track_count,
                matcher_score_top1=matcher_score_top1,
                query_template=query_template,
            )
        )
    except Exception:
        logger.exception(
            "record_non_consuming_search_attempt failed for request %s "
            "(outcome=%s)", request_id, outcome,
        )


def _candidate_to_jsonable(c: Any) -> dict[str, Any]:
    """Convert a CandidateScore (msgspec.Struct) to a plain dict for JSONB."""
    import msgspec
    return msgspec.to_builtins(c)


def _is_consumed_outcome(result: Any, plan_execution: Any) -> bool:
    """Decide whether this SearchResult represents an accepted-search slot.

    Consumption boundary: the slot is consumed once slskd accepted the
    search id (or reached a terminal state) — even if browse / match /
    enqueue later yields no_match, error, or enqueue failure.

    Pre-attempt failures (plan lookup miss, slskd submit error, empty
    query) are non-consuming. Distinguishing them from accepted-search
    failures is done via:
      * outcomes ``no_results``, ``no_match``, ``found`` always imply
        slskd accepted the search and produced a terminal state.
      * outcome ``error`` is ambiguous; the executor disambiguates by
        setting a non-None ``final_state`` only for paths that ran
        through ``_collect_search_results`` (real slskd terminal state)
        OR for the collection-crash path (synthetic
        ``final_state="collection_crash"``). Pre-submit errors leave
        ``final_state=None``.
      * outcome ``empty_query`` is always non-consuming.
      * outcome ``exhausted`` is legacy (no new emissions in U5+); if
        somehow encountered, treat as non-consuming so the cursor is
        not advanced.
    """
    outcome = (result.outcome or "").strip()
    if outcome in ("found", "no_results", "no_match"):
        return True
    if outcome in ("empty_query", "exhausted"):
        return False
    if outcome == "error":
        return result.final_state is not None
    return False


def _apply_find_download_result(
    album,
    result,
    find_result,
    failed_grab,
    grab_list=None,
    ctx=None,
) -> None:
    """Translate matching/enqueue outcome into search_log telemetry."""
    # Forensic capture: copy the per-(user, dir, filetype) score list off the
    # find_download result onto the SearchResult so `_log_search_result` can
    # persist the top-20 to `search_log.candidates`.
    result.candidates = tuple(find_result.candidates)
    # Aggregate pre-filter skip count from the find_download walk gets
    # persisted on ``search_log.pre_filter_skip_count``.
    result.pre_filter_skip_count = find_result.pre_filter_skip_count
    if ctx is not None and getattr(find_result, "metrics", None) is not None:
        metrics = find_result.metrics
        result.browse_time_s = metrics.browse_time_s
        result.match_time_s = metrics.match_time_s
        result.peers_browsed = metrics.peers_browsed
        result.peers_browsed_lazy = metrics.peers_browsed_lazy
        result.fanout_waves = metrics.fanout_waves
        ctx.browse_time_s += metrics.browse_time_s
        ctx.match_time_s += metrics.match_time_s
        ctx.peers_browsed += metrics.peers_browsed
        ctx.peers_browsed_lazy += metrics.peers_browsed_lazy
        ctx.fanout_waves += metrics.fanout_waves
        ctx.cache_pos_hits += metrics.cache_pos_hits
        ctx.cache_neg_hits += metrics.cache_neg_hits
        ctx.cache_misses += metrics.cache_misses
    elif getattr(find_result, "metrics", None) is not None:
        raise AssertionError("find_download metrics require owner context merge")
    if find_result.outcome == "found":
        result.outcome = "found"
        if grab_list is None:
            raise AssertionError("found find_download result requires grab_list merge")
        if find_result.grab_entry is None:
            raise AssertionError("found find_download result requires grab entry")
        grab_list[find_result.grab_entry.album_id] = find_result.grab_entry
        return
    result.outcome = "error" if find_result.outcome == "enqueue_failed" else "no_match"
    failed_grab.append(album)


def search_and_queue(albums, ctx):
    if ctx.cfg.parallel_searches > 1 and len(albums) > 1:
        return _search_and_queue_parallel(albums, ctx)
    grab_list = {}
    failed_grab = []
    failed_search = []
    total = len(albums)
    try:
        for i, album in enumerate(albums, 1):
            logger.info(f"Album {i}/{total}: {album.artist_name} - {album.title}")
            result = search_for_album(album, ctx)
            if result.success:
                find_ctx = prepare_find_download_context(album, ctx, result)
                find_result = find_download(album, find_ctx)
                _apply_find_download_result(
                    album, result, find_result, failed_grab, grab_list, ctx,
                )
            else:
                failed_search.append(album)
            _log_search_result(album, result, ctx)
        return grab_list, failed_search, failed_grab
    finally:
        shutdown_browse_coordinator(ctx)


def _search_and_queue_parallel(albums, ctx):
    """Pipeline searches and hand successful results to find_download workers.

    slskd constraints (from source code):
    - SemaphoreSlim(1,1) on POST /searches: one submission at a time
    - maximumConcurrentSearches=2 in Soulseek.NET: only 2 active on network

    Completed searches queue find_download work and immediately refill the
    search slot, so browse/match/enqueue no longer blocks search collection.
    """
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait

    # Pipeline depth — number of search-collection futures in flight at once.
    # Configurable via cfg.search_max_inflight (issue #198 U4). Submission
    # stays sequential through the existing 429-retry loop; only the
    # collect-side concurrency increases.
    search_cfg = ctx.cfg
    max_inflight = search_cfg.search_max_inflight

    grab_list: dict[Any, Any] = {}
    failed_grab: list[Any] = []
    failed_search: list[Any] = []
    total = len(albums)
    album_queue = list(albums)  # mutable copy we pop from

    logger.info(f"Pipelined search: {total} albums, {max_inflight} in flight")
    wall_start = time.time()

    # Map of inflight `pool.submit` future -> plan_execution context. The
    # owner thread re-attaches plan_execution to the SearchResult when the
    # collect future returns; this avoids threading the snapshot through
    # `_collect_search_results` (whose worker has no DB handle).
    inflight_plan_execution: dict[Any, Any] = {}

    def _submit_next() -> tuple[Any, Any] | None:
        """Submit the next album from the queue. Returns (future, album) or None.

        Plan-driven (U5): picks the next plan-item from the request's
        active persisted plan and snapshots cycle_count for the
        consumed-attempt write. ``inflight_plan_execution`` carries the
        snapshot from submit time through to log-emit time so the search
        log row reflects the plan-context chosen at submit.

        Issue #212 removed the cycle-entry deadline gate. Bounded runtime
        comes from two layers now: the per-search progress watchdog inside
        `_collect_search_results` (90s no-progress kill) and the systemd
        unit's `TimeoutStartSec=1h` defense-in-depth.
        """
        from lib.search import SearchResult

        while album_queue:
            album = album_queue.pop(0)
            db = ctx.pipeline_db_source._get_db()
            selection = _select_active_plan_item_for_album(album, db)

            if selection is None:
                # No active current plan — emit non-consuming pre-attempt
                # telemetry and skip slskd. After U4 reconciliation this
                # should be very rare for a row that came out of
                # ``get_wanted_searchable``.
                logger.warning(
                    f"No active plan for '{album.artist_name} - {album.title}'; "
                    f"skipping slskd search"
                )
                sr = SearchResult(
                    album_id=album.id, success=False,
                    query="",
                    outcome="empty_query",
                    variant_tag=None,
                    plan_execution=None,
                )
                _log_search_result(album, sr, ctx)
                failed_search.append(album)
                continue

            query, plan_execution = selection
            submit_result = _submit_plan_search(
                album, query, plan_execution.plan_strategy,
                search_cfg, ctx.slskd,
            )
            if submit_result is None:
                # slskd round-trip failed BEFORE search was accepted ->
                # non-consuming pre-attempt failure.
                sr = SearchResult(
                    album_id=album.id, success=False,
                    query=query,
                    outcome="error",
                    variant_tag=plan_execution.plan_strategy,
                    plan_execution=plan_execution,
                )
                _log_search_result(album, sr, ctx)
                failed_search.append(album)
                continue

            search_id, sub_query, album_id, variant_tag = submit_result
            future = pool.submit(
                _collect_search_results, search_id, sub_query, album_id,
                search_cfg, ctx.slskd, variant_tag,
            )
            inflight_plan_execution[future] = plan_execution
            return (future, album)
        return None

    def _attach_plan_execution(future, result) -> None:
        """Re-attach the submit-time plan_execution onto a returned result."""
        plan_exec = inflight_plan_execution.pop(future, None)
        if plan_exec is not None and getattr(result, "plan_execution", None) is None:
            result.plan_execution = plan_exec

    find_pool: ThreadPoolExecutor | None = None
    find_inflight: dict[Any, tuple[Any, Any]] = {}
    find_merge_time_s = 0.0

    def _submit_find_download(album, result) -> None:
        nonlocal find_pool
        if find_pool is None:
            find_pool = ThreadPoolExecutor(
                max_workers=max(1, total),
                thread_name_prefix="find-download",
            )
        find_ctx = prepare_find_download_context(album, ctx, result)
        future = find_pool.submit(find_download, album, find_ctx)
        find_inflight[future] = (album, result)
        ctx.find_download_queued += 1

    def _apply_find_future(future, *, log_search: bool = True) -> None:
        album, result = find_inflight.pop(future)
        try:
            find_result = future.result()
        except Exception:
            logger.exception(f"find_download crashed for {album.title}")
            find_result = FindDownloadResult(outcome="enqueue_failed")
        _apply_find_download_result(
            album, result, find_result, failed_grab, grab_list, ctx,
        )
        ctx.find_download_completed += 1
        if log_search:
            try:
                _log_search_result(album, result, ctx)
            except Exception:
                logger.exception(
                    "Failed to log search result after find_download for %s",
                    getattr(album, "title", album),
                )

    def _drain_completed_find() -> None:
        nonlocal find_merge_time_s
        if not find_inflight:
            return
        done, _pending = wait(
            list(find_inflight),
            timeout=0,
            return_when=FIRST_COMPLETED,
        )
        if not done:
            return
        drain_start = time.time()
        for future in done:
            _apply_find_future(future)
        elapsed = time.time() - drain_start
        find_merge_time_s += elapsed
        ctx.find_download_drain_time_s += elapsed

    def _drain_find_after_owner_exception() -> None:
        nonlocal find_pool, find_merge_time_s
        logger.exception(
            "Search pipeline owner path crashed; draining submitted "
            "find_download work before returning partial results"
        )
        if find_pool is not None:
            find_pool.shutdown(wait=True, cancel_futures=True)
            find_pool = None
        if not find_inflight:
            return
        drain_start = time.time()
        for future in as_completed(list(find_inflight)):
            try:
                _apply_find_future(future, log_search=False)
            except Exception:
                logger.exception("Failed to merge find_download result after owner crash")
        elapsed = time.time() - drain_start
        find_merge_time_s += elapsed
        ctx.find_download_drain_time_s += elapsed

    try:
        with ThreadPoolExecutor(max_workers=max_inflight) as pool:
            # Seed the pipeline with initial searches
            inflight: dict[Any, Any] = {}
            for _ in range(min(max_inflight, len(album_queue))):
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
                        # Collection crashed AFTER the search was submitted
                        # and accepted by slskd. Per the attempt-outcome
                        # contract this consumes the slot — attach the
                        # plan_execution snapshot so _log_search_result
                        # writes a consumed accepted-stage row.
                        from lib.search import SearchResult
                        plan_exec = inflight_plan_execution.pop(future, None)
                        # ``final_state="collection_crash"`` discriminates
                        # this consumed-after-acceptance path from
                        # pre-submit errors (which leave final_state=None
                        # and plan_execution=None or attached but
                        # without a final_state).
                        sr = SearchResult(
                            album_id=album.id, success=False, outcome="error",
                            variant_tag=(
                                plan_exec.plan_strategy
                                if plan_exec is not None else None),
                            plan_execution=plan_exec,
                            final_state="collection_crash",
                        )
                        _log_search_result(album, sr, ctx)
                        failed_search.append(album)
                    else:
                        _attach_plan_execution(future, result)
                        done_count = (
                            len(grab_list)
                            + len(failed_grab)
                            + len(failed_search)
                            + len(find_inflight)
                        )
                        logger.info(
                            f"Search {done_count + 1}/{total} done: {result.query} "
                            f"({result.result_count if result.result_count is not None else 'n/a'} results, "
                            f"{result.elapsed_s:.1f}s)"
                        )
                        if result.success:
                            _merge_search_result(result, ctx)
                            try:
                                _submit_find_download(album, result)
                            except Exception:
                                logger.exception(
                                    f"find_download submission failed for {album.title}"
                                )
                                find_result = FindDownloadResult(outcome="enqueue_failed")
                                _apply_find_download_result(
                                    album, result, find_result, failed_grab, grab_list, ctx,
                                )
                                _log_search_result(album, result, ctx)
                        else:
                            failed_search.append(album)
                            _log_search_result(album, result, ctx)

                    # Refill: submit next search to keep pipeline full before
                    # doing any opportunistic find-result merge work.
                    submitted = _submit_next()
                    if submitted:
                        new_future, new_album = submitted
                        inflight[new_future] = new_album
                    _drain_completed_find()

                    # Break out of the as_completed loop to re-enter with updated dict
                    break
    except Exception:
        owner_exc = sys.exception()
        _drain_find_after_owner_exception()
        shutdown_browse_coordinator(ctx, wait=True, cancel_futures=True)
        raise FindDownloadOwnerPathError(
            "Search pipeline owner path failed after find_download work was queued; "
            "submitted side effects were drained before aborting the cycle"
        ) from owner_exc

    wall_elapsed = time.time() - wall_start
    # U1 instrumentation (issue #198 R13): credit the search phase wall time
    # to the per-cycle accumulator so the cycle summary can split it from
    # browse/match. Includes both submit (network round-trip) and collect
    # (poll + result merge) since both are gated by slskd's pipeline depth.
    ctx.search_time_s += max(0.0, wall_elapsed - find_merge_time_s)

    try:
        if find_inflight:
            find_drain_start = time.time()
            for future in as_completed(list(find_inflight)):
                try:
                    _apply_find_future(future)
                except Exception:
                    logger.exception("Failed to merge find_download result")
            ctx.find_download_drain_time_s += time.time() - find_drain_start
    finally:
        if find_pool is not None:
            find_pool.shutdown(wait=True)
        shutdown_browse_coordinator(ctx)

    logger.info(f"Pipelined search complete: {total} albums in {wall_elapsed:.1f}s "
                f"(found={len(grab_list)}, no_match={len(failed_grab)}, "
                f"no_results={len(failed_search)})")

    return grab_list, failed_search, failed_grab


from lib.download import grab_most_wanted as _grab_most_wanted_impl
from lib.slskd_transfers import (cancel_and_delete as _cancel_and_delete_impl,
                                 slskd_do_enqueue as _slskd_do_enqueue_impl)


def _make_ctx():
    """Return the module-level CratediggerContext (created in main())."""
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

    parser = argparse.ArgumentParser(description="Cratedigger music download pipeline")
    parser.add_argument("-c", "--config-dir", default=os.getcwd(),
                        help="Config directory (default: cwd)")
    parser.add_argument("-v", "--var-dir", default=os.getcwd(),
                        help="Var directory for lock file and caches (default: cwd)")
    parser.add_argument("--no-lock-file", action="store_true",
                        help="Disable lock file creation")
    parser.add_argument("--redis-host", default=None,
                        help="Redis host for the pipeline peer cache")
    parser.add_argument("--redis-port", type=int, default=None,
                        help="Redis port for the pipeline peer cache")
    parser.add_argument("--reconcile-dry-run", action="store_true",
                        help="Run startup search-plan reconciliation in "
                             "read-only mode and exit -- useful for deploy "
                             "verification. No plans are generated; only "
                             "classification counts are emitted.")
    args = parser.parse_args()

    lock_file_path = os.path.join(args.var_dir, ".cratedigger.lock")
    config_file_path = os.path.join(args.config_dir, "config.ini")

    if not args.no_lock_file and os.path.exists(lock_file_path):
        logger.info("Cratedigger instance is already running.")
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
                "Under the upstream NixOS module, /var/lib/cratedigger/config.ini "
                "is rendered by preStartScript at boot."
            )
            sys.exit(1)

        # --- Parse config into typed dataclass ---
        from lib.config import CratediggerConfig
        cfg = CratediggerConfig.from_ini(config, config_dir=args.config_dir, var_dir=args.var_dir)
        if args.redis_host is not None or args.redis_port is not None:
            redis_port = (
                max(1, min(65535, args.redis_port))
                if args.redis_port is not None
                else cfg.peer_cache_redis_port
            )
            cfg = replace(
                cfg,
                peer_cache_redis_host=args.redis_host or cfg.peer_cache_redis_host,
                peer_cache_redis_port=redis_port,
            )

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

        slskd = _create_slskd_client(cfg)

        # Build context with fresh caches for this cycle
        from lib.context import CratediggerContext
        from lib.download_ownership import DownloadOwnershipWriter
        _module_ctx = CratediggerContext(
            cfg=cfg,
            slskd=slskd,
            pipeline_db_source=pipeline_db_source,
            download_ownership=DownloadOwnershipWriter(cfg.pipeline_db_dsn),
        )
        from lib.peer_cache import connect_from_config
        _module_ctx.peer_cache = connect_from_config(cfg)

        # Populate global user cooldowns (issue #39)
        try:
            db = pipeline_db_source._get_db()
            cooled = db.get_cooled_down_users()
            _module_ctx.cooled_down_users = set(cooled)
            if cooled:
                logger.info(f"User cooldowns active: {', '.join(sorted(cooled))}")
        except Exception as e:
            logger.warning(f"Failed to load user cooldowns: {e}")

        cycle_started_at = datetime.now(timezone.utc)
        cycle_start = time.time()
        # Per-cycle watchdog counter (issue #212). Reset at cycle start;
        # incremented by `_log_search_result` for every SearchResult whose
        # `watchdog_fired=True`.
        _module_ctx.cycle_searches_watchdog_killed = 0

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
            phase1_ctx = CratediggerContext(
                cfg=cfg,
                slskd=slskd,
                pipeline_db_source=phase1_source,
                cooled_down_users=_module_ctx.cooled_down_users,
            )
            try:
                _poll_impl(phase1_ctx)
            finally:
                phase1_source.close()

        # --- Startup search-plan reconciliation (U4) ---
        # Walk every wanted request and ensure each has a current-generator
        # active plan or a visible failed/retryable record. Any row that ends
        # up unclassified (no plan + no current-generator failure record) is
        # surfaced as a stop-the-deploy signal at ERROR. We never block the
        # cycle on transient failures -- the cycle continues with whatever
        # rows are searchable.
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        from lib.search_plan_service import SearchPlanService
        from lib.startup_reconciliation import reconcile_search_plans
        try:
            recon_db = pipeline_db_source._get_db()
            recon_service = (
                None if args.reconcile_dry_run
                else SearchPlanService(recon_db, cfg)
            )
            recon_summary = reconcile_search_plans(
                recon_db,
                recon_service,
                dry_run=args.reconcile_dry_run,
            )
            logger.info(recon_summary.to_log_line())
            if recon_summary.unclassified_no_plan > 0:
                logger.error(
                    "search_plan_reconciliation: %d wanted row(s) lack "
                    "explainable plan state -- stop-the-deploy signal "
                    "(see prior ERROR lines for the request ids)",
                    recon_summary.unclassified_no_plan,
                )
        except Exception:
            logger.exception(
                "Startup search-plan reconciliation failed; continuing "
                "with whatever rows are already searchable.")
            recon_summary = None

        if args.reconcile_dry_run:
            # Dry-run mode is a deploy-verification tool. Skip Phase 2
            # entirely so operators can preflight without producing
            # search traffic.
            logger.info(
                "--reconcile-dry-run set; skipping Phase 1 + Phase 2 "
                "search execution.")
            return

        # --- Plex addedAt pin reconciliation (migration 040) ---
        # Restore the original "added" date on albums that an upgrade
        # re-import (and its Plex rescan) bumped to now, so re-acquired
        # albums don't wrongly appear in Plex "Recently Added". Bounded,
        # best-effort, never blocks the cycle. Normal cycles only (skipped
        # under --reconcile-dry-run via the return above).
        try:
            from lib.plex_pin_service import reconcile_plex_added_at_pins
            pin_result = reconcile_plex_added_at_pins(
                cfg, pipeline_db_source._get_db(),
                now=datetime.now(timezone.utc))
            logger.info(pin_result.to_log_line())
        except Exception:
            logger.exception(
                "PLEX PIN: reconciliation failed; continuing with the cycle.")

        # --- Phase 0: slskd orphan-transfer convergence (issue #278) ---
        # Cancel live slskd transfers that no downloading row owns.
        # Operator actions (Replace) deliberately leave in-flight
        # transfers running instead of building per-action cancellation
        # (CLAUDE.md invariant 7); this is the convergence that reaps
        # them. Must run BEFORE Phase 1/Phase 2 start: the loop is
        # quiescent here, so an unowned live transfer is genuinely
        # orphaned rather than racing a mid-flight enqueue. Best-effort —
        # never blocks the cycle.
        try:
            from lib.slskd_transfers import converge_slskd_orphans
            converge_slskd_orphans(_module_ctx)
        except Exception:
            logger.exception(
                "SLSKD ORPHAN: convergence failed; continuing with the cycle.")

        # --- Phase 0b: on-disk orphan reaper (issue #550 defect 3) ---
        # Completed-but-unconsumed downloads have no slskd-side handle:
        # the convergence above only cancels LIVE transfers, and
        # remove_completed_downloads() purges slskd's completed-transfer
        # records at the end of every cycle. This reaper reasons from
        # filesystem + DB state instead. Same quiescent-window guarantee
        # as the convergence above; best-effort — never blocks the cycle.
        try:
            from lib.slskd_transfers import reap_disk_orphans
            reap_disk_orphans(_module_ctx)
        except Exception:
            logger.exception(
                "DISK-REAP: sweep failed; continuing with the cycle.")

        logger.info("Starting Phase 1 (poll downloads) in background...")
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="phase1") as pool:
            phase1_future = pool.submit(_run_phase1)

            # --- Phase 2: Search and enqueue new downloads (main thread) ---
            #
            # Use ``get_wanted_searchable`` so only rows with a
            # current-generator active plan execute searches. A row
            # requeued to wanted by Phase 1 mid-cycle is excluded until
            # the NEXT reconciliation pass repairs it -- the active-plan
            # FK is the gate.
            logger.info("Getting wanted records from pipeline DB...")
            wanted_records = pipeline_db_source.get_wanted_searchable(
                SEARCH_PLAN_GENERATOR_ID, limit=cfg.page_size)
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
                    logger.info("Cratedigger finished. Exiting...")
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

        # --- Pre-purge terminal transfer evidence harvest (issue #564) ---
        # remove_completed_downloads() below discards slskd's per-transfer
        # terminal state (including the failure reason) for anything that
        # completed/errored within THIS cycle before the next poll cycle
        # ever observes it. This harvest takes one last snapshot and
        # stamps that evidence into active_download_state first. MUST
        # stay ordered before the purge — best-effort, never blocks the
        # cycle (the purge still runs even if the harvest fails).
        try:
            from lib.download import harvest_terminal_transfer_evidence
            harvest_terminal_transfer_evidence(_module_ctx)
        except Exception:
            logger.exception(
                "HARVEST: pre-purge evidence harvest failed; continuing "
                "with the cycle.")

        # Clean up completed transfer UI entries
        slskd.transfers.remove_completed_downloads()

        elapsed = time.time() - cycle_start
        from lib.cycle_summary import format_cycle_summary
        logger.info(format_cycle_summary(_module_ctx, elapsed))
        cycle_completed_at = datetime.now(timezone.utc)
        try:
            db = pipeline_db_source._get_db()
            db.record_cycle_metrics(
                started_at=cycle_started_at,
                completed_at=cycle_completed_at,
                cycle_total_s=elapsed,
                browse_time_s=_module_ctx.browse_time_s,
                match_time_s=_module_ctx.match_time_s,
                search_time_s=_module_ctx.search_time_s,
                cache_pos_hits=_module_ctx.cache_pos_hits,
                cache_neg_hits=_module_ctx.cache_neg_hits,
                cache_misses=_module_ctx.cache_misses,
                cache_errors=_module_ctx.cache_errors,
                cache_fuse_tripped=_module_ctx.cache_fuse_tripped,
                cache_write_errors=_module_ctx.cache_write_errors,
                peers_browsed=_module_ctx.peers_browsed,
                peers_browsed_lazy=_module_ctx.peers_browsed_lazy,
                fanout_waves=_module_ctx.fanout_waves,
                cycle_searches_watchdog_killed=(
                    _module_ctx.cycle_searches_watchdog_killed
                ),
                find_download_queued=_module_ctx.find_download_queued,
                find_download_completed=_module_ctx.find_download_completed,
                find_download_drain_time_s=_module_ctx.find_download_drain_time_s,
            )
        except Exception as e:
            logger.warning(f"Failed to persist cycle metrics: {e}")
        try:
            observations = _module_ctx.peer_observations
            if observations:
                db = pipeline_db_source._get_db()
                new_observations = db.record_peer_observations(
                    observations,
                    observed_at=cycle_completed_at,
                )
                logger.info(
                    "Peer observations persisted: "
                    f"observed={len(observations)} new={new_observations}"
                )
        except Exception as e:
            logger.warning(f"Failed to persist peer observations: {e}")

    finally:
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
