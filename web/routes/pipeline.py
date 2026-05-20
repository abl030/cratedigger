"""Pipeline API route handlers, extracted from server.py."""

import json
import logging
import re
from pathlib import Path
from typing import Literal

import msgspec
from pydantic import BaseModel, Field, model_validator

from web.routes._pydantic import parse_body

logger = logging.getLogger(__name__)

from lib import transitions

# Module-level DI seam for ``transitions.finalize_request``. Routes call
# this name (not ``transitions.finalize_request`` directly) so tests can
# swap it via ``patch.object(routes.pipeline, "finalize_request", new=...)``
# at the same module-level scope as ``web.server.db``. See the leaf-seam
# allowlist in ``tests/_mock_audit_scanner.py``.
finalize_request = transitions.finalize_request
from lib.audio_hash import AudioHashError, hash_audio_content
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.pipeline_db import BadAudioHashInput
from web.download_history_view import (
    build_download_history_row,
    build_download_history_rows,
    classify_download_log_row,
)
from lib.quality import (QUALITY_LOSSLESS, QUALITY_UPGRADE_TIERS,
                         CandidateScore,
                         resolve_user_requeue_override,
                         should_clear_lossless_search_override,
                         top_candidates,
                         get_decision_tree)
from lib.import_preview import ImportPreviewValues, preview_import_from_values
from lib.release_identity import detect_release_source, normalize_release_id
from lib.release_cleanup import remove_and_reset_release
from lib.util import resolve_failed_path
from lib.spectral_check import (HF_DEFICIT_SUSPECT, HF_DEFICIT_MARGINAL,
                                ALBUM_SUSPECT_PCT, MIN_CLIFF_SLICES,
                                CLIFF_THRESHOLD_DB_PER_KHZ)
from web import mb as mb_api
from web import discogs as discogs_api
from web import cache as cache_api
from web.wrong_match_file_service import source_dirs_from_validation_result

DEFAULT_PIPELINE_LOG_LIMIT = 50
MAX_PIPELINE_LOG_LIMIT = 500


def _generate_plan_after_add(req_id, *, artist_name, album_title, year,
                              tracks, source, release_group_year=None):
    """Run shared plan generation after `set_tracks()` on the add path.

    Failures are recorded but never bubble up — the request is repairable
    via startup reconciliation or explicit regeneration. This keeps the
    add API contract stable: a 200 response means the request landed,
    even if plan generation needs another attempt.

    ``release_group_year`` (U5 of search-plan-entropy) feeds the
    generator's conditional ``unwild_rg_year`` slot for reissues. Pass
    ``None`` when unknown — the generator handles it gracefully.
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import SearchPlanService
    s = _server()
    try:
        svc = SearchPlanService(s._db(), read_runtime_config())
        svc.generate_for_new_request(
            req_id,
            artist_name=artist_name,
            album_title=album_title,
            year=year,
            tracks=tracks or [],
            source=source,
            release_group_year=release_group_year,
        )
    except Exception as exc:  # noqa: BLE001
        # Never fail the HTTP request because plan generation hiccupped.
        logger.warning(
            "post_pipeline_add: plan generation failed for request %s: %s",
            req_id, exc,
        )


def _server():
    """Deferred import to avoid circular deps."""
    from web import server
    return server


def _pipeline_log_limit(params: dict[str, list[str]]) -> int:
    raw = params.get("limit", [str(DEFAULT_PIPELINE_LOG_LIMIT)])[0]
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_PIPELINE_LOG_LIMIT
    return max(1, min(limit, MAX_PIPELINE_LOG_LIMIT))


# ── GET handlers ─────────────────────────────────────────────────


def get_pipeline_log(h, params: dict[str, list[str]]) -> None:
    outcome_filter = params.get("outcome", [None])[0]
    if outcome_filter not in (None, "imported", "rejected"):
        outcome_filter = None
    entries = _server()._db().get_log(
        limit=_pipeline_log_limit(params),
        outcome_filter=outcome_filter,
    )
    mbids = list(set(e["mb_release_id"] for e in entries if e.get("mb_release_id")))
    beets_info = _server().check_beets_library_detail(mbids) if mbids else {}
    result = []
    for e in entries:
        classified_row = classify_download_log_row(e)
        entry = classified_row.entry
        classified = classified_row.classified
        item = entry.to_json_dict()
        mbid = entry.mb_release_id
        bi = beets_info.get(mbid) if mbid else None
        item["in_beets"] = bi is not None
        if bi:
            item["beets_format"] = bi.get("beets_format")
            item["beets_bitrate"] = bi.get("beets_bitrate")
        item["badge"] = classified.badge
        item["badge_class"] = classified.badge_class
        item["border_color"] = classified.border_color
        item["verdict"] = classified.verdict
        item["summary"] = classified.summary
        # Issue #130: surface post-import `beet move` failures so the
        # Recents tab can render a warning chip without forcing the
        # operator to query JSONB manually. Null on clean rows.
        item["disambiguation_failure"] = classified.disambiguation_failure
        item["disambiguation_detail"] = classified.disambiguation_detail
        item["bad_extensions"] = classified.bad_extensions
        item["wrong_match_triage_action"] = classified.wrong_match_triage_action
        item["wrong_match_triage_summary"] = classified.wrong_match_triage_summary
        item["wrong_match_triage_reason"] = classified.wrong_match_triage_reason
        item["wrong_match_triage_preview_verdict"] = (
            classified.wrong_match_triage_preview_verdict
        )
        item["wrong_match_triage_preview_decision"] = (
            classified.wrong_match_triage_preview_decision
        )
        item["wrong_match_triage_stage_chain"] = (
            classified.wrong_match_triage_stage_chain
        )
        item["wrong_match_triage_detail"] = classified.wrong_match_triage_detail
        result.append(item)
    # Count recents filters plus found-search enqueue rates (single query).
    count_cur = _server()._db()._execute("""
        WITH download_counts AS (
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE outcome IN ('success', 'force_import')
                ) AS imported
            FROM download_log
        ),
        match_counts AS (
            SELECT
                COUNT(*) FILTER (
                    WHERE outcome = 'found'
                      AND created_at >= NOW() - INTERVAL '24 hours'
                )::int AS matches_24h,
                COUNT(*) FILTER (
                    WHERE outcome = 'found'
                      AND created_at >= NOW() - INTERVAL '6 hours'
                )::int AS matches_6h
            FROM search_log
        )
        SELECT
            download_counts.total,
            download_counts.imported,
            match_counts.matches_24h,
            match_counts.matches_6h
        FROM download_counts
        CROSS JOIN match_counts
    """)
    count_row = count_cur.fetchone()
    total = count_row["total"] if count_row else 0
    imported_c = count_row["imported"] if count_row else 0
    matches_24h = count_row.get("matches_24h", 0) if count_row else 0
    matches_6h = count_row.get("matches_6h", 0) if count_row else 0
    h._json({
        "log": result,
        "counts": {
            "all": total,
            "imported": imported_c,
            "rejected": total - imported_c,
            "matches_24h": matches_24h,
            "matches_6h": matches_6h,
            "matches_per_hour_24h": matches_24h / 24,
            "matches_per_hour_6h": matches_6h / 6,
        },
    })


def get_pipeline_status(h, params: dict[str, list[str]]) -> None:
    counts = _server()._db().count_by_status()
    wanted = _server()._db().get_wanted(limit=50)
    h._json({
        "counts": counts,
        "wanted": [
            {
                "id": w["id"],
                "artist": w["artist_name"],
                "album": w["album_title"],
                "mb_release_id": w["mb_release_id"],
                "source": w["source"],
                "created_at": str(w["created_at"]),
            }
            for w in wanted
        ],
    })


def get_pipeline_recent(h, params: dict[str, list[str]]) -> None:
    s = _server()
    recent = s._db().get_recent(limit=20)
    mbids = [r["mb_release_id"] for r in recent if r.get("mb_release_id")]
    beets_info = s.check_beets_library_detail(mbids) if mbids else {}
    # Batch fetch track counts and download history
    ids = [int(r["id"]) for r in recent]
    track_counts = s._db().get_track_counts(ids)
    history_batch = s._db().get_download_history_batch(ids)
    serialized = []
    for r in recent:
        item = s._serialize_row(r)
        mbid = r.get("mb_release_id")
        item["pipeline_tracks"] = track_counts.get(r["id"], 0)
        if mbid and mbid in beets_info:
            item["in_beets"] = True
            bi = beets_info[mbid]
            item["beets_tracks"] = bi["beets_tracks"]
            for k in ("beets_format", "beets_bitrate", "beets_samplerate", "beets_bitdepth"):
                if bi.get(k):
                    item[k] = bi[k]
        else:
            # Issue #123: artist+album fuzzy fallback deleted. Legacy
            # rows with an untagged beets copy now honestly read as
            # 'not in library' — fuzzy LIKE matches could return a
            # track count for an unrelated sibling pressing by the
            # same artist, which misled the UI's 'already on disk'
            # signal.
            item["in_beets"] = False
            item["beets_tracks"] = 0
        history = history_batch.get(r["id"], [])
        success = next((dl for dl in history if dl.get("outcome") == "success"), None)
        if success:
            for k in ("soulseek_username", "filetype", "bitrate",
                      "sample_rate", "bit_depth", "is_vbr",
                      "was_converted", "original_filetype"):
                val = success.get(k)
                if val is not None:
                    item["dl_" + k] = val
        serialized.append(item)
    h._json({"recent": serialized})


def _attach_latest_download_history(
    items: list[dict],
    history_batch: dict[int, list[dict]],
) -> list[dict]:
    for item in items:
        history = history_batch.get(item["id"], [])
        if history:
            last = build_download_history_row(history[0])
            item["last_verdict"] = last.verdict
            item["last_outcome"] = last.outcome
            item["last_username"] = last.soulseek_username
            item["download_count"] = len(history)
    return items


def get_pipeline_all(h, params: dict[str, list[str]]) -> None:
    s = _server()
    counts = s._db().count_by_status()
    all_data: dict[str, object] = {"counts": counts}
    status_items: dict[str, list[dict]] = {}
    all_ids: list[int] = []
    statuses: tuple[str, ...] = ("wanted", "downloading", "imported", "manual")
    # ``?include_replaced=true`` opt-in surfaces the frozen audit rows
    # for operators reviewing past Replace actions (R30). Default off so
    # the standard view stays focused on active work.
    include_replaced = (
        params.get("include_replaced", ["false"])[0].lower() == "true"
    )
    if include_replaced:
        statuses = statuses + ("replaced",)
    for status in statuses:
        rows = [s._serialize_row(r) for r in s._db().get_by_status(status)]
        status_items[status] = rows
        all_ids.extend([int(str(r["id"])) for r in rows])
    history_batch = s._db().get_download_history_batch(all_ids)
    for status in statuses:
        all_data[status] = _attach_latest_download_history(
            status_items[status],
            history_batch,
        )
    h._json(all_data)


def get_pipeline_downloading(h, params: dict[str, list[str]]) -> None:
    s = _server()
    counts = s._db().count_by_status()
    rows = [s._serialize_row(r) for r in s._db().get_by_status("downloading")]
    ids = [int(str(r["id"])) for r in rows]
    history_batch = s._db().get_download_history_batch(ids)
    h._json({
        "counts": counts,
        "downloading": _attach_latest_download_history(rows, history_batch),
    })


def get_pipeline_dashboard(h, params: dict[str, list[str]]) -> None:
    """Return operational metrics for the Pipeline dashboard subtab."""
    s = _server()
    data = s._db().get_pipeline_dashboard_metrics()
    data["redis"] = cache_api.redis_metrics()
    h._json(data)


def _runtime_rank_config():
    """Load the runtime QualityRankConfig from the same config.ini the main
    cratedigger process reads, so web simulator matches production dispatch."""
    from lib.config import read_runtime_rank_config  # type: ignore[import-not-found]

    return read_runtime_rank_config()


def get_pipeline_constants(h, params: dict[str, list[str]]) -> None:
    """Return decision tree structure + thresholds for the diagram.

    The runtime rank config is threaded into ``get_decision_tree`` so the
    transcode-detection threshold displayed in the UI tracks the live
    ``cfg.mp3_vbr.excellent`` (issue #66 follow-up). Without this, an
    operator who retuned the gate would see a stale Decisions tab while
    the actual pipeline ran at the new threshold.
    """
    rank_cfg = _runtime_rank_config()
    tree = get_decision_tree(cfg=rank_cfg)
    tree["constants"]["HF_DEFICIT_SUSPECT"] = HF_DEFICIT_SUSPECT
    tree["constants"]["HF_DEFICIT_MARGINAL"] = HF_DEFICIT_MARGINAL
    tree["constants"]["ALBUM_SUSPECT_PCT"] = ALBUM_SUSPECT_PCT
    tree["constants"]["MIN_CLIFF_SLICES"] = MIN_CLIFF_SLICES
    tree["constants"]["CLIFF_THRESHOLD_DB_PER_KHZ"] = CLIFF_THRESHOLD_DB_PER_KHZ
    # Expose the runtime rank config to the UI so the Decisions tab shows
    # the configured gate_min_rank, bitrate_metric, and the same-rank
    # tolerance. The frontend renders these three as labeled badges at
    # the top of the tab (issue #68).
    tree["constants"]["rank_gate_min_rank"] = rank_cfg.gate_min_rank.name
    tree["constants"]["rank_bitrate_metric"] = rank_cfg.bitrate_metric.value
    tree["constants"]["rank_within_tolerance_kbps"] = (
        rank_cfg.within_rank_tolerance_kbps)
    # Expose the runtime audio_check_mode so the simulator presets can
    # reflect deployments with `[Beets Validation] audio_check = off`.
    # Without this, the Decisions tab would claim corrupt downloads get
    # rejected even though run_preimport_gates() skips validation there
    # (issue #91 codex round 2).
    from lib.config import read_runtime_config  # type: ignore[import-not-found]
    tree["constants"]["audio_check_mode"] = read_runtime_config().audio_check_mode
    h._json(tree)


def get_pipeline_simulate(h, params: dict[str, list[str]]) -> None:
    """Run full_pipeline_decision() with query-string inputs."""

    def _str(key: str) -> str | None:
        v = params.get(key, [None])[0]
        return v if v else None

    def _int(key: str) -> int | None:
        v = _str(key)
        return int(v) if v else None

    def _bool(key: str) -> bool:
        v = _str(key)
        return v in ("true", "1", "yes") if v else False

    # is_vbr defaults to None (not False) so the simulator can tell
    # "not supplied, derive from is_cbr" apart from "explicit CBR".
    def _opt_bool(key: str) -> bool | None:
        v = _str(key)
        if v is None:
            return None
        return v in ("true", "1", "yes")

    preview = preview_import_from_values(
        ImportPreviewValues(
            is_flac=_bool("is_flac"),
            min_bitrate=_int("min_bitrate") or 0,
            is_cbr=_bool("is_cbr"),
            is_vbr=_opt_bool("is_vbr"),
            avg_bitrate=_int("avg_bitrate"),
            spectral_grade=_str("spectral_grade"),
            spectral_bitrate=_int("spectral_bitrate"),
            existing_min_bitrate=_int("existing_min_bitrate"),
            existing_avg_bitrate=_int("existing_avg_bitrate"),
            existing_spectral_grade=_str("existing_spectral_grade"),
            existing_spectral_bitrate=_int("existing_spectral_bitrate"),
            override_min_bitrate=_int("override_min_bitrate"),
            existing_format=_str("existing_format"),
            existing_is_cbr=_bool("existing_is_cbr"),
            new_format=_str("new_format"),
            post_conversion_min_bitrate=_int("post_conversion_min_bitrate"),
            converted_count=_int("converted_count") or 0,
            verified_lossless=_bool("verified_lossless"),
            target_format=_str("target_format"),
            verified_lossless_target=_str("verified_lossless_target"),
            # Preimport gate inputs (issue #91). Defaults preserve legacy simulator
            # behavior — a caller that omits these runs the pipeline as if audio
            # validation passed and the auto path flattened the download.
            audio_check_mode=_str("audio_check_mode") or "normal",
            audio_corrupt=_bool("audio_corrupt"),
            import_mode=_str("import_mode") or "auto",
            has_nested_audio=_bool("has_nested_audio"),
            candidate_v0_probe_avg=_int("candidate_v0_probe_avg"),
            candidate_v0_probe_min=_int("candidate_v0_probe_min"),
            existing_v0_probe_avg=_int("existing_v0_probe_avg"),
            candidate_v0_probe_kind=_str("candidate_v0_probe_kind"),
            existing_v0_probe_kind=_str("existing_v0_probe_kind"),
            supported_lossless_source=_opt_bool("supported_lossless_source"),
        ),
        cfg=_runtime_rank_config(),
    )
    h._json(preview.simulation or {})


def _build_last_search_payload(
    search_history: list[dict[str, object]],
) -> dict[str, object] | None:
    """Build the ``last_search`` slice of the request-detail response.

    Single decode site (per ``.claude/rules/code-quality.md`` § Wire-boundary
    types) for the ``search_log.candidates`` JSONB blob: ``msgspec.convert``
    turns it into ``list[CandidateScore]`` here, and the response is
    re-encoded via ``msgspec.to_builtins`` for symmetric strictness. Older
    rows with ``candidates=NULL`` (or missing) read as ``[]`` — no
    ``ValidationError``. Returns ``None`` when the request has no
    search_log rows yet.
    """
    if not search_history:
        return None
    latest = search_history[0]  # get_search_history orders newest first
    raw_candidates = latest.get("candidates")
    candidates: list[CandidateScore]
    if raw_candidates is None:
        candidates = []
    else:
        try:
            candidates = msgspec.convert(
                raw_candidates, type=list[CandidateScore]
            )
        except msgspec.ValidationError as exc:
            # Mirrors the CLI's defensive guard in
            # scripts/pipeline_cli.py:_render_search_forensics_summary —
            # production writes via the same Struct so this should never trip,
            # but a corrupted historical row must not 500 the detail route.
            logger.warning(
                "search_log.candidates JSONB failed msgspec validation "
                "(request_id=%s, search_log_id=%s): %s",
                latest.get("request_id"), latest.get("id"), exc,
            )
            candidates = []
    # Top-3 by (matched_tracks DESC, avg_ratio DESC) for compact list view;
    # full forensic blob still reachable via the search history for
    # operators who want it. Shared ranking lives in lib/quality.py.
    top = top_candidates(candidates, limit=3)
    return {
        "variant": latest.get("variant"),
        "final_state": latest.get("final_state"),
        "outcome": latest.get("outcome"),
        "top_candidates": [msgspec.to_builtins(c) for c in top],
    }


def get_pipeline_detail(h, params: dict[str, list[str]], req_id_str: str) -> None:
    s = _server()
    req_id = int(req_id_str)
    req = s._db().get_request(req_id)
    if not req:
        h._error("Not found", 404)
        return
    tracks = s._db().get_tracks(req_id)
    history = s._db().get_download_history(req_id)
    history_items = [item.to_dict() for item in build_download_history_rows(history)]
    search_history = s._db().get_search_history(req_id)
    last_search = _build_last_search_payload(search_history)
    result: dict[str, object] = {
        "request": s._serialize_row(req),
        "tracks": tracks,
        "history": history_items,
        "manual_reason": req.get("manual_reason"),
        "last_search": last_search,
    }
    mbid = req.get("mb_release_id")
    b = s._beets_db()
    if mbid and b:
        tracks = b.get_tracks_by_mb_release_id(mbid)
        if tracks is not None:
            result["beets_tracks"] = tracks
    h._json(result)


def get_pipeline_search_plan(
    h, params: dict[str, list[str]], req_id_str: str,
) -> None:
    """U6: read-only view of a request's persisted search plan.

    Mirrors ``pipeline-cli search-plan show --json`` so the future
    dashboard and operators see the same JSON. The U8 stats block is
    included by default; pass ``stats=0`` to suppress it for a leaner
    payload (the show endpoint stays a single contract).
    """
    from lib.search_plan_inspection import (
        RequestNotFound,
        build_inspection_payload,
    )
    include_stats = params.get("stats", ["1"])[0] != "0"
    db = _server()._db()
    payload = build_inspection_payload(
        db, int(req_id_str), include_stats=include_stats)
    if isinstance(payload, RequestNotFound):
        h._error("Not found", 404)
        return
    h._json(payload)


def get_pipeline_search_plan_dry_run(
    h, params: dict[str, list[str]], req_id_str: str,
) -> None:
    """U6: ``GET /api/pipeline/<id>/search-plan/dry-run``.

    Read-only generator simulator. Runs ``generate_search_plan``
    against the current persisted snapshot for ``<id>`` and returns
    the resulting plan items without writing anything. Counterpart of
    ``pipeline-cli search-plan dry-run``; both surfaces wrap
    ``SearchPlanService.dry_run_for_request`` — keep them in sync
    (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Query string:
      * ``prepend_artist`` — ``1`` to enable (defaults to runtime
        config's ``album_prepend_artist``).

    Status-code mapping:
      * 200 — ``RESULT_DRY_RUN_SUCCESS`` or
        ``RESULT_DRY_RUN_GENERATION_FAILED`` (the latter is a
        deterministic generator outcome the operator wants to see in
        the body — the route is informational, not a hard error).
      * 404 — ``RESULT_REQUEST_NOT_FOUND``.
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        RESULT_DRY_RUN_GENERATION_FAILED,
        RESULT_DRY_RUN_SUCCESS,
        RESULT_REQUEST_NOT_FOUND,
        SearchPlanService,
        dry_run_payload,
    )
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    prepend_artist_raw = params.get("prepend_artist", [None])[0]
    prepend_artist: bool | None
    if prepend_artist_raw is None or prepend_artist_raw == "":
        prepend_artist = None
    else:
        prepend_artist = prepend_artist_raw == "1"

    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.dry_run_for_request(
        request_id, prepend_artist=prepend_artist,
    )
    row = db.get_request(request_id)
    has_active = False
    if row is not None:
        try:
            active = db.get_active_search_plan(request_id)
            has_active = active is not None
        except Exception:  # noqa: BLE001
            has_active = False
    payload = dry_run_payload(
        result,
        current_generator_id=svc.generator_id,
        request_row=row,
        has_active_plan=has_active,
    )
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        payload["error"] = result.error_message or "Not found"
        h._json(payload, status=404)
        return
    if result.outcome in (
            RESULT_DRY_RUN_SUCCESS, RESULT_DRY_RUN_GENERATION_FAILED):
        h._json(payload)
        return
    # Defensive fallback for any future outcome string.
    h._error(f"Unknown dry-run outcome: {result.outcome}", 500)


def get_pipeline_search_plan_saturation(
    h, params: dict[str, list[str]], req_id_str: str,
) -> None:
    """U7: ``GET /api/pipeline/<id>/search-plan/saturation``.

    Read-only telemetry aggregator. Returns the saturation rate (rows
    whose ``final_state`` contains ``LimitReached``) and total
    ``pre_filter_skip_count`` over the last ``window_days`` (default
    14) of ``search_log`` rows for ``<id>``. Counterpart of
    ``pipeline-cli search-plan saturation``; both surfaces wrap
    ``SearchPlanService.saturation_for_request`` — keep them in sync
    (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Query string:
      * ``window_days`` — int in
        ``[SATURATION_WINDOW_MIN_DAYS, SATURATION_WINDOW_MAX_DAYS]``;
        defaults to ``SATURATION_WINDOW_DEFAULT_DAYS``.

    Status-code mapping:
      * 200 — ``RESULT_SATURATION_SUCCESS`` (counts may be zero — a
        valid "found but quiet" state).
      * 400 — ``window_days`` not parseable as int, or
        ``RESULT_SATURATION_INPUT_INVALID``.
      * 404 — ``RESULT_REQUEST_NOT_FOUND`` (request_id does not exist
        in ``album_requests``; distinct from a real request whose
        window happens to be empty).
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        RESULT_REQUEST_NOT_FOUND,
        RESULT_SATURATION_INPUT_INVALID,
        RESULT_SATURATION_SUCCESS,
        SATURATION_WINDOW_DEFAULT_DAYS,
        SearchPlanService,
        saturation_payload,
    )
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    window_raw = params.get("window_days", [None])[0]
    if window_raw is None or window_raw == "":
        window_days = SATURATION_WINDOW_DEFAULT_DAYS
    else:
        try:
            window_days = int(window_raw)
        except (TypeError, ValueError):
            h._error(
                f"window_days must be an integer; got {window_raw!r}",
                status=400,
            )
            return

    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.saturation_for_request(
        request_id, window_days=window_days,
    )
    payload = saturation_payload(result)
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        payload["error"] = result.error_message or "Not found"
        h._json(payload, status=404)
        return
    if result.outcome == RESULT_SATURATION_INPUT_INVALID:
        payload["error"] = (
            result.error_message or "Invalid window_days")
        h._json(payload, status=400)
        return
    if result.outcome == RESULT_SATURATION_SUCCESS:
        h._json(payload)
        return
    # Defensive fallback for any future outcome string.
    h._error(f"Unknown saturation outcome: {result.outcome}", 500)


def get_pipeline_search_plan_history(
    h, params: dict[str, list[str]], req_id_str: str,
) -> None:
    """``GET /api/pipeline/<id>/search-plan/history``.

    Cursor-paginated read of one request's ``search_log`` rows. Wraps
    ``SearchPlanService.history_for_request``; both this route and
    ``pipeline-cli search-plan history`` go through the same service
    method so the input bounds and outcome mapping cannot drift.

    Query string:
      * ``limit`` — int in ``[1, 200]``; defaults to
        ``HISTORY_PAGE_DEFAULT_LIMIT`` when omitted.
      * ``before_id`` — int >= 1; the ``next_before_id`` from the
        previous page. Omit for the first page.

    Status-code mapping:
      * 200 — ``RESULT_HISTORY_PAGE_SUCCESS``
      * 400 — query string non-int / ``RESULT_HISTORY_PAGE_INPUT_INVALID``
      * 404 — ``RESULT_REQUEST_NOT_FOUND``
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        HISTORY_PAGE_DEFAULT_LIMIT,
        RESULT_HISTORY_PAGE_INPUT_INVALID,
        RESULT_HISTORY_PAGE_SUCCESS,
        RESULT_REQUEST_NOT_FOUND,
        SearchPlanService,
    )
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    limit_raw = params.get("limit", [None])[0]
    if limit_raw is None or limit_raw == "":
        limit = HISTORY_PAGE_DEFAULT_LIMIT
    else:
        try:
            limit = int(limit_raw)
        except (TypeError, ValueError):
            h._error("limit must be an integer")
            return
    before_id_raw = params.get("before_id", [None])[0]
    before_id: int | None
    if before_id_raw is None or before_id_raw == "":
        before_id = None
    else:
        try:
            before_id = int(before_id_raw)
        except (TypeError, ValueError):
            h._error("before_id must be an integer")
            return

    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.history_for_request(
        request_id, limit=limit, before_id=before_id,
    )
    s = _server()
    if result.outcome == RESULT_HISTORY_PAGE_SUCCESS:
        # F1: map rows through _serialize_row so datetime fields
        # (created_at) become ISO strings before json.dumps is called.
        # F5: pass candidates through msgspec.convert + msgspec.to_builtins
        # for symmetric wire-boundary strictness (mirrors _build_last_search_payload).
        serialized_rows: list[dict[str, object]] = []
        for r in result.rows:
            sr = s._serialize_row(r)
            raw_candidates = sr.get("candidates")
            if raw_candidates is not None:
                try:
                    candidates = msgspec.convert(
                        raw_candidates, type=list[CandidateScore])
                    sr["candidates"] = [
                        msgspec.to_builtins(c) for c in candidates]
                except msgspec.ValidationError as exc:
                    logger.warning(
                        "search_log.candidates JSONB failed msgspec validation "
                        "(request_id=%s, search_log_id=%s): %s",
                        r.get("request_id"), r.get("id"), exc,
                    )
                    sr["candidates"] = None
            serialized_rows.append(sr)
        payload: dict[str, object] = {
            "request_id": result.request_id,
            "rows": serialized_rows,
            "next_before_id": result.next_before_id,
        }
        h._json(payload)
        return
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        # F3: match h._error() shape used by neighbor routes (get_pipeline_detail etc.)
        h._error(result.error_message or "Request not found", 404)
        return
    if result.outcome == RESULT_HISTORY_PAGE_INPUT_INVALID:
        h._error(result.error_message or "Invalid history page request", 400)
        return
    # Defensive fallback for any future outcome string.
    h._error(f"Unknown history outcome: {result.outcome}", 500)


class PipelineSearchPlanRegenerateRequest(BaseModel):
    # ``strict=True`` because Pydantic v2's default lax mode coerces
    # ``"true"``/``"false"`` strings to bool — the regenerate route's
    # contract is JSON-bool only and the test pins that.
    prepend_artist: bool | None = Field(default=None, strict=True)


def post_pipeline_search_plan_regenerate(
    h, body: dict, req_id_str: str,
) -> None:
    """U8: ``POST /api/pipeline/<id>/search-plan/regenerate``.

    Wraps ``SearchPlanService.generate_for_request(regenerate=True)``.
    Allowed for any request status; only ``wanted`` requests are
    executable, surfaced via ``executable`` in the response so
    operators can't misread "regenerated" as "now downloading".

    Status-code mapping mirrors the CLI's exit codes:
      * 200 — ``RESULT_SUCCESS`` or ``RESULT_NOOP_ACTIVE_PLAN_EXISTS``
      * 404 — ``RESULT_REQUEST_NOT_FOUND``
      * 422 — ``RESULT_FAILED_DETERMINISTIC`` (sticky, body explains)
      * 503 — ``RESULT_FAILED_TRANSIENT`` (retryable)
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        RESULT_FAILED_DETERMINISTIC,
        RESULT_FAILED_TRANSIENT,
        RESULT_NOOP_ACTIVE_PLAN_EXISTS,
        RESULT_REQUEST_NOT_FOUND,
        RESULT_SUCCESS,
        SearchPlanService,
    )
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    req_body = parse_body(h, body or {}, PipelineSearchPlanRegenerateRequest)
    if req_body is None:
        return
    prepend_artist: bool | None = req_body.prepend_artist
    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.generate_for_request(
        request_id, regenerate=True, prepend_artist=prepend_artist,
    )

    payload: dict[str, object] = {
        "request_id": request_id,
        "outcome": result.outcome,
        "plan_id": result.plan_id,
        "is_supersede": result.is_supersede,
        "failure_class": result.failure_class,
        "error_message": result.error_message,
    }

    req = db.get_request(request_id)
    if req is not None:
        payload["request_status"] = req.get("status")
        payload["executable"] = (
            req.get("status") == "wanted"
            and result.outcome == RESULT_SUCCESS
        )
    else:
        payload["request_status"] = None
        payload["executable"] = False

    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        # Symmetric body shape with 422 / 503: clients expect to see
        # request_id / outcome / plan_id (None) / failure_class /
        # error_message even on the not-found path.
        payload["error"] = "Not found"
        h._json(payload, status=404)
        return
    if result.outcome == RESULT_FAILED_DETERMINISTIC:
        payload["error"] = result.error_message or "Plan generation failed"
        h._json(payload, status=422)
        return
    if result.outcome == RESULT_FAILED_TRANSIENT:
        payload["error"] = result.error_message or "Plan generation retryable"
        h._json(payload, status=503)
        return
    # RESULT_SUCCESS or RESULT_NOOP_ACTIVE_PLAN_EXISTS.
    if result.outcome not in (RESULT_SUCCESS, RESULT_NOOP_ACTIVE_PLAN_EXISTS):
        # Defensive fallback; surface as 500 so we notice unknown shapes.
        h._error(f"Unknown plan generation outcome: {result.outcome}", 500)
        return
    h._json(payload)


class PipelineSearchPlanAdvanceRequest(BaseModel):
    """HTTP body for ``POST /api/pipeline/<id>/search-plan/advance``.

    Exactly one of ``to_ordinal`` / ``to_strategy`` is required. The
    ``@model_validator`` enforces the XOR — Pydantic checks types but
    not "exactly one of two".
    """

    to_ordinal: int | None = None
    to_strategy: str | None = None

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "PipelineSearchPlanAdvanceRequest":
        if (self.to_ordinal is None) == (self.to_strategy is None):
            raise ValueError(
                "exactly one of to_ordinal or to_strategy is required"
            )
        return self


def post_pipeline_search_plan_advance(
    h, body: dict, req_id_str: str,
) -> None:
    """``POST /api/pipeline/<id>/search-plan/advance``.

    Forward-only operator advance of the search-plan cursor. Counterpart
    of ``pipeline-cli search-plan advance``. Both surfaces wrap
    ``SearchPlanService.advance_for_request`` — keep them in sync (see
    ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Body: exactly one of
      * ``{"to_ordinal": <int>}`` — absolute target ordinal
      * ``{"to_strategy": <str>}`` — first slot past cursor whose strategy
        starts with this prefix

    Status-code mapping mirrors the CLI exit codes:
      * 200 — ``RESULT_ADVANCED``
      * 400 — body validation failure (missing/extra keys, wrong type)
      * 404 — ``RESULT_REQUEST_NOT_FOUND``
      * 409 — ``RESULT_NO_ACTIVE_PLAN`` (request needs ``regenerate`` first)
      * 422 — ``RESULT_INVALID_TARGET`` (out of range, backward, no slot
        matches strategy)
      * 503 — ``RESULT_FAILED_TRANSIENT`` (lock contention)
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        RESULT_ADVANCED,
        RESULT_FAILED_TRANSIENT,
        RESULT_INVALID_TARGET,
        RESULT_NO_ACTIVE_PLAN,
        RESULT_REQUEST_NOT_FOUND,
        SearchPlanService,
    )
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    req_body = parse_body(h, body or {}, PipelineSearchPlanAdvanceRequest)
    if req_body is None:
        return

    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.advance_for_request(
        request_id,
        to_ordinal=req_body.to_ordinal,
        to_strategy=req_body.to_strategy,
    )
    payload: dict[str, object] = {
        "request_id": result.request_id,
        "outcome": result.outcome,
        "plan_id": result.plan_id,
        "previous_ordinal": result.previous_ordinal,
        "new_ordinal": result.new_ordinal,
        "new_strategy": result.new_strategy,
        "new_query": result.new_query,
        "error_message": result.error_message,
    }
    if result.outcome == RESULT_ADVANCED:
        h._json(payload)
        return
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        payload["error"] = result.error_message or "Not found"
        h._json(payload, status=404)
        return
    if result.outcome == RESULT_NO_ACTIVE_PLAN:
        payload["error"] = (
            result.error_message or "No active plan; regenerate first")
        h._json(payload, status=409)
        return
    if result.outcome == RESULT_INVALID_TARGET:
        payload["error"] = (
            result.error_message or "Invalid advance target")
        h._json(payload, status=422)
        return
    if result.outcome == RESULT_FAILED_TRANSIENT:
        payload["error"] = (
            result.error_message or "Plan lock contention; retry")
        h._json(payload, status=503)
        return
    # Defensive: any unknown outcome string is a bug.
    h._error(f"Unknown advance outcome: {result.outcome}", 500)


def post_pipeline_resolve_rg(h, body: dict, req_id_str: str) -> None:
    """``POST /api/pipeline/<id>/resolve-rg``.

    Lazy-backfill ``album_requests.mb_release_group_id`` for a single
    legacy row that was added before the RG field was populated.

    Used by ``web/js/replace_picker.js`` standard-mode when the row has
    a null RG — the picker calls this endpoint, persists the resolved
    RG back to the row, then continues into the sibling fetch.

    The persisted side-effect is intentionally idempotent: if the row
    already has a non-null RG the route returns it untouched (no
    redundant MB hit because ``get_release(fresh=False)`` is cache-served).

    Status-code mapping:
      * 200 — ``status='resolved'`` (RG found; row updated or already set)
      * 404 — request id does not exist
      * 422 — MB lookup returned no release_group_id (e.g. the row's
              ``mb_release_id`` is a numeric Discogs id, or the upstream
              MB release has no RG attached)
      * 503 — transient MB-mirror error (timeout, network, malformed
              JSON) — retryable
    """
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return

    db = _server()._db()
    row = db.get_request(request_id)
    if row is None:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": "not_found",
            "error": f"request {request_id} not found",
        }, status=404)
        return

    existing_rg = row.get("mb_release_group_id")
    if existing_rg:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": existing_rg,
            "status": "resolved",
        })
        return

    mb_release_id = row.get("mb_release_id")
    if not mb_release_id:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": "missing_release_id",
            "error": (
                f"request {request_id} has no mb_release_id to resolve"
            ),
        }, status=422)
        return

    # MB release ids are UUIDs; numeric ids are Discogs and have no
    # release-group concept here. Surface 422 so the picker can show a
    # clean error rather than letting the mirror return 404 / 500.
    try:
        import uuid as _uuid
        _uuid.UUID(str(mb_release_id))
    except (ValueError, TypeError, AttributeError):
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": "non_mb_release_id",
            "error": (
                f"request {request_id}.mb_release_id "
                f"{mb_release_id!r} is not a MusicBrainz UUID"
            ),
        }, status=422)
        return

    # MB-mirror transient errors (network, JSON decode) are retryable.
    # See ``lib/mbid_replace_service.py::_TRANSIENT_LOOKUP_EXCEPTIONS``
    # for the rationale and the same exception set.
    import socket as _socket
    from urllib.error import URLError
    transient: tuple[type[BaseException], ...] = (
        URLError, TimeoutError, _socket.timeout, ConnectionError,
        json.JSONDecodeError,
    )
    try:
        data = mb_api.get_release(mb_release_id, fresh=False)
    except transient as exc:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": "transient",
            "error": f"MB lookup failed (transient): {exc}",
        }, status=503)
        return
    except Exception as exc:  # noqa: BLE001
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": "lookup_failed",
            "error": (
                f"MB lookup for {mb_release_id} failed: {exc}"
            ),
        }, status=422)
        return

    rg_id = (data or {}).get("release_group_id") if isinstance(data, dict) else None
    if not rg_id:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": "no_release_group",
            "error": (
                f"MB release {mb_release_id} has no release_group_id"
            ),
        }, status=422)
        return

    db.update_request_fields(request_id, mb_release_group_id=rg_id)
    h._json({
        "request_id": request_id,
        "mb_release_group_id": rg_id,
        "status": "resolved",
    })


class PipelineReplaceRequest(BaseModel):
    target_mb_release_id: str = Field(min_length=1)


def post_pipeline_replace(h, body: dict, req_id_str: str) -> None:
    """``POST /api/pipeline/<id>/replace``.

    Supersede the source request with a new row at ``target_mb_release_id``.
    Counterpart of ``pipeline-cli replace``. Both surfaces wrap
    ``MbidReplaceService.replace_request_mbid`` — keep them in sync (see
    ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Body: ``{"target_mb_release_id": "<mbid>"}``.

    Status-code mapping mirrors the CLI exit codes:
      * 200 — ``RESULT_REPLACED``
      * 400 — body validation failure (missing/empty target)
      * 404 — ``RESULT_NOT_FOUND``
      * 409 — ``RESULT_WRONG_STATE`` (including supersede race —
              ``descendant_request_id`` populated so the UI can
              deep-link the operator to the new request) or
              ``RESULT_TARGET_COLLISION_REQUEST``
      * 422 — ``RESULT_TARGET_INVALID``, ``RESULT_TARGET_RELEASE_GROUP_MISMATCH``,
              ``RESULT_TARGET_SAME_AS_CURRENT``
      * 503 — ``RESULT_TRANSIENT`` (MB-mirror unreachable etc.)
    """
    from lib.config import read_runtime_config
    from lib.mbid_replace_service import (
        MbidReplaceService,
        RESULT_NOT_FOUND,
        RESULT_REPLACED,
        RESULT_TARGET_COLLISION_REQUEST,
        RESULT_TARGET_INVALID,
        RESULT_TARGET_RELEASE_GROUP_MISMATCH,
        RESULT_TARGET_SAME_AS_CURRENT,
        RESULT_TRANSIENT,
        RESULT_WRONG_STATE,
    )

    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return

    req_body = parse_body(h, body, PipelineReplaceRequest)
    if req_body is None:
        return
    target = req_body.target_mb_release_id.strip()
    if not target:
        h._json({
            "error": "target_mb_release_id must be a non-empty string",
        }, status=400)
        return

    db = _server()._db()
    cfg = read_runtime_config()
    svc = MbidReplaceService(db=db, config=cfg)
    result = svc.replace_request_mbid(
        request_id, target_mb_release_id=target,
    )

    payload: dict[str, object] = {
        "outcome": result.outcome,
        "request_id": result.request_id,
        "new_request_id": result.new_request_id,
        "current_status": result.current_status,
        "descendant_request_id": result.descendant_request_id,
        "error_message": result.error_message,
        "warnings": list(result.warnings),
    }
    if result.outcome == RESULT_REPLACED:
        h._json(payload)
        return
    if result.outcome == RESULT_NOT_FOUND:
        payload["error"] = result.error_message or "Not found"
        h._json(payload, status=404)
        return
    if result.outcome in (
        RESULT_WRONG_STATE,
        RESULT_TARGET_COLLISION_REQUEST,
    ):
        payload["error"] = result.error_message or "Wrong state"
        h._json(payload, status=409)
        return
    if result.outcome in (
        RESULT_TARGET_INVALID,
        RESULT_TARGET_RELEASE_GROUP_MISMATCH,
        RESULT_TARGET_SAME_AS_CURRENT,
    ):
        payload["error"] = result.error_message or "Semantic violation"
        h._json(payload, status=422)
        return
    if result.outcome == RESULT_TRANSIENT:
        payload["error"] = result.error_message or "Transient; retry"
        h._json(payload, status=503)
        return
    h._error(f"Unknown replace outcome: {result.outcome}", 500)


def get_pipeline_requests_by_rg(h, params: dict, rg_id: str) -> None:
    """``GET /api/pipeline/requests-by-rg/<rg_id>``.

    Returns the non-replaced ``album_requests`` rows sharing the given
    release group, in id-descending order. Used by the Browse-search
    inverted-click picker (R7) to ask the operator which existing
    request should be replaced.
    """
    db = _server()._db()
    rows = db.list_requests_in_release_group(rg_id, exclude_replaced=True)
    requests = [
        {
            "id": int(r["id"]),
            "mb_release_id": r.get("mb_release_id"),
            "mb_release_group_id": r.get("mb_release_group_id"),
            "status": r.get("status"),
            "artist_name": r.get("artist_name"),
            "album_title": r.get("album_title"),
        }
        for r in rows
    ]
    h._json({"requests": requests})


def get_pipeline_active_rgs(h, params: dict) -> None:
    """``GET /api/pipeline/active-rgs``.

    Returns the distinct set of ``mb_release_group_id`` values held by
    any non-replaced ``album_requests`` row. The frontend builds a Set
    from this list and uses ``set.has(row.release_group_id)`` per
    Browse-search row to compute the Replace button enable state.
    """
    db = _server()._db()
    ids = sorted(db.list_active_release_group_ids())
    h._json({"release_group_ids": ids})


def _serialize_import_job(job) -> dict[str, object]:
    if hasattr(job, "to_json_dict"):
        return job.to_json_dict()
    return dict(job)


def get_import_jobs(h, params: dict[str, list[str]]) -> None:
    status = params.get("status", [None])[0]
    request_id_raw = params.get("request_id", [None])[0]
    if status not in (None, "", "queued", "running", "completed", "failed"):
        h._error("Invalid import job status")
        return
    status = status or None
    try:
        request_id = int(request_id_raw) if request_id_raw else None
    except ValueError:
        h._error("Invalid request_id")
        return
    jobs = _server()._db().list_import_jobs(
        status=status,
        request_id=request_id,
        limit=50,
    )
    h._json({
        "jobs": [_serialize_import_job(job) for job in jobs],
        "counts": _server()._db().count_import_jobs_by_status(),
    })


def get_import_jobs_timeline(h, params: dict[str, list[str]]) -> None:
    db = _server()._db()
    jobs = db.list_import_job_timeline(limit=50)
    serialized = []
    for job in jobs:
        item = _serialize_import_job(job)
        request_id = item.get("request_id")
        if isinstance(request_id, (int, str)) and not isinstance(request_id, bool):
            req = db.get_request(int(request_id))
            if req:
                item["artist_name"] = req.get("artist_name")
                item["album_title"] = req.get("album_title")
                item["mb_release_id"] = req.get("mb_release_id")
        serialized.append(item)
    h._json({
        "jobs": serialized,
        "counts": db.count_import_jobs_by_status(),
    })


def get_import_job(h, params: dict[str, list[str]], job_id_str: str) -> None:
    job = _server()._db().get_import_job(int(job_id_str))
    if job is None:
        h._error("Import job not found", 404)
        return
    h._json({"job": _serialize_import_job(job)})


# ── POST handlers ────────────────────────────────────────────────


class PipelineAddRequest(BaseModel):
    """HTTP body for ``POST /api/pipeline/add``.

    At least one of ``mb_release_id`` / ``discogs_release_id`` is required;
    the @model_validator below enforces that. Both IDs are normalised
    after parsing (downcase, strip) by ``normalize_release_id`` inside
    the handler — keeping normalisation in route logic, not in the model,
    matches how other handlers consume these fields.
    """

    mb_release_id: str | None = None
    discogs_release_id: str | None = None
    source: str = "request"

    @model_validator(mode="after")
    def _at_least_one_release_id(self) -> "PipelineAddRequest":
        if not self.mb_release_id and not self.discogs_release_id:
            raise ValueError("Missing mb_release_id or discogs_release_id")
        return self


def post_pipeline_add(h, body: dict) -> None:
    req = parse_body(h, body, PipelineAddRequest)
    if req is None:
        return
    s = _server()
    mbid = normalize_release_id(req.mb_release_id)
    discogs_id = normalize_release_id(req.discogs_release_id)
    source = req.source

    if discogs_id:
        # Discogs flow: store discogs ID in both columns for pipeline compat
        existing = s._db().get_request_by_release_id(discogs_id)
        if existing:
            payload: dict[str, object] = {
                "status": "exists",
                "id": existing["id"],
                "current_status": existing["status"],
            }
            if existing["status"] == "replaced":
                descendant = s._db().get_request_by_replaces_request_id(
                    existing["id"])
                if descendant is not None:
                    payload["descendant_request_id"] = descendant["id"]
                    payload["descendant_status"] = descendant.get("status")
            h._json(payload)
            return

        # Bypass the 24h meta cache — this write path persists artist /
        # title / tracks into `album_requests`. A stale cached snapshot
        # would silently bake yesterday's pre-correction metadata into
        # the pipeline DB (Codex review, issue #101).
        release = discogs_api.get_release(int(discogs_id), fresh=True)

        req_id = s._db().add_request(
            mb_release_id=discogs_id,
            discogs_release_id=discogs_id,
            mb_artist_id=str(release.get("artist_id") or ""),
            artist_name=release["artist_name"],
            album_title=release["title"],
            year=release.get("year"),
            country=release.get("country"),
            source=source,
        )

        if release.get("tracks"):
            s._db().set_tracks(req_id, release["tracks"])

        _generate_plan_after_add(
            req_id,
            artist_name=release["artist_name"],
            album_title=release["title"],
            year=release.get("year"),
            tracks=release.get("tracks") or [],
            source=source,
        )

        h._json({
            "status": "added",
            "id": req_id,
            "artist": release["artist_name"],
            "album": release["title"],
            "tracks": len(release.get("tracks", [])),
        })
        return

    # MusicBrainz flow (unchanged)
    existing = s._db().get_request_by_release_id(mbid)
    if existing:
        payload: dict[str, object] = {
            "status": "exists",
            "id": existing["id"],
            "current_status": existing["status"],
        }
        # R33 / U10: when the existing row is a frozen audit row from a
        # past Replace, surface the descendant id so the UI can render a
        # "previously abandoned — active request is at /pipeline/<id>"
        # forward-link instead of the generic "already in pipeline"
        # message.
        if existing["status"] == "replaced":
            descendant = s._db().get_request_by_replaces_request_id(
                existing["id"])
            if descendant is not None:
                payload["descendant_request_id"] = descendant["id"]
                payload["descendant_status"] = descendant.get("status")
        h._json(payload)
        return

    # Bypass the 24h meta cache — same reason as the Discogs branch
    # above. Writing stale metadata into the pipeline DB is worse than
    # an extra MB mirror round trip on add.
    release = mb_api.get_release(mbid, fresh=True)

    # Persist release-group year alongside release year so the generator
    # can emit a year-anchored slot matching how users on Soulseek file
    # reissues. ``get_release_group_year`` returns None on 404 /
    # unparseable date; column accepts NULL.
    rg_id = release.get("release_group_id")
    rg_year = mb_api.get_release_group_year(rg_id) if rg_id else None

    req_id = s._db().add_request(
        mb_release_id=mbid,
        mb_release_group_id=rg_id,
        mb_artist_id=release.get("artist_id"),
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        release_group_year=rg_year,
        country=release.get("country"),
        source=source,
    )

    if release.get("tracks"):
        s._db().set_tracks(req_id, release["tracks"])

    _generate_plan_after_add(
        req_id,
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        tracks=release.get("tracks") or [],
        source=source,
        release_group_year=rg_year,
    )

    h._json({
        "status": "added",
        "id": req_id,
        "artist": release["artist_name"],
        "album": release["title"],
        "tracks": len(release.get("tracks", [])),
    })


class PipelineUpdateRequest(BaseModel):
    id: int = Field(gt=0)
    status: Literal["wanted", "imported", "manual"]


def post_pipeline_update(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineUpdateRequest)
    if req_body is None:
        return
    s = _server()
    req_id = req_body.id
    new_status = req_body.status

    req = s._db().get_request(int(req_id))
    if not req:
        h._error("Not found", 404)
        return

    if new_status == "wanted" and req["status"] != "wanted":
        mbid = req.get("mb_release_id")
        quality = None
        min_br = None
        b = s._beets_db()
        if mbid and b:
            if b.album_exists(mbid):
                # Preserve a stricter existing override (e.g. "lossless"
                # set by the quality gate) — reverting status shouldn't
                # re-open tiers the gate intentionally closed.
                quality = resolve_user_requeue_override(
                    req.get("search_filetype_override"))
                min_br = b.get_min_bitrate(mbid)
        wanted_fields: dict[str, object] = {}
        if quality is not None:
            wanted_fields["search_filetype_override"] = quality
        if min_br is not None:
            wanted_fields["min_bitrate"] = min_br
        finalize_request(
            s._db(),
            int(req_id),
            transitions.RequestTransition.to_wanted_fields(
                from_status=req["status"],
                fields=wanted_fields,
            ),
        )
    else:
        finalize_request(
            s._db(),
            int(req_id),
            transitions.RequestTransition.status_only(
                new_status,
                from_status=req["status"],
            ),
        )

    h._json({"status": "ok", "id": req_id, "new_status": new_status})


class PipelineUpgradeRequest(BaseModel):
    """HTTP body for ``POST /api/pipeline/upgrade``."""

    mb_release_id: str = Field(min_length=1)


def post_pipeline_upgrade(h, body: dict) -> None:
    req = parse_body(h, body, PipelineUpgradeRequest)
    if req is None:
        return
    s = _server()
    mbid = normalize_release_id(req.mb_release_id)
    if not mbid:
        # ``normalize_release_id`` strips/lowercases and can return None
        # for whitespace-only inputs that passed the min_length=1 check.
        h._error("Missing mb_release_id")
        return

    source = detect_release_source(mbid)

    min_bitrate = None
    b = s._beets_db()
    if b:
        min_bitrate = b.get_min_bitrate(mbid)

    existing = s._db().get_request_by_release_id(mbid)
    if existing:
        # Preserve a stricter existing override (e.g. "lossless" set by
        # the quality gate after a CBR 320 import) so clicking Upgrade
        # doesn't re-open tiers the gate already closed, which would
        # re-enqueue same-quality MP3 sources that get rejected as
        # downgrades in a loop.
        quality = resolve_user_requeue_override(
            existing.get("search_filetype_override"))
        req_id = existing["id"]
        transition_fields: dict[str, object] = {
            "search_filetype_override": quality,
        }
        if min_bitrate is not None:
            transition_fields["min_bitrate"] = min_bitrate
        finalize_request(
            s._db(),
            req_id,
            transitions.RequestTransition.to_wanted_fields(
                from_status=existing["status"],
                fields=transition_fields,
            ),
        )
        h._json({
            "status": "upgrade_queued",
            "id": req_id,
            "min_bitrate": min_bitrate,
            "search_filetype_override": quality,
        })
    else:
        # Brand-new request — no prior override to preserve.
        quality = QUALITY_UPGRADE_TIERS
        # Discogs upgrade leaves release_group_year NULL (no MB release-group).
        rg_year_upgrade: int | None = None
        # Bypass the 24h meta cache — both branches persist metadata
        # into the pipeline DB (artist / title / tracks). Stale cache
        # reads would silently bake pre-correction data from an earlier
        # browse. Cheap extra mirror hit on a write path.
        if source == "discogs":
            release = discogs_api.get_release(int(mbid), fresh=True)
            req_id = s._db().add_request(
                mb_release_id=mbid,
                discogs_release_id=mbid,
                mb_artist_id=str(release.get("artist_id") or ""),
                artist_name=release["artist_name"],
                album_title=release["title"],
                year=release.get("year"),
                country=release.get("country"),
                source="request",
            )
        else:
            release = mb_api.get_release(mbid, fresh=True)
            rg_id_upgrade = release.get("release_group_id")
            rg_year_upgrade = (
                mb_api.get_release_group_year(rg_id_upgrade)
                if rg_id_upgrade else None
            )
            req_id = s._db().add_request(
                mb_release_id=mbid,
                mb_release_group_id=rg_id_upgrade,
                mb_artist_id=release.get("artist_id"),
                artist_name=release["artist_name"],
                album_title=release["title"],
                year=release.get("year"),
                release_group_year=rg_year_upgrade,
                country=release.get("country"),
                source="request",
            )
        if release.get("tracks"):
            s._db().set_tracks(req_id, release["tracks"])
        _generate_plan_after_add(
            req_id,
            artist_name=release["artist_name"],
            album_title=release["title"],
            year=release.get("year"),
            tracks=release.get("tracks") or [],
            source="request",
            release_group_year=rg_year_upgrade,
        )
        # Newly added request — status is already 'wanted', set quality override
        finalize_request(
            s._db(),
            req_id,
            transitions.RequestTransition.to_wanted(
                from_status="wanted",
                search_filetype_override=quality,
                min_bitrate=min_bitrate,
            ),
        )
        h._json({
            "status": "upgrade_queued",
            "id": req_id,
            "min_bitrate": min_bitrate,
            "search_filetype_override": quality,
            "created": True,
        })


class PipelineSetQualityRequest(BaseModel):
    mb_release_id: str = Field(min_length=1)
    status: Literal["", "wanted", "imported", "manual"] = ""
    min_bitrate: int | None = None


def post_pipeline_set_quality(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineSetQualityRequest)
    if req_body is None:
        return
    s = _server()
    mbid = normalize_release_id(req_body.mb_release_id)
    new_status = req_body.status
    min_bitrate = req_body.min_bitrate

    if not mbid:
        h._error("Missing mb_release_id")
        return

    existing = s._db().get_request_by_release_id(mbid)
    if not existing:
        h._error("Not found in pipeline", 404)
        return

    req_id = existing["id"]

    if min_bitrate is not None:
        min_bitrate = int(min_bitrate)
        s._db().update_request_fields(req_id, min_bitrate=min_bitrate)

    if new_status:
        if new_status not in ("wanted", "imported", "manual"):
            h._error(f"Invalid status: {new_status}")
            return
        if new_status == "imported":
            if min_bitrate is None and mbid:
                b = s._beets_db()
                if b:
                    min_bitrate = b.get_avg_bitrate_kbps(mbid)
            imported_fields: dict[str, object] = {
                "search_filetype_override": None,
            }
            if min_bitrate is not None:
                imported_fields["min_bitrate"] = int(min_bitrate)
            finalize_request(
                s._db(),
                req_id,
                transitions.RequestTransition.to_imported_fields(
                    from_status=existing["status"],
                    fields=imported_fields,
                ),
            )
        elif new_status == "wanted" and existing["status"] != "wanted":
            finalize_request(
                s._db(),
                req_id,
                transitions.RequestTransition.to_wanted(
                    from_status=existing["status"]),
            )
        else:
            finalize_request(
                s._db(),
                req_id,
                transitions.RequestTransition.status_only(
                    new_status,
                    from_status=existing["status"],
                ),
            )

    h._json({
        "status": "ok",
        "id": req_id,
        "new_status": new_status or existing["status"],
        "min_bitrate": min_bitrate,
    })


class PipelineSetIntentRequest(BaseModel):
    """HTTP body for ``POST /api/pipeline/set-intent``.

    ``intent`` aliases (``flac``/``flac_only`` → ``lossless``,
    ``best_effort``/``upgrade`` → ``default``) are normalised inside the
    handler, not the model — the model accepts any string and the
    handler validates against the canonical set after the alias swap.
    """

    id: int = Field(gt=0)
    intent: str = ""


def post_pipeline_set_intent(h, body: dict) -> None:
    """Toggle lossless-on-disk intent for a pipeline request.

    Accepts intent: "lossless" (keep lossless on disk) or "default" (pipeline decides).
    Backward compat: "flac", "flac_only" → "lossless"; "best_effort" → "default".
    """
    req_body = parse_body(h, body, PipelineSetIntentRequest)
    if req_body is None:
        return
    s = _server()
    req_id = req_body.id
    intent_str = req_body.intent.strip()

    # Normalize to toggle: lossless or default
    _ALIASES = {"flac": "lossless", "flac_only": "lossless",
                "best_effort": "default", "upgrade": "default"}
    intent_str = _ALIASES.get(intent_str, intent_str)
    if intent_str not in ("lossless", "default"):
        h._error(f"Invalid intent: {intent_str!r}. Valid: lossless, default")
        return

    target_format = QUALITY_LOSSLESS if intent_str == "lossless" else None

    req = s._db().get_request(int(req_id))
    if not req:
        h._error("Not found", 404)
        return

    if req["status"] == "downloading":
        h._error("Cannot set intent while album is downloading")
        return

    if req["status"] == "imported" and target_format:
        # Re-queue to search for lossless source
        min_br = req.get("min_bitrate")
        finalize_request(
            s._db(),
            int(req_id),
            transitions.RequestTransition.to_wanted(
                from_status="imported",
                search_filetype_override=QUALITY_LOSSLESS,
                min_bitrate=min_br,
            ),
        )
        s._db().update_request_fields(int(req_id), target_format=target_format)
        h._json({
            "status": "ok",
            "id": int(req_id),
            "intent": intent_str,
            "target_format": target_format,
            "requeued": True,
        })
    else:
        # Just update the persistent intent for next search (wanted or manual)
        update_fields = {"target_format": target_format}
        if should_clear_lossless_search_override(
            new_target_format=target_format,
            old_target_format=req.get("target_format"),
            search_filetype_override=req.get("search_filetype_override"),
        ):
            update_fields["search_filetype_override"] = None
        s._db().update_request_fields(int(req_id), **update_fields)
        h._json({
            "status": "ok",
            "id": int(req_id),
            "intent": intent_str,
            "target_format": target_format,
            "requeued": False,
        })


class PipelineBanSourceRequest(BaseModel):
    request_id: int = Field(gt=0)
    mb_release_id: str = Field(min_length=1)
    username: str | None = None


def post_pipeline_ban_source(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineBanSourceRequest)
    if req_body is None:
        return
    s = _server()
    req_id = req_body.request_id
    username_in = req_body.username.strip() if req_body.username else ""
    mb_release_id = normalize_release_id(req_body.mb_release_id)

    if not mb_release_id:
        # ``normalize_release_id`` can strip whitespace down to None
        # even when the min_length=1 raw input passed Pydantic.
        h._error("Missing mb_release_id")
        return

    db = s._db()
    request_id_int = int(req_id)

    # E1.3: race-check — never run the bad-rip flow against a release
    # the importer is actively touching. The importer's beets-mutating
    # window can overlap with the file paths we're about to hash and
    # ``beet remove`` here; bail with 409 so the curator retries.
    active_job = db.get_active_import_job_for_request(request_id_int)
    if active_job is not None:
        h._json(
            {"error": "importer_busy", "retry_after_seconds": 30},
            status=409,
        )
        return

    # Resolve `reported_username` server-side (R3). Body still accepts
    # an explicit `username` for back-compat with non-UI callers; the
    # web UI no longer sends it. If neither side resolves a user, the
    # ban still proceeds (E1.1) — hashes are recorded with NULL and
    # no denylist row is written.
    reported_username: str | None = (
        username_in if username_in
        else db.get_recent_successful_uploader(request_id_int)
    )

    reason = "manually banned via web UI"

    # Hash capture MUST happen before ``remove_and_reset_release``
    # because that call deletes the underlying audio files. R6: a
    # per-track hash failure must not block the ban — accumulate
    # those failures and surface them in ``partial_failures``.
    hash_capture_errors: list[dict[str, object]] = []
    hashes: list[BadAudioHashInput] = []
    b = s._beets_db()
    if b:
        item_paths = b.get_item_paths(mb_release_id)
    else:
        item_paths = []

    if not item_paths:
        # E1.2: album not in beets (or release id mismatch). Don't
        # 404 — the user clicked "bad rip", they want the album gone
        # regardless. Surface as a hash_capture_error so the toast
        # explains why no hashes were recorded.
        hash_capture_errors.append({
            "track_path": None,
            "reason": "no_tracks_in_beets",
        })
    else:
        for _item_id, raw_path in item_paths:
            track_path = Path(raw_path)
            fmt = track_path.suffix.lstrip(".").lower()
            try:
                digest = hash_audio_content(track_path, fmt)
            except AudioHashError as exc:
                hash_capture_errors.append({
                    "track_path": str(track_path),
                    "reason": str(exc),
                })
                continue
            except Exception as exc:  # pragma: no cover — defensive
                hash_capture_errors.append({
                    "track_path": str(track_path),
                    "reason": f"unexpected error: {exc}",
                })
                continue
            hashes.append(BadAudioHashInput(
                hash_value=digest,
                audio_format=fmt,
            ))

    # Insert hashes BEFORE the denylist + remove so a downstream
    # failure (e.g. denylist DB error) still leaves the bad-byte
    # ripple-stop in place. ``add_bad_audio_hashes`` handles
    # ON CONFLICT (hash, format) DO NOTHING — re-clicks are no-ops.
    hashes_recorded = 0
    if hashes:
        hashes_recorded = db.add_bad_audio_hashes(
            request_id_int,
            reported_username,
            reason,
            hashes,
        )

    # Denylist only when we resolved a user (E1.1). When the route
    # was driven without a uploader-on-record, the bytes are still
    # protected via ``bad_audio_hashes``; nothing useful to denylist.
    if reported_username:
        db.add_denylist(request_id_int, reported_username, reason)

    # Atomic pair (issue #121): if the album is in beets, run
    # ``beet remove -d`` across every selector the release ID could
    # live under (UUID → ``mb_albumid`` only; Discogs numeric →
    # ``discogs_albumid`` AND ``mb_albumid`` so both new-layout and
    # legacy imports are covered). Once beets no longer holds it
    # (whether this handler just removed it or a prior ``beet rm``
    # did), clear the pipeline DB's on-disk quality fields in the
    # same call so nothing downstream reasons about ghost state.
    #
    # Issue #123 PR B: ``remove_and_reset_release`` now returns a
    # typed result. ``selector_failures`` surfaces per-selector
    # problems (timeout, non-zero rc, exception) so the ban-source
    # handler can tell a user the ban succeeded but the remove was
    # incomplete, rather than silently reporting success after a
    # denylist-committed / album-still-on-disk split brain.
    beets_removed = False
    cleanup_errors: list[dict[str, object]] = []
    if mb_release_id and b:
        cleanup = remove_and_reset_release(
            beets_db=b,
            pipeline_db=db,
            release_id=mb_release_id,
            request_id=request_id_int,
        )
        beets_removed = cleanup.beets_removed
        # ``msgspec.to_builtins`` so future fields on ``SelectorFailure``
        # (e.g. a timestamp) propagate to the route response without
        # anyone having to remember to update the literal here (issue
        # #123 PR B review feedback; ``SelectorFailure`` is a
        # ``msgspec.Struct`` post-issue #141).
        cleanup_errors = [msgspec.to_builtins(f)
                          for f in cleanup.selector_failures]

    req = db.get_request(request_id_int)
    if req:
        quality = resolve_user_requeue_override(
            req.get("search_filetype_override"))
        min_br = req.get("min_bitrate")
        ban_fields: dict[str, object] = {
            "search_filetype_override": quality,
        }
        if min_br is not None:
            ban_fields["min_bitrate"] = min_br
        finalize_request(
            db,
            request_id_int,
            transitions.RequestTransition.to_wanted_fields(
                from_status=req["status"],
                fields=ban_fields,
            ),
        )

    partial_failures: dict[str, list[dict[str, object]]] = {}
    if cleanup_errors:
        partial_failures["cleanup_errors"] = cleanup_errors
    if hash_capture_errors:
        partial_failures["hash_capture_errors"] = hash_capture_errors

    # Record the ban as a download_log event (#188 follow-up). It's just
    # another event — surfacing it through the same audit channel makes
    # it appear uniformly on recents, the pipeline tab's "last:" verdict
    # line, and per-row download history without per-surface plumbing.
    ban_detail = (
        f"Marked bad rip; {hashes_recorded} hashes captured"
        if hashes_recorded > 0
        else "Marked bad rip (no tracks hashed)"
    )
    ban_validation = json.dumps({
        "scenario": "curator_ban",
        "hashes_recorded": hashes_recorded,
        "denylisted_username": reported_username,
        "reason": reason,
        "cleanup_errors": cleanup_errors,
        "hash_capture_errors": hash_capture_errors,
    })
    db.log_download(
        request_id=request_id_int,
        soulseek_username=reported_username,
        outcome="curator_ban",
        beets_detail=ban_detail,
        validation_result=ban_validation,
    )

    payload: dict[str, object] = {
        "status": "ok",
        "username": reported_username,
        "beets_removed": beets_removed,
        "hashes_recorded": hashes_recorded,
    }
    if partial_failures:
        payload["partial_failures"] = partial_failures

    h._json(payload)


class PipelineForceImportRequest(BaseModel):
    download_log_id: int = Field(gt=0)


def post_pipeline_force_import(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineForceImportRequest)
    if req_body is None:
        return
    s = _server()
    log_id = req_body.download_log_id

    entry = s._db().get_download_log_entry(int(log_id))
    if not entry:
        h._error(f"Download log entry {log_id} not found", 404)
        return

    request_id = entry["request_id"]

    vr_raw = entry.get("validation_result")
    if not vr_raw:
        h._error("No validation_result on this download log entry")
        return
    vr = vr_raw if isinstance(vr_raw, dict) else json.loads(vr_raw)
    failed_path = vr.get("failed_path")
    if not failed_path:
        h._error("No failed_path in validation_result")
        return

    req = s._db().get_request(request_id)
    if not req:
        h._error(f"Album request {request_id} not found", 404)
        return

    resolved_path = resolve_failed_path(str(failed_path))
    if resolved_path is None:
        h._error(f"Files not found at: {failed_path}")
        return

    job = s._db().enqueue_import_job(
        IMPORT_JOB_FORCE,
        request_id=request_id,
        dedupe_key=force_import_dedupe_key(int(log_id)),
        payload=force_import_payload(
            download_log_id=int(log_id),
            failed_path=resolved_path,
            source_username=entry.get("soulseek_username"),
            source_dirs=source_dirs_from_validation_result(vr),
        ),
        message=f"Force import queued for {req['artist_name']} - {req['album_title']}",
    )

    h._json({
        "status": "queued",
        "job_id": job.id,
        "job": _serialize_import_job(job),
        "deduped": bool(getattr(job, "deduped", False)),
        "request_id": request_id,
        "artist": req["artist_name"],
        "album": req["album_title"],
        "message": "Import queued",
    }, status=202)


class PipelineDeleteRequest(BaseModel):
    id: int = Field(gt=0)


def post_pipeline_delete(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineDeleteRequest)
    if req_body is None:
        return
    s = _server()
    req_id = req_body.id
    db = s._db()
    req = db.get_request(int(req_id))
    if not req:
        h._error("Not found", 404)
        return
    # ``album_requests.replaces_request_id`` uses ON DELETE RESTRICT
    # (migration 023) so a descendant Replace blocks deletion of the
    # frozen ancestor. Surface 409 with the descendant chain rather
    # than letting psycopg2 raise a 500 from the FK violation.
    descendant = db.get_request_by_replaces_request_id(int(req_id))
    if descendant is not None:
        descendant_ids: list[int] = []
        cursor: dict | None = descendant
        while cursor is not None:
            descendant_ids.append(int(cursor["id"]))
            cursor = db.get_request_by_replaces_request_id(int(cursor["id"]))
        h._json({
            "error": (
                f"request {req_id} is referenced by a superseding "
                "request — delete descendants first"
            ),
            "descendant_request_ids": descendant_ids,
        }, status=409)
        return
    import psycopg2.errors
    try:
        db.delete_request(int(req_id))
    except psycopg2.errors.ForeignKeyViolation as exc:
        # Defensive — a descendant landed between the read above and
        # the delete. Re-walk the chain so the operator gets the same
        # 409 response shape.
        descendant_ids = []
        descendant = db.get_request_by_replaces_request_id(int(req_id))
        cursor = descendant
        while cursor is not None:
            descendant_ids.append(int(cursor["id"]))
            cursor = db.get_request_by_replaces_request_id(int(cursor["id"]))
        h._json({
            "error": (
                f"request {req_id} is referenced by a superseding "
                f"request — delete descendants first ({exc})"
            ),
            "descendant_request_ids": descendant_ids,
        }, status=409)
        return
    h._json({"status": "ok", "id": req_id})


# ── Route tables ─────────────────────────────────────────────────

class _RedisFingerprintCache:
    """Adapt ``web/cache.py``'s Redis client to the ``BeetsDistanceCache`` protocol.

    Our fingerprints are msgspec-encoded bytes, while ``web/cache.py``
    targets JSON-serialisable dicts/lists — so we bypass the JSON
    wrapping and talk to the Redis client directly. Falls back to a
    no-op cache when Redis is unavailable so single-call dev shells
    still work (just without the cached fast-path).
    """

    def __init__(self) -> None:
        from web import cache as _cache_mod
        self._redis = getattr(_cache_mod, "_redis", None)

    def get(self, key: str):
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(key)  # type: ignore[union-attr]
        except Exception:
            return None
        if raw is None:
            return None
        # web/cache.py initialises Redis with ``decode_responses=True``,
        # so ``get`` returns str. msgspec.json.decode handles bytes;
        # encoding is cheap.
        if isinstance(raw, str):
            return raw.encode("utf-8")
        return raw

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        if self._redis is None:
            return
        try:
            self._redis.setex(key, ttl_seconds, value)  # type: ignore[union-attr]
        except Exception:
            pass


_BEETS_DISTANCE_OUTCOME_STATUS: dict[str, int] = {
    "ok": 200,
    "download_log_not_found": 404,
    "request_not_found": 404,
    "folder_missing": 410,
    "no_audio": 410,
    "mb_lookup_failed": 503,
    "mb_no_release_group": 422,
    "wrong_release_group": 422,
    "distance_failed": 500,
}


def get_beets_distance(
    h, params: dict[str, list[str]],
    download_log_id_str: str, mbid: str,
) -> None:
    """``GET /api/beets-distance/<download_log_id>/<mbid>``.

    Real beets match distance for one ``(download_log_id, mbid)``
    pair. The service does the heavy lifting (see
    ``lib/beets_distance.compute_beets_distance``); this handler is a
    thin adapter that maps the typed ``BeetsDistanceResult`` outcomes
    to HTTP status codes per the CLI ⇄ API symmetry rule.

    Status-code mapping:
      * 200 — ``ok`` (distance is in ``response.distance``)
      * 404 — ``download_log_not_found`` / ``request_not_found``
      * 410 — ``folder_missing`` / ``no_audio`` (the data the caller
              wanted to compare against is gone)
      * 422 — ``mb_no_release_group`` / ``wrong_release_group``
              (semantic input violations — including the
              cross-release-group guardrail)
      * 503 — ``mb_lookup_failed`` (MB mirror transient)
      * 500 — ``distance_failed`` (unexpected beets error)
    """
    from lib.beets_distance import compute_beets_distance

    try:
        download_log_id = int(download_log_id_str)
    except (TypeError, ValueError):
        h._error("Invalid download_log_id")
        return

    s = _server()
    result = compute_beets_distance(
        download_log_id,
        mbid,
        pdb=s._db(),
        mb_get_release=lambda m: mb_api.get_release(m, fresh=False),
        cache=_RedisFingerprintCache(),
    )

    status = _BEETS_DISTANCE_OUTCOME_STATUS.get(result.outcome, 500)
    payload = msgspec.to_builtins(result)
    h._json(payload, status=status)


GET_ROUTES: dict[str, object] = {
    "/api/pipeline/log": get_pipeline_log,
    "/api/pipeline/status": get_pipeline_status,
    "/api/pipeline/recent": get_pipeline_recent,
    "/api/pipeline/all": get_pipeline_all,
    "/api/pipeline/downloading": get_pipeline_downloading,
    "/api/pipeline/dashboard": get_pipeline_dashboard,
    "/api/pipeline/constants": get_pipeline_constants,
    "/api/pipeline/simulate": get_pipeline_simulate,
    "/api/import-jobs": get_import_jobs,
    "/api/import-jobs/timeline": get_import_jobs_timeline,
    "/api/pipeline/active-rgs": get_pipeline_active_rgs,
}

GET_PATTERNS: list[tuple[re.Pattern[str], object]] = [
    # /api/beets-distance/<download_log_id>/<mbid> — real beets distance
    # for one (download_log_id, mbid) pair. See get_beets_distance above.
    (re.compile(r"^/api/beets-distance/(\d+)/([a-f0-9-]{36})$"),
     get_beets_distance),
    (re.compile(r"^/api/pipeline/(\d+)$"), get_pipeline_detail),
    (re.compile(r"^/api/pipeline/(\d+)/search-plan$"),
     get_pipeline_search_plan),
    (re.compile(r"^/api/pipeline/(\d+)/search-plan/dry-run$"),
     get_pipeline_search_plan_dry_run),
    (re.compile(r"^/api/pipeline/(\d+)/search-plan/saturation$"),
     get_pipeline_search_plan_saturation),
    (re.compile(r"^/api/pipeline/(\d+)/search-plan/history$"),
     get_pipeline_search_plan_history),
    (re.compile(r"^/api/pipeline/requests-by-rg/([a-f0-9-]{36})$"),
     get_pipeline_requests_by_rg),
    (re.compile(r"^/api/import-jobs/(\d+)$"), get_import_job),
]

POST_ROUTES: dict[str, object] = {
    "/api/pipeline/add": post_pipeline_add,
    "/api/pipeline/update": post_pipeline_update,
    "/api/pipeline/upgrade": post_pipeline_upgrade,
    "/api/pipeline/set-quality": post_pipeline_set_quality,
    "/api/pipeline/set-intent": post_pipeline_set_intent,
    "/api/pipeline/ban-source": post_pipeline_ban_source,
    "/api/pipeline/force-import": post_pipeline_force_import,
    "/api/pipeline/delete": post_pipeline_delete,
}

POST_PATTERNS: list[tuple[re.Pattern[str], object]] = [
    (re.compile(r"^/api/pipeline/(\d+)/search-plan/regenerate$"),
     post_pipeline_search_plan_regenerate),
    (re.compile(r"^/api/pipeline/(\d+)/search-plan/advance$"),
     post_pipeline_search_plan_advance),
    (re.compile(r"^/api/pipeline/(\d+)/replace$"),
     post_pipeline_replace),
    (re.compile(r"^/api/pipeline/(\d+)/resolve-rg$"),
     post_pipeline_resolve_rg),
]
