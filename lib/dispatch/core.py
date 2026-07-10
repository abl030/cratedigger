"""Core import dispatch — the import_one.py orchestration state machine.

``dispatch_import_core`` is the funnel every import path (auto / force /
manual) runs through: acquire the RELEASE advisory lock, load evidence,
run the subprocess, and dispatch on the decision. ``finalize_request``,
``cleanup_disambiguation_orphans``, and ``_cleanup_staged_dir`` are looked
up here (tests patch them on this module).
"""

from __future__ import annotations

import logging
import subprocess as sp
from datetime import datetime, timezone
from typing import Sequence, TYPE_CHECKING

from lib import transitions

# Module-level DI seam for ``transitions.finalize_request``.
finalize_request = transitions.finalize_request

from lib.processing_paths import normalize_source_dirs
from lib.quality import (AlbumQualityEvidenceDecisionFacts, DownloadInfo,
                         ImportResult, QUALITY_UPGRADE_TIERS, ValidationResult,
                         comparison_basis_from_decision,
                         dispatch_action, evidence_decision_name,
                         extract_usernames,
                         full_pipeline_decision_from_evidence,
                         narrow_override_on_downgrade,
                         narrow_override_on_lossless_source_lock,
                         override_bitrate_from_current_evidence,
                         rejection_backfill_override)
from lib.quality_evidence import (audit_v0_probe_from_metric,
                                  legacy_current_lossless_v0_probe_from_request)
from lib.util import cleanup_disambiguation_orphans, trigger_meelo_clean

from lib.dispatch.types import (DispatchOutcome, FORCE_MANUAL_SCENARIOS,
                                QualityGateFn)
from lib.dispatch.subprocess_runner import run_import_one
from lib.dispatch.helpers import (_cleanup_staged_dir, _guard_failure_detail,
                                  _log_postflight_bad_extensions,
                                  _populate_dl_info_from_import_result,
                                  _quarantine_duplicate_remove_guard_source,
                                  _should_cleanup_path)
from lib.dispatch.evidence_gate import (_current_evidence_allows_action,
                                        _import_allowed_by_evidence_pipeline,
                                        _load_evidence_import_gate,
                                        _refresh_current_evidence_after_import,
                                        _remove_quality_evidence_action_file,
                                        _requeue_import_job_to_preview,
                                        _write_album_sidecar_after_import,
                                        _write_quality_evidence_action_file)
from lib.dispatch.outcome_actions import (_do_mark_done,
                                          _record_rejection_and_maybe_requeue,
                                          _reject_import_from_evidence_decision)
from lib.dispatch.quality_gate import _check_quality_gate_core

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.import_evidence import CandidateEvidenceActionResult
    from lib.pipeline_db import DownloadLogOutcome, PipelineDB

logger = logging.getLogger("cratedigger")


def dispatch_import_core(
    *,
    path: str,
    mb_release_id: str,
    request_id: int,
    label: str,
    force: bool = False,
    override_min_bitrate: int | None = None,
    target_format: str | None = None,
    verified_lossless_target: str = "",
    beets_harness_path: str,
    db: "PipelineDB",
    dl_info: DownloadInfo,
    distance: float | None = None,
    scenario: str = "auto_import",
    files: Sequence[object] | None = None,
    cfg: "CratediggerConfig | None" = None,
    outcome_label: DownloadLogOutcome = "success",
    requeue_on_failure: bool = True,
    cooled_down_users: set[str] | None = None,
    source_dirs: list[str] | None = None,
    candidate_import_job_id: int | None = None,
    candidate_download_log_id: int | None = None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
    quality_gate_fn: QualityGateFn = _check_quality_gate_core,
) -> "DispatchOutcome":
    """Core import dispatch — takes plain params + PipelineDB directly.

    Runs import_one.py, parses result, dispatches on decision (mark_done/failed,
    denylist, quality gate, media server notifiers, cleanup). Returns DispatchOutcome.

    Used by the auto-import flow in ``lib.download`` and by
    ``dispatch_import_from_db()`` (force/manual import).
    """
    from lib.util import trigger_meelo_scan as _trigger_meelo
    from lib.util import trigger_plex_scan as _trigger_plex
    from lib.util import trigger_jellyfin_scan as _trigger_jellyfin

    source_dirs = normalize_source_dirs(source_dirs or [])

    mode = (
        "FORCE-IMPORT" if force
        else "MANUAL-IMPORT" if scenario == "manual_import"
        else "AUTO-IMPORT"
    )
    dist_label = f"{distance:.4f}" if distance is not None else "unmeasured"
    logger.info(f"{mode}: {label} "
                f"(source=request, dist={dist_label})")

    outcome_success = False
    outcome_message = ""

    # Acquire the RELEASE (per-MBID) advisory lock for the duration of
    # the ``import_one.py`` subprocess. This is the funnel every path
    # goes through (auto, force, manual), so the lock here closes the
    # cross-process race that could produce Palo Santo-*class* data loss
    # (issues #132 P1 / #133) for every entry point. The actual 04-20
    # Palo Santo incident had a different proximate cause (YAML misconfig —
    # see CLAUDE.md § Resolved canonical RCs); this lock defends against
    # an independent race vector the original fix left open.
    # Auto path: ``_handle_valid_result`` has already acquired RELEASE
    # outer — this acquisition is a session-reentrant no-op. Force/
    # manual path: this is the first RELEASE acquisition, nested inside
    # the IMPORT lock held by ``dispatch_import_from_db``.
    # See ``docs/advisory-locks.md`` for the full rationale, the
    # ordering rules, and the call-site index.
    from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE,
                                 release_id_to_lock_key)
    release_lock_key: int | None
    if mb_release_id:
        release_lock_key = release_id_to_lock_key(mb_release_id)
    else:
        # Defensive: ``dispatch_import_from_db`` already rejects empty
        # mbids before reaching here; the auto-import flow passes
        # ``album_data.mb_release_id or ""``. An empty mbid means
        # there's nothing to serialise across, so skip the lock.
        release_lock_key = None
        logger.warning(
            f"{mode}: mb_release_id is empty; skipping release lock "
            "(no cross-release race to serialise)")

    if release_lock_key is not None:
        lock_ctx = db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE, release_lock_key)
    else:
        # No-op context manager that yields True (treat as "got lock"
        # so the critical section runs). ``contextlib.nullcontext``
        # forwards the enter value unchanged.
        from contextlib import nullcontext
        lock_ctx = nullcontext(True)

    with lock_ctx as got_release_lock:
        if not got_release_lock:
            logger.warning(
                f"{mode} SKIPPED: {label} — release lock held by "
                f"another process (mbid={mb_release_id})")
            # Contention == deferred retry. The entire function now
            # returns ``DispatchOutcome(deferred=True)`` without
            # mutating ANY state:
            #
            # - No status transition (was: reset to 'wanted'). The
            #   auto path's outer ``_run_completed_processing`` now
            #   branches on ``outcome.deferred`` — no flip to
            #   ``imported`` and no reset to ``wanted``; the request
            #   stays ``downloading`` with its ``active_download_state``
            #   intact, so ``poll_active_downloads`` re-enters
            #   ``process_completed_album`` on the next cycle and
            #   retries exactly where we stopped.
            # - No staged-dir cleanup (was: ``_cleanup_staged_dir``).
            #   Codex PR #136 R3 P3: if the competing import later
            #   fails, wiping the staged copy forces a redownload
            #   from Soulseek. Staging is preserved so the retry
            #   resumes with the local files already in place.
            # - No spectral clear. Codex PR #136 R3 P2: the prior
            #   reset-to-wanted left ``current_spectral_*`` populated
            #   from a download that was never imported, skewing the
            #   next cycle's quality-gate decisions. With no reset,
            #   ``measure_preimport_state`` re-runs on retry and
            #   re-populates spectral from the same files.
            #
            # Force/manual paths (scenario in FORCE_MANUAL_SCENARIOS)
            # surface the message to the user via
            # ``dispatch_import_from_db``; no state change needed
            # because the request wasn't ``downloading`` to begin
            # with.
            return DispatchOutcome(
                success=False,
                message=("Another import is already in progress for "
                         f"this release ({mb_release_id})"),
                deferred=True,
            )

        quality_evidence_action_file: str | None = None
        try:
            try:
                request_row = db.get_request(request_id)
            except Exception:
                logger.debug(
                    "Failed to read request row before import",
                    exc_info=True,
                )
                request_row = None

            evidence_gate = _load_evidence_import_gate(
                db,
                request_id=request_id,
                mb_release_id=mb_release_id,
                path=path,
                quality_ranks=cfg.quality_ranks if cfg is not None else None,
                candidate_import_job_id=candidate_import_job_id,
                candidate_download_log_id=candidate_download_log_id,
                prevalidated_candidate_result=prevalidated_candidate_result,
                beets_library_root=getattr(cfg, "beets_directory", "") if cfg is not None else "",
            )
            existing_v0_probe = audit_v0_probe_from_metric(
                evidence_gate.current.v0_metric
                if evidence_gate.current is not None
                else None
            )
            evidence_override = override_bitrate_from_current_evidence(
                evidence_gate.current
            )
            if evidence_override is not None:
                override_min_bitrate = evidence_override
            if existing_v0_probe is None:
                existing_v0_probe = legacy_current_lossless_v0_probe_from_request(
                    request_row)

            if (
                (candidate_import_job_id is not None
                 or candidate_download_log_id is not None)
                and evidence_gate.candidate is None
            ):
                # U4: outer callers (``_dispatch_import_from_db_locked`` and
                # ``lib/download.py::_process_beets_validation``) already
                # call ``ensure_candidate_evidence_for_action`` and requeue
                # via ``_requeue_import_job_to_preview`` when evidence is
                # missing. Reaching this inner site means a caller bypassed
                # the outer gate (test seam or future misuse). Behave
                # consistently with the outer invariant — requeue rather
                # than hard-fail — so the importer never measures and
                # never writes a terminal failure on missing evidence.
                reason = evidence_gate.candidate_reason or evidence_gate.candidate_status
                return _requeue_import_job_to_preview(
                    db,
                    import_job_id=candidate_import_job_id,
                    reason=reason or "missing",
                )
            if evidence_gate.candidate is not None and not _current_evidence_allows_action(
                evidence_gate
            ):
                reason = evidence_gate.current_reason or evidence_gate.current_status
                return DispatchOutcome(
                    success=False,
                    message=(
                        "Current quality evidence unavailable at import "
                        f"time: {reason or 'missing'}"
                    ),
                )

            if evidence_gate.candidate is not None:
                # U11: ``full_pipeline_decision_from_evidence`` is the single
                # decision function. Folder/audio-integrity facts
                # (audio_corrupt / bad_audio_hash / nested_layout /
                # empty_fileset) are early-exit rejects at the top of that
                # function — the unified reject helper below recognises them
                # via ``_PREIMPORT_FACT_REJECT_DECISIONS`` and forces
                # ``requeue=True`` so the parent request self-heals.
                facts = AlbumQualityEvidenceDecisionFacts(
                    import_mode=(
                        "force"
                        if force
                        else "manual"
                        if scenario == "manual_import"
                        else "auto"
                    ),
                    verified_lossless_target=verified_lossless_target or None,
                    target_format=target_format,
                )
                evidence_decision = full_pipeline_decision_from_evidence(
                    evidence_gate.candidate,
                    evidence_gate.current,
                    facts=facts,
                    cfg=cfg.quality_ranks if cfg is not None else None,
                )
                if not _import_allowed_by_evidence_pipeline(evidence_decision):
                    decision = evidence_decision_name(evidence_decision)
                    detail = (
                        "import-time persisted evidence rejected candidate "
                        f"(decision={decision})"
                    )
                    evidence_import_result = ImportResult(
                        decision=decision,
                        new_measurement=evidence_gate.candidate.measurement,
                        existing_measurement=(
                            evidence_gate.current.measurement
                            if evidence_gate.current is not None
                            else None
                        ),
                        v0_probe=audit_v0_probe_from_metric(
                            evidence_gate.candidate.v0_metric
                        ),
                        existing_v0_probe=existing_v0_probe,
                        comparison_basis=comparison_basis_from_decision(
                            evidence_decision
                        ),
                    )
                    return _reject_import_from_evidence_decision(
                        db=db,
                        request_id=request_id,
                        dl_info=dl_info,
                        import_result=evidence_import_result,
                        distance=distance,
                        decision=decision,
                        detail=detail,
                        requeue_on_failure=requeue_on_failure,
                        validation_result=dl_info.validation_result,
                        staged_path=path,
                        scenario=scenario,
                        files=files,
                        source_path_cleanup_scenario=scenario,
                        cooled_down_users=cooled_down_users,
                    )
                quality_evidence_action_file = _write_quality_evidence_action_file(
                    candidate=evidence_gate.candidate,
                    current=evidence_gate.current,
                    decision=evidence_decision,
                    target_format=target_format,
                    verified_lossless_target=verified_lossless_target,
                    gate=evidence_gate,
                )
            # Mark the subprocess as launching on the auto-import path
            # so the resume guard can distinguish "never started" from
            # "may have written to beets" if this process crashes
            # before recording the result. The DB-side method is a
            # no-op when ``active_download_state`` is NULL (force /
            # manual paths), so calling unconditionally would also be
            # safe — we still gate to make the intent explicit.
            # See ``docs/advisory-locks.md`` and
            # ``lib/download.py::_import_subprocess_already_started``.
            if scenario not in FORCE_MANUAL_SCENARIOS:
                try:
                    db.mark_import_subprocess_started(
                        request_id,
                        datetime.now(timezone.utc).isoformat(),
                    )
                except Exception:
                    logger.exception(
                        "Failed to stamp import_subprocess_started_at "
                        "for request %s; continuing with subprocess "
                        "launch (resume guard may fail-open until "
                        "completion)",
                        request_id,
                    )
            # Force/manual import operates on the user's only copy of the source
            # material (typically failed_imports/…). Tell the harness to keep
            # lossless originals intact until the quality decision — on
            # downgrade/transcode_downgrade verdicts we exit before deletion so
            # the user's FLACs survive (#111). Auto-import stages to disposable
            # /Incoming and does not need the flag.
            run = run_import_one(
                path=path,
                mb_release_id=mb_release_id,
                request_id=request_id,
                force=force,
                preserve_source=scenario in FORCE_MANUAL_SCENARIOS,
                override_min_bitrate=override_min_bitrate,
                target_format=target_format,
                verified_lossless_target=verified_lossless_target,
                beets_harness_path=beets_harness_path,
                quality_rank_config_json=(
                    cfg.quality_ranks.to_json() if cfg is not None else None
                ),
                existing_v0_probe=existing_v0_probe,
                quality_evidence_action_file=quality_evidence_action_file,
            )
            _remove_quality_evidence_action_file(quality_evidence_action_file)
            quality_evidence_action_file = None
            for line in run.stderr.strip().split("\n"):
                if line.strip():
                    logger.info(f"  [import] {line}")

            ir = run.import_result
            if ir is None:
                logger.error(
                    f"{mode} FAILED (no JSON, rc={run.returncode}): {label}")
                for line in run.stdout.strip().split("\n"):
                    logger.error(f"  {line}")
                _record_rejection_and_maybe_requeue(
                    db, request_id, dl_info,
                    distance=distance,
                    scenario="no_json_result",
                    detail=f"import_one.py rc={run.returncode}, no JSON",
                    error=f"rc={run.returncode}",
                    requeue=requeue_on_failure,
                    outcome_label="failed",
                    validation_result=ValidationResult(
                        distance=distance,
                        scenario="no_json_result",
                        detail=f"import_one.py rc={run.returncode}, no JSON",
                        error=f"rc={run.returncode}",
                        source_dirs=source_dirs,
                    ).to_json(),
                    staged_path=path)
                outcome_message = f"No JSON result (rc={run.returncode})"
            else:
                _populate_dl_info_from_import_result(dl_info, ir)
                _log_postflight_bad_extensions(
                    ir=ir,
                    mode=mode,
                    request_id=request_id,
                    label=label,
                )
                decision = ir.decision or "unknown"
                action = dispatch_action(decision)
                file_list = files or []
                usernames = extract_usernames(file_list) if action.denylist else set()
                if action.denylist and dl_info.username:
                    usernames.add(dl_info.username)
                narrowed_override = None
                current_override = None

                new_br = ir.new_measurement.min_bitrate_kbps if ir.new_measurement else None
                prev_br = ir.existing_measurement.min_bitrate_kbps if ir.existing_measurement else None

                # --- Mark done or failed with decision-specific details ---
                if action.mark_done:
                    logger.info(f"{mode} OK: {label} (decision={decision})")
                    mark_scenario = (
                        decision
                        if decision == "provisional_lossless_upgrade"
                        else scenario
                    )
                    _do_mark_done(
                        db, request_id, dl_info,
                        distance=distance, scenario=mark_scenario,
                        dest_path=path, outcome_label=outcome_label,
                        imported_path=ir.postflight.imported_path,
                        clear_stale_v0_probe=(
                            decision != "preflight_existing"
                        ))
                    try:
                        _refresh_current_evidence_after_import(
                            db,
                            request_id=request_id,
                            mb_release_id=mb_release_id,
                            quality_ranks=(
                                cfg.quality_ranks if cfg is not None else None
                            ),
                            source_candidate=evidence_gate.candidate,
                            import_result=ir,
                            beets_library_root=(
                                cfg.beets_directory if cfg is not None else ""
                            ),
                        )
                    except Exception:
                        logger.exception(
                            "Failed to refresh current quality evidence "
                            "after import for request %s",
                            request_id,
                        )
                    try:
                        _write_album_sidecar_after_import(
                            db,
                            request_id=request_id,
                            mb_release_id=mb_release_id,
                            cfg=cfg,
                        )
                    except Exception:
                        logger.exception(
                            "Failed to write verified-lossless sidecar "
                            "after import for request %s",
                            request_id,
                        )
                    if decision in ("import", "preflight_existing"):
                        if prev_br is not None or new_br is not None:
                            try:
                                finalize_request(
                                    db,
                                    request_id,
                                    transitions.RequestTransition.to_imported(
                                        from_status="imported",
                                        prev_min_bitrate=prev_br,
                                        min_bitrate=new_br,
                                    ),
                                )
                            except Exception:
                                logger.exception("Failed to update upgrade delta")
                    outcome_success = True
                    outcome_message = "Import successful"
                elif action.record_rejection:
                    if decision == "downgrade":
                        fail_scenario = "quality_downgrade"
                        fail_detail: str | None = (f"new {new_br}kbps "
                                                   f"<= existing {prev_br}kbps")
                        logger.warning(f"QUALITY DOWNGRADE PREVENTED: {label}")
                    elif decision == "transcode_downgrade":
                        fail_scenario = "transcode_downgrade"
                        fail_detail = (f"transcode {new_br}kbps "
                                       f"<= existing {prev_br}kbps")
                        logger.warning(f"TRANSCODE REJECTED: {label} "
                                       f"at {new_br}kbps — not an upgrade")
                    elif decision == "suspect_lossless_downgrade":
                        fail_scenario = "suspect_lossless_downgrade"
                        candidate_avg = (
                            ir.v0_probe.avg_bitrate_kbps
                            if ir.v0_probe else None
                        )
                        existing_avg = (
                            ir.existing_v0_probe.avg_bitrate_kbps
                            if ir.existing_v0_probe else None
                        )
                        fail_detail = (
                            f"lossless-source V0 avg {candidate_avg}kbps "
                            f"<= existing source V0 avg {existing_avg}kbps "
                            "within tolerance"
                        )
                        logger.warning(
                            f"SUSPECT LOSSLESS REJECTED: {label} "
                            f"candidate_v0_avg={candidate_avg} "
                            f"existing_v0_avg={existing_avg}")
                    elif decision == "suspect_lossless_probe_missing":
                        fail_scenario = "suspect_lossless_probe_missing"
                        fail_detail = ir.error or (
                            "suspect lossless source lacks comparable V0 probe"
                        )
                        logger.warning(
                            f"SUSPECT LOSSLESS REJECTED: {label} "
                            "missing comparable V0 probe")
                    elif decision == "lossless_source_locked":
                        fail_scenario = "lossless_source_locked"
                        existing_avg = (
                            ir.existing_v0_probe.avg_bitrate_kbps
                            if ir.existing_v0_probe else None
                        )
                        fail_detail = ir.error or (
                            f"lossy candidate cannot override existing "
                            f"lossless-source V0 probe {existing_avg}kbps"
                        )
                        logger.warning(
                            f"LOSSLESS SOURCE LOCKED: {label} "
                            f"existing_v0_avg={existing_avg}kbps")
                    elif decision == "duplicate_remove_guard_failed":
                        fail_scenario = "duplicate_remove_guard_failed"
                        fail_detail = _guard_failure_detail(ir)
                        _quarantine_duplicate_remove_guard_source(
                            ir=ir,
                            path=path,
                            request_id=request_id,
                            cfg=cfg,
                        )
                        dl_info.import_result = ir.to_json()
                        guard = ir.postflight.duplicate_remove_guard
                        if guard is not None:
                            logger.error(
                                "DUPLICATE REMOVE GUARD: request_id=%s "
                                "target=%s:%s duplicates=%s candidates=%s",
                                request_id,
                                guard.target_source or "unknown",
                                guard.target_release_id,
                                guard.duplicate_count,
                                [
                                    {
                                        "beets_album_id": c.beets_album_id,
                                        "mb_albumid": c.mb_albumid,
                                        "discogs_albumid": c.discogs_albumid,
                                        "album_path": c.album_path,
                                        "item_count": c.item_count,
                                    }
                                    for c in guard.candidates
                                ],
                            )
                    else:
                        fail_scenario = decision or "import_error"
                        fail_detail = ir.error
                        logger.error(f"{mode} FAILED: {label} "
                                     f"(decision={decision}, error={ir.error})")
                    fail_error = (
                        ir.error
                        if decision not in (
                            "downgrade",
                            "transcode_downgrade",
                            "suspect_lossless_downgrade",
                            "suspect_lossless_probe_missing",
                            "lossless_source_locked",
                        )
                        else None
                    )

                    if decision == "downgrade":
                        try:
                            req_row = db.get_request(request_id)
                            current_override = req_row.get("search_filetype_override") if req_row else None
                            narrowed_override = narrow_override_on_downgrade(
                                current_override, dl_info)
                            if narrowed_override is None and current_override is None and req_row:
                                from lib.beets_db import BeetsDB
                                from lib.quality import QualityRankConfig
                                _gate_cfg = (
                                    cfg.quality_ranks if cfg is not None
                                    else QualityRankConfig.defaults())
                                with BeetsDB() as beets:
                                    beets_info = beets.get_album_info(
                                        mb_release_id, _gate_cfg)
                                if beets_info:
                                    narrowed_override = rejection_backfill_override(
                                        is_cbr=beets_info.is_cbr,
                                        min_bitrate_kbps=beets_info.min_bitrate_kbps,
                                        spectral_grade=req_row.get(
                                            "current_spectral_grade"),
                                        verified_lossless=bool(
                                            req_row.get("verified_lossless")),
                                        cfg=_gate_cfg,
                                    )
                                    if narrowed_override:
                                        logger.info(
                                            f"BACKFILL: {label} search_filetype_override=NULL"
                                            f" → '{narrowed_override}' on downgrade"
                                            f" ({beets_info.min_bitrate_kbps}kbps,"
                                            f" cbr={beets_info.is_cbr})")
                        except Exception:
                            logger.debug(
                                "Failed to inspect search_filetype_override before downgrade reset")

                    elif decision == "lossless_source_locked":
                        # R7 / AE2: once the library row carries a comparable
                        # lossless-source V0 probe, no lossy candidate can
                        # override it. Narrow the search to lossless-only so
                        # future cycles stop re-finding lossy candidates that
                        # would just hit the lock again. See
                        # docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md
                        try:
                            req_row = db.get_request(request_id)
                            current_override = (
                                req_row.get("search_filetype_override")
                                if req_row else None
                            )
                            narrowed_override = narrow_override_on_lossless_source_lock(
                                current_override)
                        except Exception:
                            logger.debug(
                                "Failed to inspect search_filetype_override"
                                " before lossless_source_locked narrow")

                    _record_rejection_and_maybe_requeue(
                        db, request_id, dl_info,
                        distance=distance,
                        scenario=fail_scenario,
                        detail=fail_detail,
                        error=fail_error,
                        requeue=requeue_on_failure,
                        outcome_label="rejected",
                        search_filetype_override=narrowed_override,
                        validation_result=(dl_info.validation_result
                                           or ValidationResult(
                                               distance=distance,
                                               scenario=fail_scenario,
                                               detail=fail_detail,
                                               error=fail_error,
                                               source_dirs=source_dirs,
                                           ).to_json()),
                        staged_path=path)
                    if narrowed_override is not None:
                        logger.info(
                            f"  Narrowed search_filetype_override '{current_override}'"
                            f" -> '{narrowed_override}' after downgrade")
                    outcome_message = f"Rejected: {fail_scenario} — {fail_detail}"

                # --- Common actions driven by flags ---
                if action.denylist:
                    if decision == "downgrade":
                        reason = "quality downgrade prevented"
                    elif decision == "provisional_lossless_upgrade":
                        reason = "provisional lossless source imported"
                    elif decision.startswith("suspect_lossless"):
                        reason = "suspect lossless source not an upgrade"
                    elif decision.startswith("transcode"):
                        reason = f"transcode: {new_br}kbps" if new_br else "transcode detected"
                    elif decision == "duplicate_remove_guard_failed":
                        reason = "duplicate remove guard failed"
                    else:
                        reason = f"rejected: {decision}"
                    if (decision == "duplicate_remove_guard_failed"
                            and not usernames):
                        logger.error(
                            "DUPLICATE REMOVE GUARD: no source username "
                            "available to denylist for request %s",
                            request_id,
                        )
                    for username in usernames:
                        db.add_denylist(request_id, username, reason)
                        if cooled_down_users is not None:
                            if db.check_and_apply_cooldown(username):
                                cooled_down_users.add(username)
                    logger.info(f"  Denylisted {usernames} for request {request_id}")

                # Rejected auto-imports are already requeued by
                # _record_rejection_and_maybe_requeue(), which preserves retry
                # counters and records the validation attempt. This second
                # requeue is only for successful imports that intentionally go
                # back to wanted to keep searching for a better source.
                if action.requeue and action.mark_done:
                    requeue_fields: dict[str, object] = {
                        "search_filetype_override": QUALITY_UPGRADE_TIERS,
                    }
                    if action.mark_done and new_br is not None:
                        requeue_fields["min_bitrate"] = new_br
                    finalize_request(
                        db,
                        request_id,
                        transitions.RequestTransition.to_wanted_fields(
                            from_status="imported",
                            fields=requeue_fields,
                        ),
                    )

                if action.run_quality_gate:
                    quality_gate_fn(
                        mb_id=mb_release_id,
                        label=label,
                        request_id=request_id,
                        files=list(file_list),
                        db=db,
                        quality_ranks=cfg.quality_ranks if cfg is not None else None,
                    )
                if action.trigger_notifiers and cfg is not None:
                    _trigger_meelo(cfg)
                    # Capture the album's pre-upgrade Plex addedAt BEFORE the
                    # refresh re-stamps it, so the reconciler (5-min cycle) can
                    # restore it and keep upgrades out of "Recently Added"
                    # (migration 040). No-op for genuinely-new albums (not yet
                    # in Plex) and when Plex is unconfigured; best-effort.
                    try:
                        from lib.plex_pin_service import capture_plex_added_at_pin
                        capture_plex_added_at_pin(
                            cfg, db, ir.postflight.imported_path, request_id)
                    except Exception:
                        logger.exception(
                            "PLEX PIN: capture wiring failed (non-fatal)")
                    _trigger_plex(cfg, ir.postflight.imported_path)
                    # Same capture-before-refresh dance for Jellyfin
                    # (migration 046, issue #574): snapshot the album's
                    # DateCreated + item ids while the pre-upgrade items
                    # still exist, so the reconciler can restore the date
                    # once the rescan re-stamps them. No-op for
                    # genuinely-new albums and when Jellyfin is
                    # unconfigured; best-effort.
                    try:
                        from lib.jellyfin_pin_service import (
                            capture_jellyfin_date_created_pin,
                        )
                        capture_jellyfin_date_created_pin(
                            cfg, db, ir.postflight.imported_path, request_id)
                    except Exception:
                        logger.exception(
                            "JELLYFIN PIN: capture wiring failed (non-fatal)")
                    _trigger_jellyfin(cfg)
                if action.cleanup and _should_cleanup_path(scenario, action):
                    # Issue #89: force/manual paths pass the user's
                    # ``failed_imports/…`` folder as ``path`` — cleanup is
                    # data loss on a ``downgrade`` / ``transcode_downgrade``
                    # decision where beets never moved the files.
                    # ``_should_cleanup_path`` only allows cleanup on force/
                    # manual when the decision actually imported (mark_done=
                    # True, i.e. beets has moved the files and the source
                    # directory is now empty), which keeps the wrong-matches
                    # tab honest and prevents duplicate re-imports of an
                    # already-imported album. Auto-import scenarios always
                    # clean — their staging dir under ``/Incoming`` is
                    # disposable by design.
                    _cleanup_staged_dir(path)
                if action.mark_done and ir.postflight.disambiguated and ir.postflight.imported_path:
                    removed = cleanup_disambiguation_orphans(
                        ir.postflight.imported_path,
                        beets_directory=cfg.beets_directory if cfg is not None else "",
                    )
                    if removed and cfg is not None:
                        trigger_meelo_clean(cfg)
        except sp.TimeoutExpired:
            logger.error(f"{mode} TIMEOUT: {label}")
            _record_rejection_and_maybe_requeue(
                db, request_id, dl_info,
                distance=distance, scenario="timeout",
                detail="import_one.py timed out", error="timeout",
                requeue=requeue_on_failure, outcome_label="failed",
                validation_result=ValidationResult(
                    distance=distance,
                    scenario="timeout",
                    detail="import_one.py timed out",
                    error="timeout",
                    source_dirs=source_dirs,
                ).to_json(),
                staged_path=path)
            outcome_message = "Import timed out"
        except Exception:
            logger.exception(f"{mode} ERROR: {label}")
            _record_rejection_and_maybe_requeue(
                db, request_id, dl_info,
                distance=distance, scenario="exception",
                detail="unhandled exception in auto-import", error="exception",
                requeue=requeue_on_failure, outcome_label="failed",
                validation_result=ValidationResult(
                    distance=distance,
                    scenario="exception",
                    detail="unhandled exception in auto-import",
                    error="exception",
                    source_dirs=source_dirs,
                ).to_json(),
                staged_path=path)
            outcome_message = "Unhandled exception"
        finally:
            _remove_quality_evidence_action_file(quality_evidence_action_file)

    return DispatchOutcome(success=outcome_success, message=outcome_message)
