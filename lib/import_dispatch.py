"""Import dispatch — auto-import decision tree.

Extracted from soularr.py process_completed_album(). Contains the logic
that runs import_one.py and dispatches on the ImportResult decision.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess as sp
import sys
from dataclasses import dataclass
from typing import Sequence, TYPE_CHECKING

from lib.quality import (parse_import_result, DownloadInfo, ImportResult,
                         SpectralMeasurement,
                         ValidationResult,
                         QUALITY_UPGRADE_TIERS, QUALITY_LOSSLESS,
                         dispatch_action, compute_effective_override_bitrate,
                         extract_usernames, narrow_override_on_downgrade,
                         rejection_backfill_override)
from lib.transitions import apply_transition
from lib.util import cleanup_disambiguation_orphans, trigger_meelo_clean

if TYPE_CHECKING:
    from lib.config import SoularrConfig
    from lib.context import SoularrContext
    from lib.grab_list import GrabListEntry
    from lib.pipeline_db import PipelineDB
    from lib.quality import AudioQualityMeasurement, QualityRankConfig

logger = logging.getLogger("soularr")


@dataclass(frozen=True)
class QualityGateState:
    """Resolved on-disk state for a quality-gate evaluation."""
    measurement: AudioQualityMeasurement
    min_bitrate_kbps: int
    spectral_bitrate_kbps: int | None
    spectral_grade: str | None


def load_quality_gate_state(
    *,
    request_id: int,
    db: "PipelineDB",
    mb_id: str | None = None,
    quality_ranks: "QualityRankConfig | None" = None,
) -> QualityGateState | None:
    """Load the current on-disk measurement for quality-gate evaluation.

    Shared adapter for all post-import quality-gate callers. This is the
    single place that combines:
    - Beets on-disk metadata (min/avg/format/is_cbr)
    - request-row overrides (`final_format`, `verified_lossless`)
    - grade-aware spectral override logic
    """
    from lib.beets_db import BeetsDB
    from lib.quality import AudioQualityMeasurement, QualityRankConfig

    if quality_ranks is None:
        quality_ranks = QualityRankConfig.defaults()

    req = None
    try:
        req = db.get_request(request_id)
    except Exception:
        logger.debug("QUALITY GATE: DB lookup failed for request row")

    resolved_mb_id = mb_id or (str(req["mb_release_id"]) if req and req.get("mb_release_id") else None)
    if not resolved_mb_id:
        return None

    with BeetsDB() as beets:
        info = beets.get_album_info(resolved_mb_id, quality_ranks)
    if not info:
        return None

    min_br_kbps = info.min_bitrate_kbps
    spectral_grade = req.get("current_spectral_grade") if req else None
    raw_br = req.get("current_spectral_bitrate") if req else None
    raw_br_int = raw_br if isinstance(raw_br, int) else None
    spectral_br: int | None = None
    effective = compute_effective_override_bitrate(
        min_br_kbps, raw_br_int, spectral_grade)
    if effective is not None and effective < min_br_kbps:
        spectral_br = raw_br_int

    album_format = info.format
    verified_lossless = bool(req.get("verified_lossless")) if req else False
    if req and req.get("final_format"):
        album_format = str(req["final_format"])

    current = AudioQualityMeasurement(
        min_bitrate_kbps=min_br_kbps,
        avg_bitrate_kbps=info.avg_bitrate_kbps,
        median_bitrate_kbps=info.median_bitrate_kbps,
        format=album_format,
        is_cbr=info.is_cbr,
        verified_lossless=verified_lossless,
        spectral_bitrate_kbps=spectral_br,
    )
    return QualityGateState(
        measurement=current,
        min_bitrate_kbps=min_br_kbps,
        spectral_bitrate_kbps=spectral_br,
        spectral_grade=spectral_grade,
    )


def _do_mark_done(
    db: "PipelineDB",
    request_id: int,
    dl_info: DownloadInfo,
    distance: float,
    scenario: str | None,
    dest_path: str | None,
    outcome_label: str = "success",
    detail: str | None = None,
) -> None:
    """Mark album as imported — standalone version of DatabaseSource.mark_done.

    Takes PipelineDB directly instead of going through DatabaseSource.
    Uses outcome_label for download_log (e.g. "force_import" instead of "success").
    """
    from lib.quality import SpectralMeasurement, is_verified_lossless
    from lib.pipeline_db import RequestSpectralStateUpdate

    update_fields: dict[str, object] = dict(
        beets_distance=distance,
        beets_scenario=scenario,
        imported_path=dest_path,
    )
    verified_lossless = (
        bool(dl_info.verified_lossless_override)
        if dl_info.verified_lossless_override is not None
        else is_verified_lossless(
            dl_info.was_converted,
            dl_info.original_filetype,
            dl_info.download_spectral.grade if dl_info.download_spectral else None,
        )
    )
    # Persist the full current quality state, not only truthy upgrades.
    # Otherwise old verified/final-format labels leak into later imports.
    update_fields["verified_lossless"] = verified_lossless
    if dl_info.download_spectral is not None:
        current_spectral = dl_info.download_spectral
        if update_fields.get("verified_lossless") and dl_info.bitrate:
            current_spectral = SpectralMeasurement(
                grade=dl_info.download_spectral.grade,
                bitrate_kbps=dl_info.bitrate // 1000,
            )
        update_fields.update(
            RequestSpectralStateUpdate(
                last_download=dl_info.download_spectral,
                current=current_spectral,
                ).as_update_fields()
        )
    update_fields["final_format"] = dl_info.final_format
    apply_transition(db, request_id, "imported", **update_fields)

    db.log_download(
        request_id=request_id,
        soulseek_username=dl_info.username,
        filetype=dl_info.filetype,
        beets_distance=distance,
        beets_scenario=scenario,
        beets_detail=detail,
        outcome=outcome_label,
        staged_path=dest_path,
        bitrate=dl_info.bitrate,
        sample_rate=dl_info.sample_rate,
        bit_depth=dl_info.bit_depth,
        is_vbr=dl_info.is_vbr,
        was_converted=dl_info.was_converted,
        original_filetype=dl_info.original_filetype,
        slskd_filetype=dl_info.slskd_filetype,
        slskd_bitrate=dl_info.slskd_bitrate,
        actual_filetype=dl_info.actual_filetype,
        actual_min_bitrate=dl_info.actual_min_bitrate,
        spectral_grade=dl_info.download_spectral.grade if dl_info.download_spectral else None,
        spectral_bitrate=(
            dl_info.download_spectral.bitrate_kbps if dl_info.download_spectral else None
        ),
        existing_min_bitrate=dl_info.existing_min_bitrate,
        existing_spectral_bitrate=(
            dl_info.current_spectral.bitrate_kbps if dl_info.current_spectral else None
        ),
        import_result=dl_info.import_result,
        validation_result=dl_info.validation_result,
        final_format=dl_info.final_format,
    )


def _record_rejection_and_maybe_requeue(
    db: "PipelineDB",
    request_id: int,
    dl_info: DownloadInfo,
    distance: float,
    scenario: str,
    detail: str | None,
    error: str | None,
    *,
    requeue: bool = True,
    outcome_label: str = "rejected",
    search_filetype_override: str | None = None,
    validation_result: str | None = None,
    staged_path: str | None = None,
) -> None:
    """Record a rejected import and optionally requeue the request.

    When requeue=True (auto-import): transitions to "wanted", records attempt.
    When requeue=False (force/manual import): only logs to download_log.

    Note: denylisting and cooldown are handled by the caller (dispatch_import_core)
    via action.denylist, not here.
    """
    if requeue:
        transition_kwargs: dict[str, object] = dict(
            beets_distance=distance,
            beets_scenario=scenario,
        )
        if search_filetype_override is not None:
            transition_kwargs["search_filetype_override"] = search_filetype_override
        apply_transition(db, request_id, "wanted", **transition_kwargs)
        db.record_attempt(request_id, "validation")

    db.log_download(
        request_id=request_id,
        soulseek_username=dl_info.username,
        filetype=dl_info.filetype,
        beets_distance=distance,
        beets_scenario=scenario,
        beets_detail=detail,
        outcome=outcome_label,
        staged_path=staged_path,
        error_message=error,
        bitrate=dl_info.bitrate,
        sample_rate=dl_info.sample_rate,
        bit_depth=dl_info.bit_depth,
        is_vbr=dl_info.is_vbr,
        was_converted=dl_info.was_converted,
        original_filetype=dl_info.original_filetype,
        slskd_filetype=dl_info.slskd_filetype,
        slskd_bitrate=dl_info.slskd_bitrate,
        actual_filetype=dl_info.actual_filetype,
        actual_min_bitrate=dl_info.actual_min_bitrate,
        spectral_grade=dl_info.download_spectral.grade if dl_info.download_spectral else None,
        spectral_bitrate=(
            dl_info.download_spectral.bitrate_kbps if dl_info.download_spectral else None
        ),
        existing_min_bitrate=dl_info.existing_min_bitrate,
        existing_spectral_bitrate=(
            dl_info.current_spectral.bitrate_kbps if dl_info.current_spectral else None
        ),
        import_result=dl_info.import_result,
        validation_result=(validation_result
                           if validation_result is not None
                           else dl_info.validation_result),
    )


def _populate_dl_info_from_import_result(dl_info: DownloadInfo,
                                         ir: ImportResult) -> None:
    """Populate a DownloadInfo from an ImportResult (pure, no I/O)."""
    conv = ir.conversion
    new_m = ir.new_measurement
    existing_m = ir.existing_measurement
    if conv.was_converted:
        dl_info.was_converted = True
        dl_info.original_filetype = conv.original_filetype
        dl_info.filetype = conv.target_filetype
        dl_info.is_vbr = True
        dl_info.slskd_filetype = conv.original_filetype
        dl_info.actual_filetype = conv.target_filetype
    else:
        dl_info.slskd_filetype = dl_info.filetype
        dl_info.actual_filetype = dl_info.filetype
    if new_m:
        if new_m.min_bitrate_kbps is not None:
            dl_info.bitrate = new_m.min_bitrate_kbps * 1000
        dl_info.download_spectral = SpectralMeasurement.from_parts(
            new_m.spectral_grade, new_m.spectral_bitrate_kbps)
        dl_info.verified_lossless_override = new_m.verified_lossless
    if existing_m:
        dl_info.current_spectral = SpectralMeasurement.from_parts(
            existing_m.spectral_grade, existing_m.spectral_bitrate_kbps)
        if existing_m.min_bitrate_kbps is not None:
            dl_info.existing_min_bitrate = existing_m.min_bitrate_kbps
    dl_info.import_result = ir.to_json()
    if ir.final_format:
        dl_info.final_format = ir.final_format


def _cleanup_staged_dir(dest: str) -> None:
    """Remove a staged directory and its parent if empty."""
    if os.path.isdir(dest):
        shutil.rmtree(dest)
        logger.info(f"  Cleaned up staged dir: {dest}")
        parent = os.path.dirname(dest)
        if os.path.isdir(parent) and not os.listdir(parent):
            os.rmdir(parent)
            logger.info(f"  Cleaned up empty artist dir: {parent}")


def _build_download_info(album_data: GrabListEntry) -> DownloadInfo:
    """Extract audio quality metadata from album files for download logging."""
    files = album_data.files
    if not files:
        return DownloadInfo()
    usernames = set(f.username for f in files if f.username)
    filetypes = set(f.filename.split(".")[-1].lower() for f in files if "." in f.filename)
    bitrates = [f.bitRate for f in files if f.bitRate is not None]
    sample_rates = [f.sampleRate for f in files if f.sampleRate is not None]
    bit_depths = [f.bitDepth for f in files if f.bitDepth is not None]
    vbr_flags = [f.isVariableBitRate for f in files if f.isVariableBitRate is not None]

    return DownloadInfo(
        username=", ".join(sorted(usernames)) if usernames else None,
        filetype=", ".join(sorted(filetypes)) if filetypes else None,
        bitrate=min(bitrates) if bitrates else None,
        sample_rate=max(sample_rates) if sample_rates else None,
        bit_depth=max(bit_depths) if bit_depths else None,
        is_vbr=any(vbr_flags) if vbr_flags else None,
    )


def _check_quality_gate_core(
    mb_id: str,
    label: str,
    request_id: int,
    files: Sequence[object],
    db: "PipelineDB",
    quality_ranks: "QualityRankConfig | None" = None,
) -> None:
    """Post-import quality gate — standalone version taking plain params + PipelineDB.

    Reads beets DB for on-disk quality, runs quality_gate_decision, dispatches
    requeue/accept. Used by both auto-import (via wrapper) and core dispatch.

    ``quality_ranks`` is used by ``BeetsDB.get_album_info()`` to reduce
    mixed-format albums via ``cfg.mixed_format_precedence``. Defaults to
    ``QualityRankConfig.defaults()`` so existing tests and callers that
    don't care about mixed-format reduction still work. Commit 5 will thread
    the real runtime config through from dispatch_import_core().
    """
    from lib.quality import quality_gate_decision, QualityRankConfig, gate_rank

    if quality_ranks is None:
        quality_ranks = QualityRankConfig.defaults()

    if not mb_id:
        return
    try:
        state = load_quality_gate_state(
            request_id=request_id,
            db=db,
            mb_id=mb_id,
            quality_ranks=quality_ranks,
        )
        if not state:
            return
        current = state.measurement
        min_br_kbps = state.min_bitrate_kbps
        spectral_br = state.spectral_bitrate_kbps
        spectral_grade = state.spectral_grade
        if spectral_br is not None:
            logger.info(f"QUALITY GATE: using current_spectral={spectral_br}kbps "
                        f"(lower than beets min_bitrate={min_br_kbps}kbps, "
                        f"grade={spectral_grade})")
        decision = quality_gate_decision(current, cfg=quality_ranks)

        spectral_note = f" (spectral={spectral_br}kbps)" if spectral_br else ""

        if decision == "requeue_upgrade":
            upgrade_override = QUALITY_UPGRADE_TIERS
            apply_transition(db, request_id, "wanted",
                             from_status="imported",
                             search_filetype_override=upgrade_override,
                             min_bitrate=min_br_kbps)
            usernames = extract_usernames(files)
            gate_br = compute_effective_override_bitrate(
                min_br_kbps, spectral_br, spectral_grade) or min_br_kbps
            actual_rank = gate_rank(current, quality_ranks)
            gate_min = quality_ranks.gate_min_rank
            br_note = (f"spectral {spectral_br}kbps (beets {min_br_kbps}kbps)"
                       if spectral_br and spectral_br < min_br_kbps
                       else f"{min_br_kbps}kbps")
            reason = (f"quality gate: rank {actual_rank.name} < {gate_min.name} "
                      f"({br_note})")
            for username in usernames:
                db.add_denylist(request_id, username, reason)
            logger.info(
                f"QUALITY GATE: {label} "
                f"rank={actual_rank.name} < {gate_min.name} "
                f"(gate_bitrate={gate_br}kbps{spectral_note}), "
                f"queued for upgrade, denylisted {usernames} "
                f"(searching {upgrade_override})")
        elif decision == "requeue_lossless":
            lossless_override = QUALITY_LOSSLESS
            apply_transition(db, request_id, "wanted",
                             from_status="imported",
                             search_filetype_override=lossless_override,
                             min_bitrate=min_br_kbps)
            logger.info(
                f"QUALITY GATE: {label} "
                f"min_bitrate={min_br_kbps}kbps CBR, not verified lossless — "
                f"searching for lossless to verify")
        else:  # accept
            apply_transition(
                db,
                request_id,
                "imported",
                from_status="imported",
                min_bitrate=min_br_kbps,
                search_filetype_override=None,  # done searching
            )
            if current.verified_lossless:
                logger.info(f"QUALITY GATE: {label} min_bitrate={min_br_kbps}kbps — quality OK")
            else:
                logger.info(f"QUALITY GATE: {label} min_bitrate={min_br_kbps}kbps VBR — quality OK")
    except Exception:
        logger.exception("QUALITY GATE: failed to check quality")



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
    distance: float = 0.0,
    scenario: str = "auto_import",
    files: Sequence[object] | None = None,
    cfg: "SoularrConfig | None" = None,
    outcome_label: str = "success",
    requeue_on_failure: bool = True,
    cooled_down_users: set[str] | None = None,
) -> "DispatchOutcome":
    """Core import dispatch — takes plain params + PipelineDB directly.

    Runs import_one.py, parses result, dispatches on decision (mark_done/failed,
    denylist, quality gate, meelo/plex scan, cleanup). Returns DispatchOutcome.

    Used by dispatch_import() (auto-import adapter) and dispatch_import_from_db()
    (force/manual import) — eliminates the need for heavyweight wrapper objects.
    """
    from lib.util import trigger_meelo_scan as _trigger_meelo
    from lib.util import trigger_plex_scan as _trigger_plex

    import_script = os.path.join(
        os.path.dirname(beets_harness_path), "import_one.py")
    mode = (
        "FORCE-IMPORT" if force
        else "MANUAL-IMPORT" if scenario == "manual_import"
        else "AUTO-IMPORT"
    )
    logger.info(f"{mode}: {label} "
                f"(source=request, dist={distance:.4f})")

    outcome_success = False
    outcome_message = ""

    try:
        cmd = [sys.executable, import_script, path, mb_release_id,
               "--request-id", str(request_id)]
        if force:
            cmd.append("--force")
        if verified_lossless_target:
            cmd.extend(["--verified-lossless-target", verified_lossless_target])
        if target_format:
            cmd.extend(["--target-format", target_format])
        if override_min_bitrate is not None:
            cmd.extend(["--override-min-bitrate", str(override_min_bitrate)])
        # Serialize the runtime QualityRankConfig so the harness classifies
        # with the same policy as the caller. Missing cfg (e.g. legacy test
        # path) → harness falls back to QualityRankConfig.defaults().
        if cfg is not None:
            cmd.extend(["--quality-rank-config", cfg.quality_ranks.to_json()])
        import_env = {**os.environ, "HOME": "/home/abl030"}
        result = sp.run(cmd, capture_output=True, text=True,
                        timeout=1800, env=import_env)
        for line in (result.stderr or "").strip().split("\n"):
            if line.strip():
                logger.info(f"  [import] {line}")

        ir = parse_import_result(result.stdout or "")
        if ir is None:
            logger.error(
                f"{mode} FAILED (no JSON, rc={result.returncode}): {label}")
            for line in (result.stdout or "").strip().split("\n"):
                logger.error(f"  {line}")
            _record_rejection_and_maybe_requeue(
                db, request_id, dl_info,
                distance=distance,
                scenario="no_json_result",
                detail=f"import_one.py rc={result.returncode}, no JSON",
                error=f"rc={result.returncode}",
                requeue=requeue_on_failure,
                outcome_label="failed",
                validation_result=ValidationResult(
                    distance=distance,
                    scenario="no_json_result",
                    detail=f"import_one.py rc={result.returncode}, no JSON",
                    error=f"rc={result.returncode}",
                ).to_json(),
                staged_path=path)
            outcome_message = f"No JSON result (rc={result.returncode})"
        else:
            _populate_dl_info_from_import_result(dl_info, ir)
            decision = ir.decision or "unknown"
            action = dispatch_action(decision)
            file_list = files or []
            usernames = extract_usernames(file_list) if action.denylist else set()
            narrowed_override = None
            current_override = None

            new_br = ir.new_measurement.min_bitrate_kbps if ir.new_measurement else None
            prev_br = ir.existing_measurement.min_bitrate_kbps if ir.existing_measurement else None

            # --- Mark done or failed with decision-specific details ---
            if action.mark_done:
                logger.info(f"{mode} OK: {label} (decision={decision})")
                _do_mark_done(
                    db, request_id, dl_info,
                    distance=distance, scenario=scenario,
                    dest_path=path, outcome_label=outcome_label)
                if decision in ("import", "preflight_existing"):
                    if prev_br is not None or new_br is not None:
                        try:
                            apply_transition(db, request_id, "imported",
                                             from_status="imported",
                                             prev_min_bitrate=prev_br,
                                             min_bitrate=new_br)
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
                else:
                    fail_scenario = decision or "import_error"
                    fail_detail = ir.error
                    logger.error(f"{mode} FAILED: {label} "
                                 f"(decision={decision}, error={ir.error})")
                fail_error = ir.error if decision not in ("downgrade", "transcode_downgrade") else None

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
                elif decision.startswith("transcode"):
                    reason = f"transcode: {new_br}kbps" if new_br else "transcode detected"
                else:
                    reason = f"rejected: {decision}"
                for username in usernames:
                    db.add_denylist(request_id, username, reason)
                    if cooled_down_users is not None:
                        if db.check_and_apply_cooldown(username):
                            cooled_down_users.add(username)
                logger.info(f"  Denylisted {usernames} for request {request_id}")

            if action.requeue and (requeue_on_failure or not action.record_rejection):
                requeue_fields: dict[str, object] = {
                    "search_filetype_override": QUALITY_UPGRADE_TIERS,
                }
                if action.mark_done and new_br is not None:
                    requeue_fields["min_bitrate"] = new_br
                apply_transition(db, request_id, "wanted", **requeue_fields)

            if action.run_quality_gate:
                _check_quality_gate_core(
                    mb_id=mb_release_id,
                    label=label,
                    request_id=request_id,
                    files=list(file_list),
                    db=db,
                    quality_ranks=cfg.quality_ranks if cfg is not None else None,
                )
            if action.trigger_meelo and cfg is not None:
                _trigger_meelo(cfg)
                _trigger_plex(cfg, ir.postflight.imported_path)
            if action.cleanup:
                _cleanup_staged_dir(path)
            if action.mark_done and ir.postflight.disambiguated and ir.postflight.imported_path:
                removed = cleanup_disambiguation_orphans(ir.postflight.imported_path)
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
            ).to_json(),
            staged_path=path)
        outcome_message = "Unhandled exception"

    return DispatchOutcome(success=outcome_success, message=outcome_message)


def dispatch_import(album_data: "GrabListEntry", bv_result: ValidationResult, dest: str,
                    dl_info: DownloadInfo, request_id: int,
                    ctx: "SoularrContext", *, force: bool = False) -> None:
    """Import decision tree — thin adapter extracting plain params for the core.

    Called from process_completed_album() for auto-import.
    """
    db = ctx.pipeline_db_source._get_db()

    # Compute override_min_bitrate from DB — grade-aware: current_spectral_bitrate
    # only lowers the override when current_spectral_grade is suspect/likely_transcode.
    override_min_bitrate: int | None = None
    try:
        req = db.get_request(request_id)
        if req:
            override_min_bitrate = compute_effective_override_bitrate(
                req.get("min_bitrate"),
                req.get("current_spectral_bitrate"),
                req.get("current_spectral_grade"))
    except Exception:
        logger.debug("DB lookup failed for override-min-bitrate")

    dispatch_import_core(
        path=dest,
        mb_release_id=album_data.mb_release_id or "",
        request_id=request_id,
        label=f"{album_data.artist} - {album_data.title}",
        force=force,
        override_min_bitrate=override_min_bitrate,
        target_format=album_data.db_target_format,
        verified_lossless_target=ctx.cfg.verified_lossless_target,
        beets_harness_path=ctx.cfg.beets_harness_path,
        db=db,
        dl_info=dl_info,
        distance=bv_result.distance if bv_result.distance is not None else 0.0,
        scenario=bv_result.scenario or "auto_import",
        files=album_data.files,
        cfg=ctx.cfg,
        requeue_on_failure=True,
        cooled_down_users=ctx.cooled_down_users,
    )


@dataclass(frozen=True)
class DispatchOutcome:
    """Result of dispatch_import_from_db — typed return for web/CLI callers."""
    success: bool
    message: str

def dispatch_import_from_db(
    db: "PipelineDB",
    request_id: int,
    failed_path: str,
    *,
    force: bool = False,
    outcome_label: str = "force_import",
    source_username: str | None = None,
) -> "DispatchOutcome":
    """Run a force-import or manual-import through the full dispatch pipeline.

    Calls dispatch_import_core directly with plain params — no DatabaseSource
    wrapper, no monkey-patching. All quality checks (downgrade prevention,
    quality gate, meelo scan, denylist) run identically to auto-import.

    Args:
        db: PipelineDB instance
        request_id: Album request ID
        failed_path: Path to the files on disk
        force: Pass --force to import_one.py (bypass distance check)
        outcome_label: download_log outcome string (e.g. "force_import", "manual_import")
        source_username: Original Soulseek username for force-import audit/denylist flows
    """
    from lib.grab_list import DownloadFile

    req = db.get_request(request_id)
    if not req:
        return DispatchOutcome(success=False, message=f"Request {request_id} not found")

    mbid = req.get("mb_release_id", "")
    if not mbid:
        return DispatchOutcome(success=False, message="No MusicBrainz release ID")

    if not os.path.isdir(failed_path):
        return DispatchOutcome(success=False, message=f"Path not found: {failed_path}")

    from lib.config import read_runtime_config

    cfg = read_runtime_config()

    files: list[DownloadFile] = []
    if source_username:
        files = [DownloadFile(
            filename="", id="", file_dir="",
            username=source_username, size=0,
        )]

    # Compute override from DB state — grade-aware: current_spectral_bitrate only
    # lowers the override when current_spectral_grade is suspect/likely_transcode.
    override_min_bitrate = compute_effective_override_bitrate(
        req.get("min_bitrate"),
        req.get("current_spectral_bitrate"),
        req.get("current_spectral_grade"))

    return dispatch_import_core(
        path=failed_path,
        mb_release_id=mbid,
        request_id=request_id,
        label=f"{req.get('artist_name', '')} - {req.get('album_title', '')}",
        force=force,
        override_min_bitrate=override_min_bitrate,
        target_format=req.get("target_format"),
        verified_lossless_target=cfg.verified_lossless_target,
        beets_harness_path=cfg.beets_harness_path,
        db=db,
        dl_info=DownloadInfo(username=source_username),
        distance=0.0,
        scenario="force_import" if force else "manual_import",
        files=files,
        cfg=cfg,
        outcome_label=outcome_label,
        requeue_on_failure=False,
    )
