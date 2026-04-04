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
from typing import TYPE_CHECKING

from lib.quality import (parse_import_result, DownloadInfo, ImportResult,
                         ValidationResult,
                         QUALITY_MIN_BITRATE_KBPS,
                         QualityIntent, intent_to_quality_override,
                         dispatch_action, compute_effective_override_bitrate,
                         extract_usernames)
from lib.transitions import apply_transition
from lib.util import cleanup_disambiguation_orphans, trigger_meelo_clean

if TYPE_CHECKING:
    from lib.context import SoularrContext
    from lib.grab_list import GrabListEntry

logger = logging.getLogger("soularr")


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
        dl_info.spectral_grade = new_m.spectral_grade
        dl_info.spectral_bitrate = new_m.spectral_bitrate_kbps
        dl_info.verified_lossless_override = new_m.verified_lossless
    if existing_m:
        dl_info.existing_spectral_bitrate = existing_m.spectral_bitrate_kbps
        if existing_m.min_bitrate_kbps is not None:
            dl_info.existing_min_bitrate = existing_m.min_bitrate_kbps
    dl_info.import_result = ir.to_json()


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


def _check_quality_gate(album_data: GrabListEntry, request_id: int,
                        ctx: "SoularrContext") -> None:
    """Post-import quality gate: if min track bitrate is below V0, queue for upgrade."""
    from lib.quality import quality_gate_decision, AudioQualityMeasurement
    from lib.beets_db import BeetsDB

    mb_id = album_data.mb_release_id
    if not mb_id or not ctx.cfg.pipeline_db_enabled or ctx.pipeline_db_source is None:
        return
    try:
        with BeetsDB() as beets:
            info = beets.get_album_info(mb_id)
        if not info:
            return
        min_br_kbps = info.min_bitrate_kbps
        is_cbr = info.is_cbr

        spectral_br: int | None = None
        req = None
        try:
            req = ctx.pipeline_db_source._get_db().get_request(request_id)
            raw_br = req.get("spectral_bitrate") if req else None
            spectral_br = raw_br if isinstance(raw_br, int) else None
            effective = compute_effective_override_bitrate(min_br_kbps, spectral_br)
            if effective is not None and effective < min_br_kbps:
                logger.info(f"QUALITY GATE: using spectral_bitrate={spectral_br}kbps "
                            f"(lower than beets min_bitrate={min_br_kbps}kbps)")
        except Exception:
            logger.debug("QUALITY GATE: DB lookup failed for spectral override")
        verified_lossless = bool(req.get("verified_lossless")) if req else False

        current = AudioQualityMeasurement(
            min_bitrate_kbps=min_br_kbps, is_cbr=is_cbr,
            verified_lossless=verified_lossless,
            spectral_bitrate_kbps=spectral_br)
        decision = quality_gate_decision(current)

        label = f"{album_data.artist} - {album_data.title}"
        spectral_note = f" (spectral={spectral_br}kbps)" if spectral_br else ""

        if decision == "requeue_upgrade":
            if verified_lossless:
                logger.info(
                    f"QUALITY GATE: {label} gate_bitrate < {QUALITY_MIN_BITRATE_KBPS}kbps "
                    f"but verified_lossless=True — accepting")
                db = ctx.pipeline_db_source._get_db()
                update_fields_vl: dict[str, object] = {"min_bitrate": min_br_kbps}
                if spectral_br:
                    update_fields_vl["spectral_bitrate"] = spectral_br
                apply_transition(db, request_id, "imported",
                                 from_status="imported", **update_fields_vl)
                return
            upgrade_override = intent_to_quality_override(QualityIntent.upgrade)
            db = ctx.pipeline_db_source._get_db()
            apply_transition(db, request_id, "wanted",
                             from_status="imported",
                             quality_override=upgrade_override,
                             min_bitrate=min_br_kbps)
            usernames = extract_usernames(album_data.files)
            gate_br = compute_effective_override_bitrate(min_br_kbps, spectral_br) or min_br_kbps
            if spectral_br and spectral_br < min_br_kbps:
                reason = (f"quality gate: spectral {spectral_br}kbps "
                          f"(beets {min_br_kbps}kbps) < {QUALITY_MIN_BITRATE_KBPS}kbps")
            else:
                reason = f"quality gate: {min_br_kbps}kbps < {QUALITY_MIN_BITRATE_KBPS}kbps"
            for username in usernames:
                db.add_denylist(request_id, username, reason)
            logger.info(
                f"QUALITY GATE: {label} "
                f"gate_bitrate={gate_br}kbps{spectral_note} < {QUALITY_MIN_BITRATE_KBPS}kbps, "
                f"queued for upgrade, denylisted {usernames} "
                f"(searching {upgrade_override})")
        elif decision == "requeue_flac":
            flac_override = intent_to_quality_override(QualityIntent.flac_only)
            db = ctx.pipeline_db_source._get_db()
            apply_transition(db, request_id, "wanted",
                             from_status="imported",
                             quality_override=flac_override,
                             min_bitrate=min_br_kbps)
            logger.info(
                f"QUALITY GATE: {label} "
                f"min_bitrate={min_br_kbps}kbps CBR, not verified lossless — "
                f"searching for FLAC to verify")
        else:  # accept
            db = ctx.pipeline_db_source._get_db()
            update_fields: dict[str, object] = {"min_bitrate": min_br_kbps}
            if spectral_br:
                update_fields["spectral_bitrate"] = spectral_br
            apply_transition(db, request_id, "imported",
                             from_status="imported", **update_fields)
            if verified_lossless:
                logger.info(f"QUALITY GATE: {label} min_bitrate={min_br_kbps}kbps — quality OK")
            else:
                logger.info(f"QUALITY GATE: {label} min_bitrate={min_br_kbps}kbps VBR — quality OK")
    except Exception:
        logger.exception("QUALITY GATE: failed to check quality")


def trigger_meelo_scan(ctx: "SoularrContext") -> None:
    """Trigger Meelo scan via lib.util — wrapper that passes cfg."""
    from lib.util import trigger_meelo_scan as _trigger
    _trigger(ctx.cfg)


def trigger_plex_scan(ctx: "SoularrContext", imported_path: str | None = None) -> None:
    """Trigger Plex partial scan via lib.util — wrapper that passes cfg."""
    from lib.util import trigger_plex_scan as _trigger
    _trigger(ctx.cfg, imported_path)


def dispatch_import(album_data: GrabListEntry, bv_result: ValidationResult, dest: str,
                    dl_info: DownloadInfo, request_id: int,
                    ctx: "SoularrContext") -> None:
    """Auto-import decision tree: run import_one.py and dispatch on result.

    Called from process_completed_album() when source=request and
    distance <= threshold.
    """
    import_script = os.path.join(
        os.path.dirname(ctx.cfg.beets_harness_path), "import_one.py")
    mb_id = album_data.mb_release_id or ""
    label = f"{album_data.artist} - {album_data.title}"
    logger.info(f"AUTO-IMPORT: {label} "
                f"(source=request, dist={bv_result.distance:.4f})")
    try:
        cmd = [sys.executable, import_script, dest, mb_id,
               "--request-id", str(request_id)]
        try:
            req = ctx.pipeline_db_source._get_db().get_request(request_id)
            if req:
                effective_br = compute_effective_override_bitrate(
                    req.get("min_bitrate"), req.get("on_disk_spectral_bitrate"))
                if effective_br is not None:
                    cmd.extend(["--override-min-bitrate", str(effective_br)])
        except Exception:
            logger.debug("DB lookup failed for override-min-bitrate")
        import_env = {**os.environ, "HOME": "/home/abl030"}
        result = sp.run(cmd, capture_output=True, text=True,
                        timeout=1800, env=import_env)
        for line in (result.stderr or "").strip().split("\n"):
            if line.strip():
                logger.info(f"  [import] {line}")

        ir = parse_import_result(result.stdout or "")
        if ir is None:
            logger.error(
                f"AUTO-IMPORT FAILED (no JSON, rc={result.returncode}): {label}")
            for line in (result.stdout or "").strip().split("\n"):
                logger.error(f"  {line}")
            ctx.pipeline_db_source.mark_failed(
                album_data,
                ValidationResult(distance=bv_result.distance,
                                 scenario="no_json_result",
                                 detail=f"import_one.py rc={result.returncode}, no JSON",
                                 error=f"rc={result.returncode}"),
                download_info=dl_info)
        else:
            _populate_dl_info_from_import_result(dl_info, ir)
            decision = ir.decision or "unknown"
            action = dispatch_action(decision)
            usernames = extract_usernames(album_data.files) if action.denylist else set()

            new_br = ir.new_measurement.min_bitrate_kbps if ir.new_measurement else None
            prev_br = ir.existing_measurement.min_bitrate_kbps if ir.existing_measurement else None

            # --- Mark done or failed with decision-specific details ---
            if action.mark_done:
                logger.info(f"AUTO-IMPORT OK: {label} (decision={decision})")
                ctx.pipeline_db_source.mark_done(
                    album_data, bv_result, dest_path=dest, download_info=dl_info)
                if decision in ("import", "preflight_existing"):
                    if prev_br is not None or new_br is not None:
                        try:
                            db = ctx.pipeline_db_source._get_db()
                            apply_transition(db, request_id, "imported",
                                             from_status="imported",
                                             prev_min_bitrate=prev_br,
                                             min_bitrate=new_br)
                        except Exception:
                            logger.exception("Failed to update upgrade delta")
            elif action.mark_failed:
                if decision == "downgrade":
                    scenario = "quality_downgrade"
                    detail = (f"new {new_br}kbps "
                              f"<= existing {prev_br}kbps")
                    logger.warning(f"QUALITY DOWNGRADE PREVENTED: {label}")
                elif decision == "transcode_downgrade":
                    scenario = "transcode_downgrade"
                    detail = (f"transcode {new_br}kbps "
                              f"<= existing {prev_br}kbps")
                    logger.warning(f"TRANSCODE REJECTED: {label} "
                                   f"at {new_br}kbps — not an upgrade")
                else:
                    scenario = decision or "import_error"
                    detail = ir.error
                    logger.error(f"AUTO-IMPORT FAILED: {label} "
                                 f"(decision={decision}, error={ir.error})")
                ctx.pipeline_db_source.mark_failed(
                    album_data,
                    ValidationResult(
                        distance=bv_result.distance, scenario=scenario,
                        detail=detail,
                        error=ir.error if decision not in ("downgrade", "transcode_downgrade") else None),
                    usernames=usernames if action.denylist else None,
                    download_info=dl_info)

            # --- Common actions driven by flags ---
            if action.denylist:
                db = ctx.pipeline_db_source._get_db()
                if decision == "downgrade":
                    reason = "quality downgrade prevented"
                elif decision.startswith("transcode"):
                    reason = f"transcode: {new_br}kbps" if new_br else "transcode detected"
                else:
                    reason = f"rejected: {decision}"
                for username in usernames:
                    db.add_denylist(request_id, username, reason)
                logger.info(f"  Denylisted {usernames} for request {request_id}")

            if action.requeue:
                db = ctx.pipeline_db_source._get_db()
                apply_transition(
                    db, request_id, "wanted",
                    quality_override=intent_to_quality_override(QualityIntent.upgrade),
                    min_bitrate=new_br if action.mark_done else None)

            if action.run_quality_gate:
                _check_quality_gate(album_data, request_id, ctx)
            if action.trigger_meelo:
                trigger_meelo_scan(ctx)
                trigger_plex_scan(ctx, ir.postflight.imported_path)
            if action.cleanup:
                _cleanup_staged_dir(dest)
            if action.mark_done and ir.postflight.disambiguated and ir.postflight.imported_path:
                removed = cleanup_disambiguation_orphans(ir.postflight.imported_path)
                if removed:
                    trigger_meelo_clean(ctx.cfg)
    except sp.TimeoutExpired:
        logger.error(f"AUTO-IMPORT TIMEOUT: {label}")
        timeout_dl = _build_download_info(album_data)
        ctx.pipeline_db_source.mark_failed(
            album_data,
            ValidationResult(distance=bv_result.distance,
                             scenario="timeout", detail="import_one.py timed out",
                             error="timeout"),
            download_info=timeout_dl)
    except Exception:
        logger.exception(f"AUTO-IMPORT ERROR: {label}")
        err_dl = _build_download_info(album_data)
        ctx.pipeline_db_source.mark_failed(
            album_data,
            ValidationResult(distance=bv_result.distance,
                             scenario="exception",
                             detail="unhandled exception in auto-import",
                             error="exception"),
            download_info=err_dl)
