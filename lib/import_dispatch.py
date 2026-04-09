"""Import dispatch — auto-import decision tree.

Extracted from soularr.py process_completed_album(). Contains the logic
that runs import_one.py and dispatches on the ImportResult decision.
"""

from __future__ import annotations

import configparser
import logging
import os
import shutil
import subprocess as sp
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lib.quality import (parse_import_result, DownloadInfo, ImportResult,
                         SpectralMeasurement,
                         ValidationResult,
                         QUALITY_MIN_BITRATE_KBPS,
                         QUALITY_UPGRADE_TIERS, QUALITY_LOSSLESS,
                         dispatch_action, compute_effective_override_bitrate,
                         extract_usernames, narrow_override_on_downgrade,
                         rejection_backfill_override)
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
            spectral_grade = req.get("current_spectral_grade") if req else None
            raw_br = req.get("current_spectral_bitrate") if req else None
            # Only use spectral bitrate to override when grade is suspect —
            # genuine files can have low spectral bitrate due to quiet/sparse
            # music, not bad source quality (e.g. ambient at genuine 320kbps
            # shows ~160kbps spectral estimate because the music has no HF)
            if spectral_grade == "suspect":
                spectral_br = raw_br if isinstance(raw_br, int) else None
            if spectral_br is not None:
                effective = compute_effective_override_bitrate(min_br_kbps, spectral_br)
                if effective is not None and effective < min_br_kbps:
                    logger.info(f"QUALITY GATE: using current_spectral={spectral_br}kbps "
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
                apply_transition(
                    db,
                    request_id,
                    "imported",
                    from_status="imported",
                    min_bitrate=min_br_kbps,
                )
                return
            upgrade_override = QUALITY_UPGRADE_TIERS
            db = ctx.pipeline_db_source._get_db()
            apply_transition(db, request_id, "wanted",
                             from_status="imported",
                             search_filetype_override=upgrade_override,
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
        elif decision == "requeue_lossless":
            lossless_override = QUALITY_LOSSLESS
            db = ctx.pipeline_db_source._get_db()
            apply_transition(db, request_id, "wanted",
                             from_status="imported",
                             search_filetype_override=lossless_override,
                             min_bitrate=min_br_kbps)
            logger.info(
                f"QUALITY GATE: {label} "
                f"min_bitrate={min_br_kbps}kbps CBR, not verified lossless — "
                f"searching for lossless to verify")
        else:  # accept
            db = ctx.pipeline_db_source._get_db()
            apply_transition(
                db,
                request_id,
                "imported",
                from_status="imported",
                min_bitrate=min_br_kbps,
                search_filetype_override=None,  # done searching
            )
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
                    ctx: "SoularrContext", *, force: bool = False) -> None:
    """Import decision tree: run import_one.py and dispatch on result.

    Called from process_completed_album() for auto-import, and from
    dispatch_import_from_db() for force-import and manual-import.
    When force=True, --force is passed to import_one.py (bypasses distance check).
    """
    import_script = os.path.join(
        os.path.dirname(ctx.cfg.beets_harness_path), "import_one.py")
    mb_id = album_data.mb_release_id or ""
    label = f"{album_data.artist} - {album_data.title}"
    mode = "FORCE-IMPORT" if force else "AUTO-IMPORT"
    logger.info(f"{mode}: {label} "
                f"(source=request, dist={bv_result.distance:.4f})")
    try:
        cmd = [sys.executable, import_script, dest, mb_id,
               "--request-id", str(request_id)]
        if force:
            cmd.append("--force")
        if ctx.cfg.verified_lossless_target:
            cmd.extend(["--verified-lossless-target", ctx.cfg.verified_lossless_target])
        if album_data.db_target_format:
            cmd.extend(["--target-format", album_data.db_target_format])
        try:
            req = ctx.pipeline_db_source._get_db().get_request(request_id)
            if req:
                effective_br = compute_effective_override_bitrate(
                    req.get("min_bitrate"), req.get("current_spectral_bitrate"))
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
                f"{mode} FAILED (no JSON, rc={result.returncode}): {label}")
            for line in (result.stdout or "").strip().split("\n"):
                logger.error(f"  {line}")
            ctx.pipeline_db_source.mark_failed(
                album_data,
                ValidationResult(distance=bv_result.distance,
                                 scenario="no_json_result",
                                 detail=f"import_one.py rc={result.returncode}, no JSON",
                                 error=f"rc={result.returncode}"),
                download_info=dl_info,
                cooled_down_users=ctx.cooled_down_users)
        else:
            _populate_dl_info_from_import_result(dl_info, ir)
            decision = ir.decision or "unknown"
            action = dispatch_action(decision)
            usernames = extract_usernames(album_data.files) if action.denylist else set()
            narrowed_override = None
            current_override = None

            new_br = ir.new_measurement.min_bitrate_kbps if ir.new_measurement else None
            prev_br = ir.existing_measurement.min_bitrate_kbps if ir.existing_measurement else None

            # --- Mark done or failed with decision-specific details ---
            if action.mark_done:
                logger.info(f"{mode} OK: {label} (decision={decision})")
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
                    logger.error(f"{mode} FAILED: {label} "
                                 f"(decision={decision}, error={ir.error})")
                if decision == "downgrade" and ctx.pipeline_db_source is not None:
                    try:
                        db = ctx.pipeline_db_source._get_db()
                        req_row = db.get_request(request_id)
                        current_override = req_row.get("search_filetype_override") if req_row else None
                        narrowed_override = narrow_override_on_downgrade(
                            current_override, dl_info)
                        # Backfill: if no override exists yet, check if on-disk
                        # state warrants one (breaks CBR 320 loops)
                        if narrowed_override is None and current_override is None and req_row:
                            from lib.beets_db import BeetsDB
                            with BeetsDB() as beets:
                                beets_info = beets.get_album_info(
                                    album_data.mb_release_id)
                            if beets_info:
                                narrowed_override = rejection_backfill_override(
                                    is_cbr=beets_info.is_cbr,
                                    min_bitrate_kbps=beets_info.min_bitrate_kbps,
                                    spectral_grade=req_row.get(
                                        "current_spectral_grade"),
                                    verified_lossless=bool(
                                        req_row.get("verified_lossless")),
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
                ctx.pipeline_db_source.mark_failed(
                    album_data,
                    ValidationResult(
                        distance=bv_result.distance, scenario=scenario,
                        detail=detail,
                        error=ir.error if decision not in ("downgrade", "transcode_downgrade") else None),
                    usernames=usernames if action.denylist else None,
                    download_info=dl_info,
                    search_filetype_override=narrowed_override,
                    cooled_down_users=ctx.cooled_down_users)
                if narrowed_override is not None:
                    logger.info(
                        f"  Narrowed search_filetype_override '{current_override}'"
                        f" -> '{narrowed_override}' after downgrade")

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
                requeue_fields: dict[str, object] = {
                    "search_filetype_override": QUALITY_UPGRADE_TIERS,
                }
                if action.mark_done and new_br is not None:
                    requeue_fields["min_bitrate"] = new_br
                apply_transition(db, request_id, "wanted", **requeue_fields)

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
        logger.error(f"{mode} TIMEOUT: {label}")
        timeout_dl = _build_download_info(album_data)
        ctx.pipeline_db_source.mark_failed(
            album_data,
            ValidationResult(distance=bv_result.distance,
                             scenario="timeout", detail="import_one.py timed out",
                             error="timeout"),
            download_info=timeout_dl,
            cooled_down_users=ctx.cooled_down_users)
    except Exception:
        logger.exception(f"{mode} ERROR: {label}")
        err_dl = _build_download_info(album_data)
        ctx.pipeline_db_source.mark_failed(
            album_data,
            ValidationResult(distance=bv_result.distance,
                             scenario="exception",
                             detail="unhandled exception in auto-import",
                             error="exception"),
            download_info=err_dl,
            cooled_down_users=ctx.cooled_down_users)


@dataclass(frozen=True)
class DispatchOutcome:
    """Result of dispatch_import_from_db — typed return for web/CLI callers."""
    success: bool
    message: str


def _read_minimal_config() -> dict[str, str]:
    """Read beets_harness_path and verified_lossless_target from runtime config.

    Used by dispatch_import_from_db which runs outside the main soularr process.
    """
    path = os.environ.get("SOULARR_RUNTIME_CONFIG") or "/var/lib/soularr/config.ini"
    result = {"beets_harness_path": "", "verified_lossless_target": ""}
    if not os.path.exists(path):
        return result
    parser = configparser.ConfigParser(interpolation=configparser.BasicInterpolation())
    try:
        parser.read(path)
    except (configparser.Error, OSError):
        return result
    result["beets_harness_path"] = parser.get(
        "Beets Validation", "harness_path", fallback="")
    result["verified_lossless_target"] = parser.get(
        "Beets Validation", "verified_lossless_target", fallback="").strip()
    return result


def dispatch_import_from_db(
    db: object,
    request_id: int,
    failed_path: str,
    *,
    force: bool = False,
    outcome_label: str = "force_import",
) -> DispatchOutcome:
    """Run a force-import or manual-import through the full dispatch pipeline.

    Constructs the lightweight wrappers needed by dispatch_import() from DB
    state, then delegates. All quality checks (downgrade prevention, quality
    gate, meelo scan, denylist) run identically to auto-import.

    Args:
        db: PipelineDB instance
        request_id: Album request ID
        failed_path: Path to the files on disk
        force: Pass --force to import_one.py (bypass distance check)
        outcome_label: download_log outcome string (e.g. "force_import", "manual_import")
    """
    from album_source import DatabaseSource
    from lib.config import SoularrConfig
    from lib.context import SoularrContext
    from lib.grab_list import GrabListEntry

    # Look up album request
    req = db.get_request(request_id)  # type: ignore[union-attr]
    if not req:
        return DispatchOutcome(success=False, message=f"Request {request_id} not found")

    mbid = req.get("mb_release_id", "")
    if not mbid:
        return DispatchOutcome(success=False, message="No MusicBrainz release ID")

    if not os.path.isdir(failed_path):
        return DispatchOutcome(success=False, message=f"Path not found: {failed_path}")

    # Read minimal config for import_one.py flags
    cfg_values = _read_minimal_config()

    # Construct minimal GrabListEntry
    album_data = GrabListEntry(
        album_id=0,
        files=[],
        filetype="",
        title=req.get("album_title", ""),
        artist=req.get("artist_name", ""),
        year=str(req.get("year", "")),
        mb_release_id=mbid,
        db_request_id=request_id,
        db_source="request",
        db_target_format=req.get("target_format"),
    )

    # Construct minimal SoularrConfig
    cfg = SoularrConfig(
        beets_harness_path=cfg_values["beets_harness_path"],
        verified_lossless_target=cfg_values["verified_lossless_target"],
        pipeline_db_enabled=True,
    )

    # Construct minimal DatabaseSource wrapping the existing PipelineDB
    db_source = DatabaseSource.__new__(DatabaseSource)
    db_source.dsn = ""
    db_source._db = db  # type: ignore[attr-defined]

    # Construct minimal SoularrContext
    ctx = SoularrContext(
        cfg=cfg,
        slskd=None,
        pipeline_db_source=db_source,
    )

    # Build validation result from original distance (if available)
    bv_result = ValidationResult(
        distance=0.0,
        scenario="force_import" if force else "manual_import",
    )
    dl_info = DownloadInfo()

    # Capture whether dispatch_import marks done or failed
    original_mark_done = db_source.mark_done
    original_mark_failed = db_source.mark_failed
    outcome = {"success": False, "message": ""}

    def tracking_mark_done(album_record, bv, dest_path=None, download_info=None):
        outcome["success"] = True
        outcome["message"] = "Import successful"
        # Log with force_import/manual_import outcome label
        dl = download_info if isinstance(download_info, DownloadInfo) else DownloadInfo()
        from lib.import_service import extract_import_log_fields
        log_fields = extract_import_log_fields(dl.import_result)
        db.log_download(  # type: ignore[union-attr]
            request_id=request_id,
            outcome=outcome_label,
            import_result=dl.import_result,
            staged_path=failed_path,
            **log_fields,
        )
        # Delegate status update to original
        original_mark_done(album_record, bv, dest_path=dest_path,
                           download_info=download_info)

    def tracking_mark_failed(album_record, bv, usernames=None,
                             download_info=None, search_filetype_override=None,
                             cooled_down_users=None):
        outcome["success"] = False
        outcome["message"] = f"Rejected: {bv.scenario} — {bv.detail}"
        original_mark_failed(album_record, bv, usernames=usernames,
                             download_info=download_info,
                             search_filetype_override=search_filetype_override,
                             cooled_down_users=cooled_down_users)

    db_source.mark_done = tracking_mark_done  # type: ignore[method-assign]
    db_source.mark_failed = tracking_mark_failed  # type: ignore[method-assign]

    dispatch_import(album_data, bv_result, failed_path, dl_info,
                    request_id, ctx, force=force)

    if outcome["success"]:
        return DispatchOutcome(success=True, message=str(outcome["message"]))
    elif outcome["message"]:
        return DispatchOutcome(success=False, message=str(outcome["message"]))
    else:
        # dispatch_import may have logged but not called mark_done/mark_failed
        # (e.g. timeout/exception paths that go through _build_download_info)
        return DispatchOutcome(success=False, message="Import did not complete")
