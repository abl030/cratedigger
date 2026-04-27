"""Import dispatch — auto-import decision tree.

Extracted from cratedigger.py process_completed_album(). Contains the logic
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

from lib import transitions
from lib.quality import (parse_import_result, DispatchAction, DownloadInfo,
                         ImportResult, SpectralMeasurement, V0ProbeEvidence,
                         ValidationResult, QUALITY_UPGRADE_TIERS, QUALITY_LOSSLESS,
                         V0_PROBE_LOSSLESS_SOURCE,
                         dispatch_action, compute_effective_override_bitrate,
                         extract_usernames, is_comparable_lossless_source_probe,
                         narrow_override_on_downgrade,
                         rejection_backfill_override)
from lib.util import (beets_subprocess_env, cleanup_disambiguation_orphans,
                      repair_mp3_headers, trigger_meelo_clean)
from lib.preimport import inspect_local_files, run_preimport_gates

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.grab_list import GrabListEntry
    from lib.pipeline_db import PipelineDB
    from lib.quality import AudioQualityMeasurement, QualityRankConfig

logger = logging.getLogger("cratedigger")

# Scenarios whose ``path`` is the user's source data (``failed_imports/…``),
# NOT a disposable staging directory. Used to gate ``_cleanup_staged_dir``
# so a ``downgrade`` / ``transcode_downgrade`` decision from the harness
# can never delete the user's only copy of the source. Auto-import uses
# bv_result.scenario values like ``strong_match`` / ``weak_match`` /
# ``auto_import``, none of which appear here — their staging dir under
# ``/Incoming`` is always safe to remove (see issue #89).
FORCE_MANUAL_SCENARIOS: frozenset[str] = frozenset({"force_import", "manual_import"})


def _should_cleanup_path(scenario: str, action: "DispatchAction") -> bool:
    """Whether ``_cleanup_staged_dir`` is safe for this dispatch outcome.

    Issue #89 rules:

    * Auto-import (scenario not in ``FORCE_MANUAL_SCENARIOS``) always
      cleans its disposable ``/Incoming`` staging dir.
    * Force/manual-import paths pass the user's ``failed_imports/…``
      folder — cleanup is only safe on a successful import
      (``action.mark_done=True``, meaning beets has moved the files out
      and the source directory is now empty). On a ``downgrade`` /
      ``transcode_downgrade`` decision (mark_done=False) the files are
      still in the source folder, so cleanup would delete the user's
      data.
    * Successful force/manual import MUST clean so the wrong-matches tab
      (``lib.pipeline_db.get_wrong_matches``) stops treating the
      still-existing folder as an active pending entry — otherwise the
      album would show up as re-importable even though beets already
      has it.
    """
    if scenario not in FORCE_MANUAL_SCENARIOS:
        return True
    return action.mark_done


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


@dataclass(frozen=True)
class DispatchOutcome:
    """Summary of an import outcome."""

    success: bool
    message: str
    deferred: bool = False


@dataclass(frozen=True)
class ImportOneRun:
    """Result of one import_one.py subprocess protocol invocation."""

    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    import_result: ImportResult | None


def import_one_script_from_harness(beets_harness_path: str) -> str:
    """Resolve import_one.py beside the configured harness wrapper."""
    return os.path.join(os.path.dirname(beets_harness_path), "import_one.py")


def build_import_one_command(
    *,
    path: str,
    mb_release_id: str,
    beets_harness_path: str,
    request_id: int | None = None,
    force: bool = False,
    preserve_source: bool = False,
    dry_run: bool = False,
    override_min_bitrate: int | None = None,
    target_format: str | None = None,
    verified_lossless_target: str = "",
    quality_rank_config_json: str | None = None,
    existing_v0_probe: V0ProbeEvidence | None = None,
) -> list[str]:
    """Build the single shared import_one.py command line."""
    cmd = [
        sys.executable,
        import_one_script_from_harness(beets_harness_path),
        path,
        mb_release_id,
    ]
    if request_id is not None:
        cmd.extend(["--request-id", str(request_id)])
    if force:
        cmd.append("--force")
    if preserve_source:
        cmd.append("--preserve-source")
    if dry_run:
        cmd.append("--dry-run")
    if verified_lossless_target:
        cmd.extend(["--verified-lossless-target", verified_lossless_target])
    if target_format:
        cmd.extend(["--target-format", target_format])
    if override_min_bitrate is not None:
        cmd.extend(["--override-min-bitrate", str(override_min_bitrate)])
    if quality_rank_config_json:
        cmd.extend(["--quality-rank-config", quality_rank_config_json])
    if existing_v0_probe is not None:
        if existing_v0_probe.min_bitrate_kbps is not None:
            cmd.extend([
                "--existing-v0-probe-min-bitrate",
                str(existing_v0_probe.min_bitrate_kbps),
            ])
        if existing_v0_probe.avg_bitrate_kbps is not None:
            cmd.extend([
                "--existing-v0-probe-avg-bitrate",
                str(existing_v0_probe.avg_bitrate_kbps),
            ])
        if existing_v0_probe.median_bitrate_kbps is not None:
            cmd.extend([
                "--existing-v0-probe-median-bitrate",
                str(existing_v0_probe.median_bitrate_kbps),
            ])
    return cmd


def run_import_one(
    *,
    path: str,
    mb_release_id: str,
    beets_harness_path: str,
    request_id: int | None = None,
    force: bool = False,
    preserve_source: bool = False,
    dry_run: bool = False,
    override_min_bitrate: int | None = None,
    target_format: str | None = None,
    verified_lossless_target: str = "",
    quality_rank_config_json: str | None = None,
    existing_v0_probe: V0ProbeEvidence | None = None,
    timeout: int = 1800,
) -> ImportOneRun:
    """Run import_one.py and parse its ImportResult sentinel."""
    cmd = build_import_one_command(
        path=path,
        mb_release_id=mb_release_id,
        beets_harness_path=beets_harness_path,
        request_id=request_id,
        force=force,
        preserve_source=preserve_source,
        dry_run=dry_run,
        override_min_bitrate=override_min_bitrate,
        target_format=target_format,
        verified_lossless_target=verified_lossless_target,
        quality_rank_config_json=quality_rank_config_json,
        existing_v0_probe=existing_v0_probe,
    )
    result = sp.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=beets_subprocess_env(),
    )
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    return ImportOneRun(
        command=tuple(cmd),
        returncode=int(result.returncode),
        stdout=stdout,
        stderr=stderr,
        import_result=parse_import_result(stdout),
    )


def _v0_probe_log_fields(dl_info: DownloadInfo) -> dict[str, int | str | None]:
    probe = dl_info.v0_probe
    existing = dl_info.existing_v0_probe
    return {
        "v0_probe_kind": probe.kind if probe else None,
        "v0_probe_min_bitrate": (
            probe.min_bitrate_kbps if probe else None
        ),
        "v0_probe_avg_bitrate": (
            probe.avg_bitrate_kbps if probe else None
        ),
        "v0_probe_median_bitrate": (
            probe.median_bitrate_kbps if probe else None
        ),
        "existing_v0_probe_kind": existing.kind if existing else None,
        "existing_v0_probe_min_bitrate": (
            existing.min_bitrate_kbps if existing else None
        ),
        "existing_v0_probe_avg_bitrate": (
            existing.avg_bitrate_kbps if existing else None
        ),
        "existing_v0_probe_median_bitrate": (
            existing.median_bitrate_kbps if existing else None
        ),
    }


def _current_lossless_v0_probe_from_request(
    row: dict[str, object] | None,
) -> V0ProbeEvidence | None:
    """Build the current comparable source-probe evidence from album_requests."""
    if not row:
        return None
    avg = row.get("current_lossless_source_v0_probe_avg_bitrate")
    if avg is None:
        return None
    return V0ProbeEvidence(
        kind=V0_PROBE_LOSSLESS_SOURCE,
        min_bitrate_kbps=row.get(
            "current_lossless_source_v0_probe_min_bitrate"),
        avg_bitrate_kbps=avg,
        median_bitrate_kbps=row.get(
            "current_lossless_source_v0_probe_median_bitrate"),
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
    imported_path: str | None = None,
    clear_stale_v0_probe: bool = True,
) -> int | None:
    """Mark album as imported — standalone version of DatabaseSource.mark_done.

    Takes PipelineDB directly instead of going through DatabaseSource.
    Uses outcome_label for download_log (e.g. "force_import" instead of "success").

    ``imported_path`` is the beets destination (from
    ``ImportResult.postflight.imported_path``) — what shows up in the UI's
    "Imported to" label. ``dest_path`` is the source/staging path passed to
    the importer. When callers have both (auto/force/manual paths that ran
    beets), they pass ``imported_path`` so ``album_requests.imported_path``
    reflects the actual on-disk location. Callers that only stage for manual
    review (``album_source.mark_done``) leave ``imported_path=None``; it
    falls back to ``dest_path`` so legacy behavior is preserved (issue #93).
    """
    from lib.quality import SpectralMeasurement, is_verified_lossless
    from lib.pipeline_db import RequestSpectralStateUpdate, RequestV0ProbeStateUpdate

    update_fields: dict[str, object] = dict(
        beets_distance=distance,
        beets_scenario=scenario,
        imported_path=imported_path if imported_path else dest_path,
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
    if is_comparable_lossless_source_probe(dl_info.v0_probe):
        update_fields.update(
            RequestV0ProbeStateUpdate(
                current_lossless_source=dl_info.v0_probe,
            ).as_update_fields()
        )
    elif clear_stale_v0_probe:
        update_fields.update(
            RequestV0ProbeStateUpdate(
                clear_current_lossless_source=True,
            ).as_update_fields()
        )
    update_fields["final_format"] = dl_info.final_format
    transitions.finalize_request(
        db,
        request_id,
        transitions.RequestTransition.to_imported_fields(fields=update_fields),
    )

    return db.log_download(
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
        **_v0_probe_log_fields(dl_info),
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
        transition_kwargs: dict[str, object] = {}
        if search_filetype_override is not None:
            transition_kwargs["search_filetype_override"] = search_filetype_override
        transitions.finalize_request(
            db,
            request_id,
            transitions.RequestTransition.to_wanted_fields(
                fields=transition_kwargs),
        )
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
        **_v0_probe_log_fields(dl_info),
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
            dl_info.actual_min_bitrate = new_m.min_bitrate_kbps
        dl_info.download_spectral = SpectralMeasurement.from_parts(
            new_m.spectral_grade, new_m.spectral_bitrate_kbps)
        dl_info.verified_lossless_override = new_m.verified_lossless
    if existing_m:
        dl_info.current_spectral = SpectralMeasurement.from_parts(
            existing_m.spectral_grade, existing_m.spectral_bitrate_kbps)
        if existing_m.min_bitrate_kbps is not None:
            dl_info.existing_min_bitrate = existing_m.min_bitrate_kbps
    dl_info.import_result = ir.to_json()
    dl_info.v0_probe = ir.v0_probe
    dl_info.existing_v0_probe = ir.existing_v0_probe
    if ir.final_format:
        dl_info.final_format = ir.final_format


def _log_postflight_bad_extensions(
    *,
    ir: ImportResult,
    mode: str,
    request_id: int,
    label: str,
) -> None:
    """Emit an error-level service log for warning-only postflight anomalies."""
    bad_extensions = ir.postflight.bad_extensions
    if not bad_extensions:
        return
    logger.error(
        "POSTFLIGHT BAD EXTENSIONS: %s request_id=%s label=%s files=%s; "
        "import remains successful but warning is persisted in "
        "download_log.import_result.postflight.bad_extensions",
        mode,
        request_id,
        label,
        ", ".join(bad_extensions),
    )


def _guard_failure_detail(ir: ImportResult) -> str | None:
    guard = ir.postflight.duplicate_remove_guard
    if guard is None:
        return ir.error
    detail = f"{guard.reason}: {guard.message}"
    if guard.duplicate_count:
        detail = f"{detail} (duplicates={guard.duplicate_count})"
    return detail


def _quarantine_duplicate_remove_guard_source(
    *,
    ir: ImportResult,
    path: str,
    request_id: int,
    cfg: "CratediggerConfig | None",
) -> None:
    guard = ir.postflight.duplicate_remove_guard
    if guard is None:
        return

    from lib.duplicate_remove_guard import (
        quarantine_duplicate_remove_guard_source,
    )

    staging_dir = (
        cfg.beets_staging_dir
        if cfg is not None and cfg.beets_staging_dir
        else os.path.dirname(os.path.abspath(path))
    )
    result = quarantine_duplicate_remove_guard_source(
        source_path=path,
        staging_dir=staging_dir,
        request_id=request_id,
    )
    guard.quarantine_path = result.quarantine_path
    guard.quarantine_error = result.error
    if result.success:
        logger.error(
            "DUPLICATE REMOVE GUARD: quarantined staged source for "
            "request_id=%s from %s to %s",
            request_id,
            result.source_path,
            result.quarantine_path,
        )
    else:
        logger.error(
            "DUPLICATE REMOVE GUARD: failed to quarantine staged source for "
            "request_id=%s path=%s error=%s",
            request_id,
            path,
            result.error,
        )


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
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="imported",
                    search_filetype_override=upgrade_override,
                    min_bitrate=min_br_kbps,
                ),
            )
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
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="imported",
                    search_filetype_override=lossless_override,
                    min_bitrate=min_br_kbps,
                ),
            )
            logger.info(
                f"QUALITY GATE: {label} "
                f"min_bitrate={min_br_kbps}kbps CBR, not verified lossless — "
                f"searching for lossless to verify")
        else:  # accept
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_imported(
                    from_status="imported",
                    min_bitrate=min_br_kbps,
                    search_filetype_override=None,  # done searching
                ),
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
    cfg: "CratediggerConfig | None" = None,
    outcome_label: str = "success",
    requeue_on_failure: bool = True,
    cooled_down_users: set[str] | None = None,
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

    mode = (
        "FORCE-IMPORT" if force
        else "MANUAL-IMPORT" if scenario == "manual_import"
        else "AUTO-IMPORT"
    )
    logger.info(f"{mode}: {label} "
                f"(source=request, dist={distance:.4f})")

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
            #   ``run_preimport_gates`` re-runs on retry and
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

        try:
            try:
                request_row = db.get_request(request_id)
            except Exception:
                logger.debug(
                    "Failed to read current V0 probe state before import",
                    exc_info=True,
                )
                request_row = None
            existing_v0_probe = _current_lossless_v0_probe_from_request(
                request_row)
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
            )
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
                    if decision in ("import", "preflight_existing"):
                        if prev_br is not None or new_br is not None:
                            try:
                                transitions.finalize_request(
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

                if action.requeue and (requeue_on_failure or not action.record_rejection):
                    requeue_fields: dict[str, object] = {
                        "search_filetype_override": QUALITY_UPGRADE_TIERS,
                    }
                    if action.mark_done and new_br is not None:
                        requeue_fields["min_bitrate"] = new_br
                    transitions.finalize_request(
                        db,
                        request_id,
                        transitions.RequestTransition.to_wanted_fields(
                            from_status="imported",
                            fields=requeue_fields,
                        ),
                    )

                if action.run_quality_gate:
                    _check_quality_gate_core(
                        mb_id=mb_release_id,
                        label=label,
                        request_id=request_id,
                        files=list(file_list),
                        db=db,
                        quality_ranks=cfg.quality_ranks if cfg is not None else None,
                    )
                if action.trigger_notifiers and cfg is not None:
                    _trigger_meelo(cfg)
                    _trigger_plex(cfg, ir.postflight.imported_path)
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

    Runs the same pre-import gates (audio integrity + spectral transcode
    detection) as the auto path via ``lib.preimport.run_preimport_gates``
    — only the beets *distance* check is skipped when ``force=True``.
    All other quality checks (downgrade prevention, quality gate, meelo scan,
    denylist) run identically to auto-import.

    Concurrency (issue #92): a per-``request_id`` advisory lock (IMPORT
    namespace) is taken up front. Two concurrent force/manual imports
    on the same request (double-click in the UI, racing CLI
    invocations) would otherwise each run the full pipeline and write
    duplicate ``download_log`` rows. The second caller fast-fails
    without side effects. ``dispatch_import_core`` below will acquire
    the RELEASE lock as the inner nested acquisition. See
    ``docs/advisory-locks.md`` for namespaces, ordering, and the
    call-site index.

    Args:
        db: PipelineDB instance
        request_id: Album request ID
        failed_path: Path to the files on disk
        force: Pass --force to import_one.py (bypass distance check)
        outcome_label: download_log outcome string (e.g. "force_import", "manual_import")
        source_username: Original Soulseek username for force-import audit/denylist flows
    """
    from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT

    with db.advisory_lock(ADVISORY_LOCK_NAMESPACE_IMPORT, request_id) as acquired:
        if not acquired:
            mode = "FORCE-IMPORT" if force else "MANUAL-IMPORT"
            logger.warning(
                f"{mode} SKIPPED: request {request_id} — "
                f"another import is already in progress")
            return DispatchOutcome(
                success=False,
                message=f"Another import is already in progress for request {request_id}",
            )
        return _dispatch_import_from_db_locked(
            db, request_id, failed_path,
            force=force,
            outcome_label=outcome_label,
            source_username=source_username,
        )


def _dispatch_import_from_db_locked(
    db: "PipelineDB",
    request_id: int,
    failed_path: str,
    *,
    force: bool,
    outcome_label: str,
    source_username: str | None,
) -> "DispatchOutcome":
    """Body of dispatch_import_from_db, called once the advisory lock is held."""
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

    label = f"{req.get('artist_name', '')} - {req.get('album_title', '')}"

    # --- Shared pre-import gates (audio + spectral) ---
    # Force only skips the beets distance check (--force in import_one.py);
    # audio integrity and spectral transcode detection always run so a
    # force/manual import can't quietly replace an existing copy with a
    # transcode the auto path would have rejected.
    #
    # Repair MP3 headers BEFORE inspect_local_files — broken headers can make
    # mutagen fail to read bitrate_mode, leaving download_is_vbr=None. Treated
    # as CBR by the spectral gate, that would falsely reject a VBR album
    # force-imported here (the auto path only gets headers repaired before
    # validate_audio, but inspection metadata there comes from slskd, not
    # filesystem scan — so the problem is unique to force/manual). Idempotent:
    # run_preimport_gates still calls repair_mp3_headers internally.
    try:
        repair_mp3_headers(failed_path)
    except Exception:
        logger.debug("pre-inspect mp3 header repair failed", exc_info=True)
    inspection = inspect_local_files(failed_path)

    # --- Reject nested-folder layouts early ---
    # The preimport gates (validate_audio / analyze_album / repair_mp3_headers)
    # recurse, but the downstream harness (harness/import_one.py) still uses
    # os.listdir for bitrate measurement and conversion. A nested force/manual
    # import would pass gates and then produce an empty/misclassified
    # measurement — better to fail fast with a clear message so the user can
    # flatten the folder themselves. Auto-path downloads are already
    # flattened by process_completed_album, so this only affects force/manual.
    if inspection.has_nested_audio:
        mode = "FORCE-IMPORT" if force else "MANUAL-IMPORT"
        detail = ("Audio files are in subdirectories — flatten the folder "
                  "before import (multi-disc layouts are not supported here).")
        logger.warning(f"{mode} REJECTED (nested layout): {label} — {detail}")
        _record_rejection_and_maybe_requeue(
            db, request_id, DownloadInfo(username=source_username),
            distance=0.0,
            scenario="nested_layout",
            detail=detail,
            error=None,
            requeue=False,
            # outcome="rejected" — force_import/manual_import are reserved for
            # SUCCESSFUL imports (see CLAUDE.md). The /api/pipeline/log "imported"
            # counter filters on outcome IN ('success','force_import'), so tagging
            # a rejection as force_import mis-counts it as imported. Source
            # attribution for rejections is available via download_log.soulseek_username
            # + the surrounding request row.
            outcome_label="rejected",
            validation_result=ValidationResult(
                distance=0.0,
                scenario="nested_layout",
                detail=detail,
                failed_path=failed_path,
            ).to_json(),
            staged_path=failed_path,
        )
        return DispatchOutcome(success=False, message=detail)
    # download_log.soulseek_username can be a comma-joined list of peers for
    # multi-source downloads. Split before denylisting so a spectral reject
    # blocks each real peer, not the literal combined string.
    source_usernames: set[str] = {
        u.strip() for u in (source_username or "").split(",") if u.strip()
    }
    preimport = run_preimport_gates(
        path=failed_path,
        mb_release_id=mbid,
        label=label,
        download_filetype=inspection.filetype,
        download_min_bitrate_bps=inspection.min_bitrate_bps,
        download_is_vbr=inspection.is_vbr,
        cfg=cfg,
        db=db,
        request_id=request_id,
        usernames=source_usernames,
        # Don't propagate the download's spectral into on-disk state on the
        # force/manual path: if dispatch_import_core subsequently fails
        # (downgrade, no JSON, timeout), the DB would be left claiming the
        # failed download is on-disk. The auto path is safe to propagate
        # because the spectral decision happens immediately before the
        # import subprocess and a failure there still means the files
        # ended up in failed_imports/ unchanged.
        propagate_download_to_existing=False,
        # Reuse the inspection computed for the nested-layout gate to
        # avoid a second mutagen walk (~100ms per album).
        precomputed_inspection=inspection,
    )

    if not preimport.valid:
        mode = "FORCE-IMPORT" if force else "MANUAL-IMPORT"
        logger.warning(
            f"{mode} REJECTED (preimport gate): {label} "
            f"scenario={preimport.scenario} detail={preimport.detail}")

        dl_info = DownloadInfo(
            username=source_username,
            filetype=inspection.filetype or None,
            bitrate=inspection.min_bitrate_bps,
            is_vbr=inspection.is_vbr,
            download_spectral=preimport.download_spectral,
            current_spectral=preimport.existing_spectral,
            existing_min_bitrate=preimport.existing_min_bitrate,
        )
        _record_rejection_and_maybe_requeue(
            db, request_id, dl_info,
            distance=0.0,
            scenario=preimport.scenario or "preimport_reject",
            detail=preimport.detail,
            error=None,
            requeue=False,
            # outcome="rejected" — force_import/manual_import are reserved for
            # SUCCESSFUL imports (see CLAUDE.md). Tagging a gate-rejection as
            # force_import would mis-count it as imported in the UI's "imported"
            # counter (web/routes/pipeline.py and lib/pipeline_db.py::get_log).
            outcome_label="rejected",
            validation_result=ValidationResult(
                distance=0.0,
                scenario=preimport.scenario or "preimport_reject",
                detail=preimport.detail,
                failed_path=failed_path,
                corrupt_files=list(preimport.corrupt_files),
            ).to_json(),
            staged_path=failed_path,
        )
        return DispatchOutcome(
            success=False,
            message=f"Pre-import gate rejected: {preimport.detail or preimport.scenario}")

    # Compute override from DB state — grade-aware: current_spectral_bitrate only
    # lowers the override when current_spectral_grade is suspect/likely_transcode.
    # Re-read the request row so we pick up the measured existing spectral that
    # run_preimport_gates just wrote via _persist_spectral_state. (Force/manual
    # paths pass propagate_download_to_existing=False, so no download-as-proxy
    # propagation happens — we only pick up what beets actually measured.)
    req = db.get_request(request_id) or req
    override_min_bitrate = compute_effective_override_bitrate(
        req.get("min_bitrate"),
        req.get("current_spectral_bitrate"),
        req.get("current_spectral_grade"))

    return dispatch_import_core(
        path=failed_path,
        mb_release_id=mbid,
        request_id=request_id,
        label=label,
        force=force,
        override_min_bitrate=override_min_bitrate,
        target_format=req.get("target_format"),
        verified_lossless_target=cfg.verified_lossless_target,
        beets_harness_path=cfg.beets_harness_path,
        db=db,
        dl_info=DownloadInfo(
            username=source_username,
            filetype=inspection.filetype or None,
            bitrate=inspection.min_bitrate_bps,
            is_vbr=inspection.is_vbr,
            download_spectral=preimport.download_spectral,
            current_spectral=preimport.existing_spectral,
            existing_min_bitrate=preimport.existing_min_bitrate,
        ),
        distance=0.0,
        scenario="force_import" if force else "manual_import",
        files=files,
        cfg=cfg,
        outcome_label=outcome_label,
        requeue_on_failure=False,
    )
