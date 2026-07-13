"""Small pure helpers for the dispatch package.

Cleanup, DownloadInfo assembly, ImportResult -> DownloadInfo population,
V0-probe log-field extraction, postflight logging, and the duplicate-remove
guard quarantine. No import decisions live here.
"""

from __future__ import annotations

import logging
import os
import shutil
from typing import TypedDict, TYPE_CHECKING

from lib.quality import DownloadInfo, SpectralMeasurement

from lib.dispatch.types import FORCE_MANUAL_SCENARIOS

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.grab_list import GrabListEntry
    from lib.quality import DispatchAction, ImportResult

logger = logging.getLogger("cratedigger")


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


class _V0ProbeLogFields(TypedDict):
    """V0-probe kwargs splatted into ``log_download`` — TypedDict so the
    unpack type-checks against the annotated signature per key."""

    v0_probe_kind: str | None
    v0_probe_min_bitrate: int | None
    v0_probe_avg_bitrate: int | None
    v0_probe_median_bitrate: int | None
    existing_v0_probe_kind: str | None
    existing_v0_probe_min_bitrate: int | None
    existing_v0_probe_avg_bitrate: int | None
    existing_v0_probe_median_bitrate: int | None


def _v0_probe_log_fields(dl_info: DownloadInfo) -> _V0ProbeLogFields:
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


def _populate_dl_info_from_import_result(dl_info: DownloadInfo,
                                         ir: ImportResult) -> None:
    """Populate a DownloadInfo from an ImportResult (pure, no I/O)."""
    conv = ir.conversion
    new_m = ir.new_measurement
    materialized_m = ir.materialized_measurement
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
    # ``actual_*`` means the materialized output, not the candidate/proxy
    # measurement used to authorize it. Legacy/non-mutating results without a
    # separate output retain the historical new_measurement fallback.
    actual_m = materialized_m or new_m
    if actual_m and actual_m.min_bitrate_kbps is not None:
        dl_info.bitrate = actual_m.min_bitrate_kbps * 1000
        dl_info.actual_min_bitrate = actual_m.min_bitrate_kbps
    if new_m:
        dl_info.download_spectral = SpectralMeasurement.from_parts(
            new_m.spectral_grade, new_m.spectral_bitrate_kbps)
        dl_info.verified_lossless_override = new_m.verified_lossless
    if existing_m:
        dl_info.current_spectral = SpectralMeasurement.from_parts(
            existing_m.spectral_grade, existing_m.spectral_bitrate_kbps)
        if existing_m.min_bitrate_kbps is not None:
            dl_info.existing_min_bitrate = existing_m.min_bitrate_kbps
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
