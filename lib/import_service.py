"""Unified import service — single entry point for running import_one.py.

Replaces 4 duplicated codepaths (force-import CLI/web, manual-import CLI/web)
with one run_import() + log_and_update() flow.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Any

from lib.quality import ImportResult

logger = logging.getLogger("soularr")


@dataclass(frozen=True)
class ImportOutcome:
    """Result of running import_one.py."""
    success: bool
    exit_code: int
    message: str
    import_result_json: str | None = None


def _apply_request_spectral_fields(
    fields: dict[str, object],
    *,
    grade: object,
    spectral_bitrate_kbps: object,
    min_bitrate_kbps: object,
    verified_lossless: bool,
) -> None:
    """Populate album_requests spectral fields from import measurements."""
    if grade is None and spectral_bitrate_kbps is None:
        return
    fields["last_download_spectral_grade"] = grade
    fields["last_download_spectral_bitrate"] = spectral_bitrate_kbps
    current_bitrate = spectral_bitrate_kbps
    if verified_lossless and isinstance(min_bitrate_kbps, int):
        current_bitrate = min_bitrate_kbps
    fields["current_spectral_grade"] = grade
    fields["current_spectral_bitrate"] = current_bitrate


def parse_import_result_stdout(stdout: str) -> str | None:
    """Extract ImportResult JSON from import_one.py stdout sentinel line."""
    for line in stdout.splitlines():
        if "__IMPORT_RESULT__" in line:
            try:
                return line.split("__IMPORT_RESULT__")[1].strip()
            except IndexError:
                return None
    return None


def extract_import_update_fields(import_result_json: str | None) -> dict[str, object]:
    """Extract DB update fields from ImportResult JSON.

    Handles both v2 (new_measurement) and v1 (quality/spectral sub-objects)
    formats. Returns a dict suitable for passing to update_status/apply_transition.
    """
    if not import_result_json:
        return {}
    try:
        ir = json.loads(import_result_json)
    except (json.JSONDecodeError, TypeError):
        return {}

    fields: dict[str, object] = {}
    new_m = ir.get("new_measurement") or {}
    if new_m:
        # v2 format
        verified_lossless = bool(new_m.get("verified_lossless"))
        _apply_request_spectral_fields(
            fields,
            grade=new_m.get("spectral_grade"),
            spectral_bitrate_kbps=new_m.get("spectral_bitrate_kbps"),
            min_bitrate_kbps=new_m.get("min_bitrate_kbps"),
            verified_lossless=verified_lossless,
        )
        if new_m.get("min_bitrate_kbps") is not None:
            fields["min_bitrate"] = new_m["min_bitrate_kbps"]
        if verified_lossless:
            fields["verified_lossless"] = True
    else:
        # v1 fallback
        spectral = ir.get("spectral", {})
        quality = ir.get("quality", {})
        conv = ir.get("conversion", {})
        verified_lossless = (
            conv.get("was_converted")
            and conv.get("original_filetype", "").lower() == "flac"
            and spectral.get("grade") == "genuine"
        )
        _apply_request_spectral_fields(
            fields,
            grade=spectral.get("grade"),
            spectral_bitrate_kbps=spectral.get("bitrate"),
            min_bitrate_kbps=quality.get("new_min_bitrate"),
            verified_lossless=bool(verified_lossless),
        )
        if quality.get("new_min_bitrate") is not None:
            fields["min_bitrate"] = quality["new_min_bitrate"]
        if verified_lossless:
            fields["verified_lossless"] = True

    return fields


def extract_import_log_fields(import_result_json: str | None) -> dict[str, object]:
    """Extract download_log fields from ImportResult JSON via typed dataclass."""
    if not import_result_json:
        return {}
    try:
        ir = ImportResult.from_json(import_result_json)
    except (json.JSONDecodeError, TypeError, KeyError):
        return {}

    fields: dict[str, object] = {}
    conv = ir.conversion
    new_m = ir.new_measurement
    existing_m = ir.existing_measurement

    if conv.was_converted:
        fields["was_converted"] = True
        fields["original_filetype"] = conv.original_filetype
        fields["filetype"] = conv.target_filetype
        fields["is_vbr"] = True
        fields["slskd_filetype"] = conv.original_filetype
        fields["actual_filetype"] = conv.target_filetype

    if new_m is not None:
        if new_m.min_bitrate_kbps is not None:
            fields["bitrate"] = new_m.min_bitrate_kbps * 1000
            fields["actual_min_bitrate"] = new_m.min_bitrate_kbps
        if new_m.spectral_grade is not None:
            fields["spectral_grade"] = new_m.spectral_grade
        if new_m.spectral_bitrate_kbps is not None:
            fields["spectral_bitrate"] = new_m.spectral_bitrate_kbps

    if existing_m is not None:
        if existing_m.min_bitrate_kbps is not None:
            fields["existing_min_bitrate"] = existing_m.min_bitrate_kbps
        if existing_m.spectral_bitrate_kbps is not None:
            fields["existing_spectral_bitrate"] = existing_m.spectral_bitrate_kbps

    return fields


def run_import(
    path: str,
    mb_release_id: str,
    *,
    request_id: int,
    import_one_path: str,
    force: bool = False,
    override_min_bitrate: int | None = None,
) -> ImportOutcome:
    """Run import_one.py and return a typed outcome.

    Unifies force-import and manual-import subprocess patterns.
    """
    if not os.path.isdir(path):
        return ImportOutcome(
            success=False, exit_code=3,
            message=f"Path not found: {path}",
        )

    cmd = [
        sys.executable, import_one_path,
        path, mb_release_id,
        "--request-id", str(request_id),
    ]
    if force:
        cmd.append("--force")
    if override_min_bitrate is not None:
        cmd.extend(["--override-min-bitrate", str(override_min_bitrate)])

    logger.info("IMPORT-SERVICE: running %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=1800,
            env={**os.environ, "HOME": "/home/abl030"},
        )
    except subprocess.TimeoutExpired:
        return ImportOutcome(
            success=False, exit_code=-1,
            message="import_one.py timed out after 30 minutes",
        )

    # Log stderr
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            logger.info("  [import] %s", line)

    import_result_json = parse_import_result_stdout(result.stdout or "")

    if result.returncode == 0:
        return ImportOutcome(
            success=True, exit_code=0,
            message="Import successful",
            import_result_json=import_result_json,
        )
    else:
        return ImportOutcome(
            success=False, exit_code=result.returncode,
            message=f"import_one.py exited with code {result.returncode}",
            import_result_json=import_result_json,
        )


def log_and_update_import(
    db: Any,
    request_id: int,
    outcome: ImportOutcome,
    *,
    outcome_label: str,
    staged_path: str | None = None,
) -> None:
    """Write download_log row and update album_requests status.

    Args:
        db: PipelineDB instance
        request_id: Album request ID
        outcome: Result from run_import()
        outcome_label: download_log outcome string (e.g. "force_import", "manual_import")
        staged_path: Path to staged files
    """
    from lib.transitions import apply_transition

    log_fields = extract_import_log_fields(outcome.import_result_json)
    db.log_download(
        request_id=request_id,
        outcome=outcome_label if outcome.success else "failed",
        import_result=outcome.import_result_json,
        staged_path=staged_path,
        error_message=None if outcome.success else outcome.message,
        **log_fields,
    )

    if outcome.success:
        update_fields = extract_import_update_fields(outcome.import_result_json)
        apply_transition(db, request_id, "imported", **update_fields)
