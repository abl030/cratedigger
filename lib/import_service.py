"""Import service — extraction helpers for ImportResult JSON.

The subprocess execution and dispatch logic lives in lib/import_dispatch.py
(dispatch_import + dispatch_import_from_db). This module provides helpers for
extracting typed fields from ImportResult JSON blobs.
"""

from __future__ import annotations

import json
import logging

from lib.quality import ImportResult

logger = logging.getLogger("cratedigger")


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


