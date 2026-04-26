"""Unified import preview service.

Preview answers the operator's "would this import?" question without beets,
pipeline DB, queue, denylist, or source-folder mutation. Real-folder preview
uses the same preimport gates and import_one.py harness protocol as force/manual
import, but runs both against isolated temporary copies.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from typing import Any

import msgspec

from lib.import_dispatch import run_import_one
from lib.preimport import inspect_local_files, run_preimport_gates
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    QualityRankConfig,
    compute_effective_override_bitrate,
    full_pipeline_decision,
    quality_gate_decision,
)
from lib.util import repair_mp3_headers, resolve_failed_path


class ImportPreviewValues(msgspec.Struct, frozen=True):
    """Typed values for synthetic import-preview simulation."""

    is_flac: bool = False
    min_bitrate: int | None = None
    is_cbr: bool = False
    is_vbr: bool | None = None
    avg_bitrate: int | None = None
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    existing_min_bitrate: int | None = None
    existing_avg_bitrate: int | None = None
    existing_spectral_bitrate: int | None = None
    existing_spectral_grade: str | None = None
    override_min_bitrate: int | None = None
    existing_format: str | None = None
    existing_is_cbr: bool = False
    post_conversion_min_bitrate: int | None = None
    converted_count: int = 0
    verified_lossless: bool = False
    verified_lossless_target: str | None = None
    target_format: str | None = None
    new_format: str | None = None
    audio_check_mode: str = "normal"
    audio_corrupt: bool = False
    import_mode: str = "auto"
    has_nested_audio: bool = False


class ImportPreviewResult(msgspec.Struct):
    """Common preview result returned by CLI/API/triage code."""

    mode: str
    verdict: str
    would_import: bool = False
    confident_reject: bool = False
    uncertain: bool = False
    cleanup_eligible: bool = False
    decision: str | None = None
    reason: str | None = None
    detail: str | None = None
    stage_chain: list[str] = []
    request_id: int | None = None
    download_log_id: int | None = None
    source_path: str | None = None
    import_result: ImportResult | None = None
    simulation: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return msgspec.to_builtins(self)  # type: ignore[no-any-return]

    def to_json(self) -> str:
        return msgspec.json.encode(self).decode()


def _preview_result(
    *,
    mode: str,
    verdict: str,
    decision: str | None = None,
    reason: str | None = None,
    detail: str | None = None,
    stage_chain: list[str] | None = None,
    request_id: int | None = None,
    download_log_id: int | None = None,
    source_path: str | None = None,
    import_result: ImportResult | None = None,
    simulation: dict[str, Any] | None = None,
    cleanup_eligible: bool = False,
) -> ImportPreviewResult:
    would_import = verdict == "would_import"
    confident_reject = verdict == "confident_reject"
    uncertain = verdict == "uncertain"
    return ImportPreviewResult(
        mode=mode,
        verdict=verdict,
        would_import=would_import,
        confident_reject=confident_reject,
        uncertain=uncertain,
        cleanup_eligible=cleanup_eligible if confident_reject else False,
        decision=decision,
        reason=reason or decision,
        detail=detail,
        stage_chain=stage_chain or [],
        request_id=request_id,
        download_log_id=download_log_id,
        source_path=source_path,
        import_result=import_result,
        simulation=simulation,
    )


def _stage_chain_from_simulation(simulation: dict[str, Any]) -> list[str]:
    chain: list[str] = []
    for key in (
        "preimport_nested",
        "preimport_audio",
        "stage0_spectral_gate",
        "stage1_spectral",
        "stage2_import",
        "stage3_quality_gate",
    ):
        value = simulation.get(key)
        if value is not None:
            chain.append(f"{key}:{value}")
    return chain


_IMPORT_STAGE_DECISIONS: frozenset[str] = frozenset({
    "import",
    "preflight_existing",
    "transcode_upgrade",
    "transcode_first",
})
_REJECT_STAGE_DECISIONS: frozenset[str] = frozenset({
    "downgrade",
    "transcode_downgrade",
})
_QUALITY_GATE_REQUEUE_DECISIONS: frozenset[str] = frozenset({
    "requeue_upgrade",
    "requeue_lossless",
})


def _classify_import_stages(
    stage2: object,
    stage3: object,
    *,
    imported: bool,
) -> tuple[str, bool, str | None]:
    stage2_decision = str(stage2) if isinstance(stage2, str) else None
    stage3_decision = str(stage3) if isinstance(stage3, str) else None

    if stage2_decision in _REJECT_STAGE_DECISIONS:
        return "confident_reject", True, stage2_decision

    if stage2_decision in _IMPORT_STAGE_DECISIONS or imported:
        reason = (
            stage3_decision
            if stage3_decision in _QUALITY_GATE_REQUEUE_DECISIONS
            else stage2_decision or stage3_decision or "import"
        )
        return "would_import", False, reason

    if stage3_decision in _QUALITY_GATE_REQUEUE_DECISIONS:
        return "uncertain", False, stage3_decision

    return "uncertain", False, stage2_decision or stage3_decision or "unknown"


def _classify_simulation(simulation: dict[str, Any]) -> tuple[str, bool, str | None]:
    if simulation.get("preimport_nested") == "reject_nested":
        return "confident_reject", True, "nested_layout"
    if simulation.get("preimport_audio") == "reject_corrupt":
        return "confident_reject", True, "audio_corrupt"
    if simulation.get("stage1_spectral") == "reject":
        return "confident_reject", True, "spectral_reject"
    return _classify_import_stages(
        simulation.get("stage2_import"),
        simulation.get("stage3_quality_gate"),
        imported=bool(simulation.get("imported")),
    )


def preview_import_from_values(
    values: ImportPreviewValues,
    *,
    cfg: QualityRankConfig | None = None,
) -> ImportPreviewResult:
    """Preview a synthetic typed scenario through the shared simulator seam."""
    simulation = full_pipeline_decision(
        is_flac=values.is_flac,
        min_bitrate=values.min_bitrate or 0,
        is_cbr=values.is_cbr,
        is_vbr=values.is_vbr,
        avg_bitrate=values.avg_bitrate,
        spectral_grade=values.spectral_grade,
        spectral_bitrate=values.spectral_bitrate,
        existing_min_bitrate=values.existing_min_bitrate,
        existing_avg_bitrate=values.existing_avg_bitrate,
        existing_spectral_grade=values.existing_spectral_grade,
        existing_spectral_bitrate=values.existing_spectral_bitrate,
        override_min_bitrate=values.override_min_bitrate,
        existing_format=values.existing_format,
        existing_is_cbr=values.existing_is_cbr,
        post_conversion_min_bitrate=values.post_conversion_min_bitrate,
        converted_count=values.converted_count,
        verified_lossless=values.verified_lossless,
        verified_lossless_target=values.verified_lossless_target,
        target_format=values.target_format,
        new_format=values.new_format,
        audio_check_mode=values.audio_check_mode,
        audio_corrupt=values.audio_corrupt,
        import_mode=values.import_mode,
        has_nested_audio=values.has_nested_audio,
        cfg=cfg,
    )
    verdict, cleanup_eligible, reason = _classify_simulation(simulation)
    return _preview_result(
        mode="values",
        verdict=verdict,
        decision=reason,
        reason=reason,
        stage_chain=_stage_chain_from_simulation(simulation),
        simulation=simulation,
        cleanup_eligible=cleanup_eligible,
    )


def _validation_result_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _quality_gate_stage(measurement: AudioQualityMeasurement | None,
                        cfg: QualityRankConfig) -> str | None:
    if measurement is None:
        return None
    return quality_gate_decision(measurement, cfg=cfg)


def _classify_import_result(
    ir: ImportResult | None,
    *,
    cfg: QualityRankConfig,
) -> tuple[str, bool, str | None, list[str]]:
    if ir is None:
        return "uncertain", False, "no_json_result", ["harness:no_json_result"]
    decision = ir.decision or "unknown"
    chain = [f"stage2_import:{decision}"]
    gate: str | None = None
    if decision in ("import", "preflight_existing"):
        gate = _quality_gate_stage(ir.new_measurement, cfg)
        if gate is not None:
            chain.append(f"stage3_quality_gate:{gate}")
    if decision in ("conversion_failed", "target_conversion_failed"):
        return "uncertain", False, decision, chain
    verdict, cleanup_eligible, reason = _classify_import_stages(
        decision,
        gate if decision in ("import", "preflight_existing") else None,
        imported=decision in _IMPORT_STAGE_DECISIONS,
    )
    return verdict, cleanup_eligible, reason, chain


def _request_label(req: dict[str, Any]) -> str:
    return f"{req.get('artist_name', '')} - {req.get('album_title', '')}".strip(" -")


def preview_import_from_path(
    db: Any,
    *,
    request_id: int,
    path: str,
    force: bool = True,
    source_username: str | None = None,
    download_log_id: int | None = None,
) -> ImportPreviewResult:
    """Preview a real source folder with no source, beets, or DB mutation."""
    req = db.get_request(request_id)
    if not req:
        return _preview_result(
            mode="path",
            verdict="uncertain",
            decision="request_not_found",
            reason=f"Request {request_id} not found",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    mbid = str(req.get("mb_release_id") or "")
    if not mbid:
        return _preview_result(
            mode="path",
            verdict="uncertain",
            decision="missing_release_id",
            reason="No MusicBrainz release ID",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    if not os.path.isdir(path):
        return _preview_result(
            mode="path",
            verdict="uncertain",
            decision="path_missing",
            reason=f"Path not found: {path}",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    from lib.config import read_runtime_config

    cfg = read_runtime_config()
    temp_root = tempfile.mkdtemp(prefix="cratedigger-import-preview-")
    try:
        preview_path = os.path.join(
            temp_root,
            os.path.basename(os.path.abspath(path)) or "album",
        )
        shutil.copytree(path, preview_path)
        try:
            repair_mp3_headers(preview_path)
        except Exception:
            pass
        inspection = inspect_local_files(preview_path)
        if inspection.has_nested_audio:
            detail = (
                "Audio files are in subdirectories — flatten the folder "
                "before import."
            )
            return _preview_result(
                mode="path",
                verdict="confident_reject",
                decision="nested_layout",
                reason="nested_layout",
                detail=detail,
                stage_chain=["preimport_nested:reject_nested"],
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                cleanup_eligible=True,
            )

        preimport = run_preimport_gates(
            path=preview_path,
            mb_release_id=mbid,
            label=_request_label(req),
            download_filetype=inspection.filetype,
            download_min_bitrate_bps=inspection.min_bitrate_bps,
            download_is_vbr=inspection.is_vbr,
            cfg=cfg,
            db=None,
            request_id=None,
            usernames=set(),
            propagate_download_to_existing=False,
            precomputed_inspection=inspection,
        )
        if not preimport.valid:
            scenario = preimport.scenario or "preimport_reject"
            return _preview_result(
                mode="path",
                verdict="confident_reject",
                decision=scenario,
                reason=scenario,
                detail=preimport.detail,
                stage_chain=[f"preimport:{scenario}"],
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                cleanup_eligible=True,
            )

        existing_spectral = preimport.existing_spectral
        existing_grade = existing_spectral.grade if existing_spectral else req.get("current_spectral_grade")
        existing_bitrate = (
            existing_spectral.bitrate_kbps
            if existing_spectral is not None
            else req.get("current_spectral_bitrate")
        )
        override_min_bitrate = compute_effective_override_bitrate(
            req.get("min_bitrate"),
            existing_bitrate if isinstance(existing_bitrate, int) else None,
            existing_grade if isinstance(existing_grade, str) else None,
        )

        run = run_import_one(
            path=preview_path,
            mb_release_id=mbid,
            request_id=None,
            force=force,
            preserve_source=True,
            dry_run=True,
            override_min_bitrate=override_min_bitrate,
            target_format=req.get("target_format"),
            verified_lossless_target=cfg.verified_lossless_target,
            beets_harness_path=cfg.beets_harness_path,
            quality_rank_config_json=cfg.quality_ranks.to_json(),
        )
        verdict, cleanup_eligible, reason, chain = _classify_import_result(
            run.import_result,
            cfg=cfg.quality_ranks,
        )
        return _preview_result(
            mode="path",
            verdict=verdict,
            decision=run.import_result.decision if run.import_result else reason,
            reason=reason,
            detail=run.import_result.error if run.import_result else "import_one.py emitted no JSON",
            stage_chain=chain,
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
            import_result=run.import_result,
            cleanup_eligible=cleanup_eligible,
        )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def preview_import_from_download_log(
    db: Any,
    download_log_id: int,
) -> ImportPreviewResult:
    """Preview the failed source referenced by one download_log row."""
    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        return _preview_result(
            mode="download_log",
            verdict="uncertain",
            decision="download_log_not_found",
            reason=f"Download log entry {download_log_id} not found",
            download_log_id=download_log_id,
        )
    request_id_raw = entry.get("request_id")
    if not isinstance(request_id_raw, int):
        return _preview_result(
            mode="download_log",
            verdict="uncertain",
            decision="missing_request_id",
            reason="Download log row has no request_id",
            download_log_id=download_log_id,
        )
    vr = _validation_result_dict(entry.get("validation_result"))
    raw_path = vr.get("failed_path")
    if not isinstance(raw_path, str) or not raw_path:
        return _preview_result(
            mode="download_log",
            verdict="uncertain",
            decision="missing_failed_path",
            reason="Download log row has no failed_path",
            request_id=request_id_raw,
            download_log_id=download_log_id,
        )
    resolved = resolve_failed_path(raw_path)
    if resolved is None:
        return _preview_result(
            mode="download_log",
            verdict="uncertain",
            decision="path_missing",
            reason=f"Path not found: {raw_path}",
            request_id=request_id_raw,
            download_log_id=download_log_id,
            source_path=raw_path,
        )
    return preview_import_from_path(
        db,
        request_id=request_id_raw,
        path=resolved,
        force=True,
        source_username=entry.get("soulseek_username"),
        download_log_id=download_log_id,
    )
