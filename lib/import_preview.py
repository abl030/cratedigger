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
from lib.quality_evidence import (
    audio_snapshot_matches,
    load_or_backfill_current_evidence,
    persist_candidate_evidence_from_import_result,
    request_current_owner,
    snapshot_audio_files,
)
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    QualityRankConfig,
    compute_effective_override_bitrate,
    full_pipeline_decision,
    quality_gate_decision,
    V0_PROBE_LOSSLESS_SOURCE,
    V0ProbeEvidence,
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
    candidate_v0_probe_avg: int | None = None
    candidate_v0_probe_min: int | None = None
    existing_v0_probe_avg: int | None = None
    candidate_v0_probe_kind: str | None = None
    existing_v0_probe_kind: str | None = None
    supported_lossless_source: bool | None = None


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
    "provisional_lossless_upgrade",
})
_REJECT_STAGE_DECISIONS: frozenset[str] = frozenset({
    "downgrade",
    "transcode_downgrade",
    "suspect_lossless_downgrade",
    "suspect_lossless_probe_missing",
    "lossless_source_locked",
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
    if (simulation.get("stage1_spectral") == "reject"
            and not simulation.get("stage2_import")):
        return "confident_reject", True, "spectral_reject"
    return _classify_import_stages(
        simulation.get("stage2_import"),
        simulation.get("stage3_quality_gate"),
        imported=bool(simulation.get("imported")),
    )


def _current_lossless_v0_probe(req: dict[str, Any]) -> V0ProbeEvidence | None:
    avg = req.get("current_lossless_source_v0_probe_avg_bitrate")
    if not isinstance(avg, int):
        return None
    min_br = req.get("current_lossless_source_v0_probe_min_bitrate")
    median_br = req.get("current_lossless_source_v0_probe_median_bitrate")
    return V0ProbeEvidence(
        kind=V0_PROBE_LOSSLESS_SOURCE,
        min_bitrate_kbps=min_br if isinstance(min_br, int) else None,
        avg_bitrate_kbps=avg,
        median_bitrate_kbps=median_br if isinstance(median_br, int) else None,
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
        candidate_v0_probe_avg=values.candidate_v0_probe_avg,
        candidate_v0_probe_min=values.candidate_v0_probe_min,
        existing_v0_probe_avg=values.existing_v0_probe_avg,
        candidate_v0_probe_kind=values.candidate_v0_probe_kind,
        existing_v0_probe_kind=values.existing_v0_probe_kind,
        supported_lossless_source=values.supported_lossless_source,
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
    import_job_id: int | None = None,
    persist_candidate_evidence: bool = False,
) -> ImportPreviewResult:
    """Preview a real source folder without mutating source files or beets.

    DB evidence persistence is opt-in for the async preview worker. Ad-hoc
    preview and cleanup authorization callers receive an audit/UI verdict only.
    """
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
    source_snapshot = None
    if persist_candidate_evidence:
        try:
            source_snapshot = snapshot_audio_files(path)
        except OSError as exc:
            return _preview_result(
                mode="path",
                verdict="uncertain",
                decision="evidence_snapshot_failed",
                reason="evidence_snapshot_failed",
                detail=str(exc),
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )
        if not source_snapshot:
            return _preview_result(
                mode="path",
                verdict="uncertain",
                decision="evidence_empty_fileset",
                reason="evidence_empty_fileset",
                detail="no audio files found",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )

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
        current_evidence = None
        try:
            current_evidence = db.load_album_quality_evidence(
                request_current_owner(request_id)
            )
        except Exception:
            current_evidence = None
        if persist_candidate_evidence and current_evidence is None:
            try:
                current_result = load_or_backfill_current_evidence(
                    db,
                    request_id=request_id,
                    mb_release_id=mbid,
                    quality_ranks=cfg.quality_ranks,
                )
                current_evidence = current_result.evidence
            except Exception:
                current_evidence = None
        if current_evidence is not None:
            current_m = current_evidence.measurement
            existing_grade = current_m.spectral_grade
            existing_bitrate = current_m.spectral_bitrate_kbps
        override_min_bitrate = compute_effective_override_bitrate(
            (
                current_evidence.measurement.min_bitrate_kbps
                if current_evidence is not None
                else req.get("min_bitrate")
            ),
            existing_bitrate if isinstance(existing_bitrate, int) else None,
            existing_grade if isinstance(existing_grade, str) else None,
        )

        existing_v0_probe = _current_lossless_v0_probe(req)
        if current_evidence is not None and current_evidence.v0_metric is not None:
            metric = current_evidence.v0_metric
            if metric.source_lineage == "lossless_source":
                existing_v0_probe = V0ProbeEvidence(
                    kind=V0_PROBE_LOSSLESS_SOURCE,
                    min_bitrate_kbps=metric.min_bitrate_kbps,
                    avg_bitrate_kbps=metric.avg_bitrate_kbps,
                    median_bitrate_kbps=metric.median_bitrate_kbps,
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
            existing_v0_probe=existing_v0_probe,
        )
        verdict, cleanup_eligible, reason, chain = _classify_import_result(
            run.import_result,
            cfg=cfg.quality_ranks,
        )
        evidence_status: str | None = None
        evidence_reason: str | None = None
        if persist_candidate_evidence:
            if source_snapshot is None or not audio_snapshot_matches(path, source_snapshot):
                return _preview_result(
                    mode="path",
                    verdict="uncertain",
                    decision="source_changed_during_preview",
                    reason="source_changed_during_preview",
                    detail="source files changed while preview was running",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
            try:
                evidence = persist_candidate_evidence_from_import_result(
                    db,
                    source_path=path,
                    import_result=run.import_result,
                    download_log_id=download_log_id,
                    import_job_id=import_job_id,
                    target_format=req.get("target_format") if req else None,
                    files=source_snapshot,
                )
                evidence_status = evidence.status
                evidence_reason = evidence.reason
            except Exception as exc:
                evidence_status = "failed"
                evidence_reason = f"{type(exc).__name__}: {exc}"
            if evidence_status != "ready":
                return _preview_result(
                    mode="path",
                    verdict="uncertain",
                    decision=f"evidence_{evidence_status}",
                    reason=f"evidence_{evidence_status}",
                    detail=evidence_reason,
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
        return _preview_result(
            mode="path",
            verdict=verdict,
            decision=run.import_result.decision if run.import_result else reason,
            reason=reason,
            detail=(
                run.import_result.error
                if run.import_result and run.import_result.error
                else evidence_reason
                if evidence_status in {"failed", "incomplete", "empty_fileset"}
                else "import_one.py emitted no JSON"
                if run.import_result is None
                else None
            ),
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
