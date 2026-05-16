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
    legacy_current_lossless_v0_probe_from_request,
    load_or_backfill_current_evidence,
    lossless_source_v0_probe_from_metric,
    persist_candidate_evidence_from_import_result,
    request_current_owner,
    snapshot_audio_files,
)
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    MeasurementFailure,
    MeasurementFailureReason,
    QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
    QualityRankConfig,
    classify_full_pipeline_decision,
    classify_quality_import_stages,
    compute_effective_override_bitrate,
    full_pipeline_decision,
    quality_gate_decision,
)
from lib.util import repair_mp3_headers, resolve_failed_path

# Verdict values for `ImportPreviewResult.verdict`. After U5 the preview worker
# emits only the two new verdicts (`evidence_ready` / `measurement_failed`) when
# called in `worker_mode=True`; legacy callers (CLI inspector, wrong_match
# triage, values-mode synthetic preview) still receive `would_import` /
# `confident_reject` / `uncertain` from the classifier.
PREVIEW_VERDICT_WOULD_IMPORT = "would_import"
PREVIEW_VERDICT_CONFIDENT_REJECT = "confident_reject"
PREVIEW_VERDICT_UNCERTAIN = "uncertain"
PREVIEW_VERDICT_EVIDENCE_READY = "evidence_ready"
PREVIEW_VERDICT_MEASUREMENT_FAILED = "measurement_failed"


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
    """Common preview result returned by CLI/API/triage code.

    U5 added two new verdicts: ``evidence_ready`` and ``measurement_failed``.
    The preview worker (``scripts/import_preview_worker.py``) emits only these
    two in production after U5; legacy callers (CLI inspector, wrong-match
    triage, values-mode synthetic preview) continue to receive
    ``would_import`` / ``confident_reject`` / ``uncertain`` from the classifier.

    When ``verdict='measurement_failed'``, ``failure`` carries the typed
    ``MeasurementFailure`` payload that the preview worker passes to
    ``_record_preview_measurement_failed`` for self-healing finalize.
    """

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
    failure: MeasurementFailure | None = None

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
    failure: MeasurementFailure | None = None,
) -> ImportPreviewResult:
    would_import = verdict == PREVIEW_VERDICT_WOULD_IMPORT
    confident_reject = verdict == PREVIEW_VERDICT_CONFIDENT_REJECT
    uncertain = verdict == PREVIEW_VERDICT_UNCERTAIN
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
        failure=failure,
    )


def _measurement_failed_result(
    *,
    mode: str,
    reason: MeasurementFailureReason,
    decision: str,
    detail: str,
    source_path: str | None = None,
    request_id: int | None = None,
    download_log_id: int | None = None,
    import_result: ImportResult | None = None,
    stage_chain: list[str] | None = None,
) -> ImportPreviewResult:
    """Build a ``verdict='measurement_failed'`` preview result with typed payload."""
    payload = MeasurementFailure(
        reason=reason,
        detail=detail,
        source_path=source_path or "",
    )
    return _preview_result(
        mode=mode,
        verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
        decision=decision,
        reason=reason,
        detail=detail,
        stage_chain=stage_chain,
        request_id=request_id,
        download_log_id=download_log_id,
        source_path=source_path,
        import_result=import_result,
        failure=payload,
    )


def _evidence_ready_result(
    *,
    mode: str,
    decision: str,
    reason: str | None = None,
    detail: str | None = None,
    stage_chain: list[str] | None = None,
    request_id: int | None = None,
    download_log_id: int | None = None,
    source_path: str | None = None,
    import_result: ImportResult | None = None,
) -> ImportPreviewResult:
    """Build a ``verdict='evidence_ready'`` preview result.

    Used by the worker-mode entry point when preview successfully measured the
    candidate and persisted evidence. The importer (U6) reads the persisted
    evidence and decides accept/reject via ``preimport_decide``.
    """
    return _preview_result(
        mode=mode,
        verdict=PREVIEW_VERDICT_EVIDENCE_READY,
        decision=decision,
        reason=reason or decision,
        detail=detail,
        stage_chain=stage_chain,
        request_id=request_id,
        download_log_id=download_log_id,
        source_path=source_path,
        import_result=import_result,
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
    verdict, cleanup_eligible, reason = classify_full_pipeline_decision(simulation)
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
    verdict, cleanup_eligible, reason = classify_quality_import_stages(
        decision,
        gate if decision in ("import", "preflight_existing") else None,
        imported=decision in QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
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
    worker_mode: bool = False,
) -> ImportPreviewResult:
    """Preview a real source folder without mutating source files or beets.

    DB evidence persistence is opt-in for the async preview worker. Ad-hoc
    preview and cleanup authorization callers receive an audit/UI verdict only.

    When ``worker_mode=True`` (the preview-worker entry point), failed exits
    return ``verdict='measurement_failed'`` with a typed ``MeasurementFailure``
    payload, and successful exits return ``verdict='evidence_ready'``. The
    importer then reads the persisted evidence and decides. Legacy callers
    (CLI, wrong-match triage) keep ``worker_mode=False`` and receive the
    classifier's ``would_import`` / ``confident_reject`` / ``uncertain``
    verdicts unchanged.
    """
    req = db.get_request(request_id)
    if not req:
        if worker_mode:
            return _measurement_failed_result(
                mode="path",
                reason="request_not_found",
                decision="request_not_found",
                detail=f"Request {request_id} not found",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="request_not_found",
            reason=f"Request {request_id} not found",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    mbid = str(req.get("mb_release_id") or "")
    if not mbid:
        if worker_mode:
            return _measurement_failed_result(
                mode="path",
                reason="missing_release_id",
                decision="missing_release_id",
                detail="No MusicBrainz release ID",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="missing_release_id",
            reason="No MusicBrainz release ID",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    if not os.path.isdir(path):
        if worker_mode:
            return _measurement_failed_result(
                mode="path",
                reason="source_vanished",
                decision="path_missing",
                detail=f"Path not found: {path}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
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
            if worker_mode:
                return _measurement_failed_result(
                    mode="path",
                    reason="snapshot_stale",
                    decision="evidence_snapshot_failed",
                    detail=str(exc),
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                )
            return _preview_result(
                mode="path",
                verdict=PREVIEW_VERDICT_UNCERTAIN,
                decision="evidence_snapshot_failed",
                reason="evidence_snapshot_failed",
                detail=str(exc),
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )
        if not source_snapshot:
            # Empty source snapshot: in worker mode we still want to persist
            # the empty-fileset fact and let the importer reject — but evidence
            # persistence requires at least one file. For now mark
            # measurement_failed; U6 will pick this up via the evidence facts
            # once persist_candidate_evidence_from_import_result is extended
            # to allow zero-file snapshots.
            if worker_mode:
                return _measurement_failed_result(
                    mode="path",
                    reason="source_vanished",
                    decision="evidence_empty_fileset",
                    detail="no audio files found",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                )
            return _preview_result(
                mode="path",
                verdict=PREVIEW_VERDICT_UNCERTAIN,
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
            if worker_mode:
                # In worker mode the importer (U6) decides reject from the
                # persisted ``folder_layout='nested'`` fact. The evidence
                # persistence pipeline will populate that field. We surface
                # ``evidence_ready`` here so the importer claims the job;
                # without persistence (no source_snapshot), we cannot write
                # evidence and the importer would have no facts — fall back
                # to ``measurement_failed`` in that subcase.
                if persist_candidate_evidence and source_snapshot:
                    # Persist nested-layout evidence and mark evidence_ready.
                    return _evidence_ready_result(
                        mode="path",
                        decision="nested_layout",
                        reason="nested_layout",
                        detail=detail,
                        stage_chain=["preimport_nested:reject_nested"],
                        request_id=request_id,
                        download_log_id=download_log_id,
                        source_path=path,
                    )
                return _measurement_failed_result(
                    mode="path",
                    reason="materialization_error",
                    decision="nested_layout",
                    detail=detail,
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    stage_chain=["preimport_nested:reject_nested"],
                )
            return _preview_result(
                mode="path",
                verdict=PREVIEW_VERDICT_CONFIDENT_REJECT,
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
            if worker_mode:
                # Preview no longer makes the reject decision. Persist the
                # measurement facts; the importer (U6) reads the evidence and
                # invokes ``preimport_decide`` to reject. We mark evidence_ready
                # when persistence is plausible; otherwise surface
                # measurement_failed so the parent request self-heals.
                if persist_candidate_evidence and source_snapshot:
                    return _evidence_ready_result(
                        mode="path",
                        decision=scenario,
                        reason=scenario,
                        detail=preimport.detail,
                        stage_chain=[f"preimport:{scenario}"],
                        request_id=request_id,
                        download_log_id=download_log_id,
                        source_path=path,
                    )
                return _measurement_failed_result(
                    mode="path",
                    reason="measurement_crashed",
                    decision=scenario,
                    detail=preimport.detail or scenario,
                    stage_chain=[f"preimport:{scenario}"],
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                )
            return _preview_result(
                mode="path",
                verdict=PREVIEW_VERDICT_CONFIDENT_REJECT,
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
                    beets_library_root=getattr(cfg, "beets_directory", ""),
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

        existing_v0_probe = legacy_current_lossless_v0_probe_from_request(req)
        if current_evidence is not None and current_evidence.v0_metric is not None:
            existing_v0_probe = lossless_source_v0_probe_from_metric(
                current_evidence.v0_metric
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
                detail = "source files changed while preview was running"
                if worker_mode:
                    return _measurement_failed_result(
                        mode="path",
                        reason="snapshot_stale",
                        decision="source_changed_during_preview",
                        detail=detail,
                        request_id=request_id,
                        download_log_id=download_log_id,
                        source_path=path,
                        import_result=run.import_result,
                    )
                return _preview_result(
                    mode="path",
                    verdict=PREVIEW_VERDICT_UNCERTAIN,
                    decision="source_changed_during_preview",
                    reason="source_changed_during_preview",
                    detail=detail,
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
                if worker_mode:
                    return _measurement_failed_result(
                        mode="path",
                        reason="evidence_persist_failed",
                        decision=f"evidence_{evidence_status}",
                        detail=evidence_reason or f"evidence_{evidence_status}",
                        request_id=request_id,
                        download_log_id=download_log_id,
                        source_path=path,
                        import_result=run.import_result,
                    )
                return _preview_result(
                    mode="path",
                    verdict=PREVIEW_VERDICT_UNCERTAIN,
                    decision=f"evidence_{evidence_status}",
                    reason=f"evidence_{evidence_status}",
                    detail=evidence_reason,
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
        final_decision = (
            run.import_result.decision if run.import_result else reason
        )
        final_detail = (
            run.import_result.error
            if run.import_result and run.import_result.error
            else evidence_reason
            if evidence_status in {"failed", "incomplete", "empty_fileset"}
            else "import_one.py emitted no JSON"
            if run.import_result is None
            else None
        )
        if worker_mode:
            # Worker mode: only emit evidence_ready or measurement_failed.
            # ``_classify_import_result`` returns "uncertain" when the
            # harness emitted no JSON OR when conversion failed — both are
            # measurement failures (the harness blew up). Anything else
            # (would_import / confident_reject) means the harness produced
            # facts we already persisted as evidence; the importer decides.
            if run.import_result is None:
                return _measurement_failed_result(
                    mode="path",
                    reason="measurement_crashed",
                    decision=final_decision or "no_json_result",
                    detail=final_detail or "import_one.py emitted no JSON",
                    stage_chain=chain,
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
            if verdict == PREVIEW_VERDICT_UNCERTAIN:
                # conversion_failed / target_conversion_failed
                return _measurement_failed_result(
                    mode="path",
                    reason="measurement_crashed",
                    decision=final_decision or reason or "uncertain",
                    detail=final_detail or reason or "measurement uncertain",
                    stage_chain=chain,
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
            # Both would_import and confident_reject map to evidence_ready
            # under the new contract; the importer reads the persisted
            # evidence and decides.
            return _evidence_ready_result(
                mode="path",
                decision=final_decision or reason or "evidence_ready",
                reason=reason,
                detail=final_detail,
                stage_chain=chain,
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
            )
        return _preview_result(
            mode="path",
            verdict=verdict,
            decision=final_decision,
            reason=reason,
            detail=final_detail,
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
    *,
    persist_candidate_evidence: bool = False,
    worker_mode: bool = False,
) -> ImportPreviewResult:
    """Preview the failed source referenced by one download_log row.

    When ``worker_mode=True`` (force/manual-import preview-worker entry), the
    four sanity-check exits return ``verdict='measurement_failed'`` with a
    typed payload. Legacy callers (wrong-match triage, ad-hoc CLI inspection)
    keep ``worker_mode=False`` and receive the legacy verdicts.
    """
    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        if worker_mode:
            return _measurement_failed_result(
                mode="download_log",
                reason="download_log_not_found",
                decision="download_log_not_found",
                detail=f"Download log entry {download_log_id} not found",
                download_log_id=download_log_id,
            )
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="download_log_not_found",
            reason=f"Download log entry {download_log_id} not found",
            download_log_id=download_log_id,
        )
    request_id_raw = entry.get("request_id")
    if not isinstance(request_id_raw, int):
        if worker_mode:
            return _measurement_failed_result(
                mode="download_log",
                reason="request_not_found",
                decision="missing_request_id",
                detail="Download log row has no request_id",
                download_log_id=download_log_id,
            )
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="missing_request_id",
            reason="Download log row has no request_id",
            download_log_id=download_log_id,
        )
    vr = _validation_result_dict(entry.get("validation_result"))
    raw_path = vr.get("failed_path")
    if not isinstance(raw_path, str) or not raw_path:
        if worker_mode:
            return _measurement_failed_result(
                mode="download_log",
                reason="missing_failed_path",
                decision="missing_failed_path",
                detail="Download log row has no failed_path",
                request_id=request_id_raw,
                download_log_id=download_log_id,
            )
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="missing_failed_path",
            reason="Download log row has no failed_path",
            request_id=request_id_raw,
            download_log_id=download_log_id,
        )
    resolved = resolve_failed_path(raw_path)
    if resolved is None:
        if worker_mode:
            return _measurement_failed_result(
                mode="download_log",
                reason="source_vanished",
                decision="path_missing",
                detail=f"Path not found: {raw_path}",
                request_id=request_id_raw,
                download_log_id=download_log_id,
                source_path=raw_path,
            )
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
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
        persist_candidate_evidence=persist_candidate_evidence,
        worker_mode=worker_mode,
    )
