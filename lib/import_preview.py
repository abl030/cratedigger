"""Preview measurement and candidate-evidence persistence.

Foreign paths are descriptor-copied before media tools run. A completed album
under the private processing root is Cratedigger working state: it may be
normalized in place, measured, and imported as those exact bytes.

A Cratedigger-owned canonical processing album (``processing/albums/``) must
stay an exact media manifest at every moment — no preview JSON, action file,
or other control-plane artifact ever belongs inside it, whether or not
preview snapshots the directory first (issue #859). The action-time
evidence file every preview writes for the dry-run harness therefore always
lives outside the previewed directory, via the single shared writer in
``lib.evidence_action_file``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import msgspec

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.pipeline_db import BadAudioHashRow
    from lib.pipeline_db.rows import DownloadLogWithEvidenceRow

from lib.current_library_evidence import (
    CurrentLibraryAuthorityUnavailable,
    CurrentLibraryEvidence,
    CurrentLibraryEvidenceDB,
    authorize_current_evidence_for_preview,
    load_current_evidence_for_preview,
    persist_exact_current_spectral_from_attempt,
    resolve_current_library_evidence,
)
from lib.dispatch import run_import_one
from lib.dispatch.types import ImportOneRun
from lib.evidence_action_file import (
    remove_quality_evidence_action_file,
    write_quality_evidence_action_file,
)
from lib.fs_authority import (
    FilesystemAuthorityError,
    classify_path_errno,
    observe_directory,
    refusal_is_indeterminate,
)
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    checkpoint,
)
from lib.measurement import (
    AacLatticeMeasureFn,
    CdRipVerifyFn,
    ExistingSpectralResolver,
    LocalFileInspection,
    PreimportMeasurement,
    SpectralDetailAnalyzer,
    diagnostic_from_stderr,
    inspect_local_files,
    measure_aac_lattice,
    measure_preimport_state,
)
from lib.media_readiness import normalize_media_metadata
from lib.preview_snapshot import (
    remove_preview_snapshot,
    snapshot_authorized_directory,
    snapshot_configured_quarantine_directory,
)
from lib.processing_paths import (
    path_is_within_root,
    processing_albums_dir,
)
from lib.quality import (
    QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    AudioValidationMeasurementError,
    AudioValidationReport,
    ImportResult,
    MeasurementFailure,
    MeasurementFailureReason,
    QualityEvidenceActionPayload,
    QualityEvidenceActionProvenance,
    QualityRankConfig,
    SpectralAnalysisDetail,
    SpectralDetail,
    SpectralEvidenceFacts,
    SpectralInterpretation,
    SpectralMeasurement,
    TargetQualityContract,
    V0ProbeEvidence,
    classify_full_pipeline_decision,
    classify_quality_import_stages,
    compute_effective_override_bitrate,
    full_pipeline_decision,
    interpret_measurement,
    interpret_spectral_evidence,
    quality_gate_decision,
)
from lib.quality.gates import preimport_corrupt_outranks_nested
from lib.quality_evidence import (
    CandidateEvidencePersistenceReceipt,
    EvidenceBuildResult,
    audio_snapshot_matches,
    audit_v0_probe_from_metric,
    evidence_from_measurement,
    persist_candidate_evidence_from_import_result,
    persist_candidate_evidence_from_measurement,
    snapshot_audio_files,
)
from lib.validation_envelope import decode_validation_envelope

logger = logging.getLogger("cratedigger")

HeaderRepairFn = Callable[[str], None]
"""Repair one file's container header in place before it is measured."""


def prepare_preview_media(path: str) -> None:
    """Normalize only a ready private view; measurement owns invalid evidence."""
    normalize_media_metadata(path, fail_closed=False)


def _existing_spectral_interpretation(
    *,
    current_evidence: AlbumQualityEvidence | None,
    measured_existing: SpectralMeasurement | None,
    persisted_existing: SpectralAnalysisDetail,
) -> SpectralInterpretation:
    """Interpret the installed copy's spectral evidence in its codec's terms.

    Mirrors the grade/bitrate resolution order the override used to inline:
    linked current evidence wins, then this attempt's fresh measurement of
    the installed files, then the persisted source-subject detail. Issue #829
    Phase 5 PR2b: the override may only ever consume a codec-aware class, and
    the linked-evidence branch additionally resolves the album-level context
    (``storage_format``/``filetype_band``) the bare audit details lack.
    """
    if current_evidence is not None:
        return interpret_measurement(
            current_evidence.measurement,
            storage_format=current_evidence.storage_format,
            filetype_band=current_evidence.filetype_band,
        )
    source: SpectralMeasurement | SpectralAnalysisDetail = (
        measured_existing if measured_existing is not None else persisted_existing
    )
    return interpret_spectral_evidence(SpectralEvidenceFacts(
        spectral_grade=source.grade,
        codec_family=source.codec_family,
        cliff_hz=source.cliff_hz,
        spectral_bitrate_kbps=source.bitrate_kbps,
    ))




def _prefer_successful_spectral_detail(
    measured: SpectralAnalysisDetail | None,
    harness: SpectralAnalysisDetail | None,
) -> SpectralAnalysisDetail | None:
    """Prefer successful audit evidence; retain an error only as fallback."""
    if measured is not None and measured.attempted and measured.error is None:
        return measured
    if harness is not None and harness.attempted and harness.error is None:
        return harness
    if measured is not None and measured.attempted:
        return measured
    return harness


def compose_attempt_spectral_audit(
    measured: SpectralDetail,
    harness: SpectralDetail,
) -> SpectralDetail:
    """Compose IN from the best scan and HAVE from the preview measurement."""
    candidate = _prefer_successful_spectral_detail(
        measured.candidate, harness.candidate)
    existing = measured.existing
    return SpectralDetail(
        cliff_freq_hz=harness.cliff_freq_hz,
        suspect_pct=(candidate.suspect_pct or 0.0) if candidate else 0.0,
        per_track=list(candidate.per_track) if candidate else [],
        existing_suspect_pct=(
            existing.suspect_pct or 0.0 if existing else 0.0
        ),
        candidate=candidate,
        existing=existing,
    )


def _lossless_candidate_spectral_failure(
    measurement: PreimportMeasurement,
    *,
    lossless_candidate: bool,
) -> str | None:
    """Return diagnostics when lossless verification lacks a usable grade."""
    if not lossless_candidate:
        return None
    if measurement.cd_rip_verification is not None:
        return None
    candidate = measurement.spectral_audit.candidate
    if candidate is None or not candidate.attempted:
        return "lossless candidate spectral analysis did not run"
    if candidate.error:
        return candidate.error
    if candidate.grade in (None, "error"):
        return "lossless candidate spectral analysis returned no usable grade"
    return None


def _write_preview_spectral_evidence_file(
    *,
    mb_release_id: str,
    source_path: str,
    measurement: PreimportMeasurement,
    files: list[AlbumQualityEvidenceFile] | None,
    lossless_candidate: bool,
    cancellation_token: CancellationToken | None = None,
) -> str | None:
    """Carry preview-measured lossless spectral facts into the dry-run harness.

    Delegates the write to ``lib.evidence_action_file`` — the ONE
    tempfile-write implementation shared with the importer's action-time
    writer. Never writes into the previewed directory: a Cratedigger-owned
    canonical processing album under ``processing/albums/`` must stay an
    exact media manifest (issue #859 — a sidecar written in-directory
    poisoned ``_materialize_processing_dir``'s exact-manifest guard and
    stalled automation imports forever).
    """
    if not lossless_candidate:
        return None
    built = evidence_from_measurement(
        mb_release_id=mb_release_id,
        source_path=source_path,
        measurement=measurement,
        files=files,
    )
    if built.evidence is None:
        raise ValueError(
            built.reason or "could not encode preview spectral evidence"
        )
    payload = QualityEvidenceActionPayload(
        candidate=built.evidence,
        provenance=QualityEvidenceActionProvenance(
            candidate_status="preview_measured",
            snapshot_status="matched",
        ),
    )
    checkpoint(cancellation_token)
    return write_quality_evidence_action_file(payload)


def _cleanup_preview_artifacts(
    *,
    preview_spectral_file: str | None,
    temp_root: str | None,
    cfg: CratediggerConfig,
    cancellation_token: CancellationToken | None,
) -> None:
    """Clean owned preview artifacts only while execution remains authorized."""
    checkpoint(cancellation_token)
    remove_quality_evidence_action_file(preview_spectral_file)
    if temp_root is not None:
        remove_preview_snapshot(
            temp_root,
            cfg,
            cancellation_token=cancellation_token,
        )


@runtime_checkable
class ImportPreviewDB(CurrentLibraryEvidenceDB, Protocol):
    """The PipelineDB surface the preview entry points use (#409).

    Extends ``CurrentLibraryEvidenceDB`` because the handle is forwarded into
    the current-library (HAVE) evidence persisters, which in turn extends
    ``QualityEvidenceDB`` for the candidate persisters. Parity tests live in
    ``tests/test_import_preview.py``.

    The members added here are the preview lanes' own: the download-log read,
    plus the two bad-hash members, which exist because both lanes forward this
    handle into ``measure_preimport_state`` as its ``BadHashGateDB`` port —
    the curator bad-rip gate fires during preview measurement or not at all.
    """

    def get_download_log_entry(
        self, log_id: int,
    ) -> DownloadLogWithEvidenceRow | None: ...

    def has_any_bad_audio_hashes(self) -> bool: ...

    def lookup_bad_audio_hash(
        self,
        hash_value: bytes,
        audio_format: str,
    ) -> BadAudioHashRow | None: ...


# Verdict values for `ImportPreviewResult.verdict`. After U5 the
# measure-and-persist entry point (`measure_and_persist_candidate_evidence`)
# emits only the two new verdicts (`evidence_ready` / `measurement_failed`);
# the classify entry points (`preview_import_from_path` and friends — CLI
# inspector, wrong_match triage, values-mode synthetic preview) still return
# `would_import` / `confident_reject` / `uncertain` from the classifier.
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
    post_conversion_is_cbr: bool | None = None
    converted_count: int = 0
    candidate_verified_lossless_proof: bool = False
    verified_lossless_target: str | None = None
    target_format: str | None = None
    new_format: str | None = None
    audio_check_mode: str = "normal"
    audio_corrupt: bool = False
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
    # Force/quarantine previews retain this private copy through Beets.  It is
    # action data, not launch/audit authority; ``source_path`` remains the
    # original source reference.
    action_path: str | None = None
    import_result: ImportResult | None = None
    simulation: dict[str, Any] | None = None
    failure: MeasurementFailure | None = None
    candidate_evidence_receipt: CandidateEvidencePersistenceReceipt | None = None

    def to_dict(self) -> dict[str, Any]:
        return msgspec.to_builtins(self)

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
    candidate_evidence_receipt: CandidateEvidencePersistenceReceipt | None = None,
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
        candidate_evidence_receipt=candidate_evidence_receipt,
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
    subprocess_stderr: str | None = None,
    audio_validation: AudioValidationReport | None = None,
) -> ImportPreviewResult:
    """Build a ``verdict='measurement_failed'`` preview result with typed payload.

    ``subprocess_stderr`` is only the bounded fallback for a harness failure
    that returned no typed result. Normal validation and conversion failures
    carry ``AudioValidationReport`` / ``ConversionInfo`` instead.
    """
    full_detail = detail
    if subprocess_stderr:
        diagnostic = diagnostic_from_stderr(subprocess_stderr)
        if diagnostic:
            full_detail = f"{detail} | {diagnostic}"
            logger.warning(
                "measurement_failed decision=%s request_id=%s: %s",
                decision, request_id, diagnostic,
            )
    payload = MeasurementFailure(
        reason=reason,
        detail=full_detail,
        source_path=source_path or "",
        audio_validation=audio_validation,
    )
    return _preview_result(
        mode=mode,
        verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
        decision=decision,
        reason=reason,
        detail=full_detail,
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
    candidate_evidence_receipt: CandidateEvidencePersistenceReceipt | None = None,
) -> ImportPreviewResult:
    """Build a ``verdict='evidence_ready'`` preview result.

    Used by the worker-mode entry point when preview successfully measured the
    candidate and persisted evidence. The importer reads the persisted
    evidence and decides accept/reject via
    ``full_pipeline_decision_from_evidence`` (U11).
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
        candidate_evidence_receipt=candidate_evidence_receipt,
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
        post_conversion_is_cbr=values.post_conversion_is_cbr,
        converted_count=values.converted_count,
        candidate_verified_lossless_proof=(
            values.candidate_verified_lossless_proof
        ),
        verified_lossless_target=values.verified_lossless_target,
        target_format=values.target_format,
        new_format=values.new_format,
        audio_check_mode=values.audio_check_mode,
        audio_corrupt=values.audio_corrupt,
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


def _quality_gate_stage(
    measurement: AudioQualityMeasurement | None,
    cfg: QualityRankConfig,
    target_contract: TargetQualityContract | None = None,
    verified_lossless_proof: bool = False,
) -> str | None:
    if measurement is None:
        return None
    return quality_gate_decision(
        measurement,
        cfg=cfg,
        target_contract=target_contract,
        verified_lossless_proof=verified_lossless_proof,
    )


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
        gate = _quality_gate_stage(
            ir.source_measurement,
            cfg,
            ir.target_quality_contract,
            ir.verified_lossless_proof is not None,
        )
        if gate is not None:
            chain.append(f"stage3_quality_gate:{gate}")
    if decision in ("conversion_failed", "target_conversion_failed", "crash"):
        return "uncertain", False, decision, chain
    verdict, cleanup_eligible, reason = classify_quality_import_stages(
        decision,
        gate if decision in ("import", "preflight_existing") else None,
        imported=decision in QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
    )
    return verdict, cleanup_eligible, reason, chain


def _resolve_lane_current_evidence(
    db: ImportPreviewDB,
    *,
    request_id: int,
    mb_release_id: str,
    cfg: CratediggerConfig,
    loader: Callable[..., EvidenceBuildResult],
    audit_path: str,
    download_log_id: int | None,
) -> CurrentLibraryEvidence | ImportPreviewResult:
    """Render the shared HAVE resolution into this lane's failure shape.

    The sequence itself lives in
    ``lib.current_library_evidence.resolve_current_library_evidence``, which
    the preview worker's front-gate reuse path calls too. This adapter adds
    only the preview lanes' own translation of an unavailable authority into
    the lane-shared ``measurement_failed`` result; ``loader`` is the two
    preview lanes' only difference and is documented there.
    """
    resolved = resolve_current_library_evidence(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        quality_ranks=cfg.quality_ranks,
        beets_library_root=getattr(cfg, "beets_directory", ""),
        loader=loader,
    )
    if isinstance(resolved, CurrentLibraryAuthorityUnavailable):
        return _measurement_failed_result(
            mode="path",
            reason="measurement_crashed",
            decision="current_evidence_failed",
            detail=resolved.detail,
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=audit_path,
        )
    return resolved


@dataclass(frozen=True)
class _MeasuredLaneWorld:
    """A completed preview measurement plus the possibly-refreshed HAVE row."""

    measurement: PreimportMeasurement
    current_evidence: AlbumQualityEvidence | None


def _measure_lane_world(
    db: ImportPreviewDB,
    *,
    request_id: int,
    mb_release_id: str,
    label: str,
    preview_path: str,
    inspection: LocalFileInspection,
    cfg: CratediggerConfig,
    lane_evidence: CurrentLibraryEvidence,
    audit_path: str,
    raw_path: str,
    download_log_id: int | None,
    cancellation_token: CancellationToken | None,
    spectral_detail_analyzer: SpectralDetailAnalyzer | None = None,
    existing_spectral_resolver: ExistingSpectralResolver | None = None,
    aac_lattice_measure_fn: AacLatticeMeasureFn | None = None,
    capture_cd_rip_verification: bool = False,
    measure_fn: Callable[..., PreimportMeasurement] | None = None,
) -> _MeasuredLaneWorld | ImportPreviewResult:
    """Run the pure measurement and refresh exact-current spectral state.

    Shared skeleton stage for both preview lanes. The lane policy split
    lives in the four trailing keyword arguments, all defaulting to
    "measure nothing extra":

    - the measure-and-persist lane supplies ``aac_lattice_measure_fn``
      (tens of seconds of CPU per track — every producer of persisted
      candidate evidence pays it) and sets ``capture_cd_rip_verification``
      (album-scoped, wall-clock-bounded in ``lib/cd_rip_verifier.py``),
      plus its test injection seams for the analyzer and existing
      resolver;
    - the classify lane (CLI inspector, wrong-match triage UI) supplies
      none of them — a synchronous operator surface must not block on the
      expensive captures.

    ``raw_path`` vs ``audit_path`` is the lanes' remaining audit-provenance
    difference: an ``AudioValidationMeasurementError`` reports the lane's
    raw input ``path`` while other collaborator failures report the
    operator-facing display path (``source_display_path`` when set),
    mirroring each lane's historical behaviour exactly (for the classify
    lane the two are the same string).
    """
    current_evidence = lane_evidence.evidence
    try:
        cd_rip_verify_fn: CdRipVerifyFn | None = None
        if capture_cd_rip_verification:
            # The verifier imports numpy. Keep it on the background
            # measurement lane instead of adding that import cost to every
            # web/API process that imports preview orchestration — and keep
            # the import inside this boundary so a broken world maps to
            # ``measurement_failed`` like any other collaborator failure.
            from lib.cd_rip_verifier import verify_cd_rip

            cd_rip_verify_fn = verify_cd_rip
        checkpoint(cancellation_token)
        # ``measure_fn`` is the sanctioned kwarg-DI seam for tests that
        # need to observe or stub the measurement call; resolved at call
        # time so the module attribute stays patchable for legacy tests.
        measurement = (measure_fn or measure_preimport_state)(
            path=preview_path,
            mb_release_id=mb_release_id,
            label=label,
            download_filetype=inspection.filetype,
            download_min_bitrate_bps=inspection.min_bitrate_bps,
            download_is_vbr=inspection.is_vbr,
            cfg=cfg,
            # The lanes' only DB handoff into measurement: the read-only
            # curator bad-hash gate. HAVE spectral state is persisted
            # later via the content-addressed AlbumQualityEvidence row
            # that the importer reads; measurement itself never writes.
            bad_hash_db=db,
            existing_spectral_evidence=lane_evidence.existing_spectral_evidence,
            reuse_existing_spectral_evidence=lane_evidence.reuse_have_evidence,
            preserve_existing_source_spectral=lane_evidence.preserve_have_source,
            precomputed_inspection=inspection,
            spectral_detail_analyzer=spectral_detail_analyzer,
            existing_spectral_resolver=existing_spectral_resolver,
            aac_lattice_measure_fn=aac_lattice_measure_fn,
            cd_rip_verify_fn=cd_rip_verify_fn,
        )
        checkpoint(cancellation_token)
        if not lane_evidence.reuse_have_evidence:
            spectral_result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=request_id,
                current_evidence=current_evidence,
                measured_existing=measurement.spectral_audit.existing,
                measured_existing_path=measurement.existing_spectral_path,
            )
            if spectral_result.evidence is not None:
                current_evidence = spectral_result.evidence
    except ExecutionCancelled:
        raise
    except AudioValidationMeasurementError as exc:
        return _measurement_failed_result(
            mode="path",
            reason="measurement_crashed",
            decision="audio_validation_measurement_failed",
            detail=exc.report.diagnostics[0].category,
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=raw_path,
            audio_validation=exc.report,
        )
    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return _measurement_failed_result(
            mode="path",
            reason="measurement_crashed",
            decision="measurement_crashed",
            detail=f"{type(exc).__name__}: {exc}",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=audit_path,
        )
    return _MeasuredLaneWorld(
        measurement=measurement,
        current_evidence=current_evidence,
    )


def _harness_decision_inputs(
    *,
    current_evidence: AlbumQualityEvidence | None,
    measurement: PreimportMeasurement,
    existing_spectral_evidence: SpectralAnalysisDetail,
) -> tuple[int | None, V0ProbeEvidence | None]:
    """Derive the harness-bound comparison inputs (both lanes, pure)."""
    override_min_bitrate = compute_effective_override_bitrate(
        (
            current_evidence.measurement.min_bitrate_kbps
            if current_evidence is not None
            else measurement.existing_min_bitrate
        ),
        _existing_spectral_interpretation(
            current_evidence=current_evidence,
            measured_existing=measurement.existing_spectral,
            persisted_existing=existing_spectral_evidence,
        ),
    )
    existing_v0_probe: V0ProbeEvidence | None = None
    if current_evidence is not None and current_evidence.v0_metric is not None:
        existing_v0_probe = audit_v0_probe_from_metric(
            current_evidence.v0_metric
        )
    return override_min_bitrate, existing_v0_probe


def _invoke_preview_harness(
    *,
    preview_path: str,
    mb_release_id: str,
    force: bool,
    override_min_bitrate: int | None,
    target_format: str | None,
    cfg: CratediggerConfig,
    existing_v0_probe: V0ProbeEvidence | None,
    quality_evidence_action_file: str | None,
    cancellation_token: CancellationToken | None,
    run_import_fn: Callable[..., ImportOneRun] | None = None,
) -> ImportOneRun:
    """Run the dry-run harness with the lane-shared argument set.

    An injected ``run_import_fn`` (test seam on the measure-and-persist
    lane) is called without the cancellation token, preserving that seam's
    historical call shape.
    """
    checkpoint(cancellation_token)
    if run_import_fn is None:
        run = run_import_one(
            path=preview_path,
            mb_release_id=mb_release_id,
            request_id=None,
            force=force,
            preserve_source=True,
            dry_run=True,
            override_min_bitrate=override_min_bitrate,
            target_format=target_format,
            verified_lossless_target=cfg.verified_lossless_target,
            beets_harness_path=cfg.beets_harness_path,
            quality_rank_config_json=cfg.quality_ranks.to_json(),
            existing_v0_probe=existing_v0_probe,
            quality_evidence_action_file=quality_evidence_action_file,
            cancellation_token=cancellation_token,
        )
    else:
        run = run_import_fn(
            path=preview_path,
            mb_release_id=mb_release_id,
            request_id=None,
            force=force,
            preserve_source=True,
            dry_run=True,
            override_min_bitrate=override_min_bitrate,
            target_format=target_format,
            verified_lossless_target=cfg.verified_lossless_target,
            beets_harness_path=cfg.beets_harness_path,
            quality_rank_config_json=cfg.quality_ranks.to_json(),
            existing_v0_probe=existing_v0_probe,
            quality_evidence_action_file=quality_evidence_action_file,
        )
    checkpoint(cancellation_token)
    return run


def _request_label(req: Mapping[str, Any]) -> str:
    return f"{req.get('artist_name', '')} - {req.get('album_title', '')}".strip(" -")


def measure_and_persist_candidate_evidence(
    db: ImportPreviewDB,
    *,
    request_id: int,
    path: str,
    source_display_path: str | None = None,
    force: bool = True,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    persist_measurement_fn: Callable[..., EvidenceBuildResult] | None = None,
    run_import_fn: Callable[..., ImportOneRun] | None = None,
    spectral_detail_analyzer: SpectralDetailAnalyzer | None = None,
    existing_spectral_resolver: ExistingSpectralResolver | None = None,
    current_evidence_loader: Callable[..., EvidenceBuildResult] | None = None,
    runtime_config: CratediggerConfig | None = None,
    repair_fn: HeaderRepairFn | None = None,
    cancellation_token: CancellationToken | None = None,
    aac_lattice_measure_fn: AacLatticeMeasureFn | None = measure_aac_lattice,
    measure_fn: Callable[..., PreimportMeasurement] | None = None,
) -> ImportPreviewResult:
    """Measure a source folder and persist candidate evidence; never decide.

    The worker/refresh contract (preview worker, #271 stale-evidence
    refresh): purely a fact-gathering surface. It calls
    ``measure_preimport_state`` and persists the resulting facts on
    ``AlbumQualityEvidence``, returning only ``evidence_ready`` /
    ``measurement_failed``. The importer's
    ``full_pipeline_decision_from_evidence`` (U11) reads the persisted
    evidence row and makes every import decision — folder/audio-integrity
    facts are early-exit reject branches at the top of that function.
    For the classify contract (CLI inspector, wrong-match triage UI,
    values preview) use ``preview_import_from_path``.

    ``download_log_id`` / ``import_job_id`` are how the persisted evidence
    gets linked onto the addressing entities (``download_log.
    candidate_evidence_id`` / ``import_jobs.candidate_evidence_id``).
    At least one is REQUIRED: persistence refuses an unowned candidate row
    (``evidence_unowned`` / "no persisted candidate owner"), so omitting
    both turns the whole call into ``measurement_failed`` — pass whichever
    ID the call site has.

    This is the one surface that enables the AAC frame-lattice capture
    (issue #829 PR-A): the default supplies the real measurement, so every
    producer of persisted candidate evidence captures it. The read-only
    classify contract does NOT — ``preview_import_from_path`` calls
    ``measure_preimport_state`` without a measure fn, because a synchronous
    operator surface must not block on tens of seconds of CPU per track.
    (The curator bad-hash gate is different: BOTH lanes run it — a
    decode-speed per-track content hash when the ``bad_audio_hashes``
    table is non-empty, comparable to the strict decode
    ``validate_audio`` already performs on the same call, not a
    tens-of-seconds capture.)

    Flow:
      1. Validate request / mbid / path inputs (return measurement_failed on
         any sanity-check failure).
      2. Snapshot source files via ``snapshot_audio_files`` for the candidate
         evidence ``files`` column AND the post-measurement stale-source guard.
      3. Materialize into a temp copy so the harness has an isolated working
         dir (matches existing preview behavior).
      4. Inspect the temp copy for filetype / bitrate / vbr hints.
      5. Call ``measure_preimport_state`` (the pure measurement helper — no
         denylist writes, no decision branches, no DB writes). This runs the
         audio integrity gate, the curator bad-hash gate (via this lane's DB
         handle as the ``BadHashGateDB`` port), and the spectral gate; the
         measured HAVE spectral is persisted afterwards via
         ``persist_exact_current_spectral_from_attempt`` (issue #90
         propagation, evidence-row addressed).
      6. If the measurement carries an importer-rejecting fact (audio_corrupt,
         bad_audio_hash, nested layout, empty fileset), persist evidence
         straight from the measurement (no harness call) and return
         ``evidence_ready``. The importer's
         ``full_pipeline_decision_from_evidence`` (U11) reads those facts off
         the persisted evidence row and rejects via the four-fact early-exit
         branches upstream of the quality gate.
      7. Otherwise, run ``run_import_one`` in dry-run mode to produce an
         ``ImportResult`` with ``source_measurement``. Persist evidence built
         from both the measurement (U1 facts) and the import result (audio
         measurement, spectral, V0 probe).
      8. Return ``evidence_ready`` when persistence succeeded; otherwise
         ``measurement_failed`` with the appropriate ``MeasurementFailureReason``.
    """
    from lib.config import read_runtime_config

    # A force job measures a private snapshot, but its audit trail must keep
    # the original download-log failed_path rather than leaking that private
    # implementation location into operator-facing provenance.
    audit_path = source_display_path or path

    # --- Sanity checks ---
    req = db.get_request(request_id)
    if not req:
        return _measurement_failed_result(
            mode="path",
            reason="request_not_found",
            decision="request_not_found",
            detail=f"Request {request_id} not found",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=audit_path,
        )

    mbid = str(req.get("mb_release_id") or "")
    if not mbid:
        return _measurement_failed_result(
            mode="path",
            reason="missing_release_id",
            decision="missing_release_id",
            detail="No MusicBrainz release ID",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=audit_path,
        )

    cfg = runtime_config or read_runtime_config()
    lane_evidence = _resolve_lane_current_evidence(
        db,
        request_id=request_id,
        mb_release_id=mbid,
        cfg=cfg,
        loader=current_evidence_loader or load_current_evidence_for_preview,
        audit_path=audit_path,
        download_log_id=download_log_id,
    )
    if isinstance(lane_evidence, ImportPreviewResult):
        return lane_evidence
    current_evidence = lane_evidence.evidence
    existing_spectral_evidence = lane_evidence.existing_spectral_evidence

    repair = repair_fn or prepare_preview_media
    owns_path = path_is_within_root(path, processing_albums_dir(cfg.processing_dir))
    # The processing album is the trust transition.  Normalise it before the
    # preview snapshot so evidence and the later importer both see the same
    # bytes.  Foreign paths still reach repair only after descriptor copying.
    if owns_path:
        checkpoint(cancellation_token)
        repair(path)
        checkpoint(cancellation_token)
    try:
        temp_root = (
            None
            if owns_path
            else snapshot_authorized_directory(
                path,
                cfg,
                cancellation_token=cancellation_token,
            )
        )
    except (FilesystemAuthorityError, OSError) as exc:
        return _measurement_failed_result(
            mode="path",
            reason="materialization_error",
            decision="materialization_failed",
            detail=f"private descriptor snapshot failed: {exc}",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=audit_path,
        )
    # Removed unconditionally in the outermost ``finally`` below — on
    # success, every early return, and every exception — regardless of
    # ``temp_root``. A canonical processing album must never carry this
    # file, whether preview snapshots it or normalizes it in place
    # (issue #859).
    preview_spectral_file: str | None = None
    try:
        preview_path = path if temp_root is None else temp_root
        if not owns_path:
            checkpoint(cancellation_token)
            repair(preview_path)
            checkpoint(cancellation_token)
        try:
            # Address evidence from precisely the immutable private bytes
            # being inspected and passed to the harness.  Never re-inventory
            # the external source after the descriptor copy completed.
            source_snapshot = snapshot_audio_files(preview_path)
        except OSError as exc:
            return _measurement_failed_result(
                mode="path",
                reason="snapshot_stale",
                decision="evidence_snapshot_failed",
                detail=str(exc),
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=audit_path,
            )
        inspection = inspect_local_files(preview_path)

        # --- Run the pure measurement helper (no decision) ---
        measured = _measure_lane_world(
            db,
            request_id=request_id,
            mb_release_id=mbid,
            label=_request_label(req),
            preview_path=preview_path,
            inspection=inspection,
            cfg=cfg,
            lane_evidence=lane_evidence,
            audit_path=audit_path,
            raw_path=path,
            download_log_id=download_log_id,
            cancellation_token=cancellation_token,
            spectral_detail_analyzer=spectral_detail_analyzer,
            existing_spectral_resolver=existing_spectral_resolver,
            aac_lattice_measure_fn=aac_lattice_measure_fn,
            capture_cd_rip_verification=True,
            measure_fn=measure_fn,
        )
        if isinstance(measured, ImportPreviewResult):
            return measured
        measurement = measured.measurement
        current_evidence = measured.current_evidence

        # Integrity facts are authoritative even when the same corrupt
        # lossless source also prevents spectral analysis from producing a
        # grade. Persist/reject the concrete decode failure instead of
        # demoting it to the secondary measurement failure.
        measurement_rejecting = (
            measurement.audio_corrupt
            or measurement.matched_bad_hash_id is not None
            or measurement.folder_layout == "nested"
            or (measurement.audio_file_count == 0 and not source_snapshot)
        )
        spectral_failure = _lossless_candidate_spectral_failure(
            measurement,
            lossless_candidate=measurement.lossless_candidate,
        )
        if spectral_failure is not None and not measurement_rejecting:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="spectral_analysis_failed",
                detail=spectral_failure,
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=audit_path,
                import_result=ImportResult(spectral=measurement.spectral_audit),
            )

        # --- Measurement-only evidence path ---
        # When the measurement carries any importer-rejecting fact, skip the
        # harness (it would either fail or produce misleading state) and
        # persist evidence straight from the measurement. The importer's
        # ``full_pipeline_decision_from_evidence`` (U11) reads those facts
        # off the persisted evidence row and rejects via the four-fact
        # early-exit branches.
        audit_result = ImportResult(spectral=measurement.spectral_audit)
        if measurement_rejecting:
            try:
                checkpoint(cancellation_token)
                evidence_result = (
                    persist_measurement_fn
                    or persist_candidate_evidence_from_measurement
                )(
                    db,
                    mb_release_id=mbid,
                    source_path=audit_path,
                    measurement=measurement,
                    download_log_id=download_log_id,
                    import_job_id=import_job_id,
                    files=source_snapshot,
                )
                checkpoint(cancellation_token)
            except ExecutionCancelled:
                raise
            except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                return _measurement_failed_result(
                    mode="path",
                    reason="evidence_persist_failed",
                    decision="evidence_persist_failed",
                    detail=f"{type(exc).__name__}: {exc}",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=audit_path,
                    import_result=audit_result,
                )
            if evidence_result.status != "ready":
                return _measurement_failed_result(
                    mode="path",
                    reason="evidence_persist_failed",
                    decision=f"evidence_{evidence_result.status}",
                    detail=evidence_result.reason or f"evidence_{evidence_result.status}",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=audit_path,
                    import_result=audit_result,
                )
            decision_hint = _measurement_decision_hint(measurement)
            return _evidence_ready_result(
                mode="path",
                decision=decision_hint,
                reason=decision_hint,
                detail=f"measurement persisted: {decision_hint}",
                stage_chain=[f"measure_preimport:{decision_hint}"],
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=audit_path,
                import_result=ImportResult(spectral=measurement.spectral_audit),
                candidate_evidence_receipt=(
                    evidence_result.persistence_receipt
                ),
            )

        # --- Harness path: measurement allows continuing ---
        override_min_bitrate, existing_v0_probe = _harness_decision_inputs(
            current_evidence=current_evidence,
            measurement=measurement,
            existing_spectral_evidence=existing_spectral_evidence,
        )

        try:
            preview_spectral_file = _write_preview_spectral_evidence_file(
                mb_release_id=mbid,
                source_path=audit_path,
                measurement=measurement,
                files=source_snapshot,
                lossless_candidate=measurement.lossless_candidate,
                cancellation_token=cancellation_token,
            )
            run = _invoke_preview_harness(
                preview_path=preview_path,
                mb_release_id=mbid,
                force=force,
                override_min_bitrate=override_min_bitrate,
                target_format=req.get("target_format"),
                cfg=cfg,
                existing_v0_probe=existing_v0_probe,
                quality_evidence_action_file=preview_spectral_file,
                cancellation_token=cancellation_token,
                run_import_fn=run_import_fn,
            )
        except ExecutionCancelled:
            raise
        except AudioValidationMeasurementError as exc:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="audio_validation_measurement_failed",
                detail=exc.report.diagnostics[0].category,
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                audio_validation=exc.report,
            )
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="harness_crashed",
                detail=f"{type(exc).__name__}: {exc}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=audit_path,
                import_result=audit_result,
            )

        if run.import_result is None:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="no_json_result",
                detail="import_one.py emitted no JSON",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=audit_path,
                import_result=audit_result,
                subprocess_stderr=run.stderr,
            )
        # The preview worker's independent two-sided audit is the attempt
        # record. Keep it separate from decision measurements and replace the
        # harness-local spectral detail with the best successful evidence from
        # either pass, retaining an error only when neither pass succeeded.
        run.import_result.spectral = compose_attempt_spectral_audit(
            measurement.spectral_audit,
            run.import_result.spectral,
        )
        conversion_validation = run.import_result.conversion.source_validation
        if (
            run.import_result.decision
            in {"conversion_failed", "target_conversion_failed"}
            and conversion_validation is not None
            and conversion_validation.outcome == "audio_corrupt"
        ):
            diagnostics = conversion_validation.diagnostics
            first_diagnostic = diagnostics[0] if diagnostics else None
            failed_paths = (
                run.import_result.conversion.source_validation_failed_paths
                or [
                    diagnostic.relative_path
                    for diagnostic in diagnostics
                    if diagnostic.relative_path
                ]
            )
            measurement = msgspec.structs.replace(
                measurement,
                corrupt_files=failed_paths,
                audio_validation=conversion_validation,
                audio_corrupt=True,
                audio_error=(
                    first_diagnostic.stderr_excerpt
                    if (
                        first_diagnostic is not None
                        and first_diagnostic.stderr_excerpt
                    )
                    else (
                        first_diagnostic.category
                        if first_diagnostic is not None
                        else "strict source revalidation failed"
                    )
                ),
            )
            if not audio_snapshot_matches(path, source_snapshot):
                return _measurement_failed_result(
                    mode="path",
                    reason="snapshot_stale",
                    decision="source_changed_during_conversion",
                    detail="source files changed while conversion was running",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
            try:
                checkpoint(cancellation_token)
                evidence_result = (
                    persist_measurement_fn
                    or persist_candidate_evidence_from_measurement
                )(
                    db,
                    mb_release_id=mbid,
                    source_path=path,
                    measurement=measurement,
                    download_log_id=download_log_id,
                    import_job_id=import_job_id,
                    files=source_snapshot,
                )
                checkpoint(cancellation_token)
            except ExecutionCancelled:
                raise
            except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                return _measurement_failed_result(
                    mode="path",
                    reason="evidence_persist_failed",
                    decision="evidence_persist_failed",
                    detail=f"{type(exc).__name__}: {exc}",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
            if evidence_result.status != "ready":
                return _measurement_failed_result(
                    mode="path",
                    reason="evidence_persist_failed",
                    decision=f"evidence_{evidence_result.status}",
                    detail=(
                        evidence_result.reason
                        or f"evidence_{evidence_result.status}"
                    ),
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
            return _evidence_ready_result(
                mode="path",
                decision="audio_corrupt",
                reason="audio_corrupt",
                detail="strict conversion-source validation failed",
                stage_chain=["conversion_source:audio_corrupt"],
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
                candidate_evidence_receipt=(
                    evidence_result.persistence_receipt
                ),
            )
        if run.import_result.decision in (
            "conversion_failed",
            "target_conversion_failed",
            "crash",
        ):
            # "crash" is the harness's top-level exception envelope: whatever
            # partial measurements it carries were interrupted mid-build
            # (2026-07-18: the proof mint crashed AFTER source_measurement was
            # set, so the partial result looked persistable but had lost its
            # verified-lossless proof). Analysis failure aborts loudly — it
            # never becomes evidence_ready.
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision=run.import_result.decision,
                detail=run.import_result.error or run.import_result.decision,
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=audit_path,
                import_result=run.import_result,
                audio_validation=(
                    conversion_validation
                    if (
                        conversion_validation is not None
                        and conversion_validation.outcome
                        == "measurement_failed"
                    )
                    else None
                ),
            )
        if run.import_result.source_measurement is None:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision=run.import_result.decision or "missing_source_measurement",
                detail="ImportResult missing source_measurement",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=audit_path,
                import_result=run.import_result,
                subprocess_stderr=run.stderr,
            )

        # --- Persist candidate evidence ---
        try:
            checkpoint(cancellation_token)
            evidence_result = persist_candidate_evidence_from_import_result(
                db,
                mb_release_id=mbid,
                source_path=audit_path,
                import_result=run.import_result,
                download_log_id=download_log_id,
                import_job_id=import_job_id,
                files=source_snapshot,
                measurement=measurement,
            )
            checkpoint(cancellation_token)
        except ExecutionCancelled:
            raise
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            return _measurement_failed_result(
                mode="path",
                reason="evidence_persist_failed",
                decision="evidence_persist_failed",
                detail=f"{type(exc).__name__}: {exc}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=audit_path,
                import_result=run.import_result,
            )
        if evidence_result.status != "ready":
            return _measurement_failed_result(
                mode="path",
                reason="evidence_persist_failed",
                decision=f"evidence_{evidence_result.status}",
                detail=evidence_result.reason or f"evidence_{evidence_result.status}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=audit_path,
                import_result=run.import_result,
            )

        return _evidence_ready_result(
            mode="path",
            decision=run.import_result.decision or "evidence_ready",
            reason=run.import_result.decision,
            detail=run.import_result.error,
            stage_chain=[
                "measure_preimport:ok",
                f"stage2_import:{run.import_result.decision}",
            ],
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=audit_path,
            import_result=run.import_result,
            candidate_evidence_receipt=evidence_result.persistence_receipt,
        )
    finally:
        _cleanup_preview_artifacts(
            preview_spectral_file=preview_spectral_file,
            temp_root=temp_root,
            cfg=cfg,
            cancellation_token=cancellation_token,
        )


def _measurement_decision_hint(measurement: Any) -> str:
    """Derive a short label for measurement-only evidence_ready returns.

    Used purely for log/decision-string display — the importer's
    ``full_pipeline_decision_from_evidence`` (U11) makes the actual reject
    call from the persisted evidence via its four-fact early-exit branches.
    The audio_corrupt vs. nested_layout choice routes through the one
    shared precedence function (``preimport_corrupt_outranks_nested``,
    issue #1355 item 1) rather than an independently-authored order over
    the same keys — the exact shape that let ``preview_import_from_path``
    drift from the real decider before this PR's C1 fix.
    """
    corrupt_or_nested = preimport_corrupt_outranks_nested(
        audio_corrupt=measurement.audio_corrupt,
        nested_layout=measurement.folder_layout == "nested",
    )
    if corrupt_or_nested == "audio_corrupt":
        return "audio_corrupt"
    if measurement.matched_bad_hash_id is not None:
        return "bad_audio_hash"
    if corrupt_or_nested == "nested_layout":
        return "nested_layout"
    if measurement.audio_file_count == 0:
        return "empty_fileset"
    return "evidence_ready"


def _refusal_is_world_failure(exc: BaseException) -> bool:
    """Did the storage layer refuse, rather than the name being wrong?

    One helper for both snapshot boundaries. A typed authority refusal
    delegates to ``lib.fs_authority.refusal_is_indeterminate``; a bare
    ``OSError`` escaping the snapshot is classified through the very same
    errno table (issue #1063). That classifier returns ``None`` for a
    code outside its ``Literal`` rather than lose its exhaustiveness
    guard; ``is True`` keeps that unclassifiable case on the fail-safe
    side — reported as a refusal, not as a retryable blip.
    """
    if isinstance(exc, FilesystemAuthorityError):
        return refusal_is_indeterminate(exc.code) is True
    if isinstance(exc, OSError):
        return refusal_is_indeterminate(classify_path_errno(exc)) is True
    return False


def preview_import_from_path(
    db: ImportPreviewDB,
    *,
    request_id: int,
    path: str,
    force: bool = True,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    persist_candidate_evidence: bool = False,
    _already_isolated: bool = False,
    cancellation_token: CancellationToken | None = None,
    measure_fn: Callable[..., PreimportMeasurement] | None = None,
) -> ImportPreviewResult:
    """Classify a real source folder without mutating source files or beets.

    The classify contract (CLI inspector, wrong-match triage UI, values
    preview): returns the classifier's ``would_import`` /
    ``confident_reject`` / ``uncertain`` verdicts for audit/UI display.
    DB evidence persistence is opt-in via ``persist_candidate_evidence``.
    For the measure-and-persist worker/refresh contract (verdicts
    ``evidence_ready`` / ``measurement_failed``) use
    ``measure_and_persist_candidate_evidence``.

    Contract: preview only measures. Facts come from
    ``measure_preimport_state``; the five folder/audio-integrity facts are
    inlined as a confident_reject verdict for CLI/triage UI. Spectral /
    codec rank / V0 / quality-gate decisions belong to the importer's
    ``full_pipeline_decision_from_evidence``.
    """
    req = db.get_request(request_id)
    if not req:
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
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="missing_release_id",
            reason="No MusicBrainz release ID",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    observation = observe_directory(path)
    if observation.indeterminate:
        # Explicit-path preview under an identity that cannot traverse the
        # private processing tree used to answer "Path not found" — a
        # definitive negative fact it had no evidence for (issue #1063).
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="path_unavailable",
            reason=f"Path could not be observed: {observation.unavailable_reason()}",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    if observation.absent:
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
    lane_evidence = _resolve_lane_current_evidence(
        db,
        request_id=request_id,
        mb_release_id=mbid,
        cfg=cfg,
        loader=authorize_current_evidence_for_preview,
        audit_path=path,
        download_log_id=download_log_id,
    )
    if isinstance(lane_evidence, ImportPreviewResult):
        return lane_evidence
    current_evidence = lane_evidence.evidence
    existing_spectral_evidence = lane_evidence.existing_spectral_evidence

    # Every preview runs against one bounded, descriptor-copied private
    # snapshot.  The download-log entry point has already made that snapshot;
    # direct CLI path mode takes the same no-follow route here rather than an
    # unbounded ``copytree``.  No external tool receives a DB- or CLI-supplied
    # pathname before this boundary is complete.
    try:
        temp_root = (
            None
            if _already_isolated
            else snapshot_authorized_directory(
                path,
                cfg,
                cancellation_token=cancellation_token,
            )
        )
    except (FilesystemAuthorityError, OSError) as exc:
        # A refused observation is a world failure, not a verdict about
        # the operator's path. Saying "unauthorized" about an EACCES on
        # our own private tree is the #1063 shape one layer down.
        decision = (
            "path_unavailable"
            if _refusal_is_world_failure(exc)
            else "path_unauthorized"
        )
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision=decision,
            reason=decision,
            detail=str(exc),
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    # Removed unconditionally in the outermost ``finally`` below — on
    # success, every early return, and every exception — regardless of
    # ``temp_root``. See ``measure_and_persist_candidate_evidence`` (issue
    # #859) for why this must never depend on the snapshot branch.
    preview_spectral_file: str | None = None
    try:
        preview_path = path if temp_root is None else temp_root
        # Header repair is deliberately against the private copy. This keeps
        # the source immutable while preserving the importer-facing order:
        # repair before inspection, measurement, and the dry-run harness.
        try:
            checkpoint(cancellation_token)
            prepare_preview_media(preview_path)
            checkpoint(cancellation_token)
        except ExecutionCancelled:
            raise
        except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
            pass

        source_snapshot = None
        if persist_candidate_evidence:
            try:
                source_snapshot = snapshot_audio_files(preview_path)
            except OSError as exc:
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
                # Empty source snapshot: evidence persistence requires at
                # least one file, so surface the empty fileset as uncertain.
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
        inspection = inspect_local_files(preview_path)

        # Preview measures; never decides. Shared skeleton stage: collect
        # facts via ``measure_preimport_state`` (no denylist writes, no
        # decision branches), then surface the five folder/audio-integrity
        # facts as a confident reject for the CLI/triage UI. The classify
        # lane supplies none of the expensive capture fns — see
        # ``_measure_lane_world``'s docstring for the lane policy split.
        # ``inspection.has_nested_audio`` used to short-circuit here, before
        # measurement ever ran, which could disagree with production's real
        # decision twins on a candidate that was both corrupt and nested
        # (issue #1355 item 1's residual): this surface showed
        # ``nested_layout`` while ``full_pipeline_decision_from_evidence``
        # denylisted the same candidate as ``audio_corrupt``. Measurement now
        # always runs first, matching the measure-and-persist lane's own
        # order; the four-fact block below decides which reject reason wins.
        measured = _measure_lane_world(
            db,
            request_id=request_id,
            mb_release_id=mbid,
            label=_request_label(req),
            preview_path=preview_path,
            inspection=inspection,
            cfg=cfg,
            lane_evidence=lane_evidence,
            audit_path=path,
            raw_path=path,
            download_log_id=download_log_id,
            cancellation_token=cancellation_token,
            measure_fn=measure_fn,
        )
        if isinstance(measured, ImportPreviewResult):
            return measured
        measurement = measured.measurement
        current_evidence = measured.current_evidence

        # Four-fact reject (mirror of the measure-and-persist lane's
        # ``measurement_rejecting`` derivation). ``empty_fileset`` is also
        # handled by the ``not source_snapshot`` branch on the persist path;
        # this defensive check keeps the measurement-derived fact the single
        # source of truth when persistence is skipped. The audio_corrupt vs.
        # nested_layout choice routes through the one shared precedence
        # function (``preimport_corrupt_outranks_nested``, issue #1355 item
        # 1) so this display surface cannot independently drift from the
        # real decision twins again.
        audio_corrupt = measurement.audio_corrupt
        bad_audio_hash = measurement.matched_bad_hash_id is not None
        nested_layout = measurement.folder_layout == "nested"
        empty_fileset = measurement.audio_file_count == 0
        corrupt_or_nested = preimport_corrupt_outranks_nested(
            audio_corrupt=audio_corrupt, nested_layout=nested_layout,
        )
        if audio_corrupt or bad_audio_hash or nested_layout or empty_fileset:
            scenario = (
                "audio_corrupt" if corrupt_or_nested == "audio_corrupt"
                else "bad_audio_hash" if bad_audio_hash
                else "nested_layout" if corrupt_or_nested == "nested_layout"
                else "empty_fileset"
            )
            detail: str | None = None
            if scenario == "audio_corrupt":
                detail = measurement.audio_error
                if detail is None and measurement.corrupt_files:
                    detail = (
                        f"{len(measurement.corrupt_files)} files failed ffmpeg decode"
                    )
            elif scenario == "bad_audio_hash" and measurement.matched_bad_track_path:
                detail = (
                    f"matched bad_audio_hash id={measurement.matched_bad_hash_id} "
                    f"on track {measurement.matched_bad_track_path}"
                )
            elif scenario == "nested_layout":
                detail = (
                    "Audio files are in subdirectories — flatten the folder "
                    "before import."
                )
            return _preview_result(
                mode="path",
                verdict=PREVIEW_VERDICT_CONFIDENT_REJECT,
                decision=scenario,
                reason=scenario,
                detail=detail,
                stage_chain=[f"preimport:{scenario}"],
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                cleanup_eligible=True,
            )

        spectral_failure = _lossless_candidate_spectral_failure(
            measurement,
            lossless_candidate=measurement.lossless_candidate,
        )
        if spectral_failure is not None:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="spectral_analysis_failed",
                detail=spectral_failure,
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=ImportResult(spectral=measurement.spectral_audit),
            )

        override_min_bitrate, existing_v0_probe = _harness_decision_inputs(
            current_evidence=current_evidence,
            measurement=measurement,
            existing_spectral_evidence=existing_spectral_evidence,
        )

        preview_spectral_file = _write_preview_spectral_evidence_file(
            mb_release_id=mbid,
            source_path=path,
            measurement=measurement,
            files=source_snapshot,
            lossless_candidate=measurement.lossless_candidate,
            cancellation_token=cancellation_token,
        )
        run = _invoke_preview_harness(
            preview_path=preview_path,
            mb_release_id=mbid,
            force=force,
            override_min_bitrate=override_min_bitrate,
            target_format=req.get("target_format"),
            cfg=cfg,
            existing_v0_probe=existing_v0_probe,
            quality_evidence_action_file=preview_spectral_file,
            cancellation_token=cancellation_token,
        )
        if run.import_result is not None:
            run.import_result.spectral = compose_attempt_spectral_audit(
                measurement.spectral_audit,
                run.import_result.spectral,
            )
        verdict, cleanup_eligible, reason, chain = _classify_import_result(
            run.import_result,
            cfg=cfg.quality_ranks,
        )
        evidence_status: str | None = None
        evidence_reason: str | None = None
        if persist_candidate_evidence:
            if source_snapshot is None:
                detail = "source files changed while preview was running"
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
                checkpoint(cancellation_token)
                evidence = persist_candidate_evidence_from_import_result(
                    db,
                    mb_release_id=mbid,
                    source_path=path,
                    import_result=run.import_result,
                    download_log_id=download_log_id,
                    import_job_id=import_job_id,
                    files=source_snapshot,
                )
                checkpoint(cancellation_token)
                evidence_status = evidence.status
                evidence_reason = evidence.reason
            except ExecutionCancelled:
                raise
            except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                evidence_status = "failed"
                evidence_reason = f"{type(exc).__name__}: {exc}"
            if evidence_status != "ready":
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
        _cleanup_preview_artifacts(
            preview_spectral_file=preview_spectral_file,
            temp_root=temp_root,
            cfg=cfg,
            cancellation_token=cancellation_token,
        )


def preview_import_from_download_log(
    db: ImportPreviewDB,
    download_log_id: int,
    *,
    cancellation_token: CancellationToken | None = None,
) -> ImportPreviewResult:
    """Preview the failed source referenced by one download_log row.

    Classify contract only (wrong-match triage, ad-hoc CLI inspection) —
    delegates to ``preview_import_from_path`` after resolving the row's
    ``failed_path``.
    """
    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="download_log_not_found",
            reason=f"Download log entry {download_log_id} not found",
            download_log_id=download_log_id,
        )
    # ``request_id`` is a required, non-nullable ``download_log`` column
    # (DownloadLogWithEvidenceRow), so the row type already proves this is
    # an ``int`` — no runtime narrowing needed once the row comes through
    # the typed projection.
    request_id_raw = entry["request_id"]
    vr = decode_validation_envelope(entry.get("validation_result"))
    raw_path = vr.failed_path
    if not raw_path:
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="missing_failed_path",
            reason="Download log row has no failed_path",
            request_id=request_id_raw,
            download_log_id=download_log_id,
        )
    from lib.config import read_runtime_config

    cfg = read_runtime_config()
    try:
        snapshot = snapshot_configured_quarantine_directory(
            raw_path,
            cfg,
            cancellation_token=cancellation_token,
        )
    except (FilesystemAuthorityError, OSError) as exc:
        unavailable = _refusal_is_world_failure(exc)
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="path_unavailable" if unavailable else "path_unauthorized",
            reason=(
                f"Failed path could not be read: {raw_path}"
                if unavailable
                else f"Failed path is missing or unauthorized: {raw_path}"
            ),
            detail=str(exc),
            request_id=request_id_raw,
            download_log_id=download_log_id,
            source_path=raw_path,
        )
    try:
        return preview_import_from_path(
            db,
            request_id=request_id_raw,
            path=snapshot,
            force=True,
            download_log_id=download_log_id,
            _already_isolated=True,
            cancellation_token=cancellation_token,
        )
    finally:
        remove_preview_snapshot(
            snapshot,
            cfg,
            cancellation_token=cancellation_token,
        )
