"""Unified import preview service.

Preview answers the operator's "would this import?" question without beets,
pipeline DB, queue, denylist, or source-folder mutation. Real-folder preview
uses the same preimport gates and import_one.py harness protocol as force/manual
import, but runs both against isolated temporary copies.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

import msgspec

from lib.dispatch import run_import_one
from lib.dispatch.types import ImportOneRun
from lib.measurement import (
    inspect_local_files,
    measure_preimport_state,
    spectral_detail_from_persisted_source,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    QualityEvidenceDB,
    audio_snapshot_matches,
    legacy_current_lossless_v0_probe_from_request,
    load_or_backfill_current_evidence,
    audit_v0_probe_from_metric,
    persist_candidate_evidence_from_import_result,
    persist_candidate_evidence_from_measurement,
    snapshot_audio_files,
)
from lib.quality import (
    LOSSLESS_CODECS,
    V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
    AlbumQualityEvidence,
    AudioQualityMeasurement,
    ImportResult,
    MeasurementFailure,
    MeasurementFailureReason,
    QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
    QualityRankConfig,
    SpectralAnalysisDetail,
    SpectralDetail,
    TargetQualityContract,
    classify_full_pipeline_decision,
    classify_quality_import_stages,
    compute_effective_override_bitrate,
    full_pipeline_decision,
    quality_gate_decision,
)
from lib.util import repair_mp3_headers, resolve_failed_path
from lib.validation_envelope import decode_validation_envelope

logger = logging.getLogger("cratedigger")


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


@runtime_checkable
class ImportPreviewDB(QualityEvidenceDB, Protocol):
    """The PipelineDB surface the preview entry points use (#409).

    Extends ``QualityEvidenceDB`` because the handle is forwarded into the
    evidence persisters. Parity tests live in
    ``tests/test_import_preview.py``.
    """

    def get_download_log_entry(self, log_id: int) -> dict[str, Any] | None: ...


def load_persisted_existing_spectral(
    db: ImportPreviewDB,
    request_id: int,
    req: dict[str, Any],
) -> tuple[AlbumQualityEvidence | None, SpectralAnalysisDetail, bool]:
    """Load persisted HAVE provenance for the conditional audit boundary.

    Once ``current_evidence_id`` exists, that row is authoritative even when
    it deliberately carries no spectral fields. The request scalar columns are
    a legacy fallback only for requests that have never been linked to current
    evidence; using them after an empty or unreadable linked row could resurrect
    stale pre-reset provenance.
    """
    try:
        evidence_id = db.get_request_current_evidence_id(request_id)
    except Exception:
        logger.warning(
            "Unable to resolve current spectral evidence for request %s",
            request_id,
            exc_info=True,
        )
        return None, SpectralAnalysisDetail(attempted=False), True
    if evidence_id is None:
        return (
            None,
            spectral_detail_from_persisted_source(
                req.get("current_spectral_grade"),
                req.get("current_spectral_bitrate"),
            ),
            False,
        )
    try:
        current_evidence = db.load_album_quality_evidence_by_id(evidence_id)
    except Exception:
        logger.warning(
            "Unable to load current spectral evidence %s for request %s",
            evidence_id,
            request_id,
            exc_info=True,
        )
        return None, SpectralAnalysisDetail(attempted=False), True
    if current_evidence is None:
        logger.warning(
            "Current spectral evidence %s is missing for request %s",
            evidence_id,
            request_id,
        )
        return None, SpectralAnalysisDetail(attempted=False), True
    measurement = current_evidence.measurement
    return (
        current_evidence,
        spectral_detail_from_persisted_source(
            measurement.spectral_grade,
            measurement.spectral_bitrate_kbps,
        ),
        True,
    )


def preserve_existing_source_spectral(
    current_evidence: AlbumQualityEvidence | None,
) -> bool:
    """Whether HAVE must retain lossless-source pre-conversion evidence."""
    if current_evidence is None:
        return False
    converted_from = (
        current_evidence.measurement.was_converted_from or ""
    ).lower()
    if converted_from in LOSSLESS_CODECS:
        return True
    v0_metric = current_evidence.v0_metric
    return (
        converted_from == "m4a"
        and v0_metric is not None
        and v0_metric.source_lineage == V0_SOURCE_LINEAGE_LOSSLESS_SOURCE
    )


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


# Bound on how much of a subprocess's stderr we fold into a persisted
# MeasurementFailure.detail / the journal — this is diagnostic breadcrumb,
# not a log dump. Individual import_one.py [FAIL] lines are themselves
# already bounded to a ~230-char ffmpeg-stderr tail (see
# harness/import_one.py::convert_lossless), so a handful of them fits
# comfortably; this is a hard ceiling against pathological inputs.
_STDERR_DIAGNOSTIC_MAX_CHARS = 2000


def _diagnostic_from_stderr(stderr: str, max_chars: int = _STDERR_DIAGNOSTIC_MAX_CHARS) -> str:
    """Extract the useful per-file failure signal from an import_one.py stderr blob.

    ``convert_lossless`` (``harness/import_one.py``) prints one
    ``[FAIL] <file>: <ffmpeg tail>`` line per failed conversion to stderr,
    but the aggregate ``ImportResult.error`` on a ``conversion_failed`` /
    ``target_conversion_failed`` result only ever says e.g. "11 FLAC files
    failed to convert" — the per-file *why* was otherwise dropped on the
    floor (never persisted, never streamed to the journal on the preview
    path, unlike the importer's ``dispatch_import_core``). This pulls every
    ``[FAIL]`` line back out so the true tool-level reason survives.

    Falls back to a bounded tail of the raw stderr when no ``[FAIL]`` marker
    is present (e.g. a crash before the per-file loop even started), so a
    non-empty stderr never yields an empty diagnostic.

    Keeps whole lines (never slices a line in half) so a captured ``[FAIL]``
    marker is never partially truncated; only hard-truncates as a last
    resort for a single line that alone exceeds ``max_chars``. Never raises
    — this runs on arbitrary subprocess output.
    """
    if not stderr or not stderr.strip():
        return ""

    lines = [ln.strip() for ln in stderr.splitlines() if ln.strip()]
    fail_lines = [ln for ln in lines if "[FAIL]" in ln]
    source_lines = fail_lines if fail_lines else lines

    # Keep the most recent lines within the char budget (newest failures are
    # the most actionable), without truncating a kept line mid-string.
    kept: list[str] = []
    total = 0
    for line in reversed(source_lines):
        joiner_cost = 3 if kept else 0  # " / "
        added = len(line) + joiner_cost
        if total + added > max_chars and kept:
            break
        kept.append(line)
        total += added
    kept.reverse()

    result = " / ".join(kept)
    if len(result) > max_chars:
        # Pathological single oversized line — hard ceiling wins.
        result = result[:max_chars]
    return result


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
) -> ImportPreviewResult:
    """Build a ``verdict='measurement_failed'`` preview result with typed payload.

    ``subprocess_stderr``, when supplied, is the raw ``ImportOneRun.stderr``
    from the ``run_import_one`` call that triggered this failure. The
    per-file tool diagnostic it carries (e.g. the real ffmpeg decode error
    behind a "conversion_failed") is folded into ``detail`` (so it reaches
    both the persisted ``MeasurementFailure`` payload and the operator-
    facing preview result) and logged at WARNING so it is visible in the
    journal without needing to reproduce the failure. This closes the gap
    where a conversion failure's only DB/journal trace was an aggregate
    count ("11 FLAC files failed to convert") with the actual per-file
    reason discarded.
    """
    full_detail = detail
    if subprocess_stderr:
        diagnostic = _diagnostic_from_stderr(subprocess_stderr)
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


def _quality_gate_stage(
    measurement: AudioQualityMeasurement | None,
    cfg: QualityRankConfig,
    target_contract: TargetQualityContract | None = None,
) -> str | None:
    if measurement is None:
        return None
    return quality_gate_decision(
        measurement,
        cfg=cfg,
        target_contract=target_contract,
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
        )
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


def measure_and_persist_candidate_evidence(
    db: ImportPreviewDB,
    *,
    request_id: int,
    path: str,
    force: bool = True,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    persist_measurement_fn: Callable[..., EvidenceBuildResult] | None = None,
    run_import_fn: Callable[..., ImportOneRun] | None = None,
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
    Omitting both still persists the content-addressed evidence row, but
    triage's FK walk won't find it — pass whichever IDs the call site has.

    Flow:
      1. Validate request / mbid / path inputs (return measurement_failed on
         any sanity-check failure).
      2. Snapshot source files via ``snapshot_audio_files`` for the candidate
         evidence ``files`` column AND the post-measurement stale-source guard.
      3. Materialize into a temp copy so the harness has an isolated working
         dir (matches existing preview behavior).
      4. Inspect the temp copy for filetype / bitrate / vbr hints.
      5. Call ``measure_preimport_state`` (the pure measurement helper — no
         denylist writes, no decision branches). This runs the audio integrity
         gate, bad-hash gate, spectral gate, and persists on-disk spectral
         state to ``album_requests`` per issue #90 propagation.
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
            source_path=path,
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
            source_path=path,
        )

    if not os.path.isdir(path):
        return _measurement_failed_result(
            mode="path",
            reason="source_vanished",
            decision="path_missing",
            detail=f"Path not found: {path}",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    # --- Source cleanup BEFORE snapshot ---
    # mp3val runs once on the source so the snapshot captures the
    # post-cleanup state. Source is then immutable until beets consumes
    # it: the importer's freshness check, the harness's
    # ``_validate_quality_evidence_action_snapshot``, and any later
    # wrong-match triage all see the same bytes the preview measured.
    try:
        repair_mp3_headers(path)
    except Exception:
        pass

    # --- Snapshot for freshness guard + evidence files column ---
    try:
        source_snapshot = snapshot_audio_files(path)
    except OSError as exc:
        return _measurement_failed_result(
            mode="path",
            reason="snapshot_stale",
            decision="evidence_snapshot_failed",
            detail=str(exc),
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    cfg = read_runtime_config()
    (
        current_evidence,
        existing_spectral_evidence,
        current_evidence_authoritative,
    ) = load_persisted_existing_spectral(db, request_id, req)
    preserve_have_source = preserve_existing_source_spectral(current_evidence)

    temp_root = tempfile.mkdtemp(prefix="cratedigger-import-preview-")
    try:
        preview_path = os.path.join(
            temp_root,
            os.path.basename(os.path.abspath(path)) or "album",
        )
        try:
            shutil.copytree(path, preview_path)
        except OSError as exc:
            return _measurement_failed_result(
                mode="path",
                reason="materialization_error",
                decision="materialization_failed",
                detail=f"shutil.copytree failed: {exc}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )
        inspection = inspect_local_files(preview_path)

        # --- Run the pure measurement helper (no decision) ---
        try:
            measurement = measure_preimport_state(
                path=preview_path,
                mb_release_id=mbid,
                label=_request_label(req),
                download_filetype=inspection.filetype,
                download_min_bitrate_bps=inspection.min_bitrate_bps,
                download_is_vbr=inspection.is_vbr,
                cfg=cfg,
                # db=None / request_id=None: spectral propagation happens
                # later via the persisted AlbumQualityEvidence row that the
                # importer reads. Preview is now a pure measurement surface.
                db=None,
                request_id=None,
                existing_spectral_evidence=existing_spectral_evidence,
                preserve_existing_source_spectral=preserve_have_source,
                propagate_download_to_existing=False,
                precomputed_inspection=inspection,
            )
        except Exception as exc:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="measurement_crashed",
                detail=f"{type(exc).__name__}: {exc}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )

        # --- Measurement-only evidence path ---
        # When the measurement carries any importer-rejecting fact, skip the
        # harness (it would either fail or produce misleading state) and
        # persist evidence straight from the measurement. The importer's
        # ``full_pipeline_decision_from_evidence`` (U11) reads those facts
        # off the persisted evidence row and rejects via the four-fact
        # early-exit branches.
        measurement_rejecting = (
            measurement.audio_corrupt
            or measurement.matched_bad_hash_id is not None
            or measurement.folder_layout == "nested"
            or (measurement.audio_file_count == 0 and not source_snapshot)
        )
        audit_result = ImportResult(spectral=measurement.spectral_audit)
        if measurement_rejecting:
            if not audio_snapshot_matches(path, source_snapshot):
                return _measurement_failed_result(
                    mode="path",
                    reason="snapshot_stale",
                    decision="source_changed_during_preview",
                    detail="source files changed while preview was running",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=audit_result,
                )
            try:
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
                    target_format=req.get("target_format"),
                    files=source_snapshot,
                )
            except Exception as exc:
                return _measurement_failed_result(
                    mode="path",
                    reason="evidence_persist_failed",
                    decision="evidence_persist_failed",
                    detail=f"{type(exc).__name__}: {exc}",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
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
                    source_path=path,
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
                source_path=path,
                import_result=ImportResult(spectral=measurement.spectral_audit),
            )

        # --- Harness path: measurement allows continuing ---
        existing_v0_probe = legacy_current_lossless_v0_probe_from_request(req)
        if current_evidence is None and not current_evidence_authoritative:
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
        existing_spectral = measurement.existing_spectral
        existing_grade = (
            existing_spectral.grade
            if existing_spectral is not None
            else existing_spectral_evidence.grade
        )
        existing_bitrate = (
            existing_spectral.bitrate_kbps
            if existing_spectral is not None
            else existing_spectral_evidence.bitrate_kbps
        )
        if current_evidence is not None:
            current_m = current_evidence.measurement
            existing_grade = current_m.spectral_grade
            existing_bitrate = current_m.spectral_bitrate_kbps
            if current_evidence.v0_metric is not None:
                existing_v0_probe = audit_v0_probe_from_metric(
                    current_evidence.v0_metric
                )
        override_min_bitrate = compute_effective_override_bitrate(
            (
                current_evidence.measurement.min_bitrate_kbps
                if current_evidence is not None
                else req.get("min_bitrate")
            ),
            existing_bitrate if isinstance(existing_bitrate, int) else None,
            existing_grade if isinstance(existing_grade, str) else None,
        )

        try:
            run = (run_import_fn or run_import_one)(
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
        except Exception as exc:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="harness_crashed",
                detail=f"{type(exc).__name__}: {exc}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
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
                source_path=path,
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
        if run.import_result.decision in (
            "conversion_failed",
            "target_conversion_failed",
        ):
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision=run.import_result.decision,
                detail=run.import_result.error or run.import_result.decision,
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
                subprocess_stderr=run.stderr,
            )
        if run.import_result.source_measurement is None:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision=run.import_result.decision or "missing_source_measurement",
                detail="ImportResult missing source_measurement",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
                subprocess_stderr=run.stderr,
            )

        # --- Snapshot freshness guard (post-measurement) ---
        if not audio_snapshot_matches(path, source_snapshot):
            return _measurement_failed_result(
                mode="path",
                reason="snapshot_stale",
                decision="source_changed_during_preview",
                detail="source files changed while preview was running",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
            )

        # --- Persist candidate evidence ---
        try:
            evidence_result = persist_candidate_evidence_from_import_result(
                db,
                mb_release_id=mbid,
                source_path=path,
                import_result=run.import_result,
                download_log_id=download_log_id,
                import_job_id=import_job_id,
                files=source_snapshot,
                measurement=measurement,
            )
        except Exception as exc:
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
                detail=evidence_result.reason or f"evidence_{evidence_result.status}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
            )

        return _evidence_ready_result(
            mode="path",
            decision=run.import_result.decision or "evidence_ready",
            reason=run.import_result.decision,
            detail=run.import_result.error,
            stage_chain=[
                f"measure_preimport:ok",
                f"stage2_import:{run.import_result.decision}",
            ],
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
            import_result=run.import_result,
        )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def _measurement_decision_hint(measurement: Any) -> str:
    """Derive a short label for measurement-only evidence_ready returns.

    Used purely for log/decision-string display — the importer's
    ``full_pipeline_decision_from_evidence`` (U11) makes the actual reject
    call from the persisted evidence via its four-fact early-exit branches.
    Order mirrors that decider's evaluation order.
    """
    if measurement.audio_corrupt:
        return "audio_corrupt"
    if measurement.matched_bad_hash_id is not None:
        return "bad_audio_hash"
    if measurement.folder_layout == "nested":
        return "nested_layout"
    if measurement.audio_file_count == 0:
        return "empty_fileset"
    return "evidence_ready"


def preview_import_from_path(
    db: ImportPreviewDB,
    *,
    request_id: int,
    path: str,
    force: bool = True,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    persist_candidate_evidence: bool = False,
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
    ``measure_preimport_state``; the four folder/audio-integrity facts are
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
    if not os.path.isdir(path):
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
    (
        current_evidence,
        existing_spectral_evidence,
        current_evidence_authoritative,
    ) = load_persisted_existing_spectral(db, request_id, req)
    preserve_have_source = preserve_existing_source_spectral(current_evidence)

    # --- Source cleanup BEFORE snapshot ---
    # mp3val runs once on the source so the snapshot captures the
    # post-cleanup state and the source stays stable through the
    # importer + harness lifecycle.
    try:
        repair_mp3_headers(path)
    except Exception:
        pass

    source_snapshot = None
    if persist_candidate_evidence:
        try:
            source_snapshot = snapshot_audio_files(path)
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
            # Empty source snapshot: evidence persistence requires at least
            # one file, so surface the empty fileset as an uncertain verdict.
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
        inspection = inspect_local_files(preview_path)
        if inspection.has_nested_audio:
            detail = (
                "Audio files are in subdirectories — flatten the folder "
                "before import."
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

        # Preview measures; never decides. Mirror the measure-and-persist
        # pattern: collect facts via ``measure_preimport_state`` (no denylist
        # writes, no decision branches), then surface the four folder/audio-
        # integrity facts as a confident reject for the CLI/triage UI.
        # ``db=None`` / ``request_id=None`` / ``propagate_download_to_existing=False``:
        # spectral propagation belongs to the persisted ``AlbumQualityEvidence``
        # row that the importer reads — preview is now a pure measurement
        # surface.
        measurement = measure_preimport_state(
            path=preview_path,
            mb_release_id=mbid,
            label=_request_label(req),
            download_filetype=inspection.filetype,
            download_min_bitrate_bps=inspection.min_bitrate_bps,
            download_is_vbr=inspection.is_vbr,
            cfg=cfg,
            db=None,
            request_id=None,
            existing_spectral_evidence=(
                existing_spectral_evidence
            ),
            preserve_existing_source_spectral=preserve_have_source,
            propagate_download_to_existing=False,
            precomputed_inspection=inspection,
        )

        # Four-fact reject (mirror worker-mode lines 517-522). ``nested_layout``
        # is already handled by the ``inspection.has_nested_audio`` branch
        # above; ``empty_fileset`` is handled by the ``not source_snapshot``
        # branch on the persist path. At this site only ``audio_corrupt`` and
        # ``bad_audio_hash`` can fire — but we check ``folder_layout``/
        # ``audio_file_count`` defensively so the measurement-derived facts
        # stay the single source of truth.
        audio_corrupt = measurement.audio_corrupt
        bad_audio_hash = measurement.matched_bad_hash_id is not None
        nested_layout = measurement.folder_layout == "nested"
        empty_fileset = measurement.audio_file_count == 0
        if audio_corrupt or bad_audio_hash or nested_layout or empty_fileset:
            scenario = (
                "audio_corrupt" if audio_corrupt
                else "bad_audio_hash" if bad_audio_hash
                else "nested_layout" if nested_layout
                else "empty_fileset"
            )
            detail: str | None = None
            if audio_corrupt and measurement.corrupt_files:
                detail = (
                    f"{len(measurement.corrupt_files)} files failed ffmpeg decode"
                )
            elif bad_audio_hash and measurement.matched_bad_track_path:
                detail = (
                    f"matched bad_audio_hash id={measurement.matched_bad_hash_id} "
                    f"on track {measurement.matched_bad_track_path}"
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

        existing_spectral = measurement.existing_spectral
        existing_grade = (
            existing_spectral.grade
            if existing_spectral is not None
            else existing_spectral_evidence.grade
        )
        existing_bitrate = (
            existing_spectral.bitrate_kbps
            if existing_spectral is not None
            else existing_spectral_evidence.bitrate_kbps
        )
        if (
            persist_candidate_evidence
            and current_evidence is None
            and not current_evidence_authoritative
        ):
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
            existing_v0_probe = audit_v0_probe_from_metric(
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
            if source_snapshot is None or not audio_snapshot_matches(path, source_snapshot):
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
                evidence = persist_candidate_evidence_from_import_result(
                    db,
                    mb_release_id=mbid,
                    source_path=path,
                    import_result=run.import_result,
                    download_log_id=download_log_id,
                    import_job_id=import_job_id,
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
        shutil.rmtree(temp_root, ignore_errors=True)


def preview_import_from_download_log(
    db: ImportPreviewDB,
    download_log_id: int,
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
    request_id_raw = entry.get("request_id")
    if not isinstance(request_id_raw, int):
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="missing_request_id",
            reason="Download log row has no request_id",
            download_log_id=download_log_id,
        )
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
    resolved = resolve_failed_path(raw_path)
    if resolved is None:
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
        download_log_id=download_log_id,
    )
