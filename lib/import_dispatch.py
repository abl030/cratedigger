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
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence, TYPE_CHECKING

import msgspec

from lib import transitions
from lib.quality import (parse_import_result, DispatchAction, DownloadInfo,
                         ImportResult, MeasurementFailure,
                         SpectralMeasurement, V0ProbeEvidence,
                         ValidationResult, QUALITY_UPGRADE_TIERS, QUALITY_LOSSLESS,
                         AlbumQualityEvidence,
                         AlbumQualityEvidenceDecisionFacts,
                         QualityEvidenceActionPayload,
                         QualityEvidenceActionProvenance,
                         dispatch_action, compute_effective_override_bitrate,
                         evidence_decision_name,
                         extract_usernames, is_comparable_lossless_source_probe,
                         full_pipeline_decision_from_evidence,
                         narrow_override_on_downgrade,
                         narrow_override_on_lossless_source_lock,
                         override_bitrate_from_current_evidence,
                         rejection_backfill_override)
from lib.quality_evidence import (
    backfill_current_evidence_from_album_info,
    legacy_current_lossless_v0_probe_from_request,
    lossless_source_v0_probe_from_metric,
    propagate_candidate_evidence_to_current,
    verified_lossless_proof_from_import_result,
)
from lib.import_evidence import (
    CandidateEvidenceActionResult,
    CURRENT_STATUS_MISSING,
    ensure_candidate_evidence_for_action,
    load_current_evidence_for_action,
)
from lib.processing_paths import normalize_source_dirs
from lib.util import (beets_subprocess_env, cleanup_disambiguation_orphans,
                      trigger_meelo_clean)

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.grab_list import GrabListEntry
    from lib.pipeline_db import PipelineDB
    from lib.quality import AudioQualityMeasurement, QualityRankConfig

logger = logging.getLogger("cratedigger")
# U2: when the importer claim arrives without valid candidate evidence
# (missing row, stale snapshot, incomplete), dispatch flips the row back to
# the preview lane via ``PipelineDB.requeue_import_job_for_preview`` and
# returns this code. The importer interprets it as "yield, do NOT
# write terminal failure, do NOT bump retry counters." Preview's next
# sweep recovers the row.
DISPATCH_CODE_REQUEUED_FOR_PREVIEW = "requeued_for_preview"
# U2: when the requeue UPDATE itself raised (DB transient, connection drop),
# dispatch swallows the exception and returns this code so the importer
# leaves the job in ``running`` for ``requeue_running_import_jobs`` on next
# worker boot to recover. NEVER write terminal failure on this code.
DISPATCH_CODE_REQUEUE_FAILED = "requeue_failed"
# U4: programmer-error code returned by ``dispatch_import_from_db`` when
# neither ``import_job_id`` nor ``download_log_id`` is supplied. After U3
# the only production caller (``scripts/importer.py``) always supplies
# ``import_job_id``, so this code only surfaces from test seams or future
# misuse. The legacy direct-measurement branch that previously handled
# this case was deleted in U4 because no production path reaches it.
DISPATCH_CODE_BAD_REQUEST = "bad_request"
# Canonical terminal rejection from ``full_pipeline_decision_from_evidence``.
# Consumers may react to this outcome, but must not re-run a parallel import
# decision to prove it again.
DISPATCH_CODE_QUALITY_PIPELINE_REJECTED = "quality_pipeline_rejected"

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


def _requeue_import_job_to_preview(
    db: "PipelineDB",
    *,
    import_job_id: int | None,
    reason: str,
) -> "DispatchOutcome":
    """Shared requeue helper for the two outer evidence-required branches.

    Called from ``_dispatch_import_from_db_locked`` (force/manual) and from
    ``lib.download._process_beets_validation`` (automation) when
    ``ensure_candidate_evidence_for_action`` reports the candidate row is
    missing, stale, or incomplete.

    Lock context differs by caller. The force/manual call site holds the
    per-request IMPORT advisory lock; the automation call site holds the
    RELEASE lock. Either way the evidence-check + state-flip pair sits
    inside whatever lock the caller already has, which is sufficient for
    importer-vs-importer atomicity (only one importer worker drains the
    queue serially) — concurrent preview-worker claims of a still-running
    job are prevented by the importer's own ``status='running'``
    invariant, not by this lock.

    If the requeue UPDATE itself raises (DB transient), we swallow and
    return ``DISPATCH_CODE_REQUEUE_FAILED`` — the job stays in
    ``running`` for ``requeue_running_import_jobs`` on next importer
    boot to recover.

    ``import_job_id=None`` covers the automation pre-import branch in
    ``lib/download.py`` for paths that did not enqueue an import_job
    (legacy or test seam). Returns a hard requeue-failed outcome in
    that case rather than crashing — there's no row to flip.
    """
    detail = f"Candidate quality evidence unavailable at import time: {reason}"
    if import_job_id is None:
        # No row to flip. Report as a requeue failure so the importer
        # leaves the job (if any) in running for startup recovery.
        return DispatchOutcome(
            success=False,
            message=detail + " (no import_job_id; cannot requeue)",
            code=DISPATCH_CODE_REQUEUE_FAILED,
        )
    try:
        updated = db.requeue_import_job_for_preview(import_job_id, reason=reason)
    except Exception as exc:  # noqa: BLE001 — swallow + log for retry
        logger.exception(
            "Failed to requeue import_job %s for preview", import_job_id
        )
        return DispatchOutcome(
            success=False,
            message=f"Requeue to preview failed: {type(exc).__name__}: {exc}",
            code=DISPATCH_CODE_REQUEUE_FAILED,
        )
    if updated is None:
        # Row was not in ``status='running'`` when the UPDATE fired — either
        # already requeued by a concurrent worker, terminal, or never existed.
        # Conflating this with a successful requeue would hide drift; report
        # as a requeue failure so startup recovery handles whatever state the
        # job is actually in.
        logger.warning(
            "Requeue for import_job %s matched zero rows; job may already be "
            "requeued or terminal", import_job_id
        )
        return DispatchOutcome(
            success=False,
            message=detail + " (requeue UPDATE matched zero rows)",
            code=DISPATCH_CODE_REQUEUE_FAILED,
        )
    return DispatchOutcome(
        success=False,
        message=detail + "; requeued for preview",
        code=DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
    )


@dataclass(frozen=True)
class DispatchOutcome:
    """Summary of an import outcome."""

    success: bool
    message: str
    deferred: bool = False
    code: str | None = None


@dataclass(frozen=True)
class ImportOneRun:
    """Result of one import_one.py subprocess protocol invocation."""

    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    import_result: ImportResult | None


@dataclass(frozen=True)
class EvidenceImportGate:
    """Action-time quality evidence loaded for one mutating import."""

    current: AlbumQualityEvidence | None = None
    candidate: AlbumQualityEvidence | None = None
    candidate_status: str | None = None
    candidate_reason: str | None = None
    current_status: str | None = None
    current_reason: str | None = None
    current_fail_closed: bool = False
    snapshot_guard: str | None = None


def _import_allowed_by_evidence_pipeline(result: dict[str, object]) -> bool:
    return bool(result.get("imported"))


def _current_evidence_allows_action(gate: EvidenceImportGate) -> bool:
    """Current evidence may be absent only when Beets has no current album."""

    return not gate.current_fail_closed


def _download_info_from_candidate_evidence(
    candidate: AlbumQualityEvidence,
    *,
    username: str | None,
) -> DownloadInfo:
    """Build the force/manual audit info without remeasuring the candidate."""

    measurement = candidate.measurement
    bitrate = (
        measurement.min_bitrate_kbps * 1000
        if measurement.min_bitrate_kbps is not None
        else None
    )
    return DownloadInfo(
        username=username,
        filetype=(
            candidate.storage_format
            or measurement.format
            or candidate.container
            or candidate.codec
        ),
        bitrate=bitrate,
        is_vbr=not measurement.is_cbr,
        download_spectral=SpectralMeasurement.from_parts(
            measurement.spectral_grade,
            measurement.spectral_bitrate_kbps,
        ),
        v0_probe=lossless_source_v0_probe_from_metric(candidate.v0_metric),
    )


# U11: ``_build_preimport_measurement_from_evidence``,
# ``_PREIMPORT_REJECT_DENYLIST_REASONS``, and
# ``_route_preimport_decision_reject`` have all been folded into the unified
# decider + reject helper. The four folder/audio-integrity facts are now
# early-exit branches inside ``full_pipeline_decision_from_evidence``; the
# unified ``_reject_import_from_evidence_decision`` below handles their
# denylist policy + forced-requeue invariant alongside the existing
# quality-side rejects. See CLAUDE.md § "Quality decisions live in ONE place".


# Reject reasons that come from folder/audio-integrity facts persisted on
# ``AlbumQualityEvidence`` (formerly emitted by ``preimport_decide``). The
# unified reject helper forces ``requeue=True`` for these regardless of the
# caller's ``requeue_on_failure`` flag — the candidate failed *upstream* of
# any beets mutation, so the parent request must always self-heal back to
# ``wanted`` even when the operator chose force/manual import.
_PREIMPORT_FACT_REJECT_DECISIONS: frozenset[str] = frozenset({
    "audio_corrupt",
    "bad_audio_hash",
    "nested_layout",
    "empty_fileset",
})


def _write_quality_evidence_action_file(
    *,
    candidate: AlbumQualityEvidence,
    current: AlbumQualityEvidence | None,
    decision: dict[str, object],
    target_format: str | None,
    verified_lossless_target: str,
    gate: EvidenceImportGate,
) -> str:
    """Write the action-time evidence payload consumed by import_one.py."""

    payload = QualityEvidenceActionPayload(
        candidate=candidate,
        current=current,
        decision=decision,
        decision_name=evidence_decision_name(decision),
        target_format=target_format,
        verified_lossless_target=verified_lossless_target or None,
        provenance=QualityEvidenceActionProvenance(
            candidate_status=gate.candidate_status,
            current_status=gate.current_status,
            snapshot_status=gate.snapshot_guard,
            fallback_reason=gate.candidate_reason,
        ),
    )
    handle = tempfile.NamedTemporaryFile(
        prefix="cratedigger-quality-evidence-action-",
        suffix=".json",
        delete=False,
    )
    try:
        handle.write(msgspec.json.encode(payload))
        return handle.name
    finally:
        handle.close()


def _remove_quality_evidence_action_file(path: str | None) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        logger.debug(
            "Failed to remove quality evidence action file %s",
            path,
            exc_info=True,
        )


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
    quality_evidence_action_file: str | None = None,
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
    if quality_evidence_action_file:
        cmd.extend(["--quality-evidence-action-file", quality_evidence_action_file])
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
    quality_evidence_action_file: str | None = None,
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
        quality_evidence_action_file=quality_evidence_action_file,
    )
    result = sp.run(
        cmd,
        capture_output=True,
        text=True,
        errors="replace",
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


def _load_evidence_import_gate(
    db: "PipelineDB",
    *,
    request_id: int,
    mb_release_id: str,
    path: str,
    quality_ranks: "QualityRankConfig | None",
    candidate_import_job_id: int | None,
    candidate_download_log_id: int | None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
    beets_library_root: str = "",
) -> EvidenceImportGate:
    """Load persisted evidence for import-time quality authority."""

    if candidate_import_job_id is None and candidate_download_log_id is None:
        return EvidenceImportGate()

    candidate_result = prevalidated_candidate_result
    if candidate_result is None:
        candidate_result = ensure_candidate_evidence_for_action(
            db,
            source_path=path,
            import_job_id=candidate_import_job_id,
            download_log_id=candidate_download_log_id,
        )
    if not candidate_result.available:
        return EvidenceImportGate(
            candidate=None,
            candidate_status=candidate_result.provenance.candidate_status,
            candidate_reason=candidate_result.provenance.fallback_reason,
            snapshot_guard=candidate_result.provenance.snapshot_guard,
        )

    current_result = load_current_evidence_for_action(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        quality_ranks=quality_ranks,
        beets_library_root=beets_library_root,
    )
    if current_result is None:
        return EvidenceImportGate(
            current=None,
            candidate=candidate_result.evidence,
            candidate_status=candidate_result.provenance.candidate_status,
            candidate_reason=candidate_result.provenance.fallback_reason,
            current_status=CURRENT_STATUS_MISSING,
            current_reason="album not in beets",
            current_fail_closed=False,
            snapshot_guard=candidate_result.provenance.snapshot_guard,
        )

    return EvidenceImportGate(
        current=current_result.evidence if current_result.available else None,
        candidate=candidate_result.evidence,
        candidate_status=candidate_result.provenance.candidate_status,
        candidate_reason=candidate_result.provenance.fallback_reason,
        current_status=current_result.provenance.current_status,
        current_reason=current_result.provenance.fallback_reason,
        current_fail_closed=current_result.provenance.fail_closed,
        snapshot_guard=candidate_result.provenance.snapshot_guard,
    )


def _refresh_current_evidence_after_import(
    db: "PipelineDB",
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: "QualityRankConfig | None",
    source_candidate: AlbumQualityEvidence | None = None,
    import_result: ImportResult | None = None,
    beets_library_root: str = "",
) -> None:
    """Persist current evidence for the just-imported Beets album.

    When ``source_candidate`` is supplied (the normal post-U10 path), the new
    library-side evidence row is built by propagating the candidate's full
    measurement payload — see
    :func:`lib.quality_evidence.propagate_candidate_evidence_to_current`.
    Renamed-only imports inherit spectral grade, V0 lineage, and
    bad-audio-hash matches; transcoded imports inherit only the verified-
    lossless proof. Bitrate/format always re-derive from ``album_info``
    (dual-check against the candidate measurement).

    When ``source_candidate`` is ``None`` (rare — legacy callers, an evidence
    record that vanished, or non-post-import callers reusing this helper),
    fall back to the pre-U10 ``backfill_current_evidence_from_album_info``
    path. That path rebuilds evidence from beets fields plus a carried-
    forward ``verified_lossless_proof`` and is preserved for non-post-import
    callers (e.g., wrong-match triage backfilling library evidence for
    pre-refactor albums).
    """

    from lib.beets_db import BeetsDB
    from lib.quality import QualityRankConfig

    cfg = quality_ranks if quality_ranks is not None else QualityRankConfig.defaults()
    # ``beets_library_root`` must be set: ``BeetsDB.get_album_info`` returns a
    # path *relative* to the library root when constructed without one, which
    # breaks ``snapshot_audio_files`` (host-side filesystem ops) — see the
    # BeetsDB docstring. Both the U10 propagation path and the legacy
    # ``backfill_current_evidence_from_album_info`` path depend on an
    # absolute ``album_info.album_path`` to read the just-imported files.
    with BeetsDB(library_root=beets_library_root) as beets:
        album_info = beets.get_album_info(mb_release_id, cfg)
    if album_info is None:
        return

    if source_candidate is not None:
        propagate_candidate_evidence_to_current(
            db,
            request_id=request_id,
            candidate_evidence=source_candidate,
            album_info=album_info,
        )
        return

    # Legacy fallback: no candidate evidence on hand. Rebuild from beets +
    # carry-forward verified_lossless_proof from the import_result, matching
    # pre-U10 behaviour exactly.
    decision = import_result.decision if import_result is not None else None
    verified_lossless_proof = None
    if decision != "preflight_existing":
        verified_lossless_proof = (
            verified_lossless_proof_from_import_result(import_result)
            if import_result is not None
            else None
        )
    backfill_current_evidence_from_album_info(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        album_info=album_info,
        verified_lossless_proof=verified_lossless_proof,
        preserve_existing_verified_lossless_proof=(
            import_result is None or decision == "preflight_existing"
        ),
    )


def _reject_import_from_evidence_decision(
    *,
    db: "PipelineDB",
    request_id: int,
    dl_info: DownloadInfo,
    import_result: ImportResult,
    distance: float,
    decision: str,
    detail: str,
    requeue_on_failure: bool,
    validation_result: str | None,
    staged_path: str,
    scenario: str,
    files: Sequence[object] | None,
    source_path_cleanup_scenario: str,
    cooled_down_users: set[str] | None,
) -> DispatchOutcome:
    """Record a persisted-evidence rejection before beets can mutate files.

    Unified rejection helper for every ``full_pipeline_decision_from_evidence``
    reject outcome — quality-side (downgrade / suspect_lossless / etc.) AND
    folder/audio-integrity (audio_corrupt / bad_audio_hash / nested_layout /
    empty_fileset, formerly routed through the deleted
    ``_route_preimport_decision_reject``). One decision function, one
    rejection helper, one denylist policy.

    Threads ``import_result`` through ``_populate_dl_info_from_import_result``
    so the same top-level ``download_log`` columns the post-import reject
    path populates (``bitrate``, ``actual_filetype``, ``spectral_grade``,
    ``existing_min_bitrate``, ``v0_probe_*``, etc.) get filled here too.
    Without this, the Recents UI rendered evidence-decision rejections
    as just ``"downgrade · username"`` because every quality column
    came back NULL — see ``TestRejectImportFromEvidenceDecision``.

    **U11 forced-requeue invariant.** When ``decision`` names a
    folder/audio-integrity fact (``_PREIMPORT_FACT_REJECT_DECISIONS``), the
    helper forces ``requeue=True`` regardless of the caller's
    ``requeue_on_failure`` flag. These rejects fire upstream of any beets
    mutation and upstream of any operator intent — the album is still
    desired, only this specific source is bad — so the parent request must
    always self-heal back to ``wanted``. Quality-side rejects continue to
    honour ``requeue_on_failure`` (force/manual paths pass ``False``
    because the operator already chose to act on this source).
    """

    _populate_dl_info_from_import_result(dl_info, import_result)
    action = dispatch_action(decision)
    # U11: force requeue on folder/audio-integrity rejects (formerly the
    # invariant enforced by the deleted ``_route_preimport_decision_reject``).
    effective_requeue = requeue_on_failure or decision in _PREIMPORT_FACT_REJECT_DECISIONS
    _record_rejection_and_maybe_requeue(
        db,
        request_id,
        dl_info,
        distance=distance,
        scenario=decision or scenario,
        detail=detail,
        error=None,
        requeue=effective_requeue,
        outcome_label="rejected",
        validation_result=validation_result,
        staged_path=staged_path,
    )
    if action.denylist:
        usernames = extract_usernames(files or [])
        if dl_info.username:
            usernames.add(dl_info.username)
        # Unified denylist policy. Quality-side and four-fact reject reasons
        # both live here — formerly split across ``_route_preimport_decision_reject``
        # (folder/audio-integrity) and the quality-side branch below.
        reason = (
            "quality downgrade prevented"
            if decision == "downgrade"
            else "suspect lossless source not an upgrade"
            if decision.startswith("suspect_lossless")
            else "lossless source locked"
            if decision == "lossless_source_locked"
            else "audio decode failures"
            if decision == "audio_corrupt"
            else "matched curated bad audio hash"
            if decision == "bad_audio_hash"
            else "spectral analysis rejected the source"
            if decision == "spectral_reject"
            else f"rejected: {decision}"
        )
        for username in usernames:
            db.add_denylist(request_id, username, reason)
            if cooled_down_users is not None:
                if db.check_and_apply_cooldown(username):
                    cooled_down_users.add(username)
    if action.cleanup and _should_cleanup_path(source_path_cleanup_scenario, action):
        _cleanup_staged_dir(staged_path)
    return DispatchOutcome(
        success=False,
        message=f"Rejected by persisted quality evidence: {decision}",
        code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
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


def _finalize_request_and_log_rejection(
    db: "PipelineDB",
    request_id: int | None,
    log_download_kwargs: dict[str, Any],
    *,
    requeue_to_wanted: bool,
    search_filetype_override: str | None = None,
    record_validation_attempt: bool = True,
    import_job_id: int | None = None,
    import_job_error: str = "",
    import_job_message: str | None = None,
    import_job_result: dict[str, Any] | None = None,
    denylist_username: str | None = None,
    denylist_reason: str | None = None,
) -> int:
    """Sole writer of the four self-healing rejection side effects (U4).

    The single source of truth for "a candidate was rejected; clean up
    state so the parent request can advance." Both the importer-side
    ``_record_rejection_and_maybe_requeue`` (with full ``DownloadInfo``
    context) and the preview-side ``_record_preview_measurement_failed``
    (without slskd context) delegate here. Grep for this function name to
    find every call site — there should be exactly two production callers.

    Side effects, in order:

      1. Optional request → ``wanted`` transition via
         ``transitions.finalize_request`` (skipped when ``request_id is
         None`` or ``requeue_to_wanted=False``). When the transition
         fires and ``record_validation_attempt=True``, also bumps the
         validation attempt counter — matches pre-U4 importer behavior.
      2. ``download_log`` row write via ``db.log_download(**log_download_kwargs)``.
         Always fires. Returns the new row id.
      3. ``source_denylist`` write when ``denylist_username`` is supplied
         AND ``request_id is not None`` (denylist FK-references a
         request). The importer-side entry point currently passes None
         here and handles denylist externally; the preview-side path
         passes a username when the 5-strikes rule applies.
      4. ``import_jobs.status='failed'`` via ``mark_import_job_failed``
         when ``import_job_id`` is supplied. The importer-side caller
         leaves this to the worker (``scripts/importer.py``) so it
         continues to pass None here; the preview-side caller fires it
         so the poll loop's active-import-job guard releases.

    Returns the new ``download_log`` row id. The ``request_not_found``
    subcase (``request_id is None``) writes the log + marks the job
    failed but skips finalize_request and denylist.
    """
    if requeue_to_wanted and request_id is not None:
        transition_kwargs: dict[str, object] = {}
        if search_filetype_override is not None:
            transition_kwargs["search_filetype_override"] = search_filetype_override
        transitions.finalize_request(
            db,
            request_id,
            transitions.RequestTransition.to_wanted_fields(
                fields=transition_kwargs),
        )
        if record_validation_attempt:
            db.record_attempt(request_id, "validation")

    download_log_id = db.log_download(
        request_id=request_id,
        **log_download_kwargs,
    )

    if denylist_username and request_id is not None:
        db.add_denylist(request_id, denylist_username, reason=denylist_reason)

    if import_job_id is not None:
        db.mark_import_job_failed(
            import_job_id,
            error=import_job_error,
            message=import_job_message,
            result=import_job_result,
        )

    return download_log_id


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
) -> int:
    """Importer-side rejection entry point.

    Builds the ``log_download`` kwargs from ``DownloadInfo`` (slskd context:
    username, bitrate, spectral, V0 probe, etc.) and delegates to
    ``_finalize_request_and_log_rejection``. Behavior is preserved from
    pre-U4: optional requeue-to-wanted with attempt bump, mandatory
    download_log row, no denylist (caller handles via ``action.denylist``
    in ``dispatch_import_core``), no job-failed mark (caller in
    ``scripts/importer.py`` handles it on the outer return).

    When ``requeue=True`` (auto-import): transitions to "wanted", records
    attempt. When ``requeue=False`` (force/manual import): only logs to
    download_log.

    Returns the new ``download_log`` row id — captured by the
    auto-import path for downstream Wrong Matches triage.
    """
    log_download_kwargs: dict[str, Any] = {
        "soulseek_username": dl_info.username,
        "filetype": dl_info.filetype,
        "beets_distance": distance,
        "beets_scenario": scenario,
        "beets_detail": detail,
        "outcome": outcome_label,
        "staged_path": staged_path,
        "error_message": error,
        "bitrate": dl_info.bitrate,
        "sample_rate": dl_info.sample_rate,
        "bit_depth": dl_info.bit_depth,
        "is_vbr": dl_info.is_vbr,
        "was_converted": dl_info.was_converted,
        "original_filetype": dl_info.original_filetype,
        "slskd_filetype": dl_info.slskd_filetype,
        "slskd_bitrate": dl_info.slskd_bitrate,
        "actual_filetype": dl_info.actual_filetype,
        "actual_min_bitrate": dl_info.actual_min_bitrate,
        "spectral_grade": (dl_info.download_spectral.grade
                           if dl_info.download_spectral else None),
        "spectral_bitrate": (dl_info.download_spectral.bitrate_kbps
                             if dl_info.download_spectral else None),
        "existing_min_bitrate": dl_info.existing_min_bitrate,
        "existing_spectral_bitrate": (dl_info.current_spectral.bitrate_kbps
                                      if dl_info.current_spectral else None),
        "import_result": dl_info.import_result,
        "validation_result": (validation_result
                              if validation_result is not None
                              else dl_info.validation_result),
    }
    log_download_kwargs.update(_v0_probe_log_fields(dl_info))
    return _finalize_request_and_log_rejection(
        db,
        request_id,
        log_download_kwargs,
        requeue_to_wanted=requeue,
        search_filetype_override=search_filetype_override,
        record_validation_attempt=True,
        # Importer-side leaves job-failed + denylist to its caller.
        import_job_id=None,
        denylist_username=None,
    )


def _record_preview_measurement_failed(
    db: "PipelineDB",
    *,
    request_id: int | None,
    import_job_id: int,
    payload: MeasurementFailure,
    denylist_username: str | None = None,
    denylist_reason: str | None = None,
) -> int:
    """Preview-side measurement_failed entry point (U4).

    Called when preview cannot produce evidence — measurement crashed, the
    source folder vanished, the snapshot went stale after retry, or one of
    the pre-claim sanity checks failed (request_not_found, missing MBID,
    etc.). Has no slskd context because no transfer is in flight; the
    ``download_log`` row carries NULL for username/bitrate/filetype/spectral
    columns and the typed ``MeasurementFailure`` payload as its
    ``validation_result`` JSONB.

    Delegates to ``_finalize_request_and_log_rejection`` for the four
    self-healing side effects:

      * ``download_log`` row written with ``outcome='measurement_failed'``,
        ``beets_scenario='measurement_failed'``, and the
        ``MeasurementFailure`` JSON as ``validation_result``.
      * Parent request → ``wanted`` via ``transitions.finalize_request``
        (skipped when ``request_id is None`` — the
        ``reason='request_not_found'`` subcase).
      * Optional denylist write when ``denylist_username`` is supplied.
      * ``import_jobs.status='failed'`` via ``mark_import_job_failed`` so
        the poll loop's active-import-job guard releases on the next tick.

    Returns the new ``download_log`` row id.
    """
    requeue = request_id is not None
    validation_json = msgspec.json.encode(payload).decode("utf-8")
    log_download_kwargs: dict[str, Any] = {
        # NULL for all slskd-only fields — preview has no transfer context.
        "soulseek_username": None,
        "filetype": None,
        "beets_distance": None,
        "beets_scenario": "measurement_failed",
        "beets_detail": payload.detail,
        "outcome": "measurement_failed",
        "staged_path": payload.source_path or None,
        "error_message": None,
        "validation_result": validation_json,
    }
    job_result = msgspec.to_builtins(payload)
    assert isinstance(job_result, dict), \
        "msgspec.to_builtins on a Struct returns a dict"
    return _finalize_request_and_log_rejection(
        db,
        request_id,
        log_download_kwargs,
        requeue_to_wanted=requeue,
        record_validation_attempt=False,  # preview failures aren't validation attempts
        import_job_id=import_job_id,
        import_job_error=payload.reason,
        import_job_message=payload.detail,
        import_job_result=job_result,
        denylist_username=denylist_username,
        denylist_reason=denylist_reason,
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
    source_dirs: list[str] | None = None,
    candidate_import_job_id: int | None = None,
    candidate_download_log_id: int | None = None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
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

    source_dirs = normalize_source_dirs(source_dirs or [])

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
            #   ``measure_preimport_state`` re-runs on retry and
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

        quality_evidence_action_file: str | None = None
        try:
            try:
                request_row = db.get_request(request_id)
            except Exception:
                logger.debug(
                    "Failed to read request row before import",
                    exc_info=True,
                )
                request_row = None

            evidence_gate = _load_evidence_import_gate(
                db,
                request_id=request_id,
                mb_release_id=mb_release_id,
                path=path,
                quality_ranks=cfg.quality_ranks if cfg is not None else None,
                candidate_import_job_id=candidate_import_job_id,
                candidate_download_log_id=candidate_download_log_id,
                prevalidated_candidate_result=prevalidated_candidate_result,
                beets_library_root=getattr(cfg, "beets_directory", "") if cfg is not None else "",
            )
            existing_v0_probe = lossless_source_v0_probe_from_metric(
                evidence_gate.current.v0_metric
                if evidence_gate.current is not None
                else None
            )
            evidence_override = override_bitrate_from_current_evidence(
                evidence_gate.current
            )
            if evidence_override is not None:
                override_min_bitrate = evidence_override
            if existing_v0_probe is None:
                existing_v0_probe = legacy_current_lossless_v0_probe_from_request(
                    request_row)

            if (
                (candidate_import_job_id is not None
                 or candidate_download_log_id is not None)
                and evidence_gate.candidate is None
            ):
                # U4: outer callers (``_dispatch_import_from_db_locked`` and
                # ``lib/download.py::_process_beets_validation``) already
                # call ``ensure_candidate_evidence_for_action`` and requeue
                # via ``_requeue_import_job_to_preview`` when evidence is
                # missing. Reaching this inner site means a caller bypassed
                # the outer gate (test seam or future misuse). Behave
                # consistently with the outer invariant — requeue rather
                # than hard-fail — so the importer never measures and
                # never writes a terminal failure on missing evidence.
                reason = evidence_gate.candidate_reason or evidence_gate.candidate_status
                return _requeue_import_job_to_preview(
                    db,
                    import_job_id=candidate_import_job_id,
                    reason=reason or "missing",
                )
            if evidence_gate.candidate is not None and not _current_evidence_allows_action(
                evidence_gate
            ):
                reason = evidence_gate.current_reason or evidence_gate.current_status
                return DispatchOutcome(
                    success=False,
                    message=(
                        "Current quality evidence unavailable at import "
                        f"time: {reason or 'missing'}"
                    ),
                )

            if evidence_gate.candidate is not None:
                # U11: ``full_pipeline_decision_from_evidence`` is the single
                # decision function. Folder/audio-integrity facts
                # (audio_corrupt / bad_audio_hash / nested_layout /
                # empty_fileset) are early-exit rejects at the top of that
                # function — the unified reject helper below recognises them
                # via ``_PREIMPORT_FACT_REJECT_DECISIONS`` and forces
                # ``requeue=True`` so the parent request self-heals.
                facts = AlbumQualityEvidenceDecisionFacts(
                    import_mode=(
                        "force"
                        if force
                        else "manual"
                        if scenario == "manual_import"
                        else "auto"
                    ),
                    verified_lossless_target=verified_lossless_target or None,
                    target_format=target_format,
                )
                evidence_decision = full_pipeline_decision_from_evidence(
                    evidence_gate.candidate,
                    evidence_gate.current,
                    facts=facts,
                    cfg=cfg.quality_ranks if cfg is not None else None,
                )
                if not _import_allowed_by_evidence_pipeline(evidence_decision):
                    decision = evidence_decision_name(evidence_decision)
                    detail = (
                        "import-time persisted evidence rejected candidate "
                        f"(decision={decision})"
                    )
                    evidence_import_result = ImportResult(
                        decision=decision,
                        new_measurement=evidence_gate.candidate.measurement,
                        existing_measurement=(
                            evidence_gate.current.measurement
                            if evidence_gate.current is not None
                            else None
                        ),
                        v0_probe=lossless_source_v0_probe_from_metric(
                            evidence_gate.candidate.v0_metric
                        ),
                        existing_v0_probe=existing_v0_probe,
                    )
                    return _reject_import_from_evidence_decision(
                        db=db,
                        request_id=request_id,
                        dl_info=dl_info,
                        import_result=evidence_import_result,
                        distance=distance,
                        decision=decision,
                        detail=detail,
                        requeue_on_failure=requeue_on_failure,
                        validation_result=dl_info.validation_result,
                        staged_path=path,
                        scenario=scenario,
                        files=files,
                        source_path_cleanup_scenario=scenario,
                        cooled_down_users=cooled_down_users,
                    )
                quality_evidence_action_file = _write_quality_evidence_action_file(
                    candidate=evidence_gate.candidate,
                    current=evidence_gate.current,
                    decision=evidence_decision,
                    target_format=target_format,
                    verified_lossless_target=verified_lossless_target,
                    gate=evidence_gate,
                )
            # Mark the subprocess as launching on the auto-import path
            # so the resume guard can distinguish "never started" from
            # "may have written to beets" if this process crashes
            # before recording the result. The DB-side method is a
            # no-op when ``active_download_state`` is NULL (force /
            # manual paths), so calling unconditionally would also be
            # safe — we still gate to make the intent explicit.
            # See ``docs/advisory-locks.md`` and
            # ``lib/download.py::_import_subprocess_already_started``.
            if scenario not in FORCE_MANUAL_SCENARIOS:
                try:
                    db.mark_import_subprocess_started(
                        request_id,
                        datetime.now(timezone.utc).isoformat(),
                    )
                except Exception:
                    logger.exception(
                        "Failed to stamp import_subprocess_started_at "
                        "for request %s; continuing with subprocess "
                        "launch (resume guard may fail-open until "
                        "completion)",
                        request_id,
                    )
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
                quality_evidence_action_file=quality_evidence_action_file,
            )
            _remove_quality_evidence_action_file(quality_evidence_action_file)
            quality_evidence_action_file = None
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
                        source_dirs=source_dirs,
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
                    try:
                        _refresh_current_evidence_after_import(
                            db,
                            request_id=request_id,
                            mb_release_id=mb_release_id,
                            quality_ranks=(
                                cfg.quality_ranks if cfg is not None else None
                            ),
                            source_candidate=evidence_gate.candidate,
                            import_result=ir,
                            beets_library_root=(
                                cfg.beets_directory if cfg is not None else ""
                            ),
                        )
                    except Exception:
                        logger.exception(
                            "Failed to refresh current quality evidence "
                            "after import for request %s",
                            request_id,
                        )
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
                    elif decision == "lossless_source_locked":
                        fail_scenario = "lossless_source_locked"
                        existing_avg = (
                            ir.existing_v0_probe.avg_bitrate_kbps
                            if ir.existing_v0_probe else None
                        )
                        fail_detail = ir.error or (
                            f"lossy candidate cannot override existing "
                            f"lossless-source V0 probe {existing_avg}kbps"
                        )
                        logger.warning(
                            f"LOSSLESS SOURCE LOCKED: {label} "
                            f"existing_v0_avg={existing_avg}kbps")
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
                            "lossless_source_locked",
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

                    elif decision == "lossless_source_locked":
                        # R7 / AE2: once the library row carries a comparable
                        # lossless-source V0 probe, no lossy candidate can
                        # override it. Narrow the search to lossless-only so
                        # future cycles stop re-finding lossy candidates that
                        # would just hit the lock again. See
                        # docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md
                        try:
                            req_row = db.get_request(request_id)
                            current_override = (
                                req_row.get("search_filetype_override")
                                if req_row else None
                            )
                            narrowed_override = narrow_override_on_lossless_source_lock(
                                current_override)
                        except Exception:
                            logger.debug(
                                "Failed to inspect search_filetype_override"
                                " before lossless_source_locked narrow")

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
                                               source_dirs=source_dirs,
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

                # Rejected auto-imports are already requeued by
                # _record_rejection_and_maybe_requeue(), which preserves retry
                # counters and records the validation attempt. This second
                # requeue is only for successful imports that intentionally go
                # back to wanted to keep searching for a better source.
                if action.requeue and action.mark_done:
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
                    removed = cleanup_disambiguation_orphans(
                        ir.postflight.imported_path,
                        beets_directory=cfg.beets_directory if cfg is not None else "",
                    )
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
                    source_dirs=source_dirs,
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
                    source_dirs=source_dirs,
                ).to_json(),
                staged_path=path)
            outcome_message = "Unhandled exception"
        finally:
            _remove_quality_evidence_action_file(quality_evidence_action_file)

    return DispatchOutcome(success=outcome_success, message=outcome_message)

def dispatch_import_from_db(
    db: "PipelineDB",
    request_id: int,
    failed_path: str,
    *,
    force: bool = False,
    outcome_label: str = "force_import",
    source_username: str | None = None,
    source_dirs: list[str] | None = None,
    import_job_id: int | None = None,
    download_log_id: int | None = None,
) -> "DispatchOutcome":
    """Run a force-import or manual-import through the full dispatch pipeline.

    Requires pre-recorded candidate evidence: the caller supplies either
    ``import_job_id`` or ``download_log_id`` (or both), and dispatch loads
    the candidate ``AlbumQualityEvidence`` via
    ``ensure_candidate_evidence_for_action``. The preview worker is the
    only producer of candidate measurements; dispatch never invokes
    ``measure_preimport_state`` itself. When evidence is missing or stale, the
    job is requeued back to the preview lane via
    ``_requeue_import_job_to_preview`` (returning
    ``DISPATCH_CODE_REQUEUED_FOR_PREVIEW``); the actual measurement happens
    on the preview worker's next claim. Quality decisions (downgrade
    prevention, quality gate, meelo scan, denylist) still run identically
    to auto-import — only the beets *distance* check is skipped when
    ``force=True``.

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
        outcome_label: download_log outcome label for successful imports
        source_username: Soulseek peer who supplied the source files
        source_dirs: Remote directories the source was downloaded from
        import_job_id: Import-job row this dispatch belongs to. Required
            in production (the importer always supplies it); ``None`` is
            a developer-error precondition error.
        download_log_id: Originating download_log row for Wrong Matches
            force-imports; scopes candidate-evidence lookup to that
            owner. Optional but typically supplied for force-imports.
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
            source_dirs=source_dirs,
            import_job_id=import_job_id,
            download_log_id=download_log_id,
        )


def _dispatch_import_from_db_locked(
    db: "PipelineDB",
    request_id: int,
    failed_path: str,
    *,
    force: bool,
    outcome_label: str,
    source_username: str | None,
    source_dirs: list[str] | None,
    import_job_id: int | None,
    download_log_id: int | None,
) -> "DispatchOutcome":
    """Body of dispatch_import_from_db, called once the advisory lock is held.

    Precondition: at least one of ``import_job_id`` or ``download_log_id``
    MUST be supplied. After U4 (importer-never-measures refactor) the only
    production caller is ``scripts/importer.py``, which always supplies
    ``import_job_id``. The previous legacy direct-measurement branch that
    ran ``inspect_local_files`` / ``measure_preimport_state`` for callers
    that omitted both IDs has been deleted; the importer never measures.
    """
    from lib.grab_list import DownloadFile

    if import_job_id is None and download_log_id is None:
        # Programmer-error: every production caller supplies at least
        # ``import_job_id``. Reject up front rather than silently measuring.
        return DispatchOutcome(
            success=False,
            message=(
                "dispatch_import_from_db requires import_job_id or "
                "download_log_id (importer never measures; preview owns "
                "candidate evidence production)"
            ),
            code=DISPATCH_CODE_BAD_REQUEST,
        )

    source_dirs = normalize_source_dirs(source_dirs or [])

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

    candidate_result = ensure_candidate_evidence_for_action(
        db,
        source_path=failed_path,
        import_job_id=import_job_id,
        download_log_id=download_log_id,
    )
    if not candidate_result.available or candidate_result.evidence is None:
        reason = (
            candidate_result.provenance.fallback_reason
            or candidate_result.provenance.candidate_status
            or "missing"
        )
        # U2: requeue to preview rather than failing. Preview owns
        # candidate-evidence production; the importer never measures.
        return _requeue_import_job_to_preview(
            db,
            import_job_id=import_job_id,
            reason=reason,
        )
    dl_info = _download_info_from_candidate_evidence(
        candidate_result.evidence,
        username=source_username,
    )
    return dispatch_import_core(
        path=failed_path,
        mb_release_id=mbid,
        request_id=request_id,
        label=label,
        force=force,
        override_min_bitrate=None,
        target_format=req.get("target_format"),
        verified_lossless_target=cfg.verified_lossless_target,
        beets_harness_path=cfg.beets_harness_path,
        db=db,
        dl_info=dl_info,
        distance=0.0,
        scenario="force_import" if force else "manual_import",
        files=files,
        cfg=cfg,
        outcome_label=outcome_label,
        requeue_on_failure=False,
        source_dirs=source_dirs,
        candidate_import_job_id=import_job_id,
        candidate_download_log_id=download_log_id,
        prevalidated_candidate_result=candidate_result,
    )
