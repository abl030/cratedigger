"""Dispatch value types + module-level constants.

Extracted from ``lib/import_dispatch.py`` (issue #139). Holds the typed
results and the taxonomy/scenario constants shared across the dispatch
package. The import-attempt accumulator here owns result finalization; the
remaining types are value-only definitions shared above every use site.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Protocol, Sequence, TYPE_CHECKING

from lib.wrong_match_policy import PREIMPORT_FACT_REJECTION_SCENARIOS
from lib.terminal_outcomes import ImportJobRequestAction

logger = logging.getLogger("cratedigger")

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.import_evidence import CandidateEvidenceActionResult
    from lib.pipeline_db import DownloadLogOutcome, PipelineDB
    from lib.quality import (AlbumQualityEvidence, AudioQualityMeasurement,
                             DownloadInfo, ImportResult, SpectralDetail,
                             TargetQualityContract)


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
DISPATCH_CODE_IMPORT_MANIFEST_REJECTED = "import_manifest_rejected"

# Scenarios whose ``path`` is the user's source data (``failed_imports/…``),
# NOT a disposable staging directory. Used to gate ``_cleanup_staged_dir``
# so a ``downgrade`` / ``transcode_downgrade`` decision from the harness
# can never delete the user's only copy of the source. Auto-import uses
# bv_result.scenario values like ``strong_match`` / ``weak_match`` /
# ``auto_import``, none of which appear here — their staging dir under
# ``/Incoming`` is always safe to remove (see issue #89).
FORCE_MANUAL_SCENARIOS: frozenset[str] = frozenset({"force_import", "manual_import"})


@dataclass(frozen=True)
class QualityGateState:
    """Resolved on-disk state for a quality-gate evaluation."""
    measurement: AudioQualityMeasurement
    min_bitrate_kbps: int
    spectral_bitrate_kbps: int | None
    spectral_grade: str | None
    target_contract: TargetQualityContract | None = None


@dataclass(frozen=True)
class DispatchOutcome:
    """Summary of an import outcome."""

    success: bool
    message: str
    deferred: bool = False
    code: str | None = None
    request_action: ImportJobRequestAction = ImportJobRequestAction.unchanged
    terminal_outcome_expected: bool = False


class DispatchCoreFn(Protocol):
    """Exact callable contract for the test-injected core dispatch seam.

    Production calls ``dispatch_import_core`` directly. This protocol keeps
    the explicit test seam honest without allowing a ``dict[str, Any]`` splat
    to erase argument checking at the shared boundary.
    """

    def __call__(
        self,
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
        dl_info: "DownloadInfo",
        distance: float | None = None,
        scenario: str = "auto_import",
        files: Sequence[object] | None = None,
        cfg: "CratediggerConfig | None" = None,
        outcome_label: "DownloadLogOutcome" = "success",
        requeue_on_failure: bool = True,
        cooled_down_users: set[str] | None = None,
        source_dirs: list[str] | None = None,
        candidate_import_job_id: int | None = None,
        candidate_download_log_id: int | None = None,
        prevalidated_candidate_result: "CandidateEvidenceActionResult | None" = None,
        quality_gate_fn: "QualityGateFn" = ...,
    ) -> DispatchOutcome: ...


@dataclass(frozen=True)
class ImportOneRun:
    """Result of one import_one.py subprocess protocol invocation."""

    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    import_result: ImportResult | None


@dataclass
class ImportAttemptResult:
    """Own the richest result persisted for one dispatch attempt.

    The preview audit is display-only state. Harness results and later
    postflight mutations flow through this owner; serialization happens only
    when a terminal download-log writer calls :meth:`finalize_into`.
    """

    _audit: "SpectralDetail | None"
    _result: "ImportResult | None" = field(init=False, default=None, repr=False)

    @property
    def audit(self) -> "SpectralDetail | None":
        return self._audit

    @property
    def result(self) -> "ImportResult | None":
        return self._result

    @classmethod
    def from_import_job(
        cls,
        db: "PipelineDB",
        import_job_id: int | None,
        audit: "SpectralDetail | None" = None,
    ) -> "ImportAttemptResult":
        """Recover the preview audit once, before any terminal branch."""
        if audit is not None or import_job_id is None:
            return cls(audit)
        try:
            job = db.get_import_job(import_job_id)
            raw = (
                job.preview_result.get("import_result")
                if job is not None and job.preview_result is not None
                else None
            )
            if isinstance(raw, dict):
                from lib.quality import ImportResult
                audit = ImportResult.from_dict(raw).spectral
        except Exception:
            logger.warning(
                "Unable to decode preview spectral audit for import job %s",
                import_job_id,
                exc_info=True,
            )
        return cls(audit)

    def merge(self, result: "ImportResult") -> "ImportResult":
        if self._audit is not None:
            result.spectral = self._audit
        self._result = result
        return result

    def apply(self, mutation: Callable[["ImportResult"], None]) -> None:
        if self._result is None:
            raise RuntimeError("cannot mutate an import attempt before a result exists")
        mutation(self._result)

    def finalize_into(self, dl_info: "DownloadInfo") -> None:
        result = self._result
        if result is None and self._audit is not None:
            from lib.quality import ImportResult
            result = ImportResult(spectral=self._audit)
            self._result = result
        dl_info.import_result = result.to_json() if result is not None else None


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
_PREIMPORT_FACT_REJECT_DECISIONS = PREIMPORT_FACT_REJECTION_SCENARIOS



QualityGateFn = Callable[..., None]
"""Type of the post-import quality-gate callable injected into
``dispatch_import_core``. Production passes :func:`_check_quality_gate_core`;
tests can pass a stub or a recorder instead of patching the module
attribute. Signature matches ``_check_quality_gate_core`` (keyword-args
including ``mb_id``, ``label``, ``request_id``, ``files``, ``db``,
``quality_ranks``)."""
