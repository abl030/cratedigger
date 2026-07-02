"""Dispatch value types + module-level constants.

Extracted from ``lib/import_dispatch.py`` (issue #139). Holds the typed
results and the taxonomy/scenario constants shared across the dispatch
package. No behaviour; these types are defined above every use site.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.quality import (AlbumQualityEvidence, AudioQualityMeasurement,
                             ImportResult)


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
    "mixed_source",
})



QualityGateFn = Callable[..., None]
"""Type of the post-import quality-gate callable injected into
``dispatch_import_core``. Production passes :func:`_check_quality_gate_core`;
tests can pass a stub or a recorder instead of patching the module
attribute. Signature matches ``_check_quality_gate_core`` (keyword-args
including ``mb_id``, ``label``, ``request_id``, ``files``, ``db``,
``quality_ranks``)."""
