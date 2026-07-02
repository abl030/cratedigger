"""lib.dispatch — import dispatch package (split from lib/import_dispatch.py, issue #139).

Re-exports the historically-importable surface so ``from lib.dispatch import X``
works for every X callers/tests used to import from ``lib.import_dispatch``.
Patch targets moved to concrete submodules; see docstrings there.
"""

from __future__ import annotations

from lib.dispatch.types import (
    DISPATCH_CODE_BAD_REQUEST,
    DISPATCH_CODE_IMPORT_MANIFEST_REJECTED,
    DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
    DISPATCH_CODE_REQUEUE_FAILED,
    DispatchOutcome,
    QualityGateFn,
)
from lib.dispatch.subprocess_runner import (
    build_import_one_command,
    run_import_one,
)
from lib.dispatch.helpers import (
    _build_download_info,
    _cleanup_staged_dir,
    _populate_dl_info_from_import_result,
)
from lib.dispatch.evidence_gate import (
    _download_info_from_candidate_evidence,
    _load_evidence_import_gate,
    _refresh_current_evidence_after_import,
    _requeue_import_job_to_preview,
    _write_album_sidecar_after_import,
)
from lib.dispatch.outcome_actions import (
    _do_mark_done,
    _record_preview_measurement_failed,
    _record_rejection_and_maybe_requeue,
    _reject_import_from_evidence_decision,
)
from lib.dispatch.quality_gate import (
    _check_quality_gate_core,
    load_quality_gate_state,
)
from lib.dispatch.core import (
    dispatch_import_core,
)
from lib.dispatch.entry_points import (
    dispatch_import_from_db,
)

__all__ = [
    "DISPATCH_CODE_BAD_REQUEST",
    "DISPATCH_CODE_IMPORT_MANIFEST_REJECTED",
    "DISPATCH_CODE_QUALITY_PIPELINE_REJECTED",
    "DISPATCH_CODE_REQUEUED_FOR_PREVIEW",
    "DISPATCH_CODE_REQUEUE_FAILED",
    "DispatchOutcome",
    "QualityGateFn",
    "_build_download_info",
    "_check_quality_gate_core",
    "_cleanup_staged_dir",
    "_do_mark_done",
    "_download_info_from_candidate_evidence",
    "_load_evidence_import_gate",
    "_populate_dl_info_from_import_result",
    "_record_preview_measurement_failed",
    "_record_rejection_and_maybe_requeue",
    "_refresh_current_evidence_after_import",
    "_reject_import_from_evidence_decision",
    "_requeue_import_job_to_preview",
    "_write_album_sidecar_after_import",
    "build_import_one_command",
    "dispatch_import_core",
    "dispatch_import_from_db",
    "load_quality_gate_state",
    "run_import_one",
]
