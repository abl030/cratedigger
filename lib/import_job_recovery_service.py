"""Operator resolution for ambiguous Beets import operations (#703)."""

from __future__ import annotations

import logging
from typing import Protocol

import msgspec

from lib.import_queue import ImportJob

logger = logging.getLogger("cratedigger")


RECOVERY_RESOLUTION_RETRY = "retry"
RECOVERY_RESOLUTION_CLOSE = "close"
RECOVERY_RESOLUTIONS = frozenset({
    RECOVERY_RESOLUTION_RETRY,
    RECOVERY_RESOLUTION_CLOSE,
})


class ImportRecoveryDB(Protocol):
    def get_import_job(self, job_id: int) -> ImportJob | None: ...

    def resolve_import_job_recovery(
        self,
        job_id: int,
        *,
        resolution: str,
        reason: str,
    ) -> tuple[ImportJob, ImportJob | None] | None: ...


class ImportRecoveryResolution(msgspec.Struct, frozen=True):
    outcome: str
    job: ImportJob | None = None
    retry_job: ImportJob | None = None
    message: str = ""


def resolve_import_job_recovery(
    db: ImportRecoveryDB,
    job_id: int,
    *,
    resolution: str,
    reason: str,
) -> ImportRecoveryResolution:
    """Apply one explicit operator decision without inferring Beets state."""
    if resolution not in RECOVERY_RESOLUTIONS:
        raise ValueError(
            "resolution must be 'retry' (operator confirmed not applied) "
            "or 'close' (operator reconciled without replay)"
        )
    reason = reason.strip()
    if not reason:
        raise ValueError("recovery resolution requires a non-empty reason")

    current = db.get_import_job(int(job_id))
    if current is None:
        return ImportRecoveryResolution(
            outcome="not_found",
            message=f"Import job {job_id} not found",
        )
    if current.status != "recovery_required":
        return ImportRecoveryResolution(
            outcome="wrong_state",
            job=current,
            message=(
                f"Import job {job_id} is {current.status!r}, not "
                "'recovery_required'"
            ),
        )

    resolved = db.resolve_import_job_recovery(
        int(job_id),
        resolution=resolution,
        reason=reason,
    )
    if resolved is None:
        latest = db.get_import_job(int(job_id))
        return ImportRecoveryResolution(
            outcome="authority_changed",
            job=latest,
            message=(
                "Recovery authority changed; inspect the request, release, "
                "and source before trying again"
            ),
        )
    job, retry_job = resolved
    if job.job_type == "force_import" and job.preview_result is not None:
        action_path = job.preview_result.get("action_path")
        if isinstance(action_path, str) and action_path:
            try:
                from lib.config import read_runtime_config
                from lib.import_preview import cleanup_force_action_copy_for_job

                cleanup_force_action_copy_for_job(
                    action_path,
                    read_runtime_config(),
                    import_job_id=job.id,
                )
            except Exception:
                # The operator resolution is durable. A private deterministic
                # copy can be reclaimed later; it must not undo that decision.
                logger.exception(
                    "Failed to remove resolved force action copy for job %s", job.id,
                )
    return ImportRecoveryResolution(
        outcome=("retry_queued" if retry_job is not None else "closed"),
        job=job,
        retry_job=retry_job,
        message=(
            f"Queued fresh import job {retry_job.id}"
            if retry_job is not None
            else "Recovery closed without automatic replay"
        ),
    )
