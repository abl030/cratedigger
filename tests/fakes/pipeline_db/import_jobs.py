"""FakePipelineDB import_jobs cluster — mirrors ``lib/pipeline_db/import_jobs.py``.

The two import-job lanes: enqueue, claim, lease, recover.
"""
from __future__ import annotations

import copy
import json
import os
from collections.abc import (
    Callable,
    Iterable,
    Mapping,
)
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import (
    Any,
)

import msgspec

from lib import transitions
from lib.automation_recovery_debris import (
    RecoveryDebrisRemovalFn,
    RecoveryDebrisReport,
    remove_recovery_debris,
)
from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
)
from lib.import_job_lane import IMPORT_LANE, PREVIEW_LANE, JobLane
from lib.import_queue import (
    IMPORT_JOB_ACTIVE_STATUSES,
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES,
    IMPORT_JOB_LOCAL,
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_WAITING,
    IMPORT_JOB_RECOVERY_REQUIRED,
    IMPORT_JOB_YOUTUBE,
    AutomationHandoffResult,
    ForceImportPayload,
    ImportJob,
    YoutubeImportPayload,
    automation_import_dedupe_key,
    automation_import_payload,
    import_preview_requeue_delay,
    validate_job_type,
    validate_payload,
    validate_preview_failure_status,
    validate_status,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    CleanupJournalConflict,
)
from lib.pipeline_db.import_jobs import (
    AutomationRecoveryCAS,
    AutomationRecoveryEvidenceChanged,
    _default_force_action_copy_path,
    _recovery_owner_matches,
)
from lib.pipeline_db.terminal_outcomes import (
    ImportJobTerminalConflict,
)
from lib.quality import (
    ActiveDownloadState,
)
from lib.terminal_outcomes import (
    AutomationTerminalAuthority,
    ImportJobTerminal,
)
from tests.fakes._shared import _as_datetime, _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase
from tests.fakes.pipeline_db._shared import _reject_nonstandard_json_constant

#: ``import_jobs.preview_message``'s column DEFAULT, set by
#: ``migrations/005_import_preview_opt_in_default.sql`` and restated by
#: ``migrations/018_neutral_import_job_preview_ready.sql``. No Python
#: production code spells it — the database supplies it to any INSERT whose
#: column list omits ``preview_message``, which is exactly what
#: ``handoff_automation_import`` does. ``tests/test_pipeline_db.py`` pins the
#: real DEFAULT against real PostgreSQL; this is the fake's copy of it.
PREVIEW_GATE_DISABLED_DEFAULT = "Preview gate disabled"


def _noop_owner_checkpoint() -> None:
    """Fake owner proof: the fake has no session, lease, or cancellation.

    Production passes ``require_automation_recovery_owner`` here. The fake's
    ownership checks already run inside ``recover_automation_import_job``, and
    it has no concurrent writer to lose a race to.
    """


def _jsonb_scalar_text(value: object) -> str | None:
    """Mirror Postgres jsonb ``->>``/``#>>`` text-extraction semantics.

    Real SQL's ``->>``/``#>>`` return the JSON value's TEXT form: a JSON
    boolean becomes the literal ``"true"``/``"false"``, a JSON string
    passes through unchanged, and a missing key or JSON ``null`` becomes
    SQL NULL. Comparing a Python ``bool`` via ``is not True`` (the pre-fix
    shape) diverges from this the moment a value is stored as the JSON
    STRING ``"true"`` rather than the JSON boolean ``true`` — the real
    query's ``IS DISTINCT FROM 'true'`` treats both identically, since both
    stringify to the same text (issue #1122 review MINOR-6).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    return str(value)


class _FakeImportJobsMixin(_FakePipelineDBBase):
    """The two import-job lanes: enqueue, claim, lease, recover."""

    def _is_attached_processing_owner(self, job_id: int) -> bool:
        return any(
            request.get("status") == "processing"
            and request.get("active_automation_import_job_id") == job_id
            for request in self._requests.values()
        )


    def _recovery_lease_matches(
        self,
        row,
        lease: ExecutionLeaseSnapshot | None,
    ) -> bool:
        """Compare a recovery's expected lease, INCLUDING the leaseless case.

        ``_execution_lease_matches`` refuses ``None`` because every other
        caller means "no expectation supplied". Recovery is the one caller for
        which ``None`` is a positive expectation: an owner no execution ever
        claimed, which production compares with ``IS NOT DISTINCT FROM NULL``.
        """
        if lease is not None:
            return self._execution_lease_matches(
                row,
                lease,
                include_child=True,
            )
        return all(
            row.get(column) is None
            for column in (
                "execution_invocation_id",
                "execution_host_boot_id",
                "execution_systemd_unit",
                "execution_worker_pid",
                "execution_worker_start_ticks",
                "execution_beets_pid",
                "execution_beets_start_ticks",
            )
        )

    @staticmethod
    def _persist_execution_lease(
        row: dict[str, Any],
        lease: ExecutionLeaseSnapshot,
    ) -> None:
        row["execution_invocation_id"] = lease.invocation_id
        row["execution_host_boot_id"] = lease.host_boot_id
        row["execution_systemd_unit"] = lease.systemd_unit
        row["execution_worker_pid"] = lease.worker.pid
        row["execution_worker_start_ticks"] = lease.worker.start_ticks
        row["execution_beets_pid"] = (
            lease.beets.pid if lease.beets is not None else None
        )
        row["execution_beets_start_ticks"] = (
            lease.beets.start_ticks if lease.beets is not None else None
        )

    @staticmethod
    def _clear_execution_lease(row) -> None:
        row["execution_invocation_id"] = None
        row["execution_host_boot_id"] = None
        row["execution_systemd_unit"] = None
        row["execution_worker_pid"] = None
        row["execution_worker_start_ticks"] = None
        row["execution_beets_pid"] = None
        row["execution_beets_start_ticks"] = None

    def _append_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None,
        dedupe_key: str | None,
        payload,
        message: str | None,
        expected_request_status: str | None = None,
        leaves_preview_defaults_to_database: bool = False,
    ) -> ImportJob:
        """Mint one row after the caller has enforced its creation policy.

        ``leaves_preview_defaults_to_database`` mirrors the one place
        production's INSERTs into ``import_jobs`` disagree.
        ``enqueue_import_job`` and
        ``enqueue_youtube_import_and_mark_success`` both name
        ``preview_message``, ``preview_completed_at`` and
        ``importable_at`` in their column lists and write each as an
        explicit ``NULL``. ``handoff_automation_import``'s column list
        omits all three, so migrations 005/018's ``DEFAULT``s fire and a
        fresh automation job is born carrying ``'Preview gate disabled'``
        and two ``NOW()`` stamps. Collapsing that into one always-NULL
        shape (as this fake did until #1347) erases a real difference:
        see ``TestFakeAutomationHandoffRowShape`` for the live evidence
        and for why it decides queue order.
        """
        self._next_import_job_id += 1
        now = _utcnow()
        row: dict[str, Any] = {
            "id": self._next_import_job_id,
            "job_type": job_type,
            "status": "queued",
            "request_id": request_id,
            "dedupe_key": dedupe_key,
            "payload": copy.deepcopy(payload),
            "result": None,
            "message": message,
            "error": None,
            "attempts": 0,
            "worker_id": None,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "heartbeat_at": None,
            "completed_at": None,
            "preview_status": IMPORT_JOB_PREVIEW_WAITING,
            "preview_result": None,
            "preview_message": (
                PREVIEW_GATE_DISABLED_DEFAULT
                if leaves_preview_defaults_to_database else None
            ),
            "preview_error": None,
            "preview_attempts": 0,
            "preview_worker_id": None,
            "preview_started_at": None,
            "preview_heartbeat_at": None,
            "preview_completed_at": (
                now if leaves_preview_defaults_to_database else None
            ),
            "importable_at": (
                now if leaves_preview_defaults_to_database else None
            ),
            "candidate_evidence_id": None,
            "expected_request_status": (
                expected_request_status
                if expected_request_status is not None
                else (
                    self._requests.get(request_id, {}).get("status")
                    if request_id is not None
                    else None
                )
            ),
            "beets_launch_authorized_at": None,
            "beets_launch_release_id": None,
            "beets_launch_source_path": None,
            "beets_launch_request_status": None,
            "beets_launch_snapshot_fingerprint": None,
            "execution_invocation_id": None,
            "execution_host_boot_id": None,
            "execution_systemd_unit": None,
            "execution_worker_pid": None,
            "execution_worker_start_ticks": None,
            "execution_beets_pid": None,
            "execution_beets_start_ticks": None,
        }
        self._import_jobs.append(row)
        return ImportJob.from_row(copy.deepcopy(row))

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob:
        validate_job_type(job_type)
        if job_type == IMPORT_JOB_AUTOMATION:
            raise ValueError(
                "automation_import jobs may only be created by "
                "handoff_automation_import"
            )
        payload = validate_payload(job_type, payload or {})
        if dedupe_key is not None:
            existing = self._get_import_job_by_dedupe_key(dedupe_key)
            if existing is not None:
                return ImportJob.from_row(existing.to_dict(), deduped=True)
        if job_type == IMPORT_JOB_YOUTUBE and request_id is not None:
            for row in self._import_jobs:
                if (
                    row.get("job_type") == IMPORT_JOB_YOUTUBE
                    and row.get("request_id") == request_id
                    and row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
                ):
                    raise ValueError(
                        "active youtube_import already exists for "
                        f"request_id={request_id}"
                    )

        return self._append_import_job(
            job_type,
            request_id=request_id,
            dedupe_key=dedupe_key,
            payload=payload,
            message=message,
        )

    def _automation_handoff_write_boundary(
        self,
        index: int,
        label: str,
    ) -> None:
        """Post-write fault-injection seam; tests override or patch it."""

    def _automation_handoff_enforce_witness(self) -> bool:
        """Mirror the production witness-guard qualification seam."""
        return True

    def handoff_automation_import(
        self,
        *,
        request_id: int,
        expected_enqueued_at: str,
        canonical_path: str,
        message: str,
    ) -> AutomationHandoffResult:
        """In-memory transcript of the exact PostgreSQL handoff command."""
        if not expected_enqueued_at:
            raise ValueError("expected_enqueued_at must be non-empty")
        if not canonical_path:
            raise ValueError("canonical_path must be non-empty")

        with self.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            request_id,
        ) as acquired:
            if not acquired:
                return AutomationHandoffResult("lock_unavailable")

            request = self._requests.get(request_id)
            if request is None:
                return AutomationHandoffResult("request_missing")
            if request.get("status") != "downloading":
                return AutomationHandoffResult("not_downloading")
            raw_state = request.get("active_download_state")
            if raw_state is None:
                return AutomationHandoffResult("missing_state")
            if isinstance(raw_state, str):
                try:
                    state = json.loads(
                        raw_state,
                        parse_constant=_reject_nonstandard_json_constant,
                    )
                except ValueError:
                    return AutomationHandoffResult("missing_state")
            else:
                state = copy.deepcopy(raw_state)
            if not isinstance(state, dict):
                return AutomationHandoffResult("missing_state")
            if (
                self._automation_handoff_enforce_witness()
                and state.get("enqueued_at") != expected_enqueued_at
            ):
                return AutomationHandoffResult("witness_mismatch")
            if request.get("active_automation_import_job_id") is not None:
                return AutomationHandoffResult("owner_conflict")
            if any(
                row.get("job_type") == IMPORT_JOB_AUTOMATION
                and row.get("request_id") == request_id
                and row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
                for row in self._import_jobs
            ):
                return AutomationHandoffResult("owner_conflict")

            request_before = copy.deepcopy(request)
            job_count_before = len(self._import_jobs)
            status_count_before = len(self.status_history)
            try:
                job = self._append_import_job(
                    IMPORT_JOB_AUTOMATION,
                    request_id=request_id,
                    dedupe_key=automation_import_dedupe_key(request_id),
                    payload=automation_import_payload(),
                    message=message,
                    expected_request_status="processing",
                    leaves_preview_defaults_to_database=True,
                )
                self._automation_handoff_write_boundary(
                    1,
                    "import_job.inserted",
                )
                now = _utcnow()
                state["current_path"] = canonical_path
                state["processing_started_at"] = now.isoformat()
                request["status"] = "processing"
                request["active_automation_import_job_id"] = job.id
                request["active_download_state"] = state
                request["updated_at"] = now
                self.status_history.append((request_id, "processing"))
                self._automation_handoff_write_boundary(
                    2,
                    "request.processing_owner",
                )
            except Exception:
                request.clear()
                request.update(request_before)
                del self._import_jobs[job_count_before:]
                del self.status_history[status_count_before:]
                # PostgreSQL sequences are non-transactional. Deliberately do
                # not rewind ``_next_import_job_id`` on rollback.
                raise
            return AutomationHandoffResult("committed", job)

    def get_import_job(self, job_id: int) -> ImportJob | None:
        for row in self._import_jobs:
            if row["id"] == job_id:
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def _get_import_job_by_dedupe_key(
        self,
        dedupe_key: str,
        *,
        active_only: bool = True,
    ) -> ImportJob | None:
        rows = [
            row for row in self._import_jobs
            if row.get("dedupe_key") == dedupe_key
            and (
                not active_only
                or row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
            )
        ]
        rows.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]), reverse=True)
        return ImportJob.from_row(copy.deepcopy(rows[0])) if rows else None

    def list_import_jobs(
        self,
        *,
        status: str | None = None,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        if status is not None:
            validate_status(status)
        rows = list(self._import_jobs)
        if status is not None:
            rows = [row for row in rows if row.get("status") == status]
        if request_id is not None:
            rows = [row for row in rows if row.get("request_id") == request_id]
        rows.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]), reverse=True)
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows[:limit]]

    def list_active_import_jobs(
        self,
        *,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        rows = [
            row for row in self._import_jobs
            if row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
            and (request_id is None or row.get("request_id") == request_id)
        ]
        rows.sort(key=lambda row: (_as_datetime(row.get("created_at")), row["id"]))
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows[:limit]]

    def list_active_import_jobs_for_wrong_match(
        self,
        *,
        download_log_id: int,
        request_id: int | None,
        failed_paths: Iterable[str],
        source_dirs: Iterable[str],
        ignore_import_job_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        paths = {str(path) for path in failed_paths if path}
        dirs = {str(path) for path in source_dirs if path}
        jobs: list[ImportJob] = []
        for row in self._import_jobs:
            if row.get("status") not in IMPORT_JOB_ACTIVE_STATUSES:
                continue
            if (
                ignore_import_job_id is not None
                and int(row["id"]) == int(ignore_import_job_id)
            ):
                continue
            job = ImportJob.from_row(copy.deepcopy(row))
            if isinstance(job.payload, ForceImportPayload):
                matches = (
                    job.payload.download_log_id == download_log_id
                    or job.payload.failed_path in paths
                    or bool(dirs.intersection(job.payload.source_dirs))
                )
            else:
                matches = False
            if matches:
                jobs.append(job)
        jobs.sort(
            key=lambda job: (
                job.created_at is None,
                job.created_at or datetime.min.replace(tzinfo=UTC),
                job.id,
            ),
        )
        return jobs[:limit]

    def count_import_jobs_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self._import_jobs:
            status = str(row.get("status"))
            counts[status] = counts.get(status, 0) + 1
        return counts

    def list_import_job_timeline(self, *, limit: int = 50) -> list[ImportJob]:
        active_rows = [
            row for row in self._import_jobs
            if row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
        ]

        def sort_key(row: dict[str, Any]) -> tuple[int, datetime, datetime, int]:
            status = row.get("status")
            preview_status = row.get("preview_status")
            if status == IMPORT_JOB_RECOVERY_REQUIRED:
                bucket = 0
            elif (
                status == "queued"
                and preview_status in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
            ):
                bucket = 1
            elif status == "running":
                bucket = 2
            elif status == "queued" and preview_status == "running":
                bucket = 3
            elif status == "queued" and preview_status == "waiting":
                bucket = 4
            else:
                bucket = 5
            return (
                bucket,
                _as_datetime(row.get("importable_at")),
                _as_datetime(row.get("created_at")),
                int(row["id"]),
            )

        rows = sorted(active_rows, key=sort_key)
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows[:limit]]

    def list_automation_import_jobs_for_startup_recovery(
        self,
    ) -> list[ImportJob]:
        rows = [
            row
            for row in self._import_jobs
            if self._automation_job_has_authority(row)
            and (
                (
                    row.get("status") == "queued"
                    and row.get("preview_status") == "running"
                )
                or (
                    row.get("status") == "running"
                    and row.get("preview_status")
                    in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
                )
                # A still-attached ``recovery_required`` owner is a close that
                # stopped mid-frame, never a resting state; recovery must be
                # able to finish it.
                or row.get("status") == IMPORT_JOB_RECOVERY_REQUIRED
            )
        ]
        rows.sort(key=lambda row: (
            _as_datetime(row.get("created_at")),
            int(row["id"]),
        ))
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows]

    def list_terminal_force_action_cleanup_jobs(self) -> list[ImportJob]:
        rows = []
        for row in self._import_jobs:
            if (
                # issue #1176 PR3 widened this to local_import too.
                row.get("job_type") not in (IMPORT_JOB_FORCE, IMPORT_JOB_LOCAL)
                or row.get("status") not in ("completed", "failed")
            ):
                continue
            preview = row.get("preview_result")
            action_path = (
                preview.get("action_path")
                if isinstance(preview, dict) else None
            )
            if not isinstance(action_path, str) or not action_path:
                continue
            result = row.get("result")
            cleanup = (
                result.get("force_action_cleanup")
                if isinstance(result, dict) else None
            )
            removed = (
                cleanup.get("removed")
                if isinstance(cleanup, dict) else None
            )
            if removed is not True:
                rows.append(row)
        rows.sort(key=lambda row: (
            _as_datetime(row.get("created_at")),
            int(row["id"]),
        ))
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows]

    def list_terminal_force_wrong_match_cleanup_jobs(self) -> list[ImportJob]:
        from lib.dispatch import (
            DISPATCH_CODE_REQUEUE_EXHAUSTED,
            DISPATCH_CODE_REQUEUE_FAILED,
        )

        rows = []
        for row in self._import_jobs:
            if row.get("job_type") != IMPORT_JOB_FORCE:
                continue
            result = row.get("result")
            result_dict = result if isinstance(result, dict) else {}
            # Era-AND-lane marker (issue #1122 review MAJOR-2/3): only a row
            # whose terminal commit went through ``_job_result`` carries this
            # key. Absent for every historical/non-adjudicating shape
            # (pre-#1122 rows, preview-stage terminalization, the
            # executor-crash literal, ad hoc operator terminalization) and
            # for a genuinely NULL ``result`` column — all stay receiptless
            # forever by design, mirroring the real SQL's ``result ?
            # 'post_commit_wrong_match_scenario'`` top-level AND clause.
            if "post_commit_wrong_match_scenario" not in result_dict:
                continue
            status = row.get("status")
            if status == "completed":
                dismissal = result_dict.get("wrong_match_dismissal")
                success = (
                    dismissal.get("success")
                    if isinstance(dismissal, dict) else None
                )
                # Success-KEYED, not presence-keyed (MAJOR-1): a receipt
                # can be present with success=false (entry not found, an
                # unsafe path, an rmtree failure, EACCES-shaped
                # path_unavailable — the #1063 shape) and must still be
                # retried, never parked.
                if _jsonb_scalar_text(success) != "true":
                    rows.append(row)
            elif status == "failed":
                cleanup = result_dict.get("cleanup")
                success = (
                    cleanup.get("success")
                    if isinstance(cleanup, dict) else None
                )
                if (
                    _jsonb_scalar_text(success) != "true"
                    and _jsonb_scalar_text(result_dict.get("code"))
                        != DISPATCH_CODE_REQUEUE_FAILED
                    and _jsonb_scalar_text(result_dict.get("code"))
                        != DISPATCH_CODE_REQUEUE_EXHAUSTED
                    and _jsonb_scalar_text(result_dict.get("deferred"))
                        != "true"
                ):
                    rows.append(row)
        rows.sort(key=lambda row: (
            _as_datetime(row.get("created_at")),
            int(row["id"]),
        ))
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows]

    def _candidate_job_type_routes(
        self,
        row: Mapping[str, object],
        *,
        execution_lease: ExecutionLeaseSnapshot | None,
    ) -> bool:
        """Positive routing, the fake's copy of ``_CANDIDATE_JOB_TYPE_ROUTING``.

        ``youtube_import`` is the sole unguarded type; ``automation_import``
        and ``force_import``/``local_import`` each carry their own guard.

        One method for the same reason production is one SQL fragment
        (issue #1176 PR3 had to land its fix in two byte-identical copies,
        and #1314 collapsed the production pair while leaving the fake's):
        both import-lane and preview-lane candidate scans route by the
        identical rule, and the two lanes' real differences — which preview
        statuses are eligible, the preview lane's requeue backoff, and the
        sort key — stay at their own call sites where they belong.

        A live Beets child refuses EVERY type, which production spells as an
        early ``return []`` in each peek rather than inside the fragment. Per
        row here instead of once per scan: same answer, since a true guard
        refuses every row, and it keeps the rule in the one place both lanes
        already read. The predicate itself is shared with the four other
        callers that spell the same rule (#1347).
        """
        if self._live_beets_child_refuses(execution_lease):
            return False
        job_type = row.get("job_type")
        if job_type == IMPORT_JOB_YOUTUBE:
            return True
        if job_type == IMPORT_JOB_AUTOMATION:
            return (
                execution_lease is not None
                and self._automation_job_has_authority(row)
            )
        if job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_LOCAL):
            # `isinstance` rather than the `int(...)` the two copies used:
            # production LEFT JOINs the request, so a row with no usable
            # request_id compares its status against NULL and is refused
            # there too, and narrowing here keeps this method free of a
            # typing escape hatch.
            request_id = row.get("request_id")
            return isinstance(request_id, int) and (
                self._force_job_request_is_current(row, request_id=request_id)
            )
        return False

    def _import_job_candidate_rows(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> list[dict[str, object]]:
        queued = [
            row for row in self._import_jobs
            if row.get("status") == "queued"
            and row.get("preview_status") in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
            and self._candidate_job_type_routes(
                row, execution_lease=execution_lease,
            )
        ]
        queued.sort(key=lambda row: (
            _as_datetime(row.get("importable_at")),
            _as_datetime(row.get("created_at")),
            row["id"],
        ))
        return queued

    def peek_import_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]:
        if limit <= 0:
            raise ValueError("import candidate limit must be positive")
        if offset < 0:
            raise ValueError("import candidate offset cannot be negative")
        return [
            ImportJob.from_row(copy.deepcopy(row))
            for row in self._import_job_candidate_rows(
                execution_lease=execution_lease,
            )[offset:offset + limit]
        ]

    def _lane_claimable_row(
        self,
        job_id: int,
        *,
        lane: JobLane,
        job_type: str,
        request_id: int | None = None,
    ):
        """The exact row a claim in ``lane`` may take, or ``None``.

        Mirrors the production claim's own row guard: exact id, exact type,
        ``status='queued'``, and the lane's entry ``preview_status``.
        """
        return next(
            (
                candidate
                for candidate in self._import_jobs
                if candidate.get("id") == job_id
                and candidate.get("job_type") == job_type
                and candidate.get("status") == "queued"
                and candidate.get("preview_status")
                == lane.entry_preview_status
                and (
                    request_id is None
                    or candidate.get("request_id") == request_id
                )
            ),
            None,
        )

    def _claim_unguarded_candidate_in_lane(
        self,
        job_id: int,
        *,
        lane: JobLane,
        worker_id: str | None,
    ) -> ImportJob | None:
        # youtube_import only (issue #1176 PR3) — local_import claims
        # through the request-scoped path instead.
        row = self._lane_claimable_row(
            job_id, lane=lane, job_type=IMPORT_JOB_YOUTUBE,
        )
        if row is None:
            return None
        return self._claim_job_row_in_lane(
            row,
            lane=lane,
            worker_id=worker_id,
            execution_lease=None,
        )

    def _claim_automation_job_in_lane(
        self,
        job_id: int,
        *,
        lane: JobLane,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None:
        row = self._lane_claimable_row(
            job_id,
            lane=lane,
            job_type=IMPORT_JOB_AUTOMATION,
            request_id=request_id,
        )
        if row is None or not self._automation_job_has_authority(row):
            return None
        return self._claim_job_row_in_lane(
            row,
            lane=lane,
            worker_id=worker_id,
            execution_lease=execution_lease,
        )

    def _claim_request_scoped_job_in_lane(
        self,
        job_id: int,
        *,
        lane: JobLane,
        job_type: str,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None:
        """Force and local share one request-currency guard (issue #1176 PR3)."""
        row = self._lane_claimable_row(
            job_id, lane=lane, job_type=job_type, request_id=request_id,
        )
        if row is None or not self._force_job_request_is_current(
            row,
            request_id=request_id,
        ):
            return None
        return self._claim_job_row_in_lane(
            row,
            lane=lane,
            worker_id=worker_id,
            execution_lease=None,
        )

    def claim_import_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        return self._claim_unguarded_candidate_in_lane(
            job_id, lane=IMPORT_LANE, worker_id=worker_id,
        )

    def claim_automation_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None:
        return self._claim_automation_job_in_lane(
            job_id,
            lane=IMPORT_LANE,
            request_id=request_id,
            worker_id=worker_id,
            execution_lease=execution_lease,
        )

    def _force_job_request_is_current(
        self,
        row: Mapping[str, object],
        *,
        request_id: int,
    ) -> bool:
        request = self._requests.get(request_id)
        return bool(
            request is not None
            and row.get("request_id") == request_id
            and row.get("expected_request_status") is not None
            and request.get("status") == row.get("expected_request_status")
            and request.get("status") not in ("processing", "replaced")
            and request.get("active_automation_import_job_id") is None
        )

    def claim_force_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None:
        return self._claim_request_scoped_job_in_lane(
            job_id,
            lane=IMPORT_LANE,
            job_type=IMPORT_JOB_FORCE,
            request_id=request_id,
            worker_id=worker_id,
        )

    def claim_local_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None:
        return self._claim_request_scoped_job_in_lane(
            job_id,
            lane=IMPORT_LANE,
            job_type=IMPORT_JOB_LOCAL,
            request_id=request_id,
            worker_id=worker_id,
        )

    def _claim_job_row_in_lane(
        self,
        row: dict[str, Any],
        *,
        lane: JobLane,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot | None,
    ) -> ImportJob:
        """Stamp one claim, reading every column name from the lane value."""
        now = _utcnow()
        row[lane.status_column] = "running"
        row[lane.attempts_column] = int(row.get(lane.attempts_column) or 0) + 1
        row[lane.worker_id_column] = worker_id
        row[lane.started_at_column] = row.get(lane.started_at_column) or now
        row[lane.heartbeat_at_column] = now
        for column in lane.cleared_columns:
            row[column] = None
        if execution_lease is not None:
            self._persist_execution_lease(row, execution_lease)
        row["updated_at"] = now
        return ImportJob.from_row(copy.deepcopy(row))

    def heartbeat_import_job(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool:
        for row in self._import_jobs:
            if (
                row["id"] != job_id
                or row.get("status") != "running"
                or row.get("preview_status")
                not in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
            ):
                continue
            if row.get("job_type") == IMPORT_JOB_AUTOMATION and (
                not self._automation_job_has_authority(row)
                or not self._execution_lease_matches(
                    row,
                    expected_execution_lease,
                    include_child=True,
                )
            ):
                return False
            now = _utcnow()
            row["heartbeat_at"] = now
            row["updated_at"] = now
            return True
        return False

    def mark_import_job_completed(
        self,
        job_id: int,
        *,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("job_type") != IMPORT_JOB_AUTOMATION
                and row.get("status") in ("queued", "running")
            ):
                now = _utcnow()
                row["status"] = "completed"
                row["result"] = copy.deepcopy(result or {})
                row["message"] = message
                row["error"] = None
                row["completed_at"] = now
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def authorize_import_job_launch(
        self,
        job_id: int,
        *,
        request_id: int,
        release_id: str,
        source_path: str,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None:
        if self._live_beets_child_refuses(expected_execution_lease):
            return None
        request = self._requests.get(request_id)
        for row in self._import_jobs:
            if (
                row["id"] != job_id
                or row.get("status") != "running"
                or row.get("beets_launch_authorized_at") is not None
                or row.get("request_id") != request_id
                or request is None
                or row.get("expected_request_status") is None
                or request.get("status") != row.get("expected_request_status")
                or request.get("status") == "replaced"
                or request.get("mb_release_id") != release_id
            ):
                continue
            evidence_id = row.get("candidate_evidence_id")
            evidence = (
                self._evidence_by_id.get(int(evidence_id))
                if evidence_id is not None else None
            )
            if (
                evidence is None
                or evidence.mb_release_id != release_id
                or not evidence.snapshot_fingerprint
            ):
                return None
            job = ImportJob.from_row(copy.deepcopy(row))
            if job.job_type == IMPORT_JOB_AUTOMATION:
                state = request.get("active_download_state")
                if (
                    request.get("status") != "processing"
                    or request.get("active_automation_import_job_id") != job.id
                    or row.get("preview_status")
                    not in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
                    or not self._execution_lease_matches(
                        row,
                        expected_execution_lease,
                        include_child=True,
                    )
                    or expected_execution_lease is None
                    or not isinstance(state, dict)
                    or state.get("current_path") != source_path
                ):
                    return None
            elif job.job_type == IMPORT_JOB_FORCE:
                if (
                    not isinstance(job.payload, ForceImportPayload)
                    or job.payload.failed_path != source_path
                    or request.get("status") == "processing"
                    or request.get("active_automation_import_job_id") is not None
                ):
                    return None
            elif job.job_type == IMPORT_JOB_YOUTUBE:
                if (
                    request.get("status") not in ("wanted", "unsearchable")
                    or not isinstance(job.payload, YoutubeImportPayload)
                    or job.payload.staged_path != source_path
                ):
                    return None
            elif job.job_type == IMPORT_JOB_LOCAL:
                # No payload-field equality check (issue #1176 PR3) — see
                # lib.pipeline_db.import_jobs.PipelineDB
                # .authorize_import_job_launch's docstring: local-import's
                # ``source_path`` IS the private action copy already
                # (source_reference_path=None), deterministic from job.id
                # alone, so there is no separate durable payload field it
                # could usefully equal.
                if (
                    request.get("status") == "processing"
                    or request.get("active_automation_import_job_id") is not None
                ):
                    return None
            else:
                return None
            now = _utcnow()
            row["beets_launch_authorized_at"] = now
            row["beets_launch_release_id"] = release_id
            row["beets_launch_source_path"] = source_path
            row["beets_launch_request_status"] = request.get("status")
            row["beets_launch_snapshot_fingerprint"] = (
                evidence.snapshot_fingerprint
            )
            row["updated_at"] = now
            return ImportJob.from_row(copy.deepcopy(row))
        return None

    def record_import_job_beets_child(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot,
        beets_pid: int,
        beets_start_ticks: int,
    ) -> ImportJob | None:
        if beets_pid <= 0:
            raise ValueError("beets_pid must be positive")
        if beets_start_ticks < 0:
            raise ValueError("beets_start_ticks must be non-negative")
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("job_type") == IMPORT_JOB_AUTOMATION
                and row.get("status") == "running"
                and row.get("preview_status")
                in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
                and row.get("beets_launch_authorized_at") is not None
                and self._automation_job_has_authority(row)
                and self._execution_lease_matches(
                    row,
                    expected_execution_lease,
                    include_child=True,
                )
                and expected_execution_lease.beets is None
            ):
                row["execution_beets_pid"] = beets_pid
                row["execution_beets_start_ticks"] = beets_start_ticks
                row["updated_at"] = _utcnow()
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def _automation_recovery_rows(
        self,
        expected: AutomationRecoveryCAS,
    ):
        request = self._requests.get(expected.request_id)
        job = next(
            (
                row
                for row in self._import_jobs
                if row["id"] == expected.job_id
                and row.get("request_id") == expected.request_id
            ),
            None,
        )
        journal = self._processing_cleanup_journals.get(
            (expected.job_id, expected.request_id)
        )
        return request, job, journal

    def require_automation_recovery_owner(
        self,
        expected: AutomationRecoveryCAS,
    ) -> None:
        request, job, _journal = self._automation_recovery_rows(expected)
        if not _recovery_owner_matches(
            expected,
            request=request,
            job=job,
        ):
            raise AutomationRecoveryEvidenceChanged(
                "automation recovery owner evidence changed"
            )

    def mark_import_job_failed(
        self,
        job_id: int,
        *,
        error: str,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("job_type") != IMPORT_JOB_AUTOMATION
                and row.get("status") in ("queued", "running")
            ):
                now = _utcnow()
                row["status"] = "failed"
                row["result"] = copy.deepcopy(result or {})
                row["message"] = message
                row["error"] = error
                row["completed_at"] = now
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def merge_import_job_result(
        self,
        job_id: int,
        patch: dict[str, Any],
    ) -> ImportJob | None:
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("status") in ("completed", "failed")
                and not self._is_attached_processing_owner(job_id)
            ):
                result = row.get("result")
                merged = copy.deepcopy(result) if isinstance(result, dict) else {}
                merged.update(copy.deepcopy(patch))
                row["result"] = merged
                row["updated_at"] = _utcnow()
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def recover_running_import_jobs(
        self,
        *,
        requeue_message: str,
        recovery_message: str,
        limit: int = 50,
        debris_removal_fn: RecoveryDebrisRemovalFn = remove_recovery_debris,
        force_action_copy_path_fn: Callable[
            [int], str,
        ] = _default_force_action_copy_path,
    ) -> list[ImportJob]:
        """Fake mirror of PipelineDB.recover_running_import_jobs.

        ``debris_removal_fn`` (issue #1089) mirrors the real method exactly:
        production default unless a test injects a stub. Issue #1089 review
        MAJOR-1/MAJOR-2: a force job's confinement root is derived via
        ``force_action_copy_path_fn``, never the stored
        ``beets_launch_source_path`` (which records the operator's ORIGINAL
        failed path, not the force-action copy Beets actually imports
        from); an unclassified exception from ``debris_removal_fn`` is
        caught and surfaced in the job's audit record rather than
        propagating and crash-looping the whole importer.
        """
        from lib.terminal_outcomes import non_automation_failure_terminal_outcome

        running = [
            row for row in self._import_jobs
            if row.get("status") == "running"
            and row.get("job_type") != IMPORT_JOB_AUTOMATION
        ]
        running.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]))
        updated_jobs = []
        for row in running[:limit]:
            now = _utcnow()
            launched = row.get("beets_launch_authorized_at") is not None
            if launched:
                no_replay_reason = (
                    "Automatic replay refused because Beets may have mutated "
                    "the library"
                )
                job = ImportJob.from_row(copy.deepcopy(row))
                try:
                    # Issue #1089 review round 3 item 4: the confinement-root
                    # derivation moved inside this guard too, mirroring the
                    # real method exactly.
                    confinement_path = (
                        force_action_copy_path_fn(job.id)
                        if job.job_type == IMPORT_JOB_FORCE
                        else job.beets_launch_source_path
                    )
                    debris_report = debris_removal_fn(
                        launch_release_id=job.beets_launch_release_id,
                        launch_source_path=confinement_path,
                    )
                except Exception as exc:  # noqa: BLE001 - #1089 review MAJOR-2
                    debris_report = RecoveryDebrisReport(
                        outcome="check_raised",
                        detail=f"{type(exc).__name__}: {exc}",
                    )
                if debris_report.outcome != "no_launch":
                    no_replay_reason = (
                        f"{no_replay_reason}; recovery debris "
                        f"{debris_report.outcome}"
                    )
                    if debris_report.album_id is not None:
                        no_replay_reason = (
                            f"{no_replay_reason} "
                            f"(beets album {debris_report.album_id})"
                        )
                    if debris_report.detail:
                        no_replay_reason = (
                            f"{no_replay_reason}: {debris_report.detail}"
                        )
                recovery_debris_removal = msgspec.to_builtins(debris_report)
                terminal = self.persist_import_terminal_outcome(
                    non_automation_failure_terminal_outcome(
                        job,
                        error=no_replay_reason,
                        message=f"{recovery_message}: {no_replay_reason}",
                        result={
                            "success": False,
                            "recovery": "launch_authorized_no_replay",
                            "recovery_debris_removal": recovery_debris_removal,
                        },
                    )
                )
                updated_jobs.append(terminal.job)
                continue
            row["status"] = "queued"
            row["message"] = requeue_message
            row["error"] = None
            row["worker_id"] = None
            row["started_at"] = None
            row["heartbeat_at"] = None
            row["completed_at"] = None
            row["updated_at"] = now
            updated_jobs.append(ImportJob.from_row(copy.deepcopy(row)))
        return updated_jobs

    def recover_automation_import_job(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None,
        decision: ExecutionLivenessDecision,
        requeue_message: str,
        recovery_message: str,
        debris_removal_fn: RecoveryDebrisRemovalFn = remove_recovery_debris,
    ) -> ImportJob | None:
        """Fake mirror of PipelineDB.recover_automation_import_job.

        Mirrors by DELEGATION wherever the production path is not SQL: the same
        cleanup resume, the same audit bundle builder, and the same terminal
        command run here, so the two cannot drift on the part that decides
        whether a request keeps being acquired. ``debris_removal_fn`` (issue
        #1089) mirrors the same way: production default unless a test injects
        a stub.
        """
        from lib.download import _local_completion_terminal_outcome
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.pipeline_db.import_jobs import (
            AUTOMATION_WORLD_FAILURE_AUDIT_PREFIX,
            _job_terminal_stage,
            automation_completion_receipt,
        )
        from lib.processing_cleanup import (
            ProcessingCleanupError,
            complete_owner_processing_cleanup,
        )
        from lib.terminal_outcomes import (
            cleanup_journal_refusal_disposition,
        )

        if (
            decision.status != "dead"
            or decision.evidence.lease != expected_execution_lease
        ):
            return None
        row = next(
            (item for item in self._import_jobs if item["id"] == job_id),
            None,
        )
        if (
            row is None
            or not self._automation_job_has_authority(row)
            or not self._recovery_lease_matches(
                row,
                expected_execution_lease,
            )
            or row.get("completed_at") is not None
            or not (
                (
                    row.get("status") == "queued"
                    and row.get("preview_status") == "running"
                )
                or (
                    row.get("status") == "running"
                    and row.get("preview_status")
                    in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
                )
                or row.get("status") == IMPORT_JOB_RECOVERY_REQUIRED
            )
        ):
            return None
        request_id = int(row["request_id"])
        with self.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            request_id,
        ) as acquired:
            if not acquired or not self._automation_job_has_authority(row):
                return None
            launched = row.get("beets_launch_authorized_at") is not None
            journalled = (
                (job_id, request_id) in self._processing_cleanup_journals
            )
            historical_recovery = (
                row.get("status") == IMPORT_JOB_RECOVERY_REQUIRED
            )
            if not launched and not journalled and not historical_recovery:
                row["status"] = "queued"
                if row.get("preview_status") == "running":
                    row["preview_status"] = IMPORT_JOB_PREVIEW_WAITING
                row["message"] = requeue_message
                row["error"] = None
                row["worker_id"] = None
                row["started_at"] = None
                row["heartbeat_at"] = None
                row["preview_worker_id"] = None
                row["preview_started_at"] = None
                row["preview_heartbeat_at"] = None
                self._clear_execution_lease(row)
                row["updated_at"] = _utcnow()
                return ImportJob.from_row(copy.deepcopy(row))

            job = ImportJob.from_row(copy.deepcopy(row))
            detail = f"{AUTOMATION_WORLD_FAILURE_AUDIT_PREFIX}: {recovery_message}"
            try:
                request = self._requests.get(request_id)
                raw_state = (
                    None
                    if request is None
                    else request.get("active_download_state")
                )
                if request is None or raw_state is None:
                    return None
                state = ActiveDownloadState.from_raw(raw_state)
                if state.current_path is None:
                    return None
                debris_report = debris_removal_fn(
                    launch_release_id=job.beets_launch_release_id,
                    launch_source_path=job.beets_launch_source_path,
                )
                if debris_report.outcome != "no_launch":
                    detail = f"{detail}; recovery debris {debris_report.outcome}"
                    if debris_report.album_id is not None:
                        detail = f"{detail} (beets album {debris_report.album_id})"
                    if debris_report.detail:
                        detail = f"{detail}: {debris_report.detail}"
                cleanup_refusal = None
                try:
                    cleanup_receipt = complete_owner_processing_cleanup(
                        self,
                        request_id=request_id,
                        job_id=job_id,
                        source_path=os.path.abspath(state.current_path),
                        owner_checkpoint=_noop_owner_checkpoint,
                    )
                except ProcessingCleanupError as exc:
                    cleanup_receipt = None
                    cleanup_refusal = cleanup_journal_refusal_disposition(
                        self._processing_cleanup_journals.get(
                            (job_id, request_id)
                        ),
                        error_code=exc.code,
                        error_message=str(exc),
                    )
                    detail = (
                        f"{detail}; processor cleanup refused "
                        f"({exc.code}): {exc}"
                    )
                completion_receipt = automation_completion_receipt(job)
                expected_job_status = _job_terminal_stage(job.status)
                pending = _local_completion_terminal_outcome(
                    reconstruct_grab_list_entry(request, state),
                    state,
                    request_id=request_id,
                    import_job_id=job_id,
                    transition=transitions.RequestTransition.to_wanted(
                        from_status="processing",
                        attempt_type="validation",
                    ),
                    outcome="failed",
                    detail=detail,
                    error_message=detail,
                )
                terminal = self.persist_import_terminal_outcome(
                    replace(
                        pending,
                        automation=AutomationTerminalAuthority(
                            expected_job_status=expected_job_status,
                            expected_preview_status=job.preview_status,
                            expected_execution_lease=expected_execution_lease,
                            cleanup_receipt=cleanup_receipt,
                            completion_receipt=completion_receipt,
                            cleanup_refusal=cleanup_refusal,
                        ),
                    ).with_job(ImportJobTerminal(
                        status="failed",
                        error=detail,
                        result={
                            "automation_recovery_self_heal": recovery_message,
                            "recovery_debris_removal": msgspec.to_builtins(
                                debris_report,
                            ),
                        },
                        message=detail,
                    ))
                )
            except (
                AutomationRecoveryEvidenceChanged,
                CleanupJournalConflict,
                ImportJobTerminalConflict,
            ):
                return None
            return terminal.job

    def requeue_import_job_for_preview(
        self,
        job_id: int,
        *,
        reason: str,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None:
        """Fake mirror of PipelineDB.requeue_import_job_for_preview.

        Only matches rows currently in ``status='running'``. Clears writer
        state, sets preview_status='waiting', preserves attempt counters.
        """
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("status") == "running"
                and row.get("beets_launch_authorized_at") is None
                and (
                    row.get("job_type") != IMPORT_JOB_AUTOMATION
                    or (
                        row.get("preview_status")
                        in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
                        and self._automation_job_has_authority(row)
                        and self._execution_lease_matches(
                            row,
                            expected_execution_lease,
                            include_child=True,
                        )
                    )
                )
            ):
                now = _utcnow()
                row["status"] = "queued"
                row["preview_status"] = IMPORT_JOB_PREVIEW_WAITING
                row["message"] = reason
                row["error"] = None
                row["worker_id"] = None
                row["started_at"] = None
                row["heartbeat_at"] = None
                row["preview_message"] = None
                row["preview_error"] = None
                if row.get("job_type") == IMPORT_JOB_AUTOMATION:
                    self._clear_execution_lease(row)
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def _import_preview_job_candidate_rows(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> list[dict[str, object]]:
        now = _utcnow()
        queued = [
            row for row in self._import_jobs
            if row.get("status") == "queued"
            and row.get("preview_status") == "waiting"
            and _as_datetime(row.get("updated_at")) <= (
                now - import_preview_requeue_delay(int(row.get("attempts") or 0))
            )
            and self._candidate_job_type_routes(
                row, execution_lease=execution_lease,
            )
        ]
        queued.sort(key=lambda row: (_as_datetime(row.get("created_at")), row["id"]))
        return queued

    def peek_import_preview_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]:
        if limit <= 0:
            raise ValueError("preview candidate limit must be positive")
        if offset < 0:
            raise ValueError("preview candidate offset cannot be negative")
        return [
            ImportJob.from_row(copy.deepcopy(row))
            for row in self._import_preview_job_candidate_rows(
                execution_lease=execution_lease,
            )[offset:offset + limit]
        ]

    def claim_import_preview_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        return self._claim_unguarded_candidate_in_lane(
            job_id, lane=PREVIEW_LANE, worker_id=worker_id,
        )

    def claim_automation_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None:
        return self._claim_automation_job_in_lane(
            job_id,
            lane=PREVIEW_LANE,
            request_id=request_id,
            worker_id=worker_id,
            execution_lease=execution_lease,
        )

    def claim_force_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None:
        return self._claim_request_scoped_job_in_lane(
            job_id,
            lane=PREVIEW_LANE,
            job_type=IMPORT_JOB_FORCE,
            request_id=request_id,
            worker_id=worker_id,
        )

    def claim_local_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None:
        return self._claim_request_scoped_job_in_lane(
            job_id,
            lane=PREVIEW_LANE,
            job_type=IMPORT_JOB_LOCAL,
            request_id=request_id,
            worker_id=worker_id,
        )

    def heartbeat_import_job_preview(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool:
        if self._live_beets_child_refuses(expected_execution_lease):
            return False
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("status") == "queued"
                and row.get("preview_status") == "running"
            ):
                if row.get("job_type") == IMPORT_JOB_AUTOMATION and (
                    not self._automation_job_has_authority(row)
                    or not self._execution_lease_matches(
                        row,
                        expected_execution_lease,
                        include_child=True,
                    )
                    or expected_execution_lease is None
                ):
                    return False
                now = _utcnow()
                row["preview_heartbeat_at"] = now
                row["updated_at"] = now
                return True
        return False

    def mark_import_job_preview_importable(
        self,
        job_id: int,
        *,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None:
        if self._live_beets_child_refuses(expected_execution_lease):
            return None
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("status") == "queued"
                and row.get("preview_status") in ("waiting", "running")
                and (
                    row.get("job_type") != IMPORT_JOB_AUTOMATION
                    or (
                        row.get("preview_status") == "running"
                        and self._automation_job_has_authority(row)
                        and self._execution_lease_matches(
                            row,
                            expected_execution_lease,
                            include_child=True,
                        )
                        and expected_execution_lease is not None
                    )
                )
            ):
                now = _utcnow()
                row["preview_status"] = IMPORT_JOB_PREVIEW_EVIDENCE_READY
                row["preview_result"] = copy.deepcopy(preview_result or {})
                row["preview_message"] = message
                row["preview_error"] = None
                row["preview_completed_at"] = now
                row["importable_at"] = row.get("importable_at") or now
                row["preview_worker_id"] = None
                row["preview_heartbeat_at"] = None
                if row.get("job_type") == IMPORT_JOB_AUTOMATION:
                    self._clear_execution_lease(row)
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def mark_import_job_preview_failed(
        self,
        job_id: int,
        *,
        preview_status: str,
        error: str,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        validate_preview_failure_status(preview_status)
        result = copy.deepcopy(preview_result or {})
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("job_type") != IMPORT_JOB_AUTOMATION
                and row.get("status") == "queued"
                and row.get("preview_status") in ("waiting", "running")
            ):
                now = _utcnow()
                row["status"] = "failed"
                row["preview_status"] = preview_status
                row["preview_result"] = result
                row["preview_message"] = message
                row["preview_error"] = error
                row["result"] = {"preview": copy.deepcopy(result)}
                row["message"] = message
                row["error"] = error
                row["preview_completed_at"] = now
                row["completed_at"] = now
                row["preview_worker_id"] = None
                row["preview_heartbeat_at"] = None
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def requeue_stale_import_preview_jobs(
        self,
        *,
        older_than: timedelta,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = _utcnow() - older_than
        stale = []
        for row in self._import_jobs:
            if (
                row.get("status") != "queued"
                or row.get("preview_status") != "running"
                or row.get("job_type") == IMPORT_JOB_AUTOMATION
            ):
                continue
            last = _as_datetime(
                row.get("preview_heartbeat_at")
                or row.get("preview_started_at")
                or row.get("updated_at")
            )
            if last < cutoff:
                stale.append(row)
        stale.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]))
        updated_jobs = []
        for row in stale[:limit]:
            now = _utcnow()
            row["preview_status"] = "waiting"
            row["preview_message"] = message
            row["preview_error"] = None
            row["preview_worker_id"] = None
            row["preview_started_at"] = None
            row["preview_heartbeat_at"] = None
            row["updated_at"] = now
            updated_jobs.append(ImportJob.from_row(copy.deepcopy(row)))
        return updated_jobs

    def requeue_running_import_preview_jobs(
        self,
        *,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        running = [
            row for row in self._import_jobs
            if row.get("status") == "queued"
            and row.get("preview_status") == "running"
            and row.get("job_type") != IMPORT_JOB_AUTOMATION
        ]
        running.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]))
        updated_jobs = []
        for row in running[:limit]:
            now = _utcnow()
            row["preview_status"] = "waiting"
            row["preview_message"] = message
            row["preview_error"] = None
            row["preview_worker_id"] = None
            row["preview_started_at"] = None
            row["preview_heartbeat_at"] = None
            row["updated_at"] = now
            updated_jobs.append(ImportJob.from_row(copy.deepcopy(row)))
        return updated_jobs

