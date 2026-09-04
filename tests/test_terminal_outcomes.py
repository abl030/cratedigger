"""Real-PostgreSQL contracts for terminal import/preview outcome atomicity."""

from __future__ import annotations

import dataclasses
import json
import tempfile
import threading
import unittest
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import msgspec
import psycopg2.errors

from lib import transitions
from lib.dispatch import DispatchOutcome
from lib.dispatch.types import PostCommitCleanup
from lib.download_processing import Completed, CompletionFailed, CompletionResult
from lib.import_execution import (
    AutomationOwnerFailStop,
    CancellationToken,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
    ProcessIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
    local_import_payload,
    youtube_import_payload,
)
from lib.import_worker_loop import execution_lease_from_job
from lib.pipeline_db import (
    BACKOFF_BASE_MINUTES,
    PipelineDB,
)
from lib.pipeline_db.cleanup_journal import (
    _CleanupCursor,
    _LockedCleanupScope,
)
from lib.quality import ActiveDownloadState
from lib.quality_evidence import snapshot_audio_files
from lib.terminal_outcomes import (
    AutomationTerminalAuthority,
    ImportJobTerminal,
    ImportTerminalOutcome,
    PreviewTerminalOutcome,
    RequestPolicyOutcome,
    RequestRejectionOutcome,
    RequestSuccessOutcome,
    TerminalCooldown,
    TerminalDenylist,
    TerminalDownloadAudit,
)
from tests.dispatch_helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.fakes.download import RecordingProcessAlbum
from tests.helpers import make_ctx_with_fake_db, make_request_row
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


class InjectedTerminalWriteFailure(RuntimeError):
    """Deterministic failure raised immediately after one DB write."""


class FaultInjectingPipelineDB(PipelineDB):
    """Real adapter whose only fake seam is the post-write failure hook."""

    def __init__(self, dsn: str, *, fail_after_write: int) -> None:
        super().__init__(dsn)
        self.fail_after_write = fail_after_write
        self.write_boundaries: list[str] = []

    def _terminal_outcome_write_boundary(self, index: int, label: str) -> None:
        self.write_boundaries.append(label)
        if index == self.fail_after_write:
            raise InjectedTerminalWriteFailure(label)


class PausingTerminalPipelineDB(PipelineDB):
    """Expose the point after the terminal transaction owns the row lock."""

    def __init__(
        self,
        dsn: str,
        *,
        locked: threading.Event,
        release: threading.Event,
    ) -> None:
        super().__init__(dsn)
        self.locked = locked
        self.release = release

    def _lock_terminal_request_status(self, request_id: int) -> str | None:
        status = super()._lock_terminal_request_status(request_id)
        self.locked.set()
        if not self.release.wait(timeout=10):
            raise TimeoutError("terminal row-lock test was not released")
        return status


class PausingAutomationTerminalPipelineDB(PipelineDB):
    """Pause after request lock, before terminalization locks request jobs."""

    def __init__(
        self,
        dsn: str,
        *,
        request_locked: threading.Event,
        release: threading.Event,
    ) -> None:
        super().__init__(dsn)
        self.request_locked = request_locked
        self.release = release

    def _lock_processing_cleanup_scope(
        self,
        cur: _CleanupCursor,
        *,
        request_id: int,
    ) -> _LockedCleanupScope:
        cur.execute(
            """
            SELECT status, active_automation_import_job_id
            FROM album_requests
            WHERE id = %s
            FOR UPDATE
            """,
            (request_id,),
        )
        if cur.fetchone() is None:
            raise AssertionError("terminal request disappeared")
        self.request_locked.set()
        if not self.release.wait(timeout=10):
            raise TimeoutError("automation terminal overlap was not released")
        return super()._lock_processing_cleanup_scope(
            cur,
            request_id=request_id,
        )


class ObservedOperatorPipelineDB(PipelineDB):
    """Signal immediately before the operator's status CAS can block."""

    def __init__(self, dsn: str, *, cas_started: threading.Event) -> None:
        super().__init__(dsn)
        self.cas_started = cas_started

    def update_status(
        self,
        request_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        self.cas_started.set()
        return super().update_status(
            request_id,
            status,
            expected_status=expected_status,
            **extra,
        )

    def compare_request_status(
        self,
        request_id: int,
        *,
        expected_status: str,
    ) -> bool:
        self.cas_started.set()
        return super().compare_request_status(
            request_id,
            expected_status=expected_status,
        )

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        clear_retry_counters: bool = True,
        **extra: Any,
    ) -> bool:
        self.cas_started.set()
        return super().reset_to_wanted(
            request_id,
            expected_status=expected_status,
            clear_retry_counters=clear_retry_counters,
            **extra,
        )


def _snapshot(db: PipelineDB, request_id: int, job_id: int) -> dict[str, object]:
    request_cur = db._execute(
        """
        SELECT status, active_download_state, download_attempts,
               validation_attempts,
               active_automation_import_job_id,
               search_filetype_override, beets_distance, beets_scenario,
               min_bitrate, prev_min_bitrate,
               verified_lossless, rescued_at, prior_unfindable_category,
               unfindable_category
        FROM album_requests WHERE id = %s
        """,
        (request_id,),
    )
    request = request_cur.fetchone()
    job_cur = db._execute(
        """
        SELECT status, result, message, error, completed_at,
               preview_status, preview_result, preview_message,
               preview_error, preview_completed_at,
               execution_invocation_id, execution_host_boot_id,
               execution_systemd_unit, execution_worker_pid,
               execution_worker_start_ticks, execution_beets_pid,
               execution_beets_start_ticks
        FROM import_jobs WHERE id = %s
        """,
        (job_id,),
    )
    job = job_cur.fetchone()
    counts_cur = db._execute(
        """
        SELECT
          (SELECT COUNT(*)::int FROM download_log WHERE request_id = %s) AS logs,
          (SELECT COUNT(*)::int FROM source_denylist WHERE request_id = %s) AS denied,
          (SELECT COUNT(*)::int FROM user_cooldowns) AS cooldowns,
          (SELECT COUNT(*)::int FROM processing_cleanup_journal
           WHERE request_id = %s AND job_id = %s) AS cleanup_journals
        """,
        (request_id, request_id, request_id, job_id),
    )
    counts = counts_cur.fetchone()
    assert request is not None and job is not None and counts is not None
    return {
        "request": dict(request),
        "job": dict(job),
        "counts": dict(counts),
    }


SEEDED_BACKOFF_MINUTES = 7
"""A deliberately unproducible prior backoff — real ones are 30 * 2**n."""

_SEEDED_LAST_ATTEMPT_AT = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


def _seed_terminal_retry_state(
    db: PipelineDB,
    request_id: int,
    *,
    search_attempts: int,
    download_attempts: int,
    validation_attempts: int,
) -> None:
    """Give the locked row real prior automatic-retry history to preserve."""
    db._execute(
        """
        UPDATE album_requests
        SET search_attempts = %s,
            download_attempts = %s,
            validation_attempts = %s,
            last_attempt_at = %s,
            next_retry_after = %s
        WHERE id = %s
        """,
        (
            search_attempts,
            download_attempts,
            validation_attempts,
            _SEEDED_LAST_ATTEMPT_AT,
            _SEEDED_LAST_ATTEMPT_AT + timedelta(
                minutes=SEEDED_BACKOFF_MINUTES,
            ),
            request_id,
        ),
    )


def read_terminal_retry_state(
    db: PipelineDB,
    request_id: int,
) -> dict[str, object]:
    """Read exactly the retry-accounting columns a terminal edge rewrites."""
    cur = db._execute(
        """
        SELECT search_attempts, download_attempts, validation_attempts,
               last_attempt_at, next_retry_after
        FROM album_requests
        WHERE id = %s
        """,
        (request_id,),
    )
    row = cur.fetchone()
    assert row is not None
    return dict(row)


def terminal_backoff_minutes(state: Mapping[str, object]) -> int | None:
    """Return the automatic backoff window the row currently carries."""
    last_attempt_at = state["last_attempt_at"]
    next_retry_after = state["next_retry_after"]
    if last_attempt_at is None or next_retry_after is None:
        return None
    assert isinstance(last_attempt_at, datetime)
    assert isinstance(next_retry_after, datetime)
    return round(
        (next_retry_after - last_attempt_at).total_seconds() / 60
    )


def _seed_running_import(
    *,
    unfindable: bool = False,
    automation_state: bool = False,
    cooldown_username: str | None = None,
    job_type: str = IMPORT_JOB_FORCE,
    payload: dict[str, object] | None = None,
) -> tuple[PipelineDB, int, int]:
    db = make_db()
    request_id = db.add_request(
        mb_release_id="terminal-outcome",
        artist_name="Atomic",
        album_title="Outcome",
        source="request",
    )
    if unfindable:
        from datetime import datetime

        db.set_unfindable_category(
            request_id,
            category="artist_absent",
            categorised_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
    if cooldown_username is not None:
        for _ in range(5):
            db.log_download(
                request_id,
                soulseek_username=cooldown_username,
                outcome="failed",
                error_message="prior source failure",
            )
    if automation_state:
        processing_path = tempfile.mkdtemp(prefix="atomic-processing-")
        with open(
            f"{processing_path}/01.flac",
            "wb",
        ) as fixture:
            fixture.write(b"terminal fixture")
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-07-14T00:00:00+00:00",
            files=[],
            processing_started_at="2026-07-14T00:01:00+00:00",
            current_path=processing_path,
        )
        job = handoff_automation_owner(
            db,
            request_id,
            state=state.to_json(),
            canonical_path=processing_path,
        )
        preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="terminal-boot",
            invocation_id=f"terminal-preview-{job.id}",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(7101, 71001),
        )
        assert claim_next_import_preview_job(db, worker_id="terminal-preview",
        execution_lease=preview_lease,) is not None
        files = snapshot_audio_files(processing_path)
        evidence = make_album_quality_evidence(
            mb_release_id="terminal-outcome",
            source_path=processing_path,
            files=files,
        )
        db.upsert_album_quality_evidence(evidence)
        persisted_evidence = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert (
            persisted_evidence is not None
            and persisted_evidence.id is not None
        )
        db.set_import_job_candidate_evidence(
            job.id,
            persisted_evidence.id,
            expected_execution_lease=preview_lease,
        )
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
            expected_execution_lease=preview_lease,
        )
        execution_lease = ExecutionLeaseSnapshot(
            host_boot_id="terminal-boot",
            invocation_id=f"terminal-importer-{job.id}",
            systemd_unit="cratedigger-importer.service",
            worker=ProcessIdentity(7102, 71002),
        )
    else:
        db._execute(
            "UPDATE album_requests SET status = 'downloading', "
            "active_download_state = %s::jsonb WHERE id = %s",
            ("{}", request_id),
        )
        job = db.enqueue_import_job(
            job_type,
            request_id=request_id,
            dedupe_key=f"atomic:{request_id}:{job_type}",
            payload=payload or {
                "download_log_id": 1,
                "failed_path": "/tmp/atomic-force",
            },
        )
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
        )
        execution_lease = None
    claimed = claim_next_import_job(db, worker_id="atomic-test",
    execution_lease=execution_lease,)
    assert claimed is not None
    return db, request_id, claimed.id


def _seed_running_preview() -> tuple[PipelineDB, int, int]:
    db = make_db()
    request_id = db.add_request(
        mb_release_id="terminal-preview",
        artist_name="Atomic",
        album_title="Preview",
        source="request",
    )
    db._execute(
        "UPDATE album_requests SET status = 'downloading', "
        "active_download_state = '{}'::jsonb WHERE id = %s",
        (request_id,),
    )
    db.enqueue_import_job(
        IMPORT_JOB_FORCE,
        request_id=request_id,
        dedupe_key=f"preview:{request_id}",
        payload={
            "download_log_id": 1,
            "failed_path": "/tmp/atomic-preview",
        },
    )
    claimed = claim_next_import_preview_job(db, worker_id="preview-test")
    assert claimed is not None
    return db, request_id, claimed.id


def _seed_running_automation_preview() -> tuple[PipelineDB, int, int]:
    db = make_db()
    request_id = db.add_request(
        mb_release_id="terminal-automation-preview",
        artist_name="Atomic",
        album_title="Automation Preview",
        source="request",
    )
    processing_path = tempfile.mkdtemp(prefix="atomic-preview-processing-")
    with open(f"{processing_path}/01.flac", "wb") as fixture:
        fixture.write(b"terminal preview fixture")
    job = handoff_automation_owner(
        db,
        request_id,
        state=ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-07-14T00:00:00+00:00",
            files=[],
            current_path=processing_path,
        ).to_json(),
        canonical_path=processing_path,
    )
    preview_lease = ExecutionLeaseSnapshot(
        host_boot_id="terminal-boot",
        invocation_id=f"terminal-preview-{job.id}",
        systemd_unit="cratedigger-import-preview-worker.service",
        worker=ProcessIdentity(7201, 72001),
    )
    claimed = claim_next_import_preview_job(db, worker_id="terminal-preview",
    execution_lease=preview_lease,)
    assert claimed is not None
    return db, request_id, claimed.id


def _searching_import_outcome(
    request_id: int,
    job_id: int,
) -> ImportTerminalOutcome:
    return ImportTerminalOutcome(
        request_id=request_id,
        import_job_id=job_id,
        initial_transition=transitions.RequestTransition.to_imported(
            verified_lossless=False,
        ),
        audit=TerminalDownloadAudit(outcome="success"),
        post_audit_transitions=(
            transitions.RequestTransition.to_wanted(
                from_status="imported",
                search_filetype_override="lossless",
                min_bitrate=320,
            ),
        ),
        job=ImportJobTerminal(
            status="completed",
            result={"success": True},
            message="Import successful",
        ),
    )


def _prepare_automation_terminal_command(
    db: PipelineDB,
    request_id: int,
    job_id: int,
) -> ImportTerminalOutcome:
    """Complete cleanup, then build the exact owner-atomic terminal command."""
    from scripts import importer

    job = db.get_import_job(job_id)
    request = db.get_request(request_id)
    assert job is not None and request is not None
    lease = execution_lease_from_job(job)
    assert lease is not None
    state = ActiveDownloadState.from_raw(request["active_download_state"])
    assert state.current_path is not None
    token = CancellationToken()
    with db._pin_owner_session(token) as owner_session_identity:
        cleanup_receipt = importer._complete_automation_processing_cleanup(
            db,
            job,
            DispatchOutcome(
                success=True,
                message="automation terminal fixture",
                post_commit_cleanup=PostCommitCleanup(
                    staged_path=state.current_path,
                ),
            ),
            execution_lease=lease,
            cancellation_token=token,
            owner_session_identity=owner_session_identity,
        )
    return ImportTerminalOutcome(
        request_id=request_id,
        import_job_id=job_id,
        initial_transition=transitions.RequestTransition.to_imported(
            from_status="processing",
            verified_lossless=True,
        ),
        audit=TerminalDownloadAudit(
            outcome="success",
            soulseek_username="automation-peer",
            validation_result=(
                '{"valid":true,"scenario":"automation_terminal"}'
            ),
        ),
        denylists=(
            TerminalDenylist(
                "cooldown-peer",
                "automation terminal",
                apply_cooldown=True,
            ),
        ),
        job=ImportJobTerminal(
            status="completed",
            result={"success": True},
            message="automation imported",
        ),
        successful_terminal_acceptance=True,
        automation=AutomationTerminalAuthority(
            expected_job_status="running",
            expected_preview_status=job.preview_status,
            expected_execution_lease=lease,
            cleanup_receipt=cleanup_receipt,
        ),
    )


def automation_requeue_command(
    prepared: ImportTerminalOutcome,
    *,
    attempt_type: str | None,
) -> ImportTerminalOutcome:
    """Rebuild a prepared owner command as a processing -> wanted rejection."""
    return replace(
        prepared,
        initial_transition=transitions.RequestTransition.to_wanted(
            from_status="processing",
            attempt_type=attempt_type,
        ),
        audit=TerminalDownloadAudit(
            outcome="rejected",
            soulseek_username="automation-peer",
            validation_result='{"valid":false,"scenario":"automation_reject"}',
        ),
        job=ImportJobTerminal(
            status="failed",
            result={"success": False},
            message="automation rejected",
            error="automation rejected",
        ),
        successful_terminal_acceptance=False,
    )


def _prepare_automation_preview_terminal_command(
    db: PipelineDB,
    request_id: int,
    job_id: int,
) -> PreviewTerminalOutcome:
    from scripts import importer

    job = db.get_import_job(job_id)
    request = db.get_request(request_id)
    assert job is not None and request is not None
    lease = execution_lease_from_job(job)
    assert lease is not None
    state = ActiveDownloadState.from_raw(request["active_download_state"])
    assert state.current_path is not None
    token = CancellationToken()
    with db._pin_owner_session(token) as owner_session_identity:
        cleanup_receipt = importer._complete_automation_processing_cleanup(
            db,
            job,
            DispatchOutcome(
                success=False,
                message="automation preview terminal fixture",
                post_commit_cleanup=PostCommitCleanup(
                    staged_path=state.current_path,
                ),
            ),
            execution_lease=lease,
            cancellation_token=token,
            owner_session_identity=owner_session_identity,
        )
    return PreviewTerminalOutcome(
        request_id=request_id,
        import_job_id=job_id,
        request_transition=transitions.RequestTransition.to_wanted(
            from_status="processing",
            attempt_type="validation",
        ),
        audit=TerminalDownloadAudit(
            outcome="measurement_failed",
            validation_result=(
                '{"reason":"snapshot_stale",'
                '"scenario":"measurement_failed"}'
            ),
        ),
        preview_status="measurement_failed",
        preview_result={"reason": "snapshot_stale"},
        message="Preview measurement failed",
        error="snapshot_stale",
        automation=AutomationTerminalAuthority(
            expected_job_status="queued",
            expected_preview_status=job.preview_status,
            expected_execution_lease=lease,
            cleanup_receipt=cleanup_receipt,
        ),
    )


@requires_postgres
class TestTerminalOutcomeAtomicity(unittest.TestCase):
    """Every injected write-boundary failure is invisible to a fresh session."""

    def _assert_rolls_back_at_every_boundary(
        self,
        *,
        seed,
        command_factory,
        expected_boundaries: tuple[str, ...],
        persist_method: str,
    ) -> None:
        assert TEST_DSN is not None
        for fail_after, expected_label in enumerate(expected_boundaries, start=1):
            with self.subTest(boundary=expected_label):
                seed_db, request_id, job_id = seed()
                self.addCleanup(seed_db.close)
                before_observer = PipelineDB(TEST_DSN)
                before = _snapshot(before_observer, request_id, job_id)
                before_observer.close()
                command = command_factory(request_id, job_id)
                failing = FaultInjectingPipelineDB(
                    TEST_DSN,
                    fail_after_write=fail_after,
                )
                try:
                    with self.assertRaises(InjectedTerminalWriteFailure):
                        getattr(failing, persist_method)(command)
                    self.assertEqual(failing.write_boundaries[-1], expected_label)
                finally:
                    failing.close()

                observer = PipelineDB(TEST_DSN)
                try:
                    self.assertEqual(
                        _snapshot(observer, request_id, job_id),
                        before,
                        "fresh observer saw a partial terminal outcome",
                    )
                finally:
                    observer.close()

    def test_automation_terminal_bundle_is_all_or_none_at_every_boundary(
        self,
    ) -> None:
        assert TEST_DSN is not None
        expected_boundaries = (
            "download_log",
            "denylist",
            "cooldown",
            "import_job.completed",
            "processing_cleanup.consumed",
            "request.processing_metadata",
            "request.processing_to_imported",
        )
        for fail_after, expected_label in enumerate(
            expected_boundaries,
            start=1,
        ):
            with self.subTest(boundary=expected_label):
                seed, request_id, job_id = _seed_running_import(
                    automation_state=True,
                    cooldown_username="cooldown-peer",
                )
                command = _prepare_automation_terminal_command(
                    seed,
                    request_id,
                    job_id,
                )
                before = _snapshot(seed, request_id, job_id)
                seed.close()
                writer = FaultInjectingPipelineDB(
                    TEST_DSN,
                    fail_after_write=fail_after,
                )
                try:
                    with self.assertRaises(InjectedTerminalWriteFailure):
                        writer.persist_import_terminal_outcome(command)
                    self.assertEqual(
                        writer.write_boundaries[-1],
                        expected_label,
                    )
                finally:
                    writer.close()

                observer = PipelineDB(TEST_DSN)
                try:
                    self.assertEqual(
                        _snapshot(observer, request_id, job_id),
                        before,
                        "owner-atomic terminal bundle leaked a partial write",
                    )
                finally:
                    observer.close()

    def test_automation_terminal_bundle_consumes_exact_cleanup_last_owner(
        self,
    ) -> None:
        db, request_id, job_id = _seed_running_import(
            automation_state=True,
        )
        self.addCleanup(db.close)
        command = _prepare_automation_terminal_command(
            db,
            request_id,
            job_id,
        )

        result = db.persist_import_terminal_outcome(command)

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "imported")
        self.assertIsNone(request["active_automation_import_job_id"])
        self.assertIsNone(request["active_download_state"])
        self.assertEqual(result.job.status, "completed")
        assert result.job.result is not None
        authority = command.automation
        assert authority is not None
        expected_cleanup = msgspec.to_builtins(
            authority.cleanup_receipt
        )
        self.assertEqual(
            result.job.result["processing_cleanup"],
            expected_cleanup,
        )
        history = db.get_download_history(request_id)
        self.assertEqual(len(history), 1)
        validation = history[0]["validation_result"]
        assert isinstance(validation, dict)
        self.assertEqual(
            validation["processing_cleanup"],
            result.job.result["processing_cleanup"],
        )
        counts = _snapshot(db, request_id, job_id)["counts"]
        assert isinstance(counts, dict)
        self.assertEqual(
            counts["cleanup_journals"],
            0,
        )

    def test_automation_terminal_preserves_decoded_json_shapes(self) -> None:
        db, request_id, job_id = _seed_running_import(
            automation_state=True,
        )
        self.addCleanup(db.close)
        command = _prepare_automation_terminal_command(
            db,
            request_id,
            job_id,
        )
        current_job = db.get_import_job(job_id)
        assert current_job is not None
        existing_result = dict(current_job.result or {})
        existing_json = {
            "nested": {
                "items": [1, 1.25, True, None, {"label": "existing"}],
            },
            "empty_object": {},
            "empty_list": [],
        }
        existing_result["existing_json"] = existing_json
        db._execute(
            "UPDATE import_jobs SET result = %s::jsonb WHERE id = %s",
            (json.dumps(existing_result), job_id),
        )
        terminal_json = {
            "success": True,
            "terminal_json": {
                "items": [0, -4.5, False, None, {"label": "terminal"}],
                "empty_object": {},
                "empty_list": [],
            },
        }
        audit_json = {
            "valid": True,
            "audit_json": {
                "items": [2, 9.75, False, None, {"label": "audit"}],
                "empty_object": {},
                "empty_list": [],
            },
        }
        command = replace(
            command,
            audit=replace(
                command.audit,
                validation_result=json.dumps(audit_json),
            ),
            job=replace(command.job, result=terminal_json),
        )

        result = db.persist_import_terminal_outcome(command)

        assert result.job.result is not None
        self.assertEqual(
            result.job.result["existing_json"],
            existing_json,
        )
        self.assertEqual(
            result.job.result["terminal_json"],
            terminal_json["terminal_json"],
        )
        validation = db.get_download_history(request_id)[0][
            "validation_result"
        ]
        assert isinstance(validation, dict)
        self.assertEqual(validation["audit_json"], audit_json["audit_json"])

    def test_automation_preview_terminal_bundle_is_all_or_none(
        self,
    ) -> None:
        assert TEST_DSN is not None
        expected_boundaries = (
            "download_log",
            "import_job.preview_failed",
            "processing_cleanup.consumed",
            "request.processing_to_wanted",
        )
        for fail_after, expected_label in enumerate(
            expected_boundaries,
            start=1,
        ):
            with self.subTest(boundary=expected_label):
                seed, request_id, job_id = (
                    _seed_running_automation_preview()
                )
                command = _prepare_automation_preview_terminal_command(
                    seed,
                    request_id,
                    job_id,
                )
                before = _snapshot(seed, request_id, job_id)
                seed.close()
                writer = FaultInjectingPipelineDB(
                    TEST_DSN,
                    fail_after_write=fail_after,
                )
                try:
                    with self.assertRaises(InjectedTerminalWriteFailure):
                        writer.persist_preview_terminal_outcome(command)
                    self.assertEqual(
                        writer.write_boundaries[-1],
                        expected_label,
                    )
                finally:
                    writer.close()
                observer = PipelineDB(TEST_DSN)
                try:
                    self.assertEqual(
                        _snapshot(observer, request_id, job_id),
                        before,
                    )
                finally:
                    observer.close()

    def test_automation_preview_terminal_consumes_cleanup_and_owner(
        self,
    ) -> None:
        db, request_id, job_id = _seed_running_automation_preview()
        self.addCleanup(db.close)
        command = _prepare_automation_preview_terminal_command(
            db,
            request_id,
            job_id,
        )

        result = db.persist_preview_terminal_outcome(command)

        request = db.get_request(request_id)
        assert request is not None and result.job.result is not None
        self.assertEqual(request["status"], "wanted")
        self.assertIsNone(request["active_automation_import_job_id"])
        self.assertIsNone(request["active_download_state"])
        self.assertEqual(result.job.status, "failed")
        self.assertEqual(result.job.preview_status, "measurement_failed")
        cleanup = result.job.result["processing_cleanup"]
        history = db.get_download_history(request_id)
        validation = history[0]["validation_result"]
        assert isinstance(validation, dict)
        self.assertEqual(
            validation["processing_cleanup"],
            cleanup,
        )
        counts = _snapshot(db, request_id, job_id)["counts"]
        assert isinstance(counts, dict)
        self.assertEqual(
            counts["cleanup_journals"],
            0,
        )

    def test_fake_matches_real_automation_preview_terminal_bundle(
        self,
    ) -> None:
        from scripts import importer

        real, request_id, job_id = _seed_running_automation_preview()
        self.addCleanup(real.close)
        real_command = _prepare_automation_preview_terminal_command(
            real,
            request_id,
            job_id,
        )

        class RecordingFakePipelineDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self.boundaries: list[str] = []

            def _terminal_outcome_write_boundary(
                self,
                index: int,
                label: str,
            ) -> None:
                del index
                self.boundaries.append(label)

        fake = RecordingFakePipelineDB()
        fake.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id="terminal-automation-preview",
        ))
        processing_path = tempfile.mkdtemp(prefix="fake-preview-terminal-")
        with open(f"{processing_path}/01.flac", "wb") as fixture:
            fixture.write(b"terminal preview fixture")
        fake_job = handoff_automation_owner(
            fake,
            42,
            state=ActiveDownloadState(
                filetype="flac",
                enqueued_at="2026-07-14T00:00:00+00:00",
                files=[],
                current_path=processing_path,
            ).to_json(),
            canonical_path=processing_path,
        )
        lease = ExecutionLeaseSnapshot(
            host_boot_id="terminal-boot",
            invocation_id=f"terminal-preview-{fake_job.id}",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(7201, 72001),
        )
        claimed = claim_next_import_preview_job(fake, worker_id="terminal-preview",
        execution_lease=lease,)
        assert claimed is not None
        token = CancellationToken()
        with fake._pin_owner_session(token) as owner_session_identity:
            receipt = importer._complete_automation_processing_cleanup(
                fake,
                claimed,
                DispatchOutcome(
                    success=False,
                    message="fake preview parity",
                    post_commit_cleanup=PostCommitCleanup(
                        staged_path=processing_path,
                    ),
                ),
                execution_lease=lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )
        fake_command = replace(
            real_command,
            request_id=42,
            import_job_id=claimed.id,
            automation=AutomationTerminalAuthority(
                expected_job_status="queued",
                expected_preview_status=claimed.preview_status,
                expected_execution_lease=lease,
                cleanup_receipt=receipt,
            ),
        )

        assert TEST_DSN is not None
        recording_real = FaultInjectingPipelineDB(
            TEST_DSN,
            fail_after_write=999,
        )
        self.addCleanup(recording_real.close)
        real_result = recording_real.persist_preview_terminal_outcome(
            real_command
        )
        fake_result = fake.persist_preview_terminal_outcome(fake_command)
        real_request = recording_real.get_request(request_id)
        assert (
            real_request is not None
            and real_result.job.result is not None
            and fake_result.job.result is not None
        )
        fake_request = fake.request(42)
        self.assertEqual(
            (
                real_request["status"],
                real_request["active_automation_import_job_id"],
                real_result.job.status,
                real_result.job.preview_status,
                real_result.job.result["processing_cleanup"]["action"],
            ),
            (
                fake_request["status"],
                fake_request["active_automation_import_job_id"],
                fake_result.job.status,
                fake_result.job.preview_status,
                fake_result.job.result["processing_cleanup"]["action"],
            ),
        )
        expected_boundaries = [
            "download_log",
            "import_job.preview_failed",
            "processing_cleanup.consumed",
            "request.processing_to_wanted",
        ]
        self.assertEqual(recording_real.write_boundaries, expected_boundaries)
        self.assertEqual(fake.boundaries, expected_boundaries)

    def test_automation_completion_capture_is_exact_and_idempotent(
        self,
    ) -> None:
        from lib.import_execution import read_process_start_ticks
        from lib.import_job_recovery_service import (
            AutomationCompletionReceipt,
        )

        db, request_id, job_id = _seed_running_import(
            automation_state=True,
        )
        self.addCleanup(db.close)
        job = db.get_import_job(job_id)
        request = db.get_request(request_id)
        assert job is not None and request is not None
        lease = ExecutionLeaseSnapshot(
            host_boot_id=job.execution_host_boot_id or "",
            invocation_id=job.execution_invocation_id or "",
            systemd_unit=job.execution_systemd_unit or "",
            worker=ProcessIdentity(
                job.execution_worker_pid or 0,
                job.execution_worker_start_ticks or 0,
            ),
        )
        state = ActiveDownloadState.from_raw(
            request["active_download_state"]
        )
        assert state.current_path is not None
        authorized = db.authorize_import_job_launch(
            job_id,
            request_id=request_id,
            release_id="terminal-outcome",
            source_path=state.current_path,
            expected_execution_lease=lease,
        )
        assert authorized is not None
        beets = ProcessIdentity(
            pid=1,
            start_ticks=read_process_start_ticks(1),
        )
        child = db.record_import_job_beets_child(
            job_id,
            expected_execution_lease=lease,
            beets_pid=beets.pid,
            beets_start_ticks=beets.start_ticks,
        )
        assert child is not None
        child_lease = replace(lease, beets=beets)
        receipt = AutomationCompletionReceipt(
            job_id=job_id,
            request_id=request_id,
            release_id="terminal-outcome",
            canonical_path=state.current_path,
            returncode=0,
            captured_at="2026-07-29T04:00:00+00:00",
        )

        first = db.capture_automation_import_completion(
            job_id,
            expected_execution_lease=child_lease,
            receipt=receipt,
        )
        replay = db.capture_automation_import_completion(
            job_id,
            expected_execution_lease=child_lease,
            receipt=receipt,
        )
        conflict = db.capture_automation_import_completion(
            job_id,
            expected_execution_lease=child_lease,
            receipt=msgspec.structs.replace(receipt, returncode=1),
        )

        assert (
            first is not None
            and replay is not None
            and first.result is not None
            and replay.result is not None
        )
        self.assertEqual(first.result, replay.result)
        self.assertEqual(
            first.result["automation_completion"],
            msgspec.to_builtins(receipt),
        )
        self.assertIsNone(conflict)
        persisted = db.get_import_job(job_id)
        assert persisted is not None
        self.assertEqual(persisted.result, first.result)

    def test_automation_completion_capture_rejects_stale_fences(
        self,
    ) -> None:
        from lib.import_execution import read_process_start_ticks
        from lib.import_job_recovery_service import (
            AutomationCompletionReceipt,
        )

        db, request_id, job_id = _seed_running_import(
            automation_state=True,
        )
        self.addCleanup(db.close)
        job = db.get_import_job(job_id)
        request = db.get_request(request_id)
        assert job is not None and request is not None
        lease = ExecutionLeaseSnapshot(
            host_boot_id=job.execution_host_boot_id or "",
            invocation_id=job.execution_invocation_id or "",
            systemd_unit=job.execution_systemd_unit or "",
            worker=ProcessIdentity(
                job.execution_worker_pid or 0,
                job.execution_worker_start_ticks or 0,
            ),
        )
        state = ActiveDownloadState.from_raw(
            request["active_download_state"]
        )
        assert state.current_path is not None
        assert db.authorize_import_job_launch(
            job_id,
            request_id=request_id,
            release_id="terminal-outcome",
            source_path=state.current_path,
            expected_execution_lease=lease,
        ) is not None
        beets = ProcessIdentity(1, read_process_start_ticks(1))
        assert db.record_import_job_beets_child(
            job_id,
            expected_execution_lease=lease,
            beets_pid=beets.pid,
            beets_start_ticks=beets.start_ticks,
        ) is not None
        child_lease = replace(lease, beets=beets)
        receipt = AutomationCompletionReceipt(
            job_id=job_id,
            request_id=request_id,
            release_id="terminal-outcome",
            canonical_path=state.current_path,
            returncode=0,
            captured_at="2026-07-29T04:00:00+00:00",
        )
        stale_receipts = (
            msgspec.structs.replace(receipt, job_id=job_id + 1),
            msgspec.structs.replace(receipt, request_id=request_id + 1),
            msgspec.structs.replace(receipt, release_id="wrong-release"),
            msgspec.structs.replace(receipt, canonical_path="/wrong/path"),
        )
        for stale in stale_receipts:
            with self.subTest(stale=stale):
                self.assertIsNone(db.capture_automation_import_completion(
                    job_id,
                    expected_execution_lease=child_lease,
                    receipt=stale,
                ))
        self.assertIsNone(db.capture_automation_import_completion(
            job_id,
            expected_execution_lease=replace(
                child_lease,
                invocation_id="wrong-invocation",
            ),
            receipt=receipt,
        ))
        persisted = db.get_import_job(job_id)
        assert persisted is not None
        self.assertTrue(
            persisted.result is None
            or "automation_completion" not in persisted.result
        )

    def test_fake_matches_real_automation_terminal_owner_bundle(self) -> None:
        from scripts import importer

        real, request_id, job_id = _seed_running_import(
            automation_state=True,
        )
        self.addCleanup(real.close)
        real_command = _prepare_automation_terminal_command(
            real,
            request_id,
            job_id,
        )

        class RecordingFakePipelineDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self.boundaries: list[str] = []

            def _terminal_outcome_write_boundary(
                self,
                index: int,
                label: str,
            ) -> None:
                del index
                self.boundaries.append(label)

        fake = RecordingFakePipelineDB()
        fake.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id="terminal-outcome",
        ))
        processing_path = tempfile.mkdtemp(prefix="fake-terminal-")
        with open(f"{processing_path}/01.flac", "wb") as fixture:
            fixture.write(b"terminal fixture")
        fake_job = handoff_automation_owner(
            fake,
            42,
            state=ActiveDownloadState(
                filetype="flac",
                enqueued_at="2026-07-14T00:00:00+00:00",
                files=[],
                current_path=processing_path,
            ).to_json(),
            canonical_path=processing_path,
        )
        preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="terminal-boot",
            invocation_id=f"terminal-preview-{fake_job.id}",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(7101, 71001),
        )
        assert claim_next_import_preview_job(fake, worker_id="terminal-preview",
        execution_lease=preview_lease,) is not None
        fake.mark_import_job_preview_importable(
            fake_job.id,
            preview_result={"ready": True},
            expected_execution_lease=preview_lease,
        )
        importer_lease = ExecutionLeaseSnapshot(
            host_boot_id="terminal-boot",
            invocation_id=f"terminal-importer-{fake_job.id}",
            systemd_unit="cratedigger-importer.service",
            worker=ProcessIdentity(7102, 71002),
        )
        claimed = claim_next_import_job(fake, worker_id="terminal-importer",
        execution_lease=importer_lease,)
        assert claimed is not None
        token = CancellationToken()
        with fake._pin_owner_session(token) as owner_session_identity:
            receipt = importer._complete_automation_processing_cleanup(
                fake,
                claimed,
                DispatchOutcome(
                    success=True,
                    message="fake parity",
                    post_commit_cleanup=PostCommitCleanup(
                        staged_path=processing_path,
                    ),
                ),
                execution_lease=importer_lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )
        fake_command = replace(
            real_command,
            request_id=42,
            import_job_id=claimed.id,
            automation=AutomationTerminalAuthority(
                expected_job_status="running",
                expected_preview_status=claimed.preview_status,
                expected_execution_lease=importer_lease,
                cleanup_receipt=receipt,
            ),
        )

        assert TEST_DSN is not None
        recording_real = FaultInjectingPipelineDB(
            TEST_DSN,
            fail_after_write=999,
        )
        self.addCleanup(recording_real.close)
        real_result = recording_real.persist_import_terminal_outcome(
            real_command
        )
        fake_result = fake.persist_import_terminal_outcome(fake_command)

        real_request = recording_real.get_request(request_id)
        assert (
            real_request is not None
            and real_result.job.result is not None
            and fake_result.job.result is not None
        )
        fake_request = fake.request(42)
        real_audit = recording_real.get_download_history(request_id)[0]
        fake_audit = fake.download_logs[0]
        real_cleanup = real_result.job.result["processing_cleanup"]
        fake_cleanup = fake_result.job.result["processing_cleanup"]
        assert isinstance(fake_audit.validation_result, str)
        fake_audit_cleanup = json.loads(fake_audit.validation_result)[
            "processing_cleanup"
        ]
        self.assertEqual(
            (
                real_request["status"],
                real_request["active_automation_import_job_id"],
                real_request["active_download_state"],
                real_result.job.status,
                real_cleanup["outcome"],
                real_cleanup["action"],
            ),
            (
                fake_request["status"],
                fake_request["active_automation_import_job_id"],
                fake_request["active_download_state"],
                fake_result.job.status,
                fake_cleanup["outcome"],
                fake_cleanup["action"],
            ),
        )
        real_validation = real_audit["validation_result"]
        assert isinstance(real_validation, dict)
        self.assertEqual(real_validation["processing_cleanup"], real_cleanup)
        self.assertEqual(fake_audit_cleanup, fake_cleanup)
        expected_boundaries = [
            "download_log",
            "denylist",
            "import_job.completed",
            "processing_cleanup.consumed",
            "request.processing_metadata",
            "request.processing_to_imported",
        ]
        self.assertEqual(recording_real.write_boundaries, expected_boundaries)
        self.assertEqual(fake.boundaries, expected_boundaries)

    def test_automation_terminal_rejects_stale_authority_before_first_write(
        self,
    ) -> None:
        assert TEST_DSN is not None
        cases = ("job", "status", "preview", "lease", "cleanup")
        for mutation in cases:
            with self.subTest(mutation=mutation):
                db, request_id, job_id = _seed_running_import(
                    automation_state=True,
                )
                command = _prepare_automation_terminal_command(
                    db,
                    request_id,
                    job_id,
                )
                authority = command.automation
                assert authority is not None
                if mutation == "job":
                    command = replace(command, import_job_id=job_id + 1000)
                elif mutation == "status":
                    command = replace(
                        command,
                        automation=replace(
                            authority,
                            expected_job_status="queued",
                        ),
                    )
                elif mutation == "preview":
                    command = replace(
                        command,
                        automation=replace(
                            authority,
                            expected_preview_status="waiting",
                        ),
                    )
                elif mutation == "lease":
                    lease = authority.expected_execution_lease
                    assert lease is not None
                    command = replace(
                        command,
                        automation=replace(
                            authority,
                            expected_execution_lease=replace(
                                lease,
                                invocation_id="stale-invocation",
                            ),
                        ),
                    )
                else:
                    cleanup_receipt = authority.cleanup_receipt
                    assert cleanup_receipt is not None
                    command = replace(
                        command,
                        automation=replace(
                            authority,
                            cleanup_receipt=msgspec.structs.replace(
                                cleanup_receipt,
                                details={"mutant": True},
                            ),
                        ),
                    )
                before = _snapshot(db, request_id, job_id)
                db.close()
                writer = FaultInjectingPipelineDB(
                    TEST_DSN,
                    fail_after_write=999,
                )
                try:
                    with self.assertRaises(RuntimeError):
                        writer.persist_import_terminal_outcome(command)
                    self.assertEqual(writer.write_boundaries, [])
                finally:
                    writer.close()
                observer = PipelineDB(TEST_DSN)
                try:
                    self.assertEqual(
                        _snapshot(observer, request_id, job_id),
                        before,
                    )
                finally:
                    observer.close()

    def test_import_success_with_quality_requeue_is_all_or_none(self):
        def command(request_id: int, job_id: int) -> ImportTerminalOutcome:
            return ImportTerminalOutcome(
                request_id=request_id,
                import_job_id=job_id,
                initial_transition=transitions.RequestTransition.to_imported(
                    beets_distance=0.04,
                    beets_scenario="strong_match",
                    verified_lossless=True,
                ),
                audit=TerminalDownloadAudit(
                    outcome="success",
                    soulseek_username="atomic-peer",
                    filetype="flac",
                    beets_detail="imported",
                    validation_result=(
                        '{"valid":true,"distance":0.04,"scenario":"strong_match"}'
                    ),
                ),
                post_audit_transitions=(
                    transitions.RequestTransition.to_imported(
                        from_status="imported",
                        prev_min_bitrate=192,
                        min_bitrate=320,
                    ),
                    transitions.RequestTransition.to_wanted(
                        from_status="imported",
                        search_filetype_override="FLAC|WAV",
                        min_bitrate=320,
                    ),
                ),
                denylists=(
                    TerminalDenylist(
                        username="cooldown-peer",
                        reason="quality gate",
                        apply_cooldown=True,
                    ),
                ),
                job=ImportJobTerminal(
                    status="completed",
                    result={"success": True, "message": "Import successful"},
                    message="Import successful",
                ),
            )

        expected = (
            "request.imported",
            "request.metadata",
            "download_log",
            "request.imported",
            "request.metadata",
            "request.wanted",
            "denylist",
            "cooldown",
            "import_job.completed",
        )
        self._assert_rolls_back_at_every_boundary(
            seed=lambda: _seed_running_import(
                unfindable=True,
                cooldown_username="cooldown-peer",
            ),
            command_factory=command,
            expected_boundaries=expected,
            persist_method="persist_import_terminal_outcome",
        )

    def test_import_rejection_is_all_or_none(self):
        def command(request_id: int, job_id: int) -> ImportTerminalOutcome:
            return ImportTerminalOutcome(
                request_id=request_id,
                import_job_id=job_id,
                initial_transition=transitions.RequestTransition.to_wanted(
                    attempt_type="validation",
                    search_filetype_override="MP3 V0|FLAC|WAV",
                ),
                audit=TerminalDownloadAudit(
                    outcome="rejected",
                    soulseek_username="bad-peer",
                    filetype="flac",
                    beets_detail="wrong pressing",
                    validation_result=(
                        '{"valid":false,"scenario":"strict_count_mismatch"}'
                    ),
                ),
                denylists=(
                    TerminalDenylist(
                        username="bad-peer",
                        reason="beets validation rejected",
                    ),
                ),
                job=ImportJobTerminal(
                    status="failed",
                    result={"success": False},
                    message="Rejected: strict_count_mismatch",
                    error="Rejected: strict_count_mismatch",
                ),
            )

        expected = (
            "request.wanted",
            "request.attempt.validation",
            "download_log",
            "denylist",
            "import_job.failed",
        )
        self._assert_rolls_back_at_every_boundary(
            seed=_seed_running_import,
            command_factory=command,
            expected_boundaries=expected,
            persist_method="persist_import_terminal_outcome",
        )

    def test_cooldown_only_analysis_abort_is_all_or_none(self):
        def seed() -> tuple[PipelineDB, int, int]:
            db, request_id, job_id = _seed_running_import()
            for _ in range(4):
                db.log_download(
                    request_id,
                    soulseek_username="analysis-peer",
                    outcome="failed",
                )
            return db, request_id, job_id

        def command(request_id: int, job_id: int) -> ImportTerminalOutcome:
            return ImportTerminalOutcome(
                request_id=request_id,
                import_job_id=job_id,
                initial_transition=transitions.RequestTransition.to_wanted(
                    attempt_type="validation",
                ),
                audit=TerminalDownloadAudit(
                    outcome="have_analysis_error",
                    soulseek_username="analysis-peer",
                    beets_scenario="have_analysis_error",
                    validation_result=(
                        '{"failure_category":"analyser_failure",'
                        '"error":"ffmpeg crashed"}'
                    ),
                ),
                cooldowns=(TerminalCooldown("analysis-peer"),),
                job=ImportJobTerminal(
                    status="failed",
                    result={"success": False},
                    message="analysis failed",
                    error="analysis failed",
                ),
            )

        self._assert_rolls_back_at_every_boundary(
            seed=seed,
            command_factory=command,
            expected_boundaries=(
                "request.wanted",
                "request.attempt.validation",
                "download_log",
                "cooldown",
                "import_job.failed",
            ),
            persist_method="persist_import_terminal_outcome",
        )

    def test_preview_measurement_failure_is_all_or_none(self):
        def command(request_id: int, job_id: int) -> PreviewTerminalOutcome:
            return PreviewTerminalOutcome(
                request_id=request_id,
                import_job_id=job_id,
                request_transition=transitions.RequestTransition.to_wanted(),
                audit=TerminalDownloadAudit(
                    outcome="measurement_failed",
                    beets_scenario="measurement_failed",
                    beets_detail="source vanished",
                    staged_path="/incoming/vanished",
                    validation_result=(
                        '{"reason":"source_missing","detail":"source vanished"}'
                    ),
                ),
                preview_status="measurement_failed",
                preview_result={"verdict": "measurement_failed"},
                message="Preview measurement failed: source_missing",
                error="source_missing",
                denylists=(
                    TerminalDenylist(
                        username="preview-peer",
                        reason="preview failure",
                    ),
                ),
            )

        expected = (
            "request.wanted",
            "download_log",
            "denylist",
            "import_job.preview_failed",
        )
        self._assert_rolls_back_at_every_boundary(
            seed=_seed_running_preview,
            command_factory=command,
            expected_boundaries=expected,
            persist_method="persist_preview_terminal_outcome",
        )

    def test_preview_terminal_audit_copies_job_candidate_evidence(self):
        db, request_id, job_id = _seed_running_preview()
        self.addCleanup(db.close)
        evidence = make_album_quality_evidence(
            mb_release_id="terminal-preview-candidate"
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job_id, persisted.id)

        result = db.persist_preview_terminal_outcome(PreviewTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            request_transition=transitions.RequestTransition.to_wanted(),
            audit=TerminalDownloadAudit(
                outcome="measurement_failed",
                soulseek_username="comma,name",
                contributor_usernames=("comma,name",),
                beets_scenario="measurement_failed",
                beets_detail="late preview failure",
                error_message="late preview failure",
            ),
            preview_status="measurement_failed",
            preview_result={"verdict": "measurement_failed"},
            message="late preview failure",
            error="late preview failure",
        ))

        self.assertEqual(
            db.get_download_log_candidate_evidence_id(result.download_log_id),
            persisted.id,
        )
        row = db._execute(
            "SELECT candidate_evidence_direct, candidate_contributor_usernames "
            "FROM download_log WHERE id = %s",
            (result.download_log_id,),
        ).fetchone()
        assert row is not None
        self.assertTrue(row["candidate_evidence_direct"])
        self.assertEqual(row["candidate_contributor_usernames"], ["comma,name"])

    def test_import_terminal_audit_copies_job_candidate_evidence(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        evidence = make_album_quality_evidence(
            mb_release_id="terminal-import-candidate"
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job_id, persisted.id)

        result = db.persist_import_terminal_outcome(ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_wanted(
                attempt_type="validation"
            ),
            audit=TerminalDownloadAudit(
                outcome="rejected",
                soulseek_username="alice, comma,name",
                contributor_usernames=("alice", "comma,name"),
                beets_detail="audio decode failed",
                error_message="audio decode failed",
            ),
            job=ImportJobTerminal(
                status="failed",
                error="audio decode failed",
                result={"success": False},
                message="audio decode failed",
            ),
        ))

        self.assertEqual(
            db.get_download_log_candidate_evidence_id(result.download_log_id),
            persisted.id,
        )
        row = db._execute(
            "SELECT candidate_evidence_direct, candidate_contributor_usernames "
            "FROM download_log WHERE id = %s",
            (result.download_log_id,),
        ).fetchone()
        assert row is not None
        self.assertTrue(row["candidate_evidence_direct"])
        self.assertEqual(
            row["candidate_contributor_usernames"],
            ["alice", "comma,name"],
        )

    def test_preview_failure_preserves_current_operator_status(self):
        db, request_id, job_id = _seed_running_preview()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )
        result = db.persist_preview_terminal_outcome(PreviewTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            request_transition=transitions.RequestTransition.to_wanted(),
            audit=TerminalDownloadAudit(
                outcome="measurement_failed",
                beets_scenario="measurement_failed",
                beets_detail="source vanished",
                validation_result=(
                    '{"reason":"source_missing","detail":"source vanished"}'
                ),
            ),
            preview_status="measurement_failed",
            preview_result={"verdict": "measurement_failed"},
            message="Preview measurement failed: source_missing",
            error="source_missing",
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "unsearchable")
        self.assertEqual(result.transitions, ())

    def test_import_rejection_preserves_operator_stop_and_policy_effects(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable', min_bitrate = 320 "
            "WHERE id = %s",
            (request_id,),
        )
        result = db.persist_import_terminal_outcome(ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_wanted(
                from_status="unsearchable",
                attempt_type="validation",
                search_filetype_override="lossless",
                min_bitrate=245,
            ),
            audit=TerminalDownloadAudit(
                outcome="rejected",
                validation_result='{"valid":false}',
            ),
            job=ImportJobTerminal(
                status="failed",
                result={"success": False},
                message="rejected",
                error="rejected",
            ),
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "unsearchable")
        self.assertEqual(request["validation_attempts"], 1)
        self.assertEqual(request["search_filetype_override"], "lossless")
        self.assertEqual(request["min_bitrate"], 245)
        self.assertEqual(request["prev_min_bitrate"], 320)
        self.assertEqual(
            tuple(item.target_status for item in result.transitions),
            ("unsearchable",),
        )

    def test_operator_action_waiting_behind_terminal_lock_retries(self):
        assert TEST_DSN is not None
        seed, request_id, job_id = _seed_running_import()
        seed._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )
        seed.close()
        locked = threading.Event()
        release = threading.Event()
        cas_started = threading.Event()
        terminal_db = PausingTerminalPipelineDB(
            TEST_DSN,
            locked=locked,
            release=release,
        )
        operator_db = ObservedOperatorPipelineDB(
            TEST_DSN,
            cas_started=cas_started,
        )
        self.addCleanup(terminal_db.close)
        self.addCleanup(operator_db.close)
        terminal_errors: list[BaseException] = []
        operator_results: list[transitions.TransitionResult] = []

        command = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_wanted(
                from_status="unsearchable",
                attempt_type="validation",
            ),
            audit=TerminalDownloadAudit(
                outcome="rejected",
                validation_result='{"valid":false}',
            ),
            job=ImportJobTerminal(
                status="failed",
                result={"success": False},
                message="rejected",
                error="rejected",
            ),
        )

        def persist_terminal() -> None:
            try:
                terminal_db.persist_import_terminal_outcome(command)
            except BaseException as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                terminal_errors.append(exc)

        def clear_operator_stop() -> None:
            operator_results.append(transitions.finalize_operator_request(
                operator_db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="unsearchable",
                ),
            ))

        terminal_thread = threading.Thread(target=persist_terminal)
        operator_thread = threading.Thread(target=clear_operator_stop)
        terminal_thread.start()
        self.assertTrue(locked.wait(timeout=10))
        operator_thread.start()
        self.assertTrue(cas_started.wait(timeout=10))
        release.set()
        terminal_thread.join(timeout=10)
        operator_thread.join(timeout=10)
        self.assertFalse(terminal_thread.is_alive())
        self.assertFalse(operator_thread.is_alive())
        self.assertEqual(terminal_errors, [])
        self.assertEqual(len(operator_results), 1)
        self.assertIsInstance(
            operator_results[0],
            transitions.TransitionApplied,
        )
        observer = PipelineDB(TEST_DSN)
        self.addCleanup(observer.close)
        request = observer.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")
        self.assertEqual(request["validation_attempts"], 0)

    def test_same_status_operator_action_still_serializes_behind_lock(self):
        assert TEST_DSN is not None
        seed, request_id, job_id = _seed_running_import()
        seed._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )
        seed.close()
        locked = threading.Event()
        release = threading.Event()
        cas_started = threading.Event()
        terminal_db = PausingTerminalPipelineDB(
            TEST_DSN,
            locked=locked,
            release=release,
        )
        operator_db = ObservedOperatorPipelineDB(
            TEST_DSN,
            cas_started=cas_started,
        )
        self.addCleanup(terminal_db.close)
        self.addCleanup(operator_db.close)
        terminal_errors: list[BaseException] = []
        operator_results: list[transitions.TransitionResult] = []
        command = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_imported(
                from_status="unsearchable",
            ),
            audit=TerminalDownloadAudit(outcome="success"),
            job=ImportJobTerminal(
                status="completed",
                result={"success": True},
                message="imported",
            ),
        )

        def persist_terminal() -> None:
            try:
                terminal_db.persist_import_terminal_outcome(command)
            except BaseException as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                terminal_errors.append(exc)

        def reassert_operator_stop() -> None:
            operator_results.append(transitions.finalize_operator_request(
                operator_db,
                request_id,
                transitions.RequestTransition.to_unsearchable(
                    from_status="unsearchable",
                ),
            ))

        terminal_thread = threading.Thread(target=persist_terminal)
        operator_thread = threading.Thread(target=reassert_operator_stop)
        terminal_thread.start()
        self.assertTrue(locked.wait(timeout=10))
        operator_thread.start()
        self.assertTrue(cas_started.wait(timeout=10))
        release.set()
        terminal_thread.join(timeout=10)
        operator_thread.join(timeout=10)
        self.assertFalse(terminal_thread.is_alive())
        self.assertFalse(operator_thread.is_alive())
        self.assertEqual(terminal_errors, [])
        self.assertEqual(len(operator_results), 1)
        self.assertIsInstance(
            operator_results[0],
            transitions.TransitionApplied,
        )
        observer = PipelineDB(TEST_DSN)
        self.addCleanup(observer.close)
        request = observer.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "unsearchable")

    def test_import_terminal_policy_preserves_current_operator_stop(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )

        result = db.persist_import_terminal_outcome(
            _searching_import_outcome(request_id, job_id)
        )

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "unsearchable")
        self.assertEqual(request["search_filetype_override"], "lossless")
        self.assertEqual(request["min_bitrate"], 320)
        self.assertEqual(
            tuple(item.target_status for item in result.transitions),
            ("unsearchable", "unsearchable"),
        )

    def test_import_terminal_policy_does_not_restore_cleared_stop(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )
        command = _searching_import_outcome(request_id, job_id)
        db._execute(
            "UPDATE album_requests SET status = 'wanted' WHERE id = %s",
            (request_id,),
        )

        result = db.persist_import_terminal_outcome(command)

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")
        self.assertEqual(request["search_filetype_override"], "lossless")
        self.assertEqual(
            tuple(item.target_status for item in result.transitions),
            ("imported", "wanted"),
        )

    def test_import_terminal_acceptance_supersedes_operator_stop(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )
        command = _searching_import_outcome(request_id, job_id)
        command = ImportTerminalOutcome(
            request_id=command.request_id,
            import_job_id=command.import_job_id,
            initial_transition=command.initial_transition,
            audit=command.audit,
            post_audit_transitions=(
                transitions.RequestTransition.to_imported(
                    from_status="imported",
                    min_bitrate=320,
                ),
            ),
            job=command.job,
            successful_terminal_acceptance=True,
        )

        result = db.persist_import_terminal_outcome(command)

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "imported")
        self.assertEqual(request["min_bitrate"], 320)
        self.assertEqual(
            tuple(item.target_status for item in result.transitions),
            ("imported", "imported"),
        )

    def test_import_rejection_preserving_imported_keeps_operator_stop(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )

        db.persist_import_terminal_outcome(ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_imported(),
            audit=TerminalDownloadAudit(outcome="rejected"),
            job=ImportJobTerminal(
                status="failed",
                error="verified_lossless_locked",
                result={"success": False},
                message="verified lossless proof locked",
            ),
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "unsearchable")

    def test_import_success_round_trip_returns_complete_bundle(self):
        db, request_id, job_id = _seed_running_import(unfindable=True)
        self.addCleanup(db.close)
        outcome = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_imported(
                beets_distance=0.04,
                beets_scenario="strong_match",
                verified_lossless=True,
            ),
            audit=TerminalDownloadAudit(
                outcome="success",
                soulseek_username="atomic-peer",
                validation_result=(
                    '{"valid":true,"distance":0.04,"scenario":"strong_match"}'
                ),
            ),
            job=ImportJobTerminal(
                status="completed",
                result={"success": True},
                message="done",
            ),
        )

        result = db.persist_import_terminal_outcome(outcome)

        self.assertEqual(result.job.status, "completed")
        self.assertEqual(result.download_log_id, db.get_download_history(request_id)[0]["id"])
        row = db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        force_distance = row["beets_distance"]
        assert force_distance is not None
        self.assertEqual(float(force_distance), 0.04)
        self.assertEqual(row["prior_unfindable_category"], "artist_absent")

    def test_job_backed_force_audit_preserves_origin_distance(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        origin_id = db.log_download(
            request_id,
            outcome="rejected",
            validation_result=(
                '{"valid":false,"distance":0.2328,"scenario":"high_distance"}'
            ),
        )
        outcome = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_imported(
            ),
            audit=TerminalDownloadAudit(
                outcome="force_import",
                validation_result=(
                    '{"valid":true,"distance":0.0,"scenario":"force_import"}'
                ),
                source_download_log_id=origin_id,
            ),
            job=ImportJobTerminal(
                status="completed",
                result={"success": True},
                message="done",
            ),
        )

        result = db.persist_import_terminal_outcome(outcome)

        row = db.get_download_log_entry(result.download_log_id)
        assert row is not None
        self.assertEqual(row["source_download_log_id"], origin_id)
        self.assertAlmostEqual(
            float(cast(float, row["original_beets_distance"])), 0.2328)

    def test_job_backed_source_less_terminal_audit_persists_typed_null(self):
        """A source-less non-automation outcome inserts a bigint NULL."""
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        outcome = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_wanted(
                attempt_type="validation",
            ),
            audit=TerminalDownloadAudit(
                outcome="failed",
                error_message="source-less terminal audit",
            ),
            job=ImportJobTerminal(
                status="failed",
                error="source-less terminal audit",
                result={"success": False},
                message="source-less terminal audit",
            ),
        )

        result = db.persist_import_terminal_outcome(outcome)

        row = db.get_download_log_entry(result.download_log_id)
        assert row is not None
        self.assertIsNone(row["source_download_log_id"])
        self.assertEqual(row["source"], "slskd")
        self.assertEqual(result.job.status, "failed")

    def test_job_backed_local_import_terminal_audit_lands_source_local(self):
        """Issue #1176 PR1 round 2: the headline provenance fix.

        ``_insert_terminal_download_audit``'s job_type-derived CASE is the
        writer of the OPERATOR-VISIBLE terminal ``download_log`` row (not
        ``log_download``'s own ``source`` default parameter, which only
        applies to *direct* callers) — a ``local_import`` job's terminal
        outcome must land ``source='local'``, not silently fall through to
        the ``ELSE 'slskd'`` branch and claim a Soulseek transfer that never
        happened."""
        db, request_id, job_id = _seed_running_import(
            job_type=IMPORT_JOB_LOCAL,
            payload=local_import_payload(
                source_path="/mnt/virtio/Music/Incoming/local/x",
                request_id=1,
            ),
        )
        self.addCleanup(db.close)
        outcome = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_imported(),
            audit=TerminalDownloadAudit(outcome="success"),
            job=ImportJobTerminal(
                status="completed",
                result={"success": True},
                message="local import complete",
            ),
        )

        result = db.persist_import_terminal_outcome(outcome)

        row = db.get_download_log_entry(result.download_log_id)
        assert row is not None
        self.assertEqual(row["source"], "local")

    def test_job_backed_youtube_import_terminal_audit_still_lands_source_youtube(
        self,
    ):
        """Must-still-work pin: adding the local_import CASE arm must not
        disturb the pre-existing youtube_import arm."""
        db, request_id, job_id = _seed_running_import(
            job_type=IMPORT_JOB_YOUTUBE,
            payload=youtube_import_payload(
                staged_path="/Incoming/auto-import/Artist - Album",
                request_id=1,
                browse_id="MPREb_terminal",
                download_log_id=1,
            ),
        )
        self.addCleanup(db.close)
        outcome = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_imported(),
            audit=TerminalDownloadAudit(outcome="success"),
            job=ImportJobTerminal(
                status="completed",
                result={"success": True},
                message="youtube import complete",
            ),
        )

        result = db.persist_import_terminal_outcome(outcome)

        row = db.get_download_log_entry(result.download_log_id)
        assert row is not None
        self.assertEqual(row["source"], "youtube")

    def test_job_backed_force_import_terminal_audit_still_lands_source_slskd(
        self,
    ):
        """Must-still-work pin: adding the local_import CASE arm must not
        disturb the ELSE branch every other (force/automation) job_type
        falls through to."""
        db, request_id, job_id = _seed_running_import()  # default: force_import
        self.addCleanup(db.close)
        outcome = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_imported(),
            audit=TerminalDownloadAudit(outcome="force_import"),
            job=ImportJobTerminal(
                status="completed",
                result={"success": True},
                message="force import complete",
            ),
        )

        result = db.persist_import_terminal_outcome(outcome)

        row = db.get_download_log_entry(result.download_log_id)
        assert row is not None
        self.assertEqual(row["source"], "slskd")

    def _run_job_backed_automation_result(
        self,
        db: PipelineDB,
        job_id: int,
        completion: CompletionResult,
    ) -> ImportJob | None:
        from scripts import importer

        job = db.get_import_job(job_id)
        assert job is not None
        execution_lease = execution_lease_from_job(job)
        assert execution_lease is not None
        process_album = RecordingProcessAlbum(outcome=completion)

        # Deliberate seam: the real-PG db rides in the fake source, whose
        # only surface this flow reaches is ``_get_db()`` (plus the mock
        # cfg). The cast spells that one type lie at the seam instead of
        # hiding it behind an Any-typed helper.
        ctx = make_ctx_with_fake_db(cast(FakePipelineDB, db))

        def execute(
            owner: PipelineDB,
            claimed: ImportJob,
            *,
            ctx: Any = None,
            execution_lease: ExecutionLeaseSnapshot,
            cancellation_token: CancellationToken,
            owner_session_identity: OwnerSessionIdentity,
        ) -> DispatchOutcome:
            return importer.execute_automation_import_job(
                owner,
                claimed,
                ctx=ctx,
                execution_lease=execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
                process_album_fn=process_album,
            )

        token = CancellationToken()
        with db._pin_owner_session(token) as owner_session_identity:
            return importer.process_claimed_job(
                db,
                job,
                ctx=ctx,
                execute_fn=execute,
                execution_lease=execution_lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )

    def test_job_backed_completed_commits_request_audit_and_job_once(self):
        db, request_id, job_id = _seed_running_import(automation_state=True)
        self.addCleanup(db.close)

        updated = self._run_job_backed_automation_result(
            db,
            job_id,
            Completed(),
        )

        assert updated is not None
        self.assertEqual(updated.status, "completed")
        row = db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        history = db.get_download_history(request_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["outcome"], "success")

    def test_job_backed_completion_failed_commits_attempt_audit_and_job_once(self):
        db, request_id, job_id = _seed_running_import(automation_state=True)
        self.addCleanup(db.close)

        updated = self._run_job_backed_automation_result(
            db,
            job_id,
            CompletionFailed(reason="staged_path_missing"),
        )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        row = db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["download_attempts"], 1)
        history = db.get_download_history(request_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["outcome"], "failed")
        self.assertEqual(history[0]["error_message"], "staged_path_missing")

    def test_automation_requeue_preserves_prior_retry_counters(self):
        """processing -> wanted retains counters so backoff keeps growing.

        The private owner edge stands in for ``("downloading", "wanted")``,
        whose canonical ``TransitionSideEffects`` does NOT clear retry
        counters. Zeroing them here would pin every repeatedly-failing
        automation request at the base retry interval forever.
        """
        db, request_id, job_id = _seed_running_import(automation_state=True)
        self.addCleanup(db.close)
        _seed_terminal_retry_state(
            db,
            request_id,
            search_attempts=5,
            download_attempts=2,
            validation_attempts=1,
        )
        prepared = _prepare_automation_terminal_command(db, request_id, job_id)

        db.persist_import_terminal_outcome(automation_requeue_command(
            prepared,
            attempt_type="validation",
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")
        state = read_terminal_retry_state(db, request_id)
        self.assertEqual(
            (
                state["search_attempts"],
                state["download_attempts"],
                state["validation_attempts"],
            ),
            (5, 2, 2),
        )
        # prior validation_attempts=1 grows the window to 30 * 2**1 minutes.
        self.assertEqual(terminal_backoff_minutes(state), 60)
        self.assertNotEqual(
            terminal_backoff_minutes(state),
            BACKOFF_BASE_MINUTES,
        )

    def test_automation_upgrade_requeue_still_clears_retry_counters(self):
        """imported -> wanted keeps the canonical clean-slate re-queue."""
        db, request_id, job_id = _seed_running_import(automation_state=True)
        self.addCleanup(db.close)
        _seed_terminal_retry_state(
            db,
            request_id,
            search_attempts=5,
            download_attempts=2,
            validation_attempts=1,
        )
        prepared = _prepare_automation_terminal_command(db, request_id, job_id)

        db.persist_import_terminal_outcome(replace(
            prepared,
            post_audit_transitions=(
                transitions.RequestTransition.to_wanted(
                    from_status="imported",
                    search_filetype_override="lossless",
                    min_bitrate=320,
                ),
            ),
            successful_terminal_acceptance=False,
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")
        self.assertEqual(request["search_filetype_override"], "lossless")
        state = read_terminal_retry_state(db, request_id)
        self.assertEqual(
            (
                state["search_attempts"],
                state["download_attempts"],
                state["validation_attempts"],
            ),
            (0, 0, 0),
        )
        self.assertIsNone(state["last_attempt_at"])
        self.assertIsNone(state["next_retry_after"])

    def test_job_backed_local_outcomes_roll_back_when_job_write_faults(self):
        assert TEST_DSN is not None
        cases = (
            (Completed(), 3),
            (CompletionFailed(reason="staged_path_missing"), 4),
        )
        for completion, job_write_boundary in cases:
            with self.subTest(completion=type(completion).__name__):
                seed_db, request_id, job_id = _seed_running_import(
                    automation_state=True,
                )
                before = _snapshot(seed_db, request_id, job_id)
                seed_db.close()
                failing = FaultInjectingPipelineDB(
                    TEST_DSN,
                    fail_after_write=job_write_boundary,
                )
                try:
                    # The owner-atomic transaction contains every write, then
                    # fail-stops the still-owning worker so lease-proven
                    # recovery can reopen the request automatically. A fault
                    # that failed to fire would leave a completed terminal, so
                    # `after == before` still proves the write actually
                    # faulted.
                    with self.assertRaises(AutomationOwnerFailStop):
                        self._run_job_backed_automation_result(
                            failing,
                            job_id,
                            completion,
                        )
                finally:
                    failing.close()

                observer = PipelineDB(TEST_DSN)
                try:
                    after = _snapshot(observer, request_id, job_id)
                    raw_counts = after["counts"]
                    assert isinstance(raw_counts, dict)
                    after_counts = dict(raw_counts)
                    self.assertEqual(after_counts["cleanup_journals"], 1)
                    after_counts["cleanup_journals"] = 0
                    after["counts"] = after_counts
                    self.assertEqual(
                        after,
                        before,
                    )
                finally:
                    observer.close()

    def test_stale_source_rolls_back_audit_and_job(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )
        before = _snapshot(db, request_id, job_id)
        command = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_imported(
                from_status="downloading",
            ),
            audit=TerminalDownloadAudit(
                outcome="success",
                validation_result='{"valid":true}',
            ),
            job=ImportJobTerminal(
                status="completed",
                result={"success": True},
                message="done",
            ),
        )

        with self.assertRaises(transitions.RequestTransitionConflict):
            db.persist_import_terminal_outcome(command)

        self.assertEqual(_snapshot(db, request_id, job_id), before)

    def test_fake_matches_real_import_terminal_method(self):
        real, request_id, job_id = _seed_running_import()
        self.addCleanup(real.close)
        fake = FakePipelineDB()
        fake.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": []},
        ))
        fake_job = fake.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={
                "download_log_id": 1,
                "failed_path": "/tmp/parity-force",
            },
        )
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = claim_next_import_job(fake, worker_id="parity")
        assert fake_claimed is not None

        def command(owner: int, owned_job: int) -> ImportTerminalOutcome:
            return ImportTerminalOutcome(
                request_id=owner,
                import_job_id=owned_job,
                initial_transition=transitions.RequestTransition.to_wanted(
                    attempt_type="validation"
                ),
                audit=TerminalDownloadAudit(
                    outcome="rejected",
                    soulseek_username="parity-peer",
                    validation_result='{"scenario":"parity"}',
                ),
                denylists=(TerminalDenylist("parity-peer", "parity"),),
                job=ImportJobTerminal(
                    status="failed",
                    error="parity",
                    result={"success": False},
                    message="parity",
                ),
            )

        real_result = real.persist_import_terminal_outcome(
            command(request_id, job_id)
        )
        fake_result = fake.persist_import_terminal_outcome(
            command(42, fake_claimed.id)
        )
        real_row = real.get_request(request_id)
        assert real_row is not None
        fake_row = fake.request(42)
        self.assertEqual(
            (
                real_row["status"],
                real_row["validation_attempts"],
                real_result.job.status,
                real.get_download_history(request_id)[0]["outcome"],
                [
                    row["username"]
                    for row in real.get_denylisted_users(request_id)
                ],
            ),
            (
                fake_row["status"],
                fake_row["validation_attempts"],
                fake_result.job.status,
                fake.download_logs[0].outcome,
                [
                    row["username"]
                    for row in fake.get_denylisted_users(42)
                ],
            ),
        )

    def test_fake_write_boundaries_and_cooldown_match_real(self):
        assert TEST_DSN is not None
        seed_db, request_id, job_id = _seed_running_import(
            cooldown_username="parity-peer",
        )
        seed_db.close()
        real = FaultInjectingPipelineDB(TEST_DSN, fail_after_write=999)
        self.addCleanup(real.close)

        class RecordingFakePipelineDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self.write_boundaries: list[str] = []

            def _terminal_outcome_write_boundary(
                self,
                index: int,
                label: str,
            ) -> None:
                del index
                self.write_boundaries.append(label)

        fake = RecordingFakePipelineDB()
        fake.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": []},
        ))
        fake_job = fake.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={
                "download_log_id": 1,
                "failed_path": "/tmp/parity-boundaries",
            },
        )
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = claim_next_import_job(fake, worker_id="parity-boundaries")
        assert fake_claimed is not None
        for _ in range(5):
            fake.log_download(
                42,
                soulseek_username="parity-peer",
                outcome="failed",
                error_message="prior source failure",
            )
        fake.set_cooldown_result(True)

        def command(owner: int, owned_job: int) -> ImportTerminalOutcome:
            return ImportTerminalOutcome(
                request_id=owner,
                import_job_id=owned_job,
                initial_transition=transitions.RequestTransition.to_wanted(
                    attempt_type="validation",
                ),
                audit=TerminalDownloadAudit(
                    outcome="rejected",
                    soulseek_username="parity-peer",
                    validation_result='{"scenario":"parity"}',
                ),
                denylists=(
                    TerminalDenylist(
                        "parity-peer",
                        "parity",
                        apply_cooldown=True,
                    ),
                ),
                job=ImportJobTerminal(
                    status="failed",
                    error="parity",
                    result={"success": False},
                    message="parity",
                ),
            )

        real.persist_import_terminal_outcome(command(request_id, job_id))
        fake.persist_import_terminal_outcome(command(42, fake_claimed.id))

        expected = [
            "request.wanted",
            "request.attempt.validation",
            "download_log",
            "denylist",
            "cooldown",
            "import_job.failed",
        ]
        self.assertEqual(real.write_boundaries, expected)
        self.assertEqual(fake.write_boundaries, expected)
        self.assertIn("parity-peer", real.get_cooled_down_users())
        self.assertIn("parity-peer", fake.user_cooldowns)

    def test_cooldown_only_command_matches_real_without_denylist(self):
        real, request_id, job_id = _seed_running_import()
        self.addCleanup(real.close)
        for _ in range(4):
            real.log_download(
                request_id,
                soulseek_username="analysis-peer",
                outcome="failed",
            )

        fake = FakePipelineDB()
        fake.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": []},
        ))
        fake_job = fake.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={
                "download_log_id": 1,
                "failed_path": "/tmp/analysis-parity",
            },
        )
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = claim_next_import_job(fake, worker_id="analysis-parity")
        assert fake_claimed is not None
        for _ in range(4):
            fake.log_download(
                42,
                soulseek_username="analysis-peer",
                outcome="failed",
            )
        fake.set_cooldown_result(True)

        def command(owner: int, owned_job: int) -> ImportTerminalOutcome:
            return ImportTerminalOutcome(
                request_id=owner,
                import_job_id=owned_job,
                initial_transition=transitions.RequestTransition.to_wanted(
                    attempt_type="validation",
                ),
                audit=TerminalDownloadAudit(
                    outcome="have_analysis_error",
                    soulseek_username="analysis-peer",
                    beets_scenario="have_analysis_error",
                    validation_result=(
                        '{"failure_category":"analyser_failure",'
                        '"error":"ffmpeg crashed"}'
                    ),
                ),
                cooldowns=(TerminalCooldown("analysis-peer"),),
                job=ImportJobTerminal(
                    status="failed",
                    error="analysis failed",
                    result={"success": False},
                    message="analysis failed",
                ),
            )

        real_result = real.persist_import_terminal_outcome(
            command(request_id, job_id)
        )
        fake_result = fake.persist_import_terminal_outcome(
            command(42, fake_claimed.id)
        )

        real_row = real.get_request(request_id)
        assert real_row is not None
        self.assertEqual(real_row["status"], fake.request(42)["status"])
        self.assertEqual(real_row["validation_attempts"], 1)
        self.assertEqual(fake.request(42)["validation_attempts"], 1)
        self.assertEqual(real_result.cooled_down_users, frozenset({"analysis-peer"}))
        self.assertEqual(fake_result.cooled_down_users, frozenset({"analysis-peer"}))
        self.assertEqual(real.get_denylisted_users(request_id), [])
        self.assertEqual(fake.get_denylisted_users(42), [])
        self.assertIn("analysis-peer", real.get_cooled_down_users())
        self.assertIn("analysis-peer", fake.get_cooled_down_users())


class TestRequestRejectionOutcomeAtomicity(unittest.TestCase):
    """Real-PostgreSQL contract for the job-less rejection bundle (issue
    #1355 item 3): a job-less rejection's request transition, audit row,
    and denylist/cooldown entries commit together or not at all — the
    job-less counterpart of ``TestTerminalOutcomeAtomicity`` above.
    """

    def _seed_wanted_request(self, db: PipelineDB) -> int:
        return db.add_request(
            mb_release_id="rejection-atomicity-mbid",
            artist_name="Rejection Atomicity Artist",
            album_title="Rejection Atomicity Album",
            source="request",
        )

    def test_bundle_commits_transition_audit_and_denylist_together(self) -> None:
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)

        result = db.persist_request_rejection_outcome(RequestRejectionOutcome(
            request_id=request_id,
            audit=TerminalDownloadAudit(
                outcome="rejected",
                soulseek_username="peer-one",
                beets_detail="high distance",
                error_message="distance too high",
                validation_result=json.dumps(
                    {"distance": 0.5, "scenario": "high_distance"}
                ),
            ),
            transition=transitions.RequestTransition.to_wanted_fields(
                attempt_type="validation", fields={},
            ),
            denylists=(
                TerminalDenylist(
                    "peer-one", "beets validation rejected", apply_cooldown=True,
                ),
            ),
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")
        self.assertEqual(request["validation_attempts"], 1)
        history = db.get_download_history(request_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["id"], result.download_log_id)
        self.assertEqual(history[0]["outcome"], "rejected")
        denied = db.get_denylisted_users(request_id)
        self.assertEqual({d["username"] for d in denied}, {"peer-one"})
        assert result.transition is not None
        self.assertEqual(result.transition.target_status, "wanted")

    def test_bundle_round_trips_explicit_beets_columns_and_contributors(
        self,
    ) -> None:
        """``_insert_nonjob_download_audit`` must forward the audit's own
        explicit ``beets_distance``/``beets_scenario``/
        ``contributor_usernames`` rather than only ever deriving them from
        ``validation_result`` or defaulting to empty — mutation testing
        (issue #1355 item 3) found dropping any of the three, or flipping
        the ``or None`` on the contributor list to ``and None``, survived
        every prior test because none set these fields explicitly."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)

        result = db.persist_request_rejection_outcome(RequestRejectionOutcome(
            request_id=request_id,
            audit=TerminalDownloadAudit(
                outcome="rejected",
                soulseek_username="peer-explicit",
                contributor_usernames=("peer-explicit", "peer-other"),
                beets_distance=0.42,
                beets_scenario="explicit_scenario",
                validation_result=json.dumps({"error": "unrelated"}),
            ),
            transition=None,
        ))

        history = db.get_download_history(request_id)
        self.assertEqual(len(history), 1)
        row = history[0]
        self.assertEqual(row["id"], result.download_log_id)
        self.assertEqual(row["beets_distance"], 0.42)
        self.assertEqual(row["beets_scenario"], "explicit_scenario")
        self.assertEqual(
            sorted(row["candidate_contributor_usernames"] or []),
            ["peer-explicit", "peer-other"],
        )

    def test_bundle_round_trips_every_audit_field(self) -> None:
        """Rule A (test-fidelity.md): every ``TerminalDownloadAudit`` field
        must be readable back through the real PostgreSQL boundary, not
        only the handful the two tests above happen to set. A dropped
        column in ``_insert_nonjob_download_audit``'s INSERT list is
        otherwise invisible until the exact field it silently nulls is
        the one an existing test checks."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)

        # Seed a prior download_log row for ``source_download_log_id`` to
        # reference — the column carries a real foreign key to another
        # ``download_log`` row (migration 052).
        prior = db.persist_request_rejection_outcome(RequestRejectionOutcome(
            request_id=request_id,
            audit=TerminalDownloadAudit(outcome="rejected"),
            transition=None,
        ))

        audit = TerminalDownloadAudit(
            outcome="rejected",
            soulseek_username="every-field-peer",
            contributor_usernames=("every-field-peer", "second-peer"),
            filetype="flac",
            download_path="/tmp/every-field/download",
            beets_distance=0.17,
            beets_scenario="every_field_scenario",
            beets_detail="every-field detail",
            valid=False,
            staged_path="/tmp/every-field/staged",
            error_message="every-field error",
            bitrate=999,
            sample_rate=44100,
            bit_depth=16,
            is_vbr=True,
            was_converted=True,
            original_filetype="mp3",
            slskd_filetype="MP3",
            actual_filetype="flac",
            actual_min_bitrate=990,
            spectral_grade="cd",
            spectral_bitrate=980,
            existing_min_bitrate=320,
            existing_spectral_bitrate=970,
            import_result=json.dumps({"every": "field"}),
            validation_result=json.dumps({"error": "no distance conflict"}),
            final_format="flac",
            v0_probe_kind="lossless_source_v0",
            v0_probe_min_bitrate=190,
            v0_probe_avg_bitrate=195,
            v0_probe_median_bitrate=192,
            existing_v0_probe_kind="on_disk_research_v0",
            existing_v0_probe_min_bitrate=180,
            existing_v0_probe_avg_bitrate=185,
            existing_v0_probe_median_bitrate=182,
            source_download_log_id=prior.download_log_id,
        )
        result = db.persist_request_rejection_outcome(RequestRejectionOutcome(
            request_id=request_id,
            audit=audit,
            transition=None,
        ))

        history = db.get_download_history(request_id)
        row = next(r for r in history if r["id"] == result.download_log_id)

        # Fields whose DB column name differs from the audit's own field
        # name, or whose value is transformed in flight (normalized,
        # derived, or read back as a parsed JSONB object).
        list_fields = {
            "contributor_usernames": "candidate_contributor_usernames",
        }
        json_fields = {"import_result", "validation_result"}
        for field in dataclasses.fields(audit):
            expected = getattr(audit, field.name)
            if field.name in list_fields:
                self.assertEqual(
                    sorted(row[list_fields[field.name]] or []),
                    sorted(expected),
                    f"field {field.name} was dropped or altered at the "
                    "PG boundary",
                )
                continue
            if field.name in json_fields:
                self.assertEqual(
                    row[field.name], json.loads(expected),
                    f"field {field.name} was dropped or altered at the "
                    "PG boundary",
                )
                continue
            self.assertEqual(
                row[field.name], expected,
                f"field {field.name} was dropped at the PG boundary",
            )

    def test_bundle_holds_row_lock_for_the_exact_request(self) -> None:
        """``_lock_terminal_request_status(command.request_id)`` must lock
        THIS request's row before the transition/audit/denylist writes —
        a mutant that passes the wrong id (or ``None``) is invisible to
        every other test here because nothing else runs concurrently
        against the same row (issue #1355 item 3)."""
        assert TEST_DSN is not None
        seed_db = make_db()
        request_id = self._seed_wanted_request(seed_db)
        seed_db.close()

        locked = threading.Event()
        release = threading.Event()
        terminal_db = PausingTerminalPipelineDB(
            TEST_DSN, locked=locked, release=release,
        )
        self.addCleanup(terminal_db.close)
        errors: list[BaseException] = []

        def run_bundle() -> None:
            try:
                terminal_db.persist_request_rejection_outcome(
                    RequestRejectionOutcome(
                        request_id=request_id,
                        audit=TerminalDownloadAudit(
                            outcome="rejected", soulseek_username="peer-lock",
                        ),
                        transition=transitions.RequestTransition.to_wanted_fields(
                            attempt_type="validation", fields={},
                        ),
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                errors.append(exc)

        thread = threading.Thread(target=run_bundle)
        thread.start()
        self.assertTrue(
            locked.wait(timeout=10),
            "terminal bundle never reached its row lock",
        )

        observer = PipelineDB(TEST_DSN)
        try:
            observer.conn.autocommit = False
            with self.assertRaises(psycopg2.errors.LockNotAvailable):
                observer._execute(
                    "SELECT status FROM album_requests "
                    "WHERE id = %s FOR UPDATE NOWAIT",
                    (request_id,),
                )
        finally:
            observer.conn.rollback()
            observer.conn.autocommit = True
            observer.close()

        release.set()
        thread.join(timeout=10)
        self.assertEqual(errors, [])

    def test_bundle_applies_an_earned_cooldown(self) -> None:
        """The same streak evaluator ``check_and_apply_cooldown`` uses,
        reached inside this bundle's own transaction (#1176-era decision
        20 follow-up), actually fires when the peer has earned it."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)
        for _ in range(5):
            db.log_download(
                request_id=request_id,
                soulseek_username="repeat-offender",
                outcome="rejected",
            )

        result = db.persist_request_rejection_outcome(RequestRejectionOutcome(
            request_id=request_id,
            audit=TerminalDownloadAudit(
                outcome="rejected", soulseek_username="repeat-offender",
            ),
            transition=transitions.RequestTransition.to_wanted_fields(
                attempt_type="validation", fields={},
            ),
            denylists=(
                TerminalDenylist(
                    "repeat-offender", "beets validation rejected",
                    apply_cooldown=True,
                ),
            ),
        ))

        self.assertEqual(result.cooled_down_users, frozenset({"repeat-offender"}))
        self.assertIn("repeat-offender", db.get_cooled_down_users())

    def test_bundle_applies_a_standalone_cooldown_with_no_denylist(self) -> None:
        """``command.cooldowns`` (used by the installed-HAVE abort lane,
        which never denylists) reaches ``_persist_terminal_cooldown``
        directly — exercised nowhere else in this class, since every other
        test earns its cooldown through a ``denylists`` entry instead."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)
        for _ in range(5):
            db.log_download(
                request_id=request_id,
                soulseek_username="cooldown-only-peer",
                outcome="rejected",
            )

        result = db.persist_request_rejection_outcome(RequestRejectionOutcome(
            request_id=request_id,
            audit=TerminalDownloadAudit(
                outcome="rejected", soulseek_username="cooldown-only-peer",
            ),
            transition=None,
            cooldowns=(TerminalCooldown("cooldown-only-peer"),),
        ))

        self.assertEqual(
            result.cooled_down_users, frozenset({"cooldown-only-peer"}),
        )
        self.assertIn("cooldown-only-peer", db.get_cooled_down_users())
        self.assertEqual(db.get_denylisted_users(request_id), [])

    def test_bundle_leaves_no_transition_when_none_is_given(self) -> None:
        """Force-import's ``requeue_on_failure=False`` shape: audit and
        denylist commit, the request's own lifecycle status is untouched."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)
        db.set_downloading(
            request_id,
            ActiveDownloadState(
                filetype="flac",
                enqueued_at="2026-07-01T00:00:00+00:00",
                files=[],
            ).to_json(),
            expected_status="wanted",
        )
        before = db.get_request(request_id)
        assert before is not None
        self.assertEqual(before["status"], "downloading")

        result = db.persist_request_rejection_outcome(RequestRejectionOutcome(
            request_id=request_id,
            audit=TerminalDownloadAudit(
                outcome="rejected", soulseek_username="force-peer",
            ),
            transition=None,
        ))

        after = db.get_request(request_id)
        assert after is not None
        self.assertEqual(after["status"], "downloading")
        self.assertIsNone(result.transition)
        self.assertEqual(len(db.get_download_history(request_id)), 1)

    def test_bundle_preserves_operator_stop_and_policy_effects(self) -> None:
        """Issue #1355 item A4: a job-less rejection against an
        ``unsearchable`` request stays ``unsearchable`` — the same
        arbitration ``persist_import_terminal_outcome`` already applies to
        the job-backed lane (mirrors
        ``TestTerminalOutcomeAtomicity.test_import_rejection_preserves_
        operator_stop_and_policy_effects``). Policy fields, the attempt
        counter, the audit row, and the denylist entry all still land;
        only the status stays put. Authority: "it should stay stopped.
        i've marked in unsearchable because slskd can't find it for some
        reason." —
        https://github.com/abl030/cratedigger/issues/1355#issuecomment-5521174387
        """
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable', "
            "min_bitrate = 320 WHERE id = %s",
            (request_id,),
        )
        db.conn.commit()

        result = db.persist_request_rejection_outcome(RequestRejectionOutcome(
            request_id=request_id,
            audit=TerminalDownloadAudit(
                outcome="rejected", soulseek_username="stopped-peer",
            ),
            transition=transitions.RequestTransition.to_wanted_fields(
                attempt_type="validation", fields={},
            ),
            denylists=(
                TerminalDenylist(
                    "stopped-peer", "beets validation rejected",
                    apply_cooldown=True,
                ),
            ),
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "unsearchable")
        self.assertEqual(request["validation_attempts"], 1)
        self.assertEqual(request["min_bitrate"], 320)
        history = db.get_download_history(request_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["outcome"], "rejected")
        self.assertEqual(
            {d["username"] for d in db.get_denylisted_users(request_id)},
            {"stopped-peer"},
        )
        assert result.transition is not None
        self.assertEqual(result.transition.target_status, "unsearchable")

    def test_bundle_rolls_back_completely_when_row_is_processing_locked(
        self,
    ) -> None:
        """A rejection that races a still-attached automation owner must
        neither transition, nor audit, nor denylist — the exact
        partial-write world issue #1355 item 3 exists to make
        unreachable."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)
        handoff_automation_owner(db, request_id)
        before = db.get_request(request_id)
        assert before is not None
        self.assertEqual(before["status"], "processing")

        command = RequestRejectionOutcome(
            request_id=request_id,
            audit=TerminalDownloadAudit(
                outcome="rejected", soulseek_username="peer-two",
            ),
            transition=transitions.RequestTransition.to_wanted_fields(
                attempt_type="validation", fields={},
            ),
            denylists=(
                TerminalDenylist("peer-two", "world failure", apply_cooldown=True),
            ),
        )

        with self.assertRaises(transitions.RequestTransitionConflict) as caught:
            db.persist_request_rejection_outcome(command)
        self.assertEqual(
            caught.exception.conflict.kind,
            transitions.TransitionConflictKind.processing_locked,
        )

        after = db.get_request(request_id)
        assert after is not None
        self.assertEqual(dict(after), dict(before))
        self.assertEqual(db.get_download_history(request_id), [])
        self.assertEqual(db.get_denylisted_users(request_id), [])

    def test_bundle_rolls_back_completely_on_mid_write_injected_failure(
        self,
    ) -> None:
        """Fault-inject a failure after each real write boundary the
        bundle crosses and prove a fresh observer sees NOTHING — not just
        a fence at the very first statement (``test_conflict_stops_
        attempt_and_audit_side_effects`` in ``tests/test_album_source.py``
        already covers that trivial case; a conflict at the first
        statement writes nothing regardless of atomicity). This is the
        genuine mid-sequence proof: the transition's own writes succeed
        internally before a LATER step fails, and the whole bundle must
        still roll back together."""
        assert TEST_DSN is not None
        expected_boundaries = (
            "request.wanted",
            "request.attempt.validation",
            "download_log",
            "denylist",
        )
        for fail_after, expected_label in enumerate(
            expected_boundaries, start=1,
        ):
            with self.subTest(boundary=expected_label):
                seed_db = make_db()
                request_id = self._seed_wanted_request(seed_db)
                seed_db.close()

                before_observer = PipelineDB(TEST_DSN)
                before = before_observer.get_request(request_id)
                assert before is not None
                before_observer.close()

                command = RequestRejectionOutcome(
                    request_id=request_id,
                    audit=TerminalDownloadAudit(
                        outcome="rejected",
                        soulseek_username="peer-three",
                        validation_result=json.dumps(
                            {"distance": 0.5, "scenario": "high_distance"}
                        ),
                    ),
                    transition=transitions.RequestTransition.to_wanted_fields(
                        attempt_type="validation", fields={},
                    ),
                    denylists=(
                        TerminalDenylist(
                            "peer-three", "world failure", apply_cooldown=True,
                        ),
                    ),
                )
                writer = FaultInjectingPipelineDB(
                    TEST_DSN, fail_after_write=fail_after,
                )
                try:
                    with self.assertRaises(InjectedTerminalWriteFailure):
                        writer.persist_request_rejection_outcome(command)
                    self.assertEqual(writer.write_boundaries[-1], expected_label)
                finally:
                    writer.close()

                observer = PipelineDB(TEST_DSN)
                try:
                    after = observer.get_request(request_id)
                    assert after is not None
                    self.assertEqual(dict(after), dict(before))
                    self.assertEqual(observer.get_download_history(request_id), [])
                    self.assertEqual(observer.get_denylisted_users(request_id), [])
                finally:
                    observer.close()

    def test_fake_write_boundaries_and_cooldown_match_real(self) -> None:
        """``FakePipelineDB.persist_request_rejection_outcome`` self-test:
        the same write-boundary sequence and cooldown decision the real
        transaction produces, over the same world."""
        assert TEST_DSN is not None
        seed_db = make_db()
        request_id = self._seed_wanted_request(seed_db)
        seed_db.close()
        real = FaultInjectingPipelineDB(TEST_DSN, fail_after_write=999)
        self.addCleanup(real.close)
        for _ in range(5):
            real.log_download(
                request_id=request_id,
                soulseek_username="parity-peer",
                outcome="failed",
                error_message="prior source failure",
            )

        class RecordingFakePipelineDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self.write_boundaries: list[str] = []

            def _terminal_outcome_write_boundary(
                self, index: int, label: str,
            ) -> None:
                del index
                self.write_boundaries.append(label)

        fake = RecordingFakePipelineDB()
        fake.seed_request(make_request_row(id=42, status="wanted"))
        for _ in range(5):
            fake.log_download(
                42,
                soulseek_username="parity-peer",
                outcome="failed",
                error_message="prior source failure",
            )
        fake.set_cooldown_result(True)

        def command(owner: int) -> RequestRejectionOutcome:
            return RequestRejectionOutcome(
                request_id=owner,
                audit=TerminalDownloadAudit(
                    outcome="rejected",
                    soulseek_username="parity-peer",
                    validation_result='{"scenario":"parity"}',
                ),
                transition=transitions.RequestTransition.to_wanted_fields(
                    attempt_type="validation", fields={},
                ),
                denylists=(
                    TerminalDenylist(
                        "parity-peer", "parity", apply_cooldown=True,
                    ),
                ),
            )

        real_result = real.persist_request_rejection_outcome(command(request_id))
        fake_result = fake.persist_request_rejection_outcome(command(42))

        expected = [
            "request.wanted",
            "request.attempt.validation",
            "download_log",
            "denylist",
            "cooldown",
        ]
        self.assertEqual(real.write_boundaries, expected)
        self.assertEqual(fake.write_boundaries, expected)
        self.assertEqual(
            real_result.cooled_down_users, frozenset({"parity-peer"}),
        )
        self.assertEqual(
            fake_result.cooled_down_users, frozenset({"parity-peer"}),
        )
        self.assertIn("parity-peer", real.get_cooled_down_users())
        self.assertIn("parity-peer", fake.user_cooldowns)


class TestRequestSuccessOutcomeAtomicity(unittest.TestCase):
    """Real-PostgreSQL contract for the job-less success bundle (issue
    #1355 item A1): a job-less acceptance's request transition and audit
    row commit together or not at all — the success counterpart of
    ``TestRequestRejectionOutcomeAtomicity`` above.
    """

    def _seed_wanted_request(self, db: PipelineDB) -> int:
        return db.add_request(
            mb_release_id="success-atomicity-mbid",
            artist_name="Success Atomicity Artist",
            album_title="Success Atomicity Album",
            source="request",
        )

    def test_bundle_commits_transition_and_audit_together(self) -> None:
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)

        result = db.persist_request_success_outcome(RequestSuccessOutcome(
            request_id=request_id,
            transition=transitions.RequestTransition.to_imported_fields(
                fields={
                    "beets_distance": 0.02,
                    "beets_scenario": "strong_match",
                },
            ),
            audit=TerminalDownloadAudit(
                outcome="success",
                soulseek_username="success-peer",
                validation_result=json.dumps(
                    {"valid": True, "distance": 0.02, "scenario": "strong_match"},
                ),
            ),
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "imported")
        self.assertEqual(request["beets_distance"], 0.02)
        history = db.get_download_history(request_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["id"], result.download_log_id)
        self.assertEqual(history[0]["outcome"], "success")
        assert result.transition is not None
        self.assertEqual(result.transition.target_status, "imported")

    def test_bundle_round_trips_every_audit_field(self) -> None:
        """Rule A (test-fidelity.md): every ``TerminalDownloadAudit`` field
        must be readable back through the real PostgreSQL boundary, not
        only the handful the pin above happens to set."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)

        prior = db.persist_request_success_outcome(RequestSuccessOutcome(
            request_id=request_id,
            transition=transitions.RequestTransition.to_imported_fields(
                fields={},
            ),
            audit=TerminalDownloadAudit(outcome="success"),
        ))

        audit = TerminalDownloadAudit(
            outcome="success",
            soulseek_username="every-field-peer",
            contributor_usernames=("every-field-peer", "second-peer"),
            filetype="flac",
            download_path="/tmp/every-field-success/download",
            beets_distance=0.03,
            beets_scenario="every_field_scenario",
            beets_detail="every-field detail",
            valid=True,
            staged_path="/tmp/every-field-success/staged",
            error_message=None,
            bitrate=999,
            sample_rate=44100,
            bit_depth=16,
            is_vbr=True,
            was_converted=True,
            original_filetype="mp3",
            slskd_filetype="MP3",
            actual_filetype="flac",
            actual_min_bitrate=990,
            spectral_grade="cd",
            spectral_bitrate=980,
            existing_min_bitrate=320,
            existing_spectral_bitrate=970,
            import_result=json.dumps({"every": "field"}),
            validation_result=json.dumps({"valid": True}),
            final_format="flac",
            v0_probe_kind="lossless_source_v0",
            v0_probe_min_bitrate=190,
            v0_probe_avg_bitrate=195,
            v0_probe_median_bitrate=192,
            existing_v0_probe_kind="on_disk_research_v0",
            existing_v0_probe_min_bitrate=180,
            existing_v0_probe_avg_bitrate=185,
            existing_v0_probe_median_bitrate=182,
            source_download_log_id=prior.download_log_id,
        )
        result = db.persist_request_success_outcome(RequestSuccessOutcome(
            request_id=request_id,
            transition=transitions.RequestTransition.to_imported_fields(
                fields={},
            ),
            audit=audit,
        ))

        history = db.get_download_history(request_id)
        row = next(r for r in history if r["id"] == result.download_log_id)

        list_fields = {
            "contributor_usernames": "candidate_contributor_usernames",
        }
        json_fields = {"import_result", "validation_result"}
        for field in dataclasses.fields(audit):
            expected = getattr(audit, field.name)
            if field.name in list_fields:
                self.assertEqual(
                    sorted(row[list_fields[field.name]] or []),
                    sorted(expected),
                    f"field {field.name} was dropped or altered at the "
                    "PG boundary",
                )
                continue
            if field.name in json_fields:
                self.assertEqual(
                    row[field.name], json.loads(expected),
                    f"field {field.name} was dropped or altered at the "
                    "PG boundary",
                )
                continue
            self.assertEqual(
                row[field.name], expected,
                f"field {field.name} was dropped at the PG boundary",
            )

    def test_bundle_supersedes_operator_stop(self) -> None:
        """Authority: "A successful exact-release terminal import
        acceptance supersedes an operator-owned `unsearchable` search
        stop and records the request as `imported`." —
        https://github.com/abl030/cratedigger/issues/737#issuecomment-5013436918
        — the job-less lane gets the same supersession the job-backed
        one already has (mirrors ``TestTerminalOutcomeAtomicity.
        test_import_terminal_acceptance_supersedes_operator_stop``)."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_wanted_request(db)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )
        db.conn.commit()

        result = db.persist_request_success_outcome(RequestSuccessOutcome(
            request_id=request_id,
            transition=transitions.RequestTransition.to_imported_fields(
                fields={"min_bitrate": 320},
            ),
            audit=TerminalDownloadAudit(outcome="success"),
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "imported")
        self.assertEqual(request["min_bitrate"], 320)
        assert result.transition is not None
        self.assertEqual(result.transition.target_status, "imported")

    def test_bundle_holds_row_lock_for_the_exact_request(self) -> None:
        """Mirrors ``TestRequestRejectionOutcomeAtomicity``'s lock proof
        for the success counterpart."""
        assert TEST_DSN is not None
        seed_db = make_db()
        request_id = self._seed_wanted_request(seed_db)
        seed_db.close()

        locked = threading.Event()
        release = threading.Event()
        terminal_db = PausingTerminalPipelineDB(
            TEST_DSN, locked=locked, release=release,
        )
        self.addCleanup(terminal_db.close)
        errors: list[BaseException] = []

        def run_bundle() -> None:
            try:
                terminal_db.persist_request_success_outcome(
                    RequestSuccessOutcome(
                        request_id=request_id,
                        transition=(
                            transitions.RequestTransition.to_imported_fields(
                                fields={},
                            )
                        ),
                        audit=TerminalDownloadAudit(
                            outcome="success", soulseek_username="peer-lock",
                        ),
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                errors.append(exc)

        thread = threading.Thread(target=run_bundle)
        thread.start()
        self.assertTrue(
            locked.wait(timeout=10),
            "terminal bundle never reached its row lock",
        )

        observer = PipelineDB(TEST_DSN)
        try:
            observer.conn.autocommit = False
            with self.assertRaises(psycopg2.errors.LockNotAvailable):
                observer._execute(
                    "SELECT status FROM album_requests "
                    "WHERE id = %s FOR UPDATE NOWAIT",
                    (request_id,),
                )
        finally:
            observer.conn.rollback()
            observer.conn.autocommit = True
            observer.close()

        release.set()
        thread.join(timeout=10)
        self.assertEqual(errors, [])

    def test_bundle_rolls_back_completely_on_mid_write_injected_failure(
        self,
    ) -> None:
        """Fault-inject a failure after each real write boundary and prove
        a fresh observer sees NOTHING — the crash-between-writes world
        that used to leave a request already transitioned to ``imported``
        with no audit row explaining why (addendum to issue #1355 item 3,
        https://github.com/abl030/cratedigger/issues/1355#issuecomment-5520032997)."""
        assert TEST_DSN is not None
        expected_boundaries = (
            "request.imported",
            "request.metadata",
            "download_log",
        )
        for fail_after, expected_label in enumerate(
            expected_boundaries, start=1,
        ):
            with self.subTest(boundary=expected_label):
                seed_db = make_db()
                request_id = self._seed_wanted_request(seed_db)
                seed_db.close()

                before_observer = PipelineDB(TEST_DSN)
                before = before_observer.get_request(request_id)
                assert before is not None
                before_observer.close()

                command = RequestSuccessOutcome(
                    request_id=request_id,
                    transition=transitions.RequestTransition.to_imported_fields(
                        fields={"beets_distance": 0.04},
                    ),
                    audit=TerminalDownloadAudit(
                        outcome="success", soulseek_username="peer-fault",
                    ),
                )
                writer = FaultInjectingPipelineDB(
                    TEST_DSN, fail_after_write=fail_after,
                )
                try:
                    with self.assertRaises(InjectedTerminalWriteFailure):
                        writer.persist_request_success_outcome(command)
                    self.assertEqual(writer.write_boundaries[-1], expected_label)
                finally:
                    writer.close()

                observer = PipelineDB(TEST_DSN)
                try:
                    after = observer.get_request(request_id)
                    assert after is not None
                    self.assertEqual(dict(after), dict(before))
                    self.assertEqual(observer.get_download_history(request_id), [])
                finally:
                    observer.close()

    def test_fake_write_boundaries_match_real(self) -> None:
        """``FakePipelineDB.persist_request_success_outcome`` self-test:
        the same write-boundary sequence the real transaction produces,
        over the same world."""
        assert TEST_DSN is not None
        seed_db = make_db()
        request_id = self._seed_wanted_request(seed_db)
        seed_db.close()
        real = FaultInjectingPipelineDB(TEST_DSN, fail_after_write=999)
        self.addCleanup(real.close)

        class RecordingFakePipelineDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self.write_boundaries: list[str] = []

            def _terminal_outcome_write_boundary(
                self, index: int, label: str,
            ) -> None:
                del index
                self.write_boundaries.append(label)

        fake = RecordingFakePipelineDB()
        fake.seed_request(make_request_row(id=42, status="wanted"))

        def command(owner: int) -> RequestSuccessOutcome:
            return RequestSuccessOutcome(
                request_id=owner,
                transition=transitions.RequestTransition.to_imported_fields(
                    fields={"beets_distance": 0.05},
                ),
                audit=TerminalDownloadAudit(
                    outcome="success", soulseek_username="parity-peer",
                ),
            )

        real_result = real.persist_request_success_outcome(command(request_id))
        fake_result = fake.persist_request_success_outcome(command(42))

        expected = ["request.imported", "request.metadata", "download_log"]
        self.assertEqual(real.write_boundaries, expected)
        self.assertEqual(fake.write_boundaries, expected)
        self.assertIsNotNone(real_result.transition)
        self.assertIsNotNone(fake_result.transition)


class TestRequestPolicyOutcomeAtomicity(unittest.TestCase):
    """Real-PostgreSQL contract for the job-less transition-plus-denylist
    bundle (issue #1355 item A2): ``lib/dispatch/quality_gate.py`` and
    ``lib/dispatch/post_import.py``'s job-less writers commit their
    transition and every denylist/cooldown entry together or not at all.
    """

    def _seed_imported_request(self, db: PipelineDB) -> int:
        request_id = db.add_request(
            mb_release_id="policy-atomicity-mbid",
            artist_name="Policy Atomicity Artist",
            album_title="Policy Atomicity Album",
            source="request",
        )
        db._execute(
            "UPDATE album_requests SET status = 'imported' WHERE id = %s",
            (request_id,),
        )
        db.conn.commit()
        return request_id

    def test_bundle_commits_transition_and_denylists_together(self) -> None:
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_imported_request(db)

        result = db.persist_request_policy_outcome(RequestPolicyOutcome(
            request_id=request_id,
            transition=transitions.RequestTransition.to_wanted(
                from_status="imported",
                search_filetype_override="lossless",
                min_bitrate=245,
            ),
            denylists=(
                TerminalDenylist("policy-peer", "quality gate: no proof"),
            ),
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")
        self.assertEqual(request["min_bitrate"], 245)
        self.assertEqual(request["search_filetype_override"], "lossless")
        denied = db.get_denylisted_users(request_id)
        self.assertEqual({d["username"] for d in denied}, {"policy-peer"})
        self.assertEqual(
            tuple(item.target_status for item in result.transitions),
            ("wanted",),
        )

    def test_bundle_preserves_operator_stop_for_non_accepting_plan(
        self,
    ) -> None:
        """The job-less quality-gate requeue plan must not clear an
        operator's ``unsearchable`` stop — the same arbitration the
        job-backed plan already gets via ``persist_import_terminal_
        outcome``'s ``post_audit_transitions``."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_imported_request(db)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable', "
            "min_bitrate = 320 WHERE id = %s",
            (request_id,),
        )
        db.conn.commit()

        result = db.persist_request_policy_outcome(RequestPolicyOutcome(
            request_id=request_id,
            transition=transitions.RequestTransition.to_wanted(
                from_status="imported",
                search_filetype_override="lossless",
                min_bitrate=245,
            ),
            denylists=(
                TerminalDenylist("stopped-policy-peer", "quality gate: no proof"),
            ),
            successful_terminal_acceptance=False,
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "unsearchable")
        self.assertEqual(request["min_bitrate"], 245)
        self.assertEqual(request["search_filetype_override"], "lossless")
        self.assertEqual(
            {d["username"] for d in db.get_denylisted_users(request_id)},
            {"stopped-policy-peer"},
        )
        self.assertEqual(
            tuple(item.target_status for item in result.transitions),
            ("unsearchable",),
        )

    def test_bundle_supersedes_operator_stop_for_accepting_plan(self) -> None:
        """A ``successful_terminal_acceptance=True`` policy plan clears an
        operator stop exactly as the job-backed bundle does — proven with
        a transition that carries no ``from_status`` fence, matching how
        ``_apply_terminal_request_transition`` itself decides (never how
        one specific caller happens to spell its own transition)."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = self._seed_imported_request(db)
        db._execute(
            "UPDATE album_requests SET status = 'unsearchable' WHERE id = %s",
            (request_id,),
        )
        db.conn.commit()

        result = db.persist_request_policy_outcome(RequestPolicyOutcome(
            request_id=request_id,
            transition=transitions.RequestTransition.to_imported(
                min_bitrate=320,
            ),
            successful_terminal_acceptance=True,
        ))

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "imported")
        self.assertEqual(request["min_bitrate"], 320)
        self.assertEqual(
            tuple(item.target_status for item in result.transitions),
            ("imported",),
        )

    def test_bundle_holds_row_lock_for_the_exact_request(self) -> None:
        """Mutant-runner finding (issue #1355 item A2): unlike its success
        and rejection siblings, this bundle had no test proving
        ``_lock_terminal_request_status`` actually locks the row before
        the transition/denylist writes — a mutant swapping that call for
        an unlocked read survived every other test in this module."""
        assert TEST_DSN is not None
        seed_db = make_db()
        request_id = self._seed_imported_request(seed_db)
        seed_db.close()

        locked = threading.Event()
        release = threading.Event()
        terminal_db = PausingTerminalPipelineDB(
            TEST_DSN, locked=locked, release=release,
        )
        self.addCleanup(terminal_db.close)
        errors: list[BaseException] = []

        def run_bundle() -> None:
            try:
                terminal_db.persist_request_policy_outcome(
                    RequestPolicyOutcome(
                        request_id=request_id,
                        transition=transitions.RequestTransition.to_wanted(
                            from_status="imported",
                        ),
                        denylists=(
                            TerminalDenylist("lock-policy-peer", "test reason"),
                        ),
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                errors.append(exc)

        thread = threading.Thread(target=run_bundle)
        thread.start()
        self.assertTrue(
            locked.wait(timeout=10),
            "terminal bundle never reached its row lock",
        )

        observer = PipelineDB(TEST_DSN)
        try:
            observer.conn.autocommit = False
            with self.assertRaises(psycopg2.errors.LockNotAvailable):
                observer._execute(
                    "SELECT status FROM album_requests "
                    "WHERE id = %s FOR UPDATE NOWAIT",
                    (request_id,),
                )
        finally:
            observer.conn.rollback()
            observer.conn.autocommit = True
            observer.close()

        release.set()
        thread.join(timeout=10)
        self.assertEqual(errors, [])

    def test_bundle_rolls_back_completely_on_mid_write_injected_failure(
        self,
    ) -> None:
        assert TEST_DSN is not None
        expected_boundaries = ("request.wanted", "denylist")
        for fail_after, expected_label in enumerate(
            expected_boundaries, start=1,
        ):
            with self.subTest(boundary=expected_label):
                seed_db = make_db()
                request_id = seed_db.add_request(
                    mb_release_id="policy-fault-mbid",
                    artist_name="Policy Fault Artist",
                    album_title="Policy Fault Album",
                    source="request",
                )
                seed_db.close()

                before_observer = PipelineDB(TEST_DSN)
                before = before_observer.get_request(request_id)
                assert before is not None
                before_observer.close()

                command = RequestPolicyOutcome(
                    request_id=request_id,
                    transition=transitions.RequestTransition.to_wanted_fields(
                        attempt_type=None, fields={"min_bitrate": 200},
                    ),
                    denylists=(
                        TerminalDenylist("policy-fault-peer", "fault test"),
                    ),
                )
                writer = FaultInjectingPipelineDB(
                    TEST_DSN, fail_after_write=fail_after,
                )
                try:
                    with self.assertRaises(InjectedTerminalWriteFailure):
                        writer.persist_request_policy_outcome(command)
                    self.assertEqual(writer.write_boundaries[-1], expected_label)
                finally:
                    writer.close()

                observer = PipelineDB(TEST_DSN)
                try:
                    after = observer.get_request(request_id)
                    assert after is not None
                    self.assertEqual(dict(after), dict(before))
                    self.assertEqual(observer.get_denylisted_users(request_id), [])
                finally:
                    observer.close()

    def test_fake_write_boundaries_match_real(self) -> None:
        """``FakePipelineDB.persist_request_policy_outcome`` self-test."""
        assert TEST_DSN is not None
        seed_db = make_db()
        request_id = seed_db.add_request(
            mb_release_id="policy-parity-mbid",
            artist_name="Policy Parity Artist",
            album_title="Policy Parity Album",
            source="request",
        )
        seed_db.close()
        real = FaultInjectingPipelineDB(TEST_DSN, fail_after_write=999)
        self.addCleanup(real.close)

        class RecordingFakePipelineDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self.write_boundaries: list[str] = []

            def _terminal_outcome_write_boundary(
                self, index: int, label: str,
            ) -> None:
                del index
                self.write_boundaries.append(label)

        fake = RecordingFakePipelineDB()
        fake.seed_request(make_request_row(id=42, status="wanted"))

        def command(owner: int) -> RequestPolicyOutcome:
            return RequestPolicyOutcome(
                request_id=owner,
                transition=transitions.RequestTransition.to_wanted_fields(
                    attempt_type=None, fields={"min_bitrate": 200},
                ),
                denylists=(
                    TerminalDenylist("policy-parity-peer", "parity test"),
                ),
            )

        real_result = real.persist_request_policy_outcome(command(request_id))
        fake_result = fake.persist_request_policy_outcome(command(42))

        expected = ["request.wanted", "denylist"]
        self.assertEqual(real.write_boundaries, expected)
        self.assertEqual(fake.write_boundaries, expected)
        self.assertEqual(
            tuple(item.target_status for item in real_result.transitions),
            tuple(item.target_status for item in fake_result.transitions),
        )


if __name__ == "__main__":
    unittest.main()
