"""Real-PostgreSQL contracts for terminal import/preview outcome atomicity."""

from __future__ import annotations

import threading
import unittest
from typing import Any

from lib import transitions
from lib.dispatch import DispatchOutcome
from lib.download_processing import Completed, CompletionFailed, CompletionResult
from lib.import_queue import IMPORT_JOB_AUTOMATION, ImportJob
from lib.pipeline_db import PipelineDB
from lib.quality import ActiveDownloadState
from lib.terminal_outcomes import (
    ImportJobTerminal,
    ImportTerminalOutcome,
    PreviewTerminalOutcome,
    TerminalCooldown,
    TerminalDenylist,
    TerminalDownloadAudit,
)
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres
from tests.fakes import FakePipelineDB
from tests.fakes.download import RecordingProcessAlbum
from tests.helpers import make_ctx_with_fake_db, make_request_row


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


def _snapshot(db: PipelineDB, request_id: int, job_id: int) -> dict[str, object]:
    request_cur = db._execute(
        """
        SELECT status, active_download_state, download_attempts,
               validation_attempts,
               search_filetype_override, beets_distance, beets_scenario,
               min_bitrate, prev_min_bitrate, imported_path,
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
               preview_error, preview_completed_at
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
          (SELECT COUNT(*)::int FROM user_cooldowns) AS cooldowns
        """,
        (request_id, request_id),
    )
    counts = counts_cur.fetchone()
    assert request is not None and job is not None and counts is not None
    return {
        "request": dict(request),
        "job": dict(job),
        "counts": dict(counts),
    }


def _seed_running_import(
    *,
    unfindable: bool = False,
    automation_state: bool = False,
    cooldown_username: str | None = None,
) -> tuple[PipelineDB, int, int]:
    db = make_db()
    request_id = db.add_request(
        mb_release_id="terminal-outcome",
        artist_name="Atomic",
        album_title="Outcome",
        source="request",
    )
    if unfindable:
        from datetime import datetime, timezone

        db.set_unfindable_category(
            request_id,
            category="artist_absent",
            categorised_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )
    active_state = (
        ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-07-14T00:00:00+00:00",
            files=[],
            processing_started_at="2026-07-14T00:01:00+00:00",
            current_path="/tmp/atomic-processing",
        ).to_json()
        if automation_state
        else "{}"
    )
    db._execute(
        "UPDATE album_requests SET status = 'downloading', "
        "active_download_state = %s::jsonb WHERE id = %s",
        (active_state, request_id),
    )
    if cooldown_username is not None:
        for _ in range(5):
            db.log_download(
                request_id,
                soulseek_username=cooldown_username,
                outcome="failed",
                error_message="prior source failure",
            )
    job = db.enqueue_import_job(
        IMPORT_JOB_AUTOMATION,
        request_id=request_id,
        dedupe_key=f"atomic:{request_id}",
        payload={},
    )
    db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
    claimed = db.claim_next_import_job(worker_id="atomic-test")
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
    job = db.enqueue_import_job(
        IMPORT_JOB_AUTOMATION,
        request_id=request_id,
        dedupe_key=f"preview:{request_id}",
        payload={},
    )
    claimed = db.claim_next_import_preview_job(worker_id="preview-test")
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
            imported_path="/music/Atomic/Outcome",
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

    def test_import_success_with_quality_requeue_is_all_or_none(self):
        def command(request_id: int, job_id: int) -> ImportTerminalOutcome:
            return ImportTerminalOutcome(
                request_id=request_id,
                import_job_id=job_id,
                initial_transition=transitions.RequestTransition.to_imported(
                    beets_distance=0.04,
                    beets_scenario="strong_match",
                    imported_path="/music/Atomic/Outcome",
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

    def test_preview_failure_preserves_current_operator_status(self):
        db, request_id, job_id = _seed_running_preview()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'manual' WHERE id = %s",
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
        self.assertEqual(request["status"], "manual")
        self.assertEqual(result.transitions, ())

    def test_import_rejection_preserves_operator_stop_and_policy_effects(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'manual', min_bitrate = 320 "
            "WHERE id = %s",
            (request_id,),
        )
        result = db.persist_import_terminal_outcome(ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_wanted(
                from_status="downloading",
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
        self.assertEqual(request["status"], "manual")
        self.assertEqual(request["validation_attempts"], 1)
        self.assertEqual(request["search_filetype_override"], "lossless")
        self.assertEqual(request["min_bitrate"], 245)
        self.assertEqual(request["prev_min_bitrate"], 320)
        self.assertEqual(
            tuple(item.target_status for item in result.transitions),
            ("manual",),
        )

    def test_operator_action_waiting_behind_terminal_lock_retries(self):
        assert TEST_DSN is not None
        seed, request_id, job_id = _seed_running_import()
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
                from_status="downloading",
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
            except BaseException as exc:
                terminal_errors.append(exc)

        def apply_operator_stop() -> None:
            operator_results.append(transitions.finalize_operator_request(
                operator_db,
                request_id,
                transitions.RequestTransition.to_manual(
                    from_status="downloading",
                ),
            ))

        terminal_thread = threading.Thread(target=persist_terminal)
        operator_thread = threading.Thread(target=apply_operator_stop)
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
        self.assertEqual(request["status"], "manual")
        self.assertEqual(request["validation_attempts"], 1)

    def test_same_status_operator_action_still_serializes_behind_lock(self):
        assert TEST_DSN is not None
        seed, request_id, job_id = _seed_running_import()
        seed._execute(
            "UPDATE album_requests SET status = 'manual' WHERE id = %s",
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
                from_status="manual",
                imported_path="/music/Atomic/Outcome",
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
            except BaseException as exc:
                terminal_errors.append(exc)

        def reassert_operator_stop() -> None:
            operator_results.append(transitions.finalize_operator_request(
                operator_db,
                request_id,
                transitions.RequestTransition.to_manual(
                    from_status="manual",
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
        self.assertEqual(request["status"], "manual")

    def test_import_terminal_policy_preserves_current_operator_stop(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'manual' WHERE id = %s",
            (request_id,),
        )

        result = db.persist_import_terminal_outcome(
            _searching_import_outcome(request_id, job_id)
        )

        request = db.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "manual")
        self.assertEqual(request["search_filetype_override"], "lossless")
        self.assertEqual(request["min_bitrate"], 320)
        self.assertEqual(
            tuple(item.target_status for item in result.transitions),
            ("imported", "imported", "manual"),
        )

    def test_import_terminal_policy_does_not_restore_cleared_stop(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'manual' WHERE id = %s",
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
            "UPDATE album_requests SET status = 'manual' WHERE id = %s",
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
            "UPDATE album_requests SET status = 'manual' WHERE id = %s",
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
        self.assertEqual(request["status"], "manual")

    def test_import_success_round_trip_returns_complete_bundle(self):
        db, request_id, job_id = _seed_running_import(unfindable=True)
        self.addCleanup(db.close)
        outcome = ImportTerminalOutcome(
            request_id=request_id,
            import_job_id=job_id,
            initial_transition=transitions.RequestTransition.to_imported(
                beets_distance=0.04,
                beets_scenario="strong_match",
                imported_path="/music/Atomic/Outcome",
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
        self.assertEqual(float(row["beets_distance"]), 0.04)
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
                imported_path="/music/Atomic/Outcome",
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
        self.assertAlmostEqual(float(row["original_beets_distance"]), 0.2328)

    def _run_job_backed_automation_result(
        self,
        db: PipelineDB,
        job_id: int,
        completion: CompletionResult,
    ) -> ImportJob | None:
        from scripts import importer

        job = db.get_import_job(job_id)
        assert job is not None
        process_album = RecordingProcessAlbum(outcome=completion)
        ctx = make_ctx_with_fake_db(db)

        def execute(
            owner: PipelineDB,
            claimed: ImportJob,
            *,
            ctx: Any = None,
        ) -> DispatchOutcome:
            return importer.execute_automation_import_job(
                owner,
                claimed,
                ctx=ctx,
                process_album_fn=process_album,
            )

        return importer.process_claimed_job(
            db,
            job,
            ctx=ctx,
            execute_fn=execute,
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
                    with self.assertRaises(InjectedTerminalWriteFailure):
                        self._run_job_backed_automation_result(
                            failing,
                            job_id,
                            completion,
                        )
                finally:
                    failing.close()

                observer = PipelineDB(TEST_DSN)
                try:
                    self.assertEqual(
                        _snapshot(observer, request_id, job_id),
                        before,
                    )
                finally:
                    observer.close()

    def test_stale_source_rolls_back_audit_and_job(self):
        db, request_id, job_id = _seed_running_import()
        self.addCleanup(db.close)
        db._execute(
            "UPDATE album_requests SET status = 'manual' WHERE id = %s",
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
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            payload={},
        )
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = fake.claim_next_import_job(worker_id="parity")
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
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            payload={},
        )
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = fake.claim_next_import_job(worker_id="parity-boundaries")
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
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            payload={},
        )
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = fake.claim_next_import_job(worker_id="analysis-parity")
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


if __name__ == "__main__":
    unittest.main()
