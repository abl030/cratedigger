"""Real-PostgreSQL contracts for terminal import/preview outcome atomicity."""

from __future__ import annotations

import unittest

from lib import transitions
from lib.import_queue import IMPORT_JOB_AUTOMATION
from lib.pipeline_db import PipelineDB
from lib.terminal_outcomes import (
    ImportJobTerminal,
    ImportTerminalOutcome,
    PreviewTerminalOutcome,
    TerminalDenylist,
    TerminalDownloadAudit,
)
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


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


def _snapshot(db: PipelineDB, request_id: int, job_id: int) -> dict[str, object]:
    request_cur = db._execute(
        """
        SELECT status, active_download_state, validation_attempts,
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


def _seed_running_import(*, unfindable: bool = False) -> tuple[PipelineDB, int, int]:
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
    db._execute(
        "UPDATE album_requests SET status = 'downloading', "
        "active_download_state = '{}'::jsonb WHERE id = %s",
        (request_id,),
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
                        username="atomic-peer",
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
            "import_job.completed",
        )
        self._assert_rolls_back_at_every_boundary(
            seed=lambda: _seed_running_import(unfindable=True),
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


if __name__ == "__main__":
    unittest.main()
