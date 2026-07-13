"""Terminal import/preview outcome transaction contracts (CD-SEC-17)."""

from __future__ import annotations

import copy
import os
import sys
import threading
import time
import unittest
from unittest.mock import patch
from typing import Any, cast

import msgspec

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401  -- starts the ephemeral PostgreSQL fixture

from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    IMPORT_JOB_YOUTUBE,
    automation_import_payload,
    force_import_payload,
    manual_import_payload,
    youtube_import_payload,
)
from lib.dispatch import DispatchOutcome
from scripts import importer
from lib.pipeline_db import PipelineDB
from lib.terminal_outcomes import (
    DenylistWrite,
    DownloadAuditWrite,
    ImportJobOutcomeResult,
    ImportedRequestWrite,
    ImportSuccessOutcome,
    ImporterRejectionOutcome,
    PreviewMeasurementFailureOutcome,
    TerminalOutcomeBoundary,
    TerminalOutcomeConflict,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


TEST_DSN = os.environ.get("TEST_DB_DSN")


def _job_result(*, success: bool, message: str) -> ImportJobOutcomeResult:
    return ImportJobOutcomeResult(
        success=success,
        message=message,
        deferred=False,
        code=None,
    )


def _audit(outcome: str = "success") -> DownloadAuditWrite:
    return DownloadAuditWrite(
        outcome=outcome,
        soulseek_username="alice",
        filetype="flac",
        staged_path="/Incoming/Artist - Album",
        beets_distance=0.03,
        beets_scenario="strong_match",
        beets_detail="exact pressing",
        validation_result_json=(
            '{"valid":true,"distance":0.03,"scenario":"strong_match"}'
        ),
    )


def _success(request_id: int, job_id: int | None) -> ImportSuccessOutcome:
    return ImportSuccessOutcome(
        request_id=request_id,
        import_job_id=job_id,
        request=ImportedRequestWrite(
            beets_distance=0.03,
            beets_scenario="strong_match",
            imported_path="/Music/Artist/Album",
            verified_lossless=True,
            final_format="flac",
            write_spectral=True,
            last_download_spectral_grade="genuine",
            last_download_spectral_bitrate=930,
            current_spectral_grade="genuine",
            current_spectral_bitrate=930,
            write_v0_probe=True,
            current_lossless_source_v0_probe_min_bitrate=238,
            current_lossless_source_v0_probe_avg_bitrate=245,
            current_lossless_source_v0_probe_median_bitrate=244,
            write_quality_delta=True,
            prev_min_bitrate=320,
            min_bitrate=930,
        ),
        audit=_audit(),
        job_result=_job_result(success=True, message="Import successful"),
        job_message="Import successful",
        denylist=(DenylistWrite(username="alice", reason="source imported"),),
    )


def _rejection(request_id: int, job_id: int | None) -> ImporterRejectionOutcome:
    return ImporterRejectionOutcome(
        request_id=request_id,
        import_job_id=job_id,
        requeue_to_wanted=True,
        record_validation_attempt=True,
        write_search_filetype_override=True,
        search_filetype_override="flac",
        audit=_audit("rejected"),
        job_result=_job_result(success=False, message="Rejected: downgrade"),
        job_error="Rejected: downgrade",
        job_message="Rejected: downgrade",
        denylist=(
            DenylistWrite(username="alice", reason="quality downgrade prevented"),
            DenylistWrite(username="bob", reason="quality downgrade prevented"),
        ),
    )


def _preview_failure(request_id: int, job_id: int) -> PreviewMeasurementFailureOutcome:
    return PreviewMeasurementFailureOutcome(
        request_id=request_id,
        import_job_id=job_id,
        preview_status="measurement_failed",
        preview_result_json=(
            '{"mode":"path","verdict":"measurement_failed",'
            '"reason":"snapshot_stale"}'
        ),
        preview_error="snapshot_stale",
        preview_message="Preview measurement failed: snapshot_stale",
        validation_result_json=(
            '{"reason":"snapshot_stale","detail":"snapshot changed",'
            '"source_path":"/Incoming/Artist - Album"}'
        ),
        import_result_json=None,
        staged_path="/Incoming/Artist - Album",
        detail="snapshot changed",
        denylist=(DenylistWrite(username="alice", reason="bad source"),),
    )


def _snapshot_fake(db: FakePipelineDB, request_id: int, job_id: int) -> object:
    return copy.deepcopy((
        db.get_request(request_id),
        db.get_import_job(job_id),
        db.download_logs,
        db.denylist,
    ))


def _prepare_fake_import_job(job_type: str) -> tuple[FakePipelineDB, int, int]:
    db = FakePipelineDB()
    request_id = 42
    status = "manual" if job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_MANUAL) else "downloading"
    db.seed_request(make_request_row(
        id=request_id,
        status=status,
        unfindable_category="no_album_results",
    ))
    if job_type == IMPORT_JOB_FORCE:
        payload = force_import_payload(download_log_id=7, failed_path="/tmp/failed")
    elif job_type == IMPORT_JOB_MANUAL:
        payload = manual_import_payload(failed_path="/tmp/manual")
    elif job_type == IMPORT_JOB_YOUTUBE:
        payload = youtube_import_payload(
            staged_path="/tmp/youtube",
            request_id=request_id,
            browse_id="MPREb_test",
            download_log_id=8,
        )
    else:
        payload = automation_import_payload()
    job = db.enqueue_import_job(job_type, request_id=request_id, payload=payload)
    db.mark_import_job_preview_importable(job.id, preview_result={"verdict": "evidence_ready"})
    claimed = db.claim_next_import_job(worker_id="importer")
    assert claimed is not None
    return db, request_id, claimed.id


class TestTerminalOutcomeFakeContracts(unittest.TestCase):
    def test_import_success_persists_rescue_audit_denylist_and_job_together(self) -> None:
        db, request_id, job_id = _prepare_fake_import_job(IMPORT_JOB_AUTOMATION)

        persisted = db.persist_import_success(_success(request_id, job_id))

        row = db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        self.assertEqual(row["prior_unfindable_category"], "no_album_results")
        self.assertIsNotNone(row["rescued_at"])
        self.assertEqual(row["min_bitrate"], 930)
        self.assertEqual(row["prev_min_bitrate"], 320)
        self.assertEqual(persisted.download_log_id, db.download_logs[0].id)
        self.assertEqual(db.download_logs[0].outcome, "success")
        self.assertEqual(db.get_import_job(job_id).status, "completed")  # type: ignore[union-attr]
        self.assertEqual([entry.username for entry in db.denylist], ["alice"])

    def test_success_supports_every_import_job_type(self) -> None:
        for job_type in (
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_MANUAL,
            IMPORT_JOB_YOUTUBE,
        ):
            with self.subTest(job_type=job_type):
                db, request_id, job_id = _prepare_fake_import_job(job_type)
                db.persist_import_success(_success(request_id, job_id))
                self.assertEqual(db.get_import_job(job_id).status, "completed")  # type: ignore[union-attr]

    def test_importer_does_not_terminalize_a_domain_terminal_job_twice(self) -> None:
        db, request_id, job_id = _prepare_fake_import_job(IMPORT_JOB_AUTOMATION)
        claimed = db.get_import_job(job_id)
        assert claimed is not None
        original_mark_failed = db.mark_import_job_failed

        def execute_and_persist(*_args: object, **_kwargs: object) -> DispatchOutcome:
            db.persist_importer_rejection(_rejection(request_id, job_id))
            return DispatchOutcome(success=False, message="Rejected: downgrade")

        with (
            patch.object(importer, "execute_import_job", side_effect=execute_and_persist),
            patch.object(
                db,
                "mark_import_job_failed",
                wraps=original_mark_failed,
            ) as mark_failed,
        ):
            persisted = importer.process_claimed_job(cast(Any, db), claimed)

        assert persisted is not None
        self.assertEqual(persisted.status, "failed")
        self.assertEqual(mark_failed.call_count, 1)

    def test_post_terminal_supplement_failure_preserves_committed_job(self) -> None:
        db, request_id, job_id = _prepare_fake_import_job(IMPORT_JOB_FORCE)
        claimed = db.get_import_job(job_id)
        assert claimed is not None

        def execute_and_persist(*_args: object, **_kwargs: object) -> DispatchOutcome:
            db.persist_import_success(_success(request_id, job_id))
            return DispatchOutcome(success=True, message="Import successful")

        with (
            patch.object(importer, "execute_import_job", side_effect=execute_and_persist),
            patch.object(
                db,
                "supplement_terminal_import_job_result",
                side_effect=RuntimeError("supplement unavailable"),
            ),
        ):
            persisted = importer.process_claimed_job(cast(Any, db), claimed)

        assert persisted is not None
        self.assertEqual(persisted.status, "completed")
        self.assertEqual(len(db.download_logs), 1)

    def test_rejection_persists_requeue_attempt_audit_denylist_and_job_together(self) -> None:
        db, request_id, job_id = _prepare_fake_import_job(IMPORT_JOB_AUTOMATION)

        db.persist_importer_rejection(_rejection(request_id, job_id))

        row = db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["validation_attempts"], 1)
        self.assertEqual(row["search_filetype_override"], "flac")
        self.assertEqual(db.download_logs[0].outcome, "rejected")
        self.assertEqual(db.get_import_job(job_id).status, "failed")  # type: ignore[union-attr]
        self.assertEqual({entry.username for entry in db.denylist}, {"alice", "bob"})

    def test_validation_json_owns_terminal_audit_projection(self) -> None:
        db, request_id, job_id = _prepare_fake_import_job(IMPORT_JOB_AUTOMATION)
        outcome = _rejection(request_id, job_id)
        outcome = msgspec.structs.replace(
            outcome,
            audit=msgspec.structs.replace(
                outcome.audit,
                beets_distance=0.99,
                beets_scenario="wrong_top_level_value",
            ),
        )

        db.persist_importer_rejection(outcome)

        self.assertEqual(db.download_logs[0].beets_distance, 0.03)
        self.assertEqual(db.download_logs[0].beets_scenario, "strong_match")

    def test_preview_failure_persists_all_domain_effects_together(self) -> None:
        db = FakePipelineDB()
        request_id = 42
        db.seed_request(make_request_row(id=request_id, status="downloading"))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=request_id,
            payload=automation_import_payload(),
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        db.persist_preview_measurement_failure(
            _preview_failure(request_id, claimed.id)
        )

        self.assertEqual(db.get_request(request_id)["status"], "wanted")  # type: ignore[index]
        failed = db.get_import_job(job.id)
        assert failed is not None
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.preview_status, "measurement_failed")
        self.assertEqual(db.download_logs[0].outcome, "measurement_failed")
        self.assertEqual([entry.username for entry in db.denylist], ["alice"])

    def test_replaced_request_rejects_every_terminal_bundle_without_mutation(self) -> None:
        db, request_id, job_id = _prepare_fake_import_job(IMPORT_JOB_AUTOMATION)
        db.request(request_id)["status"] = "replaced"
        before = _snapshot_fake(db, request_id, job_id)
        for operation in (
            lambda: db.persist_import_success(_success(request_id, job_id)),
            lambda: db.persist_importer_rejection(_rejection(request_id, job_id)),
        ):
            with self.assertRaises(TerminalOutcomeConflict):
                operation()
            self.assertEqual(_snapshot_fake(db, request_id, job_id), before)

    def test_orphan_preview_request_fails_closed_without_partial_job_write(self) -> None:
        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=None,
            payload=automation_import_payload(),
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None
        before = copy.deepcopy(db.get_import_job(job.id))

        with self.assertRaises(TerminalOutcomeConflict):
            db.persist_preview_measurement_failure(
                _preview_failure(999, claimed.id)
            )

        self.assertEqual(db.get_import_job(job.id), before)
        self.assertEqual(db.download_logs, [])


class _FaultingPipelineDB(PipelineDB):
    fail_after: TerminalOutcomeBoundary | None = None
    block_after: TerminalOutcomeBoundary | None = None
    boundary_reached: threading.Event | None = None
    boundary_release: threading.Event | None = None

    def _terminal_outcome_boundary(self, boundary: TerminalOutcomeBoundary) -> None:
        if boundary == self.block_after:
            if self.boundary_reached is not None:
                self.boundary_reached.set()
            if self.boundary_release is not None:
                self.boundary_release.wait(timeout=5)
        if boundary == self.fail_after:
            raise RuntimeError(f"injected terminal outcome failure after {boundary.value}")


class TestTerminalOutcomeRealPgAtomicity(unittest.TestCase):
    def setUp(self) -> None:
        self.db = _FaultingPipelineDB(TEST_DSN)
        for table in (
            "album_quality_evidence",
            "import_jobs",
            "source_denylist",
            "download_log",
            "album_tracks",
            "album_requests",
        ):
            self.db._execute(f"TRUNCATE {table} CASCADE")
        self.db.conn.commit()

    def tearDown(self) -> None:
        self.db.close()

    def _seed_request(self, *, status: str = "downloading") -> int:
        request_id = self.db.add_request(
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            artist_name="Artist",
            album_title="Album",
            source="request",
        )
        self.db.update_status(request_id, status, expected_status="wanted")
        self.db._execute(
            "UPDATE album_requests SET unfindable_category = "
            "'album_absent_artist_present' "
            "WHERE id = %s",
            (request_id,),
        )
        self.db.conn.commit()
        return request_id

    def _claimed_import_job(self, request_id: int) -> int:
        job = self.db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=request_id,
            payload=automation_import_payload(),
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "evidence_ready"},
        )
        claimed = self.db.claim_next_import_job(worker_id="importer")
        assert claimed is not None
        return claimed.id

    def _claimed_preview_job(self, request_id: int) -> int:
        self.db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=request_id,
            payload=automation_import_payload(),
        )
        claimed = self.db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None
        return claimed.id

    def _snapshot(self, request_id: int, job_id: int) -> tuple[object, ...]:
        return (
            self.db.get_request(request_id),
            self.db.get_import_job(job_id),
            self.db._execute(
                "SELECT * FROM download_log ORDER BY id"
            ).fetchall(),
            self.db._execute(
                "SELECT * FROM source_denylist ORDER BY username"
            ).fetchall(),
        )

    def _assert_rollback_at_every_boundary(
        self,
        *,
        build: object,
        method_name: str,
        boundaries: tuple[TerminalOutcomeBoundary, ...],
        request_id: int,
        job_id: int,
    ) -> None:
        baseline = self._snapshot(request_id, job_id)
        for boundary in boundaries:
            with self.subTest(method=method_name, boundary=boundary):
                self.db.fail_after = boundary
                with self.assertRaises(RuntimeError):
                    getattr(self.db, method_name)(build)
                self.assertEqual(self._snapshot(request_id, job_id), baseline)
        self.db.fail_after = None

    def test_success_rolls_back_at_every_write_boundary(self) -> None:
        request_id = self._seed_request()
        job_id = self._claimed_import_job(request_id)
        self._assert_rollback_at_every_boundary(
            build=_success(request_id, job_id),
            method_name="persist_import_success",
            boundaries=(
                TerminalOutcomeBoundary.request,
                TerminalOutcomeBoundary.audit,
                TerminalOutcomeBoundary.denylist,
                TerminalOutcomeBoundary.job,
            ),
            request_id=request_id,
            job_id=job_id,
        )

    def test_success_requeue_rolls_back_at_final_request_boundary(self) -> None:
        request_id = self._seed_request()
        job_id = self._claimed_import_job(request_id)
        outcome = msgspec.structs.replace(
            _success(request_id, job_id),
            requeue_after_import=True,
            requeue_search_filetype_override="flac,mp3 v0",
            requeue_min_bitrate=245,
        )
        self._assert_rollback_at_every_boundary(
            build=outcome,
            method_name="persist_import_success",
            boundaries=(TerminalOutcomeBoundary.final_request,),
            request_id=request_id,
            job_id=job_id,
        )

    def test_rejection_rolls_back_at_every_write_boundary(self) -> None:
        request_id = self._seed_request()
        job_id = self._claimed_import_job(request_id)
        self._assert_rollback_at_every_boundary(
            build=_rejection(request_id, job_id),
            method_name="persist_importer_rejection",
            boundaries=(
                TerminalOutcomeBoundary.request,
                TerminalOutcomeBoundary.audit,
                TerminalOutcomeBoundary.denylist,
                TerminalOutcomeBoundary.job,
            ),
            request_id=request_id,
            job_id=job_id,
        )

    def test_preview_failure_rolls_back_at_every_write_boundary(self) -> None:
        request_id = self._seed_request()
        job_id = self._claimed_preview_job(request_id)
        self._assert_rollback_at_every_boundary(
            build=_preview_failure(request_id, job_id),
            method_name="persist_preview_measurement_failure",
            boundaries=(
                TerminalOutcomeBoundary.request,
                TerminalOutcomeBoundary.audit,
                TerminalOutcomeBoundary.denylist,
                TerminalOutcomeBoundary.job,
            ),
            request_id=request_id,
            job_id=job_id,
        )

    def test_mandatory_audit_constraint_failure_rolls_back_request_and_job(self) -> None:
        request_id = self._seed_request()
        job_id = self._claimed_import_job(request_id)
        outcome = _success(request_id, job_id)
        outcome = msgspec.structs.replace(
            outcome,
            audit=msgspec.structs.replace(outcome.audit, outcome="not_allowed"),
        )
        before = self._snapshot(request_id, job_id)

        with self.assertRaises(Exception):
            self.db.persist_import_success(outcome)

        self.assertEqual(self._snapshot(request_id, job_id), before)

    def test_replace_that_wins_first_freezes_request_and_rejects_bundle(self) -> None:
        request_id = self._seed_request()
        job_id = self._claimed_import_job(request_id)
        descendant = self.db.supersede_request_mbid(
            request_id,
            new_mb_release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="Artist",
            new_album_title="Album",
            new_year=2000,
            new_country="AU",
            new_tracks=[],
        )
        self.assertGreater(descendant, request_id)
        frozen = self.db.get_request(request_id)

        with self.assertRaises(TerminalOutcomeConflict):
            self.db.persist_import_success(_success(request_id, job_id))

        self.assertEqual(self.db.get_request(request_id), frozen)
        self.assertEqual(self.db._execute("SELECT COUNT(*) AS n FROM download_log").fetchone()["n"], 0)

    def test_transaction_lock_serializes_replace_after_complete_bundle(self) -> None:
        request_id = self._seed_request()
        job_id = self._claimed_import_job(request_id)
        reached = threading.Event()
        release = threading.Event()
        self.db.block_after = TerminalOutcomeBoundary.request
        self.db.boundary_reached = reached
        self.db.boundary_release = release
        replacement = PipelineDB(TEST_DSN)
        errors: list[BaseException] = []

        def persist() -> None:
            try:
                self.db.persist_import_success(_success(request_id, job_id))
            except BaseException as exc:  # pragma: no cover - assertion capture
                errors.append(exc)

        def replace() -> None:
            try:
                replacement.supersede_request_mbid(
                    request_id,
                    new_mb_release_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
                    new_mb_release_group_id=None,
                    new_mb_artist_id=None,
                    new_artist_name="Artist",
                    new_album_title="Album",
                    new_year=2000,
                    new_country="AU",
                    new_tracks=[],
                )
            except BaseException as exc:  # pragma: no cover - assertion capture
                errors.append(exc)

        persist_thread = threading.Thread(target=persist)
        replace_thread = threading.Thread(target=replace)
        persist_thread.start()
        self.assertTrue(reached.wait(timeout=5))
        replace_thread.start()
        time.sleep(0.05)
        self.assertTrue(replace_thread.is_alive(), "Replace did not wait on request row lock")
        release.set()
        persist_thread.join(timeout=5)
        replace_thread.join(timeout=5)
        replacement.close()

        self.assertEqual(errors, [])
        self.assertEqual(self.db._execute("SELECT COUNT(*) AS n FROM download_log").fetchone()["n"], 1)
        self.assertEqual(self.db.get_import_job(job_id).status, "completed")  # type: ignore[union-attr]
        self.assertEqual(self.db.get_request(request_id)["status"], "replaced")  # type: ignore[index]


if __name__ == "__main__":
    unittest.main()
