"""Tests for the shared import queue worker."""

import copy
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import replace as dataclass_replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import ANY, MagicMock, patch

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.config import CratediggerConfig
from lib.dispatch import (
    DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    DISPATCH_CODE_REQUEUE_FAILED,
    DispatchOutcome,
)
from lib.download_processing import (
    Completed,
    CompletionDeferred,
    CompletionDispatched,
    CompletionFailed,
    CompletionResult,
)
from lib.import_execution import (
    AutomationOwnerFailStop,
    CancellationToken,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
    ProcessIdentity,
)
from lib.import_preview import ImportPreviewResult
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_YOUTUBE,
    AutomationImportPayload,
    ForceImportPayload,
    ImportJob,
    YoutubeImportPayload,
    force_import_dedupe_key,
    force_import_payload,
    validate_payload,
)
from lib.pipeline_db._core import OwnerSessionLost
from lib.pipeline_db.cleanup_journal import CleanupJournalConflict
from lib.pipeline_db.terminal_outcomes import ImportJobTerminalConflict
from lib.processing_cleanup import ProcessingCleanupError
from lib.quality import (
    ActiveDownloadState,
    AudioQualityMeasurement,
    ImportResult,
    ValidationResult,
)
from lib.quality_evidence import snapshot_audio_files
from lib.staged_album import StagedAlbum
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
    hermetic_beets_config_defaults,
    make_album_quality_evidence,
    make_ctx_with_fake_db,
    make_grab_list_entry,
    make_request_row,
)

_HERMETIC_BEETS_DEFAULTS: AbstractContextManager[tuple[str, str]] | None = None
_HERMETIC_BEETS_PAIR: tuple[str, str] | None = None


def _preview_execution_lease(
    invocation_id: str = "test-preview",
) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="test-boot",
        invocation_id=invocation_id,
        systemd_unit="cratedigger-import-preview-worker.service",
        worker=ProcessIdentity(pid=7001, start_ticks=70001),
    )


def _importer_execution_lease(
    invocation_id: str = "test-importer",
) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="test-boot",
        invocation_id=invocation_id,
        systemd_unit="cratedigger-importer.service",
        worker=ProcessIdentity(pid=7002, start_ticks=70002),
    )


def _unavailable_execution_lease(
    **_kwargs: object,
) -> ExecutionLeaseSnapshot:
    raise ValueError("test run is outside systemd")


def assert_long_lived_worker_reuses_cursor(cursors: list[object]) -> None:
    if len(cursors) < 2 or any(cursor is not cursors[0] for cursor in cursors[1:]):
        raise AssertionError("long-lived worker recreated its scan cursor")


def setUpModule() -> None:
    global _HERMETIC_BEETS_DEFAULTS, _HERMETIC_BEETS_PAIR
    _HERMETIC_BEETS_DEFAULTS = hermetic_beets_config_defaults()
    _HERMETIC_BEETS_PAIR = _HERMETIC_BEETS_DEFAULTS.__enter__()


def tearDownModule() -> None:
    assert _HERMETIC_BEETS_DEFAULTS is not None
    _HERMETIC_BEETS_DEFAULTS.__exit__(None, None, None)


# Migration 021 helpers — seed evidence and wire the FK chain that
# production reads through.
def _seed_candidate_for_download_log(db, log_id: int, *, mb_release_id: str,
                                     **kwargs):
    evidence = make_album_quality_evidence(mb_release_id=mb_release_id, **kwargs)
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    db.set_download_log_candidate_evidence(log_id, persisted.id)
    return persisted


def _seed_candidate_for_import_job(db, job_id: int, *, mb_release_id: str,
                                   expected_execution_lease=None,
                                   **kwargs):
    evidence = make_album_quality_evidence(mb_release_id=mb_release_id, **kwargs)
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    if expected_execution_lease is None:
        db.set_import_job_candidate_evidence(job_id, persisted.id)
    else:
        db.set_import_job_candidate_evidence(
            job_id,
            persisted.id,
            expected_execution_lease=expected_execution_lease,
        )
    return persisted


def _seed_current_for_request(db, request_id: int, *, mb_release_id: str,
                              **kwargs):
    evidence = make_album_quality_evidence(mb_release_id=mb_release_id, **kwargs)
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    db.set_request_current_evidence(request_id, persisted.id)
    return persisted


def _make_failed_import_source() -> tuple[str, str]:
    root = tempfile.mkdtemp()
    source = os.path.join(root, "failed_imports", "Album")
    os.makedirs(source)
    return root, source


@contextmanager
def _force_preview_source():
    """Make a real configured failed-import source for worker preview tests."""
    with tempfile.TemporaryDirectory() as parent:
        download_root = os.path.join(parent, "downloads")
        source = os.path.join(download_root, "failed_imports", "Album")
        processing_dir = os.path.join(parent, "processing")
        os.makedirs(source)
        os.mkdir(processing_dir, 0o700)
        os.mkdir(os.path.join(processing_dir, "albums"), 0o700)
        os.mkdir(os.path.join(processing_dir, "preview"), 0o700)
        cfg = CratediggerConfig(
            slskd_download_dir=download_root,
            processing_dir=processing_dir,
            audio_check_mode="off",
        )
        yield source, cfg


def _force_download_log(
    db: FakePipelineDB,
    request_id: int,
    failed_path: str,
) -> int:
    """Seed the DB-owned force source; payload paths are not authority."""
    if db.get_request(request_id) is None:
        db.seed_request(make_request_row(id=request_id, status="wanted"))
    return db.log_download(
        request_id,
        outcome="rejected",
        validation_result={
            "scenario": "high_distance",
            "failed_path": failed_path,
        },
    )


class TestImportJobPayloadBoundary(unittest.TestCase):
    """The import-job row projection is the sole strict JSONB boundary."""

    def test_from_row_decodes_each_known_job_type_to_its_payload_struct(self):
        cases: list[tuple[str, dict[str, object], type[object]]] = [
            (
                IMPORT_JOB_FORCE,
                {"download_log_id": 37206, "failed_path": "/tmp/failed"},
                ForceImportPayload,
            ),
            (IMPORT_JOB_AUTOMATION, {}, AutomationImportPayload),
            (
                IMPORT_JOB_YOUTUBE,
                {
                    "staged_path": "/tmp/staged",
                    "request_id": 42,
                    "browse_id": "MPREb_boundary",
                    "download_log_id": 37207,
                },
                YoutubeImportPayload,
            ),
        ]

        for job_type, payload, payload_type in cases:
            with self.subTest(job_type=job_type):
                job = ImportJob.from_row({
                    "id": 1,
                    "job_type": job_type,
                    "status": "queued",
                    "payload": payload,
                })
                self.assertIsInstance(job.payload, payload_type)
                self.assertIsInstance(job.to_dict()["payload"], dict)
                self.assertIsInstance(job.to_json_dict()["payload"], dict)

    def test_canonical_boundaries_reject_every_malformed_payload_shape(self):
        force_valid: dict[str, object] = {
            "download_log_id": 37206,
            "failed_path": "/tmp/failed",
        }
        youtube_valid: dict[str, object] = {
            "staged_path": "/tmp/staged",
            "request_id": 42,
            "browse_id": "MPREb_boundary",
            "download_log_id": 37207,
        }
        cases: list[tuple[str, dict[str, object]]] = [
            (IMPORT_JOB_FORCE, {"failed_path": "/tmp/failed"}),
            (IMPORT_JOB_FORCE, {**force_valid, "download_log_id": None}),
            (IMPORT_JOB_FORCE, {**force_valid, "download_log_id": 0}),
            (IMPORT_JOB_FORCE, {**force_valid, "download_log_id": -1}),
            (IMPORT_JOB_FORCE, {**force_valid, "download_log_id": True}),
            (IMPORT_JOB_FORCE, {**force_valid, "download_log_id": "37206"}),
            (IMPORT_JOB_FORCE, {"download_log_id": 37206}),
            (IMPORT_JOB_FORCE, {**force_valid, "failed_path": None}),
            (IMPORT_JOB_FORCE, {**force_valid, "failed_path": ""}),
            (IMPORT_JOB_FORCE, {**force_valid, "failed_path": 1}),
            (IMPORT_JOB_FORCE, {**force_valid, "unexpected": True}),
            (IMPORT_JOB_AUTOMATION, {"unexpected": True}),
            (IMPORT_JOB_YOUTUBE, {
                key: value for key, value in youtube_valid.items()
                if key != "request_id"
            }),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "request_id": None}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "request_id": 0}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "request_id": -1}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "request_id": True}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "request_id": "42"}),
            (IMPORT_JOB_YOUTUBE, {
                key: value for key, value in youtube_valid.items()
                if key != "download_log_id"
            }),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "download_log_id": None}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "download_log_id": 0}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "download_log_id": -1}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "download_log_id": True}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "download_log_id": "37207"}),
            (IMPORT_JOB_YOUTUBE, {
                key: value for key, value in youtube_valid.items()
                if key != "staged_path"
            }),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "staged_path": None}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "staged_path": ""}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "staged_path": 1}),
            (IMPORT_JOB_YOUTUBE, {
                key: value for key, value in youtube_valid.items()
                if key != "browse_id"
            }),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "browse_id": None}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "browse_id": ""}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "browse_id": 1}),
            (IMPORT_JOB_YOUTUBE, {**youtube_valid, "unexpected": True}),
        ]

        for job_type, payload in cases:
            with self.subTest(job_type=job_type, payload=payload):
                with self.assertRaises(msgspec.ValidationError):
                    validate_payload(job_type, payload)
                with self.assertRaises(msgspec.ValidationError):
                    ImportJob.from_row({
                        "id": 1,
                        "job_type": job_type,
                        "status": "queued",
                        "payload": payload,
                    })


def assert_force_preview_authority(
    *,
    lookup_path: str,
    db_failed_path: str,
    payload_failed_path: str,
    lookup_bytes: bytes,
    expected_db_bytes: bytes,
    preview_root: str,
    preview_children: list[str],
) -> None:
    """Force evidence must see a cleaned DB-authorized private snapshot."""
    if lookup_path in {db_failed_path, payload_failed_path}:
        raise AssertionError("force front gate used an unisolated filesystem path")
    if os.path.commonpath([lookup_path, preview_root]) != preview_root:
        raise AssertionError("force front gate lookup escaped private preview")
    if lookup_bytes != expected_db_bytes:
        raise AssertionError("force front gate did not copy DB-authorized bytes")
    if preview_children:
        raise AssertionError("force front gate leaked a private preview snapshot")


class TestAutomationEvidenceReuse(unittest.TestCase):
    def test_previewed_automation_job_skips_preimport_gates(self):
        from lib.download_validation import _process_beets_validation
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
        ))

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"audio")
            job = handoff_automation_owner(
                db,
                42,
                state={
                    "filetype": "mp3",
                    "enqueued_at": "2026-07-29T00:00:00+00:00",
                    "current_path": tmpdir,
                    "files": [],
                },
                canonical_path=tmpdir,
            )
            preview_lease = _preview_execution_lease("reuse-preview")
            assert claim_next_import_preview_job(db, worker_id="preview",
            execution_lease=preview_lease,) is not None
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
                expected_execution_lease=preview_lease,
            )
            assert db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
                expected_execution_lease=preview_lease,
            ) is not None
            execution_lease = _importer_execution_lease("reuse-importer")
            claimed = claim_next_import_job(db, worker_id="importer",
            execution_lease=execution_lease,)
            assert claimed is not None and claimed.id == job.id
            cfg = CratediggerConfig(
                beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                beets_distance_threshold=0.15,
                beets_staging_dir=os.path.join(tmpdir, "staging"),
                slskd_download_dir=tmpdir,
                pipeline_db_enabled=True,
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album_data = make_grab_list_entry(
                album_id=42,
                artist="Artist",
                title="Album",
                mb_release_id="mbid-123",
                db_source="request",
                db_request_id=42,
            )
            staged_album = StagedAlbum(current_path=tmpdir, request_id=42)
            token = CancellationToken()

            with patch("lib.beets.beets_validate", return_value=ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            )), \
                 patch(
                     "lib.download_validation._handle_valid_result",
                     return_value=DispatchOutcome(True, "imported"),
                 ) as handle_valid, \
                 db._pin_owner_session(token) as owner_session_identity:
                result = _process_beets_validation(
                    album_data,
                    staged_album,
                    ctx,
                    import_job_id=claimed.id,
                    cancellation_token=token,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                )

        assert result is not None
        self.assertTrue(result.success)
        handle_valid.assert_called_once()
        self.assertEqual(
            handle_valid.call_args.kwargs["import_job_id"],
            claimed.id,
        )

    def test_action_time_candidate_drift_requeues_before_import(self):
        from lib.download_validation import _process_beets_validation
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
        ))
        job = handoff_automation_owner(db, 42)

        with tempfile.TemporaryDirectory() as tmpdir:
            track = os.path.join(tmpdir, "01 - Track.mp3")
            with open(track, "wb") as handle:
                handle.write(b"audio")
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            with open(track, "ab") as handle:
                handle.write(b" changed")
            cfg = CratediggerConfig(
                beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                beets_distance_threshold=0.15,
                beets_staging_dir=os.path.join(tmpdir, "staging"),
                slskd_download_dir=tmpdir,
                pipeline_db_enabled=True,
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album_data = make_grab_list_entry(
                album_id=42,
                artist="Artist",
                title="Album",
                mb_release_id="mbid-123",
                db_source="request",
                db_request_id=42,
            )
            staged_album = StagedAlbum(current_path=tmpdir, request_id=42)

            from lib.context import CratediggerContext
            from lib.dispatch import DispatchCoreFn, QualityGateFn
            from lib.download_validation import HandleValidFn
            from lib.grab_list import GrabListEntry
            from lib.import_evidence import CandidateEvidenceActionResult

            handle_valid_calls: list[int | None] = []

            def _record_handle_valid(
                album_data: GrabListEntry,
                bv_result: ValidationResult,
                staged_album: StagedAlbum,
                ctx: CratediggerContext,
                *,
                import_job_id: int | None = None,
                prevalidated_candidate_result: (
                    CandidateEvidenceActionResult | None
                ) = None,
                quality_gate_fn: QualityGateFn | None = None,
                dispatch_fn: DispatchCoreFn | None = None,
                cancellation_token: CancellationToken | None = None,
                execution_lease: ExecutionLeaseSnapshot | None = None,
                owner_session_identity: OwnerSessionIdentity | None = None,
            ) -> DispatchOutcome | None:
                del (
                    album_data,
                    bv_result,
                    staged_album,
                    ctx,
                    prevalidated_candidate_result,
                    quality_gate_fn,
                    dispatch_fn,
                    cancellation_token,
                    execution_lease,
                    owner_session_identity,
                )
                handle_valid_calls.append(import_job_id)
                return None

            handle_valid_recorder: HandleValidFn = _record_handle_valid

            with patch("lib.beets.beets_validate", return_value=ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            )):
                result = _process_beets_validation(
                    album_data,
                    staged_album,
                    ctx,
                    import_job_id=job.id,
                    handle_valid_fn=handle_valid_recorder,
                )

        assert result is not None
        self.assertFalse(result.success)
        self.assertIn("Candidate quality evidence unavailable", result.message)
        self.assertEqual(handle_valid_calls, [])


class TestPreviewCompletionEvidenceOwnership(unittest.TestCase):
    def _linked_force_job(self) -> tuple[str, str, FakePipelineDB, ImportJob]:
        root, source = _make_failed_import_source()
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"audio")
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="mbid-123"))
        log_id = _force_download_log(db, 42, source)
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(log_id),
            payload=force_import_payload(download_log_id=log_id, failed_path=source),
        )
        _seed_candidate_for_import_job(
            db,
            job.id,
            mb_release_id="mbid-123",
            files=snapshot_audio_files(source),
        )
        return root, source, db, job

    def test_download_log_evidence_cannot_complete_a_different_job(self):
        """#853: preview completion requires this job's exact evidence FK."""
        from scripts.import_preview_worker import _candidate_evidence_ready_for_job

        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, mb_release_id="mbid-123"))
            log_id = _force_download_log(db, 42, source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(download_log_id=log_id, failed_path=source),
            )
            _seed_candidate_for_download_log(
                db,
                log_id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(source),
            )

            ready, reason = _candidate_evidence_ready_for_job(
                db,
                job,
                ImportPreviewResult(
                    mode="path", source_path=source, verdict="evidence_ready",
                ),
            )

            self.assertFalse(ready)
            self.assertIn("did not link", reason)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_empty_candidate_fingerprint_cannot_complete_preview(self):
        """#853: a linked row still needs a non-empty exact fingerprint."""
        from scripts.import_preview_worker import _candidate_evidence_ready_for_job

        class EmptyFingerprintDB(FakePipelineDB):
            def load_album_quality_evidence_by_id(self, evidence_id):
                evidence = super().load_album_quality_evidence_by_id(evidence_id)
                return (
                    msgspec.structs.replace(evidence, snapshot_fingerprint="")
                    if evidence is not None else None
                )

        root, source, original, job = self._linked_force_job()
        db = EmptyFingerprintDB()
        db.__dict__.update(original.__dict__)
        try:
            ready, reason = _candidate_evidence_ready_for_job(
                db,
                job,
                ImportPreviewResult(
                    mode="path", source_path=source, verdict="evidence_ready",
                ),
            )
            self.assertFalse(ready)
            self.assertIn("empty snapshot fingerprint", reason)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_wrong_candidate_row_cannot_complete_preview(self):
        """#853: completion rejects a usable snapshot from another evidence row."""
        from scripts.import_preview_worker import _candidate_evidence_ready_for_job

        class WrongRowDB(FakePipelineDB):
            def load_album_quality_evidence_by_id(self, evidence_id):
                evidence = super().load_album_quality_evidence_by_id(evidence_id)
                return (
                    msgspec.structs.replace(evidence, id=(evidence.id or 0) + 1)
                    if evidence is not None else None
                )

        root, source, original, job = self._linked_force_job()
        db = WrongRowDB()
        db.__dict__.update(original.__dict__)
        try:
            ready, reason = _candidate_evidence_ready_for_job(
                db,
                job,
                ImportPreviewResult(
                    mode="path", source_path=source, verdict="evidence_ready",
                ),
            )
            self.assertFalse(ready)
            self.assertIn("does not match", reason)
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestImporterWorker(unittest.TestCase):
    def setUp(self) -> None:
        self._force_root = tempfile.mkdtemp(prefix="cratedigger-force-action-")
        downloads = os.path.join(self._force_root, "downloads")
        processing = os.path.join(self._force_root, "processing")
        os.mkdir(downloads, 0o700)
        os.mkdir(processing, 0o700)
        os.mkdir(os.path.join(processing, "albums"), 0o700)
        os.mkdir(os.path.join(processing, "preview"), 0o700)
        self._force_cfg = CratediggerConfig(
            slskd_download_dir=downloads,
            processing_dir=processing,
            audio_check_mode="off",
        )
        self._runtime_config_patch = patch(
            "lib.config.read_runtime_config", return_value=self._force_cfg,
        )
        self._runtime_config_patch.start()

    def tearDown(self) -> None:
        self._runtime_config_patch.stop()
        shutil.rmtree(self._force_root, ignore_errors=True)

    def _mark_importable(
        self,
        db: FakePipelineDB,
        job,
        *,
        preview_result: dict[str, Any] | None = None,
    ):
        payload = preview_result or {"verdict": "would_import"}
        if job.job_type == IMPORT_JOB_FORCE:
            from lib.import_preview import force_action_copy_path

            action_path = force_action_copy_path(self._force_cfg, job.id)
            raw_path = job.payload.failed_path
            if os.path.isdir(raw_path):
                shutil.copytree(raw_path, action_path)
            else:
                os.mkdir(action_path, 0o700)
            payload = {**payload, "action_path": action_path}
        if job.job_type == IMPORT_JOB_AUTOMATION:
            preview_lease = _preview_execution_lease(
                f"prepare-import-{job.id}"
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview",
            execution_lease=preview_lease,)
            assert claimed is not None and claimed.id == job.id
            updated = db.mark_import_job_preview_importable(
                job.id,
                preview_result=payload,
                message="ready",
                expected_execution_lease=preview_lease,
            )
        else:
            updated = db.mark_import_job_preview_importable(
                job.id,
                preview_result=payload,
                message="ready",
            )
        assert updated is not None
        return updated

    def _result(self, job: Any) -> dict[str, Any]:
        assert job.result is not None
        return job.result

    def _launched_force_action_job(
        self,
        db: FakePipelineDB,
        *,
        dedupe_key: str,
    ) -> tuple[ImportJob, str]:
        from lib.import_preview import force_action_copy_path

        db.seed_request(make_request_row(
            id=42,
            mb_release_id="startup-force-release",
            status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=dedupe_key,
            payload={
                "download_log_id": 1,
                "failed_path": "/tmp/startup-force-source",
            },
        )
        _seed_candidate_for_import_job(
            db,
            job.id,
            mb_release_id="startup-force-release",
        )
        self._mark_importable(db, job)
        action_path = force_action_copy_path(self._force_cfg, job.id)
        claimed = claim_next_import_job(db, worker_id="old-worker")
        assert claimed is not None
        assert db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="startup-force-release",
            source_path="/tmp/startup-force-source",
        ) is not None
        return claimed, action_path

    def test_force_stages_queued_before_automation_handoff_have_zero_effect(
        self,
    ) -> None:
        """Both force lanes re-read ownership before any stage effect."""
        from lib.import_preview import force_action_copy_path
        from scripts import import_preview_worker, importer

        for lane in ("preview", "import"):
            with self.subTest(lane=lane):
                db = FakePipelineDB()
                setattr(db, "dsn", "postgresql://fake")  # noqa: B010
                db.seed_request(make_request_row(id=42, status="wanted"))
                state = ActiveDownloadState(
                    filetype="flac",
                    enqueued_at="2026-07-29T01:00:00+00:00",
                    files=[],
                )
                self.assertTrue(db.set_downloading(
                    42,
                    state.to_json(),
                    expected_status="wanted",
                ))
                force_job = db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=42,
                    dedupe_key=f"force-before-owner:{lane}",
                    payload=force_import_payload(
                        download_log_id=7,
                        failed_path="/tmp/force-before-owner",
                    ),
                )
                if lane == "import":
                    self.assertIsNotNone(
                        db.mark_import_job_preview_importable(
                            force_job.id,
                            preview_result={"verdict": "evidence_ready"},
                            message="ready before automation handoff",
                        )
                    )
                before_job = db.get_import_job(force_job.id)
                before_evidence = copy.deepcopy(db._evidence_by_id)
                before_tree = sorted(
                    os.path.relpath(os.path.join(root, name), self._force_root)
                    for root, dirs, files in os.walk(self._force_root)
                    for name in [*dirs, *files]
                )
                dispatch = MagicMock()
                measurement = MagicMock()

                def stage_factory(
                    _dsn: str,
                    db: FakePipelineDB = db,
                    state: ActiveDownloadState = state,
                ) -> FakePipelineDB:
                    handoff = db.handoff_automation_import(
                        request_id=42,
                        expected_enqueued_at=state.enqueued_at,
                        canonical_path="/processing/albums/automation-owner",
                        message="automation wins after force queue selection",
                    )
                    self.assertTrue(handoff.committed)
                    return db

                if lane == "preview":
                    result = import_preview_worker.run_once(
                        db,
                        worker_id="force-preview",
                        runtime_config=self._force_cfg,
                        stage_db_factory=stage_factory,
                        heartbeat_db_factory=lambda _dsn, db=db: db,
                        candidate_measurement_fn=measurement,
                    )
                else:
                    result = importer.run_once(
                        db,  # pyright: ignore[reportArgumentType]
                        worker_id="force-importer",
                        stage_db_factory=stage_factory,
                        execute_fn=dispatch,
                    )

                self.assertIsNone(result)
                request = db.get_request(42)
                assert request is not None
                self.assertEqual(request["status"], "processing")
                self.assertEqual(db.get_import_job(force_job.id), before_job)
                self.assertEqual(db._evidence_by_id, before_evidence)
                self.assertEqual(
                    sorted(
                        os.path.relpath(
                            os.path.join(root, name),
                            self._force_root,
                        )
                        for root, dirs, files in os.walk(self._force_root)
                        for name in [*dirs, *files]
                    ),
                    before_tree,
                )
                self.assertFalse(os.path.exists(
                    force_action_copy_path(self._force_cfg, force_job.id),
                ))
                dispatch.assert_not_called()
                measurement.assert_not_called()

    def _log_wrong_match(
        self,
        db: FakePipelineDB,
        *,
        request_id: int = 42,
        failed_path: str,
        username: str = "alice",
    ) -> int:
        if db.get_request(request_id) is None:
            db.seed_request(make_request_row(
                id=request_id,
                status="wanted",
            ))
        db.log_download(
            request_id,
            soulseek_username=username,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "failed_path": failed_path,
            },
        )
        return db.download_logs[-1].id

    def _cleanup_preview(
        self,
        log_id: int,
        *,
        verdict: str = "confident_reject",
        cleanup_eligible: bool = True,
        reason: str = "fresh cleanup-safe reject",
    ) -> ImportPreviewResult:
        return ImportPreviewResult(
            mode="download_log",
            verdict=verdict,
            would_import=verdict == "would_import",
            confident_reject=verdict == "confident_reject",
            uncertain=verdict == "uncertain",
            cleanup_eligible=cleanup_eligible,
            decision=reason,
            reason=reason,
            download_log_id=log_id,
        )

    def _seed_cleanup_reject_evidence(
        self,
        db: FakePipelineDB,
        *,
        log_id: int,
        source_path: str,
        request_id: int = 42,
    ) -> None:
        if request_id not in db._requests:
            db.seed_request(make_request_row(
                id=request_id,
                mb_release_id="mbid-123",
                status="imported",
            ))
        else:
            row = db.get_request(request_id)
            assert row is not None
            db.seed_request({
                **row,
                "status": "imported",
            })
        _seed_candidate_for_download_log(
            db, log_id,
            mb_release_id="mbid-candidate-reject",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=128,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )
        _seed_current_for_request(
            db, request_id,
            mb_release_id="mbid-current-reject",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=245,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )

    def test_force_import_job_calls_existing_dispatch_and_completes(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        self._mark_importable(db, job)
        claimed = claim_next_import_job(db, worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            updated = importer.process_claimed_job(cast(Any, db), claimed)

        dispatch.assert_called_once_with(
            db,
            request_id=42,
            failed_path=os.path.join(
                self._force_cfg.processing_dir, "albums", f"force-action-{claimed.id}",
            ),
            source_username="alice",
            source_dirs=None,
            import_job_id=claimed.id,
            download_log_id=7,
            source_reference_path="/tmp/failed",
            cfg=self._force_cfg,
        )
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(self._result(updated)["success"], True)
        self.assertEqual(job.id, updated.id)
        self.assertFalse(os.path.exists(
            os.path.join(
                self._force_cfg.processing_dir,
                "albums",
                f"force-action-{claimed.id}",
            ),
        ))

    def test_acknowledged_force_success_dismisses_only_wrong_match_pointer(self):
        """#853: success clears review state, not the operator's raw bytes."""
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            raw_bytes = b"archival raw audio"
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(raw_bytes)
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                ),
            )
            self._mark_importable(db, job)
            action_path = os.path.join(
                self._force_cfg.processing_dir, "albums", f"force-action-{job.id}",
            )
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                execute_fn=lambda *_args, **_kwargs: DispatchOutcome(True, "imported"),
            )

            assert updated is not None
            self.assertEqual(updated.status, "completed")
            with open(os.path.join(source, "01.mp3"), "rb") as handle:
                self.assertEqual(handle.read(), raw_bytes)
            self.assertEqual(db.get_wrong_matches(), [])
            self.assertFalse(os.path.exists(action_path))
            result = self._result(updated)
            self.assertTrue(result["wrong_match_dismissal"]["success"])
            self.assertTrue(result["force_action_cleanup"]["removed"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_import_job_forwards_source_dirs(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
                source_dirs=["alice\\Artist\\Album", "alice\\Artist\\Album\\CD2"],
            ),
        )
        self._mark_importable(db, job)
        claimed = claim_next_import_job(db, worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            importer.process_claimed_job(cast(Any, db), claimed)

        dispatch.assert_called_once_with(
            db,
            request_id=42,
            failed_path=os.path.join(
                self._force_cfg.processing_dir, "albums", f"force-action-{claimed.id}",
            ),
            source_username="alice",
            source_dirs=["alice\\Artist\\Album", "alice\\Artist\\Album\\CD2"],
            import_job_id=claimed.id,
            download_log_id=7,
            source_reference_path="/tmp/failed",
            cfg=self._force_cfg,
        )

    def test_force_import_without_private_action_requeues_before_dispatch(self):
        """A force job must never fall back to the raw quarantine folder."""
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(download_log_id=7, failed_path="/tmp/raw"),
        )
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = claim_next_import_job(db, worker_id="worker")
        assert claimed is not None

        updated = importer.process_claimed_job(cast(Any, db), claimed)
        self.assertIsNone(updated)
        row = db.get_import_job(job.id)
        assert row is not None
        self.assertEqual(row.preview_status, "waiting")

    def test_force_import_job_does_not_forward_preview_import_result(self):
        from scripts import importer

        preview_ir = ImportResult(
            decision="import",
            source_measurement=AudioQualityMeasurement(min_bitrate_kbps=245),
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        self._mark_importable(
            db,
            job,
            preview_result={
                "verdict": "would_import",
                "import_result": json.loads(preview_ir.to_json()),
            },
        )
        claimed = claim_next_import_job(db, worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            importer.process_claimed_job(cast(Any, db), claimed)

        self.assertNotIn("preview_import_result", dispatch.call_args.kwargs)

    def test_force_import_job_does_not_forward_stale_preview_import_result_as_authority(self):
        from scripts import importer

        preview_ir = ImportResult(
            decision="import",
            already_in_beets=False,
            source_measurement=AudioQualityMeasurement(min_bitrate_kbps=141),
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="unsearchable",
            min_bitrate=116,
            verified_lossless=False,
            current_spectral_grade="likely_transcode",
            current_lossless_source_v0_probe_avg_bitrate=240,
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        self._mark_importable(
            db,
            job,
            preview_result={
                "verdict": "would_import",
                "import_result": json.loads(preview_ir.to_json()),
            },
        )
        claimed = claim_next_import_job(db, worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            importer.process_claimed_job(cast(Any, db), claimed)

        self.assertNotIn(
            "preview_import_result",
            dispatch.call_args.kwargs,
            "Stored preview ImportResult is audit/evidence input only; force "
            "import must recompute the action decision against current evidence.",
        )

    def test_failed_force_import_quality_pipeline_reject_preserves_raw_source(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "Rejected by persisted quality evidence: downgrade",
                    code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                ),
            ), patch(
                "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            ) as cleanup_wrong_match, patch(
                "lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence",
                side_effect=AssertionError("cleanup must not re-decide"),
            ), patch(
                "lib.quality.full_pipeline_decision_from_evidence",
                side_effect=AssertionError("cleanup must not re-decide"),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            cleanup_wrong_match.assert_not_called()
            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.exists(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            result = self._result(updated)
            self.assertEqual(result["cleanup"]["success"], True)
            self.assertEqual(
                result["cleanup"]["outcome"], "preserved_operator_force_source",
            )
            self.assertEqual(
                result["cleanup"]["dispatch_code"],
                DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_corrupt_force_action_is_reclaimed_without_protected_quarantine_copy(self):
        """#853: corrupt force bytes stay private; raw Wrong Match stays intact."""
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            raw_bytes = b"operator raw evidence"
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(raw_bytes)
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                status="unsearchable",
            ))
            db.set_tracks(42, [{"track_number": 1, "title": "One"}])
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            action_path = os.path.join(
                self._force_cfg.processing_dir, "albums", f"force-action-{job.id}",
            )
            _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(action_path),
                audio_corrupt=True,
                audio_error="01.mp3: corrupt fixture",
            )
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            with open(os.path.join(source, "01.mp3"), "rb") as handle:
                self.assertEqual(handle.read(), raw_bytes)
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertFalse(os.path.exists(action_path))
            self.assertFalse(os.path.exists(os.path.join(
                self._force_cfg.slskd_download_dir, "failed_imports", "bad_files",
            )))
            result = self._result(updated)
            self.assertNotIn("post_commit_cleanup", result)
            self.assertEqual(
                result["cleanup"]["outcome"], "preserved_operator_force_source",
            )
            self.assertTrue(result["force_action_cleanup"]["removed"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_stale_rejecting_force_evidence_requeues_before_rejection_audit(self):
        """#853: action drift invalidates even an initially corrupt reject."""
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"operator raw evidence")
            db.seed_request(make_request_row(
                id=42, mb_release_id="mbid-123", status="unsearchable",
            ))
            db.set_tracks(42, [{"track_number": 1, "title": "One"}])
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(download_log_id=log_id, failed_path=source),
            )
            self._mark_importable(db, job)
            action_path = os.path.join(
                self._force_cfg.processing_dir, "albums", f"force-action-{job.id}",
            )
            _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(action_path),
                audio_corrupt=True,
                audio_error="reject if unchanged",
            )
            with open(os.path.join(action_path, "01.mp3"), "ab") as handle:
                handle.write(b" drift")
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            updated = importer.process_claimed_job(cast(Any, db), claimed)

            self.assertIsNone(updated)
            row = db.get_import_job(job.id)
            assert row is not None
            self.assertEqual(row.status, "queued")
            self.assertEqual(row.preview_status, "waiting")
            self.assertEqual(len(db.download_logs), 1)
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertTrue(os.path.isdir(action_path))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_action_manifest_drift_requeues_before_terminal_audit(self):
        """Add/remove/rename drift is stale evidence, never a force reject."""
        from scripts import importer

        def remove(path: str) -> None:
            os.unlink(os.path.join(path, "01.mp3"))

        def add(path: str) -> None:
            with open(os.path.join(path, "bonus.mp3"), "wb") as handle:
                handle.write(b"unexpected audio")

        def rename(path: str) -> None:
            os.rename(os.path.join(path, "01.mp3"), os.path.join(path, "02.mp3"))

        for drift, mutate in (("remove", remove), ("add", add), ("rename", rename)):
            with self.subTest(drift=drift):
                db = FakePipelineDB()
                root, source = _make_failed_import_source()
                try:
                    raw_bytes = b"operator raw evidence"
                    with open(os.path.join(source, "01.mp3"), "wb") as handle:
                        handle.write(raw_bytes)
                    db.seed_request(make_request_row(
                        id=42, mb_release_id="mbid-123", status="unsearchable",
                    ))
                    db.set_tracks(42, [{"track_number": 1, "title": "One"}])
                    log_id = self._log_wrong_match(db, failed_path=source)
                    job = db.enqueue_import_job(
                        IMPORT_JOB_FORCE,
                        request_id=42,
                        dedupe_key=force_import_dedupe_key(log_id),
                        payload=force_import_payload(
                            download_log_id=log_id, failed_path=source,
                        ),
                    )
                    action_path = os.path.join(
                        self._force_cfg.processing_dir,
                        "albums",
                        f"force-action-{job.id}",
                    )
                    shutil.rmtree(action_path, ignore_errors=True)
                    self._mark_importable(db, job)
                    _seed_candidate_for_import_job(
                        db,
                        job.id,
                        mb_release_id="mbid-123",
                        files=snapshot_audio_files(action_path),
                    )
                    mutate(action_path)
                    claimed = claim_next_import_job(db, worker_id="worker")
                    assert claimed is not None

                    updated = importer.process_claimed_job(cast(Any, db), claimed)

                    self.assertIsNone(updated)
                    row = db.get_import_job(job.id)
                    assert row is not None
                    self.assertEqual(row.status, "queued")
                    self.assertEqual(row.preview_status, "waiting")
                    self.assertEqual(len(db.download_logs), 1)
                    self.assertEqual(len(db.get_wrong_matches()), 1)
                    self.assertTrue(os.path.isdir(action_path))
                    with open(os.path.join(source, "01.mp3"), "rb") as handle:
                        self.assertEqual(handle.read(), raw_bytes)
                finally:
                    shutil.rmtree(root, ignore_errors=True)

    def test_failed_force_import_non_pipeline_failure_preserves_wrong_match(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                status="unsearchable",
            ))
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(
                db,
                job,
                preview_result={
                    "verdict": "confident_reject",
                    "confident_reject": True,
                    "cleanup_eligible": True,
                },
            )
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(False, "beets failed"),
            ), patch(
                "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            ) as cleanup_wrong_match:
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            cleanup_wrong_match.assert_not_called()
            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            cleanup = self._result(updated)["cleanup"]
            self.assertTrue(cleanup["skipped"])
            self.assertEqual(
                cleanup["outcome"],
                "preserved_operator_force_source",
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_import_extra_audio_keeps_wm_and_operator_status_end_to_end(self):
        """Issue #387 composition: force-importing a folder with extra audio,
        through the REAL manifest guard (no mocked dispatch).

        Proves the two halves compose: the guard preserves the operator-owned
        ``unsearchable`` status AND its audit row does NOT inflate Wrong Matches,
        while the importer preserves the original WM entry for review (the
        ``IMPORT_MANIFEST_REJECTED`` code skips cleanup). beets never runs —
        the guard rejects upstream of it.
        """
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            with open(os.path.join(source, "bonus.mp3"), "wb") as f:
                f.write(b"audio")
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                status="unsearchable",
            ))
            # One expected track but two audio files on disk → extra audio.
            db.set_tracks(42, [{"track_number": 1, "title": "One"}])
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            action_path = os.path.join(
                self._force_cfg.processing_dir, "albums", f"force-action-{job.id}",
            )
            _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(action_path),
            )
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            # No dispatch patch — the real guard runs. beets is never reached.
            updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertEqual(db.request(42)["status"], "unsearchable")
            self.assertEqual(db.request(42)["validation_attempts"], 0)
            # The dead WM entry is preserved (extra audio → operator review),
            # and the audit row did NOT create a second entry.
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertTrue(os.path.isdir(source))
            outcomes = [
                (log.outcome, log.beets_scenario) for log in db.download_logs
            ]
            self.assertIn(("rejected", "untracked_audio"), outcomes)
            cleanup = self._result(updated)["cleanup"]
            self.assertTrue(cleanup["skipped"])
            self.assertEqual(len(db.denylist), 0)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_import_undercount_preserves_folder_and_operator_status_end_to_end(self):
        """Issue #387 regression: force-importing an UNDER-COUNT folder (fewer
        audio files than the request's track rows, no extras) must NOT delete
        the operator's partial audio.

        An under-count source physically contains audio the operator chose to
        import — it is not 'nothing to inspect'. The guard preserves the
        operator-owned status and returns ``IMPORT_MANIFEST_REJECTED``
        so the importer PRESERVES the folder (``_cleanup_failed_force_import``
        skips deletion on that code). Routing it through
        ``QUALITY_PIPELINE_REJECTED`` would ``shutil.rmtree`` the only
        surviving copy — the exact irreversible auto-decision the archivist
        frame forbids. Wrong-match deletion is reserved for the genuinely
        empty (0-file) case, which routes through the evidence pipeline, not
        this guard.
        """
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            # One disc on disk; the request expects two tracks → under-count.
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                status="unsearchable",
            ))
            db.set_tracks(42, [
                {"track_number": 1, "title": "One"},
                {"track_number": 2, "title": "Two"},
            ])
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            action_path = os.path.join(
                self._force_cfg.processing_dir, "albums", f"force-action-{job.id}",
            )
            _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(action_path),
            )
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            # THE regression assertion: the operator's partial audio survives.
            self.assertTrue(
                os.path.isdir(source),
                "under-count force-import must NOT delete the operator's folder")
            self.assertTrue(os.path.isfile(os.path.join(source, "01.mp3")))
            # The operator-owned request state is not this guard's to clear.
            self.assertEqual(db.request(42)["status"], "unsearchable")
            self.assertEqual(db.request(42)["validation_attempts"], 0)
            # WM entry preserved (something to inspect), no duplicate.
            self.assertEqual(len(db.get_wrong_matches()), 1)
            outcomes = [
                (log.outcome, log.beets_scenario) for log in db.download_logs
            ]
            self.assertIn(("rejected", "incomplete_fileset"), outcomes)
            cleanup = self._result(updated)["cleanup"]
            self.assertTrue(cleanup["skipped"])
            self.assertEqual(len(db.denylist), 0)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_import_requeue_leaves_next_preview_action_copy_intact(self):
        """A requeued importer cannot reclaim an action a new preview owns."""
        from lib.dispatch import DISPATCH_CODE_REQUEUED_FOR_PREVIEW
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            from lib.import_preview import force_action_copy_path

            action_path = force_action_copy_path(self._force_cfg, job.id)
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None
            claimed_attempts = claimed.attempts

            def fake_dispatch(*_args, **_kwargs):
                # The requeue releases the action path to the next preview.
                db.requeue_import_job_for_preview(
                    job.id,
                    reason="candidate evidence missing",
                )
                # Interleave the new preview publishing its fresh private
                # action copy before this old importer frame returns.
                shutil.rmtree(action_path)
                os.mkdir(action_path, 0o700)
                with open(os.path.join(action_path, "01.mp3"), "wb") as handle:
                    handle.write(b"fresh preview action")
                db.mark_import_job_preview_importable(
                    job.id,
                    preview_result={
                        "verdict": "evidence_ready",
                        "action_path": action_path,
                    },
                    message="fresh preview ready",
                )
                return DispatchOutcome(
                    False,
                    "Candidate evidence unavailable; requeued for preview",
                    code=DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
                )

            with patch(
                "lib.dispatch.dispatch_import_from_db",
                side_effect=fake_dispatch,
            ), patch(
                "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            ) as cleanup:
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            # Importer must NOT have written a terminal status.
            cleanup.assert_not_called()
            self.assertTrue(os.path.isdir(source))
            # The fresh preview has republished the job's action copy.
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["preview_status"], "evidence_ready")
            with open(os.path.join(action_path, "01.mp3"), "rb") as handle:
                self.assertEqual(handle.read(), b"fresh preview action")
            # Importer did not retry-count: row attempts not bumped beyond
            # the original claim.
            self.assertEqual(row["attempts"], claimed_attempts)
            # process_claimed_job returns the job ImportJob (current state),
            # not a terminal failure. The job should not be in 'failed'.
            if updated is not None:
                self.assertNotEqual(updated.status, "failed")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_import_requeue_failed_marks_job_failed(self):
        """REL-001: when dispatch returns DISPATCH_CODE_REQUEUE_FAILED (its
        requeue UPDATE itself raised), the importer must mark the job
        terminally failed rather than leaving it running. Leaving the job
        running would let startup recovery revisit it on next worker boot
        reclaim it — but the importer's claim query still matches
        preview_status='evidence_ready', so it would re-claim, hit the same
        requeue condition, fail again, and spin forever. Failing terminally
        surfaces the issue to ops; the operator re-triggers once the DB
        problem is resolved.
        """
        from lib.dispatch import DISPATCH_CODE_REQUEUE_FAILED
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "requeue UPDATE failed: boom",
                    code=DISPATCH_CODE_REQUEUE_FAILED,
                ),
            ), patch(
                "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            ) as cleanup:
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            # No wrong-match cleanup runs on the requeue-failed path (the
            # situation is a DB issue, not a quality decision).
            cleanup.assert_not_called()
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            self.assertEqual(row["status"], "failed")
            self.assertIn("requeue", row["message"])
            self.assertTrue(os.path.isdir(source))
            assert updated is not None
            self.assertEqual(updated.status, "failed")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_failed_force_import_job_clears_newer_duplicate_rejection(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source, username="old")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            def reject_again(*_args, **kwargs):
                db.log_download(
                    kwargs["request_id"],
                    soulseek_username="new",
                    outcome="rejected",
                    validation_result={
                        "scenario": "quality_downgrade",
                        "failed_path": kwargs["failed_path"],
                    },
                )
                return DispatchOutcome(
                    False,
                    "Rejected by persisted quality evidence: downgrade",
                    code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                )

            with patch(
                "lib.dispatch.dispatch_import_from_db",
                side_effect=reject_again,
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertEqual(
                self._result(updated)["cleanup"]["outcome"],
                "preserved_operator_force_source",
            )
            self.assertEqual(len(db.get_wrong_matches()), 2)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_failed_force_import_quality_reject_skips_cleanup_for_other_active_job(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key="force_import:other-active-job",
                payload={"download_log_id": 1, "failed_path": source},
            )

            with patch(
                "lib.dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "Rejected by persisted quality evidence: downgrade",
                    code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                ),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            cleanup = self._result(updated)["cleanup"]
            self.assertTrue(cleanup["skipped"])
            self.assertEqual(cleanup["outcome"], "preserved_operator_force_source")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_deferred_force_import_preserves_source_and_wrong_match(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                ),
            )
            self._mark_importable(db, job)
            claimed = claim_next_import_job(db, worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "Another import is already in progress",
                    deferred=True,
                ),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertNotIn("cleanup", self._result(updated))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_startup_requeues_abandoned_running_job_for_retry(self):
        from scripts import importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force_import:startup-recovery",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        self._mark_importable(db, job)
        claimed = claim_next_import_job(db, worker_id="old-worker")
        assert claimed is not None

        recovered = importer.recover_abandoned_running_jobs(db)

        self.assertEqual([job.id for job in recovered], [claimed.id])
        self.assertEqual(recovered[0].status, "queued")
        self.assertIsNone(recovered[0].worker_id)
        self.assertIsNone(recovered[0].heartbeat_at)

        with patch(
            "lib.dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported on retry"),
        ):
            updated = importer.run_once(
                cast(Any, db),
                worker_id="new-worker",
                stage_db_factory=lambda _dsn: db,
            )

        assert updated is not None
        self.assertEqual(updated.status, "completed")
        retried = db.get_import_job(claimed.id)
        assert retried is not None
        self.assertEqual(retried.attempts, 2)

    def test_startup_recovery_removes_launched_force_action_copy(self) -> None:
        """A terminal crash recovery releases the job's private album copy."""
        from scripts import importer

        db = FakePipelineDB()
        job, action_path = self._launched_force_action_job(
            db,
            dedupe_key="force_import:startup-terminal-cleanup",
        )
        self.assertTrue(os.path.isdir(action_path))

        recovered = importer.recover_abandoned_running_jobs(db)

        self.assertEqual([item.id for item in recovered], [job.id])
        self.assertEqual(recovered[0].status, "failed")
        self.assertFalse(os.path.exists(action_path))
        stored = db.get_import_job(job.id)
        assert stored is not None
        cleanup = self._result(stored)["force_action_cleanup"]
        assert isinstance(cleanup, dict)
        self.assertEqual(cleanup["action_path"], action_path)
        self.assertTrue(cleanup["removed"])

    def test_startup_retries_failed_force_action_cleanup(self) -> None:
        """A transient filesystem refusal remains a durable startup task."""
        from scripts import importer

        db = FakePipelineDB()
        job, action_path = self._launched_force_action_job(
            db,
            dedupe_key="force_import:startup-cleanup-retry",
        )
        with patch(
            "lib.config.read_runtime_config",
            side_effect=RuntimeError("temporary config refusal"),
        ):
            importer.recover_abandoned_running_jobs(db)

        failed_cleanup_job = db.get_import_job(job.id)
        assert failed_cleanup_job is not None
        cleanup = self._result(
            failed_cleanup_job
        )["force_action_cleanup"]
        assert isinstance(cleanup, dict)
        self.assertFalse(cleanup["removed"])
        self.assertTrue(os.path.isdir(action_path))

        importer.recover_abandoned_running_jobs(db)

        converged = db.get_import_job(job.id)
        assert converged is not None
        cleanup = self._result(converged)["force_action_cleanup"]
        assert isinstance(cleanup, dict)
        self.assertTrue(cleanup["removed"])
        self.assertFalse(os.path.exists(action_path))

    def test_startup_retries_force_cleanup_after_result_merge_failure(
        self,
    ) -> None:
        """A kill-equivalent missing DB receipt re-drives idempotent cleanup."""
        from scripts import importer

        db = FakePipelineDB()
        job, action_path = self._launched_force_action_job(
            db,
            dedupe_key="force_import:startup-cleanup-merge-retry",
        )
        with patch.object(
            db,
            "merge_import_job_result",
            side_effect=RuntimeError("lost cleanup receipt"),
        ):
            importer.recover_abandoned_running_jobs(db)

        self.assertFalse(os.path.exists(action_path))
        unrecorded = db.get_import_job(job.id)
        assert unrecorded is not None
        self.assertNotIn(
            "force_action_cleanup",
            unrecorded.result or {},
        )

        importer.recover_abandoned_running_jobs(db)

        converged = db.get_import_job(job.id)
        assert converged is not None
        cleanup = self._result(converged)["force_action_cleanup"]
        assert isinstance(cleanup, dict)
        self.assertTrue(cleanup["removed"])

    def test_importer_does_not_claim_job_waiting_for_preview(self):
        from scripts import importer

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force_import:waiting-preview",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )

        self.assertIsNone(importer.run_once(cast(Any, db), worker_id="worker"))

    def test_automation_job_reconstructs_active_state_and_uses_processing_path(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = handoff_automation_owner(
            db,
            42,
            state=db.request(42)["active_download_state"],
        )
        self._mark_importable(db, job)
        lease = _importer_execution_lease("reconstruct-state")
        claimed = claim_next_import_job(db, worker_id="worker",
        execution_lease=lease,)
        assert claimed is not None
        token = CancellationToken()

        with patch(
            "lib.download._run_completed_processing",
            return_value=Completed(),
        ) as processing, db._pin_owner_session(token) as owner_session_identity:
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
                execution_lease=lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )

        processing.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "Automation import processing completed")

    def test_automation_job_completes_from_dispatch_outcome(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = handoff_automation_owner(
            db,
            42,
            state=db.request(42)["active_download_state"],
        )
        self._mark_importable(db, job)
        lease = _importer_execution_lease("dispatch-success")
        claimed = claim_next_import_job(db, worker_id="worker",
        execution_lease=lease,)
        assert claimed is not None
        token = CancellationToken()

        with patch(
            "lib.download._run_completed_processing",
            return_value=CompletionDispatched(
                outcome=DispatchOutcome(True, "Imported by dispatch")),
        ) as processing, db._pin_owner_session(token) as owner_session_identity:
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
                execution_lease=lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )

        self.assertEqual(processing.call_args.kwargs["import_job_id"], job.id)
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "Imported by dispatch")
        self.assertEqual(self._result(updated)["success"], True)

    def test_automation_job_deferred_message_carries_detail(self):
        """Issue #859: a deferred completion's job message must carry the
        honest ``CompletionDeferred.detail`` — not the generic "deferred or
        requires manual recovery" string alone. The detail (e.g.
        "incomplete_or_unsafe_canonical") is the diagnostic that told the
        operator WHY the automation import kept stalling in ``downloading``.
        """
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = handoff_automation_owner(
            db,
            42,
            state=db.request(42)["active_download_state"],
        )
        self._mark_importable(db, job)
        lease = _importer_execution_lease("deferred-detail")
        claimed = claim_next_import_job(db, worker_id="worker",
        execution_lease=lease,)
        assert claimed is not None
        token = CancellationToken()

        with patch(
            "lib.download._run_completed_processing",
            return_value=CompletionDeferred(
                detail="incomplete_or_unsafe_canonical"),
        ), db._pin_owner_session(token) as owner_session_identity:
            updated = importer.process_claimed_job(
                db,  # pyright: ignore[reportArgumentType]
                claimed,
                ctx=object(),
                execution_lease=lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(
            updated.message,
            "Automation import was deferred or requires manual recovery: "
            "incomplete_or_unsafe_canonical",
        )

    def test_automation_job_failed_message_carries_reason(self):
        """Issue #865: a failed completion's job message must carry the honest
        ``CompletionFailed.reason`` — the same #859 rule that already applies
        to ``CompletionDeferred.detail``."""
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = handoff_automation_owner(
            db,
            42,
            state=db.request(42)["active_download_state"],
        )
        self._mark_importable(db, job)
        lease = _importer_execution_lease("failed-reason")
        claimed = claim_next_import_job(db, worker_id="worker",
        execution_lease=lease,)
        assert claimed is not None
        token = CancellationToken()

        with patch(
            "lib.download._run_completed_processing",
            return_value=CompletionFailed(reason="event_path_never_stamped"),
        ), db._pin_owner_session(token) as owner_session_identity:
            updated = importer.process_claimed_job(
                db,  # pyright: ignore[reportArgumentType]
                claimed,
                ctx=object(),
                execution_lease=lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(
            updated.message,
            "Automation import processing failed: event_path_never_stamped",
        )

    def test_automation_job_fails_from_dispatch_outcome(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = handoff_automation_owner(
            db,
            42,
            state=db.request(42)["active_download_state"],
        )
        self._mark_importable(db, job)
        lease = _importer_execution_lease("dispatch-failure")
        claimed = claim_next_import_job(db, worker_id="worker",
        execution_lease=lease,)
        assert claimed is not None
        token = CancellationToken()

        with patch(
            "lib.download._run_completed_processing",
            return_value=CompletionDispatched(
                outcome=DispatchOutcome(False, "Pre-import gate rejected")),
        ), db._pin_owner_session(token) as owner_session_identity:
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
                execution_lease=lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.message, "Pre-import gate rejected")
        self.assertEqual(updated.error, "Pre-import gate rejected")
        self.assertEqual(self._result(updated)["success"], False)

    def test_force_worker_forwards_pinned_session_authority(self) -> None:
        """The force worker must not drop authority before dispatch."""
        from scripts import importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:pinned-authority",
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/force-pinned-authority",
            ),
        )
        self._mark_importable(db, job)
        observed: dict[str, object] = {}

        def execute(
            _stage_db: object,
            _job: ImportJob,
            *,
            ctx: object | None = None,
            cancellation_token: CancellationToken,
            owner_session_identity: OwnerSessionIdentity,
        ) -> DispatchOutcome:
            del ctx
            observed["token"] = cancellation_token
            observed["identity"] = owner_session_identity
            return DispatchOutcome(False, "stop after authority proof")

        result = importer.run_once(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="force-authority",
            stage_db_factory=lambda _dsn: db,
            execution_lease_factory=_unavailable_execution_lease,
            execute_fn=execute,
        )

        self.assertIsNotNone(result)
        self.assertIsInstance(observed["token"], CancellationToken)
        self.assertIsInstance(
            observed["identity"],
            OwnerSessionIdentity,
        )

    def test_force_executor_forwards_authority_into_dispatch(self) -> None:
        """The executor preserves the token/session pair at dispatch."""
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:dispatch-authority",
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/force-dispatch-authority",
            ),
        )
        ready = self._mark_importable(db, job)
        claimed = claim_next_import_job(db, worker_id="force-dispatch")
        assert claimed is not None
        token = CancellationToken()
        observed: dict[str, object] = {}

        def dispatch(
            _db: object,
            **kwargs: object,
        ) -> DispatchOutcome:
            observed.update(kwargs)
            return DispatchOutcome(False, "authority recorded")

        with db._pin_owner_session(token) as identity:
            importer.execute_import_job(
                db,  # pyright: ignore[reportArgumentType]
                claimed,
                cancellation_token=token,
                owner_session_identity=identity,
                force_dispatch_fn=dispatch,
            )

        self.assertEqual(ready.id, claimed.id)
        self.assertIs(observed["cancellation_token"], token)
        self.assertEqual(
            observed["owner_session_identity"],
            identity,
        )

    def test_force_executor_rejects_partial_authority_before_any_fallback(
        self,
    ) -> None:
        """Token/session authority is atomic even when the action is absent."""
        from scripts import importer

        setup_db = FakePipelineDB()
        setup_db.seed_request(make_request_row(id=42, status="wanted"))
        job = setup_db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:partial-execution-authority",
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/partial-execution-authority",
            ),
        )
        ready = setup_db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "evidence_ready"},
        )
        assert ready is not None
        claimed = claim_next_import_job(setup_db, worker_id="partial-authority")
        assert claimed is not None
        self.assertNotIn("action_path", claimed.preview_result or {})

        partials = (
            (
                "token-only",
                CancellationToken(),
                None,
            ),
            (
                "session-only",
                None,
                OwnerSessionIdentity(connection_object_id=1, backend_pid=2),
            ),
        )
        for label, token, identity in partials:
            with self.subTest(label=label):
                before_rows = copy.deepcopy(setup_db._import_jobs)
                runtime_config = MagicMock(
                    side_effect=AssertionError(
                        "partial authority reached runtime config"
                    )
                )
                dispatch = MagicMock(
                    side_effect=AssertionError(
                        "partial authority reached force dispatch"
                    )
                )
                with (
                    patch(
                        "lib.config.read_runtime_config",
                        runtime_config,
                    ),
                    self.assertRaisesRegex(ValueError, "must be paired"),
                ):
                    importer.execute_import_job(
                        setup_db,  # pyright: ignore[reportArgumentType]
                        claimed,
                        cancellation_token=token,
                        owner_session_identity=identity,
                        force_dispatch_fn=dispatch,
                    )

                runtime_config.assert_not_called()
                dispatch.assert_not_called()
                self.assertEqual(setup_db._import_jobs, before_rows)

    def test_importer_scans_past_contended_force_candidate(self) -> None:
        """One busy request cannot starve a later unrelated import."""
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT
        from scripts import importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        for request_id in (42, 43):
            db.seed_request(make_request_row(id=request_id, status="wanted"))
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key=f"force:scan:{request_id}",
                payload=force_import_payload(
                    download_log_id=request_id,
                    failed_path=f"/tmp/force-scan-{request_id}",
                ),
            )
            self._mark_importable(db, job)

        class StageSession:
            def __getattr__(self, name: str) -> object:
                return getattr(db, name)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                with db._pin_owner_session(token) as identity:
                    yield identity

            @contextmanager
            def advisory_lock(self, namespace: int, key: int):
                if (
                    namespace == ADVISORY_LOCK_NAMESPACE_IMPORT
                    and key == 42
                ):
                    yield False
                else:
                    with db.advisory_lock(namespace, key) as acquired:
                        yield acquired

            def claim_force_import_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
            ) -> ImportJob | None:
                return db.claim_force_import_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                )

            def close(self) -> None:
                return None

        executed: list[int] = []

        def execute(
            _stage_db: object,
            claimed: ImportJob,
            *,
            ctx: object | None = None,
            cancellation_token: CancellationToken,
            owner_session_identity: OwnerSessionIdentity,
        ) -> DispatchOutcome:
            del ctx, cancellation_token, owner_session_identity
            assert claimed.request_id is not None
            executed.append(claimed.request_id)
            return DispatchOutcome(False, "bounded progress proof")

        importer.run_once(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="force-scan",
            stage_db_factory=lambda _dsn: StageSession(),
            execution_lease_factory=_unavailable_execution_lease,
            execute_fn=execute,
        )

        self.assertEqual(executed, [43])
        first = db.get_import_job(1)
        second = db.get_import_job(2)
        assert first is not None and second is not None
        self.assertEqual(first.status, "queued")
        self.assertEqual(second.status, "failed")

    def test_main_reuses_one_scan_cursor_across_polls(self) -> None:
        """The process loop, not callers, owns the persistent import cursor."""
        from scripts import importer

        class PollProbeComplete(RuntimeError):
            pass

        candidate_db = FakePipelineDB()
        for request_id in range(1, importer.IMPORT_CANDIDATE_SCAN_LIMIT + 1):
            candidate_db.seed_request(make_request_row(
                id=request_id,
                status="wanted",
            ))
            candidate_db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key=f"cursor-probe:{request_id}",
                payload=force_import_payload(
                    download_log_id=request_id,
                    failed_path=f"/tmp/cursor-probe-{request_id}",
                ),
            )
            candidate_db._import_jobs[-1]["preview_status"] = (
                "evidence_ready"
            )
        candidates = candidate_db.peek_import_job_candidates(
            limit=importer.IMPORT_CANDIDATE_SCAN_LIMIT,
        )
        observed_offsets: list[int] = []

        class CursorProbeDB:
            @contextmanager
            def advisory_lock(self, _namespace: int, _key: int):
                yield True

            def recover_running_import_jobs(
                self,
                *,
                requeue_message: str,
                recovery_message: str,
                limit: int,
            ) -> list[ImportJob]:
                del requeue_message, recovery_message, limit
                return []

            def list_automation_import_jobs_for_startup_recovery(
                self,
            ) -> list[ImportJob]:
                return []

            def list_terminal_force_action_cleanup_jobs(
                self,
            ) -> list[ImportJob]:
                return []

            def peek_import_job_candidates(
                self,
                *,
                execution_lease: ExecutionLeaseSnapshot | None = None,
                limit: int,
                offset: int = 0,
            ) -> list[ImportJob]:
                del execution_lease, limit
                observed_offsets.append(offset)
                if len(observed_offsets) == 2:
                    raise PollProbeComplete
                return candidates

            def close(self) -> None:
                return None

        db = CursorProbeDB()

        argv = [
            "importer.py",
            "--dsn",
            "postgresql://fake",
            "--poll-interval",
            "0",
            "--worker-id",
            "cursor-probe",
        ]
        with (
            patch("sys.argv", argv),
            patch("scripts.importer.PipelineDB", return_value=db),
            patch("scripts.importer.time.sleep"),
            self.assertRaises(PollProbeComplete),
        ):
            importer.main()

        self.assertEqual(
            observed_offsets,
            [0, importer.IMPORT_CANDIDATE_SCAN_LIMIT],
        )


class TestAutomationImporterOwnership(unittest.TestCase):
    def _importable_owner(
        self,
        db: FakePipelineDB,
        *,
        request_id: int = 42,
    ) -> ImportJob:
        db.seed_request(make_request_row(
            id=request_id,
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-07-29T00:00:00+00:00",
                "current_path": f"/processing/albums/request-{request_id}",
                "files": [],
            },
        ))
        job = handoff_automation_owner(
            db,
            request_id,
            state=db.request(request_id)["active_download_state"],
        )
        preview_lease = _preview_execution_lease(
            f"importer-owner-{request_id}"
        )
        claimed = claim_next_import_preview_job(db, worker_id="preview",
        execution_lease=preview_lease,)
        assert claimed is not None
        ready = db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
            expected_execution_lease=preview_lease,
        )
        assert ready is not None
        return ready

    def test_pins_before_import_and_uses_same_session_for_release(self) -> None:
        from lib.pipeline_db import (
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            ADVISORY_LOCK_NAMESPACE_RELEASE,
        )
        from scripts import importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        self._importable_owner(db)
        lease = _importer_execution_lease("lock-trace")
        trace: list[str] = []

        class StageSession:
            def __init__(self) -> None:
                self.closed = False

            def __getattr__(self, name: str) -> object:
                return getattr(db, name)

            def get_import_job(self, job_id: int) -> ImportJob | None:
                return db.get_import_job(job_id)

            def get_request(
                self,
                request_id: int,
            ) -> Mapping[str, object] | None:
                return db.get_request(request_id)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                token.raise_if_cancelled()
                trace.append("pin-enter")
                try:
                    yield OwnerSessionIdentity(id(self), 4242)
                finally:
                    trace.append("pin-exit")

            @contextmanager
            def advisory_lock(self, namespace: int, key: int):
                trace.append(f"lock-enter:{namespace}:{key}")
                try:
                    yield True
                finally:
                    trace.append(f"lock-exit:{namespace}:{key}")

            def claim_automation_import_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
                execution_lease: ExecutionLeaseSnapshot,
            ) -> ImportJob | None:
                self.assert_claim_inside_import_lock()
                trace.append("claim")
                return db.claim_automation_import_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                    execution_lease=execution_lease,
                )

            @staticmethod
            def assert_claim_inside_import_lock() -> None:
                if trace[-1] != (
                    f"lock-enter:{ADVISORY_LOCK_NAMESPACE_IMPORT}:42"
                ):
                    raise AssertionError(
                        "automation claim escaped the pinned IMPORT lock"
                    )

            def close(self) -> None:
                self.closed = True
                trace.append("close")

        stage = StageSession()

        def execute(
            stage_db: object,
            _job: ImportJob,
            *,
            ctx: object | None = None,
            execution_lease: ExecutionLeaseSnapshot,
            cancellation_token: CancellationToken,
            owner_session_identity: OwnerSessionIdentity,
        ) -> DispatchOutcome:
            del ctx, owner_session_identity
            self.assertIs(stage_db, stage)
            self.assertEqual(execution_lease, lease)
            self.assertIsInstance(cancellation_token, CancellationToken)
            runtime = importer._build_runtime_context(
                stage,  # pyright: ignore[reportArgumentType]
                borrow_session=True,
            )
            self.assertIs(runtime.pipeline_db_source._get_db(), stage)
            runtime.pipeline_db_source.close()
            self.assertFalse(stage.closed)
            with stage.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_RELEASE,
                99,
            ) as acquired:
                self.assertTrue(acquired)
            return DispatchOutcome(False, "prelaunch defer", deferred=True)

        result = importer.run_once(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="importer",
            stage_db_factory=lambda _dsn: stage,
            execution_lease_factory=lambda **_kwargs: lease,
            execute_fn=execute,
        )

        self.assertIsNone(result)
        stored = db.get_import_job(1)
        assert stored is not None
        self.assertEqual((stored.status, stored.preview_status), (
            "queued",
            "waiting",
        ))
        self.assertEqual(trace, [
            "pin-enter",
            f"lock-enter:{ADVISORY_LOCK_NAMESPACE_IMPORT}:42",
            "claim",
            f"lock-enter:{ADVISORY_LOCK_NAMESPACE_RELEASE}:99",
            f"lock-exit:{ADVISORY_LOCK_NAMESPACE_RELEASE}:99",
            f"lock-exit:{ADVISORY_LOCK_NAMESPACE_IMPORT}:42",
            "pin-exit",
            "close",
        ])

    def test_wrong_owner_stops_before_execution(self) -> None:
        from scripts import importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        self._importable_owner(db)
        lease = _importer_execution_lease("wrong-owner")

        class StageSession:
            def __getattr__(self, name: str) -> object:
                return getattr(db, name)

            def get_import_job(self, job_id: int) -> ImportJob | None:
                return db.get_import_job(job_id)

            def get_request(
                self,
                request_id: int,
            ) -> Mapping[str, object] | None:
                return db.get_request(request_id)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                token.raise_if_cancelled()
                yield OwnerSessionIdentity(id(self), 4242)

            @contextmanager
            def advisory_lock(self, _namespace: int, _key: int):
                yield True

            def claim_automation_import_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
                execution_lease: ExecutionLeaseSnapshot,
            ) -> ImportJob | None:
                return db.claim_automation_import_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                    execution_lease=execution_lease,
                )

            def close(self) -> None:
                return None

        execute = MagicMock()

        def stage_factory(_dsn: str) -> StageSession:
            db._requests[42]["active_automation_import_job_id"] = 999
            return StageSession()

        result = importer.run_once(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="importer",
            stage_db_factory=stage_factory,
            execution_lease_factory=lambda **_kwargs: lease,
            execute_fn=execute,
        )

        self.assertIsNone(result)
        execute.assert_not_called()
        stored = db.get_import_job(1)
        assert stored is not None
        self.assertEqual((stored.status, stored.attempts), ("queued", 0))
        self.assertIsNone(stored.execution_invocation_id)

    def test_import_lock_contention_leaves_claimable_then_progresses(self) -> None:
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT
        from scripts import importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        self._importable_owner(db)
        lease = _importer_execution_lease("import-lock-contention")
        writes: list[str] = []

        class StageSession:
            def __init__(self, *, acquire: bool) -> None:
                self.acquire = acquire

            def __getattr__(self, name: str) -> object:
                if not self.acquire and name in {
                    "requeue_import_job_for_preview",
                    "mark_import_job_failed",
                }:
                    def forbidden_write(
                        *_args: object,
                        **_kwargs: object,
                    ) -> None:
                        writes.append(name)
                    return forbidden_write
                return getattr(db, name)

            def get_import_job(self, job_id: int) -> ImportJob | None:
                return db.get_import_job(job_id)

            def get_request(
                self,
                request_id: int,
            ) -> Mapping[str, object] | None:
                return db.get_request(request_id)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                token.raise_if_cancelled()
                yield OwnerSessionIdentity(id(self), 4242)

            @contextmanager
            def advisory_lock(self, namespace: int, _key: int):
                self.assert_import_namespace(namespace)
                yield self.acquire

            @staticmethod
            def assert_import_namespace(namespace: int) -> None:
                if namespace != ADVISORY_LOCK_NAMESPACE_IMPORT:
                    raise AssertionError(f"unexpected lock namespace {namespace}")

            def claim_automation_import_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
                execution_lease: ExecutionLeaseSnapshot,
            ) -> ImportJob | None:
                return db.claim_automation_import_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                    execution_lease=execution_lease,
                )

            def close(self) -> None:
                return None

        execute = MagicMock(return_value=DispatchOutcome(
            False,
            "prelaunch defer",
            deferred=True,
        ))
        blocked = importer.run_once(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="importer",
            stage_db_factory=lambda _dsn: StageSession(acquire=False),
            execution_lease_factory=lambda **_kwargs: lease,
            execute_fn=execute,
        )

        self.assertIsNone(blocked)
        self.assertEqual(writes, [])
        execute.assert_not_called()
        stored = db.get_import_job(1)
        assert stored is not None
        self.assertEqual((stored.status, stored.attempts), ("queued", 0))
        self.assertIsNone(importer._execution_lease_from_job(stored))

        progressed = importer.run_once(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="importer",
            stage_db_factory=lambda _dsn: StageSession(acquire=True),
            execution_lease_factory=lambda **_kwargs: lease,
            execute_fn=execute,
        )

        self.assertIsNone(progressed)
        execute.assert_called_once()
        retried = db.get_import_job(1)
        assert retried is not None
        self.assertEqual((retried.status, retried.attempts), ("queued", 1))

    def test_owner_session_cancellation_fail_stops_without_requeue(self) -> None:
        from scripts import importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        self._importable_owner(db)
        lease = _importer_execution_lease("session-loss")

        class StageSession:
            def __getattr__(self, name: str) -> object:
                return getattr(db, name)

            def get_import_job(self, job_id: int) -> ImportJob | None:
                return db.get_import_job(job_id)

            def get_request(
                self,
                request_id: int,
            ) -> Mapping[str, object] | None:
                return db.get_request(request_id)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                token.raise_if_cancelled()
                yield OwnerSessionIdentity(id(self), 4242)

            @contextmanager
            def advisory_lock(self, _namespace: int, _key: int):
                yield True

            def claim_automation_import_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
                execution_lease: ExecutionLeaseSnapshot,
            ) -> ImportJob | None:
                return db.claim_automation_import_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                    execution_lease=execution_lease,
                )

            def close(self) -> None:
                return None

        def lose_session(
            _db: object,
            _job: ImportJob,
            *,
            ctx: object | None = None,
            execution_lease: ExecutionLeaseSnapshot,
            cancellation_token: CancellationToken,
            owner_session_identity: OwnerSessionIdentity,
        ) -> DispatchOutcome:
            del ctx, execution_lease, owner_session_identity
            cancellation_token.cancel("owner_session_lost")
            cancellation_token.raise_if_cancelled()
            raise AssertionError("unreachable")

        with self.assertRaises(ExecutionCancelled):
            importer.run_once(
                db,  # pyright: ignore[reportArgumentType]
                worker_id="importer",
                stage_db_factory=lambda _dsn: StageSession(),
                execution_lease_factory=lambda **_kwargs: lease,
                execute_fn=lose_session,
            )

        stored = db.get_import_job(1)
        assert stored is not None
        self.assertEqual(stored.status, "running")
        self.assertEqual(stored.preview_status, "evidence_ready")

    def test_startup_recovery_requires_exact_dead_importer_lease(self) -> None:
        from lib.import_execution import (
            ExecutionLivenessEvidence,
        )
        from scripts import importer

        class Probe:
            def __init__(self, evidence: ExecutionLivenessEvidence) -> None:
                self.evidence = evidence

            def observe(
                self,
                lease: ExecutionLeaseSnapshot,
            ) -> ExecutionLivenessEvidence:
                self_outer.assertEqual(lease, expected_lease)
                return self.evidence

        self_outer = self
        db = FakePipelineDB()
        self._importable_owner(db)
        expected_lease = _importer_execution_lease("startup-recovery")
        claimed = claim_next_import_job(db, worker_id="old-importer",
        execution_lease=expected_lease,)
        assert claimed is not None

        unknown = ExecutionLivenessEvidence(
            lease=expected_lease,
            current_host_boot_id=None,
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
            probe_error="systemd unavailable",
        )
        dead = ExecutionLivenessEvidence(
            lease=expected_lease,
            current_host_boot_id="replacement-boot",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )

        self.assertEqual(
            importer.recover_abandoned_running_jobs(
                cast(Any, db),
                liveness_probe=Probe(unknown),
            ),
            [],
        )
        still_running = db.get_import_job(claimed.id)
        assert still_running is not None
        self.assertEqual(still_running.status, "running")

        recovered = importer.recover_abandoned_running_jobs(
            cast(Any, db),
            liveness_probe=Probe(dead),
        )
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].id, claimed.id)
        self.assertEqual(recovered[0].status, "queued")

    def test_production_completion_chain_receives_exact_cancellation_authority(
        self,
    ) -> None:
        from scripts import importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        self._importable_owner(db)
        lease = _importer_execution_lease("token-chain")
        claimed = claim_next_import_job(db, worker_id="importer",
        execution_lease=lease,)
        assert claimed is not None
        token = CancellationToken()
        observed_authority: dict[str, object] = {}

        def process_completed(
            *_args: object,
            **kwargs: object,
        ) -> CompletionDeferred:
            observed_authority.update(kwargs)
            return CompletionDeferred("test boundary")

        with db._pin_owner_session(token) as identity:
            outcome = importer.execute_automation_import_job(
                db,  # pyright: ignore[reportArgumentType]
                claimed,
                completed_processing_fn=process_completed,
                execution_lease=lease,
                cancellation_token=token,
                owner_session_identity=identity,
            )

        self.assertTrue(outcome.deferred)
        self.assertIs(
            observed_authority["cancellation_token"],
            token,
        )
        self.assertEqual(
            observed_authority["execution_lease"],
            lease,
        )
        self.assertEqual(
            observed_authority["owner_session_identity"],
            identity,
        )

    def test_validation_forwards_token_to_dispatch_without_drop(self) -> None:
        from lib.download_validation import _handle_valid_result

        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "processing-album")
            os.mkdir(source)
            with open(os.path.join(source, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            staging = os.path.join(root, "incoming")
            os.mkdir(staging)
            os.makedirs(os.path.join(staging, "auto-import", "Artist"))
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-07-29T00:00:00+00:00",
                    "current_path": source,
                    "files": [],
                },
            ))
            job = handoff_automation_owner(
                db,
                42,
                state=db.request(42)["active_download_state"],
                canonical_path=source,
            )
            preview_lease = _preview_execution_lease("token-preview")
            assert claim_next_import_preview_job(db, worker_id="preview",
            execution_lease=preview_lease,) is not None
            assert db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
                expected_execution_lease=preview_lease,
            ) is not None
            lease = _importer_execution_lease("token-dispatch")
            claimed = claim_next_import_job(db, worker_id="importer",
            execution_lease=lease,)
            assert claimed is not None
            token = CancellationToken()
            ctx = make_ctx_with_fake_db(
                db,
                cfg=CratediggerConfig(
                    beets_staging_dir=staging,
                    pipeline_db_enabled=True,
                ),
            )
            album = make_grab_list_entry(
                album_id=42,
                artist="Artist",
                title="Album",
                mb_release_id="mbid-123",
                db_source="request",
                db_request_id=42,
            )
            dispatch = MagicMock(
                return_value=DispatchOutcome(
                    False,
                    "test defer",
                    deferred=True,
                )
            )

            with db._pin_owner_session(token) as identity:
                outcome = _handle_valid_result(
                    album,
                    ValidationResult(
                        valid=True,
                        distance=0.01,
                        scenario="strong_match",
                    ),
                    StagedAlbum(current_path=source, request_id=42),
                    ctx,
                    import_job_id=claimed.id,
                    dispatch_fn=dispatch,
                    cancellation_token=token,
                    execution_lease=lease,
                    owner_session_identity=identity,
                )

        assert outcome is not None
        self.assertIs(dispatch.call_args.kwargs["cancellation_token"], token)
        self.assertEqual(dispatch.call_args.kwargs["execution_lease"], lease)
        self.assertEqual(
            dispatch.call_args.kwargs["owner_session_identity"],
            identity,
        )


class TestImportPreviewWorker(unittest.TestCase):
    def test_execution_lease_unit_matches_nix_service(self) -> None:
        """Recovery probes must address the exact deployed systemd unit."""
        from scripts import import_preview_worker

        module_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "nix",
            "module.nix",
        )
        with open(module_path, encoding="utf-8") as handle:
            module_text = handle.read()
        service_name = import_preview_worker.PREVIEW_SYSTEMD_UNIT.removesuffix(
            ".service"
        )
        self.assertIn(
            f"systemd.services.{service_name} =",
            module_text,
        )

    def test_preview_scans_past_contended_force_candidate(self) -> None:
        """One busy request cannot starve a later unrelated preview."""
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT
        from scripts import import_preview_worker

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        for request_id in (42, 43):
            db.seed_request(make_request_row(id=request_id, status="wanted"))
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key=f"force:preview-scan:{request_id}",
                payload=force_import_payload(
                    download_log_id=request_id,
                    failed_path=f"/tmp/force-preview-scan-{request_id}",
                ),
            )

        class StageSession:
            def __getattr__(self, name: str) -> object:
                return getattr(db, name)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                with db._pin_owner_session(token) as identity:
                    yield identity

            @contextmanager
            def advisory_lock(self, namespace: int, key: int):
                if (
                    namespace == ADVISORY_LOCK_NAMESPACE_IMPORT
                    and key == 42
                ):
                    yield False
                else:
                    with db.advisory_lock(namespace, key) as acquired:
                        yield acquired

            def claim_force_import_preview_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
            ) -> ImportJob | None:
                return db.claim_force_import_preview_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                )

            def close(self) -> None:
                return None

        processed: list[int] = []

        def pause_after_claim(
            _stage_db: object,
            claimed: ImportJob,
            **_kwargs: object,
        ) -> ImportJob:
            assert claimed.request_id is not None
            processed.append(claimed.request_id)
            return claimed

        result = import_preview_worker.run_once(
            db,
            worker_id="preview-scan",
            stage_db_factory=lambda _dsn: StageSession(),
            execution_lease_factory=_unavailable_execution_lease,
            process_fn=pause_after_claim,
        )

        self.assertIsNotNone(result)
        self.assertEqual(processed, [43])
        first = db.get_import_job(1)
        second = db.get_import_job(2)
        assert first is not None and second is not None
        self.assertEqual(first.preview_status, "waiting")
        self.assertEqual(second.preview_status, "running")

    def test_terminal_force_preview_failure_reclaims_unpublished_stale_action(self):
        """A failed preview owns and removes a stale action left by its retry."""
        from lib.import_preview import force_action_copy_path
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            db = FakePipelineDB()
            download_log_id = _force_download_log(db, 42, source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            action_path = force_action_copy_path(cfg, job.id)
            os.mkdir(action_path, 0o700)
            with open(os.path.join(action_path, "stale.mp3"), "wb") as handle:
                handle.write(b"prior action")

            terminal = db.mark_import_job_preview_failed(
                job.id,
                preview_status="measurement_failed",
                error="preparation failed",
            )
            assert terminal is not None
            returned = import_preview_worker._cleanup_terminal_preview_force_action(
                claimed,
                terminal,
                action_path=None,
                runtime_config=cfg,
            )

            self.assertEqual(returned, terminal)
            self.assertFalse(os.path.exists(action_path))

    def _preview(
        self,
        verdict: str,
        *,
        reason: str | None = None,
        source_path: str | None = None,
    ) -> ImportPreviewResult:
        """Build a preview result for worker tests.

        After U5 the worker emits only ``evidence_ready`` and
        ``measurement_failed``. For backward-compat with existing tests we
        translate the legacy verdict labels:

          * ``would_import`` / ``confident_reject`` → ``evidence_ready`` (the
            importer would have read these and decided; in U5 onward, the
            importer reads the persisted evidence instead).
          * ``uncertain`` → ``measurement_failed`` (preview could not produce
            evidence; self-healing finalize fires).

        Explicit ``evidence_ready`` / ``measurement_failed`` callers get those
        verdicts unchanged.
        """
        from lib.quality import MeasurementFailure

        if verdict in ("would_import", "evidence_ready"):
            translated = "evidence_ready"
            failure = None
        elif verdict in ("uncertain", "confident_reject", "measurement_failed"):
            translated = "measurement_failed"
            failure = MeasurementFailure(
                reason="measurement_crashed",
                detail=reason or verdict,
                source_path=source_path or "",
            )
        else:
            translated = verdict
            failure = None
        return ImportPreviewResult(
            mode="path",
            verdict=translated,
            would_import=verdict == "would_import",
            confident_reject=verdict == "confident_reject",
            uncertain=verdict == "uncertain",
            decision=reason,
            reason=reason,
            stage_chain=[f"preview:{reason or verdict}"],
            source_path=source_path,
            failure=failure,
        )

    def _seed_job_candidate_evidence(
        self,
        db: object,
        job_id: int,
        source_path: str,
    ) -> None:
        _seed_candidate_for_import_job(
            db, job_id,
            mb_release_id=f"mbid-job-{job_id}",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )

    def test_force_job_preview_would_import_marks_importable(self):
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None

            preview_result = self._preview(
                "would_import",
                reason="import",
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                # Simulate production: preview measures + persists evidence.
                self._seed_job_candidate_evidence(db, claimed.id, source)
                return preview_result

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    runtime_config=cfg,
                )

        preview.assert_called_once_with(
            db,
            request_id=42,
            path=ANY,
            source_display_path=source,
            force=True,
            download_log_id=download_log_id,
            import_job_id=claimed.id,
            runtime_config=cfg,
            repair_fn=ANY,
        )
        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(updated.preview_result["verdict"], "evidence_ready")
        self.assertIsNotNone(updated.importable_at)

    def test_force_execute_path_requires_forwarded_runtime_config(self):
        """The real execute path must retain the caller's private config.

        The force request is present because claim now requires fresh request
        authority. Dropping ``runtime_config`` before ``execute_preview_job``
        still makes the production config reject this private test source at
        the exact execute boundary.
        """
        from lib.fs_authority import FilesystemAuthorityError
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"database authority")
            parent = os.path.dirname(os.path.dirname(os.path.dirname(source)))
            payload_source = os.path.join(
                parent, "payload", "failed_imports", "Payload",
            )
            os.makedirs(payload_source)
            with open(os.path.join(payload_source, "01.mp3"), "wb") as handle:
                handle.write(b"payload metadata")

            def new_job() -> tuple[FakePipelineDB, ImportJob]:
                db = FakePipelineDB()
                download_log_id = _force_download_log(db, 42, source)
                db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=42,
                    dedupe_key=force_import_dedupe_key(download_log_id),
                    payload=force_import_payload(
                        download_log_id=download_log_id,
                        failed_path=payload_source,
                    ),
                )
                claimed = claim_next_import_preview_job(db, worker_id="preview")
                assert claimed is not None
                return db, claimed

            def run_with(
                runtime_config: CratediggerConfig | None,
            ) -> tuple[ImportJob, FakePipelineDB]:
                db, claimed = new_job()
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    runtime_config=runtime_config,
                )
                assert updated is not None
                return updated, db

            updated, _ = run_with(cfg)
            self.assertEqual(updated.status, "failed")
            self.assertEqual(updated.preview_status, "measurement_failed")
            assert updated.preview_result is not None
            self.assertEqual(
                updated.preview_result["reason"],
                "measurement_crashed",
            )
            self.assertEqual(updated.preview_result["source_path"], source)
            self.assertEqual(
                os.listdir(os.path.join(cfg.processing_dir, "preview")),
                [],
            )

            # Known-bad fault injection: this invokes the real execute path
            # after discarding the config at its exact call boundary.
            mutant_db, mutant_job = new_job()
            with self.assertRaises(FilesystemAuthorityError):
                import_preview_worker.execute_preview_job(
                    mutant_db,
                    mutant_job,
                    runtime_config=None,
                )

    def test_automation_job_preview_uses_active_download_current_path(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            handoff_automation_owner(
                db,
                42,
                state=db.request(42)["active_download_state"],
                canonical_path=staged,
            )
            lease = _preview_execution_lease()
            claimed = claim_next_import_preview_job(db, worker_id="preview",
            execution_lease=lease,)
            assert claimed is not None
            authority = import_preview_worker._AutomationPreviewAuthority(
                request=db.request(42),
                state=ActiveDownloadState.from_raw(
                    db.request(42)["active_download_state"]
                ),
                canonical_path=staged,
            )

            preview_result = self._preview(
                "would_import",
                reason="import",
                source_path=staged,
            )
            measurement_calls: list[tuple[object, dict[str, object]]] = []

            def fake_preview(
                preview_db: object,
                **kwargs: object,
            ) -> ImportPreviewResult:
                measurement_calls.append((preview_db, kwargs))
                self._seed_job_candidate_evidence(
                    preview_db,
                    claimed.id,
                    staged,
                )
                return preview_result

            token = CancellationToken()
            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                execution_lease=lease,
                automation_authority=authority,
                cancellation_token=token,
                candidate_measurement_fn=fake_preview,
            )
            self.assertEqual(len(measurement_calls), 1)
            preview_db, preview_kwargs = measurement_calls[0]
            self.assertIsInstance(
                preview_db,
                import_preview_worker._AutomationPreviewDB,
            )
            self.assertEqual(preview_kwargs["request_id"], 42)
            self.assertEqual(preview_kwargs["path"], staged)
            self.assertEqual(preview_kwargs["force"], False)
            self.assertIsNone(preview_kwargs["download_log_id"])
            self.assertEqual(preview_kwargs["import_job_id"], claimed.id)
            self.assertIs(preview_kwargs["cancellation_token"], token)
            assert updated is not None
            self.assertEqual(updated.preview_status, "evidence_ready")

    def test_automation_preview_reject_with_evidence_marks_ready_for_dispatch(self):
        from lib.download_materialization import Materialized
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            handoff_automation_owner(
                db,
                42,
                state=db.request(42)["active_download_state"],
                canonical_path=staged,
            )
            lease = _preview_execution_lease()
            claimed = claim_next_import_preview_job(db, worker_id="preview",
            execution_lease=lease,)
            assert claimed is not None
            authority = import_preview_worker._AutomationPreviewAuthority(
                request=db.request(42),
                state=ActiveDownloadState.from_raw(
                    db.request(42)["active_download_state"]
                ),
                canonical_path=staged,
            )
            self._seed_job_candidate_evidence(
                import_preview_worker._AutomationPreviewDB(db, lease),
                claimed.id,
                staged,
            )

            def reject_preview(
                _preview_db: object,
                **_kwargs: object,
            ) -> ImportPreviewResult:
                return self._preview(
                    "confident_reject",
                    reason="spectral_reject",
                    source_path=staged,
                )

            materialize_calls: list[int] = []

            def materialize(*_args: object, **_kwargs: object) -> Materialized:
                materialize_calls.append(claimed.id)
                return Materialized()

            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                execution_lease=lease,
                automation_authority=authority,
                cancellation_token=CancellationToken(),
                candidate_measurement_fn=reject_preview,
                automation_materialize_fn=materialize,
            )

            assert updated is not None
            self.assertEqual(materialize_calls, [claimed.id])
            self.assertEqual(updated.status, "queued")
            self.assertEqual(updated.preview_status, "evidence_ready")
            self.assertIsNone(updated.preview_error)
            self.assertEqual(
                db.request(42)["active_automation_import_job_id"],
                updated.id,
            )
            claimed_for_import = claim_next_import_job(db, worker_id="importer",
            execution_lease=_preview_execution_lease("test-importer"),)
            assert claimed_for_import is not None
            self.assertEqual(claimed_for_import.id, updated.id)

    def test_preview_lock_contention_leaves_claimable_then_progresses(self):
        """A transient IMPORT miss stays queued and the next poll progresses."""
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.processing_paths import (
            canonical_folder_for_row,
            processing_albums_dir,
        )
        from scripts import import_preview_worker

        class StageSession:
            def __init__(
                self,
                inner: FakePipelineDB,
                trace: list[str],
                *,
                acquire: bool,
            ) -> None:
                self._inner = inner
                self._trace = trace
                self._acquire = acquire
                self._pinned = False
                self._lock_held = False

            def __getattr__(self, name: str) -> object:
                return getattr(self._inner, name)

            @contextmanager
            def advisory_lock(self, namespace: int, key: int):
                if not self._pinned:
                    raise AssertionError("IMPORT lock used an unpinned session")
                self._trace.append(f"lock-enter:{namespace}:{key}")
                self._lock_held = self._acquire
                try:
                    yield self._acquire
                finally:
                    self._lock_held = False
                    self._trace.append(f"lock-exit:{namespace}:{key}")

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                token.raise_if_cancelled()
                self._pinned = True
                self._trace.append("pin-enter")
                try:
                    yield object()
                finally:
                    self._trace.append("pin-exit")
                    self._pinned = False

            def claim_automation_import_preview_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
                execution_lease: ExecutionLeaseSnapshot,
            ) -> ImportJob | None:
                if not self._pinned or not self._lock_held:
                    raise AssertionError(
                        "preview claim escaped the pinned IMPORT lock"
                    )
                return self._inner.claim_automation_import_preview_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                    execution_lease=execution_lease,
                )

            def set_import_job_candidate_evidence(
                self,
                import_job_id: int,
                evidence_id: int | None,
                *,
                expected_execution_lease: ExecutionLeaseSnapshot | None = None,
            ) -> bool:
                if not self._pinned or not self._lock_held:
                    raise AssertionError(
                        "evidence commit escaped the pinned IMPORT lock"
                    )
                self._trace.append("commit:evidence")
                return self._inner.set_import_job_candidate_evidence(
                    import_job_id,
                    evidence_id,
                    expected_execution_lease=expected_execution_lease,
                )

            def mark_import_job_preview_importable(
                self,
                job_id: int,
                *,
                preview_result: dict[str, object] | None = None,
                message: str | None = None,
                expected_execution_lease: ExecutionLeaseSnapshot | None = None,
            ) -> ImportJob | None:
                if not self._pinned or not self._lock_held:
                    raise AssertionError(
                        "preview commit escaped the pinned IMPORT lock"
                    )
                self._trace.append("commit:preview")
                return self._inner.mark_import_job_preview_importable(
                    job_id,
                    preview_result=preview_result,
                    message=message,
                    expected_execution_lease=expected_execution_lease,
                )

            def close(self) -> None:
                self._trace.append("close")

        with tempfile.TemporaryDirectory() as root:
            processing_dir = os.path.join(root, "processing")
            slskd_download_dir = os.path.join(root, "slskd")
            os.mkdir(processing_dir, 0o700)
            os.mkdir(processing_albums_dir(processing_dir), 0o700)
            os.mkdir(slskd_download_dir, 0o700)
            request = make_request_row(
                id=42,
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": "/pending/canonical-path",
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            )
            state = ActiveDownloadState.from_raw(
                request["active_download_state"]
            )
            canonical_path = canonical_folder_for_row(
                reconstruct_grab_list_entry(request, state),
                processing_albums_dir(processing_dir),
            )
            request["active_download_state"]["current_path"] = canonical_path
            os.mkdir(canonical_path, 0o700)
            with open(os.path.join(canonical_path, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            setattr(db, "dsn", "postgresql://fake")  # noqa: B010
            db.seed_request(request)
            job = handoff_automation_owner(
                db,
                42,
                state=db.request(42)["active_download_state"],
                canonical_path=canonical_path,
            )
            lease = _preview_execution_lease("resume-handoff")
            trace: list[str] = []
            blocked_trace: list[str] = []
            captured_units: list[str] = []
            persisted_claim_leases: list[tuple[str | None, str | None]] = []

            def capture_lease(
                *,
                systemd_unit: str,
            ) -> ExecutionLeaseSnapshot:
                captured_units.append(systemd_unit)
                return lease

            def preview(
                preview_db: object,
                *_args: Any,
                **_kwargs: Any,
            ) -> ImportPreviewResult:
                persisted = db.get_import_job(job.id)
                assert persisted is not None
                persisted_claim_leases.append((
                    persisted.execution_invocation_id,
                    persisted.execution_systemd_unit,
                ))
                self._seed_job_candidate_evidence(
                    preview_db,
                    job.id,
                    canonical_path,
                )
                return self._preview(
                    "would_import",
                    reason="import",
                    source_path=canonical_path,
                )

            def stage_factory(_dsn: str) -> StageSession:
                return StageSession(db, trace, acquire=True)

            blocked = import_preview_worker.run_once(
                db,
                worker_id="preview",
                heartbeat_interval=3600,
                runtime_config=CratediggerConfig(
                    processing_dir=processing_dir,
                    slskd_download_dir=slskd_download_dir,
                ),
                stage_db_factory=lambda _dsn: StageSession(
                    db,
                    blocked_trace,
                    acquire=False,
                ),
                heartbeat_db_factory=lambda _dsn: db,
                execution_lease_factory=capture_lease,
                candidate_measurement_fn=preview,
            )
            self.assertIsNone(blocked)
            unclaimed = db.get_import_job(job.id)
            assert unclaimed is not None
            self.assertEqual(
                (
                    unclaimed.preview_status,
                    unclaimed.preview_attempts,
                    unclaimed.execution_invocation_id,
                ),
                ("waiting", 0, None),
            )

            updated = import_preview_worker.run_once(
                db,
                worker_id="preview",
                heartbeat_interval=3600,
                runtime_config=CratediggerConfig(
                    processing_dir=processing_dir,
                    slskd_download_dir=slskd_download_dir,
                ),
                stage_db_factory=stage_factory,
                heartbeat_db_factory=lambda _dsn: db,
                execution_lease_factory=capture_lease,
                candidate_measurement_fn=preview,
            )

        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        self.assertEqual(
            captured_units,
            [
                import_preview_worker.PREVIEW_SYSTEMD_UNIT,
                import_preview_worker.PREVIEW_SYSTEMD_UNIT,
            ],
        )
        self.assertEqual(
            persisted_claim_leases,
            [(lease.invocation_id, lease.systemd_unit)],
        )
        self.assertEqual(
            trace,
            [
                "pin-enter",
                f"lock-enter:{import_preview_worker.ADVISORY_LOCK_NAMESPACE_IMPORT}:42",
                "commit:evidence",
                "commit:preview",
                f"lock-exit:{import_preview_worker.ADVISORY_LOCK_NAMESPACE_IMPORT}:42",
                "pin-exit",
                "close",
            ],
        )
        self.assertEqual(
            blocked_trace,
            [
                "pin-enter",
                f"lock-enter:{import_preview_worker.ADVISORY_LOCK_NAMESPACE_IMPORT}:42",
                f"lock-exit:{import_preview_worker.ADVISORY_LOCK_NAMESPACE_IMPORT}:42",
                "pin-exit",
                "close",
            ],
        )

    def test_automation_wrong_owner_stops_before_filesystem_or_measurement(self):
        from scripts import import_preview_worker

        class StageSession:
            def __init__(self, inner: FakePipelineDB) -> None:
                self._inner = inner

            def __getattr__(self, name: str) -> object:
                return getattr(self._inner, name)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                token.raise_if_cancelled()
                yield object()

            @contextmanager
            def advisory_lock(self, _namespace: int, _key: int):
                yield True

            def claim_automation_import_preview_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
                execution_lease: ExecutionLeaseSnapshot,
            ) -> ImportJob | None:
                return self._inner.claim_automation_import_preview_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                    execution_lease=execution_lease,
                )

            def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as canonical_path:
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            job = handoff_automation_owner(
                db,
                42,
                state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": canonical_path,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
                canonical_path=canonical_path,
            )
            lease = _preview_execution_lease("wrong-owner")
            candidates = db.peek_import_preview_job_candidates(
                execution_lease=lease,
                limit=1,
            )
            assert candidates
            candidate = candidates[0]
            db._requests[42]["active_automation_import_job_id"] = job.id + 1

            updated = import_preview_worker._process_automation_claim(
                candidate,
                dsn="postgresql://fake",
                worker_id="preview",
                execution_lease=lease,
                heartbeat_interval=3600,
                runtime_config=CratediggerConfig(),
                stage_db_factory=lambda _dsn: StageSession(db),
                heartbeat_db_factory=lambda _dsn: db,
            )
            self.assertEqual(os.listdir(canonical_path), [])

        self.assertIsNone(updated)
        stored = db.get_import_job(candidate.id)
        assert stored is not None
        self.assertIsNone(stored.candidate_evidence_id)

    def test_claimed_preview_with_lost_authority_fail_stops_worker(self) -> None:
        """A claimed row cannot return to the daemon under its live lease."""
        from scripts import import_preview_worker

        class AuthorityLostAfterClaimSession:
            def __init__(self, inner: FakePipelineDB) -> None:
                self._inner = inner

            def __getattr__(self, name: str) -> object:
                return getattr(self._inner, name)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                token.raise_if_cancelled()
                yield object()

            @contextmanager
            def advisory_lock(self, _namespace: int, _key: int):
                yield True

            def claim_automation_import_preview_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
                execution_lease: ExecutionLeaseSnapshot,
            ) -> ImportJob | None:
                claimed = (
                    self._inner.claim_automation_import_preview_job_under_lock(
                        job_id,
                        request_id=request_id,
                        worker_id=worker_id,
                        execution_lease=execution_lease,
                    )
                )
                if claimed is not None:
                    self._inner._requests[request_id][
                        "active_download_state"
                    ] = None
                return claimed

            def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as canonical_path:
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            handoff_automation_owner(
                db,
                42,
                state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": canonical_path,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
                canonical_path=canonical_path,
            )
            lease = _preview_execution_lease("authority-lost-after-claim")
            candidate = db.peek_import_preview_job_candidates(
                execution_lease=lease,
                limit=1,
            )[0]

            with self.assertRaises(AutomationOwnerFailStop):
                import_preview_worker._process_automation_claim(
                    candidate,
                    dsn="postgresql://fake",
                    worker_id="preview",
                    execution_lease=lease,
                    heartbeat_interval=3600,
                    runtime_config=CratediggerConfig(),
                    stage_db_factory=(
                        lambda _dsn: AuthorityLostAfterClaimSession(db)
                    ),
                    heartbeat_db_factory=lambda _dsn: db,
                )

        stored = db.get_import_job(candidate.id)
        assert stored is not None
        self.assertEqual(stored.preview_status, "running")
        self.assertEqual(db.request(42)["status"], "processing")
        self.assertEqual(
            db.request(42)["active_automation_import_job_id"],
            candidate.id,
        )

    def test_automation_pinned_session_loss_before_lock_is_fail_stop(self):
        """Known-bad reconnect between pin and IMPORT lock cannot run preview."""
        from lib.pipeline_db._core import OwnerSessionLost
        from scripts import import_preview_worker

        class LostStageSession:
            def __init__(self, inner: FakePipelineDB) -> None:
                self._inner = inner
                self._pinned = False
                self._token: CancellationToken | None = None

            def __getattr__(self, name: str) -> object:
                return getattr(self._inner, name)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                token.raise_if_cancelled()
                self._pinned = True
                self._token = token
                try:
                    yield object()
                finally:
                    self._pinned = False

            @contextmanager
            def advisory_lock(self, namespace: int, key: int):
                del namespace, key
                if not self._pinned or self._token is None:
                    raise AssertionError(
                        "known-bad lock-before-pin ordering returned"
                    )
                self._token.cancel("owner_session_lost_before_import_lock")
                raise OwnerSessionLost(
                    "pinned backend died before IMPORT lock acquisition"
                )
                yield False  # pragma: no cover - contextmanager shape only

            def claim_automation_import_preview_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
                execution_lease: ExecutionLeaseSnapshot,
            ) -> ImportJob | None:
                raise AssertionError(
                    "claim ran after owner-session loss"
                )

            def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as canonical_path:
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            handoff_automation_owner(
                db,
                42,
                state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": canonical_path,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
                canonical_path=canonical_path,
            )
            lease = _preview_execution_lease("lost-before-lock")
            candidates = db.peek_import_preview_job_candidates(
                execution_lease=lease,
                limit=1,
            )
            assert candidates
            candidate = candidates[0]

            with self.assertRaises(OwnerSessionLost):
                import_preview_worker._process_automation_claim(
                    candidate,
                    dsn="postgresql://fake",
                    worker_id="preview",
                    execution_lease=lease,
                    heartbeat_interval=3600,
                    runtime_config=CratediggerConfig(),
                    stage_db_factory=lambda _dsn: LostStageSession(db),
                    heartbeat_db_factory=lambda _dsn: db,
                )
            self.assertEqual(os.listdir(canonical_path), [])

        stored = db.get_import_job(candidate.id)
        assert stored is not None
        self.assertEqual(stored.preview_status, "waiting")
        self.assertIsNone(stored.execution_invocation_id)
        self.assertIsNone(stored.candidate_evidence_id)

    def test_automation_session_loss_during_materialization_blocks_later_mutation(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as root:
            canonical_path = os.path.join(root, "processing", "albums", "album")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            handoff_automation_owner(
                db,
                42,
                state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": canonical_path,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
                canonical_path=canonical_path,
            )
            lease = _preview_execution_lease("session-loss")
            claimed = claim_next_import_preview_job(db, worker_id="preview",
            execution_lease=lease,)
            assert claimed is not None
            authority = import_preview_worker._AutomationPreviewAuthority(
                request=db.request(42),
                state=ActiveDownloadState.from_raw(
                    db.request(42)["active_download_state"]
                ),
                canonical_path=canonical_path,
            )
            token = CancellationToken()
            materialize_calls: list[dict[str, object]] = []

            def lose_session(
                *_args: object,
                **kwargs: object,
            ) -> None:
                materialize_calls.append(kwargs)
                self.assertIs(kwargs["cancellation_token"], token)
                token.cancel("owner_session_lost")
                token.raise_if_cancelled()

            with self.assertRaises(ExecutionCancelled):
                import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    runtime_config=CratediggerConfig(
                        slskd_download_dir=root,
                        processing_dir=os.path.join(root, "processing"),
                    ),
                    execution_lease=lease,
                    automation_authority=authority,
                    cancellation_token=token,
                    automation_materialize_fn=lose_session,
                )

            stored = db.get_import_job(claimed.id)
            assert stored is not None
            self.assertEqual(stored.preview_status, "running")
            self.assertIsNone(stored.candidate_evidence_id)
            self.assertEqual(len(materialize_calls), 1)

    def test_evidence_readiness_fallback_preserves_collected_audit(self):
        from lib.quality import SpectralAnalysisDetail, SpectralDetail
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, status="downloading"))
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key="force_import:evidence-readiness-fallback",
                payload={"download_log_id": 1, "failed_path": source},
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            audit = SpectralDetail(
                candidate=SpectralAnalysisDetail(
                    attempted=True, grade="suspect", bitrate_kbps=128),
                existing=SpectralAnalysisDetail(
                    attempted=True, grade="genuine"),
            )
            preview_result = ImportPreviewResult(
                mode="path",
                verdict="evidence_ready",
                decision="import",
                source_path=source,
                request_id=42,
                import_result=ImportResult(spectral=audit),
            )

            updated = import_preview_worker.process_claimed_preview_job(
                db, claimed,
                preview_fn=lambda db, job: preview_result,
            )

        assert updated is not None
        self.assertEqual(updated.preview_status, "measurement_failed")
        logged = ImportResult.from_json(db.download_logs[-1].import_result)
        self.assertEqual(logged.spectral, audit)

    def test_run_once_heartbeats_while_preview_is_running(self):
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            setattr(db, "dsn", "postgresql://fake")  # noqa: B010 - test-only protocol seam
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            initial_claim = claim_next_import_preview_job(db, worker_id="peek")
            assert initial_claim is not None
            assert initial_claim.preview_heartbeat_at is not None
            db.requeue_stale_import_preview_jobs(
                older_than=timedelta(seconds=-1),
                message="test reset",
            )
            heartbeat_seen = threading.Event()

            def preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                baseline = db.get_import_job(initial_claim.id)
                assert baseline is not None
                baseline_heartbeat = baseline.preview_heartbeat_at
                assert baseline_heartbeat is not None
                deadline = time.monotonic() + 0.5
                while time.monotonic() < deadline:
                    current = db.get_import_job(initial_claim.id)
                    assert current is not None
                    if (
                        current.preview_heartbeat_at is not None
                        and current.preview_heartbeat_at > baseline_heartbeat
                    ):
                        heartbeat_seen.set()
                        break
                    time.sleep(0.005)
                # Simulate production: preview persists evidence as a
                # side-effect so the post-measurement gate sees it.
                self._seed_job_candidate_evidence(db, initial_claim.id, source)
                return self._preview(
                    "would_import",
                    reason="import",
                    source_path=source,
                )

            with (
                patch("scripts.import_preview_worker.PipelineDB",
                      side_effect=lambda dsn: db),
                patch(
                    "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                    side_effect=preview,
                ),
            ):
                updated = import_preview_worker.run_once(
                    db,
                    worker_id="preview",
                    heartbeat_interval=0.01,
                    runtime_config=cfg,
                )

            assert updated is not None
            self.assertEqual(updated.preview_status, "evidence_ready")
            self.assertTrue(heartbeat_seen.is_set())

    def test_preview_recovery_loop_requeues_abandoned_running_rows(self):
        from scripts import import_preview_worker

        db = FakePipelineDB()
        dsn = "postgresql://fake"
        setattr(db, "dsn", dsn)  # noqa: B010 - test-only protocol seam
        db.seed_request(make_request_row(id=42, status="wanted"))
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
            ),
        )
        claimed = claim_next_import_preview_job(db, worker_id="dead-worker")
        assert claimed is not None
        old = datetime.now(UTC) - timedelta(hours=2)
        for row in db._import_jobs:
            if row["id"] == claimed.id:
                row["preview_started_at"] = old
                row["preview_heartbeat_at"] = old
                row["updated_at"] = old

        stop = threading.Event()
        thread = threading.Thread(
            target=import_preview_worker.preview_recovery_loop,
            kwargs={
                "dsn": dsn,
                "stop": stop,
                "interval": 0.01,
                "db_factory": lambda dsn: db,
            },
        )
        thread.start()
        try:
            deadline = time.monotonic() + 0.5
            recovered = None
            while time.monotonic() < deadline:
                recovered = db.get_import_job(claimed.id)
                if recovered is not None and recovered.preview_status == "waiting":
                    break
                time.sleep(0.005)
        finally:
            stop.set()
            thread.join(timeout=1.0)

        assert recovered is not None
        self.assertEqual(recovered.preview_status, "waiting")
        self.assertEqual(
            recovered.preview_message,
            import_preview_worker.STALE_PREVIEW_MESSAGE,
        )

    def test_automation_startup_recovery_requires_exact_dead_execution(self):
        from lib.import_execution import (
            CgroupObservation,
            ExecutionLivenessEvidence,
            InvocationObservation,
            ProcessObservation,
        )
        from scripts import import_preview_worker

        class Probe:
            def __init__(
                self,
                expected_lease: ExecutionLeaseSnapshot,
                evidence: ExecutionLivenessEvidence,
            ) -> None:
                self.expected_lease = expected_lease
                self.evidence = evidence

            def observe(
                self,
                lease: ExecutionLeaseSnapshot,
            ) -> ExecutionLivenessEvidence:
                if lease != self.expected_lease:
                    raise AssertionError("startup probe received wrong lease")
                return self.evidence

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        handoff_automation_owner(db, 42)
        lease = _preview_execution_lease("startup-recovery")
        claimed = claim_next_import_preview_job(db, worker_id="old-preview",
        execution_lease=lease,)
        assert claimed is not None

        live = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id=lease.host_boot_id,
            boot_error=None,
            worker=ProcessObservation(
                identity=lease.worker,
                state="exact",
                observed_start_ticks=lease.worker.start_ticks,
                cgroup_path=(
                    "/system.slice/cratedigger-import-preview-worker.service"
                ),
                reason="exact worker",
            ),
            beets=None,
            invocation=InvocationObservation(
                state="exact",
                stored_invocation_id=lease.invocation_id,
                observed_invocation_id=lease.invocation_id,
                control_group=(
                    "/system.slice/cratedigger-import-preview-worker.service"
                ),
                reason="exact invocation",
                active_state="active",
                sub_state="running",
            ),
            cgroup=CgroupObservation(
                state="exact",
                path="/system.slice/cratedigger-import-preview-worker.service",
                member_pids=(lease.worker.pid,),
                reason="exact cgroup",
            ),
        )
        unknown = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id=None,
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
            probe_error="systemd unavailable",
        )
        dead = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="new-boot",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )

        for evidence in (live, unknown):
            with self.subTest(status=evidence.probe_error or "live"):
                recovered = import_preview_worker.recover_running_preview_jobs(
                    db,  # pyright: ignore[reportArgumentType]
                    liveness_probe=Probe(lease, evidence),
                )
                self.assertEqual(recovered, [])
                stored = db.get_import_job(claimed.id)
                assert stored is not None
                self.assertEqual(stored.preview_status, "running")
                self.assertEqual(
                    stored.execution_invocation_id,
                    lease.invocation_id,
                )

        recovered = import_preview_worker.recover_running_preview_jobs(
            db,  # pyright: ignore[reportArgumentType]
            liveness_probe=Probe(lease, dead),
        )
        self.assertEqual([job.id for job in recovered], [claimed.id])
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        self.assertEqual(stored.preview_status, "waiting")
        self.assertIsNone(stored.execution_invocation_id)

        # The periodic age sweep is deliberately non-automation even after
        # the row has been claimed again under a new exact lease.
        reclaimed = claim_next_import_preview_job(db, worker_id="new-preview",
        execution_lease=_preview_execution_lease("new-preview"),)
        assert reclaimed is not None
        self.assertEqual(
            import_preview_worker.recover_abandoned_preview_jobs(
                db,  # pyright: ignore[reportArgumentType]
                older_than=timedelta(seconds=-1),
            ),
            [],
        )
        reclaimed_job = db.get_import_job(reclaimed.id)
        assert reclaimed_job is not None
        self.assertEqual(
            reclaimed_job.preview_status,
            "running",
        )

    def test_confident_reject_fails_job_without_denylisting_source(self):
        """Post-U5: legacy ``confident_reject`` translates to ``measurement_failed``.

        The ``_preview`` helper translates ``confident_reject`` → ``measurement_failed``;
        the worker routes it through U4's self-healing helper, marking the job
        ``status='failed'`` with ``preview_status='measurement_failed'``. No
        denylist write fires (preview measurement failures are infrastructure-
        class, not user-induced).
        """
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        claimed = claim_next_import_preview_job(db, worker_id="preview")
        assert claimed is not None

        with patch(
            "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
            return_value=self._preview("confident_reject", reason="spectral_reject"),
        ):
            updated = import_preview_worker.process_claimed_preview_job(db, claimed)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.preview_status, "measurement_failed")
        self.assertEqual(db.get_denylisted_users(42), [])

    def test_uncertain_preview_fails_without_denylisting(self):
        """Post-U5: legacy ``uncertain`` translates to ``measurement_failed``.

        U4's self-healing helper writes a ``download_log`` row with
        ``outcome='measurement_failed'`` and finalizes the parent request to
        ``wanted`` so the poll loop's active-import-job guard releases.
        """
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        claimed = claim_next_import_preview_job(db, worker_id="preview")
        assert claimed is not None

        with patch(
            "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
            return_value=self._preview("uncertain", reason="path_missing"),
        ):
            updated = import_preview_worker.process_claimed_preview_job(db, claimed)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.preview_status, "measurement_failed")
        self.assertEqual(db.get_denylisted_users(42), [])

    def test_measurement_failure_prepares_have_before_terminal_then_enriches(self):
        """Every eligible failure completes the same HAVE lifecycle."""
        from scripts import import_preview_worker

        order: list[str] = []

        class RecordingDB(FakePipelineDB):
            def persist_preview_terminal_outcome(self, command: Any) -> Any:
                order.append("terminal")
                return super().persist_preview_terminal_outcome(command)

        db = RecordingDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            status="unsearchable",
        ))
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force_import:failure-have-lifecycle",
            payload={"download_log_id": 1, "failed_path": "/tmp/corrupt-audio"},
        )
        claimed = claim_next_import_preview_job(db, worker_id="preview")
        assert claimed is not None
        assert _HERMETIC_BEETS_PAIR is not None
        cfg = CratediggerConfig(
            beets_library_db=_HERMETIC_BEETS_PAIR[0],
            beets_directory=_HERMETIC_BEETS_PAIR[1],
        )

        def prepare(db_arg: Any, **kwargs: Any) -> str:
            self.assertIs(db_arg, db)
            self.assertEqual(kwargs, {
                "request_id": 42,
                "mb_release_id": "mbid-42",
                "quality_ranks": cfg.quality_ranks,
                "beets_library_root": cfg.beets_directory,
            })
            order.append("prepare")
            return "ready"

        def enrich(db_arg: Any, **kwargs: Any) -> str:
            self.assertIs(db_arg, db)
            self.assertEqual(kwargs, {
                "request_id": 42,
                "mb_release_id": "mbid-42",
                "quality_ranks": cfg.quality_ranks,
                "beets_library_root": cfg.beets_directory,
            })
            order.append("enrich")
            return "complete"

        with patch(
            "scripts.import_preview_worker.read_runtime_config",
            return_value=cfg,
        ):
            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                preview_fn=lambda _db, _job: self._preview(
                    "measurement_failed",
                    reason="decoder rejected corrupt audio",
                    source_path="/tmp/corrupt-audio",
                ),
                prepare_failure_have_fn=prepare,
                enrich_failure_have_fn=enrich,
            )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(order, ["prepare", "terminal", "enrich"])
        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(
            db.download_logs[0].error_message,
            "decoder rejected corrupt audio",
        )

    def test_have_lifecycle_errors_never_suppress_measurement_terminal(self):
        """Config, preparation, and enrichment are all fail-soft."""
        from scripts import import_preview_worker

        for failing_stage in ("config", "prepare", "enrich"):
            with self.subTest(failing_stage=failing_stage):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=42,
                    mb_release_id="mbid-42",
                    status="unsearchable",
                ))
                db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=42,
                    dedupe_key=f"force_import:failure-have-{failing_stage}",
                    payload={"download_log_id": 1, "failed_path": "/tmp/corrupt-audio"},
                )
                claimed = claim_next_import_preview_job(db, worker_id="preview")
                assert claimed is not None
                assert _HERMETIC_BEETS_PAIR is not None
                cfg = CratediggerConfig(
                    beets_library_db=_HERMETIC_BEETS_PAIR[0],
                    beets_directory=_HERMETIC_BEETS_PAIR[1],
                )
                prepare = MagicMock(return_value="ready")
                enrich = MagicMock(return_value="complete")
                config_error = (
                    RuntimeError("config unavailable")
                    if failing_stage == "config"
                    else None
                )
                if failing_stage == "prepare":
                    prepare.side_effect = RuntimeError("prepare crashed")
                if failing_stage == "enrich":
                    enrich.side_effect = RuntimeError("enrich crashed")

                with patch(
                    "scripts.import_preview_worker.read_runtime_config",
                    return_value=cfg,
                    side_effect=config_error,
                ), patch("scripts.import_preview_worker.logger.warning"):
                    updated = import_preview_worker.process_claimed_preview_job(
                        db,
                        claimed,
                        preview_fn=lambda _db, _job: self._preview(
                            "measurement_failed",
                            reason="decode failure",
                        ),
                        prepare_failure_have_fn=prepare,
                        enrich_failure_have_fn=enrich,
                    )

                assert updated is not None
                self.assertEqual(updated.status, "failed")
                self.assertEqual(updated.preview_status, "measurement_failed")
                self.assertEqual(len(db.download_logs), 1)
                self.assertEqual(db.download_logs[0].error_message, "decode failure")
                if failing_stage in ("config", "prepare"):
                    enrich.assert_not_called()

    def test_measurement_failure_without_mbid_skips_have_lifecycle(self):
        """Identity-less failures remain terminal but cannot address HAVE."""
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id=None,
            status="unsearchable",
        ))
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force_import:failure-have-no-mbid",
            payload={"download_log_id": 1, "failed_path": "/tmp/corrupt-audio"},
        )
        claimed = claim_next_import_preview_job(db, worker_id="preview")
        assert claimed is not None
        prepare = MagicMock()
        enrich = MagicMock()

        with patch(
            "scripts.import_preview_worker.read_runtime_config",
        ):
            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                preview_fn=lambda _db, _job: self._preview(
                    "measurement_failed",
                    reason="missing identity",
                ),
                prepare_failure_have_fn=prepare,
                enrich_failure_have_fn=enrich,
            )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(len(db.download_logs), 1)
        prepare.assert_not_called()
        enrich.assert_not_called()

    def test_threaded_worker_exits_nonzero_when_worker_thread_crashes(self):
        from scripts import import_preview_worker

        class ThreadDB:
            def close(self):
                pass

        calls = 0
        calls_lock = threading.Lock()

        def run_once(db, *, worker_id, scan_cursor=None):
            del scan_cursor
            nonlocal calls
            with calls_lock:
                calls += 1
                if calls == 1:
                    raise RuntimeError("db connection died")

        with (
            patch("scripts.import_preview_worker.PipelineDB",
                  side_effect=lambda dsn: ThreadDB()),
            patch("scripts.import_preview_worker.run_once",
                  side_effect=run_once),
            patch("scripts.import_preview_worker.logger.exception"),
            patch("scripts.import_preview_worker.logger.error"),
        ):
            exit_code = import_preview_worker.run_threaded_workers(
                dsn="postgresql://example",
                worker_id="preview-test",
                worker_count=2,
                poll_interval=60.0,
            )

        self.assertEqual(exit_code, 1)

    def test_threaded_worker_treats_db_operational_error_as_transient(self):
        """A dead DB connection raised mid-poll must NOT kill the worker.

        Live failure mode (2026-05-20): PostgreSQL drops the
        preview-worker connection during long idle windows between
        jobs; libpq doesn't notice until the next send, so the next
        ``claim_next_import_preview_job`` raises
        ``psycopg2.OperationalError``. Previously this propagated out of
        ``worker_loop`` into the ``BaseException`` handler, which set
        ``stop`` and crashed the whole process with exit-code 1 — even
        though ``PipelineDB._execute`` now reconnects on subsequent
        calls. Defense in depth: the worker must catch the transient
        error, log it, back off, and keep polling.
        """
        import psycopg2

        from scripts import import_preview_worker

        class ThreadDB:
            def close(self):
                pass

        calls = 0
        calls_lock = threading.Lock()
        stop_holder: dict[str, Any] = {}

        def run_once(db, *, worker_id, scan_cursor=None):
            del scan_cursor
            nonlocal calls
            with calls_lock:
                calls += 1
                current = calls
            if current == 1:
                raise psycopg2.OperationalError(
                    "server closed the connection unexpectedly"
                )
            # On the second iteration, stop the workers so the test
            # terminates. We grab the live ``stop`` event via the
            # ``run_threaded_workers`` frame for visibility.
            stop = stop_holder.get("stop")
            if stop is not None:
                stop.set()

        # Capture the ``stop`` event from inside ``run_threaded_workers``
        # by monkeypatching ``threading.Event``.
        real_event = threading.Event

        def capturing_event():
            ev = real_event()
            stop_holder.setdefault("stop", ev)
            return ev

        with (
            patch("scripts.import_preview_worker.PipelineDB",
                  side_effect=lambda dsn: ThreadDB()),
            patch("scripts.import_preview_worker.run_once",
                  side_effect=run_once),
            patch("scripts.import_preview_worker.threading.Event",
                  side_effect=capturing_event),
            patch("scripts.import_preview_worker.logger.warning"),
            patch("scripts.import_preview_worker.logger.exception"),
            patch("scripts.import_preview_worker.logger.error"),
        ):
            exit_code = import_preview_worker.run_threaded_workers(
                dsn="postgresql://example",
                worker_id="preview-test",
                worker_count=1,
                poll_interval=0.01,
            )

        self.assertEqual(exit_code, 0)
        # We saw at least the transient raise + one post-recover poll.
        self.assertGreaterEqual(calls, 2)

    def test_threaded_worker_reuses_one_scan_cursor_across_polls(self):
        """Each long-lived preview thread retains its own rotating cursor."""
        from scripts import import_preview_worker

        class PollProbeComplete(RuntimeError):
            pass

        class ThreadDB:
            def close(self) -> None:
                return None

        observed: list[object] = []

        def observe_poll(
            _db: object,
            *,
            worker_id: str,
            scan_cursor: object,
        ) -> None:
            del worker_id
            observed.append(scan_cursor)
            if len(observed) == 2:
                raise PollProbeComplete

        with (
            patch(
                "scripts.import_preview_worker.PipelineDB",
                side_effect=lambda _dsn: ThreadDB(),
            ),
            patch(
                "scripts.import_preview_worker.run_once",
                side_effect=observe_poll,
            ),
            patch("scripts.import_preview_worker.logger.exception"),
            patch("scripts.import_preview_worker.logger.error"),
        ):
            exit_code = import_preview_worker.run_threaded_workers(
                dsn="postgresql://example",
                worker_id="preview-cursor-probe",
                worker_count=1,
                poll_interval=0,
            )

        self.assertEqual(exit_code, 1)
        assert_long_lived_worker_reuses_cursor(observed)

        def cursor_recreation_mutant() -> list[object]:
            return [
                import_preview_worker._CandidateScanCursor(),
                import_preview_worker._CandidateScanCursor(),
            ]

        with self.assertRaisesRegex(AssertionError, "recreated"):
            assert_long_lived_worker_reuses_cursor(cursor_recreation_mutant())

    def test_main_requeues_running_preview_jobs_on_startup(self):
        """A preview job left in ``preview_status='running'`` by a dead
        worker process must be flipped back to ``waiting`` the moment
        ``main()`` runs — not after the 15-minute stale-recovery
        window. Mirrors the importer's startup self-heal.
        """
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        claimed = claim_next_import_preview_job(db, worker_id="dead-worker")
        assert claimed is not None
        self.assertEqual(claimed.preview_status, "running")

        argv = ["import_preview_worker.py", "--dsn", "postgresql://fake", "--once"]
        with (
            patch("sys.argv", argv),
            patch("scripts.import_preview_worker.PipelineDB",
                  side_effect=lambda dsn: db),
            patch("scripts.import_preview_worker.run_once", return_value=None),
            patch("scripts.import_preview_worker.logger.warning"),
        ):
            exit_code = import_preview_worker.main()

        self.assertEqual(exit_code, 0)
        recovered = db.get_import_job(claimed.id)
        assert recovered is not None
        self.assertEqual(recovered.preview_status, "waiting")
        self.assertIsNone(recovered.preview_worker_id)
        self.assertIsNone(recovered.preview_heartbeat_at)


class TestImportPreviewWorkerFrontGate(unittest.TestCase):
    """U1: worker short-circuits measurement when stored candidate evidence
    already passes the snapshot guard.

    Covers AE4 (re-claim of valid evidence skips measurement) for both
    force and automation job types.
    """

    def _seed_evidence_for_job(
        self,
        db: object,
        job_id: int,
        source_path: str,
    ) -> None:
        _seed_candidate_for_import_job(
            db, job_id,
            mb_release_id=f"mbid-frontgate-job-{job_id}",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )

    def _seed_evidence_for_download_log(
        self,
        db: FakePipelineDB,
        download_log_id: int,
        source_path: str,
    ) -> None:
        _seed_candidate_for_download_log(
            db, download_log_id,
            mb_release_id=f"mbid-frontgate-dl-{download_log_id}",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )

    def test_force_job_valid_evidence_skips_measurement(self):
        """AE4 force: matching snapshot + valid evidence → no measurement."""
        from lib.beets_db import AlbumInfo
        from lib.quality import SpectralAnalysisDetail
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg), \
             tempfile.TemporaryDirectory() as existing:
            for root in (source, existing):
                with open(os.path.join(root, "01.mp3"), "wb") as handle:
                    handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            _seed_current_for_request(
                db,
                42,
                mb_release_id="test-mbid-0042",
                source_path=existing,
                files=snapshot_audio_files(existing),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=122,
                    avg_bitrate_kbps=127,
                    median_bitrate_kbps=127,
                    format="Opus",
                    spectral_grade="likely_transcode",
                    was_converted_from="flac",
                    spectral_subject="source",
                    spectral_provenance="carried",
                ),
                codec="opus",
                container="opus",
                storage_format="Opus",
            )
            fake_beets = FakeBeetsDB()
            fake_beets.set_album_info("test-mbid-0042", AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=127,
                avg_bitrate_kbps=127,
                median_bitrate_kbps=127,
                is_cbr=True,
                album_path=existing,
                format="Opus",
            ))
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            # Seed download_log_candidate evidence — force path uses it.
            self._seed_evidence_for_download_log(db, download_log_id, source)

            audit_calls: list[str] = []
            def analyze(path: str) -> SpectralAnalysisDetail:
                audit_calls.append(path)
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade="likely_transcode",
                )

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
            ) as preview, patch(
                "lib.measurement.measure_preimport_state",
            ) as preimport, patch(
                "lib.beets_db.BeetsDB",
                lambda *_args, **_kwargs: fake_beets,
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    spectral_detail_analyzer=analyze,
                    runtime_config=cfg,
                )

        preview.assert_not_called()
        preimport.assert_not_called()
        self.assertEqual(
            audit_calls,
            [],
            "matching candidate evidence must not trigger another source scan",
        )
        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )
        preview_result = ImportResult.from_dict(cast(
            dict[str, Any],
            updated.preview_result["import_result"],
        ))
        assert preview_result.spectral.existing is not None
        self.assertEqual(
            preview_result.spectral.existing.grade,
            "likely_transcode",
        )
        self.assertIsNotNone(updated.importable_at)

    def test_reused_candidate_fails_when_have_enrichment_loses_authority(self):
        """The front gate cannot reinterpret stale HAVE as library absence."""
        from lib.quality_evidence import EvidenceBuildResult
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, status="wanted"))
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            self._seed_evidence_for_download_log(db, download_log_id, source)

            def stale_current(*_args: Any, **_kwargs: Any):
                return EvidenceBuildResult(
                    None,
                    "stale",
                    "current files changed during V0 probe",
                )

            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                prepare_failure_have_fn=(
                    lambda *_args, **_kwargs: "no_current_evidence"
                ),
                current_evidence_loader=stale_current,
                runtime_config=cfg,
            )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.preview_status, "measurement_failed")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result["decision"],
            "current_evidence_failed",
        )

    def test_reused_candidate_fails_when_have_authority_loader_raises(self):
        """An authority adapter exception cannot authorize candidate reuse."""
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, status="wanted"))
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            self._seed_evidence_for_download_log(db, download_log_id, source)

            def raising_current(*_args: Any, **_kwargs: Any):
                raise RuntimeError("Beets authority unavailable")

            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                prepare_failure_have_fn=(
                    lambda *_args, **_kwargs: "no_current_evidence"
                ),
                current_evidence_loader=raising_current,
                runtime_config=cfg,
            )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.preview_status, "measurement_failed")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result["decision"],
            "current_evidence_failed",
        )
        self.assertIn(
            "Beets authority unavailable",
            str(updated.preview_result["detail"]),
        )

    def test_reused_evidence_reuses_complete_matching_have(self):
        """Candidate and HAVE reuse are independently snapshot-authorized."""
        from lib.beets_db import AlbumInfo
        from lib.measurement import ExistingSpectralAuditLookup
        from lib.quality import SpectralAnalysisDetail
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg), \
             tempfile.TemporaryDirectory() as existing:
            for root in (source, existing):
                with open(os.path.join(root, "01.mp3"), "wb") as handle:
                    handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, mb_release_id="mbid-42"))
            _seed_current_for_request(
                db,
                42,
                mb_release_id="mbid-42",
                source_path=existing,
                files=snapshot_audio_files(existing),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    spectral_grade="genuine",
                    spectral_bitrate_kbps=192,
                    spectral_subject="installed",
                    spectral_provenance="measured",
                    was_converted_from=None,
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            self._seed_evidence_for_download_log(db, download_log_id, source)
            calls: list[str] = []
            fake_beets = FakeBeetsDB()
            fake_beets.set_album_info("mbid-42", AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                is_cbr=True,
                album_path=existing,
                format="MP3",
            ))

            def analyze(path: str) -> SpectralAnalysisDetail:
                calls.append(path)
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade="suspect" if path == existing else "genuine",
                    bitrate_kbps=128 if path == existing else None,
                )

            with patch(
                "lib.beets_db.BeetsDB",
                lambda *_args, **_kwargs: fake_beets,
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    spectral_detail_analyzer=analyze,
                    existing_spectral_resolver=lambda _mbid: (
                        ExistingSpectralAuditLookup(path=existing)
                    ),
                    runtime_config=cfg,
                )

        self.assertEqual(
            calls,
            [],
            "matching complete HAVE evidence must skip spectral analysis",
        )
        assert updated is not None and updated.preview_result is not None
        import_result = ImportResult.from_dict(cast(
            dict[str, Any],
            updated.preview_result["import_result"],
        ))
        assert import_result.spectral.existing is not None
        self.assertEqual(import_result.spectral.existing.grade, "genuine")
        self.assertEqual(import_result.spectral.existing.bitrate_kbps, 192)

    def test_reused_evidence_persists_missing_have_spectral(self):
        """Front-gate reuse must make its HAVE scan durable pre-decision.

        download_log 37206 (French Quarter): the reuse fast path scanned
        the installed album for the audit payload but never persisted the
        result, so the importer's decision ran with a spectrally blind
        HAVE side and called a ~96k transcode an upgrade.
        """
        from lib.beets_db import AlbumInfo
        from lib.measurement import ExistingSpectralAuditLookup
        from lib.quality import SpectralAnalysisDetail
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg), \
             tempfile.TemporaryDirectory() as existing:
            for root in (source, existing):
                with open(os.path.join(root, "01.mp3"), "wb") as handle:
                    handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, mb_release_id="mbid-42"))
            _seed_current_for_request(
                db,
                42,
                mb_release_id="mbid-42",
                source_path=existing,
                files=snapshot_audio_files(existing),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    is_cbr=True,
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            fake_beets = FakeBeetsDB()
            fake_beets.set_album_info("mbid-42", AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                is_cbr=True,
                album_path=existing,
                format="MP3",
            ))
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            self._seed_evidence_for_download_log(db, download_log_id, source)

            def analyze(path: str) -> SpectralAnalysisDetail:
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade="suspect" if path == existing else "genuine",
                    bitrate_kbps=128 if path == existing else None,
                )

            with patch(
                "lib.beets_db.BeetsDB",
                lambda *_args, **_kwargs: fake_beets,
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    spectral_detail_analyzer=analyze,
                    existing_spectral_resolver=lambda _mbid: (
                        ExistingSpectralAuditLookup(path=existing)
                    ),
                    runtime_config=cfg,
                )

            linked_id = db.get_request_current_evidence_id(42)
            linked = db.load_album_quality_evidence_by_id(linked_id)

        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert linked is not None
        self.assertEqual(linked.measurement.spectral_grade, "suspect")
        self.assertEqual(linked.measurement.spectral_bitrate_kbps, 128)

    def test_reused_evidence_never_overwrites_present_have_spectral(self):
        """A fresh audit scan must not clobber persisted HAVE provenance."""
        from lib.measurement import ExistingSpectralAuditLookup
        from lib.quality import SpectralAnalysisDetail
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg), \
             tempfile.TemporaryDirectory() as existing:
            for root in (source, existing):
                with open(os.path.join(root, "01.mp3"), "wb") as handle:
                    handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, mb_release_id="mbid-42"))
            _seed_current_for_request(
                db,
                42,
                mb_release_id="mbid-42",
                source_path=existing,
                files=snapshot_audio_files(existing),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3",
                    spectral_grade="genuine",
                    spectral_bitrate_kbps=None,
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            self._seed_evidence_for_download_log(db, download_log_id, source)

            def analyze(path: str) -> SpectralAnalysisDetail:
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade="suspect" if path == existing else "genuine",
                    bitrate_kbps=128 if path == existing else None,
                )

            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                spectral_detail_analyzer=analyze,
                existing_spectral_resolver=lambda _mbid: (
                    ExistingSpectralAuditLookup(path=existing)
                ),
                runtime_config=cfg,
            )

            linked_id = db.get_request_current_evidence_id(42)
            linked = db.load_album_quality_evidence_by_id(linked_id)

        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert linked is not None
        self.assertEqual(linked.measurement.spectral_grade, "genuine")
        self.assertIsNone(linked.measurement.spectral_bitrate_kbps)

    def test_have_lookup_failure_does_not_reanalyze_reused_candidate(self):
        """A HAVE lookup failure cannot revoke matching candidate evidence."""
        from lib.measurement import ExistingSpectralAuditLookup
        from lib.quality import SpectralAnalysisDetail
        from scripts import import_preview_worker

        class HaveLookupFailureDB(FakePipelineDB):
            def get_request_current_evidence_id(self, request_id: int):
                del request_id
                raise RuntimeError("current evidence unavailable")

        with _force_preview_source() as (source, cfg):
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = HaveLookupFailureDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            self._seed_evidence_for_download_log(db, download_log_id, source)
            calls: list[str] = []

            def analyze(path: str) -> SpectralAnalysisDetail:
                calls.append(path)
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade="likely_transcode",
                )

            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                spectral_detail_analyzer=analyze,
                existing_spectral_resolver=lambda _mbid: (
                    ExistingSpectralAuditLookup()
                ),
                runtime_config=cfg,
            )

        self.assertEqual(calls, [])
        assert updated is not None
        assert updated.preview_result is not None
        import_result = ImportResult.from_dict(cast(
            dict[str, Any],
            updated.preview_result["import_result"],
        ))
        assert import_result.spectral.candidate is not None
        self.assertEqual(
            import_result.spectral.candidate.grade,
            "genuine",
        )
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "evidence_ready")
        self.assertIsNotNone(updated.importable_at)

    def test_reused_evidence_does_not_call_candidate_analyzer(self):
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            self._seed_evidence_for_download_log(db, download_log_id, source)

            analyzer_calls: list[str] = []

            def raising_analyzer(path: str):
                analyzer_calls.append(path)
                raise RuntimeError("spectral backend unavailable")

            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                spectral_detail_analyzer=raising_analyzer,
                runtime_config=cfg,
            )

        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        self.assertIsNotNone(updated.importable_at)
        assert updated.preview_result is not None
        result = ImportResult.from_dict(
            cast(dict[str, Any], updated.preview_result["import_result"])
        )
        assert result.spectral.candidate is not None
        assert result.spectral.existing is not None
        self.assertEqual(analyzer_calls, [])
        self.assertTrue(result.spectral.candidate.attempted)
        self.assertEqual(result.spectral.candidate.grade, "genuine")
        self.assertIsNone(result.spectral.candidate.error)
        self.assertFalse(result.spectral.existing.attempted)
        self.assertIsNone(result.spectral.existing.error)

    def test_automation_job_valid_evidence_skips_measurement_after_materialization(
        self,
    ) -> None:
        """AE4 automation: exact manifest + matching evidence skips measurement."""
        from lib.download_materialization import Materialized
        from lib.quality import SpectralAnalysisDetail
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            handoff_automation_owner(
                db,
                42,
                state=db.request(42)["active_download_state"],
                canonical_path=staged,
            )
            lease = _preview_execution_lease()
            claimed = claim_next_import_preview_job(db, worker_id="preview",
            execution_lease=lease,)
            assert claimed is not None
            authority = import_preview_worker._AutomationPreviewAuthority(
                request=db.request(42),
                state=ActiveDownloadState.from_raw(
                    db.request(42)["active_download_state"]
                ),
                canonical_path=staged,
            )
            self._seed_evidence_for_job(
                import_preview_worker._AutomationPreviewDB(db, lease),
                claimed.id,
                staged,
            )

            candidate_audit_calls: list[str] = []
            materialize_calls: list[str] = []

            def analyze(path: str):
                candidate_audit_calls.append(path)
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                )

            def materialize(
                *_args: object,
                **_kwargs: object,
            ) -> Materialized:
                materialize_calls.append(staged)
                return Materialized()

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
            ) as preview, patch(
                "lib.measurement.measure_preimport_state",
            ) as preimport:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    spectral_detail_analyzer=analyze,
                    execution_lease=lease,
                    automation_authority=authority,
                    cancellation_token=CancellationToken(),
                    automation_materialize_fn=materialize,
                )

        preview.assert_not_called()
        preimport.assert_not_called()
        self.assertEqual(materialize_calls, [staged])
        self.assertEqual(candidate_audit_calls, [])
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )

    def test_ready_evidence_never_runs_before_canonical_materialization(
        self,
    ) -> None:
        """A rejected manifest cannot be marked importable by stored evidence."""
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 5,
                    }],
                },
            ))
            handoff_automation_owner(
                db,
                42,
                state=db.request(42)["active_download_state"],
                canonical_path=staged,
            )
            lease = _preview_execution_lease("manifest-before-evidence")
            claimed = claim_next_import_preview_job(
                db,
                worker_id="preview",
                execution_lease=lease,
            )
            assert claimed is not None
            authority = import_preview_worker._AutomationPreviewAuthority(
                request=db.request(42),
                state=ActiveDownloadState.from_raw(
                    db.request(42)["active_download_state"]
                ),
                canonical_path=staged,
            )
            self._seed_evidence_for_job(
                import_preview_worker._AutomationPreviewDB(db, lease),
                claimed.id,
                staged,
            )

            materialize_calls: list[str] = []

            def reject_manifest(
                *_args: object,
                **_kwargs: object,
            ) -> None:
                materialize_calls.append(staged)
                raise RuntimeError("incomplete_or_unsafe_canonical")

            token = CancellationToken()
            with db._pin_owner_session(token) as owner_session_identity:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    execution_lease=lease,
                    automation_authority=authority,
                    cancellation_token=token,
                    owner_session_identity=owner_session_identity,
                    automation_materialize_fn=reject_manifest,
                )

        self.assertEqual(materialize_calls, [staged])
        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertIsNone(
            db.request(42)["active_automation_import_job_id"],
        )

    @given(
        variant=st.sampled_from((
            "empty",
            "missing_member",
            "extra_entry",
            "root_symlink",
        )),
    )
    def test_generated_unsafe_existing_canonical_never_materializes(
        self,
        variant: str,
    ) -> None:
        """Exact no-follow manifest authority rejects every unsafe world."""
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.processing_paths import (
            canonical_folder_for_row,
            processing_albums_dir,
        )
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as root:
            processing_dir = os.path.join(root, "processing")
            slskd_download_dir = os.path.join(root, "slskd")
            albums_dir = processing_albums_dir(processing_dir)
            os.mkdir(processing_dir, 0o700)
            os.mkdir(slskd_download_dir, 0o700)
            os.mkdir(albums_dir, 0o700)
            request = make_request_row(
                id=42,
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": "/pending/canonical-path",
                    "files": [
                        {
                            "username": "alice",
                            "filename": "Artist\\Album\\01.flac",
                            "file_dir": "Artist\\Album",
                            "size": 5,
                        },
                        {
                            "username": "alice",
                            "filename": "Artist\\Album\\02.flac",
                            "file_dir": "Artist\\Album",
                            "size": 5,
                        },
                    ],
                },
            )
            state = ActiveDownloadState.from_raw(
                request["active_download_state"]
            )
            canonical_path = canonical_folder_for_row(
                reconstruct_grab_list_entry(request, state),
                albums_dir,
            )
            request["active_download_state"]["current_path"] = canonical_path
            target_path = f"{canonical_path}-target"
            destination = (
                target_path if variant == "root_symlink" else canonical_path
            )
            os.mkdir(destination, 0o700)
            if variant != "empty":
                with open(
                    os.path.join(destination, "01.flac"),
                    "wb",
                ) as handle:
                    handle.write(b"audio")
            if variant in {"extra_entry", "root_symlink"}:
                with open(
                    os.path.join(destination, "02.flac"),
                    "wb",
                ) as handle:
                    handle.write(b"audio")
            if variant == "extra_entry":
                with open(
                    os.path.join(destination, "preview-control.json"),
                    "w",
                    encoding="utf-8",
                ) as handle:
                    handle.write("{}")
            if variant == "root_symlink":
                os.symlink(target_path, canonical_path)

            db = FakePipelineDB()
            db.seed_request(request)
            job = handoff_automation_owner(
                db,
                42,
                state=db.request(42)["active_download_state"],
                canonical_path=canonical_path,
            )
            authority = import_preview_worker._AutomationPreviewAuthority(
                request=db.request(42),
                state=ActiveDownloadState.from_raw(
                    db.request(42)["active_download_state"]
                ),
                canonical_path=canonical_path,
            )

            with self.assertRaisesRegex(RuntimeError, "could not be materialized"):
                import_preview_worker._materialize_automation_authority(
                    db,
                    job,
                    authority,
                    runtime_config=CratediggerConfig(
                        processing_dir=processing_dir,
                        slskd_download_dir=slskd_download_dir,
                    ),
                    cancellation_token=CancellationToken(),
                )

    def test_missing_evidence_falls_through_to_full_measurement(self):
        """No evidence row → worker runs full preview measurement (legacy path)."""
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            # No evidence seeded → front-gate misses → measurement runs.

            # Post-U5: worker-mode preview emits ``evidence_ready`` (not the
            # legacy ``would_import``); the importer reads the evidence and
            # decides.
            preview_result = ImportPreviewResult(
                mode="path",
                verdict="evidence_ready",
                decision="import",
                reason="import",
                stage_chain=["preview:import"],
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                # Simulate production: preview persists candidate evidence
                # and wires the FK chain that the front-gate reads from.
                _seed_candidate_for_import_job(
                    db, claimed.id,
                    mb_release_id="mbid-missing-falls-through",
                    files=snapshot_audio_files(source),
                )
                return preview_result

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    runtime_config=cfg,
                )

        # Front-gate misses (no evidence) → preview is called.
        preview.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        # Provenance reflects the measured path, not the reused path.
        assert updated.preview_result is not None
        self.assertNotEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )

    def test_snapshot_mismatch_falls_through_to_full_measurement(self):
        """Stale snapshot → measurement runs; new evidence replaces stale row."""
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = _force_download_log(db, 42, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            # Seed evidence with files that don't match the on-disk snapshot.
            from lib.quality import AlbumQualityEvidenceFile
            _seed_candidate_for_download_log(
                db, download_log_id,
                mb_release_id="mbid-stale",
                files=[AlbumQualityEvidenceFile(
                    relative_path="stale.mp3",
                    size_bytes=999,
                    mtime_ns=1,
                    extension="mp3",
                    container="mp3",
                    codec="mp3",
                )],
            )

            # Post-U5: worker-mode preview emits ``evidence_ready``.
            preview_result = ImportPreviewResult(
                mode="path",
                verdict="evidence_ready",
                decision="import",
                reason="import",
                stage_chain=["preview:import"],
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                # Simulate production: preview re-measures and persists fresh
                # evidence with the actual on-disk snapshot, rewiring the FK.
                _seed_candidate_for_import_job(
                    db, claimed.id,
                    mb_release_id="mbid-fresh",
                    files=snapshot_audio_files(source),
                )
                return preview_result

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    runtime_config=cfg,
                )

        # Snapshot mismatch → front-gate misses → preview ran.
        preview.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        # The stale evidence row was replaced — the FK now points at fresh
        # content-addressed evidence.
        evidence_id = db.get_import_job_candidate_evidence_id(claimed.id)
        self.assertIsNotNone(evidence_id)
        evidence = db.load_album_quality_evidence_by_id(evidence_id)
        assert evidence is not None
        self.assertEqual(len(evidence.files), 1)
        self.assertEqual(evidence.files[0].relative_path, "01.mp3")

    def test_rolling_stones_force_reuses_all_twelve_unchanged_flacs(self):
        """Live dl 37709: unchanged force-import candidate is measured once."""
        from lib.measurement import ExistingSpectralAuditLookup
        from lib.quality import SpectralAnalysisDetail
        from lib.quality_evidence import EvidenceBuildResult
        from scripts import import_preview_worker

        with _force_preview_source() as (source, cfg):
            for track in range(1, 13):
                with open(
                    os.path.join(source, f"{track:02d}.flac"),
                    "wb",
                ) as handle:
                    handle.write(f"rolling-stones-track-{track}".encode())

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=8883,
                status="wanted",
                mb_release_id="1f9fdeeb-59b4-4751-91b6-be38fb76c380",
                artist_name="The Rolling Stones",
                album_title="The Rolling Stones No. 2",
            ))
            download_log_id = _force_download_log(db, 8883, source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=8883,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="buckwheat8404",
                ),
            )
            claimed = claim_next_import_preview_job(db, worker_id="preview")
            assert claimed is not None
            _seed_candidate_for_download_log(
                db,
                download_log_id,
                mb_release_id="1f9fdeeb-59b4-4751-91b6-be38fb76c380",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=3301,
                    avg_bitrate_kbps=3505,
                    median_bitrate_kbps=3515,
                    format="FLAC",
                    spectral_grade="genuine",
                    spectral_subject="source",
                    spectral_provenance="measured",
                ),
                codec="flac",
                container="flac",
                storage_format="FLAC",
                target_format="opus 128",
            )
            candidate_scans: list[str] = []
            preview_calls = 0

            def analyze(path: str) -> SpectralAnalysisDetail:
                candidate_scans.append(path)
                return SpectralAnalysisDetail(attempted=True, grade="genuine")

            def full_preview(*_args: Any, **_kwargs: Any):
                nonlocal preview_calls
                preview_calls += 1
                raise AssertionError("matching evidence must skip full preview")

            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                spectral_detail_analyzer=analyze,
                existing_spectral_resolver=(
                    lambda _release_id: ExistingSpectralAuditLookup()
                ),
                preview_fn=full_preview,
                current_evidence_loader=(
                    lambda *_args, **_kwargs: EvidenceBuildResult(
                        None,
                        "empty_current",
                        "exact album not in beets",
                    )
                ),
                runtime_config=cfg,
            )

        self.assertEqual(preview_calls, 0)
        self.assertEqual(candidate_scans, [])
        assert updated is not None and updated.preview_result is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        self.assertEqual(updated.preview_result["candidate_status"], "reused")


class TestYoutubeImportJobType(unittest.TestCase):
    """Constant + helper coverage for the YT-rescue ``youtube_import`` job_type.

    Covers U2's import_queue extensions:
    - ``IMPORT_JOB_YOUTUBE`` is registered in ``IMPORT_JOB_TYPES``
    - ``youtube_import_dedupe_key`` is keyed on the download_log id
    - ``youtube_import_payload`` produces the {staged_path, request_id, browse_id}
      shape the U9 dispatcher consumes
    - ``validate_payload`` enforces all three fields are present and typed
    """

    def test_constant_is_in_registered_job_types(self):
        from lib.import_queue import IMPORT_JOB_TYPES, IMPORT_JOB_YOUTUBE
        self.assertIn(IMPORT_JOB_YOUTUBE, IMPORT_JOB_TYPES)
        self.assertEqual(IMPORT_JOB_YOUTUBE, "youtube_import")

    def test_validate_job_type_accepts_youtube_import(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, validate_job_type
        self.assertEqual(
            validate_job_type(IMPORT_JOB_YOUTUBE), IMPORT_JOB_YOUTUBE,
        )

    def test_youtube_import_payload_roundtrip(self):
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            validate_payload,
            youtube_import_payload,
        )
        payload = youtube_import_payload(
            staged_path="/Incoming/auto-import/Artist - Album",
            request_id=42,
            browse_id="MPREb_abc",
            download_log_id=99,
        )
        self.assertEqual(payload, {
            "staged_path": "/Incoming/auto-import/Artist - Album",
            "request_id": 42,
            "browse_id": "MPREb_abc",
            "download_log_id": 99,
        })
        self.assertEqual(validate_payload(IMPORT_JOB_YOUTUBE, payload), payload)

    def test_youtube_import_payload_preserves_positive_ids(self):
        from lib.import_queue import youtube_import_payload
        payload = youtube_import_payload(
            staged_path="/Incoming/auto-import/Artist - Album",
            request_id=42,
            browse_id="MPREb_abc",
            download_log_id=99,
        )
        self.assertIsInstance(payload["request_id"], int)
        self.assertEqual(payload["download_log_id"], 99)

    def test_validate_payload_youtube_rejects_missing_staged_path(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, validate_payload
        with self.assertRaises(msgspec.ValidationError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "request_id": 42, "browse_id": "MPREb_abc",
                "download_log_id": 99,
            })

    def test_validate_payload_youtube_rejects_empty_staged_path(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, validate_payload
        with self.assertRaises(msgspec.ValidationError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "",
                "request_id": 42,
                "browse_id": "MPREb_abc",
                "download_log_id": 99,
            })

    def test_validate_payload_youtube_rejects_missing_request_id(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, validate_payload
        with self.assertRaises(msgspec.ValidationError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "/Incoming/auto-import/x",
                "browse_id": "MPREb_abc",
                "download_log_id": 99,
            })

    def test_validate_payload_youtube_rejects_non_int_request_id(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, validate_payload
        with self.assertRaises(msgspec.ValidationError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": "42",  # str, not int
                "browse_id": "MPREb_abc",
                "download_log_id": 99,
            })

    def test_validate_payload_youtube_rejects_missing_browse_id(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, validate_payload
        with self.assertRaises(msgspec.ValidationError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": 42,
                "download_log_id": 99,
            })

    def test_validate_payload_youtube_rejects_missing_download_log_id(self):
        with self.assertRaises(msgspec.ValidationError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": 42,
                "browse_id": "MPREb_abc",
            })

    def test_validate_payload_youtube_rejects_non_int_download_log_id(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, validate_payload
        with self.assertRaises(msgspec.ValidationError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": 42,
                "browse_id": "MPREb_abc",
                "download_log_id": "99",
            })

    def test_dedupe_key_uses_download_log_id(self):
        from lib.import_queue import youtube_import_dedupe_key
        self.assertEqual(
            youtube_import_dedupe_key(7),
            "youtube_import:download_log:7",
        )
        # Same id ⇒ same key (idempotency).
        self.assertEqual(
            youtube_import_dedupe_key(7), youtube_import_dedupe_key(7),
        )
        # Different id ⇒ different key.
        self.assertNotEqual(
            youtube_import_dedupe_key(7), youtube_import_dedupe_key(8),
        )


class TestExecuteYoutubeImportJob(unittest.TestCase):
    """U9: importer dispatcher for ``youtube_import`` job_type.

    Covers AE7 (happy-path import to terminal state), AE8 (long-tail
    rescue audit chain), AE9 (rescue from ``unsearchable``), the preview-worker
    front-gate path-resolution divergence, no-cooldown-leakage, and
    payload type-validation.

    Test shape mirrors ``TestImporterWorker``: drive the production
    ``importer.process_claimed_job`` entry point with a ``FakePipelineDB``,
    seed a queued YT job, mark it importable so the importer can claim it,
    and patch the leaf seam (``lib.download_processing.process_completed_album``) so
    we can assert dispatcher behaviour without exercising the full beets
    pipeline (which is covered by its own integration slices).
    """

    def _mark_importable(self, db: FakePipelineDB, job: Any) -> Any:
        updated = db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        assert updated is not None
        return updated

    def _enqueue_youtube_job(
        self,
        db: FakePipelineDB,
        *,
        request_id: int,
        staged_path: str,
        browse_id: str = "MPREb_abc",
        download_log_id: int = 1,
    ) -> Any:
        from lib.import_queue import (
            youtube_import_dedupe_key,
            youtube_import_payload,
        )
        return db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=request_id,
            dedupe_key=youtube_import_dedupe_key(download_log_id),
            payload=youtube_import_payload(
                staged_path=staged_path,
                request_id=request_id,
                browse_id=browse_id,
                download_log_id=download_log_id,
            ),
        )

    def _claim(self, db: FakePipelineDB) -> Any:
        claimed = claim_next_import_job(db, worker_id="worker")
        assert claimed is not None
        return claimed

    def test_happy_path_drives_through_pipeline_and_returns_success(self):
        """AE7: a YT job dispatched through importer.process_claimed_job
        runs the existing per-job pipeline (process_completed_album)
        with the staged path coming from the payload, NOT from
        active_download_state."""
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.opus"), "wb") as fp:
                fp.write(b"audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                # No active_download_state — YT path must NOT depend on it.
                active_download_state=None,
                artist_name="Test Artist",
                album_title="Test Album",
                mb_release_id="mbid-yt-happy",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=11,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download_processing.process_completed_album",
                return_value=CompletionDispatched(
                    outcome=DispatchOutcome(True, "Imported by dispatch")),
            ) as proc:
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        # Pipeline invoked exactly once with the import_job_id forwarded.
        proc.assert_called_once()
        self.assertEqual(proc.call_args.kwargs["import_job_id"], job.id)
        # The GrabListEntry passed in is sourced from the request row +
        # YT payload's staged_path, not from active_download_state.
        entry_arg = proc.call_args.args[0]
        self.assertEqual(entry_arg.import_folder, staged)
        self.assertEqual(entry_arg.db_request_id, 42)
        self.assertEqual([f.filename for f in entry_arg.files], ["01.opus"])
        self.assertEqual([f.username for f in entry_arg.files], [""])
        # Terminal queue state reflects the DispatchOutcome.
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "Imported by dispatch")

    def test_youtube_staged_audio_manifest_uses_real_files(self):
        """Real dispatcher + process_completed_album reaches beets reject
        without the manifest guard classifying staged YT audio as
        untracked_audio.
        """
        from scripts import importer

        with tempfile.TemporaryDirectory() as tmpdir:
            root = os.path.abspath(tmpdir)
            staged = os.path.join(root, "yt-staged")
            os.makedirs(staged)
            for name in ("01.opus", "02.opus"):
                with open(os.path.join(staged, name), "wb") as fp:
                    fp.write(b"audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                artist_name="Test Artist",
                album_title="Test Album",
                mb_release_id="mbid-yt-real-manifest",
                search_filetype_override="opus",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=110,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)
            cfg = CratediggerConfig(
                beets_validation_enabled=True,
                beets_harness_path="/unused",
                beets_staging_dir=os.path.join(root, "Incoming"),
                slskd_download_dir=os.path.join(root, "slskd"),
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)

            with patch(
                "lib.beets.beets_validate",
                return_value=ValidationResult(
                    valid=False,
                    scenario="high_distance",
                    detail="distance=1.0",
                    target_mbid="mbid-yt-real-manifest",
                ),
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=ctx,
                )

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            source = ctx.pipeline_db_source
            self.assertEqual(len(source.reject_and_requeue_calls), 1)
            rejected = source.reject_and_requeue_calls[0]["bv_result"]
            self.assertEqual(rejected.scenario, "high_distance")
            self.assertNotEqual(rejected.scenario, "untracked_audio")
            self.assertFalse(
                os.path.exists(
                    os.path.join(root, "failed_imports", "untracked_audio")
                )
            )

    def test_happy_path_finalizes_request_to_imported_via_rescue(self):
        """AE7: when process_completed_album returns True (legacy non-
        DispatchOutcome path), the dispatcher reports success and the
        request row remains untouched here — the actual status flip
        happens inside the dispatch path via finalize_request →
        mark_imported_with_rescue. We assert success-mapping without
        re-deriving the status flip (which is covered separately by
        TestRescueAuditChain below)."""
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                mb_release_id="mbid-yt-true",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=12,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download_processing.process_completed_album",
                return_value=Completed(),
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "YouTube import processing completed")

    def test_rescue_audit_chain_wanted_to_imported_via_finalize_request(self):
        """AE8: a YT import on a previously-unfindable request populates
        ``rescued_at`` + ``prior_unfindable_category`` atomically. The
        dispatcher invokes the pipeline; the pipeline invokes
        ``finalize_request`` which routes to ``mark_imported_with_rescue``.

        We drive the rescue capture seam directly by having the patched
        pipeline call ``finalize_request(to_imported)`` against the fake DB,
        which mirrors what the production pipeline does inside
        ``dispatch_import_from_db`` on import success.
        """
        from lib import transitions
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                mb_release_id="mbid-yt-rescue",
                unfindable_category="wrong_pressing_available",
                unfindable_categorised_at=datetime(
                    2026, 5, 1, tzinfo=UTC),
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=13,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            def _run_real_finalize(*args: Any, **kwargs: Any) -> CompletionDispatched:
                # Mirror the production seam: dispatch_import_from_db
                # would call finalize_request(to_imported) on auto-import
                # success. That call routes to mark_imported_with_rescue.
                transitions.finalize_request(
                    cast(Any, db),
                    42,
                    transitions.RequestTransition.to_imported(
                        from_status="wanted",
                    ),
                )
                return CompletionDispatched(outcome=DispatchOutcome(True, "Imported"))

            with patch(
                "lib.download_processing.process_completed_album",
                side_effect=_run_real_finalize,
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        # The job completed successfully…
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        # …and the request row carries the long-tail-rescue audit chain.
        row = db.get_request(42)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        self.assertIsNotNone(row["rescued_at"])
        self.assertEqual(
            row["prior_unfindable_category"], "wrong_pressing_available")
        # Current category is cleared (the rescue IS the resolution).
        self.assertIsNone(row["unfindable_category"])

    def test_rescue_from_unsearchable_transitions_to_imported(self):
        """AE9: a request started ``unsearchable`` transitions to
        ``imported`` through the same single source-agnostic
        write site (``mark_imported_with_rescue``)."""
        from lib import transitions
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="unsearchable",
                active_download_state=None,
                mb_release_id="mbid-yt-from-unsearchable",
                unfindable_category="album_absent_artist_present",
                unfindable_categorised_at=datetime(
                    2026, 5, 1, tzinfo=UTC),
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=14,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            def _run_real_finalize(*args: Any, **kwargs: Any) -> CompletionDispatched:
                transitions.finalize_request(
                    cast(Any, db),
                    42,
                    transitions.RequestTransition.to_imported(
                        from_status="unsearchable",
                    ),
                )
                return CompletionDispatched(
                    outcome=DispatchOutcome(
                        True, "Imported from unsearchable"
                    ))

            with patch(
                "lib.download_processing.process_completed_album",
                side_effect=_run_real_finalize,
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "completed")
        row = db.get_request(42)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        self.assertIsNotNone(row["rescued_at"])
        self.assertEqual(
            row["prior_unfindable_category"], "album_absent_artist_present")

    def test_no_cooldown_leakage_on_wrong_match_reject(self):
        """The dispatcher running through the wrong-matches reject path
        does not denylist any user or apply any cooldown — YT produces
        no peer to attribute failures to.
        """
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                mb_release_id="mbid-yt-reject",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=15,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            # Simulate a wrong-matches reject from the pipeline.
            with patch(
                "lib.download_processing.process_completed_album",
                return_value=CompletionDispatched(outcome=DispatchOutcome(
                    False, "Rejected: high_distance")),
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        # No slskd peer means nothing to denylist and nothing to cool.
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.user_cooldowns, {})

    def test_no_cooldown_leakage_on_quality_pipeline_reject(self):
        """Same invariant on the quality-pipeline reject path: empty
        files list ⇒ no usernames to attribute ⇒ denylist + cooldowns
        remain untouched."""
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                mb_release_id="mbid-yt-quality-reject",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=16,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download_processing.process_completed_album",
                return_value=CompletionDispatched(outcome=DispatchOutcome(
                    False,
                    "Quality pipeline rejected",
                    code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                )),
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.user_cooldowns, {})

    def test_dispatcher_does_not_read_or_write_active_download_state(self):
        """KTD1: the YT dispatcher never touches active_download_state.
        The path source-of-truth is the payload's ``staged_path``.

        Seed a row with a DIFFERENT active_download_state.current_path
        than the payload's staged_path; assert the pipeline gets the
        payload's path, and assert the row's active_download_state is
        unchanged after dispatch.
        """
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            unrelated_state = {
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "current_path": "/some/unrelated/slskd/path",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            }
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                # Even if some unrelated automation state happens to be
                # populated, the YT path must ignore it.
                active_download_state=unrelated_state,
                mb_release_id="mbid-yt-ktd1",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=17,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download_processing.process_completed_album",
                return_value=CompletionDispatched(
                    outcome=DispatchOutcome(True, "ok")),
            ) as proc:
                importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        # The entry handed to the pipeline carries the YT staged_path,
        # NOT the unrelated active_download_state current_path.
        entry_arg = proc.call_args.args[0]
        self.assertEqual(entry_arg.import_folder, staged)
        self.assertEqual(entry_arg.files, [])
        # And the row's active_download_state is untouched.
        row = db.get_request(42)
        assert row is not None
        self.assertEqual(
            row.get("active_download_state"), unrelated_state)

    def test_deferred_message_carries_detail(self):
        """Issue #859: mirrors
        ``TestImporterWorker.test_automation_job_deferred_message_carries_detail``
        for the YouTube caller — both route through the same
        ``_dispatch_outcome_from_completion`` mapper."""
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42, status="wanted", mb_release_id="mbid-yt-deferred",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=19,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download_processing.process_completed_album",
                return_value=CompletionDeferred(
                    detail="incomplete_or_unsafe_canonical"),
            ):
                updated = importer.process_claimed_job(
                    db,  # pyright: ignore[reportArgumentType]
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(
            updated.message,
            "YouTube import was deferred or requires manual recovery: "
            "incomplete_or_unsafe_canonical",
        )

    def test_failed_message_carries_reason(self):
        """Issue #865: YouTube twin of the failed-reason mapper rule."""
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42, status="wanted", mb_release_id="mbid-yt-failed",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=19,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download_processing.process_completed_album",
                return_value=CompletionFailed(reason="staged_path_missing"),
            ):
                updated = importer.process_claimed_job(
                    db,  # pyright: ignore[reportArgumentType]
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(
            updated.message,
            "YouTube import processing failed: staged_path_missing",
        )

    def test_missing_request_returns_failed_dispatch_outcome(self):
        from scripts import importer

        db = FakePipelineDB()
        # Enqueue against a request_id that has no row.
        job = self._enqueue_youtube_job(
            db, request_id=999, staged_path="/Incoming/auto-import/x",
            download_log_id=18,
        )
        self._mark_importable(db, job)
        claimed = self._claim(db)

        updated = importer.process_claimed_job(
            cast(Any, db),
            claimed,
            ctx=object(),
        )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertIn("not found", str(updated.message))

    def test_missing_request_id_returns_failed_dispatch_outcome(self):
        from lib.import_queue import ImportJob
        from scripts import importer

        # Construct an ImportJob with request_id=None directly — this is
        # a defensive guard for a path the production codepaths shouldn't
        # produce (the YT submit guards block it upstream), but the
        # dispatcher must still fail-fast rather than crash.
        db = FakePipelineDB()
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": None,
            "dedupe_key": "youtube_import:download_log:99",
            "payload": {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": 1,
                "browse_id": "MPREb_abc",
                "download_log_id": 99,
            },
        })

        outcome = importer.execute_youtube_import_job(
            cast(Any, db), job, ctx=object(),
        )

        self.assertFalse(outcome.success)
        self.assertIn("request_id", outcome.message)

    def test_malformed_payload_is_rejected_at_import_job_row_boundary(self):
        """Missing required YT fields never reach an importer consumer."""
        from lib.import_queue import ImportJob

        with self.assertRaises(msgspec.ValidationError):
            ImportJob.from_row({
                "id": 1,
                "job_type": IMPORT_JOB_YOUTUBE,
                "status": "queued",
                "request_id": 42,
                "dedupe_key": "youtube_import:download_log:42",
                "payload": {"request_id": 42, "browse_id": "MPREb_abc"},
            })

    def test_malformed_payload_with_wrong_type_for_request_id(self):
        """Wrong type at the wire seam (request_id is str, not int) is
        a ValidationError, not a silent coerce."""
        from lib.import_queue import ImportJob

        with self.assertRaises(msgspec.ValidationError):
            ImportJob.from_row({
                "id": 1,
                "job_type": IMPORT_JOB_YOUTUBE,
                "status": "queued",
                "request_id": 42,
                "dedupe_key": "youtube_import:download_log:43",
                "payload": {
                    "staged_path": "/Incoming/auto-import/x",
                    "request_id": "42",
                    "browse_id": "MPREb_abc",
                },
            })


class TestFrontGateSourcePathYoutubeImport(unittest.TestCase):
    """U9: preview-worker front-gate divergence between job_types.

    ``_front_gate_source_path`` is the cheap path-derivation helper the
    preview worker uses to test stored candidate evidence's snapshot
    against the current source location before deciding whether to skip
    measurement. For YT jobs the path comes from the payload; for
    automation jobs it comes from ``active_download_state``. The two
    branches are independent and the YT branch never reads
    ``active_download_state`` (KTD1).

    Also covers ``_preview_input`` parity — the worker can fall through
    to full measurement for a YT job by reading the same payload
    seam (not active_download_state).
    """

    def test_youtube_job_returns_payload_staged_path(self):
        from lib.import_queue import (
            ImportJob,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": youtube_import_dedupe_key(99),
            "payload": youtube_import_payload(
                staged_path="/Incoming/auto-import/Artist - Album",
                request_id=42,
                browse_id="MPREb_abc",
                download_log_id=99,
            ),
        })

        result = import_preview_worker._front_gate_source_path(
            db,
            job,
        )

        self.assertEqual(result, "/Incoming/auto-import/Artist - Album")

    def test_youtube_job_does_not_read_active_download_state(self):
        """KTD1 at the front-gate: even if active_download_state has a
        current_path populated, the YT branch returns the payload's
        staged_path."""
        from lib.import_queue import (
            ImportJob,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )
        from scripts import import_preview_worker

        db = FakePipelineDB()
        # Seed a row with active_download_state pointing somewhere else.
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "current_path": "/totally/different/path",
                "files": [],
            },
        ))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": youtube_import_dedupe_key(99),
            "payload": youtube_import_payload(
                staged_path="/Incoming/auto-import/Artist - Album",
                request_id=42,
                browse_id="MPREb_abc",
                download_log_id=99,
            ),
        })

        authority = import_preview_worker._AutomationPreviewAuthority(
            request=db.request(42),
            state=ActiveDownloadState.from_raw(
                db.request(42)["active_download_state"]
            ),
            canonical_path="/slskd/Test Artist - Test Album",
        )
        result = import_preview_worker._front_gate_source_path(
            db,
            job,
            automation_authority=authority,
        )

        # Returns the payload path, not the active_download_state path.
        self.assertEqual(result, "/Incoming/auto-import/Artist - Album")

    def test_automation_branch_uses_authoritative_current_path(self):
        """Automation keeps using active_download_state, not YT payload."""
        from lib.import_queue import ImportJob, automation_import_dedupe_key
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "current_path": "/slskd/Test Artist - Test Album",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_AUTOMATION,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": automation_import_dedupe_key(42),
            "payload": {},
        })

        authority = import_preview_worker._AutomationPreviewAuthority(
            request=db.request(42),
            state=ActiveDownloadState.from_raw(
                db.request(42)["active_download_state"]
            ),
            canonical_path="/slskd/Test Artist - Test Album",
        )
        result = import_preview_worker._front_gate_source_path(
            db,
            job,
            automation_authority=authority,
        )

        self.assertEqual(result, "/slskd/Test Artist - Test Album")

    @given(
        leaf=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyz0123456789-_",
            min_size=1,
            max_size=24,
        ),
        authority_captured=st.booleans(),
    )
    def test_automation_front_gate_generated_never_derives_owner_path(
        self,
        leaf: str,
        authority_captured: bool,
    ) -> None:
        """Known-bad path derivation loses to the captured owner snapshot."""
        from lib.import_queue import ImportJob, automation_import_dedupe_key
        from scripts import import_preview_worker

        current_path = f"/processing/albums/{leaf}"
        state = ActiveDownloadState.from_raw({
            "filetype": "flac",
            "enqueued_at": "2026-04-25T00:00:00+00:00",
            "current_path": current_path,
            "files": [{
                "username": "alice",
                "filename": "Artist\\Album\\01.flac",
                "file_dir": "Artist\\Album",
                "size": 123,
            }],
        })
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="processing",
            active_download_state=msgspec.to_builtins(state),
        ))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_AUTOMATION,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": automation_import_dedupe_key(42),
            "payload": {},
        })
        authority = (
            import_preview_worker._AutomationPreviewAuthority(
                request=db.request(42),
                state=state,
                canonical_path=current_path,
            )
            if authority_captured
            else None
        )

        actual = import_preview_worker._front_gate_source_path(
            db,
            job,
            automation_authority=authority,
        )
        expected = current_path if authority_captured else None
        self.assertEqual(actual, expected)
        if not authority_captured:
            # Fault injection for the removed behavior: rereading/deriving the
            # row path would have authorized a filesystem read without the
            # lock-and-session snapshot.
            owner_blind_mutant = state.current_path
            self.assertNotEqual(actual, owner_blind_mutant)

    def test_youtube_job_malformed_payload_is_rejected_before_front_gate(self):
        """The preview worker only receives a decoded YT payload struct."""
        from lib.import_queue import ImportJob

        with self.assertRaises(msgspec.ValidationError):
            ImportJob.from_row({
                "id": 1,
                "job_type": IMPORT_JOB_YOUTUBE,
                "status": "queued",
                "request_id": 42,
                "dedupe_key": "youtube_import:download_log:99",
                "payload": {"request_id": 42, "browse_id": "MPREb_abc"},
            })

    def test_preview_input_uses_payload_staged_path_for_youtube(self):
        """The ``_preview_input`` helper (the slow-path measurement seam)
        also reads the YT payload, not active_download_state."""
        from lib.import_queue import (
            ImportJob,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": youtube_import_dedupe_key(99),
            "payload": youtube_import_payload(
                staged_path="/Incoming/auto-import/Artist - Album",
                request_id=42,
                browse_id="MPREb_abc",
                download_log_id=99,
            ),
        })

        result = import_preview_worker._preview_input(
            db, job,
        )

        self.assertEqual(result["path"], "/Incoming/auto-import/Artist - Album")
        self.assertEqual(result["request_id"], 42)
        # The measurement core has no peer notion — no source_username key.
        self.assertNotIn("source_username", result)
        self.assertFalse(result["force"])

    def test_preview_input_never_receives_malformed_youtube_payload(self):
        from lib.import_queue import ImportJob

        with self.assertRaises(msgspec.ValidationError):
            ImportJob.from_row({
                "id": 1,
                "job_type": IMPORT_JOB_YOUTUBE,
                "status": "queued",
                "request_id": 42,
                "dedupe_key": "youtube_import:download_log:99",
                "payload": {"request_id": 42, "browse_id": "MPREb_abc"},
            })


class TestForcePreviewPathAuthority(unittest.TestCase):
    def test_front_gate_uses_db_failed_path_not_payload_and_cleans_snapshot(self) -> None:
        """The force payload is audit metadata, never filesystem authority."""
        from lib.quality_evidence import EvidenceBuildResult
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as parent:
            incoming = os.path.join(parent, "Incoming")
            downloads = os.path.join(parent, "downloads")
            processing = os.path.join(parent, "processing")
            for directory in (incoming, downloads, processing):
                os.mkdir(directory, 0o700)
            os.mkdir(os.path.join(processing, "albums"), 0o700)
            os.mkdir(os.path.join(processing, "preview"), 0o700)
            db_source = os.path.join(
                incoming, "auto-import", "Database", "failed_imports", "Album",
            )
            payload_source = os.path.join(
                incoming, "manual", "Payload", "failed_imports", "Album",
            )
            os.makedirs(payload_source)
            os.makedirs(db_source)
            with open(os.path.join(db_source, "01.mp3"), "wb") as handle:
                handle.write(b"database")
            with open(os.path.join(payload_source, "01.mp3"), "wb") as handle:
                handle.write(b"payload")
            db = FakePipelineDB()
            download_log_id = _force_download_log(db, 42, db_source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=payload_source,
                ),
            )
            cfg = CratediggerConfig(
                slskd_download_dir=downloads,
                beets_staging_dir=incoming,
                processing_dir=processing,
                audio_check_mode="off",
            )
            seen: list[tuple[str, bytes]] = []

            def capture_lookup(
                _db: object,
                *,
                source_path: str,
                download_log_id: int | None = None,
                import_job_id: int | None = None,
            ) -> EvidenceBuildResult:
                del download_log_id, import_job_id
                lookup_path = source_path
                with open(os.path.join(lookup_path, "01.mp3"), "rb") as handle:
                    seen.append((lookup_path, handle.read()))
                return EvidenceBuildResult(None, "missing")

            result, display_path, action_path = import_preview_worker._front_gate_check(
                db,
                job,
                runtime_config=cfg,
                candidate_evidence_loader=capture_lookup,
            )

            self.assertIsNotNone(result)
            self.assertEqual(display_path, db_source)
            self.assertIsNotNone(action_path)
            self.assertEqual(len(seen), 1)
            assert_force_preview_authority(
                lookup_path=seen[0][0],
                db_failed_path=db_source,
                payload_failed_path=payload_source,
                lookup_bytes=seen[0][1],
                expected_db_bytes=b"database",
                preview_root=os.path.join(processing, "albums"),
                preview_children=os.listdir(os.path.join(processing, "preview")),
            )

    def test_force_authority_checker_trips_on_payload_lookup(self) -> None:
        with self.assertRaises(AssertionError):
            assert_force_preview_authority(
                lookup_path="/payload",
                db_failed_path="/database",
                payload_failed_path="/payload",
                lookup_bytes=b"payload",
                expected_db_bytes=b"database",
                preview_root="/preview",
                preview_children=[],
            )


# --- Owned-processor cleanup + terminal stage --------------------------------
#
# Three invariants of the automation branch of ``process_claimed_job``, stated
# before the code:
#
#   1. Every fault the post-execution stage can raise either reaches the
#      caller unchanged (execution/owner-session fail-stop) or is handled — it
#      never escapes and kills the importer process, which drains the shared
#      queue for every request.
#   2. The owned-processor cleanup intent is journalled as an immutable,
#      unamendable plan, so it must refuse an unusable configured quarantine
#      root instead of resolving it against the importer's CWD.
#   3. The automation importer NEVER parks a request for human adjudication. A
#      failure that leaves the world in an unexpected state is recorded as
#      audit evidence (so it reads in Recents) and the request goes straight
#      back to the search pool: the request is the source of truth and the next
#      cycle rebuilds everything derived from it. ``recovery_required`` behind
#      ``status='processing'`` is never a resting state, because ``get_wanted``
#      (``WHERE status = 'wanted'``) would never select that request again — the
#      album would silently stop being acquired with nothing telling the
#      operator the world needed fixing.
#
# All checkers are module-level so the known-bad self-tests below can call
# them directly.

_TERMINAL_STAGE_FAIL_STOP: tuple[type[BaseException], ...] = (
    ExecutionCancelled,
    OwnerSessionLost,
)
# Every exception class the cleanup + terminal-persist stage is documented to
# raise, plus the two fail-stop classes it must never convert.
_TERMINAL_STAGE_FAULTS: tuple[type[BaseException], ...] = (
    RuntimeError,
    ValueError,
    OSError,
    msgspec.ValidationError,
    ProcessingCleanupError,
    CleanupJournalConflict,
    ImportJobTerminalConflict,
    OwnerSessionLost,
)


# A request outside these statuses is a request the pipeline has stopped
# acquiring: ``get_wanted`` only ever selects ``wanted``.
_WORLD_FAILURE_SEARCHABLE = frozenset({"wanted", "imported"})


def automation_world_failure_violation(
    *,
    label: str,
    escaped: BaseException | None,
    job_status: str,
    request_status: str,
    active_owner: int | None,
) -> str | None:
    """Return why one automation world failure broke invariant 3.

    Exactly two end states are legal:

    * the self-heal committed — the job is terminally ``failed``, the request is
      searchable again and no automation owner is attached; or
    * no owner-atomic write was authorized at all — the row is untouched, still
      ``running`` under its persisted lease and still the exact owner, while an
      ``AutomationOwnerFailStop`` ends the daemon for lease-proven automatic
      recovery. Returning normally with that same lease live is illegal. Never
      fabricate a write the owner authority refuses.

    ``recovery_required`` is never legal: it strands ``processing`` behind an
    inactive job that ``get_wanted`` cannot see.
    """
    if escaped is not None and not isinstance(
        escaped,
        AutomationOwnerFailStop,
    ):
        return (
            f"{label} escaped process_claimed_job as "
            f"{type(escaped).__name__}"
        )
    if job_status == "recovery_required":
        return f"{label} parked the job for human intervention"
    if job_status == "failed":
        if escaped is not None:
            return (
                f"{label} fail-stopped after writing terminal job status "
                f"{job_status}"
            )
        if request_status not in _WORLD_FAILURE_SEARCHABLE:
            return (
                f"{label} self-healed the job but left the request "
                f"{request_status}"
            )
        if active_owner is not None:
            return (
                f"{label} self-healed the job but left automation owner "
                f"{active_owner} attached"
            )
        return None
    if job_status == "running":
        if request_status != "processing" or active_owner is None:
            return (
                f"{label} left a torn owner: job running, request "
                f"{request_status}, owner {active_owner}"
            )
        if not isinstance(escaped, AutomationOwnerFailStop):
            return (
                f"{label} returned to the live daemon with owner "
                f"{active_owner} still attached"
            )
        return None
    return f"{label} left the job at {job_status}"


def terminal_stage_fault_violation(
    *,
    fault: type[BaseException],
    escaped: BaseException | None,
    job_status: str,
    request_status: str,
    active_owner: int | None,
) -> str | None:
    """Return why one injected terminal-stage fault broke invariant 1."""
    if issubclass(fault, _TERMINAL_STAGE_FAIL_STOP):
        if not isinstance(escaped, fault):
            return f"fail-stop {fault.__name__} did not reach the caller"
        if job_status != "running":
            return f"fail-stop {fault.__name__} moved the job to {job_status}"
        if request_status != "processing" or active_owner is None:
            return (
                f"fail-stop {fault.__name__} released the owner: request "
                f"{request_status}, owner {active_owner}"
            )
        return None
    return automation_world_failure_violation(
        label=fault.__name__,
        escaped=escaped,
        job_status=job_status,
        request_status=request_status,
        active_owner=active_owner,
    )


def quarantine_intent_violation(
    *,
    configured_root: str,
    refused: bool,
    destination_path: str | None,
) -> str | None:
    """Return why one cleanup-intent quarantine root broke invariant 2."""
    usable = bool(configured_root) and os.path.isdir(configured_root)
    if not usable:
        if refused:
            return None
        return (
            f"unusable quarantine root {configured_root!r} was journalled "
            f"with destination {destination_path!r}"
        )
    if refused:
        return f"usable quarantine root {configured_root!r} was refused"
    if destination_path is None:
        return f"usable quarantine root {configured_root!r} named no destination"
    bucket = os.path.join(
        os.path.abspath(configured_root), "failed_imports", "bad_files",
    )
    if destination_path != bucket and not destination_path.startswith(
        bucket + os.sep
    ):
        return (
            f"destination {destination_path!r} escaped the configured "
            f"quarantine bucket {bucket!r}"
        )
    return None


class _TerminalBoundaryFaultDB(FakePipelineDB):
    """Fake whose terminal write fails at its first durable boundary.

    ``_terminal_outcome_write_boundary`` is the fake's own fault-injection
    seam (see ``tests/test_terminal_outcomes.py``); production raises the
    real conflict/loss types from the same place when its compare-and-set
    loses the row or the pinned session dies mid-bundle.
    """

    fault: type[BaseException] = RuntimeError

    def _terminal_outcome_write_boundary(self, index: int, label: str) -> None:
        del index, label
        raise self.fault("injected terminal-stage fault")


_TERMINAL_STAGE_RELEASE = "terminal-stage-release"


def assert_world_failure_audit(
    case: unittest.TestCase,
    db: FakePipelineDB,
    *,
    diagnostic: str,
) -> None:
    """Assert one Recents-visible audit row carries the world diagnostic.

    The label is read from the production module, never retyped here, so the
    pin cannot assert copy no producer emits.
    """
    from scripts import importer

    rows = [row for row in db.download_logs if row.outcome == "failed"]
    case.assertTrue(rows, "no download_log row recorded the world failure")
    message = rows[-1].error_message or ""
    case.assertIn(importer._WORLD_FAILURE_AUDIT_PREFIX, message)
    case.assertIn(diagnostic, message)


def launch_automation_owner(
    db: FakePipelineDB,
    canonical_path: str,
    *,
    request_id: int = 42,
    capture_completion: bool = True,
    authorize_launch: bool = True,
    retry_counters: Mapping[str, int] | None = None,
) -> tuple[ImportJob, ExecutionLeaseSnapshot]:
    """Drive the real lifecycle to a running (by default launched) owner."""
    from lib.import_job_recovery_service import AutomationCompletionReceipt

    if capture_completion and not authorize_launch:
        raise AssertionError(
            "a completion receipt cannot exist before launch authorization"
        )
    db.seed_request(make_request_row(
        id=request_id,
        mb_release_id=_TERMINAL_STAGE_RELEASE,
        **dict(retry_counters or {}),
    ))
    job = handoff_automation_owner(
        db,
        request_id,
        state={
            "filetype": "flac",
            "enqueued_at": "2026-07-29T00:00:00+00:00",
            "current_path": canonical_path,
            "files": [],
        },
        canonical_path=canonical_path,
    )
    preview_lease = _preview_execution_lease(f"stage-preview-{job.id}")
    claimed_preview = claim_next_import_preview_job(
        db,
        worker_id="preview",
        execution_lease=preview_lease,
    )
    assert claimed_preview is not None
    _seed_candidate_for_import_job(
        db,
        job.id,
        mb_release_id=_TERMINAL_STAGE_RELEASE,
        expected_execution_lease=preview_lease,
    )
    ready = db.mark_import_job_preview_importable(
        job.id,
        preview_result={"verdict": "would_import"},
        message="ready",
        expected_execution_lease=preview_lease,
    )
    assert ready is not None
    lease = _importer_execution_lease(f"stage-import-{job.id}")
    claimed = claim_next_import_job(
        db,
        worker_id="worker",
        execution_lease=lease,
    )
    assert claimed is not None
    if authorize_launch:
        authorized = db.authorize_import_job_launch(
            job.id,
            request_id=request_id,
            release_id=_TERMINAL_STAGE_RELEASE,
            source_path=canonical_path,
            expected_execution_lease=lease,
        )
        assert authorized is not None
    if capture_completion:
        child = db.record_import_job_beets_child(
            job.id,
            expected_execution_lease=lease,
            beets_pid=1,
            beets_start_ticks=1,
        )
        assert child is not None
        captured = db.capture_automation_import_completion(
            job.id,
            expected_execution_lease=dataclass_replace(
                lease,
                beets=ProcessIdentity(1, 1),
            ),
            receipt=AutomationCompletionReceipt(
                job_id=job.id,
                request_id=request_id,
                release_id=_TERMINAL_STAGE_RELEASE,
                canonical_path=canonical_path,
                returncode=0,
                captured_at="2026-07-29T04:00:00+00:00",
            ),
        )
        assert captured is not None
    return claimed, lease


def terminal_stage_canonical_dir(case: unittest.TestCase) -> str:
    """Create one real canonical owner directory for the calling test."""
    path = tempfile.mkdtemp(prefix="cratedigger-terminal-stage-")
    with open(os.path.join(path, "01.flac"), "wb") as handle:
        handle.write(b"canonical owner fixture")
    case.addCleanup(shutil.rmtree, path, ignore_errors=True)
    return path


class TestAutomationTerminalStageFaultRouting(unittest.TestCase):
    """Invariant 1 — the post-execution stage never kills the worker."""

    def _canonical_dir(self) -> str:
        return terminal_stage_canonical_dir(self)

    def _drift_the_cleanup_journal(
        self,
        db: FakePipelineDB,
        job_id: int,
        canonical_path: str,
        *,
        request_id: int = 42,
    ) -> None:
        """Journal a plan for a path that is no longer the canonical owner."""
        from lib.pipeline_db import CleanupJournalIntent
        from lib.processing_cleanup import (
            PROCESSING_CLEANUP_NO_OP,
            cleanup_manifest_builtins,
            cleanup_manifest_hash,
        )

        db.create_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            intent=CleanupJournalIntent(
                action=PROCESSING_CLEANUP_NO_OP,
                source_path=os.path.join(canonical_path, "superseded"),
                source_manifest=cleanup_manifest_builtins(()),
                source_manifest_hash=cleanup_manifest_hash(()),
            ),
        )

    def _run_terminal_stage(
        self,
        db: FakePipelineDB,
        claimed: ImportJob,
        lease: ExecutionLeaseSnapshot,
        *,
        drop_owner_session: bool = False,
    ) -> tuple[ImportJob | None, BaseException | None]:
        """Run the real automation branch through to its terminal stage."""
        from scripts import importer

        def completed_processing(
            *_args: object,
            **_kwargs: object,
        ) -> CompletionResult:
            """Stand in for the beets-mutating completion pipeline only."""
            return CompletionDispatched(
                outcome=DispatchOutcome(True, "Imported by dispatch"),
            )

        def execute(
            owner: FakePipelineDB,
            owned_job: ImportJob,
            *,
            ctx: object = None,
            execution_lease: ExecutionLeaseSnapshot,
            cancellation_token: CancellationToken,
            owner_session_identity: OwnerSessionIdentity,
        ) -> DispatchOutcome:
            return importer.execute_automation_import_job(
                owner,  # pyright: ignore[reportArgumentType]
                owned_job,
                ctx=ctx,
                execution_lease=execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
                completed_processing_fn=completed_processing,
            )

        token = CancellationToken()
        escaped: BaseException | None = None
        updated: ImportJob | None = None
        with db._pin_owner_session(token) as owner_session_identity:
            if drop_owner_session:
                # Leaving the pin scope is exactly what production observes
                # when the pinned backend goes away mid-execution.
                db._owner_session_pin = None
            try:
                updated = importer.process_claimed_job(
                    db,  # pyright: ignore[reportArgumentType]
                    claimed,
                    ctx=object(),
                    execute_fn=execute,
                    execution_lease=lease,
                    cancellation_token=token,
                    owner_session_identity=owner_session_identity,
                )
            except BaseException as exc:  # noqa: BLE001 - the pin under test
                escaped = exc
        return updated, escaped

    def test_cleanup_failure_fail_stops_for_automatic_recovery(self) -> None:
        """A journal that no longer names the canonical owner fail-stops.

        The cleanup cannot complete, so no terminal bundle can consume its
        receipt and no owner-atomic write is authorable. The row therefore stays
        ``running`` under its persisted lease while the worker exits. Systemd
        restart makes that lease provably dead, and startup recovery resumes
        the exact journal without an operator action.
        """
        db = FakePipelineDB()
        canonical = self._canonical_dir()
        claimed, lease = launch_automation_owner(db, canonical)
        self._drift_the_cleanup_journal(db, claimed.id, canonical)

        updated, escaped = self._run_terminal_stage(db, claimed, lease)

        self.assertIsInstance(escaped, AutomationOwnerFailStop)
        self.assertIsNone(updated)
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        self.assertNotEqual(stored.status, "recovery_required")
        self.assertTrue(os.path.isdir(canonical))
        row = db.request(42)
        self.assertIsNone(
            terminal_stage_fault_violation(
                fault=RuntimeError,
                escaped=escaped,
                job_status=stored.status,
                request_status=str(row["status"]),
                active_owner=row["active_automation_import_job_id"],
            )
        )

    def test_terminal_persist_conflict_never_parks_the_request(self) -> None:
        """A terminal CAS that always loses fail-stops instead of parking."""
        from lib.pipeline_db.terminal_outcomes import ImportJobTerminalConflict

        class ConflictDB(_TerminalBoundaryFaultDB):
            fault = ImportJobTerminalConflict

        db = ConflictDB()
        canonical = self._canonical_dir()
        claimed, lease = launch_automation_owner(db, canonical)

        updated, escaped = self._run_terminal_stage(db, claimed, lease)

        self.assertIsInstance(escaped, AutomationOwnerFailStop)
        self.assertIsNone(updated)
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        self.assertNotEqual(stored.status, "recovery_required")
        row = db.request(42)
        self.assertIsNone(
            terminal_stage_fault_violation(
                fault=ImportJobTerminalConflict,
                escaped=escaped,
                job_status=stored.status,
                request_status=str(row["status"]),
                active_owner=row["active_automation_import_job_id"],
            )
        )

    def test_recoverable_terminal_fault_self_heals_when_the_retry_can_write(
        self,
    ) -> None:
        """A malformed first bundle self-heals on the second, valid write.

        This is the terminal-stage fault that CAN reach a terminal write: only
        the first ``persist_import_terminal_outcome`` call fails, so the
        self-heal's own minimal bundle commits and the request rejoins the
        search pool.
        """
        class FaultOnceDB(_TerminalBoundaryFaultDB):
            calls = 0

            def _terminal_outcome_write_boundary(
                self, index: int, label: str,
            ) -> None:
                del index, label
                if type(self).calls == 0:
                    type(self).calls = 1
                    raise RuntimeError("injected first-bundle fault")

        db = FaultOnceDB()
        canonical = self._canonical_dir()
        claimed, lease = launch_automation_owner(db, canonical)

        updated, escaped = self._run_terminal_stage(db, claimed, lease)

        self.assertIsNone(escaped)
        assert updated is not None
        self.assertEqual(updated.status, "failed")
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        row = db.request(42)
        self.assertIsNone(
            terminal_stage_fault_violation(
                fault=RuntimeError,
                escaped=escaped,
                job_status=stored.status,
                request_status=str(row["status"]),
                active_owner=row["active_automation_import_job_id"],
            )
        )
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["active_automation_import_job_id"])
        assert_world_failure_audit(
            self, db, diagnostic="injected first-bundle fault",
        )

    def test_execution_cancellation_in_the_terminal_stage_still_fail_stops(
        self,
    ) -> None:
        """Must-still-work: losing the pinned session is not recoverable."""
        db = FakePipelineDB()
        canonical = self._canonical_dir()
        claimed, lease = launch_automation_owner(db, canonical)

        _updated, escaped = self._run_terminal_stage(
            db,
            claimed,
            lease,
            drop_owner_session=True,
        )

        self.assertIsInstance(escaped, ExecutionCancelled)
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        # Fail-stop leaves the row running for lease-proven startup recovery.
        self.assertEqual(stored.status, "running")
        row = db.request(42)
        self.assertEqual(row["status"], "processing")
        self.assertEqual(row["active_automation_import_job_id"], claimed.id)
        self.assertIsNone(
            terminal_stage_fault_violation(
                fault=ExecutionCancelled,
                escaped=escaped,
                job_status=stored.status,
                request_status=str(row["status"]),
                active_owner=row["active_automation_import_job_id"],
            )
        )

    def test_owner_session_loss_in_the_terminal_stage_still_fail_stops(
        self,
    ) -> None:
        """Must-still-work: a dead pinned session cannot self-heal either."""
        class LostDB(_TerminalBoundaryFaultDB):
            fault = OwnerSessionLost

        db = LostDB()
        canonical = self._canonical_dir()
        claimed, lease = launch_automation_owner(db, canonical)

        _updated, escaped = self._run_terminal_stage(db, claimed, lease)

        self.assertIsInstance(escaped, OwnerSessionLost)
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        self.assertEqual(stored.status, "running")
        row = db.request(42)
        self.assertEqual(row["status"], "processing")
        self.assertEqual(row["active_automation_import_job_id"], claimed.id)
        self.assertIsNone(
            terminal_stage_fault_violation(
                fault=OwnerSessionLost,
                escaped=escaped,
                job_status=stored.status,
                request_status=str(row["status"]),
                active_owner=row["active_automation_import_job_id"],
            )
        )

    @given(fault=st.sampled_from(_TERMINAL_STAGE_FAULTS))
    def test_generated_every_terminal_stage_fault_recovers_or_fail_stops(
        self,
        fault: type[BaseException],
    ) -> None:
        """Property: invariant 1 holds for every fault class the stage raises."""
        class GeneratedFaultDB(_TerminalBoundaryFaultDB):
            pass

        GeneratedFaultDB.fault = fault
        db = GeneratedFaultDB()
        canonical = self._canonical_dir()
        claimed, lease = launch_automation_owner(db, canonical)

        _updated, escaped = self._run_terminal_stage(db, claimed, lease)

        stored = db.get_import_job(claimed.id)
        assert stored is not None
        row = db.request(42)
        violation = terminal_stage_fault_violation(
            fault=fault,
            escaped=escaped,
            job_status=stored.status,
            request_status=str(row["status"]),
            active_owner=row["active_automation_import_job_id"],
        )
        self.assertIsNone(violation, violation)


class TestAutomationCleanupQuarantineRoot(unittest.TestCase):
    """Invariant 2 — an unusable quarantine root is refused, never journalled."""

    def _source_dir(self) -> str:
        path = tempfile.mkdtemp(prefix="cratedigger-quarantine-source-")
        with open(os.path.join(path, "01.flac"), "wb") as handle:
            handle.write(b"corrupt candidate fixture")
        self.addCleanup(shutil.rmtree, path, ignore_errors=True)
        return path

    def _plan(self, source: str, root: str | None):
        from lib.dispatch.types import PostCommitCleanup

        return PostCommitCleanup(
            audio_quarantine_source_path=source,
            audio_quarantine_root=root,
        )

    def test_unconfigured_staging_dir_is_refused_before_any_journal(
        self,
    ) -> None:
        """The blank root comes from the producer, not from a test literal."""
        from scripts import importer

        # lib/dispatch/core.py threads beets_staging_dir straight into the
        # plan, and lib/config.py defaults it to "".
        blank_root = CratediggerConfig().beets_staging_dir
        self.assertEqual(blank_root, "")
        source = self._source_dir()

        with self.assertRaises(RuntimeError) as caught:
            importer._automation_cleanup_intent(
                source_path=source,
                plan=self._plan(source, blank_root),
            )

        self.assertIn("quarantine root", str(caught.exception))

    def test_missing_quarantine_root_directory_is_refused(self) -> None:
        """Mirror of quarantine_corrupt_audio_source's isdir precondition."""
        from scripts import importer

        source = self._source_dir()
        missing = os.path.join(source, "does-not-exist")

        with self.assertRaises(RuntimeError) as caught:
            importer._automation_cleanup_intent(
                source_path=source,
                plan=self._plan(source, missing),
            )

        self.assertIn("quarantine root", str(caught.exception))

    def test_refused_root_never_reaches_the_immutable_journal(self) -> None:
        """The refusal happens before the intent is persisted."""
        from lib.dispatch import DispatchOutcome
        from scripts import importer

        db = FakePipelineDB()
        canonical = terminal_stage_canonical_dir(self)
        claimed, lease = launch_automation_owner(
            db,
            canonical,
            capture_completion=False,
        )
        outcome = DispatchOutcome(
            success=False,
            message="Corrupt audio",
            post_commit_cleanup=self._plan(
                canonical,
                CratediggerConfig().beets_staging_dir,
            ),
        )
        token = CancellationToken()

        with (
            db._pin_owner_session(token) as owner_session_identity,
            self.assertRaises(RuntimeError),
        ):
            importer._complete_automation_processing_cleanup(
                db,
                claimed,
                outcome,
                execution_lease=lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )

        self.assertIsNone(db.get_processing_cleanup_journal(
            request_id=42,
            job_id=claimed.id,
        ))
        self.assertTrue(os.path.isdir(canonical))

    def test_configured_root_still_selects_a_destination_under_it(self) -> None:
        """Must-still-work: a real configured root keeps quarantining."""
        from scripts import importer

        source = self._source_dir()
        root = tempfile.mkdtemp(prefix="cratedigger-quarantine-root-")
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)

        intent = importer._automation_cleanup_intent(
            source_path=source,
            plan=self._plan(source, root),
        )

        assert intent.destination_path is not None
        self.assertIsNone(quarantine_intent_violation(
            configured_root=root,
            refused=False,
            destination_path=intent.destination_path,
        ))
        self.assertFalse(
            intent.destination_path.startswith(os.getcwd() + os.sep)
            and not root.startswith(os.getcwd() + os.sep)
        )

    @given(root_kind=st.integers(min_value=0, max_value=4))
    def test_generated_only_a_usable_root_produces_a_destination(
        self,
        root_kind: int,
    ) -> None:
        """Property: invariant 2 holds across every configured-root shape."""
        from scripts import importer

        source = self._source_dir()
        usable_root = tempfile.mkdtemp(prefix="cratedigger-quarantine-root-")
        self.addCleanup(shutil.rmtree, usable_root, ignore_errors=True)
        roots: tuple[str | None, ...] = (
            None,
            "",
            "   ",
            os.path.join(usable_root, "missing"),
            usable_root,
        )
        root = roots[root_kind]

        refused = False
        destination: str | None = None
        try:
            intent = importer._automation_cleanup_intent(
                source_path=source,
                plan=self._plan(source, root),
            )
        except RuntimeError:
            refused = True
        else:
            destination = intent.destination_path

        violation = quarantine_intent_violation(
            configured_root=root or "",
            refused=refused,
            destination_path=destination,
        )
        self.assertIsNone(violation, violation)


# Every distinct world-failure class the automation branch of
# ``process_claimed_job`` can reach, and whether an owner-atomic terminal write
# is authorable for it. The two ``False`` rows are not policy exceptions: the
# cleanup could not complete (so no receipt exists to consume) or the terminal
# CAS refuses every bundle. Both leave the row untouched and ``running`` under
# its persisted lease while ``AutomationOwnerFailStop`` ends the process so
# automatic death-proven recovery can proceed — never parked and never returned
# to the live daemon loop.
_WORLD_FAILURE_CLASSES: dict[str, bool] = {
    "execute_crash_after_launch": True,
    "beets_acknowledgement_ambiguous": True,
    "no_terminal_outcome_pre_launch": True,
    "missing_completion_receipt": True,
    "prelaunch_requeue_failed": True,
    "recoverable_terminal_stage_fault": True,
    "cleanup_journal_drift": False,
    "terminal_cas_always_conflicts": False,
}


class TestAutomationWorldFailureNeverParks(unittest.TestCase):
    """Invariant 3 — a world failure re-opens the search, never parks it.

    Every pin here drives the REAL ``process_claimed_job`` automation branch
    and asserts the DOMAIN outcome: the request rejoins the search pool, the
    exact processing owner is cleared, the diagnostic is recorded as a
    ``download_log`` row an operator can read in Recents, and no job is left in
    ``recovery_required``.
    """

    def _launch(
        self,
        *,
        capture_completion: bool = True,
        authorize_launch: bool = True,
        retry_counters: Mapping[str, int] | None = None,
        db: FakePipelineDB | None = None,
    ) -> tuple[FakePipelineDB, str, ImportJob, ExecutionLeaseSnapshot]:
        owner_db = FakePipelineDB() if db is None else db
        canonical = terminal_stage_canonical_dir(self)
        claimed, lease = launch_automation_owner(
            owner_db,
            canonical,
            capture_completion=capture_completion,
            authorize_launch=authorize_launch,
            retry_counters=retry_counters,
        )
        return owner_db, canonical, claimed, lease

    def _run(
        self,
        db: FakePipelineDB,
        claimed: ImportJob,
        lease: ExecutionLeaseSnapshot,
        execute_fn: Callable[..., DispatchOutcome],
    ) -> tuple[ImportJob | None, BaseException | None]:
        from scripts import importer

        token = CancellationToken()
        escaped: BaseException | None = None
        updated: ImportJob | None = None
        with db._pin_owner_session(token) as owner_session_identity:
            try:
                updated = importer.process_claimed_job(
                    db,  # pyright: ignore[reportArgumentType]
                    claimed,
                    ctx=object(),
                    execute_fn=execute_fn,
                    execution_lease=lease,
                    cancellation_token=token,
                    owner_session_identity=owner_session_identity,
                )
            except BaseException as exc:  # noqa: BLE001 - the policy under test
                escaped = exc
        return updated, escaped

    def _assert_returned_to_search_pool(
        self,
        db: FakePipelineDB,
        claimed: ImportJob,
        updated: ImportJob | None,
        escaped: BaseException | None,
        *,
        label: str,
        diagnostic: str,
    ) -> None:
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        row = db.request(42)
        violation = automation_world_failure_violation(
            label=label,
            escaped=escaped,
            job_status=stored.status,
            request_status=str(row["status"]),
            active_owner=row["active_automation_import_job_id"],
        )
        self.assertIsNone(violation, violation)
        # The positive half the checker cannot demand: the write COMMITTED.
        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(stored.status, "failed")
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["active_automation_import_job_id"])
        self.assertIsNone(row["active_download_state"])
        assert_world_failure_audit(self, db, diagnostic=diagnostic)

    # -- one deterministic pin per converted failure class -------------------

    def test_crash_after_launch_returns_the_request_to_the_search_pool(
        self,
    ) -> None:
        """An exception escaping a launched execution self-heals."""
        db, _canonical, claimed, lease = self._launch()

        def execute(*_args: object, **_kwargs: object) -> DispatchOutcome:
            raise RuntimeError("beets wrapper vanished mid-import")

        updated, escaped = self._run(db, claimed, lease, execute)

        self._assert_returned_to_search_pool(
            db, claimed, updated, escaped,
            label="execute_crash_after_launch",
            diagnostic="RuntimeError: beets wrapper vanished mid-import",
        )

    def test_ambiguous_beets_acknowledgement_reopens_the_search(self) -> None:
        """The canonical "did Beets mutate the library?" ambiguity self-heals.

        The outcome comes from the REAL producer
        (``lib.dispatch.core._capture_automation_completion``) losing its
        capture CAS, not from a hand-typed code/message pair.
        """
        db, canonical, claimed, lease = self._launch(capture_completion=False)
        ambiguous = _real_ambiguous_completion_outcome(
            db, claimed, canonical, lease,
        )
        self.assertEqual(ambiguous.code, "beets_acknowledgement_ambiguous")

        def execute(*_args: object, **_kwargs: object) -> DispatchOutcome:
            return ambiguous

        updated, escaped = self._run(db, claimed, lease, execute)

        self._assert_returned_to_search_pool(
            db, claimed, updated, escaped,
            label="beets_acknowledgement_ambiguous",
            diagnostic=ambiguous.message,
        )

    def test_processor_without_a_terminal_outcome_reopens_the_search(
        self,
    ) -> None:
        """A pre-launch processor that returns no owner-atomic bundle."""
        db, _canonical, claimed, lease = self._launch(
            capture_completion=False,
            authorize_launch=False,
        )

        def execute(*_args: object, **_kwargs: object) -> DispatchOutcome:
            return DispatchOutcome(False, "processor produced nothing")

        updated, escaped = self._run(db, claimed, lease, execute)

        self._assert_returned_to_search_pool(
            db, claimed, updated, escaped,
            label="no_terminal_outcome_pre_launch",
            diagnostic=(
                "Automation processor returned no owner-atomic terminal outcome"
            ),
        )

    def test_missing_completion_receipt_reopens_the_search(self) -> None:
        """A launched job with no captured receipt self-heals, not parks.

        The narrow world-failure terminal shape accepts the missing receipt
        while retaining the exact owner/lease CAS, so the request returns
        directly to ``wanted`` without a ``recovery_required`` transit.
        """
        db, canonical, claimed, lease = self._launch(capture_completion=False)
        updated, escaped = self._run(
            db,
            claimed,
            lease,
            _completion_stub_execute_fn(Completed()),
        )

        self._assert_returned_to_search_pool(
            db, claimed, updated, escaped,
            label="missing_completion_receipt",
            diagnostic="Automation completion receipt is missing or invalid",
        )
        self.assertFalse(os.path.isdir(canonical))

    def test_prelaunch_requeue_failure_reopens_the_search(self) -> None:
        """A failed preview requeue cannot rest under this daemon's lease."""
        db, _canonical, claimed, lease = self._launch(
            capture_completion=False,
            authorize_launch=False,
        )

        updated, escaped = self._run(
            db,
            claimed,
            lease,
            _fixed_outcome_execute_fn(DispatchOutcome(
                False,
                "preview requeue UPDATE failed",
                code=DISPATCH_CODE_REQUEUE_FAILED,
            )),
        )

        self._assert_returned_to_search_pool(
            db,
            claimed,
            updated,
            escaped,
            label="prelaunch_requeue_failed",
            diagnostic="preview requeue UPDATE failed",
        )

    # -- must-still-work guards ---------------------------------------------

    def test_a_successful_import_still_terminalizes_as_imported(self) -> None:
        """Must-still-work: self-healing did not break the happy path."""
        db, canonical, claimed, lease = self._launch()

        updated, escaped = self._run(
            db,
            claimed,
            lease,
            _completion_stub_execute_fn(
                CompletionDispatched(
                    outcome=DispatchOutcome(True, "Imported by dispatch"),
                ),
            ),
        )

        self.assertIsNone(escaped)
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["active_automation_import_job_id"])
        self.assertFalse(os.path.isdir(canonical))
        self.assertEqual(
            [row.outcome for row in db.download_logs], ["success"],
        )

    def test_execution_cancellation_does_not_self_heal(self) -> None:
        """Must-still-work: a cancelled execution fail-stops, never writes.

        ``ExecutionCancelled`` is what dispatch raises when this execution's
        authority is revoked mid-flight. It must reach the caller untouched:
        the self-heal write would need the same revoked authority.
        """
        db, canonical, claimed, lease = self._launch()

        def execute(*_args: object, **_kwargs: object) -> DispatchOutcome:
            raise ExecutionCancelled("owner_session_reverification_failed")

        updated, escaped = self._run(db, claimed, lease, execute)

        self.assertIsInstance(escaped, ExecutionCancelled)
        self.assertIsNone(updated)
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        self.assertEqual(stored.status, "running")
        row = db.request(42)
        self.assertEqual(row["status"], "processing")
        self.assertEqual(row["active_automation_import_job_id"], claimed.id)
        self.assertEqual(db.download_logs, [])
        self.assertTrue(os.path.isdir(canonical))

    def test_owner_session_loss_does_not_self_heal(self) -> None:
        """Must-still-work: a dead pinned session cannot self-heal either."""
        class LostDB(_TerminalBoundaryFaultDB):
            fault = OwnerSessionLost

        db, _canonical, claimed, lease = self._launch(db=LostDB())

        updated, escaped = self._run(
            db,
            claimed,
            lease,
            _completion_stub_execute_fn(
                CompletionDispatched(
                    outcome=DispatchOutcome(True, "Imported by dispatch"),
                ),
            ),
        )

        self.assertIsInstance(escaped, OwnerSessionLost)
        self.assertIsNone(updated)
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        self.assertEqual(stored.status, "running")
        row = db.request(42)
        self.assertEqual(row["status"], "processing")
        self.assertEqual(row["active_automation_import_job_id"], claimed.id)

    def test_self_heal_retains_retry_counters_so_backoff_grows(self) -> None:
        """A repeatedly broken world backs off instead of hot-looping."""
        db, _canonical, claimed, lease = self._launch(
            retry_counters={
                "search_attempts": 5,
                "download_attempts": 4,
                "validation_attempts": 3,
            },
        )

        def execute(*_args: object, **_kwargs: object) -> DispatchOutcome:
            raise RuntimeError("the world is still wrong")

        updated, escaped = self._run(db, claimed, lease, execute)

        self._assert_returned_to_search_pool(
            db, claimed, updated, escaped,
            label="retained_counters",
            diagnostic="RuntimeError: the world is still wrong",
        )
        row = db.request(42)
        # Retained, not cleared — and the failed attempt is recorded, so the
        # next retry is scheduled further out.
        self.assertEqual(row["search_attempts"], 5)
        self.assertEqual(row["download_attempts"], 4)
        self.assertEqual(row["validation_attempts"], 4)
        self.assertIsNotNone(row["next_retry_after"])
        self.assertIsNotNone(row["last_attempt_at"])

    # -- generated property over the whole failure-class space --------------

    @given(kind=st.sampled_from(sorted(_WORLD_FAILURE_CLASSES)))
    def test_generated_no_world_failure_ever_parks_the_request(
        self,
        kind: str,
    ) -> None:
        """Property: invariant 3 holds for every automation world failure."""
        db, claimed, updated, escaped = self._drive_world_failure(kind)

        stored = db.get_import_job(claimed.id)
        assert stored is not None
        row = db.request(42)
        violation = automation_world_failure_violation(
            label=kind,
            escaped=escaped,
            job_status=stored.status,
            request_status=str(row["status"]),
            active_owner=row["active_automation_import_job_id"],
        )
        self.assertIsNone(violation, violation)
        self.assertNotEqual(stored.status, "recovery_required")
        # Sharper than the checker alone: a class that CAN self-heal must, and
        # a class that cannot must leave the row exactly as it found it.
        self.assertEqual(
            updated is not None,
            _WORLD_FAILURE_CLASSES[kind],
            f"{kind} changed its self-heal reachability",
        )
        if not _WORLD_FAILURE_CLASSES[kind]:
            self.assertIsInstance(escaped, AutomationOwnerFailStop)

    def _drive_world_failure(
        self,
        kind: str,
    ) -> tuple[
        FakePipelineDB, ImportJob, ImportJob | None, BaseException | None,
    ]:
        """Reach one named world-failure class through the real branch."""
        if kind == "execute_crash_after_launch":
            db, _canonical, claimed, lease = self._launch()

            def crash(*_args: object, **_kwargs: object) -> DispatchOutcome:
                raise RuntimeError("generated crash")

            updated, escaped = self._run(db, claimed, lease, crash)
            return db, claimed, updated, escaped
        if kind == "beets_acknowledgement_ambiguous":
            db, canonical, claimed, lease = self._launch(
                capture_completion=False,
            )
            ambiguous = _real_ambiguous_completion_outcome(
                db, claimed, canonical, lease,
            )
            updated, escaped = self._run(
                db, claimed, lease, _fixed_outcome_execute_fn(ambiguous),
            )
            return db, claimed, updated, escaped
        if kind == "no_terminal_outcome_pre_launch":
            db, _canonical, claimed, lease = self._launch(
                capture_completion=False,
                authorize_launch=False,
            )
            updated, escaped = self._run(
                db,
                claimed,
                lease,
                _fixed_outcome_execute_fn(
                    DispatchOutcome(False, "generated empty bundle"),
                ),
            )
            return db, claimed, updated, escaped
        if kind == "missing_completion_receipt":
            db, _canonical, claimed, lease = self._launch(
                capture_completion=False,
            )
            updated, escaped = self._run(
                db, claimed, lease, _completion_stub_execute_fn(Completed()),
            )
            return db, claimed, updated, escaped
        if kind == "prelaunch_requeue_failed":
            db, _canonical, claimed, lease = self._launch(
                capture_completion=False,
                authorize_launch=False,
            )
            updated, escaped = self._run(
                db,
                claimed,
                lease,
                _fixed_outcome_execute_fn(DispatchOutcome(
                    False,
                    "generated preview requeue failed",
                    code=DISPATCH_CODE_REQUEUE_FAILED,
                )),
            )
            return db, claimed, updated, escaped
        if kind == "recoverable_terminal_stage_fault":
            class FaultOnceDB(_TerminalBoundaryFaultDB):
                calls = 0

                def _terminal_outcome_write_boundary(
                    self, index: int, label: str,
                ) -> None:
                    del index, label
                    if type(self).calls == 0:
                        type(self).calls = 1
                        raise RuntimeError("generated first-bundle fault")

            db, _canonical, claimed, lease = self._launch(db=FaultOnceDB())
            updated, escaped = self._run(
                db,
                claimed,
                lease,
                _completion_stub_execute_fn(
                    CompletionDispatched(
                        outcome=DispatchOutcome(True, "Imported by dispatch"),
                    ),
                ),
            )
            return db, claimed, updated, escaped
        if kind == "cleanup_journal_drift":
            from lib.pipeline_db import CleanupJournalIntent
            from lib.processing_cleanup import (
                PROCESSING_CLEANUP_NO_OP,
                cleanup_manifest_builtins,
                cleanup_manifest_hash,
            )

            db, canonical, claimed, lease = self._launch()
            db.create_processing_cleanup_journal(
                request_id=42,
                job_id=claimed.id,
                intent=CleanupJournalIntent(
                    action=PROCESSING_CLEANUP_NO_OP,
                    source_path=os.path.join(canonical, "superseded"),
                    source_manifest=cleanup_manifest_builtins(()),
                    source_manifest_hash=cleanup_manifest_hash(()),
                ),
            )
            updated, escaped = self._run(
                db,
                claimed,
                lease,
                _completion_stub_execute_fn(
                    CompletionDispatched(
                        outcome=DispatchOutcome(True, "Imported by dispatch"),
                    ),
                ),
            )
            return db, claimed, updated, escaped
        if kind == "terminal_cas_always_conflicts":
            from lib.pipeline_db.terminal_outcomes import (
                ImportJobTerminalConflict,
            )

            class ConflictDB(_TerminalBoundaryFaultDB):
                fault = ImportJobTerminalConflict

            db, _canonical, claimed, lease = self._launch(db=ConflictDB())
            updated, escaped = self._run(
                db,
                claimed,
                lease,
                _completion_stub_execute_fn(
                    CompletionDispatched(
                        outcome=DispatchOutcome(True, "Imported by dispatch"),
                    ),
                ),
            )
            return db, claimed, updated, escaped
        raise AssertionError(f"unknown world-failure class {kind!r}")


def _real_ambiguous_completion_outcome(
    db: FakePipelineDB,
    job: ImportJob,
    canonical_path: str,
    lease: ExecutionLeaseSnapshot,
) -> DispatchOutcome:
    """Make the REAL producer emit its ambiguous-acknowledgement outcome.

    ``_capture_automation_completion`` returns the ambiguity exactly when its
    capture compare-and-set loses, which a lease naming a child process the job
    never recorded reproduces. Deriving the trigger and the message from the
    producer keeps the pin falsifiable (test-fidelity Rule C).
    """
    from lib.dispatch.core import _capture_automation_completion

    outcome = _capture_automation_completion(
        db,  # pyright: ignore[reportArgumentType]
        import_job_id=job.id,
        request_id=42,
        release_id=_TERMINAL_STAGE_RELEASE,
        canonical_path=canonical_path,
        returncode=0,
        execution_lease=dataclass_replace(
            lease,
            beets=ProcessIdentity(424242, 424242),
        ),
    )
    if outcome is None:
        raise AssertionError(
            "the completion capture unexpectedly succeeded; this pin needs a "
            "conflicting lease to reach the ambiguity producer"
        )
    return outcome


def _fixed_outcome_execute_fn(
    outcome: DispatchOutcome,
) -> Callable[..., DispatchOutcome]:
    """Return an ``execute_fn`` yielding one already-built outcome."""
    def execute(*_args: object, **_kwargs: object) -> DispatchOutcome:
        return outcome

    return execute


def _completion_stub_execute_fn(
    result: CompletionResult,
) -> Callable[..., DispatchOutcome]:
    """Run the REAL automation executor over one stubbed completion result.

    Only the beets-mutating completion pipeline is stood in for; the fallback
    terminal-outcome construction, dispatch-outcome mapping and every owner
    write below it are production code.
    """
    def execute(
        owner: FakePipelineDB,
        owned_job: ImportJob,
        *,
        ctx: object = None,
        execution_lease: ExecutionLeaseSnapshot,
        cancellation_token: CancellationToken,
        owner_session_identity: OwnerSessionIdentity,
    ) -> DispatchOutcome:
        from scripts import importer

        def completed_processing(
            *_args: object,
            **_kwargs: object,
        ) -> CompletionResult:
            return result

        return importer.execute_automation_import_job(
            owner,  # pyright: ignore[reportArgumentType]
            owned_job,
            ctx=ctx,
            execution_lease=execution_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
            completed_processing_fn=completed_processing,
        )

    return execute


class TestForceJobFailuresAreRecordedNotParked(unittest.TestCase):
    """Invariant 3 for the non-owning job types.

    A force or YouTube job never owns its request's ``processing`` status, so
    there is nothing to self-heal — but it must still never park. The terminal
    ``failed``/``completed`` status IS the surface, and it stops no request.
    """

    def _force_job(self, db: FakePipelineDB) -> ImportJob:
        """Drive a real LAUNCH-AUTHORIZED force job.

        Launch authorization is what made the old parking attempt succeed; a
        pre-launch force job always fell through to the terminal status. So the
        pins below only prove the policy change on a launched job.
        """
        db.seed_request(make_request_row(id=42, mb_release_id="force-release"))
        source_download_log_id = db.log_download(
            42,
            outcome="rejected",
            error_message="manual force-import source",
        )
        source = tempfile.mkdtemp(prefix="cratedigger-force-never-parks-")
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"audio")
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(source_download_log_id),
            payload=force_import_payload(
                download_log_id=source_download_log_id,
                failed_path=source,
            ),
        )
        _seed_candidate_for_import_job(
            db, job.id, mb_release_id="force-release",
        )
        assert db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        ) is not None
        claimed = claim_next_import_job(db, worker_id="worker")
        assert claimed is not None
        authorized = db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="force-release",
            source_path=source,
        )
        assert authorized is not None
        return claimed

    def _assert_recents_visible_failure(
        self,
        db: FakePipelineDB,
        claimed: ImportJob,
    ) -> None:
        """The failure contract is the real Recents classifier, not a job row."""
        from web.classify import LogEntry, classify_log_entry

        assert isinstance(claimed.payload, ForceImportPayload)
        failed_rows = [
            row for row in db.download_logs
            if row.outcome == "failed"
            and row.source_download_log_id == claimed.payload.download_log_id
        ]
        self.assertEqual(len(failed_rows), 1)
        audit = db.get_download_log_entry(failed_rows[0].id)
        assert audit is not None
        rendered = classify_log_entry(LogEntry.from_row(dict(audit)))
        self.assertEqual(rendered.badge, "Failed")
        self.assertTrue(rendered.verdict.startswith("Force import attempt failed:"))

    def _run(
        self,
        db: FakePipelineDB,
        claimed: ImportJob,
        execute_fn: Callable[..., DispatchOutcome],
    ) -> ImportJob | None:
        from scripts import importer

        return importer.process_claimed_job(
            db,  # pyright: ignore[reportArgumentType]
            claimed,
            ctx=object(),
            execute_fn=execute_fn,
        )

    def test_crashed_force_job_is_failed_not_parked(self) -> None:
        db = FakePipelineDB()
        claimed = self._force_job(db)

        def execute(*_args: object, **_kwargs: object) -> DispatchOutcome:
            raise RuntimeError("force wrapper vanished")

        updated = self._run(db, claimed, execute)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        stored = db.get_import_job(claimed.id)
        assert stored is not None
        self.assertEqual(stored.status, "failed")
        self.assertEqual(stored.error, "RuntimeError")
        self._assert_recents_visible_failure(db, claimed)

    def test_bundleless_force_failure_is_failed_not_parked(self) -> None:
        db = FakePipelineDB()
        claimed = self._force_job(db)

        updated = self._run(
            db,
            claimed,
            _fixed_outcome_execute_fn(
                DispatchOutcome(False, "force import produced no bundle"),
            ),
        )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.error, "force import produced no bundle")
        self._assert_recents_visible_failure(db, claimed)

    def test_bundleless_force_success_is_completed_not_parked(self) -> None:
        db = FakePipelineDB()
        claimed = self._force_job(db)

        updated = self._run(
            db,
            claimed,
            _fixed_outcome_execute_fn(
                DispatchOutcome(True, "force import committed"),
            ),
        )

        assert updated is not None
        self.assertEqual(updated.status, "completed")

    def test_requeue_failed_after_launch_is_failed_not_parked(self) -> None:
        db = FakePipelineDB()
        claimed = self._force_job(db)

        updated = self._run(
            db,
            claimed,
            _fixed_outcome_execute_fn(
                DispatchOutcome(
                    False,
                    "requeue UPDATE failed: boom",
                    code=DISPATCH_CODE_REQUEUE_FAILED,
                ),
            ),
        )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertIn("requeue-to-preview failed", updated.message or "")
        self._assert_recents_visible_failure(db, claimed)

    def test_failed_force_attempt_preserves_operator_or_terminal_request_state(
        self,
    ) -> None:
        """A non-owning failure cannot undo a current explicit request state."""
        for status in ("unsearchable", "imported"):
            with self.subTest(status=status):
                db = FakePipelineDB()
                claimed = self._force_job(db)
                db.request(42)["status"] = status
                updated = self._run(
                    db,
                    claimed,
                    _fixed_outcome_execute_fn(
                        DispatchOutcome(False, "failed after operator action"),
                    ),
                )
                assert updated is not None
                self.assertEqual(db.request(42)["status"], status)
                self._assert_recents_visible_failure(db, claimed)


class TestTerminalStageInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: an unfalsifiable checker is not a checker."""

    def test_escaping_crash_is_reported(self) -> None:
        violation = terminal_stage_fault_violation(
            fault=RuntimeError,
            escaped=RuntimeError("boom"),
            job_status="running",
            request_status="processing",
            active_owner=1,
        )
        assert violation is not None
        self.assertIn("escaped process_claimed_job", violation)

    def test_parked_job_is_reported(self) -> None:
        """The whole point of invariant 3: parking is a violation."""
        violation = terminal_stage_fault_violation(
            fault=RuntimeError,
            escaped=None,
            job_status="recovery_required",
            request_status="processing",
            active_owner=1,
        )
        assert violation is not None
        self.assertIn("parked the job for human intervention", violation)

    def test_unknown_terminal_job_status_is_reported(self) -> None:
        violation = terminal_stage_fault_violation(
            fault=RuntimeError,
            escaped=None,
            job_status="queued",
            request_status="processing",
            active_owner=1,
        )
        assert violation is not None
        self.assertIn("left the job at queued", violation)

    def test_self_heal_that_left_the_request_processing_is_reported(
        self,
    ) -> None:
        violation = terminal_stage_fault_violation(
            fault=RuntimeError,
            escaped=None,
            job_status="failed",
            request_status="processing",
            active_owner=None,
        )
        assert violation is not None
        self.assertIn("left the request processing", violation)

    def test_self_heal_that_kept_the_owner_attached_is_reported(self) -> None:
        violation = terminal_stage_fault_violation(
            fault=RuntimeError,
            escaped=None,
            job_status="failed",
            request_status="wanted",
            active_owner=7,
        )
        assert violation is not None
        self.assertIn("left automation owner 7 attached", violation)

    def test_torn_running_owner_is_reported(self) -> None:
        violation = terminal_stage_fault_violation(
            fault=RuntimeError,
            escaped=None,
            job_status="running",
            request_status="wanted",
            active_owner=None,
        )
        assert violation is not None
        self.assertIn("left a torn owner", violation)

    def test_silently_retained_live_owner_is_reported(self) -> None:
        violation = terminal_stage_fault_violation(
            fault=RuntimeError,
            escaped=None,
            job_status="running",
            request_status="processing",
            active_owner=1,
        )
        assert violation is not None
        self.assertIn("returned to the live daemon", violation)

    def test_swallowed_fail_stop_is_reported(self) -> None:
        violation = terminal_stage_fault_violation(
            fault=ExecutionCancelled,
            escaped=None,
            job_status="recovery_required",
            request_status="processing",
            active_owner=1,
        )
        assert violation is not None
        self.assertIn("did not reach the caller", violation)

    def test_fail_stop_that_wrote_a_terminal_status_is_reported(self) -> None:
        violation = terminal_stage_fault_violation(
            fault=OwnerSessionLost,
            escaped=OwnerSessionLost("gone"),
            job_status="failed",
            request_status="processing",
            active_owner=1,
        )
        assert violation is not None
        self.assertIn("moved the job to failed", violation)

    def test_fail_stop_that_released_the_owner_is_reported(self) -> None:
        violation = terminal_stage_fault_violation(
            fault=OwnerSessionLost,
            escaped=OwnerSessionLost("gone"),
            job_status="running",
            request_status="wanted",
            active_owner=None,
        )
        assert violation is not None
        self.assertIn("released the owner", violation)

    def test_cwd_derived_destination_is_reported(self) -> None:
        violation = quarantine_intent_violation(
            configured_root="",
            refused=False,
            destination_path=os.path.join(
                os.getcwd(), "failed_imports", "bad_files", "Album",
            ),
        )
        assert violation is not None
        self.assertIn("was journalled", violation)

    def test_refusing_a_usable_root_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            violation = quarantine_intent_violation(
                configured_root=root,
                refused=True,
                destination_path=None,
            )
            assert violation is not None
            self.assertIn("was refused", violation)

    def test_destination_outside_the_configured_root_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            violation = quarantine_intent_violation(
                configured_root=root,
                refused=False,
                destination_path="/elsewhere/failed_imports/bad_files/Album",
            )
            assert violation is not None
            self.assertIn("escaped the configured", violation)
