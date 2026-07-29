"""Tests for dispatch_import_core — orchestration outcomes with FakePipelineDB.

Orchestration tests assert domain state: request status, download_log rows,
denylist entries, requeue behavior. Seam tests (argv, flag forwarding) are
in a separate class and explicitly labeled.
"""

import os
import tempfile
import unittest
from collections.abc import Callable
from typing import TypedDict
from unittest.mock import MagicMock, patch

import msgspec

from lib.beets_db import AlbumInfo
from lib.config import CratediggerConfig
from lib.dispatch.types import DispatchOutcome, EvidenceImportGate, ImportOneRun
from lib.import_execution import (
    CancellationToken,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
    ProcessIdentity,
)
from lib.import_queue import IMPORT_JOB_FORCE
from lib.pipeline_db import DownloadLogOutcome
from lib.quality import (
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    DownloadInfo,
    ImportResult,
    QualityEvidenceActionPayload,
    VerifiedLosslessProof,
)
from lib.quality_evidence import snapshot_audio_files
from lib.terminal_outcomes import ImportJobTerminal
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    finalize_claimed_dispatch,
    handoff_automation_owner,
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
    noop_quality_gate,
    patch_dispatch_externals,
    pinned_dispatch_authority,
)


# Migration 021 helpers — seed evidence and wire the FK chain that
# production reads through (download_log.candidate_evidence_id,
# import_jobs.candidate_evidence_id, album_requests.current_evidence_id).
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


_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"


class TestDispatchExecutionAuthority(unittest.TestCase):
    def test_partial_force_authority_cannot_hide_behind_no_lease_fast_path(self):
        from lib.dispatch.core import _validate_automation_dispatch_authority

        db = FakePipelineDB()
        partials = (
            (CancellationToken(), None),
            (None, OwnerSessionIdentity(connection_object_id=1, backend_pid=2)),
        )
        for token, identity in partials:
            with self.subTest(
                token=token is not None,
                identity=identity is not None,
            ), self.assertRaisesRegex(ValueError, "must be paired"):
                _validate_automation_dispatch_authority(
                    db,  # pyright: ignore[reportArgumentType]
                    force=True,
                    import_job_id=7,
                    execution_lease=None,
                    cancellation_token=token,
                    owner_session_identity=identity,
                )


def _owned_test_runner(**kwargs):
    """Persist synthetic child proof before exercising the patched run seam."""
    from lib.dispatch.subprocess_runner import run_import_one

    on_spawn = kwargs.pop("on_spawn", None)
    cancellation_token = kwargs.pop("cancellation_token", None)
    kwargs.pop("owner_session_probe", None)
    if on_spawn is not None:
        on_spawn(os.getpid())
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()
    return run_import_one(**kwargs)


class _DispatchWorld(TypedDict):
    result: DispatchOutcome
    cmd: object
    db: FakePipelineDB
    path: str
    cleanup_calls: int


def _patch_beets_album(album_path: str | None, *, min_bitrate: int = 128):
    beets = FakeBeetsDB()
    if album_path is not None:
        beets.set_album_info(
            "mbid-123",
            AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=min_bitrate,
                avg_bitrate_kbps=min_bitrate,
                median_bitrate_kbps=min_bitrate,
                is_cbr=True,
                album_path=album_path,
                format="MP3",
            ),
        )
    return patch("lib.beets_db.BeetsDB", return_value=beets)


class TestDispatchCoreOrchestration(unittest.TestCase):
    """Orchestration tests — assert domain state via FakePipelineDB."""

    def _dispatch(self, ir: ImportResult | None = None, force: bool = False,
                  outcome_label: DownloadLogOutcome = "success",
                  requeue_on_failure: bool = True,
                  override_min_bitrate: int | None = None,
                  source_username: str | None = None,
                  target_format: str | None = None,
                  verified_lossless_target: str = "",
                  request_overrides: dict[str, object] | None = None,
                  candidate_kwargs: dict[str, object] | None = None,
                  beets_staging_dir: str | None = None,
                  slskd_download_dir: str | None = None,
                  path_parent: str | None = None,
                  post_dispatch_fn: Callable[
                      [DispatchOutcome, FakePipelineDB, str], None,
                  ] | None = None,
                  finalize: bool = True) -> _DispatchWorld:
        from lib.dispatch import dispatch_import_core
        if ir is None:
            ir = make_import_result(decision="import", new_min_bitrate=245)

        # Automation must resolve its processing owner to wanted/imported.
        # Retained-current-status outcomes are operator-owned force imports.
        force = force or not requeue_on_failure

        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            verified_lossless_target=verified_lossless_target,
            beets_staging_dir=beets_staging_dir or "/staging",
            slskd_download_dir=slskd_download_dir or "/slskd",
        )
        dl_info = DownloadInfo(username=source_username)

        tmpdir = tempfile.mkdtemp(dir=path_parent)
        try:
            db = FakePipelineDB()
            req = make_request_row(
                id=42, status="downloading" if force else "wanted",
                mb_release_id="mbid-123",
                min_bitrate=180, current_spectral_bitrate=128,
                active_download_state={
                    "files": [],
                    "filetype": "flac",
                    "current_path": tmpdir,
                } if force else None,
                **(request_overrides or {}),
            )
            db.seed_request(req)
            preview_lease: ExecutionLeaseSnapshot | None = None
            if force:
                job = db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=42,
                    payload={"download_log_id": 1, "failed_path": tmpdir},
                )
            else:
                state = {
                    "files": [],
                    "filetype": "flac",
                    "enqueued_at": "2026-07-29T00:00:00+00:00",
                    "current_path": tmpdir,
                }
                job = handoff_automation_owner(
                    db,
                    42,
                    state=state,
                    canonical_path=tmpdir,
                )
                preview_lease = ExecutionLeaseSnapshot(
                    host_boot_id="dispatch-core-boot",
                    invocation_id=f"dispatch-core-preview-{job.id}",
                    systemd_unit="cratedigger-import-preview-worker.service",
                    worker=ProcessIdentity(8201, 82001),
                )
                claimed_preview = db.claim_next_import_preview_job(
                    worker_id="dispatch-core-preview",
                    execution_lease=preview_lease,
                )
                assert claimed_preview is not None
            candidate = _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
                expected_execution_lease=preview_lease,
                **(candidate_kwargs or {}),
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
                expected_execution_lease=preview_lease,
            )
            execution_lease = (
                None
                if force
                else ExecutionLeaseSnapshot(
                    host_boot_id="dispatch-core-boot",
                    invocation_id=f"dispatch-core-importer-{job.id}",
                    systemd_unit="cratedigger-importer.service",
                    worker=ProcessIdentity(8202, 82002),
                )
            )
            claimed = db.claim_next_import_job(
                worker_id="dispatch-core-test",
                execution_lease=execution_lease,
            )
            assert claimed is not None
            cancellation_token = (
                CancellationToken() if execution_lease is not None else None
            )
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 pinned_dispatch_authority(
                     db,
                     execution_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    force=force,
                    override_min_bitrate=override_min_bitrate,
                    target_format=target_format,
                    verified_lossless_target=verified_lossless_target,
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username=source_username or "user1",
                                     filename="01 - Track.mp3")],
                    cfg=cfg,
                    outcome_label=outcome_label,
                    requeue_on_failure=requeue_on_failure,
                    candidate_import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                    evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(
                        candidate=candidate,
                    ),
                    execution_lease=execution_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                    run_import_fn=(
                        _owned_test_runner
                        if execution_lease is not None else None
                    ),
                )
                if post_dispatch_fn is not None:
                    post_dispatch_fn(result, db, tmpdir)
                if not finalize:
                    pass
                elif force and result.terminal_outcome is not None:
                    db.persist_import_terminal_outcome(
                        result.terminal_outcome.with_job(ImportJobTerminal(
                            status="completed" if result.success else "failed",
                            result={"success": result.success},
                            message=result.message,
                            error=None if result.success else result.message,
                        ))
                    )
                elif not force:
                    finalize_claimed_dispatch(db, claimed, result)
                cmd = ext.run.call_args[0][0] if ext.run.call_args else []
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        return {
            "result": result,
            "cmd": cmd,
            "db": db,
            "path": tmpdir,
            "cleanup_calls": ext.cleanup.call_count,
        }

    # --- Success path ---

    def test_successful_import_marks_imported(self):
        r = self._dispatch()
        self.assertTrue(r["result"].success)
        self.assertEqual(r["db"].request(42)["status"], "imported")

    def test_audio_corrupt_automation_uses_beets_staging_quarantine_root(self):
        r = self._dispatch(
            candidate_kwargs={"audio_corrupt": True},
            beets_staging_dir="/configured/staging",
            slskd_download_dir="/configured/slskd",
            finalize=False,
        )
        cleanup = r["result"].post_commit_cleanup
        assert cleanup is not None
        self.assertEqual(cleanup.audio_quarantine_root, "/configured/staging")
        self.assertNotEqual(cleanup.audio_quarantine_root, "/configured/slskd")

    def test_audio_corrupt_dispatch_composes_atomic_staging_quarantine(self):
        with tempfile.TemporaryDirectory() as root:
            incoming = os.path.join(root, "Incoming")
            auto_import = os.path.join(incoming, "auto-import")
            slskd = os.path.join(root, "slskd")
            os.makedirs(auto_import)
            os.makedirs(os.path.join(incoming, "failed_imports"))
            os.makedirs(slskd)
            observed: dict[str, object] = {}
            def post_dispatch(
                result: DispatchOutcome,
                db: FakePipelineDB,
                source: str,
            ) -> None:
                os.makedirs(os.path.join(source, "Disc 1"))
                with open(os.path.join(source, "Disc 1", "01.flac"), "wb") as f:
                    f.write(b"bad")
                with open(os.path.join(source, "cover.jpg"), "wb") as f:
                    f.write(b"cover")
                observed["source"] = source
            dispatched = self._dispatch(
                candidate_kwargs={"audio_corrupt": True}, path_parent=auto_import,
                beets_staging_dir=incoming, slskd_download_dir=slskd,
                post_dispatch_fn=post_dispatch,
            )
            source = observed["source"]
            assert isinstance(source, str)
            completed_job = dispatched["db"].get_import_job(1)
            assert completed_job is not None
            receipt = (completed_job.result or {})["processing_cleanup"]
            assert isinstance(receipt, dict)
            target = receipt["selected_destination_path"]
            assert isinstance(target, str)
            self.assertFalse(os.path.exists(source))
            self.assertTrue(target.startswith(os.path.join(incoming, "failed_imports", "bad_files")))
            self.assertFalse(target.startswith(slskd))
            self.assertTrue(os.path.exists(os.path.join(target, "Disc 1", "01.flac")))
            self.assertTrue(os.path.exists(os.path.join(target, "cover.jpg")))
            self.assertEqual(receipt["outcome"], "completed")
            audit = msgspec.json.decode(
                dispatched["db"].download_logs[-1].validation_result or "{}",
            )
            self.assertEqual(audit["processing_cleanup"], receipt)

    def test_successful_import_creates_one_log_row(self):
        r = self._dispatch()
        self.assertEqual(len(r["db"].download_logs), 1)
        self.assertEqual(r["db"].download_logs[0].outcome, "success")

    def test_job_owned_destructive_cleanup_is_returned_for_post_commit(self):
        r = self._dispatch()

        self.assertEqual(r["cleanup_calls"], 0)
        cleanup = r["result"].post_commit_cleanup
        assert cleanup is not None
        self.assertEqual(cleanup.staged_path, r["path"])

    def test_stale_request_stops_before_import_subprocess(self):
        from lib.dispatch import dispatch_import_core

        class StaleDB(FakePipelineDB):
            def authorize_import_job_launch(self, *args, **kwargs):
                return None

        db = StaleDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            mb_release_id="mbid-123",
            active_download_state={"files": [], "filetype": "flac"},
        ))
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                mb_release_id="mbid-123",
            ))
            state = {
                    "files": [],
                    "filetype": "flac",
                    "enqueued_at": "2026-07-29T00:00:00+00:00",
                    "current_path": tmpdir,
            }
            job = handoff_automation_owner(
                db,
                42,
                state=state,
                canonical_path=tmpdir,
            )
            preview_lease = ExecutionLeaseSnapshot(
                host_boot_id="stale-test-boot",
                invocation_id="stale-preview",
                systemd_unit="cratedigger-import-preview-worker.service",
                worker=ProcessIdentity(8301, 83001),
            )
            assert db.claim_next_import_preview_job(
                worker_id="stale-preview",
                execution_lease=preview_lease,
            ) is not None
            candidate = _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
                expected_execution_lease=preview_lease,
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
                expected_execution_lease=preview_lease,
            )
            execution_lease = ExecutionLeaseSnapshot(
                host_boot_id="stale-test-boot",
                invocation_id="stale-importer",
                systemd_unit="cratedigger-importer.service",
                worker=ProcessIdentity(8302, 83002),
            )
            assert db.claim_next_import_job(
                worker_id="stale-test",
                execution_lease=execution_lease,
            ) is not None
            cancellation_token = CancellationToken()
            with patch_dispatch_externals() as ext, \
                 pinned_dispatch_authority(
                     db,
                     execution_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                outcome = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    force=False,
                    override_min_bitrate=None,
                    target_format=None,
                    verified_lossless_target="",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    candidate_import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                    evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(
                        candidate=candidate,
                    ),
                    execution_lease=execution_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                    run_import_fn=_owned_test_runner,
                )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, "launch_authority_conflict")
        ext.run.assert_not_called()
        self.assertEqual(db.request(42)["status"], "processing")
        self.assertEqual(db.download_logs, [])

    def test_force_job_status_change_after_enqueue_stops_before_beets(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                mb_release_id="mbid-123",
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": tmpdir},
            )
            candidate = _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="stale-force-test")
            assert claimed is not None
            self.assertEqual(claimed.expected_request_status, "wanted")

            # The job was prepared under wanted. A later request transition
            # cannot become its own expectation at the launch boundary.
            db.request(42)["status"] = "imported"
            recorder = MagicMock()
            outcome = dispatch_import_core(
                path=tmpdir,
                mb_release_id="mbid-123",
                request_id=42,
                label="Test Artist - Test Album",
                force=True,
                beets_harness_path=cfg.beets_harness_path,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(username="user1"),
                distance=0.05,
                scenario="force_import",
                cfg=cfg,
                candidate_import_job_id=job.id,
                quality_gate_fn=noop_quality_gate,
                evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(
                    candidate=candidate,
                ),
                run_import_fn=recorder,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, "launch_authority_conflict")
        recorder.assert_not_called()
        current = db.get_import_job(job.id)
        assert current is not None
        self.assertIsNone(current.beets_launch_authorized_at)

    def test_outcome_label_in_download_log(self):
        r = self._dispatch(outcome_label="force_import")
        self.assertEqual(r["db"].download_logs[0].outcome, "force_import")

    def test_start_log_names_automatic_operation_not_eventual_outcome(self):
        with self.assertLogs("cratedigger", level="INFO") as captured:
            self._dispatch(force=False, outcome_label="success")
        self.assertTrue(any(
            "AUTO-IMPORT: Test Artist - Test Album" in message
            for message in captured.output
        ))

    def test_start_log_names_force_operation(self):
        with self.assertLogs("cratedigger", level="INFO") as captured:
            self._dispatch(force=True, outcome_label="force_import")
        self.assertTrue(any(
            "FORCE-IMPORT: Test Artist - Test Album" in message
            for message in captured.output
        ))

    # --- Downgrade prevention ---

    def test_downgrade_prevented(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        self.assertFalse(r["result"].success)

    def test_downgrade_logs_rejection(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        self.assertEqual(len(r["db"].download_logs), 1)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertIn("quality_downgrade", r["db"].download_logs[0].beets_scenario or "")

    def test_downgrade_denylists_user(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, source_username="baduser")
        denylisted = [e.username for e in r["db"].denylist]
        self.assertIn("baduser", denylisted)

    def test_persisted_candidate_evidence_rejects_before_mutating_import(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "flac"},
        ))
        log_id = db.log_download(request_id=42, outcome="rejected")
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"download_log_id": 1, "failed_path": "/tmp/pending"},
        )

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.flac", "wb") as handle:
                handle.write(b"audio")
            with open(f"{current_dir}/01.mp3", "wb") as handle:
                handle.write(b"current")
            files = snapshot_audio_files(tmpdir)
            _seed_candidate_for_download_log(
                db, log_id,
                mb_release_id="mbid-123",
                files=files,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=900,
                    avg_bitrate_kbps=900,
                    median_bitrate_kbps=900,
                    format="FLAC",
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=128,
                    spectral_subject="source",
                    spectral_provenance="measured",
                ),
                codec="flac",
                container="flac",
                storage_format="flac",
                target_format="opus 128",
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=141,
                    avg_bitrate_kbps=240,
                    median_bitrate_kbps=240,
                    subject="source",
                ),
            )
            _seed_current_for_request(
                db, 42,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(current_dir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=116,
                    avg_bitrate_kbps=131,
                    median_bitrate_kbps=131,
                    format="Opus",
                    spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=96,
                    spectral_subject="source",
                    spectral_provenance="carried",
                ),
                codec="opus",
                container="opus",
                storage_format="opus",
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=211,
                    avg_bitrate_kbps=260,
                    median_bitrate_kbps=260,
                    subject="source",
                ),
            )
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
                verified_lossless_target="opus 128",
            )
            with patch_dispatch_externals() as ext:
                ext.run.side_effect = AssertionError(
                    "importer measurement/probe mutant executed"
                )
                with _patch_beets_album(current_dir, min_bitrate=116):
                    result = dispatch_import_core(
                        path=tmpdir,
                        mb_release_id="mbid-123",
                        request_id=42,
                        label="Test Artist - Test Album",
                        force=True,
                        target_format="opus 128",
                        verified_lossless_target="opus 128",
                        beets_harness_path=cfg.beets_harness_path,
                        db=db,  # type: ignore[arg-type]
                        dl_info=DownloadInfo(username="baduser"),
                        distance=0.99,
                        scenario="force_import",
                        files=[MagicMock(username="baduser", filename="01.flac")],
                        cfg=cfg,
                        requeue_on_failure=False,
                        candidate_download_log_id=log_id,
                        candidate_import_job_id=job.id,
                    )

            self.assertFalse(result.success)
            ext.run.assert_not_called()
            self.assertIsNotNone(result.terminal_outcome)
            assert result.terminal_outcome is not None
            self.assertEqual(result.terminal_outcome.audit.outcome, "rejected")
            self.assertEqual(
                result.terminal_outcome.audit.source_download_log_id,
                log_id,
            )
            denylisted = [
                entry.username for entry in result.terminal_outcome.denylists
            ]
            self.assertIn("baduser", denylisted)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_persisted_candidate_evidence_imports_via_evidence_action_file(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            mb_release_id="mbid-123",
            active_download_state={"files": [], "filetype": "flac"},
        ))

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            with open(f"{current_dir}/01.mp3", "wb") as handle:
                handle.write(b"current")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": tmpdir},
            )
            import_job_id = job.id
            files = snapshot_audio_files(tmpdir)
            _seed_candidate_for_import_job(
                db, import_job_id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
                files=files,
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
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            assert db.claim_next_import_job(worker_id="dispatch-test") is not None
            _seed_current_for_request(
                db, 42,
                mb_release_id="mbid-123-current",
                files=snapshot_audio_files(current_dir),
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
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            ir = make_import_result(decision="import", new_min_bitrate=245)
            decoded_payload: dict[str, QualityEvidenceActionPayload] = {}
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 _patch_beets_album(current_dir, min_bitrate=128):
                def run_side_effect(cmd, *_args, **_kwargs):
                    idx = cmd.index("--quality-evidence-action-file")
                    with open(cmd[idx + 1], "rb") as handle:
                        decoded_payload["payload"] = msgspec.json.decode(
                            handle.read(),
                            type=QualityEvidenceActionPayload,
                        )
                    return MagicMock(returncode=0, stdout="", stderr="")

                ext.run.side_effect = run_side_effect
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    candidate_import_job_id=import_job_id,
                    quality_gate_fn=noop_quality_gate,
                )

            self.assertTrue(result.success)
            cmd = ext.run.call_args[0][0]
            self.assertIn("--quality-evidence-action-file", cmd)
            self.assertNotIn("--preview-import-result-file", cmd)
            payload = decoded_payload["payload"]
            # Post-migration 021: candidate evidence is content-addressed by
            # (mb_release_id, snapshot_fingerprint); addressing back to the
            # import_job is via the FK we wired in the helper.
            self.assertEqual(payload.candidate.mb_release_id, "mbid-123")
            assert payload.current is not None
            self.assertEqual(payload.current.mb_release_id, "mbid-123")
            self.assertIs(payload.decision["imported"], True)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_legacy_successful_lossy_import_clears_old_verified_lossless_proof(self):
        from lib.dispatch import _refresh_current_evidence_after_import

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="unsearchable",
            verified_lossless=False,
        ))
        proof = VerifiedLosslessProof(
            provenance="measured",
            source="flac",
            classifier="spectral_verified_lossless",
            detail="genuine",
        )
        _seed_current_for_request(
            db, 42,
            mb_release_id="mbid-123",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=116,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
            ),
            verified_lossless_proof=proof,
            storage_format="Opus",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            with _patch_beets_album(tmpdir, min_bitrate=245):
                _refresh_current_evidence_after_import(
                    db,
                    request_id=42,
                    mb_release_id="mbid-123",
                    quality_ranks=None,
                    source_candidate=None,
                    import_result=ImportResult(
                        decision="import",
                        source_measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=245,
                            avg_bitrate_kbps=256,
                            median_bitrate_kbps=252,
                            format="MP3",
                        ),
                    ),
                )

        # After the refresh, request_current FK points at the new evidence.
        refreshed_id = db.get_request_current_evidence_id(42)
        self.assertIsNotNone(refreshed_id)
        loaded = db.load_album_quality_evidence_by_id(refreshed_id)
        assert loaded is not None
        self.assertIsNone(loaded.verified_lossless_proof)

    def test_persisted_candidate_evidence_imports_when_no_current_album(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            mb_release_id="mbid-123",
            active_download_state={"files": [], "filetype": "flac"},
        ))

        tmpdir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": tmpdir},
            )
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
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
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            assert db.claim_next_import_job(worker_id="dispatch-test") is not None
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            ir = make_import_result(decision="import", new_min_bitrate=245)
            decoded_payload: dict[str, QualityEvidenceActionPayload] = {}
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 _patch_beets_album(None):
                def run_side_effect(cmd, *_args, **_kwargs):
                    idx = cmd.index("--quality-evidence-action-file")
                    with open(cmd[idx + 1], "rb") as handle:
                        decoded_payload["payload"] = msgspec.json.decode(
                            handle.read(),
                            type=QualityEvidenceActionPayload,
                        )
                    return MagicMock(returncode=0, stdout="", stderr="")

                ext.run.side_effect = run_side_effect
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    candidate_import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                )

            self.assertTrue(result.success)
            payload = decoded_payload["payload"]
            self.assertIsNone(payload.current)
            self.assertEqual(payload.provenance.current_status, "missing")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_stale_current_backfill_requires_fresh_enrichment_before_decision(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"candidate")
            with open(f"{current_dir}/01.mp3", "wb") as handle:
                handle.write(b"current")
            old_current_files = snapshot_audio_files(current_dir)
            with open(f"{current_dir}/01.mp3", "ab") as handle:
                handle.write(b" changed")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": tmpdir},
            )
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123-candidate",
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
            _seed_current_for_request(
                db, 42,
                mb_release_id="mbid-123",
                files=old_current_files,
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
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            with patch_dispatch_externals() as ext, \
                 _patch_beets_album(current_dir, min_bitrate=320):
                ext.run.side_effect = AssertionError(
                    "importer measurement/probe mutant executed"
                )
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    requeue_on_failure=False,
                    candidate_import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertIn("Installed HAVE analysis failed", result.message)
            self.assertEqual(result.code, "have_analysis_error")
            ext.run.assert_not_called()
            refreshed_id = db.get_request_current_evidence_id(42)
            self.assertIsNotNone(refreshed_id)
            refreshed = db.load_album_quality_evidence_by_id(refreshed_id)
            assert refreshed is not None
            self.assertEqual(refreshed.measurement.min_bitrate_kbps, 320)
            self.assertIsNone(refreshed.measurement.spectral_grade)
            self.assertIsNone(refreshed.v0_metric)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_persisted_candidate_evidence_fails_when_current_album_has_no_files(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"candidate")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": tmpdir},
            )
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
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            with patch_dispatch_externals() as ext, \
                 _patch_beets_album(current_dir, min_bitrate=320):
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    requeue_on_failure=False,
                    candidate_import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertIn("Installed HAVE analysis failed", result.message)
            ext.run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_persisted_candidate_evidence_fails_closed_on_current_error(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": tmpdir},
            )
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
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            with patch_dispatch_externals() as ext, \
                 _patch_beets_album(tmpdir, min_bitrate=128), \
                 patch(
                     "lib.import_evidence.ensure_current_evidence_for_action",
                     side_effect=RuntimeError("beets unavailable"),
                 ):
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    candidate_import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertIn("Installed HAVE analysis failed", result.message)
            ext.run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_downgrade_preserves_validation_result_and_staged_path(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, requeue_on_failure=False)
        log = r["db"].download_logs[0]
        self.assertEqual(log.staged_path, r["path"])
        self.assertIsNotNone(log.validation_result)
        self.assertIn("quality_downgrade", log.validation_result or "")

    # --- Requeue behavior ---

    def test_failed_no_requeue_stays_downloading(self):
        """When requeue_on_failure=False, status should not change to wanted."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, requeue_on_failure=False)
        # Should NOT have transitioned to wanted
        self.assertNotEqual(r["db"].request(42)["status"], "wanted")

    def test_failed_with_requeue_transitions_to_wanted(self):
        """When requeue_on_failure=True, failed import requeues to wanted."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, requeue_on_failure=True)
        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["last_attempt_at"])
        self.assertIsNotNone(row["next_retry_after"])

    # --- Transcode paths ---

    def test_transcode_upgrade_requeues_for_better(self):
        ir = make_import_result(decision="transcode_upgrade",
                                new_min_bitrate=227)
        r = self._dispatch(ir=ir)
        self.assertTrue(r["result"].success)
        # Should be requeued to wanted for upgrade search
        self.assertEqual(r["db"].request(42)["status"], "wanted")

    def test_transcode_upgrade_denylists_user(self):
        ir = make_import_result(decision="transcode_upgrade",
                                new_min_bitrate=227)
        r = self._dispatch(ir=ir, source_username="transuser")
        denylisted = [e.username for e in r["db"].denylist]
        self.assertIn("transuser", denylisted)

    def test_transcode_downgrade_no_requeue_when_disabled(self):
        ir = make_import_result(decision="transcode_downgrade",
                                new_min_bitrate=190, prev_min_bitrate=320)
        r = self._dispatch(ir=ir, requeue_on_failure=False)
        self.assertNotEqual(r["db"].request(42)["status"], "wanted")

    # --- lossless_source_locked + search-narrowing (R7, AE2) ---

    def test_lossless_source_locked_narrows_search_filetype_override(self):
        """R7 / AE2: when a lossy candidate hits lossless_source_locked
        during importer dispatch, the request's search_filetype_override
        narrows to 'lossless' so future cycles only ask for lossless
        candidates that can actually win against the existing
        lossless-source library row.

        Without this narrowing, the search planner keeps re-asking
        Soulseek with no filetype filter, each new peer serves the
        same lossy file, and the lock fires repeatedly. The narrowing
        closes that wasted-cycle window.
        """
        ir = make_import_result(decision="lossless_source_locked")
        r = self._dispatch(
            ir=ir,
            request_overrides={
                "search_filetype_override": "lossless,mp3 v0,mp3 320",
            },
        )
        self.assertEqual(
            r["db"].request(42)["search_filetype_override"], "lossless")

    def test_lossless_source_locked_narrowing_is_idempotent(self):
        """AE7: when the override is already 'lossless', the lock
        firing again is a no-op (no spurious DB write that would churn
        change tracking or audit logs)."""
        ir = make_import_result(decision="lossless_source_locked")
        r = self._dispatch(
            ir=ir,
            request_overrides={"search_filetype_override": "lossless"},
        )
        self.assertEqual(
            r["db"].request(42)["search_filetype_override"], "lossless")


class TestDispatchCoreSeams(unittest.TestCase):
    """Seam tests — assert subprocess argv construction."""

    def _get_cmd(self, **kwargs):
        from lib.dispatch import dispatch_import_core
        ir = kwargs.pop("ir", make_import_result())
        beets_directory = kwargs.pop("beets_directory", "")
        beets_config_dir = kwargs.pop("beets_config_dir", "")
        beets_python = kwargs.pop("beets_python", "")
        runner_hook = kwargs.pop("runner_hook", None)
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            beets_directory=beets_directory,
            beets_config_dir=beets_config_dir,
            beets_python=beets_python,
            pipeline_db_enabled=True,
        )
        tmpdir = tempfile.mkdtemp()
        try:
            db = FakePipelineDB()
            force = bool(kwargs.get("force", False))
            db.seed_request(make_request_row(
                id=42,
                status="downloading" if force else "wanted",
                mb_release_id="mbid-123",
                active_download_state={
                    "files": [],
                    "filetype": "flac",
                    "current_path": tmpdir,
                } if force else None,
            ))
            preview_lease: ExecutionLeaseSnapshot | None = None
            if force:
                job = db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=42,
                    payload={"download_log_id": 1, "failed_path": tmpdir},
                )
            else:
                job = handoff_automation_owner(
                    db,
                    42,
                    state={
                        "files": [],
                        "filetype": "flac",
                        "enqueued_at": "2026-07-29T00:00:00+00:00",
                        "current_path": tmpdir,
                    },
                    canonical_path=tmpdir,
                )
                preview_lease = ExecutionLeaseSnapshot(
                    host_boot_id="seam-test-boot",
                    invocation_id=f"seam-preview-{job.id}",
                    systemd_unit="cratedigger-import-preview-worker.service",
                    worker=ProcessIdentity(8401, 84001),
                )
                assert db.claim_next_import_preview_job(
                    worker_id="seam-preview",
                    execution_lease=preview_lease,
                ) is not None
            candidate = _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
                expected_execution_lease=preview_lease,
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
                expected_execution_lease=preview_lease,
            )
            execution_lease = (
                None
                if force
                else ExecutionLeaseSnapshot(
                    host_boot_id="seam-test-boot",
                    invocation_id=f"seam-importer-{job.id}",
                    systemd_unit="cratedigger-importer.service",
                    worker=ProcessIdentity(8402, 84002),
                )
            )
            assert db.claim_next_import_job(
                worker_id="seam-test",
                execution_lease=execution_lease,
            ) is not None
            cancellation_token = (
                CancellationToken() if execution_lease is not None else None
            )
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 pinned_dispatch_authority(
                     db,
                     execution_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                if runner_hook is not None:
                    kwargs["run_import_fn"] = runner_hook
                elif execution_lease is not None:
                    kwargs["run_import_fn"] = _owned_test_runner
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(),
                    cfg=cfg,
                    candidate_import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                    evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(
                        candidate=candidate,
                    ),
                    execution_lease=execution_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                    **kwargs,
                )
                return ext.run.call_args[0][0] if ext.run.call_args else []
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_force_flag_passed(self):
        cmd = self._get_cmd(force=True)
        self.assertIn("--force", cmd)

    def test_no_force_by_default(self):
        cmd = self._get_cmd(force=False)
        self.assertNotIn("--force", cmd)

    def test_override_min_bitrate_passed(self):
        cmd = self._get_cmd(override_min_bitrate=128)
        idx = cmd.index("--override-min-bitrate")
        self.assertEqual(cmd[idx + 1], "128")

    def test_verified_lossless_target_flag(self):
        cmd = self._get_cmd(verified_lossless_target="opus 128")
        self.assertIn("--verified-lossless-target", cmd)
        idx = cmd.index("--verified-lossless-target")
        self.assertEqual(cmd[idx + 1], "opus 128")

    def test_target_format_flag(self):
        cmd = self._get_cmd(target_format="flac")
        self.assertIn("--target-format", cmd)
        idx = cmd.index("--target-format")
        self.assertEqual(cmd[idx + 1], "flac")

    def test_shared_import_one_command_supports_preview_without_request_id(self):
        from lib.dispatch import build_import_one_command

        cmd = build_import_one_command(
            path="/tmp/album",
            mb_release_id="mbid-123",
            beets_harness_path=_HARNESS,
            dry_run=True,
            preserve_source=True,
        )

        self.assertIn("--dry-run", cmd)
        self.assertIn("--preserve-source", cmd)
        self.assertNotIn("--request-id", cmd)

    def test_shared_import_one_command_does_not_accept_preview_result_file(self):
        from lib.dispatch import build_import_one_command

        cmd = build_import_one_command(
            path="/tmp/album",
            mb_release_id="mbid-123",
            beets_harness_path=_HARNESS,
        )

        self.assertNotIn("--preview-import-result-file", cmd)

    def test_dispatch_core_has_no_preview_import_result_channel(self):
        cmd = self._get_cmd()

        self.assertNotIn("--preview-import-result-file", cmd)

    def test_injected_runner_gets_authority_snapshotted_before_runtime_swap(self):
        received: dict[str, object] = {}
        with tempfile.TemporaryDirectory() as root:
            swapped = os.path.join(root, "swapped-runtime.ini")
            with open(swapped, "w", encoding="utf-8") as handle:
                handle.write("[Beets]\nlibrary = /swapped/library.db\n")

            def runner_after_swap(
                *,
                beets_config_dir: str | None,
                beets_python: str | None,
                beets_library_db_path: str | None,
                beets_library_root: str | None,
                **_kwargs: object,
            ) -> ImportOneRun:
                os.environ["CRATEDIGGER_RUNTIME_CONFIG"] = swapped
                received.update({
                    "beets_config_dir": beets_config_dir,
                    "beets_python": beets_python,
                    "beets_library_db_path": beets_library_db_path,
                    "beets_library_root": beets_library_root,
                })
                return ImportOneRun(
                    command=(), returncode=1, stdout="", stderr="", import_result=None,
                )

            with patch.dict(os.environ, {}, clear=False):
                self._get_cmd(
                    beets_config_dir="/original/beets-config",
                    beets_python="/original/pinned-python",
                    beets_library_db_path="/original/library.db",
                    beets_library_root="/original/library",
                    runner_hook=runner_after_swap,
                )

        self.assertEqual(received["beets_config_dir"], "/original/beets-config")
        self.assertEqual(received["beets_python"], "/original/pinned-python")
        self.assertEqual(received["beets_library_db_path"], "/original/library.db")
        self.assertEqual(received["beets_library_root"], "/original/library")


if __name__ == "__main__":
    unittest.main()
