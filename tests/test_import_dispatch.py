"""Tests for lib/dispatch/ — auto-import decision tree.

Orchestration tests (TestDispatchImport, TestQualityGate*) use FakePipelineDB
and assert domain state. Seam tests (TestOverrideMinBitrate, TestOpus*,
TestTargetFormat*) exercise the surviving auto-import seam in
``lib.download_validation._handle_valid_result`` and the core subprocess wiring.
Pure function tests (TestPopulateDlInfo*, TestCleanupStagedDir) test in/out.
"""
import configparser
import inspect
import json
import os
import shutil
import sqlite3
import subprocess as sp
import tempfile
import threading
import unittest
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import replace
from datetime import UTC
from typing import ClassVar, Never
from unittest.mock import MagicMock, patch

import msgspec

from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.dispatch import core as dispatch_core_module
from lib.dispatch.types import DispatchDB, DispatchOutcome, DispatchRequest
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    MonitoredProcessGroup,
    ProcessIdentity,
)
from lib.import_queue import ImportJob
from lib.library_delete_notifiers import DeleteNotification
from lib.quality import (
    QUALITY_FLAC_ONLY,
    QUALITY_UPGRADE_TIERS,
    V0_PROBE_LOSSLESS_SOURCE,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    CodecFamily,
    ConversionInfo,
    DownloadInfo,
    DuplicateRemoveCandidate,
    DuplicateRemoveGuardInfo,
    EvidenceProvenance,
    EvidenceSubject,
    ImportResult,
    QualityRankConfig,
    SpectralMeasurement,
    TargetQualityContract,
    V0ProbeEvidence,
    ValidationResult,
    VerifiedLosslessProof,
)
from lib.quality_evidence import snapshot_audio_files, snapshot_fingerprint
from tests.dispatch_helpers import (
    RecordingQualityGate,
    claim_next_import_job,
    claim_next_import_preview_job,
    finalize_claimed_dispatch,
    make_dispatch_request,
    noop_quality_gate,
    patch_dispatch_externals,
    pinned_dispatch_authority,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import (
    hermetic_beets_config_defaults,
    make_ctx_with_fake_db,
    make_download_file,
    make_import_result,
    make_request_row,
)

_HERMETIC_BEETS_DEFAULTS: AbstractContextManager[tuple[str, str]] | None = None
_HERMETIC_BEETS_PAIR: tuple[str, str] | None = None


def setUpModule() -> None:
    global _HERMETIC_BEETS_DEFAULTS, _HERMETIC_BEETS_PAIR
    _HERMETIC_BEETS_DEFAULTS = hermetic_beets_config_defaults()
    _HERMETIC_BEETS_PAIR = _HERMETIC_BEETS_DEFAULTS.__enter__()


def tearDownModule() -> None:
    assert _HERMETIC_BEETS_DEFAULTS is not None
    _HERMETIC_BEETS_DEFAULTS.__exit__(None, None, None)


class TestHermeticBeetsConfigDefaults(unittest.TestCase):
    def test_implicit_config_uses_disposable_complete_pair(self) -> None:
        from lib.beets_db import validate_beets_storage_pair

        config = CratediggerConfig()

        self.assertNotIn(config.beets_library_db, {
            "/mnt/virtio/Music/beets-library.db",
            "/var/lib/cratedigger-beets-db/beets-library.db",
        })
        self.assertNotIn(config.beets_directory, {
            "/mnt/virtio/Music/Beets",
            "/var/lib/cratedigger",
        })
        self.assertTrue(os.path.isfile(config.beets_library_db))
        self.assertTrue(os.path.isdir(config.beets_directory))
        validate_beets_storage_pair(
            db_path=config.beets_library_db,
            library_root=config.beets_directory,
        )

    def test_direct_config_rejects_one_sided_authority(self) -> None:
        for library_db in (
            "/mnt/virtio/Music/beets-library.db",
            "/var/lib/cratedigger-beets-db/beets-library.db",
        ):
            with self.subTest(library_db=library_db), self.assertRaisesRegex(AssertionError, "both library DB and root"):
                CratediggerConfig(beets_library_db=library_db)
        with self.assertRaisesRegex(AssertionError, "both library DB and root"):
            CratediggerConfig(beets_directory="/music/library")

    def test_ini_config_requires_complete_authority(self) -> None:
        assert _HERMETIC_BEETS_PAIR is not None
        library_db, library_root = _HERMETIC_BEETS_PAIR

        absent = CratediggerConfig.from_ini(configparser.ConfigParser())
        self.assertEqual(absent.beets_library_db, library_db)
        self.assertEqual(absent.beets_directory, library_root)

        for library in (
            "/mnt/virtio/Music/beets-library.db",
            "/var/lib/cratedigger-beets-db/beets-library.db",
        ):
            partial = configparser.ConfigParser()
            partial["Beets"] = {"library": library}
            with self.subTest(library=library), self.assertRaisesRegex(AssertionError, "both library and directory"):
                CratediggerConfig.from_ini(partial)

        root_only = configparser.ConfigParser()
        root_only["Beets"] = {"directory": "/music/library"}
        with self.assertRaisesRegex(AssertionError, "both library and directory"):
            CratediggerConfig.from_ini(root_only)

        complete = configparser.ConfigParser()
        complete["Beets"] = {
            "library": library_db,
            "directory": library_root,
        }
        config = CratediggerConfig.from_ini(complete)
        self.assertEqual(config.beets_library_db, library_db)
        self.assertEqual(config.beets_directory, library_root)


# --- Local helpers for auto-import seam tests ---

def _make_album_data(artist="Test Artist", title="Test Album",
                     mb_release_id="test-mbid", db_request_id=42,
                     db_source="request"):
    """Build a mock GrabListEntry."""
    mock = MagicMock()
    mock.artist = artist
    mock.title = title
    mock.mb_release_id = mb_release_id
    mock.db_request_id = db_request_id
    mock.db_source = db_source
    mock.db_target_format = None
    mock.current_min_bitrate = None
    mock.current_spectral = None
    mock.files = [MagicMock(
        username="user1",
        filename="01 - Track.mp3",
        bitRate=None,
        sampleRate=None,
        bitDepth=None,
        isVariableBitRate=None,
    )]
    return mock


def _make_ctx(**cfg_overrides: object):
    """Build a CratediggerContext wired to a seeded FakePipelineDB.

    The DB is seeded with request id 42 in ``downloading`` status — the
    auto-import dispatch path expects to find an owning request. The
    config remains a ``MagicMock`` because the tests only read a handful
    of attributes from it; ``cfg`` is not a stateful-collaborator name
    in the audit's heuristic. ``cfg_overrides`` set additional attributes
    on that mock at construction time.
    """
    cfg = MagicMock()
    cfg.beets_harness_path = "/nix/store/fake/harness/run_beets_harness.sh"
    cfg.beets_distance_threshold = 0.15
    cfg.beets_staging_dir = "/tmp/staging"
    cfg.verified_lossless_target = ""
    cfg.quality_ranks = QualityRankConfig.defaults()
    for name, value in cfg_overrides.items():
        setattr(cfg, name, value)
    fake_db = FakePipelineDB()
    fake_db.seed_request(make_request_row(
        id=42,
        status="downloading",
        active_download_state={"files": [], "filetype": "mp3"},
    ))
    ctx = make_ctx_with_fake_db(fake_db, cfg=cfg)
    ctx.cooled_down_users = set()
    return ctx


def _ctx_cfg(ctx: CratediggerContext) -> MagicMock:
    """The ctx's ``_make_ctx`` MagicMock config, typed for post-hoc
    attribute setting (every ctx in this module carries one)."""
    cfg = ctx.cfg
    assert isinstance(cfg, MagicMock)
    return cfg


def _make_bv_result(distance=0.05):
    """Build a mock beets validation result with attribute access."""
    mock = MagicMock()
    mock.distance = distance
    mock.scenario = "strong_match"
    mock.detail = None
    mock.error = None
    mock.to_json.return_value = '{"valid": true}'
    return mock


_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"


def _full_dispatch_config() -> CratediggerConfig:
    ini = configparser.RawConfigParser()
    ini["Beets Validation"] = {"harness_path": _HARNESS}
    ini["Pipeline DB"] = {"enabled": "true"}
    return CratediggerConfig.from_ini(ini)


def _claim_dispatch_job(
    db: FakePipelineDB,
    *,
    path: str,
    release_id: str,
    force: bool = False,
    request_id: int = 42,
    evidence_kwargs=None,
    candidate_evidence=None,
):
    """Create the production-shaped job/evidence authority for a core test."""
    from lib.import_evidence import (
        ActionEvidenceProvenance,
        CandidateEvidenceActionResult,
    )
    from lib.import_queue import IMPORT_JOB_FORCE
    from tests.dispatch_helpers import handoff_automation_owner

    # Production-shaped dispatch tests must persist the snapshot of the path
    # they later hand to the freshness guard.
    os.makedirs(path, mode=0o700, exist_ok=True)
    fixture_track = os.path.join(path, "01 - Track.mp3")
    if not os.path.exists(fixture_track):
        with open(fixture_track, "wb") as handle:
            handle.write(b"fixture audio")
    files = snapshot_audio_files(path)
    request = db.request(request_id)
    request["mb_release_id"] = release_id
    preview_lease: ExecutionLeaseSnapshot | None = None
    if not force:
        state = dict(request.get("active_download_state") or {})
        state.setdefault("enqueued_at", "2026-07-29T00:00:00+00:00")
        state["current_path"] = path
        state.setdefault("filetype", "mp3")
        state.setdefault("files", [])
        db.seed_request({
            **request,
            "status": "wanted",
            "active_download_state": None,
            "active_automation_import_job_id": None,
        })
        job = handoff_automation_owner(
            db,
            request_id,
            state=state,
            canonical_path=path,
        )
        preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="dispatch-test-boot",
            invocation_id=f"dispatch-preview-{job.id}",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(8101, 81001),
        )
        claimed_preview = claim_next_import_preview_job(db, worker_id="dispatch-preview",
        execution_lease=preview_lease,)
        assert claimed_preview is not None
    else:
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            payload={"download_log_id": 1, "failed_path": path},
        )
    evidence = (
        msgspec.structs.replace(
            candidate_evidence,
            mb_release_id=release_id,
            source_path=path,
            files=files,
            snapshot_fingerprint=snapshot_fingerprint(files),
        )
        if candidate_evidence is not None
        else make_album_quality_evidence(
            mb_release_id=release_id,
            source_path=path,
            files=files,
            **(evidence_kwargs or {}),
        )
    )
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    if force:
        db.set_import_job_candidate_evidence(job.id, persisted.id)
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
        )
        execution_lease = None
    else:
        assert preview_lease is not None
        db.set_import_job_candidate_evidence(
            job.id,
            persisted.id,
            expected_execution_lease=preview_lease,
        )
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
            expected_execution_lease=preview_lease,
        )
        execution_lease = ExecutionLeaseSnapshot(
            host_boot_id="dispatch-test-boot",
            invocation_id=f"dispatch-importer-{job.id}",
            systemd_unit="cratedigger-importer.service",
            worker=ProcessIdentity(8102, 81002),
        )
    claimed = claim_next_import_job(db, worker_id="dispatch-test",
    execution_lease=execution_lease,)
    assert claimed is not None
    return claimed, CandidateEvidenceActionResult(
        evidence=persisted,
        provenance=ActionEvidenceProvenance(
            candidate_status="reused",
            snapshot_guard="matched",
        ),
    ), execution_lease


def _owned_test_runner(**kwargs):
    """Exercise the legacy patched sp.run seam after persisting child proof."""
    from lib.dispatch.subprocess_runner import run_import_one

    on_spawn = kwargs.pop("on_spawn", None)
    cancellation_token = kwargs.pop("cancellation_token", None)
    kwargs.pop("owner_session_probe", None)
    if on_spawn is not None:
        on_spawn(os.getpid())
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()
    return run_import_one(**kwargs)


class TestAutomationDispatchExecutionFence(unittest.TestCase):
    """Production dispatch retains exact automation authority around Beets."""

    def _world(self, root: str):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id="test-mbid",
        ))
        claimed, candidate, execution_lease = _claim_dispatch_job(
            db,
            path=root,
            release_id="test-mbid",
        )
        assert execution_lease is not None
        return db, claimed, candidate, execution_lease

    def _dispatch(
        self,
        *,
        root: str,
        db: FakePipelineDB,
        claimed,
        candidate,
        execution_lease: ExecutionLeaseSnapshot,
        token: CancellationToken,
        runner,
        evidence_gate_fn=None,
    ):
        from lib.dispatch import dispatch_import_core

        kwargs = {}
        if evidence_gate_fn is not None:
            kwargs["evidence_gate_fn"] = evidence_gate_fn
        with pinned_dispatch_authority(
            db,
            execution_lease,
            cancellation_token=token,
        ) as (pinned_token, owner_session_identity):
            assert pinned_token is token
            assert owner_session_identity is not None
            return dispatch_import_core(
                make_dispatch_request(
                    path=root,
                    mb_release_id="test-mbid",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype="mp3"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[],
                    candidate_import_job_id=claimed.id,
                    prevalidated_candidate_result=candidate,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                ),
                db,
                cfg=_full_dispatch_config(),
                quality_gate_fn=noop_quality_gate,
                cancellation_token=pinned_token,
                run_import_fn=runner,
                **kwargs,
            )

    def test_child_identity_is_persisted_before_wait_and_loss_stops_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            db, claimed, candidate, execution_lease = self._world(root)
            token = CancellationToken()
            observations: list[str] = []

            def lose_session_after_spawn(**kwargs):
                current = db.get_import_job(claimed.id)
                assert current is not None
                self.assertIsNotNone(current.beets_launch_authorized_at)
                self.assertIsNone(current.execution_beets_pid)
                observations.append("authorized")

                on_spawn = kwargs["on_spawn"]
                on_spawn(os.getpid())
                persisted = db.get_import_job(claimed.id)
                assert persisted is not None
                self.assertEqual(persisted.execution_beets_pid, os.getpid())
                self.assertIsNotNone(persisted.execution_beets_start_ticks)
                observations.append("child-persisted")

                token.cancel("owner_session_lost")
                token.raise_if_cancelled()
                raise AssertionError("cancelled runner must not return")

            with patch.object(
                dispatch_core_module,
                "_write_quality_evidence_action_file",
                return_value=None,
            ), patch.object(
                dispatch_core_module,
                "_refresh_current_evidence_after_import",
            ) as refresh, patch.object(
                dispatch_core_module,
                "_write_album_sidecar_after_import",
            ) as sidecar, self.assertRaisesRegex(
                ExecutionCancelled,
                "owner_session_lost",
            ):
                self._dispatch(
                    root=root,
                    db=db,
                    claimed=claimed,
                    candidate=candidate,
                    execution_lease=execution_lease,
                    token=token,
                    runner=lose_session_after_spawn,
                )

            self.assertEqual(observations, ["authorized", "child-persisted"])
            refresh.assert_not_called()
            sidecar.assert_not_called()
            persisted = db.get_import_job(claimed.id)
            assert persisted is not None
            self.assertEqual(persisted.status, "running")
            self.assertEqual(
                db.request(42)["active_automation_import_job_id"],
                claimed.id,
            )

    def test_owner_change_immediately_before_launch_refuses_spawn(self) -> None:
        from lib.dispatch.types import EvidenceImportGate

        with tempfile.TemporaryDirectory() as root:
            db, claimed, candidate, execution_lease = self._world(root)
            token = CancellationToken()
            runner = MagicMock()

            def revoke_owner(*_args, **_kwargs):
                request = db.request(42)
                db.seed_request({
                    **request,
                    "active_automation_import_job_id": claimed.id + 100,
                })
                return EvidenceImportGate(candidate=candidate.evidence)

            with patch.object(
                dispatch_core_module,
                "_write_quality_evidence_action_file",
                return_value=None,
            ):
                outcome = self._dispatch(
                    root=root,
                    db=db,
                    claimed=claimed,
                    candidate=candidate,
                    execution_lease=execution_lease,
                    token=token,
                    runner=runner,
                    evidence_gate_fn=revoke_owner,
                )

            self.assertEqual(outcome.code, "launch_authority_conflict")
            runner.assert_not_called()
            persisted = db.get_import_job(claimed.id)
            assert persisted is not None
            self.assertIsNone(persisted.beets_launch_authorized_at)
            self.assertIsNone(persisted.execution_beets_pid)

    def test_completion_is_captured_before_every_post_beets_effect(
        self,
    ) -> None:
        from lib.dispatch.types import ImportOneRun
        from lib.quality_evidence import EvidenceBuildResult

        with tempfile.TemporaryDirectory() as root:
            db, claimed, candidate, execution_lease = self._world(root)
            token = CancellationToken()
            order: list[str] = []
            original_capture = db.capture_automation_import_completion

            def capture(*args, **kwargs):
                order.append("completion-captured")
                return original_capture(*args, **kwargs)

            def runner(**kwargs):
                kwargs["on_spawn"](os.getpid())
                order.append("beets-returned")
                return ImportOneRun(
                    command=("import_one",),
                    returncode=0,
                    stdout="",
                    stderr="",
                    import_result=make_import_result(decision="import"),
                )

            def refresh_effect(*_args, **_kwargs):
                order.append("evidence-refreshed")
                return EvidenceBuildResult(
                    evidence=None,
                    status="failed",
                    reason="synthetic post-effect fixture",
                )

            with patch.object(
                db,
                "capture_automation_import_completion",
                side_effect=capture,
            ), patch.object(
                dispatch_core_module,
                "_write_quality_evidence_action_file",
                return_value="/tmp/automation-completion-action",
            ), patch.object(
                dispatch_core_module,
                "_remove_quality_evidence_action_file",
                side_effect=lambda _path: order.append("action-removed"),
            ), patch.object(
                dispatch_core_module,
                "_refresh_current_evidence_after_import",
                side_effect=refresh_effect,
            ), patch.object(
                dispatch_core_module,
                "_write_album_sidecar_after_import",
                side_effect=lambda *_args, **_kwargs: order.append(
                    "sidecar-written"
                ),
            ), patch_dispatch_externals():
                outcome = self._dispatch(
                    root=root,
                    db=db,
                    claimed=claimed,
                    candidate=candidate,
                    execution_lease=execution_lease,
                    token=token,
                    runner=runner,
                )

            self.assertTrue(outcome.success)
            self.assertEqual(order[:2], [
                "beets-returned",
                "completion-captured",
            ])
            capture_index = order.index("completion-captured")
            self.assertGreater(order.index("action-removed"), capture_index)
            for effect in ("evidence-refreshed", "sidecar-written"):
                if effect in order:
                    self.assertGreater(order.index(effect), capture_index)
            persisted = db.get_import_job(claimed.id)
            assert persisted is not None and persisted.result is not None
            receipt = persisted.result["automation_completion"]
            self.assertEqual(receipt["job_id"], claimed.id)
            self.assertEqual(receipt["request_id"], 42)
            self.assertEqual(receipt["canonical_path"], root)
            self.assertEqual(receipt["returncode"], 0)

    def test_completion_capture_conflict_stops_post_beets_effects(
        self,
    ) -> None:
        from lib.dispatch.types import ImportOneRun

        with tempfile.TemporaryDirectory() as root:
            db, claimed, candidate, execution_lease = self._world(root)
            token = CancellationToken()
            runner_returned: list[bool] = []

            def runner(**kwargs):
                kwargs["on_spawn"](os.getpid())
                runner_returned.append(True)
                return ImportOneRun(
                    command=("import_one",),
                    returncode=0,
                    stdout="",
                    stderr="",
                    import_result=make_import_result(decision="import"),
                )

            with patch.object(
                db,
                "capture_automation_import_completion",
                return_value=None,
            ), patch.object(
                dispatch_core_module,
                "_write_quality_evidence_action_file",
                return_value="/tmp/automation-conflict-action",
            ), patch.object(
                dispatch_core_module,
                "_remove_quality_evidence_action_file",
            ) as remove_action, patch.object(
                dispatch_core_module,
                "_refresh_current_evidence_after_import",
            ) as refresh, patch.object(
                dispatch_core_module,
                "_write_album_sidecar_after_import",
            ) as sidecar, patch_dispatch_externals():
                outcome = self._dispatch(
                    root=root,
                    db=db,
                    claimed=claimed,
                    candidate=candidate,
                    execution_lease=execution_lease,
                    token=token,
                    runner=runner,
                )

            self.assertEqual(runner_returned, [True])
            self.assertEqual(outcome.code, "beets_acknowledgement_ambiguous")
            remove_action.assert_not_called()
            refresh.assert_not_called()
            sidecar.assert_not_called()
            persisted = db.get_import_job(claimed.id)
            assert persisted is not None
            self.assertEqual(persisted.status, "running")


def _dispatch_valid_result_cmd(
    *,
    album_data=None,
    ctx=None,
    db_fields=None,
    ir=None,
):
    """Run the surviving auto-import seam and return the harness argv."""
    from lib.download_validation import _handle_valid_result
    from lib.staged_album import StagedAlbum

    album_data = album_data or _make_album_data()
    ctx = ctx or _make_ctx()
    if db_fields is not None:
        # Reseed request 42 with the test-supplied row shape. The default
        # _make_ctx() ships a downloading row keyed by id=42; tests that
        # need a different shape pass ``db_fields`` and we overwrite.
        # Force id=42 so ``_handle_valid_result`` finds the override when
        # looking up by ``album_data.db_request_id``.
        override = dict(db_fields)
        override["id"] = album_data.db_request_id
        override["status"] = "downloading"
        if override.get("active_download_state") is None:
            override["active_download_state"] = {
                "files": [],
                "filetype": "mp3",
            }
        fake_db = ctx.pipeline_db_source._get_db()
        fake_db.seed_request(override)
    bv_result = _make_bv_result()
    ir = ir or make_import_result(decision="import")

    with tempfile.TemporaryDirectory() as tmpdir:
        source_dir = os.path.join(tmpdir, "import")
        os.makedirs(source_dir)
        with open(os.path.join(source_dir, "01 - Track.mp3"), "w", encoding="utf-8") as fp:
            fp.write("fake audio")

        # Drive the real ``stage_to_ai_path`` by pointing the staging dir at
        # the tempdir. ``StagedAlbum.move_to`` creates the destination
        # directory itself, so we just need the staging root to exist.
        cfg = _ctx_cfg(ctx)
        cfg.beets_staging_dir = tmpdir
        # This argv seam deliberately has no installed album. Supply a
        # disposable complete authority pair anyway, so an accidental return
        # to the real current-evidence loader can never consult host Beets.
        cfg.beets_library_db = os.path.join(tmpdir, "beets-library.db")
        cfg.beets_directory = os.path.join(tmpdir, "beets-library")
        os.makedirs(cfg.beets_directory)

        def no_current_evidence(*_args: object, **_kwargs: object) -> None:
            """Typed current-evidence boundary for this subprocess argv seam."""
            return

        with patch("lib.download_validation.log_validation_result"), \
             patch_dispatch_externals() as ext, \
             patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
            from lib.dispatch import dispatch_import_core

            def dispatch_with_job(
                request: DispatchRequest,
                db: DispatchDB,
                **_kwargs: object,
            ) -> DispatchOutcome:
                # ``db`` is the handle ``_handle_valid_result`` resolved;
                # this seam re-reads the same fake through the ctx so it can
                # claim the job with the fake's own typed API.
                fake_db = ctx.pipeline_db_source._get_db()
                claimed, candidate, execution_lease = _claim_dispatch_job(
                    fake_db,
                    path=request.path,
                    release_id=request.mb_release_id,
                )
                cancellation_token = CancellationToken()
                with pinned_dispatch_authority(
                    fake_db,
                    execution_lease,
                    cancellation_token=cancellation_token,
                ) as (cancellation_token, owner_session_identity):
                    return dispatch_import_core(
                        replace(
                            request,
                            candidate_import_job_id=claimed.id,
                            prevalidated_candidate_result=candidate,
                            execution_lease=execution_lease,
                            owner_session_identity=owner_session_identity,
                        ),
                        fake_db,
                        cfg=ctx.cfg,
                        quality_gate_fn=noop_quality_gate,
                        current_evidence_loader=no_current_evidence,
                        run_import_fn=_owned_test_runner,
                        cancellation_token=cancellation_token,
                    )

            outcome = _handle_valid_result(
                album_data,
                bv_result,
                StagedAlbum(
                    current_path=source_dir,
                    request_id=album_data.db_request_id,
                ),
                ctx,
                quality_gate_fn=noop_quality_gate,
                dispatch_fn=dispatch_with_job,
            )
            assert ext.run.call_args is not None, outcome
            return ext.run.call_args[0][0]


class TestPopulateDlInfoFromImportResult(unittest.TestCase):

    def test_converted_flac_to_v0(self):
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        ir = make_import_result(was_converted=True, original_filetype="flac",
                                target_filetype="mp3", new_min_bitrate=245)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertTrue(dl.was_converted)
        self.assertEqual(dl.original_filetype, "flac")
        self.assertEqual(dl.slskd_filetype, "flac")
        self.assertEqual(dl.actual_filetype, "mp3")
        self.assertTrue(dl.is_vbr)
        self.assertEqual(dl.bitrate, 245000)
        assert dl.download_spectral is not None
        self.assertEqual(dl.download_spectral.grade, "genuine")

    def test_no_conversion(self):
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="mp3")
        ir = make_import_result(was_converted=False, new_min_bitrate=320)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertFalse(dl.was_converted)
        self.assertEqual(dl.slskd_filetype, "mp3")
        self.assertEqual(dl.actual_filetype, "mp3")

    def test_populates_actual_min_bitrate_from_new_measurement(self):
        """Point-in-time min bitrate must land in dl.actual_min_bitrate so the
        download_log column is non-NULL. Recents UI relies on this column to
        render per-row 'upgrade X to Y' verdicts — when NULL the UI silently
        falls through to album_requests.min_bitrate (current state), painting
        every historical row with the latest value.
        Live reproducer: request 1055, rows 3628/3631 both have NULL column
        despite JSONB carrying 119 and 162.
        """
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="mp3")
        ir = make_import_result(was_converted=False, new_min_bitrate=119)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertEqual(dl.actual_min_bitrate, 119)

    def test_populates_actual_min_bitrate_for_flac_conversion(self):
        """Same guarantee for the FLAC→V0 conversion path — the V0 min bitrate
        is the point-in-time value and must land on the column."""
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        ir = make_import_result(was_converted=True, original_filetype="flac",
                                target_filetype="mp3", new_min_bitrate=245)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertEqual(dl.actual_min_bitrate, 245)

    def test_materialized_output_owns_actual_bitrate_not_preview_v0_proxy(self):
        """Gas / November 89: the candidate measurement was the temporary
        MP3 V0 proof (191k min), while the stored Opus output measured 102k.
        ``actual_min_bitrate`` must describe the bytes that landed on disk.
        """
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        ir = ImportResult(
            decision="import",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=191,
                avg_bitrate_kbps=224,
                median_bitrate_kbps=237,
                format="Opus",
            ),
            materialized_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=102,
                avg_bitrate_kbps=132,
                median_bitrate_kbps=144,
                format="Opus",
            ),
            conversion=ConversionInfo(
                was_converted=True,
                original_filetype="flac",
                target_filetype="opus",
            ),
        )

        _populate_dl_info_from_import_result(dl, ir)

        self.assertEqual(dl.actual_filetype, "opus")
        self.assertEqual(dl.actual_min_bitrate, 102)
        self.assertEqual(dl.bitrate, 102000)

    def test_leaves_actual_min_bitrate_none_when_measurement_missing(self):
        """If there's no new_measurement in the ImportResult, we don't
        fabricate a value — NULL is the honest signal for consumers."""
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="mp3")
        ir = ImportResult(decision="import_failed", source_measurement=None)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertIsNone(dl.actual_min_bitrate)

    def test_populates_v0_probe_evidence(self):
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        probe = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=165,
            avg_bitrate_kbps=228,
            median_bitrate_kbps=225,
        )
        existing = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=128,
            avg_bitrate_kbps=171,
            median_bitrate_kbps=169,
        )
        ir = make_import_result(
            was_converted=True,
            original_filetype="flac",
            target_filetype="mp3",
            v0_probe=probe,
            existing_v0_probe=existing,
        )

        _populate_dl_info_from_import_result(dl, ir)

        self.assertEqual(dl.v0_probe, probe)
        self.assertEqual(dl.existing_v0_probe, existing)


class TestCleanupStagedDir(unittest.TestCase):

    def test_removes_dir_and_empty_parent(self):
        from lib.dispatch import _cleanup_staged_dir
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"fixture audio")
            parent = os.path.join(tmpdir, "Artist")
            staged = os.path.join(parent, "Album")
            os.makedirs(staged)
            open(os.path.join(staged, "track.mp3"), "w").close()
            _cleanup_staged_dir(staged)
            self.assertFalse(os.path.exists(staged))
            self.assertFalse(os.path.exists(parent))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_preserves_nonempty_parent(self):
        from lib.dispatch import _cleanup_staged_dir
        tmpdir = tempfile.mkdtemp()
        try:
            parent = os.path.join(tmpdir, "Artist")
            staged = os.path.join(parent, "Album1")
            other = os.path.join(parent, "Album2")
            os.makedirs(staged)
            os.makedirs(other)
            _cleanup_staged_dir(staged)
            self.assertFalse(os.path.exists(staged))
            self.assertTrue(os.path.exists(parent))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_realpath_protects_a_protected_parent_reached_via_symlink(self):
        """Issue #1077, R3-3 (round-3 review): the guard compared
        ``os.path.abspath`` on both sides, which does not resolve
        symlinks. A ``dest`` reached through a symlinked path component
        would compare unequal to the canonical ``protected_parent`` even
        though they name the SAME directory on disk — silently defeating
        the guard and letting it ``rmdir`` the shared root anyway.
        Switched to ``os.path.realpath`` on both sides so this world is
        correctly recognized as protected."""
        from lib.dispatch import _cleanup_staged_dir
        tmpdir = tempfile.mkdtemp()
        try:
            real_albums = os.path.join(tmpdir, "real_albums")
            os.makedirs(real_albums)
            staged = os.path.join(real_albums, "Album")
            os.makedirs(staged)
            symlinked_albums = os.path.join(tmpdir, "albums_link")
            os.symlink(real_albums, symlinked_albums)
            dest_via_symlink = os.path.join(symlinked_albums, "Album")

            _cleanup_staged_dir(
                dest_via_symlink, protected_parents=frozenset({real_albums}),
            )

            self.assertFalse(os.path.exists(staged))
            self.assertTrue(
                os.path.isdir(real_albums),
                "the protected root must survive even when dest was "
                "reached through a symlinked path component",
            )
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestRunPostCommitCleanupProtectedParent(unittest.TestCase):
    """Issue #1077, R3-3 (round-3 review; docstring corrected R4-3): the
    importer's deferred post-commit cleanup
    (``scripts.importer._run_post_commit_cleanup``) is the THIRD real
    caller of ``_cleanup_staged_dir`` and the furthest from ``cfg`` —
    reached only through the ``PostCommitCleanup`` plan a caller built
    earlier and handed across the queue-owner boundary. Every
    ``is_automation`` branch in ``process_claimed_job`` returns before
    reaching this call site (the journaled ``_complete_automation_
    processing_cleanup`` lane owns automation cleanup instead) — the real
    motivating scenario is a FORCE job's success plan, whose
    ``staged_path`` (``R4-1``) is
    ``<processing_dir>/albums/force-action-<id>``, a direct child of the
    same shared root. Before this fix it called
    ``_cleanup_staged_dir(plan.staged_path)`` with no guard at all, so a
    successful force import whose ``staged_path`` was the last remaining
    canonical album under that root could ``rmdir`` the shared,
    Nix-provisioned ``<processing_dir>/albums/`` root right out from under
    every other request. This proves the guard now travels end to end: the
    plan carries ``staged_path_protected_parents``, and the real
    (unpatched) ``_cleanup_staged_dir`` honours it."""

    def test_post_commit_cleanup_never_removes_the_processing_albums_root(self):
        from lib.dispatch.types import DispatchOutcome, PostCommitCleanup
        from lib.processing_paths import processing_albums_dir
        from scripts.importer import _run_post_commit_cleanup

        tmpdir = tempfile.mkdtemp()
        try:
            processing_dir = os.path.join(tmpdir, "processing")
            albums_root = processing_albums_dir(processing_dir)
            staged_path = os.path.join(albums_root, "Artist - Album")
            os.makedirs(staged_path)
            with open(os.path.join(staged_path, "01.flac"), "wb") as handle:
                handle.write(b"imported audio")

            outcome = DispatchOutcome(
                success=True,
                message="imported",
                post_commit_cleanup=PostCommitCleanup(
                    staged_path=staged_path,
                    staged_path_protected_parents=frozenset({albums_root}),
                ),
            )

            details = _run_post_commit_cleanup(outcome)

            assert details is not None
            staged_path_detail = details["staged_path"]
            assert isinstance(staged_path_detail, dict)
            self.assertTrue(staged_path_detail["success"])
            self.assertFalse(os.path.exists(staged_path))
            self.assertTrue(
                os.path.isdir(albums_root),
                "the shared processing albums root must survive even "
                "though it is now empty — it is a Nix-provisioned root, "
                "not a disposable per-artist directory",
            )
            self.assertEqual(os.listdir(albums_root), [])
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestAudioCorruptBanAndDelete(unittest.TestCase):
    """Bad rips are ban + delete, never quarantined (issue #1077, D3).

    Equivalence note: this class replaces the retired
    ``TestAudioCorruptPostCommitQuarantine``. Its three lowest-level tests
    (atomic cross-device move failure, missing quarantine root) covered
    ``lib.dispatch.quarantine.quarantine_corrupt_audio_source`` and
    ``PostCommitCleanup.audio_quarantine_source_path`` — both deleted with
    this PR, since ``_run_post_commit_cleanup`` no longer has any
    audio-specific branch (audio_corrupt now reuses the plain
    ``staged_path`` cleanup exercised end to end by
    ``tests.test_dispatch_core.TestDispatchCoreOrchestration``'s
    ``test_audio_corrupt_*`` tests, real journaled REMOVE_SOURCE included).
    The atomic-rename-never-falls-back-to-copy safety property those tests
    also covered lives on in ``move_failed_import_whole`` itself (now Lane
    B's whole-folder mover, D4) — see ``tests/test_import_manifest.py``.
    The evidence-link-failure-survives-cleanly property is now covered
    parametrically (including ``audio_corrupt``) by
    ``TestGeneratedDeleteIneligibleScenarioIsolation`` in
    ``tests/test_dispatch_outcomes_generated.py``.
    """

    def test_force_corrupt_source_is_banned_and_deleted(self):
        """D3 + D8: a force-import audio_corrupt reject bans the peer and
        deletes the ORIGINAL Wrong Matches source — not the disposable
        force action copy dispatch itself touches."""
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.dispatch.types import ImportAttemptResult
        from lib.import_queue import IMPORT_JOB_FORCE
        from lib.quality import AudioQualityMeasurement
        from lib.quality_evidence import snapshot_audio_files
        from scripts.importer import process_claimed_job

        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "wrong_matches", "Artist - Album")
            action_copy = os.path.join(parent, "action-copy")
            os.makedirs(source)
            os.makedirs(action_copy)
            with open(os.path.join(source, "01.flac"), "wb") as handle:
                handle.write(b"corrupt audio")
            with open(os.path.join(action_copy, "01.flac"), "wb") as handle:
                handle.write(b"corrupt audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=835,
                status="unsearchable",
                mb_release_id="test-mbid",
            ))
            original_log_id = db.log_download(
                request_id=835,
                outcome="rejected",
                validation_result=json.dumps({
                    "scenario": "strong_mismatch",
                    "failed_path": source,
                }),
                staged_path=source,
            )
            queued = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=835,
                payload={
                    "download_log_id": original_log_id,
                    "failed_path": source,
                },
            )
            candidate = make_album_quality_evidence(
                mb_release_id="test-mbid",
                source_path=action_copy,
                files=snapshot_audio_files(action_copy),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=900,
                    avg_bitrate_kbps=900,
                    median_bitrate_kbps=900,
                    format="FLAC",
                ),
                codec="flac",
                container="flac",
                storage_format="FLAC",
                audio_corrupt=True,
                audio_error="decoder rejected source",
            )
            db.upsert_album_quality_evidence(candidate)
            persisted_candidate = db.find_album_quality_evidence(
                mb_release_id=candidate.mb_release_id,
                snapshot_fingerprint=candidate.snapshot_fingerprint,
            )
            assert (
                persisted_candidate is not None
                and persisted_candidate.id is not None
            )
            db.set_import_job_candidate_evidence(
                queued.id,
                persisted_candidate.id,
            )
            db.mark_import_job_preview_importable(
                queued.id,
                preview_result={"ready": True, "action_path": action_copy},
            )
            claimed = claim_next_import_job(db, worker_id="force-corrupt")
            assert claimed is not None

            import_result = make_import_result(decision="audio_corrupt")
            import_result.source_measurement = AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=900,
                format="FLAC",
            )
            attempt = ImportAttemptResult(None)
            attempt.merge(import_result)
            outcome = _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=835,
                    dl_info=DownloadInfo(filetype='flac', username='bad-peer'),
                    distance=0.0,
                    requeue_on_failure=False,
                    path=action_copy,
                    scenario='force_import',
                    files=[],
                    cooled_down_users=None,
                    candidate_import_job_id=claimed.id,
                    candidate_download_log_id=original_log_id,
                ),
                db,
                attempt_result=attempt,
                decision='audio_corrupt',
                detail='decoder rejected source',
            )
            self.assertEqual(
                outcome.post_commit_wrong_match_scenario,
                "audio_corrupt",
            )

            # No mocking needed for the force action-copy reclaim:
            # `read_runtime_config()` soft-fails to a default config with no
            # env var set, and `_cleanup_terminal_force_action` catches
            # `FilesystemAuthorityError`/any exception internally — this
            # test asserts the Wrong Matches source deletion, not that leaf.
            completed = process_claimed_job(
                db,  # pyright: ignore[reportArgumentType]
                claimed,
                execute_fn=lambda *_args, **_kwargs: outcome,
            )

            assert completed is not None and completed.result is not None
            cleanup = completed.result["cleanup"]
            assert isinstance(cleanup, dict)
            self.assertTrue(cleanup["success"])
            self.assertEqual(cleanup["deleted_path"], os.path.abspath(source))
            # The ORIGINAL Wrong Matches source is gone; the request is
            # denylisted (`action.denylist` for the ``audio_corrupt``
            # decision), and no post-commit archival concept survives.
            self.assertFalse(os.path.exists(source))
            self.assertEqual(
                [entry.username for entry in db.denylist],
                ["bad-peer"],
            )
            terminal_log = db.download_logs[-1]
            terminal_audit = msgspec.json.decode(
                terminal_log.validation_result,
            )
            self.assertNotIn("post_commit_quarantine", terminal_audit)
            self.assertEqual(db.request(835)["status"], "unsearchable")


class TestAutomationWrongMatchPostCommitTriage(unittest.TestCase):
    def test_high_distance_rejection_triages_after_owner_terminal_commit(
        self,
    ) -> None:
        from lib.dispatch import (
            DispatchOutcome,
            _record_rejection_and_maybe_requeue,
        )
        from lib.terminal_outcomes import PendingImportTerminalOutcome

        with tempfile.TemporaryDirectory() as root:
            processing_albums = os.path.join(root, "processing", "albums")
            canonical_path = os.path.join(
                processing_albums,
                "Stone Sour - Come What(ever) May",
            )
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                mb_release_id="stone-sour-release",
            ))
            claimed, _candidate, _execution_lease = _claim_dispatch_job(
                db,
                path=canonical_path,
                release_id="stone-sour-release",
            )

            wrong_matches = os.path.join(processing_albums, "wrong_matches")
            os.makedirs(wrong_matches)
            failed_path = os.path.join(
                wrong_matches,
                "Stone Sour - Come What(ever) May",
            )
            os.rename(canonical_path, failed_path)

            validation = ValidationResult(
                valid=False,
                distance=0.1697,
                scenario="high_distance",
                detail="distance=0.1697",
                failed_path=failed_path,
                soulseek_username="Strudel",
            )
            pending = _record_rejection_and_maybe_requeue(
                db,
                42,
                DownloadInfo(filetype="mp3", username="Strudel"),
                detail=validation.detail,
                error=None,
                validation_result=validation.to_json(),
                requeue=True,
                import_job_id=claimed.id,
            )
            self.assertIsInstance(pending, PendingImportTerminalOutcome)
            assert isinstance(pending, PendingImportTerminalOutcome)
            outcome = DispatchOutcome(
                success=False,
                message="Rejected: high_distance - distance=0.1697",
                terminal_outcome=pending,
                post_commit_wrong_match_scenario="high_distance",
            )
            observed: list[tuple[int, int | None, str, str]] = []

            def cleanup_after_commit(
                db_arg,
                download_log_id: int,
                *,
                ignore_import_job_id: int | None,
            ) -> None:
                request = db_arg.request(42)
                terminal_job = db_arg.get_import_job(claimed.id)
                assert terminal_job is not None
                observed.append((
                    download_log_id,
                    ignore_import_job_id,
                    str(request["status"]),
                    terminal_job.status,
                ))

            with patch(
                "lib.wrong_match_cleanup_service.cleanup_wrong_match",
                side_effect=cleanup_after_commit,
            ):
                terminal_job = finalize_claimed_dispatch(db, claimed, outcome)

            assert terminal_job is not None
            terminal_log = db.download_logs[-1]
            self.assertEqual(
                observed,
                [(terminal_log.id, claimed.id, "wanted", "failed")],
            )
            self.assertEqual(
                terminal_log.candidate_evidence_id,
                claimed.candidate_evidence_id,
            )
            self.assertTrue(os.path.isdir(failed_path))


class TestRecordRejectionAndRequeueSeam(unittest.TestCase):
    """Seam tests for the shared rejection finalizer."""

    def test_requeue_defers_from_status_lookup_to_finalize_request(
        self,
    ) -> None:
        """The shared job-less rejection bundle (issue #1355 item 3) never
        hardcodes ``from_status`` — the transition command it builds carries
        none, so the real transition engine derives it from the live row.
        Proven two ways: the committed command's own ``from_status`` field,
        and the actually-applied effect (a live row starting in
        ``unsearchable`` really lands in ``wanted`` with its attempt bumped
        — hardcoding a stale ``from_status`` here would have refused it)."""
        from lib.dispatch import _record_rejection_and_maybe_requeue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="unsearchable"))

        _record_rejection_and_maybe_requeue(
            db,
            42,
            DownloadInfo(username="user1"),
            detail="too low",
            error=None,
            validation_result=ValidationResult(
                distance=0.5,
                scenario="quality_downgrade",
                detail="too low",
            ).to_json(),
            requeue=True,
        )

        self.assertEqual(len(db.persist_request_rejection_outcome_calls), 1)
        command = db.persist_request_rejection_outcome_calls[0]
        self.assertEqual(command.request_id, 42)
        assert command.transition is not None
        self.assertIsNone(command.transition.from_status)
        self.assertEqual(command.transition.attempt_type, "validation")
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["validation_attempts"], 1)

    def test_requeue_only_forwards_fields_persisted_by_wanted_transition(self) -> None:
        from lib.dispatch import _record_rejection_and_maybe_requeue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        _record_rejection_and_maybe_requeue(
            db,
            42,
            DownloadInfo(username="user1"),
            detail="too low",
            error=None,
            validation_result=ValidationResult(
                distance=0.5,
                scenario="quality_downgrade",
                detail="too low",
            ).to_json(),
            requeue=True,
            search_filetype_override="flac,mp3 v0",
        )

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "flac,mp3 v0")
        self.assertIsNone(row["beets_distance"])
        self.assertIsNone(row["beets_scenario"])

    def test_job_less_audit_carries_every_download_info_field(self) -> None:
        """Every ``DownloadInfo`` field the audit is supposed to carry
        actually lands on the committed ``download_log`` row — not just
        the handful the other seam tests happen to assert. Mutation
        testing (issue #1355 item 3) found this gap: nulling any single
        field in ``_record_rejection_and_maybe_requeue``'s
        ``TerminalDownloadAudit`` construction survived every existing
        test."""
        from lib.dispatch import _record_rejection_and_maybe_requeue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        dl_info = DownloadInfo(
            username="user1",
            contributor_usernames=("user1", "user2"),
            filetype="flac",
            bitrate=1000,
            sample_rate=44100,
            bit_depth=16,
            is_vbr=True,
            was_converted=True,
            original_filetype="mp3 320",
            slskd_filetype="flac",
            actual_filetype="mp3",
            actual_min_bitrate=192,
            download_spectral=SpectralMeasurement(grade="genuine", bitrate_kbps=320),
            current_spectral=SpectralMeasurement(grade="likely_transcode", bitrate_kbps=192),
            existing_min_bitrate=256,
            import_result='{"decision":"downgrade"}',
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=200,
                avg_bitrate_kbps=210,
                median_bitrate_kbps=205,
            ),
            existing_v0_probe=V0ProbeEvidence(
                kind="native_lossy",
                min_bitrate_kbps=190,
                avg_bitrate_kbps=195,
                median_bitrate_kbps=193,
            ),
        )

        _record_rejection_and_maybe_requeue(
            db,
            42,
            dl_info,
            detail="too low",
            error="rejected error",
            validation_result=ValidationResult(
                distance=0.5, scenario="quality_downgrade", detail="too low",
            ).to_json(),
            requeue=True,
            outcome_label="rejected",
            staged_path="/tmp/staged-full-audit",
            source_download_log_id=99,
        )

        db.assert_log(
            self, 0,
            outcome="rejected",
            beets_detail="too low",
            soulseek_username="user1",
            candidate_contributor_usernames=["user1", "user2"],
            filetype="flac",
            staged_path="/tmp/staged-full-audit",
            error_message="rejected error",
            bitrate=1000,
            sample_rate=44100,
            bit_depth=16,
            is_vbr=True,
            was_converted=True,
            original_filetype="mp3 320",
            slskd_filetype="flac",
            actual_filetype="mp3",
            actual_min_bitrate=192,
            spectral_grade="genuine",
            spectral_bitrate=320,
            existing_min_bitrate=256,
            existing_spectral_bitrate=192,
            import_result='{"decision":"downgrade"}',
            source_download_log_id=99,
            v0_probe_kind="lossless_source_v0",
            v0_probe_min_bitrate=200,
            v0_probe_avg_bitrate=210,
            v0_probe_median_bitrate=205,
            existing_v0_probe_kind="native_lossy",
            existing_v0_probe_min_bitrate=190,
            existing_v0_probe_avg_bitrate=195,
            existing_v0_probe_median_bitrate=193,
        )


class TestRejectImportFromEvidenceDecision(unittest.TestCase):
    """Evidence-decision rejections must populate download_log columns.

    Bug: ``_reject_import_from_evidence_decision`` built ``ImportResult``
    JSON for the JSONB column but skipped
    ``_populate_dl_info_from_import_result``, so every top-level
    quality column landed NULL. The Recents UI rendered just
    ``"downgrade · username"`` instead of the full quality verdict.

    Live reproducer: download_log id 14570 — Faux Pas - Entropy Begins
    at Home, decision=downgrade, new=127kbps mp3 likely_transcode,
    existing=192kbps mp3 cbr. JSONB had everything; columns were all
    NULL.
    """

    def test_evidence_rejection_populates_download_log_columns(self) -> None:
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.dispatch.types import ImportAttemptResult
        from lib.quality import (
            AudioQualityMeasurement,
            ImportResult,
            SpectralAnalysisDetail,
            SpectralDetail,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        dl_info = DownloadInfo(filetype="mp3", username="user1")
        ir = ImportResult(
            decision="downgrade",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=127,
                avg_bitrate_kbps=127,
                median_bitrate_kbps=128,
                format="MP3",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            current_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=192,
                avg_bitrate_kbps=192,
                median_bitrate_kbps=192,
                format="MP3",
                is_cbr=True,
            ),
        )
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="suspect", bitrate_kbps=96),
            existing=SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=192),
        )
        attempt_result = ImportAttemptResult(audit)
        attempt_result.merge(ir)

        with patch_dispatch_externals():
            _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=42,
                    dl_info=dl_info,
                    distance=0.1279,
                    requeue_on_failure=True,
                    path='/tmp/cratedigger-evidence-reject-test',
                    scenario='downgrade',
                    files=None,
                    cooled_down_users=None,
                ),
                db,
                attempt_result=attempt_result,
                decision='downgrade',
                detail='import-time persisted evidence rejected candidate',
            )

        self.assertEqual(len(db.download_logs), 1)
        log = db.download_logs[0]
        self.assertEqual(log.outcome, "rejected")
        self.assertEqual(log.beets_scenario, "downgrade")
        self.assertEqual(log.beets_distance, 0.1279)
        # Top-level quality columns the UI reads.
        self.assertEqual(log.extra["actual_filetype"], "mp3")
        self.assertEqual(log.extra["slskd_filetype"], "mp3")
        self.assertEqual(log.extra["bitrate"], 127_000)
        self.assertEqual(log.extra["actual_min_bitrate"], 127)
        self.assertEqual(log.extra["spectral_grade"], "likely_transcode")
        self.assertEqual(log.extra["spectral_bitrate"], 128)
        self.assertEqual(log.extra["existing_min_bitrate"], 192)
        self.assertEqual(log.extra["existing_spectral_bitrate"], None)
        # The full ImportResult is still serialized into the JSONB.
        self.assertIsNotNone(log.import_result)
        assert log.import_result is not None
        self.assertEqual(ImportResult.from_json(log.import_result).spectral, audit)

    def test_lemonade_downgrade_persists_lossless_only_from_have_audit(self) -> None:
        """Request 5524: linked evidence is spectrally empty; attempt HAVE wins."""
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.dispatch.types import ImportAttemptResult
        from lib.quality import (
            AudioQualityMeasurement,
            ImportResult,
            QualityRankConfig,
            SpectralAnalysisDetail,
            SpectralDetail,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            current_spectral_grade=None,
            search_filetype_override=QUALITY_UPGRADE_TIERS,
            target_format=None,
        ))
        current = AudioQualityMeasurement(
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            spectral_grade=None,
        )
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True,
                grade="genuine",
            ),
            existing=SpectralAnalysisDetail(
                attempted=True,
                grade="genuine",
            ),
        )
        attempt_result = ImportAttemptResult(audit)
        attempt_result.merge(ImportResult(
            decision="downgrade",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=226,
                avg_bitrate_kbps=226,
                median_bitrate_kbps=226,
                format="MP3",
            ),
            current_measurement=current,
        ))

        with patch_dispatch_externals():
            _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=42,
                    dl_info=DownloadInfo(filetype='mp3', username='qreature'),
                    distance=0.0,
                    requeue_on_failure=True,
                    path='/tmp/lemonade',
                    scenario='quality_downgrade',
                    files=None,
                    cooled_down_users=None,
                ),
                db,
                attempt_result=attempt_result,
                decision='downgrade',
                detail='import-time persisted evidence rejected candidate',
                quality_ranks=QualityRankConfig.defaults(),
            )

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "lossless")
        self.assertIsNone(row["target_format"])
        self.assertIsNone(row["current_spectral_grade"])

    def test_downgrade_missing_have_audit_does_not_fallback_to_measurement(self) -> None:
        """A failed preview-audit decode must fail open to all search tiers."""
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.dispatch.types import ImportAttemptResult
        from lib.quality import (
            AudioQualityMeasurement,
            ImportResult,
            QualityRankConfig,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=43,
            status="downloading",
            search_filetype_override=QUALITY_UPGRADE_TIERS,
            target_format=None,
        ))
        malformed_job = MagicMock(
            preview_result={
                "import_result": {
                    "version": 4,
                    "spectral": "malformed-preview-audit",
                },
            },
        )
        with patch.object(db, "get_import_job", return_value=malformed_job):
            attempt_result = ImportAttemptResult.from_import_job(
                db,
                9001,
            )
        self.assertIsNone(attempt_result.audit)
        attempt_result.merge(ImportResult(
            decision="downgrade",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=226,
                avg_bitrate_kbps=226,
                format="MP3",
            ),
            current_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                # Persisted measurement state must not impersonate the
                # missing attempt-local HAVE audit.
                spectral_grade="genuine",
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
        ))

        with patch_dispatch_externals():
            _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=43,
                    dl_info=DownloadInfo(filetype='mp3', username='qreature', is_vbr=True),
                    distance=0.0,
                    requeue_on_failure=True,
                    path='/tmp/missing-have-audit',
                    scenario='quality_downgrade',
                    files=None,
                    cooled_down_users=None,
                ),
                db,
                attempt_result=attempt_result,
                decision='downgrade',
                detail='preview audit decode failed before rejection',
                quality_ranks=QualityRankConfig.defaults(),
            )

        row = db.request(43)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(
            row["search_filetype_override"],
            "lossless,mp3 320,aac,opus,ogg",
        )
        self.assertIsNone(row["target_format"])

    def test_every_contributing_peer_is_denylisted(self) -> None:
        """A rejected album denylists EVERY peer that contributed to it.

        ``request.files`` is the only peer attribution dispatch has, and
        ``extract_usernames`` is the only thing it reads off them. Every
        pre-existing fixture passed one file or none, so the collection was
        interchangeable with an empty list — a review mutant replacing it
        with ``[]`` survived, and a multi-peer album would have banned only
        the ``dl_info`` username while the other source kept serving the
        same bad rip.
        """
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.dispatch.types import ImportAttemptResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        attempt_result = ImportAttemptResult(None)
        attempt_result.merge(make_import_result(
            decision="downgrade", new_min_bitrate=128, prev_min_bitrate=320))

        with patch_dispatch_externals():
            _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=42,
                    dl_info=DownloadInfo(filetype="mp3", username="seeder-a"),
                    path="/tmp/cratedigger-multi-peer-reject",
                    scenario="strong_match",
                    files=[
                        make_download_file(
                            username="seeder-b", filename="01.mp3"),
                        make_download_file(
                            username="seeder-c", filename="02.mp3"),
                    ],
                ),
                db,
                attempt_result=attempt_result,
                decision="downgrade",
                detail="import-time persisted evidence rejected candidate",
            )

        self.assertEqual(
            sorted(entry.username for entry in db.denylist),
            ["seeder-a", "seeder-b", "seeder-c"],
        )

    def _reject_and_report_cleanup(self, *, scenario: str):
        """Drive one reject through the helper and report its cleanup plan."""
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.dispatch.types import ImportAttemptResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        attempt_result = ImportAttemptResult(None)
        attempt_result.merge(make_import_result(
            decision="downgrade", new_min_bitrate=128, prev_min_bitrate=320))

        with patch_dispatch_externals() as ext:
            outcome = _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=42,
                    dl_info=DownloadInfo(filetype="mp3", username="user1"),
                    path="/tmp/cratedigger-cleanup-gate-source",
                    scenario=scenario,
                    requeue_on_failure=scenario != "force_import",
                ),
                db,
                attempt_result=attempt_result,
                decision="downgrade",
                detail="import-time persisted evidence rejected candidate",
            )
        return outcome, ext.cleanup

    def test_force_reject_never_deletes_the_operators_only_copy(self) -> None:
        """Issue #89's guard, keyed on the SCENARIO, not the decision.

        A force import's ``path`` is the operator's own folder. On a
        ``downgrade`` verdict beets moved nothing, so deleting it is data
        loss — ``_should_cleanup_path`` refuses cleanup for a force scenario
        unless the decision actually imported.

        The gate reads ``request.scenario``. A review mutant that passed the
        DECISION instead survived 413 tests: no decision name is ever in
        ``FORCE_IMPORT_SCENARIOS``, so the guard degraded to "always clean"
        and this exact data loss became unguarded.
        """
        outcome, cleanup = self._reject_and_report_cleanup(
            scenario="force_import")

        self.assertIsNone(outcome.post_commit_cleanup)
        cleanup.assert_not_called()

    def test_auto_reject_still_disposes_of_its_processing_source(self) -> None:
        """Must-still-work twin: the auto lane's source IS disposable, so
        the same decision on a non-force scenario must still clean up."""
        _outcome, cleanup = self._reject_and_report_cleanup(
            scenario="strong_match")

        cleanup.assert_called_once()
        self.assertEqual(
            cleanup.call_args.args[0],
            "/tmp/cratedigger-cleanup-gate-source",
        )


class TestRejectImportFromEvidenceDecisionCallerLifecycle(unittest.TestCase):
    """Every rejection honors the lifecycle authority chosen by its caller.

    Automatic imports pass ``requeue_on_failure=True`` so a bad candidate
    self-heals to ``wanted``. Force imports pass False because the operator's
    ``unsearchable`` status must not be cleared by a
    candidate-integrity fact.
    """

    FOUR_FACT_DECISIONS: ClassVar = ["audio_corrupt", "bad_audio_hash", "nested_layout", "empty_fileset"]

    TEST_BEETS_STAGING_DIR: ClassVar = (
        "/tmp/cratedigger-caller-lifecycle-beets-staging"
    )

    def _reject(
        self,
        *,
        decision: str,
        requeue_on_failure: bool,
        pending: bool = False,
        search_filetype_override: str | None = None,
        initial_status: str = "downloading",
        processing_dir: str | None = None,
        capture_cleanup_call: dict[str, object] | None = None,
        capture_post_commit_cleanup: dict[str, object] | None = None,
    ):
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.dispatch.types import ImportAttemptResult
        from lib.import_queue import IMPORT_JOB_FORCE
        from lib.processing_paths import protected_staging_roots
        from lib.quality import AudioQualityMeasurement, ImportResult
        from lib.terminal_outcomes import ImportJobTerminal

        protected_roots = (
            protected_staging_roots(
                processing_dir=processing_dir,
                beets_staging_dir=self.TEST_BEETS_STAGING_DIR,
            )
            if processing_dir is not None else None
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status=initial_status,
            mb_release_id="test-mbid",
            search_filetype_override=search_filetype_override,
        ))
        dl_info = DownloadInfo(filetype="mp3", username="user1")
        ir = ImportResult(
            decision=decision,
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
            ),
        )
        attempt_result = ImportAttemptResult(None)
        attempt_result.merge(ir)
        import_job_id = None
        if pending:
            import_job_id = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={
                    "download_log_id": 1,
                    "failed_path": "/tmp/cratedigger-caller-lifecycle-test",
                },
            ).id
        with patch_dispatch_externals() as ext:
            outcome = _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=42,
                    dl_info=dl_info,
                    distance=0.0,
                    requeue_on_failure=requeue_on_failure,
                    path='/tmp/cratedigger-caller-lifecycle-test',
                    scenario=decision,
                    files=None,
                    cooled_down_users=None,
                    candidate_import_job_id=import_job_id,
                ),
                db,
                attempt_result=attempt_result,
                decision=decision,
                detail=f'test {decision}',
                protected_roots=protected_roots,
            )
            if capture_cleanup_call is not None and ext.cleanup.call_args is not None:
                capture_cleanup_call["args"] = ext.cleanup.call_args.args
                capture_cleanup_call["kwargs"] = ext.cleanup.call_args.kwargs
            if capture_post_commit_cleanup is not None:
                capture_post_commit_cleanup["value"] = outcome.post_commit_cleanup
        if pending:
            self.assertIsNotNone(outcome.terminal_outcome)
            assert outcome.terminal_outcome is not None
            db.persist_import_terminal_outcome(
                outcome.terminal_outcome.with_job(ImportJobTerminal(
                    status="failed",
                    error=outcome.message,
                    result={"success": False},
                    message=outcome.message,
                ))
            )
        return db

    def test_four_fact_rejects_preserve_status_when_caller_says_no(self) -> None:
        for decision in self.FOUR_FACT_DECISIONS:
            with self.subTest(decision=decision):
                db = self._reject(decision=decision, requeue_on_failure=False)
                self.assertEqual(
                    db.request(42)["status"],
                    "downloading",
                    f"{decision} reject with requeue_on_failure=False must "
                    "preserve the caller-owned request status",
                )

    def test_four_fact_rejects_also_requeue_when_caller_says_yes(self) -> None:
        # Baseline: requeue_on_failure=True keeps the same self-heal behavior.
        for decision in self.FOUR_FACT_DECISIONS:
            with self.subTest(decision=decision):
                db = self._reject(decision=decision, requeue_on_failure=True)
                self.assertEqual(db.request(42)["status"], "wanted")

    def test_quality_reject_honors_requeue_flag(self) -> None:
        # Non-four-fact reject (downgrade) must NOT be force-requeued.
        # When the caller passes requeue_on_failure=False the request stays
        # in its current status — the operator chose to act on this source.
        db = self._reject(decision="downgrade", requeue_on_failure=False)
        self.assertEqual(db.request(42)["status"], "downloading")

    def test_verified_lossless_lock_preserves_terminal_imported_state(self) -> None:
        """The proof lock audits and cleans without reopening acquisition."""
        db = self._reject(
            decision="verified_lossless_locked",
            requeue_on_failure=True,
            search_filetype_override=QUALITY_UPGRADE_TIERS,
        )
        self.assertEqual(db.request(42)["status"], "imported")
        self.assertEqual(
            db.request(42)["search_filetype_override"],
            QUALITY_UPGRADE_TIERS,
        )
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.download_logs[-1].outcome, "rejected")

    def test_verified_lossless_lock_holds_for_force_imports(self) -> None:
        """Decision 21: a force import against a proof-bearing
        request is declined by the same lock (requeue_on_failure=False is
        the operator paths' setting) — force bypasses only the beets
        distance; Replace/re-request is the way back in.
        """
        db = self._reject(
            decision="verified_lossless_locked",
            requeue_on_failure=False,
            search_filetype_override=QUALITY_UPGRADE_TIERS,
        )
        self.assertEqual(db.request(42)["status"], "imported")
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.download_logs[-1].outcome, "rejected")

    def test_verified_lossless_lock_pending_outcome_is_atomic(self) -> None:
        """The import-job owner commits the proof lock and audit together."""
        db = self._reject(
            decision="verified_lossless_locked",
            requeue_on_failure=True,
            pending=True,
            search_filetype_override=QUALITY_UPGRADE_TIERS,
        )
        self.assertEqual(db.request(42)["status"], "imported")
        self.assertEqual(
            db.request(42)["search_filetype_override"],
            QUALITY_UPGRADE_TIERS,
        )

    def test_verified_lossless_lock_pending_preserves_operator_stop(self) -> None:
        """A proof-lock rejection is not successful terminal acceptance."""
        db = self._reject(
            decision="verified_lossless_locked",
            requeue_on_failure=False,
            pending=True,
            search_filetype_override=QUALITY_UPGRADE_TIERS,
            initial_status="unsearchable",
        )

        self.assertEqual(db.request(42)["status"], "unsearchable")
        self.assertEqual(db.download_logs[-1].outcome, "rejected")
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.download_logs[-1].outcome, "rejected")

    def test_processing_dir_threads_the_protected_parents_guard(self) -> None:
        """Issue #1077, R3-3 (round-3 review; widened issue #1122, review
        round 2): the synchronous cleanup branch inside
        ``_reject_import_from_evidence_decision`` is one of the two
        unguarded ``_cleanup_staged_dir`` call sites the reviewer found —
        this one fires on every quality reject whose ``staged_path`` is a
        canonical processing album (or, since a YouTube rescue imports in
        place, the auto-import staging root). Proves the function threads
        its caller-supplied ``protected_roots`` (issue #1122: the
        derivation moved to the caller — ``lib.dispatch.core`` computes
        ``protected_staging_roots(cfg.processing_dir,
        cfg.beets_staging_dir)`` — so this function stays decoupled from
        that naming and just passes the set through) all the way to
        ``_cleanup_staged_dir`` — a seam assertion on the SAME mocked
        cleanup call the shared ``_reject`` helper (and every other test
        in this class) already exercises, so this reuses that call site's
        existing ``# type: ignore[arg-type]`` rather than adding a new one
        (tests/test_typing_ratchet.py's escape-hatch baseline must match
        the live scan exactly; adding one here would grow it). The real
        (unpatched) ``_cleanup_staged_dir``
        honouring this guard is proven separately by
        ``TestCleanupStagedDir.
        test_realpath_protects_a_protected_parent_reached_via_symlink``."""
        from lib.processing_paths import processing_albums_dir

        capture: dict[str, object] = {}
        processing_dir = "/tmp/cratedigger-r3-3-processing-dir"
        self._reject(
            decision="bad_audio_hash",
            requeue_on_failure=True,
            processing_dir=processing_dir,
            capture_cleanup_call=capture,
        )

        kwargs = capture.get("kwargs")
        assert isinstance(kwargs, dict)
        protected_parents = kwargs.get("protected_parents")
        assert protected_parents is not None
        self.assertIn(
            processing_albums_dir(processing_dir),
            protected_parents,
        )

    def test_no_processing_dir_passes_no_protected_parents(self) -> None:
        """Must-still-work control: without a ``processing_dir`` (the
        non-canonical staged lane), the guard stays ``None`` rather than
        inventing a spurious protected root."""
        capture: dict[str, object] = {}
        self._reject(
            decision="bad_audio_hash",
            requeue_on_failure=True,
            capture_cleanup_call=capture,
        )

        kwargs = capture.get("kwargs")
        assert isinstance(kwargs, dict)
        self.assertIsNone(kwargs.get("protected_parents"))

    def test_deferred_plan_also_carries_the_protected_parents_guard(self) -> None:
        """Issue #1077, R4-3 (round-4 review; widened issue #1122, review
        round 2): the SYNC branch's guard
        (``test_processing_dir_threads_the_protected_parents_guard`` above)
        does not prove the DEFERRED branch (``import_job_id is not None``)
        also carries it — ``PostCommitCleanup(staged_path=staged_path,
        staged_path_protected_parents=protected_roots)`` is a second,
        independent assignment (``lib/dispatch/outcome_actions.py``) that
        could silently regress to dropping the field without either sync
        test noticing. Drives the ``pending=True`` (force job) path and
        asserts directly on the returned ``PostCommitCleanup``."""
        from lib.dispatch.types import PostCommitCleanup
        from lib.processing_paths import processing_albums_dir

        processing_dir = "/tmp/cratedigger-r4-3-processing-dir"
        capture: dict[str, object] = {}
        self._reject(
            decision="bad_audio_hash",
            requeue_on_failure=False,
            pending=True,
            processing_dir=processing_dir,
            capture_post_commit_cleanup=capture,
        )

        plan = capture.get("value")
        assert isinstance(plan, PostCommitCleanup)
        assert plan.staged_path_protected_parents is not None
        self.assertIn(
            processing_albums_dir(processing_dir),
            plan.staged_path_protected_parents,
        )


class TestHaveAnalysisErrorAbort(unittest.TestCase):
    """A failed installed-HAVE analysis is an attempt-local abort."""

    def _dispatch_with_current_result(
        self,
        current_result,
        *,
        force: bool,
        db: FakePipelineDB | None = None,
        candidate=None,
    ):
        from lib.dispatch import dispatch_import_core
        from lib.import_evidence import (
            ActionEvidenceProvenance,
            CandidateEvidenceActionResult,
        )
        if db is None:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="unsearchable" if force else "wanted",
                search_filetype_override="lossless",
            ))
        if candidate is None:
            candidate = make_album_quality_evidence(
                mb_release_id="test-mbid",
                source_path="/tmp/candidate",
            )
        candidate_result = CandidateEvidenceActionResult(
            evidence=candidate,
            provenance=ActionEvidenceProvenance(
                candidate_status="reused",
                snapshot_guard="matched",
            ),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"fixture audio")
            candidate = msgspec.structs.replace(
                candidate,
                mb_release_id="test-mbid",
                source_path=tmpdir,
                files=snapshot_audio_files(tmpdir),
                snapshot_fingerprint=snapshot_fingerprint(
                    snapshot_audio_files(tmpdir),
                ),
            )
            claimed, candidate_result, execution_lease = _claim_dispatch_job(
                db,
                path=tmpdir,
                release_id="test-mbid",
                force=force,
                candidate_evidence=candidate,
            )
            persisted = candidate_result.evidence
            candidate_result = msgspec.structs.replace(
                candidate_result,
                evidence=persisted,
            )
            cancellation_token = (
                CancellationToken() if execution_lease is not None else None
            )
            with patch_dispatch_externals() as ext, patch(
                "lib.dispatch.subprocess_runner.parse_import_result",
                return_value=make_import_result(decision="import"),
            ), pinned_dispatch_authority(
                db,
                execution_lease,
                cancellation_token=cancellation_token,
            ) as (cancellation_token, owner_session_identity):
                outcome = dispatch_import_core(
                    make_dispatch_request(
                        path=tmpdir,
                        mb_release_id='test-mbid',
                        request_id=42,
                        label='Test Artist - Test Album',
                        force=force,
                        beets_harness_path=_HARNESS,
                        dl_info=DownloadInfo(filetype='flac', username='bad-peer'),
                        scenario='force_import' if force else 'strong_match',
                        requeue_on_failure=not force,
                        candidate_import_job_id=claimed.id,
                        prevalidated_candidate_result=candidate_result,
                        execution_lease=execution_lease,
                        owner_session_identity=owner_session_identity,
                    ),
                    db,
                    cfg=_full_dispatch_config(),
                    quality_gate_fn=noop_quality_gate,
                    current_evidence_loader=lambda *_args, **_kwargs: current_result,
                    cancellation_token=cancellation_token,
                    run_import_fn=_owned_test_runner if execution_lease is not None else None,
                )
        return db, claimed, outcome, ext

    def _persist_failed_outcome(self, db, claimed, outcome) -> None:
        self.assertIsNotNone(outcome.terminal_outcome)
        from tests.dispatch_helpers import finalize_claimed_dispatch

        finalize_claimed_dispatch(db, claimed, outcome)

    def _failed_current_result(self, raw_error: str):
        from lib.import_evidence import (
            ActionEvidenceProvenance,
            CurrentEvidenceActionResult,
        )

        return CurrentEvidenceActionResult(
            evidence=None,
            provenance=ActionEvidenceProvenance(
                current_status="failed",
                snapshot_guard="failed",
                fallback_reason=raw_error,
                installed_path="/library/Test Artist/Test Album",
                fail_closed=True,
            ),
        )

    def test_force_import_fail_closed_current_analysis_preserves_operator_status(self) -> None:
        db, claimed, outcome, ext = self._dispatch_with_current_result(
            self._failed_current_result(
                "PermissionError: [Errno 13] Permission denied"
            ),
            force=True,
        )
        db.set_cooldown_result(True)
        self._persist_failed_outcome(db, claimed, outcome)

        row = db.request(42)
        self.assertEqual(row["status"], "unsearchable")
        self.assertEqual(row["validation_attempts"], 0)
        self.assertIsNone(row["next_retry_after"])
        self.assertEqual(row["search_filetype_override"], "lossless")
        self.assertEqual(db.download_logs[-1].outcome, "have_analysis_error")
        self.assertEqual(db.download_logs[-1].soulseek_username, "bad-peer")
        self.assertEqual(
            db.download_logs[-1].extra["download_path"],
            "/library/Test Artist/Test Album",
        )
        payload = json.loads(db.download_logs[-1].validation_result)
        self.assertEqual(payload["failure_category"], "permission_denied")
        self.assertEqual(
            payload["error"],
            "PermissionError: [Errno 13] Permission denied",
        )
        self.assertEqual(
            payload["installed_path"],
            "/library/Test Artist/Test Album",
        )
        self.assertTrue(payload["candidate_reference"])
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.cooldowns_applied, ["bad-peer"])
        self.assertIn("bad-peer", db.user_cooldowns)
        ext.run.assert_not_called()

    def test_automatic_import_gets_the_same_non_quality_abort(self) -> None:
        db, claimed, outcome, ext = self._dispatch_with_current_result(
            self._failed_current_result("FileNotFoundError: path not found"),
            force=False,
        )
        db.set_cooldown_result(True)
        self._persist_failed_outcome(db, claimed, outcome)

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertEqual(db.download_logs[-1].outcome, "have_analysis_error")
        payload = json.loads(db.download_logs[-1].validation_result)
        self.assertEqual(payload["failure_category"], "path_missing")
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.cooldowns_applied, ["bad-peer"])
        self.assertIn("bad-peer", db.user_cooldowns)
        ext.run.assert_not_called()

    def test_missing_have_is_not_an_analysis_failure(self) -> None:
        from lib.import_evidence import (
            ActionEvidenceProvenance,
            CurrentEvidenceActionResult,
        )

        missing = CurrentEvidenceActionResult(
            evidence=None,
            provenance=ActionEvidenceProvenance(
                current_status="missing",
                snapshot_guard="missing",
                fallback_reason="no current album in beets",
                fail_closed=True,
            ),
        )
        db, _claimed, outcome, ext = self._dispatch_with_current_result(
            missing,
            force=False,
        )

        ext.run.assert_called_once()
        self.assertNotEqual(outcome.code, "have_analysis_error")
        self.assertFalse(any(
            row.outcome == "have_analysis_error" for row in db.download_logs
        ))

    def test_failure_category_taxonomy(self) -> None:
        from lib.import_evidence import classify_have_analysis_failure

        cases = (
            ("PermissionError: permission denied", "permission_denied"),
            ("FileNotFoundError: no such file", "path_missing"),
            ("no audio files found", "no_audio_files"),
            ("snapshot changed during analysis", "snapshot_changed"),
            ("ffmpeg analyser exited 1", "analyser_failure"),
        )
        for raw_error, expected in cases:
            with self.subTest(raw_error=raw_error):
                self.assertEqual(
                    classify_have_analysis_failure(raw_error),
                    expected,
                )
        self.assertEqual(
            classify_have_analysis_failure(
                "current album files changed since evidence capture",
                snapshot_guard="stale",
            ),
            "snapshot_changed",
        )

    def test_abort_is_attempt_local_and_next_healthy_attempt_proceeds(self) -> None:
        from lib.import_evidence import (
            ActionEvidenceProvenance,
            CurrentEvidenceActionResult,
        )
        from lib.quality import AudioQualityMeasurement

        candidate = make_album_quality_evidence(
            mb_release_id="test-mbid",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade="genuine",
            ),
        )
        db, first_claimed, first, first_ext = (
            self._dispatch_with_current_result(
            self._failed_current_result("analyser crashed"),
            force=True,
            candidate=candidate,
            )
        )
        self._persist_failed_outcome(db, first_claimed, first)
        first_ext.run.assert_not_called()

        request = db.request(42)
        request["status"] = "downloading"
        request["active_download_state"] = {"files": [], "filetype": "mp3"}
        healthy = CurrentEvidenceActionResult(
            evidence=make_album_quality_evidence(
                mb_release_id="test-mbid",
                source_path="/library/Test Artist/Test Album",
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=96,
                    avg_bitrate_kbps=96,
                    median_bitrate_kbps=96,
                    format="MP3",
                    spectral_grade="genuine",
                ),
            ),
            provenance=ActionEvidenceProvenance(
                current_status="loaded",
                snapshot_guard="matched",
            ),
        )
        db, _second_claimed, second, second_ext = (
            self._dispatch_with_current_result(
            healthy,
            force=True,
            db=db,
            candidate=candidate,
            )
        )

        second_ext.run.assert_called_once()
        self.assertNotEqual(second.code, "have_analysis_error")
        self.assertEqual(
            sum(log.outcome == "have_analysis_error" for log in db.download_logs),
            1,
        )


class TestDispatchImport(unittest.TestCase):
    """Orchestration tests — assert domain state via FakePipelineDB."""

    _SENTINEL = object()

    def _dispatch(
        self,
        ir=_SENTINEL,
        request_overrides=None,
        *,
        scenario="strong_match",
        force=False,
        initial_status="downloading",
        queued=False,
        cfg=None,
        path_parent=None,
        cleanup_tmpdir=True,
        skip_finalize=False,
    ):
        """``path_parent``/``cleanup_tmpdir``/``skip_finalize`` (issue
        #1122 F2, review round 2): let a caller control where the staged
        ``path`` directory lives (e.g. under a real auto-import staging
        root), opt out of the default post-dispatch teardown so it can
        drive real post-commit cleanup against the still-present directory
        afterward, and opt out of ``finalize_claimed_dispatch`` -- whose
        AUTOMATION branch owns its own journaled cleanup lane
        (``_complete_automation_processing_cleanup``) that deletes the
        staged directory through a completely different path before a
        caller could ever reach ``_run_post_commit_cleanup``, per
        ``TestRunPostCommitCleanupProtectedParent``'s own docstring. Every
        existing caller keeps the original arbitrary-tmpdir,
        always-cleaned-up, always-finalized behavior by construction (all
        three default to the prior literal behavior)."""
        from lib.dispatch import dispatch_import_core
        if ir is self._SENTINEL:
            ir = make_import_result(decision="import")

        cfg = cfg or _full_dispatch_config()
        dl_info = DownloadInfo(filetype="mp3")

        mock_gate = RecordingQualityGate()
        tmpdir = tempfile.mkdtemp(dir=path_parent)
        try:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"fixture audio")
            del queued  # every Beets seam now requires a claimed job
            db = FakePipelineDB()
            supplied_overrides = dict(request_overrides or {})
            active_state = dict(
                supplied_overrides.get("active_download_state") or {}
            )
            active_state.setdefault("files", [])
            active_state.setdefault("filetype", "mp3")
            active_state["current_path"] = tmpdir
            request_overrides = {
                "mb_release_id": "test-mbid",
                **supplied_overrides,
                "active_download_state": active_state,
            }
            db.seed_request(make_request_row(
                id=42, status=initial_status,
                **request_overrides,
            ))
            claimed, candidate_result, execution_lease = _claim_dispatch_job(
                db,
                path=tmpdir,
                release_id="test-mbid",
                force=force,
            )
            import_job_id = claimed.id
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
                outcome = dispatch_import_core(
                    make_dispatch_request(
                        path=tmpdir,
                        mb_release_id='test-mbid',
                        request_id=42,
                        label='Test Artist - Test Album',
                        beets_harness_path=_HARNESS,
                        dl_info=dl_info,
                        distance=0.05,
                        scenario=scenario,
                        files=[make_download_file(username='user1', filename='01 - Track.mp3')],
                        force=force,
                        requeue_on_failure=not force,
                        candidate_import_job_id=import_job_id,
                        prevalidated_candidate_result=candidate_result,
                        execution_lease=execution_lease,
                        owner_session_identity=owner_session_identity,
                    ),
                    db,
                    cfg=cfg,
                    quality_gate_fn=mock_gate,
                    cancellation_token=cancellation_token,
                    run_import_fn=_owned_test_runner if execution_lease is not None else None,
                )
            if not skip_finalize:
                from tests.dispatch_helpers import finalize_claimed_dispatch

                finalize_claimed_dispatch(db, claimed, outcome)
        finally:
            if cleanup_tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)

        return {
            "db": db,
            "outcome": outcome,
            "mock_cleanup": ext.cleanup,
            "mock_plex": ext.plex,
            "mock_jellyfin": ext.jellyfin,
            "mock_gate": mock_gate,
            "tmpdir": tmpdir,
        }

    def test_operator_retained_import_decisions_record_policy_without_reopening(self):
        # Force imports resolve through the same quality/search mapping as
        # automatic imports, but only the operator may clear the search stop.
        # The quality fields are recorded and the current operator stop holds.
        decisions = (
            ("provisional_lossless_upgrade", "lossless"),
            ("transcode_upgrade", None),
            ("transcode_first", None),
        )
        operator_modes = (("force", "force_import", True),)
        for mode, scenario, force in operator_modes:
            for decision, expected_override in decisions:
                with self.subTest(mode=mode, decision=decision):
                    result = self._dispatch(
                        make_import_result(decision=decision),
                        scenario=scenario,
                        force=force,
                        initial_status="unsearchable",
                        queued=True,
                    )
                    row = result["db"].request(42)
                    self.assertEqual(row["status"], "unsearchable")
                    self.assertEqual(
                        row["search_filetype_override"], expected_override)
                    self.assertEqual(
                        [e.username for e in result["db"].denylist],
                        ["user1"])
                    result["mock_gate"].assert_not_called()

    def test_force_retained_import_from_wanted_remains_wanted(self):
        result = self._dispatch(
            make_import_result(decision="provisional_lossless_upgrade"),
            scenario="force_import",
            force=True,
            initial_status="wanted",
            queued=True,
        )
        row = result["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "lossless")

    def test_retained_import_cannot_widen_existing_lossless_scope(self):
        result = self._dispatch(
            make_import_result(decision="transcode_upgrade"),
            scenario="force_import",
            force=True,
            initial_status="wanted",
            queued=True,
            request_overrides={"search_filetype_override": "lossless"},
        )

        row = result["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "lossless")

    def test_import_success(self):
        imported_path = "/mnt/virtio/Music/Beets/Test Artist/2026 - Test Album"
        ir = make_import_result(
            decision="import", imported_path=imported_path)
        r = self._dispatch(ir)
        self.assertEqual(r["db"].request(42)["status"], "imported")
        self.assertEqual(len(r["db"].download_logs), 1)
        self.assertEqual(r["db"].download_logs[0].outcome, "success")
        r["mock_plex"].assert_called_once()
        self.assertEqual(r["mock_plex"].call_args.args[1], imported_path)
        r["mock_jellyfin"].assert_called_once()
        self.assertEqual(
            r["mock_jellyfin"].call_args.args[1], imported_path)
        cleanup = r["outcome"].post_commit_cleanup
        assert cleanup is not None
        self.assertIsNotNone(cleanup.staged_path)
        # Issue #1077, R4-1 (round-4 review; widened issue #1122 F2, review
        # round 2): the SUCCESS-path plan must carry the same
        # ``staged_path_protected_parents`` guard as the reject path's plan
        # (R3-3) — before this fix a successful force job's ``staged_path``
        # (``<processing_dir>/albums/force-action-<id>``, a direct child of
        # the shared albums root) reached ``_run_post_commit_cleanup``
        # completely unguarded, and after R4-1 it protected ONLY that root
        # even though the same ``post_commit_staged_path = path`` assignment
        # also covers a successful YouTube rescue's auto-import-root child
        # (F2) — so the full set is asserted, not just the processing root.
        from lib.processing_paths import protected_staging_roots

        full_cfg = _full_dispatch_config()
        self.assertEqual(
            cleanup.staged_path_protected_parents,
            protected_staging_roots(
                processing_dir=full_cfg.processing_dir,
                beets_staging_dir=full_cfg.beets_staging_dir,
            ),
        )
        r["mock_gate"].assert_called_once()

    def test_youtube_shaped_success_protects_the_auto_import_root(self):
        """Issue #1122 F2 (review round 2): before this fix,
        ``dispatch_import_core``'s success builder hardcoded
        ``staged_path_protected_parents`` to ONLY
        ``processing_albums_dir``, even though ``post_commit_staged_path =
        path`` (~:1448) is set for EVERY successful cleanup-eligible lane —
        including a YouTube rescue, whose ``path`` is a direct child of the
        auto-import staging root (it imports in place, never materialized
        under the canonical processing root). ``test_import_success`` above
        proves the RETURNED set is correct in isolation; this test drives
        the same real ``dispatch_import_core`` with a YT-SHAPED ``path``
        (a direct child of a real auto-import root, its only child) through
        a real successful import -- ``skip_finalize=True`` because
        ``finalize_claimed_dispatch``'s AUTOMATION branch owns its own
        journaled cleanup lane
        (``_complete_automation_processing_cleanup``) that would delete
        the staged directory through a completely different path before
        this test could ever reach ``_run_post_commit_cleanup`` -- then
        feeds the real returned outcome to the real
        ``scripts.importer._run_post_commit_cleanup`` (-> real
        ``_cleanup_staged_dir``) directly and proves the shared,
        externally provisioned auto-import root survives even though the
        staged folder was its only child."""
        import dataclasses

        from lib.processing_paths import stage_to_ai_root
        from scripts.importer import _run_post_commit_cleanup

        with tempfile.TemporaryDirectory() as world_root:
            cfg = dataclasses.replace(
                _full_dispatch_config(),
                processing_dir=os.path.join(world_root, "processing"),
                beets_staging_dir=os.path.join(world_root, "Incoming"),
            )
            auto_import_root = stage_to_ai_root(
                staging_dir=cfg.beets_staging_dir, auto_import=True,
            )
            os.makedirs(auto_import_root)

            imported_path = (
                "/mnt/virtio/Music/Beets/Test Artist/2026 - Test Album"
            )
            ir = make_import_result(
                decision="import", imported_path=imported_path)
            r = self._dispatch(
                ir,
                cfg=cfg,
                path_parent=auto_import_root,
                cleanup_tmpdir=False,
                skip_finalize=True,
            )

            self.assertTrue(r["outcome"].success)
            self.assertTrue(
                os.path.isdir(r["tmpdir"]),
                "dispatch_import_core's success path never mutates the "
                "filesystem synchronously -- cleanup is a deferred plan "
                "the caller runs later",
            )

            details = _run_post_commit_cleanup(r["outcome"])

            assert details is not None
            staged_path_detail = details["staged_path"]
            assert isinstance(staged_path_detail, dict)
            self.assertTrue(staged_path_detail["success"])
            self.assertFalse(os.path.exists(r["tmpdir"]))
            self.assertTrue(
                os.path.isdir(auto_import_root),
                "the shared auto-import staging root must survive even "
                "though it is now empty -- it is externally provisioned, "
                "not a disposable per-artist directory",
            )
            self.assertEqual(os.listdir(auto_import_root), [])

    def test_import_with_bad_extensions_logs_error_and_persists_jsonb(self):
        from lib.quality import ImportResult

        ir = make_import_result(decision="import")
        ir.postflight.bad_extensions = ["01 Track.bak"]

        with self.assertLogs("cratedigger", level="ERROR") as logs:
            r = self._dispatch(ir)

        self.assertIn("POSTFLIGHT BAD EXTENSIONS", "\n".join(logs.output))
        raw = r["db"].download_logs[0].import_result
        assert isinstance(raw, str)
        persisted = ImportResult.from_json(raw)
        self.assertEqual(persisted.postflight.bad_extensions,
                         ["01 Track.bak"])

    def test_preflight_existing(self):
        ir = make_import_result(decision="preflight_existing")
        r = self._dispatch(ir)
        self.assertEqual(r["db"].request(42)["status"], "imported")
        self.assertEqual(r["db"].download_logs[0].outcome, "success")

    def test_import_with_upgrade_delta(self):
        ir = make_import_result(decision="import", new_min_bitrate=245,
                                prev_min_bitrate=192)
        r = self._dispatch(ir)
        self.assertEqual(r["db"].request(42)["status"], "imported")

    def test_downgrade_narrows_transparent_genuine_have_to_lossless(self):
        """The real post-subprocess dispatch persists the pure-policy result."""
        from lib.quality import SpectralAnalysisDetail, SpectralDetail

        ir = ImportResult(
            decision="downgrade",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=226,
                avg_bitrate_kbps=226,
                median_bitrate_kbps=226,
                format="MP3",
                is_cbr=False,
            ),
            current_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
            ),
            spectral=SpectralDetail(
                existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                ),
            ),
        )

        result = self._dispatch(ir, request_overrides={
            "current_spectral_grade": None,
            "search_filetype_override": None,
            "target_format": None,
        })

        row = result["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "lossless")
        self.assertIsNone(row["target_format"])

    def test_import_clears_stale_current_source_probe(self):
        ir = make_import_result(decision="import", new_min_bitrate=245)
        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_min_bitrate": 165,
            "current_lossless_source_v0_probe_avg_bitrate": 228,
            "current_lossless_source_v0_probe_median_bitrate": 225,
        })

        row = r["db"].request(42)
        self.assertIsNone(row["current_lossless_source_v0_probe_min_bitrate"])
        self.assertIsNone(row["current_lossless_source_v0_probe_avg_bitrate"])
        self.assertIsNone(row["current_lossless_source_v0_probe_median_bitrate"])

    def test_preflight_existing_preserves_current_source_probe(self):
        ir = make_import_result(decision="preflight_existing")
        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_min_bitrate": 165,
            "current_lossless_source_v0_probe_avg_bitrate": 228,
            "current_lossless_source_v0_probe_median_bitrate": 225,
        })

        row = r["db"].request(42)
        self.assertEqual(row["current_lossless_source_v0_probe_min_bitrate"], 165)
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 228)
        self.assertEqual(row["current_lossless_source_v0_probe_median_bitrate"], 225)

    def test_downgrade_rejected(self):
        ir = make_import_result(decision="downgrade", new_min_bitrate=192,
                                prev_min_bitrate=320)
        r = self._dispatch(ir)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertEqual(r["db"].request(42)["status"], "wanted")
        self.assertTrue(len(r["db"].denylist) > 0)
        cleanup = r["outcome"].post_commit_cleanup
        assert cleanup is not None
        self.assertIsNotNone(cleanup.staged_path)

    def test_downgrade_passes_narrowed_override_to_transition(self):
        ir = make_import_result(decision="downgrade", new_min_bitrate=320,
                                prev_min_bitrate=320)
        r = self._dispatch(ir, request_overrides={
            "search_filetype_override": "flac,mp3 v0,mp3 320",
        })
        self.assertEqual(
            r["db"].request(42)["search_filetype_override"], "flac,mp3 v0")

    def test_downgrade_preserves_override_when_tier_not_matched(self):
        ir = make_import_result(decision="downgrade", new_min_bitrate=320,
                                prev_min_bitrate=320)
        r = self._dispatch(ir, request_overrides={
            "search_filetype_override": "flac",
        })
        # No narrowing: "mp3 320" tier not in "flac"-only override
        # reset_to_wanted without search_filetype_override → preserved
        # The override should not have been changed from what reset_to_wanted sets
        override = r["db"].request(42)["search_filetype_override"]
        # narrowing returns None when no tier matches, so reset_to_wanted
        # doesn't pass search_filetype_override, preserving the original "flac"
        self.assertEqual(override, "flac")

    def test_transcode_upgrade(self):
        ir = make_import_result(decision="transcode_upgrade",
                                new_min_bitrate=227)
        r = self._dispatch(ir)
        self.assertEqual(r["db"].download_logs[0].outcome, "success")
        self.assertEqual(r["db"].request(42)["status"], "wanted")
        self.assertTrue(len(r["db"].denylist) > 0)

    def test_transcode_downgrade(self):
        ir = make_import_result(decision="transcode_downgrade",
                                new_min_bitrate=190)
        r = self._dispatch(ir)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertTrue(len(r["db"].denylist) > 0)
        self.assertEqual(r["db"].request(42)["status"], "wanted")

    def test_provisional_lossless_upgrade_imports_requeues_and_persists_probe(self):
        probe = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=165,
            avg_bitrate_kbps=228,
            median_bitrate_kbps=225,
        )
        ir = make_import_result(
            decision="provisional_lossless_upgrade",
            new_min_bitrate=128,
            spectral_grade="suspect",
            spectral_bitrate=160,
            verified_lossless=False,
            final_format="opus 128",
            v0_probe=probe,
        )

        r = self._dispatch(ir)

        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertFalse(row["verified_lossless"])
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 228)
        self.assertEqual(row["search_filetype_override"], QUALITY_FLAC_ONLY)
        self.assertEqual(r["db"].download_logs[0].outcome, "success")
        self.assertEqual(r["db"].download_logs[0].beets_scenario,
                         "provisional_lossless_upgrade")
        self.assertEqual(r["db"].download_logs[0].extra["v0_probe_avg_bitrate"],
                         228)
        self.assertTrue(len(r["db"].denylist) > 0)

    def test_suspect_lossless_downgrade_rejects_without_probe_update(self):
        probe = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=165,
            avg_bitrate_kbps=175,
            median_bitrate_kbps=174,
        )
        existing = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=128,
            avg_bitrate_kbps=171,
            median_bitrate_kbps=169,
        )
        ir = make_import_result(
            decision="suspect_lossless_downgrade",
            new_min_bitrate=128,
            spectral_grade="suspect",
            spectral_bitrate=160,
            verified_lossless=False,
            v0_probe=probe,
            existing_v0_probe=existing,
        )

        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_avg_bitrate": 171,
        })

        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 171)
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["next_retry_after"])
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertEqual(r["db"].download_logs[0].beets_scenario,
                         "suspect_lossless_downgrade")
        self.assertEqual(r["db"].download_logs[0].extra["v0_probe_avg_bitrate"],
                         175)
        self.assertEqual(
            r["db"].download_logs[0].extra["existing_v0_probe_avg_bitrate"],
            171,
        )
        self.assertTrue(len(r["db"].denylist) > 0)

    def test_suspect_lossless_probe_missing_requeues_without_probe_update(self):
        ir = make_import_result(
            decision="suspect_lossless_probe_missing",
            new_min_bitrate=128,
            spectral_grade="suspect",
            spectral_bitrate=160,
            verified_lossless=False,
            error="suspect lossless source lacks a comparable V0 probe",
        )

        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_min_bitrate": 128,
            "current_lossless_source_v0_probe_avg_bitrate": 171,
            "current_lossless_source_v0_probe_median_bitrate": 169,
        })

        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 171)
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["next_retry_after"])
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertEqual(r["db"].download_logs[0].beets_scenario,
                         "suspect_lossless_probe_missing")
        self.assertIn(
            "comparable V0 probe",
            r["db"].download_logs[0].beets_detail,
        )
        self.assertTrue(len(r["db"].denylist) > 0)

    def test_lossless_source_locked_rejects_lossy_candidate(self):
        # Wire-boundary test: import_one.py emits decision=lossless_source_locked
        # for a lossy candidate the gate refused to compare against an
        # existing lossless-source V0 probe. Dispatch must:
        #   - record a rejected download_log with beets_scenario=lossless_source_locked
        #   - put a human-readable detail referencing the existing probe
        #   - clear ir.error from the stored row (it's a domain rejection, not a crash)
        #   - denylist + requeue the request to wanted
        existing = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=210,
            avg_bitrate_kbps=240,
            median_bitrate_kbps=235,
        )
        ir = make_import_result(
            decision="lossless_source_locked",
            new_min_bitrate=176,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            verified_lossless=False,
            existing_v0_probe=existing,
            error=("existing has lossless-source V0 probe 240kbps; lossy "
                   "candidate cannot produce comparable evidence"),
        )

        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_avg_bitrate": 240,
        })

        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 240)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertEqual(r["db"].download_logs[0].beets_scenario,
                         "lossless_source_locked")
        self.assertIn(
            "240",
            r["db"].download_logs[0].beets_detail or "",
        )
        # ir.error is suppressed for lossless_source_locked — domain rejections
        # should not bleed into the error_message column (mirrors suspect_lossless_*).
        self.assertIsNone(r["db"].download_logs[0].error_message)
        self.assertTrue(len(r["db"].denylist) > 0)

    def test_error_decision(self):
        ir = make_import_result(decision="conversion_failed",
                                error="ffmpeg failed")
        r = self._dispatch(ir)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")

    def test_duplicate_remove_guard_failure_denylists_and_quarantines(self):
        from lib.dispatch import dispatch_import_core
        from lib.quality import SpectralAnalysisDetail, SpectralDetail

        root = tempfile.mkdtemp()
        processing_dir = os.path.join(root, "processing")
        processing_albums = os.path.join(processing_dir, "albums")
        staging_root = os.path.join(root, "Incoming")
        source = os.path.join(processing_albums, "Artist - Album")
        os.makedirs(source)
        os.makedirs(staging_root)
        with open(os.path.join(source, "track.mp3"), "w", encoding="utf-8") as f:
            f.write("x")

        guard = DuplicateRemoveGuardInfo(
            reason="duplicate_count_not_one",
            target_source="musicbrainz",
            target_release_id="test-mbid",
            duplicate_count=2,
            message="beets reported 2 duplicate albums; expected exactly 1",
            candidates=[
                DuplicateRemoveCandidate(
                    beets_album_id=100,
                    mb_albumid="test-mbid",
                    album_path="/Beets/Artist/Album",
                    item_count=10,
                ),
                DuplicateRemoveCandidate(
                    beets_album_id=101,
                    mb_albumid="other-mbid",
                    album_path="/Beets/Artist/Album [2006]",
                    item_count=11,
                ),
            ],
        )
        ir = make_import_result(
            decision="duplicate_remove_guard_failed",
            new_min_bitrate=245,
            prev_min_bitrate=128,
            was_converted=True,
            original_filetype="flac",
            target_filetype="opus",
        )
        ir.exit_code = 7
        ir.error = guard.message
        ir.postflight.duplicate_remove_guard = guard
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="suspect", bitrate_kbps=128),
            existing=SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=245),
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "mp3"},
        ))
        claimed, candidate, execution_lease = _claim_dispatch_job(
            db,
            path=source,
            release_id="test-mbid",
        )
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            beets_staging_dir=staging_root,
            processing_dir=processing_dir,
            pipeline_db_enabled=True,
        )
        try:
            cancellation_token = CancellationToken()
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 pinned_dispatch_authority(
                     db,
                     execution_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                outcome = dispatch_import_core(
                    make_dispatch_request(
                        path=source,
                        mb_release_id='test-mbid',
                        request_id=42,
                        label='Artist - Album',
                        beets_harness_path=_HARNESS,
                        dl_info=DownloadInfo(filetype='mp3', username='user1'),
                        distance=0.05,
                        scenario='strong_match',
                        files=[],
                        requeue_on_failure=True,
                        attempt_spectral_audit=audit,
                        candidate_import_job_id=claimed.id,
                        prevalidated_candidate_result=candidate,
                        execution_lease=execution_lease,
                        owner_session_identity=owner_session_identity,
                    ),
                    db,
                    cfg=cfg,
                    cancellation_token=cancellation_token,
                    run_import_fn=_owned_test_runner,
                )
                assert outcome.terminal_outcome is not None
                from tests.dispatch_helpers import finalize_claimed_dispatch

                finalize_claimed_dispatch(db, claimed, outcome)
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(db.download_logs[0].outcome, "rejected")
        self.assertEqual(db.download_logs[0].beets_scenario,
                         "duplicate_remove_guard_failed")
        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertNotEqual(db.request(42)["status"], "unsearchable")
        self.assertEqual(len(db.denylist), 1)
        self.assertEqual(db.denylist[0].username, "user1")
        ext.cleanup.assert_not_called()

        persisted = ImportResult.from_json(db.download_logs[0].import_result)
        self.assertEqual(persisted.spectral, audit)
        self.assertEqual(persisted.decision, "duplicate_remove_guard_failed")
        self.assertIsNotNone(persisted.source_measurement)
        self.assertIsNotNone(persisted.current_measurement)
        self.assertTrue(persisted.conversion.was_converted)
        persisted_guard = persisted.postflight.duplicate_remove_guard
        assert persisted_guard is not None
        self.assertIsNone(persisted_guard.quarantine_path)
        completed_job = db.get_import_job(claimed.id)
        assert completed_job is not None and completed_job.result is not None
        quarantine = completed_job.result["processing_cleanup"]
        self.assertIsNotNone(quarantine["destination_path"])
        assert quarantine["destination_path"] is not None
        self.assertTrue(quarantine["destination_path"].startswith(
            os.path.join(processing_albums, "duplicate-remove-guard"),
        ))
        self.assertFalse(quarantine["destination_path"].startswith(staging_root))
        self.assertFalse(os.path.exists(source))

    def _assert_world_failure_self_heal(
        self,
        db: FakePipelineDB,
        job: ImportJob,
        *,
        diagnostic: str,
    ) -> None:
        """Invariant 11: a broken automation world surfaces and restarts.

        The exact owner is released, the request rejoins the search pool with
        the attempt counted, and the diagnostic reads in Recents as one
        ``failed`` download_log row. ``recovery_required`` would instead
        strand ``processing`` behind an inactive job that ``get_wanted``
        never selects again — the album silently stops being acquired.
        """
        from tests.test_import_queue import (
            assert_world_failure_audit,
            automation_world_failure_violation,
        )

        row = db.request(42)
        self.assertEqual(job.status, "failed")
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["active_automation_import_job_id"])
        self.assertIsNone(automation_world_failure_violation(
            label=diagnostic,
            escaped=None,
            job_status=job.status,
            request_status=str(row["status"]),
            active_owner=row["active_automation_import_job_id"],
        ))
        # Retry cadence is retained counters plus backoff, never parking.
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["next_retry_after"])
        assert_world_failure_audit(self, db, diagnostic=diagnostic)
        # The self-healed job is terminal: nothing replays it behind the
        # request that is already back in the pool.
        self.assertIsNone(claim_next_import_job(db, worker_id="automatic-replay"))

    def test_no_json_result(self):
        r = self._dispatch(None)
        db = r["db"]
        job = db.get_import_job(1)
        assert job is not None
        self._assert_world_failure_self_heal(
            db,
            job,
            diagnostic="Beets returned without a terminal result",
        )

    def test_timeout(self):
        from lib.dispatch import dispatch_import_core
        from scripts.importer import process_claimed_job
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "mp3"},
        ))
        claimed, candidate, execution_lease = _claim_dispatch_job(
            db,
            path="/tmp/dest",
            release_id="test-mbid",
        )

        def execute(
            db_arg,
            _job,
            *,
            ctx=None,
            execution_lease=None,
            cancellation_token=None,
            owner_session_identity=None,
        ):
            del ctx
            assert execution_lease is not None
            assert cancellation_token is not None
            assert owner_session_identity is not None
            return dispatch_import_core(
                make_dispatch_request(
                    path='/tmp/dest',
                    mb_release_id='test-mbid',
                    request_id=42,
                    label='Test',
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype='mp3'),
                    candidate_import_job_id=claimed.id,
                    prevalidated_candidate_result=candidate,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                ),
                db_arg,
                cancellation_token=cancellation_token,
                run_import_fn=_owned_test_runner,
            )

        cancellation_token = CancellationToken()
        with patch(
            "lib.dispatch.subprocess_runner.sp.run",
            side_effect=sp.TimeoutExpired(cmd="test", timeout=1800),
        ), pinned_dispatch_authority(
            db,
            execution_lease,
            cancellation_token=cancellation_token,
        ) as (cancellation_token, owner_session_identity):
            recovered = process_claimed_job(
                db,  # type: ignore[arg-type]
                claimed,
                execute_fn=execute,
                execution_lease=execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )

        assert recovered is not None
        self._assert_world_failure_self_heal(
            db,
            recovered,
            diagnostic="Import timed out after Beets launch",
        )

    def test_exception(self):
        from lib.dispatch import dispatch_import_core
        from scripts.importer import process_claimed_job
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "mp3"},
        ))
        claimed, candidate, execution_lease = _claim_dispatch_job(
            db,
            path="/tmp/dest",
            release_id="test-mbid",
        )

        def execute(
            db_arg,
            _job,
            *,
            ctx=None,
            execution_lease=None,
            cancellation_token=None,
            owner_session_identity=None,
        ):
            del ctx
            assert execution_lease is not None
            assert cancellation_token is not None
            assert owner_session_identity is not None
            return dispatch_import_core(
                make_dispatch_request(
                    path='/tmp/dest',
                    mb_release_id='test-mbid',
                    request_id=42,
                    label='Test',
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype='mp3'),
                    candidate_import_job_id=claimed.id,
                    prevalidated_candidate_result=candidate,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                ),
                db_arg,
                cancellation_token=cancellation_token,
                run_import_fn=_owned_test_runner,
            )

        cancellation_token = CancellationToken()
        with patch(
            "lib.dispatch.subprocess_runner.sp.run",
            side_effect=RuntimeError("boom"),
        ), pinned_dispatch_authority(
            db,
            execution_lease,
            cancellation_token=cancellation_token,
        ) as (cancellation_token, owner_session_identity):
            recovered = process_claimed_job(
                db,  # type: ignore[arg-type]
                claimed,
                execute_fn=execute,
                execution_lease=execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )

        assert recovered is not None
        self._assert_world_failure_self_heal(
            db,
            recovered,
            diagnostic=(
                "Import failed after Beets launch without a terminal "
                "acknowledgement"
            ),
        )


class TestImportDispatchRescueCapture(unittest.TestCase):
    """U14: long-tail-rescue audit columns populated atomically on import.

    When ``dispatch_import_core`` flips a request to ``imported`` and
    that request was previously categorised unfindable, the importer
    must capture the rescue event (``rescued_at``,
    ``prior_unfindable_category``) in the same atomic write as the
    status flip.

    Verifies the wiring through ``apply_transition`` →
    ``mark_imported_with_rescue`` on the FakePipelineDB; the real-PG
    atomicity contract lives in
    ``tests/test_pipeline_db.py::TestMarkImportedWithRescue`` and
    ``tests/test_integration_slices.py::TestRescueCaptureSlice``.
    """

    _HARNESS_PATH = _HARNESS

    def _dispatch_with_unfindable(self, *, prior_category, rescued_at=None,
                                  prior_rescue_category=None):
        """Drive a successful import on a previously-unfindable request."""
        from datetime import datetime

        from lib.dispatch import dispatch_import_core

        ir = make_import_result(decision="import")
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            active_download_state={"files": [], "filetype": "mp3"},
        ))
        # Seed the row's unfindable state directly so the test starts
        # from the "categorised, just finished downloading" shape.
        if prior_category is not None:
            db._requests[42]["unfindable_category"] = prior_category
            db._requests[42]["unfindable_categorised_at"] = datetime(
                2026, 5, 20, tzinfo=UTC)
        if rescued_at is not None:
            db._requests[42]["rescued_at"] = rescued_at
        if prior_rescue_category is not None:
            db._requests[42]["prior_unfindable_category"] = (
                prior_rescue_category)
        cfg = CratediggerConfig(
            beets_harness_path=self._HARNESS_PATH,
            pipeline_db_enabled=True,
        )

        tmpdir = tempfile.mkdtemp()
        try:
            claimed, candidate, execution_lease = _claim_dispatch_job(
                db,
                path=tmpdir,
                release_id="test-mbid",
            )
            cancellation_token = CancellationToken()
            with patch_dispatch_externals(), \
                 patch("lib.dispatch.subprocess_runner.parse_import_result",
                       return_value=ir), \
                 pinned_dispatch_authority(
                     db,
                     execution_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                outcome = dispatch_import_core(
                    make_dispatch_request(
                        path=tmpdir,
                        mb_release_id='test-mbid',
                        request_id=42,
                        label='Rescue Artist - Album',
                        beets_harness_path=self._HARNESS_PATH,
                        dl_info=DownloadInfo(filetype='mp3'),
                        distance=0.05,
                        scenario='strong_match',
                        files=[make_download_file(username='u1', filename='01 - T.mp3')],
                        candidate_import_job_id=claimed.id,
                        prevalidated_candidate_result=candidate,
                        execution_lease=execution_lease,
                        owner_session_identity=owner_session_identity,
                    ),
                    db,
                    cfg=cfg,
                    quality_gate_fn=noop_quality_gate,
                    cancellation_token=cancellation_token,
                    run_import_fn=_owned_test_runner,
                )
                assert outcome.terminal_outcome is not None
                from tests.dispatch_helpers import finalize_claimed_dispatch

                finalize_claimed_dispatch(db, claimed, outcome)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        return db

    def test_import_captures_rescue_when_unfindable_category_was_set(self):
        for category in (
            "artist_absent",
            "album_absent_artist_present",
            "one_track_structural",
            "wrong_pressing_available",
        ):
            with self.subTest(category=category):
                db = self._dispatch_with_unfindable(prior_category=category)
                row = db.request(42)
                self.assertEqual(row["status"], "imported")
                self.assertIsNone(row["unfindable_category"])
                self.assertEqual(
                    row["prior_unfindable_category"], category)
                self.assertIsNotNone(row["rescued_at"])

    def test_import_without_prior_unfindable_does_not_stamp_rescue(self):
        db = self._dispatch_with_unfindable(prior_category=None)
        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["rescued_at"])
        self.assertIsNone(row["prior_unfindable_category"])
        self.assertIsNone(row["unfindable_category"])

    def test_re_import_after_prior_rescue_does_not_overwrite_audit_columns(
        self,
    ):
        """One-shot capture — first rescue wins forever."""
        from datetime import datetime

        original_rescue_at = datetime(2026, 1, 15, tzinfo=UTC)
        db = self._dispatch_with_unfindable(
            prior_category="album_absent_artist_present",
            rescued_at=original_rescue_at,
            prior_rescue_category="artist_absent",
        )
        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertEqual(row["rescued_at"], original_rescue_at)
        self.assertEqual(row["prior_unfindable_category"], "artist_absent")
        # The current (later) category still gets cleared.
        self.assertIsNone(row["unfindable_category"])


class TestOverrideMinBitrate(unittest.TestCase):
    """Seam tests — subprocess arg wiring for --override-min-bitrate.

    Tests the surviving auto-import seam's override computation.

    The override must be grade-aware: spectral bitrate only participates when
    current_spectral_grade is in {suspect, likely_transcode}. Genuine/marginal/
    None grades must leave the container bitrate untouched — see issue #61.
    """

    def _get_override_value(self, min_br, spectral_br, grade,
                            codec_family: CodecFamily = "mp3"):
        album_data = _make_album_data()
        album_data.current_min_bitrate = min_br
        album_data.current_spectral = SpectralMeasurement.from_parts(
            grade,
            spectral_br,
            # ``collect_release_attempt_spectral_audit`` stamps the measured
            # family on every fresh audit (issue #829 Phase 5 PR1), and PR2b
            # made the override read it. A fixture omitting it describes a
            # measurement this seam's producer cannot emit.
            codec_family=codec_family,
        )
        cmd = _dispatch_valid_result_cmd(album_data=album_data)

        for i, arg in enumerate(cmd):
            if arg == "--override-min-bitrate" and i + 1 < len(cmd):
                return int(cmd[i + 1])
        return None

    # (description, min_bitrate, current_spectral_bitrate, current_spectral_grade, expected)
    CASES: ClassVar = [
        ("suspect spectral lower wins",             320, 128, "suspect",          128),
        ("likely_transcode spectral lower wins",    320, 128, "likely_transcode", 128),
        ("genuine spectral ignored even if lower",  320, 128, "genuine",          320),
        ("marginal spectral ignored even if lower", 320, 128, "marginal",         320),
        ("grade None ignores spectral",             320, 128, None,               320),
        ("suspect grade but spectral higher",       192, 256, "suspect",          192),
        ("no spectral, grade genuine",              320, None, "genuine",         320),
        ("no spectral, grade None",                 320, None, None,              320),
        ("no container no spectral",                None, None, None,             None),
        ("no container, suspect spectral",          None, 128, "suspect",         128),
        ("no container, genuine spectral ignored",  None, 128, "genuine",         None),
    ]

    def test_uncalibrated_codec_never_floors_the_have(self):
        """Issue #829 (download 37946) at the auto-import seam.

        The same 320/128/suspect facts that floor an MP3 to 128 must leave
        an AAC untouched: LAME's encoder table says nothing about an AAC's
        natural rolloff, so the container bitrate stands. Opus asserts
        nothing at all either.
        """
        families: tuple[CodecFamily, ...] = ("aac", "opus", "other")
        for family in families:
            with self.subTest(codec_family=family):
                self.assertEqual(
                    self._get_override_value(
                        320, 128, "suspect", codec_family=family),
                    320,
                )
        self.assertEqual(
            self._get_override_value(320, 128, "suspect", codec_family="mp3"),
            128,
        )

    def test_override_from_attempt_local_have_table(self):
        for desc, min_br, spectral_br, grade, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    self._get_override_value(min_br, spectral_br, grade), expected,
                    f"{desc}: override from min_bitrate={min_br!r} "
                    f"spectral_bitrate={spectral_br!r} grade={grade!r} "
                    f"expected {expected!r}",
                )


class TestDispatchRankConfigArgv(unittest.TestCase):
    """Seam test — harness argv must carry --quality-rank-config JSON.

    Verifies the QualityRankConfig round-trips through the subprocess
    boundary unchanged, so the harness's rank classification matches the
    caller's runtime config. Will break if import_one becomes a library
    call (#48) or if QualityRankConfig.to_json() changes shape.
    """

    def _run_dispatch_capture_cmd(self, cfg_obj):
        """Call dispatch_import_core with cfg_obj, return captured argv."""
        from lib.dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "mp3"},
        ))
        ir = make_import_result(decision="import")
        claimed, candidate, execution_lease = _claim_dispatch_job(
            db,
            path="/tmp/dest",
            release_id="mbid-1",
        )

        cancellation_token = CancellationToken()
        with patch_dispatch_externals() as ext, \
             patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
             pinned_dispatch_authority(
                 db,
                 execution_lease,
                 cancellation_token=cancellation_token,
             ) as (cancellation_token, owner_session_identity):
            dispatch_import_core(
                make_dispatch_request(
                    path='/tmp/dest',
                    mb_release_id='mbid-1',
                    request_id=42,
                    label='Test Artist - Test Album',
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype='mp3'),
                    files=[make_download_file(username='user1', filename='01.mp3')],
                    candidate_import_job_id=claimed.id,
                    prevalidated_candidate_result=candidate,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                ),
                db,
                cfg=cfg_obj,
                quality_gate_fn=noop_quality_gate,
                cancellation_token=cancellation_token,
                run_import_fn=_owned_test_runner,
            )
            return ext.run.call_args[0][0]

    def _extract_rank_config_json(self, cmd):
        for i, arg in enumerate(cmd):
            if arg == "--quality-rank-config" and i + 1 < len(cmd):
                return cmd[i + 1]
        return None

    def test_default_cfg_serializes_to_argv(self):
        """Default QualityRankConfig → argv contains the round-trip JSON."""
        from lib.config import CratediggerConfig
        from lib.quality import QualityRankConfig
        cfg = CratediggerConfig(beets_harness_path=_HARNESS)
        cmd = self._run_dispatch_capture_cmd(cfg)
        raw = self._extract_rank_config_json(cmd)
        self.assertIsNotNone(raw)
        assert raw is not None  # for pyright
        # Round-trip must produce an equal QualityRankConfig
        restored = QualityRankConfig.from_json(raw)
        self.assertEqual(restored, cfg.quality_ranks)

    def test_custom_cfg_serializes_to_argv(self):
        """Custom policy and codec bands survive the argv round-trip."""
        from lib.config import CratediggerConfig
        from lib.quality import CodecRankBands, QualityRankConfig, RankBitrateMetric
        vorbis = CodecRankBands(
            transparent=201, excellent=161, good=113, acceptable=97)
        wma = CodecRankBands(
            transparent=321, excellent=257, good=193, acceptable=129)
        custom_ranks = QualityRankConfig(
            bitrate_metric=RankBitrateMetric.MIN,
            within_rank_tolerance_kbps=15,
            vorbis=vorbis,
            wma=wma,
        )
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS, quality_ranks=custom_ranks)
        cmd = self._run_dispatch_capture_cmd(cfg)
        raw = self._extract_rank_config_json(cmd)
        self.assertIsNotNone(raw)
        assert raw is not None  # for pyright
        restored = QualityRankConfig.from_json(raw)
        self.assertEqual(restored.bitrate_metric, RankBitrateMetric.MIN)
        self.assertEqual(restored.within_rank_tolerance_kbps, 15)
        self.assertEqual(restored.vorbis, vorbis)
        self.assertEqual(restored.wma, wma)

    def test_missing_cfg_omits_argv(self):
        """When cfg=None, the --quality-rank-config argv is not emitted.

        Harness falls back to QualityRankConfig.defaults() in that case.
        """
        cmd = self._run_dispatch_capture_cmd(None)
        self.assertNotIn("--quality-rank-config", cmd)

    def test_existing_v0_probe_state_serializes_to_argv(self):
        from lib.dispatch.types import EvidenceImportGate

        current = make_album_quality_evidence(
            mb_release_id="test-mbid",
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="carried",
                min_bitrate_kbps=128,
                avg_bitrate_kbps=171,
                median_bitrate_kbps=169,
            ),
        )

        # Keep this seam test focused on core-to-subprocess argv propagation;
        # loader freshness and FK behavior have their own action-evidence tests.
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "mp3"},
        ))
        claimed, candidate_result, execution_lease = _claim_dispatch_job(
            db,
            path="/tmp/dest",
            release_id="test-mbid",
            evidence_kwargs={
                "measurement": AudioQualityMeasurement(
                    min_bitrate_kbps=1000,
                    avg_bitrate_kbps=1000,
                    median_bitrate_kbps=1000,
                    format="FLAC",
                    spectral_grade="genuine",
                    spectral_bitrate_kbps=None,
                    spectral_subject="source",
                    spectral_provenance="measured",
                ),
                "codec": "flac",
                "container": "flac",
                "storage_format": "FLAC",
                # Production-shaped: the preview grinds a source V0 probe
                # into every lossless-source evidence row before the
                # importer decides (issue #990 — a probe-less candidate
                # cannot displace an anchored copy and would reject
                # before the subprocess this seam test exists to observe).
                "v0_metric": AlbumQualityV0Metric(
                    subject="source",
                    min_bitrate_kbps=238,
                    avg_bitrate_kbps=250,
                ),
            },
        )
        assert candidate_result.evidence is not None
        cancellation_token = CancellationToken()
        with patch_dispatch_externals() as ext, patch(
            "lib.dispatch.subprocess_runner.parse_import_result",
            return_value=make_import_result(decision="import"),
        ), pinned_dispatch_authority(
            db,
            execution_lease,
            cancellation_token=cancellation_token,
        ) as (cancellation_token, owner_session_identity):
            dispatch_import_core(
                make_dispatch_request(
                    path='/tmp/dest',
                    mb_release_id='test-mbid',
                    request_id=42,
                    label='Test Artist - Test Album',
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype='mp3'),
                    files=[make_download_file(username='user1', filename='01.mp3')],
                    candidate_import_job_id=claimed.id,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                ),
                db,
                cfg=_full_dispatch_config(),
                quality_gate_fn=noop_quality_gate,
                evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(candidate=candidate_result.evidence, current=current),
                cancellation_token=cancellation_token,
                run_import_fn=_owned_test_runner,
            )
            cmd = ext.run.call_args[0][0]

        self.assertIn("--existing-v0-probe-min-bitrate", cmd)
        self.assertEqual(
            cmd[cmd.index("--existing-v0-probe-min-bitrate") + 1], "128")
        self.assertIn("--existing-v0-probe-avg-bitrate", cmd)
        self.assertEqual(
            cmd[cmd.index("--existing-v0-probe-avg-bitrate") + 1], "171")
        self.assertIn("--existing-v0-probe-median-bitrate", cmd)
        self.assertEqual(
            cmd[cmd.index("--existing-v0-probe-median-bitrate") + 1], "169")

    def test_audio_corrupt_evidence_persists_exact_decoder_diagnostic(self):
        from lib.dispatch import dispatch_import_core
        from lib.dispatch.types import EvidenceImportGate

        decode_error = (
            "5/8 files failed: 01.flac: Invalid data found when processing "
            "input; 02.flac: End of file"
        )
        candidate = make_album_quality_evidence(
            mb_release_id="test-mbid",
            storage_format="FLAC",
            codec="flac",
            container="flac",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=900,
                format="FLAC",
                is_cbr=False,
            ),
            audio_corrupt=True,
            audio_error=decode_error,
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            mb_release_id="test-mbid",
            active_download_state={"files": [], "filetype": "flac"},
        ))

        with patch_dispatch_externals() as ext:
            dispatch_import_core(
                make_dispatch_request(
                    path='/tmp/badlands',
                    mb_release_id='test-mbid',
                    request_id=42,
                    label='Dirty Beaches - Badlands',
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype='flac', username='peer'),
                    files=[make_download_file(username='peer', filename='01.flac')],
                ),
                db,
                cfg=_full_dispatch_config(),
                quality_gate_fn=noop_quality_gate,
                evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(candidate=candidate),
            )

        ext.run.assert_not_called()
        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(db.download_logs[0].outcome, "rejected")
        self.assertEqual(db.download_logs[0].beets_detail, decode_error)
        self.assertEqual(db.download_logs[0].error_message, decode_error)


class TestLoadQualityGateState(unittest.TestCase):
    """Direct tests for the shared quality-gate state adapter."""

    @staticmethod
    def _load_state(db, mb_id: str):
        """The one seam where a FakePipelineDB crosses the PipelineDB type.

        A single shared helper, so this class carries exactly one crossing
        however many scenarios it grows — and the crossing needs no escape
        hatch at all: the parameter is deliberately unannotated here, which
        is what the per-call-site ``# type: ignore[arg-type]`` was
        compensating for.
        """
        from lib.dispatch import load_quality_gate_state

        return load_quality_gate_state(
            request_id=42,
            db=db,
            mb_id=mb_id,
        )

    def test_uses_linked_measurement_and_ignores_request_quality_stamps(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id="mbid-123",
            verified_lossless=True,
            final_format="mp3 v0",
            current_spectral_grade="genuine",
            current_spectral_bitrate=96,
        ))
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-123",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=207,
                avg_bitrate_kbps=207,
                median_bitrate_kbps=207,
                format="MP3",
                is_cbr=False,
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        with patch.object(
            db,
            "get_request",
            side_effect=AssertionError("explicit MBID must avoid request lookup"),
        ) as get_request:
            state = self._load_state(db, mb_id="mbid-123")

        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.measurement.min_bitrate_kbps, 207)
        self.assertEqual(state.measurement.format, "MP3")
        self.assertEqual(state.measurement.avg_bitrate_kbps, 207)
        self.assertFalse(state.measurement.is_cbr)
        self.assertFalse(state.verified_lossless_proof)
        self.assertIsNone(state.measurement.spectral_bitrate_kbps)
        get_request.assert_not_called()

    def _state_for(self, *, measurement, storage_format, filetype_band):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id="mbid-ctx",
        ))
        evidence = msgspec.structs.replace(
            make_album_quality_evidence(
                mb_release_id="mbid-ctx", measurement=measurement,
            ),
            storage_format=storage_format,
            filetype_band=filetype_band,
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)
        return self._load_state(db, mb_id="mbid-ctx")

    def test_state_carries_the_album_level_codec_context(self):
        """Issue #829 Phase 5 PR2b review S6.

        Only the evidence ROW carries ``storage_format`` and
        ``filetype_band``. The gate resolves the codec with them, so the
        state must carry that context — every downstream consumer (the
        ``pipeline-cli quality`` simulator) has to resolve with the same
        evidence, not a weaker measurement-only view.
        """
        state = self._state_for(
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320, avg_bitrate_kbps=320, format="MP3",
                is_cbr=True, spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128, spectral_subject="installed",
                spectral_provenance="measured", codec_family="mp3",
            ),
            storage_format="mp3",
            filetype_band="mp3",
        )
        assert state is not None
        assert state.spectral_context is not None
        self.assertEqual(state.spectral_context.storage_format, "mp3")
        self.assertEqual(state.spectral_context.filetype_band, "mp3")
        # ...and it really resolves the class the gate used.
        interp = state.spectral_context.interpret(state.measurement)
        self.assertTrue(interp.decision_grade)
        self.assertEqual(interp.inferred_class_kbps, 128)
        self.assertEqual(state.measurement.spectral_bitrate_kbps, 128)

    def test_a_mixed_codec_album_fails_closed_through_the_carried_context(self):
        """The context is not decoration — it is the fail-closed evidence.

        ``filetype_band`` spanning codec families is the only signal that an
        album's spectral grade was averaged ACROSS codecs (``codec_family``
        capture is the FIRST track's). Without the carried context a
        consumer resolves ``format='MP3'`` and admits a class the gate
        itself withheld.
        """
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=320, avg_bitrate_kbps=320, format="MP3",
            is_cbr=True, spectral_grade="likely_transcode",
            spectral_bitrate_kbps=128, spectral_subject="installed",
            spectral_provenance="measured", codec_family="mp3",
        )
        state = self._state_for(
            measurement=measurement,
            storage_format="mp3",
            filetype_band="mixed_lossy",
        )
        assert state is not None
        assert state.spectral_context is not None
        self.assertFalse(
            state.spectral_context.interpret(state.measurement).decision_grade)
        # The gate itself withheld too: no clamped value on the measurement.
        self.assertIsNone(state.measurement.spectral_bitrate_kbps)
        # And the measurement-only view — what a consumer that dropped the
        # context would compute — WOULD admit the class. That gap is the
        # whole reason the context is carried.
        from lib.quality import interpret_measurement
        self.assertTrue(interpret_measurement(measurement).decision_grade)


class TestQualityGateUsesIntent(unittest.TestCase):
    """Orchestration tests for _check_quality_gate_core via FakePipelineDB.

    Each scenario builds linked evidence from an ``AlbumInfo``-shaped fixture
    whose measurement produces the desired ``quality_gate_decision`` branch
    when classified by the real (un-stubbed) decision function. See
    ``tests/test_quality_decisions.py::TestQualityGateDecision.CASES`` for
    the canonical input → decision table — these tests pick inputs from the
    same table so the orchestration test exercises the same code path the
    decision unit tests pin.
    """

    def _run_quality_gate(
        self,
        *,
        info,
        verified_lossless_proof: bool = False,
        linked_spectral_grade: str | None = None,
        linked_spectral_bitrate: int | None = None,
        linked_spectral_subject: EvidenceSubject | None = None,
        linked_spectral_provenance: EvidenceProvenance | None = None,
        **extra_req_fields,
    ):
        """Drive ``_check_quality_gate_core`` with a real ``AlbumInfo`` and the
        real ``quality_gate_decision`` (no patch on the pure decision)."""
        from lib.dispatch import _check_quality_gate_core
        db = FakePipelineDB()
        merged = {"status": "imported", "current_spectral_bitrate": None,
                  "current_spectral_grade": None,
                  "verified_lossless": False}
        merged.update(extra_req_fields)
        db.seed_request(make_request_row(id=42, **merged))
        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=info.min_bitrate_kbps,
                avg_bitrate_kbps=info.avg_bitrate_kbps,
                median_bitrate_kbps=info.median_bitrate_kbps,
                format=info.format,
                is_cbr=info.is_cbr,
                spectral_grade=linked_spectral_grade,
                spectral_bitrate_kbps=linked_spectral_bitrate,
                spectral_subject=(
                    linked_spectral_subject
                    if linked_spectral_grade is not None
                    else None
                ) or (
                    "installed" if linked_spectral_grade is not None else None
                ),
                spectral_provenance=(
                    linked_spectral_provenance
                    if linked_spectral_grade is not None
                    else None
                ) or (
                    "measured" if linked_spectral_grade is not None else None
                ),
            ),
            verified_lossless_proof=(
                VerifiedLosslessProof(
                    provenance="carried",
                    source="flac",
                    classifier="spectral_verified_lossless",
                ) if verified_lossless_proof else None
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        _check_quality_gate_core(
            mb_id="test-mbid", label="Test Artist - Test Album",
            request_id=42,
            files=[make_download_file(username="user1", filename="01.mp3")],
            db=db,
        )

        return db

    @staticmethod
    def _bare_mp3_vbr_low():
        """MP3 VBR at 150 kbps → ACCEPTABLE < EXCELLENT → requeue_upgrade.

        Matches the pinned "bare MP3 VBR below rank" case in
        TestQualityGateDecision.CASES.
        """
        from lib.beets_db import AlbumInfo
        return AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=150, avg_bitrate_kbps=150,
            format="MP3", is_cbr=False,
            album_path="/Beets/Artist/Album",
        )

    @staticmethod
    def _cbr_320_unverified():
        """CBR 320 unverified → TRANSPARENT but CBR + !verified → requeue_lossless.

        Matches the pinned "bare MP3 CBR 320 unverified" case.
        """
        from lib.beets_db import AlbumInfo
        return AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=320, avg_bitrate_kbps=320,
            format="MP3", is_cbr=True,
            album_path="/Beets/Artist/Album",
        )

    def test_no_mb_id_returns_early(self):
        """Empty mb_id should return without doing anything."""
        from lib.dispatch import _check_quality_gate_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="imported"))
        _check_quality_gate_core(
            mb_id="", label="Test", request_id=42, files=[],
            db=db)
        # Status unchanged — gate returned early
        self.assertEqual(db.request(42)["status"], "imported")

    def test_missing_linked_evidence_reopens_full_tier_search(self):
        """An unverified import cannot become terminal by losing its FK."""
        from lib.dispatch import _check_quality_gate_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="imported",
            min_bitrate=245,
            search_filetype_override="lossless",
            verified_lossless=True,
        ))

        plan = _check_quality_gate_core(
            mb_id="test-mbid",
            label="Missing Evidence",
            request_id=42,
            files=[make_download_file(username="winner", filename="01.mp3")],
            db=db,
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertFalse(plan.successful_terminal_acceptance)
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["min_bitrate"], 245)
        self.assertIsNone(row["search_filetype_override"])
        # Decision 18: a local bookkeeping failure is never attributed to
        # the winning peer — the request reopens, the peer stays available.
        self.assertEqual(db.denylist, [])

    def test_linked_evidence_load_error_reopens_full_tier_search(self):
        """Adapter errors follow the same explicit retry path as absence."""
        from lib.dispatch import _check_quality_gate_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="imported"))

        def unavailable_state(**_kwargs):
            raise RuntimeError("evidence store unavailable")

        plan = _check_quality_gate_core(
            mb_id="test-mbid",
            label="Failed Evidence",
            request_id=42,
            files=[make_download_file(username="winner", filename="01.mp3")],
            db=db,
            state_loader=unavailable_state,
        )

        self.assertIsNotNone(plan)
        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertIsNone(db.request(42)["search_filetype_override"])
        # Decision 18: adapter errors reopen without blaming the peer.
        self.assertEqual(db.denylist, [])

    def test_quality_decision_error_reopens_even_with_terminal_proof(self):
        """A decider crash cannot turn proof into a terminal acceptance."""
        from lib.dispatch import _check_quality_gate_core
        from lib.dispatch.types import QualityGateState

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="imported",
            min_bitrate=777,
            search_filetype_override="lossless",
        ))
        state = QualityGateState(
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=900,
                format="FLAC",
            ),
            verified_lossless_proof=True,
        )

        def exploding_decision(
            current: AudioQualityMeasurement,
            cfg: QualityRankConfig | None = None,
            *,
            target_contract: TargetQualityContract | None = None,
            verified_lossless_proof: bool = False,
        ) -> Never:
            self.assertIs(current, state.measurement)
            self.assertIsNotNone(cfg)
            self.assertIsNone(target_contract)
            self.assertTrue(verified_lossless_proof)
            raise RuntimeError("decision engine unavailable")

        plan = _check_quality_gate_core(
            mb_id="test-mbid",
            label="Decision Failure",
            request_id=42,
            files=[make_download_file(username="winner", filename="01.flac")],
            db=db,
            state_loader=lambda **_kwargs: state,
            quality_decision_fn=exploding_decision,
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.transition.target_status, "wanted")
        self.assertIsNone(
            plan.transition.fields.get("search_filetype_override")
        )
        self.assertEqual(plan.denylists, ())
        self.assertFalse(plan.successful_terminal_acceptance)
        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertEqual(db.request(42)["min_bitrate"], 777)
        self.assertIsNone(db.request(42)["search_filetype_override"])
        self.assertEqual(db.denylist, [])

    def test_requeue_upgrade_uses_intent(self):
        db = self._run_quality_gate(info=self._bare_mp3_vbr_low())
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["search_filetype_override"])

    def test_requeue_upgrade_cannot_widen_existing_lossless_scope(self):
        db = self._run_quality_gate(
            info=self._bare_mp3_vbr_low(),
            search_filetype_override="lossless",
        )

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "lossless")

    def test_verified_lossless_proof_accepts_regardless_of_rank(self):
        db = self._run_quality_gate(
            info=self._bare_mp3_vbr_low(), verified_lossless_proof=True)
        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["search_filetype_override"])

    def test_verified_lossless_plan_marks_terminal_acceptance(self):
        from lib.dispatch import _check_quality_gate_core
        from lib.dispatch.types import QualityGateState

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="imported"))
        state = QualityGateState(
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=150,
                avg_bitrate_kbps=150,
                median_bitrate_kbps=150,
                format="MP3",
                is_cbr=False,
            ),
            verified_lossless_proof=True,
        )

        plan = _check_quality_gate_core(
            mb_id="test-mbid",
            label="Terminal Acceptance",
            request_id=42,
            files=[],
            db=db,
            apply=False,
            state_loader=lambda **_kwargs: state,
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertTrue(plan.successful_terminal_acceptance)

    def test_verified_lossless_proof_does_not_denylist(self):
        db = self._run_quality_gate(
            info=self._bare_mp3_vbr_low(),
            verified_lossless_proof=True,
            current_spectral_grade=None,
        )
        self.assertEqual(db.denylist, [])

    def test_full_tier_denylist_reason_names_missing_proof(self):
        """The persisted reason explains policy, not a retired rank floor."""
        from lib.beets_db import AlbumInfo

        db = self._run_quality_gate(info=AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=150, avg_bitrate_kbps=150,
            format="MP3", is_cbr=False,
            album_path="/Beets/Artist/Album",
        ))

        self.assertEqual(len(db.denylist), 1)
        reason = db.denylist[0].reason or ""
        self.assertIn("no verified-lossless proof", reason)
        self.assertIn("full-tier search", reason)
        self.assertNotIn("ACCEPTABLE", reason)
        self.assertNotIn("EXCELLENT", reason)

    def test_transparent_genuine_copy_narrows_to_lossless(self):
        db = self._run_quality_gate(
            info=self._cbr_320_unverified(),
            linked_spectral_grade="genuine",
        )
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_FLAC_ONLY)
        self.assertEqual(len(db.denylist), 1)

    def test_transparent_carried_source_grade_also_narrows(self):
        # Decision 17: narrowing keys on the genuine grade, never the
        # subject label — the carried source grade narrows identically.
        db = self._run_quality_gate(
            info=self._cbr_320_unverified(),
            linked_spectral_grade="genuine",
            linked_spectral_subject="source",
            linked_spectral_provenance="carried",
        )
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_FLAC_ONLY)

    def test_quality_gate_ignores_request_spectral_stamps(self):
        """Mutating request-row quality stamps cannot change linked policy."""
        from lib.beets_db import AlbumInfo

        db = self._run_quality_gate(
            info=AlbumInfo(
                album_id=1, track_count=10,
                min_bitrate_kbps=226, avg_bitrate_kbps=226,
                format="MP3", is_cbr=False,
                album_path="/Beets/Artist/Album",
            ),
            last_download_spectral_bitrate=180,
            last_download_spectral_grade="likely_transcode",
            current_spectral_bitrate=96,
            current_spectral_grade="likely_transcode",
            final_format="mp3 64",
        )

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertIsNone(db.request(42)["search_filetype_override"])

    def test_genuine_v0_replacing_transcode_accepted(self):
        """Genuine V0 replacing a transcode should be accepted, not requeued."""
        from lib.beets_db import AlbumInfo
        db = self._run_quality_gate(
            info=AlbumInfo(
                album_id=1, track_count=10,
                min_bitrate_kbps=226, avg_bitrate_kbps=226,
                format="MP3", is_cbr=False,
                album_path="/Beets/Artist/Album",
            ),
            linked_spectral_grade="genuine",
        )

        # Genuine evidence below TRANSPARENT is retained on full tiers.
        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertIsNone(db.request(42)["search_filetype_override"])

    def test_quality_gate_uses_likely_transcode_spectral(self):
        """likely_transcode album grade must feed into the gate, not just suspect.

        Regression for issue #61: _check_quality_gate_core previously only
        accepted "suspect", silently ignoring the album-level "likely_transcode"
        grade produced by classify_album when >=60% of tracks are suspect.

        Observable proof: with spectral=180 and grade="likely_transcode",
        the spectral clamp pulls the MP3 VBR 226 rank from EXCELLENT down to
        GOOD, which is < EXCELLENT (gate_min) → requeue_upgrade. Without
        the clamp the status would stay ``imported``.
        """
        from lib.beets_db import AlbumInfo
        db = self._run_quality_gate(
            info=AlbumInfo(
                album_id=1, track_count=10,
                min_bitrate_kbps=226, avg_bitrate_kbps=226,
                format="MP3", is_cbr=False,
                album_path="/Beets/Artist/Album",
            ),
            linked_spectral_grade="likely_transcode",
            linked_spectral_bitrate=180,
        )

        self.assertEqual(
            db.request(42)["status"], "wanted",
            "likely_transcode spectral=180 must clamp the gate rank below "
            "EXCELLENT and trigger requeue_upgrade")

    def test_quality_gate_ignores_genuine_low_spectral(self):
        """Genuine grade with low spectral estimate must NOT lower the gate bitrate.

        Guards the original #31 fix: a lo-fi genuine V0 (e.g. ~160kbps cliff
        estimate) must not trigger a requeue loop when beets reports 226kbps.
        Observable: ``compute_effective_override_bitrate`` returns the
        container bitrate for non-transcode grades, so the gate sees a
        clean EXCELLENT rank and the request stays imported.
        """
        from lib.beets_db import AlbumInfo
        db = self._run_quality_gate(
            info=AlbumInfo(
                album_id=1, track_count=10,
                min_bitrate_kbps=226, avg_bitrate_kbps=226,
                format="MP3", is_cbr=False,
                album_path="/Beets/Artist/Album",
            ),
            linked_spectral_grade="genuine",
            linked_spectral_bitrate=160,
        )

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertIsNone(db.request(42)["search_filetype_override"])

    def test_dispatch_requeue_uses_intent(self):
        """Transcode-upgrade requeue path uses quality constants."""
        from lib.dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "mp3"},
        ))
        ir = make_import_result(decision="transcode_upgrade",
                                new_min_bitrate=227)
        claimed, candidate, execution_lease = _claim_dispatch_job(
            db,
            path="/tmp/dest",
            release_id="test-mbid",
        )

        cancellation_token = CancellationToken()
        with patch_dispatch_externals(), \
             patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
             pinned_dispatch_authority(
                 db,
                 execution_lease,
                 cancellation_token=cancellation_token,
             ) as (cancellation_token, owner_session_identity):
            outcome = dispatch_import_core(
                make_dispatch_request(
                    path='/tmp/dest',
                    mb_release_id='test-mbid',
                    request_id=42,
                    label='Test',
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype='mp3'),
                    files=[make_download_file(username='user1', filename='01.mp3')],
                    candidate_import_job_id=claimed.id,
                    prevalidated_candidate_result=candidate,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                ),
                db,
                quality_gate_fn=noop_quality_gate,
                cancellation_token=cancellation_token,
                run_import_fn=_owned_test_runner,
            )
        assert outcome.terminal_outcome is not None
        from tests.dispatch_helpers import finalize_claimed_dispatch

        finalize_claimed_dispatch(db, claimed, outcome)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["search_filetype_override"])


class TestQualityGatePreservesTargetFormat(unittest.TestCase):
    """Quality gate accept must clear search_filetype_override but preserve target_format."""

    def _run_quality_gate_accept(self, target_format="flac"):
        """Drive a real accept via FLAC verified-lossless input — no decision stub."""
        from lib.beets_db import AlbumInfo
        from lib.dispatch import _check_quality_gate_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            target_format=target_format,
            verified_lossless=True,
            current_spectral_bitrate=None,
            search_filetype_override="lossless",  # should be cleared
        ))
        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="carried",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="carried",
                source="flac",
                classifier="spectral_verified_lossless",
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        # FLAC → measurement_rank returns LOSSLESS regardless of bitrate, so
        # the real quality_gate_decision accepts.
        info = AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=900, avg_bitrate_kbps=900,
            format="FLAC", is_cbr=False,
            album_path="/Beets/Artist/Album",
        )
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = info
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42, files=[],
                db=db)

        return db

    def test_accept_clears_search_override_not_target_format(self):
        db = self._run_quality_gate_accept(target_format="flac")
        row = db.request(42)
        self.assertIsNone(row["search_filetype_override"])
        self.assertEqual(row["target_format"], "flac")
        self.assertEqual(row["status"], "imported")


class TestOpusConversionDispatch(unittest.TestCase):
    """Seam tests — --verified-lossless-target flag wiring.

    Exercised through the surviving auto-import seam in lib.download.
    """

    def _get_cmd(self, verified_lossless_target=""):
        album_data = _make_album_data()
        ctx = _make_ctx(verified_lossless_target=verified_lossless_target)
        ir = make_import_result(decision="import", was_converted=True,
                                original_filetype="flac", target_filetype="mp3")
        return _dispatch_valid_result_cmd(album_data=album_data, ctx=ctx, ir=ir)

    def test_target_flag_passed_when_set(self):
        cmd = self._get_cmd(verified_lossless_target="opus 128")
        self.assertIn("--verified-lossless-target", cmd)
        idx = cmd.index("--verified-lossless-target")
        self.assertEqual(cmd[idx + 1], "opus 128")

    def test_target_flag_not_passed_when_empty(self):
        cmd = self._get_cmd(verified_lossless_target="")
        self.assertNotIn("--verified-lossless-target", cmd)

    def test_opus_import_result_populates_dl_info(self):
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        ir = ImportResult(
            decision="import",
            final_format="opus 128",
            v0_verification_bitrate=247,
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                was_converted_from="flac"),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured", source="flac", classifier="spectral"
            ),
            conversion=ConversionInfo(
                was_converted=True, original_filetype="flac",
                target_filetype="opus", final_format="opus 128"),
        )
        _populate_dl_info_from_import_result(dl, ir)
        self.assertEqual(dl.actual_filetype, "opus")
        self.assertEqual(dl.slskd_filetype, "flac")
        self.assertTrue(dl.is_vbr)
        self.assertEqual(dl.bitrate, 128000)
        self.assertEqual(dl.final_format, "opus 128")


class TestTargetFormatDispatch(unittest.TestCase):
    """Seam tests — --target-format flag wiring.

    Exercised through the surviving auto-import seam in lib.download.
    """

    def _get_cmd(self, target_format=None):
        album_data = _make_album_data()
        album_data.db_target_format = target_format
        ctx = _make_ctx()
        ir = make_import_result(decision="import")
        return _dispatch_valid_result_cmd(album_data=album_data, ctx=ctx, ir=ir)

    def test_target_format_passed_when_set(self):
        cmd = self._get_cmd(target_format="flac")
        self.assertIn("--target-format", cmd)
        idx = cmd.index("--target-format")
        self.assertEqual(cmd[idx + 1], "flac")

    def test_target_format_not_passed_when_none(self):
        cmd = self._get_cmd(target_format=None)
        self.assertNotIn("--target-format", cmd)


class TestDispatchJellyfinPinCaptureSlice(unittest.TestCase):
    """End-to-end slice for the path-changing-upgrade pin capture: dispatch
    threads ``postflight.replaced_albums`` into the REAL
    ``capture_jellyfin_date_created_pin`` → REAL ``jellyfin_find_album_by_path``
    (old-path fallback), with only the Jellyfin HTTP leaf
    (``lib.util._jellyfin_get_json``) faked."""

    NEW_REL = "Test Artist/0000 - Test Album"
    OLD_CONTAINER = "/jf/Test Artist/2007 - Test Album"
    ORIGINAL = "2026-04-01T00:00:00Z"

    def _fake_get_json(self, cfg, path, **params):
        if path == "/Items" and params.get("includeItemTypes") == "MusicAlbum":
            return {"Items": [{
                "Id": "alb-old",
                "Path": self.OLD_CONTAINER,
                "DateCreated": self.ORIGINAL,
                "Name": "Test Album",
                "AlbumArtist": "Test Artist",
            }]}
        if path == "/Items" and params.get("includeItemTypes") == "MusicArtist":
            return {"Items": []}
        if path == "/Items" and "parentId" in params:
            return {"Items": [
                {"Id": "tr-old-1", "DateCreated": self.ORIGINAL},
            ]}
        return {"Items": []}

    def test_replaced_album_old_path_reaches_capture_and_pins(self):
        from lib.dispatch import dispatch_import_core

        assert _HERMETIC_BEETS_PAIR is not None
        beets_library_db, beets_library_root = _HERMETIC_BEETS_PAIR
        old_album_path = f"{beets_library_root}/Test Artist/2007 - Test Album"
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            active_download_state={"files": [], "filetype": "mp3"}))
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            beets_library_db=beets_library_db,
            beets_directory=beets_library_root,
            jellyfin_url="http://jf:8096",
            jellyfin_token="tok",
            jellyfin_path_map=f"{beets_library_root}:/jf",
        )
        ir = make_import_result(
            decision="import", imported_path=self.NEW_REL)
        ir.postflight.replaced_albums = [DuplicateRemoveCandidate(
            beets_album_id=3902,
            mb_albumid="test-mbid",
            album_path=old_album_path,
            item_count=19,
        )]

        tmpdir = tempfile.mkdtemp()
        try:
            claimed, candidate, execution_lease = _claim_dispatch_job(
                db,
                path=tmpdir,
                release_id="test-mbid",
            )
            cancellation_token = CancellationToken()
            with patch_dispatch_externals(), \
                 patch("lib.dispatch.subprocess_runner.parse_import_result",
                       return_value=ir), \
                 patch("lib.util._jellyfin_get_json",
                       side_effect=self._fake_get_json), \
                 pinned_dispatch_authority(
                     db,
                     execution_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                dispatch_import_core(
                    make_dispatch_request(
                        path=tmpdir,
                        mb_release_id='test-mbid',
                        request_id=42,
                        label='Test Artist - Test Album',
                        beets_harness_path=_HARNESS,
                        dl_info=DownloadInfo(filetype='mp3'),
                        distance=0.05,
                        scenario='strong_match',
                        files=[make_download_file(username='user1', filename='01 - Track.mp3')],
                        candidate_import_job_id=claimed.id,
                        prevalidated_candidate_result=candidate,
                        beets_library_db_path=beets_library_db,
                        beets_library_root=beets_library_root,
                        execution_lease=execution_lease,
                        owner_session_identity=owner_session_identity,
                    ),
                    db,
                    cfg=cfg,
                    quality_gate_fn=noop_quality_gate,
                    cancellation_token=cancellation_token,
                    run_import_fn=_owned_test_runner,
                    # This test is about the pin capture, not the vanished-
                    # path reconciler (issue #1203 item 2's own coverage is
                    # tests.test_import_dispatch.TestVanishedPathReconciliation).
                    # replaced_albums's old path differs from imported_path
                    # here, which would otherwise reach a REAL
                    # notify_library_delete against this test's real
                    # jellyfin_url — stub it out directly via the kwarg-DI
                    # seam rather than a module patch.
                    media_server_notify_fn=MagicMock(return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES),
                )
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        self.assertEqual(len(db.jellyfin_date_created_pins), 1)
        pin = db.jellyfin_date_created_pins[0]
        # The pre-upgrade item was found at the replaced album's OLD path;
        # the pin joins on the NEW path for the reconciler.
        self.assertEqual(pin["imported_path"], self.NEW_REL)
        self.assertEqual(pin["album_item_id"], "alb-old")
        self.assertEqual(pin["children_item_ids"], ["tr-old-1"])
        self.assertEqual(pin["original_date_created"], self.ORIGINAL)
        self.assertEqual(pin["request_id"], 42)

    def test_snapshot_reaches_capture_when_replaced_albums_never_recorded(
        self,
    ) -> None:
        """Issue #1203 item 2 review finding 3 (round 2), corrected round 3:
        ``pre_import_album_directories`` (the PRIMARY, authoritative
        source) must reach the pin capture too, not only
        ``postflight.replaced_albums`` (the secondary source) — because
        ``replaced_albums`` structurally only ever reports an album the
        import's dup-guard answered "remove" for; it cannot report a
        directory that left the library for any other reason (see
        ``TestVanishedPathReconciliation``'s own regression pin for the
        live corpus evidence). With ``replaced_albums`` empty, the
        snapshot source is the only thing that can find the pre-upgrade
        Jellyfin item; without this union the capture would degrade to a
        floor pin (``album_item_id`` NULL) instead of the true historical
        ``DateCreated``.

        (An earlier version of this test instead constructed a
        ``replaced_albums`` row that already showed the NEW path, calling
        it "stale" — a shape the real harness cannot produce. That claim
        is retracted; see the regression pin above for why.)"""
        from lib.dispatch import dispatch_import_core
        from lib.dispatch.types import EvidenceImportGate

        assert _HERMETIC_BEETS_PAIR is not None
        beets_library_db, beets_library_root = _HERMETIC_BEETS_PAIR
        release_id = "pin-union-stale-mbid"
        old_dir = os.path.join(
            beets_library_root, "Test Artist", "2007 - Test Album")
        new_rel = "Test Artist/0000 - Test Album"
        new_dir = os.path.join(beets_library_root, new_rel)
        album_id = _seed_beets_album(
            beets_library_db, beets_library_root,
            mb_release_id=release_id, album_dir=old_dir)
        self.addCleanup(_delete_beets_album, beets_library_db, album_id)

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            active_download_state={"files": [], "filetype": "mp3"}))
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            beets_library_db=beets_library_db,
            beets_directory=beets_library_root,
            jellyfin_url="http://jf:8096",
            jellyfin_token="tok",
            jellyfin_path_map=f"{beets_library_root}:/jf",
        )
        # replaced_albums stays at its default `[]` -- this rename left no
        # dup-guard-remove trace at all; the snapshot source alone must
        # still find the old item.
        ir = make_import_result(decision="import", imported_path=new_rel)

        def _fake_get_json(cfg, path, **params):
            if path == "/Items" and params.get("includeItemTypes") == "MusicAlbum":
                return {"Items": [{
                    "Id": "alb-old", "Path": self.OLD_CONTAINER,
                    "DateCreated": self.ORIGINAL, "Name": "Test Album",
                    "AlbumArtist": "Test Artist",
                }]}
            if path == "/Items" and params.get("includeItemTypes") == "MusicArtist":
                return {"Items": []}
            if path == "/Items" and "parentId" in params:
                return {"Items": [
                    {"Id": "tr-old-1", "DateCreated": self.ORIGINAL}]}
            return {"Items": []}

        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"fixture audio")
            claimed, candidate, execution_lease = _claim_dispatch_job(
                db, path=tmpdir, release_id=release_id)
            cancellation_token = CancellationToken()
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result",
                       return_value=ir), \
                 patch("lib.util._jellyfin_get_json",
                       side_effect=_fake_get_json), \
                 pinned_dispatch_authority(
                     db, execution_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                def _move(*_a, **_k):
                    _move_beets_album_item(beets_library_db, album_id, new_dir)
                    return MagicMock(returncode=0, stdout="", stderr="")
                ext.run.side_effect = _move
                dispatch_import_core(
                    make_dispatch_request(
                        path=tmpdir,
                        mb_release_id=release_id,
                        request_id=42,
                        label='Test Artist - Test Album',
                        beets_harness_path=_HARNESS,
                        dl_info=DownloadInfo(filetype='mp3'),
                        distance=0.05,
                        scenario='strong_match',
                        files=[make_download_file(username='user1', filename='01 - Track.mp3')],
                        candidate_import_job_id=claimed.id,
                        prevalidated_candidate_result=candidate,
                        beets_library_db_path=beets_library_db,
                        beets_library_root=beets_library_root,
                        execution_lease=execution_lease,
                        owner_session_identity=owner_session_identity,
                    ),
                    db,
                    cfg=cfg,
                    quality_gate_fn=noop_quality_gate,
                    # A real Beets row now exists for this release, so the
                    # REAL evidence gate would try to spectrally measure it
                    # (not real audio) -- bypass, matching
                    # TestVanishedPathReconciliation's own pattern. This
                    # test is about the pin capture, not evidence gating.
                    evidence_gate_fn=lambda *_a, **_kw: EvidenceImportGate(candidate=candidate.evidence),
                    cancellation_token=cancellation_token,
                    run_import_fn=_owned_test_runner,
                    # This test is about the pin capture, not the vanished-
                    # path reconciler (issue #1203 item 2's own coverage is
                    # tests.test_import_dispatch.TestVanishedPathReconciliation).
                    # replaced_albums's old path differs from imported_path
                    # here, which would otherwise reach a REAL
                    # notify_library_delete against this test's real
                    # jellyfin_url — stub it out directly via the kwarg-DI
                    # seam rather than a module patch.
                    media_server_notify_fn=MagicMock(return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES),
                )
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        self.assertEqual(len(db.jellyfin_date_created_pins), 1)
        pin = db.jellyfin_date_created_pins[0]
        self.assertEqual(pin["album_item_id"], "alb-old")
        self.assertEqual(pin["children_item_ids"], ["tr-old-1"])
        self.assertEqual(pin["original_date_created"], self.ORIGINAL)

    def test_pin_capture_does_not_leak_a_surviving_siblings_directory(
        self,
    ) -> None:
        """Issue #1203 item 2 review finding 4 (round 3):
        ``pre_import_album_directories`` is release-id-keyed and returns
        EVERY album Beets holds for that release, not just ones that
        vanished. A genuinely-new import whose release is ALSO held by a
        surviving sibling (the split-brain "multiple same-identity rows"
        state ``BeetsDB.get_all_album_ids_for_release`` also guards
        against) must never see that sibling's directory reach the pin
        capture — only the genuinely vanished set
        (``lib.dispatch.core._vanished_album_directories``) may. Without
        this guard, ``capture_jellyfin_date_created_pin`` would find the
        SIBLING's real Jellyfin item and wrongly pin THIS request's
        ``DateCreated`` against it, clamping the new album's date
        backwards and hiding it from Recently Added. This drives the real
        pin capture end to end, not just ``notify_fn`` — the reconciler
        (issue #1203 item 2's own primary concern) is unaffected either
        way here, since nothing vanished for it to reconcile."""
        from lib.dispatch import dispatch_import_core
        from lib.dispatch.types import EvidenceImportGate

        assert _HERMETIC_BEETS_PAIR is not None
        beets_library_db, beets_library_root = _HERMETIC_BEETS_PAIR
        release_id = "pin-sibling-mbid"
        sibling_dir = os.path.join(
            beets_library_root, "Test Artist", "Sibling Album")
        new_rel = "Test Artist/0000 - New Album"
        new_dir = os.path.join(beets_library_root, new_rel)
        # The sibling is a PRE-EXISTING album under the SAME release
        # identity — it never moves.
        sibling_id = _seed_beets_album(
            beets_library_db, beets_library_root,
            mb_release_id=release_id, album_dir=sibling_dir)
        self.addCleanup(_delete_beets_album, beets_library_db, sibling_id)

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            active_download_state={"files": [], "filetype": "mp3"}))
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            beets_library_db=beets_library_db,
            beets_directory=beets_library_root,
            jellyfin_url="http://jf:8096",
            jellyfin_token="tok",
            jellyfin_path_map=f"{beets_library_root}:/jf",
        )
        ir = make_import_result(decision="import", imported_path=new_rel)
        sibling_container = "/jf/Test Artist/Sibling Album"

        def _fake_get_json(cfg, path, **params):
            if path == "/Items" and params.get("includeItemTypes") == "MusicAlbum":
                # The sibling has a REAL Jellyfin item; the genuinely-new
                # album at new_rel does not (nothing has scanned it yet).
                return {"Items": [{
                    "Id": "alb-sibling", "Path": sibling_container,
                    "DateCreated": self.ORIGINAL, "Name": "Sibling Album",
                    "AlbumArtist": "Test Artist",
                }]}
            if path == "/Items" and params.get("includeItemTypes") == "MusicArtist":
                return {"Items": []}
            if path == "/Items" and "parentId" in params:
                return {"Items": [
                    {"Id": "tr-sibling-1", "DateCreated": self.ORIGINAL}]}
            return {"Items": []}

        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"fixture audio")
            claimed, candidate, execution_lease = _claim_dispatch_job(
                db, path=tmpdir, release_id=release_id)
            cancellation_token = CancellationToken()
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result",
                       return_value=ir), \
                 patch("lib.util._jellyfin_get_json",
                       side_effect=_fake_get_json), \
                 pinned_dispatch_authority(
                     db, execution_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                def _create_new_album(*_a, **_k):
                    new_id = _seed_beets_album(
                        beets_library_db, beets_library_root,
                        mb_release_id=release_id, album_dir=new_dir)
                    self.addCleanup(
                        _delete_beets_album, beets_library_db, new_id)
                    return MagicMock(returncode=0, stdout="", stderr="")
                ext.run.side_effect = _create_new_album
                dispatch_import_core(
                    make_dispatch_request(
                        path=tmpdir,
                        mb_release_id=release_id,
                        request_id=42,
                        label='Test Artist - New Album',
                        beets_harness_path=_HARNESS,
                        dl_info=DownloadInfo(filetype='mp3'),
                        distance=0.05,
                        scenario='strong_match',
                        files=[make_download_file(username='user1', filename='01 - Track.mp3')],
                        candidate_import_job_id=claimed.id,
                        prevalidated_candidate_result=candidate,
                        beets_library_db_path=beets_library_db,
                        beets_library_root=beets_library_root,
                        execution_lease=execution_lease,
                        owner_session_identity=owner_session_identity,
                    ),
                    db,
                    cfg=cfg,
                    quality_gate_fn=noop_quality_gate,
                    # A real Beets row now exists for this release, so the
                    # REAL evidence gate would try to spectrally measure it
                    # (not real audio) -- bypass, matching
                    # TestVanishedPathReconciliation's own pattern. This
                    # test is about the pin capture, not evidence gating.
                    evidence_gate_fn=lambda *_a, **_kw: EvidenceImportGate(candidate=candidate.evidence),
                    cancellation_token=cancellation_token,
                    run_import_fn=_owned_test_runner,
                    # This test is about the pin capture, not the vanished-
                    # path reconciler (issue #1203 item 2's own coverage is
                    # tests.test_import_dispatch.TestVanishedPathReconciliation).
                    # replaced_albums's old path differs from imported_path
                    # here, which would otherwise reach a REAL
                    # notify_library_delete against this test's real
                    # jellyfin_url — stub it out directly via the kwarg-DI
                    # seam rather than a module patch.
                    media_server_notify_fn=MagicMock(return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES),
                )
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        # The correct outcome for a genuinely-new import: nothing captured
        # at all -- the sibling's directory never vanished, so it must
        # never reach the pin capture's lookup, and the new album has no
        # Jellyfin item of its own yet (capture_jellyfin_date_created_pin's
        # own "genuinely-new album... writes nothing" contract).
        self.assertEqual(db.jellyfin_date_created_pins, [])


def _seed_beets_album(
    beets_library_db: str,
    beets_library_root: str,
    *,
    mb_release_id: str,
    album_dir: str,
) -> int:
    """Insert one minimal real-Beets album + item directly into the shared
    hermetic Beets DB (issue #1203 item 2 snapshot tests), returning the
    beets album id. Uses the real ``beets.library`` API (mirroring
    ``tests/test_beets_retag.py``'s pattern) rather than hand-rolled SQL, so
    schema drift (NOT NULL columns, defaults) can't silently diverge from
    what production Beets actually writes."""
    from beets import library as beets_library_module

    os.makedirs(album_dir, exist_ok=True)
    track_path = os.path.join(album_dir, "01 Track.mp3")
    with open(track_path, "wb") as handle:
        handle.write(b"fixture audio")
    lib = beets_library_module.Library(beets_library_db, beets_library_root)
    try:
        item = beets_library_module.Item(
            path=track_path, title="Track", artist="Artist", album="Album",
            albumartist="Artist", track=1, disc=1, year=2026,
            mb_albumid=mb_release_id,
            mb_trackid=f"{mb_release_id}-track-1",
        )
        album = lib.add_album([item])
        assert album.id is not None
        return album.id
    finally:
        lib._close()


def _move_beets_album_item(
    beets_library_db: str, album_id: int, new_dir: str,
) -> None:
    """Directly rewrite the stored item path(s) for ``album_id`` — the
    minimal simulate-a-rename primitive these tests need, since the harness
    subprocess is otherwise fully mocked by ``patch_dispatch_externals``.
    Mirrors the raw-SQL ``UPDATE`` pattern in ``tests/test_beets_retag.py``
    (real Beets performs this exact class of mutation mid-import)."""
    os.makedirs(new_dir, exist_ok=True)
    conn = sqlite3.connect(beets_library_db)
    try:
        rows = conn.execute(
            "SELECT id, path FROM items WHERE album_id = ?", (album_id,),
        ).fetchall()
        for item_id, raw_path in rows:
            old_path = (
                raw_path.decode() if isinstance(raw_path, bytes) else raw_path
            )
            new_path = os.path.join(new_dir, os.path.basename(old_path))
            conn.execute(
                "UPDATE items SET path = ? WHERE id = ?",
                (new_path.encode(), item_id),
            )
        conn.commit()
    finally:
        conn.close()


def _delete_beets_album(beets_library_db: str, album_id: int) -> None:
    """Remove a seeded album's rows. The hermetic Beets DB is shared for the
    whole test module (``setUpModule``/``tearDownModule``); each test below
    uses its own unique release id so cross-test pollution was never a real
    risk, but cleanup after seeding is cheap insurance regardless."""
    conn = sqlite3.connect(beets_library_db)
    try:
        conn.execute("DELETE FROM items WHERE album_id = ?", (album_id,))
        conn.execute("DELETE FROM albums WHERE id = ?", (album_id,))
        conn.commit()
    finally:
        conn.close()


# The REAL notify_library_delete always returns exactly one DeleteNotification
# per provider (two total) -- never (). A MagicMock(return_value=()) fake
# for media_server_notify_fn is a Rule-B violation (test-fidelity.md): it is
# strictly more permissive than the production edge it stands in for.
# (What actually let a mutant deleting the entire outcome-logging loop in
# _reconcile_vanished_replaced_album_paths survive every test in this class
# was that NO test asserted log output at all before this PR -- the missing
# assertion, not this fake's shape by itself. test_reconciliation_outcomes_are_logged
# below is the assertion that closes that gap; this production-shaped
# return value is a separate, independently-owed Rule B fix.) Every WIRING
# test below returns this shape.
_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES = (
    DeleteNotification("plex", "skipped", "Plex is not configured"),
    DeleteNotification("jellyfin", "skipped", "Jellyfin is not configured"),
)


class TestVanishedPathReconciliation(unittest.TestCase):
    """Issue #1203 item 2. Invariant: after a successful import that
    triggers notifiers, every album directory Beets previously held for
    that request's release identity, and no longer holds, is reconciled
    with both media servers exactly once — never before the Jellyfin pin
    capture has read those paths, and never by escalating to a Plex
    library-root scan. ("Never by calling the Jellyfin refresh endpoint"
    is impossible by construction since issue #1221 item 1 deleted the
    refresh machinery entirely — see
    ``lib.library_delete_notifiers.notify_library_delete``'s own
    docstring.)

    The Beets before/after directory-set diff
    (``lib.beets_db.BeetsDB.get_current_album_directories``, composed via
    ``lib.dispatch.core._vanished_album_directories``) is the PRIMARY,
    authoritative source. ``postflight.replaced_albums`` (the harness's
    mid-import serialization) is a SECONDARY source unioned in — it
    reports only an album the import's dup-guard answered "remove" for; it
    structurally cannot report a directory that left the library for any
    OTHER reason. Verified live (the defect this class's regression pin
    reproduces): the ``…1969 - David Bowie [1969]`` directory (request
    8964, the incident that motivated this issue) left the Beets library
    with NO ``download_log`` row anywhere in the corpus naming that path —
    so whatever removed it was not a dup-guard removal this pipeline ever
    recorded, and ``replaced_albums`` had nothing to say about it. A
    before/after Beets directory snapshot observes what Beets actually
    held, so it catches a directory leaving the release regardless of the
    reason — exactly the property ``replaced_albums`` cannot have. The
    secondary source still covers a replaced album whose OWN identity
    differs from the one being imported. See
    ``lib.dispatch.core._paths_needing_media_server_reconciliation``.

    ``media_server_notify_fn`` (``dispatch_import_core``'s kwarg-DI seam,
    forwarded to ``_reconcile_vanished_replaced_album_paths``'s own
    ``notify_fn``) replaces mocking ``notify_library_delete`` directly —
    that function grew real escalation-decision logic (issue #1203 item 2
    review) and no longer qualifies for the mock-audit's "thin wrapper, at
    most ten lines" allowlist bound (code-quality.md). The detect-and-report
    MECHANICS are covered by ``tests/test_library_delete_notifiers*.py`` and
    the composed generated property in
    ``tests/test_media_server_reconcile_generated.py``. This class is the
    WIRING pin: the right paths reach the seam, with
    ``allow_escalation=False``, in the right order.
    """

    def _dispatch(
        self,
        ir,
        *,
        cfg=None,
        configure_ext=None,
        release_id="test-mbid",
        media_server_notify_fn=None,
        album_directory_snapshot_fn=None,
        bypass_current_evidence_measurement=False,
    ):
        """Drive a real accepting ``dispatch_import_core`` call. Returns the
        ``patch_dispatch_externals()`` namespace and the ``FakePipelineDB``.

        ``bypass_current_evidence_measurement`` skips the REAL evidence
        gate's own current-library audio measurement (it would otherwise try
        to spectrally analyze the seeded fixture track, which is not real
        audio, and reject before ever reaching the notifier). Needed only by
        tests that seed a real Beets album under the SAME release id being
        dispatched (the primary-source snapshot tests below) — the
        secondary-source-only tests never have a current Beets row for
        their release id, so the real gate's ``current`` is trivially
        ``None`` and nothing is measured.
        """
        from lib.dispatch import dispatch_import_core

        cfg = cfg or _full_dispatch_config()
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"fixture audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42, status="downloading", mb_release_id=release_id,
                active_download_state={
                    "files": [], "filetype": "mp3", "current_path": tmpdir,
                },
            ))
            claimed, candidate_result, execution_lease = _claim_dispatch_job(
                db, path=tmpdir, release_id=release_id)
            cancellation_token = CancellationToken()
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result",
                       return_value=ir), \
                 pinned_dispatch_authority(
                     db, execution_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                if configure_ext is not None:
                    configure_ext(ext)
                from lib.dispatch.core import (
                    _snapshot_current_album_directories,
                )
                extra_kwargs: dict[str, object] = {}
                if bypass_current_evidence_measurement:
                    from lib.dispatch.types import EvidenceImportGate

                    extra_kwargs["evidence_gate_fn"] = (
                        lambda *_args, **_kwargs: EvidenceImportGate(
                            candidate=candidate_result.evidence))
                outcome = dispatch_import_core(
                    make_dispatch_request(
                        path=tmpdir,
                        mb_release_id=release_id,
                        request_id=42,
                        label="Test Artist - Test Album",
                        beets_harness_path=_HARNESS,
                        dl_info=DownloadInfo(filetype="mp3"),
                        distance=0.05,
                        scenario="strong_match",
                        files=[make_download_file(
                            username="user1", filename="01 - Track.mp3")],
                        candidate_import_job_id=claimed.id,
                        prevalidated_candidate_result=candidate_result,
                        execution_lease=execution_lease,
                        owner_session_identity=owner_session_identity,
                    ),
                    db,
                    cfg=cfg,
                    quality_gate_fn=noop_quality_gate,
                    cancellation_token=cancellation_token,
                    run_import_fn=_owned_test_runner,
                    media_server_notify_fn=media_server_notify_fn,
                    album_directory_snapshot_fn=(
                        album_directory_snapshot_fn
                        or _snapshot_current_album_directories),
                    **extra_kwargs,  # pyright: ignore[reportArgumentType]
                )
            finalize_claimed_dispatch(db, claimed, outcome)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        return ext, db

    # -- secondary source: postflight.replaced_albums ------------------

    def test_every_distinct_changed_replaced_path_is_reconciled_once(self):
        imported_path = (
            "/mnt/virtio/Music/Beets/David Bowie/2026 - Album [SBL 7912]")
        old_path = "/mnt/virtio/Music/Beets/David Bowie/1969 - Album [1969]"
        ir = make_import_result(decision="import", imported_path=imported_path)
        ir.postflight.replaced_albums = [
            DuplicateRemoveCandidate(beets_album_id=1, album_path=old_path),
            # Trailing-slash duplicate of the same real path — dedupe.
            DuplicateRemoveCandidate(
                beets_album_id=2, album_path=old_path + "/"),
            # Same as the NEW path — must not be reconciled.
            DuplicateRemoveCandidate(
                beets_album_id=3, album_path=imported_path),
            # Blank — must not be reconciled.
            DuplicateRemoveCandidate(beets_album_id=4, album_path=""),
        ]
        notify_fn = MagicMock(
            return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES)
        self._dispatch(ir, media_server_notify_fn=notify_fn)
        self.assertEqual(notify_fn.call_count, 1)
        call = notify_fn.call_args
        self.assertEqual(call.args[1], old_path)
        self.assertEqual(call.kwargs, {"allow_escalation": False})

    def test_unchanged_replaced_path_produces_no_reconciliation(self):
        imported_path = "/mnt/virtio/Music/Beets/Artist/2026 - Album"
        ir = make_import_result(decision="import", imported_path=imported_path)
        ir.postflight.replaced_albums = [
            DuplicateRemoveCandidate(
                beets_album_id=1, album_path=imported_path),
        ]
        notify_fn = MagicMock(
            return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES)
        self._dispatch(ir, media_server_notify_fn=notify_fn)
        notify_fn.assert_not_called()

    def test_no_replaced_albums_produces_no_reconciliation(self):
        ir = make_import_result(
            decision="import",
            imported_path="/mnt/virtio/Music/Beets/Artist/2026 - Album")
        notify_fn = MagicMock(
            return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES)
        self._dispatch(ir, media_server_notify_fn=notify_fn)
        notify_fn.assert_not_called()

    def test_reconciliation_runs_after_the_jellyfin_pin_capture(self):
        """``capture_jellyfin_date_created_pin`` reads the replaced albums'
        old paths synchronously to find the pre-upgrade Jellyfin item (item
        identity is a hash of the path) — reconciling a path before that
        capture runs would destroy the very item the capture needs. Drives
        the REAL capture function (only its true HTTP leaf,
        ``lib.util._jellyfin_get_json``, is faked — the same seam
        ``TestDispatchJellyfinPinCaptureSlice`` uses) rather than mocking our
        own orchestration code, per the leaf-seam-only mock policy."""
        order: list[str] = []

        def _fake_get_json(cfg, path, **params):
            order.append("jellyfin_pin_capture_http")
            return {"Items": []}

        def _configure(ext):
            ext.jellyfin.side_effect = (
                lambda *a, **k: order.append("trigger_jellyfin"))

        def _notify(*_a, **_k):
            order.append("reconcile")
            return _PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES

        imported_path = (
            "/mnt/virtio/Music/Beets/David Bowie/2026 - Album [SBL 7912]")
        old_path = "/mnt/virtio/Music/Beets/David Bowie/1969 - Album [1969]"
        ir = make_import_result(decision="import", imported_path=imported_path)
        ir.postflight.replaced_albums = [
            DuplicateRemoveCandidate(beets_album_id=1, album_path=old_path),
        ]
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            jellyfin_url="http://jf:8096",
            jellyfin_token="tok",
            jellyfin_path_map="/mnt/virtio/Music/Beets:/jf",
        )
        with patch("lib.util._jellyfin_get_json", side_effect=_fake_get_json):
            self._dispatch(
                ir, cfg=cfg, configure_ext=_configure,
                media_server_notify_fn=_notify)

        capture_indices = [
            i for i, v in enumerate(order) if v == "jellyfin_pin_capture_http"]
        self.assertTrue(capture_indices, order)
        self.assertLess(max(capture_indices), order.index("trigger_jellyfin"))
        self.assertLess(order.index("trigger_jellyfin"), order.index("reconcile"))

    def test_reconciliation_outcomes_are_logged(self) -> None:
        """Issue #1203 item 2 review: deleting the entire ``for outcome in
        outcomes: log_fn(...)`` loop in
        ``_reconcile_vanished_replaced_album_paths`` survived every test in
        this class, because every ``media_server_notify_fn`` fake returned
        ``()`` — a shape the REAL ``notify_library_delete`` can never
        produce (it always returns exactly one ``DeleteNotification`` per
        provider, two total). This is the reconciler's only
        operator-facing output; drive a fake that returns the real
        two-outcome shape and assert the journal actually receives both
        lines."""
        imported_path = "/mnt/virtio/Music/Beets/Artist/2026 - Album [NEW]"
        old_path = "/mnt/virtio/Music/Beets/Artist/2007 - Album [OLD]"
        ir = make_import_result(decision="import", imported_path=imported_path)
        ir.postflight.replaced_albums = [
            DuplicateRemoveCandidate(beets_album_id=1, album_path=old_path),
        ]
        outcomes = (
            DeleteNotification(
                "plex", "submitted", "HTTP 200; test-marker-plex-detail"),
            DeleteNotification(
                "jellyfin", "warning", "test-marker-jellyfin-detail"),
        )
        notify_fn = MagicMock(return_value=outcomes)

        with self.assertLogs("cratedigger", level="INFO") as cm:
            self._dispatch(ir, media_server_notify_fn=notify_fn)

        joined = "\n".join(cm.output)
        self.assertIn("MEDIA SERVER RECONCILE", joined)
        self.assertIn("test-marker-plex-detail", joined)
        self.assertIn("test-marker-jellyfin-detail", joined)

    # -- primary source: the Beets before/after snapshot diff ----------

    def test_snapshot_diff_reconciles_a_rename_replaced_albums_never_recorded(
        self,
    ) -> None:
        """THE REGRESSION PIN (#1203 item 2 review, round 3 correction).

        ``postflight.replaced_albums`` reports only albums the import's
        dup-guard answered "remove" for — it structurally cannot report a
        directory that left the library for any OTHER reason. Live corpus
        check for request 8964 (the David Bowie incident that motivated
        this issue): the ``…1969 - David Bowie [1969]`` directory left the
        Beets library with NO ``download_log`` row anywhere in the corpus
        naming that path — so whatever removed it was not a dup-guard
        removal this pipeline ever recorded, and ``replaced_albums`` was
        correctly EMPTY, not stale, for that transition.

        (An earlier version of this pin instead constructed a
        ``replaced_albums`` row that already showed the NEW path, calling
        it "stale" — a shape the real harness cannot produce: it records
        the true on-disk directory at ``get_duplicate_action`` time,
        before ``manipulate_files``, and a corpus check found no
        demonstrated stale record anywhere. That claim is retracted.)

        This pin drives the producible world directly: an ordinary import
        with ``replaced_albums == []`` (matching ``download_log`` 40197's
        own recorded shape) whose Beets directory nonetheless moves
        between the pre- and post-import snapshot. A before/after Beets
        directory snapshot is the only source that can prove that
        happened. RED against commit 4d81a0fa (replaced_albums-only):
        with nothing in ``replaced_albums``, the old code had no way to
        see this rename at all.
        """
        assert _HERMETIC_BEETS_PAIR is not None
        beets_library_db, beets_library_root = _HERMETIC_BEETS_PAIR
        release_id = "recon-defect-mbid"
        old_dir = os.path.join(
            beets_library_root, "David Bowie", "1969 - David Bowie [1969]")
        new_dir = os.path.join(
            beets_library_root, "David Bowie", "1969 - David Bowie [SBL 7912]")
        album_id = _seed_beets_album(
            beets_library_db, beets_library_root,
            mb_release_id=release_id, album_dir=old_dir)
        self.addCleanup(_delete_beets_album, beets_library_db, album_id)

        # replaced_albums stays at its default `[]` -- this rename left no
        # dup-guard-remove trace at all, matching the live corpus.
        ir = make_import_result(decision="import", imported_path=new_dir)

        def _configure(ext):
            def _move(*_args, **_kwargs):
                _move_beets_album_item(beets_library_db, album_id, new_dir)
                return MagicMock(returncode=0, stdout="", stderr="")
            ext.run.side_effect = _move

        notify_fn = MagicMock(
            return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES)
        self._dispatch(
            ir, release_id=release_id, configure_ext=_configure,
            media_server_notify_fn=notify_fn,
            bypass_current_evidence_measurement=True)

        self.assertEqual(notify_fn.call_count, 1)
        call = notify_fn.call_args
        self.assertEqual(call.args[1], old_dir)
        self.assertEqual(call.kwargs, {"allow_escalation": False})

    def test_snapshot_diff_alone_reconciles_a_rename_with_no_replaced_albums(
        self,
    ) -> None:
        """The primary source works standalone with no replaced_albums at
        all, proving it does not depend on the harness's serialization."""
        assert _HERMETIC_BEETS_PAIR is not None
        beets_library_db, beets_library_root = _HERMETIC_BEETS_PAIR
        release_id = "recon-solo-mbid"
        old_dir = os.path.join(beets_library_root, "Artist", "2007 - Album")
        new_dir = os.path.join(beets_library_root, "Artist", "2026 - Album")
        album_id = _seed_beets_album(
            beets_library_db, beets_library_root,
            mb_release_id=release_id, album_dir=old_dir)
        self.addCleanup(_delete_beets_album, beets_library_db, album_id)

        ir = make_import_result(decision="import", imported_path=new_dir)

        def _configure(ext):
            def _move(*_args, **_kwargs):
                _move_beets_album_item(beets_library_db, album_id, new_dir)
                return MagicMock(returncode=0, stdout="", stderr="")
            ext.run.side_effect = _move

        notify_fn = MagicMock(
            return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES)
        self._dispatch(
            ir, release_id=release_id, configure_ext=_configure,
            media_server_notify_fn=notify_fn,
            bypass_current_evidence_measurement=True)

        self.assertEqual(notify_fn.call_count, 1)
        self.assertEqual(notify_fn.call_args.args[1], old_dir)

    def test_unchanged_beets_path_produces_no_reconciliation(self) -> None:
        """No rename between the pre- and post-import snapshot — the common
        case, at zero reconciliation cost.

        Issue #1203 item 2 review finding 5: a single unmoved album whose
        directory happens to equal ``imported_path`` patrols a bystander —
        deleting ``_vanished_album_directories``'s own survival check (``or
        norm in post_normalized``) makes it report EVERY pre-import
        directory as vanished, but that one still gets filtered downstream
        by ``_paths_needing_media_server_reconciliation``'s
        distinct-from-``imported_path`` gate, so the mutant survives. A
        SECOND unmoved album at a genuinely different directory is not
        equal to ``imported_path`` and so is not caught by that downstream
        gate — only ``_vanished_album_directories`` correctly recognizing
        its survival keeps it out of ``notify_fn``."""
        assert _HERMETIC_BEETS_PAIR is not None
        beets_library_db, beets_library_root = _HERMETIC_BEETS_PAIR
        release_id = "recon-unchanged-mbid"
        album_dir = os.path.join(beets_library_root, "Artist", "2026 - Album")
        other_dir = os.path.join(beets_library_root, "Artist", "Other Album")
        album_id = _seed_beets_album(
            beets_library_db, beets_library_root,
            mb_release_id=release_id, album_dir=album_dir)
        self.addCleanup(_delete_beets_album, beets_library_db, album_id)
        other_album_id = _seed_beets_album(
            beets_library_db, beets_library_root,
            mb_release_id=release_id, album_dir=other_dir)
        self.addCleanup(_delete_beets_album, beets_library_db, other_album_id)

        ir = make_import_result(decision="import", imported_path=album_dir)
        notify_fn = MagicMock(
            return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES)
        self._dispatch(
            ir, release_id=release_id, media_server_notify_fn=notify_fn,
            bypass_current_evidence_measurement=True)

        notify_fn.assert_not_called()

    def test_release_held_by_two_albums_only_the_moved_one_is_reconciled(
        self,
    ) -> None:
        """A release currently held by two beets album rows (the split-
        brain "multiple same-identity rows" state
        ``BeetsDB.get_all_album_ids_for_release`` also guards against, or a
        curated duplicate-pressing collection, CLAUDE.md invariant 5) —
        only the one that moves is reconciled; the other's unchanged
        directory is left alone."""
        assert _HERMETIC_BEETS_PAIR is not None
        beets_library_db, beets_library_root = _HERMETIC_BEETS_PAIR
        release_id = "recon-two-albums-mbid"
        dir_a_old = os.path.join(beets_library_root, "Artist", "Album A Old")
        dir_a_new = os.path.join(beets_library_root, "Artist", "Album A New")
        dir_b = os.path.join(beets_library_root, "Artist", "Album B")
        album_a = _seed_beets_album(
            beets_library_db, beets_library_root,
            mb_release_id=release_id, album_dir=dir_a_old)
        self.addCleanup(_delete_beets_album, beets_library_db, album_a)
        album_b = _seed_beets_album(
            beets_library_db, beets_library_root,
            mb_release_id=release_id, album_dir=dir_b)
        self.addCleanup(_delete_beets_album, beets_library_db, album_b)

        ir = make_import_result(decision="import", imported_path=dir_a_new)

        def _configure(ext):
            def _move(*_args, **_kwargs):
                _move_beets_album_item(beets_library_db, album_a, dir_a_new)
                return MagicMock(returncode=0, stdout="", stderr="")
            ext.run.side_effect = _move

        notify_fn = MagicMock(
            return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES)
        self._dispatch(
            ir, release_id=release_id, configure_ext=_configure,
            media_server_notify_fn=notify_fn,
            bypass_current_evidence_measurement=True)

        self.assertEqual(notify_fn.call_count, 1)
        self.assertEqual(notify_fn.call_args.args[1], dir_a_old)

    def test_snapshot_capture_failure_is_best_effort(self) -> None:
        """A raising snapshot mechanism never fails the import, and the
        SECONDARY source (replaced_albums) still reconciles even when the
        primary snapshot-diff source fails on both sides — proving the
        best-effort boundary sits at the call site
        (``_capture_album_directory_snapshot``), not inside the
        reconciliation decision itself."""
        def _raise(**_kwargs):
            raise RuntimeError("beets db exploded")

        imported_path = "/mnt/virtio/Music/Beets/Artist/2026 - Album [NEW]"
        old_path = "/mnt/virtio/Music/Beets/Artist/2007 - Album [OLD]"
        ir = make_import_result(decision="import", imported_path=imported_path)
        ir.postflight.replaced_albums = [
            DuplicateRemoveCandidate(beets_album_id=1, album_path=old_path),
        ]
        notify_fn = MagicMock(
            return_value=_PRODUCTION_SHAPED_NOT_CONFIGURED_OUTCOMES)
        _ext, db = self._dispatch(
            ir, media_server_notify_fn=notify_fn,
            album_directory_snapshot_fn=_raise)

        self.assertEqual(db.request(42)["status"], "imported")
        notify_fn.assert_called_once()
        call = notify_fn.call_args
        self.assertEqual(call.args[1], old_path)
        self.assertEqual(call.kwargs, {"allow_escalation": False})


class _RecordingProcessGroup:
    """Typed stand-in for the injected child-supervision group."""

    def __init__(
        self,
        process: sp.Popen[bytes],
        *,
        returncode: int = 0,
        log: list[str] | None = None,
        wait_hook: Callable[[], None] | None = None,
    ) -> None:
        self.process = process
        self.calls: list[str] = log if log is not None else []
        self.waited_tokens: list[CancellationToken] = []
        self.probe_results: list[bool] = []
        # Set by ``terminate_and_wait``, so a test can block inside ``wait``
        # until the deadline timer's termination has actually landed.
        self.terminated = threading.Event()
        self._returncode = returncode
        self._wait_hook = wait_hook

    @property
    def pid(self) -> int:
        return 4321

    def terminate_and_wait(self, *, timeout: float = 5.0) -> int:
        del timeout
        self.calls.append("terminate_and_wait")
        self.terminated.set()
        return -15

    def wait(
        self,
        token: CancellationToken,
        *,
        owner_session_probe: Callable[[], bool] | None = None,
        probe_interval: float = 1.0,
    ) -> int:
        del probe_interval
        self.calls.append("wait")
        self.waited_tokens.append(token)
        if owner_session_probe is not None:
            self.probe_results.append(owner_session_probe())
        if self._wait_hook is not None:
            self._wait_hook()
        return self._returncode


class TestRunImportOneProcessGroupSeam(unittest.TestCase):
    """``run_import_one`` supervises the child through an injected factory.

    The seam exists so tests never replace ``MonitoredProcessGroup`` — a
    ~120-line supervisor carrying SIGTERM->SIGKILL escalation, deadline
    arithmetic and termination-idempotence locking — by patching the module
    binding. Definition-time defaults are injected, never patched.
    """

    def test_default_factory_is_the_production_supervisor(self) -> None:
        from lib.dispatch.subprocess_runner import run_import_one

        default = inspect.signature(run_import_one).parameters[
            "process_group_factory"
        ].default
        self.assertIs(default, MonitoredProcessGroup)

    def test_injected_factory_supervises_the_spawned_child(self) -> None:
        from lib.dispatch.subprocess_runner import run_import_one

        token = CancellationToken()
        events: list[str] = []
        built: list[_RecordingProcessGroup] = []

        def factory(process: sp.Popen[bytes]) -> _RecordingProcessGroup:
            group = _RecordingProcessGroup(process, log=events)
            built.append(group)
            events.append("factory")
            return group

        with patch("lib.dispatch.subprocess_runner.sp.Popen") as popen:
            run = run_import_one(
                path="/tmp/source",
                mb_release_id="release-1",
                beets_harness_path="/tmp/harness/run",
                cancellation_token=token,
                on_spawn=lambda pid: events.append(f"spawn:{pid}"),
                owner_session_probe=lambda: True,
                process_group_factory=factory,
            )

        self.assertEqual(len(built), 1)
        group = built[0]
        # The factory wraps the REAL spawned handle, and the group it returns
        # is what the runner supervises — pid, wait, and cancellation token.
        self.assertIs(group.process, popen.return_value)
        self.assertEqual(events, ["factory", "spawn:4321", "wait"])
        self.assertEqual(group.waited_tokens, [token])
        self.assertEqual(group.probe_results, [True])
        self.assertEqual(run.returncode, 0)

    def test_spawn_failure_terminates_the_injected_group(self) -> None:
        from lib.dispatch.subprocess_runner import run_import_one

        built: list[_RecordingProcessGroup] = []

        def factory(process: sp.Popen[bytes]) -> _RecordingProcessGroup:
            group = _RecordingProcessGroup(process)
            built.append(group)
            return group

        def reject_child(_pid: int) -> None:
            raise RuntimeError("child lease CAS rejected")

        with (
            patch("lib.dispatch.subprocess_runner.sp.Popen"),
            self.assertRaisesRegex(RuntimeError, "child lease CAS rejected"),
        ):
            run_import_one(
                path="/tmp/source",
                mb_release_id="release-1",
                beets_harness_path="/tmp/harness/run",
                cancellation_token=CancellationToken(),
                on_spawn=reject_child,
                process_group_factory=factory,
            )

        self.assertEqual(len(built), 1)
        self.assertEqual(built[0].calls, ["terminate_and_wait"])

    def test_owner_cancellation_wins_when_the_deadline_also_fires(self) -> None:
        from lib.dispatch.subprocess_runner import run_import_one

        token = CancellationToken()
        built: list[_RecordingProcessGroup] = []

        def lose_owner_while_waiting() -> None:
            token.cancel("owner_session_lost")
            # The zero-second deadline timer terminates the group from its own
            # thread; block until that has landed so the timeout and the
            # ownership cancellation genuinely race.
            self.assertTrue(built[0].terminated.wait(timeout=5.0))

        def factory(process: sp.Popen[bytes]) -> _RecordingProcessGroup:
            group = _RecordingProcessGroup(
                process,
                returncode=-15,
                wait_hook=lose_owner_while_waiting,
            )
            built.append(group)
            return group

        with (
            patch("lib.dispatch.subprocess_runner.sp.Popen"),
            self.assertRaisesRegex(ExecutionCancelled, "owner_session_lost"),
        ):
            run_import_one(
                path="/tmp/source",
                mb_release_id="release-1",
                beets_harness_path="/tmp/harness/run",
                timeout=0,
                cancellation_token=token,
                process_group_factory=factory,
            )

        # Ownership loss is the stronger cause: TimeoutExpired must not
        # displace it even though the deadline demonstrably expired and
        # terminated the group. The two land on different threads, so only
        # their occurrence is ordered evidence, not their sequence.
        self.assertEqual(len(built), 1)
        self.assertEqual(sorted(built[0].calls), ["terminate_and_wait", "wait"])
        self.assertTrue(built[0].terminated.is_set())


if __name__ == "__main__":
    unittest.main()
