"""Tests for lib/download.py — download processing functions.

Tests _build_download_info, cancel_and_delete, poll_active_downloads,
and grab_most_wanted.

Pre-import measurement behavior (audio integrity + spectral analysis) is
shared with the force-import path and tested directly against
``lib.measurement.measure_preimport_state`` in ``tests/test_measurement.py``
and end-to-end through
``tests/test_integration_slices.py::TestSpectralPropagationSlice``.
"""
import atexit
import copy
import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
import unittest
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, cast
from unittest.mock import MagicMock, patch

from lib.current_library_evidence import HaveEnrichment, HavePreparation
from lib.download_materialization import (
    Materialized,
    MaterializeFailed,
)
from lib.download_ownership import DownloadOwnershipWriter
from lib.import_execution import (
    CancellationToken,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
    ProcessIdentity,
)
from lib.pipeline_db import AlbumRequestRow, TransferLedgerRow
from lib.quality import CURRENT_EVIDENCE_LINEAGE_VERSION
from lib.slskd_client import TransferSnapshot
from tests.dispatch_helpers import (
    claim_next_import_preview_job,
    handoff_automation_owner,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI
from tests.helpers import (
    make_ctx_with_fake_db,
    make_cycle_collaborators,
    make_download_directory,
    make_download_file,
    make_download_user,
    make_grab_list_entry,
    make_request_row,
    make_requests_http_error,
    make_transfer_snapshot,
    own_transfer_keys,
    rebind_collaborators,
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _private_processing_dir(parent: str) -> str:
    """Create the private processing tree required by path-authority tests."""
    processing_dir = os.path.join(parent, "processing")
    os.mkdir(processing_dir, 0o700)
    os.mkdir(os.path.join(processing_dir, "albums"), 0o700)
    os.mkdir(os.path.join(processing_dir, "preview"), 0o700)
    return processing_dir


def _write_test_audio(path: str) -> None:
    """Write a tiny decodable fixture in the container named by ``path``."""
    codec = "flac" if path.endswith(".flac") else "libmp3lame"
    subprocess.run(
        [
            "ffmpeg", "-v", "error", "-nostdin", "-f", "lavfi",
            "-i", "sine=frequency=440:duration=0.1", "-c:a", codec, path,
        ],
        check=True,
        timeout=30,
    )


def _make_ctx(cfg=None, slskd=None, pipeline_db_source=None):
    """Build a mock CratediggerContext."""
    from lib.context import CratediggerContext
    if cfg is None:
        cfg = MagicMock()
        # Each materialization test gets an owned 0700 processing root
        # separate from its (adversarial) slskd fixture.  Keep the
        # TemporaryDirectory on the config so its cleanup lifetime matches
        # the context that uses it.
        # The dev shell's TMPDIR has private tmpfs ancestry, matching the
        # production requirement without writing scratch data to the checkout.
        processing_parent = tempfile.mkdtemp(
            prefix="cratedigger-test-processing-")
        atexit.register(shutil.rmtree, processing_parent, ignore_errors=True)
        cfg.processing_dir = _private_processing_dir(processing_parent)
        cfg.slskd_download_dir = "/tmp/test_downloads"
        cfg.beets_validation_enabled = False
        cfg.beets_distance_threshold = 0.15
        cfg.beets_staging_dir = "/tmp/staging"
        cfg.beets_harness_path = "/tmp/harness"
        cfg.audio_check_mode = "normal"
        cfg.stalled_timeout = 300
        cfg.remote_queue_timeout = 120
        cfg.slskd_host_url = "http://localhost:5030"
        cfg.slskd_url_base = "/"
        cfg.pipeline_db_enabled = True
    if slskd is None:
        slskd = FakeSlskdAPI()
    if pipeline_db_source is None:
        pipeline_db_source = FakePipelineDBSource()
    return CratediggerContext(
        collaborators=make_cycle_collaborators(
            cfg=cfg,
            slskd=slskd,
            pipeline_db_source=pipeline_db_source,
        ),
    )


class TestDownloadModuleBoundary(unittest.TestCase):
    """Moved reconstruction must not remain importable from its old module."""

    def test_reconstruct_grab_list_entry_is_not_reexported(self):
        from lib import download as download_module

        with self.assertRaises(KeyError):
            vars(download_module)["reconstruct_grab_list_entry"]


class TestBuildDownloadInfo(unittest.TestCase):

    def test_basic(self):
        from lib.dispatch import _build_download_info
        files = [make_download_file(bitRate=320, sampleRate=44100)]
        album = make_grab_list_entry(files=files)
        dl = _build_download_info(album)
        self.assertEqual(dl.username, "user1")
        self.assertEqual(dl.filetype, "mp3")
        self.assertEqual(dl.bitrate, 320)
        self.assertEqual(dl.sample_rate, 44100)

    def test_empty_files(self):
        from lib.dispatch import _build_download_info
        album = make_grab_list_entry(files=[])
        dl = _build_download_info(album)
        self.assertIsNone(dl.username)
        self.assertIsNone(dl.filetype)

    def test_multi_user(self):
        from lib.dispatch import _build_download_info
        files = [
            make_download_file(username="beta_user"),
            make_download_file(username="alpha_user"),
        ]
        album = make_grab_list_entry(files=files)
        dl = _build_download_info(album)
        self.assertEqual(dl.username, "alpha_user, beta_user")


class TestPostRejectionWrongMatchTriage(unittest.TestCase):
    def test_runs_cleanup_for_new_wrong_match_log_row(self):
        from lib.download_rejection import _run_post_rejection_wrong_match_cleanup

        db = FakePipelineDB()
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
            result = _run_post_rejection_wrong_match_cleanup(
                ctx,
                123,
                scenario="high_distance",
            )

        cleanup.assert_called_once_with(
            db,
            123,
            ignore_import_job_id=None,
        )
        self.assertIs(result, cleanup.return_value)

    def test_copies_import_job_candidate_evidence_before_cleanup(self):
        from lib.download_rejection import _run_post_rejection_wrong_match_cleanup

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        log_id = db.log_download(
            1,
            outcome="rejected",
            validation_result={"scenario": "wrong_match", "failed_path": "/tmp/source"},
        )
        job = handoff_automation_owner(db, 1)
        preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="download-test-boot",
            invocation_id="download-test-preview",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=4044, start_ticks=44),
        )
        claimed = claim_next_import_preview_job(db, worker_id="download-test-preview",
        execution_lease=preview_lease,)
        assert claimed is not None and claimed.id == job.id
        self.assertTrue(db.set_import_job_candidate_evidence(
            job.id,
            44,
            expected_execution_lease=preview_lease,
        ))
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
            result = _run_post_rejection_wrong_match_cleanup(
                ctx,
                log_id,
                scenario="high_distance",
                import_job_id=job.id,
            )

        self.assertEqual(db.get_download_log_candidate_evidence_id(log_id), 44)
        cleanup.assert_called_once_with(
            db,
            log_id,
            ignore_import_job_id=job.id,
        )
        self.assertIs(result, cleanup.return_value)

    def test_links_evidence_for_visible_but_not_delete_eligible_scenario(self):
        """Issue #1077, F3/F8: the two-stage gate is worklist visibility
        FIRST, then evidence-linking, then delete-eligibility — matching
        ``scripts/importer.py::_cleanup_committed_wrong_match_rejection``.
        Before this fix, delete-eligibility was the OUTER gate, so a kept,
        banned, visible-but-never-delete-eligible row (e.g.
        ``untracked_audio``, Lane B's own vocabulary) never even attempted
        to link its candidate evidence — the worklist card rendered with no
        candidate measurement even though the evidence was one call away.
        """
        from lib.download_rejection import _run_post_rejection_wrong_match_cleanup

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        log_id = db.log_download(
            1,
            outcome="rejected",
            validation_result={
                "scenario": "untracked_audio", "failed_path": "/tmp/source",
            },
        )
        job = handoff_automation_owner(db, 1)
        preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="download-test-boot",
            invocation_id="download-test-preview-untracked",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=4055, start_ticks=55),
        )
        claimed = claim_next_import_preview_job(
            db, worker_id="download-test-preview-untracked",
            execution_lease=preview_lease,
        )
        assert claimed is not None and claimed.id == job.id
        self.assertTrue(db.set_import_job_candidate_evidence(
            job.id,
            55,
            expected_execution_lease=preview_lease,
        ))
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
            result = _run_post_rejection_wrong_match_cleanup(
                ctx,
                log_id,
                scenario="untracked_audio",
                import_job_id=job.id,
                contributor_usernames=("peer-one", "peer-two"),
            )

        self.assertEqual(db.get_download_log_candidate_evidence_id(log_id), 55)
        linked_row = next(row for row in db.download_logs if row.id == log_id)
        self.assertEqual(
            linked_row.candidate_contributor_usernames,
            ["peer-one", "peer-two"],
        )
        cleanup.assert_not_called()
        self.assertIsNone(result)

    def test_skips_every_non_match_rejection_scenario(self):
        from lib.download_rejection import _run_post_rejection_wrong_match_cleanup
        from lib.wrong_match_policy import WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS

        db = FakePipelineDB()
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
            for scenario in sorted(WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS):
                with self.subTest(scenario=scenario):
                    result = _run_post_rejection_wrong_match_cleanup(
                        ctx,
                        123,
                        scenario=scenario,
                    )
                    self.assertIsNone(result)

        cleanup.assert_not_called()

    def test_skips_every_scenario_outside_the_delete_eligible_allowlist(self):
        """Issue #1077, D6: the cleanup lane is an allowlist, not a
        fail-open exclusion set. World failures with a reviewable folder,
        every unknown/novel scenario string, and ``None`` are never
        delete-eligible — the reducer is never even consulted for them."""
        from lib.download_rejection import _run_post_rejection_wrong_match_cleanup

        db = FakePipelineDB()
        ctx = make_ctx_with_fake_db(db)

        ineligible_scenarios = (
            None,
            "untracked_audio",
            "request_missing_mbid",
            "request_missing_request_id",
            "validation_error",
            "strong_match",
            "a-brand-new-scenario-nobody-has-classified-yet",
            "",
        )
        with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
            for scenario in ineligible_scenarios:
                with self.subTest(scenario=scenario):
                    result = _run_post_rejection_wrong_match_cleanup(
                        ctx,
                        123,
                        scenario=scenario,
                    )
                    self.assertIsNone(result)

        cleanup.assert_not_called()

    def test_admits_every_delete_eligible_scenario(self):
        """The positive half of D6: exactly the allowlist reaches cleanup."""
        from lib.download_rejection import _run_post_rejection_wrong_match_cleanup
        from lib.wrong_match_policy import DELETE_ELIGIBLE_REJECTION_SCENARIOS

        db = FakePipelineDB()
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
            for scenario in sorted(DELETE_ELIGIBLE_REJECTION_SCENARIOS):
                with self.subTest(scenario=scenario):
                    _run_post_rejection_wrong_match_cleanup(
                        ctx,
                        123,
                        scenario=scenario,
                    )

        self.assertEqual(
            cleanup.call_count, len(DELETE_ELIGIBLE_REJECTION_SCENARIOS),
        )

    def test_rejected_download_handler_triggers_triage_after_logging(self):
        import tempfile

        from lib.download_rejection import _handle_rejected_result
        from lib.quality import ValidationResult
        from lib.staged_album import StagedAlbum

        class Source:
            def __init__(self, db):
                self.db = db
                self.rejected = False
                self.reject_args = None
                self.reject_kwargs = None

            def _get_db(self):
                return self.db

            def reject_and_requeue(self, *args, **kwargs):
                self.rejected = True
                self.reject_args = args
                self.reject_kwargs = kwargs
                return 77

        db = FakePipelineDB()
        source = Source(db)
        ctx = _make_ctx(pipeline_db_source=source)

        with tempfile.TemporaryDirectory() as tmpdir:
            current_path = os.path.join(tmpdir, "Artist - Album")
            os.makedirs(current_path)
            with open(os.path.join(current_path, "01 - Track.mp3"), "w",
                      encoding="utf-8") as fp:
                fp.write("audio")
            cfg = cast(Any, ctx.cfg)
            cfg.beets_tracking_file = os.path.join(tmpdir, "tracking.jsonl")
            album = make_grab_list_entry(
                files=[make_download_file(username="user1")],
                artist="Artist",
                title="Album",
                mb_release_id="test-mbid",
                db_request_id=42,
            )
            result = ValidationResult(
                valid=False,
                distance=0.4,
                scenario="high_distance",
                detail="too far",
            )

            with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
                _handle_rejected_result(
                    album,
                    result,
                    StagedAlbum(current_path=current_path, request_id=42),
                    ctx,
                )

        self.assertTrue(source.rejected)
        assert source.reject_args is not None
        stored = source.reject_args[1]
        self.assertEqual(stored.source_dirs, ["user1\\Music"])
        cleanup.assert_called_once_with(
            db,
            77,
            ignore_import_job_id=None,
        )


class TestResolveRequestRejectionId(unittest.TestCase):

    def test_refuses_release_id_presence_mismatch(self):
        from lib.download_rejection import _resolved_request_rejection_id

        for desc, row_mbid, album_mbid in [
            ("row missing mbid", "", "test-mbid"),
            ("album missing mbid", "test-mbid", ""),
        ]:
            with self.subTest(desc=desc):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=1,
                    status="downloading",
                    artist_name="Artist",
                    album_title="Album",
                    year=2024,
                    mb_release_id=row_mbid,
                ))
                cfg = cast(Any, _make_ctx().cfg)
                ctx = make_ctx_with_fake_db(db, cfg=cfg)
                album = make_grab_list_entry(
                    album_id=1,
                    artist="Artist",
                    title="Album",
                    year="2024",
                    mb_release_id=album_mbid,
                    db_request_id=None,
                    db_source="request",
                )

                resolved_db, request_id = _resolved_request_rejection_id(album, ctx)

                self.assertIs(resolved_db, db)
                self.assertIsNone(request_id)


class TestDownloadRejectionExtraction(unittest.TestCase):
    """The completed-download module no longer owns reject writers."""

    def test_reject_writer_functions_live_only_in_focused_module(self):
        import ast
        from pathlib import Path

        rejection_names = {
            "_run_post_rejection_wrong_match_cleanup",
            "_resolved_request_rejection_id",
            "_reject_request_auto_import",
            "_handle_rejected_result",
        }
        shared_names = {"source_dirs_for_album"}
        processing_tree = ast.parse(
            Path("lib/download_processing.py").read_text(encoding="utf-8")
        )
        rejection_tree = ast.parse(
            Path("lib/download_rejection.py").read_text(encoding="utf-8")
        )
        paths_tree = ast.parse(
            Path("lib/processing_paths.py").read_text(encoding="utf-8")
        )
        processing_defs = {
            node.name for node in ast.walk(processing_tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        rejection_defs = {
            node.name for node in ast.walk(rejection_tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        paths_defs = {
            node.name for node in ast.walk(paths_tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertTrue(rejection_names.isdisjoint(processing_defs))
        self.assertTrue(rejection_names.issubset(rejection_defs))
        self.assertTrue(shared_names.isdisjoint(processing_defs | rejection_defs))
        self.assertTrue(shared_names.issubset(paths_defs))

    def test_validation_imports_only_rejection_handoffs(self):
        import ast
        from pathlib import Path

        validation_tree = ast.parse(
            Path("lib/download_validation.py").read_text(encoding="utf-8")
        )
        imported = {
            alias.name
            for node in ast.walk(validation_tree)
            if isinstance(node, ast.ImportFrom)
            and node.module == "lib.download_rejection"
            for alias in node.names
        }
        self.assertEqual(
            imported,
            {"_handle_rejected_result", "_reject_request_auto_import"},
        )


class TestDownloadMaterializationExtraction(unittest.TestCase):
    """Materialization and recovery have one focused owning module."""

    MATERIALIZATION_NAMES: ClassVar = {
        "Materialized",
        "MaterializeFailed",
        "MaterializeGuarded",
        "MaterializeResult",
        "classify_staged_album_location",
        "_evaluate_staged_path_readiness",
        "_materialize_processing_dir",
    }

    def test_materialization_symbols_are_not_compatibility_exported(self):
        import lib.download_materialization as materialization
        import lib.download_processing as processing

        for name in self.MATERIALIZATION_NAMES:
            with self.subTest(name=name):
                self.assertTrue(hasattr(materialization, name))
                self.assertFalse(hasattr(processing, name))

    def test_processing_uses_qualified_materialization_dependency(self):
        import ast
        from pathlib import Path

        processing_tree = ast.parse(
            Path("lib/download_processing.py").read_text(encoding="utf-8")
        )
        imported_names = {
            alias.name
            for node in ast.walk(processing_tree)
            if isinstance(node, ast.ImportFrom)
            and node.module == "lib.download_materialization"
            for alias in node.names
        }
        self.assertEqual(imported_names, set())
        self.assertTrue(any(
            isinstance(node, ast.ImportFrom)
            and node.module == "lib"
            and any(
                alias.name == "download_materialization"
                for alias in node.names
            )
            for node in ast.walk(processing_tree)
        ))


## TestGatherSpectralContext and TestCheckQualityGateDecision removed:
## - TestGatherSpectralContext never called the function it claimed to test —
##   it reimplemented the condition logic in test code and asserted on that.
## - TestCheckQualityGateDecision duplicated tests already in
##   test_quality_decisions.py::TestQualityGateDecision.


# === NEW tests for functions moving to lib/download.py ===

class _RecordingOwnershipWriter(DownloadOwnershipWriter):
    """A real ownership writer that records what it was asked (#1313).

    A subclass rather than an assignment over ``owned_transfer_keys``:
    the field is typed ``DownloadOwnershipWriter | None`` since #1313, and
    pyright rejects assigning to a method. Both counters matter and neither
    implies the other. ``owned_transfer_keys`` returns early on empty keys
    before opening a handle, so a caller can reach the writer without ever
    reaching ``db_factory``.
    """

    def __init__(self, db: FakePipelineDB) -> None:
        self._db = db
        self.opened = 0
        self.asked: list[list[tuple[str, str]]] = []
        super().__init__(db_factory=self._open, close_after_use=False)

    def _open(self) -> FakePipelineDB:
        self.opened += 1
        return self._db

    def owned_transfer_keys(
        self, keys: Sequence[tuple[str, str]],
    ) -> set[tuple[str, str]]:
        self.asked.append(list(keys))
        return super().owned_transfer_keys(keys)


class TestCancelAndDelete(unittest.TestCase):
    """cancel_and_delete deletes completed payloads at their authoritative
    (event-derived) local paths — never at inferred folder locations
    (issue #146 phase 3; kills the shared-``CD1/`` rmtree hazard)."""

    def _ctx(self, slskd=None):
        from lib.download_ownership import DownloadOwnershipWriter

        slskd = slskd or FakeSlskdAPI()
        ctx = _make_ctx(slskd=slskd)
        tmpdir = tempfile.mkdtemp(prefix="cratedigger-cancel-test-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        cast(Any, ctx.cfg).slskd_download_dir = tmpdir
        # Destruction is ledger-gated (#1278), so these tests carry the
        # real ownership collaborator over a fake DB rather than a ctx
        # with no ownership wired at all — production always has one.
        self.ledger_db = FakePipelineDB()
        rebind_collaborators(
            ctx,
            download_ownership=DownloadOwnershipWriter(
            db_factory=lambda: self.ledger_db, close_after_use=False),
        )
        return ctx, slskd, tmpdir

    def _own(self, *files) -> None:
        """Ledger these files as ours: write-ahead intent, then the
        accepted-POST confirmation that makes it destructive authority."""
        from lib.pipeline_db import TransferLedgerRow

        self.ledger_db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=1, username=f.username, filename=f.filename)
            for f in files
        ])
        for f in files:
            self.ledger_db.confirm_transfer_enqueue(
                f.username, f.filename, request_id=1)

    def _file_event(self, slskd, *, id, username, filename, local_filename):
        import json as _json
        return slskd.events.make_event(
            id=id, timestamp="2026-07-02T10:00:00.0000000Z",
            type="DownloadFileComplete",
            data=_json.dumps({
                "version": 0,
                "localFilename": local_filename,
                "remoteFilename": filename,
                "transfer": {
                    "id": f"tid-{id}", "username": username,
                    "filename": filename, "size": 10,
                },
            }))

    def _dir_event(self, slskd, *, id, username, remote_dir, local_dir):
        import json as _json
        return slskd.events.make_event(
            id=id, timestamp="2026-07-02T10:00:00.0000000Z",
            type="DownloadDirectoryComplete",
            data=_json.dumps({
                "version": 0,
                "localDirectoryName": local_dir,
                "remoteDirectoryName": remote_dir,
                "username": username,
            }))

    def test_cancels_and_deletes_stamped_file_pruning_empty_dir(self):
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_dir = os.path.join(tmpdir, "Album Folder")
        os.makedirs(local_dir)
        local_path = os.path.join(local_dir, "01 - Track.mp3")
        with open(local_path, "w") as fp:
            fp.write("x")
        f = make_download_file(file_dir="someuser\\Album Folder")
        f.local_path = local_path
        self._own(f)

        ok = cancel_and_delete([f], ctx)

        self.assertTrue(ok)
        self.assertEqual(
            [(call.username, call.id)
             for call in slskd.transfers.cancel_download_calls],
            [("user1", "file-id-1")],
        )
        self.assertFalse(os.path.exists(local_path))
        self.assertFalse(os.path.isdir(local_dir))

    def test_shared_dir_survives_when_other_files_remain(self):
        """The CD1 regression: deleting one request's file must not take
        an unrelated sibling in the same on-disk folder with it."""
        from lib.slskd_transfers import cancel_and_delete
        ctx, _, tmpdir = self._ctx()
        local_dir = os.path.join(tmpdir, "CD1")
        os.makedirs(local_dir)
        mine = os.path.join(local_dir, "01 - Mine.mp3")
        theirs = os.path.join(local_dir, "01 - Theirs.mp3")
        for path in (mine, theirs):
            with open(path, "w") as fp:
                fp.write("x")
        f = make_download_file(file_dir="someuser\\CD1")
        f.local_path = mine
        self._own(f)

        cancel_and_delete([f], ctx)

        self.assertFalse(os.path.exists(mine))
        self.assertTrue(os.path.exists(theirs))
        self.assertTrue(os.path.isdir(local_dir))

    def test_unstamped_file_resolved_via_fresh_events(self):
        """A file that completed after the cycle's ingest pass has no
        stamp yet — a fresh events-page lookup still finds its payload."""
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_dir = os.path.join(tmpdir, "Album Folder")
        os.makedirs(local_dir)
        local_path = os.path.join(local_dir, "01 - Track.mp3")
        with open(local_path, "w") as fp:
            fp.write("x")
        f = make_download_file(
            filename="someuser\\Music\\Album Folder\\01 - Track.mp3",
            file_dir="someuser\\Music\\Album Folder")
        slskd.events.set_events([self._file_event(
            slskd, id="ev-1", username=f.username, filename=f.filename,
            local_filename=local_path)])
        self._own(f)

        cancel_and_delete([f], ctx)

        self.assertFalse(os.path.exists(local_path))
        self.assertFalse(os.path.isdir(local_dir))

    def test_directory_event_prunes_empty_authoritative_dir(self):
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_dir = os.path.join(tmpdir, "Album Folder")
        os.makedirs(local_dir)  # already emptied by an earlier cleanup
        f = make_download_file(
            filename="someuser\\Music\\Album Folder\\01 - Track.mp3",
            file_dir="someuser\\Music\\Album Folder")
        slskd.events.set_events([self._dir_event(
            slskd, id="ev-1", username=f.username,
            remote_dir=f.file_dir, local_dir=local_dir)])
        self._own(f)

        cancel_and_delete([f], ctx)

        self.assertFalse(os.path.isdir(local_dir))

    def test_never_deletes_outside_download_root(self):
        from lib.slskd_transfers import cancel_and_delete
        ctx, _, _ = self._ctx()
        outside = tempfile.mkdtemp(prefix="cratedigger-outside-")
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        stray = os.path.join(outside, "01 - Track.mp3")
        with open(stray, "w") as fp:
            fp.write("x")
        f = make_download_file()
        f.local_path = stray
        self._own(f)  # owned, so root containment is the check under test

        cancel_and_delete([f], ctx)

        self.assertTrue(os.path.exists(stray))

    def test_cancel_failure_continues(self):
        """Should not raise if cancel_download throws."""
        from lib.slskd_transfers import cancel_and_delete
        slskd = FakeSlskdAPI()
        slskd.transfers.cancel_download_error = Exception("network error")
        ctx, slskd, _ = self._ctx(slskd)
        f = make_download_file()
        self._own(f)
        with self.assertLogs("cratedigger", level="WARNING") as logs:
            ok = cancel_and_delete([f], ctx)  # should not raise
        self.assertFalse(ok)
        self.assertIn("Failed to cancel download", "\n".join(logs.output))
        self.assertEqual(
            [(call.username, call.id)
             for call in slskd.transfers.cancel_download_calls],
            [("user1", "file-id-1")],
        )

    def test_foreign_completion_at_the_same_key_is_never_destroyed(self):
        """#1278: the payload behind an UNOWNED queue key belongs to
        someone else on a shared slskd, and root containment cannot say
        so — a foreign client's download lands under the same root.

        ``recent_completion_paths`` reads the instance-wide events feed
        keyed on ``(username, filename)``, so when our POST for that key
        was rejected and a human's succeeded, the fresh lookup hands us
        THEIR local path. Before this gate, cleanup deleted it.
        """
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_dir = os.path.join(tmpdir, "Album Folder")
        os.makedirs(local_dir)
        theirs = os.path.join(local_dir, "01 - Track.mp3")
        with open(theirs, "w") as fp:
            fp.write("x")
        f = make_download_file(
            filename="someuser\\Music\\Album Folder\\01 - Track.mp3",
            file_dir="someuser\\Music\\Album Folder")
        slskd.events.set_events([self._file_event(
            slskd, id="ev-1", username=f.username, filename=f.filename,
            local_filename=theirs)])
        # Deliberately NOT ledgered as ours: our POST never got accepted.

        with self.assertLogs("cratedigger", level="WARNING") as logs:
            ok = cancel_and_delete([f], ctx)

        self.assertTrue(os.path.exists(theirs))
        self.assertTrue(os.path.isdir(local_dir))
        self.assertEqual(slskd.transfers.cancel_download_calls, [])
        self.assertFalse(ok)
        self.assertIn("not ledger-owned", "\n".join(logs.output))

    def test_pending_intent_alone_is_not_destructive_authority(self):
        """A write-ahead row records that we ASKED, never that slskd
        agreed. Only ``confirm_transfer_enqueue`` grants authority —
        the same boundary ``stamp_transfer_completion`` enforces."""
        from lib.pipeline_db import TransferLedgerRow
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_path = os.path.join(tmpdir, "01 - Track.mp3")
        with open(local_path, "w") as fp:
            fp.write("x")
        f = make_download_file()
        f.local_path = local_path
        self.ledger_db.record_transfer_enqueue([TransferLedgerRow(
            request_id=1, username=f.username, filename=f.filename)])

        cancel_and_delete([f], ctx)

        self.assertTrue(os.path.exists(local_path))
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_no_ownership_collaborator_destroys_nothing(self):
        """The fail-closed arm the docstring calls load-bearing.

        Independent review (#1278) found this arm had zero coverage AND
        was reachable in production at the same time: `cratedigger.py`'s
        Phase-1 poll context did not forward `download_ownership`, so
        every download-timeout cleanup took exactly this path. A mutant
        flipping it to `return list(files)` — destroy everything when we
        cannot ask — survived the entire suite.
        """
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_path = os.path.join(tmpdir, "01 - Track.mp3")
        with open(local_path, "w") as fp:
            fp.write("x")
        f = make_download_file()
        f.local_path = local_path
        self._own(f)  # owned — but with no collaborator we cannot know it
        rebind_collaborators(ctx, download_ownership=None)

        with self.assertLogs("cratedigger", level="WARNING") as logs:
            ok = cancel_and_delete([f], ctx)

        self.assertTrue(os.path.exists(local_path))
        self.assertEqual(slskd.transfers.cancel_download_calls, [])
        self.assertFalse(ok)
        self.assertIn(
            "no ownership collaborator wired", "\n".join(logs.output))

    def test_empty_file_list_asks_the_ledger_nothing(self):
        """An empty batch is not an ownership question.

        The early exit is what keeps a caller with nothing to clean up
        from asking the ledger about zero keys, and from opening a DB
        handle to do it. Both halves are asserted, because they are not
        the same claim: ``owned_transfer_keys`` early-returns on empty
        keys BEFORE it opens a handle, so deleting
        ``lib/slskd_transfers.py``'s own ``if not files`` guard would send
        an empty batch all the way into the writer while leaving the
        handle count at zero (#1313 review).
        """
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, _ = self._ctx()
        writer = _RecordingOwnershipWriter(self.ledger_db)
        rebind_collaborators(ctx, download_ownership=writer)

        ok = cancel_and_delete([], ctx)

        self.assertTrue(ok)
        self.assertEqual(writer.asked, [], "the writer is never consulted")
        self.assertEqual(writer.opened, 0, "and no DB handle is opened")
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_empty_file_list_with_no_collaborator_stays_silent(self):
        """...and from accusing the operator about zero files.

        The skipped-keys warning cannot fire on an empty batch in any
        world (`skipped` is `0 - 0`), so only this scenario — no
        collaborator AND nothing to do — makes the "stays silent" half
        of the early exit falsifiable. Asserted separately because a
        single test claiming both was inert on the warning half
        (#1278 review).
        """
        from lib.slskd_transfers import cancel_and_delete
        ctx, _, _ = self._ctx()
        rebind_collaborators(ctx, download_ownership=None)

        with patch.object(logging.getLogger("cratedigger"), "warning") as warn:
            ok = cancel_and_delete([], ctx)

        self.assertTrue(ok)
        warn.assert_not_called()

    def test_owned_and_foreign_files_are_partitioned_in_one_call(self):
        """A mixed batch destroys exactly its owned half — the foreign
        half is neither cancelled nor unlinked."""
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        mine_path = os.path.join(tmpdir, "01 - Mine.mp3")
        theirs_path = os.path.join(tmpdir, "01 - Theirs.mp3")
        for path in (mine_path, theirs_path):
            with open(path, "w") as fp:
                fp.write("x")
        mine = make_download_file(
            filename="someuser\\Mine.mp3", id="mine-id")
        mine.local_path = mine_path
        theirs = make_download_file(
            filename="someuser\\Theirs.mp3", id="theirs-id")
        theirs.local_path = theirs_path
        self._own(mine)

        cancel_and_delete([mine, theirs], ctx)

        self.assertFalse(os.path.exists(mine_path))
        self.assertTrue(os.path.exists(theirs_path))
        self.assertEqual(
            [call.id for call in slskd.transfers.cancel_download_calls],
            ["mine-id"],
        )

    def test_ownership_lookup_failure_destroys_nothing(self):
        """Fail closed: an unavailable ledger is not evidence of
        ownership, so a DB failure skips destruction (the reaper remains
        the backstop) instead of falling back to root containment."""
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_path = os.path.join(tmpdir, "01 - Track.mp3")
        with open(local_path, "w") as fp:
            fp.write("x")
        f = make_download_file()
        f.local_path = local_path
        self._own(f)

        from lib.download_ownership import DownloadOwnershipWriter

        def _boom() -> FakePipelineDB:
            raise RuntimeError("ledger unavailable")

        # The DB handle itself is what fails in production, so the failure
        # is injected at the writer's own db_factory rather than by
        # replacing its method (#1313).
        rebind_collaborators(ctx, download_ownership=DownloadOwnershipWriter(
            db_factory=_boom, close_after_use=False))

        with self.assertLogs("cratedigger", level="WARNING") as logs:
            ok = cancel_and_delete([f], ctx)

        self.assertTrue(os.path.exists(local_path))
        self.assertEqual(slskd.transfers.cancel_download_calls, [])
        self.assertFalse(ok)
        self.assertIn("ownership lookup failed", "\n".join(logs.output))

    def test_events_lookup_failure_never_blocks_cancel(self):
        from lib.slskd_transfers import cancel_and_delete
        slskd = FakeSlskdAPI()
        slskd.events.list_error = RuntimeError("events down")
        ctx, slskd, _ = self._ctx(slskd)
        f = make_download_file()  # unstamped → triggers the events lookup
        self._own(f)

        ok = cancel_and_delete([f], ctx)  # must not raise

        self.assertTrue(ok)
        self.assertEqual(len(slskd.transfers.cancel_download_calls), 1)


class TestSlskdDoEnqueue(unittest.TestCase):

    def test_successful_enqueue(self):
        from lib.slskd_transfers import slskd_do_enqueue
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "track.mp3", "id": "new-id"}],
            }],
        }])
        ctx = _make_ctx(slskd=slskd)
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "new-id")
        self.assertEqual(slskd.transfers.enqueue_calls[0].files, files)
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True])

    def test_enqueue_failure_returns_none(self):
        from lib.slskd_transfers import slskd_do_enqueue
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = Exception("fail")
        ctx = _make_ctx(slskd=slskd)
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", [], "dir", ctx)
        self.assertIsNone(result)

    def test_enqueue_polls_until_ids_found(self):
        """Transfer IDs appear on 2nd poll — should resolve in 2 iterations, not 5s."""
        from lib.slskd_transfers import slskd_do_enqueue
        snapshot_with_id = [{
            "username": "user1",
            "directories": [{"files": [{"filename": "track.mp3", "id": "tid-1"}]}],
        }]
        snapshot_without_id = [{
            "username": "user1",
            "directories": [{"files": []}],
        }]
        slskd = FakeSlskdAPI(
            download_snapshots=[snapshot_without_id, snapshot_with_id])
        ctx = _make_ctx(slskd=slskd)
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "tid-1")
        # Should have polled twice
        self.assertEqual(len(slskd.transfers.get_all_downloads_calls), 2)

    def test_enqueue_timeout_tracks_files_without_transfer_ids(self):
        """Transfer IDs never appear — accepted enqueue is tracked for re-derivation."""
        from lib.slskd_transfers import slskd_do_enqueue
        # Never returns the transfer ID
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"files": []}],
        }])
        ctx = _make_ctx(slskd=slskd)
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].filename, "track.mp3")
        self.assertEqual(result[0].id, "")

    def test_enqueue_partial_transfer_reconciliation_tracks_every_file(self):
        """Accepted enqueue with partial IDs still tracks every requested file."""
        from lib.slskd_transfers import slskd_do_enqueue
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "track1.mp3", "id": "new-id"}],
            }],
        }])
        ctx = _make_ctx(slskd=slskd)
        files = [
            {"filename": "track1.mp3", "size": 5000000},
            {"filename": "track2.mp3", "size": 5000000},
        ]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual([download.filename for download in result], [
            "track1.mp3",
            "track2.mp3",
        ])
        self.assertEqual([download.id for download in result], ["new-id", ""])
        self.assertEqual(slskd.transfers.cancel_download_calls, [])


class TestSlskdEnqueueWithOutcome(unittest.TestCase):
    """slskd_enqueue_with_outcome must distinguish a hard 'user offline'
    HTTP rejection (verifiable, transfers will never appear) from a
    generic exception (transient, retry next cycle)."""

    def _make_offline_http_error(self, body: str) -> Exception:
        """Build a ``requests.HTTPError`` whose ``.response.text`` mirrors
        what slskd returns when the peer is offline. The detector matches
        structurally on ``.response.text``."""
        return make_requests_http_error(body)

    def _make_http_error(self, body: str, status_code: int = 500) -> Exception:
        """Build a ``requests.HTTPError`` with a full response (body +
        status code) for the enqueue-failure reason-extraction tests."""
        return make_requests_http_error(body, status_code=status_code)

    def test_offline_http_error_body_returns_rejected(self):
        """The canonical slskd response body 'User pooyork appears to be
        offline' must be classified as rejected — the safety net that
        unblocks the 60s vanish timeout."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_offline_http_error(
            "User pooyork appears to be offline")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "pooyork", [{"filename": "track.flac", "size": 1}],
                "pooyork\\Music", ctx)

        self.assertEqual(outcome.status, "rejected")
        self.assertIsNone(outcome.downloads)

    def test_offline_http_error_carries_offline_reason(self):
        """Issue #564 C4: the offline classification carries a stable,
        human-readable reason regardless of the raw response body text."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_offline_http_error(
            "User pooyork appears to be offline")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "pooyork", [{"filename": "track.flac", "size": 1}],
                "pooyork\\Music", ctx)

        self.assertEqual(outcome.reason, "peer appears to be offline")

    def test_offline_http_error_case_insensitive(self):
        """Body match must tolerate slskd-version body casing variants."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_offline_http_error(
            "User FOO Appears To BE OFFLINE")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "foo", [{"filename": "t.flac", "size": 1}], "foo\\m", ctx)

        self.assertEqual(outcome.status, "rejected")

    def test_non_offline_http_error_returns_unknown(self):
        """HTTPError without the offline marker is still unknown — the
        request stays downloading for poll-cycle recovery."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_offline_http_error(
            "internal server error")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.status, "unknown")

    def test_non_offline_http_error_carries_response_body_as_reason(self):
        """Issue #564 C4: the real slskd body (e.g. a
        DownloadEnqueueException message) is the reason, not the
        discarded default."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_http_error(
            "Soulseek.DownloadEnqueueException: File not shared.")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(
            outcome.reason,
            "Soulseek.DownloadEnqueueException: File not shared.")

    def test_html_error_body_falls_back_to_status_code_message(self):
        """An HTML error page body isn't a usable reason — fall back to a
        generic HTTP-status message."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_http_error(
            "<html><body>502 Bad Gateway</body></html>", status_code=502)
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.reason, "slskd enqueue failed (HTTP 502)")

    def test_overlong_error_body_falls_back_to_status_code_message(self):
        """An implausibly long body isn't a short exception message —
        fall back rather than storing a huge blob."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_http_error(
            "x" * 600, status_code=500)
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.reason, "slskd enqueue failed (HTTP 500)")

    def test_http_error_with_no_response_returns_unknown(self):
        """Defensive: HTTPError without an attached response should not
        crash — fall through to unknown."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = Exception("synthetic — no .response")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.status, "unknown")
        self.assertEqual(outcome.reason, "synthetic — no .response")

    def test_connection_error_returns_unknown(self):
        """Generic non-HTTPError exceptions (network drop, etc.) stay in
        the unknown / ambiguous bucket — only the verifiable user-offline
        body promotes to rejected."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome

        class _ConnectionError(Exception):
            """Synthetic stand-in. ``test_beets_validation.py`` mocks
            ``sys.modules['requests']`` so importing real
            ``requests.ConnectionError`` here yields a non-class type."""

        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = _ConnectionError("dropped")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.status, "unknown")
        self.assertEqual(outcome.reason, "dropped")

    def test_falsy_enqueue_response_still_returns_rejected(self):
        """Regression guard: when slskd-api ever returns falsy (rather
        than raising), the existing rejected branch still fires."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_result = False
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.status, "rejected")

    def test_successful_enqueue_still_returns_accepted(self):
        """Regression guard: the happy path is unchanged."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "track.mp3", "id": "tid-1"}],
            }],
        }])
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "track.mp3", "size": 5000000}],
                "user1\\Music", ctx)

        self.assertEqual(outcome.status, "accepted")
        self.assertIsNotNone(outcome.downloads)
        assert outcome.downloads is not None
        self.assertEqual(outcome.downloads[0].id, "tid-1")

    def _duplicate_key_snapshot(self) -> FakeSlskdAPI:
        """Two records for the SAME (username, filename) queue key: a
        stale Succeeded record from a much older attempt, and the
        current attempt's genuine Errored record — the exact #820 shape,
        reused here for reconciliation (issue #822 item 3)."""
        return FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [
                    {
                        "filename": "shared\\01.flac",
                        "id": "stale-succeeded",
                        "state": "Completed, Succeeded",
                        "endedAt": "2026-01-01T00:00:00+00:00",
                    },
                    {
                        "filename": "shared\\01.flac",
                        "id": "current-errored",
                        "state": "Completed, Errored",
                        "endedAt": "2026-06-01T00:00:10+00:00",
                    },
                ],
            }],
        }])

    def test_not_before_omitted_can_reconcile_to_stale_prior_attempt_id(self):
        """Documents the accepted default-branch risk (issue #822 item 3
        review): without a not_before boundary, reconciliation can pick a
        stale prior-attempt id over the genuine current one. The #821
        review found the worst case is a transient stale id whose cancel
        is a no-op, corrected next poll — defensible, but this pin makes
        the risk visible rather than implicit."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = self._duplicate_key_snapshot()
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "shared\\01.flac", "size": 100}],
                "user1\\Music", ctx)

        assert outcome.downloads is not None
        self.assertEqual(outcome.downloads[0].id, "stale-succeeded")

    def test_not_before_scopes_reconciliation_to_the_current_attempt(self):
        """The fix half of the pin above: passing not_before (threaded
        from claim.enqueued_at / state.enqueued_at in production, issue
        #822 item 3) excludes the stale pre-boundary record and
        reconciles to the current attempt's own id."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = self._duplicate_key_snapshot()
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "shared\\01.flac", "size": 100}],
                "user1\\Music", ctx,
                not_before="2026-06-01T00:00:00+00:00",
            )

        assert outcome.downloads is not None
        self.assertEqual(outcome.downloads[0].id, "current-errored")


class TestDownloadOwnershipWriterFence(unittest.TestCase):
    def test_expected_witness_is_independent_of_outgoing_state(self):
        from lib.download_ownership import DownloadOwnershipWriter

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "attempt-a",
                "files": [],
            },
        ))
        writer = DownloadOwnershipWriter(db_factory=lambda: db)

        applied = writer.update_state_if_downloading(
            42,
            '{"filetype":"mp3","enqueued_at":"attempt-b","files":[]}',
            expected_enqueued_at="attempt-a",
        )

        self.assertFalse(applied)
        self.assertEqual(
            db.request(42)["active_download_state"],
            {
                "filetype": "flac",
                "enqueued_at": "attempt-a",
                "files": [],
            },
        )


class TestTransferLedgerWriteAheadOrdering(unittest.TestCase):
    """T1 pin (issue #571): slskd_enqueue_with_outcome -- the ONE
    production call site of ctx.slskd.transfers.enqueue -- ledgers every
    file BEFORE issuing the POST. Order-recording fakes, same shape as
    the search ledger's I2 pins in tests/test_slskd_searches.py."""

    def _ctx_with_ownership(self, db, slskd):
        from lib.download_ownership import DownloadOwnershipWriter
        ctx = _make_ctx(slskd=slskd)
        rebind_collaborators(
            ctx,
            download_ownership=DownloadOwnershipWriter(db_factory=lambda: db),
        )
        return ctx

    def test_ledger_insert_precedes_the_post(self):
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        from tests.fakes import FakePipelineDB

        order: list[str] = []
        db = FakePipelineDB()
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [
                    {"filename": "01.flac", "id": "tid-1"},
                    {"filename": "02.flac", "id": "tid-2"},
                ],
            }],
        }])
        ctx = self._ctx_with_ownership(db, slskd)

        real_record = db.record_transfer_enqueue

        def recording_record(rows):
            order.append(f"ledger:{len(rows)}")
            return real_record(rows)

        db.record_transfer_enqueue = recording_record

        real_enqueue = slskd.transfers.enqueue

        def recording_enqueue(*, username, files):
            order.append(f"post:{len(files)}")
            return real_enqueue(username=username, files=files)

        slskd.transfers.enqueue = recording_enqueue  # type: ignore[method-assign]

        files = [
            {"filename": "01.flac", "size": 1},
            {"filename": "02.flac", "size": 2},
        ]
        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", files, "user1\\Music", ctx,
                request_id=42, attempt_fp="fp-1")

        self.assertEqual(outcome.status, "accepted")
        self.assertEqual(order, ["ledger:2", "post:2"])
        rows = db.record_transfer_enqueue_calls
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {r.filename for r in rows}, {"01.flac", "02.flac"})
        for row in rows:
            self.assertEqual(row.attempt_fingerprint, "fp-1")
            self.assertEqual(row.request_id, 42)
        self.assertEqual(
            db.get_owned_transfer_keys(),
            {("user1", "01.flac"), ("user1", "02.flac")},
        )

    def test_ledger_row_survives_a_simulated_kill_at_the_post(self):
        """Kill-safety: the POST raising AFTER the ledger write still
        leaves a durable ownership row -- proves T1 holds even when the
        enqueue call itself fails/dies."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        from tests.fakes import FakePipelineDB

        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = RuntimeError("simulated kill mid-POST")
        ctx = self._ctx_with_ownership(db, slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "a.flac", "size": 1}],
                "user1\\Music", ctx, request_id=7)

        self.assertEqual(outcome.status, "unknown")
        rows = db.record_transfer_enqueue_calls
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].filename, "a.flac")
        self.assertEqual(db.get_owned_transfer_keys(), set())

    def test_definitive_rejection_never_owns_a_later_manual_transfer(self):
        """A rejected POST leaves intent evidence, not destructive authority."""
        from lib.slskd_events import ingest_download_file_events
        from lib.slskd_transfers import (
            purge_completed_transfers,
            slskd_enqueue_with_outcome,
        )
        from tests.fakes import FakePipelineDB
        from tests.helpers import make_file_complete_event_data

        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        error = make_requests_http_error(
            "User peer1 appears to be offline"
        )
        slskd.transfers.enqueue_error = error
        ctx = self._ctx_with_ownership(db, slskd)
        filename = "Music\\Album\\01.flac"

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "peer1",
                [{"filename": filename, "size": 1}],
                "Music\\Album",
                ctx,
                request_id=7,
            )

        self.assertEqual(outcome.status, "rejected")
        self.assertEqual(len(db._transfer_ledger), 1)
        self.assertEqual(db.get_owned_transfer_keys(), set())

        slskd.transfers.enqueue_error = None
        slskd.add_transfer(
            username="peer1",
            directory="Music\\Album",
            filename=filename,
            id="human-later",
            state="Completed, Succeeded",
        )
        db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")
        slskd.events.set_events([
            slskd.events.make_event(
                id="ev-human",
                timestamp="2026-07-01T10:00:00.0000000Z",
                type="DownloadFileComplete",
                data=make_file_complete_event_data(
                    username="peer1",
                    filename=filename,
                    local_filename="/downloads/manual/01.flac",
                ),
            ),
            slskd.events.make_event(
                id="ev-cursor",
                timestamp="2026-07-01T00:00:00.0000000Z",
                type="Noise",
                data="{}",
            ),
        ])

        ingest = ingest_download_file_events(db, slskd)

        self.assertEqual(ingest.transfers_stamped, 0)
        pending = next(iter(db._transfer_ledger.values()))
        self.assertIsNone(pending.accepted_at)
        self.assertIsNone(pending.local_path)

        summary = purge_completed_transfers(ctx)

        self.assertEqual(summary.removed, 0)
        self.assertEqual(summary.foreign_count, 1)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_no_request_id_skips_ledger_but_still_enqueues(self):
        """Documents the guard: the legacy/test fallback shape (no
        ownership context) never blocks the enqueue -- it just can't be
        ledgered, matching _claim_initial_download_ownership's own
        request_id-is-None carve-out.

        #1278 item 2: the CONFIRM half now skips identically. Writing no
        row and then confirming anyway was the latent hazard -- with no
        request to scope to, the confirm reached for whatever pending row
        the key carried, which can only be ANOTHER request's. The sibling
        pending row below is the world that made that visible; it must
        still be pending afterwards.
        """
        from lib.pipeline_db import TransferLedgerRow
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        from tests.fakes import FakePipelineDB

        db = FakePipelineDB()
        db.record_transfer_enqueue([TransferLedgerRow(
            request_id=4242, username="user1", filename="a.flac")])
        # The recorder is the assertion below; drop the fixture's own
        # seeding call so it only reports what the enqueue path wrote.
        db.record_transfer_enqueue_calls.clear()
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "a.flac", "id": "tid-1"}],
            }],
        }])
        ctx = self._ctx_with_ownership(db, slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "a.flac", "size": 1}],
                "user1\\Music", ctx, request_id=None)

        self.assertEqual(outcome.status, "accepted")
        self.assertEqual(db.record_transfer_enqueue_calls, [])
        # Not "asked and got nothing back" -- never asked at all. The
        # scoping in confirm_transfer_enqueue would make an unscoped ask
        # inert anyway, so only the call recorder distinguishes the two.
        self.assertEqual(db.confirm_transfer_enqueue_calls, [])
        self.assertEqual(db.get_owned_transfer_keys(), set())
        sibling_row = next(iter(db._transfer_ledger.values()))
        self.assertEqual(sibling_row.request_id, 4242)
        self.assertIsNone(sibling_row.accepted_at)

    def test_no_download_ownership_skips_ledger_but_still_enqueues(self):
        from lib.slskd_transfers import slskd_enqueue_with_outcome

        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "a.flac", "id": "tid-1"}],
            }],
        }])
        ctx = _make_ctx(slskd=slskd)  # download_ownership stays None

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "a.flac", "size": 1}],
                "user1\\Music", ctx, request_id=99)

        self.assertEqual(outcome.status, "accepted")

    def test_empty_files_list_writes_no_ledger_row(self):
        from lib.slskd_transfers import slskd_do_enqueue
        from tests.fakes import FakePipelineDB

        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        ctx = self._ctx_with_ownership(db, slskd)

        with patch("time.sleep"):
            slskd_do_enqueue("user1", [], "dir", ctx, request_id=1)

        self.assertEqual(db.record_transfer_enqueue_calls, [])

class TestGrabMostWanted(unittest.TestCase):
    """grab_most_wanted summarizes worker-owned enqueue results."""

    def test_no_albums_returns_zero(self):
        from lib.download import grab_most_wanted
        ctx = _make_ctx()
        search_fn = MagicMock(return_value=({}, [], []))
        count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 0)

    def test_failed_search_counted(self):
        from album_source import AlbumRecord
        from lib.download import grab_most_wanted
        ctx = _make_ctx()
        failed_album = AlbumRecord(
            id=-1, title="Album", release_date="2024-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[], db_request_id=1, db_source="request",
            db_mb_release_id="", db_search_filetype_override=None, db_target_format=None,
        )
        search_fn = MagicMock(return_value=({}, [failed_album], []))
        count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 1)

    def test_failed_grab_counted(self):
        from album_source import AlbumRecord
        from lib.download import grab_most_wanted
        ctx = _make_ctx()
        failed_album = AlbumRecord(
            id=-1, title="Album", release_date="2024-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[], db_request_id=1, db_source="request",
            db_mb_release_id="", db_search_filetype_override=None, db_target_format=None,
        )
        search_fn = MagicMock(return_value=({}, [], [failed_album]))
        count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 1)

    def test_does_not_claim_download_ownership(self):
        """The search worker must have claimed before reporting the result."""
        from lib.download import grab_most_wanted
        entry = make_grab_list_entry(
            album_id=1,
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            db_request_id=42,
            db_source="request",
            files=[make_download_file(
                filename="u\\M\\01.flac",
                id="tid-1",
                file_dir="u\\M",
                username="user1",
                size=30000000,
            )],
        )
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=42, status="wanted"))
        ctx = make_ctx_with_fake_db(fake_db)
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        grab_most_wanted([], search_fn, ctx)
        row = fake_db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["active_download_state"])
        self.assertEqual(fake_db.status_history, [])

    def test_no_blocking_monitor(self):
        """grab_most_wanted returns immediately without blocking."""
        import time as _time

        from lib.download import grab_most_wanted
        entry = make_grab_list_entry(
            album_id=1,
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            db_request_id=42,
            db_source="request",
            files=[make_download_file(
                filename="u\\M\\01.flac",
                id="tid-1",
                file_dir="u\\M",
                username="user1",
                size=30000000,
            )],
        )
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=42, status="wanted"))
        ctx = make_ctx_with_fake_db(fake_db)
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        start = _time.time()
        grab_most_wanted([], search_fn, ctx)
        elapsed = _time.time() - start
        self.assertLess(elapsed, 2.0)  # Must return fast (no blocking loop)


class TestMatchTransferId(unittest.TestCase):
    """Test _match_transfer_id() — find slskd transfer ID by filename."""

    def test_exact_filename_match(self):
        from lib.slskd_transfers import _match_transfer_id
        downloads = make_download_user(directories=[
            make_download_directory(directory="user\\Music", files=[
                make_transfer_snapshot(filename="user\\Music\\01.flac", id="abc-123"),
                make_transfer_snapshot(filename="user\\Music\\02.flac", id="def-456"),
            ]),
        ])
        result = _match_transfer_id(downloads, "user\\Music\\01.flac")
        self.assertEqual(result, "abc-123")

    def test_not_found(self):
        from lib.slskd_transfers import _match_transfer_id
        downloads = make_download_user(directories=[
            make_download_directory(directory="user\\Music", files=[]),
        ])
        result = _match_transfer_id(downloads, "user\\Music\\missing.flac")
        self.assertIsNone(result)

    def test_multi_directory(self):
        from lib.slskd_transfers import _match_transfer_id
        downloads = make_download_user(directories=[
            make_download_directory(directory="d1", files=[
                make_transfer_snapshot(filename="d1\\01.flac", id="id-1"),
            ]),
            make_download_directory(directory="d2", files=[
                make_transfer_snapshot(filename="d2\\01.flac", id="id-2"),
            ]),
        ])
        result = _match_transfer_id(downloads, "d2\\01.flac")
        self.assertEqual(result, "id-2")

    def test_bulk_downloads_respects_username(self):
        from lib.slskd_transfers import _match_transfer_id
        downloads = [
            make_download_user(username="Mr. Odd", directories=[
                make_download_directory(directory="a", files=[
                    make_transfer_snapshot(filename="shared\\01.flac", id="wrong-id"),
                ]),
            ]),
            make_download_user(username="Miick Starr", directories=[
                make_download_directory(directory="b", files=[
                    make_transfer_snapshot(filename="shared\\01.flac", id="right-id"),
                ]),
            ]),
        ]
        result = _match_transfer_id(
            downloads,
            "shared\\01.flac",
            username="Miick Starr",
        )
        self.assertEqual(result, "right-id")

    def test_not_before_omitted_reaches_all_history_matching(self):
        """Default (no not_before) still reaches the private all-history
        walk (issue #822 item 1/3) — a stale prior-attempt Succeeded
        record can still outrank a fresher Errored one, exactly as
        ``_match_transfer_all_history`` alone would. This is the
        documented, still-accepted default for callers with no attempt
        boundary available (see ``test_not_before_scopes_to_attempt_boundary``
        below for the scoped alternative)."""
        from lib.slskd_transfers import _match_transfer_id
        downloads = make_download_user(username="user1", directories=[
            make_download_directory(directory="d", files=[
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="stale-succeeded",
                    state="Completed, Succeeded",
                    ended_at="2026-01-01T00:00:00+00:00",
                ),
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="current-errored",
                    state="Completed, Errored",
                    ended_at="2026-06-01T00:00:10+00:00",
                ),
            ]),
        ])
        result = _match_transfer_id(downloads, "shared\\01.flac", username="user1")
        self.assertEqual(result, "stale-succeeded")

    def test_not_before_scopes_to_attempt_boundary(self):
        """Passing not_before (issue #822 item 3) routes to
        ``match_transfer_for_attempt`` and excludes the stale pre-boundary
        record the default branch above returns."""
        from lib.slskd_transfers import _match_transfer_id
        downloads = make_download_user(username="user1", directories=[
            make_download_directory(directory="d", files=[
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="stale-succeeded",
                    state="Completed, Succeeded",
                    ended_at="2026-01-01T00:00:00+00:00",
                ),
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="current-errored",
                    state="Completed, Errored",
                    ended_at="2026-06-01T00:00:10+00:00",
                ),
            ]),
        ])
        result = _match_transfer_id(
            downloads, "shared\\01.flac", username="user1",
            not_before="2026-06-01T00:00:00+00:00",
        )
        self.assertEqual(result, "current-errored")


class TestMatchTransferAllHistory(unittest.TestCase):
    """Test _match_transfer_all_history() — the private, deliberately
    narrow all-history walk behind _match_transfer_id's default branch
    (issue #822 item 1; renamed from the formerly-public match_transfer)."""

    def test_bulk_downloads_prefers_active_over_old_completed(self):
        from lib.slskd_transfers import _match_transfer_all_history
        downloads = [
            make_download_user(username="user1", directories=[
                make_download_directory(directory="d", files=[
                    make_transfer_snapshot(
                        filename="shared\\01.flac",
                        id="completed-id",
                        state="Completed, Succeeded",
                        ended_at="2026-04-03T21:00:00+00:00",
                    ),
                    make_transfer_snapshot(
                        filename="shared\\01.flac",
                        id="active-id",
                        state="InProgress",
                        started_at="2026-04-03T22:00:00+00:00",
                    ),
                ]),
            ]),
        ]
        result = _match_transfer_all_history(
            downloads, "shared\\01.flac", username="user1")
        assert result is not None
        self.assertEqual(result.id, "active-id")

    def test_bulk_downloads_prefers_latest_successful_attempt(self):
        from lib.slskd_transfers import _match_transfer_all_history
        downloads = [
            make_download_user(username="user1", directories=[
                make_download_directory(directory="d", files=[
                    make_transfer_snapshot(
                        filename="shared\\01.flac",
                        id="old-cancelled",
                        state="Completed, Cancelled",
                        ended_at="2026-04-03T20:00:00+00:00",
                    ),
                    make_transfer_snapshot(
                        filename="shared\\01.flac",
                        id="new-succeeded",
                        state="Completed, Succeeded",
                        ended_at="2026-04-03T21:00:00+00:00",
                    ),
                ]),
            ]),
        ]
        result = _match_transfer_all_history(
            downloads, "shared\\01.flac", username="user1")
        assert result is not None
        self.assertEqual(result.id, "new-succeeded")


class TestMatchTransferForAttempt(unittest.TestCase):
    """Test match_transfer_for_attempt() — issue #820: a stale prior-attempt
    terminal record for the SAME (username, filename) queue key must never
    shadow, nor silently suppress, the current attempt's genuine transfer.
    """

    def test_no_survivors_returns_none(self):
        """Only a pre-boundary terminal record exists — a genuinely
        vanished current attempt still returns None (must-still-work
        guard: this is NOT a regression to fall back on)."""
        from lib.slskd_transfers import match_transfer_for_attempt
        downloads = make_download_user(username="user1", directories=[
            make_download_directory(directory="d", files=[
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="old-completed",
                    state="Completed, Succeeded",
                    ended_at="2026-04-03T21:00:00+00:00",
                ),
            ]),
        ])
        result = match_transfer_for_attempt(
            downloads, "shared\\01.flac", username="user1",
            not_before="2026-04-03T22:00:00+00:00",
        )
        self.assertIsNone(result)

    def test_active_current_transfer_outranks_stale_terminal_record(self):
        """Must-still-work guard: an ACTIVE (non-terminal) current
        transfer still outranks any terminal record, stale or not."""
        from lib.slskd_transfers import match_transfer_for_attempt
        downloads = make_download_user(username="user1", directories=[
            make_download_directory(directory="d", files=[
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="old-completed",
                    state="Completed, Succeeded",
                    ended_at="2026-04-03T21:00:00+00:00",
                ),
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="active-id",
                    state="InProgress",
                    started_at="2026-04-03T22:30:00+00:00",
                ),
            ]),
        ])
        result = match_transfer_for_attempt(
            downloads, "shared\\01.flac", username="user1",
            not_before="2026-04-03T22:00:00+00:00",
        )
        assert result is not None
        self.assertEqual(result.id, "active-id")

    def test_succeeded_still_outranks_errored_within_one_attempt(self):
        """Must-still-work guard: within ONE attempt (both post-boundary),
        success-preference is unchanged — a Succeeded record still
        outranks an Errored duplicate for the same key."""
        from lib.slskd_transfers import match_transfer_for_attempt
        downloads = make_download_user(username="user1", directories=[
            make_download_directory(directory="d", files=[
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="retry-errored",
                    state="Completed, Errored",
                    ended_at="2026-04-03T22:10:00+00:00",
                ),
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="retry-succeeded",
                    state="Completed, Succeeded",
                    ended_at="2026-04-03T22:20:00+00:00",
                ),
            ]),
        ])
        result = match_transfer_for_attempt(
            downloads, "shared\\01.flac", username="user1",
            not_before="2026-04-03T22:00:00+00:00",
        )
        assert result is not None
        self.assertEqual(result.id, "retry-succeeded")

    def test_stale_succeeded_never_shadows_genuine_post_boundary_errored(self):
        """I2 core pin (issue #820): a stale pre-boundary
        Completed,Succeeded record must never shadow — nor, via the old
        'rank first, staleness-check only the winner' bug, silently
        suppress into None — the current attempt's genuine post-boundary
        Completed,Errored record. Real production values: request 4190
        track 09 (HumDrum, The Pictures - Pieces of Eight, 2026-07-22)."""
        from lib.slskd_transfers import match_transfer_for_attempt
        filename = (
            "@@fdcrt\\POWER POP - Tagged\\Pictures, The - "
            "Pieces Of Eight (2005)\\09 - Downhill From Here.mp3"
        )
        downloads = make_download_user(username="HumDrum", directories=[
            make_download_directory(
                directory="@@fdcrt\\POWER POP - Tagged\\Pictures, The - "
                          "Pieces Of Eight (2005)",
                files=[
                    make_transfer_snapshot(
                        filename=filename,
                        id="stale-may-id",
                        state="Completed, Succeeded",
                        requested_at="2026-05-18T23:01:32+00:00",
                        ended_at="2026-05-18T23:04:58+00:00",
                        bytes_transferred=5274623,
                    ),
                    make_transfer_snapshot(
                        filename=filename,
                        id="current-errored-id",
                        state="Completed, Errored",
                        requested_at="2026-07-22T02:01:26.725+00:00",
                        started_at="2026-07-22T02:05:00.222+00:00",
                        ended_at="2026-07-22T02:05:00.222+00:00",
                        bytes_transferred=0,
                        exception=(
                            "Download of 09 - Downhill From Here.mp3 "
                            "reported as failed by HumDrum"
                        ),
                    ),
                ],
            ),
        ])
        result = match_transfer_for_attempt(
            downloads, filename, username="HumDrum",
            not_before="2026-07-22T02:01:25.759358+00:00",
        )
        assert result is not None
        self.assertEqual(result.id, "current-errored-id")
        self.assertEqual(result.state, "Completed, Errored")
        self.assertEqual(
            result.exception,
            "Download of 09 - Downhill From Here.mp3 reported as failed "
            "by HumDrum",
        )

    def test_timestampless_terminal_record_excluded_fail_closed(self):
        """Issue #822 item 2: a terminal record with ZERO parseable
        lifecycle timestamps cannot prove it belongs to this attempt and
        must not bind to it (fail-closed) -- even though its bare
        "Completed, Succeeded" state would otherwise outrank a genuine
        post-boundary Errored record on priority alone. Pre-fix,
        ``_is_terminal_transfer_before`` special-cased ``latest_ts ==
        datetime.min`` to return False (survives filtering); this is the
        exact shape that let it launder past attempt-boundary scoping."""
        from lib.slskd_transfers import match_transfer_for_attempt
        downloads = make_download_user(username="user1", directories=[
            make_download_directory(directory="d", files=[
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="timestampless-succeeded",
                    state="Completed, Succeeded",
                    # No requested_at/started_at/ended_at/enqueued_at --
                    # nothing proves which attempt this belongs to.
                ),
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="current-errored",
                    state="Completed, Errored",
                    ended_at="2026-07-22T02:05:00+00:00",
                ),
            ]),
        ])
        result = match_transfer_for_attempt(
            downloads, "shared\\01.flac", username="user1",
            not_before="2026-07-22T02:01:25+00:00",
        )
        assert result is not None
        self.assertEqual(result.id, "current-errored")

    def test_timestampless_terminal_record_alone_returns_none(self):
        """Must-still-work guard: when the ONLY candidate is a
        timestampless terminal record, the matcher returns None -- same
        as any other zero-survivor world -- never the unprovable
        candidate itself."""
        from lib.slskd_transfers import match_transfer_for_attempt
        downloads = make_download_user(username="user1", directories=[
            make_download_directory(directory="d", files=[
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="timestampless-succeeded",
                    state="Completed, Succeeded",
                ),
            ]),
        ])
        result = match_transfer_for_attempt(
            downloads, "shared\\01.flac", username="user1",
            not_before="2026-07-22T02:01:25+00:00",
        )
        self.assertIsNone(result)

    def test_timestampless_non_terminal_record_still_survives(self):
        """Must-still-work guard: non-terminal records are untouched by
        this fix -- they always survive attempt-boundary filtering
        regardless of timestamps, exactly as before."""
        from lib.slskd_transfers import match_transfer_for_attempt
        downloads = make_download_user(username="user1", directories=[
            make_download_directory(directory="d", files=[
                make_transfer_snapshot(
                    filename="shared\\01.flac",
                    id="active-no-ts",
                    state="InProgress",
                ),
            ]),
        ])
        result = match_transfer_for_attempt(
            downloads, "shared\\01.flac", username="user1",
            not_before="2026-07-22T02:01:25+00:00",
        )
        assert result is not None
        self.assertEqual(result.id, "active-no-ts")


class TestRederiveTransferIds(unittest.TestCase):
    """Test rederive_transfer_ids() — re-derive IDs from slskd API."""

    def test_updates_files_in_place(self):
        from lib.grab_list import DownloadFile, GrabListEntry
        from lib.slskd_transfers import rederive_transfer_ids
        entry = GrabListEntry(
            album_id=1, files=[
                DownloadFile(filename="u\\M\\01.flac", id="", file_dir="u\\M",
                             username="user1", size=1000),
                DownloadFile(filename="u\\M\\02.flac", id="", file_dir="u\\M",
                             username="user1", size=2000),
            ],
            filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "u\\M", "files": [
                {"filename": "u\\M\\01.flac", "id": "new-id-1"},
                {"filename": "u\\M\\02.flac", "id": "new-id-2"},
            ]}],
        }])
        rederive_transfer_ids(entry, slskd)
        self.assertEqual(entry.files[0].id, "new-id-1")
        self.assertEqual(entry.files[1].id, "new-id-2")
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True])

    def test_missing_transfer_keeps_empty_id(self):
        from lib.grab_list import DownloadFile, GrabListEntry
        from lib.slskd_transfers import rederive_transfer_ids
        entry = GrabListEntry(
            album_id=1, files=[
                DownloadFile(filename="u\\M\\01.flac", id="", file_dir="u\\M",
                             username="user1", size=1000),
            ],
            filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "u\\M", "files": []}],
        }])
        rederive_transfer_ids(entry, slskd)
        self.assertEqual(entry.files[0].id, "")

    def test_uses_bulk_downloads_for_spacey_usernames(self):
        from lib.grab_list import DownloadFile, GrabListEntry
        from lib.slskd_transfers import rederive_transfer_ids
        entry = GrabListEntry(
            album_id=1,
            files=[
                DownloadFile(
                    filename="Miick Starr\\Album\\01.flac",
                    id="",
                    file_dir="Miick Starr\\Album",
                    username="Miick Starr",
                    size=1000,
                ),
                DownloadFile(
                    filename="Mr. Odd\\Album\\01.flac",
                    id="",
                    file_dir="Mr. Odd\\Album",
                    username="Mr. Odd",
                    size=1000,
                ),
            ],
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[
            {
                "username": "Mr. Odd",
                "directories": [{"directory": "Mr. Odd\\Album", "files": [
                    {"filename": "Mr. Odd\\Album\\01.flac", "id": "odd-id"},
                ]}],
            },
            {
                "username": "Miick Starr",
                "directories": [{"directory": "Miick Starr\\Album", "files": [
                    {"filename": "Miick Starr\\Album\\01.flac", "id": "starr-id"},
                ]}],
            },
        ])

        rederive_transfer_ids(entry, slskd)

        self.assertEqual(entry.files[0].id, "starr-id")
        self.assertEqual(entry.files[1].id, "odd-id")

    def test_terminal_snapshot_sets_file_status(self):
        from lib.grab_list import DownloadFile, GrabListEntry
        from lib.slskd_transfers import rederive_transfer_ids
        entry = GrabListEntry(
            album_id=1,
            files=[
                DownloadFile(
                    filename="user1\\Album\\01.flac",
                    id="",
                    file_dir="user1\\Album",
                    username="user1",
                    size=1000,
                ),
            ],
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "user1\\Album", "files": [
                {
                    "filename": "user1\\Album\\01.flac",
                    "id": "done-id",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 1000,
                },
            ]}],
        }])

        rederive_transfer_ids(entry, slskd)

        self.assertEqual(entry.files[0].id, "done-id")
        status = entry.files[0].status
        self.assertIsNotNone(status)
        assert status is not None
        self.assertEqual(status.state, "Completed, Succeeded")

    def test_not_before_ignores_stale_terminal_transfer(self):
        from lib.grab_list import DownloadFile, GrabListEntry
        from lib.slskd_transfers import rederive_transfer_ids
        entry = GrabListEntry(
            album_id=1,
            files=[
                DownloadFile(
                    filename="user1\\Album\\01.flac",
                    id="",
                    file_dir="user1\\Album",
                    username="user1",
                    size=1000,
                ),
            ],
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "user1\\Album", "files": [
                {
                    "filename": "user1\\Album\\01.flac",
                    "id": "old-completed",
                    "state": "Completed, Succeeded",
                    "endedAt": "2026-05-05T01:00:00+00:00",
                },
            ]}],
        }])

        rederive_transfer_ids(
            entry,
            slskd,
            not_before="2026-05-05T02:00:00+00:00",
        )

        self.assertEqual(entry.files[0].id, "")
        self.assertIsNone(entry.files[0].status)


class TestProcessCompletedAlbumReturnOwnership(unittest.TestCase):
    """Test process_completed_album return ownership."""

    def test_zero_sample_flac_is_normalized_only_after_canonical_publish(self):
        """#1062: Beets' later validation receives a ready owned album view."""
        from mediafile import MediaFile

        from lib.download_processing import Completed, process_completed_album
        from lib.media_readiness import flac_total_samples_only_changed, inspect_media
        from tests.audio_fixtures import make_test_flac

        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = os.path.join(tmpdir, "downloads", "Music")
            os.makedirs(source_dir)
            source = os.path.join(source_dir, "01 - Five Suns.flac")
            make_test_flac(source, duration=1)
            source_bytes = bytearray(Path(source).read_bytes())
            packed = int.from_bytes(source_bytes[18:26], "big")
            source_bytes[18:26] = (packed & ~((1 << 36) - 1)).to_bytes(8, "big")
            source_bytes[26:42] = b"\0" * 16
            Path(source).write_bytes(source_bytes)

            file = make_download_file(
                filename="user1\\Music\\01 - Five Suns.flac",
                file_dir="user1\\Music",
            )
            file.local_path = source
            album = make_grab_list_entry(files=[file], mb_release_id="")
            ctx = _make_ctx()
            for name, value in {
                "slskd_download_dir": os.path.join(tmpdir, "downloads"),
                "beets_validation_enabled": False,
            }.items():
                setattr(ctx.cfg, name, value)

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertIsNotNone(album.import_folder)
            ready = inspect_media(album.import_folder or "").files[0]
            self.assertGreater(ready.duration_seconds, 0)
            canonical = Path(album.import_folder or "") / "01 - Five Suns.flac"
            # This is the same MediaFile reader Beets validation routes through:
            # the historical incident reached a zero local length before it
            # could become a quality/match decision.
            self.assertGreater(MediaFile(str(canonical)).length, 0)
            self.assertTrue(flac_total_samples_only_changed(bytes(source_bytes), canonical.read_bytes()))
            self.assertFalse(Path(source).exists(), "materialization consumes only after canonical publish")

    def test_readiness_audio_corrupt_uses_rejection_owner(self):
        """Corrupt canonical bytes retain the normal audit and peer policy."""
        from lib.config import CratediggerConfig
        from lib.download_processing import (
            CompletionDispatched,
            process_completed_album,
        )
        from tests.audio_fixtures import make_test_flac

        with tempfile.TemporaryDirectory() as tmpdir:
            downloads = os.path.join(tmpdir, "downloads")
            source_dir = os.path.join(downloads, "Music")
            os.makedirs(source_dir)
            source = os.path.join(source_dir, "01 - Track.flac")
            make_test_flac(source, duration=1)
            corrupt = bytearray(Path(source).read_bytes())
            corrupt[-5] ^= 0xFF
            Path(source).write_bytes(corrupt)
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42, status="downloading", artist_name="Artist",
                album_title="Album", year=2024, mb_release_id="test-mbid",
            ))
            cfg = CratediggerConfig(
                slskd_download_dir=downloads,
                beets_validation_enabled=False,
                processing_dir=_private_processing_dir(tmpdir),
                beets_tracking_file=os.path.join(tmpdir, "beets-tracking.jsonl"),
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            file = make_download_file(
                filename="user1\\Music\\01 - Track.flac", file_dir="user1\\Music",
                username="user1",
            )
            file.local_path = source
            album = make_grab_list_entry(
                files=[file], artist="Artist", title="Album", year="2024",
                mb_release_id="test-mbid", db_request_id=42, db_source="request",
            )
            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionDispatched)
            source_db = ctx.pipeline_db_source
            assert isinstance(source_db, FakePipelineDBSource)
            self.assertEqual(len(source_db.reject_and_requeue_calls), 1)
            rejected = source_db.reject_and_requeue_calls[0]["bv_result"]
            self.assertEqual(rejected.scenario, "audio_corrupt")
            self.assertEqual(rejected.denylisted_users, ["user1"])
            # Issue #1077, D3: bad rips are ban + delete, never quarantined.
            # No failed_path means no Wrong Matches worklist row; the
            # canonical processing folder this materialized into is gone
            # outright, not moved to failed_imports/bad_files.
            self.assertIsNone(rejected.failed_path)
            self.assertIsNotNone(album.import_folder)
            self.assertFalse(os.path.exists(album.import_folder or ""))

    def test_zero_byte_mp3_album_uses_audio_corrupt_rejection_owner(self):
        """#1301: the exact sevennorth manifest must ban, delete, and retry."""
        from lib.config import CratediggerConfig
        from lib.download_processing import (
            CompletionDispatched,
            process_completed_album,
        )

        filenames = (
            "01 - sea and the rhythm.mp3",
            "02 - jesus the mexican boy.mp3",
            "03 - red dust.mp3",
            "04 - someday the waves.mp3",
            "05 - overhead.mp3",
            "06 - dead man's will.mp3",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            downloads = os.path.join(tmpdir, "downloads")
            source_dir = os.path.join(downloads, "Summer 2002 Tour EP")
            os.makedirs(source_dir)
            files = []
            for filename in filenames:
                source = os.path.join(source_dir, filename)
                Path(source).write_bytes(b"")
                file = make_download_file(
                    filename=f"sevennorth\\Summer 2002 Tour EP\\{filename}",
                    file_dir="sevennorth\\Summer 2002 Tour EP",
                    username="sevennorth",
                    size=0,
                    bitRate=216,
                )
                file.local_path = source
                files.append(file)

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=8900,
                status="downloading",
                artist_name="Iron & Wine",
                album_title="Summer 2002 Tour EP",
                year=2002,
                mb_release_id="live-release-mbid",
            ))
            cfg = CratediggerConfig(
                slskd_download_dir=downloads,
                beets_validation_enabled=False,
                processing_dir=_private_processing_dir(tmpdir),
                beets_tracking_file=os.path.join(tmpdir, "beets-tracking.jsonl"),
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album = make_grab_list_entry(
                files=files,
                artist="Iron & Wine",
                title="Summer 2002 Tour EP",
                year="2002",
                mb_release_id="live-release-mbid",
                db_request_id=8900,
                db_source="request",
            )

            result = process_completed_album(album, ctx, import_job_id=61316)

            self.assertIsInstance(result, CompletionDispatched)
            source_db = ctx.pipeline_db_source
            assert isinstance(source_db, FakePipelineDBSource)
            self.assertEqual(len(source_db.reject_and_requeue_calls), 1)
            rejected = source_db.reject_and_requeue_calls[0]["bv_result"]
            self.assertEqual(rejected.scenario, "audio_corrupt")
            self.assertEqual(rejected.denylisted_users, ["sevennorth"])
            self.assertIsNone(rejected.failed_path)
            self.assertEqual(db.request(8900)["status"], "wanted")
            self.assertEqual(db.download_logs[-1].outcome, "rejected")
            self.assertFalse(os.path.exists(album.import_folder or ""))

    def test_audio_corrupt_delete_never_removes_the_processing_albums_root(self):
        """Issue #1077, "smalls" (round-2 review): ``_cleanup_staged_dir``'s
        empty-parent prune was written for the nested slskd download-dir
        layout (``Artist/Album/``), where removing an empty ``Artist/``
        after its last album is desired. A canonical processing album is a
        FLAT child of the shared ``<processing_dir>/albums/`` root
        (CLAUDE.md invariant 9), so that root IS the "parent" once the last
        album deletes — and the Nix-provisioned, 0700 root that
        ``open_private_child_directory`` refuses to recreate would be
        ``rmdir``'d right along with it if it ever happened to be empty.
        Today that's shielded only by lock-shard files never being
        unlinked — an incidental side effect, not a guard. This proves the
        real guard: the shared root survives even when it is the ONLY
        album and ends up genuinely empty."""
        from lib.config import CratediggerConfig
        from lib.download_rejection import _handle_rejected_result
        from lib.quality import ValidationResult
        from lib.staged_album import StagedAlbum

        with tempfile.TemporaryDirectory() as tmpdir:
            processing_dir = _private_processing_dir(tmpdir)
            albums_root = os.path.join(processing_dir, "albums")
            current_path = os.path.join(albums_root, "Artist - Album")
            os.makedirs(current_path)
            with open(os.path.join(current_path, "01.flac"), "wb") as handle:
                handle.write(b"corrupt bytes")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=555, status="downloading", mb_release_id="test-mbid-555",
            ))
            cfg = CratediggerConfig(processing_dir=processing_dir)
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album = make_grab_list_entry(
                files=[make_download_file(username="peer555")],
                artist="Artist", title="Album",
                mb_release_id="test-mbid-555", db_request_id=555,
                db_source="request",
            )
            result = ValidationResult(
                valid=False, distance=None, scenario="audio_corrupt",
                detail="garbled",
            )
            staged_album = StagedAlbum(current_path=current_path, request_id=555)

            _handle_rejected_result(album, result, staged_album, ctx)

            self.assertFalse(os.path.exists(current_path))
            self.assertTrue(
                os.path.isdir(albums_root),
                "the shared processing albums root must survive even "
                "though it is now empty — it is a Nix-provisioned root, "
                "not a disposable per-artist directory",
            )
            self.assertEqual(os.listdir(albums_root), [])

    def test_audio_corrupt_delete_never_removes_the_auto_import_root(self):
        """Issue #1122, review round 3 (F-A hold): the processing-albums-
        root pin above proves protected_staging_roots protects
        processing_albums_dir, but a site-local mutant reverting
        lib.download_rejection._checkpoint_then_delete_rejected_source's
        protected_parents back to
        frozenset({processing_albums_dir(processing_dir)}) alone survives
        the entire suite -- nothing exercises this call site with a
        YouTube-shaped current_path. A YouTube rescue's staged audio is
        never materialized under the canonical processing root (it
        imports in place from the auto-import staging child), so
        audio_corrupt can reach this exact function with the auto-import
        root as current_path's parent instead. Same shape as
        TestCleanupStagedDirProtectedRoots.
        test_youtube_shaped_success_path_protects_the_auto_import_root in
        tests/test_import_one_stages.py, but through the real rejection
        path (_handle_rejected_result), not the harness."""
        from lib.config import CratediggerConfig
        from lib.download_rejection import _handle_rejected_result
        from lib.processing_paths import stage_to_ai_root
        from lib.quality import ValidationResult
        from lib.staged_album import StagedAlbum

        with tempfile.TemporaryDirectory() as tmpdir:
            processing_dir = _private_processing_dir(tmpdir)
            beets_staging_dir = os.path.join(tmpdir, "Incoming")
            auto_import_root = stage_to_ai_root(
                staging_dir=beets_staging_dir, auto_import=True,
            )
            os.makedirs(auto_import_root)
            current_path = os.path.join(auto_import_root, "Artist - Album")
            os.makedirs(current_path)
            with open(os.path.join(current_path, "01.opus"), "wb") as handle:
                handle.write(b"corrupt bytes")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=556, status="downloading", mb_release_id="test-mbid-556",
            ))
            cfg = CratediggerConfig(
                processing_dir=processing_dir,
                beets_staging_dir=beets_staging_dir,
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album = make_grab_list_entry(
                files=[make_download_file(username="peer556")],
                artist="Artist", title="Album",
                mb_release_id="test-mbid-556", db_request_id=556,
                db_source="request",
            )
            result = ValidationResult(
                valid=False, distance=None, scenario="audio_corrupt",
                detail="garbled",
            )
            staged_album = StagedAlbum(current_path=current_path, request_id=556)

            _handle_rejected_result(album, result, staged_album, ctx)

            self.assertFalse(os.path.exists(current_path))
            self.assertTrue(
                os.path.isdir(auto_import_root),
                "the shared auto-import staging root must survive even "
                "though it is now empty -- it is externally provisioned, "
                "not a disposable per-artist directory",
            )
            self.assertEqual(os.listdir(auto_import_root), [])

    def test_audio_corrupt_delete_failure_still_records_rejection(self):
        """Issue #1077, F4: a failed delete must never block the rejection
        record — invariant 11 ("broken worlds surface and restart; nothing
        is parked"). Before this fix, an uncaught delete exception
        propagated straight out of ``_handle_rejected_result``: no
        download_log row, no denylist entry, no requeue, and the request
        stayed wherever it was before the delete — the opposite of
        restart-on-failure. Pinned three ways: delete succeeds records the
        rejection (the must-still-work control); delete fails before
        touching anything records it identically; delete fails PARTWAY
        through a real ``shutil.rmtree`` walk (round-2 review: one file
        genuinely removed for real, then a raise — not just a full-
        function mock standing in for "partial") records it identically
        too — a partial delete is not a special case."""
        from lib.download_rejection import _handle_rejected_result
        from lib.quality import ValidationResult
        from lib.staged_album import StagedAlbum

        def _fail_after_removing_one_file(path: str) -> None:
            # A REAL partial ``shutil.rmtree`` failure: one file genuinely
            # gone before the walk raises, proving the "midway" claim
            # rather than asserting it from a full-function mock.
            first = os.path.join(path, "01.flac")
            if os.path.exists(first):
                os.remove(first)
            raise OSError("simulated mid-delete failure")

        def _run(*, mode: str):
            with tempfile.TemporaryDirectory() as tmpdir:
                current_path = os.path.join(tmpdir, "Artist - Album")
                os.makedirs(current_path)
                with open(os.path.join(current_path, "01.flac"), "wb") as handle:
                    handle.write(b"corrupt bytes")
                with open(os.path.join(current_path, "02.flac"), "wb") as handle:
                    handle.write(b"corrupt bytes too")

                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=99, status="downloading", mb_release_id="test-mbid-99",
                ))
                ctx = make_ctx_with_fake_db(db)
                album = make_grab_list_entry(
                    files=[make_download_file(username="peer99")],
                    artist="Artist", title="Album",
                    mb_release_id="test-mbid-99", db_request_id=99,
                    db_source="request",
                )
                result = ValidationResult(
                    valid=False, distance=None, scenario="audio_corrupt",
                    detail="garbled",
                )
                staged_album = StagedAlbum(current_path=current_path, request_id=99)

                if mode == "raises_immediately":
                    with patch(
                        "lib.download_rejection._cleanup_staged_dir",
                        side_effect=OSError("simulated delete failure"),
                    ):
                        outcome = _handle_rejected_result(
                            album, result, staged_album, ctx,
                        )
                elif mode == "raises_midway":
                    with patch(
                        "lib.dispatch.helpers.shutil.rmtree",
                        side_effect=_fail_after_removing_one_file,
                    ):
                        outcome = _handle_rejected_result(
                            album, result, staged_album, ctx,
                        )
                else:
                    outcome = _handle_rejected_result(
                        album, result, staged_album, ctx,
                    )

                source = ctx.pipeline_db_source
                assert isinstance(source, FakePipelineDBSource)
                # Checked before the TemporaryDirectory cleans itself up on
                # exit — checking the caller-visible ``current_path`` after
                # ``_run`` returns would always report "gone".
                folder_survives = os.path.exists(current_path)
                one_file_really_removed = mode == "raises_midway" and not (
                    os.path.exists(os.path.join(current_path, "01.flac"))
                )
                return source, outcome, folder_survives, one_file_really_removed

        for mode in ("success", "raises_immediately", "raises_midway"):
            with self.subTest(mode=mode):
                source, outcome, folder_survives, one_file_really_removed = (
                    _run(mode=mode)
                )
                self.assertFalse(outcome.success)
                self.assertEqual(len(source.reject_and_requeue_calls), 1)
                rejected = source.reject_and_requeue_calls[0]["bv_result"]
                self.assertEqual(rejected.scenario, "audio_corrupt")
                self.assertIsNone(rejected.failed_path)
                self.assertEqual(rejected.denylisted_users, ["peer99"])
                # The delete genuinely failed for both non-success modes —
                # the folder is still there — but that must never be why
                # the record above is missing.
                self.assertEqual(folder_survives, mode != "success")
                if mode == "raises_midway":
                    self.assertTrue(
                        one_file_really_removed,
                        "the partial-delete fixture must actually remove "
                        "something for real, or this isn't pinning a "
                        "mid-walk failure at all",
                    )

    def test_benign_non_audio_sidecar_never_strands_the_rejection(self):
        """Issue #1077, B1 (round-2 review blocker): the reviewer's exact
        reproduction, driven through the real Lane A entry point. A benign
        non-audio subdirectory (e.g. a ``Scans/`` sidecar) with an exactly-
        matching audio manifest used to trip the leftover check AFTER the
        curated move — files move but the empty directory skeleton they
        left behind doesn't — raising post-mutation, outside the rollback
        block. That used to strand the album in ``wrong_matches/`` with
        zero download_log rows, zero denylist writes, and no requeue: the
        exact invisible-quarantine pathology this issue exists to kill, and
        a regression vs main. The move must complete with no raise, the
        rejection record (row + ban + requeue) must land, and the sidecar's
        own contents must end up under the wrong_matches destination too."""
        from lib.download_rejection import _handle_rejected_result
        from lib.quality import ValidationResult
        from lib.staged_album import StagedAlbum

        with tempfile.TemporaryDirectory() as tmpdir:
            current_path = os.path.join(tmpdir, "Artist - Album")
            os.makedirs(current_path)
            with open(os.path.join(current_path, "01.flac"), "wb") as handle:
                handle.write(b"audio bytes")
            scans_dir = os.path.join(current_path, "Scans")
            os.makedirs(scans_dir)
            with open(os.path.join(scans_dir, "front.jpg"), "wb") as handle:
                handle.write(b"scan bytes")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=321, status="downloading", mb_release_id="test-mbid-321",
            ))
            ctx = make_ctx_with_fake_db(db)
            album = make_grab_list_entry(
                files=[make_download_file(
                    username="peer321", filename="01.flac", file_dir="",
                )],
                artist="Artist", title="Album",
                mb_release_id="test-mbid-321", db_request_id=321,
                db_source="request",
            )
            result = ValidationResult(
                valid=False, distance=0.4, scenario="high_distance",
                detail="too far",
            )
            staged_album = StagedAlbum(current_path=current_path, request_id=321)

            # Must not raise.
            outcome = _handle_rejected_result(album, result, staged_album, ctx)

            source = ctx.pipeline_db_source
            assert isinstance(source, FakePipelineDBSource)
            self.assertFalse(outcome.success)
            self.assertEqual(len(source.reject_and_requeue_calls), 1)
            rejected = source.reject_and_requeue_calls[0]["bv_result"]
            self.assertEqual(rejected.scenario, "high_distance")
            self.assertIsNotNone(rejected.failed_path)
            assert rejected.failed_path is not None
            self.assertEqual(rejected.denylisted_users, ["peer321"])
            # A benign sidecar-only world is not an anomaly — no spurious
            # note in the persisted detail.
            self.assertNotIn("swept into", rejected.detail or "")
            self.assertTrue(
                os.path.exists(os.path.join(rejected.failed_path, "01.flac")))
            self.assertTrue(os.path.exists(
                os.path.join(rejected.failed_path, "Scans", "front.jpg")))
            self.assertFalse(os.path.exists(current_path))

    def test_sweep_anomaly_reaches_the_persisted_download_log_row(self):
        """Issue #1077, R3-6 (round-3 review): a REAL sweep anomaly must
        reach the persisted ``download_log`` row, not just the in-memory
        ``ValidationResult`` the caller builds. The chain: a genuinely
        untracked audio file on disk (out-of-manifest — the caller's
        ``allowed_audio`` set is narrower than what is really there) makes
        ``move_failed_import_curated`` sweep instead of raising and compose
        ``CuratedMoveResult.anomaly``; ``_handle_rejected_result`` folds
        that into ``bv_result.detail``; ``dl_info.validation_result =
        bv_result.to_json()`` carries the same string into the JSON
        envelope; and the SAME ``bv_result.detail`` is written verbatim to
        ``download_log.beets_detail`` (this is the actual production
        column the persisted row exposes for Recents forensics — see
        ``album_source.DatabaseSource.reject_and_requeue``'s synchronous
        branch, which this test drives for real through
        ``FakePipelineDBSource`` rather than stopping at its previously
        no-op call-recording shim)."""
        from lib.download_rejection import _handle_rejected_result
        from lib.quality import ValidationResult
        from lib.staged_album import StagedAlbum

        with tempfile.TemporaryDirectory() as tmpdir:
            current_path = os.path.join(tmpdir, "Artist - Album")
            os.makedirs(current_path)
            with open(os.path.join(current_path, "01.flac"), "wb") as handle:
                handle.write(b"audio bytes")
            # Untracked audio the download manifest below never names — the
            # exact "genuinely unexpected residue" world
            # ``_sweep_residue_into_destination`` exists for.
            with open(os.path.join(current_path, "bonus.opus"), "wb") as handle:
                handle.write(b"bonus audio bytes")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=717, status="downloading", mb_release_id="test-mbid-717",
            ))
            ctx = make_ctx_with_fake_db(db)
            album = make_grab_list_entry(
                files=[make_download_file(
                    username="peer717", filename="01.flac", file_dir="",
                )],
                artist="Artist", title="Album",
                mb_release_id="test-mbid-717", db_request_id=717,
                db_source="request",
            )
            result = ValidationResult(
                valid=False, distance=0.4, scenario="high_distance",
                detail="too far",
            )
            staged_album = StagedAlbum(current_path=current_path, request_id=717)

            outcome = _handle_rejected_result(album, result, staged_album, ctx)

            self.assertFalse(outcome.success)
            self.assertEqual(len(db.download_logs), 1)
            persisted = db.download_logs[0]
            self.assertEqual(persisted.request_id, 717)
            self.assertIsNotNone(persisted.beets_detail)
            assert persisted.beets_detail is not None
            self.assertIn("swept into", persisted.beets_detail)
            # The bundled JSON envelope carries the identical composed
            # string (``dl_info.validation_result = bv_result.to_json()``)
            # — the same detail lands in both places, not just one.
            self.assertIsNotNone(persisted.validation_result)
            assert persisted.validation_result is not None
            self.assertIn("swept into", persisted.validation_result)
            # The untracked file really did get swept into the quarantine
            # destination rather than silently vanishing.
            source = ctx.pipeline_db_source
            assert isinstance(source, FakePipelineDBSource)
            rejected = source.reject_and_requeue_calls[0]["bv_result"]
            assert rejected.failed_path is not None
            self.assertTrue(os.path.exists(
                os.path.join(rejected.failed_path, "bonus.opus")))

    def test_prune_failure_never_strands_the_rejection_or_leaks_wrong_matches(self):
        """Issue #1077, R3-1 (HIGH, round-3 review): the B1 fix moved the
        fragile post-move filesystem work (prune, leftover-check, sweep,
        final removal) into ``move_failed_import_curated`` but left every
        one of those statements unwrapped. A transient ``OSError`` there
        (EACCES/EIO/ENOTEMPTY race — virtiofs makes these real) used to
        propagate straight out of the function, past
        ``log_validation_result``/``reject_and_requeue`` — recreating the
        exact record-nothing stranding B1 exists to kill. Worse: because
        the request would stay parked, ``poll_active_downloads`` would
        re-enter every cycle and leak a fresh ``wrong_matches/<name>_N``
        folder via the ``exist_ok=False`` allocation, unbounded — the
        reviewer's exact probe world: ``os.rmdir`` failing specifically on
        the benign ``Scans/`` skeleton. The move must still complete
        without raising and the row + ban + requeue must land.

        Issue #1077, R4-2 (round-4 review): this world has NO untracked
        content — ``Scans/`` is empty (its one file already moved during
        the main loop), only the directory node itself resists removal.
        The composed detail must say so honestly (a hedged "could not
        verify" note) rather than the confident "left untracked content
        behind ... swept into" wording, which would be a false accusation
        for a world where nothing was ever swept because there was nothing
        to sweep. And — because the call completes on its first attempt
        rather than needing to retry — exactly ONE ``wrong_matches/``
        destination folder exists: no per-cycle leak."""
        from lib.download_rejection import _handle_rejected_result
        from lib.quality import ValidationResult
        from lib.staged_album import StagedAlbum

        real_rmdir = os.rmdir

        with tempfile.TemporaryDirectory() as tmpdir:
            current_path = os.path.join(tmpdir, "Artist - Album")
            os.makedirs(current_path)
            with open(os.path.join(current_path, "01.flac"), "wb") as handle:
                handle.write(b"audio bytes")
            scans_dir = os.path.join(current_path, "Scans")
            os.makedirs(scans_dir)
            with open(os.path.join(scans_dir, "front.jpg"), "wb") as handle:
                handle.write(b"scan bytes")

            def _flaky_rmdir(path, *args, **kwargs):
                if os.path.abspath(path) == os.path.abspath(scans_dir):
                    raise OSError("simulated ENOTEMPTY race on Scans/")
                return real_rmdir(path, *args, **kwargs)

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=911, status="downloading", mb_release_id="test-mbid-911",
            ))
            ctx = make_ctx_with_fake_db(db)
            album = make_grab_list_entry(
                files=[make_download_file(
                    username="peer911", filename="01.flac", file_dir="",
                )],
                artist="Artist", title="Album",
                mb_release_id="test-mbid-911", db_request_id=911,
                db_source="request",
            )
            result = ValidationResult(
                valid=False, distance=0.4, scenario="high_distance",
                detail="too far",
            )
            staged_album = StagedAlbum(current_path=current_path, request_id=911)

            with patch("os.rmdir", side_effect=_flaky_rmdir):
                # Must not raise.
                outcome = _handle_rejected_result(album, result, staged_album, ctx)

            self.assertFalse(outcome.success)
            # Row + ban + requeue landed despite the internal rmdir failure.
            self.assertEqual(len(db.download_logs), 1)
            persisted = db.download_logs[0]
            self.assertIsNotNone(persisted.beets_detail)
            assert persisted.beets_detail is not None
            # Truthful, hedged copy for a world with no real leftover
            # content — never the confident "left untracked content
            # behind" accusation this world does not earn.
            self.assertIn("could not verify", persisted.beets_detail)
            self.assertNotIn(
                "left untracked content behind", persisted.beets_detail)
            self.assertEqual(len(db.denylist), 1)
            self.assertEqual(db.denylist[0].username, "peer911")
            self.assertEqual(db.request(911)["status"], "wanted")
            # No per-cycle leak: exactly one wrong_matches destination
            # folder, no "_1"/"_2" suffix from a retry that never had to
            # happen because the call above completed on its first pass.
            quarantine_root = os.path.join(tmpdir, "wrong_matches")
            self.assertEqual(os.listdir(quarantine_root), ["Artist - Album"])
            dest = os.path.join(quarantine_root, "Artist - Album")
            self.assertTrue(os.path.exists(os.path.join(dest, "01.flac")))
            self.assertTrue(os.path.exists(
                os.path.join(dest, "Scans", "front.jpg")))

    def test_transient_observation_failure_never_falsely_accuses_a_clean_move(
        self,
    ) -> None:
        """Issue #1077, R4-2 (round-4 review): the reviewer's exact probe —
        a genuinely CLEAN move (no residue, no benign skeleton, nothing to
        sweep) where the leftover-presence check itself hits one transient
        read failure right after the move. Before this fix, that failure
        was worst-cased identically to confirmed content, composing "left
        untracked content behind ... swept into" for a source that was
        correctly, fully removed — a false accusation persisted to
        ``beets_detail``/Recents. Now it composes the honest hedged note,
        and — because the transient failure resolves on the very next
        check — the source is still correctly removed: cleanup is not
        blocked by having been unable to verify once."""
        from lib.download_rejection import _handle_rejected_result
        from lib.quality import ValidationResult
        from lib.staged_album import StagedAlbum

        real_walk = os.walk
        calls = {"n": 0}

        def _flaky_walk(path, *args, **kwargs):
            calls["n"] += 1
            # Call #1 is the main move loop's own walk (moving 01.flac) —
            # must succeed normally. Call #2 is the FIRST leftover
            # observation right after the move — the one this test
            # targets. Every later call succeeds, simulating a failure
            # that resolves on retry.
            if calls["n"] == 2:
                raise OSError("simulated transient EIO on first observation")
            yield from real_walk(path, *args, **kwargs)

        with tempfile.TemporaryDirectory() as tmpdir:
            current_path = os.path.join(tmpdir, "Artist - Album")
            os.makedirs(current_path)
            with open(os.path.join(current_path, "01.flac"), "wb") as handle:
                handle.write(b"audio bytes")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=912, status="downloading", mb_release_id="test-mbid-912",
            ))
            ctx = make_ctx_with_fake_db(db)
            album = make_grab_list_entry(
                files=[make_download_file(
                    username="peer912", filename="01.flac", file_dir="",
                )],
                artist="Artist", title="Album",
                mb_release_id="test-mbid-912", db_request_id=912,
                db_source="request",
            )
            result = ValidationResult(
                valid=False, distance=0.4, scenario="high_distance",
                detail="too far",
            )
            staged_album = StagedAlbum(current_path=current_path, request_id=912)

            with patch("os.walk", side_effect=_flaky_walk):
                outcome = _handle_rejected_result(album, result, staged_album, ctx)

            self.assertFalse(outcome.success)
            self.assertEqual(len(db.download_logs), 1)
            persisted = db.download_logs[0]
            self.assertIsNotNone(persisted.beets_detail)
            assert persisted.beets_detail is not None
            self.assertIn("could not verify", persisted.beets_detail)
            self.assertNotIn(
                "left untracked content behind", persisted.beets_detail)
            # The transient failure resolved on the retry: the move
            # genuinely was clean, so the source is correctly removed —
            # an unverified FIRST check must not permanently block cleanup.
            self.assertFalse(os.path.exists(current_path))
            dest = os.path.join(tmpdir, "wrong_matches", "Artist - Album")
            self.assertTrue(os.path.exists(os.path.join(dest, "01.flac")))

    def test_multi_audio_non_flac_never_reaches_beets_validation(self):
        import subprocess

        from lib.config import CratediggerConfig
        from lib.download_processing import CompletionFailed, process_completed_album

        with tempfile.TemporaryDirectory() as tmpdir:
            downloads = os.path.join(tmpdir, "downloads")
            source_dir = os.path.join(downloads, "Music")
            os.makedirs(source_dir)
            source = os.path.join(source_dir, "01 - Two Streams.m4a")
            subprocess.run([
                "ffmpeg", "-v", "error", "-nostdin", "-f", "lavfi",
                "-i", "sine=frequency=440:duration=0.2", "-f", "lavfi",
                "-i", "sine=frequency=880:duration=0.2", "-map", "0:a",
                "-map", "1:a", "-c:a", "aac", source,
            ], check=True, timeout=30)
            cfg = CratediggerConfig(
                slskd_download_dir=downloads, beets_validation_enabled=True,
                processing_dir=_private_processing_dir(tmpdir),
            )
            file = make_download_file(
                filename="user1\\Music\\01 - Two Streams.m4a", file_dir="user1\\Music",
            )
            file.local_path = source
            album = make_grab_list_entry(files=[file], mb_release_id="test-mbid")
            result = process_completed_album(
                album, _make_ctx(cfg=cfg), import_job_id=1,
                validate_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    AssertionError("must not reach Beets"),
                ),
            )
            self.assertIsInstance(result, CompletionFailed)
            assert isinstance(result, CompletionFailed)
            self.assertTrue(result.reason.startswith("media_readiness_ambiguous"))

    def test_returns_true_on_success(self):
        """Successful file move + processing returns Completed."""
        import os
        import tempfile

        from lib.download_processing import Completed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source file
            src_dir = os.path.join(tmpdir, "source_dir")
            os.makedirs(src_dir)
            src_file = os.path.join(src_dir, "01 - Track.mp3")
            _write_test_audio(src_file)

            files = [make_download_file(filename="source_dir\\01 - Track.mp3",
                                        file_dir="source_dir")]
            files[0].local_path = src_file
            album = make_grab_list_entry(files=files, mb_release_id="")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False
            result = process_completed_album(album, ctx, import_job_id=1)
            self.assertIsInstance(result, Completed)

    def test_dispatch_outcome_summary_is_returned_to_queue_owner(
        self,
    ):
        """Auto-import summaries must survive for the importer queue result."""
        import os
        import tempfile

        from lib.dispatch import DispatchOutcome
        from lib.download_processing import (
            CompletionDispatched,
            process_completed_album,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            src_dir = os.path.join(tmpdir, "source_dir")
            os.makedirs(src_dir)
            src_file = os.path.join(src_dir, "01 - Track.mp3")
            _write_test_audio(src_file)

            files = [make_download_file(filename="source_dir\\01 - Track.mp3",
                                        file_dir="source_dir")]
            files[0].local_path = src_file
            album = make_grab_list_entry(files=files, mb_release_id="test-mbid")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = True
            stub_outcome = DispatchOutcome(
                success=True,
                message="Import successful",
            )
            from lib.context import CratediggerContext
            from lib.dispatch import DispatchCoreFn
            from lib.download_validation import HandleValidFn, ValidateFn
            from lib.grab_list import GrabListEntry
            from lib.staged_album import StagedAlbum

            validate_calls: list[int] = []

            def _stub_validate(
                album_data: GrabListEntry,
                staged_album: StagedAlbum,
                ctx: CratediggerContext,
                *,
                import_job_id: int,
                handle_valid_fn: HandleValidFn | None = None,
                dispatch_fn: DispatchCoreFn | None = None,
                execution_lease: ExecutionLeaseSnapshot | None = None,
                cancellation_token: CancellationToken | None = None,
                owner_session_identity: OwnerSessionIdentity | None = None,
            ) -> DispatchOutcome | None:
                del (
                    album_data,
                    staged_album,
                    ctx,
                    handle_valid_fn,
                    dispatch_fn,
                    execution_lease,
                    cancellation_token,
                    owner_session_identity,
                )
                validate_calls.append(import_job_id)
                return stub_outcome

            validate_recorder: ValidateFn = _stub_validate

            result = process_completed_album(
                album, ctx, import_job_id=1, validate_fn=validate_recorder,
            )

            assert isinstance(result, CompletionDispatched)
            self.assertIs(result.outcome, stub_outcome)
            self.assertEqual(validate_calls, [1])

    @patch("lib.beets.beets_validate")
    def test_beets_rejection_summary_is_returned_to_queue_owner(
        self,
        mock_beets_validate,
    ):
        """Validation rejections must fail the queue job, not look completed."""
        import tempfile

        from lib.download_processing import (
            CompletionDispatched,
            process_completed_album,
        )
        from lib.quality import ValidationResult

        with tempfile.TemporaryDirectory() as tmpdir:
            downloads_root = os.path.join(tmpdir, "downloads")
            source_dir = os.path.join(downloads_root, "Music")
            os.makedirs(source_dir)
            source_file = os.path.join(source_dir, "01 - Track.mp3")
            _write_test_audio(source_file)

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                active_download_state={"files": [], "filetype": "mp3"},
                artist_name="Artist",
                album_title="Album",
                year=2024,
                mb_release_id="test-mbid",
            ))
            cfg = cast(Any, _make_ctx().cfg)
            cfg.slskd_download_dir = downloads_root
            cfg.beets_validation_enabled = True
            cfg.beets_tracking_file = os.path.join(tmpdir, "beets-tracking.jsonl")
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            mock_beets_validate.return_value = ValidationResult(
                valid=False,
                distance=0.1919,
                scenario="high_distance",
                detail="distance=0.1919",
            )
            stamped = make_download_file(
                filename="user1\\Music\\01 - Track.mp3",
                file_dir="user1\\Music",
            )
            stamped.local_path = source_file
            album = make_grab_list_entry(
                files=[stamped],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="test-mbid",
                db_request_id=42,
                db_source="request",
            )

            result = process_completed_album(album, ctx, import_job_id=1)

            assert isinstance(result, CompletionDispatched)
            outcome = result.outcome
            self.assertFalse(outcome.success)
            self.assertFalse(outcome.deferred)
            self.assertEqual(
                outcome.message,
                "Rejected: high_distance - distance=0.1919",
            )
            source = ctx.pipeline_db_source
            assert isinstance(source, FakePipelineDBSource)
            self.assertEqual(len(source.reject_and_requeue_calls), 1)

    def test_returns_false_on_file_move_failure(self):
        """A mid-copy failure leaves every stamped source untouched."""
        import os
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            src_dir = os.path.join(tmpdir, "Music")
            os.makedirs(src_dir)
            srcs = []
            files = []
            for i in (1, 2):
                src = os.path.join(src_dir, f"0{i} - Track.mp3")
                with open(src, "w") as f:
                    f.write("fake audio")
                srcs.append(src)
                file = make_download_file(
                    filename=f"user1\\Music\\0{i} - Track.mp3",
                    file_dir="user1\\Music",
                )
                file.local_path = src
                files.append(file)
            album = make_grab_list_entry(files=files, mb_release_id="")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False

            copied = 0

            def fail_before_second_copy() -> None:
                nonlocal copied
                copied += 1
                if copied == 2:
                    raise OSError("disk full")

            result = process_completed_album(
                album,
                ctx,
                import_job_id=1,
                materialize_before_file_copy=fail_before_second_copy,
            )

            self.assertIsInstance(result, CompletionFailed)
            # Publish never happened, so the sources were never unlinked.
            self.assertTrue(os.path.exists(srcs[0]))
            self.assertTrue(os.path.exists(srcs[1]))

    def test_resumes_from_persisted_current_path(self):
        """A post-move retry must process the persisted current_path, not slskd."""
        import tempfile

        from lib.download_processing import Completed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            resumed_path = os.path.join(tmpdir, "staging", "Artist", "Album")
            os.makedirs(resumed_path)
            resumed_file = os.path.join(resumed_path, "01 - Track.mp3")
            _write_test_audio(resumed_file)

            files = [make_download_file(
                filename="user1\\Music\\01 - Track.mp3",
                file_dir="user1\\Music",
                size=len("fake audio"),
            )]
            album = make_grab_list_entry(
                files=files,
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(files[0].import_path, resumed_file)
            self.assertTrue(os.path.exists(resumed_file))

    def test_resumes_multi_disc_from_persisted_current_path(self):
        """Resume must preserve the staged multi-disc filenames on disk."""
        import tempfile

        from lib.download_processing import Completed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            resumed_path = os.path.join(tmpdir, "staging", "Artist", "Album")
            os.makedirs(resumed_path)
            resumed_file = os.path.join(resumed_path, "Disk 2 - 01 - Track.flac")
            _write_test_audio(resumed_file)

            file = make_download_file(
                filename="user1\\CD2\\01 - Track.flac",
                file_dir="user1\\CD2",
                size=len("fake audio"),
            )
            file.disk_no = 2
            file.disk_count = 2
            album = make_grab_list_entry(
                files=[file],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(file.import_path, resumed_file)
            self.assertTrue(os.path.exists(resumed_file))

    def test_fresh_materialization_does_not_write_request_lifecycle_path(self):
        """Only the handoff publishes lifecycle paths; materialization is local."""
        import tempfile

        from lib.download_processing import Completed, process_completed_album
        from lib.processing_paths import attempt_fingerprint
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_db = FakePipelineDB()
            fake_db.seed_request(make_request_row(id=42))
            before = copy.deepcopy(fake_db.get_request(42))
            source_dir = os.path.join(tmpdir, "downloads", "Music")
            os.makedirs(source_dir)
            source_file = os.path.join(source_dir, "01 - Track.mp3")
            _write_test_audio(source_file)

            stamped = make_download_file(
                filename="user1\\Music\\01 - Track.mp3",
                file_dir="user1\\Music",
            )
            stamped.local_path = source_file
            album = make_grab_list_entry(
                files=[stamped],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
                db_request_id=42,
            )
            cfg = cast(Any, _make_ctx().cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False
            ctx = make_ctx_with_fake_db(fake_db, cfg=cfg)

            result = process_completed_album(album, ctx, import_job_id=1)

            fp = attempt_fingerprint([("user1", "user1\\Music\\01 - Track.mp3")])
            self.assertIsInstance(result, Completed)
            self.assertEqual(fake_db.get_request(42), before)
            self.assertEqual(
                album.import_folder,
                os.path.join(
                    cfg.processing_dir,
                    "albums",
                    f"Artist - Album (2024) [{fp}]",
                ),
            )

    def test_post_validation_staged_path_without_request_id_still_resumes(
        self,
    ):
        """Post-validation staging remains resumable without the auto-import guard."""
        import tempfile

        from lib.download_processing import Completed, process_completed_album
        from lib.processing_paths import stage_to_ai_path
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Artist",
                title="Album",
                staging_dir=staging_root,
                request_id=42,
                auto_import=False,
            )
            os.makedirs(resumed_path)
            resumed_file = os.path.join(resumed_path, "01 - Track.mp3")
            _write_test_audio(resumed_file)

            album = make_grab_list_entry(
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
                db_request_id=None,
                db_source="redownload",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_staging_dir = staging_root
            cfg.beets_validation_enabled = False
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(album.files[0].import_path, resumed_file)

    def test_returns_false_when_persisted_current_path_missing_dir(self):
        """Resume must fail closed when the persisted directory no longer exists."""
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            album = make_grab_list_entry(
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
            )
            album.import_folder = os.path.join(tmpdir, "missing")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)

    def test_returns_false_when_persisted_current_path_missing_file(self):
        """Resume dir must contain every tracked file before processing continues."""
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            resumed_path = os.path.join(tmpdir, "staging", "Artist", "Album")
            os.makedirs(resumed_path)
            album = make_grab_list_entry(
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)


class TestHandleValidResultMissingMbid(unittest.TestCase):
    """_handle_valid_result guards for request rows without an MBID."""

    def test_request_source_without_mbid_requeues_instead_of_marking_done(self):
        """Request rows without an MBID must requeue, not mark imported."""
        import tempfile

        from lib.download_validation import _handle_valid_result
        from lib.staged_album import StagedAlbum

        album = make_grab_list_entry(
            files=[make_download_file()],
            mb_release_id="",
            db_source="request",
            db_request_id=42,
        )
        bv_result = MagicMock()
        bv_result.distance = 0.05
        bv_result.scenario = "strong_match"
        bv_result.to_json.return_value = '{"valid": true}'

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        cfg = MagicMock()
        cfg.beets_distance_threshold = 0.15
        ctx = make_ctx_with_fake_db(db, cfg=cfg)

        with tempfile.TemporaryDirectory() as tmpdir, \
             patch("lib.download_rejection.log_validation_result"):
            import_dir = os.path.join(tmpdir, "Test Artist - Test Album")
            os.makedirs(import_dir)
            with open(os.path.join(import_dir, "01 - Track.mp3"), "w",
                      encoding="utf-8") as fp:
                fp.write("x")

            outcome = _handle_valid_result(
                album,
                bv_result,
                StagedAlbum(current_path=import_dir, request_id=42),
                ctx,
            )

            assert outcome is not None
            self.assertFalse(outcome.success)
            self.assertFalse(os.path.exists(import_dir))
            wrong_matches_dir = os.path.join(tmpdir, "wrong_matches")
            self.assertTrue(os.path.isdir(wrong_matches_dir))
            self.assertEqual(len(os.listdir(wrong_matches_dir)), 1)

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertEqual(db.request(42)["validation_attempts"], 1)
        self.assertEqual(len(db.download_logs), 1)
        # Must-still-work guard (#550 defect #4 corollary): this reject
        # reaches _reject_request_auto_import AFTER beets already measured
        # a real distance (0.05) — that measurement must survive, not get
        # nulled or replaced.
        self.assertEqual(db.download_logs[0].beets_distance, 0.05)
        # Issue #1077, D1/D4: Lane B is a world failure with a reviewable
        # folder — kept + banned + shown, not silently re-fetchable.
        self.assertEqual([entry.username for entry in db.denylist], ["user1"])
        visible = db.get_wrong_matches()
        self.assertEqual(len(visible), 1)
        self.assertEqual(visible[0]["soulseek_username"], "user1")

    def test_measured_perfect_zero_distance_is_preserved_not_nulled(self):
        """A genuinely measured 0.0 (perfect match) must persist as 0.0,
        not be confused for 'unmeasured' and nulled — 0.0 is falsy in
        Python, so a naive ``if bv_result.distance`` check would silently
        drop it. Same missing-mbid reject path, distance=0.0 instead."""
        import tempfile

        from lib.download_validation import _handle_valid_result
        from lib.staged_album import StagedAlbum

        album = make_grab_list_entry(
            files=[make_download_file()],
            mb_release_id="",
            db_source="request",
            db_request_id=42,
        )
        bv_result = MagicMock()
        bv_result.distance = 0.0
        bv_result.scenario = "strong_match"
        bv_result.to_json.return_value = '{"valid": true}'

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        cfg = MagicMock()
        cfg.beets_distance_threshold = 0.15
        ctx = make_ctx_with_fake_db(db, cfg=cfg)

        with tempfile.TemporaryDirectory() as tmpdir, \
             patch("lib.download_rejection.log_validation_result"):
            import_dir = os.path.join(tmpdir, "Test Artist - Test Album")
            os.makedirs(import_dir)
            with open(os.path.join(import_dir, "01 - Track.mp3"), "w",
                      encoding="utf-8") as fp:
                fp.write("x")

            _handle_valid_result(
                album,
                bv_result,
                StagedAlbum(current_path=import_dir, request_id=42),
                ctx,
            )

        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(db.download_logs[0].beets_distance, 0.0)


class TestEventPathMaterialization(unittest.TestCase):
    """Issue #146 phase 3: the event-stream local_path is the ONLY source
    of file locations. An unstamped file (with no already-moved evidence)
    is a hard failure — the resolver fallback is gone.
    """

    FNAME = "04 How To Disappear Completely.mp3"

    def _album(self, tmpdir, *, local_path, files=None):
        """Build one album wired to ``tmpdir`` as the slskd download root.

        Pass ``files`` for a multi-file manifest; ``local_path`` is then
        ignored (each supplied file carries its own stamp).
        """
        if files is None:
            files = [make_download_file(
                filename=f"@@wcren\\Music\\Radiohead\\Kid A\\{self.FNAME}",
                file_dir="@@wcren\\Music\\Radiohead\\Kid A",
                size=len("fake audio"),
            )]
            files[0].local_path = local_path
        album = make_grab_list_entry(
            files=files, mb_release_id="", artist="Radiohead",
            title="Kid A", year="2000")
        ctx = _make_ctx()
        cfg = cast(Any, ctx.cfg)
        cfg.slskd_download_dir = tmpdir
        cfg.beets_validation_enabled = False
        return album, ctx

    def _canonical_dir(self, ctx, files):
        from lib.processing_paths import attempt_fingerprint, canonical_processing_path
        fp = attempt_fingerprint([(f.username, f.filename) for f in files])
        return canonical_processing_path(
            artist="Radiohead", title="Kid A", year="2000",
            slskd_download_dir=os.path.join(ctx.cfg.processing_dir, "albums"),
            attempt_fingerprint=fp,
        )

    def _moved(self, ctx, files):
        return os.listdir(self._canonical_dir(ctx, files))

    def test_stamped_file_moves_with_clean_basename(self):
        # slskd placed the file at an arbitrary event-reported location
        # with a collision suffix; the move follows the stamp and the
        # destination keeps the clean remote basename.
        import tempfile

        from lib.download_processing import Completed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            event_path = os.path.join(
                tmpdir, "somewhere-unrelated",
                "04 How To Disappear Completely_638827305447447018.mp3")
            os.makedirs(os.path.dirname(event_path))
            _write_test_audio(event_path)
            album, ctx = self._album(tmpdir, local_path=event_path)

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertFalse(os.path.exists(event_path))
            self.assertEqual(self._moved(ctx, album.files), [self.FNAME])

    def test_stamped_forward_slash_remote_path_keeps_basename(self):
        # Destination basename extraction accepts slash-normalized remote
        # paths regardless of where the stamped source lives.
        import tempfile

        from lib.download_processing import Completed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, "Kid A", self.FNAME)
            os.makedirs(os.path.dirname(src))
            _write_test_audio(src)
            files = [make_download_file(
                filename=f"@@wcren/Music/Radiohead/Kid A/{self.FNAME}",
                file_dir="@@wcren/Music/Radiohead/Kid A",
                size=len("fake audio"),
            )]
            files[0].local_path = src
            album = make_grab_list_entry(
                files=files, mb_release_id="", artist="Radiohead",
                title="Kid A", year="2000")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(self._moved(ctx, album.files), [self.FNAME])

    def test_unstamped_file_is_hard_failure(self):
        # No event was ever ingested for this file (pre-bootstrap
        # completion or cursor gap) — hard failure with diagnostics, no
        # guessing at on-disk locations.
        #
        # Issue #868 I2: slskd never told us where the file landed. It must
        # NOT share a reason with the stale-stamp case below, which is a
        # different operator problem from a stamp pointing at nothing.
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            # File IS on disk at the historical inferred location — the
            # point is that phase 3 no longer looks there.
            src = os.path.join(tmpdir, "Kid A", self.FNAME)
            os.makedirs(os.path.dirname(src))
            with open(src, "w") as fp:
                fp.write("fake audio")
            album, ctx = self._album(tmpdir, local_path=None)

            with self.assertLogs("cratedigger", level=logging.ERROR) as logs:
                result = process_completed_album(
                    album, ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            assert isinstance(result, CompletionFailed)
            self.assertEqual(result.reason, "event_path_never_stamped")
            self.assertTrue(os.path.exists(src))
            joined = "\n".join(logs.output)
            self.assertIn("reason=event_path_never_stamped", joined)

    def test_repeated_unstamped_preflight_reports_the_same_reason(self):
        """Repeated processor executions report one stable refusal reason."""
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            album, ctx = self._album(tmpdir, local_path=None)

            reasons = []
            for _ in range(14):
                result = process_completed_album(album, ctx, import_job_id=1)
                self.assertIsInstance(result, CompletionFailed)
                assert isinstance(result, CompletionFailed)
                reasons.append(result.reason)

            self.assertEqual(set(reasons), {"event_path_never_stamped"})

    def test_stale_stamp_without_dst_is_hard_failure(self):
        # Stamped path vanished and the destination has no already-moved
        # copy — hard failure, diagnostics name the stale stamp.
        #
        # Issue #868 I2: distinct from the never-stamped case above. The
        # event feed DID report a location; the bytes are simply not
        # there any more.
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            album, ctx = self._album(
                tmpdir, local_path=os.path.join(tmpdir, "gone", "x.mp3"))

            with self.assertLogs("cratedigger", level=logging.ERROR) as logs:
                result = process_completed_album(
                    album, ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            assert isinstance(result, CompletionFailed)
            self.assertEqual(result.reason, "event_path_gone_from_disk")
            joined = "\n".join(logs.output)
            self.assertIn("reason=event_path_gone_from_disk", joined)

    def test_one_missing_file_in_a_healthy_share_is_not_blamed_on_the_share(self):
        """Issue #868 D1: the share is now opened ONCE and held for the
        whole manifest, so a refusal of the ROOT is attributable. The other
        direction must still hold: a healthy share missing one file is the
        FILE's problem, and must not escalate to a `slskd_root_*` reason.
        """
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            present = os.path.join(tmpdir, "Kid A", "01 Present.mp3")
            os.makedirs(os.path.dirname(present))
            with open(present, "w") as fp:
                fp.write("fake audio")
            files = [
                make_download_file(
                    filename="@@w\\Music\\R\\Kid A\\01 Present.mp3",
                    file_dir="@@w\\Music\\R\\Kid A", size=len("fake audio"),
                ),
                make_download_file(
                    filename="@@w\\Music\\R\\Kid A\\02 Absent.mp3",
                    file_dir="@@w\\Music\\R\\Kid A", size=10,
                ),
            ]
            files[0].local_path = present
            files[1].local_path = os.path.join(tmpdir, "Kid A", "02 Absent.mp3")
            album, ctx = self._album(tmpdir, local_path=None, files=files)

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            assert isinstance(result, CompletionFailed)
            self.assertEqual(result.reason, "event_path_gone_from_disk")
            self.assertFalse(result.reason.startswith("slskd_root_"))
            # The first file's bytes are untouched: a refused preflight
            # copies and unlinks nothing.
            self.assertTrue(os.path.exists(present))

    def test_symlinked_stamp_is_a_containment_reason_not_a_storage_one(self):
        """Issue #868 I3: a symlinked source path is a SECURITY refusal.

        Before the split it shared ``unsafe_source_path`` with ordinary
        ESTALE/EIO storage failures, so a containment violation and a sick
        mount were indistinguishable in the audit trail.
        """
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            outside = os.path.join(tmpdir, "outside.mp3")
            with open(outside, "w") as fp:
                fp.write("fake audio")
            stamped = os.path.join(tmpdir, "Kid A", self.FNAME)
            os.makedirs(os.path.dirname(stamped))
            os.symlink(outside, stamped)
            album, ctx = self._album(tmpdir, local_path=stamped)

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            assert isinstance(result, CompletionFailed)
            self.assertEqual(result.reason, "unsafe_source_path")
            # The symlink target is untouched: preflight copies nothing.
            self.assertTrue(os.path.exists(outside))

    def test_escaping_stamp_is_a_containment_reason(self):
        """Issue #868 I3: a stamp naming a path outside the slskd root is
        a containment violation, never a storage error."""
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            root = os.path.join(tmpdir, "downloads")
            os.makedirs(root)
            escaped = os.path.join(tmpdir, "elsewhere.mp3")
            with open(escaped, "w") as fp:
                fp.write("fake audio")
            album, ctx = self._album(root, local_path=escaped)

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            assert isinstance(result, CompletionFailed)
            self.assertEqual(result.reason, "unsafe_source_path")
            self.assertTrue(os.path.exists(escaped))

    def test_storage_open_failure_carries_its_errno_not_a_containment_reason(self):
        """Issue #868 I2/I3: an unreadable-but-present stamped file is a
        STORAGE failure and records which errno it was. The live shape is
        virtiofs ESTALE/EIO; EACCES reproduces it without a sick mount."""
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            locked_dir = os.path.join(tmpdir, "Kid A")
            os.makedirs(locked_dir)
            stamped = os.path.join(locked_dir, self.FNAME)
            with open(stamped, "w") as fp:
                fp.write("fake audio")
            os.chmod(stamped, 0o000)
            album, ctx = self._album(tmpdir, local_path=stamped)
            try:
                result = process_completed_album(album, ctx, import_job_id=1)
            finally:
                os.chmod(stamped, 0o600)

            self.assertIsInstance(result, CompletionFailed)
            assert isinstance(result, CompletionFailed)
            self.assertEqual(result.reason, "source_open_failed_EACCES")
            self.assertTrue(os.path.exists(stamped))

    def test_mixed_album_fails_preflight_without_moving_stamped_file(self):
        """One stamped file, one unstamped: the pre-flight check fails the
        album BEFORE any move, so the stamped file stays at its event
        location — no move-then-rollback churn."""
        import tempfile

        from lib.download_processing import CompletionFailed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            event_src = os.path.join(
                tmpdir, "somewhere-unrelated", "01 Track One.mp3")
            os.makedirs(os.path.dirname(event_src))
            with open(event_src, "w") as fp:
                fp.write("fake audio")
            files = [
                make_download_file(
                    filename="@@w\\Music\\R\\Kid A\\01 Track One.mp3",
                    file_dir="@@w\\Music\\R\\Kid A",
                    size=len("fake audio"),
                ),
                make_download_file(
                    filename="@@w\\Music\\R\\Kid A\\02 Track Two.mp3",
                    file_dir="@@w\\Music\\R\\Kid A",
                    size=10,
                ),
            ]
            files[0].local_path = event_src
            album, ctx = self._album(tmpdir, local_path=None, files=files)

            with self.assertLogs("cratedigger", level=logging.ERROR) as logs:
                result = process_completed_album(
                    album, ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            assert isinstance(result, CompletionFailed)
            self.assertEqual(result.reason, "event_path_never_stamped")
            self.assertTrue(os.path.exists(event_src))
            self.assertIn("reason=event_path_never_stamped", "\n".join(logs.output))
            dst_dir = self._canonical_dir(ctx, album.files)
            self.assertTrue(
                not os.path.isdir(dst_dir) or os.listdir(dst_dir) == [])

    def test_already_moved_resume_still_skips(self):
        # Crash-resume: dst exists, the stamped source is gone. The file
        # counts as already moved; processing succeeds.
        import tempfile

        from lib.download_processing import Completed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            album, ctx = self._album(
                tmpdir, local_path=os.path.join(tmpdir, "Kid A", self.FNAME))
            dst_dir = self._canonical_dir(ctx, album.files)
            os.makedirs(dst_dir)
            _write_test_audio(os.path.join(dst_dir, self.FNAME))

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(self._moved(ctx, album.files), [self.FNAME])

    def test_unstamped_already_moved_resume_still_skips(self):
        # Even an unstamped file counts as already moved when the
        # destination copy exists — pre-flight must not hard-fail it.
        import tempfile

        from lib.download_processing import Completed, process_completed_album
        with tempfile.TemporaryDirectory() as tmpdir:
            album, ctx = self._album(tmpdir, local_path=None)
            dst_dir = self._canonical_dir(ctx, album.files)
            os.makedirs(dst_dir)
            _write_test_audio(os.path.join(dst_dir, self.FNAME))

            result = process_completed_album(album, ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(self._moved(ctx, album.files), [self.FNAME])


class TestAttemptScopedCanonicalFolder(unittest.TestCase):
    """Issue #550 phase 2: the canonical processing folder must be keyed
    to the attempt's own manifest, not just artist/title/year.

    Before this fix, two different download attempts for the same
    artist/title/year (e.g. a retry that grabbed from a different
    Soulseek user after the first attempt was abandoned) shared the
    SAME canonical folder. A stale prior attempt's leftover audio
    silently blended into a fresh attempt's validation scope, producing
    a false ``untracked_audio`` rejection (#550 defect #2). This test
    proves a fresh attempt's materialized folder contains ONLY that
    attempt's own manifest files — never files another attempt placed.
    """

    def test_materialize_never_blends_files_from_a_different_attempt(self):
        import tempfile

        from lib.download_materialization import (
            Materialized,
            _materialize_processing_dir,
        )
        from lib.processing_paths import canonical_folder_for_row
        from lib.staged_album import StagedAlbum
        with tempfile.TemporaryDirectory() as tmpdir:
            download_root = os.path.join(tmpdir, "downloads")
            os.makedirs(download_root)

            # A prior, DIFFERENT attempt (different source user/filename)
            # already materialized into the bare artist/title/year folder
            # and left an alien file behind — exactly what the pre-#550p2
            # canonical folder would have been, regardless of which files
            # produced it.
            stale_dir = os.path.join(
                download_root, "Test Artist - Test Album (2020)")
            os.makedirs(stale_dir)
            with open(os.path.join(stale_dir, "alien-track.flac"), "wb") as fp:
                fp.write(b"alien audio from a different attempt")

            # This attempt's real, event-stamped source file — a
            # different (username, filename) pair than whatever produced
            # the stale folder above.
            source_dir = os.path.join(download_root, "user2", "Music")
            os.makedirs(source_dir)
            source_file = os.path.join(source_dir, "01 - Track.flac")
            with open(source_file, "wb") as fp:
                fp.write(b"this attempt's real audio")

            stamped = make_download_file(
                filename="user2\\Music\\01 - Track.flac",
                file_dir="user2\\Music",
                username="user2",
            )
            stamped.local_path = source_file
            album = make_grab_list_entry(
                files=[stamped],
                artist="Test Artist",
                title="Test Album",
                year="2020",
                mb_release_id="",
            )
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = download_root

            staged_album = StagedAlbum.from_entry(
                album,
                default_path=canonical_folder_for_row(
                    album, os.path.join(ctx.cfg.processing_dir, "albums")),
            )
            result = _materialize_processing_dir(album, staged_album, ctx)

            self.assertIsInstance(result, Materialized)
            # This attempt's own manifest fingerprint must route it to a
            # folder distinct from the stale one.
            self.assertNotEqual(staged_album.current_path, stale_dir)
            validation_scope = os.listdir(staged_album.current_path)
            self.assertEqual(validation_scope, ["01 - Track.flac"])
            self.assertNotIn("alien-track.flac", validation_scope)
            # The stale folder (a different attempt's manifest) is
            # untouched — proves this isn't accidental cleanup, just
            # non-collision.
            self.assertEqual(
                os.listdir(stale_dir), ["alien-track.flac"])


class TestPreMatchRejectRecordsNullDistance(unittest.TestCase):
    """Issue #550 defect #4 (request 2812): a reject that fires BEFORE
    beets ever runs (the manifest guard's ``untracked_audio`` scenario)
    must never fabricate a measured distance.

    Invariant: no unmeasured distance is ever persisted as a number — a
    beets distance is only ever recorded when beets validation actually
    produced a candidate comparison. The Wrong Matches UI treats
    ``distance 0.0 <= threshold`` as a green, importable candidate; before
    this fix a pre-match reject wrote a fabricated ``0.0`` and the card
    rendered green with no evidence and no tracklist.
    """

    def test_untracked_audio_reject_persists_null_distance_not_fabricated_zero(self):
        import tempfile

        import msgspec

        from lib.download_materialization import Materialized
        from lib.download_processing import (
            CompletionDispatched,
            process_completed_album,
        )
        from lib.processing_paths import canonical_folder_for_row
        from lib.quality import ValidationResult

        with tempfile.TemporaryDirectory() as tmpdir:
            download_root = os.path.join(tmpdir, "downloads")
            os.makedirs(download_root)

            source_dir = os.path.join(download_root, "user1", "Music")
            os.makedirs(source_dir)
            source_file = os.path.join(source_dir, "01 - Track.mp3")
            with open(source_file, "wb") as fp:
                fp.write(b"this attempt's real, tracked audio")

            stamped = make_download_file(
                filename="user1\\Music\\01 - Track.mp3",
                file_dir="user1\\Music",
                username="user1",
            )
            stamped.local_path = source_file
            album = make_grab_list_entry(
                files=[stamped],
                artist="Palo Santo Reject",
                title="Wrong Match Test",
                year="2026",
                mb_release_id="test-mbid-2812",
                db_request_id=2812,
                db_source="request",
            )

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=2812, status="downloading",
                active_download_state={"files": [], "filetype": "mp3"},
                artist_name="Palo Santo Reject",
                album_title="Wrong Match Test",
                mb_release_id="test-mbid-2812",
            ))

            cfg = MagicMock()
            cfg.slskd_download_dir = download_root
            cfg.processing_dir = _private_processing_dir(tmpdir)
            cfg.beets_validation_enabled = True
            cfg.beets_staging_dir = os.path.join(tmpdir, "staging")
            # An unset MagicMock attribute answers ``__index__()`` with 1,
            # so a real path is required here — otherwise
            # ``log_validation_result``'s ``open(cfg.beets_tracking_file,
            # "a")`` silently reopens fd 1 (the process's real stdout) and
            # closes it on exit, corrupting output for the rest of the
            # test process.
            cfg.beets_tracking_file = os.path.join(tmpdir, "tracking.jsonl")

            ctx = make_ctx_with_fake_db(db, cfg=cfg)

            # A leftover untracked audio file already sits in the canonical
            # destination (e.g. left behind by a prior crashed attempt).
            # The manifest guard fires BEFORE beets_validate ever runs, so
            # no beets distance has been — or ever will be — measured for
            # this reject.
            canonical_path = canonical_folder_for_row(
                album, os.path.join(cfg.processing_dir, "albums"))
            os.makedirs(canonical_path, exist_ok=True)
            _write_test_audio(os.path.join(canonical_path, "leftover.mp3"))

            # This test owns the rejection/audit boundary, not the
            # materializer. A pre-existing extra file is deliberately an
            # authority guard now, so pass the lower-layer result through the
            # explicit processing seam to reach the pre-match rejection sink.
            def materialize_ready(*_args: object, **_kwargs: object) -> Materialized:
                return Materialized()

            result = process_completed_album(
                album,
                ctx,
                import_job_id=1,
                materialize_fn=materialize_ready,
            )

        self.assertIsInstance(result, CompletionDispatched)
        self.assertEqual(len(db.download_logs), 1)
        log = db.download_logs[0]
        self.assertEqual(log.outcome, "rejected")
        self.assertEqual(log.beets_scenario, "untracked_audio")

        # THE regression pin: no fabricated 0.0 default. A pre-match
        # reject never measured a beets distance, so both persisted sinks
        # must be NULL, not 0.0.
        self.assertIsNone(
            log.beets_distance,
            "pre-match untracked_audio reject must record NULL "
            "distance in the download_log.beets_distance column, not a "
            "fabricated 0.0 (issue #550 defect #4 / request 2812)")

        assert log.validation_result is not None
        vr = msgspec.json.decode(log.validation_result, type=ValidationResult)
        self.assertIsNone(
            vr.distance,
            "pre-match untracked_audio reject must record NULL "
            "distance in validation_result JSONB, not a fabricated 0.0 "
            "(issue #550 defect #4 / request 2812)")
        # The card must still surface for manual review — get_wrong_matches
        # keys on validation_result.failed_path.
        self.assertTrue(vr.failed_path)




class TestEvaluateStagedPathReadiness(unittest.TestCase):
    """Already-staged inputs are validated only against their manifest."""

    def _build(
        self,
        tmpdir: str,
        *,
        dir_exists: bool,
        files_present: bool,
        request_id: int = 1,
    ):
        from lib.staged_album import StagedAlbum

        current_path = os.path.join(tmpdir, "staged")
        entry = make_grab_list_entry(
            files=[make_download_file(filename="01 - Track.flac")],
            db_request_id=request_id,
            db_source="request",
            mb_release_id="test-mbid-509",
        )
        staged_album = StagedAlbum(current_path=current_path, request_id=request_id)
        if dir_exists:
            os.makedirs(current_path, exist_ok=True)
        if files_present:
            dest = staged_album.import_path_for(entry.files[0])
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "w") as fp:
                fp.write("fake audio")
        return entry, staged_album

    CASES: ClassVar = [
        ("directory_missing", False, False, MaterializeFailed, "staged_path_missing"),
        ("manifest_complete", True, True, Materialized, None),
        (
            "tracked_file_missing",
            True,
            False,
            MaterializeFailed,
            "staged_path_missing_tracked_files",
        ),
    ]

    def test_decision_table(self):
        for (
            desc,
            dir_exists,
            files_present,
            expected_type,
            expected_reason,
        ) in self.CASES:
            with self.subTest(desc=desc), tempfile.TemporaryDirectory() as tmpdir:
                from lib.download_materialization import (
                    _evaluate_staged_path_readiness,
                )
                entry, staged_album = self._build(
                    tmpdir,
                    dir_exists=dir_exists,
                    files_present=files_present,
                )
                result = _evaluate_staged_path_readiness(entry, staged_album)
                self.assertIsInstance(result, expected_type)
                if expected_reason is not None:
                    assert isinstance(result, MaterializeFailed)
                    self.assertEqual(result.reason, expected_reason)


def _fail_file(*, last_state=None, last_exception=None):
    from tests.helpers import make_download_file
    return make_download_file(last_state=last_state, last_exception=last_exception)


class TestSummarizeFileFailures(unittest.TestCase):
    """Pure unit tests for summarize_file_failures() — issue #564 C5."""

    def test_no_files_returns_none(self):
        from lib.download import summarize_file_failures
        self.assertIsNone(summarize_file_failures([]))

    CASES: ClassVar = [
        (
            "no evidence at all",
            [_fail_file()],
            None,
        ),
        (
            "only succeeded state contributes nothing",
            [_fail_file(last_state="Completed, Succeeded")],
            None,
        ),
        (
            "non-terminal state without exception contributes nothing",
            [_fail_file(last_state="InProgress")],
            None,
        ),
        (
            "single exception",
            [_fail_file(last_exception="Transfer rejected: Banned")],
            "1× 'Transfer rejected: Banned'",
        ),
        (
            "terminal state fallback when no exception",
            [_fail_file(last_state="Completed, Errored")],
            "1× 'Completed, Errored'",
        ),
        (
            "exception preferred over terminal state",
            [_fail_file(last_state="Completed, Errored",
                        last_exception="Read error: Connection reset by peer")],
            "1× 'Read error: Connection reset by peer'",
        ),
        (
            "same reason counted across files",
            [_fail_file(last_exception="Transfer rejected: Banned"),
             _fail_file(last_exception="Transfer rejected: Banned")],
            "2× 'Transfer rejected: Banned'",
        ),
        (
            "mixed reasons sorted by count desc then alphabetically",
            [_fail_file(last_exception="Transfer rejected: File not shared."),
             _fail_file(last_exception="Transfer rejected: File not shared."),
             _fail_file(last_exception="Read error: Connection reset by peer")],
            ("2× 'Transfer rejected: File not shared.', "
            "1× 'Read error: Connection reset by peer'"),
        ),
        (
            "tie broken alphabetically",
            [_fail_file(last_exception="Zed reason"),
             _fail_file(last_exception="Alpha reason")],
            "1× 'Alpha reason', 1× 'Zed reason'",
        ),
        (
            "one file with evidence, others without, still summarized",
            [_fail_file(),
             _fail_file(last_state="Completed, Succeeded"),
             _fail_file(last_exception="Transfer rejected: Banned")],
            "1× 'Transfer rejected: Banned'",
        ),
    ]

    def test_summary_table(self):
        from lib.download import summarize_file_failures
        for desc, files, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(summarize_file_failures(files), expected)


class TestVanishedTimeoutReason(unittest.TestCase):
    """Pure unit tests for _vanished_timeout_reason() — issue #564 C5/I2."""

    def test_no_evidence_claims_never_observed(self):
        from lib.download import _vanished_timeout_reason
        reason = _vanished_timeout_reason([_fail_file()])
        self.assertEqual(
            reason,
            "transfers vanished from slskd before any status was "
            "observed (slskd restart?)")

    def test_evidence_names_last_observed_summary(self):
        from lib.download import _vanished_timeout_reason
        reason = _vanished_timeout_reason(
            [_fail_file(last_exception="Transfer rejected: Banned")])
        self.assertEqual(
            reason,
            "transfers no longer in slskd — last observed: "
            "1× 'Transfer rejected: Banned'")

    def test_never_observed_phrase_only_appears_without_evidence(self):
        from lib.download import _vanished_timeout_reason
        with_evidence = _vanished_timeout_reason(
            [_fail_file(last_state="Completed, Errored")])
        without_evidence = _vanished_timeout_reason([_fail_file()])
        self.assertNotIn("before any status was observed", with_evidence)
        self.assertIn("before any status was observed", without_evidence)


class TestEnrichTimeoutReason(unittest.TestCase):
    """Pure unit tests for _enrich_timeout_reason() — issue #564 C5/I2."""

    def test_no_evidence_leaves_reason_unchanged(self):
        from lib.download import _enrich_timeout_reason
        reason = _enrich_timeout_reason("all 3 files errored", [_fail_file()])
        self.assertEqual(reason, "all 3 files errored")

    def test_appends_summary_when_evidence_exists(self):
        from lib.download import _enrich_timeout_reason
        reason = _enrich_timeout_reason(
            "all 1 files errored",
            [_fail_file(last_exception="Transfer rejected: Banned")])
        self.assertEqual(
            reason,
            "all 1 files errored — 1× 'Transfer rejected: Banned'")

    def test_does_not_duplicate_summary_already_embedded(self):
        """The vanished-branch reason already embeds the summary inline
        -- the generic append must not repeat it."""
        from lib.download import _enrich_timeout_reason, _vanished_timeout_reason
        files = [_fail_file(last_exception="Transfer rejected: Banned")]
        vanished_reason = _vanished_timeout_reason(files)
        enriched = _enrich_timeout_reason(vanished_reason, files)
        self.assertEqual(enriched, vanished_reason)
        self.assertEqual(enriched.count("Transfer rejected: Banned"), 1)


class TestHarvestTerminalTransferEvidence(unittest.TestCase):
    """Test harvest_terminal_transfer_evidence() — issue #564 root cause
    #3 / C3: the pre-purge harvest that stamps terminal slskd transfer
    evidence into active_download_state before remove_completed_downloads()
    discards slskd's own record of it.
    """

    def _row(self, request_id, *, files, processing_started_at=None):
        state_dict: dict[str, Any] = {
            "filetype": "flac",
            "enqueued_at": _utc_now_iso(),
            "files": files,
        }
        if processing_started_at is not None:
            state_dict["processing_started_at"] = processing_started_at
        return {
            "id": request_id,
            "album_title": "Test Album",
            "artist_name": "Test Artist",
            "year": 2020,
            "mb_release_id": f"test-mbid-{request_id}",
            "source": "request",
            "search_filetype_override": None,
            "target_format": None,
            "status": "downloading",
            "active_download_state": state_dict,
        }

    def _ctx(self, rows, slskd_downloads):
        fake_db = FakePipelineDB()
        for row in rows:
            fake_db.seed_request(row)
        ctx = make_ctx_with_fake_db(
            fake_db, slskd=FakeSlskdAPI(downloads=slskd_downloads))
        return ctx, fake_db

    def test_stamps_terminal_transfer_before_local_processing(self):
        enqueued_at = _utc_now_iso()
        row = self._row(1, files=[
            {"username": "user1", "filename": "user1\\Music\\01.flac",
             "file_dir": "user1\\Music", "size": 1000, "last_state": "InProgress"},
        ])
        row["active_download_state"]["enqueued_at"] = enqueued_at
        slskd_downloads = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": "user1\\Music\\01.flac",
                "id": "tid-1",
                "state": "Completed, Rejected",
                "bytesTransferred": 500,
                "exception": "Transfer rejected: Banned",
                # requestedAt at/after enqueued_at -- issue #822 item 2:
                # a timestampless terminal record can't prove attempt
                # membership and is excluded fail-closed.
                "requestedAt": enqueued_at,
            }]}],
        }]
        ctx, fake_db = self._ctx([row], slskd_downloads)
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)

        state = fake_db.request(1)["active_download_state"]
        f = state["files"][0]
        self.assertEqual(f["last_state"], "Completed, Rejected")
        self.assertEqual(f["last_exception"], "Transfer rejected: Banned")
        self.assertEqual(f["bytes_transferred"], 500)

    def test_skips_rows_with_processing_started(self):
        """Files already handed to local processing are no longer purely
        slskd-side transfers — the harvest must not touch them."""
        row = self._row(
            1,
            files=[
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 1000,
                 "last_state": "InProgress"},
            ],
            processing_started_at="2026-01-01T00:00:00+00:00",
        )
        slskd_downloads = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": "user1\\Music\\01.flac",
                "id": "tid-1",
                "state": "Completed, Succeeded",
                "bytesTransferred": 1000,
            }]}],
        }]
        ctx, fake_db = self._ctx([row], slskd_downloads)
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)

        self.assertEqual(fake_db.update_download_state_calls, [])

    def test_skips_files_already_terminal(self):
        """A file whose persisted last_state is already terminal is left
        alone — no redundant re-match/re-persist."""
        row = self._row(1, files=[
            {"username": "user1", "filename": "user1\\Music\\01.flac",
             "file_dir": "user1\\Music", "size": 1000,
             "last_state": "Completed, Succeeded"},
        ])
        ctx, fake_db = self._ctx([row], slskd_downloads=[])
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)

        self.assertEqual(fake_db.update_download_state_calls, [])

    def test_non_terminal_match_does_not_persist(self):
        """A matched but still-in-progress transfer is not evidence worth
        persisting here -- the ordinary poll cycle owns in-progress state."""
        row = self._row(1, files=[
            {"username": "user1", "filename": "user1\\Music\\01.flac",
             "file_dir": "user1\\Music", "size": 1000,
             "last_state": "InProgress"},
        ])
        slskd_downloads = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": "user1\\Music\\01.flac",
                "id": "tid-1",
                "state": "InProgress",
                "bytesTransferred": 200,
            }]}],
        }]
        ctx, fake_db = self._ctx([row], slskd_downloads)
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)

        self.assertEqual(fake_db.update_download_state_calls, [])

    def test_missing_active_download_state_is_skipped(self):
        row = self._row(1, files=[])
        row["active_download_state"] = None
        ctx, fake_db = self._ctx([row], slskd_downloads=[])
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)  # must not raise

        self.assertEqual(fake_db.update_download_state_calls, [])

    def test_undecodable_active_download_state_is_skipped(self):
        row = self._row(1, files=[])
        row["active_download_state"] = {"garbage": True}
        ctx, fake_db = self._ctx([row], slskd_downloads=[])
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)  # must not raise

        self.assertEqual(fake_db.update_download_state_calls, [])

    def test_snapshot_failure_is_a_noop(self):
        row = self._row(1, files=[
            {"username": "user1", "filename": "user1\\Music\\01.flac",
             "file_dir": "user1\\Music", "size": 1000,
             "last_state": "InProgress"},
        ])
        ctx, fake_db = self._ctx([row], slskd_downloads=[])
        cast(Any, ctx.slskd).transfers.get_all_downloads_error = RuntimeError("boom")
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)  # must not raise

        self.assertEqual(fake_db.update_download_state_calls, [])

    def test_one_rows_write_failure_does_not_abort_remaining_rows(self):
        """Review finding (issue #564): the per-row guard must cover the
        WHOLE loop body including the state write — one row's failing
        write must never abort harvesting the remaining rows, because
        the purge runs immediately after and would destroy their
        un-harvested evidence (the I1b failure mode)."""
        row1 = self._row(1, files=[
            {"username": "user1", "filename": "user1\\Music\\01.flac",
             "file_dir": "user1\\Music", "size": 1000,
             "last_state": "InProgress"},
        ])
        row2 = self._row(2, files=[
            {"username": "user2", "filename": "user2\\Music\\01.flac",
             "file_dir": "user2\\Music", "size": 1000,
             "last_state": "InProgress"},
        ])
        slskd_downloads = [
            {
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Errored",
                    "bytesTransferred": 0,
                    "exception": "Read error: Connection reset by peer",
                    "requestedAt": row1["active_download_state"]["enqueued_at"],
                }]}],
            },
            {
                "username": "user2",
                "directories": [{"directory": "user2\\Music", "files": [{
                    "filename": "user2\\Music\\01.flac",
                    "id": "tid-2",
                    "state": "Completed, Rejected",
                    "bytesTransferred": 0,
                    "exception": "Transfer rejected: Banned",
                    # requestedAt at/after row2's enqueued_at -- issue
                    # #822 item 2 fail-closed exclusion otherwise.
                    "requestedAt": row2["active_download_state"]["enqueued_at"],
                }]}],
            },
        ]
        ctx, fake_db = self._ctx([row1, row2], slskd_downloads)
        fake_db.set_update_download_state_error(
            1, RuntimeError("UPDATE failed"))
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)  # must not raise

        # Row 1's write failed — its persisted state is unchanged.
        state1 = fake_db.request(1)["active_download_state"]
        self.assertEqual(state1["files"][0]["last_state"], "InProgress")
        # Row 2 was still harvested despite row 1's failure.
        state2 = fake_db.request(2)["active_download_state"]
        self.assertEqual(
            state2["files"][0]["last_state"], "Completed, Rejected")
        self.assertEqual(
            state2["files"][0]["last_exception"], "Transfer rejected: Banned")

    def test_write_goes_through_status_guarded_update(self):
        """Review finding (issue #564): the harvest write must use the
        status-guarded update_download_state_if_downloading — a row a
        concurrent operator action flipped out of 'downloading' between
        the get_downloading() read and the write is never rewritten."""
        row = self._row(1, files=[
            {"username": "user1", "filename": "user1\\Music\\01.flac",
             "file_dir": "user1\\Music", "size": 1000,
             "last_state": "InProgress"},
        ])
        slskd_downloads = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": "user1\\Music\\01.flac",
                "id": "tid-1",
                "state": "Completed, Rejected",
                "bytesTransferred": 0,
                "exception": "Transfer rejected: Banned",
                "requestedAt": row["active_download_state"]["enqueued_at"],
            }]}],
        }]
        ctx, fake_db = self._ctx([row], slskd_downloads)
        # Simulate the concurrent flip: get_downloading() returns deep
        # copies, so flipping the stored row's status right after the
        # read mirrors an operator action landing mid-cycle.
        original_get_downloading = fake_db.get_downloading

        def get_downloading_then_flip():
            rows = original_get_downloading()
            fake_db._requests[1]["status"] = "unsearchable"
            return rows

        cast(Any, fake_db).get_downloading = get_downloading_then_flip
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)

        # The guarded write refused: persisted state is unchanged.
        state = fake_db.request(1)["active_download_state"]
        self.assertEqual(state["files"][0]["last_state"], "InProgress")
        self.assertNotIn("last_exception", state["files"][0])

    def test_same_status_new_incarnation_rejects_stale_harvest_and_continues(self):
        """A same-row A→B replacement cannot receive A's terminal evidence."""
        from lib.download import harvest_terminal_transfer_evidence

        first = self._row(1, files=[{
            "username": "user1",
            "filename": "user1\\Music\\01.flac",
            "file_dir": "user1\\Music",
            "size": 1000,
            "last_state": "InProgress",
        }])
        second = self._row(2, files=[{
            "username": "user2",
            "filename": "user2\\Music\\02.flac",
            "file_dir": "user2\\Music",
            "size": 2000,
            "last_state": "InProgress",
        }])
        first["active_download_state"]["current_path"] = "/same/path"
        first_enqueued_at = first["active_download_state"]["enqueued_at"]
        second_enqueued_at = second["active_download_state"]["enqueued_at"]
        replacement_enqueued_at = (
            datetime.now(UTC) + timedelta(seconds=1)
        ).isoformat()
        replacement_state = {
            **first["active_download_state"],
            "enqueued_at": replacement_enqueued_at,
            "last_progress_at": replacement_enqueued_at,
        }
        installed_b_rows = []
        downloads = [
            {
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Rejected",
                    "bytesTransferred": 0,
                    "exception": "stale A failure",
                    "requestedAt": first_enqueued_at,
                }]}],
            },
            {
                "username": "user2",
                "directories": [{"directory": "user2\\Music", "files": [{
                    "filename": "user2\\Music\\02.flac",
                    "id": "tid-2",
                    "state": "Completed, Errored",
                    "bytesTransferred": 0,
                    "exception": "current row failure",
                    "requestedAt": second_enqueued_at,
                }]}],
            },
        ]

        class ReplaceFirstIncarnationDB(FakePipelineDB):
            def update_download_state_if_downloading(
                self,
                request_id: int,
                state_json: str,
                *,
                expected_enqueued_at: str,
            ) -> bool:
                if request_id == 1:
                    self._requests[1]["active_download_state"] = replacement_state
                    installed_b_rows.append(self.get_request(1))
                return super().update_download_state_if_downloading(
                    request_id,
                    state_json,
                    expected_enqueued_at=expected_enqueued_at,
                )

        db = ReplaceFirstIncarnationDB()
        for row in (first, second):
            db.seed_request(row)
        ctx = make_ctx_with_fake_db(
            db,
            slskd=FakeSlskdAPI(downloads=downloads),
        )

        with self.assertLogs("cratedigger", level=logging.INFO) as captured:
            harvest_terminal_transfer_evidence(ctx)

        self.assertEqual(db.get_request(1), installed_b_rows[0])
        harvested_second = db.request(2)["active_download_state"]["files"][0]
        self.assertEqual(harvested_second["last_state"], "Completed, Errored")
        self.assertEqual(
            harvested_second["last_exception"], "current row failure",
        )
        self.assertIn(
            "1 downloading row(s)",
            "\n".join(captured.output),
        )

    def test_invalid_enqueue_witness_cannot_match_terminal_history(self):
        """Malformed attempt time is exclusion, never datetime.min authority."""
        from lib.download import harvest_terminal_transfer_evidence

        row = self._row(1, files=[{
            "username": "user1",
            "filename": "user1\\Music\\01.flac",
            "file_dir": "user1\\Music",
            "size": 1000,
            "last_state": "InProgress",
        }])
        row["active_download_state"]["enqueued_at"] = "not-a-timestamp"
        downloads = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": "user1\\Music\\01.flac",
                "id": "tid-old",
                "state": "Completed, Succeeded",
                "bytesTransferred": 1000,
                "requestedAt": "2020-01-01T00:00:00+00:00",
            }]}],
        }]
        ctx, fake_db = self._ctx([row], downloads)

        harvest_terminal_transfer_evidence(ctx)

        self.assertEqual(fake_db.update_download_state_calls, [])
        self.assertEqual(
            fake_db.request(1)["active_download_state"]["files"][0][
                "last_state"
            ],
            "InProgress",
        )

    def test_no_downloading_rows_is_a_noop(self):
        ctx, fake_db = self._ctx([], slskd_downloads=[])
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)

        self.assertEqual(fake_db.update_download_state_calls, [])

    def _row_with_boundary(self, request_id, *, enqueued_at, files):
        """Like ``_row`` but with a caller-controlled ``enqueued_at``
        attempt boundary — issue #820's stale-shadowing scenarios need a
        boundary strictly AFTER a months-old prior-attempt record."""
        return {
            "id": request_id,
            "album_title": "Test Album",
            "artist_name": "Test Artist",
            "year": 2020,
            "mb_release_id": f"test-mbid-{request_id}",
            "source": "request",
            "search_filetype_override": None,
            "target_format": None,
            "status": "downloading",
            "active_download_state": {
                "filetype": "mp3",
                "enqueued_at": enqueued_at,
                "files": files,
            },
        }

    def test_stale_prior_attempt_terminal_record_is_not_stamped(self):
        """I1 orchestration pin (issue #820, seam 1): with no OTHER
        candidate for the key, the harvest must not stamp a pre-boundary
        terminal record — it must leave the file's evidence exactly as
        unobserved as ``rederive_transfer_ids``/the poll path already
        require of themselves."""
        username = "HumDrum"
        filename = (
            "@@fdcrt\\POWER POP - Tagged\\Pictures, The - "
            "Pieces Of Eight (2005)\\09 - Downhill From Here.mp3"
        )
        row = self._row_with_boundary(
            1,
            enqueued_at="2026-07-22T02:01:25.759358+00:00",
            files=[
                {"username": username, "filename": filename,
                 "file_dir": "@@fdcrt\\POWER POP - Tagged\\Pictures, The - "
                              "Pieces Of Eight (2005)",
                 "size": 5274623},
            ],
        )
        slskd_downloads = [{
            "username": username,
            "directories": [{
                "directory": "@@fdcrt\\POWER POP - Tagged\\Pictures, The - "
                              "Pieces Of Eight (2005)",
                "files": [{
                    "filename": filename,
                    "id": "stale-may-id",
                    "state": "Completed, Succeeded",
                    "requestedAt": "2026-05-18T23:01:32+00:00",
                    "endedAt": "2026-05-18T23:04:58+00:00",
                    "bytesTransferred": 5274623,
                }],
            }],
        }]
        ctx, fake_db = self._ctx([row], slskd_downloads)
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)

        self.assertEqual(fake_db.update_download_state_calls, [])
        state = fake_db.request(1)["active_download_state"]
        self.assertIsNone(state["files"][0].get("last_state"))

    def test_genuine_post_boundary_terminal_record_is_stamped_despite_stale_shadow(
        self,
    ):
        """I1+I2 orchestration pin (issue #820, seam 1 — the core fix):
        the harvest must stamp the CURRENT attempt's genuine terminal
        state, never the stale prior-attempt record it ranks above by
        success-preference alone. Real production values: request 4190
        track 09 (HumDrum, The Pictures - Pieces of Eight, 2026-07-22)."""
        username = "HumDrum"
        file_dir = (
            "@@fdcrt\\POWER POP - Tagged\\Pictures, The - "
            "Pieces Of Eight (2005)"
        )
        filename = file_dir + "\\09 - Downhill From Here.mp3"
        row = self._row_with_boundary(
            1,
            enqueued_at="2026-07-22T02:01:25.759358+00:00",
            files=[
                {"username": username, "filename": filename,
                 "file_dir": file_dir, "size": 5274623},
            ],
        )
        slskd_downloads = [{
            "username": username,
            "directories": [{"directory": file_dir, "files": [
                {
                    # Prior HumDrum attempt, months old — still visible
                    # via includeRemoved=True.
                    "filename": filename,
                    "id": "stale-may-id",
                    "state": "Completed, Succeeded",
                    "requestedAt": "2026-05-18T23:01:32+00:00",
                    "endedAt": "2026-05-18T23:04:58+00:00",
                    "bytesTransferred": 5274623,
                },
                {
                    # Current attempt's genuine terminal error.
                    "filename": filename,
                    "id": "current-errored-id",
                    "state": "Completed, Errored",
                    "requestedAt": "2026-07-22T02:01:26.725+00:00",
                    "startedAt": "2026-07-22T02:05:00.222+00:00",
                    "endedAt": "2026-07-22T02:05:00.222+00:00",
                    "bytesTransferred": 0,
                    "exception": (
                        "Download of 09 - Downhill From Here.mp3 "
                        "reported as failed by HumDrum"
                    ),
                },
            ]}],
        }]
        ctx, fake_db = self._ctx([row], slskd_downloads)
        from lib.download import harvest_terminal_transfer_evidence

        harvest_terminal_transfer_evidence(ctx)

        state = fake_db.request(1)["active_download_state"]
        harvested = state["files"][0]
        self.assertEqual(harvested["last_state"], "Completed, Errored")
        self.assertEqual(
            harvested["last_exception"],
            "Download of 09 - Downhill From Here.mp3 reported as failed "
            "by HumDrum",
        )
        self.assertEqual(harvested.get("bytes_transferred", 0), 0)


class TestPollActiveDownloads(unittest.TestCase):
    """Test poll_active_downloads() — core polling function."""

    INVALID_ACTIVE_STATES: ClassVar[list[tuple[str, object]]] = [
        ("none", None),
        ("malformed_json", "{not-json"),
        ("malformed_shape", {"garbage": True}),
        ("missing_witness", {"filetype": "flac", "files": []}),
        ("empty_witness", {
            "filetype": "flac", "enqueued_at": "", "files": [],
        }),
        ("invalid_witness", {
            "filetype": "flac",
            "enqueued_at": "not-a-timestamp",
            "files": [],
        }),
    ]

    def _assert_no_poll_effects(
        self,
        db: FakePipelineDB,
        slskd: FakeSlskdAPI,
        *,
        expected_state_updates: int = 0,
        local_path: str | None = None,
    ) -> None:
        self.assertEqual(len(db.update_download_state_calls), expected_state_updates)
        self.assertEqual(db.status_history, [])
        self.assertEqual(db.download_logs, [])
        self.assertEqual(db.list_import_jobs(request_id=1), [])
        self.assertEqual(db.cooldowns_applied, [])
        self.assertEqual(db.denylist, [])
        self.assertEqual(slskd.transfers.enqueue_calls, [])
        self.assertEqual(slskd.transfers.cancel_download_calls, [])
        if local_path is not None:
            self.assertTrue(os.path.isfile(local_path))

    def _assert_pre_enqueue_failure_event(
        self,
        db: FakePipelineDB,
        *,
        reason: str,
        filetype: str,
        expected_verdict: str,
    ) -> None:
        from lib.failure_presentation import FailureEvidence, present_failure

        self.assertEqual(db.status_history, [(1, "wanted")])
        self.assertEqual(len(db.download_logs), 1)
        row = db.download_logs[0]
        self.assertEqual(
            (
                row.outcome,
                row.soulseek_username,
                row.filetype,
                row.beets_detail,
                row.error_message,
            ),
            ("failed", None, filetype, reason, reason),
        )
        presentation = present_failure(FailureEvidence(
            outcome=row.outcome or "",
            soulseek_username=row.soulseek_username,
            error_message=row.error_message,
            beets_detail=row.beets_detail,
        ))
        self.assertEqual(presentation.verdict, expected_verdict)
        self.assertEqual(db.cooldowns_applied, [])
        self.assertEqual(db.list_import_jobs(request_id=1), [])

    def _make_downloading_row(self, request_id=1, state_dict=None):
        """Build a mock album_requests row with status='downloading'."""
        if state_dict is None:
            state_dict = {
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            }
        return {
            "id": request_id,
            "album_title": "Test Album",
            "artist_name": "Test Artist",
            "year": 2020,
            # Per-row mbid — the fake enforces UNIQUE(mb_release_id).
            "mb_release_id": f"test-mbid-{request_id}",
            "source": "request",
            "search_filetype_override": None,
            "target_format": None,
            "status": "downloading",
            "active_download_state": state_dict,
        }

    def _make_poll_ctx(
        self,
        downloading_rows=None,
        slskd_downloads=None,
        fake_db: FakePipelineDB | None = None,
        slskd: FakeSlskdAPI | None = None,
        own_files: bool = True,
    ):
        """Build context with fake DB + fake slskd for polling.

        ``slskd`` lets a caller pass an already-constructed
        ``FakeSlskdAPI`` and keep a properly-typed local reference to it
        (avoiding a ``cast(FakeSlskdAPI, ctx.slskd)`` round-trip through
        the loosely-typed context field) — used by tests that need to
        both seed the initial snapshot AND queue follow-up snapshots or
        inspect call recordings after the poll runs.

        ``own_files`` defaults True because that is the steady state a
        `downloading` request settles into. Pass False for the pending-only
        world `_leave_claim_for_poll_recovery` leaves behind — a claim that
        persisted `active_download_state` while its POST stayed ambiguous,
        so the ledger holds intent and no acceptance. Without the opt-out
        the fixture makes that world unreachable, and the #1278 item 1
        ownership gate's whole behaviour change lives in it.
        """
        if slskd_downloads is None:
            # Default: return transfers that match the files
            slskd_downloads = [{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "tid-1",
                        "state": "InProgress",
                        "bytesTransferred": 1,
                    },
                ]}],
            }]
        cfg = cast(Any, _make_ctx().cfg)
        tmpdir = tempfile.mkdtemp(prefix="cratedigger-poll-test-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
        cfg.beets_staging_dir = os.path.join(tmpdir, "staging")
        os.makedirs(cfg.slskd_download_dir, exist_ok=True)
        os.makedirs(cfg.beets_staging_dir, exist_ok=True)
        # Stamp BEFORE seeding — seed_request deep-copies the row.
        for row in downloading_rows or []:
            raw_state = row.get("active_download_state")
            if not isinstance(raw_state, dict):
                continue
            for file_state in raw_state.get("files") or []:
                if not isinstance(file_state, dict):
                    continue
                file_dir = str(file_state.get("file_dir") or "")
                filename = str(file_state.get("filename") or "")
                folder_leaf = file_dir.replace("/", "\\").split("\\")[-1]
                basename = filename.replace("/", "\\").split("\\")[-1]
                if not folder_leaf or not basename:
                    continue
                local_dir = os.path.join(cfg.slskd_download_dir, folder_leaf)
                os.makedirs(local_dir, exist_ok=True)
                local_path = os.path.join(local_dir, basename)
                with open(local_path, "wb") as fp:
                    fp.write(b"test audio")
                # Production shape post-#146: every completed file carries
                # its event-stamped local_path. Tests exercising unstamped
                # behaviour opt out with an explicit "local_path": None.
                if "local_path" not in file_state:
                    file_state["local_path"] = local_path
        fake_db = fake_db or FakePipelineDB()
        for row in downloading_rows or []:
            fake_db.seed_request(row)
            # Steady state post-#571: a downloading row's files carry an
            # ACCEPTED write-ahead ledger row, and #1278 item 1 made that
            # ownership the gate on event stamping too. Seeding the state
            # without the ledger models the pending-only recovery world
            # instead, which silently disables stamping — deliberate only
            # when the caller asks for it.
            raw_state = row.get("active_download_state")
            if not isinstance(raw_state, dict) or not own_files:
                continue
            own_transfer_keys(
                fake_db,
                [
                    (str(f.get("username")), str(f.get("filename")))
                    for f in raw_state.get("files") or []
                    if isinstance(f, dict)
                ],
                request_id=int(row["id"]),
            )
        ctx = make_ctx_with_fake_db(
            fake_db,
            cfg=cfg,
            slskd=slskd if slskd is not None
            else FakeSlskdAPI(downloads=slskd_downloads),
        )
        return ctx, fake_db

    def test_poll_lost_ownership_stops_before_every_verdict_effect(self):
        """A same-status A→B replacement wins before stale effects land."""
        from lib.download import poll_active_downloads
        from lib.quality import ActiveDownloadState

        row = self._make_downloading_row()
        original_state = row["active_download_state"]
        original_state["current_path"] = "/same/path"
        original_enqueued_at = original_state["enqueued_at"]
        replacement_enqueued_at = (
            datetime.now(UTC) + timedelta(seconds=1)
        ).isoformat()
        replacement_state = {
            **original_state,
            "enqueued_at": replacement_enqueued_at,
            "last_progress_at": replacement_enqueued_at,
        }
        installed_b_rows = []

        class ReplaceIncarnationOnPersistDB(FakePipelineDB):
            def update_download_state_if_downloading(
                self,
                request_id: int,
                state_json: str,
                *,
                expected_enqueued_at: str,
            ) -> bool:
                self._requests[request_id][
                    "active_download_state"
                ] = replacement_state
                installed_b_rows.append(self.get_request(request_id))
                return super().update_download_state_if_downloading(
                    request_id,
                    state_json,
                    expected_enqueued_at=expected_enqueued_at,
                )

        losing_db = ReplaceIncarnationOnPersistDB()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                    "requestedAt": original_enqueued_at,
                }]}],
            }],
            fake_db=losing_db,
        )
        seeded_state = fake_db.request(1)["active_download_state"]
        assert isinstance(seeded_state, dict)
        local_path = seeded_state["files"][0]["local_path"]
        replacement_state["files"][0]["local_path"] = local_path
        replacement_state_json = ActiveDownloadState.from_dict(
            replacement_state,
        ).to_json()

        poll_active_downloads(ctx)

        current = fake_db.get_request(1)
        assert current is not None
        self.assertEqual(current["status"], "downloading")
        self.assertEqual(current, installed_b_rows[0])
        current_state = current["active_download_state"]
        assert isinstance(current_state, dict)
        self.assertEqual(
            ActiveDownloadState.from_dict(current_state).to_json(),
            replacement_state_json,
        )
        self.assertEqual(
            [request_id for request_id, _state_json
             in fake_db.update_download_state_calls],
            [1],
        )
        self.assertEqual(fake_db.list_import_jobs(request_id=1), [])
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.status_history, [])
        self.assertEqual(fake_db.cooldowns_applied, [])
        self.assertEqual(fake_db.denylist, [])
        slskd = cast(FakeSlskdAPI, ctx.slskd)
        self.assertEqual(slskd.transfers.enqueue_calls, [])
        self.assertEqual(slskd.transfers.cancel_download_calls, [])
        self.assertTrue(os.path.isfile(local_path))

    def test_handoff_between_poll_persist_and_failure_effect_blocks_effect(self):
        """A committed processor owner wins over timeout and file-retry effects."""
        from lib.download import poll_active_downloads

        for verdict in ("timeout", "retry"):
            with self.subTest(verdict=verdict):
                row = self._make_downloading_row()
                witness = row["active_download_state"]["enqueued_at"]
                if verdict == "retry":
                    row["active_download_state"]["files"].append({
                        "username": "user1",
                        "filename": "user1\\Music\\02.flac",
                        "file_dir": "user1\\Music",
                        "size": 30000000,
                    })

                class HandoffAfterPollPersistDB(FakePipelineDB):
                    def update_download_state_if_downloading(
                        self,
                        request_id: int,
                        state_json: str,
                        *,
                        expected_enqueued_at: str,
                    ) -> bool:
                        applied = super().update_download_state_if_downloading(
                            request_id,
                            state_json,
                            expected_enqueued_at=expected_enqueued_at,
                        )
                        if applied:
                            handoff = self.handoff_automation_import(
                                request_id=request_id,
                                expected_enqueued_at=expected_enqueued_at,
                                canonical_path="/processing/albums/owned",
                                message="concurrent winner",
                            )
                            if not handoff.committed:
                                raise AssertionError(handoff.outcome)
                        return applied

                files = [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Rejected",
                    "exception": "rejected",
                    "bytesTransferred": 0,
                    "requestedAt": witness,
                }]
                if verdict == "retry":
                    files.append({
                        "filename": "user1\\Music\\02.flac",
                        "id": "tid-2",
                        "state": "InProgress",
                        "bytesTransferred": 1,
                        "requestedAt": witness,
                    })
                slskd = FakeSlskdAPI(downloads=[{
                    "username": "user1",
                    "directories": [{
                        "directory": "user1\\Music",
                        "files": files,
                    }],
                }])
                ctx, fake_db = self._make_poll_ctx(
                    downloading_rows=[row],
                    fake_db=HandoffAfterPollPersistDB(),
                    slskd=slskd,
                )

                poll_active_downloads(ctx)

                self.assertEqual(fake_db.request(1)["status"], "processing")
                self.assertEqual(
                    len(fake_db.list_import_jobs(request_id=1)),
                    1,
                )
                self.assertEqual(fake_db.download_logs, [])
                self.assertEqual(fake_db.cooldowns_applied, [])
                self.assertEqual(slskd.transfers.enqueue_calls, [])
                self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def _download_state(self, fake_db: FakePipelineDB, request_id: int = 1):
        state = fake_db.request(request_id)["active_download_state"]
        assert isinstance(state, dict)
        return state

    def test_poll_ingests_events_and_persists_local_path(self):
        """Issue #146 phase 1 wiring: a DownloadFileComplete event seen at
        poll time lands as ``local_path`` in the persisted state."""
        import json as _json

        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        event_timestamp = (
            datetime.fromisoformat(
                row["active_download_state"]["enqueued_at"],
            ) + timedelta(seconds=1)
        ).isoformat()
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])
        fake_db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")
        slskd = cast(Any, ctx.slskd)
        slskd.events.set_events([
            slskd.events.make_event(
                id="ev-1", timestamp=event_timestamp,
                type="DownloadFileComplete",
                data=_json.dumps({
                    "version": 0,
                    "localFilename": "/dl/Music/01.flac",
                    "remoteFilename": "user1\\Music\\01.flac",
                    "transfer": {
                        "id": "tid-1", "username": "user1",
                        "filename": "user1\\Music\\01.flac",
                        "size": 30000000,
                    },
                })),
            slskd.events.make_event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z",
                type="Noise", data="{}"),
        ])

        poll_active_downloads(ctx)

        state = self._download_state(fake_db)
        self.assertEqual(state["files"][0]["local_path"], "/dl/Music/01.flac")
        cursor = fake_db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-1")

    def test_poll_fetches_snapshot_before_ingesting_events(self):
        """Ordering pin (#146 phase 2): the transfer snapshot is taken
        BEFORE event ingestion, so any transfer the snapshot shows
        Completed already has its DownloadFileComplete event in the feed
        — closing the same-cycle race that made healthy completions
        process unstamped."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, _ = self._make_poll_ctx(downloading_rows=[row])

        poll_active_downloads(ctx)

        call_log = cast(Any, ctx.slskd).call_log
        self.assertIn("transfers.get_all_downloads", call_log)
        self.assertIn("events.list", call_log)
        self.assertLess(
            call_log.index("transfers.get_all_downloads"),
            call_log.index("events.list"))

    def test_real_ingest_outcomes_refresh_and_exclude_b(self):
        """Normal, no-op, and raised ingestion all refresh away snapshot B."""
        from lib.download import poll_active_downloads
        from tests.fakes.slskd import FakeSlskdEvents

        def make_installing_events(
            slskd_api: FakeSlskdAPI,
            database: FakePipelineDB,
            replacement: dict[str, object],
            installed: list[AlbumRequestRow | None],
            ingest_outcome: str,
        ) -> FakeSlskdEvents:
            class InstallBOnEventList(FakeSlskdEvents):
                installed = False

                def list(self, *, limit=500, offset=0):
                    if not self.installed:
                        self.installed = True
                        database._requests[1][
                            "active_download_state"
                        ] = replacement
                        installed.append(database.get_request(1))
                    if ingest_outcome == "raised":
                        raise RuntimeError("events down")
                    return super().list(limit=limit, offset=offset)

            return InstallBOnEventList(slskd_api)

        for outcome in ("ingested", "no_new_events", "raised"):
            with self.subTest(outcome=outcome):
                now = datetime.now(UTC)
                a_enqueued_at = (now - timedelta(seconds=2)).isoformat()
                b_enqueued_at = (now - timedelta(seconds=1)).isoformat()
                row = self._make_downloading_row(state_dict={
                    "filetype": "flac",
                    "enqueued_at": a_enqueued_at,
                    "files": [{
                        "username": "user1",
                        "filename": "user1\\Music\\01.flac",
                        "file_dir": "user1\\Music",
                        "size": 30000000,
                    }],
                })
                snapshot = [{
                    "username": "user1",
                    "directories": [{
                        "directory": "user1\\Music",
                        "files": [{
                            "filename": "user1\\Music\\01.flac",
                            "id": "tid-a",
                            "state": "InProgress",
                            "bytesTransferred": 1,
                            "requestedAt": a_enqueued_at,
                        }],
                    }],
                }]
                replacement_state = {
                    **row["active_download_state"],
                    "enqueued_at": b_enqueued_at,
                    "last_progress_at": b_enqueued_at,
                    "files": [
                        dict(file_state)
                        for file_state in row["active_download_state"]["files"]
                    ],
                }
                installed_rows: list[AlbumRequestRow | None] = []
                slskd = FakeSlskdAPI(downloads=snapshot)
                ctx, fake_db = self._make_poll_ctx(
                    downloading_rows=[row],
                    slskd_downloads=snapshot,
                    slskd=slskd,
                )

                slskd.events = make_installing_events(
                    slskd,
                    fake_db,
                    replacement_state,
                    installed_rows,
                    outcome,
                )
                fake_db.upsert_slskd_event_cursor(
                    "ev-cursor", (now - timedelta(minutes=1)).isoformat(),
                )
                events = [slskd.events.make_event(
                    id="ev-cursor",
                    timestamp=(now - timedelta(minutes=1)).isoformat(),
                    type="Noise",
                    data="{}",
                )]
                if outcome == "ingested":
                    events.insert(0, slskd.events.make_event(
                        id="ev-new",
                        timestamp=now.isoformat(),
                        type="Noise",
                        data="{}",
                    ))
                slskd.events.set_events(events)

                poll_active_downloads(ctx)

                self.assertEqual(fake_db.get_request(1), installed_rows[0])
                self._assert_no_poll_effects(fake_db, slskd)

    def test_poll_refresh_failure_skips_every_row(self):
        """Without the post-ingest refresh, no prefetched row may poll."""
        from lib.download import poll_active_downloads

        class FailRefreshDB(FakePipelineDB):
            get_downloading_call_count = 0

            def get_downloading(self):
                self.get_downloading_call_count += 1
                if self.get_downloading_call_count == 2:
                    raise RuntimeError("refresh failed")
                return super().get_downloading()

        row = self._make_downloading_row()
        db = FailRefreshDB()
        slskd = FakeSlskdAPI()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            fake_db=db,
            slskd=slskd,
        )
        fake_db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00+00:00",
        )
        slskd.events.set_events([slskd.events.make_event(
            id="ev-cursor",
            timestamp="2026-07-01T00:00:00+00:00",
            type="Noise",
            data="{}",
        )])

        poll_active_downloads(ctx)

        self.assertEqual(db.get_downloading_call_count, 2)
        self._assert_no_poll_effects(fake_db, slskd)

    def test_invalid_initial_incarnations_are_excluded_without_reset(self):
        """Missing, malformed, empty, and invalid witnesses own no snapshot."""
        from lib.download import poll_active_downloads

        for name, invalid_state in self.INVALID_ACTIVE_STATES:
            with self.subTest(state=name):
                row = self._make_downloading_row()
                row["active_download_state"] = invalid_state
                slskd = FakeSlskdAPI()
                ctx, fake_db = self._make_poll_ctx(
                    downloading_rows=[row],
                    slskd=slskd,
                )
                poll_active_downloads(ctx)

                self.assertEqual(
                    slskd.transfers.get_all_downloads_calls,
                    [],
                )
                self._assert_no_poll_effects(fake_db, slskd)

    def test_invalid_only_cycle_still_ingests_event_feed(self):
        """Invalid poll admission cannot stop cursor maintenance."""
        from lib.download import poll_active_downloads

        row = self._make_downloading_row()
        row["active_download_state"] = None
        slskd = FakeSlskdAPI()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd=slskd,
        )

        poll_active_downloads(ctx)

        self.assertEqual(slskd.events.list_calls, [(1, 0)])
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [])
        self.assertEqual(fake_db.status_history, [])
        self.assertEqual(fake_db.update_download_state_calls, [])

    def test_invalid_refreshed_incarnations_are_excluded_without_reset(self):
        """A valid pre-snapshot A cannot carry an invalid refreshed row."""
        from lib.download import poll_active_downloads
        from tests.fakes.slskd import FakeSlskdEvents

        def make_invalidating_events(
            slskd_api: FakeSlskdAPI,
            database: FakePipelineDB,
            invalid: object,
        ) -> FakeSlskdEvents:
            class InstallInvalidOnEventList(FakeSlskdEvents):
                installed = False

                def list(self, *, limit=500, offset=0):
                    if not self.installed:
                        self.installed = True
                        database._requests[1][
                            "active_download_state"
                        ] = invalid
                    return super().list(limit=limit, offset=offset)

            return InstallInvalidOnEventList(slskd_api)

        for name, invalid_state in self.INVALID_ACTIVE_STATES:
            with self.subTest(state=name):
                row = self._make_downloading_row()
                slskd = FakeSlskdAPI()
                ctx, fake_db = self._make_poll_ctx(
                    downloading_rows=[row],
                    slskd=slskd,
                )

                slskd.events = make_invalidating_events(
                    slskd,
                    fake_db,
                    invalid_state,
                )
                fake_db.upsert_slskd_event_cursor(
                    "ev-cursor", "2026-07-01T00:00:00+00:00",
                )
                slskd.events.set_events([slskd.events.make_event(
                    id="ev-cursor",
                    timestamp="2026-07-01T00:00:00+00:00",
                    type="Noise",
                    data="{}",
                )])

                poll_active_downloads(ctx)

                self.assertEqual(
                    len(slskd.transfers.get_all_downloads_calls),
                    1,
                )
                self._assert_no_poll_effects(fake_db, slskd)

    def test_real_event_ingest_stamps_b_but_poll_admission_excludes_it(self):
        """Real U3 stamps refreshed B; U4 still requires pre-snapshot B."""
        import json as _json

        from lib.download import poll_active_downloads
        from tests.fakes.slskd import FakeSlskdEvents

        a_enqueued_at = "2026-07-01T09:00:00+00:00"
        b_enqueued_at = "2026-07-01T10:00:00+00:00"
        a_filename = "user-a\\Album\\01.flac"
        b_filename = "user-b\\Album\\01.flac"
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": a_enqueued_at,
            "files": [{
                "username": "user-a",
                "filename": a_filename,
                "file_dir": "user-a\\Album",
                "size": 30000000,
            }],
        })
        transfer_snapshot = [{
            "username": "user-a",
            "directories": [{"directory": "user-a\\Album", "files": [{
                "filename": a_filename,
                "id": "tid-a",
                "state": "InProgress",
                "bytesTransferred": 1,
                "requestedAt": a_enqueued_at,
            }]}],
        }]
        b_state = {
            "filetype": "flac",
            "enqueued_at": b_enqueued_at,
            "files": [{
                "username": "user-b",
                "filename": b_filename,
                "file_dir": "user-b\\Album",
                "size": 30000000,
            }],
        }

        slskd = FakeSlskdAPI(downloads=transfer_snapshot)
        db = FakePipelineDB()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=transfer_snapshot,
            fake_db=db,
            slskd=slskd,
        )

        class InstallBOnEventList(FakeSlskdEvents):
            installed = False

            def list(self, *, limit=500, offset=0):
                if not self.installed:
                    self.installed = True
                    fake_db._requests[1]["active_download_state"] = b_state
                return super().list(limit=limit, offset=offset)

        slskd.events = InstallBOnEventList(slskd)
        # Incarnation B is a real fresh enqueue, so its own queue key
        # carries an accepted write-ahead ledger row exactly as A's does
        # (_make_poll_ctx seeds A's). Without it the #1278 item 1
        # ownership gate refuses B's completion and the stamp under test
        # never happens.
        own_transfer_keys(fake_db, [("user-b", b_filename)], request_id=1)
        fake_db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00+00:00",
        )
        slskd.events.set_events([
            slskd.events.make_event(
                id="ev-b",
                timestamp="2026-07-01T10:01:00+00:00",
                type="DownloadFileComplete",
                data=_json.dumps({
                    "version": 0,
                    "localFilename": "/dl/user-b/Album/01.flac",
                    "remoteFilename": b_filename,
                    "transfer": {
                        "id": "tid-b",
                        "username": "user-b",
                        "filename": b_filename,
                        "size": 30000000,
                    },
                }),
            ),
            slskd.events.make_event(
                id="ev-cursor",
                timestamp="2026-07-01T00:00:00+00:00",
                type="Noise",
                data="{}",
            ),
        ])
        poll_active_downloads(ctx)

        persisted = fake_db.request(1)["active_download_state"]
        self.assertEqual(
            persisted["files"][0]["local_path"],
            "/dl/user-b/Album/01.flac",
        )
        self.assertEqual(persisted["enqueued_at"], b_enqueued_at)
        self._assert_no_poll_effects(
            fake_db,
            slskd,
            expected_state_updates=1,
        )
        self.assertLess(
            slskd.call_log.index("transfers.get_all_downloads"),
            slskd.call_log.index("events.list"),
        )

    def test_real_ingest_lost_cas_excludes_b_with_and_without_cursor_gap(self):
        """Both U3 lost-CAS outcomes keep B off A's transfer snapshot."""
        import json as _json

        from lib.download import poll_active_downloads
        from lib.slskd_events import EVENT_PAGE_LIMIT, MAX_EVENT_PAGES

        def make_lost_stamp_db(
            replacement: dict[str, object],
            installed: list[AlbumRequestRow | None],
        ) -> FakePipelineDB:
            class LoseStampToBDB(FakePipelineDB):
                lost_once = False

                def update_download_state_if_downloading(
                    self,
                    request_id: int,
                    state_json: str,
                    *,
                    expected_enqueued_at: str,
                ) -> bool:
                    if not self.lost_once:
                        self.lost_once = True
                        self._requests[request_id][
                            "active_download_state"
                        ] = replacement
                        installed.append(self.get_request(request_id))
                    return super().update_download_state_if_downloading(
                        request_id,
                        state_json,
                        expected_enqueued_at=expected_enqueued_at,
                    )

            return LoseStampToBDB()

        for cursor_gap in (False, True):
            with self.subTest(cursor_gap=cursor_gap):
                now = datetime.now(UTC)
                a_enqueued_at = (now - timedelta(seconds=2)).isoformat()
                b_enqueued_at = now.isoformat()
                filename = "user1\\Music\\01.flac"
                row = self._make_downloading_row(state_dict={
                    "filetype": "flac",
                    "enqueued_at": a_enqueued_at,
                    "current_path": "/same/path",
                    "files": [{
                        "username": "user1",
                        "filename": filename,
                        "file_dir": "user1\\Music",
                        "size": 30000000,
                    }],
                })
                b_state = {
                    **row["active_download_state"],
                    "enqueued_at": b_enqueued_at,
                    "last_progress_at": b_enqueued_at,
                    "files": [
                        dict(file_state)
                        for file_state in row["active_download_state"]["files"]
                    ],
                }
                installed_b_rows: list[AlbumRequestRow | None] = []

                snapshot = [{
                    "username": "user1",
                    "directories": [{
                        "directory": "user1\\Music",
                        "files": [{
                            "filename": filename,
                            "id": "tid-a",
                            "state": "Completed, Succeeded",
                            "bytesTransferred": 30000000,
                            "requestedAt": a_enqueued_at,
                        }],
                    }],
                }]
                slskd = FakeSlskdAPI(downloads=snapshot)
                db = make_lost_stamp_db(b_state, installed_b_rows)
                ctx, fake_db = self._make_poll_ctx(
                    downloading_rows=[row],
                    slskd_downloads=snapshot,
                    fake_db=db,
                    slskd=slskd,
                )
                initial = fake_db.request(1)["active_download_state"]
                local_path = initial["files"][0]["local_path"]
                cursor_timestamp = (now - timedelta(minutes=1)).isoformat()
                event_timestamp = (now - timedelta(seconds=1)).isoformat()
                fake_db.upsert_slskd_event_cursor(
                    "ev-cursor",
                    cursor_timestamp,
                )
                completion = slskd.events.make_event(
                    id="ev-a",
                    timestamp=event_timestamp,
                    type="DownloadFileComplete",
                    data=_json.dumps({
                        "version": 0,
                        "localFilename": "/dl/user1/Music/01.flac",
                        "remoteFilename": filename,
                        "transfer": {
                            "id": "tid-a",
                            "username": "user1",
                            "filename": filename,
                            "size": 30000000,
                        },
                    }),
                )
                if cursor_gap:
                    events = [completion]
                    events.extend(
                        slskd.events.make_event(
                            id=f"noise-{index}",
                            timestamp=event_timestamp,
                            type="Noise",
                            data="{}",
                        )
                        for index in range(EVENT_PAGE_LIMIT * MAX_EVENT_PAGES)
                    )
                else:
                    events = [
                        completion,
                        slskd.events.make_event(
                            id="ev-cursor",
                            timestamp=cursor_timestamp,
                            type="Noise",
                            data="{}",
                        ),
                    ]
                slskd.events.set_events(events)

                poll_active_downloads(ctx)

                self.assertEqual(fake_db.get_request(1), installed_b_rows[0])
                cursor = fake_db.get_slskd_event_cursor()
                assert cursor is not None
                self.assertEqual(
                    cursor["last_event_id"],
                    "ev-a" if cursor_gap else "ev-cursor",
                )
                self._assert_no_poll_effects(
                    fake_db,
                    slskd,
                    expected_state_updates=1,
                    local_path=local_path,
                )

    def test_exact_admission_preserves_refreshed_order_and_decoded_witness(self):
        """Admission returns only exact pairs in refreshed DB order."""
        from lib.download import (
            _admit_download_incarnations,
            _decode_valid_download_incarnations,
        )

        first = self._make_downloading_row(
            request_id=1,
            state_dict={
                "filetype": "flac",
                "enqueued_at": "2026-07-01T10:00:00Z",
                "files": [],
            },
        )
        invalid = self._make_downloading_row(
            request_id=2,
            state_dict={
                "filetype": "flac",
                "enqueued_at": "invalid",
                "files": [],
            },
        )
        third = self._make_downloading_row(
            request_id=3,
            state_dict={
                "filetype": "flac",
                "enqueued_at": "2026-07-01T12:00:00+00:00",
                "files": [],
            },
        )

        pre_db = FakePipelineDB()
        for row in (first, third):
            pre_db.seed_request(row)
        pre_snapshot = _decode_valid_download_incarnations(
            pre_db.get_downloading(),
            phase="test pre-snapshot",
        )
        replacement = self._make_downloading_row(
            request_id=1,
            state_dict={
                "filetype": "flac",
                "enqueued_at": "2026-07-01T10:00:01Z",
                "files": [],
            },
        )
        refreshed_db = FakePipelineDB()
        for row in (invalid, third, replacement):
            refreshed_db.seed_request(row)
        admitted = _admit_download_incarnations(
            pre_snapshot,
            refreshed_db.get_downloading(),
            refreshed_phase="test refresh",
        )

        self.assertEqual(
            [
                (row["id"], state.enqueued_at)
                for row, state in admitted
            ],
            [
                (3, "2026-07-01T12:00:00+00:00"),
            ],
        )

    def test_poll_survives_events_api_failure(self):
        """Events API outage stamps nothing this cycle — polling continues."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": _utc_now_iso(),
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "local_path": None},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])
        cast(Any, ctx.slskd).events.list_error = RuntimeError("events down")

        poll_active_downloads(ctx)  # must not raise

        # Row still got polled (transfer id re-derived, state persisted).
        self.assertIsNone(
            self._download_state(fake_db)["files"][0].get("local_path"))

    def test_poll_active_no_downloading(self):
        """No downloading albums → no-op."""
        from lib.download import poll_active_downloads
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[])
        poll_active_downloads(ctx)
        self.assertEqual(fake_db._import_jobs, [])
        self.assertEqual(fake_db.download_logs, [])

    def test_poll_active_all_complete(self):
        """Completion publishes one exact owner before any file mutation."""
        from lib.download import poll_active_downloads
        from lib.import_queue import IMPORT_JOB_AUTOMATION
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                    # requestedAt at/after enqueued_at -- issue #822
                    # item 2 fail-closed exclusion otherwise.
                    "requestedAt": row["active_download_state"]["enqueued_at"],
                }]}],
            }],
        )

        source_path = fake_db.request(1)[
            "active_download_state"
        ]["files"][0]["local_path"]

        poll_active_downloads(ctx)

        self.assertGreaterEqual(len(fake_db.update_download_state_calls), 1)
        request = fake_db.request(1)
        self.assertEqual(request["status"], "processing")
        self.assertIsNotNone(request["active_automation_import_job_id"])
        self.assertIsNotNone(self._download_state(fake_db)["current_path"])
        self.assertIsNotNone(
            self._download_state(fake_db)["processing_started_at"],
        )
        jobs = fake_db.list_import_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].job_type, IMPORT_JOB_AUTOMATION)
        self.assertEqual(jobs[0].request_id, 1)
        self.assertEqual(
            request["active_automation_import_job_id"],
            jobs[0].id,
        )
        self.assertEqual(jobs[0].expected_request_status, "processing")
        self.assertTrue(os.path.isfile(source_path))

    def _pending_only_world(self, *, accept: bool):
        """One poll cycle over the world `_leave_claim_for_poll_recovery`
        leaves behind, composed end to end.

        `_claim_initial_download_ownership` persists `active_download_state`
        via `writer.claim_downloading` BEFORE `_enqueue_with_claim_outcome`
        writes the ledger row or issues the POST, so a claim whose POST
        stayed ambiguous durably leaves a `downloading` request whose keys
        hold pending intent and no acceptance. `accept=True` is the same
        world after the POST was confirmed — the must-still-work control.
        """
        import json as _json

        from lib.download import poll_active_downloads
        from lib.quality import ActiveDownloadState

        enqueued_at = "2026-07-01T09:00:00+00:00"
        filename = "user1\\Music\\01.flac"
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": enqueued_at,
            "files": [{
                "username": "user1",
                "filename": filename,
                "file_dir": "user1\\Music",
                "size": 30000000,
                # No event stamp yet: this attempt's completion is in the
                # window the poll is about to ingest.
                "local_path": None,
            }],
        })
        snapshot = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": filename,
                "id": "tid-1",
                "state": "Completed, Succeeded",
                "bytesTransferred": 30000000,
                "requestedAt": enqueued_at,
            }]}],
        }]
        slskd = FakeSlskdAPI(downloads=snapshot)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=snapshot,
            slskd=slskd,
            own_files=False,
        )
        # Write-ahead intent, exactly as `_write_ahead_transfer_ledger`
        # writes it before the POST.
        fake_db.record_transfer_enqueue([TransferLedgerRow(
            request_id=1, username="user1", filename=filename)])
        if accept:
            fake_db.confirm_transfer_enqueue(
                "user1", filename, request_id=1)
        fake_db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00+00:00")
        local_path = os.path.join(
            ctx.cfg.slskd_download_dir, "Music", "01.flac")
        slskd.events.set_events([
            slskd.events.make_event(
                id="ev-complete",
                timestamp="2026-07-01T10:00:00+00:00",
                type="DownloadFileComplete",
                data=_json.dumps({
                    "version": 0,
                    "localFilename": local_path,
                    "remoteFilename": filename,
                    "transfer": {
                        "id": "tid-1",
                        "username": "user1",
                        "filename": filename,
                        "size": 30000000,
                    },
                }),
            ),
            slskd.events.make_event(
                id="ev-cursor",
                timestamp="2026-07-01T00:00:00+00:00",
                type="Noise",
                data="{}",
            ),
        ])

        poll_active_downloads(ctx)

        request = fake_db.request(1)
        state = ActiveDownloadState.from_dict(
            request["active_download_state"])
        return ctx, request, state

    def test_pending_only_ledger_world_refuses_the_stamp_and_hard_fails(self):
        """#1278 item 1 review F3: the composed consequence of the gate.

        No poll test could reach this world before — `_make_poll_ctx`
        seeded accepted ledger rows for every downloading row — yet it is
        exactly where the gate changes behaviour: the stamp is refused,
        the poll still publishes the processing owner, and the processor
        then hard-fails `event_path_never_stamped`, which returns the
        request to `wanted` and re-downloads the whole album.
        """
        from lib.download_processing import (
            CompletionFailed,
            process_completed_album,
        )
        from lib.download_reconstruction import reconstruct_grab_list_entry

        ctx, request, state = self._pending_only_world(accept=False)

        self.assertIsNone(state.files[0].local_path)
        # The poll still hands off — refusing the stamp is not refusing
        # the lifecycle; the processor owns the source-path contract.
        self.assertEqual(request["status"], "processing")
        job_id = request["active_automation_import_job_id"]
        assert job_id is not None

        result = process_completed_album(
            reconstruct_grab_list_entry(request, state),
            ctx,
            import_job_id=job_id,
        )

        self.assertIsInstance(result, CompletionFailed)
        assert isinstance(result, CompletionFailed)
        self.assertEqual(result.reason, "event_path_never_stamped")

    def test_the_same_world_with_an_accepted_row_stamps_and_materializes(self):
        """Must-still-work: acceptance is the ONLY difference, and the same
        completion then stamps and materializes normally.

        The gate has to fail closed on an unowned key WITHOUT failing closed
        on an owned one, so this drives the identical world one confirm
        apart and follows it to the materialized bytes — the state the
        refused world never reaches.
        """
        from lib.download_processing import Completed, process_completed_album
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.processing_paths import (
            canonical_folder_for_row,
            processing_albums_dir,
        )

        ctx, request, state = self._pending_only_world(accept=True)

        stamped = state.files[0].local_path
        self.assertEqual(
            stamped,
            os.path.join(ctx.cfg.slskd_download_dir, "Music", "01.flac"))
        job_id = request["active_automation_import_job_id"]
        assert job_id is not None
        # The poll fixture writes a placeholder byte string; materialization
        # is followed by a real decode, so give the stamped path real audio.
        assert stamped is not None
        os.unlink(stamped)
        _write_test_audio(stamped)
        entry = reconstruct_grab_list_entry(request, state)
        canonical = canonical_folder_for_row(
            entry, processing_albums_dir(ctx.cfg.processing_dir))

        result = process_completed_album(entry, ctx, import_job_id=job_id)

        self.assertIsInstance(result, Completed)
        self.assertTrue(
            os.path.isfile(os.path.join(canonical, "01.flac")), canonical)

    def test_crash_after_handoff_leaves_target_absent_and_source_byte_exact(
        self,
    ):
        """The downloader publishes ownership without touching either tree."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import (
            canonical_folder_for_row,
            processing_albums_dir,
        )

        enqueued_at = _utc_now_iso()
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": enqueued_at,
            "files": [
                {
                    "username": "user1",
                    "filename": "user1\\Music\\01.flac",
                    "file_dir": "user1\\Music",
                    "size": 10,
                },
                {
                    "username": "user1",
                    "filename": "user1\\Music\\02.flac",
                    "file_dir": "user1\\Music",
                    "size": 20,
                },
            ],
        })
        ctx, db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{
                    "directory": "user1\\Music",
                    "files": [
                        {
                            "filename": "user1\\Music\\01.flac",
                            "id": "one",
                            "state": "Completed, Succeeded",
                            "bytesTransferred": 10,
                            "requestedAt": enqueued_at,
                        },
                        {
                            "filename": "user1\\Music\\02.flac",
                            "id": "two",
                            "state": "Completed, Succeeded",
                            "bytesTransferred": 20,
                            "requestedAt": enqueued_at,
                        },
                    ],
                }],
            }],
        )
        state = self._download_state(db)
        source_paths = [
            file["local_path"]
            for file in state["files"]
        ]

        def source_manifest() -> dict[str, str]:
            return {
                os.path.basename(path): hashlib.sha256(
                    Path(path).read_bytes(),
                ).hexdigest()
                for path in source_paths
            }

        before_manifest = source_manifest()
        entry = make_grab_list_entry(
            artist=row["artist_name"],
            title=row["album_title"],
            year=str(row["year"]),
            db_request_id=1,
            files=[
                make_download_file(
                    username=file["username"],
                    filename=file["filename"],
                    file_dir=file["file_dir"],
                    size=file["size"],
                )
                for file in state["files"]
            ],
        )
        target = canonical_folder_for_row(
            entry,
            processing_albums_dir(ctx.cfg.processing_dir),
        )
        self.assertFalse(os.path.exists(target))

        poll_active_downloads(ctx)

        self.assertEqual(db.request(1)["status"], "processing")
        self.assertEqual(len(db.list_import_jobs(request_id=1)), 1)
        self.assertFalse(os.path.exists(target))
        self.assertEqual(source_manifest(), before_manifest)

    def test_poll_active_all_complete_uses_async_preview_gate(self):
        """Completion publishes a queued preview owner without materializing."""
        from lib.download import poll_active_downloads

        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                    # requestedAt at/after enqueued_at -- issue #822
                    # item 2 fail-closed exclusion otherwise.
                    "requestedAt": row["active_download_state"]["enqueued_at"],
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        jobs = fake_db.list_import_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].preview_status, "waiting")
        # Completion publishes through handoff_automation_import, whose INSERT
        # omits preview_message and so takes migration 018's DEFAULT. The
        # literal rather than the fake's constant, so this still fails if the
        # fake drifts. Asserted as None until #1347, which was the enqueue
        # writer's shape on a row this path never uses.
        self.assertEqual(jobs[0].preview_message, "Preview gate disabled")
        claimed = claim_next_import_preview_job(fake_db, worker_id="preview",
        execution_lease=ExecutionLeaseSnapshot(
            host_boot_id="download-test-boot",
            invocation_id="download-test-preview",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=5200, start_ticks=52),
        ),)
        assert claimed is not None
        self.assertEqual(claimed.id, jobs[0].id)

    def test_poll_active_all_complete_no_beets(self):
        """beets_validation_enabled=False still queues importer ownership."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                    # requestedAt at/after enqueued_at -- issue #822
                    # item 2 fail-closed exclusion otherwise.
                    "requestedAt": row["active_download_state"]["enqueued_at"],
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertGreaterEqual(len(fake_db.update_download_state_calls), 1)
        request = fake_db.request(1)
        jobs = fake_db.list_import_jobs()
        self.assertEqual(request["status"], "processing")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(
            request["active_automation_import_job_id"],
            jobs[0].id,
        )

    def test_poll_active_timeout(self):
        """No byte/state progress for stalled_timeout → cancel, log, reset to wanted."""
        from lib.download import poll_active_downloads
        stale = "2020-01-01T00:00:00+00:00"
        state_dict = {
            "filetype": "flac",
            "enqueued_at": stale,
            "last_progress_at": stale,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "bytes_transferred": 12345, "last_state": "InProgress"},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "InProgress",
                    "bytesTransferred": 12345,
                }]}],
            }],
        )

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db.recorded_attempts, [(1, "download")])

    def test_poll_active_timeout_really_destroys_its_owned_payload(self):
        """#1278 composition: the timeout path drives the REAL
        ``cancel_and_delete``, not a patched stand-in.

        Every other timeout test above patches it away, so the ownership
        gate was correct at module scope, ``_timeout_album`` was correct
        at module scope, and nothing exercised the two together — the
        #853/#859 shape, where the defect lives in the composition. This
        test runs the real helper over a real payload with the queue key
        ledger-owned, which is the world a genuine timeout is in: the
        request is ``downloading`` on our own accepted attempt.
        """
        from lib.download import poll_active_downloads
        from lib.download_ownership import DownloadOwnershipWriter
        from lib.pipeline_db import TransferLedgerRow

        stale = "2020-01-01T00:00:00+00:00"
        state_dict = {
            "filetype": "flac",
            "enqueued_at": stale,
            "last_progress_at": stale,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "bytes_transferred": 12345, "last_state": "InProgress"},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": "user1\\Music\\01.flac",
                "id": "tid-1",
                "state": "InProgress",
                "bytesTransferred": 12345,
            }]}],
        }])
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row], slskd=slskd)
        payload = fake_db.request(1)[
            "active_download_state"]["files"][0]["local_path"]
        self.assertTrue(os.path.exists(payload))
        fake_db.record_transfer_enqueue([TransferLedgerRow(
            request_id=1, username="user1",
            filename="user1\\Music\\01.flac")])
        fake_db.confirm_transfer_enqueue(
            "user1", "user1\\Music\\01.flac", request_id=1)
        rebind_collaborators(
            ctx,
            download_ownership=DownloadOwnershipWriter(
            db_factory=lambda: fake_db, close_after_use=False),
        )

        poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(
            [(call.username, call.id)
             for call in slskd.transfers.cancel_download_calls],
            [("user1", "tid-1")],
        )
        self.assertFalse(os.path.exists(payload))

    def test_poll_active_old_album_with_progress_does_not_timeout(self):
        """Fresh byte progress should refresh stall timer even for an old album."""
        from lib.download import poll_active_downloads
        stale = "2020-01-01T00:00:00+00:00"
        state_dict = {
            "filetype": "flac",
            "enqueued_at": stale,
            "last_progress_at": stale,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "bytes_transferred": 12345, "last_state": "InProgress"},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "InProgress",
                    "bytesTransferred": 22345,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        persisted = self._download_state(fake_db)
        self.assertEqual(persisted["files"][0]["bytes_transferred"], 22345)
        self.assertIsNotNone(persisted["last_progress_at"])

    def test_poll_active_transfer_vanished_all(self):
        """slskd returns no matching transfers → treat as timeout."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": (
                datetime.now(UTC) - timedelta(minutes=2)
            ).isoformat(),
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": []}],
            }],
        )
        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")

    def test_poll_active_uses_persisted_terminal_failures_when_snapshot_drops_rows(self):
        """Terminal slskd failures remain actionable after removed rows disappear.

        Live elgoognplus M4A failures reached ``Completed, Rejected`` /
        ``Completed, Errored`` with zero bytes. A later poll snapshot no
        longer exposed those terminal rows, and the old code collapsed that
        persisted evidence into the misleading ``all transfers vanished``
        timeout.
        """
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "m4a",
            "enqueued_at": (
                datetime.now(UTC) - timedelta(minutes=2)
            ).isoformat(),
            "files": [
                {
                    "username": "elgoognplus",
                    "filename": (
                        "Music\\78 Saab\\Crossed Lines\\01 No Illusions.m4a"
                    ),
                    "file_dir": "Music\\78 Saab\\Crossed Lines",
                    "size": 26799968,
                    "last_state": "Completed, Rejected",
                    "bytes_transferred": 0,
                },
                {
                    "username": "elgoognplus",
                    "filename": (
                        "Music\\78 Saab\\Crossed Lines\\02 Cops.m4a"
                    ),
                    "file_dir": "Music\\78 Saab\\Crossed Lines",
                    "size": 29382804,
                    "last_state": "Completed, Errored",
                    "bytes_transferred": 0,
                },
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "elgoognplus",
                "directories": [{
                    "directory": "Music\\78 Saab\\Crossed Lines",
                    "files": [],
                }],
            }],
        )

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        log = fake_db.download_logs[0]
        # Issue #564 C5: the generic reason is now enriched with the real
        # per-file evidence instead of staying a bare "all N files
        # errored" — the exact fix for this test's own reproduction.
        self.assertEqual(
            log.error_message,
            "all 2 files errored — 1× 'Completed, Errored', "
            "1× 'Completed, Rejected'")
        self.assertNotEqual(log.error_message, "all transfers vanished from slskd")
        self.assertEqual(log.filetype, "m4a")
        self.assertEqual(log.soulseek_username, "elgoognplus")
        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db.cooldowns_applied, ["elgoognplus"])
        self.assertEqual(fake_db.denylist, [])

    def test_poll_active_fresh_preclaim_with_empty_snapshot_stays_downloading(self):
        """A same-cycle preclaim should not be reset before slskd registers transfers."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": []}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")

    def test_poll_active_completed_removed_transfer_uses_snapshot_status(self):
        """Completed transfers from includeRemoved=true should enqueue, not timeout."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": "2026-04-03T20:00:00+00:00",
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "done-id",
                        "state": "Completed, Succeeded",
                        "bytesTransferred": 30000000,
                        "endedAt": "2026-04-03T21:00:00+00:00",
                    },
                ]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        request = fake_db.request(1)
        jobs = fake_db.list_import_jobs()
        self.assertEqual(request["status"], "processing")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(
            request["active_automation_import_job_id"],
            jobs[0].id,
        )

    def test_poll_active_restored_completed_success_queues_importer(self):
        """Persisted success remains complete after slskd drops removed rows."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "m4a",
            "enqueued_at": (
                datetime.now(UTC) - timedelta(minutes=2)
            ).isoformat(),
            "files": [
                {
                    "username": "elgoognplus",
                    "filename": (
                        "Music\\78 Saab\\Crossed Lines\\01 No Illusions.m4a"
                    ),
                    "file_dir": "Music\\78 Saab\\Crossed Lines",
                    "size": 26799968,
                    "last_state": "Completed, Succeeded",
                    "bytes_transferred": 26799968,
                },
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "elgoognplus",
                "directories": [{
                    "directory": "Music\\78 Saab\\Crossed Lines",
                    "files": [],
                }],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.status_history, [(1, "processing")])
        self.assertEqual(fake_db.cooldowns_applied, [])
        self.assertEqual(fake_db.denylist, [])
        request = fake_db.request(1)
        jobs = fake_db.list_import_jobs(request_id=1)
        self.assertEqual(request["status"], "processing")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(
            request["active_automation_import_job_id"],
            jobs[0].id,
        )

    def test_poll_ignores_stale_terminal_transfer_before_claim(self):
        """A new claim must not bind to an older includeRemoved terminal transfer."""
        from lib.download import poll_active_downloads
        now = datetime.now(UTC)
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": (now - timedelta(minutes=2)).isoformat(),
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "old-done-id",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                    "endedAt": (now - timedelta(hours=1)).isoformat(),
                }]}],
            }],
        )

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db.list_import_jobs(), [])

    def test_stale_prior_attempt_never_launders_current_errored_file_to_complete(
        self,
    ):
        """I3 end-to-end regression pin (issue #820). Real production
        world: request 4190 track 09 (HumDrum, The Pictures - Pieces of
        Eight, 2026-07-22) — a months-old prior-attempt
        ``Completed, Succeeded`` slskd record (removed) for the same
        (username, filename) queue key sits alongside the current
        attempt's genuine ``Completed, Errored`` record. Exercises BOTH
        seams together: the end-of-cycle harvest (seam 1) must capture
        the genuine error, and the next poll's matcher (seam 2) must
        still observe it fresh rather than return None. The file must be
        routed to the errored-file path (retry_files → eventual timeout
        + cooldown), NEVER laundered into a false 'download complete'.
        """
        from lib.download import (
            harvest_terminal_transfer_evidence,
            poll_active_downloads,
        )

        username = "HumDrum"
        file_dir = (
            "@@fdcrt\\POWER POP - Tagged\\Pictures, The - "
            "Pieces Of Eight (2005)"
        )
        errored_filename = file_dir + "\\09 - Downhill From Here.mp3"
        ok_filename = file_dir + "\\01 - Track One.mp3"
        enqueued_at = "2026-07-22T02:01:25.759358+00:00"

        state_dict = {
            "filetype": "mp3",
            "enqueued_at": enqueued_at,
            "files": [
                {"username": username, "filename": ok_filename,
                 "file_dir": file_dir, "size": 6000000,
                 "last_state": "Completed, Succeeded",
                 "bytes_transferred": 6000000},
                {"username": username, "filename": errored_filename,
                 "file_dir": file_dir, "size": 5274623,
                 "local_path": None},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)

        dual_candidate_snapshot = [{
            "username": username,
            "directories": [{"directory": file_dir, "files": [
                {
                    "filename": ok_filename,
                    "id": "ok-id",
                    "state": "Completed, Succeeded",
                    "requestedAt": enqueued_at,
                    "bytesTransferred": 6000000,
                },
                {
                    # Prior HumDrum attempt, months old, removed — still
                    # visible via includeRemoved=True.
                    "filename": errored_filename,
                    "id": "stale-may-id",
                    "state": "Completed, Succeeded",
                    "requestedAt": "2026-05-18T23:01:32+00:00",
                    "endedAt": "2026-05-18T23:04:58+00:00",
                    "bytesTransferred": 5274623,
                },
                {
                    # Current attempt's genuine terminal error.
                    "filename": errored_filename,
                    "id": "current-errored-id",
                    "state": "Completed, Errored",
                    "requestedAt": "2026-07-22T02:01:26.725+00:00",
                    "startedAt": "2026-07-22T02:05:00.222+00:00",
                    "endedAt": "2026-07-22T02:05:00.222+00:00",
                    "bytesTransferred": 0,
                    "exception": (
                        "Download of 09 - Downhill From Here.mp3 "
                        "reported as failed by HumDrum"
                    ),
                },
            ]}],
        }]

        slskd = FakeSlskdAPI(downloads=dual_candidate_snapshot)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row], slskd_downloads=dual_candidate_snapshot,
            slskd=slskd,
        )

        # Cycle N end-of-cycle harvest — must capture the GENUINE
        # current-attempt error, never the stale May success.
        harvest_terminal_transfer_evidence(ctx)
        harvested = fake_db.request(1)["active_download_state"]["files"][1]
        self.assertEqual(harvested["last_state"], "Completed, Errored")
        self.assertEqual(harvested.get("bytes_transferred", 0), 0)

        # Cycle N+1 poll — the same slskd history is still visible; the
        # matcher must observe the genuine error fresh (not return None
        # and fall back to trusting persisted state), routing to retry —
        # never to 'complete'.
        slskd.queue_download_snapshots(dual_candidate_snapshot, [{
            "username": username,
            "directories": [{"directory": file_dir, "files": [{
                "filename": errored_filename,
                "id": "retry-id",
                "state": "Queued, Locally",
            }]}],
        }])

        with patch("time.sleep"):
            poll_active_downloads(ctx)

        # Never routed to complete: no import job was ever enqueued.
        self.assertEqual(fake_db.list_import_jobs(), [])
        self.assertEqual(fake_db.download_logs, [])
        # Routed to the errored-file path: re-enqueue attempted for the
        # errored file, not the already-succeeded one.
        self.assertEqual(len(slskd.transfers.enqueue_calls), 1)
        enqueue_call = slskd.transfers.enqueue_calls[0]
        self.assertEqual(enqueue_call.username, username)
        self.assertEqual(
            enqueue_call.files,
            [{"filename": errored_filename, "size": 5274623}],
        )
        persisted = self._download_state(fake_db)
        self.assertEqual(persisted["files"][1]["retry_count"], 1)

    def test_stale_prior_attempt_timestampless_never_launders_current_errored_file_to_complete(
        self,
    ):
        """Issue #822 item 2 end-to-end pin: a prior-attempt terminal
        ``Completed, Succeeded`` record with ZERO parseable lifecycle
        timestamps -- the shape a synthetic/reconstructed
        ``TransferSnapshot`` can carry (e.g.
        ``lib/download_reconstruction.py``'s ``_restored_terminal_status``
        or the vanished-transfer fallback in ``lib/download.py``) -- must
        never bind to this attempt, even though its bare state alone
        would outrank the genuine current-attempt Errored record on
        priority. Same two-seam shape as the dated #820-I3 pin
        (``test_stale_prior_attempt_never_launders_current_errored_file_to_complete``
        above), but the stale record's staleness is proven by ABSENCE of
        evidence rather than an old timestamp -- pre-fix,
        ``_is_terminal_transfer_before`` let a timestampless record
        survive filtering (``latest_ts == datetime.min`` special-cased to
        False), so it won on priority and got stamped/trusted as
        'complete'; the decided/persisted outcome asserted below must
        route to retry instead."""
        from lib.download import (
            harvest_terminal_transfer_evidence,
            poll_active_downloads,
        )

        username = "user-822"
        file_dir = "user-822\\Album"
        errored_filename = file_dir + "\\09.flac"
        ok_filename = file_dir + "\\01.flac"
        enqueued_at = "2026-07-22T02:01:25.759358+00:00"

        state_dict = {
            "filetype": "flac",
            "enqueued_at": enqueued_at,
            "files": [
                {"username": username, "filename": ok_filename,
                 "file_dir": file_dir, "size": 6000000,
                 "last_state": "Completed, Succeeded",
                 "bytes_transferred": 6000000},
                {"username": username, "filename": errored_filename,
                 "file_dir": file_dir, "size": 5274623,
                 "local_path": None},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)

        dual_candidate_snapshot = [{
            "username": username,
            "directories": [{"directory": file_dir, "files": [
                {
                    "filename": ok_filename,
                    "id": "ok-id",
                    "state": "Completed, Succeeded",
                    "requestedAt": enqueued_at,
                    "bytesTransferred": 6000000,
                },
                {
                    # Timestampless prior attempt -- no requestedAt/
                    # startedAt/endedAt at all (unlike #820's dated-stale
                    # shape), so it cannot prove which attempt it belongs
                    # to.
                    "filename": errored_filename,
                    "id": "timestampless-stale-id",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 5274623,
                },
                {
                    # Current attempt's genuine terminal error.
                    "filename": errored_filename,
                    "id": "current-errored-id",
                    "state": "Completed, Errored",
                    "requestedAt": "2026-07-22T02:01:26.725+00:00",
                    "startedAt": "2026-07-22T02:05:00.222+00:00",
                    "endedAt": "2026-07-22T02:05:00.222+00:00",
                    "bytesTransferred": 0,
                    "exception": "Read error: Connection reset by peer",
                },
            ]}],
        }]

        slskd = FakeSlskdAPI(downloads=dual_candidate_snapshot)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row], slskd_downloads=dual_candidate_snapshot,
            slskd=slskd,
        )

        # Cycle N end-of-cycle harvest -- must capture the GENUINE
        # current-attempt error, never the timestampless stale success.
        harvest_terminal_transfer_evidence(ctx)
        harvested = fake_db.request(1)["active_download_state"]["files"][1]
        self.assertEqual(harvested["last_state"], "Completed, Errored")
        self.assertEqual(harvested.get("bytes_transferred", 0), 0)

        # Cycle N+1 poll -- the same slskd history is still visible; the
        # matcher must observe the genuine error fresh, routing to
        # retry -- never laundering into 'complete'.
        slskd.queue_download_snapshots(dual_candidate_snapshot, [{
            "username": username,
            "directories": [{"directory": file_dir, "files": [{
                "filename": errored_filename,
                "id": "retry-id",
                "state": "Queued, Locally",
            }]}],
        }])

        with patch("time.sleep"):
            poll_active_downloads(ctx)

        # Never routed to complete: no import job was ever enqueued.
        self.assertEqual(fake_db.list_import_jobs(), [])
        self.assertEqual(fake_db.download_logs, [])
        # Routed to the errored-file path: re-enqueue attempted for the
        # errored file, not the already-succeeded one.
        self.assertEqual(len(slskd.transfers.enqueue_calls), 1)
        enqueue_call = slskd.transfers.enqueue_calls[0]
        self.assertEqual(enqueue_call.username, username)
        self.assertEqual(
            enqueue_call.files,
            [{"filename": errored_filename, "size": 5274623}],
        )
        persisted = self._download_state(fake_db)
        self.assertEqual(persisted["files"][1]["retry_count"], 1)

    def test_poll_active_in_progress(self):
        """Files still downloading with fresh state transition → persist progress snapshot."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "InProgress",
                    "bytesTransferred": 2048,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        # Should NOT process or timeout
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")

    def test_poll_active_multiple_albums(self):
        """2 albums: 1 completes, 1 in progress → correct handling."""
        from lib.download import poll_active_downloads
        row1 = self._make_downloading_row(request_id=1)
        state2 = {
            "filetype": "mp3 v0",
            "enqueued_at": _utc_now_iso(),
            "files": [
                {"username": "user2", "filename": "user2\\Music\\01.mp3",
                 "file_dir": "user2\\Music", "size": 5000000},
            ],
        }
        row2 = self._make_downloading_row(request_id=2, state_dict=state2)
        row2["album_title"] = "Album 2"
        row2["artist_name"] = "Artist 2"

        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row1, row2],
            slskd_downloads=[
                {
                    "username": "user1",
                    "directories": [{"directory": "user1\\Music", "files": [{
                        "filename": "user1\\Music\\01.flac",
                        "id": "tid-1",
                        "state": "Completed, Succeeded",
                        "bytesTransferred": 30000000,
                        # requestedAt at/after enqueued_at -- issue #822
                        # item 2 fail-closed exclusion otherwise.
                        "requestedAt": row1["active_download_state"]["enqueued_at"],
                    }]}],
                },
                {
                    "username": "user2",
                    "directories": [{"directory": "user2\\Music", "files": [{
                        "filename": "user2\\Music\\01.mp3",
                        "id": "tid-2",
                        "state": "InProgress",
                        "bytesTransferred": 2048,
                    }]}],
                },
            ],
        )

        # slskd returns transfers for both users
        self.assertEqual(cast(FakeSlskdAPI, ctx.slskd).transfers.get_all_downloads_calls, [])

        poll_active_downloads(ctx)

        # Each album persists exactly one complete reduced state.
        self.assertEqual(len(fake_db.update_download_state_calls), 2)
        update_request_ids = [
            request_id for request_id, _ in fake_db.update_download_state_calls
        ]
        self.assertEqual(update_request_ids, [1, 2])
        self.assertIsNotNone(fake_db.request(1)["active_download_state"])
        self.assertIsNotNone(self._download_state(fake_db, 2)["last_progress_at"])
        self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_crash_recovery_no_state(self):
        """Missing attempt witness is excluded without an inferred reset."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        row["active_download_state"] = None  # Simulates crash
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertEqual(fake_db.status_history, [])
        self.assertEqual(fake_db.update_download_state_calls, [])
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.list_import_jobs(request_id=1), [])

    def test_poll_active_all_errors(self):
        """All files errored → timeout the album.

        ``poll_active_downloads`` enqueues an import job rather than
        calling ``process_completed_album`` directly, so the absence of a
        new ``import_jobs`` row is the observable contract for "no
        downstream processing started".
        """
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "tid-1",
                        "state": "Completed, Errored",
                        # requestedAt at/after enqueued_at -- issue #822
                        # item 2 fail-closed exclusion otherwise.
                        "requestedAt": row["active_download_state"]["enqueued_at"],
                    },
                ]}],
            }],
        )
        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        self.assertEqual(fake_db._import_jobs, [])
        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")

    def test_poll_active_remote_queue_timeout(self):
        """All files queued remotely past timeout → timeout."""
        # enqueued long enough ago to exceed remote_queue_timeout but not stalled_timeout
        from datetime import datetime, timedelta

        from lib.download import poll_active_downloads
        past = (datetime.now(UTC) - timedelta(seconds=200)).isoformat()
        state_dict = {
            "filetype": "flac",
            "enqueued_at": past,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Queued, Remotely",
                }]}],
            }],
        )
        cfg = cast(Any, ctx.cfg)
        cfg.remote_queue_timeout = 120  # 2 minutes
        cfg.stalled_timeout = 600  # 10 minutes (not exceeded)

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")

    def test_poll_active_remote_queue_does_not_use_stalled_timeout(self):
        """Fully remote-queued albums should not hit stalled_timeout first."""
        from lib.download import poll_active_downloads
        now = datetime.now(UTC)
        enqueued_at = (now - timedelta(seconds=200)).isoformat()
        stale_progress = (now - timedelta(seconds=1200)).isoformat()
        state_dict = {
            "filetype": "flac",
            "enqueued_at": enqueued_at,
            "last_progress_at": stale_progress,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Queued, Remotely",
                }]}],
            }],
        )
        cfg = cast(Any, ctx.cfg)
        cfg.remote_queue_timeout = 3600
        cfg.stalled_timeout = 120

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        # First observation of "Queued, Remotely" is new evidence (issue
        # #564 evidence rule) even though it isn't forward progress — the
        # complete reduced state persists, but stalled_timeout still doesn't fire.
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        self.assertIn(
            "Queued, Remotely",
            fake_db.update_download_state_calls[0][1])
        self.assertEqual(fake_db.request(1)["status"], "downloading")

    def test_poll_transfer_vanished_partial(self):
        """7/12 files vanish → treated as errors, not complete.

        ``poll_active_downloads`` only kicks off downstream processing
        via the import-job queue, so the assertions below check the
        observable contract (no new import_jobs row, no download_log).
        """
        from lib.download import poll_active_downloads
        # 12 files, only 5 have transfers in slskd
        files = []
        for i in range(12):
            files.append({"username": "user1",
                          "filename": f"user1\\Music\\{i:02d}.flac",
                          "file_dir": "user1\\Music", "size": 30000000})
        state_dict = {
            "filetype": "flac",
            "enqueued_at": _utc_now_iso(),
            "files": files,
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])

        # Only files 0-4 have transfers in slskd
        slskd_files = [
            {
                "filename": f"user1\\Music\\{i:02d}.flac",
                "id": f"tid-{i}",
                "state": "InProgress",
            }
            for i in range(5)
        ]
        slskd = cast(FakeSlskdAPI, ctx.slskd)
        slskd.set_downloads([{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": slskd_files}],
        }])
        slskd.transfers.enqueue_result = False

        with self.assertLogs("cratedigger", level="WARNING") as logs:
            poll_active_downloads(ctx)

        # Should NOT process — 7 files vanished (errored), album not complete
        self.assertEqual(fake_db._import_jobs, [])
        self.assertEqual(
            "\n".join(logs.output).count("Failed to re-enqueue file"),
            7,
        )
        self.assertEqual(len(slskd.transfers.enqueue_calls), 7)
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")


    def test_poll_active_partial_errors_with_retry(self):
        """Some files errored, retries available → re-enqueue those files."""
        from lib.download import poll_active_downloads
        # 3 files: 2 complete, 1 errored
        state_dict = {
            "filetype": "flac",
            "enqueued_at": _utc_now_iso(),
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
                {"username": "user1", "filename": "user1\\Music\\02.flac",
                 "file_dir": "user1\\Music", "size": 25000000},
                {"username": "user1", "filename": "user1\\Music\\03.flac",
                 "file_dir": "user1\\Music", "size": 20000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[],
        )

        # First snapshot is the poll cycle; second snapshot is the
        # post-requeue transfer-id lookup.
        # requestedAt at/after enqueued_at on every terminal entry --
        # issue #822 item 2 fail-closed exclusion otherwise.
        enqueued_at = row["active_download_state"]["enqueued_at"]
        poll_snapshot = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [
                {
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "requestedAt": enqueued_at,
                },
                {
                    "filename": "user1\\Music\\02.flac",
                    "id": "tid-2",
                    "state": "Completed, Succeeded",
                    "requestedAt": enqueued_at,
                },
                {
                    "filename": "user1\\Music\\03.flac",
                    "id": "tid-3",
                    "state": "Completed, Errored",
                    "requestedAt": enqueued_at,
                },
            ]}],
        }]
        requeue_snapshot = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": "user1\\Music\\03.flac",
                "id": "new-tid-3",
                "state": "Queued, Locally",
            }]}],
        }]
        slskd = cast(FakeSlskdAPI, ctx.slskd)
        slskd.queue_download_snapshots(poll_snapshot, requeue_snapshot)

        with patch("time.sleep"):
            poll_active_downloads(ctx)

        # Should NOT process (not all done) and NOT timeout
        self.assertEqual(fake_db._import_jobs, [])
        self.assertEqual(fake_db.download_logs, [])
        # Should re-enqueue the errored file
        self.assertEqual(len(slskd.transfers.enqueue_calls), 1)
        enqueue_call = slskd.transfers.enqueue_calls[0]
        self.assertEqual(enqueue_call.username, "user1")
        self.assertEqual(
            enqueue_call.files,
            [{"filename": "user1\\Music\\03.flac", "size": 20000000}],
        )
        persisted = self._download_state(fake_db)
        self.assertEqual(persisted["files"][2]["retry_count"], 1)

    def test_retry_lock_contention_preserves_budget_and_has_no_slskd_effect(
        self,
    ):
        """IMPORT contention cannot count a retry that was never attempted."""
        from lib.download import poll_active_downloads

        enqueued_at = _utc_now_iso()
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": enqueued_at,
            "files": [
                {
                    "username": "user1",
                    "filename": "user1\\Music\\01.flac",
                    "file_dir": "user1\\Music",
                    "size": 100,
                    "retry_count": 2,
                },
                {
                    "username": "user1",
                    "filename": "user1\\Music\\02.flac",
                    "file_dir": "user1\\Music",
                    "size": 100,
                },
            ],
        })
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "failed",
                        "state": "Completed, Errored",
                        "requestedAt": enqueued_at,
                    },
                    {
                        "filename": "user1\\Music\\02.flac",
                        "id": "progress",
                        "state": "InProgress",
                        "bytesTransferred": 1,
                        "requestedAt": enqueued_at,
                    },
                ],
            }],
        }])
        ctx, db = self._make_poll_ctx(
            downloading_rows=[row],
            fake_db=FakePipelineDB(),
            slskd=slskd,
        )
        db.set_advisory_lock_result(False)
        before = copy.deepcopy(db.get_request(1))

        poll_active_downloads(ctx)

        self.assertEqual(db.get_request(1), before)
        self.assertEqual(db.update_download_state_calls, [])
        self.assertEqual(slskd.transfers.enqueue_calls, [])
        self.assertEqual(slskd.transfers.cancel_download_calls, [])
        self.assertEqual(db.list_import_jobs(request_id=1), [])

    def test_poll_active_get_all_downloads_api_error_waits_for_next_cycle(self):
        """Transient bulk-download API failures must not be treated as vanished transfers."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])
        cast(FakeSlskdAPI, ctx.slskd).transfers.get_all_downloads_error = (
            RuntimeError("temporary slskd failure")
        )

        with patch("lib.download.cancel_and_delete") as mock_cancel:
            poll_active_downloads(ctx)

        mock_cancel.assert_not_called()
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.update_download_state_calls, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertEqual(fake_db.status_history, [])

    def test_poll_active_completion_publishes_exact_processing_owner(self):
        """Completion atomically publishes the job, owner, path, and state."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import attempt_fingerprint
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                    # requestedAt at/after enqueued_at -- issue #822
                    # item 2 fail-closed exclusion otherwise.
                    "requestedAt": row["active_download_state"]["enqueued_at"],
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.status_history, [(1, "processing")])
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        request = fake_db.request(1)
        self.assertEqual(request["status"], "processing")
        persisted = self._download_state(fake_db)
        self.assertIsNotNone(persisted["processing_started_at"])
        self.assertIsNotNone(persisted["current_path"])
        fp = attempt_fingerprint([("user1", "user1\\Music\\01.flac")])
        self.assertTrue(
            persisted["current_path"].endswith(
                f"Test Artist - Test Album (2020) [{fp}]")
        )
        jobs = fake_db.list_import_jobs(request_id=1)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(
            request["active_automation_import_job_id"],
            jobs[0].id,
        )




















    def test_poll_no_redownload_window(self):
        """Completion cannot reopen wanted between downloader and processor."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                    # requestedAt at/after enqueued_at -- issue #822
                    # item 2 fail-closed exclusion otherwise.
                    "requestedAt": row["active_download_state"]["enqueued_at"],
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertGreaterEqual(len(fake_db.update_download_state_calls), 1)
        self.assertNotIn((1, "wanted"), fake_db.status_history)
        request = fake_db.request(1)
        jobs = fake_db.list_import_jobs(request_id=1)
        self.assertEqual(request["status"], "processing")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(
            request["active_automation_import_job_id"],
            jobs[0].id,
        )

    def test_poll_overlong_album_title_truncates_and_processes(self):
        """Overlong artist/title now truncates to ext4's 255-byte limit.

        History: a Sade row with 240+ char artist + title produced a
        canonical path over the 255-byte component limit; os.makedirs
        raised OSError(36) and (pre-guard) starved later rows. Since the
        #550-phase-2 fingerprint suffix, canonical_processing_path
        byte-truncates the base name, so the row now materializes and
        imports instead of failing. Loop containment for genuinely
        unexpected per-row exceptions stays pinned by
        test_poll_continues_after_per_row_unexpected_exception.
        """
        from lib.download import poll_active_downloads
        long_name = "X" * 250
        poison = self._make_downloading_row(
            request_id=1,
            state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000,
                     "last_state": "Completed, Succeeded"},
                ],
            },
        )
        poison["artist_name"] = long_name
        poison["album_title"] = long_name

        healthy = self._make_downloading_row(
            request_id=2,
            state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\02.flac",
                     "file_dir": "user1\\Music", "size": 30000000,
                     "last_state": "Completed, Succeeded"},
                ],
            },
        )

        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[poison, healthy], slskd_downloads=[],
        )

        poll_active_downloads(ctx)

        self.assertEqual(
            len(fake_db.list_import_jobs(request_id=2)), 1,
            "Healthy row never got an import job",
        )
        # The overlong row now truncates and processes like any other.
        self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)
        state = fake_db.request(1)["active_download_state"]
        folder = state["current_path"].rsplit("/", 1)[-1]
        self.assertLessEqual(len(folder.encode("utf-8")), 255)

    def test_poll_continues_after_per_row_unexpected_exception(self):
        """Belt-and-braces: any unhandled per-row exception must not abort the loop.

        Even if a future change reintroduces an uncaught exception in the
        materialize / enqueue path, the per-row guard in
        ``poll_active_downloads`` must contain it so other rows still
        process.
        """
        from lib import download as download_module
        from lib.download import poll_active_downloads

        poison = self._make_downloading_row(
            request_id=1,
            state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000,
                     "last_state": "Completed, Succeeded"},
                ],
            },
        )
        healthy = self._make_downloading_row(
            request_id=2,
            state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\02.flac",
                     "file_dir": "user1\\Music", "size": 30000000,
                     "last_state": "Completed, Succeeded"},
                ],
            },
        )

        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[poison, healthy], slskd_downloads=[],
        )

        real_enqueue = download_module._enqueue_completed_processing

        def selectively_explode(entry, request_id, state, db, ctx):
            if request_id == 1:
                raise RuntimeError("synthetic kaboom for test")
            return real_enqueue(entry, request_id, state, db, ctx)

        with patch.object(
            download_module,
            "_enqueue_completed_processing",
            side_effect=selectively_explode,
        ):
            poll_active_downloads(ctx)

        self.assertEqual(
            len(fake_db.list_import_jobs(request_id=2)), 1,
            "Per-row exception aborted the loop; healthy row never got "
            "an import job",
        )


class TestBuildActiveDownloadState(unittest.TestCase):
    """Test build_active_download_state() — GrabListEntry → ActiveDownloadState."""

    def test_basic(self):
        from lib.download import build_active_download_state
        from lib.grab_list import DownloadFile, GrabListEntry
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.filetype, "flac")
        self.assertIsNotNone(state.enqueued_at)
        self.assertEqual(len(state.files), 1)
        self.assertEqual(state.files[0].username, "user1")
        self.assertEqual(state.files[0].filename, "u\\M\\01.flac")
        self.assertEqual(state.files[0].size, 30000000)
        self.assertEqual(state.files[0].retry_count, 0)

    def test_multi_disc(self):
        from lib.download import build_active_download_state
        from lib.grab_list import DownloadFile, GrabListEntry
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(filename="u\\M\\D1-01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000,
                             disk_no=1, disk_count=2),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.files[0].disk_no, 1)
        self.assertEqual(state.files[0].disk_count, 2)

    def test_persists_retry_count(self):
        from lib.download import build_active_download_state
        from lib.grab_list import DownloadFile, GrabListEntry
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000,
                             retry=4),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.files[0].retry_count, 4)

    def test_persists_progress_fields(self):
        from lib.download import build_active_download_state
        from lib.grab_list import DownloadFile, GrabListEntry
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(
                    filename="u\\M\\01.flac", id="tid-1",
                    file_dir="u\\M", username="user1", size=30000000,
                    bytes_transferred=2048, last_state="InProgress",
                ),
            ],
        )
        state = build_active_download_state(
            entry,
            enqueued_at="2026-04-03T12:00:00+00:00",
            last_progress_at="2026-04-03T12:05:00+00:00",
        )
        self.assertEqual(state.last_progress_at, "2026-04-03T12:05:00+00:00")
        self.assertEqual(state.files[0].bytes_transferred, 2048)
        self.assertEqual(state.files[0].last_state, "InProgress")

    def test_persists_last_exception(self):
        """Issue #564: the real slskd failure reason survives the
        GrabListEntry -> ActiveDownloadState round trip."""
        from lib.download import build_active_download_state
        from lib.grab_list import DownloadFile, GrabListEntry
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(
                    filename="u\\M\\01.flac", id="tid-1",
                    file_dir="u\\M", username="user1", size=30000000,
                    last_state="Completed, Rejected",
                    last_exception="Transfer rejected: Banned",
                ),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(
            state.files[0].last_exception, "Transfer rejected: Banned")

    def test_enqueued_at_is_utc_iso(self):
        from datetime import datetime as dt

        from lib.download import build_active_download_state
        from lib.grab_list import DownloadFile, GrabListEntry
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid", files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=1000),
            ],
        )
        state = build_active_download_state(entry)
        parsed = dt.fromisoformat(state.enqueued_at)
        self.assertEqual(parsed.tzinfo, UTC)
        self.assertEqual(state.last_progress_at, state.enqueued_at)

    def test_initial_state_has_no_processor_owned_path(self):
        from lib.download import build_active_download_state
        from lib.grab_list import DownloadFile, GrabListEntry
        entry = GrabListEntry(
            album_id=1,
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            import_folder="/tmp/staged/A/T",
            files=[
                DownloadFile(
                    filename="u\\M\\01.flac", id="tid-1",
                    file_dir="u\\M", username="user1", size=1000,
                ),
            ],
        )
        state = build_active_download_state(entry)
        self.assertIsNone(state.current_path)

    def test_computes_attempt_fingerprint_agreeing_with_the_ledger(self):
        """#1196 item 1: the state's ``attempt_fingerprint`` is computed
        with the SAME ``lib.processing_paths.attempt_fingerprint``
        function over the SAME (username, filename) pairs the transfer
        ledger's own fingerprint is computed from (``lib.enqueue.
        _enqueue_with_claim_outcome``'s ``attempt_fp``) -- agreement by
        construction, proven here by calling the shared function
        directly rather than a second formula."""
        from lib.download import build_active_download_state
        from lib.grab_list import DownloadFile, GrabListEntry
        from lib.processing_paths import attempt_fingerprint
        files = [
            DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                         file_dir="u\\M", username="user1", size=1000),
            DownloadFile(filename="u\\M\\02.flac", id="tid-2",
                         file_dir="u\\M", username="user1", size=2000),
        ]
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid", files=files,
        )
        state = build_active_download_state(entry)
        self.assertEqual(
            state.attempt_fingerprint,
            attempt_fingerprint([(f.username, f.filename) for f in files]),
        )

    def test_attempt_fingerprint_is_none_for_empty_files(self):
        """Mirrors the ledger's own ``attempt_fp = ... if
        claim.entry.files else None`` guard (lib/enqueue.py) -- an empty
        planned set has no identity to fingerprint."""
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid", files=[],
        )
        state = build_active_download_state(entry)
        self.assertIsNone(state.attempt_fingerprint)


class TestReconstructGrabListEntry(unittest.TestCase):
    """Test reconstruct_grab_list_entry() — rebuild GrabListEntry from DB row + state."""

    def test_reconstruct_basic(self):
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadFileState, ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music", size=30000000,
                ),
            ],
        )
        request = {
            "id": 42,
            "album_title": "Test Album",
            "artist_name": "Test Artist",
            "year": 2020,
            "mb_release_id": "test-mbid",
            "source": "request",
            "search_filetype_override": None,
            "target_format": None,
        }
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.album_id, 42)
        self.assertEqual(entry.title, "Test Album")
        self.assertEqual(entry.artist, "Test Artist")
        self.assertEqual(entry.year, "2020")
        self.assertEqual(entry.filetype, "flac")
        self.assertEqual(entry.mb_release_id, "test-mbid")
        self.assertEqual(entry.db_request_id, 42)
        self.assertEqual(entry.db_source, "request")
        self.assertEqual(len(entry.files), 1)
        self.assertEqual(entry.files[0].filename, "user1\\Music\\01.flac")
        self.assertEqual(entry.files[0].id, "")  # Must be re-derived
        self.assertEqual(entry.files[0].retry, 0)

    def test_reconstruct_applies_live_transfer_ids_by_attempt_identity(self):
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadFileState, ActiveDownloadState

        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1",
                    filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music",
                    size=30000000,
                ),
                ActiveDownloadFileState(
                    username="user2",
                    filename="user2\\Music\\02.flac",
                    file_dir="user2\\Music",
                    size=25000000,
                ),
            ],
        )
        request = {
            "id": 42,
            "album_title": "Test Album",
            "artist_name": "Test Artist",
            "year": 2020,
            "mb_release_id": "test-mbid",
            "source": "request",
            "search_filetype_override": None,
            "target_format": None,
        }

        entry = reconstruct_grab_list_entry(
            request,
            state,
            transfer_ids={("user1", "user1\\Music\\01.flac"): "transfer-42"},
        )

        self.assertEqual(entry.files[0].id, "transfer-42")
        self.assertEqual(entry.files[1].id, "")

    def test_reconstruct_multi_disc(self):
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadFileState, ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\D1-01.flac",
                    file_dir="user1\\Music", size=30000000,
                    disk_no=1, disk_count=2,
                ),
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\D2-01.flac",
                    file_dir="user1\\Music", size=25000000,
                    disk_no=2, disk_count=2,
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].disk_no, 1)
        self.assertEqual(entry.files[0].disk_count, 2)
        self.assertEqual(entry.files[1].disk_no, 2)

    def test_reconstruct_search_filetype_override(self):
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(filetype="flac", enqueued_at="now", files=[])
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": "flac", "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.db_search_filetype_override, "flac")

    def test_reconstruct_retry_count(self):
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadFileState, ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music", size=30000000, retry_count=5,
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].retry, 5)

    def test_reconstruct_progress_fields(self):
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadFileState, ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            last_progress_at="2026-04-03T12:05:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1",
                    filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music",
                    size=30000000,
                    bytes_transferred=4096,
                    last_state="InProgress",
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].bytes_transferred, 4096)
        self.assertEqual(entry.files[0].last_state, "InProgress")
        self.assertIsNone(entry.files[0].status)

    def test_reconstruct_restores_terminal_status_from_persisted_state(self):
        """Once slskd reports a terminal state, later snapshot gaps must not
        erase that evidence."""
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadFileState, ActiveDownloadState
        state = ActiveDownloadState(
            filetype="m4a",
            enqueued_at="now",
            files=[
                ActiveDownloadFileState(
                    username="elgoognplus",
                    filename="Music\\78 Saab\\Crossed Lines\\01 No Illusions.m4a",
                    file_dir="Music\\78 Saab\\Crossed Lines",
                    size=26799968,
                    bytes_transferred=0,
                    last_state="Completed, Rejected",
                ),
            ],
        )
        request = {"id": 10, "album_title": "Crossed Lines",
                   "artist_name": "78 Saab", "year": 2004,
                   "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}

        entry = reconstruct_grab_list_entry(request, state)

        self.assertEqual(
            entry.files[0].status,
            TransferSnapshot(state="Completed, Rejected", bytes_transferred=0),
        )

    def test_reconstruct_restores_exception_onto_terminal_status(self):
        """Issue #564: a persisted exception rehydrates onto the
        synthetic TransferSnapshot AND the DownloadFile field directly."""
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadFileState, ActiveDownloadState
        state = ActiveDownloadState(
            filetype="m4a",
            enqueued_at="now",
            files=[
                ActiveDownloadFileState(
                    username="elgoognplus",
                    filename="Music\\78 Saab\\Crossed Lines\\01 No Illusions.m4a",
                    file_dir="Music\\78 Saab\\Crossed Lines",
                    size=26799968,
                    bytes_transferred=0,
                    last_state="Completed, Rejected",
                    last_exception="Transfer rejected: Banned",
                ),
            ],
        )
        request = {"id": 10, "album_title": "Crossed Lines",
                   "artist_name": "78 Saab", "year": 2004,
                   "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}

        entry = reconstruct_grab_list_entry(request, state)

        self.assertEqual(
            entry.files[0].status,
            TransferSnapshot(state="Completed, Rejected", bytes_transferred=0,
                              exception="Transfer rejected: Banned"),
        )
        self.assertEqual(
            entry.files[0].last_exception, "Transfer rejected: Banned")

    def test_reconstruct_persists_last_exception_independent_of_terminal_status(self):
        """A non-terminal state carries no synthetic status, but the
        persisted exception field itself must still survive the round
        trip — evidence the pre-purge harvest (issue #564 C3) relies on."""
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadFileState, ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music", size=1000,
                    last_state="InProgress",
                    last_exception="Read error: Connection reset by peer",
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}

        entry = reconstruct_grab_list_entry(request, state)

        self.assertIsNone(entry.files[0].status)
        self.assertEqual(
            entry.files[0].last_exception,
            "Read error: Connection reset by peer")

    def test_reconstruct_missing_year(self):
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(filetype="flac", enqueued_at="now", files=[])
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": None, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.year, "")

    def test_reconstruct_current_path_to_import_folder(self):
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            current_path="/tmp/staged/A/B",
            files=[],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.import_folder, "/tmp/staged/A/B")

    def test_reconstruct_fallback_current_path(self):
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            current_path="/tmp/legacy/A/B",
            files=[],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.import_folder, "/tmp/legacy/A/B")


# ============================================================================
# _compute_rejection_backfill — orchestration test for cfg threading
# ============================================================================
#
# Pins that ctx.cfg.quality_ranks actually reaches rejection_backfill_override.
# Pure-function tests in test_quality_decisions.py cover the decision logic
# itself; this test guards the wiring layer between download.py and
# lib/quality/ so a future refactor can't silently drop the cfg argument.

class TestComputeRejectionBackfillCfgThreading(unittest.TestCase):
    """Validation rejects use linked current evidence and live rank config."""

    def _setup(
        self,
        *,
        quality_ranks,
        on_disk_min_bitrate=180,
        linked_grade: str | None = "genuine",
        link_evidence=True,
        evidence_mbid="mbid-test",
        existing_override: str | None = None,
    ):
        from lib.quality import AudioQualityMeasurement

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-test",
            # Deliberately stale: linked evidence is authoritative.
            current_spectral_grade="genuine",
            verified_lossless=False,
            search_filetype_override=existing_override,
        ))

        from types import SimpleNamespace
        cfg = SimpleNamespace(quality_ranks=quality_ranks)
        ctx = make_ctx_with_fake_db(fake_db, cfg=cfg)

        album_data = make_grab_list_entry(
            db_request_id=42,
            db_search_filetype_override=existing_override,
            mb_release_id="mbid-test",
        )
        if link_evidence:
            evidence = make_album_quality_evidence(
                mb_release_id=evidence_mbid,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=on_disk_min_bitrate,
                    avg_bitrate_kbps=on_disk_min_bitrate,
                    median_bitrate_kbps=on_disk_min_bitrate,
                    format="MP3",
                    is_cbr=False,
                    spectral_grade=linked_grade,
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            fake_db.upsert_album_quality_evidence(evidence)
            persisted = fake_db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            fake_db.set_request_current_evidence(42, persisted.id)
        return album_data, ctx

    def _run(self, album_data, ctx):
        from lib.download_rejection import _compute_rejection_backfill
        return _compute_rejection_backfill(album_data, ctx)

    def test_custom_transparent_band_fires_backfill(self):
        """The caller's custom transparent codec band reaches the policy."""
        from lib.quality import QUALITY_LOSSLESS, CodecRankBands, QualityRankConfig

        custom = QualityRankConfig(
            mp3=CodecRankBands(
                transparent=180,
                excellent=170,
                good=130,
                acceptable=96,
            ),
        )
        album_data, ctx = self._setup(quality_ranks=custom)
        result = self._run(album_data, ctx)
        self.assertEqual(result, QUALITY_LOSSLESS)

    def test_default_transparent_band_blocks_excellent_have(self):
        from lib.quality import QualityRankConfig

        album_data, ctx = self._setup(
            quality_ranks=QualityRankConfig.defaults(),
        )
        result = self._run(album_data, ctx)
        self.assertIsNone(result)

    def test_default_transparent_mp3_have_fires_backfill(self):
        """The validation-reject caller recognizes a normal CBR 320 HAVE."""
        from lib.quality import QUALITY_LOSSLESS, QualityRankConfig

        album_data, ctx = self._setup(
            quality_ranks=QualityRankConfig.defaults(),
            on_disk_min_bitrate=320,
        )
        self.assertEqual(self._run(album_data, ctx), QUALITY_LOSSLESS)

    def test_full_upgrade_ladder_narrows_from_linked_transparent_have(self):
        from lib.quality import (
            QUALITY_UPGRADE_TIERS,
            QualityRankConfig,
        )

        album_data, ctx = self._setup(
            quality_ranks=QualityRankConfig.defaults(),
            on_disk_min_bitrate=320,
            existing_override=QUALITY_UPGRADE_TIERS,
        )

        self.assertEqual(self._run(album_data, ctx), "lossless")

    def test_full_upgrade_ladder_is_preserved_when_linked_rule_fails(self):
        from lib.quality import (
            QUALITY_UPGRADE_TIERS,
            QualityRankConfig,
        )

        album_data, ctx = self._setup(
            quality_ranks=QualityRankConfig.defaults(),
            on_disk_min_bitrate=320,
            linked_grade="suspect",
            existing_override=QUALITY_UPGRADE_TIERS,
        )

        self.assertIsNone(self._run(album_data, ctx))

    def test_already_lossless_override_is_a_noop(self):
        from lib.quality import QualityRankConfig

        album_data, ctx = self._setup(
            quality_ranks=QualityRankConfig.defaults(),
            on_disk_min_bitrate=320,
            existing_override="lossless",
        )

        self.assertIsNone(self._run(album_data, ctx))

    def test_missing_linked_evidence_ignores_stale_request_scalar(self):
        from lib.quality import QualityRankConfig

        album_data, ctx = self._setup(
            quality_ranks=QualityRankConfig.defaults(),
            on_disk_min_bitrate=320,
            link_evidence=False,
        )
        self.assertIsNone(self._run(album_data, ctx))

    def test_linked_evidence_without_genuine_grade_fails_open(self):
        from lib.quality import QualityRankConfig

        album_data, ctx = self._setup(
            quality_ranks=QualityRankConfig.defaults(),
            on_disk_min_bitrate=320,
            linked_grade=None,
        )
        self.assertIsNone(self._run(album_data, ctx))

    def test_linked_evidence_for_another_pressing_fails_open(self):
        from lib.quality import QualityRankConfig

        album_data, ctx = self._setup(
            quality_ranks=QualityRankConfig.defaults(),
            on_disk_min_bitrate=320,
            evidence_mbid="other-mbid",
        )
        self.assertIsNone(self._run(album_data, ctx))


if TYPE_CHECKING:
    from lib.download import DownloadDB as _DownloadDB
    from lib.download_ownership import DownloadOwnershipDB as _OwnershipDB
    from lib.pipeline_db import PipelineDB

    # Static parity proof (#409) — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    _pipeline_db_satisfies_download_protocol: _DownloadDB = cast("PipelineDB", None)
    _fake_db_satisfies_download_protocol: _DownloadDB = cast("FakePipelineDB", None)
    _pipeline_db_satisfies_ownership_protocol: _OwnershipDB = cast("PipelineDB", None)
    _fake_db_satisfies_ownership_protocol: _OwnershipDB = cast("FakePipelineDB", None)


class TestDownloadDBProtocolParity(unittest.TestCase):
    """#409: both impls must satisfy DownloadDB and DownloadOwnershipDB."""

    def test_pipeline_db_satisfies_protocols(self) -> None:
        from lib.download import DownloadDB
        from lib.download_ownership import DownloadOwnershipDB
        from lib.pipeline_db import PipelineDB

        self.assertTrue(issubclass(PipelineDB, DownloadDB))
        self.assertTrue(issubclass(PipelineDB, DownloadOwnershipDB))

    def test_fake_pipeline_db_satisfies_protocols(self) -> None:
        from lib.download import DownloadDB
        from lib.download_ownership import DownloadOwnershipDB

        self.assertTrue(issubclass(FakePipelineDB, DownloadDB))
        self.assertTrue(issubclass(FakePipelineDB, DownloadOwnershipDB))

    def test_protocols_extend_transitions_protocol(self) -> None:
        """Both forward their handle into transitions.finalize_request."""
        from lib.download import DownloadDB
        from lib.download_ownership import DownloadOwnershipDB
        from lib.transitions import TransitionsDB

        self.assertTrue(issubclass(DownloadDB, TransitionsDB))
        self.assertTrue(issubclass(DownloadOwnershipDB, TransitionsDB))


class TestConvergeSlskdOrphans(unittest.TestCase):
    """#278 Phase 0 convergence, ledger-positive since #571 PR 3: cancel
    LEDGERED live transfers no downloading row backs. A live transfer
    absent from cratedigger's write-ahead ledger is foreign — never
    cancelled, whatever its state or age (C1)."""

    OWNED_FILE = "Music\\Owned\\01.flac"
    ORPHAN_FILE = "Music\\Orphan\\01.flac"

    def _make_ctx(self, slskd, rows=(), ledger=(), request_id=1):
        fake_db = FakePipelineDB()
        for row in rows:
            fake_db.seed_request(row)
        if ledger:
            fake_db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=request_id, username=username,
                    filename=filename)
                for (username, filename) in ledger
            ])
            for username, filename in ledger:
                fake_db.confirm_transfer_enqueue(
                    username, filename, request_id=request_id)
        return make_ctx_with_fake_db(fake_db, slskd=slskd)

    def _seed_slskd(self):
        slskd = FakeSlskdAPI()
        slskd.add_transfer(username="peer1", directory="Music\\Owned",
                           filename=self.OWNED_FILE, id="t-owned",
                           state="InProgress")
        slskd.add_transfer(username="peer2", directory="Music\\Orphan",
                           filename=self.ORPHAN_FILE, id="t-orphan",
                           state="Queued, Remotely")
        slskd.add_transfer(username="peer3", directory="Music\\Done",
                           filename="Music\\Done\\01.flac", id="t-done",
                           state="Completed, Succeeded")
        return slskd

    def _owning_row(self):
        return make_request_row(
            id=1, status="downloading",
            active_download_state={
                "filetype": "flac",
                "files": [{"username": "peer1",
                           "filename": self.OWNED_FILE}]})

    OWNED_ORPHAN_LEDGER = (
        ("peer1", OWNED_FILE),
        ("peer2", ORPHAN_FILE),
    )

    def test_cancels_only_ledgered_unbacked_transfers(self):
        """C2: peer1/OWNED is ledgered AND backed by the downloading row
        -> left alone. peer2/ORPHAN is ledgered but unbacked -> cancelled.
        Every transfer here is ledgered (mirrors production: every enqueue
        write-aheads), so this isolates the backed/unbacked axis."""
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        ctx = self._make_ctx(
            slskd, rows=[self._owning_row()], ledger=self.OWNED_ORPHAN_LEDGER)

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 1)
        calls = slskd.transfers.cancel_download_calls
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].username, "peer2")
        self.assertEqual(calls[0].id, "t-orphan")

    def test_snapshot_fetch_excludes_removed_transfers(self):
        """#479 item 3: convergence only needs live transfers — trim the
        payload by requesting includeRemoved=False (it filters
        Completed* itself, so removed/terminal history is dead weight)."""
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        ctx = self._make_ctx(
            slskd, rows=[self._owning_row()], ledger=self.OWNED_ORPHAN_LEDGER)

        converge_slskd_orphans(ctx)

        self.assertEqual(slskd.transfers.get_all_downloads_calls, [False])

    def test_no_downloading_rows_cancels_stranded_ledgered_transfers(self):
        """The Replace scenario: zero downloading rows, two LIVE LEDGERED
        transfers (cratedigger created both, per its write-ahead ledger) —
        both are strays now that nothing backs them."""
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        ctx = self._make_ctx(slskd, rows=[], ledger=self.OWNED_ORPHAN_LEDGER)

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 2)
        cancelled_ids = {c.id for c in slskd.transfers.cancel_download_calls}
        self.assertEqual(cancelled_ids, {"t-owned", "t-orphan"})

    def test_processing_owner_does_not_back_a_live_transfer(self):
        from lib.download import build_active_download_state
        from lib.slskd_transfers import converge_slskd_orphans

        slskd = self._seed_slskd()
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=1, status="wanted"))
        state = build_active_download_state(make_grab_list_entry(files=[
            make_download_file(
                username="peer1",
                filename=self.OWNED_FILE,
                file_dir="Music\\Owned",
            ),
        ]))
        handoff_automation_owner(fake_db, 1, state=state.to_json())
        fake_db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=1,
                username="peer1",
                filename=self.OWNED_FILE,
            ),
        ])
        fake_db.confirm_transfer_enqueue(
            "peer1", self.OWNED_FILE, request_id=1)
        ctx = make_ctx_with_fake_db(fake_db, slskd=slskd)

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(fake_db.get_downloading(), [])
        self.assertEqual(cancelled, 1)
        self.assertEqual(
            [call.id for call in slskd.transfers.cancel_download_calls],
            ["t-owned"],
        )

    def test_ledgered_transfer_self_healed_to_wanted_is_still_cancelled(self):
        """Edge case pinned per the #571 PR 3 brief: a ledgered transfer
        whose request already self-healed back to `wanted` (e.g. after a
        failed cancel/timeout) is STILL the stray C2 targets — the
        ledger row, not the request's current status, proves cratedigger
        created it. `wanted` != `downloading`, so nothing backs it."""
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = FakeSlskdAPI()
        slskd.add_transfer(username="peer2", directory="Music\\Orphan",
                           filename=self.ORPHAN_FILE, id="t-orphan",
                           state="InProgress")
        healed_row = make_request_row(id=7, status="wanted")
        ctx = self._make_ctx(
            slskd, rows=[healed_row], ledger=[("peer2", self.ORPHAN_FILE)],
            request_id=7)

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 1)
        self.assertEqual(slskd.transfers.cancel_download_calls[0].id, "t-orphan")

    def test_foreign_live_transfer_never_cancelled(self):
        """C1 pin, the flip of the old doctrine: zero ledger knowledge and
        zero downloading rows used to mean BOTH live transfers were
        cancelled as "unowned". Now, with no ledger rows at all, neither
        is cratedigger's — both are foreign and neither is touched."""
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        ctx = self._make_ctx(slskd, rows=[])

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 0)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_pending_write_ahead_intent_never_authorizes_cancellation(self):
        """A failed/unknown POST leaves intent, not destructive authority."""
        from lib.slskd_transfers import converge_slskd_orphans

        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="peer2",
            directory="Music\\Manual",
            filename=self.ORPHAN_FILE,
            id="human-transfer",
            state="InProgress",
        )
        fake_db = FakePipelineDB()
        fake_db.record_transfer_enqueue([TransferLedgerRow(
            request_id=7,
            username="peer2",
            filename=self.ORPHAN_FILE,
        )])
        ctx = make_ctx_with_fake_db(fake_db, slskd=slskd)

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 0)
        self.assertEqual(fake_db.get_owned_transfer_keys(), set())
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_snapshot_failure_cancels_nothing(self):
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        slskd.transfers.get_all_downloads_error = RuntimeError("slskd down")
        ctx = self._make_ctx(slskd, rows=[], ledger=self.OWNED_ORPHAN_LEDGER)

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 0)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_cancel_error_does_not_abort_remaining_orphans(self):
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        slskd.transfers.cancel_download_error = RuntimeError("cancel failed")
        ctx = self._make_ctx(slskd, rows=[], ledger=self.OWNED_ORPHAN_LEDGER)

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 0)
        # Both ledgered strays were still attempted despite the first failure.
        self.assertEqual(len(slskd.transfers.cancel_download_calls), 2)

    def test_clean_state_is_a_noop(self):
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = FakeSlskdAPI()
        slskd.add_transfer(username="peer1", directory="Music\\Owned",
                           filename=self.OWNED_FILE, id="t-owned",
                           state="InProgress")
        ctx = self._make_ctx(
            slskd, rows=[self._owning_row()],
            ledger=[("peer1", self.OWNED_FILE)])

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 0)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])


class TestPurgeCompletedTransfers(unittest.TestCase):
    """Every terminal attempt for a ledgered queue key is removed."""

    FILENAME = "Music\\Album\\01 - Track.flac"

    def _make_ctx(self, slskd, ledger_rows=()):
        fake_db = FakePipelineDB()
        for username, filename in ledger_rows:
            fake_db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=1, username=username, filename=filename),
            ])
            fake_db.confirm_transfer_enqueue(
                username, filename, request_id=1)
        return make_ctx_with_fake_db(fake_db, slskd=slskd)

    def _seed_slskd(self):
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="peer1", directory="Music\\Album",
            filename=self.FILENAME, id="t-owned",
            state="Completed, Succeeded")
        slskd.add_transfer(
            username="peer2", directory="Music\\Other",
            filename="Music\\Other\\02.flac", id="t-foreign",
            state="Completed, Succeeded")
        return slskd

    def test_owned_record_is_removed_by_current_slskd_id(self):
        from lib.slskd_transfers import purge_completed_transfers
        slskd = self._seed_slskd()
        ctx = self._make_ctx(slskd, ledger_rows=[
            ("peer1", self.FILENAME),
        ])

        summary = purge_completed_transfers(ctx)

        self.assertEqual(summary.removed, 1)
        calls = slskd.transfers.cancel_download_calls
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].username, "peer1")
        self.assertEqual(calls[0].id, "t-owned")
        self.assertTrue(calls[0].remove)

    def test_slskd_retry_with_a_new_id_is_removed_by_queue_ownership(self):
        """One ledgered enqueue owns every slskd attempt for that queue key."""
        from lib.slskd_transfers import purge_completed_transfers

        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="peer1",
            directory="Music\\Album",
            filename=self.FILENAME,
            id="t-successor",
            state="Completed, Errored",
        )
        ctx = self._make_ctx(slskd, ledger_rows=[
            ("peer1", self.FILENAME),
        ])

        summary = purge_completed_transfers(ctx)

        self.assertEqual(summary.removed, 1)
        self.assertEqual(summary.foreign_count, 0)
        self.assertEqual(
            [(call.id, call.remove)
             for call in slskd.transfers.cancel_download_calls],
            [("t-successor", True)],
        )

    def test_every_owned_terminal_state_is_removed(self):
        from lib.slskd_transfers import purge_completed_transfers
        for index, state in enumerate((
            "Completed, Succeeded",
            "Completed, Aborted",
            "Completed, Cancelled",
            "Completed, Errored",
            "Completed, Rejected",
            "Completed, TimedOut",
        )):
            with self.subTest(state=state):
                slskd = FakeSlskdAPI()
                transfer_id = f"t-{index}"
                filename = f"Music\\Failure\\{index}.flac"
                slskd.add_transfer(
                    username="peer1", directory="Music\\Failure",
                    filename=filename, id=transfer_id, state=state)
                ctx = self._make_ctx(slskd, ledger_rows=[
                    ("peer1", filename),
                ])

                summary = purge_completed_transfers(ctx)

                self.assertEqual(summary.removed, 1)
                self.assertEqual(
                    [(call.id, call.remove)
                     for call in slskd.transfers.cancel_download_calls],
                    [(transfer_id, True)],
                )

    def test_terminal_accounting_conservation_table(self):
        from lib.slskd_transfers import purge_completed_transfers

        cases = (
            ("removed", True, False),
            ("removal_failed", True, True),
            ("foreign_count", False, False),
        )
        for expected_field, owned, removal_fails in cases:
            with self.subTest(category=expected_field):
                transfer_id = f"t-{expected_field}"
                filename = f"Music\\{expected_field}\\track.flac"
                slskd = FakeSlskdAPI()
                slskd.add_transfer(
                    username="peer", directory=f"Music\\{expected_field}",
                    filename=filename, id=transfer_id,
                    state="Completed, Succeeded",
                )
                ledger_rows = [("peer", filename)] if owned else []
                if removal_fails:
                    slskd.transfers.cancel_download_errors_by_id[transfer_id] = (
                        RuntimeError("remove failed"))
                summary = purge_completed_transfers(
                    self._make_ctx(slskd, ledger_rows=ledger_rows))

                self.assertEqual(getattr(summary, expected_field), 1)
                self.assertEqual(
                    summary.removed
                    + summary.removal_failed
                    + summary.foreign_count,
                    1,
                )

    def test_foreign_record_is_never_removed(self):
        from lib.slskd_transfers import purge_completed_transfers
        slskd = self._seed_slskd()
        ctx = self._make_ctx(slskd, ledger_rows=[])

        summary = purge_completed_transfers(ctx)

        self.assertEqual(summary.removed, 0)
        self.assertEqual(summary.foreign_count, 2)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_snapshot_fetch_excludes_removed_transfers(self):
        from lib.slskd_transfers import purge_completed_transfers
        slskd = self._seed_slskd()
        ctx = self._make_ctx(slskd, ledger_rows=[
            ("peer1", self.FILENAME),
        ])

        purge_completed_transfers(ctx)

        self.assertEqual(slskd.transfers.get_all_downloads_calls, [False])

    def test_snapshot_failure_removes_nothing(self):
        from lib.slskd_transfers import purge_completed_transfers
        slskd = self._seed_slskd()
        slskd.transfers.get_all_downloads_error = RuntimeError("slskd down")
        ctx = self._make_ctx(slskd, ledger_rows=[
            ("peer1", self.FILENAME),
        ])

        summary = purge_completed_transfers(ctx)

        self.assertEqual(summary.removed, 0)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_removal_error_does_not_abort_remaining_removals(self):
        from lib.slskd_transfers import purge_completed_transfers
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="peer1", directory="Music\\A",
            filename="Music\\A\\01.flac", id="t-1",
            state="Completed, Succeeded")
        slskd.add_transfer(
            username="peer2", directory="Music\\B",
            filename="Music\\B\\01.flac", id="t-2",
            state="Completed, Succeeded")
        slskd.transfers.cancel_download_error = RuntimeError("remove failed")
        ctx = self._make_ctx(slskd, ledger_rows=[
            ("peer1", "Music\\A\\01.flac"),
            ("peer2", "Music\\B\\01.flac"),
        ])

        summary = purge_completed_transfers(ctx)

        self.assertEqual(summary.removed, 0)
        self.assertEqual(summary.removal_failed, 2)
        self.assertEqual(
            summary.removed + summary.removal_failed
            + summary.foreign_count,
            2,
        )
        self.assertEqual(len(slskd.transfers.cancel_download_calls), 2)

    def test_removal_false_return_is_failed_and_record_remains(self):
        """A rejected slskd removal is retained and counted for retry."""
        from lib.slskd_transfers import purge_completed_transfers
        filename = "Music\\A\\01.flac"
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="peer1", directory="Music\\A",
            filename=filename, id="t-1",
            state="Completed, Succeeded")
        slskd.transfers.cancel_download_results_by_id["t-1"] = False
        ctx = self._make_ctx(slskd, ledger_rows=[
            ("peer1", filename),
        ])

        with patch("lib.slskd_transfers.logger"):
            summary = purge_completed_transfers(ctx)

        self.assertEqual(summary.removed, 0)
        self.assertEqual(summary.removal_failed, 1)
        self.assertEqual(
            summary.removed + summary.removal_failed
            + summary.foreign_count,
            1,
        )
        remaining_ids = {
            transfer.id
            for user in slskd.transfers.get_all_downloads()
            for directory in user.directories
            for transfer in directory.files
        }
        self.assertEqual(remaining_ids, {"t-1"})
        self.assertEqual(
            [call.id for call in slskd.transfers.cancel_download_calls],
            ["t-1"],
        )

    def test_clean_state_is_a_noop(self):
        from lib.slskd_transfers import purge_completed_transfers
        slskd = FakeSlskdAPI()
        ctx = self._make_ctx(slskd, ledger_rows=[])

        summary = purge_completed_transfers(ctx)

        self.assertEqual(summary.removed, 0)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])


class TestFailureEvidenceEnrichmentHook(unittest.TestCase):
    """Download-phase failures opportunistically complete HAVE evidence.

    The request's on-disk copy is already in the library, so a failed
    download is a measurement opportunity, not a dead end: the hook fills
    missing HAVE spectral/V0 evidence through the once-only preview-owned
    helpers, bounded per cycle so failure bursts never balloon the loop.
    """

    WITNESS = "2026-07-29T00:00:00+00:00"

    def _seed_downloading(
        self,
        db: FakePipelineDB,
        **overrides: Any,
    ) -> None:
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": self.WITNESS,
                "files": [],
            },
            **overrides,
        ))

    def _recorder(
        self,
        outcome: HaveEnrichment = HaveEnrichment.ENRICHED,
        error: Exception | None = None,
    ):
        calls: list[int] = []

        def enrich(
            db: Any, *, request_id: int, **_kwargs: Any,
        ) -> HaveEnrichment:
            calls.append(request_id)
            if error is not None:
                raise error
            return outcome

        return enrich, calls

    def _entry(self):
        return make_grab_list_entry(
            files=[make_download_file(filename="01.flac", id="xfer-1",
                                      file_dir="Music\\Album",
                                      username="peer", size=1000)],
            filetype="flac", title="Album", artist="Artist",
            mb_release_id="mb-uuid", db_request_id=42,
        )

    def test_fresh_context_has_default_budget(self):
        ctx = make_ctx_with_fake_db(FakePipelineDB())
        self.assertEqual(ctx.evidence_enrichment_budget, 2)

    def test_work_outcome_consumes_budget(self):
        from lib.download import _enrich_have_evidence_after_failure
        ctx = make_ctx_with_fake_db(FakePipelineDB())
        enrich, calls = self._recorder(HaveEnrichment.ENRICHED)

        _enrich_have_evidence_after_failure(
            42,
            "mb-uuid",
            ctx,
            prepared_outcome=HavePreparation.READY,
            enrich_fn=enrich,
        )

        self.assertEqual(calls, [42])
        self.assertEqual(ctx.evidence_enrichment_budget, 1)

    def test_hook_wires_release_identity_and_beets_config(self):
        from lib.config import CratediggerConfig
        from lib.download import _prepare_have_evidence_before_failure_log

        cfg = CratediggerConfig(beets_directory="/library")
        ctx = make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg)
        received: dict[str, Any] = {}

        def prepare(db: Any, **kwargs: Any) -> HavePreparation:
            received.update(kwargs)
            return HavePreparation.READY

        outcome = _prepare_have_evidence_before_failure_log(
            42,
            "mb-exact-release",
            ctx,
            prepare_fn=prepare,
        )

        self.assertEqual(outcome, HavePreparation.READY)
        self.assertEqual(received["request_id"], 42)
        self.assertEqual(received["mb_release_id"], "mb-exact-release")
        self.assertIs(received["quality_ranks"], cfg.quality_ranks)
        self.assertEqual(received["beets_library_root"], "/library")

    def test_post_failure_hook_wires_release_identity_and_beets_config(self):
        from lib.config import CratediggerConfig
        from lib.download import _enrich_have_evidence_after_failure

        cfg = CratediggerConfig(beets_directory="/library")
        ctx = make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg)
        received: dict[str, Any] = {}

        def enrich(db: Any, **kwargs: Any) -> HaveEnrichment:
            received.update(kwargs)
            return HaveEnrichment.ENRICHED

        _enrich_have_evidence_after_failure(
            42,
            "mb-exact-release",
            ctx,
            prepared_outcome=HavePreparation.READY,
            enrich_fn=enrich,
        )

        self.assertEqual(received["request_id"], 42)
        self.assertEqual(received["mb_release_id"], "mb-exact-release")
        self.assertIs(received["quality_ranks"], cfg.quality_ranks)
        self.assertEqual(received["beets_library_root"], "/library")

    def test_free_outcomes_do_not_consume_budget(self):
        from lib.download import _enrich_have_evidence_after_failure
        for outcome in (
            HaveEnrichment.COMPLETE,
            HaveEnrichment.NO_CURRENT_EVIDENCE,
            HaveEnrichment.STALE,
        ):
            with self.subTest(outcome=outcome):
                ctx = make_ctx_with_fake_db(FakePipelineDB())
                enrich, calls = self._recorder(outcome)

                _enrich_have_evidence_after_failure(
                    42,
                    "mb-uuid",
                    ctx,
                    prepared_outcome=HavePreparation.READY,
                    enrich_fn=enrich,
                )

                self.assertEqual(calls, [42])
                self.assertEqual(ctx.evidence_enrichment_budget, 2)

    def test_exhausted_budget_skips_enrichment(self):
        from lib.download import _enrich_have_evidence_after_failure
        ctx = make_ctx_with_fake_db(FakePipelineDB())
        ctx.evidence_enrichment_budget = 0
        enrich, calls = self._recorder(HaveEnrichment.ENRICHED)

        _enrich_have_evidence_after_failure(
            42,
            "mb-uuid",
            ctx,
            prepared_outcome=HavePreparation.READY,
            enrich_fn=enrich,
        )

        self.assertEqual(calls, [])

    def test_enrichment_error_is_contained_and_budgeted(self):
        from lib.download import _enrich_have_evidence_after_failure
        ctx = make_ctx_with_fake_db(FakePipelineDB())
        enrich, calls = self._recorder(error=RuntimeError("scan exploded"))

        _enrich_have_evidence_after_failure(
            42,
            "mb-uuid",
            ctx,
            prepared_outcome=HavePreparation.READY,
            enrich_fn=enrich,
        )

        self.assertEqual(calls, [42])
        self.assertEqual(ctx.evidence_enrichment_budget, 1)

    def test_timeout_album_runs_enrichment_and_failure_bookkeeping(self):
        from lib.download import _timeout_album
        db = FakePipelineDB()
        self._seed_downloading(db)
        ctx = make_ctx_with_fake_db(db)
        calls: list[int] = []

        def prepare(db: Any, **_kwargs: Any) -> HavePreparation:
            self.assertEqual(db.get_log(limit=1), [])
            self.assertEqual(db.request(42)["status"], "downloading")
            return HavePreparation.READY

        def enrich(
            db: Any, *, request_id: int, **_kwargs: Any,
        ) -> HaveEnrichment:
            db.assert_log(self, 0, outcome="timeout")
            self.assertEqual(db.request(request_id)["status"], "wanted")
            calls.append(request_id)
            return HaveEnrichment.ENRICHED

        with patch("lib.download.cancel_and_delete"):
            _timeout_album(
                self._entry(),
                42,
                "stalled",
                ctx,
                expected_enqueued_at=self.WITNESS,
                prepare_fn=prepare,
                enrich_fn=enrich,
            )

        db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertEqual(calls, [42])
        self.assertEqual(ctx.evidence_enrichment_budget, 1)

    def test_failed_have_preparation_consumes_budget(self):
        from lib.download import _prepare_have_evidence_before_failure_log

        ctx = make_ctx_with_fake_db(FakePipelineDB())

        outcome = _prepare_have_evidence_before_failure_log(
            42,
            "mb-uuid",
            ctx,
            prepare_fn=lambda *_args, **_kwargs: HavePreparation.FAILED,
        )

        self.assertEqual(outcome, HavePreparation.FAILED)
        self.assertEqual(ctx.evidence_enrichment_budget, 1)

    def test_absent_have_preparation_does_not_consume_budget(self):
        from lib.download import _prepare_have_evidence_before_failure_log

        ctx = make_ctx_with_fake_db(FakePipelineDB())

        outcome = _prepare_have_evidence_before_failure_log(
            42,
            "mb-uuid",
            ctx,
            prepare_fn=lambda *_args, **_kwargs: HavePreparation.NO_CURRENT_EVIDENCE,
        )

        self.assertEqual(outcome, HavePreparation.NO_CURRENT_EVIDENCE)
        self.assertEqual(ctx.evidence_enrichment_budget, 2)

    def test_exhausted_budget_attempts_no_have_preparation(self):
        """An exhausted budget returns None — no outcome, nothing attempted.

        ``None`` is distinct from every member ``HavePreparation`` can carry,
        and it must NOT satisfy the downstream ``== READY`` gate. Nothing
        covered the equivalent early return before issue #1313 (it used to
        return the caller-invented token ``"budget_exhausted"``).
        """
        from lib.download import (
            _enrich_have_evidence_after_failure,
            _prepare_have_evidence_before_failure_log,
        )

        ctx = make_ctx_with_fake_db(FakePipelineDB())
        ctx.evidence_enrichment_budget = 0
        prepare_calls: list[int] = []
        enrich, enrich_calls = self._recorder(HaveEnrichment.ENRICHED)

        def prepare(
            _db: object, *, request_id: int, **_kwargs: object,
        ) -> HavePreparation:
            prepare_calls.append(request_id)
            return HavePreparation.READY

        outcome = _prepare_have_evidence_before_failure_log(
            42, "mb-uuid", ctx, prepare_fn=prepare,
        )

        self.assertIsNone(outcome)
        self.assertEqual(prepare_calls, [])
        self.assertEqual(ctx.evidence_enrichment_budget, 0)

        # The None must fall through the enrichment gate exactly as a
        # non-READY outcome does, rather than being treated as prepared.
        _enrich_have_evidence_after_failure(
            42, "mb-uuid", ctx, prepared_outcome=outcome, enrich_fn=enrich,
        )
        self.assertEqual(enrich_calls, [])

    def test_timeout_album_default_enrichment_marks_v0_attempted(self):
        """Default wiring, no injection: a real (failing) probe still lands
        the once-only attempted marker on the exact current snapshot."""
        from lib.beets_db import AlbumInfo
        from lib.download import _timeout_album
        from lib.quality import AudioQualityMeasurement
        from lib.quality_evidence import snapshot_audio_files
        from tests.fakes import FakeBeetsDB

        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"garbage bytes: ffmpeg will fail fast on these")

        db = FakePipelineDB()
        self._seed_downloading(db)
        evidence = make_album_quality_evidence(
            mb_release_id="mb-uuid",
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                spectral_grade="genuine",
                spectral_bitrate_kbps=96,
            ),
            v0_metric=None,
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_request_current_evidence(42, stored.id)
        ctx = make_ctx_with_fake_db(db)
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_info("mb-uuid", AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            is_cbr=True,
            album_path=source,
            format="MP3",
        ))

        with patch("lib.download.cancel_and_delete"), patch(
            "lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets,
        ):
            _timeout_album(
                self._entry(),
                42,
                "stalled",
                ctx,
                expected_enqueued_at=self.WITNESS,
            )

        persisted = db.load_album_quality_evidence_by_id(stored.id)
        assert persisted is not None
        self.assertTrue(persisted.on_disk_v0_research_attempted)
        self.assertEqual(ctx.evidence_enrichment_budget, 1)
        db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(db.request(42)["status"], "wanted")

    def test_timeout_backfill_predates_failure_log_for_recents(self):
        """A newly linked HAVE remains provably historical for its card."""
        from lib.beets_db import AlbumInfo
        from lib.config import CratediggerConfig
        from lib.download import _timeout_album
        from tests.fakes import FakeBeetsDB

        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"garbage bytes: analyzers fail fast")

        db = FakePipelineDB()
        self._seed_downloading(db, mb_release_id="mb-uuid")
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_info("mb-uuid", AlbumInfo(
            album_id=1,
            track_count=17,
            min_bitrate_kbps=183,
            avg_bitrate_kbps=190,
            median_bitrate_kbps=191,
            is_cbr=False,
            album_path=source,
            format="MP3",
        ))
        ctx = make_ctx_with_fake_db(
            db,
            cfg=CratediggerConfig(beets_directory=source),
        )

        with patch("lib.download.cancel_and_delete"), patch(
            "lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets,
        ):
            _timeout_album(
                self._entry(),
                42,
                "stalled",
                ctx,
                expected_enqueued_at=self.WITNESS,
            )

        evidence_id = db.get_request_current_evidence_id(42)
        self.assertIsNotNone(evidence_id)
        log_row = db.get_log(limit=1)[0]
        self.assertEqual(log_row["outcome"], "timeout")
        self.assertTrue(log_row["_current_evidence_is_pre_attempt"])
        self.assertEqual(log_row["_current_evidence_format"], "MP3")
        self.assertEqual(log_row["_current_evidence_avg_bitrate"], 190)

    def test_timeout_v1_reauthorizes_after_wanted_and_preserves_history(self):
        """Timeout bookkeeping reauthorizes v4 repair after ``wanted``."""
        from lib.beets_db import AlbumInfo
        from lib.config import CratediggerConfig
        from lib.download import _timeout_album
        from lib.quality import AlbumQualityV0Metric, AudioQualityMeasurement
        from lib.quality_evidence import snapshot_audio_files
        from tests.fakes import FakeBeetsDB

        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, "01.m4a"), "wb") as handle:
            handle.write(b"legacy current-library bytes")

        db = FakePipelineDB()
        self._seed_downloading(db, mb_release_id="mb-uuid")
        legacy = make_album_quality_evidence(
            mb_release_id="mb-uuid",
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                format="AAC",
                is_cbr=True,
                spectral_grade="genuine",
                spectral_bitrate_kbps=96,
            ),
            lineage_version=1,
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=259,
                avg_bitrate_kbps=267,
                median_bitrate_kbps=269,
                subject="installed",
            ),
        )
        db.upsert_album_quality_evidence(legacy)
        stored = db.find_album_quality_evidence(
            mb_release_id=legacy.mb_release_id,
            snapshot_fingerprint=legacy.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_request_current_evidence(42, stored.id)

        fake_beets = FakeBeetsDB()
        fake_beets.set_album_info("mb-uuid", AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=256,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=256,
            is_cbr=True,
            album_path=source,
            format="AAC",
        ))
        original_resolve_current_release = fake_beets.resolve_current_release
        beets_statuses: list[str] = []

        def resolve_current_release(*args: Any, **kwargs: Any):
            beets_statuses.append(str(db.request(42)["status"]))
            return original_resolve_current_release(*args, **kwargs)

        ctx = make_ctx_with_fake_db(
            db,
            cfg=CratediggerConfig(beets_directory=source),
        )
        with patch("lib.download.cancel_and_delete"), patch(
            "lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets,
        ), patch.object(
            fake_beets,
            "resolve_current_release",
            side_effect=resolve_current_release,
        ):
            _timeout_album(
                self._entry(),
                42,
                "stalled",
                ctx,
                expected_enqueued_at=self.WITNESS,
            )

        self.assertEqual(beets_statuses, ["downloading", "wanted"])
        self.assertEqual(db.request(42)["status"], "wanted")
        current = db.load_album_quality_evidence_by_id(stored.id)
        assert current is not None
        self.assertEqual(
            current.lineage_version,
            CURRENT_EVIDENCE_LINEAGE_VERSION)
        self.assertEqual(current.measured_at, stored.measured_at)
        self.assertEqual(ctx.evidence_enrichment_budget, 1)
        log_row = db.get_log(limit=1)[0]
        self.assertTrue(log_row["_current_evidence_is_pre_attempt"])
        self.assertEqual(log_row["_current_evidence_format"], "AAC")
        self.assertEqual(log_row["_current_evidence_avg_bitrate"], 256)


if __name__ == "__main__":
    unittest.main()
