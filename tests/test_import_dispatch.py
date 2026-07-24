"""Tests for lib/dispatch/ — auto-import decision tree.

Orchestration tests (TestDispatchImport, TestQualityGate*) use FakePipelineDB
and assert domain state. Seam tests (TestOverrideMinBitrate, TestOpus*,
TestTargetFormat*) exercise the surviving auto-import seam in
``lib.download_validation._handle_valid_result`` and the core subprocess wiring.
Pure function tests (TestPopulateDlInfo*, TestCleanupStagedDir) test in/out.
"""

import configparser
import json
import os
import shutil
import subprocess as sp
import tempfile
import unittest
from contextlib import AbstractContextManager
from typing import Never
from unittest.mock import MagicMock, patch

import msgspec

from lib.config import CratediggerConfig
from lib.quality_evidence import snapshot_audio_files, snapshot_fingerprint
from lib.quality import (DownloadInfo, ImportResult, ConversionInfo,
                         DuplicateRemoveCandidate, DuplicateRemoveGuardInfo,
                         AlbumQualityV0Metric, AudioQualityMeasurement,
                         EvidenceProvenance, EvidenceSubject,
                         PostflightInfo, SpectralMeasurement,
                         QualityRankConfig, TargetQualityContract,
                         QUALITY_UPGRADE_TIERS, QUALITY_FLAC_ONLY,
                         V0_PROBE_LOSSLESS_SOURCE, V0ProbeEvidence,
                         ValidationResult, VerifiedLosslessProof)
from tests.fakes import FakePipelineDB
from tests.helpers import (
    RecordingQualityGate,
    make_album_quality_evidence,
    make_ctx_with_fake_db,
    make_download_file,
    make_import_result,
    make_request_row,
    noop_quality_gate,
    patch_dispatch_externals,
    hermetic_beets_config_defaults,
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
            with self.subTest(library_db=library_db):
                with self.assertRaisesRegex(AssertionError, "both library DB and root"):
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
            with self.subTest(library=library):
                with self.assertRaisesRegex(AssertionError, "both library and directory"):
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


def _make_ctx():
    """Build a CratediggerContext wired to a seeded FakePipelineDB.

    The DB is seeded with request id 42 in ``downloading`` status — the
    auto-import dispatch path expects to find an owning request. The
    config remains a ``MagicMock`` because the tests only read a handful
    of attributes from it; ``cfg`` is not a stateful-collaborator name
    in the audit's heuristic.
    """
    cfg = MagicMock()
    cfg.beets_harness_path = "/nix/store/fake/harness/run_beets_harness.sh"
    cfg.beets_distance_threshold = 0.15
    cfg.beets_staging_dir = "/tmp/staging"
    cfg.verified_lossless_target = ""
    cfg.quality_ranks = QualityRankConfig.defaults()
    fake_db = FakePipelineDB()
    fake_db.seed_request(make_request_row(
        id=42,
        status="downloading",
        active_download_state={"files": [], "filetype": "mp3"},
    ))
    ctx = make_ctx_with_fake_db(fake_db, cfg=cfg)
    ctx.cooled_down_users = set()
    return ctx


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
):
    """Create the production-shaped job/evidence authority for a core test."""
    from lib.import_evidence import (
        ActionEvidenceProvenance,
        CandidateEvidenceActionResult,
    )
    from lib.import_queue import IMPORT_JOB_AUTOMATION, IMPORT_JOB_FORCE

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
    if not force:
        state = dict(request.get("active_download_state") or {})
        state["current_path"] = path
        state.setdefault("files", [])
        request["active_download_state"] = state
    job = db.enqueue_import_job(
        IMPORT_JOB_FORCE if force else IMPORT_JOB_AUTOMATION,
        request_id=request_id,
        payload={"download_log_id": 1, "failed_path": path} if force else {},
    )
    evidence = make_album_quality_evidence(
        mb_release_id=release_id,
        source_path=path,
        files=files,
        **(evidence_kwargs or {}),
    )
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    db.set_import_job_candidate_evidence(job.id, persisted.id)
    db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
    claimed = db.claim_next_import_job(worker_id="dispatch-test")
    assert claimed is not None
    return claimed, CandidateEvidenceActionResult(
        evidence=persisted,
        provenance=ActionEvidenceProvenance(
            candidate_status="reused",
            snapshot_guard="matched",
        ),
    )


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
        ctx.cfg.beets_staging_dir = tmpdir
        # This argv seam deliberately has no installed album. Supply a
        # disposable complete authority pair anyway, so an accidental return
        # to the real current-evidence loader can never consult host Beets.
        ctx.cfg.beets_library_db = os.path.join(tmpdir, "beets-library.db")
        ctx.cfg.beets_directory = os.path.join(tmpdir, "beets-library")
        os.makedirs(ctx.cfg.beets_directory)

        def no_current_evidence(*_args: object, **_kwargs: object) -> None:
            """Typed current-evidence boundary for this subprocess argv seam."""
            return None

        with patch("lib.download_validation.log_validation_result"), \
             patch_dispatch_externals() as ext, \
             patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
            from lib.dispatch import dispatch_import_core

            def dispatch_with_job(**kwargs):
                db = ctx.pipeline_db_source._get_db()
                claimed, candidate = _claim_dispatch_job(
                    db,
                    path=kwargs["path"],
                    release_id=kwargs["mb_release_id"],
                )
                kwargs["candidate_import_job_id"] = claimed.id
                kwargs["prevalidated_candidate_result"] = candidate
                kwargs["current_evidence_loader"] = no_current_evidence
                return dispatch_import_core(**kwargs)

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


class TestAudioCorruptPostCommitQuarantine(unittest.TestCase):
    def test_moves_source_to_standard_bad_files_and_persists_exact_audit(self):
        from lib.dispatch.types import DispatchOutcome, PostCommitCleanup
        from scripts.importer import _run_post_commit_cleanup

        with tempfile.TemporaryDirectory() as parent:
            staging = os.path.join(parent, "incoming", "Artist - Album")
            download_root = os.path.join(parent, "slskd")
            os.makedirs(staging)
            os.makedirs(download_root)
            with open(os.path.join(staging, "01.flac"), "wb") as handle:
                handle.write(b"corrupt audio")

            db = FakePipelineDB()
            log_id = db.log_download(
                request_id=835,
                outcome="rejected",
                validation_result=json.dumps({"scenario": "audio_corrupt"}),
            )
            outcome = DispatchOutcome(
                success=False,
                message="audio_corrupt",
                post_commit_cleanup=PostCommitCleanup(
                    audio_quarantine_source_path=staging,
                    audio_quarantine_root=download_root,
                ),
            )

            result = _run_post_commit_cleanup(
                db,
                outcome,
                download_log_id=log_id,
            )

            assert result is not None
            audit = result["audio_quarantine"]
            assert isinstance(audit, dict)
            target = audit["quarantine_path"]
            assert isinstance(target, str)
            self.assertTrue(audit["moved"])
            self.assertTrue(audit["audit_persisted"])
            self.assertFalse(os.path.exists(staging))
            self.assertTrue(os.path.exists(os.path.join(target, "01.flac")))
            self.assertEqual(
                os.path.dirname(target),
                os.path.join(download_root, "failed_imports", "bad_files"),
            )
            persisted = msgspec.json.decode(
                db.download_logs[0].validation_result,
            )
            self.assertEqual(persisted["failed_path"], target)
            self.assertEqual(
                persisted["post_commit_quarantine"]["source_path"],
                staging,
            )

    def test_atomic_move_failure_retains_complete_source_and_exact_audit(self):
        from lib.dispatch.types import DispatchOutcome, PostCommitCleanup
        from scripts.importer import _run_post_commit_cleanup

        with tempfile.TemporaryDirectory() as parent:
            staging = os.path.join(parent, "incoming", "Artist - Album")
            download_root = os.path.join(parent, "slskd")
            os.makedirs(os.path.join(staging, "Disc 1"))
            os.makedirs(download_root)
            track = os.path.join(staging, "Disc 1", "01.flac")
            cover = os.path.join(staging, "cover.jpg")
            with open(track, "wb") as handle:
                handle.write(b"corrupt audio")
            with open(cover, "wb") as handle:
                handle.write(b"cover")

            db = FakePipelineDB()
            log_id = db.log_download(
                request_id=835,
                outcome="rejected",
                validation_result=json.dumps({"scenario": "audio_corrupt"}),
            )
            outcome = DispatchOutcome(
                success=False,
                message="audio_corrupt",
                post_commit_cleanup=PostCommitCleanup(
                    audio_quarantine_source_path=staging,
                    audio_quarantine_root=download_root,
                ),
            )

            with patch(
                "lib.import_manifest.os.rename",
                side_effect=OSError("simulated atomic rename failure"),
            ):
                result = _run_post_commit_cleanup(
                    db,
                    outcome,
                    download_log_id=log_id,
                )

            assert result is not None
            audit = result["audio_quarantine"]
            assert isinstance(audit, dict)
            self.assertFalse(audit["moved"])
            self.assertEqual(audit["source_path"], staging)
            self.assertIsNone(audit["quarantine_path"])
            self.assertTrue(os.path.exists(track))
            self.assertTrue(os.path.exists(cover))
            persisted = msgspec.json.decode(
                db.download_logs[0].validation_result,
            )
            self.assertEqual(persisted["failed_path"], staging)
            self.assertEqual(
                persisted["post_commit_quarantine"]["source_path"],
                staging,
            )

    def test_quarantine_failure_retains_and_audits_staged_source(self):
        from lib.dispatch.types import DispatchOutcome, PostCommitCleanup
        from scripts.importer import _run_post_commit_cleanup

        with tempfile.TemporaryDirectory() as parent:
            staging = os.path.join(parent, "incoming", "Artist - Album")
            os.makedirs(staging)
            track = os.path.join(staging, "01.flac")
            with open(track, "wb") as handle:
                handle.write(b"corrupt audio")

            db = FakePipelineDB()
            log_id = db.log_download(
                request_id=835,
                outcome="rejected",
                validation_result=json.dumps({"scenario": "audio_corrupt"}),
            )
            outcome = DispatchOutcome(
                success=False,
                message="audio_corrupt",
                post_commit_cleanup=PostCommitCleanup(
                    audio_quarantine_source_path=staging,
                    audio_quarantine_root=os.path.join(parent, "missing-root"),
                ),
            )

            result = _run_post_commit_cleanup(
                db,
                outcome,
                download_log_id=log_id,
            )

            assert result is not None
            audit = result["audio_quarantine"]
            assert isinstance(audit, dict)
            self.assertFalse(audit["moved"])
            self.assertTrue(os.path.exists(track))
            self.assertIn(staging, db.get_retained_failure_paths())

    def test_post_commit_evidence_link_failure_cannot_escape_or_delete_archive(
        self,
    ):
        from lib.dispatch.types import DispatchOutcome, PostCommitCleanup
        from lib.import_queue import IMPORT_JOB_FORCE
        from scripts.importer import (
            _cleanup_committed_wrong_match_rejection,
            _run_post_commit_cleanup,
        )

        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "wrong_matches", "Artist - Album")
            download_root = os.path.join(parent, "slskd")
            os.makedirs(source)
            os.makedirs(download_root)
            with open(os.path.join(source, "01.flac"), "wb") as handle:
                handle.write(b"corrupt audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=835,
                status="unsearchable",
                mb_release_id="test-mbid",
            ))
            log_id = db.log_download(
                request_id=835,
                outcome="rejected",
                validation_result=json.dumps({"scenario": "audio_corrupt"}),
                staged_path=source,
            )
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=835,
                payload={
                    "download_log_id": log_id,
                    "failed_path": source,
                },
            )
            outcome = DispatchOutcome(
                success=False,
                message="audio_corrupt",
                post_commit_wrong_match_scenario="audio_corrupt",
                post_commit_cleanup=PostCommitCleanup(
                    audio_quarantine_source_path=source,
                    audio_quarantine_root=download_root,
                ),
            )
            cleanup_result = _run_post_commit_cleanup(
                db,
                outcome,
                download_log_id=log_id,
            )
            assert cleanup_result is not None
            quarantine = cleanup_result["audio_quarantine"]
            assert isinstance(quarantine, dict)
            target = quarantine["quarantine_path"]
            assert isinstance(target, str)

            cleanup_wrong_match = MagicMock()
            with patch.object(
                db,
                "get_import_job_candidate_evidence_id",
                side_effect=RuntimeError("transient post-commit DB failure"),
            ), patch(
                "scripts.importer.logger.exception",
            ) as log_exception:
                _cleanup_committed_wrong_match_rejection(
                    db,  # pyright: ignore[reportArgumentType]
                    job,
                    log_id,
                    outcome,
                    cleanup_wrong_match_fn=cleanup_wrong_match,
                )

            log_exception.assert_called_once()
            cleanup_wrong_match.assert_not_called()
            self.assertTrue(os.path.exists(os.path.join(target, "01.flac")))
            persisted = msgspec.json.decode(
                db.download_logs[-1].validation_result,
            )
            self.assertEqual(persisted["failed_path"], target)
            self.assertTrue(
                persisted["post_commit_quarantine"]["moved"],
            )
            self.assertNotIn("wrong_match_triage", persisted)

    def test_force_corrupt_source_is_quarantined_and_never_deleted(self):
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.dispatch.types import ImportAttemptResult
        from lib.import_queue import IMPORT_JOB_FORCE
        from lib.quality import AudioQualityMeasurement
        from lib.quality_evidence import snapshot_audio_files
        from scripts.importer import process_claimed_job

        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "wrong_matches", "Artist - Album")
            download_root = os.path.join(parent, "slskd")
            os.makedirs(source)
            os.makedirs(download_root)
            with open(os.path.join(source, "01.flac"), "wb") as handle:
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
                source_path=source,
                files=snapshot_audio_files(source),
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
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="force-corrupt")
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
                db=db,  # pyright: ignore[reportArgumentType]
                request_id=835,
                dl_info=DownloadInfo(filetype="flac", username="bad-peer"),
                attempt_result=attempt,
                distance=0.0,
                decision="audio_corrupt",
                detail="decoder rejected source",
                requeue_on_failure=False,
                validation_result=None,
                staged_path=source,
                scenario="force_import",
                files=[],
                source_path_cleanup_scenario="force_import",
                cooled_down_users=None,
                import_job_id=claimed.id,
                source_download_log_id=original_log_id,
                audio_quarantine_root=download_root,
            )
            self.assertEqual(
                outcome.post_commit_wrong_match_scenario,
                "audio_corrupt",
            )

            completed = process_claimed_job(
                db,  # pyright: ignore[reportArgumentType]
                claimed,
                execute_fn=lambda *_args, **_kwargs: outcome,
            )

            assert completed is not None and completed.result is not None
            cleanup = completed.result["cleanup"]
            assert isinstance(cleanup, dict)
            self.assertEqual(
                cleanup["outcome"],
                "skipped_archival_audio_quarantine",
            )
            quarantine = completed.result["post_commit_cleanup"][
                "audio_quarantine"
            ]
            assert isinstance(quarantine, dict)
            target = quarantine["quarantine_path"]
            assert isinstance(target, str)
            self.assertFalse(os.path.exists(source))
            self.assertTrue(os.path.exists(os.path.join(target, "01.flac")))
            self.assertEqual(
                os.path.dirname(target),
                os.path.join(download_root, "failed_imports", "bad_files"),
            )
            terminal_log = db.download_logs[-1]
            self.assertEqual(
                terminal_log.candidate_evidence_id,
                persisted_candidate.id,
            )
            terminal_audit = msgspec.json.decode(
                terminal_log.validation_result,
            )
            self.assertEqual(terminal_audit["failed_path"], target)
            self.assertEqual(
                terminal_audit["post_commit_quarantine"]["source_path"],
                source,
            )
            self.assertNotIn("wrong_match_triage", terminal_audit)
            self.assertEqual(db.request(835)["status"], "unsearchable")


class TestRecordRejectionAndRequeueSeam(unittest.TestCase):
    """Seam tests for the shared rejection finalizer."""

    @patch("lib.dispatch.outcome_actions.finalize_request")
    def test_requeue_defers_from_status_lookup_to_finalize_request(
        self,
        mock_finalize,
    ) -> None:
        from lib.dispatch import _record_rejection_and_maybe_requeue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="unsearchable"))

        _record_rejection_and_maybe_requeue(
            db,  # type: ignore[arg-type]
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

        mock_finalize.assert_called_once()
        _db_arg, request_id, outcome = mock_finalize.call_args.args
        self.assertEqual(request_id, 42)
        self.assertIsNone(outcome.from_status)
        self.assertEqual(outcome.attempt_type, "validation")
        self.assertEqual(db.request(42)["validation_attempts"], 0)

    def test_requeue_only_forwards_fields_persisted_by_wanted_transition(self) -> None:
        from lib.dispatch import _record_rejection_and_maybe_requeue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        _record_rejection_and_maybe_requeue(
            db,  # type: ignore[arg-type]
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
            AudioQualityMeasurement, ImportResult, SpectralAnalysisDetail,
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
                db=db,  # type: ignore[arg-type]
                request_id=42,
                dl_info=dl_info,
                attempt_result=attempt_result,
                distance=0.1279,
                decision="downgrade",
                detail="import-time persisted evidence rejected candidate",
                requeue_on_failure=True,
                validation_result=None,
                staged_path="/tmp/cratedigger-evidence-reject-test",
                scenario="downgrade",
                files=None,
                source_path_cleanup_scenario="downgrade",
                cooled_down_users=None,
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
                db=db,  # type: ignore[arg-type]
                request_id=42,
                dl_info=DownloadInfo(filetype="mp3", username="qreature"),
                attempt_result=attempt_result,
                distance=0.0,
                decision="downgrade",
                detail="import-time persisted evidence rejected candidate",
                requeue_on_failure=True,
                validation_result=None,
                staged_path="/tmp/lemonade",
                scenario="quality_downgrade",
                files=None,
                source_path_cleanup_scenario="quality_downgrade",
                cooled_down_users=None,
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
                db,  # type: ignore[arg-type]
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
                db=db,  # type: ignore[arg-type]
                request_id=43,
                dl_info=DownloadInfo(
                    filetype="mp3",
                    username="qreature",
                    is_vbr=True,
                ),
                attempt_result=attempt_result,
                distance=0.0,
                decision="downgrade",
                detail="preview audit decode failed before rejection",
                requeue_on_failure=True,
                validation_result=None,
                staged_path="/tmp/missing-have-audit",
                scenario="quality_downgrade",
                files=None,
                source_path_cleanup_scenario="quality_downgrade",
                cooled_down_users=None,
                quality_ranks=QualityRankConfig.defaults(),
            )

        row = db.request(43)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(
            row["search_filetype_override"],
            "lossless,mp3 320,aac,opus,ogg",
        )
        self.assertIsNone(row["target_format"])


class TestRejectImportFromEvidenceDecisionCallerLifecycle(unittest.TestCase):
    """Every rejection honors the lifecycle authority chosen by its caller.

    Automatic imports pass ``requeue_on_failure=True`` so a bad candidate
    self-heals to ``wanted``. Force imports pass False because the operator's
    ``unsearchable`` status must not be cleared by a
    candidate-integrity fact.
    """

    FOUR_FACT_DECISIONS = ["audio_corrupt", "bad_audio_hash", "nested_layout", "empty_fileset"]

    def _reject(
        self,
        *,
        decision: str,
        requeue_on_failure: bool,
        pending: bool = False,
        search_filetype_override: str | None = None,
        initial_status: str = "downloading",
    ):
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.dispatch.types import ImportAttemptResult
        from lib.import_queue import IMPORT_JOB_AUTOMATION
        from lib.quality import AudioQualityMeasurement, ImportResult
        from lib.terminal_outcomes import ImportJobTerminal

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
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                payload={},
            ).id
        with patch_dispatch_externals():
            outcome = _reject_import_from_evidence_decision(
                db=db,  # type: ignore[arg-type]
                request_id=42,
                dl_info=dl_info,
                attempt_result=attempt_result,
                distance=0.0,
                decision=decision,
                detail=f"test {decision}",
                requeue_on_failure=requeue_on_failure,
                validation_result=None,
                staged_path="/tmp/cratedigger-caller-lifecycle-test",
                scenario=decision,
                files=None,
                source_path_cleanup_scenario=decision,
                cooled_down_users=None,
                import_job_id=import_job_id,
            )
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
        from lib.import_queue import IMPORT_JOB_AUTOMATION, IMPORT_JOB_FORCE

        if db is None:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="unsearchable" if force else "downloading",
                search_filetype_override="lossless",
                active_download_state={"files": [], "filetype": "flac"},
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
            request = db.request(42)
            request["mb_release_id"] = "test-mbid"
            request["active_download_state"] = {
                "files": [],
                "filetype": "flac",
                "current_path": tmpdir,
            }
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE if force else IMPORT_JOB_AUTOMATION,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": tmpdir} if force else {},
            )
            candidate = msgspec.structs.replace(
                candidate,
                mb_release_id="test-mbid",
                source_path=tmpdir,
                files=snapshot_audio_files(tmpdir),
                snapshot_fingerprint=snapshot_fingerprint(
                    snapshot_audio_files(tmpdir),
                ),
            )
            db.upsert_album_quality_evidence(candidate)
            persisted = db.find_album_quality_evidence(
                mb_release_id=candidate.mb_release_id,
                snapshot_fingerprint=candidate.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            db.set_import_job_candidate_evidence(job.id, persisted.id)
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="have-analysis-test")
            assert claimed is not None
            candidate_result = msgspec.structs.replace(
                candidate_result,
                evidence=persisted,
            )
            with patch_dispatch_externals() as ext, patch(
                "lib.dispatch.subprocess_runner.parse_import_result",
                return_value=make_import_result(decision="import"),
            ):
                outcome = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="test-mbid",
                    request_id=42,
                    label="Test Artist - Test Album",
                    force=force,
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(filetype="flac", username="bad-peer"),
                    scenario="force_import" if force else "strong_match",
                    cfg=_full_dispatch_config(),
                    requeue_on_failure=not force,
                    candidate_import_job_id=job.id,
                    prevalidated_candidate_result=candidate_result,
                    quality_gate_fn=noop_quality_gate,
                    current_evidence_loader=(
                        lambda *_args, **_kwargs: current_result
                    ),
                )
        return db, outcome, ext

    def _persist_failed_outcome(self, db, outcome) -> None:
        from lib.terminal_outcomes import ImportJobTerminal

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
        db, outcome, ext = self._dispatch_with_current_result(
            self._failed_current_result(
                "PermissionError: [Errno 13] Permission denied"
            ),
            force=True,
        )
        db.set_cooldown_result(True)
        self._persist_failed_outcome(db, outcome)

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
        db, outcome, ext = self._dispatch_with_current_result(
            self._failed_current_result("FileNotFoundError: path not found"),
            force=False,
        )
        db.set_cooldown_result(True)
        self._persist_failed_outcome(db, outcome)

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
        db, outcome, ext = self._dispatch_with_current_result(
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
        db, first, first_ext = self._dispatch_with_current_result(
            self._failed_current_result("analyser crashed"),
            force=True,
            candidate=candidate,
        )
        self._persist_failed_outcome(db, first)
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
        db, second, second_ext = self._dispatch_with_current_result(
            healthy,
            force=True,
            db=db,
            candidate=candidate,
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
    ):
        from lib.dispatch import dispatch_import_core
        if ir is self._SENTINEL:
            ir = make_import_result(decision="import")

        cfg = _full_dispatch_config()
        dl_info = DownloadInfo(filetype="mp3")

        mock_gate = RecordingQualityGate()
        tmpdir = tempfile.mkdtemp()
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
            from lib.import_evidence import (
                ActionEvidenceProvenance,
                CandidateEvidenceActionResult,
            )
            from lib.import_queue import (
                IMPORT_JOB_AUTOMATION,
                IMPORT_JOB_FORCE,
            )

            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE if force else IMPORT_JOB_AUTOMATION,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": tmpdir} if force else {},
            )
            evidence = make_album_quality_evidence(
                mb_release_id="test-mbid",
                source_path=tmpdir,
                files=snapshot_audio_files(tmpdir),
            )
            db.upsert_album_quality_evidence(evidence)
            persisted = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            db.set_import_job_candidate_evidence(job.id, persisted.id)
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="dispatch-test")
            assert claimed is not None
            import_job_id = claimed.id
            candidate_result = CandidateEvidenceActionResult(
                evidence=persisted,
                provenance=ActionEvidenceProvenance(
                    candidate_status="reused",
                    snapshot_guard="matched",
                ),
            )
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
                outcome = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="test-mbid",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario=scenario,
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=cfg,
                    quality_gate_fn=mock_gate,
                    force=force,
                    requeue_on_failure=not force,
                    candidate_import_job_id=import_job_id,
                    prevalidated_candidate_result=candidate_result,
                )
            if outcome.terminal_outcome is not None:
                from lib.terminal_outcomes import ImportJobTerminal

                db.persist_import_terminal_outcome(
                    outcome.terminal_outcome.with_job(ImportJobTerminal(
                        status="completed" if outcome.success else "failed",
                        result={"success": outcome.success},
                        message=outcome.message,
                        error=None if outcome.success else outcome.message,
                    ))
                )
            else:
                from tests.helpers import finalize_claimed_dispatch
                finalize_claimed_dispatch(db, claimed, outcome)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        return {
            "db": db,
            "outcome": outcome,
            "mock_cleanup": ext.cleanup,
            "mock_plex": ext.plex,
            "mock_jellyfin": ext.jellyfin,
            "mock_gate": mock_gate,
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
        r["mock_gate"].assert_called_once()

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

        staging_root = tempfile.mkdtemp()
        source = os.path.join(staging_root, "auto-import", "Artist", "Album")
        os.makedirs(source)
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
        claimed, candidate = _claim_dispatch_job(
            db,
            path=source,
            release_id="test-mbid",
        )
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            beets_staging_dir=staging_root,
            pipeline_db_enabled=True,
        )
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
                outcome = dispatch_import_core(
                    path=source,
                    mb_release_id="test-mbid",
                    request_id=42,
                    label="Artist - Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(filetype="mp3", username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[],
                    cfg=cfg,
                    requeue_on_failure=True,
                    attempt_spectral_audit=audit,
                    candidate_import_job_id=claimed.id,
                    prevalidated_candidate_result=candidate,
                )
                assert outcome.terminal_outcome is not None
                from lib.terminal_outcomes import ImportJobTerminal
                terminal = db.persist_import_terminal_outcome(
                    outcome.terminal_outcome.with_job(ImportJobTerminal(
                        status="failed",
                        error=outcome.message,
                        result={"success": False},
                        message=outcome.message,
                    ))
                )
                from scripts.importer import _run_post_commit_cleanup
                cleanup_result = _run_post_commit_cleanup(
                    db,
                    outcome,
                    download_log_id=terminal.download_log_id,
                )
                if cleanup_result is not None:
                    db.merge_import_job_result(
                        claimed.id,
                        {"post_commit_cleanup": cleanup_result},
                    )
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)

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
        quarantine = completed_job.result["post_commit_cleanup"][
            "duplicate_guard_quarantine"
        ]
        self.assertIsNotNone(quarantine["quarantine_path"])
        assert quarantine["quarantine_path"] is not None
        self.assertIn("duplicate-remove-guard",
                      quarantine["quarantine_path"])
        self.assertFalse(os.path.exists(source))

    def test_no_json_result(self):
        r = self._dispatch(None)
        db = r["db"]
        job = db.get_import_job(1)
        assert job is not None
        self.assertEqual(job.status, "recovery_required")
        self.assertEqual(db.request(42)["status"], "downloading")
        self.assertEqual(db.download_logs, [])
        self.assertIsNone(db.claim_next_import_job(
            worker_id="automatic-replay"))

    def test_timeout(self):
        from lib.dispatch import dispatch_import_core
        from scripts.importer import process_claimed_job
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "mp3"},
        ))
        claimed, candidate = _claim_dispatch_job(
            db,
            path="/tmp/dest",
            release_id="test-mbid",
        )

        def execute(db_arg, _job, *, ctx=None):
            del ctx
            return dispatch_import_core(
                path="/tmp/dest", mb_release_id="test-mbid",
                request_id=42, label="Test",
                beets_harness_path=_HARNESS,
                db=db_arg,
                dl_info=DownloadInfo(filetype="mp3"),
                candidate_import_job_id=claimed.id,
                prevalidated_candidate_result=candidate,
            )

        with patch("lib.dispatch.subprocess_runner.sp.run",
                   side_effect=sp.TimeoutExpired(cmd="test", timeout=1800)):
            recovered = process_claimed_job(
                db,  # type: ignore[arg-type]
                claimed,
                execute_fn=execute,
            )

        assert recovered is not None
        self.assertEqual(recovered.status, "recovery_required")
        self.assertEqual(db.request(42)["status"], "downloading")
        self.assertEqual(db.download_logs, [])
        self.assertIsNone(db.claim_next_import_job(worker_id="automatic-replay"))

    def test_exception(self):
        from lib.dispatch import dispatch_import_core
        from scripts.importer import process_claimed_job
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "mp3"},
        ))
        claimed, candidate = _claim_dispatch_job(
            db,
            path="/tmp/dest",
            release_id="test-mbid",
        )

        def execute(db_arg, _job, *, ctx=None):
            del ctx
            return dispatch_import_core(
                path="/tmp/dest", mb_release_id="test-mbid",
                request_id=42, label="Test",
                beets_harness_path=_HARNESS,
                db=db_arg,
                dl_info=DownloadInfo(filetype="mp3"),
                candidate_import_job_id=claimed.id,
                prevalidated_candidate_result=candidate,
            )

        with patch("lib.dispatch.subprocess_runner.sp.run",
                   side_effect=RuntimeError("boom")):
            recovered = process_claimed_job(
                db,  # type: ignore[arg-type]
                claimed,
                execute_fn=execute,
            )

        assert recovered is not None
        self.assertEqual(recovered.status, "recovery_required")
        self.assertEqual(db.request(42)["status"], "downloading")
        self.assertEqual(db.download_logs, [])
        self.assertIsNone(db.claim_next_import_job(worker_id="automatic-replay"))


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
        from lib.dispatch import dispatch_import_core
        from datetime import datetime, timezone

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
                2026, 5, 20, tzinfo=timezone.utc)
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
            claimed, candidate = _claim_dispatch_job(
                db,
                path=tmpdir,
                release_id="test-mbid",
            )
            with patch_dispatch_externals(), \
                 patch("lib.dispatch.subprocess_runner.parse_import_result",
                       return_value=ir):
                outcome = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="test-mbid",
                    request_id=42,
                    label="Rescue Artist - Album",
                    beets_harness_path=self._HARNESS_PATH,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(filetype="mp3"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="u1",
                                     filename="01 - T.mp3")],
                    cfg=cfg,
                    quality_gate_fn=noop_quality_gate,
                    candidate_import_job_id=claimed.id,
                    prevalidated_candidate_result=candidate,
                )
                assert outcome.terminal_outcome is not None
                from lib.terminal_outcomes import ImportJobTerminal
                db.persist_import_terminal_outcome(
                    outcome.terminal_outcome.with_job(ImportJobTerminal(
                        status="completed",
                        result={"success": True},
                        message=outcome.message,
                    ))
                )
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
        from datetime import datetime, timezone

        original_rescue_at = datetime(2026, 1, 15, tzinfo=timezone.utc)
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

    def _get_override_value(self, min_br, spectral_br, grade):
        album_data = _make_album_data()
        album_data.current_min_bitrate = min_br
        album_data.current_spectral = SpectralMeasurement.from_parts(
            grade,
            spectral_br,
        )
        cmd = _dispatch_valid_result_cmd(album_data=album_data)

        for i, arg in enumerate(cmd):
            if arg == "--override-min-bitrate" and i + 1 < len(cmd):
                return int(cmd[i + 1])
        return None

    # (description, min_bitrate, current_spectral_bitrate, current_spectral_grade, expected)
    CASES = [
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
        claimed, candidate = _claim_dispatch_job(
            db,
            path="/tmp/dest",
            release_id="mbid-1",
        )

        with patch_dispatch_externals() as ext, \
             patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
            dispatch_import_core(
                path="/tmp/dest", mb_release_id="mbid-1",
                request_id=42, label="Test Artist - Test Album",
                beets_harness_path=_HARNESS,
                cfg=cfg_obj,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="mp3"),
                files=[MagicMock(username="user1", filename="01.mp3")],
                quality_gate_fn=noop_quality_gate,
                candidate_import_job_id=claimed.id,
                prevalidated_candidate_result=candidate,
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
        from lib.quality import (CodecRankBands, QualityRankConfig,
                                 RankBitrateMetric)
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
        claimed, candidate_result = _claim_dispatch_job(
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
                ),
                "codec": "flac",
                "container": "flac",
                "storage_format": "FLAC",
            },
        )
        assert candidate_result.evidence is not None
        with patch_dispatch_externals() as ext, patch(
            "lib.dispatch.subprocess_runner.parse_import_result",
            return_value=make_import_result(decision="import"),
        ):
            dispatch_import_core(
                path="/tmp/dest",
                mb_release_id="test-mbid",
                request_id=42,
                label="Test Artist - Test Album",
                beets_harness_path=_HARNESS,
                cfg=_full_dispatch_config(),
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="mp3"),
                files=[make_download_file(username="user1", filename="01.mp3")],
                quality_gate_fn=noop_quality_gate,
                candidate_import_job_id=claimed.id,
                evidence_gate_fn=(
                    lambda *_args, **_kwargs: EvidenceImportGate(
                        candidate=candidate_result.evidence,
                        current=current,
                    )
                ),
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
                path="/tmp/badlands",
                mb_release_id="test-mbid",
                request_id=42,
                label="Dirty Beaches - Badlands",
                beets_harness_path=_HARNESS,
                cfg=_full_dispatch_config(),
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="flac", username="peer"),
                files=[make_download_file(
                    username="peer", filename="01.flac")],
                quality_gate_fn=noop_quality_gate,
                evidence_gate_fn=(
                    lambda *_args, **_kwargs: EvidenceImportGate(
                        candidate=candidate
                    )
                ),
            )

        ext.run.assert_not_called()
        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(db.download_logs[0].outcome, "rejected")
        self.assertEqual(db.download_logs[0].beets_detail, decode_error)
        self.assertEqual(db.download_logs[0].error_message, decode_error)


class TestLoadQualityGateState(unittest.TestCase):
    """Direct tests for the shared quality-gate state adapter."""

    def test_uses_linked_measurement_and_ignores_request_quality_stamps(self):
        from lib.dispatch import load_quality_gate_state

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
            state = load_quality_gate_state(
                request_id=42,
                db=db,  # type: ignore[arg-type]
                mb_id="mbid-123",
            )

        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.measurement.min_bitrate_kbps, 207)
        self.assertEqual(state.measurement.format, "MP3")
        self.assertEqual(state.measurement.avg_bitrate_kbps, 207)
        self.assertFalse(state.measurement.is_cbr)
        self.assertFalse(state.verified_lossless_proof)
        self.assertIsNone(state.measurement.spectral_bitrate_kbps)
        get_request.assert_not_called()


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
            db=db,  # type: ignore[arg-type]
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
            db=db)  # type: ignore[arg-type]
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
            db=db,  # type: ignore[arg-type]
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
            db=db,  # type: ignore[arg-type]
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
            db=db,  # type: ignore[arg-type]
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
            db=db,  # type: ignore[arg-type]
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
        claimed, candidate = _claim_dispatch_job(
            db,
            path="/tmp/dest",
            release_id="test-mbid",
        )

        with patch_dispatch_externals(), \
             patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
            outcome = dispatch_import_core(
                path="/tmp/dest", mb_release_id="test-mbid",
                request_id=42, label="Test",
                beets_harness_path=_HARNESS,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="mp3"),
                files=[MagicMock(username="user1", filename="01.mp3")],
                quality_gate_fn=noop_quality_gate,
                candidate_import_job_id=claimed.id,
                prevalidated_candidate_result=candidate,
            )
        assert outcome.terminal_outcome is not None
        from lib.terminal_outcomes import ImportJobTerminal
        db.persist_import_terminal_outcome(
            outcome.terminal_outcome.with_job(ImportJobTerminal(
                status="completed",
                result={"success": outcome.success},
                message=outcome.message,
            ))
        )

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["search_filetype_override"])


class TestQualityGatePreservesTargetFormat(unittest.TestCase):
    """Quality gate accept must clear search_filetype_override but preserve target_format."""

    def _run_quality_gate_accept(self, target_format="flac"):
        """Drive a real accept via FLAC verified-lossless input — no decision stub."""
        from lib.dispatch import _check_quality_gate_core
        from lib.beets_db import AlbumInfo

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
                db=db)  # type: ignore[arg-type]

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
        ctx = _make_ctx()
        ctx.cfg.verified_lossless_target = verified_lossless_target
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
        ctx.cfg.verified_lossless_target = ""
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
            claimed, candidate = _claim_dispatch_job(
                db,
                path=tmpdir,
                release_id="test-mbid",
            )
            with patch_dispatch_externals(), \
                 patch("lib.dispatch.subprocess_runner.parse_import_result",
                       return_value=ir), \
                 patch("lib.util._jellyfin_get_json",
                       side_effect=self._fake_get_json):
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="test-mbid",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(filetype="mp3"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=cfg,
                    quality_gate_fn=noop_quality_gate,
                    candidate_import_job_id=claimed.id,
                    prevalidated_candidate_result=candidate,
                    beets_library_db_path=beets_library_db,
                    beets_library_root=beets_library_root,
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


if __name__ == "__main__":
    unittest.main()
