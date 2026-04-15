"""Force/manual-import must run the same pre-import gates as auto-import.

RED tests that lock in the contract: force-import and manual-import paths
may only skip the beets *distance* check. All other pre-import gates
(audio integrity, spectral transcode detection) run identically.

These tests FAIL against the current code because dispatch_import_from_db
calls dispatch_import_core directly, bypassing the audio + spectral gates
that _process_beets_validation runs in the auto path.
"""

from __future__ import annotations

import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from lib.beets_db import AlbumInfo
from lib.config import SoularrConfig
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_import_result,
    make_request_row,
    patch_dispatch_externals,
)
from tests.test_integration_slices import _HARNESS, _make_stdout, _mock_beets_db


def _import_one_called(mock_run) -> bool:
    """Did any sp.run call invoke import_one.py (vs mp3val/ffmpeg/etc.)?"""
    for call in mock_run.call_args_list:
        cmd = call[0][0] if call[0] else call[1].get("args", [])
        if any("import_one.py" in str(arg) or arg == "--force" for arg in cmd):
            return True
    return False


def _analyze_result(grade: str, bitrate: int | None, suspect_pct: float = 0.0,
                    cliff_count: int = 0):
    """Build a SimpleNamespace mimicking spectral_check.analyze_album's return."""
    tracks = [SimpleNamespace(cliff_detected=True) for _ in range(cliff_count)]
    return SimpleNamespace(
        grade=grade,
        estimated_bitrate_kbps=bitrate,
        suspect_pct=suspect_pct,
        tracks=tracks,
    )


class TestForceImportRunsSpectralGate(unittest.TestCase):
    """Force-import must run the spectral gate — not just skip beets distance.

    The live bug: album 903 had existing ~96kbps spectral on disk. A teckdevaz
    download with ~96kbps spectral was force-imported and replaced the existing
    copy, because dispatch_import_from_db skipped the spectral gate that the
    auto path runs in lib/download.py._apply_spectral_decision.
    """

    def _run_force_import(self, *, download_spectral_grade: str,
                          download_spectral_bitrate: int | None,
                          existing_spectral_bitrate: int | None,
                          existing_min_bitrate: int,
                          download_bitrate: int = 320,
                          is_cbr: bool = True):
        """Common fixture: spin up force-import with controlled spectral outputs."""
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=903,
            status="manual",
            mb_release_id="mbid-luce",
            min_bitrate=existing_min_bitrate,
            current_spectral_bitrate=existing_spectral_bitrate,
            current_spectral_grade=(
                "likely_transcode" if existing_spectral_bitrate else None),
        ))

        ir = make_import_result(
            decision="import",
            new_min_bitrate=download_bitrate,
            prev_min_bitrate=existing_min_bitrate,
        )
        stdout = _make_stdout(ir)
        beets_info = AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=existing_min_bitrate,
            avg_bitrate_kbps=existing_min_bitrate,
            format="MP3", is_cbr=is_cbr,
            album_path="/Beets/Luce",
        )

        cfg = SoularrConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            audio_check_mode="normal",
        )

        # Build a file so audio_check scans something (real path that exists).
        tmpdir = tempfile.mkdtemp()
        import os
        with open(os.path.join(tmpdir, "01 - track.mp3"), "wb") as f:
            f.write(b"fake mp3 content")

        # Patch inspect_local_files so tests don't depend on mutagen reading
        # fake-byte MP3 files. Real files would be real CBR/VBR; tests
        # simulate whatever the scenario requires.
        from lib.preimport import LocalFileInspection
        inspection_result = LocalFileInspection(
            filetype="mp3",
            min_bitrate_bps=download_bitrate * 1000,
            is_vbr=not is_cbr,
        )
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg), \
                 patch("lib.import_dispatch.inspect_local_files",
                       return_value=inspection_result), \
                 patch(
                     "lib.preimport.spectral_analyze",
                     side_effect=[
                         _analyze_result(
                             grade=download_spectral_grade,
                             bitrate=download_spectral_bitrate,
                             suspect_pct=80.0 if download_spectral_grade
                             in ("suspect", "likely_transcode") else 0.0,
                             cliff_count=5 if download_spectral_grade
                             in ("suspect", "likely_transcode") else 0,
                         ),
                         _analyze_result(
                             grade=(
                                 "likely_transcode" if existing_spectral_bitrate
                                 else "genuine"),
                             bitrate=existing_spectral_bitrate,
                             suspect_pct=80.0 if existing_spectral_bitrate
                             else 0.0,
                         ),
                     ],
                 ), \
                 patch(
                     "lib.preimport.validate_audio",
                     return_value=SimpleNamespace(
                         valid=True, error=None, failed_files=[]),
                 ), \
                 patch("os.path.isdir", return_value=True):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                result = dispatch_import_from_db(
                    db, request_id=903, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="teckdevaz",
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
        return db, result, ext

    def test_force_import_rejects_transcode_equal_to_existing(self):
        """Force-import a 96kbps transcode over existing 96kbps transcode → REJECT.

        This reproduces the live bug from album 903. Pre-fix: dispatch_import_core
        runs, container bitrate (128 vs 96) says "better", import succeeds.
        Post-fix: spectral gate rejects (96 <= 96) before import_one.py runs.
        """
        db, result, ext = self._run_force_import(
            download_spectral_grade="likely_transcode",
            download_spectral_bitrate=96,
            existing_spectral_bitrate=96,
            existing_min_bitrate=96,
            download_bitrate=128,
        )

        self.assertFalse(
            result.success,
            "force-import of equivalent spectral transcode must be rejected")
        self.assertFalse(
            _import_one_called(ext.run),
            "import_one.py must NOT run after spectral rejection")
        row = db.request(903)
        self.assertNotEqual(
            row["status"], "imported",
            "request must not flip to 'imported' on spectral rejection")
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(self, 0, beets_scenario="spectral_reject")

    def test_force_import_denylists_user_on_spectral_reject(self):
        """Spectral rejection on force-import must denylist the source user
        with a spectral-scoped reason.

        The auto path writes `reason="spectral: Xkbps <= existing Ykbps"` in
        _apply_spectral_decision. Force-import must match — today the user
        gets denylisted only after the file is imported, via the post-import
        quality gate, with a different reason.
        """
        db, _, _ = self._run_force_import(
            download_spectral_grade="likely_transcode",
            download_spectral_bitrate=96,
            existing_spectral_bitrate=96,
            existing_min_bitrate=96,
            download_bitrate=128,
        )

        self.assertEqual(
            len(db.denylist), 1,
            "user who supplied the transcode must be denylisted exactly once")
        entry = db.denylist[0]
        self.assertEqual(entry.username, "teckdevaz")
        self.assertEqual(entry.request_id, 903)
        self.assertIn(
            "spectral", (entry.reason or "").lower(),
            f"reason must identify spectral as the cause — got {entry.reason!r}")

    def test_force_import_allows_genuine_spectral(self):
        """Pre-import gates must NOT over-reject: genuine spectral still imports.

        Guard against the fix becoming too aggressive — a force-import of a
        genuine file must still make it through.
        """
        db, result, ext = self._run_force_import(
            download_spectral_grade="genuine",
            download_spectral_bitrate=None,
            existing_spectral_bitrate=96,
            existing_min_bitrate=96,
            download_bitrate=245,
        )

        self.assertTrue(result.success)
        self.assertTrue(
            _import_one_called(ext.run),
            "import_one.py must run when spectral gate passes")
        db.assert_log(self, 0, outcome="force_import")


class TestForceImportRunsAudioCheck(unittest.TestCase):
    """Force-import must run the audio corruption check.

    The auto path rejects corrupt audio via validate_audio in
    _process_beets_validation. Force-import currently skips this entirely,
    so a corrupt MP3 can be force-imported into beets.
    """

    def test_force_import_rejects_corrupt_audio(self):
        """validate_audio returns invalid → force-import must not call import_one.py."""
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual", mb_release_id="mbid-123",
        ))

        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3", is_cbr=True,
            album_path="/Beets/Test")
        cfg = SoularrConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            audio_check_mode="normal",
        )

        tmpdir = tempfile.mkdtemp()
        import os
        with open(os.path.join(tmpdir, "01 - track.mp3"), "wb") as f:
            f.write(b"corrupt mp3 bytes")

        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg), \
                 patch(
                     "lib.preimport.validate_audio",
                     return_value=SimpleNamespace(
                         valid=False,
                         error="ffmpeg decode failed: Header missing",
                         failed_files=[("01 - track.mp3",
                                        "ffmpeg decode failed")]),
                 ), \
                 patch("lib.preimport.spectral_analyze",
                       return_value=_analyze_result("genuine", None)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout="", stderr="")
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="user1",
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        self.assertFalse(
            result.success,
            "force-import of corrupt audio must fail")
        self.assertFalse(
            _import_one_called(ext.run),
            "import_one.py must NOT run when audio check fails")
        row = db.request(42)
        self.assertNotEqual(row["status"], "imported")
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(self, 0, beets_scenario="audio_corrupt")


class TestForceImportStillSkipsBeetsDistance(unittest.TestCase):
    """Regression guard: --force must still bypass the beets distance check.

    Force exists precisely to accept imports that beets would reject on
    distance. The fix must preserve this while adding the spectral/audio gates.
    """

    def test_force_flag_still_passed_to_import_one(self):
        """When all pre-import gates pass, --force is still forwarded."""
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual", mb_release_id="mbid-123",
        ))

        ir = make_import_result(decision="import", new_min_bitrate=320)
        stdout = _make_stdout(ir)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3", is_cbr=True,
            album_path="/Beets/Test")
        cfg = SoularrConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            audio_check_mode="normal",
        )

        tmpdir = tempfile.mkdtemp()
        import os
        with open(os.path.join(tmpdir, "01 - track.mp3"), "wb") as f:
            f.write(b"ok mp3")

        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg), \
                 patch("lib.preimport.validate_audio",
                       return_value=SimpleNamespace(
                           valid=True, error=None, failed_files=[])), \
                 patch("lib.preimport.spectral_analyze",
                       return_value=_analyze_result("genuine", None)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="user1",
                )

                cmd = ext.run.call_args[0][0]
                self.assertIn(
                    "--force", cmd,
                    "--force must still be passed to import_one.py")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestInspectLocalFilesRecursive(unittest.TestCase):
    """inspect_local_files() must walk subdirectories so multi-disc layouts
    (``Album/CD1/*.mp3``) classify correctly — otherwise the spectral gate
    silently skips nested manual/force imports.
    """

    def test_multi_disc_layout_detects_mp3(self):
        """Audio files under a subdirectory must be discovered."""
        import os
        from lib.preimport import inspect_local_files

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            os.makedirs(cd1)
            with open(os.path.join(cd1, "01 - track.mp3"), "wb") as f:
                f.write(b"fake")
            inspection = inspect_local_files(tmpdir)
            self.assertIn("mp3", inspection.filetype,
                          "subdirectory MP3 must be discovered")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_validate_audio_recurses_into_subdirs(self):
        """validate_audio must walk subdirectories so nested discs are decoded.

        Auto path always passes flat folders, but force/manual-import can point
        at user folders with ``Album/CD1/*.mp3``. If validate_audio only lists
        the root, no nested file is decoded and corrupt audio silently passes.
        """
        import os
        from lib.util import validate_audio

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            os.makedirs(cd1)
            with open(os.path.join(cd1, "01.mp3"), "wb") as f:
                f.write(b"bad mp3 bytes")
            result = validate_audio(tmpdir, "normal")
            self.assertFalse(
                result.valid,
                "nested corrupt MP3 must trigger audio rejection")
            self.assertTrue(
                any("01.mp3" in name for name, _ in result.failed_files),
                f"failed_files must include the nested file, got {result.failed_files}")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_analyze_album_recurses_into_subdirs(self):
        """analyze_album must walk subdirectories so nested discs are analyzed.

        Without recursion, a multi-disc folder returns an empty result that
        looks like 'genuine' (no tracks = no cliffs), and the spectral gate
        silently passes a potential transcode on force/manual-import.
        """
        import os
        from unittest.mock import patch
        from lib.spectral_check import analyze_album

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            os.makedirs(cd1)
            with open(os.path.join(cd1, "01.mp3"), "wb") as f:
                f.write(b"fake")
            with patch("lib.spectral_check.analyze_track") as mock_track:
                mock_track.return_value = SimpleNamespace(
                    grade="suspect", error=None,
                    estimated_bitrate_kbps=128,
                    cliff_detected=True, cliff_freq_hz=12000,
                )
                _ = analyze_album(tmpdir)
            self.assertEqual(
                mock_track.call_count, 1,
                "analyze_album must reach the nested file (call_count=0 means "
                "it only listed the root)")
            called_path = mock_track.call_args[0][0]
            self.assertIn("CD1", called_path,
                          "analyze_album must call analyze_track with the nested path")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestForceImportSplitsMultiUserSources(unittest.TestCase):
    """download_log.soulseek_username can be a comma-joined list
    (``"disc1user, disc2user"``) when the download pulled from multiple peers.
    The preimport denylist must block each real peer, not the literal string.
    """

    def test_comma_separated_usernames_split_before_denylist(self):
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=903, status="manual", mb_release_id="mbid-luce",
            min_bitrate=96, current_spectral_bitrate=96,
            current_spectral_grade="likely_transcode",
        ))
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=96,
            avg_bitrate_kbps=96, format="MP3", is_cbr=True,
            album_path="/Beets/Luce")
        cfg = SoularrConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True, audio_check_mode="normal",
        )
        tmpdir = tempfile.mkdtemp()
        import os
        with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
            f.write(b"x")

        from lib.preimport import LocalFileInspection
        inspection = LocalFileInspection(
            filetype="mp3", min_bitrate_bps=320_000, is_vbr=False)

        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg), \
                 patch("lib.import_dispatch.inspect_local_files",
                       return_value=inspection), \
                 patch("lib.preimport.validate_audio",
                       return_value=SimpleNamespace(
                           valid=True, error=None, failed_files=[])), \
                 patch("lib.preimport.spectral_analyze",
                       side_effect=[
                           _analyze_result("likely_transcode", 96, 80.0, 5),
                           _analyze_result("likely_transcode", 96, 80.0, 5),
                       ]), \
                 patch("os.path.isdir", return_value=True):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout="", stderr="")
                dispatch_import_from_db(
                    db, request_id=903, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True,
                    source_username="disc1user, disc2user",
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        usernames = {e.username for e in db.denylist}
        self.assertIn("disc1user", usernames)
        self.assertIn("disc2user", usernames)
        self.assertNotIn("disc1user, disc2user", usernames,
                         "must not denylist the literal combined string")


class TestPreimportRejectionPreservesCorruptFiles(unittest.TestCase):
    """Audio-corrupt rejection in the preimport path must preserve the list of
    corrupt files in ``download_log.validation_result`` for debuggability — the
    auto path preserves this, force/manual must match.
    """

    def test_corrupt_files_land_in_validation_result_jsonb(self):
        import json
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual", mb_release_id="mbid-123",
        ))
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3", is_cbr=True,
            album_path="/Beets/Test")
        cfg = SoularrConfig(
            beets_harness_path=_HARNESS, pipeline_db_enabled=True,
            audio_check_mode="normal",
        )
        tmpdir = tempfile.mkdtemp()
        import os
        with open(os.path.join(tmpdir, "01 - track.mp3"), "wb") as f:
            f.write(b"bad")

        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg), \
                 patch("lib.preimport.validate_audio",
                       return_value=SimpleNamespace(
                           valid=False,
                           error="ffmpeg decode failed",
                           failed_files=[
                               ("01 - track.mp3", "ffmpeg decode failed"),
                               ("02 - track.mp3", "Header missing"),
                           ])), \
                 patch("lib.preimport.spectral_analyze",
                       return_value=_analyze_result("genuine", None)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout="", stderr="")
                dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="user1",
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        self.assertEqual(len(db.download_logs), 1)
        vr = json.loads(db.download_logs[0].validation_result or "{}")
        self.assertEqual(vr.get("scenario"), "audio_corrupt")
        self.assertIn("01 - track.mp3", vr.get("corrupt_files", []))
        self.assertIn("02 - track.mp3", vr.get("corrupt_files", []))


class TestForceImportDoesNotCorruptSpectralStateOnFailure(unittest.TestCase):
    """Force/manual import must NOT propagate the download's spectral into
    on-disk state speculatively: if ``dispatch_import_core`` later fails
    (downgrade, no JSON, timeout), the DB would otherwise be left claiming
    the failed download is on-disk, skewing later override/gate decisions.

    Only the MEASURED existing spectral (from beets) is persisted during the
    preimport gate. The propagation shortcut is reserved for the auto path.
    """

    def test_propagation_skipped_when_existing_unmeasured(self):
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual", mb_release_id="mbid-123",
            min_bitrate=192,                   # something IS on disk
            current_spectral_grade=None,       # but no measured spectral yet
            current_spectral_bitrate=None,
        ))

        ir = make_import_result(decision="import", new_min_bitrate=320)
        stdout = _make_stdout(ir)
        # Existing album exists but album_path is not on disk — beets returns
        # info without a walkable path, so no EXISTING spectral is measured.
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=192,
            avg_bitrate_kbps=192, format="MP3", is_cbr=True,
            album_path="/Beets/NonexistentPath")
        cfg = SoularrConfig(
            beets_harness_path=_HARNESS, pipeline_db_enabled=True,
            audio_check_mode="normal",
        )
        tmpdir = tempfile.mkdtemp()
        import os
        with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
            f.write(b"x")

        from lib.preimport import LocalFileInspection
        inspection = LocalFileInspection(
            filetype="mp3", min_bitrate_bps=320_000, is_vbr=False)

        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg), \
                 patch("lib.import_dispatch.inspect_local_files",
                       return_value=inspection), \
                 patch("lib.preimport.validate_audio",
                       return_value=SimpleNamespace(
                           valid=True, error=None, failed_files=[])), \
                 patch("lib.preimport.spectral_analyze",
                       return_value=_analyze_result(
                           "likely_transcode", 96, 80.0, 5)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="user1",
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        row = db.request(42)
        # On the force/manual path the download's spectral must NOT be written
        # as "current" even though min_bitrate is set. Existing spectral
        # stays None because /Beets/NonexistentPath isn't walkable.
        self.assertIsNone(
            row["current_spectral_grade"],
            "force-import preimport must not speculatively propagate download spectral")
        self.assertIsNone(row["current_spectral_bitrate"])


class TestAutoPathPreservesSpectralPropagation(unittest.TestCase):
    """The auto path still propagates: run_preimport_gates with
    propagate_download_to_existing=True (the default) adopts the download's
    spectral as current when min_bitrate is set but spectral is unmeasured.
    """

    def test_auto_path_propagates_download_spectral(self):
        from lib.config import SoularrConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, min_bitrate=256,
            current_spectral_grade=None, current_spectral_bitrate=None,
        ))
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=256,
            avg_bitrate_kbps=256, format="MP3", is_cbr=True,
            album_path="/Beets/NonexistentPath")
        cfg = SoularrConfig(audio_check_mode="off")

        with patch("lib.preimport.spectral_analyze",
                   return_value=_analyze_result("suspect", 192, 80.0, 5)), \
             patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
            run_preimport_gates(
                path="/tmp/dl",
                mb_release_id="mbid-123",
                label="Test",
                download_filetype="mp3",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=1,
                usernames=set(),
                # Default is True — auto path preserves propagation.
            )

        row = db.request(1)
        self.assertEqual(
            row["current_spectral_grade"], "suspect",
            "auto path must propagate download spectral when existing unmeasured")
        self.assertEqual(row["current_spectral_bitrate"], 192)


class TestRepairMp3HeadersRecurses(unittest.TestCase):
    """repair_mp3_headers must walk subdirectories — otherwise nested MP3s
    with fixable header issues reach ffmpeg unrepaired and falsely reject.
    """

    def test_mp3val_called_on_nested_file(self):
        import os
        from unittest.mock import patch, MagicMock
        from lib.util import repair_mp3_headers

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            os.makedirs(cd1)
            nested = os.path.join(cd1, "01.mp3")
            with open(nested, "wb") as f:
                f.write(b"fake")
            with patch("lib.util.sp.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="")
                repair_mp3_headers(tmpdir)
            called_paths = [c[0][0][-1] for c in mock_run.call_args_list]
            self.assertTrue(
                any(nested == p for p in called_paths),
                f"mp3val must be called on nested {nested}, got {called_paths}")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestAudioFailuresPreserveSubdirContext(unittest.TestCase):
    """When validate_audio walks subdirectories, the failed-file list must
    record the path relative to the audit root so multi-disc layouts don't
    collapse ``CD1/01.mp3`` and ``CD2/01.mp3`` into the same entry.
    """

    def test_nested_failures_keep_subdir_in_name(self):
        import os
        from unittest.mock import patch
        from lib.util import validate_audio

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            cd2 = os.path.join(tmpdir, "CD2")
            os.makedirs(cd1)
            os.makedirs(cd2)
            with open(os.path.join(cd1, "01.mp3"), "wb") as f:
                f.write(b"x")
            with open(os.path.join(cd2, "01.mp3"), "wb") as f:
                f.write(b"x")
            # Both files fail
            with patch("lib.util.sp.run") as mock_run:
                from unittest.mock import MagicMock
                mock_run.return_value = MagicMock(
                    returncode=1, stderr="Invalid data")
                result = validate_audio(tmpdir, "normal")
            names = [name for name, _err in result.failed_files]
            self.assertIn("CD1/01.mp3", names,
                          f"CD1 path must survive in failed_files, got {names}")
            self.assertIn("CD2/01.mp3", names,
                          f"CD2 path must survive in failed_files, got {names}")


        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestForceImportRepairsBeforeInspection(unittest.TestCase):
    """Broken MP3 headers can prevent mutagen from reading bitrate_mode,
    leaving download_is_vbr=None. The spectral gate then treats the folder
    as CBR and can spectrally reject a VBR album that the auto path would
    have skipped. ``dispatch_import_from_db`` must repair headers before
    inspect_local_files so that VBR detection is accurate.
    """

    def test_repair_runs_before_inspect(self):
        import os
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual", mb_release_id="mbid-123"))
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3", is_cbr=True,
            album_path="/Beets/Test")
        cfg = SoularrConfig(
            beets_harness_path=_HARNESS, pipeline_db_enabled=True,
            audio_check_mode="normal")

        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
            f.write(b"x")

        try:
            call_order: list[str] = []
            original_repair = __import__(
                "lib.util", fromlist=["repair_mp3_headers"]).repair_mp3_headers
            original_inspect = __import__(
                "lib.preimport", fromlist=["inspect_local_files"]).inspect_local_files

            def tracking_repair(p):
                call_order.append("repair")
                return original_repair(p)

            def tracking_inspect(p):
                call_order.append("inspect")
                return original_inspect(p)

            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg), \
                 patch("lib.import_dispatch.repair_mp3_headers",
                       side_effect=tracking_repair), \
                 patch("lib.import_dispatch.inspect_local_files",
                       side_effect=tracking_inspect), \
                 patch("lib.preimport.validate_audio",
                       return_value=SimpleNamespace(
                           valid=True, error=None, failed_files=[])), \
                 patch("lib.preimport.spectral_analyze",
                       return_value=_analyze_result("genuine", None)):
                ext.run.return_value = MagicMock(
                    returncode=0,
                    stdout=_make_stdout(make_import_result(
                        decision="import", new_min_bitrate=320)),
                    stderr="")
                dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="user1")

            self.assertIn("repair", call_order,
                          "repair_mp3_headers must run on force-import path")
            self.assertIn("inspect", call_order,
                          "inspect_local_files must run on force-import path")
            self.assertLess(
                call_order.index("repair"),
                call_order.index("inspect"),
                "repair_mp3_headers must run BEFORE inspect_local_files so "
                "mutagen can read the repaired bitrate_mode")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestPreimportFallsBackToPersistedSpectral(unittest.TestCase):
    """When BeetsDB can't walk the on-disk album_path (stale/missing), the
    gate must fall back to the spectral state already stored on
    album_requests. Otherwise spectral_import_decision compares against
    existing_min_bitrate (container) and can reject a genuine upgrade —
    e.g. 192kbps transcode rejected as <= 320 even though
    current_spectral_bitrate says the on-disk copy is only 128kbps.
    """

    def test_stored_spectral_used_when_beets_lookup_empty(self):
        from lib.config import SoularrConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        # Request row has stored spectral: on-disk is actually a 128 transcode,
        # even though beets reports 320 as the container min_bitrate.
        db.seed_request(make_request_row(
            id=42,
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=128,
        ))
        # Beets knows the album exists at 320 but its album_path is not on
        # disk, so _analyze_existing returns (320, None) — no measured spectral.
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3", is_cbr=True,
            album_path="/Beets/NonexistentPath")
        cfg = SoularrConfig(audio_check_mode="off")

        with patch("lib.preimport.spectral_analyze",
                   return_value=_analyze_result(
                       "likely_transcode", 192, 80.0, 5)), \
             patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
            result = run_preimport_gates(
                path="/tmp/dl",
                mb_release_id="mbid-123",
                label="Test",
                download_filetype="mp3",
                download_min_bitrate_bps=192_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
                usernames=set(),
                propagate_download_to_existing=False,
            )

        # With stored spectral fallback, the decision compares
        # new_spectral=192kbps vs stored_existing_spectral=128kbps → import.
        # Without the fallback it compares 192 vs min_bitrate=320 → reject.
        self.assertTrue(
            result.valid,
            "192kbps upgrade over 128kbps-spectral existing must not be rejected")


class TestPreimportRepairsEvenWhenAudioCheckOff(unittest.TestCase):
    """MP3 header repair must run regardless of audio_check_mode — installs
    that disable ffmpeg validation still rely on mp3val to fix fixable
    header issues before spectral analysis and the import subprocess.
    Matches the pre-refactor auto-path behavior.
    """

    def test_repair_runs_with_audio_check_off(self):
        import os
        from unittest.mock import patch
        from lib.config import SoularrConfig
        from lib.preimport import run_preimport_gates

        cfg = SoularrConfig(audio_check_mode="off")
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
                f.write(b"x")
            with patch("lib.preimport.repair_mp3_headers") as mock_repair, \
                 patch("lib.preimport.spectral_analyze",
                       return_value=_analyze_result("genuine", None)):
                run_preimport_gates(
                    path=tmpdir,
                    mb_release_id="",  # skip existing lookup
                    label="Test",
                    download_filetype="mp3",
                    download_min_bitrate_bps=None,
                    download_is_vbr=None,
                    cfg=cfg,
                    usernames=set(),
                )
            self.assertEqual(
                mock_repair.call_count, 1,
                "repair_mp3_headers must run even with audio_check_mode=off")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestFallbackIgnoresNonTranscodeStoredSpectral(unittest.TestCase):
    """The persisted-spectral fallback must be grade-aware: a stored
    ``current_spectral_grade='genuine', current_spectral_bitrate=96`` is
    stale (genuine files have no cliff). Feeding that 96 kbps into
    spectral_import_decision would let real transcodes be imported as
    "upgrades". Matches compute_effective_override_bitrate and
    load_quality_gate_state — only transcode grades are authoritative.
    """

    def test_genuine_stored_spectral_ignored(self):
        from lib.config import SoularrConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        # Stored grade=genuine, bitrate=96 — a stale leftover from prior runs.
        # Not a transcode grade, so the bitrate must NOT be used as authoritative.
        db.seed_request(make_request_row(
            id=42,
            min_bitrate=320,
            current_spectral_grade="genuine",
            current_spectral_bitrate=96,
        ))
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3", is_cbr=True,
            album_path="/Beets/NonexistentPath")
        cfg = SoularrConfig(audio_check_mode="off")

        # Download is a suspect 192kbps transcode. If the fallback used the
        # stored grade=genuine/bitrate=96 verbatim, 192 > 96 would wrongly
        # import the transcode. With grade-aware handling, the 96 is ignored
        # and decision falls back to min_bitrate=320 → reject.
        with patch("lib.preimport.spectral_analyze",
                   return_value=_analyze_result(
                       "likely_transcode", 192, 80.0, 5)), \
             patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
            result = run_preimport_gates(
                path="/tmp/dl",
                mb_release_id="mbid-123",
                label="Test",
                download_filetype="mp3",
                download_min_bitrate_bps=192_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
                usernames=set(),
                propagate_download_to_existing=False,
            )

        self.assertFalse(
            result.valid,
            "stale genuine stored spectral must not be used to import a 192kbps "
            "transcode over a 320kbps on-disk album")
        self.assertEqual(result.scenario, "spectral_reject")


class TestUnknownVbrSkipsSpectralGate(unittest.TestCase):
    """When VBR status is unknown (mutagen couldn't read bitrate_mode), the
    spectral gate must be skipped. Treating unknown as confirmed CBR would
    falsely reject VBR uploads with damaged headers on the force/manual path.
    The auto path gets is_vbr from slskd metadata (usually reliable), so the
    common case still runs; this only affects the unknown-mode edge case.
    """

    def test_is_vbr_none_skips_spectral(self):
        from lib.config import SoularrConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        cfg = SoularrConfig(audio_check_mode="off")

        with patch("lib.preimport.spectral_analyze") as mock_spectral:
            result = run_preimport_gates(
                path="/tmp/dl",
                mb_release_id="",
                label="Test",
                download_filetype="mp3",
                download_min_bitrate_bps=None,
                download_is_vbr=None,   # mutagen couldn't determine
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=1,
                usernames=set(),
            )

        self.assertTrue(result.valid,
                        "unknown VBR status must not reject at the gate")
        self.assertEqual(
            mock_spectral.call_count, 0,
            "spectral_analyze must NOT run when VBR status is unknown")


if __name__ == "__main__":
    unittest.main()
