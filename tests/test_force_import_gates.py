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

        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg), \
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

        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg), \
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


if __name__ == "__main__":
    unittest.main()
