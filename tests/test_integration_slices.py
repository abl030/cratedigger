"""Integration slice tests — real code paths with minimal patching.

These exercise real orchestration flows end-to-end, patching only external
edges: subprocess (sp.run), filesystem cleanup, network calls (meelo/plex),
and BeetsDB (requires real beets SQLite DB on disk).

The key difference from unit/orchestration tests is that parse_import_result
and _check_quality_gate_core run for real, not patched.
"""

import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from lib.beets_db import AlbumInfo
from lib.config import SoularrConfig
from lib.quality import (
    IMPORT_RESULT_SENTINEL,
    QUALITY_LOSSLESS,
    QUALITY_UPGRADE_TIERS,
    DownloadInfo,
    ImportResult,
)
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_ctx_with_fake_db,
    make_download_file,
    make_grab_list_entry,
    make_import_result,
    make_request_row,
    make_validation_result,
    patch_dispatch_externals,
)


_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"


def _make_stdout(ir: ImportResult) -> str:
    """Build subprocess stdout containing the import result sentinel line."""
    return f"some log output\n{IMPORT_RESULT_SENTINEL}{ir.to_json()}\n"


def _mock_beets_db(beets_info):
    """Configure a mocked BeetsDB context manager returning beets_info."""
    mock_beets_instance = MagicMock()
    mock_beets_instance.get_album_info.return_value = beets_info
    mock_cls = MagicMock()
    mock_cls.return_value.__enter__ = MagicMock(return_value=mock_beets_instance)
    mock_cls.return_value.__exit__ = MagicMock(return_value=False)
    return mock_cls


class TestDispatchThroughQualityGate(unittest.TestCase):
    """Integration slice: dispatch_import_core → real parse_import_result
    → real _check_quality_gate_core → domain state assertions.

    Patches only: sp.run, cleanup, meelo/plex, BeetsDB.
    Runs for real: parse_import_result, dispatch_action, _do_mark_done,
    _check_quality_gate_core, quality_gate_decision, apply_transition.
    """

    def _run_dispatch(self, ir, beets_info, request_overrides=None):
        """Dispatch an import and return the FakePipelineDB state."""
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            **(request_overrides or {}),
        ))

        cfg = SoularrConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        dl_info = DownloadInfo(username="user1")
        stdout = _make_stdout(ir)

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=cfg,
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        return db

    def test_import_quality_accept(self):
        """VBR 245kbps → quality gate accepts → imported, override cleared."""
        ir = make_import_result(decision="import", new_min_bitrate=245)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["search_filetype_override"])
        self.assertEqual(row["min_bitrate"], 245)
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(self, 0, outcome="success", request_id=42)

    def test_import_quality_requeue_upgrade(self):
        """VBR 180kbps → quality gate requeues for upgrade → wanted, denylist."""
        ir = make_import_result(decision="import", new_min_bitrate=180)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=180,
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)
        self.assertEqual(row["min_bitrate"], 180)
        # Source denylisted for low quality
        self.assertEqual(len(db.denylist), 1)
        self.assertEqual(db.denylist[0].username, "user1")
        self.assertIn("quality gate", db.denylist[0].reason or "")

    def test_import_quality_requeue_lossless(self):
        """CBR 320 → quality gate requeues for lossless → wanted, lossless override."""
        ir = make_import_result(decision="import", new_min_bitrate=320)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            is_cbr=True, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_LOSSLESS)


    def test_transcode_upgrade_requeues_with_denylist(self):
        """Transcode upgrade → mark_done + requeue to upgrade tiers + denylist."""
        ir = make_import_result(decision="transcode_upgrade", new_min_bitrate=227)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=227,
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        # Transcode upgrade requeues directly (no quality gate)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)
        # Transcode source denylisted
        self.assertTrue(len(db.denylist) >= 1)
        db.assert_log(self, 0, outcome="success")

    def test_downgrade_prevented(self):
        """Downgrade → mark_failed + denylist, no quality gate."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=320)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=128,
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertTrue(len(db.denylist) >= 1)
        db.assert_log(self, 0, outcome="rejected")


class TestQualityGateVerifiedLosslessBypass(unittest.TestCase):
    """Integration slice: quality gate stays imported for verified_lossless
    even when bitrate < 210."""

    def test_verified_lossless_low_bitrate_accepts(self):
        """207kbps VBR from verified FLAC → accepted despite < 210 threshold."""
        from lib.import_dispatch import _check_quality_gate_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported", verified_lossless=True))

        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=207,
            is_cbr=False, album_path="/Beets/Test")

        with patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
            _check_quality_gate_core(
                mb_id="mbid-123",
                label="Lo-Fi Album",
                request_id=42,
                files=[MagicMock(username="user1")],
                db=db,  # type: ignore[arg-type]
            )

        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertEqual(row["min_bitrate"], 207)
        self.assertEqual(len(db.denylist), 0)


class TestQualityGateSpectralOverride(unittest.TestCase):
    """Integration slice: quality gate uses spectral bitrate when it's lower
    than beets container bitrate."""

    def test_suspect_spectral_triggers_requeue(self):
        """Container 320kbps but spectral says 128kbps → requeue for upgrade."""
        from lib.import_dispatch import _check_quality_gate_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            current_spectral_grade="suspect",
            current_spectral_bitrate=128,
        ))

        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            is_cbr=False, album_path="/Beets/Test")

        with patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
            _check_quality_gate_core(
                mb_id="mbid-123",
                label="Fake 320 Album",
                request_id=42,
                files=[MagicMock(username="user1")],
                db=db,  # type: ignore[arg-type]
            )

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)
        self.assertEqual(len(db.denylist), 1)
        self.assertIn("spectral", db.denylist[0].reason or "")


class TestSpectralPropagationSlice(unittest.TestCase):
    """Integration slice: gather spectral context then apply real decision."""

    def test_suspect_download_updates_current_spectral_and_denylists(self):
        from lib.download import _apply_spectral_decision, _gather_spectral_context

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        ctx = make_ctx_with_fake_db(db)
        album = make_grab_list_entry(
            db_request_id=42,
            mb_release_id="mbid-123",
            files=[make_download_file(
                filename="user1\\Music\\01 - Track.mp3",
                file_dir="user1\\Music",
                username="user1",
                bitRate=320,
                isVariableBitRate=False,
            )],
            filetype="mp3",
        )
        bv_result = make_validation_result()
        beets_info = AlbumInfo(
            album_id=1,
            track_count=10,
            min_bitrate_kbps=320,
            is_cbr=True,
            album_path="/Beets/Test",
        )

        with patch(
            "lib.download.spectral_analyze",
            side_effect=[
                SimpleNamespace(
                    grade="suspect",
                    estimated_bitrate_kbps=128,
                    suspect_pct=90.0,
                ),
                SimpleNamespace(
                    grade="genuine",
                    estimated_bitrate_kbps=320,
                    suspect_pct=0.0,
                ),
            ],
        ), patch("lib.download.BeetsDB", _mock_beets_db(beets_info)), \
             patch("os.path.isdir", return_value=True):
            spec_ctx = _gather_spectral_context(album, "/tmp/download", ctx)
            with self.assertLogs("soularr", level="WARNING") as logs:
                _apply_spectral_decision(
                    album,
                    bv_result,
                    spec_ctx,
                    "/tmp/download",
                    ctx,
                )

        row = db.request(42)
        self.assertIn("SPECTRAL REJECT", "\n".join(logs.output))
        self.assertEqual(row["current_spectral_grade"], "genuine")
        self.assertEqual(row["current_spectral_bitrate"], 320)
        self.assertFalse(bv_result.valid)
        self.assertEqual(bv_result.scenario, "spectral_reject")
        self.assertEqual(len(db.denylist), 1)
        self.assertEqual(db.denylist[0].username, "user1")
        self.assertIn("spectral: 128kbps <= existing 320kbps",
                      db.denylist[0].reason or "")


class TestDispatchNoJsonResult(unittest.TestCase):
    """Integration slice: sp.run returns no sentinel → mark_failed."""

    def test_no_json_marks_failed_and_requeues(self):
        """No __IMPORT_RESULT__ in stdout → scenario=no_json_result, requeue."""
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        cfg = SoularrConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(None)):
                ext.run.return_value = MagicMock(
                    returncode=1, stdout="some error\n", stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    cfg=cfg,
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(self, 0, outcome="failed")


class TestForceImportSlice(unittest.TestCase):
    """Integration slice: dispatch_import_from_db with force=True."""

    def test_force_import_success(self):
        """Force-import → imported, download_log outcome=force_import."""
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual", mb_release_id="mbid-123",
            min_bitrate=180, current_spectral_bitrate=128,
        ))

        ir = make_import_result(decision="import", new_min_bitrate=320)
        stdout = _make_stdout(ir)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            is_cbr=False, album_path="/Beets/Test")

        cfg = SoularrConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.import_dispatch._read_runtime_config",
                       return_value=cfg):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="user1",
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        self.assertTrue(result.success)
        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        db.assert_log(self, 0, outcome="force_import")


if __name__ == "__main__":
    unittest.main()
