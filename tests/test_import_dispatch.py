"""Tests for lib/import_dispatch.py — auto-import decision tree.

Tests each branch of dispatch_import() with mocked dependencies.
"""

import os
import shutil
import subprocess as sp
import tempfile
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

from lib.quality import (DownloadInfo, ImportResult, ConversionInfo,
                         AudioQualityMeasurement, PostflightInfo,
                         SpectralMeasurement,
                         QUALITY_UPGRADE_TIERS,
                         QualityIntent, intent_to_quality_override)
from tests.helpers import make_request_row


def _make_import_result(decision="import", new_min_bitrate=245,
                        prev_min_bitrate=None, was_converted=False,
                        original_filetype=None, target_filetype=None,
                        spectral_grade="genuine", spectral_bitrate=None,
                        error=None):
    """Build an ImportResult for testing."""
    return ImportResult(
        decision=decision,
        error=error,
        new_measurement=AudioQualityMeasurement(
            min_bitrate_kbps=new_min_bitrate,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            verified_lossless=was_converted and spectral_grade == "genuine",
            was_converted_from=original_filetype if was_converted else None,
        ),
        existing_measurement=(AudioQualityMeasurement(min_bitrate_kbps=prev_min_bitrate)
                              if prev_min_bitrate is not None else None),
        conversion=ConversionInfo(
            was_converted=was_converted,
            original_filetype=original_filetype or "",
            target_filetype=target_filetype or "",
        ),
        postflight=PostflightInfo(),
    )


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
    mock.files = [MagicMock(username="user1", filename="01 - Track.mp3")]
    return mock


def _make_ctx():
    """Build a mock SoularrContext."""
    ctx = MagicMock()
    ctx.cfg.beets_harness_path = "/nix/store/fake/harness/run_beets_harness.sh"
    ctx.cfg.beets_distance_threshold = 0.15
    ctx.pipeline_db_source = MagicMock()
    db_mock = MagicMock()
    db_mock.get_request.return_value = make_request_row(status="downloading")
    ctx.pipeline_db_source._get_db.return_value = db_mock
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


class TestPopulateDlInfoFromImportResult(unittest.TestCase):

    def test_converted_flac_to_v0(self):
        from lib.import_dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        ir = _make_import_result(was_converted=True, original_filetype="flac",
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
        from lib.import_dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="mp3")
        ir = _make_import_result(was_converted=False, new_min_bitrate=320)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertFalse(dl.was_converted)
        self.assertEqual(dl.slskd_filetype, "mp3")
        self.assertEqual(dl.actual_filetype, "mp3")


class TestCleanupStagedDir(unittest.TestCase):

    def test_removes_dir_and_empty_parent(self):
        from lib.import_dispatch import _cleanup_staged_dir
        tmpdir = tempfile.mkdtemp()
        try:
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
        from lib.import_dispatch import _cleanup_staged_dir
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


class TestDispatchImport(unittest.TestCase):
    """Test the import decision tree branches."""

    def _dispatch(self, ir_json, album_data=None, ctx=None, bv_result=None,
                  dest="/tmp/fake/dest"):
        from lib.import_dispatch import dispatch_import
        if album_data is None:
            album_data = _make_album_data()
        if ctx is None:
            ctx = _make_ctx()
        if bv_result is None:
            bv_result = _make_bv_result()
        dl_info = DownloadInfo(filetype="mp3")
        request_id = album_data.db_request_id

        with patch("lib.import_dispatch.sp.run") as mock_run, \
             patch("lib.import_dispatch._cleanup_staged_dir") as mock_cleanup, \
             patch("lib.import_dispatch.trigger_meelo_scan") as mock_meelo, \
             patch("lib.import_dispatch._check_quality_gate") as mock_gate, \
             patch("lib.import_dispatch.parse_import_result", return_value=ir_json):
            mock_run.return_value = MagicMock(
                returncode=0, stdout="", stderr="")
            dispatch_import(album_data, bv_result, dest, dl_info,
                            request_id, ctx)

        return {
            "mock_run": mock_run,
            "mock_cleanup": mock_cleanup,
            "mock_meelo": mock_meelo,
            "mock_gate": mock_gate,
            "pipeline_db_source": ctx.pipeline_db_source,
            "dl_info": dl_info,
        }

    def test_import_success(self):
        ir = _make_import_result(decision="import")
        result = self._dispatch(ir)
        result["pipeline_db_source"].mark_done.assert_called_once()
        result["mock_meelo"].assert_called_once()
        result["mock_cleanup"].assert_called_once()
        result["mock_gate"].assert_called_once()

    def test_preflight_existing(self):
        ir = _make_import_result(decision="preflight_existing")
        result = self._dispatch(ir)
        result["pipeline_db_source"].mark_done.assert_called_once()
        result["mock_meelo"].assert_called_once()

    def test_import_with_upgrade_delta(self):
        ir = _make_import_result(decision="import", new_min_bitrate=245,
                                 prev_min_bitrate=192)
        result = self._dispatch(ir)
        db = result["pipeline_db_source"]._get_db()
        db.update_status.assert_called()

    def test_downgrade_rejected(self):
        ir = _make_import_result(decision="downgrade", new_min_bitrate=192,
                                 prev_min_bitrate=320)
        result = self._dispatch(ir)
        result["pipeline_db_source"].mark_failed.assert_called_once()
        result["pipeline_db_source"].mark_done.assert_not_called()
        result["mock_cleanup"].assert_called_once()
        # Should denylist
        db = result["pipeline_db_source"]._get_db()
        db.add_denylist.assert_called()

    def test_downgrade_passes_narrowed_override_to_mark_failed(self):
        ir = _make_import_result(decision="downgrade", new_min_bitrate=320,
                                 prev_min_bitrate=320)
        ctx = _make_ctx()
        db = ctx.pipeline_db_source._get_db()
        db.get_request.return_value = make_request_row(
            status="downloading",
            quality_override="flac,mp3 v0,mp3 320",
        )

        result = self._dispatch(ir, ctx=ctx)

        call_kwargs = result["pipeline_db_source"].mark_failed.call_args.kwargs
        self.assertEqual(call_kwargs.get("quality_override"), "flac,mp3 v0")

    def test_downgrade_narrows_flac_preferred_before_mark_failed(self):
        ir = _make_import_result(decision="downgrade", new_min_bitrate=320,
                                 prev_min_bitrate=320)
        ctx = _make_ctx()
        db = ctx.pipeline_db_source._get_db()
        db.get_request.return_value = make_request_row(
            status="downloading",
            quality_override="flac_preferred",
        )

        result = self._dispatch(ir, ctx=ctx)

        call_kwargs = result["pipeline_db_source"].mark_failed.call_args.kwargs
        self.assertEqual(call_kwargs.get("quality_override"), "flac,mp3 v0")

    def test_transcode_upgrade(self):
        ir = _make_import_result(decision="transcode_upgrade",
                                 new_min_bitrate=227)
        result = self._dispatch(ir)
        result["pipeline_db_source"].mark_done.assert_called_once()
        result["mock_meelo"].assert_called_once()
        db = result["pipeline_db_source"]._get_db()
        db.add_denylist.assert_called()
        db.reset_to_wanted.assert_called_once()

    def test_transcode_downgrade(self):
        ir = _make_import_result(decision="transcode_downgrade",
                                 new_min_bitrate=190)
        result = self._dispatch(ir)
        result["pipeline_db_source"].mark_failed.assert_called_once()
        db = result["pipeline_db_source"]._get_db()
        db.add_denylist.assert_called()
        db.reset_to_wanted.assert_called_once()

    def test_error_decision(self):
        ir = _make_import_result(decision="conversion_failed",
                                 error="ffmpeg failed")
        result = self._dispatch(ir)
        result["pipeline_db_source"].mark_failed.assert_called_once()
        result["pipeline_db_source"].mark_done.assert_not_called()

    def test_no_json_result(self):
        """parse_import_result returns None when no JSON in output."""
        result = self._dispatch(None)  # None = no JSON parsed
        result["pipeline_db_source"].mark_failed.assert_called_once()

    def test_timeout(self):
        from lib.import_dispatch import dispatch_import
        album_data = _make_album_data()
        ctx = _make_ctx()
        bv_result = _make_bv_result()
        dl_info = DownloadInfo(filetype="mp3")

        with patch("lib.import_dispatch.sp.run",
                   side_effect=sp.TimeoutExpired(cmd="test", timeout=1800)), \
             patch("lib.import_dispatch._build_download_info",
                   return_value=DownloadInfo()):
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx)

        ctx.pipeline_db_source.mark_failed.assert_called_once()

    def test_exception(self):
        from lib.import_dispatch import dispatch_import
        album_data = _make_album_data()
        ctx = _make_ctx()
        bv_result = _make_bv_result()
        dl_info = DownloadInfo(filetype="mp3")

        with patch("lib.import_dispatch.sp.run",
                   side_effect=RuntimeError("boom")), \
             patch("lib.import_dispatch._build_download_info",
                   return_value=DownloadInfo()):
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx)

        ctx.pipeline_db_source.mark_failed.assert_called_once()


class TestOverrideMinBitrate(unittest.TestCase):
    """Test that --override-min-bitrate uses spectral when lower than container."""

    def _get_override_value(self, db_fields):
        """Run dispatch_import with a mock DB request, return the override passed."""
        from lib.import_dispatch import dispatch_import
        album_data = _make_album_data()
        ctx = _make_ctx()
        db_mock = ctx.pipeline_db_source._get_db.return_value
        db_mock.get_request.return_value = db_fields
        bv_result = _make_bv_result()
        dl_info = DownloadInfo(filetype="mp3")
        ir = _make_import_result(decision="import")

        with patch("lib.import_dispatch.sp.run") as mock_run, \
             patch("lib.import_dispatch._cleanup_staged_dir"), \
             patch("lib.import_dispatch.trigger_meelo_scan"), \
             patch("lib.import_dispatch._check_quality_gate"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            mock_run.return_value = MagicMock(
                returncode=0, stdout="", stderr="")
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx)
            cmd = mock_run.call_args[0][0]

        # Find --override-min-bitrate value in cmd
        for i, arg in enumerate(cmd):
            if arg == "--override-min-bitrate" and i + 1 < len(cmd):
                return int(cmd[i + 1])
        return None

    def test_uses_spectral_when_lower(self):
        """Container says 320, spectral says 128 — should pass 128."""
        val = self._get_override_value(
            make_request_row(min_bitrate=320, current_spectral_bitrate=128))
        self.assertEqual(val, 128)

    def test_uses_container_when_no_spectral(self):
        """No spectral data — should pass container bitrate."""
        val = self._get_override_value(
            make_request_row(min_bitrate=320, current_spectral_bitrate=None))
        self.assertEqual(val, 320)

    def test_uses_container_when_spectral_higher(self):
        """Spectral is higher than container — use container (more conservative)."""
        val = self._get_override_value(
            make_request_row(min_bitrate=192, current_spectral_bitrate=256))
        self.assertEqual(val, 192)

    def test_no_override_when_no_bitrate(self):
        """No min_bitrate and no spectral — no override passed."""
        val = self._get_override_value(
            make_request_row(min_bitrate=None, current_spectral_bitrate=None))
        self.assertIsNone(val)


class TestQualityGateUsesIntent(unittest.TestCase):
    """Verify _check_quality_gate uses intent_to_quality_override."""

    def _run_quality_gate(self, gate_decision, **extra_req_fields):
        """Run _check_quality_gate with a mocked quality_gate_decision."""
        from lib.import_dispatch import _check_quality_gate
        album_data = _make_album_data()
        ctx = _make_ctx()
        db = ctx.pipeline_db_source._get_db.return_value
        merged = {"current_spectral_bitrate": None, "verified_lossless": False}
        merged.update(extra_req_fields)
        db.get_request.return_value = make_request_row(**merged)

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls, \
             patch("lib.quality.quality_gate_decision",
                   return_value=gate_decision):
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = MagicMock(
                min_bitrate_kbps=192, is_cbr=True)
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate(album_data, 42, ctx)

        return db

    def test_requeue_upgrade_uses_intent(self):
        """requeue_upgrade should use intent_to_quality_override(upgrade)."""
        db = self._run_quality_gate("requeue_upgrade")
        call_args = db.reset_to_wanted.call_args
        self.assertEqual(
            call_args.kwargs.get("quality_override") or call_args[1].get("quality_override"),
            intent_to_quality_override(QualityIntent.upgrade),
        )

    def test_requeue_upgrade_verified_lossless_accepts(self):
        """verified_lossless=True should accept, not requeue, even on requeue_upgrade."""
        db = self._run_quality_gate("requeue_upgrade", verified_lossless=True)
        # Should NOT have called reset_to_wanted
        db.reset_to_wanted.assert_not_called()
        # Should NOT have denylisted anyone
        db.add_denylist.assert_not_called()

    def test_requeue_flac_uses_intent(self):
        """requeue_flac should use intent_to_quality_override(flac_only)."""
        db = self._run_quality_gate("requeue_flac")
        call_args = db.reset_to_wanted.call_args
        self.assertEqual(
            call_args.kwargs.get("quality_override") or call_args[1].get("quality_override"),
            intent_to_quality_override(QualityIntent.flac_only),
        )

    def test_quality_gate_reads_current_spectral_not_last_download(self):
        """Quality gate must use current_spectral_bitrate (what's on disk),
        not last_download_spectral_bitrate (stale from a previous download)."""
        from lib.import_dispatch import _check_quality_gate
        from lib.quality import AudioQualityMeasurement
        album_data = _make_album_data()
        ctx = _make_ctx()
        db = ctx.pipeline_db_source._get_db.return_value
        # current is None (genuine, no cliff) but last_download is stale 192
        db.get_request.return_value = make_request_row(
            last_download_spectral_bitrate=192,  # stale from old download
            current_spectral_bitrate=None,       # genuine files, cleared by mark_done
            verified_lossless=False)

        captured_measurement = {}

        def capture_decision(measurement):
            captured_measurement["m"] = measurement
            return "accept"  # would be accept with no spectral drag

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls, \
             patch("lib.quality.quality_gate_decision",
                   side_effect=capture_decision):
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = MagicMock(
                min_bitrate_kbps=226, is_cbr=False)
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate(album_data, 42, ctx)

        m = captured_measurement["m"]
        # spectral_bitrate_kbps on the measurement should be None (from current),
        # NOT 192 (from stale download spectral)
        self.assertIsNone(m.spectral_bitrate_kbps,
                          "quality gate should use current_spectral_bitrate, "
                          "not stale last_download_spectral_bitrate")

    def test_genuine_v0_replacing_transcode_accepted(self):
        """Contract test: genuine V0 replacing a transcode should be accepted
        by the quality gate, not requeued. Tests the full mark_done → quality
        gate data flow. Regression test for issue #18."""
        from lib.import_dispatch import _check_quality_gate
        from lib.quality import quality_gate_decision, AudioQualityMeasurement

        album_data = _make_album_data()
        ctx = _make_ctx()
        db = ctx.pipeline_db_source._get_db.return_value

        # After mark_done fix: genuine import clears stale spectral data
        db.get_request.return_value = make_request_row(
            last_download_spectral_bitrate=None,  # cleared by mark_done (was 192)
            last_download_spectral_grade="genuine",
            current_spectral_bitrate=None,        # cleared by mark_done (was 192)
            current_spectral_grade="genuine",
            verified_lossless=False,          # MP3 V0, not from FLAC
        )

        captured = {}

        def capture_and_decide(measurement):
            captured["m"] = measurement
            # Run the REAL decision function
            return quality_gate_decision(measurement)

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls, \
             patch("lib.quality.quality_gate_decision",
                   side_effect=capture_and_decide):
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = MagicMock(
                min_bitrate_kbps=226, is_cbr=False)
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate(album_data, 42, ctx)

        m = captured["m"]
        # Should see: 226kbps VBR, no spectral drag, not verified lossless
        self.assertEqual(m.min_bitrate_kbps, 226)
        self.assertFalse(m.is_cbr)
        self.assertIsNone(m.spectral_bitrate_kbps)
        # Decision should be "accept" (VBR >= 210), NOT "requeue_upgrade"
        self.assertEqual(quality_gate_decision(m), "accept")
        # Should NOT have called reset_to_wanted (no requeue)
        db.reset_to_wanted.assert_not_called()

    def test_dispatch_requeue_uses_intent(self):
        """dispatch_import requeue path should use intent_to_quality_override."""
        ir = _make_import_result(decision="transcode_upgrade",
                                 new_min_bitrate=227)
        album_data = _make_album_data()
        ctx = _make_ctx()
        bv_result = _make_bv_result()
        dl_info = DownloadInfo(filetype="mp3")

        with patch("lib.import_dispatch.sp.run") as mock_run, \
             patch("lib.import_dispatch._cleanup_staged_dir"), \
             patch("lib.import_dispatch.trigger_meelo_scan"), \
             patch("lib.import_dispatch._check_quality_gate"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            mock_run.return_value = MagicMock(
                returncode=0, stdout="", stderr="")
            from lib.import_dispatch import dispatch_import
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx)

        db = ctx.pipeline_db_source._get_db()
        call_args = db.reset_to_wanted.call_args
        self.assertEqual(
            call_args.kwargs.get("quality_override") or call_args[1].get("quality_override"),
            intent_to_quality_override(QualityIntent.upgrade),
        )


class TestOpusConversionDispatch(unittest.TestCase):
    """Test --opus-conversion flag passing and Opus dl_info population."""

    def _get_cmd(self, opus_conversion=False):
        """Run dispatch_import, capture the cmd passed to sp.run."""
        from lib.import_dispatch import dispatch_import
        album_data = _make_album_data()
        ctx = _make_ctx()
        ctx.cfg.opus_conversion = opus_conversion
        bv_result = _make_bv_result()
        dl_info = DownloadInfo(filetype="flac")
        ir = _make_import_result(decision="import", was_converted=True,
                                 original_filetype="flac", target_filetype="mp3")

        with patch("lib.import_dispatch.sp.run") as mock_run, \
             patch("lib.import_dispatch._cleanup_staged_dir"), \
             patch("lib.import_dispatch.trigger_meelo_scan"), \
             patch("lib.import_dispatch._check_quality_gate"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            mock_run.return_value = MagicMock(
                returncode=0, stdout="", stderr="")
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx)
            return mock_run.call_args[0][0]

    def test_opus_flag_passed_when_enabled(self):
        cmd = self._get_cmd(opus_conversion=True)
        self.assertIn("--opus-conversion", cmd)

    def test_opus_flag_not_passed_when_disabled(self):
        cmd = self._get_cmd(opus_conversion=False)
        self.assertNotIn("--opus-conversion", cmd)

    def test_opus_import_result_populates_dl_info(self):
        """ImportResult with final_format='opus 128' should update dl_info."""
        from lib.import_dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        ir = ImportResult(
            decision="import",
            final_format="opus 128",
            v0_verification_bitrate=247,
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128, verified_lossless=True,
                was_converted_from="flac"),
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


if __name__ == "__main__":
    unittest.main()
