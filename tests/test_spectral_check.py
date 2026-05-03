"""Tests for lib/spectral_check.py — spectral quality verification."""

import math
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))


class TestParseRmsFromStat(unittest.TestCase):
    """Test parsing RMS amplitude from sox stat stderr output."""

    def test_parse_valid_output(self):
        from lib.spectral_check import parse_rms_from_stat
        stderr = (
            "Samples read:          12658176\n"
            "Length (seconds):    143.516735\n"
            "RMS     amplitude:     0.170998\n"
            "Maximum delta:         0.816424\n"
        )
        result = parse_rms_from_stat(stderr)
        assert result is not None
        self.assertAlmostEqual(result, 0.170998, places=6)

    def test_parse_very_small_rms(self):
        from lib.spectral_check import parse_rms_from_stat
        stderr = "RMS     amplitude:     0.000003\n"
        result = parse_rms_from_stat(stderr)
        assert result is not None
        self.assertAlmostEqual(result, 0.000003, places=9)

    def test_parse_missing_rms_returns_none(self):
        from lib.spectral_check import parse_rms_from_stat
        self.assertIsNone(parse_rms_from_stat("no rms here\n"))

    def test_parse_empty_string(self):
        from lib.spectral_check import parse_rms_from_stat
        self.assertIsNone(parse_rms_from_stat(""))


class TestRmsToDb(unittest.TestCase):
    """Test RMS to dB conversion."""

    def test_positive_rms(self):
        from lib.spectral_check import rms_to_db
        # 20 * log10(0.01) ≈ -40
        self.assertAlmostEqual(rms_to_db(0.01), -40.0, places=1)

    def test_unity_rms(self):
        from lib.spectral_check import rms_to_db
        self.assertAlmostEqual(rms_to_db(1.0), 0.0, places=1)

    def test_very_small_rms(self):
        from lib.spectral_check import rms_to_db
        result = rms_to_db(0.0000001)
        self.assertLess(result, -100)

    def test_zero_rms_returns_floor(self):
        from lib.spectral_check import rms_to_db
        result = rms_to_db(0.0)
        self.assertEqual(result, -140.0)

    def test_negative_rms_returns_floor(self):
        from lib.spectral_check import rms_to_db
        result = rms_to_db(-0.5)
        self.assertEqual(result, -140.0)


class TestGradientCalculation(unittest.TestCase):
    """Test spectral gradient (cliff) detection."""

    def test_flat_spectrum_no_cliff(self):
        from lib.spectral_check import detect_cliff
        # All slices at roughly the same dB level
        slices = [{"freq": 12000 + i * 500, "db": -50.0} for i in range(16)]
        result = detect_cliff(slices, threshold_db_per_khz=-12, min_slices=2, slice_width_hz=500)
        self.assertIsNone(result)

    def test_steep_dropoff_detects_cliff(self):
        from lib.spectral_check import detect_cliff
        # Normal until 16kHz, then cliff
        slices = []
        for i in range(16):
            freq = 12000 + i * 500
            if freq < 16000:
                slices.append({"freq": freq, "db": -50.0})
            elif freq == 16000:
                slices.append({"freq": freq, "db": -60.0})  # -20 dB/kHz
            else:
                slices.append({"freq": freq, "db": -90.0})  # -60 dB/kHz
        result = detect_cliff(slices, threshold_db_per_khz=-12, min_slices=2, slice_width_hz=500)
        assert result is not None
        self.assertGreaterEqual(result, 15500)
        self.assertLessEqual(result, 16500)

    def test_single_steep_slice_no_cliff(self):
        from lib.spectral_check import detect_cliff
        # One steep drop, then recovery — not a cliff
        slices = [{"freq": 12000 + i * 500, "db": -50.0} for i in range(16)]
        slices[5]["db"] = -70.0  # single spike
        slices[6]["db"] = -50.0  # recovery
        result = detect_cliff(slices, threshold_db_per_khz=-12, min_slices=2, slice_width_hz=500)
        self.assertIsNone(result)

    def test_gradual_rolloff_no_cliff(self):
        from lib.spectral_check import detect_cliff
        # Smooth rolloff at -5 dB/kHz (natural, not a cliff)
        slices = [{"freq": 12000 + i * 500, "db": -50.0 - i * 2.5} for i in range(16)]
        result = detect_cliff(slices, threshold_db_per_khz=-12, min_slices=2, slice_width_hz=500)
        self.assertIsNone(result)


class TestEstimateOriginalBitrate(unittest.TestCase):
    """Test bitrate estimation from cliff frequency."""

    def test_cliff_at_16khz_is_128(self):
        from lib.spectral_check import estimate_bitrate_from_cliff
        result = estimate_bitrate_from_cliff(16000)
        self.assertEqual(result, 128)

    def test_cliff_at_17khz_is_128(self):
        from lib.spectral_check import estimate_bitrate_from_cliff
        result = estimate_bitrate_from_cliff(17000)
        self.assertEqual(result, 128)

    def test_cliff_at_15khz_is_96(self):
        from lib.spectral_check import estimate_bitrate_from_cliff
        result = estimate_bitrate_from_cliff(15000)
        self.assertEqual(result, 96)

    def test_cliff_at_18khz_is_192(self):
        from lib.spectral_check import estimate_bitrate_from_cliff
        result = estimate_bitrate_from_cliff(18500)
        self.assertEqual(result, 192)

    def test_cliff_at_19khz_is_256(self):
        from lib.spectral_check import estimate_bitrate_from_cliff
        result = estimate_bitrate_from_cliff(19500)
        self.assertEqual(result, 256)

    def test_no_cliff_returns_none(self):
        from lib.spectral_check import estimate_bitrate_from_cliff
        self.assertIsNone(estimate_bitrate_from_cliff(None))


class TestClassifyTrack(unittest.TestCase):
    """Test per-track classification logic."""

    def test_genuine(self):
        from lib.spectral_check import classify_track
        result = classify_track(hf_deficit_db=35.0, cliff_freq_hz=None)
        self.assertEqual(result.grade, "genuine")
        self.assertFalse(result.cliff_detected)

    def test_suspect_cliff(self):
        from lib.spectral_check import classify_track
        result = classify_track(hf_deficit_db=45.0, cliff_freq_hz=16000)
        self.assertEqual(result.grade, "suspect")
        self.assertTrue(result.cliff_detected)
        self.assertEqual(result.estimated_bitrate_kbps, 128)

    def test_suspect_hf_deficit(self):
        from lib.spectral_check import classify_track
        result = classify_track(hf_deficit_db=65.0, cliff_freq_hz=None)
        self.assertEqual(result.grade, "suspect")

    def test_marginal(self):
        from lib.spectral_check import classify_track
        result = classify_track(hf_deficit_db=50.0, cliff_freq_hz=None)
        self.assertEqual(result.grade, "marginal")

    def test_marginal_boundary_40(self):
        from lib.spectral_check import classify_track
        result = classify_track(hf_deficit_db=40.0, cliff_freq_hz=None)
        self.assertEqual(result.grade, "marginal")

    def test_genuine_boundary_39(self):
        from lib.spectral_check import classify_track
        result = classify_track(hf_deficit_db=39.9, cliff_freq_hz=None)
        self.assertEqual(result.grade, "genuine")


class TestClassifyAlbum(unittest.TestCase):
    """Test album-level classification from track results."""

    def test_all_genuine(self):
        from lib.spectral_check import classify_album, TrackResult
        tracks = [TrackResult("genuine", 35.0, False, None, None)] * 10
        grade, pct = classify_album(tracks)
        self.assertEqual(grade, "genuine")
        self.assertEqual(pct, 0.0)

    def test_majority_suspect(self):
        from lib.spectral_check import classify_album, TrackResult
        tracks = ([TrackResult("suspect", 70.0, True, 16000, 128)] * 7 +
                  [TrackResult("genuine", 35.0, False, None, None)] * 3)
        grade, pct = classify_album(tracks)
        self.assertEqual(grade, "suspect")
        self.assertEqual(pct, 70.0)

    def test_below_threshold(self):
        from lib.spectral_check import classify_album, TrackResult
        tracks = ([TrackResult("suspect", 70.0, True, 16000, 128)] * 4 +
                  [TrackResult("genuine", 35.0, False, None, None)] * 6)
        grade, pct = classify_album(tracks)
        self.assertEqual(grade, "genuine")
        self.assertEqual(pct, 40.0)

    def test_empty_tracks(self):
        from lib.spectral_check import classify_album
        grade, pct = classify_album([])
        self.assertEqual(grade, "genuine")


class TestAnalyzeTrackMocked(unittest.TestCase):
    """Test analyze_track with mocked subprocess (no sox needed)."""

    def _make_sox_output(self, rms):
        return "", "RMS     amplitude:     %.6f\n" % rms

    @patch("lib.spectral_check.subprocess.run")
    def test_calls_sox_with_correct_args(self, mock_run):
        from lib.spectral_check import analyze_track
        mock_run.return_value = MagicMock(
            stderr="RMS     amplitude:     0.100000\n",
            returncode=0
        )
        analyze_track("/fake/path.mp3", trim_seconds=30)
        # Should be called 17 times: 1 reference + 16 slices
        self.assertEqual(mock_run.call_count, 17)
        # First call should be reference band 1000-4000
        first_call_args = mock_run.call_args_list[0][0][0]
        self.assertIn("1000-4000", first_call_args)
        self.assertIn("trim", first_call_args)
        self.assertIn("30", first_call_args)

    @patch("lib.spectral_check.subprocess.run")
    def test_genuine_profile(self, mock_run):
        from lib.spectral_check import analyze_track
        # Simulate genuine file: ref=0.1, all slices gradually decreasing
        def side_effect(cmd, **kwargs):
            sinc_arg = [a for a in cmd if "-" in a and a[0].isdigit()]
            if sinc_arg and sinc_arg[0].startswith("1000"):
                rms = 0.1  # reference
            else:
                rms = 0.005  # ~-26dB below ref = healthy HF
            return MagicMock(stderr="RMS     amplitude:     %.6f\n" % rms, returncode=0)
        mock_run.side_effect = side_effect
        result = analyze_track("/fake/genuine.mp3")
        self.assertEqual(result.grade, "genuine")
        self.assertFalse(result.cliff_detected)

    @patch("lib.spectral_check.subprocess.run")
    def test_sox_not_found(self, mock_run):
        from lib.spectral_check import analyze_track
        mock_run.side_effect = FileNotFoundError("sox not found")
        result = analyze_track("/fake/path.mp3")
        self.assertEqual(result.grade, "error")

    @patch("lib.spectral_check.subprocess.run")
    def test_sox_timeout(self, mock_run):
        import subprocess
        from lib.spectral_check import analyze_track
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="sox", timeout=60)
        result = analyze_track("/fake/path.mp3")
        self.assertEqual(result.grade, "error")


# ============================================================
# Integration tests — require sox + test audio files
# ============================================================

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "albums")
HAS_SOX = shutil.which("sox") is not None
HAS_FIXTURES = os.path.isdir(FIXTURES_DIR) and os.path.isdir(os.path.join(FIXTURES_DIR, "01_genuine_flac"))


def _fixture(name):
    return os.path.join(FIXTURES_DIR, name)


@unittest.skipUnless(HAS_SOX, "sox not available")
@unittest.skipUnless(HAS_FIXTURES, "fixtures not generated — run tests/fixtures/generate_fixtures.sh")
class TestSpectralIntegration(unittest.TestCase):
    """Integration tests with real sox and generated audio fixtures."""

    def test_genuine_flac_all_genres(self):
        from lib.spectral_check import analyze_album
        result = analyze_album(_fixture("01_genuine_flac"))
        self.assertEqual(result.grade, "genuine",
                         f"Genuine FLACs flagged as {result.grade} ({result.suspect_pct:.0f}% suspect)")

    def test_genuine_v0_all_genres(self):
        from lib.spectral_check import analyze_album
        result = analyze_album(_fixture("02_genuine_v0"))
        self.assertEqual(result.grade, "genuine",
                         f"Genuine V0s flagged as {result.grade} ({result.suspect_pct:.0f}% suspect)")

    def test_genuine_320_all_genres(self):
        from lib.spectral_check import analyze_album
        result = analyze_album(_fixture("04_cbr_320"))
        self.assertEqual(result.grade, "genuine",
                         f"Genuine 320s flagged as {result.grade} ({result.suspect_pct:.0f}% suspect)")

    def test_transcode_128_flac_detected(self):
        from lib.spectral_check import analyze_album
        result = analyze_album(_fixture("09_fake_flac_128"))
        self.assertIn(result.grade, ("suspect", "likely_transcode"),
                      f"128→FLAC transcodes not detected: {result.grade} ({result.suspect_pct:.0f}%)")
        # Should estimate ~128kbps
        for t in result.tracks:
            if t.cliff_detected:
                self.assertIsNotNone(t.estimated_bitrate_kbps)
                self.assertLessEqual(t.estimated_bitrate_kbps, 160)

    def test_transcode_192_flac_detected(self):
        from lib.spectral_check import analyze_album
        result = analyze_album(_fixture("11_fake_flac_192"))
        self.assertIn(result.grade, ("suspect", "likely_transcode"),
                      f"192→FLAC transcodes not detected: {result.grade} ({result.suspect_pct:.0f}%)")

    def test_album_grade_has_estimated_bitrate(self):
        from lib.spectral_check import analyze_album
        result = analyze_album(_fixture("09_fake_flac_128"))
        self.assertIsNotNone(result.estimated_bitrate_kbps,
                             "Album-level estimated bitrate should be set for transcodes")


class TestDecodeFailureNotGenuine(unittest.TestCase):
    """A sox decode failure must NOT be silently graded as genuine.

    Bug discovered 2026-05-03: sox in the dev shell has no handler for
    .m4a/.aac/.alac. Calling `sox file.m4a -n stat` exits non-zero with
    stderr "FAIL formats: no handler for file extension `m4a'" and emits
    no "RMS amplitude:" line. parse_rms_from_stat returned None for every
    band, the early-out at analyze_track returned grade='genuine' with
    hf_deficit_db=0.0 on every track, and ALAC files were silently
    classified as verified-lossless without ever being measured.
    """

    @patch("lib.spectral_check.subprocess.run")
    def test_decode_failure_grades_error_not_genuine(self, mock_run):
        from lib.spectral_check import analyze_track
        # Real sox stderr when handed an undecodable file:
        mock_run.return_value = MagicMock(
            stderr="sox FAIL formats: no handler for file extension `m4a'\n",
            returncode=2,
        )
        result = analyze_track("/fake/path.m4a")
        # Currently returns 'genuine' (the bug). The fix must return 'error'.
        self.assertEqual(
            result.grade, "error",
            "Decode failure (no RMS line in sox stderr) must grade 'error', "
            "not silently fall through the silent-track early-out as 'genuine'.",
        )

    @patch("lib.spectral_check.subprocess.run")
    def test_silent_track_still_grades_genuine(self, mock_run):
        """Guard that the failure-vs-silent fix doesn't over-broaden.

        A real silent track (sox decoded fine, RMS legitimately ~0) should
        still hit the silent-track early-out and grade 'genuine'.
        """
        from lib.spectral_check import analyze_track
        mock_run.return_value = MagicMock(
            stderr="RMS     amplitude:     0.0000001\n",
            returncode=0,
        )
        result = analyze_track("/fake/silent.flac")
        self.assertEqual(result.grade, "genuine")
        self.assertEqual(result.hf_deficit_db, 0.0)


HAS_FFMPEG = shutil.which("ffmpeg") is not None


@unittest.skipUnless(HAS_SOX and HAS_FFMPEG, "needs sox + ffmpeg")
class TestM4aFallback(unittest.TestCase):
    """Real ALAC .m4a file must produce a real spectral measurement.

    Drives the ffmpeg pipe fallback: sox can't decode m4a natively, so
    spectral_check must route the file through `ffmpeg -i in -f wav -` and
    feed sox via stdin. After the fix, a synthetic 1kHz tone in ALAC must
    grade somewhere meaningful (not the all-zero default), and a fake-FLAC
    pattern (steep cliff at 16kHz) in ALAC must be detectable.
    """

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="spectral_m4a_test_")
        # Pure 1kHz tone, encoded as ALAC inside an .m4a container.
        # Sox can't decode this; the fallback must.
        cls.tone_m4a = os.path.join(cls.tmpdir, "tone.m4a")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi",
             "-i", "sine=frequency=1000:duration=35",
             "-c:a", "alac", cls.tone_m4a],
            capture_output=True, check=True,
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_m4a_alac_actually_analyzed(self):
        from lib.spectral_check import analyze_track
        result = analyze_track(self.tone_m4a, trim_seconds=30)
        # A pure 1kHz tone has effectively zero energy at 18-20kHz, so the
        # measured HF deficit will be enormous (well above HF_DEFICIT_SUSPECT).
        # The point is that we get a REAL measurement, not the default 0.0.
        self.assertNotEqual(
            result.grade, "error",
            "ffmpeg fallback should let sox decode the m4a — saw error",
        )
        # Either we measured a real deficit (suspect) OR the tone hit some
        # other genuine path; what we MUST NOT see is the default-0.0 grade.
        # The default-0.0 case is grade='genuine' with hf_deficit_db == 0.0
        # AND cliff_freq_hz is None — that's the bug fingerprint.
        is_default_zero = (
            result.grade == "genuine"
            and result.hf_deficit_db == 0.0
            and result.cliff_freq_hz is None
        )
        self.assertFalse(
            is_default_zero,
            f"m4a hit the silent-track default (grade={result.grade}, "
            f"hf_deficit_db={result.hf_deficit_db}, cliff={result.cliff_freq_hz}) "
            "— ffmpeg fallback is not wired up.",
        )


if __name__ == "__main__":
    unittest.main()
