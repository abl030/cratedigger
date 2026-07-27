"""Tests for lib/spectral_check.py — spectral quality verification."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from typing import ClassVar
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


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
        from lib.spectral_check import _Slice, detect_cliff
        # All slices at roughly the same dB level
        slices: list[_Slice] = [
            {"freq": 12000 + i * 500, "db": -50.0} for i in range(16)]
        result = detect_cliff(slices, threshold_db_per_khz=-12, min_slices=2, slice_width_hz=500)
        self.assertIsNone(result)

    def test_steep_dropoff_detects_cliff(self):
        from lib.spectral_check import _Slice, detect_cliff
        # Normal until 16kHz, then cliff
        slices: list[_Slice] = []
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
        from lib.spectral_check import _Slice, detect_cliff
        # One steep drop, then recovery — not a cliff
        slices: list[_Slice] = [
            {"freq": 12000 + i * 500, "db": -50.0} for i in range(16)]
        slices[5]["db"] = -70.0  # single spike
        slices[6]["db"] = -50.0  # recovery
        result = detect_cliff(slices, threshold_db_per_khz=-12, min_slices=2, slice_width_hz=500)
        self.assertIsNone(result)

    def test_gradual_rolloff_no_cliff(self):
        from lib.spectral_check import _Slice, detect_cliff
        # Smooth rolloff at -5 dB/kHz (natural, not a cliff)
        slices: list[_Slice] = [
            {"freq": 12000 + i * 500, "db": -50.0 - i * 2.5} for i in range(16)]
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
        from lib.spectral_check import TrackResult, classify_album
        tracks = [TrackResult("genuine", 35.0, False, None, None)] * 10
        grade, pct = classify_album(tracks)
        self.assertEqual(grade, "genuine")
        self.assertEqual(pct, 0.0)

    def test_majority_suspect(self):
        from lib.spectral_check import TrackResult, classify_album
        tracks = ([TrackResult("suspect", 70.0, True, 16000, 128)] * 7 +
                  [TrackResult("genuine", 35.0, False, None, None)] * 3)
        grade, pct = classify_album(tracks)
        self.assertEqual(grade, "suspect")
        self.assertEqual(pct, 70.0)

    def test_below_threshold(self):
        from lib.spectral_check import TrackResult, classify_album
        tracks = ([TrackResult("suspect", 70.0, True, 16000, 128)] * 4 +
                  [TrackResult("genuine", 35.0, False, None, None)] * 6)
        grade, pct = classify_album(tracks)
        self.assertEqual(grade, "genuine")
        self.assertEqual(pct, 40.0)

    def test_empty_tracks(self):
        from lib.spectral_check import classify_album
        grade, _pct = classify_album([])
        self.assertEqual(grade, "genuine")


class TestAnalyzeTrackMocked(unittest.TestCase):
    """Test analyze_track with mocked subprocess (no sox needed)."""

    def _make_sox_output(self, rms):
        return "", f"RMS     amplitude:     {rms:.6f}\n"

    @patch("lib.spectral_check.subprocess.run")
    def test_calls_sox_with_correct_args(self, mock_run):
        from lib.spectral_check import analyze_track
        mock_run.return_value = MagicMock(
            stderr="RMS     amplitude:     0.100000\n",
            returncode=0
        )
        analyze_track("/fake/path.mp3", trim_seconds=30)
        # Should be called 21 times: 1 reference + 16 in-window slices + 4
        # extension slices (issue #829 Phase 5 PR1 capture).
        self.assertEqual(mock_run.call_count, 21)
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
            return MagicMock(stderr=f"RMS     amplitude:     {rms:.6f}\n", returncode=0)
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


class TestArgvFlagConfusion(unittest.TestCase):
    """Peer-controlled filenames starting with '-' must not be parsed as
    sox/ffmpeg flags. Soulseek peers can name files arbitrarily; the
    pipeline must treat the filename as data, not argv."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="spectral_dash_test_")
        # Real FLAC tone with a leading-dash filename — what a hostile peer
        # could ship via Soulseek. Sox CAN decode FLAC natively, so any
        # failure here proves argv-flag confusion (not a decode failure).
        cls.dash_flac = os.path.join(cls.tmpdir, "-evil.flac")
        subprocess.run(
            ["sox", "-n", "-r", "44100", "-c", "2",
             cls.dash_flac, "synth", "2", "sin", "1000", "vol", "0.5"],
            check=True, capture_output=True,
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_leading_dash_filename_does_not_get_parsed_as_flag(self):
        """A real FLAC named '-evil.flac' must analyze normally, not be
        parsed as a sox flag and abort."""
        from lib.spectral_check import analyze_track
        result = analyze_track(self.dash_flac, trim_seconds=2)
        self.assertNotEqual(
            result.grade, "error",
            f"sox parsed '-evil.flac' as a flag instead of as a path "
            f"(error={result.error}). Need to prefix relative paths with './'.",
        )


class TestAlbumLevelSilentGenuineCollapse(unittest.TestCase):
    """When every track in an album errors out, analyze_album must NOT
    return grade='genuine'. The pre-existing behavior (drop error tracks
    from track_results, then classify the empty list as genuine) is the
    same bug class the codec fix targets — at the album level instead of
    the track level. Surfaced by ce-adversarial-reviewer 2026-05-03."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="spectral_album_err_")
        # Two undecodable files with audio extensions so analyze_album
        # finds them but analyze_track errors on each.
        for name in ("01-track.m4a", "02-track.m4a"):
            with open(os.path.join(cls.tmpdir, name), "wb") as f:
                f.write(b"not real audio")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_all_tracks_error_grades_album_error_not_genuine(self):
        """If every track in a non-empty album errors, the album grade
        must be 'error', not the silent-default 'genuine'."""
        from lib.spectral_check import analyze_album
        result = analyze_album(self.tmpdir, trim_seconds=2)
        self.assertEqual(
            result.grade, "error",
            f"All-error album silently graded {result.grade!r} — same "
            "silent-genuine bug class the codec fix was meant to close. "
            "Empty track_results from a non-empty file list must fail closed.",
        )
        # issue #829 Phase 5 PR1 review round 2, should-fix 11:
        # spectral_measurement_version claims "cliff_hz/
        # ultrasonic_deficit_db were measured by this code" — an
        # all-errored album measured neither, so it must stay None.
        self.assertIsNone(result.spectral_measurement_version)


class TestRcZeroNoRmsLineNoLongerSilent(unittest.TestCase):
    """Sox returncode==0 with no 'RMS amplitude:' line in stderr was the
    'legacy permissive path' the codec PR's docstring acknowledged. After
    review, that path is closed: any missing RMS line now surfaces as a
    decode failure regardless of return code. Same failure-shape contract
    as the rc!=0 path."""

    @patch("lib.spectral_check.subprocess.run")
    def test_clean_exit_no_rms_line_grades_error(self, mock_run):
        from lib.spectral_check import analyze_track
        mock_run.return_value = MagicMock(
            stderr="some warning that has no RMS line\n",
            returncode=0,
        )
        result = analyze_track("/fake/path.flac")
        self.assertEqual(
            result.grade, "error",
            "sox returning rc=0 with no RMS line must grade 'error', not "
            "fall through the silent-track early-out as 'genuine'.",
        )


class TestNaNRmsGuard(unittest.TestCase):
    """parse_rms_from_stat must reject NaN/inf to avoid silent-genuine
    via NaN comparisons (NaN >= 60 is False everywhere → 'genuine')."""

    def test_nan_rms_returns_none(self):
        from lib.spectral_check import parse_rms_from_stat
        self.assertIsNone(parse_rms_from_stat("RMS     amplitude:     nan\n"))

    def test_inf_rms_returns_none(self):
        from lib.spectral_check import parse_rms_from_stat
        self.assertIsNone(parse_rms_from_stat("RMS     amplitude:     inf\n"))


# ============================================================
# issue #829 Phase 5 PR1 — spectral capture (cliff_hz, codec_family,
# ultrasonic_deficit_db, spectral_measurement_version). Capture only:
# nothing here may change grade/cliff_detected/estimated_bitrate_kbps.
# ============================================================

class TestSliceWindowUnchanged(unittest.TestCase):
    """Locks the decision-path window: widening SLICE_FREQS shifts cliff
    detections and introduces false cliffs on genuine lossless near 20kHz
    (measured, see the Phase 5 plan). The extension slices are a separate,
    additive constant."""

    def test_slice_freqs_is_still_the_original_16_element_window(self):
        from lib.spectral_check import SLICE_FREQS
        self.assertEqual(SLICE_FREQS, list(range(12000, 20000, 500)))
        self.assertEqual(len(SLICE_FREQS), 16)

    def test_extension_slice_freqs(self):
        from lib.spectral_check import EXTENSION_SLICE_FREQS
        self.assertEqual(EXTENSION_SLICE_FREQS, [20000, 20500, 21000, 21500])

    def test_ultrasonic_deficit_slice_freqs_is_the_top_three_extension_slices(self):
        from lib.spectral_check import ULTRASONIC_DEFICIT_SLICE_FREQS
        self.assertEqual(ULTRASONIC_DEFICIT_SLICE_FREQS, [20500, 21000, 21500])

    def test_spectral_measurement_version_is_2(self):
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
        self.assertEqual(SPECTRAL_MEASUREMENT_VERSION, 2)


class TestCodecFamilyFromExtension(unittest.TestCase):
    """Extension-based codec family normalisation (issue #829 Phase 5 PR1).

    ``.ogg``/``.m4a`` are deliberately absent here — extension cannot
    resolve them (see ``TestCodecFamilyAmbiguousContainersProbeTheRealCodec``
    below), so these fixtures never touch a real file and stick to the
    extensions that genuinely are unambiguous.

    ``.aif``/``.aiff``/``.au``/``.alac``/``.ape`` are deliberately absent
    too (round 3 review finding E): none of them is in
    ``AUDIO_EXTENSIONS_DOTTED``, the file-enumeration filter
    ``analyze_album`` applies before ever calling
    ``codec_family_from_extension`` — production never reaches this
    function with one of these extensions, so pinning them as "lossless"
    here would assert unreachable behaviour. They fall through the shared
    ``.get(ext, CODEC_FAMILY_OTHER)`` default like any other extension
    outside the six-family vocabulary (see ``track.mid``/``track`` below).
    """

    CASES: ClassVar[list[tuple[str, str]]] = [
        ("track.mp3", "mp3"),
        ("track.MP3", "mp3"),
        ("track.aac", "aac"),
        ("track.opus", "opus"),
        ("track.flac", "lossless"),
        ("track.wav", "lossless"),
        ("track.wma", "other"),
        ("track.mid", "other"),
        ("track", "other"),
        ("track.aiff", "other"),
        ("track.alac", "other"),
    ]

    def test_extension_maps_to_expected_family(self):
        from lib.spectral_check import codec_family_from_extension
        for filename, expected in self.CASES:
            with self.subTest(filename=filename):
                self.assertEqual(
                    codec_family_from_extension(f"/some/dir/{filename}"),
                    expected,
                )

    def test_result_is_always_one_of_the_six_known_families(self):
        from lib.spectral_check import codec_family_from_extension
        known = {"mp3", "aac", "opus", "vorbis", "lossless", "other"}
        for filename, _expected in self.CASES:
            self.assertIn(
                codec_family_from_extension(f"/x/{filename}"), known,
            )


class TestCodecFamilyAmbiguousContainersProbeTheRealCodec(unittest.TestCase):
    """issue #829 BLOCKING 2: ``.ogg`` and ``.m4a`` cannot be resolved by
    extension alone — an Opus stream in an .ogg container is Opus, not
    Vorbis, and an ALAC stream in an .m4a container is lossless, not AAC.
    Verified against real encoded files (real ffprobe, real codec_family_
    from_extension — no mocks): guessing wrong here reproduces exactly the
    codec-blind bug class #829 exists to fix (an Opus album would be
    scored on Vorbis's decision-grade ladder in PR2, which cannot apply to
    Opus)."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="spectral_codec_family_test_")

        cls.opus_in_ogg = os.path.join(cls.tmpdir, "opus.ogg")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "sine=frequency=1000:duration=1",
             "-c:a", "libopus", cls.opus_in_ogg],
            capture_output=True, check=True,
        )

        cls.vorbis_in_ogg = os.path.join(cls.tmpdir, "vorbis.ogg")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "sine=frequency=1000:duration=1",
             "-c:a", "libvorbis", cls.vorbis_in_ogg],
            capture_output=True, check=True,
        )

        cls.alac_in_m4a = os.path.join(cls.tmpdir, "alac.m4a")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "sine=frequency=1000:duration=1",
             "-c:a", "alac", cls.alac_in_m4a],
            capture_output=True, check=True,
        )

        cls.aac_in_m4a = os.path.join(cls.tmpdir, "aac.m4a")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "sine=frequency=1000:duration=1",
             "-c:a", "aac", cls.aac_in_m4a],
            capture_output=True, check=True,
        )

        cls.flac_in_ogg = os.path.join(cls.tmpdir, "flac.ogg")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "sine=frequency=1000:duration=1",
             "-c:a", "flac", cls.flac_in_ogg],
            capture_output=True, check=True,
        )

        # round 3 review finding B: a genuinely unprobeable .m4a (ffprobe
        # cannot identify a stream at all) must degrade to "other", never
        # guess "aac" from the ambiguous extension.
        cls.unprobeable_m4a = os.path.join(cls.tmpdir, "corrupt.m4a")
        with open(cls.unprobeable_m4a, "wb") as fh:
            fh.write(b"not a real container\x00\x01\x02")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_opus_in_ogg_is_opus_not_vorbis(self):
        from lib.spectral_check import codec_family_from_extension
        self.assertEqual(
            codec_family_from_extension(self.opus_in_ogg), "opus",
        )

    def test_vorbis_in_ogg_is_vorbis(self):
        from lib.spectral_check import codec_family_from_extension
        self.assertEqual(
            codec_family_from_extension(self.vorbis_in_ogg), "vorbis",
        )

    def test_alac_in_m4a_is_lossless_not_aac(self):
        from lib.spectral_check import codec_family_from_extension
        self.assertEqual(
            codec_family_from_extension(self.alac_in_m4a), "lossless",
        )

    def test_aac_in_m4a_is_aac(self):
        from lib.spectral_check import codec_family_from_extension
        self.assertEqual(
            codec_family_from_extension(self.aac_in_m4a), "aac",
        )

    def test_flac_in_ogg_is_lossless(self):
        """Ogg-FLAC is a real, if rare, container (round 3 review finding
        B) — ``native_codec_format_label`` has no "flac" entry, so this
        needs the same probed-codec special case ALAC-in-M4A already
        gets."""
        from lib.spectral_check import codec_family_from_extension
        self.assertEqual(
            codec_family_from_extension(self.flac_in_ogg), "lossless",
        )

    def test_unprobeable_m4a_degrades_to_other_not_aac(self):
        """Round 3 review finding B: the old code passed the ambiguous
        extension as a fallback to ``native_codec_format_label``, so a
        genuinely unprobeable .m4a (ffprobe returns no codec at all — could
        be a truncated/corrupt ALAC file) was silently stamped "aac". This
        is exactly the codec-blind guess class issue #829 exists to fix."""
        from lib.spectral_check import codec_family_from_extension
        self.assertEqual(
            codec_family_from_extension(self.unprobeable_m4a), "other",
        )


class TestComputeUltrasonicDeficitDb(unittest.TestCase):
    """Level-invariant ultrasonic deficit (issue #829 Phase 5 PR1)."""

    def test_matches_reference_formula(self):
        from lib.spectral_check import _Slice, compute_ultrasonic_deficit_db
        slices: list[_Slice] = [
            {"freq": 20000, "db": -50.0},  # captured, excluded from the mean
            {"freq": 20500, "db": -60.0},
            {"freq": 21000, "db": -70.0},
            {"freq": 21500, "db": -80.0},
        ]
        # ref_db - mean(20500, 21000, 21500) = -10 - (-70) = 60
        result = compute_ultrasonic_deficit_db(-10.0, slices)
        assert result is not None
        self.assertAlmostEqual(result, 60.0)

    def test_20000hz_slice_is_captured_but_excluded_from_the_mean(self):
        """Changing ONLY the 20000Hz value must not move the result —
        proves the statistic really excludes it (matches score_v3)."""
        from lib.spectral_check import _Slice, compute_ultrasonic_deficit_db
        base: list[_Slice] = [
            {"freq": 20000, "db": -50.0},
            {"freq": 20500, "db": -60.0},
            {"freq": 21000, "db": -70.0},
            {"freq": 21500, "db": -80.0},
        ]
        moved: list[_Slice] = [
            {"freq": s["freq"], "db": s["db"]} for s in base
        ]
        moved[0]["db"] = -999.0
        self.assertEqual(
            compute_ultrasonic_deficit_db(-10.0, base),
            compute_ultrasonic_deficit_db(-10.0, moved),
        )

    def test_missing_required_slice_returns_none(self):
        from lib.spectral_check import _Slice, compute_ultrasonic_deficit_db
        # 21500Hz missing — only two of the three required slices present.
        slices: list[_Slice] = [
            {"freq": 20000, "db": -50.0},
            {"freq": 20500, "db": -60.0},
            {"freq": 21000, "db": -70.0},
        ]
        self.assertIsNone(compute_ultrasonic_deficit_db(-10.0, slices))

    def test_no_slices_at_all_returns_none(self):
        from lib.spectral_check import compute_ultrasonic_deficit_db
        self.assertIsNone(compute_ultrasonic_deficit_db(-10.0, []))


class TestUltrasonicDeficitUnmeasurableBandReturnsNone(unittest.TestCase):
    """issue #829 BLOCKING 3: a sox-native file whose sample rate puts the
    20.5-22kHz extension bands above Nyquist must report
    ultrasonic_deficit_db=None, not a fabricated ~115dB deficit that would
    be indistinguishable from a genuine launder under PR3's U>=62 gate.
    Real sox, real low-sample-rate WAV, real analyze_track — no mocks."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="spectral_low_samplerate_test_")

        # 32kHz: Nyquist=16kHz, well below the 20.5-22kHz extension bands
        # sox must refuse ("filter frequency must be less than
        # sample-rate/2") — the exact shape the review reproduced.
        cls.low_rate_wav = os.path.join(cls.tmpdir, "01 - low_rate.wav")
        subprocess.run(
            ["sox", "-n", "-r", "32000", "-c", "2",
             cls.low_rate_wav, "synth", "3", "sin", "1000", "vol", "0.5"],
            check=True, capture_output=True,
        )

        # 44.1kHz control — same tone, real Nyquist headroom for every
        # extension band.
        cls.control_flac = os.path.join(cls.tmpdir, "02 - control.flac")
        subprocess.run(
            ["sox", "-n", "-r", "44100", "-c", "2",
             cls.control_flac, "synth", "3", "sin", "1000", "vol", "0.5"],
            check=True, capture_output=True,
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_sub_44khz_file_reports_none_not_a_fabricated_deficit(self):
        from lib.spectral_check import analyze_track
        result = analyze_track(self.low_rate_wav, trim_seconds=2)
        self.assertNotEqual(result.grade, "error")
        self.assertIsNone(
            result.ultrasonic_deficit_db,
            "a 32kHz file's unmeasurable extension bands must report "
            "None, not a Nyquist-artifact deficit "
            f"(got {result.ultrasonic_deficit_db})",
        )

    def test_44khz_control_reports_a_real_bounded_deficit(self):
        """Guard against over-correcting to None-always: a genuine
        44.1kHz file must still produce a real, boundedly-small deficit."""
        from lib.spectral_check import analyze_track
        result = analyze_track(self.control_flac, trim_seconds=2)
        self.assertNotEqual(result.grade, "error")
        assert result.ultrasonic_deficit_db is not None
        # A pure synthetic tone's ultrasonic bands are silent by
        # construction, same as a genuine sparse-HF master — this asserts
        # the value is a normal measured deficit, nowhere near the ~115dB
        # Nyquist-artifact magnitude BLOCKING 3 produced.
        self.assertLess(result.ultrasonic_deficit_db, 100.0)


class TestAggregateAlbumSpectralCapture(unittest.TestCase):
    """Pure album-level aggregation of per-track capture facts (issue #829
    Phase 5 PR1) — direct unit tests, no I/O."""

    def test_empty_track_list(self):
        from lib.spectral_check import aggregate_album_spectral_capture
        self.assertEqual(
            aggregate_album_spectral_capture([]), (None, None, None),
        )

    def test_cliff_hz_is_min_of_detected_cliffs(self):
        from lib.spectral_check import (
            TrackResult,
            aggregate_album_spectral_capture,
        )
        tracks = [
            TrackResult("suspect", cliff_freq_hz=18000, codec_family="mp3"),
            TrackResult("suspect", cliff_freq_hz=16000, codec_family="mp3"),
            TrackResult("genuine", cliff_freq_hz=None, codec_family="mp3"),
        ]
        cliff_hz, _codec_family, _deficit = (
            aggregate_album_spectral_capture(tracks)
        )
        self.assertEqual(cliff_hz, 16000)

    def test_cliff_hz_none_when_no_track_has_a_cliff(self):
        from lib.spectral_check import (
            TrackResult,
            aggregate_album_spectral_capture,
        )
        tracks = [TrackResult("genuine", codec_family="lossless") for _ in range(3)]
        cliff_hz, _codec_family, _deficit = (
            aggregate_album_spectral_capture(tracks)
        )
        self.assertIsNone(cliff_hz)

    def test_ultrasonic_deficit_db_is_mean_of_available_values(self):
        from lib.spectral_check import (
            TrackResult,
            aggregate_album_spectral_capture,
        )
        tracks = [
            TrackResult("genuine", ultrasonic_deficit_db=40.0),
            TrackResult("genuine", ultrasonic_deficit_db=60.0),
            TrackResult("genuine", ultrasonic_deficit_db=None),  # excluded
        ]
        _cliff_hz, _codec_family, deficit = (
            aggregate_album_spectral_capture(tracks)
        )
        assert deficit is not None
        self.assertAlmostEqual(deficit, 50.0)

    def test_ultrasonic_deficit_db_none_when_no_track_has_one(self):
        from lib.spectral_check import (
            TrackResult,
            aggregate_album_spectral_capture,
        )
        tracks = [TrackResult("genuine") for _ in range(2)]
        _cliff_hz, _codec_family, deficit = (
            aggregate_album_spectral_capture(tracks)
        )
        self.assertIsNone(deficit)

    def test_codec_family_is_first_track(self):
        from lib.spectral_check import (
            TrackResult,
            aggregate_album_spectral_capture,
        )
        tracks = [
            TrackResult("genuine", codec_family="opus"),
            TrackResult("genuine", codec_family="mp3"),
        ]
        _cliff_hz, codec_family, _deficit = (
            aggregate_album_spectral_capture(tracks)
        )
        self.assertEqual(codec_family, "opus")


class TestExtensionSlicesNeverFeedCliffDetection(unittest.TestCase):
    """issue #829 Phase 5 PR1: the four extension slices (20000-21500Hz)
    must never influence detect_cliff/cliff_detected/cliff_freq_hz/grade.
    """

    # 16 in-window slices (12000..19500) with no cliff, plus 4 extension
    # slices (20000..21500) that DO contain a steep dropoff.
    _NO_CLIFF_IN_WINDOW: ClassVar[list[float]] = [-20.0] * 16
    _CLIFF_IN_EXTENSION_ONLY: ClassVar[list[float]] = [-20.0, -35.0, -60.0, -90.0]

    def test_fixture_would_show_a_cliff_if_extension_slices_leaked_in(self):
        """Sanity check: feeding detect_cliff the WIDER 20-slice vector
        DOES find a cliff — proves the exclusion below is a real
        constraint, not a no-op."""
        from lib.spectral_check import (
            EXTENSION_SLICE_FREQS,
            SLICE_FREQS,
            _Slice,
            detect_cliff,
        )
        in_window: list[_Slice] = [
            {"freq": f, "db": d}
            for f, d in zip(SLICE_FREQS, self._NO_CLIFF_IN_WINDOW, strict=True)
        ]
        self.assertIsNone(detect_cliff(in_window))

        extended: list[_Slice] = in_window + [
            {"freq": f, "db": d}
            for f, d in zip(
                EXTENSION_SLICE_FREQS, self._CLIFF_IN_EXTENSION_ONLY, strict=True,
            )
        ]
        self.assertIsNotNone(
            detect_cliff(extended),
            "fixture must show a cliff when extension slices are "
            "included — otherwise this test doesn't prove anything",
        )

    @patch("lib.spectral_check.subprocess.run")
    def test_analyze_track_ignores_extension_band_content(self, mock_run):
        """The production analyze_track() must grade this track 'genuine'
        with no cliff, even though the SAME dB values — if detect_cliff
        saw them — would report one at the window boundary."""
        from lib.spectral_check import (
            EXTENSION_SLICE_FREQS,
            SLICE_FREQS,
            analyze_track,
        )

        ref_db_value = -20.0
        band_db = {1000: ref_db_value}
        band_db.update(zip(SLICE_FREQS, self._NO_CLIFF_IN_WINDOW, strict=True))
        band_db.update(
            zip(EXTENSION_SLICE_FREQS, self._CLIFF_IN_EXTENSION_ONLY, strict=True)
        )

        def _rms_for_db(db):
            return 10 ** (db / 20.0)

        def side_effect(cmd, **kwargs):
            sinc_idx = cmd.index("sinc")
            lo_hz = int(cmd[sinc_idx + 1].split("-")[0])
            db = band_db.get(lo_hz, ref_db_value)
            return MagicMock(
                stderr=f"RMS     amplitude:     {_rms_for_db(db):.8f}\n",
                returncode=0,
            )

        mock_run.side_effect = side_effect
        result = analyze_track("/fake/no_cliff.flac", trim_seconds=30)

        self.assertFalse(result.cliff_detected)
        self.assertIsNone(result.cliff_freq_hz)
        self.assertEqual(result.grade, "genuine")


class TestAnalyzeAlbumCaptureFieldsRealAudio(unittest.TestCase):
    """Real sox end-to-end: analyze_album must stamp the new capture
    fields on genuine audio, not just in mocked unit tests."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="spectral_capture_test_")
        cls.tone_flac = os.path.join(cls.tmpdir, "01 - Tone.flac")
        subprocess.run(
            ["sox", "-n", "-r", "44100", "-c", "2",
             cls.tone_flac, "synth", "3", "sin", "1000", "vol", "0.5"],
            check=True, capture_output=True,
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_album_result_carries_capture_fields(self):
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION, analyze_album
        result = analyze_album(self.tmpdir, trim_seconds=2)
        self.assertNotEqual(result.grade, "error")
        self.assertEqual(
            result.spectral_measurement_version, SPECTRAL_MEASUREMENT_VERSION,
        )
        self.assertEqual(result.codec_family, "lossless")
        self.assertEqual(len(result.tracks), 1)
        self.assertEqual(result.tracks[0].codec_family, "lossless")
        # issue #829 BLOCKING 3 regression guard: a real 44.1kHz file has
        # Nyquist headroom for every extension band, so this must be a
        # real measured value (not None) and nowhere near the ~115dB
        # Nyquist-artifact magnitude a sub-44.1kHz file used to fabricate.
        self.assertIsNotNone(result.ultrasonic_deficit_db)
        assert result.ultrasonic_deficit_db is not None
        self.assertLess(result.ultrasonic_deficit_db, 100.0)


if __name__ == "__main__":
    unittest.main()
