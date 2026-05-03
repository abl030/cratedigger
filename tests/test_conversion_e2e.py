#!/usr/bin/env python3
"""End-to-end conversion tests with real audio files.

Tests the full conversion pipeline: generate FLAC → convert via
ConversionSpec → verify files on disk and bitrates. Uses synthetic
audio fixtures with deterministic V0 bitrates.

Also tests pure functions: ConversionSpec, parse_verified_lossless_target,
determine_verified_lossless.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HARNESS_DIR = os.path.join(ROOT_DIR, "harness")
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, HARNESS_DIR)

from tests.audio_fixtures import make_test_flac, make_test_album, get_bitrate_kbps


# ============================================================================
# Audio fixtures sanity
# ============================================================================

@unittest.skipUnless(shutil.which("sox"), "sox not available")
class TestAudioFixtures(unittest.TestCase):
    """Verify the synthetic audio fixture produces expected bitrates."""

    def test_genuine_flac_produces_high_v0_bitrate(self):
        """15500Hz cutoff → V0 bitrate above 210kbps threshold."""
        with tempfile.TemporaryDirectory() as d:
            flac = os.path.join(d, "test.flac")
            mp3 = os.path.join(d, "test.mp3")
            make_test_flac(flac, cutoff_hz=15500)
            subprocess.run(
                ["ffmpeg", "-i", flac, "-codec:a", "libmp3lame", "-q:a", "0",
                 "-map_metadata", "0", "-id3v2_version", "3", "-y", mp3],
                capture_output=True, timeout=30)
            br = get_bitrate_kbps(mp3)
            self.assertGreater(br, 210, f"Genuine FLAC V0 bitrate {br}kbps should be > 210")
            self.assertLess(br, 300, f"V0 bitrate {br}kbps unexpectedly high")

    def test_transcode_flac_produces_low_v0_bitrate(self):
        """12000Hz cutoff → V0 bitrate below 210kbps threshold."""
        with tempfile.TemporaryDirectory() as d:
            flac = os.path.join(d, "test.flac")
            mp3 = os.path.join(d, "test.mp3")
            make_test_flac(flac, cutoff_hz=12000)
            subprocess.run(
                ["ffmpeg", "-i", flac, "-codec:a", "libmp3lame", "-q:a", "0",
                 "-map_metadata", "0", "-id3v2_version", "3", "-y", mp3],
                capture_output=True, timeout=30)
            br = get_bitrate_kbps(mp3)
            self.assertLess(br, 210, f"Transcode FLAC V0 bitrate {br}kbps should be < 210")

    def test_make_test_album_creates_tracks(self):
        with tempfile.TemporaryDirectory() as d:
            album_dir = os.path.join(d, "album")
            paths = make_test_album(album_dir, track_count=3)
            self.assertEqual(len(paths), 3)
            for p in paths:
                self.assertTrue(os.path.exists(p))
                self.assertTrue(p.endswith(".flac"))


# ============================================================================
# ConversionSpec + parse_verified_lossless_target
# ============================================================================

class TestConversionSpec(unittest.TestCase):
    """Test ConversionSpec dataclass and V0_SPEC constant."""

    def test_v0_spec_values(self):
        from harness.import_one import V0_SPEC
        self.assertEqual(V0_SPEC.codec, "libmp3lame")
        self.assertEqual(V0_SPEC.codec_args, ("-q:a", "0"))
        self.assertEqual(V0_SPEC.extension, "mp3")
        self.assertEqual(V0_SPEC.label, "mp3 v0")
        self.assertIn("-id3v2_version", V0_SPEC.metadata_args)

    def test_frozen(self):
        from harness.import_one import V0_SPEC
        with self.assertRaises(AttributeError):
            V0_SPEC.codec = "other"  # type: ignore[misc]


class TestParseVerifiedLosslessTarget(unittest.TestCase):
    """Test parsing target format strings into ConversionSpec."""

    def _parse(self, spec):
        from harness.import_one import parse_verified_lossless_target
        return parse_verified_lossless_target(spec)

    # --- Opus ---

    def test_opus_128(self):
        s = self._parse("opus 128")
        self.assertEqual(s.codec, "libopus")
        self.assertEqual(s.codec_args, ("-b:a", "128k"))
        self.assertEqual(s.extension, "opus")
        self.assertEqual(s.label, "opus 128")

    def test_opus_96(self):
        s = self._parse("opus 96")
        self.assertEqual(s.codec_args, ("-b:a", "96k"))

    def test_opus_case_insensitive(self):
        s = self._parse("Opus 128")
        self.assertEqual(s.codec, "libopus")

    # --- MP3 VBR ---

    def test_mp3_v0(self):
        s = self._parse("mp3 v0")
        self.assertEqual(s.codec, "libmp3lame")
        self.assertEqual(s.codec_args, ("-q:a", "0"))
        self.assertEqual(s.extension, "mp3")
        self.assertIn("-id3v2_version", s.metadata_args)

    def test_mp3_v2(self):
        s = self._parse("mp3 v2")
        self.assertEqual(s.codec_args, ("-q:a", "2"))

    # --- MP3 CBR ---

    def test_mp3_192(self):
        s = self._parse("mp3 192")
        self.assertEqual(s.codec_args, ("-b:a", "192k"))
        self.assertEqual(s.extension, "mp3")

    # --- AAC ---

    def test_aac_128(self):
        s = self._parse("aac 128")
        self.assertEqual(s.codec, "aac")
        self.assertEqual(s.codec_args, ("-b:a", "128k"))
        self.assertEqual(s.extension, "m4a")

    # --- Error cases ---

    def test_empty_string_raises(self):
        with self.assertRaises(ValueError):
            self._parse("")

    def test_single_word_raises(self):
        with self.assertRaises(ValueError):
            self._parse("opus")

    def test_unknown_codec_raises(self):
        with self.assertRaises(ValueError):
            self._parse("vorbis 128")

    def test_opus_non_numeric_raises(self):
        with self.assertRaises(ValueError):
            self._parse("opus high")

    def test_mp3_bad_quality_raises(self):
        with self.assertRaises(ValueError):
            self._parse("mp3 best")

    # --- Range validation ---

    def test_mp3_v10_out_of_range(self):
        with self.assertRaises(ValueError):
            self._parse("mp3 v10")

    def test_opus_0_out_of_range(self):
        with self.assertRaises(ValueError):
            self._parse("opus 0")

    def test_mp3_cbr_400_out_of_range(self):
        with self.assertRaises(ValueError):
            self._parse("mp3 400")

    def test_aac_0_out_of_range(self):
        with self.assertRaises(ValueError):
            self._parse("aac 0")

    def test_whitespace_trimmed(self):
        s = self._parse("  opus 128  ")
        self.assertEqual(s.codec, "libopus")


# ============================================================================
# determine_verified_lossless
# ============================================================================

class TestDetermineVerifiedLossless(unittest.TestCase):
    """Single source of truth for verified lossless derivation."""

    def _dvl(self, target_format=None, spectral_grade=None,
             converted_count=0, is_transcode=False, v0_probe=None):
        from lib.quality import determine_verified_lossless
        return determine_verified_lossless(
            target_format, spectral_grade, converted_count, is_transcode,
            v0_probe=v0_probe)

    # --- FLAC-on-disk path ---

    def test_flac_genuine_is_verified(self):
        self.assertTrue(self._dvl(target_format="flac", spectral_grade="genuine"))

    def test_flac_marginal_is_verified(self):
        self.assertTrue(self._dvl(target_format="flac", spectral_grade="marginal"))

    def test_flac_no_spectral_is_verified(self):
        """No spectral ran → FLAC on disk is still verified (it IS lossless)."""
        self.assertTrue(self._dvl(target_format="flac", spectral_grade=None))

    def test_flac_suspect_is_not_verified(self):
        self.assertFalse(self._dvl(target_format="flac", spectral_grade="suspect"))

    def test_flac_likely_transcode_is_not_verified(self):
        self.assertFalse(self._dvl(target_format="flac",
                                   spectral_grade="likely_transcode"))

    def test_flac_ignores_converted_count(self):
        """FLAC path doesn't need conversion to prove lossless."""
        self.assertTrue(self._dvl(target_format="flac", spectral_grade="genuine",
                                  converted_count=0))

    # --- Standard conversion path ---

    def test_converted_genuine_is_verified(self):
        self.assertTrue(self._dvl(converted_count=12, is_transcode=False))

    def test_converted_transcode_is_not_verified(self):
        self.assertFalse(self._dvl(converted_count=12, is_transcode=True))

    def test_not_converted_is_not_verified(self):
        self.assertFalse(self._dvl(converted_count=0, is_transcode=False))

    def test_spectral_irrelevant_for_standard_path(self):
        """Standard path uses is_transcode (derived from spectral), not spectral directly."""
        self.assertTrue(self._dvl(spectral_grade="suspect",
                                  converted_count=12, is_transcode=False))

    # --- "lossless" target_format (same as "flac") ---

    def test_lossless_genuine_is_verified(self):
        self.assertTrue(self._dvl(target_format="lossless", spectral_grade="genuine"))

    def test_lossless_no_spectral_is_verified(self):
        self.assertTrue(self._dvl(target_format="lossless", spectral_grade=None))

    def test_lossless_suspect_is_not_verified(self):
        self.assertFalse(self._dvl(target_format="lossless", spectral_grade="suspect"))

    # --- V0-avg trust override ---
    # When spectral disagrees with V0 evidence (the spoken-word and
    # sparse-HF-music false-positive case — Bill Hicks 1990 "Dangerous"
    # being the canonical example), trust the V0 probe. A
    # ``lossless_source_v0`` probe with avg ≥ 230kbps AND min ≥ 200kbps is
    # strong evidence the source carried genuine HF complexity that LAME
    # couldn't throw away — i.e. a real lossless master, not a fake-FLAC.
    # Below those thresholds, we defer to spectral as before.

    def _v0(self, kind="lossless_source_v0", avg=None, min=None, median=None):
        from lib.quality import V0ProbeEvidence
        return V0ProbeEvidence(
            kind=kind, avg_bitrate_kbps=avg, min_bitrate_kbps=min,
            median_bitrate_kbps=median,
        )

    def test_v0_override_bill_hicks_shape_verifies_despite_suspect(self):
        """Real case: ALAC of Bill Hicks comedy. spectral=suspect (speech has
        no HF), V0 probe avg=241/min=219 (genuine lossless source).
        is_transcode=True (set by transcode_detection seeing 'suspect').
        Expect: V0 override flips verified_lossless to True."""
        self.assertTrue(self._dvl(
            converted_count=10, is_transcode=True,
            spectral_grade="suspect",
            v0_probe=self._v0(avg=241, min=219, median=239),
        ))

    def test_v0_override_fake_flac_shape_stays_unverified(self):
        """Fake-FLAC of 128k MP3 source: V0 probe avg=190/min=180. Below
        either threshold → override does NOT fire → stays unverified."""
        self.assertFalse(self._dvl(
            converted_count=10, is_transcode=True,
            spectral_grade="suspect",
            v0_probe=self._v0(avg=190, min=180, median=185),
        ))

    def test_v0_override_min_below_floor_stays_unverified(self):
        """Mixed album: 9 great tracks + 1 transcoded track. avg=240 passes
        but min=110 fails the floor. One bad track must not whitelist the
        whole album."""
        self.assertFalse(self._dvl(
            converted_count=10, is_transcode=True,
            spectral_grade="suspect",
            v0_probe=self._v0(avg=240, min=110, median=235),
        ))

    def test_v0_override_boundary_inclusive(self):
        """Thresholds are inclusive: avg=230 AND min=200 must pass."""
        self.assertTrue(self._dvl(
            converted_count=10, is_transcode=True,
            spectral_grade="suspect",
            v0_probe=self._v0(avg=230, min=200, median=215),
        ))

    def test_v0_override_avg_just_below_threshold(self):
        """avg=229 (below 230) → override does not fire."""
        self.assertFalse(self._dvl(
            converted_count=10, is_transcode=True,
            spectral_grade="suspect",
            v0_probe=self._v0(avg=229, min=210, median=220),
        ))

    def test_v0_override_wrong_probe_kind_ignored(self):
        """Only lossless_source_v0 probes count. native_lossy_research_v0
        and on_disk_research_v0 are research evidence, not policy input."""
        self.assertFalse(self._dvl(
            converted_count=10, is_transcode=True,
            spectral_grade="suspect",
            v0_probe=self._v0(kind="native_lossy_research_v0",
                              avg=241, min=219, median=239),
        ))

    def test_v0_override_no_probe_falls_back_to_legacy(self):
        """No V0 probe → behaves exactly as before. is_transcode wins."""
        self.assertFalse(self._dvl(
            converted_count=10, is_transcode=True,
            spectral_grade="suspect",
            v0_probe=None,
        ))

    def test_v0_override_partial_probe_data_stays_unverified(self):
        """V0 probe with avg high but min=None → override does not fire.
        Partial probe data must not pass the floor check by accident."""
        self.assertFalse(self._dvl(
            converted_count=10, is_transcode=True,
            spectral_grade="suspect",
            v0_probe=self._v0(avg=241, min=None, median=239),
        ))

    def test_v0_override_applies_to_lossless_on_disk_path(self):
        """target_format='flac' + spectral=suspect + high V0 → override
        flips verified to True for the keep-on-disk path too. A genuine
        spoken-word FLAC kept on disk should be verified the same way."""
        self.assertTrue(self._dvl(
            target_format="flac", spectral_grade="suspect",
            v0_probe=self._v0(avg=241, min=219, median=239),
        ))

    def test_v0_override_does_not_unfire_when_already_verified(self):
        """Override is monotonic — it can only flip False→True. A normal
        verified import (is_transcode=False) stays verified regardless of
        V0 probe shape (even when V0 is missing or low)."""
        self.assertTrue(self._dvl(
            converted_count=10, is_transcode=False,
            spectral_grade="genuine",
            v0_probe=self._v0(avg=180, min=170, median=175),
        ))


# ============================================================================
# Lossless virtual tier matching
# ============================================================================

class TestLosslessTierMatching(unittest.TestCase):
    """The 'lossless' tier should match flac, alac, and wav files."""

    def test_lossless_matches_flac(self):
        from lib.quality import parse_filetype_config, file_identity, filetype_matches
        lossless = parse_filetype_config("lossless")
        flac = file_identity({"filename": "track.flac", "bitRate": 900})
        self.assertTrue(filetype_matches(flac, lossless))

    def test_lossless_matches_alac(self):
        from lib.quality import parse_filetype_config, file_identity, filetype_matches
        lossless = parse_filetype_config("lossless")
        alac = file_identity({"filename": "track.m4a", "bitRate": 900, "bitDepth": 16})
        self.assertTrue(filetype_matches(alac, lossless))

    def test_lossless_matches_wav(self):
        from lib.quality import parse_filetype_config, file_identity, filetype_matches
        lossless = parse_filetype_config("lossless")
        wav = file_identity({"filename": "track.wav", "bitRate": 1411})
        self.assertTrue(filetype_matches(wav, lossless))

    def test_lossless_rejects_mp3(self):
        from lib.quality import parse_filetype_config, file_identity, filetype_matches
        lossless = parse_filetype_config("lossless")
        mp3 = file_identity({"filename": "track.mp3", "bitRate": 320})
        self.assertFalse(filetype_matches(mp3, lossless))

    def test_lossless_rejects_opus(self):
        from lib.quality import parse_filetype_config, file_identity, filetype_matches
        lossless = parse_filetype_config("lossless")
        opus = file_identity({"filename": "track.opus", "bitRate": 128})
        self.assertFalse(filetype_matches(opus, lossless))

    def test_lossless_rejects_aac(self):
        """AAC (lossy m4a) should not match lossless tier."""
        from lib.quality import parse_filetype_config, file_identity, filetype_matches
        lossless = parse_filetype_config("lossless")
        aac = file_identity({"filename": "track.m4a", "bitRate": 256})
        self.assertFalse(filetype_matches(aac, lossless))

    def test_verify_filetype_lossless_flac(self):
        """verify_filetype with 'lossless' spec matches FLAC."""
        from lib.quality import verify_filetype
        self.assertTrue(verify_filetype(
            {"filename": "track.flac", "bitRate": 900}, "lossless"))

    def test_verify_filetype_lossless_wav(self):
        from lib.quality import verify_filetype
        self.assertTrue(verify_filetype(
            {"filename": "track.wav", "bitRate": 1411}, "lossless"))

    def test_verify_filetype_lossless_rejects_mp3(self):
        from lib.quality import verify_filetype
        self.assertFalse(verify_filetype(
            {"filename": "track.mp3", "bitRate": 320}, "lossless"))


# ============================================================================
# FLAC_SPEC and conversion_target with "lossless"
# ============================================================================

class TestFlacSpec(unittest.TestCase):
    def test_flac_spec_values(self):
        from harness.import_one import FLAC_SPEC
        self.assertEqual(FLAC_SPEC.codec, "flac")
        self.assertEqual(FLAC_SPEC.codec_args, ())
        self.assertEqual(FLAC_SPEC.extension, "flac")

    def test_conversion_target_lossless(self):
        """target_format='lossless' should return 'lossless' (keep on disk)."""
        from harness.import_one import conversion_target
        self.assertEqual(
            conversion_target("lossless", True, "opus 128"), "lossless")

    def test_conversion_target_flac_backward_compat(self):
        """target_format='flac' still works (backward compat with old DB rows)."""
        from harness.import_one import conversion_target
        self.assertEqual(
            conversion_target("flac", True, "opus 128"), "lossless")


# ============================================================================
# E2E conversion tests — real files through convert_lossless
# ============================================================================

@unittest.skipUnless(shutil.which("sox"), "sox not available")
class TestConvertLosslessE2E(unittest.TestCase):
    """Generate real FLAC files, convert with ConversionSpec, verify disk state."""

    def _count_by_ext(self, directory):
        """Count files by extension in a directory."""
        counts = {}
        for f in os.listdir(directory):
            ext = os.path.splitext(f)[1].lower()
            counts[ext] = counts.get(ext, 0) + 1
        return counts

    def _get_codec_name(self, path):
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name", "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=30,
        )
        return result.stdout.strip().lower()

    def test_v0_conversion_genuine(self):
        """Genuine FLAC → V0: only .mp3 files on disk, bitrate > 210."""
        from harness.import_one import convert_lossless, V0_SPEC
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=15500)
            converted, failed, orig_ext = convert_lossless(album, V0_SPEC)
            self.assertEqual(converted, 2)
            self.assertEqual(failed, 0)
            self.assertEqual(orig_ext, "flac")
            exts = self._count_by_ext(album)
            self.assertEqual(exts.get(".mp3", 0), 2)
            self.assertNotIn(".flac", exts, "FLAC files should be removed")
            # Check bitrate
            for f in os.listdir(album):
                if f.endswith(".mp3"):
                    br = get_bitrate_kbps(os.path.join(album, f))
                    self.assertGreater(br, 210)

    def test_v0_conversion_transcode(self):
        """Transcode FLAC → V0: .mp3 on disk, bitrate < 210."""
        from harness.import_one import convert_lossless, V0_SPEC
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=12000)
            converted, failed, orig_ext = convert_lossless(album, V0_SPEC)
            self.assertEqual(converted, 2)
            self.assertEqual(failed, 0)
            for f in os.listdir(album):
                if f.endswith(".mp3"):
                    br = get_bitrate_kbps(os.path.join(album, f))
                    self.assertLess(br, 210)

    def test_v0_keep_source(self):
        """keep_source=True preserves FLAC alongside MP3."""
        from harness.import_one import convert_lossless, V0_SPEC
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=15500)
            convert_lossless(album, V0_SPEC, keep_source=True)
            exts = self._count_by_ext(album)
            self.assertEqual(exts.get(".mp3", 0), 2)
            self.assertEqual(exts.get(".flac", 0), 2, "FLAC should be preserved")

    def test_opus_128_conversion(self):
        """FLAC → Opus 128: only .opus files on disk."""
        from harness.import_one import convert_lossless, parse_verified_lossless_target
        spec = parse_verified_lossless_target("opus 128")
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=15500)
            converted, failed, orig_ext = convert_lossless(album, spec)
            self.assertEqual(converted, 2)
            self.assertEqual(failed, 0)
            exts = self._count_by_ext(album)
            self.assertEqual(exts.get(".opus", 0), 2)
            self.assertNotIn(".flac", exts)

    def test_mp3_v2_conversion(self):
        """FLAC → MP3 V2: .mp3 files, bitrate lower than V0."""
        from harness.import_one import convert_lossless, parse_verified_lossless_target
        spec = parse_verified_lossless_target("mp3 v2")
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=15500)
            converted, failed, _ = convert_lossless(album, spec)
            self.assertEqual(converted, 2)
            exts = self._count_by_ext(album)
            self.assertEqual(exts.get(".mp3", 0), 2)
            self.assertNotIn(".flac", exts)

    def test_aac_128_conversion(self):
        """FLAC → AAC 128: .m4a files on disk."""
        from harness.import_one import convert_lossless, parse_verified_lossless_target
        spec = parse_verified_lossless_target("aac 128")
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=15500)
            converted, failed, _ = convert_lossless(album, spec)
            self.assertEqual(converted, 2)
            exts = self._count_by_ext(album)
            self.assertEqual(exts.get(".m4a", 0), 2)
            self.assertNotIn(".flac", exts)

    def test_aac_target_handles_alac_same_extension_collision(self):
        """ALAC .m4a → AAC .m4a should replace the source, not skip it."""
        from harness.import_one import (
            V0_SPEC,
            _remove_files_by_ext,
            _remove_lossless_files,
            convert_lossless,
            parse_verified_lossless_target,
        )
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            os.makedirs(album)
            src = os.path.join(album, "01 - Track 1.m4a")
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                 "-c:a", "alac", "-y", src],
                capture_output=True, check=True, timeout=30,
            )

            converted, failed, _ = convert_lossless(album, V0_SPEC, keep_source=True)
            self.assertEqual((converted, failed), (1, 0))

            target_spec = parse_verified_lossless_target("aac 128")
            converted, failed, _ = convert_lossless(album, target_spec, keep_source=True)
            self.assertEqual((converted, failed), (1, 0))

            _remove_files_by_ext(album, "." + V0_SPEC.extension)
            _remove_lossless_files(album)

            files = sorted(os.listdir(album))
            self.assertEqual(files, ["01 - Track 1.m4a"])
            self.assertEqual(self._get_codec_name(os.path.join(album, files[0])), "aac")

    def test_wav_to_flac_normalization(self):
        """WAV → FLAC via FLAC_SPEC: .flac files on disk, WAV removed."""
        from harness.import_one import convert_lossless, FLAC_SPEC
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            os.makedirs(album)
            wav = os.path.join(album, "01 - Track 1.wav")
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                 "-y", wav],
                capture_output=True, check=True, timeout=30)
            converted, failed, orig_ext = convert_lossless(album, FLAC_SPEC)
            self.assertEqual(converted, 1)
            self.assertEqual(failed, 0)
            self.assertEqual(orig_ext, "wav")
            exts = self._count_by_ext(album)
            self.assertEqual(exts.get(".flac", 0), 1)
            self.assertNotIn(".wav", exts)

    def test_alac_to_flac_normalization(self):
        """ALAC .m4a → FLAC via FLAC_SPEC: .flac on disk, .m4a removed."""
        from harness.import_one import convert_lossless, FLAC_SPEC
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            os.makedirs(album)
            src = os.path.join(album, "01 - Track 1.m4a")
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                 "-c:a", "alac", "-y", src],
                capture_output=True, check=True, timeout=30)
            converted, failed, orig_ext = convert_lossless(album, FLAC_SPEC)
            self.assertEqual(converted, 1)
            self.assertEqual(orig_ext, "m4a")
            exts = self._count_by_ext(album)
            self.assertEqual(exts.get(".flac", 0), 1)
            self.assertNotIn(".m4a", exts)
            # Verify it's actually FLAC codec
            flac_file = os.path.join(album, "01 - Track 1.flac")
            self.assertEqual(self._get_codec_name(flac_file), "flac")

    def test_flac_to_flac_recompresses(self):
        """FLAC → FLAC via FLAC_SPEC: re-compresses (lossless, no quality loss).

        This is harmless — the normalization path in import_one.py main()
        only calls FLAC_SPEC for non-FLAC sources (ALAC/WAV).
        """
        from harness.import_one import convert_lossless, FLAC_SPEC
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=1, cutoff_hz=15500)
            converted, failed, _ = convert_lossless(album, FLAC_SPEC)
            self.assertEqual(converted, 1)  # re-compresses via temp file
            exts = self._count_by_ext(album)
            self.assertEqual(exts.get(".flac", 0), 1)

    def test_no_lossless_files_noop(self):
        """Directory with only MP3s → no conversion."""
        from harness.import_one import convert_lossless, V0_SPEC
        with tempfile.TemporaryDirectory() as d:
            # Create a fake mp3
            with open(os.path.join(d, "track.mp3"), "w") as f:
                f.write("not real")
            converted, failed, orig_ext = convert_lossless(d, V0_SPEC)
            self.assertEqual(converted, 0)
            self.assertEqual(failed, 0)
            self.assertIsNone(orig_ext)

    def test_dry_run_no_output(self):
        """Dry run should not create output files."""
        from harness.import_one import convert_lossless, V0_SPEC
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=1, cutoff_hz=15500)
            converted, failed, _ = convert_lossless(album, V0_SPEC, dry_run=True)
            self.assertEqual(converted, 1)
            exts = self._count_by_ext(album)
            self.assertNotIn(".mp3", exts, "Dry run should not create files")
            self.assertEqual(exts.get(".flac", 0), 1, "Source should remain")


# ============================================================================
# Full pipeline decision tests — real files through decision chain
# ============================================================================

@unittest.skipUnless(shutil.which("sox"), "sox not available")
class TestConversionPipelineE2E(unittest.TestCase):
    """Exercise the full decision chain with real files.

    Generate FLAC → convert → measure → run decision functions →
    verify the decision matches what the simulator would predict.
    """

    def test_genuine_flac_default_is_verified_lossless(self):
        """Genuine FLAC → V0 → bitrate > 210 → verified lossless."""
        from harness.import_one import convert_lossless, V0_SPEC
        from lib.quality import (determine_verified_lossless,
                                 transcode_detection)
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=15500)
            converted, failed, _ = convert_lossless(album, V0_SPEC)

            # Measure V0 bitrate
            min_br = None
            for f in os.listdir(album):
                if f.endswith(".mp3"):
                    br = get_bitrate_kbps(os.path.join(album, f))
                    if min_br is None or br < min_br:
                        min_br = br

            # Decision chain
            is_transcode = transcode_detection(
                converted, min_br, spectral_grade="genuine")
            self.assertFalse(is_transcode)

            verified = determine_verified_lossless(
                None, "genuine", converted, is_transcode)
            self.assertTrue(verified)

    def test_transcode_flac_not_verified(self):
        """Transcode FLAC → V0 → bitrate < 210 → NOT verified lossless."""
        from harness.import_one import convert_lossless, V0_SPEC
        from lib.quality import (determine_verified_lossless,
                                 transcode_detection)
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=12000)
            converted, failed, _ = convert_lossless(album, V0_SPEC)

            min_br = None
            for f in os.listdir(album):
                if f.endswith(".mp3"):
                    br = get_bitrate_kbps(os.path.join(album, f))
                    if min_br is None or br < min_br:
                        min_br = br

            is_transcode = transcode_detection(
                converted, min_br, spectral_grade="suspect")
            self.assertTrue(is_transcode)

            verified = determine_verified_lossless(
                None, "suspect", converted, is_transcode)
            self.assertFalse(verified)

    def test_genuine_flac_with_target_converts_twice(self):
        """Genuine FLAC → V0 (verify) → Opus 128 (final): only .opus on disk."""
        from harness.import_one import (convert_lossless, V0_SPEC,
                                parse_verified_lossless_target)
        from lib.quality import (determine_verified_lossless,
                                 transcode_detection)
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=15500)

            # Step 1: V0 verification (keep source for second pass)
            converted, _, _ = convert_lossless(album, V0_SPEC, keep_source=True)

            # Measure V0 for verification
            v0_bitrates = []
            for f in os.listdir(album):
                if f.endswith(".mp3"):
                    v0_bitrates.append(get_bitrate_kbps(os.path.join(album, f)))
            v0_min = min(v0_bitrates)
            self.assertGreater(v0_min, 210)

            # Step 2: Decision — verified lossless, convert to target
            is_transcode = transcode_detection(
                converted, v0_min, spectral_grade="genuine")
            verified = determine_verified_lossless(
                None, "genuine", converted, is_transcode)
            self.assertTrue(verified)

            # Step 3: Convert FLAC → Opus (from originals, not V0)
            spec = parse_verified_lossless_target("opus 128")
            opus_converted, opus_failed, _ = convert_lossless(album, spec)
            self.assertEqual(opus_converted, 2)

            # Step 4: Clean up V0 (ephemeral) + FLAC (consumed)
            for f in os.listdir(album):
                fp = os.path.join(album, f)
                if f.endswith(".mp3") or f.endswith(".flac"):
                    os.remove(fp)

            # Verify final state: only opus
            exts = {}
            for f in os.listdir(album):
                ext = os.path.splitext(f)[1].lower()
                exts[ext] = exts.get(ext, 0) + 1
            self.assertEqual(exts, {".opus": 2})

    def test_transcode_flac_with_target_skips_second_conversion(self):
        """Transcode FLAC + target configured → keep V0, skip target conversion."""
        from harness.import_one import convert_lossless, V0_SPEC
        from lib.quality import (determine_verified_lossless,
                                 transcode_detection)
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=12000)

            # V0 verification (keep source because target was configured)
            converted, _, _ = convert_lossless(album, V0_SPEC, keep_source=True)

            v0_min = None
            for f in os.listdir(album):
                if f.endswith(".mp3"):
                    br = get_bitrate_kbps(os.path.join(album, f))
                    if v0_min is None or br < v0_min:
                        v0_min = br

            is_transcode = transcode_detection(
                converted, v0_min, spectral_grade="suspect")
            self.assertTrue(is_transcode)

            verified = determine_verified_lossless(
                None, "suspect", converted, is_transcode)
            self.assertFalse(verified)
            # Target conversion skipped — clean up kept FLAC
            for f in os.listdir(album):
                if f.endswith(".flac"):
                    os.remove(os.path.join(album, f))

            # Final state: only V0 MP3
            exts = {}
            for f in os.listdir(album):
                ext = os.path.splitext(f)[1].lower()
                exts[ext] = exts.get(ext, 0) + 1
            self.assertEqual(exts, {".mp3": 2})


    def test_mp3_v2_target_same_extension_as_v0(self):
        """MP3 V2 target has same .mp3 extension as V0 — must remove V0 first.

        Regression test: without removing V0 .mp3 files before the target
        conversion, convert_lossless() would skip all files (output exists)
        and leave zero audio files after cleanup.
        """
        from harness.import_one import (convert_lossless, V0_SPEC,
                                parse_verified_lossless_target,
                                _remove_files_by_ext, _remove_lossless_files)
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            make_test_album(album, track_count=2, cutoff_hz=15500)

            # Step 1: V0 verification (keep source)
            convert_lossless(album, V0_SPEC, keep_source=True)

            # Step 2: Remove V0 before target conversion (same extension)
            target_spec = parse_verified_lossless_target("mp3 v2")
            self.assertEqual(target_spec.extension, V0_SPEC.extension)
            _remove_files_by_ext(album, "." + V0_SPEC.extension)

            # Step 3: Convert FLAC → MP3 V2
            converted, failed, _ = convert_lossless(album, target_spec,
                                                    keep_source=True)
            self.assertEqual(converted, 2)
            self.assertEqual(failed, 0)

            # Step 4: Clean up FLAC
            _remove_lossless_files(album)

            # Final state: only MP3 V2 files
            exts = {}
            for f in os.listdir(album):
                ext = os.path.splitext(f)[1].lower()
                exts[ext] = exts.get(ext, 0) + 1
            self.assertEqual(exts, {".mp3": 2})

            # V2 bitrate should be lower than V0 (typically ~190 vs ~236)
            for f in os.listdir(album):
                if f.endswith(".mp3"):
                    br = get_bitrate_kbps(os.path.join(album, f))
                    self.assertLess(br, 220,
                                    f"V2 bitrate {br}kbps seems too high for V2")


# ============================================================================
# Native lossy V0 research probe — temp re-encode of lossy candidates
# ============================================================================

@unittest.skipUnless(shutil.which("sox") and shutil.which("ffmpeg"),
                     "sox or ffmpeg not available")
class TestProbeNativeLossyAsV0(unittest.TestCase):
    """`_probe_native_lossy_as_v0` re-encodes a candidate's lossy audio files
    to V0 in a temp dir and returns research-kind probe evidence. It must
    never claim the lossless-source kind."""

    def _make_lossy_album(self, album_dir, *, cutoff_hz, codec_args, ext,
                          track_count=2):
        """Generate FLAC fixture and re-encode each track to a lossy format,
        leaving only the lossy files in album_dir."""
        os.makedirs(album_dir, exist_ok=True)
        for i in range(1, track_count + 1):
            flac_path = os.path.join(album_dir, f"{i:02d} - Track {i}.flac")
            from tests.audio_fixtures import make_test_flac
            make_test_flac(flac_path, cutoff_hz=cutoff_hz)
            out_path = os.path.join(album_dir, f"{i:02d} - Track {i}.{ext}")
            subprocess.run(
                ["ffmpeg", "-i", flac_path, *codec_args, "-y", out_path],
                capture_output=True, check=True, timeout=30)
            os.remove(flac_path)

    def test_genuine_320_mp3_yields_research_probe(self):
        """Real-content MP3 320 → research probe with sensible avg bitrate.
        Mirrors the somafalls 320 case from request 2257."""
        from harness.import_one import _probe_native_lossy_as_v0
        from lib.quality import V0_PROBE_NATIVE_LOSSY_RESEARCH
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            self._make_lossy_album(
                album, cutoff_hz=15500,
                codec_args=["-c:a", "libmp3lame", "-b:a", "320k"],
                ext="mp3")
            probe = _probe_native_lossy_as_v0(album)
            self.assertIsNotNone(probe)
            assert probe is not None  # for type narrowing
            self.assertEqual(probe.kind, V0_PROBE_NATIVE_LOSSY_RESEARCH)
            self.assertIsNotNone(probe.avg_bitrate_kbps)
            self.assertIsNotNone(probe.min_bitrate_kbps)
            self.assertIsNotNone(probe.median_bitrate_kbps)
            # Genuine content re-encoded to V0 produces ~190-260kbps.
            assert probe.avg_bitrate_kbps is not None
            self.assertGreater(probe.avg_bitrate_kbps, 150)
            self.assertLess(probe.avg_bitrate_kbps, 320)

    def test_returns_none_when_no_lossy_files(self):
        """Empty/lossless-only folder → None (nothing to probe)."""
        from harness.import_one import _probe_native_lossy_as_v0
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            from tests.audio_fixtures import make_test_album
            make_test_album(album, track_count=1, cutoff_hz=15500)
            self.assertIsNone(_probe_native_lossy_as_v0(album))

    def test_returns_none_for_empty_dir(self):
        from harness.import_one import _probe_native_lossy_as_v0
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            os.makedirs(album)
            self.assertIsNone(_probe_native_lossy_as_v0(album))

    def test_research_probe_does_not_pass_comparable_guard(self):
        """Decision purity: a research probe must not be eligible for
        provisional_lossless_decision comparison."""
        from harness.import_one import _probe_native_lossy_as_v0
        from lib.quality import is_comparable_lossless_source_probe
        with tempfile.TemporaryDirectory() as d:
            album = os.path.join(d, "album")
            self._make_lossy_album(
                album, cutoff_hz=15500,
                codec_args=["-c:a", "libmp3lame", "-b:a", "320k"],
                ext="mp3")
            probe = _probe_native_lossy_as_v0(album)
            self.assertIsNotNone(probe)
            self.assertFalse(is_comparable_lossless_source_probe(probe))


if __name__ == "__main__":
    unittest.main()
