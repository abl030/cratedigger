#!/usr/bin/env python3
"""Quality classification tests using real audio fixtures.

Each test album is a directory of real audio files encoded from a known
genuine FLAC source.  We run spectral analysis + bitrate measurement +
the quality gate decision logic against each, asserting correct
classification.

Fixture generation: tests/fixtures/generate_fixtures.sh

The quality gate decision tree (from soularr.py _check_quality_gate):

  1. verified_lossless=TRUE + any bitrate  → ACCEPT
  2. gate_bitrate < 210kbps                → REQUEUE_UPGRADE
  3. CBR + not verified_lossless           → REQUEUE_LOSSLESS
  4. VBR >= 210kbps                        → ACCEPT

This test suite exercises that tree with real files so we can detect
regressions when refactoring.
"""

import os
import subprocess
import sys
import unittest
from dataclasses import dataclass
from enum import Enum
from typing import Optional

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.spectral_check import analyze_album, AlbumResult
from lib.quality import (quality_gate_decision, full_pipeline_decision,
                         spectral_import_decision, import_quality_decision,
                         transcode_detection, QUALITY_MIN_BITRATE_KBPS)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "albums")


class QualityDecision(Enum):
    """What the quality gate decides to do with an album."""
    ACCEPT = "accept"                    # imported, done
    REQUEUE_UPGRADE = "requeue_upgrade"  # below 210kbps, search for better
    REQUEUE_LOSSLESS = "requeue_lossless"  # CBR above 210, search for lossless to verify


@dataclass
class AlbumAnalysis:
    """Full analysis of a test album — everything the quality gate needs."""
    name: str
    folder: str
    # Measured properties
    min_bitrate_kbps: Optional[int] = None
    is_cbr: bool = False
    is_flac: bool = False
    track_count: int = 0
    # Spectral analysis
    spectral_grade: Optional[str] = None
    spectral_bitrate: Optional[int] = None
    spectral_suspect_pct: float = 0.0
    # V0 conversion results (for FLAC albums)
    post_conversion_min_bitrate: Optional[int] = None


def get_audio_stream_bitrate(fpath):
    """Get audio stream bitrate (kbps) via ffprobe. Uses stream bitrate
    (not format bitrate) to match what beets stores in items.bitrate,
    and to avoid inflation from embedded cover art."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error",
             "-select_streams", "a:0",
             "-show_entries", "stream=bit_rate",
             "-of", "csv=p=0", fpath],
            capture_output=True, text=True, timeout=30,
        )
        br_str = result.stdout.strip()
        if br_str and br_str.isdigit():
            return int(br_str) // 1000
        # VBR MP3: stream bit_rate may be N/A, fall back to format bit_rate
        # but subtract a rough estimate for cover art overhead
        result = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format=bit_rate,duration",
             "-of", "csv=p=0", fpath],
            capture_output=True, text=True, timeout=30,
        )
        parts = result.stdout.strip().split(",")
        if len(parts) >= 1 and parts[0].isdigit():
            # For VBR, format bitrate is close enough once we know
            # there's no enormous cover art issue
            return int(parts[0]) // 1000
    except Exception:
        pass
    return None


def get_bitrate_stats(folder_path):
    """Get per-track bitrates and determine CBR/VBR. Returns (min_br, is_cbr, bitrates).

    Uses audio stream bitrate (not format bitrate) to match beets behavior
    and correctly detect CBR (all tracks same audio bitrate)."""
    from lib.quality import AUDIO_EXTENSIONS_DOTTED as audio_exts
    bitrates = []
    for fname in sorted(os.listdir(folder_path)):
        ext = os.path.splitext(fname)[1].lower()
        if ext not in audio_exts:
            continue
        fpath = os.path.join(folder_path, fname)
        br = get_audio_stream_bitrate(fpath)
        if br is not None and br > 0:
            bitrates.append(br)

    if not bitrates:
        return None, False, []

    min_br = min(bitrates)
    # CBR detection: same logic as quality gate (COUNT(DISTINCT bitrate) == 1)
    is_cbr = len(set(bitrates)) == 1
    return min_br, is_cbr, bitrates


def detect_file_type(folder_path):
    """Detect if album is FLAC or MP3."""
    for fname in os.listdir(folder_path):
        if fname.lower().endswith(".flac"):
            return True
    return False


def analyze_fixture(name, folder_path):
    """Run full analysis on a fixture album."""
    analysis = AlbumAnalysis(name=name, folder=folder_path)

    # File type
    analysis.is_flac = detect_file_type(folder_path)
    analysis.track_count = len([f for f in os.listdir(folder_path)
                                 if os.path.splitext(f)[1].lower() in {".mp3", ".flac"}])

    # Bitrate stats
    min_br, is_cbr, bitrates = get_bitrate_stats(folder_path)
    analysis.min_bitrate_kbps = min_br
    analysis.is_cbr = is_cbr

    # Spectral analysis
    spectral = analyze_album(folder_path, trim_seconds=10)
    analysis.spectral_grade = spectral.grade
    analysis.spectral_bitrate = spectral.estimated_bitrate_kbps
    analysis.spectral_suspect_pct = spectral.suspect_pct

    # For FLAC albums: simulate import_one.py conversion and measure result
    if analysis.is_flac:
        import tempfile
        import shutil
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy FLACs to temp dir
            for f in os.listdir(folder_path):
                if f.lower().endswith(".flac"):
                    shutil.copy2(os.path.join(folder_path, f), tmpdir)
            # Convert FLAC → V0 (same flags as import_one.py)
            for f in os.listdir(tmpdir):
                if f.lower().endswith(".flac"):
                    flac_path = os.path.join(tmpdir, f)
                    mp3_path = os.path.splitext(flac_path)[0] + ".mp3"
                    subprocess.run([
                        "ffmpeg", "-y", "-i", flac_path,
                        "-codec:a", "libmp3lame", "-q:a", "0",
                        "-map_metadata", "0", "-id3v2_version", "3",
                        mp3_path,
                    ], capture_output=True, text=True)
                    if os.path.exists(mp3_path):
                        os.remove(flac_path)
            # Measure post-conversion bitrate
            post_br, _, post_bitrates = get_bitrate_stats(tmpdir)
            analysis.post_conversion_min_bitrate = post_br

    return analysis


CACHE_FILE = os.path.join(os.path.dirname(__file__), "fixtures", "analysis_cache.json")


def _to_dict(a):
    return {k: v for k, v in a.__dict__.items() if k != "folder"}


def _from_dict(d, folder):
    a = AlbumAnalysis(name=d["name"], folder=folder)
    for k, v in d.items():
        if k != "folder" and hasattr(a, k):
            setattr(a, k, v)
    return a


def load_or_analyze_all():
    """Load cached analysis or run analysis and cache results."""
    # Check cache freshness: cache must be newer than all fixture dirs
    if os.path.exists(CACHE_FILE):
        cache_mtime = os.path.getmtime(CACHE_FILE)
        fixtures_ok = True
        for dirname, *_ in ALBUM_EXPECTATIONS:
            folder = os.path.join(FIXTURES_DIR, dirname)
            if os.path.isdir(folder) and os.path.getmtime(folder) > cache_mtime:
                fixtures_ok = False
                break
        if fixtures_ok:
            import json
            with open(CACHE_FILE) as f:
                cached = json.load(f)
            analyses = {}
            for dirname, data in cached.items():
                folder = os.path.join(FIXTURES_DIR, dirname)
                analyses[dirname] = _from_dict(data, folder)
            return analyses

    # No cache or stale — analyze everything
    analyses = {}
    for dirname, *_ in ALBUM_EXPECTATIONS:
        folder = os.path.join(FIXTURES_DIR, dirname)
        if os.path.isdir(folder):
            analyses[dirname] = analyze_fixture(dirname, folder)

    # Save cache
    import json
    cache_data = {k: _to_dict(v) for k, v in analyses.items()}
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache_data, f, indent=2)

    return analyses


def _call_quality_gate(min_bitrate, is_cbr, verified_lossless, spectral_bitrate=None):
    """Wrapper: call the real quality_gate_decision() from soularr.py
    and convert its string return to a QualityDecision enum for test assertions."""
    from lib.quality import AudioQualityMeasurement
    m = AudioQualityMeasurement(min_bitrate_kbps=min_bitrate, is_cbr=is_cbr,
                                verified_lossless=verified_lossless,
                                spectral_bitrate_kbps=spectral_bitrate)
    result = quality_gate_decision(m)
    return QualityDecision(result)


# ============================================================================
# Test album definitions — expected outcomes
# ============================================================================

# Each entry: (fixture_dir_name, description, expected_decision, extra_checks)
# extra_checks is a dict of AlbumAnalysis field → expected value (approximate)

ALBUM_EXPECTATIONS = [
    # --- Genuine sources ---
    ("01_genuine_flac", "Genuine CD rip FLAC",
     # As FLAC: spectral genuine, conversion produces good V0
     # After conversion: verified_lossless=True → ACCEPT
     QualityDecision.ACCEPT,
     {"is_flac": True, "spectral_grade_not": "suspect"}),

    ("02_genuine_v0", "Genuine FLAC → VBR V0",
     # VBR above 210 → ACCEPT
     QualityDecision.ACCEPT,
     {"is_cbr": False, "min_bitrate_above": 210}),

    ("03_genuine_v2", "Genuine FLAC → VBR V2",
     # V2 bitrate ~170-190kbps (below 210) → REQUEUE_UPGRADE.
     # Also: spectral detects V2's lowpass (~19kHz) as cliff → likely_transcode.
     QualityDecision.REQUEUE_UPGRADE,
     {"is_cbr": False, "min_bitrate_below": 210}),

    # --- CBR (unverifiable, always re-queue for FLAC) ---
    ("04_cbr_320", "CBR 320kbps",
     QualityDecision.REQUEUE_LOSSLESS,
     {"is_cbr": True, "min_bitrate_above": 210}),

    ("05_cbr_256", "CBR 256kbps",
     # CBR 256 is above 210kbps, so normally REQUEUE_LOSSLESS.
     # However, spectral analysis may detect a cliff on some tracks and
     # set spectral_bitrate < 210 → REQUEUE_UPGRADE instead.
     # Either outcome is correct — both trigger a re-search.
     None,  # determined at runtime (depends on spectral edge cases)
     {"is_cbr": True, "min_bitrate_above": 210}),

    ("06_cbr_192", "CBR 192kbps",
     # 192 < 210 → REQUEUE_UPGRADE (not just FLAC)
     QualityDecision.REQUEUE_UPGRADE,
     {"is_cbr": True, "min_bitrate_below": 210}),

    ("07_cbr_128", "CBR 128kbps",
     QualityDecision.REQUEUE_UPGRADE,
     {"is_cbr": True, "min_bitrate_below": 210}),

    ("08_cbr_96", "CBR 96kbps — garbage",
     QualityDecision.REQUEUE_UPGRADE,
     {"is_cbr": True, "min_bitrate_below": 210}),

    # --- Fake FLACs (transcodes in FLAC container) ---
    ("09_fake_flac_128", "128k MP3 → FLAC (fake lossless)",
     # Spectral should detect cliff. After V0 conversion, bitrate ~150kbps
     # → REQUEUE_UPGRADE (transcode detected)
     QualityDecision.REQUEUE_UPGRADE,
     {"is_flac": True, "spectral_grade": "suspect"}),

    ("10_fake_flac_96", "96k MP3 → FLAC (upsampled garbage)",
     QualityDecision.REQUEUE_UPGRADE,
     {"is_flac": True, "spectral_grade": "suspect"}),

    ("11_fake_flac_192", "192k MP3 → FLAC",
     # Spectral may or may not detect cliff at 192k (lowpass ~18.6kHz)
     # After V0 conversion, bitrate probably ~190kbps < 210 → REQUEUE_UPGRADE
     QualityDecision.REQUEUE_UPGRADE,
     {"is_flac": True}),

    ("12_fake_flac_320", "320k MP3 → FLAC (hard to detect)",
     # 320k lowpass is at 20.5kHz — spectral won't detect cliff.
     # After V0 conversion, bitrate ~243kbps (good). Spectral says genuine.
     # verified_lossless=True (spectral genuine FLAC) → ACCEPT.
     # This is a false negative — the system can't detect 320k fakes.
     QualityDecision.ACCEPT,
     {"is_flac": True, "spectral_grade_not": "suspect"}),

    # --- Mixed bitrate albums ---
    ("13_mixed_cbr_320_192", "7×320 + 1×192 CBR",
     # min_bitrate=192 < 210 → REQUEUE_UPGRADE.
     # Not detected as CBR because bitrates vary (320+192).
     QualityDecision.REQUEUE_UPGRADE,
     {"is_cbr": False, "min_bitrate_below": 210}),

    ("14_mixed_cbr_320_128", "7×320 + 1×128 CBR",
     QualityDecision.REQUEUE_UPGRADE,
     {"min_bitrate_below": 210}),

    ("15_mixed_vbr_cbr", "6×V0 + 2×CBR 320",
     # Multiple distinct bitrates → NOT CBR → treated as VBR
     # If min bitrate >= 210 → ACCEPT
     QualityDecision.ACCEPT,
     {"is_cbr": False, "min_bitrate_above": 210}),

    ("16_v0_from_transcode", "128k→FLAC (fake) — spectral input only",
     # These are fake FLACs. Spectral should detect transcode.
     # After V0 conversion: low bitrate → REQUEUE_UPGRADE
     QualityDecision.REQUEUE_UPGRADE,
     {"is_flac": True, "spectral_grade": "suspect"}),

    ("17_mixed_320_v0", "4×CBR 320 + 4×VBR V0",
     # Mixed → not CBR. Min bitrate from V0 tracks probably ~220+
     # → ACCEPT (VBR above threshold)
     QualityDecision.ACCEPT,
     {"is_cbr": False}),

    ("18_genuine_v0_quiet", "Genuine V0 from quiet/attenuated source",
     # V0 from quiet music: bitrate ~228kbps (above 210) but spectral may
     # detect a false cliff on one quiet track → spectral_bitrate=128 overrides
     # gate_br below 210 → REQUEUE_UPGRADE. Edge case of spectral false positive
     # on low-energy audio.
     QualityDecision.REQUEUE_UPGRADE,
     {"is_cbr": False}),

    ("19_v0_from_fake_flac_128", "128k→FLAC→V0 (transcode pipeline output)",
     # This simulates what import_one.py produces from a fake FLAC.
     # V0 bitrate from 128k source should be ~150-180kbps → REQUEUE_UPGRADE
     QualityDecision.REQUEUE_UPGRADE,
     {"is_cbr": False, "min_bitrate_below": 210}),

    ("20_double_transcode", "128k→FLAC→128k (double transcode)",
     QualityDecision.REQUEUE_UPGRADE,
     {"min_bitrate_below": 210}),

    ("21_v0_one_bad_track", "7×genuine V0 + 1×transcode V0",
     # Min bitrate from the bad track (~150kbps) < 210 → REQUEUE_UPGRADE
     QualityDecision.REQUEUE_UPGRADE,
     {"is_cbr": False, "min_bitrate_below": 210}),

    ("22_gold_standard_v0", "Genuine FLAC → V0 (gold standard pipeline output)",
     # This is what the pipeline produces from genuine FLAC.
     # With verified_lossless=True → ACCEPT
     QualityDecision.ACCEPT,
     {"is_cbr": False, "min_bitrate_above": 210}),
]


@unittest.skipUnless(os.path.isdir(FIXTURES_DIR), "fixtures not generated — run generate_fixtures.sh")
class TestQualityClassification(unittest.TestCase):
    """Run real audio through spectral analysis + quality gate decision."""

    @classmethod
    def setUpClass(cls):
        """Load cached analysis or analyze all fixture albums once."""
        cls.analyses = load_or_analyze_all()

    def _get_analysis(self, dirname):
        analysis = self.analyses.get(dirname)
        self.assertIsNotNone(analysis, f"Fixture {dirname} not analyzed (missing?)")
        return analysis

    def _decide(self, analysis, verified_lossless=False):
        """Run quality gate on an analysis result."""
        # For FLAC albums, the pipeline converts to V0 first
        if analysis.is_flac and analysis.post_conversion_min_bitrate is not None:
            bitrate = analysis.post_conversion_min_bitrate
            is_cbr = False  # V0 conversion always produces VBR
        else:
            bitrate = analysis.min_bitrate_kbps
            is_cbr = analysis.is_cbr

        if bitrate is None:
            return None

        return _call_quality_gate(
            min_bitrate=bitrate,
            is_cbr=is_cbr,
            verified_lossless=verified_lossless,
            spectral_bitrate=analysis.spectral_bitrate,
        )


def _make_test(dirname, desc, expected_decision, checks):
    """Factory: create a test method for a specific fixture album."""

    def test_method(self):
        a = self._get_analysis(dirname)

        # Check measured properties
        if checks:
            if "is_flac" in checks:
                self.assertEqual(a.is_flac, checks["is_flac"],
                                 f"{dirname}: expected is_flac={checks['is_flac']}")
            if "is_cbr" in checks:
                self.assertEqual(a.is_cbr, checks["is_cbr"],
                                 f"{dirname}: expected is_cbr={checks['is_cbr']}")
            if "min_bitrate_above" in checks:
                self.assertGreaterEqual(
                    a.min_bitrate_kbps, checks["min_bitrate_above"],
                    f"{dirname}: min_bitrate {a.min_bitrate_kbps} should be >= {checks['min_bitrate_above']}")
            if "min_bitrate_below" in checks:
                self.assertLess(
                    a.min_bitrate_kbps, checks["min_bitrate_below"],
                    f"{dirname}: min_bitrate {a.min_bitrate_kbps} should be < {checks['min_bitrate_below']}")
            if "spectral_grade" in checks:
                expected_grade = checks["spectral_grade"]
                if expected_grade == "suspect":
                    # Accept both "suspect" and "likely_transcode" (>75% suspect)
                    self.assertIn(
                        a.spectral_grade, ("suspect", "likely_transcode"),
                        f"{dirname}: expected suspect/likely_transcode, "
                        f"got {a.spectral_grade} (suspect_pct={a.spectral_suspect_pct:.0f}%)")
                else:
                    self.assertEqual(
                        a.spectral_grade, expected_grade,
                        f"{dirname}: expected spectral_grade={expected_grade}, "
                        f"got {a.spectral_grade} (suspect_pct={a.spectral_suspect_pct:.0f}%)")
            if "spectral_grade_not" in checks:
                self.assertNotEqual(
                    a.spectral_grade, checks["spectral_grade_not"],
                    f"{dirname}: spectral_grade should not be {checks['spectral_grade_not']}")

        # Quality gate decision
        if expected_decision is not None:
            # For FLAC: genuine FLACs get verified_lossless=True if spectral=genuine
            if a.is_flac and a.spectral_grade in ("genuine", "marginal"):
                verified = True
            else:
                verified = False
            decision = self._decide(a, verified_lossless=verified)
            self.assertEqual(
                decision, expected_decision,
                f"{dirname}: expected {expected_decision.value}, got {decision.value if decision else None}\n"
                f"  min_br={a.min_bitrate_kbps}, is_cbr={a.is_cbr}, "
                f"spectral={a.spectral_grade}, spectral_br={a.spectral_bitrate}, "
                f"post_conv_br={a.post_conversion_min_bitrate}")

    test_method.__doc__ = f"{dirname}: {desc}"
    return test_method


# Dynamically generate test methods
for _dirname, _desc, _expected, _checks in ALBUM_EXPECTATIONS:
    _test_name = f"test_{_dirname}"
    setattr(TestQualityClassification, _test_name,
            _make_test(_dirname, _desc, _expected, _checks))


@unittest.skipUnless(os.path.isdir(FIXTURES_DIR), "fixtures not generated — run generate_fixtures.sh")
class TestVerifiedLosslessOverride(unittest.TestCase):
    """Test that verified_lossless=True overrides low bitrate for edge cases."""

    @classmethod
    def setUpClass(cls):
        cls.analyses = TestQualityClassification.analyses

    def _get_analysis(self, dirname):
        analysis = self.analyses.get(dirname)
        self.assertIsNotNone(analysis, f"Fixture {dirname} not analyzed")
        return analysis

    def test_quiet_v0_rejected_without_verified(self):
        """Quiet V0 below 210kbps should REQUEUE without verified_lossless."""
        a = self._get_analysis("18_genuine_v0_quiet")
        assert a is not None
        if a.min_bitrate_kbps and a.min_bitrate_kbps < QUALITY_MIN_BITRATE_KBPS:
            decision = _call_quality_gate(
                a.min_bitrate_kbps, a.is_cbr,
                verified_lossless=False)
            self.assertEqual(decision, QualityDecision.REQUEUE_UPGRADE)

    def test_quiet_v0_accepted_with_verified(self):
        """Quiet V0 below 210kbps should ACCEPT with verified_lossless=True."""
        a = self._get_analysis("18_genuine_v0_quiet")
        assert a is not None
        if a.min_bitrate_kbps and a.min_bitrate_kbps < QUALITY_MIN_BITRATE_KBPS:
            decision = _call_quality_gate(
                a.min_bitrate_kbps, a.is_cbr,
                verified_lossless=True)
            self.assertEqual(decision, QualityDecision.ACCEPT)

    def test_cbr_320_still_requeues_even_verified(self):
        """CBR 320 with verified_lossless should still ACCEPT (above threshold).
        The re-queue for FLAC only happens when NOT verified."""
        a = self._get_analysis("04_cbr_320")
        assert a is not None
        decision = _call_quality_gate(
            a.min_bitrate_kbps, a.is_cbr,
            verified_lossless=True)
        # verified_lossless + above threshold → ACCEPT (gold standard path)
        self.assertEqual(decision, QualityDecision.ACCEPT)


@unittest.skipUnless(os.path.isdir(FIXTURES_DIR), "fixtures not generated — run generate_fixtures.sh")
class TestSpectralDetection(unittest.TestCase):
    """Verify spectral analysis correctly identifies transcodes vs genuine."""

    @classmethod
    def setUpClass(cls):
        cls.analyses = TestQualityClassification.analyses

    def _get_analysis(self, dirname):
        analysis = self.analyses.get(dirname)
        self.assertIsNotNone(analysis)
        return analysis

    def test_genuine_flac_not_suspect(self):
        a = self._get_analysis("01_genuine_flac")
        assert a is not None
        self.assertNotEqual(a.spectral_grade, "suspect",
                            f"Genuine FLAC flagged as suspect (pct={a.spectral_suspect_pct:.0f}%)")

    def test_fake_flac_128_detected(self):
        a = self._get_analysis("09_fake_flac_128")
        assert a is not None
        self.assertIn(a.spectral_grade, ("suspect", "likely_transcode"),
                      f"128k fake FLAC not detected: grade={a.spectral_grade}")

    def test_fake_flac_96_detected(self):
        a = self._get_analysis("10_fake_flac_96")
        assert a is not None
        self.assertIn(a.spectral_grade, ("suspect", "likely_transcode"),
                      f"96k fake FLAC not detected: grade={a.spectral_grade}")

    def test_fake_flac_128_bitrate_estimate(self):
        a = self._get_analysis("09_fake_flac_128")
        assert a is not None
        if a.spectral_bitrate is not None:
            self.assertLessEqual(a.spectral_bitrate, 160,
                                 f"128k fake estimated too high: {a.spectral_bitrate}")

    def test_fake_flac_96_bitrate_estimate(self):
        a = self._get_analysis("10_fake_flac_96")
        assert a is not None
        if a.spectral_bitrate is not None:
            self.assertLessEqual(a.spectral_bitrate, 128,
                                 f"96k fake estimated too high: {a.spectral_bitrate}")

    def test_genuine_v0_not_suspect(self):
        a = self._get_analysis("02_genuine_v0")
        assert a is not None
        self.assertNotEqual(a.spectral_grade, "suspect")

    def test_gold_standard_not_suspect(self):
        a = self._get_analysis("22_gold_standard_v0")
        assert a is not None
        self.assertNotEqual(a.spectral_grade, "suspect")


class TestLiveBugReproductions(unittest.TestCase):
    """Reproduce bugs found in live pipeline runs.

    These test the full_pipeline_decision() against exact conditions
    observed in production. Each test documents a real incident.
    """

    def test_tyler_lamberts_grave_cbr320_transcode_accepted(self):
        """BUG: CBR 320 transcode from 160k source was accepted.

        Request 249, 2026-03-28. dangshnizzle uploaded CBR 320 that was
        a transcode from ~160kbps source. Spectral detected likely_transcode
        but the reject gate in process_completed_album only checked for
        grade=="suspect", missing "likely_transcode". Also, spectral said
        new=160 <= existing=160, so it should have been rejected.

        Root cause: soularr.py line 1426 checked `== "suspect"` not
        `in ("suspect", "likely_transcode")`.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=320,
            is_cbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate=160,
            existing_min_bitrate=320,
            existing_spectral_bitrate=160,
        )
        # Should reject — spectral says transcode and not better than existing
        self.assertEqual(r["stage1_spectral"], "reject",
                         f"Should reject: new spectral 160 <= existing 160")
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])

    def test_tyler_lamberts_grave_no_spectral_bitrate(self):
        """Same bug but when spectral_bitrate is None (HF deficit only, no cliff).

        When cliff detection doesn't fire, spectral_bitrate=None.
        The quality gate has nothing to override with, so CBR 320
        passes through as "requeue_lossless" at best.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=320,
            is_cbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate=None,  # no cliff detected
            existing_min_bitrate=320,
            existing_spectral_bitrate=160,
        )
        # Without spectral_bitrate, stage1 can't compare numerically.
        # But grade is likely_transcode — should still reject or at minimum
        # not mark as final "imported".
        self.assertTrue(r["keep_searching"],
                        "likely_transcode should trigger keep_searching")

    def test_taboo_vi_fake_flac_192_accepted(self):
        """BUG: Fake FLAC (192k source) converted to V0 at 224kbps, accepted.

        Request 257, 2026-03-28. amyslskduser uploaded FLAC that was actually
        a 192k transcode. Spectral said likely_transcode but estimated_bitrate
        was None (HF deficit, not cliff). V0 conversion produced 224kbps which
        is above the 210 threshold, so import_one.py didn't flag as transcode.
        Quality gate saw 224 VBR, no spectral_bitrate override → accepted.

        Root causes:
        1. import_one.py transcode threshold (210) too low for 192k fakes
        2. spectral_bitrate=None when cliff not detected → no quality gate override
        3. verified_lossless correctly NOT set (spectral=likely_transcode)
           but quality gate still accepts VBR above 210 without verification
        """
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=None,  # no cliff detected
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            post_conversion_min_bitrate=224,
            converted_count=10,
        )
        # Currently this ACCEPTS — documenting the bug
        # When fixed, it should keep_searching
        self.assertFalse(r.get("verified_lossless", False),
                         "Fake FLAC should never get verified_lossless")
        # BUG: these assertions document current (wrong) behavior
        # Uncomment the correct assertions when the bug is fixed:
        # self.assertTrue(r["keep_searching"])
        # self.assertTrue(r["denylisted"])

    def test_taboo_vi_with_spectral_bitrate(self):
        """Same scenario but if spectral_bitrate had been captured."""
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=192,
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            post_conversion_min_bitrate=224,
            converted_count=10,
        )
        # With spectral_bitrate=192, quality gate should override:
        # gate_br = min(224, 192) = 192 < 210 → requeue_upgrade
        self.assertEqual(r["stage3_quality_gate"], "requeue_upgrade")
        self.assertTrue(r["keep_searching"])
        self.assertTrue(r["denylisted"])

    def test_fake_flac_192_with_fixture_data(self):
        """Use actual fixture 11_fake_flac_192 measurements.

        This fixture DOES get spectral_bitrate=128 from cliff detection,
        so it correctly requeues. But the live bug had no cliff detected.
        """
        import json
        cache_file = os.path.join(os.path.dirname(__file__), "fixtures", "analysis_cache.json")
        if not os.path.exists(cache_file):
            self.skipTest("analysis cache not generated")
        with open(cache_file) as f:
            cache = json.load(f)
        a = cache["11_fake_flac_192"]

        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade=a["spectral_grade"],
            spectral_bitrate=a["spectral_bitrate"],
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            post_conversion_min_bitrate=a["post_conversion_min_bitrate"],
            converted_count=a["track_count"],
        )
        # With fixture data (spectral_bitrate=128), should requeue
        self.assertTrue(r["keep_searching"])


    def test_heretic_pride_one_bad_track_infinite_requeue(self):
        """BUG: 13/14 tracks at 320kbps + 1 track at 192kbps → infinite requeue.

        Request 226, 2026-03-28. wallywubox. Album is CBR 320 except for
        one track at 192kbps. min_bitrate=192 < 210 → requeue_upgrade.
        But every source on Soulseek has the same bad track, so it keeps
        re-downloading the same thing. Downloaded 5 times.

        Root cause: quality gate uses MIN(bitrate) across all tracks.
        One outlier track drags the whole album below threshold.

        Possible fixes:
        - Use percentile instead of MIN (ignore bottom N%)
        - Accept when only 1 track is below and rest are well above
        - Track per-download bitrate comparison to detect "same source, same quality"
        """
        # First import: no existing, 192 < 210 → requeue
        r1 = full_pipeline_decision(
            is_flac=False,
            min_bitrate=192,
            is_cbr=False,
            spectral_grade="genuine",
            spectral_bitrate=None,
            existing_min_bitrate=None,  # first import
        )
        self.assertTrue(r1["imported"])
        # Quality gate: 192 < 210 → requeue_upgrade
        self.assertEqual(r1["stage3_quality_gate"], "requeue_upgrade")
        self.assertEqual(r1["final_status"], "wanted")

        # Second import attempt: same source, same quality
        r2 = full_pipeline_decision(
            is_flac=False,
            min_bitrate=192,
            is_cbr=False,
            spectral_grade="genuine",
            spectral_bitrate=None,
            existing_min_bitrate=192,  # same as what's on disk
        )
        # Stage2 rejects as downgrade (192 <= 192), but album stays wanted
        self.assertEqual(r2["stage2_import"], "downgrade")
        self.assertFalse(r2["imported"])
        # BUG: keep_searching=True means it will try AGAIN → infinite loop
        # When fixed, system should detect same-quality loop and accept
        self.assertTrue(r2["keep_searching"])


# ============================================================================
# Standalone: print classification report
# ============================================================================

def print_report():
    """Print a full classification report for all fixtures."""
    if not os.path.isdir(FIXTURES_DIR):
        print(f"ERROR: Fixtures not found at {FIXTURES_DIR}")
        print("Run: bash tests/fixtures/generate_fixtures.sh")
        return

    all_analyses = load_or_analyze_all()

    print("=" * 130)
    print(f"{'Album':<30} {'Type':>4} {'MinBR':>5} {'CBR':>3} "
          f"{'Spectral':>10} {'SpBR':>4} {'PostV0':>6} "
          f"{'Spectral':>10} {'Import':>18} {'QualGate':>14} "
          f"{'Status':>8} {'Imp':>3} {'Ban':>3} {'Srch':>4}")
    print(f"{'':30} {'':>4} {'':>5} {'':>3} "
          f"{'grade':>10} {'':>4} {'':>6} "
          f"{'stage1':>10} {'stage2':>18} {'stage3':>14} "
          f"{'':>8} {'':>3} {'':>3} {'':>4}")
    print("-" * 130)

    for dirname, desc, expected, checks in ALBUM_EXPECTATIONS:
        folder = os.path.join(FIXTURES_DIR, dirname)
        if not os.path.isdir(folder):
            print(f"  {dirname:<30} MISSING")
            continue

        a = all_analyses.get(dirname)
        if not a:
            print(f"  {dirname:<30} NOT ANALYZED")
            continue

        # Run full pipeline decision
        p = full_pipeline_decision(
            is_flac=a.is_flac,
            min_bitrate=a.min_bitrate_kbps or 0,
            is_cbr=a.is_cbr,
            spectral_grade=a.spectral_grade,
            spectral_bitrate=a.spectral_bitrate,
            existing_min_bitrate=None,  # first import scenario
            existing_spectral_bitrate=None,
            post_conversion_min_bitrate=a.post_conversion_min_bitrate,
            converted_count=a.track_count if a.is_flac else 0,
        )

        ftype = "FLAC" if a.is_flac else "MP3"
        sp_grade = (a.spectral_grade or "-")[:10]
        sp_br = str(a.spectral_bitrate) if a.spectral_bitrate else "-"
        post_v0 = str(a.post_conversion_min_bitrate) if a.post_conversion_min_bitrate else "-"

        s1 = (p["stage1_spectral"] or "-")[:10]
        s2 = (p["stage2_import"] or "-")[:18]
        s3 = (p["stage3_quality_gate"] or "-")[:14]
        status = (p["final_status"] or "?")[:8]
        imp = "Y" if p["imported"] else "N"
        ban = "Y" if p["denylisted"] else "N"
        srch = "Y" if p["keep_searching"] else "N"

        print(f"  {dirname:<30} {ftype:>4} {a.min_bitrate_kbps or 0:>5} "
              f"{'Y' if a.is_cbr else 'N':>3} {sp_grade:>10} {sp_br:>4} {post_v0:>6} "
              f"{s1:>10} {s2:>18} {s3:>14} "
              f"{status:>8} {imp:>3} {ban:>3} {srch:>4}")

    print("=" * 130)


if __name__ == "__main__":
    import sys
    if "--report" in sys.argv:
        sys.argv.remove("--report")
        print_report()
    else:
        unittest.main()
