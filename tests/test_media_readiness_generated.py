"""Generated metadata-mutant contract for issue #1062 media readiness."""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.media_readiness import (
    MediaReadinessError,
    average_bitrate_kbps_from_frames,
    flac_total_samples_only_changed,
    normalize_media_metadata,
    prepare_media_readiness,
)
from tests.audio_fixtures import make_test_flac
from tests.test_media_readiness import _streaminfo_span, _zero_flac_duration_metadata


class TestUnprobeableMediaClassificationProperty(unittest.TestCase):
    """Every non-empty zero-byte audio manifest is source corruption."""

    @given(
        file_count=st.integers(min_value=1, max_value=6),
        extension=st.sampled_from(("mp3", "flac", "m4a", "ogg", "wav", "wma")),
    )
    def test_zero_byte_audio_is_never_a_measurement_failure(
        self, *, file_count: int, extension: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            for ordinal in range(1, file_count + 1):
                (Path(tmp) / f"{ordinal:02d}.{extension}").write_bytes(b"")

            with self.assertRaises(MediaReadinessError) as raised:
                normalize_media_metadata(tmp)

            self.assertEqual(raised.exception.kind, "audio_corrupt")


class TestFlacMetadataMutationProperty(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._fixture_dir = tempfile.mkdtemp(prefix="media-readiness-fixture-")
        cls._fixture = Path(cls._fixture_dir) / "clean.flac"
        make_test_flac(str(cls._fixture), duration=1)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls._fixture_dir)

    @settings(max_examples=8)
    @given(
        clear_md5=st.booleans(),
        clear_frame_sizes=st.booleans(),
        declared_total=st.sampled_from((0, 1, 44_099)),
    )
    def test_metadata_only_mutants_have_clean_stream_facts(
        self, *, clear_md5: bool, clear_frame_sizes: bool, declared_total: int,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clean_path = Path(tmp) / "clean.flac"
            broken_path = Path(tmp) / "broken.flac"
            shutil.copyfile(self._fixture, clean_path)
            shutil.copyfile(self._fixture, broken_path)
            clean = prepare_media_readiness(tmp)
            _zero_flac_duration_metadata(broken_path)
            raw = bytearray(broken_path.read_bytes())
            start, _ = _streaminfo_span(bytes(raw))
            packed = int.from_bytes(raw[start + 10:start + 18], "big")
            raw[start + 10:start + 18] = (
                (packed & ~((1 << 36) - 1)) | declared_total
            ).to_bytes(8, "big")
            broken_path.write_bytes(raw)
            if not clear_md5 or not clear_frame_sizes:
                raw = bytearray(broken_path.read_bytes())
                if not clear_md5:
                    raw[26:42] = self._fixture.read_bytes()[26:42]
                if not clear_frame_sizes:
                    raw[12:18] = self._fixture.read_bytes()[12:18]
                broken_path.write_bytes(raw)
            broken_bytes = broken_path.read_bytes()
            repaired = prepare_media_readiness(tmp)
            clean_facts = {Path(f.path).name: f for f in clean.files}
            repaired_facts = {Path(f.path).name: f for f in repaired.files}
            clean_fact = clean_facts["clean.flac"]
            repaired_fact = repaired_facts["broken.flac"]
            self.assertEqual(
                (
                    clean_fact.codec, clean_fact.container, clean_fact.sample_rate,
                    clean_fact.channels, clean_fact.bit_depth, clean_fact.sample_count,
                    clean_fact.duration_seconds, clean_fact.compressed_audio_bytes,
                    clean_fact.average_bitrate_kbps,
                ),
                (
                    repaired_fact.codec, repaired_fact.container, repaired_fact.sample_rate,
                    repaired_fact.channels, repaired_fact.bit_depth, repaired_fact.sample_count,
                    repaired_fact.duration_seconds, repaired_fact.compressed_audio_bytes,
                    repaired_fact.average_bitrate_kbps,
                ),
            )
            # The packet payload and every non-total-samples metadata byte
            # survive the property mutant and its repair exactly.
            self.assertTrue(
                flac_total_samples_only_changed(broken_bytes, broken_path.read_bytes()),
            )

    @given(low_nibble=st.integers(min_value=0, max_value=15))
    def test_checker_allows_only_total_samples_low_nibble(
        self, *, low_nibble: int,
    ) -> None:
        before = self._fixture.read_bytes()
        start, _ = _streaminfo_span(before)
        allowed = bytearray(before)
        allowed[start + 13] = (allowed[start + 13] & 0xF0) | low_nibble
        self.assertTrue(flac_total_samples_only_changed(before, bytes(allowed)))
        forbidden = bytearray(allowed)
        forbidden[start + 13] ^= 0x10
        self.assertFalse(flac_total_samples_only_changed(before, bytes(forbidden)))


# Track 04 of Koppel *Improvisationer for Klaver* exactly as ffprobe reported
# it (evidence 36856): the real quotient is the integer 256, but the float
# path yielded 255.99999999999997 and truncated to 255.
_KOPPEL_TRACK_04 = {
    "compressed_bytes": 8_517_888,
    "sample_count": 12_776_832,
    "sample_rate": 48_000,
}


def bitrate_derivation_violations(
    *,
    compressed_bytes: int,
    sample_count: int,
    sample_rate: int,
    derived: int | None,
) -> list[str]:
    """Every way a derived average bitrate can misreport its own stream.

    Accumulating rather than short-circuiting: each clause is evaluated on
    every world, so an earlier violation can never mask a later one.
    """
    # Two clauses below are deliberately SUBSUMED rather than independent:
    # when the ratio is an exact integer, `nearest` equals it, and a negative
    # `derived` can never equal a non-negative `nearest`. They are kept for
    # the named diagnosis they produce, not for extra detection power.
    violations: list[str] = []
    degenerate = compressed_bytes <= 0 or sample_count <= 0 or sample_rate <= 0
    if degenerate:
        if derived is not None:
            violations.append(
                f"degenerate stream invented a rate: got {derived}"
            )
        return violations
    if derived is None:
        violations.append("measurable stream withheld a rate")
        return violations
    numerator = compressed_bytes * 8 * sample_rate
    denominator = sample_count * 1000
    if numerator % denominator == 0:
        exact = numerator // denominator
        if derived != exact:
            violations.append(
                f"exact integer rate {exact} reported as {derived}"
            )
    nearest = (numerator + denominator // 2) // denominator
    if derived != nearest:
        violations.append(
            f"rate {derived} is not the nearest integer {nearest}"
        )
    if derived < 0:
        violations.append(f"negative rate {derived}")
    return violations


class TestAverageBitrateDerivationProperty(unittest.TestCase):
    """A constant-bitrate stream must read as its own constant rate.

    Patrols the world space around the Koppel pin in
    ``tests/test_media_readiness.py`` — the float-truncation defect that
    made one 256 kbps track of ten report 255, broke ``is_cbr`` uniformity
    and re-imported an album over itself (dl 39947).
    """

    @settings(max_examples=250)
    @given(
        nominal_kbps=st.sampled_from((96, 128, 160, 192, 224, 256, 320)),
        sample_rate=st.sampled_from((44_100, 48_000, 32_000, 22_050)),
        seconds_milli=st.integers(min_value=1_000, max_value=900_000),
    )
    def test_exact_constant_streams_report_their_own_rate(
        self, *, nominal_kbps: int, sample_rate: int, seconds_milli: int,
    ) -> None:
        # Build a stream whose true rate is EXACTLY nominal_kbps, then let the
        # real production derivation read it back.
        sample_count = max(1, (sample_rate * seconds_milli) // 1000)
        numerator = nominal_kbps * 1000 * sample_count
        if numerator % (8 * sample_rate):
            return  # not an exact whole-byte stream; covered by the general clause
        compressed_bytes = numerator // (8 * sample_rate)
        derived = average_bitrate_kbps_from_frames(
            compressed_bytes, sample_count, sample_rate,
        )
        self.assertEqual(
            bitrate_derivation_violations(
                compressed_bytes=compressed_bytes,
                sample_count=sample_count,
                sample_rate=sample_rate,
                derived=derived,
            ),
            [],
        )
        self.assertEqual(derived, nominal_kbps)

    @settings(max_examples=250)
    @given(
        compressed_bytes=st.integers(min_value=0, max_value=200_000_000),
        sample_count=st.integers(min_value=0, max_value=200_000_000),
        sample_rate=st.sampled_from((0, 8_000, 22_050, 32_000, 44_100, 48_000, 96_000)),
    )
    def test_arbitrary_streams_never_misreport(
        self, *, compressed_bytes: int, sample_count: int, sample_rate: int,
    ) -> None:
        derived = average_bitrate_kbps_from_frames(
            compressed_bytes, sample_count, sample_rate,
        )
        self.assertEqual(
            bitrate_derivation_violations(
                compressed_bytes=compressed_bytes,
                sample_count=sample_count,
                sample_rate=sample_rate,
                derived=derived,
            ),
            [],
        )


class TestBitrateDerivationCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-test per CLAUSE — a guard that never fires proves nothing."""

    def test_exact_integer_clause_trips(self) -> None:
        # The live defect world: exact 256, reported 255.
        self.assertIn(
            "exact integer rate 256 reported as 255",
            bitrate_derivation_violations(**_KOPPEL_TRACK_04, derived=255),
        )

    def test_nearest_integer_clause_trips(self) -> None:
        # A non-exact stream reported one low. 44100 Hz, 1 s, 191.6 kbps.
        found = bitrate_derivation_violations(
            compressed_bytes=191_600 // 8, sample_count=44_100,
            sample_rate=44_100, derived=191,
        )
        self.assertTrue(
            any("is not the nearest integer 192" in v for v in found), found,
        )

    def test_withheld_rate_clause_trips(self) -> None:
        self.assertIn(
            "measurable stream withheld a rate",
            bitrate_derivation_violations(**_KOPPEL_TRACK_04, derived=None),
        )

    def test_invented_rate_clause_trips(self) -> None:
        self.assertIn(
            "degenerate stream invented a rate: got 128",
            bitrate_derivation_violations(
                compressed_bytes=0, sample_count=44_100,
                sample_rate=44_100, derived=128,
            ),
        )

    def test_negative_rate_clause_trips(self) -> None:
        found = bitrate_derivation_violations(**_KOPPEL_TRACK_04, derived=-1)
        self.assertTrue(any("negative rate -1" in v for v in found), found)

    def test_clean_world_has_no_violations(self) -> None:
        self.assertEqual(
            bitrate_derivation_violations(**_KOPPEL_TRACK_04, derived=256), [],
        )
