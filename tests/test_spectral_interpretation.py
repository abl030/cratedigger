"""Deterministic pins for per-codec spectral interpretation (issue #829 PR2a).

Exhaustive over every measured ladder boundary, every codec regime, the
measured-subject resolution ladder, and the comparability rule. The
generated properties that patrol the world space around these pins live in
``tests/test_spectral_interpretation_generated.py``.

Conventions follow ``tests/test_quality_decisions.py``: a ``CASES`` table
plus one ``subTest`` method per decision matrix.
"""

import os
import sys
import unittest
from typing import ClassVar

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.quality import (
    AAC_FLOOR_HIGH_CLASS_KBPS,
    AAC_FLOOR_LOW_CLASS_KBPS,
    MP3_TOP_CLASS_KBPS,
    VORBIS_TOP_CLASS_KBPS,
    CodecFamily,
    SpectralEvidenceFacts,
    interpret_spectral_cliff,
    interpret_spectral_evidence,
    is_mixed_codec_album,
    ladder_class_kbps,
    resolve_measured_codec_family,
    spectral_classes_comparable,
)

# ---------------------------------------------------------------------------
# Real live worlds, verified on prod 2026-07-29. Every field carries the
# exact persisted column value — no invented literals, no convenient NULLs
# (.claude/rules/test-fidelity.md Rule C).
# ---------------------------------------------------------------------------

#: evidence 33591 (download 37946, request 6387, Wavves — "Wavves"). The
#: row that opened issue #829: a 256 kbps CBR AAC graded
#: ``likely_transcode`` with a LAME-table ``spectral_bitrate_kbps=128``.
#: ``codec`` is the ambiguous bare container ``m4a``; only ``format`` and
#: ``storage_format`` name the real codec, and they corroborate rather
#: than conflict. ``filetype_band='m4a'`` must not trip the mixed detector.
WAVVES_EVIDENCE_33591 = SpectralEvidenceFacts(
    spectral_grade="likely_transcode",
    codec_family=None,
    spectral_subject="source",
    was_converted_from=None,
    format="AAC",
    storage_format="AAC",
    filetype_band="m4a",
    cliff_hz=None,
    spectral_bitrate_kbps=128,
    sbr_present=None,
)

#: evidence 33592 — the same Wavves album's INSTALLED subject, and the
#: best #829 exhibit in the library: a genuinely v2-captured AAC cliff
#: (``cliff_hz=15500``, ``codec_family='aac'``,
#: ``spectral_measurement_version=2``) that the LAME table buckets as a
#: 128 transcode on a 256 kbps source.
WAVVES_EVIDENCE_33592 = SpectralEvidenceFacts(
    spectral_grade="likely_transcode",
    codec_family="aac",
    spectral_subject="installed",
    was_converted_from=None,
    format="AAC",
    storage_format="AAC",
    filetype_band="m4a",
    cliff_hz=15500,
    spectral_bitrate_kbps=128,
    sbr_present=None,
)

#: evidence id 34219, request 8902, Iron & Wine — "Fall 2007". A live
#: PR1-captured v2 row: ``cliff_hz=16500`` re-derives to the 160 class in
#: detector space while its own stored legacy bucket says 128.
FALL_2007_34219 = SpectralEvidenceFacts(
    spectral_grade="likely_transcode",
    codec_family="mp3",
    spectral_subject="installed",
    was_converted_from=None,
    format="MP3",
    storage_format="MP3",
    filetype_band="mp3",
    cliff_hz=16500,
    spectral_bitrate_kbps=128,
    sbr_present=None,
)

#: evidence 33735 — a FLAC graded ``likely_transcode`` by the HF-DEFICIT
#: leg alone: ``cliff_hz`` NULL and ``spectral_bitrate_kbps`` NULL, so
#: cliff presence sees nothing. 890 live lossless rows are in this state
#: (588 ``likely_transcode`` + 302 ``suspect``).
DEFICIT_ONLY_FAKE_FLAC_33735 = SpectralEvidenceFacts(
    spectral_grade="likely_transcode",
    codec_family=None,
    spectral_subject="source",
    was_converted_from=None,
    format="FLAC",
    storage_format="FLAC",
    filetype_band="flac",
    cliff_hz=None,
    spectral_bitrate_kbps=None,
    sbr_present=None,
)

#: evidence 3689 — an Opus copy wearing its source FLAC's spectral
#: evidence under R19, graded ``genuine`` but carrying a stored
#: ``spectral_bitrate_kbps=198``. 1,589 live rows.
R19_GENUINE_LOSSLESS_3689 = SpectralEvidenceFacts(
    spectral_grade="genuine",
    codec_family=None,
    spectral_subject="source",
    was_converted_from="flac",
    format="opus",
    storage_format="opus",
    filetype_band="opus",
    cliff_hz=None,
    spectral_bitrate_kbps=198,
    sbr_present=None,
)

#: evidence 5144 — an MP3 graded ``genuine`` whose stored
#: ``spectral_bitrate_kbps=202`` IS its container bitrate
#: (``min_bitrate_kbps=202``), not a LAME bucket. 2,503 of 30,251 live
#: rows carry container bitrates in that column, up to 738. 1,411 of them
#: are MP3.
CONTAINER_BITRATE_IN_SPECTRAL_5144 = SpectralEvidenceFacts(
    spectral_grade="genuine",
    codec_family=None,
    spectral_subject="source",
    was_converted_from=None,
    format="MP3",
    storage_format="MP3",
    filetype_band="mp3",
    cliff_hz=None,
    spectral_bitrate_kbps=202,
    sbr_present=None,
)


class TestMp3DetectorLadder(unittest.TestCase):
    """MP3 detector-space buckets, measured on four arms:
    <15000->96 | <16000->128 | <17250->160 | <18250->192 | <19250->256 |
    >=19250->320. Every bucket edge is pinned on both sides."""

    CASES: ClassVar = [
        # desc, cliff_hz, expected class
        ("far below the window floor",      12000, 96),
        ("last Hz of the 96 bucket",        14999, 96),
        ("first Hz of the 128 bucket",      15000, 128),
        ("measured CBR-128 median",         15500, 128),
        ("last Hz of the 128 bucket",       15999, 128),
        ("first Hz of the 160 bucket",      16000, 160),
        ("measured CBR-160 median",         16500, 160),
        ("last Hz of the 160 bucket",       17249, 160),
        ("first Hz of the 192 bucket",      17250, 192),
        ("measured CBR-192 median",         18000, 192),
        ("last Hz of the 192 bucket",       18249, 192),
        ("first Hz of the 256 bucket",      18250, 256),
        ("measured CBR-224/256 median",     19000, 256),
        ("last Hz of the 256 bucket",       19249, 256),
        ("first Hz of the 320 bucket",      19250, 320),
        ("measured CBR-320 median",         19500, 320),
        ("above the extension window",      22000, 320),
    ]

    def test_bucket_table(self):
        for desc, cliff_hz, expected in self.CASES:
            with self.subTest(desc=desc, cliff_hz=cliff_hz):
                self.assertEqual(ladder_class_kbps("mp3", cliff_hz), expected)

    def test_top_bucket_constant_matches_the_table(self):
        self.assertEqual(ladder_class_kbps("mp3", 19250), MP3_TOP_CLASS_KBPS)

    def test_detector_space_is_one_tier_above_the_legacy_encoder_table(self):
        """The whole point of the re-derivation: the shipped
        ``LAME_LOWPASS``-derived table buckets a 16500Hz cliff as 128
        (encoder space); detector space says 160."""
        from lib.spectral_check import estimate_bitrate_from_cliff

        self.assertEqual(estimate_bitrate_from_cliff(16500), 128)
        self.assertEqual(ladder_class_kbps("mp3", 16500), 160)


class TestVorbisDetectorLadder(unittest.TestCase):
    """Vorbis q0-q4 detector-space ladder, replicated exactly on four arms:
    <15250->64 | <16500->96 | <17750->112 | <19000->128 | >=19000->160."""

    CASES: ClassVar = [
        # desc, cliff_hz, expected class
        ("below the window floor",          12000, 64),
        ("measured q0 median",              14500, 64),
        ("last Hz of the 64 bucket",        15249, 64),
        ("first Hz of the 96 bucket",       15250, 96),
        ("measured q2 median",              16000, 96),
        ("last Hz of the 96 bucket",        16499, 96),
        ("first Hz of the 112 bucket",      16500, 112),
        ("measured q3 median",              17000, 112),
        ("last Hz of the 112 bucket",       17749, 112),
        ("first Hz of the 128 bucket",      17750, 128),
        ("measured q4 median",              18500, 128),
        ("last Hz of the 128 bucket",       18999, 128),
        ("first Hz of the 160 bucket",      19000, 160),
        ("above the extension window",      22000, 160),
    ]

    def test_bucket_table(self):
        for desc, cliff_hz, expected in self.CASES:
            with self.subTest(desc=desc, cliff_hz=cliff_hz):
                self.assertEqual(ladder_class_kbps("vorbis", cliff_hz), expected)

    def test_top_bucket_constant_matches_the_table(self):
        self.assertEqual(
            ladder_class_kbps("vorbis", 19000), VORBIS_TOP_CLASS_KBPS
        )

    def test_the_same_cliff_means_different_things_per_codec(self):
        """Cutoff Hz is not a currency: a 17000Hz cliff is the 160 class in
        MP3 and the 112 class in Vorbis."""
        self.assertEqual(ladder_class_kbps("mp3", 17000), 160)
        self.assertEqual(ladder_class_kbps("vorbis", 17000), 112)


class TestNoLadderForNonLadderCodecs(unittest.TestCase):
    """Only MP3 and Vorbis have an invertible ladder."""

    CASES: ClassVar[list[tuple[str, CodecFamily]]] = [
        ("aac", "aac"),
        ("opus", "opus"),
        ("lossless", "lossless"),
        ("other", "other"),
    ]

    def test_ladder_class_is_none(self):
        for desc, family in self.CASES:
            with self.subTest(desc=desc):
                self.assertIsNone(ladder_class_kbps(family, 16500))


class TestMp3Interpretation(unittest.TestCase):
    """MP3 is decision-grade when a class was actually inferred."""

    def test_cliff_derives_the_class(self):
        result = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode", cliff_hz=16500,
        )
        self.assertEqual(result.inferred_class_kbps, 160)
        self.assertEqual(result.basis, "cliff_hz")
        self.assertEqual(result.reason, "ladder_class_from_cliff")
        self.assertTrue(result.decision_grade)
        self.assertTrue(result.invertible_ladder)
        self.assertFalse(result.floor_only)
        self.assertTrue(result.supports_transcode_accusation)

    def test_stored_bucket_is_the_legacy_fallback(self):
        result = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode", stored_bitrate_kbps=128,
        )
        self.assertEqual(result.inferred_class_kbps, 128)
        self.assertEqual(result.basis, "stored_bucket")
        self.assertEqual(result.reason, "ladder_class_from_stored_bucket")
        self.assertTrue(result.decision_grade)

    def test_cliff_wins_over_the_stored_bucket(self):
        result = interpret_spectral_cliff(
            "mp3", spectral_grade="suspect",
            cliff_hz=16500, stored_bitrate_kbps=128,
        )
        self.assertEqual(result.inferred_class_kbps, 160)
        self.assertEqual(result.basis, "cliff_hz")

    def test_no_cliff_and_no_bucket_asserts_nothing(self):
        result = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode",
        )
        self.assertIsNone(result.inferred_class_kbps)
        self.assertEqual(result.basis, "none")
        self.assertEqual(result.reason, "ladder_no_evidence")
        self.assertFalse(result.decision_grade)
        self.assertTrue(result.invertible_ladder)

    def test_nonpositive_stored_bucket_is_not_a_class(self):
        for stored in (0, -1):
            with self.subTest(stored=stored):
                result = interpret_spectral_cliff(
                    "mp3", spectral_grade="likely_transcode",
                    stored_bitrate_kbps=stored,
                )
                self.assertIsNone(result.inferred_class_kbps)
                self.assertFalse(result.decision_grade)


class TestLadderGradeGate(unittest.TestCase):
    """A class is inferred only when production's spectral verdict
    authorizes one — the SAME gate the importer applies in
    ``compute_effective_override_bitrate``. This module reads that verdict;
    it never reconstructs a narrower one from cliff presence."""

    AUTHORIZING: ClassVar = ["suspect", "likely_transcode"]
    NON_AUTHORIZING: ClassVar = ["genuine", "marginal", "error", None, "wat"]

    def test_authorizing_grades_infer_a_class_from_a_cliff(self):
        for grade in self.AUTHORIZING:
            with self.subTest(grade=grade):
                result = interpret_spectral_cliff(
                    "mp3", spectral_grade=grade, cliff_hz=16500,
                )
                self.assertEqual(result.inferred_class_kbps, 160)
                self.assertTrue(result.decision_grade)
                self.assertTrue(result.supports_transcode_accusation)

    def test_non_authorizing_grades_infer_nothing_from_a_cliff(self):
        """A ``genuine`` album verdict means the album-level decision
        already rejected the minority cliff that produced ``cliff_hz``.
        Deriving a class from it would contradict a decision that exists."""
        for grade in self.NON_AUTHORIZING:
            with self.subTest(grade=grade):
                result = interpret_spectral_cliff(
                    "mp3", spectral_grade=grade, cliff_hz=16500,
                )
                self.assertIsNone(result.inferred_class_kbps)
                self.assertEqual(result.basis, "none")
                self.assertEqual(result.reason, "ladder_grade_not_transcode")
                self.assertFalse(result.decision_grade)
                self.assertFalse(result.supports_transcode_accusation)

    def test_non_authorizing_grades_infer_nothing_from_a_stored_bucket(self):
        for grade in self.NON_AUTHORIZING:
            with self.subTest(grade=grade):
                result = interpret_spectral_cliff(
                    "mp3", spectral_grade=grade, stored_bitrate_kbps=202,
                )
                self.assertIsNone(result.inferred_class_kbps)
                self.assertEqual(result.reason, "ladder_grade_not_transcode")
                self.assertFalse(result.decision_grade)

    def test_no_evidence_is_named_apart_from_an_unauthorized_grade(self):
        blocked = interpret_spectral_cliff(
            "mp3", spectral_grade="genuine", cliff_hz=16500,
        )
        nothing = interpret_spectral_cliff("mp3", spectral_grade="genuine")
        self.assertEqual(blocked.reason, "ladder_grade_not_transcode")
        self.assertEqual(nothing.reason, "ladder_no_evidence")

    def test_only_lame_bucket_values_survive_as_a_stored_class(self):
        """The stored column does not always hold a LAME bucket: 2,503 of
        30,251 live rows carry a container bitrate there, up to 738.
        ``estimate_bitrate_from_cliff`` returns only ``LAME_LOWPASS``
        members, so nothing legitimate falls outside this set."""
        from lib.spectral_check import LAME_LOWPASS

        for _lowpass_hz, bucket in LAME_LOWPASS:
            with self.subTest(bucket=bucket):
                result = interpret_spectral_cliff(
                    "mp3", spectral_grade="likely_transcode",
                    stored_bitrate_kbps=bucket,
                )
                self.assertEqual(result.inferred_class_kbps, bucket)
                self.assertEqual(result.basis, "stored_bucket")
                self.assertTrue(result.decision_grade)

    def test_a_non_bucket_stored_value_never_becomes_a_class(self):
        """Values drawn from the real live non-bucket tail. All 53
        authorizing-graded non-bucket rows on prod today resolve to Opus
        or a lossless lineage, so none currently REACHES this branch — the
        codec-family gate stops them first. This is the boundary guard for
        the ladder families, pinned with producible values."""
        for stored in (121, 122, 123, 124, 126, 131, 223, 226, 738):
            with self.subTest(stored=stored):
                result = interpret_spectral_cliff(
                    "mp3", spectral_grade="likely_transcode",
                    stored_bitrate_kbps=stored,
                )
                self.assertIsNone(result.inferred_class_kbps)
                self.assertEqual(result.basis, "none")
                self.assertEqual(
                    result.reason, "ladder_stored_value_not_a_bucket"
                )
                self.assertFalse(result.decision_grade)

    def test_a_non_bucket_value_is_named_apart_from_the_other_refusals(self):
        not_a_bucket = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode", stored_bitrate_kbps=226,
        )
        unauthorized = interpret_spectral_cliff(
            "mp3", spectral_grade="genuine", stored_bitrate_kbps=128,
        )
        nothing = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode",
        )
        self.assertEqual(
            not_a_bucket.reason, "ladder_stored_value_not_a_bucket"
        )
        self.assertEqual(unauthorized.reason, "ladder_grade_not_transcode")
        self.assertEqual(nothing.reason, "ladder_no_evidence")

    def test_the_allowlist_never_touches_the_cliff_path(self):
        """A cliff is a raw measurement, not a bucket: 16500 is not a
        ``LAME_LOWPASS`` class value and must still produce 160."""
        from lib.spectral_check import LAME_LOWPASS

        self.assertNotIn(16500, {kbps for _hz, kbps in LAME_LOWPASS})
        result = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode", cliff_hz=16500,
        )
        self.assertEqual(result.inferred_class_kbps, 160)
        self.assertEqual(result.basis, "cliff_hz")

    def test_evidence_634_real_non_bucket_row_yields_no_class(self):
        """A real prod row from the 53: ``spectral_bitrate_kbps=226`` on a
        ``likely_transcode`` Opus copy of a FLAC source. It yields no class
        — but note the CODEC-FAMILY gate is what stops it (the measured
        subject is the source FLAC), not the allowlist. Recorded so the
        two guards are not confused for one another."""
        facts = SpectralEvidenceFacts(
            spectral_grade="likely_transcode",
            codec_family=None,
            format="Opus",
            storage_format="Opus",
            filetype_band="",
            spectral_subject="source",
            was_converted_from="flac",
            cliff_hz=None,
            spectral_bitrate_kbps=226,
        )
        result = interpret_spectral_evidence(facts)
        self.assertEqual(result.codec_family, "lossless")
        self.assertIsNone(result.inferred_class_kbps)
        self.assertEqual(result.reason, "lossless_transcode_grade")

    def test_a_deficit_only_flagged_mp3_still_accuses_without_a_class(self):
        """The HF-deficit leg catches CBR-64, whose 11kHz lowpass sits
        BELOW the analysis window, so it never produces a cliff. The album
        is still flagged; it just has no class."""
        result = interpret_spectral_cliff(
            "mp3", spectral_grade="suspect",
        )
        self.assertIsNone(result.inferred_class_kbps)
        self.assertFalse(result.decision_grade)
        self.assertTrue(result.supports_transcode_accusation)


class TestAacContentFloor(unittest.TestCase):
    """An AAC cliff is a one-sided content floor and nothing more.

    94-96% of AAC cliffs on every arm land in 13-18 kHz, produced by
    everything from 96 to 320 kbps across ffmpeg-native, libfdk AND Apple
    CoreAudio. It is never a bitrate and never a transcode accusation.
    """

    CASES: ClassVar = [
        # desc, cliff_hz, expected floor, expected reason
        ("junk below the measurable floor",  9000, None,
         "aac_cliff_below_measurable_floor"),
        ("last junk Hz",                    12999, None,
         "aac_cliff_below_measurable_floor"),
        ("first floored Hz",                13000, AAC_FLOOR_LOW_CLASS_KBPS,
         "aac_content_floor_low"),
        ("fdk 192/256/320 median",          16500, AAC_FLOOR_LOW_CLASS_KBPS,
         "aac_content_floor_low"),
        ("apple cbr128 median",             17000, AAC_FLOOR_LOW_CLASS_KBPS,
         "aac_content_floor_low"),
        ("top of the pooled floor band",    18000, AAC_FLOOR_LOW_CLASS_KBPS,
         "aac_content_floor_low"),
        ("unlifted gap stays at the floor", 18499, AAC_FLOOR_LOW_CLASS_KBPS,
         "aac_content_floor_low"),
        ("first lifted Hz",                 18500, AAC_FLOOR_HIGH_CLASS_KBPS,
         "aac_content_floor_high"),
        ("apple abr192 / tvbr91 median",    18500, AAC_FLOOR_HIGH_CLASS_KBPS,
         "aac_content_floor_high"),
        ("well above the lift",             20000, AAC_FLOOR_HIGH_CLASS_KBPS,
         "aac_content_floor_high"),
    ]

    def test_floor_table(self):
        for desc, cliff_hz, expected, reason in self.CASES:
            with self.subTest(desc=desc, cliff_hz=cliff_hz):
                result = interpret_spectral_cliff(
                    "aac", spectral_grade="likely_transcode",
                    cliff_hz=cliff_hz,
                )
                self.assertEqual(result.inferred_class_kbps, expected)
                self.assertEqual(result.reason, reason)
                self.assertEqual(result.semantics, "content_floor")
                self.assertTrue(result.floor_only)
                self.assertFalse(result.decision_grade)
                self.assertFalse(result.invertible_ladder)
                self.assertFalse(result.supports_transcode_accusation)

    def test_no_cliff_asserts_no_floor(self):
        result = interpret_spectral_cliff(
            "aac", spectral_grade="likely_transcode",
        )
        self.assertIsNone(result.inferred_class_kbps)
        self.assertEqual(result.reason, "aac_no_cliff")
        self.assertEqual(result.basis, "none")
        self.assertFalse(result.supports_transcode_accusation)

    def test_legacy_stored_bucket_never_becomes_an_aac_class(self):
        """The download-37946 defect in one assertion: the LAME-table
        ``spectral_bitrate_kbps=128`` stored on an AAC row is not evidence
        of anything and must not survive as a class."""
        result = interpret_spectral_cliff(
            "aac", spectral_grade="likely_transcode",
            stored_bitrate_kbps=128,
        )
        self.assertIsNone(result.inferred_class_kbps)
        self.assertEqual(result.reason, "aac_no_cliff")
        self.assertFalse(result.supports_transcode_accusation)

    def test_the_floor_is_deliberately_not_grade_gated(self):
        """Unlike the ladder, the AAC floor ignores ``spectral_grade``: it
        is not a decision input, and the grade for an AAC album is derived
        from cliffs the calibration proves are native behaviour — the
        exact input #829 says not to trust here."""
        for grade in ("likely_transcode", "suspect", "genuine", None):
            with self.subTest(grade=grade):
                result = interpret_spectral_cliff(
                    "aac", spectral_grade=grade, cliff_hz=16500,
                )
                self.assertEqual(
                    result.inferred_class_kbps, AAC_FLOOR_LOW_CLASS_KBPS
                )
                self.assertFalse(result.decision_grade)
                self.assertFalse(result.supports_transcode_accusation)


class TestOpusAndSbrAreAuditOnly(unittest.TestCase):
    """Opus >=32k and HE-AAC carry no spectral signal at all."""

    def test_opus_asserts_nothing_whatever_was_measured(self):
        for cliff_hz in (None, 12000, 16500, 19500):
            for grade in ("likely_transcode", "genuine"):
                with self.subTest(cliff_hz=cliff_hz, grade=grade):
                    result = interpret_spectral_cliff(
                        "opus", spectral_grade=grade,
                        cliff_hz=cliff_hz, stored_bitrate_kbps=128,
                    )
                    self.assertEqual(result.semantics, "audit_only")
                    self.assertEqual(result.reason, "opus_no_spectral_signal")
                    self.assertIsNone(result.inferred_class_kbps)
                    self.assertFalse(result.decision_grade)
                    self.assertFalse(result.supports_transcode_accusation)

    def test_sbr_forces_audit_only_over_every_lossy_family(self):
        """fdk-he1-64 is 96-100% no-cliff — HE-AAC reads as lossless, so an
        SBR stream's spectral evidence is meaningless in both directions.

        ``lossless`` is excluded on purpose and pinned separately below."""
        for family in ("mp3", "aac", "opus", "vorbis", "other"):
            with self.subTest(family=family):
                result = interpret_spectral_cliff(
                    family, spectral_grade="likely_transcode",
                    cliff_hz=16500, stored_bitrate_kbps=128,
                    sbr_present=True,
                )
                self.assertEqual(result.semantics, "audit_only")
                self.assertEqual(result.reason, "sbr_audit_only")
                self.assertIsNone(result.inferred_class_kbps)
                self.assertFalse(result.decision_grade)
                self.assertFalse(result.supports_transcode_accusation)

    def test_sbr_never_disarms_the_lossless_fake_detector(self):
        """Which of the two errors is unrecoverable decides the direction.
        For AAC it is wrongly ASSERTING quality, so audit-only is right;
        for a lossless container it is failing to detect a FAKE, so
        audit-only would fail OPEN. SBR is inert on this branch."""
        armed = interpret_spectral_cliff(
            "lossless", spectral_grade="likely_transcode", cliff_hz=16500,
        )
        for sbr in (None, False, True):
            with self.subTest(sbr_present=sbr):
                result = interpret_spectral_cliff(
                    "lossless", spectral_grade="likely_transcode",
                    cliff_hz=16500, sbr_present=sbr,
                )
                self.assertEqual(result, armed)
                self.assertEqual(result.semantics, "lossless_authenticity")
                self.assertTrue(result.supports_transcode_accusation)

    def test_sbr_not_captured_is_not_sbr_absent(self):
        """``None`` means the AAC object type was never probed (no producer
        until PR3); it must not behave like ``False`` being asserted."""
        unprobed = interpret_spectral_cliff(
            "aac", spectral_grade="likely_transcode", cliff_hz=16500,
        )
        explicit_false = interpret_spectral_cliff(
            "aac", spectral_grade="likely_transcode",
            cliff_hz=16500, sbr_present=False,
        )
        self.assertEqual(unprobed, explicit_false)
        self.assertNotEqual(
            unprobed,
            interpret_spectral_cliff(
                "aac", spectral_grade="likely_transcode",
                cliff_hz=16500, sbr_present=True,
            ),
        )


class TestLosslessSemanticsUnchanged(unittest.TestCase):
    """A lossless container's fake-FLAC detector is armed by production's
    spectral GRADE — the union of the cliff and HF-deficit legs — never by
    a reconstruction from cliff presence."""

    def test_transcode_grade_arms_the_detector(self):
        result = interpret_spectral_cliff(
            "lossless", spectral_grade="likely_transcode", cliff_hz=16500,
        )
        self.assertEqual(result.semantics, "lossless_authenticity")
        self.assertIsNone(result.inferred_class_kbps)
        self.assertFalse(result.decision_grade)
        self.assertFalse(result.invertible_ladder)
        self.assertFalse(result.floor_only)
        self.assertTrue(result.supports_transcode_accusation)
        self.assertEqual(result.reason, "lossless_transcode_grade")

    def test_deficit_only_detection_still_arms_the_detector(self):
        """Evidence 33735's shape: no ``cliff_hz``, no stored bucket, and
        still ``likely_transcode`` — caught by the HF-deficit leg alone.
        890 live lossless rows are in this state."""
        result = interpret_spectral_cliff(
            "lossless", spectral_grade="likely_transcode",
        )
        self.assertTrue(result.supports_transcode_accusation)
        self.assertEqual(result.reason, "lossless_transcode_grade")

    def test_suspect_also_arms_the_detector(self):
        result = interpret_spectral_cliff("lossless", spectral_grade="suspect")
        self.assertTrue(result.supports_transcode_accusation)

    def test_a_stored_bucket_alone_never_arms_the_detector(self):
        """Evidence 3689's shape: an R19 lossless-lineage row graded
        ``genuine`` that still carries ``spectral_bitrate_kbps=198``. The
        bucket is not a verdict. 1,589 live rows."""
        result = interpret_spectral_cliff(
            "lossless", spectral_grade="genuine", stored_bitrate_kbps=198,
        )
        self.assertFalse(result.supports_transcode_accusation)
        self.assertEqual(result.reason, "lossless_grade_not_transcode")
        self.assertIsNone(result.inferred_class_kbps)

    def test_genuine_is_the_affirmative_input(self):
        result = interpret_spectral_cliff("lossless", spectral_grade="genuine")
        self.assertEqual(result.reason, "lossless_grade_not_transcode")
        self.assertFalse(result.supports_transcode_accusation)
        self.assertIsNone(result.inferred_class_kbps)


class TestUnknownAndUncalibratedFamilies(unittest.TestCase):
    def test_unknown_family_fails_closed(self):
        result = interpret_spectral_cliff(
            None, spectral_grade="likely_transcode",
            cliff_hz=16500, stored_bitrate_kbps=128,
        )
        self.assertEqual(result.semantics, "audit_only")
        self.assertEqual(result.reason, "unknown_codec_family")
        self.assertIsNone(result.inferred_class_kbps)
        self.assertFalse(result.supports_transcode_accusation)

    def test_other_family_fails_closed(self):
        result = interpret_spectral_cliff(
            "other", spectral_grade="likely_transcode",
            cliff_hz=16500, stored_bitrate_kbps=128,
        )
        self.assertEqual(result.semantics, "audit_only")
        self.assertEqual(result.reason, "uncalibrated_codec_family")
        self.assertIsNone(result.inferred_class_kbps)
        self.assertFalse(result.supports_transcode_accusation)


class TestMeasuredCodecFamilyResolution(unittest.TestCase):
    """The codec that matters is the codec of what was MEASURED."""

    CASES: ClassVar = [
        # desc, facts, expected family, expected basis
        (
            "PR1 capture wins outright",
            SpectralEvidenceFacts(
                codec_family="lossless", format="Opus",
                was_converted_from="flac", spectral_subject="source",
            ),
            "lossless", "codec_family",
        ),
        (
            "non-converted row reads format",
            SpectralEvidenceFacts(format="AAC", spectral_subject="source"),
            "aac", "format",
        ),
        (
            "mixed-case format label",
            SpectralEvidenceFacts(format="MP3"),
            "mp3", "format",
        ),
        (
            "trailing qualifier is stripped",
            SpectralEvidenceFacts(format="opus 128"),
            "opus", "format",
        ),
        (
            "vbr qualifier is stripped",
            SpectralEvidenceFacts(format="mp3 v0"),
            "mp3", "format",
        ),
        (
            "storage_format is the fallback",
            SpectralEvidenceFacts(format=None, storage_format="FLAC"),
            "lossless", "storage_format",
        ),
        (
            "storage_format covers an ambiguous format",
            SpectralEvidenceFacts(format="m4a", storage_format="ALAC"),
            "lossless", "storage_format",
        ),
        (
            "R19 lossless lineage: measured subject is the source FLAC",
            SpectralEvidenceFacts(
                format="opus", was_converted_from="flac",
                spectral_subject="source",
            ),
            "lossless", "was_converted_from",
        ),
        (
            "converted from m4a is ambiguous, not the derivative's opus",
            SpectralEvidenceFacts(
                format="opus", was_converted_from="m4a",
                spectral_subject="source",
            ),
            None, "unresolved",
        ),
        (
            "installed subject on a converted row describes the derivative",
            SpectralEvidenceFacts(
                format="opus", was_converted_from="flac",
                spectral_subject="installed",
            ),
            "opus", "format",
        ),
        (
            "bare m4a container is ambiguous (AAC or ALAC)",
            SpectralEvidenceFacts(format="m4a"),
            None, "unresolved",
        ),
        (
            "bare ogg container is ambiguous (Vorbis or Opus)",
            SpectralEvidenceFacts(format="ogg"),
            None, "unresolved",
        ),
        (
            "unrecognised label fails closed",
            SpectralEvidenceFacts(format="shorten"),
            None, "unresolved",
        ),
        (
            "empty labels fail closed",
            SpectralEvidenceFacts(format="   ", storage_format=""),
            None, "unresolved",
        ),
        (
            "wma is a known but uncalibrated family",
            SpectralEvidenceFacts(format="WMA"),
            "other", "format",
        ),
        (
            "contradictory labels fail closed",
            SpectralEvidenceFacts(format="MP3", storage_format="FLAC"),
            None, "conflicting_labels",
        ),
        (
            "comma band spans codecs",
            SpectralEvidenceFacts(format="MP3", filetype_band="m4a, mp3"),
            None, "mixed_album",
        ),
        (
            "mixed_lossy band spans codecs",
            SpectralEvidenceFacts(format="MP3", filetype_band="mixed_lossy"),
            None, "mixed_album",
        ),
        (
            "mixed band outranks a PR1 capture",
            SpectralEvidenceFacts(codec_family="mp3", filetype_band="mixed"),
            None, "mixed_album",
        ),
        (
            "mixed_lossless is still exactly one family",
            SpectralEvidenceFacts(
                format="FLAC", filetype_band="mixed_lossless",
            ),
            "lossless", "format",
        ),
    ]

    def test_resolution_table(self):
        for desc, facts, expected_family, expected_basis in self.CASES:
            with self.subTest(desc=desc):
                resolution = resolve_measured_codec_family(facts)
                self.assertEqual(resolution.family, expected_family)
                self.assertEqual(resolution.basis, expected_basis)

    def test_mixed_band_detection(self):
        for band, expected in (
            ("", False),
            ("mp3", False),
            ("flac", False),
            ("mixed_lossless", False),
            ("mixed", True),
            ("mixed_lossy", True),
            ("MIXED_LOSSY", True),
            ("m4a, mp3", True),
            ("flac, m4a", True),
        ):
            with self.subTest(band=band):
                self.assertEqual(is_mixed_codec_album(band), expected)


class TestComparability(unittest.TestCase):
    """Compare in inferred-class space, never in cutoff space, and only
    when both sides have an invertible ladder AND derived their class the
    same way."""

    CASES: ClassVar = [
        # desc, left kwargs, right kwargs, comparable, reason
        (
            "MP3 cliff vs MP3 cliff",
            ("mp3", 16500, None), ("mp3", 18000, None),
            True, "comparable_same_derivation",
        ),
        (
            "MP3 cliff vs Vorbis cliff — valid in class space",
            ("mp3", 16500, None), ("vorbis", 17000, None),
            True, "comparable_same_derivation",
        ),
        (
            "both legacy stored buckets, same codec",
            ("mp3", None, 128), ("mp3", None, 192),
            True, "comparable_same_derivation",
        ),
        (
            "both legacy stored buckets, both Vorbis — same bias",
            ("vorbis", None, 128), ("vorbis", None, 192),
            True, "comparable_same_derivation",
        ),
        (
            "MP3 legacy bucket vs Vorbis legacy bucket: table bias",
            ("mp3", None, 128), ("vorbis", None, 192),
            False, "cross_codec_legacy_bucket",
        ),
        (
            "the same refusal in the other direction",
            ("vorbis", None, 192), ("mp3", None, 128),
            False, "cross_codec_legacy_bucket",
        ),
        (
            "cliff vs stored bucket — mixed derivation",
            ("mp3", 16500, None), ("mp3", None, 128),
            False, "mixed_derivation_basis",
        ),
        (
            "stored bucket vs cliff — mixed derivation",
            ("mp3", None, 128), ("mp3", 16500, None),
            False, "mixed_derivation_basis",
        ),
        (
            "AAC floor contributes no comparison",
            ("aac", 16500, None), ("mp3", 16500, None),
            False, "left_not_decision_grade",
        ),
        (
            "Opus contributes nothing",
            ("mp3", 16500, None), ("opus", 16500, None),
            False, "right_not_decision_grade",
        ),
        (
            "lossless is not a class",
            ("lossless", 16500, None), ("mp3", 16500, None),
            False, "left_not_decision_grade",
        ),
        (
            "no cliff on the right asserts nothing",
            ("mp3", 16500, None), ("mp3", None, None),
            False, "right_not_decision_grade",
        ),
        (
            "no ladder on either side",
            ("aac", 16500, None), ("opus", 16500, None),
            False, "left_not_decision_grade",
        ),
    ]

    def test_comparability_table(self):
        for desc, left_args, right_args, comparable, reason in self.CASES:
            with self.subTest(desc=desc):
                left = interpret_spectral_cliff(
                    left_args[0], spectral_grade="likely_transcode",
                    cliff_hz=left_args[1], stored_bitrate_kbps=left_args[2],
                )
                right = interpret_spectral_cliff(
                    right_args[0], spectral_grade="likely_transcode",
                    cliff_hz=right_args[1], stored_bitrate_kbps=right_args[2],
                )
                result = spectral_classes_comparable(left, right)
                self.assertEqual(result.comparable, comparable)
                self.assertEqual(result.reason, reason)

    def test_an_unauthorized_grade_removes_a_side_from_comparison(self):
        authorized = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode", cliff_hz=16500,
        )
        unauthorized = interpret_spectral_cliff(
            "mp3", spectral_grade="genuine", cliff_hz=18000,
        )
        refusal = spectral_classes_comparable(authorized, unauthorized)
        self.assertFalse(refusal.comparable)
        self.assertEqual(refusal.reason, "right_not_decision_grade")

    def test_cross_codec_is_licensed_only_by_each_codecs_own_ladder(self):
        """The measured 98% MP3<->Vorbis ordering accuracy was obtained on
        classes derived through each codec's OWN ladder. The same two
        codecs read through the single LAME-shaped legacy table are not
        covered by that evidence, so the comparison is withheld."""
        mp3_cliff = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode", cliff_hz=16500,
        )
        vorbis_cliff = interpret_spectral_cliff(
            "vorbis", spectral_grade="likely_transcode", cliff_hz=17000,
        )
        self.assertTrue(
            spectral_classes_comparable(mp3_cliff, vorbis_cliff).comparable
        )

        mp3_legacy = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode", stored_bitrate_kbps=160,
        )
        vorbis_legacy = interpret_spectral_cliff(
            "vorbis", spectral_grade="likely_transcode",
            stored_bitrate_kbps=192,
        )
        refusal = spectral_classes_comparable(mp3_legacy, vorbis_legacy)
        self.assertFalse(refusal.comparable)
        self.assertEqual(refusal.reason, "cross_codec_legacy_bucket")

    def test_cutoff_hz_is_not_a_currency(self):
        """The same 17000Hz cliff means the 160 class in MP3 and only a
        96-class floor in AAC — and the AAC side never enters the
        comparison at all."""
        mp3 = interpret_spectral_cliff(
            "mp3", spectral_grade="likely_transcode", cliff_hz=17000,
        )
        aac = interpret_spectral_cliff(
            "aac", spectral_grade="likely_transcode", cliff_hz=17000,
        )
        self.assertEqual(mp3.inferred_class_kbps, 160)
        self.assertEqual(aac.inferred_class_kbps, AAC_FLOOR_LOW_CLASS_KBPS)
        self.assertFalse(spectral_classes_comparable(mp3, aac).comparable)
        self.assertFalse(spectral_classes_comparable(aac, mp3).comparable)


class TestLiveWorldPins(unittest.TestCase):
    """The real prod rows this PR is built from."""

    def test_wavves_33591_resolves_aac_via_format_not_the_m4a_container(self):
        resolution = resolve_measured_codec_family(WAVVES_EVIDENCE_33591)
        self.assertEqual(resolution.family, "aac")
        self.assertEqual(resolution.basis, "format")

    def test_wavves_33591_m4a_band_does_not_trip_the_mixed_detector(self):
        self.assertFalse(is_mixed_codec_album(WAVVES_EVIDENCE_33591.filetype_band))

    def test_wavves_33591_yields_no_accusation_and_no_class(self):
        """The origin defect: a LAME-table 128 stored on a 256 kbps CBR
        AAC. Under the codec-aware model that stored value is not evidence
        of anything, and AAC can never accuse — even though the row is
        graded ``likely_transcode``."""
        result = interpret_spectral_evidence(WAVVES_EVIDENCE_33591)
        self.assertEqual(result.codec_family, "aac")
        self.assertEqual(result.semantics, "content_floor")
        self.assertTrue(result.floor_only)
        self.assertIsNone(result.inferred_class_kbps)
        self.assertFalse(result.decision_grade)
        self.assertFalse(result.supports_transcode_accusation)
        self.assertEqual(result.reason, "aac_no_cliff")

    def test_wavves_33592_v2_captured_aac_cliff_floors_at_96(self):
        """The best #829 exhibit in the library: a genuinely v2-captured
        AAC cliff at 15500Hz on a 256 kbps source. The LAME table buckets
        that as a 128 transcode; the codec-aware model says only "at least
        the 96 class", and never accuses."""
        result = interpret_spectral_evidence(WAVVES_EVIDENCE_33592)
        self.assertEqual(result.codec_family, "aac")
        self.assertEqual(result.semantics, "content_floor")
        self.assertEqual(result.inferred_class_kbps, AAC_FLOOR_LOW_CLASS_KBPS)
        self.assertEqual(result.basis, "cliff_hz")
        self.assertEqual(result.reason, "aac_content_floor_low")
        self.assertTrue(result.floor_only)
        self.assertFalse(result.decision_grade)
        self.assertFalse(result.supports_transcode_accusation)

    def test_wavves_33592_resolves_through_the_pr1_capture(self):
        resolution = resolve_measured_codec_family(WAVVES_EVIDENCE_33592)
        self.assertEqual(resolution.family, "aac")
        self.assertEqual(resolution.basis, "codec_family")

    def test_evidence_33735_deficit_only_fake_flac_still_accuses(self):
        """A FLAC graded ``likely_transcode`` with no ``cliff_hz`` and no
        stored bucket — the HF-deficit leg alone. Reading cliff presence
        instead of the grade would disarm the fake-FLAC detector on 890
        live rows."""
        result = interpret_spectral_evidence(DEFICIT_ONLY_FAKE_FLAC_33735)
        self.assertEqual(result.codec_family, "lossless")
        self.assertEqual(result.semantics, "lossless_authenticity")
        self.assertTrue(result.supports_transcode_accusation)
        self.assertEqual(result.reason, "lossless_transcode_grade")

    def test_evidence_3689_genuine_r19_row_does_not_accuse(self):
        """An Opus copy wearing its source FLAC's spectral evidence,
        graded ``genuine`` but carrying a stored bucket of 198. The bucket
        is not a verdict — 1,589 live rows."""
        result = interpret_spectral_evidence(R19_GENUINE_LOSSLESS_3689)
        self.assertEqual(result.codec_family, "lossless")
        self.assertFalse(result.supports_transcode_accusation)
        self.assertEqual(result.reason, "lossless_grade_not_transcode")

    def test_evidence_5144_container_bitrate_never_becomes_a_class(self):
        """``spectral_bitrate_kbps=202`` on this row IS the container
        bitrate, not a LAME bucket — 2,503 of 30,251 live rows carry
        container bitrates there, up to 738. The grade gate is what keeps
        it out of class space: the row is ``genuine``."""
        result = interpret_spectral_evidence(CONTAINER_BITRATE_IN_SPECTRAL_5144)
        self.assertEqual(result.codec_family, "mp3")
        self.assertIsNone(result.inferred_class_kbps)
        self.assertFalse(result.decision_grade)
        self.assertEqual(result.reason, "ladder_grade_not_transcode")
        self.assertFalse(result.supports_transcode_accusation)

    def test_fall_2007_34219_rederives_160_from_its_captured_cliff(self):
        result = interpret_spectral_evidence(FALL_2007_34219)
        self.assertEqual(result.codec_family, "mp3")
        self.assertEqual(result.inferred_class_kbps, 160)
        self.assertEqual(result.basis, "cliff_hz")
        self.assertTrue(result.decision_grade)

    def test_fall_2007_34219_is_not_comparable_to_its_own_legacy_bucket(self):
        """The upgrade-loop mechanism: the same album's re-derived 160 and
        its stored legacy 128 are one tier apart purely by derivation, so
        the two must never be compared against each other."""
        from dataclasses import replace

        rederived = interpret_spectral_evidence(FALL_2007_34219)
        legacy = interpret_spectral_evidence(
            replace(FALL_2007_34219, cliff_hz=None)
        )
        self.assertEqual(rederived.inferred_class_kbps, 160)
        self.assertEqual(legacy.inferred_class_kbps, 128)
        self.assertEqual(rederived.basis, "cliff_hz")
        self.assertEqual(legacy.basis, "stored_bucket")

        result = spectral_classes_comparable(rederived, legacy)
        self.assertFalse(result.comparable)
        self.assertEqual(result.reason, "mixed_derivation_basis")

    def test_r19_lossless_lineage_cohort_keeps_its_source_semantics(self):
        """6,193 live rows are ``codec=opus, was_converted_from=flac,
        spectral_subject=source`` — the Opus copy wears its source FLAC's
        spectral evidence, so the measured subject is lossless."""
        facts = SpectralEvidenceFacts(
            spectral_grade="genuine",
            codec_family=None,
            format="opus",
            storage_format="opus 128",
            was_converted_from="flac",
            spectral_subject="source",
            cliff_hz=None,
            spectral_bitrate_kbps=None,
        )
        result = interpret_spectral_evidence(facts)
        self.assertEqual(result.codec_family, "lossless")
        self.assertEqual(result.semantics, "lossless_authenticity")
        self.assertFalse(result.supports_transcode_accusation)


class TestEveryCodecFamilyIsRouted(unittest.TestCase):
    """No family falls off the end of the interpretation switch."""

    FAMILIES: ClassVar[list[CodecFamily]] = [
        "mp3", "aac", "opus", "vorbis", "lossless", "other",
    ]

    def test_every_family_produces_a_typed_semantics(self):
        expected = {
            "mp3": "ladder",
            "vorbis": "ladder",
            "aac": "content_floor",
            "opus": "audit_only",
            "lossless": "lossless_authenticity",
            "other": "audit_only",
        }
        for family in self.FAMILIES:
            with self.subTest(family=family):
                result = interpret_spectral_cliff(
                    family, spectral_grade="likely_transcode", cliff_hz=16500,
                )
                self.assertEqual(result.semantics, expected[family])
                self.assertEqual(result.codec_family, family)


if __name__ == "__main__":
    unittest.main()
