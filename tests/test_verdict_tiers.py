"""Deterministic pins for the proof-gate verdict tiers (issue #829 PR4)."""

import unittest
from typing import ClassVar

from lib.quality import (
    PRODUCIBLE_PROOF_TIERS,
    PROOF_LEG_AAC_LATTICE,
    PROOF_LEG_IN_WINDOW_CLIFF,
    PROOF_LEG_NO_ULTRASONIC,
    PROOF_TIER_CEILING_AND_NO_ULTRASONIC,
    PROOF_TIER_CEILING_ONLY,
    PROOF_TIER_DETECTED,
    PROOF_TIER_NO_FINDING,
    PROOF_TIER_NO_ULTRASONIC,
    VERIFIED_LOSSLESS_CLASSIFIER,
    VERIFIED_LOSSLESS_CLASSIFIER_V3,
    VERIFIED_LOSSLESS_CLASSIFIER_V4,
    AacLatticeCapture,
    album_proof_verdict,
    interpret_spectral_cliff,
    proof_tier_statement,
    proof_verdict_from_facts,
    ultrasonic_proof_leg,
    verified_lossless_generation_label,
)
from lib.quality.decisions import (
    AAC_LATTICE_PROOF_DENY_MODAL_COUNT,
    ULTRASONIC_PROOF_DENY_DEFICIT_DB,
    aac_lattice_proof_leg,
)


def _spectral(family, grade, *, cliff_hz=None):
    return interpret_spectral_cliff(
        family, spectral_grade=grade, cliff_hz=cliff_hz)


def _ultrasonic(deficit):
    """A sox-native, current-measurement ultrasonic leg at ``deficit`` dB."""
    return ultrasonic_proof_leg(
        deficit_db=deficit,
        spectral_measurement_version=2,
        decode_path="sox_native",
        preserved_source_spectral=False,
    )


def _lattice(modal_count, scored_tracks=6):
    return aac_lattice_proof_leg(AacLatticeCapture(
        modal_count=modal_count, scored_tracks=scored_tracks, max_z=3.0))


class TestUltrasonicProofDenyDeficitConstant(unittest.TestCase):
    """Every other test here exercises this deny threshold only relative to
    itself (``_ultrasonic(ULTRASONIC_PROOF_DENY_DEFICIT_DB)``,
    ``... + 5.0``), never pins its own value — a changed threshold moves
    which albums the proof gate denies and must be a deliberate edit.
    """

    def test_value(self) -> None:
        self.assertEqual(ULTRASONIC_PROOF_DENY_DEFICIT_DB, 59.5)


class TestAlbumProofVerdict(unittest.TestCase):
    """Every branch of the tier ladder, one row per branch."""

    DENY = ULTRASONIC_PROOF_DENY_DEFICIT_DB
    K = AAC_LATTICE_PROOF_DENY_MODAL_COUNT

    CASES: ClassVar = [
        # (desc, spectral, ultrasonic, lattice, tier, fired legs)
        (
            "lossless graded likely_transcode fires the cliff leg",
            _spectral("lossless", "likely_transcode"), None, None,
            PROOF_TIER_DETECTED, (PROOF_LEG_IN_WINDOW_CLIFF,),
        ),
        (
            "mp3 graded suspect fires the cliff leg",
            _spectral("mp3", "suspect", cliff_hz=15000), None, None,
            PROOF_TIER_DETECTED, (PROOF_LEG_IN_WINDOW_CLIFF,),
        ),
        (
            "AAC graded likely_transcode fires NOTHING — #829's whole point",
            _spectral("aac", "likely_transcode", cliff_hz=15000), None, None,
            PROOF_TIER_NO_FINDING, (),
        ),
        (
            "Opus graded likely_transcode fires nothing either",
            _spectral("opus", "likely_transcode", cliff_hz=15000), None, None,
            PROOF_TIER_NO_FINDING, (),
        ),
        (
            "lattice denial alone is tier 1 on a genuine-graded album",
            _spectral("lossless", "genuine"), None, _lattice(K),
            PROOF_TIER_DETECTED, (PROOF_LEG_AAC_LATTICE,),
        ),
        (
            "ultrasonic denial alone is tier 4",
            _spectral("lossless", "genuine"), _ultrasonic(DENY), None,
            PROOF_TIER_NO_ULTRASONIC, (PROOF_LEG_NO_ULTRASONIC,),
        ),
        (
            "cliff outranks a simultaneous ultrasonic denial",
            _spectral("lossless", "likely_transcode"), _ultrasonic(DENY), None,
            PROOF_TIER_DETECTED,
            (PROOF_LEG_IN_WINDOW_CLIFF, PROOF_LEG_NO_ULTRASONIC),
        ),
        (
            "every leg clear is tier 5",
            _spectral("lossless", "genuine"), _ultrasonic(DENY - 10.0),
            _lattice(1),
            PROOF_TIER_NO_FINDING, (),
        ),
        (
            "a withheld ultrasonic leg asserts nothing",
            _spectral("lossless", "genuine"),
            ultrasonic_proof_leg(
                deficit_db=None, spectral_measurement_version=2,
                decode_path="sox_native", preserved_source_spectral=False),
            None,
            PROOF_TIER_NO_FINDING, (),
        ),
        (
            "a withheld lattice leg asserts nothing",
            _spectral("lossless", "genuine"), None,
            aac_lattice_proof_leg(None),
            PROOF_TIER_NO_FINDING, (),
        ),
    ]

    def test_tier_and_fired_legs(self):
        for desc, spectral, ultra, lattice, tier, fired in self.CASES:
            with self.subTest(desc=desc):
                verdict = album_proof_verdict(
                    spectral=spectral,
                    spectral_grade="likely_transcode"
                    if spectral.supports_transcode_accusation
                    else "genuine",
                    ultrasonic_leg=ultra,
                    aac_lattice_leg=lattice,
                )
                self.assertEqual(verdict.tier, tier)
                self.assertEqual(set(verdict.fired_legs), set(fired))
                self.assertIn(verdict.tier, PRODUCIBLE_PROOF_TIERS)

    def test_ceiling_tiers_are_reserved_and_never_produced(self):
        """No input can produce tier 2 or 3 — production has no ceiling leg.

        The reserved numbers exist so the shipped tiers keep meaning what
        the Phase 5 plan §1 table measured; nothing may emit them, because
        no copy is shipped for them (Rule C).
        """
        for reserved in (
            PROOF_TIER_CEILING_AND_NO_ULTRASONIC, PROOF_TIER_CEILING_ONLY,
        ):
            self.assertNotIn(reserved, PRODUCIBLE_PROOF_TIERS)
        for desc, spectral, ultra, lattice, _tier, _fired in self.CASES:
            with self.subTest(desc=desc):
                verdict = album_proof_verdict(
                    spectral=spectral, ultrasonic_leg=ultra,
                    aac_lattice_leg=lattice)
                self.assertNotIn(verdict.tier, (
                    PROOF_TIER_CEILING_AND_NO_ULTRASONIC,
                    PROOF_TIER_CEILING_ONLY,
                ))


class TestEvaluatedLegsSeparateSilenceFromClearance(unittest.TestCase):
    """Tier 5 means two very different things; ``has_finding`` splits them."""

    def test_ungraded_album_has_no_finding(self):
        verdict = album_proof_verdict(
            spectral=_spectral("lossless", None), spectral_grade=None)
        self.assertEqual(verdict.tier, PROOF_TIER_NO_FINDING)
        self.assertFalse(verdict.has_finding)
        self.assertEqual(verdict.evaluated_legs, ())
        self.assertEqual(
            proof_tier_statement(verdict),
            "No proof-gate test could run on this album")

    def test_graded_genuine_lossless_album_has_a_finding(self):
        verdict = album_proof_verdict(
            spectral=_spectral("lossless", "genuine"),
            spectral_grade="genuine")
        self.assertEqual(verdict.tier, PROOF_TIER_NO_FINDING)
        self.assertTrue(verdict.has_finding)
        self.assertEqual(
            verdict.evaluated_legs, (PROOF_LEG_IN_WINDOW_CLIFF,))

    def test_aac_album_never_evaluates_the_cliff_leg(self):
        """The audit-only codecs are untested, not cleared."""
        verdict = album_proof_verdict(
            spectral=_spectral("aac", "likely_transcode", cliff_hz=15000),
            spectral_grade="likely_transcode")
        self.assertEqual(verdict.evaluated_legs, ())
        self.assertFalse(verdict.has_finding)
        self.assertFalse(verdict.spectral_accusation_admissible)


class TestProofTierStatement(unittest.TestCase):
    """The one operator sentence per tier, and what it may not claim."""

    def test_tier_one_names_the_instrument(self):
        cliff = album_proof_verdict(
            spectral=_spectral("lossless", "likely_transcode"),
            spectral_grade="likely_transcode")
        self.assertEqual(
            proof_tier_statement(cliff),
            "Transcode detected: in-window spectral cliff")
        lattice = album_proof_verdict(
            spectral=_spectral("lossless", "genuine"),
            spectral_grade="genuine",
            aac_lattice_leg=_lattice(AAC_LATTICE_PROOF_DENY_MODAL_COUNT))
        self.assertEqual(
            proof_tier_statement(lattice),
            "Transcode detected: AAC encoder frame lattice")

    def test_tier_four_is_distinct_from_a_finding(self):
        verdict = album_proof_verdict(
            spectral=_spectral("lossless", "genuine"),
            spectral_grade="genuine",
            ultrasonic_leg=_ultrasonic(ULTRASONIC_PROOF_DENY_DEFICIT_DB))
        self.assertEqual(
            proof_tier_statement(verdict),
            "No ultrasonic content — not spectrally provable")

    def test_no_statement_claims_bit_faithfulness_or_posterior_odds(self):
        """§1.7 bounds the claim; §1's base-rate caveat bounds the wording."""
        forbidden = (
            "bit-perfect", "bit perfect", "bit-faithful", "guaranteed",
            "probably fake", "fake", "proven lossless",
        )
        statements = [
            proof_tier_statement(album_proof_verdict(
                spectral=_spectral("lossless", grade),
                spectral_grade=grade,
                ultrasonic_leg=ultra,
                aac_lattice_leg=lattice,
            ))
            for grade in ("genuine", "likely_transcode")
            for ultra in (
                None,
                _ultrasonic(ULTRASONIC_PROOF_DENY_DEFICIT_DB),
                _ultrasonic(10.0),
            )
            for lattice in (
                None, _lattice(1), _lattice(AAC_LATTICE_PROOF_DENY_MODAL_COUNT),
            )
        ]
        self.assertTrue(statements)
        for statement in statements:
            for token in forbidden:
                self.assertNotIn(token, statement.lower(), statement)


class TestVerifiedLosslessGenerationLabel(unittest.TestCase):
    """Every classifier a producer can mint has a label — Rule C."""

    def test_every_minted_classifier_is_labelled(self):
        for classifier in (
            VERIFIED_LOSSLESS_CLASSIFIER,
            VERIFIED_LOSSLESS_CLASSIFIER_V3,
            VERIFIED_LOSSLESS_CLASSIFIER_V4,
        ):
            with self.subTest(classifier=classifier):
                label = verified_lossless_generation_label(classifier)
                self.assertIsNotNone(label)
                assert label is not None
                self.assertNotEqual(label, classifier)

    def test_no_proof_has_no_label(self):
        self.assertIsNone(verified_lossless_generation_label(None))
        self.assertIsNone(verified_lossless_generation_label(""))

    def test_unknown_generation_is_echoed_not_described(self):
        """A renderer must never describe a test suite it does not know."""
        self.assertEqual(
            verified_lossless_generation_label("spectral_verified_lossless_v9"),
            "spectral_verified_lossless_v9")


class TestProofVerdictFromFacts(unittest.TestCase):
    """The flat-facts adapter the render path uses."""

    def _facts(self, **overrides):
        facts = {
            "spectral_grade": "genuine",
            "spectral_bitrate_kbps": None,
            "cliff_hz": None,
            "codec_family": None,
            "format": "FLAC",
            "storage_format": "FLAC",
            "filetype_band": "flac",
            "spectral_subject": "source",
            "was_converted_from": None,
            "container_labels": [".flac"],
            "ultrasonic_deficit_db": None,
            "spectral_measurement_version": 2,
            "aac_lattice": None,
        }
        facts.update(overrides)
        return facts

    def test_flac_transcode_grade_is_tier_one(self):
        verdict = proof_verdict_from_facts(
            **self._facts(spectral_grade="likely_transcode"))
        self.assertEqual(verdict.tier, PROOF_TIER_DETECTED)
        self.assertTrue(verdict.spectral_accusation_admissible)

    def test_aac_transcode_grade_is_never_an_accusation(self):
        verdict = proof_verdict_from_facts(**self._facts(
            spectral_grade="likely_transcode",
            format="AAC", storage_format="AAC", filetype_band="m4a",
            container_labels=[".m4a"], cliff_hz=15000,
        ))
        self.assertFalse(verdict.spectral_accusation_admissible)
        self.assertEqual(verdict.fired_legs, ())

    def test_ultrasonic_denial_needs_a_sox_native_decode_path(self):
        """A non-sox container's deficit is on a different scale (§1.5c)."""
        sox = proof_verdict_from_facts(**self._facts(
            ultrasonic_deficit_db=ULTRASONIC_PROOF_DENY_DEFICIT_DB + 5.0))
        self.assertEqual(sox.tier, PROOF_TIER_NO_ULTRASONIC)
        ffmpeg = proof_verdict_from_facts(**self._facts(
            ultrasonic_deficit_db=ULTRASONIC_PROOF_DENY_DEFICIT_DB + 5.0,
            container_labels=[".m4a"],
        ))
        self.assertEqual(ffmpeg.tier, PROOF_TIER_NO_FINDING)
        self.assertEqual(ffmpeg.ultrasonic_outcome, "withheld")

    def test_mixed_codec_album_withholds_everything(self):
        verdict = proof_verdict_from_facts(**self._facts(
            spectral_grade="likely_transcode", filetype_band="mixed"))
        self.assertFalse(verdict.spectral_accusation_admissible)
        self.assertFalse(verdict.has_finding)


if __name__ == "__main__":
    unittest.main()
