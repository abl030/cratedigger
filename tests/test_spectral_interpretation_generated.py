"""Generated tests — issue #829 Phase 5 PR2a per-codec spectral interpretation.

The deterministic pins in ``tests/test_spectral_interpretation.py`` prove
the exact measured ladder boundaries and the live worlds this PR is built
from (evidence 33591 / 33592 / 34219 / 33735 / 3689 / 5144, every field a
real persisted value). These properties patrol the world space around
them: any evidence-field combination, any grade, any cliff, any stored
bucket, any SBR tri-state.

Nine invariants, each shipped as a PAIR (deterministic pin + generated
property) per ``.claude/rules/code-quality.md``, and each with a
module-level checker plus a known-bad self-test proving the checker
actually trips on a planted violation:

1. The measured-codec family resolves to exactly what the evidence
   supports — checked against an INDEPENDENT oracle, not against the
   resolver itself — and an unresolved family never yields a class, an
   accusation, or a comparison.
2. AAC never yields a transcode accusation, never a decision-grade class,
   and never anything but a one-sided content floor.
3. Opus never yields a decision-grade class — audit-only, unconditional.
4. Two interpretations whose classes were derived differently are never
   comparable.
5. Both ladders are monotonic non-decreasing in cliff Hz.
6. A stored legacy ``spectral_bitrate_kbps`` never becomes an inferred
   class for a codec with no invertible ladder — the download-37946
   defect, as a permanent law.
7. ``sbr_present`` never disarms the lossless fake-lossless detector,
   whatever an object-type probe reports, and that detector tracks
   production's spectral GRADE.
8. A granted cross-codec comparison is always in ``cliff_hz`` basis —
   two codecs are never weighed against each other through the single
   LAME-shaped legacy table.
9. No spectral finding — no inferred class that can order two albums,
   no transcode accusation — without an authorizing ``spectral_grade``.
   This module reads production's verdict; it never manufactures one.
10. A class derived from the legacy stored column is always a
    ``LAME_LOWPASS`` member — a container bitrate parked in that column
    never becomes a ladder class.

Checkers that would otherwise need a mutated production module take their
dependency as a keyword-only argument defaulting to the real function
(the ``decider=`` seam pattern from
``tests/test_spectral_capture_generated.py``). The self-tests pass a
decoy explicitly; production always gets the default.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import os
import sys
import unittest
from collections.abc import Callable
from dataclasses import replace

from hypothesis import example, given
from hypothesis import strategies as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.quality import (
    AAC_FLOOR_HIGH_CLASS_KBPS,
    AAC_FLOOR_LOW_CLASS_KBPS,
    CodecFamily,
    SpectralComparability,
    SpectralEvidenceFacts,
    SpectralInterpretation,
    interpret_spectral_cliff,
    interpret_spectral_evidence,
    ladder_class_kbps,
    resolve_measured_codec_family,
    spectral_classes_comparable,
)

Comparator = Callable[
    [SpectralInterpretation, SpectralInterpretation], SpectralComparability
]
LadderFn = Callable[[CodecFamily, int], "int | None"]
Interpreter = Callable[..., SpectralInterpretation]

_CODEC_FAMILIES: tuple[CodecFamily, ...] = (
    "mp3", "aac", "opus", "vorbis", "lossless", "other",
)

#: Format / storage_format / was_converted_from labels seen live, plus the
#: junk a fail-closed resolver must survive. No plausibility filter: every
#: label is drawn against every other field.
_FORMAT_LABELS: tuple[str, ...] = (
    "MP3", "mp3", "mp3 v0", "mp3 320",
    "AAC", "aac", "ALAC", "alac",
    "m4a", "M4A", "mp4",
    "opus", "Opus", "opus 128",
    "vorbis", "ogg", "oga",
    "FLAC", "flac", "wav", "WAV", "aiff", "ape",
    "WMA", "wma",
    "", "   ", ".mp3", "shorten", "unknown-codec", "0",
)

#: Every band ``derive_filetype_band`` can emit plus the comma-joined slskd
#: filetype strings that reach the column directly.
_FILETYPE_BANDS: tuple[str, ...] = (
    "", "mp3", "flac", "m4a", "opus", "ogg",
    "mixed", "mixed_lossy", "mixed_lossless",
    "m4a, mp3", "flac, m4a", "MIXED_LOSSY", "  ",
)

_SUBJECTS = ("installed", "source")

#: Every grade ``classify_album`` can emit, the ``error`` grade the
#: measurement path writes, plus junk a fail-closed gate must survive.
_GRADE_VALUES: tuple["str | None", ...] = (
    "genuine", "marginal", "suspect", "likely_transcode", "error",
    None, "", "LIKELY_TRANSCODE", "unknown-future-grade",
)

#: The authorizing grade, spelled once. Every call site states a grade
#: explicitly — a default would re-hide the input whose absence was the
#: BLOCKING defect.
_T = "likely_transcode"

#: Real ``LAME_LOWPASS`` class values, and the real non-bucket tail
#: observed on the live corpus for authorizing-graded rows. Drawn as a
#: union with the unconstrained range so both populations are
#: well-represented — a union, not a plausibility filter.
_LAME_BUCKET_SAMPLES = (96, 112, 128, 160, 192, 224, 256, 320)
_LIVE_NON_BUCKET_SAMPLES = (121, 122, 123, 124, 126, 130, 131, 198, 202,
                            223, 226, 234, 235, 236, 244, 247, 738)

_CLIFFS = st.one_of(st.none(), st.integers(min_value=0, max_value=24000))
_STORED = st.one_of(
    st.none(),
    st.sampled_from(_LAME_BUCKET_SAMPLES),
    st.sampled_from(_LIVE_NON_BUCKET_SAMPLES),
    st.integers(min_value=-10, max_value=1400),
)
_SBR = st.one_of(st.none(), st.booleans())
_GRADES = st.sampled_from(_GRADE_VALUES)


@st.composite
def _spectral_evidence_facts(draw: st.DrawFn) -> SpectralEvidenceFacts:
    """Any evidence-field world. Deliberately unconstrained: an impossible
    combination must still fail closed rather than crash or assert."""
    return SpectralEvidenceFacts(
        spectral_grade=draw(_GRADES),
        codec_family=draw(
            st.one_of(st.none(), st.sampled_from(_CODEC_FAMILIES))
        ),
        spectral_subject=draw(
            st.one_of(st.none(), st.sampled_from(_SUBJECTS))
        ),
        was_converted_from=draw(
            st.one_of(st.none(), st.sampled_from(_FORMAT_LABELS))
        ),
        format=draw(st.one_of(st.none(), st.sampled_from(_FORMAT_LABELS))),
        storage_format=draw(
            st.one_of(st.none(), st.sampled_from(_FORMAT_LABELS))
        ),
        filetype_band=draw(st.sampled_from(_FILETYPE_BANDS)),
        cliff_hz=draw(_CLIFFS),
        spectral_bitrate_kbps=draw(_STORED),
        sbr_present=draw(_SBR),
    )


@st.composite
def _interpretations(draw: st.DrawFn) -> SpectralInterpretation:
    """Any interpretation the production entry point can produce."""
    return interpret_spectral_cliff(
        draw(st.one_of(st.none(), st.sampled_from(_CODEC_FAMILIES))),
        spectral_grade=draw(_GRADES),
        cliff_hz=draw(_CLIFFS),
        stored_bitrate_kbps=draw(_STORED),
        sbr_present=draw(_SBR),
    )


# ---------------------------------------------------------------------------
# Invariant 1: the resolver resolves exactly what the evidence supports,
# and an unresolved family asserts nothing, ever.
#
# The precondition below is derived from an INDEPENDENT oracle, never from
# ``resolve_measured_codec_family`` itself. A checker that asks the
# function under test "did you resolve?" is unfalsifiable: any mutant that
# resolves MORE aggressively simply makes the property vacuous. Three such
# mutants (``m4a -> aac``, ``is_mixed_codec_album`` always False,
# ``conflicting_labels`` guard deleted) survived the earlier self-
# referential version of this property.
# ---------------------------------------------------------------------------

#: Independently restated label vocabulary. Deliberately a different SHAPE
#: from production's single dict — one set per family, explicit ambiguity
#: set, hand-rolled dot stripping — so a production table edit cannot
#: silently move both sides together.
_ORACLE_LOSSLESS_TOKENS = frozenset(
    {"flac", "wav", "wave", "alac", "aiff", "aif", "ape"}
)
_ORACLE_OTHER_TOKENS = frozenset({"wma"})
_ORACLE_AMBIGUOUS_TOKENS = frozenset({"m4a", "mp4", "ogg", "oga"})
_ORACLE_LOSSY_TOKENS: dict[str, CodecFamily] = {
    "mp3": "mp3", "aac": "aac", "opus": "opus", "vorbis": "vorbis",
}
_ORACLE_MIXED_BANDS = frozenset({"mixed", "mixed_lossy"})


def _oracle_family(label: "str | None") -> "CodecFamily | None":
    """Independent restatement of label -> measured codec family."""
    if label is None:
        return None
    text = label.strip().lower()
    if not text:
        return None
    head = text.split(" ")[0]
    while head.startswith("."):
        head = head[1:]
    if not head or head in _ORACLE_AMBIGUOUS_TOKENS:
        return None
    if head in _ORACLE_LOSSLESS_TOKENS:
        return "lossless"
    if head in _ORACLE_OTHER_TOKENS:
        return "other"
    return _ORACLE_LOSSY_TOKENS.get(head)


def _oracle_resolution(
    facts: SpectralEvidenceFacts,
) -> "tuple[CodecFamily | None, str]":
    """Independent restatement of the whole resolution ladder."""
    band = (facts.filetype_band or "").strip().lower()
    if band and ("," in band or band in _ORACLE_MIXED_BANDS):
        return None, "mixed_album"
    if facts.codec_family is not None:
        return facts.codec_family, "codec_family"
    if facts.spectral_subject == "source" and facts.was_converted_from is not None:
        source = _oracle_family(facts.was_converted_from)
        if source is None:
            return None, "unresolved"
        return source, "was_converted_from"
    from_format = _oracle_family(facts.format)
    from_storage = _oracle_family(facts.storage_format)
    if (
        from_format is not None
        and from_storage is not None
        and from_format != from_storage
    ):
        return None, "conflicting_labels"
    if from_format is not None:
        return from_format, "format"
    if from_storage is not None:
        return from_storage, "storage_format"
    return None, "unresolved"


def unresolved_family_asserts_nothing(
    facts: SpectralEvidenceFacts,
    interpretation: SpectralInterpretation,
    partner: SpectralInterpretation,
) -> bool:
    """Invariant checker, two clauses of one law.

    First: ``resolve_measured_codec_family`` must return exactly what the
    independent oracle says the evidence supports — same family, same
    basis. This clause is what kills a resolver that guesses at an
    ambiguous container, stops detecting mixed-codec albums, or drops the
    contradictory-label guard.

    Second: when the evidence supports NO family, the interpretation must
    carry no class, no accusation, no decision grade, and must not be
    comparable against ``partner`` in either direction.
    """
    expected_family, expected_basis = _oracle_resolution(facts)
    actual = resolve_measured_codec_family(facts)
    if actual.family != expected_family or actual.basis != expected_basis:
        return False
    if expected_family is not None:
        return True
    return (
        interpretation.codec_family is None
        and interpretation.semantics == "audit_only"
        and interpretation.inferred_class_kbps is None
        and interpretation.decision_grade is False
        and interpretation.invertible_ladder is False
        and interpretation.floor_only is False
        and interpretation.supports_transcode_accusation is False
        and interpretation.basis == "none"
        and not spectral_classes_comparable(interpretation, partner).comparable
        and not spectral_classes_comparable(partner, interpretation).comparable
    )


class TestUnresolvedFamilyCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip on a planted
    unresolved-family interpretation that leaks a class, an accusation, or
    a comparison, AND on a resolver that disagrees with the oracle."""

    UNRESOLVED = SpectralEvidenceFacts(format="m4a")
    PARTNER = interpret_spectral_cliff(
        "mp3", spectral_grade="likely_transcode", cliff_hz=16500,
    )

    def test_checker_trips_when_the_resolver_disagrees_with_the_oracle(self):
        """The oracle clause, proven directly: three planted resolver
        behaviours the earlier self-referential checker could not see."""
        cases = (
            ("guesses at an ambiguous container",
             SpectralEvidenceFacts(format="m4a"), "aac", "format"),
            ("stops detecting mixed-codec albums",
             SpectralEvidenceFacts(format="MP3", filetype_band="m4a, mp3"),
             "mp3", "format"),
            ("drops the contradictory-label guard",
             SpectralEvidenceFacts(format="MP3", storage_format="FLAC"),
             "mp3", "format"),
        )
        for desc, facts, mutant_family, mutant_basis in cases:
            with self.subTest(desc=desc):
                expected_family, expected_basis = _oracle_resolution(facts)
                self.assertNotEqual(
                    (expected_family, expected_basis),
                    (mutant_family, mutant_basis),
                    "the oracle must disagree with the mutant, or the "
                    "clause proves nothing",
                )

    def test_checker_passes_for_the_real_production_result(self):
        self.assertTrue(
            unresolved_family_asserts_nothing(
                self.UNRESOLVED,
                interpret_spectral_evidence(self.UNRESOLVED),
                self.PARTNER,
            )
        )

    def test_checker_trips_when_an_unresolved_world_leaks_a_class(self):
        leaky = replace(
            interpret_spectral_evidence(self.UNRESOLVED),
            inferred_class_kbps=128,
        )
        self.assertFalse(
            unresolved_family_asserts_nothing(
                self.UNRESOLVED, leaky, self.PARTNER,
            )
        )

    def test_checker_trips_when_an_unresolved_world_becomes_comparable(self):
        comparable = replace(
            interpret_spectral_evidence(self.UNRESOLVED),
            codec_family="mp3",
            semantics="ladder",
            inferred_class_kbps=128,
            decision_grade=True,
            invertible_ladder=True,
            basis="cliff_hz",
        )
        self.assertTrue(
            spectral_classes_comparable(comparable, self.PARTNER).comparable
        )
        self.assertFalse(
            unresolved_family_asserts_nothing(
                self.UNRESOLVED, comparable, self.PARTNER,
            )
        )

    def test_checker_trips_when_an_unresolved_world_accuses(self):
        accusing = replace(
            interpret_spectral_evidence(self.UNRESOLVED),
            supports_transcode_accusation=True,
        )
        self.assertFalse(
            unresolved_family_asserts_nothing(
                self.UNRESOLVED, accusing, self.PARTNER,
            )
        )

    def test_checker_is_vacuous_when_the_family_resolves(self):
        resolved = SpectralEvidenceFacts(format="MP3", cliff_hz=16500)
        self.assertTrue(
            unresolved_family_asserts_nothing(
                resolved,
                interpret_spectral_evidence(resolved),
                self.PARTNER,
            )
        )


class TestUnresolvedFamilyNeverAsserts(unittest.TestCase):
    """Pin + generated property: an unresolvable measured subject fails
    closed everywhere."""

    PARTNER = interpret_spectral_cliff("mp3", spectral_grade=_T, cliff_hz=16500)

    def test_pin_ambiguous_m4a_container(self):
        facts = SpectralEvidenceFacts(format="m4a", spectral_bitrate_kbps=128)
        self.assertTrue(
            unresolved_family_asserts_nothing(
                facts, interpret_spectral_evidence(facts), self.PARTNER,
            )
        )

    def test_pin_mixed_codec_album_outranks_a_pr1_capture(self):
        facts = SpectralEvidenceFacts(
            codec_family="mp3", filetype_band="m4a, mp3", cliff_hz=16500,
        )
        interpretation = interpret_spectral_evidence(facts)
        self.assertEqual(interpretation.reason, "mixed_codec_album")
        self.assertTrue(
            unresolved_family_asserts_nothing(
                facts, interpretation, self.PARTNER,
            )
        )

    @given(facts=_spectral_evidence_facts(), partner=_interpretations())
    @example(
        facts=SpectralEvidenceFacts(format="ogg", cliff_hz=16500),
        partner=interpret_spectral_cliff("mp3", spectral_grade=_T, cliff_hz=16500),
    )
    @example(
        facts=SpectralEvidenceFacts(format="MP3", storage_format="FLAC"),
        partner=interpret_spectral_cliff("vorbis", spectral_grade=_T, cliff_hz=17000),
    )
    def test_across_generated_worlds(self, facts, partner):
        self.assertTrue(
            unresolved_family_asserts_nothing(
                facts, interpret_spectral_evidence(facts), partner,
            )
        )


# ---------------------------------------------------------------------------
# Invariant 2: AAC is a one-sided floor and never an accusation.
# ---------------------------------------------------------------------------

def aac_is_floor_only_and_never_accuses(
    interpretation: SpectralInterpretation,
) -> bool:
    """Invariant checker: an AAC interpretation must be a content floor —
    never decision-grade, never an invertible ladder, never a transcode
    accusation — and any class it does carry must be one of the two
    measured floor values.

    Vacuously true for any other codec family.
    """
    if interpretation.codec_family != "aac":
        return True
    if interpretation.semantics == "audit_only":
        # The SBR pre-gate legitimately demotes an AAC row to audit-only;
        # that is strictly weaker than a floor, so it satisfies the law.
        return (
            interpretation.inferred_class_kbps is None
            and interpretation.decision_grade is False
            and interpretation.supports_transcode_accusation is False
        )
    return (
        interpretation.semantics == "content_floor"
        and interpretation.floor_only is True
        and interpretation.decision_grade is False
        and interpretation.invertible_ladder is False
        and interpretation.supports_transcode_accusation is False
        and interpretation.inferred_class_kbps in (
            None, AAC_FLOOR_LOW_CLASS_KBPS, AAC_FLOOR_HIGH_CLASS_KBPS,
        )
    )


class TestAacFloorCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the AAC checker must trip on an accusation, a
    decision-grade class, and a class off the measured floor values."""

    BASE = interpret_spectral_cliff("aac", spectral_grade=_T, cliff_hz=16500)

    def test_checker_passes_for_the_real_production_result(self):
        self.assertTrue(aac_is_floor_only_and_never_accuses(self.BASE))

    def test_checker_trips_on_a_transcode_accusation(self):
        self.assertFalse(
            aac_is_floor_only_and_never_accuses(
                replace(self.BASE, supports_transcode_accusation=True)
            )
        )

    def test_checker_trips_on_a_decision_grade_aac(self):
        self.assertFalse(
            aac_is_floor_only_and_never_accuses(
                replace(self.BASE, decision_grade=True)
            )
        )

    def test_checker_trips_on_a_lame_table_class(self):
        """The exact download-37946 shape: the legacy 128 surviving as an
        AAC class."""
        self.assertFalse(
            aac_is_floor_only_and_never_accuses(
                replace(self.BASE, inferred_class_kbps=128)
            )
        )

    def test_checker_is_vacuous_for_other_families(self):
        self.assertTrue(
            aac_is_floor_only_and_never_accuses(
                interpret_spectral_cliff("mp3", spectral_grade=_T, cliff_hz=16500)
            )
        )


class TestAacNeverAccuses(unittest.TestCase):
    """Pin + generated property: AAC contributes a one-sided floor and
    nothing else, whatever was measured."""

    def test_pin_wavves_37946_stored_lame_bucket(self):
        facts = SpectralEvidenceFacts(format="AAC", spectral_bitrate_kbps=128)
        self.assertTrue(
            aac_is_floor_only_and_never_accuses(
                interpret_spectral_evidence(facts)
            )
        )

    def test_pin_fdk_median_cliff(self):
        self.assertTrue(
            aac_is_floor_only_and_never_accuses(
                interpret_spectral_cliff("aac", spectral_grade=_T, cliff_hz=16500)
            )
        )

    @given(grade=_GRADES, cliff_hz=_CLIFFS, stored=_STORED, sbr=_SBR)
    @example(grade=_T, cliff_hz=None, stored=128, sbr=None)
    @example(grade=_T, cliff_hz=12999, stored=None, sbr=None)
    @example(grade="genuine", cliff_hz=18500, stored=320, sbr=False)
    def test_across_generated_worlds(self, grade, cliff_hz, stored, sbr):
        self.assertTrue(
            aac_is_floor_only_and_never_accuses(
                interpret_spectral_cliff(
                    "aac", spectral_grade=grade, cliff_hz=cliff_hz,
                    stored_bitrate_kbps=stored, sbr_present=sbr,
                )
            )
        )


# ---------------------------------------------------------------------------
# Invariant 3: Opus carries no spectral signal at all.
# ---------------------------------------------------------------------------

def opus_is_audit_only(interpretation: SpectralInterpretation) -> bool:
    """Invariant checker: an Opus interpretation asserts nothing — no
    class, no decision grade, no ladder, no floor, no accusation.

    Vacuously true for any other codec family.
    """
    if interpretation.codec_family != "opus":
        return True
    return (
        interpretation.semantics == "audit_only"
        and interpretation.inferred_class_kbps is None
        and interpretation.decision_grade is False
        and interpretation.invertible_ladder is False
        and interpretation.floor_only is False
        and interpretation.supports_transcode_accusation is False
        and interpretation.basis == "none"
    )


class TestOpusAuditOnlyCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the Opus checker must trip on any assertion."""

    BASE = interpret_spectral_cliff("opus", spectral_grade=_T, cliff_hz=14000)

    def test_checker_passes_for_the_real_production_result(self):
        self.assertTrue(opus_is_audit_only(self.BASE))

    def test_checker_trips_on_a_decision_grade_class(self):
        self.assertFalse(
            opus_is_audit_only(
                replace(
                    self.BASE, inferred_class_kbps=96,
                    decision_grade=True, invertible_ladder=True,
                    semantics="ladder", basis="cliff_hz",
                )
            )
        )

    def test_checker_trips_on_a_bare_floor(self):
        self.assertFalse(
            opus_is_audit_only(
                replace(self.BASE, floor_only=True, semantics="content_floor")
            )
        )

    def test_checker_is_vacuous_for_other_families(self):
        self.assertTrue(
            opus_is_audit_only(
                interpret_spectral_cliff("vorbis", spectral_grade=_T, cliff_hz=16500)
            )
        )


class TestOpusNeverDecisionGrade(unittest.TestCase):
    """Pin + generated property: Opus >=32k is statistically
    indistinguishable from genuine lossless on all four arms."""

    def test_pin_opus_with_a_low_cliff_still_asserts_nothing(self):
        self.assertTrue(
            opus_is_audit_only(interpret_spectral_cliff("opus", spectral_grade=_T, cliff_hz=13500))
        )

    def test_pin_opus_carrying_a_legacy_lame_bucket(self):
        self.assertTrue(
            opus_is_audit_only(
                interpret_spectral_cliff(
                    "opus", spectral_grade=_T, stored_bitrate_kbps=96,
                )
            )
        )

    @given(grade=_GRADES, cliff_hz=_CLIFFS, stored=_STORED, sbr=_SBR)
    @example(grade=_T, cliff_hz=16500, stored=128, sbr=None)
    def test_across_generated_worlds(self, grade, cliff_hz, stored, sbr):
        self.assertTrue(
            opus_is_audit_only(
                interpret_spectral_cliff(
                    "opus", spectral_grade=grade, cliff_hz=cliff_hz,
                    stored_bitrate_kbps=stored, sbr_present=sbr,
                )
            )
        )


# ---------------------------------------------------------------------------
# Invariant 4: mixed derivations are never comparable.
# ---------------------------------------------------------------------------

def mixed_derivation_is_never_comparable(
    left: SpectralInterpretation,
    right: SpectralInterpretation,
    *,
    comparator: Comparator = spectral_classes_comparable,
) -> bool:
    """Invariant checker: a granted comparison must always be two
    decision-grade sides that actually carry a class and derived it the
    same way. In particular a class re-derived from ``cliff_hz`` is never
    comparable against a legacy stored bucket — they are systematically one
    tier apart by derivation alone.

    ``comparator`` is injectable ONLY so the known-bad self-test can prove
    this checker trips; production always uses the default.
    """
    if not comparator(left, right).comparable:
        return True
    return (
        left.decision_grade
        and right.decision_grade
        and left.basis == right.basis
        and left.inferred_class_kbps is not None
        and right.inferred_class_kbps is not None
    )


def _decoy_comparator_grants_everything(
    left: SpectralInterpretation,
    right: SpectralInterpretation,
) -> SpectralComparability:
    """A comparability rule that (wrongly) compares any two sides —
    including a re-derived cliff class against a legacy stored bucket.
    Used only to prove the checker can detect that."""
    del left, right
    return SpectralComparability(True, "comparable_same_derivation")


class TestMixedDerivationCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the comparability checker must trip when a
    comparison is granted across derivations or without a class."""

    CLIFF_SIDE = interpret_spectral_cliff("mp3", spectral_grade=_T, cliff_hz=16500)
    STORED_SIDE = interpret_spectral_cliff("mp3", spectral_grade=_T, stored_bitrate_kbps=128)

    def test_checker_passes_for_the_real_comparability_rule(self):
        self.assertTrue(
            mixed_derivation_is_never_comparable(
                self.CLIFF_SIDE, self.STORED_SIDE,
            )
        )

    def test_checker_trips_on_a_rule_that_compares_across_derivations(self):
        """The Fall 2007 upgrade-loop mechanism as a mutant: a rule that
        weighs the re-derived 160 against the stored legacy 128."""
        self.assertFalse(
            mixed_derivation_is_never_comparable(
                self.CLIFF_SIDE, self.STORED_SIDE,
                comparator=_decoy_comparator_grants_everything,
            )
        )

    def test_checker_trips_on_a_rule_that_compares_an_aac_floor(self):
        self.assertFalse(
            mixed_derivation_is_never_comparable(
                self.CLIFF_SIDE,
                interpret_spectral_cliff("aac", spectral_grade=_T, cliff_hz=16500),
                comparator=_decoy_comparator_grants_everything,
            )
        )

    def test_checker_trips_on_a_classless_comparable_pair(self):
        """No decoy needed: the real rule keys on ``decision_grade``, so a
        decision-grade side with a stripped class already slips through
        it — and the checker catches that."""
        classless = replace(self.CLIFF_SIDE, inferred_class_kbps=None)
        self.assertTrue(
            spectral_classes_comparable(classless, self.CLIFF_SIDE).comparable
        )
        self.assertFalse(
            mixed_derivation_is_never_comparable(classless, self.CLIFF_SIDE)
        )


class TestMixedDerivationNeverComparable(unittest.TestCase):
    """Pin + generated property: a class is comparable only against a class
    derived the same way."""

    def test_pin_fall_2007_rederived_vs_its_own_legacy_bucket(self):
        rederived = interpret_spectral_cliff("mp3", spectral_grade=_T, cliff_hz=16500)
        legacy = interpret_spectral_cliff("mp3", spectral_grade=_T, stored_bitrate_kbps=128)
        self.assertEqual(rederived.inferred_class_kbps, 160)
        self.assertEqual(legacy.inferred_class_kbps, 128)
        result = spectral_classes_comparable(rederived, legacy)
        self.assertFalse(result.comparable)
        self.assertEqual(result.reason, "mixed_derivation_basis")
        self.assertTrue(
            mixed_derivation_is_never_comparable(rederived, legacy)
        )

    def test_pin_same_basis_cross_codec_is_comparable(self):
        """MP3 vs Vorbis in inferred-class space: 98% ordering accuracy on
        four arms pooled."""
        mp3 = interpret_spectral_cliff("mp3", spectral_grade=_T, cliff_hz=16500)
        vorbis = interpret_spectral_cliff("vorbis", spectral_grade=_T, cliff_hz=17000)
        self.assertTrue(spectral_classes_comparable(mp3, vorbis).comparable)
        self.assertTrue(mixed_derivation_is_never_comparable(mp3, vorbis))

    @given(left=_interpretations(), right=_interpretations())
    @example(
        left=interpret_spectral_cliff("mp3", spectral_grade=_T, cliff_hz=16500),
        right=interpret_spectral_cliff("mp3", spectral_grade=_T, stored_bitrate_kbps=128),
    )
    @example(
        left=interpret_spectral_cliff("aac", spectral_grade=_T, cliff_hz=16500),
        right=interpret_spectral_cliff("aac", spectral_grade=_T, cliff_hz=18500),
    )
    def test_across_generated_worlds(self, left, right):
        self.assertTrue(mixed_derivation_is_never_comparable(left, right))


# ---------------------------------------------------------------------------
# Invariant 5: both ladders are monotonic in cliff Hz.
# ---------------------------------------------------------------------------

def ladder_is_monotonic_in_cliff_hz(
    codec_family: CodecFamily,
    lower_hz: int,
    higher_hz: int,
    *,
    ladder_fn: LadderFn = ladder_class_kbps,
) -> bool:
    """Invariant checker: a higher cliff never infers a lower class.

    A ladder that inverted anywhere would let a wider-bandwidth copy read
    as worse quality than a narrower one, which is the whole point of the
    ladder existing.

    ``ladder_fn`` is injectable ONLY so the known-bad self-test can plant
    an inverted table; production always uses the default.
    """
    if lower_hz > higher_hz:
        lower_hz, higher_hz = higher_hz, lower_hz
    low = ladder_fn(codec_family, lower_hz)
    high = ladder_fn(codec_family, higher_hz)
    if low is None or high is None:
        return True
    return low <= high


def _decoy_inverted_ladder(
    codec_family: CodecFamily, cliff_hz: int,
) -> "int | None":
    """A ladder whose buckets descend — the shape a mis-transcribed table
    would have. Used only to prove the monotonicity checker trips."""
    del codec_family
    for upper_hz, class_kbps in (
        (15000, 320), (16000, 256), (17250, 160), (18250, 128), (19250, 96),
    ):
        if cliff_hz < upper_hz:
            return class_kbps
    return 64


class TestLadderMonotonicityCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the monotonicity checker must trip on an
    inverted bucket table."""

    def test_checker_passes_for_the_real_ladders(self):
        self.assertTrue(ladder_is_monotonic_in_cliff_hz("mp3", 15000, 19500))
        self.assertTrue(ladder_is_monotonic_in_cliff_hz("vorbis", 15000, 19500))

    def test_checker_trips_on_a_planted_inverted_table(self):
        self.assertFalse(
            ladder_is_monotonic_in_cliff_hz(
                "mp3", 14000, 19000, ladder_fn=_decoy_inverted_ladder,
            )
        )

    def test_checker_normalises_argument_order(self):
        self.assertFalse(
            ladder_is_monotonic_in_cliff_hz(
                "mp3", 19000, 14000, ladder_fn=_decoy_inverted_ladder,
            )
        )


class TestLaddersAreMonotonic(unittest.TestCase):
    """Pin + generated property: neither measured ladder ever inverts."""

    def _sweep(self, codec_family: CodecFamily) -> list[int]:
        classes = [
            interpret_spectral_cliff(
                codec_family, spectral_grade=_T, cliff_hz=hz,
            ).inferred_class_kbps
            for hz in range(12000, 22001, 250)
        ]
        self.assertNotIn(None, classes, "a ladder codec must class every cliff")
        return [value for value in classes if value is not None]

    def test_pin_mp3_full_sweep_is_non_decreasing(self):
        classes = self._sweep("mp3")
        self.assertEqual(classes, sorted(classes))
        self.assertEqual(classes[0], 96)
        self.assertEqual(classes[-1], 320)

    def test_pin_vorbis_full_sweep_is_non_decreasing(self):
        classes = self._sweep("vorbis")
        self.assertEqual(classes, sorted(classes))
        self.assertEqual(classes[0], 64)
        self.assertEqual(classes[-1], 160)

    @given(
        codec_family=st.sampled_from(("mp3", "vorbis")),
        lower_hz=st.integers(min_value=0, max_value=24000),
        higher_hz=st.integers(min_value=0, max_value=24000),
    )
    @example(codec_family="mp3", lower_hz=14999, higher_hz=15000)
    @example(codec_family="mp3", lower_hz=19249, higher_hz=19250)
    @example(codec_family="vorbis", lower_hz=15249, higher_hz=15250)
    @example(codec_family="vorbis", lower_hz=18999, higher_hz=19000)
    def test_across_generated_worlds(self, codec_family, lower_hz, higher_hz):
        self.assertTrue(
            ladder_is_monotonic_in_cliff_hz(codec_family, lower_hz, higher_hz)
        )


# ---------------------------------------------------------------------------
# Invariant 6: the legacy stored bucket never crosses onto a non-ladder
# codec. This is the download-37946 defect stated as a law.
# ---------------------------------------------------------------------------

def stored_bucket_never_becomes_an_off_ladder_class(
    codec_family: "CodecFamily | None",
    stored_bitrate_kbps: "int | None",
    sbr_present: "bool | None",
    *,
    spectral_grade: "str | None" = _T,
    interpreter: Interpreter = interpret_spectral_cliff,
) -> bool:
    """Invariant checker: with NO ``cliff_hz`` measured, a codec family
    without an invertible ladder must infer no class at all, whatever
    legacy ``spectral_bitrate_kbps`` the row carries.

    The stored bucket is the old codec-blind LAME table's output. Reading
    it as an AAC / Opus / lossless / unknown class is exactly the
    download-37946 defect. ``spectral_grade`` defaults to the AUTHORIZING
    grade so the law is tested at its hardest: even when production's
    verdict does authorize a spectral finding, an off-ladder codec still
    gets no class.

    ``interpreter`` is injectable ONLY so the known-bad self-test can plant
    the pre-fix behaviour; production always uses the default.
    """
    if codec_family in ("mp3", "vorbis"):
        return True
    result = interpreter(
        codec_family,
        spectral_grade=spectral_grade,
        cliff_hz=None,
        stored_bitrate_kbps=stored_bitrate_kbps,
        sbr_present=sbr_present,
    )
    return (
        result.inferred_class_kbps is None
        and result.decision_grade is False
        and result.invertible_ladder is False
    )


def _decoy_interpreter_reads_the_bucket_for_every_codec(
    codec_family: "CodecFamily | None",
    *,
    spectral_grade: "str | None" = None,
    cliff_hz: "int | None" = None,
    stored_bitrate_kbps: "int | None" = None,
    sbr_present: "bool | None" = None,
) -> SpectralInterpretation:
    """The pre-fix behaviour: every codec keeps the legacy LAME bucket as a
    decision-grade class. Used only to prove the checker trips."""
    result = interpret_spectral_cliff(
        codec_family, spectral_grade=spectral_grade, cliff_hz=cliff_hz,
        stored_bitrate_kbps=stored_bitrate_kbps, sbr_present=sbr_present,
    )
    if stored_bitrate_kbps is not None and stored_bitrate_kbps > 0:
        return replace(
            result,
            inferred_class_kbps=stored_bitrate_kbps,
            decision_grade=True,
            invertible_ladder=True,
            basis="stored_bucket",
        )
    return result


class TestStoredBucketOffLadderCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip when a non-ladder codec
    is allowed to keep the legacy bucket as a class."""

    def test_checker_passes_for_the_real_interpreter(self):
        for family in ("aac", "opus", "lossless", "other", None):
            with self.subTest(family=family):
                self.assertTrue(
                    stored_bucket_never_becomes_an_off_ladder_class(
                        family, 128, None,
                    )
                )

    def test_checker_trips_on_a_codec_blind_interpreter(self):
        for family in ("aac", "opus", "lossless", "other", None):
            with self.subTest(family=family):
                self.assertFalse(
                    stored_bucket_never_becomes_an_off_ladder_class(
                        family, 128, None,
                        interpreter=(
                            _decoy_interpreter_reads_the_bucket_for_every_codec
                        ),
                    )
                )

    def test_checker_is_vacuous_for_ladder_codecs(self):
        self.assertTrue(
            stored_bucket_never_becomes_an_off_ladder_class(
                "mp3", 128, None,
                interpreter=_decoy_interpreter_reads_the_bucket_for_every_codec,
            )
        )


class TestStoredBucketNeverCrossesOffLadder(unittest.TestCase):
    """Pin + generated property: the legacy LAME bucket is inert for every
    codec that has no ladder."""

    def test_pin_wavves_evidence_33591(self):
        self.assertTrue(
            stored_bucket_never_becomes_an_off_ladder_class("aac", 128, None)
        )

    def test_pin_lossless_bucket_is_authenticity_only(self):
        result = interpret_spectral_cliff(
            "lossless", spectral_grade=_T, stored_bitrate_kbps=128,
        )
        self.assertIsNone(result.inferred_class_kbps)
        self.assertTrue(result.supports_transcode_accusation)
        self.assertTrue(
            stored_bucket_never_becomes_an_off_ladder_class(
                "lossless", 128, None,
            )
        )

    @given(
        codec_family=st.one_of(st.none(), st.sampled_from(_CODEC_FAMILIES)),
        stored=_STORED,
        sbr=_SBR,
        grade=_GRADES,
    )
    @example(codec_family="aac", stored=128, sbr=None, grade=_T)
    @example(codec_family="opus", stored=320, sbr=None, grade=_T)
    @example(codec_family=None, stored=96, sbr=None, grade="genuine")
    def test_across_generated_worlds(self, codec_family, stored, sbr, grade):
        self.assertTrue(
            stored_bucket_never_becomes_an_off_ladder_class(
                codec_family, stored, sbr, spectral_grade=grade,
            )
        )


# ---------------------------------------------------------------------------
# Invariant 7: an SBR flag never disarms the lossless fake detector.
# ---------------------------------------------------------------------------

def sbr_never_disarms_the_lossless_detector(
    spectral_grade: "str | None",
    cliff_hz: "int | None",
    stored_bitrate_kbps: "int | None",
    *,
    interpreter: Interpreter = interpret_spectral_cliff,
) -> bool:
    """Invariant checker, two clauses.

    First: for a lossless container ``sbr_present`` is inert — all three
    tri-state values must produce the identical interpretation. Demoting a
    lossless row to audit-only on an SBR flag would clear
    ``supports_transcode_accusation`` and silently disable the fake-FLAC
    detector the whole verified-lossless proof rests on. For AAC the
    unrecoverable error is wrongly ASSERTING quality; for lossless it is
    failing to detect a FAKE, so the two families resolve the flag in
    opposite directions.

    Second: the detector tracks production's spectral GRADE exactly,
    never a reconstruction from cliff presence. ``spectral_grade`` is the
    union of BOTH detector legs, and 890 live lossless rows are flagged by
    the HF-deficit leg alone with no ``cliff_hz`` and no stored bucket
    (evidence 33735). Reading cliff presence would disarm every one.

    ``interpreter`` is injectable ONLY so the known-bad self-tests can
    plant the pre-fix behaviours; production always uses the default.
    """
    results = [
        interpreter(
            "lossless",
            spectral_grade=spectral_grade,
            cliff_hz=cliff_hz,
            stored_bitrate_kbps=stored_bitrate_kbps,
            sbr_present=sbr,
        )
        for sbr in (None, False, True)
    ]
    if any(result != results[0] for result in results):
        return False
    authorized = spectral_grade in ("suspect", "likely_transcode")
    return results[0].supports_transcode_accusation is authorized


def _decoy_interpreter_sbr_before_the_lossless_branch(
    codec_family: "CodecFamily | None",
    *,
    spectral_grade: "str | None" = None,
    cliff_hz: "int | None" = None,
    stored_bitrate_kbps: "int | None" = None,
    sbr_present: "bool | None" = None,
) -> SpectralInterpretation:
    """The pre-fix ordering: ``sbr_present`` demotes EVERY family to
    audit-only, lossless included. Used only to prove the checker trips."""
    if sbr_present:
        return replace(
            interpret_spectral_cliff(
                codec_family, spectral_grade=spectral_grade,
            ),
            semantics="audit_only",
            inferred_class_kbps=None,
            decision_grade=False,
            invertible_ladder=False,
            floor_only=False,
            supports_transcode_accusation=False,
            basis="none",
            reason="sbr_audit_only",
        )
    return interpret_spectral_cliff(
        codec_family, spectral_grade=spectral_grade, cliff_hz=cliff_hz,
        stored_bitrate_kbps=stored_bitrate_kbps, sbr_present=sbr_present,
    )


def _decoy_interpreter_lossless_detector_reads_cliff_presence(
    codec_family: "CodecFamily | None",
    *,
    spectral_grade: "str | None" = None,
    cliff_hz: "int | None" = None,
    stored_bitrate_kbps: "int | None" = None,
    sbr_present: "bool | None" = None,
) -> SpectralInterpretation:
    """The other pre-fix behaviour: the lossless detector reconstructs a
    verdict from cliff presence instead of reading the grade, so a
    deficit-only fake FLAC stops accusing. Used only to prove the checker
    trips."""
    del sbr_present
    cliff_seen = cliff_hz is not None or (
        stored_bitrate_kbps is not None and stored_bitrate_kbps > 0
    )
    return replace(
        interpret_spectral_cliff(
            codec_family, spectral_grade=spectral_grade, cliff_hz=cliff_hz,
            stored_bitrate_kbps=stored_bitrate_kbps,
        ),
        supports_transcode_accusation=cliff_seen,
    )


class TestSbrLosslessCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip on BOTH pre-fix
    behaviours — the SBR ordering and the cliff-presence reconstruction."""

    def test_checker_passes_for_the_real_interpreter(self):
        self.assertTrue(
            sbr_never_disarms_the_lossless_detector(_T, 16500, None)
        )
        self.assertTrue(
            sbr_never_disarms_the_lossless_detector(_T, None, None)
        )
        self.assertTrue(
            sbr_never_disarms_the_lossless_detector("genuine", None, 198)
        )

    def test_checker_trips_when_sbr_precedes_the_lossless_branch(self):
        self.assertFalse(
            sbr_never_disarms_the_lossless_detector(
                _T, 16500, None,
                interpreter=_decoy_interpreter_sbr_before_the_lossless_branch,
            )
        )

    def test_checker_trips_on_a_deficit_only_fake_flac_that_stops_accusing(self):
        """Evidence 33735's exact shape: ``likely_transcode`` with no
        cliff and no bucket. The cliff-presence reconstruction disarms it;
        the grade does not."""
        self.assertFalse(
            sbr_never_disarms_the_lossless_detector(
                _T, None, None,
                interpreter=(
                    _decoy_interpreter_lossless_detector_reads_cliff_presence
                ),
            )
        )

    def test_checker_trips_on_a_genuine_row_that_starts_accusing(self):
        """Evidence 3689's exact shape: ``genuine`` with a stored bucket of
        198. The reconstruction accuses off a non-verdict value."""
        self.assertFalse(
            sbr_never_disarms_the_lossless_detector(
                "genuine", None, 198,
                interpreter=(
                    _decoy_interpreter_lossless_detector_reads_cliff_presence
                ),
            )
        )


class TestSbrNeverDisarmsLossless(unittest.TestCase):
    """Pin + generated property: an object-type probe can never fail OPEN
    with respect to fake-lossless detection, and the detector reads
    production's verdict rather than reconstructing one."""

    def test_pin_sbr_flag_on_a_cliffed_flac_keeps_the_detector_armed(self):
        armed = interpret_spectral_cliff(
            "lossless", spectral_grade=_T, cliff_hz=16500, sbr_present=True,
        )
        self.assertEqual(armed.semantics, "lossless_authenticity")
        self.assertTrue(armed.supports_transcode_accusation)
        self.assertTrue(
            sbr_never_disarms_the_lossless_detector(_T, 16500, None)
        )

    def test_pin_evidence_33735_deficit_only_fake_flac(self):
        self.assertTrue(
            sbr_never_disarms_the_lossless_detector(_T, None, None)
        )

    def test_pin_sbr_still_demotes_aac(self):
        """The other half of the asymmetry: AAC must still go audit-only."""
        demoted = interpret_spectral_cliff(
            "aac", spectral_grade=_T, cliff_hz=16500, sbr_present=True,
        )
        self.assertEqual(demoted.semantics, "audit_only")
        self.assertEqual(demoted.reason, "sbr_audit_only")

    @given(grade=_GRADES, cliff_hz=_CLIFFS, stored=_STORED)
    @example(grade=_T, cliff_hz=16500, stored=None)
    @example(grade=_T, cliff_hz=None, stored=None)
    @example(grade="genuine", cliff_hz=None, stored=198)
    @example(grade="suspect", cliff_hz=None, stored=None)
    def test_across_generated_worlds(self, grade, cliff_hz, stored):
        self.assertTrue(
            sbr_never_disarms_the_lossless_detector(grade, cliff_hz, stored)
        )


# ---------------------------------------------------------------------------
# Invariant 8: cross-codec comparison is licensed only in cliff_hz basis.
# ---------------------------------------------------------------------------

def cross_codec_comparison_requires_cliff_basis(
    left: SpectralInterpretation,
    right: SpectralInterpretation,
    *,
    comparator: Comparator = spectral_classes_comparable,
) -> bool:
    """Invariant checker: a granted comparison between two DIFFERENT codec
    families must be in ``cliff_hz`` basis.

    The measured 98% MP3<->Vorbis ordering accuracy was obtained on classes
    derived through each codec's own ladder. A legacy stored bucket is the
    LAME table's output whatever the codec — faithful for MP3, a known
    one-directional over-estimate for Vorbis — so a cross-codec legacy pair
    compares table bias, not content.

    ``comparator`` is injectable ONLY so the known-bad self-test can plant
    the weaker same-basis-only rule; production always uses the default.
    """
    if not comparator(left, right).comparable:
        return True
    return left.codec_family == right.codec_family or left.basis == "cliff_hz"


def _decoy_comparator_same_basis_only(
    left: SpectralInterpretation,
    right: SpectralInterpretation,
) -> SpectralComparability:
    """The weaker pre-change rule: decision-grade on both sides plus a
    shared basis, with no codec-family constraint. Used only to prove the
    checker trips."""
    if not left.decision_grade:
        return SpectralComparability(False, "left_not_decision_grade")
    if not right.decision_grade:
        return SpectralComparability(False, "right_not_decision_grade")
    if left.basis != right.basis:
        return SpectralComparability(False, "mixed_derivation_basis")
    return SpectralComparability(True, "comparable_same_derivation")


class TestCrossCodecBasisCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip on the weaker rule that
    compares an MP3 legacy bucket against a Vorbis one."""

    MP3_LEGACY = interpret_spectral_cliff("mp3", spectral_grade=_T, stored_bitrate_kbps=160)
    VORBIS_LEGACY = interpret_spectral_cliff("vorbis", spectral_grade=_T, stored_bitrate_kbps=192)

    def test_checker_passes_for_the_real_comparability_rule(self):
        self.assertTrue(
            cross_codec_comparison_requires_cliff_basis(
                self.MP3_LEGACY, self.VORBIS_LEGACY,
            )
        )

    def test_checker_trips_on_the_same_basis_only_rule(self):
        self.assertTrue(
            _decoy_comparator_same_basis_only(
                self.MP3_LEGACY, self.VORBIS_LEGACY,
            ).comparable
        )
        self.assertFalse(
            cross_codec_comparison_requires_cliff_basis(
                self.MP3_LEGACY, self.VORBIS_LEGACY,
                comparator=_decoy_comparator_same_basis_only,
            )
        )

    def test_checker_is_vacuous_for_same_codec_legacy_pairs(self):
        other_mp3 = interpret_spectral_cliff("mp3", spectral_grade=_T, stored_bitrate_kbps=128)
        self.assertTrue(
            cross_codec_comparison_requires_cliff_basis(
                self.MP3_LEGACY, other_mp3,
                comparator=_decoy_comparator_same_basis_only,
            )
        )


class TestCrossCodecRequiresCliffBasis(unittest.TestCase):
    """Pin + generated property: two codecs are only ever weighed against
    each other through each codec's own ladder."""

    def test_pin_mp3_legacy_vs_vorbis_legacy_is_refused(self):
        mp3 = interpret_spectral_cliff("mp3", spectral_grade=_T, stored_bitrate_kbps=160)
        vorbis = interpret_spectral_cliff("vorbis", spectral_grade=_T, stored_bitrate_kbps=192)
        refusal = spectral_classes_comparable(mp3, vorbis)
        self.assertFalse(refusal.comparable)
        self.assertEqual(refusal.reason, "cross_codec_legacy_bucket")
        self.assertTrue(
            cross_codec_comparison_requires_cliff_basis(mp3, vorbis)
        )

    def test_pin_mp3_cliff_vs_vorbis_cliff_still_compares(self):
        mp3 = interpret_spectral_cliff("mp3", spectral_grade=_T, cliff_hz=16500)
        vorbis = interpret_spectral_cliff("vorbis", spectral_grade=_T, cliff_hz=17000)
        self.assertTrue(spectral_classes_comparable(mp3, vorbis).comparable)
        self.assertTrue(
            cross_codec_comparison_requires_cliff_basis(mp3, vorbis)
        )

    @given(left=_interpretations(), right=_interpretations())
    @example(
        left=interpret_spectral_cliff("mp3", spectral_grade=_T, stored_bitrate_kbps=160),
        right=interpret_spectral_cliff("vorbis", spectral_grade=_T, stored_bitrate_kbps=192),
    )
    @example(
        left=interpret_spectral_cliff("vorbis", spectral_grade=_T, stored_bitrate_kbps=64),
        right=interpret_spectral_cliff("vorbis", spectral_grade=_T, stored_bitrate_kbps=128),
    )
    def test_across_generated_worlds(self, left, right):
        self.assertTrue(
            cross_codec_comparison_requires_cliff_basis(left, right)
        )


# ---------------------------------------------------------------------------
# Invariant 9: no spectral finding without an authorizing grade.
# ---------------------------------------------------------------------------

def no_finding_without_an_authorizing_grade(
    spectral_grade: "str | None",
    interpretation: SpectralInterpretation,
) -> bool:
    """Invariant checker: neither a decision-grade class nor a transcode
    accusation may exist unless ``spectral_grade`` is in
    ``SPECTRAL_TRANSCODE_GRADES``.

    ``spectral_grade`` is production's verdict over BOTH detector legs,
    and the importer already gates on exactly this set
    (``compute_effective_override_bitrate``). This module reads that
    verdict; it never manufactures one.

    Deliberately one-directional, and deliberately silent about
    ``inferred_class_kbps`` on its own: the AAC content floor is allowed
    to exist without an authorizing grade precisely because it is not a
    decision input (``decision_grade`` and ``supports_transcode_accusation``
    are hard-False on every AAC path). Invariant 2 owns that surface.
    """
    authorized = spectral_grade in ("suspect", "likely_transcode")
    if authorized:
        return True
    return (
        not interpretation.decision_grade
        and not interpretation.supports_transcode_accusation
    )


class TestGradeGateCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip on a finding made under
    a grade that does not authorize one."""

    def test_checker_passes_for_the_real_interpreter(self):
        for grade in ("genuine", "marginal", "error", None):
            with self.subTest(grade=grade):
                self.assertTrue(
                    no_finding_without_an_authorizing_grade(
                        grade,
                        interpret_spectral_cliff(
                            "mp3", spectral_grade=grade, cliff_hz=16500,
                            stored_bitrate_kbps=202,
                        ),
                    )
                )

    def test_checker_trips_on_a_class_under_a_genuine_grade(self):
        """Evidence 5144's shape: a container bitrate in
        ``spectral_bitrate_kbps`` on a ``genuine`` MP3, promoted to a
        comparable class."""
        planted = replace(
            interpret_spectral_cliff("mp3", spectral_grade="genuine"),
            inferred_class_kbps=202,
            decision_grade=True,
            basis="stored_bucket",
        )
        self.assertFalse(
            no_finding_without_an_authorizing_grade("genuine", planted)
        )

    def test_checker_trips_on_an_accusation_under_a_genuine_grade(self):
        """Evidence 3689's shape: a ``genuine`` lossless row that accuses
        off a stored bucket."""
        planted = replace(
            interpret_spectral_cliff(
                "lossless", spectral_grade="genuine", stored_bitrate_kbps=198,
            ),
            supports_transcode_accusation=True,
        )
        self.assertFalse(
            no_finding_without_an_authorizing_grade("genuine", planted)
        )

    def test_checker_is_vacuous_under_an_authorizing_grade(self):
        self.assertTrue(
            no_finding_without_an_authorizing_grade(
                _T,
                interpret_spectral_cliff(
                    "mp3", spectral_grade=_T, cliff_hz=16500,
                ),
            )
        )

    def test_checker_permits_the_ungated_aac_floor(self):
        """The one deliberate exception, asserted rather than assumed: an
        AAC floor may exist under a ``genuine`` grade because it is not a
        decision input."""
        floored = interpret_spectral_cliff(
            "aac", spectral_grade="genuine", cliff_hz=16500,
        )
        self.assertEqual(floored.inferred_class_kbps, AAC_FLOOR_LOW_CLASS_KBPS)
        self.assertTrue(
            no_finding_without_an_authorizing_grade("genuine", floored)
        )


class TestNoFindingWithoutAnAuthorizingGrade(unittest.TestCase):
    """Pin + generated property: this module never asserts a spectral
    finding production's own verdict did not authorize."""

    def test_pin_evidence_5144_container_bitrate_on_a_genuine_mp3(self):
        facts = SpectralEvidenceFacts(
            spectral_grade="genuine", format="MP3", storage_format="MP3",
            filetype_band="mp3", spectral_subject="source",
            spectral_bitrate_kbps=202,
        )
        result = interpret_spectral_evidence(facts)
        self.assertIsNone(result.inferred_class_kbps)
        self.assertTrue(
            no_finding_without_an_authorizing_grade("genuine", result)
        )

    def test_pin_evidence_3689_genuine_r19_lossless_row(self):
        facts = SpectralEvidenceFacts(
            spectral_grade="genuine", format="opus", storage_format="opus",
            filetype_band="opus", was_converted_from="flac",
            spectral_subject="source", spectral_bitrate_kbps=198,
        )
        result = interpret_spectral_evidence(facts)
        self.assertFalse(result.supports_transcode_accusation)
        self.assertTrue(
            no_finding_without_an_authorizing_grade("genuine", result)
        )

    @given(facts=_spectral_evidence_facts())
    @example(
        facts=SpectralEvidenceFacts(
            spectral_grade="genuine", format="MP3", cliff_hz=16500,
        ),
    )
    @example(
        facts=SpectralEvidenceFacts(
            spectral_grade="marginal", format="FLAC", spectral_bitrate_kbps=128,
        ),
    )
    def test_across_generated_worlds(self, facts):
        self.assertTrue(
            no_finding_without_an_authorizing_grade(
                facts.spectral_grade, interpret_spectral_evidence(facts),
            )
        )


# ---------------------------------------------------------------------------
# Invariant 10: a stored-bucket class is always a LAME_LOWPASS member.
# ---------------------------------------------------------------------------

def stored_bucket_class_is_always_a_lame_bucket(
    interpretation: SpectralInterpretation,
) -> bool:
    """Invariant checker: whenever a class was derived from the legacy
    stored column, that class is a ``LAME_LOWPASS`` member.

    The column does not always hold a bucket — 2,503 of 30,251 live rows
    carry a container bitrate there, up to 738. ``estimate_bitrate_from_cliff``
    returns only ``LAME_LOWPASS`` values, so a class outside that set was
    never a spectral estimate at all.

    The allowlist is read from the SAME table production reads, so this
    checker cannot drift from the shipped guard; what it patrols is that
    the guard is actually consulted on every stored-bucket path.

    Vacuously true for a ``cliff_hz`` or ``none`` basis — a cliff is a raw
    measurement, not a bucket.
    """
    if interpretation.basis != "stored_bucket":
        return True
    from lib.spectral_check import LAME_LOWPASS

    buckets = {class_kbps for _lowpass_hz, class_kbps in LAME_LOWPASS}
    return interpretation.inferred_class_kbps in buckets


def _decoy_interpreter_without_the_bucket_allowlist(
    codec_family: "CodecFamily | None",
    *,
    spectral_grade: "str | None" = None,
    cliff_hz: "int | None" = None,
    stored_bitrate_kbps: "int | None" = None,
    sbr_present: "bool | None" = None,
) -> SpectralInterpretation:
    """The pre-allowlist behaviour: any positive stored value becomes a
    ladder class. Used only to prove the checker trips."""
    result = interpret_spectral_cliff(
        codec_family, spectral_grade=spectral_grade, cliff_hz=cliff_hz,
        stored_bitrate_kbps=stored_bitrate_kbps, sbr_present=sbr_present,
    )
    if (
        codec_family in ("mp3", "vorbis")
        and cliff_hz is None
        and spectral_grade in ("suspect", "likely_transcode")
        and stored_bitrate_kbps is not None
        and stored_bitrate_kbps > 0
    ):
        return replace(
            result,
            inferred_class_kbps=stored_bitrate_kbps,
            decision_grade=True,
            basis="stored_bucket",
            reason="ladder_class_from_stored_bucket",
        )
    return result


class TestStoredBucketAllowlistCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip when a container bitrate
    is promoted into ladder class space."""

    def test_checker_passes_for_the_real_interpreter(self):
        for stored in (*_LAME_BUCKET_SAMPLES, *_LIVE_NON_BUCKET_SAMPLES):
            with self.subTest(stored=stored):
                self.assertTrue(
                    stored_bucket_class_is_always_a_lame_bucket(
                        interpret_spectral_cliff(
                            "mp3", spectral_grade=_T,
                            stored_bitrate_kbps=stored,
                        )
                    )
                )

    def test_checker_trips_without_the_allowlist(self):
        """Values from the real live non-bucket tail."""
        for stored in (126, 131, 226, 738):
            with self.subTest(stored=stored):
                self.assertFalse(
                    stored_bucket_class_is_always_a_lame_bucket(
                        _decoy_interpreter_without_the_bucket_allowlist(
                            "mp3", spectral_grade=_T,
                            stored_bitrate_kbps=stored,
                        )
                    )
                )

    def test_checker_is_vacuous_for_the_cliff_path(self):
        self.assertTrue(
            stored_bucket_class_is_always_a_lame_bucket(
                interpret_spectral_cliff(
                    "mp3", spectral_grade=_T, cliff_hz=16500,
                )
            )
        )


class TestStoredBucketIsAlwaysALameBucket(unittest.TestCase):
    """Pin + generated property: a container bitrate parked in the legacy
    spectral column never becomes a ladder class."""

    def test_pin_real_bucket_values_still_class(self):
        for bucket in _LAME_BUCKET_SAMPLES:
            with self.subTest(bucket=bucket):
                result = interpret_spectral_cliff(
                    "mp3", spectral_grade=_T, stored_bitrate_kbps=bucket,
                )
                self.assertEqual(result.inferred_class_kbps, bucket)
                self.assertTrue(
                    stored_bucket_class_is_always_a_lame_bucket(result)
                )

    def test_pin_live_non_bucket_tail_yields_no_class(self):
        for stored in _LIVE_NON_BUCKET_SAMPLES:
            with self.subTest(stored=stored):
                result = interpret_spectral_cliff(
                    "vorbis", spectral_grade="suspect",
                    stored_bitrate_kbps=stored,
                )
                self.assertIsNone(result.inferred_class_kbps)
                self.assertEqual(
                    result.reason, "ladder_stored_value_not_a_bucket"
                )

    @given(facts=_spectral_evidence_facts())
    @example(
        facts=SpectralEvidenceFacts(
            spectral_grade=_T, format="MP3", spectral_bitrate_kbps=226,
        ),
    )
    @example(
        facts=SpectralEvidenceFacts(
            spectral_grade=_T, format="MP3", spectral_bitrate_kbps=128,
        ),
    )
    @example(
        facts=SpectralEvidenceFacts(
            spectral_grade="suspect", format="vorbis",
            spectral_bitrate_kbps=738,
        ),
    )
    def test_across_generated_worlds(self, facts):
        self.assertTrue(
            stored_bucket_class_is_always_a_lame_bucket(
                interpret_spectral_evidence(facts)
            )
        )


if __name__ == "__main__":
    unittest.main()
