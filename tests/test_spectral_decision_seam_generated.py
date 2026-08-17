"""Generated tests — issue #829 Phase 5 PR2b: the codec-aware DECISION seam.

PR2a's properties (``tests/test_spectral_interpretation_generated.py``)
patrol what a spectral measurement MEANS in its own codec's terms. These
patrol what the decider is allowed to DO with that meaning, by driving the
real production entry points — ``full_pipeline_decision_from_evidence``
(the function the importer calls) and ``compare_quality`` — over generated
worlds.

The deterministic halves of these pairs are
``tests/test_quality_classification.py``'s
``TestWavvesAacCodecBlindSpectral`` (download 37946 / evidence 33591 and
33592) and ``TestFall2007AntiLoop`` (request 8902 / evidence 34219), plus
``tests/test_quality_decisions.py``'s
``TestCompareQualitySharedSpectralBucket``.

Five invariants, each with a module-level checker and a known-bad
self-test proving the checker trips on a planted violation:

1. When the candidate's spectral interpretation WITHHOLDS, the decided
   outcome does not depend on the spectral numbers at all. This is
   download 37946 as a permanent law: an AAC's LAME-bucketed "128" can
   move nothing.
2. An uncalibrated codec never produces a spectral rejection, a transcode
   bound, or a claim that production's gate would have measured it.
3. The comparison is spectrally clamped ONLY when the two classes are
   comparable, or when the one licensed role-neutral bound applies.
4. The Fall 2007 fixed point: a transcode-classed copy and a known-clean
   copy never mutually displace each other; each one-class verdict agrees
   with its effective values and configured same-family tolerance.
5. A lossless container's decision is invariant under every codec-capture
   field. Its cliff is the fake-lossless detector, driven by the GRADE;
   PR2b derives no kbps class for it and must not weaken that in either
   direction.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import os
import sys
import unittest
from collections.abc import Sequence
from dataclasses import dataclass

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.quality import (
    SPECTRAL_TRANSCODE_GRADES,
    AlbumQualityEvidence,
    AudioQualityMeasurement,
    CodecFamily,
    EvidenceSubject,
    QualityComparisonBasis,
    QualityRankConfig,
    SpectralInterpretation,
    _is_explicit_label,
    _selected_bitrate,
    compare_quality,
    decision_class_kbps,
    full_pipeline_decision_from_evidence,
    interpret_measurement,
    measurement_rank,
    spectral_classes_comparable,
)
from lib.quality.ranks import _codec_family_of
from tests.helpers import (
    build_parity_candidate_evidence,
    build_parity_current_evidence,
)

CFG = QualityRankConfig.defaults()

#: The only values ``estimate_bitrate_from_cliff`` can emit, plus the
#: container bitrates that really are parked in that column on live rows
#: (2,503 of 30,251) and ``None``. Not a plausibility filter — a union of
#: the two populations a producer can actually write.
_STORED_BUCKETS = (96, 112, 128, 160, 192, 224, 256, 320)
_LIVE_NON_BUCKETS = (121, 130, 198, 235, 247, 738)

#: Grades ``classify_album`` emits, plus the ``error`` grade and None.
_GRADES: tuple[str | None, ...] = (
    None, "genuine", "marginal", "suspect", "likely_transcode", "error",
)

#: The measured-format labels the evidence rows carry, paired with the
#: codec token ``build_parity_candidate_evidence`` stamps alongside them.
_LOSSY_CODECS: tuple[tuple[str, str], ...] = (
    ("MP3", "mp3"), ("AAC", "aac"), ("Opus", "opus"),
    ("Vorbis", "vorbis"), ("WMA", "wma"),
)

#: The families whose spectral evidence #829 proved is inadmissible as a
#: class: AAC's cliff is a one-sided content floor, Opus carries no signal
#: at all, and ``other``/unknown fail closed.
_UNCALIBRATED_LOSSY: tuple[tuple[str, str], ...] = (
    ("AAC", "aac"), ("Opus", "opus"), ("WMA", "wma"),
)

_CLIFF_HZ = st.one_of(st.none(), st.integers(min_value=11000, max_value=22000))
_STORED = st.one_of(
    st.none(),
    st.sampled_from(_STORED_BUCKETS),
    st.sampled_from(_LIVE_NON_BUCKETS),
)

#: The decided outcome. Everything the pipeline actually DOES; the branch
#: tag and the intermediate ranks are deliberately absent, because a pin on
#: a proxy field is not a pin on a consequence.
_OUTCOME_KEYS = (
    "stage2_import", "stage3_quality_gate", "final_status",
    "imported", "denylisted", "keep_searching", "verified_lossless",
)


def _outcome(decision: dict[str, object]) -> tuple[object, ...]:
    return tuple(decision[key] for key in _OUTCOME_KEYS)


def _with_spectral_numbers(
    evidence: AlbumQualityEvidence,
    *,
    stored: int | None,
    cliff_hz: int | None,
) -> AlbumQualityEvidence:
    """Re-stamp one evidence row's spectral NUMBERS, grade untouched."""
    return msgspec.structs.replace(
        evidence,
        measurement=msgspec.structs.replace(
            evidence.measurement,
            spectral_bitrate_kbps=stored,
            cliff_hz=cliff_hz,
        ),
    )


# ===========================================================================
# Invariant 1 — a withholding interpretation moves nothing
# ===========================================================================

def assert_outcome_independent_of_spectral_numbers(
    outcomes: Sequence[tuple[object, ...]],
    *,
    context: str,
) -> None:
    """Every world in ``outcomes`` decided identically.

    The candidate's spectral interpretation withholds in all of them, so
    the spectral numbers are not evidence — varying them must not move the
    pipeline. Download 37946 is exactly the violation: an AAC's LAME-table
    "128" changed a decided outcome.
    """
    distinct = {outcome for outcome in outcomes}
    if len(distinct) > 1:
        raise AssertionError(
            "a withheld spectral interpretation changed the decided "
            f"outcome ({context}): "
            + " | ".join(repr(dict(zip(_OUTCOME_KEYS, o, strict=True)))
                         for o in sorted(distinct, key=repr))
        )


class TestWithheldSpectralCheckerSelfTest(unittest.TestCase):
    """The checker trips on a planted violation."""

    def test_differing_outcomes_are_rejected(self):
        with self.assertRaises(AssertionError) as caught:
            assert_outcome_independent_of_spectral_numbers(
                [
                    ("import", None, "imported", True, False, False, False),
                    ("downgrade", None, "imported", False, True, True, False),
                ],
                context="planted",
            )
        self.assertIn("changed the decided outcome", str(caught.exception))

    def test_identical_outcomes_pass(self):
        outcome = ("import", None, "imported", True, False, False, False)
        assert_outcome_independent_of_spectral_numbers(
            [outcome, outcome, outcome], context="planted")


@dataclass(frozen=True)
class WithholdingWorld:
    """A native-lossy world whose CANDIDATE codec has no invertible ladder."""

    native_format: str
    native_codec: str
    min_bitrate: int
    is_cbr: bool
    spectral_grade: str | None
    current_min: int | None
    current_format: str
    current_is_cbr: bool
    current_grade: str | None
    current_stored: int | None
    current_cliff: int | None


@st.composite
def withholding_candidate_worlds(draw) -> WithholdingWorld:
    """Native-lossy worlds whose CANDIDATE codec has no invertible ladder.

    Every field is free — grade, bitrates, the HAVE's whole shape. The one
    constraint is the candidate's codec family, which is what makes the
    interpretation withhold.
    """
    fmt, codec = draw(st.sampled_from(_UNCALIBRATED_LOSSY))
    return WithholdingWorld(
        native_format=fmt,
        native_codec=codec,
        min_bitrate=draw(st.integers(min_value=32, max_value=1000)),
        is_cbr=draw(st.booleans()),
        spectral_grade=draw(st.sampled_from(_GRADES)),
        current_min=draw(st.one_of(
            st.none(), st.integers(min_value=32, max_value=1200))),
        current_format=draw(st.sampled_from(
            [label for label, _ in _LOSSY_CODECS] + ["FLAC"])),
        current_is_cbr=draw(st.booleans()),
        current_grade=draw(st.sampled_from(_GRADES)),
        current_stored=draw(_STORED),
        current_cliff=draw(_CLIFF_HZ),
    )


class TestWithheldSpectralMovesNothing(unittest.TestCase):
    """Invariant 1 — download 37946 as a permanent law.

    The AAC in the live defect was 256 kbps CBR whose natural rolloff read
    as ``likely_transcode`` / 128. Here the candidate's stored bucket and
    raw cliff are swept across everything a producer can write, on a codec
    whose ladder is not invertible, and the pipeline must decide the same
    thing every time.
    """

    @example(world=WithholdingWorld(
        # The CANDIDATE is download 37946 / evidence 33591 exactly: a
        # 256 kbps CBR AAC graded ``likely_transcode`` with a LAME-table
        # 128. The HAVE is a COUNTERFACTUAL — an MP3 whose own cliff says
        # 192 — chosen because it is the shape where the AAC's spurious
        # class was decisive. (The live HAVE was 320 with a 128 estimate,
        # which ``main`` already imports, so it cannot show the flip.)
        native_format="AAC", native_codec="aac",
        min_bitrate=256, is_cbr=True,
        spectral_grade="likely_transcode",
        current_min=192, current_format="MP3", current_is_cbr=True,
        current_grade="likely_transcode", current_stored=192,
        current_cliff=None,
    ))
    @given(world=withholding_candidate_worlds())
    def test_spectral_numbers_never_move_an_uncalibrated_codec(self, world):
        current = build_parity_current_evidence(
            min_bitrate=world.current_min,
            avg_bitrate=world.current_min,
            format=world.current_format,
            is_cbr=world.current_is_cbr,
            spectral_grade=world.current_grade,
            spectral_bitrate=(
                world.current_stored
                if world.current_grade is not None else None
            ),
            cliff_hz=(
                world.current_cliff
                if world.current_grade is not None else None
            ),
            codec_family=None,
        )
        outcomes: list[tuple[object, ...]] = []
        for stored in (None, *_STORED_BUCKETS):
            for cliff in (None, 14000, 16500, 19000):
                candidate = build_parity_candidate_evidence(
                    is_flac=False,
                    min_bitrate=world.min_bitrate,
                    avg_bitrate=world.min_bitrate,
                    is_cbr=world.is_cbr,
                    native_format=world.native_format,
                    native_codec=world.native_codec,
                    spectral_grade=world.spectral_grade,
                )
                if world.spectral_grade is not None:
                    candidate = _with_spectral_numbers(
                        candidate, stored=stored, cliff_hz=cliff)
                outcomes.append(_outcome(
                    full_pipeline_decision_from_evidence(candidate, current)))
        assert_outcome_independent_of_spectral_numbers(
            outcomes, context=repr(world))

    @example(world=WithholdingWorld(
        # An AAC HAVE with a REAL cliff at 15.5 kHz. Its content floor (96)
        # is a LOWER bound, the opposite direction to every clamp in the
        # decision path — admitting it would floor a 256 kbps installed
        # album to 96 and hand the release to any passing candidate.
        native_format="AAC", native_codec="aac",
        min_bitrate=192, is_cbr=True,
        spectral_grade=None,
        current_min=256, current_format="AAC", current_is_cbr=True,
        current_grade="likely_transcode", current_stored=128,
        current_cliff=15500,
    ))
    @given(world=withholding_candidate_worlds())
    def test_spectral_numbers_never_move_an_uncalibrated_have(self, world):
        """The same law on the HAVE side, where the one-sided floor lives.

        ``override_bitrate_from_current_evidence`` represents an installed
        album by its own spectral class so a fake-high copy cannot block a
        genuine upgrade. That representation is only honest when the class
        means something in the album's own codec — otherwise it is the
        download-37946 defect pointed at the library instead of the
        candidate.
        """
        candidate = build_parity_candidate_evidence(
            is_flac=False,
            min_bitrate=world.min_bitrate,
            avg_bitrate=world.min_bitrate,
            is_cbr=world.is_cbr,
            native_format="MP3",
            native_codec="mp3",
            spectral_grade=None,
        )
        have_format, _have_codec = ("AAC", "aac")
        outcomes: list[tuple[object, ...]] = []
        for stored in (None, *_STORED_BUCKETS):
            for cliff in (None, 14000, 15500, 19000):
                current = build_parity_current_evidence(
                    min_bitrate=world.current_min or 256,
                    avg_bitrate=world.current_min or 256,
                    format=have_format,
                    is_cbr=world.current_is_cbr,
                    spectral_grade=world.current_grade,
                    spectral_bitrate=(
                        stored if world.current_grade is not None else None),
                    cliff_hz=(
                        cliff if world.current_grade is not None else None),
                )
                outcomes.append(_outcome(
                    full_pipeline_decision_from_evidence(candidate, current)))
        assert_outcome_independent_of_spectral_numbers(
            outcomes, context=repr(world))


# ===========================================================================
# Invariant 2 — an uncalibrated codec never accuses
# ===========================================================================

def assert_no_uncalibrated_accusation(
    decision: dict[str, object],
    *,
    context: str,
) -> None:
    """No spectral rejection and no transcode bound for an uncalibrated codec.

    "Fail closed" in this project means WITHHOLD the spectral opinion. It
    never means reject the album — so an AAC, an Opus or an unmapped codec
    must never lose on spectral grounds, however its LAME-bucketed number
    reads.
    """
    if decision.get("stage1_spectral") == "reject":
        raise AssertionError(
            f"uncalibrated codec rejected at Stage 1 ({context})")
    if decision.get("stage0_spectral_gate") == "would_run":
        raise AssertionError(
            "the gate mirror claims production would measure an "
            f"uncalibrated codec ({context})")
    basis = decision.get("comparison_basis")
    if isinstance(basis, dict) and basis.get("branch") == "spectral_candidate_bound":
        raise AssertionError(
            f"uncalibrated codec produced a transcode bound ({context})")
    if isinstance(basis, dict) and basis.get("spectral_clamped"):
        raise AssertionError(
            f"uncalibrated codec was spectrally clamped ({context})")


class TestUncalibratedAccusationCheckerSelfTest(unittest.TestCase):
    def test_stage1_reject_trips(self):
        with self.assertRaises(AssertionError):
            assert_no_uncalibrated_accusation(
                {"stage1_spectral": "reject"}, context="planted")

    def test_would_run_gate_trips(self):
        with self.assertRaises(AssertionError):
            assert_no_uncalibrated_accusation(
                {"stage0_spectral_gate": "would_run"}, context="planted")

    def test_candidate_bound_branch_trips(self):
        with self.assertRaises(AssertionError):
            assert_no_uncalibrated_accusation(
                {"comparison_basis": {"branch": "spectral_candidate_bound"}},
                context="planted")

    def test_spectral_clamp_trips(self):
        with self.assertRaises(AssertionError):
            assert_no_uncalibrated_accusation(
                {"comparison_basis": {"branch": "rank",
                                      "spectral_clamped": True}},
                context="planted")

    def test_clean_decision_passes(self):
        assert_no_uncalibrated_accusation(
            {"stage1_spectral": None,
             "stage0_spectral_gate": "skipped_uncalibrated_codec",
             "comparison_basis": {"branch": "rank", "spectral_clamped": False}},
            context="planted")


class TestUncalibratedCodecNeverAccuses(unittest.TestCase):
    """Invariant 2 — withholding is never a rejection."""

    @given(world=withholding_candidate_worlds(), stored=_STORED,
           cliff=_CLIFF_HZ)
    def test_uncalibrated_candidate_never_loses_on_spectral(
        self, world, stored, cliff,
    ):
        candidate = build_parity_candidate_evidence(
            is_flac=False,
            min_bitrate=world.min_bitrate,
            avg_bitrate=world.min_bitrate,
            is_cbr=world.is_cbr,
            native_format=world.native_format,
            native_codec=world.native_codec,
            spectral_grade=world.spectral_grade,
        )
        if world.spectral_grade is not None:
            candidate = _with_spectral_numbers(
                candidate, stored=stored, cliff_hz=cliff)
        current = build_parity_current_evidence(
            min_bitrate=world.current_min,
            avg_bitrate=world.current_min,
            format=world.current_format,
            is_cbr=world.current_is_cbr,
            spectral_grade=world.current_grade,
            spectral_bitrate=(
                world.current_stored
                if world.current_grade is not None else None
            ),
        )
        assert_no_uncalibrated_accusation(
            full_pipeline_decision_from_evidence(candidate, current),
            context=f"{world!r} stored={stored!r} cliff={cliff!r}",
        )


# ===========================================================================
# Invariant 3 — the clamp requires comparability
# ===========================================================================

def _one_sided_bound_licence_failures(
    classed: AudioQualityMeasurement,
    raw: AudioQualityMeasurement,
    classed_spectral: SpectralInterpretation,
    raw_spectral: SpectralInterpretation,
) -> list[str]:
    """Every role-neutral one-sided spectral-bound gate, restated here.

    Every gate, deliberately — an incomplete restatement is how a checker
    silently stops patrolling a gate (PR2b review S3: encoding only two of
    them let a mutant that dropped the transcode-HAVE refusal survive a
    6,000-example burst). Each returns its own name so a failure says which
    gate the production code stopped applying.

    The property drives ``compare_quality`` without a target contract, so
    ``classed.format`` is the production comparison hint.
    """
    failures: list[str] = []
    if not classed_spectral.decision_grade:
        failures.append("classed encode is not decision-grade")
    if not classed_spectral.supports_transcode_accusation:
        failures.append("classed encode cannot support a transcode accusation")
    if _is_explicit_label(classed.format) or _is_explicit_label(raw.format):
        failures.append("a side carries an explicit contract label")
    if _codec_family_of(classed.format) != _codec_family_of(raw.format):
        failures.append("sides are not in one codec family")
    if raw.spectral_grade is None:
        failures.append("raw encode has no spectral verdict at all")
    elif raw.spectral_grade in SPECTRAL_TRANSCODE_GRADES:
        failures.append("raw encode is itself transcode-graded, so not known clean")
    if decision_class_kbps(raw_spectral) is not None:
        failures.append("raw encode carries a class of its own")
    class_value = decision_class_kbps(classed_spectral)
    class_raw = _selected_bitrate(classed, CFG)
    if class_value is not None and class_raw is not None and class_value > class_raw:
        failures.append("the bound does not bind (class above the raw metric)")
    return failures


def assert_clamp_requires_comparability(
    basis: QualityComparisonBasis,
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    *,
    context: str,
) -> None:
    """``spectral_clamped`` implies a licensed spectral participation.

    Exactly two licences exist. The symmetric one requires
    ``spectral_classes_comparable`` — both sides decision-grade, the same
    derivation basis, and either the same codec or a ``cliff_hz`` basis.
    The one-sided licence is role-neutral: its class owns the spectral value
    whether it is the candidate or the HAVE, and its gates are restated in
    full by ``_one_sided_bound_licence_failures``.
    """
    if not basis.spectral_clamped:
        return
    new_spectral = interpret_measurement(new)
    existing_spectral = interpret_measurement(existing)
    if basis.branch in ("spectral_candidate_bound", "spectral_existing_bound"):
        class_is_new = basis.branch == "spectral_candidate_bound"
        failures = _one_sided_bound_licence_failures(
            new if class_is_new else existing,
            existing if class_is_new else new,
            new_spectral if class_is_new else existing_spectral,
            existing_spectral if class_is_new else new_spectral,
        )
        if failures:
            raise AssertionError(
                f"unlicensed one-sided bound ({context}): "
                + "; ".join(failures)
                + f" | new={new_spectral!r} existing={existing_spectral!r}")
        return
    if not spectral_classes_comparable(new_spectral, existing_spectral).comparable:
        raise AssertionError(
            f"spectral clamp fired on a non-comparable pair ({context}): "
            f"new={new_spectral!r} existing={existing_spectral!r}")


class TestClampComparabilityCheckerSelfTest(unittest.TestCase):
    def test_clamp_on_a_non_comparable_pair_trips(self):
        # An AAC can never be decision-grade, so any clamp naming it is
        # unlicensed.
        aac = AudioQualityMeasurement(
            min_bitrate_kbps=256, avg_bitrate_kbps=256, format="AAC",
            is_cbr=True, spectral_grade="likely_transcode",
            spectral_bitrate_kbps=128, spectral_subject="source",
            spectral_provenance="measured", codec_family="aac",
        )
        mp3 = AudioQualityMeasurement(
            min_bitrate_kbps=192, avg_bitrate_kbps=192, format="MP3",
            is_cbr=True, spectral_grade="likely_transcode",
            spectral_bitrate_kbps=192, spectral_subject="installed",
            spectral_provenance="measured", codec_family="mp3",
        )
        planted = QualityComparisonBasis(
            verdict="better", branch="rank", new_rank="good",
            existing_rank="acceptable", spectral_clamped=True,
        )
        with self.assertRaises(AssertionError) as caught:
            assert_clamp_requires_comparability(
                planted, aac, mp3, context="planted")
        self.assertIn("non-comparable", str(caught.exception))

    def test_unlicensed_candidate_bound_trips(self):
        mp3 = AudioQualityMeasurement(
            min_bitrate_kbps=320, avg_bitrate_kbps=320, format="MP3",
            is_cbr=True, spectral_grade="likely_transcode",
            spectral_bitrate_kbps=128, spectral_subject="source",
            spectral_provenance="measured", codec_family="mp3",
        )
        unmeasured = AudioQualityMeasurement(
            min_bitrate_kbps=160, avg_bitrate_kbps=160, format="MP3",
            is_cbr=True,
        )
        planted = QualityComparisonBasis(
            verdict="equivalent", branch="spectral_candidate_bound",
            new_rank="acceptable", existing_rank="acceptable",
            spectral_clamped=True,
        )
        with self.assertRaises(AssertionError) as caught:
            assert_clamp_requires_comparability(
                planted, mp3, unmeasured, context="planted")
        self.assertIn("unlicensed one-sided bound", str(caught.exception))

    def test_unlicensed_existing_bound_trips(self):
        classed = AudioQualityMeasurement(
            min_bitrate_kbps=320, avg_bitrate_kbps=320, format="MP3",
            is_cbr=True, spectral_grade="likely_transcode",
            spectral_bitrate_kbps=128, spectral_subject="installed",
            spectral_provenance="measured", codec_family="mp3",
        )
        unmeasured = AudioQualityMeasurement(
            min_bitrate_kbps=160, avg_bitrate_kbps=160, format="MP3",
            is_cbr=True,
        )
        planted = QualityComparisonBasis(
            verdict="equivalent", branch="spectral_existing_bound",
            new_rank="acceptable", existing_rank="acceptable",
            spectral_clamped=True,
        )
        with self.assertRaises(AssertionError) as caught:
            assert_clamp_requires_comparability(
                planted, unmeasured, classed, context="planted")
        self.assertIn("unlicensed one-sided bound", str(caught.exception))

    def test_unclamped_basis_is_always_fine(self):
        m = AudioQualityMeasurement(min_bitrate_kbps=192, format="MP3")
        assert_clamp_requires_comparability(
            QualityComparisonBasis(
                verdict="equivalent", branch="metric_tiebreak",
                new_rank="good", existing_rank="good"),
            m, m, context="planted")


#: Bare measured codec labels PLUS the explicit contract labels a
#: conversion target produces. The bound refuses a contract label — a
#: contract's rank ignores measured bitrate entirely, so a bound would be a
#: claim the rank never consumes — and a strategy drawing only bare labels
#: cannot reach that gate at all (PR2b review S3).
_FORMAT_LABELS: tuple[str, ...] = (
    "MP3", "AAC", "Opus", "Vorbis", "WMA", "FLAC",
    "mp3 v0", "mp3 v2", "mp3 320", "opus 128", "aac 192",
)


@st.composite
def measurement_pairs(draw) -> tuple[AudioQualityMeasurement, AudioQualityMeasurement]:
    """Two freely-drawn measurements — any codec against any other codec."""
    def _side(subject: EvidenceSubject) -> AudioQualityMeasurement:
        fmt = draw(st.sampled_from(_FORMAT_LABELS))
        grade = draw(st.sampled_from(_GRADES))
        bitrate = draw(st.integers(min_value=32, max_value=1200))
        family: CodecFamily | None = draw(st.sampled_from(
            (None, "mp3", "aac", "opus", "vorbis", "lossless", "other")))
        return AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            format=fmt,
            is_cbr=draw(st.booleans()),
            spectral_grade=grade,
            spectral_bitrate_kbps=(
                draw(_STORED) if grade is not None else None),
            spectral_subject=subject if grade is not None else None,
            spectral_provenance="measured" if grade is not None else None,
            cliff_hz=draw(_CLIFF_HZ) if grade is not None else None,
            codec_family=family if grade is not None else None,
        )
    return _side("source"), _side("installed")


#: The shrunk mixed-derivation world. Both sides are decision-grade MP3
#: with an authorizing grade, so a "both decision-grade" clamp gate admits
#: them — but one class came from a legacy stored bucket and the other from
#: a raw ``cliff_hz``, and a re-derived class sits systematically one tier
#: above a stored one. Pinned as an ``@example`` because the mixed-basis
#: region is roughly 0.5% of a free draw: the shipped ``suite`` tier's 150
#: derandomized examples never reach it, so without this pin the property
#: only patrols at the fuzz tier (PR2b review S2).
_MIXED_BASIS_PAIR = (
    AudioQualityMeasurement(
        min_bitrate_kbps=320, avg_bitrate_kbps=320, format="MP3", is_cbr=True,
        spectral_grade="suspect", spectral_bitrate_kbps=None,
        spectral_subject="source", spectral_provenance="measured",
        cliff_hz=11000, codec_family="mp3",
    ),
    AudioQualityMeasurement(
        min_bitrate_kbps=320, avg_bitrate_kbps=320, format="MP3", is_cbr=True,
        spectral_grade="suspect", spectral_bitrate_kbps=96,
        spectral_subject="installed", spectral_provenance="measured",
        cliff_hz=None, codec_family="mp3",
    ),
)

#: The same shape one step further out: two DIFFERENT ladder codecs whose
#: classes both came from the single LAME-shaped legacy table. Comparing
#: them measures the table's known one-directional Vorbis over-read, not
#: content (five such rows are live).
_CROSS_CODEC_LEGACY_PAIR = (
    AudioQualityMeasurement(
        min_bitrate_kbps=320, avg_bitrate_kbps=320, format="MP3", is_cbr=True,
        spectral_grade="likely_transcode", spectral_bitrate_kbps=192,
        spectral_subject="source", spectral_provenance="measured",
        codec_family="mp3",
    ),
    AudioQualityMeasurement(
        min_bitrate_kbps=320, avg_bitrate_kbps=320, format="Vorbis",
        spectral_grade="likely_transcode", spectral_bitrate_kbps=128,
        spectral_subject="installed", spectral_provenance="measured",
        codec_family="vorbis",
    ),
)


class TestClampRequiresComparability(unittest.TestCase):
    """Invariant 3 — cutoff Hz is not a common currency."""

    @example(pair=_MIXED_BASIS_PAIR)
    @example(pair=_CROSS_CODEC_LEGACY_PAIR)
    @given(pair=measurement_pairs())
    def test_compare_quality_never_clamps_a_non_comparable_pair(self, pair):
        new, existing = pair
        basis = compare_quality(new, existing, CFG)
        assert_clamp_requires_comparability(
            basis, new, existing, context=f"new={new!r} existing={existing!r}")

    def test_the_pinned_worlds_really_are_non_comparable(self):
        """Rule C for a property pin: prove the world reaches the branch.

        An ``@example`` that quietly stopped being mixed-basis would keep
        passing while patrolling nothing. Assert the refusal REASON, which
        is the fact the pin exists to exercise.
        """
        for pair, reason in (
            (_MIXED_BASIS_PAIR, "mixed_derivation_basis"),
            (_CROSS_CODEC_LEGACY_PAIR, "cross_codec_legacy_bucket"),
        ):
            new, existing = pair
            with self.subTest(reason=reason):
                new_spectral = interpret_measurement(new)
                existing_spectral = interpret_measurement(existing)
                self.assertTrue(new_spectral.decision_grade)
                self.assertTrue(existing_spectral.decision_grade)
                verdict = spectral_classes_comparable(
                    new_spectral, existing_spectral)
                self.assertFalse(verdict.comparable)
                self.assertEqual(verdict.reason, reason)
                # ...and the clamp really is withheld end to end.
                self.assertFalse(
                    compare_quality(new, existing, CFG).spectral_clamped)


# ===========================================================================
# Invariant 4 — the Fall 2007 fixed point
# ===========================================================================

def assert_no_mutual_displacement(
    forward: dict[str, object],
    backward: dict[str, object],
    *,
    context: str,
) -> None:
    """A pair of copies never displaces each other in both directions.

    That state IS the loop: request 8902 spent its life alternating between
    a fake 320 and a genuine 160, each "upgrading" over the other. Pinning
    one direction alone cannot rule it out. Authority: #829's accepted
    Fall 2007 case —
    https://github.com/abl030/cratedigger/issues/829#issuecomment-5098696861
    """
    if forward.get("imported") and backward.get("imported"):
        raise AssertionError(
            f"both directions import — the pair oscillates ({context})")


def assert_bound_verdict_uses_effective_values(
    basis: QualityComparisonBasis,
    *,
    context: str,
) -> None:
    """A one-sided bound compares its class and raw metric with tolerance."""
    if basis.branch not in ("spectral_candidate_bound", "spectral_existing_bound"):
        return
    if basis.new_value_kbps is None or basis.existing_value_kbps is None:
        return
    delta = basis.new_value_kbps - basis.existing_value_kbps
    tolerance = basis.tolerance_kbps or 0
    expected = (
        "equivalent" if abs(delta) <= tolerance
        else ("better" if delta > 0 else "worse")
    )
    if basis.verdict != expected:
        raise AssertionError(
            f"bounded verdict {basis.verdict!r} does not match effective "
            f"values {basis.new_value_kbps} vs {basis.existing_value_kbps} "
            f"({context})")


class TestFixedPointCheckerSelfTest(unittest.TestCase):
    def test_mutual_displacement_trips(self):
        with self.assertRaises(AssertionError) as caught:
            assert_no_mutual_displacement(
                {"imported": True}, {"imported": True}, context="planted")
        self.assertIn("oscillates", str(caught.exception))

    def test_one_direction_passes(self):
        assert_no_mutual_displacement(
            {"imported": True}, {"imported": False}, context="planted")

    def test_bound_verdict_against_its_effective_values_trips(self):
        with self.assertRaises(AssertionError) as caught:
            assert_bound_verdict_uses_effective_values(
                QualityComparisonBasis(
                    verdict="better", branch="spectral_candidate_bound",
                    new_rank="acceptable", existing_rank="acceptable",
                    new_value_kbps=160, existing_value_kbps=160,
                    tolerance_kbps=5),
                context="planted")
        self.assertIn("does not match effective values", str(caught.exception))

    def test_bound_equivalent_on_equal_effective_values_passes(self):
        assert_bound_verdict_uses_effective_values(
            QualityComparisonBasis(
                verdict="equivalent", branch="spectral_candidate_bound",
                new_rank="acceptable", existing_rank="acceptable",
                new_value_kbps=160, existing_value_kbps=160,
                tolerance_kbps=5),
            context="planted")


@dataclass(frozen=True)
class Fall2007World:
    """One transcode-classed MP3 and one known-clean MP3, same album."""

    fake_container: int
    fake_grade: str
    fake_cliff: int | None
    fake_stored: int
    clean_container: int
    #: None is the HAVE that was never spectrally measured — not a
    #: known-clean HAVE, so the bound must decline. Generated here so the
    #: is None half of the bound's HAVE gate is patrolled, not only the
    #: not a transcode grade half.
    clean_grade: str | None


@st.composite
def fall_2007_worlds(draw) -> Fall2007World:
    """One transcode-classed MP3 and one known-clean MP3, same album.

    The live shape: both CBR MP3, one carrying an authorizing grade plus a
    measured cliff, the other carrying an affirmative non-transcode verdict
    and no spectral bitrate at all.
    """
    return Fall2007World(
        fake_container=draw(st.sampled_from((128, 160, 192, 256, 320))),
        fake_grade=draw(st.sampled_from(("suspect", "likely_transcode"))),
        fake_cliff=draw(st.sampled_from(
            (None, 14500, 15500, 16500, 18000, 19000, 19500))),
        fake_stored=draw(st.sampled_from(_STORED_BUCKETS)),
        clean_container=draw(st.sampled_from((128, 160, 192, 256, 320))),
        clean_grade=draw(st.sampled_from(("genuine", "marginal", None))),
    )


class TestFall2007FixedPoint(unittest.TestCase):
    """Invariant 4 — the loop request 8902 lived in has no fixed point."""

    @staticmethod
    def _fake_candidate(world: Fall2007World) -> AlbumQualityEvidence:
        return build_parity_candidate_evidence(
            is_flac=False, native_format="MP3", native_codec="mp3",
            min_bitrate=world.fake_container,
            avg_bitrate=world.fake_container,
            is_cbr=True,
            spectral_grade=world.fake_grade,
            spectral_bitrate=world.fake_stored,
            cliff_hz=world.fake_cliff,
            codec_family="mp3",
        )

    @staticmethod
    def _fake_have(world: Fall2007World) -> AlbumQualityEvidence:
        have = build_parity_current_evidence(
            format="MP3",
            min_bitrate=world.fake_container,
            avg_bitrate=world.fake_container,
            is_cbr=True,
            spectral_grade=world.fake_grade,
            spectral_bitrate=world.fake_stored,
            cliff_hz=world.fake_cliff,
            codec_family="mp3",
        )
        assert have is not None  # container is always a definite int
        return have

    @staticmethod
    def _clean_candidate(world: Fall2007World) -> AlbumQualityEvidence:
        return build_parity_candidate_evidence(
            is_flac=False, native_format="MP3", native_codec="mp3",
            min_bitrate=world.clean_container,
            avg_bitrate=world.clean_container,
            is_cbr=True,
            spectral_grade=world.clean_grade,
        )

    @staticmethod
    def _clean_have(world: Fall2007World) -> AlbumQualityEvidence:
        have = build_parity_current_evidence(
            format="MP3",
            min_bitrate=world.clean_container,
            avg_bitrate=world.clean_container,
            is_cbr=True,
            spectral_grade=world.clean_grade,
        )
        assert have is not None
        return have

    @example(world=Fall2007World(
        # Request 8902 / evidence 34219, exactly.
        fake_container=320, fake_grade="likely_transcode",
        fake_cliff=16500, fake_stored=128,
        clean_container=160, clean_grade="genuine",
    ))
    @given(world=fall_2007_worlds())
    def test_the_pair_never_oscillates(self, world):
        if world.clean_grade is None:
            # Domain boundary, not a plausibility filter. Issue #911's
            # invariant is about a transcode-classed copy and a KNOWN-CLEAN
            # copy; an unmeasured HAVE is neither, and the bound
            # deliberately declines there
            # (test_an_unmeasured_have_is_not_a_known_clean_have).
            #
            # Those worlds DO contain a loop — a fake 160 whose class is 96
            # against an unmeasured 128 displaces and is displaced — but it
            # is pre-existing, not introduced here: neither direction routes
            # through any mechanism PR2b changed (no class on the
            # unmeasured side, and with no cliff the fake's class is its own
            # stored bucket, so both directions compute exactly what they
            # computed before). Asserting it here would claim this PR fixed
            # something it did not. The worlds are still generated because
            # the licensing and effective-value properties below need the
            # bound's grade is None gate exercised.
            return
        forward = full_pipeline_decision_from_evidence(
            self._fake_candidate(world),
            self._clean_have(world))
        backward = full_pipeline_decision_from_evidence(
            self._clean_candidate(world),
            self._fake_have(world))
        assert_no_mutual_displacement(
            forward, backward, context=repr(world))

    @example(world=Fall2007World(
        fake_container=320, fake_grade="likely_transcode",
        fake_cliff=16500, fake_stored=128,
        clean_container=160, clean_grade="genuine",
    ))
    @given(world=fall_2007_worlds())
    def test_a_bounded_verdict_uses_its_effective_values(self, world):
        decision = full_pipeline_decision_from_evidence(
            self._fake_candidate(world),
            self._clean_have(world))
        raw = decision.get("comparison_basis")
        if not isinstance(raw, dict):
            return
        assert_bound_verdict_uses_effective_values(
            msgspec.convert(raw, type=QualityComparisonBasis),
            context=repr(world))

    @given(world=fall_2007_worlds())
    def test_the_bound_never_exceeds_the_candidates_own_metric(self, world):
        """A bound is a CEILING. It can only ever lower the candidate."""
        candidate = self._fake_candidate(world)
        decision = full_pipeline_decision_from_evidence(
            candidate, self._clean_have(world))
        raw = decision.get("comparison_basis")
        if not isinstance(raw, dict) or raw.get("branch") != "spectral_candidate_bound":
            return
        bounded = raw.get("new_value_kbps")
        self.assertIsNotNone(bounded)
        assert isinstance(bounded, int)
        self.assertLessEqual(bounded, world.fake_container)
        self.assertLessEqual(
            measurement_rank(
                msgspec.structs.replace(
                    candidate.measurement, min_bitrate_kbps=bounded,
                    avg_bitrate_kbps=bounded, median_bitrate_kbps=bounded,
                ),
                CFG,
            ),
            measurement_rank(candidate.measurement, CFG),
        )


# ===========================================================================
# Invariant 5 — the lossless domain is untouched
# ===========================================================================

def assert_lossless_outcome_invariant(
    outcomes: Sequence[tuple[object, ...]],
    *,
    context: str,
) -> None:
    """A lossless container's decision ignores every codec-capture field.

    For a lossless container the cliff is the fake-lossless detector and
    the GRADE is the verdict; this module derives no kbps class for one.
    PR2b must not weaken that in either direction — neither by letting a
    ladder class leak in, nor by disarming the detector.
    """
    distinct = {outcome for outcome in outcomes}
    if len(distinct) > 1:
        raise AssertionError(
            f"a lossless candidate's decision moved with a codec-capture "
            f"field ({context}): "
            + " | ".join(repr(dict(zip(_OUTCOME_KEYS, o, strict=True)))
                         for o in sorted(distinct, key=repr))
        )


class TestLosslessInvarianceCheckerSelfTest(unittest.TestCase):
    def test_differing_outcomes_trip(self):
        with self.assertRaises(AssertionError) as caught:
            assert_lossless_outcome_invariant(
                [
                    ("import", None, "imported", True, False, False, True),
                    ("downgrade", None, "imported", False, True, True, False),
                ],
                context="planted")
        self.assertIn("moved with a codec-capture field", str(caught.exception))

    def test_identical_outcomes_pass(self):
        outcome = ("import", None, "imported", True, False, False, True)
        assert_lossless_outcome_invariant([outcome] * 3, context="planted")


@dataclass(frozen=True)
class LosslessWorld:
    """A lossless-container candidate against an arbitrary MP3 HAVE."""

    grade: str
    post_conversion: int | None
    v0_avg: int | None
    v0_min: int | None
    current_min: int | None
    current_grade: str | None
    current_stored: int | None


@st.composite
def lossless_candidate_worlds(draw) -> LosslessWorld:
    return LosslessWorld(
        grade=draw(st.sampled_from(
            ("genuine", "marginal", "suspect", "likely_transcode"))),
        post_conversion=draw(st.one_of(
            st.none(), st.integers(min_value=120, max_value=300))),
        v0_avg=draw(st.one_of(
            st.none(), st.integers(min_value=120, max_value=300))),
        v0_min=draw(st.one_of(
            st.none(), st.integers(min_value=100, max_value=300))),
        current_min=draw(st.one_of(
            st.none(), st.integers(min_value=64, max_value=1000))),
        current_grade=draw(st.sampled_from(_GRADES)),
        current_stored=draw(_STORED),
    )


class TestLosslessDomainUnchanged(unittest.TestCase):
    """Invariant 5 — PR2b must not weaken the fake-lossless detector."""

    @given(world=lossless_candidate_worlds())
    def test_codec_capture_never_moves_a_lossless_decision(self, world):
        current = build_parity_current_evidence(
            min_bitrate=world.current_min,
            avg_bitrate=world.current_min,
            format="MP3",
            is_cbr=True,
            spectral_grade=world.current_grade,
            spectral_bitrate=(
                world.current_stored
                if world.current_grade is not None else None),
        )
        outcomes: list[tuple[object, ...]] = []
        for stored in (None, 96, 128, 192, 320):
            for cliff in (None, 14000, 17000, 20000):
                candidate = build_parity_candidate_evidence(
                    is_flac=True,
                    min_bitrate=0,
                    is_cbr=False,
                    spectral_grade=world.grade,
                    post_conversion_min_bitrate=world.post_conversion,
                    candidate_v0_probe_avg=world.v0_avg,
                    candidate_v0_probe_min=world.v0_min,
                    cliff_hz=cliff,
                    codec_family="lossless",
                )
                candidate = _with_spectral_numbers(
                    candidate, stored=stored, cliff_hz=cliff)
                outcomes.append(_outcome(
                    full_pipeline_decision_from_evidence(candidate, current)))
        assert_lossless_outcome_invariant(outcomes, context=repr(world))


if __name__ == "__main__":
    unittest.main()
