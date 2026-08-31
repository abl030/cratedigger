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
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    SPECTRAL_AFFIRMATIVE_GRADES,
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
    ladder_class_kbps,
    measurement_rank,
    spectral_classes_comparable,
)
from lib.quality.ranks import _codec_family_of
from lib.quality.spectral_interpretation import _family_from_label
from tests.evidence_helpers import (
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

    Pre-existing gap, not introduced by this PR: this checker's "does the
    bound bind" clause below uses ``_selected_bitrate`` (the plain
    tolerance-comparison helper), while production
    (``_one_sided_spectral_bitrates``) uses
    ``_selected_quality_bitrate_with_source`` with the caller's V0 probe.
    The two agree UNLESS a ``lossless_source`` V0 probe is present, which
    this checker's callers never supply — so this checker cannot patrol a
    V0-probe-influenced bind/non-bind boundary.
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
    classed_label_family = _family_from_label(classed.format)
    if (
        classed_label_family is not None
        and classed_spectral.codec_family != classed_label_family
    ):
        # Restates the ``lib/quality/compare.py::_one_sided_spectral_bitrates``
        # SELF gate (issue #1204 defect 1, amended invariant). The raw-label
        # check just above is not enough on its own: a persisted
        # ``codec_family`` can override the label on the CLASSED side
        # (``resolve_measured_codec_family`` rule 2), so the classed side's
        # raw label can agree with the raw side's label while the classed
        # side's own INTERPRETED family disagrees with that same label —
        # the exact shape that survived this checker before this clause
        # existed, and the shape ``quality_rank(classed.format, ...)`` will
        # actually classify the returned class value through.
        #
        # Compared via ``_family_from_label`` — the SAME resolver
        # ``resolve_measured_codec_family`` uses for labels, not the
        # ranks-module ``_codec_family_of`` the cross-family clause above
        # uses (coarser: bare container tokens like "ogg"/"m4a" resolve to
        # "unknown" there). An unresolvable label is no-opinion, never a
        # failure — only two RESOLVED families that disagree trip this
        # clause, mirroring production exactly.
        #
        # Deliberately checks ONLY the classed side, never the raw side's
        # interpreted FAMILY — that family never licenses the bound and
        # never classifies a returned value; the only thing consumed from
        # the raw side's interpretation is the required ABSENCE of a class
        # (the clause below). An earlier version of this checker also
        # compared the classed side's interpreted family against the RAW
        # side's interpreted family — review proved that cross-side check
        # fail-open on the R19 converted-lineage cohort
        # (``resolve_measured_codec_family`` rule 3: a converted raw row
        # legitimately resolves to its SOURCE's family while its label
        # describes the on-disk derivative) — gating on it patrolled a
        # requirement production does not enforce.
        failures.append(
            "classed side's interpreted family disagrees with its own raw "
            "label")
    if raw.spectral_grade not in SPECTRAL_AFFIRMATIVE_GRADES:
        failures.append("raw encode is not affirmatively known clean")
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

    def test_classed_side_interpreted_family_disagrees_with_own_label_trips(self):
        # The fuzz-shrunk counterexample this fix's own burst found (issue
        # #1204 defect 1 World D): BOTH sides carry the SAME persisted
        # ``codec_family="mp3"``, and both raw LABELS agree ("FLAC" ==
        # "FLAC") — a pre-#1204 raw-label-only gate licenses this pair — but
        # the classed side's own interpreted family ('mp3') disagrees with
        # its OWN raw label ('flac'), which is the label
        # ``quality_rank(classed.format, ...)`` will actually classify its
        # bound value through. Every OTHER clause passes: the classed side
        # is decision-grade (cliff_hz=11000 on the mp3 ladder -> class 96),
        # authorized ("suspect"), neither side carries an explicit contract
        # label, the raw side is affirmatively clean ("genuine") with no
        # class of its own, and the class (96) binds under the classed
        # side's own raw metric (96) — so only the SELF clause can trip here.
        classed = AudioQualityMeasurement(
            min_bitrate_kbps=96, avg_bitrate_kbps=96, format="FLAC",
            is_cbr=False, spectral_grade="suspect",
            spectral_bitrate_kbps=None, spectral_subject="source",
            spectral_provenance="measured", cliff_hz=11000,
            codec_family="mp3",
        )
        raw = AudioQualityMeasurement(
            min_bitrate_kbps=32, avg_bitrate_kbps=32, format="FLAC",
            is_cbr=False, spectral_grade="genuine",
            spectral_bitrate_kbps=None, spectral_subject="installed",
            spectral_provenance="measured", codec_family="mp3",
        )
        planted = QualityComparisonBasis(
            verdict="worse", branch="spectral_candidate_bound",
            new_rank="lossless", existing_rank="lossless",
            spectral_clamped=True,
        )
        with self.assertRaises(AssertionError) as caught:
            assert_clamp_requires_comparability(
                planted, classed, raw, context="planted")
        self.assertIn(
            "disagrees with its own raw label", str(caught.exception))

    def test_only_the_classed_side_mismatches_its_label_trips(self):
        """World D above is not enough on its own (review F5): its RAW side
        also mismatches its own label (``codec_family="mp3"`` vs "FLAC"), so
        a SIDE-SWAP mutant — reading ``raw``/``raw_spectral`` instead of
        ``classed``/``classed_spectral`` in the clause under test — would
        ALSO trip on World D and survive undetected. This world (issue
        #1204 defect 1 World C) is the discriminator: ONLY the classed side
        mismatches — the raw side's own interpretation ('vorbis') agrees
        with its own label ("Vorbis") — so a side-swapped clause reads a
        MATCH on the raw side and never appends a failure, while the real
        clause (reading the classed side) correctly trips.
        """
        classed = AudioQualityMeasurement(
            min_bitrate_kbps=320, avg_bitrate_kbps=320, format="Vorbis",
            is_cbr=True, spectral_grade="likely_transcode",
            spectral_bitrate_kbps=None, spectral_subject="source",
            spectral_provenance="measured", cliff_hz=11000,
            codec_family="mp3",
        )
        raw = AudioQualityMeasurement(
            min_bitrate_kbps=320, avg_bitrate_kbps=320, format="Vorbis",
            is_cbr=True, spectral_grade="genuine",
            spectral_bitrate_kbps=None, spectral_subject="installed",
            spectral_provenance="measured",
        )
        planted = QualityComparisonBasis(
            verdict="worse", branch="spectral_candidate_bound",
            new_rank="acceptable", existing_rank="transparent",
            spectral_clamped=True,
        )
        with self.assertRaises(AssertionError) as caught:
            assert_clamp_requires_comparability(
                planted, classed, raw, context="planted")
        self.assertIn(
            "disagrees with its own raw label", str(caught.exception))

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


#: Conversion lineage a real row can legitimately carry
#: (``resolve_measured_codec_family`` rule 3): ``format`` describes the
#: on-disk DERIVATIVE while ``was_converted_from`` names the pre-conversion
#: SOURCE container. The R19 cohort (issue #1204 defect 1's amended
#: invariant) is 15,368 live rows this way; 15,333 of them carry
#: ``spectral_provenance='carried'`` (the common shape
#: ``build_parity_current_evidence``'s ``was_converted_from`` param
#: matches). ``_family_from_label`` RESOLVES ``flac``/``wav``/``wave``/
#: ``alac``/``aiff``/``aif``/``ape`` (all -> lossless, alac alone 5 live
#: rows) plus ``mp3``/``aac``/``opus``/``vorbis``/``wma``; it does NOT
#: resolve ``m4a`` (nor ``mp4``/``ogg``/``oga``) — those are AMBIGUOUS
#: containers (``_AMBIGUOUS_FORMAT_TOKENS``) and return ``None``. Drawing
#: "m4a" here therefore exercises the UNRESOLVED-source branch of rule 3
#: (``MeasuredCodecFamilyResolution(None, "unresolved")``), not a second
#: resolving token — both branches are real production paths worth
#: covering.
_WAS_CONVERTED_FROM: st.SearchStrategy[str | None] = st.one_of(
    st.none(), st.sampled_from(("flac", "m4a")))
_SPECTRAL_SUBJECT: st.SearchStrategy[EvidenceSubject] = st.sampled_from(
    (EVIDENCE_SUBJECT_SOURCE, EVIDENCE_SUBJECT_INSTALLED))


@st.composite
def measurement_pairs(draw) -> tuple[AudioQualityMeasurement, AudioQualityMeasurement]:
    """Two freely-drawn measurements — any codec against any other codec.

    ``spectral_subject`` and ``was_converted_from`` are drawn independently
    per side, not pinned to the side's comparison role (candidate vs
    existing) — a pre-#1204-review version of this strategy pinned
    ``spectral_subject`` to "source"/"installed" by role, which made the
    R19 converted-lineage shape (``resolve_measured_codec_family`` rule 3:
    ``spectral_subject == SOURCE`` + ``was_converted_from`` set) reachable
    only on the candidate side. This widening feeds the CHECKER's world
    space (``TestClampRequiresComparability`` — the ``spectral_clamped
    ⇒ licensed`` direction), which it does patrol either way a converted
    row lands. It does NOT, on its own, patrol the licensed-fires direction
    a regression like the resurrected CROSS gate needs (reverting this
    widening alone still passes every test here — review proved it): that
    is ``licensed_one_sided_worlds`` below, a CONSTRUCTIVE strategy that
    builds worlds already satisfying every one-sided licence gate,
    including a converted raw side.
    """
    def _side() -> AudioQualityMeasurement:
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
            spectral_subject=(
                draw(_SPECTRAL_SUBJECT) if grade is not None else None),
            spectral_provenance="measured" if grade is not None else None,
            cliff_hz=draw(_CLIFF_HZ) if grade is not None else None,
            codec_family=family if grade is not None else None,
            was_converted_from=(
                draw(_WAS_CONVERTED_FROM) if grade is not None else None),
        )
    return _side(), _side()


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

#: Issue #1204 defect 1, World A — verbatim from the 2026-08-18/19 overnight
#: journal (both nights, first run after PR #1187). The EXISTING side's raw
#: label reads "FLAC", but its persisted ``codec_family="mp3"`` resolves
#: (rule 2) to a decision-grade MP3-ladder class (cliff_hz=11000 -> 96) —
#: while the NEW side is a genuinely lossless, unmeasured-class FLAC.
#:
#: PRODUCTION (current, post-fix): ``_one_sided_spectral_bitrates`` refuses
#: this pair via the SELF gate — the EXISTING side's interpreted family
#: ('mp3') disagrees with its own raw label ('FLAC' -> lossless) — so
#: ``compare_quality`` never clamps it, whatever its terminal branch turns
#: out to be.
#:
#: THE CHECKER, historically (how the overnight fuzz actually found this,
#: pre-fix): the pre-#1204 raw-label-only gate compared "FLAC" == "FLAC"
#: and licensed the bound; both sides' RAW labels were still "FLAC", so
#: ``quality_rank`` put both at LOSSLESS regardless of the wrongly-bound
#: values, collapsing the terminal branch to ``lossless_same_rank`` while
#: still carrying ``spectral_clamped=True`` — the one-sided branch NAME
#: disappeared, so ``assert_clamp_requires_comparability`` routed through
#: the SYMMETRIC licence (``spectral_classes_comparable``) instead of
#: ``_one_sided_bound_licence_failures`` directly, and THAT symmetric check
#: (the NEW side is not decision-grade) is what actually tripped.
_ONE_SIDED_LABEL_MASKED_LOSSLESS_CLIFF_PAIR = (
    AudioQualityMeasurement(
        min_bitrate_kbps=32, avg_bitrate_kbps=32, format="FLAC", is_cbr=False,
        spectral_grade="genuine", spectral_bitrate_kbps=None,
        spectral_subject="source", spectral_provenance="measured",
        cliff_hz=None, codec_family=None,
    ),
    AudioQualityMeasurement(
        min_bitrate_kbps=96, avg_bitrate_kbps=96, format="FLAC", is_cbr=False,
        spectral_grade="suspect", spectral_bitrate_kbps=None,
        spectral_subject="installed", spectral_provenance="measured",
        cliff_hz=11000, codec_family="mp3",
    ),
)

#: Issue #1204 defect 1, World B — the same shape via a legacy STORED bucket
#: instead of a raw ``cliff_hz``: the EXISTING side's ``codec_family="mp3"``
#: resolves its stored ``spectral_bitrate_kbps=128`` to a decision-grade
#: class through the ``stored_bucket`` derivation. Both raw labels are
#: "FLAC" again, so this is the second overnight-journal shrink of the same
#: bug, not a second bug.
_ONE_SIDED_LABEL_MASKED_LOSSLESS_STORED_PAIR = (
    AudioQualityMeasurement(
        min_bitrate_kbps=357, avg_bitrate_kbps=357, format="FLAC",
        is_cbr=False, spectral_grade="marginal", spectral_bitrate_kbps=96,
        spectral_subject="source", spectral_provenance="measured",
        cliff_hz=None, codec_family=None,
    ),
    AudioQualityMeasurement(
        min_bitrate_kbps=1130, avg_bitrate_kbps=1130, format="FLAC",
        is_cbr=False, spectral_grade="likely_transcode",
        spectral_bitrate_kbps=128, spectral_subject="installed",
        spectral_provenance="measured", cliff_hz=None, codec_family="mp3",
    ),
)

#: Issue #1204 defect 1, World C — the CHECKER-EVADING variant (not from the
#: journal; constructed to prove the checker lockstep matters). Both raw
#: labels are "Vorbis" (not "FLAC"), so ``quality_rank`` does NOT collapse
#: both sides to LOSSLESS: the classed side's bound value (96) and the raw
#: side's own metric (320) land in different Vorbis rank bands (ACCEPTABLE
#: vs TRANSPARENT), so the terminal branch KEEPS the one-sided name
#: ("spectral_candidate_bound") instead of losing it to
#: ``lossless_same_rank`` the way World A/B do. That means this pair reaches
#: ``_one_sided_bound_licence_failures`` DIRECTLY — and before this PR's new
#: clause, every existing clause in that checker passes it (raw labels
#: "Vorbis" == "Vorbis"), so the checker agreed with the bug by construction.
#: This is exactly the gap issue #1204 named: "a variant like classed side
#: format='Vorbis' + codec_family='mp3' vs clean Vorbis raw side keeps the
#: one-sided branch name and survives the property today."
_ONE_SIDED_LABEL_MASKED_LOSSY_PAIR = (
    AudioQualityMeasurement(
        min_bitrate_kbps=320, avg_bitrate_kbps=320, format="Vorbis",
        is_cbr=True, spectral_grade="likely_transcode",
        spectral_bitrate_kbps=None, spectral_subject="source",
        spectral_provenance="measured", cliff_hz=11000, codec_family="mp3",
    ),
    AudioQualityMeasurement(
        min_bitrate_kbps=320, avg_bitrate_kbps=320, format="Vorbis",
        is_cbr=True, spectral_grade="genuine", spectral_bitrate_kbps=None,
        spectral_subject="installed", spectral_provenance="measured",
        cliff_hz=None, codec_family=None,
    ),
)

#: Issue #1204 defect 1, World D — NOT from the journal; a genuine gap found
#: by this PR's OWN fuzz burst while qualifying an EARLIER, since-dropped
#: cross-side check (``class_spectral.codec_family != raw_spectral.codec_
#: family``): both sides here carry the SAME persisted ``codec_family=
#: "mp3"``, so that (now-removed) check alone would have passed ('mp3' ==
#: 'mp3') even though BOTH sides' raw labels read "FLAC". The class value
#: (96, from the NEW side's cliff_hz=11000) is about to be classified via
#: ``quality_rank("FLAC", 96, cfg)``, which is LOSSLESS regardless of the
#: number — the exact mis-classification the SELF gate exists to prevent.
#: Fixed by (and still the reason for) the SELF check:
#: ``class_spectral.codec_family != class_family`` (the classed side's
#: interpreted family must match its OWN raw label) — the surviving gate
#: after the issue's amended invariant dropped the cross-side check as
#: fail-open on the R19 converted-lineage cohort.
_ONE_SIDED_SHARED_MISLABELED_FAMILY_PAIR = (
    AudioQualityMeasurement(
        min_bitrate_kbps=96, avg_bitrate_kbps=96, format="FLAC", is_cbr=False,
        spectral_grade="suspect", spectral_bitrate_kbps=None,
        spectral_subject="source", spectral_provenance="measured",
        cliff_hz=11000, codec_family="mp3",
    ),
    AudioQualityMeasurement(
        min_bitrate_kbps=32, avg_bitrate_kbps=32, format="FLAC", is_cbr=False,
        spectral_grade="genuine", spectral_bitrate_kbps=None,
        spectral_subject="installed", spectral_provenance="measured",
        cliff_hz=None, codec_family="mp3",
    ),
)


class TestClampRequiresComparability(unittest.TestCase):
    """Invariant 3 — cutoff Hz is not a common currency."""

    @example(pair=_MIXED_BASIS_PAIR)
    @example(pair=_CROSS_CODEC_LEGACY_PAIR)
    @example(pair=_ONE_SIDED_LABEL_MASKED_LOSSLESS_CLIFF_PAIR)
    @example(pair=_ONE_SIDED_LABEL_MASKED_LOSSLESS_STORED_PAIR)
    @example(pair=_ONE_SIDED_LABEL_MASKED_LOSSY_PAIR)
    @example(pair=_ONE_SIDED_SHARED_MISLABELED_FAMILY_PAIR)
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

    def test_the_label_masked_pins_reach_the_one_sided_licence_gate(self):
        """Rule C sibling for the four World A/B/C/D one-sided pins.

        Unlike ``_MIXED_BASIS_PAIR``/``_CROSS_CODEC_LEGACY_PAIR`` (both
        sides decision-grade, refused by the SYMMETRIC licence), each of
        these pins has exactly ONE decision-grade side and is refused by
        the ONE-SIDED licence's SELF gate (the classed side's own
        interpreted family disagrees with its own raw label) — all four,
        per the issue's amended invariant: an earlier CROSS gate (classed
        vs raw interpreted families) was dropped after review proved it
        fail-open on the R19 converted-lineage cohort; SELF alone refuses
        every one of these worlds. Every pin (a) really would have passed
        the pre-#1204 raw-label-only gate — same raw ``format`` family on
        both sides — and (b) really trips the SELF clause, asserted by
        ATTRIBUTION (calling the real checker and requiring its SELF
        failure string), not by inferring it from a world property such as
        "the two interpreted families differ" — a world can have that
        property for reasons unrelated to the clause under test.
        """
        cases = (
            ("world_a_cliff", _ONE_SIDED_LABEL_MASKED_LOSSLESS_CLIFF_PAIR,
             False, True, "lossless", "mp3", 96, "lossless_same_rank"),
            ("world_b_stored", _ONE_SIDED_LABEL_MASKED_LOSSLESS_STORED_PAIR,
             False, True, "lossless", "mp3", 128, "lossless_same_rank"),
            ("world_c_vorbis_label", _ONE_SIDED_LABEL_MASKED_LOSSY_PAIR,
             True, False, "mp3", "vorbis", 96, "metric_tiebreak"),
            ("world_d_shared_mislabel", _ONE_SIDED_SHARED_MISLABELED_FAMILY_PAIR,
             True, False, "mp3", "mp3", 96, "lossless_same_rank"),
        )
        for (
            name, pair, new_is_classed, existing_is_classed,
            new_family, existing_family, expected_class, expected_branch,
        ) in cases:
            new, existing = pair
            with self.subTest(world=name):
                new_spectral = interpret_measurement(new)
                existing_spectral = interpret_measurement(existing)
                self.assertEqual(new_spectral.decision_grade, new_is_classed)
                self.assertEqual(
                    existing_spectral.decision_grade, existing_is_classed)
                classed, raw = (
                    (new, existing) if new_is_classed else (existing, new))
                classed_spectral, raw_spectral = (
                    (new_spectral, existing_spectral) if new_is_classed
                    else (existing_spectral, new_spectral))
                self.assertEqual(
                    decision_class_kbps(classed_spectral), expected_class)
                # (a) the raw LABEL family is identical on both sides — the
                # pre-#1204 gate has nothing to refuse this pair on.
                self.assertEqual(
                    _codec_family_of(new.format), _codec_family_of(existing.format))
                self.assertEqual(new_spectral.codec_family, new_family)
                self.assertEqual(existing_spectral.codec_family, existing_family)
                # (b) attribution: the real checker names the SELF clause.
                failures = _one_sided_bound_licence_failures(
                    classed, raw, classed_spectral, raw_spectral)
                self.assertIn(
                    "classed side's interpreted family disagrees with its "
                    "own raw label", failures)
                # ...and, post-fix, the clamp is withheld end to end and the
                # decision lands on the expected unclamped terminal branch.
                basis = compare_quality(new, existing, CFG)
                self.assertFalse(basis.spectral_clamped)
                self.assertEqual(basis.branch, expected_branch)


@st.composite
def licensed_one_sided_worlds(
    draw,
) -> tuple[AudioQualityMeasurement, AudioQualityMeasurement, str]:
    """Construct a world that passes EVERY one-sided licence gate BY
    CONSTRUCTION, for both roles.

    Invariant 3's missing POSITIVE direction (review F4):
    ``assert_clamp_requires_comparability`` is one-directional —
    ``spectral_clamped ⇒ licensed`` — so it can NEVER catch a regression
    that withholds MORE than production actually withholds; over-refusing
    is always 'safe' to a checker that only ever asks "if clamped, was it
    licensed". Only a property that asserts the bound actually FIRES can
    catch that shape — exactly the resurrected-CROSS regression review
    found: before this property existed, the R19 must-still-work pin below
    was the ONLY thing killing that mutant.

    The classed side is built to satisfy every gate: its interpreted
    family (resolved from its bare label, no persisted ``codec_family``
    override) agrees with its own label, its grade authorizes an
    accusation, and its cliff derives a class that binds under its own raw
    metric. The raw side is independently free to be a converted row
    (``was_converted_from`` set, resolving through a DIFFERENT family than
    its own label via rule 3) — SELF must never refuse on that basis; only
    a resurrected CROSS gate would.
    """
    class_is_new = draw(st.booleans())
    family: CodecFamily = draw(st.sampled_from(("mp3", "vorbis")))
    label = "MP3" if family == "mp3" else "Vorbis"
    grade = draw(st.sampled_from(("suspect", "likely_transcode")))
    cliff_hz = draw(st.integers(min_value=11000, max_value=22000))
    class_value = ladder_class_kbps(family, cliff_hz)
    assert class_value is not None  # mp3/vorbis always bucket somewhere
    raw_grade = draw(st.sampled_from(("genuine", "marginal")))
    raw_bitrate = draw(st.integers(min_value=32, max_value=1200))
    # Never below raw_bitrate: when class_is_new, a transcode-grade "new"
    # with a lower REAL (unclamped) rank than a non-transcode "existing"
    # is refused by ``_transcode_candidate_real_rank_regresses`` BEFORE
    # ``_one_sided_spectral_bitrates`` ever runs — a real, earlier gate
    # this property must construct AROUND, not one it exists to patrol.
    class_bitrate = (
        max(class_value, raw_bitrate)
        + draw(st.integers(min_value=0, max_value=400)))
    raw_was_converted_from = draw(st.one_of(
        st.none(),
        st.sampled_from(("flac", "m4a", "aac", "opus", "vorbis", "mp3"))))
    raw_subject = (
        EVIDENCE_SUBJECT_SOURCE if raw_was_converted_from is not None
        else draw(_SPECTRAL_SUBJECT))

    classed_measurement = AudioQualityMeasurement(
        min_bitrate_kbps=class_bitrate, avg_bitrate_kbps=class_bitrate,
        format=label, is_cbr=draw(st.booleans()),
        spectral_grade=grade, cliff_hz=cliff_hz,
        spectral_subject=EVIDENCE_SUBJECT_SOURCE,
        spectral_provenance="measured",
    )
    raw_measurement = AudioQualityMeasurement(
        min_bitrate_kbps=raw_bitrate, avg_bitrate_kbps=raw_bitrate,
        format=label, is_cbr=draw(st.booleans()),
        spectral_grade=raw_grade,
        spectral_subject=raw_subject, spectral_provenance="measured",
        was_converted_from=raw_was_converted_from,
    )
    if class_is_new:
        return classed_measurement, raw_measurement, "spectral_candidate_bound"
    return raw_measurement, classed_measurement, "spectral_existing_bound"


#: The reviewer's exact R19 repro, pinned as an ``@example`` so the
#: converted-raw-with-differing-family shape is always tested
#: deterministically, not left to fuzz probability.
_R19_LICENSED_EXAMPLE = (
    AudioQualityMeasurement(
        min_bitrate_kbps=320, avg_bitrate_kbps=320, format="MP3",
        is_cbr=True, spectral_grade="likely_transcode",
        spectral_bitrate_kbps=160, spectral_subject="source",
        spectral_provenance="measured",
    ),
    AudioQualityMeasurement(
        min_bitrate_kbps=245, avg_bitrate_kbps=245, format="MP3",
        is_cbr=False, spectral_grade="genuine",
        was_converted_from="flac", spectral_subject="source",
        spectral_provenance="carried",
    ),
    "spectral_candidate_bound",
)


class TestLicensedOneSidedWorldsAlwaysFire(unittest.TestCase):
    """Invariant 3's missing POSITIVE direction — see the strategy above."""

    @example(world=_R19_LICENSED_EXAMPLE)
    @given(world=licensed_one_sided_worlds())
    def test_a_licensed_one_sided_world_always_fires(self, world):
        new, existing, expected_branch = world
        basis = compare_quality(new, existing, CFG)
        self.assertTrue(
            basis.spectral_clamped,
            f"licensed world was NOT clamped: new={new!r} existing={existing!r} "
            f"basis={basis!r}")
        self.assertEqual(basis.branch, expected_branch)


class TestR19ConvertedLineageMustStillWork(unittest.TestCase):
    """Must-still-work: dropping the CROSS gate did not disarm SELF.

    Issue #1204 defect 1's amended invariant: an earlier version of
    ``_one_sided_spectral_bitrates`` also required the RAW side's
    interpreted family to match the classed side's (the CROSS gate).
    Review proved that fail-open on the R19 converted-lineage cohort
    (15,368 live rows; 134 eligible as the known-clean raw side of this
    exact comparison; 8 of their requests currently ``wanted``):
    ``resolve_measured_codec_family`` rule 3 legitimately resolves a
    converted row (``spectral_subject='source'`` + ``was_converted_from``
    set) to its SOURCE's family while its ``format`` label still names the
    on-disk derivative — CROSS then refused a licensed bound the ORIGINAL
    raw-label-only gate (and today's SELF-only gate) both license, letting
    a fake CBR-320/class-160 candidate displace a genuine converted
    MP3-245 copy. These pins drive the REAL decider
    (``full_pipeline_decision_from_evidence``), not ``compare_quality``
    directly, because the consequence that matters is what the pipeline
    DECIDES to do, not an intermediate comparison basis.
    """

    def test_the_r19_shape_correctly_rejects_the_fake_transcode(self):
        """Reviewer's exact repro, both directions of "the shape holds"."""
        fake_candidate = build_parity_candidate_evidence(
            is_flac=False, native_format="MP3", native_codec="mp3",
            min_bitrate=320, avg_bitrate=320, is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=160,
        )
        converted_have = build_parity_current_evidence(
            min_bitrate=245, avg_bitrate=245, format="MP3", is_cbr=False,
            spectral_grade="genuine", was_converted_from="flac",
        )
        assert converted_have is not None
        decision = full_pipeline_decision_from_evidence(
            fake_candidate, converted_have)
        basis = decision.get("comparison_basis")
        assert isinstance(basis, dict)
        self.assertEqual(basis.get("branch"), "spectral_candidate_bound")
        self.assertEqual(basis.get("verdict"), "worse")
        self.assertFalse(decision.get("imported"))

    def test_the_control_documents_the_like_shape_without_conversion(self):
        """Same pair, ``was_converted_from=None`` — the outcome must not move.

        NOT a general claim that conversion lineage is never
        decision-relevant — it can be, because lineage decides which SIDE
        of a comparison gets classed at all (review F2): a candidate MP3
        245 genuine versus a HAVE MP3 320 suspect/cliff-11000 flips from
        `spectral_existing_bound`/better when the HAVE is native to
        `rank`/worse when the SAME HAVE is converted-from-FLAC — because a
        converted row's interpretation runs through its LOSSLESS source
        (rule 3), and lossless interpretations never reach
        ``decision_grade=True`` (155 ``suspect`` + 453 ``likely_transcode``
        live rows sit in that exact shape). This test isolates the
        narrower claim the R19 pin actually needs: with an affirmatively
        ``genuine`` grade, the HAVE has no class in EITHER lineage — SELF
        never reads the raw side's interpretation, so there is nothing left
        that COULD move the outcome between native and converted for this
        specific pair.
        """
        fake_candidate = build_parity_candidate_evidence(
            is_flac=False, native_format="MP3", native_codec="mp3",
            min_bitrate=320, avg_bitrate=320, is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=160,
        )
        native_have = build_parity_current_evidence(
            min_bitrate=245, avg_bitrate=245, format="MP3", is_cbr=False,
            spectral_grade="genuine",
        )
        assert native_have is not None
        decision = full_pipeline_decision_from_evidence(
            fake_candidate, native_have)
        basis = decision.get("comparison_basis")
        assert isinstance(basis, dict)
        self.assertEqual(basis.get("branch"), "spectral_candidate_bound")
        self.assertEqual(basis.get("verdict"), "worse")
        self.assertFalse(decision.get("imported"))

    def test_a_plainly_licensed_one_sided_world_fires_in_both_roles(self):
        """SELF-only still licenses the ordinary one-sided bound either way.

        Dropping CROSS must not have weakened the ordinary, non-anomalous
        case: a decision-grade MP3 class against a known-clean same-family
        MP3 raw side still binds whichever side carries the class.
        """
        classed_mp3 = AudioQualityMeasurement(
            min_bitrate_kbps=320, avg_bitrate_kbps=320, format="MP3",
            is_cbr=True, spectral_grade="suspect",
            spectral_bitrate_kbps=160, spectral_subject="source",
            spectral_provenance="measured",
        )
        clean_mp3 = AudioQualityMeasurement(
            min_bitrate_kbps=128, avg_bitrate_kbps=128, format="MP3",
            is_cbr=True, spectral_grade="genuine",
            spectral_subject="installed", spectral_provenance="measured",
        )
        candidate_bound = compare_quality(classed_mp3, clean_mp3, CFG)
        self.assertTrue(candidate_bound.spectral_clamped)
        self.assertEqual(candidate_bound.branch, "spectral_candidate_bound")
        existing_bound = compare_quality(clean_mp3, classed_mp3, CFG)
        self.assertTrue(existing_bound.spectral_clamped)
        self.assertEqual(existing_bound.branch, "spectral_existing_bound")


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
