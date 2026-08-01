"""Generated tests — issue #829 Phase 5 PR4 verdict tiers and their copy.

The deterministic pins live in ``tests/test_verdict_tiers.py``; these
properties patrol the world space around them over any evidence-field
combination, any grade, any leg outcome, any classifier.

Six invariants, each a PAIR with its pin per
``.claude/rules/code-quality.md``, and each with a module-level checker
plus a known-bad self-test proving the checker trips on a planted
violation:

**V1 — the tier is a pure function of the fired-leg set.** Two albums with
the same fired legs always land on the same tier, and the fired legs are
always a subset of the evaluated ones. Nothing else may enter the ladder.

**V2 — the reserved ceiling tiers are never produced.** Production has no
ceiling leg, so no input may reach tier 2 or 3 and no operator copy exists
for them (Rule C: copy keyed on a scenario no producer emits).

**V3 — an audit-only codec never gets a spectral finding.** For AAC, Opus
and every unresolved family the cliff leg neither fires nor counts as
evaluated, whatever the codec-blind analyzer graded. This is issue #829's
opening defect stated as a permanent law over the DISPLAY surface.

**V4 — the verdict a surface renders is the verdict the decider's legs
produce.** ``proof_verdict_from_evidence`` (whole row, what
``pipeline-cli quality`` runs) and ``proof_verdict_from_facts`` (flat
columns, what the Recents render path runs) agree on every world. Two
surfaces disagreeing about one album is the drift this pair exists to
prevent.

**V5 — no statement widens the claim.** No tier sentence, at any tier,
ever asserts bit-faithfulness, a guarantee, or posterior odds (Phase 5
plan §1.7 and §1's base-rate caveat).

**V6 — a verdict with no evaluated leg never reads as a clearance.**
"nothing was found" and "nothing was looked for" are different facts;
``has_finding`` separates them and the statement says so.

Checkers take their production dependency as a keyword-only argument
defaulting to the real function, so the known-bad self-tests can pass a
decoy explicitly while production always gets the default.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import datetime
import os
import sys
import unittest
from collections.abc import Callable, Sequence
from dataclasses import replace

from hypothesis import example, given
from hypothesis import strategies as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
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
    SPECTRAL_DECODE_PATH_FFMPEG_RESAMPLED,
    SPECTRAL_DECODE_PATH_SOX_NATIVE,
    AacLatticeCapture,
    AlbumProofVerdict,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    SpectralDecodePath,
    album_proof_verdict,
    interpret_spectral_cliff,
    proof_tier_statement,
    proof_verdict_from_evidence,
    proof_verdict_from_facts,
)
from lib.quality.decisions import (
    aac_lattice_proof_leg,
    ultrasonic_proof_leg,
)

_CODEC_FAMILIES = ("mp3", "aac", "opus", "vorbis", "lossless", "other")
_AUDIT_ONLY_FAMILIES = ("aac", "opus", "other")
_GRADE_VALUES: tuple["str | None", ...] = (
    "genuine", "marginal", "suspect", "likely_transcode", "error",
    None, "", "LIKELY_TRANSCODE", "unknown-future-grade",
)
_FORMAT_LABELS: tuple["str | None", ...] = (
    None, "MP3", "mp3 v0", "AAC", "ALAC", "Opus", "opus 128", "Vorbis",
    "FLAC", "flac", "wav", "m4a", "ogg", "WMA", "", "unknown-codec",
)
_FILETYPE_BANDS = (
    "", "mp3", "flac", "m4a", "opus", "ogg",
    "mixed", "mixed_lossy", "mixed_lossless", "m4a, mp3",
)
_EXTENSIONS = (".flac", ".mp3", ".m4a", ".ogg", ".opus", ".wav", ".wma", "")
_SUBJECTS: tuple["str | None", ...] = (None, "installed", "source")
_DECODE_PATHS: tuple["SpectralDecodePath | None", ...] = (
    None, SPECTRAL_DECODE_PATH_SOX_NATIVE, SPECTRAL_DECODE_PATH_FFMPEG_RESAMPLED)
_ULTRA_OUTCOMES = ("denied", "passed", "withheld")
_LATTICE_OUTCOMES = ("denied", "passed", "withheld")


@st.composite
def _ultrasonic_legs(draw: st.DrawFn):
    """Any ultrasonic leg the production function can emit."""
    return ultrasonic_proof_leg(
        deficit_db=draw(st.one_of(
            st.none(), st.floats(min_value=0.0, max_value=120.0))),
        spectral_measurement_version=draw(
            st.one_of(st.none(), st.integers(min_value=0, max_value=4))),
        decode_path=draw(st.sampled_from(_DECODE_PATHS)),
        preserved_source_spectral=draw(st.booleans()),
    )


@st.composite
def _lattice_legs(draw: st.DrawFn):
    """Any lattice leg the production function can emit."""
    if draw(st.booleans()):
        return aac_lattice_proof_leg(None)
    scored = draw(st.integers(min_value=0, max_value=8))
    return aac_lattice_proof_leg(AacLatticeCapture(
        modal_count=draw(
            st.one_of(st.none(), st.integers(min_value=1, max_value=8))),
        scored_tracks=scored,
        max_z=draw(st.one_of(
            st.none(), st.floats(min_value=0.0, max_value=40.0))),
    ))


@st.composite
def _verdict_worlds(draw: st.DrawFn):
    """Any (interpretation, grade, ultrasonic leg, lattice leg) world."""
    family = draw(st.one_of(st.none(), st.sampled_from(_CODEC_FAMILIES)))
    grade = draw(st.sampled_from(_GRADE_VALUES))
    spectral = interpret_spectral_cliff(
        family,
        spectral_grade=grade,
        cliff_hz=draw(st.one_of(
            st.none(), st.integers(min_value=0, max_value=24000))),
        stored_bitrate_kbps=draw(st.one_of(
            st.none(), st.integers(min_value=-10, max_value=1400))),
    )
    return (
        spectral,
        grade,
        draw(st.one_of(st.none(), _ultrasonic_legs())),
        draw(st.one_of(st.none(), _lattice_legs())),
    )


@st.composite
def _evidence_facts(draw: st.DrawFn) -> dict[str, object]:
    """Any flat persisted-column world the render path can hand over."""
    return {
        "spectral_grade": draw(st.sampled_from(_GRADE_VALUES)),
        "spectral_bitrate_kbps": draw(st.one_of(
            st.none(), st.integers(min_value=-10, max_value=1400))),
        "cliff_hz": draw(st.one_of(
            st.none(), st.integers(min_value=0, max_value=24000))),
        "codec_family": draw(
            st.one_of(st.none(), st.sampled_from(_CODEC_FAMILIES))),
        "format": draw(st.sampled_from(_FORMAT_LABELS)),
        "storage_format": draw(st.sampled_from(_FORMAT_LABELS)),
        "filetype_band": draw(st.sampled_from(_FILETYPE_BANDS)),
        "spectral_subject": draw(st.sampled_from(_SUBJECTS)),
        "was_converted_from": draw(st.sampled_from(_FORMAT_LABELS)),
        "container_labels": draw(st.lists(
            st.sampled_from(_EXTENSIONS), min_size=0, max_size=3)),
        "ultrasonic_deficit_db": draw(st.one_of(
            st.none(), st.floats(min_value=0.0, max_value=120.0))),
        "spectral_measurement_version": draw(
            st.one_of(st.none(), st.integers(min_value=0, max_value=4))),
        "aac_lattice": draw(st.one_of(st.none(), st.builds(
            AacLatticeCapture,
            modal_count=st.one_of(
                st.none(), st.integers(min_value=1, max_value=8)),
            scored_tracks=st.integers(min_value=0, max_value=8),
            max_z=st.one_of(
                st.none(), st.floats(min_value=0.0, max_value=40.0)),
        ))),
    }


def _evidence_from_facts(facts: dict[str, object]) -> AlbumQualityEvidence:
    """The same world as a whole persisted row, for the V4 parity check."""
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=256,
        format=facts["format"],  # pyright: ignore[reportArgumentType]
        spectral_grade=facts["spectral_grade"],  # pyright: ignore[reportArgumentType]
        spectral_bitrate_kbps=facts["spectral_bitrate_kbps"],  # pyright: ignore[reportArgumentType]
        spectral_subject=facts["spectral_subject"],  # pyright: ignore[reportArgumentType]
        was_converted_from=facts["was_converted_from"],  # pyright: ignore[reportArgumentType]
        cliff_hz=facts["cliff_hz"],  # pyright: ignore[reportArgumentType]
        codec_family=facts["codec_family"],  # pyright: ignore[reportArgumentType]
        ultrasonic_deficit_db=facts["ultrasonic_deficit_db"],  # pyright: ignore[reportArgumentType]
        spectral_measurement_version=facts["spectral_measurement_version"],  # pyright: ignore[reportArgumentType]
    )
    labels: Sequence[str] = facts["container_labels"]  # pyright: ignore[reportAssignmentType]
    return AlbumQualityEvidence(
        mb_release_id="rel",
        snapshot_fingerprint="fp",
        source_path="/tmp/album",
        measurement=measurement,
        measured_at=datetime.datetime(2026, 8, 1, tzinfo=datetime.UTC),
        files=[
            AlbumQualityEvidenceFile(
                relative_path=f"{index:02d}.audio",
                size_bytes=1,
                mtime_ns=1,
                extension=extension,
                container=extension.lstrip("."),
            )
            for index, extension in enumerate(labels)
        ],
        storage_format=facts["storage_format"],  # pyright: ignore[reportArgumentType]
        filetype_band=facts["filetype_band"],  # pyright: ignore[reportArgumentType]
        aac_lattice=facts["aac_lattice"],  # pyright: ignore[reportArgumentType]
    )


# ---------------------------------------------------------------------------
# Invariant checkers (module level so the self-tests can call them directly)
# ---------------------------------------------------------------------------


def check_tier_follows_fired_legs(
    verdict: AlbumProofVerdict,
    *,
    tier_of: Callable[[tuple[str, ...]], int] | None = None,
) -> None:
    """V1: the tier is exactly the fired-leg set's severity band."""
    def _default(fired: tuple[str, ...]) -> int:
        if PROOF_LEG_IN_WINDOW_CLIFF in fired or PROOF_LEG_AAC_LATTICE in fired:
            return PROOF_TIER_DETECTED
        if PROOF_LEG_NO_ULTRASONIC in fired:
            return PROOF_TIER_NO_ULTRASONIC
        return PROOF_TIER_NO_FINDING

    resolve = tier_of if tier_of is not None else _default
    expected = resolve(verdict.fired_legs)
    if verdict.tier != expected:
        raise AssertionError(
            f"tier {verdict.tier} does not follow fired legs "
            f"{verdict.fired_legs} (expected {expected})")
    if not set(verdict.fired_legs) <= set(verdict.evaluated_legs):
        raise AssertionError(
            f"fired legs {verdict.fired_legs} are not a subset of evaluated "
            f"legs {verdict.evaluated_legs}")


def check_reserved_ceiling_tiers_unused(verdict: AlbumProofVerdict) -> None:
    """V2: nothing may produce the unimplemented ceiling tiers."""
    if verdict.tier in (
        PROOF_TIER_CEILING_AND_NO_ULTRASONIC, PROOF_TIER_CEILING_ONLY,
    ):
        raise AssertionError(
            f"tier {verdict.tier} is reserved for the ceiling leg production "
            "does not measure, and has no operator copy")
    if verdict.tier not in PRODUCIBLE_PROOF_TIERS:
        raise AssertionError(f"tier {verdict.tier} is not a producible tier")


def check_audit_only_codec_has_no_spectral_finding(
    verdict: AlbumProofVerdict, family: "str | None",
) -> None:
    """V3: an audit-only codec never fires or evaluates the cliff leg."""
    if family is not None and family not in _AUDIT_ONLY_FAMILIES:
        return
    if verdict.spectral_accusation_admissible:
        raise AssertionError(
            f"codec family {family!r} must never admit a transcode accusation")
    if PROOF_LEG_IN_WINDOW_CLIFF in verdict.fired_legs:
        raise AssertionError(
            f"codec family {family!r} fired the in-window cliff leg")
    if PROOF_LEG_IN_WINDOW_CLIFF in verdict.evaluated_legs:
        raise AssertionError(
            f"codec family {family!r} counted the cliff leg as evaluated — "
            "an untested album must never read as a cleared one")


def check_surfaces_agree(
    row_verdict: AlbumProofVerdict, facts_verdict: AlbumProofVerdict,
) -> None:
    """V4: the whole-row and flat-column derivations never disagree."""
    if row_verdict != facts_verdict:
        raise AssertionError(
            "pipeline-cli and the render path disagree about one album: "
            f"{row_verdict} vs {facts_verdict}")


_CLAIM_WIDENING_TOKENS = (
    "bit-perfect", "bit perfect", "bit-faithful", "bit faithful",
    "guarantee", "guaranteed", "certain", "probably fake", "definitely",
    "proven lossless", "authentic",
)


def check_statement_does_not_widen_the_claim(statement: str) -> None:
    """V5: no tier sentence may claim more than the tests support."""
    lowered = statement.lower()
    for token in _CLAIM_WIDENING_TOKENS:
        if token in lowered:
            raise AssertionError(
                f"tier statement {statement!r} widens the claim via {token!r}")


def check_untested_album_is_not_a_clearance(
    verdict: AlbumProofVerdict, statement: str,
) -> None:
    """V6: no evaluated leg means the statement must not read as a pass."""
    if verdict.has_finding:
        return
    if statement != "No proof-gate test could run on this album":
        raise AssertionError(
            f"verdict with no evaluated leg rendered {statement!r}, which "
            "reads as a clearance nothing tested for")


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------


class TestVerdictTierProperties(unittest.TestCase):

    @example(world=(
        interpret_spectral_cliff(
            "aac", spectral_grade="likely_transcode", cliff_hz=15000),
        "likely_transcode", None, None,
    ))
    @example(world=(
        interpret_spectral_cliff("lossless", spectral_grade="likely_transcode"),
        "likely_transcode", None, None,
    ))
    @given(world=_verdict_worlds())
    def test_tier_and_legs_hold(self, world) -> None:
        spectral, grade, ultra, lattice = world
        verdict = album_proof_verdict(
            spectral=spectral, spectral_grade=grade,
            ultrasonic_leg=ultra, aac_lattice_leg=lattice)
        check_tier_follows_fired_legs(verdict)
        check_reserved_ceiling_tiers_unused(verdict)
        check_audit_only_codec_has_no_spectral_finding(
            verdict, spectral.codec_family)
        statement = proof_tier_statement(verdict)
        check_statement_does_not_widen_the_claim(statement)
        check_untested_album_is_not_a_clearance(verdict, statement)

    @example(facts={
        "spectral_grade": "likely_transcode",
        "spectral_bitrate_kbps": 128,
        "cliff_hz": 15000,
        "codec_family": "aac",
        "format": "AAC",
        "storage_format": "AAC",
        "filetype_band": "m4a",
        "spectral_subject": "installed",
        "was_converted_from": None,
        "container_labels": [".m4a"],
        "ultrasonic_deficit_db": None,
        "spectral_measurement_version": 2,
        "aac_lattice": None,
    })
    @given(facts=_evidence_facts())
    def test_render_path_and_cli_agree(self, facts) -> None:
        """V4 over any world: one album, one verdict, both surfaces."""
        facts_verdict = proof_verdict_from_facts(**facts)
        row_verdict = proof_verdict_from_evidence(_evidence_from_facts(facts))
        check_surfaces_agree(row_verdict, facts_verdict)
        check_tier_follows_fired_legs(facts_verdict)
        check_reserved_ceiling_tiers_unused(facts_verdict)
        statement = proof_tier_statement(facts_verdict)
        check_statement_does_not_widen_the_claim(statement)
        check_untested_album_is_not_a_clearance(facts_verdict, statement)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Every checker owes a planted violation proving it can fail."""

    def _verdict(self, **overrides) -> AlbumProofVerdict:
        base = AlbumProofVerdict(
            tier=PROOF_TIER_NO_FINDING,
            fired_legs=(),
            evaluated_legs=(PROOF_LEG_IN_WINDOW_CLIFF,),
            spectral_accusation_admissible=False,
        )
        return replace(base, **overrides)

    def test_tier_checker_trips_on_a_mismatched_ladder(self):
        with self.assertRaises(AssertionError):
            check_tier_follows_fired_legs(
                self._verdict(
                    fired_legs=(PROOF_LEG_NO_ULTRASONIC,),
                    evaluated_legs=(PROOF_LEG_NO_ULTRASONIC,),
                ),
                tier_of=lambda _fired: PROOF_TIER_DETECTED,
            )

    def test_tier_checker_trips_when_a_leg_fired_without_being_evaluated(self):
        with self.assertRaises(AssertionError):
            check_tier_follows_fired_legs(self._verdict(
                tier=PROOF_TIER_DETECTED,
                fired_legs=(PROOF_LEG_AAC_LATTICE,),
                evaluated_legs=(),
            ))

    def test_reserved_tier_checker_trips(self):
        with self.assertRaises(AssertionError):
            check_reserved_ceiling_tiers_unused(
                self._verdict(tier=PROOF_TIER_CEILING_ONLY))
        with self.assertRaises(AssertionError):
            check_reserved_ceiling_tiers_unused(self._verdict(tier=7))

    def test_audit_only_checker_trips_on_an_aac_accusation(self):
        with self.assertRaises(AssertionError):
            check_audit_only_codec_has_no_spectral_finding(
                self._verdict(spectral_accusation_admissible=True), "aac")
        with self.assertRaises(AssertionError):
            check_audit_only_codec_has_no_spectral_finding(
                self._verdict(
                    tier=PROOF_TIER_DETECTED,
                    fired_legs=(PROOF_LEG_IN_WINDOW_CLIFF,),
                    evaluated_legs=(PROOF_LEG_IN_WINDOW_CLIFF,),
                ),
                "opus",
            )

    def test_surface_agreement_checker_trips(self):
        with self.assertRaises(AssertionError):
            check_surfaces_agree(
                self._verdict(), self._verdict(tier=PROOF_TIER_DETECTED))

    def test_claim_checker_trips_on_widened_copy(self):
        with self.assertRaises(AssertionError):
            check_statement_does_not_widen_the_claim(
                "Verified lossless: guaranteed bit-perfect")

    def test_clearance_checker_trips_on_an_untested_album(self):
        with self.assertRaises(AssertionError):
            check_untested_album_is_not_a_clearance(
                self._verdict(evaluated_legs=()),
                "No evidence of lossy origin from the tests that ran")


if __name__ == "__main__":
    unittest.main()
