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
produce.** Three derivations must agree on every world:
``proof_verdict_from_evidence`` (whole row, what ``pipeline-cli quality``
runs), ``proof_verdict_from_facts`` (flat columns), and — the one that
actually ships to the browser — ``web.classify.proof_gate_projection``
driven over the SQL row aliases the render path really receives. The
adapter is included deliberately: reading a lineage-gated overlay key
instead of its own evidence alias made the two surfaces disagree on 26,503
live rows while both library functions stayed in lockstep, and a property
that stops at the library cannot see it.

**V5 — no statement widens the claim.** No tier sentence, at any tier,
ever asserts bit-faithfulness, a guarantee, or posterior odds (Phase 5
plan §1.7 and §1's base-rate caveat).

**V6 — a verdict with no evaluated leg never reads as a clearance.**
"nothing was found" and "nothing was looked for" are different facts;
``has_finding`` separates them and the statement says so.

**V7 — every surface's audit-only flags come from the one rule.** Six
operator surfaces now render a spectral grade, reaching the rule through
three different input adapters: a whole ``AlbumQualityEvidence``
(``evidence_accusation_flags``, the request-detail header), a joined
column block under either alias prefix
(``evidence_column_accusation_flags``, Wrong Matches and the long-tail
console), and the proof-gate verdict (``proof_gate_projection``, Recents).
Over any world all four answers are the same pair, so no two surfaces can
state different facts about one album — and the withheld reason is never
``audit_only_codec`` when no codec was resolved, which is the fabrication
Rule C exists to stop.

Checkers take their production dependency as a keyword-only argument
defaulting to the real function, so the known-bad self-tests can pass a
decoy explicitly while production always gets the default.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import contextlib
import datetime
import io
import os
import re
import sys
import unittest
from collections.abc import Callable, Sequence
from dataclasses import replace

import msgspec.structs
from hypothesis import example, given
from hypothesis import strategies as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.pipeline_db._shared import (
    CANDIDATE_EVIDENCE_PREFIX,
    CURRENT_EVIDENCE_PREFIX,
)
from lib.pipeline_db.download_log import _DownloadLogMixin
from lib.quality import (
    EVIDENCE_PROVENANCE_CARRIED,
    EVIDENCE_PROVENANCE_MEASURED,
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
    VerifiedLosslessProof,
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
from scripts.pipeline_cli.quality import _print_proof_gate_verdict
from web.classify import (
    ACCUSATION_WITHHELD_AUDIT_ONLY_CODEC,
    ACCUSATION_WITHHELD_CODEC_UNRESOLVED,
    AccusationFlags,
    ProofGateProjection,
    evidence_accusation_flags,
    evidence_column_accusation_flags,
    proof_gate_projection,
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


def _row_aliases_from_facts(facts: dict[str, object]) -> dict[str, object]:
    """The same world as the SQL row the Recents render path receives.

    Deliberately includes the LINEAGE-GATED ``source_format`` key set to
    None, which is what the overlay leaves on 26,503 of 30,467 live rows.
    An adapter that reads it instead of ``_evidence_format`` resolves a
    different codec from the same evidence and the property fails.
    """
    labels: Sequence[str] = facts["container_labels"]  # pyright: ignore[reportAssignmentType]
    lattice = facts["aac_lattice"]
    return {
        "candidate_evidence_id": 1,
        "source_format": None,
        "spectral_grade": facts["spectral_grade"],
        "spectral_bitrate": facts["spectral_bitrate_kbps"],
        "_evidence_format": facts["format"],
        "_evidence_cliff_hz": facts["cliff_hz"],
        "_evidence_codec_family": facts["codec_family"],
        "_evidence_storage_format": facts["storage_format"],
        "_evidence_filetype_band": facts["filetype_band"],
        "_evidence_spectral_subject": facts["spectral_subject"],
        "_evidence_was_converted_from": facts["was_converted_from"],
        "_evidence_container_extensions": list(labels),
        "_evidence_ultrasonic_deficit_db": facts["ultrasonic_deficit_db"],
        "_evidence_spectral_measurement_version": (
            facts["spectral_measurement_version"]),
        "_evidence_aac_lattice_modal_count": (
            lattice.modal_count if isinstance(lattice, AacLatticeCapture)
            else None
        ),
        "_evidence_aac_lattice_scored_tracks": (
            lattice.scored_tracks if isinstance(lattice, AacLatticeCapture)
            else None
        ),
        "_evidence_aac_lattice_max_z": (
            lattice.max_z if isinstance(lattice, AacLatticeCapture) else None
        ),
        "_evidence_verified_lossless_classifier": None,
    }


def _accusation_column_row(
    facts: dict[str, object], prefix: str,
) -> dict[str, object]:
    """The nine accusation aliases a join projects under ``prefix``.

    Mirrors ``lib/pipeline_db/_shared.py::accusation_evidence_columns``,
    which is what the four production queries actually emit.
    """
    return {
        f"{prefix}format": facts["format"],
        f"{prefix}spectral_grade": facts["spectral_grade"],
        f"{prefix}spectral_bitrate": facts["spectral_bitrate_kbps"],
        f"{prefix}spectral_subject": facts["spectral_subject"],
        f"{prefix}was_converted_from": facts["was_converted_from"],
        f"{prefix}cliff_hz": facts["cliff_hz"],
        f"{prefix}codec_family": facts["codec_family"],
        f"{prefix}storage_format": facts["storage_format"],
        f"{prefix}filetype_band": facts["filetype_band"],
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


def check_projection_matches_verdict(
    projection: "ProofGateProjection",
    verdict: AlbumProofVerdict,
    *,
    project: "Callable[[AlbumProofVerdict], tuple[object, ...]] | None" = None,
) -> None:
    """V4, at the adapter: what the browser gets IS the derived verdict.

    The projection nulls the tier and statement when no leg adjudicated —
    that is the ``has_finding`` rule, not a disagreement — so the check is
    against the verdict's own reduction, never against the raw fields.
    """
    def _expected(source: AlbumProofVerdict) -> tuple[object, ...]:
        return (
            source.tier if source.has_finding else None,
            proof_tier_statement(source) if source.has_finding else None,
            list(source.fired_legs),
        )

    reduce = project if project is not None else _expected
    actual = (
        projection.verdict_tier,
        projection.verdict_tier_statement,
        projection.verdict_fired_legs,
    )
    if actual != reduce(verdict):
        raise AssertionError(
            "the render adapter projected a different verdict than the "
            f"derivation produced: {actual} vs {reduce(verdict)}")


_CLAIM_WIDENING_TOKENS = (
    "bit-perfect", "bit perfect", "bit-faithful", "bit faithful",
    "guarantee", "guaranteed", "certain", "probably fake", "definitely",
    "proven lossless", "authentic",
)


def check_accusation_flags_agree(
    flags: "Sequence[tuple[str, AccusationFlags]]",
) -> None:
    """V7: every adapter into the audit-only rule answers identically.

    ``flags`` is ``(surface-name, pair)``. Disagreement here is exactly
    the failure mode issue #829 PR4 shipped against: one surface calling
    an album a transcode while another says its codec cannot support the
    accusation.
    """
    if not flags:
        raise AssertionError("no surfaces supplied to compare")
    first_name, first = flags[0]
    for name, pair in flags[1:]:
        if pair != first:
            raise AssertionError(
                f"{name} and {first_name} disagree about one album: "
                f"{pair} vs {first}")


def check_withheld_reason_matches_the_world(
    pair: "AccusationFlags", family: "str | None",
) -> None:
    """V7: only one of the two withholding worlds may be described.

    ``audit_only_codec`` is a claim ABOUT a resolved codec, so it may
    never be the reason on a row where none resolved; ``codec_unresolved``
    is the converse. A reason at all requires the accusation to have been
    withheld from an accusing grade.
    """
    if pair.withheld is None:
        return
    if pair.admissible is not False:
        raise AssertionError(
            f"withheld reason {pair.withheld!r} on an admissible grade "
            f"({pair.admissible!r})")
    if family is None and pair.withheld != ACCUSATION_WITHHELD_CODEC_UNRESOLVED:
        raise AssertionError(
            f"reason {pair.withheld!r} describes a codec, but none resolved")
    if family is not None and (
        pair.withheld != ACCUSATION_WITHHELD_AUDIT_ONLY_CODEC
    ):
        raise AssertionError(
            f"reason {pair.withheld!r} on a resolved {family!r} album")


def check_statement_does_not_widen_the_claim(statement: str) -> None:
    """V5: no tier sentence may claim more than the tests support."""
    lowered = statement.lower()
    for token in _CLAIM_WIDENING_TOKENS:
        if token in lowered:
            raise AssertionError(
                f"tier statement {statement!r} widens the claim via {token!r}")


def _proof_evidence(
    *, lineage: int, provenance: str, classifier: str | None,
) -> AlbumQualityEvidence:
    """One evidence row on the proof-attribution axis only.

    The spectral world is deliberately blank: attribution answers "whose
    bytes were tested", which is orthogonal to what the tests found.
    """
    return msgspec.structs.replace(
        _evidence_from_facts({
            "format": "FLAC",
            "spectral_grade": None,
            "spectral_bitrate_kbps": None,
            "spectral_subject": None,
            "was_converted_from": None,
            "cliff_hz": None,
            "codec_family": "lossless",
            "storage_format": "FLAC",
            "filetype_band": "flac",
            "container_labels": [".flac"],
            "ultrasonic_deficit_db": None,
            "spectral_measurement_version": 2,
            "aac_lattice": None,
        }),
        lineage_version=lineage,
        verified_lossless_proof=(
            None if classifier is None else VerifiedLosslessProof(
                provenance=provenance,  # pyright: ignore[reportArgumentType]
                source="flac",
                classifier=classifier,
            )
        ),
    )


def cli_states_a_proof(
    evidence: AlbumQualityEvidence,
    *,
    printer: "Callable[..., None]" = _print_proof_gate_verdict,
) -> bool:
    """Whether ``pipeline-cli quality`` prints a "proved by" line.

    Drives the real printer and reads its real stdout, so the answer is the
    operator's, not a re-derivation of it. The printer is injected so the
    known-bad self-test can hand in the ungated decoy.
    """
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        printer("IN", evidence)
    return "proved by" in captured.getvalue()


def recents_states_a_proof(evidence: AlbumQualityEvidence) -> bool:
    """Whether the Recents card renders a proof generation for the same row.

    Goes through the production read seam
    (``_overlay_evidence_onto_download_log_row``) exactly as ``get_log``
    does, because that seam is where Recents' half of the attribution rule
    is applied.
    """
    proof = evidence.verified_lossless_proof
    row: dict[str, object] = {
        "candidate_evidence_id": 1,
        "_request_mb_release_id": evidence.mb_release_id,
        "_evidence_mb_release_id": evidence.mb_release_id,
        "_evidence_lineage_version": evidence.lineage_version,
        "_evidence_verified_lossless_classifier": (
            proof.classifier if proof is not None else None),
    }
    overlaid = _DownloadLogMixin._overlay_evidence_onto_download_log_row(row)
    projection = proof_gate_projection(overlaid)
    return projection.verified_lossless_generation is not None


def check_proof_attribution_agrees(
    cli_says: bool,
    recents_says: bool,
    *,
    attributable: bool,
) -> None:
    """V8: both operator surfaces state the SAME proof for one album.

    ``pipeline-cli quality`` and the Recents card each report
    ``album_quality_evidence``'s minted proof, and each read it off a
    different shape — a whole evidence row, a joined column block behind
    the read seam. Both were ungated: Recents put "MP3 320, verified
    lossless" on a never-converted MP3 wearing its FLAC sibling's proof,
    and the CLI attributed a cross-walked sibling's proof on 4,910 live
    requests. One predicate now answers for both, so a divergence here is
    a second spelling of the rule.
    """
    if cli_says != recents_says:
        raise AssertionError(
            f"pipeline-cli says proved={cli_says} while Recents says "
            f"proved={recents_says} for the same album"
        )
    if cli_says != attributable:
        raise AssertionError(
            f"surfaces say proved={cli_says} for an album whose proof is "
            f"attributable={attributable}"
        )


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
    @example(world=(
        # The tier-4 band: the ultrasonic leg DENIES and nothing else
        # fires. Pinned rather than left to the strategy because
        # ``derandomize=True`` seeds from the test function's digest, so
        # editing this body reshuffles the whole example sequence — the
        # arm ran 4 times in 150 examples before issue #1094's audit
        # touched the body and 0 times after, silently taking the
        # ladder's NO_ULTRASONIC branch out of the deterministic tier.
        interpret_spectral_cliff("lossless", spectral_grade="genuine"),
        "genuine",
        ultrasonic_proof_leg(
            deficit_db=61.0,
            spectral_measurement_version=2,
            decode_path=SPECTRAL_DECODE_PATH_SOX_NATIVE,
            preserved_source_spectral=False,
        ),
        None,
    ))
    @example(world=(
        # A DENYING lattice leg, pinned for the same reason: it is the
        # only world in which a leg can fire, and the fired-legs-subset
        # clause is the only guard that sees it.
        interpret_spectral_cliff("aac", spectral_grade="genuine"),
        "genuine",
        None,
        aac_lattice_proof_leg(AacLatticeCapture(
            modal_count=4, scored_tracks=6, max_z=9.0)),
    ))
    @given(world=_verdict_worlds())
    def test_tier_and_legs_hold(self, world) -> None:
        spectral, grade, ultra, lattice = world
        verdict = album_proof_verdict(
            spectral=spectral, spectral_grade=grade,
            ultrasonic_leg=ultra, aac_lattice_leg=lattice)
        # The reserved-tier guard runs FIRST deliberately (issue #1094's
        # per-clause audit). ``check_tier_follows_fired_legs`` re-derives
        # the ladder, and that re-derivation only ever expects a
        # PRODUCIBLE tier — so run before it, no world that emits tier 2,
        # 3 or an unknown number can ever reach the reserved-tier clauses.
        # Both orders kill the same mutants; this one attributes each kill
        # to the clause that actually legislates it.
        check_reserved_ceiling_tiers_unused(verdict)
        check_tier_follows_fired_legs(verdict)
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
    @example(facts={
        # The ONLY tier-4 world these columns see. ``_evidence_facts``
        # must clear four independent gates at once for the ultrasonic
        # leg to DENY through ``album_ultrasonic_proof_leg`` — v2+
        # measurement, a non-NULL deficit at or above 59.5 dB, an
        # all-sox-native container set, and no cliff or lattice firing —
        # and in the deterministic ``suite`` tier it never did: the leg
        # adjudicated 6 times in 150 examples and denied 0 of them, so
        # the NO_ULTRASONIC band of the ladder ran through the flat
        # columns and the render adapter exactly never (issue #1094 Q3).
        "spectral_grade": "genuine",
        "spectral_bitrate_kbps": None,
        "cliff_hz": None,
        "codec_family": "lossless",
        "format": "FLAC",
        "storage_format": "FLAC",
        "filetype_band": "flac",
        "spectral_subject": "installed",
        "was_converted_from": None,
        "container_labels": [".flac"],
        "ultrasonic_deficit_db": 61.0,
        "spectral_measurement_version": 2,
        "aac_lattice": None,
    })
    @given(facts=_evidence_facts())
    def test_render_path_and_cli_agree(self, facts) -> None:
        """V4 over any world: one album, one verdict, every surface."""
        facts_verdict = proof_verdict_from_facts(**facts)
        row_verdict = proof_verdict_from_evidence(_evidence_from_facts(facts))
        check_surfaces_agree(row_verdict, facts_verdict)
        # The adapter the browser actually gets, over the SQL row aliases
        # the render path really receives.
        check_projection_matches_verdict(
            proof_gate_projection(_row_aliases_from_facts(facts)),
            facts_verdict,
        )
        # Reserved-tier guard first, for the reason given in
        # ``test_tier_and_legs_hold``.
        check_reserved_ceiling_tiers_unused(facts_verdict)
        check_tier_follows_fired_legs(facts_verdict)
        statement = proof_tier_statement(facts_verdict)
        check_statement_does_not_widen_the_claim(statement)
        check_untested_album_is_not_a_clearance(facts_verdict, statement)

    @example(facts={
        # The audit-only HAVE world the four newly-carried surfaces exist
        # for: a 256 kbps CBR AAC the codec-blind analyzer graded
        # ``likely_transcode`` with a LAME-table 128 bucket (download
        # 37946, issue #829's opening defect).
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
    @example(facts={
        # Nothing resolved a codec: the reason must be the unresolved one,
        # never a claim about an encoder that was never identified.
        "spectral_grade": "suspect",
        "spectral_bitrate_kbps": 192,
        "cliff_hz": 18000,
        "codec_family": None,
        "format": None,
        "storage_format": None,
        "filetype_band": "",
        "spectral_subject": "installed",
        "was_converted_from": None,
        "container_labels": [],
        "ultrasonic_deficit_db": None,
        "spectral_measurement_version": 2,
        "aac_lattice": None,
    })
    @given(facts=_evidence_facts())
    def test_every_surface_derives_the_same_audit_only_flags(
        self, facts,
    ) -> None:
        """V7 over any world: one album, one audit-only pair, six surfaces."""
        verdict = proof_verdict_from_facts(**facts)
        projection = proof_gate_projection(_row_aliases_from_facts(facts))
        pairs: list[tuple[str, AccusationFlags]] = [
            ("Recents (proof_gate_projection)", AccusationFlags(
                admissible=projection.spectral_accusation_admissible,
                withheld=projection.spectral_accusation_withheld,
            )),
            ("request detail (whole evidence row)",
             evidence_accusation_flags(_evidence_from_facts(facts))),
            ("Wrong Matches candidate chip (candidate aliases)",
             evidence_column_accusation_flags(
                 _accusation_column_row(facts, CANDIDATE_EVIDENCE_PREFIX),
                 prefix=CANDIDATE_EVIDENCE_PREFIX)),
            ("long-tail console chip (current aliases)",
             evidence_column_accusation_flags(
                 _accusation_column_row(facts, CURRENT_EVIDENCE_PREFIX),
                 prefix=CURRENT_EVIDENCE_PREFIX)),
        ]
        check_accusation_flags_agree(pairs)
        check_withheld_reason_matches_the_world(
            pairs[0][1], verdict.codec_family)

    @given(facts=_evidence_facts())
    def test_an_unjoined_row_keeps_the_accusing_render(self, facts) -> None:
        """V7's fail-accusing half: no evidence join, no flags, ever.

        A LEFT JOIN that matched nothing hands the adapter all-NULL
        columns. Both flags must come back empty so every surface falls
        back to its historical accusing render — the safe direction for a
        display-only fact, and the reason the flags are tri-state rather
        than boolean.
        """
        for prefix in (CANDIDATE_EVIDENCE_PREFIX, CURRENT_EVIDENCE_PREFIX):
            unjoined = dict.fromkeys(
                _accusation_column_row(facts, prefix), None)
            self.assertEqual(
                evidence_column_accusation_flags(unjoined, prefix=prefix),
                AccusationFlags(),
            )
        self.assertEqual(evidence_accusation_flags(None), AccusationFlags())

    @given(
        lineage=st.sampled_from((1, 3, 4)),
        provenance=st.sampled_from(
            (EVIDENCE_PROVENANCE_MEASURED, EVIDENCE_PROVENANCE_CARRIED)),
        classifier=st.sampled_from(
            (None, "spectral_verified_lossless",
             "spectral_verified_lossless_v3", "spectral_verified_lossless_v4")),
    )
    @example(  # the live shape: a cross-walked v1 row, 4,910 CLI requests
        lineage=1,
        provenance=EVIDENCE_PROVENANCE_MEASURED,
        classifier="spectral_verified_lossless",
    )
    @example(  # the must-still-work twin
        lineage=4,
        provenance=EVIDENCE_PROVENANCE_MEASURED,
        classifier="spectral_verified_lossless_v4",
    )
    @example(  # a library row holding its OWN album's carried proof
        lineage=4,
        provenance=EVIDENCE_PROVENANCE_CARRIED,
        classifier="spectral_verified_lossless_v4",
    )
    def test_both_operator_surfaces_attribute_the_same_proof(
        self, lineage: int, provenance: str, classifier: str | None,
    ) -> None:
        """V8 over any world: one rule, two surfaces, one answer per album.

        The expectation restates the invariant rather than calling the
        production predicate: a property that asks the implementation what
        the answer should be can only patrol agreement, and would pass with
        the rule deleted. Provenance is generated deliberately and is NOT in
        the expectation — a carried proof is the album's own, propagated to
        its library row, and both surfaces must keep stating it.
        """
        evidence = _proof_evidence(
            lineage=lineage, provenance=provenance, classifier=classifier)
        check_proof_attribution_agrees(
            cli_states_a_proof(evidence),
            recents_states_a_proof(evidence),
            attributable=classifier is not None and lineage in (3, 4),
        )


def _exactly(message: str) -> str:
    """A pattern matching one clause's whole message and nothing else."""
    return f"^{re.escape(message)}$"


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Every CLAUSE owes a planted violation naming its own message.

    Per-clause proof, issue #1094 (``docs/generated-testing.md`` § "Per-clause
    proof"). Each world below makes exactly ONE clause's condition true while
    every earlier clause in the same checker passes, and asserts that clause's
    own message — a bare ``assertRaises(AssertionError)`` over a world that
    violates several clauses only ever exercises the first, because these are
    short-circuiting ``raise`` chains.
    """

    def _verdict(self, **overrides) -> AlbumProofVerdict:
        base = AlbumProofVerdict(
            tier=PROOF_TIER_NO_FINDING,
            fired_legs=(),
            evaluated_legs=(PROOF_LEG_IN_WINDOW_CLIFF,),
            spectral_accusation_admissible=False,
        )
        return replace(base, **overrides)

    def test_tier_checker_clauses_each_trip(self):
        """V1's two clauses: the ladder, then the fired-subset rule."""
        cases = (
            (
                "ladder mismatch against the injected resolver",
                self._verdict(
                    fired_legs=(PROOF_LEG_NO_ULTRASONIC,),
                    evaluated_legs=(PROOF_LEG_NO_ULTRASONIC,),
                ),
                {"tier_of": lambda _fired: PROOF_TIER_DETECTED},
                ("tier 5 does not follow fired legs ('no_ultrasonic',) "
                 "(expected 1)"),
            ),
            (
                # The world mutant M1 reaches through production: collapse
                # the ladder's NO_ULTRASONIC branch into NO_FINDING.
                "ladder mismatch against the real severity bands",
                self._verdict(
                    fired_legs=(PROOF_LEG_NO_ULTRASONIC,),
                    evaluated_legs=(PROOF_LEG_NO_ULTRASONIC,),
                ),
                {},
                ("tier 5 does not follow fired legs ('no_ultrasonic',) "
                 "(expected 4)"),
            ),
            (
                # Ladder-consistent, so the first clause passes and the
                # subset clause is the one under test.
                "a leg fired without ever being evaluated",
                self._verdict(
                    tier=PROOF_TIER_DETECTED,
                    fired_legs=(PROOF_LEG_AAC_LATTICE,),
                    evaluated_legs=(),
                ),
                {},
                ("fired legs ('aac_lattice',) are not a subset of evaluated "
                 "legs ()"),
            ),
        )
        for desc, verdict, kwargs, message in cases:
            with self.subTest(desc=desc), self.assertRaisesRegex(
                AssertionError, _exactly(message)
            ):
                check_tier_follows_fired_legs(verdict, **kwargs)

    def test_reserved_tier_checker_clauses_each_trip(self):
        """V2's two clauses: the reserved band, then any unknown number."""
        reserved = (
            "is reserved for the ceiling leg production does not measure, "
            "and has no operator copy"
        )
        cases = (
            ("ceiling + no-ultrasonic", PROOF_TIER_CEILING_AND_NO_ULTRASONIC,
             f"tier 2 {reserved}"),
            ("ceiling only", PROOF_TIER_CEILING_ONLY, f"tier 3 {reserved}"),
            # 7 is outside the reserved pair, so the first clause passes
            # and the producible-set clause is the one under test.
            ("a tier number no ladder produces", 7,
             "tier 7 is not a producible tier"),
        )
        for desc, tier, message in cases:
            with self.subTest(desc=desc), self.assertRaisesRegex(
                AssertionError, _exactly(message)
            ):
                check_reserved_ceiling_tiers_unused(self._verdict(tier=tier))

    def test_audit_only_checker_clauses_each_trip(self):
        """V3's three clauses: admissible, fired, then merely evaluated."""
        cases = (
            (
                "an audit-only family admitting an accusation",
                self._verdict(spectral_accusation_admissible=True),
                "aac",
                "codec family 'aac' must never admit a transcode accusation",
            ),
            (
                "an unresolved family admitting an accusation",
                self._verdict(spectral_accusation_admissible=True),
                None,
                "codec family None must never admit a transcode accusation",
            ),
            (
                # Inadmissible, so the first clause passes; the cliff leg
                # is in ``fired`` anyway, which is the second.
                "the cliff leg fired on an audit-only family",
                self._verdict(
                    tier=PROOF_TIER_DETECTED,
                    fired_legs=(PROOF_LEG_IN_WINDOW_CLIFF,),
                    evaluated_legs=(PROOF_LEG_IN_WINDOW_CLIFF,),
                ),
                "opus",
                "codec family 'opus' fired the in-window cliff leg",
            ),
            (
                # Inadmissible AND unfired, so both earlier clauses pass.
                # This is the fail-open mutant M7 reaches through
                # production: an album the cliff leg never ran on reads as
                # one it ran on and cleared.
                "the cliff leg counted as evaluated on an audit-only family",
                self._verdict(fired_legs=()),
                "opus",
                ("codec family 'opus' counted the cliff leg as evaluated "
                 "— an untested album must never read as a cleared one"),
            ),
        )
        for desc, verdict, family, message in cases:
            with self.subTest(desc=desc), self.assertRaisesRegex(
                AssertionError, _exactly(message)
            ):
                check_audit_only_codec_has_no_spectral_finding(verdict, family)

    def test_surface_agreement_checker_trips(self):
        with self.assertRaisesRegex(
            AssertionError,
            "^pipeline-cli and the render path disagree about one album: "
            r"AlbumProofVerdict\(tier=5",
        ):
            check_surfaces_agree(
                self._verdict(), self._verdict(tier=PROOF_TIER_DETECTED))

    def test_accusation_agreement_checker_clauses_each_trip(self):
        """V7's two clauses: no surfaces at all, then a split pair."""
        with self.subTest(desc="no surfaces supplied"), self.assertRaisesRegex(
            AssertionError, _exactly("no surfaces supplied to compare")
        ):
            check_accusation_flags_agree([])
        with self.subTest(desc="a split pair"), self.assertRaisesRegex(
            AssertionError,
            _exactly(
                "long-tail console and Recents disagree about one album: "
                "AccusationFlags(admissible=True, withheld=None) vs "
                "AccusationFlags(admissible=False, "
                "withheld='audit_only_codec')"
            ),
        ):
            check_accusation_flags_agree([
                ("Recents", AccusationFlags(
                    admissible=False,
                    withheld=ACCUSATION_WITHHELD_AUDIT_ONLY_CODEC)),
                ("long-tail console", AccusationFlags(admissible=True)),
            ])

    def test_withheld_reason_checker_clauses_each_trip(self):
        """V7's three clauses, in the order the checker evaluates them."""
        cases = (
            (
                "a reason withheld from a grade that was admitted",
                AccusationFlags(
                    admissible=True,
                    withheld=ACCUSATION_WITHHELD_AUDIT_ONLY_CODEC),
                "aac",
                ("withheld reason 'audit_only_codec' on an admissible "
                 "grade (True)"),
            ),
            (
                # ``None`` is not ``False``: a reason on a row with no
                # accusation to withhold is the same clause.
                "a reason on a grade that could not accuse at all",
                AccusationFlags(
                    withheld=ACCUSATION_WITHHELD_CODEC_UNRESOLVED),
                None,
                ("withheld reason 'codec_unresolved' on an admissible "
                 "grade (None)"),
            ),
            (
                # The Rule C failure this checker exists for: describing
                # native encoder rolloff on a row where no encoder was
                # ever resolved.
                "a codec claim on a row where no codec resolved",
                AccusationFlags(
                    admissible=False,
                    withheld=ACCUSATION_WITHHELD_AUDIT_ONLY_CODEC),
                None,
                ("reason 'audit_only_codec' describes a codec, but none "
                 "resolved"),
            ),
            (
                "the unresolved reason on a row whose codec resolved",
                AccusationFlags(
                    admissible=False,
                    withheld=ACCUSATION_WITHHELD_CODEC_UNRESOLVED),
                "aac",
                "reason 'codec_unresolved' on a resolved 'aac' album",
            ),
        )
        for desc, pair, family, message in cases:
            with self.subTest(desc=desc), self.assertRaisesRegex(
                AssertionError, _exactly(message)
            ):
                check_withheld_reason_matches_the_world(pair, family)

    def test_attribution_checker_clauses_each_trip(self) -> None:
        """V8's two clauses: the surfaces split, then both agree and lie.

        The first decoy is the shipped line — print the generation whenever
        a proof object exists — so the checker is proved against the real
        divergence rather than a hypothetical one. The second needs no
        decoy at all: both real surfaces answer through the one production
        rule, and the planted violation is an expectation they contradict,
        which is the "agreed, and both wrong" world the split clause
        cannot see.
        """
        from lib.quality import verified_lossless_generation_label

        def ungated(side: str, evidence: AlbumQualityEvidence) -> None:
            proof = evidence.verified_lossless_proof
            if proof is not None:
                print(f"      verified lossless {side}: proved by "
                      f"{verified_lossless_generation_label(proof.classifier)}")

        legacy = _proof_evidence(
            lineage=1,
            provenance=EVIDENCE_PROVENANCE_MEASURED,
            classifier="spectral_verified_lossless",
        )
        self.assertTrue(cli_states_a_proof(legacy, printer=ungated))
        self.assertFalse(recents_states_a_proof(legacy))
        with self.subTest(desc="one surface ungated"), self.assertRaisesRegex(
            AssertionError,
            _exactly("pipeline-cli says proved=True while Recents says "
                     "proved=False for the same album"),
        ):
            check_proof_attribution_agrees(
                cli_states_a_proof(legacy, printer=ungated),
                recents_states_a_proof(legacy),
                attributable=False,
            )

        # Both surfaces agree — the split clause passes — and the world
        # says they are wrong. Mutant M17 reaches this through production
        # by widening the ONE shared rule
        # (``evidence_is_source_semantic``), which moves both surfaces at
        # once and is therefore invisible to the clause above.
        current = _proof_evidence(
            lineage=4,
            provenance=EVIDENCE_PROVENANCE_MEASURED,
            classifier="spectral_verified_lossless_v4",
        )
        agreed = cli_states_a_proof(current)
        self.assertTrue(agreed)
        self.assertEqual(agreed, recents_states_a_proof(current))
        with self.subTest(desc="an unearned proof"), self.assertRaisesRegex(
            AssertionError,
            _exactly("surfaces say proved=True for an album whose proof "
                     "is attributable=False"),
        ):
            check_proof_attribution_agrees(
                agreed, recents_states_a_proof(current), attributable=False,
            )
        withheld = cli_states_a_proof(legacy)
        self.assertFalse(withheld)
        with self.subTest(desc="an earned proof withheld"), self.assertRaisesRegex(
            AssertionError,
            _exactly("surfaces say proved=False for an album whose proof "
                     "is attributable=True"),
        ):
            check_proof_attribution_agrees(
                withheld, recents_states_a_proof(legacy), attributable=True,
            )

    def test_projection_checker_trips_on_a_lineage_gated_read(self):
        """The exact defect: the adapter reading the overlaid format key.

        ``source_format`` is only overlaid for lineage 3/4 rows, so on a
        legacy row it is None while the decider still sees the
        measurement's own format. Re-deriving through it resolves no codec,
        which withholds a finding the CLI reports — a real divergence on
        26,503 live rows. The decoy adapter below reads that key; the
        checker must catch it.
        """
        facts = {
            "spectral_grade": "likely_transcode",
            "spectral_bitrate_kbps": 128,
            "cliff_hz": 15500,
            "codec_family": None,
            "format": "FLAC",
            "storage_format": None,
            "filetype_band": "",
            "spectral_subject": "installed",
            "was_converted_from": None,
            "container_labels": [".flac"],
            "ultrasonic_deficit_db": None,
            "spectral_measurement_version": 2,
            "aac_lattice": None,
        }
        verdict = proof_verdict_from_facts(**facts)
        self.assertEqual(verdict.tier, PROOF_TIER_DETECTED)
        row = _row_aliases_from_facts(facts)
        lineage_gated = dict(row)
        lineage_gated["_evidence_format"] = lineage_gated["source_format"]
        # The expected half comes from the producer, never a copy literal
        # (``.claude/rules/test-fidelity.md`` Rule C).
        expected = (
            verdict.tier, proof_tier_statement(verdict),
            list(verdict.fired_legs),
        )
        with self.assertRaisesRegex(
            AssertionError,
            _exactly(
                "the render adapter projected a different verdict than the "
                f"derivation produced: (None, None, []) vs {expected}"
            ),
        ):
            check_projection_matches_verdict(
                proof_gate_projection(lineage_gated), verdict)
        # …and the real adapter over the real row agrees with the verdict.
        check_projection_matches_verdict(proof_gate_projection(row), verdict)

    def test_projection_checker_trips_on_a_drifted_reduction(self):
        verdict = self._verdict(evaluated_legs=(PROOF_LEG_IN_WINDOW_CLIFF,))
        expected = (
            verdict.tier, proof_tier_statement(verdict),
            list(verdict.fired_legs),
        )
        with self.assertRaisesRegex(
            AssertionError,
            _exactly(
                "the render adapter projected a different verdict than the "
                f"derivation produced: (None, None, []) vs {expected}"
            ),
        ):
            check_projection_matches_verdict(ProofGateProjection(), verdict)

    def test_claim_checker_trips_on_every_widening_token(self):
        """V5's one clause, once per token, so none of them is decoration.

        ``guaranteed`` is deliberately expected to be NAMED as
        ``guarantee``: it is a superstring of an earlier entry, so the
        clause fires on the shorter one first. The entry is redundant
        rather than wrong, and pinning that here stops a future reader
        from "fixing" the list on a guess.
        """
        cases = (
            ("Tier statement: bit-perfect copy", "bit-perfect"),
            ("Tier statement: bit perfect copy", "bit perfect"),
            ("Tier statement: bit-faithful to its source", "bit-faithful"),
            ("Tier statement: bit faithful to its source", "bit faithful"),
            ("Tier statement: we guarantee the source", "guarantee"),
            ("Tier statement: guaranteed clean", "guarantee"),
            ("Tier statement: certain to be a genuine rip", "certain"),
            ("Tier statement: probably fake", "probably fake"),
            ("Tier statement: definitely lossy in origin", "definitely"),
            ("Tier statement: proven lossless", "proven lossless"),
            ("Tier statement: an authentic pressing", "authentic"),
        )
        self.assertEqual(
            {token for _statement, token in cases} | {"guaranteed"},
            set(_CLAIM_WIDENING_TOKENS),
            "every widening token owes a statement that reaches it",
        )
        for statement, token in cases:
            with self.subTest(token=token), self.assertRaisesRegex(
                AssertionError,
                _exactly(f"tier statement {statement!r} widens the claim "
                         f"via {token!r}"),
            ):
                check_statement_does_not_widen_the_claim(statement)

    def test_clearance_checker_trips_on_an_untested_album(self):
        # The clearance sentence comes from the producer, so the pin
        # cannot outlive the copy it claims to reject (Rule C).
        clearance = proof_tier_statement(
            self._verdict(evaluated_legs=(PROOF_LEG_IN_WINDOW_CLIFF,)))
        with self.assertRaisesRegex(
            AssertionError,
            _exactly(f"verdict with no evaluated leg rendered {clearance!r}, "
                     "which reads as a clearance nothing tested for"),
        ):
            check_untested_album_is_not_a_clearance(
                self._verdict(evaluated_legs=()), clearance)


if __name__ == "__main__":
    unittest.main()
