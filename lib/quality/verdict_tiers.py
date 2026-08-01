"""Verdict tiers — what the proof gate FOUND, as one operator statement.

Issue #829 Phase 5 PR4. Pure derivation, never a decision: everything here
reads leg outcomes the decider has already computed
(``lib/quality/decisions.py``) and turns them into the severity band and
the single sentence an operator surface prints. No branch in the pipeline
reads any of it.

The ladder and its numbering
----------------------------
The Phase 5 plan §1 measured five tiers over 100 genuine controls and 300
FLAC-container launders, partitioned by WHICH LEGS FIRED:

    tier  fired legs                genuine  launders
    1     in-window cliff                 0        99
    2     ceiling + no-ultrasonic        18       155
    3     ceiling only                    2         1
    4     no-ultrasonic only             16        45
    5     none                           64         0

**Tiers 2 and 3 have no producer in this codebase and are deliberately
never emitted.** They require the frozen scorer's CEILING leg — a >=15 dB
step at a consistent slice index across an album's tracks — which needs
per-track slice vectors production does not persist and has never
measured. The plan records this as a known residual against the PR3
threshold freeze (``lib/quality/decisions.py``,
``ULTRASONIC_PROOF_DENY_DEFICIT_DB``, "production evaluates only TWO of the
frozen scorer's three legs"). Their numbers stay reserved rather than
renumbered so this module's tiers keep meaning the same thing as the
measured table; inventing copy for a band nothing can produce is the
exact defect ``.claude/rules/test-fidelity.md`` Rule C exists to stop
(issue #882's ``no_candidates``).

Production instead has a leg the research table predates: the AAC
frame-lattice leg (``aac_lattice_proof_leg``). It is a POSITIVE detection
of an AAC encoder's MDCT frame lattice with an analytic false-positive
floor of ~0.0023 albums per 5000 — the same severity as the in-window
cliff, and the only instrument here that sees the Apple/CoreAudio family.
It therefore joins tier 1 rather than taking a new number, and the
persisted fired-leg set (not the tier) says which of the two fired.

What the tiers claim, and what they do not
------------------------------------------
Every tier is a statement about the TESTS, never about the album:

* tier 1 — a test positively detected lossy origin.
* tier 4 — the album carries no ultrasonic content, which is evidence but
  not proof (16 of 100 genuine controls land here).
* tier 5 — nothing the tests could run found anything. It is NOT "this is
  bit-faithful to a lossless source", and it is not by itself a proof:
  WHICH tests actually ran is the separate ``verified_lossless_classifier``
  axis (``verified_lossless_generation_label`` below). The Phase 5 plan
  §1.7 bounds the claim to "no evidence of lossy origin was found by the
  tests we have", and no copy here may widen it.

**Base rates are load-bearing (plan §1).** The corpus was 1 genuine : 3
launders BY CONSTRUCTION; real peer-shared content is 49.2% mp3 / 48.3%
flac / 1.7% AAC / 0.43% opus. A fired leg means "worth a look", never
"probably fake", and a withheld proof never rejects, denylists or accuses
(plan §2).
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from lib.quality.decisions import (
    VERIFIED_LOSSLESS_CLASSIFIER,
    VERIFIED_LOSSLESS_CLASSIFIER_V3,
    VERIFIED_LOSSLESS_CLASSIFIER_V4,
    AacLatticeProofLeg,
    UltrasonicProofLeg,
    aac_lattice_proof_leg,
    album_ultrasonic_proof_leg,
)
from lib.quality.evidence_types import (
    AacLatticeCapture,
    CodecFamily,
    EvidenceSubject,
)
from lib.quality.spectral_interpretation import (
    SpectralEvidenceFacts,
    SpectralInterpretation,
    interpret_spectral_evidence,
)

#: A leg of the proof gate that production actually evaluates.
#:
#: ``in_window_cliff`` is the album spectral GRADE read through its own
#: codec's semantics — production's cliff leg is the grade, which unions
#: the cliff and HF-deficit detectors (``lib/spectral_check.py``), and
#: ``SpectralInterpretation.supports_transcode_accusation`` is the one
#: place that says whether the grade is admissible as a transcode finding
#: for the measured codec at all.
ProofLeg = Literal["in_window_cliff", "no_ultrasonic", "aac_lattice"]

PROOF_LEG_IN_WINDOW_CLIFF: ProofLeg = "in_window_cliff"
PROOF_LEG_NO_ULTRASONIC: ProofLeg = "no_ultrasonic"
PROOF_LEG_AAC_LATTICE: ProofLeg = "aac_lattice"

#: A positive detection of lossy origin: the in-window cliff leg, the AAC
#: frame-lattice leg, or both. 0/100 genuine controls on four arms for the
#: cliff; an analytic ~0.0023 per 5000 for the lattice.
PROOF_TIER_DETECTED = 1

#: Reserved for the frozen scorer's ceiling leg, which production does not
#: measure. Never emitted — see the module docstring.
PROOF_TIER_CEILING_AND_NO_ULTRASONIC = 2
PROOF_TIER_CEILING_ONLY = 3

#: The album's ultrasonic band is empty and no positive-detection leg
#: fired. Evidence, not proof: 16 of 100 genuine controls land here.
PROOF_TIER_NO_ULTRASONIC = 4

#: No leg fired. What that is worth depends entirely on which legs could
#: run, which is the classifier axis, not this one.
PROOF_TIER_NO_FINDING = 5

#: Spectral semantics whose family admits a transcode finding at all, so a
#: graded album on one of them has actually been TESTED by the cliff leg
#: (whether or not the leg fired). Derived from the two
#: ``SpectralInterpretation`` branches that set
#: ``supports_transcode_accusation`` from the grade — AAC's content floor,
#: Opus's audit-only and every unresolved family are hard-False there, so
#: on them the cliff leg never runs, which is exactly issue #829's fix.
_CLIFF_ADMISSIBLE_SEMANTICS = frozenset({"ladder", "lossless_authenticity"})

#: Tier numbers this module can actually produce, in severity order.
PRODUCIBLE_PROOF_TIERS: tuple[int, ...] = (
    PROOF_TIER_DETECTED,
    PROOF_TIER_NO_ULTRASONIC,
    PROOF_TIER_NO_FINDING,
)


@dataclass(frozen=True)
class AlbumProofVerdict:
    """Which proof-gate legs fired for one album, and the tier that implies.

    ``fired_legs`` is the durable fact and ``tier`` is a pure function of
    it; the tier is carried alongside because it is the severity band every
    surface sorts and colours by, and re-deriving it per surface is how two
    surfaces start disagreeing.

    ``spectral_accusation_admissible`` is NOT a leg outcome — it is why the
    cliff leg did or did not fire, and operator surfaces need it separately:
    an AAC or Opus album graded ``likely_transcode`` by the codec-blind
    analyzer must stop being rendered as a transcode accusation
    (issue #829's opening defect, download 37946), while still showing the
    measured grade as the audit fact it is.
    """

    tier: int
    fired_legs: tuple[ProofLeg, ...]
    evaluated_legs: tuple[ProofLeg, ...]
    spectral_accusation_admissible: bool
    ultrasonic_outcome: str | None = None
    aac_lattice_outcome: str | None = None

    @property
    def detected_lossy_origin(self) -> bool:
        """Whether a positive-detection leg fired (tier 1)."""
        return self.tier == PROOF_TIER_DETECTED

    @property
    def has_finding(self) -> bool:
        """Whether ANY leg adjudicated — i.e. whether the tier means anything.

        Tier 5 over an empty ``evaluated_legs`` is the shape a surface must
        NOT render as a clearance: no test ran, so "nothing was found" is a
        statement about this pipeline, not about the album. Most legacy
        rows are exactly that world.
        """
        return bool(self.evaluated_legs)


def album_proof_verdict(
    *,
    spectral: SpectralInterpretation,
    spectral_grade: str | None = None,
    ultrasonic_leg: UltrasonicProofLeg | None = None,
    aac_lattice_leg: AacLatticeProofLeg | None = None,
) -> AlbumProofVerdict:
    """Reduce one album's proof-gate legs to a fired-leg set and a tier.

    Pure, and deliberately total: every input combination yields a tier.
    A ``None`` leg is exactly a ``withheld`` one — it asserts nothing, and
    the caller that has no such evidence gets the same answer as the caller
    whose evidence could not adjudicate. That equality is the point: most
    of the library will never have an ultrasonic or lattice measurement at
    any price (Phase 5 plan §1.5a), and a surface must not read their
    absence as either a finding or a clearance.

    The cliff leg fires on ``supports_transcode_accusation``, never on the
    raw grade. That single flag is where issue #829's fix lives: it is
    hard-False for AAC, Opus, HE-AAC, ``other`` and unknown families no
    matter what the codec-blind analyzer graded, and it tracks the grade
    for the families whose spectral evidence is admissible (mp3, vorbis,
    lossless).

    ``spectral_grade`` is taken separately from the interpretation that was
    built from it, and only to answer one question the interpretation
    cannot: did the cliff leg RUN? "Admissible family, graded genuine" and
    "never measured" both leave ``supports_transcode_accusation`` False,
    and a surface that cannot tell them apart renders "nothing was found"
    over an album nothing was tested on — the tier-5-without-evidence lie
    ``has_finding`` exists to stop.
    """
    fired: list[ProofLeg] = []
    evaluated: list[ProofLeg] = []
    if spectral.semantics in _CLIFF_ADMISSIBLE_SEMANTICS and (
        spectral_grade not in (None, "", "error")
    ):
        evaluated.append(PROOF_LEG_IN_WINDOW_CLIFF)
    if spectral.supports_transcode_accusation:
        fired.append(PROOF_LEG_IN_WINDOW_CLIFF)
    if aac_lattice_leg is not None and aac_lattice_leg.outcome != "withheld":
        evaluated.append(PROOF_LEG_AAC_LATTICE)
    if aac_lattice_leg is not None and aac_lattice_leg.denies_promotion:
        fired.append(PROOF_LEG_AAC_LATTICE)
    if ultrasonic_leg is not None and ultrasonic_leg.outcome != "withheld":
        evaluated.append(PROOF_LEG_NO_ULTRASONIC)
    if ultrasonic_leg is not None and ultrasonic_leg.denies_promotion:
        fired.append(PROOF_LEG_NO_ULTRASONIC)

    if (
        PROOF_LEG_IN_WINDOW_CLIFF in fired
        or PROOF_LEG_AAC_LATTICE in fired
    ):
        tier = PROOF_TIER_DETECTED
    elif PROOF_LEG_NO_ULTRASONIC in fired:
        tier = PROOF_TIER_NO_ULTRASONIC
    else:
        tier = PROOF_TIER_NO_FINDING
    return AlbumProofVerdict(
        tier=tier,
        fired_legs=tuple(fired),
        evaluated_legs=tuple(evaluated),
        spectral_accusation_admissible=(
            spectral.supports_transcode_accusation
        ),
        ultrasonic_outcome=(
            ultrasonic_leg.outcome if ultrasonic_leg is not None else None
        ),
        aac_lattice_outcome=(
            aac_lattice_leg.outcome if aac_lattice_leg is not None else None
        ),
    )


def proof_verdict_from_facts(
    *,
    spectral_grade: str | None,
    spectral_bitrate_kbps: int | None,
    cliff_hz: int | None,
    codec_family: CodecFamily | None,
    format: str | None,
    storage_format: str | None,
    filetype_band: str,
    spectral_subject: EvidenceSubject | None,
    was_converted_from: str | None,
    container_labels: Sequence[str],
    ultrasonic_deficit_db: float | None,
    spectral_measurement_version: int | None,
    aac_lattice: AacLatticeCapture | None,
) -> AlbumProofVerdict:
    """Build one album's verdict from the persisted evidence columns.

    THE single derivation, and the reason there is one: the operator
    surfaces do not hold an ``AlbumQualityEvidence``. ``pipeline-cli
    quality`` loads whole rows, the Recents render path joins a projection
    of the same columns, and both must produce the same verdict for the
    same album or the two surfaces start telling different stories about
    it. Every leg here is built by the production function that owns it —
    ``interpret_spectral_evidence``, ``album_ultrasonic_proof_leg``,
    ``aac_lattice_proof_leg`` — so this composes rather than re-decides.

    ``container_labels`` are the snapshot's own file extensions and answer
    the ultrasonic leg's decode-path question. A caller that cannot supply
    them gets a leg that withholds, which is the fail-closed direction: it
    asserts nothing rather than gating a value against a threshold frozen
    on a different instrument (Phase 5 plan §1.5c, +3.09 dB skew).
    """
    spectral = interpret_spectral_evidence(SpectralEvidenceFacts(
        spectral_grade=spectral_grade,
        codec_family=codec_family,
        spectral_subject=spectral_subject,
        was_converted_from=was_converted_from,
        format=format,
        storage_format=storage_format,
        filetype_band=filetype_band,
        cliff_hz=cliff_hz,
        spectral_bitrate_kbps=spectral_bitrate_kbps,
    ))
    return album_proof_verdict(
        spectral=spectral,
        spectral_grade=spectral_grade,
        ultrasonic_leg=album_ultrasonic_proof_leg(
            ultrasonic_deficit_db=ultrasonic_deficit_db,
            spectral_measurement_version=spectral_measurement_version,
            spectral_subject=spectral_subject,
            was_converted_from=was_converted_from,
            container_labels=container_labels,
        ),
        aac_lattice_leg=aac_lattice_proof_leg(aac_lattice),
    )


#: The ONE operator sentence per tier, shared verbatim by
#: ``pipeline-cli quality`` and the web evidence panel so the two surfaces
#: cannot drift into two different claims about the same album.
#:
#: Wording rules, all from the Phase 5 plan and binding:
#:
#: * tier 1 is a finding, and the only transcode statement a surface makes
#:   — the grade chip beside it must not repeat the accusation (plan §3
#:   PR4: "reconciled ... so there is ONE statement, not two");
#: * tier 4 says what is missing, not what the album is: 16 of 100 genuine
#:   controls have no measurable ultrasonic content;
#: * tier 5 claims nothing beyond the tests that ran (plan §1.7).
_TIER_STATEMENTS: dict[int, str] = {
    PROOF_TIER_DETECTED: "Transcode detected",
    PROOF_TIER_NO_ULTRASONIC: "No ultrasonic content — not spectrally provable",
    PROOF_TIER_NO_FINDING: "No evidence of lossy origin from the tests that ran",
}

#: What a tier-5 verdict says when NO leg adjudicated. Distinct from the
#: tier-5 statement on purpose: "nothing was found" and "nothing was
#: looked for" are different facts, and conflating them is how a surface
#: reports a clearance it never earned. The glance surfaces skip the block
#: entirely in this state; ``pipeline-cli quality`` prints it, because a
#: diagnostic command's job is to say why there is nothing to show.
_NO_TEST_RAN_STATEMENT = "No proof-gate test could run on this album"

#: Which leg produced a tier-1 finding, appended to the tier statement.
#: Both are positive detections; they are different instruments and an
#: operator triaging one wants to know which.
_LEG_DETAIL: dict[ProofLeg, str] = {
    PROOF_LEG_IN_WINDOW_CLIFF: "in-window spectral cliff",
    PROOF_LEG_AAC_LATTICE: "AAC encoder frame lattice",
}


def proof_tier_statement(verdict: AlbumProofVerdict) -> str:
    """The single operator sentence for one album's proof-gate verdict.

    Never an accusation and never an instruction: a fired leg means the
    album is worth a look, and withholding a proof never rejects,
    denylists or accuses (Phase 5 plan §2). The corpus behind the tier
    table was 1 genuine : 3 launders by construction, so no wording here
    may read as posterior odds (plan §1, base-rate caveat).
    """
    if not verdict.has_finding:
        return _NO_TEST_RAN_STATEMENT
    statement = _TIER_STATEMENTS[verdict.tier]
    if verdict.tier != PROOF_TIER_DETECTED:
        return statement
    details = [
        _LEG_DETAIL[leg] for leg in verdict.fired_legs if leg in _LEG_DETAIL
    ]
    if not details:
        return statement
    return f"{statement}: {' + '.join(details)}"


#: What each minted proof generation was actually tested for. The column
#: (``album_quality_evidence.verified_lossless_classifier``) is written at
#: exactly one site, ``mint_verified_lossless_proof``, and until this PR
#: was rendered to zero operator surfaces — so "verified lossless" silently
#: meant two different things across the library (Phase 5 plan, PR3 hard
#: constraint 3).
#:
#: The wording states the tested class, never bit-faithfulness (§1.7).
_CLASSIFIER_LABELS: dict[str, str] = {
    VERIFIED_LOSSLESS_CLASSIFIER: "cliff/grade gate only",
    VERIFIED_LOSSLESS_CLASSIFIER_V3: "cliff/grade + ultrasonic legs",
    VERIFIED_LOSSLESS_CLASSIFIER_V4: (
        "cliff/grade + ultrasonic + AAC-lattice legs"
    ),
}


def verified_lossless_generation_label(classifier: str | None) -> str | None:
    """Operator label for the model that minted a verified-lossless proof.

    ``None`` for a row carrying no proof. An unrecognised classifier — a
    generation minted by code newer than this renderer — returns the raw
    value rather than a fabricated description: a surface must never
    describe a test suite it does not know.
    """
    if not classifier:
        return None
    return _CLASSIFIER_LABELS.get(classifier, classifier)
