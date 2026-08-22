"""Pure import/quality decision functions and their I/O Structs.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import msgspec

from lib.quality.audio_validation import AudioValidationReport
from lib.quality.compare import compare_quality
from lib.quality.evidence_types import (
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    SPECTRAL_TRANSCODE_GRADES,
    AacLatticeCapture,
    AudioQualityMeasurement,
    CodecFamily,
    EvidenceSubject,
    QualityComparisonBasis,
    TargetQualityContract,
    V0ProbeEvidence,
    VerifiedLosslessProof,
    is_comparable_lossless_source_probe,
)
from lib.quality.ranks import QualityRank, QualityRankConfig, gate_rank
from lib.quality.spectral_interpretation import (
    SPECTRAL_DECODE_PATH_SOX_NATIVE,
    SpectralDecodePath,
    SpectralInterpretation,
    resolve_spectral_decode_path,
)

DECISION_PROVISIONAL_LOSSLESS_UPGRADE = "provisional_lossless_upgrade"
DECISION_SUSPECT_LOSSLESS_DOWNGRADE = "suspect_lossless_downgrade"
DECISION_SUSPECT_LOSSLESS_PROBE_MISSING = "suspect_lossless_probe_missing"
DECISION_LOSSLESS_SOURCE_LOCKED = "lossless_source_locked"
DECISION_VERIFIED_LOSSLESS_LOCKED = "verified_lossless_locked"
#: Issue #1241. The quality comparison said "not better", but the INSTALLED
#: copy is positively missing a declared audio component and this candidate
#: provably covers the whole declared program — so the two sides are not the
#: same program and the destructive reject is withheld. Deliberately NOT a
#: member of ``QUALITY_DECISION_REJECT_STAGE_DECISIONS``: that omission is
#: what makes the Wrong Matches cleanup reducer KEEP the folder for the
#: operator instead of deleting it. See ``docs/quality-verification.md`` and
#: ``docs/rejection-routing.md``.
DECISION_INSTALLED_INCOMPLETE_HOLD = "installed_incomplete_hold"


# ---------------------------------------------------------------------------
# MeasurementFailure — U4 wire-boundary type for preview measurement failures
# ---------------------------------------------------------------------------

MeasurementFailureReason = Literal[
    "snapshot_stale",          # source folder changed after retry (AE5)
    "source_vanished",         # ENOENT mid-measure (AE6); also covers
                               # path-missing pre-claim and force-import
                               # failed_path-no-longer-on-disk cases
    "materialization_error",   # tempdir copy / shutil failure during measure
    "measurement_crashed",     # ffmpeg / sox / mutagen blew up
    "evidence_persist_failed", # DB write failed after measurement completed
    "request_not_found",       # parent album_request gone (no-finalize subcase)
    "missing_release_id",      # request has no mb_release_id
    "download_log_not_found",  # force-import UI: download_log row gone
    "missing_failed_path",     # force-import UI: download_log lacks failed_path
]


class MeasurementFailure(msgspec.Struct, frozen=True):
    """Typed wire-boundary payload for preview-side measurement failures.

    Carried through ``import_jobs.preview_result`` (JSONB) and
    ``download_log.validation_result`` (JSONB). The Recents UI grep-classifies
    on ``reason`` to render the appropriate badge.

    ``reason`` is a coarse ``Literal`` tag drawn from the
    ``MeasurementFailureReason`` taxonomy — callers can switch on it
    without parsing free text. ``detail`` is a short human-readable string
    for logs and the audit trail; do not parse it. ``source_path`` is the
    folder/file the measurement attempted, when known (``""`` when the
    failure happened before any path was resolved — e.g. ``request_not_found``).

    Wire-boundary type per ``.claude/rules/code-quality.md`` § "Wire-boundary
    types" — encode via ``msgspec.json.encode`` / ``msgspec.to_builtins``,
    decode via ``msgspec.convert``. Strict validation at the boundary catches
    drift between the Struct's declared taxonomy and what the producer wrote.
    Mirrors the precedent set by ``lib.beets_album_op.BeetsOpFailure``.
    """
    reason: MeasurementFailureReason
    detail: str
    source_path: str = ""
    audio_validation: AudioValidationReport | None = None


# ---------------------------------------------------------------------------
# U11: ``preimport_decide`` and ``PreimportDecision`` have been folded into
# ``full_pipeline_decision_from_evidence``. The five folder/audio-integrity
# facts (``audio_corrupt``, ``bad_audio_hash``, ``nested_layout``,
# ``empty_fileset``, ``mixed_source``) are now early-exit reject branches at
# the top of that function. There is exactly one decision function for the
# importer.
#
# See CLAUDE.md § "Quality decisions live in ONE place" and the U11 entry in
# ``docs/plans/2026-05-16-002-refactor-evidence-canonical-cleanup-plan.md``.
# ---------------------------------------------------------------------------


def spectral_import_decision(
    spectral_grade: str | None,
    spectral_bitrate: int | None,
    existing_spectral_bitrate: int | None,
) -> Literal["import", "import_upgrade", "import_no_exist", "reject"]:
    """Decide whether to import a download based on spectral analysis.

    Pure comparison of spectral evidence against spectral evidence. Container
    bitrate is intentionally NOT consulted — that violates evidence-set parity
    (absence of an existing spectral measurement is not evidence the existing
    file is genuine, only that we haven't measured it yet).

    Returns one of:
        "import"          — spectral says genuine/marginal, OR the spectral
                            estimate ties the existing one (defer the verdict
                            to the full codec-aware comparison in Stage 2)
        "import_upgrade"  — spectral says suspect but better than existing
        "import_no_exist" — spectral says suspect but nothing on disk yet
        "reject"          — spectral says suspect and STRICTLY worse than existing

    A tie on the spectral estimate is NOT evidence the candidate is worse — it
    is evidence the two sides carry the same amount of real content on the one
    metric this coarse stage measures. Rejecting on a tie let a strictly-better
    copy (higher container bitrate / better grade / higher V0) be discarded as
    "not better", because this stage short-circuits before Stage 2 ever runs
    (Mark DeNardo "Lion, Tiger, Bear", request 1308: MP3 192 CBR / suspect /
    spectral 128 rejected against MP3 128 CBR / likely_transcode / spectral
    128). So a tie defers to ``compare_quality`` — the single codec-aware
    comparison built to break exactly this tie on the raw metric
    (``_shared_spectral_bitrates``). Only a STRICTLY lower spectral estimate is
    affirmative evidence the candidate has less real content; that still
    rejects here, which also protects Stage 2's transcode-vs-transcode blind
    spot (``_transcode_candidate_real_rank_regresses`` only guards a
    transcode over a NON-transcode existing album).

    Inputs:
        spectral_grade:             "genuine" | "marginal" | "suspect" | "likely_transcode"
        spectral_bitrate:           estimated bitrate from cliff detection (kbps), or None
        existing_spectral_bitrate:  spectral estimate of what's already in beets (kbps), or 0/None
    """
    if spectral_grade not in ("suspect", "likely_transcode"):
        return "import"

    new_q = spectral_bitrate or 0
    existing_q = existing_spectral_bitrate or 0

    # Strictly-less-than, not <=: an equal spectral floor is a tie, not a
    # downgrade — it defers to Stage 2's raw-metric tiebreak (see docstring).
    if new_q and existing_q and new_q < existing_q:
        return "reject"
    elif new_q and existing_q and new_q > existing_q:
        return "import_upgrade"
    elif not existing_q:
        return "import_no_exist"
    else:
        return "import"


# ---------------------------------------------------------------------------
# import_one.py decisions (FLAC conversion path)
# ---------------------------------------------------------------------------

class ImportQualityDecision(msgspec.Struct, frozen=True):
    """A decision string plus the comparison basis that produced it.

    ``basis`` is None exactly when no comparison ran (no existing album).
    """
    decision: str
    basis: QualityComparisonBasis | None = None


def import_quality_decision(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement | None,
    is_transcode: bool = False,
    cfg: QualityRankConfig | None = None,
    *,
    target_contract: TargetQualityContract | None = None,
    v0_probe: V0ProbeEvidence | None = None,
    verified_lossless_proof: bool = False,
    source_spectral: SpectralInterpretation | None = None,
    current_spectral: SpectralInterpretation | None = None,
    installed_incomplete: bool = False,
    candidate_covers_declared_program: bool = False,
) -> ImportQualityDecision:
    """Decide whether to import based on codec-aware quality comparison (issue #60).

    Called in import_one.py after FLAC→V0 conversion (if applicable)
    and before running the beets harness.

    Uses compare_quality() which classifies both measurements into
    QualityRank bands (via quality_rank/measurement_rank), so cross-codec
    comparisons (Opus 128 vs MP3 V0) are correctly treated as equivalent.

    The verified-lossless proof bypass is now a tier-gated preference:
    ``verified_lossless_proof=True`` still forces an import when the verdict is
    "better" or "equivalent", but NOT when it would be a downgrade — this
    blocks a deliberately too-low ``verified_lossless_target`` (e.g. Opus
    64) from replacing a good existing album. Proof-backed imports use the
    ordinary ``import`` decision so dispatch runs the terminal quality gate;
    conversion remains recorded separately on ``ImportResult``. When the
    bypass CHANGED the outcome (an "equivalent" verdict imported), the
    returned basis records ``verified_lossless_bypass=True`` so the persisted
    audit trail explains the import; a "better" verdict imports on its own
    merits and the flag stays False.

    Returns an ImportQualityDecision whose ``decision`` is one of:
        "import"              — new files are better (or no existing), proceed;
                                also the terminal path for proof-backed targets
        "downgrade"           — new files are worse, skip (exit 5)
        "transcode_upgrade"   — transcode but better than existing, import + denylist (exit 6)
        "transcode_downgrade" — transcode and not better, skip + denylist (exit 6)
        "transcode_first"     — transcode but nothing on disk yet, import (exit 6)
        "installed_incomplete_hold"
                              — the comparison said "equivalent", but the
                                installed copy is positively missing a
                                declared component and this candidate covers
                                the whole declared program: withhold the
                                reject and stage for the operator (#1241)

    Args:
        new: measurement of the new download
        existing: measurement of what's already in beets, or None
                  (caller resolves override_min_bitrate into existing.min_bitrate_kbps)
        is_transcode: True if FLAC→V0 produced a transcode (from transcode_detection)
        cfg: QualityRankConfig. Defaults to QualityRankConfig.defaults().
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()

    if existing is None:
        return ImportQualityDecision(
            decision=(
                "transcode_first"
                if is_transcode and not verified_lossless_proof
                else "import"
            )
        )

    basis = compare_quality(
        new,
        existing,
        cfg,
        new_target_contract=target_contract,
        new_v0_probe=v0_probe,
        new_spectral=source_spectral,
        existing_spectral=current_spectral,
    )
    verdict = basis.verdict

    # Verified-lossless proof is a soft preference: "better" or "equivalent"
    # still import, but "worse" is blocked regardless of proof status.
    # This prevents a deliberately too-low verified-lossless target from
    # blindly replacing a good existing album (issue #60 acceptance criterion).
    #
    # It is a preference in ONE direction only. The comparison itself stays
    # proof-blind: a proof-DENIED album that measures better imports and
    # replaces, and no brake may ever be added here. Authority: "we measure,
    # we assign evidence, grade and whatever else. then, we import, it
    # compares to whats on disk, if its better it imports. it makes no
    # difference if it's laundered or not, is it better? if you denied
    # laundered audio you'd never get anything for this particular release."
    # — https://github.com/abl030/cratedigger/issues/829
    if verified_lossless_proof and verdict in ("better", "equivalent"):
        return ImportQualityDecision(
            decision="import",
            basis=(
                msgspec.structs.replace(basis, verified_lossless_bypass=True)
                if verdict == "equivalent"
                else basis
            ),
        )

    if verdict == "better":
        return ImportQualityDecision(
            decision="transcode_upgrade" if is_transcode else "import",
            basis=basis,
        )

    # Issue #1241. An installed copy that is positively MISSING a declared
    # audio component is not a sound baseline for a destructive "not better"
    # verdict: the two sides are not the same program. When the candidate
    # provably covers the whole declared program (beets' own extra_tracks
    # reject at ``lib/beets.py::apply_candidate_scenario`` fell through for
    # THIS attempt) and the comparison said "equivalent" — not "worse" —
    # withhold the reject and hand the album to the operator instead of
    # deleting the folder.
    #
    # "worse" stays blocked regardless, exactly as it is under the
    # verified-lossless bypass above (issue #60 acceptance criterion). The
    # accepted residual: a genuinely-worse-but-complete candidate against an
    # incomplete installed copy is still rejected. That cohort is countable
    # via this decision string before anyone widens the guard.
    if (
        verdict == "equivalent"
        and installed_incomplete
        and candidate_covers_declared_program
    ):
        return ImportQualityDecision(
            decision=DECISION_INSTALLED_INCOMPLETE_HOLD,
            basis=msgspec.structs.replace(basis, installed_incomplete_hold=True),
        )

    # "worse" or "equivalent" without verified_lossless bypass → reject.
    return ImportQualityDecision(
        decision="transcode_downgrade" if is_transcode else "downgrade",
        basis=basis,
    )


class MeasuredImportDecisionInput(msgspec.Struct, frozen=True):
    """Pure input for the measured import decision reducer.

    This is the common shape shared by the typed simulator, import preview,
    and the real import harness once files have been measured. It deliberately
    contains no filesystem, database, or subprocess concerns.
    """
    source_measurement: AudioQualityMeasurement
    current_measurement: AudioQualityMeasurement | None = None
    is_transcode: bool = False
    target_contract: TargetQualityContract | None = None
    v0_probe: V0ProbeEvidence | None = None
    verified_lossless_proof: bool = False
    # issue #829 Phase 5 PR2b — each side's codec-aware spectral
    # interpretation, when the caller resolved it with album-level context
    # the measurement cannot carry (``filetype_band``'s mixed-codec
    # fail-closed). ``None`` means "derive from the measurement", which is
    # what the harness does; supplying one can only ever withhold MORE.
    source_spectral: SpectralInterpretation | None = None
    current_spectral: SpectralInterpretation | None = None
    # issue #1241 — the two conjuncts of the installed-incomplete hold.
    # ``installed_incomplete`` is a POSITIVE verdict on the installed copy
    # (never "unknown"); ``candidate_covers_declared_program`` is beets' own
    # proof that this attempt's candidate carried no extra/missing declared
    # track. Both default False, so an unmeasured world behaves as before.
    installed_incomplete: bool = False
    candidate_covers_declared_program: bool = False


class MeasuredImportDecisionResult(msgspec.Struct, frozen=True):
    """Pure measured-decision result with preview-friendly classification."""
    decision: str
    exit_code: int = 0
    would_import: bool = False
    confident_reject: bool = False
    uncertain: bool = False
    cleanup_eligible: bool = False
    stage_chain: list[str] = []
    reason: str | None = None
    # The comparison compare_quality() performed, None when no existing
    # album was compared. Persisted onto ImportResult so the UI renders
    # the decision's own explanation instead of re-deriving one.
    comparison_basis: QualityComparisonBasis | None = None


class ProvisionalLosslessDecisionInput(msgspec.Struct, frozen=True):
    """Pure input for the unproven lossless-source provisional lane.

    ``will_be_verified`` is the caller's ``determine_verified_lossless``
    answer for this candidate — proof legs included — and it keys lane
    ENTRY: grade decides accusations, proof decides terminality, and
    using the grade as an entry proxy is what let a genuine-graded,
    proof-denied FLAC bypass the anchor and re-import equal copies
    forever (issue #990, request 2066).

    ``spectral_grade`` retains two narrow roles, neither of which is an
    entry proxy for the proof:

    * a source spectral analysis never MEASURED (grade ``None``/
      ``error``) keeps its historical measured routing — production
      surfaces those worlds as ``measurement_failed`` before any
      decision runs;
    * the fail direction when the candidate carries no comparable probe:
      an ACCUSED source (suspect/likely_transcode) without evidence keeps
      its historical fail-closed reject; an unaccused one is rejected
      when a comparable anchor exists (no evidence can challenge the
      recorded truth-of-source) and otherwise falls through to the
      measured policy's historical first-import behavior. On the
      production path the preview grinds the probe into the evidence row
      before the importer decides, so the fall-through is an
      abnormal-evidence seam.
    """

    candidate_probe: V0ProbeEvidence | None = None
    existing_probe: V0ProbeEvidence | None = None
    will_be_verified: bool = False
    spectral_grade: str | None = None
    supported_lossless_source: bool = False


class ProvisionalLosslessDecisionResult(msgspec.Struct, frozen=True):
    """Decision result for the suspect lossless-source lane."""

    decision: str | None = None
    would_import: bool = False
    confident_reject: bool = False
    cleanup_eligible: bool = False
    reason: str | None = None
    stage_chain: list[str] = []


#: The grades a source spectral analysis actually MEASURED. The lane owns
#: unproven sources among these; ``None``/``error`` worlds never reach a
#: decision in production (``measurement_failed``) and keep their
#: historical measured routing in the simulator.
_PROVISIONAL_LANE_MEASURED_GRADES: frozenset[str] = frozenset({
    "genuine", "marginal", "suspect", "likely_transcode",
})


def provisional_lossless_decision(
    candidate: ProvisionalLosslessDecisionInput,
    *,
    cfg: QualityRankConfig | None = None,
) -> ProvisionalLosslessDecisionResult:
    """Compare unproven lossless-source V0 probes inside the provisional lane.

    Returns ``decision=None`` for candidates that should continue through the
    existing import policy (native lossy without a lock anchor, or a
    candidate whose verified-lossless proof WILL be minted). For every other
    supported lossless source — suspect-graded, genuine-graded-but-denied,
    unmeasured — V0 probe avg bitrate is the comparison signal and
    ``within_rank_tolerance_kbps`` is the only tolerance knob.

    Entry is keyed on PROOF ABSENCE (``will_be_verified``), never on the
    spectral grade: "provisional" means unproven, and an unproven source
    must beat the recorded truth-of-source anchor to displace the copy it
    would replace. Grade-keyed entry let a genuine-graded FLAC whose proof
    the ultrasonic leg denied re-import equal transcode-lineage copies
    forever (request 2066, 95 downloads). Callers that certify via the
    V0-avg trust override bypass this lane BEFORE calling — a denial never
    suppresses that bypass (V5's surviving core).
    Authority: "HENCE PROVISIONAL LOSSLESS SOURCE" — the lane owns the
    unproven cohort;
    https://github.com/abl030/cratedigger/issues/990#issuecomment-5158156922

    When ``supported_lossless_source`` is False (lossy candidate) AND
    ``existing_probe`` is a comparable lossless-source probe, the function
    returns ``DECISION_LOSSLESS_SOURCE_LOCKED`` — a lossy candidate cannot
    produce a comparable measurement, and the recorded probe is the truth-
    of-source anchor. ``candidate_probe`` is ignored in that branch.
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()

    if not candidate.supported_lossless_source:
        # Lossless-source lock: when the existing album was previously
        # imported as a provisional lossless source we transcoded down (so
        # the linked source-subject V0 metric is the only comparable anchor),
        # a lossy candidate cannot produce comparable
        # evidence and must not be allowed to override on raw avg alone.
        # The recorded V0 probe is the truth-of-source anchor; only another
        # lossless-container candidate (which can be ground to V0) is
        # eligible to displace it.
        if is_comparable_lossless_source_probe(candidate.existing_probe):
            assert candidate.existing_probe is not None
            existing_avg = candidate.existing_probe.avg_bitrate_kbps
            decision = DECISION_LOSSLESS_SOURCE_LOCKED
            return ProvisionalLosslessDecisionResult(
                decision=decision,
                would_import=False,
                confident_reject=True,
                cleanup_eligible=True,
                reason=(
                    f"existing has lossless-source V0 probe "
                    f"{existing_avg}kbps; lossy candidate cannot produce "
                    f"comparable evidence (only another lossless source "
                    f"can override)"
                ),
                stage_chain=[f"stage2_provisional:{decision}"],
            )
        return ProvisionalLosslessDecisionResult()

    if candidate.will_be_verified:
        return ProvisionalLosslessDecisionResult()

    if candidate.spectral_grade not in _PROVISIONAL_LANE_MEASURED_GRADES:
        # Spectral never ran (None) or errored: production surfaces those
        # as ``measurement_failed`` before any decision, and simulator /
        # legacy worlds keep their historical measured routing (missing
        # analysis reads as a transcode there). The lane owns unproven
        # MEASURED sources only.
        return ProvisionalLosslessDecisionResult()

    if not is_comparable_lossless_source_probe(candidate.candidate_probe):
        if candidate.spectral_grade in SPECTRAL_TRANSCODE_GRADES:
            decision = DECISION_SUSPECT_LOSSLESS_PROBE_MISSING
            return ProvisionalLosslessDecisionResult(
                decision=decision,
                would_import=False,
                confident_reject=True,
                cleanup_eligible=True,
                reason="suspect lossless source lacks a comparable V0 probe",
                stage_chain=[f"stage2_provisional:{decision}"],
            )
        if is_comparable_lossless_source_probe(candidate.existing_probe):
            # The recorded anchor is the truth-of-source; a candidate with
            # no comparable evidence cannot displace the copy it anchors,
            # whatever the contract-vs-measured ranks would say — the same
            # doctrine as the lossy lock above (#990 review finding 5).
            decision = DECISION_SUSPECT_LOSSLESS_PROBE_MISSING
            return ProvisionalLosslessDecisionResult(
                decision=decision,
                would_import=False,
                confident_reject=True,
                cleanup_eligible=True,
                reason=(
                    "unproven lossless source lacks a comparable V0 probe "
                    "to challenge the recorded source anchor"
                ),
                stage_chain=[f"stage2_provisional:{decision}"],
            )
        # Unaccused, unanchored, and evidence-less: continue through the
        # measured policy — there is no anchor to defend. That includes an
        # existing UNANCHORED copy, which the measured compare may still
        # displace; self-limiting, because the import records the anchor
        # and every later probe-carrying candidate takes the lane. On the
        # production path the preview grinds the V0 probe into the
        # evidence row BEFORE the importer decides, so this fall-through
        # is an abnormal-evidence seam, not the ordinary route.
        return ProvisionalLosslessDecisionResult()

    candidate_probe = candidate.candidate_probe
    assert candidate_probe is not None
    candidate_avg = candidate_probe.avg_bitrate_kbps
    assert candidate_avg is not None
    existing_probe = (
        candidate.existing_probe
        if is_comparable_lossless_source_probe(candidate.existing_probe)
        else None
    )

    if existing_probe is None:
        decision = DECISION_PROVISIONAL_LOSSLESS_UPGRADE
        return ProvisionalLosslessDecisionResult(
            decision=decision,
            would_import=True,
            reason="no existing comparable lossless-source V0 probe",
            stage_chain=[f"stage2_provisional:{decision}"],
        )

    existing_avg = existing_probe.avg_bitrate_kbps
    assert existing_avg is not None
    delta = candidate_avg - existing_avg
    if delta <= cfg.within_rank_tolerance_kbps:
        decision = DECISION_SUSPECT_LOSSLESS_DOWNGRADE
        return ProvisionalLosslessDecisionResult(
            decision=decision,
            would_import=False,
            confident_reject=True,
            cleanup_eligible=True,
            reason=(
                f"candidate V0 probe avg {candidate_avg}kbps is not more than "
                f"{cfg.within_rank_tolerance_kbps}kbps above existing "
                f"{existing_avg}kbps"
            ),
            stage_chain=[f"stage2_provisional:{decision}"],
        )

    decision = DECISION_PROVISIONAL_LOSSLESS_UPGRADE
    return ProvisionalLosslessDecisionResult(
        decision=decision,
        would_import=True,
        reason=(
            f"candidate V0 probe avg {candidate_avg}kbps beats existing "
            f"{existing_avg}kbps by more than "
            f"{cfg.within_rank_tolerance_kbps}kbps"
        ),
        stage_chain=[f"stage2_provisional:{decision}"],
    )


def build_existing_quality_measurement(
    *,
    min_bitrate_kbps: int | None,
    avg_bitrate_kbps: int | None = None,
    median_bitrate_kbps: int | None = None,
    format: str | None = None,
    is_cbr: bool = False,
    override_min_bitrate: int | None = None,
    spectral_grade: str | None = None,
    spectral_bitrate_kbps: int | None = None,
    cliff_hz: int | None = None,
    codec_family: CodecFamily | None = None,
) -> AudioQualityMeasurement | None:
    """Build an existing-album measurement from primitive quality facts.

    The spectral override clamps avg/median only for CBR albums. VBR existing
    albums keep their real avg/median so a stale spectral floor cannot erase
    the genuine rank signal that compare_quality() should use.

    ``cliff_hz``/``codec_family`` are the issue #829 Phase 5 PR1 measured
    facts. They carry through so the reconstructed measurement can still be
    interpreted in its own codec's terms — the reconstruction dropping them
    is exactly how a LAME-table number reached a cross-codec comparison.
    """
    if min_bitrate_kbps is None:
        return None

    effective_min = (
        override_min_bitrate
        if override_min_bitrate is not None
        else min_bitrate_kbps
    )
    # No fabricated fallbacks: an unmeasured avg/median stays None so the
    # persisted basis labels the classified value "min" instead of claiming
    # an avg nobody measured (dl 36660 display-lie class). Value-neutral
    # under the AVG (deployed) and MIN metrics — selection falls back to
    # the same min the old fabrication aliased. Only a hypothetical
    # bitrate_metric=median config sees different stage-2 values, and
    # median was never honest on this path (no real median crosses the
    # flat interface). The CBR+override clamp below is different — that's
    # deliberate policy (a CBR album's avg IS its min), pinned by its own
    # tests.
    raw_avg = avg_bitrate_kbps
    raw_median = median_bitrate_kbps
    if is_cbr and override_min_bitrate is not None:
        effective_avg = override_min_bitrate
        effective_median = override_min_bitrate
    else:
        effective_avg = raw_avg
        effective_median = raw_median

    return AudioQualityMeasurement(
        min_bitrate_kbps=effective_min,
        avg_bitrate_kbps=effective_avg,
        median_bitrate_kbps=effective_median,
        format=format,
        is_cbr=is_cbr,
        spectral_grade=spectral_grade,
        spectral_bitrate_kbps=spectral_bitrate_kbps,
        spectral_subject=(
            EVIDENCE_SUBJECT_INSTALLED if spectral_grade is not None else None
        ),
        spectral_provenance=(
            EVIDENCE_PROVENANCE_MEASURED if spectral_grade is not None else None
        ),
        cliff_hz=cliff_hz if spectral_grade is not None else None,
        codec_family=codec_family if spectral_grade is not None else None,
    )


def measured_import_decision(
    measured: MeasuredImportDecisionInput,
    *,
    cfg: QualityRankConfig | None = None,
) -> MeasuredImportDecisionResult:
    """Reduce measured import facts to a decision and preview classification."""
    quality = import_quality_decision(
        measured.source_measurement,
        measured.current_measurement,
        measured.is_transcode,
        cfg=cfg,
        target_contract=measured.target_contract,
        v0_probe=measured.v0_probe,
        verified_lossless_proof=measured.verified_lossless_proof,
        source_spectral=measured.source_spectral,
        current_spectral=measured.current_spectral,
        installed_incomplete=measured.installed_incomplete,
        candidate_covers_declared_program=(
            measured.candidate_covers_declared_program
        ),
    )
    decision = quality.decision
    exit_code = 0
    if decision == "downgrade":
        exit_code = 5
    elif decision == "transcode_downgrade":
        exit_code = 6

    would_import = decision in {
        "import",
        "transcode_upgrade",
        "transcode_first",
        DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
    }
    confident_reject = decision in {
        "downgrade",
        "transcode_downgrade",
        DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
        DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
    }
    reason = decision
    if measured.current_measurement is None:
        reason = f"{decision}: no existing album"
    elif confident_reject:
        reason = (
            f"{decision}: measured candidate is not an upgrade over existing"
        )

    return MeasuredImportDecisionResult(
        decision=decision,
        exit_code=exit_code,
        would_import=would_import,
        confident_reject=confident_reject,
        # issue #1241: the hold is the one measured outcome that neither
        # imports nor confidently rejects — it stages for the operator.
        uncertain=decision == DECISION_INSTALLED_INCOMPLETE_HOLD,
        cleanup_eligible=confident_reject,
        stage_chain=[f"stage2_import:{decision}"],
        reason=reason,
        comparison_basis=quality.basis,
    )


def transcode_detection(
    converted_count: int, *, spectral_grade: str | None = None,
) -> bool:
    """Detect whether a FLAC→V0 conversion produced a transcode.

    Called in import_one.py after convert_flac_to_v0().

    Returns True if the converted files are likely transcodes
    (MP3 wrapped in FLAC container).

    Inputs:
        converted_count: number of FLAC files converted
        spectral_grade:  affirmative album spectral grade; absent or errored
                         analysis fails closed
    """
    if converted_count == 0:
        return False
    # Suspect/likely-transcode are affirmative disagreement and may later be
    # rescued by the V0 override. Missing/error/unknown evidence is an abort,
    # represented conservatively here so it can never mint verification.
    return spectral_grade not in ("genuine", "marginal")


# ---------------------------------------------------------------------------
# Verified lossless derivation (post-import, used by album_source.py)
# ---------------------------------------------------------------------------

_LOSSLESS_EXTS = {"flac", "m4a", "wav", "alac"}


# V0-avg trust override thresholds. A lossless_source_v0 probe with avg
# AND min at-or-above these levels is strong evidence the source carried
# enough HF complexity that LAME couldn't strip it — i.e. a real lossless
# master, not a fake-FLAC of a lossy intermediate. Below either bar we
# defer to spectral as before. Tuned against Bill Hicks 1990 "Dangerous"
# (avg=241/min=219, spoken-word lossless that spectral false-positives as
# suspect because speech has no HF energy for the music-tuned thresholds
# to measure against).
V0_OVERRIDE_AVG_KBPS: int = 230
V0_OVERRIDE_MIN_KBPS: int = 200


# ---------------------------------------------------------------------------
# Proof gate v3 — the ultrasonic deficit leg (issue #829 Phase 5 PR3)
# ---------------------------------------------------------------------------

#: Deny verified-lossless promotion when the album's level-invariant
#: ultrasonic deficit reaches this many dB.
#:
#: FROZEN 2026-07-31 by re-running the frozen scorer's own leg logic
#: (``docs/research/calibration-data/score_v3.py.frozen``, ``_window_legs``
#: / ``gate``) over all four committed arms at production's SINGLE window
#: 0 — the only window production measures. Ablation, cliff + ceiling +
#: ultrasonic, 100 genuine controls and 300 FLAC-container launders:
#:
#:     T      leaked launders   genuine denied
#:     57.0        0               43/100
#:     59.5        0               34/100
#:     61.5        0               28/100
#:     62.0        1               26/100
#:
#: The binding minimum — the lowest ``U`` over launder albums where this
#: leg is the LAST line — is 61.55 dB, so 59.5 carries 2.05 dB of
#: headroom, and the whole table reproduces the Phase 5 plan §1.5b figure
#: exactly. Raising it to 62 leaks a ROUND-3 vorbis-q5 launder; lowering
#: it buys nothing and costs genuine albums.
#:
#: A denial is NOT a rejection. The album imports normally and simply
#: carries no spectral verified-lossless proof, so it stays on the search
#: surface (Phase 5 plan §2's authority; §1.7's claim). It does not change
#: the album's stored format either: the configured
#: ``verified_lossless_target`` applies to every lossless-sourced import,
#: proved or not (issue #829, operator decision 2026-08-01 — "the contract
#: is not around verified or not, is the stored format for lossless
#: absolutely"). Proof decides names; config decides formats.
#:
#: KNOWN RESIDUAL, measured in the same run: production evaluates only
#: TWO of the frozen scorer's three legs — its cliff leg is the album
#: spectral GRADE, and it has no ceiling leg, which needs per-track slice
#: vectors production does not persist. Against that reduced leg set one
#: launder of 300 survives at any threshold above 57.03 dB
#: (TRAINING / ``t-vorbisq5-flac`` / Autechre, ``U=57.03``, caught by the
#: ceiling leg alone). Dropping to 57.0 to catch it would leave 0.03 dB of
#: headroom — a coincidence, not a margin — and deny 43 of 100 genuine
#: controls instead of 34. It is recorded rather than chased: §1.7 already
#: bounds the claim to "no evidence of lossy origin by the tests we have",
#: and a ceiling leg is new capture, not PR3's scope.
ULTRASONIC_PROOF_DENY_DEFICIT_DB: float = 59.5

#: The ``spectral_measurement_version`` at which ``ultrasonic_deficit_db``
#: began being measured at all (``lib/spectral_check.py``'s PR1 capture).
#: A row below it did not fail to measure the statistic — the code that
#: measures it had not shipped.
ULTRASONIC_PROOF_MIN_MEASUREMENT_VERSION = 2

UltrasonicProofOutcome = Literal["denied", "passed", "withheld"]

UltrasonicProofReason = Literal[
    # The leg adjudicated.
    "deficit_below_threshold",
    "deficit_at_or_above_threshold",
    # The three distinct NULL states, deliberately never conflated.
    "preserved_source_spectral",
    "legacy_measurement",
    "not_measured",
    # Measured, but not on the scale the threshold was calibrated for.
    "uncalibrated_decode_path",
]


@dataclass(frozen=True)
class UltrasonicProofLeg:
    """What the ultrasonic deficit leg asserts about one album (pure).

    Three outcomes, and the difference between two of them is the whole
    point:

    * ``denied``   — the measured deficit reaches the frozen threshold.
                     Withhold proof.
    * ``passed``   — the leg ran on a comparable measurement and found no
                     evidence of a laundered ultrasonic band.
    * ``withheld`` — the leg could not run. It asserts NOTHING; promotion
                     falls through to the pre-v3 rules exactly as before.

    ``withheld`` must never be read as either evidence or denial. Most of
    the library is in that state and always will be: the source of 6,273
    proof rows was converted away and cannot be re-measured at any price
    (Phase 5 plan §1.5a / the PR3 hard-constraint section).
    """

    outcome: UltrasonicProofOutcome
    reason: UltrasonicProofReason
    deficit_db: float | None = None

    @property
    def denies_promotion(self) -> bool:
        return self.outcome == "denied"

    @property
    def proves_ultrasonic_content(self) -> bool:
        return self.outcome == "passed"


def ultrasonic_proof_leg(
    *,
    deficit_db: float | None,
    spectral_measurement_version: int | None,
    decode_path: "SpectralDecodePath | None",
    preserved_source_spectral: bool,
) -> UltrasonicProofLeg:
    """Evaluate the v3 ultrasonic deficit leg for one album (pure).

    ``U = mean_over_tracks[ref_db(1-4kHz) - mean(20.5-22kHz)]``, measured
    by ``lib/spectral_check.py`` and persisted album-level. Normalising
    against the album's own midband is what makes it comparable across
    masters: in absolute dB a quiet record's genuine ultrasonic content is
    indistinguishable from a loud record's launder leakage.

    ``ultrasonic_deficit_db IS NULL`` is THREE different states and this
    function is where they stop being one (Phase 5 plan, PR3 hard
    constraint 1):

    a. ``preserved_source_spectral`` — an R19 converted copy wearing its
       source's spectral, measured before the statistic existed. The
       source was converted away, so this row can never be re-measured on
       any timescale. 6,273 proof rows, 6,251 albums.
    b. ``legacy_measurement`` — measured before PR1's capture shipped.
       Re-measurable in principle: the lossless files are still on disk
       for 8,273 proof rows.
    c. ``not_measured`` — the current measurement code ran and honestly
       reported no value, because the 20.5-22kHz bands were outside the
       file's Nyquist (any sox-native source below ~44.1kHz). Permanent
       for those bytes, and NOT a measurement failure.

    All three withhold, so the acquisition behaviour is identical; they
    are separated because they are different facts about the world, they
    are what a triage surface has to tell apart, and conflating them was
    called out as mis-handling most of the library.

    ``decode_path`` scoping (Phase 5 plan §1.5c) is the fourth
    non-adjudicating case and the one that is easiest to get wrong: the
    SAME BITS measure 50.26 dB through ``_ffmpeg_to_wav`` at 48kHz versus
    47.17 dB sox-native at 44.1kHz — a +3.09 dB skew, larger than the
    gate's entire 2.05 dB margin (isolated on request 8923, the only
    ``was_converted_from='alac'`` control; the other 16 reproduce their
    stored value to 1e-7). The threshold was frozen against sox-native
    FLAC decodes, so a value from any other path is on a different scale
    and this leg refuses to gate it. That refusal is the conservative
    direction in both senses: it neither denies a genuine album on a
    mis-scaled number nor lets one buy a v3 proof it did not earn.
    """
    if (
        spectral_measurement_version is None
        or spectral_measurement_version < ULTRASONIC_PROOF_MIN_MEASUREMENT_VERSION
    ):
        return UltrasonicProofLeg(
            outcome="withheld",
            reason=(
                "preserved_source_spectral"
                if preserved_source_spectral
                else "legacy_measurement"
            ),
        )
    if deficit_db is None:
        return UltrasonicProofLeg(outcome="withheld", reason="not_measured")
    if decode_path != SPECTRAL_DECODE_PATH_SOX_NATIVE:
        return UltrasonicProofLeg(
            outcome="withheld",
            reason="uncalibrated_decode_path",
            deficit_db=deficit_db,
        )
    if deficit_db >= ULTRASONIC_PROOF_DENY_DEFICIT_DB:
        return UltrasonicProofLeg(
            outcome="denied",
            reason="deficit_at_or_above_threshold",
            deficit_db=deficit_db,
        )
    return UltrasonicProofLeg(
        outcome="passed",
        reason="deficit_below_threshold",
        deficit_db=deficit_db,
    )


def is_preserved_source_spectral(
    spectral_subject: EvidenceSubject | None,
    was_converted_from: str | None,
) -> bool:
    """Whether a spectral fact describes a copy's pre-conversion SOURCE.

    The R19 shape and nothing else — the same predicate
    ``resolve_measured_codec_family`` uses to know it is looking at a
    converted row. Spelled once so the leg's NULL tristate cannot drift
    from the codec resolver's idea of the same world.
    """
    return (
        spectral_subject == EVIDENCE_SUBJECT_SOURCE
        and was_converted_from is not None
    )


def album_ultrasonic_proof_leg(
    *,
    ultrasonic_deficit_db: float | None,
    spectral_measurement_version: int | None,
    spectral_subject: EvidenceSubject | None,
    was_converted_from: str | None,
    container_labels: "Sequence[str]",
) -> UltrasonicProofLeg:
    """Build the proof leg for a caller that holds the raw containers.

    The harness's entry point: it has the source folder's real file
    extensions but no ``SpectralCodecContext``. Resolves the decode path
    from those containers and delegates. A caller that already carries a
    resolved ``SpectralCodecContext`` (the decision twins) calls
    ``ultrasonic_proof_leg`` with the context's own
    ``spectral_decode_path`` instead.

    Both paths run the SAME resolver (``resolve_spectral_decode_path``), so
    the RULE cannot drift; the inputs differ, and that is worth stating
    exactly rather than claiming the two can never disagree. The harness
    lists the source directory non-recursively (``os.listdir`` filtered by
    ``AUDIO_EXTENSIONS``); the decider reads the evidence snapshot, which
    ``snapshot_audio_files`` walks recursively. The two file sets differ
    only when audio sits in a subdirectory — a multi-disc layout — and
    that world does not reach either caller through automation: downloads
    are flattened upstream in ``process_completed_album``, and a snapshot
    that is still nested is rejected as ``nested_layout`` before any proof
    is considered.
    """
    return ultrasonic_proof_leg(
        deficit_db=ultrasonic_deficit_db,
        spectral_measurement_version=spectral_measurement_version,
        decode_path=resolve_spectral_decode_path(
            spectral_subject=spectral_subject,
            was_converted_from=was_converted_from,
            container_labels=container_labels,
        ),
        preserved_source_spectral=is_preserved_source_spectral(
            spectral_subject, was_converted_from,
        ),
    )


# ---------------------------------------------------------------------------
# Proof gate v4 — the AAC frame-lattice leg (issue #829 AAC-lattice leg PR-B)
# ---------------------------------------------------------------------------

#: Deny verified-lossless promotion when this many of an album's scored
#: tracks recover the SAME MDCT frame offset.
#:
#: The one statistic in the whole #829 research whose false-positive rate
#: is a CALCULATION rather than a fitted threshold, and the reason this
#: leg is proof grade rather than triage grade
#: (``docs/research/calibration-data/derrien-refinement/README.md``
#: § "The offset-concentration rule and its false-positive floor"). 1024
#: is the MDCT lattice size and 4 is an integer count; there is nothing
#: here to tune.
#:
#: Measured, over the 17-album genuine control arm and the 1,136-track
#: wild arm (``q3c_out.txt`` § D, ``q3d_out.txt``, ``q3f_out.txt``):
#:
#:     rule        genuine albums   wild folders   analytic FP/5000 albums
#:     k >= 2           0/17              -                  ~322.3
#:     k >= 3           0/17            1/115                  ~1.049
#:     k >= 4           0/17            1/115                  ~0.00231
#:
#: and its recall over the Apple/CoreAudio family at ``k >= 4``:
#: ``qaac-cvbr256`` 17/17, ``qaac-cvbr320`` 17/17, ``qaac-tvbr91`` 17/17,
#: ``qaac-abr192`` 17/17, ``qaac-cbr128`` 16/17 — reproduced across two
#: independent Apple builds. The mechanism is exact: qaac/CoreAudio primes
#: 2112 samples, ``2112 mod 1024 = 64``, so its lattice lands at 960 in
#: ~97% of tracks, while a genuine album's offsets are uniform over
#: 0-1023 (chi^2 19.5 on 31 df, max repeat 3 across ALL 197 pooled tracks
#: and 0/17 albums at k >= 2).
AAC_LATTICE_PROOF_DENY_MODAL_COUNT: int = 4

#: Deny promotion when the album's best per-track lattice contrast
#: exceeds this.
#:
#: ``z = (peak - median) / std`` over the 1024-offset sweep. The naive
#: zero-false-positive threshold is 6.914 — but that is the MAXIMUM
#: OBSERVED genuine track z over 197 tracks, an in-sample maximum and not
#: a false-positive rate. A Gumbel fit to the 17 genuine album-max values
#: (mu=5.598, beta=0.580) prices ``z > 6.914`` at ~492 false-positive
#: albums per 5000 (``q3d_out.txt``). 12 is the conservative operating
#: point: ~0.1 per 5000 analytically, and measured 0/197 genuine control
#: tracks and 0/1136 wild peer tracks (``q3e_out.txt``).
#:
#: Strictly greater, matching the research's own ``z > 12`` framing.
AAC_LATTICE_PROOF_DENY_MAX_Z: float = 12.0

#: Below this many successfully scored tracks the leg cannot say
#: "adjudicated clean" — the offset-concentration rule needs 4 coincident
#: tracks to fire at all, so an album with fewer has not been tested by
#: the rule and a clean result from it means nothing. It withholds.
#:
#: Deliberately the SAME integer as the denial count: "enough evidence to
#: clear an album" is exactly "enough evidence for the rule to have been
#: able to condemn it". ``lib/aac_lattice.py::MAX_SCORED_TRACKS`` scores 6
#: for headroom over per-track errors.
AAC_LATTICE_PROOF_MIN_SCORED_TRACKS: int = AAC_LATTICE_PROOF_DENY_MODAL_COUNT

AacLatticeProofOutcome = Literal["denied", "passed", "withheld"]

AacLatticeProofReason = Literal[
    # The leg adjudicated.
    "offset_concentration",
    "z_exceeded",
    "adjudicated_clean",
    # The leg could not adjudicate.
    "not_measured",
    "insufficient_scored_tracks",
]


@dataclass(frozen=True)
class AacLatticeProofLeg:
    """What the AAC frame-lattice leg asserts about one album (pure).

    Three outcomes, with the same discipline as ``UltrasonicProofLeg``:

    * ``denied``   — the album's tracks share an MDCT frame lattice.
                     Withhold proof.
    * ``passed``   — the detector scored enough tracks to run the
                     offset-concentration rule, and found no lattice.
    * ``withheld`` — the leg could not run. It asserts NOTHING; promotion
                     falls through to every pre-existing rule unchanged.

    ``withheld`` is where essentially the whole library sits and always
    will: the capture is gated to the promotion-plausible cohort because
    it costs tens of seconds of CPU per track, and every row measured
    before the capture shipped has no lattice evidence at any price.

    **``modal_offset`` is deliberately not a field here.** Absolute
    offsets are decode-path relative — a container whose decoder applies
    encoder-delay priming shifts the sample origin — so the literal 960
    (Apple) and 0 (ffmpeg-native) constants are not portable facts about
    an album. CONCENTRATION is, which is why the deployable rule counts
    coincidences instead of matching values, and why this leg cannot be
    "improved" by comparing an offset to a constant.
    """

    outcome: AacLatticeProofOutcome
    reason: AacLatticeProofReason
    scored_tracks: int = 0
    modal_count: int | None = None
    max_z: float | None = None

    @property
    def denies_promotion(self) -> bool:
        return self.outcome == "denied"

    @property
    def proves_no_aac_lattice(self) -> bool:
        return self.outcome == "passed"


def aac_lattice_proof_leg(
    capture: AacLatticeCapture | None,
) -> AacLatticeProofLeg:
    """Evaluate the AAC frame-lattice proof leg for one album (pure).

    The capture is measured by ``lib/aac_lattice.py`` and persisted on the
    candidate evidence row; this function is the only thing that reads it
    as policy. Two independent denial conditions, either sufficient:

    1. **Offset concentration** — ``modal_count >=
       AAC_LATTICE_PROOF_DENY_MODAL_COUNT``. Parameter-free, with an
       analytic false-positive floor of ~0.0023 albums per 5000, and it
       closes the entire Apple/CoreAudio family, which every spectral leg
       is blind to by construction.
    2. **Sweep contrast** — ``max_z > AAC_LATTICE_PROOF_DENY_MAX_Z``.
       Catches a laundered album whose per-track offsets scattered (the
       ``ffmpeg``-native AAC shape) but whose sweep still spikes.

    A denial fires on ANY amount of scored evidence: ``modal_count`` is
    bounded above by ``scored_tracks``, so condition 1 implies four scored
    tracks by construction, but condition 2 does not and must not — one
    track at z=28 is evidence, and refusing to read it because five others
    failed to decode would fail OPEN, which is the wrong direction for a
    proof gate.

    A clean result, by contrast, needs enough evidence to be worth
    anything: below ``AAC_LATTICE_PROOF_MIN_SCORED_TRACKS`` successfully
    scored tracks the concentration rule could not have fired whatever the
    audio was, so "it did not fire" is not a finding. That world withholds.

    Withheld asserts nothing in either direction — never evidence, never
    denial — exactly like the ultrasonic leg's NULL states.
    """
    if capture is None:
        return AacLatticeProofLeg(outcome="withheld", reason="not_measured")
    scored = capture.scored_tracks
    modal_count = capture.modal_count
    max_z = capture.max_z
    if (
        modal_count is not None
        and modal_count >= AAC_LATTICE_PROOF_DENY_MODAL_COUNT
    ):
        return AacLatticeProofLeg(
            outcome="denied",
            reason="offset_concentration",
            scored_tracks=scored,
            modal_count=modal_count,
            max_z=max_z,
        )
    if max_z is not None and max_z > AAC_LATTICE_PROOF_DENY_MAX_Z:
        return AacLatticeProofLeg(
            outcome="denied",
            reason="z_exceeded",
            scored_tracks=scored,
            modal_count=modal_count,
            max_z=max_z,
        )
    if scored < AAC_LATTICE_PROOF_MIN_SCORED_TRACKS:
        return AacLatticeProofLeg(
            outcome="withheld",
            reason="insufficient_scored_tracks",
            scored_tracks=scored,
            modal_count=modal_count,
            max_z=max_z,
        )
    return AacLatticeProofLeg(
        outcome="passed",
        reason="adjudicated_clean",
        scored_tracks=scored,
        modal_count=modal_count,
        max_z=max_z,
    )


def v0_probe_overrides_spectral(probe: V0ProbeEvidence | None) -> bool:
    """Decide whether a V0 probe is strong enough to override a suspect
    spectral grade and certify the source as genuine lossless.

    Only ``lossless_source_v0`` probes are eligible — research probes
    (``native_lossy_research_v0``, ``on_disk_research_v0``) carry no
    policy weight here.
    """
    if not is_comparable_lossless_source_probe(probe):
        return False
    assert probe is not None  # narrowed by the helper above
    avg = probe.avg_bitrate_kbps
    mn = probe.min_bitrate_kbps
    if avg is None or mn is None:
        return False
    return avg >= V0_OVERRIDE_AVG_KBPS and mn >= V0_OVERRIDE_MIN_KBPS


def determine_verified_lossless(
    target_format: str | None,
    spectral_grade: str | None,
    converted_count: int,
    is_transcode: bool,
    v0_probe: V0ProbeEvidence | None = None,
    *,
    has_lossy_passthrough: bool = False,
    ultrasonic_leg: UltrasonicProofLeg | None = None,
    aac_lattice_leg: AacLatticeProofLeg | None = None,
) -> bool:
    """Single source of truth for verified lossless status (pure).

    Two paths, both requiring affirmative spectral evidence:
    1. target_format="lossless"/"flac" (lossless kept on disk): verified when
       spectral says genuine or marginal.
    2. Default (lossless→V0/target): verified when lossless files were
       converted and spectral affirmatively says genuine or marginal.

    V0-avg trust override (issue #205-style — Bill Hicks): in either path,
    when spectral disagrees with V0 evidence (suspect/likely_transcode but a
    ``lossless_source_v0`` probe at avg≥230kbps AND min≥200kbps), trust
    the V0 probe and certify as verified. The override is monotonic — it
    only flips False→True, never True→False.

    Mixed-source guard (``has_lossy_passthrough``): when the source folder
    contains lossless audio AND audio that will pass through unconverted
    (e.g. 15 FLAC + 2 MP3 bonus tracks), the album can never be
    verified-lossless regardless of converted_count / spectral / V0. The
    decision layer rejects these sources outright via
    ``preimport_mixed_source`` in ``full_pipeline_decision_from_evidence``;
    this argument is the harness-side defense in depth so the persisted
    candidate measurement field is honest even on the never-imported row.

    Proof gate v3 (``ultrasonic_leg``, issue #829 Phase 5 PR3): a
    ``denied`` leg is a hard veto, ahead of every other rule INCLUDING
    the V0-avg trust override. The ordering is deliberate. The V0 probe
    was tuned to rescue spoken-word and other HF-poor lossless from a
    false ``suspect`` grade; it is measured NOT to separate the
    FLAC-container launder classes this leg exists to catch
    (``docs/research/calibration-data/probe_pair.tsv.gz``, 5,670 files:
    only mp3-128 separates, and the cliff leg already catches that one).
    Letting the override outrank the leg would therefore reopen the exact
    hole, in exchange for rescuing albums whose only cost is staying on
    the search surface.

    A ``withheld`` or absent leg changes nothing: every pre-v3 rule below
    applies unchanged. Omitting the argument entirely is the pre-v3
    behaviour by construction, which is what keeps a caller that has no
    ultrasonic evidence — most of the library — exactly where it was.

    Proof gate v4 (``aac_lattice_leg``, issue #829 AAC-lattice leg PR-B):
    a ``denied`` lattice leg is a hard veto in the same precedence
    position, and for the same reason. It measures the one thing every
    spectral instrument here is blind to — the MDCT frame lattice an AAC
    encoder leaves in the samples — so no spectral-derived rescue below,
    the V0-avg trust override included, can speak to the evidence it
    carries. The two legs are independent conditions on one proof: either
    denies alone, and neither can overrule the other's denial.
    """
    if has_lossy_passthrough:
        return False
    if ultrasonic_leg is not None and ultrasonic_leg.denies_promotion:
        return False
    if aac_lattice_leg is not None and aac_lattice_leg.denies_promotion:
        return False
    if spectral_grade in (None, "error"):
        return False
    spectral_affirms = spectral_grade in ("genuine", "marginal")
    spectral_disagrees = spectral_grade in (
        "suspect",
        "likely_transcode",
    )
    if target_format in ("flac", "lossless"):
        if spectral_affirms:
            return True
        return spectral_disagrees and v0_probe_overrides_spectral(v0_probe)
    if converted_count > 0 and spectral_affirms and not is_transcode:
        return True
    return bool(converted_count > 0 and spectral_disagrees and v0_probe_overrides_spectral(v0_probe))


#: The classifier a proof minted without the v3 ultrasonic leg carries —
#: the cliff/grade gate alone, which is what every proof in the library
#: before issue #829 Phase 5 PR3 was minted under.
VERIFIED_LOSSLESS_CLASSIFIER = "spectral_verified_lossless"

#: The classifier a proof carries when the v3 ultrasonic deficit leg
#: actually RAN on a comparable measurement and found nothing.
VERIFIED_LOSSLESS_CLASSIFIER_V3 = "spectral_verified_lossless_v3"

#: The classifier a proof carries when BOTH the v3 ultrasonic leg AND the
#: AAC frame-lattice leg actually RAN and found nothing. The lattice leg
#: is the only instrument here that can see the Apple/CoreAudio family —
#: v3's named blind spot — so a row that cleared both was tested for a
#: strictly larger class of laundering than one that cleared v3 alone.
VERIFIED_LOSSLESS_CLASSIFIER_V4 = "spectral_verified_lossless_v4"


def mint_verified_lossless_proof(
    will_be_verified_lossless: bool,
    *,
    was_converted_from: str | None,
    detected_source_format: str | None,
    spectral_grade: str | None,
    ultrasonic_leg: UltrasonicProofLeg | None = None,
    aac_lattice_leg: AacLatticeProofLeg | None = None,
) -> VerifiedLosslessProof | None:
    """Mint the measured verified-lossless proof for a harness attempt (pure).

    Single policy owner for proof construction: the harness supplies only
    measured facts. ``source`` prefers the conversion's original filetype
    (the lossless input actually consumed, e.g. ``flac``) over the detected
    on-disk source format; both normalise to lowercase, and an undetectable
    format (``UNKNOWN``) falls through to the ``lossless_source`` sentinel
    rather than minting a proof sourced to "unknown".

    ``classifier`` names WHICH MODEL proved the row, and issue #829 Phase
    5 PR3 makes that distinction load-bearing rather than decorative. The
    v3 name is minted only when the ultrasonic leg ADJUDICATED and passed
    — not merely because v3 code ran. A leg that withheld (no measurement,
    a legacy row, an R19 preserved source, an uncalibrated decode path)
    proves nothing new, and stamping v3 on it would make the column mean
    two things again, which is exactly the ambiguity it exists to remove.

    The claim the v3 name carries is bounded (Phase 5 plan §1.7): "no
    evidence of lossy origin was found by the tests we have", never "this
    is bit-faithful to a lossless source". The named blind spot is the
    Apple/CoreAudio family, which applies essentially no lowpass in the
    measured band and which no spectral instrument separates.

    The v4 name (issue #829 AAC-lattice leg PR-B) is exactly that blind
    spot closing, and it composes on the same rule rather than replacing
    it — the classifier names which MODELS ran, so:

        ultrasonic   lattice     classifier
        passed       passed      v4
        passed       withheld    v3
        anything else            the base name

    A lattice pass with no ultrasonic adjudication is deliberately the
    BASE name, not v4 and not some v4-minus: the names are a ladder of
    what was tested, and claiming the top rung for a row that skipped a
    rung would make the column mean two things again. A DENIED leg never
    reaches here at all — the proof was already vetoed.

    ``spectral_measurement_version`` is deliberately NOT used as this
    axis: it is a measurement-shape version, and 47 live proofs carry
    version 2 while having been proved under the OLD gate — 7 of them
    rows v3 denies.
    """
    if not will_be_verified_lossless:
        return None
    detected = (detected_source_format or "").strip().lower()
    if detected == "unknown":
        detected = ""
    source = (
        (was_converted_from or "").strip().lower()
        or detected
        or "lossless_source"
    )
    ultrasonic_adjudicated = (
        ultrasonic_leg is not None
        and ultrasonic_leg.proves_ultrasonic_content
    )
    lattice_adjudicated = (
        aac_lattice_leg is not None
        and aac_lattice_leg.proves_no_aac_lattice
    )
    if ultrasonic_adjudicated and lattice_adjudicated:
        classifier = VERIFIED_LOSSLESS_CLASSIFIER_V4
    elif ultrasonic_adjudicated:
        classifier = VERIFIED_LOSSLESS_CLASSIFIER_V3
    else:
        classifier = VERIFIED_LOSSLESS_CLASSIFIER
    return VerifiedLosslessProof(
        provenance=EVIDENCE_PROVENANCE_MEASURED,
        source=source,
        classifier=classifier,
        detail=spectral_grade,
    )


def is_verified_lossless(was_converted: bool, original_filetype: str | None,
                         spectral_grade: str | None) -> bool:
    """Legacy derivation for album_source.py fallback path.

    Used when import_one.py didn't set verified_lossless_override
    (old download_log rows). Delegates to determine_verified_lossless
    for the standard (non-FLAC-on-disk) case.

    Stricter than determine_verified_lossless: requires spectral_grade="genuine"
    exactly, and validates the original filetype was lossless.
    """
    if not was_converted or original_filetype is None or spectral_grade != "genuine":
        return False
    return original_filetype.lower() in _LOSSLESS_EXTS


QualityGateDecision = Literal["accept", "requeue_upgrade", "requeue_lossless"]


@dataclass(frozen=True)
class PostImportSearchAction:
    """Canonical state transition for a retained automatic import.

    ``search_filetype_override=None`` deliberately means the ordinary full
    search surface, including the catch-all lane for codecs the rank model
    does not know.  Lossless narrowing is the sole non-null policy override.
    """

    status: Literal["imported", "wanted"]
    search_filetype_override: Literal["lossless"] | None
    denylist: bool


_POST_IMPORT_SEARCH_ACTIONS: dict[str, PostImportSearchAction] = {
    "accept": PostImportSearchAction("imported", None, False),
    "requeue_lossless": PostImportSearchAction("wanted", "lossless", True),
    "requeue_upgrade": PostImportSearchAction("wanted", None, True),
    DECISION_PROVISIONAL_LOSSLESS_UPGRADE: PostImportSearchAction(
        "wanted", "lossless", True
    ),
    "transcode_upgrade": PostImportSearchAction("wanted", None, True),
    "transcode_first": PostImportSearchAction("wanted", None, True),
}


def post_import_search_action(decision: str) -> PostImportSearchAction:
    """Map every retained-import decision to status, scope, and exclusion.

    This is the one policy owner shared by the post-import gate, the import
    dispatch tail, and the simulator.  Kept lossy/provisional sources are
    denylisted because another offer from the same peer cannot improve the
    copy already on disk.
    """

    action = _POST_IMPORT_SEARCH_ACTIONS.get(decision)
    if action is None:
        raise ValueError(f"unknown retained-import decision: {decision}")
    return action


def post_import_search_action_if_known(
    decision: str,
) -> PostImportSearchAction | None:
    """Return automatic retained-import policy when ``decision`` owns one."""

    return _POST_IMPORT_SEARCH_ACTIONS.get(decision)


def quality_gate_decision(
    current: AudioQualityMeasurement,
    cfg: QualityRankConfig | None = None,
    *,
    target_contract: TargetQualityContract | None = None,
    verified_lossless_proof: bool = False,
) -> QualityGateDecision:
    """Choose post-import search policy from proof and measured authority.

    Verified-lossless proof is the only terminal boundary.  A transparent
    copy with a genuine spectral grade narrows to lossless-only regardless
    of the grade's subject label (decision 17): for an unconverted lossy
    import the source-subject grade describes the installed bytes, and
    out-of-band mutation is outside the state model.  Every other retained
    copy stays wanted on the full search surface.

    Args:
        current: measurement of the files now on disk (from beets DB + spectral)
        cfg: QualityRankConfig. Defaults to QualityRankConfig.defaults().
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()

    if verified_lossless_proof:
        return "accept"
    rank = gate_rank(
        current,
        cfg,
        target_contract=target_contract,
        verified_lossless_proof=verified_lossless_proof,
    )
    if (
        rank == QualityRank.TRANSPARENT
        and current.spectral_grade == "genuine"
    ):
        return "requeue_lossless"
    return "requeue_upgrade"
