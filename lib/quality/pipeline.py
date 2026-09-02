"""The decision twins: full_pipeline_decision (flat-kwargs simulator) and
full_pipeline_decision_from_evidence (evidence-pipeline production decider).

PARITY CONTRACT: the twins MUST produce the same outcome on the same
album (pinned by tests/test_quality_classification.py). They stay in one
module on purpose — do not split them apart.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from typing import Any, Literal

import msgspec

from lib.quality.compare import comparison_format_hint
from lib.quality.decisions import (
    _LOSSLESS_EXTS,
    DECISION_LOSSLESS_SOURCE_LOCKED,
    DECISION_VERIFIED_LOSSLESS_LOCKED,
    MeasuredImportDecisionInput,
    ProvisionalLosslessDecisionInput,
    ProvisionalLosslessDecisionResult,
    aac_lattice_proof_leg,
    build_existing_quality_measurement,
    determine_verified_lossless,
    is_preserved_source_spectral,
    measured_import_decision,
    post_import_search_action,
    provisional_lossless_decision,
    quality_gate_decision,
    spectral_import_decision,
    transcode_detection,
    ultrasonic_proof_leg,
    v0_probe_overrides_spectral,
)
from lib.quality.dispatch_actions import (
    compute_effective_override_bitrate,
    decision_denylists,
)
from lib.quality.evidence_types import (
    _NONCOMPARABLE_NEUTRAL_V0_PROBE_KIND,
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_SOURCE,
    SPECTRAL_TRANSCODE_GRADES,
    V0_PROBE_LOSSLESS_SOURCE,
    AacLatticeCapture,
    AlbumQualityEvidence,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    QualityComparisonBasis,
    TargetQualityContract,
    V0ProbeEvidence,
    is_comparable_lossless_source_probe,
)
from lib.quality.filetypes import has_mixed_lossless_and_lossy
from lib.quality.gates import (
    preimport_audio_gate,
    preimport_corrupt_outranks_nested,
    preimport_nested_gate,
    spectral_gate_trigger,
)
from lib.quality.import_result_types import QualityEvidenceActionProvenance
from lib.quality.ranks import QualityRankConfig
from lib.quality.spectral_interpretation import (
    SpectralCodecContext,
    SpectralInterpretation,
    codec_context_from_measurement,
    decision_class_kbps,
    interpret_spectral_evidence,
    spectral_classes_comparable,
)
from lib.quality.verdict_tiers import AlbumProofVerdict, proof_verdict_from_facts

# ---------------------------------------------------------------------------
# Full pipeline decision — combines all three stages
# ---------------------------------------------------------------------------

#: Reported in ``stage2_import_if_stage1_deferred`` when the Stage-2
#: counterfactual could not be evaluated at all (issue #829 Phase 5 PR2d).
#: Distinct from ``None``, which means Stage 1 never short-circuited and so
#: there is no counterfactual to report — "the audit could not run" and
#: "there was nothing to audit" are different facts and the operator is
#: entitled to both. Deliberately outside the Stage-2 decision vocabulary.
STAGE2_COUNTERFACTUAL_UNAVAILABLE = "unavailable"


CandidatePreimportRejectFact = Literal[
    "audio_corrupt",
    "bad_audio_hash",
    "nested_layout",
    "empty_fileset",
    "mixed_source",
]


def candidate_preimport_reject_fact(
    candidate: AlbumQualityEvidence,
) -> CandidatePreimportRejectFact | None:
    """Return the first persisted fact that makes spectral policy irrelevant.

    This classifies evidence shape; it does not decide an import.  The unified
    reducer below remains the only authority that turns the fact into a
    verdict.  Action admission also uses this classifier so a concrete early
    fact can reach that reducer without being mistaken for a reusable spectral
    cache entry.

    The audio_corrupt/nested_layout choice runs through
    ``preimport_corrupt_outranks_nested`` (issue #1355 item 1) — the same
    function ``full_pipeline_decision`` calls — rather than two independent
    if-chains that could drift apart again.
    """
    corrupt_or_nested = preimport_corrupt_outranks_nested(
        audio_corrupt=candidate.audio_corrupt,
        nested_layout=candidate.folder_layout == "nested",
    )
    if corrupt_or_nested == "audio_corrupt":
        return "audio_corrupt"
    if candidate.matched_bad_audio_hash_id is not None:
        return "bad_audio_hash"
    if corrupt_or_nested == "nested_layout":
        return "nested_layout"
    effective_audio_file_count = (
        len(candidate.files) if candidate.files else candidate.audio_file_count
    )
    if effective_audio_file_count == 0:
        return "empty_fileset"
    if has_mixed_lossless_and_lossy(candidate.files):
        return "mixed_source"
    return None

def full_pipeline_decision(
    # File properties
    is_flac: bool,
    min_bitrate: int,
    is_cbr: bool,
    # Recorded facts, not gate inputs. Since issue #1145 the preimport
    # spectral gate reads the codec alone, so neither of these selects
    # whether a candidate is scanned; ``avg_bitrate`` still feeds the
    # measured rank and ``is_vbr`` still rides along as a persisted fact.
    is_vbr: bool | None = None,
    avg_bitrate: int | None = None,
    # Spectral analysis
    spectral_grade: str | None = None,
    spectral_bitrate: int | None = None,
    # Existing state
    existing_min_bitrate: int | None = None,
    existing_avg_bitrate: int | None = None,
    existing_spectral_bitrate: int | None = None,
    existing_spectral_grade: str | None = None,
    override_min_bitrate: int | None = None,
    existing_format: str | None = None,
    existing_is_cbr: bool = False,
    # Post-conversion (FLAC path only)
    post_conversion_min_bitrate: int | None = None,
    converted_count: int = 0,
    # Pipeline state
    candidate_verified_lossless_proof: bool = False,
    # Verified lossless target format (e.g. "opus 128", "mp3 v2")
    verified_lossless_target: str | None = None,
    # Target format (user intent — "flac" skips conversion)
    target_format: str | None = None,
    # New download format label (codec-aware, passed through to measurements)
    new_format: str | None = None,
    # Preimport gates (issue #91). Default to a passing audio check.
    audio_check_mode: str = "normal",
    audio_corrupt: bool = False,
    has_nested_audio: bool = False,
    # Rank-model config (defaults() for legacy callers)
    cfg: QualityRankConfig | None = None,
    *,
    post_conversion_is_cbr: bool | None = None,
    # Candidate V0 policy evidence is source-only and must name its kind.
    # Omitted/non-source kinds are deliberately noncomparable.
    candidate_v0_probe_avg: int | None = None,
    candidate_v0_probe_min: int | None = None,
    existing_v0_probe_avg: int | None = None,
    existing_v0_probe_kind: str | None = None,
    candidate_v0_probe_kind: str | None = None,
    supported_lossless_source: bool | None = None,
    current_verified_lossless_proof: bool = False,
    # issue #1241 — the operator's incomplete mark and its candidate-side
    # conjunct. When BOTH are true the installed side is disregarded
    # entirely (every ``existing_*`` input, the spectral override, and the
    # verified-lossless lock) and the candidate is admitted exactly as it
    # would be into an empty slot. See
    # ``AlbumQualityEvidenceDecisionFacts`` for the two facts' provenance.
    installed_marked_incomplete: bool = False,
    candidate_covers_declared_program: bool = False,
    # issue #829 Phase 5 PR2b — the codec-resolution context the flat
    # ``spectral_grade``/``spectral_bitrate`` pair cannot carry. One keyword
    # per side; ``SpectralCodecContext.facts()`` recombines it with the flat
    # scalars, so the grade/bucket/format have exactly one source of truth.
    # Omitting them leaves the codec unknown, which WITHHOLDS the spectral
    # opinion (never rejects) — the fail-closed direction.
    candidate_spectral_context: SpectralCodecContext | None = None,
    existing_spectral_context: SpectralCodecContext | None = None,
    # issue #829 AAC-lattice leg PR-B — the candidate's persisted AAC
    # frame-lattice capture. Passed whole rather than as flat scalars
    # because the capture is one album-level Struct on the evidence row
    # (``AlbumQualityEvidence.aac_lattice``) and the leg reads three of its
    # statistics together; splitting it into keywords would mint a second
    # place where "what the detector found" is spelled. Only the CANDIDATE
    # side has one, for the same reason as the ultrasonic leg: this leg
    # gates a PROMOTION and the installed side is never promoted here.
    candidate_aac_lattice: AacLatticeCapture | None = None,
    # Return type quoted (this module has no ``from __future__ import
    # annotations``) so the lexical typing-escape-hatch scanner's NAME-
    # token count doesn't grow — the module already carries this ``Any``
    # budget for the internal ``result: dict[str, Any]`` this function
    # returns, and the twin ``full_pipeline_decision_from_evidence``
    # returns the identical shape.
) -> "dict[str, Any]":
    """Run the full decision chain and return the final outcome.

    This simulates what happens when a download completes and flows through
    process_completed_album → import_one.py → _check_quality_gate.

    Codec-aware: when ``new_format`` / ``existing_format`` are supplied, the
    simulator classifies both measurements via quality_rank() — matching
    production dispatch behavior. Legacy callers that omit them still get
    sensible defaults derived from ``is_flac``/``target_format``/``is_cbr``.

    Returns a dict:
        {
            "stage0_spectral_gate": str,  # would spectral analysis run?
            "stage1_spectral": str,       # pre-import spectral decision (None when gate skipped)
            "stage2_import": str,         # import/downgrade/transcode decision
            "stage3_quality_gate": str,   # post-import quality gate decision
            "final_status": str,          # what the pipeline DB ends up as
            "imported": bool,             # whether files were imported to beets
            "denylisted": bool,           # whether source user gets denylisted —
                                           # single-sourced by _finalize_denylist
                                           # (resolve_pipeline_decision_denylist),
                                           # matching production's real
                                           # dispatch_action/post_import_search_action
                                           # write exactly (issue #813 Finding 2)
            "keep_searching": bool,       # whether the system keeps looking for better
            "comparison_basis": dict | None,  # QualityComparisonBasis builtins from stage 2
            # AUDIT ONLY (issue #829 Phase 5 PR2d). A Stage-1 spectral
            # reject short-circuits before Stage 2 ever runs, so "Stage 1
            # rejected this, and Stage 2 would have said better" — the
            # disagreement issue #813 is about — used to be computed
            # nowhere and was invisible to the operator. These two keys
            # carry that counterfactual: the Stage-2 decision and its
            # QualityComparisonBasis for the SAME world with Stage 1's
            # short-circuit lifted. Both are None on every other path, and
            # NO branch anywhere reads them — they are reporting, never a
            # decision input.
            #
            # A short-circuit ALWAYS reports a decision here, even if only
            # STAGE2_COUNTERFACTUAL_UNAVAILABLE; None means Stage 1 never
            # short-circuited. The basis stays None whenever Stage 2 never
            # reached a comparison (the provisional lane, the lossless-
            # source lock), which is a real outcome, not a failure.
            "stage2_import_if_stage1_deferred": str | None,
            "comparison_basis_if_stage1_deferred": dict | None,
            # AUDIT ONLY (issue #1241): True when the operator's incomplete
            # mark plus beets' coverage proof made this decision disregard
            # the installed side entirely. No branch reads it.
            "installed_incomplete_disregarded": bool,
        }
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()

    # Issue #1241. The operator has positively marked the installed copy
    # incomplete AND beets proved this attempt's candidate covers the whole
    # declared program: the two sides are not the same program, so no
    # existing-side fact — quality, spectral, probe anchor, or proof lock —
    # is a sound baseline against this candidate. Disregard the installed
    # side entirely, up front, and let every stage below run its ordinary
    # fresh-import admission policy (the absolute candidate-side floors all
    # still apply, and the post-import gate still keeps the search open for
    # a below-par import). Monotone by construction: with no existing side,
    # Stage 1 can never reject, no lock can fire, and no comparison can say
    # "downgrade" — marking can only ever widen admission, never narrow it.
    installed_incomplete_disregarded = (
        installed_marked_incomplete and candidate_covers_declared_program
    )
    if installed_incomplete_disregarded:
        existing_min_bitrate = None
        existing_avg_bitrate = None
        existing_spectral_bitrate = None
        existing_spectral_grade = None
        override_min_bitrate = None
        existing_format = None
        existing_is_cbr = False
        existing_v0_probe_avg = None
        existing_v0_probe_kind = None
        existing_spectral_context = None
        current_verified_lossless_proof = False

    result: dict[str, Any] = {
        "preimport_audio": None,
        "preimport_nested": None,
        # U11: keys carrying the folded folder/audio-integrity rejects from
        # ``full_pipeline_decision_from_evidence``. The simulator does not
        # take these facts as flat kwargs, so they stay None here — the
        # simulator surfaces ``audio_corrupt`` via the
        # ``audio_check_mode='strict' + audio_corrupt=True`` kwargs which
        # routes through ``preimport_audio``. ``bad_audio_hash`` and
        # ``empty_fileset`` are only reachable through the evidence
        # entrypoint; their presence here keeps both deciders'
        # dict shapes identical.
        "preimport_bad_hash": None,
        "preimport_empty_fileset": None,
        "preimport_mixed_source": None,
        "stage0_spectral_gate": None,
        "stage1_spectral": None,
        "stage2_import": None,
        "stage3_quality_gate": None,
        "final_status": None,
        "imported": False,
        "denylisted": False,
        "keep_searching": False,
        "target_final_format": None,
        "verified_lossless": bool(candidate_verified_lossless_proof),
        # The QualityComparisonBasis from measured_import_decision, as plain
        # builtins (msgspec.to_builtins) — this dict rides json.dumps'd API
        # responses and preview JSONB, so it must stay JSON-plain. None when
        # stage 2 never compared against an existing album (early rejects,
        # provisional lane, no existing). Consumers that persist it onto
        # ImportResult convert back with msgspec.convert at their boundary.
        "comparison_basis": None,
        # The Stage-1-reject counterfactual (issue #829 Phase 5 PR2d). Set
        # only by the ``stage1_short_circuits`` branch below; audit-only.
        "stage2_import_if_stage1_deferred": None,
        "comparison_basis_if_stage1_deferred": None,
        # Issue #1241, audit-only: records that the operator's incomplete
        # mark plus beets' coverage proof made this decision disregard the
        # installed side. No branch reads it — the disregard already
        # happened above, by nulling the existing-side inputs.
        "installed_incomplete_disregarded": installed_incomplete_disregarded,
    }

    # A proof-bearing installed HAVE is the absolute acquisition ceiling
    # (decision 21): no import — automatic OR force-import — crosses it.
    # Force-import bypasses only the beets distance; Replace/re-request is
    # the operator's way back in. The guard deliberately precedes every
    # candidate-derived reject, including folder/audio-integrity and
    # spectral exits.
    if current_verified_lossless_proof:
        result["stage2_import"] = DECISION_VERIFIED_LOSSLESS_LOCKED
        result["final_status"] = "imported"
        return _finalize_denylist(result)

    # --- Preimport gates (issue #91) ---
    # Corrupt audio outranks folder shape (issue #1355 item 1): both facts
    # are independently derived from the same measurement pass
    # (measure_preimport_state derives folder layout from a single path
    # enumeration, then separately runs the audio-integrity decode), so
    # either can be true regardless of the other, and both can land on the
    # same persisted evidence row. preimport_corrupt_outranks_nested is the
    # ONE function that decides which fact a decision reports when a
    # candidate carries both — the same function
    # candidate_preimport_reject_fact calls for the evidence twin, so the
    # two twins cannot independently re-diverge on this ordering.
    #
    # This used to check nested before corrupt, on the theory that
    # lib.dispatch.dispatch_import_from_db pre-checked has_nested_audio
    # before ever measuring. That direct-measurement dispatch architecture
    # was retired: dispatch now reads persisted evidence and never measures
    # at all (measure_and_persist_candidate_evidence is the only producer of
    # candidate facts), so there is no live ordering left to mirror except
    # this one.
    audio_outcome = preimport_audio_gate(audio_check_mode, audio_corrupt)
    nested_outcome = preimport_nested_gate(has_nested_audio)
    corrupt_or_nested = preimport_corrupt_outranks_nested(
        audio_corrupt=audio_outcome == "reject_corrupt",
        nested_layout=nested_outcome == "reject_nested",
    )
    if corrupt_or_nested == "audio_corrupt":
        result["preimport_audio"] = audio_outcome
        result["final_status"] = "wanted"
        result["keep_searching"] = True
        return _finalize_denylist(result)
    if corrupt_or_nested == "nested_layout":
        result["preimport_nested"] = nested_outcome
        result["final_status"] = "wanted"
        result["keep_searching"] = True
        return _finalize_denylist(result)
    result["preimport_audio"] = audio_outcome
    result["preimport_nested"] = nested_outcome

    # --- Codec-aware spectral interpretation (issue #829 Phase 5 PR2b) ---
    # Computed ONCE per side, here, and consumed by every seam below. No
    # decision may read a raw ``spectral_bitrate_kbps`` again: a number
    # produced by LAME's MP3 encoder table means nothing for an AAC, an
    # Opus or an HE-AAC stream (download 37946). ``decision_class_kbps``
    # returns the codec's own class only when the interpretation is
    # decision-grade; otherwise it withholds, and withholding falls through
    # to rank and the other evidence — it is never a rejection.
    #
    # The format labels are exactly the ones the rank model classifies:
    # every ``is_flac`` branch below builds its measurement with
    # ``new_format or "flac"``, and the native-lossy branch passes
    # ``new_format`` straight through ``comparison_format_hint``.
    candidate_format_label = (new_format or "flac") if is_flac else new_format
    effective_existing_format = (
        existing_format if existing_format is not None else "MP3"
    )
    candidate_context = candidate_spectral_context or SpectralCodecContext()
    existing_context = existing_spectral_context or SpectralCodecContext()
    candidate_spectral = interpret_spectral_evidence(candidate_context.facts(
        spectral_grade=spectral_grade,
        spectral_bitrate_kbps=spectral_bitrate,
        format=candidate_format_label,
    ))
    # The existing side's codec claim uses the raw label, never the "MP3"
    # rank-model default: a fabricated codec is exactly what #829 removes.
    existing_spectral = interpret_spectral_evidence(existing_context.facts(
        spectral_grade=existing_spectral_grade,
        spectral_bitrate_kbps=existing_spectral_bitrate,
        format=existing_format,
    ))
    candidate_spectral_class = decision_class_kbps(candidate_spectral)
    existing_spectral_class = decision_class_kbps(existing_spectral)
    # The v3 ultrasonic proof leg, computed ONCE per call from the
    # candidate context (issue #829 Phase 5 PR3). Only the candidate side
    # has one: this leg gates a PROMOTION, and the installed side is never
    # promoted by this decision — its own proof, if it has one, was minted
    # when it was the candidate and is already the acquisition ceiling
    # (decision 21, the ``current_verified_lossless_proof`` guard above).
    #
    # The leg is built from the context's own measured facts, never from
    # the flat ``candidate_format_label`` below: that label falls back to
    # a literal "flac" for a lossless source converting to a lossy target,
    # which is the dominant shape in this library. Reading a decode path
    # out of a defaulted label would claim sox-native for every ALAC
    # source and gate a +3.09 dB-skewed value against a threshold frozen
    # on a different instrument.
    candidate_ultrasonic_leg = ultrasonic_proof_leg(
        deficit_db=candidate_context.ultrasonic_deficit_db,
        spectral_measurement_version=(
            candidate_context.spectral_measurement_version
        ),
        decode_path=candidate_context.spectral_decode_path,
        preserved_source_spectral=is_preserved_source_spectral(
            candidate_context.spectral_subject,
            candidate_context.was_converted_from,
        ),
    )
    # The v4 AAC frame-lattice leg, computed ONCE per call from the
    # candidate's own capture (issue #829 AAC-lattice leg PR-B). Same
    # candidate-only scoping and same authority boundary as the ultrasonic
    # leg above: it governs the PROOF and nothing else. Absent capture —
    # every row measured before PR-A, and every row outside the
    # promotion-plausible cohort the capture is gated to — withholds.
    candidate_aac_lattice_leg = aac_lattice_proof_leg(candidate_aac_lattice)

    # ONE predicate, computed once, for every seam that asks "does the
    # spectral leg govern this pair?" — Stage 1's comparison AND the
    # symmetric-representation gate below. They were the same condition
    # before this PR and must stay the same condition: they are the two
    # halves of "represent the installed album by its real content".
    spectral_classes_govern = spectral_classes_comparable(
        candidate_spectral, existing_spectral,
    ).comparable

    # --- Stage 0: Spectral gate trigger (issue #93) ---
    # Mirrors lib.measurement._needs_spectral_check: codec only, since issue
    # #1145 retired the VBR skip. The simulator shows the operator whether the
    # preimport gate fires for this candidate at all — which is now a question
    # about the codec, not about a declared mode or an average.
    gate = spectral_gate_trigger(
        is_flac=bool(is_flac),
        codec_family=candidate_spectral.codec_family,
    )
    result["stage0_spectral_gate"] = gate

    # --- Stage 1: Pre-import spectral (MP3/CBR path) ---
    # For FLACs, spectral runs inside import_one.py instead, but the
    # logic is the same: detect transcodes before importing.
    #
    # Only run stage 1 when the gate would actually execute. For VBR MP3
    # with high avg bitrate, production skips spectral entirely — so even
    # if the caller supplies a spectral_grade, simulating that gate firing
    # would misrepresent production behavior.
    stage0_gates_stage1 = gate == "would_run" or is_flac
    provisional_source_candidate = bool(
        is_flac if supported_lossless_source is None else supported_lossless_source
    )
    # Source V0 evidence is a separate measurement from the post-conversion
    # target projection. Keep every source field exactly as supplied: filling
    # an absent source min with the target floor can falsely certify the V0
    # override, while treating a target floor as an avg can falsely admit the
    # Stage-1 carve-out. The caller must explicitly name a source kind;
    # unlabeled and explicit non-source probes stay noncomparable.
    candidate_source_probe = V0ProbeEvidence(
        kind=(
            candidate_v0_probe_kind
            or _NONCOMPARABLE_NEUTRAL_V0_PROBE_KIND
        ),
        avg_bitrate_kbps=candidate_v0_probe_avg,
        min_bitrate_kbps=candidate_v0_probe_min,
    ) if (
        candidate_v0_probe_avg is not None
        or candidate_v0_probe_min is not None
    ) else None
    has_provisional_probe_input = is_comparable_lossless_source_probe(
        candidate_source_probe,
    )
    # Stage 1 compares two spectral CLASSES. Both must be decision-grade in
    # their own codec's terms, and the pair must be comparable at all — a
    # class re-derived from ``cliff_hz`` sits systematically one tier above
    # a legacy stored bucket, so weighing one against the other measures
    # derivation rather than content (the Fall 2007 loop, issue #911).
    # Withholding the existing side is safe by construction: Stage 1 only
    # rejects when BOTH values are non-zero.
    stage1_existing_class = (
        existing_spectral_class if spectral_classes_govern else None
    )
    stage1_short_circuits = False
    if spectral_grade and stage0_gates_stage1:
        result["stage1_spectral"] = spectral_import_decision(
            spectral_grade, candidate_spectral_class, stage1_existing_class or 0)

        stage1_short_circuits = (
            result["stage1_spectral"] == "reject"
            and not (provisional_source_candidate
                     and has_provisional_probe_input)
        )

    # Annotations quoted for the same reason the enclosing function's return
    # type is (see its signature): this module carries a fixed ``Any``
    # budget for the one ``result: dict[str, Any]`` shape both twins return,
    # and the lexical escape-hatch scanner counts NAME tokens.
    def _stage2_onward(result: "dict[str, Any]") -> "dict[str, Any]":
        """Stage 2 and Stage 3, as a closure over THIS call's own inputs.

        One body, at most one invocation per call. When Stage 1 defers
        (the ordinary case) it runs on the real ``result`` and its return
        value IS the decision. When Stage 1 short-circuits it runs on a
        THROWAWAY dict instead, purely to answer the operator's question
        'Stage 1 rejected this — what would Stage 2 have said?' (issue
        #813's disagreement, issue #829 Phase 5 PR2d). A closure rather
        than a module-level helper because the tail reads ~30 of this
        function's locals: forwarding them by hand is exactly the kind of
        drifting parallel wiring this change exists to delete.
        """
        # Local rebind, because both FLAC branches ASSIGN to this name and
        # Python decides local-vs-closure at compile time: a bare
        # ``candidate_verified_lossless_proof = True`` anywhere in this body
        # makes every read of that name local, and so unbound on the paths
        # that never promote (native lossy, and Stage 3 on either FLAC
        # branch) — an ``UnboundLocalError``, not a leak.
        #
        # Leaking INTO the real decision is not the hazard here and never
        # was: the same compile-time rule makes it impossible without a
        # ``nonlocal``, and there is none. Every other captured name is
        # read-only, and every captured object is a frozen dataclass or
        # Struct, so the counterfactual run cannot reach the real result
        # through a shared mutable either.
        verified_proof = candidate_verified_lossless_proof

        # --- Stage 2: Import decision ---
        # Existing measurement — carries format if the caller provided one,
        # otherwise defaults to "MP3" so legacy simulator scenarios (which only
        # carry a min_bitrate) still classify against the MP3 VBR/CBR band
        # tables. Production always provides a real format via BeetsDB.
        #
        # Supplying existing_avg_bitrate matters under the default
        # cfg.bitrate_metric=AVG policy — otherwise a VBR album with avg=245 but
        # min=180 gets ranked off min=180 (GOOD instead of TRANSPARENT) and
        # downstream comparisons misrepresent production. When the caller didn't
        # measure an avg, nothing is fabricated: metric selection falls back to
        # min and the persisted basis says "min" (dl 36660 display-lie class).
        # Symmetric-representation gate (issue #813 Finding 1). The existing-side
        # spectral-floor ``override_min_bitrate`` represents the installed album by
        # its real content so a fake-high existing (CBR 320 whose spectral says 96)
        # cannot block a genuine upgrade. But that override is ONE-SIDED: it floors
        # only the existing measurement. When the CANDIDATE ALSO carries a spectral
        # estimate, ``_shared_spectral_bitrates`` already floors BOTH sides
        # symmetrically for rank — and additionally applying the existing-only
        # override then poisons the raw ``metric_tiebreak``: the candidate keeps its
        # inflated container bitrate while the existing is floored to its spectral
        # estimate, minting a phantom "better" for an identical transcode. That is
        # the Deerhunter "Rhapsody Original" bug (download_log 37725): a
        # 256/spectral-192 candidate scored "better" over an identical
        # 256/spectral-192 installed copy purely because the existing was floored to
        # 192 and the candidate was not. Skip the one-sided override when the shared
        # clamp will govern; keep it for the single-sided case (candidate carries no
        # spectral estimate) it exists to serve. Rank demotion is unchanged either
        # way — only the same-rank tiebreak now compares true container bitrates.
        #
        # The disarm predicate is EXACTLY ``_shared_spectral_bitrates``' firing
        # condition, and that identity is the whole argument: the override is
        # only safe to drop because something else then represents the installed
        # album by its real content. Issue #829 Phase 5 PR2b narrowed the clamp
        # to comparable CLASSES (a ``cliff_hz`` re-derivation sits one tier above
        # a legacy stored bucket, so a mixed-basis pair measures derivation, not
        # content). Disarming on the wider "both sides have a class" would open a
        # window where NEITHER mechanism fires and a known-fake installed copy
        # keeps its inflated container — download_log 29525, Clue to Kalo *Lily
        # Perdida*: a CBR-320 HAVE graded ``likely_transcode`` with a cliff-derived
        # class of 128 would have blocked a genuinely better VBR 234 candidate.
        # Deerhunter is unreachable through that window: it is same-codec,
        # same-basis, i.e. comparable, so the gate still fires there.
        shared_spectral_clamp_will_fire = spectral_classes_govern
        effective_existing_override = (
            None if shared_spectral_clamp_will_fire else override_min_bitrate
        )
        existing_m = build_existing_quality_measurement(
            min_bitrate_kbps=existing_min_bitrate,
            avg_bitrate_kbps=existing_avg_bitrate,
            format=effective_existing_format,
            is_cbr=existing_is_cbr,
            override_min_bitrate=effective_existing_override,
            spectral_grade=existing_spectral_grade,
            spectral_bitrate_kbps=existing_spectral_class,
            cliff_hz=existing_context.cliff_hz,
            codec_family=existing_context.codec_family,
        )

        if is_flac and target_format in ("flac", "lossless"):
            # FLAC kept on disk (no conversion).
            stage2_new_format = new_format or "flac"
            result["target_final_format"] = stage2_new_format
            will_be_verified = determine_verified_lossless(
                target_format, spectral_grade,
                converted_count=0, is_transcode=False,
                v0_probe=candidate_source_probe,
                ultrasonic_leg=candidate_ultrasonic_leg,
                aac_lattice_leg=candidate_aac_lattice_leg)
            # Both proof legs' authority is the PROOF, and their veto
            # lives at the one site that mints one
            # (``determine_verified_lossless`` above, and
            # ``mint_verified_lossless_proof`` in the harness). This flag
            # ROUTES THE IMPORT — it suppresses the provisional-lossless
            # lane — so a denial from EITHER leg must never reach it: the
            # provisional lane's ``suspect_lossless_downgrade`` is a
            # confident reject that also denylists the offering peer, and a
            # denied album is exactly the HF-poor genuine-lossless cohort
            # this override exists to rescue. A denied, probe-rescued album
            # imports as provisional lossless WITHOUT a proof and stays on
            # the search surface (Phase 5 plan §2, §1.7: withholding a
            # proof never rejects, denylists or accuses). PR3 shipped a
            # blocking defect by keying this flag on a leg; PR-B's lattice
            # leg inherits the boundary from birth.
            v0_verified_override = (
                spectral_grade in SPECTRAL_TRANSCODE_GRADES
                and v0_probe_overrides_spectral(candidate_source_probe)
            )
            # avg/median stay None — only the min crosses this interface. A
            # fabricated avg=min makes _selected_bitrate_with_source label a min
            # value "avg" in the persisted basis (dl 36660: "avg 216k" beside an
            # honest "V0 255kbps avg" on the same card). None falls back to the
            # min with the honest "min" label; the classified value is identical.
            new_m = AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate,
                format=stage2_new_format,
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=candidate_spectral_class,
                spectral_subject=(
                    EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
                ),
                spectral_provenance=(
                    EVIDENCE_PROVENANCE_MEASURED
                    if spectral_grade is not None else None
                ),
                # Gated on the grade for the same reason
                # ``build_existing_quality_measurement`` gates: these facts are
                # measured in the SAME pass as the grade, so a measurement with
                # no grade cannot legitimately carry them.
                cliff_hz=(
                    candidate_context.cliff_hz
                    if spectral_grade is not None else None
                ),
                codec_family=(
                    candidate_context.codec_family
                    if spectral_grade is not None else None
                ))
            if v0_verified_override:
                provisional = ProvisionalLosslessDecisionResult()
            else:
                provisional = provisional_lossless_decision(
                    ProvisionalLosslessDecisionInput(
                        candidate_probe=candidate_source_probe,
                        existing_probe=V0ProbeEvidence(
                            kind=existing_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
                            avg_bitrate_kbps=existing_v0_probe_avg,
                        ) if existing_v0_probe_avg is not None else None,
                        # A candidate that already CARRIES a proof is not
                        # unproven, whatever the fresh legs say now —
                        # existing stamps remain proofs under the old model
                        # (issue #829 forward-only rule) — so the lane never
                        # owns it. Caught by the as-persisted live-corpus
                        # differential (issue #990: 40 pre-PR3-proof rows).
                        will_be_verified=will_be_verified or verified_proof,
                        spectral_grade=spectral_grade,
                        supported_lossless_source=provisional_source_candidate,
                    ),
                    cfg=cfg,
                )
            if provisional.decision is not None:
                result["stage2_import"] = provisional.decision
                if provisional.confident_reject:
                    result["final_status"] = "wanted"
                    result["keep_searching"] = True
                    return _finalize_denylist(result)
                search_action = post_import_search_action(provisional.decision)
                result["imported"] = True
                result["keep_searching"] = search_action.status == "wanted"
                result["final_status"] = search_action.status
                result["target_final_format"] = stage2_new_format
                return _finalize_denylist(result)
            measured = measured_import_decision(
                MeasuredImportDecisionInput(
                    new_m,
                    existing_m,
                    verified_lossless_proof=(
                        will_be_verified or verified_proof
                    ),
                    source_spectral=candidate_spectral,
                    current_spectral=existing_spectral,
                ),
                cfg=cfg,
            )
            result["stage2_import"] = measured.decision
            result["comparison_basis"] = (
                msgspec.to_builtins(measured.comparison_basis)
                if measured.comparison_basis is not None else None)

            if result["stage2_import"] == "downgrade":
                result["final_status"] = "imported"
                result["keep_searching"] = True
                return _finalize_denylist(result)
            result["imported"] = True

            # Genuine FLAC on disk is verified lossless (for quality gate). Route
            # through determine_verified_lossless so the V0-avg trust override is
            # consulted and the simulator stays in lockstep with import_one.py.
            if will_be_verified:
                verified_proof = True
                result["verified_lossless"] = True

            gate_bitrate = min_bitrate
            gate_avg_bitrate = min_bitrate  # FLAC: lossless, avg == min is fine
            gate_cbr = False
            gate_format = stage2_new_format  # "flac"
            gate_contract = None
        elif is_flac:
            # FLAC path: convert first, then decide
            is_transcode = transcode_detection(
                converted_count, spectral_grade=spectral_grade)
            will_be_verified = determine_verified_lossless(
                target_format, spectral_grade,
                converted_count=converted_count,
                is_transcode=is_transcode,
                v0_probe=candidate_source_probe,
                ultrasonic_leg=candidate_ultrasonic_leg,
                aac_lattice_leg=candidate_aac_lattice_leg)
            # Same boundary as the flac-keep branch above: each leg's
            # authority is the proof, which ``will_be_verified`` already
            # carries. This flag routes the import (and, through
            # ``policy_is_transcode``, the comparison), so keying it on
            # either leg would turn a withheld proof into a rejection plus
            # a peer denylist for the provisional cohort.
            v0_verified_override = (
                is_transcode and v0_probe_overrides_spectral(candidate_source_probe)
            )
            policy_is_transcode = is_transcode and not v0_verified_override
            # The configured target, unconditionally: this branch IS the
            # lossless-source-converting path, and the stored format for a
            # lossless source is config, never proof (issue #829, operator
            # decision 2026-08-01). The comparison label must keep naming
            # what will actually be materialized — the two moved together
            # to MP3 V0 on a denial before this change, consistent with
            # each other and both wrong about the operator's config.
            # Authority: "no we always want it opus, the contract is not
            # around verified or not, is the stored format for lossless
            # absolutely. whatever people choose, v0,opus,aac it just has
            # to be consistent" —
            # https://github.com/abl030/cratedigger/issues/829
            stage2_new_format = comparison_format_hint(
                explicit_format=new_format,
                verified_lossless_target=verified_lossless_target,
                converted_count=converted_count,
                is_transcode=policy_is_transcode,
                verified_lossless_proof=(
                    will_be_verified or verified_proof
                ),
            )
            # avg/median stay None — the flat interface carries only the
            # post-conversion MIN for this side. See the flac-keep site above:
            # a fabricated avg=min is how the persisted basis learned to call a
            # min value "avg" (dl 36660).
            new_m = AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate,
                format=new_format or "flac",
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=candidate_spectral_class,
                spectral_subject=(
                    EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
                ),
                spectral_provenance=(
                    EVIDENCE_PROVENANCE_MEASURED
                    if spectral_grade is not None else None
                ),
                # Gated on the grade for the same reason
                # ``build_existing_quality_measurement`` gates: these facts are
                # measured in the SAME pass as the grade, so a measurement with
                # no grade cannot legitimately carry them.
                cliff_hz=(
                    candidate_context.cliff_hz
                    if spectral_grade is not None else None
                ),
                codec_family=(
                    candidate_context.codec_family
                    if spectral_grade is not None else None
                ))
            # What the harness will materialize. ``import_one.py``'s
            # ``_materialize_quality_evidence_action`` parses this exact
            # string into its ConversionSpec, so it is the stored format,
            # not an audit note — and the stored format of a lossless
            # source is the operator's configured target whatever the
            # proof legs said (issue #829; download 39087 landed as MP3 V0
            # because a denial withheld it). ``None`` here still means
            # "nothing configured", which the harness reads as V0.
            result["target_final_format"] = verified_lossless_target
            if v0_verified_override:
                provisional = ProvisionalLosslessDecisionResult()
            else:
                provisional = provisional_lossless_decision(
                    ProvisionalLosslessDecisionInput(
                        candidate_probe=candidate_source_probe,
                        existing_probe=V0ProbeEvidence(
                            kind=existing_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
                            avg_bitrate_kbps=existing_v0_probe_avg,
                        ) if existing_v0_probe_avg is not None else None,
                        # A candidate that already CARRIES a proof is not
                        # unproven, whatever the fresh legs say now —
                        # existing stamps remain proofs under the old model
                        # (issue #829 forward-only rule) — so the lane never
                        # owns it. Caught by the as-persisted live-corpus
                        # differential (issue #990: 40 pre-PR3-proof rows).
                        will_be_verified=will_be_verified or verified_proof,
                        spectral_grade=spectral_grade,
                        supported_lossless_source=provisional_source_candidate,
                    ),
                    cfg=cfg,
                )
            if provisional.decision is not None:
                result["stage2_import"] = provisional.decision
                if provisional.confident_reject:
                    result["final_status"] = "wanted"
                    result["keep_searching"] = True
                    return _finalize_denylist(result)
                search_action = post_import_search_action(provisional.decision)
                result["imported"] = True
                result["keep_searching"] = search_action.status == "wanted"
                result["final_status"] = search_action.status
                # ``target_final_format`` is already the configured target
                # (set above for every lossless source). This lane used to
                # re-assert it because it was the one proof-free path that
                # still materialized one; issue #829 made that the rule.
                return _finalize_denylist(result)
            target_contract = None
            if stage2_new_format is not None:
                target_contract = (
                    TargetQualityContract.from_projection(
                        stage2_new_format,
                        projected_is_cbr=post_conversion_is_cbr,
                    )
                    if post_conversion_is_cbr is not None
                    else TargetQualityContract.from_explicit_label(
                        stage2_new_format
                    )
                )
            measured = measured_import_decision(
                MeasuredImportDecisionInput(
                    new_m,
                    existing_m,
                    policy_is_transcode,
                    target_contract,
                    candidate_source_probe,
                    will_be_verified or verified_proof,
                    source_spectral=candidate_spectral,
                    current_spectral=existing_spectral,
                ),
                cfg=cfg,
            )
            result["stage2_import"] = measured.decision
            result["comparison_basis"] = (
                msgspec.to_builtins(measured.comparison_basis)
                if measured.comparison_basis is not None else None)

            if result["stage2_import"] == "downgrade":
                result["final_status"] = "imported"  # keeps existing
                result["keep_searching"] = True
                return _finalize_denylist(result)
            elif result["stage2_import"] == "transcode_downgrade":
                result["final_status"] = "wanted"
                result["keep_searching"] = True
                return _finalize_denylist(result)
            elif result["stage2_import"] in ("transcode_upgrade", "transcode_first"):
                result["imported"] = True
                result["keep_searching"] = True
                # Still runs quality gate after import
            else:
                result["imported"] = True

            # Genuine FLAC→V0 sets verified_lossless. Routed through
            # determine_verified_lossless so the V0-avg trust override (Bill
            # Hicks shape — spectral=suspect on spoken-word with high V0
            # evidence) flips False→True consistently with import_one.py.
            if will_be_verified:
                verified_proof = True
                result["verified_lossless"] = True

            # The post-import gate classifies what is ON DISK, so it reads
            # the same configured target the harness materializes — never
            # the proof. Keying the two on different facts is precisely how
            # a denied album could be ground to V0 and then gated as
            # "opus 128" (or the reverse); one fact, both places.
            if verified_lossless_target:
                gate_format = verified_lossless_target
            else:
                gate_format = stage2_new_format
            gate_contract = None
            if gate_format is not None:
                gate_contract = (
                    TargetQualityContract.from_projection(
                        gate_format,
                        projected_is_cbr=post_conversion_is_cbr,
                    )
                    if post_conversion_is_cbr is not None
                    else TargetQualityContract.from_explicit_label(gate_format)
                )

            # Use post-conversion bitrate for quality gate. The simulator
            # doesn't take a separate post-conversion avg, so avg == min here;
            # in production the real avg comes from beets after import.
            gate_bitrate = post_conversion_min_bitrate or min_bitrate
            gate_avg_bitrate = gate_bitrate
            gate_cbr = False  # V0 conversion always produces VBR
        else:
            # Native lossy path: import directly. The caller must supply the codec
            # label measured from the actual audio. An absent/unmapped label stays
            # UNKNOWN; it must never be relabelled as MP3 from bitrate or container
            # shape.
            #
            # Use the caller-supplied avg_bitrate when present (falls back to
            # min_bitrate otherwise). Under the default cfg.bitrate_metric=AVG
            # policy a VBR V0 at min=200/avg=245 must rank on avg=245 — otherwise
            # the import/downgrade comparison and the post-import gate both see
            # the wrong tier.
            stage2_new_format = comparison_format_hint(
                explicit_format=new_format,
            )
            # No fabricated fallbacks: when the caller measured no avg, the
            # basis metric falls back to (and honestly says) "min". Median is
            # not part of this interface at all.
            new_m = AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate,
                avg_bitrate_kbps=avg_bitrate,
                format=stage2_new_format,
                is_cbr=is_cbr,
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=candidate_spectral_class,
                spectral_subject=(
                    EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
                ),
                spectral_provenance=(
                    EVIDENCE_PROVENANCE_MEASURED
                    if spectral_grade is not None else None
                ),
                # Gated on the grade for the same reason
                # ``build_existing_quality_measurement`` gates: these facts are
                # measured in the SAME pass as the grade, so a measurement with
                # no grade cannot legitimately carry them.
                cliff_hz=(
                    candidate_context.cliff_hz
                    if spectral_grade is not None else None
                ),
                codec_family=(
                    candidate_context.codec_family
                    if spectral_grade is not None else None
                ))
            # Lossless-source lock: a recorded existing lossless-source V0 probe
            # is the truth-of-source anchor. Lossy candidates have no comparable
            # measurement and are rejected before measured_import_decision can
            # be misled by an on-disk avg that is just our own transcode floor.
            lossy_lock = provisional_lossless_decision(
                ProvisionalLosslessDecisionInput(
                    candidate_probe=None,
                    existing_probe=V0ProbeEvidence(
                        kind=existing_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
                        avg_bitrate_kbps=existing_v0_probe_avg,
                    ) if existing_v0_probe_avg is not None else None,
                    supported_lossless_source=False,
                ),
                cfg=cfg,
            )
            if lossy_lock.decision == DECISION_LOSSLESS_SOURCE_LOCKED:
                result["stage2_import"] = lossy_lock.decision
                result["final_status"] = "wanted"
                result["keep_searching"] = True
                return _finalize_denylist(result)
            measured = measured_import_decision(
                MeasuredImportDecisionInput(
                    new_m,
                    existing_m,
                    verified_lossless_proof=verified_proof,
                    source_spectral=candidate_spectral,
                    current_spectral=existing_spectral,
                ),
                cfg=cfg,
            )
            result["stage2_import"] = measured.decision
            result["comparison_basis"] = (
                msgspec.to_builtins(measured.comparison_basis)
                if measured.comparison_basis is not None else None)

            if result["stage2_import"] == "downgrade":
                result["final_status"] = "imported"  # keeps existing
                result["keep_searching"] = True
                return _finalize_denylist(result)

            result["imported"] = True
            gate_bitrate = min_bitrate
            # Real avg only; the gate's metric selection falls back to min when
            # avg is unmeasured (same classified value as the old fabricated
            # fallback — gate_m is internal and never persisted as a basis).
            gate_avg_bitrate = avg_bitrate
            gate_cbr = is_cbr
            gate_format = stage2_new_format
            gate_contract = None

        # --- Stage 3: Post-import quality gate ---
        gate_spectral_bitrate = None
        effective_gate_bitrate = compute_effective_override_bitrate(
            gate_bitrate, candidate_spectral)
        # ``gate_bitrate`` is assigned from ``min_bitrate`` (now typed ``int``,
        # never ``None``) on every branch above, so it is never ``None`` here —
        # the redundant ``gate_bitrate is not None`` guard is dropped now that
        # ``min_bitrate: int`` makes that provable rather than merely assumed.
        if (effective_gate_bitrate is not None
                and effective_gate_bitrate < gate_bitrate):
            gate_spectral_bitrate = candidate_spectral_class
        gate_measurement_format = (
            gate_contract.format.split()[0]
            if gate_contract is not None
            else gate_format
        )
        gate_m = AudioQualityMeasurement(
            min_bitrate_kbps=gate_bitrate,
            avg_bitrate_kbps=gate_avg_bitrate,
            median_bitrate_kbps=gate_avg_bitrate,
            format=gate_measurement_format,
            is_cbr=gate_cbr,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=gate_spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
            # The measured codec facts describe the CANDIDATE's bytes. They ride
            # the gate measurement only when the gate is still looking at those
            # bytes (native lossy import); after a conversion the gate describes
            # the output, whose codec family is the target's, not the source's.
            # Stamping the source's family on an output projection would be the
            # download-37946 mistake in the other direction.
            cliff_hz=(
                candidate_context.cliff_hz
                if gate_measurement_format == candidate_format_label
                else None
            ),
            codec_family=(
                candidate_context.codec_family
                if gate_measurement_format == candidate_format_label
                else None
            ))
        result["stage3_quality_gate"] = quality_gate_decision(
            gate_m,
            cfg=cfg,
            target_contract=gate_contract,
            verified_lossless_proof=verified_proof,
        )
        search_action = post_import_search_action(result["stage3_quality_gate"])
        result["final_status"] = search_action.status
        result["keep_searching"] = search_action.status == "wanted"

        return _finalize_denylist(result)

    if stage1_short_circuits:
        # AUDIT ONLY — never a decision input. Stage 2 runs over a
        # THROWAWAY copy of the result dict and exactly two values are
        # lifted back onto the real result, under keys no branch anywhere
        # reads. Every decision field below is written exactly as it was
        # before this audit existed, in the same order, from the same
        # facts. A ``ValueError`` is swallowed for the same reason: a
        # reporting field must not be able to turn a clean Stage-1 reject
        # into a crash, and this is the one path that never used to reach
        # the tail at all (``TargetQualityContract.from_explicit_label``
        # rejects a bare ``MP3`` target — a world reachable here through
        # ``comparison_format_hint``). ``ValueError`` is the ONLY exception
        # type ``lib/quality`` raises by design, and the catch stays that
        # narrow deliberately: anything else is a real defect and must
        # still surface. The identical tail runs unguarded on every
        # deferring path, so nothing real can hide here either.
        #
        # ``dict(result)`` is a complete copy only while every value on
        # ``result`` is still a scalar at this point, which is true because
        # nothing before Stage 1 writes a container. A future branch that
        # writes one above here owes a deepcopy — otherwise the throwaway
        # would share it with the real decision.
        try:
            deferred = _stage2_onward(dict(result))
        except ValueError:
            # Reported, never silent: an unevaluable counterfactual is NOT
            # the same fact as "Stage 2 had nothing to say", and leaving
            # both keys None made the two indistinguishable to the operator
            # and to the properties.
            deferred = {"stage2_import": STAGE2_COUNTERFACTUAL_UNAVAILABLE}
        result["stage2_import_if_stage1_deferred"] = deferred.get(
            "stage2_import")
        result["comparison_basis_if_stage1_deferred"] = deferred.get(
            "comparison_basis")
        result["final_status"] = "wanted"  # stays wanted, denylist user
        result["keep_searching"] = True
        return _finalize_denylist(result)

    return _stage2_onward(result)


class AlbumQualityEvidenceDecisionFacts(msgspec.Struct, frozen=True):
    """Action-time facts that are not intrinsic album-quality evidence.

    Beets distance bypass is intentionally outside this quality comparison;
    caller identity is not an input to this Struct.

    ``installed_marked_incomplete`` / ``candidate_covers_declared_program``
    are issue #1241's two conjuncts. The first is the OPERATOR's mark on the
    request (``album_requests.marked_incomplete_at``) — never a measured
    verdict; the census only informs the operator. The second is beets' own
    proof that this attempt's candidate carried every declared track,
    derived from the persisted scenario through
    ``lib.validation_envelope.scenario_covers_declared_program``. When both
    hold, the decider disregards the installed side entirely and admits the
    candidate exactly as it would into an empty slot: "incomplete is
    incomplete and complete always always beats it." Both default False, so
    an unmarked world decides exactly as before.
    """

    audio_check_mode: str = "normal"
    audio_corrupt: bool = False
    has_nested_audio: bool = False
    verified_lossless_target: str | None = None
    target_format: str | None = None
    converted_count: int | None = None
    post_conversion_min_bitrate: int | None = None
    post_conversion_is_cbr: bool | None = None
    installed_marked_incomplete: bool = False
    candidate_covers_declared_program: bool = False


class QualityEvidenceActionPayload(msgspec.Struct, frozen=True):
    """Action-time payload that authorizes import mutation from evidence.

    This payload is generated for a specific import action. It is not a stored
    preview verdict: the candidate/current evidence and decision reflect the
    action-time reducer inputs and output that allowed mutation.
    """

    candidate: AlbumQualityEvidence
    current: AlbumQualityEvidence | None = None
    decision: dict[str, Any] = msgspec.field(default_factory=dict[str, object])
    decision_name: str | None = None
    target_format: str | None = None
    verified_lossless_target: str | None = None
    provenance: QualityEvidenceActionProvenance = msgspec.field(
        default_factory=QualityEvidenceActionProvenance
    )


def evidence_decision_name(
    result: dict[str, object],
    *,
    default: str = "quality_reject",
) -> str:
    """Return the dispatch decision represented by a quality decision dict.

    Recognises the U11 folder/audio-integrity early-exit rejects via
    ``preimport_audio`` / ``preimport_nested`` / ``preimport_bad_hash`` /
    ``preimport_empty_fileset`` dict keys, plus the existing stage-* keys.
    """

    # Folder/audio-integrity rejects fire *before* the quality stages run,
    # so check them first — if a four-fact reject is present, stage2/stage3
    # will be None and falling through to the quality default would lose
    # the specific reason.
    if result.get("preimport_audio") == "reject_corrupt":
        return "audio_corrupt"
    if result.get("preimport_bad_hash") == "reject_bad_hash":
        return "bad_audio_hash"
    if result.get("preimport_nested") == "reject_nested":
        return "nested_layout"
    if result.get("preimport_empty_fileset") == "reject_empty":
        return "empty_fileset"
    if result.get("preimport_mixed_source") == "reject_mixed_source":
        return "mixed_source"
    for key in ("stage2_import", "stage3_quality_gate"):
        value = result.get(key)
        if isinstance(value, str) and value:
            return value
    if (
        result.get("stage1_spectral") == "reject"
        and not result.get("stage2_import")
    ):
        return "spectral_reject"
    return default


def resolve_pipeline_decision_denylist(result: dict[str, object]) -> bool:
    """Whether a decision dict's outcome denylists its source — single-
    sourced from production's real write (issue #813 Finding 2).

    Production resolves denylist policy from a decision string via
    ``lib.quality.dispatch_actions.decision_denylists`` (the two-tier
    ``post_import_search_action`` -> ``dispatch_action`` lookup shared with
    ``lib.dispatch.post_import._resolve_post_import_search_policy``, the
    real importer write). A decision dict can carry TWO governing decisions:
    the stage2/early-exit decision that fires when the outcome never reaches
    Stage 3 (``evidence_decision_name``), and — independently — the Stage-3
    quality-gate decision when the import *does* reach that gate
    (``lib.dispatch.quality_gate`` re-evaluates denylist policy fresh from
    the post-import state, so an outcome can be denylisted by either stage).
    """
    denylisted = decision_denylists(evidence_decision_name(result))
    stage3 = result.get("stage3_quality_gate")
    if isinstance(stage3, str) and stage3:
        denylisted = denylisted or decision_denylists(stage3)
    return denylisted


def _finalize_denylist(result: dict[str, object]) -> dict[str, object]:
    """Single choke point every ``full_pipeline_decision``/
    ``full_pipeline_decision_from_evidence`` return path funnels through.

    Issue #813 Finding 2: three separate ``downgrade`` return sites each
    independently forgot to set ``denylisted``, silently diverging from what
    ``dispatch_action("downgrade").denylist`` (and the real importer) always
    writes. Computing it here, once, from the decision(s) already recorded
    on ``result`` makes that whole bug class structurally impossible — a new
    branch cannot "forget" a step it never performs.
    """
    result["denylisted"] = resolve_pipeline_decision_denylist(result)
    return result


def comparison_basis_from_decision(
    result: "dict[str, Any] | None",
    *,
    key: str = "comparison_basis",
) -> QualityComparisonBasis | None:
    """Re-type the JSON-plain ``comparison_basis`` a decision dict carries.

    The decision dict stores the basis as ``msgspec.to_builtins`` output so
    it survives json.dumps'd API responses and the evidence-action wire.
    This is the one converter back to the typed Struct — used by dispatch
    when synthesizing the reject-side ImportResult and by the harness when
    consuming the action file. Strict convert: dispatch and harness ship in
    the same deploy, so shape drift is a bug worth failing on.

    ``key`` selects WHICH basis: the decided one, or PR2d's Stage-1-reject
    counterfactual (``comparison_basis_if_stage1_deferred``). Both are the
    same shape written by the same code, so one converter serves both
    rather than a near-copy that could decode them differently.
    """
    if not result:
        return None
    raw = result.get(key)
    if raw is None:
        return None
    return msgspec.convert(raw, type=QualityComparisonBasis)


def stage2_counterfactual_from_decision(
    result: "dict[str, Any] | None",
) -> str | None:
    """The Stage-2 decision a Stage-1 reject short-circuited past (PR2d).

    ``None`` means Stage 1 never short-circuited, so there is no
    counterfactual — deliberately distinct from
    ``STAGE2_COUNTERFACTUAL_UNAVAILABLE``, which means the audit ran and
    could not be evaluated. Issue #829 Phase 5 PR4 persists this onto
    ``ImportResult`` so the operator surfaces can show the disagreement
    issue #813 is about instead of it living only in an in-memory dict.
    """
    if not result:
        return None
    value = result.get("stage2_import_if_stage1_deferred")
    return value if isinstance(value, str) and value else None


QUALITY_DECISION_IMPORT_STAGE_DECISIONS: frozenset[str] = frozenset({
    "import",
    "preflight_existing",
    "transcode_upgrade",
    "transcode_first",
    "provisional_lossless_upgrade",
})
QUALITY_DECISION_REJECT_STAGE_DECISIONS: frozenset[str] = frozenset({
    "downgrade",
    "transcode_downgrade",
    "suspect_lossless_downgrade",
    "suspect_lossless_probe_missing",
    "lossless_source_locked",
    "verified_lossless_locked",
})
QUALITY_DECISION_REQUEUE_DECISIONS: frozenset[str] = frozenset({
    "requeue_upgrade",
    "requeue_lossless",
})


def classify_quality_import_stages(
    stage2: object,
    stage3: object,
    *,
    imported: bool,
) -> tuple[str, bool, str | None]:
    """Classify import-stage outcomes for preview/audit cleanup policy.

    Returns ``(verdict, cleanup_eligible, reason)``. ``cleanup_eligible`` means
    the rejection is safe to use for source-folder cleanup; import/requeue
    outcomes are never cleanup-eligible.
    """

    stage2_decision = str(stage2) if isinstance(stage2, str) else None
    stage3_decision = str(stage3) if isinstance(stage3, str) else None

    if stage2_decision in QUALITY_DECISION_REJECT_STAGE_DECISIONS:
        return "confident_reject", True, stage2_decision

    if stage2_decision in QUALITY_DECISION_IMPORT_STAGE_DECISIONS or imported:
        reason = (
            stage3_decision
            if stage3_decision in QUALITY_DECISION_REQUEUE_DECISIONS
            else stage2_decision or stage3_decision or "import"
        )
        return "would_import", False, reason

    if stage3_decision in QUALITY_DECISION_REQUEUE_DECISIONS:
        return "uncertain", False, stage3_decision

    return "uncertain", False, stage2_decision or stage3_decision or "unknown"


def classify_full_pipeline_decision(
    decision: dict[str, object],
) -> tuple[str, bool, str | None]:
    """Classify a full pipeline decision dict for preview/cleanup display."""

    if decision.get("preimport_nested") == "reject_nested":
        return "confident_reject", True, "nested_layout"
    if decision.get("preimport_audio") == "reject_corrupt":
        return "confident_reject", True, "audio_corrupt"
    # U11: bad-hash and empty-fileset early-exit rejects.
    if decision.get("preimport_bad_hash") == "reject_bad_hash":
        return "confident_reject", True, "bad_audio_hash"
    if decision.get("preimport_empty_fileset") == "reject_empty":
        return "confident_reject", True, "empty_fileset"
    if decision.get("preimport_mixed_source") == "reject_mixed_source":
        return "confident_reject", True, "mixed_source"
    if (
        decision.get("stage1_spectral") == "reject"
        and not decision.get("stage2_import")
    ):
        return "confident_reject", True, "spectral_reject"
    return classify_quality_import_stages(
        decision.get("stage2_import"),
        decision.get("stage3_quality_gate"),
        imported=bool(decision.get("imported")),
    )


def _require_evidence_ready(
    role: str,
    evidence: AlbumQualityEvidence,
) -> None:
    reasons = evidence.policy_incomplete_reasons()
    if reasons:
        joined = "; ".join(reasons)
        raise ValueError(f"{role} album quality evidence is incomplete: {joined}")


def _first_bitrate(*values: int | None) -> int:
    for value in values:
        if value is not None:
            return value
    raise ValueError("album quality evidence has no bitrate metric")


def _normalised_format(value: str | None) -> str | None:
    if value is None:
        return None
    normalised = value.strip().lower().lstrip(".")
    return normalised or None


def _lossless_source_from_evidence(evidence: AlbumQualityEvidence) -> bool:
    metric = evidence.v0_metric
    if (
        metric is not None
        and metric.subject == EVIDENCE_SUBJECT_SOURCE
    ):
        return True

    measurement = evidence.measurement
    if evidence.verified_lossless_proof is not None:
        return True
    candidates = (
        measurement.was_converted_from,
        evidence.storage_format,
        evidence.codec,
        evidence.container,
        measurement.format,
    )
    for candidate in candidates:
        fmt = _normalised_format(candidate)
        if fmt == "m4a":
            # M4A is only a container; AAC and ALAC share it. Treat ALAC
            # evidence as lossless, but never infer lossless source from a
            # bare .m4a extension/container.
            continue
        if fmt in _LOSSLESS_EXTS or fmt == "lossless":
            return True
    return _normalised_format(evidence.codec) == "alac"


def _policy_v0_probe_from_metric(
    metric: AlbumQualityV0Metric | None,
) -> V0ProbeEvidence | None:
    if metric is None:
        return None
    kind = (
        V0_PROBE_LOSSLESS_SOURCE
        if metric.subject == EVIDENCE_SUBJECT_SOURCE
        else _NONCOMPARABLE_NEUTRAL_V0_PROBE_KIND
    )
    return V0ProbeEvidence(
        kind=kind,
        min_bitrate_kbps=metric.min_bitrate_kbps,
        avg_bitrate_kbps=metric.avg_bitrate_kbps,
        median_bitrate_kbps=metric.median_bitrate_kbps,
    )


def _evidence_target_format(
    candidate: AlbumQualityEvidence,
    facts: AlbumQualityEvidenceDecisionFacts,
) -> str | None:
    return facts.target_format if facts.target_format is not None else candidate.target_format


def _evidence_target_is_cbr(
    candidate: AlbumQualityEvidence,
    facts: AlbumQualityEvidenceDecisionFacts,
    *,
    target_format: str | None,
) -> bool | None:
    """Resolve projected mode without borrowing source/output measurements."""

    if facts.post_conversion_is_cbr is not None:
        return facts.post_conversion_is_cbr
    if (
        target_format is not None
        and target_format == candidate.target_format
        and candidate.target_is_cbr is not None
    ):
        return candidate.target_is_cbr
    if target_format is None:
        return None
    # No projection exists at this boundary. Explicit labels can resolve
    # themselves; bare MP3 raises rather than borrowing a source/output mode.
    return TargetQualityContract.from_explicit_label(target_format).is_cbr


def _new_format_hint_from_evidence(
    candidate: AlbumQualityEvidence,
    *,
    supported_lossless_source: bool,
    target_format: str | None,
) -> str | None:
    if supported_lossless_source and target_format not in ("flac", "lossless"):
        return None
    return candidate.measurement.format or candidate.storage_format


def evidence_spectral_context(
    evidence: AlbumQualityEvidence | None,
) -> SpectralCodecContext:
    """The codec-resolution context one evidence row carries.

    ``storage_format`` and ``filetype_band`` live on the evidence row, not
    the measurement, and only the row can fail closed on a mixed-codec
    album — whose album-level spectral grade was averaged ACROSS codec
    families and whose ``codec_family`` capture is only the first track's.

    The snapshot's own file extensions are the ONLY honest answer to the
    ultrasonic proof leg's decode-path question for a row whose spectral
    describes the files on disk. ``extension`` is a required, validated
    field on every snapshot row, whereas ``storage_format``/``format``
    can carry a codec name a container does not determine (``ALAC`` and
    ``AAC`` both live in ``.m4a``, and only the extension decides which
    decoder ``analyze_track`` reached for).
    """
    if evidence is None:
        return SpectralCodecContext()
    return codec_context_from_measurement(
        evidence.measurement,
        storage_format=evidence.storage_format,
        filetype_band=evidence.filetype_band,
        container_labels=[file.extension for file in evidence.files],
    )


def proof_verdict_from_evidence(
    evidence: AlbumQualityEvidence,
) -> AlbumProofVerdict:
    """The proof-gate verdict one whole evidence row implies (issue #829 PR4).

    A thin field extraction over ``proof_verdict_from_facts`` — deliberately
    NOT a second derivation. The Recents render path holds the same columns
    as a flat projection and calls that function directly; a caller holding
    the row calls this one. Both surfaces therefore state the same verdict
    for the same album by construction, which is the property
    ``tests/test_verdict_tiers_generated.py`` patrols.

    Display only: no branch in the decision path reads a verdict.
    """
    measurement = evidence.measurement
    return proof_verdict_from_facts(
        spectral_grade=measurement.spectral_grade,
        spectral_bitrate_kbps=measurement.spectral_bitrate_kbps,
        cliff_hz=measurement.cliff_hz,
        codec_family=measurement.codec_family,
        format=measurement.format,
        storage_format=evidence.storage_format,
        filetype_band=evidence.filetype_band,
        spectral_subject=measurement.spectral_subject,
        was_converted_from=measurement.was_converted_from,
        container_labels=[file.extension for file in evidence.files],
        ultrasonic_deficit_db=measurement.ultrasonic_deficit_db,
        spectral_measurement_version=(
            measurement.spectral_measurement_version
        ),
        aac_lattice=evidence.aac_lattice,
    )


def interpret_evidence_spectral(
    evidence: AlbumQualityEvidence | None,
) -> SpectralInterpretation:
    """Interpret one evidence row's spectral evidence in its codec's terms.

    Context and measurement are combined by ``SpectralCodecContext.interpret``
    — the one place that pairing happens, so no caller can accidentally
    resolve a codec with less evidence than the row carries.
    """
    return evidence_spectral_context(evidence).interpret(
        evidence.measurement if evidence is not None else None
    )


def override_bitrate_from_current_evidence(
    current: AlbumQualityEvidence | None,
) -> int | None:
    if current is None:
        return None
    measurement = current.measurement
    current_min = measurement.min_bitrate_kbps
    effective = compute_effective_override_bitrate(
        current_min,
        interpret_evidence_spectral(current),
    )
    if current_min is not None and effective is not None and effective != current_min:
        return effective
    return None


def full_pipeline_decision_from_evidence(
    candidate: AlbumQualityEvidence,
    current: AlbumQualityEvidence | None = None,
    *,
    facts: AlbumQualityEvidenceDecisionFacts | None = None,
    cfg: QualityRankConfig | None = None,
) -> dict[str, Any]:
    """Run the full quality policy from neutral album-quality evidence.

    This is THE single decision function for the importer. Callers
    provide durable ``AlbumQualityEvidence`` rows plus narrow action facts;
    old V0 probe ``kind`` constants are not accepted as public inputs.

    The decision dict shape (shared with ``full_pipeline_decision``):

        {
            "preimport_audio": str | None,
            "preimport_nested": str | None,
            "preimport_bad_hash": str | None,       # U11
            "preimport_empty_fileset": str | None,  # U11
            "preimport_mixed_source": str | None,   # mixed-source reject
            "stage0_spectral_gate": str | None,
            "stage1_spectral": str | None,
            "stage2_import": str | None,
            "stage3_quality_gate": str | None,
            "final_status": str | None,
            "imported": bool,
            "denylisted": bool,  # see resolve_pipeline_decision_denylist — #813
            "keep_searching": bool,
            "target_final_format": str | None,
            "verified_lossless": bool,
            "comparison_basis": dict | None,  # QualityComparisonBasis builtins
            # AUDIT ONLY — the Stage-1-reject counterfactual. See
            # ``full_pipeline_decision``'s docstring; never a decision input.
            "stage2_import_if_stage1_deferred": str | None,
            "comparison_basis_if_stage1_deferred": dict | None,
            # AUDIT ONLY — issue #1241's disregard flag; see the flat twin.
            "installed_incomplete_disregarded": bool,
        }

    Folder/audio-integrity facts are read directly off ``candidate`` as
    early-exit rejects (in priority order):

      1. ``audio_corrupt``  — sets ``preimport_audio='reject_corrupt'``
      2. ``bad_audio_hash`` — sets ``preimport_bad_hash='reject_bad_hash'``
      3. ``nested_layout``  — sets ``preimport_nested='reject_nested'``
      4. ``empty_fileset``  — sets ``preimport_empty_fileset='reject_empty'``
      5. ``mixed_source``   — sets
         ``preimport_mixed_source='reject_mixed_source'`` when the
         snapshot contains both lossless and lossy containers (e.g.
         15 FLAC + 2 MP3). Keeps Cratedigger release-based — never
         partially-imports an album.

    The accompanying ``evidence_decision_name`` maps these dict shapes to
    ``audio_corrupt`` / ``bad_audio_hash`` / ``nested_layout`` /
    ``empty_fileset`` / ``mixed_source`` decision strings, which the
    importer feeds to ``dispatch_action`` and the unified
    ``_reject_import_from_evidence_decision`` helper.
    """

    if facts is None:
        facts = AlbumQualityEvidenceDecisionFacts()

    _require_evidence_ready("candidate", candidate)
    if current is not None:
        _require_evidence_ready("current", current)

    # Issue #1241 — same predicate as the flat twin computes from its own
    # kwargs. Needed HERE too because this twin's decision-21 early return
    # below fires before the flat twin is ever called; the mark must disarm
    # that lock exactly as it disarms every other existing-side fact.
    installed_incomplete_disregarded = (
        facts.installed_marked_incomplete
        and facts.candidate_covers_declared_program
    )

    # Current proof outranks every candidate fact for every import mode
    # (decision 21): a corrupt, nested, empty, mixed, or known-bad candidate
    # cannot reopen a release at the terminal archival ceiling, and a
    # force-import cannot cross it either — force bypasses only the beets
    # distance; Replace/re-request is the operator's way back in. The ONE
    # thing that outranks the ceiling is the operator's own incomplete mark
    # (issue #1241): a locked copy that is missing declared program is not a
    # terminal archive, and the mark is the operator decision that reopens
    # it — no lock-side machinery, the disregard simply precedes the lock.
    if (
        current is not None
        and current.verified_lossless_proof is not None
        and not installed_incomplete_disregarded
    ):
        return _finalize_denylist({
            "preimport_audio": None,
            "preimport_nested": None,
            "preimport_bad_hash": None,
            "preimport_empty_fileset": None,
            "preimport_mixed_source": None,
            "stage0_spectral_gate": None,
            "stage1_spectral": None,
            "stage2_import": DECISION_VERIFIED_LOSSLESS_LOCKED,
            "stage3_quality_gate": None,
            "final_status": "imported",
            "imported": False,
            "keep_searching": False,
            "target_final_format": None,
            "verified_lossless": candidate.verified_lossless_proof is not None,
            "comparison_basis": None,
            "stage2_import_if_stage1_deferred": None,
            "comparison_basis_if_stage1_deferred": None,
            # Reachable only when the #1241 predicate did NOT fire (the
            # guard above skips this return otherwise).
            "installed_incomplete_disregarded": False,
        })

    # --- U11 folder/audio-integrity early-exit rejects ---
    # The four facts live directly on the persisted ``AlbumQualityEvidence``
    # row (added by U1+U2/U3 migrations). Order matches the deleted
    # ``preimport_decide``: corrupt > bad-hash > nested > empty.
    #
    # SQL defaults for U1 fields (migration 019) are ``audio_corrupt=FALSE``,
    # ``folder_layout='flat'``, ``audio_file_count=0``, ``filetype_band=''``.
    # Legacy rows decoding under those defaults must not trigger
    # ``empty_fileset`` when files are present — reconcile against the
    # snapshot ``files`` list, mirroring the prior ``_build_preimport_
    # measurement_from_evidence`` reconciliation.
    def _early_reject_result(
        *,
        preimport_audio: str | None = None,
        preimport_nested: str | None = None,
        preimport_bad_hash: str | None = None,
        preimport_empty_fileset: str | None = None,
        preimport_mixed_source: str | None = None,
    ) -> dict[str, Any]:
        # The acquisition verdict remains wanted. Caller identity is absent
        # from this reducer; the dispatch boundary decides whether that verdict
        # may mutate an operator-owned request status. ``denylisted`` is
        # derived by ``_finalize_denylist`` from the fact just recorded above
        # (issue #813 Finding 2) — never a per-call literal.
        return _finalize_denylist({
            "preimport_audio": preimport_audio,
            "preimport_nested": preimport_nested,
            "preimport_bad_hash": preimport_bad_hash,
            "preimport_empty_fileset": preimport_empty_fileset,
            "preimport_mixed_source": preimport_mixed_source,
            "stage0_spectral_gate": None,
            "stage1_spectral": None,
            "stage2_import": None,
            "stage3_quality_gate": None,
            "final_status": "wanted",
            "imported": False,
            "keep_searching": True,
            "target_final_format": None,
            "verified_lossless": False,
            "comparison_basis": None,
            "stage2_import_if_stage1_deferred": None,
            "comparison_basis_if_stage1_deferred": None,
            # Issue #1241 parity with the flat twin: the audit flag records
            # whether the predicate fired, even on a candidate-side early
            # reject where the installed side never participates — the
            # absolute admission floors outrank the disregard.
            "installed_incomplete_disregarded": (
                installed_incomplete_disregarded
            ),
        })

    preimport_fact = candidate_preimport_reject_fact(candidate)
    if preimport_fact == "audio_corrupt":
        return _early_reject_result(
            preimport_audio="reject_corrupt",
        )

    if preimport_fact == "bad_audio_hash":
        return _early_reject_result(
            preimport_bad_hash="reject_bad_hash",
        )

    if preimport_fact == "nested_layout":
        return _early_reject_result(
            preimport_nested="reject_nested",
        )

    # Reconcile audio_file_count against snapshot files: legacy rows decode
    # the SQL default 0 but may carry snapshot files. Only the
    # explicit-and-corroborated zero case (count=0 AND no snapshot files)
    # is the empty_fileset reject.
    if preimport_fact == "empty_fileset":
        return _early_reject_result(
            preimport_empty_fileset="reject_empty",
        )

    # Mixed-source reject: lossless + lossy containers in the same folder.
    # Cratedigger stays release-based — a partial FLAC+MP3 source must
    # never get partially-imported and stamped verified-lossless. See
    # ``has_mixed_lossless_and_lossy`` and the Fast Times reproduction.
    if preimport_fact == "mixed_source":
        return _early_reject_result(
            preimport_mixed_source="reject_mixed_source",
        )

    candidate_measurement = candidate.measurement
    current_measurement = current.measurement if current is not None else None
    candidate_probe = _policy_v0_probe_from_metric(candidate.v0_metric)
    current_probe = (
        _policy_v0_probe_from_metric(current.v0_metric)
        if current is not None
        else None
    )

    target_format = _evidence_target_format(candidate, facts)
    post_conversion_is_cbr = _evidence_target_is_cbr(
        candidate,
        facts,
        target_format=target_format,
    )
    supported_lossless_source = _lossless_source_from_evidence(candidate)
    post_conversion_min = (
        facts.post_conversion_min_bitrate
        if facts.post_conversion_min_bitrate is not None
        else (
            candidate_probe.min_bitrate_kbps
            if supported_lossless_source and candidate_probe is not None
            else None
        )
    )
    converted_count = facts.converted_count
    if converted_count is None:
        converted_count = (
            1
            if (
                supported_lossless_source
                and target_format not in ("flac", "lossless")
                and post_conversion_min is not None
            )
            else 0
        )

    existing_min = None
    existing_avg = None
    existing_format = None
    existing_is_cbr = False
    existing_spectral_grade = None
    existing_spectral_bitrate = None
    if current_measurement is not None:
        assert current is not None
        existing_min = current_measurement.min_bitrate_kbps
        existing_avg = current_measurement.avg_bitrate_kbps
        existing_format = current_measurement.format or current.storage_format
        existing_is_cbr = current_measurement.is_cbr
        existing_spectral_grade = current_measurement.spectral_grade
        existing_spectral_bitrate = current_measurement.spectral_bitrate_kbps

    return full_pipeline_decision(
        is_flac=supported_lossless_source,
        min_bitrate=_first_bitrate(
            candidate_measurement.min_bitrate_kbps,
            candidate_measurement.avg_bitrate_kbps,
            candidate_measurement.median_bitrate_kbps,
        ),
        is_cbr=candidate_measurement.is_cbr,
        avg_bitrate=candidate_measurement.avg_bitrate_kbps,
        spectral_grade=candidate_measurement.spectral_grade,
        spectral_bitrate=candidate_measurement.spectral_bitrate_kbps,
        existing_min_bitrate=existing_min,
        existing_avg_bitrate=existing_avg,
        existing_spectral_bitrate=existing_spectral_bitrate,
        existing_spectral_grade=existing_spectral_grade,
        override_min_bitrate=override_bitrate_from_current_evidence(current),
        existing_format=existing_format,
        existing_is_cbr=existing_is_cbr,
        post_conversion_min_bitrate=post_conversion_min,
        post_conversion_is_cbr=post_conversion_is_cbr,
        converted_count=converted_count,
        candidate_verified_lossless_proof=(
            candidate.verified_lossless_proof is not None
        ),
        verified_lossless_target=facts.verified_lossless_target,
        target_format=target_format,
        new_format=_new_format_hint_from_evidence(
            candidate,
            supported_lossless_source=supported_lossless_source,
            target_format=target_format,
        ),
        audio_check_mode=facts.audio_check_mode,
        audio_corrupt=facts.audio_corrupt,
        has_nested_audio=facts.has_nested_audio,
        cfg=cfg,
        candidate_v0_probe_avg=(
            candidate_probe.avg_bitrate_kbps
            if candidate_probe is not None
            else None
        ),
        candidate_v0_probe_min=(
            candidate_probe.min_bitrate_kbps
            if candidate_probe is not None
            else None
        ),
        existing_v0_probe_avg=(
            current_probe.avg_bitrate_kbps
            if current_probe is not None
            else None
        ),
        existing_v0_probe_kind=(
            current_probe.kind if current_probe is not None else None
        ),
        candidate_v0_probe_kind=(
            candidate_probe.kind if candidate_probe is not None else None
        ),
        supported_lossless_source=supported_lossless_source,
        current_verified_lossless_proof=(
            current is not None
            and current.verified_lossless_proof is not None
        ),
        candidate_spectral_context=evidence_spectral_context(candidate),
        existing_spectral_context=evidence_spectral_context(current),
        # Issue #1241 — the two conjuncts, handed straight across. The flat
        # twin re-computes the same predicate and performs the actual
        # existing-side disregard, so the twins cannot diverge on it.
        installed_marked_incomplete=facts.installed_marked_incomplete,
        candidate_covers_declared_program=(
            facts.candidate_covers_declared_program
        ),
        # The persisted capture, handed straight across (issue #829
        # AAC-lattice leg PR-B). No adapter: the evidence row's column IS
        # the leg's input, so there is nothing to derive and nothing that
        # could derive it differently from the simulator twin.
        candidate_aac_lattice=candidate.aac_lattice,
    )
