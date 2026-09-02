"""Generated (property-based) quality-decision tests — issue #548.

Hypothesis-driven properties over the quality decision twins:

* ``full_pipeline_decision`` — the flat-kwargs simulator twin, driven
  through ``simulate()`` (the canonical scenario language of the album
  test set).
* ``full_pipeline_decision_from_evidence`` — the production decider,
  driven through the shared parity builders in
  ``tests/evidence_helpers.py``.

Two tiers, selected by ``CRATEDIGGER_HYPOTHESIS_PROFILE`` (see
``tests/_hypothesis_profiles.py``):

* ``suite`` (default) — deterministic, bounded; runs on every
  ``scripts/run_tests.sh`` like any other test.
* ``fuzz`` — randomized burst for local exploration when quality policy
  changes::

      nix-shell --run "CRATEDIGGER_HYPOTHESIS_PROFILE=fuzz \\
          python3 -m unittest tests.test_quality_generated -v"

Promotion policy: when the fuzz tier finds a real failure, Hypothesis
shrinks it to a minimal world — commit that world as a named
``@example(...)`` pin here, or as a full scenario in the album test set
(``tests/test_quality_classification.py``). No JSON corpus.
Full usage guide: docs/generated-testing.md.
"""

import os
import re
import sys
import unittest
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Never

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec
from hypothesis import assume, example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

# The harness twin of the stored-format decision. Imported here rather
# than lazily because checker F2 compares it against the decider's own
# answer, and the two must be read side by side to mean anything.
from harness.import_one import conversion_target
from lib.dispatch.quality_gate import (
    QualityGatePlan,
    _check_quality_gate_core,
    _QualityGateStateLoader,
)
from lib.dispatch.types import QualityGateState
from lib.quality import (
    AAC_LATTICE_PROOF_DENY_MAX_Z,
    AAC_LATTICE_PROOF_DENY_MODAL_COUNT,
    CODEC_FAMILY_MP3,
    COMPARISON_BASIS_BRANCHES,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    QUALITY_UPGRADE_TIERS,
    ULTRASONIC_PROOF_DENY_DEFICIT_DB,
    VERIFIED_LOSSLESS_CLASSIFIER,
    VERIFIED_LOSSLESS_CLASSIFIER_V3,
    VERIFIED_LOSSLESS_CLASSIFIER_V4,
    AacLatticeCapture,
    AacLatticeProofLeg,
    AlbumQualityEvidence,
    AlbumQualityEvidenceDecisionFacts,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    CodecFamily,
    ProvisionalLosslessDecisionInput,
    QualityComparisonBasis,
    QualityRank,
    QualityRankConfig,
    SpectralCodecContext,
    SpectralComparability,
    SpectralEvidenceFacts,
    TargetQualityContract,
    UltrasonicProofLeg,
    V0ProbeEvidence,
    VerifiedLosslessProof,
    aac_lattice_proof_leg,
    album_ultrasonic_proof_leg,
    classify_full_pipeline_decision,
    compute_effective_override_bitrate,
    decision_class_kbps,
    determine_verified_lossless,
    evidence_decision_name,
    full_pipeline_decision,
    full_pipeline_decision_from_evidence,
    interpret_spectral_evidence,
    is_comparable_lossless_source_probe,
    ladder_class_kbps,
    legacy_unrecorded_audio_validation_report,
    mint_verified_lossless_proof,
    provisional_lossless_decision,
    quality_gate_decision,
    quality_rank,
    spectral_classes_comparable,
    spectral_import_decision,
    ultrasonic_proof_leg,
    v0_probe_overrides_spectral,
)
from lib.quality.decisions import DECISION_VERIFIED_LOSSLESS_LOCKED
from lib.quality.filetypes import has_mixed_lossless_and_lossy
from lib.quality.pipeline import (
    _lossless_source_from_evidence,
    _policy_v0_probe_from_metric,
    evidence_spectral_context,
)
from lib.spectral_check import (
    _SOX_NATIVE_EXTS,
    MIN_CLIFF_SLICES,
    SLICE_FREQS,
)
from tests.evidence_helpers import (
    PROVISIONAL_LANE_DECISIONS,
    build_parity_candidate_evidence,
    build_parity_current_evidence,
    make_aac_lattice_capture,
    make_audio_corrupt_validation_report,
)
from tests.test_simulator_scenarios import (
    AlbumState,
    DownloadScenario,
    SimResult,
    _derive_album_format,
    assert_denylist_has_valid_cause,
    simulate,
)

_GRADES = (None, "genuine", "marginal", "suspect", "likely_transcode")
_TARGET_FORMATS = (None, "flac", "lossless", "mp3 v0", "opus 128")
_VL_TARGETS = (None, "opus 128", "mp3 v0")
_LOSSY_FORMATS = ("MP3", "Opus", "AAC", "Vorbis", "WMA")
_CURRENT_FORMATS = ("MP3", "Opus", "AAC", "Vorbis", "WMA", "FLAC")
_RANKED_CODEC_FAMILIES = frozenset(
    {"mp3", "opus", "aac", "vorbis", "wma", "flac", "alac", "wav", "lossless"}
)


def _bitrates(min_value: int = 1, max_value: int = 3000) -> st.SearchStrategy[int]:
    return st.integers(min_value=min_value, max_value=max_value)


def _optional_bitrates(max_value: int = 3000) -> st.SearchStrategy[int | None]:
    return st.one_of(st.none(), _bitrates(max_value=max_value))


def _unmapped_codec_labels() -> st.SearchStrategy[str]:
    return st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz",
        min_size=1,
        max_size=16,
    ).filter(lambda value: value not in _RANKED_CODEC_FAMILIES)


# ===========================================================================
# Invariant checkers — module functions so the known-bad self-tests below
# can prove each one trips on a violating decision (harness RED/GREEN).
# ===========================================================================

_VALID_FINAL_STATUSES = ("imported", "wanted")


def assert_decision_is_definitive(result: SimResult) -> None:
    """Totality: every auto-mode decision is a well-formed, definitive one."""
    if not isinstance(result.imported, bool):
        raise AssertionError(  # noqa: TRY004 - generated invariant failure
            f"imported is not bool: {result.imported!r}"
        )
    if not isinstance(result.keep_searching, bool):
        raise AssertionError(  # noqa: TRY004 - generated invariant failure
            f"keep_searching is not bool: {result.keep_searching!r}")
    if not isinstance(result.denylisted, bool):
        raise AssertionError(  # noqa: TRY004 - generated invariant failure
            f"denylisted is not bool: {result.denylisted!r}"
        )
    if result.final_status not in _VALID_FINAL_STATUSES:
        raise AssertionError(
            f"auto-mode decision must end imported/wanted, got "
            f"final_status={result.final_status!r}")


def assert_lossy_not_imported_over_verified_lossless(result: SimResult) -> None:
    """A raw verified-lossless FLAC on disk is terminal quality — no lossy
    candidate may replace it."""
    if result.imported:
        raise AssertionError(
            "lossy candidate imported over raw verified-lossless FLAC: "
            f"{result!r}")


def assert_obvious_downgrade_not_accepted(result: SimResult) -> None:
    """A transparent existing lossy album must not accept an obviously
    lower-rank lossy candidate."""
    if result.imported or result.stage3_quality_gate == "accept":
        raise AssertionError(
            f"obvious lower-rank lossy candidate accepted: {result!r}")


def assert_unverified_lossy_never_terminal(result: SimResult) -> None:
    """A retained lossy first copy is inventory, never a stopping proof."""
    if not result.imported:
        raise AssertionError(f"usable lossy first copy was not retained: {result!r}")
    if result.stage3_quality_gate == "accept":
        raise AssertionError(f"unverified lossy copy was accepted terminally: {result!r}")
    if result.final_status != "wanted" or not result.keep_searching:
        raise AssertionError(f"unverified lossy copy stopped searching: {result!r}")
    if not result.denylisted:
        raise AssertionError(f"retained lossy source was not denylisted: {result!r}")


def assert_proof_backed_target_uses_terminal_gate(result: dict) -> None:
    """A proof-backed configured target must use the terminal import path."""
    actual = (
        result["stage2_import"],
        result["stage3_quality_gate"],
        result["final_status"],
        result["imported"],
        result["keep_searching"],
        result["denylisted"],
    )
    expected = ("import", "accept", "imported", True, False, False)
    if actual != expected:
        raise AssertionError(
            f"proof-backed configured target was not terminal: "
            f"{actual!r} != {expected!r}"
        )


_POST_IMPORT_EXPECTATIONS = {
    "accept": ("imported", None, False),
    "requeue_lossless": ("wanted", "lossless", True),
    "requeue_upgrade": ("wanted", None, True),
}


def assert_post_import_action_matches(
    *,
    decision: str,
    status: str,
    search_filetype_override: str | None,
    denylist: bool,
) -> None:
    """Independent oracle for every observable post-import action field."""
    expected = _POST_IMPORT_EXPECTATIONS[decision]
    actual = (status, search_filetype_override, denylist)
    if actual != expected:
        raise AssertionError(
            f"post-import mapping drift for {decision}: {actual!r} != {expected!r}"
        )


def assert_search_override_is_a_string(raw_override: object) -> str | None:
    """The gate's search override crosses the transition as a string or None.

    A module function rather than an inline narrowing guard so the known-bad
    self-test can call it directly (docs/generated-testing.md § "Per-clause
    proof").
    """
    if raw_override is not None and not isinstance(raw_override, str):
        raise AssertionError(
            f"quality gate wrote a non-string override: {raw_override!r}"
        )
    return raw_override


def assert_quality_decision_failure_reopens_full_tier(
    plan: QualityGatePlan | None,
) -> None:
    """A post-import decider failure keeps acquisition open without blame."""

    if plan is None:
        raise AssertionError("quality decision failure returned no recovery plan")
    actual = (
        plan.transition.target_status,
        plan.transition.fields.get("search_filetype_override"),
        bool(plan.denylists),
        plan.successful_terminal_acceptance,
    )
    expected = ("wanted", None, False, False)
    if actual != expected:
        raise AssertionError(
            "quality decision failure did not reopen full tiers: "
            f"{actual!r} != {expected!r}"
        )


def assert_verified_lossless_proof_locks_candidate(result: SimResult) -> None:
    """A proof-bearing HAVE is terminal for every automatic candidate."""
    if result.imported:
        raise AssertionError("proof-bearing HAVE was automatically replaced")
    if result.stage2_import != "verified_lossless_locked":
        raise AssertionError(
            "proof-bearing HAVE missed verified_lossless_locked: "
            f"{result.stage2_import!r}"
        )
    if result.final_status != "imported" or result.keep_searching:
        raise AssertionError(
            "proof lock did not preserve terminal imported state: "
            f"status={result.final_status!r}, keep={result.keep_searching!r}"
        )
    if result.denylisted:
        raise AssertionError("proof lock punished the candidate source")


def assert_evidence_proof_lock_preserves_imported(result: dict) -> None:
    """The evidence twin must ignore every automatic candidate reject."""
    if result["stage2_import"] != "verified_lossless_locked":
        raise AssertionError(
            f"evidence proof lock missed: {result['stage2_import']!r}"
        )
    if result["final_status"] != "imported" or result["imported"]:
        raise AssertionError(
            "evidence proof lock did not preserve the installed HAVE"
        )
    if result["denylisted"] or result["keep_searching"]:
        raise AssertionError("evidence proof lock reopened or punished source")
    for key in _EARLY_EXIT_REJECT_VALUES:
        if result[key] is not None:
            raise AssertionError(
                f"evidence proof lock leaked candidate reject {key}="
                f"{result[key]!r}"
            )


def assert_verified_lossless_has_affirmative_evidence(
    result: bool,
    *,
    spectral_grade: str | None,
    v0_probe_kind: str | None,
    v0_avg: int | None,
    v0_min: int | None,
) -> None:
    """Verification needs an affirmative grade or a qualifying disagreement."""
    if not result:
        return
    if spectral_grade in ("genuine", "marginal"):
        return
    override_qualifies = (
        spectral_grade in ("suspect", "likely_transcode")
        and v0_probe_kind == "lossless_source_v0"
        and v0_avg is not None
        and v0_avg >= 230
        and v0_min is not None
        and v0_min >= 200
    )
    if not override_qualifies:
        raise AssertionError(
            "verified lossless minted without affirmative spectral evidence: "
            f"grade={spectral_grade!r}, probe_kind={v0_probe_kind!r}, "
            f"avg={v0_avg!r}, min={v0_min!r}"
        )


def assert_unmapped_first_copy_stays_searchable(result: SimResult) -> None:
    """An unmapped exact-release first copy is retained without a ceiling."""
    if not result.imported or result.stage2_import != "import":
        raise AssertionError(
            f"unmapped first copy was not retained: {result!r}"
        )
    if result.final_status != "wanted" or not result.keep_searching:
        raise AssertionError(
            f"unmapped first copy became terminal: {result!r}"
        )
    if result.stage3_quality_gate == "accept":
        raise AssertionError(
            f"unmapped first copy claimed a quality ceiling: {result!r}"
        )
    if result.search_filetype_override_after == "lossless":
        raise AssertionError(
            f"unmapped first copy narrowed to lossless: {result!r}"
        )


def assert_only_strictly_lower_spectral_rejects(
    decision: str,
    *,
    grade: str,
    new_spectral: int,
    existing_spectral: int,
) -> None:
    """Stage-1 spectral pre-gate policy: a transcode-grade candidate rejects
    at Stage 1 ONLY when its spectral estimate is STRICTLY below the existing
    one.

    An EQUAL spectral floor is a tie on the single metric this coarse stage
    measures, not affirmative worse-content evidence — it must defer to Stage
    2's codec-aware comparison rather than reject (Mark DeNardo request 1308:
    a tie-reject discarded a strictly-higher-bitrate copy). A strictly-lower
    estimate is affirmative worse content and must reject. Both estimates are
    assumed positive (the caller only invokes this on nonzero pairs).
    """
    if new_spectral < existing_spectral:
        if decision != "reject":
            raise AssertionError(
                f"strictly-lower spectral {new_spectral} < {existing_spectral} "
                f"(grade={grade}) must reject at Stage 1, got {decision!r}"
            )
    else:
        # Equal or strictly-higher: never a Stage-1 reject.
        if decision == "reject":
            raise AssertionError(
                f"spectral {new_spectral} vs {existing_spectral} "
                f"(grade={grade}) is a tie or upgrade but was rejected at "
                f"Stage 1 instead of deferring to Stage 2: {decision!r}"
            )


def assert_existing_override_noop_under_shared_clamp(
    with_override: dict,
    without_override: dict,
) -> None:
    """When BOTH sides carry a spectral estimate, the existing-side spectral-
    floor ``override_min_bitrate`` must not change the Stage-2 outcome.

    ``_shared_spectral_bitrates`` already floors both sides symmetrically for
    rank, so the one-sided override would only poison the raw
    ``metric_tiebreak`` — comparing the candidate's inflated container bitrate
    against the existing's spectral floor and minting a phantom "better". This
    is the Deerhunter bug (download_log 37725, issue #813 Finding 1): an
    identical transcode read as an upgrade purely because the existing was
    floored and the candidate was not. Under the symmetric-representation gate
    the override is a strict no-op whenever the shared clamp governs.
    """
    a = with_override.get("comparison_basis") or {}
    b = without_override.get("comparison_basis") or {}
    if with_override["stage2_import"] != without_override["stage2_import"]:
        raise AssertionError(
            "existing-side spectral override changed the Stage-2 decision "
            f"under a shared spectral clamp: {with_override['stage2_import']!r} "
            f"(override) vs {without_override['stage2_import']!r} (none)"
        )
    if a.get("verdict") != b.get("verdict"):
        raise AssertionError(
            "existing-side spectral override changed the comparison verdict "
            f"under a shared spectral clamp: {a.get('verdict')!r} (override) "
            f"vs {b.get('verdict')!r} (none)"
        )


def _fixed_gate_state(state: QualityGateState) -> _QualityGateStateLoader:
    """A ``state_loader`` seam that always yields ``state``.

    A named factory rather than an inline lambda: the loader seam is a
    typed Protocol now (issue #1277 review), and binding the loop's state
    through a parameter both satisfies ruff's late-binding rule and keeps
    pyright's inference on ``QualityGateState``.
    """

    def _load(**_kwargs: object) -> QualityGateState:
        return state

    return _load


@dataclass(frozen=True)
class StageParityWorld:
    """Primitive candidate/existing facts shared by Stage 1
    (``spectral_import_decision``) and Stage 2 (``compare_quality``) for the
    SAME evidence — the world space issue #813 Finding 1's no-disagreement
    property patrols.

    Spectral estimates are kept ``<= their own container`` (``new_spectral
    <= new_container``, ``existing_spectral <= existing_container``) — the
    domain ``_shared_spectral_bitrates``/``compute_effective_override_bitrate``
    assume, since a cliff estimate is a pessimistic FLOOR on real content,
    never evidence of MORE content than the container itself measured. This
    is the same established convention as
    ``test_existing_spectral_override_is_noop_when_candidate_has_spectral``
    (which clamps ``candidate_spectral``/``existing_spectral`` to their own
    containers).

    ``spectral > own container`` IS reachable from an ordinary FRESH single
    measurement, not only via evidence carried forward across snapshots
    (correction, PR #827 review F3 — the original text here overclaimed
    cross-snapshot carry-forward as the only path). ``estimate_bitrate_from_
    cliff`` (``lib/spectral_check.py``) maps a per-track cliff frequency to
    a fixed, container-INDEPENDENT bucket (96/128/.../320), and
    ``analyze_album`` aggregates the album-level estimate as
    ``min(track estimates)`` over ONLY the tracks that had a cliff detected
    at all — tracks with no cliff are excluded from that ``min``, and the
    album's overall grade classification (``classify_album``'s 60%/75%
    suspect-percentage thresholds) is computed independently of this
    aggregation. So a single outlier track with a cliff at or above the
    highest lowpass bucket (≥19,550 Hz) yields an album spectral estimate
    of 320 even when the album's overall grade stays "genuine" (too few
    suspect tracks to cross the percentage threshold) and its real average
    container bitrate is much lower (e.g. 246). This domain remains a data-
    hygiene/aggregation-policy question in a different subsystem
    (``lib/spectral_check.py::analyze_album``'s own min-over-cliffed-tracks
    policy), not a Stage1/Stage2 decision disagreement — out of scope here,
    and precisely the domain this property does NOT reach, which is why
    Stage 1 remains load-bearing (issue #813 Finding 1 audit conclusion)
    even though this property finds zero disagreement inside its own
    (consistent-evidence) domain.

    ``new_format``/``existing_format`` are deliberately the SAME codec
    family on both sides (see ``stage_parity_worlds``) — but only because
    that is where the same-rank spectral tiebreak this property patrols
    lives, NOT because the cross-codec domain is safe.

    **CORRECTION (issue #829 Phase 5 PR2c).** This docstring used to
    justify the cross-codec exclusion by arguing that ``LAME_LOWPASS`` is
    MP3/LAME-calibrated and the preimport gate only fires on MP3-shaped
    candidates, so "a cross-codec spectral pairing is itself evidence of a
    mismatch … not an independent decision-logic gap", and that a fresh
    single measurement never produces one codec's spectral estimate paired
    against a DIFFERENT codec's raw measurement. **Every clause of that is
    false.** ``collect_attempt_spectral_audit`` measured EVERY codec
    through the LAME table and persisted the result as decision-facing
    evidence, so a fresh measurement produced exactly that pairing all the
    time — download 37946 (request 6387, Wavves — *Wavves*) is an ordinary
    AAC whose natural rolloff read as "MP3 128 transcode" and drove a live
    cross-codec clamp. It WAS an independent decision-logic gap, it was
    load-bearing in production, and issue #829's four-arm calibration
    (60,102 measurements) settled the question this docstring called out of
    scope: cutoff Hz is not a cross-codec currency (a 17 kHz cliff means
    ~160 kbps in MP3 and 256-320 in AAC), so the comparison is refused
    rather than rescaled. Phase 5 PR2b shipped the refusal
    (``spectral_classes_comparable``); ``inadmissible_spectral_pair_worlds``
    below patrols the domain this world type still excludes.

    **MIXED DERIVATION BASIS IS NOW IN SCOPE (issue #829 Phase 5 PR2d).**
    ``new_cliff_hz``/``existing_cliff_hz`` carry each side's raw cliff, so a
    world can pair a cliff-derived class against a legacy stored bucket
    WITHIN one codec family — the ``mixed_derivation_basis`` refusal, which
    is same-codec and therefore was never in the cross-codec property's
    domain either. It is where a Stage-1-ONLY defect can diverge from Stage
    2 at all: the comparability gate is the only seam that moves Stage 1
    without moving the shared clamp with it. Measured over 5,000 draws of
    ``stage_parity_worlds``: the shipped code produces ZERO contradictions
    while reaching 236 Stage-1 rejects, reverting the gate produces 5 (1 in
    1,000), the full pre-#829 seam (raw stored buckets AND no gate) 130 (1
    in 38), and raw stored buckets WITHOUT the gate revert 90 (1 in 55).

    That last figure corrects a claim an earlier draft of this PR made.
    Raw stored buckets change the class BOTH stages read, so it is tempting
    to reason that the stages move together and no parity property could
    ever see it. They do not: the comparability gate keeps Stage 1 on the
    real interpretation, so the change is asymmetric wherever a grade does
    not authorize a class, and this property kills it at both tiers. Do not
    infer from a mutant's inputs which stages it moves — measure.

    The class ≤ own container convention above is load-bearing for those
    measurements, not decoration. Over 25,000 draws of a probe domain that
    relaxes ONLY that constraint (a one-shot, not this strategy), the
    SHIPPED code reports 139 "contradictions" — one per ~180 — and every
    one is in the unbound / self-inconsistent evidence domain #828 item 1
    records as deliberately unpatrolled (``docs/quality-verification.md`` §
    "Stage 1 / Stage 2 parity"). The cliff strategy below therefore derives
    its bound through ``ladder_class_kbps``, the same function the decider
    uses.
    """
    grade: str
    new_container: int
    new_spectral: int
    existing_container: int
    existing_spectral: int
    existing_grade: str | None
    new_is_cbr: bool
    existing_is_cbr: bool
    new_format: str
    existing_format: str
    # Raw per-side cliffs. ``None`` on both sides is the legacy
    # stored-bucket world every pin below describes.
    new_cliff_hz: int | None = None
    existing_cliff_hz: int | None = None
    # Lossless-source dimensions, drawn only by ``stage1_rejecting_flac_worlds``
    # (issue #829 Phase 5 PR2d review S1): they select which of
    # ``full_pipeline_decision``'s three Stage-2 branches the counterfactual
    # runs, and the no-disagreement property above leaves them at the
    # native-lossy defaults.
    is_flac: bool = False
    target_format: str | None = None
    supported_lossless_source: bool | None = None


def _stage_parity_decision(world: StageParityWorld) -> dict[str, object]:
    """Drive the REAL decider over one ``StageParityWorld``.

    ``min_bitrate == avg_bitrate == container`` on each side, both spectral
    estimates carried as the stored bucket the world declares, and no
    existing-side override (the disarm identity has its own property). Every
    value the properties below read comes out of this one call.

    ``is_flac``/``target_format``/``supported_lossless_source`` default to
    the native-lossy shape and are drawn only by
    ``stage1_rejecting_flac_worlds`` — they choose which Stage-2 branch the
    counterfactual actually runs.
    """
    return full_pipeline_decision(
        is_flac=world.is_flac,
        target_format=world.target_format,
        supported_lossless_source=world.supported_lossless_source,
        min_bitrate=world.new_container,
        avg_bitrate=world.new_container,
        is_cbr=world.new_is_cbr,
        is_vbr=not world.new_is_cbr,
        new_format=world.new_format,
        spectral_grade=world.grade,
        spectral_bitrate=world.new_spectral,
        existing_min_bitrate=world.existing_container,
        existing_avg_bitrate=world.existing_container,
        existing_format=world.existing_format,
        existing_is_cbr=world.existing_is_cbr,
        existing_spectral_grade=world.existing_grade,
        existing_spectral_bitrate=world.existing_spectral,
        candidate_spectral_context=SpectralCodecContext(
            cliff_hz=world.new_cliff_hz),
        existing_spectral_context=SpectralCodecContext(
            cliff_hz=world.existing_cliff_hz),
        override_min_bitrate=None,
    )


def _stage_parity_deferred_decision(
    world: StageParityWorld,
) -> dict[str, object]:
    """The same world, decided with Stage 1's short-circuit lifted.

    The counterfactual reference for
    ``assert_counterfactual_is_the_deferred_stage2``. It uses production's
    OWN Stage-1 carve-out — ``provisional_source_candidate and
    has_provisional_probe_input``, the branch that lets a lossless-source
    candidate with probe evidence past a Stage-1 spectral reject — rather
    than a test-only switch. That carve-out is the only lever in
    ``full_pipeline_decision`` that disables the short-circuit while leaving
    every Stage-2 input untouched, and "untouched" is CHECKED, not assumed:
    ``test_the_stage1_carve_out_lever_does_not_move_stage_2`` drives the
    same pair over worlds where Stage 1 does not reject and requires the
    Stage-2 outcome to be identical.

    Why the two kwargs leave Stage 2 alone, in the native-lossy branch this
    world type describes: ``supported_lossless_source`` is read ONLY by
    ``provisional_source_candidate``, and ``candidate_v0_probe_avg`` only by
    ``has_provisional_probe_input`` and by the FLAC branches' provisional
    lane. The native-lossy branch's own lock passes ``candidate_probe=None``
    unconditionally, and ``is_flac`` — which chooses the branch — is
    unchanged.
    """
    return full_pipeline_decision(
        is_flac=False,
        min_bitrate=world.new_container,
        avg_bitrate=world.new_container,
        is_cbr=world.new_is_cbr,
        is_vbr=not world.new_is_cbr,
        new_format=world.new_format,
        spectral_grade=world.grade,
        spectral_bitrate=world.new_spectral,
        existing_min_bitrate=world.existing_container,
        existing_avg_bitrate=world.existing_container,
        existing_format=world.existing_format,
        existing_is_cbr=world.existing_is_cbr,
        existing_spectral_grade=world.existing_grade,
        existing_spectral_bitrate=world.existing_spectral,
        candidate_spectral_context=SpectralCodecContext(
            cliff_hz=world.new_cliff_hz),
        existing_spectral_context=SpectralCodecContext(
            cliff_hz=world.existing_cliff_hz),
        override_min_bitrate=None,
        supported_lossless_source=True,
        candidate_v0_probe_avg=world.new_container,
        candidate_v0_probe_kind="lossless_source_v0",
    )


def _stage_parity_verdicts(
    world: StageParityWorld,
) -> tuple[str | None, QualityComparisonBasis]:
    """Read Stage 1's and Stage 2's verdicts off ONE real decider run.

    Both come from ``full_pipeline_decision`` — the seam owner that
    ``full_pipeline_decision_from_evidence`` delegates to — never from a
    copy of its wiring (issue #829 Phase 5 PR2d). Stage 1's ``reject``
    short-circuits before Stage 2 runs, so the decider now reports the
    counterfactual Stage-2 verdict for exactly those worlds under
    ``comparison_basis_if_stage1_deferred``; that audit key is what makes
    this harness possible without a second implementation. Until PR2d it
    reproduced the decider's Stage-1 wiring inline and was blind by
    construction to every mutant living in ``lib/quality/pipeline.py``.
    """
    decision = _stage_parity_decision(world)
    stage1 = decision["stage1_spectral"]
    # ``None`` is a real Stage-1 outcome: the Stage-0 preimport gate did not
    # fire, so Stage 1 never ran. See the checker's docstring.
    assert stage1 is None or isinstance(stage1, str), repr(decision)
    raw_basis = (
        decision["comparison_basis"]
        if decision["comparison_basis"] is not None
        else decision["comparison_basis_if_stage1_deferred"]
    )
    # Both sides always carry a container here, so Stage 2 always compares.
    # A None basis means the world never reached the comparison at all,
    # which would make the property silently vacuous rather than passing.
    assert raw_basis is not None, repr(decision)
    return stage1, msgspec.convert(raw_basis, type=QualityComparisonBasis)


#: The three ways ``spectral_classes_comparable`` can refuse a pair whose
#: CANDIDATE side is decision-grade. Each is a ``SpectralComparabilityReason``
#: production emits, used here as the world's own declared shape so
#: ``TestInadmissiblePairDomainIsWhatItClaims`` can check the strategy
#: against the real refusal rather than against a comment.
_INADMISSIBLE_SHAPES = (
    "cross_codec_legacy_bucket",
    "mixed_derivation_basis",
    "right_not_decision_grade",
)

#: HAVE codec labels with no invertible ladder — AAC's cliff is a one-sided
#: content floor, Opus carries no signal, WMA is uncalibrated.
_NON_LADDER_HAVE_FORMATS = ("AAC", "Opus", "WMA")

#: Album verdicts that do NOT authorize a spectral finding
#: (``_grade_authorizes`` admits only ``SPECTRAL_TRANSCODE_GRADES``) AND can
#: still carry spectral numbers. Two omissions are deliberate, both Rule C
#: (``.claude/rules/test-fidelity.md``): ``None``, because an evidence row
#: with no grade may not carry a spectral bitrate at all; and ``"error"``,
#: because the only producer of an album-level ``error``
#: (``analyze_album``'s all-tracks-errored branch) returns
#: ``estimated_bitrate_kbps=None`` and no ``cliff_hz`` by construction.
#: A HAVE carrying either alongside a stored bucket is a world no producer
#: can write, and this domain exists for a HAVE whose numbers the pre-#829
#: seam really would have consumed. ``genuine`` with a real estimate is the
#: canonical live shape (Springsteen: CBR 320 graded ``genuine``,
#: spectral 96).
_NON_AUTHORIZING_GRADES = ("genuine", "marginal")

#: Every raw ``cliff_hz`` ``detect_cliff`` can return, DERIVED from the
#: production constants rather than transcribed (``.claude/rules/
#: test-fidelity.md`` Rule C). ``detect_cliff`` returns
#: ``slices[i - 1]["freq"]`` only after ``MIN_CLIFF_SLICES`` consecutive
#: steep gradients, so the last two ``SLICE_FREQS`` entries can never be a
#: cliff START — the reachable set is 12000-18500, and the live corpus
#: agrees exactly (max ``cliff_hz`` = 18500, zero rows above, and the
#: column has no operator override).
#:
#: Consequence worth stating rather than leaving implicit: the MP3 320
#: class (cliff >= 19250) and the Vorbis 160 class (cliff >= 19000) are
#: NOT reachable from a cliff at all. They exist only as legacy stored
#: buckets, which is how ``_LAME_BUCKET_CLASSES`` reaches them here.
_CLIFF_HZ_VALUES: tuple[int, ...] = tuple(SLICE_FREQS[:-MIN_CLIFF_SLICES])


@dataclass(frozen=True)
class InadmissibleSpectralPairWorld:
    """A Stage-1-reachable world whose two spectral classes may NOT be paired.

    The domain issue #828 item 1 asked for and issue #829 defined. Every
    world here satisfies three things BY CONSTRUCTION, so the property
    below never has to ask production whether its own precondition holds:

    * the CANDIDATE is an MP3 whose interpretation is decision-grade — an
      authorizing album verdict plus either a raw ``cliff_hz`` or a stored
      ``LAME_LOWPASS`` bucket. MP3 is not a simplification: since PR2b made
      ``spectral_gate_trigger`` codec-aware, ``stage0_gates_stage1`` is
      only true for MP3 and lossless candidates, and a lossless container
      never yields a kbps class — so an MP3 candidate is the ONLY shape
      that can reach a Stage-1 spectral REJECTION (the checker's clause 1).
      A lossless candidate still reaches Stage 1 and can still trip clause
      2, which this domain does not cover; see
      ``test_every_generated_candidate_reaches_stage_1_with_a_class``;
    * the preimport gate would actually fire (CBR, or VBR below
      ``cfg.mp3_vbr_spectral_gate_kbps``), so ``stage1_spectral`` is a real
      verdict rather than ``None``;
    * the HAVE carries real spectral evidence that the pre-#829 seam WOULD
      have consumed, while ``spectral_classes_comparable`` refuses it —
      for the reason named in ``shape``.

    Containers are drawn free of the spectral values. Stage 1 never
    consults a container (evidence-set parity: it compares spectral
    against spectral), and ``spectral > own container`` is reachable from
    an ordinary fresh measurement anyway — see ``StageParityWorld``.
    """

    shape: str
    grade: str
    new_container: int
    new_spectral: int | None
    new_cliff_hz: int | None
    new_is_cbr: bool
    existing_format: str
    existing_grade: str
    existing_container: int
    existing_spectral: int | None
    existing_cliff_hz: int | None
    existing_is_cbr: bool


def _inadmissible_pair_stage1(
    world: InadmissibleSpectralPairWorld,
    *,
    withhold_existing_spectral: bool,
) -> str | None:
    """Drive the REAL Stage-1 seam and return its verdict.

    Drives ``full_pipeline_decision`` — the seam's owner, which
    ``full_pipeline_decision_from_evidence`` (the function the importer
    calls) delegates to. Until issue #829 Phase 5 PR2d this was the only
    Stage-1 property that did: ``_stage_parity_verdicts`` reproduced the
    decider's Stage-1 wiring inline and was blind by construction to any
    mutant planted in it. It now drives the same owner, so the two
    properties differ in INVARIANT, not in fidelity.

    ``withhold_existing_spectral`` removes the installed copy's spectral
    evidence entirely — grade, stored bucket and raw cliff — which is the
    counterfactual the invariant is stated against.
    """
    decision = full_pipeline_decision(
        is_flac=False,
        min_bitrate=world.new_container,
        avg_bitrate=world.new_container,
        is_cbr=world.new_is_cbr,
        is_vbr=not world.new_is_cbr,
        new_format="MP3",
        spectral_grade=world.grade,
        spectral_bitrate=world.new_spectral,
        candidate_spectral_context=SpectralCodecContext(
            cliff_hz=world.new_cliff_hz,
        ),
        existing_min_bitrate=world.existing_container,
        existing_avg_bitrate=world.existing_container,
        existing_format=world.existing_format,
        existing_is_cbr=world.existing_is_cbr,
        existing_spectral_grade=(
            None if withhold_existing_spectral else world.existing_grade
        ),
        existing_spectral_bitrate=(
            None if withhold_existing_spectral else world.existing_spectral
        ),
        existing_spectral_context=SpectralCodecContext(
            cliff_hz=(
                None if withhold_existing_spectral else world.existing_cliff_hz
            ),
        ),
        override_min_bitrate=None,
    )
    return decision["stage1_spectral"]


@dataclass(frozen=True)
class HaveRepresentationWorld:
    """An installed copy whose own decision-grade class contradicts its container.

    The world space for the disarm identity (issue #829 Phase 5 PR2c item
    6). The HAVE is decision-grade BY CONSTRUCTION — a ladder family with
    an authorizing album verdict and either a raw ``cliff_hz`` or a stored
    ``LAME_LOWPASS`` bucket — so exactly one representation mechanism must
    always fire for it. The CANDIDATE is deliberately free, because which
    mechanism fires depends entirely on whether the candidate's own class
    is comparable against the HAVE's.

    ``existing_is_cbr`` is fixed True and that is a real scope limit, not
    an oversight: ``build_existing_quality_measurement`` clamps avg/median
    from the override only for CBR albums, deliberately, so a VBR HAVE
    keeps its real avg and a stale spectral floor cannot erase a genuine
    rank signal. On a VBR HAVE the raw avg legitimately survives the
    override and this invariant does not hold.
    """

    candidate_grade: str | None
    candidate_container: int
    candidate_spectral: int | None
    candidate_cliff_hz: int | None
    candidate_is_cbr: bool
    existing_format: str
    existing_grade: str
    existing_container: int
    existing_spectral: int | None
    existing_cliff_hz: int | None


def _have_representation_decision(
    world: HaveRepresentationWorld,
) -> tuple[dict[str, object], int]:
    """Drive the real decider the way production's callers drive it.

    The override is DERIVED with ``compute_effective_override_bitrate``
    over the real interpretation — the same function
    ``lib/import_preview.py`` and ``lib/dispatch/quality_gate.py`` call to
    produce it — never a literal, so the world cannot feed the seam a value
    no caller would compute (``.claude/rules/test-fidelity.md`` Rule C).
    """
    existing_interpretation = interpret_spectral_evidence(SpectralEvidenceFacts(
        spectral_grade=world.existing_grade,
        format=world.existing_format,
        cliff_hz=world.existing_cliff_hz,
        spectral_bitrate_kbps=world.existing_spectral,
    ))
    class_kbps = decision_class_kbps(existing_interpretation)
    # Guaranteed by the strategy; asserted so a strategy regression is loud
    # rather than a silently vacuous property.
    assert class_kbps is not None, repr(world)
    decision = full_pipeline_decision(
        is_flac=False,
        min_bitrate=world.candidate_container,
        avg_bitrate=world.candidate_container,
        is_cbr=world.candidate_is_cbr,
        is_vbr=not world.candidate_is_cbr,
        new_format="MP3",
        spectral_grade=world.candidate_grade,
        spectral_bitrate=world.candidate_spectral,
        candidate_spectral_context=SpectralCodecContext(
            cliff_hz=world.candidate_cliff_hz,
        ),
        existing_min_bitrate=world.existing_container,
        existing_avg_bitrate=world.existing_container,
        existing_format=world.existing_format,
        existing_is_cbr=True,
        existing_spectral_grade=world.existing_grade,
        existing_spectral_bitrate=world.existing_spectral,
        existing_spectral_context=SpectralCodecContext(
            cliff_hz=world.existing_cliff_hz,
        ),
        override_min_bitrate=compute_effective_override_bitrate(
            world.existing_container, existing_interpretation,
        ),
    )
    return decision, class_kbps


def _inadmissible_pair_comparability(
    world: InadmissibleSpectralPairWorld,
) -> SpectralComparability:
    """Ask production whether this world's pair really is inadmissible.

    Used ONLY by the domain pin, never by the property: a checker that
    asks the function under test whether its own precondition holds goes
    vacuous the moment that function is the thing that broke.
    """
    return spectral_classes_comparable(
        interpret_spectral_evidence(SpectralEvidenceFacts(
            spectral_grade=world.grade,
            format="MP3",
            cliff_hz=world.new_cliff_hz,
            spectral_bitrate_kbps=world.new_spectral,
        )),
        interpret_spectral_evidence(SpectralEvidenceFacts(
            spectral_grade=world.existing_grade,
            format=world.existing_format,
            cliff_hz=world.existing_cliff_hz,
            spectral_bitrate_kbps=world.existing_spectral,
        )),
    )


def assert_stage1_never_contradicts_stage2(
    stage1: str | None, stage2: QualityComparisonBasis,
) -> None:
    """Issue #813 Finding 1 — the core no-disagreement parity contract.

    Operational semantics, from how ``full_pipeline_decision`` actually
    combines the two stages (``lib/quality/pipeline.py``): Stage 1's ONLY
    gating effect is its ``"reject"`` verdict, which short-circuits BEFORE
    Stage 2 ever runs. Every other Stage 1 verdict (``"import"``,
    ``"import_upgrade"``, ``"import_no_exist"``) defers unconditionally —
    Stage 2 decides. So the only verdict pair that can operationally
    "disagree" is Stage 1 rejecting a candidate that Stage 2, given the
    same evidence, would score ``"better"`` (an upgrade Stage 1's
    short-circuit would have discarded) — exactly the shape of both the
    Mark DeNardo (#812) and this PR's remaining same-rank-tiebreak bug.
    Stage 1 rejecting while Stage 2 says ``"worse"``/``"equivalent"`` is NOT
    a disagreement: both stages agree the candidate should not be accepted.

    ``stage1 is None`` means the Stage-0 preimport gate never fired for this
    candidate at all (a non-MP3 codec, or a VBR MP3 whose average is at or
    above ``cfg.mp3_vbr_spectral_gate_kbps``), so Stage 1 has no verdict to
    contradict with. That case became VISIBLE only in issue #829 Phase 5
    PR2d: the harness used to reproduce the decider's Stage-1 wiring inline
    and computed a Stage-1 verdict for worlds where production skips the
    gate entirely.
    """
    if stage1 == "reject" and stage2.verdict == "better":
        raise AssertionError(
            "Stage 1 rejected a candidate Stage 2 scores as an upgrade: "
            f"stage1={stage1!r} stage2.verdict={stage2.verdict!r} "
            f"stage2.branch={stage2.branch!r}"
        )


#: The decision fields a Stage-1 short-circuit owns, and the exact value each
#: one must hold. Derived from ``full_pipeline_decision``'s Stage-1 reject
#: branch, which writes ``final_status``/``keep_searching`` and returns
#: through ``_finalize_denylist`` without ever entering Stage 2 — so every
#: Stage-2/Stage-3 output stays at its initialised ``None``.
_STAGE1_REJECT_DECISION_FIELDS: dict[str, object] = {
    "stage2_import": None,
    "stage3_quality_gate": None,
    "comparison_basis": None,
    "target_final_format": None,
    "final_status": "wanted",
    "keep_searching": True,
    "imported": False,
}

#: The audit-only keys. Reporting, never a decision input.
_STAGE2_COUNTERFACTUAL_KEYS = (
    "stage2_import_if_stage1_deferred",
    "comparison_basis_if_stage1_deferred",
)


def assert_stage1_reject_leaks_no_stage2_state(
    decision: dict[str, object],
    *,
    context: str = "",
) -> None:
    """Issue #829 Phase 5 PR2d — the audit field is INERT.

    ``full_pipeline_decision`` now runs Stage 2 even when Stage 1
    short-circuits, so that the operator can be told "Stage 1 rejected
    this, and Stage 2 would have said X" — the disagreement issue #813 is
    about, which until PR2d was computed nowhere. The whole safety of that
    change is that the counterfactual run touches NOTHING except the two
    ``*_if_stage1_deferred`` keys: it decides on a throwaway result dict
    and exactly two values are lifted back.

    So on a Stage-1 short-circuit every decision field must still hold
    exactly what the reject branch writes, with every Stage-2/Stage-3
    output left at ``None``. A single leaked value — a ``stage2_import``
    copied across, a ``comparison_basis`` populated, an ``imported`` flipped
    — is a decision change, whatever the audit keys say.
    """
    leaks = [
        f"{field}={decision.get(field)!r} (expected {expected!r})"
        for field, expected in _STAGE1_REJECT_DECISION_FIELDS.items()
        if decision.get(field) != expected
    ]
    if leaks:
        raise AssertionError(
            "Stage-2 state leaked onto a Stage-1 reject decision: "
            + "; ".join(leaks)
            + (f" [{context}]" if context else "")
        )


def assert_counterfactual_is_the_deferred_stage2(
    short_circuited: dict[str, object],
    deferred: dict[str, object],
    *,
    context: str = "",
) -> None:
    """Issue #829 Phase 5 PR2d — the audit field is TRUE, not fabricated.

    The reported counterfactual must be what Stage 2 actually produces for
    the same world once Stage 1's short-circuit is lifted — not a
    plausible-looking value computed some other way. ``deferred`` is the
    same call with production's OWN Stage-1 carve-out engaged (see
    ``_stage_parity_deferred_decision``), which is the only lever in the
    function that disables the short-circuit without touching a single
    Stage-2 input.
    """
    mismatches = [
        f"{audit_key}={short_circuited.get(audit_key)!r} but the deferred run "
        f"produced {deferred.get(real_key)!r}"
        for audit_key, real_key in zip(
            _STAGE2_COUNTERFACTUAL_KEYS,
            ("stage2_import", "comparison_basis"),
            strict=True,
        )
        if short_circuited.get(audit_key) != deferred.get(real_key)
    ]
    if mismatches:
        raise AssertionError(
            "the reported Stage-2 counterfactual is not what Stage 2 decides: "
            + "; ".join(mismatches)
            + (f" [{context}]" if context else "")
        )


def assert_carve_out_lever_is_stage2_inert(
    decision: dict[str, object],
    levered: dict[str, object],
    *,
    context: str = "",
) -> None:
    """The Stage-1 carve-out lever disables the short-circuit and nothing else.

    Qualifies the counterfactual reference used by
    ``assert_counterfactual_is_the_deferred_stage2``: over worlds where Stage
    1 does not short-circuit anyway, the levered and unlevered runs must reach
    the same Stage-2 decision and the same basis.
    """
    if (
        decision["stage2_import"] != levered["stage2_import"]
        or decision["comparison_basis"] != levered["comparison_basis"]
    ):
        raise AssertionError(
            "the Stage-1 carve-out lever moved Stage 2: "
            f"{decision['stage2_import']!r}/{decision['comparison_basis']!r} "
            f"vs {levered['stage2_import']!r}/"
            f"{levered['comparison_basis']!r}"
            + (f" [{context}]" if context else "")
        )


def assert_counterfactual_reported_exactly_when_stage1_short_circuits(
    decision: dict[str, object],
    *,
    context: str = "",
) -> None:
    """Issue #829 Phase 5 PR2d — the audit keys exist, and say something
    exactly where they mean something. BOTH directions.

    * Both keys are part of the documented result shape on EVERY path.
    * A counterfactual reported next to a REAL ``stage2_import`` would be a
      second, contradictory answer to the same question.
    * A short-circuit ALWAYS reports a decision — if only
      ``STAGE2_COUNTERFACTUAL_UNAVAILABLE``. Without this clause a
      counterfactual that could not be evaluated is byte-identical to "Stage
      1 never short-circuited", which is a different fact, and nothing would
      notice the difference (PR2d review S2). The BASIS is deliberately
      exempt: it is legitimately ``None`` whenever Stage 2 never reached a
      comparison (the provisional lane, the lossless-source lock).
    """
    missing = [key for key in _STAGE2_COUNTERFACTUAL_KEYS if key not in decision]
    if missing:
        raise AssertionError(
            f"decision dict is missing audit keys {missing}"
            + (f" [{context}]" if context else "")
        )
    if decision.get("stage2_import") is None:
        # A Stage-1 reject with no real Stage-2 decision IS the
        # short-circuit — the same condition ``evidence_decision_name``
        # reads to call the outcome ``spectral_reject``.
        if decision.get("stage1_spectral") == "reject" and (
            decision["stage2_import_if_stage1_deferred"] is None
        ):
            raise AssertionError(
                "Stage 1 short-circuited but reported no Stage-2 "
                "counterfactual at all — 'could not be evaluated' and "
                "'nothing to report' must not look identical"
                + (f" [{context}]" if context else "")
            )
        return
    populated = [
        f"{key}={decision[key]!r}"
        for key in _STAGE2_COUNTERFACTUAL_KEYS
        if decision[key] is not None
    ]
    if populated:
        raise AssertionError(
            "a Stage-2 counterfactual was reported alongside a real Stage-2 "
            "decision: " + "; ".join(populated)
            + (f" [{context}]" if context else "")
        )


def assert_have_is_represented_by_its_own_class(
    existing_rank: str,
    class_rank: str,
    *,
    context: str = "",
) -> None:
    """Issue #829 Phase 5 PR2c — the "never neither" half of the disarm identity.

    Two mechanisms can represent an installed album by its real content
    rather than its container: the symmetric clamp inside
    ``_shared_spectral_bitrates``, and the one-sided
    ``override_min_bitrate``. ``full_pipeline_decision`` disarms the
    one-sided override precisely when the clamp governs instead, and the
    two predicates being the SAME condition is the whole argument — the
    override is safe to drop only because something else then represents
    the installed album by its own content.

    **So exactly one of them always fires**, and the observable consequence
    is that a HAVE carrying a decision-grade class is never RANKED above
    that class. Widen the disarm to the plausible-looking "both sides have
    a class" and a window opens where neither mechanism fires and a
    known-fake 320 keeps its inflated ``transparent`` rank — download_log
    29525, Clue to Kalo *Lily Perdida*, 132 of 9,219 live pairs.

    The complementary "never BOTH" half is
    ``assert_existing_override_noop_under_shared_clamp``. That one was
    already patrolled by a generated property; this one shipped with
    deterministic pins only (``TestMixedBasisDisarmWindow``), so a mutant
    confined to the disarm predicate was caught by nothing generated.

    Ranks, not values: ``metric_tiebreak`` deliberately falls back to the
    RAW configured metric so equal spectral buckets can still converge
    upward by bitrate (Mark DeNardo request 1308), which means the
    displayed ``existing_value_kbps`` legitimately exceeds the class on
    that branch. Rank is what governs the verdict and is the honest
    invariant.
    """
    existing = QualityRank[existing_rank.upper()]
    bound = QualityRank[class_rank.upper()]
    if existing > bound:
        where = f" [{context}]" if context else ""
        raise AssertionError(
            "the installed copy was ranked above its own spectral class"
            f"{where}: existing_rank={existing_rank!r} but its class alone "
            f"ranks {class_rank!r} — neither the symmetric clamp nor the "
            "one-sided override represented it by its real content"
        )


def assert_stage1_ignores_inadmissible_existing_spectral(
    stage1: str | None,
    stage1_without_existing_spectral: str | None,
    *,
    context: str = "",
) -> None:
    """Issue #829 Phase 5 PR2c — the cross-codec half of the parity contract.

    **Stage 1 must not reject on a spectral comparison Stage 2 is not
    permitted to make.** Whenever ``spectral_classes_comparable`` refuses
    the pair, the existing side's spectral evidence is inadmissible, and
    inadmissible evidence must move NOTHING: Stage 1's verdict has to be
    the verdict the same world produces with that evidence absent
    entirely. The research finding this encodes, verbatim from
    ``docs/research/spectral-calibration-findings.md``: "cross-codec
    spectral comparison is undefined and fails closed", not a translation
    table.

    Two clauses, in the order they matter:

    1. **No Stage-1 rejection.** Stage 1's only operative effect is
       ``"reject"``, which short-circuits ``full_pipeline_decision``
       before Stage 2 runs (denylist the source, stay ``wanted``). A
       rejection built on a class Stage 2 refuses to weigh is the #829
       defect at the parity seam: download 37946's shape pointed at the
       candidate instead of the library.
    2. **Withheld evidence moves nothing.** The silent direction clause 1
       misses: an inadmissible class turning ``import_no_exist`` into
       ``import_upgrade`` changes no outcome today, but it means the seam
       consumed evidence it may not see.

    Neither clause subsumes the other, which is why both are asserted.
    Clause 2 alone would let a mutant that fabricates an existing class
    from nothing pass, because BOTH runs would then reject and the two
    verdicts would agree — the third case in the known-bad self-test.

    Clause 1 also makes this property subsume ``assert_stage1_never_
    contradicts_stage2`` on this whole domain, for EVERY possible Stage 2:
    that checker's antecedent is ``stage1 == "reject"``, which clause 1
    forbids outright. That is why the old checker is not re-run here — not
    because it cannot fire on cross-codec worlds. It can: a cross-codec
    pair at DIFFERENT ranks takes ``compare_quality``'s ``rank`` branch,
    where ``"better"`` is perfectly reachable. Only the
    ``cross_family_same_rank`` branch is structurally ``"equivalent"``.
    """
    where = f" [{context}]" if context else ""
    if stage1 == "reject":
        raise AssertionError(
            "Stage 1 rejected on a spectral comparison Stage 2 is not "
            f"permitted to make{where}: the two spectral classes are not "
            "comparable, so the existing side contributes no class at all"
        )
    if stage1 != stage1_without_existing_spectral:
        raise AssertionError(
            "an inadmissible existing-side spectral class changed the "
            f"Stage 1 verdict{where}: {stage1!r} (evidence present) vs "
            f"{stage1_without_existing_spectral!r} (evidence withheld)"
        )


_MEASURED_STAGE2_DECISIONS = frozenset({
    "import", "downgrade", "transcode_upgrade", "transcode_downgrade",
    "transcode_first",
})
_BASIS_SAME_RANK_BRANCHES = frozenset({
    "lossless_same_rank", "cross_family_same_rank",
    "label_contract_same_rank", "spectral_tiebreak", "metric_tiebreak",
    "metric_missing",
})
_BASIS_METRICS = frozenset({"min", "avg", "median", "contract"})


def assert_basis_consistent(result: SimResult) -> None:
    """The persisted comparison basis can never contradict the decision it
    explains (request 6039 — the anti-display-lie invariants I2/I3/I4)."""
    basis = result.comparison_basis
    stage2 = result.stage2_import
    if basis is None:
        # Only decisions that REQUIRE a comparison must carry one:
        # downgrade/transcode_downgrade/transcode_upgrade are unreachable
        # without an existing album; import/transcode_first are not.
        if stage2 in ("downgrade", "transcode_downgrade", "transcode_upgrade"):
            raise AssertionError(
                f"stage2={stage2!r} requires a comparison but lost its basis")
        return
    if stage2 not in _MEASURED_STAGE2_DECISIONS or stage2 == "transcode_first":
        raise AssertionError(
            f"basis present on non-compared stage2 {stage2!r}")
    if basis["branch"] not in COMPARISON_BASIS_BRANCHES:
        raise AssertionError(f"unknown basis branch: {basis['branch']!r}")
    if (basis["new_metric"] not in _BASIS_METRICS
            or basis["existing_metric"] not in _BASIS_METRICS):
        raise AssertionError(f"malformed basis metrics: {basis!r}")
    verdict = basis["verdict"]
    if stage2 in ("import", "transcode_upgrade"):
        imports_ok = verdict == "better" or (
            verdict == "equivalent" and basis["verified_lossless_bypass"])
        if not imports_ok:
            raise AssertionError(
                f"import decision contradicts basis verdict: {basis!r}")
    else:  # downgrade / transcode_downgrade
        if verdict not in ("worse", "equivalent"):
            raise AssertionError(
                f"reject decision contradicts basis verdict: {basis!r}")
        if basis["verified_lossless_bypass"]:
            raise AssertionError(
                f"reject decision claims a verified-lossless bypass: {basis!r}")
    branch = basis["branch"]
    if branch == "rank" and basis["new_rank"] == basis["existing_rank"]:
        raise AssertionError(f"rank branch with equal ranks: {basis!r}")
    if branch == "rank_within_tolerance":
        # Issue #1145 H2: the one branch where the ranks DIFFER and the
        # verdict is still equivalent. Equal ranks would mean the ordinary
        # same-rank tiebreak should have fired instead, and any other verdict
        # would contradict the branch's whole meaning.
        if basis["new_rank"] == basis["existing_rank"]:
            raise AssertionError(
                f"within-tolerance branch with equal ranks: {basis!r}")
        if verdict != "equivalent":
            raise AssertionError(
                f"within-tolerance branch is not equivalent: {basis!r}")
    if (branch in _BASIS_SAME_RANK_BRANCHES
            and basis["new_rank"] != basis["existing_rank"]):
        raise AssertionError(f"same-rank branch with differing ranks: {basis!r}")
    if branch == "transcode_rank_regression" and verdict != "worse":
        raise AssertionError(
            f"transcode rank regression must be worse: {basis!r}")


_COMPARED_STAGE2_DECISIONS = (
    "import", "downgrade", "transcode_upgrade", "transcode_downgrade",
)


def assert_measured_decision_carries_basis(result: SimResult) -> None:
    """A measured decision against an existing album always explains itself."""
    if (
        result.stage2_import in _COMPARED_STAGE2_DECISIONS
        and result.comparison_basis is None
    ):
        raise AssertionError(
            f"measured decision {result.stage2_import!r} against an "
            f"existing album lost its comparison basis: {result!r}")


def assert_basis_metrics_truthful(
    album: AlbumState, download: DownloadScenario, result: SimResult,
) -> None:
    """A basis side never claims a statistic the world didn't measure.

    Download_log 36660: the decision layer synthesized the compared
    candidate measurement with avg fabricated = the post-conversion MIN,
    so the persisted basis read "avg 216k" beside an honest "255kbps avg"
    V0-probe row on the same card. The rule: an explicit target is a
    ``contract``; otherwise the flat decision interface carries a real
    candidate avg only on the native-lossy path, and FLAC paths classify the
    post-conversion min and must say "min". The
    existing side has a real avg only when the album measured one, except
    the deliberate CBR spectral-override clamp (its own pinned policy,
    where a CBR album's avg IS its min). "median" never crosses the flat
    interface on either side.
    """
    basis = result.comparison_basis
    if basis is None:
        return
    if "median" in (basis["new_metric"], basis["existing_metric"]):
        raise AssertionError(
            f"median never crosses the flat interface: {basis!r}")
    if basis["new_metric"] == "avg" and (
            download.is_flac or download.avg_bitrate is None):
        raise AssertionError(
            f"candidate basis claims 'avg' but the world measured none: {basis!r}")
    if basis["existing_metric"] == "avg" and album.avg_bitrate is None:
        clamped_cbr = album.is_cbr and compute_effective_override_bitrate(
            album.min_bitrate,
            interpret_spectral_evidence(SpectralEvidenceFacts(
                spectral_grade=album.spectral_grade,
                spectral_bitrate_kbps=album.spectral_bitrate,
                # The same label ``simulate`` feeds the decider, so this
                # mirror cannot resolve a different codec than the run did.
                format=_derive_album_format(album),
            )),
        ) != album.min_bitrate
        if not clamped_cbr:
            raise AssertionError(
                f"existing basis claims 'avg' but the album measured none: {basis!r}")


_PARITY_FIELDS = (
    "imported",
    "keep_searching",
    "denylisted",
    "final_status",
    "stage0_spectral_gate",
    "stage1_spectral",
    "stage2_import",
    "stage3_quality_gate",
    "comparison_basis",
    "installed_incomplete_disregarded",
)


def assert_twins_agree(sim: SimResult, evidence_result: dict) -> None:
    """The parity contract: same world → same outcome from both twins."""
    diffs = []
    for field in _PARITY_FIELDS:
        sim_value = getattr(sim, field)
        ev_value = evidence_result.get(field)
        if sim_value != ev_value:
            diffs.append(f"{field}: simulator={sim_value!r} evidence={ev_value!r}")
    if diffs:
        raise AssertionError(
            "decision twins diverged on the same world:\n  " + "\n  ".join(diffs))


# ===========================================================================
# Wild simulator-space strategies (totality + policy invariants)
#
# Deliberately NO plausibility filters beyond what the types require: the
# V0-evidence bug (fix 6cf26a4) lived in a state a "plausible worlds only"
# generator would have skipped. Anything the schema can express is fair.
# ===========================================================================

@st.composite
def album_states(draw) -> AlbumState:
    return AlbumState(
        name="generated_album",
        min_bitrate=draw(_optional_bitrates(max_value=4000)),
        is_cbr=draw(st.booleans()),
        spectral_grade=draw(st.sampled_from(_GRADES)),
        spectral_bitrate=draw(_optional_bitrates(max_value=4000)),
        verified_lossless=draw(st.booleans()),
        search_filetype_override=draw(
            st.sampled_from((None, "lossless", QUALITY_UPGRADE_TIERS))),
        target_format=draw(st.sampled_from(_TARGET_FORMATS)),
        existing_format=draw(
            st.sampled_from((None, *_CURRENT_FORMATS))),
        avg_bitrate=draw(_optional_bitrates(max_value=4000)),
        existing_v0_probe_avg=draw(_optional_bitrates(max_value=4000)),
    )


@st.composite
def download_scenarios(draw) -> DownloadScenario:
    is_flac = draw(st.booleans())
    converted_count = draw(st.integers(min_value=0, max_value=30)) if is_flac else 0
    post_conversion_min_bitrate = (
        draw(_optional_bitrates(max_value=400)) if is_flac else None
    )
    return DownloadScenario(
        name="generated_download",
        is_flac=is_flac,
        min_bitrate=draw(_bitrates(max_value=4000)),
        is_cbr=draw(st.booleans()),
        spectral_grade=draw(st.sampled_from(_GRADES)),
        spectral_bitrate=draw(_optional_bitrates(max_value=4000)),
        converted_count=converted_count,
        post_conversion_min_bitrate=post_conversion_min_bitrate,
        post_conversion_is_cbr=(
            draw(st.booleans())
            if is_flac
            and (converted_count > 0 or post_conversion_min_bitrate is not None)
            else None
        ),
        new_format=(None if is_flac else draw(st.sampled_from(_LOSSY_FORMATS))),
        is_vbr=draw(st.sampled_from((None, True, False))),
        avg_bitrate=draw(_optional_bitrates(max_value=4000)),
        candidate_v0_probe_avg=draw(_optional_bitrates(max_value=400)),
        candidate_v0_probe_min=draw(_optional_bitrates(max_value=400)),
    )


@st.composite
def raw_verified_lossless_albums(draw) -> AlbumState:
    """Existing album: raw verified-lossless FLAC on disk.

    Grades are limited to the clean verified shapes — contradictory states
    (verified_lossless=True + likely_transcode) are covered by the totality
    property, not this policy assertion.
    """
    return AlbumState(
        name="generated_raw_flac",
        min_bitrate=draw(_bitrates(min_value=500, max_value=4000)),
        is_cbr=False,
        spectral_grade=draw(st.sampled_from((None, "genuine"))),
        spectral_bitrate=None,
        verified_lossless=True,
        search_filetype_override=None,
        existing_format="FLAC",
        avg_bitrate=None,
    )


@st.composite
def lossy_downloads(draw) -> DownloadScenario:
    return DownloadScenario(
        name="generated_lossy",
        is_flac=False,
        min_bitrate=draw(_bitrates(max_value=2000)),
        is_cbr=draw(st.booleans()),
        spectral_grade=draw(st.sampled_from(_GRADES)),
        spectral_bitrate=draw(_optional_bitrates(max_value=400)),
        new_format=draw(st.sampled_from(_LOSSY_FORMATS)),
        is_vbr=draw(st.sampled_from((None, True, False))),
        avg_bitrate=draw(_optional_bitrates(max_value=2000)),
    )


@st.composite
def obvious_lower_rank_lossy_downloads(draw) -> DownloadScenario:
    bitrate = draw(_bitrates(max_value=190))
    is_cbr = draw(st.booleans())
    return DownloadScenario(
        name="generated_lower_rank_lossy",
        is_flac=False,
        min_bitrate=bitrate,
        is_cbr=is_cbr,
        spectral_grade=draw(st.sampled_from(_GRADES)),
        spectral_bitrate=draw(_optional_bitrates(max_value=190)),
        new_format=draw(st.sampled_from(_LOSSY_FORMATS)),
        is_vbr=not is_cbr,
        avg_bitrate=bitrate,
    )


#: The only values ``estimate_bitrate_from_cliff`` can emit. A spectral
#: number outside this set is not a legacy bucket, carries no class under
#: the codec-aware interpretation (issue #829 Phase 5 PR2b), and so
#: describes a world where the clamp these properties patrol never fires.
_LAME_BUCKET_CLASSES = (96, 112, 128, 160, 192, 224, 256, 320)

#: The lossy families with an invertible class ladder — the only ones whose
#: spectral evidence can reach the shared clamp at all. See
#: ``stage_parity_worlds``.
_LADDER_FORMATS = ("MP3", "Vorbis")


def _lame_buckets_at_or_below(container: int) -> st.SearchStrategy[int]:
    """A producible spectral class no higher than its own container."""
    candidates = [b for b in _LAME_BUCKET_CLASSES if b <= container]
    return st.sampled_from(candidates or [_LAME_BUCKET_CLASSES[0]])


def _cliffs_classing_at_or_below(
    codec_family: CodecFamily, container: int,
) -> st.SearchStrategy[int | None]:
    """A raw cliff whose DERIVED class stays within its own container.

    The bound is computed by ``ladder_class_kbps`` — the same function the
    decider uses — never by a transcribed table
    (``.claude/rules/test-fidelity.md`` Rule C). ``None`` (no cliff, so the
    class comes from the legacy stored bucket instead) is always offered:
    it is what makes a MIXED-derivation pair drawable.
    """
    usable = [
        cliff_hz for cliff_hz in _CLIFF_HZ_VALUES
        if (ladder_class_kbps(codec_family, cliff_hz) or 0) <= container
    ]
    return st.sampled_from([None, *usable])


@st.composite
def stage_parity_worlds(draw) -> StageParityWorld:
    """Worlds for the issue #813 Finding 1 no-disagreement property.

    Only ``suspect``/``likely_transcode`` grades ever let Stage 1 reject
    (``spectral_import_decision`` returns ``"import"`` unconditionally for
    every other grade) — sampling only these two focuses the search on the
    space where a disagreement could exist, the same convention
    ``test_only_strictly_lower_spectral_rejects_at_stage1`` already uses.
    ``existing_grade`` is free. Format is shared across both sides (like
    ``test_existing_spectral_override_is_noop_when_candidate_has_spectral``'s
    fixed ``"MP3"``) so the search spends its budget on the same-codec-
    family same-rank tiebreak this property patrols, rather than diluting
    across the ``cross_family_same_rank`` branch, which returns
    ``"equivalent"`` unconditionally and so can never disagree with a
    Stage-1 reject.

    **That is a fact about the BRANCH, not about cross-codec worlds**
    (correction, issue #829 Phase 5 PR2c — the original wording invited the
    wider reading). ``cross_family_same_rank`` only fires at the SAME rank;
    a cross-codec pair at DIFFERENT ranks takes ``compare_quality``'s
    ``rank`` branch, where ``"better"`` is perfectly reachable, so a
    cross-codec world CAN trip ``assert_stage1_never_contradicts_stage2``.
    The negative is a code fact, not a sample: ``cross_family_same_rank``
    hardcodes ``"equivalent"`` in ``lib/quality/compare.py``, so that
    branch alone can never disagree — while ``rank`` and ``metric_tiebreak``
    can, and are both reachable cross-codec. The one-class spectral branches
    are deliberately same-family only. Measured over a 46,286-world sweep of
    MP3-candidate worlds simulating the pre-PR2b Stage-1 seam: 1,142 worlds
    flipped Stage 1 to ``"reject"`` and 326 of those carried a Stage-2
    ``"better"``, predominantly through ``rank``. That domain is patrolled by
    ``inadmissible_spectral_pair_worlds`` /
    ``test_stage1_never_consumes_an_inadmissible_existing_class``, whose
    checker forbids the Stage-1 rejection outright and therefore subsumes
    this checker there.

    **The shared format is MP3** (issue #829 Phase 5 PR2d). Same coverage
    argument the paragraphs above already make, applied to a fact the
    harness could not see until it started driving the real decider:
    ``spectral_gate_trigger`` fires the preimport gate ONLY for an MP3
    candidate, so outside that domain ``stage1_spectral`` is ``None`` and
    the disagreement this property hunts is unreachable by construction —
    not "safe", *absent*. The old inline harness computed a Stage-1 verdict
    regardless, which is how Vorbis worlds looked like coverage while
    production never ran Stage 1 on them at all: over 5,000 draws of the
    pre-PR2d strategy, 3,189 (63.8%) were worlds production gate-skipped at
    the time (2,347 uncalibrated-codec, 842 high-average VBR — the latter
    842 no longer skip anywhere, since #1145 removed that half).

    The second half of that narrowing is gone: PR2d also had to hold a VBR
    candidate's container below the preimport gate's average threshold,
    because a high-average VBR MP3 was gate-skipped and had no Stage-1
    verdict either. Issue #1145 removed the skip, so every MP3 container
    is in-domain now and the strategy draws the full range — a strictly
    wider world set, not a relaxed one.

    Vorbis is consequently no longer drawn HERE. Its ladder is still real
    and a same-Vorbis pair is still comparable at Stage 2 — that is
    patrolled by the parity property in ``TestGeneratedParity`` and the
    cross-codec property below; what a Vorbis candidate cannot do is reach
    Stage 1.
    """
    new_is_cbr = draw(st.booleans())
    # Every MP3 reaches Stage 1 since issue #1145, so the container is drawn
    # across the whole range whatever the declared mode.
    new_container = draw(_bitrates(min_value=1, max_value=3000))
    new_spectral = draw(_lame_buckets_at_or_below(new_container))
    existing_container = draw(_bitrates(min_value=1, max_value=3000))
    existing_spectral = draw(_lame_buckets_at_or_below(existing_container))
    shared_format = "MP3"
    return StageParityWorld(
        grade=draw(st.sampled_from(("suspect", "likely_transcode"))),
        new_container=new_container,
        new_spectral=new_spectral,
        existing_container=existing_container,
        existing_spectral=existing_spectral,
        existing_grade=draw(st.sampled_from(
            (None, "genuine", "marginal", "suspect", "likely_transcode"))),
        new_is_cbr=new_is_cbr,
        existing_is_cbr=draw(st.booleans()),
        new_format=shared_format,
        existing_format=shared_format,
        new_cliff_hz=draw(_cliffs_classing_at_or_below(CODEC_FAMILY_MP3, new_container)),
        existing_cliff_hz=draw(
            _cliffs_classing_at_or_below(CODEC_FAMILY_MP3, existing_container)),
    )


#: Every MP3 class a raw ``cliff_hz`` can actually derive, and the cliffs
#: that derive it — computed through ``ladder_class_kbps`` rather than
#: transcribed (``.claude/rules/test-fidelity.md`` Rule C).
_MP3_CLIFFS_BY_CLASS: dict[int, tuple[int, ...]] = {
    class_kbps: tuple(
        cliff_hz for cliff_hz in _CLIFF_HZ_VALUES
        if ladder_class_kbps(CODEC_FAMILY_MP3, cliff_hz) == class_kbps
    )
    for class_kbps in sorted(
        {
            derived for derived in (
                ladder_class_kbps(CODEC_FAMILY_MP3, cliff_hz)
                for cliff_hz in _CLIFF_HZ_VALUES
            )
            if derived is not None
        }
    )
}


@st.composite
def stage1_rejecting_worlds(draw) -> StageParityWorld:
    """Worlds that reach a Stage-1 spectral REJECT by construction.

    The audit-field properties (issue #829 Phase 5 PR2d) only have anything
    to check on the short-circuit path, and a Stage-1 reject is a small
    minority of ``stage_parity_worlds`` — filtering for it would spend the
    budget on discards. So the four conditions
    ``spectral_import_decision`` needs are established here instead:

    1. both album verdicts authorize a spectral finding (only ``suspect``
       and ``likely_transcode`` do, per ``SPECTRAL_TRANSCODE_GRADES``);
    2. both classes are derived the SAME way — either both from a raw
       ``cliff_hz`` or both from a legacy stored bucket — so
       ``spectral_classes_comparable`` admits the pair;
    3. the candidate's class is strictly lower than the HAVE's;
    4. the candidate is an MP3 whose preimport gate fires, so a Stage-1
       verdict exists at all.

    The provisional carve-out that can spare a Stage-1 reject
    (``provisional_source_candidate and has_provisional_probe_input``) is
    inactive by construction: these are native-lossy worlds and
    ``_stage_parity_decision`` passes neither ``supported_lossless_source``
    nor a candidate probe.
    """
    basis = draw(st.sampled_from(("stored_bucket", "cliff_hz")))
    new_is_cbr = draw(st.booleans())
    # No gate threshold since issue #1145: every MP3 is scanned, so the class
    # ceiling is the container range itself whatever the declared mode.
    class_ceiling = 3000
    if basis == "cliff_hz":
        classes = tuple(_MP3_CLIFFS_BY_CLASS)
    else:
        classes = _LAME_BUCKET_CLASSES
    lower = [c for c in classes if c < max(classes) and c <= class_ceiling]
    new_class = draw(st.sampled_from(lower))
    existing_class = draw(st.sampled_from([c for c in classes if c > new_class]))

    new_container = draw(_bitrates(min_value=new_class, max_value=class_ceiling))
    existing_container = draw(
        _bitrates(min_value=existing_class, max_value=3000))
    if basis == "cliff_hz":
        new_cliff = draw(st.sampled_from(_MP3_CLIFFS_BY_CLASS[new_class]))
        existing_cliff = draw(
            st.sampled_from(_MP3_CLIFFS_BY_CLASS[existing_class]))
        # The stored column is free: a cliffed row often carries a legacy
        # bucket too, and the cliff wins. Drawing it freely keeps the world
        # honest rather than quietly agreeing with the cliff.
        new_stored = draw(_lame_buckets_at_or_below(new_container))
        existing_stored = draw(_lame_buckets_at_or_below(existing_container))
    else:
        new_cliff = existing_cliff = None
        new_stored, existing_stored = new_class, existing_class
    return StageParityWorld(
        grade=draw(st.sampled_from(("suspect", "likely_transcode"))),
        new_container=new_container,
        new_spectral=new_stored,
        existing_container=existing_container,
        existing_spectral=existing_stored,
        existing_grade=draw(st.sampled_from(("suspect", "likely_transcode"))),
        new_is_cbr=new_is_cbr,
        existing_is_cbr=draw(st.booleans()),
        new_format="MP3",
        existing_format="MP3",
        new_cliff_hz=new_cliff,
        existing_cliff_hz=existing_cliff,
    )


@st.composite
def stage1_rejecting_flac_worlds(draw) -> StageParityWorld:
    """Stage-1-rejecting worlds that run the counterfactual through the two
    LOSSLESS-SOURCE Stage-2 branches (issue #829 Phase 5 PR2d review S1).

    Every world in ``stage1_rejecting_worlds`` is native-lossy, so the
    throwaway run only ever exercised ``full_pipeline_decision``'s third
    branch. These take the other two.

    ``target_format`` selects between them: a lossless target keeps the FLAC
    on disk, anything else takes the convert-then-decide branch. The
    candidate keeps an MP3 format label because a lossless container yields
    NO spectral class (``interpret_spectral_cliff``'s lossless branch never
    derives kbps), and without a class Stage 1 cannot reject at all — so an
    ``is_flac`` world with a lossless label is unreachable for this property
    by construction, not by choice.

    ``supported_lossless_source`` is drawn rather than left implicit, and
    the two values reach genuinely different code:

    * ``True``/``None`` (i.e. ``is_flac``) is the shape the EVIDENCE
      entrypoint produces — and there the counterfactual ALWAYS terminates
      in the provisional lane, because the Stage-1 carve-out
      (``provisional_source_candidate and has_provisional_probe_input``)
      spares every lossless-source candidate that has probe evidence, so the
      only ones that short-circuit are those with none. The deterministic
      twin ``TestStage2CounterfactualAudit`` pins that live shape through
      ``full_pipeline_decision_from_evidence``.
    * ``False`` with ``is_flac=True`` is simulator-only, and it is the ONLY
      way to drive the throwaway run past the provisional lane into
      ``measured_import_decision``, the ``TargetQualityContract``
      construction and the ``verified_proof`` rebind. It is included
      precisely because those are the lines review S1 found unguarded; the
      property it feeds is about the AUDIT MECHANISM, which is
      branch-agnostic, not about album plausibility. Some of these worlds
      make Stage 2 raise, which is the point — the sentinel path is part of
      the mechanism.
    """
    lossy = draw(stage1_rejecting_worlds())
    return replace(
        lossy,
        is_flac=True,
        target_format=draw(st.sampled_from((None, "flac", "lossless", "mp3 v0"))),
        supported_lossless_source=draw(st.sampled_from((None, True, False))),
    )


@st.composite
def inadmissible_spectral_pair_worlds(draw) -> InadmissibleSpectralPairWorld:
    """Worlds whose two spectral classes may NOT be compared (issue #829 PR2c).

    The domain ``stage_parity_worlds`` excludes and #828 item 1 asked for.
    Incomparability is a property of the WORLD, established by
    construction, so the property's precondition never routes through
    ``spectral_classes_comparable`` — a mutant inside that function would
    otherwise flip the precondition too and the property would go vacuous
    exactly when it was needed.

    ``shape`` is drawn first and weighted evenly across the three refusal
    reasons. A flat sweep over containers/grades/formats would spend ~92%
    of its budget on ``right_not_decision_grade`` (measured: 37,029 of
    40,181 incomparable worlds in a 46,286-world grid), starving the two
    shapes where BOTH sides carry a real class — which are the shapes the
    #829 calibration exists to separate.

    * ``cross_codec_legacy_bucket`` — MP3 candidate vs Vorbis HAVE, both
      classes derived from a legacy stored bucket. Live on prod: five rows
      resolve to a Vorbis measured subject through ``format`` (evidence ids
      33935/33941/33942/33943/33974), two carrying the documented LAME
      over-read of Vorbis as 192.
    * ``mixed_derivation_basis`` — same codec, but exactly one side carries
      a raw ``cliff_hz``. A cliff re-derivation sits systematically one tier
      above the legacy bucket, so the pair measures derivation, not content
      (the Fall 2007 loop, issue #911, evidence id 34219).
    * ``right_not_decision_grade`` — the HAVE has no admissible class at
      all: a family with no invertible ladder (AAC's content floor, Opus's
      absent signal, WMA's uncalibrated cliff), or an MP3 whose own album
      verdict does not authorize a spectral finding. Download 37946 is the
      first sub-case pointed at the library instead of the candidate.
    """
    shape = draw(st.sampled_from(_INADMISSIBLE_SHAPES))
    grade = draw(st.sampled_from(("suspect", "likely_transcode")))
    # The preimport gate must fire for Stage 1 to produce a verdict at all.
    # That is now true of every MP3 whatever its declared mode (issue #1145
    # retired the VBR average skip), so the container is drawn across the
    # whole range for both modes. The VBR arm used to be capped at 209 to
    # stay under the retired threshold, which left every high-average VBR
    # world unreachable for this property.
    new_is_cbr = draw(st.booleans())
    new_container = draw(_bitrates(min_value=1, max_value=3000))
    existing_container = draw(_bitrates(min_value=1, max_value=3000))
    existing_is_cbr = draw(st.booleans())

    # The candidate is decision-grade by construction: an authorizing
    # verdict plus either a raw cliff or a real ``LAME_LOWPASS`` bucket.
    # ``cross_codec_legacy_bucket`` additionally needs stored-bucket basis
    # on BOTH sides — a cliff on either side makes the pair refuse for the
    # mixed-basis reason instead, which is a different shape.
    new_from_cliff = draw(st.booleans()) if shape != (
        "cross_codec_legacy_bucket") else False
    new_cliff_hz = draw(st.sampled_from(_CLIFF_HZ_VALUES)) if new_from_cliff else None
    new_spectral = None if new_from_cliff else draw(
        st.sampled_from(_LAME_BUCKET_CLASSES))

    if shape == "cross_codec_legacy_bucket":
        return InadmissibleSpectralPairWorld(
            shape=shape, grade=grade,
            new_container=new_container, new_spectral=new_spectral,
            new_cliff_hz=None, new_is_cbr=new_is_cbr,
            existing_format="Vorbis",
            existing_grade=draw(st.sampled_from(
                ("suspect", "likely_transcode"))),
            existing_container=existing_container,
            existing_spectral=draw(st.sampled_from(_LAME_BUCKET_CLASSES)),
            existing_cliff_hz=None,
            existing_is_cbr=existing_is_cbr,
        )

    if shape == "mixed_derivation_basis":
        # Same family, opposite derivation: whichever side has no cliff
        # falls back to its stored bucket, and the two bases differ.
        existing_cliff_hz = (
            None if new_from_cliff else draw(st.sampled_from(_CLIFF_HZ_VALUES))
        )
        return InadmissibleSpectralPairWorld(
            shape=shape, grade=grade,
            new_container=new_container, new_spectral=new_spectral,
            new_cliff_hz=new_cliff_hz, new_is_cbr=new_is_cbr,
            existing_format="MP3",
            existing_grade=draw(st.sampled_from(
                ("suspect", "likely_transcode"))),
            existing_container=existing_container,
            existing_spectral=(
                None if existing_cliff_hz is not None
                else draw(st.sampled_from(_LAME_BUCKET_CLASSES))
            ),
            existing_cliff_hz=existing_cliff_hz,
            existing_is_cbr=existing_is_cbr,
        )

    # right_not_decision_grade: either an unladdered family (any verdict),
    # or an MP3 whose verdict does not authorize a spectral finding.
    have_is_unladdered = draw(st.booleans())
    if have_is_unladdered:
        existing_format = draw(st.sampled_from(_NON_LADDER_HAVE_FORMATS))
        existing_grade = draw(st.sampled_from(
            ("genuine", "marginal", "suspect", "likely_transcode")))
    else:
        existing_format = "MP3"
        existing_grade = draw(st.sampled_from(_NON_AUTHORIZING_GRADES))
    # The HAVE always carries at least one real number. A world where it
    # carries none is inadmissible too, but trivially so — there is nothing
    # for the pre-#829 seam to have consumed, and this domain exists to
    # patrol the worlds where there was.
    existing_stored, existing_cliff_hz = draw(st.one_of(
        st.tuples(st.sampled_from(_LAME_BUCKET_CLASSES), st.none()),
        st.tuples(st.none(), st.sampled_from(_CLIFF_HZ_VALUES)),
        st.tuples(st.sampled_from(_LAME_BUCKET_CLASSES),
                  st.sampled_from(_CLIFF_HZ_VALUES)),
    ))
    return InadmissibleSpectralPairWorld(
        shape=shape, grade=grade,
        new_container=new_container, new_spectral=new_spectral,
        new_cliff_hz=new_cliff_hz, new_is_cbr=new_is_cbr,
        existing_format=existing_format,
        existing_grade=existing_grade,
        existing_container=existing_container,
        existing_spectral=existing_stored,
        existing_cliff_hz=existing_cliff_hz,
        existing_is_cbr=existing_is_cbr,
    )


@st.composite
def have_representation_worlds(draw) -> HaveRepresentationWorld:
    """Worlds for the disarm identity (issue #829 Phase 5 PR2c item 6).

    The HAVE is always decision-grade: a ladder family, an authorizing
    album verdict, and a class from either derivation. The CANDIDATE is
    free — grade included — because the two mechanisms are selected by
    whether the candidate's class is comparable against the HAVE's, so a
    constrained candidate would explore only one arm of the identity.
    """
    existing_from_cliff = draw(st.booleans())
    candidate_from_cliff = draw(st.booleans())
    return HaveRepresentationWorld(
        candidate_grade=draw(st.sampled_from(_GRADES)),
        candidate_container=draw(_bitrates(min_value=1, max_value=3000)),
        candidate_spectral=(
            None if candidate_from_cliff
            else draw(st.one_of(
                st.none(), st.sampled_from(_LAME_BUCKET_CLASSES)))
        ),
        candidate_cliff_hz=(
            draw(st.sampled_from(_CLIFF_HZ_VALUES))
            if candidate_from_cliff else None
        ),
        candidate_is_cbr=draw(st.booleans()),
        existing_format=draw(st.sampled_from(_LADDER_FORMATS)),
        existing_grade=draw(st.sampled_from(("suspect", "likely_transcode"))),
        existing_container=draw(_bitrates(min_value=1, max_value=3000)),
        existing_spectral=(
            None if existing_from_cliff
            else draw(st.sampled_from(_LAME_BUCKET_CLASSES))
        ),
        existing_cliff_hz=(
            draw(st.sampled_from(_CLIFF_HZ_VALUES))
            if existing_from_cliff else None
        ),
    )


#: The live world that found the disarm window: download_log 29525, Clue to
#: Kalo *Lily Perdida*. The HAVE (evidence 17273) is an MP3 CBR 320 graded
#: ``likely_transcode`` whose measured ``cliff_hz=15500`` re-derives to the
#: 128 class; the candidate (evidence 22689) carries only a legacy stored
#: bucket, so the bases differ, the clamp withholds, and the one-sided
#: override is the only thing standing between a known-fake 320 and a
#: ``transparent`` rank. Deterministic twin:
#: ``tests/test_quality_classification.py::TestMixedBasisDisarmWindow``.
_LILY_PERDIDA_WORLD = HaveRepresentationWorld(
    candidate_grade="likely_transcode",
    candidate_container=234,
    candidate_spectral=192,
    candidate_cliff_hz=None,
    candidate_is_cbr=False,
    existing_format="MP3",
    existing_grade="likely_transcode",
    existing_container=320,
    existing_spectral=None,
    existing_cliff_hz=15500,
)


#: A world that reaches a Stage-1 spectral reject, so the two audit-field
#: properties are never vacuous at their pinned example. Comparable classes
#: (same codec, both legacy stored buckets, both grades authorizing), the
#: candidate's strictly lower — the only shape ``spectral_import_decision``
#: rejects on. Deterministic pin twin:
#: ``tests/test_quality_classification.py::TestStage2CounterfactualAudit``.
_STAGE1_REJECT_COUNTERFACTUAL_WORLD = StageParityWorld(
    grade="likely_transcode",
    new_container=256, new_spectral=128,
    existing_container=256, existing_spectral=192,
    existing_grade="likely_transcode",
    new_is_cbr=True, existing_is_cbr=True,
    new_format="MP3", existing_format="MP3",
)


#: The shrunk/live-shaped worlds pinned as ``@example``s on the property AND
#: re-used by the domain pin, so the two can never describe different worlds.
_PINNED_INADMISSIBLE_WORLDS: tuple[InadmissibleSpectralPairWorld, ...] = (
    # Live shape. An MP3 CBR 256 candidate whose own cliff-free stored
    # bucket says 128, against a Vorbis HAVE carrying the documented LAME
    # over-read of q4 as 192 (evidence 33942/33943 shape). The pre-#829 seam
    # weighed 128 against 192 and REJECTED the MP3 on a number produced by
    # applying LAME's table to a Vorbis stream.
    InadmissibleSpectralPairWorld(
        shape="cross_codec_legacy_bucket", grade="likely_transcode",
        new_container=256, new_spectral=128, new_cliff_hz=None,
        new_is_cbr=True,
        existing_format="Vorbis", existing_grade="likely_transcode",
        existing_container=128, existing_spectral=192,
        existing_cliff_hz=None, existing_is_cbr=True,
    ),
    # Fall 2007 shape (issue #911, evidence 34219): the HAVE carries a raw
    # cliff at 16.5 kHz that re-derives to the 160 class, one tier above the
    # candidate's legacy 128 bucket purely because of how each was derived.
    InadmissibleSpectralPairWorld(
        shape="mixed_derivation_basis", grade="suspect",
        new_container=320, new_spectral=128, new_cliff_hz=None,
        new_is_cbr=True,
        existing_format="MP3", existing_grade="likely_transcode",
        existing_container=160, existing_spectral=None,
        existing_cliff_hz=16500, existing_is_cbr=True,
    ),
    # Download 37946's defect pointed at the LIBRARY instead of the
    # candidate. The installed AAC's 15.5 kHz cliff is native behaviour at
    # every rate the calibration measured from 96 to 320, and its
    # LAME-bucketed 192 is not a class in any codec's terms — yet the
    # pre-#829 seam let that number out-rank an MP3 candidate's real 128
    # and reject a genuine upgrade over a 112 kbps AAC.
    InadmissibleSpectralPairWorld(
        shape="right_not_decision_grade", grade="likely_transcode",
        new_container=256, new_spectral=128, new_cliff_hz=None,
        new_is_cbr=True,
        existing_format="AAC", existing_grade="likely_transcode",
        existing_container=112, existing_spectral=192,
        existing_cliff_hz=15500, existing_is_cbr=True,
    ),
    # The same refusal from the other direction: a same-codec MP3 HAVE whose
    # album verdict is ``genuine``, so no spectral finding is authorized from
    # its stored 320. The pre-#829 seam consumed that number regardless of
    # the grade and rejected a 128-class candidate on it.
    InadmissibleSpectralPairWorld(
        shape="right_not_decision_grade", grade="likely_transcode",
        new_container=192, new_spectral=128, new_cliff_hz=None,
        new_is_cbr=True,
        existing_format="MP3", existing_grade="genuine",
        existing_container=320, existing_spectral=320,
        existing_cliff_hz=None, existing_is_cbr=True,
    ),
)


_FRESH_ALBUM = AlbumState(
    "generated_fresh_request", None, False, None, None, False, None)


#: (min_bitrate, avg_bitrate, is_cbr, existing_format) — the two ways an
#: installed MP3 reaches TRANSPARENT. Since issue #1145 the V0 shape gets
#: there through its proven ``mp3 v0`` contract, not through a second band
#: table: a bare measured 245 is GOOD, and the label is what production now
#: mints for it from the LAME header. Both shapes are what
#: ``album_info_from_current`` really produces for those albums.
_TRANSPARENT_EXISTING_SHAPES = (
    (320, 320, True, "MP3"),
    (245, 245, False, "mp3 v0"),
)


@st.composite
def transparent_mp3_albums(draw) -> AlbumState:
    min_br, avg_br, is_cbr, existing_format = draw(
        st.sampled_from(_TRANSPARENT_EXISTING_SHAPES))
    return AlbumState(
        name="generated_transparent_mp3",
        min_bitrate=min_br,
        is_cbr=is_cbr,
        spectral_grade="genuine",
        spectral_bitrate=None,
        verified_lossless=False,
        search_filetype_override=None,
        existing_format=existing_format,
        avg_bitrate=avg_br,
    )


class TestGeneratedSimulatorInvariants(unittest.TestCase):
    """Policy invariants over generated simulator worlds."""

    @given(album=album_states(), download=download_scenarios())
    def test_generated_decisions_are_definitive(self, album, download):
        result = simulate(album, download)
        assert_decision_is_definitive(result)

    @given(album=album_states(), download=download_scenarios())
    def test_generated_denylist_always_has_valid_cause(self, album, download):
        """Issue #813 Finding 2, generated half of the PAIR: over randomly
        generated (album, download) worlds driven through the real decider
        (``full_pipeline_decision`` via ``simulate()``), a denylisted
        outcome must always trace to a real reject/retained-nonterminal
        decision. Deterministic pin:
        ``TestLiveBugReproductions.test_tyler_lamberts_grave_cbr320_transcode_accepted``
        (``tests/test_quality_classification.py``) and
        ``TestSimulatorInvariants.test_denylist_requires_cause`` (fixture
        matrix, same module as this checker)."""
        result = simulate(album, download)
        assert_denylist_has_valid_cause(result)

    @given(album=raw_verified_lossless_albums(), download=lossy_downloads())
    def test_raw_verified_lossless_never_imports_lossy_candidate(
            self, album, download):
        result = simulate(album, download)
        assert_lossy_not_imported_over_verified_lossless(result)

    @given(album=raw_verified_lossless_albums(), download=download_scenarios())
    def test_current_proof_blocks_every_automatic_candidate(
            self, album, download):
        result = simulate(
            album,
            download,
            current_verified_lossless_proof=True,
        )
        assert_verified_lossless_proof_locks_candidate(result)

    @given(album=transparent_mp3_albums(),
           download=obvious_lower_rank_lossy_downloads())
    def test_transparent_existing_never_accepts_obvious_downgrade(
            self, album, download):
        result = simulate(album, download)
        assert_obvious_downgrade_not_accepted(result)

    @given(download=lossy_downloads())
    def test_unverified_lossy_first_copy_never_accepts_at_any_bitrate(
            self, download):
        result = simulate(_FRESH_ALBUM, download)
        assert_unverified_lossy_never_terminal(result)

    def test_real_quality_gate_matches_post_import_action_table(self):
        for decision in _POST_IMPORT_EXPECTATIONS:
            with self.subTest(decision=decision):
                measurement = {
                    "accept": AudioQualityMeasurement(
                        format="opus 64", min_bitrate_kbps=64,
                        avg_bitrate_kbps=64,
                    ),
                    "requeue_lossless": AudioQualityMeasurement(
                        format="MP3", min_bitrate_kbps=320,
                        avg_bitrate_kbps=320, is_cbr=True,
                        spectral_grade="genuine",
                        spectral_subject=EVIDENCE_SUBJECT_INSTALLED,
                        spectral_provenance="measured",
                    ),
                    "requeue_upgrade": AudioQualityMeasurement(
                        format="MP3", min_bitrate_kbps=320,
                        avg_bitrate_kbps=320, is_cbr=True,
                        spectral_grade="suspect", spectral_bitrate_kbps=192,
                        spectral_subject=EVIDENCE_SUBJECT_INSTALLED,
                        spectral_provenance="measured",
                    ),
                }[decision]
                self.assertEqual(
                    quality_gate_decision(
                        measurement,
                        verified_lossless_proof=decision == "accept",
                    ),
                    decision,
                )
                state = QualityGateState(
                    measurement=measurement,
                    verified_lossless_proof=decision == "accept",
                )
                plan = _check_quality_gate_core(
                    mb_id="generated-mbid", label="Generated",
                    request_id=42,
                    files=[SimpleNamespace(username="peer")],
                    db=SimpleNamespace(
                        get_request=lambda _request_id: {
                            "search_filetype_override": None,
                        },
                    ),  # type: ignore[arg-type]
                    apply=False,
                    state_loader=_fixed_gate_state(state),
                )
                self.assertIsNotNone(plan)
                assert plan is not None
                raw_override = assert_search_override_is_a_string(
                    plan.transition.fields.get("search_filetype_override"),
                )
                assert_post_import_action_matches(
                    decision=decision,
                    status=plan.transition.target_status,
                    search_filetype_override=raw_override,
                    denylist=bool(plan.denylists),
                )

    @given(
        verified_lossless_proof=st.booleans(),
        min_bitrate_kbps=_bitrates(),
        avg_bitrate_kbps=_bitrates(),
        format_name=st.sampled_from(_CURRENT_FORMATS),
        is_cbr=st.booleans(),
        error_type=st.sampled_from((RuntimeError, ValueError, LookupError)),
        error_message=st.text(min_size=0, max_size=80),
    )
    def test_quality_decision_errors_always_reopen_full_tiers(
        self,
        verified_lossless_proof,
        min_bitrate_kbps,
        avg_bitrate_kbps,
        format_name,
        is_cbr,
        error_type,
        error_message,
    ):
        state = QualityGateState(
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate_kbps,
                avg_bitrate_kbps=avg_bitrate_kbps,
                format=format_name,
                is_cbr=is_cbr,
            ),
            verified_lossless_proof=verified_lossless_proof,
        )

        def raise_decision(
            current: AudioQualityMeasurement,
            cfg: QualityRankConfig | None = None,
            *,
            target_contract: TargetQualityContract | None = None,
            verified_lossless_proof: bool = False,
        ) -> Never:
            del current, cfg, target_contract, verified_lossless_proof
            raise error_type(error_message)

        with self.assertLogs("cratedigger", level="ERROR") as captured:
            plan = _check_quality_gate_core(
                mb_id="generated-mbid",
                label="Generated Decision Failure",
                request_id=42,
                files=[SimpleNamespace(username="peer")],
                db=SimpleNamespace(),  # type: ignore[arg-type]
                apply=False,
                state_loader=lambda **_kwargs: state,
                quality_decision_fn=raise_decision,
            )
        self.assertIn("reopening full-tier search", "\n".join(captured.output))
        assert_quality_decision_failure_reopens_full_tier(plan)

    def test_lossless_narrowing_is_subject_blind_for_genuine_transparent(self):
        # Decision 17: the transparent+genuine narrowing rule keys on the
        # grade, never the subject label — an unconverted import's
        # source-subject grade describes the installed bytes.
        subjects = (
            EVIDENCE_SUBJECT_SOURCE,
            EVIDENCE_SUBJECT_INSTALLED,
        )
        for subject in subjects:
            with self.subTest(subject=subject):
                measurement = AudioQualityMeasurement(
                    format="MP3",
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    is_cbr=True,
                    spectral_grade="genuine",
                    spectral_subject=subject,
                    spectral_provenance=(
                        "measured"
                        if subject == EVIDENCE_SUBJECT_INSTALLED
                        else "carried"
                    ),
                )
                self.assertEqual(
                    quality_gate_decision(measurement), "requeue_lossless")

    @given(
        target_format=st.sampled_from(_TARGET_FORMATS),
        spectral_grade=st.sampled_from(
            (None, "error", "genuine", "marginal", "suspect", "likely_transcode")
        ),
        converted_count=st.integers(min_value=0, max_value=24),
        is_transcode=st.booleans(),
        probe_kind=st.one_of(
            st.none(),
            st.sampled_from(("lossless_source_v0", "native_lossy_research_v0")),
        ),
        v0_avg=_optional_bitrates(max_value=400),
        v0_min=_optional_bitrates(max_value=400),
    )
    def test_verified_lossless_requires_affirmative_spectral_evidence(
        self,
        target_format,
        spectral_grade,
        converted_count,
        is_transcode,
        probe_kind,
        v0_avg,
        v0_min,
    ):
        from lib.quality import V0ProbeEvidence

        probe = (
            V0ProbeEvidence(
                kind=probe_kind,
                avg_bitrate_kbps=v0_avg,
                min_bitrate_kbps=v0_min,
            )
            if probe_kind is not None else None
        )
        result = determine_verified_lossless(
            target_format,
            spectral_grade,
            converted_count,
            is_transcode,
            v0_probe=probe,
        )
        assert_verified_lossless_has_affirmative_evidence(
            result,
            spectral_grade=spectral_grade,
            v0_probe_kind=probe_kind,
            v0_avg=v0_avg,
            v0_min=v0_min,
        )

    @given(
        grade=st.sampled_from(("suspect", "likely_transcode")),
        existing_spectral=_bitrates(min_value=2, max_value=3000),
        delta=st.integers(min_value=-600, max_value=600),
    )
    @example(grade="suspect", existing_spectral=128, delta=0)  # Mark DeNardo tie
    @example(grade="likely_transcode", existing_spectral=160, delta=0)
    @example(grade="suspect", existing_spectral=128, delta=-32)  # strictly worse
    def test_only_strictly_lower_spectral_rejects_at_stage1(
        self, grade, existing_spectral, delta,
    ):
        """PAIR (generated half) with the Mark DeNardo pin in
        ``tests/test_quality_decisions.py::TestSpectralImportDecision`` and
        ``tests/test_quality_classification.py``: an equal spectral floor
        defers to Stage 2, only a strictly-lower estimate rejects at Stage 1.
        ``delta=0`` gives the tie real fuzz coverage, not just the pins."""
        new_spectral = max(1, existing_spectral + delta)
        decision = spectral_import_decision(
            grade, new_spectral, existing_spectral,
        )
        assert_only_strictly_lower_spectral_rejects(
            decision,
            grade=grade,
            new_spectral=new_spectral,
            existing_spectral=existing_spectral,
        )

    @given(
        candidate_container=_bitrates(min_value=64, max_value=320),
        existing_container=_bitrates(min_value=64, max_value=320),
        candidate_spectral=st.sampled_from(_LAME_BUCKET_CLASSES),
        existing_spectral=st.sampled_from(_LAME_BUCKET_CLASSES),
        grade=st.sampled_from(("suspect", "likely_transcode")),
        existing_grade=st.sampled_from(("suspect", "likely_transcode")),
    )
    @example(  # Deerhunter dl 37725: identical 256/spectral-192 transcode.
        candidate_container=256, existing_container=256,
        candidate_spectral=192, existing_spectral=192,
        grade="likely_transcode", existing_grade="likely_transcode",
    )
    # The Deerhunter pin moves the DECISION, so it proves the first clause and
    # short-circuits before the second. This world moves only the VERDICT
    # ('equivalent' with the override, 'worse' without) — both map to
    # ``downgrade``, so ``stage2_import`` holds and the verdict clause is the
    # only one that can fire. 356 such worlds exist in this strategy's domain
    # but the derandomized budget draws the bucket twice, and any edit to this
    # property body reshuffles the sequence, so it is pinned rather than left
    # to the draw (#1094 round-2 review).
    @example(
        candidate_container=64, existing_container=128,
        candidate_spectral=96, existing_spectral=96,
        grade="suspect", existing_grade="suspect",
    )
    def test_existing_spectral_override_is_noop_when_candidate_has_spectral(
        self, candidate_container, existing_container,
        candidate_spectral, existing_spectral, grade, existing_grade,
    ):
        """PAIR (generated half) with the Deerhunter pin in
        ``tests/test_quality_classification.py`` (issue #813 Finding 1).

        When both sides carry a spectral estimate, the existing-side
        spectral-floor override must not change the decision — the shared
        clamp already floors both symmetrically. The Deerhunter ``@example``
        makes the override decisive (with it -> phantom "better"; gated off ->
        "equivalent"), so a mutant reverting the gate dies here.
        """
        # A spectral floor above the container is not what this property is
        # about; raise the containers instead of clamping the classes, which
        # would produce values no cliff estimator can emit (issue #829 Phase
        # 5 PR2b: an off-ladder number carries no class at all, so a clamped
        # world would stop exercising the clamp this property names).
        candidate_container = max(candidate_container, candidate_spectral)
        existing_container = max(existing_container, existing_spectral)
        # The floor override_bitrate_from_current_evidence would derive.
        override = min(existing_container, existing_spectral)

        def decide(override_min):
            return full_pipeline_decision(
                is_flac=False,
                min_bitrate=candidate_container,
                is_cbr=True,
                avg_bitrate=candidate_container,
                new_format="MP3",
                spectral_grade=grade,
                spectral_bitrate=candidate_spectral,
                existing_min_bitrate=existing_container,
                existing_avg_bitrate=existing_container,
                existing_format="MP3",
                existing_is_cbr=True,
                existing_spectral_grade=existing_grade,
                existing_spectral_bitrate=existing_spectral,
                override_min_bitrate=override_min,
            )

        assert_existing_override_noop_under_shared_clamp(
            decide(override), decide(None),
        )

    @given(world=stage_parity_worlds())
    @example(  # Mark DeNardo request 1308: Stage 1 defers, Stage 2 scores.
        world=StageParityWorld(
            grade="suspect", new_container=192, new_spectral=128,
            existing_container=128, existing_spectral=128,
            existing_grade="likely_transcode",
            new_is_cbr=True, existing_is_cbr=True,
            new_format="MP3", existing_format="MP3",
        )
    )
    @example(  # Deerhunter dl 37725: identical transcode, Stage 1 defers.
        world=StageParityWorld(
            grade="likely_transcode", new_container=256, new_spectral=192,
            existing_container=256, existing_spectral=192,
            existing_grade="likely_transcode",
            new_is_cbr=True, existing_is_cbr=True,
            new_format="MP3", existing_format="MP3",
        )
    )
    @example(  # Shrunk regression: coarse rank band buckets UNEQUAL
        # spectral (96 vs 112) into the same tier; before the
        # spectral_tiebreak fix the fully-unclamped raw metric (1000 vs
        # 235) would launder the worse-spectral candidate in as "better".
        # Stage 1 rejects and Stage 2 says "worse" — the fix holding.
        #
        # RE-DERIVED in issue #829 Phase 5 PR2d. The originally shrunk
        # world used 200/230, and neither is a ``LAME_LOWPASS`` member, so
        # under PR2b's stored-bucket allowlist neither side carries a class
        # at all: the clamp this pin exists for never fired once the
        # harness started driving the real decider instead of feeding
        # ``compare_quality`` raw stored numbers. 96/112 are real buckets
        # that reproduce the same decisive shape (unequal classes bucketed
        # into one rank, raw metric strongly favouring the worse side).
        world=StageParityWorld(
            grade="likely_transcode", new_container=1000, new_spectral=96,
            existing_container=235, existing_spectral=112,
            existing_grade="likely_transcode",
            new_is_cbr=True, existing_is_cbr=True,
            new_format="MP3", existing_format="MP3",
        )
    )
    @example(  # Fault-injection pin (issue #829 Phase 5 PR2d mutation run).
        # A MIXED-derivation pair inside one codec family: the candidate's
        # class comes from a legacy stored bucket (112) and the HAVE's from
        # a raw cliff (18000 Hz -> the 192 class), so
        # ``spectral_classes_comparable`` refuses the pairing and Stage 1
        # must see NO existing class. It defers (``import_no_exist``) and
        # Stage 2 imports on the raw metric (760 vs 598).
        #
        # Revert the comparability gate — ``stage1_existing_class =
        # existing_spectral_class`` — and Stage 1 rejects on 112 < 192
        # while Stage 2 still scores the candidate ``better``, because the
        # clamp's own comparability check is unchanged and refuses. That is
        # the ONE shape where a Stage-1-ONLY defect can diverge from Stage 2
        # at all. Measured over 5,000 draws of the strategy above: the
        # shipped code produces 0 contradictions while reaching 236 Stage-1
        # rejects (so the property is not vacuous), and the gate revert
        # produces 5 — 1 in 1,000, i.e. roughly a 14% chance of detection
        # over 150 derandomized examples. Hence the pin: without it the gate
        # revert survives the suite tier.
        world=StageParityWorld(
            grade="likely_transcode", new_container=760, new_spectral=112,
            existing_container=598, existing_spectral=160,
            existing_grade="likely_transcode",
            new_is_cbr=True, existing_is_cbr=False,
            new_format="MP3", existing_format="MP3",
            new_cliff_hz=None, existing_cliff_hz=18000,
        )
    )
    # The CBR/VBR band-table regression (shrunk regression #2) used to be
    # pinned here as a third ``@example`` and was REMOVED in issue #829
    # Phase 5 PR2d, because it could no longer fire. Its shrunk world used
    # spectral 245/300 — neither a ``LAME_LOWPASS`` member, so under PR2b's
    # stored-bucket allowlist neither side carries a class and the clamp it
    # exists for never fires; it only ever "worked" because the old harness
    # fed ``compare_quality`` raw stored numbers. The producible values
    # (256/320) do fire the clamp, but the CONTRADICTION shape cannot
    # follow: a VBR MP3 is gate-skipped at or above
    # ``cfg.mp3_vbr_spectral_gate_kbps``
    # and below it no container can exceed a high enough class for the
    # candidate to be spectral-bound, so Stage 1 has no verdict at all — the
    # checker returns immediately. An exhaustive sweep of the producible
    # bucket ladder with the CBR-forcing reverted found ZERO worlds where
    # Stage 1 rejects and Stage 2 says "better".
    #
    # The regression guard for that class is therefore the deterministic
    # twin, ``tests/test_quality_comparison_basis.py``'s "CBR bands classify
    # a spectral-bound value even when that side is VBR", which pins the
    # same 256/320 pair at the ``compare_quality`` boundary where the world
    # IS constructible. An ``@example`` that provably cannot fire is a
    # readability hazard, so the pointer stays and the pin does not.
    def test_stage1_never_contradicts_stage2(self, world):
        """PAIR (generated half) — issue #813 Finding 1's core deliverable:
        for every world, Stage 1's verdict never contradicts Stage 2's.

        Drives ``full_pipeline_decision`` — the real seam owner — ONCE per
        world and reads both stages off its result (issue #829 Phase 5
        PR2d). Before PR2d the harness reproduced the decider's Stage-1
        wiring inline, because Stage 2's verdict is needed in exactly the
        worlds where Stage 1 short-circuits; the decider now reports that
        counterfactual itself, so the copy is gone and a mutant planted in
        the wiring moves this property.

        This property found and pins TWO independent Stage2 gaps, both
        inside the shared spectral clamp:

        1. Same-rank tiebreak (``spectral_tiebreak`` branch, first
           ``@example``): the coarse rank band can bucket two genuinely
           UNEQUAL spectral estimates together, and the fully-unclamped raw
           metric tiebreak used to decide instead — reversing a real
           spectral ordering. Deterministic pin twin:
           ``tests/test_quality_comparison_basis.py``'s "differing clamped
           values decide the same-rank tiebreak directly" case.
        2. CBR/VBR band-table mismatch (second ``@example``): the
           spectral-bucket values are calibrated to the CBR band
           thresholds (``LAME_LOWPASS`` — see ``_shared_spectral_bitrates``'s
           docstring), not the more generous VBR ones. Classifying a
           spectral-bound clamped value through a VBR-tagged side's own
           ``is_cbr=False`` let a worse-spectral VBR-tagged candidate
           outrank a better-spectral CBR-tagged existing purely from table
           choice. Random probing (millions of iterations, not reproduced
           by Hypothesis at either the suite or fuzz tier within a
           reasonable budget — this class needed the deterministic pin to
           be caught reliably) found this at roughly a 1-in-8000 rate over
           the general world space. **That class left this property's
           reach in PR2d**: the shape needs a spectral-bound VBR MP3
           candidate, which the preimport gate now provably cannot admit —
           see that ``@example``'s comment for the sweep and for the
           deterministic twin that still guards it.

        PR #827 review round: both fixes above shipped requiring only ONE
        side spectral-bound to fire, which introduced two NEW flip worlds
        (findings F1/F2, pinned in
        ``tests/test_quality_classification.py::TestLiveBugReproductions``'s
        ``test_stage_parity_review_f1_*``/``test_stage_parity_review_f2_*``).
        Both fixes now require BOTH sides bound before firing — see
        ``both_spectral_bound`` in ``lib/quality/compare.py``.
        """
        stage1, stage2 = _stage_parity_verdicts(world)
        assert_stage1_never_contradicts_stage2(stage1, stage2)

    @given(world=st.one_of(
        stage_parity_worlds(),
        stage1_rejecting_worlds(),
        stage1_rejecting_flac_worlds(),
    ))
    @example(world=_STAGE1_REJECT_COUNTERFACTUAL_WORLD)
    def test_the_counterfactual_is_reported_exactly_when_stage1_short_circuits(
        self, world,
    ):
        """PAIR (generated half) — issue #829 Phase 5 PR2d: the audit keys
        are on every result, and say something exactly where they mean
        something.

        A counterfactual reported next to a REAL Stage-2 decision would be a
        second, contradictory answer to the same question; a missing key
        would break the documented dict shape both twins share; and a
        short-circuit that reports nothing at all is indistinguishable from
        "Stage 1 never fired", which is a different fact (review S2).

        Drawn from all three strategies so the clause is checked on
        deferring worlds, on short-circuiting native-lossy worlds and on
        short-circuiting lossless-source worlds — the last of which includes
        the ones whose Stage 2 cannot be evaluated at all.
        """
        assert_counterfactual_reported_exactly_when_stage1_short_circuits(
            _stage_parity_decision(world), context=repr(world))

    @given(world=st.one_of(
        stage1_rejecting_worlds(), stage1_rejecting_flac_worlds()))
    @example(world=_STAGE1_REJECT_COUNTERFACTUAL_WORLD)
    def test_the_stage1_reject_decision_is_unchanged_by_its_audit(self, world):
        """PAIR (generated half) — issue #829 Phase 5 PR2d: reporting the
        Stage-2 counterfactual changes no decision.

        Running Stage 2 on a path that previously returned before it is the
        entire risk of this change. The counterfactual decides on a
        throwaway result dict and exactly two ``*_if_stage1_deferred`` keys
        are lifted back, so every decision field on a Stage-1 short-circuit
        must still hold what the reject branch alone writes. Deterministic
        pin twin: ``tests/test_quality_classification.py``'s
        ``TestStage2CounterfactualAudit``.

        Both world types are drawn (review S1): inertness is a property of
        the audit MECHANISM, not of one Stage-2 branch, and until PR2d's
        review nothing drove the throwaway run through either lossless-source
        branch — including the ``verified_proof`` rebind, the provisional
        lane, the ``TargetQualityContract`` construction and the paths that
        make Stage 2 raise.
        """
        decision = _stage_parity_decision(world)
        # The strategies establish the reject by construction, so a
        # regression in one must be loud rather than silently vacuous.
        assert decision["stage1_spectral"] == "reject", repr(world)
        assert_stage1_reject_leaks_no_stage2_state(decision, context=repr(world))

    @given(world=stage1_rejecting_worlds())
    @example(world=_STAGE1_REJECT_COUNTERFACTUAL_WORLD)
    def test_the_reported_counterfactual_is_what_stage_2_decides(self, world):
        """PAIR (generated half) — issue #829 Phase 5 PR2d: the reported
        counterfactual is TRUE, not a plausible fabrication.

        The audit answers "what would Stage 2 have said?", so it must equal
        what Stage 2 does say for the same world once the short-circuit is
        lifted — both the decision and the whole comparison basis. See
        ``_stage_parity_deferred_decision`` for the lever, and the property
        below for the check that the lever itself moves nothing.

        Native-lossy worlds ONLY, deliberately: the lever is provably
        Stage-2-inert only on that branch (on the lossless-source branches
        both of its kwargs feed ``provisional_lossless_decision``), so
        widening this property would swap a checked reference for an
        unchecked one. The lossless-source branches get the inertness half
        above plus the deterministic evidence-path pin.
        """
        decision = _stage_parity_decision(world)
        assert decision["stage1_spectral"] == "reject", repr(world)
        assert_counterfactual_is_the_deferred_stage2(
            decision,
            _stage_parity_deferred_decision(world),
            context=repr(world),
        )

    @given(world=stage_parity_worlds())
    def test_the_stage1_carve_out_lever_does_not_move_stage_2(self, world):
        """Qualifies the counterfactual reference above (issue #829 Phase 5
        PR2d).

        ``_stage_parity_deferred_decision`` claims that engaging
        production's Stage-1 carve-out leaves every Stage-2 input untouched.
        That claim is what makes the truthfulness property meaningful, so it
        is checked rather than asserted in prose: over worlds where Stage 1
        does not short-circuit anyway, the levered and unlevered runs must
        reach the same Stage-2 decision and the same basis.
        """
        decision = _stage_parity_decision(world)
        assume(decision["stage1_spectral"] != "reject")
        assert_carve_out_lever_is_stage2_inert(
            decision,
            _stage_parity_deferred_decision(world),
            context=repr(world),
        )

    @given(world=inadmissible_spectral_pair_worlds())
    @example(world=_PINNED_INADMISSIBLE_WORLDS[0])
    @example(world=_PINNED_INADMISSIBLE_WORLDS[1])
    @example(world=_PINNED_INADMISSIBLE_WORLDS[2])
    @example(world=_PINNED_INADMISSIBLE_WORLDS[3])
    def test_stage1_never_consumes_an_inadmissible_existing_class(self, world):
        """PAIR (generated half) — issue #829 Phase 5 PR2c: the cross-codec
        domain the #813/#827 parity property excluded, patrolled with the
        semantics the four-arm calibration defined.

        **This closes the CROSS-CODEC half of #828 item 1, not the item.**
        That item names two deliberately-unpatrolled classes. The other —
        unbound / self-inconsistent evidence, where a side's raw container
        measures LOWER than its own spectral estimate — is untouched here
        and remains recorded-only; ``docs/quality-verification.md`` §
        "Stage 1 / Stage 2 parity" states why it is a different subsystem's
        aggregation question.

        Stage 1 must not reject on a spectral comparison Stage 2 is not
        permitted to make, and an inadmissible existing-side class must
        move nothing at Stage 1. Drives the REAL seam owner
        (``full_pipeline_decision``, which
        ``full_pipeline_decision_from_evidence`` delegates to) twice over
        the same world — once as generated, once with the installed copy's
        spectral evidence removed entirely — and requires the two Stage-1
        verdicts to agree.

        Deterministic pin twins:
        ``tests/test_quality_classification.py::TestLiveBugReproductions``'s
        ``test_stage_parity_cross_codec_*`` (and its ``_via_evidence``
        mirror), which assert the DECIDED outcome the pre-#829 seam
        destroyed.
        """
        assert_stage1_ignores_inadmissible_existing_spectral(
            _inadmissible_pair_stage1(
                world, withhold_existing_spectral=False),
            _inadmissible_pair_stage1(
                world, withhold_existing_spectral=True),
            context=repr(world),
        )

    @given(world=have_representation_worlds())
    @example(world=_LILY_PERDIDA_WORLD)
    def test_the_installed_copy_is_never_ranked_above_its_own_class(
        self, world,
    ):
        """PAIR (generated half) — issue #829 Phase 5 PR2c item 6: the
        override-disarm predicate, which shipped in PR2b with deterministic
        pins and no property.

        ``spectral_classes_govern`` has two consumers. The clamp's own
        firing condition was already patrolled; the symmetric-representation
        disarm at the other consumer was not, so a mutant confined to it was
        caught by nothing generated. This is the "never NEITHER" half of
        that identity — its "never BOTH" twin is
        ``test_existing_spectral_override_is_noop_when_candidate_has_spectral``.

        Deterministic pin twin:
        ``tests/test_quality_classification.py::TestMixedBasisDisarmWindow``
        (download_log 29525, pinned here as the ``@example``).
        """
        decision, class_kbps = _have_representation_decision(world)
        basis = decision["comparison_basis"]
        # A Stage-1 short-circuit or a pre-comparison exit means no
        # comparison ran, so there is no representation to check. ``assume``
        # rather than ``return``: a return spends the example as a PASS and
        # silently shrinks the budget (docs/generated-testing.md).
        assume(isinstance(basis, dict))
        assert isinstance(basis, dict)
        existing_rank = basis["existing_rank"]
        assert isinstance(existing_rank, str)
        assert_have_is_represented_by_its_own_class(
            existing_rank,
            quality_rank(
                world.existing_format, class_kbps,
                QualityRankConfig.defaults(),
            ).name.lower(),
            context=repr(world),
        )

    @given(
        codec_label=_unmapped_codec_labels(),
        bitrate=_bitrates(max_value=4000),
        is_cbr=st.booleans(),
        spectral_grade=st.sampled_from((None, "genuine", "marginal")),
    )
    def test_unmapped_codec_first_copy_never_claims_a_ceiling(
        self,
        codec_label,
        bitrate,
        is_cbr,
        spectral_grade,
    ):
        result = simulate(
            _FRESH_ALBUM,
            DownloadScenario(
                name="generated_unmapped_codec",
                is_flac=False,
                min_bitrate=bitrate,
                is_cbr=is_cbr,
                is_vbr=not is_cbr,
                avg_bitrate=bitrate,
                spectral_grade=spectral_grade,
                new_format=codec_label,
            ),
        )
        assert_unmapped_first_copy_stays_searchable(result)

    @given(album=album_states(), download=download_scenarios())
    def test_generated_basis_never_contradicts_decision(self, album, download):
        result = simulate(album, download)
        assert_basis_consistent(result)

    @given(album=album_states(), download=download_scenarios())
    def test_generated_basis_metrics_are_truthful(self, album, download):
        result = simulate(album, download)
        assert_basis_metrics_truthful(album, download, result)

    @given(album=transparent_mp3_albums(), download=download_scenarios())
    def test_measured_decisions_with_existing_carry_basis(
            self, album, download):
        result = simulate(album, download)
        assert_measured_decision_carries_basis(result)
        assert_basis_consistent(result)


# ===========================================================================
# Parity property — the twins must agree on every world both can express.
#
# The world space here is the twins' COMMON language, i.e. exactly what the
# shared parity builders (tests/evidence_helpers.py) can encode:
#   * candidate V0 probes only on FLAC candidates (a lossy candidate with a
#     lossless-source V0 metric is not expressible in the flat kwargs);
#   * ``is_vbr`` is always derived as ``not is_cbr`` (the evidence decider
#     never receives an explicit is_vbr);
#   * raw FLAC worlds have target flac/lossless, converted FLAC worlds have
#     a lossy/None target (a "converted" candidate with a keep-FLAC target
#     is a contradictory world description);
#   * conversion facts are passed explicitly on both sides.
# Divergence inside this space is a real parity-contract violation.
# ===========================================================================

@dataclass(frozen=True)
class ParityWorld:
    """One album-vs-candidate world expressed in the twins' common language."""
    # Current (existing) album; current_min=None means no current album.
    current_min: int | None
    current_avg: int | None
    current_format: str
    current_is_cbr: bool
    current_grade: str | None
    current_spectral_bitrate: int | None
    current_v0_avg: int | None
    current_verified_lossless_proof: bool
    # Candidate download.
    candidate_kind: str  # "lossy" | "flac_raw" | "flac_converted"
    min_bitrate: int
    is_cbr: bool
    avg_bitrate: int | None
    grade: str | None
    spectral_bitrate: int | None
    candidate_format: str
    converted_count: int
    post_conversion_min_bitrate: int | None
    post_conversion_is_cbr: bool | None
    v0_avg: int | None
    v0_min: int | None
    # Action facts.
    target_format: str | None
    verified_lossless_target: str | None
    # issue #1241 — the operator's incomplete mark and beets' coverage
    # proof. Defaulted so the promoted @example pins above this change keep
    # describing their own worlds.
    installed_marked_incomplete: bool = False
    candidate_covers_declared_program: bool = False


@st.composite
def parity_worlds(draw) -> ParityWorld:
    has_current = draw(st.booleans())
    if has_current:
        current_min = draw(_bitrates())
        current_avg = draw(_bitrates())
        current_format = draw(st.sampled_from(_CURRENT_FORMATS))
        current_is_cbr = draw(st.booleans())
        current_grade = draw(st.sampled_from(_GRADES))
        current_spectral_bitrate = (
            draw(_optional_bitrates(max_value=400))
            if current_grade is not None
            else None
        )
        current_v0_avg = draw(_optional_bitrates(max_value=400))
        current_verified_lossless_proof = draw(st.booleans())
    else:
        current_min = current_avg = None
        current_format = "MP3"
        current_is_cbr = False
        current_grade = None
        current_spectral_bitrate = None
        current_v0_avg = None
        current_verified_lossless_proof = False

    # candidate_format only matters for lossy worlds; FLAC kinds carry the
    # placeholder "FLAC" (the evidence builder ignores native_codec/format
    # when is_flac=True).
    kind = draw(st.sampled_from(("lossy", "flac_raw", "flac_converted")))
    grade = draw(st.sampled_from(_GRADES))
    spectral_bitrate = (
        draw(_optional_bitrates(max_value=400))
        if grade is not None
        else None
    )
    if kind == "lossy":
        min_bitrate = draw(_bitrates(max_value=2000))
        is_cbr = draw(st.booleans())
        avg_bitrate = draw(_bitrates(max_value=2000))
        candidate_format = draw(st.sampled_from(_LOSSY_FORMATS))
        converted_count = 0
        post_conversion = None
        post_conversion_is_cbr = None
        v0_avg = v0_min = None
        target_format = draw(st.sampled_from(_TARGET_FORMATS))
    elif kind == "flac_raw":
        min_bitrate = draw(_bitrates(max_value=3000))
        is_cbr = False
        avg_bitrate = None
        candidate_format = "FLAC"
        converted_count = 0
        post_conversion = None
        post_conversion_is_cbr = None
        v0_avg = draw(_optional_bitrates(max_value=400))
        v0_min = draw(_optional_bitrates(max_value=400))
        target_format = draw(st.sampled_from(("flac", "lossless")))
    else:  # flac_converted
        min_bitrate = draw(_bitrates(max_value=3000))
        is_cbr = False
        avg_bitrate = None
        candidate_format = "FLAC"
        converted_count = draw(st.integers(min_value=1, max_value=30))
        projected_bitrates = draw(st.lists(
            _bitrates(max_value=400), min_size=1, max_size=8
        ))
        post_conversion = min(projected_bitrates)
        post_conversion_is_cbr = len(set(projected_bitrates)) == 1
        v0_avg = draw(_optional_bitrates(max_value=400))
        v0_min = draw(_optional_bitrates(max_value=400))
        target_format = draw(st.sampled_from((None, "mp3 v0", "opus 128")))

    return ParityWorld(
        current_min=current_min,
        current_avg=current_avg,
        current_format=current_format,
        current_is_cbr=current_is_cbr,
        current_grade=current_grade,
        current_spectral_bitrate=current_spectral_bitrate,
        current_v0_avg=current_v0_avg,
        current_verified_lossless_proof=current_verified_lossless_proof,
        candidate_kind=kind,
        min_bitrate=min_bitrate,
        is_cbr=is_cbr,
        avg_bitrate=avg_bitrate,
        grade=grade,
        spectral_bitrate=spectral_bitrate,
        candidate_format=candidate_format,
        converted_count=converted_count,
        post_conversion_min_bitrate=post_conversion,
        post_conversion_is_cbr=post_conversion_is_cbr,
        v0_avg=v0_avg,
        v0_min=v0_min,
        target_format=target_format,
        verified_lossless_target=draw(st.sampled_from(_VL_TARGETS)),
        installed_marked_incomplete=draw(st.booleans()),
        candidate_covers_declared_program=draw(st.booleans()),
    )


_NATIVE_CODECS = {
    "MP3": "mp3",
    "Opus": "opus",
    "AAC": "aac",
    "Vorbis": "vorbis",
    "WMA": "wma",
    "FLAC": "flac",
}


def _parity_simulator_result(world: ParityWorld) -> SimResult:
    is_flac = world.candidate_kind != "lossy"
    album = AlbumState(
        name="parity_current",
        min_bitrate=world.current_min,
        is_cbr=world.current_is_cbr,
        spectral_grade=world.current_grade,
        spectral_bitrate=world.current_spectral_bitrate,
        verified_lossless=False,
        search_filetype_override=None,
        target_format=world.target_format,
        existing_format=(
            world.current_format if world.current_min is not None else None),
        avg_bitrate=world.current_avg,
        existing_v0_probe_avg=world.current_v0_avg,
    )
    download = DownloadScenario(
        name="parity_candidate",
        is_flac=is_flac,
        min_bitrate=world.min_bitrate,
        is_cbr=world.is_cbr,
        spectral_grade=world.grade,
        spectral_bitrate=world.spectral_bitrate,
        converted_count=world.converted_count,
        post_conversion_min_bitrate=world.post_conversion_min_bitrate,
        post_conversion_is_cbr=world.post_conversion_is_cbr,
        new_format=(None if is_flac else world.candidate_format),
        is_vbr=None,  # both twins derive is_vbr = not is_cbr
        avg_bitrate=(None if is_flac else world.avg_bitrate),
        candidate_v0_probe_avg=world.v0_avg,
        candidate_v0_probe_min=world.v0_min,
        candidate_v0_probe_kind=(
            "lossless_source_v0"
            if world.v0_avg is not None or world.v0_min is not None
            else None
        ),
    )
    return simulate(
        album, download,
        verified_lossless_target=world.verified_lossless_target,
        current_verified_lossless_proof=world.current_verified_lossless_proof,
        installed_marked_incomplete=world.installed_marked_incomplete,
        candidate_covers_declared_program=(
            world.candidate_covers_declared_program
        ),
    )


def _parity_evidence_inputs(
    world: ParityWorld,
) -> tuple[
    AlbumQualityEvidence,
    AlbumQualityEvidence | None,
    AlbumQualityEvidenceDecisionFacts,
]:
    """The world encoded as the evidence decider's three real arguments.

    Split out of ``_parity_evidence_result`` so a property that has to
    perturb ONE evidence field (the ultrasonic proof leg's) can reuse the
    canonical world → evidence mapping instead of encoding worlds twice.
    """
    # flac_converted note: the simulator side carries the raw FLAC
    # min_bitrate while the evidence measurement carries post_conversion —
    # inert today because the FLAC-convert branch of full_pipeline_decision
    # only consults post_conversion. If that branch ever starts reading the
    # raw min, this mapping (not the twins) is what diverged.
    candidate = build_parity_candidate_evidence(
        is_flac=world.candidate_kind != "lossy",
        min_bitrate=world.min_bitrate,
        is_cbr=world.is_cbr,
        avg_bitrate=world.avg_bitrate,
        spectral_grade=world.grade,
        spectral_bitrate=world.spectral_bitrate,
        candidate_v0_probe_avg=world.v0_avg,
        candidate_v0_probe_min=world.v0_min,
        native_codec=_NATIVE_CODECS[world.candidate_format],
        native_format=world.candidate_format,
    )
    v0_metric = None
    if world.current_v0_avg is not None:
        v0_metric = AlbumQualityV0Metric(
            min_bitrate_kbps=None,
            avg_bitrate_kbps=world.current_v0_avg,
            median_bitrate_kbps=world.current_v0_avg,
            subject=EVIDENCE_SUBJECT_SOURCE,
            provenance="measured",
        )
    current = build_parity_current_evidence(
        min_bitrate=world.current_min,
        avg_bitrate=world.current_avg,
        format=world.current_format,
        is_cbr=world.current_is_cbr,
        spectral_grade=world.current_grade,
        spectral_bitrate=world.current_spectral_bitrate,
        v0_metric=v0_metric,
    )
    if current is not None and world.current_verified_lossless_proof:
        current = msgspec.structs.replace(
            current,
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="generated",
                classifier="generated",
            ),
        )
    facts = AlbumQualityEvidenceDecisionFacts(
        verified_lossless_target=world.verified_lossless_target,
        target_format=world.target_format,
        converted_count=world.converted_count,
        post_conversion_min_bitrate=world.post_conversion_min_bitrate,
        post_conversion_is_cbr=world.post_conversion_is_cbr,
        installed_marked_incomplete=world.installed_marked_incomplete,
        candidate_covers_declared_program=(
            world.candidate_covers_declared_program
        ),
    )
    return candidate, current, facts


def _parity_evidence_result(world: ParityWorld) -> dict:
    candidate, current, facts = _parity_evidence_inputs(world)
    return full_pipeline_decision_from_evidence(candidate, current, facts=facts)


# Promoted pins — live-bug shapes from the album test set, kept here so the
# parity property always replays them first (the @example form of the
# "failure becomes permanent regression" policy).
_MOUNTAIN_GOATS_FLUX_WORLD = ParityWorld(
    current_min=320, current_avg=320, current_format="MP3",
    current_is_cbr=True, current_grade=None, current_spectral_bitrate=None,
    current_v0_avg=None, current_verified_lossless_proof=False,
    candidate_kind="flac_converted", min_bitrate=900, is_cbr=False,
    avg_bitrate=None, grade="suspect", spectral_bitrate=160,
    candidate_format="FLAC", converted_count=13,
    post_conversion_min_bitrate=198, v0_avg=211, v0_min=198,
    post_conversion_is_cbr=False,
    target_format=None, verified_lossless_target=None,
)
# Fault-injection pin (2026-07-08 mutation run): dropping the evidence
# adapter's spectral-override derivation survived the suite AND push
# entropy tiers — random worlds rarely make the override decisive. This
# world makes it decisive deterministically: the existing 320 CBR album is
# flagged likely_transcode at 96 kbps, so its effective quality is 96; a
# 192 CBR candidate is an upgrade WITH the override and a downgrade
# without it. The twins can only agree if both derive the override.
_SPECTRAL_OVERRIDE_DECISIVE_WORLD = ParityWorld(
    current_min=320, current_avg=320, current_format="MP3",
    current_is_cbr=True, current_grade="likely_transcode",
    current_spectral_bitrate=96, current_v0_avg=None,
    current_verified_lossless_proof=False,
    candidate_kind="lossy", min_bitrate=192, is_cbr=True, avg_bitrate=192,
    grade=None, spectral_bitrate=None, candidate_format="MP3",
    converted_count=0, post_conversion_min_bitrate=None, v0_avg=None,
    post_conversion_is_cbr=None,
    v0_min=None, target_format=None, verified_lossless_target=None,
)
_HERETIC_PRIDE_WORLD = ParityWorld(
    current_min=192, current_avg=192, current_format="MP3",
    current_is_cbr=False, current_grade="genuine",
    current_spectral_bitrate=None, current_v0_avg=None,
    current_verified_lossless_proof=False,
    candidate_kind="lossy", min_bitrate=192, is_cbr=False, avg_bitrate=192,
    grade="genuine", spectral_bitrate=None, candidate_format="MP3",
    converted_count=0, post_conversion_min_bitrate=None, v0_avg=None,
    post_conversion_is_cbr=None,
    v0_min=None, target_format=None, verified_lossless_target=None,
)
# The #829 PR3 review world (2026-07-31). The Bill Hicks rescue shape —
# HF-poor lossless graded ``suspect`` with a lossless_source_v0 probe at
# avg 241 / min 219 — against a PROVISIONAL installed album: previously
# imported from a lossless source we ground down, so its linked probe
# (avg 240) is the only comparable anchor, and it carries no proof. 241
# does not clear 240 by the rank tolerance, so the provisional lane
# answers ``suspect_lossless_downgrade`` — a confident reject that
# denylists the peer — while the measured lane imports this candidate over
# a 128kbps CBR copy. A denial that reached the lane choice therefore cost
# the album its import outright. Pinned here so the generated property
# always replays it first.
_DENIAL_PROVISIONAL_COHORT_WORLD = ParityWorld(
    current_min=128, current_avg=128, current_format="MP3",
    current_is_cbr=True, current_grade=None, current_spectral_bitrate=None,
    current_v0_avg=240, current_verified_lossless_proof=False,
    candidate_kind="flac_converted", min_bitrate=900, is_cbr=False,
    avg_bitrate=None, grade="suspect", spectral_bitrate=None,
    candidate_format="FLAC", converted_count=1,
    post_conversion_min_bitrate=219, post_conversion_is_cbr=False,
    v0_avg=241, v0_min=219,
    target_format=None, verified_lossless_target="opus 128",
)
#: Request 2066 (Sound Dimension — *Jamaica Soul Shake, Vol. 1*), download
#: 39207: a genuine-graded FLAC whose proof the ultrasonic leg denies
#: (71.29 dB, injected by the checkers), converting to an opus-128 target
#: that measures 110k avg on disk. Candidate probe 175 vs anchor 177 —
#: within tolerance, NOT better. The world that bought issue #990.
_REQUEST_2066_WORLD = ParityWorld(
    current_min=95, current_avg=110, current_format="OPUS",
    current_is_cbr=False, current_grade="genuine",
    current_spectral_bitrate=None,
    current_v0_avg=177, current_verified_lossless_proof=False,
    candidate_kind="flac_converted", min_bitrate=354, is_cbr=False,
    avg_bitrate=None, grade="genuine", spectral_bitrate=128,
    candidate_format="FLAC", converted_count=1,
    post_conversion_min_bitrate=93, post_conversion_is_cbr=False,
    v0_avg=175, v0_min=148,
    target_format=None, verified_lossless_target="opus 128",
)
#: The #993 R4 world: a proof-denied, affirming-grade lossless source with
#: an explicit candidate V0 probe and no comparable current anchor. It must
#: import through the provisional lane; a measured reject would be a routing
#: bypass, not a comparison the candidate's source evidence authorizes.
_UNANCHORED_PROVISIONAL_IMPORT_WORLD = replace(
    _REQUEST_2066_WORLD,
    current_v0_avg=None,
)
#: The R4 comparator-bypass world. The real provisional lane imports this
#: unproven lossless source, but a router that skips the lane reaches the
#: production comparator and correctly gets a measured downgrade: Opus 64
#: cannot replace an existing MP3 320 CBR copy.
_UNANCHORED_MEASURED_REJECT_WORLD = replace(
    _UNANCHORED_PROVISIONAL_IMPORT_WORLD,
    current_min=320,
    current_avg=320,
    current_format="MP3",
    current_is_cbr=True,
    current_grade=None,
    post_conversion_min_bitrate=64,
    target_format="opus 64",
    verified_lossless_target="opus 64",
)
#: Request 2066 with both #1241 conjuncts set: the operator marked the
#: installed OPUS 110 copy incomplete AND this attempt's candidate covers
#: the whole declared program. Production disregards the installed side
#: entirely on this predicate — the 177/175 anchor comparison V6 polices
#: never happens, because there is no existing side left to anchor against.
#: V6's checker must recognise this as out of its own scope, not assert the
#: anchor law over a world production never treats as anchored.
_REQUEST_2066_MARKED_INCOMPLETE_WORLD = replace(
    _REQUEST_2066_WORLD,
    installed_marked_incomplete=True,
    candidate_covers_declared_program=True,
)
# Issue #1241 pin — request 1852, Dirt Dress *Theme Songs* (Discogs
# 4738671, download_log 40355). Installed AAC ~128 missing 700 s of declared
# program (operator-marked incomplete); candidate MP3 ~196 that beets proved
# complete. The unmarked comparison is an honest cross_family_same_rank
# "equivalent" → downgrade, which is exactly how the reducer deleted the
# only copy of the missing half hour. Replayed first on every run of the
# disregard property.
_DIRT_DRESS_MARKED_INCOMPLETE_WORLD = ParityWorld(
    current_min=128, current_avg=128, current_format="AAC",
    current_is_cbr=False, current_grade=None,
    current_spectral_bitrate=None, current_v0_avg=None,
    current_verified_lossless_proof=False,
    candidate_kind="lossy", min_bitrate=196, is_cbr=False, avg_bitrate=196,
    grade=None, spectral_bitrate=None, candidate_format="MP3",
    converted_count=0, post_conversion_min_bitrate=None, v0_avg=None,
    post_conversion_is_cbr=None,
    v0_min=None, target_format=None, verified_lossless_target=None,
    installed_marked_incomplete=True,
    candidate_covers_declared_program=True,
)
_PARTS_AND_LABOR_VORBIS_WORLD = ParityWorld(
    current_min=128, current_avg=128, current_format="MP3",
    current_is_cbr=True, current_grade=None,
    current_spectral_bitrate=None, current_v0_avg=None,
    current_verified_lossless_proof=False,
    candidate_kind="lossy", min_bitrate=192, is_cbr=False, avg_bitrate=192,
    grade="genuine", spectral_bitrate=None, candidate_format="Vorbis",
    converted_count=0, post_conversion_min_bitrate=None, v0_avg=None,
    post_conversion_is_cbr=None,
    v0_min=None, target_format=None, verified_lossless_target=None,
)


class TestGeneratedParity(unittest.TestCase):
    """Machine-checks 'quality decisions live in ONE place' over the whole
    generated common-language space, not just the hand-picked album set."""

    @given(world=parity_worlds())
    @example(world=_MOUNTAIN_GOATS_FLUX_WORLD)
    @example(world=_HERETIC_PRIDE_WORLD)
    @example(world=_SPECTRAL_OVERRIDE_DECISIVE_WORLD)
    @example(world=_PARTS_AND_LABOR_VORBIS_WORLD)
    def test_decision_twins_agree(self, world):
        sim = _parity_simulator_result(world)
        evidence_result = _parity_evidence_result(world)
        assert_twins_agree(sim, evidence_result)

    @given(world=parity_worlds())
    def test_proof_bearing_current_blocks_every_candidate_in_both_twins(
            self, world):
        # ``installed_marked_incomplete=False``: the ONE thing that outranks
        # the decision-21 ceiling is the operator's own incomplete mark
        # (issue #1241), whose disarm has its own property below. This
        # property states the lock's contract for every UNMARKED world.
        proof_world = replace(
            world,
            current_min=(world.current_min or 245),
            current_avg=(world.current_avg or 245),
            current_verified_lossless_proof=True,
            installed_marked_incomplete=False,
        )
        sim = _parity_simulator_result(proof_world)
        evidence_result = _parity_evidence_result(proof_world)
        assert_verified_lossless_proof_locks_candidate(sim)
        assert_twins_agree(sim, evidence_result)

    @given(world=parity_worlds())
    @example(world=_DIRT_DRESS_MARKED_INCOMPLETE_WORLD)
    # The other three corners of the same world: neither conjunct alone may
    # change anything. Random worlds reach this exact equivalent-verdict
    # downgrade shape rarely, so the corners are pinned deterministically.
    @example(world=replace(
        _DIRT_DRESS_MARKED_INCOMPLETE_WORLD,
        installed_marked_incomplete=False))
    @example(world=replace(
        _DIRT_DRESS_MARKED_INCOMPLETE_WORLD,
        candidate_covers_declared_program=False))
    @example(world=replace(
        _DIRT_DRESS_MARKED_INCOMPLETE_WORLD,
        installed_marked_incomplete=False,
        candidate_covers_declared_program=False))
    # Completeness outranks quality at EVERY level: a complete 96 CBR
    # candidate against a marked incomplete 320 install still imports.
    @example(world=replace(
        _DIRT_DRESS_MARKED_INCOMPLETE_WORLD,
        current_format="MP3", current_min=320, current_avg=320,
        current_is_cbr=True,
        candidate_format="MP3", min_bitrate=96, avg_bitrate=96, is_cbr=True))
    # The decision-21 ceiling yields to the mark: a proof-LOCKED installed
    # copy is disregarded like any other when the operator marked it.
    @example(world=replace(
        _DIRT_DRESS_MARKED_INCOMPLETE_WORLD,
        current_verified_lossless_proof=True))
    # The lossless-source anchor yields too (#1257 review F5): an installed
    # provisional copy with a source V0 probe rejects every lossy candidate
    # as lossless_source_locked, and random worlds reach that lock with the
    # mark set too rarely to rely on — a mutant keeping the probe under the
    # disregard died only at the fuzz tier before this pin.
    @example(world=replace(
        _DIRT_DRESS_MARKED_INCOMPLETE_WORLD,
        current_format="OPUS", current_min=110, current_avg=116,
        current_v0_avg=240,
        candidate_format="MP3", min_bitrate=245, avg_bitrate=245))
    def test_operator_incomplete_mark_disregards_the_installed_side(
        self, world,
    ):
        """Issue #1241, both halves of the invariant, on both twins.

        Positive half — when the operator marked the installed copy
        incomplete AND beets proved this attempt's candidate covers the
        declared program, the decision must be BYTE-IDENTICAL to the same
        candidate arriving at an empty slot (no current album at all),
        except for the audit flag recording the disregard. Fresh-import
        admission IS the policy: the absolute candidate-side floors still
        apply, nothing existing-side can block, and the post-import gate
        still keeps a below-par import searching.

        Negative half — in every other world the decision dict must be
        byte-identical to the same world with both facts absent. Neither
        conjunct alone may change anything. This is the no-regression half.
        """
        disregarded = (
            world.installed_marked_incomplete
            and world.candidate_covers_declared_program
        )
        sim = _parity_simulator_result(world)
        evidence_result = _parity_evidence_result(world)
        assert_twins_agree(sim, evidence_result)

        if disregarded:
            fresh_world = replace(
                world,
                current_min=None, current_avg=None,
                current_format="MP3", current_is_cbr=False,
                current_grade=None, current_spectral_bitrate=None,
                current_v0_avg=None,
                current_verified_lossless_proof=False,
                installed_marked_incomplete=False,
                candidate_covers_declared_program=False,
            )
            expected = dict(_parity_evidence_result(fresh_world))
            expected["installed_incomplete_disregarded"] = True
            self.assertEqual(
                evidence_result,
                expected,
                "a marked+covered world must decide exactly as the same "
                "candidate arriving at an empty slot",
            )
        else:
            baseline_world = replace(
                world,
                installed_marked_incomplete=False,
                candidate_covers_declared_program=False,
            )
            self.assertEqual(
                evidence_result,
                _parity_evidence_result(baseline_world),
                "neither #1241 fact may change any world where the "
                "predicate does not hold",
            )


# ===========================================================================
# Evidence-side properties — reach the branches the simulator language
# cannot express: the folder/audio-integrity early exits and the
# fail-closed handling of incomplete evidence rows.
# ===========================================================================

_EVIDENCE_EXTS = ("mp3", "flac", "opus", "aac", "wav", "alac", "m4a")


@st.composite
def wild_ready_candidate_evidence(draw) -> AlbumQualityEvidence:
    exts = draw(st.lists(st.sampled_from(_EVIDENCE_EXTS), min_size=1, max_size=4))
    files = [
        AlbumQualityEvidenceFile(
            relative_path=f"{i:02d}.{ext}",
            size_bytes=1, mtime_ns=1,
            extension=ext, container=ext, codec=ext,
        )
        for i, ext in enumerate(exts)
    ]
    v0_metric = None
    if draw(st.booleans()):
        # Readiness floor: a stored V0 metric carries at least one bitrate.
        v0_metric = AlbumQualityV0Metric(
            min_bitrate_kbps=draw(_optional_bitrates(max_value=400)),
            avg_bitrate_kbps=draw(_bitrates(max_value=400)),
            median_bitrate_kbps=None,
            subject=draw(st.sampled_from((
                EVIDENCE_SUBJECT_SOURCE,
                EVIDENCE_SUBJECT_INSTALLED,
            ))),
            provenance="measured",
        )
    # verified_lossless=True is only a ready (storable-for-action) state
    # when a proof provenance rides along — pair them, as production does.
    verified_lossless = draw(st.booleans())
    proof = (
        VerifiedLosslessProof(
            provenance="measured", source="generated",
            classifier="generated")
        if verified_lossless else None
    )
    measured_format = draw(st.sampled_from(("MP3", "FLAC", "Opus", "AAC")))
    codec = draw(st.sampled_from(_EVIDENCE_EXTS))
    container = draw(st.sampled_from(_EVIDENCE_EXTS))
    target_format = None
    target_is_cbr = None
    lossless_source = (
        measured_format == "FLAC"
        or codec in {"flac", "wav", "alac"}
        or container in {"flac", "wav", "alac"}
        or verified_lossless
        or (
            v0_metric is not None
            and v0_metric.subject == EVIDENCE_SUBJECT_SOURCE
        )
    )
    if lossless_source:
        # Actionable v4 evidence from a lossless source has already projected
        # its target. Measurement-only rows are the separate early-reject
        # writer and never enter this ready-candidate strategy.
        target_format = draw(
            st.sampled_from(("MP3", "mp3 v0", "opus 128", "flac"))
        )
        target_is_cbr = draw(st.booleans())
    spectral_grade = draw(st.sampled_from(_GRADES))
    spectral_bitrate = (
        draw(_optional_bitrates(max_value=400))
        if spectral_grade is not None else None
    )
    # issue #829 Phase 5 PR3 proof-leg facts. Drawn WIDE and unfiltered,
    # including deficits either side of the frozen threshold and every
    # measurement-version state, so the wild properties actually reach the
    # ultrasonic leg's branches instead of only its withheld default. The
    # evidence row's own validation forbids these without a grade.
    ultrasonic_deficit_db = (
        draw(st.one_of(
            st.none(),
            st.floats(min_value=0.0, max_value=140.0,
                      allow_nan=False, allow_infinity=False),
        ))
        if spectral_grade is not None else None
    )
    spectral_measurement_version = (
        draw(st.sampled_from((None, 1, 2, 3)))
        if spectral_grade is not None else None
    )
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=draw(_bitrates(max_value=4000)),
        avg_bitrate_kbps=draw(_optional_bitrates(max_value=4000)),
        median_bitrate_kbps=draw(_optional_bitrates(max_value=4000)),
        format=measured_format,
        is_cbr=draw(st.booleans()),
        spectral_grade=spectral_grade,
        spectral_bitrate_kbps=spectral_bitrate,
        spectral_subject=("source" if spectral_grade is not None else None),
        spectral_provenance=(
            "measured" if spectral_grade is not None else None
        ),
        ultrasonic_deficit_db=ultrasonic_deficit_db,
        spectral_measurement_version=spectral_measurement_version,
    )
    has_bad_hash = draw(st.booleans())
    audio_corrupt = draw(st.booleans())
    if audio_corrupt:
        files[0] = msgspec.structs.replace(files[0], decode_ok=False)
    return AlbumQualityEvidence(
        mb_release_id="generated-evidence",
        snapshot_fingerprint="sha256:generated-fingerprint",
        source_path="/Incoming/auto-import/generated",
        measurement=measurement,
        measured_at=datetime(2026, 7, 8, tzinfo=UTC),
        files=files,
        codec=codec,
        container=container,
        storage_format=measured_format,
        target_format=target_format,
        target_is_cbr=target_is_cbr,
        v0_metric=v0_metric,
        verified_lossless_proof=proof,
        audio_validation=(
            make_audio_corrupt_validation_report(files[0].relative_path)
            if audio_corrupt
            else legacy_unrecorded_audio_validation_report()
        ),
        audio_corrupt=audio_corrupt,
        folder_layout=draw(st.sampled_from(("flat", "nested"))),
        audio_file_count=draw(st.sampled_from((0, len(files)))),
        filetype_band="generated",
        matched_bad_audio_hash_id=(1 if has_bad_hash else None),
        matched_bad_audio_hash_path=("01.mp3" if has_bad_hash else None),
        # issue #829 AAC-lattice leg PR-B. Drawn WIDE and unfiltered, so
        # the wild properties reach the leg's denial branches instead of
        # only its unmeasured default. Unlike the ultrasonic facts the
        # evidence row places no grade prerequisite on this column.
        aac_lattice=draw(st.one_of(st.none(), _aac_lattice_captures())),
    )


def _expected_early_exit_key(candidate: AlbumQualityEvidence) -> str | None:
    """Documented priority order of the integrity early exits."""
    if candidate.audio_corrupt:
        return "preimport_audio"
    if candidate.matched_bad_audio_hash_id is not None:
        return "preimport_bad_hash"
    if candidate.folder_layout == "nested":
        return "preimport_nested"
    effective_audio_file_count = (
        len(candidate.files) if candidate.files else candidate.audio_file_count
    )
    if effective_audio_file_count == 0:
        return "preimport_empty_fileset"
    if has_mixed_lossless_and_lossy(candidate.files):
        return "preimport_mixed_source"
    return None


_EARLY_EXIT_REJECT_VALUES = {
    "preimport_audio": "reject_corrupt",
    "preimport_bad_hash": "reject_bad_hash",
    "preimport_nested": "reject_nested",
    "preimport_empty_fileset": "reject_empty",
    "preimport_mixed_source": "reject_mixed_source",
}

_EARLY_EXIT_FACT_NAMES = {
    "preimport_audio": "audio_corrupt",
    "preimport_bad_hash": "bad_audio_hash",
    "preimport_nested": "nested_layout",
    "preimport_empty_fileset": "empty_fileset",
    "preimport_mixed_source": "mixed_source",
}

_INTEGRITY_FACTS = (
    "audio_corrupt",
    "bad_audio_hash",
    "nested_layout",
    "empty_fileset",
    "mixed_source",
)


def _with_integrity_fact(
    candidate: AlbumQualityEvidence,
    fact: str,
) -> AlbumQualityEvidence:
    mp3_file = AlbumQualityEvidenceFile(
        relative_path="01.mp3",
        size_bytes=1,
        mtime_ns=1,
        extension="mp3",
        container="mp3",
        codec="mp3",
    )
    clean = msgspec.structs.replace(
        candidate,
        files=[mp3_file],
        audio_validation=legacy_unrecorded_audio_validation_report(),
        audio_corrupt=False,
        folder_layout="flat",
        audio_file_count=1,
        matched_bad_audio_hash_id=None,
        matched_bad_audio_hash_path=None,
    )
    if fact == "audio_corrupt":
        return msgspec.structs.replace(
            clean,
            files=[msgspec.structs.replace(mp3_file, decode_ok=False)],
            audio_validation=make_audio_corrupt_validation_report(
                mp3_file.relative_path,
            ),
            audio_corrupt=True,
        )
    if fact == "bad_audio_hash":
        return msgspec.structs.replace(
            clean,
            matched_bad_audio_hash_id=1,
            matched_bad_audio_hash_path="01.mp3",
        )
    if fact == "nested_layout":
        return msgspec.structs.replace(clean, folder_layout="nested")
    if fact == "empty_fileset":
        return msgspec.structs.replace(clean, files=[], audio_file_count=0)
    if fact == "mixed_source":
        flac_file = AlbumQualityEvidenceFile(
            relative_path="02.flac",
            size_bytes=1,
            mtime_ns=1,
            extension="flac",
            container="flac",
            codec="flac",
        )
        return msgspec.structs.replace(clean, files=[mp3_file, flac_file])
    raise AssertionError(f"unknown generated integrity fact: {fact}")

_VALID_VERDICTS = ("confident_reject", "would_import", "uncertain")


def assert_classification_coherent(
    decision: dict,
    expected_early_exit_key: str | None,
    *,
    classify_fn: Callable[
        [dict[str, object]], tuple[str, bool, str | None]
    ] = classify_full_pipeline_decision,
    name_fn: Callable[[dict[str, object]], str] = evidence_decision_name,
) -> None:
    """The classification layer (cleanup eligibility + dispatch decision
    name) must be coherent with the decision dict it classifies.

    Added after the fuzz-tier coverage diagnostic showed
    ``classify_full_pipeline_decision`` / ``evidence_decision_name``
    (which gate wrong-match folder cleanup) were the one decision-policy
    layer no generated test reached.

    Three of the clauses below key on what the classifiers RETURN, not on the
    decision dict, and today's production pair cannot return those shapes at
    all — an unknown verdict, a falsy dispatch name, or ``cleanup_eligible``
    decoupled from ``confident_reject``. They are the fail-closed half of the
    checker, so the classifiers are kwarg-injected (house DI seam) and the
    known-bad self-test drives them with a planted classifier rather than
    asserting the clauses in prose.
    """
    verdict, cleanup_eligible, reason = classify_fn(decision)
    name = name_fn(decision)
    if verdict not in _VALID_VERDICTS:
        raise AssertionError(f"unknown classification verdict: {verdict!r}")
    if not name or not isinstance(name, str):
        raise AssertionError(f"evidence_decision_name returned {name!r}")
    if cleanup_eligible and verdict != "confident_reject":
        raise AssertionError(
            f"cleanup_eligible without confident_reject: {verdict!r}/{reason!r}")
    if expected_early_exit_key is not None:
        fact = _EARLY_EXIT_FACT_NAMES[expected_early_exit_key]
        if (verdict, cleanup_eligible, reason) != ("confident_reject", True, fact):
            raise AssertionError(
                f"integrity fact {fact} classified as "
                f"({verdict!r}, {cleanup_eligible!r}, {reason!r})")
        if name != fact:
            raise AssertionError(
                f"integrity fact {fact} named {name!r} for dispatch")
    elif decision.get("imported"):
        if verdict != "would_import" or cleanup_eligible:
            raise AssertionError(
                f"imported decision classified as "
                f"({verdict!r}, cleanup_eligible={cleanup_eligible!r})")


class TestGeneratedEvidenceDecider(unittest.TestCase):
    """Properties of the production decider the simulator can't reach."""

    @given(candidate=wild_ready_candidate_evidence())
    def test_integrity_facts_always_reject_in_priority_order(self, candidate):
        result = full_pipeline_decision_from_evidence(candidate, None)

        self.assertIsInstance(result["imported"], bool)
        expected_key = _expected_early_exit_key(candidate)
        if expected_key is None:
            for key, reject_value in _EARLY_EXIT_REJECT_VALUES.items():
                self.assertNotEqual(
                    result[key], reject_value,
                    f"clean candidate tripped integrity reject {key}")
            return

        self.assertFalse(
            result["imported"],
            f"integrity fact {expected_key} must never import")
        self.assertEqual(
            result[expected_key], _EARLY_EXIT_REJECT_VALUES[expected_key])
        for key, reject_value in _EARLY_EXIT_REJECT_VALUES.items():
            if key != expected_key:
                self.assertNotEqual(
                    result[key], reject_value,
                    f"{key} fired alongside higher-priority {expected_key}")
        self.assertEqual(result["final_status"], "wanted")
        self.assertTrue(result["keep_searching"])

    @given(
        candidate=wild_ready_candidate_evidence(),
        integrity_fact=st.sampled_from(_INTEGRITY_FACTS),
    )
    def test_current_proof_precedes_integrity_before_any_import(
        self,
        candidate,
        integrity_fact,
    ):
        candidate = _with_integrity_fact(candidate, integrity_fact)
        current = build_parity_current_evidence(
            min_bitrate=128,
            avg_bitrate=128,
            format="Opus",
        )
        assert current is not None
        current = msgspec.structs.replace(
            current,
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="generated",
                classifier="generated",
            ),
        )

        decision = full_pipeline_decision_from_evidence(candidate, current)
        assert_evidence_proof_lock_preserves_imported(decision)
        self.assertEqual(
            decision["stage2_import"],
            "verified_lossless_locked",
        )

    @given(candidate=wild_ready_candidate_evidence())
    def test_decision_classification_is_coherent(self, candidate):
        result = full_pipeline_decision_from_evidence(candidate, None)
        assert_classification_coherent(
            result, _expected_early_exit_key(candidate))

    def test_incomplete_evidence_fails_closed(self):
        """Evidence rows below the policy floor must raise, not decide."""
        ready = build_parity_candidate_evidence(
            is_flac=False, min_bitrate=245, is_cbr=False)
        no_format = msgspec.structs.replace(
            ready,
            measurement=msgspec.structs.replace(ready.measurement, format=None),
        )
        with self.assertRaises(ValueError):
            full_pipeline_decision_from_evidence(no_format, None)

        no_bitrates = msgspec.structs.replace(
            ready,
            measurement=msgspec.structs.replace(
                ready.measurement,
                min_bitrate_kbps=None,
                avg_bitrate_kbps=None,
                median_bitrate_kbps=None,
            ),
        )
        with self.assertRaises(ValueError):
            full_pipeline_decision_from_evidence(no_bitrates, None)

    def test_current_proof_is_absolute_without_mode_input(self):
        """Decision 21: the verified-lossless proof lock is inside the
        mode-blind reducer; Replace/re-request is the operator's way back in.
        """
        candidate = build_parity_candidate_evidence(
            is_flac=False,
            min_bitrate=320,
            avg_bitrate=320,
            is_cbr=True,
        )
        current = build_parity_current_evidence(
            min_bitrate=128,
            avg_bitrate=128,
            format="MP3",
            is_cbr=True,
        )
        assert current is not None
        current = msgspec.structs.replace(
            current,
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="test",
                classifier="test",
            ),
        )
        result = full_pipeline_decision_from_evidence(
            candidate,
            current,
        )
        self.assertEqual(
            result["stage2_import"],
            "verified_lossless_locked",
        )
        self.assertFalse(result["imported"])
        self.assertEqual(result["final_status"], "imported")


# ===========================================================================
# Twin precedence parity — issue #1355 item 1. The flat simulator twin and
# the evidence-pipeline twin must agree on which folder/audio-integrity fact
# wins, and on the resulting denylist outcome, whenever a candidate carries
# audio_corrupt and/or nested_layout. They used to independently encode the
# ordering and disagreed on exactly the combined world.
# ===========================================================================

@dataclass(frozen=True)
class PreimportTwinWorld:
    """The two folder/audio-integrity facts plus the incidental format
    dimensions needed to drive both decision twins for issue #1355 item 1.
    """
    audio_corrupt: bool
    has_nested_audio: bool
    is_flac: bool
    min_bitrate: int
    is_cbr: bool


@st.composite
def preimport_corrupt_or_nested_worlds(draw) -> PreimportTwinWorld:
    return PreimportTwinWorld(
        audio_corrupt=draw(st.booleans()),
        has_nested_audio=draw(st.booleans()),
        is_flac=draw(st.booleans()),
        min_bitrate=draw(_bitrates(max_value=2000)),
        is_cbr=draw(st.booleans()),
    )


def _flat_twin_preimport_result(world: PreimportTwinWorld) -> dict[str, object]:
    return full_pipeline_decision(
        is_flac=world.is_flac,
        min_bitrate=world.min_bitrate,
        is_cbr=world.is_cbr,
        audio_corrupt=world.audio_corrupt,
        has_nested_audio=world.has_nested_audio,
    )


def _evidence_twin_preimport_result(world: PreimportTwinWorld) -> dict[str, object]:
    candidate = build_parity_candidate_evidence(
        is_flac=world.is_flac,
        min_bitrate=world.min_bitrate,
        is_cbr=world.is_cbr,
        audio_corrupt=world.audio_corrupt,
        folder_layout="nested" if world.has_nested_audio else "flat",
    )
    return full_pipeline_decision_from_evidence(candidate, None)


def preimport_twin_precedence_violations(
    flat_result: dict[str, object],
    evidence_result: dict[str, object],
) -> list[str]:
    """Both violation clauses for the issue #1355 item 1 twin-precedence
    invariant: the two decision twins must agree on the reject reason and
    on the resulting denylist outcome.

    Accumulates rather than short-circuiting (house convention) so one
    generated world tripping both clauses reports both.
    """
    violations: list[str] = []
    flat_name = evidence_decision_name(flat_result)
    evidence_name = evidence_decision_name(evidence_result)
    if flat_name != evidence_name:
        violations.append(
            f"reject reason diverged: flat={flat_name!r} "
            f"evidence={evidence_name!r}")
    if flat_result["denylisted"] != evidence_result["denylisted"]:
        violations.append(
            "denylist outcome diverged: "
            f"flat={flat_result['denylisted']!r} "
            f"evidence={evidence_result['denylisted']!r}")
    return violations


class TestGeneratedPreimportTwinPrecedence(unittest.TestCase):
    """Issue #1355 item 1: the flat simulator and the evidence pipeline must
    agree on which folder/audio-integrity fact wins, and therefore on the
    denylist consequence, whenever a candidate carries audio_corrupt and/or
    nested_layout."""

    @given(world=preimport_corrupt_or_nested_worlds())
    @example(world=PreimportTwinWorld(
        audio_corrupt=True, has_nested_audio=True,
        is_flac=False, min_bitrate=256, is_cbr=False,
    ))
    def test_twins_agree_on_corrupt_and_nested_precedence(
        self, world: PreimportTwinWorld,
    ):
        if not (world.audio_corrupt or world.has_nested_audio):
            return
        flat = _flat_twin_preimport_result(world)
        evidence = _evidence_twin_preimport_result(world)
        violations = preimport_twin_precedence_violations(flat, evidence)
        self.assertEqual(violations, [], f"{violations} for world {world!r}")
        if world.audio_corrupt:
            self.assertEqual(evidence_decision_name(flat), "audio_corrupt")
            self.assertTrue(flat["denylisted"])


class TestPreimportTwinPrecedenceChecker(unittest.TestCase):
    """Known-bad self-tests for ``preimport_twin_precedence_violations``,
    per clause (code-quality.md § Testing — Red/Green TDD)."""

    _WORLD = PreimportTwinWorld(
        audio_corrupt=True, has_nested_audio=True,
        is_flac=False, min_bitrate=256, is_cbr=False,
    )

    def _agreeing_pair(self) -> tuple[dict[str, object], dict[str, object]]:
        return (
            _flat_twin_preimport_result(self._WORLD),
            _evidence_twin_preimport_result(self._WORLD),
        )

    def test_reject_reason_clause_trips_on_a_planted_mismatch(self):
        """Q1: the clause fires when the reject reasons diverge."""
        flat, evidence = self._agreeing_pair()
        planted_flat = dict(flat)
        planted_flat["preimport_audio"] = None
        planted_flat["preimport_nested"] = "reject_nested"
        violations = preimport_twin_precedence_violations(planted_flat, evidence)
        self.assertTrue(
            any(v.startswith("reject reason diverged") for v in violations),
            violations,
        )

    def test_denylist_clause_trips_on_a_planted_mismatch(self):
        """Q1: the clause fires when the denylist outcomes diverge."""
        flat, evidence = self._agreeing_pair()
        planted_flat = dict(flat)
        planted_flat["denylisted"] = False
        violations = preimport_twin_precedence_violations(planted_flat, evidence)
        self.assertTrue(
            any(v.startswith("denylist outcome diverged") for v in violations),
            violations,
        )

    def test_clauses_stay_quiet_on_an_agreeing_pair(self):
        """Q3: both clauses stay quiet on a pair that really agrees, for
        both a fired-fact world and a clean world."""
        flat, evidence = self._agreeing_pair()
        self.assertEqual(
            preimport_twin_precedence_violations(flat, evidence), [])

        clean_world = PreimportTwinWorld(
            audio_corrupt=False, has_nested_audio=False,
            is_flac=False, min_bitrate=256, is_cbr=False,
        )
        self.assertEqual(
            preimport_twin_precedence_violations(
                _flat_twin_preimport_result(clean_world),
                _evidence_twin_preimport_result(clean_world),
            ),
            [],
        )


# ===========================================================================
# Harness self-tests (RED/GREEN of the fuzzer itself) — each invariant
# checker must trip on a planted violating decision, and a planted-bad
# decider must be caught end-to-end through the Hypothesis machinery.
# ===========================================================================

def _planted_bad_import(
    album: AlbumState | None = None,
    download: DownloadScenario | None = None,
) -> SimResult:
    """A decider handed a world that it ignores — it always imports.

    The parameters exist so the planted defect is modelled faithfully: the
    world REACHES this decider and never reaches the decision. Callers that
    only need the bad result omit them.
    """
    del album, download  # the planted defect: the world never reaches the decision
    return SimResult(
        imported=True,
        keep_searching=False,
        denylisted=False,
        final_status="imported",
        stage0_spectral_gate="would_run",
        stage1_spectral=None,
        stage2_import="import",
        stage3_quality_gate="accept",
        backfill_override=None,
        search_filetype_override_after=None,
    )


def _ill_typed_sim_result(field: str, value: object) -> SimResult:
    """A ``SimResult`` whose runtime type for one field violates its annotation.

    The totality clauses of ``assert_decision_is_definitive`` exist for exactly
    the values the annotations forbid, so their world can only be built by
    going around the frozen dataclass rather than through it.
    """
    result = _planted_bad_import()
    object.__setattr__(result, field, value)
    return result


class TestInadmissiblePairDomainIsWhatItClaims(unittest.TestCase):
    """The #829 PR2c domain really is the domain it says it is.

    ``test_stage1_never_consumes_an_inadmissible_existing_class`` asserts
    unconditionally, because a checker that asks the function under test
    whether its own precondition holds goes vacuous the moment that
    function is what broke. These two tests carry the precondition
    instead: they hold ``inadmissible_spectral_pair_worlds`` to production's
    OWN refusal — comparability False, for the reason the world declares.
    Mutate ``spectral_classes_comparable`` and both halves fail: this one
    because the refusal disappears, the property because Stage 1 then
    consumes the class.
    """

    def test_the_pinned_worlds_really_are_inadmissible(self):
        for world in _PINNED_INADMISSIBLE_WORLDS:
            with self.subTest(shape=world.shape, world=world):
                comparability = _inadmissible_pair_comparability(world)
                self.assertFalse(comparability.comparable)
                self.assertEqual(comparability.reason, world.shape)

    @given(world=inadmissible_spectral_pair_worlds())
    def test_every_generated_world_really_is_inadmissible(self, world):
        comparability = _inadmissible_pair_comparability(world)
        self.assertFalse(
            comparability.comparable,
            f"strategy produced a COMPARABLE pair: {world!r}",
        )
        self.assertEqual(
            comparability.reason, world.shape,
            f"world declares {world.shape!r} but production refuses for "
            f"{comparability.reason!r}: {world!r}",
        )

    @given(world=inadmissible_spectral_pair_worlds())
    def test_every_generated_candidate_reaches_stage_1_with_a_class(
        self, world,
    ):
        """The candidate side is decision-grade AND the gate fires.

        Without both, clause 1 is unreachable: ``spectral_import_decision``
        only rejects when BOTH values are non-zero, so a candidate with no
        class can never produce the Stage-1 rejection this domain exists to
        forbid. That is an entropy-budget constraint of the same kind
        ``stage_parity_worlds``' ladder-narrowing already had to fix once.

        **It is not a claim that a class-less candidate is inert.** Clause 2
        still bites there: with an authorizing grade and no candidate class,
        ``spectral_import_decision('suspect', None, 0)`` is
        ``import_no_exist`` while admitting the existing class gives
        ``import`` — a real withheld-evidence violation. Those are
        ``left_not_decision_grade`` worlds, the FOURTH refusal reason,
        deliberately absent from ``_INADMISSIBLE_SHAPES``; they are very
        live (the 2,503 rows parking a container bitrate in
        ``spectral_bitrate_kbps`` are exactly this shape). The exclusion is
        a budget decision, not a safety argument, and it is recorded here so
        the next reader can widen it on purpose.
        """
        candidate = interpret_spectral_evidence(SpectralEvidenceFacts(
            spectral_grade=world.grade,
            format="MP3",
            cliff_hz=world.new_cliff_hz,
            spectral_bitrate_kbps=world.new_spectral,
        ))
        self.assertIsNotNone(decision_class_kbps(candidate), repr(world))
        self.assertIsNotNone(
            _inadmissible_pair_stage1(
                world, withhold_existing_spectral=False),
            f"the preimport gate never fired: {world!r}",
        )


# ===========================================================================
# Proof gate v3 — the ultrasonic deficit leg (issue #829 Phase 5 PR3)
#
# Three invariants, each with a module-level checker and a known-bad
# self-test. The deterministic pins live in
# ``tests/test_quality_classification.py::TestUltrasonicProofGateV3`` and
# ``::TestVerifiedLosslessClassifierGeneration``.
#
#   V1  A denying leg is a hard veto: no verified-lossless status survives
#       it, by ANY other route, including the V0-avg trust override.
#   V2  The leg's ENTIRE effect is a veto: result(leg) == result(no leg)
#       AND NOT leg.denies. A non-denying leg is completely inert (this
#       is "never retroactively demote" — most of the library is
#       permanently in that state) and a PASSING leg never grants proof
#       the pre-v3 rules refused. V2b drives the same law through the
#       whole evidence decider.
#   V3  ``verified_lossless_classifier`` says v3 exactly when the leg
#       adjudicated and passed, never merely because v3 code ran.
#   V5  The leg decides the PROOF, never the LANE. The V0 trust override
#       selects between the provisional-lossless lane and the measured
#       comparison on the probe's evidence alone; a denial that re-routed
#       that choice would turn a withheld proof into the lane's confident
#       reject — a discarded album and an accused peer.
# ===========================================================================

#: Container tokens whose decode path is sox-native, derived here from the
#: analyzer's OWN routing table rather than from the interpretation module
#: under test — an oracle that asked the code under test would be
#: unfalsifiable.
_SOX_NATIVE_TOKENS_ORACLE = frozenset(
    ext.lstrip(".") for ext in _SOX_NATIVE_EXTS
)


def _leg_is_withheld_by_oracle(candidate: AlbumQualityEvidence) -> bool:
    """Independent oracle for "the ultrasonic leg cannot adjudicate".

    Restates the rule from the plan rather than asking the production
    resolver, so a mutant that widened or narrowed adjudication cannot
    make the property vacuous. ``wild_ready_candidate_evidence`` never
    sets ``was_converted_from``, so the measured subject is always the
    snapshot and the decode path is its files' own containers.
    """
    measurement = candidate.measurement
    version = measurement.spectral_measurement_version
    if version is None or version < 2:
        return True
    if measurement.ultrasonic_deficit_db is None:
        return True
    tokens = {file.extension.strip().lower().lstrip(".")
              for file in candidate.files}
    # No containers is no answer: the production resolver returns None for
    # an empty label set (``len(paths) != 1``), which the leg reads as
    # ``uncalibrated_decode_path`` and withholds. An oracle that called
    # that world adjudicable would assert the leg ran where production
    # fails closed.
    return tokens == set() or tokens - _SOX_NATIVE_TOKENS_ORACLE != set()


def _without_proof_leg_facts(
    candidate: AlbumQualityEvidence,
) -> AlbumQualityEvidence:
    """The same album with no ultrasonic evidence at all — the pre-v3
    world, which is where the library's un-backfillable cohort lives."""
    return msgspec.structs.replace(
        candidate,
        measurement=msgspec.structs.replace(
            candidate.measurement,
            ultrasonic_deficit_db=None,
            spectral_measurement_version=None,
        ),
    )


def denying_leg_is_a_hard_veto(
    *,
    spectral_grade: "str | None",
    target_format: "str | None",
    converted_count: int,
    is_transcode: bool,
    v0_probe: "V0ProbeEvidence | None",
    leg: UltrasonicProofLeg,
    decider: "Callable[..., bool]" = determine_verified_lossless,
) -> bool:
    """Invariant checker V1: a ``denied`` leg admits no verified-lossless
    status through any other route.

    The route that matters is the V0-avg trust override, which exists to
    rescue HF-poor lossless from a false ``suspect`` grade and which the
    measured probe axis (``probe_pair.tsv.gz``, 5,670 files) does NOT
    separate the FLAC-container launder classes with. If the override
    could outrank the leg, the leg would be decorative.

    ``decider`` is injectable ONLY so the known-bad self-test can plant
    the wrong ordering; production always uses the default.
    """
    if leg.outcome != "denied":
        return True
    return decider(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, ultrasonic_leg=leg,
    ) is False


def _decoy_decider_checks_the_leg_after_the_v0_override(
    target_format: "str | None",
    spectral_grade: "str | None",
    converted_count: int,
    is_transcode: bool,
    *,
    v0_probe: "V0ProbeEvidence | None" = None,
    has_lossy_passthrough: bool = False,
    ultrasonic_leg: "UltrasonicProofLeg | None" = None,
) -> bool:
    """The ordering bug: consult the leg only when nothing else already
    said yes, so a V0-rescued suspect launder keeps its proof. Used only
    to prove the checker trips."""
    verified = determine_verified_lossless(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, has_lossy_passthrough=has_lossy_passthrough,
    )
    if not verified:
        return False
    rescued = (
        spectral_grade in ("suspect", "likely_transcode")
        and v0_probe_overrides_spectral(v0_probe)
    )
    if rescued:
        return True
    return not (ultrasonic_leg is not None and ultrasonic_leg.denies_promotion)


def the_leg_only_ever_subtracts(
    *,
    spectral_grade: "str | None",
    target_format: "str | None",
    converted_count: int,
    is_transcode: bool,
    v0_probe: "V0ProbeEvidence | None",
    leg: UltrasonicProofLeg,
    decider: "Callable[..., bool]" = determine_verified_lossless,
) -> bool:
    """Invariant checker V2: the leg's ENTIRE effect on verified-lossless
    status is a veto.

        result(leg) == result(no leg) AND NOT leg.denies_promotion

    Two halves, both load-bearing:

    * A non-denying leg is completely inert. This is "never retroactively
      demote" stated so it cannot be lost: 6,273 proof rows can never be
      re-measured (their lossless source was converted away) and 8,273
      more predate the capture, so a leg that could move ANY of them
      would silently change the shipped behaviour of most of the library.
    * A ``passed`` leg is inert too — the leg NEVER grants proof the
      pre-v3 rules would have refused. It is a denial instrument, not an
      affirmative one; the only thing a pass earns is the v3 classifier.

    ``decider`` is injectable ONLY so the known-bad self-tests can plant a
    reader that violates one half; production always uses the default.
    """
    with_leg = decider(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, ultrasonic_leg=leg,
    )
    without_leg = decider(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, ultrasonic_leg=None,
    )
    return with_leg == (without_leg and not leg.denies_promotion)


def _decoy_decider_treats_a_withheld_leg_as_denying(
    target_format: "str | None",
    spectral_grade: "str | None",
    converted_count: int,
    is_transcode: bool,
    *,
    v0_probe: "V0ProbeEvidence | None" = None,
    has_lossy_passthrough: bool = False,
    ultrasonic_leg: "UltrasonicProofLeg | None" = None,
) -> bool:
    """The fail-closed-too-hard reader: "no ultrasonic evidence" read as
    "no ultrasonic content". It demotes every legacy and un-backfillable
    row in the library. Used only to prove the checker trips."""
    if ultrasonic_leg is not None and ultrasonic_leg.outcome != "passed":
        return False
    return determine_verified_lossless(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, has_lossy_passthrough=has_lossy_passthrough,
        ultrasonic_leg=ultrasonic_leg,
    )


def _decoy_decider_lets_a_pass_grant_proof(
    target_format: "str | None",
    spectral_grade: "str | None",
    converted_count: int,
    is_transcode: bool,
    *,
    v0_probe: "V0ProbeEvidence | None" = None,
    has_lossy_passthrough: bool = False,
    ultrasonic_leg: "UltrasonicProofLeg | None" = None,
) -> bool:
    """The other direction, and the more seductive one: a passing leg is
    affirmative evidence, so let it certify an album the pre-v3 rules
    refused. That would make an errored or ungraded album
    verified-lossless on one statistic. Used only to prove the checker
    trips."""
    if ultrasonic_leg is not None and ultrasonic_leg.outcome == "passed":
        return True
    return determine_verified_lossless(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, has_lossy_passthrough=has_lossy_passthrough,
        ultrasonic_leg=ultrasonic_leg,
    )


def withheld_leg_leaves_the_decision_untouched(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None",
    *,
    decider: "Callable[..., dict[str, object]]" = (
        full_pipeline_decision_from_evidence
    ),
) -> bool:
    """Invariant checker V2b, the composed half: when the leg cannot
    adjudicate, the WHOLE decision dict is bit-identical to the same
    album carrying no ultrasonic evidence at all.

    V2 pins the pure decision; this one drives the real evidence decider
    end to end, so a reader that picked the raw fields up somewhere other
    than ``determine_verified_lossless`` is caught too.

    ``decider`` is injectable ONLY so the known-bad self-test can plant
    such a reader; production always uses the default.
    """
    if not _leg_is_withheld_by_oracle(candidate):
        return True
    return decider(candidate, current) == decider(
        _without_proof_leg_facts(candidate), current,
    )


def _decoy_decider_reads_the_raw_deficit_outside_the_leg(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
) -> "dict[str, object]":
    """A reader that consults the raw persisted deficit directly instead
    of going through the leg, so an uncalibrated or legacy value reaches a
    decision anyway. Used only to prove the checker trips."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current)
    )
    deficit = candidate.measurement.ultrasonic_deficit_db
    if deficit is not None and deficit >= ULTRASONIC_PROOF_DENY_DEFICIT_DB:
        result["verified_lossless"] = False
    return result


#: ROUND-3 / William Basinski's measured ``U=65.16`` FLAC-container
#: launder — a real denial from the committed calibration arms, and a
#: genuine control comfortably below the frozen threshold. Both worlds are
#: built by MOVING this one field on an otherwise identical album, so the
#: only thing the comparison can attribute a difference to is the leg.
_DENYING_DEFICIT_DB = 65.16
_PASSING_DEFICIT_DB = 45.0


def _with_adjudicable_ultrasonic(
    candidate: AlbumQualityEvidence, deficit: float,
) -> AlbumQualityEvidence:
    """The same album as measured by the PR1+ capture at ``deficit``."""
    return msgspec.structs.replace(
        candidate,
        measurement=msgspec.structs.replace(
            candidate.measurement,
            ultrasonic_deficit_db=deficit,
            spectral_measurement_version=2,
        ),
    )


#: The decision fields the provisional lane owns. Inside that lane the
#: leg is not an input at all — the lane reads the two V0 probes, the
#: spectral grade and the lossless-source fact — so a denied album and a
#: passing one must come out of it identical.
_PROVISIONAL_LANE_DECIDED_KEYS = (
    "stage2_import", "imported", "denylisted", "keep_searching",
    "final_status", "verified_lossless",
)


def _lane_membership_follows_the_proof(
    denied: "dict[str, object]",
    passing: "dict[str, object]",
    *,
    spectral_grade: "str | None",
    supported_lossless_source: bool,
    candidate_probe_comparable: bool,
    anchor_comparable: bool,
) -> bool:
    """Shared body of the amended V5/L5 checkers (issue #990).

    Two cohorts, one law each:

    * NON-AFFIRMING grades (suspect/likely_transcode/error), and every
      lossy candidate: the legs cannot move the proof except through the
      leg-blind V0-avg trust override question, so lane membership AND
      every lane-decided field are leg-invariant. This is PR3's surviving
      core — a denial must never suppress the override's rescue (the
      decoy that reached review), and inside the lane the legs have no
      voice.

    * AFFIRMING grades (genuine/marginal) on a supported lossless source
      — the #990 cohort, where the leg CAN flip the proof. Three rules,
      each stated on observables so a reported field can't lie about a
      world we cannot see:

        R1  a lane-decided variant is never proven;
        R2  when the candidate carries a comparable probe, an unproven
            IMPORT only ever comes out of the lane — the measured compare
            granting one is exactly the equal-copy churn request 2066
            shipped. A probe-less, unanchored, unaccused candidate
            legitimately continues to the measured policy: on the
            production path the preview grinds the probe into evidence
            before the importer decides, so that fall-through is an
            abnormal-evidence seam;
        R3  when both variants are lane-decided, the legs moved nothing
            inside it;
        R4  a denial is still not a rejection: with a comparable candidate
            probe and NO comparable anchor, the proof-denied, still
            unproven variant's every reached Stage-2 outcome must be the
            provisional IMPORT. This is a routing law, not merely a
            lane-internal one: a measured reject that bypasses the lane
            would discard an album whose only fault was a withheld proof
            and denylist its peer, the exact shape PR3's law was written
            to forbid (#990 review finding 1).

      Decision record:
      https://github.com/abl030/cratedigger/issues/990#issuecomment-5158156922
    """
    denied_in_lane = denied["stage2_import"] in PROVISIONAL_LANE_DECISIONS
    passing_in_lane = passing["stage2_import"] in PROVISIONAL_LANE_DECISIONS
    if supported_lossless_source and spectral_grade in ("genuine", "marginal"):
        if denied["stage2_import"] is None or passing["stage2_import"] is None:
            # A pre-stage-2 exit (preimport fact reject, Stage-1 short
            # circuit) is leg-independent: both variants must have taken
            # it, and the lane never saw the album.
            return denied["stage2_import"] is None and (
                passing["stage2_import"] is None
            )
        for variant in (denied, passing):
            in_lane = variant["stage2_import"] in PROVISIONAL_LANE_DECISIONS
            if in_lane and variant["verified_lossless"]:
                return False  # R1
            if (
                candidate_probe_comparable
                and variant["imported"]
                and not variant["verified_lossless"]
                and not in_lane
            ):
                return False  # R2
        if (
            candidate_probe_comparable
            and not anchor_comparable
            and not denied["verified_lossless"]
            and (
                denied["stage2_import"] != "provisional_lossless_upgrade"
                or not denied["imported"]
            )
        ):
            return False  # R4
        if denied_in_lane and passing_in_lane:
            return all(  # R3
                denied[key] == passing[key]
                for key in _PROVISIONAL_LANE_DECIDED_KEYS
            )
        return True
    if denied_in_lane != passing_in_lane:
        return False
    if not denied_in_lane:
        return True
    return all(
        denied[key] == passing[key]
        for key in _PROVISIONAL_LANE_DECIDED_KEYS
    )


def post_conversion_minimum_never_invents_comparable_v0_evidence(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None",
    *,
    facts: AlbumQualityEvidenceDecisionFacts,
    post_conversion_mins: tuple[int, ...],
    decider: "Callable[..., dict[str, object]]" = (
        full_pipeline_decision_from_evidence
    ),
) -> bool:
    """Target minima cannot invent or complete source V0 routing evidence.

    This covers absent, partial, and non-source V0 metrics. A target
    projection may affect the post-import quality measurement, but must never
    change Stage 1, Stage 2, its deferred counterfactual, or the source-proof
    bit by filling a candidate source probe field.
    """
    if not _lossless_source_from_evidence(candidate):
        return True
    routing = {
        (
            result["stage1_spectral"],
            result["stage2_import"],
            result["stage2_import_if_stage1_deferred"],
            result["verified_lossless"],
        )
        for result in (
            decider(
                candidate,
                current,
                facts=msgspec.structs.replace(
                    facts,
                    post_conversion_min_bitrate=post_conversion_min,
                ),
            )
            for post_conversion_min in post_conversion_mins
        )
    }
    return len(routing) == 1


@dataclass(frozen=True)
class FlatV0KindProbe:
    """One direct flat-simulator source-V0 boundary case."""

    name: str
    kind: str | None
    avg_bitrate_kbps: int | None
    min_bitrate_kbps: int | None


_FLAT_V0_KIND_PROBES = (
    FlatV0KindProbe("omitted kind, full metrics", None, 241, 219),
    FlatV0KindProbe(
        "non-source kind, full metrics", "native_lossy_research_v0", 241, 219,
    ),
    FlatV0KindProbe("source kind, full metrics", "lossless_source_v0", 241, 219),
    FlatV0KindProbe("source kind, average only", "lossless_source_v0", 241, None),
    FlatV0KindProbe("source kind, minimum only", "lossless_source_v0", None, 219),
)


def _flat_v0_kind_stage1_decision(
    kind: str | None,
    avg_bitrate_kbps: int | None,
    min_bitrate_kbps: int | None,
) -> dict[str, object]:
    """Drive the flat Stage-1 carve-out with a real comparable pair."""
    return full_pipeline_decision(
        is_flac=False,
        supported_lossless_source=True,
        min_bitrate=128,
        avg_bitrate=128,
        is_cbr=True,
        new_format="MP3",
        spectral_grade="likely_transcode",
        spectral_bitrate=128,
        existing_min_bitrate=192,
        existing_avg_bitrate=192,
        existing_format="MP3",
        existing_is_cbr=True,
        existing_spectral_grade="likely_transcode",
        existing_spectral_bitrate=192,
        candidate_v0_probe_kind=kind,
        candidate_v0_probe_avg=avg_bitrate_kbps,
        candidate_v0_probe_min=min_bitrate_kbps,
    )


def _flat_v0_kind_source_decision(
    kind: str | None,
    avg_bitrate_kbps: int | None,
    min_bitrate_kbps: int | None,
    post_conversion_min_bitrate: int,
) -> dict[str, object]:
    """Drive the real lossless-source route without constructing evidence."""
    return full_pipeline_decision(
        is_flac=True,
        min_bitrate=354,
        is_cbr=False,
        spectral_grade="suspect",
        converted_count=12,
        post_conversion_min_bitrate=post_conversion_min_bitrate,
        post_conversion_is_cbr=False,
        verified_lossless_target="opus 128",
        candidate_v0_probe_kind=kind,
        candidate_v0_probe_avg=avg_bitrate_kbps,
        candidate_v0_probe_min=min_bitrate_kbps,
    )


FlatV0KindDecider = Callable[
    [str | None, int | None, int | None, int], dict[str, object],
]


def flat_v0_kind_boundary_holds(
    probe: FlatV0KindProbe,
    *,
    post_conversion_mins: tuple[int, ...],
    decider: FlatV0KindDecider = _flat_v0_kind_source_decision,
) -> bool:
    """Only explicit source-kind V0 facts may route the flat simulator."""
    source_comparable = (
        probe.kind == "lossless_source_v0"
        and probe.avg_bitrate_kbps is not None
    )
    stage1 = _flat_v0_kind_stage1_decision(
        probe.kind,
        probe.avg_bitrate_kbps,
        probe.min_bitrate_kbps,
    )
    if stage1["stage1_spectral"] != "reject":
        return False
    if source_comparable:
        if (
            stage1["stage2_import"] != "downgrade"
            or stage1["stage2_import_if_stage1_deferred"] is not None
        ):
            return False
    elif (
        stage1["stage2_import"] is not None
        or stage1["stage2_import_if_stage1_deferred"] != "downgrade"
    ):
        return False

    source_routing = {
        (
            result["stage1_spectral"],
            result["stage2_import"],
            result["stage2_import_if_stage1_deferred"],
            result["verified_lossless"],
            result["imported"],
        )
        for result in (
            decider(
                probe.kind,
                probe.avg_bitrate_kbps,
                probe.min_bitrate_kbps,
                post_conversion_min,
            )
            for post_conversion_min in post_conversion_mins
        )
    }
    if len(source_routing) != 1:
        return False
    ((_, stage2, _, verified, imported),) = source_routing
    if not source_comparable:
        return (
            stage2 == "suspect_lossless_probe_missing"
            and verified is False
            and imported is False
        )
    if probe.min_bitrate_kbps is None:
        return (
            stage2 == "provisional_lossless_upgrade"
            and verified is False
            and imported is True
        )
    return stage2 == "import" and verified is True and imported is True


def _decoy_flat_v0_kind_defaults_to_source(
    kind: str | None,
    avg_bitrate_kbps: int | None,
    min_bitrate_kbps: int | None,
    post_conversion_min_bitrate: int,
) -> dict[str, object]:
    """Known-bad flat boundary: an omitted kind regains source authority."""
    return _flat_v0_kind_source_decision(
        kind or "lossless_source_v0",
        avg_bitrate_kbps,
        min_bitrate_kbps,
        post_conversion_min_bitrate,
    )


def _decoy_decider_invents_v0_evidence_from_post_conversion_minimum(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
) -> "dict[str, object]":
    """Known-bad #993 shape: target output becomes a source V0 probe."""
    if facts is None or facts.post_conversion_min_bitrate is None:
        return full_pipeline_decision_from_evidence(candidate, current, facts=facts)
    synthetic_average = facts.post_conversion_min_bitrate
    fabricated_candidate = msgspec.structs.replace(
        candidate,
        v0_metric=AlbumQualityV0Metric(
            subject=EVIDENCE_SUBJECT_SOURCE,
            min_bitrate_kbps=synthetic_average,
            avg_bitrate_kbps=synthetic_average,
            median_bitrate_kbps=synthetic_average,
        ),
    )
    return full_pipeline_decision_from_evidence(
        fabricated_candidate,
        current,
        facts=facts,
    )


def a_denial_never_reroutes_the_provisional_lane(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None",
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
    decider: "Callable[..., dict[str, object]]" = (
        full_pipeline_decision_from_evidence
    ),
) -> bool:
    """Invariant checker V5 (amended, issue #990): the leg reaches the
    lane only THROUGH the proof — never beside it.

    PR3's original blanket law ("the leg decides the PROOF, never the
    LANE") was disproved by request 2066: if proof-absence cannot select
    the anchor lane, the unproven-lossless cohort has no defense against
    equal-copy churn, because the measured compare ranks the incoming
    side by its target contract and the installed side by its measured
    average. What survives, verbatim, is the core the decoy self-test
    plants: a candidate the V0-avg trust override certifies is NEVER
    rerouted by a denial, and inside the lane the legs move nothing —
    the lane reads the probes and the lossless-source fact alone.
    ``_lane_membership_follows_the_proof`` states both cohorts' laws.

    ``decider`` is injectable ONLY so the known-bad self-tests can plant
    the shape that reached review (denial suppresses the override) and
    the shape that shipped (unproven album skips the anchor); production
    always uses the default.
    """
    if candidate.measurement.spectral_grade is None:
        # Evidence-row validation forbids proof-leg facts without a grade,
        # so this world has no producible denying twin.
        return True
    if current is not None and current.verified_lossless_proof is not None:
        # A proof-bearing HAVE locks every candidate out before stage 2;
        # the legs are irrelevant to that ceiling.
        return True
    denied_candidate = _with_adjudicable_ultrasonic(
        candidate, _DENYING_DEFICIT_DB,
    )
    if _leg_is_withheld_by_oracle(denied_candidate):
        return True
    denied = decider(denied_candidate, current, facts=facts)
    passing = decider(
        _with_adjudicable_ultrasonic(candidate, _PASSING_DEFICIT_DB),
        current, facts=facts,
    )
    return _lane_membership_follows_the_proof(
        denied, passing,
        spectral_grade=candidate.measurement.spectral_grade,
        supported_lossless_source=_lossless_source_from_evidence(candidate),
        candidate_probe_comparable=is_comparable_lossless_source_probe(
            _policy_v0_probe_from_metric(candidate.v0_metric)
        ),
        anchor_comparable=(
            current is not None
            and is_comparable_lossless_source_probe(
                _policy_v0_probe_from_metric(current.v0_metric)
            )
        ),
    )


def _decoy_decider_lets_a_denial_reject_the_album(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
) -> "dict[str, object]":
    """The shape that reached review: extend the denial past the proof
    into the V0 trust override's downstream routing, so a denied album
    falls back into the provisional-lossless lane. Against a provisional
    installed album that lane answers ``suspect_lossless_downgrade`` —
    a CONFIDENT reject — turning a withheld proof into a rejection plus a
    peer denylist. Used only to prove the checker trips."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current, facts=facts)
    )
    measurement = candidate.measurement
    leg = album_ultrasonic_proof_leg(
        ultrasonic_deficit_db=measurement.ultrasonic_deficit_db,
        spectral_measurement_version=measurement.spectral_measurement_version,
        spectral_subject=measurement.spectral_subject,
        was_converted_from=measurement.was_converted_from,
        container_labels=[file.extension for file in candidate.files],
    )
    if not leg.denies_promotion:
        return result
    provisional = provisional_lossless_decision(
        ProvisionalLosslessDecisionInput(
            candidate_probe=_policy_v0_probe_from_metric(candidate.v0_metric),
            existing_probe=(
                _policy_v0_probe_from_metric(current.v0_metric)
                if current is not None else None
            ),
            will_be_verified=False,
            spectral_grade=candidate.measurement.spectral_grade,
            supported_lossless_source=_lossless_source_from_evidence(candidate),
        ),
    )
    if provisional.decision is not None and provisional.confident_reject:
        result["stage2_import"] = provisional.decision
        result["imported"] = False
        result["denylisted"] = True
        result["final_status"] = "wanted"
        result["keep_searching"] = True
    return result


def classifier_names_the_model_that_proved_it(
    leg: "UltrasonicProofLeg | None",
    *,
    minter: "Callable[..., VerifiedLosslessProof | None]" = (
        mint_verified_lossless_proof
    ),
) -> bool:
    """Invariant checker V3: the v3 classifier is minted exactly when the
    ultrasonic leg adjudicated and passed.

    ``verified_lossless_classifier`` is the "which model proved it" axis.
    Stamping v3 on a row whose leg withheld would make the column mean two
    things again — the ambiguity PR3 exists to remove, and the reason
    ``spectral_measurement_version`` is NOT used for this (47 live proofs
    carry version 2 while having been proved under the old gate).
    """
    proof = minter(
        True,
        was_converted_from="flac",
        detected_source_format="flac",
        spectral_grade="genuine",
        ultrasonic_leg=leg,
    )
    if proof is None:
        return False
    expected_v3 = leg is not None and leg.outcome == "passed"
    return proof.classifier == (
        VERIFIED_LOSSLESS_CLASSIFIER_V3
        if expected_v3
        else VERIFIED_LOSSLESS_CLASSIFIER
    )


def _decoy_minter_stamps_v3_whenever_v3_code_ran(
    will_be_verified_lossless: bool,
    *,
    was_converted_from: "str | None",
    detected_source_format: "str | None",
    spectral_grade: "str | None",
    ultrasonic_leg: "UltrasonicProofLeg | None" = None,
) -> "VerifiedLosslessProof | None":
    """The plausible mistake: v3 is the shipped gate, so stamp v3. It
    mislabels every row the leg could not evaluate. Used only to prove the
    checker trips."""
    proof = mint_verified_lossless_proof(
        will_be_verified_lossless,
        was_converted_from=was_converted_from,
        detected_source_format=detected_source_format,
        spectral_grade=spectral_grade,
    )
    if proof is None:
        return None
    return msgspec.structs.replace(
        proof, classifier=VERIFIED_LOSSLESS_CLASSIFIER_V3,
    )


@st.composite
def _ultrasonic_legs(draw) -> UltrasonicProofLeg:
    """Every leg the production evaluator can emit, built by calling it —
    never hand-constructed, so an outcome no producer can reach cannot
    sneak into a property (test-fidelity.md Rule C)."""
    return ultrasonic_proof_leg(
        deficit_db=draw(st.one_of(
            st.none(),
            st.floats(min_value=0.0, max_value=140.0,
                      allow_nan=False, allow_infinity=False),
        )),
        spectral_measurement_version=draw(
            st.sampled_from((None, 1, 2, 3))
        ),
        decode_path=draw(
            st.sampled_from((None, "sox_native", "ffmpeg_resampled"))
        ),
        preserved_source_spectral=draw(st.booleans()),
    )


class TestUltrasonicProofLegProperties(unittest.TestCase):
    """Generated half of the v3 proof-gate invariant pairs."""

    @given(
        leg=_ultrasonic_legs(),
        spectral_grade=st.sampled_from(_GRADES),
        target_format=st.sampled_from(_TARGET_FORMATS),
        converted_count=st.integers(min_value=0, max_value=24),
        is_transcode=st.booleans(),
        probe_kind=st.one_of(
            st.none(),
            st.sampled_from(("lossless_source_v0", "native_lossy_research_v0")),
        ),
        v0_avg=_optional_bitrates(max_value=400),
        v0_min=_optional_bitrates(max_value=400),
    )
    @example(
        # The Bill Hicks shape against a launder deficit: the world where
        # the V0 override and the leg actively disagree.
        leg=ultrasonic_proof_leg(
            deficit_db=65.16, spectral_measurement_version=2,
            decode_path="sox_native", preserved_source_spectral=False,
        ),
        spectral_grade="suspect", target_format=None, converted_count=10,
        is_transcode=True, probe_kind="lossless_source_v0",
        v0_avg=241, v0_min=219,
    )
    def test_v1_a_denying_leg_is_a_hard_veto(
        self, leg, spectral_grade, target_format, converted_count,
        is_transcode, probe_kind, v0_avg, v0_min,
    ):
        from lib.quality import V0ProbeEvidence
        probe = (
            V0ProbeEvidence(
                kind=probe_kind, avg_bitrate_kbps=v0_avg,
                min_bitrate_kbps=v0_min,
            )
            if probe_kind is not None else None
        )
        self.assertTrue(
            denying_leg_is_a_hard_veto(
                spectral_grade=spectral_grade,
                target_format=target_format,
                converted_count=converted_count,
                is_transcode=is_transcode,
                v0_probe=probe,
                leg=leg,
            )
        )

    @given(
        leg=_ultrasonic_legs(),
        spectral_grade=st.sampled_from(_GRADES),
        target_format=st.sampled_from(_TARGET_FORMATS),
        converted_count=st.integers(min_value=0, max_value=24),
        is_transcode=st.booleans(),
        probe_kind=st.one_of(
            st.none(),
            st.sampled_from(("lossless_source_v0", "native_lossy_research_v0")),
        ),
        v0_avg=_optional_bitrates(max_value=400),
        v0_min=_optional_bitrates(max_value=400),
    )
    @example(
        # The un-backfillable shape: a genuine lossless source whose
        # ultrasonic statistic can never exist.
        leg=ultrasonic_proof_leg(
            deficit_db=None, spectral_measurement_version=None,
            decode_path="sox_native", preserved_source_spectral=True,
        ),
        spectral_grade="genuine", target_format=None, converted_count=12,
        is_transcode=False, probe_kind=None, v0_avg=None, v0_min=None,
    )
    @example(
        # A passing leg on a world the pre-v3 rules refuse.
        leg=ultrasonic_proof_leg(
            deficit_db=20.0, spectral_measurement_version=2,
            decode_path="sox_native", preserved_source_spectral=False,
        ),
        spectral_grade="error", target_format="flac", converted_count=0,
        is_transcode=False, probe_kind=None, v0_avg=None, v0_min=None,
    )
    def test_v2_the_leg_only_ever_subtracts(
        self, leg, spectral_grade, target_format, converted_count,
        is_transcode, probe_kind, v0_avg, v0_min,
    ):
        probe = (
            V0ProbeEvidence(
                kind=probe_kind, avg_bitrate_kbps=v0_avg,
                min_bitrate_kbps=v0_min,
            )
            if probe_kind is not None else None
        )
        self.assertTrue(
            the_leg_only_ever_subtracts(
                spectral_grade=spectral_grade,
                target_format=target_format,
                converted_count=converted_count,
                is_transcode=is_transcode,
                v0_probe=probe,
                leg=leg,
            )
        )

    @given(
        candidate=wild_ready_candidate_evidence(),
        current=st.one_of(st.none(), wild_ready_candidate_evidence()),
    )
    def test_v2b_a_withheld_leg_leaves_the_whole_decision_untouched(
        self, candidate, current,
    ):
        self.assertTrue(
            withheld_leg_leaves_the_decision_untouched(candidate, current)
        )

    @given(leg=st.one_of(st.none(), _ultrasonic_legs()))
    def test_v3_the_classifier_names_the_model_that_proved_it(self, leg):
        self.assertTrue(classifier_names_the_model_that_proved_it(leg))

    @given(world=parity_worlds())
    @example(world=_DENIAL_PROVISIONAL_COHORT_WORLD)
    @example(world=_REQUEST_2066_WORLD)
    @example(world=_UNANCHORED_PROVISIONAL_IMPORT_WORLD)
    def test_v5_a_denial_never_reroutes_the_provisional_lane(self, world):
        candidate, current, facts = _parity_evidence_inputs(world)
        self.assertTrue(
            a_denial_never_reroutes_the_provisional_lane(
                candidate, current, facts=facts,
            )
        )

    @given(
        post_conversion_mins=st.lists(
            st.integers(min_value=1, max_value=400), min_size=2, max_size=6,
        ),
        probe_shape=st.sampled_from((
            "absent",
            "source_avg_only",
            "source_min_only",
            "non_source_avg_only",
            "non_source_full",
        )),
    )
    @example(post_conversion_mins=[199, 200], probe_shape="source_avg_only")
    @example(post_conversion_mins=[199, 200], probe_shape="source_min_only")
    @example(post_conversion_mins=[199, 200], probe_shape="non_source_full")
    def test_v5_post_conversion_minimum_never_invents_v0_evidence(
        self, post_conversion_mins, probe_shape,
    ):
        candidate, current, facts = _parity_evidence_inputs(
            _REQUEST_2066_WORLD,
        )
        # The unanchored form makes a partial source avg's false V0 override
        # visible as a real measured-route escape, rather than allowing a
        # current-side comparison to mask the routing difference.
        current = None
        metrics = {
            "absent": None,
            "source_avg_only": AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_SOURCE,
                avg_bitrate_kbps=241,
                min_bitrate_kbps=None,
            ),
            "source_min_only": AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_SOURCE,
                avg_bitrate_kbps=None,
                min_bitrate_kbps=241,
            ),
            "non_source_avg_only": AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_INSTALLED,
                avg_bitrate_kbps=241,
                min_bitrate_kbps=None,
            ),
            "non_source_full": AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_INSTALLED,
                avg_bitrate_kbps=241,
                min_bitrate_kbps=241,
            ),
        }
        candidate = msgspec.structs.replace(
            candidate,
            v0_metric=metrics[probe_shape],
        )
        candidate = _with_adjudicable_ultrasonic(
            candidate, _DENYING_DEFICIT_DB,
        )
        self.assertTrue(
            post_conversion_minimum_never_invents_comparable_v0_evidence(
                candidate,
                current,
                facts=facts,
                post_conversion_mins=tuple(post_conversion_mins),
            )
        )

    @given(world=parity_worlds())
    @example(world=_REQUEST_2066_WORLD)
    @example(world=_REQUEST_2066_MARKED_INCOMPLETE_WORLD)
    def test_v6_an_unproven_source_never_outranks_its_anchor(self, world):
        candidate, current, facts = _parity_evidence_inputs(world)
        self.assertTrue(
            an_unproven_lossless_source_never_outranks_its_anchor(
                candidate, current, facts=facts,
            )
        )


class TestFlatV0KindBoundary(unittest.TestCase):
    """Direct pins and Hypothesis patrol for #993's flat kind boundary."""

    def test_explicit_source_kind_owns_flat_v0_routing(self):
        for probe in _FLAT_V0_KIND_PROBES:
            with self.subTest(probe=probe.name):
                self.assertTrue(
                    flat_v0_kind_boundary_holds(
                        probe,
                        post_conversion_mins=(199, 200),
                    )
                )

    @given(
        probe=st.sampled_from(_FLAT_V0_KIND_PROBES),
        post_conversion_mins=st.lists(
            st.integers(min_value=1, max_value=400), min_size=2, max_size=6,
        ),
    )
    @example(
        probe=_FLAT_V0_KIND_PROBES[0], post_conversion_mins=[199, 200],
    )
    @example(
        probe=_FLAT_V0_KIND_PROBES[3], post_conversion_mins=[199, 200],
    )
    @example(
        probe=_FLAT_V0_KIND_PROBES[4], post_conversion_mins=[199, 200],
    )
    def test_generated_flat_v0_kind_boundary(
        self, probe, post_conversion_mins,
    ):
        self.assertTrue(
            flat_v0_kind_boundary_holds(
                probe,
                post_conversion_mins=tuple(post_conversion_mins),
            )
        )


class TestFlatV0KindBoundarySelfTests(unittest.TestCase):
    """Known-bad qualification for the direct flat simulator patrol."""

    def test_checker_trips_when_omitted_kind_defaults_to_source(self):
        self.assertFalse(
            flat_v0_kind_boundary_holds(
                _FLAT_V0_KIND_PROBES[0],
                post_conversion_mins=(199, 200),
                decider=_decoy_flat_v0_kind_defaults_to_source,
            )
        )


class TestUltrasonicProofLegCheckerSelfTests(unittest.TestCase):
    """Known-bad self-tests for the v3 proof-gate invariant checkers.

    Each checker gets both halves: it passes for the real decider/minter,
    and it TRIPS on a planted reader that violates the invariant. A checker
    that has never failed anything is unfalsifiable until proven otherwise.
    """

    _DENYING_LEG = ultrasonic_proof_leg(
        deficit_db=65.16, spectral_measurement_version=2,
        decode_path="sox_native", preserved_source_spectral=False,
    )

    def test_v1_checker_passes_for_the_real_decider(self):
        from lib.quality import V0ProbeEvidence
        self.assertTrue(
            denying_leg_is_a_hard_veto(
                spectral_grade="suspect", target_format=None,
                converted_count=10, is_transcode=True,
                v0_probe=V0ProbeEvidence(
                    kind="lossless_source_v0",
                    avg_bitrate_kbps=241, min_bitrate_kbps=219,
                ),
                leg=self._DENYING_LEG,
            )
        )

    def test_v1_checker_trips_when_the_v0_override_outranks_the_leg(self):
        from lib.quality import V0ProbeEvidence
        self.assertFalse(
            denying_leg_is_a_hard_veto(
                spectral_grade="suspect", target_format=None,
                converted_count=10, is_transcode=True,
                v0_probe=V0ProbeEvidence(
                    kind="lossless_source_v0",
                    avg_bitrate_kbps=241, min_bitrate_kbps=219,
                ),
                leg=self._DENYING_LEG,
                decider=_decoy_decider_checks_the_leg_after_the_v0_override,
            )
        )

    def _legacy_lossless_candidate(self) -> AlbumQualityEvidence:
        """The un-backfillable shape: a lossless source with no ultrasonic
        evidence, which is where most of the proof cohort lives."""
        return build_parity_candidate_evidence(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine",
            candidate_v0_probe_avg=245,
            candidate_v0_probe_min=245,
            codec_family="lossless",
            ultrasonic_deficit_db=None,
            spectral_measurement_version=None,
        )

    _WITHHELD_LEG = ultrasonic_proof_leg(
        deficit_db=None, spectral_measurement_version=None,
        decode_path="sox_native", preserved_source_spectral=True,
    )
    _PASSING_LEG = ultrasonic_proof_leg(
        deficit_db=20.0, spectral_measurement_version=2,
        decode_path="sox_native", preserved_source_spectral=False,
    )

    def _subtracts(self, leg, **overrides):
        world = {
            "spectral_grade": "genuine", "target_format": None,
            "converted_count": 12, "is_transcode": False, "v0_probe": None,
        }
        world.update(overrides)
        return the_leg_only_ever_subtracts(leg=leg, **world)

    def test_v2_checker_passes_for_the_real_decider(self):
        for leg in (self._WITHHELD_LEG, self._PASSING_LEG, self._DENYING_LEG):
            with self.subTest(outcome=leg.outcome):
                self.assertTrue(self._subtracts(leg))

    def test_v2_checker_trips_when_withheld_is_read_as_denying(self):
        self.assertFalse(
            self._subtracts(
                self._WITHHELD_LEG,
                decider=_decoy_decider_treats_a_withheld_leg_as_denying,
            )
        )

    def test_v2_checker_trips_when_a_pass_is_read_as_affirmative(self):
        self.assertFalse(
            self._subtracts(
                self._PASSING_LEG, spectral_grade="error",
                target_format="flac", converted_count=0,
                decider=_decoy_decider_lets_a_pass_grant_proof,
            )
        )

    def test_v2b_checker_passes_for_the_real_decider(self):
        candidate = self._legacy_lossless_candidate()
        self.assertTrue(_leg_is_withheld_by_oracle(candidate))
        self.assertTrue(
            withheld_leg_leaves_the_decision_untouched(candidate, None)
        )

    def test_v2b_checker_trips_on_a_raw_deficit_reader(self):
        """A row whose deficit is above the threshold but whose leg
        withholds — a legacy version-1 measurement. The leg says nothing;
        a raw reader demotes it anyway."""
        candidate = build_parity_candidate_evidence(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine",
            candidate_v0_probe_avg=245, candidate_v0_probe_min=245,
            codec_family="lossless",
            ultrasonic_deficit_db=65.16,
            spectral_measurement_version=1,
        )
        self.assertTrue(_leg_is_withheld_by_oracle(candidate))
        self.assertFalse(
            withheld_leg_leaves_the_decision_untouched(
                candidate, None,
                decider=_decoy_decider_reads_the_raw_deficit_outside_the_leg,
            )
        )

    def test_v3_checker_passes_for_the_real_minter(self):
        self.assertTrue(
            classifier_names_the_model_that_proved_it(self._DENYING_LEG)
        )
        self.assertTrue(classifier_names_the_model_that_proved_it(None))

    def test_v3_checker_trips_when_v3_is_stamped_unconditionally(self):
        withheld = ultrasonic_proof_leg(
            deficit_db=None, spectral_measurement_version=None,
            decode_path="sox_native", preserved_source_spectral=True,
        )
        self.assertEqual(withheld.outcome, "withheld")
        self.assertFalse(
            classifier_names_the_model_that_proved_it(
                withheld,
                minter=_decoy_minter_stamps_v3_whenever_v3_code_ran,
            )
        )


    def test_v5_checker_passes_for_the_real_decider(self):
        candidate, current, facts = _parity_evidence_inputs(
            _DENIAL_PROVISIONAL_COHORT_WORLD,
        )
        # The world is only evidence of anything if its leg adjudicates.
        self.assertFalse(
            _leg_is_withheld_by_oracle(
                _with_adjudicable_ultrasonic(candidate, _DENYING_DEFICIT_DB),
            )
        )
        self.assertTrue(
            a_denial_never_reroutes_the_provisional_lane(
                candidate, current, facts=facts,
            )
        )

    def test_v5_checker_trips_when_a_denial_reroutes_the_lane(self):
        """The review world: an installed provisional album whose own
        ``lossless_source_v0`` probe (avg 240) sits a hair under the
        candidate's (avg 241). A decider that lets the denial suppress the
        V0 trust override drops that album into the provisional lane,
        where that pair is a confident reject — the album is discarded and
        the peer denylisted."""
        candidate, current, facts = _parity_evidence_inputs(
            _DENIAL_PROVISIONAL_COHORT_WORLD,
        )
        self.assertFalse(
            a_denial_never_reroutes_the_provisional_lane(
                candidate, current, facts=facts,
                decider=_decoy_decider_lets_a_denial_reject_the_album,
            )
        )

    def test_v5_checker_trips_on_the_shipped_grade_keyed_router(self):
        """R2's known-bad: the pre-#990 production, where a genuine-graded
        proof-denied FLAC skipped the anchor lane and the measured ranks
        imported the equal copy — an unproven import outside the lane."""
        candidate, current, facts = _parity_evidence_inputs(
            _REQUEST_2066_WORLD,
        )
        self.assertFalse(
            a_denial_never_reroutes_the_provisional_lane(
                candidate, current, facts=facts,
                decider=_decoy_decider_lets_an_unproven_album_skip_the_anchor,
            )
        )

    def test_v5_checker_trips_when_an_unanchored_denial_is_rejected(self):
        """R4's known-bad: the review's planted mutant — a denied,
        anchor-less album confidently rejected instead of imported
        provisionally. The amended checker must call it out."""
        candidate, current, facts = _parity_evidence_inputs(
            _REQUEST_2066_WORLD,
        )
        assert current is not None
        unanchored_current = msgspec.structs.replace(current, v0_metric=None)
        self.assertFalse(
            a_denial_never_reroutes_the_provisional_lane(
                candidate, unanchored_current, facts=facts,
                decider=_decoy_decider_lets_a_denial_reject_an_unanchored_album,
            )
        )

    def test_v5_checker_trips_when_an_unanchored_denial_bypasses_to_measured_reject(self):
        """R4 rejects the real comparator path when a router skips its lane."""
        candidate, current, facts = _parity_evidence_inputs(
            _UNANCHORED_MEASURED_REJECT_WORLD,
        )
        bypass = _decoy_decider_bypasses_unanchored_lane_via_production_comparator(
            _with_adjudicable_ultrasonic(candidate, _DENYING_DEFICIT_DB),
            current,
            facts=facts,
        )
        self.assertEqual(bypass["stage2_import"], "downgrade")
        self.assertFalse(bypass["imported"])
        self.assertIsNotNone(bypass["comparison_basis"])
        self.assertFalse(
            a_denial_never_reroutes_the_provisional_lane(
                candidate,
                current,
                facts=facts,
                decider=(
                    _decoy_decider_bypasses_unanchored_lane_via_production_comparator
                ),
            )
        )

    def test_v5_probe_checker_trips_when_target_minimum_fabricates_v0_evidence(self):
        """The outer evidence decider must reject a fabricated source probe."""
        candidate, current, facts = _parity_evidence_inputs(
            replace(_REQUEST_2066_WORLD, v0_avg=None, v0_min=None),
        )
        candidate = _with_adjudicable_ultrasonic(
            candidate, _DENYING_DEFICIT_DB,
        )
        self.assertFalse(
            post_conversion_minimum_never_invents_comparable_v0_evidence(
                candidate,
                current,
                facts=facts,
                post_conversion_mins=(93, 128, 320),
                decider=(
                    _decoy_decider_invents_v0_evidence_from_post_conversion_minimum
                ),
            )
        )

    def test_v6_checker_passes_for_the_real_decider(self):
        candidate, current, facts = _parity_evidence_inputs(
            _REQUEST_2066_WORLD,
        )
        self.assertTrue(
            an_unproven_lossless_source_never_outranks_its_anchor(
                candidate, current, facts=facts,
            )
        )

    def test_v6_checker_trips_on_the_shipped_grade_keyed_router(self):
        """The 2066 world under the decider that shipped it: probe 175 vs
        anchor 177 imports anyway. The checker must call that out."""
        candidate, current, facts = _parity_evidence_inputs(
            _REQUEST_2066_WORLD,
        )
        self.assertFalse(
            an_unproven_lossless_source_never_outranks_its_anchor(
                candidate, current, facts=facts,
                decider=_decoy_decider_lets_an_unproven_album_skip_the_anchor,
            )
        )

    def test_v6_out_of_scope_when_installed_incomplete_disregarded(self):
        """Issue #1241's predicate is out of V6's scope, Q1 half: when both
        conjuncts hold, production nulls the anchor probe and admits the
        candidate fresh, so there is no anchor left for V6 to police. Drive
        the exact denial the checker itself would construct, straight
        through the real decider, and confirm the decided outcome is NOT
        the anchor law's confident reject — proving the scope exit is
        load-bearing, not vacuous — while the checker still reports True."""
        candidate, current, facts = _parity_evidence_inputs(
            _REQUEST_2066_MARKED_INCOMPLETE_WORLD,
        )
        denied_candidate = _with_adjudicable_ultrasonic(
            candidate, _DENYING_DEFICIT_DB,
        )
        self.assertFalse(_leg_is_withheld_by_oracle(denied_candidate))
        real = full_pipeline_decision_from_evidence(
            denied_candidate, current, facts=facts,
        )
        self.assertTrue(real["installed_incomplete_disregarded"])
        self.assertIsNone(real["comparison_basis"])
        self.assertEqual(real["stage2_import"], "provisional_lossless_upgrade")
        self.assertTrue(real["imported"])
        self.assertTrue(
            an_unproven_lossless_source_never_outranks_its_anchor(
                candidate, current, facts=facts,
            )
        )

    def test_v6_stays_in_scope_when_only_one_1241_conjunct_holds(self):
        """Issue #1241's predicate is out of V6's scope, Q2 half: neither
        conjunct alone disarms the anchor law (mirrors the disregard
        property's own four-corners pin at
        ``test_operator_incomplete_mark_disregards_the_installed_side``).
        A marked-but-not-covered or covered-but-not-marked world must
        still decide the ordinary anchor law and the checker must still
        catch the shipped grade-keyed-router bug on both."""
        for label, world in (
            ("marked_only", replace(
                _REQUEST_2066_WORLD, installed_marked_incomplete=True)),
            ("covered_only", replace(
                _REQUEST_2066_WORLD, candidate_covers_declared_program=True)),
        ):
            with self.subTest(world=label):
                candidate, current, facts = _parity_evidence_inputs(world)
                denied_candidate = _with_adjudicable_ultrasonic(
                    candidate, _DENYING_DEFICIT_DB,
                )
                real = full_pipeline_decision_from_evidence(
                    denied_candidate, current, facts=facts,
                )
                self.assertFalse(real["installed_incomplete_disregarded"])
                self.assertEqual(
                    real["stage2_import"], "suspect_lossless_downgrade",
                )
                self.assertTrue(
                    an_unproven_lossless_source_never_outranks_its_anchor(
                        candidate, current, facts=facts,
                    )
                )
                self.assertFalse(
                    an_unproven_lossless_source_never_outranks_its_anchor(
                        candidate, current, facts=facts,
                        decider=(
                            _decoy_decider_lets_an_unproven_album_skip_the_anchor
                        ),
                    )
                )

# ===========================================================================
# Proof gate v4 — the AAC frame-lattice leg (issue #829 AAC-lattice leg PR-B)
#
# The v3 block above is the template, deliberately: this is the SAME kind
# of instrument (a measured statistic that can only ever withhold a proof)
# reading a different fact, so it owes the same invariants. The
# deterministic pins live in
# ``tests/test_quality_classification.py::TestAacLatticeProofGate`` and
# ``::TestVerifiedLosslessClassifierGeneration``.
#
#   L1  A denying leg is a hard veto: no verified-lossless status survives
#       it, by ANY other route, including the V0-avg trust override.
#   L2  The leg's ENTIRE effect is a veto: result(leg) == result(no leg)
#       AND NOT leg.denies. A withheld leg is completely inert — which is
#       where essentially the whole library is, because the capture is
#       gated to the promotion-plausible cohort — and a PASSING leg never
#       grants proof the pre-existing rules refused. L2b drives the same
#       law through the whole evidence decider.
#   L3  ``verified_lossless_classifier`` says v4 exactly when BOTH legs
#       adjudicated and passed, never merely because the lattice ran.
#   L5  The leg decides the PROOF, never the LANE. PR3 shipped a blocking
#       defect on exactly this boundary; the lattice leg inherits the
#       invariant from birth rather than after an incident.
# ===========================================================================


@st.composite
def _aac_lattice_captures(draw) -> AacLatticeCapture:
    """Captures built by the PRODUCTION derivation over generated tracks.

    Per-track ``(offset, z)`` rows go through
    ``AacLatticeCapture.from_tracks`` — the function
    ``lib/aac_lattice.py::measure_album_aac_lattice`` itself calls — so no
    property can assert on an album statistic no measurement could produce
    (test-fidelity.md Rule C).

    Offsets are drawn from a deliberately SMALL pool so coincidences
    actually happen: over the real 0-1023 range four tracks sharing an
    offset is a ~1e-9 event and the denial branch would never be reached.

    ``z`` is drawn mostly from the MEASURED genuine range (album maxima
    4.58-6.91 over the 17-album control arm) and only sometimes from the
    launder range. A uniform 0-40 draw puts ~70% of tracks over the
    threshold of 12, so nearly every generated album denied on
    ``z_exceeded`` and the ``offset_concentration`` and ``passed``
    branches went hungry at the suite tier's example budget.
    """
    count = draw(st.integers(min_value=0, max_value=6))
    tracks: list[tuple[int | None, float | None]] = []
    for _ in range(count):
        # Per-track detector errors are the exception in production (96 kHz
        # input, an undecodable file), not half the album. A fair coin here
        # also starved the concentration branch, which needs four SCORED
        # tracks to coincide.
        if draw(st.integers(min_value=0, max_value=4)) == 0:
            tracks.append((None, None))
            continue
        tracks.append((
            draw(st.sampled_from((0, 137, 512, 803, 960))),
            draw(st.one_of(
                st.floats(min_value=3.0, max_value=12.0,
                          allow_nan=False, allow_infinity=False),
                st.floats(min_value=0.0, max_value=40.0,
                          allow_nan=False, allow_infinity=False),
            )),
        ))
    return make_aac_lattice_capture(tracks)


@st.composite
def _aac_lattice_legs(draw) -> AacLatticeProofLeg:
    """Every leg the production evaluator can emit, built by CALLING it."""
    return aac_lattice_proof_leg(
        draw(st.one_of(st.none(), _aac_lattice_captures()))
    )


def _lattice_leg_does_not_deny_by_oracle(
    candidate: AlbumQualityEvidence,
) -> bool:
    """Independent oracle for "the lattice leg does not deny this album".

    Restates the two denial conditions from the research README rather
    than asking the production evaluator, so a mutant that widened or
    narrowed them cannot make the property vacuous.

    Deliberately "not denying", NOT "cannot adjudicate". A ``passed`` leg
    is every bit as inert as a withheld one — the leg is a denial
    instrument and a clean lattice grants nothing (``the_lattice_leg_only_
    ever_subtracts``) — so scoping the oracle to withheld worlds would
    leave L2b vacuous on exactly the population the leg newly adjudicates,
    and would leave the A-I1 retirement's equivalence claim
    (``tests/test_aac_lattice_capture_generated.py``, the section header
    above ``decision_ignores_the_installed_lattice``) only half proven.
    """
    capture = candidate.aac_lattice
    if capture is None:
        return True
    modal_count = capture.modal_count
    if (
        modal_count is not None
        and modal_count >= AAC_LATTICE_PROOF_DENY_MODAL_COUNT
    ):
        return False
    max_z = capture.max_z
    return not (max_z is not None and max_z > AAC_LATTICE_PROOF_DENY_MAX_Z)


def _without_lattice_capture(
    candidate: AlbumQualityEvidence,
) -> AlbumQualityEvidence:
    """The same album with no lattice evidence at all — the pre-PR-A world,
    which is where every row measured before the capture shipped lives."""
    return msgspec.structs.replace(candidate, aac_lattice=None)


def denying_lattice_leg_is_a_hard_veto(
    *,
    spectral_grade: "str | None",
    target_format: "str | None",
    converted_count: int,
    is_transcode: bool,
    v0_probe: "V0ProbeEvidence | None",
    leg: AacLatticeProofLeg,
    decider: "Callable[..., bool]" = determine_verified_lossless,
) -> bool:
    """Invariant checker L1: a ``denied`` leg admits no verified-lossless
    status through any other route.

    The route that matters is the V0-avg trust override. It measures how
    hard the source is to re-encode; it cannot see an MDCT frame grid at
    all, so it has nothing to say about the evidence this leg carries. If
    the override could outrank the leg, the leg would be decorative.

    ``decider`` is injectable ONLY so the known-bad self-test can plant
    the wrong ordering; production always uses the default.
    """
    if leg.outcome != "denied":
        return True
    return decider(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, aac_lattice_leg=leg,
    ) is False


def _decoy_decider_checks_the_lattice_after_the_v0_override(
    target_format: "str | None",
    spectral_grade: "str | None",
    converted_count: int,
    is_transcode: bool,
    *,
    v0_probe: "V0ProbeEvidence | None" = None,
    has_lossy_passthrough: bool = False,
    ultrasonic_leg: "UltrasonicProofLeg | None" = None,
    aac_lattice_leg: "AacLatticeProofLeg | None" = None,
) -> bool:
    """The ordering bug: consult the lattice only when nothing else already
    said yes, so a V0-rescued suspect Apple launder keeps its proof. Used
    only to prove the checker trips."""
    verified = determine_verified_lossless(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, has_lossy_passthrough=has_lossy_passthrough,
        ultrasonic_leg=ultrasonic_leg,
    )
    if not verified:
        return False
    rescued = (
        spectral_grade in ("suspect", "likely_transcode")
        and v0_probe_overrides_spectral(v0_probe)
    )
    if rescued:
        return True
    return not (
        aac_lattice_leg is not None and aac_lattice_leg.denies_promotion
    )


def the_lattice_leg_only_ever_subtracts(
    *,
    spectral_grade: "str | None",
    target_format: "str | None",
    converted_count: int,
    is_transcode: bool,
    v0_probe: "V0ProbeEvidence | None",
    leg: AacLatticeProofLeg,
    decider: "Callable[..., bool]" = determine_verified_lossless,
) -> bool:
    """Invariant checker L2: the leg's ENTIRE effect on verified-lossless
    status is a veto.

        result(leg) == result(no leg) AND NOT leg.denies_promotion

    Both halves are load-bearing:

    * A withheld leg is completely inert. The capture is gated to the
      promotion-plausible cohort and every row measured before it shipped
      has none, so a leg that could move a withheld row would silently
      change the shipped behaviour of the entire library.
    * A ``passed`` leg is inert too — a clean lattice NEVER grants proof
      the pre-existing rules would have refused. "This album is not an AAC
      transcode" is not "this album is lossless": it is one negative
      finding about one laundering family. All a pass earns is the v4
      classifier.

    ``decider`` is injectable ONLY so the known-bad self-tests can plant a
    reader that violates one half; production always uses the default.
    """
    with_leg = decider(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, aac_lattice_leg=leg,
    )
    without_leg = decider(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, aac_lattice_leg=None,
    )
    return with_leg == (without_leg and not leg.denies_promotion)


def _decoy_decider_treats_a_withheld_lattice_as_denying(
    target_format: "str | None",
    spectral_grade: "str | None",
    converted_count: int,
    is_transcode: bool,
    *,
    v0_probe: "V0ProbeEvidence | None" = None,
    has_lossy_passthrough: bool = False,
    ultrasonic_leg: "UltrasonicProofLeg | None" = None,
    aac_lattice_leg: "AacLatticeProofLeg | None" = None,
) -> bool:
    """The fail-closed-too-hard reader: "no lattice evidence" read as "not
    cleared of an AAC lattice". It demotes every row outside the cohort
    gate, i.e. almost all of them. Used only to prove the checker trips."""
    if aac_lattice_leg is not None and aac_lattice_leg.outcome != "passed":
        return False
    return determine_verified_lossless(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, has_lossy_passthrough=has_lossy_passthrough,
        ultrasonic_leg=ultrasonic_leg, aac_lattice_leg=aac_lattice_leg,
    )


def _decoy_decider_lets_a_clean_lattice_grant_proof(
    target_format: "str | None",
    spectral_grade: "str | None",
    converted_count: int,
    is_transcode: bool,
    *,
    v0_probe: "V0ProbeEvidence | None" = None,
    has_lossy_passthrough: bool = False,
    ultrasonic_leg: "UltrasonicProofLeg | None" = None,
    aac_lattice_leg: "AacLatticeProofLeg | None" = None,
) -> bool:
    """The seductive direction: a clean lattice is affirmative evidence, so
    let it certify an album the pre-existing rules refused. That would make
    an errored or ungraded album verified-lossless on one negative finding.
    Used only to prove the checker trips."""
    if aac_lattice_leg is not None and aac_lattice_leg.outcome == "passed":
        return True
    return determine_verified_lossless(
        target_format, spectral_grade, converted_count, is_transcode,
        v0_probe=v0_probe, has_lossy_passthrough=has_lossy_passthrough,
        ultrasonic_leg=ultrasonic_leg, aac_lattice_leg=aac_lattice_leg,
    )


def a_non_denying_lattice_leg_leaves_the_decision_untouched(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None",
    *,
    decider: "Callable[..., dict[str, object]]" = (
        full_pipeline_decision_from_evidence
    ),
) -> bool:
    """Invariant checker L2b, the composed half: unless the leg DENIES,
    the WHOLE decision dict is bit-identical to the same album carrying no
    lattice capture at all.

    Covers withheld AND passed, because both are inert: the leg is a
    denial instrument, and a clean lattice grants nothing the pre-existing
    rules refused. Scoping this to withheld worlds would go vacuous on
    exactly the population the leg newly adjudicates.

    L2 pins the pure decision; this one drives the real evidence decider
    end to end, so a reader that picked the raw capture up somewhere other
    than ``determine_verified_lossless`` is caught too.

    ``decider`` is injectable ONLY so the known-bad self-tests can plant
    such a reader; production always uses the default.
    """
    if not _lattice_leg_does_not_deny_by_oracle(candidate):
        return True
    return decider(candidate, current) == decider(
        _without_lattice_capture(candidate), current,
    )


def _decoy_decider_reads_the_raw_modal_count_outside_the_leg(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
) -> "dict[str, object]":
    """A reader that consults the raw persisted concentration directly
    instead of going through the leg, so a capture too thin to adjudicate
    demotes the album anyway. Used only to prove the checker trips."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current)
    )
    capture = candidate.aac_lattice
    if capture is not None and (capture.modal_count or 0) >= 2:
        result["verified_lossless"] = False
    return result


def _decoy_decider_lets_a_clean_lattice_leak_into_the_decision(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
) -> "dict[str, object]":
    """The half the narrower oracle could not see: a reader that acts on a
    leg that ADJUDICATED AND PASSED.

    Plausible rot — "the detector cleared this album, so record that" — and
    it moves rows the leg has no authority over. Only reachable by a
    checker whose oracle admits passing worlds. Used only to prove the
    checker trips."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current)
    )
    if aac_lattice_proof_leg(candidate.aac_lattice).proves_no_aac_lattice:
        result["final_status"] = "CORRUPTED_BY_A_CLEAN_LATTICE"
    return result


#: A denying capture and a passing one, built from the measured shapes:
#: ``qaac-cvbr256`` puts ~97.5% of an album's tracks on one offset (17/17
#: albums at k>=4), while the 17-album genuine control arm never reached
#: even k>=2 and topped out at album-max z 6.91. Both worlds are built by
#: swapping ONE field on an otherwise identical album, so the comparison
#: can attribute a difference only to the leg.
_DENYING_LATTICE_CAPTURE = make_aac_lattice_capture([
    (960, 28.60), (960, 29.11), (960, 30.02), (960, 28.35),
    (960, 31.13), (512, 27.44),
])
_PASSING_LATTICE_CAPTURE = make_aac_lattice_capture([
    (13, 4.58), (205, 4.80), (418, 4.97),
    (611, 5.17), (803, 5.28), (1001, 6.91),
])
#: Measured and unusable: three scored tracks and three detector errors
#: (the 96 kHz / undecodable shape). Adjudicates nothing.
_THIN_LATTICE_CAPTURE = make_aac_lattice_capture([
    (13, 4.58), (205, 4.80), (418, 4.97),
    (None, None), (None, None), (None, None),
])

#: A passing ultrasonic leg, built by calling the v3 evaluator, for the
#: classifier-composition cells that need both legs adjudicating.
_ULTRASONIC_PASSING_LEG = ultrasonic_proof_leg(
    deficit_db=45.0, spectral_measurement_version=2,
    decode_path="sox_native", preserved_source_spectral=False,
)


def _with_lattice_capture(
    candidate: AlbumQualityEvidence, capture: AacLatticeCapture,
) -> AlbumQualityEvidence:
    """The same album as measured by the PR-A capture."""
    return msgspec.structs.replace(candidate, aac_lattice=capture)


def a_lattice_denial_never_reroutes_the_provisional_lane(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None",
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
    decider: "Callable[..., dict[str, object]]" = (
        full_pipeline_decision_from_evidence
    ),
) -> bool:
    """Invariant checker L5 (amended, issue #990): the leg reaches the
    lane only THROUGH the proof — never beside it.

    The lattice twin of ``a_denial_never_reroutes_the_provisional_lane``;
    ``_lane_membership_follows_the_proof`` states both cohorts' laws and
    that docstring carries the amendment's rationale and decision record.

    ``decider`` is injectable ONLY so the known-bad self-tests can plant
    the shape PR3 shipped; production always uses the default.
    """
    if current is not None and current.verified_lossless_proof is not None:
        # A proof-bearing HAVE locks every candidate out before stage 2;
        # the legs are irrelevant to that ceiling.
        return True
    denied = decider(
        _with_lattice_capture(candidate, _DENYING_LATTICE_CAPTURE),
        current, facts=facts,
    )
    passing = decider(
        _with_lattice_capture(candidate, _PASSING_LATTICE_CAPTURE),
        current, facts=facts,
    )
    return _lane_membership_follows_the_proof(
        denied, passing,
        spectral_grade=candidate.measurement.spectral_grade,
        supported_lossless_source=_lossless_source_from_evidence(candidate),
        candidate_probe_comparable=is_comparable_lossless_source_probe(
            _policy_v0_probe_from_metric(candidate.v0_metric)
        ),
        anchor_comparable=(
            current is not None
            and is_comparable_lossless_source_probe(
                _policy_v0_probe_from_metric(current.v0_metric)
            )
        ),
    )


def an_unproven_lossless_source_never_outranks_its_anchor(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None",
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
    decider: "Callable[..., dict[str, object]]" = (
        full_pipeline_decision_from_evidence
    ),
) -> bool:
    """Invariant checker for issue #990's churn-killer, stated directly.

    An affirming-graded supported lossless-source candidate made unproven
    by a denying ultrasonic leg, facing a comparable ``lossless_source_v0``
    anchor it does not beat by more than the rank tolerance, is the
    request-2066 world: the copy on disk is already as good as anything
    this source can produce. The decided outcome must be the lane's
    confident reject — never an import. 95 live downloads bought this
    property.

    Accused grades are out of scope on purpose: their lane/override laws
    belong to V5/L5, and the V0-avg trust override may legitimately import
    over a within-tolerance anchor (PR3's core).

    Issue #1241's disregard predicate is out of scope too, for the same
    reason: when the operator marked the installed copy incomplete AND this
    attempt's candidate covers the whole declared program, production nulls
    every existing-side input — including the anchor probe this checker
    reads — before Stage 2 ever runs, so there is no anchor left to compare
    against. The decided outcome in that world is the ordinary fresh-import
    admission (``installed_incomplete_disregarded=True``,
    ``comparison_basis=None``), never this lane's confident reject.
    """
    if candidate.measurement.spectral_grade not in ("genuine", "marginal"):
        return True
    if (
        facts is not None
        and facts.installed_marked_incomplete
        and facts.candidate_covers_declared_program
    ):
        return True
    if current is None or current.verified_lossless_proof is not None:
        return True
    if not _lossless_source_from_evidence(candidate):
        return True
    candidate_probe = _policy_v0_probe_from_metric(candidate.v0_metric)
    anchor = _policy_v0_probe_from_metric(current.v0_metric)
    if not is_comparable_lossless_source_probe(candidate_probe):
        return True
    if not is_comparable_lossless_source_probe(anchor):
        return True
    assert candidate_probe is not None and anchor is not None
    assert candidate_probe.avg_bitrate_kbps is not None
    assert anchor.avg_bitrate_kbps is not None
    tolerance = QualityRankConfig.defaults().within_rank_tolerance_kbps
    if candidate_probe.avg_bitrate_kbps - anchor.avg_bitrate_kbps > tolerance:
        return True
    denied_candidate = _with_adjudicable_ultrasonic(
        candidate, _DENYING_DEFICIT_DB,
    )
    if _leg_is_withheld_by_oracle(denied_candidate):
        return True
    r = decider(denied_candidate, current, facts=facts)
    if r["stage2_import"] is None:
        # A pre-stage-2 exit (preimport fact reject, Stage-1 short
        # circuit) never reached the lane; nothing to assert.
        return True
    return (
        r["stage2_import"] == "suspect_lossless_downgrade"
        and not r["imported"]
        and bool(r["keep_searching"])
    )


def _decoy_decider_lets_a_denial_reject_an_unanchored_album(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
) -> "dict[str, object]":
    """The #990 review's planted mutant (finding 1): a denial on a world
    with NO comparable anchor turned into the lane's confident reject —
    an album whose only fault was a withheld proof discarded and its peer
    denylisted, the shape PR3's law forbids. Used only to prove R4
    trips."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current, facts=facts)
    )
    if result["stage2_import"] == "provisional_lossless_upgrade":
        result["stage2_import"] = "suspect_lossless_downgrade"
        result["imported"] = False
        result["denylisted"] = True
        result["final_status"] = "wanted"
        result["keep_searching"] = True
    return result


def _decoy_decider_bypasses_unanchored_lane_via_production_comparator(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
) -> "dict[str, object]":
    """Known-bad #993 router: suppress lane entry, then run production.

    The only planted defect is the false ``supported_lossless_source`` route
    bit. The returned downgrade comes from ``full_pipeline_decision``'s real
    target-contract comparator, not a hand-rewritten decision dictionary.
    """
    if facts is None or current is None:
        return full_pipeline_decision_from_evidence(candidate, current, facts=facts)
    measurement = candidate.measurement
    current_measurement = current.measurement
    candidate_probe = _policy_v0_probe_from_metric(candidate.v0_metric)
    current_probe = _policy_v0_probe_from_metric(current.v0_metric)
    return full_pipeline_decision(
        is_flac=True,
        min_bitrate=measurement.min_bitrate_kbps or 1,
        is_cbr=measurement.is_cbr,
        avg_bitrate=measurement.avg_bitrate_kbps,
        spectral_grade=measurement.spectral_grade,
        spectral_bitrate=measurement.spectral_bitrate_kbps,
        existing_min_bitrate=current_measurement.min_bitrate_kbps,
        existing_avg_bitrate=current_measurement.avg_bitrate_kbps,
        existing_spectral_bitrate=current_measurement.spectral_bitrate_kbps,
        existing_spectral_grade=current_measurement.spectral_grade,
        existing_format=current_measurement.format or current.storage_format,
        existing_is_cbr=current_measurement.is_cbr,
        post_conversion_min_bitrate=facts.post_conversion_min_bitrate,
        post_conversion_is_cbr=facts.post_conversion_is_cbr,
        converted_count=facts.converted_count or 0,
        candidate_verified_lossless_proof=(
            candidate.verified_lossless_proof is not None
        ),
        verified_lossless_target=facts.verified_lossless_target,
        target_format=facts.target_format or candidate.target_format,
        candidate_v0_probe_avg=(
            candidate_probe.avg_bitrate_kbps
            if candidate_probe is not None else None
        ),
        candidate_v0_probe_min=(
            candidate_probe.min_bitrate_kbps
            if candidate_probe is not None else None
        ),
        candidate_v0_probe_kind=(
            candidate_probe.kind if candidate_probe is not None else None
        ),
        existing_v0_probe_avg=(
            current_probe.avg_bitrate_kbps
            if current_probe is not None else None
        ),
        existing_v0_probe_kind=(
            current_probe.kind if current_probe is not None else None
        ),
        # The intentional mutation: an unproven lossless source is sent to
        # the measured route. Every later comparison remains production code.
        supported_lossless_source=False,
        current_verified_lossless_proof=(
            current.verified_lossless_proof is not None
        ),
        candidate_spectral_context=evidence_spectral_context(candidate),
        existing_spectral_context=evidence_spectral_context(current),
        candidate_aac_lattice=candidate.aac_lattice,
    )


def _decoy_decider_lets_an_unproven_album_skip_the_anchor(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
) -> "dict[str, object]":
    """The shape that SHIPPED (issue #990, request 2066): lane entry keyed
    on the spectral grade, so a genuine-graded, proof-denied candidate
    skips the anchor compare and the measured lane's contract-vs-measured
    ranks import an equal copy over the copy it equals. Used only to prove
    the checkers trip."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current, facts=facts)
    )
    if (
        candidate.measurement.spectral_grade in ("genuine", "marginal")
        and result["stage2_import"] in PROVISIONAL_LANE_DECISIONS
    ):
        result["stage2_import"] = "import"
        result["imported"] = True
        result["verified_lossless"] = False
        result["final_status"] = "wanted"
        result["keep_searching"] = True
    return result


def _decoy_decider_lets_a_lattice_denial_reject_the_album(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
) -> "dict[str, object]":
    """The shape PR3 shipped, keyed on the lattice leg: extend the denial
    past the proof into the V0 trust override's downstream routing, so a
    denied album falls back into the provisional-lossless lane. Against a
    provisional installed album that lane answers
    ``suspect_lossless_downgrade`` — a CONFIDENT reject — turning a
    withheld proof into a rejection plus a peer denylist. Used only to
    prove the checker trips."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current, facts=facts)
    )
    leg = aac_lattice_proof_leg(candidate.aac_lattice)
    if not leg.denies_promotion:
        return result
    provisional = provisional_lossless_decision(
        ProvisionalLosslessDecisionInput(
            candidate_probe=_policy_v0_probe_from_metric(candidate.v0_metric),
            existing_probe=(
                _policy_v0_probe_from_metric(current.v0_metric)
                if current is not None else None
            ),
            will_be_verified=False,
            spectral_grade=candidate.measurement.spectral_grade,
            supported_lossless_source=_lossless_source_from_evidence(candidate),
        ),
    )
    if provisional.decision is not None and provisional.confident_reject:
        result["stage2_import"] = provisional.decision
        result["imported"] = False
        result["denylisted"] = True
        result["final_status"] = "wanted"
        result["keep_searching"] = True
    return result


def _proof_world_perturbations(
    candidate: AlbumQualityEvidence,
) -> "list[AlbumQualityEvidence]":
    """One album under every producible proof world.

    Each perturbation moves ONLY proof evidence — a deficit or a lattice
    capture — so anything that differs between the decisions is
    attributable to the proof and nothing else.
    """
    worlds = [
        candidate,
        _with_lattice_capture(candidate, _DENYING_LATTICE_CAPTURE),
        _with_lattice_capture(candidate, _PASSING_LATTICE_CAPTURE),
    ]
    if candidate.measurement.spectral_grade is not None:
        # Evidence-row validation forbids proof-leg facts without a grade,
        # so an ungraded album simply has no producible ultrasonic twin.
        worlds += [
            _with_adjudicable_ultrasonic(candidate, _DENYING_DEFICIT_DB),
            _with_adjudicable_ultrasonic(candidate, _PASSING_DEFICIT_DB),
        ]
    return worlds


def the_stored_format_never_depends_on_the_proof(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None",
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
    decider: "Callable[..., dict[str, object]]" = (
        full_pipeline_decision_from_evidence
    ),
) -> bool:
    """Invariant checker F1: the proof decides the NAME, never the FORMAT.

    ``target_final_format`` is what the harness materializes
    (``_materialize_quality_evidence_action`` parses it into the
    ConversionSpec), so a proof-keyed value silently changes the bytes on
    disk. Download 39087 is the incident: genuine FLAC, ultrasonic leg
    denied, stored as MP3 V0 instead of the configured ``opus 128``.

    Authority: "no we always want it opus, the contract is not around
    verified or not, is the stored format for lossless absolutely.
    whatever people choose, v0,opus,aac it just has to be consistent" —
    https://github.com/abl030/cratedigger/issues/829

    Two halves, both load-bearing:

    * every producible proof world for one album stores the SAME format;
      and
    * a lossless source converting under a configured target stores THAT
      target — otherwise a decider that never stores anything would be
      trivially proof-blind.

    ``decider`` is injectable ONLY so the known-bad self-tests can plant a
    reader that violates one half; production always uses the default.
    """
    formats = {
        decider(perturbed, current, facts=facts)["target_final_format"]
        for perturbed in _proof_world_perturbations(candidate)
    }
    if len(formats) != 1:
        return False

    baseline = decider(candidate, current, facts=facts)
    target_format = facts.target_format if facts is not None else None
    configured = facts.verified_lossless_target if facts is not None else None
    reached_the_lossless_branch = (
        _lossless_source_from_evidence(candidate)
        and target_format not in ("flac", "lossless")
        # An early reject, a Stage-1 short-circuit and the installed-proof
        # ceiling all return before the branch runs at all.
        and baseline["stage2_import"] not in (
            None, DECISION_VERIFIED_LOSSLESS_LOCKED,
        )
    )
    if not reached_the_lossless_branch:
        return True
    return baseline["target_final_format"] == configured


def _decoy_decider_keys_the_stored_format_on_the_proof(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
) -> "dict[str, object]":
    """The shipped defect: license the configured target with the proof,
    so a denial quietly grinds the album to V0 instead. Used only to prove
    the checker trips."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current, facts=facts)
    )
    if not result["verified_lossless"]:
        result["target_final_format"] = None
    return result


def _decoy_decider_never_stores_the_configured_target(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
    *,
    facts: "AlbumQualityEvidenceDecisionFacts | None" = None,
) -> "dict[str, object]":
    """Perfectly proof-blind and perfectly wrong: never store a target at
    all. Used only to prove the checker's second half trips."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current, facts=facts)
    )
    result["target_final_format"] = None
    return result


def _production_stored_format(
    *,
    target_format: "str | None",
    lossless_source: bool,
    verified_lossless_target: "str | None",
    will_be_verified_lossless: bool,
) -> "str | None":
    """The harness's ConversionSpec choice for this world.

    ``will_be_verified_lossless`` is accepted and IGNORED — that is the
    invariant, and giving the property an argument to vary is the only way
    to state it about a function that no longer takes the proof at all.
    """
    del will_be_verified_lossless
    return conversion_target(
        target_format, lossless_source, verified_lossless_target,
    )


def _decoy_stored_format_keyed_on_the_proof(
    *,
    target_format: "str | None",
    lossless_source: bool,
    verified_lossless_target: "str | None",
    will_be_verified_lossless: bool,
) -> "str | None":
    """The pre-fix call site: pass the PROOF where the lossless-source
    fact belongs. Used only to prove the checker trips."""
    del lossless_source
    return conversion_target(
        target_format, will_be_verified_lossless, verified_lossless_target,
    )


def the_harness_stored_format_never_reads_the_proof(
    *,
    target_format: "str | None",
    lossless_source: bool,
    verified_lossless_target: "str | None",
    stored_format_fn: "Callable[..., str | None]" = _production_stored_format,
) -> bool:
    """Invariant checker F2: F1 for the harness's own twin.

    The legacy (non-evidence-authorized) harness path chooses its target
    with ``conversion_target``. The two twins must key on the same fact,
    so this asks the same question of it: does the proof move the answer?
    """
    return len({
        stored_format_fn(
            target_format=target_format,
            lossless_source=lossless_source,
            verified_lossless_target=verified_lossless_target,
            will_be_verified_lossless=proof,
        )
        for proof in (True, False)
    }) == 1


def classifier_names_both_models_that_proved_it(
    ultrasonic_leg: "UltrasonicProofLeg | None",
    lattice_leg: "AacLatticeProofLeg | None",
    *,
    minter: "Callable[..., VerifiedLosslessProof | None]" = (
        mint_verified_lossless_proof
    ),
) -> bool:
    """Invariant checker L3: the classifier composes over BOTH legs.

    v4 exactly when both adjudicated and passed; v3 when the ultrasonic
    leg did and the lattice did not; the base name otherwise. A lattice
    pass with no ultrasonic adjudication is deliberately the BASE name —
    the classifier is a ladder of what was tested, and skipping a rung
    must not buy the top one.
    """
    proof = minter(
        True,
        was_converted_from="flac",
        detected_source_format="flac",
        spectral_grade="genuine",
        ultrasonic_leg=ultrasonic_leg,
        aac_lattice_leg=lattice_leg,
    )
    if proof is None:
        return False
    ultrasonic_passed = (
        ultrasonic_leg is not None and ultrasonic_leg.outcome == "passed"
    )
    lattice_passed = (
        lattice_leg is not None and lattice_leg.outcome == "passed"
    )
    if ultrasonic_passed and lattice_passed:
        expected = VERIFIED_LOSSLESS_CLASSIFIER_V4
    elif ultrasonic_passed:
        expected = VERIFIED_LOSSLESS_CLASSIFIER_V3
    else:
        expected = VERIFIED_LOSSLESS_CLASSIFIER
    return proof.classifier == expected


def _decoy_minter_stamps_v4_whenever_the_lattice_ran(
    will_be_verified_lossless: bool,
    *,
    was_converted_from: "str | None",
    detected_source_format: "str | None",
    spectral_grade: "str | None",
    ultrasonic_leg: "UltrasonicProofLeg | None" = None,
    aac_lattice_leg: "AacLatticeProofLeg | None" = None,
) -> "VerifiedLosslessProof | None":
    """The plausible mistake: the lattice measurement exists, so stamp v4.
    It claims the Apple family was cleared on rows whose capture was too
    thin to test it, and on rows whose ultrasonic leg never adjudicated.
    Used only to prove the checker trips."""
    proof = mint_verified_lossless_proof(
        will_be_verified_lossless,
        was_converted_from=was_converted_from,
        detected_source_format=detected_source_format,
        spectral_grade=spectral_grade,
        ultrasonic_leg=ultrasonic_leg,
    )
    if proof is None or aac_lattice_leg is None:
        return proof
    return msgspec.structs.replace(
        proof, classifier=VERIFIED_LOSSLESS_CLASSIFIER_V4,
    )


class TestAacLatticeProofLegProperties(unittest.TestCase):
    """Generated half of the v4 proof-gate invariant pairs."""

    @given(
        leg=_aac_lattice_legs(),
        spectral_grade=st.sampled_from(_GRADES),
        target_format=st.sampled_from(_TARGET_FORMATS),
        converted_count=st.integers(min_value=0, max_value=24),
        is_transcode=st.booleans(),
        probe_kind=st.one_of(
            st.none(),
            st.sampled_from(("lossless_source_v0", "native_lossy_research_v0")),
        ),
        v0_avg=_optional_bitrates(max_value=400),
        v0_min=_optional_bitrates(max_value=400),
    )
    @example(
        # The Bill Hicks shape against an Apple launder: the world where
        # the V0 override and the leg actively disagree.
        leg=aac_lattice_proof_leg(_DENYING_LATTICE_CAPTURE),
        spectral_grade="suspect", target_format=None, converted_count=10,
        is_transcode=True, probe_kind="lossless_source_v0",
        v0_avg=241, v0_min=219,
    )
    def test_l1_a_denying_leg_is_a_hard_veto(
        self, leg, spectral_grade, target_format, converted_count,
        is_transcode, probe_kind, v0_avg, v0_min,
    ):
        probe = (
            V0ProbeEvidence(
                kind=probe_kind, avg_bitrate_kbps=v0_avg,
                min_bitrate_kbps=v0_min,
            )
            if probe_kind is not None else None
        )
        self.assertTrue(
            denying_lattice_leg_is_a_hard_veto(
                spectral_grade=spectral_grade,
                target_format=target_format,
                converted_count=converted_count,
                is_transcode=is_transcode,
                v0_probe=probe,
                leg=leg,
            )
        )

    @given(
        leg=_aac_lattice_legs(),
        spectral_grade=st.sampled_from(_GRADES),
        target_format=st.sampled_from(_TARGET_FORMATS),
        converted_count=st.integers(min_value=0, max_value=24),
        is_transcode=st.booleans(),
        probe_kind=st.one_of(
            st.none(),
            st.sampled_from(("lossless_source_v0", "native_lossy_research_v0")),
        ),
        v0_avg=_optional_bitrates(max_value=400),
        v0_min=_optional_bitrates(max_value=400),
    )
    @example(
        # The un-measurable shape: a genuine lossless source outside the
        # cohort gate, which is where almost the whole library sits.
        leg=aac_lattice_proof_leg(None),
        spectral_grade="genuine", target_format=None, converted_count=12,
        is_transcode=False, probe_kind=None, v0_avg=None, v0_min=None,
    )
    @example(
        # A clean lattice on a world the pre-existing rules refuse.
        leg=aac_lattice_proof_leg(_PASSING_LATTICE_CAPTURE),
        spectral_grade="error", target_format="flac", converted_count=0,
        is_transcode=False, probe_kind=None, v0_avg=None, v0_min=None,
    )
    def test_l2_the_leg_only_ever_subtracts(
        self, leg, spectral_grade, target_format, converted_count,
        is_transcode, probe_kind, v0_avg, v0_min,
    ):
        probe = (
            V0ProbeEvidence(
                kind=probe_kind, avg_bitrate_kbps=v0_avg,
                min_bitrate_kbps=v0_min,
            )
            if probe_kind is not None else None
        )
        self.assertTrue(
            the_lattice_leg_only_ever_subtracts(
                spectral_grade=spectral_grade,
                target_format=target_format,
                converted_count=converted_count,
                is_transcode=is_transcode,
                v0_probe=probe,
                leg=leg,
            )
        )

    @given(
        candidate=wild_ready_candidate_evidence(),
        current=st.one_of(st.none(), wild_ready_candidate_evidence()),
    )
    def test_l2b_a_non_denying_leg_leaves_the_whole_decision_untouched(
        self, candidate, current,
    ):
        self.assertTrue(
            a_non_denying_lattice_leg_leaves_the_decision_untouched(
                candidate, current,
            )
        )

    @given(
        ultrasonic_leg=st.one_of(st.none(), _ultrasonic_legs()),
        lattice_leg=st.one_of(st.none(), _aac_lattice_legs()),
    )
    def test_l3_the_classifier_names_both_models(
        self, ultrasonic_leg, lattice_leg,
    ):
        self.assertTrue(
            classifier_names_both_models_that_proved_it(
                ultrasonic_leg, lattice_leg,
            )
        )

    @given(world=parity_worlds())
    @example(world=_DENIAL_PROVISIONAL_COHORT_WORLD)
    @example(world=_UNANCHORED_PROVISIONAL_IMPORT_WORLD)
    def test_l5_a_denial_never_reroutes_the_provisional_lane(self, world):
        candidate, current, facts = _parity_evidence_inputs(world)
        self.assertTrue(
            a_lattice_denial_never_reroutes_the_provisional_lane(
                candidate, current, facts=facts,
            )
        )


class TestAacLatticeProofLegCheckerSelfTests(unittest.TestCase):
    """Known-bad self-tests for the v4 proof-gate invariant checkers.

    Each checker gets both halves: it passes for the real decider/minter,
    and it TRIPS on a planted reader that violates the invariant.
    """

    _DENYING_LEG = aac_lattice_proof_leg(_DENYING_LATTICE_CAPTURE)
    _PASSING_LEG = aac_lattice_proof_leg(_PASSING_LATTICE_CAPTURE)
    _WITHHELD_LEG = aac_lattice_proof_leg(_THIN_LATTICE_CAPTURE)

    def test_the_fixtures_have_the_outcomes_they_claim(self):
        """The fixtures are only evidence if the production evaluator
        actually reads them the way their names assert."""
        self.assertEqual(self._DENYING_LEG.outcome, "denied")
        self.assertEqual(self._DENYING_LEG.reason, "offset_concentration")
        self.assertEqual(self._PASSING_LEG.outcome, "passed")
        self.assertEqual(self._WITHHELD_LEG.outcome, "withheld")
        self.assertEqual(
            self._WITHHELD_LEG.reason, "insufficient_scored_tracks",
        )
        self.assertEqual(_ULTRASONIC_PASSING_LEG.outcome, "passed")

    def test_l1_checker_passes_for_the_real_decider(self):
        self.assertTrue(
            denying_lattice_leg_is_a_hard_veto(
                spectral_grade="suspect", target_format=None,
                converted_count=10, is_transcode=True,
                v0_probe=V0ProbeEvidence(
                    kind="lossless_source_v0",
                    avg_bitrate_kbps=241, min_bitrate_kbps=219,
                ),
                leg=self._DENYING_LEG,
            )
        )

    def test_l1_checker_trips_when_the_v0_override_outranks_the_leg(self):
        self.assertFalse(
            denying_lattice_leg_is_a_hard_veto(
                spectral_grade="suspect", target_format=None,
                converted_count=10, is_transcode=True,
                v0_probe=V0ProbeEvidence(
                    kind="lossless_source_v0",
                    avg_bitrate_kbps=241, min_bitrate_kbps=219,
                ),
                leg=self._DENYING_LEG,
                decider=(
                    _decoy_decider_checks_the_lattice_after_the_v0_override
                ),
            )
        )

    def _subtracts(self, leg, **overrides):
        world = {
            "spectral_grade": "genuine", "target_format": None,
            "converted_count": 12, "is_transcode": False, "v0_probe": None,
        }
        world.update(overrides)
        return the_lattice_leg_only_ever_subtracts(leg=leg, **world)

    def test_l2_checker_passes_for_the_real_decider(self):
        for leg in (self._WITHHELD_LEG, self._PASSING_LEG, self._DENYING_LEG):
            with self.subTest(outcome=leg.outcome):
                self.assertTrue(self._subtracts(leg))

    def test_l2_checker_trips_when_withheld_is_read_as_denying(self):
        self.assertFalse(
            self._subtracts(
                self._WITHHELD_LEG,
                decider=_decoy_decider_treats_a_withheld_lattice_as_denying,
            )
        )

    def test_l2_checker_trips_when_a_clean_lattice_is_affirmative(self):
        self.assertFalse(
            self._subtracts(
                self._PASSING_LEG, spectral_grade="error",
                target_format="flac", converted_count=0,
                decider=_decoy_decider_lets_a_clean_lattice_grant_proof,
            )
        )

    def _thin_capture_candidate(self) -> AlbumQualityEvidence:
        """A lossless candidate whose capture ran but scored too few
        tracks to test anything — the shape a raw reader mis-reads."""
        return build_parity_candidate_evidence(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine",
            candidate_v0_probe_avg=245, candidate_v0_probe_min=245,
            codec_family="lossless",
            aac_lattice=_THIN_LATTICE_CAPTURE,
        )

    def _passing_capture_candidate(self) -> AlbumQualityEvidence:
        """The same album with a capture the leg ADJUDICATES and clears.

        The oracle must admit this world, or L2b never patrols the
        population the leg newly speaks about."""
        return msgspec.structs.replace(
            self._thin_capture_candidate(),
            aac_lattice=_PASSING_LATTICE_CAPTURE,
        )

    def test_l2b_checker_passes_for_the_real_decider(self):
        for name, candidate in (
            ("withheld", self._thin_capture_candidate()),
            ("passed", self._passing_capture_candidate()),
        ):
            with self.subTest(leg=name):
                self.assertTrue(
                    _lattice_leg_does_not_deny_by_oracle(candidate)
                )
                self.assertTrue(
                    a_non_denying_lattice_leg_leaves_the_decision_untouched(
                        candidate, None,
                    )
                )

    def test_l2b_checker_trips_on_a_clean_lattice_leaking_into_the_decision(
        self,
    ):
        """The half a withheld-only oracle cannot reach: the leg
        ADJUDICATED and PASSED, and a reader acted on it anyway. A clean
        lattice is one negative finding about one laundering family; it
        licenses nothing but the v4 classifier."""
        candidate = self._passing_capture_candidate()
        self.assertEqual(
            aac_lattice_proof_leg(candidate.aac_lattice).outcome, "passed",
        )
        self.assertTrue(_lattice_leg_does_not_deny_by_oracle(candidate))
        self.assertFalse(
            a_non_denying_lattice_leg_leaves_the_decision_untouched(
                candidate, None,
                decider=(
                    _decoy_decider_lets_a_clean_lattice_leak_into_the_decision
                ),
            )
        )

    def test_l2b_checker_trips_on_a_raw_concentration_reader(self):
        """Two of the three scored tracks coincide — a modal count of 2,
        which the concentration rule prices at ~322 false positives per
        5000 albums and therefore ignores. A raw reader that treats any
        coincidence as evidence demotes the album anyway."""
        two_of_three = make_aac_lattice_capture([
            (13, 4.58), (13, 4.80), (418, 4.97),
            (None, None), (None, None), (None, None),
        ])
        self.assertEqual(two_of_three.modal_count, 2)
        candidate = msgspec.structs.replace(
            self._thin_capture_candidate(), aac_lattice=two_of_three,
        )
        self.assertTrue(_lattice_leg_does_not_deny_by_oracle(candidate))
        self.assertFalse(
            a_non_denying_lattice_leg_leaves_the_decision_untouched(
                candidate, None,
                decider=(
                    _decoy_decider_reads_the_raw_modal_count_outside_the_leg
                ),
            )
        )

    def test_l3_checker_passes_for_the_real_minter(self):
        for ultrasonic in (None, _ULTRASONIC_PASSING_LEG):
            for lattice in (None, self._PASSING_LEG, self._WITHHELD_LEG,
                            self._DENYING_LEG):
                with self.subTest(
                    ultrasonic=ultrasonic is not None,
                    lattice=lattice.outcome if lattice else None,
                ):
                    self.assertTrue(
                        classifier_names_both_models_that_proved_it(
                            ultrasonic, lattice,
                        )
                    )

    def test_l3_checker_trips_when_v4_is_stamped_for_a_leg_that_only_ran(self):
        self.assertFalse(
            classifier_names_both_models_that_proved_it(
                _ULTRASONIC_PASSING_LEG, self._WITHHELD_LEG,
                minter=_decoy_minter_stamps_v4_whenever_the_lattice_ran,
            )
        )

    def test_l5_checker_passes_for_the_real_decider(self):
        candidate, current, facts = _parity_evidence_inputs(
            _DENIAL_PROVISIONAL_COHORT_WORLD,
        )
        self.assertTrue(
            a_lattice_denial_never_reroutes_the_provisional_lane(
                candidate, current, facts=facts,
            )
        )

    def test_l5_checker_trips_when_a_denial_reroutes_the_lane(self):
        """PR3's review world, keyed on the lattice leg: an installed
        provisional album whose own ``lossless_source_v0`` probe (avg 240)
        sits a hair under the candidate's (avg 241). A decider that lets
        the denial suppress the V0 trust override drops that album into
        the provisional lane, where that pair is a confident reject."""
        candidate, current, facts = _parity_evidence_inputs(
            _DENIAL_PROVISIONAL_COHORT_WORLD,
        )
        self.assertFalse(
            a_lattice_denial_never_reroutes_the_provisional_lane(
                candidate, current, facts=facts,
                decider=_decoy_decider_lets_a_lattice_denial_reject_the_album,
            )
        )

    def test_l5_checker_trips_when_an_unanchored_denial_bypasses_to_measured_reject(self):
        """L5 inherits R4's routing rule, including the bypass direction."""
        candidate, current, facts = _parity_evidence_inputs(
            _UNANCHORED_MEASURED_REJECT_WORLD,
        )
        self.assertFalse(
            a_lattice_denial_never_reroutes_the_provisional_lane(
                candidate,
                current,
                facts=facts,
                decider=(
                    _decoy_decider_bypasses_unanchored_lane_via_production_comparator
                ),
            )
        )


class TestStoredFormatIsProofBlindProperties(unittest.TestCase):
    """Generated half of the stored-format invariant pair (issue #829).

    Deterministic twin:
    ``tests/test_quality_classification.py::TestLosslessStoredFormatIsProofBlind``
    and ``tests/test_conversion_e2e.py::
    TestDeniedProofStillStoresTheConfiguredTarget``.
    """

    @given(world=parity_worlds())
    @example(world=_DENIAL_PROVISIONAL_COHORT_WORLD)
    @example(
        # The Badlands shape: a genuine-graded lossless source converting
        # to the library's configured target, with an installed album to
        # be compared against.
        world=replace(
            _DENIAL_PROVISIONAL_COHORT_WORLD,
            grade="genuine",
            verified_lossless_target="opus 128",
            target_format=None,
        ),
    )
    def test_f1_the_stored_format_never_depends_on_the_proof(self, world):
        candidate, current, facts = _parity_evidence_inputs(world)
        self.assertTrue(
            the_stored_format_never_depends_on_the_proof(
                candidate, current, facts=facts,
            )
        )

    @given(
        target_format=st.sampled_from(_TARGET_FORMATS),
        lossless_source=st.booleans(),
        verified_lossless_target=st.sampled_from(_VL_TARGETS),
    )
    def test_f2_the_harness_target_never_reads_the_proof(
        self, target_format, lossless_source, verified_lossless_target,
    ):
        self.assertTrue(
            the_harness_stored_format_never_reads_the_proof(
                target_format=target_format,
                lossless_source=lossless_source,
                verified_lossless_target=verified_lossless_target,
            )
        )


class TestProofBackedTargetTerminalProperties(unittest.TestCase):
    @given(
        source_bitrate=st.integers(min_value=400, max_value=1400),
        current_bitrate=st.one_of(
            st.none(), st.integers(min_value=1, max_value=130)
        ),
        converted_count=st.integers(min_value=1, max_value=30),
    )
    def test_configured_target_finishes_for_every_non_downgrade(
        self,
        source_bitrate,
        current_bitrate,
        converted_count,
    ):
        candidate = build_parity_candidate_evidence(
            is_flac=True,
            min_bitrate=source_bitrate,
            is_cbr=False,
        )
        candidate = msgspec.structs.replace(
            candidate,
            target_format="opus 128",
            target_is_cbr=False,
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier="generated",
            ),
        )
        current = build_parity_current_evidence(
            min_bitrate=current_bitrate,
            avg_bitrate=current_bitrate,
            format="Opus",
        )
        result = full_pipeline_decision_from_evidence(
            candidate,
            current,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target="opus 128",
                target_format="opus 128",
                converted_count=converted_count,
                post_conversion_min_bitrate=114,
                post_conversion_is_cbr=False,
            ),
        )

        assert_proof_backed_target_uses_terminal_gate(result)


class TestStoredFormatCheckerSelfTests(unittest.TestCase):
    """Known-bad self-tests for the stored-format invariant checkers."""

    #: A genuine-graded lossless source with the library's configured
    #: target — the population the whole invariant is about.
    _WORLD = replace(
        _DENIAL_PROVISIONAL_COHORT_WORLD,
        grade="genuine",
        verified_lossless_target="opus 128",
        target_format=None,
    )

    def _inputs(self):
        return _parity_evidence_inputs(self._WORLD)

    def test_f1_checker_passes_for_the_real_decider(self):
        candidate, current, facts = self._inputs()
        self.assertTrue(
            the_stored_format_never_depends_on_the_proof(
                candidate, current, facts=facts,
            )
        )

    def test_f1_checker_trips_when_the_proof_gates_the_format(self):
        """The shipped defect, planted: withhold the configured target
        whenever the proof was withheld. That is exactly what download
        39087 did — a genuine FLAC stored as MP3 V0."""
        candidate, current, facts = self._inputs()
        self.assertFalse(
            the_stored_format_never_depends_on_the_proof(
                candidate, current, facts=facts,
                decider=_decoy_decider_keys_the_stored_format_on_the_proof,
            )
        )

    def test_f1_checker_trips_when_no_world_stores_a_target(self):
        """The other half: a decider that never stores the configured
        format is perfectly proof-blind and perfectly wrong."""
        candidate, current, facts = self._inputs()
        self.assertFalse(
            the_stored_format_never_depends_on_the_proof(
                candidate, current, facts=facts,
                decider=_decoy_decider_never_stores_the_configured_target,
            )
        )

    def test_f2_checker_passes_for_the_real_harness(self):
        self.assertTrue(
            the_harness_stored_format_never_reads_the_proof(
                target_format=None, lossless_source=True,
                verified_lossless_target="opus 128",
            )
        )

    def test_f2_checker_trips_on_the_proof_keyed_call_site(self):
        """``harness/import_one.py`` used to pass
        ``will_be_verified_lossless`` into this slot."""
        self.assertFalse(
            the_harness_stored_format_never_reads_the_proof(
                target_format=None, lossless_source=True,
                verified_lossless_target="opus 128",
                stored_format_fn=_decoy_stored_format_keyed_on_the_proof,
            )
        )


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: prove the harness detects what it claims to."""

    def test_definitive_checker_trips_on_every_clause(self):
        """All four totality clauses, each proven by its own message.

        The three type clauses short-circuit ahead of the status clause, so a
        single bogus world only ever proved the one it reached.
        """
        bogus_status = SimResult(
            imported=False, keep_searching=False, denylisted=False,
            final_status=None, stage0_spectral_gate=None,
            stage1_spectral=None, stage2_import=None,
            stage3_quality_gate=None, backfill_override=None,
            search_filetype_override_after=None)
        for label, bad, expected in (
            ("imported", _ill_typed_sim_result("imported", "yes"),
             r"^imported is not bool: 'yes'$"),
            ("keep_searching", _ill_typed_sim_result("keep_searching", "no"),
             r"^keep_searching is not bool: 'no'$"),
            ("denylisted", _ill_typed_sim_result("denylisted", 1),
             r"^denylisted is not bool: 1$"),
            ("final_status", bogus_status,
             (r"^auto-mode decision must end imported/wanted, "
              r"got final_status=None$")),
            ("final_status_value",
             replace(_planted_bad_import(), final_status="processing"),
             (r"^auto-mode decision must end imported/wanted, "
              r"got final_status='processing'$")),
        ):
            with self.subTest(clause=label), self.assertRaisesRegex(
                    AssertionError, expected):
                assert_decision_is_definitive(bad)

    def test_verified_lossless_checker_trips_on_import(self):
        with self.assertRaisesRegex(
                AssertionError,
                r"^lossy candidate imported over raw verified-lossless FLAC: "):
            assert_lossy_not_imported_over_verified_lossless(
                _planted_bad_import())

    def test_proof_lock_checker_kills_proof_ignoring_mutant(self):
        """Omitting the proof input recreates the pre-U7 replacement bug."""
        mutant = simulate(
            AlbumState(
                "proof_ignored",
                207,
                False,
                "genuine",
                None,
                True,
                None,
                existing_format="MP3",
                avg_bitrate=207,
            ),
            DownloadScenario(
                "higher_candidate",
                is_flac=False,
                min_bitrate=240,
                is_cbr=False,
                is_vbr=True,
                avg_bitrate=245,
                new_format="MP3",
            ),
            current_verified_lossless_proof=False,
        )
        with self.assertRaisesRegex(
                AssertionError,
                r"^proof-bearing HAVE was automatically replaced$"):
            assert_verified_lossless_proof_locks_candidate(mutant)

    def test_proof_lock_checker_trips_on_every_clause(self):
        """Each proof-lock clause proven from a real proof-locked decision.

        The baseline is what ``simulate`` really produces under a current
        proof, so every planted violation below mutates a shape production
        emits rather than a hand-written one (Rule C).
        """
        locked = simulate(
            AlbumState(
                "proof_locked", 245, False, "genuine", None, True, None,
                existing_format="FLAC", avg_bitrate=245),
            DownloadScenario(
                "candidate", is_flac=False, min_bitrate=320, is_cbr=True,
                avg_bitrate=320, new_format="MP3"),
            current_verified_lossless_proof=True)
        assert_verified_lossless_proof_locks_candidate(locked)
        for label, bad, expected in (
            ("replaced", replace(locked, imported=True),
             r"^proof-bearing HAVE was automatically replaced$"),
            ("missed_lock", replace(locked, stage2_import="import"),
             r"^proof-bearing HAVE missed verified_lossless_locked: 'import'$"),
            ("not_terminal", replace(locked, final_status="wanted"),
             (r"^proof lock did not preserve terminal imported state: "
              r"status='wanted', keep=False$")),
            ("keep_searching", replace(locked, keep_searching=True),
             (r"^proof lock did not preserve terminal imported state: "
              r"status='imported', keep=True$")),
            ("punished", replace(locked, denylisted=True),
             r"^proof lock punished the candidate source$"),
        ):
            with self.subTest(clause=label), self.assertRaisesRegex(
                    AssertionError, expected):
                assert_verified_lossless_proof_locks_candidate(bad)

    def _evidence_proof_locked_decision(self, **overrides: object) -> dict:
        """A real proof-locked evidence decision, optionally corrupted."""
        current = build_parity_current_evidence(
            min_bitrate=128, avg_bitrate=128, format="Opus")
        assert current is not None
        decision = full_pipeline_decision_from_evidence(
            build_parity_candidate_evidence(
                is_flac=False, min_bitrate=245, is_cbr=False),
            msgspec.structs.replace(
                current,
                verified_lossless_proof=VerifiedLosslessProof(
                    provenance="measured",
                    source="generated",
                    classifier="generated",
                ),
            ),
        )
        assert decision["stage2_import"] == "verified_lossless_locked"
        decision.update(overrides)
        return decision

    def test_evidence_proof_lock_checker_trips_on_every_clause(self):
        """Each evidence proof-lock clause proven by its own message."""
        clean = self._evidence_proof_locked_decision()
        assert_evidence_proof_lock_preserves_imported(clean)
        for label, overrides, expected in (
            ("missed_lock", {"stage2_import": "import"},
             r"^evidence proof lock missed: 'import'$"),
            ("lost_have", {"final_status": "wanted"},
             r"^evidence proof lock did not preserve the installed HAVE$"),
            ("replaced", {"imported": True},
             r"^evidence proof lock did not preserve the installed HAVE$"),
            ("punished", {"denylisted": True},
             r"^evidence proof lock reopened or punished source$"),
            ("reopened", {"keep_searching": True},
             r"^evidence proof lock reopened or punished source$"),
            ("leaked_reject", {"preimport_audio": "reject_corrupt"},
             (r"^evidence proof lock leaked candidate reject "
              r"preimport_audio='reject_corrupt'$")),
        ):
            with self.subTest(clause=label), self.assertRaisesRegex(
                    AssertionError, expected):
                assert_evidence_proof_lock_preserves_imported(
                    self._evidence_proof_locked_decision(**overrides))

    def test_downgrade_checker_trips_on_accept(self):
        with self.assertRaisesRegex(
                AssertionError,
                r"^obvious lower-rank lossy candidate accepted: "):
            assert_obvious_downgrade_not_accepted(_planted_bad_import())

    def test_unverified_lossy_checker_trips_on_every_clause(self):
        """All four retained-inventory clauses, from a real retained world."""
        retained = simulate(
            _FRESH_ALBUM,
            DownloadScenario(
                "usable_lossy", is_flac=False, min_bitrate=245, is_cbr=False,
                is_vbr=True, avg_bitrate=245, new_format="MP3"))
        assert_unverified_lossy_never_terminal(retained)
        for label, bad, expected in (
            ("not_retained", replace(retained, imported=False),
             r"^usable lossy first copy was not retained: "),
            ("terminal_accept",
             replace(retained, stage3_quality_gate="accept"),
             r"^unverified lossy copy was accepted terminally: "),
            ("stopped_searching",
             replace(retained, final_status="imported", keep_searching=False),
             r"^unverified lossy copy stopped searching: "),
            ("not_denylisted", replace(retained, denylisted=False),
             r"^retained lossy source was not denylisted: "),
        ):
            with self.subTest(clause=label), self.assertRaisesRegex(
                    AssertionError, expected):
                assert_unverified_lossy_never_terminal(bad)

    def test_search_override_checker_trips_on_a_non_string(self):
        self.assertIsNone(assert_search_override_is_a_string(None))
        self.assertEqual(assert_search_override_is_a_string("lossless"),
                         "lossless")
        with self.assertRaisesRegex(
                AssertionError,
                r"^quality gate wrote a non-string override: 7$"):
            assert_search_override_is_a_string(7)

    def test_proof_backed_target_checker_trips_on_transcode_routing(self):
        with self.assertRaisesRegex(
                AssertionError,
                r"^proof-backed configured target was not terminal: "):
            assert_proof_backed_target_uses_terminal_gate({
                "stage2_import": "transcode_upgrade",
                "stage3_quality_gate": "accept",
                "final_status": "imported",
                "imported": True,
                "keep_searching": False,
                "denylisted": False,
            })

    def test_action_mapping_checker_trips_on_each_output_field(self):
        for field, overrides in (
            ("status", {"status": "imported"}),
            ("override", {"search_filetype_override": "lossless"}),
            ("denylist", {"denylist": False}),
        ):
            kwargs = {
                "decision": "requeue_upgrade",
                "status": "wanted",
                "search_filetype_override": None,
                "denylist": True,
                **overrides,
            }
            with self.subTest(field=field), self.assertRaisesRegex(
                    AssertionError,
                    r"^post-import mapping drift for requeue_upgrade: "):
                assert_post_import_action_matches(**kwargs)

    def test_quality_failure_checker_trips_on_both_clauses(self):
        from lib import transitions

        with self.assertRaisesRegex(
                AssertionError,
                r"^quality decision failure returned no recovery plan$"):
            assert_quality_decision_failure_reopens_full_tier(None)

        bad = QualityGatePlan(
            transition=transitions.RequestTransition.to_imported(
                from_status="imported",
            ),
            successful_terminal_acceptance=True,
        )
        with self.assertRaisesRegex(
                AssertionError,
                r"^quality decision failure did not reopen full tiers: "):
            assert_quality_decision_failure_reopens_full_tier(bad)

    def test_affirmative_verification_checker_trips_on_absent_evidence(self):
        with self.assertRaisesRegex(
                AssertionError,
                r"^verified lossless minted without affirmative spectral "
                r"evidence: grade=None, probe_kind='lossless_source_v0', "
                r"avg=300, min=250$"):
            assert_verified_lossless_has_affirmative_evidence(
                True,
                spectral_grade=None,
                v0_probe_kind="lossless_source_v0",
                v0_avg=300,
                v0_min=250,
            )

    def test_strictly_lower_spectral_checker_trips_on_violations(self):
        # Planted tie-reject (the Mark DeNardo bug): equal floor rejected.
        with self.assertRaisesRegex(
                AssertionError,
                r"^spectral 128 vs 128 \(grade=suspect\) is a tie or upgrade "
                r"but was rejected at Stage 1 instead of deferring to Stage 2: "
                r"'reject'$"):
            assert_only_strictly_lower_spectral_rejects(
                "reject",
                grade="suspect",
                new_spectral=128,
                existing_spectral=128,
            )
        # Planted strictly-lower non-reject: worse content that failed to reject.
        with self.assertRaisesRegex(
                AssertionError,
                r"^strictly-lower spectral 96 < 128 \(grade=likely_transcode\) "
                r"must reject at Stage 1, got 'import'$"):
            assert_only_strictly_lower_spectral_rejects(
                "import",
                grade="likely_transcode",
                new_spectral=96,
                existing_spectral=128,
            )

    def test_existing_override_noop_checker_trips_on_both_clauses(self):
        # Planted phantom upgrade (the Deerhunter bug): applying the existing-
        # side spectral override flips the Stage-2 decision.
        with self.assertRaisesRegex(
                AssertionError,
                r"^existing-side spectral override changed the Stage-2 "
                r"decision under a shared spectral clamp: 'import' "
                r"\(override\) vs 'downgrade' \(none\)$"):
            assert_existing_override_noop_under_shared_clamp(
                {"stage2_import": "import",
                 "comparison_basis": {"verdict": "better"}},
                {"stage2_import": "downgrade",
                 "comparison_basis": {"verdict": "equivalent"}},
            )
        # The verdict clause, which the decision clause short-circuits past:
        # the two runs agree on the decision and disagree on WHY. Both of the
        # worlds this self-test used to carry moved ``stage2_import``, so the
        # verdict clause had no proof at all.
        with self.assertRaisesRegex(
                AssertionError,
                r"^existing-side spectral override changed the comparison "
                r"verdict under a shared spectral clamp: 'better' "
                r"\(override\) vs 'equivalent' \(none\)$"):
            assert_existing_override_noop_under_shared_clamp(
                {"stage2_import": "import",
                 "comparison_basis": {"verdict": "better"}},
                {"stage2_import": "import",
                 "comparison_basis": {"verdict": "equivalent"}},
            )
        # An invariant (identical) pair must NOT trip.
        assert_existing_override_noop_under_shared_clamp(
            {"stage2_import": "downgrade",
             "comparison_basis": {"verdict": "equivalent"}},
            {"stage2_import": "downgrade",
             "comparison_basis": {"verdict": "equivalent"}},
        )

    def test_stage1_stage2_parity_checker_trips_on_planted_disagreement(self):
        """Issue #813 Finding 1 known-bad self-test: a planted Stage-1
        reject alongside a planted Stage-2 "better" must trip the checker;
        every other verdict pairing (including a real reject-vs-worse
        agreement) must NOT."""
        with self.assertRaisesRegex(
                AssertionError,
                r"^Stage 1 rejected a candidate Stage 2 scores as an upgrade: "
                r"stage1='reject' stage2\.verdict='better' "
                r"stage2\.branch='spectral_tiebreak'$"):
            assert_stage1_never_contradicts_stage2(
                "reject",
                QualityComparisonBasis(
                    verdict="better", branch="spectral_tiebreak",
                    new_rank="good", existing_rank="good",
                ),
            )
        # Stage 1 rejecting while Stage 2 agrees (worse/equivalent) is not
        # a disagreement — must not trip.
        assert_stage1_never_contradicts_stage2(
            "reject",
            QualityComparisonBasis(
                verdict="worse", branch="spectral_tiebreak",
                new_rank="good", existing_rank="good",
            ),
        )
        assert_stage1_never_contradicts_stage2(
            "reject",
            QualityComparisonBasis(
                verdict="equivalent", branch="metric_tiebreak",
                new_rank="good", existing_rank="good",
            ),
        )
        # Stage 1 deferring (any non-reject verdict) never trips, whatever
        # Stage 2 says — Stage 1 has no gating effect in that case.
        assert_stage1_never_contradicts_stage2(
            "import",
            QualityComparisonBasis(
                verdict="better", branch="rank",
                new_rank="transparent", existing_rank="good",
            ),
        )
        # A gate-skipped world (no Stage-1 verdict at all) is the same.
        assert_stage1_never_contradicts_stage2(
            None,
            QualityComparisonBasis(
                verdict="better", branch="rank",
                new_rank="transparent", existing_rank="good",
            ),
        )

    def _stage1_reject_decision(self, **overrides: object) -> dict[str, object]:
        """A real Stage-1 short-circuit result, optionally corrupted.

        The clean baseline comes from the production decider, not a
        hand-written dict, so a planted leak below is a mutation of a shape
        production really emits (``.claude/rules/test-fidelity.md`` Rule C).
        """
        decision = dict(
            _stage_parity_decision(_STAGE1_REJECT_COUNTERFACTUAL_WORLD))
        assert decision["stage1_spectral"] == "reject", repr(decision)
        decision.update(overrides)
        return decision

    def test_stage1_reject_inertness_checker_trips_on_each_leaked_field(self):
        """Issue #829 Phase 5 PR2d known-bad self-test: every decision field
        the counterfactual could leak into must trip the checker."""
        clean = self._stage1_reject_decision()
        assert_stage1_reject_leaks_no_stage2_state(clean)
        for field, leaked in (
            ("stage2_import", "import"),
            ("stage3_quality_gate", "accept"),
            ("comparison_basis", {"verdict": "better"}),
            ("target_final_format", "mp3 v0"),
            ("final_status", "imported"),
            ("keep_searching", False),
            ("imported", True),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(
                    AssertionError,
                    r"^Stage-2 state leaked onto a Stage-1 reject decision: "
                    + re.escape(f"{field}={leaked!r}")):
                assert_stage1_reject_leaks_no_stage2_state(
                    self._stage1_reject_decision(**{field: leaked}))

    def test_carve_out_lever_checker_trips_on_a_moved_stage_2(self):
        """Issue #829 Phase 5 PR2d: the lever-inertness clause, proven.

        Extracted from the property body so it can be driven directly — the
        baseline is a real deferring decision and its real levered twin.
        """
        world = replace(_STAGE1_REJECT_COUNTERFACTUAL_WORLD, new_spectral=192)
        decision = _stage_parity_decision(world)
        levered = _stage_parity_deferred_decision(world)
        assert decision["stage1_spectral"] != "reject", repr(decision)
        assert_carve_out_lever_is_stage2_inert(decision, levered)
        for label, moved in (
            ("stage2_import", {"stage2_import": "planted_other_decision"}),
            ("comparison_basis",
             {"comparison_basis": {"verdict": "planted_other_verdict"}}),
        ):
            planted = dict(levered)
            planted.update(moved)
            assert planted[label] != decision[label], repr(decision)
            with self.subTest(field=label), self.assertRaisesRegex(
                    AssertionError,
                    r"^the Stage-1 carve-out lever moved Stage 2: "):
                assert_carve_out_lever_is_stage2_inert(decision, planted)

    def test_counterfactual_truth_checker_trips_on_a_fabricated_value(self):
        """Issue #829 Phase 5 PR2d known-bad self-test: a counterfactual that
        does not match what Stage 2 decides must trip, in either field."""
        world = _STAGE1_REJECT_COUNTERFACTUAL_WORLD
        short_circuited = _stage_parity_decision(world)
        deferred = _stage_parity_deferred_decision(world)
        assert_counterfactual_is_the_deferred_stage2(short_circuited, deferred)
        for field, fabricated in (
            ("stage2_import_if_stage1_deferred", "import"),
            ("comparison_basis_if_stage1_deferred", {"verdict": "better"}),
            # The absent case is a lie too: an audit that reports nothing
            # where Stage 2 has an answer is not "no opinion", it is wrong.
            ("stage2_import_if_stage1_deferred", None),
        ):
            planted = dict(short_circuited)
            planted[field] = fabricated
            with self.subTest(field=field, value=fabricated), \
                    self.assertRaisesRegex(
                        AssertionError,
                        r"^the reported Stage-2 counterfactual is not what "
                        r"Stage 2 decides: " + re.escape(f"{field}=")):
                assert_counterfactual_is_the_deferred_stage2(planted, deferred)

    def test_counterfactual_reporting_checker_trips_on_all_three_violations(self):
        """Issue #829 Phase 5 PR2d known-bad self-test: a missing audit key,
        a counterfactual reported alongside a real Stage-2 decision, and a
        short-circuit that reports nothing at all (review S2)."""
        deferring = _stage_parity_decision(
            replace(_STAGE1_REJECT_COUNTERFACTUAL_WORLD, new_spectral=192))
        assert deferring["stage2_import"] is not None, repr(deferring)
        assert_counterfactual_reported_exactly_when_stage1_short_circuits(
            deferring)

        dropped = dict(deferring)
        del dropped["comparison_basis_if_stage1_deferred"]
        with self.assertRaisesRegex(
                AssertionError,
                r"^decision dict is missing audit keys "
                r"\['comparison_basis_if_stage1_deferred'\]$"):
            assert_counterfactual_reported_exactly_when_stage1_short_circuits(
                dropped)

        doubled = dict(deferring)
        doubled["stage2_import_if_stage1_deferred"] = "downgrade"
        with self.assertRaisesRegex(
                AssertionError,
                r"^a Stage-2 counterfactual was reported alongside a real "
                r"Stage-2 decision: "
                r"stage2_import_if_stage1_deferred='downgrade'$"):
            assert_counterfactual_reported_exactly_when_stage1_short_circuits(
                doubled)

        # The S2 gap: a real short-circuit whose audit key is None. Before
        # the sentinel this was byte-identical to "Stage 1 never fired" and
        # no property could tell them apart.
        short_circuited = _stage_parity_decision(
            _STAGE1_REJECT_COUNTERFACTUAL_WORLD)
        assert short_circuited["stage1_spectral"] == "reject"
        assert_counterfactual_reported_exactly_when_stage1_short_circuits(
            short_circuited)
        silent = dict(short_circuited)
        silent["stage2_import_if_stage1_deferred"] = None
        with self.assertRaisesRegex(
                AssertionError,
                r"^Stage 1 short-circuited but reported no Stage-2 "
                r"counterfactual at all"):
            assert_counterfactual_reported_exactly_when_stage1_short_circuits(
                silent)

    def test_have_representation_checker_trips(self):
        """Issue #829 PR2c item 6 known-bad self-test.

        The planted violation is the download_log 29525 world exactly: a
        HAVE ranked ``transparent`` on its 320 container while its own
        cliff-derived class of 128 ranks ``acceptable``.
        """
        with self.assertRaisesRegex(
                AssertionError,
                r"^the installed copy was ranked above its own spectral class: "
                r"existing_rank='transparent' but its class alone ranks "
                r"'acceptable' — neither the symmetric clamp nor the one-sided "
                r"override represented it by its real content$"):
            assert_have_is_represented_by_its_own_class(
                "transparent", "acceptable")
        # One tier over is still a violation.
        with self.assertRaisesRegex(
                AssertionError,
                r"^the installed copy was ranked above its own spectral class: "
                r"existing_rank='good'"):
            assert_have_is_represented_by_its_own_class("good", "acceptable")
        # Equal is the normal clamped/overridden case, and BELOW the class
        # is fine too — the raw metric can be the tighter of the two.
        assert_have_is_represented_by_its_own_class(
            "acceptable", "acceptable")
        assert_have_is_represented_by_its_own_class("poor", "transparent")

    def test_inadmissible_existing_class_checker_trips(self):
        """Issue #829 PR2c known-bad self-test — both clauses trip."""
        # Clause 1: a Stage-1 rejection built on an inadmissible pair.
        with self.assertRaisesRegex(
                AssertionError,
                r"^Stage 1 rejected on a spectral comparison Stage 2 is not "
                r"permitted to make: the two spectral classes are not "
                r"comparable, so the existing side contributes no class at "
                r"all$"):
            assert_stage1_ignores_inadmissible_existing_spectral(
                "reject", "import_no_exist")
        # Clause 2: the silent direction — no rejection, but the withheld
        # evidence still moved the verdict.
        with self.assertRaisesRegex(
                AssertionError,
                r"^an inadmissible existing-side spectral class changed the "
                r"Stage 1 verdict: 'import_upgrade' \(evidence present\) vs "
                r"'import_no_exist' \(evidence withheld\)$"):
            assert_stage1_ignores_inadmissible_existing_spectral(
                "import_upgrade", "import_no_exist")
        # A rejection trips even when the withheld run rejects too — a
        # mutant that fabricates a class from nothing must not slip through
        # the equality clause.
        with self.assertRaisesRegex(
                AssertionError,
                r"^Stage 1 rejected on a spectral comparison Stage 2 is not "
                r"permitted to make: "):
            assert_stage1_ignores_inadmissible_existing_spectral(
                "reject", "reject")
        # Invariant worlds must NOT trip, including the gate-skipped shape.
        assert_stage1_ignores_inadmissible_existing_spectral(
            "import_no_exist", "import_no_exist")
        assert_stage1_ignores_inadmissible_existing_spectral("import", "import")
        assert_stage1_ignores_inadmissible_existing_spectral(None, None)

    def test_unmapped_codec_checker_trips_on_every_clause(self):
        """All four retained-without-a-ceiling clauses, from a real world."""
        retained = simulate(
            _FRESH_ALBUM,
            DownloadScenario(
                name="generated_unmapped_codec", is_flac=False,
                min_bitrate=192, is_cbr=True, is_vbr=False, avg_bitrate=192,
                spectral_grade=None, new_format="zzz"))
        assert_unmapped_first_copy_stays_searchable(retained)
        for label, bad, expected in (
            ("not_imported", replace(retained, imported=False),
             r"^unmapped first copy was not retained: "),
            ("not_stage2_import",
             replace(retained, stage2_import="downgrade"),
             r"^unmapped first copy was not retained: "),
            ("terminal",
             replace(retained, final_status="imported", keep_searching=False),
             r"^unmapped first copy became terminal: "),
            ("claimed_ceiling",
             replace(retained, stage3_quality_gate="accept"),
             r"^unmapped first copy claimed a quality ceiling: "),
            ("narrowed",
             replace(retained, search_filetype_override_after="lossless"),
             r"^unmapped first copy narrowed to lossless: "),
        ):
            with self.subTest(clause=label), self.assertRaisesRegex(
                    AssertionError, expected):
                assert_unmapped_first_copy_stays_searchable(bad)

    def test_classification_checker_trips_on_bad_verdict(self):
        # A dict claiming both imported and a reject-stage decision would
        # classify confident_reject while imported — the checker must trip.
        bad = {
            "imported": True,
            "stage2_import": "downgrade",
            "stage3_quality_gate": None,
        }
        with self.assertRaisesRegex(
                AssertionError,
                r"^imported decision classified as \('confident_reject', "
                r"cleanup_eligible=True\)$"):
            assert_classification_coherent(bad, None)

    def test_classification_checker_trips_on_misnamed_fact(self):
        # An audio-corrupt early exit whose dict carries the wrong reject
        # value yields a quality-flavoured name instead of the fact name.
        bad = {
            "preimport_audio": "reject_nested",  # planted wrong value
            "imported": False,
        }
        with self.assertRaisesRegex(
                AssertionError,
                r"^integrity fact audio_corrupt classified as "
                r"\('uncertain', False, 'unknown'\)$"):
            assert_classification_coherent(bad, "preimport_audio")

    def test_classification_checker_trips_on_a_fact_named_for_dispatch(self):
        """The name clause, which the classification clause short-circuits past.

        Before issue #1355 item 1, ``classify_full_pipeline_decision`` and
        ``evidence_decision_name`` independently encoded the audio_corrupt-
        vs-nested_layout precedence and disagreed on a dict carrying both
        facts — the same class of bug the issue exists to remove. Both now
        route through the one shared ``preimport_corrupt_outranks_nested``
        precedence and neither twin's dict can carry both keys as reject
        values, so no real dict can trigger this clause any more. Exercise
        it through the checker's own name_fn injection seam instead, on an
        ordinary single-fact dict.
        """
        nested_only = {
            "preimport_nested": "reject_nested",
            "imported": False,
        }
        with self.assertRaisesRegex(
                AssertionError,
                r"^integrity fact nested_layout named 'audio_corrupt' for "
                r"dispatch$"):
            assert_classification_coherent(
                nested_only, "preimport_nested",
                name_fn=lambda _decision: "audio_corrupt")

    def test_classification_checker_trips_on_a_planted_classifier(self):
        """The three clauses keyed on what the classifiers RETURN.

        No decision dict can reach them: ``classify_full_pipeline_decision``
        only ever returns one of three verdicts and only ever pairs
        ``cleanup_eligible=True`` with ``confident_reject``, and
        ``evidence_decision_name`` is typed ``str`` and never returns an empty
        one. They are fail-closed legislation over the classifier pair, so the
        world that fires them is a planted classifier, injected through the
        checker's own kwarg seam rather than patched.
        """
        clean = {"imported": True, "stage2_import": "import"}
        assert_classification_coherent(clean, None)

        with self.assertRaisesRegex(
                AssertionError, r"^unknown classification verdict: 'vibes'$"):
            assert_classification_coherent(
                clean, None,
                classify_fn=lambda _decision: ("vibes", False, None))

        with self.assertRaisesRegex(
                AssertionError, r"^evidence_decision_name returned ''$"):
            assert_classification_coherent(
                clean, None, name_fn=lambda _decision: "")

        with self.assertRaisesRegex(
                AssertionError,
                r"^cleanup_eligible without confident_reject: "
                r"'would_import'/'import'$"):
            assert_classification_coherent(
                clean, None,
                classify_fn=lambda _decision: ("would_import", True, "import"))

    def test_integrity_fact_builder_refuses_an_unknown_fact(self):
        """The generated taxonomy's own fall-through, proven.

        ``_INTEGRITY_FACTS`` drives the strategy; a fact added there and not
        here must fail loudly rather than silently return a clean candidate.
        """
        candidate = build_parity_candidate_evidence(
            is_flac=False, min_bitrate=245, is_cbr=False)
        for fact in _INTEGRITY_FACTS:
            with self.subTest(fact=fact):
                self.assertIsNotNone(_with_integrity_fact(candidate, fact))
        with self.assertRaisesRegex(
                AssertionError,
                r"^unknown generated integrity fact: not_a_real_fact$"):
            _with_integrity_fact(candidate, "not_a_real_fact")

    def _planted_basis(self, **overrides):
        basis = {
            "verdict": "better", "branch": "rank",
            "new_rank": "transparent", "existing_rank": "good",
            "new_metric": "avg", "existing_metric": "avg",
            "new_value_kbps": 288, "existing_value_kbps": 196,
            "new_format": "MP3", "existing_format": "MP3",
            "spectral_clamped": False, "tolerance_kbps": None,
            "verified_lossless_bypass": False,
        }
        basis.update(overrides)
        return basis

    def _result_with_basis(self, stage2, basis):
        return SimResult(
            imported=stage2 in ("import", "transcode_upgrade"),
            keep_searching=True, denylisted=False, final_status="wanted",
            stage0_spectral_gate=None, stage1_spectral=None,
            stage2_import=stage2, stage3_quality_gate=None,
            backfill_override=None, search_filetype_override_after=None,
            comparison_basis=basis)

    def test_basis_checker_trips_on_every_clause(self):
        """All twelve basis clauses, each proven by its own message.

        The checker raises rather than accumulating, so the four clauses that
        used to have a self-test only ever proved the four they reached first;
        the remaining six were unfalsified.
        """
        for label, stage2, basis, expected in (
            ("lost_basis", "downgrade", None,
             r"^stage2='downgrade' requires a comparison but lost its basis$"),
            ("non_compared", "verified_lossless_locked", self._planted_basis(),
             (r"^basis present on non-compared stage2 "
              r"'verified_lossless_locked'$")),
            ("transcode_first", "transcode_first", self._planted_basis(),
             r"^basis present on non-compared stage2 'transcode_first'$"),
            ("unknown_branch", "import", self._planted_basis(branch="vibes"),
             r"^unknown basis branch: 'vibes'$"),
            ("malformed_metric", "import",
             self._planted_basis(new_metric="p95"),
             r"^malformed basis metrics: "),
            ("import_contradiction", "import",
             self._planted_basis(verdict="worse"),
             r"^import decision contradicts basis verdict: "),
            ("reject_contradiction", "downgrade",
             self._planted_basis(verdict="better"),
             r"^reject decision contradicts basis verdict: "),
            ("reject_claims_bypass", "downgrade",
             self._planted_basis(verdict="worse",
                                 verified_lossless_bypass=True),
             r"^reject decision claims a verified-lossless bypass: "),
            ("rank_branch_equal_ranks", "import",
             self._planted_basis(existing_rank="transparent"),
             r"^rank branch with equal ranks: "),
            ("same_rank_branch_differing_ranks", "import",
             self._planted_basis(branch="metric_tiebreak"),
             r"^same-rank branch with differing ranks: "),
            ("transcode_regression_not_worse", "import",
             self._planted_basis(branch="transcode_rank_regression"),
             r"^transcode rank regression must be worse: "),
            # Issue #1145 H2's two clauses. The first is reached with a
            # verdict the branch allows, so only the equal-ranks clause can
            # fire; the second keeps the ranks differing so only the verdict
            # clause can.
            ("within_tolerance_equal_ranks", "downgrade",
             self._planted_basis(branch="rank_within_tolerance",
                                 verdict="equivalent",
                                 existing_rank="transparent"),
             r"^within-tolerance branch with equal ranks: "),
            ("within_tolerance_not_equivalent", "downgrade",
             self._planted_basis(branch="rank_within_tolerance",
                                 verdict="worse"),
             r"^within-tolerance branch is not equivalent: "),
        ):
            with self.subTest(clause=label), self.assertRaisesRegex(
                    AssertionError, expected):
                assert_basis_consistent(
                    self._result_with_basis(stage2, basis))

    def test_measured_decision_basis_checker_trips_on_a_lost_basis(self):
        """The extracted per-property clause, proven directly."""
        compared = simulate(
            AlbumState("transparent_mp3", 320, True, None, None, False, None,
                       existing_format="MP3", avg_bitrate=320),
            DownloadScenario("candidate", is_flac=False, min_bitrate=192,
                             is_cbr=True, avg_bitrate=192, new_format="MP3"))
        assert compared.comparison_basis is not None, repr(compared)
        assert_measured_decision_carries_basis(compared)
        with self.assertRaisesRegex(
                AssertionError,
                r"^measured decision 'downgrade' against an existing album "
                r"lost its comparison basis: "):
            assert_measured_decision_carries_basis(
                replace(compared, comparison_basis=None))

    def test_metric_truthfulness_trips_on_fabricated_flac_avg(self):
        # The dl 36660 shape: a FLAC-source world whose basis claims the
        # candidate classified an "avg" — no real avg crosses the flat
        # interface on the FLAC paths.
        album = AlbumState(
            "planted", 256, False, None, None, False, None,
            existing_format="AAC", avg_bitrate=256)
        download = DownloadScenario(
            "planted", is_flac=True, min_bitrate=0, is_cbr=False,
            post_conversion_min_bitrate=216, converted_count=14)
        bad = self._planted_basis(
            new_metric="avg", new_value_kbps=216,
            branch="cross_family_same_rank", verdict="equivalent",
            new_rank="transparent", existing_rank="transparent")
        with self.assertRaisesRegex(
                AssertionError,
                r"^candidate basis claims 'avg' but the world measured none: "):
            assert_basis_metrics_truthful(
                album, download, self._result_with_basis("downgrade", bad))

    def test_metric_truthfulness_trips_on_fabricated_existing_avg(self):
        album = AlbumState(
            "planted", 256, False, None, None, False, None,
            existing_format="MP3", avg_bitrate=None)
        download = DownloadScenario(
            "planted", is_flac=False, min_bitrate=200, is_cbr=False,
            avg_bitrate=245)
        bad = self._planted_basis(existing_metric="avg")
        with self.assertRaisesRegex(
                AssertionError,
                r"^existing basis claims 'avg' but the album measured none: "):
            assert_basis_metrics_truthful(
                album, download, self._result_with_basis("import", bad))

    def test_metric_truthfulness_trips_on_median_claim(self):
        album = AlbumState(
            "planted", 256, False, None, None, False, None,
            existing_format="MP3", avg_bitrate=256)
        download = DownloadScenario(
            "planted", is_flac=False, min_bitrate=200, is_cbr=False,
            avg_bitrate=245)
        bad = self._planted_basis(new_metric="median")
        with self.assertRaisesRegex(
                AssertionError,
                r"^median never crosses the flat interface: "):
            assert_basis_metrics_truthful(
                album, download, self._result_with_basis("import", bad))

    def test_metric_truthfulness_passes_honest_labels(self):
        album = AlbumState(
            "planted", 194, False, None, None, False, None,
            existing_format="MP3", avg_bitrate=196)
        download = DownloadScenario(
            "planted", is_flac=False, min_bitrate=194, is_cbr=False,
            avg_bitrate=288)
        good = self._planted_basis()
        assert_basis_metrics_truthful(
            album, download, self._result_with_basis("import", good))

    def test_basis_checker_passes_a_coherent_basis(self):
        good = self._planted_basis()
        assert_basis_consistent(self._result_with_basis("import", good))

    def test_parity_checker_trips_on_divergence(self):
        sim = _planted_bad_import()
        evidence_result = {field: getattr(sim, field) for field in _PARITY_FIELDS}
        evidence_result["stage2_import"] = "downgrade"
        evidence_result["imported"] = False
        with self.assertRaisesRegex(
                AssertionError,
                r"^decision twins diverged on the same world:\n"
                r"  imported: simulator=True evidence=False\n"
                r"  stage2_import: simulator='import' evidence='downgrade'$"):
            assert_twins_agree(sim, evidence_result)

    def test_hypothesis_harness_detects_planted_bad_decider(self):
        """End-to-end RED proof: strategies + checker + Hypothesis catch a
        decider that always imports."""

        @given(album=raw_verified_lossless_albums(),
               download=lossy_downloads())
        @settings(max_examples=5, derandomize=True, database=None)
        def prop(album, download):
            # The world reaches the planted decider, which ignores it.
            assert_lossy_not_imported_over_verified_lossless(
                _planted_bad_import(album, download))

        with self.assertRaisesRegex(
                AssertionError,
                r"lossy candidate imported over raw verified-lossless FLAC: "):
            prop()


if __name__ == "__main__":
    unittest.main()
