"""Generated (property-based) quality-decision tests — issue #548.

Hypothesis-driven properties over the quality decision twins:

* ``full_pipeline_decision`` — the flat-kwargs simulator twin, driven
  through ``simulate()`` (the canonical scenario language of the album
  test set).
* ``full_pipeline_decision_from_evidence`` — the production decider,
  driven through the shared parity builders in ``tests/helpers.py``.

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
import sys
import unittest
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Never

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec
from hypothesis import assume, example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.dispatch.quality_gate import QualityGatePlan, _check_quality_gate_core
from lib.dispatch.types import QualityGateState
from lib.quality import (
    CODEC_FAMILY_MP3,
    COMPARISON_BASIS_BRANCHES,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    QUALITY_UPGRADE_TIERS,
    AlbumQualityEvidence,
    AlbumQualityEvidenceDecisionFacts,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    CodecFamily,
    QualityComparisonBasis,
    QualityRank,
    QualityRankConfig,
    SpectralCodecContext,
    SpectralComparability,
    SpectralEvidenceFacts,
    TargetQualityContract,
    VerifiedLosslessProof,
    classify_full_pipeline_decision,
    compute_effective_override_bitrate,
    decision_class_kbps,
    determine_verified_lossless,
    evidence_decision_name,
    full_pipeline_decision,
    full_pipeline_decision_from_evidence,
    interpret_spectral_evidence,
    ladder_class_kbps,
    legacy_unrecorded_audio_validation_report,
    quality_gate_decision,
    quality_rank,
    spectral_classes_comparable,
    spectral_import_decision,
)
from lib.quality.filetypes import has_mixed_lossless_and_lossy
from lib.spectral_check import MIN_CLIFF_SLICES, SLICE_FREQS
from tests.helpers import (
    build_parity_candidate_evidence,
    build_parity_current_evidence,
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
      ``cfg.mp3_vbr.excellent``), so ``stage1_spectral`` is a real verdict
      rather than ``None``;
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
    above ``cfg.mp3_vbr.excellent``), so Stage 1 has no verdict to
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
    if (branch in _BASIS_SAME_RANK_BRANCHES
            and basis["new_rank"] != basis["existing_rank"]):
        raise AssertionError(f"same-rank branch with differing ranks: {basis!r}")
    if branch == "transcode_rank_regression" and verdict != "worse":
        raise AssertionError(
            f"transcode rank regression must be worse: {basis!r}")


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

#: The average bitrate at or above which ``spectral_gate_trigger`` skips a
#: VBR MP3 — i.e. above which no Stage-1 verdict exists at all. READ from
#: the production config, exactly as ``full_pipeline_decision`` reads it
#: (``.claude/rules/test-fidelity.md`` Rule C: the trigger comes from the
#: producer, never a transcribed literal).
_PREIMPORT_GATE_VBR_THRESHOLD = QualityRankConfig.defaults().mp3_vbr.excellent


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
    branch alone can never disagree — while ``rank``,
    ``spectral_candidate_bound`` and ``metric_tiebreak`` all can, and are
    all reachable cross-codec. Measured over a 46,286-world sweep of
    MP3-candidate worlds simulating the pre-PR2b Stage-1 seam: 1,142 worlds
    flipped Stage 1 to ``"reject"`` and 326 of those carried a Stage-2
    ``"better"``, predominantly through ``rank``. That domain is patrolled by
    ``inadmissible_spectral_pair_worlds`` /
    ``test_stage1_never_consumes_an_inadmissible_existing_class``, whose
    checker forbids the Stage-1 rejection outright and therefore subsumes
    this checker there.

    **The shared format is MP3, and a VBR candidate's container stays
    below the preimport gate's threshold** (issue #829 Phase 5 PR2d). Both
    narrowings are the same coverage argument the paragraphs above already
    make, applied to facts the harness could not see until it started
    driving the real decider. ``spectral_gate_trigger`` fires the preimport
    gate ONLY for an MP3 candidate, and skips a VBR MP3 whose average is at
    or above ``cfg.mp3_vbr.excellent``; outside that domain
    ``stage1_spectral`` is ``None`` and the disagreement this property
    hunts is unreachable by construction — not "safe", *absent*. The old
    inline harness computed a Stage-1 verdict regardless, which is how
    Vorbis and high-average VBR worlds looked like coverage while
    production never ran Stage 1 on them at all: over 5,000 draws of the
    pre-PR2d strategy, 3,189 (63.8%) were worlds production gate-skips
    (2,347 uncalibrated-codec, 842 high-average VBR). The strategy below
    measures 0/5,000. The threshold is READ from the production config,
    never transcribed (``.claude/rules/test-fidelity.md`` Rule C).

    Vorbis is consequently no longer drawn HERE. Its ladder is still real
    and a same-Vorbis pair is still comparable at Stage 2 — that is
    patrolled by the parity property in ``TestGeneratedParity`` and the
    cross-codec property below; what a Vorbis candidate cannot do is reach
    Stage 1.
    """
    new_is_cbr = draw(st.booleans())
    # A VBR MP3 at or above the threshold is gate-skipped, so its Stage-1
    # verdict does not exist. Draw inside the domain where it does.
    new_container = draw(_bitrates(
        min_value=1,
        max_value=3000 if new_is_cbr else _PREIMPORT_GATE_VBR_THRESHOLD - 1,
    ))
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
    # A VBR MP3 at or above the gate threshold is skipped, so its class must
    # fit below that threshold too (the class never exceeds its container).
    class_ceiling = 3000 if new_is_cbr else _PREIMPORT_GATE_VBR_THRESHOLD - 1
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
    # The preimport gate must fire for Stage 1 to produce a verdict at all:
    # MP3 CBR always, MP3 VBR only below ``cfg.mp3_vbr.excellent`` (210).
    new_is_cbr = draw(st.booleans())
    new_container = draw(
        _bitrates(min_value=1, max_value=3000) if new_is_cbr
        else _bitrates(min_value=1, max_value=209)
    )
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


_TRANSPARENT_EXISTING_SHAPES = (
    # (min_bitrate, avg_bitrate, is_cbr) — MP3 320 CBR and MP3 V0.
    (320, 320, True),
    (245, 245, False),
)


@st.composite
def transparent_mp3_albums(draw) -> AlbumState:
    min_br, avg_br, is_cbr = draw(st.sampled_from(_TRANSPARENT_EXISTING_SHAPES))
    return AlbumState(
        name="generated_transparent_mp3",
        min_bitrate=min_br,
        is_cbr=is_cbr,
        spectral_grade="genuine",
        spectral_bitrate=None,
        verified_lossless=False,
        search_filetype_override=None,
        existing_format="MP3",
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
                    state_loader=lambda state=state, **_kwargs: state,
                )
                self.assertIsNotNone(plan)
                assert plan is not None
                raw_override = plan.transition.fields.get(
                    "search_filetype_override",
                )
                if raw_override is not None and not isinstance(raw_override, str):
                    raise AssertionError(
                        "quality gate wrote a non-string override: "
                        f"{raw_override!r}"
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
    # follow: a VBR MP3 is gate-skipped at or above ``cfg.mp3_vbr.excellent``
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
        levered = _stage_parity_deferred_decision(world)
        if (
            decision["stage2_import"] != levered["stage2_import"]
            or decision["comparison_basis"] != levered["comparison_basis"]
        ):
            raise AssertionError(
                "the Stage-1 carve-out lever moved Stage 2: "
                f"{decision['stage2_import']!r}/{decision['comparison_basis']!r} "
                f"vs {levered['stage2_import']!r}/"
                f"{levered['comparison_basis']!r} [{world!r}]"
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
                world.existing_format, class_kbps, True,
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
        if (
            result.stage2_import in (
                "import",
                "downgrade",
                "transcode_upgrade",
                "transcode_downgrade",
            )
            and result.comparison_basis is None
        ):
            raise AssertionError(
                f"measured decision {result.stage2_import!r} against an "
                    f"existing album lost its comparison basis: {result!r}")
        assert_basis_consistent(result)


# ===========================================================================
# Parity property — the twins must agree on every world both can express.
#
# The world space here is the twins' COMMON language, i.e. exactly what the
# shared parity builders (tests/helpers.py) can encode:
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
    )
    return simulate(
        album, download,
        verified_lossless_target=world.verified_lossless_target,
        current_verified_lossless_proof=world.current_verified_lossless_proof,
    )


def _parity_evidence_result(world: ParityWorld) -> dict:
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
        post_conversion_min_bitrate=world.post_conversion_min_bitrate,
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
    )
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
        proof_world = replace(
            world,
            current_min=(world.current_min or 245),
            current_avg=(world.current_avg or 245),
            current_verified_lossless_proof=True,
        )
        sim = _parity_simulator_result(proof_world)
        evidence_result = _parity_evidence_result(proof_world)
        assert_verified_lossless_proof_locks_candidate(sim)
        assert_twins_agree(sim, evidence_result)


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
    decision: dict, expected_early_exit_key: str | None) -> None:
    """The classification layer (cleanup eligibility + dispatch decision
    name) must be coherent with the decision dict it classifies.

    Added after the fuzz-tier coverage diagnostic showed
    ``classify_full_pipeline_decision`` / ``evidence_decision_name``
    (which gate wrong-match folder cleanup) were the one decision-policy
    layer no generated test reached.
    """
    verdict, cleanup_eligible, reason = classify_full_pipeline_decision(decision)
    name = evidence_decision_name(decision)
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


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: prove the harness detects what it claims to."""

    def test_definitive_checker_trips_on_bogus_status(self):
        bad = SimResult(
            imported=False, keep_searching=False, denylisted=False,
            final_status=None, stage0_spectral_gate=None,
            stage1_spectral=None, stage2_import=None,
            stage3_quality_gate=None, backfill_override=None,
            search_filetype_override_after=None)
        with self.assertRaises(AssertionError):
            assert_decision_is_definitive(bad)

    def test_verified_lossless_checker_trips_on_import(self):
        with self.assertRaises(AssertionError):
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
        with self.assertRaises(AssertionError):
            assert_verified_lossless_proof_locks_candidate(mutant)

    def test_evidence_proof_lock_checker_trips_on_integrity_reject(self):
        with self.assertRaises(AssertionError):
            assert_evidence_proof_lock_preserves_imported({
                "stage2_import": None,
                "final_status": "wanted",
                "imported": False,
                "denylisted": True,
                "keep_searching": True,
                "preimport_audio": "reject_corrupt",
                "preimport_bad_hash": None,
                "preimport_nested": None,
                "preimport_empty_fileset": None,
                "preimport_mixed_source": None,
            })

    def test_downgrade_checker_trips_on_accept(self):
        with self.assertRaises(AssertionError):
            assert_obvious_downgrade_not_accepted(_planted_bad_import())

    def test_unverified_lossy_checker_trips_on_terminal_import(self):
        with self.assertRaises(AssertionError):
            assert_unverified_lossy_never_terminal(_planted_bad_import())

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
            with self.subTest(field=field), self.assertRaises(AssertionError):
                assert_post_import_action_matches(**kwargs)

    def test_quality_failure_checker_trips_on_terminal_acceptance(self):
        from lib import transitions

        bad = QualityGatePlan(
            transition=transitions.RequestTransition.to_imported(
                from_status="imported",
            ),
            successful_terminal_acceptance=True,
        )
        with self.assertRaises(AssertionError):
            assert_quality_decision_failure_reopens_full_tier(bad)

    def test_affirmative_verification_checker_trips_on_absent_evidence(self):
        with self.assertRaises(AssertionError):
            assert_verified_lossless_has_affirmative_evidence(
                True,
                spectral_grade=None,
                v0_probe_kind="lossless_source_v0",
                v0_avg=300,
                v0_min=250,
            )

    def test_strictly_lower_spectral_checker_trips_on_violations(self):
        # Planted tie-reject (the Mark DeNardo bug): equal floor rejected.
        with self.assertRaises(AssertionError):
            assert_only_strictly_lower_spectral_rejects(
                "reject",
                grade="suspect",
                new_spectral=128,
                existing_spectral=128,
            )
        # Planted strictly-lower non-reject: worse content that failed to reject.
        with self.assertRaises(AssertionError):
            assert_only_strictly_lower_spectral_rejects(
                "import",
                grade="likely_transcode",
                new_spectral=96,
                existing_spectral=128,
            )

    def test_existing_override_noop_checker_trips_on_divergence(self):
        # Planted phantom upgrade (the Deerhunter bug): applying the existing-
        # side spectral override flips the verdict from equivalent to better.
        with self.assertRaises(AssertionError):
            assert_existing_override_noop_under_shared_clamp(
                {"stage2_import": "import",
                 "comparison_basis": {"verdict": "better"}},
                {"stage2_import": "downgrade",
                 "comparison_basis": {"verdict": "equivalent"}},
            )
        # A stage2-only divergence must also trip (verdict alone is not enough).
        with self.assertRaises(AssertionError):
            assert_existing_override_noop_under_shared_clamp(
                {"stage2_import": "import",
                 "comparison_basis": {"verdict": "better"}},
                {"stage2_import": "downgrade",
                 "comparison_basis": {"verdict": "better"}},
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
        with self.assertRaises(AssertionError):
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
            with self.subTest(field=field), self.assertRaises(AssertionError):
                assert_stage1_reject_leaks_no_stage2_state(
                    self._stage1_reject_decision(**{field: leaked}))

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
                    self.assertRaises(AssertionError):
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
        with self.assertRaises(AssertionError):
            assert_counterfactual_reported_exactly_when_stage1_short_circuits(
                dropped)

        doubled = dict(deferring)
        doubled["stage2_import_if_stage1_deferred"] = "downgrade"
        with self.assertRaises(AssertionError):
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
        with self.assertRaises(AssertionError):
            assert_counterfactual_reported_exactly_when_stage1_short_circuits(
                silent)

    def test_have_representation_checker_trips(self):
        """Issue #829 PR2c item 6 known-bad self-test.

        The planted violation is the download_log 29525 world exactly: a
        HAVE ranked ``transparent`` on its 320 container while its own
        cliff-derived class of 128 ranks ``acceptable``.
        """
        with self.assertRaises(AssertionError) as caught:
            assert_have_is_represented_by_its_own_class(
                "transparent", "acceptable")
        self.assertIn("ranked above its own spectral class",
                      str(caught.exception))
        # One tier over is still a violation.
        with self.assertRaises(AssertionError):
            assert_have_is_represented_by_its_own_class("good", "acceptable")
        # Equal is the normal clamped/overridden case, and BELOW the class
        # is fine too — the raw metric can be the tighter of the two.
        assert_have_is_represented_by_its_own_class(
            "acceptable", "acceptable")
        assert_have_is_represented_by_its_own_class("poor", "transparent")

    def test_inadmissible_existing_class_checker_trips(self):
        """Issue #829 PR2c known-bad self-test — both clauses trip."""
        # Clause 1: a Stage-1 rejection built on an inadmissible pair.
        with self.assertRaises(AssertionError) as caught:
            assert_stage1_ignores_inadmissible_existing_spectral(
                "reject", "import_no_exist")
        self.assertIn("not permitted to make", str(caught.exception))
        # Clause 2: the silent direction — no rejection, but the withheld
        # evidence still moved the verdict.
        with self.assertRaises(AssertionError) as caught:
            assert_stage1_ignores_inadmissible_existing_spectral(
                "import_upgrade", "import_no_exist")
        self.assertIn("changed the Stage 1 verdict", str(caught.exception))
        # A rejection trips even when the withheld run rejects too — a
        # mutant that fabricates a class from nothing must not slip through
        # the equality clause.
        with self.assertRaises(AssertionError):
            assert_stage1_ignores_inadmissible_existing_spectral(
                "reject", "reject")
        # Invariant worlds must NOT trip, including the gate-skipped shape.
        assert_stage1_ignores_inadmissible_existing_spectral(
            "import_no_exist", "import_no_exist")
        assert_stage1_ignores_inadmissible_existing_spectral("import", "import")
        assert_stage1_ignores_inadmissible_existing_spectral(None, None)

    def test_unmapped_codec_checker_trips_on_terminal_narrowing(self):
        bad = SimResult(
            imported=True,
            keep_searching=False,
            denylisted=False,
            final_status="imported",
            stage0_spectral_gate="skip_vbr_high",
            stage1_spectral=None,
            stage2_import="import",
            stage3_quality_gate="accept",
            backfill_override=None,
            search_filetype_override_after="lossless",
        )
        with self.assertRaises(AssertionError):
            assert_unmapped_first_copy_stays_searchable(bad)

    def test_classification_checker_trips_on_bad_verdict(self):
        # A dict claiming both imported and a reject-stage decision would
        # classify confident_reject while imported — the checker must trip.
        bad = {
            "imported": True,
            "stage2_import": "downgrade",
            "stage3_quality_gate": None,
        }
        with self.assertRaises(AssertionError):
            assert_classification_coherent(bad, None)

    def test_classification_checker_trips_on_misnamed_fact(self):
        # An audio-corrupt early exit whose dict carries the wrong reject
        # value yields a quality-flavoured name instead of the fact name.
        bad = {
            "preimport_audio": "reject_nested",  # planted wrong value
            "imported": False,
        }
        with self.assertRaises(AssertionError):
            assert_classification_coherent(bad, "preimport_audio")

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

    def test_basis_checker_trips_on_lost_basis(self):
        with self.assertRaises(AssertionError):
            assert_basis_consistent(self._result_with_basis("downgrade", None))

    def test_basis_checker_trips_on_verdict_contradiction(self):
        bad = self._planted_basis(verdict="worse")
        with self.assertRaises(AssertionError):
            assert_basis_consistent(self._result_with_basis("import", bad))

    def test_basis_checker_trips_on_rank_incoherence(self):
        bad = self._planted_basis(existing_rank="transparent")
        with self.assertRaises(AssertionError):
            assert_basis_consistent(self._result_with_basis("import", bad))

    def test_basis_checker_trips_on_unknown_branch(self):
        bad = self._planted_basis(branch="vibes")
        with self.assertRaises(AssertionError):
            assert_basis_consistent(self._result_with_basis("import", bad))

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
        with self.assertRaises(AssertionError):
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
        with self.assertRaises(AssertionError):
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
        with self.assertRaises(AssertionError):
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
        with self.assertRaises(AssertionError):
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

        with self.assertRaises(AssertionError):
            prop()


if __name__ == "__main__":
    unittest.main()
