#!/usr/bin/env python3
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
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Never
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

import msgspec
from hypothesis import example, given, settings
from hypothesis import strategies as st

from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceDecisionFacts,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    COMPARISON_BASIS_BRANCHES,
    QUALITY_UPGRADE_TIERS,
    EVIDENCE_SUBJECT_SOURCE,
    EVIDENCE_SUBJECT_INSTALLED,
    QualityRankConfig,
    TargetQualityContract,
    VerifiedLosslessProof,
    classify_full_pipeline_decision,
    compute_effective_override_bitrate,
    determine_verified_lossless,
    evidence_decision_name,
    full_pipeline_decision_from_evidence,
    quality_gate_decision,
)
from lib.dispatch.quality_gate import QualityGatePlan, _check_quality_gate_core
from lib.dispatch.types import QualityGateState
from lib.quality.filetypes import has_mixed_lossless_and_lossy
from tests.helpers import (
    build_parity_candidate_evidence,
    build_parity_current_evidence,
)
from tests.test_simulator_scenarios import (
    AlbumState,
    DownloadScenario,
    SimResult,
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
        raise AssertionError(f"imported is not bool: {result.imported!r}")
    if not isinstance(result.keep_searching, bool):
        raise AssertionError(
            f"keep_searching is not bool: {result.keep_searching!r}")
    if not isinstance(result.denylisted, bool):
        raise AssertionError(f"denylisted is not bool: {result.denylisted!r}")
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


_MEASURED_STAGE2_DECISIONS = frozenset({
    "import", "downgrade", "transcode_upgrade", "transcode_downgrade",
    "transcode_first",
})
_BASIS_SAME_RANK_BRANCHES = frozenset({
    "lossless_same_rank", "cross_family_same_rank",
    "label_contract_same_rank", "metric_tiebreak", "metric_missing",
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
            album.min_bitrate, album.spectral_bitrate, album.spectral_grade,
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

    @given(decision=st.sampled_from(tuple(_POST_IMPORT_EXPECTATIONS)))
    def test_real_quality_gate_matches_post_import_action_table(self, decision):
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
            db=SimpleNamespace(),  # type: ignore[arg-type]
            apply=False,
            state_loader=lambda **_kwargs: state,
        )
        self.assertIsNotNone(plan)
        assert plan is not None
        raw_override = plan.transition.fields.get("search_filetype_override")
        if raw_override is not None and not isinstance(raw_override, str):
            raise AssertionError(
                f"quality gate wrote a non-string override: {raw_override!r}"
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

        with patch("lib.dispatch.quality_gate.logger.exception"):
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
        assert_quality_decision_failure_reopens_full_tier(plan)

    @given(subject=st.sampled_from((
        EVIDENCE_SUBJECT_SOURCE,
        EVIDENCE_SUBJECT_INSTALLED,
    )))
    def test_lossless_narrowing_is_subject_blind_for_genuine_transparent(
        self, subject
    ):
        # Decision 17: the transparent+genuine narrowing rule keys on the
        # grade, never the subject label — an unconverted import's
        # source-subject grade describes the installed bytes.
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
        if result.stage2_import in ("import", "downgrade",
                                    "transcode_upgrade",
                                    "transcode_downgrade"):
            if result.comparison_basis is None:
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
    return AlbumQualityEvidence(
        mb_release_id="generated-evidence",
        snapshot_fingerprint="sha256:generated-fingerprint",
        source_path="/Incoming/auto-import/generated",
        measurement=measurement,
        measured_at=datetime(2026, 7, 8, tzinfo=timezone.utc),
        files=files,
        codec=codec,
        container=container,
        storage_format=measured_format,
        target_format=target_format,
        target_is_cbr=target_is_cbr,
        v0_metric=v0_metric,
        verified_lossless_proof=proof,
        audio_corrupt=draw(st.booleans()),
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
        audio_corrupt=False,
        folder_layout="flat",
        audio_file_count=1,
        matched_bad_audio_hash_id=None,
        matched_bad_audio_hash_path=None,
    )
    if fact == "audio_corrupt":
        return msgspec.structs.replace(clean, audio_corrupt=True)
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

def _planted_bad_import() -> SimResult:
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
            del album, download  # the planted decider ignores its world
            assert_lossy_not_imported_over_verified_lossless(
                _planted_bad_import())

        with self.assertRaises(AssertionError):
            prop()


if __name__ == "__main__":
    unittest.main()
