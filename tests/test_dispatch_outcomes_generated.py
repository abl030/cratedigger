#!/usr/bin/env python3
"""Generated (property-based) dispatch/outcome tests — issue #548.

Hypothesis-driven properties over the importer dispatch/outcome layer:
``lib/dispatch/core.py::dispatch_import_core`` (the funnel every import path
runs through) and ``lib/dispatch/outcome_actions.py::_reject_import_from_evidence_decision``
(the unified reject helper that owns the U11 forced-requeue override).

Two harnesses, both lifted verbatim from the established hand-written
recipes (no new scaffolding):

* ``_run_dispatch`` mirrors
  ``tests/test_dispatch_core.py::TestDispatchCoreOrchestration._dispatch`` —
  a fresh ``FakePipelineDB`` + ``patch_dispatch_externals()`` +
  ``patch("lib.dispatch.subprocess_runner.parse_import_result", ...)`` to
  feed a generated ``ImportResult`` decision (or ``None`` for the
  "no JSON" crash path) into the real ``dispatch_import_core``.
* ``_reject_via_evidence_decision`` mirrors
  ``tests/test_import_dispatch.py::TestRejectImportFromEvidenceDecisionForcedRequeue._reject`` —
  drives the real ``_reject_import_from_evidence_decision`` directly with a
  generated ``decision`` string, generalizing that class's 4-decision
  hand-written table to the FULL production
  ``_PREIMPORT_FACT_REJECT_DECISIONS`` frozenset (5 entries — the
  hand-written table is missing ``mixed_source``).

Two tiers, selected by ``CRATEDIGGER_HYPOTHESIS_PROFILE`` (see
``tests/_hypothesis_profiles.py``):

* ``suite`` (default) — deterministic, bounded; runs on every
  ``scripts/run_tests.sh`` like any other test.
* ``fuzz`` — randomized burst for local exploration::

      nix-shell --run "CRATEDIGGER_HYPOTHESIS_PROFILE=fuzz \\
          python3 -m unittest tests.test_dispatch_outcomes_generated -v"

Full usage guide: docs/generated-testing.md.
"""

import os
import shutil
import sys
import tempfile
import unittest
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given, settings
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.dispatch import DispatchOutcome
from lib.dispatch.types import _PREIMPORT_FACT_REJECT_DECISIONS
from lib.quality import (
    QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
    QUALITY_DECISION_REJECT_STAGE_DECISIONS,
    DownloadInfo,
    dispatch_action,
)
from tests.fakes import DownloadLogRow, FakePipelineDB
from tests.helpers import (
    make_import_result,
    make_request_row,
    noop_quality_gate,
    patch_dispatch_externals,
)

_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"
_GRADES = ("genuine", "marginal", "suspect", "likely_transcode")

# Every decision string the legacy (subprocess-return) dispatch path can
# see in production, PLUS the five folder/audio-integrity fact names —
# those never actually arrive via ``ir.decision`` in production (they are
# evidence-pipeline early exits, exercised separately below), but
# ``dispatch_action`` routes them identically regardless of origin, so
# driving them through here proves the legacy path degrades safely on an
# unexpected decision string instead of mis-routing it.
_KNOWN_DECISIONS = tuple(sorted(
    QUALITY_DECISION_IMPORT_STAGE_DECISIONS
    | QUALITY_DECISION_REJECT_STAGE_DECISIONS
    | _PREIMPORT_FACT_REJECT_DECISIONS
    | {"spectral_reject", "duplicate_remove_guard_failed",
       "totally_unmapped_decision"}
))


def _bitrates(min_value: int = 1, max_value: int = 3000) -> st.SearchStrategy[int]:
    return st.integers(min_value=min_value, max_value=max_value)


# ===========================================================================
# World + harness — legacy (subprocess-return) dispatch path.
# ===========================================================================

@dataclass(frozen=True)
class DispatchWorld:
    """One dispatch_import_core invocation: either a decision string
    returned by import_one.py, or the ``no_json`` crash-ish path (harness
    produced no ``__IMPORT_RESULT__`` sentinel)."""
    mode: str  # "decision" | "no_json"
    decision: str | None
    new_min_bitrate: int | None
    prev_min_bitrate: int | None
    spectral_grade: str
    spectral_bitrate: int | None
    was_converted: bool
    requeue_on_failure: bool
    source_username: str | None


@st.composite
def dispatch_worlds(draw) -> DispatchWorld:
    mode = draw(st.sampled_from(("decision", "no_json")))
    requeue_on_failure = draw(st.booleans())
    source_username = draw(st.sampled_from(("user1", "user2", "baduser", None)))
    if mode == "no_json":
        return DispatchWorld(
            mode="no_json", decision=None, new_min_bitrate=None,
            prev_min_bitrate=None, spectral_grade="genuine",
            spectral_bitrate=None, was_converted=False,
            requeue_on_failure=requeue_on_failure,
            source_username=source_username,
        )
    return DispatchWorld(
        mode="decision",
        decision=draw(st.sampled_from(_KNOWN_DECISIONS)),
        new_min_bitrate=draw(_bitrates()),
        prev_min_bitrate=draw(st.one_of(st.none(), _bitrates())),
        spectral_grade=draw(st.sampled_from(_GRADES)),
        spectral_bitrate=draw(st.one_of(st.none(), _bitrates(max_value=400))),
        was_converted=draw(st.booleans()),
        requeue_on_failure=requeue_on_failure,
        source_username=source_username,
    )


def _run_dispatch(world: DispatchWorld) -> dict:
    """Established recipe (mirrors
    ``tests/test_dispatch_core.py::TestDispatchCoreOrchestration._dispatch``)
    for driving the REAL ``dispatch_import_core`` with a generated decision
    fed through the ``parse_import_result`` seam."""
    from lib.dispatch import dispatch_import_core

    ir = None
    if world.mode == "decision":
        assert world.decision is not None and world.new_min_bitrate is not None
        ir = make_import_result(
            decision=world.decision,
            new_min_bitrate=world.new_min_bitrate,
            prev_min_bitrate=world.prev_min_bitrate,
            spectral_grade=world.spectral_grade,
            spectral_bitrate=world.spectral_bitrate,
            was_converted=world.was_converted,
        )

    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42, status="downloading",
        min_bitrate=180, current_spectral_bitrate=128,
    ))
    cfg = CratediggerConfig(
        beets_harness_path=_HARNESS,
        pipeline_db_enabled=True,
    )
    dl_info = DownloadInfo(username=world.source_username)

    tmpdir = tempfile.mkdtemp()
    try:
        with patch_dispatch_externals(), \
             patch("lib.dispatch.subprocess_runner.parse_import_result",
                   return_value=ir):
            result = dispatch_import_core(
                path=tmpdir,
                mb_release_id="mbid-generated",
                request_id=42,
                label="Generated Artist - Generated Album",
                beets_harness_path=cfg.beets_harness_path,
                db=db,  # type: ignore[arg-type]
                dl_info=dl_info,
                distance=0.05,
                scenario="strong_match",
                files=[MagicMock(username=world.source_username or "user1",
                                 filename="01 - Track.mp3")],
                cfg=cfg,
                requeue_on_failure=world.requeue_on_failure,
                quality_gate_fn=noop_quality_gate,
            )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    return {"db": db, "result": result}


# ===========================================================================
# World + harness — evidence-decision reject path (owns the U11 override).
# ===========================================================================

def _reject_via_evidence_decision(
    *, decision: str, requeue_on_failure: bool, new_min_bitrate: int,
    source_username: str | None = "user1",
) -> FakePipelineDB:
    """Established recipe (mirrors
    ``tests/test_import_dispatch.py::TestRejectImportFromEvidenceDecisionForcedRequeue._reject``)
    for driving the REAL ``_reject_import_from_evidence_decision`` directly
    with a generated ``decision`` string."""
    from lib.dispatch import _reject_import_from_evidence_decision

    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42, status="downloading", mb_release_id="test-mbid"))
    dl_info = DownloadInfo(filetype="mp3", username=source_username)
    ir = make_import_result(decision=decision, new_min_bitrate=new_min_bitrate)
    with patch_dispatch_externals():
        _reject_import_from_evidence_decision(
            db=db,  # type: ignore[arg-type]
            request_id=42,
            dl_info=dl_info,
            import_result=ir,
            distance=0.0,
            decision=decision,
            detail=f"generated {decision}",
            requeue_on_failure=requeue_on_failure,
            validation_result=None,
            staged_path="/tmp/cratedigger-generated-reject-test",
            scenario=decision,
            files=None,
            source_path_cleanup_scenario=decision,
            cooled_down_users=None,
        )
    return db


# ===========================================================================
# Invariant checkers — module functions so the known-bad self-tests below
# can prove each one trips on a violating outcome.
# ===========================================================================

def assert_download_log_row_created(db: FakePipelineDB, *, min_count: int = 1) -> None:
    """The auditability law (CLAUDE.md § code-quality.md): every download
    outcome — success, rejection, or crash — MUST create a download_log row
    with a real (non-empty) outcome string."""
    if len(db.download_logs) < min_count:
        raise AssertionError(
            f"expected >= {min_count} download_log row(s), got "
            f"{len(db.download_logs)}")
    last = db.download_logs[-1]
    if not last.outcome:
        raise AssertionError(
            f"download_log row has empty/None outcome: {last!r}")


def assert_dispatch_outcome_matches_routing(
    world: DispatchWorld, db: FakePipelineDB, outcome: DispatchOutcome,
) -> None:
    """The auditability + success/self-heal oracle for the legacy dispatch
    path: whatever ``dispatch_action(decision)`` prescribes is what actually
    landed in the DB — for the no-JSON crash path AND every known decision
    string.
    """
    assert_download_log_row_created(db)
    log = db.download_logs[-1]
    status = db.request(42)["status"]

    if world.mode == "no_json":
        if log.outcome != "failed":
            raise AssertionError(
                f"no-JSON crash logged outcome={log.outcome!r}, want 'failed'")
        expected_status = "wanted" if world.requeue_on_failure else "downloading"
        if status != expected_status:
            raise AssertionError(
                f"no-JSON crash requeue_on_failure={world.requeue_on_failure} "
                f"left status={status!r}, want {expected_status!r}")
        if outcome.success:
            raise AssertionError("no-JSON crash reported success=True")
        return

    assert world.decision is not None
    action = dispatch_action(world.decision)
    if action.mark_done:
        if log.outcome != "success":
            raise AssertionError(
                f"decision={world.decision!r} mark_done=True but logged "
                f"outcome={log.outcome!r}, want 'success'")
        if not outcome.success:
            raise AssertionError(
                f"decision={world.decision!r} mark_done=True but "
                "result.success=False")
        expected_status = "wanted" if action.requeue else "imported"
        if status != expected_status:
            raise AssertionError(
                f"decision={world.decision!r} mark_done=True left "
                f"status={status!r}, want {expected_status!r} "
                f"(action.requeue={action.requeue})")
    elif action.record_rejection:
        if log.outcome != "rejected":
            raise AssertionError(
                f"decision={world.decision!r} record_rejection=True but "
                f"logged outcome={log.outcome!r}, want 'rejected'")
        if outcome.success:
            raise AssertionError(
                f"decision={world.decision!r} reject reported success=True")
        expected_status = "wanted" if world.requeue_on_failure else "downloading"
        if status != expected_status:
            raise AssertionError(
                f"decision={world.decision!r} "
                f"requeue_on_failure={world.requeue_on_failure} left "
                f"status={status!r}, want {expected_status!r}")
    else:
        raise AssertionError(
            f"dispatch_action({world.decision!r}) sets neither mark_done "
            "nor record_rejection — dispatch_import_core has no routing "
            "for this outcome")


def assert_preimport_fact_always_self_heals(
    decision: str, db: FakePipelineDB,
) -> None:
    """U11 invariant: folder/audio-integrity facts fire upstream of any
    beets mutation and upstream of any operator intent — the parent request
    must always self-heal back to 'wanted', regardless of the caller's
    ``requeue_on_failure`` flag (force/manual paths pass False)."""
    status = db.request(42)["status"]
    if status != "wanted":
        raise AssertionError(
            f"preimport-fact reject {decision!r} left status={status!r}, "
            "want 'wanted' (U11 forced-requeue override must always "
            "self-heal — the album is still desired, only this source "
            "is bad)")


def assert_quality_side_reject_honors_caller_flag(
    decision: str, requeue_on_failure: bool, db: FakePipelineDB,
) -> None:
    """Quality-side rejects (downgrade, transcode_downgrade, suspect
    lossless, lossless_source_locked) are NOT in
    ``_PREIMPORT_FACT_REJECT_DECISIONS`` — they honor the caller's
    ``requeue_on_failure`` flag normally (force/manual paths that pass
    False stay put; the operator already chose to act on this source)."""
    status = db.request(42)["status"]
    expected = "wanted" if requeue_on_failure else "downloading"
    if status != expected:
        raise AssertionError(
            f"quality-side reject {decision!r} "
            f"requeue_on_failure={requeue_on_failure} left status="
            f"{status!r}, want {expected!r}")


# ===========================================================================
# Properties
# ===========================================================================

class TestGeneratedDispatchOutcomes(unittest.TestCase):
    """Properties over the legacy (subprocess-return) dispatch path."""

    @given(world=dispatch_worlds())
    def test_every_outcome_creates_a_download_log_row(self, world):
        outcome = _run_dispatch(world)
        assert_download_log_row_created(outcome["db"])

    @given(world=dispatch_worlds())
    def test_outcome_matches_dispatch_action_routing(self, world):
        outcome = _run_dispatch(world)
        assert_dispatch_outcome_matches_routing(
            world, outcome["db"], outcome["result"])


class TestGeneratedEvidenceRejectSelfHeal(unittest.TestCase):
    """Properties over ``_reject_import_from_evidence_decision`` — the U11
    forced-requeue override, generalized to the FULL production
    ``_PREIMPORT_FACT_REJECT_DECISIONS`` frozenset (5 entries; the
    hand-written ``TestRejectImportFromEvidenceDecisionForcedRequeue`` table
    only covers 4 — it is missing ``mixed_source``)."""

    @given(decision=st.sampled_from(sorted(_PREIMPORT_FACT_REJECT_DECISIONS)),
           requeue_on_failure=st.booleans(),
           new_min_bitrate=_bitrates())
    def test_preimport_facts_always_self_heal(
            self, decision, requeue_on_failure, new_min_bitrate):
        db = _reject_via_evidence_decision(
            decision=decision, requeue_on_failure=requeue_on_failure,
            new_min_bitrate=new_min_bitrate)
        assert_download_log_row_created(db)
        assert_preimport_fact_always_self_heals(decision, db)

    @given(decision=st.sampled_from(sorted(QUALITY_DECISION_REJECT_STAGE_DECISIONS)),
           requeue_on_failure=st.booleans(),
           new_min_bitrate=_bitrates())
    def test_quality_side_rejects_honor_caller_flag(
            self, decision, requeue_on_failure, new_min_bitrate):
        db = _reject_via_evidence_decision(
            decision=decision, requeue_on_failure=requeue_on_failure,
            new_min_bitrate=new_min_bitrate)
        assert_download_log_row_created(db)
        assert_quality_side_reject_honors_caller_flag(
            decision, requeue_on_failure, db)


# ===========================================================================
# Harness self-tests (RED/GREEN of the fuzzer itself) — each invariant
# checker must trip on a planted violation, and a planted-bad router must
# be caught end-to-end through the Hypothesis machinery.
# ===========================================================================

class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: prove the harness detects what it claims to."""

    def test_log_row_checker_trips_on_empty_db(self):
        db = FakePipelineDB()
        with self.assertRaises(AssertionError):
            assert_download_log_row_created(db)

    def test_log_row_checker_trips_on_blank_outcome(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        # Bypass log_download's outcome-taxonomy check to plant a row a
        # real writer could never produce — proves the checker itself
        # (not just the CHECK constraint mirror) catches an empty outcome.
        db.download_logs.append(DownloadLogRow(request_id=42, outcome=None))
        with self.assertRaises(AssertionError):
            assert_download_log_row_created(db)

    def test_routing_checker_trips_when_import_status_wrong(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        db.log_download(request_id=42, outcome="success")
        # Planted bug: an "import" decision (mark_done=True, requeue=False)
        # that never actually flipped the request to 'imported'.
        world = DispatchWorld(
            mode="decision", decision="import", new_min_bitrate=245,
            prev_min_bitrate=None, spectral_grade="genuine",
            spectral_bitrate=None, was_converted=False,
            requeue_on_failure=True, source_username="user1")
        outcome = DispatchOutcome(success=True, message="ok")
        with self.assertRaises(AssertionError):
            assert_dispatch_outcome_matches_routing(world, db, outcome)

    def test_routing_checker_trips_on_no_json_wrong_log_outcome(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        # A crash should log 'failed', not 'success'.
        db.log_download(request_id=42, outcome="success")
        world = DispatchWorld(
            mode="no_json", decision=None, new_min_bitrate=None,
            prev_min_bitrate=None, spectral_grade="genuine",
            spectral_bitrate=None, was_converted=False,
            requeue_on_failure=True, source_username=None)
        outcome = DispatchOutcome(success=False, message="")
        with self.assertRaises(AssertionError):
            assert_dispatch_outcome_matches_routing(world, db, outcome)

    def test_self_heal_checker_trips_when_status_not_wanted(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        with self.assertRaises(AssertionError):
            assert_preimport_fact_always_self_heals("audio_corrupt", db)

    def test_quality_side_checker_trips_when_flag_ignored(self):
        db = FakePipelineDB()
        # Planted bug: status is 'wanted' even though requeue_on_failure
        # was False — the caller's flag was ignored.
        db.seed_request(make_request_row(id=42, status="wanted"))
        with self.assertRaises(AssertionError):
            assert_quality_side_reject_honors_caller_flag(
                "downgrade", False, db)

    def test_hypothesis_harness_detects_planted_bad_router(self):
        """End-to-end RED proof: strategies + checker + Hypothesis catch a
        dispatch that always reports success and never touches status.

        Narrowed to ``decision="import"`` worlds only (mirrors
        ``test_quality_generated.py``'s analogous self-test restricting to
        a single scenario shape): the planted-bad router below always
        raises at the SAME assertion site, so Hypothesis reports one
        ``AssertionError`` instead of grouping distinct failure origins
        (no-JSON / preimport-fact / mark_done mismatches all raise from
        different lines) into an ``ExceptionGroup``.
        """

        @given(new_min_bitrate=_bitrates(),
               requeue_on_failure=st.booleans())
        @settings(max_examples=15, derandomize=True, database=None)
        def prop(new_min_bitrate, requeue_on_failure):
            world = DispatchWorld(
                mode="decision", decision="import",
                new_min_bitrate=new_min_bitrate, prev_min_bitrate=None,
                spectral_grade="genuine", spectral_bitrate=None,
                was_converted=False, requeue_on_failure=requeue_on_failure,
                source_username="user1")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, status="downloading"))
            db.log_download(request_id=42, outcome="success")
            bad_outcome = DispatchOutcome(success=True, message="always ok")
            assert_dispatch_outcome_matches_routing(world, db, bad_outcome)

        with self.assertRaises(AssertionError):
            prop()


if __name__ == "__main__":
    unittest.main()
