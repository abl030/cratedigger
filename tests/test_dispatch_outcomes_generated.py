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
import json
import shutil
import sys
import tempfile
import unittest
from dataclasses import dataclass
from typing import Any, cast
from unittest.mock import MagicMock, mock_open, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given, settings
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.dispatch import DispatchOutcome
from lib.dispatch.types import ImportAttemptResult, _PREIMPORT_FACT_REJECT_DECISIONS
from lib.quality import (
    QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
    QUALITY_DECISION_REJECT_STAGE_DECISIONS,
    DownloadInfo,
    dispatch_action,
)
from tests.fakes import DownloadLogRow, FakePipelineDB
from tests.helpers import (
    make_download_file,
    make_grab_list_entry,
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
_REJECTION_WRITERS = (
    "atomic_abandon",
    "database_source",
    "evidence_decision",
    "dispatch_rejection",
    "request_auto_import",
)


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
    distance: float | None = 0.0,
) -> FakePipelineDB:
    """Established recipe (mirrors
    ``tests/test_import_dispatch.py::TestRejectImportFromEvidenceDecisionForcedRequeue._reject``)
    for driving the REAL ``_reject_import_from_evidence_decision`` directly
    with a generated ``decision`` string.

    ``distance`` defaults to ``0.0`` for the pre-existing self-heal
    properties above (they don't care about the value); the #550 defect #4
    properties below pass a generated ``float | None`` to prove the helper
    threads it through unchanged to both persisted sinks."""
    from lib.dispatch import _reject_import_from_evidence_decision

    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42, status="downloading", mb_release_id="test-mbid"))
    dl_info = DownloadInfo(filetype="mp3", username=source_username)
    ir = make_import_result(decision=decision, new_min_bitrate=new_min_bitrate)
    attempt_result = ImportAttemptResult(None)
    attempt_result.merge(ir)
    with patch_dispatch_externals():
        _reject_import_from_evidence_decision(
            db=db,  # type: ignore[arg-type]
            request_id=42,
            dl_info=dl_info,
            attempt_result=attempt_result,
            distance=distance,
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


def _run_rejection_writer(
    *,
    writer: str,
    distance: float | None,
    scenario: str | None,
    real_filesystem: bool = False,
) -> FakePipelineDB:
    """Drive every production rejection writer with one ValidationResult."""
    from album_source import DatabaseSource
    from lib.download_rejection import _reject_request_auto_import
    from lib.quality import ValidationResult
    from lib.staged_album import StagedAlbum
    from tests.helpers import make_ctx_with_fake_db

    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42,
        status="downloading",
        artist_name="Generated Artist",
        album_title="Generated Album",
        year=2026,
        mb_release_id="generated-mbid",
    ))
    validation_result = ValidationResult(
        valid=False,
        distance=distance,
        scenario=scenario,
        detail="generated reject",
        error="generated reject",
    )

    if writer == "evidence_decision":
        from lib.dispatch import _reject_import_from_evidence_decision

        attempt_result = ImportAttemptResult(None)
        attempt_result.merge(make_import_result(
            decision="downgrade",
            new_min_bitrate=128,
        ))

        with patch_dispatch_externals():
            _reject_import_from_evidence_decision(
                db=db,  # type: ignore[arg-type]
                request_id=42,
                dl_info=DownloadInfo(username="generated-user"),
                attempt_result=attempt_result,
                distance=distance,
                decision="downgrade",
                detail="generated reject",
                requeue_on_failure=True,
                validation_result=validation_result.to_json(),
                staged_path="/tmp/generated-staged",
                scenario=scenario or "generated_reject",
                files=None,
                source_path_cleanup_scenario="downgrade",
                cooled_down_users=None,
            )
        return db

    if writer == "dispatch_rejection":
        from lib.dispatch import _record_rejection_and_maybe_requeue

        _record_rejection_and_maybe_requeue(
            db=db,  # type: ignore[arg-type]
            request_id=42,
            dl_info=DownloadInfo(username="generated-user"),
            detail=validation_result.detail,
            error=validation_result.error,
            validation_result=validation_result.to_json(),
            requeue=True,
        )
        return db

    if writer == "database_source":
        source = DatabaseSource("unused-generated-dsn")
        cast(Any, source)._db = db
        album = make_grab_list_entry(
            artist="Generated Artist",
            title="Generated Album",
            year="2026",
            mb_release_id="generated-mbid",
            db_request_id=42,
            db_source="request",
        )
        source.reject_and_requeue(
            album,
            validation_result,
            download_info=DownloadInfo(
                username="generated-user",
                validation_result=validation_result.to_json(),
            ),
        )
        return db

    if writer == "atomic_abandon":
        db.request(42)["active_download_state"] = {
            "current_path": "/tmp/generated-staged",
            "import_subprocess_started_at": "2026-07-11T00:00:00+00:00",
        }
        db.abandon_auto_import_request(
            request_id=42,
            current_path="/tmp/generated-staged",
            soulseek_username="generated-user",
            filetype="flac",
            beets_detail="generated reject",
            outcome="failed",
            staged_path="/tmp/generated-staged",
            error_message="generated reject",
            validation_result=validation_result.to_json(),
        )
        return db

    if writer == "request_auto_import":
        album = make_grab_list_entry(
            artist="Generated Artist",
            title="Generated Album",
            year="2026",
            mb_release_id="generated-mbid",
            db_request_id=42,
            db_source="request",
            files=[make_download_file(
                username="generated-user",
                filename="Generated Album\\01 - Generated.mp3",
                file_dir="Generated Album",
            )],
        )
        if real_filesystem:
            # The deterministic pin below owns the full quarantine move and
            # tracking-file integration. Repeating those filesystem effects
            # for every push/fuzz example makes the property impractical
            # without adding projection coverage.
            with tempfile.TemporaryDirectory() as tmpdir:
                cfg = CratediggerConfig(
                    beets_harness_path=_HARNESS,
                    beets_tracking_file=os.path.join(tmpdir, "validation.jsonl"),
                    pipeline_db_enabled=True,
                )
                source_path = os.path.join(tmpdir, "Generated Album")
                os.makedirs(source_path)
                with open(
                    os.path.join(source_path, "01 - Generated.mp3"),
                    "wb",
                ) as audio_file:
                    audio_file.write(b"generated audio")
                ctx = make_ctx_with_fake_db(db, cfg=cfg)
                _reject_request_auto_import(
                    album,
                    validation_result,
                    StagedAlbum(current_path=source_path, request_id=42),
                    ctx,
                    detail="generated reject",
                    scenario=scenario,
                    error="generated reject",
                )
            return db

        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        ctx = make_ctx_with_fake_db(db, cfg=cfg)
        with patch(
            "lib.download_rejection.move_failed_import_curated",
            return_value="/tmp/generated-failed-import",
        ), patch("builtins.open", mock_open()):
            _reject_request_auto_import(
                album,
                validation_result,
                StagedAlbum(
                    current_path="/tmp/generated-staged",
                    request_id=42,
                ),
                ctx,
                detail="generated reject",
                scenario=scenario,
                error="generated reject",
            )
        return db

    raise AssertionError(f"unknown rejection writer {writer!r}")


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


def assert_beets_distance_round_trips(
    db: FakePipelineDB, expected_distance: float | None,
) -> None:
    """Issue #550 defect #4 invariant: no unmeasured distance is ever
    persisted as a number. Whatever ``distance`` flows INTO the reject
    path is exactly what must land in ``download_log.beets_distance`` —
    ``None`` in, ``None`` out (a pre-match/preimport-fact reject never
    fabricates a 0.0 'perfect match'), and a genuinely measured value
    (including a real 0.0) round-trips unchanged rather than being
    nulled."""
    assert_download_log_row_created(db)
    last = db.download_logs[-1]
    if last.beets_distance != expected_distance:
        raise AssertionError(
            f"expected persisted beets_distance={expected_distance!r}, "
            f"got {last.beets_distance!r} — a reject/mark-done writer "
            "must never substitute a fabricated value for the distance "
            "it was actually given")


def assert_validation_projection_matches_payload(db: FakePipelineDB) -> None:
    """Envelope distance/scenario keys must equal their query columns."""
    from lib.validation_envelope import decode_validation_envelope

    assert_download_log_row_created(db)
    last = db.download_logs[-1]
    raw = last.validation_result
    raw_object = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    if not isinstance(raw_object, dict):
        raise AssertionError("rejection writer did not persist an object envelope")
    envelope = decode_validation_envelope(raw_object)
    if "distance" in raw_object and last.beets_distance != envelope.distance:
        raise AssertionError(
            f"validation distance={envelope.distance!r} drifted from "
            f"beets_distance={last.beets_distance!r}"
        )
    if "scenario" in raw_object and last.beets_scenario != envelope.scenario:
        raise AssertionError(
            f"validation scenario={envelope.scenario!r} drifted from "
            f"beets_scenario={last.beets_scenario!r}"
        )


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


class TestGeneratedDistanceNeverFabricated(unittest.TestCase):
    """Issue #550 defect #4: no unmeasured distance is ever persisted as a
    number. ``_reject_import_from_evidence_decision`` is the reject helper
    every preimport-fact AND pre-match reject funnels through (folded in
    per U11 — see CLAUDE.md § "Quality decisions live in ONE place"); it
    must thread whatever ``distance`` it's given straight to
    ``download_log.beets_distance`` — ``None`` in, ``None`` out, and a
    genuinely measured value (including a real 0.0 perfect match) never
    gets nulled or swapped for a fabricated placeholder."""

    @given(decision=st.sampled_from(sorted(_PREIMPORT_FACT_REJECT_DECISIONS)),
           requeue_on_failure=st.booleans(),
           new_min_bitrate=_bitrates(),
           distance=st.one_of(
               st.none(),
               st.floats(min_value=0.0, max_value=1.0,
                         allow_nan=False, allow_infinity=False),
           ))
    def test_distance_round_trips_exactly_or_stays_null(
            self, decision, requeue_on_failure, new_min_bitrate, distance):
        db = _reject_via_evidence_decision(
            decision=decision, requeue_on_failure=requeue_on_failure,
            new_min_bitrate=new_min_bitrate, distance=distance)
        assert_beets_distance_round_trips(db, distance)


class TestGeneratedEveryRejectionWriterProjection(unittest.TestCase):
    """One property patrols every rejection writer through the shared sink."""

    def test_request_auto_import_writer_pin(self):
        db = _run_rejection_writer(
            writer="request_auto_import",
            distance=0.0,
            scenario="untracked_audio",
            real_filesystem=True,
        )
        assert_validation_projection_matches_payload(db)

    def test_every_rejection_writer_preserves_explicit_nulls(self):
        for writer in _REJECTION_WRITERS:
            with self.subTest(writer=writer):
                db = _run_rejection_writer(
                    writer=writer,
                    distance=None,
                    scenario=None,
                )
                assert_validation_projection_matches_payload(db)
                self.assertIsNone(db.download_logs[-1].beets_distance)
                self.assertIsNone(db.download_logs[-1].beets_scenario)
                if writer == "request_auto_import":
                    payload = json.loads(
                        db.download_logs[-1].validation_result or "{}"
                    )
                    self.assertIn(
                        "wrong_match_triage",
                        payload,
                        "request-auto-import matrix case must run the real "
                        "post-rejection cleanup orchestration",
                    )

    @given(
        writer=st.sampled_from(_REJECTION_WRITERS),
        distance=st.one_of(
            st.none(),
            st.floats(
                min_value=0.0,
                max_value=1.0,
                allow_nan=False,
                allow_infinity=False,
            ),
        ),
        scenario=st.one_of(
            st.none(),
            st.text(min_size=0, max_size=40),
        ),
    )
    def test_every_rejection_writer_projects_validation_once(
        self,
        writer,
        distance,
        scenario,
    ):
        db = _run_rejection_writer(
            writer=writer,
            distance=distance,
            scenario=scenario,
        )
        assert_validation_projection_matches_payload(db)


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

    def test_distance_checker_trips_when_null_gets_fabricated_as_zero(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        # Planted bug: the caller asked for an unmeasured (None) distance
        # but the writer fabricated a 0.0 "perfect match" — exactly the
        # #550 defect #4 regression this property exists to catch.
        db.log_download(request_id=42, outcome="rejected", beets_distance=0.0)
        with self.assertRaises(AssertionError):
            assert_beets_distance_round_trips(db, None)

    def test_distance_checker_trips_when_measured_value_gets_nulled(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        # Planted bug: a genuinely measured distance (0.07) was dropped
        # to NULL instead of being persisted as-is.
        db.log_download(request_id=42, outcome="rejected", beets_distance=None)
        with self.assertRaises(AssertionError):
            assert_beets_distance_round_trips(db, 0.07)

    def test_validation_projection_checker_trips_on_dual_sink_drift(self):
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.download_logs.append(DownloadLogRow(
            request_id=42,
            outcome="rejected",
            beets_distance=0.99,
            beets_scenario="wrong_scenario",
            validation_result=ValidationResult(
                distance=0.07,
                scenario="high_distance",
            ).to_json(),
        ))
        with self.assertRaises(AssertionError):
            assert_validation_projection_matches_payload(db)

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
