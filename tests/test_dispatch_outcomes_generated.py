#!/usr/bin/env python3
"""Generated (property-based) dispatch/outcome tests — issue #548.

Hypothesis-driven properties over the importer dispatch/outcome layer:
``lib/dispatch/core.py::dispatch_import_core`` (the funnel every import path
runs through) and ``lib/dispatch/outcome_actions.py::_reject_import_from_evidence_decision``
(the unified reject helper that honors caller-owned lifecycle authority).

Two harnesses, both lifted verbatim from the established hand-written
recipes (no new scaffolding):

* ``_run_dispatch`` mirrors
  ``tests/test_dispatch_core.py::TestDispatchCoreOrchestration._dispatch`` —
  a fresh ``FakePipelineDB`` + ``patch_dispatch_externals()`` +
  ``patch("lib.dispatch.subprocess_runner.parse_import_result", ...)`` to
  feed a generated ``ImportResult`` decision (or ``None`` for the
  "no JSON" crash path) into the real ``dispatch_import_core``.
* ``_reject_via_evidence_decision`` mirrors
  ``tests/test_import_dispatch.py::TestRejectImportFromEvidenceDecisionCallerLifecycle._reject`` —
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

import configparser
import os
import json
import shutil
import sys
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import MagicMock, mock_open, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given, settings
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
from tests.beets_world import BeetsWorld
from tests.helpers import (
    make_album_quality_evidence,
    make_download_file,
    make_grab_list_entry,
    make_import_result,
    make_request_row,
    noop_quality_gate,
    patch_dispatch_externals,
)

_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_GRADES = ("genuine", "marginal", "suspect", "likely_transcode")
_CALLER_CONTROLLED_QUALITY_REJECTS = tuple(sorted(
    QUALITY_DECISION_REJECT_STAGE_DECISIONS - {"verified_lossless_locked"}
))

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
_AUTOMATIC_RETAINED_ACTIONS = {
    "provisional_lossless_upgrade": ("wanted", "lossless", True),
    "transcode_upgrade": ("wanted", None, True),
    "transcode_first": ("wanted", None, True),
}
_REJECTION_WRITERS = (
    "atomic_abandon",
    "database_source",
    "evidence_decision",
    "dispatch_rejection",
    "request_auto_import",
)

_HAVE_ANALYSIS_FAILURES = (
    "PermissionError: [Errno 13] Permission denied",
    "FileNotFoundError: no such file",
    "no audio files found under installed album",
    "snapshot changed during analysis",
    "RuntimeError: analyser crashed",
)


def _full_dispatch_config() -> CratediggerConfig:
    ini = configparser.RawConfigParser()
    ini["Beets Validation"] = {"harness_path": _HARNESS}
    ini["Pipeline DB"] = {"enabled": "true"}
    return CratediggerConfig.from_ini(ini)


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


def _run_dispatch(
    world: DispatchWorld,
    *,
    beets: BeetsWorld,
    initial_status: str = "downloading",
    force: bool = False,
    queued: bool = False,
) -> dict:
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

    cfg = CratediggerConfig(
        beets_harness_path=_HARNESS,
        pipeline_db_enabled=True,
    )
    dl_info = DownloadInfo(username=world.source_username)

    tmpdir = tempfile.mkdtemp()
    try:
        del queued  # retained argument for existing generated call sites
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status=initial_status, mb_release_id="mbid-generated",
            min_bitrate=180, current_spectral_bitrate=128,
            active_download_state={
                "files": [],
                "filetype": "mp3",
                "current_path": tmpdir,
            },
        ))
        from lib.import_evidence import (
            ActionEvidenceProvenance,
            CandidateEvidenceActionResult,
        )
        from lib.import_queue import IMPORT_JOB_AUTOMATION, IMPORT_JOB_FORCE

        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE if force else IMPORT_JOB_AUTOMATION,
            request_id=42,
            payload={"failed_path": tmpdir} if force else {},
        )
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-generated",
            source_path=tmpdir,
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job.id, persisted.id)
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
        )
        claimed = db.claim_next_import_job(worker_id="generated-dispatch")
        assert claimed is not None
        import_job_id = claimed.id
        candidate_result = CandidateEvidenceActionResult(
            evidence=persisted,
            provenance=ActionEvidenceProvenance(
                candidate_status="reused",
                snapshot_guard="matched",
            ),
        )
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
                scenario="force_import" if force else "strong_match",
                force=force,
                files=[MagicMock(username=world.source_username or "user1",
                                 filename="01 - Track.mp3")],
                cfg=cfg,
                requeue_on_failure=world.requeue_on_failure,
                quality_gate_fn=noop_quality_gate,
                candidate_import_job_id=import_job_id,
                prevalidated_candidate_result=candidate_result,
                beets_library_db_path=str(beets.library_db),
                beets_library_root=str(beets.library_root),
            )
        if result.terminal_outcome is not None:
            from lib.terminal_outcomes import ImportJobTerminal

            db.persist_import_terminal_outcome(
                result.terminal_outcome.with_job(ImportJobTerminal(
                    status="completed" if result.success else "failed",
                    result={"success": result.success},
                    message=result.message,
                    error=None if result.success else result.message,
                ))
            )
        else:
            from tests.helpers import finalize_claimed_dispatch
            finalize_claimed_dispatch(db, claimed, result)
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
    ``tests/test_import_dispatch.py::TestRejectImportFromEvidenceDecisionCallerLifecycle._reject``)
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
            # for every fuzz example makes the property impractical
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


def _run_have_analysis_abort(
    *,
    mode: str,
    raw_error: str,
    search_override: str | None,
    username: str | None,
    cooldown_verdict: bool,
) -> FakePipelineDB:
    """Drive the real current-evidence gate through its terminal DB bundle."""

    from lib.dispatch import dispatch_import_core
    from lib.import_evidence import (
        ActionEvidenceProvenance,
        CandidateEvidenceActionResult,
        CurrentEvidenceActionResult,
    )
    from lib.import_queue import (
        IMPORT_JOB_AUTOMATION,
        IMPORT_JOB_FORCE,
    )
    from lib.terminal_outcomes import ImportJobTerminal

    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42,
        status="unsearchable" if mode == "force" else "downloading",
        search_filetype_override=search_override,
        active_download_state={"files": [], "filetype": "flac"},
    ))
    candidate = make_album_quality_evidence(
        mb_release_id="generated-have-analysis-mbid",
        source_path="/tmp/generated-candidate",
    )
    candidate_result = CandidateEvidenceActionResult(
        evidence=candidate,
        provenance=ActionEvidenceProvenance(
            candidate_status="reused",
            snapshot_guard="matched",
        ),
    )
    current_result = CurrentEvidenceActionResult(
        evidence=None,
        provenance=ActionEvidenceProvenance(
            current_status="failed",
            snapshot_guard="failed",
            fallback_reason=raw_error,
            installed_path="/library/Generated Artist/Generated Album",
            fail_closed=True,
        ),
    )
    job_type = {
        "auto": IMPORT_JOB_AUTOMATION,
        "force": IMPORT_JOB_FORCE,
    }[mode]
    scenario = {
        "auto": "strong_match",
        "force": "force_import",
    }[mode]

    with tempfile.TemporaryDirectory() as tmpdir:
        payload = {} if mode == "auto" else {"failed_path": tmpdir}
        job = db.enqueue_import_job(
            job_type,
            request_id=42,
            payload=payload,
        )
        with patch_dispatch_externals():
            outcome = dispatch_import_core(
                path=tmpdir,
                mb_release_id="generated-have-analysis-mbid",
                request_id=42,
                label="Generated Artist - Generated Album",
                force=mode == "force",
                beets_harness_path=_HARNESS,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="flac", username=username),
                scenario=scenario,
                cfg=_full_dispatch_config(),
                requeue_on_failure=mode == "auto",
                candidate_import_job_id=job.id,
                prevalidated_candidate_result=candidate_result,
                quality_gate_fn=noop_quality_gate,
                current_evidence_loader=(
                    lambda *_args, **_kwargs: current_result
                ),
            )
    if outcome.terminal_outcome is None:
        raise AssertionError("HAVE-analysis abort did not build a terminal outcome")
    db.set_cooldown_result(cooldown_verdict)
    db.persist_import_terminal_outcome(outcome.terminal_outcome.with_job(
        ImportJobTerminal(
            status="failed",
            error=outcome.message,
            result={"success": False},
            message=outcome.message,
        )
    ))
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
    status = db.request(42)["status"]

    if world.mode == "no_json":
        if db.download_logs:
            raise AssertionError(
                "no-JSON ambiguity wrote a terminal download audit")
        if status != "downloading":
            raise AssertionError(
                f"no-JSON ambiguity left status={status!r}, want 'downloading'")
        job = db.get_import_job(1)
        if job is None or job.status != "recovery_required":
            raise AssertionError(
                "no-JSON ambiguity did not stop in recovery_required")
        if outcome.success:
            raise AssertionError("no-JSON crash reported success=True")
        return

    assert_download_log_row_created(db)
    log = db.download_logs[-1]
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
        expected_status, expected_override, expected_denylist = (
            _AUTOMATIC_RETAINED_ACTIONS.get(
                world.decision,
                ("imported", None, False),
            )
        )
        if status != expected_status:
            raise AssertionError(
                f"decision={world.decision!r} mark_done=True left "
                f"status={status!r}, want {expected_status!r}")
        actual_override = db.request(42)["search_filetype_override"]
        if actual_override != expected_override:
            raise AssertionError(
                f"decision={world.decision!r} mark_done=True left override="
                f"{actual_override!r}, want {expected_override!r}"
            )
        if bool(db.denylist) != expected_denylist:
            raise AssertionError(
                f"decision={world.decision!r} mark_done=True denylist="
                f"{bool(db.denylist)!r}, want {expected_denylist!r}"
            )
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


def assert_preimport_fact_honors_caller_flag(
    decision: str, requeue_on_failure: bool, db: FakePipelineDB,
) -> None:
    """Integrity rejection preserves operator-owned search state on force."""
    status = db.request(42)["status"]
    expected = "wanted" if requeue_on_failure else "downloading"
    if status != expected:
        raise AssertionError(
            f"preimport-fact reject {decision!r} left status={status!r}, "
            f"want {expected!r} for requeue_on_failure="
            f"{requeue_on_failure}")


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
    ``requeue_on_failure`` flag normally (force-import, which passes
    False stay put; the operator already chose to act on this source)."""
    status = db.request(42)["status"]
    expected = "wanted" if requeue_on_failure else "downloading"
    if status != expected:
        raise AssertionError(
            f"quality-side reject {decision!r} "
            f"requeue_on_failure={requeue_on_failure} left status="
            f"{status!r}, want {expected!r}")


def assert_verified_lossless_lock_preserves_imported(db: FakePipelineDB) -> None:
    """The non-punitive proof lock closes acquisition without source blame."""
    row = db.request(42)
    if row["status"] != "imported":
        raise AssertionError(
            f"verified lossless lock left status={row['status']!r}, "
            "want 'imported'"
        )
    if row["search_filetype_override"] is not None:
        raise AssertionError("verified lossless lock narrowed search policy")
    if db.denylist:
        raise AssertionError("verified lossless lock denylisted the source")


def assert_have_analysis_abort_is_non_quality(
    db: FakePipelineDB,
    *,
    mode: str,
    expected_search_override: str | None,
) -> None:
    """Analysis failure preserves caller lifecycle without quality policy."""

    assert_download_log_row_created(db)
    row = db.request(42)
    expected_status = "wanted" if mode == "auto" else "unsearchable"
    if row["status"] != expected_status:
        raise AssertionError(
            f"HAVE-analysis abort left status={row['status']!r}, "
            f"want {expected_status!r} for {mode}"
        )
    if row["search_filetype_override"] != expected_search_override:
        raise AssertionError(
            "HAVE-analysis abort changed search_filetype_override from "
            f"{expected_search_override!r} to "
            f"{row['search_filetype_override']!r}"
        )
    if db.denylist:
        raise AssertionError(
            f"HAVE-analysis abort wrote quality denylist entries: {db.denylist!r}"
        )
    if db.download_logs[-1].outcome != "have_analysis_error":
        raise AssertionError(
            "HAVE-analysis abort did not persist outcome='have_analysis_error'"
        )
    expected_attempts = 1 if mode == "auto" else 0
    retry_state_wrong = (
        row["next_retry_after"] is None
        if mode == "auto"
        else row["next_retry_after"] is not None
    )
    if row["validation_attempts"] != expected_attempts or retry_state_wrong:
        raise AssertionError(
            "HAVE-analysis abort applied the wrong retry bookkeeping"
        )


def assert_have_analysis_abort_cooldown_policy(
    db: FakePipelineDB,
    *,
    username: str | None,
    cooldown_verdict: bool,
) -> None:
    """Both caller modes evaluate and persist cooldowns identically."""

    expected_evaluations = [] if username is None else [username]
    if db.cooldowns_applied != expected_evaluations:
        raise AssertionError(
            "HAVE-analysis cooldown evaluations drifted: "
            f"{db.cooldowns_applied!r} != {expected_evaluations!r}"
        )
    expected_usernames = (
        {username}
        if username is not None and cooldown_verdict
        else set()
    )
    actual_usernames = set(db.user_cooldowns)
    if actual_usernames != expected_usernames:
        raise AssertionError(
            "HAVE-analysis cooldown persistence drifted: "
            f"written={actual_usernames!r} != expected={expected_usernames!r}"
        )


def assert_operator_retained_lifecycle(
    db: FakePipelineDB,
    *,
    initial_status: str,
    expected_override: str | None,
) -> None:
    row = db.request(42)
    if row["status"] != initial_status:
        raise AssertionError(
            f"retained force import changed lifecycle from {initial_status!r} "
            f"to {row['status']!r}"
        )
    if row["search_filetype_override"] != expected_override:
        raise AssertionError(
            "retained force import failed to record canonical search policy"
        )


def assert_archival_quarantine_isolated(
    *,
    cleanup_call_count: int,
    terminal_log: DownloadLogRow,
    candidate_evidence_id: int,
) -> None:
    """An archival quarantine never enters a destructive WM reducer."""
    if cleanup_call_count:
        raise AssertionError("archival quarantine reached Wrong Matches cleanup")
    if terminal_log.candidate_evidence_id != candidate_evidence_id:
        raise AssertionError("archival terminal audit lost candidate evidence")
    validation = terminal_log.validation_result
    if isinstance(validation, str):
        validation = json.loads(validation)
    if isinstance(validation, dict) and "wrong_match_triage" in validation:
        raise AssertionError("archival terminal audit gained deletion triage")


# ===========================================================================
# Properties
# ===========================================================================

class TestGeneratedDispatchOutcomes(unittest.TestCase):
    """Properties over the legacy (subprocess-return) dispatch path."""

    def setUp(self) -> None:
        self.beets = BeetsWorld(_REPO_ROOT)
        self.addCleanup(self.beets.close)
        self.runtime = patch.dict(os.environ, {
            "CRATEDIGGER_RUNTIME_CONFIG": str(
                self.beets.poisoned_runtime_config()
            ),
            "BEETS_DB": str(self.beets.root / "poisoned-library.db"),
        })
        self.runtime.start()
        self.addCleanup(self.runtime.stop)

    @given(world=dispatch_worlds())
    def test_terminal_outcomes_are_audited_and_ambiguity_is_not(self, world):
        outcome = _run_dispatch(world, beets=self.beets)
        if world.mode == "no_json":
            self.assertEqual(outcome["db"].download_logs, [])
        else:
            assert_download_log_row_created(outcome["db"])

    @given(world=dispatch_worlds())
    def test_outcome_matches_dispatch_action_routing(self, world):
        outcome = _run_dispatch(world, beets=self.beets)
        assert_dispatch_outcome_matches_routing(
            world, outcome["db"], outcome["result"])


class TestGeneratedEvidenceRejectLifecycle(unittest.TestCase):
    """Every evidence rejection honors the caller lifecycle flag."""

    @given(decision=st.sampled_from(sorted(_PREIMPORT_FACT_REJECT_DECISIONS)),
           requeue_on_failure=st.booleans(),
           new_min_bitrate=_bitrates())
    def test_preimport_facts_honor_caller_flag(
            self, decision, requeue_on_failure, new_min_bitrate):
        db = _reject_via_evidence_decision(
            decision=decision, requeue_on_failure=requeue_on_failure,
            new_min_bitrate=new_min_bitrate)
        assert_download_log_row_created(db)
        assert_preimport_fact_honors_caller_flag(
            decision, requeue_on_failure, db)

    @given(decision=st.sampled_from(_CALLER_CONTROLLED_QUALITY_REJECTS),
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

    @given(requeue_on_failure=st.booleans(), new_min_bitrate=_bitrates())
    def test_verified_lossless_lock_always_preserves_imported(
            self, requeue_on_failure, new_min_bitrate):
        db = _reject_via_evidence_decision(
            decision="verified_lossless_locked",
            requeue_on_failure=requeue_on_failure,
            new_min_bitrate=new_min_bitrate,
        )
        assert_download_log_row_created(db)
        assert_verified_lossless_lock_preserves_imported(db)


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


class TestGeneratedHaveAnalysisAbortLifecycle(unittest.TestCase):
    """The non-quality abort invariant across auto/force lifecycles."""

    @given(
        mode=st.sampled_from(("auto", "force")),
        raw_error=st.sampled_from(_HAVE_ANALYSIS_FAILURES),
        search_override=st.sampled_from((None, "lossless", "lossless,mp3 v0")),
        username=st.sampled_from((None, "user1", "user2")),
        cooldown_verdict=st.booleans(),
    )
    def test_abort_preserves_caller_lifecycle_never_denylisted_or_narrowed(
        self,
        mode,
        raw_error,
        search_override,
        username,
        cooldown_verdict,
    ):
        db = _run_have_analysis_abort(
            mode=mode,
            raw_error=raw_error,
            search_override=search_override,
            username=username,
            cooldown_verdict=cooldown_verdict,
        )
        assert_have_analysis_abort_is_non_quality(
            db,
            mode=mode,
            expected_search_override=search_override,
        )
        assert_have_analysis_abort_cooldown_policy(
            db,
            username=username,
            cooldown_verdict=cooldown_verdict,
        )


class TestGeneratedOperatorRetainedLifecycle(unittest.TestCase):
    """Nonterminal quality policy never clears the starting search state."""

    def setUp(self) -> None:
        self.beets = BeetsWorld(_REPO_ROOT)
        self.addCleanup(self.beets.close)
        self.runtime = patch.dict(os.environ, {
            "CRATEDIGGER_RUNTIME_CONFIG": str(
                self.beets.poisoned_runtime_config()
            ),
            "BEETS_DB": str(self.beets.root / "poisoned-library.db"),
        })
        self.runtime.start()
        self.addCleanup(self.runtime.stop)

    @given(
        decision=st.sampled_from(tuple(sorted(_AUTOMATIC_RETAINED_ACTIONS))),
        initial_status=st.sampled_from(("wanted", "unsearchable")),
    )
    @example(
        decision="provisional_lossless_upgrade",
        initial_status="wanted",
    )
    def test_retained_policy_preserves_starting_search_lifecycle(
        self,
        decision,
        initial_status,
    ):
        expected_override = _AUTOMATIC_RETAINED_ACTIONS[decision][1]
        world = DispatchWorld(
            mode="decision",
            decision=decision,
            new_min_bitrate=245,
            prev_min_bitrate=192,
            spectral_grade="genuine",
            spectral_bitrate=None,
            was_converted=False,
            requeue_on_failure=False,
            source_username="user1",
        )
        outcome = _run_dispatch(
            world,
            beets=self.beets,
            initial_status=initial_status,
            force=True,
            queued=True,
        )
        assert_operator_retained_lifecycle(
            outcome["db"],
            initial_status=initial_status,
            expected_override=expected_override,
        )


class TestGeneratedArchivalQuarantineIsolation(unittest.TestCase):
    @given(
        scenario=st.one_of(
            st.none(),
            st.sampled_from((
                "force_import",
                "strong_mismatch",
                "audio_corrupt",
                "untracked_audio",
            )),
        )
    )
    @example(scenario=None)
    def test_archive_plan_never_reaches_wrong_match_cleanup(
        self,
        scenario: str | None,
    ) -> None:
        from lib.dispatch.types import PostCommitCleanup
        from lib.import_queue import IMPORT_JOB_FORCE
        from scripts.importer import _cleanup_committed_wrong_match_rejection

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=835,
            status="unsearchable",
            mb_release_id="generated-archival-mbid",
        ))
        log_id = db.log_download(
            request_id=835,
            outcome="rejected",
            validation_result=json.dumps({"scenario": "audio_corrupt"}),
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=835,
            payload={
                "download_log_id": log_id,
                "failed_path": "/failed_imports/bad_files/album",
            },
        )
        evidence = make_album_quality_evidence(
            mb_release_id="generated-archival-mbid",
            source_path="/failed_imports/bad_files/album",
            audio_corrupt=True,
            audio_error="generated decode failure",
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job.id, persisted.id)
        outcome = DispatchOutcome(
            success=False,
            message="audio_corrupt",
            post_commit_wrong_match_scenario=scenario,
            post_commit_cleanup=PostCommitCleanup(
                audio_quarantine_source_path="/source/album",
                audio_quarantine_root="/download-root",
            ),
        )

        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match",
        ) as cleanup_wrong_match:
            _cleanup_committed_wrong_match_rejection(
                cast(Any, db),
                job,
                log_id,
                outcome,
            )

        assert_archival_quarantine_isolated(
            cleanup_call_count=cleanup_wrong_match.call_count,
            terminal_log=db.download_logs[-1],
            candidate_evidence_id=persisted.id,
        )


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

    def test_archival_checker_trips_on_wrong_match_cleanup(self):
        with self.assertRaisesRegex(
            AssertionError,
            "reached Wrong Matches cleanup",
        ):
            assert_archival_quarantine_isolated(
                cleanup_call_count=1,
                terminal_log=DownloadLogRow(
                    request_id=835,
                    outcome="rejected",
                    candidate_evidence_id=7,
                    validation_result=json.dumps({
                        "scenario": "audio_corrupt",
                    }),
                ),
                candidate_evidence_id=7,
            )

    def test_verified_lossless_lock_checker_trips_on_reopened_request(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            search_filetype_override="lossless",
        ))
        with self.assertRaises(AssertionError):
            assert_verified_lossless_lock_preserves_imported(db)

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
        # Planted bug: an ordinary "import" decision
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
        # Ambiguous no-JSON work must not write a terminal success audit.
        db.log_download(request_id=42, outcome="success")
        world = DispatchWorld(
            mode="no_json", decision=None, new_min_bitrate=None,
            prev_min_bitrate=None, spectral_grade="genuine",
            spectral_bitrate=None, was_converted=False,
            requeue_on_failure=True, source_username=None)
        outcome = DispatchOutcome(success=False, message="")
        with self.assertRaises(AssertionError):
            assert_dispatch_outcome_matches_routing(world, db, outcome)

    def test_preimport_caller_flag_checker_trips_when_flag_ignored(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        with self.assertRaises(AssertionError):
            assert_preimport_fact_honors_caller_flag(
                "audio_corrupt", False, db)

    def test_quality_side_checker_trips_when_flag_ignored(self):
        db = FakePipelineDB()
        # Planted bug: status is 'wanted' even though requeue_on_failure
        # was False — the caller's flag was ignored.
        db.seed_request(make_request_row(id=42, status="wanted"))
        with self.assertRaises(AssertionError):
            assert_quality_side_reject_honors_caller_flag(
                "downgrade", False, db)

    def test_have_analysis_checker_trips_on_quality_consequences(self):
        from tests.fakes import DenylistEntry

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            validation_attempts=1,
            next_retry_after="planted-backoff",
            search_filetype_override="lossless",
        ))
        db.log_download(request_id=42, outcome="have_analysis_error")
        db.denylist.append(DenylistEntry(42, "bad-user", "planted mutant"))
        with self.assertRaises(AssertionError):
            assert_have_analysis_abort_is_non_quality(
                db,
                mode="auto",
                expected_search_override=None,
            )

    def test_have_analysis_cooldown_checker_trips_on_double_evaluation(self):
        db = FakePipelineDB()
        db.cooldowns_applied.extend(("peer", "peer"))
        with self.assertRaises(AssertionError):
            assert_have_analysis_abort_cooldown_policy(
                db,
                username="peer",
                cooldown_verdict=False,
            )

    def test_have_analysis_cooldown_checker_trips_on_missing_write(self):
        db = FakePipelineDB()
        db.cooldowns_applied.append("peer")
        with self.assertRaises(AssertionError):
            assert_have_analysis_abort_cooldown_policy(
                db,
                username="peer",
                cooldown_verdict=True,
            )

    def test_have_analysis_cooldown_checker_trips_without_username(self):
        db = FakePipelineDB()
        db.add_cooldown(
            "ghost",
            datetime.now(timezone.utc) + timedelta(days=1),
            "planted mutant",
        )
        with self.assertRaises(AssertionError):
            assert_have_analysis_abort_cooldown_policy(
                db,
                username=None,
                cooldown_verdict=False,
            )

    def test_have_analysis_cooldown_checker_trips_on_false_verdict_write(self):
        db = FakePipelineDB()
        db.cooldowns_applied.append("peer")
        db.add_cooldown(
            "peer",
            datetime.now(timezone.utc) + timedelta(days=1),
            "planted mutant",
        )
        with self.assertRaises(AssertionError):
            assert_have_analysis_abort_cooldown_policy(
                db,
                username="peer",
                cooldown_verdict=False,
            )

    def test_operator_retained_checker_trips_when_stop_is_cleared(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            search_filetype_override="lossless",
        ))
        with self.assertRaises(AssertionError):
            assert_operator_retained_lifecycle(
                db,
                initial_status="unsearchable",
                expected_override="lossless",
            )

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
