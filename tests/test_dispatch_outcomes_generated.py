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

The invariants these properties patrol (CLAUDE.md invariants 6 and 11 —
"broken worlds surface and restart, nothing is ever parked"):

* Every dispatch outcome is RECORDED where the operator reads it: exactly
  one ``download_log`` row that renders through the real Recents lens, or —
  for a caller-retained job that owns no request lifecycle and produced no
  terminal bundle — a terminal ``failed`` job row carrying the diagnostic.
  An ambiguous acknowledgement is audited like any other outcome; "ambiguity
  writes no audit" was the pre-#933 policy and is now itself a violation.
* No outcome parks anything: the owner job reaches a terminal queue status
  (``recovery_required`` is an ACTIVE status, so resting there is parking),
  no finished job stays attached as the request's automation owner, and an
  automation request lands exactly ``wanted`` or ``imported`` with its owned
  download state released.
* Whatever ``dispatch_action(decision)`` prescribes is what landed in the DB.

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
import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import product
from typing import Any, cast
from unittest.mock import MagicMock, mock_open, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.config import CratediggerConfig
from lib.dispatch import DispatchOutcome
from lib.dispatch.types import _PREIMPORT_FACT_REJECT_DECISIONS, ImportAttemptResult
from lib.import_execution import (
    CancellationToken,
    ExecutionLeaseSnapshot,
    ProcessIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_ACTIVE_STATUSES,
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_STATUSES,
)
from lib.quality import (
    QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
    QUALITY_DECISION_REJECT_STAGE_DECISIONS,
    DownloadInfo,
    dispatch_action,
)
from lib.quality.decisions import QualityGateDecision
from lib.quality_evidence import snapshot_audio_files
from tests.beets_world import BeetsWorld
from tests.dispatch_helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    finalize_claimed_dispatch,
    handoff_automation_owner,
    make_dispatch_request,
    noop_quality_gate,
    patch_dispatch_externals,
    pinned_dispatch_authority,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import DownloadLogRow, FakePipelineDB
from tests.helpers import (
    make_download_file,
    make_grab_list_entry,
    make_import_result,
    make_request_row,
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

# --- CLAUDE.md invariant 11 vocabulary ------------------------------------
# "Broken worlds surface and restart. Nothing is ever parked."
#
# Terminal queue statuses are DERIVED from production's own sets rather than
# hand-listed: whatever the queue still counts as ACTIVE is by definition not
# an exit, and ``recovery_required`` is one of those active statuses — a job
# resting there is a request whose only exit is an operator command.
_TERMINAL_IMPORT_JOB_STATUSES = frozenset(
    IMPORT_JOB_STATUSES - IMPORT_JOB_ACTIVE_STATUSES
)
# The only two statuses a dispatched automation request may end in:
# searchable again, or acquired.
_RUNNABLE_TERMINAL_REQUEST_STATUSES = frozenset({"wanted", "imported"})
# ``_run_dispatch``'s seed status. A force job owns no request lifecycle, so
# the operator's starting status must survive its dispatch untouched; the
# harness default and the checkers read the same constant so they cannot
# drift apart.
_CALLER_RETAINED_STATUS = "downloading"
# The harness enqueues exactly one import job per generated world.
_GENERATED_JOB_ID = 1


def _preview_lease(job_id: int) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="generated-dispatch-boot",
        invocation_id=f"generated-dispatch-preview-{job_id}",
        systemd_unit="cratedigger-import-preview-worker.service",
        worker=ProcessIdentity(9101, 91001),
    )


def _importer_lease(job_id: int) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="generated-dispatch-boot",
        invocation_id=f"generated-dispatch-importer-{job_id}",
        systemd_unit="cratedigger-importer.service",
        worker=ProcessIdentity(9102, 91002),
    )


def _owned_test_runner(**kwargs):
    """Persist synthetic child proof before exercising the patched run seam."""
    from lib.dispatch.subprocess_runner import run_import_one

    on_spawn = kwargs.pop("on_spawn", None)
    cancellation_token = kwargs.pop("cancellation_token", None)
    kwargs.pop("owner_session_probe", None)
    if on_spawn is not None:
        on_spawn(os.getpid())
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()
    return run_import_one(**kwargs)


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
    initial_status: str = _CALLER_RETAINED_STATUS,
    force: bool = False,
    queued: bool = False,
    terminalize: str = "production",
) -> dict:
    """Established recipe (mirrors
    ``tests/test_dispatch_core.py::TestDispatchCoreOrchestration._dispatch``)
    for driving the REAL ``dispatch_import_core`` with a generated decision
    fed through the ``parse_import_result`` seam.

    ``terminalize="production"`` finalizes through the real queue owner
    (``scripts.importer.process_claimed_job``). ``terminalize="park"`` is the
    known-bad plant for the invariant-11 checkers below: it reproduces the
    REMOVED pre-#933 policy by stopping the owner job in
    ``recovery_required`` with the request still ``processing`` behind it —
    a request whose only exit is an operator command."""
    from lib.dispatch import dispatch_import_core

    # Automation owns a request until one terminal wanted/imported outcome;
    # caller-owned "retain current status" worlds belong to force import.
    force = force or not world.requeue_on_failure

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

    dl_info = DownloadInfo(username=world.source_username)

    root = tempfile.mkdtemp()
    processing_dir = os.path.join(root, "processing")
    processing_albums = os.path.join(processing_dir, "albums")
    incoming = os.path.join(root, "Incoming")
    os.makedirs(os.path.join(processing_albums, "failed_imports"))
    os.makedirs(incoming)
    tmpdir = tempfile.mkdtemp(dir=processing_albums)
    cfg = CratediggerConfig(
        beets_harness_path=_HARNESS,
        pipeline_db_enabled=True,
        processing_dir=processing_dir,
        beets_staging_dir=incoming,
    )
    try:
        del queued  # retained argument for existing generated call sites
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status=initial_status if force else "wanted",
            mb_release_id="mbid-generated",
            min_bitrate=180, current_spectral_bitrate=128,
            active_download_state={
                "files": [],
                "filetype": "mp3",
                "enqueued_at": "2026-07-29T00:00:00+00:00",
                "current_path": tmpdir,
            } if force else None,
        ))
        from lib.import_evidence import (
            ActionEvidenceProvenance,
            CandidateEvidenceActionResult,
        )
        from lib.import_queue import IMPORT_JOB_FORCE

        preview_lease: ExecutionLeaseSnapshot | None = None
        if force:
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": tmpdir},
            )
        else:
            job = handoff_automation_owner(
                db,
                42,
                state={
                    "files": [],
                    "filetype": "mp3",
                    "enqueued_at": "2026-07-29T00:00:00+00:00",
                    "current_path": tmpdir,
                },
                canonical_path=tmpdir,
            )
            preview_lease = _preview_lease(job.id)
            claimed_preview = claim_next_import_preview_job(db, worker_id="generated-dispatch-preview",
            execution_lease=preview_lease,)
            assert claimed_preview is not None
        with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
            handle.write(b"generated fixture audio")
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-generated",
            source_path=tmpdir,
            files=snapshot_audio_files(tmpdir),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        assert db.set_import_job_candidate_evidence(
            job.id,
            persisted.id,
            expected_execution_lease=preview_lease,
        )
        assert db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
            expected_execution_lease=preview_lease,
        )
        execution_lease = None if force else _importer_lease(job.id)
        claimed = claim_next_import_job(db, worker_id="generated-dispatch",
        execution_lease=execution_lease,)
        assert claimed is not None
        import_job_id = claimed.id
        candidate_result = CandidateEvidenceActionResult(
            evidence=persisted,
            provenance=ActionEvidenceProvenance(
                candidate_status="reused",
                snapshot_guard="matched",
            ),
        )
        cancellation_token = (
            CancellationToken() if execution_lease is not None else None
        )
        with patch_dispatch_externals(), \
             patch("lib.dispatch.subprocess_runner.parse_import_result",
                   return_value=ir), \
             pinned_dispatch_authority(
                 db,
                 execution_lease,
                 cancellation_token=cancellation_token,
             ) as (cancellation_token, owner_session_identity):
            result = dispatch_import_core(
                make_dispatch_request(
                    path=tmpdir,
                    mb_release_id='mbid-generated',
                    request_id=42,
                    label='Generated Artist - Generated Album',
                    beets_harness_path=cfg.beets_harness_path,
                    dl_info=dl_info,
                    distance=0.05,
                    scenario='force_import' if force else 'strong_match',
                    force=force,
                    files=[make_download_file(username=world.source_username or 'user1', filename='01 - Track.mp3')],
                    requeue_on_failure=world.requeue_on_failure,
                    candidate_import_job_id=import_job_id,
                    prevalidated_candidate_result=candidate_result,
                    beets_library_db_path=str(beets.library_db),
                    beets_library_root=str(beets.library_root),
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                ),
                db,
                cfg=cfg,
                quality_gate_fn=noop_quality_gate,
                cancellation_token=cancellation_token,
                run_import_fn=_owned_test_runner if execution_lease is not None else None,
            )
        if terminalize == "park":
            # Planted violation, not a production path: the owner job rests in
            # ``recovery_required`` (an ACTIVE queue status) while the request
            # stays ``processing`` behind it, exactly as the removed parking
            # policy left it. There is deliberately no production writer left,
            # so the known-bad self-test plants the impossible historical row.
            owner_row = next(
                row for row in db._import_jobs
                if row["id"] == import_job_id
            )
            owner_row["status"] = "recovery_required"
            owner_row["worker_id"] = None
            owner_row["heartbeat_at"] = None
        elif result.terminal_outcome is not None:
            from lib.terminal_outcomes import ImportJobTerminal

            if force:
                db.persist_import_terminal_outcome(
                    result.terminal_outcome.with_job(ImportJobTerminal(
                        status="completed" if result.success else "failed",
                        result={"success": result.success},
                        message=result.message,
                        error=None if result.success else result.message,
                    ))
                )
            else:
                finalize_claimed_dispatch(db, claimed, result)
        else:
            finalize_claimed_dispatch(db, claimed, result)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return {"db": db, "result": result}


# ===========================================================================
# World + harness — the entry-point lane/distance seam (issue #1211 F2).
# ===========================================================================
#
# ``_run_dispatch`` above deliberately calls ``dispatch_import_core``
# directly, bypassing ``lib.dispatch.entry_points.dispatch_import_from_db``
# entirely — so it never exercises the lane ternary this property patrols.
# The real space is lane x validation-distance-presence (four cells); the
# deterministic pins in ``tests/test_integration_slices.py`` occupy three
# of them — ``TestForceImportSlice.test_force_import_success``
# (force/None, incidentally: its fake harness path never starts, so the
# validation seam measures nothing), ``test_force_import_ignores_
# measured_distance`` (force/present, explicit 0.42), and
# ``TestLocalImportSlice.test_local_import_success`` (local/present,
# explicit 0.01). Only local/None has no deterministic pin. This property
# drives the REAL ``dispatch_import_from_db`` over the full two-lane x
# float-or-None distance grid, covering all four cells including that gap.

def _run_dispatch_from_db(
    lane: str,
    measured_distance: float | None,
    *,
    beets: BeetsWorld,
) -> dict:
    """Drive the REAL ``dispatch_import_from_db`` entry point.

    ``lane`` is one of the exact two ``scenario`` literals the sole
    production caller (``scripts/importer.py::execute_import_job``) ever
    passes. ``lib.beets.beets_validate`` is patched to return a PASSING
    (``valid=True``) verdict carrying ``measured_distance`` for both
    lanes, so both reach the accept-path ternary in
    ``lib.dispatch.entry_points._dispatch_import_from_db_locked`` rather
    than local-import's separate strict-validation reject branch — that
    branch is a pre-existing, unaffected-by-#1211 path already covered by
    ``TestLocalImportSlice.test_local_import_strict_reject_lands_as_
    ordinary_wrong_match`` in ``tests/test_integration_slices.py``.
    """
    from lib.dispatch import dispatch_import_from_db
    from lib.import_queue import IMPORT_JOB_FORCE, IMPORT_JOB_LOCAL
    from lib.pipeline_db.download_log import DownloadLogOutcome
    from lib.quality import ValidationResult

    scenario: DownloadLogOutcome = (
        "force_import" if lane == "force_import" else "local_import"
    )

    root = tempfile.mkdtemp()
    processing_dir = os.path.join(root, "processing")
    processing_albums = os.path.join(processing_dir, "albums")
    os.makedirs(processing_albums)
    tmpdir = tempfile.mkdtemp(dir=processing_albums)
    cfg = CratediggerConfig(
        beets_harness_path=_HARNESS,
        pipeline_db_enabled=True,
        processing_dir=processing_dir,
        beets_distance_threshold=0.15,
    )
    try:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id="mbid-lane-distance",
        ))
        db.set_tracks(42, [{"track_number": 1, "title": "Track"}])
        with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
            handle.write(b"generated fixture audio")

        job_type = IMPORT_JOB_FORCE if lane == "force_import" else IMPORT_JOB_LOCAL
        payload = (
            {"download_log_id": 1, "failed_path": tmpdir}
            if lane == "force_import"
            else {"source_path": tmpdir, "request_id": 42}
        )
        job = db.enqueue_import_job(job_type, request_id=42, payload=payload)
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-lane-distance",
            source_path=tmpdir,
            files=snapshot_audio_files(tmpdir),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        assert db.set_import_job_candidate_evidence(job.id, persisted.id)
        assert db.mark_import_job_preview_importable(
            job.id, preview_result={"ready": True},
        )
        claimed = claim_next_import_job(db, worker_id="generated-dispatch-from-db")
        assert claimed is not None

        ir = make_import_result(decision="import", new_min_bitrate=320)
        distance_threshold = (
            cfg.beets_distance_threshold if lane == "local_import" else None
        )
        with patch_dispatch_externals(), \
             patch("lib.dispatch.subprocess_runner.parse_import_result",
                   return_value=ir), \
             patch("lib.beets.beets_validate", return_value=ValidationResult(
                 valid=True, scenario="strong_match",
                 distance=measured_distance, target_mbid="mbid-lane-distance",
             )), \
             patch("lib.config.read_runtime_config", return_value=cfg):
            result = dispatch_import_from_db(
                db, request_id=42, failed_path=tmpdir,
                import_job_id=claimed.id,
                distance_threshold=distance_threshold,
                scenario=scenario,
                quality_gate_fn=noop_quality_gate,
                beets_library_db_path=str(beets.library_db),
                beets_library_root=str(beets.library_root),
            )
        finalize_claimed_dispatch(db, claimed, result)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return {"db": db, "result": result}


class TestGeneratedLaneDistanceAudit(unittest.TestCase):
    """Issue #1211 F2 — the lane x distance-presence grid, through the
    real entry point."""

    def setUp(self) -> None:
        self.beets = BeetsWorld(_REPO_ROOT)
        self.addCleanup(self.beets.close)

    # code-quality.md: "an entropy budget miss [gets] pin[ned as] the
    # decisive world as an `@example`". `measured=None` lands exactly
    # once per lane in 150 derandomized examples, and local/None is the
    # one cell no deterministic pin covers (see the harness comment
    # above) — nothing else in this property proves that cell is
    # exercised. All four lane x measured-presence cells are pinned
    # explicitly so a future Hypothesis version or strategy tweak cannot
    # silently drop the edge probe and still report green.
    @example(lane="force_import", measured=None)
    @example(lane="force_import", measured=0.42)
    @example(lane="local_import", measured=None)
    @example(lane="local_import", measured=0.42)
    @given(
        lane=st.sampled_from(("force_import", "local_import")),
        measured=st.one_of(
            st.none(),
            st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
        ),
    )
    def test_persisted_distance_matches_lane(self, lane, measured):
        """The accept-path terminal audit records the measured distance
        for local-import and NULL for force-import, regardless of what
        the validation seam actually measured — force's own suppression
        is deliberate, not an absence of a measurement (issue #1211)."""
        outcome = _run_dispatch_from_db(lane, measured, beets=self.beets)
        self.assertTrue(outcome["result"].success)
        expected = measured if lane == "local_import" else None
        row = outcome["db"].request(42)
        self.assertEqual(row["beets_distance"], expected)
        outcome["db"].assert_log(self, 0, beets_distance=expected)


# ===========================================================================
# World + harness — evidence-decision reject path (owns the U11 override).
# ===========================================================================

def _reject_via_evidence_decision(
    *, decision: str, requeue_on_failure: bool, new_min_bitrate: int,
    source_username: str | None = "user1",
    distance: float | None = 0.0,
    starting_status: str = "downloading",
) -> FakePipelineDB:
    """Established recipe (mirrors
    ``tests/test_import_dispatch.py::TestRejectImportFromEvidenceDecisionCallerLifecycle._reject``)
    for driving the REAL ``_reject_import_from_evidence_decision`` directly
    with a generated ``decision`` string.

    ``distance`` defaults to ``0.0`` for the pre-existing self-heal
    properties above (they don't care about the value); the #550 defect #4
    properties below pass a generated ``float | None`` to prove the helper
    threads it through unchanged to both persisted sinks. ``starting_status``
    defaults to ``downloading`` for every pre-existing caller; the issue
    #1355 item A4 property below draws it from ``{"downloading",
    "unsearchable"}`` to prove the job-less rejection bundle preserves an
    operator stop instead of clearing it unconditionally."""
    from lib.dispatch import _reject_import_from_evidence_decision

    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42, status=starting_status, mb_release_id="test-mbid"))
    dl_info = DownloadInfo(filetype="mp3", username=source_username)
    ir = make_import_result(decision=decision, new_min_bitrate=new_min_bitrate)
    attempt_result = ImportAttemptResult(None)
    attempt_result.merge(ir)
    with patch_dispatch_externals():
        _reject_import_from_evidence_decision(
            make_dispatch_request(
                request_id=42,
                dl_info=dl_info,
                distance=distance,
                requeue_on_failure=requeue_on_failure,
                path='/tmp/cratedigger-generated-reject-test',
                scenario=decision,
                files=None,
                cooled_down_users=None,
            ),
            db,
            attempt_result=attempt_result,
            decision=decision,
            detail=f'generated {decision}',
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
                make_dispatch_request(
                    request_id=42,
                    # The envelope rides ``dl_info`` — exactly how production
                    # carries an already-decided validation result into the
                    # reject helper (issue #1277).
                    dl_info=DownloadInfo(
                        username="generated-user",
                        validation_result=validation_result.to_json(),
                    ),
                    distance=distance,
                    requeue_on_failure=True,
                    path="/tmp/generated-staged",
                    scenario=scenario or "generated_reject",
                    files=None,
                    cooled_down_users=None,
                ),
                db,
                attempt_result=attempt_result,
                decision="downgrade",
                detail="generated reject",
            )
        return db

    if writer == "dispatch_rejection":
        from lib.dispatch import _record_rejection_and_maybe_requeue

        _record_rejection_and_maybe_requeue(
            db=db,
            request_id=42,
            dl_info=DownloadInfo(username="generated-user"),
            detail=validation_result.detail,
            error=validation_result.error,
            validation_result=validation_result.to_json(),
            requeue=True,
        )
        return db

    if writer == "database_source":
        source = DatabaseSource(
            "unused-generated-dsn",
            musicbrainz_ws2_base="http://musicbrainz-mirror.test/ws/2",
            discogs_api_base="http://discogs-mirror.test",
        )
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
    attach_import_job: bool = True,
) -> FakePipelineDB:
    """Drive the real current-evidence gate through its terminal DB bundle.

    ``attach_import_job=False`` is the queue-less production shape (no
    ``candidate_import_job_id``), which makes ``_record_have_analysis_error``
    write directly and return a row id instead of a terminal bundle. It is the
    Q1 world for this harness's own bundle guard below.
    """

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
        status="unsearchable" if mode == "force" else "wanted",
        search_filetype_override=search_override,
        active_download_state=(
            {"files": [], "filetype": "flac"}
            if mode == "force"
            else None
        ),
    ))
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

    with tempfile.TemporaryDirectory() as root:
        processing_dir = os.path.join(root, "processing")
        if mode == "auto":
            os.makedirs(os.path.join(processing_dir, "albums"))
            tmpdir = os.path.join(processing_dir, "albums", "request-42")
            os.mkdir(tmpdir)
        else:
            tmpdir = root
        with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
            handle.write(b"generated HAVE fixture")
        payload = (
            {}
            if mode == "auto"
            else {"download_log_id": 1, "failed_path": tmpdir}
        )
        preview_lease: ExecutionLeaseSnapshot | None = None
        if mode == "auto":
            job = handoff_automation_owner(
                db,
                42,
                state={
                    "files": [],
                    "filetype": "flac",
                    "enqueued_at": "2026-07-29T00:00:00+00:00",
                    "current_path": tmpdir,
                },
                canonical_path=tmpdir,
            )
            preview_lease = _preview_lease(job.id)
            claimed_preview = claim_next_import_preview_job(db, worker_id="generated-have-preview",
            execution_lease=preview_lease,)
            assert claimed_preview is not None
        else:
            job = db.enqueue_import_job(
                job_type,
                request_id=42,
                payload=payload,
            )
        candidate = make_album_quality_evidence(
            mb_release_id="generated-have-analysis-mbid",
            source_path=tmpdir,
            files=snapshot_audio_files(tmpdir),
        )
        db.upsert_album_quality_evidence(candidate)
        persisted = db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        assert db.set_import_job_candidate_evidence(
            job.id,
            persisted.id,
            expected_execution_lease=preview_lease,
        )
        execution_lease: ExecutionLeaseSnapshot | None = None
        claimed = None
        if mode == "auto":
            assert db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
                expected_execution_lease=preview_lease,
            )
            execution_lease = _importer_lease(job.id)
            claimed = claim_next_import_job(db, worker_id="generated-have-importer",
            execution_lease=execution_lease,)
            assert claimed is not None
        candidate_result = CandidateEvidenceActionResult(
            evidence=persisted,
            provenance=ActionEvidenceProvenance(
                candidate_status="reused",
                snapshot_guard="matched",
            ),
        )
        cancellation_token = (
            CancellationToken() if execution_lease is not None else None
        )
        with patch_dispatch_externals(), pinned_dispatch_authority(
            db,
            execution_lease,
            cancellation_token=cancellation_token,
        ) as (cancellation_token, owner_session_identity):
            outcome = dispatch_import_core(
                make_dispatch_request(
                    path=tmpdir,
                    mb_release_id='generated-have-analysis-mbid',
                    request_id=42,
                    label='Generated Artist - Generated Album',
                    force=mode == 'force',
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype='flac', username=username),
                    scenario=scenario,
                    requeue_on_failure=mode == 'auto',
                    candidate_import_job_id=job.id if attach_import_job else None,
                    prevalidated_candidate_result=candidate_result,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                ),
                db,
                cfg=CratediggerConfig(beets_harness_path=_HARNESS, pipeline_db_enabled=True, processing_dir=processing_dir),
                quality_gate_fn=noop_quality_gate,
                current_evidence_loader=lambda *_args, **_kwargs: current_result,
                cancellation_token=cancellation_token,
            )
    if outcome.terminal_outcome is None:
        raise AssertionError("HAVE-analysis abort did not build a terminal outcome")
    db.set_cooldown_result(cooldown_verdict)
    if mode == "auto":
        assert claimed is not None
        finalize_claimed_dispatch(db, claimed, outcome)
    else:
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


def assert_request_never_parked(
    db: FakePipelineDB,
    *,
    retained_status: str = _CALLER_RETAINED_STATUS,
) -> None:
    """CLAUDE.md invariant 11: nothing a dispatch touches is ever parked.

    Whatever the outcome — accepted, rejected, or an ambiguous world failure
    — the finalized world must hold no state whose only exit is a human
    command. The owner job reaches a terminal queue status (production counts
    ``recovery_required`` as ACTIVE, so resting there IS parking), no active
    automation owner stays attached, and the request itself is left runnable:
    an automation owner's request lands exactly ``wanted`` (searchable again)
    or ``imported`` (acquired) with its owned download state released, while a
    force job owns no request lifecycle so the operator's starting status
    survives unless the outcome legitimately moved it to a runnable one.
    """
    job = db.get_import_job(_GENERATED_JOB_ID)
    if job is None:
        raise AssertionError(
            "dispatch finalized with no import job row to read")
    if job.status not in _TERMINAL_IMPORT_JOB_STATUSES:
        raise AssertionError(
            f"{job.job_type} job {job.id} rested in non-terminal status "
            f"{job.status!r}; invariant 11 forbids a queue state whose only "
            f"exit is an operator command (terminal: "
            f"{sorted(_TERMINAL_IMPORT_JOB_STATUSES)})")
    row = db.request(42)
    if row["active_automation_import_job_id"] is not None:
        raise AssertionError(
            "terminal dispatch left active_automation_import_job_id="
            f"{row['active_automation_import_job_id']!r} attached; the "
            "request is owned by a finished job and get_wanted() will never "
            "select it again")
    status = row["status"]
    if status == "processing":
        raise AssertionError(
            "terminal dispatch left the request in 'processing' behind an "
            "inactive job — the silent stop invariant 11 exists to forbid")
    if job.job_type == IMPORT_JOB_AUTOMATION:
        if status not in _RUNNABLE_TERMINAL_REQUEST_STATUSES:
            raise AssertionError(
                f"automation terminal left status={status!r}, want one of "
                f"{sorted(_RUNNABLE_TERMINAL_REQUEST_STATUSES)}")
        if row["active_download_state"] is not None:
            raise AssertionError(
                "automation terminal left owned download state attached")
    elif status not in _RUNNABLE_TERMINAL_REQUEST_STATUSES | {retained_status}:
        raise AssertionError(
            f"caller-retained terminal left status={status!r}, want the "
            f"operator's {retained_status!r} or one of "
            f"{sorted(_RUNNABLE_TERMINAL_REQUEST_STATUSES)}")


def assert_outcome_is_operator_visible(
    db: FakePipelineDB, outcome: DispatchOutcome,
) -> None:
    """Invariant 11's first step: the outcome is RECORDED, never silent.

    Every dispatch that produced a terminal outcome bundle, and every
    automation outcome whether it produced one or not (an ambiguous
    automation world failure self-heals through its own audit row), writes
    exactly one ``download_log`` row — and that row must render for the
    operator through the REAL Recents projection rather than a taxonomy
    re-listed here. A force/YouTube job owns no request lifecycle, so a
    bundle-less failure of one carries its diagnostic on the terminal job
    row instead; it still may not be silent.
    """
    job = db.get_import_job(_GENERATED_JOB_ID)
    if job is None:
        raise AssertionError(
            "dispatch finalized with no import job row to read")
    automation = job.job_type == IMPORT_JOB_AUTOMATION
    if outcome.terminal_outcome is None and not automation:
        if job.status != "failed":
            raise AssertionError(
                f"bundle-less caller-retained outcome left job status="
                f"{job.status!r}, want 'failed' — the job row is this "
                "outcome's only operator surface")
        if not (job.message or job.error):
            raise AssertionError(
                "bundle-less caller-retained failure recorded no readable "
                "message or error for the operator")
        return
    assert_download_log_row_created(db)
    if len(db.download_logs) != 1:
        raise AssertionError(
            f"one dispatch wrote {len(db.download_logs)} audit rows; the "
            "operator reads exactly one per outcome")
    log = db.download_logs[-1]
    # Drive the production Recents lens (``get_log``'s imported/problems
    # outcome filters) instead of asserting against a copied outcome list:
    # an outcome string no operator view selects is an invisible outcome.
    lens = "imported" if outcome.success else "rejected"
    visible = db.get_log(outcome_filter=lens)
    if not any(int(entry["id"]) == log.id for entry in visible):
        raise AssertionError(
            f"audit row outcome={log.outcome!r} never renders under the "
            f"operator's {lens!r} Recents filter — the outcome is invisible")


def assert_world_failure_self_heals(
    db: FakePipelineDB,
    outcome: DispatchOutcome,
    *,
    retained_status: str = _CALLER_RETAINED_STATUS,
) -> None:
    """Invariant 11 for the ambiguous world, which is its canonical case.

    A Beets child that was launched and never acknowledged leaves the world
    unknowable from here: it may or may not have mutated the library. The
    importer no longer adjudicates that with a human — it records the
    ambiguity as audit evidence under its own world-failure label, returns
    the request to the search pool with retry accounting retained (so a
    permanently broken world backs off instead of hot-looping), and lets the
    next cycle re-derive the truth from the request. "Ambiguity writes no
    audit" was the OLD policy and is now itself a violation: the audit row is
    how the operator learns the world needs fixing.
    """
    from scripts.importer import _WORLD_FAILURE_AUDIT_PREFIX

    if outcome.success:
        raise AssertionError("ambiguous acknowledgement reported success=True")
    assert_request_never_parked(db, retained_status=retained_status)
    assert_outcome_is_operator_visible(db, outcome)
    job = db.get_import_job(_GENERATED_JOB_ID)
    if job is None or job.job_type != IMPORT_JOB_AUTOMATION:
        # A force/YouTube ambiguity never owned the request's ``processing``
        # status, so its terminal job row (already proven above) is the whole
        # surface this outcome needs.
        return
    row = db.request(42)
    if row["status"] != "wanted":
        raise AssertionError(
            f"automation world failure left status={row['status']!r}, want "
            "'wanted' — the request must go back into the search pool")
    log = db.download_logs[-1]
    detail = " ".join(
        part for part in (log.beets_detail, log.error_message) if part
    )
    if _WORLD_FAILURE_AUDIT_PREFIX not in detail:
        raise AssertionError(
            "world-failure audit row does not carry the importer's own "
            f"world-failure label {_WORLD_FAILURE_AUDIT_PREFIX!r}: "
            f"{detail!r}")
    if not row["validation_attempts"] or row["next_retry_after"] is None:
        raise AssertionError(
            "self-heal dropped retry accounting (validation_attempts="
            f"{row['validation_attempts']!r}, next_retry_after="
            f"{row['next_retry_after']!r}); invariant 11 backs a broken world "
            "off with attempts plus growing backoff, never by parking it")


def assert_dispatch_outcome_matches_routing(
    world: DispatchWorld, db: FakePipelineDB, outcome: DispatchOutcome,
) -> None:
    """The auditability + success/self-heal oracle for the legacy dispatch
    path: whatever ``dispatch_action(decision)`` prescribes is what actually
    landed in the DB — for the ambiguous no-JSON path AND every known
    decision string.
    """
    status = db.request(42)["status"]

    if world.mode == "no_json":
        # An unacknowledged Beets child is a broken world, not a parking
        # ticket: ``dispatch_action`` has no routing for it, so the importer's
        # self-heal is the routing (CLAUDE.md invariant 11).
        assert_world_failure_self_heals(db, outcome)
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
        raise AssertionError(  # noqa: TRY004 - generated invariant failure
            "rejection writer did not persist an object envelope"
        )
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


def assert_denylist_writes_are_bundled(
    db: FakePipelineDB, request_id: int,
) -> None:
    """Issue #1355 item 3 (widened by item A2): every ``source_denylist``
    row for ``request_id`` must be traceable to a ``denylists`` entry on
    ONE of the three terminal-outcome commits (job-backed
    ``persist_import_terminal_outcome``, job-less
    ``persist_request_rejection_outcome``, or job-less
    ``persist_request_policy_outcome``) — never a separate
    ``db.add_denylist`` call outside any of them. A writer that reverts to
    denylisting a peer in its own loop, after already committing the
    rejection's transition and audit row, reintroduces the exact
    partial-write window this issue exists to close.
    """
    bundled_usernames = {
        entry.username
        for command in db.persist_request_rejection_outcome_calls
        if command.request_id == request_id
        for entry in command.denylists
    } | {
        entry.username
        for command in db.persist_import_terminal_outcome_calls
        if command.request_id == request_id
        for entry in command.denylists
    } | {
        entry.username
        for command in db.persist_request_policy_outcome_calls
        if command.request_id == request_id
        for entry in command.denylists
    }
    written_usernames = {
        entry.username
        for entry in db.denylist
        if entry.request_id == request_id
    }
    if bundled_usernames != written_usernames:
        raise AssertionError(
            "source_denylist rows drifted from the terminal-outcome "
            f"bundle: bundled={sorted(bundled_usernames)!r} written="
            f"{sorted(written_usernames)!r}"
        )


def assert_writer_denylisted_username(
    db: FakePipelineDB, request_id: int, username: str,
) -> None:
    """Companion to ``assert_denylist_writes_are_bundled``: that checker
    only compares the bundled and written SETS, so a writer that stops
    supplying ``denylists`` entirely (both sides empty) passes it silently
    — a mutant-runner finding on issue #1355 item 3 (M15/M27): dropping
    ``denylists=denylists`` from either ``_pending_rejection_outcome`` or
    ``_reject_request_auto_import``'s call survived every generated
    assertion in this module. This asserts the username a writer's own
    world-failure/downgrade policy is KNOWN to always denylist actually
    landed in ``source_denylist`` — proving the write happened at all, not
    only that it was bundled when it did.
    """
    written_usernames = {
        entry.username
        for entry in db.denylist
        if entry.request_id == request_id
    }
    if username not in written_usernames:
        raise AssertionError(
            f"expected {username!r} to be denylisted for request "
            f"{request_id}; source_denylist has {sorted(written_usernames)!r}"
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


def assert_wrong_match_evidence_link_isolated(
    *,
    cleanup_call_count: int,
    terminal_log: DownloadLogRow,
    expected_candidate_evidence_id: int | None,
) -> None:
    """A delete-ineligible scenario never enters the destructive WM reducer.

    Issue #1077, D6: the cleanup lane ("evaluate and possibly delete") is an
    explicit allowlist. Evidence-linking may still run (any scenario the
    worklist visibility predicate admits gets linked, even when it is not
    delete-eligible), but the reducer itself must never be consulted.
    """
    if cleanup_call_count:
        raise AssertionError("delete-ineligible scenario reached Wrong Matches cleanup")
    if terminal_log.candidate_evidence_id != expected_candidate_evidence_id:
        raise AssertionError("terminal audit lost its candidate evidence link")
    validation = terminal_log.validation_result
    if isinstance(validation, str):
        validation = json.loads(validation)
    if isinstance(validation, dict) and "wrong_match_triage" in validation:
        raise AssertionError("delete-ineligible terminal audit gained deletion triage")


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
    def test_every_outcome_is_audited_and_never_parks_the_request(self, world):
        """CLAUDE.md invariant 11 over the whole dispatch world space.

        Ambiguous worlds are included deliberately: under the removed policy
        an unacknowledged Beets child wrote no audit and stopped in
        ``recovery_required``, which is exactly the silent stop this property
        now forbids.
        """
        outcome = _run_dispatch(world, beets=self.beets)
        assert_outcome_is_operator_visible(outcome["db"], outcome["result"])
        assert_request_never_parked(outcome["db"])

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


class TestGeneratedJobLessRejectionPreservesOperatorStop(unittest.TestCase):
    """Issue #1355 item A4: the job-less rejection bundle
    (``persist_request_rejection_outcome``) must preserve an operator
    ``unsearchable`` stop rather than clear it unconditionally when it
    requeues to ``wanted``. Drives the REAL
    ``_reject_import_from_evidence_decision`` over both the operator-stop
    and the ordinary starting status, across the full preimport-fact
    decision taxonomy."""

    @given(
        decision=st.sampled_from(sorted(_PREIMPORT_FACT_REJECT_DECISIONS)),
        starting_status=st.sampled_from(("downloading", "unsearchable")),
    )
    def test_requeue_respects_the_starting_operator_stop(
        self, decision, starting_status,
    ):
        db = _reject_via_evidence_decision(
            decision=decision,
            requeue_on_failure=True,
            new_min_bitrate=320,
            starting_status=starting_status,
        )
        row = db.request(42)
        expected = (
            "unsearchable" if starting_status == "unsearchable" else "wanted"
        )
        self.assertEqual(row["status"], expected)


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
        assert_denylist_writes_are_bundled(db, 42)
        # World failures always denylist their contributing peer (D1/D4) —
        # proves the write actually happened, which the bundling check
        # alone cannot (both sides read empty if it silently stopped).
        assert_writer_denylisted_username(db, 42, "generated-user")


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
                    # Issue #1077, D6: the cleanup lane is an explicit
                    # allowlist. A `None` scenario is never delete-eligible,
                    # so `_run_post_rejection_wrong_match_cleanup` returns
                    # before ever calling the reducer — no `wrong_match_
                    # triage` audit is written.
                    self.assertNotIn(
                        "wrong_match_triage",
                        payload,
                        "a None scenario must never reach the delete-"
                        "eligible cleanup reducer",
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
        assert_denylist_writes_are_bundled(db, 42)
        # This helper always drives "evidence_decision" through a fixed
        # decision="downgrade" (dispatch_action("downgrade").denylist is
        # unconditionally True) and "request_auto_import" through a world
        # failure (always denylisted, D1/D4) — both independent of the
        # generated distance/scenario, so both always write exactly
        # "generated-user". "database_source"/"dispatch_rejection" never
        # denylist here (neither passes usernames), so they are correctly
        # excluded rather than asserted empty.
        if writer in ("evidence_decision", "request_auto_import"):
            assert_writer_denylisted_username(db, 42, "generated-user")


# ===========================================================================
# World + harness — the two job-less accept-path writers (issue #1355
# items A1/A2). Both drive the REAL writer directly, mirroring how
# ``_reject_via_evidence_decision`` above drives ``_reject_import_from_
# evidence_decision`` directly rather than through the full
# ``dispatch_import_core`` funnel (every existing harness in this module
# attaches a real job — force or automation — before dispatching, so none
# of them ever reaches ``import_job_id is None``/``pending is None``).
# ===========================================================================

def _run_mark_done_job_less(
    *, distance: float | None, scenario: str | None,
    source_username: str | None = "user1",
) -> FakePipelineDB:
    """Drive the REAL job-less ``_do_mark_done`` branch directly (issue
    #1355 item A1): no owning import job, so the transition and the
    mandatory ``download_log`` audit row must commit through
    ``persist_request_success_outcome`` rather than as two separate
    writes."""
    from lib.dispatch.outcome_actions import _do_mark_done

    db = FakePipelineDB()
    db.seed_request(make_request_row(id=42, status="downloading"))
    dl_info = DownloadInfo(filetype="mp3", username=source_username)
    _do_mark_done(
        db, 42, dl_info,
        distance=distance, scenario=scenario,
        dest_path="/tmp/cratedigger-generated-mark-done",
        import_job_id=None,
    )
    return db


def _run_quality_gate_job_less(
    *, decision: QualityGateDecision, usernames: tuple[str, ...],
) -> FakePipelineDB:
    """Drive the REAL job-less ``_check_quality_gate_core`` ``apply=True``
    branch directly (issue #1355 item A2): no ``PendingImportTerminalOutcome``
    to stage onto, so its transition and every denylist entry must commit
    through ``persist_request_policy_outcome``."""
    from lib.dispatch.quality_gate import _check_quality_gate_core
    from lib.dispatch.types import QualityGateState
    from lib.quality import AudioQualityMeasurement

    db = FakePipelineDB()
    db.seed_request(make_request_row(id=42, status="imported"))

    def state_loader(**_kwargs: object) -> QualityGateState:
        return QualityGateState(
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade="genuine",
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
            verified_lossless_proof=(decision == "accept"),
        )

    def decision_fn(current: object, cfg: object = None, *,
                     target_contract: object = None,
                     verified_lossless_proof: bool = False,
                     ) -> QualityGateDecision:
        del current, cfg, target_contract, verified_lossless_proof
        return decision

    _check_quality_gate_core(
        mb_id="mbid-generated-policy",
        label="Generated Artist - Generated Album",
        request_id=42,
        files=[
            make_download_file(username=u, filename=f"{u}.mp3")
            for u in usernames
        ],
        db=db,
        state_loader=state_loader,
        quality_decision_fn=decision_fn,
    )
    return db


class TestGeneratedMarkDoneJobLessBundle(unittest.TestCase):
    """Issue #1355 item A1: the job-less success bundle."""

    @given(
        distance=st.one_of(
            st.none(),
            st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
        ),
        scenario=st.one_of(
            st.none(),
            st.text(min_size=0, max_size=20),
        ),
    )
    def test_download_log_row_created_and_status_imported(
        self, distance, scenario,
    ):
        db = _run_mark_done_job_less(distance=distance, scenario=scenario)
        assert_download_log_row_created(db)
        self.assertEqual(db.request(42)["status"], "imported")
        self.assertEqual(len(db.persist_request_success_outcome_calls), 1)
        # No orphan write: the job-less success path never reaches
        # ``log_download`` directly any more (issue #1355 item A1).
        self.assertEqual(len(db.download_logs), 1)


class TestGeneratedQualityGateJobLessPolicyBundle(unittest.TestCase):
    """Issue #1355 item A2: the job-less quality-gate policy bundle."""

    @given(
        usernames=st.lists(
            st.sampled_from(("peer-a", "peer-b", "peer-c")),
            min_size=1, max_size=3, unique=True,
        ),
    )
    def test_denylist_writes_are_bundled(self, usernames):
        db = _run_quality_gate_job_less(
            decision="requeue_upgrade", usernames=tuple(usernames),
        )
        assert_denylist_writes_are_bundled(db, 42)
        self.assertEqual(len(db.persist_request_policy_outcome_calls), 1)
        self.assertEqual(
            {d.username for d in db.denylist}, set(usernames),
        )

    def test_accept_decision_writes_no_denylist(self):
        db = _run_quality_gate_job_less(decision="accept", usernames=())
        assert_denylist_writes_are_bundled(db, 42)
        self.assertEqual(db.request(42)["status"], "imported")
        self.assertEqual(db.denylist, [])


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

    def test_retained_policy_preserves_starting_search_lifecycle(self) -> None:
        cases = product(
            sorted(_AUTOMATIC_RETAINED_ACTIONS),
            ("wanted", "unsearchable"),
        )
        for decision, initial_status in cases:
            with self.subTest(
                decision=decision,
                initial_status=initial_status,
            ):
                self._assert_retained_policy(
                    decision=decision,
                    initial_status=initial_status,
                )

    def _assert_retained_policy(
        self,
        *,
        decision: str,
        initial_status: str,
    ) -> None:
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


class TestGeneratedDeleteIneligibleScenarioIsolation(unittest.TestCase):
    """Issue #1077, D6: the cleanup lane is an allowlist, not a fail-open set.

    None of these five scenarios is in ``DELETE_ELIGIBLE_REJECTION_SCENARIOS``
    — ``audio_corrupt`` is additionally excluded from worklist visibility
    itself (D3: bad rips are ban + delete, never quarantined, so evidence-
    linking is never even attempted for it), while the other four are
    visible-but-not-delete-eligible (world failures / unknown strings /
    ``None``): evidence-linking may run for them, but the reducer must not.
    """

    def test_delete_ineligible_scenario_never_reaches_wrong_match_cleanup(
        self,
    ) -> None:
        scenarios = (
            None,
            "force_import",
            "strong_mismatch",
            "audio_corrupt",
            "untracked_audio",
        )
        for scenario, link_fault in product(
            scenarios,
            ("none", "read", "write"),
        ):
            with self.subTest(scenario=scenario, link_fault=link_fault):
                self._assert_isolated(
                    scenario=scenario,
                    link_fault=link_fault,
                )

    def _assert_isolated(
        self,
        *,
        scenario: str | None,
        link_fault: str,
    ) -> None:
        from lib.import_queue import IMPORT_JOB_FORCE
        from lib.wrong_match_policy import (
            rejection_scenario_is_wrong_match_candidate,
        )
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
            message="rejected",
            post_commit_wrong_match_scenario=scenario,
        )

        # audio_corrupt fails the OUTER (worklist-visibility) gate, so
        # evidence-linking is never attempted for it at all; the other four
        # scenarios pass that gate but fail the INNER (delete-eligible) one.
        evidence_linking_attempted = rejection_scenario_is_wrong_match_candidate(
            scenario
        )
        evidence_link_patch = (
            patch.object(
                db,
                "get_import_job_candidate_evidence_id",
                side_effect=RuntimeError("generated evidence read failure"),
            )
            if link_fault == "read"
            else patch.object(
                db,
                "set_download_log_candidate_evidence",
                side_effect=RuntimeError("generated evidence write failure"),
            )
            if link_fault == "write"
            else nullcontext()
        )
        cleanup_wrong_match = MagicMock()
        with evidence_link_patch, patch(
            "scripts.importer.logger.exception",
        ) as log_exception:
            _cleanup_committed_wrong_match_rejection(
                db,  # pyright: ignore[reportArgumentType]
                job,
                log_id,
                outcome,
                cleanup_wrong_match_fn=cleanup_wrong_match,
            )
        self.assertEqual(
            log_exception.call_count,
            1 if evidence_linking_attempted and link_fault != "none" else 0,
        )

        assert_wrong_match_evidence_link_isolated(
            cleanup_call_count=cleanup_wrong_match.call_count,
            terminal_log=db.download_logs[-1],
            expected_candidate_evidence_id=(
                persisted.id
                if evidence_linking_attempted and link_fault == "none"
                else None
            ),
        )


# ===========================================================================
# Harness self-tests (RED/GREEN of the fuzzer itself) — each invariant
# checker must trip on a planted violation, and a planted-bad router must
# be caught end-to-end through the Hypothesis machinery.
# ===========================================================================

class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: prove the harness detects what it claims to.

    Per-clause proof (#1094): every checker raise site above owns a
    world here that makes THAT clause's condition true while every earlier
    clause in the same function passes, and asserts that clause's own
    message. A bare ``assertRaises`` would let a short-circuiting checker
    claim a clause it never evaluated.
    """

    def _ambiguous_world(self) -> DispatchWorld:
        return DispatchWorld(
            mode="no_json", decision=None, new_min_bitrate=None,
            prev_min_bitrate=None, spectral_grade="genuine",
            spectral_bitrate=None, was_converted=False,
            requeue_on_failure=True, source_username="user1")

    def _beets_world(self) -> BeetsWorld:
        """One real-Beets scratch world for the dispatch-driven plants."""
        beets = BeetsWorld(_REPO_ROOT)
        self.addCleanup(beets.close)
        runtime = patch.dict(os.environ, {
            "CRATEDIGGER_RUNTIME_CONFIG": str(beets.poisoned_runtime_config()),
            "BEETS_DB": str(beets.root / "poisoned-library.db"),
        })
        runtime.start()
        self.addCleanup(runtime.stop)
        return beets

    def _planted_terminal_world(
        self,
        *,
        automation: bool = True,
        job_status: str = "failed",
        request_status: str = "wanted",
        owner_attached: bool = False,
        active_download_state: dict[str, object] | None = None,
        job_message: str | None = "generated terminal message",
        job_error: str | None = "generated terminal error",
    ) -> FakePipelineDB:
        """One finalized world, planted directly, violating one clause.

        Real dispatch cannot build most of these — the terminal command
        refuses them (see the module docstring's per-clause notes) — which
        is exactly why the Q1 worlds are planted rather than dispatched.
        """
        from lib.import_queue import IMPORT_JOB_FORCE

        state = {
            "files": [],
            "filetype": "mp3",
            "enqueued_at": "2026-07-29T00:00:00+00:00",
            "current_path": "/processing/albums/request-42",
        }
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id="mbid-generated"))
        if automation:
            job = handoff_automation_owner(
                db,
                42,
                state=state,
                canonical_path="/processing/albums/request-42",
            )
        else:
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={
                    "download_log_id": 1,
                    "failed_path": "/processing/albums/wrong_matches/album",
                },
            )
        assert job.id == _GENERATED_JOB_ID
        row = next(r for r in db._import_jobs if r["id"] == job.id)
        row["status"] = job_status
        row["message"] = job_message
        row["error"] = job_error
        request = db.request(42)
        request["status"] = request_status
        request["active_automation_import_job_id"] = (
            job.id if owner_attached else None
        )
        request["active_download_state"] = active_download_state
        return db

    def _routing_db(
        self,
        *,
        status: str,
        log_outcome: str,
        **row_overrides: object,
    ) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status=status, **row_overrides))
        db.log_download(request_id=42, outcome=log_outcome)
        return db

    def _routing_world(
        self, decision: str, *, requeue_on_failure: bool = True,
    ) -> DispatchWorld:
        return DispatchWorld(
            mode="decision", decision=decision, new_min_bitrate=245,
            prev_min_bitrate=None, spectral_grade="genuine",
            spectral_bitrate=None, was_converted=False,
            requeue_on_failure=requeue_on_failure, source_username="user1")

    def test_rejection_writer_harness_refuses_an_unknown_writer(self):
        """Adding a writer name without a branch fails loudly, not silently."""
        with self.assertRaisesRegex(
            AssertionError, r"^unknown rejection writer 'phantom_writer'$",
        ):
            _run_rejection_writer(
                writer="phantom_writer", distance=None, scenario=None)

    def test_have_analysis_harness_requires_a_terminal_bundle(self):
        """The queue-less shape produces no bundle; the harness says so."""
        with self.assertRaisesRegex(
            AssertionError,
            r"^HAVE-analysis abort did not build a terminal outcome$",
        ):
            _run_have_analysis_abort(
                mode="force",
                raw_error=_HAVE_ANALYSIS_FAILURES[0],
                search_override=None,
                username="user1",
                cooldown_verdict=False,
                attach_import_job=False,
            )

    def test_never_parked_checker_trips_on_parked_recovery_required_job(self):
        """The removed policy IS the planted bug now.

        Drives the real ambiguous dispatch and then closes it the way the
        pre-#933 importer did: the owner job stops in ``recovery_required``
        with the request still ``processing`` behind it.
        """
        outcome = _run_dispatch(
            self._ambiguous_world(),
            beets=self._beets_world(),
            terminalize="park",
        )
        db = outcome["db"]
        parked = db.get_import_job(_GENERATED_JOB_ID)
        assert parked is not None
        self.assertEqual(parked.status, "recovery_required")
        self.assertEqual(db.request(42)["status"], "processing")
        parked_clause = (
            r"^automation_import job 1 rested in non-terminal status "
            r"'recovery_required';")
        with self.assertRaisesRegex(AssertionError, parked_clause):
            assert_request_never_parked(db)
        with self.assertRaisesRegex(AssertionError, parked_clause):
            assert_world_failure_self_heals(db, outcome["result"])
        with self.assertRaisesRegex(AssertionError, parked_clause):
            assert_dispatch_outcome_matches_routing(
                self._ambiguous_world(), db, outcome["result"])

    def test_never_parked_checker_trips_on_retained_processing_owner(self):
        """An unterminalized dispatch leaves the real handoff world behind."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id="mbid-generated"))
        job = handoff_automation_owner(
            db,
            42,
            state={
                "files": [],
                "filetype": "mp3",
                "enqueued_at": "2026-07-29T00:00:00+00:00",
                "current_path": "/processing/albums/request-42",
            },
            canonical_path="/processing/albums/request-42",
        )
        # Planted mutant: dispatch returned without terminalizing, so the real
        # handoff writer's ``processing`` + owner pointer are still in place.
        # The QUEUE clause is the one that fires here — ``queued`` is an
        # active status — so this world proves that clause, not the owner or
        # ``processing`` clauses below.
        self.assertEqual(job.id, _GENERATED_JOB_ID)
        self.assertEqual(db.request(42)["status"], "processing")
        with self.assertRaisesRegex(
            AssertionError,
            r"automation_import job 1 rested in non-terminal status 'queued'",
        ):
            assert_request_never_parked(db)

    def test_never_parked_checker_names_every_clause_it_trips_on(self):
        """Per-clause Q1: one planted world per raise site, own message.

        Three of these are fail-closed legislation rather than worlds a
        production mutant can still reach (#1094 Q2): nothing deletes an
        ``import_jobs`` row, the automation terminal command refuses any
        request edge outside ``wanted``/``imported``, and ``downloading ->
        unsearchable`` is not in ``transitions.VALID_TRANSITIONS`` (a
        planted force-reject transition to ``unsearchable`` raises
        ``RequestTransitionConflict`` instead of landing). They stay so a
        future writer that bypasses those boundaries fails loudly.
        """
        state = {
            "files": [],
            "filetype": "mp3",
            "enqueued_at": "2026-07-29T00:00:00+00:00",
            "current_path": "/processing/albums/request-42",
        }
        no_job = FakePipelineDB()
        no_job.seed_request(make_request_row(id=42, status="wanted"))
        cases = [
            (
                "no import job row",
                no_job,
                r"^dispatch finalized with no import job row to read$",
            ),
            (
                "owner pointer still attached",
                self._planted_terminal_world(owner_attached=True),
                (
                    r"terminal dispatch left active_automation_import_job_id=1 "
                    r"attached"
                ),
            ),
            (
                "request left processing behind an inactive job",
                self._planted_terminal_world(request_status="processing"),
                (
                    r"terminal dispatch left the request in 'processing' behind "
                    r"an inactive job"
                ),
            ),
            (
                "automation landed off the runnable set",
                self._planted_terminal_world(request_status="unsearchable"),
                r"automation terminal left status='unsearchable', want one of "
                + re.escape("['imported', 'wanted']"),
            ),
            (
                "automation kept its owned download state",
                self._planted_terminal_world(active_download_state=state),
                r"^automation terminal left owned download state attached$",
            ),
            (
                "caller-retained landed off the operator's status",
                self._planted_terminal_world(
                    automation=False, request_status="unsearchable"),
                (
                    r"caller-retained terminal left status='unsearchable', want "
                    r"the operator's 'downloading'"
                ),
            ),
        ]
        for clause, db, pattern in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, pattern,
            ):
                assert_request_never_parked(db)

    def test_operator_visible_checker_names_every_clause_it_trips_on(self):
        """Per-clause Q1 for the bundle-less and audit-count clauses.

        The "no readable message or error" clause is fail-closed
        legislation: ``non_automation_failure_terminal_outcome`` raises
        ``non-automation failure requires a diagnostic`` before an empty
        one can be persisted (#1094 Q2), so only a writer that bypasses
        that builder could produce the world it names.
        """
        failure = DispatchOutcome(success=False, message="generated failure")
        no_job = FakePipelineDB()
        no_job.seed_request(make_request_row(id=42, status="wanted"))

        with self.subTest(clause="no import job row"), self.assertRaisesRegex(
                AssertionError,
                r"^dispatch finalized with no import job row to read$",
            ):
            assert_outcome_is_operator_visible(no_job, failure)

        with self.subTest(clause="bundle-less force job is not failed"):
            db = self._planted_terminal_world(
                automation=False, job_status="completed")
            with self.assertRaisesRegex(
                AssertionError,
                r"bundle-less caller-retained outcome left job "
                r"status='completed', want 'failed'",
            ):
                assert_outcome_is_operator_visible(db, failure)

        with self.subTest(clause="bundle-less force failure says nothing"):
            db = self._planted_terminal_world(
                automation=False, job_message=None, job_error=None)
            with self.assertRaisesRegex(
                AssertionError,
                r"bundle-less caller-retained failure recorded no readable "
                r"message or error",
            ):
                assert_outcome_is_operator_visible(db, failure)

        with self.subTest(clause="two audit rows for one outcome"):
            db = self._planted_terminal_world()
            db.log_download(request_id=42, outcome="rejected")
            db.log_download(request_id=42, outcome="rejected")
            with self.assertRaisesRegex(
                AssertionError, r"one dispatch wrote 2 audit rows",
            ):
                assert_outcome_is_operator_visible(db, failure)

    def test_self_heal_checker_trips_when_ambiguity_is_recorded_as_acquired(
        self,
    ):
        """A world failure recorded as an acquisition never re-searches.

        Fail-closed legislation (#1094 Q2): planting the same world in
        production — ``_self_heal_automation_world_failure`` transitioning
        to ``imported`` — is refused by
        ``validate_automation_terminal_authority`` ("automation
        world-failure self-heal must fail the job, record a failed audit,
        and return the request to wanted") before it can commit.
        """
        from scripts.importer import _WORLD_FAILURE_AUDIT_PREFIX

        db = self._planted_terminal_world(request_status="imported")
        db.log_download(
            request_id=42,
            outcome="rejected",
            beets_detail=f"{_WORLD_FAILURE_AUDIT_PREFIX}: generated ambiguity",
        )
        with self.assertRaisesRegex(
            AssertionError,
            r"automation world failure left status='imported', want 'wanted'",
        ):
            assert_world_failure_self_heals(
                db, DispatchOutcome(success=False, message="ambiguous"))

    def test_audit_checker_trips_on_silent_and_invisible_world_failures(self):
        """A self-heal the operator cannot read is still a silent stop."""
        world = self._ambiguous_world()
        outcome = _run_dispatch(world, beets=self._beets_world())
        db = outcome["db"]
        result = outcome["result"]
        # Must-still-work guard: the real self-heal satisfies both checkers.
        assert_world_failure_self_heals(db, result)
        audit = db.download_logs[-1]
        original = (audit.outcome, audit.beets_detail, audit.error_message)

        with self.subTest(plant="no audit row at all"):
            db.download_logs.clear()
            with self.assertRaisesRegex(
                AssertionError, r"^expected >= 1 download_log row\(s\), got 0$",
            ):
                assert_outcome_is_operator_visible(db, result)
            with self.assertRaisesRegex(
                AssertionError, r"^expected >= 1 download_log row\(s\), got 0$",
            ):
                assert_world_failure_self_heals(db, result)
        db.download_logs.append(audit)

        with self.subTest(plant="outcome no Recents filter selects"):
            # 'curator_ban' is a real taxonomy value that neither the imported
            # nor the problems lens selects, so the row exists and renders
            # nowhere the operator looks.
            audit.outcome = "curator_ban"
            with self.assertRaisesRegex(AssertionError, "never renders"):
                assert_outcome_is_operator_visible(db, result)

        with self.subTest(plant="world-failure label stripped"):
            audit.outcome = original[0]
            audit.beets_detail = "beets said something"
            audit.error_message = "beets said something"
            with self.assertRaisesRegex(
                AssertionError, "world-failure label",
            ):
                assert_world_failure_self_heals(db, result)

        with self.subTest(plant="retry accounting dropped"):
            audit.beets_detail, audit.error_message = original[1], original[2]
            row = db.request(42)
            row["validation_attempts"] = 0
            row["next_retry_after"] = None
            with self.assertRaisesRegex(AssertionError, "retry accounting"):
                assert_world_failure_self_heals(db, result)

    def test_log_row_checker_trips_on_empty_db(self):
        db = FakePipelineDB()
        with self.assertRaisesRegex(
            AssertionError,
            r"^expected >= 1 download_log row\(s\), got 0$",
        ):
            assert_download_log_row_created(db)

    def test_evidence_link_checker_names_every_clause_it_trips_on(self):
        """Per-clause Q1 for the wrong-match evidence-link isolation checker.

        Issue #1077, D6: the cleanup lane ("evaluate and possibly delete")
        is an explicit allowlist — a delete-ineligible scenario may still be
        evidence-linked (any scenario the worklist-visibility predicate
        admits gets linked), but the destructive reducer itself must never
        be consulted. The deletion-triage clause is the persisted-row
        witness of the same fail-open the cleanup-call clause catches: the
        destructive reducer is injected as a recorder at that boundary, so
        only a writer that stamps triage onto a delete-ineligible audit row
        can produce it.
        """
        def planted(**overrides: object) -> DownloadLogRow:
            kwargs: dict[str, object] = {
                "request_id": 835,
                "outcome": "rejected",
                "candidate_evidence_id": 7,
                "validation_result": json.dumps({"scenario": "audio_corrupt"}),
            }
            kwargs.update(overrides)
            return DownloadLogRow(**kwargs)  # pyright: ignore[reportArgumentType]

        cases = [
            (
                "destructive reducer reached",
                1,
                planted(),
                r"^delete-ineligible scenario reached Wrong Matches cleanup$",
            ),
            (
                "candidate evidence dropped",
                0,
                planted(candidate_evidence_id=None),
                r"^terminal audit lost its candidate evidence link$",
            ),
            (
                "deletion triage attached",
                0,
                planted(validation_result=json.dumps({
                    "scenario": "audio_corrupt",
                    "wrong_match_triage": {"decision": "delete"},
                })),
                r"^delete-ineligible terminal audit gained deletion triage$",
            ),
        ]
        for clause, cleanup_calls, terminal_log, pattern in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, pattern,
            ):
                assert_wrong_match_evidence_link_isolated(
                    cleanup_call_count=cleanup_calls,
                    terminal_log=terminal_log,
                    expected_candidate_evidence_id=7,
                )

    def test_verified_lossless_lock_checker_names_every_clause(self):
        """Per-clause Q1 for the non-punitive proof lock."""
        from tests.fakes import DenylistEntry

        cases = [
            (
                "acquisition reopened",
                "wanted",
                None,
                False,
                (
                    r"^verified lossless lock left status='wanted', "
                    r"want 'imported'$"
                ),
            ),
            (
                "search policy narrowed",
                "imported",
                "lossless",
                False,
                r"^verified lossless lock narrowed search policy$",
            ),
            (
                "source blamed",
                "imported",
                None,
                True,
                r"^verified lossless lock denylisted the source$",
            ),
        ]
        for clause, status, override, denylisted, pattern in cases:
            with self.subTest(clause=clause):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=42,
                    status=status,
                    search_filetype_override=override,
                ))
                if denylisted:
                    db.denylist.append(
                        DenylistEntry(42, "user1", "planted mutant"))
                with self.assertRaisesRegex(AssertionError, pattern):
                    assert_verified_lossless_lock_preserves_imported(db)

    def test_log_row_checker_trips_on_blank_outcome(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        # Bypass log_download's outcome-taxonomy check to plant a row a
        # real writer could never produce — proves the checker itself
        # (not just the CHECK constraint mirror) catches an empty outcome.
        # Fail-closed legislation (#1094 Q2): a production writer that
        # emits outcome='' raises download_log_outcome_check instead, and
        # the request self-heals; the clause guards a writer that ever
        # reaches the row without going through log_download.
        db.download_logs.append(DownloadLogRow(request_id=42, outcome=None))
        with self.assertRaisesRegex(
            AssertionError, r"^download_log row has empty/None outcome:",
        ):
            assert_download_log_row_created(db)

    def test_routing_checker_names_every_clause_it_trips_on(self):
        """Per-clause Q1 for the routing oracle's eight mismatch clauses."""
        from tests.fakes import DenylistEntry

        denylisted = self._routing_db(status="imported", log_outcome="success")
        denylisted.denylist.append(DenylistEntry(42, "user1", "planted"))
        cases = [
            (
                "mark_done logged as a rejection",
                self._routing_db(status="imported", log_outcome="rejected"),
                self._routing_world("import"),
                DispatchOutcome(success=True, message="ok"),
                (
                    r"decision='import' mark_done=True but logged "
                    r"outcome='rejected', want 'success'"
                ),
            ),
            (
                "mark_done reported failure",
                self._routing_db(status="imported", log_outcome="success"),
                self._routing_world("import"),
                DispatchOutcome(success=False, message="ok"),
                r"decision='import' mark_done=True but result.success=False",
            ),
            (
                "mark_done never flipped the request",
                self._routing_db(status="downloading", log_outcome="success"),
                self._routing_world("import"),
                DispatchOutcome(success=True, message="ok"),
                (
                    r"decision='import' mark_done=True left status='downloading', "
                    r"want 'imported'"
                ),
            ),
            (
                "mark_done narrowed the search policy",
                self._routing_db(
                    status="imported",
                    log_outcome="success",
                    search_filetype_override="lossless",
                ),
                self._routing_world("import"),
                DispatchOutcome(success=True, message="ok"),
                (
                    r"decision='import' mark_done=True left override='lossless', "
                    r"want None"
                ),
            ),
            (
                "mark_done denylisted the source",
                denylisted,
                self._routing_world("import"),
                DispatchOutcome(success=True, message="ok"),
                r"decision='import' mark_done=True denylist=True, want False",
            ),
            (
                "rejection logged as a success",
                self._routing_db(status="wanted", log_outcome="success"),
                self._routing_world("downgrade"),
                DispatchOutcome(success=False, message="rejected"),
                (
                    r"decision='downgrade' record_rejection=True but logged "
                    r"outcome='success', want 'rejected'"
                ),
            ),
            (
                "rejection reported success",
                self._routing_db(status="wanted", log_outcome="rejected"),
                self._routing_world("downgrade"),
                DispatchOutcome(success=True, message="rejected"),
                r"decision='downgrade' reject reported success=True",
            ),
            (
                "rejection ignored the caller's requeue flag",
                self._routing_db(status="downloading", log_outcome="rejected"),
                self._routing_world("downgrade"),
                DispatchOutcome(success=False, message="rejected"),
                (
                    r"decision='downgrade' requeue_on_failure=True left "
                    r"status='downloading', want 'wanted'"
                ),
            ),
        ]
        for clause, db, world, outcome, pattern in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, pattern,
            ):
                assert_dispatch_outcome_matches_routing(world, db, outcome)

    def test_routing_checker_has_no_unroutable_decision_world(self):
        """The unroutable-decision clause is fail-closed legislation.

        ``dispatch_action``'s final ``else`` returns ``record_rejection``,
        so no decision string — known, generated, or unknown — can reach
        the checker's last ``raise``. It stays so a future
        ``DispatchAction`` that sets neither flag fails loudly instead of
        passing every routing assertion silently.
        """
        for decision in (*_KNOWN_DECISIONS, "a decision nobody wrote yet"):
            with self.subTest(decision=decision):
                action = dispatch_action(decision)
                self.assertTrue(action.mark_done or action.record_rejection)

    def test_routing_checker_trips_on_ambiguity_reporting_success(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        # Planted bug: an unacknowledged Beets child reported success, so the
        # request would be recorded as acquired on evidence nobody has.
        db.log_download(request_id=42, outcome="success")
        world = self._ambiguous_world()
        outcome = DispatchOutcome(success=True, message="")
        with self.assertRaisesRegex(
            AssertionError,
            r"^ambiguous acknowledgement reported success=True$",
        ):
            assert_dispatch_outcome_matches_routing(world, db, outcome)

    def test_preimport_caller_flag_checker_trips_when_flag_ignored(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        with self.assertRaisesRegex(
            AssertionError,
            r"^preimport-fact reject 'audio_corrupt' left status='wanted', "
            r"want 'downloading' for requeue_on_failure=False$",
        ):
            assert_preimport_fact_honors_caller_flag(
                "audio_corrupt", False, db)

    def test_quality_side_checker_trips_when_flag_ignored(self):
        db = FakePipelineDB()
        # Planted bug: status is 'wanted' even though requeue_on_failure
        # was False — the caller's flag was ignored.
        db.seed_request(make_request_row(id=42, status="wanted"))
        with self.assertRaisesRegex(
            AssertionError,
            r"^quality-side reject 'downgrade' requeue_on_failure=False left "
            r"status='wanted', want 'downloading'$",
        ):
            assert_quality_side_reject_honors_caller_flag(
                "downgrade", False, db)

    def _have_analysis_db(self, **row_overrides: object) -> FakePipelineDB:
        """A HAVE-analysis abort world that satisfies every clause."""
        row: dict[str, object] = {
            "id": 42,
            "status": "wanted",
            "validation_attempts": 1,
            "next_retry_after": "planted-backoff",
            "search_filetype_override": None,
        }
        row.update(row_overrides)
        db = FakePipelineDB()
        db.seed_request(make_request_row(**row))
        db.log_download(request_id=42, outcome="have_analysis_error")
        return db

    def test_have_analysis_checker_names_every_clause_it_trips_on(self):
        """Per-clause Q1 for the non-quality abort checker."""
        from tests.fakes import DenylistEntry

        denylisted = self._have_analysis_db()
        denylisted.denylist.append(
            DenylistEntry(42, "bad-user", "planted mutant"))
        wrong_outcome = self._have_analysis_db()
        wrong_outcome.log_download(request_id=42, outcome="rejected")
        cases = [
            (
                "caller lifecycle moved",
                self._have_analysis_db(status="unsearchable"),
                (
                    r"^HAVE-analysis abort left status='unsearchable', "
                    r"want 'wanted' for auto$"
                ),
            ),
            (
                "search policy narrowed",
                self._have_analysis_db(search_filetype_override="lossless"),
                (
                    r"HAVE-analysis abort changed search_filetype_override from "
                    r"None to 'lossless'"
                ),
            ),
            (
                "source blamed",
                denylisted,
                r"^HAVE-analysis abort wrote quality denylist entries:",
            ),
            (
                "audit taxonomy lost",
                wrong_outcome,
                (
                    r"^HAVE-analysis abort did not persist "
                    r"outcome='have_analysis_error'$"
                ),
            ),
            (
                "retry bookkeeping dropped",
                self._have_analysis_db(
                    validation_attempts=0, next_retry_after=None),
                r"^HAVE-analysis abort applied the wrong retry bookkeeping$",
            ),
        ]
        for clause, db, pattern in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, pattern,
            ):
                assert_have_analysis_abort_is_non_quality(
                    db,
                    mode="auto",
                    expected_search_override=None,
                )

    def test_have_analysis_cooldown_checker_trips_on_double_evaluation(self):
        db = FakePipelineDB()
        db.cooldowns_applied.extend(("peer", "peer"))
        with self.assertRaisesRegex(
            AssertionError,
            r"^HAVE-analysis cooldown evaluations drifted: ",
        ):
            assert_have_analysis_abort_cooldown_policy(
                db,
                username="peer",
                cooldown_verdict=False,
            )

    def test_have_analysis_cooldown_checker_trips_on_missing_write(self):
        db = FakePipelineDB()
        db.cooldowns_applied.append("peer")
        with self.assertRaisesRegex(
            AssertionError,
            r"^HAVE-analysis cooldown persistence drifted: ",
        ):
            assert_have_analysis_abort_cooldown_policy(
                db,
                username="peer",
                cooldown_verdict=True,
            )

    def test_have_analysis_cooldown_checker_trips_without_username(self):
        db = FakePipelineDB()
        db.add_cooldown(
            "ghost",
            datetime.now(UTC) + timedelta(days=1),
            "planted mutant",
        )
        with self.assertRaisesRegex(
            AssertionError,
            r"^HAVE-analysis cooldown persistence drifted: ",
        ):
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
            datetime.now(UTC) + timedelta(days=1),
            "planted mutant",
        )
        with self.assertRaisesRegex(
            AssertionError,
            r"^HAVE-analysis cooldown persistence drifted: ",
        ):
            assert_have_analysis_abort_cooldown_policy(
                db,
                username="peer",
                cooldown_verdict=False,
            )

    def test_operator_retained_checker_names_every_clause(self):
        """Per-clause Q1 for the retained force-import lifecycle checker."""
        cleared = FakePipelineDB()
        cleared.seed_request(make_request_row(
            id=42, status="wanted", search_filetype_override="lossless"))
        unrecorded = FakePipelineDB()
        unrecorded.seed_request(make_request_row(
            id=42, status="unsearchable", search_filetype_override=None))
        cases = [
            (
                "operator search stop cleared",
                cleared,
                (
                    r"^retained force import changed lifecycle from "
                    r"'unsearchable' to 'wanted'$"
                ),
            ),
            (
                "canonical search policy not recorded",
                unrecorded,
                (
                    r"^retained force import failed to record canonical search "
                    r"policy$"
                ),
            ),
        ]
        for clause, db, pattern in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, pattern,
            ):
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
        with self.assertRaisesRegex(
            AssertionError,
            r"^expected persisted beets_distance=None, got 0\.0",
        ):
            assert_beets_distance_round_trips(db, None)

    def test_distance_checker_trips_when_measured_value_gets_nulled(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        # Planted bug: a genuinely measured distance (0.07) was dropped
        # to NULL instead of being persisted as-is.
        db.log_download(request_id=42, outcome="rejected", beets_distance=None)
        with self.assertRaisesRegex(
            AssertionError,
            r"^expected persisted beets_distance=0\.07, got None",
        ):
            assert_beets_distance_round_trips(db, 0.07)

    def test_validation_projection_checker_names_every_clause(self):
        """Per-clause Q1 for the envelope/query-column projection checker."""
        from lib.quality import ValidationResult

        envelope = ValidationResult(
            distance=0.07, scenario="high_distance").to_json()
        cases = [
            (
                "no object envelope persisted",
                DownloadLogRow(
                    request_id=42,
                    outcome="rejected",
                    validation_result=None,
                ),
                r"^rejection writer did not persist an object envelope$",
            ),
            (
                "distance drifted from its envelope",
                DownloadLogRow(
                    request_id=42,
                    outcome="rejected",
                    beets_distance=0.99,
                    beets_scenario="high_distance",
                    validation_result=envelope,
                ),
                (
                    r"^validation distance=0\.07 drifted from "
                    r"beets_distance=0\.99$"
                ),
            ),
            (
                "scenario drifted from its envelope",
                DownloadLogRow(
                    request_id=42,
                    outcome="rejected",
                    beets_distance=0.07,
                    beets_scenario="wrong_scenario",
                    validation_result=envelope,
                ),
                (
                    r"^validation scenario='high_distance' drifted from "
                    r"beets_scenario='wrong_scenario'$"
                ),
            ),
        ]
        for clause, row, pattern in cases:
            with self.subTest(clause=clause):
                db = FakePipelineDB()
                db.download_logs.append(row)
                with self.assertRaisesRegex(AssertionError, pattern):
                    assert_validation_projection_matches_payload(db)

    def test_denylist_bundle_checker_names_the_stray_write(self):
        """Q1 for ``assert_denylist_writes_are_bundled`` (issue #1355 item
        3): a ``source_denylist`` row with no matching terminal-outcome
        command entry — the exact shape a reverted-to-a-separate-loop
        regression would produce — trips the checker's one clause."""
        from tests.fakes import DenylistEntry

        db = FakePipelineDB()
        db.denylist.append(DenylistEntry(42, "stray-peer", "no bundle"))

        with self.assertRaisesRegex(
            AssertionError,
            r"^source_denylist rows drifted from the terminal-outcome "
            r"bundle: bundled=\[\] written=\['stray-peer'\]$",
        ):
            assert_denylist_writes_are_bundled(db, 42)

    def test_writer_denylisted_username_checker_names_the_missing_write(
        self,
    ):
        """Q1 for ``assert_writer_denylisted_username`` (issue #1355 item
        3, mutant-runner findings M15/M27): an empty ``source_denylist``
        table — the exact shape a dropped ``denylists=denylists`` argument
        produces, which ``assert_denylist_writes_are_bundled`` alone cannot
        catch since both its sides read empty — trips this checker."""
        db = FakePipelineDB()

        with self.assertRaisesRegex(
            AssertionError,
            r"^expected 'generated-user' to be denylisted for request 42; "
            r"source_denylist has \[\]$",
        ):
            assert_writer_denylisted_username(db, 42, "generated-user")

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
