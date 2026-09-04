"""Shared dispatch/import-lane test support.

Split out of ``tests/helpers.py`` (issue #1278, "worth exploring" item 5):
the bridges from ``FakePipelineDB`` fixtures into typed production
import/dispatch surfaces (``finalize_claimed_dispatch``,
``make_database_source_with_fake_db``, ``pinned_dispatch_authority``), the
import-queue claim and automation-handoff lifecycle helpers, the
``DispatchRequest`` builder, and the dispatch-test seam stubs
(``noop_quality_gate``, ``RecordingQualityGate``,
``patch_dispatch_externals``).
"""

from __future__ import annotations

import types
from collections.abc import Generator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING, Any, Protocol
from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    from album_source import DatabaseSource
    from lib.dispatch.types import (
        DispatchOutcome,
        DispatchRequest,
        ImportAttemptResult,
    )
    from lib.import_evidence import CandidateEvidenceActionResult
    from lib.pipeline_db import DownloadLogOutcome, PipelineDB
    from lib.quality import SpectralDetail

import psycopg2.extras

from lib.grab_list import DownloadFile
from lib.import_execution import (
    CancellationToken,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    AutomationHandoffResult,
    ImportJob,
)
from lib.pipeline_db._shared import ADVISORY_LOCK_NAMESPACE_IMPORT
from lib.quality import ActiveDownloadState, DownloadInfo


@contextmanager
def pinned_dispatch_authority(
    db: _PinnedDispatchDB,
    execution_lease: ExecutionLeaseSnapshot | None,
    *,
    cancellation_token: CancellationToken | None = None,
) -> Generator[
    tuple[CancellationToken | None, OwnerSessionIdentity | None],
]:
    """Pin the real fake-DB owner session for one automation dispatch scope."""
    if execution_lease is None:
        if cancellation_token is not None:
            raise AssertionError(
                "non-automation dispatch cannot carry a cancellation token"
            )
        yield None, None
        return

    existing_pin = getattr(db, "_owner_session_pin", None)
    if existing_pin is not None:
        identity, pinned_token = existing_pin
        if (
            cancellation_token is not None
            and cancellation_token is not pinned_token
        ):
            raise AssertionError(
                "nested dispatch authority must reuse the pinned token"
            )
        yield pinned_token, identity
        return

    token = cancellation_token or CancellationToken()
    with db._pin_owner_session(token) as identity:
        yield token, identity


def make_database_source_with_fake_db(
    db: Any,
    *,
    musicbrainz_ws2_base: str,
    discogs_api_base: str,
) -> DatabaseSource:
    """``Any``-accepting bridge from a ``FakePipelineDB`` fixture into
    ``DatabaseSource``'s ``PipelineDB``-typed ``borrowed_db`` kwarg — same
    established pattern as ``finalize_claimed_dispatch`` below: one
    bridge, zero per-call-site escape hatches. Added for issue #1261's
    fallback-writer pins, which need a stateful DB to assert the persisted
    manifest rather than a call shape.
    """
    from album_source import DatabaseSource

    return DatabaseSource(
        "unused-dsn",
        musicbrainz_ws2_base=musicbrainz_ws2_base,
        discogs_api_base=discogs_api_base,
        borrowed_db=db,
    )


def make_dispatch_request(
    *,
    path: str = "/tmp/dispatch-album",
    mb_release_id: str = "mb-release-1",
    request_id: int = 1,
    label: str = "Test Artist - Test Album",
    beets_harness_path: str = "/opt/cratedigger/harness/beets_harness.py",
    dl_info: DownloadInfo | None = None,
    force: bool = False,
    scenario: str = "auto_import",
    outcome_label: DownloadLogOutcome = "success",
    requeue_on_failure: bool = True,
    distance: float | None = None,
    override_min_bitrate: int | None = None,
    target_format: str | None = None,
    verified_lossless_target: str = "",
    files: Sequence[DownloadFile] | None = None,
    cooled_down_users: set[str] | None = None,
    source_dirs: list[str] | None = None,
    candidate_import_job_id: int | None = None,
    candidate_download_log_id: int | None = None,
    attempt_spectral_audit: SpectralDetail | None = None,
    attempt_result: ImportAttemptResult | None = None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
    launch_authority_path: str | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
) -> DispatchRequest:
    """Build a complete ``DispatchRequest`` with sensible defaults (#1277).

    House idiom, same as ``make_request_row`` / ``make_download_file``: a
    test names only the fields its scenario needs. Every optional parameter
    repeats ``DispatchRequest``'s own default, and
    ``TestMakeDispatchRequestBuilder`` pins that equality field-by-field so
    the two cannot drift.

    This replaced ``dispatch_import_with_fake_db``, the ``Any``-accepting
    bridge that used to erase type-checking for all 36 dispatch kwargs at
    every call site (issue #1246 / ``.claude/rules/code-quality.md``
    § "Typing enforcement"). ``dispatch_import_core`` now takes a narrow
    ``DispatchDB`` port that ``FakePipelineDB`` satisfies structurally, so
    tests call it directly with no bridge at all.
    """
    from lib.dispatch.types import DispatchRequest

    return DispatchRequest(
        path=path,
        mb_release_id=mb_release_id,
        request_id=request_id,
        label=label,
        beets_harness_path=beets_harness_path,
        dl_info=dl_info if dl_info is not None else DownloadInfo(),
        force=force,
        scenario=scenario,
        outcome_label=outcome_label,
        requeue_on_failure=requeue_on_failure,
        distance=distance,
        override_min_bitrate=override_min_bitrate,
        target_format=target_format,
        verified_lossless_target=verified_lossless_target,
        files=files,
        cooled_down_users=cooled_down_users,
        source_dirs=source_dirs,
        candidate_import_job_id=candidate_import_job_id,
        candidate_download_log_id=candidate_download_log_id,
        attempt_spectral_audit=attempt_spectral_audit,
        attempt_result=attempt_result,
        prevalidated_candidate_result=prevalidated_candidate_result,
        beets_library_db_path=beets_library_db_path,
        beets_library_root=beets_library_root,
        launch_authority_path=launch_authority_path,
        execution_lease=execution_lease,
        owner_session_identity=owner_session_identity,
    )


def finalize_claimed_dispatch(
    db: Any,
    job: ImportJob,
    outcome: DispatchOutcome | BaseException,
) -> ImportJob | None:
    """Apply a direct dispatch result through the production queue owner.

    ``outcome`` is ordinarily the ``DispatchOutcome`` the caller already
    computed. Passing a ``BaseException`` INSTANCE instead drives
    ``process_claimed_job``'s own executor-crash handling without
    hand-rolling a raising ``execute_fn`` at the call site —
    ``tests/test_integration_slices.py``'s launched-but-unacknowledged-
    crash slice is the caller that does. ``db`` is the established,
    ``Any``-typed bridge from a ``FakePipelineDB`` fixture into the
    ``PipelineDB``-typed ``process_claimed_job``, so a crash-path caller
    reuses it instead of calling ``process_claimed_job`` directly (issue
    #1176 PR3 review round: keeps the tests typing ratchet frozen — no new
    escape hatch). ``job`` and ``outcome`` carry real types, so only the
    fake-vs-concrete DB seam needs the hatch (#1278 helpers split).
    """
    from lib.import_queue import IMPORT_JOB_AUTOMATION
    from lib.import_worker_loop import execution_lease_from_job
    from scripts.importer import process_claimed_job

    def _execute(*_args: object, **_kwargs: object) -> DispatchOutcome:
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    if job.job_type == IMPORT_JOB_AUTOMATION:
        execution_lease = execution_lease_from_job(job)
        assert execution_lease is not None, (
            "automation fixture must claim with an importer execution lease"
        )
        with pinned_dispatch_authority(
            db,
            execution_lease,
        ) as (cancellation_token, owner_session_identity):
            assert cancellation_token is not None
            assert owner_session_identity is not None
            return process_claimed_job(
                db,
                job,
                execute_fn=_execute,
                execution_lease=execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )
    return process_claimed_job(
        db,
        job,
        execute_fn=_execute,
    )


class _ImportJobClaimDB(Protocol):
    def peek_import_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]: ...

    def peek_import_preview_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]: ...

    def advisory_lock(
        self,
        namespace: int,
        key: int,
    ) -> AbstractContextManager[bool]: ...

    def claim_import_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None: ...

    def claim_import_preview_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None: ...

    def claim_automation_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None: ...

    def claim_automation_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None: ...

    def claim_force_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def claim_force_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def claim_local_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def claim_local_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...


def claim_next_import_job(
    db: _ImportJobClaimDB,
    *,
    worker_id: str | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
) -> ImportJob | None:
    """Claim the first import candidate for direct test setup.

    Production workers scan bounded candidate pages and claim exact rows. Tests
    that need a claimed fixture retain the old one-shot convenience here
    without preserving a production API that no runtime caller uses.
    """
    candidates = db.peek_import_job_candidates(
        execution_lease=execution_lease,
        limit=1,
    )
    if not candidates:
        return None
    candidate = candidates[0]
    if candidate.job_type == IMPORT_JOB_AUTOMATION:
        if execution_lease is None or candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_automation_import_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
                execution_lease=execution_lease,
            )
    if candidate.job_type == IMPORT_JOB_FORCE:
        if candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_force_import_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
            )
    if candidate.job_type == IMPORT_JOB_LOCAL:
        if candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_local_import_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
            )
    return db.claim_import_job_candidate(
        candidate.id,
        worker_id=worker_id,
    )


def claim_next_import_preview_job(
    db: _ImportJobClaimDB,
    *,
    worker_id: str | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
) -> ImportJob | None:
    """Claim the first preview candidate for direct test setup."""
    candidates = db.peek_import_preview_job_candidates(
        execution_lease=execution_lease,
        limit=1,
    )
    if not candidates:
        return None
    candidate = candidates[0]
    if candidate.job_type == IMPORT_JOB_AUTOMATION:
        if execution_lease is None or candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_automation_import_preview_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
                execution_lease=execution_lease,
            )
    if candidate.job_type == IMPORT_JOB_FORCE:
        if candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_force_import_preview_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
            )
    if candidate.job_type == IMPORT_JOB_LOCAL:
        if candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_local_import_preview_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
            )
    return db.claim_import_preview_job_candidate(
        candidate.id,
        worker_id=worker_id,
    )


def handoff_automation_owner(
    db: _AutomationHandoffDB,
    request_id: int,
    *,
    state: ActiveDownloadState | Mapping[str, object] | str | None = None,
    canonical_path: str | None = None,
    message: str = "test automation owner handoff",
) -> ImportJob:
    """Create a production-representable automation owner for tests.

    Tests must never bypass the sole lifecycle edge by inserting an
    ``automation_import`` job or assigning the owner pointer directly. This
    helper performs the real ``wanted -> downloading -> processing`` transcript
    through ``set_downloading`` and ``handoff_automation_import``.
    """
    active_state = (
        ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-07-01T00:00:00+00:00",
            files=[],
        )
        if state is None
        else ActiveDownloadState.from_raw(state)
    )
    path = (
        canonical_path
        or active_state.current_path
        or f"/processing/albums/request-{request_id}"
    )
    if not db.set_downloading(
        request_id,
        active_state.to_json(),
        expected_status="wanted",
    ):
        raise AssertionError(
            f"request {request_id} could not enter downloading for handoff"
        )
    result = db.handoff_automation_import(
        request_id=request_id,
        expected_enqueued_at=active_state.enqueued_at,
        canonical_path=path,
        message=message,
    )
    if not result.committed or result.job is None:
        raise AssertionError(
            f"request {request_id} handoff failed: {result.outcome}"
        )
    return result.job


def fail_import_job_via_sql(
    db: PipelineDB,
    job_id: int,
    *,
    error: str,
    result: dict[str, object] | None = None,
    message: str | None = None,
) -> None:
    """Terminalize one non-automation job as ``failed``, for real-DB
    fixture setup only.

    ``PipelineDB.mark_import_job_failed`` had zero production callers and
    was deleted (issue #1355 item A3); this reproduces its exact SQL shape
    as fixture machinery ONLY, for tests that need a pre-existing failed
    job row and are not themselves exercising that writer's own behavior
    (which no longer exists to exercise).
    """
    db._execute(
        """
        UPDATE import_jobs
        SET status = 'failed',
            result = %s,
            message = %s,
            error = %s,
            completed_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
          AND job_type <> 'automation_import'
          AND status IN ('queued', 'running')
        """,
        (psycopg2.extras.Json(result or {}), message, error, job_id),
    )
    db.conn.commit()


def noop_quality_gate(**_kwargs: object) -> None:
    """No-op quality-gate stub for ``dispatch_import_core(quality_gate_fn=...)``.

    Replaces the legacy module-attribute patch on
    ``_check_quality_gate_core`` for dispatch tests that don't care
    about the post-import quality gate's side effects — they want a
    no-op so the dispatch decision tree runs end-to-end without
    inspecting beets DB state."""
    return


class RecordingQualityGate:
    """Recorder ``quality_gate_fn`` stub. Replaces the legacy
    module-attribute patch on ``_check_quality_gate_core`` (paired with
    ``as mock_gate``) for tests that assert
    ``mock_gate.assert_called_once()``.

    Records each invocation's kwargs (the gate is keyword-only) so tests
    can assert call counts and arguments."""

    def __init__(self, *, result: object | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self.result = result

    def __call__(self, **kwargs: object) -> object | None:
        self.calls.append(kwargs)
        return self.result

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def assert_called_once(self) -> None:
        if len(self.calls) != 1:
            raise AssertionError(
                f"expected quality_gate_fn called exactly once, got {len(self.calls)}"
            )

    def assert_not_called(self) -> None:
        if self.calls:
            raise AssertionError(
                f"expected quality_gate_fn not called, got {len(self.calls)} call(s)"
            )


@contextmanager
def patch_dispatch_externals():
    """Patch external edges shared by all dispatch_import_core tests.

    Patches: sp.run, the evidence-rejection cleanup seam, trigger_plex_scan,
    and trigger_jellyfin_scan.

    Does NOT patch parse_import_result, _check_quality_gate_core,
    BeetsDB, read_runtime_config, or the vanished-replaced-album-path
    reconciler (issue #1203 item 2) — callers nest those as needed. The
    reconciler is a kwarg-DI seam on ``dispatch_import_core`` itself
    (``media_server_notify_fn``), not a module patch: it now contains real
    escalation-decision logic (``lib.library_delete_notifiers
    .notify_library_delete``), so it no longer qualifies as a thin leaf-seam
    wrapper for the mock-audit allowlist. Since ``sp.run`` below is always
    mocked, no test using this helper ever mutates the real Beets DB, so the
    reconciler's own before/after snapshot diff is empty by construction
    unless a test deliberately mutates Beets out of band (as
    ``tests.test_import_dispatch.TestVanishedPathReconciliation`` does) —
    ordinary dispatch tests never reach the reconciler at all and need no
    stand-in for it.

    Yields a SimpleNamespace with attributes: run, cleanup, plex, jellyfin.
    run is pre-configured with returncode=0, stdout="", stderr="".

    Importer post-commit cleanup is exercised through real inputs or its
    dedicated queue-owner seam; this helper does not patch that owned code.
    """
    cleanup = MagicMock()
    with patch("lib.dispatch.subprocess_runner.sp.run") as run, \
         patch("lib.dispatch.outcome_actions._cleanup_staged_dir", cleanup), \
         patch("lib.util.trigger_plex_scan") as plex, \
         patch("lib.util.trigger_jellyfin_scan") as jellyfin:
        run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        yield types.SimpleNamespace(
            run=run, cleanup=cleanup, plex=plex, jellyfin=jellyfin)


class _PinnedDispatchDB(Protocol):
    _owner_session_pin: tuple[OwnerSessionIdentity, CancellationToken] | None

    def _pin_owner_session(
        self,
        token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...


class _AutomationHandoffDB(Protocol):
    def set_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "wanted",
    ) -> bool: ...

    def handoff_automation_import(
        self,
        *,
        request_id: int,
        expected_enqueued_at: str,
        canonical_path: str,
        message: str,
    ) -> AutomationHandoffResult: ...
