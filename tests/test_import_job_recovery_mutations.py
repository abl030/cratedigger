"""Owner-atomic automation recovery retry and close contracts (#898 U4)."""

from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import unittest
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, nullcontext
from datetime import datetime
from functools import partial
from typing import Literal, Protocol
from unittest.mock import patch

import msgspec

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from lib.import_execution import (
    CgroupObservation,
    ExecutionLeaseSnapshot,
    ExecutionLivenessEvidence,
    ExecutionLivenessProbe,
    InvocationObservation,
    OwnerSessionIdentity,
    OwnerSessionProbe,
    ProcessIdentity,
    ProcessObservation,
)
from lib.import_job_recovery_service import (
    AutomationRecoveryMutationDB,
    apply_import_job_recovery,
    get_automation_recovery_detail,
)
from lib.pipeline_db._shared import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    release_id_to_lock_key,
)
from lib.pipeline_db.cleanup_journal import CleanupJournalIntent
from lib.pipeline_db.terminal_outcomes import ImportJobTerminalConflict
from lib.processing_cleanup import (
    cleanup_manifest_hash,
)
from tests.helpers import handoff_automation_owner

TEST_DSN = os.environ["TEST_DB_DSN"]
_RELEASE_ID = "75dbf62e-7dd2-4ddc-b57b-9bad1758b6b0"


class _RawRecoveryDB(AutomationRecoveryMutationDB, Protocol):
    def _execute(
        self,
        sql: str,
        params: tuple[object, ...] = (),
    ) -> object: ...


class _RecoveryTranscriptDB(AutomationRecoveryMutationDB, Protocol):
    def claim_next_import_preview_job(
        self,
        *,
        worker_id: str,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> object | None: ...

    def claim_next_import_job(
        self,
        *,
        worker_id: str,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> object | None: ...


def _replacement_lease(label: str) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="replacement-boot",
        invocation_id=f"replacement-{label}",
        systemd_unit="cratedigger-importer.service",
        worker=ProcessIdentity(pid=909, start_ticks=9009),
    )


class _ChangedBootProbe:
    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        return ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="boot-after-restart",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )


class _MutatingChangedBootProbe(_ChangedBootProbe):
    def __init__(self, db: _RawRecoveryDB, job_id: int) -> None:
        self.db = db
        self.job_id = job_id

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        self.db._execute(
            """
            UPDATE import_jobs
            SET execution_invocation_id = 'fresh-claim'
            WHERE id = %s
            """,
            (self.job_id,),
        )
        return super().observe(lease)


class _MutatingOnSecondChangedBootProbe(_ChangedBootProbe):
    """Introduce a race after the action has captured its DB observation."""

    def __init__(self, mutate: Callable[[], None]) -> None:
        self._mutate = mutate
        self.observe_calls = 0
        self.mutation_calls = 0

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        self.observe_calls += 1
        if self.observe_calls == 2:
            self._mutate()
            self.mutation_calls += 1
        return super().observe(lease)


def _set_fake_execution_invocation(
    rows: list[dict[str, object]],
    job_id: int,
    invocation_id: str,
) -> None:
    row = next(item for item in rows if item["id"] == job_id)
    row["execution_invocation_id"] = invocation_id


def _set_real_execution_invocation(
    db: _RawRecoveryDB,
    job_id: int,
    invocation_id: str,
) -> None:
    db._execute(
        """
        UPDATE import_jobs
        SET execution_invocation_id = %s
        WHERE id = %s
        """,
        (invocation_id, job_id),
    )


class _UnknownProbe:
    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        del lease
        raise OSError("liveness probe unavailable")


class _LiveProbe:
    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        cgroup = "/system.slice/cratedigger-importer.service"
        return ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id=lease.host_boot_id,
            boot_error=None,
            worker=ProcessObservation(
                identity=lease.worker,
                state="exact",
                observed_start_ticks=lease.worker.start_ticks,
                cgroup_path=cgroup,
                reason="exact_process_identity",
            ),
            beets=None,
            invocation=InvocationObservation(
                state="exact",
                stored_invocation_id=lease.invocation_id,
                observed_invocation_id=lease.invocation_id,
                control_group=cgroup,
                reason="exact_invocation",
                active_state="active",
                sub_state="running",
            ),
            cgroup=CgroupObservation(
                state="exact",
                path=cgroup,
                member_pids=(lease.worker.pid,),
                reason="exact_cgroup",
            ),
        )


def _plain(value: object) -> object:
    """Convert a complete DB state into deterministic comparison builtins."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Mapping):
        return {
            str(key): _plain(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    converted: object = msgspec.to_builtins(value)
    if converted is value:
        raise TypeError(f"cannot normalize state value {type(value)!r}")
    return _plain(converted)


def _filesystem_observation(path: str) -> dict[str, object]:
    """Observe path identity, tree membership, and exact file bytes."""
    if not os.path.lexists(path):
        return {"kind": "missing"}
    if os.path.islink(path):
        return {
            "kind": "symlink",
            "target": os.readlink(path),
        }
    if not os.path.isdir(path):
        with open(path, "rb") as handle:
            return {
                "kind": "file",
                "sha256": hashlib.sha256(handle.read()).hexdigest(),
            }
    entries: list[dict[str, object]] = []
    for root, dirs, files in os.walk(path, followlinks=False):
        dirs.sort()
        files.sort()
        relative_root = os.path.relpath(root, path)
        for directory in dirs:
            entries.append({
                "kind": "directory",
                "path": os.path.normpath(os.path.join(
                    relative_root,
                    directory,
                )),
            })
        for filename in files:
            absolute = os.path.join(root, filename)
            relative = os.path.normpath(os.path.join(
                relative_root,
                filename,
            ))
            if os.path.islink(absolute):
                entries.append({
                    "kind": "symlink",
                    "path": relative,
                    "target": os.readlink(absolute),
                })
                continue
            with open(absolute, "rb") as handle:
                entries.append({
                    "kind": "file",
                    "path": relative,
                    "sha256": hashlib.sha256(handle.read()).hexdigest(),
                })
    return {"kind": "directory", "entries": entries}


def _complete_recovery_state(
    db: _RecoveryTranscriptDB,
    *,
    request_id: int,
    job_id: int,
    canonical_path: str,
) -> dict[str, object]:
    request = db.get_request(request_id)
    job = db.get_import_job(job_id)
    assert request is not None and job is not None
    journal = db.get_processing_cleanup_journal(
        request_id=request_id,
        job_id=job_id,
    )
    return {
        "request": _plain(request),
        "job": _plain(job.to_dict()),
        "journal": _plain(journal),
        "filesystem": _filesystem_observation(canonical_path),
    }


def _normalize_recovery_state(
    state: dict[str, object],
) -> dict[str, object]:
    """Erase backend-assigned identities/times while retaining every field."""
    normalized = msgspec.convert(
        state,
        type=dict[str, object],
    )
    request = normalized["request"]
    job = normalized["job"]
    journal = normalized["journal"]
    assert isinstance(request, dict)
    assert isinstance(job, dict)
    request["id"] = "<request_id>"
    job["id"] = "<job_id>"

    def normalize(value: object, *, key: str = "") -> object:
        if value is None:
            return None
        if key.endswith("_at"):
            return "<timestamp>"
        if key == "request_id":
            return "<request_id>"
        if key in {"job_id", "active_automation_import_job_id"}:
            return "<job_id>"
        if key == "dedupe_key":
            return "<dedupe_key>"
        if isinstance(value, dict):
            return {
                str(child_key): normalize(
                    child_value,
                    key=str(child_key),
                )
                for child_key, child_value in value.items()
            }
        if isinstance(value, list):
            return [normalize(item) for item in value]
        return value

    result = {
        "request": normalize(request),
        "job": normalize(job),
        "journal": normalize(journal),
        "filesystem": normalized["filesystem"],
    }
    return result


def _normalized_rejection_transcript(
    db: _RecoveryTranscriptDB,
    *,
    request_id: int,
    job_id: int,
    canonical_path: str,
    expected_outcome: str,
    probe: ExecutionLivenessProbe,
    stale_revision: bool = False,
    action: Literal["retry", "close"] = "retry",
    result_status: Literal["wanted", "imported"] | None = None,
    lock_scope: AbstractContextManager[object] | None = None,
    after_apply: Callable[[], None] | None = None,
) -> dict[str, object]:
    observed = get_automation_recovery_detail(
        db,
        None,
        job_id,
        liveness_probe=probe,
    )
    assert observed.detail is not None
    revision = (
        "sha256:stale"
        if stale_revision
        else observed.detail.evidence_revision
    )
    before = _complete_recovery_state(
        db,
        request_id=request_id,
        job_id=job_id,
        canonical_path=canonical_path,
    )
    before_job = before["job"]
    assert isinstance(before_job, dict)
    try:
        with lock_scope or nullcontext() as lock_held:
            if lock_scope is not None:
                assert lock_held is True
            result = apply_import_job_recovery(
                db,
                None,
                job_id,
                action=action,
                reason=f"backend-neutral rejection {expected_outcome}",
                evidence_revision=revision,
                result_status=result_status,
                liveness_probe=probe,
            )
    finally:
        if after_apply is not None:
            after_apply()
    after = _complete_recovery_state(
        db,
        request_id=request_id,
        job_id=job_id,
        canonical_path=canonical_path,
    )
    after_job = after["job"]
    assert isinstance(after_job, dict)
    protected_job_fields = (
        "result",
        "error",
        "message",
        "updated_at",
        "started_at",
        "heartbeat_at",
        "completed_at",
        "execution_invocation_id",
        "execution_host_boot_id",
        "execution_systemd_unit",
        "execution_worker_pid",
        "execution_worker_start_ticks",
        "execution_beets_pid",
        "execution_beets_start_ticks",
    )
    return {
        "outcome": result.outcome,
        "expected_outcome": expected_outcome,
        "action_job_absent": result.job is None,
        "retry_job_absent": result.retry_job is None,
        "state_unchanged": before == after,
        "protected_job_fields_unchanged": all(
            before_job[field] == after_job[field]
            for field in protected_job_fields
        ),
        "before": _normalize_recovery_state(before),
        "after": _normalize_recovery_state(after),
    }


def _normalized_retry_transcript(
    db: _RecoveryTranscriptDB,
    *,
    request_id: int,
    job_id: int,
    canonical_path: str,
) -> dict[str, object]:
    """Run the same public recovery conversation on fake and PostgreSQL."""
    journal = db.create_processing_cleanup_journal(
        request_id=request_id,
        job_id=job_id,
        intent=CleanupJournalIntent(
            action="no_op",
            source_path=canonical_path,
            source_manifest=(),
            source_manifest_hash=cleanup_manifest_hash(()),
        ),
    )
    probe = _ChangedBootProbe()
    observed = get_automation_recovery_detail(
        db,
        None,
        job_id,
        liveness_probe=probe,
    )
    assert observed.detail is not None
    result = apply_import_job_recovery(
        db,
        None,
        job_id,
        action="retry",
        reason="backend-neutral transcript",
        evidence_revision=observed.detail.evidence_revision,
        liveness_probe=probe,
    )
    assert result.retry_job is not None
    retry_id = result.retry_job.id
    old = db.get_import_job(job_id)
    request = db.get_request(request_id)
    moved = db.get_processing_cleanup_journal(
        request_id=request_id,
        job_id=retry_id,
    )
    replacement = get_automation_recovery_detail(
        db,
        None,
        retry_id,
    )
    assert (
        old is not None
        and request is not None
        and moved is not None
        and replacement.detail is not None
    )
    preview_claim = db.claim_next_import_preview_job(
        worker_id="transcript-preview",
        execution_lease=_replacement_lease("transcript-preview"),
    )
    import_claim = db.claim_next_import_job(
        worker_id="transcript-import",
        execution_lease=_replacement_lease("transcript-import"),
    )
    return {
        "outcome": result.outcome,
        "old_status": old.status,
        "retry_status": result.retry_job.status,
        "request_status": request["status"],
        "owner_is_retry": request["active_automation_import_job_id"] == retry_id,
        "old_journal_missing": db.get_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
        ) is None,
        "journal_action": moved["action"],
        "journal_path_preserved": moved["source_path"] == canonical_path,
        "journal_revision_delta": moved["revision"] - journal["revision"],
        "preview_claimed": preview_claim is not None,
        "import_claimed": import_claim is not None,
        "replacement_liveness": (
            replacement.detail.execution_liveness.status,
            replacement.detail.execution_liveness.reason,
        ),
    }


class TestAutomationRecoveryMutationsFakeParity(unittest.TestCase):
    def _fake_owner(self, *, canonical_path: str):
        from tests.fakes import FakePipelineDB
        from tests.helpers import make_request_row

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id=_RELEASE_ID,
        ))
        job = handoff_automation_owner(
            db,
            42,
            canonical_path=canonical_path,
        )
        return db, job

    def test_fake_retry_matches_real_owner_and_journal_swap(self) -> None:
        db, job = self._fake_owner(canonical_path="/processing/fake-retry")
        row = next(item for item in db._import_jobs if item["id"] == job.id)
        row.update({
            "status": "recovery_required",
            "execution_invocation_id": "old-claim",
            "execution_host_boot_id": "boot-before-restart",
            "execution_systemd_unit": "cratedigger-importer.service",
            "execution_worker_pid": 101,
            "execution_worker_start_ticks": 1001,
        })
        journal = db.create_processing_cleanup_journal(
            request_id=42,
            job_id=job.id,
            intent=CleanupJournalIntent(
                action="no_op",
                source_path="/processing/fake-retry",
                source_manifest=(),
                source_manifest_hash=cleanup_manifest_hash(()),
            ),
        )
        probe = _ChangedBootProbe()
        detail = get_automation_recovery_detail(
            db,
            None,
            job.id,
            liveness_probe=probe,
        )
        assert detail.detail is not None

        result = apply_import_job_recovery(
            db,
            None,
            job.id,
            action="retry",
            reason="fake parity",
            evidence_revision=detail.detail.evidence_revision,
            liveness_probe=probe,
        )

        self.assertEqual(result.outcome, "retry_recovery_required")
        assert result.retry_job is not None
        self.assertEqual(result.retry_job.status, "recovery_required")
        self.assertIsNone(db.claim_next_import_preview_job(
            worker_id="must-not-replay-preview",
            execution_lease=_replacement_lease("fake-preview"),
        ))
        self.assertIsNone(db.claim_next_import_job(
            worker_id="must-not-replay-import",
            execution_lease=_replacement_lease("fake-import"),
        ))
        replacement_detail = get_automation_recovery_detail(
            db,
            None,
            result.retry_job.id,
        )
        assert replacement_detail.detail is not None
        self.assertEqual(
            replacement_detail.detail.execution_liveness.status,
            "dead",
        )
        self.assertEqual(
            replacement_detail.detail.execution_liveness.reason,
            "never_claimed",
        )
        self.assertEqual(db.request(42)["status"], "processing")
        self.assertEqual(
            db.request(42)["active_automation_import_job_id"],
            result.retry_job.id,
        )
        moved = db.get_processing_cleanup_journal(
            request_id=42,
            job_id=result.retry_job.id,
        )
        assert moved is not None
        self.assertEqual(moved["source_path"], journal["source_path"])
        self.assertEqual(moved["step_progress"], journal["step_progress"])
        self.assertEqual(moved["revision"], journal["revision"] + 1)

    def test_fake_close_matches_real_explicit_imported_no_op(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "already-gone")
            db, job = self._fake_owner(canonical_path=canonical)
            detail = get_automation_recovery_detail(
                db,
                None,
                job.id,
            )
            assert detail.detail is not None
            result = apply_import_job_recovery(
                db,
                None,
                job.id,
                action="close",
                reason="fake explicit imported",
                evidence_revision=detail.detail.evidence_revision,
                result_status="imported",
            )
            self.assertEqual(result.outcome, "closed")
            self.assertEqual(db.request(42)["status"], "imported")
            closed = db.get_import_job(job.id)
            assert closed is not None
            self.assertEqual(closed.status, "failed")
            assert closed.result is not None
            cleanup = msgspec.convert(
                closed.result["processing_cleanup"],
                type=dict[str, object],
            )
            self.assertEqual(cleanup["outcome"], "no_op")

    def test_unknown_execution_blocks_action_without_mutation(self) -> None:
        db, job = self._fake_owner(canonical_path="/processing/unknown")
        row = next(item for item in db._import_jobs if item["id"] == job.id)
        row.update({
            "status": "recovery_required",
            "execution_invocation_id": "old-claim",
            "execution_host_boot_id": "boot-before-restart",
            "execution_systemd_unit": "cratedigger-importer.service",
            "execution_worker_pid": 101,
            "execution_worker_start_ticks": 1001,
        })
        probe = _UnknownProbe()
        detail = get_automation_recovery_detail(
            db,
            None,
            job.id,
            liveness_probe=probe,
        )
        assert detail.detail is not None
        result = apply_import_job_recovery(
            db,
            None,
            job.id,
            action="retry",
            reason="must fail closed",
            evidence_revision=detail.detail.evidence_revision,
            liveness_probe=probe,
        )
        self.assertEqual(result.outcome, "execution_unknown")
        current = db.get_import_job(job.id)
        assert current is not None
        self.assertEqual(current.status, "recovery_required")
        self.assertEqual(
            db.request(42)["active_automation_import_job_id"],
            job.id,
        )


class TestAutomationRecoveryMutationsPostgres(unittest.TestCase):
    def setUp(self) -> None:
        from lib.pipeline_db import PipelineDB

        self.db = PipelineDB(TEST_DSN)
        self.db._execute("TRUNCATE album_requests CASCADE")
        self.db.conn.commit()

    def tearDown(self) -> None:
        self.db.close()

    def _owner(self, canonical_path: str):
        request_id = self.db.add_request(
            "Recovery Artist",
            "Recovery Album",
            "request",
            mb_release_id=_RELEASE_ID,
        )
        job = handoff_automation_owner(
            self.db,
            request_id,
            canonical_path=canonical_path,
        )
        return request_id, job

    def _make_recovery_required(self, job_id: int) -> None:
        self.db._execute(
            """
            UPDATE import_jobs
            SET status = 'recovery_required',
                execution_invocation_id = 'old-claim',
                execution_host_boot_id = 'boot-before-restart',
                execution_systemd_unit = 'cratedigger-importer.service',
                execution_worker_pid = 101,
                execution_worker_start_ticks = 1001
            WHERE id = %s
            """,
            (job_id,),
        )

    def _revision(
        self,
        job_id: int,
        *,
        probe: ExecutionLivenessProbe | None = None,
    ) -> str:
        result = get_automation_recovery_detail(
            self.db,
            None,
            job_id,
            liveness_probe=probe,
        )
        assert result.detail is not None
        return result.detail.evidence_revision

    def test_retry_transcript_is_identical_on_fake_and_postgres(self) -> None:
        from tests.fakes import FakePipelineDB
        from tests.helpers import make_request_row

        canonical_path = "/processing/backend-neutral-retry"
        request_id, real_job = self._owner(canonical_path)
        self._make_recovery_required(real_job.id)

        fake = FakePipelineDB()
        fake.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id=_RELEASE_ID,
        ))
        fake_job = handoff_automation_owner(
            fake,
            42,
            canonical_path=canonical_path,
        )
        fake_row = next(
            item for item in fake._import_jobs
            if item["id"] == fake_job.id
        )
        fake_row.update({
            "status": "recovery_required",
            "execution_invocation_id": "old-claim",
            "execution_host_boot_id": "boot-before-restart",
            "execution_systemd_unit": "cratedigger-importer.service",
            "execution_worker_pid": 101,
            "execution_worker_start_ticks": 1001,
        })

        fake_transcript = _normalized_retry_transcript(
            fake,
            request_id=42,
            job_id=fake_job.id,
            canonical_path=canonical_path,
        )
        real_transcript = _normalized_retry_transcript(
            self.db,
            request_id=request_id,
            job_id=real_job.id,
            canonical_path=canonical_path,
        )

        self.assertEqual(real_transcript, fake_transcript)
        self.assertEqual(real_transcript, {
            "outcome": "retry_recovery_required",
            "old_status": "failed",
            "retry_status": "recovery_required",
            "request_status": "processing",
            "owner_is_retry": True,
            "old_journal_missing": True,
            "journal_action": "no_op",
            "journal_path_preserved": True,
            "journal_revision_delta": 1,
            "preview_claimed": False,
            "import_claimed": False,
            "replacement_liveness": ("dead", "never_claimed"),
        })

    def test_rejection_matrix_is_identical_on_fake_and_postgres(self) -> None:
        from lib.pipeline_db import PipelineDB
        from tests.fakes import FakePipelineDB
        from tests.helpers import make_request_row

        cases: tuple[
            tuple[
                str,
                str,
                bool,
                Literal["retry", "close"],
                Literal["wanted", "imported"] | None,
                bool,
            ],
            ...,
        ] = (
            (
                "live",
                "execution_live",
                False,
                "retry",
                None,
                True,
            ),
            (
                "execution_unknown",
                "execution_unknown",
                False,
                "retry",
                None,
                True,
            ),
            (
                "stale_revision",
                "evidence_changed",
                True,
                "retry",
                None,
                True,
            ),
            (
                "import_lock_unavailable",
                "lock_unavailable",
                False,
                "retry",
                None,
                True,
            ),
            (
                "wrong_state",
                "wrong_state",
                False,
                "retry",
                None,
                True,
            ),
            (
                "ineligible",
                "ineligible",
                False,
                "retry",
                None,
                True,
            ),
            (
                "release_lock_unavailable",
                "lock_unavailable",
                False,
                "close",
                "wanted",
                False,
            ),
            (
                "late_cas_rejection",
                "evidence_changed",
                False,
                "retry",
                None,
                True,
            ),
            (
                "cleanup_blocked",
                "cleanup_uninspectable",
                False,
                "close",
                "wanted",
                False,
            ),
        )

        for (
            name,
            expected_outcome,
            stale_revision,
            action,
            result_status,
            seed_journal,
        ) in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as root:
                canonical_path = os.path.join(root, "canonical")
                if name == "cleanup_blocked":
                    held_source = os.path.join(root, "held-source")
                    os.mkdir(held_source)
                    with open(
                        os.path.join(held_source, "track.flac"),
                        "wb",
                    ) as handle:
                        handle.write(b"preserved audio")
                    os.symlink(
                        held_source,
                        canonical_path,
                        target_is_directory=True,
                    )
                else:
                    os.mkdir(canonical_path)
                    with open(
                        os.path.join(canonical_path, "track.flac"),
                        "wb",
                    ) as handle:
                        handle.write(b"preserved audio")

                fake = FakePipelineDB()
                fake.seed_request(make_request_row(
                    id=42,
                    status="wanted",
                    mb_release_id=_RELEASE_ID,
                    artist_name="Recovery Artist",
                    album_title="Recovery Album",
                    year=None,
                    country=None,
                ))
                fake_job = handoff_automation_owner(
                    fake,
                    42,
                    canonical_path=canonical_path,
                )
                fake_row = next(
                    item for item in fake._import_jobs
                    if item["id"] == fake_job.id
                )
                fake_row.update({
                    "status": (
                        "queued"
                        if name == "wrong_state"
                        else "recovery_required"
                    ),
                    "result": {"sentinel": "preserve"},
                    "message": "preserve-message",
                    "error": "preserve-error",
                    "attempts": 3,
                    "worker_id": "preserve-worker",
                    "preview_status": "evidence_ready",
                    "preview_message": "Preview gate disabled",
                    "preview_completed_at": fake_row["updated_at"],
                    "importable_at": fake_row["updated_at"],
                    "execution_invocation_id": "old-claim",
                    "execution_host_boot_id": "boot-before-restart",
                    "execution_systemd_unit": (
                        "cratedigger-importer.service"
                    ),
                    "execution_worker_pid": 101,
                    "execution_worker_start_ticks": 1001,
                })
                if name == "ineligible":
                    fake.request(42)["active_download_state"] = None
                if seed_journal:
                    fake.create_processing_cleanup_journal(
                        request_id=42,
                        job_id=fake_job.id,
                        intent=CleanupJournalIntent(
                            action="no_op",
                            source_path=canonical_path,
                            source_manifest=(),
                            source_manifest_hash=cleanup_manifest_hash(()),
                        ),
                    )

                self.db._execute("TRUNCATE album_requests CASCADE")
                self.db.conn.commit()
                request_id, real_job = self._owner(canonical_path)
                self.db._execute(
                    """
                    UPDATE import_jobs
                    SET status = %s,
                        result = '{"sentinel":"preserve"}'::jsonb,
                        message = 'preserve-message',
                        error = 'preserve-error',
                        attempts = 3,
                        worker_id = 'preserve-worker',
                        preview_status = 'evidence_ready',
                        preview_message = 'Preview gate disabled',
                        preview_completed_at = COALESCE(
                            preview_completed_at,
                            updated_at
                        ),
                        importable_at = COALESCE(importable_at, updated_at),
                        execution_invocation_id = 'old-claim',
                        execution_host_boot_id = 'boot-before-restart',
                        execution_systemd_unit =
                            'cratedigger-importer.service',
                        execution_worker_pid = 101,
                        execution_worker_start_ticks = 1001
                    WHERE id = %s
                    """,
                    (
                        (
                            "queued"
                            if name == "wrong_state"
                            else "recovery_required"
                        ),
                        real_job.id,
                    ),
                )
                if name == "ineligible":
                    self.db._execute(
                        """
                        UPDATE album_requests
                        SET active_download_state = NULL
                        WHERE id = %s
                        """,
                        (request_id,),
                    )
                if seed_journal:
                    self.db.create_processing_cleanup_journal(
                        request_id=request_id,
                        job_id=real_job.id,
                        intent=CleanupJournalIntent(
                            action="no_op",
                            source_path=canonical_path,
                            source_manifest=(),
                            source_manifest_hash=cleanup_manifest_hash(()),
                        ),
                    )

                fake_lock: AbstractContextManager[object] | None = None
                real_lock: AbstractContextManager[object] | None = None
                holder: PipelineDB | None = None
                fake_after_apply: Callable[[], None] | None = None
                real_after_apply: Callable[[], None] | None = None
                fake_probe: ExecutionLivenessProbe
                real_probe: ExecutionLivenessProbe
                if name == "live":
                    fake_probe = _LiveProbe()
                    real_probe = _LiveProbe()
                elif name == "execution_unknown":
                    fake_probe = _UnknownProbe()
                    real_probe = _UnknownProbe()
                elif name == "late_cas_rejection":
                    fake_probe = _MutatingOnSecondChangedBootProbe(partial(
                        _set_fake_execution_invocation,
                        fake._import_jobs,
                        fake_job.id,
                        "fresh-claim",
                    ))
                    real_probe = _MutatingOnSecondChangedBootProbe(partial(
                        _set_real_execution_invocation,
                        self.db,
                        real_job.id,
                        "fresh-claim",
                    ))
                    fake_after_apply = partial(
                        _set_fake_execution_invocation,
                        fake._import_jobs,
                        fake_job.id,
                        "old-claim",
                    )
                    real_after_apply = partial(
                        _set_real_execution_invocation,
                        self.db,
                        real_job.id,
                        "old-claim",
                    )
                else:
                    fake_probe = _ChangedBootProbe()
                    real_probe = _ChangedBootProbe()

                if name == "import_lock_unavailable":
                    fake.set_advisory_lock_result(False)
                    holder = PipelineDB(TEST_DSN)
                    real_lock = holder.advisory_lock(
                        ADVISORY_LOCK_NAMESPACE_IMPORT,
                        request_id,
                    )
                elif name == "release_lock_unavailable":
                    fake.set_advisory_lock_result(
                        lambda namespace, _key: (
                            namespace != ADVISORY_LOCK_NAMESPACE_RELEASE
                        )
                    )
                    holder = PipelineDB(TEST_DSN)
                    real_lock = holder.advisory_lock(
                        ADVISORY_LOCK_NAMESPACE_RELEASE,
                        release_id_to_lock_key(_RELEASE_ID),
                    )

                try:
                    fake_transcript = _normalized_rejection_transcript(
                        fake,
                        request_id=42,
                        job_id=fake_job.id,
                        canonical_path=canonical_path,
                        expected_outcome=expected_outcome,
                        probe=fake_probe,
                        stale_revision=stale_revision,
                        action=action,
                        result_status=result_status,
                        lock_scope=fake_lock,
                        after_apply=fake_after_apply,
                    )
                    real_transcript = _normalized_rejection_transcript(
                        self.db,
                        request_id=request_id,
                        job_id=real_job.id,
                        canonical_path=canonical_path,
                        expected_outcome=expected_outcome,
                        probe=real_probe,
                        stale_revision=stale_revision,
                        action=action,
                        result_status=result_status,
                        lock_scope=real_lock,
                        after_apply=real_after_apply,
                    )
                finally:
                    if holder is not None:
                        holder.close()

                self.assertEqual(real_transcript, fake_transcript)
                self.assertEqual(
                    real_transcript["outcome"],
                    expected_outcome,
                )
                self.assertTrue(real_transcript["action_job_absent"])
                self.assertTrue(real_transcript["retry_job_absent"])
                self.assertTrue(real_transcript["state_unchanged"])
                self.assertTrue(
                    real_transcript["protected_job_fields_unchanged"],
                )
                self.assertEqual(
                    real_transcript["before"],
                    real_transcript["after"],
                )
                if name == "release_lock_unavailable":
                    self.assertEqual(
                        [
                            namespace
                            for namespace, _key
                            in fake.advisory_lock_calls[-2:]
                        ],
                        [
                            ADVISORY_LOCK_NAMESPACE_IMPORT,
                            ADVISORY_LOCK_NAMESPACE_RELEASE,
                        ],
                    )
                if name == "late_cas_rejection":
                    assert isinstance(
                        fake_probe,
                        _MutatingOnSecondChangedBootProbe,
                    )
                    assert isinstance(
                        real_probe,
                        _MutatingOnSecondChangedBootProbe,
                    )
                    self.assertEqual(fake_probe.mutation_calls, 1)
                    self.assertEqual(real_probe.mutation_calls, 1)
                    self.assertEqual(fake_probe.observe_calls, 3)
                    self.assertEqual(real_probe.observe_calls, 3)

    def test_retry_swaps_owner_and_retargets_journal_atomically(self) -> None:
        request_id, job = self._owner("/processing/retry")
        self._make_recovery_required(job.id)
        journal = self.db.create_processing_cleanup_journal(
            request_id=request_id,
            job_id=job.id,
            intent=CleanupJournalIntent(
                action="no_op",
                source_path="/processing/retry",
                source_manifest=(),
                source_manifest_hash=cleanup_manifest_hash(()),
            ),
        )
        probe = _ChangedBootProbe()
        revision = self._revision(job.id, probe=probe)

        result = apply_import_job_recovery(
            self.db,
            None,
            job.id,
            action="retry",
            reason="operator confirmed Beets did not apply",
            evidence_revision=revision,
            liveness_probe=probe,
        )

        self.assertEqual(result.outcome, "retry_recovery_required")
        assert result.retry_job is not None
        old = self.db.get_import_job(job.id)
        request = self.db.get_request(request_id)
        moved = self.db.get_processing_cleanup_journal(
            request_id=request_id,
            job_id=result.retry_job.id,
        )
        assert old is not None and request is not None and moved is not None
        self.assertEqual(old.status, "failed")
        self.assertEqual(result.retry_job.status, "recovery_required")
        self.assertIsNone(self.db.claim_next_import_preview_job(
            worker_id="must-not-replay-preview",
            execution_lease=_replacement_lease("real-preview"),
        ))
        self.assertIsNone(self.db.claim_next_import_job(
            worker_id="must-not-replay-import",
            execution_lease=_replacement_lease("real-import"),
        ))
        replacement_detail = get_automation_recovery_detail(
            self.db,
            None,
            result.retry_job.id,
        )
        assert replacement_detail.detail is not None
        self.assertEqual(
            replacement_detail.detail.execution_liveness.status,
            "dead",
        )
        self.assertEqual(
            replacement_detail.detail.execution_liveness.reason,
            "never_claimed",
        )
        self.assertEqual(request["status"], "processing")
        self.assertEqual(
            request["active_automation_import_job_id"],
            result.retry_job.id,
        )
        self.assertIsNone(
            self.db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job.id,
            )
        )
        self.assertEqual(moved["action"], journal["action"])
        self.assertEqual(moved["source_path"], journal["source_path"])
        self.assertEqual(moved["step_progress"], journal["step_progress"])
        self.assertEqual(moved["revision"], journal["revision"] + 1)

    def test_retry_fault_and_fresh_claim_never_partially_swap_owner(self) -> None:
        request_id, job = self._owner("/processing/retry-race")
        self._make_recovery_required(job.id)
        probe = _ChangedBootProbe()
        revision = self._revision(job.id, probe=probe)

        def fail_after_insert(index: int, label: str) -> None:
            if index == 2:
                raise RuntimeError(label)

        self.db._automation_recovery_write_boundary = fail_after_insert
        with self.assertRaises(RuntimeError):
            apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="retry",
                reason="fault injection",
                evidence_revision=revision,
                liveness_probe=probe,
            )
        del self.db._automation_recovery_write_boundary
        request = self.db.get_request(request_id)
        original = self.db.get_import_job(job.id)
        assert request is not None and original is not None
        self.assertEqual(request["active_automation_import_job_id"], job.id)
        self.assertEqual(original.status, "recovery_required")
        active = [
            item
            for item in self.db.list_import_jobs(limit=20)
            if item.request_id == request_id
            and item.status in {"queued", "running", "recovery_required"}
        ]
        self.assertEqual([item.id for item in active], [job.id])

        changed = apply_import_job_recovery(
            self.db,
            None,
            job.id,
            action="retry",
            reason="claim raced observation",
            evidence_revision=revision,
            liveness_probe=_MutatingChangedBootProbe(self.db, job.id),
        )
        self.assertEqual(changed.outcome, "evidence_changed")
        request = self.db.get_request(request_id)
        original = self.db.get_import_job(job.id)
        assert request is not None and original is not None
        self.assertEqual(request["active_automation_import_job_id"], job.id)
        self.assertEqual(original.status, "recovery_required")

    def test_close_cleans_exact_tree_then_applies_explicit_wanted(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "album")
            os.mkdir(canonical)
            with open(os.path.join(canonical, "track.flac"), "wb") as handle:
                handle.write(b"audio")
            request_id, job = self._owner(canonical)
            revision = self._revision(job.id)

            result = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="operator reconciled ambiguous queued owner",
                evidence_revision=revision,
                result_status="wanted",
            )

            self.assertEqual(result.outcome, "closed")
            self.assertFalse(os.path.exists(canonical))
            request = self.db.get_request(request_id)
            closed = self.db.get_import_job(job.id)
            assert request is not None and closed is not None
            self.assertEqual(request["status"], "wanted")
            self.assertIsNone(request["active_automation_import_job_id"])
            self.assertIsNone(request["active_download_state"])
            self.assertEqual(closed.status, "failed")
            self.assertIsNone(
                self.db.get_processing_cleanup_journal(
                    request_id=request_id,
                    job_id=job.id,
                )
            )
            assert closed.result is not None
            resolution = msgspec.convert(
                closed.result["recovery_resolution"],
                type=dict[str, object],
            )
            self.assertEqual(resolution["result_status"], "wanted")
            self.assertEqual(resolution["reason"], (
                "operator reconciled ambiguous queued owner"
            ))

    def test_close_session_loss_leaves_owner_and_journal_for_resume(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "album")
            os.mkdir(canonical)
            with open(os.path.join(canonical, "track.flac"), "wb") as handle:
                handle.write(b"audio")
            request_id, job = self._owner(canonical)
            revision = self._revision(job.id)
            probes = 0

            def lose_session_before_mutation(
                identity: OwnerSessionIdentity,
            ) -> OwnerSessionProbe:
                nonlocal probes
                probes += 1
                return OwnerSessionProbe(
                    live=probes < 4,
                    reason=(
                        "exact_backend_alive"
                        if probes < 4
                        else "fault_injected_session_loss"
                    ),
                    expected=identity,
                    observed_backend_pid=(
                        identity.backend_pid if probes < 4 else None
                    ),
                )

            with (
                patch(
                    "lib.import_execution.threading.Thread",
                ),
                patch.object(
                    self.db,
                    "_probe_owner_session",
                    side_effect=lose_session_before_mutation,
                ),
            ):
                result = apply_import_job_recovery(
                    self.db,
                    None,
                    job.id,
                    action="close",
                    reason="session loss must fail stop",
                    evidence_revision=revision,
                    result_status="wanted",
                )

            self.assertEqual(result.outcome, "cleanup_failed")
            self.assertIn("fault_injected_session_loss", result.message)
            self.assertTrue(os.path.exists(canonical))
            request = self.db.get_request(request_id)
            current = self.db.get_import_job(job.id)
            journal = self.db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job.id,
            )
            assert request is not None and current is not None
            self.assertEqual(request["status"], "processing")
            self.assertEqual(
                request["active_automation_import_job_id"],
                job.id,
            )
            self.assertEqual(current.status, "queued")
            self.assertIsNotNone(journal)
            assert journal is not None
            self.assertIsNone(journal["completed_receipt"])

    def test_stale_revision_and_invalid_close_leave_owner_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "album")
            os.mkdir(canonical)
            request_id, job = self._owner(canonical)

            with self.assertRaises(ValueError):
                apply_import_job_recovery(
                    self.db,
                    None,
                    job.id,
                    action="close",
                    reason="missing explicit result",
                    evidence_revision="sha256:stale",
                )
            stale = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="stale observation",
                evidence_revision="sha256:stale",
                result_status="imported",
            )
            self.assertEqual(stale.outcome, "evidence_changed")
            self.assertTrue(os.path.isdir(canonical))
            request = self.db.get_request(request_id)
            current = self.db.get_import_job(job.id)
            assert request is not None and current is not None
            self.assertEqual(request["status"], "processing")
            self.assertEqual(request["active_automation_import_job_id"], job.id)
            self.assertEqual(current.status, "queued")

    def test_close_proven_missing_source_applies_explicit_imported_no_op(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "already-gone")
            request_id, job = self._owner(canonical)
            revision = self._revision(job.id)

            result = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="operator verified exact release in Beets",
                evidence_revision=revision,
                result_status="imported",
            )

            self.assertEqual(result.outcome, "closed")
            request = self.db.get_request(request_id)
            closed = self.db.get_import_job(job.id)
            assert request is not None and closed is not None
            self.assertEqual(request["status"], "imported")
            self.assertIsNone(request["active_automation_import_job_id"])
            self.assertEqual(closed.status, "failed")
            assert closed.result is not None
            cleanup = msgspec.convert(
                closed.result["processing_cleanup"],
                type=dict[str, object],
            )
            self.assertEqual(cleanup["outcome"], "no_op")
            resolution = msgspec.convert(
                closed.result["recovery_resolution"],
                type=dict[str, object],
            )
            self.assertEqual(resolution["result_status"], "imported")

    def test_close_resumes_completed_cleanup_after_terminal_conflict(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "album")
            os.mkdir(canonical)
            with open(os.path.join(canonical, "track.flac"), "wb") as handle:
                handle.write(b"audio")
            request_id, job = self._owner(canonical)
            revision = self._revision(job.id)

            with patch.object(
                self.db,
                "persist_import_terminal_outcome",
                side_effect=ImportJobTerminalConflict("fault after cleanup"),
            ):
                first = apply_import_job_recovery(
                    self.db,
                    None,
                    job.id,
                    action="close",
                    reason="resume exact cleanup declaration",
                    evidence_revision=revision,
                    result_status="wanted",
                )

            self.assertEqual(first.outcome, "evidence_changed")
            self.assertFalse(os.path.exists(canonical))
            assert first.detail is not None
            self.assertEqual(first.detail.cleanup_journal.status, "completed")
            request = self.db.get_request(request_id)
            assert request is not None
            self.assertEqual(request["status"], "processing")
            resumed = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="resume exact cleanup declaration",
                evidence_revision=first.detail.evidence_revision,
                result_status="wanted",
            )
            self.assertEqual(resumed.outcome, "closed")
            request = self.db.get_request(request_id)
            assert request is not None
            self.assertEqual(request["status"], "wanted")

    def test_close_blocks_uninspectable_source_without_journal_or_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            held_source = os.path.join(root, "held-source")
            os.mkdir(held_source)
            source = os.path.join(root, "uninspectable")
            os.symlink(held_source, source, target_is_directory=True)
            request_id, job = self._owner(source)
            revision = self._revision(job.id)
            result = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="must not infer absence",
                evidence_revision=revision,
                result_status="wanted",
            )
            self.assertEqual(result.outcome, "cleanup_uninspectable")
            request = self.db.get_request(request_id)
            current = self.db.get_import_job(job.id)
            assert request is not None and current is not None
            self.assertEqual(request["status"], "processing")
            self.assertEqual(current.status, "queued")
            self.assertIsNone(
                self.db.get_processing_cleanup_journal(
                    request_id=request_id,
                    job_id=job.id,
                )
            )

    def test_close_is_break_glass_for_proven_dead_wedged_running_owner(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "already-gone")
            request_id, job = self._owner(canonical)
            self.db._execute(
                """
                UPDATE import_jobs
                SET status = 'running',
                    execution_invocation_id = 'old-claim',
                    execution_host_boot_id = 'boot-before-restart',
                    execution_systemd_unit = 'cratedigger-importer.service',
                    execution_worker_pid = 101,
                    execution_worker_start_ticks = 1001
                WHERE id = %s
                """,
                (job.id,),
            )
            probe = _ChangedBootProbe()
            revision = self._revision(job.id, probe=probe)
            result = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="wedged owner is proven dead",
                evidence_revision=revision,
                result_status="wanted",
                liveness_probe=probe,
            )
            self.assertEqual(result.outcome, "closed")
            request = self.db.get_request(request_id)
            closed = self.db.get_import_job(job.id)
            assert request is not None and closed is not None
            self.assertEqual(request["status"], "wanted")
            self.assertEqual(closed.status, "failed")


if __name__ == "__main__":
    unittest.main()
