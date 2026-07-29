"""Generated exact-CAS recovery contracts (#898 U4)."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile
from lib.pipeline_db.cleanup_journal import ProcessingCleanupJournalRow
from lib.pipeline_db.import_jobs import (
    AutomationRecoveryCAS,
    _recovery_cleanup_matches,
    _recovery_owner_matches,
)

_NOW = datetime(2026, 7, 29, tzinfo=UTC)


def _expected() -> AutomationRecoveryCAS:
    return AutomationRecoveryCAS(
        request_id=42,
        job_id=7,
        job_status="recovery_required",
        preview_status="evidence_ready",
        canonical_path="/processing/album",
        beets_launch_authorized_at=_NOW,
        beets_launch_release_id="release-a",
        beets_launch_source_path="/processing/album",
        beets_launch_request_status="processing",
        beets_launch_snapshot_fingerprint="snapshot-a",
        execution_invocation_id="invocation-a",
        execution_host_boot_id="boot-a",
        execution_systemd_unit="cratedigger-importer.service",
        execution_worker_pid=101,
        execution_worker_start_ticks=1001,
        execution_beets_pid=202,
        execution_beets_start_ticks=2002,
        cleanup_job_id=7,
        cleanup_request_id=42,
        cleanup_revision=3,
        cleanup_progress={"after:unlink:track.flac": True},
    )


def _owner_rows() -> tuple[dict[str, object], dict[str, object]]:
    request: dict[str, object] = {
        "id": 42,
        "status": "processing",
        "active_automation_import_job_id": 7,
        "active_download_state": {
            "current_path": "/processing/album",
        },
    }
    job: dict[str, object] = {
        "id": 7,
        "request_id": 42,
        "job_type": "automation_import",
        "status": "recovery_required",
        "preview_status": "evidence_ready",
        "completed_at": None,
        "beets_launch_authorized_at": _NOW,
        "beets_launch_release_id": "release-a",
        "beets_launch_source_path": "/processing/album",
        "beets_launch_request_status": "processing",
        "beets_launch_snapshot_fingerprint": "snapshot-a",
        "execution_invocation_id": "invocation-a",
        "execution_host_boot_id": "boot-a",
        "execution_systemd_unit": "cratedigger-importer.service",
        "execution_worker_pid": 101,
        "execution_worker_start_ticks": 1001,
        "execution_beets_pid": 202,
        "execution_beets_start_ticks": 2002,
    }
    return request, job


def _journal() -> ProcessingCleanupJournalRow:
    return ProcessingCleanupJournalRow(
        job_id=7,
        request_id=42,
        revision=3,
        action="remove_source_tree",
        source_path="/processing/album",
        source_manifest=[],
        source_manifest_hash="hash-a",
        destination_path=None,
        destination_manifest=None,
        destination_manifest_hash=None,
        selected_destination_path=None,
        step_progress={"after:unlink:track.flac": True},
        declared_result_status=None,
        declared_reason=None,
        evidence_revision=None,
        completed_receipt=None,
        created_at=_NOW,
        updated_at=_NOW,
        completed_at=None,
    )


_JOB_CAS_FIELDS = (
    "status",
    "preview_status",
    "beets_launch_authorized_at",
    "beets_launch_release_id",
    "beets_launch_source_path",
    "beets_launch_request_status",
    "beets_launch_snapshot_fingerprint",
    "execution_invocation_id",
    "execution_host_boot_id",
    "execution_systemd_unit",
    "execution_worker_pid",
    "execution_worker_start_ticks",
    "execution_beets_pid",
    "execution_beets_start_ticks",
)


class TestAutomationRecoveryCASGenerated(unittest.TestCase):
    @given(changed_field=st.sampled_from(_JOB_CAS_FIELDS))
    def test_every_owner_stage_launch_and_lease_field_is_cas_bound(
        self,
        changed_field: str,
    ) -> None:
        request, job = _owner_rows()
        observed = job[changed_field]
        job[changed_field] = (
            "changed"
            if not isinstance(observed, int)
            else observed + 1
        )
        self.assertFalse(_recovery_owner_matches(
            _expected(),
            request=request,
            job=job,
        ))

    @given(
        changed_revision=st.integers(min_value=1, max_value=100).filter(
            lambda value: value != 3
        ),
        changed_progress=st.dictionaries(
            st.text(min_size=1, max_size=12),
            st.booleans(),
            max_size=4,
        ).filter(
            lambda value: value != {"after:unlink:track.flac": True}
        ),
    )
    def test_cleanup_revision_and_progress_are_both_cas_bound(
        self,
        changed_revision: int,
        changed_progress: dict[str, bool],
    ) -> None:
        revision_changed = _journal()
        revision_changed["revision"] = changed_revision
        progress_changed = _journal()
        progress_changed["step_progress"] = {
            key: value for key, value in changed_progress.items()
        }
        self.assertFalse(_recovery_cleanup_matches(
            _expected(),
            revision_changed,
        ))
        self.assertFalse(_recovery_cleanup_matches(
            _expected(),
            progress_changed,
        ))

    def test_known_bad_job_only_cas_misses_canonical_path_race(self) -> None:
        request, job = _owner_rows()
        state = request["active_download_state"]
        if not isinstance(state, dict):
            raise AssertionError("fixture active download state must be an object")
        state["current_path"] = "/processing/new-incarnation"

        def job_only_mutant(raw: dict[str, object]) -> bool:
            return (
                raw["id"] == 7
                and raw["status"] == "recovery_required"
                and raw["execution_invocation_id"] == "invocation-a"
            )

        self.assertTrue(job_only_mutant(job))
        self.assertFalse(_recovery_owner_matches(
            _expected(),
            request=request,
            job=job,
        ))
