"""FakePipelineDB cleanup_journal cluster — mirrors ``lib/pipeline_db/cleanup_journal.py``.

The processing-cleanup journal receipts (#898).
"""
from __future__ import annotations

import copy
from collections.abc import (
    Mapping,
)

import msgspec

from lib.pipeline_db import (
    CleanupJournalConflict,
    CleanupJournalIntent,
    CleanupJournalReceipt,
    ProcessingCleanupJournalRow,
)
from tests.fakes._shared import _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase


class _FakeCleanupJournalMixin(_FakePipelineDBBase):
    """The processing-cleanup journal receipts (#898)."""

    def get_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
    ) -> ProcessingCleanupJournalRow | None:
        row = self._processing_cleanup_journals.get((job_id, request_id))
        return copy.deepcopy(row)


    def create_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        intent: CleanupJournalIntent,
    ) -> ProcessingCleanupJournalRow:
        self._require_fake_exact_processing_owner(
            request_id=request_id,
            job_id=job_id,
        )
        key = (job_id, request_id)
        existing = self._processing_cleanup_journals.get(key)
        manifest = msgspec.convert(
            msgspec.to_builtins(intent.source_manifest),
            type=list[dict[str, object]],
        )
        destination_manifest = (
            None
            if intent.destination_manifest is None
            else msgspec.convert(
                msgspec.to_builtins(intent.destination_manifest),
                type=list[dict[str, object]],
            )
        )
        if existing is not None:
            exact = (
                existing["action"] == intent.action
                and existing["source_path"] == intent.source_path
                and existing["source_manifest"] == manifest
                and existing["source_manifest_hash"]
                == intent.source_manifest_hash
                and existing["destination_path"] == intent.destination_path
                and existing["destination_manifest"]
                == destination_manifest
                and existing["destination_manifest_hash"]
                == intent.destination_manifest_hash
                and existing["selected_destination_path"]
                == intent.selected_destination_path
            )
            if not exact:
                raise CleanupJournalConflict(
                    "intent_conflict",
                    "cleanup journal already contains a different exact intent",
                )
            return copy.deepcopy(existing)
        now = _utcnow()
        row: ProcessingCleanupJournalRow = {
            "job_id": job_id,
            "request_id": request_id,
            "revision": 1,
            "action": intent.action,
            "source_path": intent.source_path,
            "source_manifest": manifest,
            "source_manifest_hash": intent.source_manifest_hash,
            "destination_path": intent.destination_path,
            "destination_manifest": destination_manifest,
            "destination_manifest_hash": intent.destination_manifest_hash,
            "selected_destination_path": intent.selected_destination_path,
            "step_progress": {},
            "completed_receipt": None,
            "created_at": now,
            "updated_at": now,
            "completed_at": None,
        }
        self._processing_cleanup_journals[key] = row
        return copy.deepcopy(row)

    def checkpoint_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        step_progress: Mapping[str, object],
    ) -> ProcessingCleanupJournalRow:
        row = self._processing_cleanup_journals.get((job_id, request_id))
        if row is None:
            raise CleanupJournalConflict(
                "journal_missing",
                "cleanup journal does not exist for the exact owner",
            )
        if row["revision"] != expected_revision:
            raise CleanupJournalConflict(
                "revision_conflict",
                "cleanup checkpoint revision changed",
            )
        row["revision"] += 1
        row["step_progress"] = copy.deepcopy(dict(step_progress))
        row["updated_at"] = _utcnow()
        return copy.deepcopy(row)

    def complete_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        receipt: CleanupJournalReceipt,
    ) -> ProcessingCleanupJournalRow:
        row = self._processing_cleanup_journals.get((job_id, request_id))
        if row is None:
            raise CleanupJournalConflict(
                "journal_missing",
                "cleanup journal does not exist for the exact owner",
            )
        if row["revision"] != expected_revision:
            raise CleanupJournalConflict(
                "revision_conflict",
                "cleanup completion revision changed",
            )
        if row["completed_receipt"] is not None:
            if row["completed_receipt"] != receipt:
                raise CleanupJournalConflict(
                    "receipt_conflict",
                    "cleanup journal already has a different completed receipt",
                )
            return copy.deepcopy(row)
        now = _utcnow()
        row["revision"] += 1
        row["completed_receipt"] = receipt
        row["completed_at"] = now
        row["updated_at"] = now
        return copy.deepcopy(row)

