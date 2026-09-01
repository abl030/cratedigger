"""FakePipelineDB transfer_ledger cluster — mirrors ``lib/pipeline_db/transfer_ledger.py``.

The slskd transfer ownership ledger (migration 045).
"""
from __future__ import annotations

import json
from collections.abc import (
    Mapping,
    Sequence,
)
from datetime import datetime

from lib.pipeline_db import (
    TransferLedgerRow,
)
from tests.fakes._shared import _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase
from tests.fakes.rows import (
    FakeTransferLedgerRow,
)


class _FakeTransferLedgerMixin(_FakePipelineDBBase):
    """The slskd transfer ownership ledger (migration 045)."""


    @staticmethod
    def _attempt_fingerprint_from_state(
        request: Mapping[str, object],
    ) -> str | None:
        """Mirror ``active_download_state ->> 'attempt_fingerprint'``
        (#1196 item 1) for the two shapes production ever writes: the
        top-level state is SQL NULL (or a non-object value -- ``->>``
        returns NULL for a NULL or non-object jsonb regardless of key,
        matching ``None`` here), or the key is absent/JSON-null
        (``->>`` also returns NULL, matching the ``dict.get`` miss
        here). Does NOT distinguish a missing key from an explicit JSON
        ``null`` -- ``->>`` does not either.

        Known, deliberately UNRECONCILED divergence: if the
        ``attempt_fingerprint`` JSON value were ever a non-string
        scalar (a number, bool), real ``->>`` stringifies it (e.g.
        ``42`` -> ``'42'``) rather than returning NULL, but this helper
        returns ``None`` for that case. Unreachable in practice --
        ``lib.download.build_active_download_state`` (the only
        production writer) emits either a Python ``str`` or omits the
        key entirely (``omit_defaults=True``), never a bare number or
        bool -- so this divergence has no real-world state to exercise
        it against."""
        raw = request.get("active_download_state")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (TypeError, ValueError):
                return None
        if not isinstance(raw, dict):
            return None
        value = raw.get("attempt_fingerprint")
        return value if isinstance(value, str) else None

    def record_transfer_enqueue(self, rows: list[TransferLedgerRow]) -> None:
        """Write-ahead batch insert -- mirrors the real INSERT's "one row
        per input row, always appended" semantics (no dedup, unlike the
        search ledger's ON CONFLICT DO NOTHING -- there is no natural key
        here, every enqueue is a fresh row)."""
        self.record_transfer_enqueue_calls.extend(rows)
        for row in rows:
            fake_id = self._transfer_ledger_next_id
            self._transfer_ledger_next_id += 1
            self._transfer_ledger[fake_id] = FakeTransferLedgerRow(
                id=fake_id,
                request_id=row.request_id,
                username=row.username,
                filename=row.filename,
                attempt_fingerprint=row.attempt_fingerprint,
            )

    def stamp_transfer_completion(
        self,
        username: str,
        filename: str,
        local_path: str,
    ) -> int:
        """Mirror newest-open exact-key completion-path stamping."""
        if any(
            row.username == username
            and row.filename == filename
            and row.local_path == local_path
            for row in self._transfer_ledger.values()
        ):
            return 0
        candidates = [
            row for row in self._transfer_ledger.values()
            if row.username == username and row.filename == filename
            and row.accepted_at is not None
            and row.local_path is None
        ]
        if not candidates:
            return 0
        newest = max(candidates, key=lambda row: (row.enqueued_at, row.id))
        newest.local_path = local_path
        return 1

    def confirm_transfer_enqueue(
        self, username: str, filename: str, *, request_id: int,
    ) -> int:
        """Mirror the request-scoped accepted-POST promotion (#1278 item 2):
        only ``request_id``'s OWN newest pending row for this key may be
        promoted, never a sibling request's."""
        self.confirm_transfer_enqueue_calls.append(
            (username, filename, request_id))
        candidates = [
            row for row in self._transfer_ledger.values()
            if row.username == username and row.filename == filename
            and row.request_id == request_id
            and row.accepted_at is None
        ]
        if not candidates:
            return 0
        newest = max(candidates, key=lambda row: (row.enqueued_at, row.id))
        newest.accepted_at = _utcnow()
        return 1

    def get_owned_transfer_keys(self) -> set[tuple[str, str]]:
        """Mirror confirmed ownership, excluding pending write-ahead intent."""
        return {
            (r.username, r.filename)
            for r in self._transfer_ledger.values()
            if r.accepted_at is not None
        }

    def get_owned_transfer_keys_for(
        self, keys: Sequence[tuple[str, str]],
    ) -> set[tuple[str, str]]:
        """Mirror the keyed ownership read: the same accepted-only set
        ``get_owned_transfer_keys`` returns, intersected with ``keys``."""
        if not keys:
            return set()
        return self.get_owned_transfer_keys() & set(keys)

    def get_owned_local_paths(self) -> set[str]:
        return {
            r.local_path for r in self._transfer_ledger.values()
            if r.local_path is not None
        }

    def get_abandoned_owned_local_paths(self) -> set[str]:
        """Mirror the real join: owned paths whose request sits at ``wanted``
        with no ``active_download_state``, i.e. holds no reference to them."""
        abandoned: set[str] = set()
        for row in self._transfer_ledger.values():
            if row.local_path is None:
                continue
            request = self._requests.get(row.request_id)
            if request is None:
                continue
            if (
                request.get("status") == "wanted"
                and request.get("active_download_state") is None
            ):
                abandoned.add(row.local_path)
        return abandoned

    def get_conflicting_transfer_request_ids(
        self,
        keys: Sequence[tuple[str, str]],
        exclude_request_id: int,
    ) -> set[int]:
        """Mirror the real ledger join: accepted rows keyed by any of
        ``keys`` whose owning request is CURRENTLY 'downloading' on its
        CURRENT attempt, excluding ``exclude_request_id`` (#1178 PR2,
        attempt-scoped per review F2; exact fingerprint identity added
        #1196 item 1; deploy-window time fallback deleted #1199 item 2 --
        the measured cohort was empty). A missing request row (never
        seeded, or hard-deleted) mirrors the real INNER JOIN -- never a
        conflict.

        Two-armed, mirroring the real SQL ``CASE`` exactly:

          1. State carries a string ``attempt_fingerprint`` -- EXACT
             equality against the ledger row's own
             ``attempt_fingerprint`` decides in/out of scope; no clock
             comparison.
          2. Else (state is NULL, malformed, or lacks the key) -- fails
             CLOSED unconditionally: every accepted row for that
             ``'downloading'`` owner counts as in-scope (blocks),
             mirroring the real query's ``ELSE TRUE``.
        """
        key_set = set(keys)
        conflicting: set[int] = set()
        for row in self._transfer_ledger.values():
            if row.accepted_at is None:
                continue
            if row.request_id == exclude_request_id:
                continue
            if (row.username, row.filename) not in key_set:
                continue
            request = self._requests.get(row.request_id)
            if request is None:
                continue
            if request.get("status") != "downloading":
                continue
            fingerprint = self._attempt_fingerprint_from_state(request)
            if fingerprint is None:
                conflicting.add(row.request_id)
                continue
            if row.attempt_fingerprint == fingerprint:
                conflicting.add(row.request_id)
        return conflicting

    def prune_transfer_ledger(self, older_than: datetime) -> int:
        """Mirror strict age pruning: pending intents ignore request status;
        accepted rows retain active wanted/downloading protection."""
        active_statuses = ("wanted", "downloading")
        to_remove = []
        for fake_id, row in self._transfer_ledger.items():
            if row.enqueued_at >= older_than:
                continue
            request = self._requests.get(row.request_id)
            status = request.get("status") if request is not None else None
            if row.accepted_at is not None and status in active_statuses:
                continue
            to_remove.append(fake_id)
        for fake_id in to_remove:
            del self._transfer_ledger[fake_id]
        return len(to_remove)

