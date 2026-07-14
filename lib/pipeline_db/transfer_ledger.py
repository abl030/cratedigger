"""slskd transfer write-ahead ownership ledger (issue #571 good-citizen
doctrine, migration 045).

The methods this mixin adds:

* ``record_transfer_enqueue`` -- write-ahead batch INSERT, called BEFORE
  ``ctx.slskd.transfers.enqueue(...)`` (T1). This preserves intent across
  crashes without treating a rejected or ambiguous POST as ownership.
* ``confirm_transfer_enqueue`` -- accepted-POST write (T1.5). A write-ahead
  intent becomes destructive authority only after slskd accepts the POST.
* ``stamp_transfer_completion`` -- event-ingestion success write: T2. Called
  from the SAME pass ``lib/slskd_events.py`` already stamps
  ``active_download_state`` from (issue #146), using the same durable
  ``(username, filename)`` key.
* ``get_owned_transfer_keys`` / ``get_owned_local_paths`` -- purpose-shaped
  read surfaces for transfer convergence and the disk reaper.
* ``get_owned_attempt_folders`` -- read surface for the disk-reaper
  flip (issue #571 PR 4): "which canonical processing folders are
  mine?", joined to each ledgered attempt's request identity so the
  caller can re-derive the folder with
  ``lib.processing_paths.canonical_processing_path``.
* ``prune_transfer_ledger`` -- T3: keeps pending intents bounded by age;
  accepted ownership evidence is also protected while its request remains
  active (``wanted``/``downloading``).

See migrations 045 and 051 for the schema and rationale.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import msgspec
import psycopg2.extras

from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import TransferLedgerRow

# Accepted rows for active requests cannot be pruned regardless of age: the
# reaper/convergence paths may still need their ownership evidence while the
# request is being retried. Pending intents have no ownership value and are
# bounded solely by the retention window. Accepted rows for everything else
# (imported, manual, replaced, or a missing request) are also fair game once
# past that window.
_ACTIVE_REQUEST_STATUSES = ("wanted", "downloading")


class _TransferLedgerMixin(_PipelineDBBase):
    """slskd transfer write-ahead ownership ledger CRUD (migration 045)."""

    def record_transfer_enqueue(self, rows: list[TransferLedgerRow]) -> None:
        """Write-ahead batch insert: call this BEFORE
        ``ctx.slskd.transfers.enqueue(...)`` for every file in the same
        enqueue call (T1).

        The per-row INSERT column list is DERIVED from
        ``msgspec.structs.fields(TransferLedgerRow)`` -- the struct-typed
        write pattern #565 established for ``PersistedYoutubeRow``, so a
        payload field can never silently drift from the SQL (the
        ``album_title`` class of bug migration 036 fixed). A no-op on an
        empty list (nothing to enqueue -> nothing to ledger).
        """
        if not rows:
            return
        field_names = [f.name for f in msgspec.structs.fields(TransferLedgerRow)]
        col_sql = ", ".join(field_names)
        values = [
            tuple(getattr(row, name) for name in field_names)
            for row in rows
        ]
        self._ensure_conn()
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO slskd_transfer_ledger ({col_sql}) VALUES %s",
                values,
            )

    def stamp_transfer_completion(
        self,
        username: str,
        filename: str,
        local_path: str,
    ) -> int:
        """Persist an authoritative success path on the newest open enqueue.

        Completion events and write-ahead ownership share the durable
        ``(username, filename)`` identity. slskd transfer IDs are deliberately
        absent: the daemon re-issues them when a queued file retries.

        Returns 1 if an accepted row was stamped, 0 if no confirmed row
        matched (pending intent, an unledgered transfer, or replay). Events do
        not promote pending rows: a human same-key completion after a rejected
        Cratedigger POST must remain foreign.
        """
        cur = self._execute(
            """
            UPDATE slskd_transfer_ledger
            SET local_path = %s
            WHERE id = (
                SELECT id FROM slskd_transfer_ledger
                WHERE username = %s
                  AND filename = %s
                  AND accepted_at IS NOT NULL
                  AND local_path IS NULL
                ORDER BY enqueued_at DESC, id DESC
                LIMIT 1
            )
              AND accepted_at IS NOT NULL
              AND local_path IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM slskd_transfer_ledger
                  WHERE username = %s
                    AND filename = %s
                    AND local_path = %s
              )
            """,
            (
                local_path,
                username,
                filename,
                username,
                filename,
                local_path,
            ),
        )
        return cur.rowcount

    def confirm_transfer_enqueue(self, username: str, filename: str) -> int:
        """Confirm the newest pending write-ahead row after POST acceptance.

        A ledger insert precedes the network call and is only intent evidence.
        This T1.5 write is the only runtime signal that grants destructive
        authority. Completion events may add path evidence only after this
        confirmation; a same-key human event must never promote rejected
        intent.
        """
        cur = self._execute(
            """
            UPDATE slskd_transfer_ledger
            SET accepted_at = NOW()
            WHERE id = (
                SELECT id FROM slskd_transfer_ledger
                WHERE username = %s
                  AND filename = %s
                  AND accepted_at IS NULL
                ORDER BY enqueued_at DESC, id DESC
                LIMIT 1
            )
              AND accepted_at IS NULL
            """,
            (username, filename),
        )
        return cur.rowcount

    def get_owned_transfer_keys(self) -> set[tuple[str, str]]:
        """Every confirmed ``(username, filename)`` pair in the ledger -- the
        convergence flip's "is this live transfer mine?" membership set
        (#571 PR 3).

        Purpose-shaped: callers only need an unordered key set, so this
        queries only the two required columns and does not sort. Pending
        write-ahead intent is excluded; only an accepted POST proves
        Cratedigger created a transfer.
        """
        cur = self._execute(
            "SELECT username, filename FROM slskd_transfer_ledger "
            "WHERE accepted_at IS NOT NULL",
        )
        return {(r["username"], r["filename"]) for r in cur.fetchall()}

    def get_owned_local_paths(self) -> set[str]:
        """Every event-stamped ``local_path`` in the ledger -- the
        disk-reaper flip's (issue #571) "is this file mine?" set. Rows
        with no authoritative event path yet contribute nothing.
        """
        cur = self._execute(
            "SELECT local_path FROM slskd_transfer_ledger "
            "WHERE local_path IS NOT NULL",
        )
        return {r["local_path"] for r in cur.fetchall()}

    def get_owned_attempt_folders(self) -> list[dict[str, Any]]:
        """Every distinct accepted ``(request_id, attempt_fingerprint)``
        pair, joined to its request's artist/title/year identity -- the
        disk-reaper flip's (issue #571) "which canonical processing
        folders are mine?" lookup.

        The caller re-derives each folder with
        ``lib.processing_paths.canonical_processing_path`` from the
        returned ``artist_name``/``album_title``/``year``/
        ``attempt_fingerprint``. Current downloading rows instead flow
        through ``canonical_folder_for_row``, which delegates to that same
        formatter after deriving the fingerprint from persisted files. Thus
        a past attempt (imported, replaced, or
        reset-to-wanted-and-retried) whose row has since left
        ``downloading`` is STILL recognised as owned here, unlike the
        active-protection set which only tracks the row's CURRENT state.

        The ``JOIN`` to ``album_requests`` means a ``request_id`` whose
        row has been hard-deleted (the ledger's ``request_id`` carries
        no FK, migration 045) silently drops out -- conservative in the
        reap direction: the FOLDER stops being derivable as owned, but
        any individually completion-stamped file under it is still
        provable via ``get_owned_local_paths`` above, independent of
        this join.
        """
        cur = self._execute(
            """
            SELECT DISTINCT t.request_id, t.attempt_fingerprint,
                   r.artist_name, r.album_title, r.year
            FROM slskd_transfer_ledger t
            JOIN album_requests r ON r.id = t.request_id
            WHERE t.attempt_fingerprint IS NOT NULL
              AND t.accepted_at IS NOT NULL
            """,
        )
        return [dict(r) for r in cur.fetchall()]

    def prune_transfer_ledger(self, older_than: datetime) -> int:
        """Hard-delete rows strictly older than ``older_than`` (T3).

        Pending ``accepted_at IS NULL`` intent is pruned regardless of request
        status because it grants no ownership authority. Accepted evidence is
        kept while its request is ``wanted``/``downloading``; the reaper and
        convergence paths may still need it for an in-flight retry. Accepted
        rows for inactive or hard-deleted requests are pruned. The exact
        boundary survives because the comparison is strict. Returns the number
        of rows removed.
        """
        cur = self._execute(
            """
            DELETE FROM slskd_transfer_ledger t
            WHERE t.enqueued_at < %s
              AND (
                  t.accepted_at IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM album_requests r
                      WHERE r.id = t.request_id
                        AND r.status = ANY(%s)
                  )
              )
            """,
            (older_than, list(_ACTIVE_REQUEST_STATUSES)),
        )
        return cur.rowcount
