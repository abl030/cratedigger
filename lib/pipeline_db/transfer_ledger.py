"""slskd transfer write-ahead ownership ledger (issue #571 good-citizen
doctrine, migration 045).

The seven methods this mixin adds:

* ``record_transfer_enqueue`` -- write-ahead batch INSERT, called BEFORE
  ``ctx.slskd.transfers.enqueue(...)`` (T1). This is what makes the
  future reaper/convergence flips able to prove ownership: a process
  death at ANY point after the POST still leaves a durable row.
* ``stamp_transfer_completion`` -- event-ingestion write: T2. Called from
  the SAME pass ``lib/slskd_events.py`` already stamps
  ``active_download_state`` from (issue #146), matching ledger rows by
  the same (username, remote filename) key.
* ``get_owned_transfers`` / ``get_owned_transfer_keys`` /
  ``get_owned_local_paths`` -- read surface shaped for what the
  reaper/convergence flips need: full rows for forensic inspection, the
  bare "is this (username, filename) mine?" membership set the #571 PR 3
  convergence flip consumes each cycle, and "is this local_path mine?".
* ``get_owned_attempt_folders`` -- read surface for the disk-reaper
  flip (issue #571 PR 4): "which canonical processing folders are
  mine?", joined to each ledgered attempt's request identity so the
  caller can re-derive the folder with
  ``lib.processing_paths.canonical_processing_path``.
* ``prune_transfer_ledger`` -- T3: keeps the table bounded by
  hard-deleting rows that are both old AND whose request is no longer
  active (``wanted``/``downloading``).

See migration 045 for the schema and rationale.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import msgspec
import psycopg2.extras

from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import TransferLedgerRow

# Requests still active (in-flight) can't be pruned regardless of age --
# a future reaper/convergence flip may still need the ledger row while
# the request is being retried. Everything else (imported, manual,
# replaced, or a request_id whose row no longer exists) is fair game
# once past the retention window.
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
        completed_at: datetime,
    ) -> int:
        """Event-ingestion write (T2): stamp ``local_path``/``completed_at``
        onto the newest not-yet-stamped ledger row for
        ``(username, filename)``.

        Most-recent-attempt tie-break: among rows still open
        (``completed_at IS NULL``) for this key, the one with the latest
        ``enqueued_at`` wins -- a retried file mints a fresh ledger row
        (T1), so the retry's row is the one the completion event belongs
        to. Gating on ``completed_at IS NULL`` also makes re-processing
        the same event idempotent: once a row is stamped it drops out of
        the candidate set, so replaying the event finds nothing left to
        stamp for that key (unless a NEWER un-stamped attempt exists, in
        which case that is the correct row to stamp).

        Returns 1 if a row was stamped, 0 if no ledgered row matched
        (an unledgered/foreign transfer, or every matching row was
        already stamped) -- never raises for a miss.
        """
        cur = self._execute(
            """
            UPDATE slskd_transfer_ledger
            SET local_path = %s, completed_at = %s
            WHERE id = (
                SELECT id FROM slskd_transfer_ledger
                WHERE username = %s AND filename = %s AND completed_at IS NULL
                ORDER BY enqueued_at DESC
                LIMIT 1
            )
            """,
            (local_path, completed_at, username, filename),
        )
        return cur.rowcount

    def get_owned_transfers(
        self, request_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Ledger rows, optionally filtered to one ``request_id``.

        Shaped for the future convergence flip's "is this transfer
        mine?" lookup: every row exposes the full column set so the
        caller can match by (username, filename) or inspect
        attempt_fingerprint/completion state.
        """
        if request_id is not None:
            cur = self._execute(
                """
                SELECT id, request_id, username, filename, transfer_id,
                       attempt_fingerprint, enqueued_at, local_path, completed_at
                FROM slskd_transfer_ledger
                WHERE request_id = %s
                ORDER BY enqueued_at ASC
                """,
                (request_id,),
            )
        else:
            cur = self._execute(
                """
                SELECT id, request_id, username, filename, transfer_id,
                       attempt_fingerprint, enqueued_at, local_path, completed_at
                FROM slskd_transfer_ledger
                ORDER BY enqueued_at ASC
                """,
            )
        return [dict(r) for r in cur.fetchall()]

    def get_owned_transfer_keys(self) -> set[tuple[str, str]]:
        """Every ``(username, filename)`` pair in the ledger -- the
        convergence flip's "is this live transfer mine?" membership set
        (#571 PR 3).

        Purpose-shaped: callers only need an unordered key set, so this
        skips ``get_owned_transfers``'s 9-column projection and ``ORDER
        BY enqueued_at`` (wasteful every 5-min cycle). Includes stamped
        and unstamped rows alike -- ledger membership, not completion
        state, is what proves cratedigger created a transfer.
        """
        cur = self._execute(
            "SELECT username, filename FROM slskd_transfer_ledger",
        )
        return {(r["username"], r["filename"]) for r in cur.fetchall()}

    def get_owned_local_paths(self) -> set[str]:
        """Every completion-stamped ``local_path`` in the ledger -- the
        disk-reaper flip's (issue #571) "is this file mine?" set. Rows
        with no completion stamp yet contribute nothing.
        """
        cur = self._execute(
            "SELECT local_path FROM slskd_transfer_ledger "
            "WHERE local_path IS NOT NULL",
        )
        return {r["local_path"] for r in cur.fetchall()}

    def get_owned_attempt_folders(self) -> list[dict[str, Any]]:
        """Every distinct ledgered ``(request_id, attempt_fingerprint)``
        pair, joined to its request's artist/title/year identity -- the
        disk-reaper flip's (issue #571) "which canonical processing
        folders are mine?" lookup.

        The caller re-derives each folder with
        ``lib.processing_paths.canonical_processing_path`` from the
        returned ``artist_name``/``album_title``/``year``/
        ``attempt_fingerprint`` -- the SAME leaf function
        ``_protected_paths_for_downloading`` uses for a currently
        ``downloading`` row, so a past attempt (imported, replaced, or
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
            """,
        )
        return [dict(r) for r in cur.fetchall()]

    def prune_transfer_ledger(self, older_than: datetime) -> int:
        """Hard-delete rows older than ``older_than`` whose request is
        NOT currently active (T3).

        A row is kept regardless of age while its request is
        ``wanted``/``downloading`` -- the future reaper/convergence flip
        may still need it for an in-flight retry. A request that no
        longer exists (hard-deleted elsewhere) is treated as inactive --
        it can never come back to wanted/downloading. Returns the number
        of rows removed.
        """
        cur = self._execute(
            """
            DELETE FROM slskd_transfer_ledger t
            WHERE t.enqueued_at < %s
              AND NOT EXISTS (
                  SELECT 1 FROM album_requests r
                  WHERE r.id = t.request_id
                    AND r.status = ANY(%s)
              )
            """,
            (older_than, list(_ACTIVE_REQUEST_STATUSES)),
        )
        return cur.rowcount
