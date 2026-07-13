"""slskd transfer write-ahead ownership ledger (issue #571 good-citizen
doctrine, migration 045).

The methods this mixin adds:

* ``record_transfer_enqueue`` -- write-ahead batch INSERT, called BEFORE
  ``ctx.slskd.transfers.enqueue(...)`` (T1). This is what makes the
  future reaper/convergence flips able to prove ownership: a process
  death at ANY point after the POST still leaves a durable row.
* ``stamp_transfer_id`` -- enqueue-response write (T1.5, issue #571 PR 5):
  called right after ``slskd_enqueue_with_outcome`` reconciles a POST's
  transfer id, stamping it onto the row T1 just inserted.
* ``stamp_transfer_completion`` -- event-ingestion success write: T2. Called
  from the SAME pass ``lib/slskd_events.py`` already stamps
  ``active_download_state`` from (issue #146). The event's exact transfer ID
  upgrades a prior pathless failure stamp; only when that ID is globally
  absent may the exact-key fallback bind it to an open row.
* ``stamp_terminal_failures`` / ``claim_terminal_failures`` -- end-of-cycle
  terminal stamps for non-success ``Completed,*`` snapshots. Exact-ID rows
  stamp in bulk; a failure that raced T1.5 may claim one causal, exact-key,
  ID-less T1 row. Both return only IDs whose durable write succeeded.
* ``get_owned_transfer_keys`` / ``get_owned_transfer_id_sets`` /
  ``get_owned_local_paths`` -- purpose-shaped read surfaces for the
  reaper/convergence/purge flips: the bare "is this (username, filename)
  mine?" membership set the #571 PR 3 convergence flip consumes each
  cycle, the path-stamped/pathless/unstamped ``transfer_id`` sets the
  completed-transfer purge consumes each cycle, and "is this local_path
  mine?".
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
from lib.pipeline_db._shared import (
    TERMINAL_FAILURE_CLAIM_MAX_SKEW,
    TerminalFailureClaim,
    TransferIdOwnership,
    TransferLedgerRow,
)

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

    def stamp_transfer_id(
        self,
        username: str,
        filename: str,
        transfer_id: str,
    ) -> int:
        """Enqueue-response write (T1.5, issue #571 PR 5): stamp
        ``transfer_id`` onto the newest not-yet-id-stamped ledger row for
        ``(username, filename)``.

        Called right after ``slskd_enqueue_with_outcome`` reconciles a
        POST's accepted files against a fresh downloads snapshot -- the
        SAME (username, filename) key ``stamp_transfer_completion``
        matches on. Tie-break mirrors that method's: newest row with
        ``transfer_id IS NULL`` wins, so a retried file (T1 mints a fresh
        row) always gets the id captured for THIS attempt, never an
        older still-open one.

        Returns 1 if a row was stamped, 0 if no ledgered row matched (an
        unledgered/foreign transfer, a replay, or every matching row already
        has a transfer ID) -- never raises for a miss. Migration 049's global
        unique index is the concurrency backstop: an ID can authorize exactly
        one ledger attempt even when writers race.
        """
        try:
            cur = self._execute(
                """
                UPDATE slskd_transfer_ledger
                SET transfer_id = %s
                WHERE id = (
                    SELECT id FROM slskd_transfer_ledger
                    WHERE username = %s
                      AND filename = %s
                      AND transfer_id IS NULL
                      AND completed_at IS NULL
                    ORDER BY enqueued_at DESC
                    LIMIT 1
                )
                  AND NOT EXISTS (
                      SELECT 1 FROM slskd_transfer_ledger
                      WHERE transfer_id = %s
                  )
                """,
                (transfer_id, username, filename, transfer_id),
            )
        except psycopg2.errors.UniqueViolation:
            return 0
        return cur.rowcount

    def stamp_transfer_completion(
        self,
        username: str,
        filename: str,
        local_path: str,
        completed_at: datetime,
        *,
        transfer_id: str,
    ) -> int:
        """Persist an authoritative success event by exact transfer ID.

        An exact-ID row wins even when it already has a pathless failure
        stamp; the success event upgrades it with ``local_path``. If T1.5
        missed the ID entirely, the newest open exact-key row is used only
        when that ID is absent globally. Replays against an already path-
        stamped row are no-ops, and the unique transfer-ID boundary makes
        concurrent/retried writes fail closed without raising.

        Returns 1 if a row was stamped, 0 if no ledgered row matched
        (an unledgered/foreign transfer, or every matching row was
        already stamped) -- never raises for a miss.
        """
        try:
            cur = self._execute(
                """
                WITH target AS (
                    SELECT id, 0 AS priority, enqueued_at
                    FROM slskd_transfer_ledger
                    WHERE transfer_id = %s
                    UNION ALL
                    SELECT id, 1 AS priority, enqueued_at
                    FROM slskd_transfer_ledger
                    WHERE username = %s
                      AND filename = %s
                      AND transfer_id IS NULL
                      AND completed_at IS NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM slskd_transfer_ledger
                          WHERE transfer_id = %s
                      )
                    ORDER BY priority, enqueued_at DESC
                    LIMIT 1
                )
                UPDATE slskd_transfer_ledger
                SET local_path = %s,
                    completed_at = %s,
                    transfer_id = COALESCE(transfer_id, %s)
                WHERE id = (SELECT id FROM target)
                  AND local_path IS NULL
                """,
                (
                    transfer_id, username, filename, transfer_id,
                    local_path, completed_at, transfer_id,
                ),
            )
        except psycopg2.errors.UniqueViolation:
            return 0
        return cur.rowcount

    def get_owned_transfer_keys(self) -> set[tuple[str, str]]:
        """Every ``(username, filename)`` pair in the ledger -- the
        convergence flip's "is this live transfer mine?" membership set
        (#571 PR 3).

        Purpose-shaped: callers only need an unordered key set, so this
        queries only the two required columns and does not sort. Includes
        stamped and unstamped rows alike -- ledger membership, not
        completion state, is what proves cratedigger created a transfer.
        """
        cur = self._execute(
            "SELECT username, filename FROM slskd_transfer_ledger",
        )
        return {(r["username"], r["filename"]) for r in cur.fetchall()}

    def get_owned_transfer_id_sets(self) -> TransferIdOwnership:
        """Ledger IDs split into success-ready, failure-only, and open sets.

        A ``local_path`` is authoritative success evidence. ``completed_at``
        without a path records only a terminal failure observation and must
        never authorize removal of a later success snapshot with the same ID.
        The completed-transfer purge consumes all three sets in one query.

        Purpose-shaped like ``get_owned_transfer_keys``: only rows with a
        known ``transfer_id`` are relevant (a row still awaiting BOTH
        T1.5 and T2 contributes nothing to either set -- correctly so,
        since the purge matches live transfers by id).
        """
        cur = self._execute(
            "SELECT transfer_id, completed_at, local_path "
            "FROM slskd_transfer_ledger "
            "WHERE transfer_id IS NOT NULL",
        )
        path_stamped: set[str] = set()
        pathless_stamped: set[str] = set()
        unstamped: set[str] = set()
        for row in cur.fetchall():
            if row["local_path"] is not None:
                target = path_stamped
            elif row["completed_at"] is not None:
                target = pathless_stamped
            else:
                target = unstamped
            target.add(row["transfer_id"])
        return TransferIdOwnership(
            path_stamped=path_stamped,
            pathless_stamped=pathless_stamped,
            unstamped=unstamped,
        )

    def stamp_terminal_failures(
        self,
        transfer_ids: set[str],
        observed_at: datetime,
    ) -> set[str]:
        """Stamp exact-ID owned terminal failures and return confirmed IDs.

        Only still-open ledger rows can transition.  The returned IDs are
        the authorization boundary for the caller's per-ID slskd removal:
        absent/foreign IDs and already-stamped rows never appear.
        """
        if not transfer_ids:
            return set()
        cur = self._execute(
            """
            UPDATE slskd_transfer_ledger
            SET completed_at = %s
            WHERE completed_at IS NULL
              AND transfer_id = ANY(%s)
            RETURNING transfer_id
            """,
            (observed_at, sorted(transfer_ids)),
        )
        return {row["transfer_id"] for row in cur.fetchall()}

    def claim_terminal_failures(
        self,
        claims: list[TerminalFailureClaim],
        observed_at: datetime,
    ) -> set[str]:
        """Atomically claim causal ID-less T1 rows for terminal failures.

        Each claim consumes at most one exact ``(username, filename)``
        ledger row written within five minutes before slskd's request.
        Claims run oldest-first so duplicate retry keys bind one-to-one in
        lifecycle order.  Successes never call this method.
        """
        confirmed: set[str] = set()
        ordered = sorted(
            claims,
            key=lambda claim: (claim.requested_at, claim.transfer_id),
        )
        for claim in ordered:
            if claim.transfer_id in confirmed:
                continue
            try:
                cur = self._execute(
                    """
                    UPDATE slskd_transfer_ledger
                    SET transfer_id = %s, completed_at = %s
                    WHERE id = (
                        SELECT id FROM slskd_transfer_ledger
                        WHERE username = %s
                          AND filename = %s
                          AND transfer_id IS NULL
                          AND completed_at IS NULL
                          AND enqueued_at >= %s
                          AND enqueued_at <= %s
                        ORDER BY enqueued_at DESC, id DESC
                        LIMIT 1
                        FOR UPDATE
                    )
                      AND NOT EXISTS (
                          SELECT 1 FROM slskd_transfer_ledger
                          WHERE transfer_id = %s
                      )
                    RETURNING transfer_id
                    """,
                    (
                        claim.transfer_id,
                        observed_at,
                        claim.username,
                        claim.filename,
                        claim.requested_at - TERMINAL_FAILURE_CLAIM_MAX_SKEW,
                        claim.requested_at,
                        claim.transfer_id,
                    ),
                )
            except psycopg2.errors.UniqueViolation:
                continue
            row = cur.fetchone()
            if row is not None:
                confirmed.add(row["transfer_id"])
        return confirmed

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
