"""slskd transfer write-ahead ownership ledger (issue #571 good-citizen
doctrine, migration 045).

The methods this mixin adds:

* ``record_transfer_enqueue`` -- write-ahead batch INSERT, called BEFORE
  ``ctx.slskd.transfers.enqueue(...)`` (T1). This preserves intent across
  crashes without treating a rejected or ambiguous POST as ownership.
* ``confirm_transfer_enqueue`` -- accepted-POST write (T1.5), scoped to the
  request that made the POST. A write-ahead intent becomes destructive
  authority only after slskd accepts the POST, and only for its own request.
* ``stamp_transfer_completion`` -- event-ingestion success write: T2. Called
  from the SAME pass ``lib/slskd_events.py`` already stamps
  ``active_download_state`` from (issue #146), using the same durable
  ``(username, filename)`` key.
* ``get_owned_transfer_keys`` / ``get_owned_local_paths`` -- purpose-shaped
  read surfaces for transfer convergence and the disk reaper.
* ``prune_transfer_ledger`` -- T3: keeps pending intents bounded by age;
  accepted ownership evidence is also protected while its request remains
  active (``wanted``/``downloading``).

See migrations 045 and 051 for the schema and rationale.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import msgspec

from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import TransferLedgerRow, pg_execute_values

# Accepted rows for active requests cannot be pruned regardless of age: the
# reaper/convergence paths may still need their ownership evidence while the
# request is being retried. Pending intents have no ownership value and are
# bounded solely by the retention window. Accepted rows for everything else
# (imported, unsearchable, replaced, or a missing request) are also fair game
# once past that window.
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
            pg_execute_values(
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

    def confirm_transfer_enqueue(
        self, username: str, filename: str, *, request_id: int,
    ) -> int:
        """Confirm ``request_id``'s newest pending write-ahead row after
        POST acceptance.

        A ledger insert precedes the network call and is only intent evidence.
        This T1.5 write is the only runtime signal that grants destructive
        authority. Completion events may add path evidence only after this
        confirmation; a same-key human event must never promote rejected
        intent.

        ``request_id`` scopes the promotion to the request that actually
        made the POST (issue #1278 item 2). Two requests can hold pending
        intent on the SAME ``(username, filename)`` -- one peer serving one
        remote path for two pressings, or a Replace-lineage retry -- and
        this UPDATE used to take the newest pending row for the key across
        the WHOLE table. That made one request's acceptance capable of
        promoting another's rejected or still-in-flight intent into
        destructive ownership it never earned. It mirrors
        ``_write_ahead_transfer_ledger``'s own scoping exactly: that INSERT
        skips entirely without a ``request_id``, so there is no unscoped
        row for an unscoped confirm to find.
        """
        cur = self._execute(
            """
            UPDATE slskd_transfer_ledger
            SET accepted_at = NOW()
            WHERE id = (
                SELECT id FROM slskd_transfer_ledger
                WHERE username = %s
                  AND filename = %s
                  AND request_id = %s
                  AND accepted_at IS NULL
                ORDER BY enqueued_at DESC, id DESC
                LIMIT 1
            )
              AND accepted_at IS NULL
            """,
            (username, filename, request_id),
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

    def get_owned_transfer_keys_for(
        self, keys: Sequence[tuple[str, str]],
    ) -> set[tuple[str, str]]:
        """``get_owned_transfer_keys`` restricted to ``keys``.

        Same ownership question, same ``accepted_at IS NOT NULL``
        answer -- asked about a handful of specific queue keys instead of
        the whole ledger. ``converge_slskd_orphans`` legitimately wants
        the whole set once per cycle; the destructive paths in
        ``lib/slskd_transfers.py`` ask about one attempt's files and run
        on find_download worker threads, where pulling all 33k accepted
        keys per call to answer a five-key question is the wrong shape.

        Keys are passed as two parallel arrays zipped server-side by
        ``unnest(...)``, the same fixed-shape static SQL
        ``get_conflicting_transfer_request_ids`` uses -- never an
        f-string-assembled WHERE clause that grows with the input.

        ``DISTINCT`` is a wire-size optimisation, not a correctness
        requirement: one queue key retried across attempts holds many
        accepted rows, and the set comprehension below would collapse
        them regardless. Removing it changes no answer.
        """
        if not keys:
            return set()
        usernames = [key[0] for key in keys]
        filenames = [key[1] for key in keys]
        cur = self._execute(
            """
            SELECT DISTINCT l.username, l.filename
            FROM slskd_transfer_ledger l
            JOIN unnest(%s::text[], %s::text[]) AS k(username, filename)
              ON l.username = k.username AND l.filename = k.filename
            WHERE l.accepted_at IS NOT NULL
            """,
            (usernames, filenames),
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

    def get_abandoned_owned_local_paths(self) -> set[str]:
        """Owned ``local_path`` values whose attempt is provably abandoned.

        A request sitting at ``wanted`` with no ``active_download_state``
        holds no reference to any file: its attempt ended, and the next
        cycle re-derives from scratch. The files that attempt completed are
        still ledger-owned, so the reaper may take them — but under
        ``ORPHAN_MIN_AGE_DAYS`` it would wait seven days, while the retry
        arrives in minutes and re-downloads to a byte-identical path. slskd
        then resolves that collision by appending to the name, which for a
        cap-length name overflows and strands the transfer.

        Deliberately keyed on ``wanted`` alone. ``downloading`` is already
        covered by the active-protection half of the model, and
        ``processing`` must never enter a reaper status set
        (``.claude/rules/pipeline-db.md``) — materialization holds open
        descriptors on exactly these files.
        """
        cur = self._execute(
            "SELECT l.local_path "
            "FROM slskd_transfer_ledger AS l "
            "JOIN album_requests AS r ON r.id = l.request_id "
            "WHERE l.local_path IS NOT NULL "
            "AND r.status = 'wanted' "
            "AND r.active_download_state IS NULL",
        )
        return {r["local_path"] for r in cur.fetchall()}

    def get_conflicting_transfer_request_ids(
        self,
        keys: Sequence[tuple[str, str]],
        exclude_request_id: int,
    ) -> set[int]:
        """Cross-cycle half of the #1178 cross-request enqueue guard.

        Returns every OTHER request id that already holds ACCEPTED
        ownership (``accepted_at IS NOT NULL``) of any ``(username,
        filename)`` pair in ``keys``, while that owner's request is
        CURRENTLY ``downloading`` on its CURRENT attempt -- never any
        other status, and never a stale abandoned attempt.

        The status filter is deliberately the single value
        ``'downloading'``, never ``'processing'``
        (``.claude/rules/pipeline-db.md`` / CLAUDE.md critical invariant
        10: never add ``processing`` to any slskd event/transfer/ledger/
        reaper status set). A sibling request can still be mid-
        ``processing`` on the SAME queue keys at the moment this method
        is consulted -- that residual collision window is deliberate and
        is left to the requeue self-heal (#898), not to this guard. A
        ``'replaced'`` owner (Replace-lineage attempt sharing, e.g.
        requests 8781/8846) or a ``'wanted'``/``'imported'`` owner
        (already moved on) must NEVER block.

        **Attempt scoping (#1178 PR2 review F2).** A ``'downloading'``
        request can carry MANY historical accepted ledger rows: live-DB
        measurement found 80.3% of accepted rows belong to a
        non-current attempt, up to 76 distinct accepted attempt
        fingerprints for one request. Without scoping, an owner actively
        downloading a fresh peer's files would falsely block a sibling
        on a queue key from that SAME owner's OWN abandoned attempt from
        30 days earlier. The join is therefore additionally scoped to
        the owner's CURRENT attempt only, by exact fingerprint identity
        (detailed below).

        **Attempt identity, not a clock comparison (#1196 item 1).**
        ``ActiveDownloadState.attempt_fingerprint`` (written at claim time
        by ``lib.download.build_active_download_state``) carries the exact
        same ``lib.processing_paths.attempt_fingerprint`` value this
        ledger's own ``attempt_fingerprint`` column carries for every row
        this attempt writes -- both derived from the identical
        (username, filename) pair set, so they agree BY CONSTRUCTION (see
        ``build_active_download_state``'s docstring). When the owner's
        state carries that key, this join therefore requires EXACT
        fingerprint equality (``l.attempt_fingerprint = r.
        active_download_state ->> 'attempt_fingerprint'``) instead of a
        clock comparison -- no app-clock-vs-PG-clock assumption, no skew
        window, no failure direction to reason about: a stale attempt's
        ledger rows simply carry a different fingerprint and never match,
        however new their ``enqueued_at`` looks.

        **Fail closed on a missing fingerprint (#1199 item 2).** A prior
        version of this query fell back to a clock comparison
        (``l.enqueued_at >= COALESCE(..., '-infinity')``) when the owner's
        state existed but LACKED the ``attempt_fingerprint`` key -- a
        deploy-window accommodation for an in-flight download claimed by
        code from before that field existed. Live measurement on
        2026-08-19 found the cohort empty (1 ``downloading`` request, 0
        NULL states, 0 lacking ``attempt_fingerprint``), so the fallback
        arm is dead code and is deleted per the no-deprecated-helpers rule.
        The ``ELSE`` arm is now unconditional ``TRUE``: a NULL, malformed
        (``->>`` returns NULL for a non-object jsonb value regardless of
        key), or fingerprint-less ``active_download_state`` fails CLOSED
        -- every accepted row for that ``'downloading'`` owner counts as
        in-scope (blocks), with no attempt-boundary rescue and no clock
        involved at all. Only an owner whose state DOES carry
        ``attempt_fingerprint`` gets attempt-scoped blocking; the equality
        test itself is unchanged from #1196.

        A live ``'downloading'`` owner reaching this ELSE arm today is
        possible only via a NULL/malformed state or a pre-#1196 historical
        row -- ``build_active_download_state`` sets ``attempt_fingerprint``
        to ``None`` ONLY when ``entry.files`` is empty (issue #1199 review
        F9), and every enqueue persist site in ``lib/enqueue.py`` guards
        ``files_to_enqueue``/``planned_files`` non-empty (``if not
        files_to_enqueue: ... continue``) before that state is ever built,
        so a future change that lets an empty-files claim through would
        silently make THIS owner's every accepted key block, not just its
        current attempt's.
        """
        if not keys:
            return set()
        # A fixed-shape static SQL string, never an f-string-assembled
        # WHERE clause: the key set is passed as two parallel arrays and
        # zipped server-side via unnest(...) rather than growing the SQL
        # text with the input size. This keeps the query a single
        # constant string the repository's static SQL-write audit
        # (tests/test_replaced_write_audit.py) can fully resolve, and
        # it's the one JOIN this method makes to album_requests -- never
        # an UPDATE.
        usernames = [key[0] for key in keys]
        filenames = [key[1] for key in keys]
        cur = self._execute(
            """
            SELECT DISTINCT l.request_id
            FROM slskd_transfer_ledger l
            JOIN album_requests r ON r.id = l.request_id
            JOIN unnest(%s::text[], %s::text[]) AS k(username, filename)
              ON l.username = k.username AND l.filename = k.filename
            WHERE l.accepted_at IS NOT NULL
              AND r.status = 'downloading'
              AND l.request_id != %s
              AND (
                  CASE
                      WHEN r.active_download_state ->> 'attempt_fingerprint'
                          IS NOT NULL THEN
                          l.attempt_fingerprint =
                              r.active_download_state ->> 'attempt_fingerprint'
                      ELSE TRUE
                  END
              )
            """,
            (usernames, filenames, exclude_request_id),
        )
        return {r["request_id"] for r in cur.fetchall()}

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
