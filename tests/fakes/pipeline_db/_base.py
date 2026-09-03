"""Shared state and cross-cluster contract for the FakePipelineDB clusters."""
from __future__ import annotations

import copy
from collections.abc import (
    Callable,
    Mapping,
    Sequence,
)
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
)

if TYPE_CHECKING:
    from lib.convergence_service import (
        ConvergenceSignal,
    )
    from lib.pipeline_db import AlbumRequestRow
from lib.import_execution import (
    CancellationToken,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_ACTIVE_STATUSES,
    IMPORT_JOB_AUTOMATION,
    ImportJob,
)
from lib.pipeline_db import (
    BadAudioHashRow,
    CleanupJournalConflict,
    CleanupJournalIntent,
    CleanupJournalReceipt,
    ProcessingCleanupJournalRow,
    TransferLedgerRow,
    UnfindableRunMetricsRow,
)
from lib.pipeline_db._shared import CANDIDATE_EVIDENCE_PREFIX
from lib.quality import (
    AlbumQualityEvidence,
)
from lib.terminal_outcomes import (
    ImportTerminalOutcome,
    PreviewTerminalOutcome,
    RequestRejectionOutcome,
    TerminalOutcomeResult,
)
from lib.validation_envelope import (
    VALIDATION_PROJECTION_UNSET,
    ValidationProjectionUnset,
)
from tests.fakes._shared import _utcnow
from tests.fakes.cursors import FakeCursor
from tests.fakes.rows import (
    DenylistEntry,
    DownloadLogRow,
    FakeTransferLedgerRow,
    FieldResolutionRow,
    SearchLedgerRow,
    SearchLogRow,
    UserCooldownRow,
)

if TYPE_CHECKING:
    from tests.fakes.pipeline_db.search_plan import (
        _FakeSearchPlanItemRow,
        _FakeSearchPlanRow,
    )


#: The ``request_search_summary`` view's own window
#: (``migrations/031_request_search_summary_view.sql``). Every consumer of
#: that view — the triage rollup and the ``search_not_converting`` page
#: join — sees only search_log rows this recent, so the mirror must too.
_SEARCH_SUMMARY_WINDOW = timedelta(days=14)


class _FakePipelineDBBase:
    """State every FakePipelineDB cluster mixin shares.

    Mirrors ``lib/pipeline_db/_core.py::_PipelineDBBase``. ``__init__``
    owns every in-memory table; the helpers below it are the private ones
    two or more clusters call; and the stub block at the bottom declares
    the cross-cluster surface a mixin needs to type-check without
    importing the composed class. Those stub bodies never execute, since
    the composed ``FakePipelineDB`` MRO resolves each call to the owning
    mixin. The stub block's own comment states which of the two shapes
    each stub is there for.
    """

    def __init__(self, *, dsn: str = "postgresql://fake") -> None:
        self.dsn = dsn
        self._requests: dict[int, dict[str, Any]] = {}
        self._tracks: dict[int, list[dict[str, Any]]] = {}
        self.download_logs: list[DownloadLogRow] = []
        self._import_jobs: list[dict[str, Any]] = []
        self._processing_cleanup_journals: dict[
            tuple[int, int],
            ProcessingCleanupJournalRow,
        ] = {}
        self.search_logs: list[SearchLogRow] = []
        self.cycle_metrics: list[dict[str, Any]] = []
        self.unfindable_run_metrics: list[UnfindableRunMetricsRow] = []
        # Distinct-peer roster mirroring `peer_observations` (#227).
        # Keyed by username_hash.
        self.peer_observations: dict[str, dict[str, Any]] = {}
        self.user_cooldowns: dict[str, UserCooldownRow] = {}
        self._slskd_event_cursor: dict[str, Any] | None = None
        # slskd search-id write-ahead ledger (migration 044, issue #576).
        # Keyed by search_id, mirroring the real PRIMARY KEY / ON CONFLICT
        # DO NOTHING semantics — see ``record_search_id``.
        self._search_ledger: dict[str, SearchLedgerRow] = {}
        self.record_search_id_calls: list[SearchLedgerRow] = []
        # slskd transfer write-ahead ownership ledger (migration 045,
        # issue #571). Keyed by an auto-incrementing fake id, mirroring
        # the real BIGSERIAL primary key.
        self._transfer_ledger: dict[int, FakeTransferLedgerRow] = {}
        self._transfer_ledger_next_id: int = 1
        self.record_transfer_enqueue_calls: list[TransferLedgerRow] = []
        #: Every ``confirm_transfer_enqueue`` call, as
        #: ``(username, filename, request_id)``. The write-ahead ledger's
        #: T1.5 half is otherwise invisible when it legitimately promotes
        #: nothing, so a test that pins "this path must not even ASK"
        #: (#1278 item 2's ``request_id is None`` skip) has no observable
        #: state to assert on without it.
        self.confirm_transfer_enqueue_calls: list[tuple[str, str, int]] = []
        self.denylist: list[DenylistEntry] = []
        self.persist_import_terminal_outcome_calls: list[ImportTerminalOutcome] = []
        self.persist_preview_terminal_outcome_calls: list[PreviewTerminalOutcome] = []
        self.persist_request_rejection_outcome_calls: list[RequestRejectionOutcome] = []
        self.bad_audio_hashes: list[BadAudioHashRow] = []
        # Call-count tracking for the bad-audio-hash gate. Tests that
        # used to assert ``mock.assert_called_once()`` / ``assert_not_called()``
        # on the MagicMock-source can now inspect these instead.
        self.has_any_bad_audio_hashes_calls: int = 0
        self.lookup_bad_audio_hash_calls: list[tuple[bytes, str]] = []
        # Track callers that clear the request's installed-quality pointers.
        self.clear_on_disk_quality_fields_calls: list[int] = []
        # ``close()`` call count — pipeline_cli main() must close the
        # DB exactly once per invocation, regardless of subcommand exit
        # code. Tracked here so tests can assert the contract.
        self.close_calls: int = 0
        # ``update_request_fields`` is the catch-all for set-intent CLI
        # commands. Track the (request_id, fields_dict) tuples so tests
        # can assert what was written without relying on MagicMock
        # introspection.
        self.update_request_fields_calls: list[tuple[int, dict[str, Any]]] = []
        # The MusicBrainz merge rekey (#1059) — (request_id, old, new, job).
        # Recorded so ordering tests can prove the row never moves before the
        # library retag reached a ready outcome. ``job`` is ``None`` for the
        # operator merge-rekey arm (#1089), which holds no import claim.
        self.update_request_release_for_merge_calls: list[
            tuple[int, str, str, int | None]] = []
        # U13 unfindable detection writers. The R20 runtime guard
        # asserts these recorders fire while the cursor-mutation
        # recorders (``record_consumed_search_attempt_calls``,
        # ``advance_search_plan_cursor_calls``) stay empty.
        self.record_artist_probe_calls: list[
            tuple[int, int, datetime]] = []
        self.set_unfindable_category_calls: list[
            tuple[int, str | None, datetime]] = []
        # Cursor-mutation recorders. The R20 runtime guard asserts
        # these stay empty after a detection run. We instrument both
        # cursor writers and the operator-driven advance.
        self.record_consumed_search_attempt_calls: list[Any] = []
        self.record_non_consuming_search_attempt_calls: list[Any] = []
        self.advance_search_plan_cursor_calls: list[
            tuple[int, int, int]] = []
        # Keyed by (mb_release_id, snapshot_fingerprint) — content-addressed
        # after migration 021. Each row also has a surrogate ``id``; the
        # parallel ``_evidence_by_id`` dict mirrors load-by-id lookups.
        self.album_quality_evidence: dict[
            tuple[str, str], AlbumQualityEvidence,
        ] = {}
        self._evidence_by_id: dict[int, AlbumQualityEvidence] = {}
        self._next_evidence_id = 0
        self._next_bad_audio_hash_id = 0
        self.cooldowns_applied: list[str] = []
        # Migration 030 — album_request_field_resolutions. Keyed by
        # (request_id, field_name); on conflict, attempts increments
        # and resolved_at updates, mirroring the production UPSERT.
        self.field_resolutions: dict[
            tuple[int, str], FieldResolutionRow,
        ] = {}
        self._next_field_resolution_id = 0
        self.recorded_attempts: list[tuple[int, str]] = []
        self.status_history: list[tuple[int, str]] = []
        self.update_download_state_calls: list[tuple[int, str]] = []
        self.advisory_lock_calls: list[tuple[int, int]] = []
        self.closed = False
        self._owner_session_pin: tuple[
            OwnerSessionIdentity,
            CancellationToken,
        ] | None = None
        self._next_request_id = 0
        self._next_download_log_id = 0
        self._next_import_job_id = 0
        self._next_search_log_id = 0
        self._cooldown_result: bool | Callable[[str], bool] = False
        self._advisory_lock_result: (
            bool | Callable[[int, int], bool]) = True
        # Deterministic preflight-vs-in-lock request-creation race. The first
        # identity lookup sees no row; the second materializes the competing
        # row that won before RequestCreationService acquired RELEASE.
        self._request_creation_race: tuple[str, str, bool, bool] | None = None
        self._request_creation_race_lookups = 0
        # Per-request failure injection for the active_download_state
        # writer (issue #564 review): ``set_update_download_state_error``
        # makes the witnessed ``update_download_state_if_downloading`` raise
        # for one
        # request id — simulating a psycopg2 error at the UPDATE — so
        # per-row error-isolation contracts can be pinned.
        self._update_download_state_errors: dict[int, Exception] = {}
        # U1 persisted-search-plans state.
        self.search_plans: dict[int, _FakeSearchPlanRow] = {}
        self.search_plan_items: dict[int, _FakeSearchPlanItemRow] = {}
        self._next_search_plan_id = 0
        self._next_search_plan_item_id = 0
        # Migration 040 — Plex addedAt pin store. Rows mirror the production
        # column shape (status 'pending'|'done'|'skipped'); ids assigned
        # monotonically like the other fakes.
        self.plex_added_at_pins: list[dict[str, Any]] = []
        self._next_plex_pin_id = 0
        # Migration 046 — Jellyfin DateCreated pin store. Rows mirror the
        # production column shape (status 'pending'|'done'|'skipped'|
        # 'expired'); ids assigned monotonically like the other fakes.
        self.jellyfin_date_created_pins: list[dict[str, Any]] = []
        self._next_jellyfin_pin_id = 0
        # ``_execute`` stubbing for tests that drive raw-SQL CLI paths
        # (``pipeline-cli query``, ``pipeline-cli repair-spectral``, ...).
        # ``queue_execute_results`` lets tests register a deterministic
        # cursor sequence; each ``_execute`` call pops the next entry,
        # raising it if it is an ``Exception`` and otherwise returning
        # it as the cursor. ``execute_calls`` records the (sql, params)
        # arguments for assertion.
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
        self._execute_queue: list[Any] = []
        # Production ``_execute`` always returns a cursor — an unqueued
        # call degrades to "query ran, zero rows" instead of a None
        # that detonates as AttributeError at the caller's fetchall().
        self._execute_default: Any = FakeCursor()
        # U15 triage N+1 guard: every triage-bound bulk getter increments
        # its counter exactly once per call. ``list_triage`` is bounded
        # to four entries (one page + three bulk getters) regardless of
        # page size; the test asserts ``sum(query_counts.values()) <= 5``
        # (extra headroom for the per-request compose path's request
        # fetch).
        self.query_counts: dict[str, int] = {}
        self.convergence_signals: dict[int, ConvergenceSignal] = {}
        # Migration 034 — youtube_album_mappings. Keyed by
        # (release_group_identifier, source); each value is the full
        # matrix the resolver scored for that pair. Refresh always
        # replaces the whole list (no partial updates per R14).
        self._youtube_album_mappings: dict[
            tuple[str, str], list[dict[str, Any]],
        ] = {}
        self._next_youtube_mapping_id = 0

    def seed_request(self, row: Mapping[str, Any]) -> None:
        """Add a request row to the fake DB. Must include 'id'.

        Re-seeding an existing id replaces that row (an update); a NEW id
        carrying a non-NULL ``mb_release_id`` already held by another row
        raises ``UniqueViolation``, mirroring the production schema.
        """
        rid = row["id"]
        self._assert_mb_release_id_unique(
            row.get("mb_release_id"), exclude_id=rid)
        self._requests[rid] = dict(copy.deepcopy(row))
        self._next_request_id = max(self._next_request_id, rid)

    def request(self, request_id: int) -> dict[str, Any]:
        """Get a request row (for test assertions). Raises KeyError if missing."""
        return self._requests[request_id]

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def assert_log(self, test: Any, index: int, **expected: Any) -> None:
        """Assert fields on a download_log entry at the given index.

        Usage: db.assert_log(self, 0, outcome="success", request_id=42)
        """
        test.assertGreater(len(self.download_logs), index,
                           f"Expected at least {index + 1} download_log entries, "
                           f"got {len(self.download_logs)}")
        entry = self.download_logs[index]
        for field_name, value in expected.items():
            actual = getattr(entry, field_name, entry.extra.get(field_name))
            test.assertEqual(actual, value,
                             f"download_log[{index}].{field_name}: "
                             f"expected {value!r}, got {actual!r}")

    # Shared private helpers: two or more clusters call each, or the base
    # itself does (``_assert_mb_release_id_unique``, from ``seed_request``),
    # so one definition lives here instead of a copy or a stub. A helper
    # with a single consumer belongs in that consumer's module, not here.

    @staticmethod
    def _accusation_alias_projection(
        evidence: AlbumQualityEvidence | None,
        prefix: str,
    ) -> dict[str, object]:
        """Mirror ``accusation_evidence_columns`` for one evidence join.

        The production queries project these nine aliases from a LEFT
        JOIN, so an unmatched join yields all-NULL — exactly what a fake
        with no linked evidence must hand back, or the fake would be more
        permissive than production about which flags a surface can see.
        """
        measurement = evidence.measurement if evidence is not None else None
        return {
            f"{prefix}format": (
                measurement.format if measurement is not None else None),
            f"{prefix}spectral_grade": (
                measurement.spectral_grade
                if measurement is not None else None),
            f"{prefix}spectral_bitrate": (
                measurement.spectral_bitrate_kbps
                if measurement is not None else None),
            f"{prefix}spectral_subject": (
                measurement.spectral_subject
                if measurement is not None else None),
            f"{prefix}was_converted_from": (
                None
                if prefix == CANDIDATE_EVIDENCE_PREFIX
                else measurement.was_converted_from
                if measurement is not None
                else None
            ),
            f"{prefix}cliff_hz": (
                measurement.cliff_hz if measurement is not None else None),
            f"{prefix}codec_family": (
                measurement.codec_family
                if measurement is not None else None),
            f"{prefix}storage_format": (
                evidence.storage_format if evidence is not None else None),
            f"{prefix}filetype_band": (
                evidence.filetype_band if evidence is not None else None),
        }

    def _assert_mb_release_id_unique(
        self, mb_release_id: Any, exclude_id: int | None = None,
    ) -> None:
        """Mirror migrations/001's UNIQUE on album_requests.mb_release_id.

        PG UNIQUE permits any number of NULLs, so ``None`` always passes.
        Test-fidelity Rule B — the fake must not be more permissive than
        the real INSERT (#445 item 4).
        """
        if mb_release_id is None:
            return
        for rid, row in self._requests.items():
            if rid == exclude_id:
                continue
            if row.get("mb_release_id") == mb_release_id:
                import psycopg2.errors

                raise psycopg2.errors.UniqueViolation(
                    "duplicate key value violates unique constraint "
                    f'"album_requests_mb_release_id_key" — mb_release_id '
                    f"{mb_release_id!r} is already on request {rid}"
                )


    def _automation_job_has_authority(self, row) -> bool:
        request_id = row.get("request_id")
        request = (
            self._requests.get(int(request_id))
            if request_id is not None
            else None
        )
        return bool(
            row.get("job_type") == IMPORT_JOB_AUTOMATION
            and request is not None
            and request.get("status") == "processing"
            and request.get("active_automation_import_job_id") == row.get("id")
        )

    def _compute_search_summary(
        self, request_id: int,
    ) -> dict[str, Any] | None:
        """Compute one row of the ``request_search_summary`` view.

        Mirrors the SQL aggregate against ``self.search_logs``, INCLUDING
        the view's 14-day window (``migrations/031_request_search_summary_
        view.sql`` filters ``sl.created_at >= NOW() - INTERVAL '14 days'``
        in its outer WHERE and again inside the
        ``first_strategy_with_cands`` subquery). Windowing ``rows`` here
        covers both: every aggregate below, and the earliest-with-
        candidates pick, read the same windowed list.

        Returns ``None`` when the request has zero rows IN WINDOW —
        matches the view's ``GROUP BY`` semantics (empty groups produce
        no row).
        """
        cutoff = _utcnow() - _SEARCH_SUMMARY_WINDOW
        rows = [
            e for e in self.search_logs
            if e.request_id == int(request_id)
            and self._as_utc(e.created_at) >= cutoff
        ]
        if not rows:
            return None
        total = len(rows)
        with_cands = sum(
            1 for e in rows
            if e.candidates is not None and e.candidates not in ("", "[]")
        )
        found = sum(1 for e in rows if e.outcome == "found")
        near_cap = sum(
            1 for e in rows
            if (e.result_count is not None and e.result_count >= 950)
        )
        zero_results = sum(1 for e in rows if e.result_count == 0)
        pre_filter_skips = sum(
            int(e.pre_filter_skip_count or 0) for e in rows
        )
        # first_strategy_with_cands = earliest row that had candidates
        # (mirrors the view's correlated subquery ASC ordering).
        with_cands_sorted = sorted(
            (e for e in rows
             if e.candidates is not None and e.candidates not in ("", "[]")),
            key=lambda e: (e.created_at, e.id),
        )
        first_strategy = (
            with_cands_sorted[0].plan_strategy
            if with_cands_sorted else None
        )
        # dominant_rejection_reason — mode of non-null rejection_reason values.
        reason_counts: dict[str, int] = {}
        for e in rows:
            if e.rejection_reason is None:
                continue
            reason_counts[e.rejection_reason] = (
                reason_counts.get(e.rejection_reason, 0) + 1
            )
        # MODE() WITHIN GROUP (ORDER BY sl.rejection_reason) picks the most
        # frequent value and breaks a tie on that ORDER BY — the smallest
        # reason wins, never the first one seeded. Sorting by
        # (-count, reason) reproduces both halves; a plain max() on count
        # alone returned whichever tied reason happened to be inserted
        # first, which is not a fact the database has.
        dominant = (
            min(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))[0]
            if reason_counts else None
        )
        last_search = max(rows, key=lambda e: (e.created_at, e.id)).created_at
        return {
            "request_id": int(request_id),
            "total_searches": total,
            "with_cands_count": with_cands,
            "found_count": found,
            "near_cap_count": near_cap,
            "zero_results_count": zero_results,
            "pre_filter_skips_total": pre_filter_skips,
            "first_strategy_with_cands": first_strategy,
            "dominant_rejection_reason": dominant,
            "last_search_at": last_search,
        }

    def _current_evidence_for_request(
        self, row: Mapping[str, object],
    ) -> AlbumQualityEvidence | None:
        """The evidence row ``album_requests.current_evidence_id`` names."""
        evidence_id = row.get("current_evidence_id")
        if not isinstance(evidence_id, int) or isinstance(evidence_id, bool):
            return None
        return self._evidence_by_id.get(evidence_id)


    @staticmethod
    def _live_beets_child_refuses(lease: ExecutionLeaseSnapshot | None) -> bool:
        """Whether a Beets child on the caller's lease refuses this call.

        Production spells this as an unconditional early return above the
        SQL: ``heartbeat_import_job_preview``,
        ``mark_import_job_preview_importable``,
        ``authorize_import_job_launch`` and
        ``set_import_job_candidate_evidence`` each refuse before their
        statement runs, and none of those statements' non-automation arms
        re-checks the child. (Their shapes differ — two spell
        ``job_type <> 'automation_import'``, one adds a preview-status
        term, and ``authorize_import_job_launch`` has no ``<>`` arm at all
        but three positive per-type arms. What they share is that not one
        of them reads ``execution_beets_pid``.) So for a force, local or
        YouTube job this guard is the whole enforcement, and nesting it
        inside an automation arm lets every other type through
        mid-Beets-mutation.

        One predicate rather than a clause repeated at each caller, so the
        rule cannot be dropped from one of them the way it was from four
        (issue #1347). ``_candidate_job_type_routes`` reads it too, where
        it was already spelled correctly for the two candidate scans
        (#1313).
        """
        return lease is not None and lease.beets is not None

    @staticmethod
    def _execution_lease_matches(
        row,
        lease: ExecutionLeaseSnapshot | None,
        *,
        include_child: bool,
    ) -> bool:
        if lease is None:
            return False
        matches = (
            row.get("execution_invocation_id") == lease.invocation_id
            and row.get("execution_host_boot_id") == lease.host_boot_id
            and row.get("execution_systemd_unit") == lease.systemd_unit
            and row.get("execution_worker_pid") == lease.worker.pid
            and row.get("execution_worker_start_ticks")
            == lease.worker.start_ticks
        )
        if not matches or not include_child:
            return matches
        child = lease.beets
        return (
            row.get("execution_beets_pid")
            == (child.pid if child is not None else None)
            and row.get("execution_beets_start_ticks")
            == (child.start_ticks if child is not None else None)
        )


    def _mint_download_log_id(self) -> int:
        """Advance the download_log id counter, mirroring a PG sequence.

        A sequence-backed PK never regresses and never collides. Tests
        may pin ids FORWARD (``db._next_download_log_id = 41`` → next id
        42); rewinding below an existing id is the bug this guard exists
        to catch — the three log accessors silently disagree on duplicate
        ids (#445 item 4).
        """
        new_id = self._next_download_log_id + 1
        taken = {entry.id for entry in self.download_logs}
        if new_id in taken:
            import psycopg2.errors

            raise psycopg2.errors.UniqueViolation(
                "duplicate key value violates unique constraint "
                f'"download_log_pkey" — id {new_id} already exists '
                "(a test rewound _next_download_log_id)"
            )
        if any(existing > new_id for existing in taken):
            raise AssertionError(
                f"minted download_log id {new_id} precedes existing ids "
                f"{sorted(taken)} — production's sequence-backed PK can "
                "never do that (rewound _next_download_log_id)"
            )
        self._next_download_log_id = new_id
        return new_id

    def _require_fake_exact_processing_owner(
        self,
        *,
        request_id: int,
        job_id: int,
    ) -> None:
        """Mirror ``_require_exact_processing_owner``'s typed conflict kinds.

        Production raises three distinct ``CleanupJournalConflict`` kinds here
        and callers catch the class, so the fake must too.
        """
        request = self._requests.get(request_id)
        if request is None:
            raise CleanupJournalConflict(
                "request_missing",
                f"cleanup request {request_id} does not exist",
            )
        if (
            request.get("status") != "processing"
            or request.get("active_automation_import_job_id") != job_id
        ):
            raise CleanupJournalConflict(
                "owner_mismatch",
                f"job {job_id} is not request {request_id}'s exact "
                "processing owner",
            )
        job = next(
            (row for row in self._import_jobs if row["id"] == job_id),
            None,
        )
        if (
            job is None
            or job.get("request_id") != request_id
            or job.get("job_type") != IMPORT_JOB_AUTOMATION
            or job.get("status") not in IMPORT_JOB_ACTIVE_STATUSES
        ):
            raise CleanupJournalConflict(
                "job_mismatch",
                f"job {job_id} is not an active automation job for "
                f"request {request_id}",
            )



    # Cross-cluster surface: declared here so the CALLING mixin type-checks
    # without importing the composed class. Two shapes need it, and both
    # are load-bearing (measured: deleting the nine stubs of the second
    # shape produces six Pyright errors).
    #
    #   1. A mixin calls a sibling's public method on ``self``: 16 stubs.
    #   2. A mixin hands ``self`` to something typed against the shared
    #      base or against a production Protocol: 9 stubs. The five
    #      request-lane names reach ``_FakeTerminalTransitionsDB``, whose
    #      ``db`` is a ``_FakePipelineDBBase``; the four cleanup-journal
    #      names satisfy ``lib.processing_cleanup.OwnerProcessingCleanupDB``
    #      where ``_FakeImportJobsMixin`` passes ``self``.
    #
    # Each stub repeats the owning mixin's real signature (``advisory_lock``
    # is the one exception: its owner is a ``@contextmanager``, so the stub
    # declares the context-manager type the callers actually receive), which
    # is what makes Pyright report an incompatible override the moment the
    # two drift. The bodies never execute; the composed FakePipelineDB MRO
    # resolves every call to the owning sibling mixin.

    def add_cooldown(self, username: str, cooldown_until: datetime,
                     reason: str | None = None) -> None: ...

    def add_denylist(self, request_id: int, username: str,
                     reason: str | None = None) -> None: ...

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...

    def check_and_apply_cooldown(self, username: str,
                                  config: Any = None) -> bool: ...

    def checkpoint_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        step_progress: Mapping[str, object],
    ) -> ProcessingCleanupJournalRow: ...

    def complete_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        receipt: CleanupJournalReceipt,
    ) -> ProcessingCleanupJournalRow: ...

    def create_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        intent: CleanupJournalIntent,
    ) -> ProcessingCleanupJournalRow: ...

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob: ...

    def get_import_job(self, job_id: int) -> ImportJob | None: ...

    def get_import_job_candidate_evidence_id(
        self,
        import_job_id: int,
    ) -> int | None: ...

    def get_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
    ) -> ProcessingCleanupJournalRow | None: ...

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def get_search_plan_readiness(
        self,
        generator_id: str,
    ) -> dict[str, Any]: ...

    def log_download(self, request_id: int,
                     soulseek_username: str | None = None,
                     contributor_usernames: Sequence[str] | None = None,
                     filetype: str | None = None,
                     download_path: str | None = None,
                     beets_distance: float | None | ValidationProjectionUnset = (
                         VALIDATION_PROJECTION_UNSET),
                     beets_scenario: str | None | ValidationProjectionUnset = (
                         VALIDATION_PROJECTION_UNSET),
                     beets_detail: str | None = None,
                     valid: bool | None = None,
                     outcome: str | None = None,
                     staged_path: str | None = None,
                     error_message: str | None = None,
                     bitrate: int | None = None,
                     sample_rate: int | None = None,
                     bit_depth: int | None = None,
                     is_vbr: bool | None = None,
                     was_converted: bool | None = None,
                     original_filetype: str | None = None,
                     slskd_filetype: str | None = None,
                     actual_filetype: str | None = None,
                     actual_min_bitrate: int | None = None,
                     spectral_grade: str | None = None,
                     spectral_bitrate: int | None = None,
                     existing_min_bitrate: int | None = None,
                     existing_spectral_bitrate: int | None = None,
                     import_result: Any = None,
                     validation_result: Any = None,
                     final_format: str | None = None,
                     v0_probe_kind: str | None = None,
                     v0_probe_min_bitrate: int | None = None,
                     v0_probe_avg_bitrate: int | None = None,
                     v0_probe_median_bitrate: int | None = None,
                     existing_v0_probe_kind: str | None = None,
                     existing_v0_probe_min_bitrate: int | None = None,
                     existing_v0_probe_avg_bitrate: int | None = None,
                     existing_v0_probe_median_bitrate: int | None = None,
                     transfer_detail: Any = None,
                     source_download_log_id: int | None = None,
                     source: str = "slskd",
                     **extra: Any) -> int: ...

    def mark_import_job_completed(
        self,
        job_id: int,
        *,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None: ...

    def mark_import_job_failed(
        self,
        job_id: int,
        *,
        error: str,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None: ...

    def mark_import_job_preview_failed(
        self,
        job_id: int,
        *,
        preview_status: str,
        error: str,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None: ...

    def mark_imported_with_rescue(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool: ...

    def persist_import_terminal_outcome(
        self,
        command: ImportTerminalOutcome,
    ) -> TerminalOutcomeResult: ...

    def record_attempt(
        self,
        request_id: int,
        attempt_type: str,
        *,
        expected_status: str,
    ) -> bool: ...

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str = "downloading",
        **fields: Any,
    ) -> bool: ...

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> bool: ...

    def set_download_log_candidate_evidence(
        self,
        download_log_id: int,
        evidence_id: int | None,
        *,
        direct_attribution: bool = False,
        contributor_usernames: Sequence[str] | None = None,
    ) -> None: ...

    def update_request_fields(
        self,
        request_id: int,
        **fields: Any,
    ) -> bool: ...

    def update_status(
        self,
        request_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool: ...
